// Package config loads and saves the micro-agent JSON configuration file.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	DefaultBaseURL = "https://openrouter.ai/api/v1"
	DefaultModel   = "deepseek/deepseek-v4.1-flash"
)

// MCPServer is a globally configured MCP server. Exactly one of Command or URL
// must be set: Command selects the stdio transport, URL streamable HTTP, or
// the legacy HTTP+SSE transport when Transport is "sse".
type MCPServer struct {
	Command   string            `json:"command,omitempty"`
	Args      []string          `json:"args,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	URL       string            `json:"url,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Transport string            `json:"transport,omitempty"`
}

// Config is the persisted agent configuration.
type Config struct {
	APIKey          string               `json:"api_key,omitempty"`
	BaseURL         string               `json:"base_url,omitempty"`
	Model           string               `json:"model,omitempty"`
	Models          []string             `json:"models,omitempty"`
	ReasoningEffort string               `json:"reasoning_effort,omitempty"`
	MaxTokens       int                  `json:"max_tokens,omitempty"`
	MaxTurns        int                  `json:"max_turns,omitempty"`
	DefaultMode     string               `json:"default_mode,omitempty"`
	AutoCompact     *bool                `json:"auto_compact,omitempty"`
	Shell           string               `json:"shell,omitempty"`
	ShellTimeout    int                  `json:"shell_timeout_seconds,omitempty"`
	ShellEnv        map[string]string    `json:"shell_env,omitempty"`
	SystemPrompt    string               `json:"system_prompt,omitempty"`
	MCPServers      map[string]MCPServer `json:"mcp_servers,omitempty"`
}

// AutoCompactEnabled reports whether long conversations are compacted
// automatically. It is on unless turned off.
func (c Config) AutoCompactEnabled() bool { return c.AutoCompact == nil || *c.AutoCompact }

// Keys lists the scalar settings editable through /config, in display order.
var Keys = []string{"model", "reasoning_effort", "default_mode", "max_turns", "max_tokens", "auto_compact", "shell_timeout_seconds", "shell", "base_url", "system_prompt"}

// Efforts are the OpenRouter reasoning effort levels; "" leaves it to the model.
var Efforts = []string{"none", "minimal", "low", "medium", "high"}

// Store guards a Config and the file it was loaded from. Other processes
// (micro-agent login, other editor windows) may rewrite the file at any time.
type Store struct {
	path string
	mu   sync.Mutex
	cfg  Config
	seen os.FileInfo // the file cfg was last read from or written to
	// loggedOut hides OPENROUTER_API_KEY after Logout until SetAPIKey.
	loggedOut bool
}

// DefaultPath returns the platform config location for micro-agent.
func DefaultPath() (string, error) {
	if path := os.Getenv("MICRO_AGENT_CONFIG"); path != "" {
		return path, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "micro-agent", "config.json"), nil
}

// Load reads path; a missing file yields the defaults.
func Load(path string) (*Store, error) {
	store := &Store{path: path}
	if err := store.refresh(true); err != nil {
		return nil, err
	}
	return store, nil
}

// Path is the backing file.
func (s *Store) Path() string { return s.path }

// Dir is the directory holding the config file and agent state.
func (s *Store) Dir() string { return filepath.Dir(s.path) }

// Get returns a copy of the configuration with defaults applied, first picking
// up changes another process made to the file.
func (s *Store) Get() Config {
	s.mu.Lock()
	_ = s.refresh(false) // a bad file keeps the last good configuration
	cfg, loggedOut := s.cfg, s.loggedOut
	s.mu.Unlock()
	if cfg.APIKey == "" && !loggedOut {
		cfg.APIKey = os.Getenv("OPENROUTER_API_KEY")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultBaseURL
	}
	if cfg.Model == "" {
		cfg.Model = DefaultModel
	}
	if cfg.MaxTurns <= 0 {
		cfg.MaxTurns = 200
	}
	if cfg.DefaultMode == "" {
		cfg.DefaultMode = "default"
	}
	if cfg.ShellTimeout <= 0 {
		cfg.ShellTimeout = 120
	}
	return cfg
}

// Update applies change to the raw (default-free) configuration and saves it.
func (s *Store) Update(change func(*Config) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(change)
}

// SetAPIKey saves key and undoes Logout.
func (s *Store) SetAPIKey(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.update(func(c *Config) error { c.APIKey = key; return nil }); err != nil {
		return err
	}
	s.loggedOut = false
	return nil
}

// Logout clears the saved key and, so that logging out works while
// OPENROUTER_API_KEY is set, ignores that variable until SetAPIKey.
func (s *Store) Logout() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.update(func(c *Config) error { c.APIKey = ""; return nil }); err != nil {
		return err
	}
	s.loggedOut = true
	return nil
}

// update re-reads the file so that changes made elsewhere are not overwritten,
// then applies change and saves. The caller holds s.mu.
func (s *Store) update(change func(*Config) error) error {
	if err := s.refresh(true); err != nil {
		return err
	}
	next := s.cfg
	if err := change(&next); err != nil {
		return err
	}
	info, err := write(s.path, next)
	if err != nil {
		return err
	}
	s.cfg, s.seen = next, info
	return nil
}

// refresh reads the file, unless force is false and the file is unchanged
// since it was last seen. A missing or unparsable file keeps the in-memory
// configuration; the latter is reported. The caller holds s.mu.
func (s *Store) refresh(force bool) error {
	info, err := os.Stat(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !force && s.seen != nil && os.SameFile(info, s.seen) && info.Size() == s.seen.Size() && info.ModTime().Equal(s.seen.ModTime()) {
		return nil
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}
	if err := validate(cfg); err != nil {
		return fmt.Errorf("%s: %w", s.path, err)
	}
	s.cfg, s.seen = cfg, info
	return nil
}

func validate(cfg Config) error {
	for name, server := range cfg.MCPServers {
		if (server.Command == "") == (server.URL == "") {
			return fmt.Errorf("mcp_servers.%s: set exactly one of command or url", name)
		}
		switch server.Transport {
		case "":
		case "sse":
			if server.URL == "" {
				return fmt.Errorf("mcp_servers.%s: transport \"sse\" needs a url", name)
			}
		default:
			return fmt.Errorf("mcp_servers.%s: unknown transport %q (use \"sse\", or omit it for stdio or streamable HTTP)", name, server.Transport)
		}
	}
	return nil
}

// rename is os.Rename; tests replace it to make the rename fail.
var rename = os.Rename

// write saves cfg through a temporary file and a rename, so other processes
// never read a partial file. It returns the new file's info.
func write(path string, cfg Config) (os.FileInfo, error) {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return nil, err
	}
	data = append(data, '\n')
	path = resolve(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".config-*.json") // mode 0600
	if err != nil {
		return nil, err
	}
	var info os.FileInfo
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		info, err = f.Stat()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(f.Name())
		return nil, err
	}
	if err := rename(f.Name(), path); err != nil {
		_ = os.Remove(f.Name())
		// A bind-mounted config.json (Docker, Kubernetes subPath) cannot be
		// replaced (EBUSY) but can still be written in place.
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return nil, err
		}
		return os.Stat(path)
	}
	return info, nil
}

// resolve follows symlinks so that a save replaces the target of a symlinked
// config.json and keeps the link.
func resolve(path string) string {
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real
	}
	// EvalSymlinks fails on a dangling link. Follow it by hand so the save
	// creates the target, as os.WriteFile would.
	for range 255 { // the link limit EvalSymlinks uses
		link, err := os.Readlink(path)
		if err != nil {
			break
		}
		if !filepath.IsAbs(link) {
			link = filepath.Join(filepath.Dir(path), link)
		}
		path = link
	}
	return path
}

// Kind is the type of a key from Keys: "integer", "boolean" or "string".
func Kind(key string) string {
	switch key {
	case "max_turns", "max_tokens", "shell_timeout_seconds":
		return "integer"
	case "auto_compact":
		return "boolean"
	}
	return "string"
}

// Set assigns one key from Keys, parsing value as needed. An empty value
// restores the default.
func Set(cfg *Config, key, value string) error {
	value = strings.TrimSpace(value)
	integer := func(target *int) error {
		if value == "" {
			*target = 0
			return nil
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return fmt.Errorf("%s must be a non-negative integer", key)
		}
		*target = n
		return nil
	}
	switch key {
	case "model":
		cfg.Model = value
	case "reasoning_effort":
		if value != "" && !slices.Contains(Efforts, value) {
			return fmt.Errorf("reasoning_effort must be one of %s", strings.Join(Efforts, ", "))
		}
		cfg.ReasoningEffort = value
	case "default_mode":
		cfg.DefaultMode = value
	case "max_turns":
		return integer(&cfg.MaxTurns)
	case "max_tokens":
		return integer(&cfg.MaxTokens)
	case "auto_compact":
		if value == "" {
			cfg.AutoCompact = nil
			return nil
		}
		on, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("%s must be true or false", key)
		}
		cfg.AutoCompact = &on
	case "shell_timeout_seconds":
		return integer(&cfg.ShellTimeout)
	case "shell":
		cfg.Shell = value
	case "base_url":
		cfg.BaseURL = strings.TrimRight(value, "/")
	case "system_prompt":
		cfg.SystemPrompt = value
	case "api_key":
		cfg.APIKey = value
	default:
		return fmt.Errorf("unknown setting %q (known: %s)", key, strings.Join(Keys, ", "))
	}
	return nil
}

// Value renders one key from Keys for display.
func Value(cfg Config, key string) string {
	switch key {
	case "model":
		return cfg.Model
	case "reasoning_effort":
		return cfg.ReasoningEffort
	case "default_mode":
		return cfg.DefaultMode
	case "max_turns":
		return strconv.Itoa(cfg.MaxTurns)
	case "max_tokens":
		return strconv.Itoa(cfg.MaxTokens)
	case "auto_compact":
		return strconv.FormatBool(cfg.AutoCompactEnabled())
	case "shell_timeout_seconds":
		return strconv.Itoa(cfg.ShellTimeout)
	case "shell":
		return cfg.Shell
	case "base_url":
		return cfg.BaseURL
	case "system_prompt":
		return cfg.SystemPrompt
	}
	return ""
}

// ModelChoices returns the configured model list with the current model first.
func ModelChoices(cfg Config) []string {
	seen := map[string]bool{}
	var choices []string
	for _, model := range append([]string{cfg.Model}, cfg.Models...) {
		if model != "" && !seen[model] {
			seen[model] = true
			choices = append(choices, model)
		}
	}
	sort.Strings(choices[1:])
	return choices
}
