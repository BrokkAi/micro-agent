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
// must be set: Command selects the stdio transport, URL streamable HTTP.
type MCPServer struct {
	Command string            `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
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
	Shell           string               `json:"shell,omitempty"`
	ShellTimeout    int                  `json:"shell_timeout_seconds,omitempty"`
	SystemPrompt    string               `json:"system_prompt,omitempty"`
	MCPServers      map[string]MCPServer `json:"mcp_servers,omitempty"`
}

// Keys lists the scalar settings editable through /config, in display order.
var Keys = []string{"model", "reasoning_effort", "default_mode", "max_turns", "max_tokens", "shell_timeout_seconds", "shell", "base_url", "system_prompt"}

// Efforts are the OpenRouter reasoning effort levels; "" leaves it to the model.
var Efforts = []string{"none", "minimal", "low", "medium", "high"}

// Store guards a Config and the file it was loaded from.
type Store struct {
	path string
	mu   sync.Mutex
	cfg  Config
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
	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(data, &store.cfg); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	return store, nil
}

// Path is the backing file.
func (s *Store) Path() string { return s.path }

// Dir is the directory holding the config file and agent state.
func (s *Store) Dir() string { return filepath.Dir(s.path) }

// Get returns a copy of the configuration with defaults applied.
func (s *Store) Get() Config {
	s.mu.Lock()
	cfg := s.cfg
	s.mu.Unlock()
	if cfg.APIKey == "" {
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
	next := s.cfg
	if err := change(&next); err != nil {
		return err
	}
	data, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(s.path, append(data, '\n'), 0o600); err != nil {
		return err
	}
	s.cfg = next
	return nil
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
