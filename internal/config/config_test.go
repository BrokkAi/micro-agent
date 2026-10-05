package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func load(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func onDisk(t *testing.T, path string) Config {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func setModel(model string) func(*Config) error {
	return func(c *Config) error { c.Model = model; return nil }
}

func TestGetSeesExternalWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store := load(t, path)
	if err := store.Update(setModel("a/one")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"api_key":"external","model":"b/two"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if cfg := store.Get(); cfg.APIKey != "external" || cfg.Model != "b/two" {
		t.Fatalf("got key %q model %q", cfg.APIKey, cfg.Model)
	}
}

func TestUpdateKeepsExternalKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store := load(t, path)
	if err := store.Update(setModel("a/one")); err != nil {
		t.Fatal(err)
	}
	// micro-agent login in another process.
	if err := load(t, path).SetAPIKey("external"); err != nil {
		t.Fatal(err)
	}
	if err := store.Update(setModel("b/two")); err != nil {
		t.Fatal(err)
	}
	if cfg := onDisk(t, path); cfg.APIKey != "external" || cfg.Model != "b/two" {
		t.Fatalf("on disk: key %q model %q", cfg.APIKey, cfg.Model)
	}
}

func TestLogoutIgnoresEnv(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "env")
	path := filepath.Join(t.TempDir(), "config.json")
	store := load(t, path)
	if err := store.SetAPIKey("saved"); err != nil {
		t.Fatal(err)
	}
	if err := store.Logout(); err != nil {
		t.Fatal(err)
	}
	if key := store.Get().APIKey; key != "" {
		t.Fatalf("key after logout = %q", key)
	}
	if key := onDisk(t, path).APIKey; key != "" {
		t.Fatalf("key on disk after logout = %q", key)
	}
	if err := load(t, path).SetAPIKey("external"); err != nil {
		t.Fatal(err)
	}
	if key := store.Get().APIKey; key != "external" {
		t.Fatalf("external login not seen, key = %q", key)
	}
	if err := store.Logout(); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAPIKey("again"); err != nil {
		t.Fatal(err)
	}
	if key := store.Get().APIKey; key != "again" {
		t.Fatalf("key after SetAPIKey = %q", key)
	}
	if err := store.Update(func(c *Config) error { c.APIKey = ""; return nil }); err != nil {
		t.Fatal(err)
	}
	if key := store.Get().APIKey; key != "env" {
		t.Fatalf("env fallback after SetAPIKey = %q", key)
	}
}

func TestBadFileKeepsLastGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"model":"a/one"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := load(t, path)
	if err := os.WriteFile(path, []byte(`{"model":`), 0o600); err != nil {
		t.Fatal(err)
	}
	if model := store.Get().Model; model != "a/one" {
		t.Fatalf("model with invalid file = %q", model)
	}
	if err := store.Update(setModel("b/two")); err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("Update over invalid file: %v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != `{"model":` {
		t.Fatalf("invalid file overwritten: %s", data)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if model := store.Get().Model; model != "a/one" {
		t.Fatalf("model with missing file = %q", model)
	}
	if err := store.Update(func(*Config) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if model := onDisk(t, path).Model; model != "a/one" {
		t.Fatalf("model rewritten after removal = %q", model)
	}
}

func TestWriteIsAtomicAndPrivate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "config.json")
	store := load(t, path)
	if err := store.Update(func(c *Config) error { c.ShellEnv = map[string]string{"PAGER": "cat"}; return nil }); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "config.json" {
		t.Fatalf("directory holds %v", entries)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v", info.Mode().Perm())
	}
	if env := load(t, path).Get().ShellEnv; env["PAGER"] != "cat" {
		t.Fatalf("shell_env = %v", env)
	}
}

func TestWriteFollowsSymlink(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		exists, relative, via bool // via: config.json links to a second link
	}{
		{name: "existing", exists: true},
		{name: "dangling"},
		{name: "dangling relative", relative: true},
		{name: "dangling chain", relative: true, via: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, "dotfiles", "real.json")
			if tc.exists {
				if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(target, []byte(`{}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			dest := target
			if tc.relative {
				dest = filepath.Join("dotfiles", "real.json")
			}
			link := filepath.Join(dir, "config.json")
			if tc.via {
				if err := os.Symlink(dest, filepath.Join(dir, "mid.json")); err != nil {
					t.Skip("symlinks unavailable:", err)
				}
				dest = "mid.json"
			}
			if err := os.Symlink(dest, link); err != nil {
				t.Skip("symlinks unavailable:", err)
			}
			if err := load(t, link).Update(setModel("a/one")); err != nil {
				t.Fatal(err)
			}
			for _, l := range []string{link, filepath.Join(dir, "mid.json")} {
				if info, err := os.Lstat(l); err == nil && info.Mode()&os.ModeSymlink == 0 {
					t.Fatalf("%s replaced by a file", l)
				}
			}
			if model := onDisk(t, target).Model; model != "a/one" {
				t.Fatalf("target model = %q", model)
			}
		})
	}
}

func TestWriteInPlaceWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	store := load(t, path)
	if err := store.Update(setModel("a/one")); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// As for a bind-mounted file, which rename(2) reports as busy.
	rename = func(string, string) error { return errors.New("device or resource busy") }
	t.Cleanup(func() { rename = os.Rename })
	if err := store.Update(setModel("b/two")); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) {
		t.Fatal("file replaced instead of written in place")
	}
	if model := onDisk(t, path).Model; model != "b/two" {
		t.Fatalf("model on disk = %q", model)
	}
	if model := store.Get().Model; model != "b/two" {
		t.Fatalf("model = %q", model)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatalf("directory holds %v %v", entries, err)
	}
}

func TestAutoCompact(t *testing.T) {
	var cfg Config
	if !cfg.AutoCompactEnabled() || Value(cfg, "auto_compact") != "true" {
		t.Fatal("auto_compact should default to on")
	}
	if err := Set(&cfg, "auto_compact", "false"); err != nil {
		t.Fatal(err)
	}
	if cfg.AutoCompactEnabled() || Value(cfg, "auto_compact") != "false" {
		t.Fatal("auto_compact=false not applied")
	}
	if err := Set(&cfg, "auto_compact", "true"); err != nil || Value(cfg, "auto_compact") != "true" {
		t.Fatalf("auto_compact=true: %v", err)
	}
	if err := Set(&cfg, "auto_compact", ""); err != nil || cfg.AutoCompact != nil {
		t.Fatalf("empty value should restore the default: %v", err)
	}
	if err := Set(&cfg, "auto_compact", "sometimes"); err == nil {
		t.Fatal("expected an error for a non-boolean value")
	}
	data, err := json.Marshal(Config{AutoCompact: new(false)})
	if err != nil || string(data) != `{"auto_compact":false}` {
		t.Fatalf("explicit false not persisted: %s %v", data, err)
	}
}

func TestKind(t *testing.T) {
	for key, want := range map[string]string{"max_turns": "integer", "max_tokens": "integer", "shell_timeout_seconds": "integer", "auto_compact": "boolean", "model": "string", "system_prompt": "string"} {
		if got := Kind(key); got != want {
			t.Errorf("Kind(%q) = %q, want %q", key, got, want)
		}
	}
	// Every key round-trips through Value and Set.
	cfg := Config{Model: "a/one", ReasoningEffort: "low", DefaultMode: "plan", MaxTurns: 3, MaxTokens: 4, AutoCompact: new(false), ShellTimeout: 5, Shell: "zsh", BaseURL: "http://x", SystemPrompt: "hi"}
	var copied Config
	for _, key := range Keys {
		if err := Set(&copied, key, Value(cfg, key)); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
		if Value(copied, key) != Value(cfg, key) {
			t.Errorf("%s = %q, want %q", key, Value(copied, key), Value(cfg, key))
		}
	}
}

func TestTransportValidation(t *testing.T) {
	for _, tc := range []struct {
		server, transport, err string
	}{
		{`{"command":"srv"}`, "", ""},
		{`{"url":"http://x"}`, "", ""},
		{`{"url":"http://x","transport":"sse"}`, "sse", ""},
		{`{"command":"srv","transport":"sse"}`, "", `transport "sse" needs a url`},
		{`{"command":"srv","url":"http://x","transport":"sse"}`, "", `mcp_servers.s: set exactly one of command or url`},
		{`{"command":"srv","url":"http://x"}`, "", `set exactly one of command or url`},
		{`{}`, "", `set exactly one of command or url`},
		{`{"url":"http://x","transport":"websocket"}`, "", `mcp_servers.s: unknown transport "websocket"`},
	} {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(`{"mcp_servers":{"s":`+tc.server+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		store, err := Load(path)
		switch {
		case tc.err != "":
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("%s: got %v, want %q", tc.server, err, tc.err)
			}
		case err != nil:
			t.Errorf("%s: %v", tc.server, err)
		case store.Get().MCPServers["s"].Transport != tc.transport:
			t.Errorf("%s: transport = %q", tc.server, store.Get().MCPServers["s"].Transport)
		}
	}

	// A re-read that fails validation keeps the last good config, and Update
	// refuses to overwrite the file.
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(`{"mcp_servers":{"s":{"url":"http://x"}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := load(t, path)
	bad := `{"mcp_servers":{"s":{"url":"http://x","transport":"websocket"}}}`
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if server := store.Get().MCPServers["s"]; server.URL != "http://x" || server.Transport != "" {
		t.Fatalf("server after invalid re-read = %+v", server)
	}
	if err := store.Update(setModel("b/two")); err == nil || !strings.Contains(err.Error(), "unknown transport") {
		t.Fatalf("Update over invalid file: %v", err)
	}
	if data, _ := os.ReadFile(path); string(data) != bad {
		t.Fatalf("invalid file overwritten: %s", data)
	}
}
