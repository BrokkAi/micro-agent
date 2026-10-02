package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BrokkAi/acp-go"
	acpagent "github.com/BrokkAi/acp-go/agent"
	"github.com/BrokkAi/acp-go/schema"
	"github.com/BrokkAi/micro-agent/internal/config"
)

// fakeRouter serves scripted SSE responses and records request bodies.
type fakeRouter struct {
	mu        sync.Mutex
	responses [][]string
	requests  []map[string]any
}

func (f *fakeRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/models" {
		fmt.Fprint(w, `{"data":[{"id":"test/model","context_length":1000}]}`)
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.requests = append(f.requests, body)
	var chunks []string
	if len(f.responses) > 0 {
		chunks, f.responses = f.responses[0], f.responses[1:]
	}
	f.mu.Unlock()
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprint(w, ": OPENROUTER PROCESSING\n\n")
	for _, chunk := range chunks {
		fmt.Fprintf(w, "data: %s\n\n", chunk)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func toolCallChunks(id, name string, args any) []string {
	encoded, _ := json.Marshal(args)
	half := len(encoded) / 2
	first, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{
		map[string]any{"index": 0, "id": id, "function": map[string]any{"name": name, "arguments": string(encoded[:half])}},
	}}}}})
	second, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{
		map[string]any{"index": 0, "function": map[string]any{"arguments": string(encoded[half:])}},
	}}, "finish_reason": "tool_calls"}}})
	return []string{string(first), string(second)}
}

func textChunks(text string) []string {
	chunk, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"content": text}, "finish_reason": "stop"}}, "usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 5, "cost": 0.01}})
	return []string{string(chunk)}
}

type harness struct {
	t       *testing.T
	conn    *acp.Connection
	init    acp.Initialization
	router  *fakeRouter
	store   *config.Store
	dir     string
	mu      sync.Mutex
	updates []schema.SessionNotification
	answer  schema.PermissionOptionId
}

func newHarness(t *testing.T, caps schema.ClientCapabilities) *harness {
	t.Helper()
	h := &harness{t: t, router: &fakeRouter{}, dir: t.TempDir(), answer: "allow_once"}
	server := httptest.NewServer(h.router)
	t.Cleanup(server.Close)
	store, err := config.Load(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Update(func(c *config.Config) error {
		c.APIKey, c.BaseURL, c.Model = "test-key", server.URL, "test/model"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.store = store

	agentIn, clientOut := io.Pipe()
	clientIn, agentOut := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = acpagent.New(New(store, "test")).Serve(ctx, agentIn, agentOut) }()

	handler := func(_ context.Context, method string, raw json.RawMessage) (any, error) {
		if method == schema.SessionRequestPermissionMethodName {
			return schema.RequestPermissionResponse{Outcome: schema.RequestPermissionOutcome{Selected: &schema.SelectedPermissionOutcome{OptionID: h.answer}}}, nil
		}
		return nil, &acp.RPCError{Code: -32601, Message: method}
	}
	notifications := func(method string, raw json.RawMessage) error {
		if method == schema.SessionUpdateMethodName {
			var n schema.SessionNotification
			if err := json.Unmarshal(raw, &n); err != nil {
				return err
			}
			h.mu.Lock()
			h.updates = append(h.updates, n)
			h.mu.Unlock()
		}
		return nil
	}
	h.conn = acp.Connect(clientIn, clientOut, handler, notifications)
	t.Cleanup(func() { h.conn.Close() })
	h.init, err = h.conn.InitializeWithInfo(context.Background(), caps, acp.ClientInfo{Name: "test", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) kinds() []schema.SessionUpdateKind {
	h.mu.Lock()
	defer h.mu.Unlock()
	var kinds []schema.SessionUpdateKind
	for _, n := range h.updates {
		kinds = append(kinds, n.Update.Kind)
	}
	return kinds
}

func (h *harness) prompt(session acp.Session, text string) schema.StopReason {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reason, err := h.conn.Prompt(ctx, session, text)
	if err != nil {
		h.t.Fatal(err)
	}
	return reason
}

func TestPromptRunsToolWithPermission(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "write_file", map[string]string{"path": "hello.txt", "content": "hi\n"}),
		textChunks("Created the file."),
	}
	session, err := h.conn.NewSession(context.Background(), h.dir)
	if err != nil {
		t.Fatal(err)
	}
	if reason := h.prompt(session, "make hello.txt"); reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	data, err := os.ReadFile(filepath.Join(h.dir, "hello.txt"))
	if err != nil || string(data) != "hi\n" {
		t.Fatalf("file = %q, %v", data, err)
	}
	second := h.router.requests[1]["messages"].([]any)
	last := second[len(second)-1].(map[string]any)
	if last["role"] != "tool" || last["tool_call_id"] != "call_1" || !strings.Contains(last["content"].(string), "Created") {
		t.Fatalf("tool result message = %v", last)
	}
	kinds := fmt.Sprint(h.kinds())
	for _, want := range []string{"tool_call", "tool_call_update", "agent_message_chunk", "usage_update", "session_info_update"} {
		if !strings.Contains(kinds, want) {
			t.Errorf("missing %s update in %s", want, kinds)
		}
	}

	// The session is persisted, listed, and replayed on load.
	list, err := h.conn.ListSessions(context.Background(), h.init, schema.ListSessionsRequest{Cwd: &h.dir})
	if err != nil || len(list.Sessions) != 1 || list.Sessions[0].Title == nil || *list.Sessions[0].Title != "make hello.txt" {
		t.Fatalf("list = %+v, %v", list, err)
	}
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	if _, err := h.conn.LoadSession(context.Background(), h.init, schema.LoadSessionRequest{SessionID: session.SessionID, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	kinds = fmt.Sprint(h.kinds())
	for _, want := range []string{"user_message_chunk", "tool_call", "agent_message_chunk", "available_commands_update"} {
		if !strings.Contains(kinds, want) {
			t.Errorf("replay missing %s in %s", want, kinds)
		}
	}
}

func TestRejectedPermissionDoesNotWrite(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.answer = "reject_once"
	h.router.responses = [][]string{
		toolCallChunks("call_1", "shell", map[string]string{"command": "echo hi > out.txt"}),
		textChunks("ok"),
	}
	session, err := h.conn.NewSession(context.Background(), h.dir)
	if err != nil {
		t.Fatal(err)
	}
	h.prompt(session, "run it")
	if _, err := os.Stat(filepath.Join(h.dir, "out.txt")); err == nil {
		t.Fatal("command ran despite rejection")
	}
	messages := h.router.requests[1]["messages"].([]any)
	if content := messages[len(messages)-1].(map[string]any)["content"].(string); !strings.Contains(content, "denied") {
		t.Fatalf("tool result = %q", content)
	}
}

func TestSlashConfigUpdatesFile(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	session, err := h.conn.NewSession(context.Background(), h.dir)
	if err != nil {
		t.Fatal(err)
	}
	h.prompt(session, "/config max_turns=7 reasoning_effort=high")
	cfg := h.store.Get()
	if cfg.MaxTurns != 7 || cfg.ReasoningEffort != "high" {
		t.Fatalf("config = %+v", cfg)
	}
	if len(h.router.requests) != 0 {
		t.Fatal("slash command reached the model")
	}
}

func TestApplyEdit(t *testing.T) {
	cases := []struct {
		text string
		args editArgs
		want string
		err  string
	}{
		{"a b a", editArgs{OldString: "b", NewString: "c"}, "a c a", ""},
		{"a b a", editArgs{OldString: "a", NewString: "c"}, "", "matches 2 times"},
		{"a b a", editArgs{OldString: "a", NewString: "c", ReplaceAll: true}, "c b c", ""},
		{"x\r\ny\r\n", editArgs{OldString: "x\ny", NewString: "x\nz"}, "x\r\nz\r\n", ""},
		{"abc", editArgs{OldString: "q", NewString: "r"}, "", "not found"},
	}
	for _, c := range cases {
		got, err := applyEdit(c.text, c.args)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("applyEdit(%q, %+v) error = %v, want %q", c.text, c.args, err, c.err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("applyEdit(%q, %+v) = %q, %v; want %q", c.text, c.args, got, err, c.want)
		}
	}
}

func TestShellRunsLocally(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "shell", map[string]string{"command": "echo micro-agent-ok"}),
		textChunks("done"),
	}
	session, err := h.conn.NewSession(context.Background(), h.dir)
	if err != nil {
		t.Fatal(err)
	}
	h.prompt(session, "run it")
	messages := h.router.requests[1]["messages"].([]any)
	content := messages[len(messages)-1].(map[string]any)["content"].(string)
	if !strings.Contains(content, "micro-agent-ok") || !strings.Contains(content, "[exit code 0]") {
		t.Fatalf("shell result = %q (shell %s)", content, resolveShell("").path)
	}
}

func TestLimitOutput(t *testing.T) {
	var b strings.Builder
	for i := 1; i <= 5000; i++ {
		fmt.Fprintf(&b, "line %d\n", i)
	}
	text := b.String()
	tail := limitOutput(text, "test", true)
	if !strings.HasPrefix(tail, "[Showing lines 3001-5000 of 5000") || !strings.HasSuffix(tail, "line 5000\n") {
		t.Fatalf("tail = %q…", tail[:120])
	}
	head := limitOutput(text, "test", false)
	if !strings.HasPrefix(head, "line 1\n") || !strings.Contains(head, "[Showing lines 1-2000 of 5000") {
		t.Fatalf("head ends %q", head[len(head)-200:])
	}
	path := tail[strings.Index(tail, "Full output: ")+len("Full output: ") : strings.Index(tail, "]")]
	saved, err := os.ReadFile(path)
	if err != nil || string(saved) != text {
		t.Fatalf("saved output mismatch: %v", err)
	}
	os.Remove(path)
	if short := limitOutput("ok\n", "test", true); short != "ok\n" {
		t.Fatalf("short = %q", short)
	}
}
