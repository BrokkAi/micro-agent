package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BrokkAi/acp-go"
	schema "github.com/BrokkAi/acp-go/schema/unstable"
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
	t      *testing.T
	agent  *Agent
	conn   *acp.Connection
	init   schema.InitializeResponse
	router *fakeRouter
	store  *config.Store
	dir    string
	answer schema.PermissionOptionId

	mu       sync.Mutex
	updates  []schema.SessionNotification
	raw      []byte // everything the agent wrote, in order
	handlers map[string]func(json.RawMessage) (any, error)
}

// newHarness starts an agent and initializes it with caps.
func newHarness(t *testing.T, caps schema.ClientCapabilities) *harness {
	t.Helper()
	h := startHarness(t)
	var err error
	h.init, err = h.initialize(schema.InitializeRequest{
		ProtocolVersion:    schema.ProtocolVersion(acp.Version),
		ClientCapabilities: &caps,
		ClientInfo:         &schema.Implementation{Name: "test", Version: "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// startHarness starts an agent without initializing it.
func startHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, router: &fakeRouter{}, dir: t.TempDir(), answer: "allow_once", handlers: map[string]func(json.RawMessage) (any, error){}}
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
	h.store, h.agent = store, New(store, "test")

	agentIn, clientOut := io.Pipe()
	clientIn, agentOut := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = Serve(ctx, h.agent, agentIn, agentOut) }()

	handler := func(_ context.Context, method string, raw json.RawMessage) (any, error) {
		h.mu.Lock()
		handle := h.handlers[method]
		h.mu.Unlock()
		switch {
		case handle != nil:
			return handle(raw)
		case method == schema.SessionRequestPermissionMethodName:
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
	h.conn = acp.Connect(recorder{clientIn, h}, clientOut, handler, notifications)
	t.Cleanup(func() { h.conn.Close() })
	return h
}

// recorder keeps a copy of the agent's output for frame-order assertions.
type recorder struct {
	io.ReadCloser
	h *harness
}

func (r recorder) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.h.mu.Lock()
	r.h.raw = append(r.h.raw, p[:n]...)
	r.h.mu.Unlock()
	return n, err
}

// wireFrame is one JSON-RPC message as the client received it.
type wireFrame struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
}

func (h *harness) frames() []wireFrame {
	h.mu.Lock()
	defer h.mu.Unlock()
	var frames []wireFrame
	for _, line := range bytes.Split(bytes.TrimSpace(h.raw), []byte("\n")) {
		var f wireFrame
		if err := json.Unmarshal(line, &f); err != nil {
			h.t.Fatalf("frame %q: %v", line, err)
		}
		frames = append(frames, f)
	}
	return frames
}

// on answers the agent's requests for method with handle.
func (h *harness) on(method string, handle func(json.RawMessage) (any, error)) {
	h.mu.Lock()
	h.handlers[method] = handle
	h.mu.Unlock()
}

// send makes a raw call and decodes the result with unstable types.
func send[T any](h *harness, method string, params any) (T, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var result T
	err := h.conn.Call(ctx, method, params, &result)
	return result, err
}

// initialize sends a raw initialize, so tests can offer unstable client
// capabilities or other protocol versions.
func (h *harness) initialize(params any) (schema.InitializeResponse, error) {
	return send[schema.InitializeResponse](h, schema.InitializeMethodName, params)
}

func (h *harness) newSession() schema.SessionId {
	h.t.Helper()
	session, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, MCPServers: []schema.McpServer{}})
	if err != nil {
		h.t.Fatal(err)
	}
	return session.SessionID
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

func (h *harness) prompt(session schema.SessionId, text string) schema.PromptResponse {
	h.t.Helper()
	response, err := send[schema.PromptResponse](h, schema.SessionPromptMethodName, schema.PromptRequest{SessionID: session, Prompt: []schema.ContentBlock{textBlock(text)}})
	if err != nil {
		h.t.Fatal(err)
	}
	return response
}

func TestPromptRunsToolWithPermission(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "write_file", map[string]string{"path": "hello.txt", "content": "hi\n"}),
		textChunks("Created the file."),
	}
	session := h.newSession()
	if reason := h.prompt(session, "make hello.txt").StopReason; reason != schema.StopReasonEndTurn {
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
	list, err := send[schema.ListSessionsResponse](h, schema.SessionListMethodName, schema.ListSessionsRequest{Cwd: &h.dir})
	if err != nil || len(list.Sessions) != 1 || list.Sessions[0].Title == nil || *list.Sessions[0].Title != "make hello.txt" {
		t.Fatalf("list = %+v, %v", list, err)
	}
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
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
	session := h.newSession()
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
	session := h.newSession()
	h.prompt(session, "/config max_turns=7 reasoning_effort=high auto_compact=false")
	cfg := h.store.Get()
	if cfg.MaxTurns != 7 || cfg.ReasoningEffort != "high" || cfg.AutoCompactEnabled() {
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
		line int
		err  string
	}{
		{"a b a", editArgs{OldString: "b", NewString: "c"}, "a c a", 1, ""},
		{"a b a", editArgs{OldString: "a", NewString: "c"}, "", 0, "matches 2 times"},
		{"a b a", editArgs{OldString: "a", NewString: "c", ReplaceAll: true}, "c b c", 1, ""},
		{"p\nq a\na", editArgs{OldString: "a", NewString: "c", ReplaceAll: true}, "p\nq c\nc", 2, ""},
		{"x\r\ny\r\n", editArgs{OldString: "x\ny", NewString: "x\nz"}, "x\r\nz\r\n", 1, ""},
		{"w\r\nx\r\ny\r\n", editArgs{OldString: "x\ny", NewString: "z"}, "w\r\nz\r\n", 2, ""},
		{"abc", editArgs{OldString: "q", NewString: "r"}, "", 0, "not found"},
	}
	for _, c := range cases {
		got, line, err := applyEdit(c.text, c.args)
		if c.err != "" {
			if err == nil || !strings.Contains(err.Error(), c.err) {
				t.Errorf("applyEdit(%q, %+v) error = %v, want %q", c.text, c.args, err, c.err)
			}
			continue
		}
		if err != nil || got != c.want || line != c.line {
			t.Errorf("applyEdit(%q, %+v) = %q, %d, %v; want %q, %d", c.text, c.args, got, line, err, c.want, c.line)
		}
	}
}

func TestShellRunsLocally(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "shell", map[string]string{"command": "echo micro-agent-ok"}),
		textChunks("done"),
	}
	session := h.newSession()
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

func TestDeletedSessionIsNotResaved(t *testing.T) {
	store, err := config.Load(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(store, "test")
	s := &session{record: record{ID: newSessionID(), Cwd: t.TempDir()}}
	a.sessions[s.ID] = s
	if err := a.save(s); err != nil {
		t.Fatal(err)
	}
	if _, err := a.DeleteSession(context.Background(), schema.DeleteSessionRequest{SessionID: s.ID}); err != nil {
		t.Fatal(err)
	}
	if err := a.save(s); err != nil {
		t.Fatal(err)
	}
	if _, err := a.loadRecord(s.ID); err == nil {
		t.Fatal("deleted session was written back")
	}
}

func TestConfigKeepsSessionModel(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	session := h.newSession()
	h.prompt(session, "/model other/model")
	if err := h.store.Update(func(c *config.Config) error { c.Model = "global/model"; return nil }); err != nil {
		t.Fatal(err)
	}
	h.prompt(session, "/config max_turns=9")
	h.router.responses = [][]string{textChunks("hi")}
	h.prompt(session, "hello")
	if model := h.router.requests[0]["model"]; model != "other/model" {
		t.Fatalf("model = %v", model)
	}
}

func TestLogoutOverridesEnvKey(t *testing.T) {
	t.Setenv("OPENROUTER_API_KEY", "env-key")
	h := newHarness(t, schema.ClientCapabilities{})
	if _, err := send[schema.LogoutResponse](h, schema.LogoutMethodName, schema.LogoutRequest{}); err != nil {
		t.Fatal(err)
	}
	if key := h.store.Get().APIKey; key != "" {
		t.Fatalf("key after logout = %q", key)
	}
	h.prompt(h.newSession(), "/login new-key")
	if key := h.store.Get().APIKey; key != "new-key" {
		t.Fatalf("key after login = %q", key)
	}
	// Logging in again turns the environment fallback back on.
	if err := h.store.Update(func(c *config.Config) error { c.APIKey = ""; return nil }); err != nil {
		t.Fatal(err)
	}
	if key := h.store.Get().APIKey; key != "env-key" {
		t.Fatalf("key from the environment = %q", key)
	}
}

func TestSessionRootsMustBeAbsolute(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	extra := t.TempDir()
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, AdditionalDirectories: []string{extra}, MCPServers: []schema.McpServer{}})
	if err != nil {
		t.Fatal(err)
	}
	id := created.SessionID
	none := []schema.McpServer{}
	requests := map[string]struct {
		method string
		params any
	}{
		"new cwd":    {schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: "rel", MCPServers: none}},
		"new dir":    {schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, AdditionalDirectories: []string{"rel"}, MCPServers: none}},
		"load cwd":   {schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: id, Cwd: "rel", MCPServers: none}},
		"load dir":   {schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: id, Cwd: h.dir, AdditionalDirectories: []string{"rel"}, MCPServers: none}},
		"resume cwd": {schema.SessionResumeMethodName, schema.ResumeSessionRequest{SessionID: id, MCPServers: none}},
		"resume dir": {schema.SessionResumeMethodName, schema.ResumeSessionRequest{SessionID: id, Cwd: h.dir, AdditionalDirectories: []string{"rel"}, MCPServers: none}},
		"fork cwd":   {schema.SessionForkMethodName, schema.ForkSessionRequest{SessionID: id, MCPServers: none}},
		"fork dir":   {schema.SessionForkMethodName, schema.ForkSessionRequest{SessionID: id, Cwd: h.dir, AdditionalDirectories: []string{"rel"}, MCPServers: none}},
	}
	for name, r := range requests {
		if _, err := send[json.RawMessage](h, r.method, r.params); rpcCode(err) != -32602 {
			t.Errorf("%s: %v", name, err)
		}
	}

	dirs := func() []string {
		s, err := h.agent.lookup(id)
		if err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.Dirs
	}
	h.router.responses = [][]string{textChunks("hi")}
	h.prompt(id, "hello") // saves the session with its directory
	resume := schema.ResumeSessionRequest{SessionID: id, Cwd: h.dir, AdditionalDirectories: []string{extra}, MCPServers: none}
	if _, err := send[schema.ResumeSessionResponse](h, schema.SessionResumeMethodName, resume); err != nil || !slices.Equal(dirs(), []string{extra}) {
		t.Fatalf("resume with a directory: %v, dirs %v", err, dirs())
	}
	// Omitted additional directories mean none, not the saved ones.
	load := schema.LoadSessionRequest{SessionID: id, Cwd: h.dir, MCPServers: none}
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, load); err != nil || len(dirs()) != 0 {
		t.Fatalf("load without directories: %v, dirs %v", err, dirs())
	}
}

func TestForkCopiesHistoryAndLeavesSource(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{textChunks("first answer")}
	source := h.newSession()
	h.prompt(source, "first question")

	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	fork, err := send[schema.ForkSessionResponse](h, schema.SessionForkMethodName, schema.ForkSessionRequest{
		SessionID: source, Cwd: h.dir, MCPServers: []schema.McpServer{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if fork.SessionID == source || fork.Modes == nil || len(fork.ConfigOptions) == 0 {
		t.Fatalf("fork = %+v", fork)
	}
	// A later call pushes the frame order, so the queued updates are in.
	if _, err := send[schema.ListSessionsResponse](h, schema.SessionListMethodName, schema.ListSessionsRequest{}); err != nil {
		t.Fatal(err)
	}
	if kinds := h.kinds(); slices.Contains(kinds, schema.SessionUpdateKindUserMessageChunk) || slices.Contains(kinds, schema.SessionUpdateKindAgentMessageChunk) {
		t.Fatalf("fork replayed history: %v", kinds)
	}

	h.router.responses = [][]string{textChunks("forked answer")}
	if reason := h.prompt(fork.SessionID, "second question").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	forked := fmt.Sprint(h.router.requests[1]["messages"])
	if !strings.Contains(forked, "first question") || !strings.Contains(forked, "first answer") {
		t.Fatalf("fork history = %v", forked)
	}

	h.router.responses = [][]string{textChunks("source answer")}
	h.prompt(source, "third question")
	sourceMessages := fmt.Sprint(h.router.requests[2]["messages"])
	if strings.Contains(sourceMessages, "second question") || strings.Contains(sourceMessages, "forked answer") {
		t.Fatalf("source saw fork messages: %v", sourceMessages)
	}
}

func TestModelOptionsGroupByVendor(t *testing.T) {
	if options, ok := modelOptions([]string{"a/x", "a/y"}).([]schema.SessionConfigSelectOption); !ok || len(options) != 2 {
		t.Fatalf("one vendor = %+v", options)
	}
	h := newHarness(t, schema.ClientCapabilities{})
	if err := h.store.Update(func(c *config.Config) error {
		c.Models = []string{"openai/gpt-5", "anthropic/claude", "plain", "test/other"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// model returns the model option's value and its groups, as
	// "group(name):value,... ...".
	model := func(options []schema.SessionConfigOption) (string, string) {
		t.Helper()
		for _, option := range options {
			if option.ID != "model" {
				continue
			}
			data, _ := json.Marshal(option.Select.Options)
			var groups []schema.SessionConfigSelectGroup
			if err := json.Unmarshal(data, &groups); err != nil {
				t.Fatalf("model options %s: %v", data, err)
			}
			var parts []string
			for _, g := range groups {
				var values []string
				for _, o := range g.Options {
					values = append(values, string(o.Value))
				}
				parts = append(parts, fmt.Sprintf("%s(%s):%s", g.Group, g.Name, strings.Join(values, ",")))
			}
			return string(option.Select.CurrentValue), strings.Join(parts, " ")
		}
		t.Fatal("no model option")
		return "", ""
	}
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, MCPServers: []schema.McpServer{}})
	if err != nil {
		t.Fatal(err)
	}
	current, groups := model(created.ConfigOptions)
	if want := "test(test):test/model,test/other anthropic(anthropic):anthropic/claude openai(openai):openai/gpt-5 other(other):plain"; current != "test/model" || groups != want {
		t.Fatalf("model %s in %s, want %s", current, groups, want)
	}
	set := schema.SetSessionConfigOptionRequest{SessionID: created.SessionID, ConfigID: "model", ValueID: &schema.SetSessionConfigOptionRequestValueID{Value: "new/model"}}
	response, err := send[schema.SetSessionConfigOptionResponse](h, schema.SessionSetConfigOptionMethodName, set)
	if err != nil {
		t.Fatal(err)
	}
	if current, groups = model(response.ConfigOptions); current != "new/model" || !strings.HasPrefix(groups, "new(new):new/model ") {
		t.Fatalf("after set: model %s in %s", current, groups)
	}
}
