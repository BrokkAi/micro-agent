package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/BrokkAi/acp-go"
	acpmcp "github.com/BrokkAi/acp-go/mcp"
	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/config"
)

func rpcCode(err error) int {
	var rpc *acp.RPCError
	if errors.As(err, &rpc) {
		return rpc.Code
	}
	return 0
}

func TestInitializeNegotiatesVersion(t *testing.T) {
	for name, params := range map[string]string{
		"v1 shape": `{"protocolVersion":2,"clientCapabilities":{"auth":{"terminal":true}},"clientInfo":{"name":"test","version":"1"}}`,
		"v2 shape": `{"protocolVersion":2,"capabilities":{"auth":{"terminal":{}}},"info":{"name":"test","version":"1"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := startHarness(t)
			init, err := h.initialize(json.RawMessage(params))
			if err != nil {
				t.Fatal(err)
			}
			if init.ProtocolVersion != 1 {
				t.Fatalf("protocol version = %d", init.ProtocolVersion)
			}
			if !slices.ContainsFunc(init.AuthMethods, func(m schema.AuthMethod) bool { return m.Terminal != nil }) {
				t.Fatalf("terminal capability lost: %+v", init.AuthMethods)
			}
			if _, err := h.initialize(json.RawMessage(params)); rpcCode(err) != -32600 {
				t.Fatalf("second initialize: %v", err)
			}
		})
	}

	h := startHarness(t)
	if _, err := h.initialize(json.RawMessage(`{"protocolVersion":0}`)); rpcCode(err) != -32600 {
		t.Fatalf("version 0: %v", err)
	}
	if _, err := h.initialize(json.RawMessage(`{"clientInfo":{"name":"test","version":"1"}}`)); rpcCode(err) != -32602 {
		t.Fatalf("no version: %v", err)
	}
	if _, err := h.initialize(json.RawMessage(`{"protocolVersion":1}`)); err != nil {
		t.Fatalf("initialize after failures: %v", err)
	}
}

func TestDecodeInitializeIsLenient(t *testing.T) {
	v2, err := decodeInitialize(json.RawMessage(`{"protocolVersion":3,"capabilities":{"elicitation":{"form":{}},"auth":{"terminal":{}}},"info":{"name":"test","version":"1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	c := v2.ClientCapabilities
	if c == nil || c.Elicitation.Form == nil || c.Elicitation.URL != nil || !isTrue(c.Auth.Terminal) || c.Session.ConfigOptions.Boolean == nil {
		t.Fatalf("v2 capabilities = %+v", c)
	}
	// A capability of the wrong shape is dropped, not fatal.
	v1, err := decodeInitialize(json.RawMessage(`{"protocolVersion":1,"clientCapabilities":{"fs":5,"terminal":true}}`))
	if err != nil || v1.ClientCapabilities == nil || !isTrue(v1.ClientCapabilities.Terminal) {
		t.Fatalf("v1 = %+v, %v", v1.ClientCapabilities, err)
	}
}

func TestMethodGating(t *testing.T) {
	h := startHarness(t)
	for _, method := range []string{schema.SessionNewMethodName, schema.SessionForkMethodName, schema.ProvidersListMethodName} {
		if _, err := send[json.RawMessage](h, method, map[string]any{}); rpcCode(err) != -32600 {
			t.Errorf("%s before initialize: %v", method, err)
		}
	}
	var err error
	if h.init, err = h.initialize(schema.InitializeRequest{ProtocolVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if h.init.AgentCapabilities.SessionCapabilities.Fork != nil || h.init.AgentCapabilities.Providers != nil {
		t.Fatal("fork or providers advertised")
	}
	fork := schema.ForkSessionRequest{SessionID: "abc", Cwd: h.dir, MCPServers: []schema.McpServer{}}
	if _, err := send[json.RawMessage](h, schema.SessionForkMethodName, fork); rpcCode(err) != -32601 {
		t.Errorf("session/fork: %v", err)
	}
	if _, err := send[json.RawMessage](h, schema.ProvidersListMethodName, schema.ListProvidersRequest{}); rpcCode(err) != -32601 {
		t.Errorf("providers/list: %v", err)
	}
	if _, err := send[json.RawMessage](h, schema.SessionPromptMethodName, map[string]any{"sessionId": 5}); rpcCode(err) != -32602 {
		t.Errorf("bad params: %v", err)
	}
	if _, err := send[json.RawMessage](h, "_example/unknown", map[string]any{}); rpcCode(err) != -32601 {
		t.Errorf("unknown method: %v", err)
	}
}

func TestGuardGatesOptionalMethods(t *testing.T) {
	store, err := config.Load(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	a := New(store, "test")
	var reached []string
	guarded := a.guard(func(_ context.Context, method string, _ json.RawMessage) (any, error) {
		reached = append(reached, method)
		return "ok", nil
	})
	full := &schema.AgentCapabilities{
		LoadSession: ptr(true),
		Auth:        &schema.AgentAuthCapabilities{Logout: &schema.LogoutCapabilities{}},
		Providers:   &schema.ProvidersCapabilities{},
		SessionCapabilities: &schema.SessionCapabilities{
			Close: &schema.SessionCloseCapabilities{}, Delete: &schema.SessionDeleteCapabilities{}, Fork: &schema.SessionForkCapabilities{},
			List: &schema.SessionListCapabilities{}, Resume: &schema.SessionResumeCapabilities{},
		},
	}
	optional := []string{
		schema.SessionLoadMethodName, schema.SessionResumeMethodName, schema.SessionCloseMethodName, schema.SessionListMethodName,
		schema.SessionDeleteMethodName, schema.SessionForkMethodName, schema.LogoutMethodName,
		schema.ProvidersListMethodName, schema.ProvidersSetMethodName, schema.ProvidersDisableMethodName,
	}
	ctx := context.Background()
	for _, method := range optional {
		a.offered = &schema.AgentCapabilities{}
		if _, err := guarded(ctx, method, nil); rpcCode(err) != -32601 {
			t.Errorf("%s not offered: %v", method, err)
		}
		a.offered = full
		if _, err := guarded(ctx, method, nil); err != nil {
			t.Errorf("%s offered: %v", method, err)
		}
	}
	a.offered = &schema.AgentCapabilities{}
	if result, err := guarded(ctx, schema.SessionPromptMethodName, nil); err != nil || result != "ok" {
		t.Errorf("session/prompt = %v, %v", result, err)
	}
	if want := append(optional, schema.SessionPromptMethodName); !slices.Equal(reached, want) {
		t.Errorf("reached %v, want %v", reached, want)
	}
}

func TestClientGatesUnadvertisedMethods(t *testing.T) {
	store, err := config.Load(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	// No connection: a call that is not gated would panic.
	c := &clientConn{a: New(store, "test")}
	ctx := context.Background()
	form := &schema.ElicitationFormMode{Session: &schema.ElicitationSessionScope{SessionID: "s"}}
	url := &schema.ElicitationUrlMode{ElicitationID: "e", URL: "https://example.com", Session: &schema.ElicitationSessionScope{SessionID: "s"}}
	calls := map[string]func() error{
		"read":  func() error { _, err := c.ReadTextFile(ctx, schema.ReadTextFileRequest{}); return err },
		"write": func() error { _, err := c.WriteTextFile(ctx, schema.WriteTextFileRequest{}); return err },
		"form": func() error {
			_, err := c.CreateElicitation(ctx, schema.CreateElicitationRequest{Form: form})
			return err
		},
		"url": func() error {
			_, err := c.CreateElicitation(ctx, schema.CreateElicitationRequest{URL: url})
			return err
		},
		"complete": func() error { return c.CompleteElicitation(ctx, "e") },
		"create":   func() error { _, err := c.CreateTerminal(ctx, schema.CreateTerminalRequest{}); return err },
		"output":   func() error { _, err := c.TerminalOutput(ctx, schema.TerminalOutputRequest{}); return err },
		"wait":     func() error { _, err := c.WaitForTerminalExit(ctx, schema.WaitForTerminalExitRequest{}); return err },
		"kill":     func() error { _, err := c.KillTerminal(ctx, schema.KillTerminalRequest{}); return err },
		"release":  func() error { _, err := c.ReleaseTerminal(ctx, schema.ReleaseTerminalRequest{}); return err },
	}
	for name, call := range calls {
		if err := call(); rpcCode(err) != -32601 {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestConfigFormUsesTypedElicitation(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Elicitation: &schema.ElicitationCapabilities{Form: &schema.ElicitationFormCapabilities{}}})
	var params json.RawMessage
	var content map[string]schema.ElicitationContentValue
	h.on(schema.ElicitationCreateMethodName, func(raw json.RawMessage) (any, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		params = raw
		return schema.CreateElicitationResponse{Accept: &schema.ElicitationAcceptAction{Content: content}}, nil
	})
	reply := func(c map[string]schema.ElicitationContentValue) {
		h.mu.Lock()
		content = c
		h.mu.Unlock()
	}
	session := h.newSession()
	// The form offers a valid mode in place of one it cannot show.
	if err := h.store.Update(func(c *config.Config) error { c.DefaultMode = "bogus"; return nil }); err != nil {
		t.Fatal(err)
	}
	unchanged := h.store.Get()

	// Accepting with nothing filled in saves nothing.
	reply(map[string]schema.ElicitationContentValue{})
	h.prompt(session, "/config")
	if cfg := h.store.Get(); !reflect.DeepEqual(cfg, unchanged) {
		t.Fatalf("empty form saved %+v", cfg)
	}

	var wire struct {
		Mode            string           `json:"mode"`
		SessionID       schema.SessionId `json:"sessionId"`
		RequestedSchema struct {
			Properties map[string]struct {
				Default any                 `json:"default"`
				OneOf   []schema.EnumOption `json:"oneOf"`
			} `json:"properties"`
		} `json:"requestedSchema"`
	}
	h.mu.Lock()
	err := json.Unmarshal(params, &wire)
	h.mu.Unlock()
	if err != nil || wire.Mode != "form" || wire.SessionID != session || len(wire.RequestedSchema.Properties) == 0 {
		t.Fatalf("elicitation/create = %s (%v)", params, err)
	}
	mode := wire.RequestedSchema.Properties["default_mode"]
	if !slices.ContainsFunc(mode.OneOf, func(o schema.EnumOption) bool { return o.Const == "plan" && o.Title == "Plan" }) {
		t.Errorf("default_mode options = %+v", mode.OneOf)
	}
	if mode.Default != "default" || !slices.ContainsFunc(mode.OneOf, func(o schema.EnumOption) bool { return o.Const == mode.Default }) {
		t.Errorf("default_mode default = %v, not one of %+v", mode.Default, mode.OneOf)
	}
	if efforts := wire.RequestedSchema.Properties["reasoning_effort"].OneOf; len(efforts) == 0 || efforts[0].Title != "Model default" {
		t.Errorf("reasoning_effort options = %+v", efforts)
	}

	// Sending back the values the form showed saves nothing either.
	shown := map[string]schema.ElicitationContentValue{}
	for key, property := range wire.RequestedSchema.Properties {
		if property.Default != nil {
			shown[key] = property.Default
		}
	}
	reply(shown)
	h.prompt(session, "/config")
	if cfg := h.store.Get(); !reflect.DeepEqual(cfg, unchanged) {
		t.Fatalf("unchanged form saved %+v", cfg)
	}

	reply(map[string]schema.ElicitationContentValue{"max_turns": 5, "default_mode": "plan"})
	h.prompt(session, "/config")
	if cfg := h.store.Get(); cfg.MaxTurns != 5 || cfg.DefaultMode != "plan" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestResumeAnnouncesBeforeResponding(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{textChunks("hi")}
	session := h.newSession()
	h.prompt(session, "hello")
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	broken := schema.McpServer{Stdio: &schema.McpServerStdio{Name: "broken", Command: filepath.Join(h.dir, "missing"), Args: []string{}, Env: []schema.EnvVariable{}}}
	resume := schema.ResumeSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{broken}}
	if _, err := send[schema.ResumeSessionResponse](h, schema.SessionResumeMethodName, resume); err != nil {
		t.Fatal(err)
	}
	want := []schema.SessionUpdateKind{schema.SessionUpdateKindAvailableCommandsUpdate, schema.SessionUpdateKindSessionInfoUpdate, schema.SessionUpdateKindAgentMessageChunk}
	if kinds := h.kinds(); !slices.Equal(kinds, want) {
		t.Fatalf("updates before the response = %v, want %v", kinds, want)
	}
	h.mu.Lock()
	notice := h.updates[2].Update.AgentMessageChunk.Content.Text.Text
	h.mu.Unlock()
	if !strings.Contains(notice, "`broken` failed to start") {
		t.Fatalf("notice = %q", notice)
	}
}

func TestMCPMessageNotificationsReachTheCall(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.on(schema.McpMessageMethodName, func(raw json.RawMessage) (any, error) {
		var request schema.MessageMcpRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		progress := schema.MessageMcpNotification{ServerID: request.ServerID, RequestID: request.RequestID, Method: "notifications/progress", Params: map[string]any{"progress": 1}}
		if err := h.conn.Notify(context.Background(), schema.McpMessageMethodName, progress); err != nil {
			return nil, err
		}
		return schema.MessageMcpResponse{Result: &schema.MessageMcpResponseResult{Result: json.RawMessage(`{"tools":[]}`)}}, nil
	})
	notes := make(chan acpmcp.MessageNotification, 1)
	outcome, err := h.agent.client.messages.Call(context.Background(), "tools", "r1", "tools/list", nil, func(n acpmcp.MessageNotification) error {
		notes <- n
		return nil
	})
	if err != nil || string(outcome.Result) != `{"tools":[]}` {
		t.Fatalf("outcome = %+v, %v", outcome, err)
	}
	select {
	case n := <-notes:
		if n.ServerID != "tools" || n.RequestID != "r1" || n.Method != "notifications/progress" {
			t.Fatalf("notification = %+v", n)
		}
	default:
		t.Fatal("notification not delivered before the call returned")
	}
}

func TestNewSessionUpdatesFollowResponse(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	broken := schema.McpServer{Stdio: &schema.McpServerStdio{Name: "broken", Command: filepath.Join(h.dir, "missing"), Args: []string{}, Env: []schema.EnvVariable{}}}
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, MCPServers: []schema.McpServer{broken}})
	if err != nil {
		t.Fatal(err)
	}
	// Frames arrive in order, so a later response means the updates are in.
	if _, err := send[schema.ListSessionsResponse](h, schema.SessionListMethodName, schema.ListSessionsRequest{}); err != nil {
		t.Fatal(err)
	}
	answered := false
	var kinds []schema.SessionUpdateKind
	for _, f := range h.frames() {
		var result struct {
			SessionID schema.SessionId `json:"sessionId"`
		}
		var update schema.SessionNotification
		switch {
		case f.Result != nil && json.Unmarshal(f.Result, &result) == nil && result.SessionID == created.SessionID:
			answered = true
		case f.Method == schema.SessionUpdateMethodName:
			if err := json.Unmarshal(f.Params, &update); err != nil {
				t.Fatal(err)
			}
			if !answered {
				t.Fatalf("%s update before the session/new response", update.Update.Kind)
			}
			kinds = append(kinds, update.Update.Kind)
		}
	}
	want := []schema.SessionUpdateKind{schema.SessionUpdateKindAvailableCommandsUpdate, schema.SessionUpdateKindAgentMessageChunk}
	if !slices.Equal(kinds, want) {
		t.Fatalf("updates = %v, want %v", kinds, want)
	}
}

func TestOversizePromptKeepsConnection(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	session := h.newSession()
	big := schema.PromptRequest{SessionID: session, Prompt: []schema.ContentBlock{textBlock(strings.Repeat("x", 9<<20))}}
	_, err := send[schema.PromptResponse](h, schema.SessionPromptMethodName, big)
	if rpcCode(err) != -32600 || !strings.Contains(err.Error(), "8 MiB") {
		t.Fatalf("9 MiB prompt: %v", err)
	}
	h.router.responses = [][]string{textChunks("still here")}
	if reason := h.prompt(session, "hello").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
}

func TestOversizeClientReplyFailsTheCall(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Fs: &schema.FileSystemCapabilities{ReadTextFile: ptr(true)}})
	h.on(schema.FsReadTextFileMethodName, func(json.RawMessage) (any, error) {
		return schema.ReadTextFileResponse{Content: strings.Repeat("x", 9<<20)}, nil
	})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "read_file", map[string]string{"path": "big.txt"}),
		textChunks("done"),
	}
	session := h.newSession()
	if reason := h.prompt(session, "read it").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	messages := h.router.requests[1]["messages"].([]any)
	if content := messages[len(messages)-1].(map[string]any)["content"].(string); !strings.Contains(content, "8 MiB") {
		t.Fatalf("tool result = %q", content)
	}
}

func TestCancelStopsPrompt(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{toolCallChunks("call_1", "shell", map[string]string{"command": "echo hi > out.txt"})}
	session := h.newSession()
	h.on(schema.SessionRequestPermissionMethodName, func(json.RawMessage) (any, error) {
		// The cancel is on the wire ahead of this answer.
		if err := h.conn.Notify(context.Background(), schema.SessionCancelMethodName, schema.CancelNotification{SessionID: session}); err != nil {
			return nil, err
		}
		return schema.RequestPermissionResponse{Outcome: schema.RequestPermissionOutcome{Selected: &schema.SelectedPermissionOutcome{OptionID: "allow_once"}}}, nil
	})
	if reason := h.prompt(session, "run it").StopReason; reason != schema.StopReasonCancelled {
		t.Fatalf("stop reason %s", reason)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "out.txt")); err == nil {
		t.Fatal("command ran after cancel")
	}
}

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

func TestFrameWriterFlushesAfterResponse(t *testing.T) {
	var out bytes.Buffer
	w := &frameWriter{out: nopWriteCloser{&out}, queued: map[schema.SessionId][][]byte{}}
	w.hold("s1", []byte("u1\n"))
	w.hold("s2", []byte("u2\n"))
	frames := []string{
		`{"jsonrpc":"2.0","method":"x","params":{"sessionId":"s1"}}` + "\n",
		`[{"jsonrpc":"2.0","id":1,"result":{"sessionId":"s1"}}]` + "\n",
		`{"jsonrpc":"2.0","id":2,"result":{"sessionId":"s2"}}` + "\n",
	}
	for _, frame := range frames {
		if n, err := w.Write([]byte(frame)); err != nil || n != len(frame) {
			t.Fatalf("Write = %d, %v", n, err)
		}
	}
	if want := frames[0] + frames[1] + "u1\n" + frames[2] + "u2\n"; out.String() != want {
		t.Fatalf("wrote %q, want %q", out.String(), want)
	}
	if len(w.queued) != 0 {
		t.Fatalf("still queued: %v", w.queued)
	}
}

func TestFrameReaderDropsOversizeFrames(t *testing.T) {
	pad := func(prefix, suffix string, size int) string {
		return prefix + strings.Repeat("x", size-len(prefix)-len(suffix)) + suffix + "\n"
	}
	fits := pad(`{"jsonrpc":"2.0","method":"fits","params":"`, `"}`, maxFrame-1)
	input := pad(`{"jsonrpc":"2.0","method":"note","params":"`, `"}`, maxFrame) +
		pad(`{"jsonrpc":"2.0","id":7,"method":"session/prompt","params":"`, `"}`, maxFrame) +
		pad(`{"jsonrpc":"2.0","id":"r1","result":"`, `"}`, maxFrame+1) +
		// The id comes after the cut, past nested and quoted look-alikes.
		pad(`{"jsonrpc":"2.0","result":{"id":99,"text":"\"},\"id\":98,`, `"},"id":3}`, maxFrame) +
		pad(`[{"jsonrpc":"2.0","id":4,"method":"session/prompt","params":{"id":97,"text":"`,
			`"}},{"jsonrpc":"2.0","method":"note"},{"jsonrpc":"2.0","result":{},"id":"r2"},["junk"],`+
				`{"jsonrpc":"2.0","error":{"code":1,"message":"m"},"id":"r3"},{"jsonrpc":"2.0","method":"x","id":5}]`, maxFrame) +
		fits
	ready := make(chan struct{})
	close(ready)
	var out bytes.Buffer
	in := io.NopCloser(strings.NewReader(input))
	r := newFrameReader(in, &out, ready)

	// Scan the result the way acp.Connect does.
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), 8<<20)
	var lines []string
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	dropped := func(id string) string {
		return `{"jsonrpc":"2.0","id":` + id + `,"error":{"code":-32600,"message":"ACP frame over the 8 MiB limit dropped"}}`
	}
	want := []string{dropped(`"r1"`), dropped("3"), dropped(`"r2"`), dropped(`"r3"`)}
	if len(lines) != 5 || !slices.Equal(lines[:4], want) || lines[4]+"\n" != fits {
		t.Fatalf("passed %d lines, first %.120q", len(lines), lines)
	}
	if want := dropped("7") + "\n[" + dropped("4") + "," + dropped("5") + "]\n"; out.String() != want {
		t.Fatalf("answered %q, want %q", out.String(), want)
	}
}
