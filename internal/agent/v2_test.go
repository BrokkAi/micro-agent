package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	acp "github.com/BrokkAi/acp-go"
	schema1 "github.com/BrokkAi/acp-go/schema/unstable"
	schema2 "github.com/BrokkAi/acp-go/schema/v2"
	"github.com/BrokkAi/micro-agent/internal/config"
)

// v2Harness drives the agent over a draft-v2 connection through Serve, which
// picks the v2 runtime from the initialize frame.
type v2Harness struct {
	t      *testing.T
	agent  *Agent
	conn   *acp.Connection
	router *fakeRouter
	store  *config.Store
	dir    string

	mu       sync.Mutex
	updates  []schema2.UpdateSessionNotification
	asked    []json.RawMessage
	handlers map[string]func(json.RawMessage) (any, error)
}

func newV2Harness(t *testing.T) *v2Harness {
	t.Helper()
	h := &v2Harness{t: t, router: &fakeRouter{}, dir: t.TempDir(), handlers: map[string]func(json.RawMessage) (any, error){}}
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
		h.asked = append(h.asked, append(json.RawMessage(nil), raw...))
		handle := h.handlers[method]
		h.mu.Unlock()
		switch {
		case handle != nil:
			return handle(raw)
		case method == schema2.SessionRequestPermissionMethodName:
			return schema2.RequestPermissionResponse{Outcome: schema2.RequestPermissionOutcome{
				Selected: &schema2.SelectedPermissionOutcome{OptionID: "allow_once"},
			}}, nil
		}
		return nil, &acp.RPCError{Code: -32601, Message: method}
	}
	notifications := func(method string, raw json.RawMessage) error {
		if method == schema2.SessionUpdateMethodName {
			var n schema2.UpdateSessionNotification
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
	return h
}

func (h *v2Harness) call(method string, params any, result any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return h.conn.Call(ctx, method, params, result)
}

func (h *v2Harness) initialize() schema2.InitializeResponse {
	h.t.Helper()
	var response schema2.InitializeResponse
	err := h.call(schema2.InitializeMethodName, schema2.InitializeRequest{
		ProtocolVersion: 2,
		Info:            schema2.Implementation{Name: "test", Version: "1"},
		Capabilities:    &schema2.ClientCapabilities{Auth: &schema2.AuthCapabilities{Terminal: &schema2.TerminalAuthCapabilities{}}},
	}, &response)
	if err != nil {
		h.t.Fatal(err)
	}
	return response
}

func (h *v2Harness) newSession() schema2.SessionId {
	h.t.Helper()
	var created schema2.NewSessionResponse
	if err := h.call(schema2.SessionNewMethodName, schema2.NewSessionRequest{Cwd: schema2.AbsolutePath(h.dir)}, &created); err != nil {
		h.t.Fatal(err)
	}
	return created.SessionID
}

func (h *v2Harness) prompt(session schema2.SessionId, text string) schema2.PromptResponse {
	h.t.Helper()
	var response schema2.PromptResponse
	err := h.call(schema2.SessionPromptMethodName, schema2.PromptRequest{
		SessionID: session,
		Prompt:    []schema2.ContentBlock{{Text: &schema2.TextContent{Text: text}}},
	}, &response)
	if err != nil {
		h.t.Fatal(err)
	}
	return response
}

// waitIdle blocks until the session reports idle.
func (h *v2Harness) waitIdle() {
	h.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		idle := false
		for _, update := range h.updates {
			if update.Update.StateUpdate != nil && update.Update.StateUpdate.Idle != nil {
				idle = true
			}
		}
		h.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatal("the session never reported idle")
}

func (h *v2Harness) kinds() []schema2.SessionUpdateKind {
	h.mu.Lock()
	defer h.mu.Unlock()
	var kinds []schema2.SessionUpdateKind
	for _, update := range h.updates {
		kinds = append(kinds, update.Update.Kind)
	}
	return kinds
}

func TestV2InitializeAndPromptAcceptance(t *testing.T) {
	h := newV2Harness(t)
	init := h.initialize()
	if init.ProtocolVersion != 2 || init.Capabilities == nil || init.Capabilities.Session == nil {
		t.Fatalf("initialize = %+v", init)
	}
	if len(init.AuthMethods) != 2 {
		t.Fatalf("auth methods = %+v", init.AuthMethods)
	}
	if init.Capabilities.Session.Prompt == nil || init.Capabilities.Session.MCP == nil || init.Capabilities.Session.Delete == nil {
		t.Fatalf("session capabilities = %+v", init.Capabilities.Session)
	}
	session := h.newSession()
	h.router.responses = [][]string{textChunks("hello from v2")}
	response := h.prompt(session, "hi")
	if response.MessageID == "" {
		t.Fatal("prompt response carries no message ID")
	}
	h.waitIdle()

	var userMessage *schema2.UserMessage
	var chunks []string
	var messageIDs = map[schema2.MessageId]bool{}
	for _, update := range h.updates {
		switch {
		case update.Update.UserMessage != nil:
			userMessage = update.Update.UserMessage
		case update.Update.AgentMessageChunk != nil:
			chunks = append(chunks, update.Update.AgentMessageChunk.Content.Text.Text)
			messageIDs[update.Update.AgentMessageChunk.MessageID] = true
		}
	}
	if userMessage == nil || userMessage.MessageID != response.MessageID {
		t.Fatalf("user message = %+v, want ID %s", userMessage, response.MessageID)
	}
	if strings.Join(chunks, "") != "hello from v2" || len(messageIDs) != 1 {
		t.Fatalf("agent chunks = %q, ids %v", chunks, messageIDs)
	}
	// The model got the user message from the shared session history.
	messages := h.router.requests[0]["messages"].([]any)
	if last := messages[len(messages)-1].(map[string]any); last["role"] != "user" || last["content"] != "hi" {
		t.Fatalf("model messages = %v", messages)
	}
}

func TestV2ToolPermissionAndConfig(t *testing.T) {
	h := newV2Harness(t)
	h.initialize()
	session := h.newSession()
	h.router.responses = [][]string{
		toolCallChunks("call_1", "write_file", map[string]string{"path": "hello.txt", "content": "hi\n"}),
		textChunks("done"),
	}
	h.prompt(session, "make hello.txt")
	h.waitIdle()
	if data, err := os.ReadFile(filepath.Join(h.dir, "hello.txt")); err != nil || string(data) != "hi\n" {
		t.Fatalf("file = %q, %v", data, err)
	}
	h.mu.Lock()
	var permission *schema2.RequestPermissionRequest
	for _, raw := range h.asked {
		var request schema2.RequestPermissionRequest
		if json.Unmarshal(raw, &request) == nil && request.Subject != nil && request.Subject.ToolCall != nil &&
			string(request.Subject.ToolCall.ToolCall.ToolCallID) == "call_1" {
			permission = &request
		}
	}
	h.mu.Unlock()
	if permission == nil {
		t.Fatal("no v2 permission request")
	}
	if permission.Subject == nil || permission.Subject.ToolCall == nil || permission.Subject.ToolCall.ToolCall.ToolCallID != "call_1" {
		t.Fatalf("permission subject = %+v", permission.Subject)
	}
	if len(permission.Options) != 4 || permission.Title == "" {
		t.Fatalf("permission request = %+v", permission)
	}

	// Config options replace modes in draft v2.
	var set schema2.SetSessionConfigOptionResponse
	err := h.call(schema2.SessionSetConfigOptionMethodName, schema2.SetSessionConfigOptionRequest{
		SessionID: session,
		ConfigID:  "mode",
		ID:        &schema2.SetSessionConfigOptionRequestID{Value: "plan"},
	}, &set)
	if err != nil || len(set.ConfigOptions) == 0 {
		t.Fatalf("set_config_option = %+v, %v", set, err)
	}
	loaded, err := h.agent.lookup(schema1.SessionId(session))
	if err != nil {
		t.Fatal(err)
	}
	loaded.mu.Lock()
	defer loaded.mu.Unlock()
	if loaded.Mode != "plan" {
		t.Fatalf("mode = %s", loaded.Mode)
	}
}

func TestV2SessionLifecycle(t *testing.T) {
	h := newV2Harness(t)
	h.initialize()
	session := h.newSession()
	h.router.responses = [][]string{textChunks("first answer")}
	h.prompt(session, "first question")
	h.waitIdle()

	var list schema2.ListSessionsResponse
	if err := h.call(schema2.SessionListMethodName, schema2.ListSessionsRequest{}, &list); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, info := range list.Sessions {
		if info.SessionID == session {
			found = true
		}
	}
	if !found {
		t.Fatalf("session not listed: %+v", list.Sessions)
	}

	// Resume with replayFrom streams the stored conversation as v2 messages.
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	var resumed schema2.ResumeSessionResponse
	err := h.call(schema2.SessionResumeMethodName, schema2.ResumeSessionRequest{
		SessionID:  session,
		Cwd:        schema2.AbsolutePath(h.dir),
		ReplayFrom: &schema2.ReplayFrom{Start: &schema2.ReplayFromStart{}},
	}, &resumed)
	if err != nil || len(resumed.ConfigOptions) == 0 {
		t.Fatalf("resume = %+v, %v", resumed, err)
	}
	var replayedUser, replayedAgent bool
	for _, update := range h.updates {
		if update.Update.UserMessage != nil && update.Update.UserMessage.MessageID != "" {
			replayedUser = true
		}
		if update.Update.AgentMessageChunk != nil && update.Update.AgentMessageChunk.MessageID != "" {
			replayedAgent = true
		}
	}
	if !replayedUser || !replayedAgent {
		t.Fatalf("replay updates = %v", h.kinds())
	}

	var deleted schema2.DeleteSessionResponse
	if err := h.call(schema2.SessionDeleteMethodName, schema2.DeleteSessionRequest{SessionID: session}, &deleted); err != nil {
		t.Fatal(err)
	}
	var closed schema2.CloseSessionResponse
	if err := h.call(schema2.SessionCloseMethodName, schema2.CloseSessionRequest{SessionID: session}, &closed); err == nil {
		t.Fatal("closing a deleted session succeeded")
	}
}
