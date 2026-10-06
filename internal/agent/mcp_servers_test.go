package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	acpmcp "github.com/BrokkAi/acp-go/mcp"
	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

// fakeMCPSSEServer serves one legacy HTTP+SSE MCP server with an echo tool.
func fakeMCPSSEServer(t *testing.T) string {
	t.Helper()
	outbound := make(chan string, 16)
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: endpoint\ndata: messages\n\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case event := <-outbound:
				fmt.Fprint(w, event)
				w.(http.Flusher).Flush()
			case <-done:
				return
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("POST /messages", func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&msg)
		w.WriteHeader(http.StatusAccepted)
		if len(msg.ID) == 0 {
			return
		}
		var result string
		switch msg.Method {
		case "initialize":
			result = `{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"sse","version":"1"}}`
		case "tools/list":
			result = `{"tools":[{"name":"echo","description":"Echo text","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]}`
		case "tools/call":
			result = `{"content":[{"type":"text","text":"sse says hi"}]}`
		default:
			outbound <- fmt.Sprintf("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%s,\"error\":{\"code\":-32601,\"message\":\"no\"}}\n\n", msg.ID)
			return
		}
		outbound <- fmt.Sprintf("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":%s,\"result\":%s}\n\n", msg.ID, result)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(func() {
		close(done)
		server.Close()
	})
	return server.URL
}

func TestSessionConnectsClientSSEServers(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	mcp := h.init.AgentCapabilities.MCPCapabilities
	if mcp == nil || !isTrue(mcp.SSE) || !isTrue(mcp.ACP) || !isTrue(mcp.HTTP) {
		t.Fatalf("mcp capabilities = %+v", mcp)
	}
	url := fakeMCPSSEServer(t)
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{
		Cwd:        h.dir,
		MCPServers: []schema.McpServer{{SSE: &schema.McpServerSse{Name: "sse", URL: url + "/sse", Headers: []schema.HttpHeader{}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{
		toolCallChunks("call_1", mcpToolName("sse", "echo"), map[string]string{"text": "hi"}),
		textChunks("done"),
	}
	if reason := h.prompt(created.SessionID, "echo hi").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	last := h.router.requests[1]["messages"].([]any)
	result := last[len(last)-1].(map[string]any)
	if result["role"] != "tool" || !strings.Contains(result["content"].(string), "sse says hi") {
		t.Fatalf("tool result = %v", result)
	}
}

func TestSessionConnectsClientACPServers(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	router := acpmcp.NewMessageRouter(h.conn)
	var mu sync.Mutex
	var methods []string
	err := router.Register("tools", acpmcp.MessageServiceFunc(func(_ context.Context, request acpmcp.MessageRequest) (acpmcp.MessageOutcome, error) {
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		switch request.Method {
		case "tools/list":
			return acpmcp.MessageOutcome{Result: json.RawMessage(`{"tools":[{"name":"echo","description":"Echo text","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]}`)}, nil
		case "tools/call":
			return acpmcp.MessageOutcome{Result: json.RawMessage(`{"content":[{"type":"text","text":"acp says hi"}]}`)}, nil
		}
		return acpmcp.MessageOutcome{Error: &schema.McpError{Code: -32601, Message: "no " + request.Method}}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	h.on(schema.McpMessageMethodName, func(raw json.RawMessage) (any, error) {
		return router.Handle(nil)(context.Background(), schema.McpMessageMethodName, raw)
	})
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{
		Cwd:        h.dir,
		MCPServers: []schema.McpServer{{ACP: &schema.McpServerAcp{Name: "tools", ServerID: "tools"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{
		toolCallChunks("call_1", mcpToolName("tools", "echo"), map[string]string{"text": "hi"}),
		textChunks("done"),
	}
	if reason := h.prompt(created.SessionID, "echo hi").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 2 || methods[0] != "tools/list" || methods[1] != "tools/call" {
		t.Fatalf("mcp/message methods = %v", methods)
	}
	last := h.router.requests[1]["messages"].([]any)
	result := last[len(last)-1].(map[string]any)
	if result["role"] != "tool" || !strings.Contains(result["content"].(string), "acp says hi") {
		t.Fatalf("tool result = %v", result)
	}
}
