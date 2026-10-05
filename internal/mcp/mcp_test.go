package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if os.Getenv("MCP_FAKE_SERVER") == "1" {
		fakeStdioServer()
		return
	}
	os.Exit(m.Run())
}

// handle answers one request the way a tiny echo server would.
func handle(msg message) *message {
	if len(msg.ID) == 0 {
		return nil
	}
	reply := &message{JSONRPC: "2.0", ID: msg.ID}
	switch msg.Method {
	case "initialize":
		reply.Result = json.RawMessage(`{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"fake","version":"1"},"instructions":"be nice"}`)
	case "tools/list":
		reply.Result = json.RawMessage(`{"tools":[{"name":"echo","description":"Echo text","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]}`)
	case "tools/call":
		var params struct {
			Arguments struct {
				Text string `json:"text"`
			} `json:"arguments"`
		}
		_ = json.Unmarshal(msg.Params, &params)
		result, _ := json.Marshal(map[string]any{"content": []any{map[string]any{"type": "text", "text": "echo: " + params.Arguments.Text}}})
		reply.Result = result
	default:
		reply.Error = &RPCError{Code: -32601, Message: "nope"}
	}
	return reply
}

func fakeStdioServer() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var msg message
		if json.Unmarshal(scanner.Bytes(), &msg) != nil {
			continue
		}
		if reply := handle(msg); reply != nil {
			data, _ := json.Marshal(reply)
			fmt.Println(string(data))
		}
	}
}

func exercise(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if client.Instructions != "be nice" {
		t.Errorf("instructions = %q", client.Instructions)
	}
	tools, err := client.Tools(ctx)
	if err != nil || len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("tools = %+v, %v", tools, err)
	}
	result, err := client.CallTool(ctx, "echo", json.RawMessage(`{"text":"hi"}`))
	if err != nil || len(result.Content) != 1 || result.Content[0].Text != "echo: hi" {
		t.Fatalf("call = %+v, %v", result, err)
	}
}

func TestStdio(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := ConnectStdio(ctx, "fake", StdioOptions{Command: os.Args[0], Env: map[string]string{"MCP_FAKE_SERVER": "1"}}, ClientInfo{Name: "test", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	exercise(t, client)
}

func TestStreamableHTTP(t *testing.T) {
	for _, sse := range []bool{false, true} {
		t.Run(fmt.Sprintf("sse=%v", sse), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete {
					return
				}
				var msg message
				_ = json.NewDecoder(r.Body).Decode(&msg)
				if msg.Method != "initialize" && r.Header.Get("Mcp-Session-Id") != "abc" {
					http.Error(w, "missing session", http.StatusBadRequest)
					return
				}
				reply := handle(msg)
				if reply == nil {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				w.Header().Set("Mcp-Session-Id", "abc")
				data, _ := json.Marshal(reply)
				if sse {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(data)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := ConnectHTTP(ctx, "fake", HTTPOptions{URL: server.URL}, ClientInfo{Name: "test", Version: "1"})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			exercise(t, client)
		})
	}
}

// pipe is an in-memory transport: it plays the server for each message the
// client sends, answering through client.receive.
type pipe func(message)

func (p pipe) send(_ context.Context, msg message) error {
	go p(msg)
	return nil
}

func (pipe) close() error { return nil }

// elicitingClient connects to a fake server whose tools/call asks the client
// for input and returns the client's raw JSON-RPC reply as its text. It also
// returns the capabilities the client sent in initialize.
func elicitingClient(t *testing.T, info ClientInfo) (*Client, map[string]any) {
	t.Helper()
	client := newClient("fake", info)
	var capabilities map[string]any
	answers := make(chan message, 1)
	client.transport = pipe(func(msg message) {
		switch {
		case msg.Method == "initialize":
			var params struct {
				Capabilities map[string]any `json:"capabilities"`
			}
			_ = json.Unmarshal(msg.Params, &params)
			capabilities = params.Capabilities
			client.receive(*handle(msg))
		case msg.Method == "tools/call":
			client.receive(message{JSONRPC: "2.0", ID: json.RawMessage(`"e1"`), Method: "elicitation/create",
				Params: json.RawMessage(`{"message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}}}}`)})
			answer, _ := json.Marshal(<-answers)
			result, _ := json.Marshal(map[string]any{"content": []any{map[string]any{"type": "text", "text": string(answer)}}})
			client.receive(message{JSONRPC: "2.0", ID: msg.ID, Result: result})
		case msg.Method == "":
			answers <- msg
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.initialize(ctx, info.Name, info.Version); err != nil {
		t.Fatal(err)
	}
	return client, capabilities
}

// elicit runs one tool call against an elicitingClient and returns the
// client's reply to elicitation/create.
func elicit(ctx context.Context, t *testing.T, client *Client) message {
	t.Helper()
	result, err := client.CallTool(ctx, "ask", nil)
	if err != nil || len(result.Content) != 1 {
		t.Fatalf("call = %+v, %v", result, err)
	}
	var reply message
	if err := json.Unmarshal([]byte(result.Content[0].Text), &reply); err != nil || string(reply.ID) != `"e1"` {
		t.Fatalf("elicitation reply = %s, %v", result.Content[0].Text, err)
	}
	return reply
}

type callKey struct{}

func TestElicitation(t *testing.T) {
	for _, want := range []ElicitResult{
		{Action: "accept", Content: map[string]any{"name": "Ada"}},
		{Action: "decline"},
		{Action: "cancel"},
	} {
		t.Run(want.Action, func(t *testing.T) {
			hook := func(ctx context.Context, request ElicitRequest) (ElicitResult, error) {
				if ctx.Value(callKey{}) != "call-1" {
					t.Errorf("hook ctx lacks the CallTool value")
				}
				if request.Message != "Name?" || !strings.Contains(string(request.RequestedSchema), `"name"`) {
					t.Errorf("request = %+v", request)
				}
				return want, nil
			}
			client, capabilities := elicitingClient(t, ClientInfo{Name: "test", Version: "1", Elicit: hook})
			if _, ok := capabilities["elicitation"]; !ok {
				t.Errorf("capabilities = %v, want elicitation", capabilities)
			}
			ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), callKey{}, "call-1"), 5*time.Second)
			defer cancel()
			reply := elicit(ctx, t, client)
			var got ElicitResult
			if err := json.Unmarshal(reply.Result, &got); err != nil || reply.Error != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("reply = %s %v, want %+v", reply.Result, reply.Error, want)
			}
			client.mu.Lock()
			defer client.mu.Unlock()
			if len(client.calls) != 0 {
				t.Errorf("%d calls still tracked", len(client.calls))
			}
		})
	}
}

func TestElicitationWithoutHook(t *testing.T) {
	client, capabilities := elicitingClient(t, ClientInfo{Name: "test", Version: "1"})
	if _, ok := capabilities["elicitation"]; ok {
		t.Errorf("capabilities = %v, want no elicitation", capabilities)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if reply := elicit(ctx, t, client); reply.Error == nil || reply.Error.Code != -32601 {
		t.Errorf("reply = %+v, want -32601", reply)
	}
}

func TestElicitContext(t *testing.T) {
	client := newClient("fake", ClientInfo{})
	one := context.WithValue(context.Background(), callKey{}, "one")
	two := context.WithValue(context.Background(), callKey{}, "two")
	if client.elicitContext() != context.Background() {
		t.Error("no call in flight: want context.Background()")
	}
	client.calls[&one] = struct{}{}
	if client.elicitContext() != one {
		t.Error("one call in flight: want its ctx")
	}
	client.calls[&two] = struct{}{}
	if client.elicitContext() != context.Background() {
		t.Error("two calls in flight: want context.Background()")
	}
}

func TestCallResultMedia(t *testing.T) {
	var result CallResult
	err := json.Unmarshal([]byte(`{"content":[
		{"type":"image","data":"aW1n","mimeType":"image/png"},
		{"type":"audio","data":"YXVk","mimeType":"audio/wav"},
		{"type":"resource","resource":{"uri":"file:///a.txt","mimeType":"text/plain","text":"hi"}},
		{"type":"resource","resource":{"uri":"file:///a.bin","mimeType":"application/octet-stream","blob":"Ymlu"}},
		{"type":"resource_link","uri":"file:///b.md","name":"b.md","mimeType":"text/markdown"}]}`), &result)
	want := []Content{
		{Type: "image", Data: "aW1n", MimeType: "image/png"},
		{Type: "audio", Data: "YXVk", MimeType: "audio/wav"},
		{Type: "resource", Resource: &ResourceContents{URI: "file:///a.txt", MimeType: "text/plain", Text: "hi"}},
		{Type: "resource", Resource: &ResourceContents{URI: "file:///a.bin", MimeType: "application/octet-stream", Blob: "Ymlu"}},
		{Type: "resource_link", URI: "file:///b.md", Name: "b.md", MimeType: "text/markdown"},
	}
	if err != nil || !reflect.DeepEqual(result.Content, want) {
		t.Errorf("content = %+v, %v", result.Content, err)
	}
}
