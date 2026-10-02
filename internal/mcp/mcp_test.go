package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
