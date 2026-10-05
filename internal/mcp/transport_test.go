package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSSE(t *testing.T) {
	outbound := make(chan string, 16)
	answers := make(chan message, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /mcp/sse", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" || r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "bad stream request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		// The early ping comes before the endpoint, so the client must drop it.
		fmt.Fprint(w, ": hello\ndata: {\"jsonrpc\":\"2.0\",\"id\":\"early\",\"method\":\"ping\"}\n\nevent: endpoint\ndata: messages?session=1\n\n")
		w.(http.Flusher).Flush()
		for {
			select {
			case event := <-outbound:
				fmt.Fprint(w, event)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	})
	mux.HandleFunc("POST /mcp/messages", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("session") != "1" || r.Header.Get("Authorization") != "Bearer k" {
			http.Error(w, "unknown session", http.StatusNotFound)
			return
		}
		var msg message
		_ = json.NewDecoder(r.Body).Decode(&msg)
		w.WriteHeader(http.StatusAccepted)
		switch {
		case msg.Method == "notifications/initialized":
			outbound <- "data: {\"jsonrpc\":\"2.0\",\"id\":\"srv-1\",\"method\":\"ping\"}\n\n" // unnamed event
		case msg.Method == "":
			answers <- msg
		default:
			if reply := handle(msg); reply != nil {
				data, _ := json.Marshal(reply)
				outbound <- fmt.Sprintf("event: message\ndata: %s\n\n", data)
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	client, err := Connect(ctx, Config{Name: "fake", SSE: &SSEOptions{URL: server.URL + "/mcp/sse", Headers: map[string]string{"Authorization": "Bearer k"}}}, ClientInfo{Name: "test", Version: "1"})
	cancel() // like connectMCP: the stream must outlive the connect ctx
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	exercise(t, client)
	select {
	case msg := <-answers:
		if string(msg.ID) != `"srv-1"` || string(msg.Result) != `{}` {
			t.Errorf("first ping reply = %+v, want srv-1", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server ping was not answered")
	}
}

func TestSSEStreamEndsBeforeEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: message\ndata: {}\n\n")
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := ConnectSSE(ctx, "fake", SSEOptions{URL: server.URL}, ClientInfo{}); err == nil || !strings.Contains(err.Error(), "before naming an endpoint") {
		t.Fatalf("err = %v, want the stream to end the connect", err)
	}
}

func TestResolveEndpoint(t *testing.T) {
	base, _ := url.Parse("http://host:8080/mcp/sse?token=1")
	for data, want := range map[string]string{
		"/messages?sessionId=a":         "http://host:8080/messages?sessionId=a",
		"messages?sessionId=a":          "http://host:8080/mcp/messages?sessionId=a",
		"http://host:8080/m":            "http://host:8080/m",
		"http://elsewhere:8080/m":       "",
		"https://host:8080/messages":    "",
		"//elsewhere/messages?leak=yes": "",
	} {
		got, err := resolveEndpoint(base, data)
		if got != want || (err == nil) != (want != "") {
			t.Errorf("resolveEndpoint(%q) = %q, %v; want %q", data, got, err, want)
		}
	}
}

// acpCall records one call made through ACPOptions.Call.
type acpCall struct {
	serverID, requestID, method string
	params                      map[string]any
}

func TestACP(t *testing.T) {
	var mu sync.Mutex
	var calls []acpCall
	call := func(ctx context.Context, serverID, requestID, method string, params map[string]any, notify func(string, map[string]any)) (json.RawMessage, *RPCError, error) {
		mu.Lock()
		calls = append(calls, acpCall{serverID, requestID, method, params})
		mu.Unlock()
		switch method {
		case "tools/list":
			return json.RawMessage(`{"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}`), nil, nil
		case "tools/call":
			switch params["arguments"].(map[string]any)["mode"] {
			case "error":
				return nil, &RPCError{Code: -32602, Message: "bad arguments"}, nil
			case "broken":
				return nil, nil, errors.New("binding failed")
			}
			notify("notifications/tools/list_changed", nil)
			return json.RawMessage(`{"content":[{"type":"text","text":"ok"}]}`), nil, nil
		}
		return nil, &RPCError{Code: -32601, Message: "nope"}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Connect(ctx, Config{Name: "hosted", ACP: &ACPOptions{ID: "srv", Call: call}}, ClientInfo{Name: "test", Version: "1"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	tools, err := client.Tools(ctx)
	if err != nil || len(tools) != 1 || tools[0].Name != "echo" {
		t.Fatalf("tools = %+v, %v", tools, err)
	}
	result, err := client.CallTool(ctx, "echo", json.RawMessage(`{"mode":"ok","n":12345678901234567890}`))
	if err != nil || len(result.Content) != 1 || result.Content[0].Text != "ok" {
		t.Fatalf("call = %+v, %v", result, err)
	}
	if _, err := client.Tools(ctx); err != nil {
		t.Fatal(err)
	}
	var rpcErr *RPCError
	if _, err := client.CallTool(ctx, "echo", json.RawMessage(`{"mode":"error"}`)); !errors.As(err, &rpcErr) || rpcErr.Code != -32602 || rpcErr.Message != "bad arguments" {
		t.Errorf("inner MCP error = %v", err)
	}
	if _, err := client.CallTool(ctx, "echo", json.RawMessage(`{"mode":"broken"}`)); !errors.As(err, &rpcErr) || rpcErr.Code != -32603 || rpcErr.Message != "binding failed" {
		t.Errorf("binding error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	methods := []string{}
	ids := map[string]bool{}
	for _, c := range calls {
		methods = append(methods, c.method)
		if c.serverID != "srv" || c.requestID == "" || ids[c.requestID] {
			t.Errorf("call %+v: want server srv and a fresh request ID", c)
		}
		ids[c.requestID] = true
	}
	// No initialize; the list_changed notification forced the second tools/list.
	if fmt.Sprint(methods) != "[tools/list tools/call tools/list tools/call tools/call]" {
		t.Errorf("methods = %v", methods)
	}
	if n := calls[1].params["arguments"].(map[string]any)["n"]; n != json.Number("12345678901234567890") {
		t.Errorf("large integer argument = %#v", n)
	}
}

func TestConnectACPNeedsIDAndCall(t *testing.T) {
	call := func(context.Context, string, string, string, map[string]any, func(string, map[string]any)) (json.RawMessage, *RPCError, error) {
		return nil, nil, nil
	}
	for _, options := range []ACPOptions{{Call: call}, {ID: "srv"}} {
		if _, err := ConnectACP(context.Background(), "hosted", options, ClientInfo{}); err == nil {
			t.Errorf("ConnectACP(ID %q, Call set %v) succeeded", options.ID, options.Call != nil)
		}
	}
}

func TestACPCancel(t *testing.T) {
	started := make(chan struct{}, 1)
	ended := make(chan error, 1)
	call := func(ctx context.Context, _, _, _ string, _ map[string]any, _ func(string, map[string]any)) (json.RawMessage, *RPCError, error) {
		started <- struct{}{}
		<-ctx.Done()
		ended <- ctx.Err()
		return nil, nil, ctx.Err()
	}
	client, err := ConnectACP(context.Background(), "hosted", ACPOptions{ID: "srv", Call: call}, ClientInfo{})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(what string) error {
		select {
		case err := <-ended:
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("%s did not cancel the ACP call", what)
			return nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()
	if _, err := client.CallTool(ctx, "slow", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("CallTool after cancel = %v", err)
	}
	if err := wait("ctx cancel"); !errors.Is(err, context.Canceled) {
		t.Errorf("callback ctx = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := client.CallTool(context.Background(), "slow", nil)
		done <- err
	}()
	<-started
	client.Close()
	wait("Close")
	if err := <-done; err == nil {
		t.Error("CallTool succeeded after Close")
	}
}
