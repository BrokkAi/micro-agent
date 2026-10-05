// Package mcp is a minimal Model Context Protocol client covering the stdio,
// streamable HTTP, legacy HTTP+SSE and MCP-over-ACP transports, the tools
// capability and form elicitation.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const ProtocolVersion = "2025-06-18"

// Tool is a tool advertised by a server.
type Tool struct {
	Name        string          `json:"name"`
	Title       string          `json:"title,omitempty"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"inputSchema"`
	Annotations *struct {
		ReadOnlyHint *bool `json:"readOnlyHint,omitempty"`
	} `json:"annotations,omitempty"`
}

// Content is one tool result content item. Type selects the fields in use:
// "text" sets Text; "image" and "audio" set base64 Data and MimeType;
// "resource" sets Resource; "resource_link" sets URI, Name and MimeType.
type Content struct {
	Type     string            `json:"type"`
	Text     string            `json:"text,omitempty"`
	Data     string            `json:"data,omitempty"`
	MimeType string            `json:"mimeType,omitempty"`
	URI      string            `json:"uri,omitempty"`
	Name     string            `json:"name,omitempty"`
	Resource *ResourceContents `json:"resource,omitempty"`
}

// ResourceContents is an embedded resource: Text for a text resource, base64
// Blob for a binary one.
type ResourceContents struct {
	URI      string `json:"uri"`
	MimeType string `json:"mimeType,omitempty"`
	Text     string `json:"text,omitempty"`
	Blob     string `json:"blob,omitempty"`
}

// CallResult is the result of tools/call.
type CallResult struct {
	Content           []Content       `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError"`
}

// RPCError is a JSON-RPC error returned by a server.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("mcp error %d: %s", e.Code, e.Message) }

// ElicitRequest is a server's elicitation/create request: a message for the
// user and the JSON schema of the form to fill in.
type ElicitRequest struct {
	Message         string          `json:"message"`
	RequestedSchema json.RawMessage `json:"requestedSchema"`
}

// ElicitResult answers an elicitation. Action is "accept", "decline" or
// "cancel"; Content holds the form values on accept.
type ElicitResult struct {
	Action  string         `json:"action"`
	Content map[string]any `json:"content,omitempty"`
}

type message struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

// transport moves framed JSON-RPC messages. Send delivers one message;
// incoming messages are passed to the receive callback set at construction.
type transport interface {
	send(ctx context.Context, msg message) error
	close() error
}

// requestIDs numbers requests across all clients, so request-scoped
// mcp/message calls from two sessions to one ACP-hosted server never share a
// request ID.
var requestIDs atomic.Int64

// Client is a connected MCP session.
type Client struct {
	Name         string
	Instructions string

	transport transport
	roots     []string
	elicit    func(context.Context, ElicitRequest) (ElicitResult, error)

	mu      sync.Mutex
	pending map[string]chan message
	calls   map[*context.Context]struct{} // CallTool contexts in flight
	closed  bool
	err     error

	toolsMu sync.Mutex
	tools   []Tool
	changed atomic.Bool
}

func newClient(name string, info ClientInfo) *Client {
	return &Client{Name: name, roots: info.Roots, elicit: info.Elicit, pending: map[string]chan message{}, calls: map[*context.Context]struct{}{}}
}

// receive dispatches one inbound message from the transport.
func (c *Client) receive(msg message) {
	switch {
	case msg.Method != "" && len(msg.ID) > 0:
		go c.serve(msg)
	case msg.Method != "":
		if msg.Method == "notifications/tools/list_changed" {
			c.changed.Store(true)
		}
	case len(msg.ID) > 0:
		c.mu.Lock()
		ch := c.pending[string(msg.ID)]
		delete(c.pending, string(msg.ID))
		c.mu.Unlock()
		if ch != nil {
			ch <- msg
		}
	}
}

// serve answers server-initiated requests we understand.
func (c *Client) serve(msg message) {
	reply := message{JSONRPC: "2.0", ID: msg.ID}
	switch msg.Method {
	case "ping":
		reply.Result = json.RawMessage(`{}`)
	case "roots/list":
		type root struct {
			URI  string `json:"uri"`
			Name string `json:"name,omitempty"`
		}
		roots := []root{}
		for _, path := range c.roots {
			roots = append(roots, root{URI: fileURI(path)})
		}
		reply.Result, _ = json.Marshal(map[string]any{"roots": roots})
	case "elicitation/create":
		if c.elicit == nil {
			reply.Error = unsupported(msg.Method)
			break
		}
		reply.Result, reply.Error = c.elicitation(msg.Params)
	default:
		reply.Error = unsupported(msg.Method)
	}
	_ = c.transport.send(context.Background(), reply)
}

func unsupported(method string) *RPCError {
	return &RPCError{Code: -32601, Message: "method not supported by client: " + method}
}

func (c *Client) elicitation(params json.RawMessage) (json.RawMessage, *RPCError) {
	var request ElicitRequest
	if err := json.Unmarshal(params, &request); err != nil {
		return nil, &RPCError{Code: -32602, Message: err.Error()}
	}
	result, err := c.elicit(c.elicitContext(), request)
	var raw json.RawMessage
	if err == nil {
		raw, err = json.Marshal(result)
	}
	if err != nil {
		return nil, &RPCError{Code: -32603, Message: err.Error()}
	}
	return raw, nil
}

// elicitContext returns the ctx of the only CallTool in flight, or
// context.Background(); see ClientInfo.Elicit.
func (c *Client) elicitContext() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.calls) == 1 {
		for ctx := range c.calls {
			return *ctx
		}
	}
	return context.Background()
}

// fail aborts all pending calls; used when a transport dies.
func (c *Client) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.err = err
	}
	for id, ch := range c.pending {
		delete(c.pending, id)
		close(ch)
	}
}

func (c *Client) call(ctx context.Context, method string, params, result any) error {
	id := json.RawMessage(fmt.Sprint(requestIDs.Add(1)))
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	ch := make(chan message, 1)
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return c.err
	}
	c.pending[string(id)] = ch
	c.mu.Unlock()
	cleanup := func() {
		c.mu.Lock()
		delete(c.pending, string(id))
		c.mu.Unlock()
	}
	if err := c.transport.send(ctx, message{JSONRPC: "2.0", ID: id, Method: method, Params: raw}); err != nil {
		cleanup()
		return err
	}
	select {
	case <-ctx.Done():
		cleanup()
		_ = c.notify(context.Background(), "notifications/cancelled", map[string]any{"requestId": id, "reason": "cancelled"})
		return ctx.Err()
	case reply, ok := <-ch:
		if !ok {
			c.mu.Lock()
			err := c.err
			c.mu.Unlock()
			if err == nil {
				err = errors.New("mcp connection closed")
			}
			return err
		}
		if reply.Error != nil {
			return reply.Error
		}
		if result != nil && len(reply.Result) > 0 {
			return json.Unmarshal(reply.Result, result)
		}
		return nil
	}
}

func (c *Client) notify(ctx context.Context, method string, params any) error {
	var raw json.RawMessage
	if params != nil {
		var err error
		if raw, err = json.Marshal(params); err != nil {
			return err
		}
	}
	return c.transport.send(ctx, message{JSONRPC: "2.0", Method: method, Params: raw})
}

func (c *Client) initialize(ctx context.Context, clientName, clientVersion string) error {
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
		Instructions    string `json:"instructions"`
	}
	capabilities := map[string]any{"roots": map[string]any{"listChanged": false}}
	if c.elicit != nil {
		capabilities["elicitation"] = map[string]any{}
	}
	params := map[string]any{
		"protocolVersion": ProtocolVersion,
		"capabilities":    capabilities,
		"clientInfo":      map[string]any{"name": clientName, "version": clientVersion},
	}
	if err := c.call(ctx, "initialize", params, &result); err != nil {
		return fmt.Errorf("initialize: %w", err)
	}
	c.Instructions = result.Instructions
	if http, ok := c.transport.(*httpTransport); ok {
		http.setProtocolVersion(result.ProtocolVersion)
	}
	return c.notify(ctx, "notifications/initialized", nil)
}

// Tools returns the server's tools, refetching after a list_changed
// notification.
func (c *Client) Tools(ctx context.Context) ([]Tool, error) {
	c.toolsMu.Lock()
	defer c.toolsMu.Unlock()
	if c.tools != nil && !c.changed.Load() {
		return c.tools, nil
	}
	c.changed.Store(false)
	var all []Tool
	cursor := ""
	for {
		params := map[string]any{}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var page struct {
			Tools      []Tool `json:"tools"`
			NextCursor string `json:"nextCursor"`
		}
		if err := c.call(ctx, "tools/list", params, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Tools...)
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if all == nil {
		all = []Tool{}
	}
	c.tools = all
	return all, nil
}

// CallTool invokes a tool with JSON-encoded arguments. While it runs, ctx is
// the context an elicitation hook receives; see ClientInfo.Elicit.
func (c *Client) CallTool(ctx context.Context, name string, arguments json.RawMessage) (CallResult, error) {
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	c.mu.Lock()
	c.calls[&ctx] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.calls, &ctx)
		c.mu.Unlock()
	}()
	var result CallResult
	err := c.call(ctx, "tools/call", map[string]any{"name": name, "arguments": arguments}, &result)
	return result, err
}

// Close shuts the session down.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	err := c.transport.close()
	c.fail(errors.New("mcp client closed"))
	return err
}
