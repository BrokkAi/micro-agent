// Package mcp is a minimal Model Context Protocol client covering the stdio
// and streamable HTTP transports and the tools capability.
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

// Content is one tool result content item.
type Content struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	Data     string `json:"data,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	URI      string `json:"uri,omitempty"`
	Resource *struct {
		URI      string `json:"uri"`
		MimeType string `json:"mimeType,omitempty"`
		Text     string `json:"text,omitempty"`
	} `json:"resource,omitempty"`
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

// Client is a connected MCP session.
type Client struct {
	Name         string
	Instructions string

	transport transport
	roots     []string
	nextID    atomic.Int64

	mu      sync.Mutex
	pending map[string]chan message
	closed  bool
	err     error

	toolsMu sync.Mutex
	tools   []Tool
	changed atomic.Bool
}

func newClient(name string, roots []string) *Client {
	return &Client{Name: name, roots: roots, pending: map[string]chan message{}}
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
	default:
		reply.Error = &RPCError{Code: -32601, Message: "method not supported by client: " + msg.Method}
	}
	_ = c.transport.send(context.Background(), reply)
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
	id := json.RawMessage(fmt.Sprint(c.nextID.Add(1)))
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
	params := map[string]any{
		"protocolVersion": ProtocolVersion,
		"capabilities":    map[string]any{"roots": map[string]any{"listChanged": false}},
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

// CallTool invokes a tool with JSON-encoded arguments.
func (c *Client) CallTool(ctx context.Context, name string, arguments json.RawMessage) (CallResult, error) {
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
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
