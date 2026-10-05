package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// StdioOptions launches a server as a subprocess speaking newline-delimited
// JSON-RPC over stdin/stdout.
type StdioOptions struct {
	Command string
	Args    []string
	Env     map[string]string
	Dir     string
}

// HTTPOptions connects to a streamable HTTP server.
type HTTPOptions struct {
	URL     string
	Headers map[string]string
}

// SSEOptions connects to a legacy HTTP+SSE server (MCP 2024-11-05).
type SSEOptions struct {
	URL     string
	Headers map[string]string
}

// ACPOptions reaches a server the ACP client hosts on the ACP connection
// itself, through the request-scoped mcp/message binding. Call sends one MCP
// request to server ID and returns its result, or the server's error in
// mcpErr; err is a binding failure. notify receives the server's
// notifications for that request. Cancelling ctx must cancel the request.
type ACPOptions struct {
	ID   string
	Call func(ctx context.Context, serverID, requestID, method string, params map[string]any, notify func(method string, params map[string]any)) (result json.RawMessage, mcpErr *RPCError, err error)
}

// ClientInfo identifies this client to servers during initialize.
type ClientInfo struct {
	Name    string
	Version string
	Roots   []string
	// Elicit answers a server's elicitation/create request; the client
	// advertises elicitation only when it is set. MCP requests carry no link
	// to the call that caused them, so ctx is the ctx of the CallTool in flight
	// when exactly one is, letting the hook read that call's values, and
	// context.Background() otherwise.
	Elicit func(ctx context.Context, request ElicitRequest) (ElicitResult, error)
}

// ConnectStdio starts the server process and completes initialization.
func ConnectStdio(ctx context.Context, name string, options StdioOptions, info ClientInfo) (*Client, error) {
	cmd := exec.Command(options.Command, options.Args...)
	cmd.Dir = options.Dir
	cmd.Env = os.Environ()
	for key, value := range options.Env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", options.Command, err)
	}
	client := newClient(name, info)
	t := &stdioTransport{cmd: cmd, stdin: stdin, done: make(chan struct{})}
	client.transport = t
	go func() {
		defer close(t.done)
		reader := bufio.NewReaderSize(stdout, 64<<10)
		for {
			line, err := reader.ReadBytes('\n')
			if len(bytes.TrimSpace(line)) > 0 {
				var msg message
				if json.Unmarshal(line, &msg) == nil {
					client.receive(msg)
				}
			}
			if err != nil {
				client.fail(fmt.Errorf("mcp server %s exited: %w", name, err))
				_ = cmd.Wait()
				return
			}
		}
	}()
	if err := client.initialize(ctx, info.Name, info.Version); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

type stdioTransport struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	mu    sync.Mutex
	done  chan struct{}
}

func (t *stdioTransport) send(_ context.Context, msg message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	_, err = t.stdin.Write(append(data, '\n'))
	return err
}

func (t *stdioTransport) close() error {
	_ = t.stdin.Close()
	select {
	case <-t.done:
	case <-time.After(2 * time.Second):
		_ = t.cmd.Process.Kill()
		<-t.done
	}
	return nil
}

// ConnectHTTP opens a streamable HTTP session and completes initialization.
func ConnectHTTP(ctx context.Context, name string, options HTTPOptions, info ClientInfo) (*Client, error) {
	client := newClient(name, info)
	client.transport = &httpTransport{url: options.URL, headers: options.Headers, client: client, http: &http.Client{}}
	if err := client.initialize(ctx, info.Name, info.Version); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

type httpTransport struct {
	url     string
	headers map[string]string
	client  *Client
	http    *http.Client

	mu        sync.Mutex
	sessionID string
	protocol  string
}

func (t *httpTransport) setProtocolVersion(version string) {
	t.mu.Lock()
	t.protocol = version
	t.mu.Unlock()
}

func (t *httpTransport) newRequest(ctx context.Context, method string, body io.Reader) (*http.Request, error) {
	request, err := http.NewRequestWithContext(ctx, method, t.url, body)
	if err != nil {
		return nil, err
	}
	setHeaders(request, t.headers)
	t.mu.Lock()
	if t.sessionID != "" {
		request.Header.Set("Mcp-Session-Id", t.sessionID)
	}
	if t.protocol != "" {
		request.Header.Set("MCP-Protocol-Version", t.protocol)
	}
	t.mu.Unlock()
	return request, nil
}

func (t *httpTransport) send(ctx context.Context, msg message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	request, err := t.newRequest(ctx, http.MethodPost, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	response, err := t.http.Do(request)
	if err != nil {
		return err
	}
	if id := response.Header.Get("Mcp-Session-Id"); id != "" {
		t.mu.Lock()
		t.sessionID = id
		t.mu.Unlock()
	}
	if response.StatusCode/100 != 2 {
		defer response.Body.Close()
		return statusError(t.url, response)
	}
	if response.StatusCode == http.StatusAccepted || len(msg.ID) == 0 || msg.Method == "" {
		response.Body.Close()
		return nil
	}
	contentType := response.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "text/event-stream") {
		go func() {
			defer response.Body.Close()
			readEvents(response.Body, func(_ string, data []byte) { t.client.deliver(data) })
		}()
		return nil
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	t.client.deliver(body)
	return nil
}

func (t *httpTransport) close() error {
	t.mu.Lock()
	hasSession := t.sessionID != ""
	t.mu.Unlock()
	if !hasSession {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	request, err := t.newRequest(ctx, http.MethodDelete, nil)
	if err != nil {
		return err
	}
	response, err := t.http.Do(request)
	if err != nil {
		return nil
	}
	response.Body.Close()
	return nil
}

// ConnectSSE opens a legacy HTTP+SSE session: server messages arrive on a GET
// event stream whose first event names the endpoint that client messages are
// POSTed to.
func ConnectSSE(ctx context.Context, name string, options SSEOptions, info ClientInfo) (*Client, error) {
	request, err := http.NewRequest(http.MethodGet, options.URL, nil)
	if err != nil {
		return nil, err
	}
	setHeaders(request, options.Headers)
	request.Header.Set("Accept", "text/event-stream")
	// The stream outlives ctx, which only bounds connecting.
	streamCtx, cancel := context.WithCancel(context.Background())
	defer context.AfterFunc(ctx, cancel)()
	t := &sseTransport{headers: options.Headers, http: &http.Client{}, cancel: cancel, done: make(chan struct{})}
	response, err := t.http.Do(request.WithContext(streamCtx))
	if err == nil && response.StatusCode/100 != 2 {
		err = statusError(options.URL, response)
		response.Body.Close()
	}
	if err != nil {
		cancel()
		return nil, err
	}
	client := newClient(name, info)
	client.transport = t
	ready := make(chan error, 1)
	go func() {
		defer close(t.done)
		defer response.Body.Close()
		announced := false
		readEvents(response.Body, func(event string, data []byte) {
			switch {
			case event == "endpoint" && !announced:
				announced = true
				var err error
				t.endpoint, err = resolveEndpoint(request.URL, string(data))
				ready <- err
			case announced && (event == "" || event == "message"):
				client.deliver(data)
			}
		})
		client.fail(fmt.Errorf("mcp server %s closed its event stream", name))
	}()
	select {
	case err = <-ready:
	case <-t.done:
		err = fmt.Errorf("mcp server %s closed its event stream before naming an endpoint", name)
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err == nil {
		err = client.initialize(ctx, info.Name, info.Version)
	}
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

// resolveEndpoint resolves the endpoint event's (usually relative) URL. It
// must share the stream's origin because configured headers often carry
// credentials.
func resolveEndpoint(base *url.URL, data string) (string, error) {
	ref, err := url.Parse(strings.TrimSpace(data))
	if err != nil {
		return "", fmt.Errorf("mcp sse endpoint %q: %w", data, err)
	}
	endpoint := base.ResolveReference(ref)
	if endpoint.Scheme != base.Scheme || endpoint.Host != base.Host {
		return "", fmt.Errorf("mcp sse endpoint %s is not on %s://%s", endpoint, base.Scheme, base.Host)
	}
	return endpoint.String(), nil
}

type sseTransport struct {
	endpoint string // set by the stream reader before it delivers any message
	headers  map[string]string
	http     *http.Client
	cancel   context.CancelFunc
	done     chan struct{}
}

func (t *sseTransport) send(ctx context.Context, msg message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	setHeaders(request, t.headers)
	request.Header.Set("Content-Type", "application/json")
	response, err := t.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return statusError(t.endpoint, response)
	}
	return nil
}

func (t *sseTransport) close() error {
	t.cancel()
	<-t.done
	return nil
}

// ConnectACP binds a server hosted on the ACP connection. The binding targets
// stateless MCP (2026-07-28), where tools/list and tools/call need no
// initialize handshake, so none is sent; ctx is unused.
func ConnectACP(_ context.Context, name string, options ACPOptions, info ClientInfo) (*Client, error) {
	if options.ID == "" || options.Call == nil {
		return nil, errors.New("mcp acp transport needs a server ID and a Call function")
	}
	client := newClient(name, info)
	ctx, cancel := context.WithCancel(context.Background())
	client.transport = &acpTransport{options: options, client: client, ctx: ctx, cancel: cancel}
	return client, nil
}

type acpTransport struct {
	options ACPOptions
	client  *Client
	ctx     context.Context // cancelled by close, ending requests in flight
	cancel  context.CancelFunc
}

// send runs each request through Call and feeds the outcome back as a
// JSON-RPC response. Everything else is dropped: the binding carries no
// client notifications, and cancellation travels through ctx.
func (t *acpTransport) send(ctx context.Context, msg message) error {
	if msg.Method == "" || len(msg.ID) == 0 {
		return nil
	}
	var params map[string]any
	if len(msg.Params) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(msg.Params))
		decoder.UseNumber() // keep large integers in tool arguments exact
		if err := decoder.Decode(&params); err != nil {
			return err
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(t.ctx, cancel)
	go func() {
		defer cancel()
		defer stop()
		reply := message{JSONRPC: "2.0", ID: msg.ID}
		result, mcpErr, err := t.options.Call(ctx, t.options.ID, string(msg.ID), msg.Method, params, t.notify)
		switch {
		case err != nil:
			reply.Error = &RPCError{Code: -32603, Message: err.Error()}
		case mcpErr != nil:
			reply.Error = mcpErr
		default:
			reply.Result = result
		}
		t.client.receive(reply)
	}()
	return nil
}

func (t *acpTransport) notify(method string, params map[string]any) {
	var raw json.RawMessage
	if params != nil {
		raw, _ = json.Marshal(params)
	}
	t.client.receive(message{JSONRPC: "2.0", Method: method, Params: raw})
}

func (t *acpTransport) close() error {
	t.cancel()
	return nil
}

func setHeaders(request *http.Request, headers map[string]string) {
	for key, value := range headers {
		request.Header.Set(key, value)
	}
}

func statusError(url string, response *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
	return fmt.Errorf("mcp http %s: %s %s", url, response.Status, strings.TrimSpace(string(body)))
}

// deliver hands a JSON-RPC message or batch from an HTTP body or event to
// the client.
func (c *Client) deliver(data []byte) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return
	}
	if data[0] == '[' {
		var batch []message
		if json.Unmarshal(data, &batch) == nil {
			for _, msg := range batch {
				c.receive(msg)
			}
		}
		return
	}
	var msg message
	if json.Unmarshal(data, &msg) == nil {
		c.receive(msg)
	}
}

// readEvents parses a server-sent event stream, calling onEvent with each
// event's type ("" when unnamed) and joined data lines.
func readEvents(body io.Reader, onEvent func(event string, data []byte)) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	var event string
	var data []string
	flush := func() {
		if len(data) > 0 {
			onEvent(event, []byte(strings.Join(data, "\n")))
			data = data[:0]
		}
		event = ""
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if value, ok := strings.CutPrefix(line, "event:"); ok {
			event = strings.TrimPrefix(value, " ")
		} else if value, ok := strings.CutPrefix(line, "data:"); ok {
			data = append(data, strings.TrimPrefix(value, " "))
		}
	}
	flush()
}

func fileURI(path string) string {
	path = filepath.ToSlash(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path // Windows drive paths
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

var errUnsupported = errors.New("unsupported MCP transport")

// Config describes a server to connect to; exactly one transport is set.
type Config struct {
	Name  string
	Stdio *StdioOptions
	HTTP  *HTTPOptions
	SSE   *SSEOptions
	ACP   *ACPOptions
}

// Connect dials the configured transport.
func Connect(ctx context.Context, config Config, info ClientInfo) (*Client, error) {
	switch {
	case config.Stdio != nil:
		return ConnectStdio(ctx, config.Name, *config.Stdio, info)
	case config.HTTP != nil:
		return ConnectHTTP(ctx, config.Name, *config.HTTP, info)
	case config.SSE != nil:
		return ConnectSSE(ctx, config.Name, *config.SSE, info)
	case config.ACP != nil:
		return ConnectACP(ctx, config.Name, *config.ACP, info)
	}
	return nil, errUnsupported
}
