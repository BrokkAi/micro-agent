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

// ClientInfo identifies this client to servers during initialize.
type ClientInfo struct {
	Name    string
	Version string
	Roots   []string
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
	client := newClient(name, info.Roots)
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
	client := newClient(name, info.Roots)
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
	for key, value := range t.headers {
		request.Header.Set(key, value)
	}
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
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return fmt.Errorf("mcp http %s: %s %s", t.url, response.Status, strings.TrimSpace(string(body)))
	}
	if response.StatusCode == http.StatusAccepted || len(msg.ID) == 0 || msg.Method == "" {
		response.Body.Close()
		return nil
	}
	contentType := response.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "text/event-stream") {
		go func() {
			defer response.Body.Close()
			readEvents(response.Body, t.deliver)
		}()
		return nil
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	t.deliver(body)
	return nil
}

// deliver hands a JSON-RPC message or batch to the client.
func (t *httpTransport) deliver(data []byte) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return
	}
	if data[0] == '[' {
		var batch []message
		if json.Unmarshal(data, &batch) == nil {
			for _, msg := range batch {
				t.client.receive(msg)
			}
		}
		return
	}
	var msg message
	if json.Unmarshal(data, &msg) == nil {
		t.client.receive(msg)
	}
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

// readEvents parses a server-sent event stream, calling onData with each
// event's joined data lines.
func readEvents(body io.Reader, onData func([]byte)) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	var data []string
	flush := func() {
		if len(data) > 0 {
			onData([]byte(strings.Join(data, "\n")))
			data = data[:0]
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			flush()
			continue
		}
		if value, ok := strings.CutPrefix(line, "data:"); ok {
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

// Config describes a server to connect to; exactly one of Stdio or HTTP is set.
type Config struct {
	Name  string
	Stdio *StdioOptions
	HTTP  *HTTPOptions
}

// Connect dials the configured transport.
func Connect(ctx context.Context, config Config, info ClientInfo) (*Client, error) {
	switch {
	case config.Stdio != nil:
		return ConnectStdio(ctx, config.Name, *config.Stdio, info)
	case config.HTTP != nil:
		return ConnectHTTP(ctx, config.Name, *config.HTTP, info)
	}
	return nil, errUnsupported
}
