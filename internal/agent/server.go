package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/BrokkAi/acp-go"
	acpmcp "github.com/BrokkAi/acp-go/mcp"
	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/acp-go/unstable"
)

// Serve runs the agent over one ACP connection until ctx ends or the client
// hangs up, which returns nil.
func Serve(ctx context.Context, a *Agent, in io.ReadCloser, out io.WriteCloser) error {
	ready := make(chan struct{})
	w := &frameWriter{out: out, queued: map[schema.SessionId][][]byte{}}
	r := newFrameReader(in, w, ready)
	var notifications acp.Notifications
	conn := acp.Connect(r, w, a.guard(unstable.Handle(a.dispatch, a)), func(method string, raw json.RawMessage) error {
		return notifications(method, raw)
	})
	a.client = &clientConn{a: a, conn: conn, out: w, messages: acpmcp.NewMessageClient(conn)}
	notifications = a.client.messages.Notifications(a.notification)
	close(ready) // the reader holds frames back until here
	defer conn.Close()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-conn.Done():
		if err := conn.Err(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		return nil
	}
}

// guard answers initialize and turns away everything before it, and any
// optional method the initialize response did not offer. It sits outside
// unstable.Handle so session/fork and providers/* are gated too.
func (a *Agent) guard(next acp.Handler) acp.Handler {
	return func(ctx context.Context, method string, raw json.RawMessage) (any, error) {
		if method == schema.InitializeMethodName {
			request, err := decodeInitialize(raw)
			if err != nil {
				return nil, err
			}
			return a.Initialize(ctx, request)
		}
		a.mu.Lock()
		offered := a.offered
		a.mu.Unlock()
		if offered == nil {
			return nil, &acp.RPCError{Code: int(schema.ErrorCodeInvalidRequest), Message: "agent is not initialized"}
		}
		if !offers(offered, method) {
			return nil, methodNotFound(method)
		}
		return next(ctx, method, raw)
	}
}

// decodeInitialize accepts any protocol version from 1 up, since the response
// names the version the agent speaks. It reads client capabilities from the
// v1 or the v2 shape, and drops any it cannot read rather than failing.
func decodeInitialize(raw json.RawMessage) (schema.InitializeRequest, error) {
	var version struct {
		ProtocolVersion *schema.ProtocolVersion `json:"protocolVersion"`
	}
	if err := json.Unmarshal(raw, &version); err != nil || version.ProtocolVersion == nil {
		return schema.InitializeRequest{}, invalidParams("initialize needs a numeric protocolVersion")
	}
	if *version.ProtocolVersion < 1 {
		return schema.InitializeRequest{}, &acp.RPCError{Code: int(schema.ErrorCodeInvalidRequest), Message: fmt.Sprintf("unsupported ACP protocol version %d; micro-agent speaks version %d", *version.ProtocolVersion, acp.Version)}
	}
	var request schema.InitializeRequest
	_ = json.Unmarshal(raw, &request)
	if request.ClientCapabilities == nil {
		var v2 struct {
			Capabilities *struct {
				Elicitation *schema.ElicitationCapabilities `json:"elicitation"`
				Auth        *struct {
					Terminal *struct{} `json:"terminal"`
				} `json:"auth"`
			} `json:"capabilities"`
		}
		_ = json.Unmarshal(raw, &v2)
		if c := v2.Capabilities; c != nil {
			// Mapped as acp-go's agentrouter downgrades a v2 initialize.
			request.ClientCapabilities = &schema.ClientCapabilities{
				Elicitation: c.Elicitation,
				Session: &schema.ClientSessionCapabilities{ConfigOptions: &schema.SessionConfigOptionsCapabilities{
					Boolean: &schema.BooleanConfigOptionCapabilities{},
				}},
			}
			if c.Auth != nil && c.Auth.Terminal != nil {
				request.ClientCapabilities.Auth = &schema.AuthCapabilities{Terminal: ptr(true)}
			}
		}
	}
	return request, nil
}

// offers reports whether caps advertise method, for the optional ones.
func offers(caps *schema.AgentCapabilities, method string) bool {
	session := caps.SessionCapabilities
	if session == nil {
		session = &schema.SessionCapabilities{}
	}
	switch method {
	case schema.SessionLoadMethodName:
		return isTrue(caps.LoadSession)
	case schema.SessionResumeMethodName:
		return session.Resume != nil
	case schema.SessionCloseMethodName:
		return session.Close != nil
	case schema.SessionListMethodName:
		return session.List != nil
	case schema.SessionDeleteMethodName:
		return session.Delete != nil
	case schema.SessionForkMethodName:
		return session.Fork != nil
	case schema.LogoutMethodName:
		return caps.Auth != nil && caps.Auth.Logout != nil
	case schema.ProvidersListMethodName, schema.ProvidersSetMethodName, schema.ProvidersDisableMethodName:
		return caps.Providers != nil
	}
	return true
}

func (a *Agent) dispatch(ctx context.Context, method string, raw json.RawMessage) (any, error) {
	switch method {
	case schema.AuthenticateMethodName:
		return handle(ctx, raw, a.Authenticate)
	case schema.SessionNewMethodName:
		return handle(ctx, raw, a.NewSession)
	case schema.SessionLoadMethodName:
		return handle(ctx, raw, a.LoadSession)
	case schema.SessionResumeMethodName:
		return handle(ctx, raw, a.ResumeSession)
	case schema.SessionForkMethodName:
		return handle(ctx, raw, a.ForkSession)
	case schema.SessionCloseMethodName:
		return handle(ctx, raw, a.CloseSession)
	case schema.SessionListMethodName:
		return handle(ctx, raw, a.ListSessions)
	case schema.SessionDeleteMethodName:
		return handle(ctx, raw, a.DeleteSession)
	case schema.SessionPromptMethodName:
		return handle(ctx, raw, a.Prompt)
	case schema.SessionSetModeMethodName:
		return handle(ctx, raw, a.SetMode)
	case schema.SessionSetConfigOptionMethodName:
		return handle(ctx, raw, a.SetConfigOption)
	case schema.LogoutMethodName:
		return handle(ctx, raw, a.Logout)
	case schema.ProvidersListMethodName:
		return handle(ctx, raw, a.ListProviders)
	case schema.ProvidersSetMethodName:
		return handle(ctx, raw, a.SetProvider)
	case schema.ProvidersDisableMethodName:
		return handle(ctx, raw, a.DisableProvider)
	}
	return nil, methodNotFound(method)
}

// handle decodes a request's params for run; params that do not decode are
// invalid params.
func handle[Req, Resp any](ctx context.Context, raw json.RawMessage, run func(context.Context, Req) (Resp, error)) (any, error) {
	var request Req
	if err := json.Unmarshal(raw, &request); err != nil {
		return nil, invalidParams(err.Error())
	}
	return run(ctx, request)
}

// notification runs on the connection's read loop, so it must not block.
// Malformed notifications are ignored: returning an error would close the
// connection.
func (a *Agent) notification(method string, raw json.RawMessage) error {
	var cancel schema.CancelNotification
	if method == schema.SessionCancelMethodName && json.Unmarshal(raw, &cancel) == nil {
		return a.CancelSession(context.Background(), cancel)
	}
	return nil
}

func methodNotFound(method string) error {
	return &acp.RPCError{Code: int(schema.ErrorCodeMethodNotFound), Message: "method not found: " + method}
}

// clientConn is the agent's side of calls into the client. A method the
// client did not advertise fails with method-not-found without a round trip.
type clientConn struct {
	a    *Agent
	conn *acp.Connection
	out  *frameWriter
	// messages carries MCP-over-ACP calls; its notifications are routed in.
	messages *acpmcp.MessageClient
}

func (c *clientConn) Call(ctx context.Context, method string, params, result any) error {
	return c.conn.Call(ctx, method, params, result)
}

func (c *clientConn) Notify(ctx context.Context, method string, params any) error {
	return c.conn.Notify(ctx, method, params)
}

func call[T any](ctx context.Context, c *clientConn, supported bool, method string, params any) (T, error) {
	var result T
	if !supported {
		return result, notAdvertised(method)
	}
	err := c.Call(ctx, method, params, &result)
	return result, err
}

func notAdvertised(method string) error {
	return &acp.RPCError{Code: int(schema.ErrorCodeMethodNotFound), Message: "client did not advertise " + method + " support"}
}

func (c *clientConn) ReadTextFile(ctx context.Context, request schema.ReadTextFileRequest) (schema.ReadTextFileResponse, error) {
	return call[schema.ReadTextFileResponse](ctx, c, c.a.canRead(), schema.FsReadTextFileMethodName, request)
}

func (c *clientConn) WriteTextFile(ctx context.Context, request schema.WriteTextFileRequest) (schema.WriteTextFileResponse, error) {
	return call[schema.WriteTextFileResponse](ctx, c, c.a.canWrite(), schema.FsWriteTextFileMethodName, request)
}

func (c *clientConn) RequestPermission(ctx context.Context, request schema.RequestPermissionRequest) (schema.RequestPermissionResponse, error) {
	return call[schema.RequestPermissionResponse](ctx, c, true, schema.SessionRequestPermissionMethodName, request)
}

func (c *clientConn) CreateElicitation(ctx context.Context, request schema.CreateElicitationRequest) (schema.CreateElicitationResponse, error) {
	supported := request.Form != nil && c.a.canForm() || request.URL != nil && c.a.canURL()
	return call[schema.CreateElicitationResponse](ctx, c, supported, schema.ElicitationCreateMethodName, request)
}

func (c *clientConn) CompleteElicitation(ctx context.Context, id schema.ElicitationId) error {
	if !c.a.canURL() {
		return notAdvertised(schema.ElicitationCompleteMethodName)
	}
	return c.Notify(ctx, schema.ElicitationCompleteMethodName, schema.CompleteElicitationNotification{ElicitationID: id})
}

func (c *clientConn) CreateTerminal(ctx context.Context, request schema.CreateTerminalRequest) (schema.CreateTerminalResponse, error) {
	return call[schema.CreateTerminalResponse](ctx, c, c.a.canTerminal(), schema.TerminalCreateMethodName, request)
}

func (c *clientConn) TerminalOutput(ctx context.Context, request schema.TerminalOutputRequest) (schema.TerminalOutputResponse, error) {
	return call[schema.TerminalOutputResponse](ctx, c, c.a.canTerminal(), schema.TerminalOutputMethodName, request)
}

func (c *clientConn) WaitForTerminalExit(ctx context.Context, request schema.WaitForTerminalExitRequest) (schema.WaitForTerminalExitResponse, error) {
	return call[schema.WaitForTerminalExitResponse](ctx, c, c.a.canTerminal(), schema.TerminalWaitForExitMethodName, request)
}

func (c *clientConn) KillTerminal(ctx context.Context, request schema.KillTerminalRequest) (schema.KillTerminalResponse, error) {
	return call[schema.KillTerminalResponse](ctx, c, c.a.canTerminal(), schema.TerminalKillMethodName, request)
}

func (c *clientConn) ReleaseTerminal(ctx context.Context, request schema.ReleaseTerminalRequest) (schema.ReleaseTerminalResponse, error) {
	return call[schema.ReleaseTerminalResponse](ctx, c, c.a.canTerminal(), schema.TerminalReleaseMethodName, request)
}

// updater sends one session's updates; each prompt turn gets one. A queued
// updater holds them until the response that introduces the session.
type updater struct {
	a      *Agent
	id     schema.SessionId
	queued bool
}

func (u updater) Update(update schema.SessionUpdate) error {
	if u.queued {
		return u.a.queue(u.id, update)
	}
	return u.a.notify(context.Background(), u.id, update)
}

func (a *Agent) notify(ctx context.Context, id schema.SessionId, update schema.SessionUpdate) error {
	return a.client.Notify(ctx, schema.SessionUpdateMethodName, schema.SessionNotification{SessionID: id, Update: update})
}

// queue holds update until the response carrying session id has been
// written, since clients may drop updates for a session they do not know.
func (a *Agent) queue(id schema.SessionId, update schema.SessionUpdate) error {
	data, err := encode(frame{Method: schema.SessionUpdateMethodName, Params: schema.SessionNotification{SessionID: id, Update: update}})
	if err != nil {
		return err
	}
	a.client.out.hold(id, data)
	return nil
}

// frame is a JSON-RPC message written beside acp.Connection.
type frame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  any             `json:"params,omitempty"`
	Error   *acp.RPCError   `json:"error,omitempty"`
}

func encode(f frame) ([]byte, error) {
	f.JSONRPC = "2.0"
	data, err := json.Marshal(f)
	return append(data, '\n'), err
}

// frameWriter serializes frames to the client. acp.Connection writes each
// frame with a single Write, so a Write that carries a response introducing a
// session can append that session's queued updates right behind it.
type frameWriter struct {
	mu     sync.Mutex
	out    io.WriteCloser
	queued map[schema.SessionId][][]byte
}

func (w *frameWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.out.Write(p)
	if err != nil {
		return n, err
	}
	for _, id := range w.introduced(p) {
		for _, queued := range w.queued[id] {
			if _, err := w.out.Write(queued); err != nil {
				return n, err
			}
		}
		delete(w.queued, id)
	}
	return n, nil
}

func (w *frameWriter) Close() error { return w.out.Close() }

func (w *frameWriter) hold(id schema.SessionId, frame []byte) {
	w.mu.Lock()
	w.queued[id] = append(w.queued[id], frame)
	w.mu.Unlock()
}

// introduced returns the queued sessions whose ID p hands to the client as a
// response result (session/new, session/fork), alone or in a batch.
func (w *frameWriter) introduced(p []byte) []schema.SessionId {
	mentioned := false
	for id := range w.queued {
		mentioned = mentioned || bytes.Contains(p, []byte(id))
	}
	if !mentioned {
		return nil
	}
	type response struct {
		Result struct {
			SessionID schema.SessionId `json:"sessionId"`
		} `json:"result"`
	}
	var batch []response
	if json.Unmarshal(p, &batch) != nil {
		batch = make([]response, 1)
		if json.Unmarshal(p, &batch[0]) != nil {
			return nil
		}
	}
	var ids []schema.SessionId
	for _, r := range batch {
		if _, ok := w.queued[r.Result.SessionID]; ok {
			ids = append(ids, r.Result.SessionID)
		}
	}
	return ids
}

// maxFrame is the limit of acp.Connect's line scanner, which stops the whole
// connection on a longer frame.
const maxFrame = 8 << 20

// frameReader feeds inbound frames to acp.Connect and drops any over its
// limit instead of letting it close the connection. Each request in a dropped
// frame is answered with an error, and each response to one of the agent's
// own requests is replaced by an error for it, so the waiting call fails
// instead of hanging.
type frameReader struct {
	in    io.ReadCloser
	r     *bufio.Reader
	out   io.Writer
	ready <-chan struct{} // closed once Serve has wired the connection
	frame []byte          // unread rest of the current frame
	err   error
}

func newFrameReader(in io.ReadCloser, out io.Writer, ready <-chan struct{}) *frameReader {
	return &frameReader{in: in, r: bufio.NewReaderSize(in, 64<<10), out: out, ready: ready}
}

func (f *frameReader) Read(p []byte) (int, error) {
	<-f.ready
	for len(f.frame) == 0 {
		if f.err != nil {
			return 0, f.err
		}
		f.frame, f.err = f.next()
	}
	n := copy(p, f.frame)
	f.frame = f.frame[n:]
	return n, nil
}

func (f *frameReader) Close() error { return f.in.Close() }

// next returns the next frame, or the errors standing in for one too long to
// pass on. A long frame is scanned as it streams past rather than held.
func (f *frameReader) next() ([]byte, error) {
	var line []byte
	var scan *frameScan
	for {
		chunk, err := f.r.ReadSlice('\n')
		if scan != nil {
			scan.write(chunk)
		} else if line = append(line, chunk...); len(bytes.TrimSuffix(line, []byte("\n"))) >= maxFrame {
			scan = &frameScan{}
			scan.write(line)
			line = nil
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if scan == nil {
			return line, err
		}
		return f.oversize(scan), err
	}
}

// oversize answers the requests in a dropped frame, as a batch if it was one,
// and returns errors standing in for its responses.
func (f *frameReader) oversize(scan *frameScan) []byte {
	var replies []frame
	var back []byte
	for _, m := range scan.msgs {
		reply := frame{JSONRPC: "2.0", ID: m.id, Error: &acp.RPCError{
			Code:    int(schema.ErrorCodeInvalidRequest),
			Message: fmt.Sprintf("ACP frame over the %d MiB limit dropped", maxFrame>>20),
		}}
		switch {
		case m.request:
			replies = append(replies, reply)
		case m.response:
			if data, err := encode(reply); err == nil {
				back = append(back, data...)
			}
		}
	}
	if len(replies) == 0 {
		return back
	}
	var answer any = replies[0]
	if scan.batch {
		answer = replies
	}
	if data, err := json.Marshal(answer); err == nil {
		_, _ = f.out.Write(append(data, '\n'))
	}
	return back
}

// frameScan follows the JSON structure of a frame a byte at a time and notes
// each message in it that has an id: the frame itself, or each entry of a
// batch.
type frameScan struct {
	depth    int
	batch    bool
	str, esc bool   // inside a string; just after a backslash in one
	inMsg    bool   // inside a message object
	key      bool   // the next string among the message's members is a name
	name     []byte // the last member name, as far as it matters
	reading  bool   // reading the id value into id
	id       []byte
	msg      frameMsg
	msgs     []frameMsg
}

// frameMsg is what a dropped frame showed of one message.
type frameMsg struct {
	id                json.RawMessage
	request, response bool
}

func (s *frameScan) write(p []byte) {
	for _, c := range p {
		s.step(c)
	}
}

func (s *frameScan) step(c byte) {
	level := 1 // the depth of a message's members
	if s.batch {
		level = 2
	}
	member := s.inMsg && s.depth == level
	switch {
	case s.str:
		switch {
		case s.esc:
			s.esc = false
		case c == '\\':
			s.esc = true
		case c == '"':
			s.str = false
		}
		if member && s.key && s.str && len(s.name) <= len("method") {
			s.name = append(s.name, c)
		}
	case c == '"':
		s.str = true
		if member && s.key {
			s.name = s.name[:0]
		}
	case c == '{' || c == '[':
		s.batch = s.batch || s.depth == 0 && c == '['
		s.depth++
		if c == '{' && s.depth == level {
			s.inMsg, s.key, s.msg = true, true, frameMsg{}
		}
	case c == '}' || c == ']':
		if member {
			s.endID()
			if s.msg.id != nil {
				s.msgs = append(s.msgs, s.msg)
			}
			s.inMsg = false
		}
		s.depth--
	case member && c == ':':
		s.key = false
		switch string(s.name) {
		case "method", "params":
			s.msg.request = true
		case "result", "error":
			s.msg.response = true
		case "id":
			s.reading, s.id = true, s.id[:0]
			return
		}
	case member && c == ',':
		s.endID()
		s.key = true
	}
	if s.reading && len(s.id) <= maxID {
		s.id = append(s.id, c)
	}
}

// maxID bounds the id kept from a dropped frame; longer ones are not answered.
const maxID = 64

// endID keeps the id just read, unless it is null, too long or not JSON.
func (s *frameScan) endID() {
	if !s.reading {
		return
	}
	s.reading = false
	if id := bytes.TrimSpace(s.id); len(s.id) <= maxID && json.Valid(id) && string(id) != "null" {
		s.msg.id = bytes.Clone(id)
	}
}
