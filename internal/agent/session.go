package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/mcp"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

// record is the persisted form of a session.
type record struct {
	ID        schema.SessionId     `json:"id"`
	Cwd       string               `json:"cwd"`
	Dirs      []string             `json:"additional_directories,omitempty"`
	Title     string               `json:"title,omitempty"`
	UpdatedAt time.Time            `json:"updated_at"`
	Mode      string               `json:"mode"`
	Model     string               `json:"model"`
	Effort    string               `json:"effort,omitempty"`
	Cost      float64              `json:"cost,omitempty"`
	Messages  []openrouter.Message `json:"messages"`
}

type session struct {
	record

	mu sync.Mutex
	// done is closed when the running prompt turn ends; cancel aborts it.
	done    chan struct{}
	cancel  context.CancelFunc
	servers []*mcp.Client
	// decisions remembers allow_always/reject_always answers by permission key.
	decisions map[string]bool
	// deleted stops a still-running turn from re-saving a deleted session.
	deleted bool
}

func newSessionID() schema.SessionId {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return schema.SessionId(hex.EncodeToString(b[:]))
}

func (a *Agent) sessionDir() string { return filepath.Join(a.cfg.Dir(), "sessions") }

func (a *Agent) sessionPath(id schema.SessionId) (string, error) {
	name := string(id)
	if name == "" || strings.ContainsAny(name, `/\.:`) {
		return "", fmt.Errorf("invalid session id %q", name)
	}
	return filepath.Join(a.sessionDir(), name+".json"), nil
}

// save writes the session record. It holds s.mu throughout so saves are
// serialized and cannot race with deletion.
func (a *Agent) save(s *session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deleted {
		return nil
	}
	s.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(s.record)
	if err != nil {
		return err
	}
	path, err := a.sessionPath(s.ID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (a *Agent) loadRecord(id schema.SessionId) (record, error) {
	var r record
	path, err := a.sessionPath(id)
	if err != nil {
		return r, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return r, resourceNotFound("session " + string(id))
	}
	if err != nil {
		return r, err
	}
	return r, json.Unmarshal(data, &r)
}

func (a *Agent) lookup(id schema.SessionId) (*session, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.sessions[id]
	if s == nil {
		return nil, resourceNotFound("session " + string(id))
	}
	return s, nil
}

// open registers s and connects its MCP servers. It returns notices about
// servers that failed, for the caller to announce.
func (a *Agent) open(ctx context.Context, s *session, servers []schema.McpServer) []schema.SessionUpdate {
	s.decisions = map[string]bool{}
	var notices []schema.SessionUpdate
	s.servers, notices = a.connectMCP(ctx, s, servers)
	a.mu.Lock()
	if old := a.sessions[s.ID]; old != nil {
		a.shutdown(old)
	}
	a.sessions[s.ID] = s
	a.mu.Unlock()
	return notices
}

// shutdown cancels work and releases MCP connections.
func (a *Agent) shutdown(s *session) {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	servers := s.servers
	s.servers = nil
	s.mu.Unlock()
	for _, server := range servers {
		_ = server.Close()
	}
}

func (a *Agent) NewSession(ctx context.Context, request schema.NewSessionRequest) (schema.NewSessionResponse, error) {
	if err := checkRoots(request.Cwd, request.AdditionalDirectories); err != nil {
		return schema.NewSessionResponse{}, err
	}
	cfg := a.cfg.Get()
	s := &session{record: record{
		ID:     newSessionID(),
		Cwd:    request.Cwd,
		Dirs:   request.AdditionalDirectories,
		Mode:   validMode(cfg.DefaultMode),
		Model:  cfg.Model,
		Effort: cfg.ReasoningEffort,
	}}
	notices := a.open(ctx, s, request.MCPServers)
	// The client learns the session ID from the response, so its updates go
	// out right behind it.
	a.announce(s, notices, updater{a: a, id: s.ID, queued: true})
	return schema.NewSessionResponse{
		SessionID:     s.ID,
		Modes:         modeState(s.Mode),
		ConfigOptions: a.configOptions(s),
	}, nil
}

func (a *Agent) LoadSession(ctx context.Context, request schema.LoadSessionRequest) (schema.LoadSessionResponse, error) {
	s, notices, err := a.restore(ctx, request.SessionID, request.Cwd, request.AdditionalDirectories, request.MCPServers)
	if err != nil {
		return schema.LoadSessionResponse{}, err
	}
	a.replay(ctx, s)
	a.announce(s, notices, updater{a: a, id: s.ID})
	return schema.LoadSessionResponse{Modes: modeState(s.Mode), ConfigOptions: a.configOptions(s)}, nil
}

func (a *Agent) ResumeSession(ctx context.Context, request schema.ResumeSessionRequest) (schema.ResumeSessionResponse, error) {
	s, notices, err := a.restore(ctx, request.SessionID, request.Cwd, request.AdditionalDirectories, request.MCPServers)
	if err != nil {
		return schema.ResumeSessionResponse{}, err
	}
	a.announce(s, notices, updater{a: a, id: s.ID})
	return schema.ResumeSessionResponse{Modes: modeState(s.Mode), ConfigOptions: a.configOptions(s)}, nil
}

func (a *Agent) restore(ctx context.Context, id schema.SessionId, cwd string, dirs []string, servers []schema.McpServer) (*session, []schema.SessionUpdate, error) {
	if err := checkRoots(cwd, dirs); err != nil {
		return nil, nil, err
	}
	r, err := a.loadRecord(id)
	if err != nil {
		return nil, nil, err
	}
	// The request sets the roots: no additional directories means none, not
	// the ones saved last time.
	r.Cwd, r.Dirs = cwd, dirs
	r.Mode = validMode(r.Mode)
	s := &session{record: r}
	return s, a.open(ctx, s, servers), nil
}

// checkRoots rejects a session cwd or additional directory that is not an
// absolute path.
func checkRoots(cwd string, dirs []string) error {
	if !filepath.IsAbs(cwd) {
		return invalidParams("cwd must be an absolute path")
	}
	for _, dir := range dirs {
		if !filepath.IsAbs(dir) {
			return invalidParams(fmt.Sprintf("additional directory %q must be an absolute path", dir))
		}
	}
	return nil
}

func (a *Agent) CloseSession(_ context.Context, request schema.CloseSessionRequest) (schema.CloseSessionResponse, error) {
	a.mu.Lock()
	s := a.sessions[request.SessionID]
	delete(a.sessions, request.SessionID)
	a.mu.Unlock()
	if s == nil {
		return schema.CloseSessionResponse{}, resourceNotFound("session " + string(request.SessionID))
	}
	a.shutdown(s)
	return schema.CloseSessionResponse{}, nil
}

func (a *Agent) DeleteSession(_ context.Context, request schema.DeleteSessionRequest) (schema.DeleteSessionResponse, error) {
	a.mu.Lock()
	s := a.sessions[request.SessionID]
	delete(a.sessions, request.SessionID)
	a.mu.Unlock()
	if s != nil {
		a.shutdown(s)
		s.mu.Lock()
		s.deleted = true
		defer s.mu.Unlock()
	}
	path, err := a.sessionPath(request.SessionID)
	if err != nil {
		return schema.DeleteSessionResponse{}, err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return schema.DeleteSessionResponse{}, err
	}
	return schema.DeleteSessionResponse{}, nil
}

const listPageSize = 50

func (a *Agent) ListSessions(_ context.Context, request schema.ListSessionsRequest) (schema.ListSessionsResponse, error) {
	entries, err := os.ReadDir(a.sessionDir())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return schema.ListSessionsResponse{}, err
	}
	var records []record
	for _, entry := range entries {
		id, ok := strings.CutSuffix(entry.Name(), ".json")
		if !ok {
			continue
		}
		r, err := a.loadRecord(schema.SessionId(id))
		if err != nil {
			continue
		}
		if request.Cwd != nil && filepath.Clean(r.Cwd) != filepath.Clean(*request.Cwd) {
			continue
		}
		records = append(records, r)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].UpdatedAt.After(records[j].UpdatedAt) })
	start := 0
	if request.Cursor != nil {
		start, err = strconv.Atoi(*request.Cursor)
		if err != nil || start < 0 || start > len(records) {
			return schema.ListSessionsResponse{}, invalidParams("invalid cursor")
		}
	}
	end := min(start+listPageSize, len(records))
	response := schema.ListSessionsResponse{Sessions: []schema.SessionInfo{}}
	for _, r := range records[start:end] {
		updated := r.UpdatedAt.Format(time.RFC3339)
		info := schema.SessionInfo{SessionID: r.ID, Cwd: r.Cwd, AdditionalDirectories: r.Dirs, UpdatedAt: &updated}
		if r.Title != "" {
			info.Title = ptr(r.Title)
		}
		response.Sessions = append(response.Sessions, info)
	}
	if end < len(records) {
		response.NextCursor = ptr(strconv.Itoa(end))
	}
	return response, nil
}

// replay streams a restored conversation back to the client as session updates.
func (a *Agent) replay(ctx context.Context, s *session) {
	send := func(update schema.SessionUpdate) { _ = a.notify(ctx, s.ID, update) }
	results := map[string]openrouter.Message{}
	for _, m := range s.Messages {
		if m.Role == "tool" {
			results[m.ToolCallID] = m
		}
	}
	for _, m := range s.Messages {
		switch m.Role {
		case "user":
			for _, part := range contentParts(m.Content) {
				switch {
				case part.Type == "text":
					send(schema.SessionUpdate{UserMessageChunk: &schema.ContentChunk{Content: textBlock(part.Text)}})
				case part.ImageURL != nil:
					if mime, data, ok := parseDataURL(part.ImageURL.URL); ok {
						send(schema.SessionUpdate{UserMessageChunk: &schema.ContentChunk{Content: schema.ContentBlock{Image: &schema.ImageContent{MimeType: mime, Data: data}}}})
					}
				}
			}
		case "assistant":
			if m.Reasoning != "" {
				send(schema.SessionUpdate{AgentThoughtChunk: &schema.ContentChunk{Content: textBlock(m.Reasoning)}})
			}
			if text, ok := m.Content.(string); ok && text != "" {
				send(schema.SessionUpdate{AgentMessageChunk: &schema.ContentChunk{Content: textBlock(text)}})
			}
			for _, call := range m.ToolCalls {
				status := schema.ToolCallStatusCompleted
				var content []schema.ToolCallContent
				var output json.RawMessage
				if result, ok := results[call.ID]; ok {
					if text, _ := result.Content.(string); text != "" {
						content = []schema.ToolCallContent{textContent(fence(text))}
						output, _ = json.Marshal(text)
					}
				} else {
					status = schema.ToolCallStatusFailed
				}
				title, kind := describeCall(call.Function.Name, json.RawMessage(call.Function.Arguments))
				send(schema.SessionUpdate{ToolCall: &schema.ToolCall{
					ToolCallID: schema.ToolCallId(call.ID),
					Name:       ptr(call.Function.Name),
					Title:      title,
					Kind:       &kind,
					Status:     &status,
					RawInput:   rawJSON(call.Function.Arguments),
					RawOutput:  output,
					Content:    content,
				}})
			}
		}
	}
}

// announce publishes the command list, title and MCP notices for a session.
func (a *Agent) announce(s *session, notices []schema.SessionUpdate, updates updater) {
	_ = updates.Update(schema.SessionUpdate{AvailableCommandsUpdate: &schema.AvailableCommandsUpdate{AvailableCommands: commandList()}})
	if s.Title != "" {
		_ = updates.Update(schema.SessionUpdate{SessionInfoUpdate: &schema.SessionInfoUpdate{Title: ptr(s.Title)}})
	}
	for _, notice := range notices {
		_ = updates.Update(notice)
	}
}
