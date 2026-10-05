// Package agent implements micro-agent's ACP surface: sessions, the prompt
// loop, built-in tools, MCP tools, slash commands and configuration.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/BrokkAi/acp-go"
	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/config"
	"github.com/BrokkAi/micro-agent/internal/mcp"
)

const authMethodID = "openrouter-api-key"

// Agent serves micro-agent over ACP.
type Agent struct {
	cfg     *config.Store
	version string
	client  *clientConn // set by Serve

	mu       sync.Mutex
	sessions map[schema.SessionId]*session
	caps     schema.ClientCapabilities
	offered  *schema.AgentCapabilities // nil until initialize succeeds
}

// New returns an agent backed by the given configuration.
func New(cfg *config.Store, version string) *Agent {
	return &Agent{cfg: cfg, version: version, sessions: map[schema.SessionId]*session{}}
}

func (a *Agent) Initialize(_ context.Context, request schema.InitializeRequest) (schema.InitializeResponse, error) {
	var caps schema.ClientCapabilities
	if request.ClientCapabilities != nil {
		caps = *request.ClientCapabilities
	}
	methods := []schema.AuthMethod{{Agent: &schema.AuthMethodAgent{
		ID:          authMethodID,
		Name:        "OpenRouter API key",
		Description: ptr("Uses OPENROUTER_API_KEY or the api_key in " + a.cfg.Path() + "; set it with /login."),
	}}}
	if caps.Auth != nil && isTrue(caps.Auth.Terminal) {
		methods = append(methods, schema.AuthMethod{Terminal: &schema.AuthMethodTerminal{
			ID:   "terminal-login",
			Name: "Enter OpenRouter API key in a terminal",
			Args: []string{"login"},
		}})
	}
	response := schema.InitializeResponse{
		ProtocolVersion: schema.ProtocolVersion(acp.Version),
		AgentInfo:       &schema.Implementation{Name: "micro-agent", Title: ptr("Brokk micro-agent"), Version: a.version},
		AuthMethods:     methods,
		AgentCapabilities: &schema.AgentCapabilities{
			LoadSession:        ptr(true),
			MCPCapabilities:    &schema.McpCapabilities{HTTP: ptr(true), SSE: ptr(false)},
			PromptCapabilities: &schema.PromptCapabilities{Image: ptr(true), EmbeddedContext: ptr(true), Audio: ptr(false)},
			SessionCapabilities: &schema.SessionCapabilities{
				AdditionalDirectories: &schema.SessionAdditionalDirectoriesCapabilities{},
				Close:                 &schema.SessionCloseCapabilities{},
				Delete:                &schema.SessionDeleteCapabilities{},
				List:                  &schema.SessionListCapabilities{},
				Resume:                &schema.SessionResumeCapabilities{},
			},
			Auth: &schema.AgentAuthCapabilities{Logout: &schema.LogoutCapabilities{}},
		},
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.offered != nil {
		return schema.InitializeResponse{}, &acp.RPCError{Code: int(schema.ErrorCodeInvalidRequest), Message: "agent is already initialized"}
	}
	a.caps, a.offered = caps, response.AgentCapabilities
	return response, nil
}

func (a *Agent) Authenticate(_ context.Context, request schema.AuthenticateRequest) (schema.AuthenticateResponse, error) {
	if request.MethodID != authMethodID {
		return schema.AuthenticateResponse{}, invalidParams("unknown auth method " + string(request.MethodID))
	}
	if a.cfg.Get().APIKey == "" {
		return schema.AuthenticateResponse{}, authRequired()
	}
	return schema.AuthenticateResponse{}, nil
}

func (a *Agent) Logout(context.Context, schema.LogoutRequest) (schema.LogoutResponse, error) {
	return schema.LogoutResponse{}, a.cfg.Logout()
}

func (a *Agent) CancelSession(_ context.Context, notification schema.CancelNotification) error {
	if s, err := a.lookup(notification.SessionID); err == nil {
		s.mu.Lock()
		if s.cancel != nil {
			s.cancel()
		}
		s.mu.Unlock()
	}
	return nil
}

func (a *Agent) capable(test func(schema.ClientCapabilities) bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return test(a.caps)
}

func (a *Agent) canRead() bool {
	return a.capable(func(c schema.ClientCapabilities) bool { return c.Fs != nil && isTrue(c.Fs.ReadTextFile) })
}

func (a *Agent) canWrite() bool {
	return a.capable(func(c schema.ClientCapabilities) bool { return c.Fs != nil && isTrue(c.Fs.WriteTextFile) })
}

func (a *Agent) canTerminal() bool {
	return a.capable(func(c schema.ClientCapabilities) bool { return isTrue(c.Terminal) })
}

func (a *Agent) canForm() bool {
	return a.capable(func(c schema.ClientCapabilities) bool { return c.Elicitation != nil && c.Elicitation.Form != nil })
}

func (a *Agent) canURL() bool {
	return a.capable(func(c schema.ClientCapabilities) bool { return c.Elicitation != nil && c.Elicitation.URL != nil })
}

// Modes.

type mode struct {
	id, name, description string
}

var modes = []mode{
	{"default", "Default", "Ask before file edits, shell commands and MCP tools"},
	{"accept_edits", "Accept edits", "Edit files freely; ask before shell commands and MCP tools"},
	{"plan", "Plan", "Read-only: no file edits; ask before shell commands"},
	{"bypass", "Bypass permissions", "Never ask for permission"},
}

func validMode(id string) string {
	for _, m := range modes {
		if m.id == id {
			return id
		}
	}
	return "default"
}

func modeState(current string) *schema.SessionModeState {
	state := &schema.SessionModeState{CurrentModeID: schema.SessionModeId(current)}
	for _, m := range modes {
		state.AvailableModes = append(state.AvailableModes, schema.SessionMode{ID: schema.SessionModeId(m.id), Name: m.name, Description: ptr(m.description)})
	}
	return state
}

func (a *Agent) SetMode(ctx context.Context, request schema.SetSessionModeRequest) (schema.SetSessionModeResponse, error) {
	s, err := a.lookup(request.SessionID)
	if err != nil {
		return schema.SetSessionModeResponse{}, err
	}
	if err := a.setMode(ctx, s, string(request.ModeID), false); err != nil {
		return schema.SetSessionModeResponse{}, err
	}
	return schema.SetSessionModeResponse{}, nil
}

// setMode switches the session mode and keeps the mode config option in sync.
// announceMode also emits current_mode_update, for changes the client did not
// initiate.
func (a *Agent) setMode(ctx context.Context, s *session, id string, announceMode bool) error {
	if validMode(id) != id {
		return invalidParams("unknown mode " + id)
	}
	s.mu.Lock()
	s.Mode = id
	s.mu.Unlock()
	_ = a.save(s)
	if announceMode {
		_ = a.notify(ctx, s.ID, schema.SessionUpdate{CurrentModeUpdate: &schema.CurrentModeUpdate{CurrentModeID: schema.SessionModeId(id)}})
	}
	return a.notify(ctx, s.ID, schema.SessionUpdate{ConfigOptionUpdate: &schema.ConfigOptionUpdate{ConfigOptions: a.configOptions(s)}})
}

// Config options.

const defaultEffort = "default"

func (a *Agent) configOptions(s *session) []schema.SessionConfigOption {
	s.mu.Lock()
	current, model, effort := s.Mode, s.Model, s.Effort
	s.mu.Unlock()
	cfg := a.cfg.Get()
	cfg.Model = model

	var modeOptions, effortOptions []schema.SessionConfigSelectOption
	for _, m := range modes {
		modeOptions = append(modeOptions, schema.SessionConfigSelectOption{Value: schema.SessionConfigValueId(m.id), Name: m.name, Description: ptr(m.description)})
	}
	effortOptions = append(effortOptions, schema.SessionConfigSelectOption{Value: defaultEffort, Name: "Model default"})
	for _, level := range config.Efforts {
		effortOptions = append(effortOptions, schema.SessionConfigSelectOption{Value: schema.SessionConfigValueId(level), Name: level})
	}
	if effort == "" {
		effort = defaultEffort
	}
	category := func(c schema.SessionConfigOptionCategory) *schema.SessionConfigOptionCategory { return &c }
	return []schema.SessionConfigOption{
		{ID: "mode", Name: "Mode", Category: category(schema.SessionConfigOptionCategoryMode),
			Select: &schema.SessionConfigSelect{CurrentValue: schema.SessionConfigValueId(current), Options: modeOptions}},
		{ID: "model", Name: "Model", Category: category(schema.SessionConfigOptionCategoryModel),
			Select: &schema.SessionConfigSelect{CurrentValue: schema.SessionConfigValueId(model), Options: modelOptions(config.ModelChoices(cfg))}},
		{ID: "reasoning_effort", Name: "Reasoning effort", Category: category(schema.SessionConfigOptionCategoryThoughtLevel),
			Select: &schema.SessionConfigSelect{CurrentValue: schema.SessionConfigValueId(effort), Options: effortOptions}},
	}
}

// modelOptions groups model slugs by vendor, the part before '/', once there
// is more than one vendor to tell apart.
func modelOptions(choices []string) schema.SessionConfigSelectOptions {
	var flat []schema.SessionConfigSelectOption
	var groups []schema.SessionConfigSelectGroup
	index := map[string]int{}
	for _, choice := range choices {
		option := schema.SessionConfigSelectOption{Value: schema.SessionConfigValueId(choice), Name: choice}
		flat = append(flat, option)
		vendor, _, ok := strings.Cut(choice, "/")
		if !ok {
			vendor = "other"
		}
		i, seen := index[vendor]
		if !seen {
			i, index[vendor] = len(groups), len(groups)
			groups = append(groups, schema.SessionConfigSelectGroup{Group: schema.SessionConfigGroupId(vendor), Name: vendor})
		}
		groups[i].Options = append(groups[i].Options, option)
	}
	if len(groups) < 2 {
		return flat
	}
	return groups
}

func (a *Agent) SetConfigOption(ctx context.Context, request schema.SetSessionConfigOptionRequest) (schema.SetSessionConfigOptionResponse, error) {
	s, err := a.lookup(request.SessionID)
	if err != nil {
		return schema.SetSessionConfigOptionResponse{}, err
	}
	if request.ValueID == nil {
		return schema.SetSessionConfigOptionResponse{}, invalidParams("option " + string(request.ConfigID) + " takes a select value")
	}
	value := string(request.ValueID.Value)
	switch request.ConfigID {
	case "mode":
		if validMode(value) != value {
			return schema.SetSessionConfigOptionResponse{}, invalidParams("unknown mode " + value)
		}
		s.mu.Lock()
		s.Mode = value
		s.mu.Unlock()
		_ = a.notify(ctx, s.ID, schema.SessionUpdate{CurrentModeUpdate: &schema.CurrentModeUpdate{CurrentModeID: schema.SessionModeId(value)}})
	case "model":
		if err := a.setModel(s, value); err != nil {
			return schema.SetSessionConfigOptionResponse{}, err
		}
	case "reasoning_effort":
		if err := a.setEffort(s, value); err != nil {
			return schema.SetSessionConfigOptionResponse{}, err
		}
	default:
		return schema.SetSessionConfigOptionResponse{}, invalidParams("unknown config option " + string(request.ConfigID))
	}
	_ = a.save(s)
	return schema.SetSessionConfigOptionResponse{ConfigOptions: a.configOptions(s)}, nil
}

// setModel switches the session model and makes it the default for new sessions.
func (a *Agent) setModel(s *session, model string) error {
	model = strings.TrimSpace(model)
	if model == "" {
		return invalidParams("model is required")
	}
	s.mu.Lock()
	s.Model = model
	s.mu.Unlock()
	return a.cfg.Update(func(c *config.Config) error {
		c.Model = model
		if !slices.Contains(c.Models, model) {
			c.Models = append(c.Models, model)
		}
		return nil
	})
}

func (a *Agent) setEffort(s *session, effort string) error {
	if effort == defaultEffort {
		effort = ""
	}
	if effort != "" && !slices.Contains(config.Efforts, effort) {
		return invalidParams("unknown reasoning effort " + effort)
	}
	s.mu.Lock()
	s.Effort = effort
	s.mu.Unlock()
	return a.cfg.Update(func(c *config.Config) error {
		c.ReasoningEffort = effort
		return nil
	})
}

// MCP.

var unsafeName = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

// connectMCP connects the client-provided and globally configured servers
// concurrently. Failed servers are skipped, with a notice for the user.
func (a *Agent) connectMCP(ctx context.Context, s *session, servers []schema.McpServer) ([]*mcp.Client, []schema.SessionUpdate) {
	var configs []mcp.Config
	for _, server := range servers {
		switch {
		case server.Stdio != nil:
			env := map[string]string{}
			for _, v := range server.Stdio.Env {
				env[v.Name] = v.Value
			}
			configs = append(configs, mcp.Config{Name: server.Stdio.Name, Stdio: &mcp.StdioOptions{Command: server.Stdio.Command, Args: server.Stdio.Args, Env: env, Dir: s.Cwd}})
		case server.HTTP != nil:
			headers := map[string]string{}
			for _, h := range server.HTTP.Headers {
				headers[h.Name] = h.Value
			}
			configs = append(configs, mcp.Config{Name: server.HTTP.Name, HTTP: &mcp.HTTPOptions{URL: server.HTTP.URL, Headers: headers}})
		}
	}
	for name, server := range a.cfg.Get().MCPServers {
		switch {
		case server.Command != "":
			configs = append(configs, mcp.Config{Name: name, Stdio: &mcp.StdioOptions{Command: server.Command, Args: server.Args, Env: server.Env, Dir: s.Cwd}})
		case server.URL != "":
			configs = append(configs, mcp.Config{Name: name, HTTP: &mcp.HTTPOptions{URL: server.URL, Headers: server.Headers}})
		}
	}
	info := mcp.ClientInfo{Name: "micro-agent", Version: a.version, Roots: append([]string{s.Cwd}, s.Dirs...)}
	clients := make([]*mcp.Client, len(configs))
	failures := make([]error, len(configs))
	var wg sync.WaitGroup
	for i, c := range configs {
		wg.Go(func() {
			dial, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			connected, err := mcp.Connect(dial, c, info)
			if err == nil {
				_, err = connected.Tools(dial)
			}
			if err != nil {
				if connected != nil {
					_ = connected.Close()
				}
				fmt.Fprintf(os.Stderr, "micro-agent: MCP server %s: %v\n", c.Name, err)
				failures[i] = err
				return
			}
			clients[i] = connected
		})
	}
	wg.Wait()
	var notices []schema.SessionUpdate
	for i, err := range failures {
		if err != nil {
			notices = append(notices, schema.SessionUpdate{AgentMessageChunk: &schema.ContentChunk{
				Content: textBlock(fmt.Sprintf("⚠️ MCP server `%s` failed to start: %v\n\n", configs[i].Name, err)),
			}})
		}
	}
	return slices.DeleteFunc(clients, func(c *mcp.Client) bool { return c == nil }), notices
}

// mcpToolName maps a server tool to the model-facing name.
func mcpToolName(server, tool string) string {
	name := "mcp__" + unsafeName.ReplaceAllString(server, "_") + "__" + unsafeName.ReplaceAllString(tool, "_")
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

// Helpers.

func ptr[T any](v T) *T { return &v }

func isTrue(b *bool) bool { return b != nil && *b }

func textBlock(text string) schema.ContentBlock {
	return schema.ContentBlock{Text: &schema.TextContent{Text: text}}
}

func textContent(text string) schema.ToolCallContent {
	return schema.ToolCallContent{Content: &schema.Content{Content: textBlock(text)}}
}

func rawJSON(s string) json.RawMessage {
	if json.Valid([]byte(s)) {
		return json.RawMessage(s)
	}
	data, _ := json.Marshal(s)
	return data
}

func invalidParams(message string) error {
	return &acp.RPCError{Code: int(schema.ErrorCodeInvalidParams), Message: message}
}

func resourceNotFound(what string) error {
	return &acp.RPCError{Code: int(schema.ErrorCodeResourceNotFound), Message: what + " not found"}
}

func authRequired() error {
	return &acp.RPCError{Code: int(schema.ErrorCodeAuthenticationRequired), Message: "OpenRouter API key required: set OPENROUTER_API_KEY, run /login, or run `micro-agent login`"}
}
