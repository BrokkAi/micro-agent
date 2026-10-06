package agent

import (
	"context"
	"encoding/json"
	"fmt"

	schema1 "github.com/BrokkAi/acp-go/schema/unstable"
	schema2 "github.com/BrokkAi/acp-go/schema/v2"
	agent2 "github.com/BrokkAi/acp-go/v2/agent"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

// V2 serves the draft ACP v2 surface over the same sessions as the v1 server.
type V2 struct {
	a *Agent
}

// NewV2 returns the draft-v2 facade for a.
func NewV2(a *Agent) *V2 { return &V2{a: a} }

var (
	_ agent2.Agent              = (*V2)(nil)
	_ agent2.AuthLoginer        = (*V2)(nil)
	_ agent2.AuthLogouter       = (*V2)(nil)
	_ agent2.SessionDeleter     = (*V2)(nil)
	_ agent2.ConfigOptionSetter = (*V2)(nil)
)

func (v *V2) Initialize(_ context.Context, _ agent2.Client, request schema2.InitializeRequest) (schema2.InitializeResponse, error) {
	caps := request.Capabilities
	var methods []schema2.AuthMethod
	methods = append(methods, schema2.AuthMethod{Agent: &schema2.AuthMethodAgent{
		MethodID: authMethodID,
		Name:     "OpenRouter API key",
	}})
	if caps != nil && caps.Auth != nil && caps.Auth.Terminal != nil {
		methods = append(methods, schema2.AuthMethod{Terminal: &schema2.AuthMethodTerminal{
			MethodID: "terminal-login",
			Name:     "Enter OpenRouter API key in a terminal",
			Args:     []string{"login"},
		}})
	}
	v.a.mu.Lock()
	// Draft v2 always supports boolean config options.
	v.a.caps = schema1.ClientCapabilities{Session: &schema1.ClientSessionCapabilities{
		ConfigOptions: &schema1.SessionConfigOptionsCapabilities{Boolean: &schema1.BooleanConfigOptionCapabilities{}},
	}}
	if caps != nil {
		if caps.Elicitation != nil {
			elicitation := &schema1.ElicitationCapabilities{}
			if caps.Elicitation.Form != nil {
				elicitation.Form = &schema1.ElicitationFormCapabilities{}
			}
			if caps.Elicitation.URL != nil {
				elicitation.URL = &schema1.ElicitationUrlCapabilities{}
			}
			v.a.caps.Elicitation = elicitation
		}
		if caps.Auth != nil && caps.Auth.Terminal != nil {
			v.a.caps.Auth = &schema1.AuthCapabilities{Terminal: ptr(true)}
		}
	}
	v.a.mu.Unlock()
	return schema2.InitializeResponse{
		ProtocolVersion: 2,
		Info:            schema2.Implementation{Name: "micro-agent", Title: schema2.Nullable[string]{Set: true, Value: "Brokk micro-agent"}, Version: v.a.version},
		AuthMethods:     methods,
		Capabilities: &schema2.AgentCapabilities{
			Auth: &schema2.AgentAuthCapabilities{},
			Session: &schema2.SessionCapabilities{
				AdditionalDirectories: &schema2.SessionAdditionalDirectoriesCapabilities{},
				Delete:                &schema2.SessionDeleteCapabilities{},
				MCP:                   &schema2.McpCapabilities{HTTP: &schema2.McpHttpCapabilities{}, Stdio: &schema2.McpStdioCapabilities{}},
				Prompt: &schema2.PromptCapabilities{
					Image:           &schema2.PromptImageCapabilities{},
					Audio:           &schema2.PromptAudioCapabilities{},
					EmbeddedContext: &schema2.PromptEmbeddedContextCapabilities{},
				},
			},
		},
	}, nil
}

func (v *V2) NewSession(ctx context.Context, client agent2.Client, request schema2.NewSessionRequest) (schema2.NewSessionResponse, error) {
	servers, err := v2MCPServers(request.MCPServers)
	if err != nil {
		return schema2.NewSessionResponse{}, err
	}
	s, _, err := v.a.createSession(ctx, schema1.NewSessionRequest{
		Cwd:                   string(request.Cwd),
		AdditionalDirectories: absolutePaths(request.AdditionalDirectories),
		MCPServers:            servers,
	})
	if err != nil {
		return schema2.NewSessionResponse{}, err
	}
	options, err := v2ConfigOptions(v.a.configOptions(s))
	if err != nil {
		return schema2.NewSessionResponse{}, err
	}
	commands, err := v2Commands(commandList())
	if err != nil {
		return schema2.NewSessionResponse{}, err
	}
	session := schema2.SessionId(s.ID)
	return schema2.NewSessionResponse{SessionID: session, ConfigOptions: options, AvailableCommands: commands}, nil
}

func (v *V2) ResumeSession(ctx context.Context, client agent2.Client, request schema2.ResumeSessionRequest) (schema2.ResumeSessionResponse, error) {
	servers, err := v2MCPServers(request.MCPServers)
	if err != nil {
		return schema2.ResumeSessionResponse{}, err
	}
	s, _, err := v.a.restore(ctx, schema1.SessionId(request.SessionID), string(request.Cwd), absolutePaths(request.AdditionalDirectories), servers)
	if err != nil {
		return schema2.ResumeSessionResponse{}, err
	}
	options, err := v2ConfigOptions(v.a.configOptions(s))
	if err != nil {
		return schema2.ResumeSessionResponse{}, err
	}
	commands, err := v2Commands(commandList())
	if err != nil {
		return schema2.ResumeSessionResponse{}, err
	}
	if request.ReplayFrom != nil {
		if s, err := v.a.lookup(schema1.SessionId(request.SessionID)); err == nil {
			sink := newV2Sink(v2Sender{ctx: ctx, client: client, session: request.SessionID})
			v.replayV2(s, sink)
		}
	}
	return schema2.ResumeSessionResponse{ConfigOptions: options, AvailableCommands: commands}, nil
}

func (v *V2) CloseSession(ctx context.Context, _ agent2.Client, request schema2.CloseSessionRequest) (schema2.CloseSessionResponse, error) {
	_, err := v.a.CloseSession(ctx, schema1.CloseSessionRequest{SessionID: schema1.SessionId(request.SessionID)})
	return schema2.CloseSessionResponse{}, err
}

func (v *V2) DeleteSession(ctx context.Context, _ agent2.Client, request schema2.DeleteSessionRequest) (schema2.DeleteSessionResponse, error) {
	_, err := v.a.DeleteSession(ctx, schema1.DeleteSessionRequest{SessionID: schema1.SessionId(request.SessionID)})
	return schema2.DeleteSessionResponse{}, err
}

func (v *V2) ListSessions(ctx context.Context, _ agent2.Client, request schema2.ListSessionsRequest) (schema2.ListSessionsResponse, error) {
	listed, err := v.a.ListSessions(ctx, schema1.ListSessionsRequest{Cursor: sessionCursor(request.Cursor), Cwd: absolutePath(request.Cwd)})
	if err != nil {
		return schema2.ListSessionsResponse{}, err
	}
	response := schema2.ListSessionsResponse{Sessions: []schema2.SessionInfo{}}
	for _, info := range listed.Sessions {
		next := schema2.SessionInfo{
			SessionID:             schema2.SessionId(info.SessionID),
			Cwd:                   schema2.AbsolutePath(info.Cwd),
			AdditionalDirectories: v2AbsolutePaths(info.AdditionalDirectories),
		}
		if info.Title != nil {
			next.Title = schema2.Nullable[string]{Set: true, Value: *info.Title}
		}
		if info.UpdatedAt != nil {
			next.UpdatedAt = schema2.Nullable[string]{Set: true, Value: *info.UpdatedAt}
		}
		response.Sessions = append(response.Sessions, next)
	}
	if listed.NextCursor != nil {
		cursor := schema2.SessionListCursor(*listed.NextCursor)
		response.NextCursor = &cursor
	}
	return response, nil
}

// Prompt accepts the user message, then runs the turn in the background and
// reports idle when it stops, as draft v2 requires.
func (v *V2) Prompt(ctx context.Context, client agent2.Client, request schema2.PromptRequest, updates agent2.SessionUpdater) (schema2.PromptResponse, error) {
	s, err := v.a.lookup(schema1.SessionId(request.SessionID))
	if err != nil {
		return schema2.PromptResponse{}, err
	}
	blocks := make([]schema1.ContentBlock, 0, len(request.Prompt))
	for _, block := range request.Prompt {
		var converted schema1.ContentBlock
		if err := reencode(&block, &converted); err != nil {
			return schema2.PromptResponse{}, fmt.Errorf("prompt content: %w", err)
		}
		blocks = append(blocks, converted)
	}
	messageID := newV2MessageID()
	sink := newV2Sink(updates)
	accepted := make(chan error, 1)
	go v.runTurn(client, s, blocks, messageID, sink, accepted)
	if err := <-accepted; err != nil {
		return schema2.PromptResponse{}, err
	}
	return schema2.PromptResponse{MessageID: messageID}, nil
}

// runTurn accepts one v2 prompt: it starts the session's turn, inserts the
// user message, and then runs the shared turn loop to completion.
func (v *V2) runTurn(client agent2.Client, s *session, blocks []schema1.ContentBlock, messageID schema2.MessageId, sink *v2Sink, accepted chan<- error) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(context.Background()))
	end, err := s.begin(ctx, cancel, turnWait)
	if err != nil {
		accepted <- err
		return
	}
	defer end()

	// The client-visible insertion happens before acceptance, so the
	// response's message ID always names a message that exists.
	content := make([]schema2.ContentBlock, 0, len(blocks))
	for _, block := range blocks {
		converted, err := v2Block(block)
		if err != nil {
			accepted <- err
			return
		}
		content = append(content, converted)
	}
	_ = sink.send.Update(schema2.SessionUpdate{UserMessage: &schema2.UserMessage{
		MessageID: messageID,
		Content:   schema2.Nullable[[]schema2.ContentBlock]{Set: true, Value: content},
	}})
	for _, notice := range s.takePending() {
		_ = sink.send.Update(schema2.SessionUpdate{AgentMessageChunk: &schema2.ContentChunk{MessageID: newV2MessageID(), Content: schema2.ContentBlock{Text: &schema2.TextContent{Text: notice}}}})
	}
	_ = sink.running()
	accepted <- nil

	t := &turn{a: v.a, s: s, client: &v2Host{client: client, sink: sink}, updates: sink}
	response, err := v.a.runTurnWith(ctx, t, blocks)
	if err != nil {
		_ = sink.send.Update(schema2.SessionUpdate{AgentMessageChunk: &schema2.ContentChunk{
			MessageID: newV2MessageID(),
			Content:   schema2.ContentBlock{Text: &schema2.TextContent{Text: "Error: " + err.Error()}},
		}})
	}
	_ = sink.idle(v2StopReason(response.StopReason))
}

func (v *V2) CancelSession(notification schema2.CancelSessionNotification) error {
	return v.a.CancelSession(context.Background(), schema1.CancelNotification{SessionID: schema1.SessionId(notification.SessionID)})
}

func (v *V2) AuthLogin(_ context.Context, _ agent2.Client, request schema2.LoginAuthRequest) (schema2.LoginAuthResponse, error) {
	if request.MethodID != authMethodID {
		return schema2.LoginAuthResponse{}, invalidParams("unknown auth method " + string(request.MethodID))
	}
	if !v.a.credentialed() {
		return schema2.LoginAuthResponse{}, authRequired()
	}
	return schema2.LoginAuthResponse{}, nil
}

func (v *V2) AuthLogout(context.Context, agent2.Client, schema2.LogoutAuthRequest) (schema2.LogoutAuthResponse, error) {
	return schema2.LogoutAuthResponse{}, v.a.cfg.Logout()
}

func (v *V2) SetConfigOption(ctx context.Context, client agent2.Client, request schema2.SetSessionConfigOptionRequest) (schema2.SetSessionConfigOptionResponse, error) {
	s, err := v.a.lookup(schema1.SessionId(request.SessionID))
	if err != nil {
		return schema2.SetSessionConfigOptionResponse{}, err
	}
	var value *schema1.SessionConfigValueId
	if request.ID != nil {
		choice := schema1.SessionConfigValueId(request.ID.Value)
		value = &choice
	}
	var boolean *bool
	if request.Boolean != nil {
		boolean = &request.Boolean.Value
	}
	sink := newV2Sink(v2Sender{ctx: ctx, client: client, session: request.SessionID})
	if err := v.a.setConfigOption(s, schema1.SessionConfigId(request.ConfigID), value, boolean, sink); err != nil {
		return schema2.SetSessionConfigOptionResponse{}, err
	}
	options, err := v2ConfigOptions(v.a.configOptions(s))
	if err != nil {
		return schema2.SetSessionConfigOptionResponse{}, err
	}
	return schema2.SetSessionConfigOptionResponse{ConfigOptions: options}, nil
}

// v2Sender sends one session update in its own notification.
type v2Sender struct {
	ctx     context.Context
	client  agent2.Client
	session schema2.SessionId
}

func (s v2Sender) Update(update schema2.SessionUpdate) error {
	return s.client.Notify(s.ctx, schema2.SessionUpdateMethodName, schema2.UpdateSessionNotification{SessionID: s.session, Update: update})
}

// replayV2 streams a stored conversation as draft-v2 updates.
func (v *V2) replayV2(s *session, sink *v2Sink) {
	send := func(update schema1.SessionUpdate) { _ = sink.Update(update) }
	results := map[string]openrouter.Message{}
	s.mu.Lock()
	messages := append([]openrouter.Message(nil), s.Messages...)
	s.mu.Unlock()
	for _, m := range messages {
		if m.Role == "tool" {
			results[m.ToolCallID] = m
		}
	}
	for _, m := range messages {
		switch m.Role {
		case "user":
			for _, part := range contentParts(m.Content) {
				switch {
				case part.Type == "text":
					send(schema1.SessionUpdate{UserMessageChunk: &schema1.ContentChunk{Content: textBlock(part.Text)}})
				case part.ImageURL != nil:
					if mime, data, ok := parseDataURL(part.ImageURL.URL); ok {
						send(schema1.SessionUpdate{UserMessageChunk: &schema1.ContentChunk{Content: schema1.ContentBlock{Image: &schema1.ImageContent{MimeType: mime, Data: data}}}})
					}
				case part.InputAudio != nil:
					mime := "audio/" + part.InputAudio.Format
					if part.InputAudio.Format == "mp3" {
						mime = "audio/mpeg"
					}
					send(schema1.SessionUpdate{UserMessageChunk: &schema1.ContentChunk{Content: schema1.ContentBlock{Audio: &schema1.AudioContent{MimeType: mime, Data: part.InputAudio.Data}}}})
				}
			}
		case "assistant":
			if m.Reasoning != "" {
				send(schema1.SessionUpdate{AgentThoughtChunk: &schema1.ContentChunk{Content: textBlock(m.Reasoning)}})
			}
			if text, ok := m.Content.(string); ok && text != "" {
				send(schema1.SessionUpdate{AgentMessageChunk: &schema1.ContentChunk{Content: textBlock(text)}})
			}
			for _, call := range m.ToolCalls {
				status := schema1.ToolCallStatusCompleted
				var content []schema1.ToolCallContent
				var output json.RawMessage
				if result, ok := results[call.ID]; ok {
					if text, _ := result.Content.(string); text != "" {
						content = []schema1.ToolCallContent{textContent(fence(text))}
						output, _ = json.Marshal(text)
					}
				} else {
					status = schema1.ToolCallStatusFailed
				}
				title, kind := describeCall(call.Function.Name, json.RawMessage(call.Function.Arguments))
				send(schema1.SessionUpdate{ToolCall: &schema1.ToolCall{
					ToolCallID: schema1.ToolCallId(call.ID),
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
	_ = sink.Flush()
}

// v2StopReason maps a v1 stop reason onto draft v2.
func v2StopReason(reason schema1.StopReason) schema2.StopReason {
	switch reason {
	case schema1.StopReasonCancelled:
		return schema2.StopReasonCancelled
	case schema1.StopReasonMaxTokens:
		return schema2.StopReasonMaxTokens
	case schema1.StopReasonMaxTurnRequests:
		return schema2.StopReasonMaxTurnRequests
	case schema1.StopReasonRefusal:
		return schema2.StopReasonRefusal
	}
	return schema2.StopReasonEndTurn
}

func v2MCPServers(servers []schema2.McpServer) ([]schema1.McpServer, error) {
	converted := make([]schema1.McpServer, 0, len(servers))
	for _, server := range servers {
		// Draft v2 models stdio and HTTP; anything else (the unstable acp and
		// sse transports of the v1 schema) arrives as an extension payload and
		// is decoded as a v1 server.
		var next schema1.McpServer
		if err := reencode(&server, &next); err != nil {
			return nil, fmt.Errorf("mcp server: %w", err)
		}
		converted = append(converted, next)
	}
	return converted, nil
}

func absolutePaths(paths []schema2.AbsolutePath) []string {
	if paths == nil {
		return nil
	}
	converted := make([]string, 0, len(paths))
	for _, path := range paths {
		converted = append(converted, string(path))
	}
	return converted
}

func v2AbsolutePaths(paths []string) []schema2.AbsolutePath {
	if paths == nil {
		return nil
	}
	converted := make([]schema2.AbsolutePath, 0, len(paths))
	for _, path := range paths {
		converted = append(converted, schema2.AbsolutePath(path))
	}
	return converted
}

func absolutePath(path *schema2.AbsolutePath) *string {
	if path == nil {
		return nil
	}
	value := string(*path)
	return &value
}

func sessionCursor(cursor *schema2.SessionListCursor) *string {
	if cursor == nil {
		return nil
	}
	value := string(*cursor)
	return &value
}
