package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/BrokkAi/acp-go"
	acpagent "github.com/BrokkAi/acp-go/agent"
	"github.com/BrokkAi/acp-go/schema"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

func (a *Agent) Prompt(parent context.Context, client acpagent.Client, request schema.PromptRequest, updates acpagent.SessionUpdater) (schema.PromptResponse, error) {
	s, err := a.lookup(request.SessionID)
	if err != nil {
		return schema.PromptResponse{}, err
	}
	if !s.running.TryLock() {
		return schema.PromptResponse{}, &acp.RPCError{Code: int(schema.ErrorCodeInvalidRequest), Message: "a prompt is already running in this session"}
	}
	defer s.running.Unlock()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.cancel = nil
		s.mu.Unlock()
	}()

	t := &turn{a: a, s: s, client: client, updates: updates}
	if name, args, ok := parseCommand(request.Prompt); ok {
		return t.command(ctx, name, args)
	}
	if a.cfg.Get().APIKey == "" {
		if !t.login(ctx, "") {
			if ctx.Err() != nil {
				return schema.PromptResponse{StopReason: schema.StopReasonCancelled}, nil
			}
			return schema.PromptResponse{}, authRequired()
		}
	}

	s.mu.Lock()
	s.Messages = append(s.Messages, userMessage(request.Prompt))
	setTitle := s.Title == ""
	if setTitle {
		s.Title = title(request.Prompt)
	}
	s.mu.Unlock()
	if setTitle && s.Title != "" {
		_ = updates.Update(schema.SessionUpdate{SessionInfoUpdate: &schema.SessionInfoUpdate{Title: ptr(s.Title)}})
	}
	defer a.save(s)
	return t.loop(ctx)
}

func (t *turn) loop(ctx context.Context) (schema.PromptResponse, error) {
	cfg := t.a.cfg.Get()
	model := &openrouter.Client{BaseURL: cfg.BaseURL, APIKey: cfg.APIKey}
	for range cfg.MaxTurns {
		tools := t.tools(ctx)
		t.s.mu.Lock()
		request := openrouter.Request{
			Model:     t.s.Model,
			Messages:  append([]openrouter.Message{{Role: "system", Content: t.systemPrompt()}}, t.s.Messages...),
			Tools:     tools,
			MaxTokens: cfg.MaxTokens,
		}
		if t.s.Effort != "" {
			request.Reasoning = &openrouter.Reasoning{Effort: t.s.Effort}
		}
		t.s.mu.Unlock()

		result, err := model.Stream(ctx, request, openrouter.Handler{
			Text: func(delta string) error {
				return t.updates.Update(schema.SessionUpdate{AgentMessageChunk: &schema.ContentChunk{Content: textBlock(delta)}})
			},
			Reasoning: func(delta string) error {
				return t.updates.Update(schema.SessionUpdate{AgentThoughtChunk: &schema.ContentChunk{Content: textBlock(delta)}})
			},
		})
		if ctx.Err() != nil {
			// Keep any partial answer, but drop unfinished tool calls.
			if text, ok := result.Message.Content.(string); ok && text != "" {
				result.Message.ToolCalls = nil
				t.append(result.Message)
			}
			return schema.PromptResponse{StopReason: schema.StopReasonCancelled}, nil
		}
		if err != nil {
			var status *openrouter.StatusError
			if errors.As(err, &status) && status.Status == 401 {
				return schema.PromptResponse{}, authRequired()
			}
			return schema.PromptResponse{}, err
		}
		t.append(result.Message)
		t.reportUsage(ctx, model, result.Usage)

		if len(result.Message.ToolCalls) == 0 {
			switch result.FinishReason {
			case "length":
				return schema.PromptResponse{StopReason: schema.StopReasonMaxTokens}, nil
			case "content_filter":
				return schema.PromptResponse{StopReason: schema.StopReasonRefusal}, nil
			}
			return schema.PromptResponse{StopReason: schema.StopReasonEndTurn}, nil
		}
		for i, call := range result.Message.ToolCalls {
			if ctx.Err() == nil {
				t.call(ctx, call)
			}
			if ctx.Err() != nil {
				// Every tool call needs a result for the next request to be valid.
				for _, skipped := range result.Message.ToolCalls[i:] {
					if !t.answered(skipped.ID) {
						t.append(openrouter.Message{Role: "tool", ToolCallID: skipped.ID, Content: "Cancelled by the user."})
					}
				}
				return schema.PromptResponse{StopReason: schema.StopReasonCancelled}, nil
			}
		}
		_ = t.a.save(t.s)
	}
	return schema.PromptResponse{StopReason: schema.StopReasonMaxTurnRequests}, nil
}

func (t *turn) append(m openrouter.Message) {
	t.s.mu.Lock()
	t.s.Messages = append(t.s.Messages, m)
	t.s.mu.Unlock()
}

func (t *turn) answered(id string) bool {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	for i := len(t.s.Messages) - 1; i >= 0; i-- {
		m := t.s.Messages[i]
		if m.Role != "tool" {
			return false
		}
		if m.ToolCallID == id {
			return true
		}
	}
	return false
}

// call runs one tool call end to end: announce, ask permission, execute,
// report, and record the result for the model.
func (t *turn) call(ctx context.Context, call openrouter.ToolCall) {
	id := schema.ToolCallId(call.ID)
	ctx = context.WithValue(ctx, callKey{}, id)
	raw := rawJSON(call.Function.Arguments)
	respond := func(r toolResult) {
		status := schema.ToolCallStatusCompleted
		if r.failed {
			status = schema.ToolCallStatusFailed
		}
		update := &schema.ToolCallUpdate{ToolCallID: id, Status: &status, Content: r.content}
		if r.content == nil && r.output != "" {
			update.Content = []schema.ToolCallContent{textContent(fence(r.output))}
		}
		_ = t.updates.Update(schema.SessionUpdate{ToolCallUpdate: update})
		output := r.output
		if output == "" {
			output = "(no output)"
		}
		t.append(openrouter.Message{Role: "tool", ToolCallID: call.ID, Content: output})
	}

	act, failed := t.prepare(ctx, call.Function.Name, raw)
	if failed != nil {
		title, kind := describeCall(call.Function.Name, raw)
		_ = t.updates.Update(schema.SessionUpdate{ToolCall: &schema.ToolCall{
			ToolCallID: id, Title: title, Kind: &kind, Status: ptr(schema.ToolCallStatusPending), RawInput: raw,
		}})
		respond(*failed)
		return
	}
	_ = t.updates.Update(schema.SessionUpdate{ToolCall: &schema.ToolCall{
		ToolCallID: id,
		Title:      act.title,
		Kind:       &act.kind,
		Status:     ptr(schema.ToolCallStatusPending),
		Locations:  act.locations,
		Content:    act.preview,
		RawInput:   raw,
	}})

	allowed, err := t.permit(ctx, id, act, raw)
	if err != nil {
		respond(failure("permission request failed: %v", err))
		return
	}
	if !allowed {
		if ctx.Err() != nil {
			respond(toolResult{output: "Cancelled by the user.", failed: true})
			return
		}
		respond(toolResult{output: "The user denied permission for this tool call. Do not retry it; ask the user how to proceed if needed.", content: []schema.ToolCallContent{textContent("Permission denied")}, failed: true})
		return
	}
	_ = t.updates.Update(schema.SessionUpdate{ToolCallUpdate: &schema.ToolCallUpdate{ToolCallID: id, Status: ptr(schema.ToolCallStatusInProgress)}})
	respond(act.run(ctx))
}

// permit decides whether act may run, asking the client when the mode requires it.
func (t *turn) permit(ctx context.Context, id schema.ToolCallId, act *action, raw json.RawMessage) (bool, error) {
	t.s.mu.Lock()
	mode := t.s.Mode
	decision, decided := t.s.decisions[act.permissionKey]
	t.s.mu.Unlock()
	switch {
	case act.permission == permNone || mode == "bypass":
		return true, nil
	case act.permission == permEdit && mode == "accept_edits":
		return true, nil
	case decided:
		return decision, nil
	}
	response, err := t.client.RequestPermission(ctx, schema.RequestPermissionRequest{
		SessionID: t.s.ID,
		ToolCall: schema.ToolCallUpdate{
			ToolCallID: id, Title: &act.title, Kind: &act.kind, Status: ptr(schema.ToolCallStatusPending),
			Content: act.preview, Locations: act.locations, RawInput: raw,
		},
		Options: []schema.PermissionOption{
			{OptionID: "allow_once", Name: "Allow", Kind: schema.PermissionOptionKindAllowOnce},
			{OptionID: "allow_always", Name: "Always allow", Kind: schema.PermissionOptionKindAllowAlways},
			{OptionID: "reject_once", Name: "Reject", Kind: schema.PermissionOptionKindRejectOnce},
			{OptionID: "reject_always", Name: "Always reject", Kind: schema.PermissionOptionKindRejectAlways},
		},
	})
	if ctx.Err() != nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if response.Outcome.Selected == nil {
		return false, nil // cancelled
	}
	switch response.Outcome.Selected.OptionID {
	case "allow_once":
		return true, nil
	case "allow_always", "reject_always":
		allow := response.Outcome.Selected.OptionID == "allow_always"
		t.s.mu.Lock()
		t.s.decisions[act.permissionKey] = allow
		t.s.mu.Unlock()
		return allow, nil
	}
	return false, nil
}

func (t *turn) reportUsage(ctx context.Context, model *openrouter.Client, usage *openrouter.Usage) {
	if usage == nil {
		return
	}
	t.s.mu.Lock()
	t.s.Cost += usage.Cost
	cost, modelID := t.s.Cost, t.s.Model
	t.s.mu.Unlock()
	info, ok := model.Model(ctx, modelID)
	if !ok || info.ContextLength == 0 {
		return
	}
	_ = t.updates.Update(schema.SessionUpdate{UsageUpdate: &schema.UsageUpdate{
		Used: usage.PromptTokens + usage.CompletionTokens,
		Size: info.ContextLength,
		Cost: &schema.Cost{Amount: cost, Currency: "USD"},
	}})
}

// systemPrompt must be called with t.s.mu held.
func (t *turn) systemPrompt() string {
	cfg := t.a.cfg.Get()
	var b strings.Builder
	b.WriteString(`You are micro-agent, a minimal coding agent by Brokk, running inside the user's editor via the Agent Client Protocol.

You have four tools: shell, read_file, edit_file and write_file, plus any tools from connected MCP servers (named mcp__<server>__<tool>).
- Use shell for searching, listing files, git, builds and tests. Write commands for the shell named below; on Windows that is usually PowerShell, so do not assume bash syntax.
- Read a file before editing it. Prefer edit_file for changes; use write_file only for new files or complete rewrites.
- Work autonomously until the task is done, verifying changes where practical (build, test, lint).
- Avoid destructive or irreversible commands unless the user asked for them.
- Be concise. Use Markdown. Refer to code as path:line.
`)
	fmt.Fprintf(&b, "\nEnvironment:\n- Working directory: %s\n", t.s.Cwd)
	for _, dir := range t.s.Dirs {
		fmt.Fprintf(&b, "- Additional directory: %s\n", dir)
	}
	fmt.Fprintf(&b, "- Platform: %s/%s\n- Shell for bash tool: %s\n- Date: %s\n", runtime.GOOS, runtime.GOARCH, cfg.Shell, time.Now().Format("2006-01-02"))
	if t.s.Mode == "plan" {
		b.WriteString("\nPLAN MODE: you are read-only. Do not modify files or run commands with side effects. Investigate, then present a concrete plan for the user to approve.\n")
	}
	if data, err := os.ReadFile(filepath.Join(t.s.Cwd, "AGENTS.md")); err == nil {
		fmt.Fprintf(&b, "\nProject instructions (AGENTS.md):\n%s\n", data)
	}
	for _, server := range t.s.servers {
		if server.Instructions != "" {
			fmt.Fprintf(&b, "\nInstructions from MCP server %s:\n%s\n", server.Name, server.Instructions)
		}
	}
	if cfg.SystemPrompt != "" {
		fmt.Fprintf(&b, "\n%s\n", cfg.SystemPrompt)
	}
	return b.String()
}

// userMessage converts ACP prompt blocks to an OpenRouter user message.
func userMessage(blocks []schema.ContentBlock) openrouter.Message {
	var parts []openrouter.Part
	hasImage := false
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			parts = append(parts, openrouter.Part{Type: "text", Text: block.Text.Text})
		case block.Image != nil:
			hasImage = true
			url := "data:" + block.Image.MimeType + ";base64," + block.Image.Data
			if block.Image.Data == "" && block.Image.URI != nil {
				url = *block.Image.URI
			}
			parts = append(parts, openrouter.Part{Type: "image_url", ImageURL: &openrouter.ImageURL{URL: url}})
		case block.ResourceLink != nil:
			parts = append(parts, openrouter.Part{Type: "text", Text: fmt.Sprintf("[Referenced: %s (%s)]", block.ResourceLink.Name, uriPath(block.ResourceLink.URI))})
		case block.Resource != nil:
			if text := block.Resource.Resource.TextResourceContents; text != nil {
				parts = append(parts, openrouter.Part{Type: "text", Text: fmt.Sprintf("<context uri=%q>\n%s\n</context>", uriPath(text.URI), text.Text)})
			} else if blob := block.Resource.Resource.BlobResourceContents; blob != nil {
				parts = append(parts, openrouter.Part{Type: "text", Text: fmt.Sprintf("[Attached binary resource: %s]", blob.URI)})
			}
		}
	}
	if !hasImage {
		var texts []string
		for _, part := range parts {
			texts = append(texts, part.Text)
		}
		return openrouter.Message{Role: "user", Content: strings.Join(texts, "\n")}
	}
	return openrouter.Message{Role: "user", Content: parts}
}

func uriPath(uri string) string {
	if path, ok := strings.CutPrefix(uri, "file://"); ok {
		return path
	}
	return uri
}

// contentParts normalizes stored message content (a string or decoded parts).
func contentParts(content any) []openrouter.Part {
	switch c := content.(type) {
	case string:
		return []openrouter.Part{{Type: "text", Text: c}}
	case []openrouter.Part:
		return c
	case nil:
		return nil
	}
	data, _ := json.Marshal(content)
	var parts []openrouter.Part
	_ = json.Unmarshal(data, &parts)
	return parts
}

func parseDataURL(url string) (mime, data string, ok bool) {
	rest, ok := strings.CutPrefix(url, "data:")
	if !ok {
		return "", "", false
	}
	mime, data, ok = strings.Cut(rest, ";base64,")
	return mime, data, ok
}

func title(blocks []schema.ContentBlock) string {
	for _, block := range blocks {
		if block.Text == nil {
			continue
		}
		text := strings.Join(strings.Fields(block.Text.Text), " ")
		if text == "" {
			continue
		}
		if runes := []rune(text); len(runes) > 60 {
			text = string(runes[:60]) + "…"
		}
		return text
	}
	return ""
}
