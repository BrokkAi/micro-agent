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
	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

func (a *Agent) Prompt(parent context.Context, request schema.PromptRequest) (schema.PromptResponse, error) {
	s, err := a.lookup(request.SessionID)
	if err != nil {
		return schema.PromptResponse{}, err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	end, err := s.begin(ctx, cancel, turnWait)
	if err != nil {
		if ctx.Err() != nil { // cancelled while waiting for the turn before it
			return schema.PromptResponse{StopReason: schema.StopReasonCancelled}, nil
		}
		return schema.PromptResponse{}, err
	}
	defer end()

	updates := updater{a: a, id: s.ID}
	return a.runTurn(ctx, s, request.Prompt, a.client, updates)
}

// runTurn runs one prompt to completion with the given editor host and update
// sink. ctx must come from s.begin.
func (a *Agent) runTurn(ctx context.Context, s *session, prompt []schema.ContentBlock, host host, updates updateSink) (schema.PromptResponse, error) {
	t := &turn{a: a, s: s, client: host, updates: updates}
	if name, args, ok := parseCommand(prompt); ok {
		return t.command(ctx, name, args)
	}
	if _, _, _, _, disabled := a.providerSettings(); disabled {
		return schema.PromptResponse{}, providerDisabledError()
	}
	if !a.credentialed() {
		if !t.login(ctx, "") {
			if ctx.Err() != nil {
				return schema.PromptResponse{StopReason: schema.StopReasonCancelled}, nil
			}
			return schema.PromptResponse{}, authRequired()
		}
	}

	s.mu.Lock()
	s.Messages = append(s.Messages, userMessage(prompt))
	setTitle := s.Title == ""
	if setTitle {
		s.Title = title(prompt)
	}
	s.mu.Unlock()
	if setTitle && s.Title != "" {
		_ = updates.Update(schema.SessionUpdate{SessionInfoUpdate: &schema.SessionInfoUpdate{Title: ptr(s.Title)}})
	}
	defer a.save(s)
	return t.loop(ctx)
}

// turnWait bounds how long a prompt waits for the turn it cancels.
const turnWait = 5 * time.Second

// begin starts a turn, aborted by cancel, as the session's running turn and
// returns the func that ends it. It cancels the prompt before it; a turn still
// running, or still unwinding after session/cancel, is given wait to finish,
// so a client can cancel and prompt again straight away. While it waits, this
// prompt is the one session/cancel and close abort.
func (s *session) begin(ctx context.Context, cancel context.CancelFunc, wait time.Duration) (func(), error) {
	mine := &cancel // tells this prompt apart from a newer one
	s.mu.Lock()
	s.stop()
	s.cancel = mine
	s.mu.Unlock()
	leave := func() {
		s.mu.Lock()
		if s.cancel == mine {
			s.cancel = nil
		}
		s.mu.Unlock()
	}
	timeout := time.NewTimer(wait)
	defer timeout.Stop()
	for {
		s.mu.Lock()
		running := s.done
		if running == nil && ctx.Err() == nil {
			done := make(chan struct{})
			s.done = done
			s.mu.Unlock()
			return func() {
				s.mu.Lock()
				s.done = nil
				s.mu.Unlock()
				leave()
				close(done)
			}, nil
		}
		s.mu.Unlock()
		select {
		case <-running: // nil, so never ready, once ctx is done
		case <-ctx.Done():
			leave()
			return nil, ctx.Err()
		case <-timeout.C:
			leave()
			return nil, &acp.RPCError{Code: int(schema.ErrorCodeInvalidRequest), Message: "a prompt is already running in this session"}
		}
	}
}

func (t *turn) loop(ctx context.Context) (schema.PromptResponse, error) {
	cfg := t.a.cfg.Get()
	model := t.a.modelClient()
	for range cfg.MaxTurns {
		t.maybeCompact(ctx, model)
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
		output := r.output
		if output == "" {
			output = "(no output)"
		}
		update := &schema.ToolCallUpdate{ToolCallID: id, Status: &status, Content: r.content}
		update.RawOutput, _ = json.Marshal(output)
		if r.content == nil && r.output != "" {
			update.Content = []schema.ToolCallContent{textContent(fence(r.output))}
		}
		_ = t.updates.Update(schema.SessionUpdate{ToolCallUpdate: update})
		t.append(openrouter.Message{Role: "tool", ToolCallID: call.ID, Content: output})
	}

	act, failed := t.prepare(ctx, call.Function.Name, raw)
	if failed != nil {
		title, kind := describeCall(call.Function.Name, raw)
		_ = t.updates.Update(schema.SessionUpdate{ToolCall: &schema.ToolCall{
			ToolCallID: id, Name: &call.Function.Name, Title: title, Kind: &kind, Status: ptr(schema.ToolCallStatusPending), RawInput: raw,
		}})
		respond(*failed)
		return
	}
	_ = t.updates.Update(schema.SessionUpdate{ToolCall: &schema.ToolCall{
		ToolCallID: id,
		Name:       &call.Function.Name,
		Title:      act.title,
		Kind:       &act.kind,
		Status:     ptr(schema.ToolCallStatusPending),
		Locations:  act.locations,
		Content:    act.preview,
		RawInput:   raw,
	}})

	allowed, err := t.permit(ctx, id, call.Function.Name, act, raw)
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
func (t *turn) permit(ctx context.Context, id schema.ToolCallId, name string, act *action, raw json.RawMessage) (bool, error) {
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
			ToolCallID: id, Name: &name, Title: &act.title, Kind: &act.kind, Status: ptr(schema.ToolCallStatusPending),
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
	t.s.mu.Lock()
	t.s.contextSize = info.ContextLength
	t.s.contextUsed = usage.PromptTokens + usage.CompletionTokens
	t.s.mu.Unlock()
	_ = t.updates.Update(schema.SessionUpdate{UsageUpdate: &schema.UsageUpdate{
		Used: usage.PromptTokens + usage.CompletionTokens,
		Size: info.ContextLength,
		Cost: &schema.Cost{Amount: cost, Currency: "USD"},
	}})
}

// compactAt is the share of the model's context window that triggers
// automatic compaction: 85%.
const compactAt = 85

// maybeCompact compacts the history once the last model call used most of the
// model's context window. Failures are advisory; the turn carries on.
func (t *turn) maybeCompact(ctx context.Context, model *openrouter.Client) {
	if !t.a.cfg.Get().AutoCompactEnabled() {
		return
	}
	t.s.mu.Lock()
	used, size := t.s.contextUsed, t.s.contextSize
	t.s.mu.Unlock()
	if size <= 0 || used*100 < size*compactAt {
		return
	}
	if _, err := t.compact(ctx, model); err != nil && ctx.Err() == nil {
		_ = t.updates.Update(t.a.notice(schema.NoticeSeverityWarning, "Compacting the conversation failed", err.Error()))
	}
}

// compact replaces the conversation before the last user turn with a summary,
// so the next request fits the context window. It returns whether it applied
// a summary, and any summarization error.
func (t *turn) compact(ctx context.Context, model *openrouter.Client) (bool, error) {
	t.s.mu.Lock()
	history := t.s.Messages
	tail := -1
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role == "user" {
			tail = i
			break
		}
	}
	if tail <= 0 {
		t.s.mu.Unlock()
		return false, nil // nothing before the last user turn to summarize
	}
	older := append([]openrouter.Message(nil), history[:tail]...)
	modelID := t.s.Model
	t.s.mu.Unlock()

	id := newCompactionID()
	announce := func(update schema.SessionUpdate) { _ = t.updates.Update(update) }
	if t.a.canCompact() {
		announce(schema.SessionUpdate{CompactionUpdate: &schema.CompactionUpdate{CompactionID: id, Status: schema.CompactionStatusInProgress}})
	}
	failed := func(err error) (bool, error) {
		if t.a.canCompact() {
			status := schema.CompactionStatusFailed
			if ctx.Err() != nil {
				status = schema.CompactionStatusCancelled
			}
			announce(schema.SessionUpdate{CompactionUpdate: &schema.CompactionUpdate{CompactionID: id, Status: status, Error: ptr(err.Error())}})
		}
		return false, err
	}

	var summary strings.Builder
	result, err := model.Stream(ctx, openrouter.Request{
		Model: modelID,
		Messages: []openrouter.Message{
			{Role: "system", Content: "You summarize coding-agent conversations."},
			{Role: "user", Content: compactPrompt + transcript(older)},
		},
	}, openrouter.Handler{Text: func(delta string) error {
		summary.WriteString(delta)
		if t.a.canCompact() {
			announce(schema.SessionUpdate{CompactionSummaryChunk: &schema.CompactionSummaryChunk{CompactionID: id, Content: textBlock(delta)}})
		}
		return nil
	}})
	if err != nil {
		return failed(err)
	}
	text := strings.TrimSpace(summary.String())
	if text == "" {
		// An unstreamed reply still counts.
		if content, ok := result.Message.Content.(string); ok {
			text = strings.TrimSpace(content)
		}
	}
	if text == "" {
		return failed(errors.New("the model returned an empty summary"))
	}

	t.s.mu.Lock()
	t.s.Messages = append([]openrouter.Message{{
		Role:    "system",
		Content: "Summary of the earlier conversation (compacted):\n\n" + text,
	}}, t.s.Messages[tail:]...)
	t.s.Compaction = &compactionRecord{ID: id, Summary: text}
	t.s.contextUsed = 0
	t.s.mu.Unlock()
	_ = t.a.save(t.s)
	if t.a.canCompact() {
		announce(schema.SessionUpdate{CompactionUpdate: &schema.CompactionUpdate{
			CompactionID: id,
			Status:       schema.CompactionStatusCompleted,
			Summary:      []schema.ContentBlock{textBlock(text)},
		}})
	}
	return true, nil
}

// compactPrompt asks for a summary that lets the work continue.
const compactPrompt = `The conversation between the system and this message is a coding agent's history that no longer fits the context window. Summarize it for the agent that must continue the work. Keep: the user's requests, decisions made, files and code changed with paths, commands run and their results, errors and how they were resolved, and what is still to do. Reply with the summary only, in Markdown.

--- conversation ---
`

// transcript renders messages as plain text for compaction, which keeps the
// summarizer clear of tool-call shapes.
func transcript(messages []openrouter.Message) string {
	var b strings.Builder
	for _, m := range messages {
		switch content := m.Content.(type) {
		case string:
			fmt.Fprintf(&b, "\n%s: %s\n", m.Role, content)
		case []openrouter.Part:
			fmt.Fprintf(&b, "\n%s: ", m.Role)
			for _, part := range content {
				switch {
				case part.Text != "":
					b.WriteString(part.Text)
				case part.ImageURL != nil:
					b.WriteString("[image]")
				}
			}
			b.WriteString("\n")
		}
		if m.Reasoning != "" {
			fmt.Fprintf(&b, "(reasoning: %s)\n", m.Reasoning)
		}
		for _, call := range m.ToolCalls {
			fmt.Fprintf(&b, "tool call %s(%s)\n", call.Function.Name, call.Function.Arguments)
		}
		if m.ToolCallID != "" {
			fmt.Fprintf(&b, "tool result %s: %v\n", m.ToolCallID, m.Content)
		}
	}
	text := b.String()
	if len(text) > maxCompactTranscript {
		text = "[earlier transcript truncated]\n" + text[len(text)-maxCompactTranscript:]
	}
	return text
}

// maxCompactTranscript bounds what one summarization request carries.
const maxCompactTranscript = 400 << 10

// systemPrompt must be called with t.s.mu held.
func (t *turn) systemPrompt() string {
	cfg := t.a.cfg.Get()
	var b strings.Builder
	b.WriteString(`You are micro-agent, a minimal coding agent by Brokk, running inside the user's editor via the Agent Client Protocol.

You have five tools: shell, read_file, edit_file, write_file and update_plan, plus any tools from connected MCP servers (named mcp__<server>__<tool>).
- Use shell for searching, listing files, git, builds and tests. Write commands for the shell named below; on Windows that is usually PowerShell, so do not assume bash syntax.
- Read a file before editing it. Prefer edit_file for changes; use write_file only for new files or complete rewrites.
- For multi-step work, keep the user informed with update_plan: send the complete task list each time, with exactly one entry in_progress.
- Work autonomously until the task is done, verifying changes where practical (build, test, lint).
- Avoid destructive or irreversible commands unless the user asked for them.
- Be concise. Use Markdown. Refer to code as path:line.
`)
	fmt.Fprintf(&b, "\nEnvironment:\n- Working directory: %s\n", t.s.Cwd)
	for _, dir := range t.s.Dirs {
		fmt.Fprintf(&b, "- Additional directory: %s\n", dir)
	}
	fmt.Fprintf(&b, "- Platform: %s/%s\n- Shell tool runs: %s\n- Date: %s\n", runtime.GOOS, runtime.GOARCH, resolveShell(cfg.Shell).name(), time.Now().Format("2006-01-02"))
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
	structured := false
	for _, block := range blocks {
		switch {
		case block.Text != nil:
			parts = append(parts, openrouter.Part{Type: "text", Text: block.Text.Text})
		case block.Image != nil:
			structured = true
			url := "data:" + block.Image.MimeType + ";base64," + block.Image.Data
			if block.Image.Data == "" && block.Image.URI != nil {
				url = *block.Image.URI
			}
			parts = append(parts, openrouter.Part{Type: "image_url", ImageURL: &openrouter.ImageURL{URL: url}})
		case block.Audio != nil:
			structured = true
			if format, ok := audioFormat(block.Audio.MimeType); ok {
				parts = append(parts, openrouter.Part{Type: "input_audio", InputAudio: &openrouter.InputAudio{Data: block.Audio.Data, Format: format}})
			} else {
				parts = append(parts, openrouter.Part{Type: "text", Text: fmt.Sprintf("[Attached audio in unsupported format %s]", block.Audio.MimeType)})
			}
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
	if !structured {
		var texts []string
		for _, part := range parts {
			texts = append(texts, part.Text)
		}
		return openrouter.Message{Role: "user", Content: strings.Join(texts, "\n")}
	}
	return openrouter.Message{Role: "user", Content: parts}
}

// audioFormat maps an ACP audio MIME type to the codec name OpenRouter's
// input_audio part takes; only wav and mp3 are accepted.
func audioFormat(mimeType string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(mimeType)) {
	case "audio/wav", "audio/x-wav", "audio/wave":
		return "wav", true
	case "audio/mpeg", "audio/mp3":
		return "mp3", true
	}
	return "", false
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
