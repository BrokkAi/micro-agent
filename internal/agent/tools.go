package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	acpagent "github.com/BrokkAi/acp-go/agent"
	"github.com/BrokkAi/acp-go/schema"
	"github.com/BrokkAi/micro-agent/internal/mcp"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

// permission classes decide when a tool needs approval in each mode.
const (
	permNone    = ""
	permEdit    = "edit"
	permExecute = "execute"
	permMCP     = "mcp"
)

// action is a prepared tool call: everything needed to show it to the user,
// ask permission, and then run it.
type action struct {
	title      string
	kind       schema.ToolKind
	locations  []schema.ToolCallLocation
	preview    []schema.ToolCallContent
	permission string
	// permissionKey scopes allow_always/reject_always answers.
	permissionKey string
	run           func(ctx context.Context) toolResult
}

type toolResult struct {
	output  string // returned to the model
	content []schema.ToolCallContent
	failed  bool
}

func failure(format string, args ...any) toolResult {
	message := fmt.Sprintf(format, args...)
	return toolResult{output: "Error: " + message, content: []schema.ToolCallContent{textContent(message)}, failed: true}
}

// turn carries the per-prompt context tools need.
type turn struct {
	a       *Agent
	s       *session
	client  acpagent.Client
	updates acpagent.SessionUpdater
	mcp     map[string]mcpBinding
}

// mode reads the session mode, which can change while a turn runs.
func (t *turn) mode() string {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	return t.s.Mode
}

type mcpBinding struct {
	client *mcp.Client
	tool   mcp.Tool
}

func shellTool(sh shell) openrouter.Tool {
	return function("shell", "Run a command with "+sh.name()+" in the session's working directory and return its combined stdout/stderr and exit code. Use it for searching, listing, building, testing, git, and any other command-line work. Long output keeps the end and saves the full text to a file you can read or search.", `{
		"type": "object",
		"properties": {
			"command": {"type": "string", "description": "The command to run."},
			"timeout_seconds": {"type": "integer", "description": "Optional timeout; defaults to the configured limit."}
		},
		"required": ["command"]
	}`)
}

var builtinTools = []openrouter.Tool{
	function("read_file", "Read a text file. Returns numbered lines (cat -n style), up to 2000 lines or 50KB per call; use offset and limit to page through large files.", `{
		"type": "object",
		"properties": {
			"path": {"type": "string", "description": "Absolute path, or relative to the working directory."},
			"offset": {"type": "integer", "description": "1-based line to start from."},
			"limit": {"type": "integer", "description": "Maximum number of lines (default 2000)."}
		},
		"required": ["path"]
	}`),
	function("edit_file", "Replace an exact string in a file. old_string must match the file exactly (without read_file line-number prefixes) and be unique unless replace_all is true. Read the file first.", `{
		"type": "object",
		"properties": {
			"path": {"type": "string"},
			"old_string": {"type": "string", "description": "Exact text to replace."},
			"new_string": {"type": "string", "description": "Replacement text."},
			"replace_all": {"type": "boolean", "description": "Replace every occurrence."}
		},
		"required": ["path", "old_string", "new_string"]
	}`),
	function("write_file", "Create a file or overwrite it entirely, creating parent directories. Prefer edit_file for changes to existing files.", `{
		"type": "object",
		"properties": {
			"path": {"type": "string"},
			"content": {"type": "string"}
		},
		"required": ["path", "content"]
	}`),
}

func function(name, description, parameters string) openrouter.Tool {
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(parameters)); err != nil {
		panic(err)
	}
	return openrouter.Tool{Type: "function", Function: openrouter.Function{Name: name, Description: description, Parameters: compact.Bytes()}}
}

// tools lists the model-facing tools for the session's mode, rebuilding the
// MCP name table.
func (t *turn) tools(ctx context.Context) []openrouter.Tool {
	tools := []openrouter.Tool{shellTool(resolveShell(t.a.cfg.Get().Shell))}
	mode := t.mode()
	for _, tool := range builtinTools {
		if mode == "plan" && (tool.Function.Name == "edit_file" || tool.Function.Name == "write_file") {
			continue
		}
		tools = append(tools, tool)
	}
	t.mcp = map[string]mcpBinding{}
	t.s.mu.Lock()
	servers := t.s.servers
	t.s.mu.Unlock()
	for _, server := range servers {
		list, err := server.Tools(ctx)
		if err != nil {
			continue
		}
		for _, tool := range list {
			name := mcpToolName(server.Name, tool.Name)
			if _, taken := t.mcp[name]; taken {
				continue
			}
			t.mcp[name] = mcpBinding{client: server, tool: tool}
			parameters := tool.InputSchema
			if len(parameters) == 0 || string(parameters) == "null" {
				parameters = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			description := tool.Description
			if description == "" {
				description = tool.Title
			}
			tools = append(tools, openrouter.Tool{Type: "function", Function: openrouter.Function{
				Name:        name,
				Description: fmt.Sprintf("[MCP server %s] %s", server.Name, description),
				Parameters:  parameters,
			}})
		}
	}
	return tools
}

type shellArgs struct {
	Command        string `json:"command"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

type readArgs struct {
	Path   string `json:"path"`
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
}

type editArgs struct {
	Path       string `json:"path"`
	OldString  string `json:"old_string"`
	NewString  string `json:"new_string"`
	ReplaceAll bool   `json:"replace_all"`
}

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// describeCall produces a title and kind without side effects; used for
// replaying history.
func describeCall(name string, raw json.RawMessage) (string, schema.ToolKind) {
	var args struct {
		Command string `json:"command"`
		Path    string `json:"path"`
	}
	_ = json.Unmarshal(raw, &args)
	switch name {
	case "shell":
		return args.Command, schema.ToolKindExecute
	case "read_file":
		return "Read " + args.Path, schema.ToolKindRead
	case "edit_file":
		return "Edit " + args.Path, schema.ToolKindEdit
	case "write_file":
		return "Write " + args.Path, schema.ToolKindEdit
	}
	return name, schema.ToolKindOther
}

// prepare validates a tool call and builds its action. A non-nil toolResult
// means the call failed before it could run.
func (t *turn) prepare(ctx context.Context, name string, raw json.RawMessage) (*action, *toolResult) {
	decode := func(target any) *toolResult {
		if err := json.Unmarshal(raw, target); err != nil {
			r := failure("invalid arguments for %s: %v", name, err)
			return &r
		}
		return nil
	}
	switch name {
	case "shell":
		var args shellArgs
		if r := decode(&args); r != nil {
			return nil, r
		}
		if strings.TrimSpace(args.Command) == "" {
			r := failure("command is required")
			return nil, &r
		}
		return &action{
			title:         args.Command,
			kind:          schema.ToolKindExecute,
			preview:       []schema.ToolCallContent{textContent(fence(args.Command))},
			permission:    permExecute,
			permissionKey: "shell",
			run:           func(ctx context.Context) toolResult { return t.shell(ctx, args) },
		}, nil
	case "read_file":
		var args readArgs
		if r := decode(&args); r != nil {
			return nil, r
		}
		path := t.resolve(args.Path)
		line := uint32(max(args.Offset, 1))
		return &action{
			title:     "Read " + t.display(path),
			kind:      schema.ToolKindRead,
			locations: []schema.ToolCallLocation{{Path: path, Line: &line}},
			run:       func(ctx context.Context) toolResult { return t.read(ctx, path, args) },
		}, nil
	case "edit_file":
		if t.mode() == "plan" {
			r := failure("file edits are disabled in plan mode")
			return nil, &r
		}
		var args editArgs
		if r := decode(&args); r != nil {
			return nil, r
		}
		path := t.resolve(args.Path)
		old, err := t.readText(ctx, path)
		if err != nil {
			r := failure("%v", err)
			return nil, &r
		}
		updated, err := applyEdit(old, args)
		if err != nil {
			r := failure("%s: %v", t.display(path), err)
			return nil, &r
		}
		return t.writeAction("Edit "+t.display(path), path, &old, updated), nil
	case "write_file":
		if t.mode() == "plan" {
			r := failure("file writes are disabled in plan mode")
			return nil, &r
		}
		var args writeArgs
		if r := decode(&args); r != nil {
			return nil, r
		}
		path := t.resolve(args.Path)
		var old *string
		if _, err := os.Stat(path); err == nil {
			text, err := t.readText(ctx, path)
			if err != nil {
				r := failure("%v", err)
				return nil, &r
			}
			old = &text
		}
		return t.writeAction("Write "+t.display(path), path, old, args.Content), nil
	}
	if binding, ok := t.mcp[name]; ok {
		return t.mcpAction(binding, raw), nil
	}
	r := failure("unknown tool %q", name)
	return nil, &r
}

func (t *turn) writeAction(title, path string, old *string, updated string) *action {
	diff := schema.ToolCallContent{Diff: &schema.Diff{Path: path, OldText: old, NewText: updated}}
	return &action{
		title:         title,
		kind:          schema.ToolKindEdit,
		locations:     []schema.ToolCallLocation{{Path: path}},
		preview:       []schema.ToolCallContent{diff},
		permission:    permEdit,
		permissionKey: "edit",
		run: func(ctx context.Context) toolResult {
			if err := t.writeText(ctx, path, updated); err != nil {
				return failure("%v", err)
			}
			verb := "Updated"
			if old == nil {
				verb = "Created"
			}
			return toolResult{output: fmt.Sprintf("%s %s", verb, path), content: []schema.ToolCallContent{diff}}
		},
	}
}

func applyEdit(text string, args editArgs) (string, error) {
	if args.OldString == "" {
		return "", errors.New("old_string must not be empty; use write_file to create files")
	}
	if args.OldString == args.NewString {
		return "", errors.New("old_string and new_string are identical")
	}
	count := strings.Count(text, args.OldString)
	if count == 0 && strings.Contains(text, "\r\n") && !strings.Contains(args.OldString, "\r\n") {
		// Models usually emit LF; match CRLF files by converting the edit.
		args.OldString = strings.ReplaceAll(args.OldString, "\n", "\r\n")
		args.NewString = strings.ReplaceAll(args.NewString, "\n", "\r\n")
		count = strings.Count(text, args.OldString)
	}
	switch {
	case count == 0:
		return "", errors.New("old_string not found")
	case count > 1 && !args.ReplaceAll:
		return "", fmt.Errorf("old_string matches %d times; add surrounding context to make it unique or set replace_all", count)
	}
	if args.ReplaceAll {
		return strings.ReplaceAll(text, args.OldString, args.NewString), nil
	}
	return strings.Replace(text, args.OldString, args.NewString, 1), nil
}

func (t *turn) resolve(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return t.s.Cwd
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(t.s.Cwd, path)
	}
	return filepath.Clean(path)
}

// display shortens paths under the working directory for titles.
func (t *turn) display(path string) string {
	if rel, err := filepath.Rel(t.s.Cwd, path); err == nil && !strings.HasPrefix(rel, "..") {
		return filepath.ToSlash(rel)
	}
	return path
}

// readText reads through the client when it offers fs access, so unsaved
// editor buffers are visible, and from disk otherwise.
func (t *turn) readText(ctx context.Context, path string) (string, error) {
	if t.a.canRead() {
		response, err := t.client.ReadTextFile(ctx, schema.ReadTextFileRequest{SessionID: t.s.ID, Path: path})
		if err != nil {
			return "", fmt.Errorf("read %s: %w", path, err)
		}
		return response.Content, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (t *turn) writeText(ctx context.Context, path, content string) error {
	if t.a.canWrite() {
		_, err := t.client.WriteTextFile(ctx, schema.WriteTextFileRequest{SessionID: t.s.ID, Path: path, Content: content})
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

func (t *turn) read(ctx context.Context, path string, args readArgs) toolResult {
	text, err := t.readText(ctx, path)
	if err != nil {
		return failure("%v", err)
	}
	lines := strings.Split(strings.ReplaceAll(text, "\r\n", "\n"), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) == 0 {
		return toolResult{output: "[empty file]\n"}
	}
	start := max(args.Offset, 1) - 1
	if start >= len(lines) {
		return failure("offset %d is past the end of the file (%d lines)", args.Offset, len(lines))
	}
	limit := args.Limit
	if limit <= 0 || limit > maxOutputLines {
		limit = maxOutputLines
	}
	var out strings.Builder
	end := start
	for end < len(lines) && end-start < limit {
		line := fmt.Sprintf("%6d\t%s\n", end+1, lines[end])
		if out.Len()+len(line) > maxOutputBytes {
			if end == start {
				return failure("line %d is %s, over the %s limit; extract part of it with the shell tool", end+1, byteSize(len(lines[end])), byteSize(maxOutputBytes))
			}
			break
		}
		out.WriteString(line)
		end++
	}
	if end < len(lines) {
		fmt.Fprintf(&out, "[Showing lines %d-%d of %d. Use offset=%d to continue.]\n", start+1, end, len(lines), end+1)
	}
	return toolResult{output: out.String()}
}

func (t *turn) shell(ctx context.Context, args shellArgs) toolResult {
	cfg := t.a.cfg.Get()
	timeout := time.Duration(cfg.ShellTimeout) * time.Second
	if args.TimeoutSeconds > 0 {
		timeout = min(time.Duration(args.TimeoutSeconds)*time.Second, 30*time.Minute)
	}
	sh := resolveShell(cfg.Shell)
	if t.a.canTerminal() {
		return t.shellTerminal(ctx, sh, args.Command, timeout)
	}
	return t.shellLocal(ctx, sh, args.Command, timeout)
}

// terminalOutputLimit bounds what a client terminal retains for us.
const terminalOutputLimit = 4 << 20

// shellTerminal runs the command in a client terminal embedded in the tool call.
func (t *turn) shellTerminal(ctx context.Context, sh shell, command string, timeout time.Duration) toolResult {
	limit := uint64(terminalOutputLimit)
	cwd := t.s.Cwd
	created, err := t.client.CreateTerminal(ctx, schema.CreateTerminalRequest{
		SessionID: t.s.ID, Command: sh.path, Args: sh.args(command), Cwd: &cwd, OutputByteLimit: &limit,
	})
	if err != nil {
		return failure("create terminal: %v", err)
	}
	background := context.WithoutCancel(ctx)
	defer t.client.ReleaseTerminal(background, schema.ReleaseTerminalRequest{SessionID: t.s.ID, TerminalID: created.TerminalID})
	terminal := []schema.ToolCallContent{{Terminal: &schema.Terminal{TerminalID: created.TerminalID}}}
	_ = t.updates.Update(schema.SessionUpdate{ToolCallUpdate: &schema.ToolCallUpdate{
		ToolCallID: currentCall(ctx), Content: terminal,
	}})

	wait, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	exit, waitErr := t.client.WaitForTerminalExit(wait, schema.WaitForTerminalExitRequest{SessionID: t.s.ID, TerminalID: created.TerminalID})
	note := ""
	if waitErr != nil {
		_, _ = t.client.KillTerminal(background, schema.KillTerminalRequest{SessionID: t.s.ID, TerminalID: created.TerminalID})
		switch {
		case ctx.Err() != nil:
			note = "\n[cancelled by user]"
		case wait.Err() != nil:
			note = fmt.Sprintf("\n[timed out after %s and was killed]", timeout)
		default:
			note = fmt.Sprintf("\n[wait failed: %v]", waitErr)
		}
	}
	output, err := t.client.TerminalOutput(background, schema.TerminalOutputRequest{SessionID: t.s.ID, TerminalID: created.TerminalID})
	if err != nil {
		return failure("terminal output: %v", err)
	}
	status := exitDescription(exit.ExitCode, exit.Signal)
	if output.ExitStatus != nil && waitErr == nil {
		status = exitDescription(output.ExitStatus.ExitCode, output.ExitStatus.Signal)
	}
	text := limitOutput(output.Output, "shell", true)
	if output.Truncated {
		text = fmt.Sprintf("[The terminal kept only the last %s of output.]\n", byteSize(terminalOutputLimit)) + text
	}
	failed := waitErr != nil || exit.ExitCode == nil || *exit.ExitCode != 0
	return toolResult{output: text + note + "\n" + status, content: terminal, failed: failed}
}

// shellLocal runs the command directly, spooling output to a temp file so
// runaway commands cannot exhaust memory.
func (t *turn) shellLocal(ctx context.Context, sh shell, command string, timeout time.Duration) toolResult {
	spool, err := os.CreateTemp("", "micro-agent-shell-*.log")
	if err != nil {
		return failure("%v", err)
	}
	defer spool.Close()
	run, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(run, sh.path, sh.args(command)...)
	prepareCommand(cmd, sh, command)
	cmd.Dir = t.s.Cwd
	cmd.WaitDelay = 2 * time.Second
	cmd.Stdout = spool
	cmd.Stderr = spool
	err = cmd.Run()
	var status string
	var exitErr *exec.ExitError
	switch {
	case ctx.Err() != nil:
		status = "[cancelled by user]"
	case run.Err() != nil:
		status = fmt.Sprintf("[timed out after %s and was killed]", timeout)
	case errors.As(err, &exitErr):
		status = fmt.Sprintf("[exit code %d]", exitErr.ExitCode())
	case err != nil:
		os.Remove(spool.Name())
		return failure("run %s: %v", sh.path, err)
	default:
		status = "[exit code 0]"
	}
	result := spooled(spool) + "\n" + status
	return toolResult{output: result, content: []schema.ToolCallContent{textContent(fence(result))}, failed: err != nil}
}

// spooled returns the limited tail of a spool file. Small output is read whole
// and the file removed; output too large to load keeps the spool file as the
// full copy.
func spooled(spool *os.File) string {
	info, err := spool.Stat()
	if err != nil {
		return ""
	}
	if info.Size() <= terminalOutputLimit {
		data, _ := os.ReadFile(spool.Name())
		spool.Close()
		os.Remove(spool.Name())
		return limitOutput(string(data), "shell", true)
	}
	tail := make([]byte, maxOutputBytes)
	n, _ := spool.ReadAt(tail, info.Size()-int64(len(tail)))
	text := string(tail[:n])
	if i := strings.IndexByte(text, '\n'); i >= 0 {
		text = text[i+1:]
	}
	return fmt.Sprintf("[Showing the last %s of %s. Full output: %s]\n%s", byteSize(len(text)), byteSize(int(info.Size())), spool.Name(), text)
}

func exitDescription(code *uint32, signal *string) string {
	switch {
	case code != nil:
		return fmt.Sprintf("[exit code %d]", *code)
	case signal != nil:
		return "[killed by signal " + *signal + "]"
	}
	return "[exit status unknown]"
}

func (t *turn) mcpAction(binding mcpBinding, raw json.RawMessage) *action {
	title := binding.tool.Title
	if title == "" {
		title = binding.tool.Name
	}
	permission := permMCP
	if a := binding.tool.Annotations; a != nil && isTrue(a.ReadOnlyHint) {
		permission = permNone
	}
	return &action{
		title:         fmt.Sprintf("%s (%s)", title, binding.client.Name),
		kind:          schema.ToolKindOther,
		preview:       []schema.ToolCallContent{textContent(fence(string(raw)))},
		permission:    permission,
		permissionKey: mcpToolName(binding.client.Name, binding.tool.Name),
		run: func(ctx context.Context) toolResult {
			result, err := binding.client.CallTool(ctx, binding.tool.Name, raw)
			if err != nil {
				return failure("%v", err)
			}
			return mcpResult(result)
		},
	}
}

func mcpResult(result mcp.CallResult) toolResult {
	var text []string
	var content []schema.ToolCallContent
	for _, item := range result.Content {
		switch item.Type {
		case "text":
			text = append(text, item.Text)
			content = append(content, textContent(item.Text))
		case "image":
			text = append(text, fmt.Sprintf("[image %s shown to the user]", item.MimeType))
			content = append(content, schema.ToolCallContent{Content: &schema.Content{Content: schema.ContentBlock{Image: &schema.ImageContent{MimeType: item.MimeType, Data: item.Data}}}})
		case "resource":
			if item.Resource != nil {
				text = append(text, fmt.Sprintf("<resource uri=%q>\n%s\n</resource>", item.Resource.URI, item.Resource.Text))
				content = append(content, textContent(item.Resource.Text))
			}
		case "resource_link":
			text = append(text, "[resource link "+item.URI+"]")
			content = append(content, schema.ToolCallContent{Content: &schema.Content{Content: schema.ContentBlock{ResourceLink: &schema.ResourceLink{Name: item.URI, URI: item.URI}}}})
		default:
			text = append(text, "["+item.Type+" content]")
		}
	}
	if len(text) == 0 && len(result.StructuredContent) > 0 {
		text = append(text, string(result.StructuredContent))
		content = append(content, textContent(fence(string(result.StructuredContent))))
	}
	output := limitOutput(strings.Join(text, "\n"), "mcp", false)
	if result.IsError {
		output = "Error: " + output
	}
	return toolResult{output: output, content: content, failed: result.IsError}
}

func fence(s string) string {
	marker := "```"
	for strings.Contains(s, marker) {
		marker += "`"
	}
	return marker + "\n" + strings.TrimRight(s, "\n") + "\n" + marker
}

type callKey struct{}

// currentCall recovers the tool call ID for updates issued while running.
func currentCall(ctx context.Context) schema.ToolCallId {
	id, _ := ctx.Value(callKey{}).(schema.ToolCallId)
	return id
}
