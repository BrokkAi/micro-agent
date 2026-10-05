package agent

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/config"
)

// toolOutput is the last tool result the model saw in request n.
func toolOutput(h *harness, n int) string {
	h.t.Helper()
	messages := h.router.requests[n]["messages"].([]any)
	return messages[len(messages)-1].(map[string]any)["content"].(string)
}

// callUpdates returns the tool_call and the final tool_call_update for id.
func callUpdates(h *harness, id schema.ToolCallId) (*schema.ToolCall, *schema.ToolCallUpdate) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var call *schema.ToolCall
	var final *schema.ToolCallUpdate
	for _, n := range h.updates {
		if u := n.Update.ToolCall; u != nil && u.ToolCallID == id {
			call = u
		}
		if u := n.Update.ToolCallUpdate; u != nil && u.ToolCallID == id && u.Status != nil && *u.Status != schema.ToolCallStatusInProgress {
			final = u
		}
	}
	return call, final
}

// onTerminal answers terminal calls for one terminal. wait runs inside
// terminal/wait_for_exit, before it reports exit code 0.
func onTerminal(h *harness, wait func()) {
	h.on(schema.TerminalCreateMethodName, func(json.RawMessage) (any, error) {
		return schema.CreateTerminalResponse{TerminalID: "t1"}, nil
	})
	h.on(schema.TerminalWaitForExitMethodName, func(json.RawMessage) (any, error) {
		wait()
		return schema.WaitForTerminalExitResponse{ExitCode: ptr(uint32(0))}, nil
	})
	h.on(schema.TerminalOutputMethodName, func(json.RawMessage) (any, error) {
		return schema.TerminalOutputResponse{Output: "ok\n"}, nil
	})
	for _, method := range []string{schema.TerminalKillMethodName, schema.TerminalReleaseMethodName} {
		h.on(method, func(json.RawMessage) (any, error) { return struct{}{}, nil })
	}
}

func TestShellEnvironment(t *testing.T) {
	configure := func(h *harness) {
		if err := h.store.Update(func(c *config.Config) error {
			c.ShellEnv = map[string]string{"GIT_PAGER": "less", "MICRO_TEST": "x y"}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		h.answer = "allow_always"
	}

	t.Run("terminal", func(t *testing.T) {
		h := newHarness(t, schema.ClientCapabilities{Terminal: ptr(true)})
		configure(h)
		onTerminal(h, func() {})
		var created schema.CreateTerminalRequest
		h.on(schema.TerminalCreateMethodName, func(raw json.RawMessage) (any, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			return schema.CreateTerminalResponse{TerminalID: "t1"}, json.Unmarshal(raw, &created)
		})
		h.router.responses = [][]string{toolCallChunks("call_1", "shell", map[string]string{"command": "git log"}), textChunks("done")}
		h.prompt(h.newSession(), "run it")
		want := []string{"GIT_EDITOR=true", "GIT_PAGER=less", "GIT_TERMINAL_PROMPT=0", "MICRO_TEST=x y"}
		if runtime.GOOS != "windows" {
			want = append(want, "PAGER=cat")
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		var env []string
		for _, v := range created.Env {
			env = append(env, v.Name+"="+v.Value)
		}
		if !slices.Equal(env, want) {
			t.Errorf("env = %q, want %q", env, want)
		}
		if created.OutputByteLimit == nil || *created.OutputByteLimit != 1<<20 {
			t.Errorf("output byte limit = %v", created.OutputByteLimit)
		}
	})

	t.Run("local", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("POSIX shell syntax")
		}
		t.Setenv("PAGER", "less")
		h := newHarness(t, schema.ClientCapabilities{})
		configure(h)
		h.router.responses = [][]string{
			toolCallChunks("call_1", "shell", map[string]string{"command": `echo "$GIT_PAGER|$GIT_TERMINAL_PROMPT|$GIT_EDITOR|$PAGER|$MICRO_TEST|${HOME:+home}"`}),
			textChunks("done"),
		}
		h.prompt(h.newSession(), "run it")
		if output := toolOutput(h, 1); !strings.Contains(output, "less|0|true|cat|x y|home") {
			t.Fatalf("shell output = %q", output)
		}
	})
}

// serveFiles answers fs/read_text_file from disk, honoring line and limit, and
// records each request. Files named in virtual are served from memory instead.
func serveFiles(h *harness, virtual map[string]string) *[]schema.ReadTextFileRequest {
	var requests []schema.ReadTextFileRequest
	h.on(schema.FsReadTextFileMethodName, func(raw json.RawMessage) (any, error) {
		var request schema.ReadTextFileRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		h.mu.Lock()
		requests = append(requests, request)
		h.mu.Unlock()
		text, ok := virtual[request.Path]
		if !ok {
			data, err := os.ReadFile(request.Path)
			if err != nil {
				return nil, err
			}
			text = string(data)
		}
		lines := strings.SplitAfter(text, "\n")
		start := 0
		if request.Line != nil {
			start = min(int(*request.Line)-1, len(lines))
		}
		end := len(lines)
		if request.Limit != nil {
			end = min(start+int(*request.Limit), end)
		}
		return schema.ReadTextFileResponse{Content: strings.Join(lines[start:end], "")}, nil
	})
	return &requests
}

func TestReadFileFetchesLargeFilesInPart(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Fs: &schema.FileSystemCapabilities{ReadTextFile: ptr(true)}})
	var big, small strings.Builder
	for i := 1; i <= 30000; i++ {
		fmt.Fprintf(&big, "line %d %s\n", i, strings.Repeat("x", 40))
	}
	for i := 1; i <= 30; i++ {
		fmt.Fprintf(&small, "small %d\n", i)
	}
	write := func(name, text string) string {
		path := filepath.Join(h.dir, name)
		if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	bigPath, smallPath := write("big.txt", big.String()), write("small.txt", small.String())
	unsaved := filepath.Join(h.dir, "unsaved.txt")
	requests := serveFiles(h, map[string]string{unsaved: "a\nb\n"})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "read_file", map[string]any{"path": "big.txt", "offset": 100, "limit": 5}),
		toolCallChunks("call_2", "read_file", map[string]any{"path": "big.txt", "offset": 29998, "limit": 5}),
		toolCallChunks("call_3", "read_file", map[string]any{"path": "small.txt", "limit": 10}),
		toolCallChunks("call_4", "read_file", map[string]any{"path": "unsaved.txt"}),
		toolCallChunks("call_5", "read_file", map[string]any{"path": "big.txt", "offset": 40000}),
		textChunks("done"),
	}
	h.prompt(h.newSession(), "read them")

	pad := strings.Repeat("x", 40)
	want := []string{
		fmt.Sprintf("   100\tline 100 %[1]s\n   101\tline 101 %[1]s\n   102\tline 102 %[1]s\n   103\tline 103 %[1]s\n   104\tline 104 %[1]s\n", pad) +
			"[Showing lines 100-104; the file goes on. Use offset=105 to continue.]\n",
		fmt.Sprintf(" 29998\tline 29998 %[1]s\n 29999\tline 29999 %[1]s\n 30000\tline 30000 %[1]s\n", pad),
		"     1\tsmall 1\n     2\tsmall 2\n     3\tsmall 3\n     4\tsmall 4\n     5\tsmall 5\n     6\tsmall 6\n     7\tsmall 7\n     8\tsmall 8\n     9\tsmall 9\n    10\tsmall 10\n" +
			"[Showing lines 1-10 of 30. Use offset=11 to continue.]\n",
		"     1\ta\n     2\tb\n",
		"Error: offset 40000 is past the end of the file",
	}
	for i, w := range want {
		if output := toolOutput(h, i+1); output != w {
			t.Errorf("read %d = %q, want %q", i+1, output, w)
		}
	}
	line := func(n uint32) *uint32 { return &n }
	wantRequests := []schema.ReadTextFileRequest{
		{Path: bigPath, Line: line(100), Limit: line(6)},
		{Path: bigPath, Line: line(29998), Limit: line(6)},
		{Path: smallPath},
		{Path: unsaved, Line: line(1), Limit: line(2001)},
		{Path: bigPath, Line: line(40000), Limit: line(2001)},
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(*requests) != len(wantRequests) {
		t.Fatalf("requests = %+v", *requests)
	}
	for i, got := range *requests {
		w := wantRequests[i]
		if got.Path != w.Path || deref(got.Line) != deref(w.Line) || deref(got.Limit) != deref(w.Limit) {
			t.Errorf("request %d = %s line %v limit %v, want %s line %v limit %v", i+1, got.Path, deref(got.Line), deref(got.Limit), w.Path, deref(w.Line), deref(w.Limit))
		}
	}
}

// deref returns what p points to, or nil, in a form == can compare.
func deref[T any](p *T) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestLargeFilesSkipClientFs(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Fs: &schema.FileSystemCapabilities{ReadTextFile: ptr(true), WriteTextFile: ptr(true)}})
	var big strings.Builder
	for i := 1; i <= 30000; i++ {
		if i == 20000 {
			big.WriteString("marker\n")
			continue
		}
		fmt.Fprintf(&big, "line %d %s\n", i, strings.Repeat("x", 40))
	}
	bigPath, smallPath, newPath := filepath.Join(h.dir, "big.txt"), filepath.Join(h.dir, "small.txt"), filepath.Join(h.dir, "new.txt")
	if err := os.WriteFile(bigPath, []byte(big.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(smallPath, []byte("one\ntwo\nthree\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	reads := serveFiles(h, nil)
	var writes []string
	h.on(schema.FsWriteTextFileMethodName, func(raw json.RawMessage) (any, error) {
		var request schema.WriteTextFileRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		h.mu.Lock()
		writes = append(writes, request.Path)
		h.mu.Unlock()
		return schema.WriteTextFileResponse{}, os.WriteFile(request.Path, []byte(request.Content), 0o644)
	})
	var permissions []schema.ToolCallUpdate
	h.on(schema.SessionRequestPermissionMethodName, func(raw json.RawMessage) (any, error) {
		var request schema.RequestPermissionRequest
		if err := json.Unmarshal(raw, &request); err != nil {
			return nil, err
		}
		h.mu.Lock()
		permissions = append(permissions, request.ToolCall)
		h.mu.Unlock()
		return schema.RequestPermissionResponse{Outcome: schema.RequestPermissionOutcome{Selected: &schema.SelectedPermissionOutcome{OptionID: "allow_once"}}}, nil
	})
	created := strings.Repeat("y", maxFsBytes+1)
	h.router.responses = [][]string{
		toolCallChunks("call_1", "edit_file", map[string]string{"path": "big.txt", "old_string": "marker", "new_string": "MARKER"}),
		toolCallChunks("call_2", "write_file", map[string]string{"path": "new.txt", "content": created}),
		toolCallChunks("call_3", "edit_file", map[string]string{"path": "small.txt", "old_string": "three", "new_string": "3"}),
		textChunks("done"),
	}
	h.prompt(h.newSession(), "edit them")

	if data, _ := os.ReadFile(bigPath); string(data) != strings.Replace(big.String(), "marker", "MARKER", 1) {
		t.Error("big.txt not edited on disk")
	}
	if data, _ := os.ReadFile(newPath); string(data) != created {
		t.Error("new.txt not written on disk")
	}
	if data, _ := os.ReadFile(smallPath); string(data) != "one\ntwo\n3\n" {
		t.Errorf("small.txt = %q", data)
	}
	h.mu.Lock()
	if len(*reads) != 1 || (*reads)[0].Path != smallPath || !slices.Equal(writes, []string{smallPath}) {
		t.Errorf("client reads %+v, writes %v; want only small.txt", *reads, writes)
	}
	if len(permissions) != 3 {
		t.Fatalf("%d permission requests", len(permissions))
	}
	asked := permissions
	h.mu.Unlock()

	isDiff := func(content []schema.ToolCallContent) bool { return len(content) == 1 && content[0].Diff != nil }
	for i, c := range []struct {
		id   schema.ToolCallId
		line uint32
		diff bool
	}{{"call_1", 20000, false}, {"call_2", 1, false}, {"call_3", 3, true}} {
		call, final := callUpdates(h, c.id)
		if call == nil || final == nil {
			t.Fatalf("%s: tool call %v, final update %v", c.id, call, final)
		}
		if len(call.Locations) != 1 || deref(call.Locations[0].Line) != c.line {
			t.Errorf("%s locations = %+v, want line %d", c.id, call.Locations, c.line)
		}
		if isDiff(call.Content) != c.diff || isDiff(asked[i].Content) != c.diff || isDiff(final.Content) != c.diff {
			t.Errorf("%s: diff in tool call %v, permission %v, result %v; want %v", c.id, isDiff(call.Content), isDiff(asked[i].Content), isDiff(final.Content), c.diff)
		}
		if !c.diff && !strings.Contains(call.Content[0].Content.Content.Text.Text, "too large to show as a diff") {
			t.Errorf("%s summary = %+v", c.id, call.Content[0])
		}
	}
}

func TestToolCallsCarryNameAndRawOutput(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	var asked schema.ToolCallUpdate
	h.on(schema.SessionRequestPermissionMethodName, func(raw json.RawMessage) (any, error) {
		var request schema.RequestPermissionRequest
		err := json.Unmarshal(raw, &request)
		h.mu.Lock()
		asked = request.ToolCall
		h.mu.Unlock()
		return schema.RequestPermissionResponse{Outcome: schema.RequestPermissionOutcome{Selected: &schema.SelectedPermissionOutcome{OptionID: "allow_once"}}}, err
	})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "write_file", map[string]string{"path": "a.txt", "content": "hi\n"}),
		toolCallChunks("call_2", "bogus", map[string]string{}),
		textChunks("done"),
	}
	session := h.newSession()
	h.prompt(session, "go")

	check := func(when string) {
		t.Helper()
		for _, c := range []struct {
			id     schema.ToolCallId
			name   string
			output string
		}{{"call_1", "write_file", "Created " + filepath.Join(h.dir, "a.txt")}, {"call_2", "bogus", `Error: unknown tool "bogus"`}} {
			call, final := callUpdates(h, c.id)
			if call == nil || deref(call.Name) != c.name {
				t.Errorf("%s: %s tool call = %+v, want name %s", when, c.id, call, c.name)
				continue
			}
			raw := call.RawOutput // a replayed call is complete as sent
			if final != nil {
				raw = final.RawOutput
			}
			var output string
			if err := json.Unmarshal(raw, &output); err != nil || output != c.output {
				t.Errorf("%s: %s raw output = %s, want %q", when, c.id, raw, c.output)
			}
		}
	}
	check("live")
	h.mu.Lock()
	if deref(asked.Name) != "write_file" {
		t.Errorf("permission request name = %v", deref(asked.Name))
	}
	h.updates = nil
	h.mu.Unlock()
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	check("replay")
}
