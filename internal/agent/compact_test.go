package agent

import (
	"slices"
	"strings"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/config"
)

func TestCompactCommandSummarizesAndPublishes(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Session: &schema.ClientSessionCapabilities{Compaction: &schema.CompactionCapabilities{}}})
	h.router.responses = [][]string{
		textChunks("answer one"),
		textChunks("answer two"),
		textChunks("summary of the first exchange"),
	}
	session := h.newSession()
	h.prompt(session, "first question")
	h.prompt(session, "second question")
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	h.prompt(session, "/compact")

	var statuses []schema.CompactionStatus
	var chunks []string
	for _, n := range h.updates {
		if n.Update.CompactionUpdate != nil {
			statuses = append(statuses, n.Update.CompactionUpdate.Status)
			if n.Update.CompactionUpdate.Status == schema.CompactionStatusCompleted {
				if len(n.Update.CompactionUpdate.Summary) != 1 || n.Update.CompactionUpdate.Summary[0].Text == nil {
					t.Fatalf("completed compaction summary = %+v", n.Update.CompactionUpdate)
				}
			}
		}
		if n.Update.CompactionSummaryChunk != nil {
			chunks = append(chunks, n.Update.CompactionSummaryChunk.Content.Text.Text)
		}
	}
	if !slices.Equal(statuses, []schema.CompactionStatus{schema.CompactionStatusInProgress, schema.CompactionStatusCompleted}) {
		t.Fatalf("compaction statuses = %v", statuses)
	}
	if strings.Join(chunks, "") != "summary of the first exchange" {
		t.Fatalf("summary chunks = %q", chunks)
	}

	// The summarizer saw the old exchange, and the next request starts with
	// the summary followed by the surviving last turn.
	summaryRequest := h.router.requests[2]
	if !strings.Contains(summaryRequest["messages"].([]any)[1].(map[string]any)["content"].(string), "first question") {
		t.Fatalf("summarizer messages = %v", summaryRequest["messages"])
	}
	h.router.responses = [][]string{textChunks("answer three")}
	h.prompt(session, "third question")
	next := h.router.requests[3]["messages"].([]any)
	if content := next[1].(map[string]any)["content"].(string); !strings.Contains(content, "summary of the first exchange") {
		t.Fatalf("compacted history = %v", next)
	}
	if !strings.Contains(next[2].(map[string]any)["content"].(string), "second question") {
		t.Fatalf("last turn lost: %v", next)
	}

	// The compaction replays on load.
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	replayed := false
	for _, n := range h.updates {
		if n.Update.CompactionUpdate != nil && n.Update.CompactionUpdate.Status == schema.CompactionStatusCompleted {
			replayed = true
		}
	}
	if !replayed {
		t.Fatal("compaction not replayed by session/load")
	}
}

func TestAutoCompactTriggersNearTheContextLimit(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Session: &schema.ClientSessionCapabilities{Compaction: &schema.CompactionCapabilities{}}})
	h.router.responses = [][]string{
		textChunks("answer one"),
		textChunks("summary after the limit"),
		textChunks("answer two"),
	}
	session := h.newSession()
	h.prompt(session, "first question")
	loaded, err := h.agent.lookup(session)
	if err != nil {
		t.Fatal(err)
	}
	loaded.mu.Lock()
	loaded.contextUsed, loaded.contextSize = 900, 1000
	loaded.mu.Unlock()

	h.prompt(session, "second question")
	next := h.router.requests[2]["messages"].([]any)
	if content := next[1].(map[string]any)["content"].(string); !strings.Contains(content, "summary after the limit") {
		t.Fatalf("auto compaction did not run: %v", next)
	}
}

func TestAutoCompactCanBeDisabled(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Session: &schema.ClientSessionCapabilities{Compaction: &schema.CompactionCapabilities{}}})
	if err := h.store.Update(func(c *config.Config) error { c.AutoCompact = new(false); return nil }); err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{
		textChunks("answer one"),
		textChunks("answer two"),
	}
	session := h.newSession()
	h.prompt(session, "first question")
	loaded, err := h.agent.lookup(session)
	if err != nil {
		t.Fatal(err)
	}
	loaded.mu.Lock()
	loaded.contextUsed, loaded.contextSize = 900, 1000
	loaded.mu.Unlock()
	h.prompt(session, "second question")
	if len(h.router.requests) != 2 {
		t.Fatalf("requests = %d, want the two prompts only", len(h.router.requests))
	}
}

func TestCompactWithoutClientCapability(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		textChunks("answer one"),
		textChunks("answer two"),
		textChunks("quiet summary"),
		textChunks("answer three"),
	}
	session := h.newSession()
	h.prompt(session, "first question")
	h.prompt(session, "second question")
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	h.prompt(session, "/compact")
	for _, n := range h.updates {
		if n.Update.CompactionUpdate != nil || n.Update.CompactionSummaryChunk != nil {
			t.Fatal("compaction updates sent to a client that did not advertise them")
		}
	}
	h.prompt(session, "third question")
	next := h.router.requests[3]["messages"].([]any)
	if content := next[1].(map[string]any)["content"].(string); !strings.Contains(content, "quiet summary") {
		t.Fatalf("history was not compacted: %v", next)
	}
}
