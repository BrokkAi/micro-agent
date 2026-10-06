package agent

import (
	"slices"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

func TestUpdatePlanPublishesPlanUpdates(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Plan: &schema.PlanCapabilities{}})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "update_plan", map[string]any{"entries": []any{
			map[string]any{"content": "Investigate", "status": "completed", "priority": "high"},
			map[string]any{"content": "Implement", "status": "in_progress", "priority": "medium"},
		}}),
		textChunks("plan set"),
	}
	session := h.newSession()
	if reason := h.prompt(session, "start").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	var plan *schema.Plan
	var items *schema.PlanItems
	for _, n := range h.updates {
		if n.Update.Plan != nil {
			plan = n.Update.Plan
		}
		if n.Update.PlanUpdate != nil && n.Update.PlanUpdate.Plan.Items != nil {
			items = n.Update.PlanUpdate.Plan.Items
		}
	}
	if plan == nil || len(plan.Entries) != 2 || plan.Entries[1].Status != schema.PlanEntryStatusInProgress {
		t.Fatalf("plan = %+v", plan)
	}
	if items == nil || items.PlanID != planID || len(items.Entries) != 2 {
		t.Fatalf("plan_update items = %+v", items)
	}

	// The markdown document replaces the payload beside the task list.
	h.router.responses = [][]string{
		toolCallChunks("call_2", "update_plan", map[string]any{"markdown": "## Plan\n\n1. Investigate\n2. Implement"}),
		textChunks("document set"),
	}
	h.prompt(session, "document it")
	var markdown *schema.PlanMarkdown
	for _, n := range h.updates {
		if n.Update.PlanUpdate != nil && n.Update.PlanUpdate.Plan.Markdown != nil {
			markdown = n.Update.PlanUpdate.Plan.Markdown
		}
	}
	if markdown == nil || markdown.PlanID != planID || markdown.Content == "" {
		t.Fatalf("plan_update markdown = %+v", markdown)
	}

	// Clearing removes both shapes.
	h.router.responses = [][]string{
		toolCallChunks("call_3", "update_plan", map[string]any{"clear": true}),
		textChunks("cleared"),
	}
	h.prompt(session, "clear it")
	var removed *schema.PlanRemoved
	for _, n := range h.updates {
		if n.Update.PlanRemoved != nil {
			removed = n.Update.PlanRemoved
		}
	}
	if removed == nil || removed.PlanID != planID {
		t.Fatalf("plan_removed = %+v", removed)
	}
	loaded, err := h.agent.lookup(session)
	if err != nil {
		t.Fatal(err)
	}
	loaded.mu.Lock()
	defer loaded.mu.Unlock()
	if loaded.Plan != nil || loaded.PlanMarkdown != "" {
		t.Fatalf("cleared plan persisted: %+v", loaded.record)
	}
}

func TestUpdatePlanWithoutClientCapability(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "update_plan", map[string]any{"entries": []any{
			map[string]any{"content": "Only step", "status": "pending", "priority": "low"},
		}}),
		textChunks("done"),
	}
	session := h.newSession()
	h.prompt(session, "plan")
	var kinds []schema.SessionUpdateKind
	for _, n := range h.updates {
		kinds = append(kinds, n.Update.Kind)
	}
	if !slices.Contains(kinds, schema.SessionUpdateKindPlan) {
		t.Fatalf("stable plan update missing: %v", kinds)
	}
	if slices.Contains(kinds, schema.SessionUpdateKindPlanUpdate) || slices.Contains(kinds, schema.SessionUpdateKindPlanRemoved) {
		t.Fatalf("unsupported plan updates sent: %v", kinds)
	}
}

func TestLoadReplaysPlan(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Plan: &schema.PlanCapabilities{}})
	h.router.responses = [][]string{
		toolCallChunks("call_1", "update_plan", map[string]any{"entries": []any{
			map[string]any{"content": "Carry on", "status": "in_progress", "priority": "high"},
		}}),
		textChunks("noted"),
	}
	session := h.newSession()
	h.prompt(session, "plan")
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	var plan *schema.Plan
	for _, n := range h.updates {
		if n.Update.Plan != nil {
			plan = n.Update.Plan
		}
	}
	if plan == nil || len(plan.Entries) != 1 || plan.Entries[0].Content != "Carry on" {
		t.Fatalf("replayed plan = %+v", plan)
	}
}
