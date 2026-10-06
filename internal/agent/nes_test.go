package agent

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

func TestNesSuggestsEditsFromTheOpenBuffer(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{
		Nes:               &schema.ClientNesCapabilities{},
		PositionEncodings: []schema.PositionEncodingKind{schema.PositionEncodingKindUtf16},
	})
	if h.init.AgentCapabilities.Nes == nil || h.init.AgentCapabilities.Nes.Events.Document.DidChange == nil {
		t.Fatal("NES capabilities not advertised")
	}
	if encoding := h.init.AgentCapabilities.PositionEncoding; encoding == nil || *encoding != schema.PositionEncodingKindUtf16 {
		t.Fatalf("position encoding = %v", encoding)
	}
	start, err := send[schema.StartNesResponse](h, schema.NesStartMethodName, schema.StartNesRequest{
		WorkspaceFolders: []schema.WorkspaceFolder{{Name: "w", URI: "file:///w"}},
	})
	if err != nil || start.SessionID == "" {
		t.Fatalf("nes/start = %+v, %v", start, err)
	}
	uri := "file:///w/main.go"
	text := "package main\n\nfunc main() {\n\tprint(\"hi\")\n}\n"
	if err := h.conn.Notify(context.Background(), schema.DocumentDidopenMethodName, schema.DidOpenDocumentNotification{
		SessionID: start.SessionID, URI: uri, LanguageID: "go", Version: 1, Text: text,
	}); err != nil {
		t.Fatal(err)
	}

	h.router.responses = [][]string{textChunks("```json\n{\"suggestions\":[{\"oldText\":\"print(\\\"hi\\\")\",\"newText\":\"println(\\\"hi\\\")\"},{\"oldText\":\"not in the file\",\"newText\":\"x\"}]}\n```")}
	request := schema.SuggestNesRequest{
		SessionID: start.SessionID, URI: uri, Version: 1,
		Position:    schema.Position{Line: 3, Character: 6},
		TriggerKind: schema.NesTriggerKindManual,
		Context: &schema.NesSuggestContext{Diagnostics: []schema.NesDiagnostic{{
			URI: uri, Message: "unused import", Severity: schema.NesDiagnosticSeverityWarning,
			Range: schema.Range{Start: schema.Position{Line: 0, Character: 0}, End: schema.Position{Line: 0, Character: 7}},
		}}},
	}
	response, err := send[schema.SuggestNesResponse](h, schema.NesSuggestMethodName, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Suggestions) != 1 {
		t.Fatalf("suggestions = %+v", response.Suggestions)
	}
	edit := response.Suggestions[0].Edit
	if edit == nil || response.Suggestions[0].Kind != schema.NesSuggestionKindEdit {
		t.Fatalf("suggestion = %+v", response.Suggestions[0])
	}
	if len(edit.Edits) != 1 || edit.Edits[0].NewText != "println(\"hi\")" {
		t.Fatalf("edits = %+v", edit.Edits)
	}
	want := schema.Range{Start: schema.Position{Line: 3, Character: 1}, End: schema.Position{Line: 3, Character: 12}}
	if !samePosition(edit.Edits[0].Range.Start, want.Start) || !samePosition(edit.Edits[0].Range.End, want.End) {
		t.Fatalf("range = %+v, want %+v", edit.Edits[0].Range, want)
	}
	prompt := h.router.requests[0]["messages"].([]any)[1].(map[string]any)["content"].(string)
	if !strings.Contains(prompt, "unused import") || !strings.Contains(prompt, `print("hi")`) {
		t.Fatalf("prompt = %s", prompt)
	}

	// Dismissing the suggestion keeps it from coming back.
	if err := h.conn.Notify(context.Background(), schema.NesRejectMethodName, schema.RejectNesNotification{
		SessionID: start.SessionID, ID: edit.ID, Reason: ptr(schema.NesRejectReasonRejected),
	}); err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{textChunks(`{"suggestions":[{"oldText":"print(\"hi\")","newText":"println(\"hi\")"}]}`)}
	response, err = send[schema.SuggestNesResponse](h, schema.NesSuggestMethodName, request)
	if err != nil || len(response.Suggestions) != 0 {
		t.Fatalf("rejected suggestion came back: %+v, %v", response.Suggestions, err)
	}

	if _, err := send[schema.CloseNesResponse](h, schema.NesCloseMethodName, schema.CloseNesRequest{SessionID: start.SessionID}); err != nil {
		t.Fatal(err)
	}
	if _, err := send[schema.SuggestNesResponse](h, schema.NesSuggestMethodName, request); rpcCode(err) != int(schema.ErrorCodeResourceNotFound) {
		t.Fatalf("suggest after close: %v", err)
	}
}

func TestNesReadsSavedFilesAndTracksChanges(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Nes: &schema.ClientNesCapabilities{}})
	path := filepath.Join(h.dir, "notes.txt")
	if err := os.WriteFile(path, []byte("old line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	start, err := send[schema.StartNesResponse](h, schema.NesStartMethodName, schema.StartNesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	uri := "file://" + path
	h.router.responses = [][]string{textChunks(`{"suggestions":[{"oldText":"old line","newText":"new line"}]}`)}
	// Without an open buffer the file on disk is used.
	response, err := send[schema.SuggestNesResponse](h, schema.NesSuggestMethodName, schema.SuggestNesRequest{
		SessionID: start.SessionID, URI: uri, Position: schema.Position{}, TriggerKind: schema.NesTriggerKindAutomatic,
	})
	if err != nil || len(response.Suggestions) != 1 {
		t.Fatalf("suggestions = %+v, %v", response.Suggestions, err)
	}
	// A full-buffer didChange replaces what the agent sees.
	if err := h.conn.Notify(context.Background(), schema.DocumentDidchangeMethodName, schema.DidChangeDocumentNotification{
		SessionID: start.SessionID, URI: uri, Version: 2,
		ContentChanges: []schema.TextDocumentContentChangeEvent{{Text: "old line\nsecond\n"}},
	}); err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{textChunks(`{"suggestions":[{"oldText":"second","newText":"third"}]}`)}
	response, err = send[schema.SuggestNesResponse](h, schema.NesSuggestMethodName, schema.SuggestNesRequest{
		SessionID: start.SessionID, URI: uri, Version: 2, Position: schema.Position{Line: 1}, TriggerKind: schema.NesTriggerKindManual,
	})
	if err != nil || len(response.Suggestions) != 1 || response.Suggestions[0].Edit.Edits[0].NewText != "third" {
		t.Fatalf("changed buffer suggestion = %+v, %v", response.Suggestions, err)
	}
}
