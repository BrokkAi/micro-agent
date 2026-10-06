package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
	"github.com/BrokkAi/micro-agent/internal/openrouter"
)

// document is an open editor buffer, kept in sync by document/* notifications.
type document struct {
	URI        string
	LanguageID string
	Version    int64
	Text       string
}

// nesSession is one next-edit-suggestion session started by the client.
type nesSession struct {
	folders []string
	// rejected holds the edit keys the user rejected, so they stay dismissed.
	rejected map[string]bool
	// edits maps outstanding suggestion IDs to their edit keys.
	edits map[schema.NesSuggestionId]string
}

// nesCapabilities is what micro-agent asks the client to send for
// suggestions: diagnostics and recent edits with each request, and full
// document snapshots as buffers change.
func nesCapabilities() *schema.NesCapabilities {
	return &schema.NesCapabilities{
		Context: &schema.NesContextCapabilities{
			Diagnostics: &schema.NesDiagnosticsCapabilities{},
			EditHistory: &schema.NesEditHistoryCapabilities{MaxCount: ptr(uint32(10))},
		},
		Events: &schema.NesEventCapabilities{
			Document: &schema.NesDocumentEventCapabilities{
				DidOpen:   &schema.NesDocumentDidOpenCapabilities{},
				DidChange: &schema.NesDocumentDidChangeCapabilities{SyncKind: schema.TextDocumentSyncKindFull},
				DidClose:  &schema.NesDocumentDidCloseCapabilities{},
				DidFocus:  &schema.NesDocumentDidFocusCapabilities{},
				DidSave:   &schema.NesDocumentDidSaveCapabilities{},
			},
		},
	}
}

func (a *Agent) StartNes(_ context.Context, request schema.StartNesRequest) (schema.StartNesResponse, error) {
	session := &nesSession{rejected: map[string]bool{}, edits: map[schema.NesSuggestionId]string{}}
	for _, folder := range request.WorkspaceFolders {
		session.folders = append(session.folders, folder.URI)
	}
	id := newSessionID()
	a.stateMu.Lock()
	a.nes[id] = session
	a.stateMu.Unlock()
	return schema.StartNesResponse{SessionID: id}, nil
}

func (a *Agent) CloseNes(_ context.Context, request schema.CloseNesRequest) (schema.CloseNesResponse, error) {
	a.stateMu.Lock()
	_, ok := a.nes[request.SessionID]
	delete(a.nes, request.SessionID)
	a.stateMu.Unlock()
	if !ok {
		return schema.CloseNesResponse{}, resourceNotFound("NES session " + string(request.SessionID))
	}
	return schema.CloseNesResponse{}, nil
}

// nesSuggestion is one edit the model proposed, as exact snippets.
type nesSuggestion struct {
	OldText string `json:"oldText"`
	NewText string `json:"newText"`
}

func (a *Agent) SuggestNes(ctx context.Context, request schema.SuggestNesRequest) (schema.SuggestNesResponse, error) {
	a.stateMu.Lock()
	session := a.nes[request.SessionID]
	encoding := a.encoding
	a.stateMu.Unlock()
	if session == nil {
		return schema.SuggestNesResponse{}, resourceNotFound("NES session " + string(request.SessionID))
	}
	text, language, ok := a.documentText(request.URI)
	if !ok {
		return schema.SuggestNesResponse{}, resourceNotFound("document " + request.URI)
	}
	reply, err := a.nesCompletion(ctx, text, language, request)
	if err != nil {
		return schema.SuggestNesResponse{}, err
	}
	return schema.SuggestNesResponse{Suggestions: a.nesSuggestions(session, request, text, encoding, reply)}, nil
}

// nesCompletion asks the model for edit suggestions and returns its reply.
func (a *Agent) nesCompletion(ctx context.Context, text, language string, request schema.SuggestNesRequest) (string, error) {
	if _, _, _, _, disabled := a.providerSettings(); disabled {
		return "", providerDisabledError()
	}
	result, err := a.modelClient().Stream(ctx, openrouter.Request{
		Model: a.cfg.Get().Model,
		Messages: []openrouter.Message{
			{Role: "system", Content: "You are micro-agent's next-edit-suggestion engine. You reply with JSON only."},
			{Role: "user", Content: nesPrompt(text, language, request)},
		},
	}, openrouter.Handler{})
	if err != nil {
		return "", err
	}
	if text, ok := result.Message.Content.(string); ok {
		return text, nil
	}
	return "", nil
}

// nesSuggestions turns the model's reply into protocol suggestions, dropping
// edits that do not apply or were already rejected.
func (a *Agent) nesSuggestions(session *nesSession, request schema.SuggestNesRequest, text string, encoding schema.PositionEncodingKind, reply string) []schema.NesSuggestion {
	var parsed struct {
		Suggestions []nesSuggestion `json:"suggestions"`
	}
	if start, end := strings.IndexByte(reply, '{'), strings.LastIndexByte(reply, '}'); start >= 0 && end > start {
		_ = json.Unmarshal([]byte(reply[start:end+1]), &parsed)
	}
	cursor := byteOffset(text, request.Position, encoding)
	var suggestions []schema.NesSuggestion
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	for _, edit := range parsed.Suggestions {
		if len(suggestions) == 3 {
			break
		}
		key := edit.OldText + "\x00" + edit.NewText
		if edit.OldText == "" || edit.OldText == edit.NewText || session.rejected[key] {
			continue
		}
		at := nearestIndex(text, edit.OldText, cursor)
		if at < 0 {
			continue
		}
		id := newSuggestionID()
		after := positionAt(text, at+len(edit.NewText), encoding)
		session.edits[id] = key
		suggestions = append(suggestions, schema.NesSuggestion{Edit: &schema.NesEditSuggestion{
			ID:             id,
			URI:            request.URI,
			CursorPosition: &after,
			Edits: []schema.NesTextEdit{{
				Range: schema.Range{
					Start: positionAt(text, at, encoding),
					End:   positionAt(text, at+len(edit.OldText), encoding),
				},
				NewText: edit.NewText,
			}},
		}})
	}
	return suggestions
}

func newSuggestionID() schema.NesSuggestionId {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return schema.NesSuggestionId(hex.EncodeToString(b[:]))
}

// nearestIndex returns the occurrence of snippet closest to at.
func nearestIndex(text, snippet string, at int) int {
	best, bestDistance := -1, 0
	for offset := 0; ; {
		found := strings.Index(text[offset:], snippet)
		if found < 0 {
			return best
		}
		found += offset
		distance := found - at
		if distance < 0 {
			distance = -distance
		}
		if best < 0 || distance < bestDistance {
			best, bestDistance = found, distance
		}
		offset = found + 1
	}
}

// acceptNes forgets a suggestion the user applied.
func (a *Agent) acceptNes(notification schema.AcceptNesNotification) {
	a.stateMu.Lock()
	if session := a.nes[notification.SessionID]; session != nil {
		delete(session.edits, notification.ID)
	}
	a.stateMu.Unlock()
}

// AcceptNes implements unstable.NesHandler.
func (a *Agent) AcceptNes(_ context.Context, notification schema.AcceptNesNotification) error {
	a.acceptNes(notification)
	return nil
}

// rejectNes remembers a suggestion the user dismissed, so it is not offered
// again in this NES session.
func (a *Agent) rejectNes(notification schema.RejectNesNotification) {
	a.stateMu.Lock()
	if session := a.nes[notification.SessionID]; session != nil {
		if key, ok := session.edits[notification.ID]; ok {
			session.rejected[key] = true
			delete(session.edits, notification.ID)
		}
	}
	a.stateMu.Unlock()
}

// RejectNes implements unstable.NesHandler.
func (a *Agent) RejectNes(_ context.Context, notification schema.RejectNesNotification) error {
	a.rejectNes(notification)
	return nil
}

// documentText returns the current text of a document: the client's open
// buffer when it sent one, the file on disk otherwise.
func (a *Agent) documentText(uri string) (string, string, bool) {
	a.stateMu.Lock()
	doc, ok := a.docs[uri]
	a.stateMu.Unlock()
	if ok {
		return doc.Text, doc.LanguageID, true
	}
	if path := uriPath(uri); path != uri {
		if data, err := os.ReadFile(path); err == nil {
			return string(data), "", true
		}
	}
	return "", "", false
}

// Document notifications. They run on the connection's read loop, so they
// only update in-memory state.

func (a *Agent) didOpenDocument(notification schema.DidOpenDocumentNotification) {
	a.stateMu.Lock()
	a.docs[notification.URI] = document{URI: notification.URI, LanguageID: notification.LanguageID, Version: notification.Version, Text: notification.Text}
	a.stateMu.Unlock()
}

func (a *Agent) didChangeDocument(notification schema.DidChangeDocumentNotification) {
	a.stateMu.Lock()
	defer a.stateMu.Unlock()
	doc, known := a.docs[notification.URI]
	for _, change := range notification.ContentChanges {
		switch {
		case change.Range == nil:
			doc.Text = change.Text
			known = true
		case known:
			start := byteOffset(doc.Text, change.Range.Start, a.encoding)
			end := byteOffset(doc.Text, change.Range.End, a.encoding)
			if end < start {
				end = start
			}
			doc.Text = doc.Text[:start] + change.Text + doc.Text[end:]
		}
	}
	if !known {
		return
	}
	doc.Version = notification.Version
	doc.URI = notification.URI
	a.docs[notification.URI] = doc
}

func (a *Agent) didCloseDocument(notification schema.DidCloseDocumentNotification) {
	a.stateMu.Lock()
	delete(a.docs, notification.URI)
	if a.focus == notification.URI {
		a.focus = ""
	}
	a.stateMu.Unlock()
}

func (a *Agent) didFocusDocument(notification schema.DidFocusDocumentNotification) {
	a.stateMu.Lock()
	a.focus = notification.URI
	a.stateMu.Unlock()
}

func (a *Agent) didSaveDocument() {}

// nesPrompt renders the file and the request context for the model.
func nesPrompt(text, language string, request schema.SuggestNesRequest) string {
	var b strings.Builder
	fmt.Fprintf(&b, "The user is editing %s", request.URI)
	if language != "" {
		fmt.Fprintf(&b, " (%s)", language)
	}
	fmt.Fprintf(&b, " at version %d. The cursor is at line %d, character %d (0-based).", request.Version, request.Position.Line, request.Position.Character)
	if request.Selection != nil {
		fmt.Fprintf(&b, " The selection runs from line %d, character %d to line %d, character %d.", request.Selection.Start.Line, request.Selection.Start.Character, request.Selection.End.Line, request.Selection.End.Character)
	}
	fmt.Fprintf(&b, " The request was triggered %s.\n", request.TriggerKind)
	if context := request.Context; context != nil {
		if len(context.Diagnostics) > 0 {
			b.WriteString("\nDiagnostics:\n")
			for _, diagnostic := range context.Diagnostics {
				fmt.Fprintf(&b, "- %s line %d: %s: %s\n", diagnostic.URI, diagnostic.Range.Start.Line+1, diagnostic.Severity, diagnostic.Message)
			}
		}
		if len(context.EditHistory) > 0 {
			b.WriteString("\nRecent edits:\n")
			for _, entry := range context.EditHistory {
				diff := entry.Diff
				if len(diff) > 800 {
					diff = diff[:800] + "…"
				}
				fmt.Fprintf(&b, "- %s:\n%s\n", entry.URI, diff)
			}
		}
		if len(context.UserActions) > 0 {
			b.WriteString("\nRecent actions:\n")
			for _, action := range context.UserActions {
				fmt.Fprintf(&b, "- %s at %s line %d, character %d\n", action.Action, action.URI, action.Position.Line+1, action.Position.Character)
			}
		}
	}
	b.WriteString(`

Suggest at most 3 small edits that help at the cursor. Reply with JSON only:
{"suggestions":[{"oldText":"exact snippet from the file","newText":"replacement"}]}
oldText must appear verbatim in the file and be short enough to locate uniquely; newText is what replaces it. Reply {"suggestions":[]} when no edit is useful.

File (line numbers are for orientation and are not part of the text):
`)
	excerpt, first := nesExcerpt(text, request.Position.Line, 120, 80)
	for i, line := range strings.Split(excerpt, "\n") {
		fmt.Fprintf(&b, "%5d %s\n", first+uint32(i), line)
	}
	return b.String()
}

// nesExcerpt returns the lines around line and the number of the first line
// (1-based), bounding what one suggestion request carries.
func nesExcerpt(text string, line uint32, before, after int) (string, uint32) {
	lines := strings.Split(text, "\n")
	start := int(line) - before
	if start < 0 {
		start = 0
	}
	end := int(line) + after
	if end > len(lines) {
		end = len(lines)
	}
	return strings.Join(lines[start:end], "\n"), uint32(start + 1)
}
