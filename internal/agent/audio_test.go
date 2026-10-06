package agent

import (
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

func TestAudioPrompts(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	if !isTrue(h.init.AgentCapabilities.PromptCapabilities.Audio) {
		t.Fatal("audio prompt capability not advertised")
	}
	h.router.responses = [][]string{textChunks("heard it")}
	session := h.newSession()
	request := schema.PromptRequest{SessionID: session, Prompt: []schema.ContentBlock{
		{Text: &schema.TextContent{Text: "what is said here?"}},
		{Audio: &schema.AudioContent{MimeType: "audio/mpeg", Data: "QUJD"}},
		{Audio: &schema.AudioContent{MimeType: "audio/ogg", Data: "REVG"}},
	}}
	if _, err := send[schema.PromptResponse](h, schema.SessionPromptMethodName, request); err != nil {
		t.Fatal(err)
	}
	messages := h.router.requests[0]["messages"].([]any)
	user := messages[len(messages)-1].(map[string]any)
	parts := user["content"].([]any)
	if len(parts) != 3 {
		t.Fatalf("parts = %v", parts)
	}
	audio := parts[1].(map[string]any)
	input := audio["input_audio"].(map[string]any)
	if audio["type"] != "input_audio" || input["format"] != "mp3" || input["data"] != "QUJD" {
		t.Fatalf("audio part = %v", audio)
	}
	// A codec OpenRouter does not take becomes a note instead of a failure.
	unsupported := parts[2].(map[string]any)
	if unsupported["type"] != "text" || unsupported["text"] == "" {
		t.Fatalf("unsupported audio part = %v", unsupported)
	}

	// Loading the session replays the audio block.
	h.mu.Lock()
	h.updates = nil
	h.mu.Unlock()
	if _, err := send[schema.LoadSessionResponse](h, schema.SessionLoadMethodName, schema.LoadSessionRequest{SessionID: session, Cwd: h.dir, MCPServers: []schema.McpServer{}}); err != nil {
		t.Fatal(err)
	}
	var replayed *schema.AudioContent
	for _, n := range h.updates {
		if n.Update.UserMessageChunk != nil && n.Update.UserMessageChunk.Content.Audio != nil {
			replayed = n.Update.UserMessageChunk.Content.Audio
		}
	}
	if replayed == nil || replayed.MimeType != "audio/mpeg" || replayed.Data != "QUJD" {
		t.Fatalf("replayed audio = %+v", replayed)
	}
}
