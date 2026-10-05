package openrouter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAssembleKeepsPartialTextOnError(t *testing.T) {
	stop := errors.New("cancelled")
	seen := 0
	stream := "data: {\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\"world\"}}]}\n\n"
	result, err := assemble(strings.NewReader(stream), Handler{Text: func(string) error {
		seen++
		if seen == 2 {
			return stop
		}
		return nil
	}})
	if !errors.Is(err, stop) {
		t.Fatalf("err = %v", err)
	}
	if result.Message.Content != "Hello world" {
		t.Fatalf("content = %#v", result.Message.Content)
	}
}

func TestAssembleToolCalls(t *testing.T) {
	stream := `data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"shell","arguments":"{\"comm"}}]}}]}` + "\n\n" +
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"and\":\"ls\"}"}},{"index":1,"id":"b","function":{"name":"read_file"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":4}}` + "\n\ndata: [DONE]\n\n"
	result, err := assemble(strings.NewReader(stream), Handler{})
	if err != nil {
		t.Fatal(err)
	}
	calls := result.Message.ToolCalls
	if len(calls) != 2 || calls[0].Function.Arguments != `{"command":"ls"}` || calls[1].Function.Arguments != "{}" {
		t.Fatalf("calls = %+v", calls)
	}
	if result.FinishReason != "tool_calls" || result.Usage == nil || result.Usage.CompletionTokens != 4 {
		t.Fatalf("result = %+v", result)
	}
}

func TestAssembleUsageDetails(t *testing.T) {
	// The usage chunk from OpenRouter's usage accounting docs.
	stream := `data: {"object":"chat.completion.chunk","choices":[],"usage":{"completion_tokens":2,"completion_tokens_details":{"reasoning_tokens":1},"cost":0.95,"cost_details":{"upstream_inference_cost":19},"prompt_tokens":194,"prompt_tokens_details":{"cached_tokens":50,"cache_write_tokens":100,"audio_tokens":0},"total_tokens":196}}` + "\n\ndata: [DONE]\n\n"
	result, err := assemble(strings.NewReader(stream), Handler{})
	if err != nil {
		t.Fatal(err)
	}
	want := Usage{
		PromptTokens:            194,
		CompletionTokens:        2,
		TotalTokens:             196,
		Cost:                    0.95,
		PromptTokensDetails:     PromptTokensDetails{CachedTokens: 50, CacheWriteTokens: 100},
		CompletionTokensDetails: CompletionTokensDetails{ReasoningTokens: 1},
	}
	if result.Usage == nil || *result.Usage != want {
		t.Fatalf("usage = %+v", result.Usage)
	}
}

func TestPartsMarshalAndRoundTrip(t *testing.T) {
	parts := []Part{
		{Type: "text", Text: "Describe these"},
		{Type: "image_url", ImageURL: &ImageURL{URL: "data:image/png;base64,iVBO"}},
		{Type: "input_audio", InputAudio: &InputAudio{Data: "UklGR", Format: "wav"}},
		{Type: "file", File: &File{Filename: "spec.pdf", FileData: "data:application/pdf;base64,JVBER"}},
	}
	data, err := json.Marshal(Message{Role: "user", Content: parts})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"role":"user","content":[` +
		`{"type":"text","text":"Describe these"},` +
		`{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBO"}},` +
		`{"type":"input_audio","input_audio":{"data":"UklGR","format":"wav"}},` +
		`{"type":"file","file":{"filename":"spec.pdf","file_data":"data:application/pdf;base64,JVBER"}}]}`
	if string(data) != want {
		t.Fatalf("json = %s", data)
	}

	// Stored sessions decode Content as []any; the agent re-decodes it as parts.
	var stored Message
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(stored.Content)
	var decoded []Part
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, parts) {
		t.Fatalf("decoded = %+v", decoded)
	}
}

func TestHeadersOverrideDefaults(t *testing.T) {
	seen := map[string]http.Header{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen[r.URL.Path] = r.Header.Clone()
		if r.URL.Path == "/models" {
			fmt.Fprint(w, `{"data":[]}`)
			return
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer server.Close()
	resetModels(t)

	client := &Client{BaseURL: server.URL, APIKey: "sk-or-key", Headers: map[string]string{
		"Authorization": "Basic abc",
		"X-Title":       "Other",
		"X-Extra":       "1",
	}}
	if _, err := client.Stream(context.Background(), Request{Model: "m"}, Handler{}); err != nil {
		t.Fatal(err)
	}
	client.Model(context.Background(), "m")
	for _, path := range []string{"/chat/completions", "/models"} {
		header := seen[path]
		if header.Get("Authorization") != "Basic abc" || header.Get("X-Title") != "Other" || header.Get("X-Extra") != "1" || header.Get("HTTP-Referer") != "https://brokk.ai" {
			t.Fatalf("%s headers = %v", path, header)
		}
	}
	if seen["/chat/completions"].Get("Accept") != "text/event-stream" {
		t.Fatalf("accept = %q", seen["/chat/completions"].Get("Accept"))
	}

	client.Headers = nil
	if _, err := client.Stream(context.Background(), Request{Model: "m"}, Handler{}); err != nil {
		t.Fatal(err)
	}
	if got := seen["/chat/completions"].Get("Authorization"); got != "Bearer sk-or-key" {
		t.Fatalf("default authorization = %q", got)
	}
}

func TestStreamReportsRetries(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w, `{"error":{"message":"slow down","code":429}}`)
			return
		}
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()

	type retry struct {
		attempt int
		wait    time.Duration
		err     error
	}
	var retries []retry
	client := &Client{BaseURL: server.URL, backoff: time.Millisecond}
	result, err := client.Stream(context.Background(), Request{Model: "m"}, Handler{Retry: func(attempt int, wait time.Duration, err error) {
		retries = append(retries, retry{attempt, wait, err})
	}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Message.Content != "ok" {
		t.Fatalf("content = %#v", result.Message.Content)
	}
	var status *StatusError
	if len(retries) != 1 || retries[0].attempt != 1 || retries[0].wait != time.Millisecond || !errors.As(retries[0].err, &status) || status.Status != 429 || status.Body != "slow down" {
		t.Fatalf("retries = %+v", retries)
	}
}

func TestStreamGivesUpAfterRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()

	var waits []time.Duration
	client := &Client{BaseURL: server.URL, backoff: time.Millisecond}
	_, err := client.Stream(context.Background(), Request{Model: "m"}, Handler{Retry: func(_ int, wait time.Duration, _ error) {
		waits = append(waits, wait)
	}})
	var status *StatusError
	if !errors.As(err, &status) || status.Status != http.StatusBadGateway {
		t.Fatalf("err = %v", err)
	}
	if !reflect.DeepEqual(waits, []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond}) {
		t.Fatalf("waits = %v", waits)
	}
}

func TestModelInputModalities(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":[`+
			`{"id":"a/audio","context_length":100,"architecture":{"input_modalities":["text","audio"],"modality":"text+audio->text"}},`+
			`{"id":"b/plain","context_length":200}]}`)
	}))
	defer server.Close()
	resetModels(t)

	client := &Client{BaseURL: server.URL}
	audio, ok := client.Model(context.Background(), "a/audio")
	if !ok || audio.ContextLength != 100 || !audio.Accepts("audio") || audio.Accepts("image") {
		t.Fatalf("audio model = %+v", audio)
	}
	plain, ok := client.Model(context.Background(), "b/plain")
	if !ok || !plain.Accepts("image") {
		t.Fatalf("plain model = %+v", plain)
	}
}

func TestPKCE(t *testing.T) {
	verifier, challenge := NewPKCE()
	if len(verifier) != 43 {
		t.Fatalf("verifier length = %d", len(verifier))
	}
	sum := sha256.Sum256([]byte(verifier))
	if challenge != base64.RawURLEncoding.EncodeToString(sum[:]) {
		t.Fatalf("challenge = %q", challenge)
	}
	if again, _ := NewPKCE(); again == verifier {
		t.Fatal("verifier repeated")
	}

	parsed, err := url.Parse(AuthURL("http://localhost:51423/callback", challenge))
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	if parsed.Scheme+"://"+parsed.Host+parsed.Path != "https://openrouter.ai/auth" ||
		query.Get("callback_url") != "http://localhost:51423/callback" ||
		query.Get("code_challenge") != challenge || query.Get("code_challenge_method") != "S256" {
		t.Fatalf("auth url = %s", parsed)
	}
	headless, _ := url.Parse(AuthURL("", challenge))
	if headless.Query().Has("callback_url") {
		t.Fatalf("headless auth url = %s", headless)
	}
}

func TestExchangeCode(t *testing.T) {
	var body map[string]string
	var header http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header = r.Header.Clone()
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/auth/keys" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["code"] != "good" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":{"message":"Invalid code or code_verifier","code":403}}`)
			return
		}
		fmt.Fprint(w, `{"key":"sk-or-v1-new"}`)
	}))
	defer server.Close()

	client := &Client{BaseURL: server.URL + "/api/v1/"}
	key, err := client.ExchangeCode(context.Background(), "good", "verifier")
	if err != nil || key != "sk-or-v1-new" {
		t.Fatalf("key = %q, err = %v", key, err)
	}
	want := map[string]string{"code": "good", "code_verifier": "verifier", "code_challenge_method": "S256"}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body = %v", body)
	}
	if header.Get("Content-Type") != "application/json" || header.Get("Authorization") != "" {
		t.Fatalf("headers = %v", header)
	}

	_, err = client.ExchangeCode(context.Background(), "bad", "verifier")
	var status *StatusError
	if !errors.As(err, &status) || status.Status != http.StatusForbidden || status.Body != "Invalid code or code_verifier" {
		t.Fatalf("err = %v", err)
	}
}

func TestExchangeCodeWithoutKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{}`)
	}))
	defer server.Close()
	if _, err := (&Client{BaseURL: server.URL}).ExchangeCode(context.Background(), "c", "v"); err == nil {
		t.Fatal("want error for a response without a key")
	}
}

// resetModels clears the process-wide model cache around a test.
func resetModels(t *testing.T) {
	modelsMu.Lock()
	modelsCache = nil
	modelsMu.Unlock()
	t.Cleanup(func() {
		modelsMu.Lock()
		modelsCache = nil
		modelsMu.Unlock()
	})
}
