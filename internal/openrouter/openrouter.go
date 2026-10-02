// Package openrouter is a minimal streaming client for OpenRouter's
// OpenAI-compatible chat completions endpoint.
package openrouter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Message is one chat message. Content is a string or a []Part.
type Message struct {
	Role             string            `json:"role"`
	Content          any               `json:"content,omitempty"`
	Reasoning        string            `json:"reasoning,omitempty"`
	ReasoningDetails []json.RawMessage `json:"reasoning_details,omitempty"`
	ToolCalls        []ToolCall        `json:"tool_calls,omitempty"`
	ToolCallID       string            `json:"tool_call_id,omitempty"`
}

// Part is a multimodal content part.
type Part struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

type ImageURL struct {
	URL string `json:"url"`
}

type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function FunctionCall `json:"function"`
}

type FunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// Tool is a function the model may call.
type Tool struct {
	Type     string   `json:"type"`
	Function Function `json:"function"`
}

type Function struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

// Request is a chat completion request; Stream is always forced on.
type Request struct {
	Model     string     `json:"model"`
	Messages  []Message  `json:"messages"`
	Tools     []Tool     `json:"tools,omitempty"`
	MaxTokens int        `json:"max_tokens,omitempty"`
	Reasoning *Reasoning `json:"reasoning,omitempty"`
	Stream    bool       `json:"stream"`
	Usage     *struct {
		Include bool `json:"include"`
	} `json:"usage,omitempty"`
}

type Reasoning struct {
	Effort  string `json:"effort,omitempty"`
	Exclude bool   `json:"exclude,omitempty"`
}

// Usage is the token accounting reported at the end of a stream.
type Usage struct {
	PromptTokens     uint64  `json:"prompt_tokens"`
	CompletionTokens uint64  `json:"completion_tokens"`
	TotalTokens      uint64  `json:"total_tokens"`
	Cost             float64 `json:"cost"`
}

// Handler receives stream deltas as they arrive.
type Handler struct {
	Text      func(string) error
	Reasoning func(string) error
}

// Result is the assembled assistant turn.
type Result struct {
	Message      Message
	FinishReason string
	Usage        *Usage
}

// Client talks to OpenRouter.
type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

type chunk struct {
	Choices []struct {
		Delta struct {
			Content          string            `json:"content"`
			Reasoning        string            `json:"reasoning"`
			ReasoningDetails []json.RawMessage `json:"reasoning_details"`
			ToolCalls        []struct {
				Index    int    `json:"index"`
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage    `json:"usage"`
	Error *apiError `json:"error"`
}

type apiError struct {
	Code    any    `json:"code"`
	Message string `json:"message"`
}

// StatusError is a non-2xx HTTP response.
type StatusError struct {
	Status int
	Body   string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("openrouter: HTTP %d: %s", e.Status, e.Body)
}

// Stream sends request and assembles the streamed response, invoking handler
// for each text and reasoning delta. Transient failures before any output are
// retried.
func (c *Client) Stream(ctx context.Context, request Request, handler Handler) (Result, error) {
	request.Stream = true
	request.Usage = &struct {
		Include bool `json:"include"`
	}{Include: true}
	body, err := json.Marshal(request)
	if err != nil {
		return Result{}, err
	}
	var response *http.Response
	for attempt := 0; ; attempt++ {
		response, err = c.post(ctx, body)
		if err == nil {
			break
		}
		var status *StatusError
		retryable := errors.As(err, &status) && (status.Status == 429 || status.Status >= 500)
		if !retryable || attempt >= 3 || ctx.Err() != nil {
			return Result{}, err
		}
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(time.Duration(1<<attempt) * time.Second):
		}
	}
	defer response.Body.Close()
	return assemble(response.Body, handler)
}

func (c *Client) post(ctx context.Context, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.APIKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("HTTP-Referer", "https://brokk.ai")
	request.Header.Set("X-Title", "Brokk micro-agent")
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode/100 != 2 {
		defer response.Body.Close()
		data, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		message := strings.TrimSpace(string(data))
		var wrapped struct {
			Error apiError `json:"error"`
		}
		if json.Unmarshal(data, &wrapped) == nil && wrapped.Error.Message != "" {
			message = wrapped.Error.Message
		}
		return nil, &StatusError{Status: response.StatusCode, Body: message}
	}
	return response, nil
}

func assemble(body io.Reader, handler Handler) (Result, error) {
	var result Result
	result.Message.Role = "assistant"
	var text, reasoning strings.Builder
	var calls []ToolCall
	details := &detailMerger{}

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		line := scanner.Text()
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue // comments such as ": OPENROUTER PROCESSING" and blank separators
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			break
		}
		var event chunk
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return result, fmt.Errorf("openrouter: bad stream chunk: %w", err)
		}
		if event.Error != nil {
			return result, fmt.Errorf("openrouter: %s", event.Error.Message)
		}
		if event.Usage != nil {
			result.Usage = event.Usage
		}
		for _, choice := range event.Choices {
			delta := choice.Delta
			if delta.Reasoning != "" {
				reasoning.WriteString(delta.Reasoning)
				if handler.Reasoning != nil {
					if err := handler.Reasoning(delta.Reasoning); err != nil {
						return result, err
					}
				}
			}
			details.add(delta.ReasoningDetails)
			if delta.Content != "" {
				text.WriteString(delta.Content)
				if handler.Text != nil {
					if err := handler.Text(delta.Content); err != nil {
						return result, err
					}
				}
			}
			for _, call := range delta.ToolCalls {
				for len(calls) <= call.Index {
					calls = append(calls, ToolCall{Type: "function"})
				}
				target := &calls[call.Index]
				if call.ID != "" {
					target.ID = call.ID
				}
				target.Function.Name += call.Function.Name
				target.Function.Arguments += call.Function.Arguments
			}
			if choice.FinishReason != nil {
				result.FinishReason = *choice.FinishReason
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if text.Len() > 0 {
		result.Message.Content = text.String()
	}
	result.Message.Reasoning = reasoning.String()
	result.Message.ReasoningDetails = details.result()
	for i := range calls {
		if calls[i].ID == "" {
			calls[i].ID = fmt.Sprintf("call_%d_%d", time.Now().UnixNano(), i)
		}
		if strings.TrimSpace(calls[i].Function.Arguments) == "" {
			calls[i].Function.Arguments = "{}"
		}
	}
	result.Message.ToolCalls = calls
	return result, nil
}

// detailMerger folds streamed reasoning_details fragments back into whole
// entries so they can be returned to the provider on the next request.
// Fragments sharing a type and index are concatenated.
type detailMerger struct {
	order []string
	items map[string]map[string]any
}

func (m *detailMerger) add(fragments []json.RawMessage) {
	for _, raw := range fragments {
		var fragment map[string]any
		if json.Unmarshal(raw, &fragment) != nil {
			continue
		}
		key := fmt.Sprintf("%v/%v", fragment["type"], fragment["index"])
		if m.items == nil {
			m.items = map[string]map[string]any{}
		}
		existing, ok := m.items[key]
		if !ok {
			m.items[key] = fragment
			m.order = append(m.order, key)
			continue
		}
		for field, value := range fragment {
			previous, isString := existing[field].(string)
			addition, addIsString := value.(string)
			if isString && addIsString && (field == "text" || field == "summary" || field == "data") {
				existing[field] = previous + addition
			} else if value != nil {
				existing[field] = value
			}
		}
	}
}

func (m *detailMerger) result() []json.RawMessage {
	var out []json.RawMessage
	for _, key := range m.order {
		if data, err := json.Marshal(m.items[key]); err == nil {
			out = append(out, data)
		}
	}
	return out
}

// ModelInfo is the subset of OpenRouter model metadata the agent uses.
type ModelInfo struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	ContextLength uint64 `json:"context_length"`
}

var (
	modelsMu    sync.Mutex
	modelsCache map[string]ModelInfo
)

// Model looks up metadata for id, fetching and caching the public model list
// once per process.
func (c *Client) Model(ctx context.Context, id string) (ModelInfo, bool) {
	modelsMu.Lock()
	defer modelsMu.Unlock()
	if modelsCache == nil {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/models", nil)
		if err != nil {
			return ModelInfo{}, false
		}
		client := c.HTTP
		if client == nil {
			client = http.DefaultClient
		}
		response, err := client.Do(request)
		if err != nil {
			return ModelInfo{}, false
		}
		defer response.Body.Close()
		var list struct {
			Data []ModelInfo `json:"data"`
		}
		if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&list) != nil {
			return ModelInfo{}, false
		}
		modelsCache = make(map[string]ModelInfo, len(list.Data))
		for _, model := range list.Data {
			modelsCache[model.ID] = model
		}
	}
	model, ok := modelsCache[id]
	return model, ok
}
