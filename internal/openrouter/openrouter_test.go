package openrouter

import (
	"errors"
	"strings"
	"testing"
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
