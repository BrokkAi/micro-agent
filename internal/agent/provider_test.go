package agent

import (
	"encoding/json"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

func TestProviderListAndSetRouteRequests(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	list, err := send[schema.ListProvidersResponse](h, schema.ProvidersListMethodName, schema.ListProvidersRequest{})
	if err != nil || len(list.Providers) != 1 {
		t.Fatalf("providers/list = %+v, %v", list, err)
	}
	info := list.Providers[0]
	if info.ProviderID != providerID || info.Required || !slices.Equal(info.Supported, []schema.LlmProtocol{schema.LlmProtocolOpenai}) {
		t.Fatalf("provider = %+v", info)
	}
	if info.Current == nil || info.Current.BaseURL != h.store.Get().BaseURL || info.Current.APIType != schema.LlmProtocolOpenai {
		t.Fatalf("current routing = %+v", info.Current)
	}

	// Point the provider at a gateway; the stored key must not follow.
	gateway := &fakeRouter{}
	server := httptest.NewServer(gateway)
	t.Cleanup(server.Close)
	set := schema.SetProviderRequest{
		ProviderID: providerID,
		APIType:    schema.LlmProtocolOpenai,
		BaseURL:    server.URL + "/v1/",
		Headers:    map[string]string{"Authorization": "Bearer gateway", "X-Gateway": "1"},
	}
	if _, err := send[schema.SetProviderResponse](h, schema.ProvidersSetMethodName, set); err != nil {
		t.Fatal(err)
	}
	list, err = send[schema.ListProvidersResponse](h, schema.ProvidersListMethodName, schema.ListProvidersRequest{})
	if err != nil || list.Providers[0].Current == nil || list.Providers[0].Current.BaseURL != server.URL+"/v1" {
		t.Fatalf("providers/list after set = %+v, %v", list, err)
	}

	gateway.responses = [][]string{textChunks("through the gateway")}
	session := h.newSession()
	if reason := h.prompt(session, "hello").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
	if len(h.router.requests) != 0 {
		t.Fatal("request went to the default endpoint")
	}
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	if len(gateway.requests) != 1 {
		t.Fatalf("gateway requests = %d", len(gateway.requests))
	}
	if got := gateway.headers[0].Get("Authorization"); got != "Bearer gateway" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := gateway.headers[0].Get("X-Gateway"); got != "1" {
		t.Fatalf("X-Gateway = %q", got)
	}
}

func TestProviderDisableAndValidation(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{})
	for name, request := range map[string]schema.SetProviderRequest{
		"unknown provider": {ProviderID: "openai", APIType: schema.LlmProtocolOpenai, BaseURL: "https://example.com"},
		"unknown protocol": {ProviderID: providerID, APIType: schema.LlmProtocolAnthropic, BaseURL: "https://example.com"},
		"relative base":    {ProviderID: providerID, APIType: schema.LlmProtocolOpenai, BaseURL: "example.com/v1"},
		"wrong scheme":     {ProviderID: providerID, APIType: schema.LlmProtocolOpenai, BaseURL: "ftp://example.com"},
	} {
		if _, err := send[json.RawMessage](h, schema.ProvidersSetMethodName, request); rpcCode(err) != -32602 {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := send[schema.DisableProviderResponse](h, schema.ProvidersDisableMethodName, schema.DisableProviderRequest{ProviderID: "other"}); err != nil {
		t.Errorf("disabling an unknown provider: %v", err)
	}
	if _, err := send[schema.DisableProviderResponse](h, schema.ProvidersDisableMethodName, schema.DisableProviderRequest{ProviderID: providerID}); err != nil {
		t.Fatal(err)
	}
	list, err := send[schema.ListProvidersResponse](h, schema.ProvidersListMethodName, schema.ListProvidersRequest{})
	if err != nil || list.Providers[0].Current != nil {
		t.Fatalf("disabled provider still routes: %+v, %v", list, err)
	}
	session := h.newSession()
	if _, err := send[schema.PromptResponse](h, schema.SessionPromptMethodName, schema.PromptRequest{SessionID: session, Prompt: []schema.ContentBlock{textBlock("hi")}}); rpcCode(err) != -32600 || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("prompt with a disabled provider: %v", err)
	}
	// providers/set re-enables the provider.
	set := schema.SetProviderRequest{ProviderID: providerID, APIType: schema.LlmProtocolOpenai, BaseURL: h.store.Get().BaseURL, Headers: map[string]string{"Authorization": "Bearer x"}}
	if _, err := send[schema.SetProviderResponse](h, schema.ProvidersSetMethodName, set); err != nil {
		t.Fatal(err)
	}
	h.router.responses = [][]string{textChunks("back")}
	if reason := h.prompt(session, "hi").StopReason; reason != schema.StopReasonEndTurn {
		t.Fatalf("stop reason %s", reason)
	}
}
