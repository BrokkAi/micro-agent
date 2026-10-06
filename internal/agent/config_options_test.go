package agent

import (
	"encoding/json"
	"testing"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

// configOption returns the session/new config option with id.
func configOption(options []schema.SessionConfigOption, id schema.SessionConfigId) *schema.SessionConfigOption {
	for i := range options {
		if options[i].ID == id {
			return &options[i]
		}
	}
	return nil
}

func TestBooleanConfigOption(t *testing.T) {
	h := newHarness(t, schema.ClientCapabilities{Session: &schema.ClientSessionCapabilities{
		ConfigOptions: &schema.SessionConfigOptionsCapabilities{Boolean: &schema.BooleanConfigOptionCapabilities{}},
	}})
	created, err := send[schema.NewSessionResponse](h, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: h.dir, MCPServers: []schema.McpServer{}})
	if err != nil {
		t.Fatal(err)
	}
	option := configOption(created.ConfigOptions, "auto_compact")
	if option == nil || option.Boolean == nil || !option.Boolean.CurrentValue {
		t.Fatalf("auto_compact option = %+v", option)
	}
	response, err := send[schema.SetSessionConfigOptionResponse](h, schema.SessionSetConfigOptionMethodName, schema.SetSessionConfigOptionRequest{
		SessionID: created.SessionID,
		ConfigID:  "auto_compact",
		Boolean:   &schema.SetSessionConfigOptionRequestBoolean{Value: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	if option := configOption(response.ConfigOptions, "auto_compact"); option == nil || option.Boolean == nil || option.Boolean.CurrentValue {
		t.Fatalf("auto_compact after set = %+v", option)
	}
	if h.store.Get().AutoCompactEnabled() {
		t.Fatal("auto_compact was not saved")
	}
	// A boolean value for a select option, and the reverse, are rejected.
	if _, err := send[json.RawMessage](h, schema.SessionSetConfigOptionMethodName, schema.SetSessionConfigOptionRequest{
		SessionID: created.SessionID,
		ConfigID:  "mode",
		Boolean:   &schema.SetSessionConfigOptionRequestBoolean{Value: true},
	}); rpcCode(err) != -32602 {
		t.Fatalf("boolean for mode: %v", err)
	}
	if _, err := send[json.RawMessage](h, schema.SessionSetConfigOptionMethodName, schema.SetSessionConfigOptionRequest{
		SessionID: created.SessionID,
		ConfigID:  "auto_compact",
		ValueID:   &schema.SetSessionConfigOptionRequestValueID{Value: "true"},
	}); rpcCode(err) != -32602 {
		t.Fatalf("value for auto_compact: %v", err)
	}

	// Clients that do not advertise boolean options do not see it.
	plain := newHarness(t, schema.ClientCapabilities{})
	plainCreated, err := send[schema.NewSessionResponse](plain, schema.SessionNewMethodName, schema.NewSessionRequest{Cwd: plain.dir, MCPServers: []schema.McpServer{}})
	if err != nil {
		t.Fatal(err)
	}
	if option := configOption(plainCreated.ConfigOptions, "auto_compact"); option != nil {
		t.Fatalf("boolean option sent to a client without the capability: %+v", option)
	}
}
