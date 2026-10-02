package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/BrokkAi/acp-go/schema"
	"github.com/BrokkAi/micro-agent/internal/config"
)

type command struct {
	name, description, hint string
}

var commands = []command{
	{"help", "Show available commands", ""},
	{"model", "Show or switch the model (any OpenRouter model slug)", "model id, e.g. openai/gpt-5"},
	{"mode", "Show or switch the permission mode", "default | accept_edits | plan | bypass"},
	{"effort", "Show or set reasoning effort", "default | none | minimal | low | medium | high"},
	{"config", "Edit settings in a form, or set them inline", "key=value ..."},
	{"login", "Set the OpenRouter API key", "API key (optional; prompts if omitted)"},
	{"logout", "Remove the stored OpenRouter API key", ""},
	{"mcp", "List connected MCP servers and their tools", ""},
	{"clear", "Clear the conversation history of this session", ""},
}

func commandList() []schema.AvailableCommand {
	var list []schema.AvailableCommand
	for _, c := range commands {
		available := schema.AvailableCommand{Name: c.name, Description: c.description}
		if c.hint != "" {
			available.Input = &schema.AvailableCommandInput{Hint: c.hint}
		}
		list = append(list, available)
	}
	return list
}

// parseCommand recognizes a prompt that is a single known slash command.
func parseCommand(blocks []schema.ContentBlock) (string, string, bool) {
	if len(blocks) == 0 || blocks[0].Text == nil {
		return "", "", false
	}
	text := strings.TrimSpace(blocks[0].Text.Text)
	rest, ok := strings.CutPrefix(text, "/")
	if !ok {
		return "", "", false
	}
	name, args, _ := strings.Cut(rest, " ")
	for _, c := range commands {
		if c.name == name {
			return name, strings.TrimSpace(args), true
		}
	}
	return "", "", false
}

func (t *turn) say(format string, args ...any) {
	_ = t.updates.Update(schema.SessionUpdate{AgentMessageChunk: &schema.ContentChunk{Content: textBlock(fmt.Sprintf(format, args...))}})
}

func (t *turn) command(ctx context.Context, name, args string) (schema.PromptResponse, error) {
	done := schema.PromptResponse{StopReason: schema.StopReasonEndTurn}
	optionsChanged := func() {
		_ = t.updates.Update(schema.SessionUpdate{ConfigOptionUpdate: &schema.ConfigOptionUpdate{ConfigOptions: t.a.configOptions(t.s)}})
		_ = t.a.save(t.s)
	}
	switch name {
	case "help":
		var b strings.Builder
		b.WriteString("**Commands**\n\n")
		for _, c := range commands {
			fmt.Fprintf(&b, "- `/%s` — %s\n", c.name, c.description)
		}
		fmt.Fprintf(&b, "\nConfig file: `%s`\n", t.a.cfg.Path())
		t.say("%s", b.String())

	case "model":
		model := args
		if model == "" {
			cfg := t.a.cfg.Get()
			cfg.Model = t.s.Model
			values, ok := t.form(ctx, "Choose a model (any OpenRouter model slug)", []field{{
				key: "model", title: "Model", value: t.s.Model,
				description: "Known: " + strings.Join(config.ModelChoices(cfg), ", "),
			}})
			if !ok {
				t.say("Current model: `%s`. Use `/model <slug>` to switch.", t.s.Model)
				return done, nil
			}
			model = values["model"]
		}
		if err := t.a.setModel(t.s, model); err != nil {
			t.say("Could not set model: %v", err)
			return done, nil
		}
		optionsChanged()
		t.say("Model set to `%s`.", t.s.Model)

	case "mode":
		if args == "" {
			var b strings.Builder
			fmt.Fprintf(&b, "Current mode: `%s`\n\n", t.mode())
			for _, m := range modes {
				fmt.Fprintf(&b, "- `%s` — %s\n", m.id, m.description)
			}
			t.say("%s", b.String())
			return done, nil
		}
		if err := t.a.setMode(ctx, t.client, t.s, args, true); err != nil {
			t.say("%v", err)
			return done, nil
		}
		t.say("Mode set to `%s`.", args)

	case "effort":
		if args == "" {
			effort := t.s.Effort
			if effort == "" {
				effort = defaultEffort
			}
			t.say("Reasoning effort: `%s`. Options: default, %s.", effort, strings.Join(config.Efforts, ", "))
			return done, nil
		}
		if err := t.a.setEffort(t.s, args); err != nil {
			t.say("%v", err)
			return done, nil
		}
		optionsChanged()
		t.say("Reasoning effort set to `%s`.", args)

	case "config":
		saved := t.configure(ctx, args)
		_, model := saved["model"]
		_, effort := saved["reasoning_effort"]
		if model || effort {
			cfg := t.a.cfg.Get()
			t.s.mu.Lock()
			if model {
				t.s.Model = cfg.Model
			}
			if effort {
				t.s.Effort = cfg.ReasoningEffort
			}
			t.s.mu.Unlock()
			optionsChanged()
		}

	case "login":
		if t.login(ctx, args) {
			t.say("OpenRouter API key saved to `%s`.", t.a.cfg.Path())
		} else if ctx.Err() == nil {
			t.say("No key entered. Use `/login <key>` or set `OPENROUTER_API_KEY`.")
		}

	case "logout":
		if _, err := t.a.Logout(ctx, t.client, schema.LogoutRequest{}); err != nil {
			t.say("Logout failed: %v", err)
		} else {
			t.say("Stored API key removed.")
		}

	case "mcp":
		t.s.mu.Lock()
		servers := t.s.servers
		t.s.mu.Unlock()
		if len(servers) == 0 {
			t.say("No MCP servers connected. Configure them in your editor or under `mcp_servers` in `%s`.", t.a.cfg.Path())
			return done, nil
		}
		var b strings.Builder
		for _, server := range servers {
			tools, err := server.Tools(ctx)
			if err != nil {
				fmt.Fprintf(&b, "- **%s** — error: %v\n", server.Name, err)
				continue
			}
			fmt.Fprintf(&b, "- **%s** — %d tools\n", server.Name, len(tools))
			for _, tool := range tools {
				fmt.Fprintf(&b, "  - `%s`\n", tool.Name)
			}
		}
		t.say("%s", b.String())

	case "clear":
		t.s.mu.Lock()
		t.s.Messages = nil
		t.s.Cost = 0
		t.s.mu.Unlock()
		_ = t.a.save(t.s)
		t.say("Conversation cleared.")
	}
	return done, nil
}

// configure applies inline key=value pairs, or presents a form of all settings.
// It returns the settings that were saved.
func (t *turn) configure(ctx context.Context, args string) map[string]string {
	values := map[string]string{}
	if args != "" {
		for _, pair := range strings.Fields(args) {
			key, value, ok := strings.Cut(pair, "=")
			if !ok {
				t.say("Expected key=value, got `%s`. Keys: %s", pair, strings.Join(config.Keys, ", "))
				return nil
			}
			values[key] = value
		}
	} else {
		cfg := t.a.cfg.Get()
		var fields []field
		for _, key := range config.Keys {
			f := field{key: key, title: key, value: config.Value(cfg, key)}
			switch key {
			case "reasoning_effort":
				f.enum = append([]string{""}, config.Efforts...)
			case "default_mode":
				for _, m := range modes {
					f.enum = append(f.enum, m.id)
				}
			case "max_turns", "max_tokens", "shell_timeout_seconds":
				f.integer = true
			}
			fields = append(fields, f)
		}
		submitted, ok := t.form(ctx, "micro-agent settings (saved to "+t.a.cfg.Path()+")", fields)
		if !ok {
			t.showConfig()
			return nil
		}
		for key, value := range submitted {
			if value != config.Value(cfg, key) {
				values[key] = value
			}
		}
	}
	if len(values) == 0 {
		t.say("No changes.")
		return nil
	}
	err := t.a.cfg.Update(func(c *config.Config) error {
		for key, value := range values {
			if key == "api_key" {
				return errors.New("use /login to set the API key")
			}
			if err := config.Set(c, key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.say("Settings not saved: %v", err)
		return nil
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, "`"+key+"`")
	}
	sort.Strings(keys)
	t.say("Saved %s.", strings.Join(keys, ", "))
	return values
}

func (t *turn) showConfig() {
	cfg := t.a.cfg.Get()
	var b strings.Builder
	fmt.Fprintf(&b, "Settings in `%s` (change with `/config key=value`):\n\n", t.a.cfg.Path())
	for _, key := range config.Keys {
		fmt.Fprintf(&b, "- `%s` = `%s`\n", key, config.Value(cfg, key))
	}
	t.say("%s", b.String())
}

// login stores key, or asks for one with a form when key is empty.
func (t *turn) login(ctx context.Context, key string) bool {
	if key == "" {
		values, ok := t.form(ctx, "Enter your OpenRouter API key (https://openrouter.ai/keys). It is saved to "+t.a.cfg.Path()+".", []field{{key: "api_key", title: "OpenRouter API key", required: true}})
		if !ok {
			return false
		}
		key = strings.TrimSpace(values["api_key"])
	}
	if key == "" {
		return false
	}
	if err := t.a.cfg.Update(func(c *config.Config) error { c.APIKey = key; return nil }); err != nil {
		t.say("Could not save API key: %v", err)
		return false
	}
	return true
}

// field is one form input.
type field struct {
	key, title, description, value string
	enum                           []string
	integer, required              bool
}

// formRequest is the elicitation/create payload. It is hand-written because
// the generated schema type does not carry requestedSchema.
type formRequest struct {
	SessionID       schema.SessionId         `json:"sessionId"`
	Mode            string                   `json:"mode"`
	Message         string                   `json:"message"`
	RequestedSchema schema.ElicitationSchema `json:"requestedSchema"`
}

// form asks the user to fill fields. It returns false when the client cannot
// show forms or the user declines.
func (t *turn) form(ctx context.Context, message string, fields []field) (map[string]string, bool) {
	if !t.a.canForm() {
		return nil, false
	}
	object := schema.ElicitationSchemaTypeObject
	requested := schema.ElicitationSchema{Type: &object, Properties: map[string]schema.ElicitationPropertySchema{}}
	for _, f := range fields {
		var property schema.ElicitationPropertySchema
		var description *string
		if f.description != "" {
			description = ptr(f.description)
		}
		switch {
		case f.integer:
			integer := &schema.IntegerPropertySchema{Title: ptr(f.title), Description: description, Minimum: ptr(int64(0))}
			if n, err := strconv.ParseInt(f.value, 10, 64); err == nil {
				integer.Default = &n
			}
			property.Integer = integer
		default:
			str := &schema.StringPropertySchema{Title: ptr(f.title), Description: description, Enum: f.enum}
			if f.value != "" || len(f.enum) > 0 {
				str.Default = ptr(f.value)
			}
			property.String = str
		}
		requested.Properties[f.key] = property
		if f.required {
			requested.Required = append(requested.Required, f.key)
		}
	}
	var response schema.CreateElicitationResponse
	err := t.client.Call(ctx, schema.ElicitationCreateMethodName, formRequest{
		SessionID: t.s.ID, Mode: "form", Message: message, RequestedSchema: requested,
	}, &response)
	if err != nil || response.Accept == nil {
		return nil, false
	}
	values := map[string]string{}
	for key, value := range response.Accept.Content {
		switch v := value.(type) {
		case string:
			values[key] = v
		case float64:
			values[key] = strconv.FormatInt(int64(v), 10)
		case nil:
		default:
			values[key] = fmt.Sprint(v)
		}
	}
	for _, f := range fields {
		if _, ok := values[f.key]; !ok && !slices.Contains(requested.Required, f.key) {
			values[f.key] = f.value
		}
	}
	return values, true
}
