package agent

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	schema1 "github.com/BrokkAi/acp-go/schema/unstable"
	schema2 "github.com/BrokkAi/acp-go/schema/v2"
)

// updateSender sends one assembled draft-v2 session update.
type updateSender interface {
	Update(schema2.SessionUpdate) error
}

// v2Sink translates the v1-shaped updates the shared turn loop and replay
// produce into draft-v2 session updates.
type v2Sink struct {
	mu      sync.Mutex
	send    updateSender
	message schema2.MessageId // current agent message
	thought schema2.MessageId // current thought message
	user    *schema2.UserMessage
}

func newV2Sink(send updateSender) *v2Sink { return &v2Sink{send: send} }

func (s *v2Sink) Update(update schema1.SessionUpdate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case update.UserMessageChunk != nil:
		block, err := v2Block(update.UserMessageChunk.Content)
		if err != nil {
			return err
		}
		if s.user == nil {
			s.user = &schema2.UserMessage{MessageID: newV2MessageID()}
		}
		s.user.Content.Set = true
		s.user.Content.Value = append(s.user.Content.Value, block)
		return nil
	case update.AgentMessageChunk != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		if s.message == "" {
			s.message = newV2MessageID()
		}
		block, err := v2Block(update.AgentMessageChunk.Content)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{AgentMessageChunk: &schema2.ContentChunk{MessageID: s.message, Content: block}})
	case update.AgentThoughtChunk != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		if s.thought == "" {
			s.thought = newV2MessageID()
		}
		block, err := v2Block(update.AgentThoughtChunk.Content)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{AgentThoughtChunk: &schema2.ContentChunk{MessageID: s.thought, Content: block}})
	case update.ToolCall != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		s.message, s.thought = "", ""
		patch, err := v2ToolCall(*update.ToolCall)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{ToolCallUpdate: patch})
	case update.ToolCallUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		patch, err := v2ToolCallUpdate(*update.ToolCallUpdate)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{ToolCallUpdate: patch})
	case update.Plan != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		entries, err := v2PlanEntries(update.Plan.Entries)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{PlanUpdate: &schema2.PlanUpdate{Plan: schema2.PlanUpdateContent{
			Items: &schema2.PlanItems{PlanID: schema2.PlanId(planID), Entries: entries},
		}}})
	case update.PlanUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		if items := update.PlanUpdate.Plan.Items; items != nil {
			entries, err := v2PlanEntries(items.Entries)
			if err != nil {
				return err
			}
			return s.send.Update(schema2.SessionUpdate{PlanUpdate: &schema2.PlanUpdate{Plan: schema2.PlanUpdateContent{
				Items: &schema2.PlanItems{PlanID: schema2.PlanId(items.PlanID), Entries: entries},
			}}})
		}
		// Markdown plans have no draft-v2 shape.
		return nil
	case update.PlanRemoved != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{PlanUpdate: &schema2.PlanUpdate{Plan: schema2.PlanUpdateContent{
			Items: &schema2.PlanItems{PlanID: schema2.PlanId(update.PlanRemoved.PlanID), Entries: []schema2.PlanEntry{}},
		}}})
	case update.AvailableCommandsUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		commands, err := v2Commands(update.AvailableCommandsUpdate.AvailableCommands)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{AvailableCommandsUpdate: &schema2.AvailableCommandsUpdate{AvailableCommands: commands}})
	case update.ConfigOptionUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		options, err := v2ConfigOptions(update.ConfigOptionUpdate.ConfigOptions)
		if err != nil {
			return err
		}
		return s.send.Update(schema2.SessionUpdate{ConfigOptionUpdate: &schema2.ConfigOptionUpdate{ConfigOptions: options}})
	case update.SessionInfoUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		title := schema2.Nullable[string]{Set: true}
		if update.SessionInfoUpdate.Title != nil {
			title.Value = *update.SessionInfoUpdate.Title
		}
		return s.send.Update(schema2.SessionUpdate{SessionInfoUpdate: &schema2.SessionInfoUpdate{Title: title}})
	case update.UsageUpdate != nil:
		if err := s.flushUser(); err != nil {
			return err
		}
		usage := &schema2.UsageUpdate{
			Used: update.UsageUpdate.Used,
			Size: update.UsageUpdate.Size,
		}
		if update.UsageUpdate.Cost != nil {
			usage.Cost = &schema2.Cost{Amount: update.UsageUpdate.Cost.Amount, Currency: update.UsageUpdate.Cost.Currency}
		}
		return s.send.Update(schema2.SessionUpdate{UsageUpdate: usage})
	}
	// Mode updates travel as config options; notices, compactions and other
	// v1-unstable updates have no draft-v2 shape.
	return nil
}

// Flush sends an accumulated user message, for the end of a replay.
func (s *v2Sink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushUser()
}

// flushUser sends the accumulated user message; the caller holds s.mu.
func (s *v2Sink) flushUser() error {
	if s.user == nil {
		return nil
	}
	message := *s.user
	s.user = nil
	return s.send.Update(schema2.SessionUpdate{UserMessage: &message})
}

// running and requiresAction report the session's work state.
func (s *v2Sink) running() error {
	return s.send.Update(schema2.SessionUpdate{StateUpdate: &schema2.StateUpdate{Running: &schema2.RunningStateUpdate{}}})
}

func (s *v2Sink) requiresAction() error {
	return s.send.Update(schema2.SessionUpdate{StateUpdate: &schema2.StateUpdate{RequiresAction: &schema2.RequiresActionStateUpdate{}}})
}

// idle reports that foreground work stopped.
func (s *v2Sink) idle(reason schema2.StopReason) error {
	return s.send.Update(schema2.SessionUpdate{StateUpdate: &schema2.StateUpdate{Idle: &schema2.IdleStateUpdate{StopReason: &reason}}})
}

func newV2MessageID() schema2.MessageId {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return schema2.MessageId(hex.EncodeToString(b[:]))
}

// reencode converts one protocol value between schema packages through its
// wire form; the drafts share the JSON shape for value types.
func reencode(from, to any) error {
	data, err := json.Marshal(from)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, to)
}

func v2Block(block schema1.ContentBlock) (schema2.ContentBlock, error) {
	var converted schema2.ContentBlock
	if err := reencode(&block, &converted); err != nil {
		return converted, fmt.Errorf("content block: %w", err)
	}
	return converted, nil
}

func v2PlanEntries(entries []schema1.PlanEntry) ([]schema2.PlanEntry, error) {
	if entries == nil {
		entries = []schema1.PlanEntry{}
	}
	converted := make([]schema2.PlanEntry, 0, len(entries))
	for _, entry := range entries {
		var next schema2.PlanEntry
		if err := reencode(&entry, &next); err != nil {
			return nil, err
		}
		converted = append(converted, next)
	}
	return converted, nil
}

func v2Commands(commands []schema1.AvailableCommand) ([]schema2.AvailableCommand, error) {
	converted := make([]schema2.AvailableCommand, 0, len(commands))
	for _, command := range commands {
		var next schema2.AvailableCommand
		if err := reencode(&command, &next); err != nil {
			return nil, err
		}
		converted = append(converted, next)
	}
	return converted, nil
}

// v2ConfigOptions maps the v1 option shape, whose ID field and select options
// differ from draft v2, by hand.
func v2ConfigOptions(options []schema1.SessionConfigOption) ([]schema2.SessionConfigOption, error) {
	converted := make([]schema2.SessionConfigOption, 0, len(options))
	for _, option := range options {
		next := schema2.SessionConfigOption{ConfigID: schema2.SessionConfigId(option.ID), Name: option.Name}
		if option.Category != nil {
			category := schema2.SessionConfigOptionCategory(*option.Category)
			next.Category = &category
		}
		if option.Description != nil {
			next.Description = schema2.Nullable[string]{Set: true, Value: *option.Description}
		}
		if option.Select != nil {
			selectOptions, err := v2SelectOptions(option.Select.Options)
			if err != nil {
				return nil, err
			}
			next.Select = &schema2.SessionConfigSelect{CurrentValue: schema2.SessionConfigValueId(option.Select.CurrentValue), Options: selectOptions}
		}
		if option.Boolean != nil {
			next.Boolean = &schema2.SessionConfigBoolean{CurrentValue: option.Boolean.CurrentValue}
		}
		converted = append(converted, next)
	}
	return converted, nil
}

func v2SelectOptions(options schema1.SessionConfigSelectOptions) (schema2.SessionConfigSelectOptions, error) {
	switch value := options.(type) {
	case []schema1.SessionConfigSelectOption:
		converted := make([]schema2.SessionConfigSelectOption, 0, len(value))
		for _, option := range value {
			var next schema2.SessionConfigSelectOption
			if err := reencode(&option, &next); err != nil {
				return nil, err
			}
			converted = append(converted, next)
		}
		return converted, nil
	case []schema1.SessionConfigSelectGroup:
		converted := make([]schema2.SessionConfigSelectGroup, 0, len(value))
		for _, group := range value {
			next := schema2.SessionConfigSelectGroup{GroupID: schema2.SessionConfigGroupId(group.Group), Name: group.Name}
			for _, option := range group.Options {
				var item schema2.SessionConfigSelectOption
				if err := reencode(&option, &item); err != nil {
					return nil, err
				}
				next.Options = append(next.Options, item)
			}
			converted = append(converted, next)
		}
		return converted, nil
	}
	return options, nil
}

// v2ToolCall renders a new tool call as the upsert draft v2 expects.
func v2ToolCall(call schema1.ToolCall) (*schema2.ToolCallUpdate, error) {
	patch := &schema2.ToolCallUpdate{ToolCallID: schema2.ToolCallId(call.ToolCallID), RawInput: call.RawInput, RawOutput: call.RawOutput}
	if call.Name != nil {
		patch.Name = schema2.Nullable[string]{Set: true, Value: *call.Name}
	}
	patch.Title = schema2.Nullable[string]{Set: true, Value: call.Title}
	if call.Kind != nil {
		kind := schema2.ToolKind(*call.Kind)
		patch.Kind = &kind
	}
	if call.Status != nil {
		status := schema2.ToolCallStatus(*call.Status)
		patch.Status = &status
	}
	locations, err := v2Locations(call.Locations)
	if err != nil {
		return nil, err
	}
	if locations != nil {
		patch.Locations = schema2.Nullable[[]schema2.ToolCallLocation]{Set: true, Value: locations}
	}
	content, err := v2ToolContent(call.Content)
	if err != nil {
		return nil, err
	}
	if content != nil {
		patch.Content = schema2.Nullable[[]schema2.ToolCallContent]{Set: true, Value: content}
	}
	return patch, nil
}

// v2ToolCallUpdate patches an existing tool call.
func v2ToolCallUpdate(update schema1.ToolCallUpdate) (*schema2.ToolCallUpdate, error) {
	patch := &schema2.ToolCallUpdate{ToolCallID: schema2.ToolCallId(update.ToolCallID)}
	if update.Name != nil {
		patch.Name = schema2.Nullable[string]{Set: true, Value: *update.Name}
	}
	if update.Title != nil {
		patch.Title = schema2.Nullable[string]{Set: true, Value: *update.Title}
	}
	if update.Kind != nil {
		kind := schema2.ToolKind(*update.Kind)
		patch.Kind = &kind
	}
	if update.Status != nil {
		status := schema2.ToolCallStatus(*update.Status)
		patch.Status = &status
	}
	if update.RawInput != nil {
		patch.RawInput = update.RawInput
	}
	if update.RawOutput != nil {
		patch.RawOutput = update.RawOutput
	}
	locations, err := v2Locations(update.Locations)
	if err != nil {
		return nil, err
	}
	if locations != nil {
		patch.Locations = schema2.Nullable[[]schema2.ToolCallLocation]{Set: true, Value: locations}
	}
	content, err := v2ToolContent(update.Content)
	if err != nil {
		return nil, err
	}
	if content != nil {
		patch.Content = schema2.Nullable[[]schema2.ToolCallContent]{Set: true, Value: content}
	}
	return patch, nil
}

func v2Locations(locations []schema1.ToolCallLocation) ([]schema2.ToolCallLocation, error) {
	if locations == nil {
		return nil, nil
	}
	converted := make([]schema2.ToolCallLocation, 0, len(locations))
	for _, location := range locations {
		next := schema2.ToolCallLocation{Path: schema2.AbsolutePath(location.Path)}
		if location.Line != nil {
			next.Line = schema2.Nullable[uint32]{Set: true, Value: *location.Line}
		}
		converted = append(converted, next)
	}
	return converted, nil
}

// v2ToolContent converts tool call content. Draft v2 diffs carry path changes
// and an optional patch, not old and new text, so a v1 diff becomes its path.
func v2ToolContent(content []schema1.ToolCallContent) ([]schema2.ToolCallContent, error) {
	if content == nil {
		return nil, nil
	}
	converted := make([]schema2.ToolCallContent, 0, len(content))
	for _, item := range content {
		switch {
		case item.Content != nil:
			block, err := v2Block(item.Content.Content)
			if err != nil {
				return nil, err
			}
			converted = append(converted, schema2.ToolCallContent{Content: &schema2.Content{Content: block}})
		case item.Diff != nil:
			converted = append(converted, schema2.ToolCallContent{Diff: &schema2.Diff{
				Changes: []schema2.DiffChange{{Modify: &schema2.DiffPathChange{Path: schema2.AbsolutePath(item.Diff.Path)}}},
			}})
		case item.Terminal != nil:
			converted = append(converted, schema2.ToolCallContent{Terminal: &schema2.Terminal{TerminalID: schema2.TerminalId(item.Terminal.TerminalID)}})
		}
	}
	return converted, nil
}
