package agent

import (
	"context"

	schema1 "github.com/BrokkAi/acp-go/schema/unstable"
	schema2 "github.com/BrokkAi/acp-go/schema/v2"
	agent2 "github.com/BrokkAi/acp-go/v2/agent"
)

// v2Host serves the turn loop's editor calls against a draft-v2 client. Draft
// v2 has no filesystem or terminal methods, so those answer method-not-found
// and the tools fall back to local disk and processes; permission and
// elicitation requests are translated.
type v2Host struct {
	client agent2.Client
	sink   *v2Sink
}

var _ host = (*v2Host)(nil)

func (h *v2Host) RequestPermission(ctx context.Context, request schema1.RequestPermissionRequest) (schema1.RequestPermissionResponse, error) {
	v2request := schema2.RequestPermissionRequest{
		SessionID: schema2.SessionId(request.SessionID),
		Options:   make([]schema2.PermissionOption, 0, len(request.Options)),
		Subject:   &schema2.RequestPermissionSubject{ToolCall: &schema2.ToolCallPermissionSubject{}},
	}
	if request.ToolCall.Title != nil {
		v2request.Title = *request.ToolCall.Title
	}
	if v2request.Title == "" && request.ToolCall.Name != nil {
		v2request.Title = *request.ToolCall.Name
	}
	for _, option := range request.Options {
		v2request.Options = append(v2request.Options, schema2.PermissionOption{
			OptionID: schema2.PermissionOptionId(option.OptionID),
			Name:     option.Name,
			Kind:     schema2.PermissionOptionKind(option.Kind),
		})
	}
	patch, err := v2ToolCallUpdate(request.ToolCall)
	if err != nil {
		return schema1.RequestPermissionResponse{}, err
	}
	v2request.Subject.ToolCall.ToolCall = *patch
	_ = h.sink.requiresAction()
	response, err := h.client.RequestPermission(ctx, v2request)
	_ = h.sink.running()
	if err != nil {
		return schema1.RequestPermissionResponse{}, err
	}
	converted := schema1.RequestPermissionResponse{}
	if response.Outcome.Selected != nil {
		converted.Outcome.Selected = &schema1.SelectedPermissionOutcome{OptionID: schema1.PermissionOptionId(response.Outcome.Selected.OptionID)}
	}
	return converted, nil
}

func (h *v2Host) CreateElicitation(ctx context.Context, request schema1.CreateElicitationRequest) (schema1.CreateElicitationResponse, error) {
	var v2request schema2.CreateElicitationRequest
	if err := reencode(&request, &v2request); err != nil {
		return schema1.CreateElicitationResponse{}, err
	}
	_ = h.sink.requiresAction()
	response, err := h.client.CreateElicitation(ctx, v2request)
	_ = h.sink.running()
	if err != nil {
		return schema1.CreateElicitationResponse{}, err
	}
	var converted schema1.CreateElicitationResponse
	if err := reencode(&response, &converted); err != nil {
		return schema1.CreateElicitationResponse{}, err
	}
	return converted, nil
}

func (h *v2Host) CompleteElicitation(ctx context.Context, id schema1.ElicitationId) error {
	return h.client.CompleteElicitation(ctx, schema2.ElicitationId(id))
}

// The draft-v2 baseline has no client filesystem or terminal surface.

func (h *v2Host) ReadTextFile(context.Context, schema1.ReadTextFileRequest) (schema1.ReadTextFileResponse, error) {
	return schema1.ReadTextFileResponse{}, notAdvertised(schema1.FsReadTextFileMethodName)
}

func (h *v2Host) WriteTextFile(context.Context, schema1.WriteTextFileRequest) (schema1.WriteTextFileResponse, error) {
	return schema1.WriteTextFileResponse{}, notAdvertised(schema1.FsWriteTextFileMethodName)
}

func (h *v2Host) CreateTerminal(context.Context, schema1.CreateTerminalRequest) (schema1.CreateTerminalResponse, error) {
	return schema1.CreateTerminalResponse{}, notAdvertised(schema1.TerminalCreateMethodName)
}

func (h *v2Host) TerminalOutput(context.Context, schema1.TerminalOutputRequest) (schema1.TerminalOutputResponse, error) {
	return schema1.TerminalOutputResponse{}, notAdvertised(schema1.TerminalOutputMethodName)
}

func (h *v2Host) WaitForTerminalExit(context.Context, schema1.WaitForTerminalExitRequest) (schema1.WaitForTerminalExitResponse, error) {
	return schema1.WaitForTerminalExitResponse{}, notAdvertised(schema1.TerminalWaitForExitMethodName)
}

func (h *v2Host) KillTerminal(context.Context, schema1.KillTerminalRequest) (schema1.KillTerminalResponse, error) {
	return schema1.KillTerminalResponse{}, notAdvertised(schema1.TerminalKillMethodName)
}

func (h *v2Host) ReleaseTerminal(context.Context, schema1.ReleaseTerminalRequest) (schema1.ReleaseTerminalResponse, error) {
	return schema1.ReleaseTerminalResponse{}, notAdvertised(schema1.TerminalReleaseMethodName)
}
