package agent

import (
	"context"

	schema "github.com/BrokkAi/acp-go/schema/unstable"
)

// updateSink accepts the session updates a turn produces. The v1 connection
// and the draft-v2 facade each provide an implementation.
type updateSink interface {
	Update(schema.SessionUpdate) error
}

// host is the editor side the turn loop talks to. The v1 connection speaks
// the protocol directly; the draft-v2 facade translates the calls v2 has and
// answers the rest with method-not-found, which makes the tools fall back to
// local disk and processes.
type host interface {
	ReadTextFile(context.Context, schema.ReadTextFileRequest) (schema.ReadTextFileResponse, error)
	WriteTextFile(context.Context, schema.WriteTextFileRequest) (schema.WriteTextFileResponse, error)
	RequestPermission(context.Context, schema.RequestPermissionRequest) (schema.RequestPermissionResponse, error)
	CreateElicitation(context.Context, schema.CreateElicitationRequest) (schema.CreateElicitationResponse, error)
	CompleteElicitation(context.Context, schema.ElicitationId) error
	CreateTerminal(context.Context, schema.CreateTerminalRequest) (schema.CreateTerminalResponse, error)
	TerminalOutput(context.Context, schema.TerminalOutputRequest) (schema.TerminalOutputResponse, error)
	WaitForTerminalExit(context.Context, schema.WaitForTerminalExitRequest) (schema.WaitForTerminalExitResponse, error)
	KillTerminal(context.Context, schema.KillTerminalRequest) (schema.KillTerminalResponse, error)
	ReleaseTerminal(context.Context, schema.ReleaseTerminalRequest) (schema.ReleaseTerminalResponse, error)
}
