# micro-agent

A minimal [Agent Client Protocol](https://agentclientprotocol.com) coding agent from Brokk, backed by [OpenRouter](https://openrouter.ai). Its only dependency is [`acp-go`](https://github.com/BrokkAi/acp-go), which is itself standard-library only.

**Tools:** `shell`, `read_file`, `edit_file`, `write_file`, `update_plan`, plus every tool from connected MCP servers (stdio, streamable HTTP, legacy HTTP+SSE, and servers the client hosts on the ACP connection itself).

```sh
go install github.com/BrokkAi/micro-agent@latest
micro-agent login        # or set OPENROUTER_API_KEY, or use /login in your editor
```

Release binaries for Linux, macOS and Windows (amd64 and arm64) with
checksums are on the [releases page](https://github.com/BrokkAi/micro-agent/releases).

Point your ACP client (Zed, etc.) at the `micro-agent` binary.

## ACP support

- Sessions: new, load (with history replay), resume, fork, list, close, delete, additional directories
- Modes: `default`, `accept_edits`, `plan` (read-only), `bypass`. Also exposed as config options along with model and reasoning effort
- Permission requests for edits, shell commands and MCP tools, with allow/reject always
- Client file system (sees unsaved buffers) and client terminals when the client advertises them; local disk and `os/exec` otherwise
- Edit diffs, embedded terminals, thoughts, usage/cost, session titles, slash commands, elicitation forms, and live notices when the client shows them
- Auth methods (agent and terminal) plus logout; image, audio and embedded-context prompts
- Plans (`update_plan`), automatic or `/compact` context compaction, and session fork for parallel work
- Provider configuration (`providers/list`, `providers/set`, `providers/disable`) so a client can point the agent at a gateway or local OpenAI-compatible server
- Next edit suggestions (`nes/*`) from the editor's live buffers, with document sync and position-encoding negotiation
- Draft ACP v2: a v2 initialize is served with the draft-v2 prompt lifecycle (acceptance plus state updates) instead of being downgraded

## Configuration

Stored at `<user config dir>/micro-agent/config.json` (override with `-config` or `$MICRO_AGENT_CONFIG`). Sessions are saved next to it under `sessions/`.

```json
{
  "api_key": "sk-or-...",
  "model": "deepseek/deepseek-v4.1-flash",
  "models": ["openai/gpt-5", "google/gemini-3-pro"],
  "reasoning_effort": "medium",
  "default_mode": "default",
  "max_turns": 200,
  "max_tokens": 0,
  "auto_compact": true,
  "shell": "",
  "shell_timeout_seconds": 120,
  "shell_env": { "NODE_ENV": "development" },
  "system_prompt": "",
  "mcp_servers": {
    "fs": { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem", "."] },
    "remote": { "url": "https://example.com/mcp", "headers": { "Authorization": "Bearer ..." } },
    "legacy": { "url": "https://example.com/sse", "transport": "sse" }
  }
}
```

Slash commands: `/help`, `/model`, `/mode`, `/effort`, `/config` (opens a form, or takes `key=value`), `/login`, `/logout`, `/mcp`, `/compact`, `/clear`.

The `shell` tool runs PowerShell (`pwsh`, then `powershell`, then `cmd`) on Windows and `bash` (falling back to `sh`) elsewhere; set `shell` to override, e.g. `"shell": "C:\\Program Files\\Git\\bin\\bash.exe"`. Tool output over 2000 lines or 50KB is cut down for the model (shell keeps the end) and the full text is saved to a temp file the model can read.

If the project has an `AGENTS.md`, it is added to the system prompt.

## Releases

Pushing a `v*` tag runs the CI matrix (Linux, macOS, Windows), the license
checks, and a GoReleaser snapshot, then publishes versioned archives plus
`checksums.txt`. GoReleaser injects the tag into `micro-agent -version`.

## License

micro-agent is [MIT licensed](LICENSE). [NOTICE](NOTICE) and
[licenses/THIRD_PARTY_NOTICES.txt](licenses/THIRD_PARTY_NOTICES.txt) carry the
terms of everything a released binary embeds, including the Apache-2.0
`acp-go` dependency and the Go runtime notices. CI keeps the dependency
policy, the notices file, and the shipped archives in sync.
