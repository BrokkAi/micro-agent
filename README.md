# micro-agent

A minimal [Agent Client Protocol](https://agentclientprotocol.com) coding agent from Brokk, backed by [OpenRouter](https://openrouter.ai). Its only dependency is [`acp-go`](https://github.com/BrokkAi/acp-go), which is itself standard-library only.

**Tools:** `shell`, `read_file`, `edit_file`, `write_file`, plus every tool from connected MCP servers (stdio and streamable HTTP).

```sh
go install github.com/BrokkAi/micro-agent@latest
micro-agent login        # or set OPENROUTER_API_KEY, or use /login in your editor
```

Point your ACP client (Zed, etc.) at the `micro-agent` binary.

## ACP support

- Sessions: new, load (with history replay), resume, list, close, delete, additional directories
- Modes: `default`, `accept_edits`, `plan` (read-only), `bypass`. Also exposed as config options along with model and reasoning effort
- Permission requests for edits, shell commands and MCP tools, with allow/reject always
- Client file system (sees unsaved buffers) and client terminals when the client advertises them; local disk and `os/exec` otherwise
- Edit diffs, embedded terminals, thoughts, usage/cost, session titles, slash commands, elicitation forms
- Auth methods (agent and terminal) plus logout; image and embedded-context prompts

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
  "shell": "",
  "shell_timeout_seconds": 120,
  "system_prompt": "",
  "mcp_servers": {
    "fs": { "command": "npx", "args": ["-y", "@modelcontextprotocol/server-filesystem", "."] },
    "remote": { "url": "https://example.com/mcp", "headers": { "Authorization": "Bearer ..." } }
  }
}
```

Slash commands: `/help`, `/model`, `/mode`, `/effort`, `/config` (opens a form, or takes `key=value`), `/login`, `/logout`, `/mcp`, `/clear`.

The `shell` tool runs PowerShell (`pwsh`, then `powershell`, then `cmd`) on Windows and `bash` (falling back to `sh`) elsewhere; set `shell` to override, e.g. `"shell": "C:\\Program Files\\Git\\bin\\bash.exe"`. Tool output over 2000 lines or 50KB is cut down for the model (shell keeps the end) and the full text is saved to a temp file the model can read.

If the project has an `AGENTS.md`, it is added to the system prompt.
