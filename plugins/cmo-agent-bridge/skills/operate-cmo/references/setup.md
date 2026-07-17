# Setup and recovery

Use this reference when the plugin or Skill is installed but the MCP tools are absent,
`cmo_bridge_diagnose` reports incomplete setup, `cmo_bridge_status` times out, or the polling event
must be mounted or repaired.

`v0.1.3` is a Preview GitHub pre-release. Start with a saved scenario copy and do not assume
compatibility with an unverified CMO build.

## Identify the failed layer

| Observation | Failed layer | Next action |
|---|---|---|
| No `cmo_*` tools are registered | Agent client could not initialize the stdio MCP server | Run the host bootstrap below, then restart the client and open a new task |
| `cmo_bridge_diagnose` reports `unconfigured` or `not_prepared` | MCP is running, but its game root or release-bound Lua runtime is not ready | Call `cmo_bridge_prepare` in the current task |
| Diagnose reports `ready`, but `cmo_bridge_status` times out | CMO is not polling the file bridge | Recover the CMO event and advancing scenario time |
| `cmo_bridge_status` returns `ok: true` | Host and CMO are connected | Continue with the requested CMO workflow |

The plugin includes the MCP configuration and complete Skill. It does not install `uv` or edit a
scenario to add the polling event. Its stdio command downloads the pinned bridge wheel through
`uvx`; once initialized, `cmo_bridge_prepare` can deploy the release-bound runtime itself.

Do not launch `cmo-bridge serve` manually. It is a long-running stdio child owned by the Agent
client; a terminal process cannot hot-register tools in the current task.

## Prerequisites

- Windows with Command: Modern Operations installed.
- `uv` 0.11.26 or newer, with both `uv` and `uvx` on the Agent client's `PATH`.
- The `cmo-agent-bridge` plugin enabled, or both its stdio MCP server and complete `operate-cmo`
  Skill registered manually.
- A saved scenario copy in which the user can create an enabled, repeatable event.

MCP does not distribute Skills by itself. Codex and Claude plugins package both components; other
clients normally need the MCP entry and Skill installed separately.

## Recover while the MCP tools are present

Call `cmo_bridge_diagnose` before any live CMO operation. This check is host-only: it does not wait
for CMO or require the polling event.

- `ready`: continue to `cmo_bridge_status`.
- `unconfigured`: resolve the intended CMO installation and call `cmo_bridge_prepare` with its
  `game_root`.
- `not_prepared`: call `cmo_bridge_prepare`; omit `game_root` when the diagnostic already reports
  the intended saved root.
- `error`: repair the reported local configuration error, then diagnose again.

For a normal Steam installation, the Agent tool call uses these arguments:

```json
{
  "game_root": "D:\\Program Files (x86)\\Steam\\steamapps\\common\\Command - Modern Operations",
  "replace_saved_game_root": false
}
```

Do not silently replace a different saved root. Ask the user to confirm the intended installation,
then set `replace_saved_game_root` to `true`. A successful prepare hot-activates the ordinary tools
in the same MCP session; no restart or manual `serve` process is needed.

## Recover when the MCP tools are absent

When a local shell is available, perform these host-side steps yourself. Ask the user only before
installing software, when the CMO root is ambiguous, when a saved root would be replaced, or when
permissions block the command.

### 1. Check and warm the pinned runner

```powershell
Get-Command uv, uvx
uv --version

$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.3/cmo_agent_bridge-0.1.3-py3-none-any.whl"
uvx --python 3.12 --from $wheel cmo-bridge version
```

Require `uv 0.11.26` or newer. If it is missing or old, request permission before installing or
upgrading it. The first `uvx` call downloads an isolated Python 3.12 environment and can take tens
of seconds; the version probe also warms that cache before the Agent client starts the server.

### 2. Let the Agent client start MCP

Fully exit and restart the Agent client, then open a new Agent task. Verify that
`cmo_bridge_diagnose` exists. If it does, continue with the in-session recovery above.

If the tool is still absent, confirm that the plugin and MCP server are enabled and inspect the
client's MCP startup error. In Codex, the read-only checks are:

```powershell
codex plugin list
codex mcp get cmo-agent-bridge --json
```

If the tools are still absent, inspect the Agent client's MCP startup log. Do not start `serve`
manually; stdio tools must be registered by the client during task initialization.

## Mount the polling event in CMO

The user normally performs this one-time scenario-authoring step. Guide them through the exact
objects and verify each link:

1. Open a saved copy of the scenario in the **Scenario Editor**, then open the event editor.
2. Create a **Regular Time** trigger with a one-second interval.
3. Create a **Lua Script** action containing exactly:

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

4. Create or edit an event, link that trigger and action to it, and set the event **Active** and
   **Repeatable**.
5. Save the scenario. Load that saved scenario when returning to normal game mode.

Normal player mode can use an event saved by the scenario author, but it cannot create a missing
event on demand through an MCP server that has no CMO connection. No Special Action is required.

Regular Time triggers run only while scenario time advances. Use 1x while diagnosing or performing
complex Agent work. If CMO is paused, resume at 1x; the next simulated second should service the
pending request.

## Verify both layers

After diagnose reports ready, call `cmo_bridge_status`. A successful result reports the CMO build,
runtime tag, and scenario lineage.

For a direct CLI smoke test, keep CMO and the event running:

```powershell
uvx --python 3.12 --from $wheel cmo-bridge invoke bridge.status --args '{}'
```

If this times out, check that CMO is running, the intended saved scenario is loaded, the event is
Active and Repeatable, the trigger and action are linked, and scenario time is advancing. Do not
blindly retry a mutation while the outcome is uncertain.

## Optional persistent CLI

The plugin does not require a global Python installation or persistent CLI. Users who want a
shorter diagnostic command may install the same release into uv's isolated tool environment:

```powershell
uv tool install --python 3.12 $wheel
uv tool update-shell
```

After restarting PowerShell, `cmo-bridge version`, `cmo-bridge prepare`, and `cmo-bridge invoke`
are available directly. This does not replace plugin installation or the polling event.

Use CLI `prepare` only as a fallback when the MCP tools are absent:

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

If the chosen root differs from the saved root, add `--replace-saved-game-root` only after user
confirmation.
