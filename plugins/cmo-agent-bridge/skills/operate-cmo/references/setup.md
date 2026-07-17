# Setup and recovery

Use this reference only for initial setup, repair, or direct CLI smoke tests.

`v0.1.1` is a Preview GitHub pre-release. Start with a saved scenario copy and do not assume
compatibility with an unverified CMO build.

## Prerequisites

- Windows with Command: Modern Operations installed.
- `uv` 0.11.26 or newer on `PATH`.
- A scenario that can save an enabled, repeatable event.
- Either a persistent `cmo-bridge` tool installation or the pinned `uvx` commands below.

The MCP client must install and enable the `cmo-agent-bridge` plugin, or register both the stdio
server and the complete `operate-cmo` Skill manually. MCP does not distribute Skills. Start a new
Agent task after installation so the 68 MCP tools are registered.

The Codex and Claude plugins include the MCP configuration and complete Skill, but not the bridge
wheel. Their MCP configurations start the pinned GitHub Release wheel through `uvx`; other clients
must register the server and install the complete Skill separately.

## Install the CLI persistently

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.1/cmo_agent_bridge-0.1.1-py3-none-any.whl"
uv tool install --python 3.12 $wheel
uv tool update-shell
```

Restart PowerShell after updating `PATH`, then verify `Get-Command cmo-bridge`.

## Deploy the CMO-side runtime

With the persistent installation:

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

Without a persistent tool installation:

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.1/cmo_agent_bridge-0.1.1-py3-none-any.whl"
uvx --python 3.12 --from $wheel cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

This deploys the Lua dispatcher under `Lua\CMOAgentBridge` and saves the game root under
`%LOCALAPPDATA%\CMOAgentBridge`. Run `prepare` again after every bridge upgrade so the host and Lua
runtime stay release-bound.

## Mount the polling event

In the scenario editor, create an enabled, repeatable event with:

1. A **Regular Time** trigger; start with a one-second interval.
2. A **Lua Script** action containing:

```lua
return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')
```

Save the scenario after linking the trigger and action. Normal player mode can use a bridge event
already saved by the scenario author, but cannot create the missing event on demand.

Regular Time triggers run only while scenario time advances. Keep CMO at time-compression code `0`
(1x) during complex agent work. If CMO is manually paused, resume it at 1x before issuing bridge
requests; the next simulated second will service a pending request without a Special Action.

## Direct smoke test

Keep CMO and the polling event running, then use the persistent CLI from any directory:

```powershell
cmo-bridge invoke unit.list `
  --args '{"side_name":"PLAAF","page_size":3}'
```

If the CLI is not installed persistently, use the exact pinned runner:

```powershell
$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.1.1/cmo_agent_bridge-0.1.1-py3-none-any.whl"
uvx --python 3.12 --from $wheel cmo-bridge invoke unit.list `
  --args '{"side_name":"PLAAF","page_size":3}'
```

Replace `PLAAF` with a side in the loaded scenario. A successful outcome has `ok: true`. If it
times out, check that CMO is running, the correct scenario is loaded, the event is enabled and
repeatable, scenario time is advancing, and `prepare` was rerun for this bridge version.

For a no-state-change write smoke test, first read an exact unit GUID and its current name, then set
that same name again:

```powershell
cmo-bridge invoke unit.set `
  --args '{"unit_guid":"UNIT-GUID","name":"CURRENT-UNIT-NAME"}'
```

Use the unit's exact current name; do not substitute a new value for this smoke test. If a write
times out or becomes uncertain, discover the resulting CMO state before retrying.
