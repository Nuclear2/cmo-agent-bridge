# Setup and recovery

Use this reference when the plugin or Skill is installed but the MCP tools are absent,
`cmo_bridge_diagnose` reports incomplete setup, `cmo_bridge_status` times out, or the polling event
must be mounted or repaired.

`v0.4.0` is a Preview GitHub pre-release. Start with a saved scenario copy and do not assume
compatibility with an unverified CMO build.

## Identify the failed layer

| Observation | Failed layer | Next action |
|---|---|---|
| No `cmo_*` tools are registered | Agent client could not initialize the stdio MCP server | Run the host bootstrap below, then restart the client and open a new task |
| `cmo_bridge_diagnose` reports `unconfigured` or `not_prepared` | MCP is running, but its game root or release-bound Lua runtime is not ready | Call `cmo_bridge_prepare` in the current task |
| Diagnose reports `ready`, but no binding exists | CMO may be paused or not polling the file bridge | Inspect `cmo_time_get_state`; handshake directly while running or use a paused 1x handshake pulse |
| A running handshake or paused handshake pulse times out | The polling event is inactive, unloaded, or broken | Repair the event; do not keep toggling time or resubmit mutations |
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

## Gate upgrades and prepare on an idle bridge

Before upgrading the wheel/plugin, stopping the old MCP server for an upgrade, or calling
`cmo_bridge_prepare` / `cmo-bridge prepare`, stop new submissions and require the target root to
have no unfinished work:

1. Call `cmo_queue_status` or CLI `queue-status`. Require both `queued=0` and `active=0`; terminal
   history counts do not block an upgrade.
2. Wait on the original request IDs until active work is terminal. Cancel only requests that remain
   queued and are no longer wanted.
3. Allow the current worker to finish and remove its pending journal. Never delete the journal
   manually; it is durable recovery evidence.

Prepare rechecks the nonterminal queue and pending journal under the bridge lock before changing the
Lua runtime. If it returns `STATE_CONFLICT`, use its `pending_journal` and
`nonterminal_queue_requests` details. No runtime files were changed: restart the current/old release,
let its worker recover, resolve any active/quarantined work, and retry only after both gates are
clear. Apply the same rule when moving from 0.1.x to 0.2.0; never change releases during an in-flight
mutation or quarantine resolution.

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

After prepare, call `cmo_time_get_state`. If CMO is running, call `cmo_bridge_status` without
changing compression. If it is paused, call `cmo_simulation_pulse(handshake=true)`; the pulse forces
1x only long enough to complete the handshake and restores the pause. Either successful path creates
the process/runtime/scenario session binding that durable mutation submission requires. Once that
binding exists, CMO may be paused while independent mutations are enqueued. Do not accept a stale
binding after the process or loaded scenario changes; establish the new binding explicitly.

## Recover when the MCP tools are absent

When a local shell is available, perform these host-side steps yourself. Ask the user only before
installing software, when the CMO root is ambiguous, when a saved root would be replaced, or when
permissions block the command.

### 1. Check and warm the pinned runner

```powershell
Get-Command uv, uvx
uv --version

$wheel = "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/v0.4.0/cmo_agent_bridge-0.4.0-py3-none-any.whl"
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

Regular Time triggers run only while scenario time advances. For the first status handshake, leave
a running scenario at its current compression. If CMO is paused, use
`cmo_simulation_pulse(handshake=true)` instead of asking the user to release time; it briefly runs at
1x and returns to a verified pause. After a valid binding exists, ordinary mutation tools write to
a local durable FIFO queue and return immediately even while CMO is paused. They remain pending
until polling resumes or a controlled pulse services them. Before any pulse, use `cmo_request_list`
and include every current non-terminal `queued` or `active` UUID in `request_ids`; an incomplete set
is rejected before time is released. Use `cmo_request_get`,
`cmo_request_list`, `cmo_queue_status`, or `cmo_request_cancel` for still-queued work without
releasing time. A `cmo_request_wait` timeout does not cancel anything; only a request still in
`queued` can be cancelled.

Status and reads remain synchronous. Use the shortest justified 1x run window when a paused read is
necessary, then re-pause before extended analysis. If time is already advancing and a handshake or
read still times out, repair the polling event rather than changing speed repeatedly. Ask the user
to operate time manually only if `cmo_time_get_state` cannot identify or verify the CMO UI. MCP/client
shutdown does not cancel an active mutation, and restart recovery uses the original request ID. A
process/runtime/scenario mismatch rejects or quarantines the old request rather than executing it in
the new target.

The host UI state read and pause/run actions do not need Lua polling, but a pulse still needs the
mounted Regular Time event to complete a handshake or execute queued Lua work. Run the MCP server
and CMO in the same interactive Windows session with one unambiguous CMO main window. CMO need not
start in the foreground: the helper uses semantic UI Automation rather than global keyboard, mouse,
or coordinate input. CMO/WPF may nevertheless surface the window briefly during a button invoke;
restoring the prior foreground window is best-effort. An enabled modal that disables the main
window, an inaccessible UI, or ambiguous CMO instances makes the tool fail closed. Ask the user to
clear that condition rather than guessing or toggling blindly.

## Verify both layers

After diagnose reports ready, use the running `cmo_bridge_status` or paused
`cmo_simulation_pulse(handshake=true)` path above. A successful result reports the CMO build,
runtime tag, and scenario lineage and establishes the mutation queue's session binding.

Use registered MCP tools for normal Agent operation. Direct CLI `invoke` is a smoke-test or recovery
fallback, not a way to bypass the MCP pause gate. It performs the same synchronous Lua polling: if
the host UI preflight verifies that CMO is paused, it returns `SCENARIO_NOT_ADVANCING` without
publishing the operation or waiting for or retrying Lua polling. Do not loop that command while
paused. Use the MCP time tools when available to open a controlled 1x window; if they are absent,
ask the user to release time at 1x for the bounded fallback call and re-pause afterward.

For a direct CLI smoke test, first verify that CMO is running, scenario time is advancing, and the
event is Active and Repeatable:

```powershell
uvx --python 3.12 --from $wheel cmo-bridge invoke bridge.status --args '{}'
```

If this times out while the UI is verified running, inspect the structured `phase`, `likely_causes`,
and `next_steps`. Check that CMO is running, the intended saved scenario is loaded, the event is
Active and Repeatable, the trigger and action are linked, and scenario time is advancing. A verified
pause should produce `SCENARIO_NOT_ADVANCING`, not a polling timeout. For a submitted mutation,
query its durable request ID rather than resubmitting it.

## Optional persistent CLI

The plugin does not require a global Python installation or persistent CLI. Users who want a
shorter diagnostic command may install the same release into uv's isolated tool environment:

```powershell
uv tool install --python 3.12 $wheel
uv tool update-shell
```

After restarting PowerShell, `cmo-bridge version`, `cmo-bridge prepare`, `cmo-bridge invoke`,
`cmo-bridge submit`, `cmo-bridge request-get`, `cmo-bridge request-wait`,
`cmo-bridge request-cancel`, and `cmo-bridge queue-status` are available directly. This does not
replace plugin installation, the polling event, MCP-first Agent operation, or the pause rules above.

CLI `submit` only persists the mutation and returns its queue receipt. It does not start a
background worker. CLI `request-wait` starts a worker in the foreground for that command, stops it
when the request completes or the local wait times out, and never cancels the durable request. The
worker serves the whole FIFO, so waiting on a later request executes earlier queued work first. If
no MCP server is running, call `request-wait` again to continue after a timeout. `request-get`,
`request-cancel`, and `queue-status` are local inspection/control commands and do not start a worker.

Use CLI `prepare` only as a fallback when the MCP tools are absent:

```powershell
cmo-bridge prepare `
  --game-root "D:\Program Files (x86)\Steam\steamapps\common\Command - Modern Operations"
```

If the chosen root differs from the saved root, add `--replace-saved-game-root` only after user
confirmation.
