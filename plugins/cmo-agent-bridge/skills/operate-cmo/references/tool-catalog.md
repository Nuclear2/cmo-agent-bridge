# CMO Bridge Tool Catalog

Use this reference to distinguish callable MCP tools from planned or manual capabilities. The
registered tool schema is authoritative for exact argument types.

## Contents

- [Status labels](#status-labels)
- [Selection and result conventions](#selection-and-result-conventions)
- [Durable queue tools](#durable-queue-tools)
- [Current read and control tools](#current-read-and-control-tools)
- [Current mutation tools](#current-mutation-tools)
- [Current scenario-authoring tools](#current-scenario-authoring-tools)
- [Mode-restricted parameters](#mode-restricted-parameters)
- [Remaining bridge targets](#remaining-bridge-targets)
- [Manual Lua authoring capabilities](#manual-lua-authoring-capabilities)
- [Unsupported capabilities](#unsupported-capabilities)
- [Verification requirements](#verification-requirements)

## Status labels

| Label | Meaning |
|---|---|
| `CURRENT` | Registered typed MCP tool in the present plugin |
| `CURRENT / AUTHOR` | Registered tool, but use only in `SCENARIO_AUTHOR` or `UMPIRE` |
| `EXPERIMENTAL` | Intended typed bridge capability; use only if a dedicated tool is actually registered and the exact build passes its probe |
| `MANUAL LUA` | Official CMO Lua supports a missing typed operation; generate reviewed Lua and explicit mounting instructions. Do not confuse this label with the current author-only event and special-action tools that intentionally carry Lua source |
| `UNSUPPORTED` | Do not perform or claim completion |

Never infer `CURRENT` from an official Lua function. Never call a proposed tool name.

## Selection and result conventions

- Use GUID selectors for exact mutable objects whenever available.
- A unit name lookup requires exactly one side selector. A contact lookup always requires an
  observing side. A mission lookup requires one side selector and exactly one mission GUID or
  name.
- Contact GUIDs belong to the observing side and are not interchangeable with actual unit GUIDs.
- Follow `next_cursor` until null when a complete list matters. Reuse the same `page_size`.
  `cmo_unit_list` bounds expensive candidate hydration, so its page may be short or empty while a
  non-null cursor still requires another call.
- Ordinary mutation tools return `QueuedOperationReceipt` with `request_id`, operation, FIFO
  sequence, `queued` state, and submission time. They do not return CMO's eventual result.
- Use `cmo_request_get` or `cmo_request_wait` to obtain the terminal queue status and result. Treat
  every completed mutation result as a bounded projection of CMO state, then perform the
  corresponding CMO `get` or `list` after a consequential change.
- A `cmo_request_wait` timeout ends only that local wait; it does not cancel or change the durable
  request. Never resubmit solely because a wait timed out.
- Launch, RTB, refuel, attack, cargo, and special-action queue completion means CMO accepted the
  command, not that its simulated effect completed.
- Tool registration is determined when the agent task starts. Enabling a plugin does not add tools
  to an already-open task, but `cmo_bridge_prepare` can make the already registered ordinary tools
  ready in the same task.

## Durable queue tools

These tools operate on local SQLite queue state and remain usable while CMO is paused after the
session binding has been established.

| Tool | Primary inputs | Use and boundary |
|---|---|---|
| `cmo_request_get` | request UUID | Read `queued`, `active`, `completed`, `rejected`, `quarantined`, or `cancelled` state plus terminal result/error |
| `cmo_request_wait` | request UUID, non-negative timeout seconds | Wait locally for a terminal state; `timed_out=true` never cancels the request |
| `cmo_request_list` | optional positive limit | List durable requests in FIFO submission order |
| `cmo_request_cancel` | request UUID | Cancel only a request still in `queued`; an `active` or terminal request remains unchanged |
| `cmo_queue_status` | none | Return counts by queue state |

The queue executes one active mutation at a time. Submit independent work in the intended FIFO
order. If a later tool needs a GUID or other value from an earlier result, wait for the earlier
request to complete before submitting it. An active request remains pending without a mutation
execution timeout while CMO is paused. MCP/client shutdown detaches the worker rather than
cancelling it; restart and query the original request. Process, runtime, or scenario binding
mismatches produce rejection or quarantine instead of cross-scenario execution.

Reads, `cmo_bridge_status`, and destructive delete preview/confirm use synchronous contracts rather
than this queue. CMO-backed synchronous tools require the polling event and advancing scenario
time. Before each read batch, use a fresh host UI observation; verified pause produces
`SCENARIO_NOT_ADVANCING` before request publication or retry. `cmo_bridge_prepare` and host UI time
tools are host-side and remain callable while paused.

## Current read and control tools

All tools in this section are `CURRENT`. Their information use still depends on operating mode.

| Tool | Primary inputs | Use and boundary |
|---|---|---|
| `cmo_bridge_diagnose` | none | Inspect saved game root and release-runtime readiness without contacting CMO |
| `cmo_bridge_status` | optional accepted lineage | Read build, runtime identity, bridge health, polling state, and scenario lineage; success establishes the session binding required for queued mutations |
| `cmo_time_get_state` | none | Host-only read of the uniquely matched CMO window's paused/running state and selected compression; does not require Lua polling or an established session binding |
| `cmo_time_set` | `state=paused|running`; optional `rate_code=0..5` | Idempotently set UI run state and optionally compression, then verify readback; use only the configured unique CMO process and fail closed on ambiguous or unverifiable UI state |
| `cmo_simulation_pulse` | optional request UUID list; `handshake`; optional accepted lineage; timeout seconds | Only from a verified paused state: require every current non-terminal durable request UUID, force 1x, wait for that complete set and/or the bridge handshake, then attempt to re-pause and restore prior compression with explicit verification |
| `cmo_scenario_get` | none | Read scenario name, file, database, times, duration, current player-side GUID, actual compression multiplier, and projected score state |
| `cmo_scenario_context_get` | none | Read the saved scenario description, only the live current player's side briefing, plain-text projections, and that side's five victory-score thresholds; reports unsaved/missing/incompatible sources instead of exposing another side |
| `cmo_scenario_time_compression_set` | code `0..5` | Queued Lua mutation: set `0=1x`, `1=2x`, `2=5x`, `3=15x`, `4=coarse one-second slices (30x readback)`, or `5=coarse five-second slices (150x readback)`; it cannot execute while polling is frozen and is not a way to release a paused scenario |
| `cmo_side_list` | paging | Resolve sides and counts; opponent counts are not live-player intelligence |
| `cmo_side_posture_get` | observer side, target side | Read one directed side relationship; does not mutate diplomacy |
| `cmo_reference_point_list` | one side selector, paging | Resolve side-owned reference points and GUIDs |
| `cmo_unit_list` | one side selector, filters, paging | Browse one side's units; continue every non-null cursor even after a short or empty filtered page; adversary use is author or umpire only |
| `cmo_unit_get` | GUID, or side plus name | Read full projected unit detail |
| `cmo_unit_combat_status_get` | unit GUID | Read actual damage, fuel quantities, readiness or airborne time, loadout ID, and engagement state |
| `cmo_unit_loadout_get` | unit GUID | Read current aircraft loadout and carried weapon quantities |
| `cmo_unit_inventory_get` | unit GUID | Read sensors, mounts, magazines, cargo, and component or weapon state |
| `cmo_contact_list` | observing side, filters, paging | Browse contacts exactly as one side observes them |
| `cmo_contact_get` | observing side, contact GUID | Read age, uncertainty, classification, detections, emissions, BDA, and combat relationships |
| `cmo_contact_weapon_allocations_get` | observing side, contact GUID | Inspect allocations already made against one contact |
| `cmo_mission_list` | one side selector, class filter, paging | Browse one side's ordinary missions, task pools, and packages |
| `cmo_mission_get` | one side selector, mission GUID or name | Read category, parent/children, class, assignments, targets, zones, schedule, AAR, and supported options |
| `cmo_mission_flight_plan_list` | side GUID, mission GUID | Read mission takeoff/TOT values, generated flights, and their waypoint courses |
| `cmo_doctrine_get` | side, mission, or unit selector; actual flag | Read explicit or effective projected doctrine and EMCON |
| `cmo_doctrine_wra_get` | doctrine scope, weapon, target type | Read WRA salvo, shooters, firing range, and self-defence range |
| `cmo_special_action_list` | side, paging | List existing player-facing special actions |
| `cmo_score_get` | side | Read the side's current victory-point score |

For all CMO-backed synchronous rows in this table, a handshake pulse is not a read window: it
returns to pause before it returns. If fresh reads are needed from a paused scenario, inspect the
durable queue, explicitly run at 1x, execute the preselected batch, and restore verified pause and
the preserved rate in cleanup. Never retry `SCENARIO_NOT_ADVANCING` while still paused.

In `LIVE_PLAYER`, unit, mission, inventory, doctrine, and score reads apply to the commanded side.
Read adversaries through `cmo_contact_*`. In author or umpire mode, omniscient reads are permitted
only within the requested scope.

The UI time tools are specialized semantic Windows UI Automation, not Lua mutations or
keyboard/mouse/coordinate macros. They match the configured `Command.exe`, require the MCP server
and one unambiguous CMO window in the same interactive Windows session, fail closed when a modal
disables the main window or state cannot be verified, and verify the resulting state. CMO does not
need to start in the foreground. A WPF button invoke may surface it briefly; restoring the previous
foreground window is best-effort and must not be described as guaranteed invisible background
operation. Never use a pulse while CMO is already running: a running handshake or routine order
should proceed at the current compression unless its decision horizon justifies a temporary
slowdown or deliberate pause.

`cmo_simulation_pulse` accepts only a verified paused start. Before release, call
`cmo_request_list` and include every current non-terminal `queued` or `active` UUID in `request_ids`.
The default empty list is valid only when no non-terminal durable work exists. An incomplete set is
rejected before time advances because the FIFO worker would also service omitted work.
`handshake=true` performs the initial bridge handshake, with `accept_lineage_id` only when the caller
deliberately accepts that lineage.

The pulse forces 1x. `timeout_seconds` bounds the work wait after the 1x release is verified; UI
verification plus final pause/rate-restoration cleanup can extend total tool duration, and no value
guarantees an exact amount of simulated time. The UI action itself is host-side, but handshake and
queue completion still require the mounted Regular Time polling event. On timeout or work failure,
the tool attempts to pause and restore the prior rate, reports whether both were verified, and never
cancels or resubmits a durable request. Treat inability to verify the final pause as a high-severity
condition requiring immediate user attention. The registered schema and returned state model remain
authoritative.

## Current mutation tools

### Host setup

| Tool | Primary inputs | Use and important semantics |
|---|---|---|
| `cmo_bridge_prepare` | optional game root; explicit saved-root replacement flag | Deploy the release-bound Lua runtime and hot-activate ordinary tools in the same MCP session; refuses queued/active work or a pending journal and does not mount the CMO scenario event |

### Player-valid and author-valid

Every tool in this subsection is a durable queued mutation and returns `QueuedOperationReceipt`.

| Tool | Primary inputs | Use and important semantics |
|---|---|---|
| `cmo_reference_point_add` | side GUID, name, either coordinates or complete relative-anchor fields | Create a fixed point or a fixed/rotating point relative to a unit, contact, or RP; non-idempotent |
| `cmo_reference_point_update` | side GUID, RP GUID, changed absolute or relative fields | Move, re-anchor, adjust, or clear a relative point; use anchored point sets for moving mission areas |
| `cmo_unit_set` | unit GUID plus changed movement or identity fields | Set name, speed or throttle, altitude or depth, heading, hold, cavitation, sprint-and-drift, or ordered course |
| `cmo_unit_assign_mission` | unit GUID, mission GUID, escort flag | Assign one unit; use escort only when meaningful |
| `cmo_unit_unassign_mission` | unit GUID | Remove the current ordinary mission assignment |
| `cmo_unit_loadout_set` | unit GUID, loadout DBID; optional readying controls | Select an aircraft loadout; parameter-level author restrictions apply below |
| `cmo_unit_launch` | unit GUID | Submit launch; poll combat status for actual progress |
| `cmo_unit_rtb` | unit GUID | Submit RTB; poll combat status |
| `cmo_unit_refuel` | receiver GUID; optional tanker GUID or tanker mission GUID list | Request refueling using one unambiguous tanker selector |
| `cmo_unit_attack_contact` | observing side, attacker GUID, contact GUID, mode; optional weapon allocation | Submit auto target, manual target, or explicit weapon allocation |
| `cmo_unit_sensor_set` | unit GUID, sensor GUID, active state | Change an existing sensor; read inventory and EMCON first |
| `cmo_unit_cargo_transfer` | source, destination, cargo selector and quantity | Move modeled cargo between eligible units; non-idempotent |
| `cmo_unit_cargo_unload` | carrier and cargo selector | Unload modeled cargo at the current location; non-idempotent |
| `cmo_mission_create` | side GUID, unique name, category, discriminated mission details; parent pool for package | Create an inactive ordinary mission, task pool, or package for patrol, support, strike, ferry, mining, mine-clearing, or cargo work |
| `cmo_mission_update` | side GUID, mission GUID, changed supported fields | Update activation, schedule, grouping, profiles, zones, patrol, strike, mining, or cargo options |
| `cmo_mission_air_refueling_update` | side GUID, mission GUID, changed AAR fields | Configure receiver policy, permitted tanker missions, launch/follow/continue behavior, tanker minimums, receiver queue, fuel threshold, range, and support-tanker limits |
| `cmo_mission_flight_plan_create` | side GUID, mission GUID, exactly one complete target-time or takeoff-time schedule | Generate mission flights for `YYYY/MM/DD` plus `HH:MM:SS`, then return all flights visible through mission readback |
| `cmo_mission_target_add` | side GUID, mission GUID, target GUID | Add a strike target; check current targets first |
| `cmo_mission_target_remove` | side GUID, mission GUID, target GUID | Remove a strike target |
| `cmo_mission_cargo_update` | side GUID, mission GUID, add/remove, cargo identity and quantity | Maintain cargo assigned to a cargo mission |
| `cmo_doctrine_set` | scope selector and projected doctrine fields | Change side, mission, or unit doctrine |
| `cmo_emcon_set` | scope selector and radar/sonar/OECM fields | Change EMCON; returned inheritance may be unavailable on some builds |
| `cmo_doctrine_wra_set` | scope, weapon, target type, changed WRA fields | Change deliberate weapon/target release rules |
| `cmo_contact_posture_set` | observer side, contact GUID, posture | Change how the observing side classifies a contact |
| `cmo_special_action_execute` | side GUID, action GUID | Execute one existing active special action; a normal player may use a pre-authored visible control, while executing newly authored or changed Lua is an author/umpire code-execution test |

### Existing author or umpire controls

Every tool in this subsection is also a durable queued mutation.

| Tool | Primary inputs | Restriction |
|---|---|---|
| `cmo_unit_add` | side, DBID, name, and exactly one base or coordinate location form | Creates scenario objects; never use during fair player command |
| `cmo_unit_magazine_adjust` | unit, magazine, weapon, quantity operation | Direct inventory fabrication or correction |
| `cmo_unit_mount_reload_adjust` | unit, mount, weapon, quantity operation | Direct mount reload fabrication or correction |

## Current scenario-authoring tools

All tools in this section are `CURRENT / AUTHOR`. Use them only after an explicit switch to
`SCENARIO_AUTHOR` or `UMPIRE`; their availability never authorizes omniscient live-player use.

`cmo_scenario_weather_get`, `cmo_event_list`, and `cmo_event_get` are synchronous reads. The
ordinary setters, creates, updates, links, score mutation, and Special Action definitions are
durable queued mutations. Unit/mission delete preview and confirm retain their synchronous,
short-lived confirmation-token workflow.

| Tool | Primary inputs | Use and boundary |
|---|---|---|
| `cmo_scenario_weather_get` | none | Read global temperature, rainfall, undercloud fraction, and sea state for authoring or adjudication |
| `cmo_scenario_weather_set` | all four weather values | Replace the global weather tuple and return readback |
| `cmo_scenario_title_set` | non-empty title | Rename the loaded scenario |
| `cmo_scenario_timeline_set` | changed current time, start time, or duration | Set scenario-local timeline values; timestamps use `YYYY-MM-DDTHH:MM:SS` and duration uses `H:MM:SS` |
| `cmo_side_add` | unique side name | Create one side; discover first because repeated calls may create duplicates |
| `cmo_side_options_set` | side GUID and changed options | Set awareness, proficiency, civilian tracking, collective responsibility, or AI-only control |
| `cmo_side_posture_set` | side A GUID, side B GUID, `F/H/N/U` | Set one directed relationship; verify the reverse separately |
| `cmo_score_set` | side name/GUID, absolute score, reason | Set an absolute score for initialization, migration, reset, or adjudication |
| `cmo_event_list` | detail level `0..4` | List events or their trigger, condition, action, or property projection |
| `cmo_event_get` | event GUID/exact description, detail level | Read one event and its selected projection |
| `cmo_event_set` | add/update/remove, event selector, changed properties | Create inactive by default, update, rename, activate, or remove an event |
| `cmo_event_component_set` | kind, list/add/update/remove, selector, subtype, parameters | Manage trigger, condition, or action definitions through an allowlisted envelope; `LuaScript` action and Lua-condition parameters can contain executable source and are trusted-author code execution |
| `cmo_event_component_link` | kind, add/remove/replace, event and component selectors | Assemble or revise an event's trigger, condition, and action links |
| `cmo_special_action_create` | side GUID, name, Lua text, visible properties | Create a side-owned special action inactive by default; its source can execute with CMO scenario-Lua authority |
| `cmo_special_action_update` | side GUID, action selector, update/remove, changed properties | Rename, edit, enable, disable, change repeatability/script, or remove a special action; changing source is trusted-author code authoring |
| `cmo_unit_delete_preview` | unit GUID | Resolve the exact deletion and issue a short-lived confirmation token |
| `cmo_unit_delete_confirm` | same unit GUID and token | Permanently delete only the unit bound to the preview token |
| `cmo_mission_delete_preview` | side GUID, mission GUID | Resolve the exact deletion and issue a short-lived confirmation token |
| `cmo_mission_delete_confirm` | same side/mission GUIDs and token | Permanently delete only the mission bound to the preview token |

The event-component tools intentionally expose official subtype parameters without pretending that
every possible CMO subtype has a separate MCP tool. The subtype envelope is allowlisted, but a Lua
source field is not a safe typed substitute for the code it contains. A Lua-bearing event or
special action, combined with activation or execution, is equivalent to local code execution in
the CMO process. Use it only in `SCENARIO_AUTHOR` or `UMPIRE`: save a scenario copy, review every
source line, create inactive and non-repeatable by default, read back exact source and links, and
execute only after the trusted author scope approves it.

## Mode-restricted parameters

Some current tools are valid in both modes but contain author-only shortcuts:

| Tool/field | `LIVE_PLAYER` rule | `SCENARIO_AUTHOR` or `UMPIRE` |
|---|---|---|
| `cmo_unit_loadout_set.time_to_ready_minutes` | Omit; use database/default readying | May set deliberately and record the intervention |
| `cmo_unit_loadout_set.ignore_magazines` | Keep false | May use for scenario initialization or a controlled test |
| `cmo_unit_loadout_set.exclude_optional_weapons` | Use only as a legitimate loadout choice | May use for exact test composition |
| Enemy `cmo_unit_*`, `cmo_mission_*`, `cmo_doctrine_*`, inventories | Forbidden | Permitted within explicit omniscient scope |
| `cmo_contact_posture_set` | Identification/ROE decision only | May be used to prepare or correct observer-side perception |
| `cmo_scenario_time_compression_set` | Normal simulation control | May be used for test acceleration but never as deterministic single-step |
| `cmo_time_get_state`, `cmo_time_set`, `cmo_simulation_pulse` | Normal host UI time control under the decision-window policy | May also be used for authoring/test control; a pulse remains a bounded 1x run, not a zero-time or deterministic single step |
| Relative RPs, task pools/packages, flight plans/TOT, mission AAR | Friendly-side operational use is permitted | May also be used to construct or instrument the scenario |
| The 19 tools in [Current scenario-authoring tools](#current-scenario-authoring-tools) | Forbidden | Permitted only within the explicit author/umpire scope |
| Lua-bearing event components and special-action definitions | Forbidden; only execute an existing legitimate player-facing special action | Trusted code authoring only after line-by-line review, inactive creation, exact readback, saved-copy testing, and explicit activation |

## Remaining bridge targets

These capabilities are not current even though adjacent complex-planning and authoring operations
are now callable.

| Capability | Status | Boundary |
|---|---|---|
| Operation-planner phases, priority graph, H/L-hour, and start/completion dependencies | `EXPERIMENTAL` | Record the desired phase graph in the plan; do not fabricate phase or dependency readback |
| Automatic multi-mission assignment, `AllowMultiMission`, and assignment queues | `EXPERIMENTAL` | The durable command FIFO does not implement CMO unit-to-mission assignment queues; a unit can use only the current single-mission assignment tool |
| Generated-flight waypoint insert/update/delete and timing refresh | `EXPERIMENTAL` | Current tools create and inspect flight plans; do not claim route mutation |
| Exclusion, no-nav, standard, and custom-environment zone objects | `EXPERIMENTAL` | Mission areas made from reference points are current; independent zone objects are not |
| Remaining writable scenario metadata such as briefing text, database selection, complexity/difficulty, and every environment field | `MANUAL LUA` or editor | Saved description/current-side briefing are readable through `cmo_scenario_context_get`; writing them and the other fields still requires the editor or a separately reviewed author workflow |
| Deterministic zero-time or fixed-simulation-duration single-step control | `UNSUPPORTED` | UI pause/run is current, but the Regular Time trigger requires scenario time to advance and a pulse may cross more than one CMO tick |

Current complex-planning tools are deliberately narrower than complete GUI parity:

- Task pools and packages can be created and their parent/child identifiers read, but automatic
  multi-mission queues are absent.
- Flight plans can be generated from either target date/time or takeoff date/time and listed with
  all flights visible through mission readback, but waypoint mutation is absent.
- Mission-level AAR settings are writable and readable, but they do not replace observation of
  actual tanker launch, rendezvous, transfer, bingo, loss, diversion, and recovery.
- Relative points can anchor moving mission geometry, but CMO owns movement and anchor-failure
  behavior; verify it during execution rather than treating configuration readback as movement.

## Manual Lua authoring capabilities

Until typed tools exist, use these only in `SCENARIO_AUTHOR` or `UMPIRE` by generating reviewed
Lua for the user to mount manually:

- configure operation-planner phase, H/L-hour, priority, and start/completion dependency fields;
- configure automatic multi-mission unit assignment or queue semantics;
- insert, update, or delete generated-flight waypoints and refresh their timing;
- create or edit exclusion, no-nav, standard, or custom-environment zone objects;
- update scenario settings not covered by title, timeline, or global weather;
- configure group, formation, hosting, proficiency, damage, fuel, or other initial-state fields
  not exposed by dedicated tools.

The script must contain preflight, unique naming, mutation, readback, and error reporting. State
which Lua Console run or event trigger/action mounting step the user must perform. The bridge has
no single-call general-purpose evaluator. Its author-only event and special-action tools can carry
and later execute Lua, but they may be used only for a user-requested, line-by-line-reviewed
scenario artifact or umpire test, never as a `LIVE_PLAYER` escape hatch.

Do not use manual Lua for work already covered by a current authoring tool. In particular, use the
current side, posture, event/T/C/A, special-action, score, unit-delete, and mission-delete tools so
the mutation remains bounded and auditable.

## Unsupported capabilities

The current MCP surface does not provide:

- a dedicated single-call general-purpose `lua.eval` or unrestricted `lua.call` tool; author-only
  LuaScript event components and special actions can still store and execute arbitrary CMO Lua
  when deliberately composed;
- reference-point or side deletion through dedicated tools;
- deterministic zero-time or fixed-simulation-duration single-step control;
- automatic multi-mission assignment queues or generated-flight waypoint mutation;
- complete airbase runway, taxi, launch-queue, diversion, quick-turn-history, message-log,
  losses/expenditures-log, scoring-log, or refueling-history projections;
- every official doctrine, mission, operation-planner, zone, group, formation, or scenario field.

Some may become experimental targets. Until then, state the gap or use the editor/manual Lua in
author mode.

## Verification requirements

`CURRENT` means the typed tool is registered and callable. It does not remove the need to verify
CMO's effective behavior after a consequential write:

1. create or update with minimal values, then resolve the queue receipt;
2. read back every projected field and normalize accepted aliases;
3. exercise invalid inputs and verify bounded failure;
4. test normal play and editor contexts where relevant;
5. advance scenario time and verify behavior, not just storage;
6. save, reload, and reread;
7. test disappearance of dependencies such as an anchor, tanker, target, parent pool, or event
   component;
8. test multiple objects, paging, duplicate names, and GUID selection;
9. verify result size, wrapper/userdata projection, and non-idempotent retry behavior;
10. persist the compatibility result against the exact build and bridge release.

For flight plans, verify schedule format, multiple-flight enumeration, launch timing, and
save/reload; waypoint refresh remains outside the current surface. For AAR, verify every requested
setting plus actual queueing, bingo behavior, missing tankers, tanker loss, and receiver
continuation. For moving areas, verify fixed versus rotating bearing, group-lead changes, anchor
destruction, mission geometry, and save/reload. For event authoring, assemble inactive, read every
component and link, activate only after verification, and test both the firing and non-firing path.
