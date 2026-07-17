# CMO Bridge Tool Catalog

Use this reference to distinguish callable MCP tools from planned or manual capabilities. The
registered tool schema is authoritative for exact argument types.

## Contents

- [Status labels](#status-labels)
- [Selection and result conventions](#selection-and-result-conventions)
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
- Treat every mutation result as a bounded projection of CMO state. Perform the corresponding
  `get` or `list` after a multi-step change.
- Launch, RTB, refuel, attack, cargo, and special-action results mean the command was accepted, not
  that its effect completed.
- Tool registration is determined when the agent task starts. Enabling a plugin does not add tools
  to an already-open task, but `cmo_bridge_prepare` can make the already registered ordinary tools
  ready in the same task.

## Current read and control tools

All tools in this section are `CURRENT`. Their information use still depends on operating mode.

| Tool | Primary inputs | Use and boundary |
|---|---|---|
| `cmo_bridge_diagnose` | none | Inspect saved game root and release-runtime readiness without contacting CMO |
| `cmo_bridge_status` | optional accepted lineage | Read build, runtime identity, bridge health, polling state, and scenario lineage |
| `cmo_scenario_get` | none | Read scenario name, file, database, times, duration, current player-side GUID, actual compression multiplier, and projected score state |
| `cmo_scenario_time_compression_set` | code `0..5` | Set `0=1x`, `1=2x`, `2=5x`, `3=15x`, `4=coarse one-second slices (30x readback)`, or `5=coarse five-second slices (150x readback)`; result echoes the requested code and actual multiplier |
| `cmo_side_list` | paging | Resolve sides and counts; opponent counts are not live-player intelligence |
| `cmo_side_posture_get` | observer side, target side | Read one directed side relationship; does not mutate diplomacy |
| `cmo_reference_point_list` | one side selector, paging | Resolve side-owned reference points and GUIDs |
| `cmo_unit_list` | one side selector, filters, paging | Browse one side's units; adversary use is author or umpire only |
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

In `LIVE_PLAYER`, unit, mission, inventory, doctrine, and score reads apply to the commanded side.
Read adversaries through `cmo_contact_*`. In author or umpire mode, omniscient reads are permitted
only within the requested scope.

For consequential multi-step work, preserve the multiplier from `cmo_scenario_get` and map it back
to a setter code with `1->0`, `2->1`, `5->2`, `15->3`, `30->4`, or `150->5`. Set code `0`, require
`observed_time_compression=1`, refresh decision-relevant state, execute and verify at 1x, then
restore the mapped code. Leave CMO at 1x after a timeout or uncertain outcome. Regular Time
polling continues at 1x, so this workflow requires neither simulation pause control nor a Special
Action pump.

## Current mutation tools

### Host setup

| Tool | Primary inputs | Use and important semantics |
|---|---|---|
| `cmo_bridge_prepare` | optional game root; explicit saved-root replacement flag | Deploy the release-bound Lua runtime and hot-activate ordinary tools in the same MCP session; does not mount the CMO scenario event |

### Player-valid and author-valid

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

| Tool | Primary inputs | Restriction |
|---|---|---|
| `cmo_unit_add` | side, DBID, name, and exactly one base or coordinate location form | Creates scenario objects; never use during fair player command |
| `cmo_unit_magazine_adjust` | unit, magazine, weapon, quantity operation | Direct inventory fabrication or correction |
| `cmo_unit_mount_reload_adjust` | unit, mount, weapon, quantity operation | Direct mount reload fabrication or correction |

## Current scenario-authoring tools

All tools in this section are `CURRENT / AUTHOR`. Use them only after an explicit switch to
`SCENARIO_AUTHOR` or `UMPIRE`; their availability never authorizes omniscient live-player use.

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
| Relative RPs, task pools/packages, flight plans/TOT, mission AAR | Friendly-side operational use is permitted | May also be used to construct or instrument the scenario |
| The 19 tools in [Current scenario-authoring tools](#current-scenario-authoring-tools) | Forbidden | Permitted only within the explicit author/umpire scope |
| Lua-bearing event components and special-action definitions | Forbidden; only execute an existing legitimate player-facing special action | Trusted code authoring only after line-by-line review, inactive creation, exact readback, saved-copy testing, and explicit activation |

## Remaining bridge targets

These capabilities are not current even though adjacent complex-planning and authoring operations
are now callable.

| Capability | Status | Boundary |
|---|---|---|
| Operation-planner phases, priority graph, H/L-hour, and start/completion dependencies | `EXPERIMENTAL` | Record the desired phase graph in the plan; do not fabricate phase or dependency readback |
| Automatic multi-mission assignment, `AllowMultiMission`, and assignment queues | `EXPERIMENTAL` | A unit can be assigned through the current single-mission tool, but the bridge cannot manage a deterministic dynamic queue |
| Generated-flight waypoint insert/update/delete and timing refresh | `EXPERIMENTAL` | Current tools create and inspect flight plans; do not claim route mutation |
| Exclusion, no-nav, standard, and custom-environment zone objects | `EXPERIMENTAL` | Mission areas made from reference points are current; independent zone objects are not |
| Remaining scenario metadata such as briefing, database selection, complexity/difficulty, and every environment field | `MANUAL LUA` or editor | Current authoring tools cover title, timeline, and the four global weather values only |
| Agent-driven deterministic pause/start/single-step simulation control | `UNSUPPORTED` | Retail Lua time compression cannot pause; the user can press `Alt+1` while paused for CMO's built-in 15-second time step |

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
- deterministic pause/start/single-step simulation control;
- automatic multi-mission assignment queues or generated-flight waypoint mutation;
- complete airbase runway, taxi, launch-queue, diversion, quick-turn-history, message-log,
  losses/expenditures-log, scoring-log, or refueling-history projections;
- every official doctrine, mission, operation-planner, zone, group, formation, or scenario field.

Some may become experimental targets. Until then, state the gap or use the editor/manual Lua in
author mode.

## Verification requirements

`CURRENT` means the typed tool is registered and callable. It does not remove the need to verify
CMO's effective behavior after a consequential write:

1. create or update with minimal values;
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
