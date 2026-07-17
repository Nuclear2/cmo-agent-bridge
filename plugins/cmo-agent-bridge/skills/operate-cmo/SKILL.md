---
name: operate-cmo
description: "Assess, plan, command, operate, test, or author Command: Modern Operations (CMO) scenarios through cmo-agent-bridge. Use for live battlespace assessment, courses of action, missions, contacts, units, doctrine, WRA, EMCON, sensors, weapons, logistics, attacks, time control, scenario-author or umpire work, event designs, special actions, scoring, and playtesting."
---

# Operate CMO

Treat MCP results as the authority for the scenario currently open in CMO. Prefer the MCP tools
over the CLI whenever the corresponding tool exists. Never present an official Lua capability,
planned bridge capability, or manually mounted script as an already callable MCP tool.

## Select one operating mode

Start every workflow in `LIVE_PLAYER`.

Switch to `SCENARIO_AUTHOR` or `UMPIRE` only when the user explicitly requests scenario editing,
authoring, adjudication, omniscient inspection, controlled testing, state fabrication, or direct
inventory manipulation. Convenience, complexity, missing intelligence, or a desire for a better
plan never authorizes a mode change.

| Mode | Information boundary | Permitted purpose |
|---|---|---|
| `LIVE_PLAYER` | Commanded-side information; adversaries only through that side's contacts | Fair scenario play, operational planning, deployment, engagement, sustainment, and assessment |
| `SCENARIO_AUTHOR` | Omniscient scenario state as needed | Build or revise the scenario, forces, missions, logic, scoring, and presentation |
| `UMPIRE` | Omniscient state limited to the adjudication requested | Diagnose, inject, correct, or assess a controlled test without claiming fair player play |

Do not mix modes inside one decision cycle:

- Never use enemy `unit`, mission, doctrine, loadout, inventory, or true-position data to improve a
  `LIVE_PLAYER` decision.
- If omniscient information has already influenced the plan, label the rest of the workflow
  `UMPIRE-assisted`; do not relabel it as fair player command.
- Report a mode switch and its information consequence before acting.
- Do not interpret this skill mode as a CMO UI mode. It controls agent authority and information
  use, not whether a Lua function technically runs in editor or normal play.

## Load only the references needed

- For any live order, mission, contact decision, engagement, logistics action, or battle rhythm,
  read [references/live-operations.md](references/live-operations.md).
- For a campaign, multi-domain operation, complex strike package, unclear end state, consequential
  commitment, or request to compare courses of action, also read
  [references/operational-planning.md](references/operational-planning.md).
- For scenario construction, umpire intervention, events, special-action definitions, scoring
  design, or playtesting, read
  [references/scenario-authoring.md](references/scenario-authoring.md).
- For exact tool selection, arguments, mode restrictions, current capability, experimental
  capability, and unsupported gaps, read
  [references/tool-catalog.md](references/tool-catalog.md).
- For installation, polling-event mounting, repair, or CLI smoke tests, read
  [references/setup.md](references/setup.md).

## Apply the mode gate before every mutation

Use current MCP tools in `LIVE_PLAYER` for normal player actions such as:

- creating and configuring ordinary missions, task pools, packages, and fixed or relative
  reference points;
- building moving mission areas from anchored reference points, generating mission flight plans
  against a takeoff time or TOT, and configuring mission-level air-refueling policy;
- assigning friendly units, selecting normal aircraft loadouts, launching, RTB, and refueling;
- changing friendly doctrine, WRA, EMCON, sensors, routes, speed, depth, and altitude;
- classifying contacts, engaging them, moving cargo through modeled mechanisms, executing an
  existing player special action, and controlling time compression.

Require `SCENARIO_AUTHOR` or `UMPIRE` for:

- reading adversary ground truth rather than contact-derived information;
- adding database units;
- changing magazine quantities or mount reloads directly;
- overriding ready time or ignoring host magazines while setting a loadout;
- fabricating damage, fuel, weapons, detection, posture, score, or other state for a test.
- changing scenario title, timeline, weather, sides, side options, directed side posture, events,
  event components, special-action definitions, or absolute score;
- creating or changing Lua-bearing event components or special-action scripts, and executing them
  as part of an authoring or umpire test;
- deleting a unit or mission through the preview-and-confirm authoring tools.

Treat the following as unavailable until the tool catalog marks them `CURRENT`:

- a single-call general-purpose `lua.eval`/`lua.call` tool, and deletion of sides or reference
  points;
- operation-planner phase, H/L-hour, and mission dependency editing;
- automatic multi-mission assignment queues and dynamic reassignment;
- mutation of generated flight-plan waypoints;
- deterministic pause/start/single-step simulation control;
- zone-object creation or editing outside mission areas built from reference points.

Some of these are supported by official CMO Lua and are bridge targets. `EXPERIMENTAL` means a
dedicated typed operation and live-build compatibility probe are still required. It does not grant
permission to call a nonexistent tool or to misuse an authoring script carrier as a generic escape
hatch.

## Treat authoring Lua as local code execution

The bridge has no one-call general-purpose Lua evaluator. However, a `LuaScript` event component or
Lua condition can store source supplied through `cmo_event_component_set`, and
`cmo_special_action_create`/`update` can store source that a later event or
`cmo_special_action_execute` runs. That composition is equivalent to executing code inside the
local CMO process, with the authority of CMO's scenario Lua environment.

Use this capability only in an explicitly trusted `SCENARIO_AUTHOR` or `UMPIRE` scope:

1. Save or clone the scenario before writing the script.
2. Show and review the complete source line by line; reject hidden downloads, external-process
   assumptions, unrelated filesystem access, or effects outside the requested scenario purpose.
3. Create the containing event or special action inactive and non-repeatable by default.
4. Read back the exact stored source, links, visibility, repeatability, and active state.
5. Activate and execute only after the user-authorized design and readback match, then inspect all
   resulting scenario state.

Never create, alter, enable, or test a Lua-bearing authoring artifact in `LIVE_PLAYER`. A player may
execute a pre-existing scenario-authored special action when it is a legitimate visible game
control; that does not authorize inspecting or replacing its script.

## Protect consequential decision windows

Keep scenario time advancing so the one-second Regular Time polling event can service bridge
requests. Do not pause CMO for routine agent work. Before a multi-step assessment, mission build,
force assignment, strike plan, doctrine/WRA/EMCON change, scenario-authoring batch, or other work
where high time compression could invalidate the plan:

1. Call `cmo_scenario_get` and preserve its exact `time_compression` multiplier. Convert it back to
   a setter code with `1->0`, `2->1`, `5->2`, `15->3`, `30->4`, or `150->5`.
2. Call `cmo_scenario_time_compression_set` with `code=0` and require `accepted=true` plus
   `observed_time_compression=1`.
3. Refresh the decision-relevant scenario, contact, mission, and unit state after the slowdown.
4. Perform the bounded changes and read them back while CMO remains at 1x.
5. After successful verification, restore the mapped code unless the user asks to remain at
   1x. If any request fails or the result is uncertain, remain at 1x and report the blocker.

Do not cycle compression around isolated reads or trivial bounded orders when delay cannot affect
the outcome. If the user has manually paused CMO, have them resume it, select 1x, and then continue;
the normal bridge does not need a Special Action polling path.

## Preserve these universal invariants

1. Resolve exact side, unit, mission, contact, reference-point, sensor, mount, magazine, and weapon
   identities before mutation. Use GUIDs whenever the tool accepts them.
2. Keep contact GUIDs distinct from actual unit GUIDs. Use an actual unit GUID only when the
   observing side exposes it and the tool contract requires it.
3. Build new missions inactive. Configure geometry, targets, force size, doctrine, WRA, EMCON,
   support, assignments, and dependencies; read them back; activate only after the gate is met.
4. Read actual combat status, loadout, inventory, fuel, damage, readiness, sensor state, and
   existing weapon allocations before committing a force.
5. Treat launch, RTB, refuel, attack, special-action execution, cargo movement, and other
   asynchronous results as accepted orders, not completed effects. Advance or observe time and
   read the resulting state.
6. Send only fields that should change. Preserve ordered zones and courses. An empty ordered list
   is an explicit clear when the tool contract permits it.
7. Follow every list tool's `next_cursor` until null when completeness matters.
8. Do not retry a create, attack, loadout, launch, RTB, refuel, cargo, inventory, or special-action
   mutation blindly after a timeout. Discover the resulting state first.
9. Preserve exact returned values and distinguish requested values from CMO readback. A mutation
   result is a bounded projection, not a complete wrapper.
10. Stop dependent actions when the bridge, polling event, loaded scenario, or advancing scenario
    time is unavailable. Preserve the last verified state and give a precise recovery action.

## Handle capability gaps honestly

When a request needs a non-current capability:

1. Check [references/tool-catalog.md](references/tool-catalog.md) for its status.
2. If `CURRENT`, use the named MCP tool and its verified contract.
3. If `EXPERIMENTAL`, use it only when a dedicated tool is actually registered and the running CMO
   build has passed its compatibility probe. Otherwise propose a current-tool approximation or
   report the exact blocker.
4. If official Lua supports the feature but MCP does not, generate a reviewable Lua design only in
   `SCENARIO_AUTHOR` or `UMPIRE` mode and tell the user how it must be mounted manually. Do not
   execute it through a fabricated tool.
5. If `UNSUPPORTED`, do not claim completion.

## Report the outcome

State the operating mode, commanded or edited side, meaningful assumptions, actions actually
accepted by CMO, final readback, unresolved asynchronous effects, non-current dependencies, and
any information contamination. Preserve scenario, file, side, and GUID values exactly.
