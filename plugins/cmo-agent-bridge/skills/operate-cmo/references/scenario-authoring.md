# Author and Umpire CMO Scenarios

Use this reference only after an explicit switch to `SCENARIO_AUTHOR` or `UMPIRE`. It covers
scenario construction, controlled state injection, event design, scoring, and playtesting. It
does not make unavailable MCP tools callable.

Consult [tool-catalog.md](tool-catalog.md) before each stage:

- `CURRENT`: use the named MCP tool.
- `CURRENT / AUTHOR`: use the named MCP tool only in this explicitly selected author/umpire mode.
- `EXPERIMENTAL`: use only when a dedicated tool is registered and the exact CMO build has passed
  its compatibility probe.
- `MANUAL LUA`: official Lua supports a missing typed operation. Generate reviewable Lua and
  mounting instructions. Do not confuse this with current author-only event and special-action
  tools, which intentionally accept Lua source and can execute it when composed with activation.
- `UNSUPPORTED`: neither complete the step nor claim success.

## Contents

- [Establish the authoring contract](#establish-the-authoring-contract)
- [Use the authoring sequence](#use-the-authoring-sequence)
- [Design events, triggers, conditions, and actions](#design-events-triggers-conditions-and-actions)
- [Design special actions](#design-special-actions)
- [Design scoring and end conditions](#design-scoring-and-end-conditions)
- [Prepare manual Lua safely](#prepare-manual-lua-safely)
- [Playtest from clean perspectives](#playtest-from-clean-perspectives)
- [Run the release checklist](#run-the-release-checklist)

## Establish the authoring contract

Before mutation, record:

- mode: `SCENARIO_AUTHOR` or `UMPIRE`;
- scenario file, database, build, start time, duration, and intended player side;
- purpose of the change and whether it is permanent, a test fixture, or a one-time adjudication;
- permitted omniscient scope;
- object namespace or naming convention;
- rollback method, usually a known save or an unsaved working copy;
- acceptance checks and sides from which the result must be tested.

Never reuse author-derived enemy ground truth in a later fair player test. Reload a clean save and
begin a fresh `LIVE_PLAYER` information cycle.

Keep an authoring ledger:

| Step | Object type | Intended name/GUID | Operation or Lua primitive | Request ID/status | Readback | Revert |
|---|---|---|---|---|---|---|

Use stable, unique names. Search before every create. Record returned GUIDs immediately.

Before the first mutation, require a successful `cmo_bridge_status` for the loaded scenario. Most
authoring mutations return a durable queue receipt. Record its `request_id`, use
`cmo_request_get`/`cmo_request_wait` for the eventual result, and never treat a wait timeout as
cancellation. Independent changes may be submitted FIFO while CMO is paused, but a step that needs
an earlier object GUID or result must wait for that request to complete. Local queue inspection and
cancellation of a still-`queued` request remain available during the pause; active work survives
MCP/client restart. A process/runtime/scenario binding mismatch must reject or quarantine old work,
not carry it into the new authoring target. Unit/mission delete preview and confirm retain their
separate synchronous confirmation contract; keep CMO polling through both calls so the short-lived
token does not expire while paused.

## Use the authoring sequence

Follow this order so later objects never depend on an unresolved earlier layer.

Before each consequential authoring batch, preserve the scenario's current compression multiplier,
map it to a setter code with [tool-catalog.md](tool-catalog.md), submit code `0`, wait for completion,
verify multiplier `1`, and refresh the objects that batch will touch. Restore the mapped code only
after all dependent requests and readback succeed. If the user pauses while composing a batch,
enqueue only independent work and resume time to execute it before synchronous verification.

### 1. Scenario settings and authoring baseline

1. Open an unsaved working copy or create a recoverable save.
2. Read current scenario identity and timing with `cmo_scenario_get`.
3. If editing an existing saved scenario, call `cmo_scenario_context_get` to audit its saved
   description, current selected side's briefing, and score thresholds. It cannot see unsaved
   editor changes and must never be used to inspect a non-selected side during a player test.
4. Define title, briefing intent, database, date and time, duration, weather or environment,
   player side, difficulty assumptions, and victory conditions.
5. Set the title with `cmo_scenario_title_set`; set current time, start time, or duration with
   `cmo_scenario_timeline_set`.
6. Read weather with `cmo_scenario_weather_get`. When changing it, call
   `cmo_scenario_weather_set` with the complete temperature, rainfall, undercloud, and sea-state
   tuple, then verify the returned tuple.
7. Read scenario metadata again after each settings group.
8. Briefing and description writes, database selection, difficulty/complexity, and other
   unexposed environmental settings still require the editor or a reviewed author workflow. Save
   before using `cmo_scenario_context_get` as readback.
9. Establish object prefixes, for example `BLU-`, `RED-`, `NTR-`, `EVT-`, `TRG-`, `CND-`,
   `ACT-`, and `SA-`.

The authoring surface is deliberately field-specific rather than a general scenario-settings
editor. Do not bypass a current typed field with Lua merely because a script carrier exists. When
the user explicitly requests script-based scenario logic, apply the code-execution controls below.

### 2. Sides, options, and mutual posture

1. Define every side's purpose, human or AI role, awareness, proficiency, and diplomacy.
2. Read existing sides and postures with `cmo_side_list` and `cmo_side_posture_get`.
3. Create missing sides with `cmo_side_add` only after checking for an exact existing name.
4. Set awareness, proficiency, civilian tracking, collective responsibility, and AI-only control
   with `cmo_side_options_set`.
5. Set each directed posture pair with `cmo_side_posture_set`.
6. Verify posture in both directions; A-to-B and B-to-A are separate relationships.
7. Confirm the intended player side and whether neutral or civilian sides require special
   awareness or auto-tracking behavior.

`cmo_scenario_get.player_side_guid` identifies the side currently selected for play in CMO. It is
useful for player-perspective testing, but does not by itself prove which sides the author intends
to make playable.

Wait for each new side's returned GUID before submitting options or directed-posture changes that
depend on it. The reverse posture may be queued independently only after both side GUIDs are known.

Do not use side-option or posture writes to alter a live-player problem. A one-way posture result
does not prove the reverse relationship changed.

### 3. Units, bases, groups, and initial state

1. Build an order of battle with side, DBID, name, location or host base, loadout, proficiency,
   readiness, initial emissions, and intended mission role.
2. Add units with `cmo_unit_add` only in author or umpire mode. Supply either a base or coordinates
   as required by the tool contract.
3. Wait for each required creation result, retain its GUID, then read the created unit.
4. Set supported movement, course, hold, sprint-and-drift, cavitation, and sensor properties with
   current tools.
5. Set aircraft loadouts. Author mode may deliberately specify ready time or ignore magazines,
   but record the intervention because it changes normal logistics.
6. Adjust magazines or mount reloads only when defining the scenario's initial inventory or a
   controlled adjudication. Read the inventory before and after.
7. Use the editor or reviewed Lua for group creation, hosting relationships, proficiency,
   damage, fuel, and other initial-state fields not exposed by current tools.
8. To remove a unit, call `cmo_unit_delete_preview`, verify the exact bound object, then pass the
   unchanged unit GUID and short-lived token to `cmo_unit_delete_confirm`. Never bypass preview or
   reuse a token for another object.

Do not use contact posture or player-side observations as a substitute for setting an author's
ground-truth order of battle.

### 4. Reference points, zones, and geography

1. Define named geographic products: patrol boxes, prosecution areas, support tracks, axes,
   phase lines, target areas, mine areas, recovery areas, and protected zones.
2. Create and update ordinary fixed reference points with current tools. Wait for created point
   GUIDs before using them in dependent mission geometry. For moving geometry,
   create relative points anchored to a unit, contact, or reference point with distance, bearing,
   and fixed or rotating bearing type.
3. Read the points back and verify side ownership, coordinates, anchor GUID, offset, and bearing
   behavior.
4. Add them to inactive missions only after geometry is correct.
5. Advance time and test anchor movement, heading change, anchor loss, group-lead change where
   relevant, and save/reload before relying on the moving area.
6. Treat exclusion, no-navigation, standard, and custom-environment zones as non-current bridge
   capabilities unless dedicated tools are registered.

Relative mission geometry is current. Independent zone objects exposed by `ScenEdit_AddZone` and
`ScenEdit_SetZone` are not.

### 5. Missions and operation architecture

1. Translate the scenario concept into player-usable patrol, support, strike, ferry, mining,
   mine-clearing, and cargo missions.
2. Create each ordinary mission inactive with `cmo_mission_create`; wait for its returned GUID
   before submitting updates, targets, assignments, flight plans, or activation.
3. Configure ordered zones, targets, schedules, force sizes, profiles, and class-specific options
   with `cmo_mission_update` and target or cargo tools.
4. Assign only units intended to be preassigned at scenario start.
5. Read back the complete current projection before activation.
6. Activate missions that should begin active; leave conditional or player-created missions
   inactive as the design requires.
7. Remove an obsolete mission only through `cmo_mission_delete_preview` followed by
   `cmo_mission_delete_confirm` with the same side GUID, mission GUID, and issued token.

For advanced air planning:

- Create a real task pool with `cmo_mission_create(category="task_pool", ...)`, wait for its GUID,
  then create child packages with `category="package"` and that exact `parent_task_pool_guid`.
- Generate flights with `cmo_mission_flight_plan_create` using exactly one takeoff or target-time
  schedule, then inspect every returned flight and waypoint through
  `cmo_mission_flight_plan_list`.
- Configure receiver and tanker mission policy with `cmo_mission_air_refueling_update`.
- Verify package parent/child readback, flight timing, tanker fields, and save/reload before
  release.
- Operation-planner phase/dependency fields, automatic multi-mission queues, and generated-flight
  waypoint mutation remain unavailable. Model those explicitly in the design and use event gates,
  regenerated plans, or manual authoring only when necessary.

### 6. Doctrine, WRA, and EMCON

1. Establish broad side doctrine first.
2. Add mission doctrine for role-specific behavior.
3. Add unit overrides only for deliberate exceptions.
4. Read effective doctrine before every change and distinguish inherited from explicit settings.
5. Set WRA by weapon and target category only after confirming the intended weapon, target type,
   salvo size, shooter count, firing range, and self-defence range.
6. Set radar, sonar, and OECM state together with scenario detection and engagement assumptions.
7. Verify doctrine, WRA, EMCON, sensor state, and mission behavior from each relevant side.

Do not use author omniscience to hide a design that gives the player insufficient warning,
classification, fuel, weapons, or reaction time.

### 7. Events, T/C/A, and phase logic

Build in this dependency order:

1. Use `cmo_event_component_set` to add triggers, conditions, and actions. Set `kind`, `mode`,
   subtype, a unique description, and the official subtype-specific fields in `parameters`. Resolve
   creation requests before their component identifiers are needed.
2. Create the event with `cmo_event_set(mode="add", ...)` and wait for its identifier. Leave it
   inactive while assembling;
   the tool defaults new events to inactive, hidden, non-repeatable, and 100 percent probability
   unless specified otherwise.
3. Attach components with `cmo_event_component_link` in trigger, condition, then action order.
4. Read the event with `cmo_event_get` at property and component detail levels. Use
   `cmo_event_list` to detect duplicates and shared components.
5. Set visibility, repeatability, probability, and final active state with `cmo_event_set`.
6. Run isolated positive and negative tests, then reread state and effects.

Use `mode="replace"` when intentionally exchanging one linked component and supply the replacement
selector. Detach reusable components before removing an event. Remove an event or component only
after discovering every dependent link.

The event-component tool allowlists the T/C/A envelope and subtype, but `LuaScript` action or Lua
condition source supplied in `parameters` is free-form executable code. Linking and activating
such a component can execute that code inside the local CMO process with scenario-Lua authority.
Before creating it, save a scenario copy and review the complete source line by line. Keep the
event inactive and non-repeatable by default, read back the exact normalized source and links, and
activate only within the trusted `SCENARIO_AUTHOR` or `UMPIRE` scope. Never use this path in
`LIVE_PLAYER`.

### 8. Special actions

1. Define the player decision, availability conditions, visible description, repeatability,
   resulting Lua or event action, score effect, and post-execution state. Save a scenario copy and
   review every Lua source line before submitting it.
2. Create it with `cmo_special_action_create`; wait for the result before inspection or update. New
   actions are inactive and non-repeatable unless explicitly requested.
3. Inspect it with `cmo_special_action_list`, then revise or remove it with
   `cmo_special_action_update`.
4. Activate it only after the name, player description, repeatability, and exact normalized Lua
   source match the reviewed design; resolve that update request before execution.
5. Execute newly authored or changed Lua only as an explicit `SCENARIO_AUTHOR` or `UMPIRE` test,
   then inspect all resulting scenario, mission, unit, contact, event, and score state. The
   create/update plus execute composition is local CMO-process code execution.
6. Test inactive, active, one-shot, repeatable, and save/reload behavior.
7. Verify that execution communicates enough information to the player and cannot be farmed for
   unintended repeated effects.

### 9. Scoring, losses, and termination

1. Map every score change to a scenario objective, observable event, side, point value, reason,
   repeatability rule, and cap.
2. Include positive achievement, negative loss, time pressure, collateral or ROE penalties, and
   terminal victory or defeat as appropriate.
3. Prefer event `Points` actions for rule-driven scoring. Use direct score setting only for
   initialization, reset, migration, or a deliberate adjudication.
4. Use current `cmo_score_get` to read a side's score.
5. Use `cmo_score_set` for an absolute score assignment, preserve its required reason, resolve the
   queue request, and read the score back.
6. Create Points or EndScenario actions with `cmo_event_component_set(kind="action", mode="add",
   ...)`, attach them with `cmo_event_component_link`, and verify them before activating the event.
7. Scoring-log editing remains unavailable; use score/event readback and observed effects as
   evidence.
8. Test each scoring path once, repeat it when repeatability matters, and test failure cases that
   should not award points.

### 10. Playtest and release

Run three distinct passes:

1. **Author structural pass:** omniscient verification of objects, GUID links, inventories,
   doctrine, events, scoring, and persistence.
2. **Player information pass:** reload a clean save, switch to the intended side, and verify that
   briefing, contacts, uncertainty, special actions, and available controls are sufficient without
   author knowledge.
3. **Stress and recovery pass:** test manual pause/resume with queued mutations, MCP restart and
   request recovery, cancellation before activation, high and low time compression, save/reload,
   polling interruption, binding mismatch protection, mission completion, failed dependencies,
   destroyed anchors, exhausted tankers, alternate outcomes, and scenario termination.

Do not call a player playtest fair if the same agent retains author-only enemy information in its
decision context. Use a fresh task or explicitly label the pass umpire-assisted.

## Design events, triggers, conditions, and actions

For each event, write this compact specification before implementation:

| Field | Required content |
|---|---|
| Purpose | One observable scenario effect |
| Trigger | What starts evaluation, with time or area tolerance |
| Conditions | Side posture, Lua boolean, scenario-start state, or other gates |
| Actions | Mission state, points, message, teleport, Lua, or scenario end |
| Repeatability | One-shot or repeatable, plus anti-farming guard |
| Visibility | Whether the player sees the event or message |
| State | Any key/value or object state used for idempotency |
| Verification | Readback and positive/negative tests |

Use unique names and GUIDs returned by creation. Avoid a monolithic Lua action when native trigger,
condition, and action types can express the logic. When Lua is necessary:

- show and review the complete source line by line before it is stored;
- keep the script bounded and deterministic;
- validate selected objects before mutation;
- make repeatability explicit;
- store only necessary persistent state;
- return or log a clear result;
- avoid depending on UI selection or editor-only temporary variables.

For the bridge polling event itself, preserve the enabled repeatable Regular Time trigger and its
Lua action from [setup.md](setup.md). Do not repurpose it for scenario gameplay.

## Design special actions

Treat a special action as a player-facing command surface, not a hidden author shortcut.

Its Lua body is code, not configuration data. Create it inactive and non-repeatable, read back the
exact stored source, and execute it only from a saved scenario copy after trusted-author review.
Creating, changing, enabling, or testing a Lua-bearing special action is forbidden in
`LIVE_PLAYER`; executing an existing visible scenario-authored action remains a legitimate player
control when the scenario makes it available.

Specify:

- side and visible label;
- player-facing description and consequence;
- eligibility and whether the action should be active initially;
- repeatability and state guard;
- target objects and failure behavior;
- score and message effects;
- verification after execution.

Use a separate special action for materially different player choices. Do not expose author debug
actions in the release scenario unless the user intentionally wants them.

## Design scoring and end conditions

Maintain a scoring matrix:

| Objective or loss | Side | Trigger | Points | Repeatable/cap | Player message | End-state relation |
|---|---|---|---:|---|---|---|

Check that:

- points reward the desired behavior rather than a proxy that can be exploited;
- mutually exclusive outcomes do not both score;
- repeated triggers cannot farm points beyond their cap;
- partial success and mission-essential losses are represented intentionally;
- the score thresholds and scenario-end actions match the briefing;
- neutral or allied losses affect the correct side;
- late events cannot fire after termination unless intended.

## Prepare manual Lua safely

When official Lua supports a missing bridge operation:

1. State that the step is `MANUAL LUA`, not an MCP call.
2. Cite the official function and list the exact objects and fields required.
3. Generate one reviewable script with a unique namespace and comments explaining prerequisites.
4. Make creates discover-before-create where the API permits it.
5. Separate preflight, mutation, readback, and error reporting.
6. Tell the user whether to run it once in the Lua Console or mount it as an event Lua action, and
   what trigger should invoke it.
7. Give a verification script or editor checklist.
8. Record that the bridge has no one-call generic `lua.eval`/`lua.call` tool, but its author-only
   event and special-action tools can store and execute free-form Lua. Use those carriers only for
   the explicitly requested scenario artifact or umpire test after line-by-line review, inactive
   creation, exact readback, and saved-copy testing; never use them in `LIVE_PLAYER` or as an
   unreviewed generic escape hatch.

Do not generate manual Lua for current title/timeline/weather, side, posture, relative-RP,
task-pool/package, flight-plan/TOT, AAR, event/T/C/A, special-action, score, unit-delete, or
mission-delete work. Prefer those bounded MCP tools and their readback.

For event scripts, use CMO-compatible line endings and avoid external files or libraries that the
retail Lua environment cannot load.

## Playtest from clean perspectives

Use a fresh save and, preferably, a fresh agent task for each player-side test. Verify:

- after selecting that player side and saving, `cmo_scenario_context_get` returns the intended
  description, only that side's briefing, and matching score thresholds;
- briefing and objective clarity without the tester having to supply the mission separately;
- initial detections and uncertainty;
- time to first meaningful decision;
- mission and special-action discoverability;
- fuel, loadouts, weapons, readiness, and recovery feasibility;
- hostile reactions under likely and dangerous courses;
- event and scoring behavior under success, failure, and partial outcomes;
- no hidden dependency on editor mode or author-only actions;
- bridge polling after save/reload and after resuming from a manual pause;
- acceptable performance at intended time compression.

Record defects by layer: data, geometry, mission, doctrine, event, scoring, bridge, or player
information. Fix the lowest causal layer rather than compensating with unrelated scripts.

## Run the release checklist

- [ ] Scenario identity, database, time, duration, and intended player side are correct.
- [ ] All side options and directed postures are verified.
- [ ] Units, bases, loadouts, readiness, inventories, and names match the order of battle.
- [ ] Reference points and zones are owned by the correct side and survive save/reload.
- [ ] Missions are correctly active or inactive, with valid targets, assignments, and schedules.
- [ ] Relative mission areas follow their anchors; task-pool/package parents and children match.
- [ ] Generated flights, takeoff/TOT values, and mission AAR policies match the intended plan.
- [ ] Doctrine, WRA, EMCON, sensors, and fuel or weapon thresholds support intended behavior.
- [ ] Every trigger, condition, action, event, and link has positive and negative tests.
- [ ] Special actions have correct visibility, repeatability, descriptions, and effects.
- [ ] Score changes, caps, messages, and termination conditions match the briefing.
- [ ] No debug, omniscient, or author-only action remains unintentionally player-accessible.
- [ ] Any remaining operation-planner, multi-mission, waypoint-mutation, or zone-object dependency
      is implemented manually and labeled, or removed from the design.
- [ ] The polling event is enabled, repeatable, isolated from game logic, and survives save/reload.
- [ ] A clean player-perspective run is feasible without author information.
- [ ] The working scenario is saved only after all intended changes and tests are complete.
