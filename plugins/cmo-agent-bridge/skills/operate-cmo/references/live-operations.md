# Conduct Live CMO Operations

Use this reference only in `LIVE_PLAYER` mode. It covers fair player-side command of a running
scenario. For exact tool contracts and maturity, also consult
[tool-catalog.md](tool-catalog.md). For a full campaign estimate, COA comparison, or multi-phase
plan, also consult [operational-planning.md](operational-planning.md).

## Contents

- [Preserve the player information boundary](#preserve-the-player-information-boundary)
- [Choose command depth](#choose-command-depth)
- [Protect the decision window](#protect-the-decision-window)
- [Build the operating picture](#build-the-operating-picture)
- [Issue bounded orders](#issue-bounded-orders)
- [Build and activate a mission](#build-and-activate-a-mission)
- [Triage and engage contacts](#triage-and-engage-contacts)
- [Operate naval and submarine forces](#operate-naval-and-submarine-forces)
- [Manage air operations and logistics](#manage-air-operations-and-logistics)
- [Plan advanced synchronized air missions](#plan-advanced-synchronized-air-missions)
- [Use existing special actions](#use-existing-special-actions)
- [Run the engagement loop](#run-the-engagement-loop)
- [Recover from errors](#recover-from-errors)

## Preserve the player information boundary

- Read adversaries only through the commanded side's `cmo_contact_list` and `cmo_contact_get`
  results. Do not query enemy units, true inventories, loadouts, missions, doctrine, or exact
  positions.
- Use `cmo_side_list` to resolve identities. Do not treat opponent unit, contact, or mission counts
  as intelligence.
- Treat a vanished contact as lost track unless BDA or another commanded-side observation confirms
  destruction.
- Keep contact GUIDs distinct from actual unit GUIDs. Use `actual_unit_guid` only when returned to
  the observer and required by a tool.
- Label consequential statements as observed fact, assumption, or estimate. Give estimates a
  confidence level and an observable disconfirmation condition.
- Respect the scenario briefing, user intent, side posture, target identity, doctrine, WRA, and
  weapon-control status. Score is an indicator, not the mission.

If omniscient information is requested or exposed, stop the live decision cycle and switch
explicitly to `UMPIRE`; do not continue under the claim of fair play.

## Choose command depth

### Direct execution

Use for a bounded explicit order, such as moving a named patrol zone, assigning identified
aircraft, changing one doctrine field, or ordering one unit to RTB. Confirm identity and
prerequisites, execute once, and read back the result. Do not create artificial COAs.

### Abbreviated estimate

Use for a new mission or consequential local adjustment. State:

1. objective and immediate constraint;
2. relevant friendly and contact picture;
3. recommended action and one meaningful alternative or abort condition;
4. support, reserve, sustainment, and verification.

### Full operational estimate

Use [operational-planning.md](operational-planning.md) for a battle, campaign, multi-domain
operation, complex strike package, unclear end state, or high-risk force commitment.

## Protect the decision window

Use the 1x planning guard before any multi-step read-plan-mutate sequence whose result could become
stale while CMO is running at high compression:

1. Read `cmo_scenario_get`; preserve the exact `time_compression` multiplier, map it back to a
   setter code using [tool-catalog.md](tool-catalog.md), and note scenario time.
2. Set `cmo_scenario_time_compression_set(code=0)` and verify the observed multiplier is `1`.
3. Refresh all decision-relevant reads. Do not plan from the pre-slowdown snapshot.
4. Issue and verify the bounded order set at 1x.
5. Restore the preserved compression only after successful readback. On timeout, scenario change,
   or uncertain mutation outcome, leave CMO at 1x while recovering.

Use this guard for mission construction or restructuring, coordinated assignments, strike packages,
target and weapon planning, doctrine/WRA/EMCON batches, and other consequential commitments. Do not
toggle compression for an isolated read or a trivial bounded order unless the decision horizon
requires it. Keep scenario time advancing: the bridge's Regular Time trigger services requests
without a Special Action while CMO runs at 1x.

## Build the operating picture

Read only what can affect the next decision:

1. Use `cmo_scenario_get` for scenario time, duration, database, current high-level state, and
   `player_side_guid`.
2. Page through `cmo_side_list`, match `player_side_guid` while ignoring only case and surrounding
   braces, and report the matched side's exact name and returned GUID. If it is null or cannot be
   matched uniquely, stop live mutations instead of inferring a side.
3. Read relevant directed relationships from the commanded side to each other side with
   `cmo_side_posture_get`; never substitute the reverse relationship.
4. Read friendly missions, reference points, and assigned units.
5. Read friendly unit details, combat status, aircraft loadouts, and inventories for the forces
   that might be committed.
6. Read commanded-side contacts, then fetch detail for contacts that could change the plan.
7. Read effective doctrine, WRA, EMCON, sensor state, and existing weapon allocations where
   engagement or exposure is possible.
8. Record the decision horizon, information gaps, latest useful decision time, and conservative
   default if an uncertainty remains unresolved.

Follow paging to completion when comparing the entire force, contact set, mission set, or reference
point set. Do not call bridge status before every read; call it for health questions, failures, or
multi-step work that needs the exact runtime identity.

## Issue bounded orders

- Resolve GUIDs before mutation and send only fields that should change.
- Treat a `course` as an ordered waypoint list. Use `course=[]` only when intentionally clearing
  it.
- Before changing a contact posture, compare classification, age, uncertainty, emissions,
  detections, side posture, and ROE. Proximity alone does not establish hostility.
- Before a manual attack, read the attacker's inventory and existing allocations against the
  contact.
- Before mission activation or launch, read damage, actual fuel, ready/airborne time, loadout,
  sensors, and weapons. Coarse fuel and weapon state strings are not sufficient.
- Prefer mission and doctrine control for sustained behavior. Use direct movement, sensor, launch,
  RTB, refuel, or attack orders for bounded tactical corrections.
- Treat accepted commands as asynchronous. Let scenario time advance, then reread the affected
  state.

For an aircraft loadout change during fair play, use the database-default readying behavior. Do
not set an artificially short `time_to_ready_minutes` or set `ignore_magazines=true`; those are
author or umpire interventions.

## Build and activate a mission

1. State the objective, mission class, geometry, timing, supported force, support dependencies,
   reserve, abort gate, and success measure.
2. Resolve the side, candidate units, existing missions, relevant contacts, and reference points.
3. Reuse an existing mission only when purpose, geometry, assignments, doctrine, and timing can be
   safely adapted.
4. Create missing reference points in intended order. Use absolute points for fixed geography and
   relative fixed/rotating points for geometry that must follow an eligible anchor. Keep patrol
   and prosecution areas separate.
5. Check for an exact existing mission name before calling `cmo_mission_create`. A newly created
   bridge mission is inactive; retain its returned GUID.
6. While inactive, configure schedules, ordered zones, force grouping, minimum forces,
   on-station requirements, route profiles, patrol or strike behavior, and targets.
7. Read effective doctrine and WRA. Apply deliberate mission-level overrides and EMCON only where
   the role requires them.
8. Read every candidate's readiness, fuel, damage, loadout, sensors, and weapons. Assign main,
   support, escort, and reserve forces without double-committing them.
9. Read the assembled mission and its assigned units. Activate only when all dependencies and the
   decision gate are satisfied.
10. Read the mission again and report CMO's actual state.

Use contact GUIDs for perceived strike targets where the tool accepts them. Do not convert a
contact into adversary ground truth merely to simplify target assignment.

## Triage and engage contacts

1. List observer-side contacts and fetch details for candidates relevant to the objective.
2. Compare age, uncertainty, classification, emissions, detection history, BDA, and current combat
   relationships.
3. Distinguish the adversary's most likely behavior, dangerous plausible behavior, and collection
   needed to tell them apart.
4. Read side posture and effective doctrine. Change contact posture only when identification,
   authority, and requested ROE justify it.
5. Before manual allocation, inspect weapons already assigned to the contact and the attacker's
   current inventory.
6. Issue one bounded attack order. Do not add another salvo merely because effects are not yet
   visible.
7. Advance or observe time, then reread allocations, firing state, contact state, BDA, and friendly
   fuel and weapons.

## Operate naval and submarine forces

1. Read the unit, combat status, inventory, sensors, course, group or base context, and relevant
   contacts.
2. For surface forces, plan formation or course intent, throttle, hold position, sprint-and-drift,
   sensor state, EMCON, doctrine, and WRA together.
3. For submarines, also account for depth, speed, cavitation avoidance, sonar state, battery or fuel
   endurance, and the uncertainty of the datum.
4. Use patrol missions for persistent search or control, support missions for tracks, and strike
   missions for bounded attacks. Use direct commands for short corrections.
5. Reread damage, fuel, sensor state, mounts, magazines, course, and contacts after engagement or
   replenishment time.

Treat ASW contacts as probability areas. Do not chase a stale point without accounting for track
age, uncertainty, own noise, sensor geometry, and the datum created by weapons or launches.

## Manage air operations and logistics

### Air control and support

- Use AAW patrols for sustained local defence and support missions for AEW, tanker, EW, or
  reconnaissance tracks.
- Build geometry from warning and identification time, threat speed and release range, friendly
  response time and weapon reach, not from a visually neat box.
- Keep AEW and tanker tracks behind credible protection and retain a real reserve.
- Use a separate prosecution zone only when units should investigate beyond the patrol area.

### Refueling

- Build and protect tanker support missions with the current mission tools.
- Read tanker and receiver fuel, loadouts, readiness, distance, and mission assignments.
- Configure the receiver mission with `cmo_mission_air_refueling_update`: permitted tanker
  missions, tanker-usage mode, launch/follow/continue policy, tanker minimums, queue limit, fuel
  threshold, and maximum selection distance. Configure support-mission one-time or
  maximum-receiver limits where applicable.
- Read the mission again and distinguish requested policy from effective CMO readback.
- Use `cmo_unit_refuel` to request refueling, optionally selecting one tanker or a set of tanker
  missions.
- Treat the result as order acceptance. Verify whether the receiver actually rendezvoused,
  refueled, continued, diverted, or returned.
- Mission policy does not prove tanker availability or successful transfer. Observe actual tanker
  launches, queueing, bingo state, losses, receiver fuel, and continuation.

### Cargo and replenishment

1. Read source and destination inventories before moving cargo.
2. For a cargo mission, configure it inactive, update assigned cargo, assign eligible carriers,
   read it back, then activate.
3. Treat cargo transfer and unload as non-idempotent. Reread both inventories before retrying.
4. Observe normal expenditure, rearming, and replenishment during live play. Never use
   `cmo_unit_magazine_adjust` or `cmo_unit_mount_reload_adjust` as logistics.

## Plan advanced synchronized air missions

Relative mission geometry, task pools/packages, mission flight plans with takeoff/TOT scheduling,
and mission-level AAR planning are `CURRENT`. Use them as one synchronized architecture while
keeping operation-planner phases, automatic multi-mission queues, and waypoint mutation outside
the claimed result.

### Design the package

1. Define objective, target or station time, acceptable timing tolerance, launch bases, recovery
   bases, threat windows, and abort criteria.
2. Build the desired force elements: main attack or patrol, fighter escort, SEAD, AEW, EW,
   tankers, reconnaissance, recovery support, and reserve.
3. Calculate readiness, route length, on-station time, fuel, weapons, tanker demand, launch
   sequence, recovery capacity, and second-sortie cost from actual CMO state.
4. Identify synchronization dependencies and the latest time at which each element can fail,
   substitute, or abort without breaking the objective.
5. Keep all missions inactive until dependencies and readback match the plan.

### Moving mission areas

- Create each corner or track point with `cmo_reference_point_add` using
  `relative_to_type`, `relative_to_guid`, `relative_bearing_deg`,
  `relative_distance_nm`, and `bearing_type`.
- Use `bearing_type="fixed"` for a true-bearing offset and `"rotating"` when geometry should turn
  with the anchor heading. Keep all points in one area anchored consistently unless the design
  intentionally mixes frames.
- Supply the returned RP GUIDs in order to the inactive mission and verify the mission geometry.
- Advance time and reread the RPs and mission after anchor movement. Test anchor loss and
  save/reload before relying on the area through a long operation.
- Use `cmo_reference_point_update` to re-anchor or adjust the offsets; use `clear_relative=true`
  only when intentionally converting away from the relative relationship.

### Task pools and packages

- Create the pool with `cmo_mission_create(category="task_pool", ...)`.
- Create each child with `category="package"` and the exact `parent_task_pool_guid`. Keep every
  child inactive while assembling geometry, targets, timing, doctrine, support, and assignments.
- Read the pool and children back; verify each package's parent and the pool's package GUID list.
- Assign units only through the current ordinary assignment contract. The bridge does not expose
  `AllowMultiMission` or a deterministic assignment queue, so do not promise dynamic reassignment
  of one unit across several package missions.
- Use explicit decision gates or author-created events when dependencies must change activation.
  Operation-planner phase/dependency fields themselves are not current.

### TOT and per-flight plans

- Set the inactive mission's geometry, targets, force size, assignments, doctrine, and support
  before generating flights.
- Call `cmo_mission_flight_plan_create` with exactly one schedule form:
  `date_on_target` plus `time_on_target`, or `takeoff_date` plus `takeoff_time`. Dates use
  `YYYY/MM/DD`; times use `HH:MM:SS`.
- Call `cmo_mission_flight_plan_list` and inspect the mission timing, every returned flight GUID,
  and its waypoint course. Reconcile launch sequence and support timing across all package
  elements before activation.
- Treat the generated course as read-only through the current MCP surface. Waypoint
  insert/update/delete and timing refresh are not implemented; if route changes invalidate the
  plan, regenerate safely or report the need for author/manual intervention.
- Observe actual launch, transit, station/target timing, abort, and recovery. A stored TOT is a
  plan, not evidence that all elements achieved it.

### Full tanker planning

- Create and protect the tanker support mission, set its track and on-station policy, assign
  suitable tankers, and configure support-specific one-time or receiver-limit fields.
- Configure every receiver mission with `cmo_mission_air_refueling_update`. Use GUIDs for the
  allowed tanker missions and set only deliberate policy fields.
- Read back all AAR fields, then calculate whether actual tanker numbers, fuel, offload,
  rendezvous geometry, queue demand, recovery, and reserve meet the package requirement.
- Build branches for delayed launch, missing tanker, tanker loss, queue saturation, receiver bingo,
  diversion, and abort. Verify behavior after time advances and after save/reload.

These current primitives support a substantially richer ATO-style workflow, but they do not expose
operation-planner phase/dependency graphs, automatic multi-mission assignment, or generated-flight
waypoint mutation. State those boundaries explicitly when they affect feasibility.

## Use existing special actions

1. List actions only for the commanded side.
2. Read name, description, active state, and repeatability before execution.
3. Execute an existing active action only when the user requests it or it is an explicit,
   understood scenario step.
4. Treat execution as accepted, then inspect scenario, missions, units, contacts, and score for
   effects.

Do not create or edit special actions in `LIVE_PLAYER`.

## Run the engagement loop

1. Enter the 1x planning guard when the next assessment or order set is consequential.
2. Read contacts, missions, assigned units, combat status, inventories, sensors, allocations, and
   decision indicators relevant to the next action.
3. Compare observations with assumptions, adversary courses, objective, MOPs, and MOEs.
4. Issue one bounded set of orders.
5. Restore or raise compression long enough for asynchronous actions to develop, then re-enter the
   1x planning guard before the next consequential assessment.
6. Reread fuel, damage, readiness, weapons, sensors, allocations, mission coverage, contact
   uncertainty, BDA, and score.
7. Choose explicitly: continue, adjust locally, execute a branch, replenish, pause, disengage, or
   reframe.

Do not let a target of opportunity displace the objective without sufficient identification,
authority, expected gain, and a recovery path.

## Recover from errors

- MCP tools absent: enable the plugin and start a new agent task.
- `CMO_NOT_RUNNING`: start CMO and load the intended scenario.
- `BRIDGE_NOT_PREPARED`: use [setup.md](setup.md).
- `BRIDGE_UNRESPONSIVE` or a status-handshake `REQUEST_TIMEOUT`: the bridge cannot distinguish a
  paused scenario from an inactive or unloaded polling event. If paused, resume at 1x until the
  tool returns, or repeat `Alt+1` 15-second time steps as needed. If time is already advancing,
  repair the repeatable Regular Time polling event. While the call is waiting, a pending request
  will be serviced automatically when polling resumes; after the bounded attempts are exhausted,
  follow recovery state rather than assuming that the delayed mutation will still run.
- `SCENARIO_CHANGED`: accept the observed lineage only when the user intends to operate the newly
  loaded scenario.
- Timeout after a non-idempotent mutation: search for the created object or resulting state before
  any retry.
- Other structured errors: report the code and actionable message without inventing state.
