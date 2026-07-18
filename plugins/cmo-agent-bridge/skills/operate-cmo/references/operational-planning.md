# Plan and Command CMO Operations

Use this reference when the user asks the agent to assess a battlespace, devise a plan, command a
force, or adapt an operation rather than merely perform one explicit edit. It adapts public joint,
air, and maritime doctrine to CMO's operational and tactical scale. It is a decision aid, not a
rigid staff process.

Apply the operating mode selected in `SKILL.md`. In `LIVE_PLAYER`, all analysis remains inside the
commanded side's information boundary. In `SCENARIO_AUTHOR`, use the same planning method to design
forces and opposing plans, but do not present omniscient results as fair player command. Consult
[tool-catalog.md](tool-catalog.md) before translating a plan into bridge operations.

## Contents

- [Preserve the player perspective](#preserve-the-player-perspective)
- [Select planning depth](#select-planning-depth)
- [Run the command estimate and execution cycle](#run-the-command-estimate-and-execution-cycle)
- [Maintain decision matrices](#maintain-decision-matrices)
- [Apply CMO mission patterns](#apply-cmo-mission-patterns)
- [Map operational functions to CMO](#map-operational-functions-to-cmo)
- [Plan against capability maturity](#plan-against-capability-maturity)
- [Doctrine basis](#doctrine-basis)

## Preserve the player perspective

- Use only information available to the side being commanded. Treat `cmo_contact_list` and
  `cmo_contact_get` as the normal source of adversary information.
- Do not list or inspect adversary units, inventories, loadouts, doctrine, or missions through
  `cmo_unit_*`, `cmo_mission_*`, or `cmo_doctrine_*` during normal play. Do so only when the user
  explicitly requests editor, umpire, or test-mode omniscience.
- Use `cmo_side_list` to resolve side identity and posture relationships, but ignore opponent unit,
  contact, and mission counts as intelligence. Query scores and special actions only for the
  commanded side unless the scenario or user explicitly authorizes an umpire view.
- Do not treat an absent contact as an absent unit or a vanished contact as a confirmed kill.
- Keep contact GUIDs distinct from actual unit GUIDs. Use `actual_unit_guid` only when CMO exposes
  it to the observing side.
- Label every consequential statement as an observed fact, an assumption, or an estimate. Give
  estimates high, medium, or low confidence and state what would disprove them.
- Respect the scenario briefing, user intent, posture, ROE, target identity, and weapon-control
  settings. Score is an indicator, not the commander's intent.

CMO Manual section 4.5, especially pages 85-91, describes the contact-filtered information
available to a normal player. The bridge must not be used to bypass that game model.

## Select planning depth

Use the lightest process that safely fits the request.

### Direct execution

Use this for a bounded, explicit order such as moving a patrol zone, assigning named aircraft, or
changing one mission setting. Confirm identity, prerequisites, and current state; submit the
mutation; resolve its queue receipt; read back. Do not create multiple courses of action merely to
restate the user's order.

### Abbreviated estimate

Use this for a new mission, a local tactical problem, or a consequential adjustment. Produce:

1. the objective and immediate constraint;
2. the relevant friendly and adversary picture;
3. one recommended course and one meaningful alternative or abort condition;
4. the CMO mission design, support, reserve, and verification plan.

### Full operational estimate

Use this when asked to command a battle or campaign, when the end state is unclear, when several
domains or phases must be synchronized, or when failure could consume scarce high-value forces.
Run the full cycle below and compare at least two genuinely different courses of action.

## Run the command estimate and execution cycle

### 1. Establish intent, end state, and authority

After resolving `cmo_scenario_get.player_side_guid` through `cmo_side_list`, call
`cmo_scenario_context_get`. Use the scenario description and only the current side's briefing as
the primary source of assigned mission, scenario background, known intelligence, ROE, deadlines,
constraints, and victory conditions. Separate what the briefing explicitly states from your
inferences. If a later user order conflicts with it, surface the conflict and follow an override
only when it is explicit. If neither the context nor the user supplies an operational objective,
stop before autonomous force commitment and ask; force composition and existing missions do not
prove the campaign goal.

Write a one-sentence mission statement containing who, what effect, where, when, and why. Record:

- the desired end state and three to seven observable success conditions;
- must-do and must-not-do constraints;
- the planning and decision horizon;
- acceptable loss or risk guidance when the user supplied it;
- termination, pause, or hand-back conditions;
- the degree of autonomy the user granted.

Ask the user only when missing authority, ROE, target identity, or an end-state choice would
materially change the operation. Otherwise state a conservative assumption and continue.

### 2. Frame the operational environment

Adapt the four JIPOE steps:

1. Define the relevant area, forces, time window, and information boundary.
2. Describe how range, geometry, bases, sensor coverage, weapons, readiness, and any known
   environmental conditions affect both sides.
3. Evaluate the adversary's observed capabilities, dispositions, dependencies, and vulnerabilities.
4. Describe the adversary's most likely course of action and most dangerous plausible course of
   action, with indicators for each.

Build the picture with:

- `cmo_scenario_get`, `cmo_scenario_context_get`, `cmo_side_list`,
  `cmo_side_posture_get`, and existing missions;
- friendly `cmo_unit_list/get`, combat status, loadouts, inventories, doctrine, WRA, and EMCON;
- observer-side contacts, including age, uncertainty area, detection sources, emissions, possible
  matches, BDA, and weapon allocations;
- reference points and mission zones that define defended areas, patrol areas, axes, and support
  tracks.

Treat weather, terrain effects, communications state, or other data not returned by the bridge as
unknown unless supplied by the user or visibly available through another authorized interface.
When the context result is based on a saved file, treat unsaved editor changes as unknown rather
than silently substituting the older briefing.

Create an information requirement when an unknown could change the selected course. Assign it an
indicator, collection method, latest useful decision time, and conservative default action.

### 3. Convert intent into objectives, effects, and tasks

Keep these distinct:

- **Objective:** the condition that must be achieved.
- **Effect:** the required change in friendly, adversary, or environmental behavior or capability.
- **Task:** the action assigned to a force or mission.

For example: preserve access to a maritime area; prevent hostile air interference during the
transit window; maintain an AAW patrol, an AEW track, a tanker track, and a ready reserve.

Identify the main effort, supporting efforts, protected high-value assets, decisive conditions,
decision points, reserve, sustainment requirement, and initial branches. Define measures before
execution:

- A measure of performance (MOP) shows whether the force performed the assigned task.
- A measure of effectiveness (MOE) shows whether the intended battlespace condition changed.

### 4. Develop courses of action

Each full-estimate course must be suitable, feasible, acceptable, distinguishable, and complete.
Give it:

- a concept and main effort;
- phases or bounded decision windows;
- task organization and mission architecture;
- ISR and identification plan;
- air and missile defence, strike, surface, undersea, mine, and support tasks as relevant;
- doctrine, WRA, EMCON, and sensor concept;
- fuel, weapons, readiness, basing, refuelling, and recovery concept;
- a real reserve that is not already committed to routine coverage;
- abort criteria, a branch for an expected disruption, and a sequel after the current phase.

Do not present nominally different courses that only change mission names or patrol coordinates.

### 5. Wargame and compare

Run action-reaction-counteraction for each friendly course against both the adversary's most likely
and most dangerous courses. Test:

- detection and identification before engagement;
- range, timing, launch, transit, on-station, recovery, and turnaround;
- hostile fighter, missile, IADS, surface, submarine, and mine reactions as applicable;
- loss or displacement of an airbase, carrier, AEW aircraft, tanker, major sensor, or command node;
- weapons and fuel consumption over the required duration;
- whether the force culminates before the objective is achieved;
- whether a branch can be triggered early enough to matter.

Apply hard gates before weighted scoring. Reject a course that exceeds authority, cannot be
resourced, cannot meet the timing, or predictably loses a mission-essential capability.

For remaining courses, score one to five against weighted criteria. Use criteria such as mission
success, robustness against the dangerous adversary course, force preservation, time, endurance,
identification reliability, high-value-asset protection, flexibility, reserve, reversibility, and
scenario-specific ROE risk. Explain the reasons and sensitivity; do not let numerical precision
hide a hard failure.

### 6. Translate the course into CMO mission architecture

Use reference points to turn the concept into ordered geometry. Name missions so their phase and
function remain legible, for example `P1-AAW-North`, `P1-AEW-Rear`, and `P2-Strike-Airbase`.

Reuse a suitable existing mission when its purpose, geometry, assignments, and settings can be
safely adapted. Create only missing missions, and create each new mission inactive. Wait for the
creation request to complete and retain its GUID before dependent work. Then:

1. configure zones, tracks, targets, schedules, flight or group sizes, on-station requirements,
   route profiles, and mission-specific options;
2. read effective doctrine and WRA, then apply deliberate mission overrides and EMCON;
3. inspect candidate readiness, damage, actual fuel, loadout, sensors, mounts, magazines, and host
   stocks;
4. assign main, support, escort, and reserve forces without double-committing them;
5. read back the complete assembled mission;
6. activate it only when its dependencies and decision gate are satisfied.

CMO's doctrine hierarchy is side, mission, then unit. Prefer side policy for broad ROE, mission
policy for the operational role, and unit overrides only for justified exceptions.

Configure only fields marked `CURRENT` in [tool-catalog.md](tool-catalog.md). Use relative
reference points for moving geometry, real task pools and child packages for package structure,
mission flight plans for takeoff/TOT synchronization, and mission-level AAR settings for tanker
policy. Keep operation-planner phases/dependencies, automatic multi-mission queues, and
generated-flight waypoint mutation in the plan only as explicit non-current dependencies with a
manual implementation, approximation, or blocker.

### 7. Execute in bounded decision windows

An initial global plan, phase or objective transition, or multi-domain deployment with substantial
dependencies normally justifies a deliberate pause. Apply the least-intervention time policy and
paused planning sequence in [live-operations.md](live-operations.md): preserve observed UI state and
compression, obtain a fresh decision snapshot, pause, enqueue independent work, list the queue, and
service every current non-terminal request ID through a bounded 1x pulse. Verify terminal results
and explicitly restore the state and rate the Agent changed. Do not submit a dependent action before
its prerequisite result exists.

Do not turn every subsequent decision cycle into a pause. Keep the current compression for routine
execution and local adjustment when the decision horizon permits; use temporary 1x for moderate
timing risk that does not require extended replanning.

Scale real-world battle rhythms into three rolling CMO windows: current execution, the next
prepared phase, and a follow-on branch or sequel. Do not build a fixed 24-hour cycle when the
scenario's decisive events occur in minutes.

For each window:

1. read the contacts, missions, assigned units, combat status, inventories, sensors, and existing
   allocations relevant to the next decision;
2. compare observed indicators with the decision-support matrix;
3. submit one bounded set of orders, retaining request IDs and resolving dependencies;
4. explicitly select the execution compression and let asynchronous actions develop;
5. refresh the state at the next decision point, intervening in time only when its risk warrants it.

Do not let a target of opportunity automatically displace the campaign objective. Divert forces
only when identification and authority are sufficient, the expected gain is worth the disruption,
and the force can recover to the plan.

### 8. Assess and adapt

First ask whether the operational approach is still valid. Then ask whether its tasks are being
performed well.

Compare:

- planned versus actual mission coverage, launches, assignments, and timing (MOP);
- changes in adversary freedom of action, sortie generation, sensor or weapon coverage, access,
  interference, and ability to threaten the objective (MOE);
- friendly damage, fuel, weapons, readiness, high-value-asset exposure, and remaining endurance;
- BDA confidence, contact uncertainty, score change, and alternative explanations.

Choose explicitly among continue, local adjustment, execute a branch, transition to a sequel,
pause and reconstitute, disengage, or reframe and replan. Return to environment framing when a key
assumption fails, the adversary changes its approach, or repeated local adjustments do not improve
the MOE.

## Maintain decision matrices

Keep these compact. A Markdown table or structured notes are sufficient. Use only matrices that
change a decision; do not fill every template for a direct order or small tactical adjustment.

### Critical asset and threat matrix

| Friendly asset, function, or route | Threat | Likelihood | Consequence | Exposure window | Warning and reaction time | Protection or branch | Priority |
|---|---|---|---|---|---|---|---|

Include bases, carriers, AEW and tanker aircraft, replenishment units, amphibious or transport
forces, chokepoints, and sea lines of communication when they are mission-essential. Consider air,
missile, IADS, surface, submarine, mine, ISR, and EW threats. Use the result to decide what must be
defended first and what loss the plan can absorb.

### Fact, assumption, and estimate ledger

| Statement | Type | Evidence and time | Confidence | Impact if wrong | Indicator or collection action | Latest decision time | Branch |
|---|---|---|---|---|---|---|---|

Retain only assumptions that are necessary and reasonable. Bind each to a validation indicator and
an action if it fails.

### Adversary course matrix

| Course | Purpose | Main actions and capabilities | Time window | Observable indicators | Vulnerability | Friendly impact | Confidence |
|---|---|---|---|---|---|---|---|
| Most likely | | | | | | | |
| Most dangerous | | | | | | | |

Add a high-impact alternative when deception or sparse sensing makes two explanations plausible.
Actively look for disconfirming evidence and for activity that should be present but is absent.

### Mission and capability matrix

| Required task | Candidate unit or mission | Readiness and timing | Sensor/weapon fit | Range and endurance | Support dependency | Recovery or second-sortie cost | Suitable? |
|---|---|---|---|---|---|---|---|

Use actual combat status, loadout, inventory, and host stocks. A unit that can launch but cannot
reach, remain, recover, or regenerate within the decision horizon is not a feasible assignment.

### Friendly course comparison

| Criterion | Weight | Course A | Course B | Course C | Reason and sensitivity |
|---|---:|---:|---:|---:|---|
| Mission success | | | | | |
| Robustness | | | | | |
| Force preservation | | | | | |
| Time and endurance | | | | | |
| Information reliability | | | | | |
| Flexibility and reserve | | | | | |

Record hard-gate results above the table. Weights express user priorities and should not
automatically default to force preservation when the stated mission requires accepting loss.

### Synchronization and decision support

| Phase or decision point | Trigger and threshold | Information source | Default action | Branch action | Pre-positioned mission or force | Latest decision time |
|---|---|---|---|---|---|---|

Use observable thresholds. Examples include a confirmed hostile launch wave, a credible emitter
location, loss or fuel depletion of a tanker or AEW asset, weapons falling below the next planned
salvo, or a protected force reaching a geographic line.

### Assessment

| Objective | Desired effect | Task | MOP | MOE | Indicator and threshold | Source and frequency | Decision if crossed |
|---|---|---|---|---|---|---|---|

Use several indicators and trends. Unknown BDA remains unknown. Distinguish physical damage,
functional loss, system-level effect, and the need for another attack.

## Apply CMO mission patterns

### Air control and air defence

- Seek the minimum degree of air control needed at the required place and time; do not assume
  theater-wide supremacy is necessary or feasible.
- Build a chain of detection, identification, decision, engagement, and assessment. Use support
  missions for AEW, tanker, EW, or reconnaissance tracks; `aaw` patrols for sustained local
  defence; and an uncommitted or inactive reserve for surge or replacement.
- Build geometry from the outside inward: sensing and warning, classification and engagement, then
  the vital area. Size it from detection range, track quality, threat speed, decision and response
  time, hostile release range, and friendly weapon reach rather than from a visually neat box.
  Keep AEW and tanker tracks behind a credible protective layer.
- Use a separate prosecution zone only when units should investigate beyond the patrol box.
- Use `air` strike for a bounded intercept effort and `land` strike or `sead` patrol for a specific
  or emitter-driven counterair task. Protect support aircraft through geometry, escorts, EMCON,
  and threat-aware standoff.
- For an airbase objective, use a `land` strike against identified runways, facilities, aircraft,
  fuel, weapons, or command targets as the scenario represents them. Use a `sead` patrol for
  emitting air-defence threats; do not use SEAD as a generic synonym for attacking the airbase.
- Assess leaks through the defended area, hostile ability to interfere, friendly on-station
  coverage, weapons and fuel remaining, and the sustainable replacement rate.

### Maritime control and denial

- Treat sea control as a combined problem of surface warfare, undersea warfare, strike, mine
  warfare, air and missile defence, maritime awareness, and ISR.
- Combine `sea` or `naval` patrols, `asw` patrols, AAW coverage, support tracks, `sea` or `sub`
  strikes, and mining or mine-clearing missions as required by the objective.
- Protect carriers, replenishment ships, amphibious forces, and other high-value units without
  collapsing every sensor and shooter into one vulnerable cluster.
- For ASW, treat the contact as a probability area. Account for track age, uncertainty, sensor
  source, water depth information supplied by the scenario, own noise, cavitation, sonar policy,
  and the datum created by weapons or launches. Use barriers, screens, prosecution areas, and
  quiet search rather than chasing a stale point.
- Assess friendly access, adversary freedom to operate, contact confidence, weapon expenditure,
  mine clearance, and the endurance of the screen.

### Deliberate strike

- Link every target to an objective and desired effect. Do not strike an object merely because it
  is visible.
- Build the sequence: collect and identify; suppress, isolate, or deceive as needed; deliver the
  main attack; recover; assess; decide on reattack.
- Use separate missions when CAP, escort, SEAD, AEW, tanker, EW, reconnaissance, and the main
  strike require different geometry or activation gates. Add exact strike targets and inspect
  existing allocations before manual engagement.
- Check loadout, route distance, flight size, minimum aircraft, tanker availability, target
  uncertainty, WRA, weapon inventory, recovery capacity, and a fallback target or abort gate.
- For a late-breaking contact, use the condensed find-fix-track-target-engage-assess logic, but do
  not compress identification or scenario ROE out of the process.

### ISR, sensors, EW, and EMCON

- Tie every collection mission to a decision or information requirement. Collection without a
  latest useful decision time is merely activity.
- Use support tracks and patrol areas to create persistent or periodic coverage. Review sensor
  state and EMCON together; an active sensor can improve detection while exposing the emitter.
- Plan phases when useful: silent or passive transit, limited surveillance emissions, then
  engagement emissions. Identify the primary emitter, a backup source, and the platforms that
  should remain passive. If silence makes the warning-identification-engagement timeline
  impossible, emit deliberately instead of applying EMCON mechanically.
- Cross-check independent detection sources where possible. Treat ESM, passive sonar, old tracks,
  and broad uncertainty areas as estimates rather than precise firing solutions.
- Position jammers and active sensors for the needed geometry while preserving a recovery route
  and acceptable exposure. Assess whether the information or protection gained justifies the
  emissions.

### Sustainment and reconstitution

- Calculate sustainability from actual fuel, ready time, loadouts, weapon quantities, damage, host
  stocks, tanker availability, and transit/on-station demands. Do not infer it from unit count.
- Treat safe recovery and regeneration as part of feasibility. Check the return leg, recovery
  base or carrier, remaining fuel, rearming capacity, support-track protection, and the cost to the
  next sortie before committing a package.
- Use support missions for tankers, ferry missions for rebasing, cargo missions for modeled
  movement, and normal CMO replenishment behavior during player operations.
- Keep enough force uncommitted to replace losses, cover turnaround, exploit an opportunity, or
  execute a branch. A unit assigned to routine coverage is not a reserve.
- Pause, shorten the defended area, reduce tempo, rebase, refuel, RTB, or reconstitute before a
  force reaches weapons or fuel exhaustion.
- Never use direct magazine or mount adjustment as logistics unless the user explicitly requests
  editor or umpire intervention.

### Degraded command and control

- Preconfigure mission intent, zones, doctrine, WRA, EMCON, withdrawal rules, and branches so
  assigned units can continue useful behavior when the scenario models communications loss.
- Prefer mission-level control for sustained behavior and direct unit commands for bounded
  corrections.
- Event editing is available only in `SCENARIO_AUTHOR` or `UMPIRE`. Do not cross that mode boundary
  to solve a live communications problem. `LIVE_PLAYER` has no Lua escape hatch, dedicated
  out-of-comms control, or complete communications picture; author-only Lua-bearing events and
  Special Actions must never be used to bypass that boundary.

## Map operational functions to CMO

| Operational function | Primary CMO representation | Configure and verify |
|---|---|---|
| Sustained local air defence | `patrol` with `patrol_type="aaw"` | Ordered patrol and optional prosecution points, flight size, on-station, doctrine/WRA/EMCON, assigned readiness |
| AEW, tanker, EW, reconnaissance | `support` | Ordered track, loop, one-third/on-station, transit/station profile, sensors and fuel |
| ASW barrier or screen | `patrol` with `patrol_type="asw"` | Search/prosecution geometry, sonar and depth doctrine, quiet movement, suitable sensors and weapons |
| Surface search or sea control | `patrol` with `patrol_type="naval"` or `"sea"` | Choose the type from the intended CMO role, verify returned patrol type, zones, WRA, sensors, and support |
| Emitter hunting | `patrol` with `patrol_type="sead"` | Area, prosecution behavior, radar-target WRA, emitter confidence, escorts/support |
| One-off air, land, surface, or subsurface attack | `strike` with `strike_type="air"`, `"land"`, `"sea"`, or `"sub"` | Exact perceived targets, escorts, force minimums, range/flight limits, doctrine, allocations and BDA |
| Rebase | `ferry` | Destination GUID, loop behavior, loadout/readiness, arrival readback |
| Lay mines | `mining` | Ordered area, mine inventory, arming delay, spacing and laying method |
| Detect or clear mines | `mine_clearing` | Ordered area, capable sensors/sweep gear and loadout, damage and clearance evidence |
| Move or deliver cargo | `cargo` | Destination unit for transfer or ordered area for delivery, cargo assignment, carrier and inventory readback |

The CMO Manual describes missions and reference points in chapter 7, pages 200-239; doctrine,
EMCON, WRA, and mission control in pages 55-74 and 174-175; contact and combat behavior in pages
281-322; and communications disruption in pages 347-351.

## Plan against capability maturity

Build the operational concept first, then map each required control to one of three states:

| State | Planning treatment |
|---|---|
| `CURRENT` | Assign the named MCP tool, inputs, queue-result dependency, and readback rule |
| `EXPERIMENTAL` | Define the desired result and fallback; execute only with a registered typed tool and exact-build probe |
| `UNAVAILABLE` | Redesign, use an honest approximation, request manual authoring, or declare the blocker |

Do not erase an operational requirement merely because the bridge lacks it. Record the desired
dependency, its effect on feasibility, and the approximation's loss of precision.

### Synchronize complex air operations

For a task-pool, package, TOT, or tanker-dependent operation:

1. define the target or station time, tolerance, launch and recovery bases, threat windows, and
   abort criteria;
2. assign desired package elements and support relationships;
3. calculate readiness, route time, fuel, weapons, tanker demand, recovery capacity, and reserve;
4. identify dependency gates and latest substitution or abort times;
5. create anchored reference points where geometry must move and wait for their GUIDs, then create
   the task pool, wait for its GUID, and create child packages with exact parent readback;
6. configure each receiver mission's AAR policy and each tanker support mission's limits;
7. generate flights from exactly one target-time or takeoff-time schedule, resolve the request, and
   inspect all returned flights and courses;
8. keep all package missions inactive until the synchronized architecture is ready;
9. state which dependencies remain manual decision gates because the bridge does not expose
   operation-planner phase/dependency fields, automatic multi-mission queues, or waypoint
   mutation.

Use the current primitives to build a real CMO package architecture rather than simulating one
through naming alone. Still verify effective movement, parent/child persistence, flight timing,
tanker policy, launch behavior, and save/reload in the running build.

### Preserve temporal control

Represent unexposed synchronization with inactive missions, schedules, decision-support matrices,
conservative time advancement, author-created event gates where authorized, and verified
readback. Anchored mission geometry should move without periodic coordinate rewriting, but it must
still be checked after anchor movement or loss. After a successful `cmo_bridge_status` establishes
the session binding, independent mutations can be durably queued while CMO is paused and will run
FIFO when polling resumes. From a verified pause, list the queue and pass every non-terminal request
UUID to `cmo_simulation_pulse`; it services that complete FIFO set at 1x and restores the pause.
Synchronous CMO reads, the pulse handshake, and Lua queue execution still require the polling event
and advancing scenario time; host UI pause/run does not provide deterministic zero-time or
fixed-duration single stepping.

If a dependency remains queued/active, wait or inspect the same request ID; a wait timeout does not
cancel it. If it is rejected or quarantined, stop dependent orders, preserve the last verified
state, and choose explicitly among substitution, delay, abort, manual author intervention, or
replanning. Client shutdown does not cancel active work, and a process/scenario binding mismatch
must never be bypassed to carry an order forward.

## Doctrine basis

This adaptation uses public, authoritative sources:

- [JP 5-0, Joint Planning landing page](https://www.jcs.mil/Doctrine/Joint-Doctrine-Pubs/5-0-Planning-Series/):
  joint planning is the foundation for campaigns and operations, with commander judgment
  paramount.
- [JP 3-0, Joint Campaigns and Operations landing page](https://www.jcs.mil/doctrine/joint-doctrine-pubs/3-0-operations-series/):
  fundamental guidance for joint campaigns and operations.
- [Commander's Handbook for Persistent Surveillance](https://www.jcs.mil/Portals/36/Documents/Doctrine/pams_hands/surveillance_hbk.pdf),
  chapter III and glossary: the four-step JIPOE model, most likely and most dangerous adversary
  courses, information requirements, MOP, and MOE. This 2011 handbook is pre-doctrinal and is used
  only for its public summary of those joint concepts.
- [AFDP 3-60, Targeting, 1 May 2026](https://www.doctrine.af.mil/Portals/61/documents/AFDP_3-60/3-60-AFDP-TARGETING.pdf),
  pages 19-30: objectives and effects, target development, capabilities and allocation, execution,
  continuous assessment, and the F2T2EA dynamic-targeting sequence.
- [AFDP 3-01, Counterair Operations, 15 June 2023](https://www.doctrine.af.mil/Portals/61/documents/AFDP_3-01/3-01-AFDP-COUNTERAIR.pdf),
  pages 1-3 and 16-24: local and time-bounded control of the air, mission command, JIPOE-informed
  planning, acceptable risk, MOP/MOE, support, and reconstitution.
- [AFDP 3-04, Countersea Operations, 20 September 2023](https://www.doctrine.af.mil/Portals/61/documents/AFDP_3-04/3-04-AFDP-Countersea-Ops.pdf),
  pages 3-5 and 15-23: sea-control elements, air-maritime integration, alternate C2 arrangements,
  and synchronized planning, execution, and assessment.
- [ALSSA Air Operations in Maritime Surface Warfare](https://www.alssa.mil/mttps/aomsw/):
  the public description confirms integration of joint air assets in both preplanned and dynamic
  maritime surface warfare. The detailed publication is restricted and is not reproduced here.
- [USMC announcement for MCRP 5-1C, Operation Assessment](https://www.marines.mil/News/Messages/Messages-Display/Article/897477/availability-of-mcrp-5-1c-mttp-for-operation-assessment-op-assessment/):
  assessment is integrated into planning and operation processes.
