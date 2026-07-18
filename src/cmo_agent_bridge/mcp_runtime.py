from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError

from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.queue_models import (
    CancelQueuedOperationResult,
    QueueSummary,
    QueueWaitResult,
    QueuedOperationList,
    QueuedOperationReceipt,
    QueuedOperationStatus,
)
from cmo_agent_bridge.bootstrap import (
    POLL_ACTION_SCRIPT,
    ApplicationRuntime,
    PreparedBridge,
    build_application_runtime,
    prepare_bridge,
)
from cmo_agent_bridge.config import BridgeConfigStore
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget
from cmo_agent_bridge.operations.models import (
    BridgeStatusResult,
    ScenarioContextResult,
    ScenarioContextStatus,
    ScenarioResult,
    ScenarioScoringThresholds,
)
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot, render_dispatcher
from cmo_agent_bridge.scenario_context import (
    ScenarioContext,
    ScenarioContextReader,
    ScenarioContextReaderPort,
)
from cmo_agent_bridge.state.operation_queue import OperationQueueState
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.ui_time import (
    SimulationRunState,
    TimeRate,
    UiTimeController,
    UiTimeState,
)


_ERROR_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(
    dict[str, JsonValue],
    config=ConfigDict(strict=True),
)
RuntimeState = Literal["ready", "unconfigured", "not_prepared", "error"]
_TERMINAL_QUEUE_STATES = frozenset(
    {
        OperationQueueState.COMPLETED,
        OperationQueueState.REJECTED,
        OperationQueueState.QUARANTINED,
        OperationQueueState.CANCELLED,
    }
)


class McpBridgeDiagnostic(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    runtime_state: RuntimeState
    ready: bool
    bridge_version: str
    runtime_tag: str
    config_path: str
    game_root: str | None
    dispatcher_path: str | None
    inbox_path: str | None
    poll_path: str | None
    lua_action: str
    error_code: str | None
    error_message: str | None
    required_next_action: str


class McpBridgePrepareResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    ready: Literal[True]
    bridge_version: str
    runtime_tag: str
    game_root: str
    dispatcher_path: str
    inbox_path: str
    poll_path: str
    lua_action: str
    next_step: str


class McpTimeState(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    state: SimulationRunState
    rate_code: int = Field(ge=0, le=5)
    multiplier: int = Field(ge=1)
    process_pid: int = Field(ge=1)
    process_create_time: float = Field(ge=0, allow_inf_nan=False)
    window_handle: int = Field(gt=0)
    window_title: str


class McpTimeSetResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    before: McpTimeState
    after: McpTimeState
    changed: bool


class McpTimeControlError(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    code: ErrorCode
    message: str
    details: dict[str, JsonValue]


class McpSimulationPulseResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    ok: bool
    released: bool
    forced_rate_code: Literal[0] = 0
    before: McpTimeState
    after: McpTimeState | None
    requests: tuple[QueuedOperationStatus, ...]
    handshake: BridgeStatusResult | None
    timed_out: bool
    final_pause_verified: bool
    prior_rate_restored: bool
    work_error: McpTimeControlError | None
    pause_error: McpTimeControlError | None
    rate_restore_error: McpTimeControlError | None
    elapsed_seconds: float = Field(ge=0, allow_inf_nan=False)


class UiTimeControllerPort(Protocol):
    async def get_state(self) -> UiTimeState: ...

    async def pause(self) -> UiTimeState: ...

    async def resume(self, rate: TimeRate | None = None) -> UiTimeState: ...

    async def set_rate(self, rate: TimeRate) -> UiTimeState: ...

    async def play_1x(self) -> UiTimeState: ...


def _failure_outcome(error: BridgeError) -> InvocationOutcome:
    payload = _ERROR_ADAPTER.validate_python(error.to_payload())
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=payload,
    )


def _time_state(state: UiTimeState) -> McpTimeState:
    return McpTimeState(
        state=state.state,
        rate_code=state.rate.value,
        multiplier=state.multiplier,
        process_pid=state.process.pid,
        process_create_time=state.process.create_time,
        window_handle=state.window_handle,
        window_title=state.window_title,
    )


def _time_control_error(error: BridgeError) -> McpTimeControlError:
    payload = _ERROR_ADAPTER.validate_python(error.to_payload())
    raw_code = payload["code"]
    raw_message = payload["message"]
    raw_details = payload["details"]
    if not isinstance(raw_code, str) or not isinstance(raw_message, str):
        raise TypeError("bridge error payload has invalid scalar fields")
    if not isinstance(raw_details, dict):
        raise TypeError("bridge error payload details must be an object")
    return McpTimeControlError(
        code=ErrorCode(raw_code),
        message=raw_message,
        details=raw_details,
    )


def new_ui_coordination_lock(runtime: ApplicationRuntime) -> RootLock:
    ui_lock_path = runtime.paths.lock_file.with_name(f"{runtime.paths.root_key}.ui.lock")
    return RootLock(
        ui_lock_path,
        timeout_seconds=runtime.config.request_timeout_seconds,
    )


def _bridge_error_from_outcome(outcome: InvocationOutcome, *, operation: str) -> BridgeError:
    error = outcome.error or {}
    raw_code = error.get("code")
    try:
        code = ErrorCode(raw_code) if isinstance(raw_code, str) else ErrorCode.PROTOCOL_ERROR
    except ValueError:
        code = ErrorCode.PROTOCOL_ERROR
    message = error.get("message")
    details = error.get("details")
    return BridgeError(
        code,
        message if isinstance(message, str) else f"CMO {operation} failed",
        details if isinstance(details, dict) else {"operation": operation},
    )


def _diagnostic_error(
    *,
    store: BridgeConfigStore,
    error: BridgeError,
    bridge_version: str,
    runtime_tag: str,
) -> McpBridgeDiagnostic:
    unconfigured = error.code is ErrorCode.GAME_ROOT_INVALID
    action = (
        "Call cmo_bridge_prepare with the Command: Modern Operations game_root."
        if unconfigured
        else "Repair the reported host configuration error, then call cmo_bridge_diagnose again."
    )
    return McpBridgeDiagnostic(
        runtime_state="unconfigured" if unconfigured else "error",
        ready=False,
        bridge_version=bridge_version,
        runtime_tag=runtime_tag,
        config_path=str(store.config_path),
        game_root=None,
        dispatcher_path=None,
        inbox_path=None,
        poll_path=None,
        lua_action=POLL_ACTION_SCRIPT,
        error_code=error.code.value,
        error_message=error.message,
        required_next_action=action,
    )


def diagnose_bridge(
    *,
    game_root: Path | None = None,
    local_app_data: Path | None = None,
) -> McpBridgeDiagnostic:
    """Inspect host-side readiness without contacting CMO or changing bridge files."""
    store = BridgeConfigStore(local_app_data=local_app_data)
    snapshot = create_runtime_snapshot()
    try:
        config = store.load(game_root_override=game_root)
    except BridgeError as error:
        return _diagnostic_error(
            store=store,
            error=error,
            bridge_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
        )
    if config.game_root is None:
        return _diagnostic_error(
            store=store,
            error=BridgeError(
                ErrorCode.GAME_ROOT_INVALID,
                "a CMO game root has not been configured",
            ),
            bridge_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
        )

    paths = FileBridgePaths.build(config.game_root, store.local_app_data)
    dispatcher_path = paths.lua_root / "versions" / snapshot.runtime_tag / "dispatcher.lua"
    poll_path = paths.lua_root / "poll.lua"
    try:
        dispatcher_ready = (
            dispatcher_path.is_file()
            and dispatcher_path.read_bytes() == render_dispatcher(snapshot)
        )
        inbox_ready = paths.inbox.parent.is_dir()
    except OSError as error:
        return McpBridgeDiagnostic(
            runtime_state="error",
            ready=False,
            bridge_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
            config_path=str(store.config_path),
            game_root=str(paths.game_root),
            dispatcher_path=str(dispatcher_path),
            inbox_path=str(paths.inbox),
            poll_path=str(poll_path),
            lua_action=POLL_ACTION_SCRIPT,
            error_code=ErrorCode.BRIDGE_NOT_PREPARED.value,
            error_message=f"bridge runtime files cannot be inspected: {error}",
            required_next_action="Call cmo_bridge_prepare to redeploy the local Lua runtime.",
        )

    if not dispatcher_ready or not inbox_ready:
        return McpBridgeDiagnostic(
            runtime_state="not_prepared",
            ready=False,
            bridge_version=snapshot.runtime_version,
            runtime_tag=snapshot.runtime_tag,
            config_path=str(store.config_path),
            game_root=str(paths.game_root),
            dispatcher_path=str(dispatcher_path),
            inbox_path=str(paths.inbox),
            poll_path=str(poll_path),
            lua_action=POLL_ACTION_SCRIPT,
            error_code=ErrorCode.BRIDGE_NOT_PREPARED.value,
            error_message="the release-bound CMO Lua runtime is missing or does not match",
            required_next_action=(
                "Call cmo_bridge_prepare, then mount or repair the CMO polling event."
            ),
        )

    return McpBridgeDiagnostic(
        runtime_state="ready",
        ready=True,
        bridge_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        config_path=str(store.config_path),
        game_root=str(paths.game_root),
        dispatcher_path=str(dispatcher_path),
        inbox_path=str(paths.inbox),
        poll_path=str(poll_path),
        lua_action=POLL_ACTION_SCRIPT,
        error_code=None,
        error_message=None,
        required_next_action=(
            "Call cmo_bridge_status. If CMO does not respond, mount or repair the polling event "
            "using lua_action."
        ),
    )


def _prepare_result(prepared: PreparedBridge) -> McpBridgePrepareResult:
    return McpBridgePrepareResult(
        ready=True,
        bridge_version=prepared.runtime_snapshot.runtime_version,
        runtime_tag=prepared.runtime_snapshot.runtime_tag,
        game_root=str(prepared.paths.game_root),
        dispatcher_path=str(prepared.dispatcher_path),
        inbox_path=str(prepared.paths.inbox),
        poll_path=str(prepared.poll_path),
        lua_action=POLL_ACTION_SCRIPT,
        next_step=(
            "Mount lua_action on a repeatable CMO event with a Regular Time trigger, then call "
            "cmo_bridge_status."
        ),
    )


def _runtime_result(runtime: ApplicationRuntime) -> McpBridgePrepareResult:
    dispatcher_path = (
        runtime.paths.lua_root
        / "versions"
        / runtime.runtime_snapshot.runtime_tag
        / "dispatcher.lua"
    )
    return McpBridgePrepareResult(
        ready=True,
        bridge_version=runtime.runtime_snapshot.runtime_version,
        runtime_tag=runtime.runtime_snapshot.runtime_tag,
        game_root=str(runtime.paths.game_root),
        dispatcher_path=str(dispatcher_path),
        inbox_path=str(runtime.paths.inbox),
        poll_path=str(runtime.paths.lua_root / "poll.lua"),
        lua_action=POLL_ACTION_SCRIPT,
        next_step=(
            "Mount lua_action on a repeatable CMO event with a Regular Time trigger, then call "
            "cmo_bridge_status."
        ),
    )


def _normalized_identifier(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().strip("{}").casefold()


def _scenario_context_identity(scenario: ScenarioResult) -> tuple[str | bool | None, ...]:
    return (
        _normalized_identifier(scenario.guid),
        scenario.title,
        scenario.file_name.casefold(),
        scenario.file_name_path.casefold(),
        _normalized_identifier(scenario.player_side_guid),
        scenario.started,
    )


def _scenario_file_name(scenario: ScenarioResult) -> str | None:
    if not scenario.file_name:
        return None
    reported = Path(scenario.file_name_path) if scenario.file_name_path else None
    if reported is not None and reported.suffix.lower() in {".scen", ".save"}:
        return str(reported)
    return str(reported / scenario.file_name) if reported is not None else scenario.file_name


def _scenario_context_failure_status(reason: str) -> ScenarioContextStatus:
    lowered = reason.casefold()
    if "saved scenario file name" in lowered:
        return "unsaved_scenario"
    if "player side" in lowered and ("exactly one" in lowered or "not found" in lowered):
        return "side_not_found"
    if any(
        marker in lowered
        for marker in (
            "command.exe",
            "assembly",
            "deserialize",
            "unsupported root",
            "method",
            "type",
        )
    ):
        return "assembly_incompatible"
    if "could not be resolved" in lowered or "not a file" in lowered or "was not found" in lowered:
        return "file_missing"
    return "reader_failed"


def _unavailable_scenario_context(
    *,
    scenario: ScenarioResult,
    status: ScenarioContextStatus,
    reason: str,
) -> ScenarioContextResult:
    return ScenarioContextResult(
        status=status,
        scenario_guid=scenario.guid,
        title=scenario.title,
        player_side_guid=scenario.player_side_guid,
        player_side_name=None,
        scenario_file=_scenario_file_name(scenario),
        scenario_description=None,
        side_briefing=None,
        scoring_thresholds=None,
        description_truncated=False,
        briefing_truncated=False,
        saved_snapshot=False,
        warnings=[reason],
    )


def _available_scenario_context(
    *,
    scenario: ScenarioResult,
    context: ScenarioContext,
    status: ScenarioContextStatus,
    warnings: list[str],
) -> ScenarioContextResult:
    scoring = context.scoring
    return ScenarioContextResult(
        status=status,
        scenario_guid=scenario.guid,
        title=scenario.title,
        player_side_guid=scenario.player_side_guid,
        player_side_name=context.player_side_name,
        scenario_file=_scenario_file_name(scenario),
        scenario_description=context.scenario_description,
        side_briefing=context.side_briefing,
        scoring_thresholds=(
            ScenarioScoringThresholds(
                major_defeat=scoring.major_defeat,
                minor_defeat=scoring.minor_defeat,
                average=scoring.average,
                minor_victory=scoring.minor_victory,
                major_victory=scoring.major_victory,
            )
            if scoring is not None
            else None
        ),
        description_truncated=context.description_truncated,
        briefing_truncated=context.briefing_truncated,
        saved_snapshot=True,
        warnings=warnings,
    )


class McpRuntimeManager:
    """Keep a stdio MCP server alive while its release-bound CMO runtime is prepared."""

    def __init__(
        self,
        *,
        game_root: Path | None = None,
        local_app_data: Path | None = None,
        scenario_context_reader: ScenarioContextReaderPort | None = None,
        ui_time_controller: UiTimeControllerPort | None = None,
    ) -> None:
        self._game_root = game_root
        self._local_app_data = local_app_data
        self._runtime: ApplicationRuntime | None = None
        self._lock = asyncio.Lock()
        self._ui_gate = asyncio.Lock()
        self._queue_lifecycle_started = False
        self._scenario_context_reader = scenario_context_reader or ScenarioContextReader()
        self._ui_time_controller = ui_time_controller

    async def _ensure_runtime(self) -> ApplicationRuntime:
        async with self._lock:
            if self._runtime is None:
                self._runtime = build_application_runtime(
                    game_root=self._game_root,
                    local_app_data=self._local_app_data,
                )
                if self._ui_time_controller is None:
                    self._ui_time_controller = UiTimeController(self._runtime.paths.command_exe)
            runtime = self._runtime
            if self._queue_lifecycle_started:
                runtime.queue_worker.start()
            return runtime

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        try:
            runtime = await self._ensure_runtime()
            await self._fail_fast_when_cmo_is_paused(runtime, operation)
            return await runtime.application.execute(
                operation,
                arguments,
                confirmation_token=confirmation_token,
            )
        except BridgeError as error:
            return _failure_outcome(error)

    async def _fail_fast_when_cmo_is_paused(
        self,
        runtime: ApplicationRuntime,
        operation: str,
    ) -> None:
        """Avoid publishing Lua-backed synchronous work that cannot poll while paused."""

        contract = OPERATION_REGISTRY.resolve(operation)
        if contract.target is not ExecutionTarget.CMO:
            return

        observed: UiTimeState | None = None
        try:
            async with self._ui_gate:
                async with self._new_ui_lock(runtime):
                    observed = await self._require_ui_controller().get_state()
        except BridgeError:
            # UI Automation is an advisory preflight. If it is unavailable, preserve the
            # existing bridge path so a UI limitation cannot disable otherwise valid reads.
            return

        if observed is None or observed.state is not SimulationRunState.PAUSED:
            return
        raise BridgeError(
            ErrorCode.SCENARIO_NOT_ADVANCING,
            "CMO is paused; the Lua-backed operation was not published or retried",
            {
                "operation": operation,
                "observed_state": observed.state.value,
                "rate_code": observed.rate.value,
                "requires_lua_poll": True,
                "retry_suppressed": True,
                "next_tool": "cmo_time_get_state",
                "next_step": (
                    "Do not retry while CMO is paused. If fresh state is required, preserve "
                    "the current rate, open an explicit 1x read window with cmo_time_set, "
                    "complete the planned read batch, and restore pause in cleanup."
                ),
            },
        )

    async def submit(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
    ) -> QueuedOperationReceipt:
        runtime = await self._ensure_runtime()
        async with self._ui_gate:
            async with self._new_ui_lock(runtime):
                async with self._lock:
                    receipt = runtime.queue_service.submit(
                        operation=operation,
                        arguments=arguments,
                    )
                    if self._queue_lifecycle_started:
                        runtime.queue_worker.wake()
                    return receipt
        raise RuntimeError("UI coordination lock unexpectedly suppressed queue submission")

    async def queue_get(self, request_id: UUID) -> QueuedOperationStatus:
        runtime = await self._ensure_runtime()
        return runtime.queue_service.get(request_id=request_id)

    async def queue_wait(
        self,
        request_id: UUID,
        timeout_seconds: float,
    ) -> QueueWaitResult:
        runtime = await self._ensure_runtime()
        return await runtime.queue_service.wait(
            request_id=request_id,
            timeout_seconds=timeout_seconds,
        )

    async def queue_list(self, limit: int | None = None) -> QueuedOperationList:
        runtime = await self._ensure_runtime()
        return runtime.queue_service.list(limit=limit)

    async def queue_cancel(self, request_id: UUID) -> CancelQueuedOperationResult:
        runtime = await self._ensure_runtime()
        async with self._lock:
            result = runtime.queue_service.cancel(request_id=request_id)
            if self._queue_lifecycle_started:
                runtime.queue_worker.wake()
            return result

    async def queue_summary(self) -> QueueSummary:
        runtime = await self._ensure_runtime()
        return runtime.queue_service.summary()

    async def time_get_state(self) -> McpTimeState:
        runtime = await self._ensure_runtime()
        async with self._ui_gate:
            async with self._new_ui_lock(runtime):
                return _time_state(await self._require_ui_controller().get_state())
        raise RuntimeError("UI coordination lock unexpectedly suppressed state read")

    async def time_set(
        self,
        *,
        state: Literal["paused", "running"] | SimulationRunState,
        rate_code: int | None = None,
    ) -> McpTimeSetResult:
        runtime = await self._ensure_runtime()
        target_state = self._validated_run_state(state)
        target_rate = self._validated_rate(rate_code)
        async with self._ui_gate:
            async with self._new_ui_lock(runtime):
                controller = self._require_ui_controller()
                before_raw = await controller.get_state()
                current = before_raw
                if target_state is SimulationRunState.PAUSED:
                    if current.state is SimulationRunState.RUNNING:
                        current = await controller.pause()
                    if target_rate is not None and current.rate is not target_rate:
                        current = await controller.set_rate(target_rate)
                else:
                    if current.state is SimulationRunState.PAUSED:
                        current = await controller.resume(target_rate)
                    elif target_rate is not None and current.rate is not target_rate:
                        current = await controller.set_rate(target_rate)
                after_raw = await controller.get_state()
                if after_raw.state is not target_state or (
                    target_rate is not None and after_raw.rate is not target_rate
                ):
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "CMO UI time control did not reach the requested state",
                        {
                            "requested_state": target_state.value,
                            "requested_rate_code": (
                                None if target_rate is None else target_rate.value
                            ),
                            "observed_state": after_raw.state.value,
                            "observed_rate_code": after_raw.rate.value,
                        },
                    )
                before = _time_state(before_raw)
                after = _time_state(after_raw)
                return McpTimeSetResult(
                    before=before,
                    after=after,
                    changed=(before.state, before.rate_code) != (after.state, after.rate_code),
                )
        raise RuntimeError("UI coordination lock unexpectedly suppressed state change")

    async def simulation_pulse(
        self,
        *,
        request_ids: tuple[UUID, ...] = (),
        handshake: bool = False,
        accept_lineage_id: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> McpSimulationPulseResult:
        runtime = await self._ensure_runtime()
        ids, timeout = self._validated_pulse_arguments(
            request_ids=request_ids,
            handshake=handshake,
            accept_lineage_id=accept_lineage_id,
            timeout_seconds=timeout_seconds,
        )
        started_at = time.monotonic()
        async with self._ui_gate:
            async with self._new_ui_lock(runtime):
                controller = self._require_ui_controller()
                before_raw = await controller.get_state()
                if before_raw.state is not SimulationRunState.PAUSED:
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "simulation pulse requires CMO to be paused already",
                        {
                            "observed_state": before_raw.state.value,
                            "next_step": (
                                "Use ordinary bridge calls at the current speed, or explicitly "
                                "pause with cmo_time_set only when planning requires it."
                            ),
                        },
                    )

                initial_statuses = self._pulse_queue_preflight(runtime, ids)
                pending = tuple(
                    sorted(
                        (
                            item
                            for item in initial_statuses
                            if item.state not in _TERMINAL_QUEUE_STATES
                        ),
                        key=lambda item: item.sequence,
                    )
                )
                if not pending and not handshake:
                    before = _time_state(before_raw)
                    return McpSimulationPulseResult(
                        ok=True,
                        released=False,
                        before=before,
                        after=before,
                        requests=initial_statuses,
                        handshake=None,
                        timed_out=False,
                        final_pause_verified=True,
                        prior_rate_restored=True,
                        work_error=None,
                        pause_error=None,
                        rate_restore_error=None,
                        elapsed_seconds=time.monotonic() - started_at,
                    )

                if pending and self._queue_lifecycle_started:
                    runtime.queue_worker.wake()
                    await self._wait_until_first_request_armed(
                        runtime,
                        pending[0].request_id,
                        timeout_seconds=min(timeout, 1.0),
                    )

                released = False
                release_attempted = False
                work_task: (
                    asyncio.Task[
                        tuple[tuple[QueuedOperationStatus, ...], BridgeStatusResult | None]
                    ]
                    | None
                ) = None
                requests = initial_statuses
                bridge_status: BridgeStatusResult | None = None
                timed_out = False
                work_error: McpTimeControlError | None = None
                pause_error: McpTimeControlError | None = None
                restore_error: McpTimeControlError | None = None
                final_raw: UiTimeState | None = None
                unexpected: BaseException | None = None
                cancellation: asyncio.CancelledError | None = None
                try:
                    release_attempted = True
                    start_task = asyncio.create_task(controller.play_1x())
                    try:
                        running = await asyncio.shield(start_task)
                    except asyncio.CancelledError as error:
                        cancellation = error
                        running = await start_task
                    if (
                        running.state is not SimulationRunState.RUNNING
                        or running.rate is not TimeRate.X1
                    ):
                        raise BridgeError(
                            ErrorCode.STATE_CONFLICT,
                            "CMO did not enter verified 1x running state for the pulse",
                        )
                    released = True
                    if cancellation is None:
                        work_task = asyncio.create_task(
                            self._run_pulse_work(
                                runtime,
                                request_ids=ids,
                                handshake=handshake,
                                accept_lineage_id=accept_lineage_id,
                            )
                        )
                        try:
                            done, _pending_tasks = await asyncio.wait(
                                {work_task},
                                timeout=timeout,
                            )
                        except asyncio.CancelledError as error:
                            cancellation = error
                        else:
                            if not done:
                                timed_out = True
                            else:
                                try:
                                    requests, bridge_status = work_task.result()
                                except BridgeError as error:
                                    work_error = _time_control_error(error)
                                except BaseException as error:
                                    unexpected = error
                finally:
                    if release_attempted:
                        cleanup_task = asyncio.create_task(
                            self._pause_and_restore(controller, before_raw.rate)
                        )
                        try:
                            final_raw, pause_error, restore_error = await asyncio.shield(
                                cleanup_task
                            )
                        except asyncio.CancelledError as error:
                            if cancellation is None:
                                cancellation = error
                            final_raw, pause_error, restore_error = await cleanup_task
                    if work_task is not None and not work_task.done():
                        work_task.cancel()
                        try:
                            await work_task
                        except asyncio.CancelledError:
                            pass
                        except BridgeError as error:
                            if work_error is None:
                                work_error = _time_control_error(error)
                        except BaseException as error:
                            if unexpected is None:
                                unexpected = error
                    requests = tuple(runtime.queue_service.get(request_id=item) for item in ids)

                if cancellation is not None:
                    raise cancellation
                if unexpected is not None:
                    raise unexpected
                final_pause_verified = (
                    final_raw is not None and final_raw.state is SimulationRunState.PAUSED
                )
                prior_rate_restored = (
                    final_pause_verified
                    and final_raw is not None
                    and final_raw.rate is before_raw.rate
                    and restore_error is None
                )
                ok = (
                    not timed_out
                    and work_error is None
                    and pause_error is None
                    and restore_error is None
                    and final_pause_verified
                    and all(item.state in _TERMINAL_QUEUE_STATES for item in requests)
                    and (not handshake or bridge_status is not None)
                )
                return McpSimulationPulseResult(
                    ok=ok,
                    released=released,
                    before=_time_state(before_raw),
                    after=None if final_raw is None else _time_state(final_raw),
                    requests=requests,
                    handshake=bridge_status,
                    timed_out=timed_out,
                    final_pause_verified=final_pause_verified,
                    prior_rate_restored=prior_rate_restored,
                    work_error=work_error,
                    pause_error=pause_error,
                    rate_restore_error=restore_error,
                    elapsed_seconds=time.monotonic() - started_at,
                )
        raise RuntimeError("UI coordination lock unexpectedly suppressed simulation pulse")

    async def scenario_context_get(self) -> ScenarioContextResult:
        runtime = await self._ensure_runtime()
        before = await self._live_scenario(runtime)
        player_side_guid = before.player_side_guid
        if player_side_guid is None:
            return _unavailable_scenario_context(
                scenario=before,
                status="no_player_side",
                reason="CMO did not report a current player side GUID",
            )

        context = await self._scenario_context_reader.read(
            game_root=runtime.paths.game_root,
            file_name_path=before.file_name_path,
            file_name=before.file_name,
            player_side_guid=player_side_guid,
        )
        after = await self._live_scenario(runtime)
        if _scenario_context_identity(before) != _scenario_context_identity(after):
            return _unavailable_scenario_context(
                scenario=after,
                status="scenario_changed",
                reason="the loaded scenario or current player side changed while reading briefing",
            )
        if not context.available:
            reason = context.unavailable_reason or "the saved scenario context is unavailable"
            return _unavailable_scenario_context(
                scenario=after,
                status=_scenario_context_failure_status(reason),
                reason=reason,
            )

        warnings = list(context.warnings)
        if not after.started:
            warnings.append(
                "The scenario is not started; unsaved editor changes are not visible in this "
                "saved-file snapshot."
            )
        partial = bool(warnings or context.description_truncated or context.briefing_truncated)
        return _available_scenario_context(
            scenario=after,
            context=context,
            status="partial" if partial else "available",
            warnings=warnings,
        )

    async def _live_scenario(self, runtime: ApplicationRuntime) -> ScenarioResult:
        await self._fail_fast_when_cmo_is_paused(runtime, "scenario.get")
        outcome = await runtime.application.execute("scenario.get", {})
        if not outcome.ok:
            error = outcome.error or {}
            raw_code = error.get("code")
            try:
                code = (
                    ErrorCode(raw_code) if isinstance(raw_code, str) else ErrorCode.PROTOCOL_ERROR
                )
            except ValueError:
                code = ErrorCode.PROTOCOL_ERROR
            message = error.get("message")
            details = error.get("details")
            raise BridgeError(
                code,
                message if isinstance(message, str) else "CMO scenario read failed",
                details if isinstance(details, dict) else None,
            )
        try:
            return ScenarioResult.model_validate(outcome.result)
        except ValidationError as error:
            raise BridgeError(
                ErrorCode.PROTOCOL_ERROR,
                "CMO returned an invalid scenario result",
                {"operation": "scenario.get"},
            ) from error

    async def start_queue_worker(self) -> None:
        async with self._lock:
            self._queue_lifecycle_started = True
            if self._runtime is not None:
                self._runtime.queue_worker.start()

    async def stop_queue_worker(self) -> None:
        async with self._lock:
            self._queue_lifecycle_started = False
            if self._runtime is not None:
                # Keep lifecycle transitions serialized until the old task is
                # fully detached. A concurrent start can then reliably create
                # a fresh worker instead of racing the old task's cleanup.
                await self._runtime.queue_worker.stop()

    async def diagnose(self) -> McpBridgeDiagnostic:
        async with self._lock:
            selected_root = (
                self._runtime.paths.game_root if self._runtime is not None else self._game_root
            )
            return diagnose_bridge(
                game_root=selected_root,
                local_app_data=self._local_app_data,
            )

    async def prepare(
        self,
        *,
        game_root: str | None = None,
        replace_saved_game_root: bool = False,
    ) -> McpBridgePrepareResult:
        async with self._lock:
            selected_root = self._select_prepare_root(game_root)
            if self._runtime is not None:
                active_root = self._runtime.paths.game_root
                requested_root = FileBridgePaths.build(
                    selected_root,
                    BridgeConfigStore(local_app_data=self._local_app_data).local_app_data,
                ).game_root
                if requested_root != active_root:
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "an active MCP server cannot switch to a different CMO game root",
                        {
                            "active_root": str(active_root),
                            "requested_root": str(requested_root),
                            "next_step": "restart the MCP server with the requested game root",
                        },
                    )
                return _runtime_result(self._runtime)

            prepared = await asyncio.to_thread(
                prepare_bridge,
                game_root=selected_root,
                local_app_data=self._local_app_data,
                replace_saved_game_root=replace_saved_game_root,
            )
            candidate = build_application_runtime(
                game_root=prepared.paths.game_root,
                local_app_data=self._local_app_data,
            )
            self._runtime = candidate
            self._game_root = candidate.paths.game_root
            if self._ui_time_controller is None:
                self._ui_time_controller = UiTimeController(candidate.paths.command_exe)
            if self._queue_lifecycle_started:
                candidate.queue_worker.start()
            return _prepare_result(prepared)

    def _require_ui_controller(self) -> UiTimeControllerPort:
        controller = self._ui_time_controller
        if controller is None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "CMO UI time controller is unavailable before runtime preparation",
            )
        return controller

    @staticmethod
    def _new_ui_lock(runtime: ApplicationRuntime) -> RootLock:
        return new_ui_coordination_lock(runtime)

    @staticmethod
    def _validated_run_state(value: object) -> SimulationRunState:
        try:
            return SimulationRunState(value)
        except (TypeError, ValueError) as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "time state must be 'paused' or 'running'",
            ) from error

    @staticmethod
    def _validated_rate(value: object) -> TimeRate | None:
        if value is None:
            return None
        if type(value) is not int:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "time compression rate_code must be an integer from 0 through 5",
            )
        try:
            return TimeRate(value)
        except ValueError as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "time compression rate_code must be an integer from 0 through 5",
            ) from error

    @staticmethod
    def _validated_pulse_arguments(
        *,
        request_ids: tuple[UUID, ...],
        handshake: bool,
        accept_lineage_id: str | None,
        timeout_seconds: float,
    ) -> tuple[tuple[UUID, ...], float]:
        if type(request_ids) is not tuple or any(type(item) is not UUID for item in request_ids):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "simulation pulse request_ids must be a tuple of UUID values",
            )
        if len(set(request_ids)) != len(request_ids):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "simulation pulse request_ids must not contain duplicates",
            )
        if type(handshake) is not bool:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT, "simulation pulse handshake must be boolean"
            )
        if not request_ids and not handshake:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "simulation pulse requires at least one request_id or handshake=true",
            )
        if not handshake and accept_lineage_id is not None:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "accept_lineage_id is valid only when handshake=true",
            )
        if accept_lineage_id is not None and (
            type(accept_lineage_id) is not str or not accept_lineage_id.strip()
        ):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "accept_lineage_id must be a non-empty string when provided",
            )
        if type(timeout_seconds) not in {int, float} or isinstance(timeout_seconds, bool):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "simulation pulse timeout_seconds must be a finite positive number",
            )
        timeout = float(timeout_seconds)
        if not math.isfinite(timeout) or timeout <= 0 or timeout > 120:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "simulation pulse timeout_seconds must be greater than zero and at most 120",
            )
        return request_ids, timeout

    @staticmethod
    def _pulse_queue_preflight(
        runtime: ApplicationRuntime,
        request_ids: tuple[UUID, ...],
    ) -> tuple[QueuedOperationStatus, ...]:
        selected = tuple(runtime.queue_service.get(request_id=item) for item in request_ids)
        selected_ids = frozenset(request_ids)
        unselected = tuple(
            item
            for item in runtime.queue_service.list_nonterminal().items
            if item.request_id not in selected_ids
        )
        if unselected:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "simulation pulse would also advance unselected durable requests",
                {
                    "unselected_request_ids": [str(item.request_id) for item in unselected],
                    "next_step": (
                        "Include every non-terminal FIFO request in request_ids, or wait until the "
                        "queue is otherwise settled."
                    ),
                },
            )
        return selected

    @staticmethod
    async def _wait_until_first_request_armed(
        runtime: ApplicationRuntime,
        request_id: UUID,
        *,
        timeout_seconds: float,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        while True:
            status = runtime.queue_service.get(request_id=request_id)
            if status.state is OperationQueueState.ACTIVE or status.state in _TERMINAL_QUEUE_STATES:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "durable request was not armed before simulation release",
                    {"request_id": str(request_id)},
                )
            await asyncio.sleep(min(0.05, remaining))

    async def _run_pulse_work(
        self,
        runtime: ApplicationRuntime,
        *,
        request_ids: tuple[UUID, ...],
        handshake: bool,
        accept_lineage_id: str | None,
    ) -> tuple[tuple[QueuedOperationStatus, ...], BridgeStatusResult | None]:
        while True:
            requests = tuple(runtime.queue_service.get(request_id=item) for item in request_ids)
            if all(item.state in _TERMINAL_QUEUE_STATES for item in requests):
                break
            await asyncio.sleep(0.05)
        if not handshake:
            return requests, None
        outcome = await runtime.application.execute(
            "bridge.status",
            {"accept_lineage_id": accept_lineage_id},
        )
        if not outcome.ok:
            raise _bridge_error_from_outcome(outcome, operation="bridge.status")
        try:
            status = BridgeStatusResult.model_validate(outcome.result)
        except ValidationError as error:
            raise BridgeError(
                ErrorCode.PROTOCOL_ERROR,
                "CMO returned an invalid bridge status during simulation pulse",
            ) from error
        return requests, status

    @staticmethod
    async def _pause_and_restore(
        controller: UiTimeControllerPort,
        prior_rate: TimeRate,
    ) -> tuple[
        UiTimeState | None,
        McpTimeControlError | None,
        McpTimeControlError | None,
    ]:
        try:
            paused = await controller.pause()
            if paused.state is not SimulationRunState.PAUSED:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "CMO UI did not verify the final pulse pause",
                    {"observed_state": paused.state.value},
                )
        except BridgeError as error:
            return None, _time_control_error(error), None
        if paused.rate is prior_rate:
            return paused, None, None
        try:
            restored = await controller.set_rate(prior_rate)
            if restored.state is not SimulationRunState.PAUSED or restored.rate is not prior_rate:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "CMO UI did not restore the pre-pulse time compression while paused",
                    {
                        "observed_state": restored.state.value,
                        "observed_rate_code": restored.rate.value,
                        "expected_rate_code": prior_rate.value,
                    },
                )
            return restored, None, None
        except BridgeError as error:
            return paused, None, _time_control_error(error)

    def _select_prepare_root(self, value: str | None) -> Path:
        if value is not None:
            return Path(value)
        if self._game_root is not None:
            return self._game_root
        store = BridgeConfigStore(local_app_data=self._local_app_data)
        config = store.load()
        if config.game_root is None:
            raise BridgeError(
                ErrorCode.GAME_ROOT_INVALID,
                "a CMO game root is required",
                {"next_step": "pass game_root to cmo_bridge_prepare"},
            )
        return config.game_root
