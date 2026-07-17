from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter

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
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot, render_dispatcher
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


_ERROR_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(
    dict[str, JsonValue],
    config=ConfigDict(strict=True),
)
RuntimeState = Literal["ready", "unconfigured", "not_prepared", "error"]


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


def _failure_outcome(error: BridgeError) -> InvocationOutcome:
    payload = _ERROR_ADAPTER.validate_python(error.to_payload())
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=None,
        ok=False,
        result=None,
        error=payload,
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


class McpRuntimeManager:
    """Keep a stdio MCP server alive while its release-bound CMO runtime is prepared."""

    def __init__(
        self,
        *,
        game_root: Path | None = None,
        local_app_data: Path | None = None,
    ) -> None:
        self._game_root = game_root
        self._local_app_data = local_app_data
        self._runtime: ApplicationRuntime | None = None
        self._lock = asyncio.Lock()
        self._queue_lifecycle_started = False

    async def _ensure_runtime(self) -> ApplicationRuntime:
        async with self._lock:
            if self._runtime is None:
                self._runtime = build_application_runtime(
                    game_root=self._game_root,
                    local_app_data=self._local_app_data,
                )
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
            return await runtime.application.execute(
                operation,
                arguments,
                confirmation_token=confirmation_token,
            )
        except BridgeError as error:
            return _failure_outcome(error)

    async def submit(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
    ) -> QueuedOperationReceipt:
        runtime = await self._ensure_runtime()
        async with self._lock:
            receipt = runtime.queue_service.submit(operation=operation, arguments=arguments)
            if self._queue_lifecycle_started:
                runtime.queue_worker.wake()
            return receipt

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
            if self._queue_lifecycle_started:
                candidate.queue_worker.start()
            return _prepare_result(prepared)

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
