from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, overload
from uuid import uuid4

from cmo_agent_bridge.application.confirmation import ConfirmationTokenStore
from cmo_agent_bridge.application.host_quarantine import (
    HostQuarantineResolutionService,
)
from cmo_agent_bridge.application.queue_service import QueueService
from cmo_agent_bridge.application.queue_worker import QueueWorker
from cmo_agent_bridge.application.ports import (
    LocalOperationArguments,
    LocalOperationName,
    LocalOperationResult,
)
from cmo_agent_bridge.application.service import BridgeApplication
from cmo_agent_bridge.application.session_service import SessionScope, SessionService
from cmo_agent_bridge.config import BridgeConfig, BridgeConfigStore
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import (
    BridgeDoctorArgs,
    BridgePrepareArgs,
    BridgeStatusResult,
    BridgeUninstallArgs,
    CompatProbeArgs,
    CompatProbeResult,
    DoctorResult,
    PrepareResult,
    UninstallResult,
)
from cmo_agent_bridge.operations.registry import (
    OPERATION_REGISTRY,
    FrozenInvocation,
    OperationContract,
)
from cmo_agent_bridge.protocol.lua_delivery import render_idle_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.runtime_bundle import create_runtime_snapshot, render_dispatcher
from cmo_agent_bridge.state.session_store import SessionStore
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueState,
    OperationQueueStore,
)
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.atomic_io import atomic_replace_bytes
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import PsutilCmoProcessInspector
from cmo_agent_bridge.transports.file_bridge.transport import FileBridgeTransport


POLL_ACTION_SCRIPT = "return ScenEdit_RunScript('CMOAgentBridge/inbox/request.lua')"
_POLL_FILE = (POLL_ACTION_SCRIPT + "\n").encode("ascii")


@dataclass(frozen=True, slots=True)
class PreparedBridge:
    config: BridgeConfig
    paths: FileBridgePaths
    runtime_snapshot: RuntimeSnapshot
    dispatcher_path: Path
    poll_path: Path


@dataclass(frozen=True, slots=True)
class ApplicationRuntime:
    application: BridgeApplication
    host_quarantine: HostQuarantineResolutionService
    queue_service: QueueService
    queue_worker: QueueWorker
    config: BridgeConfig
    paths: FileBridgePaths
    runtime_snapshot: RuntimeSnapshot


class SystemWallClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


class TrustedLocalPolicy:
    """Small policy for the trusted same-machine bridge."""

    def __init__(self, *, allow_mutations: bool, allow_destructive: bool) -> None:
        self._allow_mutations = allow_mutations
        self._allow_destructive = allow_destructive

    def ensure_allowed(
        self,
        *,
        status: BridgeStatusResult,
        invocation: FrozenInvocation,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        del status, runtime_snapshot
        if invocation.effective_class in {OperationClass.STATUS, OperationClass.READ}:
            return
        if invocation.effective_class is OperationClass.MUTATION and self._allow_mutations:
            return
        if invocation.effective_class is OperationClass.MUTATION:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "CMO mutation operations are disabled by local configuration",
                {"operation": invocation.contract.name, "setting": "allow_mutations"},
            )
        raise BridgeError(
            ErrorCode.POLICY_DENIED,
            "destructive CMO operations are not enabled in the trusted-local runtime",
            {"operation": invocation.contract.name},
        )

    def ensure_destructive_allowed(
        self,
        *,
        status: BridgeStatusResult,
        contract: OperationContract,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        del status, runtime_snapshot
        if not self._allow_mutations:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "CMO mutation operations are disabled by local configuration",
                {"operation": contract.name, "setting": "allow_mutations"},
            )
        if not self._allow_destructive:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "destructive CMO operations are disabled by local configuration",
                {"operation": contract.name, "setting": "allow_destructive"},
            )


class UnavailableLocalOperations:
    @overload
    async def execute(
        self,
        operation: Literal["bridge.prepare"],
        arguments: BridgePrepareArgs,
    ) -> PrepareResult: ...

    @overload
    async def execute(
        self,
        operation: Literal["bridge.doctor"],
        arguments: BridgeDoctorArgs,
    ) -> DoctorResult: ...

    @overload
    async def execute(
        self,
        operation: Literal["bridge.uninstall"],
        arguments: BridgeUninstallArgs,
    ) -> UninstallResult: ...

    async def execute(
        self,
        operation: LocalOperationName,
        arguments: LocalOperationArguments,
    ) -> LocalOperationResult:
        del arguments
        raise BridgeError(
            ErrorCode.POLICY_DENIED,
            "local management operations are available as CLI commands, not invoke operations",
            {"operation": operation},
        )


class UnavailableCompatibilityProbe:
    async def execute(self, arguments: CompatProbeArgs) -> CompatProbeResult:
        del arguments
        raise BridgeError(
            ErrorCode.POLICY_DENIED,
            "compatibility probing is not enabled in the initial bridge runtime",
        )


def _loaded_config(
    *,
    game_root: Path | None,
    local_app_data: Path | None,
) -> tuple[BridgeConfigStore, BridgeConfig, FileBridgePaths]:
    store = BridgeConfigStore(local_app_data=local_app_data)
    config = store.load(game_root_override=game_root)
    if config.game_root is None:
        raise BridgeError(
            ErrorCode.GAME_ROOT_INVALID,
            "a CMO game root is required; pass --game-root or run prepare first",
        )
    paths = FileBridgePaths.build(config.game_root, store.local_app_data)
    return store, config, paths


def _dispatcher_path(paths: FileBridgePaths, snapshot: RuntimeSnapshot) -> Path:
    return paths.lua_root / "versions" / snapshot.runtime_tag / "dispatcher.lua"


def prepare_bridge(
    *,
    game_root: Path,
    local_app_data: Path | None = None,
    replace_saved_game_root: bool = False,
) -> PreparedBridge:
    return asyncio.run(
        _prepare_bridge_locked(
            game_root=game_root,
            local_app_data=local_app_data,
            replace_saved_game_root=replace_saved_game_root,
        )
    )


async def _prepare_bridge_locked(
    *,
    game_root: Path,
    local_app_data: Path | None,
    replace_saved_game_root: bool,
) -> PreparedBridge:
    store, config, paths = _loaded_config(
        game_root=game_root,
        local_app_data=local_app_data,
    )
    snapshot = create_runtime_snapshot()
    dispatcher_path = _dispatcher_path(paths, snapshot)
    poll_path = paths.lua_root / "poll.lua"
    root_lock = RootLock(paths.lock_file, timeout_seconds=config.request_timeout_seconds)
    saved = config

    async with root_lock:
        database = StateDatabase(paths.sqlite_file)
        database.initialize()
        queue_store = OperationQueueStore(database)
        nonterminal = queue_store.list(
            root_key=paths.root_key,
            states=frozenset({OperationQueueState.QUEUED, OperationQueueState.ACTIVE}),
        )
        if paths.pending_file.exists() or nonterminal:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "bridge preparation is blocked by unfinished CMO work",
                {
                    "pending_journal": paths.pending_file.exists(),
                    "nonterminal_queue_requests": len(nonterminal),
                    "next_step": (
                        "let the current bridge finish, or cancel still-queued requests; "
                        "resolve any active/quarantined request before upgrading or preparing"
                    ),
                },
            )

        dispatcher_path.parent.mkdir(parents=True, exist_ok=True)
        paths.inbox.parent.mkdir(parents=True, exist_ok=True)
        atomic_replace_bytes(
            dispatcher_path,
            render_dispatcher(snapshot),
            retry_seconds=config.replace_retry_seconds,
        )
        atomic_replace_bytes(
            paths.inbox,
            render_idle_lua(),
            retry_seconds=config.replace_retry_seconds,
        )
        atomic_replace_bytes(
            poll_path,
            _POLL_FILE,
            retry_seconds=config.replace_retry_seconds,
        )
        saved = store.save(
            config,
            replace_saved_game_root=replace_saved_game_root,
        )
    return PreparedBridge(
        config=saved,
        paths=paths,
        runtime_snapshot=snapshot,
        dispatcher_path=dispatcher_path,
        poll_path=poll_path,
    )


def build_application_runtime(
    *,
    game_root: Path | None = None,
    local_app_data: Path | None = None,
) -> ApplicationRuntime:
    _store, config, paths = _loaded_config(
        game_root=game_root,
        local_app_data=local_app_data,
    )
    snapshot = create_runtime_snapshot()
    dispatcher_path = _dispatcher_path(paths, snapshot)
    if not dispatcher_path.is_file() or not paths.inbox.parent.is_dir():
        raise BridgeError(
            ErrorCode.BRIDGE_NOT_PREPARED,
            "the CMO-side Lua runtime is not prepared for this bridge release",
            {
                "dispatcher_path": str(dispatcher_path),
                "inbox_path": str(paths.inbox),
            },
        )
    if dispatcher_path.read_bytes() != render_dispatcher(snapshot):
        raise BridgeError(
            ErrorCode.BRIDGE_NOT_PREPARED,
            "the installed CMO-side Lua runtime does not match this bridge release",
            {"dispatcher_path": str(dispatcher_path)},
        )

    database = StateDatabase(paths.sqlite_file)
    database.initialize()
    catalog = ManifestCatalog(ReleaseBinding(snapshot=snapshot, registry=OPERATION_REGISTRY))
    root_lock = RootLock(
        paths.lock_file,
        timeout_seconds=config.request_timeout_seconds,
    )
    transport = FileBridgeTransport(
        paths=paths,
        root_lock=root_lock,
        process_inspector=PsutilCmoProcessInspector(),
        catalog=catalog,
        database=database,
        max_journal_bytes=1_048_576,
        replace_retry_seconds=config.replace_retry_seconds,
        response_poll_seconds=0.05,
        cancel_ack_timeout_seconds=config.cancel_ack_timeout_seconds,
    )
    wall_clock = SystemWallClock()
    session_store = SessionStore(database)
    sessions = SessionService(
        scope=SessionScope(root_key=paths.root_key, command_exe=paths.command_exe),
        session_store=session_store,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=snapshot,
        wall_clock=wall_clock,
        uuid4_source=uuid4,
        status_timeout_seconds=config.request_timeout_seconds,
    )
    confirmations = ConfirmationTokenStore(database)
    application = BridgeApplication(
        registry=OPERATION_REGISTRY,
        transport=transport,
        sessions=sessions,
        policy=TrustedLocalPolicy(
            allow_mutations=config.allow_mutations,
            allow_destructive=config.allow_destructive,
        ),
        confirmations=confirmations,
        wall_clock=wall_clock,
        local_operations=UnavailableLocalOperations(),
        compatibility_probe=UnavailableCompatibilityProbe(),
        request_timeout_seconds=config.request_timeout_seconds,
    )
    queue_store = OperationQueueStore(database)
    queue_service = QueueService(
        root_key=paths.root_key,
        registry=OPERATION_REGISTRY,
        runtime_snapshot=snapshot,
        session_store=session_store,
        queue_store=queue_store,
        allow_mutations=config.allow_mutations,
    )
    queue_worker = QueueWorker(
        root_key=paths.root_key,
        registry=OPERATION_REGISTRY,
        queue_store=queue_store,
        transport=transport,
        ledger=transport.ledger,
    )
    return ApplicationRuntime(
        application=application,
        host_quarantine=HostQuarantineResolutionService(
            transport=transport,
            sessions=sessions,
            confirmations=confirmations,
            wall_clock=wall_clock,
        ),
        queue_service=queue_service,
        queue_worker=queue_worker,
        config=config,
        paths=paths,
        runtime_snapshot=snapshot,
    )
