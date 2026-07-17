from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import time
import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import cast

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.models import BridgeStatusWireArgs
from cmo_agent_bridge.operations.registry import FrozenInvocation, ResolvedInvocation
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua
from cmo_agent_bridge.protocol.manifest import ManifestCatalog
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    ExchangeCommand,
    PreparedDelivery,
    RequestBody,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import (
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.models import DeliveryIntent, HostRequestState
from cmo_agent_bridge.state.pending_journal import PendingJournalStore
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.cleanup import (
    PinnedResponseCleanup,
    pin_terminal_response_cleanup,
)
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    BridgeChannel,
    DurableBridgeChannel,
    RecoveryReport as _RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.mutation import MutationExchange
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
    require_single_instance,
)
from cmo_agent_bridge.transports.file_bridge.recovery import RecoveryManager
from cmo_agent_bridge.transports.file_bridge.response_waiter import ResponseWaiter


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _state_conflict(message: str, details: dict[str, object] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message, details)


def _validated_number(
    value: object,
    *,
    description: str,
    allow_zero: bool,
) -> float:
    if type(value) not in {int, float}:
        raise _invalid_argument(f"{description} must be an exact int or float")
    try:
        validated = float(cast(int | float, value))
    except OverflowError as error:
        raise _invalid_argument(f"{description} must be finite") from error
    if not math.isfinite(validated) or validated < 0 or (validated == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "strictly positive"
        raise _invalid_argument(f"{description} must be finite and {qualifier}")
    return validated


def _has_synchronous_process_method(value: object) -> bool:
    try:
        method = cast(object, inspect.getattr_static(value, "matching_processes"))
    except AttributeError:
        return False
    if isinstance(method, (classmethod, staticmethod)):
        method = cast(object, method.__func__)  # pyright: ignore[reportUnknownMemberType]
    return callable(method) and not inspect.iscoroutinefunction(method)


def _valid_process(value: object) -> bool:
    return (
        type(value) is ProcessInfo
        and type(value.pid) is int
        and value.pid > 0
        and type(value.create_time) is float
        and math.isfinite(value.create_time)
        and value.create_time > 0
        and isinstance(cast(object, value.executable), Path)
    )


def _process_details(value: ProcessInfo) -> dict[str, object]:
    return {
        "pid": value.pid,
        "create_time": value.create_time,
        "executable": str(value.executable),
    }


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _canonical_json_text(value: object) -> str:
    return _canonical_json_bytes(value).decode("utf-8")


def _same_frozen_projection(
    frozen: FrozenInvocation,
    resolved: ResolvedInvocation,
) -> bool:
    return (
        frozen.contract == resolved.contract
        and frozen.wire_arguments == resolved.wire_arguments
        and frozen.effective_class is resolved.effective_class
        and frozen.result_schema == resolved.result_schema
        and frozen.recovery_schema == resolved.recovery_schema
    )


@dataclass(frozen=True, slots=True)
class _ValidatedCommand:
    request_id: uuid.UUID
    body: RequestBody
    body_bytes: bytes
    invocation: ResolvedInvocation
    runtime_snapshot: RuntimeSnapshot
    timeout: float


class _EpochSequence:
    def __init__(self) -> None:
        self._last = 0

    def next(self, *, minimum: int = 0) -> int:
        observed = time.time_ns() // 1_000_000
        value = max(observed, self._last, minimum, 0)
        self._last = value
        return value


@dataclass(slots=True)
class _PublicationAttempt:
    delivery: PreparedDelivery
    intent: DeliveryIntent
    published_at_ms: int
    entered: bool = False
    returned: bool = False
    marker_committed: bool = False


def _new_attempts() -> list[_PublicationAttempt]:
    return []


def _new_allowed_deliveries() -> list[AllowedDelivery]:
    return []


@dataclass(slots=True)
class _ExchangeState:
    validated: _ValidatedCommand
    response_path: Path
    epochs: _EpochSequence
    recovery_schema_id: str | None
    request_record: RequestRecord | None = None
    attempts: list[_PublicationAttempt] = field(default_factory=_new_attempts)
    current_attempt: _PublicationAttempt | None = None
    allowed_deliveries: list[AllowedDelivery] = field(default_factory=_new_allowed_deliveries)
    any_publication_returned: bool = False
    artifact: ResponseArtifact | None = None
    response_accepted_at_ms: int | None = None
    idle_at_ms: int | None = None
    terminal_at_ms: int | None = None
    idle_replaced: bool = False
    idle_failed: bool = False
    terminal_transition_failed: bool = False
    cleanup_queued: bool = False
    safety_finished: bool = False
    primary: BaseException | None = None
    deferred_safety_failure: BaseException | None = None


def _classify_exchange_error(error: BaseException) -> BaseException:
    if isinstance(error, BridgeError) or not isinstance(error, Exception):
        return error
    wrapped = _state_conflict(
        "file-bridge read exchange failed",
        {"type": type(error).__name__},
    )
    wrapped.__cause__ = error
    return wrapped


def _rejection_payload(error: BaseException) -> dict[str, object]:
    if isinstance(error, BridgeError):
        return cast(dict[str, object], error.to_payload())
    return {
        "code": ErrorCode.STATE_CONFLICT.value,
        "details": {"reason": "exchange_cancelled"},
        "message": "file-bridge read exchange was cancelled",
    }


def _quarantine_json(reason: str) -> str:
    return _canonical_json_text(
        {
            "code": ErrorCode.STATE_CONFLICT.value,
            "details": {"reason": reason},
            "message": "file-bridge read exchange requires recovery",
        }
    )


def _invalid_command(message: str, error: BaseException | None = None) -> BridgeError:
    result = _invalid_argument(message)
    if error is not None:
        result.__cause__ = error
    return result


def _validate_command(
    command: object,
    catalog: ManifestCatalog,
) -> _ValidatedCommand:
    try:
        if type(command) is not ExchangeCommand:
            raise TypeError("command must be an exact ExchangeCommand")
        trusted_command = command
        if type(trusted_command.request_id) is not uuid.UUID:
            raise TypeError("request ID must be an exact UUID")
        if trusted_command.request_id.version != 4:
            raise ValueError("request ID must be a canonical UUIDv4")
        if type(trusted_command.body) is not RequestBody:
            raise TypeError("body must be an exact RequestBody")
        if type(trusted_command.invocation) is not ResolvedInvocation:
            raise TypeError("invocation must be an exact ResolvedInvocation")
        if type(trusted_command.unbounded_wait) is not bool:
            raise TypeError("unbounded wait flag must be an exact bool")
        snapshot = revalidate_runtime_snapshot(trusted_command.runtime_snapshot)
        if trusted_command.unbounded_wait:
            raise ValueError("an unbounded timeout is reserved for durable mutation workers")
        timeout = _validated_number(
            trusted_command.timeout,
            description="exchange timeout",
            allow_zero=True,
        )

        body_tree = trusted_command.body.model_dump(
            mode="python",
            round_trip=True,
            warnings=False,
        )
        body = RequestBody.model_validate(body_tree)
        body_bytes = canonical_body_bytes(body)
        if body_bytes != canonical_body_bytes(trusted_command.body):
            raise ValueError("request body does not survive defensive reconstruction")
        invocation = trusted_command.invocation
        if invocation.contract.target is not ExecutionTarget.CMO:
            raise ValueError("exchange invocation must target CMO")
        if invocation.effective_class not in {OperationClass.STATUS, OperationClass.READ}:
            raise ValueError("H8 accepts only effective status or read operations")
        if body.operation != invocation.contract.name:
            raise ValueError("request body operation differs from invocation")
        wire_arguments = invocation.wire_arguments.model_dump(mode="json")
        if _canonical_json_bytes(body.arguments) != _canonical_json_bytes(wire_arguments):
            raise ValueError("request body arguments differ from typed wire arguments")
        identity = {
            "protocol": snapshot.protocol,
            "release_id": snapshot.release_id,
            "runtime_version": snapshot.runtime_version,
            "runtime_tag": snapshot.runtime_tag,
            "runtime_asset_sha256": snapshot.runtime_asset_sha256,
            "operation_manifest_sha256": snapshot.operation_manifest_sha256,
        }
        if any(getattr(body, field) != value for field, value in identity.items()):
            raise ValueError("request body runtime identity differs from snapshot")
        has_lineage = body.expected_lineage_id is not None
        has_activation = body.expected_activation_id is not None
        if has_lineage != has_activation:
            raise ValueError("request context must contain both lineage and activation or neither")
        if invocation.effective_class is OperationClass.STATUS:
            if body.operation != "bridge.status":
                raise ValueError("the status class is reserved for bridge.status")
            if type(invocation.wire_arguments) is not BridgeStatusWireArgs:
                raise TypeError("bridge.status requires exact BridgeStatusWireArgs")
        elif not has_lineage:
            raise ValueError("read operations require lineage and activation")

        binding = catalog.resolve_running(snapshot.release_id)
        if binding.snapshot != snapshot:
            raise ValueError("catalog running snapshot differs from command snapshot")
        public_arguments = invocation.public_arguments.model_dump(
            mode="json",
            exclude_unset=True,
        )
        trusted_enrichment = None
        if body.operation == "bridge.status":
            status_arguments = cast(BridgeStatusWireArgs, invocation.wire_arguments)
            trusted_enrichment = {"activation_candidate": status_arguments.activation_candidate}
        rebuilt = binding.registry.resolve_invocation(
            body.operation,
            public_arguments,
            trusted_enrichment,
        )
        if rebuilt != invocation:
            raise ValueError("resolved invocation does not rebuild exactly")
        frozen = binding.registry.resolve_wire_invocation(body.operation, body.arguments)
        if not _same_frozen_projection(frozen, rebuilt):
            raise ValueError("wire invocation projection differs from resolved invocation")
        return _ValidatedCommand(
            request_id=trusted_command.request_id,
            body=body,
            body_bytes=body_bytes,
            invocation=rebuilt,
            runtime_snapshot=snapshot,
            timeout=timeout,
        )
    except BridgeError as error:
        raise _invalid_command("invalid file-bridge exchange command", error) from error
    except (
        AttributeError,
        ValidationError,
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise _invalid_command("invalid file-bridge exchange command", error) from error


def _fresh_uuid4() -> uuid.UUID:
    delivery_id = uuid.uuid4()
    if type(delivery_id) is not uuid.UUID or delivery_id.version != 4:
        raise _state_conflict("UUID generator returned a non-UUIDv4 delivery ID")
    return delivery_id


def _observe_single_process(
    inspector: CmoProcessInspector,
    command_exe: Path,
) -> ProcessInfo:
    try:
        observed = require_single_instance(inspector, command_exe)
    except BridgeError:
        raise
    except Exception as error:
        raise _state_conflict(
            "CMO process inspection failed",
            {"type": type(error).__name__},
        ) from error
    if not _valid_process(observed):
        raise _state_conflict(
            "CMO process inspection returned invalid process identity",
            {"type": type(observed).__name__},
        )
    return observed


def _attach_secondary(primary: BaseException, secondary: BaseException, label: str) -> None:
    primary.add_note(f"{label}: {type(secondary).__name__}: {secondary}")


def _merge_primary(
    primary: BaseException | None,
    candidate: BaseException | None,
    label: str,
) -> BaseException | None:
    if candidate is None:
        return primary
    if primary is None:
        return candidate
    _attach_secondary(primary, candidate, label)
    return primary


async def _await_protected_task(
    task: asyncio.Task[object],
    *,
    baseline_cancellations: int,
    expected_task_cancellation: bool,
) -> tuple[BaseException | None, asyncio.CancelledError | None]:
    task_failure: BaseException | None = None
    exit_cancellation: asyncio.CancelledError | None = None
    current = asyncio.current_task()
    if current is None:
        raise _state_conflict("session exit requires an active asyncio task")

    while True:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if current.cancelling() > baseline_cancellations:
                while current.cancelling() > baseline_cancellations:
                    current.uncancel()
                if exit_cancellation is None:
                    exit_cancellation = error
                else:
                    _attach_secondary(
                        exit_cancellation,
                        error,
                        "additional session-exit cancellation",
                    )
                continue
            if task.done() and task.cancelled():
                if not expected_task_cancellation:
                    task_failure = error
                break
            continue
        except BaseException as error:
            task_failure = error
            break
        else:
            break
    return task_failure, exit_cancellation


async def _await_startup_recovery_task(
    task: asyncio.Task[_RecoveryReport],
    *,
    baseline_cancellations: int,
) -> tuple[_RecoveryReport, asyncio.CancelledError | None]:
    current = asyncio.current_task()
    if current is None:
        raise _state_conflict("startup recovery requires an active asyncio task")
    primary_cancellation: asyncio.CancelledError | None = None
    worker_failure: BaseException | None = None

    while True:
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if current.cancelling() > baseline_cancellations:
                while current.cancelling() > baseline_cancellations:
                    current.uncancel()
                if primary_cancellation is None:
                    primary_cancellation = error
                else:
                    _attach_secondary(
                        primary_cancellation,
                        error,
                        "additional cancellation during startup recovery",
                    )
                continue
            if task.done() and task.cancelled():
                worker_failure = error
                break
            continue
        except BaseException as error:
            worker_failure = error
            break
        else:
            break

    if worker_failure is not None:
        if primary_cancellation is not None:
            _attach_secondary(
                primary_cancellation,
                worker_failure,
                "startup recovery worker failure",
            )
            raise primary_cancellation
        raise worker_failure

    report = task.result()
    if type(report) is not _RecoveryReport:
        invalid_report = _state_conflict("startup recovery returned an invalid report")
        if primary_cancellation is not None:
            _attach_secondary(
                primary_cancellation,
                invalid_report,
                "startup recovery report failure",
            )
            raise primary_cancellation
        raise invalid_report
    return report, primary_cancellation


class FileBridgeTransport:
    __slots__ = (
        "_catalog",
        "_cancel_ack_timeout_seconds",
        "_database",
        "_inbox",
        "_journals",
        "_ledger",
        "_paths",
        "_process_inspector",
        "_response_poll_seconds",
        "_root_lock",
    )

    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        process_inspector: CmoProcessInspector,
        catalog: ManifestCatalog,
        database: StateDatabase,
        max_journal_bytes: int,
        replace_retry_seconds: float,
        response_poll_seconds: float = 0.05,
        cancel_ack_timeout_seconds: float = 10,
    ) -> None:
        if type(paths) is not FileBridgePaths:
            raise _invalid_argument("paths must be an exact FileBridgePaths")
        if type(root_lock) is not RootLock:
            raise _invalid_argument("root lock must be an exact RootLock")
        if type(catalog) is not ManifestCatalog:
            raise _invalid_argument("catalog must be an exact ManifestCatalog")
        if type(database) is not StateDatabase:
            raise _invalid_argument("database must be an exact StateDatabase")
        if root_lock.path != paths.lock_file:
            raise _invalid_argument("root lock does not belong to the file-bridge root")
        if database.path != paths.sqlite_file:
            raise _invalid_argument("database does not belong to the file-bridge root")
        if not _has_synchronous_process_method(process_inspector):
            raise _invalid_argument("process inspector must expose synchronous matching_processes")
        if type(max_journal_bytes) is not int or max_journal_bytes <= 0:
            raise _invalid_argument("maximum journal bytes must be an exact positive integer")
        replace_seconds = _validated_number(
            replace_retry_seconds,
            description="replace retry seconds",
            allow_zero=True,
        )
        poll_seconds = _validated_number(
            response_poll_seconds,
            description="response poll seconds",
            allow_zero=False,
        )
        cancel_timeout_seconds = _validated_number(
            cancel_ack_timeout_seconds,
            description="cancel acknowledgement timeout seconds",
            allow_zero=False,
        )

        self._paths = paths
        self._root_lock = root_lock
        self._process_inspector = process_inspector
        self._catalog = catalog
        self._database = database
        self._response_poll_seconds = poll_seconds
        self._cancel_ack_timeout_seconds = cancel_timeout_seconds
        self._journals = PendingJournalStore(
            paths,
            root_lock,
            catalog,
            max_journal_bytes=max_journal_bytes,
            replace_retry_seconds=replace_seconds,
        )
        self._ledger = RequestLedger(database, catalog)
        self._inbox = InboxPublisher(paths, replace_seconds)

    @property
    def paths(self) -> FileBridgePaths:
        return self._paths

    @property
    def root_key(self) -> Sha256:
        return self._paths.root_key

    @property
    def root_lock(self) -> RootLock:
        return self._root_lock

    @property
    def process_inspector(self) -> CmoProcessInspector:
        return self._process_inspector

    @property
    def catalog(self) -> ManifestCatalog:
        return self._catalog

    @property
    def database(self) -> StateDatabase:
        return self._database

    @property
    def journals(self) -> PendingJournalStore:
        return self._journals

    @property
    def ledger(self) -> RequestLedger:
        return self._ledger

    @property
    def inbox(self) -> InboxPublisher:
        return self._inbox

    def session(self) -> AbstractAsyncContextManager[BridgeChannel]:
        return _FileBridgeSession(self)

    def worker_session(
        self,
        *,
        recovery_owner: Callable[[ProcessInfo], uuid.UUID | None],
    ) -> AbstractAsyncContextManager[DurableBridgeChannel]:
        """Open the single durable-mutation worker lane.

        Unlike an ordinary session, startup recovery keeps an owned published
        mutation attached to its original inbox delivery until CMO responds.
        The queue owner is resolved after the root lock is held and before
        recovery can touch any persisted delivery.
        """

        if not callable(recovery_owner):
            raise _invalid_argument("durable worker recovery owner must be callable")
        return _FileBridgeSession(
            self,
            durable_worker=True,
            recovery_owner=recovery_owner,
        )


class _CleanupPaths(list[Path]):
    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        artifact_resolver: Callable[[Path], ResponseArtifact | None],
    ) -> None:
        super().__init__()
        self._paths = paths
        self._root_lock = root_lock
        self._artifact_resolver = artifact_resolver
        self._records: list[PinnedResponseCleanup | BaseException | None] = []

    def append(self, path: Path) -> None:
        super().append(path)
        try:
            artifact = self._artifact_resolver(path)
            record: PinnedResponseCleanup | BaseException | None = None
            if artifact is not None:
                record = pin_terminal_response_cleanup(
                    self._paths,
                    self._root_lock,
                    path,
                    artifact,
                )
        except BaseException as error:
            record = error
        self._records.append(record)

    def run(self) -> tuple[BaseException, ...]:
        failures: list[BaseException] = []
        for record in self._records:
            if isinstance(record, BaseException):
                failures.append(record)
            elif record is not None:
                try:
                    record.delete()
                except BaseException as error:
                    failures.append(error)
        super().clear()
        self._records.clear()
        return tuple(failures)


class _FileBridgeChannel:
    def __init__(
        self,
        transport: FileBridgeTransport,
        process: ProcessInfo,
        *,
        _defer_mutation_exchange: bool = False,
        _durable_worker: bool = False,
        _owned_request_id: uuid.UUID | None = None,
    ) -> None:
        self._transport = transport
        self._process = process
        self._active_task: asyncio.Task[object] | None = None
        self._closing = False
        self._closed = False
        self._poisoned = False
        if type(_durable_worker) is not bool:
            raise _invalid_argument("durable worker flag must be an exact bool")
        self._durable_worker = _durable_worker
        if _owned_request_id is not None and type(_owned_request_id) is not uuid.UUID:
            raise _invalid_argument("channel owned request ID must be exact")
        self._owned_request_id = _owned_request_id
        self._recovery_report: _RecoveryReport | None = None
        self._cleanup_paths = _CleanupPaths(
            paths=transport.paths,
            root_lock=transport.root_lock,
            artifact_resolver=self._artifact_for_cleanup,
        )
        self._exchange_state: _ExchangeState | None = None
        recovery_manager = RecoveryManager(
            paths=transport.paths,
            root_lock=transport.root_lock,
            process_inspector=transport.process_inspector,
            expected_process=process,
            journals=transport.journals,
            ledger=transport.ledger,
            inbox=transport.inbox,
            response_poll_seconds=transport._response_poll_seconds,  # pyright: ignore[reportPrivateUsage]
            cancel_ack_timeout_seconds=transport._cancel_ack_timeout_seconds,  # pyright: ignore[reportPrivateUsage]
        )
        self._recovery_manager = recovery_manager
        self._mutation_exchange: MutationExchange
        if not _defer_mutation_exchange:
            self._initialize_mutation_exchange()

    @property
    def process_identity(self) -> ProcessInfo:
        return self._process

    @property
    def recovery_report(self) -> _RecoveryReport:
        report = self._recovery_report
        if report is None:
            raise _state_conflict("file-bridge recovery has not completed")
        return report

    def _artifact_for_cleanup(self, path: Path) -> ResponseArtifact | None:
        read_state = self._exchange_state
        if read_state is not None and read_state.response_path == path:
            return read_state.artifact

        mutation = self.__dict__.get("_mutation_exchange")
        if isinstance(mutation, MutationExchange):
            mutation_state = mutation._current_state  # pyright: ignore[reportPrivateUsage]
            if mutation_state is not None and mutation_state.response_path == path:
                if mutation_state.response_artifact is not None:
                    return mutation_state.response_artifact

        prefix = "CMOAgentBridge_Response_"
        suffix = ".inst"
        name = path.name
        if not name.startswith(prefix) or not name.endswith(suffix):
            return None
        request_text = name[len(prefix) : -len(suffix)]
        try:
            request_id = uuid.UUID(request_text)
        except ValueError:
            return None
        if str(request_id) != request_text or request_id.version != 4:
            return None
        deliveries = self._transport.ledger.list_deliveries(request_id)
        artifacts = tuple(
            delivery.response_artifact
            for delivery in deliveries
            if delivery.response_artifact is not None
            and delivery.response_artifact.filename == name
        )
        return artifacts[0] if len(artifacts) == 1 else None

    def _initialize_mutation_exchange(self) -> None:
        if "_mutation_exchange" in self.__dict__:
            raise _state_conflict("file-bridge mutation exchange was already initialized")
        products: dict[str, object] = {
            "paths": self._transport.paths,
            "root_lock": self._transport.root_lock,
            "process_inspector": self._transport.process_inspector,
            "expected_process": self._process,
            "catalog": self._transport.catalog,
            "journals": self._transport.journals,
            "ledger": self._transport.ledger,
            "inbox": self._transport.inbox,
            "response_poll_seconds": self._transport._response_poll_seconds,  # pyright: ignore[reportPrivateUsage]
            "queue_response_cleanup": self._cleanup_paths.append,
            "recovery_manager": self._recovery_manager,
        }
        if self._durable_worker:
            products["durable_worker"] = True
        self._mutation_exchange = MutationExchange(
            **products,  # pyright: ignore[reportArgumentType]
        )

    async def recover_pending(self) -> _RecoveryReport:
        if self._closing or self._closed:
            raise _state_conflict("file-bridge channel is closed")
        if self._active_task is not None:
            raise _state_conflict("file-bridge channel already has an active exchange")
        self._transport.root_lock.require_acquired()
        current = asyncio.current_task()
        if current is None:
            raise _state_conflict("startup recovery requires an active asyncio task")
        owner = cast(asyncio.Task[object], current)
        self._active_task = owner
        try:
            if self._durable_worker:
                # Worker recovery may intentionally wait through an unlimited
                # CMO pause.  Its owner must still be able to shut down: a
                # cancellation detaches immediately and leaves the durable
                # journal/inbox delivery untouched for the next worker.
                owned_request_id = self._owned_request_id
                report = (
                    await self._recovery_manager.recover_pending()
                    if owned_request_id is None
                    else await self._recovery_manager.recover_owned_pending(owned_request_id)
                )
                cancellation: asyncio.CancelledError | None = None
            else:
                baseline_cancellations = current.cancelling()
                worker = asyncio.create_task(self._recovery_manager.recover_pending())
                report, cancellation = await _await_startup_recovery_task(
                    worker,
                    baseline_cancellations=baseline_cancellations,
                )
            try:
                if report.response_cleanup_required:
                    request_id = report.request_id
                    if request_id is None:
                        raise _state_conflict("startup recovery cleanup lacks its exact request ID")
                    self._cleanup_paths.append(self._transport.paths.response_path(request_id))
            except BaseException as cleanup_error:
                if cancellation is not None:
                    _attach_secondary(
                        cancellation,
                        cleanup_error,
                        "startup recovery cleanup queue failure",
                    )
                    raise cancellation
                raise
            if cancellation is not None:
                raise cancellation
            self._recovery_report = report
            return report
        finally:
            if self._active_task is owner:
                self._active_task = None

    async def exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        if self._closing or self._closed:
            raise _state_conflict("file-bridge channel is closed")
        if self._poisoned:
            raise _state_conflict("file-bridge channel requires a fresh session")
        if self._active_task is not None:
            raise _state_conflict("file-bridge channel already has an active exchange")
        task = asyncio.current_task()
        if task is None:
            raise _state_conflict("file-bridge exchange requires an active asyncio task")
        typed_task = cast(asyncio.Task[object], task)
        self._active_task = typed_task
        try:
            return await self._perform_exchange(command)
        finally:
            if self._active_task is typed_task:
                self._active_task = None

    async def _perform_exchange(self, command: ExchangeCommand) -> ResponseArtifact:
        if (
            type(command) is ExchangeCommand
            and type(command.invocation) is ResolvedInvocation
            and command.invocation.effective_class
            in {OperationClass.MUTATION, OperationClass.DESTRUCTIVE}
        ):
            try:
                return await self._mutation_exchange.run(command)
            finally:
                if self._mutation_exchange._requires_fresh_session():  # pyright: ignore[reportPrivateUsage]
                    self._poisoned = True
        if self._exchange_state is not None and not self._exchange_state.safety_finished:
            self._poisoned = True
            raise _state_conflict("file-bridge channel has unfinished exchange safety state")
        validated = _validate_command(command, self._transport.catalog)
        response_path = self._transport.paths.response_path(validated.request_id)
        if response_path.exists():
            raise _state_conflict(
                "response path already exists before request publication",
                {"response_path": str(response_path)},
            )
        if self._transport.ledger.get_request(validated.request_id) is not None:
            raise _state_conflict(
                "request ID already belongs to an existing live or durable exchange",
                {"request_id": str(validated.request_id)},
            )
        recovery_schema_id = (
            None
            if validated.invocation.recovery_schema is None
            else validated.invocation.recovery_schema.schema_id
        )
        state = _ExchangeState(
            validated=validated,
            response_path=response_path,
            epochs=_EpochSequence(),
            recovery_schema_id=recovery_schema_id,
        )
        self._exchange_state = state
        try:
            return await self._execute_exchange(state)
        except BaseException as error:
            primary = _classify_exchange_error(error)
            state.primary = primary
            safety_failure, safety_cancellation = await self._run_safety_protected(state)
            if safety_failure is not None:
                self._poisoned = True
                state.deferred_safety_failure = safety_failure
                _attach_secondary(primary, safety_failure, "exchange safety failure")
            if safety_cancellation is not None:
                if isinstance(primary, Exception):
                    _attach_secondary(
                        safety_cancellation,
                        primary,
                        "operation failure before safety cancellation",
                    )
                    primary = safety_cancellation
                else:
                    _attach_secondary(primary, safety_cancellation, "additional cancellation")
            raise primary

    async def _execute_exchange(self, state: _ExchangeState) -> ResponseArtifact:
        validated = state.validated
        first = self._new_attempt(state)
        intended_at_ms = first.intent.intended_at_ms
        request = RequestRecord(
            request_id=validated.request_id,
            root_key=self._transport.paths.root_key,
            request_hash=first.delivery.request_hash,
            operation=validated.body.operation,
            operation_class=validated.invocation.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=validated.runtime_snapshot,
            result_schema_id=validated.invocation.result_schema.schema_id,
            recovery_schema_id=state.recovery_schema_id,
            body_json=validated.body_bytes,
            lineage_id=validated.body.expected_lineage_id,
            activation_id=validated.body.expected_activation_id,
            result_json=None,
            error_json=None,
            resolution_json=None,
            created_at_ms=intended_at_ms,
            updated_at_ms=intended_at_ms,
            terminal_at_ms=None,
        )
        state.request_record = request
        self._transport.ledger.insert_prepared(request)
        self._transport.ledger.insert_delivery(first.intent)
        self._publish_attempt(state, first)
        self._transport.ledger.transition(
            validated.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.PUBLISHED,
            updated_at_ms=first.published_at_ms,
        )

        artifact: ResponseArtifact | None = None
        for waiter_attempt in range(2):
            try:
                artifact = await self._wait_for_response(
                    validated=validated,
                    response_path=state.response_path,
                    request_hash=first.delivery.request_hash,
                    published_deliveries=tuple(state.allowed_deliveries),
                )
            except BridgeError as error:
                if error.code is ErrorCode.REQUEST_TIMEOUT and waiter_attempt == 0:
                    retry = self._new_attempt(
                        state,
                        minimum_intended_at_ms=intended_at_ms + 1,
                    )
                    self._transport.ledger.insert_delivery(retry.intent)
                    self._publish_attempt(state, retry)
                    continue
                raise
            break
        if artifact is None:
            raise _state_conflict("response waiter returned no artifact")
        return self._record_and_finish_artifact(state, artifact)

    def _new_attempt(
        self,
        state: _ExchangeState,
        *,
        minimum_intended_at_ms: int = 0,
    ) -> _PublicationAttempt:
        delivery_id = _fresh_uuid4()
        prepared = prepare_delivery(
            state.validated.body,
            request_id=state.validated.request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
        )
        rendered = render_delivery_lua(prepared, state.validated.runtime_snapshot)
        intended_at_ms = state.epochs.next(minimum=minimum_intended_at_ms)
        published_at_ms = state.epochs.next(minimum=intended_at_ms)
        intent = self._delivery_intent(
            validated=state.validated,
            delivery_id=delivery_id,
            prepared_hash=prepared.request_hash,
            rendered=rendered,
            recovery_schema_id=state.recovery_schema_id,
            intended_at_ms=intended_at_ms,
            response_path=state.response_path,
        )
        attempt = _PublicationAttempt(
            delivery=prepared,
            intent=intent,
            published_at_ms=published_at_ms,
        )
        state.attempts.append(attempt)
        state.current_attempt = attempt
        return attempt

    def _publish_attempt(
        self,
        state: _ExchangeState,
        attempt: _PublicationAttempt,
    ) -> None:
        self._require_original_process_before_publication()
        attempt.entered = True
        self._transport.inbox.publish_delivery(
            attempt.delivery,
            runtime_snapshot=state.validated.runtime_snapshot,
        )
        attempt.returned = True
        state.any_publication_returned = True
        self._transport.ledger.mark_delivery_published(
            attempt.delivery.delivery_id,
            published_at_ms=attempt.published_at_ms,
        )
        attempt.marker_committed = True
        state.allowed_deliveries.append(
            AllowedDelivery(
                delivery_id=attempt.delivery.delivery_id,
                delivery_kind="request",
            )
        )

    def _record_and_finish_artifact(
        self,
        state: _ExchangeState,
        artifact: ResponseArtifact,
    ) -> ResponseArtifact:
        state.artifact = artifact
        recorded = self._transport.ledger.record_response(artifact)
        if recorded.delivery_id != artifact.accepted_response.envelope.delivery_id:
            raise _state_conflict("recorded response delivery differs from accepted artifact")
        last_publication_at_ms = max(
            attempt.published_at_ms for attempt in state.attempts if attempt.returned
        )
        response_at_ms = state.epochs.next(minimum=last_publication_at_ms)
        state.response_accepted_at_ms = response_at_ms
        self._transport.ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.PUBLISHED}),
            new_state=HostRequestState.RESPONSE_ACCEPTED,
            updated_at_ms=response_at_ms,
        )
        self._publish_idle(state)
        idle_at_ms = state.epochs.next(minimum=response_at_ms)
        state.idle_at_ms = idle_at_ms
        self._transport.ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.RESPONSE_ACCEPTED}),
            new_state=HostRequestState.IDLE_PUBLISHED,
            updated_at_ms=idle_at_ms,
        )
        self._transition_artifact_terminal(state, artifact, minimum_epoch=idle_at_ms)
        self._queue_cleanup(state)
        state.safety_finished = True
        return artifact

    def _publish_idle(self, state: _ExchangeState) -> None:
        try:
            self._transport.inbox.publish_idle()
        except BaseException:
            state.idle_failed = True
            raise
        state.idle_replaced = True

    def _transition_artifact_terminal(
        self,
        state: _ExchangeState,
        artifact: ResponseArtifact,
        *,
        minimum_epoch: int,
    ) -> RequestRecord:
        terminal_at_ms = state.terminal_at_ms
        if terminal_at_ms is None:
            terminal_at_ms = state.epochs.next(minimum=minimum_epoch)
            state.terminal_at_ms = terminal_at_ms
        terminal_state, result_json, error_json = self._artifact_terminal_fields(artifact)
        try:
            return self._transport.ledger.transition(
                state.validated.request_id,
                expected_states=frozenset({HostRequestState.IDLE_PUBLISHED}),
                new_state=terminal_state,
                updated_at_ms=terminal_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=result_json,
                error_json=error_json,
            )
        except BaseException:
            state.terminal_transition_failed = True
            raise

    @staticmethod
    def _artifact_terminal_fields(
        artifact: ResponseArtifact,
    ) -> tuple[HostRequestState, str | None, str | None]:
        accepted = artifact.accepted_response
        settlement = accepted.settlement
        if isinstance(settlement, CompletedSettlement):
            return (
                HostRequestState.COMPLETED,
                _canonical_json_text(settlement.result),
                None,
            )
        if isinstance(settlement, RejectedSettlement):
            return (
                HostRequestState.REJECTED,
                None,
                _canonical_json_text(settlement.error.model_dump(mode="json")),
            )
        if (
            settlement is None
            and accepted.envelope.error is not None
            and accepted.envelope.error.code is ErrorCode.MANIFEST_MISMATCH
        ):
            return (
                HostRequestState.REJECTED,
                None,
                _canonical_json_text(accepted.envelope.error.model_dump(mode="json")),
            )
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "accepted status/read response has no safe terminal settlement",
        )

    def _queue_cleanup(self, state: _ExchangeState) -> None:
        if not state.cleanup_queued:
            self._cleanup_paths.append(state.response_path)
            state.cleanup_queued = True

    @staticmethod
    def _delivery_intent(
        *,
        validated: _ValidatedCommand,
        delivery_id: uuid.UUID,
        prepared_hash: str,
        rendered: bytes,
        recovery_schema_id: str | None,
        intended_at_ms: int,
        response_path: Path,
    ) -> DeliveryIntent:
        return DeliveryIntent(
            request_id=validated.request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
            original_request_delivery_id=delivery_id,
            body_json=validated.body_bytes.decode("utf-8"),
            request_hash=prepared_hash,
            runtime_snapshot=validated.runtime_snapshot,
            result_schema_id=validated.invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema_id,
            intended_at_ms=intended_at_ms,
            published_at_ms=None,
            rendered_inbox_sha256=hashlib.sha256(rendered).hexdigest(),
            rendered_inbox_size_bytes=len(rendered),
            response_filename=response_path.name,
        )

    async def _wait_for_response(
        self,
        *,
        validated: _ValidatedCommand,
        response_path: Path,
        request_hash: str,
        published_deliveries: tuple[AllowedDelivery, ...],
    ) -> ResponseArtifact:
        expectation = ResponseExpectation(
            request_id=validated.request_id,
            allowed_deliveries=published_deliveries,
            request_hash=request_hash,
            expected_lineage_id=validated.body.expected_lineage_id,
            expected_activation_id=validated.body.expected_activation_id,
            status_bootstrap=(
                validated.body.operation == "bridge.status"
                and validated.body.expected_lineage_id is None
                and validated.body.expected_activation_id is None
            ),
            activation_candidate=(
                cast(
                    BridgeStatusWireArgs,
                    validated.invocation.wire_arguments,
                ).activation_candidate
                if validated.body.operation == "bridge.status"
                else None
            ),
            runtime_snapshot=validated.runtime_snapshot,
            invocation=validated.invocation,
        )
        waiter = ResponseWaiter(
            response_path=response_path,
            expectation=expectation,
            expected_process=self._process,
            process_check=lambda: require_single_instance(
                self._transport.process_inspector,
                self._transport.paths.command_exe,
            ),
            poll_seconds=self._transport._response_poll_seconds,  # pyright: ignore[reportPrivateUsage]
        )
        return await waiter.wait(validated.timeout)

    async def _run_safety_protected(
        self,
        state: _ExchangeState,
    ) -> tuple[BaseException | None, asyncio.CancelledError | None]:
        current = asyncio.current_task()
        if current is None:
            return _state_conflict("exchange safety requires an active asyncio task"), None
        while current.cancelling() > 0:
            current.uncancel()
        task = asyncio.create_task(self._finalize_state(state))
        failure: BaseException | None = None
        deferred_cancellation: asyncio.CancelledError | None = None
        while True:
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as error:
                if task.done() and task.cancelled():
                    failure = error
                    break
                while current.cancelling() > 0:
                    current.uncancel()
                if deferred_cancellation is None:
                    deferred_cancellation = error
                else:
                    _attach_secondary(
                        deferred_cancellation,
                        error,
                        "additional exchange-safety cancellation",
                    )
                continue
            except BaseException as error:
                failure = error
                break
            else:
                break
        return failure, deferred_cancellation

    async def _finalize_state(self, state: _ExchangeState) -> None:
        if state.safety_finished:
            return
        primary = state.primary
        if primary is None:
            raise _state_conflict("exchange safety has no classified primary failure")
        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            state.safety_finished = True
            return

        current = state.current_attempt
        ambiguous = current is not None and current.entered and not current.returned
        if not state.any_publication_returned and not ambiguous:
            self._reject_prepublication(state, request, primary)
            state.safety_finished = True
            return

        reason: str | None = "publication_outcome_unknown" if ambiguous else None
        if reason is None:
            reason = self._converge_publication_evidence(state)
        durable_artifact: ResponseArtifact | None = None
        if reason is None:
            durable_artifact, reason = self._converge_artifact_evidence(state)

        if reason is not None:
            if not state.idle_replaced and not state.idle_failed:
                try:
                    self._publish_idle(state)
                except BaseException as idle_error:
                    _attach_secondary(primary, idle_error, "idle publication failure")
            if state.idle_failed:
                reason = "idle_publication_failed"
            self._quarantine(state, reason)
            state.safety_finished = True
            self._poisoned = True
            return

        if state.idle_failed:
            self._quarantine(state, "idle_publication_failed")
            state.safety_finished = True
            self._poisoned = True
            return

        if durable_artifact is not None:
            accepted_record = self._ensure_response_accepted(state)
            if accepted_record is None:
                observed = self._transport.ledger.get_request(state.validated.request_id)
                if observed is not None and observed.state in {
                    HostRequestState.IDLE_PUBLISHED,
                    HostRequestState.COMPLETED,
                    HostRequestState.REJECTED,
                }:
                    if not state.idle_replaced and not state.idle_failed:
                        try:
                            self._publish_idle(state)
                        except BaseException as idle_error:
                            _attach_secondary(primary, idle_error, "idle publication failure")
                    self._poisoned = True
                    state.safety_finished = True
                    return
                if not state.idle_replaced:
                    try:
                        self._publish_idle(state)
                    except BaseException as idle_error:
                        _attach_secondary(primary, idle_error, "idle publication failure")
                quarantine_reason = (
                    "idle_publication_failed" if state.idle_failed else "artifact_evidence_drift"
                )
                self._quarantine(state, quarantine_reason)
                state.safety_finished = True
                self._poisoned = True
                return

        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            raise _state_conflict("request disappeared during exchange safety")
        if state.terminal_transition_failed:
            if self._terminal_matches_artifact(state, request, durable_artifact):
                self._queue_cleanup(state)
            else:
                self._poisoned = True
            state.safety_finished = True
            return

        if not state.idle_replaced:
            try:
                self._publish_idle(state)
            except BaseException as idle_error:
                _attach_secondary(primary, idle_error, "idle publication failure")
                self._quarantine(state, "idle_publication_failed")
                state.safety_finished = True
                self._poisoned = True
                return

        idle_record = self._ensure_idle_published(state, durable_artifact is not None)
        if idle_record is None:
            self._poisoned = True
            state.safety_finished = True
            return
        if durable_artifact is not None:
            try:
                self._transition_artifact_terminal(
                    state,
                    durable_artifact,
                    minimum_epoch=idle_record.updated_at_ms,
                )
            except BaseException as terminal_error:
                reread = self._transport.ledger.get_request(state.validated.request_id)
                if reread is None or not self._terminal_matches_artifact(
                    state,
                    reread,
                    durable_artifact,
                ):
                    self._poisoned = True
                    state.safety_finished = True
                    raise terminal_error
        else:
            terminal_at_ms = state.terminal_at_ms
            if terminal_at_ms is None:
                terminal_at_ms = state.epochs.next(minimum=idle_record.updated_at_ms)
                state.terminal_at_ms = terminal_at_ms
            try:
                self._transport.ledger.transition(
                    state.validated.request_id,
                    expected_states=frozenset({HostRequestState.IDLE_PUBLISHED}),
                    new_state=HostRequestState.REJECTED,
                    updated_at_ms=terminal_at_ms,
                    terminal_at_ms=terminal_at_ms,
                    error_json=_canonical_json_text(_rejection_payload(primary)),
                )
            except BaseException as terminal_error:
                reread = self._transport.ledger.get_request(state.validated.request_id)
                if (
                    reread is None
                    or reread.state is not HostRequestState.REJECTED
                    or reread.updated_at_ms != terminal_at_ms
                    or reread.terminal_at_ms != terminal_at_ms
                    or reread.error_json != _canonical_json_text(_rejection_payload(primary))
                ):
                    self._poisoned = True
                    state.safety_finished = True
                    raise terminal_error
        self._queue_cleanup(state)
        state.safety_finished = True

    def _reject_prepublication(
        self,
        state: _ExchangeState,
        request: RequestRecord,
        primary: BaseException,
    ) -> None:
        expected_error = _canonical_json_text(_rejection_payload(primary))
        if request.state is HostRequestState.REJECTED:
            if (
                state.terminal_at_ms is None
                or request.updated_at_ms != state.terminal_at_ms
                or request.terminal_at_ms != state.terminal_at_ms
                or request.error_json != expected_error
            ):
                raise _state_conflict("prepublication rejection evidence drifted")
            return
        if request.state is not HostRequestState.PREPARED:
            raise _state_conflict("prepublication request left the prepared state")
        terminal_at_ms = state.terminal_at_ms
        if terminal_at_ms is None:
            terminal_at_ms = state.epochs.next(minimum=request.updated_at_ms)
            state.terminal_at_ms = terminal_at_ms
        try:
            self._transport.ledger.transition(
                state.validated.request_id,
                expected_states=frozenset({HostRequestState.PREPARED}),
                new_state=HostRequestState.REJECTED,
                updated_at_ms=terminal_at_ms,
                terminal_at_ms=terminal_at_ms,
                error_json=expected_error,
            )
        except BaseException:
            reread = self._transport.ledger.get_request(state.validated.request_id)
            if (
                reread is None
                or reread.state is not HostRequestState.REJECTED
                or reread.updated_at_ms != terminal_at_ms
                or reread.terminal_at_ms != terminal_at_ms
                or reread.error_json != expected_error
            ):
                raise

    def _converge_publication_evidence(self, state: _ExchangeState) -> str | None:
        returned = [attempt for attempt in state.attempts if attempt.returned]
        for attempt in returned:
            try:
                record = self._transport.ledger.get_delivery(attempt.delivery.delivery_id)
            except BaseException:
                if (
                    state.artifact is not None
                    and attempt.delivery.delivery_id
                    == state.artifact.accepted_response.envelope.delivery_id
                ):
                    continue
                return "publication_evidence_drift"
            if record is None:
                return "publication_evidence_drift"
            if record.published_at_ms == attempt.published_at_ms:
                attempt.marker_committed = True
            elif record.published_at_ms is None:
                try:
                    self._transport.ledger.mark_delivery_published(
                        attempt.delivery.delivery_id,
                        published_at_ms=attempt.published_at_ms,
                    )
                except BaseException:
                    reread = self._transport.ledger.get_delivery(attempt.delivery.delivery_id)
                    if reread is None or reread.published_at_ms != attempt.published_at_ms:
                        return "publication_evidence_drift"
                attempt.marker_committed = True
            else:
                return "publication_evidence_drift"

        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            return "publication_evidence_drift"
        if request.state is HostRequestState.PREPARED:
            if not returned:
                return "publication_evidence_drift"
            published_at_ms = returned[0].published_at_ms
            try:
                self._transport.ledger.transition(
                    state.validated.request_id,
                    expected_states=frozenset({HostRequestState.PREPARED}),
                    new_state=HostRequestState.PUBLISHED,
                    updated_at_ms=published_at_ms,
                )
            except BaseException:
                reread = self._transport.ledger.get_request(state.validated.request_id)
                if reread is None or reread.state is not HostRequestState.PUBLISHED:
                    return "publication_evidence_drift"
        elif request.state is HostRequestState.PUBLISHED:
            if returned and request.updated_at_ms != returned[0].published_at_ms:
                return "publication_evidence_drift"
        elif request.state is HostRequestState.RESPONSE_ACCEPTED:
            if state.artifact is None:
                return "publication_evidence_drift"
        elif request.state is HostRequestState.IDLE_PUBLISHED:
            if state.artifact is None:
                return "publication_evidence_drift"
        elif request.state in {HostRequestState.COMPLETED, HostRequestState.REJECTED}:
            if state.artifact is None:
                return "publication_evidence_drift"
        else:
            return "publication_evidence_drift"
        return None

    def _converge_artifact_evidence(
        self,
        state: _ExchangeState,
    ) -> tuple[ResponseArtifact | None, str | None]:
        artifact = state.artifact
        if artifact is None:
            return None, None
        delivery_id = artifact.accepted_response.envelope.delivery_id
        try:
            record = self._transport.ledger.get_delivery(delivery_id)
        except BaseException:
            return None, "artifact_evidence_drift"
        if record is None:
            return None, "artifact_evidence_drift"
        if record.response_artifact == artifact:
            return artifact, None
        if record.response_artifact is not None or record.settlement is not None:
            return None, "artifact_evidence_drift"
        try:
            recorded = self._transport.ledger.record_response(artifact)
        except BaseException:
            reread = self._transport.ledger.get_delivery(delivery_id)
            if reread is not None and reread.response_artifact == artifact:
                return artifact, None
            if (
                reread is not None
                and reread.response_artifact is None
                and reread.settlement is None
            ):
                return None, None
            return None, "artifact_evidence_drift"
        if recorded.delivery_id != delivery_id or recorded.response_artifact != artifact:
            return None, "artifact_evidence_drift"
        return artifact, None

    def _ensure_response_accepted(self, state: _ExchangeState) -> RequestRecord | None:
        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            raise _state_conflict("artifact owner disappeared during safety")
        if request.state is HostRequestState.RESPONSE_ACCEPTED:
            if (
                state.response_accepted_at_ms is None
                or request.updated_at_ms != state.response_accepted_at_ms
            ):
                return None
            return request
        if request.state is HostRequestState.IDLE_PUBLISHED:
            if state.idle_at_ms is None or request.updated_at_ms != state.idle_at_ms:
                return None
            return request
        if request.state in {HostRequestState.COMPLETED, HostRequestState.REJECTED}:
            return (
                request if self._terminal_matches_artifact(state, request, state.artifact) else None
            )
        if request.state is not HostRequestState.PUBLISHED:
            raise _state_conflict("artifact owner state drifted before response acceptance")
        updated_at_ms = state.response_accepted_at_ms
        if updated_at_ms is None:
            updated_at_ms = state.epochs.next(minimum=request.updated_at_ms)
            state.response_accepted_at_ms = updated_at_ms
        try:
            return self._transport.ledger.transition(
                state.validated.request_id,
                expected_states=frozenset({HostRequestState.PUBLISHED}),
                new_state=HostRequestState.RESPONSE_ACCEPTED,
                updated_at_ms=updated_at_ms,
            )
        except BaseException:
            reread = self._transport.ledger.get_request(state.validated.request_id)
            if (
                reread is None
                or reread.state is not HostRequestState.RESPONSE_ACCEPTED
                or reread.updated_at_ms != updated_at_ms
            ):
                raise
            return reread

    def _ensure_idle_published(
        self,
        state: _ExchangeState,
        has_artifact: bool,
    ) -> RequestRecord | None:
        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            raise _state_conflict("request disappeared before idle transition")
        if request.state is HostRequestState.IDLE_PUBLISHED:
            if state.idle_at_ms is None or request.updated_at_ms != state.idle_at_ms:
                return None
            return request
        predecessor = (
            HostRequestState.RESPONSE_ACCEPTED if has_artifact else HostRequestState.PUBLISHED
        )
        if request.state is not predecessor:
            raise _state_conflict("request state drifted before idle transition")
        updated_at_ms = state.idle_at_ms
        if updated_at_ms is None:
            updated_at_ms = state.epochs.next(minimum=request.updated_at_ms)
            state.idle_at_ms = updated_at_ms
        try:
            return self._transport.ledger.transition(
                state.validated.request_id,
                expected_states=frozenset({predecessor}),
                new_state=HostRequestState.IDLE_PUBLISHED,
                updated_at_ms=updated_at_ms,
            )
        except BaseException:
            reread = self._transport.ledger.get_request(state.validated.request_id)
            if (
                reread is None
                or reread.state is not HostRequestState.IDLE_PUBLISHED
                or reread.updated_at_ms != updated_at_ms
            ):
                raise
            return reread

    def _terminal_matches_artifact(
        self,
        state: _ExchangeState,
        request: RequestRecord,
        artifact: ResponseArtifact | None,
    ) -> bool:
        if artifact is None:
            return False
        terminal_state, result_json, error_json = self._artifact_terminal_fields(artifact)
        return (
            request.state is terminal_state
            and state.terminal_at_ms is not None
            and request.updated_at_ms == state.terminal_at_ms
            and request.terminal_at_ms == state.terminal_at_ms
            and request.result_json == result_json
            and request.error_json == error_json
        )

    def _quarantine(self, state: _ExchangeState, reason: str) -> None:
        expected_error = _quarantine_json(reason)
        request = self._transport.ledger.get_request(state.validated.request_id)
        if request is None:
            raise _state_conflict("quarantine target disappeared")
        if request.state is HostRequestState.QUARANTINED:
            if request.error_json != expected_error or request.terminal_at_ms is not None:
                raise _state_conflict("quarantine evidence drifted")
            return
        allowed = {
            "publication_outcome_unknown": {
                HostRequestState.PREPARED,
                HostRequestState.PUBLISHED,
            },
            "publication_evidence_drift": {
                HostRequestState.PREPARED,
                HostRequestState.PUBLISHED,
            },
            "artifact_evidence_drift": {
                HostRequestState.PUBLISHED,
                HostRequestState.RESPONSE_ACCEPTED,
            },
            "idle_publication_failed": {
                HostRequestState.PREPARED,
                HostRequestState.PUBLISHED,
                HostRequestState.RESPONSE_ACCEPTED,
            },
        }[reason]
        if request.state not in allowed:
            raise _state_conflict("request state is outside the quarantine CAS boundary")
        updated_at_ms = state.epochs.next(minimum=request.updated_at_ms)
        try:
            self._transport.ledger.transition(
                state.validated.request_id,
                expected_states=frozenset({request.state}),
                new_state=HostRequestState.QUARANTINED,
                updated_at_ms=updated_at_ms,
                error_json=expected_error,
            )
        except BaseException:
            reread = self._transport.ledger.get_request(state.validated.request_id)
            if (
                reread is None
                or reread.state is not HostRequestState.QUARANTINED
                or reread.error_json != expected_error
                or reread.terminal_at_ms is not None
            ):
                raise

    def _require_original_process_before_publication(self) -> None:
        actual = _observe_single_process(
            self._transport.process_inspector,
            self._transport.paths.command_exe,
        )
        if actual != self._process:
            raise _state_conflict(
                "CMO process identity changed before request publication",
                {
                    "expected_process": _process_details(self._process),
                    "actual_process": _process_details(actual),
                },
            )

    def _begin_close(self) -> asyncio.Task[object] | None:
        self._closing = True
        return self._active_task

    async def _last_safety_attempt(self) -> None:
        state = self._exchange_state
        if state is None:
            return
        deferred = state.deferred_safety_failure
        if not state.safety_finished:
            try:
                await self._finalize_state(state)
            except BaseException as retry_failure:
                if deferred is None:
                    raise
                if retry_failure is not deferred:
                    _attach_secondary(
                        deferred,
                        retry_failure,
                        "session safety retry failure",
                    )
        if deferred is not None:
            raise deferred

    def _finish_close(self) -> None:
        self._closed = True
        self._active_task = None

    def _run_cleanup(self) -> tuple[BaseException, ...]:
        return self._cleanup_paths.run()


class _FileBridgeSession:
    def __init__(
        self,
        transport: FileBridgeTransport,
        *,
        durable_worker: bool = False,
        recovery_owner: Callable[[ProcessInfo], uuid.UUID | None] | None = None,
    ) -> None:
        self._transport = transport
        if type(durable_worker) is not bool:
            raise _invalid_argument("durable worker flag must be an exact bool")
        self._durable_worker = durable_worker
        if recovery_owner is not None and not callable(recovery_owner):
            raise _invalid_argument("session recovery owner must be callable")
        self._recovery_owner = recovery_owner
        self._channel: _FileBridgeChannel | None = None
        self._entered = False

    async def __aenter__(self) -> _FileBridgeChannel:
        if self._entered:
            raise _state_conflict("file-bridge session context manager is single-use")
        self._entered = True
        await self._transport.root_lock.__aenter__()
        channel: _FileBridgeChannel | None = None
        try:
            process = _observe_single_process(
                self._transport.process_inspector,
                self._transport.paths.command_exe,
            )
            recovery_owner = self._recovery_owner
            owned_request_id = (
                None if recovery_owner is None else recovery_owner(process)
            )
            if owned_request_id is not None and type(owned_request_id) is not uuid.UUID:
                raise _invalid_argument("resolved durable request owner must be exact")
            channel = _FileBridgeChannel(
                self._transport,
                process,
                _defer_mutation_exchange=True,
                _durable_worker=self._durable_worker,
                _owned_request_id=owned_request_id,
            )
            await channel.recover_pending()
            channel._initialize_mutation_exchange()  # pyright: ignore[reportPrivateUsage]
            self._channel = channel
            return channel
        except BaseException as error:
            if channel is not None:
                channel._finish_close()  # pyright: ignore[reportPrivateUsage]
            try:
                await self._transport.root_lock.__aexit__(
                    type(error),
                    error,
                    error.__traceback__,
                )
            except BaseException as lock_failure:
                _attach_secondary(error, lock_failure, "root lock cleanup failure")
            if channel is not None:
                for cleanup_failure in channel._run_cleanup():  # pyright: ignore[reportPrivateUsage]
                    _attach_secondary(error, cleanup_failure, "response cleanup failure")
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        del exc_type, traceback
        channel = self._channel
        if channel is None:
            return False

        current = asyncio.current_task()
        if current is None:
            raise _state_conflict("file-bridge session exit requires an active asyncio task")
        baseline_cancellations = current.cancelling()
        primary = exc
        active = channel._begin_close()  # pyright: ignore[reportPrivateUsage]
        child_failure: BaseException | None = None
        exit_cancellation: asyncio.CancelledError | None = None

        if active is not None and active is not current:
            cancel_requested = False
            if not active.done():
                active.cancel("file-bridge session is closing")
                cancel_requested = True
            child_failure, exit_cancellation = await _await_protected_task(
                active,
                baseline_cancellations=baseline_cancellations,
                expected_task_cancellation=cancel_requested,
            )

        safety_task = asyncio.create_task(channel._last_safety_attempt())  # pyright: ignore[reportPrivateUsage]
        safety_failure, safety_cancellation = await _await_protected_task(
            cast(asyncio.Task[object], safety_task),
            baseline_cancellations=baseline_cancellations,
            expected_task_cancellation=False,
        )
        if exit_cancellation is None:
            exit_cancellation = safety_cancellation
        elif safety_cancellation is not None:
            _attach_secondary(
                exit_cancellation,
                safety_cancellation,
                "additional session-exit cancellation",
            )
        channel._finish_close()  # pyright: ignore[reportPrivateUsage]

        for candidate, label in (
            (exit_cancellation, "session-exit cancellation"),
            (child_failure, "active exchange failure"),
            (safety_failure, "session safety failure"),
        ):
            primary = _merge_primary(primary, candidate, label)

        try:
            await self._transport.root_lock.__aexit__(
                None if primary is None else type(primary),
                primary,
                None if primary is None else primary.__traceback__,
            )
        except BaseException as lock_failure:
            primary = _merge_primary(primary, lock_failure, "root lock cleanup failure")

        cleanup_failures = channel._run_cleanup()  # pyright: ignore[reportPrivateUsage]
        if primary is not None:
            for cleanup_failure in cleanup_failures:
                _attach_secondary(primary, cleanup_failure, "response cleanup failure")

        if primary is None or primary is exc:
            return False
        raise primary
