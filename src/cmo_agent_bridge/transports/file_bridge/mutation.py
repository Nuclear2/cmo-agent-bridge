from __future__ import annotations

import asyncio
import inspect
import hashlib
import json
import math
import time
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import UUID

from pydantic import BaseModel, ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
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
    validate_body_runtime_snapshot,
)
from cmo_agent_bridge.protocol.response_models import (
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
)
from cmo_agent_bridge.state.pending_journal import (
    JournalDeleteExpectation,
    JournalRevisions,
    PendingJournalStore,
)
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestLedger, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
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


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _validated_number(
    value: object,
    *,
    description: str,
    allow_zero: bool,
) -> float:
    if type(value) not in {int, float}:
        raise _invalid_argument(f"{description} must be an exact int or float")
    try:
        result = float(cast(int | float, value))
    except OverflowError as error:
        raise _invalid_argument(f"{description} must be finite") from error
    if not math.isfinite(result) or result < 0 or (result == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "strictly positive"
        raise _invalid_argument(f"{description} must be finite and {qualifier}")
    return result


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


def _fields_set_tree(value: object) -> object:
    if isinstance(value, BaseModel):
        return (
            type(value),
            frozenset(value.model_fields_set),
            tuple(
                (name, _fields_set_tree(getattr(value, name))) for name in type(value).model_fields
            ),
        )
    if isinstance(value, Mapping):
        return tuple(
            (key, _fields_set_tree(item))
            for key, item in sorted(cast(Mapping[str, object], value).items())
        )
    if isinstance(value, (list, tuple)):
        return tuple(
            _fields_set_tree(item) for item in cast(list[object] | tuple[object, ...], value)
        )
    return None


def _public_wire_relationship_matches(
    invocation: ResolvedInvocation,
    rebuilt_public: BaseModel,
) -> bool:
    wire = invocation.wire_arguments
    if invocation.contract.confirmation_required:
        public_fields = frozenset(type(rebuilt_public).model_fields)
        wire_fields = frozenset(type(wire).model_fields)
        if not public_fields <= wire_fields:
            return False
        if rebuilt_public.model_fields_set != wire.model_fields_set & public_fields:
            return False
        return all(
            _canonical_json_bytes(getattr(rebuilt_public, field))
            == _canonical_json_bytes(getattr(wire, field))
            and _fields_set_tree(getattr(rebuilt_public, field))
            == _fields_set_tree(getattr(wire, field))
            for field in public_fields
        )
    rebuilt_wire = invocation.contract.wire_resolver.from_public(rebuilt_public, {})
    return rebuilt_wire == wire and _fields_set_tree(rebuilt_wire) == _fields_set_tree(wire)


@dataclass(frozen=True, slots=True)
class _ValidatedMutationCommand:
    request_id: UUID
    body: RequestBody
    body_bytes: bytes
    invocation: ResolvedInvocation
    runtime_snapshot: RuntimeSnapshot
    timeout: float | None


class _EpochSequence:
    def __init__(self) -> None:
        self._last = 0

    def next(self, *, minimum: int = 0) -> int:
        observed = time.time_ns() // 1_000_000
        value = max(observed, self._last, minimum, 0)
        self._last = value
        return value


@dataclass(slots=True)
class _MutationState:
    validated: _ValidatedMutationCommand
    response_path: Path
    epochs: _EpochSequence
    delivery: PreparedDelivery | None = None
    rendered: bytes | None = None
    intent: DeliveryIntent | None = None
    journal: PendingJournal | None = None
    prepared_journal: PendingJournal | None = None
    published_journal: PendingJournal | None = None
    response_accepted_journal: PendingJournal | None = None
    idle_published_journal: PendingJournal | None = None
    quarantined_journal: PendingJournal | None = None
    request_record: RequestRecord | None = None
    published_request: RequestRecord | None = None
    response_accepted_request: RequestRecord | None = None
    idle_published_request: RequestRecord | None = None
    terminal_request: RequestRecord | None = None
    prepared_delivery: DeliveryRecord | None = None
    published_delivery: DeliveryRecord | None = None
    response_delivery: DeliveryRecord | None = None
    response_artifact: ResponseArtifact | None = None
    journal_create_returned: bool = False
    request_insert_returned: bool = False
    delivery_insert_entered: bool = False
    delivery_insert_returned: bool = False
    publication_entered: bool = False
    publication_returned: bool = False
    publication_boundary: str | None = None
    published_at_ms: int | None = None
    artifact_observed: bool = False
    response_journal_save_entered: bool = False
    response_journal_save_returned: bool = False
    response_journal_safety_reread_entered: bool = False
    response_journal_safety_reread_returned: bool = False
    response_journal_safety_observed_journal: PendingJournal | None = None
    response_record_entered: bool = False
    response_record_returned: bool = False
    response_record_safety_reread_entered: bool = False
    response_record_safety_reread_returned: bool = False
    response_record_safety_observed_delivery: DeliveryRecord | None = None
    response_request_transition_entered: bool = False
    response_request_transition_returned: bool = False
    response_request_safety_reread_entered: bool = False
    response_request_safety_reread_returned: bool = False
    response_request_safety_observed_request: RequestRecord | None = None
    response_acceptance_continuation_entered: bool = False
    idle_publication_entered: bool = False
    idle_publication_returned: bool = False
    idle_journal_save_entered: bool = False
    idle_journal_save_returned: bool = False
    terminal_continuation_entered: bool = False
    terminal_continuation_finished: bool = False
    idle_request_transition_entered: bool = False
    idle_request_transition_returned: bool = False
    terminal_request_transition_entered: bool = False
    terminal_request_transition_returned: bool = False
    terminal_safety_reread_entered: bool = False
    terminal_safety_reread_returned: bool = False
    terminal_safety_observed_request: RequestRecord | None = None
    journal_delete_entered: bool = False
    journal_delete_returned: bool = False
    journal_delete_observation_entered: bool = False
    journal_delete_observation_returned: bool = False
    journal_delete_observed_journal: PendingJournal | None = None
    terminal_cleanup_entered: bool = False
    terminal_cleanup_returned: bool = False
    rejected_request: RequestRecord | None = None
    safety_finished: bool = False


def _invalid_command(error: BaseException) -> BridgeError:
    result = _invalid_argument("invalid file-bridge mutation command")
    result.__cause__ = error
    return result


def _recovery_validation_error(
    command: _ValidatedMutationCommand,
    cause: BaseException,
    *,
    recovery_schema_id: str,
) -> BridgeError:
    result = BridgeError(
        ErrorCode.PROTOCOL_ERROR,
        "mutation result failed frozen recovery validation",
        {
            "request_id": str(command.request_id),
            "operation": command.body.operation,
            "recovery_schema_id": recovery_schema_id,
        },
    )
    result.__cause__ = cause
    return result


def _validate_command(
    command: object,
    catalog: ManifestCatalog,
    *,
    allow_unbounded_timeout: bool,
) -> _ValidatedMutationCommand:
    try:
        if type(command) is not ExchangeCommand:
            raise TypeError("command must be an exact ExchangeCommand")
        trusted = command
        if type(trusted.request_id) is not UUID or trusted.request_id.version != 4:
            raise ValueError("request ID must be an exact canonical UUIDv4")
        if type(trusted.body) is not RequestBody:
            raise TypeError("body must be an exact RequestBody")
        if type(trusted.invocation) is not ResolvedInvocation:
            raise TypeError("invocation must be an exact ResolvedInvocation")
        if type(trusted.unbounded_wait) is not bool:
            raise TypeError("unbounded wait flag must be an exact bool")
        snapshot = revalidate_runtime_snapshot(trusted.runtime_snapshot)
        bounded_timeout = _validated_number(
            trusted.timeout,
            description="exchange timeout",
            allow_zero=True,
        )
        if trusted.unbounded_wait:
            if not allow_unbounded_timeout:
                raise ValueError("an unbounded timeout requires a durable mutation worker")
            if bounded_timeout != 0:
                raise ValueError("an unbounded mutation command must use timeout zero")
            timeout = None
        else:
            timeout = bounded_timeout

        body_tree = trusted.body.model_dump(
            mode="python",
            round_trip=True,
            warnings=False,
        )
        body = RequestBody.model_validate(body_tree)
        body_bytes = canonical_body_bytes(body)
        if body_bytes != canonical_body_bytes(trusted.body):
            raise ValueError("request body does not survive defensive reconstruction")
        validate_body_runtime_snapshot(body, snapshot)

        invocation = trusted.invocation
        if invocation.contract.target is not ExecutionTarget.CMO:
            raise ValueError("mutation invocation must target CMO")
        if invocation.effective_class not in {
            OperationClass.MUTATION,
            OperationClass.DESTRUCTIVE,
        }:
            raise ValueError("H9 accepts only concrete mutation or destructive operations")
        if (
            type(body.expected_lineage_id) is not UUID
            or type(body.expected_activation_id) is not UUID
        ):
            raise ValueError("mutation requires exact lineage and activation IDs")
        if body.operation != invocation.contract.name:
            raise ValueError("request body operation differs from invocation")
        wire_tree = invocation.wire_arguments.model_dump(mode="json")
        if _canonical_json_bytes(body.arguments) != _canonical_json_bytes(wire_tree):
            raise ValueError("request body arguments differ from typed wire arguments")

        public_tree = invocation.public_arguments.model_dump(
            mode="json",
            exclude_unset=True,
        )
        rebuilt_public = invocation.contract.public_arguments_adapter.validate_python(public_tree)
        if not isinstance(rebuilt_public, BaseModel):
            raise TypeError("public argument adapter did not produce a Pydantic model")
        if rebuilt_public != invocation.public_arguments or _fields_set_tree(
            rebuilt_public
        ) != _fields_set_tree(invocation.public_arguments):
            raise ValueError("public arguments do not rebuild with the same fields-set structure")
        if not _public_wire_relationship_matches(invocation, rebuilt_public):
            raise ValueError("public arguments differ from the frozen wire invocation")

        binding = catalog.resolve_running(snapshot.release_id)
        if binding.snapshot != snapshot:
            raise ValueError("catalog running snapshot differs from command snapshot")
        frozen = binding.registry.resolve_wire_invocation(body.operation, body.arguments)
        if not _same_frozen_projection(frozen, invocation):
            raise ValueError("wire invocation projection differs from resolved invocation")
        if invocation.recovery_schema is None or invocation.recovery_adapter is None:
            raise ValueError("mutation invocation has no frozen recovery schema")
        return _ValidatedMutationCommand(
            request_id=trusted.request_id,
            body=body,
            body_bytes=body_bytes,
            invocation=invocation,
            runtime_snapshot=snapshot,
            timeout=timeout,
        )
    except BridgeError as error:
        raise _invalid_command(error) from error
    except (
        AttributeError,
        ValidationError,
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise _invalid_command(error) from error


def _fresh_uuid4(request_id: UUID) -> UUID:
    delivery_id = uuid.uuid4()
    if type(delivery_id) is not UUID or delivery_id.version != 4 or delivery_id == request_id:
        raise _state_conflict("UUID generator returned an invalid mutation delivery ID")
    return delivery_id


def _process_details(value: ProcessInfo) -> dict[str, object]:
    return {
        "pid": value.pid,
        "create_time": value.create_time,
        "executable": str(value.executable),
    }


def _journal_with_original(
    journal: PendingJournal,
    **changes: object,
) -> PendingJournal:
    tree = journal.original.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    tree.update(changes)
    original = PendingExchange.model_validate(tree)
    return PendingJournal(
        header=journal.header,
        original=original,
        reconcile_attempt=None,
    )


def _canonical_json_text(value: object) -> str:
    return _canonical_json_bytes(value).decode("utf-8")


def _classify_exchange_error(error: BaseException) -> BaseException:
    if isinstance(error, BridgeError) or not isinstance(error, Exception):
        return error
    wrapped = _state_conflict(
        "file-bridge mutation exchange failed",
        {"type": type(error).__name__},
    )
    wrapped.__cause__ = error
    return wrapped


def _error_payload(error: BaseException) -> dict[str, object]:
    if isinstance(error, BridgeError):
        return cast(dict[str, object], error.to_payload())
    return {
        "code": ErrorCode.STATE_CONFLICT.value,
        "details": {"reason": "exchange_cancelled"},
        "message": "file-bridge mutation exchange was cancelled before publication",
    }


def _attach_secondary(
    primary: BaseException,
    secondary: BaseException,
    label: str,
) -> None:
    primary.add_note(f"{label}: {type(secondary).__name__}: {secondary}")


def _same_journal(left: PendingJournal, right: PendingJournal) -> bool:
    return _canonical_json_bytes(left.model_dump(mode="json")) == _canonical_json_bytes(
        right.model_dump(mode="json")
    )


def _request_with(record: RequestRecord, **changes: object) -> RequestRecord:
    tree = record.model_dump(mode="python", round_trip=True, warnings=False)
    tree.update(changes)
    return RequestRecord.model_validate(tree)


def _delivery_from_intent(intent: DeliveryIntent) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=intent.delivery_id,
        request_id=intent.request_id,
        delivery_kind=intent.delivery_kind,
        original_request_delivery_id=intent.original_request_delivery_id,
        intended_at_ms=intent.intended_at_ms,
        published_at_ms=intent.published_at_ms,
        rendered_inbox_sha256=intent.rendered_inbox_sha256,
        rendered_inbox_size_bytes=intent.rendered_inbox_size_bytes,
        response_filename=intent.response_filename,
        response_artifact=None,
        settlement=None,
    )


class MutationExchange:
    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        process_inspector: CmoProcessInspector,
        expected_process: ProcessInfo,
        catalog: ManifestCatalog,
        journals: PendingJournalStore,
        ledger: RequestLedger,
        inbox: InboxPublisher,
        response_poll_seconds: float,
        queue_response_cleanup: Callable[[Path], None],
        recovery_manager: RecoveryManager,
        durable_worker: bool = False,
    ) -> None:
        if type(paths) is not FileBridgePaths:
            raise _invalid_argument("paths must be an exact FileBridgePaths")
        if type(root_lock) is not RootLock:
            raise _invalid_argument("root lock must be an exact RootLock")
        if type(catalog) is not ManifestCatalog:
            raise _invalid_argument("catalog must be an exact ManifestCatalog")
        if type(journals) is not PendingJournalStore:
            raise _invalid_argument("journals must be an exact PendingJournalStore")
        if type(ledger) is not RequestLedger:
            raise _invalid_argument("ledger must be an exact RequestLedger")
        if type(inbox) is not InboxPublisher:
            raise _invalid_argument("inbox must be an exact InboxPublisher")
        if root_lock.path != paths.lock_file:
            raise _invalid_argument("root lock does not belong to the file-bridge root")
        if not _has_synchronous_process_method(process_inspector):
            raise _invalid_argument("process inspector must expose synchronous matching_processes")
        if not _valid_process(expected_process):
            raise _invalid_argument("expected process identity is invalid")
        poll_seconds = _validated_number(
            response_poll_seconds,
            description="response poll seconds",
            allow_zero=False,
        )
        if not callable(queue_response_cleanup):
            raise _invalid_argument("response cleanup queue callback must be callable")
        if type(durable_worker) is not bool:
            raise _invalid_argument("durable worker flag must be an exact bool")
        if type(recovery_manager) is not RecoveryManager or not recovery_manager._is_bound_to(  # pyright: ignore[reportPrivateUsage]
            paths=paths,
            root_lock=root_lock,
            process_inspector=process_inspector,
            expected_process=expected_process,
            journals=journals,
            ledger=ledger,
            inbox=inbox,
            response_poll_seconds=poll_seconds,
        ):
            raise _invalid_argument("recovery manager does not match the mutation exchange")

        self._paths = paths
        self._root_lock = root_lock
        self._process_inspector = process_inspector
        self._expected_process = expected_process
        self._catalog = catalog
        self._journals = journals
        self._ledger = ledger
        self._inbox = inbox
        self._response_poll_seconds = poll_seconds
        self._queue_response_cleanup = queue_response_cleanup
        self._recovery_manager = recovery_manager
        self._durable_worker = durable_worker
        self._recovery_task: asyncio.Task[None] | None = None
        self._poisoned = False
        self._current_state: _MutationState | None = None

    async def run(self, command: ExchangeCommand) -> ResponseArtifact:
        self._root_lock.require_acquired()
        state: _MutationState | None = None
        try:
            loaded = self._journals.load()
            if loaded is not None:
                self._poisoned = True
                original = loaded.journal.original
                raise BridgeError(
                    ErrorCode.MUTATION_QUARANTINED,
                    "an unresolved mutation blocks a new mutation",
                    {
                        "request_id": str(original.request_id),
                        "state": original.state.value,
                        "required_release_id": loaded.journal.header.required_release_id,
                    },
                )
            validated = _validate_command(
                command,
                self._catalog,
                allow_unbounded_timeout=self._durable_worker,
            )
            response_path = self._paths.response_path(validated.request_id)
            if response_path.exists():
                raise _state_conflict(
                    "response path already exists before mutation publication",
                    {"response_path": str(response_path)},
                )
            if self._ledger.get_request(validated.request_id) is not None:
                raise _state_conflict(
                    "request ID already belongs to an existing live or durable exchange",
                    {"request_id": str(validated.request_id)},
                )
            state = _MutationState(
                validated=validated,
                response_path=response_path,
                epochs=_EpochSequence(),
            )
            self._current_state = state
            return await self._execute(state)
        except BaseException as error:
            if state is None:
                primary = _classify_exchange_error(error)
                if primary is error:
                    raise
                raise primary

            if state.publication_entered and not state.publication_returned:
                self._poisoned = True
                if isinstance(error, Exception):
                    primary = BridgeError(
                        ErrorCode.INDETERMINATE_OUTCOME,
                        "mutation publication outcome is indeterminate",
                        {
                            "request_id": str(state.validated.request_id),
                            "reason": "publication_outcome_unknown",
                        },
                    )
                    primary.__cause__ = error
                    raise primary
                error.add_note("publication_outcome_unknown")
                raise

            if (
                state.publication_returned
                and state.publication_boundary == "waiter"
                and not state.artifact_observed
                and isinstance(error, asyncio.CancelledError)
            ):
                if self._durable_worker and state.validated.timeout is None:
                    # The queue owns this published mutation.  A worker
                    # shutdown must only detach from it: the durable journal,
                    # SQLite record and inbox delivery are the hand-off to the
                    # next worker session.  Publishing cancel/idle here would
                    # race a later CMO tick and break at-most-once delivery.
                    raise
                self._poisoned = True
                published = state.published_journal
                if published is None:
                    error.add_note("published cancellation recovery lacks its r1 journal")
                    raise
                recovery_task = asyncio.create_task(
                    self._recovery_manager.settle_published_cancellation(published)
                )
                self._recovery_task = recovery_task
                try:
                    recovery_succeeded = await self._await_cancellation_recovery(
                        recovery_task,
                        error,
                    )
                    if recovery_succeeded:
                        self._finish_successful_cancellation_recovery(state, error)
                finally:
                    if self._recovery_task is recovery_task and recovery_task.done():
                        self._recovery_task = None
                raise

            if (
                state.publication_returned
                and state.publication_boundary == "waiter"
                and not state.artifact_observed
                and isinstance(error, BridgeError)
                and error.code is ErrorCode.REQUEST_TIMEOUT
            ):
                primary = BridgeError(
                    ErrorCode.INDETERMINATE_OUTCOME,
                    "mutation outcome is indeterminate after response timeout",
                    {
                        "request_id": str(state.validated.request_id),
                        "reason": "response_timeout",
                    },
                )
                primary.__cause__ = error
            else:
                primary = _classify_exchange_error(error)
            if state.artifact_observed:
                self._poisoned = True
                if (
                    state.response_journal_save_entered
                    and not state.response_record_entered
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_response_journal_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if (
                    state.response_record_entered
                    and not state.response_request_transition_entered
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_response_record_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if (
                    state.response_request_transition_entered
                    and not state.response_acceptance_continuation_entered
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_response_request_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if (
                    state.idle_publication_returned
                    and state.idle_journal_save_entered
                    and not state.terminal_continuation_entered
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_idle_journal_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if (
                    state.terminal_continuation_entered
                    and not state.journal_delete_entered
                    and not state.terminal_continuation_finished
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_terminal_transition_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if (
                    state.journal_delete_entered
                    and not state.terminal_cleanup_entered
                    and not state.terminal_continuation_finished
                    and not state.safety_finished
                ):
                    try:
                        if self._finalize_terminal_delete_failure(state):
                            self._poisoned = False
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "artifact safety failure")
                if primary is error:
                    raise
                raise primary
            if not state.publication_entered:
                try:
                    self._finalize_prepublication(state, primary)
                except BaseException as safety_error:
                    self._poisoned = True
                    _attach_secondary(primary, safety_error, "prepublication safety failure")
            else:
                self._poisoned = True
                if isinstance(primary, Exception):
                    try:
                        self._finalize_publication_failure(state, primary)
                    except BaseException as safety_error:
                        _attach_secondary(primary, safety_error, "publication safety failure")
            if primary is error:
                raise
            raise primary

    @staticmethod
    async def _await_cancellation_recovery(
        task: asyncio.Task[None],
        primary: asyncio.CancelledError,
    ) -> bool:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError as secondary:
                if task.done() and task.cancelled():
                    _attach_secondary(primary, secondary, "cancellation recovery task failure")
                    return False
                _attach_secondary(
                    primary,
                    secondary,
                    "additional cancellation during mutation recovery",
                )
                continue
            except BaseException as secondary:
                _attach_secondary(primary, secondary, "cancellation recovery failure")
                return False
        try:
            task.result()
        except BaseException as secondary:
            _attach_secondary(primary, secondary, "cancellation recovery failure")
            return False
        return True

    def _finish_successful_cancellation_recovery(
        self,
        state: _MutationState,
        primary: asyncio.CancelledError,
    ) -> None:
        recovery_journal = None
        request = None
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                recovery_journal = self._journals.load()
                request = self._ledger.get_request(state.validated.request_id)
            except BaseException as observation_error:
                last_error = observation_error
                continue
            break
        else:
            if last_error is not None:
                _attach_secondary(
                    primary,
                    last_error,
                    "cancellation recovery completion observation failure",
                )
            return

        if recovery_journal is not None:
            return
        if (
            request is None
            or request.state
            not in {
                HostRequestState.CANCELLED,
                HostRequestState.COMPLETED,
                HostRequestState.REJECTED,
            }
            or request.terminal_at_ms is None
        ):
            _attach_secondary(
                primary,
                _state_conflict("cancellation recovery terminal evidence is incomplete"),
                "cancellation recovery completion failure",
            )
            return

        self._poisoned = False
        try:
            self._queue_response_cleanup(state.response_path)
        except BaseException as cleanup_error:
            _attach_secondary(
                primary,
                cleanup_error,
                "cancellation recovery response cleanup queue failure",
            )

    async def _execute(self, state: _MutationState) -> ResponseArtifact:
        validated = state.validated
        delivery_id = _fresh_uuid4(validated.request_id)
        delivery = prepare_delivery(
            validated.body,
            request_id=validated.request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
        )
        rendered = render_delivery_lua(delivery, validated.runtime_snapshot)
        intended_at_ms = state.epochs.next()
        recovery_schema = validated.invocation.recovery_schema
        if recovery_schema is None:
            raise _state_conflict("validated mutation lost its recovery schema")
        intent = DeliveryIntent(
            request_id=validated.request_id,
            delivery_id=delivery_id,
            delivery_kind="request",
            original_request_delivery_id=delivery_id,
            body_json=delivery.body_json.decode("utf-8", errors="strict"),
            request_hash=delivery.request_hash,
            runtime_snapshot=validated.runtime_snapshot,
            result_schema_id=validated.invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema.schema_id,
            intended_at_ms=intended_at_ms,
            published_at_ms=None,
            rendered_inbox_sha256=hashlib.sha256(rendered).hexdigest(),
            rendered_inbox_size_bytes=len(rendered),
            response_filename=state.response_path.name,
        )
        original = PendingExchange(
            request_id=validated.request_id,
            request_hash=delivery.request_hash,
            operation=validated.body.operation,
            effective_class=validated.invocation.effective_class,
            body_json=delivery.body_json.decode("utf-8", errors="strict"),
            runtime_snapshot=validated.runtime_snapshot,
            result_schema_id=validated.invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema.schema_id,
            expected_lineage_id=validated.body.expected_lineage_id,
            expected_activation_id=validated.body.expected_activation_id,
            delivery_intents=(intent,),
            response_artifact=None,
            settlement=None,
            original_target_request_id=None,
            original_target_request_hash=None,
            revision=0,
            state=PendingPhase.PREPARED,
            created_at_ms=intended_at_ms,
            updated_at_ms=intended_at_ms,
        )
        journal = PendingJournal(
            header=PendingJournalHeader(
                format="cmo-agent-bridge/pending-journal",
                header_version=1,
                root_key=self._paths.root_key,
                required_release_id=validated.runtime_snapshot.release_id,
            ),
            original=original,
            reconcile_attempt=None,
        )
        state.delivery = delivery
        state.rendered = rendered
        state.intent = intent
        state.journal = journal
        state.prepared_journal = journal
        state.prepared_delivery = _delivery_from_intent(intent)
        revisions = self._journals.save(journal, expected_revisions=None)
        if revisions != JournalRevisions(original=0, reconcile_attempt=None):
            raise _state_conflict("revision-zero mutation journal save returned drift")
        state.journal_create_returned = True

        request = RequestRecord(
            request_id=validated.request_id,
            root_key=self._paths.root_key,
            request_hash=delivery.request_hash,
            operation=validated.body.operation,
            operation_class=validated.invocation.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=validated.runtime_snapshot,
            result_schema_id=validated.invocation.result_schema.schema_id,
            recovery_schema_id=recovery_schema.schema_id,
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
        self._ledger.insert_prepared(request)
        state.request_insert_returned = True
        state.delivery_insert_entered = True
        self._ledger.insert_delivery(intent)
        state.delivery_insert_returned = True
        self._require_original_process_before_publication()

        state.publication_entered = True
        self._inbox.publish_delivery(
            delivery,
            runtime_snapshot=validated.runtime_snapshot,
        )
        state.publication_returned = True
        published_at_ms = state.epochs.next(minimum=intended_at_ms)
        state.published_at_ms = published_at_ms
        published_intent = DeliveryIntent.model_validate(
            {
                **intent.model_dump(mode="python", round_trip=True, warnings=False),
                "published_at_ms": published_at_ms,
            }
        )
        published = _journal_with_original(
            journal,
            delivery_intents=(published_intent,),
            revision=1,
            state=PendingPhase.PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        state.journal = published
        state.published_journal = published
        state.published_delivery = _delivery_from_intent(published_intent)
        state.published_request = _request_with(
            request,
            state=HostRequestState.PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        state.publication_boundary = "journal"
        revisions = self._journals.save(
            published,
            expected_revisions=JournalRevisions(original=0, reconcile_attempt=None),
        )
        if revisions != JournalRevisions(original=1, reconcile_attempt=None):
            raise _state_conflict("published mutation journal save returned drift")
        state.publication_boundary = "delivery_marker"
        delivery_record = self._ledger.mark_delivery_published(
            delivery_id,
            published_at_ms=published_at_ms,
        )
        if (
            type(delivery_record) is not DeliveryRecord
            or delivery_record != state.published_delivery
        ):
            raise _state_conflict("published delivery marker returned drift")
        state.publication_boundary = "request_published"
        published_request = self._ledger.transition(
            validated.request_id,
            expected_states=frozenset({HostRequestState.PREPARED}),
            new_state=HostRequestState.PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        if (
            type(published_request) is not RequestRecord
            or published_request != state.published_request
        ):
            raise _state_conflict("published request transition returned drift")
        state.publication_boundary = "waiter"

        expectation = ResponseExpectation(
            request_id=validated.request_id,
            allowed_deliveries=(AllowedDelivery(delivery_id=delivery_id, delivery_kind="request"),),
            request_hash=delivery.request_hash,
            expected_lineage_id=validated.body.expected_lineage_id,
            expected_activation_id=validated.body.expected_activation_id,
            status_bootstrap=False,
            activation_candidate=None,
            runtime_snapshot=validated.runtime_snapshot,
            invocation=validated.invocation,
        )
        ownership_check: Callable[[], bool] | None = None
        if validated.timeout is None:
            rendered_request = rendered

            def owns_published_inbox() -> bool:
                try:
                    return self._paths.inbox.read_bytes() == rendered_request
                except FileNotFoundError:
                    return False

            ownership_check = owns_published_inbox
        waiter = ResponseWaiter(
            response_path=state.response_path,
            expectation=expectation,
            expected_process=self._expected_process,
            process_check=lambda: require_single_instance(
                self._process_inspector,
                self._paths.command_exe,
            ),
            poll_seconds=self._response_poll_seconds,
            ownership_check=ownership_check,
        )
        artifact = await waiter.wait(validated.timeout)
        state.artifact_observed = True
        state.response_artifact = artifact

        response_accepted_at_ms = state.epochs.next(minimum=published.original.updated_at_ms)
        response_accepted = _journal_with_original(
            published,
            response_artifact=artifact,
            settlement=artifact.accepted_response.settlement,
            revision=2,
            state=PendingPhase.RESPONSE_ACCEPTED,
            updated_at_ms=response_accepted_at_ms,
        )
        state.journal = response_accepted
        state.response_accepted_journal = response_accepted
        published_delivery = state.published_delivery
        published_request = state.published_request
        state.response_delivery = DeliveryRecord(
            delivery_id=published_delivery.delivery_id,
            request_id=published_delivery.request_id,
            delivery_kind=published_delivery.delivery_kind,
            original_request_delivery_id=published_delivery.original_request_delivery_id,
            intended_at_ms=published_delivery.intended_at_ms,
            published_at_ms=published_delivery.published_at_ms,
            rendered_inbox_sha256=published_delivery.rendered_inbox_sha256,
            rendered_inbox_size_bytes=published_delivery.rendered_inbox_size_bytes,
            response_filename=published_delivery.response_filename,
            response_artifact=artifact,
            settlement=artifact.accepted_response.settlement,
        )
        state.response_accepted_request = _request_with(
            published_request,
            state=HostRequestState.RESPONSE_ACCEPTED,
            updated_at_ms=response_accepted_at_ms,
        )
        state.response_journal_save_entered = True
        revisions = self._journals.save(
            response_accepted,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        state.response_journal_save_returned = True
        if revisions != JournalRevisions(original=2, reconcile_attempt=None):
            raise _state_conflict("response-accepted mutation journal save returned drift")

        self._continue_after_response_journal(state)
        return artifact

    def _continue_after_response_journal(self, state: _MutationState) -> None:
        self._record_response_once(state)
        self._transition_response_accepted_once(state)
        self._continue_after_response_acceptance(state)

    def _record_response_once(self, state: _MutationState) -> None:
        artifact = state.response_artifact
        candidate = state.response_delivery
        if state.response_record_entered or artifact is None or candidate is None:
            raise _state_conflict("response delivery recording lacks exact candidates")
        state.response_record_entered = True
        observed = self._ledger.record_response(artifact)
        state.response_record_returned = True
        if type(observed) is not DeliveryRecord or observed != candidate:
            raise _state_conflict("recorded mutation response delivery returned drift")
        durable = self._observe_response_delivery_once(state)
        if durable == candidate:
            return
        if durable == state.published_delivery:
            raise _state_conflict("recorded mutation response delivery postcondition failed")
        raise _state_conflict("recorded mutation response evidence drifted")

    def _observe_response_delivery_once(
        self,
        state: _MutationState,
    ) -> DeliveryRecord | None:
        if state.response_record_safety_reread_entered:
            raise _state_conflict("response delivery observation was already entered")
        candidate = state.response_delivery
        if candidate is None:
            raise _state_conflict("response delivery observation lacks its candidate")
        state.response_record_safety_reread_entered = True
        observed = self._ledger.get_delivery(candidate.delivery_id)
        state.response_record_safety_reread_returned = True
        state.response_record_safety_observed_delivery = observed
        return observed

    def _transition_response_accepted_once(self, state: _MutationState) -> None:
        candidate = state.response_accepted_request
        if state.response_request_transition_entered or candidate is None:
            raise _state_conflict("response-accepted request transition lacks exact candidate")
        state.response_request_transition_entered = True
        observed = self._ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.PUBLISHED}),
            new_state=HostRequestState.RESPONSE_ACCEPTED,
            updated_at_ms=candidate.updated_at_ms,
        )
        state.response_request_transition_returned = True
        if type(observed) is not RequestRecord or observed != candidate:
            raise _state_conflict("response-accepted request transition returned drift")
        durable = self._observe_response_request_once(state)
        if durable == candidate:
            return
        if durable == state.published_request:
            raise _state_conflict("response-accepted request transition postcondition failed")
        raise _state_conflict("response-accepted request evidence drifted")

    def _observe_response_request_once(
        self,
        state: _MutationState,
    ) -> RequestRecord | None:
        if state.response_request_safety_reread_entered:
            raise _state_conflict("response-accepted request observation was already entered")
        state.response_request_safety_reread_entered = True
        observed = self._ledger.get_request(state.validated.request_id)
        state.response_request_safety_reread_returned = True
        state.response_request_safety_observed_request = observed
        return observed

    def _continue_after_response_acceptance(self, state: _MutationState) -> None:
        validated = state.validated
        artifact = state.response_artifact
        response_accepted = state.response_accepted_journal
        response_accepted_request = state.response_accepted_request
        response_record_exact = state.response_record_returned or (
            state.response_record_safety_reread_returned
            and state.response_record_safety_observed_delivery == state.response_delivery
        )
        response_request_exact = (
            state.response_request_safety_reread_returned
            and state.response_request_safety_observed_request == response_accepted_request
        )
        if (
            artifact is None
            or response_accepted is None
            or response_accepted_request is None
            or not state.response_record_entered
            or not response_record_exact
            or not state.response_request_transition_entered
            or not response_request_exact
            or state.response_acceptance_continuation_entered
        ):
            raise _state_conflict("artifact continuation lacks exact response acceptance")
        state.response_acceptance_continuation_entered = True

        settlement = artifact.accepted_response.settlement
        if type(settlement) is CompletedSettlement:
            recovery_schema = validated.invocation.recovery_schema
            if recovery_schema is None:
                raise _state_conflict("validated mutation lost its recovery schema")
            try:
                recovery_adapter = recovery_schema.adapter
                recovered = recovery_adapter.validate_python(settlement.result)
                normalized_result = recovery_adapter.dump_python(recovered, mode="json")
                normalized_bytes = _canonical_json_bytes(normalized_result)
                if normalized_bytes != _canonical_json_bytes(settlement.result):
                    raise ValueError("normalized recovery result changed canonical bytes")
            except (
                ValidationError,
                TypeError,
                ValueError,
                OverflowError,
                RecursionError,
            ) as error:
                primary = _recovery_validation_error(
                    validated,
                    error,
                    recovery_schema_id=recovery_schema.schema_id,
                )
                self._quarantine_accepted_artifact(state, primary)
                raise primary from error
            terminal_state, result_json, error_json = (
                HostRequestState.COMPLETED,
                normalized_bytes.decode("utf-8"),
                None,
            )
        elif type(settlement) is RejectedSettlement:
            response_error = artifact.accepted_response.envelope.error
            if response_error is None:
                raise _state_conflict("rejected mutation settlement lost its response error")
            terminal_state, result_json, error_json = (
                HostRequestState.REJECTED,
                None,
                _canonical_json_text(response_error.model_dump(mode="json")),
            )
        elif settlement is None and artifact.accepted_response.envelope.error is not None:
            response_error = artifact.accepted_response.envelope.error
            primary = BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "mutation response does not prove a terminal outcome",
                {
                    "request_id": str(validated.request_id),
                    "reason": "response_without_terminal_settlement",
                    "response_code": response_error.code.value,
                },
            )
            self._quarantine_accepted_artifact(state, primary)
            raise primary
        else:
            raise _state_conflict("accepted mutation settlement is not a completed result")

        state.idle_publication_entered = True
        self._inbox.publish_idle()
        state.idle_publication_returned = True
        idle_published_at_ms = state.epochs.next(minimum=response_accepted.original.updated_at_ms)
        idle_published = _journal_with_original(
            response_accepted,
            revision=3,
            state=PendingPhase.IDLE_PUBLISHED,
            updated_at_ms=idle_published_at_ms,
        )
        state.journal = idle_published
        state.idle_published_journal = idle_published
        idle_published_request = _request_with(
            response_accepted_request,
            state=HostRequestState.IDLE_PUBLISHED,
            updated_at_ms=idle_published_at_ms,
        )
        state.idle_published_request = idle_published_request
        terminal_at_ms = state.epochs.next(minimum=idle_published_at_ms)
        state.terminal_request = _request_with(
            idle_published_request,
            state=terminal_state,
            updated_at_ms=terminal_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=result_json,
            error_json=error_json,
            resolution_json=None,
        )
        state.idle_journal_save_entered = True
        revisions = self._journals.save(
            idle_published,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        )
        state.idle_journal_save_returned = True
        if revisions != JournalRevisions(original=3, reconcile_attempt=None):
            raise _state_conflict("idle-published mutation journal save returned drift")
        self._continue_artifact_terminal(state, idle_published)

    def _continue_artifact_terminal(
        self,
        state: _MutationState,
        durable: PendingJournal,
    ) -> None:
        idle_published = state.idle_published_journal
        idle_published_request = state.idle_published_request
        terminal_request = state.terminal_request
        if (
            state.terminal_continuation_entered
            or state.safety_finished
            or idle_published is None
            or durable is not idle_published
            or state.journal is not idle_published
            or idle_published.original.state is not PendingPhase.IDLE_PUBLISHED
            or idle_published.original.revision != 3
            or idle_published_request is None
            or idle_published_request.state is not HostRequestState.IDLE_PUBLISHED
            or idle_published_request.terminal_at_ms is not None
            or terminal_request is None
            or terminal_request.state not in {HostRequestState.COMPLETED, HostRequestState.REJECTED}
            or terminal_request.terminal_at_ms is None
        ):
            raise _state_conflict("terminal continuation lacks exact idle-published evidence")

        state.terminal_continuation_entered = True
        self._transition_idle_request_once(state)
        self._transition_terminal_request_once(state)
        self._delete_terminal_journal_once(state)

    def _transition_idle_request_once(self, state: _MutationState) -> None:
        candidate = state.idle_published_request
        if state.idle_request_transition_entered or candidate is None:
            raise _state_conflict("idle-published request transition was already entered")
        state.idle_request_transition_entered = True
        observed = self._ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.RESPONSE_ACCEPTED}),
            new_state=HostRequestState.IDLE_PUBLISHED,
            updated_at_ms=candidate.updated_at_ms,
        )
        state.idle_request_transition_returned = True
        if type(observed) is not RequestRecord or observed != candidate:
            raise _state_conflict("idle-published request transition returned drift")

    def _transition_terminal_request_once(self, state: _MutationState) -> None:
        candidate = state.terminal_request
        if state.terminal_request_transition_entered or candidate is None:
            raise _state_conflict("terminal request transition was already entered")
        state.terminal_request_transition_entered = True
        observed = self._ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.IDLE_PUBLISHED}),
            new_state=candidate.state,
            updated_at_ms=candidate.updated_at_ms,
            terminal_at_ms=candidate.terminal_at_ms,
            result_json=candidate.result_json,
            error_json=candidate.error_json,
            resolution_json=candidate.resolution_json,
        )
        state.terminal_request_transition_returned = True
        if type(observed) is not RequestRecord or observed != candidate:
            raise _state_conflict("terminal request transition returned drift")

    def _delete_terminal_journal_once(self, state: _MutationState) -> None:
        if state.journal_delete_entered:
            raise _state_conflict("terminal mutation journal delete was already entered")
        state.journal_delete_entered = True
        result = cast(
            object,
            self._journals.delete(
                JournalDeleteExpectation(
                    root_key=self._paths.root_key,
                    required_release_id=state.validated.runtime_snapshot.release_id,
                    original_request_id=state.validated.request_id,
                    reconcile_attempt_request_id=None,
                    revisions=JournalRevisions(original=3, reconcile_attempt=None),
                )
            ),
        )
        state.journal_delete_returned = True
        if result is not None:
            raise _state_conflict("terminal mutation journal delete returned drift")
        observed = self._observe_terminal_journal_once(state)
        if observed is not None:
            candidate = state.idle_published_journal
            if candidate is None:
                raise _state_conflict("terminal mutation journal delete lost its candidate")
            if _same_journal(observed, candidate):
                raise _state_conflict("terminal mutation journal delete postcondition failed")
            state.journal = observed
            raise _state_conflict("terminal mutation journal delete evidence drifted")
        self._finish_terminal_cleanup_once(state)

    def _observe_terminal_journal_once(
        self,
        state: _MutationState,
    ) -> PendingJournal | None:
        if state.journal_delete_observation_entered:
            raise _state_conflict("terminal journal delete observation was already entered")
        state.journal_delete_observation_entered = True
        loaded = self._journals.load()
        state.journal_delete_observation_returned = True
        observed = None if loaded is None else loaded.journal
        state.journal_delete_observed_journal = observed
        return observed

    def _finish_terminal_cleanup_once(self, state: _MutationState) -> None:
        if state.terminal_cleanup_entered:
            raise _state_conflict("terminal response cleanup was already entered")
        state.terminal_cleanup_entered = True
        try:
            self._queue_response_cleanup(state.response_path)
        except Exception:
            pass
        else:
            state.terminal_cleanup_returned = True
        state.terminal_continuation_finished = True
        state.safety_finished = True

    def _finalize_response_journal_failure(self, state: _MutationState) -> bool:
        published = state.published_journal
        response_accepted = state.response_accepted_journal
        artifact = state.response_artifact
        response_delivery = state.response_delivery
        response_request = state.response_accepted_request
        if (
            state.safety_finished
            or not state.artifact_observed
            or not state.response_journal_save_entered
            or state.response_record_entered
            or state.response_journal_safety_reread_entered
            or published is None
            or published.original.state is not PendingPhase.PUBLISHED
            or published.original.revision != 1
            or response_accepted is None
            or response_accepted.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or response_accepted.original.revision != 2
            or artifact is None
            or response_accepted.original.response_artifact != artifact
            or response_delivery is None
            or response_delivery.response_artifact != artifact
            or response_request is None
            or response_request.state is not HostRequestState.RESPONSE_ACCEPTED
        ):
            raise _state_conflict("response-accepted journal safety lacks exact candidates")

        state.response_journal_safety_reread_entered = True
        loaded = self._journals.load()
        state.response_journal_safety_reread_returned = True
        observed = None if loaded is None else loaded.journal
        state.response_journal_safety_observed_journal = observed
        if observed is None:
            state.journal = None
            raise _state_conflict("response-accepted mutation journal evidence drifted")
        if _same_journal(observed, published):
            state.journal = observed
            state.safety_finished = True
            return False
        if not _same_journal(observed, response_accepted):
            state.journal = observed
            raise _state_conflict("response-accepted mutation journal evidence drifted")

        state.journal = response_accepted
        self._continue_after_response_journal(state)
        return True

    def _finalize_response_record_failure(self, state: _MutationState) -> bool:
        journal = state.response_accepted_journal
        predecessor = state.published_delivery
        target = state.response_delivery
        artifact = state.response_artifact
        if (
            state.safety_finished
            or not state.artifact_observed
            or not state.response_record_entered
            or state.response_request_transition_entered
            or journal is None
            or state.journal is not journal
            or journal.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or journal.original.revision != 2
            or predecessor is None
            or target is None
            or artifact is None
            or target.response_artifact != artifact
            or target.settlement != artifact.accepted_response.settlement
        ):
            raise _state_conflict("response record safety lacks exact candidates")

        if state.response_record_safety_reread_entered:
            if not state.response_record_safety_reread_returned:
                raise _state_conflict("response delivery observation did not return")
            observed = state.response_record_safety_observed_delivery
        else:
            observed = self._observe_response_delivery_once(state)

        if observed == predecessor:
            state.safety_finished = True
            return False
        if observed != target:
            raise _state_conflict("recorded mutation response evidence drifted")

        self._transition_response_accepted_once(state)
        self._continue_after_response_acceptance(state)
        return True

    def _finalize_response_request_failure(self, state: _MutationState) -> bool:
        journal = state.response_accepted_journal
        predecessor = state.published_request
        target = state.response_accepted_request
        response_delivery_exact = (
            state.response_record_safety_reread_returned
            and state.response_record_safety_observed_delivery == state.response_delivery
        )
        if (
            state.safety_finished
            or not state.artifact_observed
            or not state.response_request_transition_entered
            or state.response_acceptance_continuation_entered
            or journal is None
            or state.journal is not journal
            or journal.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or journal.original.revision != 2
            or predecessor is None
            or target is None
            or not response_delivery_exact
        ):
            raise _state_conflict("response-accepted request safety lacks exact candidates")

        if state.response_request_safety_reread_entered:
            if not state.response_request_safety_reread_returned:
                raise _state_conflict("response-accepted request observation did not return")
            observed = state.response_request_safety_observed_request
        else:
            observed = self._observe_response_request_once(state)

        if observed == predecessor:
            state.safety_finished = True
            return False
        if observed != target:
            raise _state_conflict("response-accepted request evidence drifted")

        self._continue_after_response_acceptance(state)
        return True

    def _finalize_idle_journal_failure(self, state: _MutationState) -> bool:
        response_accepted = state.response_accepted_journal
        idle_published = state.idle_published_journal
        if (
            state.safety_finished
            or not state.idle_publication_returned
            or not state.idle_journal_save_entered
            or state.terminal_continuation_entered
            or response_accepted is None
            or response_accepted.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or response_accepted.original.revision != 2
            or idle_published is None
            or idle_published.original.state is not PendingPhase.IDLE_PUBLISHED
            or idle_published.original.revision != 3
            or state.idle_published_request is None
            or state.terminal_request is None
        ):
            raise _state_conflict("idle-published journal safety lacks exact candidates")

        loaded = self._journals.load()
        if loaded is None:
            state.journal = None
            raise _state_conflict("idle-published mutation journal evidence drifted")
        durable = loaded.journal
        if _same_journal(durable, response_accepted):
            state.journal = durable
            state.safety_finished = True
            return False
        if not _same_journal(durable, idle_published):
            state.journal = durable
            raise _state_conflict("idle-published mutation journal evidence drifted")

        state.journal = idle_published
        self._continue_artifact_terminal(state, idle_published)
        return True

    def _finalize_terminal_transition_failure(self, state: _MutationState) -> bool:
        response_accepted = state.response_accepted_request
        idle_published = state.idle_published_request
        terminal = state.terminal_request
        journal = state.idle_published_journal
        if (
            not state.artifact_observed
            or not state.idle_publication_returned
            or not state.terminal_continuation_entered
            or state.terminal_continuation_finished
            or state.safety_finished
            or state.journal_delete_entered
            or state.terminal_safety_reread_entered
            or journal is None
            or state.journal is not journal
            or journal.original.state is not PendingPhase.IDLE_PUBLISHED
            or journal.original.revision != 3
            or response_accepted is None
            or idle_published is None
            or terminal is None
        ):
            raise _state_conflict("terminal transition safety lacks exact candidates")

        if state.terminal_request_transition_entered:
            if (
                not state.idle_request_transition_entered
                or not state.idle_request_transition_returned
            ):
                raise _state_conflict("terminal transition safety has inconsistent boundaries")
            predecessor = idle_published
            target = terminal
            boundary = "terminal"
        elif state.idle_request_transition_entered:
            predecessor = response_accepted
            target = idle_published
            boundary = "idle"
        else:
            raise _state_conflict("terminal transition safety has no entered boundary")

        state.terminal_safety_reread_entered = True
        observed = self._ledger.get_request(state.validated.request_id)
        state.terminal_safety_reread_returned = True
        state.terminal_safety_observed_request = observed
        if type(observed) is not RequestRecord:
            raise _state_conflict("terminal continuation request evidence drifted")
        if observed == predecessor:
            state.safety_finished = True
            return False
        if observed != target:
            raise _state_conflict("terminal continuation request evidence drifted")

        if boundary == "idle":
            self._transition_terminal_request_once(state)
        self._delete_terminal_journal_once(state)
        return True

    def _finalize_terminal_delete_failure(self, state: _MutationState) -> bool:
        journal = state.idle_published_journal
        terminal = state.terminal_request
        terminal_evidence = state.terminal_request_transition_returned or (
            state.terminal_safety_reread_returned
            and state.terminal_safety_observed_request == terminal
        )
        if (
            not state.artifact_observed
            or not state.terminal_continuation_entered
            or state.terminal_continuation_finished
            or state.safety_finished
            or not state.journal_delete_entered
            or state.terminal_cleanup_entered
            or journal is None
            or journal.original.state is not PendingPhase.IDLE_PUBLISHED
            or journal.original.revision != 3
            or terminal is None
            or terminal.state not in {HostRequestState.COMPLETED, HostRequestState.REJECTED}
            or not terminal_evidence
        ):
            raise _state_conflict("terminal journal delete safety lacks exact evidence")

        if state.journal_delete_observation_entered:
            if not state.journal_delete_observation_returned:
                raise _state_conflict("terminal journal delete observation did not return")
            observed = state.journal_delete_observed_journal
        else:
            observed = self._observe_terminal_journal_once(state)

        if observed is None:
            self._finish_terminal_cleanup_once(state)
            return True
        if _same_journal(observed, journal):
            state.journal = observed
            state.safety_finished = True
            return False
        state.journal = observed
        raise _state_conflict("terminal mutation journal delete evidence drifted")

    def _quarantine_accepted_artifact(
        self,
        state: _MutationState,
        primary: BridgeError,
    ) -> None:
        response_accepted = state.response_accepted_journal
        response_accepted_request = state.response_accepted_request
        artifact = state.response_artifact
        response_delivery = state.response_delivery
        if (
            not state.artifact_observed
            or artifact is None
            or response_delivery is None
            or response_accepted is None
            or response_accepted_request is None
            or response_accepted.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or response_accepted.original.revision != 2
            or response_accepted.original.response_artifact != artifact
            or response_accepted.original.settlement != artifact.accepted_response.settlement
            or state.journal is not response_accepted
            or response_delivery.response_artifact != artifact
            or response_delivery.settlement != artifact.accepted_response.settlement
            or response_accepted_request.state is not HostRequestState.RESPONSE_ACCEPTED
            or response_accepted_request.terminal_at_ms is not None
            or response_accepted_request.result_json is not None
            or response_accepted_request.error_json is not None
            or response_accepted_request.resolution_json is not None
            or state.quarantined_journal is not None
            or state.safety_finished
        ):
            raise _state_conflict("artifact quarantine lacks exact response-accepted evidence")

        quarantine_error_json = _canonical_json_text(_error_payload(primary))
        quarantined_at_ms = state.epochs.next(
            minimum=max(
                response_accepted.original.updated_at_ms,
                response_accepted_request.updated_at_ms,
            )
        )
        quarantined = _journal_with_original(
            response_accepted,
            revision=3,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=quarantined_at_ms,
        )
        state.quarantined_journal = quarantined
        revisions = self._journals.save(
            quarantined,
            expected_revisions=JournalRevisions(original=2, reconcile_attempt=None),
        )
        if revisions != JournalRevisions(original=3, reconcile_attempt=None):
            raise _state_conflict("quarantined mutation journal save returned drift")
        state.journal = quarantined

        quarantined_request = _request_with(
            response_accepted_request,
            state=HostRequestState.QUARANTINED,
            updated_at_ms=quarantined_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=quarantine_error_json,
            resolution_json=None,
        )
        observed_quarantined_request = self._ledger.transition(
            state.validated.request_id,
            expected_states=frozenset({HostRequestState.RESPONSE_ACCEPTED}),
            new_state=HostRequestState.QUARANTINED,
            updated_at_ms=quarantined_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=quarantine_error_json,
            resolution_json=None,
        )
        if (
            type(observed_quarantined_request) is not RequestRecord
            or observed_quarantined_request != quarantined_request
        ):
            raise _state_conflict("quarantined mutation request transition returned drift")
        state.safety_finished = True

    def _require_original_process_before_publication(self) -> None:
        try:
            actual = require_single_instance(
                self._process_inspector,
                self._paths.command_exe,
            )
        except BridgeError:
            raise
        except Exception as error:
            raise _state_conflict(
                "CMO process inspection failed",
                {"type": type(error).__name__},
            ) from error
        if not _valid_process(actual):
            raise _state_conflict(
                "CMO process inspection returned invalid process identity",
                {"type": type(actual).__name__},
            )
        if (
            actual.pid != self._expected_process.pid
            or actual.create_time != self._expected_process.create_time
            or str(actual.executable) != str(self._expected_process.executable)
        ):
            raise _state_conflict(
                "CMO process identity changed before mutation publication",
                {
                    "expected_process": _process_details(self._expected_process),
                    "actual_process": _process_details(actual),
                },
            )

    def _finalize_prepublication(
        self,
        state: _MutationState,
        primary: BaseException,
    ) -> None:
        if state.safety_finished:
            return
        loaded = self._journals.load()
        if loaded is None:
            if state.journal_create_returned:
                raise _state_conflict(
                    "prepublication journal disappeared after revision-zero creation"
                )
            state.safety_finished = True
            return
        expected = state.journal
        if expected is None:
            raise _state_conflict("prepublication safety found an unowned pending journal")
        if (
            _canonical_json_bytes(loaded.journal.model_dump(mode="json"))
            != _canonical_json_bytes(expected.model_dump(mode="json"))
            or loaded.journal.original.state is not PendingPhase.PREPARED
            or loaded.journal.original.revision != 0
        ):
            raise _state_conflict("prepublication pending journal evidence drifted")

        request = self._ledger.get_request(state.validated.request_id)
        owned = state.request_record
        if request is None:
            if state.request_insert_returned or state.delivery_insert_entered:
                raise _state_conflict(
                    "prepublication request disappeared after its insert boundary"
                )
        else:
            if type(request) is not RequestRecord or owned is None or request != owned:
                raise _state_conflict("prepublication request ownership drifted")
            payload = _canonical_json_text(_error_payload(primary))
            terminal_at_ms = state.epochs.next(minimum=request.updated_at_ms)
            rejected = _request_with(
                request,
                state=HostRequestState.REJECTED,
                updated_at_ms=terminal_at_ms,
                terminal_at_ms=terminal_at_ms,
                error_json=payload,
            )
            state.rejected_request = rejected
            try:
                observed = self._ledger.transition(
                    request.request_id,
                    expected_states=frozenset({HostRequestState.PREPARED}),
                    new_state=HostRequestState.REJECTED,
                    updated_at_ms=terminal_at_ms,
                    terminal_at_ms=terminal_at_ms,
                    error_json=payload,
                )
            except BaseException as transition_error:
                observed = self._ledger.get_request(request.request_id)
                if type(observed) is RequestRecord and observed == rejected:
                    _attach_secondary(
                        primary,
                        transition_error,
                        "prepublication rejection transition failure",
                    )
                else:
                    raise
            if type(observed) is not RequestRecord or observed != rejected:
                raise _state_conflict("prepublication request rejection did not converge")

        self._journals.delete(
            JournalDeleteExpectation(
                root_key=self._paths.root_key,
                required_release_id=state.validated.runtime_snapshot.release_id,
                original_request_id=state.validated.request_id,
                reconcile_attempt_request_id=None,
                revisions=JournalRevisions(original=0, reconcile_attempt=None),
            )
        )
        state.safety_finished = True

    def _finalize_publication_failure(
        self,
        state: _MutationState,
        primary: BaseException,
    ) -> None:
        if state.safety_finished:
            return
        prepared = state.prepared_journal
        published = state.published_journal
        if prepared is None or published is None:
            raise _state_conflict("publication safety lacks complete journal candidates")
        loaded = self._journals.load()
        if loaded is None:
            raise _state_conflict("publication pending journal disappeared")
        durable = loaded.journal
        if _same_journal(durable, prepared):
            state.safety_finished = True
            return
        if not _same_journal(durable, published):
            raise _state_conflict("publication pending journal candidate drifted")

        quarantine_at_ms = state.epochs.next(minimum=published.original.updated_at_ms)
        quarantined = _journal_with_original(
            published,
            revision=2,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=quarantine_at_ms,
        )
        state.quarantined_journal = quarantined
        revisions = self._journals.save(
            quarantined,
            expected_revisions=JournalRevisions(original=1, reconcile_attempt=None),
        )
        if revisions != JournalRevisions(original=2, reconcile_attempt=None):
            raise _state_conflict("quarantined mutation journal save returned drift")
        state.journal = quarantined

        prepared_delivery = state.prepared_delivery
        published_delivery = state.published_delivery
        prepared_request = state.request_record
        published_request = state.published_request
        if (
            prepared_delivery is None
            or published_delivery is None
            or prepared_request is None
            or published_request is None
        ):
            raise _state_conflict("publication safety lacks complete SQLite candidates")
        observed_delivery = self._ledger.get_delivery(prepared_delivery.delivery_id)
        observed_request = self._ledger.get_request(prepared_request.request_id)
        boundary = state.publication_boundary
        if boundary == "journal":
            delivery_exact = (
                type(observed_delivery) is DeliveryRecord and observed_delivery == prepared_delivery
            )
            request_exact = (
                type(observed_request) is RequestRecord and observed_request == prepared_request
            )
        elif boundary == "delivery_marker":
            delivery_exact = type(observed_delivery) is DeliveryRecord and observed_delivery in (
                prepared_delivery,
                published_delivery,
            )
            request_exact = (
                type(observed_request) is RequestRecord and observed_request == prepared_request
            )
        elif boundary == "request_published":
            delivery_exact = (
                type(observed_delivery) is DeliveryRecord
                and observed_delivery == published_delivery
            )
            request_exact = type(observed_request) is RequestRecord and observed_request in (
                prepared_request,
                published_request,
            )
        elif boundary == "waiter":
            delivery_exact = (
                type(observed_delivery) is DeliveryRecord
                and observed_delivery == published_delivery
            )
            request_exact = (
                type(observed_request) is RequestRecord and observed_request == published_request
            )
        else:
            raise _state_conflict("publication safety boundary is unknown")

        request_converged = False
        if request_exact and type(observed_request) is RequestRecord:
            payload = _canonical_json_text(_error_payload(primary))
            updated_at_ms = state.epochs.next(
                minimum=max(
                    quarantined.original.updated_at_ms,
                    observed_request.updated_at_ms,
                )
            )
            request_quarantine = _request_with(
                observed_request,
                state=HostRequestState.QUARANTINED,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=None,
                result_json=None,
                error_json=payload,
                resolution_json=None,
            )
            observed_quarantine = self._ledger.transition(
                observed_request.request_id,
                expected_states=frozenset({observed_request.state}),
                new_state=HostRequestState.QUARANTINED,
                updated_at_ms=updated_at_ms,
                error_json=payload,
            )
            request_converged = (
                type(observed_quarantine) is RequestRecord
                and observed_quarantine == request_quarantine
            )

        if not delivery_exact or not request_exact or not request_converged:
            raise _state_conflict("publication SQLite evidence did not converge exactly")
        state.safety_finished = True

    def _requires_fresh_session(self) -> bool:
        return self._poisoned
