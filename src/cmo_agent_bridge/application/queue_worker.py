"""Single-consumer FIFO worker for durable CMO operation queues."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, cast
from uuid import UUID

from pydantic import BaseModel, JsonValue

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.registry import OperationRegistry
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import ResponseArtifact
from cmo_agent_bridge.protocol.runtime import Sha256
from cmo_agent_bridge.state.models import HostRequestState
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.transports.file_bridge.models import (
    DurableBridgeChannel,
    RecoveryDisposition,
)
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo

from .queue_models import (
    QueueClock,
    QueueError,
    QueueStore,
    QueuedOperationRecord,
    QueuedOperationState,
    canonical_queue_json,
)


class QueueSessionTransport(Protocol):
    @property
    def root_key(self) -> Sha256: ...

    def worker_session(
        self,
        *,
        recovery_owner: Callable[[ProcessInfo], UUID | None],
    ) -> AbstractAsyncContextManager[DurableBridgeChannel]: ...


class _SystemClock:
    def now_ms(self) -> int:
        return time.time_ns() // 1_000_000


def _json_value(value: object) -> JsonValue:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", warnings="error")
    return cast(JsonValue, json.loads(canonical_queue_json(value)))


def _body(record: QueuedOperationRecord) -> RequestBody:
    return RequestBody.model_validate_json(record.body_json)


def _public_arguments(record: QueuedOperationRecord) -> Mapping[str, object]:
    parsed = json.loads(record.arguments_json)
    if type(parsed) is not dict:
        raise RuntimeError("queued public arguments are not an object")
    return cast(Mapping[str, object], parsed)


def _queue_error_json(error_json: str | None, *, fallback: ErrorCode, message: str) -> bytes:
    """Project a ledger ResponseError into the intentionally smaller public error."""
    if error_json is None:
        return canonical_queue_json(
            QueueError(code=fallback, message=message).model_dump(mode="json")
        )
    try:
        decoded: object = json.loads(error_json)
        if type(decoded) is not dict:
            raise ValueError("ledger error must be an object")
        parsed = cast(dict[object, object], decoded)
        projected = canonical_queue_json(
            {
                "code": parsed.get("code", fallback.value),
                "message": parsed.get("message", message),
                "details": parsed.get("details", {}),
            }
        )
        return canonical_queue_json(
            QueueError.model_validate_json(projected).model_dump(mode="json")
        )
    except (TypeError, ValueError):
        return canonical_queue_json(
            QueueError(code=fallback, message=message).model_dump(mode="json")
        )


class QueueWorker:
    """Run one durable mutation at a time, in persisted FIFO order.

    ``unbounded_wait=True`` is intentionally passed to the dedicated worker
    lane. It is not a wall-clock timeout: a request can remain published through an
    arbitrary CMO pause.  Worker cancellation leaves an ACTIVE entry untouched
    so startup recovery and the request ledger can establish its outcome.
    """

    def __init__(
        self,
        *,
        root_key: Sha256,
        registry: OperationRegistry,
        queue_store: QueueStore,
        transport: QueueSessionTransport,
        ledger: RequestLedger,
        clock: QueueClock | None = None,
        idle_poll_seconds: float = 0.25,
    ) -> None:
        if type(root_key) is not str:
            raise TypeError("queue root key must be an exact string")
        if type(registry) is not OperationRegistry:
            raise TypeError("operation registry must be exact")
        if type(ledger) is not RequestLedger:
            raise TypeError("queue worker ledger must be exact")
        if transport.root_key != root_key:
            raise ValueError("queue worker transport root key does not match queue root key")
        if idle_poll_seconds <= 0:
            raise ValueError("queue worker idle poll must be positive")
        self._root_key = root_key
        self._registry = registry
        self._queue_store = queue_store
        self._transport = transport
        self._ledger = ledger
        self._clock = _SystemClock() if clock is None else clock
        self._idle_poll_seconds = idle_poll_seconds
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def wake(self) -> None:
        self._wake.set()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="cmo-agent-bridge-queue-worker")
        self.wake()

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def run_once(self) -> bool:
        """Reconcile old evidence, then run exactly one FIFO head."""
        pending = self._queue_store.list(
            root_key=self._root_key,
            states=frozenset(
                {
                    QueuedOperationState.QUEUED,
                    QueuedOperationState.ACTIVE,
                }
            ),
        )
        if not pending:
            return False
        if self._has_unresolved_quarantine_barrier():
            return False
        try:
            async with self._transport.worker_session(
                recovery_owner=self._resolve_recovery_owner,
            ) as channel:
                report = channel.recovery_report
                self._synchronise_active(report)
                active = self._active_record()
                if active is not None:
                    return False
                if report.disposition is RecoveryDisposition.QUARANTINED:
                    # A durable mutation barrier owns the single CMO inbox.  Do
                    # not consume or reject later FIFO items until that barrier is
                    # resolved through the dedicated quarantine workflow.
                    return False
                record = self._queue_store.claim_next(
                    root_key=self._root_key, at_ms=self._clock.now_ms()
                )
                if record is None:
                    return False
                await self._run_claimed(channel, record)
                return True
        except BridgeError as error:
            if error.code is not ErrorCode.SCENARIO_CHANGED:
                raise
            return False

    async def _run(self) -> None:
        while True:
            try:
                ran = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Startup can race CMO launch, a root-lock holder, or the
                # bridge installation itself.  Preserve durable state and
                # retry; only an explicit task cancellation stops the worker.
                ran = False
            if ran:
                continue
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._idle_poll_seconds)
            except TimeoutError:
                pass

    def _resolve_recovery_owner(self, process: ProcessInfo) -> UUID | None:
        """Resolve the queue owner while the transport's root lock is held."""
        active = self._active_record()
        record = active or self._queue_store.head(root_key=self._root_key)
        if record is None:
            return None
        if (
            process.pid == record.expected_process_pid
            and process.create_time == record.expected_process_create_time
        ):
            return None if active is None else active.request_id

        message = "the running CMO process does not match the process bound at queue submission"
        details: dict[str, JsonValue] = {
            "expected_pid": record.expected_process_pid,
            "expected_create_time": record.expected_process_create_time,
            "actual_pid": process.pid,
            "actual_create_time": process.create_time,
            "request_id": str(record.request_id),
        }
        if active is None:
            self._queue_store.reject_queued(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.SCENARIO_CHANGED,
                        message=message,
                        details=details,
                    ).model_dump(mode="json")
                ),
                at_ms=self._clock.now_ms(),
            )
        else:
            error = QueueError(
                code=ErrorCode.INDETERMINATE_OUTCOME,
                message=message,
                details=details,
            )
            # Do not mutate a published journal/ledger here.  The process
            # identity mismatch means this worker has no authority to decide
            # whether the old CMO executed it; specialised recovery/manual
            # resolution retains that evidence.
            self._queue_store.quarantine(
                record.request_id,
                canonical_queue_json(error.model_dump(mode="json")),
                at_ms=self._clock.now_ms(),
            )
        raise BridgeError(ErrorCode.SCENARIO_CHANGED, message, details)

    def _has_unresolved_quarantine_barrier(self) -> bool:
        quarantined = self._queue_store.list(
            root_key=self._root_key,
            states=frozenset({QueuedOperationState.QUARANTINED}),
        )
        for record in quarantined:
            ledger = self._ledger.get_request(record.request_id)
            if ledger is not None and ledger.state not in {
                HostRequestState.COMPLETED,
                HostRequestState.REJECTED,
                HostRequestState.CANCELLED,
                HostRequestState.RESOLVED,
            }:
                return True
        return False

    def _active_record(self) -> QueuedOperationRecord | None:
        active = self._queue_store.list(
            root_key=self._root_key, states=frozenset({QueuedOperationState.ACTIVE})
        )
        if len(active) > 1:
            raise RuntimeError("durable queue has more than one active operation")
        return None if not active else active[0]

    def _synchronise_active(self, report: object) -> None:
        active = self._active_record()
        if active is None:
            return
        ledger_record = self._ledger.get_request(active.request_id)
        if ledger_record is not None and ledger_record.state in {
            HostRequestState.COMPLETED,
            HostRequestState.REJECTED,
            HostRequestState.QUARANTINED,
            HostRequestState.CANCELLED,
            HostRequestState.RESOLVED,
        }:
            self._settle_from_ledger(active, ledger_record)
            return
        # No request ledger record means the former worker crashed before a
        # durable publication; it is safe to return the exact FIFO head.
        if ledger_record is None:
            self._queue_store.reset_active_before_publication(
                active.request_id, at_ms=self._clock.now_ms()
            )
            return
        disposition = getattr(report, "disposition", None)
        if disposition is RecoveryDisposition.QUARANTINED:
            self._queue_store.quarantine(
                active.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.INDETERMINATE_OUTCOME,
                        message="published queue operation could not be recovered safely",
                    ).model_dump(mode="json")
                ),
                at_ms=self._clock.now_ms(),
            )

    def _settle_from_ledger(self, active: QueuedOperationRecord, ledger: RequestRecord) -> None:
        at_ms = self._clock.now_ms()
        if ledger.state is HostRequestState.COMPLETED and ledger.result_json is not None:
            try:
                result = self._public_result(active, json.loads(ledger.result_json))
            except Exception as error:
                self._queue_store.quarantine(
                    active.request_id,
                    canonical_queue_json(
                        QueueError(
                            code=ErrorCode.PROTOCOL_ERROR,
                            message="recovered CMO result failed public result validation",
                            details={"exception": type(error).__name__},
                        ).model_dump(mode="json")
                    ),
                    at_ms=at_ms,
                )
                return
            self._queue_store.complete(active.request_id, canonical_queue_json(result), at_ms=at_ms)
            return
        if ledger.state is HostRequestState.REJECTED and ledger.error_json is not None:
            self._queue_store.reject(
                active.request_id,
                _queue_error_json(
                    ledger.error_json,
                    fallback=ErrorCode.PROTOCOL_ERROR,
                    message="recovered queue operation was rejected by CMO",
                ),
                at_ms=at_ms,
            )
            return
        if ledger.state is HostRequestState.CANCELLED:
            # Queue cancellation occurs before publication; a ledger-side
            # cancellation therefore has no public result and is a rejection.
            self._queue_store.reject(
                active.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.PROTOCOL_ERROR, message="published operation was cancelled"
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )
            return
        self._queue_store.quarantine(
            active.request_id,
            canonical_queue_json(
                QueueError(
                    code=ErrorCode.INDETERMINATE_OUTCOME,
                    message="recovered queue operation has no definitive terminal result",
                ).model_dump(mode="json")
            ),
            at_ms=at_ms,
        )

    async def _run_claimed(
        self,
        channel: DurableBridgeChannel,
        record: QueuedOperationRecord,
    ) -> None:
        try:
            body = _body(record)
            invocation = self._registry.resolve_invocation(
                record.operation, _public_arguments(record)
            )
            if invocation.wire_arguments.model_dump(mode="json") != body.arguments:
                raise RuntimeError(
                    "queued public arguments no longer resolve to the persisted wire arguments"
                )
            command = ExchangeCommand(
                request_id=record.request_id,
                body=body,
                invocation=invocation,
                runtime_snapshot=record.runtime_snapshot,
                timeout=0.0,
                unbounded_wait=True,
            )
            artifact = await channel.exchange(command)
        except asyncio.CancelledError:
            # Preserve ACTIVE state: the next worker synchronises with ledger.
            raise
        except BridgeError as error:
            self._map_bridge_error(record, error)
            return
        except Exception as error:
            self._queue_store.reject(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.PROTOCOL_ERROR,
                        message="queue worker failed before a CMO response was accepted",
                        details={"exception": type(error).__name__},
                    ).model_dump(mode="json")
                ),
                at_ms=self._clock.now_ms(),
            )
            return
        self._settle_artifact(record, artifact)

    def _public_result(self, record: QueuedOperationRecord, wire_result: object) -> JsonValue:
        invocation = self._registry.resolve_invocation(record.operation, _public_arguments(record))
        return _json_value(invocation.public_result_adapter.validate_python(wire_result))

    def _settle_artifact(self, record: QueuedOperationRecord, artifact: ResponseArtifact) -> None:
        accepted = artifact.accepted_response
        at_ms = self._clock.now_ms()
        if accepted.envelope.request_id != record.request_id:
            self._queue_store.quarantine(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.PROTOCOL_ERROR,
                        message="queue exchange returned a response for another request",
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )
            return
        if not accepted.ok:
            error = accepted.error
            if error is None:
                self._queue_store.quarantine(
                    record.request_id,
                    canonical_queue_json(
                        QueueError(
                            code=ErrorCode.PROTOCOL_ERROR, message="failed response lacked error"
                        ).model_dump(mode="json")
                    ),
                    at_ms=at_ms,
                )
                return
            self._queue_store.reject(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=error.code, message=error.message, details=error.details
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )
            return
        result = accepted.result
        if result is None:
            self._queue_store.quarantine(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.PROTOCOL_ERROR, message="successful response lacked result"
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )
            return
        try:
            self._queue_store.complete(
                record.request_id,
                canonical_queue_json(self._public_result(record, result)),
                at_ms=at_ms,
            )
        except Exception as error:
            self._queue_store.quarantine(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=ErrorCode.PROTOCOL_ERROR,
                        message="CMO response did not match the queued operation result schema",
                        details={"exception": type(error).__name__},
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )

    def _map_bridge_error(self, record: QueuedOperationRecord, error: BridgeError) -> None:
        at_ms = self._clock.now_ms()
        # A durable exchange can raise while the journal/ledger already owns a
        # published request.  That evidence outranks a host-side exception: a
        # retry would risk duplicating the mutation.
        ledger = self._ledger.get_request(record.request_id)
        if ledger is not None:
            if ledger.state in {
                HostRequestState.COMPLETED,
                HostRequestState.REJECTED,
                HostRequestState.QUARANTINED,
                HostRequestState.CANCELLED,
                HostRequestState.RESOLVED,
            }:
                self._settle_from_ledger(record, ledger)
                return
            if ledger.state in {
                HostRequestState.PREPARED,
                HostRequestState.PUBLISHED,
                HostRequestState.CANCEL_PUBLISHED,
                HostRequestState.RESPONSE_ACCEPTED,
                HostRequestState.IDLE_PUBLISHED,
            }:
                # Keep ACTIVE.  The next durable worker session will recover
                # this exact request and synchronise a terminal ledger state.
                return
        if error.code in {ErrorCode.MUTATION_QUARANTINED, ErrorCode.INDETERMINATE_OUTCOME}:
            self._queue_store.quarantine(
                record.request_id,
                canonical_queue_json(
                    QueueError(
                        code=error.code, message=error.message, details=error.details
                    ).model_dump(mode="json")
                ),
                at_ms=at_ms,
            )
            return
        self._queue_store.reject(
            record.request_id,
            canonical_queue_json(
                QueueError(
                    code=error.code, message=error.message, details=error.details
                ).model_dump(mode="json")
            ),
            at_ms=at_ms,
        )
