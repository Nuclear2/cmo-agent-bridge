from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import UUID

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.registry import FrozenInvocation
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes, prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.models import AllowedDelivery, PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    CancelledSettlement,
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    HostRequestState,
    PendingExchange,
    PendingJournal,
    PendingPhase,
)
from cmo_agent_bridge.state.host_resolution import (
    marker_matches_pending_journal,
    parse_host_quarantine_resolution,
)
from cmo_agent_bridge.state.pending_journal import (
    HostResolvedJournalDeleteExpectation,
    JournalDeleteExpectation,
    JournalRevisions,
    PendingJournalStore,
)
from cmo_agent_bridge.state.revalidation import LoadedPendingJournal
from cmo_agent_bridge.state.request_ledger import DeliveryRecord, RequestLedger, RequestRecord
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.artifact_binding import bind_durable_response
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.models import (
    RecoveryDisposition,
    RecoveryReport,
)
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    CmoProcessInspector,
    ProcessInfo,
    require_single_instance,
)
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


def _canonical_json_text(value: object) -> str:
    return _canonical_json_bytes(value).decode("utf-8")


def _validated_number(
    value: object,
    *,
    description: str,
) -> float:
    if type(value) not in {int, float}:
        raise _invalid_argument(f"{description} must be an exact int or float")
    try:
        result = float(cast(int | float, value))
    except OverflowError as error:
        raise _invalid_argument(f"{description} must be finite") from error
    if not math.isfinite(result) or result <= 0:
        raise _invalid_argument(f"{description} must be finite and strictly positive")
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


def _same_journal(left: PendingJournal, right: PendingJournal) -> bool:
    return _canonical_json_bytes(left.model_dump(mode="json")) == _canonical_json_bytes(
        right.model_dump(mode="json")
    )


def _journal_with_original(journal: PendingJournal, **changes: object) -> PendingJournal:
    tree = journal.original.model_dump(
        mode="python",
        round_trip=True,
        warnings=False,
    )
    tree.update(changes)
    return PendingJournal(
        header=journal.header,
        original=PendingExchange.model_validate(tree),
        reconcile_attempt=None,
    )


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


def _request_with(record: RequestRecord, **changes: object) -> RequestRecord:
    tree = record.model_dump(mode="python", round_trip=True, warnings=False)
    tree.update(changes)
    return RequestRecord.model_validate(tree)


def _fresh_cancel_delivery_id(*, request_id: UUID, existing: frozenset[UUID]) -> UUID:
    delivery_id = uuid.uuid4()
    if (
        type(delivery_id) is not UUID
        or delivery_id.version != 4
        or delivery_id == request_id
        or delivery_id in existing
    ):
        raise _state_conflict("UUID generator returned an invalid cancel delivery ID")
    return delivery_id


class _EpochSequence:
    def __init__(self) -> None:
        self._last = 0

    def next(self, *, minimum: int = 0) -> int:
        observed = time.time_ns() // 1_000_000
        value = max(observed, self._last, minimum, 0)
        self._last = value
        return value


@dataclass(frozen=True, slots=True)
class _TerminalOutcome:
    state: HostRequestState
    result_json: str | None
    error_json: str | None


class RecoveryManager:
    def __init__(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        process_inspector: CmoProcessInspector,
        expected_process: ProcessInfo,
        journals: PendingJournalStore,
        ledger: RequestLedger,
        inbox: InboxPublisher,
        response_poll_seconds: float,
        cancel_ack_timeout_seconds: float,
    ) -> None:
        if type(paths) is not FileBridgePaths:
            raise _invalid_argument("recovery paths must be an exact FileBridgePaths")
        if type(root_lock) is not RootLock:
            raise _invalid_argument("recovery root lock must be an exact RootLock")
        if type(journals) is not PendingJournalStore:
            raise _invalid_argument("recovery journals must be an exact PendingJournalStore")
        if type(ledger) is not RequestLedger:
            raise _invalid_argument("recovery ledger must be an exact RequestLedger")
        if type(inbox) is not InboxPublisher:
            raise _invalid_argument("recovery inbox must be an exact InboxPublisher")
        if root_lock.path != paths.lock_file:
            raise _invalid_argument("recovery root lock does not belong to the bridge root")
        if not _has_synchronous_process_method(process_inspector):
            raise _invalid_argument(
                "recovery process inspector must expose synchronous matching_processes"
            )
        if not _valid_process(expected_process):
            raise _invalid_argument("recovery expected process identity is invalid")
        if expected_process.executable != paths.command_exe:
            raise _invalid_argument("recovery expected process does not belong to the bridge root")
        if inbox.paths is not paths:
            raise _invalid_argument("recovery inbox is not bound to the bridge paths")

        journal_paths = cast(object, getattr(journals, "_paths", None))
        journal_lock = cast(object, getattr(journals, "_root_lock", None))
        journal_catalog = cast(object, getattr(journals, "_catalog", None))
        database = cast(object, getattr(ledger, "_database", None))
        ledger_catalog = cast(object, getattr(ledger, "_catalog", None))
        if journal_paths is not paths or journal_lock is not root_lock:
            raise _invalid_argument("recovery journal store binding is inconsistent")
        if (
            type(database) is not StateDatabase
            or database.path != paths.sqlite_file
            or ledger_catalog is not journal_catalog
        ):
            raise _invalid_argument("recovery request ledger binding is inconsistent")

        poll_seconds = _validated_number(
            response_poll_seconds,
            description="recovery response poll seconds",
        )
        cancel_timeout_seconds = _validated_number(
            cancel_ack_timeout_seconds,
            description="recovery cancel acknowledgement timeout seconds",
        )

        self._paths = paths
        self._root_lock = root_lock
        self._process_inspector = process_inspector
        self._expected_process = expected_process
        self._journals = journals
        self._ledger = ledger
        self._inbox = inbox
        self._response_poll_seconds = poll_seconds
        self._cancel_ack_timeout_seconds = cancel_timeout_seconds

    def _is_bound_to(
        self,
        *,
        paths: FileBridgePaths,
        root_lock: RootLock,
        process_inspector: CmoProcessInspector,
        expected_process: ProcessInfo,
        journals: PendingJournalStore,
        ledger: RequestLedger,
        inbox: InboxPublisher,
        response_poll_seconds: float,
    ) -> bool:
        return (
            self._paths is paths
            and self._root_lock is root_lock
            and self._process_inspector is process_inspector
            and self._expected_process == expected_process
            and self._journals is journals
            and self._ledger is ledger
            and self._inbox is inbox
            and self._response_poll_seconds == response_poll_seconds
        )

    async def recover_pending(self) -> RecoveryReport:
        """Converge the one durable mutation barrier before new work is published."""

        self._root_lock.require_acquired()
        loaded = self._load_journal_observed()
        if loaded is None:
            return RecoveryReport(
                disposition=RecoveryDisposition.NO_PENDING,
                request_id=None,
                request_state=None,
                response_cleanup_required=False,
            )
        if loaded.journal.reconcile_attempt is not None or loaded.reconcile_attempt is not None:
            raise _state_conflict(
                "startup recovery cannot settle a reconciliation attempt before H11"
            )

        journal = loaded.journal
        original = journal.original
        if original.state is PendingPhase.PREPARED:
            return await self._recover_prepared(loaded)
        if original.state is PendingPhase.PUBLISHED:
            return await self._recover_published(loaded)
        if original.state is PendingPhase.CANCEL_PUBLISHED:
            cancel_intents = tuple(
                intent for intent in original.delivery_intents if intent.delivery_kind == "cancel"
            )
            if len(cancel_intents) != 1:
                raise _state_conflict("cancel recovery journal lacks one exact cancel intent")
            if cancel_intents[0].published_at_ms is None:
                await self._continue_cancel_intent(
                    journal,
                    loaded.original.invocation,
                )
            else:
                await self._continue_published_cancel(
                    journal,
                    loaded.original.invocation,
                )
            return self._report_after_recovery(
                original.request_id,
                expected_original=original,
            )
        if original.state is PendingPhase.RESPONSE_ACCEPTED:
            return self._recover_response_accepted(loaded)
        if original.state is PendingPhase.IDLE_PUBLISHED:
            return self._recover_idle_published(loaded)
        if original.state is PendingPhase.QUARANTINED:
            return self._recover_quarantined(loaded)
        raise _state_conflict("startup recovery encountered an unsupported pending phase")

    def _recover_quarantined(self, loaded: LoadedPendingJournal) -> RecoveryReport:
        journal = loaded.journal
        original = journal.original
        request = self._get_request_observed(original.request_id)
        if request is not None and request.state is HostRequestState.RESOLVED:
            return self._finish_host_resolved_quarantine(journal, request)
        if request is None:
            self._ledger.insert_prepared(self._prepared_request(journal))
            request = self._get_request_observed(original.request_id)
        if request is None or not self._request_identity_matches(request, original):
            raise _state_conflict("quarantined journal lacks exact request identity evidence")
        artifact = original.response_artifact
        responding_delivery_id = (
            None if artifact is None else artifact.accepted_response.envelope.delivery_id
        )
        for intent in original.delivery_intents:
            expected = _delivery_from_intent(intent)
            observed = self._get_delivery_observed(intent.delivery_id)
            if observed is None:
                self._ledger.insert_delivery(intent)
                observed = self._get_delivery_observed(intent.delivery_id)
            accepted = (expected,)
            if artifact is not None and intent.delivery_id == responding_delivery_id:
                response_target = expected.model_copy(
                    update={
                        "response_artifact": artifact,
                        "settlement": original.settlement,
                    }
                )
                if observed == expected:
                    try:
                        bound = bind_durable_response(
                            self._paths.response_path(original.request_id),
                            artifact,
                            loaded.original.expectation,
                            parser=parse_inst_response,
                        )
                    except BridgeError:
                        pass
                    else:
                        self._converge_response_record(
                            bound,
                            expected,
                            response_target,
                        )
                        observed = self._get_delivery_observed(intent.delivery_id)
                accepted = (
                    expected,
                    response_target,
                )
            if observed not in accepted:
                raise _state_conflict("quarantined delivery ledger evidence drifted")
        if request.state is not HostRequestState.QUARANTINED:
            updated_at_ms = max(original.updated_at_ms, request.updated_at_ms)
            error_json = _canonical_json_text(
                {
                    "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                    "details": {
                        "reason": "startup_quarantine_convergence",
                        "request_id": str(original.request_id),
                    },
                    "message": "durable mutation quarantine required ledger convergence",
                }
            )
            target = _request_with(
                request,
                state=HostRequestState.QUARANTINED,
                updated_at_ms=updated_at_ms,
                terminal_at_ms=None,
                result_json=None,
                error_json=error_json,
                resolution_json=None,
            )
            request = self._converge_request_transition(request, target)
        if request.state is not HostRequestState.QUARANTINED:
            raise _state_conflict("quarantined request ledger convergence failed")
        return RecoveryReport(
            disposition=RecoveryDisposition.QUARANTINED,
            request_id=original.request_id,
            request_state=HostRequestState.QUARANTINED,
            response_cleanup_required=False,
        )

    def _finish_host_resolved_quarantine(
        self,
        journal: PendingJournal,
        request: RequestRecord,
    ) -> RecoveryReport:
        original = journal.original
        resolution_json = request.resolution_json
        try:
            if resolution_json is None:
                raise ValueError("resolved request lacks resolution JSON")
            marker = parse_host_quarantine_resolution(resolution_json)
        except (TypeError, ValidationError, ValueError) as error:
            raise _state_conflict(
                "resolved quarantined request lacks a valid Host-only resolution marker"
            ) from error
        if (
            journal.reconcile_attempt is not None
            or original.state is not PendingPhase.QUARANTINED
            or not self._request_identity_matches(request, original)
            or not marker_matches_pending_journal(marker, journal)
            or request.updated_at_ms != marker.resolved_at_ms
            or request.terminal_at_ms != marker.resolved_at_ms
        ):
            raise _state_conflict(
                "Host-only resolved quarantine evidence does not match its journal"
            )
        self._inbox.publish_idle()
        self._journals.delete_host_resolved(
            HostResolvedJournalDeleteExpectation(
                root_key=journal.header.root_key,
                required_release_id=journal.header.required_release_id,
                original_request_id=original.request_id,
                original_request_hash=original.request_hash,
                original_revision=original.revision,
            )
        )
        return RecoveryReport(
            disposition=RecoveryDisposition.SETTLED,
            request_id=original.request_id,
            request_state=HostRequestState.RESOLVED,
            response_cleanup_required=True,
        )

    def _request_identity_matches(
        self,
        request: RequestRecord,
        original: PendingExchange,
    ) -> bool:
        return (
            request.request_id == original.request_id
            and request.root_key == self._paths.root_key
            and request.request_hash == original.request_hash
            and request.operation == original.operation
            and request.operation_class is original.effective_class
            and request.runtime_snapshot == original.runtime_snapshot
            and request.result_schema_id == original.result_schema_id
            and request.recovery_schema_id == original.recovery_schema_id
            and request.body_json == original.body_json.encode("utf-8")
            and request.lineage_id == original.expected_lineage_id
            and request.activation_id == original.expected_activation_id
            and request.created_at_ms == original.created_at_ms
        )

    async def _recover_prepared(self, loaded: LoadedPendingJournal) -> RecoveryReport:
        journal = loaded.journal
        original = journal.original
        if original.state is not PendingPhase.PREPARED or len(original.delivery_intents) != 1:
            raise _state_conflict("prepared startup recovery journal is inconsistent")
        intent = original.delivery_intents[0]
        if intent.delivery_kind != "request" or intent.published_at_ms is not None:
            raise _state_conflict("prepared startup request intent is inconsistent")

        request = self._prepared_request(journal)
        rejected = self._startup_failed_before_publish_request(journal, request)
        delivery = _delivery_from_intent(intent)
        observed_request = self._get_request_observed(original.request_id)
        if observed_request is None:
            self._ledger.insert_prepared(request)
            observed_request = self._get_request_observed(original.request_id)
        already_rejected = observed_request == rejected
        if observed_request != request and not already_rejected:
            raise _state_conflict("prepared startup request ledger evidence drifted")
        observed_delivery = self._get_delivery_observed(intent.delivery_id)
        if observed_delivery is None:
            self._ledger.insert_delivery(intent)
            observed_delivery = self._get_delivery_observed(intent.delivery_id)
        if observed_delivery != delivery:
            raise _state_conflict("prepared startup delivery ledger evidence drifted")

        response_path = self._paths.response_path(original.request_id)
        inbox = self._read_inbox_observed()
        try:
            response_absent = self._response_file_absent(response_path)
        except BridgeError as error:
            if already_rejected:
                raise
            published = self._promote_prepared(journal, request, intent)
            published_request = self._get_request_observed(original.request_id)
            if published_request is None:
                raise _state_conflict("promoted startup request disappeared")
            self._quarantine_pending(
                published,
                published_request,
                reason=str(error.details.get("reason", "prepared_response_observation_failed")),
                message="prepared response evidence could not be observed safely",
            )
            return self._report_after_recovery(
                original.request_id,
                expected_original=original,
            )
        if response_absent and inbox == render_idle_lua():
            if not self._response_file_absent(response_path):
                response_absent = False
        if response_absent and inbox == render_idle_lua():
            if already_rejected:
                if not self._response_file_absent(response_path):
                    raise _state_conflict(
                        "prepared response appeared before rejected journal delete"
                    )
                self._converge_journal_delete(journal)
                return RecoveryReport(
                    disposition=RecoveryDisposition.FAILED_BEFORE_PUBLISH,
                    request_id=original.request_id,
                    request_state=HostRequestState.REJECTED,
                    response_cleanup_required=False,
                )
            self._converge_request_transition(request, rejected)
            if not self._response_file_absent(response_path):
                raise _state_conflict("prepared response appeared after rejection transition")
            self._converge_journal_delete(journal)
            return RecoveryReport(
                disposition=RecoveryDisposition.FAILED_BEFORE_PUBLISH,
                request_id=original.request_id,
                request_state=HostRequestState.REJECTED,
                response_cleanup_required=False,
            )

        if already_rejected:
            raise _state_conflict(
                "prepared rejected request gained conflicting publication evidence"
            )

        published = self._promote_prepared(journal, request, intent)
        reloaded = self._load_journal_observed()
        if reloaded is None or not _same_journal(reloaded.journal, published):
            raise _state_conflict("promoted startup journal changed before cancellation")
        return await self._recover_published(reloaded)

    @staticmethod
    def _response_file_absent(path: Path) -> bool:
        try:
            with path.open("rb") as stream:
                stream.read(1)
        except FileNotFoundError:
            return True
        except OSError as error:
            failure = BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "prepared response file could not be observed safely",
                {
                    "reason": "response_observation_failed",
                    "response_path": str(path),
                },
            )
            failure.__cause__ = error
            raise failure
        return False

    async def _recover_published(self, loaded: LoadedPendingJournal) -> RecoveryReport:
        journal = loaded.journal
        request = self._converge_published_sqlite(journal)
        try:
            artifact = self._probe_startup_response(loaded)
        except BridgeError as error:
            self._quarantine_pending(
                journal,
                request,
                reason=str(error.details.get("reason", "published_response_invalid")),
                message="published mutation response failed startup validation",
            )
            return self._report_after_recovery(
                journal.original.request_id,
                expected_original=journal.original,
            )
        if artifact is not None:
            accepted_at_ms = _EpochSequence().next(
                minimum=max(journal.original.updated_at_ms, artifact.accepted_at_ms)
            )
            response_accepted = _journal_with_original(
                journal,
                response_artifact=artifact,
                settlement=artifact.accepted_response.settlement,
                revision=journal.original.revision + 1,
                state=PendingPhase.RESPONSE_ACCEPTED,
                updated_at_ms=accepted_at_ms,
            )
            self._save_journal_advance(
                journal,
                response_accepted,
                label="startup original response acceptance",
            )
            self._continue_response_accepted(
                response_accepted,
                invocation=loaded.original.invocation,
                epochs=_EpochSequence(),
            )
            return self._report_after_recovery(
                journal.original.request_id,
                expected_original=journal.original,
            )

        intent = journal.original.delivery_intents[0]
        rendered_request = self._render_intent(intent)
        inbox = self._read_inbox_observed()
        if inbox != rendered_request:
            self._quarantine_pending(
                journal,
                request,
                reason="published_inbox_evidence_missing",
                message="published mutation no longer owns the CMO inbox",
            )
            return self._report_after_recovery(
                journal.original.request_id,
                expected_original=journal.original,
            )

        await self.settle_published_cancellation(journal)
        return self._report_after_recovery(
            journal.original.request_id,
            expected_original=journal.original,
        )

    def _probe_startup_response(
        self,
        loaded: LoadedPendingJournal,
    ) -> ResponseArtifact | None:
        request_id = loaded.journal.original.request_id
        path = self._paths.response_path(request_id)
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        except OSError as error:
            failure = BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "startup response file could not be read",
                {
                    "reason": "response_read_failed",
                    "request_id": str(request_id),
                    "response_path": str(path),
                },
            )
            failure.__cause__ = error
            raise failure
        try:
            accepted = parse_inst_response(raw, loaded.original.expectation)
        except BridgeError as error:
            failure = BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "startup response file failed protocol validation",
                {
                    "reason": "response_parse_failed",
                    "request_id": str(request_id),
                    "response_path": str(path),
                },
            )
            failure.__cause__ = error
            raise failure
        accepted_at_ms = _EpochSequence().next(minimum=loaded.journal.original.updated_at_ms)
        return ResponseArtifact(
            filename=path.name,
            sha256=hashlib.sha256(raw).hexdigest(),
            size_bytes=len(raw),
            accepted_at_ms=accepted_at_ms,
            accepted_response=accepted,
        )

    def _prepared_request(self, journal: PendingJournal) -> RequestRecord:
        original = journal.original
        return RequestRecord(
            request_id=original.request_id,
            root_key=self._paths.root_key,
            request_hash=original.request_hash,
            operation=original.operation,
            operation_class=original.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=original.runtime_snapshot,
            result_schema_id=original.result_schema_id,
            recovery_schema_id=original.recovery_schema_id,
            body_json=original.body_json.encode("utf-8", errors="strict"),
            lineage_id=original.expected_lineage_id,
            activation_id=original.expected_activation_id,
            result_json=None,
            error_json=None,
            resolution_json=None,
            created_at_ms=original.created_at_ms,
            updated_at_ms=original.created_at_ms,
            terminal_at_ms=None,
        )

    @staticmethod
    def _startup_failed_before_publish_request(
        journal: PendingJournal,
        request: RequestRecord,
    ) -> RequestRecord:
        rejected_at_ms = max(journal.original.updated_at_ms, request.updated_at_ms)
        payload = _canonical_json_text(
            {
                "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                "details": {
                    "reason": "startup_failed_before_publish",
                    "request_id": str(journal.original.request_id),
                },
                "message": "mutation did not reach request publication",
            }
        )
        return _request_with(
            request,
            state=HostRequestState.REJECTED,
            updated_at_ms=rejected_at_ms,
            terminal_at_ms=rejected_at_ms,
            result_json=None,
            error_json=payload,
            resolution_json=None,
        )

    def _promote_prepared(
        self,
        journal: PendingJournal,
        request: RequestRecord,
        intent: DeliveryIntent,
    ) -> PendingJournal:
        published_at_ms = _EpochSequence().next(minimum=journal.original.updated_at_ms)
        published_intent = DeliveryIntent.model_validate(
            {
                **intent.model_dump(mode="python", round_trip=True, warnings=False),
                "published_at_ms": published_at_ms,
            }
        )
        published = _journal_with_original(
            journal,
            delivery_intents=(published_intent,),
            revision=journal.original.revision + 1,
            state=PendingPhase.PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        self._save_journal_advance(journal, published, label="startup request publication")
        self._converge_delivery_publication(
            _delivery_from_intent(intent),
            _delivery_from_intent(published_intent),
        )
        published_request = _request_with(
            request,
            state=HostRequestState.PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        self._converge_request_transition(request, published_request)
        return published

    def _converge_published_sqlite(self, journal: PendingJournal) -> RequestRecord:
        original = journal.original
        if original.state is not PendingPhase.PUBLISHED or len(original.delivery_intents) != 1:
            raise _state_conflict("published startup journal is inconsistent")
        intent = original.delivery_intents[0]
        if intent.delivery_kind != "request" or intent.published_at_ms is None:
            raise _state_conflict("published startup request intent is inconsistent")
        prepared_intent = DeliveryIntent.model_validate(
            {
                **intent.model_dump(mode="python", round_trip=True, warnings=False),
                "published_at_ms": None,
            }
        )
        prepared_delivery = _delivery_from_intent(prepared_intent)
        published_delivery = _delivery_from_intent(intent)
        observed_delivery = self._get_delivery_observed(intent.delivery_id)
        if observed_delivery == prepared_delivery:
            observed_delivery = self._converge_delivery_publication(
                prepared_delivery,
                published_delivery,
            )
        if observed_delivery != published_delivery:
            raise _state_conflict("published startup delivery ledger evidence drifted")

        prepared_request = self._prepared_request(
            _journal_with_original(
                journal,
                delivery_intents=(prepared_intent,),
                revision=0,
                state=PendingPhase.PREPARED,
                updated_at_ms=original.created_at_ms,
            )
        )
        published_request = _request_with(
            prepared_request,
            state=HostRequestState.PUBLISHED,
            updated_at_ms=original.updated_at_ms,
        )
        observed_request = self._get_request_observed(original.request_id)
        if observed_request == prepared_request:
            observed_request = self._converge_request_transition(
                prepared_request,
                published_request,
            )
        if observed_request != published_request:
            raise _state_conflict("published startup request ledger evidence drifted")
        return published_request

    def _quarantine_pending(
        self,
        journal: PendingJournal,
        request: RequestRecord,
        *,
        reason: str,
        message: str,
    ) -> PendingJournal:
        quarantine_at_ms = _EpochSequence().next(
            minimum=max(journal.original.updated_at_ms, request.updated_at_ms)
        )
        quarantined = _journal_with_original(
            journal,
            revision=journal.original.revision + 1,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=quarantine_at_ms,
        )
        self._save_journal_advance(journal, quarantined, label="startup quarantine")
        error_json = _canonical_json_text(
            {
                "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                "details": {
                    "reason": reason,
                    "request_id": str(journal.original.request_id),
                },
                "message": message,
            }
        )
        target = _request_with(
            request,
            state=HostRequestState.QUARANTINED,
            updated_at_ms=quarantine_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=error_json,
            resolution_json=None,
        )
        self._converge_request_transition(request, target)
        return quarantined

    def _report_after_recovery(
        self,
        request_id: UUID,
        *,
        expected_original: PendingExchange | None = None,
    ) -> RecoveryReport:
        loaded = self._load_journal_observed()
        request = self._get_request_observed(request_id)
        identity = expected_original
        if identity is None and loaded is not None:
            identity = loaded.journal.original
        if (
            request is not None
            and identity is not None
            and not self._request_identity_matches(
                request,
                identity,
            )
        ):
            raise _state_conflict("startup recovery report request identity drifted")
        if loaded is not None:
            if (
                loaded.journal.original.request_id == request_id
                and loaded.journal.original.state is PendingPhase.QUARANTINED
                and request is not None
                and request.state is HostRequestState.QUARANTINED
            ):
                return RecoveryReport(
                    disposition=RecoveryDisposition.QUARANTINED,
                    request_id=request_id,
                    request_state=HostRequestState.QUARANTINED,
                    response_cleanup_required=False,
                )
            raise _state_conflict("startup recovery left a nonterminal pending barrier")
        if (
            request is None
            or request.state
            not in {
                HostRequestState.COMPLETED,
                HostRequestState.CANCELLED,
                HostRequestState.REJECTED,
            }
            or request.terminal_at_ms is None
        ):
            raise _state_conflict("startup recovery lacks exact terminal ledger evidence")
        return RecoveryReport(
            disposition=RecoveryDisposition.SETTLED,
            request_id=request_id,
            request_state=request.state,
            response_cleanup_required=True,
        )

    async def settle_published_cancellation(self, expected_journal: PendingJournal) -> None:
        self._root_lock.require_acquired()
        journal, request, request_intent, request_delivery = self._load_exact_published(
            expected_journal
        )
        epochs = _EpochSequence()

        body = self._rebuild_body(journal.original)
        cancel_delivery_id = _fresh_cancel_delivery_id(
            request_id=journal.original.request_id,
            existing=frozenset(intent.delivery_id for intent in journal.original.delivery_intents),
        )
        cancel_delivery = prepare_delivery(
            body,
            request_id=journal.original.request_id,
            delivery_id=cancel_delivery_id,
            delivery_kind="cancel",
        )
        expected_body = journal.original.body_json.encode("utf-8")
        if (
            cancel_delivery.body_json != expected_body
            or cancel_delivery.request_hash != journal.original.request_hash
        ):
            raise _state_conflict("cancel delivery differs from the durable request identity")
        original_delivery = PreparedDelivery(
            request_id=request_intent.request_id,
            delivery_id=request_intent.delivery_id,
            delivery_kind="request",
            request_hash=request_intent.request_hash,
            body_json=request_intent.body_json.encode("utf-8", errors="strict"),
        )
        rendered_original = render_delivery_lua(
            original_delivery,
            journal.original.runtime_snapshot,
        )
        if (
            hashlib.sha256(rendered_original).hexdigest() != request_intent.rendered_inbox_sha256
            or len(rendered_original) != request_intent.rendered_inbox_size_bytes
        ):
            raise _state_conflict("published request rendering differs from its durable intent")
        rendered_cancel = render_delivery_lua(cancel_delivery, journal.original.runtime_snapshot)
        intended_at_ms = epochs.next(
            minimum=max(
                journal.original.updated_at_ms,
                request.updated_at_ms,
                request_intent.published_at_ms or 0,
                request_delivery.published_at_ms or 0,
            )
        )
        cancel_intent = DeliveryIntent(
            request_id=journal.original.request_id,
            delivery_id=cancel_delivery.delivery_id,
            delivery_kind="cancel",
            original_request_delivery_id=request_intent.delivery_id,
            body_json=cancel_delivery.body_json.decode("utf-8", errors="strict"),
            request_hash=cancel_delivery.request_hash,
            runtime_snapshot=journal.original.runtime_snapshot,
            result_schema_id=journal.original.result_schema_id,
            recovery_schema_id=journal.original.recovery_schema_id,
            intended_at_ms=intended_at_ms,
            published_at_ms=None,
            rendered_inbox_sha256=hashlib.sha256(rendered_cancel).hexdigest(),
            rendered_inbox_size_bytes=len(rendered_cancel),
            response_filename=self._paths.response_path(journal.original.request_id).name,
        )
        cancel_intended = _journal_with_original(
            journal,
            delivery_intents=(request_intent, cancel_intent),
            revision=2,
            state=PendingPhase.CANCEL_PUBLISHED,
            updated_at_ms=intended_at_ms,
        )
        self._save_journal_advance(
            journal,
            cancel_intended,
            label="cancel-intent",
        )
        loaded = self._load_journal_observed()
        if loaded is None or not _same_journal(loaded.journal, cancel_intended):
            raise _state_conflict("cancel-intent journal changed before continuation")
        await self._continue_cancel_intent(
            cancel_intended,
            loaded.original.invocation,
        )

    async def _continue_cancel_intent(
        self,
        cancel_intended: PendingJournal,
        invocation: FrozenInvocation,
    ) -> None:
        original = cancel_intended.original
        if original.state is not PendingPhase.CANCEL_PUBLISHED:
            raise _state_conflict("cancel-intent continuation requires cancel phase")
        if len(original.delivery_intents) != 2:
            raise _state_conflict("cancel-intent continuation requires two deliveries")
        request_intent, cancel_intent = original.delivery_intents
        if (
            request_intent.delivery_kind != "request"
            or request_intent.published_at_ms is None
            or cancel_intent.delivery_kind != "cancel"
            or cancel_intent.published_at_ms is not None
        ):
            raise _state_conflict("cancel-intent continuation delivery evidence is inconsistent")

        request = self._published_request_for(original, request_intent)
        observed_request = self._get_request_observed(original.request_id)
        if observed_request != request:
            raise _state_conflict("cancel-intent request ledger evidence drifted")
        expected_unpublished = _delivery_from_intent(cancel_intent)
        self._converge_delivery_insert(cancel_intent, expected_unpublished)

        rendered_original = self._render_intent(request_intent)
        rendered_cancel = self._render_intent(cancel_intent)
        observed_inbox = self._read_inbox_observed()
        if observed_inbox not in {None, rendered_original, rendered_cancel}:
            self._quarantine_pending(
                cancel_intended,
                request,
                reason="cancel_intent_inbox_conflict",
                message="cancel intent cannot overwrite unexplained CMO inbox bytes",
            )
            return
        if observed_inbox != rendered_cancel:
            try:
                self._require_expected_process()
            except Exception:
                self._quarantine_pending(
                    cancel_intended,
                    request,
                    reason="cancel_publication_process_changed",
                    message="CMO process identity changed before cancel publication",
                )
                return
            cancel_delivery = PreparedDelivery(
                request_id=cancel_intent.request_id,
                delivery_id=cancel_intent.delivery_id,
                delivery_kind="cancel",
                request_hash=cancel_intent.request_hash,
                body_json=cancel_intent.body_json.encode("utf-8", errors="strict"),
            )
            self._converge_inbox_publication(
                cancel_delivery,
                runtime_snapshot=cancel_intent.runtime_snapshot,
                rendered=rendered_cancel,
                allowed_predecessors=frozenset({None, rendered_original}),
            )

        epochs = _EpochSequence()
        published_at_ms = epochs.next(minimum=original.updated_at_ms)
        published_cancel_intent = DeliveryIntent.model_validate(
            {
                **cancel_intent.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "published_at_ms": published_at_ms,
            }
        )
        cancel_published = _journal_with_original(
            cancel_intended,
            delivery_intents=(request_intent, published_cancel_intent),
            revision=original.revision + 1,
            state=PendingPhase.CANCEL_PUBLISHED,
            updated_at_ms=published_at_ms,
        )
        self._save_journal_advance(
            cancel_intended,
            cancel_published,
            label="cancel-publication",
        )
        await self._continue_published_cancel(
            cancel_published,
            invocation,
            epochs=epochs,
        )

    async def _continue_published_cancel(
        self,
        cancel_published: PendingJournal,
        invocation: FrozenInvocation,
        *,
        epochs: _EpochSequence | None = None,
    ) -> None:
        original = cancel_published.original
        if original.state is not PendingPhase.CANCEL_PUBLISHED:
            raise _state_conflict("published cancel continuation requires cancel phase")
        if len(original.delivery_intents) != 2:
            raise _state_conflict("published cancel continuation requires two deliveries")
        request_intent, cancel_intent = original.delivery_intents
        if (
            request_intent.delivery_kind != "request"
            or request_intent.published_at_ms is None
            or cancel_intent.delivery_kind != "cancel"
            or cancel_intent.published_at_ms is None
        ):
            raise _state_conflict("published cancel delivery evidence is inconsistent")
        epochs = _EpochSequence() if epochs is None else epochs

        unpublished_intent = DeliveryIntent.model_validate(
            {
                **cancel_intent.model_dump(
                    mode="python",
                    round_trip=True,
                    warnings=False,
                ),
                "published_at_ms": None,
            }
        )
        unpublished_delivery = _delivery_from_intent(unpublished_intent)
        published_delivery = _delivery_from_intent(cancel_intent)
        observed_delivery = self._get_delivery_observed(cancel_intent.delivery_id)
        if observed_delivery == unpublished_delivery:
            observed_delivery = self._converge_delivery_publication(
                unpublished_delivery,
                published_delivery,
            )
        if observed_delivery != published_delivery:
            raise _state_conflict("published cancel ledger evidence drifted")

        published_request = self._published_request_for(original, request_intent)
        cancel_request = _request_with(
            published_request,
            state=HostRequestState.CANCEL_PUBLISHED,
            updated_at_ms=original.updated_at_ms,
        )
        observed_request = self._get_request_observed(original.request_id)
        if observed_request == published_request:
            observed_request = self._converge_request_transition(
                published_request,
                cancel_request,
            )
        if observed_request != cancel_request:
            raise _state_conflict("published cancel request ledger evidence drifted")

        loaded = self._load_journal_observed()
        if loaded is None or not _same_journal(loaded.journal, cancel_published):
            raise _state_conflict("cancel-published journal changed before acknowledgement wait")
        expectation = loaded.original.expectation
        expected_allowed = (
            AllowedDelivery(delivery_id=request_intent.delivery_id, delivery_kind="request"),
            AllowedDelivery(delivery_id=cancel_intent.delivery_id, delivery_kind="cancel"),
        )
        if expectation.allowed_deliveries != expected_allowed:
            raise _state_conflict("durable cancel response expectation is not ordered and dual")
        if loaded.original.invocation != invocation:
            raise _state_conflict("durable cancel invocation changed before acknowledgement wait")

        waiter = ResponseWaiter(
            response_path=self._paths.response_path(original.request_id),
            expectation=expectation,
            expected_process=self._expected_process,
            process_check=lambda: require_single_instance(
                self._process_inspector,
                self._paths.command_exe,
            ),
            poll_seconds=self._response_poll_seconds,
        )
        try:
            artifact = await waiter.wait(self._cancel_ack_timeout_seconds)
        except asyncio.CancelledError as primary:
            try:
                self._quarantine(
                    cancel_published,
                    cancel_request,
                    published_delivery,
                    epochs=epochs,
                    artifact=None,
                    reason="cancel_wait_failed",
                    message="mutation cancellation response wait failed",
                )
            except BaseException as secondary:
                primary.add_note(
                    "cancellation recovery quarantine failure: "
                    f"{type(secondary).__name__}: {secondary}"
                )
            raise
        except BridgeError as error:
            if error.code is ErrorCode.REQUEST_TIMEOUT:
                reason = "cancel_ack_timeout"
                message = "mutation cancellation acknowledgement timed out"
            else:
                reason = "cancel_wait_failed"
                message = "mutation cancellation response wait failed"
            self._quarantine(
                cancel_published,
                cancel_request,
                published_delivery,
                epochs=epochs,
                artifact=None,
                reason=reason,
                message=message,
            )
            return
        except BaseException:
            self._quarantine(
                cancel_published,
                cancel_request,
                published_delivery,
                epochs=epochs,
                artifact=None,
                reason="cancel_wait_failed",
                message="mutation cancellation response wait failed",
            )
            return

        self._settle_artifact(
            cancel_published,
            cancel_request,
            published_delivery,
            epochs=epochs,
            artifact=artifact,
            invocation=invocation,
        )

    def _published_request_for(
        self,
        original: PendingExchange,
        request_intent: DeliveryIntent,
    ) -> RequestRecord:
        published_at_ms = request_intent.published_at_ms
        if published_at_ms is None:
            raise _state_conflict("published request intent lacks its publication epoch")
        prepared = RequestRecord(
            request_id=original.request_id,
            root_key=self._paths.root_key,
            request_hash=original.request_hash,
            operation=original.operation,
            operation_class=original.effective_class,
            state=HostRequestState.PREPARED,
            runtime_snapshot=original.runtime_snapshot,
            result_schema_id=original.result_schema_id,
            recovery_schema_id=original.recovery_schema_id,
            body_json=original.body_json.encode("utf-8", errors="strict"),
            lineage_id=original.expected_lineage_id,
            activation_id=original.expected_activation_id,
            result_json=None,
            error_json=None,
            resolution_json=None,
            created_at_ms=original.created_at_ms,
            updated_at_ms=original.created_at_ms,
            terminal_at_ms=None,
        )
        return _request_with(
            prepared,
            state=HostRequestState.PUBLISHED,
            updated_at_ms=published_at_ms,
        )

    def _require_expected_process(self) -> None:
        actual = require_single_instance(
            self._process_inspector,
            self._paths.command_exe,
        )
        if actual != self._expected_process:
            raise _state_conflict("CMO process identity changed during startup recovery")

    def _settle_artifact(
        self,
        cancel_published: PendingJournal,
        cancel_request: RequestRecord,
        cancel_delivery: DeliveryRecord,
        *,
        epochs: _EpochSequence,
        artifact: ResponseArtifact,
        invocation: FrozenInvocation,
    ) -> None:
        accepted_at_ms = epochs.next(
            minimum=max(
                cancel_published.original.updated_at_ms,
                cancel_request.updated_at_ms,
                cancel_delivery.published_at_ms or 0,
                artifact.accepted_at_ms,
            )
        )
        response_accepted = _journal_with_original(
            cancel_published,
            response_artifact=artifact,
            settlement=artifact.accepted_response.settlement,
            revision=4,
            state=PendingPhase.RESPONSE_ACCEPTED,
            updated_at_ms=accepted_at_ms,
        )
        self._save_journal_advance(
            cancel_published,
            response_accepted,
            label="response-accepted cancellation",
        )

        self._continue_response_accepted(
            response_accepted,
            invocation=invocation,
            epochs=epochs,
        )

    def _recover_response_accepted(
        self,
        loaded: LoadedPendingJournal,
    ) -> RecoveryReport:
        journal = loaded.journal
        artifact = journal.original.response_artifact
        if artifact is None:
            raise _state_conflict("response-accepted startup journal lacks its artifact")
        try:
            bound = bind_durable_response(
                self._paths.response_path(journal.original.request_id),
                artifact,
                loaded.original.expectation,
                parser=parse_inst_response,
            )
            if bound != artifact:
                raise _state_conflict("bound startup response artifact drifted")
        except BridgeError as error:
            request = self._get_request_observed(journal.original.request_id)
            if request is None:
                raise _state_conflict("unbound startup artifact lacks request ledger evidence")
            self._quarantine_pending(
                journal,
                request,
                reason=str(error.details.get("reason", "response_artifact_unbound")),
                message="startup response artifact failed raw-file binding",
            )
            return self._report_after_recovery(
                journal.original.request_id,
                expected_original=journal.original,
            )

        self._continue_response_accepted(
            journal,
            invocation=loaded.original.invocation,
            epochs=_EpochSequence(),
        )
        return self._report_after_recovery(
            journal.original.request_id,
            expected_original=journal.original,
        )

    def _recover_idle_published(
        self,
        loaded: LoadedPendingJournal,
    ) -> RecoveryReport:
        journal = loaded.journal
        original = journal.original
        artifact = original.response_artifact
        if artifact is None or original.settlement is None:
            raise _state_conflict("idle-published startup journal lacks terminal artifact evidence")
        try:
            bound = bind_durable_response(
                self._paths.response_path(original.request_id),
                artifact,
                loaded.original.expectation,
                parser=parse_inst_response,
            )
            if bound != artifact:
                raise _state_conflict("bound idle response artifact drifted")
        except BridgeError as error:
            request = self._get_request_observed(original.request_id)
            if request is None:
                raise _state_conflict("unbound idle artifact lacks request ledger evidence")
            self._quarantine_pending(
                journal,
                request,
                reason=str(error.details.get("reason", "idle_response_artifact_unbound")),
                message="idle-published response failed raw-file binding",
            )
            return self._report_after_recovery(
                original.request_id,
                expected_original=original,
            )

        outcome = self._classify_terminal_artifact(
            artifact,
            invocation=loaded.original.invocation,
            request_id=original.request_id,
            operation=original.operation,
        )
        responding_intent = next(
            (
                intent
                for intent in original.delivery_intents
                if intent.delivery_id == artifact.accepted_response.envelope.delivery_id
            ),
            None,
        )
        if responding_intent is None or responding_intent.published_at_ms is None:
            raise _state_conflict("idle artifact does not identify a published delivery")
        response_delivery = _delivery_from_intent(responding_intent).model_copy(
            update={
                "response_artifact": artifact,
                "settlement": artifact.accepted_response.settlement,
            }
        )
        if self._get_delivery_observed(responding_intent.delivery_id) != response_delivery:
            raise _state_conflict("idle response delivery ledger evidence drifted")

        request = self._get_request_observed(original.request_id)
        if request is None or not self._request_identity_matches(request, original):
            raise _state_conflict("idle request ledger identity drifted")

        published_intents = tuple(
            intent for intent in original.delivery_intents if intent.published_at_ms is not None
        )
        if not published_intents:
            raise _state_conflict("idle journal lacks a published inbox predecessor")
        predecessor = self._render_intent(published_intents[-1])
        inbox = self._read_inbox_observed()
        if inbox in {None, predecessor}:
            try:
                self._require_expected_process()
                self._converge_idle_publication(predecessor)
            except Exception:
                self._quarantine_pending(
                    journal,
                    request,
                    reason="idle_publication_process_changed",
                    message="CMO process identity changed before idle recovery",
                )
                return self._report_after_recovery(
                    original.request_id,
                    expected_original=original,
                )
        # A foreign inbox may have been written after the durable idle marker. It is
        # later evidence, not permission to overwrite it or to revoke the terminal proof.
        if request.state is HostRequestState.RESPONSE_ACCEPTED:
            idle_request = _request_with(
                request,
                state=HostRequestState.IDLE_PUBLISHED,
                updated_at_ms=original.updated_at_ms,
            )
            request = self._converge_request_transition(request, idle_request)
        if request.state is HostRequestState.IDLE_PUBLISHED:
            if request.updated_at_ms != original.updated_at_ms:
                raise _state_conflict("idle request epoch differs from its journal")
            terminal_at_ms = _EpochSequence().next(minimum=request.updated_at_ms)
            terminal = _request_with(
                request,
                state=outcome.state,
                updated_at_ms=terminal_at_ms,
                terminal_at_ms=terminal_at_ms,
                result_json=outcome.result_json,
                error_json=outcome.error_json,
                resolution_json=None,
            )
            request = self._converge_request_transition(request, terminal)
        if not self._request_matches_outcome(
            request, outcome, minimum_epoch=original.updated_at_ms
        ):
            raise _state_conflict("idle terminal request evidence drifted")
        self._converge_journal_delete(journal)
        return self._report_after_recovery(
            original.request_id,
            expected_original=original,
        )

    @staticmethod
    def _request_matches_outcome(
        request: RequestRecord,
        outcome: _TerminalOutcome,
        *,
        minimum_epoch: int,
    ) -> bool:
        return (
            request.state is outcome.state
            and request.terminal_at_ms is not None
            and request.updated_at_ms >= minimum_epoch
            and request.terminal_at_ms == request.updated_at_ms
            and request.result_json == outcome.result_json
            and request.error_json == outcome.error_json
            and request.resolution_json is None
        )

    def _continue_response_accepted(
        self,
        response_accepted: PendingJournal,
        *,
        invocation: FrozenInvocation,
        epochs: _EpochSequence,
    ) -> None:
        artifact = response_accepted.original.response_artifact
        if (
            response_accepted.original.state is not PendingPhase.RESPONSE_ACCEPTED
            or artifact is None
        ):
            raise _state_conflict("response continuation lacks an accepted artifact")

        envelope = artifact.accepted_response.envelope
        responding_intent = next(
            (
                intent
                for intent in response_accepted.original.delivery_intents
                if intent.delivery_id == envelope.delivery_id
            ),
            None,
        )
        if responding_intent is None or responding_intent.published_at_ms is None:
            raise _state_conflict("cancellation response does not identify a published intent")
        responding_delivery = _delivery_from_intent(responding_intent)
        response_delivery = responding_delivery.model_copy(
            update={
                "response_artifact": artifact,
                "settlement": artifact.accepted_response.settlement,
            }
        )
        self._converge_response_record(
            artifact,
            responding_delivery,
            response_delivery,
        )

        observed_request = self._get_request_observed(response_accepted.original.request_id)
        if observed_request is None or not self._request_identity_matches(
            observed_request,
            response_accepted.original,
        ):
            raise _state_conflict("response continuation request identity drifted")
        response_request = _request_with(
            observed_request,
            state=HostRequestState.RESPONSE_ACCEPTED,
            updated_at_ms=response_accepted.original.updated_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=None,
            resolution_json=None,
        )
        if observed_request.state in {
            HostRequestState.PUBLISHED,
            HostRequestState.CANCEL_PUBLISHED,
        }:
            observed_request = self._converge_request_transition(
                observed_request,
                response_request,
            )
        if observed_request != response_request:
            raise _state_conflict("response continuation request ledger evidence drifted")

        try:
            outcome = self._classify_terminal_artifact(
                artifact,
                invocation=invocation,
                request_id=response_accepted.original.request_id,
                operation=response_accepted.original.operation,
            )
        except BridgeError as error:
            self._quarantine_accepted_artifact(
                response_accepted,
                response_request,
                epochs=epochs,
                error=error,
            )
            return

        published_intents = tuple(
            intent
            for intent in response_accepted.original.delivery_intents
            if intent.published_at_ms is not None
        )
        if not published_intents:
            raise _state_conflict("response continuation lacks a published inbox predecessor")
        current_predecessor = self._render_intent(published_intents[-1])
        try:
            self._require_expected_process()
            self._converge_idle_publication(current_predecessor)
        except Exception as cause:
            error = BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "startup response could not safely publish idle",
                {
                    "reason": "idle_publication_unsafe",
                    "request_id": str(response_accepted.original.request_id),
                },
            )
            error.__cause__ = cause
            self._quarantine_accepted_artifact(
                response_accepted,
                response_request,
                epochs=epochs,
                error=error,
            )
            return
        idle_at_ms = epochs.next(minimum=response_accepted.original.updated_at_ms)
        idle_published = _journal_with_original(
            response_accepted,
            revision=response_accepted.original.revision + 1,
            state=PendingPhase.IDLE_PUBLISHED,
            updated_at_ms=idle_at_ms,
        )
        self._save_journal_advance(
            response_accepted,
            idle_published,
            label="idle-publication cancellation",
        )

        idle_request = _request_with(
            response_request,
            state=HostRequestState.IDLE_PUBLISHED,
            updated_at_ms=idle_at_ms,
        )
        self._converge_request_transition(response_request, idle_request)
        terminal_at_ms = epochs.next(minimum=idle_at_ms)
        terminal_request = _request_with(
            idle_request,
            state=outcome.state,
            updated_at_ms=terminal_at_ms,
            terminal_at_ms=terminal_at_ms,
            result_json=outcome.result_json,
            error_json=outcome.error_json,
            resolution_json=None,
        )
        self._converge_request_transition(idle_request, terminal_request)
        self._converge_journal_delete(idle_published)

    def _classify_terminal_artifact(
        self,
        artifact: ResponseArtifact,
        *,
        invocation: FrozenInvocation,
        request_id: UUID,
        operation: str,
    ) -> _TerminalOutcome:
        accepted = artifact.accepted_response
        settlement = accepted.settlement
        if type(settlement) is CompletedSettlement:
            recovery_schema = invocation.recovery_schema
            if recovery_schema is None:
                raise BridgeError(
                    ErrorCode.PROTOCOL_ERROR,
                    "mutation result failed frozen recovery validation",
                    {
                        "request_id": str(request_id),
                        "operation": operation,
                        "recovery_schema_id": None,
                    },
                )
            try:
                recovered = recovery_schema.adapter.validate_python(settlement.result)
                normalized = recovery_schema.adapter.dump_python(recovered, mode="json")
                normalized_bytes = _canonical_json_bytes(normalized)
                if normalized_bytes != _canonical_json_bytes(settlement.result):
                    raise ValueError("normalized recovery result changed canonical bytes")
            except (
                ValidationError,
                TypeError,
                ValueError,
                OverflowError,
                RecursionError,
            ) as cause:
                error = BridgeError(
                    ErrorCode.PROTOCOL_ERROR,
                    "mutation result failed frozen recovery validation",
                    {
                        "request_id": str(request_id),
                        "operation": operation,
                        "recovery_schema_id": recovery_schema.schema_id,
                    },
                )
                error.__cause__ = cause
                raise error
            return _TerminalOutcome(
                state=HostRequestState.COMPLETED,
                result_json=normalized_bytes.decode("utf-8"),
                error_json=None,
            )
        if type(settlement) is CancelledSettlement:
            acknowledgement = accepted.cancel_ack
            if (
                accepted.delivery_kind != "cancel"
                or acknowledgement is None
                or acknowledgement.status != "cancelled"
            ):
                raise _state_conflict("cancelled settlement lost its exact acknowledgement")
            return _TerminalOutcome(
                state=HostRequestState.CANCELLED,
                result_json=None,
                error_json=None,
            )
        if type(settlement) is RejectedSettlement:
            response_error = accepted.envelope.error
            if accepted.delivery_kind != "request" or response_error is None:
                raise _state_conflict("rejected settlement lost its request error")
            return _TerminalOutcome(
                state=HostRequestState.REJECTED,
                result_json=None,
                error_json=_canonical_json_text(response_error.model_dump(mode="json")),
            )
        response_error = accepted.envelope.error
        if settlement is None and response_error is not None:
            raise BridgeError(
                ErrorCode.INDETERMINATE_OUTCOME,
                "mutation response does not prove a terminal outcome",
                {
                    "request_id": str(request_id),
                    "reason": "response_without_terminal_settlement",
                    "response_code": response_error.code.value,
                },
            )
        raise _state_conflict("accepted cancellation response has no terminal classification")

    def _quarantine_accepted_artifact(
        self,
        response_accepted: PendingJournal,
        response_request: RequestRecord,
        *,
        epochs: _EpochSequence,
        error: BridgeError,
    ) -> None:
        quarantined_at_ms = epochs.next(minimum=response_accepted.original.updated_at_ms)
        quarantined = _journal_with_original(
            response_accepted,
            revision=response_accepted.original.revision + 1,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=quarantined_at_ms,
        )
        self._save_journal_advance(
            response_accepted,
            quarantined,
            label="accepted-artifact quarantine",
        )
        quarantined_request = _request_with(
            response_request,
            state=HostRequestState.QUARANTINED,
            updated_at_ms=quarantined_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=_canonical_json_text(error.to_payload()),
            resolution_json=None,
        )
        self._converge_request_transition(response_request, quarantined_request)
        loaded = self._load_journal_observed()
        if loaded is None or not _same_journal(loaded.journal, quarantined):
            raise _state_conflict("accepted-artifact quarantine journal drifted")
        if self._get_request_observed(response_request.request_id) != quarantined_request:
            raise _state_conflict("accepted-artifact quarantine request drifted")

    def _render_intent(self, intent: DeliveryIntent) -> bytes:
        delivery = PreparedDelivery(
            request_id=intent.request_id,
            delivery_id=intent.delivery_id,
            delivery_kind=intent.delivery_kind,
            request_hash=intent.request_hash,
            body_json=intent.body_json.encode("utf-8", errors="strict"),
        )
        rendered = render_delivery_lua(delivery, intent.runtime_snapshot)
        if (
            hashlib.sha256(rendered).hexdigest() != intent.rendered_inbox_sha256
            or len(rendered) != intent.rendered_inbox_size_bytes
        ):
            raise _state_conflict("durable delivery rendering drifted")
        return rendered

    def _converge_idle_publication(self, predecessor: bytes) -> None:
        target = render_idle_lua()
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._read_inbox_observed()
            if observed == target:
                return
            if observed not in {None, predecessor}:
                raise _state_conflict("idle publication refused unexplained inbox bytes")
            try:
                self._inbox.publish_idle()
            except BaseException as error:
                last_error = error
        observed = self._read_inbox_observed()
        if observed == target:
            return
        if observed not in {None, predecessor}:
            raise _state_conflict("idle publication observed unexplained inbox bytes after retry")
        if last_error is not None:
            raise last_error
        raise _state_conflict("idle publication did not converge")

    def _converge_journal_delete(self, idle_published: PendingJournal) -> None:
        expectation = JournalDeleteExpectation(
            root_key=self._paths.root_key,
            required_release_id=idle_published.header.required_release_id,
            original_request_id=idle_published.original.request_id,
            reconcile_attempt_request_id=None,
            revisions=JournalRevisions(
                original=idle_published.original.revision,
                reconcile_attempt=None,
            ),
        )
        last_error: BaseException | None = None
        for _attempt in range(2):
            loaded = self._load_journal_observed()
            if loaded is None:
                return
            if not _same_journal(loaded.journal, idle_published):
                raise _state_conflict("terminal cancellation journal drifted before delete")
            try:
                self._journals.delete(expectation)
            except BaseException as error:
                last_error = error
        if self._load_journal_observed() is None:
            return
        if last_error is not None:
            raise last_error
        raise _state_conflict("terminal cancellation journal delete did not converge")

    def _load_journal_observed(self) -> LoadedPendingJournal | None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                return self._journals.load()
            except BaseException as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise AssertionError("journal observation retry loop did not execute")

    def _get_request_observed(self, request_id: UUID) -> RequestRecord | None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                return self._ledger.get_request(request_id)
            except BaseException as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise AssertionError("request observation retry loop did not execute")

    def _get_delivery_observed(self, delivery_id: UUID) -> DeliveryRecord | None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                return self._ledger.get_delivery(delivery_id)
            except BaseException as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise AssertionError("delivery observation retry loop did not execute")

    def _read_inbox_observed(self) -> bytes | None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            try:
                return self._paths.inbox.read_bytes()
            except FileNotFoundError:
                return None
            except (PermissionError, OSError) as error:
                last_error = error
        if last_error is not None:
            raise last_error
        raise AssertionError("inbox observation retry loop did not execute")

    def _save_journal_advance(
        self,
        predecessor: PendingJournal,
        target: PendingJournal,
        *,
        label: str,
    ) -> None:
        expected_revisions = JournalRevisions(
            original=predecessor.original.revision,
            reconcile_attempt=None,
        )
        target_revisions = JournalRevisions(
            original=target.original.revision,
            reconcile_attempt=None,
        )
        last_error: BaseException | None = None
        for _attempt in range(2):
            loaded = self._load_journal_observed()
            if loaded is not None and _same_journal(loaded.journal, target):
                return
            if loaded is None or not _same_journal(loaded.journal, predecessor):
                raise _state_conflict(f"{label} journal durable state drifted") from last_error
            try:
                revisions = self._journals.save(
                    target,
                    expected_revisions=expected_revisions,
                )
                if revisions != target_revisions:
                    last_error = _state_conflict(f"{label} journal save returned revision drift")
            except BaseException as error:
                last_error = error
        loaded = self._load_journal_observed()
        if loaded is not None and _same_journal(loaded.journal, target):
            return
        if last_error is not None:
            raise last_error
        raise _state_conflict(f"{label} journal did not converge")

    def _converge_delivery_insert(
        self,
        intent: DeliveryIntent,
        target: DeliveryRecord,
    ) -> None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._get_delivery_observed(intent.delivery_id)
            if observed == target:
                return
            if observed is not None:
                raise _state_conflict("cancel delivery insert durable state drifted")
            try:
                inserted = cast(object, self._ledger.insert_delivery(intent))
                if inserted is not None:
                    last_error = _state_conflict(
                        "cancel delivery insert returned an unexpected value"
                    )
            except BaseException as error:
                last_error = error
        if self._get_delivery_observed(intent.delivery_id) == target:
            return
        if last_error is not None:
            raise last_error
        raise _state_conflict("cancel delivery insert did not converge")

    def _converge_inbox_publication(
        self,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
        rendered: bytes,
        allowed_predecessors: frozenset[bytes | None],
    ) -> None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._read_inbox_observed()
            if observed == rendered:
                return
            if observed not in allowed_predecessors:
                raise _state_conflict(
                    "cancel publication refused to overwrite unexplained inbox bytes"
                )
            try:
                self._inbox.publish_delivery(
                    delivery,
                    runtime_snapshot=runtime_snapshot,
                )
            except BaseException as error:
                last_error = error
        observed = self._read_inbox_observed()
        if observed == rendered:
            return
        if observed not in allowed_predecessors:
            raise _state_conflict("cancel publication observed unexplained inbox bytes after retry")
        if last_error is not None:
            raise last_error
        raise _state_conflict("cancel inbox publication did not converge")

    def _converge_delivery_publication(
        self,
        predecessor: DeliveryRecord,
        target: DeliveryRecord,
    ) -> DeliveryRecord:
        published_at_ms = target.published_at_ms
        if published_at_ms is None:
            raise _state_conflict("cancel publication target lacks its epoch")
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._get_delivery_observed(target.delivery_id)
            if observed == target:
                return target
            if observed != predecessor:
                raise _state_conflict("cancel delivery publication durable state drifted")
            try:
                returned = self._ledger.mark_delivery_published(
                    target.delivery_id,
                    published_at_ms=published_at_ms,
                )
                if returned != target:
                    last_error = _state_conflict("published cancel delivery marker returned drift")
            except BaseException as error:
                last_error = error
        observed = self._get_delivery_observed(target.delivery_id)
        if observed == target:
            return target
        if last_error is not None:
            raise last_error
        raise _state_conflict("cancel delivery publication did not converge")

    def _converge_request_transition(
        self,
        predecessor: RequestRecord,
        target: RequestRecord,
    ) -> RequestRecord:
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._get_request_observed(target.request_id)
            if observed == target:
                return target
            if observed != predecessor:
                raise _state_conflict("recovery request durable state drifted")
            try:
                returned = self._ledger.transition(
                    target.request_id,
                    expected_states=frozenset({predecessor.state}),
                    new_state=target.state,
                    updated_at_ms=target.updated_at_ms,
                    terminal_at_ms=target.terminal_at_ms,
                    result_json=target.result_json,
                    error_json=target.error_json,
                    resolution_json=target.resolution_json,
                )
                if returned != target:
                    last_error = _state_conflict("recovery request transition returned drift")
            except BaseException as error:
                last_error = error
        observed = self._get_request_observed(target.request_id)
        if observed == target:
            return target
        if last_error is not None:
            raise last_error
        raise _state_conflict("recovery request transition did not converge")

    def _load_exact_published(
        self,
        expected_journal: PendingJournal,
    ) -> tuple[PendingJournal, RequestRecord, DeliveryIntent, DeliveryRecord]:
        if type(expected_journal) is not PendingJournal:
            raise _invalid_argument("expected recovery journal must be an exact PendingJournal")
        loaded = self._load_journal_observed()
        if loaded is None or not _same_journal(loaded.journal, expected_journal):
            raise _state_conflict("published recovery journal identity changed")
        journal = loaded.journal
        original = journal.original
        if (
            journal.reconcile_attempt is not None
            or loaded.reconcile_attempt is not None
            or original.revision != 1
            or original.state is not PendingPhase.PUBLISHED
            or original.response_artifact is not None
            or original.settlement is not None
            or len(original.delivery_intents) != 1
        ):
            raise _state_conflict("recovery requires an exact published revision-one journal")
        request_intent = original.delivery_intents[0]
        if (
            request_intent.delivery_kind != "request"
            or request_intent.published_at_ms is None
            or request_intent.published_at_ms != original.updated_at_ms
            or request_intent.response_filename
            != self._paths.response_path(original.request_id).name
        ):
            raise _state_conflict("published recovery request intent is inconsistent")

        request = self._get_request_observed(original.request_id)
        if request is None or not self._request_matches_published(request, original):
            raise _state_conflict("published recovery request ledger evidence is inconsistent")
        delivery = self._get_delivery_observed(request_intent.delivery_id)
        if delivery != _delivery_from_intent(request_intent):
            raise _state_conflict("published recovery delivery ledger evidence is inconsistent")
        return journal, request, request_intent, cast(DeliveryRecord, delivery)

    def _request_matches_published(
        self,
        request: RequestRecord,
        original: PendingExchange,
    ) -> bool:
        return (
            request.request_id == original.request_id
            and request.root_key == self._paths.root_key
            and request.request_hash == original.request_hash
            and request.operation == original.operation
            and request.operation_class is original.effective_class
            and request.state is HostRequestState.PUBLISHED
            and request.runtime_snapshot == original.runtime_snapshot
            and request.result_schema_id == original.result_schema_id
            and request.recovery_schema_id == original.recovery_schema_id
            and request.body_json == original.body_json.encode("utf-8")
            and request.lineage_id == original.expected_lineage_id
            and request.activation_id == original.expected_activation_id
            and request.result_json is None
            and request.error_json is None
            and request.resolution_json is None
            and request.created_at_ms == original.created_at_ms
            and request.updated_at_ms == original.updated_at_ms
            and request.terminal_at_ms is None
        )

    @staticmethod
    def _rebuild_body(original: PendingExchange) -> RequestBody:
        try:
            body = RequestBody.model_validate_json(original.body_json)
        except (
            ValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _state_conflict("durable recovery request body could not be rebuilt") from error
        if canonical_body_bytes(body) != original.body_json.encode("utf-8"):
            raise _state_conflict("durable recovery request body is not canonical")
        return body

    def _quarantine(
        self,
        cancel_published: PendingJournal,
        cancel_request: RequestRecord,
        cancel_delivery: DeliveryRecord,
        *,
        epochs: _EpochSequence,
        artifact: ResponseArtifact | None,
        reason: str,
        message: str,
    ) -> None:
        quarantine_at_ms = epochs.next(
            minimum=max(
                cancel_published.original.updated_at_ms,
                cancel_request.updated_at_ms,
                cancel_delivery.published_at_ms or 0,
                0 if artifact is None else artifact.accepted_at_ms,
            )
        )
        settlement = None if artifact is None else artifact.accepted_response.settlement
        quarantined = _journal_with_original(
            cancel_published,
            response_artifact=artifact,
            settlement=settlement,
            revision=4,
            state=PendingPhase.QUARANTINED,
            updated_at_ms=quarantine_at_ms,
        )
        self._save_journal_advance(
            cancel_published,
            quarantined,
            label="quarantine",
        )

        if artifact is not None:
            envelope = artifact.accepted_response.envelope
            responding_intent = next(
                (
                    intent
                    for intent in quarantined.original.delivery_intents
                    if intent.delivery_id == envelope.delivery_id
                ),
                None,
            )
            if responding_intent is None:
                raise _state_conflict("quarantined response does not identify a delivery intent")
            expected_response_delivery = _delivery_from_intent(responding_intent).model_copy(
                update={
                    "response_artifact": artifact,
                    "settlement": settlement,
                }
            )
            self._converge_response_record(
                artifact,
                _delivery_from_intent(responding_intent),
                expected_response_delivery,
            )

        error_json = _canonical_json_text(
            {
                "code": ErrorCode.INDETERMINATE_OUTCOME.value,
                "details": {
                    "reason": reason,
                    "request_id": str(cancel_published.original.request_id),
                },
                "message": message,
            }
        )
        expected_request = _request_with(
            cancel_request,
            state=HostRequestState.QUARANTINED,
            updated_at_ms=quarantine_at_ms,
            terminal_at_ms=None,
            result_json=None,
            error_json=error_json,
            resolution_json=None,
        )
        self._converge_request_transition(cancel_request, expected_request)

        loaded = self._load_journal_observed()
        if loaded is None or not _same_journal(loaded.journal, quarantined):
            raise _state_conflict("quarantined journal postcondition drifted")
        if self._get_request_observed(cancel_published.original.request_id) != expected_request:
            raise _state_conflict("quarantined request postcondition drifted")

    def _converge_response_record(
        self,
        artifact: ResponseArtifact,
        predecessor: DeliveryRecord,
        target: DeliveryRecord,
    ) -> None:
        last_error: BaseException | None = None
        for _attempt in range(2):
            observed = self._get_delivery_observed(target.delivery_id)
            if observed == target:
                return
            if observed != predecessor:
                raise _state_conflict("quarantined response durable state drifted")
            try:
                returned = self._ledger.record_response(artifact)
                if returned != target:
                    last_error = _state_conflict(
                        "quarantined response delivery record returned drift"
                    )
            except BaseException as error:
                last_error = error
        if self._get_delivery_observed(target.delivery_id) == target:
            return
        if last_error is not None:
            raise last_error
        raise _state_conflict("quarantined response record did not converge")
