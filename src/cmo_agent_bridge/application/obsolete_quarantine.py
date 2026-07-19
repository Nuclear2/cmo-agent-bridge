from __future__ import annotations

import base64
import binascii
import re
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictInt

from cmo_agent_bridge.application.confirmation import (
    OBSOLETE_QUARANTINE_ABANDONMENT_CONFIRMATION_FORMAT,
    ConfirmationBinding,
    IssuedConfirmation,
    ObsoleteQuarantineAbandonmentConfirmationDescriptor,
    obsolete_quarantine_abandonment_confirmation_binding,
)
from cmo_agent_bridge.application.ports import WallClockPort
from cmo_agent_bridge.application.queue_models import QueueError, canonical_queue_json
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.runtime import Sha256
from cmo_agent_bridge.state.host_resolution import (
    HOST_QUARANTINE_RESOLUTION_FORMAT,
    HostQuarantineResolutionMarker,
    canonical_host_quarantine_resolution,
    marker_matches_pending_journal,
    parse_host_quarantine_resolution,
)
from cmo_agent_bridge.state.models import HostRequestState, PendingJournal, PendingPhase
from cmo_agent_bridge.state.operation_queue import (
    OperationQueueRecord,
    OperationQueueState,
    OperationQueueStore,
)
from cmo_agent_bridge.state.pending_journal import (
    HostResolvedJournalDeleteExpectation,
    PendingJournalStore,
)
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.lock import RootLock


_CONFIRMATION_LIFETIME_MS = 60_000
_SQLITE_INT_MAX = 2**63 - 1
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPERATION = "host.quarantine.abandon_obsolete_lineage"
_IMPACT = (
    "Host-only abandonment of one explicitly obsolete scenario lineage. The durable "
    "request is resolved as not_applied, the CMO inbox is returned to idle, and the "
    "matching quarantine journal is removed. CMO is not contacted and the original "
    "operation is never replayed."
)


class ObsoleteQuarantineAbandonmentPreview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    phase: Literal["preview"]
    mode: Literal["obsolete_lineage_abandonment"]
    disposition: Literal["not_applied"]
    cmo_contacted: Literal[False]
    original_operation_replayed: Literal[False]
    unrelated_nonterminal_work_absent: Literal[True]
    target_active_queue_recovery_required: bool
    request_id: UUID
    request_hash: Sha256
    operation: str
    original_journal_revision: StrictInt
    required_release_id: Sha256
    scenario_lineage_id: UUID
    original_activation_id: UUID
    impact: str
    confirmation_token: str
    confirmation_expires_at_utc: datetime


class ObsoleteQuarantineAbandonmentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    phase: Literal["resolved"]
    mode: Literal["obsolete_lineage_abandonment"]
    disposition: Literal["not_applied"]
    cmo_contacted: Literal[False]
    original_operation_replayed: Literal[False]
    unrelated_nonterminal_work_absent: Literal[True]
    target_active_queue_recovered: bool
    request_id: UUID
    request_hash: Sha256
    operation: str
    required_release_id: Sha256
    scenario_lineage_id: UUID
    resolved: Literal[True]
    resolved_at_utc: datetime


class _ConfirmationStore(Protocol):
    def issue(
        self,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> IssuedConfirmation: ...

    def lookup_active(
        self,
        token: str,
        *,
        now_ms: int,
    ) -> ConfirmationBinding: ...

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256: ...


class ObsoleteQuarantineAbandonmentService:
    """Abandon an old lineage without a CMO process, handshake, or replay."""

    def __init__(
        self,
        *,
        root_key: Sha256,
        running_release_id: Sha256,
        coordination_lock: RootLock,
        root_lock: RootLock,
        journals: PendingJournalStore,
        ledger: RequestLedger,
        queue_store: OperationQueueStore,
        confirmations: _ConfirmationStore,
        inbox: InboxPublisher,
        wall_clock: WallClockPort,
    ) -> None:
        if type(root_key) is not str or _SHA256_RE.fullmatch(root_key) is None:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "abandonment root key is invalid")
        if (
            type(running_release_id) is not str
            or _SHA256_RE.fullmatch(running_release_id) is None
        ):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "abandonment running release ID is invalid",
            )
        if type(coordination_lock) is not RootLock or type(root_lock) is not RootLock:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "abandonment locks are invalid")
        if type(journals) is not PendingJournalStore:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "abandonment journal store is invalid")
        if type(ledger) is not RequestLedger or type(queue_store) is not OperationQueueStore:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "abandonment durable stores are invalid")
        if type(inbox) is not InboxPublisher:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "abandonment inbox is invalid")
        self._root_key = root_key
        self._running_release_id = running_release_id
        self._coordination_lock = coordination_lock
        self._root_lock = root_lock
        self._journals = journals
        self._ledger = ledger
        self._queue_store = queue_store
        self._confirmations = confirmations
        self._inbox = inbox
        self._wall_clock = wall_clock

    async def preview(
        self,
    ) -> ObsoleteQuarantineAbandonmentPreview | ObsoleteQuarantineAbandonmentResult:
        async with self._coordination_lock:
            async with self._root_lock:
                context = self._load_context()
                if context.committed_marker is not None:
                    return self._finish_committed(context)
                now_ms = _validated_issue_epoch(self._wall_clock.now_ms())
                binding = obsolete_quarantine_abandonment_confirmation_binding(
                    context.descriptor
                )
                issued = self._confirmations.issue(binding, now_ms=now_ms)
                _validate_issuance(issued, now_ms)
                original = context.journal.original
                return ObsoleteQuarantineAbandonmentPreview(
                    phase="preview",
                    mode="obsolete_lineage_abandonment",
                    disposition="not_applied",
                    cmo_contacted=False,
                    original_operation_replayed=False,
                    unrelated_nonterminal_work_absent=True,
                    target_active_queue_recovery_required=(
                        context.target_active_queue is not None
                    ),
                    request_id=original.request_id,
                    request_hash=original.request_hash,
                    operation=original.operation,
                    original_journal_revision=original.revision,
                    required_release_id=context.journal.header.required_release_id,
                    scenario_lineage_id=context.descriptor.scenario_lineage_id,
                    original_activation_id=context.descriptor.original_activation_id,
                    impact=_IMPACT,
                    confirmation_token=issued.token,
                    confirmation_expires_at_utc=_utc_from_ms(issued.expires_at_ms),
                )
        raise RuntimeError("abandonment locks unexpectedly suppressed preview")

    async def confirm(
        self,
        confirmation_token: str,
    ) -> ObsoleteQuarantineAbandonmentResult:
        async with self._coordination_lock:
            async with self._root_lock:
                lookup_at_ms = _validated_epoch(self._wall_clock.now_ms())
                try:
                    stored = self._confirmations.lookup_active(
                        confirmation_token,
                        now_ms=lookup_at_ms,
                    )
                except BridgeError as error:
                    raise _confirmation_denied() from error
                context = self._load_context()
                if context.committed_marker is not None:
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "obsolete quarantine is already committed; rerun without a token to finish recovery",
                    )
                rebuilt = obsolete_quarantine_abandonment_confirmation_binding(
                    context.descriptor
                )
                if (
                    stored != rebuilt
                    or stored.root_key != self._root_key
                    or stored.operation != _OPERATION
                    or stored.binding_format
                    != OBSOLETE_QUARANTINE_ABANDONMENT_CONFIRMATION_FORMAT
                ):
                    raise _confirmation_denied()
                consume_at_ms = _validated_epoch(self._wall_clock.now_ms())
                proof = self._confirmations.consume(
                    confirmation_token,
                    rebuilt,
                    now_ms=consume_at_ms,
                )
                if type(proof) is not str or _SHA256_RE.fullmatch(proof) is None:
                    raise BridgeError(
                        ErrorCode.PROTOCOL_ERROR,
                        "confirmation store returned an invalid proof",
                    )

                original = context.journal.original
                resolved_at_ms = max(
                    _validated_epoch(self._wall_clock.now_ms()),
                    context.record.updated_at_ms,
                    original.updated_at_ms,
                    (
                        0
                        if context.target_active_queue is None
                        else context.target_active_queue.updated_at_ms
                    ),
                )
                target_active_queue_recovered = self._converge_target_active_queue(
                    context,
                    at_ms=resolved_at_ms,
                )
                marker = HostQuarantineResolutionMarker(
                    format=HOST_QUARANTINE_RESOLUTION_FORMAT,
                    mode="host_only",
                    manual_evidence=True,
                    root_key=context.journal.header.root_key,
                    required_release_id=context.journal.header.required_release_id,
                    request_id=original.request_id,
                    request_hash=original.request_hash,
                    original_journal_revision=original.revision,
                    scenario_lineage_id=context.descriptor.scenario_lineage_id,
                    original_activation_id=context.descriptor.original_activation_id,
                    disposition="not_applied",
                    resolved_at_ms=resolved_at_ms,
                )
                resolution_json = canonical_host_quarantine_resolution(marker)
                resolved = self._ledger.transition(
                    original.request_id,
                    expected_states=frozenset({HostRequestState.QUARANTINED}),
                    new_state=HostRequestState.RESOLVED,
                    updated_at_ms=resolved_at_ms,
                    terminal_at_ms=resolved_at_ms,
                    result_json=None,
                    error_json=None,
                    resolution_json=resolution_json,
                )
                if (
                    resolved.state is not HostRequestState.RESOLVED
                    or resolved.resolution_json != resolution_json
                ):
                    raise BridgeError(
                        ErrorCode.STATE_CONFLICT,
                        "obsolete quarantine transition returned drift",
                    )
                return self._finish_committed(
                    context.model_copy(
                        update={
                            "record": resolved,
                            "committed_marker": marker,
                            "target_active_queue": None,
                            "target_active_queue_recovered": target_active_queue_recovered,
                        }
                    )
                )
        raise RuntimeError("abandonment locks unexpectedly suppressed confirmation")

    def _load_context(self) -> _AbandonmentContext:
        journal = self._journals.load_for_obsolete_abandonment()
        if journal is None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "no obsolete Host quarantine journal is pending",
            )
        original = journal.original
        if journal.reconcile_attempt is not None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete abandonment refuses a journal with a reconciliation attempt",
            )
        if original.state is not PendingPhase.QUARANTINED:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete abandonment requires a quarantined original journal",
                {"state": original.state.value},
            )
        if journal.header.root_key != self._root_key:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete quarantine root does not match the selected bridge",
            )
        if journal.header.required_release_id == self._running_release_id:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "obsolete quarantine abandonment is only available across bridge releases",
                {
                    "required_release_id": journal.header.required_release_id,
                    "running_release_id": self._running_release_id,
                    "next_step": (
                        "Use resolve-quarantine for a quarantine created by the running release."
                    ),
                },
            )
        record = self._ledger.get_request(original.request_id)
        if record is None or not _record_matches_original(record, journal):
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete quarantine ledger identity does not match the pending journal",
            )
        committed_marker: HostQuarantineResolutionMarker | None = None
        if record.state is HostRequestState.RESOLVED:
            if record.resolution_json is None:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "resolved obsolete quarantine lacks its resolution marker",
                )
            try:
                candidate_marker = parse_host_quarantine_resolution(
                    record.resolution_json
                )
            except ValueError as error:
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "resolved obsolete quarantine marker is invalid",
                ) from error
            if (
                candidate_marker.mode != "host_only"
                or candidate_marker.disposition != "not_applied"
                or not candidate_marker.manual_evidence
                or not marker_matches_pending_journal(candidate_marker, journal)
            ):
                raise BridgeError(
                    ErrorCode.STATE_CONFLICT,
                    "resolved obsolete quarantine marker does not match the pending journal",
                )
            committed_marker = candidate_marker
        elif record.state is not HostRequestState.QUARANTINED:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete quarantine ledger is not unresolved",
                {"state": record.state.value},
            )
        nonterminal = self._queue_store.list(
            root_key=self._root_key,
            states=frozenset(
                {OperationQueueState.QUEUED, OperationQueueState.ACTIVE}
            ),
        )
        target_active_queue: OperationQueueRecord | None = None
        blockers: list[OperationQueueRecord] = []
        for item in nonterminal:
            if (
                item.state is OperationQueueState.ACTIVE
                and item.request_id == original.request_id
                and _queue_matches_original(item, journal)
                and target_active_queue is None
            ):
                target_active_queue = item
            else:
                blockers.append(item)
        if blockers:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete abandonment is blocked by queued or active CMO work",
                {
                    "request_ids": [str(item.request_id) for item in blockers],
                    "states": [item.state.value for item in blockers],
                },
            )
        lineage_id = original.expected_lineage_id
        activation_id = original.expected_activation_id
        if lineage_id is None or activation_id is None:
            raise BridgeError(
                ErrorCode.JOURNAL_CORRUPT,
                "obsolete quarantined mutation lacks exact scenario context",
            )
        descriptor = ObsoleteQuarantineAbandonmentConfirmationDescriptor(
            format=OBSOLETE_QUARANTINE_ABANDONMENT_CONFIRMATION_FORMAT,
            root_key=journal.header.root_key,
            required_release_id=journal.header.required_release_id,
            request_id=original.request_id,
            request_hash=original.request_hash,
            original_journal_revision=original.revision,
            scenario_lineage_id=lineage_id,
            original_activation_id=activation_id,
            disposition="not_applied",
        )
        return _AbandonmentContext(
            journal=journal,
            record=record,
            descriptor=descriptor,
            committed_marker=committed_marker,
            target_active_queue=target_active_queue,
            target_active_queue_recovered=False,
        )

    def _finish_committed(
        self,
        context: _AbandonmentContext,
    ) -> ObsoleteQuarantineAbandonmentResult:
        marker = context.committed_marker
        if marker is None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "obsolete quarantine has not been durably committed",
            )
        target_active_queue_recovered = (
            context.target_active_queue_recovered
            or self._converge_target_active_queue(
                context,
                at_ms=max(
                    marker.resolved_at_ms,
                    (
                        0
                        if context.target_active_queue is None
                        else context.target_active_queue.updated_at_ms
                    ),
                ),
            )
        )
        self._inbox.publish_idle()
        self._journals.delete_obsolete_host_resolved(
            _delete_expectation(context.journal)
        )
        original = context.journal.original
        return ObsoleteQuarantineAbandonmentResult(
            phase="resolved",
            mode="obsolete_lineage_abandonment",
            disposition="not_applied",
            cmo_contacted=False,
            original_operation_replayed=False,
            unrelated_nonterminal_work_absent=True,
            target_active_queue_recovered=target_active_queue_recovered,
            request_id=original.request_id,
            request_hash=original.request_hash,
            operation=original.operation,
            required_release_id=context.journal.header.required_release_id,
            scenario_lineage_id=context.descriptor.scenario_lineage_id,
            resolved=True,
            resolved_at_utc=_utc_from_ms(marker.resolved_at_ms),
        )

    def _converge_target_active_queue(
        self,
        context: _AbandonmentContext,
        *,
        at_ms: int,
    ) -> bool:
        target = context.target_active_queue
        if target is None:
            return context.target_active_queue_recovered
        error = QueueError(
            code=ErrorCode.INDETERMINATE_OUTCOME,
            message=(
                "obsolete quarantine abandonment converged an interrupted active queue row"
            ),
            details={
                "request_id": str(target.request_id),
                "source": "host.quarantine.abandon_obsolete_lineage",
            },
        )
        converged = self._queue_store.quarantine(
            target.request_id,
            canonical_queue_json(error.model_dump(mode="json")),
            at_ms=at_ms,
        )
        if converged is None or converged.state is not OperationQueueState.QUARANTINED:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "matching active queue row changed before obsolete abandonment recovery",
            )
        return True


class _AbandonmentContext(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        arbitrary_types_allowed=True,
    )

    journal: PendingJournal
    record: RequestRecord
    descriptor: ObsoleteQuarantineAbandonmentConfirmationDescriptor
    committed_marker: HostQuarantineResolutionMarker | None
    target_active_queue: OperationQueueRecord | None
    target_active_queue_recovered: bool


def _record_matches_original(record: RequestRecord, journal: PendingJournal) -> bool:
    original = journal.original
    return (
        record.request_id == original.request_id
        and record.root_key == journal.header.root_key
        and record.request_hash == original.request_hash
        and record.operation == original.operation
        and record.operation_class is original.effective_class
        and record.runtime_snapshot == original.runtime_snapshot
        and record.result_schema_id == original.result_schema_id
        and record.recovery_schema_id == original.recovery_schema_id
        and record.body_json == original.body_json.encode("utf-8")
        and record.lineage_id == original.expected_lineage_id
        and record.activation_id == original.expected_activation_id
        and record.created_at_ms == original.created_at_ms
    )


def _queue_matches_original(
    record: OperationQueueRecord,
    journal: PendingJournal,
) -> bool:
    original = journal.original
    return (
        record.request_id == original.request_id
        and record.root_key == journal.header.root_key
        and record.operation == original.operation
        and record.body_json == original.body_json.encode("utf-8")
        and record.runtime_snapshot == original.runtime_snapshot
        and record.result_schema_id == original.result_schema_id
        and record.recovery_schema_id == original.recovery_schema_id
        and record.expected_lineage_id == original.expected_lineage_id
        and record.expected_activation_id == original.expected_activation_id
    )


def _delete_expectation(
    journal: PendingJournal,
) -> HostResolvedJournalDeleteExpectation:
    original = journal.original
    return HostResolvedJournalDeleteExpectation(
        root_key=journal.header.root_key,
        required_release_id=journal.header.required_release_id,
        original_request_id=original.request_id,
        original_request_hash=original.request_hash,
        original_revision=original.revision,
    )


def _validated_issue_epoch(value: object) -> int:
    if (
        type(value) is not int
        or value < 0
        or value > _SQLITE_INT_MAX - _CONFIRMATION_LIFETIME_MS
    ):
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "wall clock returned an invalid confirmation epoch",
        )
    return value


def _validated_epoch(value: object) -> int:
    if type(value) is not int or value < 0 or value > _SQLITE_INT_MAX:
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "wall clock returned an invalid confirmation epoch",
        )
    return value


def _validate_issuance(issued: object, now_ms: int) -> None:
    if (
        type(issued) is not IssuedConfirmation
        or not _is_canonical_token(issued.token)
        or issued.expires_at_ms != now_ms + _CONFIRMATION_LIFETIME_MS
    ):
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "confirmation store returned an invalid issuance",
        )


def _is_canonical_token(value: object) -> bool:
    if type(value) is not str or _TOKEN_RE.fullmatch(value) is None:
        return False
    try:
        decoded = base64.b64decode(
            (value + "=").encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError):
        return False
    return (
        len(decoded) == 32
        and base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
        == value
    )


def _utc_from_ms(value: int) -> datetime:
    seconds, milliseconds = divmod(value, 1000)
    try:
        return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(
            milliseconds=milliseconds
        )
    except (OSError, OverflowError, ValueError) as error:
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "confirmation epoch is outside the supported UTC range",
        ) from error


def _confirmation_denied() -> BridgeError:
    return BridgeError(ErrorCode.POLICY_DENIED, "confirmation denied")
