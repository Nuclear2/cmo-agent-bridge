from __future__ import annotations

import base64
import binascii
import re
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictInt

from cmo_agent_bridge.application.confirmation import (
    HOST_QUARANTINE_CONFIRMATION_FORMAT,
    ConfirmationBinding,
    HostQuarantineConfirmationDescriptor,
    IssuedConfirmation,
    host_quarantine_confirmation_binding,
)
from cmo_agent_bridge.application.ports import WallClockPort
from cmo_agent_bridge.application.session_service import SessionActivation
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256
from cmo_agent_bridge.state.host_resolution import (
    HOST_QUARANTINE_RESOLUTION_FORMAT,
    HostQuarantineResolutionMarker,
    canonical_host_quarantine_resolution,
)
from cmo_agent_bridge.state.models import HostRequestState, PendingJournal, PendingPhase
from cmo_agent_bridge.state.pending_journal import (
    HostResolvedJournalDeleteExpectation,
    PendingJournalStore,
)
from cmo_agent_bridge.state.request_ledger import RequestLedger, RequestRecord
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.models import BridgeChannel


_CONFIRMATION_LIFETIME_MS = 60_000
_SQLITE_INT_MAX = 2**63 - 1
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPERATION = "host.quarantine.resolve"
_IMPACT = (
    "Host-only manual resolution: marks the durable Host request as resolved and "
    "removes its quarantine journal. It does not inspect, replay, or modify the "
    "original CMO operation. Use only after independently determining whether the "
    "original operation was applied."
)


class HostQuarantineResolutionPreview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    phase: Literal["preview"]
    mode: Literal["host_only"]
    manual_evidence_required: Literal[True]
    runtime_barrier_absent: Literal[True]
    request_id: UUID
    request_hash: Sha256
    operation: str
    disposition: Literal["applied", "not_applied"]
    original_journal_revision: StrictInt
    scenario_lineage_id: UUID
    original_activation_id: UUID
    impact: str
    confirmation_token: str
    confirmation_expires_at_utc: datetime


class HostQuarantineResolutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    phase: Literal["resolved"]
    mode: Literal["host_only"]
    manual_evidence: Literal[True]
    runtime_barrier_absent: Literal[True]
    request_id: UUID
    request_hash: Sha256
    operation: str
    disposition: Literal["applied", "not_applied"]
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


class _SessionCoordinator(Protocol):
    @property
    def runtime_snapshot(self) -> RuntimeSnapshot: ...

    async def handshake(
        self,
        channel: BridgeChannel,
        *,
        accept_lineage_id: UUID | None = None,
        reserved_activation_candidate: UUID | None = None,
    ) -> SessionActivation: ...


class _Transport(Protocol):
    @property
    def root_key(self) -> Sha256: ...

    @property
    def journals(self) -> PendingJournalStore: ...

    @property
    def ledger(self) -> RequestLedger: ...

    @property
    def inbox(self) -> InboxPublisher: ...

    def session(self) -> AbstractAsyncContextManager[BridgeChannel]: ...


class HostQuarantineResolutionService:
    def __init__(
        self,
        *,
        transport: _Transport,
        sessions: _SessionCoordinator,
        confirmations: _ConfirmationStore,
        wall_clock: WallClockPort,
    ) -> None:
        self._transport = transport
        self._sessions = sessions
        self._confirmations = confirmations
        self._wall_clock = wall_clock

    async def preview(
        self,
        disposition: Literal["applied", "not_applied"],
    ) -> HostQuarantineResolutionPreview:
        selected = _validate_disposition(disposition)
        async with self._transport.session() as channel:
            context = await self._load_context(channel, selected)
            now_ms = _validated_issue_epoch(self._wall_clock.now_ms())
            binding = host_quarantine_confirmation_binding(context.descriptor)
            issued = self._confirmations.issue(binding, now_ms=now_ms)
            _validate_issuance(issued, now_ms)
            expires_at = _utc_from_ms(issued.expires_at_ms)
            return HostQuarantineResolutionPreview(
                phase="preview",
                mode="host_only",
                manual_evidence_required=True,
                runtime_barrier_absent=True,
                request_id=context.journal.original.request_id,
                request_hash=context.journal.original.request_hash,
                operation=context.journal.original.operation,
                disposition=selected,
                original_journal_revision=context.journal.original.revision,
                scenario_lineage_id=context.descriptor.scenario_lineage_id,
                original_activation_id=context.descriptor.original_activation_id,
                impact=_IMPACT,
                confirmation_token=issued.token,
                confirmation_expires_at_utc=expires_at,
            )

    async def confirm(
        self,
        disposition: Literal["applied", "not_applied"],
        confirmation_token: str,
    ) -> HostQuarantineResolutionResult:
        selected = _validate_disposition(disposition)
        async with self._transport.session() as channel:
            lookup_at_ms = _validated_epoch(self._wall_clock.now_ms())
            try:
                stored = self._confirmations.lookup_active(
                    confirmation_token,
                    now_ms=lookup_at_ms,
                )
            except BridgeError as error:
                raise _confirmation_denied() from error
            context = await self._load_context(channel, selected)
            rebuilt = host_quarantine_confirmation_binding(context.descriptor)
            if (
                stored != rebuilt
                or stored.root_key != self._transport.root_key
                or stored.operation != _OPERATION
                or stored.binding_format != HOST_QUARANTINE_CONFIRMATION_FORMAT
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
            record = context.record
            resolved_at_ms = max(
                _validated_epoch(self._wall_clock.now_ms()),
                record.updated_at_ms,
                original.updated_at_ms,
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
                disposition=selected,
                resolved_at_ms=resolved_at_ms,
            )
            resolution_json = canonical_host_quarantine_resolution(marker)
            resolved = self._transport.ledger.transition(
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
                    "host quarantine resolution transition returned drift",
                )
            self._transport.inbox.publish_idle()
            self._transport.journals.delete_host_resolved(
                _delete_expectation(context.journal)
            )
            return HostQuarantineResolutionResult(
                phase="resolved",
                mode="host_only",
                manual_evidence=True,
                runtime_barrier_absent=True,
                request_id=original.request_id,
                request_hash=original.request_hash,
                operation=original.operation,
                disposition=selected,
                resolved=True,
                resolved_at_utc=_utc_from_ms(resolved_at_ms),
            )

    async def _load_context(
        self,
        channel: BridgeChannel,
        disposition: Literal["applied", "not_applied"],
    ) -> _ResolutionContext:
        loaded = self._transport.journals.load()
        if loaded is None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "no Host quarantine journal is pending",
            )
        journal = loaded.journal
        original = journal.original
        if journal.reconcile_attempt is not None:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "Host-only resolution refuses a journal with a reconciliation attempt",
            )
        if original.state is not PendingPhase.QUARANTINED:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "Host-only resolution requires a quarantined original journal",
                {"state": original.state.value},
            )
        record = self._transport.ledger.get_request(original.request_id)
        if record is None or not _record_matches_original(record, journal):
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "Host quarantine ledger identity does not match the pending journal",
            )
        if record.state is not HostRequestState.QUARANTINED:
            raise BridgeError(
                ErrorCode.STATE_CONFLICT,
                "Host quarantine ledger is not unresolved",
                {"state": record.state.value},
            )
        activation = await self._sessions.handshake(channel)
        status = activation.status
        if status.pending_request_id is not None or status.quarantined:
            raise BridgeError(
                ErrorCode.POLICY_DENIED,
                "Host-only resolution is forbidden while the CMO runtime reports a barrier",
                {
                    "pending_request_id": (
                        None
                        if status.pending_request_id is None
                        else str(status.pending_request_id)
                    ),
                    "quarantined": status.quarantined,
                },
            )
        lineage_id = original.expected_lineage_id
        activation_id = original.expected_activation_id
        if lineage_id is None or activation_id is None:
            raise BridgeError(
                ErrorCode.JOURNAL_CORRUPT,
                "quarantined mutation lacks exact scenario context",
            )
        if status.lineage_id != lineage_id:
            raise BridgeError(
                ErrorCode.SCENARIO_CHANGED,
                "Host quarantine belongs to a different scenario lineage",
                {
                    "expected_lineage_id": str(lineage_id),
                    "observed_lineage_id": str(status.lineage_id),
                },
            )
        descriptor = HostQuarantineConfirmationDescriptor(
            format=HOST_QUARANTINE_CONFIRMATION_FORMAT,
            root_key=journal.header.root_key,
            required_release_id=journal.header.required_release_id,
            request_id=original.request_id,
            request_hash=original.request_hash,
            original_journal_revision=original.revision,
            scenario_lineage_id=lineage_id,
            original_activation_id=activation_id,
            disposition=disposition,
        )
        return _ResolutionContext(
            journal=journal,
            record=record,
            descriptor=descriptor,
        )


class _ResolutionContext(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        arbitrary_types_allowed=True,
    )

    journal: PendingJournal
    record: RequestRecord
    descriptor: HostQuarantineConfirmationDescriptor


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


def _validate_disposition(
    value: object,
) -> Literal["applied", "not_applied"]:
    if value not in {"applied", "not_applied"} or type(value) is not str:
        raise BridgeError(
            ErrorCode.INVALID_ARGUMENT,
            "disposition must be applied or not_applied",
        )
    return cast(Literal["applied", "not_applied"], value)


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
