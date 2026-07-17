from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.manifest import ManifestCatalog, ReleaseBinding
from cmo_agent_bridge.state.models import (
    DeliveryIntent,
    PendingExchange,
    PendingJournal,
    PendingJournalHeader,
    PendingPhase,
    _canonical_json_bytes,  # pyright: ignore[reportPrivateUsage]
    _canonical_model_bytes,  # pyright: ignore[reportPrivateUsage]
    _parse_duplicate_free_json,  # pyright: ignore[reportPrivateUsage]
)
from cmo_agent_bridge.state.revalidation import (
    DurableValidationError,
    LoadedPendingJournal,
    revalidate_pending_exchange,
)
from cmo_agent_bridge.transports.file_bridge.atomic_io import atomic_replace_bytes
from cmo_agent_bridge.transports.file_bridge.lock import RootLock
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NUMBER_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")
_PHASE_TRANSITIONS: dict[PendingPhase, frozenset[PendingPhase]] = {
    PendingPhase.PREPARED: frozenset({PendingPhase.PUBLISHED}),
    PendingPhase.PUBLISHED: frozenset(
        {
            PendingPhase.CANCEL_PUBLISHED,
            PendingPhase.RESPONSE_ACCEPTED,
            PendingPhase.QUARANTINED,
        }
    ),
    PendingPhase.CANCEL_PUBLISHED: frozenset(
        {
            PendingPhase.CANCEL_PUBLISHED,
            PendingPhase.RESPONSE_ACCEPTED,
            PendingPhase.QUARANTINED,
        }
    ),
    PendingPhase.RESPONSE_ACCEPTED: frozenset(
        {PendingPhase.IDLE_PUBLISHED, PendingPhase.QUARANTINED}
    ),
    PendingPhase.IDLE_PUBLISHED: frozenset({PendingPhase.QUARANTINED}),
    PendingPhase.QUARANTINED: frozenset(),
}


def _state_conflict(message: str, details: dict[str, object] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message, details)


def _journal_corrupt(message: str) -> BridgeError:
    return BridgeError(ErrorCode.JOURNAL_CORRUPT, message)


def _is_exact_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_RE.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class JournalRevisions:
    original: int
    reconcile_attempt: int | None

    def __post_init__(self) -> None:
        if not _is_exact_nonnegative_int(self.original):
            raise ValueError("original journal revision must be an exact non-negative integer")
        if self.reconcile_attempt is not None and not _is_exact_nonnegative_int(
            self.reconcile_attempt
        ):
            raise ValueError(
                "attempt journal revision must be null or an exact non-negative integer"
            )


@dataclass(frozen=True, slots=True)
class JournalDeleteExpectation:
    root_key: str
    required_release_id: str
    original_request_id: UUID
    reconcile_attempt_request_id: UUID | None
    revisions: JournalRevisions

    def __post_init__(self) -> None:
        if not _is_sha256(self.root_key) or not _is_sha256(self.required_release_id):
            raise ValueError("journal delete root and release must be lowercase SHA-256")
        if type(self.original_request_id) is not UUID:
            raise TypeError("journal delete original request ID must be an exact UUID")
        if (
            self.reconcile_attempt_request_id is not None
            and type(self.reconcile_attempt_request_id) is not UUID
        ):
            raise TypeError("journal delete attempt request ID must be null or an exact UUID")
        if type(self.revisions) is not JournalRevisions:
            raise TypeError("journal delete revisions must be exact")
        if (self.reconcile_attempt_request_id is None) != (
            self.revisions.reconcile_attempt is None
        ):
            raise ValueError(
                "journal delete attempt identity and revision must be both null or present"
            )


@dataclass(frozen=True, slots=True)
class HostResolvedJournalDeleteExpectation:
    root_key: str
    required_release_id: str
    original_request_id: UUID
    original_request_hash: str
    original_revision: int

    def __post_init__(self) -> None:
        if (
            not _is_sha256(self.root_key)
            or not _is_sha256(self.required_release_id)
            or not _is_sha256(self.original_request_hash)
        ):
            raise ValueError(
                "host-resolved journal delete hashes must be lowercase SHA-256"
            )
        if type(self.original_request_id) is not UUID:
            raise TypeError(
                "host-resolved journal delete request ID must be an exact UUID"
            )
        if not _is_exact_nonnegative_int(self.original_revision):
            raise ValueError(
                "host-resolved journal delete revision must be an exact non-negative integer"
            )


@dataclass(slots=True)
class _ContainerFrame:
    kind: str
    state: str


class _JsonScanner:
    def __init__(self, text: str) -> None:
        self.text = text
        self.length = len(text)

    def whitespace(self, index: int) -> int:
        while index < self.length and self.text[index] in " \t\r\n":
            index += 1
        return index

    def string(self, index: int, *, decode: bool) -> tuple[int, str | None]:
        if index >= self.length or self.text[index] != '"':
            raise ValueError("JSON string expected")
        start = index
        index += 1
        while index < self.length:
            character = self.text[index]
            if character == '"':
                end = index + 1
                if not decode:
                    return end, None
                value = json.loads(self.text[start:end])
                if type(value) is not str:
                    raise ValueError("decoded JSON string is invalid")
                return end, value
            if ord(character) < 0x20:
                raise ValueError("unescaped JSON control character")
            if character == "\\":
                index += 1
                if index >= self.length:
                    raise ValueError("incomplete JSON escape")
                escape = self.text[index]
                if escape == "u":
                    if index + 4 >= self.length or any(
                        value not in "0123456789abcdefABCDEF"
                        for value in self.text[index + 1 : index + 5]
                    ):
                        raise ValueError("invalid JSON unicode escape")
                    index += 5
                    continue
                if escape not in '"\\/bfnrt':
                    raise ValueError("invalid JSON escape")
            index += 1
        raise ValueError("unterminated JSON string")

    def scalar(self, index: int) -> int:
        if index >= self.length:
            raise ValueError("JSON value expected")
        character = self.text[index]
        if character == '"':
            return self.string(index, decode=False)[0]
        for literal in ("true", "false", "null"):
            if self.text.startswith(literal, index):
                return index + len(literal)
        number = _NUMBER_RE.match(self.text, index)
        if number is not None:
            return number.end()
        raise ValueError("invalid JSON scalar")

    def skip_value(self, index: int) -> int:
        index = self.whitespace(index)
        if index >= self.length:
            raise ValueError("JSON value expected")
        if self.text[index] not in "[{":
            return self.scalar(index)
        frames: list[_ContainerFrame] = []
        if self.text[index] == "{":
            frames.append(_ContainerFrame("object", "first_key_or_end"))
        else:
            frames.append(_ContainerFrame("array", "first_value_or_end"))
        index += 1
        while frames:
            frame = frames[-1]
            index = self.whitespace(index)
            if frame.kind == "object":
                if frame.state == "first_key_or_end":
                    if index < self.length and self.text[index] == "}":
                        frames.pop()
                        index += 1
                        continue
                    index = self.string(index, decode=False)[0]
                    frame.state = "colon"
                    continue
                if frame.state == "key":
                    index = self.string(index, decode=False)[0]
                    frame.state = "colon"
                    continue
                if frame.state == "colon":
                    if index >= self.length or self.text[index] != ":":
                        raise ValueError("JSON object member lacks colon")
                    frame.state = "value"
                    index += 1
                    continue
                if frame.state == "value":
                    index = self.whitespace(index)
                    frame.state = "comma_or_end"
                    if index < self.length and self.text[index] in "[{":
                        kind = "object" if self.text[index] == "{" else "array"
                        state = "first_key_or_end" if kind == "object" else "first_value_or_end"
                        frames.append(_ContainerFrame(kind, state))
                        index += 1
                    else:
                        index = self.scalar(index)
                    continue
                if index < self.length and self.text[index] == "}":
                    frames.pop()
                    index += 1
                    continue
                if index >= self.length or self.text[index] != ",":
                    raise ValueError("JSON object expects comma or end")
                frame.state = "key"
                index += 1
                continue

            if frame.state == "first_value_or_end":
                if index < self.length and self.text[index] == "]":
                    frames.pop()
                    index += 1
                    continue
                frame.state = "comma_or_end"
            elif frame.state == "value":
                frame.state = "comma_or_end"
            else:
                if index < self.length and self.text[index] == "]":
                    frames.pop()
                    index += 1
                    continue
                if index >= self.length or self.text[index] != ",":
                    raise ValueError("JSON array expects comma or end")
                frame.state = "value"
                index += 1
                continue
            index = self.whitespace(index)
            if index < self.length and self.text[index] in "[{":
                kind = "object" if self.text[index] == "{" else "array"
                state = "first_key_or_end" if kind == "object" else "first_value_or_end"
                frames.append(_ContainerFrame(kind, state))
                index += 1
            else:
                index = self.scalar(index)
        return index

    def top_members(self) -> dict[str, tuple[int, int]]:
        index = self.whitespace(0)
        if index >= self.length or self.text[index] != "{":
            raise ValueError("journal top level must be an object")
        index += 1
        members: dict[str, tuple[int, int]] = {}
        first = True
        while True:
            index = self.whitespace(index)
            if index < self.length and self.text[index] == "}":
                index += 1
                break
            if not first:
                if index >= self.length or self.text[index] != ",":
                    raise ValueError("journal top level expects comma")
                index = self.whitespace(index + 1)
                if index < self.length and self.text[index] == "}":
                    raise ValueError("journal top level has trailing comma")
            first = False
            index, key = self.string(index, decode=True)
            if key is None or key in members:
                raise ValueError("journal top-level member is duplicated")
            index = self.whitespace(index)
            if index >= self.length or self.text[index] != ":":
                raise ValueError("journal top-level member lacks colon")
            start = self.whitespace(index + 1)
            end = self.skip_value(start)
            members[key] = (start, end)
            index = self.whitespace(end)
        if self.whitespace(index) != self.length:
            raise ValueError("journal has trailing data")
        if set(members) != {"header", "original", "reconcile_attempt"}:
            raise ValueError("journal top-level members are not exact")
        return members


def _probe_header(raw: bytes) -> PendingJournalHeader:
    text = raw.decode("utf-8", errors="strict")
    scanner = _JsonScanner(text)
    members = scanner.top_members()
    start, end = members["header"]
    parsed = _parse_duplicate_free_json(text[start:end])
    if not isinstance(parsed, dict):
        raise ValueError("journal header must be an object")
    return PendingJournalHeader.model_validate(parsed)


def _revisions(journal: PendingJournal) -> JournalRevisions:
    return JournalRevisions(
        original=journal.original.revision,
        reconcile_attempt=(
            None if journal.reconcile_attempt is None else journal.reconcile_attempt.revision
        ),
    )


def _same_model(left: object, right: object) -> bool:
    if not hasattr(left, "model_dump") or not hasattr(right, "model_dump"):
        return False
    return _canonical_model_bytes(cast(PendingExchange, left)) == _canonical_model_bytes(
        cast(PendingExchange, right)
    )


class PendingJournalStore:
    def __init__(
        self,
        paths: FileBridgePaths,
        root_lock: RootLock,
        catalog: ManifestCatalog,
        *,
        max_journal_bytes: int,
        replace_retry_seconds: float,
    ) -> None:
        if type(paths) is not FileBridgePaths or type(root_lock) is not RootLock:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "journal store dependencies are invalid")
        if type(catalog) is not ManifestCatalog:
            raise BridgeError(ErrorCode.INVALID_ARGUMENT, "journal manifest catalog is invalid")
        if root_lock.path != paths.lock_file:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "journal store lock does not belong to its bridge root",
            )
        if type(max_journal_bytes) is not int or max_journal_bytes <= 0:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "maximum journal bytes must be an exact positive integer",
            )
        raw_retry = cast(object, replace_retry_seconds)
        if (
            isinstance(raw_retry, bool)
            or not isinstance(raw_retry, (int, float))
            or not math.isfinite(float(raw_retry))
            or float(raw_retry) < 0
        ):
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "journal replace retry seconds must be finite and non-negative",
            )
        self._paths = paths
        self._root_lock = root_lock
        self._catalog = catalog
        self._max_journal_bytes = max_journal_bytes
        self._replace_retry_seconds = float(raw_retry)

    def load(self) -> LoadedPendingJournal | None:
        self._require_bound_lock()
        raw = self._read_raw()
        if raw is None:
            return None
        try:
            header = _probe_header(raw)
        except (UnicodeError, ValidationError, TypeError, ValueError, RecursionError) as error:
            raise _journal_corrupt("pending journal header or top-level JSON is corrupt") from error
        if header.root_key != self._paths.root_key:
            raise _journal_corrupt("pending journal belongs to a different bridge root")
        if header.required_release_id != self._catalog.running_release_id:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "pending journal requires a different bridge release",
                {
                    "root_key": self._paths.root_key,
                    "required_release_id": header.required_release_id,
                    "running_release_id": self._catalog.running_release_id,
                },
            )

        try:
            tree = _parse_duplicate_free_json(raw)
            if _canonical_json_bytes(tree) != raw:
                raise ValueError("journal JSON is not canonical")
        except (UnicodeError, TypeError, ValueError, OverflowError, RecursionError) as error:
            raise _journal_corrupt(
                "pending journal body JSON is corrupt or noncanonical"
            ) from error
        try:
            binding = self._catalog.resolve_running(header.required_release_id)
        except BridgeError as error:
            if error.code is ErrorCode.MANIFEST_MISMATCH:
                raise
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "running release binding is inconsistent",
                {
                    "root_key": self._paths.root_key,
                    "required_release_id": header.required_release_id,
                    "running_release_id": self._catalog.running_release_id,
                },
            ) from error
        try:
            journal = PendingJournal.model_validate_json(raw, strict=True)
            if _canonical_json_bytes(journal.model_dump(mode="json")) != raw:
                raise ValueError("typed journal representation is not canonical")
            original = revalidate_pending_exchange(journal.original, binding=binding)
            attempt = (
                None
                if journal.reconcile_attempt is None
                else revalidate_pending_exchange(journal.reconcile_attempt, binding=binding)
            )
            self._validate_attempt_target(journal)
        except (
            ValidationError,
            DurableValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _journal_corrupt("pending journal semantic reconstruction failed") from error
        return LoadedPendingJournal(journal=journal, original=original, reconcile_attempt=attempt)

    def save(
        self,
        journal: PendingJournal,
        *,
        expected_revisions: JournalRevisions | None,
    ) -> JournalRevisions:
        self._require_bound_lock()
        existing = self.load()
        if existing is None:
            if expected_revisions is not None:
                raise _state_conflict("pending journal does not exist at expected revisions")
        else:
            if expected_revisions is None:
                raise _state_conflict("pending journal already exists")
            if type(expected_revisions) is not JournalRevisions:
                raise _state_conflict("expected journal revisions are invalid")
            if _revisions(existing.journal) != expected_revisions:
                raise _state_conflict("pending journal revisions changed")
        candidate, _binding = self._validate_candidate(journal)
        candidate_bytes = _canonical_json_bytes(candidate.model_dump(mode="json"))
        if len(candidate_bytes) > self._max_journal_bytes:
            raise _state_conflict("pending journal exceeds the configured byte limit")
        if existing is None:
            self._validate_create(candidate)
        else:
            self._validate_replace(existing.journal, candidate)
        self._paths.pending_file.parent.mkdir(parents=True, exist_ok=True)
        atomic_replace_bytes(
            self._paths.pending_file,
            candidate_bytes,
            retry_seconds=self._replace_retry_seconds,
        )
        return _revisions(candidate)

    def delete(self, expected: JournalDeleteExpectation) -> None:
        self._require_bound_lock()
        if type(expected) is not JournalDeleteExpectation:
            raise _state_conflict("journal delete expectation is invalid")
        loaded = self.load()
        if loaded is None:
            raise _state_conflict("pending journal does not exist")
        journal = loaded.journal
        attempt = journal.reconcile_attempt
        actual = JournalDeleteExpectation(
            root_key=journal.header.root_key,
            required_release_id=journal.header.required_release_id,
            original_request_id=journal.original.request_id,
            reconcile_attempt_request_id=None if attempt is None else attempt.request_id,
            revisions=_revisions(journal),
        )
        if actual != expected:
            raise _state_conflict("pending journal delete identity changed")
        allowed = (
            attempt is None
            and journal.original.state in {PendingPhase.PREPARED, PendingPhase.IDLE_PUBLISHED}
        ) or (
            attempt is not None
            and journal.original.state is PendingPhase.QUARANTINED
            and attempt.state is PendingPhase.IDLE_PUBLISHED
        )
        if not allowed:
            raise _state_conflict("pending journal is not at an allowed delete endpoint")
        try:
            self._paths.pending_file.unlink()
        except FileNotFoundError as error:
            raise _state_conflict(
                "pending journal disappeared before conditional delete"
            ) from error

    def delete_host_resolved(
        self,
        expected: HostResolvedJournalDeleteExpectation,
    ) -> None:
        self._require_bound_lock()
        if type(expected) is not HostResolvedJournalDeleteExpectation:
            raise _state_conflict(
                "host-resolved journal delete expectation is invalid"
            )
        loaded = self.load()
        if loaded is None:
            raise _state_conflict("pending journal does not exist")
        journal = loaded.journal
        original = journal.original
        actual = HostResolvedJournalDeleteExpectation(
            root_key=journal.header.root_key,
            required_release_id=journal.header.required_release_id,
            original_request_id=original.request_id,
            original_request_hash=original.request_hash,
            original_revision=original.revision,
        )
        if actual != expected:
            raise _state_conflict(
                "host-resolved journal delete identity changed"
            )
        if (
            journal.reconcile_attempt is not None
            or original.state is not PendingPhase.QUARANTINED
        ):
            raise _state_conflict(
                "host-resolved journal delete requires one quarantined original"
            )
        try:
            self._paths.pending_file.unlink()
        except FileNotFoundError as error:
            raise _state_conflict(
                "pending journal disappeared before conditional delete"
            ) from error

    def _read_raw(self) -> bytes | None:
        try:
            with self._paths.pending_file.open("rb") as stream:
                raw = stream.read(self._max_journal_bytes + 1)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise _journal_corrupt("pending journal could not be read") from error
        if len(raw) > self._max_journal_bytes:
            raise _journal_corrupt("pending journal exceeds the configured byte limit")
        return raw

    def _require_bound_lock(self) -> None:
        self._root_lock.require_acquired()
        if self._root_lock.path != self._paths.lock_file:
            raise _state_conflict("journal store root lock path changed after construction")

    def _validate_candidate(self, journal: PendingJournal) -> tuple[PendingJournal, ReleaseBinding]:
        if type(journal) is not PendingJournal:
            raise _state_conflict("pending journal candidate must be exact")
        try:
            candidate = PendingJournal.model_validate(
                journal.model_dump(mode="python", round_trip=True, warnings=False)
            )
            if candidate.header.root_key != self._paths.root_key:
                raise ValueError("candidate root does not match journal store")
            if candidate.header.required_release_id != self._catalog.running_release_id:
                raise ValueError("candidate release does not match running release")
            binding = self._catalog.resolve_running(candidate.header.required_release_id)
            revalidate_pending_exchange(candidate.original, binding=binding)
            if candidate.reconcile_attempt is not None:
                revalidate_pending_exchange(candidate.reconcile_attempt, binding=binding)
            self._validate_attempt_target(candidate)
            return candidate, binding
        except (
            BridgeError,
            ValidationError,
            DurableValidationError,
            TypeError,
            ValueError,
            OverflowError,
            RecursionError,
        ) as error:
            raise _state_conflict("pending journal candidate failed semantic validation") from error

    @staticmethod
    def _validate_attempt_target(journal: PendingJournal) -> None:
        attempt = journal.reconcile_attempt
        if attempt is None:
            return
        if attempt.original_target_request_id != journal.original.request_id:
            raise ValueError("reconcile attempt target ID does not match immutable original")
        if attempt.original_target_request_hash != journal.original.request_hash:
            raise ValueError("reconcile attempt target hash does not match immutable original")

    @staticmethod
    def _validate_create(candidate: PendingJournal) -> None:
        if (
            candidate.original.revision != 0
            or candidate.original.state is not PendingPhase.PREPARED
            or candidate.reconcile_attempt is not None
        ):
            raise _state_conflict(
                "new pending journal is not at the revision-zero prepared endpoint"
            )

    def _validate_replace(self, old: PendingJournal, new: PendingJournal) -> None:
        if _canonical_model_bytes(old.header) != _canonical_model_bytes(new.header):
            raise _state_conflict("pending journal header changed")
        old_attempt = old.reconcile_attempt
        new_attempt = new.reconcile_attempt
        if old_attempt is None and new_attempt is None:
            self._validate_exchange_advance(old.original, new.original)
            return
        if old_attempt is None and new_attempt is not None:
            if not _same_model(old.original, new.original):
                raise _state_conflict("immutable original changed while creating reconcile attempt")
            if new_attempt.revision != 0 or new_attempt.state is not PendingPhase.PREPARED:
                raise _state_conflict("new reconcile attempt is not revision-zero prepared")
            return
        if old_attempt is not None and new_attempt is None:
            raise _state_conflict("reconcile attempt cannot be removed")
        if old_attempt is None or new_attempt is None:
            raise AssertionError("unreachable reconcile attempt shape")
        if not _same_model(old.original, new.original):
            raise _state_conflict("immutable original changed after reconcile attempt creation")
        self._validate_exchange_advance(old_attempt, new_attempt)

    def _validate_exchange_advance(self, old: PendingExchange, new: PendingExchange) -> None:
        if new.revision != old.revision + 1:
            raise _state_conflict("pending exchange revision did not advance exactly once")
        if new.updated_at_ms < old.updated_at_ms:
            raise _state_conflict("pending exchange update time moved backwards")
        old_tree = old.model_dump(mode="json")
        new_tree = new.model_dump(mode="json")
        mutable = {
            "delivery_intents",
            "response_artifact",
            "settlement",
            "revision",
            "state",
            "updated_at_ms",
        }
        for field in old_tree:
            if field not in mutable and old_tree[field] != new_tree[field]:
                raise _state_conflict(f"pending exchange immutable field changed: {field}")
        self._validate_delivery_advance(old.delivery_intents, new.delivery_intents)
        if len(new.delivery_intents) == len(old.delivery_intents) + 1 and not (
            old.state is PendingPhase.PUBLISHED and new.state is PendingPhase.CANCEL_PUBLISHED
        ):
            raise _state_conflict("cancel intent append must enter cancel_published phase")
        if old.response_artifact is not None and _canonical_model_bytes(
            old.response_artifact
        ) != _canonical_model_bytes(new.response_artifact):
            raise _state_conflict("accepted response artifact changed after persistence")
        if old.settlement is not None and _canonical_model_bytes(
            old.settlement
        ) != _canonical_model_bytes(new.settlement):
            raise _state_conflict("accepted response settlement changed after persistence")
        if old.response_artifact is not None and new.response_artifact is None:
            raise _state_conflict("accepted response artifact cannot be removed")
        if new.state not in _PHASE_TRANSITIONS[old.state]:
            raise _state_conflict("pending exchange phase transition is not forward-only")
        if old.state is new.state and old.state is PendingPhase.CANCEL_PUBLISHED:
            old_cancel = next(
                intent for intent in old.delivery_intents if intent.delivery_kind == "cancel"
            )
            new_cancel = next(
                intent for intent in new.delivery_intents if intent.delivery_kind == "cancel"
            )
            if old_cancel.published_at_ms is not None or new_cancel.published_at_ms is None:
                raise _state_conflict("same-phase cancel save must record its one publication time")

    @staticmethod
    def _validate_delivery_advance(
        old: tuple[DeliveryIntent, ...], new: tuple[DeliveryIntent, ...]
    ) -> None:
        if len(new) not in {len(old), len(old) + 1}:
            raise _state_conflict("delivery intent set changed unexpectedly")
        for old_intent, new_intent in zip(old, new, strict=False):
            old_tree = old_intent.model_dump(mode="json")
            new_tree = new_intent.model_dump(mode="json")
            for field in old_tree:
                if field != "published_at_ms" and old_tree[field] != new_tree[field]:
                    raise _state_conflict("existing delivery intent identity changed")
            if old_intent.published_at_ms is not None:
                if new_intent.published_at_ms != old_intent.published_at_ms:
                    raise _state_conflict("delivery publication time changed after being set")
            elif new_intent.published_at_ms is not None and (
                new_intent.published_at_ms < new_intent.intended_at_ms
            ):
                raise _state_conflict("delivery publication time precedes intent")
        if len(new) == len(old) + 1:
            appended = new[-1]
            if appended.delivery_kind != "cancel" or appended.published_at_ms is not None:
                raise _state_conflict("only one unpublished cancel intent may be appended")
