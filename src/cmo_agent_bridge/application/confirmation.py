from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
import secrets
import sqlite3
from collections.abc import Callable, Mapping
from typing import Final, Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    JsonValue,
    StrictInt,
    StrictStr,
    ValidationError,
    field_validator,
    model_validator,
)

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.runtime import Sha256
from cmo_agent_bridge.state.sqlite import StateDatabase


_CONFIRMATION_LIFETIME_MS = 60_000
_SQLITE_INT_MAX = 2**63 - 1
_TOKEN_BYTES = 32
_TOKEN_CHARACTERS = 43
_TOKEN_INSERT_ATTEMPTS = 3
_URLSAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]+$")
_BINDING_FORMAT_RE = re.compile(r"^cmo-agent-bridge/[a-z0-9]+(?:-[a-z0-9]+)*/[1-9][0-9]*$")

DESTRUCTIVE_CONFIRMATION_FORMAT: Final[Literal["cmo-agent-bridge/destructive-confirmation/1"]] = (
    "cmo-agent-bridge/destructive-confirmation/1"
)
HOST_QUARANTINE_CONFIRMATION_FORMAT: Final[
    Literal["cmo-agent-bridge/host-quarantine-confirmation/1"]
] = "cmo-agent-bridge/host-quarantine-confirmation/1"
OBSOLETE_QUARANTINE_ABANDONMENT_CONFIRMATION_FORMAT: Final[
    Literal["cmo-agent-bridge/obsolete-quarantine-abandonment-confirmation/1"]
] = "cmo-agent-bridge/obsolete-quarantine-abandonment-confirmation/1"


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _state_conflict(message: str) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message)


def _policy_denied() -> BridgeError:
    return BridgeError(ErrorCode.POLICY_DENIED, "confirmation denied")


def _round_trip_model(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="python", round_trip=True, warnings=False)
    return value


class DestructiveTarget(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    guid: str
    name: str
    type: str

    @field_validator("guid", "name", "type", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("destructive target strings must be exact")
        return value

    @field_validator("guid")
    @classmethod
    def validate_nonempty_guid(cls, value: str) -> str:
        if not value:
            raise ValueError("destructive target GUID must be nonempty")
        return value


class DestructiveConfirmationDescriptor(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    format: Literal["cmo-agent-bridge/destructive-confirmation/1"]
    root_key: Sha256
    operation: Literal["unit.delete", "mission.delete"]
    public_arguments: dict[str, JsonValue]
    resolved_target: DestructiveTarget
    scenario_lineage_id: UUID
    reserved_activation_id: UUID
    release_id: Sha256

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_target(cls, value: object) -> object:
        normalized = _round_trip_model(value)
        if not isinstance(normalized, Mapping):
            return normalized
        values: dict[str, object] = dict(cast(Mapping[str, object], normalized))
        if "resolved_target" in values:
            values["resolved_target"] = _round_trip_model(values["resolved_target"])
        return values

    @field_validator("format", "root_key", "operation", "release_id", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("destructive descriptor strings must be exact")
        return value

    @field_validator("public_arguments", mode="before")
    @classmethod
    def validate_exact_public_arguments(cls, value: object) -> object:
        if type(value) is not dict:
            raise ValueError("destructive public arguments must be an exact dictionary")
        arguments = cast(dict[object, object], value)
        if any(type(key) is not str or type(item) is not str for key, item in arguments.items()):
            raise ValueError("destructive public arguments must contain exact strings")
        return arguments

    @field_validator("scenario_lineage_id", "reserved_activation_id", mode="before")
    @classmethod
    def validate_exact_uuids(cls, value: object) -> object:
        if type(value) is not UUID:
            raise ValueError("destructive descriptor UUIDs must be exact")
        return value

    @field_validator("reserved_activation_id")
    @classmethod
    def validate_reserved_activation_uuid4(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("reserved activation ID must be UUIDv4")
        return value

    @model_validator(mode="after")
    def validate_operation_arguments(self) -> DestructiveConfirmationDescriptor:
        if type(self.resolved_target) is not DestructiveTarget:
            raise ValueError("resolved destructive target must be exact")
        if self.operation == "unit.delete":
            if self.public_arguments != {"unit_guid": self.resolved_target.guid}:
                raise ValueError("unit delete arguments do not match the resolved target")
            return self
        if (
            set(self.public_arguments) != {"side_guid", "mission_guid"}
            or not self.public_arguments["side_guid"]
            or not self.public_arguments["mission_guid"]
            or self.public_arguments["mission_guid"] != self.resolved_target.guid
        ):
            raise ValueError("mission delete arguments do not match the resolved target")
        return self


class ConfirmationBinding(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    root_key: Sha256
    operation: StrictStr
    binding_format: StrictStr
    binding_sha256: Sha256
    lineage_id: UUID
    activation_id: UUID

    @field_validator("root_key", "operation", "binding_format", "binding_sha256", mode="before")
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("confirmation binding strings must be exact")
        return value

    @field_validator("operation")
    @classmethod
    def validate_operation(cls, value: str) -> str:
        if not value:
            raise ValueError("confirmation operation must be nonempty")
        return value

    @field_validator("binding_format")
    @classmethod
    def validate_binding_format(cls, value: str) -> str:
        if _BINDING_FORMAT_RE.fullmatch(value) is None:
            raise ValueError("confirmation binding format must be versioned")
        return value

    @field_validator("lineage_id", "activation_id")
    @classmethod
    def validate_exact_uuids(cls, value: UUID) -> UUID:
        if type(value) is not UUID:
            raise ValueError("confirmation binding UUIDs must be exact")
        return value


class HostQuarantineConfirmationDescriptor(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    format: Literal["cmo-agent-bridge/host-quarantine-confirmation/1"]
    root_key: Sha256
    required_release_id: Sha256
    request_id: UUID
    request_hash: Sha256
    original_journal_revision: StrictInt
    scenario_lineage_id: UUID
    original_activation_id: UUID
    disposition: Literal["applied", "not_applied"]

    @field_validator(
        "format",
        "root_key",
        "required_release_id",
        "request_hash",
        "disposition",
        mode="before",
    )
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("host quarantine descriptor strings must be exact")
        return value

    @field_validator(
        "request_id",
        "scenario_lineage_id",
        "original_activation_id",
        mode="before",
    )
    @classmethod
    def validate_exact_uuids(cls, value: object) -> object:
        if type(value) is not UUID:
            raise ValueError("host quarantine descriptor UUIDs must be exact")
        return value

    @field_validator("original_journal_revision")
    @classmethod
    def validate_revision(cls, value: int) -> int:
        if type(value) is not int or value < 0:
            raise ValueError("host quarantine journal revision must be non-negative")
        return value


class ObsoleteQuarantineAbandonmentConfirmationDescriptor(BaseModel):
    """Exact binding for abandoning one obsolete scenario lineage.

    This is deliberately distinct from the ordinary host-quarantine workflow:
    a token issued for one workflow cannot authorize the other.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    format: Literal[
        "cmo-agent-bridge/obsolete-quarantine-abandonment-confirmation/1"
    ]
    root_key: Sha256
    required_release_id: Sha256
    request_id: UUID
    request_hash: Sha256
    original_journal_revision: StrictInt
    scenario_lineage_id: UUID
    original_activation_id: UUID
    disposition: Literal["not_applied"]

    @field_validator(
        "format",
        "root_key",
        "required_release_id",
        "request_hash",
        "disposition",
        mode="before",
    )
    @classmethod
    def validate_exact_strings(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("obsolete abandonment descriptor strings must be exact")
        return value

    @field_validator(
        "request_id",
        "scenario_lineage_id",
        "original_activation_id",
        mode="before",
    )
    @classmethod
    def validate_exact_uuids(cls, value: object) -> object:
        if type(value) is not UUID:
            raise ValueError("obsolete abandonment descriptor UUIDs must be exact")
        return value

    @field_validator("original_journal_revision")
    @classmethod
    def validate_revision(cls, value: int) -> int:
        if type(value) is not int or value < 0:
            raise ValueError("obsolete abandonment journal revision must be non-negative")
        return value

def _require_destructive_descriptor(
    value: DestructiveConfirmationDescriptor,
) -> DestructiveConfirmationDescriptor:
    if type(value) is not DestructiveConfirmationDescriptor:
        raise TypeError("destructive confirmation descriptor must be exact")
    return DestructiveConfirmationDescriptor.model_validate(
        value.model_dump(mode="python", round_trip=True, warnings=False)
    )


def canonical_destructive_confirmation_bytes(
    descriptor: DestructiveConfirmationDescriptor,
) -> bytes:
    candidate = _require_destructive_descriptor(descriptor)
    return json.dumps(
        candidate.model_dump(mode="json", round_trip=True, warnings=False),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def destructive_confirmation_binding(
    descriptor: DestructiveConfirmationDescriptor,
) -> ConfirmationBinding:
    candidate = _require_destructive_descriptor(descriptor)
    digest = hashlib.sha256(canonical_destructive_confirmation_bytes(candidate)).hexdigest()
    return ConfirmationBinding(
        root_key=candidate.root_key,
        operation=candidate.operation,
        binding_format=DESTRUCTIVE_CONFIRMATION_FORMAT,
        binding_sha256=digest,
        lineage_id=candidate.scenario_lineage_id,
        activation_id=candidate.reserved_activation_id,
    )


def canonical_host_quarantine_confirmation_bytes(
    descriptor: HostQuarantineConfirmationDescriptor,
) -> bytes:
    if type(descriptor) is not HostQuarantineConfirmationDescriptor:
        raise TypeError("host quarantine confirmation descriptor must be exact")
    candidate = HostQuarantineConfirmationDescriptor.model_validate(
        descriptor.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return json.dumps(
        candidate.model_dump(mode="json", round_trip=True, warnings=False),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def host_quarantine_confirmation_binding(
    descriptor: HostQuarantineConfirmationDescriptor,
) -> ConfirmationBinding:
    if type(descriptor) is not HostQuarantineConfirmationDescriptor:
        raise TypeError("host quarantine confirmation descriptor must be exact")
    candidate = HostQuarantineConfirmationDescriptor.model_validate(
        descriptor.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return ConfirmationBinding(
        root_key=candidate.root_key,
        operation="host.quarantine.resolve",
        binding_format=HOST_QUARANTINE_CONFIRMATION_FORMAT,
        binding_sha256=hashlib.sha256(
            canonical_host_quarantine_confirmation_bytes(candidate)
        ).hexdigest(),
        lineage_id=candidate.scenario_lineage_id,
        activation_id=candidate.original_activation_id,
    )


def canonical_obsolete_quarantine_abandonment_confirmation_bytes(
    descriptor: ObsoleteQuarantineAbandonmentConfirmationDescriptor,
) -> bytes:
    if type(descriptor) is not ObsoleteQuarantineAbandonmentConfirmationDescriptor:
        raise TypeError("obsolete abandonment confirmation descriptor must be exact")
    candidate = ObsoleteQuarantineAbandonmentConfirmationDescriptor.model_validate(
        descriptor.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return json.dumps(
        candidate.model_dump(mode="json", round_trip=True, warnings=False),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def obsolete_quarantine_abandonment_confirmation_binding(
    descriptor: ObsoleteQuarantineAbandonmentConfirmationDescriptor,
) -> ConfirmationBinding:
    if type(descriptor) is not ObsoleteQuarantineAbandonmentConfirmationDescriptor:
        raise TypeError("obsolete abandonment confirmation descriptor must be exact")
    candidate = ObsoleteQuarantineAbandonmentConfirmationDescriptor.model_validate(
        descriptor.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return ConfirmationBinding(
        root_key=candidate.root_key,
        operation="host.quarantine.abandon_obsolete_lineage",
        binding_format=OBSOLETE_QUARANTINE_ABANDONMENT_CONFIRMATION_FORMAT,
        binding_sha256=hashlib.sha256(
            canonical_obsolete_quarantine_abandonment_confirmation_bytes(candidate)
        ).hexdigest(),
        lineage_id=candidate.scenario_lineage_id,
        activation_id=candidate.original_activation_id,
    )


class IssuedConfirmation(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    token: StrictStr
    expires_at_ms: StrictInt

    @field_validator("token", mode="before")
    @classmethod
    def validate_exact_token(cls, value: object) -> object:
        if type(value) is not str:
            raise ValueError("confirmation token must be an exact string")
        return value

    @field_validator("expires_at_ms")
    @classmethod
    def validate_expiry(cls, value: int) -> int:
        if type(value) is not int or not 0 <= value <= _SQLITE_INT_MAX:
            raise ValueError("confirmation expiry must be an exact SQLite epoch")
        return value


class ConfirmationTokenStore:
    def __init__(
        self,
        database: StateDatabase,
        *,
        token_factory: Callable[[int], str] | None = None,
    ) -> None:
        if type(database) is not StateDatabase:
            raise _invalid_argument("confirmation store database is invalid")
        if token_factory is not None and not callable(token_factory):
            raise _invalid_argument("confirmation token factory is invalid")
        self._database = database
        self._token_factory = secrets.token_urlsafe if token_factory is None else token_factory

    def issue(self, binding: ConfirmationBinding, *, now_ms: int) -> IssuedConfirmation:
        candidate = self._require_binding(binding)
        issued_at_ms = self._require_epoch(
            now_ms,
            maximum=_SQLITE_INT_MAX - _CONFIRMATION_LIFETIME_MS,
            label="confirmation issue epoch",
        )
        expires_at_ms = issued_at_ms + _CONFIRMATION_LIFETIME_MS

        for _ in range(_TOKEN_INSERT_ATTEMPTS):
            token = self._generate_token()
            token_sha256 = hashlib.sha256(token.encode("utf-8")).hexdigest()
            try:
                with self._database._transaction(  # pyright: ignore[reportPrivateUsage]
                    write=True
                ) as connection:
                    connection.execute(
                        """
                        INSERT INTO confirmations(
                            token_sha256,root_key,operation,binding_format,binding_sha256,
                            lineage_id,activation_id,expires_at_ms,used_at_ms
                        ) VALUES(?,?,?,?,?,?,?,?,NULL)
                        """,
                        (
                            token_sha256,
                            candidate.root_key,
                            candidate.operation,
                            candidate.binding_format,
                            candidate.binding_sha256,
                            str(candidate.lineage_id),
                            str(candidate.activation_id),
                            expires_at_ms,
                        ),
                    )
            except sqlite3.IntegrityError:
                continue
            return IssuedConfirmation(token=token, expires_at_ms=expires_at_ms)
        raise _state_conflict("confirmation token collision limit reached")

    def lookup_active(
        self,
        token: str,
        *,
        now_ms: int,
    ) -> ConfirmationBinding:
        lookup_at_ms = self._require_epoch(
            now_ms,
            maximum=_SQLITE_INT_MAX,
            label="confirmation lookup epoch",
        )
        if not self._valid_token(token):
            raise _policy_denied()
        token_sha256 = hashlib.sha256(token.encode("utf-8")).hexdigest()

        with self._database._transaction(  # pyright: ignore[reportPrivateUsage]
            write=False
        ) as connection:
            row = connection.execute(
                """
                SELECT root_key,operation,binding_format,binding_sha256,lineage_id,activation_id
                FROM confirmations
                WHERE token_sha256=?
                  AND used_at_ms IS NULL
                  AND typeof(expires_at_ms)='integer'
                  AND expires_at_ms > ?
                """,
                (token_sha256, lookup_at_ms),
            ).fetchone()
        if row is None:
            raise _policy_denied()

        try:
            stored = {name: row[name] for name in row.keys()}
            if any(type(value) is not str for value in stored.values()):
                raise ValueError("confirmation row strings must be exact")
            lineage_text = cast(str, stored["lineage_id"])
            activation_text = cast(str, stored["activation_id"])
            lineage_id = UUID(lineage_text)
            activation_id = UUID(activation_text)
            if str(lineage_id) != lineage_text or str(activation_id) != activation_text:
                raise ValueError("confirmation row UUIDs must be canonical")
            return ConfirmationBinding.model_validate(
                {
                    "root_key": stored["root_key"],
                    "operation": stored["operation"],
                    "binding_format": stored["binding_format"],
                    "binding_sha256": stored["binding_sha256"],
                    "lineage_id": lineage_id,
                    "activation_id": activation_id,
                }
            )
        except (AttributeError, KeyError, TypeError, ValidationError, ValueError) as error:
            raise _policy_denied() from error

    def consume(
        self,
        token: str,
        binding: ConfirmationBinding,
        *,
        now_ms: int,
    ) -> Sha256:
        candidate = self._require_binding(binding)
        consumed_at_ms = self._require_epoch(
            now_ms,
            maximum=_SQLITE_INT_MAX,
            label="confirmation consume epoch",
        )
        if not self._valid_token(token):
            raise _policy_denied()
        token_sha256 = hashlib.sha256(token.encode("utf-8")).hexdigest()

        with self._database._transaction(write=True) as connection:  # pyright: ignore[reportPrivateUsage]
            cursor = connection.execute(
                """
                UPDATE confirmations
                SET used_at_ms=?
                WHERE token_sha256=?
                  AND root_key=?
                  AND operation=?
                  AND binding_format=?
                  AND binding_sha256=?
                  AND lineage_id=?
                  AND activation_id=?
                  AND used_at_ms IS NULL
                  AND typeof(expires_at_ms)='integer'
                  AND expires_at_ms > ?
                """,
                (
                    consumed_at_ms,
                    token_sha256,
                    candidate.root_key,
                    candidate.operation,
                    candidate.binding_format,
                    candidate.binding_sha256,
                    str(candidate.lineage_id),
                    str(candidate.activation_id),
                    consumed_at_ms,
                ),
            )
            if cursor.rowcount == 1:
                return token_sha256
            if cursor.rowcount == 0:
                raise _policy_denied()
            raise _state_conflict("confirmation consume affected an unexpected row count")

    def _generate_token(self) -> str:
        try:
            token = self._token_factory(_TOKEN_BYTES)
        except Exception as error:
            raise _state_conflict("confirmation token generation failed") from error
        if not self._valid_token(token):
            raise _state_conflict("confirmation token generator returned invalid output")
        return token

    @staticmethod
    def _valid_token(value: object) -> bool:
        if (
            type(value) is not str
            or len(value) != _TOKEN_CHARACTERS
            or _URLSAFE_TOKEN_RE.fullmatch(value) is None
        ):
            return False
        padding = "=" * (-len(value) % 4)
        try:
            decoded = base64.b64decode(
                (value + padding).encode("ascii"), altchars=b"-_", validate=True
            )
        except (UnicodeEncodeError, binascii.Error, ValueError):
            return False
        canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
        return len(decoded) == _TOKEN_BYTES and canonical == value

    @staticmethod
    def _require_binding(value: ConfirmationBinding) -> ConfirmationBinding:
        try:
            if type(value) is not ConfirmationBinding:
                raise TypeError("confirmation binding must be exact")
            return ConfirmationBinding.model_validate(
                value.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (AttributeError, TypeError, ValidationError, ValueError) as error:
            raise _invalid_argument("confirmation binding is invalid") from error

    @staticmethod
    def _require_epoch(value: int, *, maximum: int, label: str) -> int:
        if type(value) is not int or not 0 <= value <= maximum:
            raise _invalid_argument(f"{label} is invalid")
        return value
