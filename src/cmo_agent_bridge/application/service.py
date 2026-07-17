from __future__ import annotations

import base64
import binascii
import hashlib
import inspect
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue, TypeAdapter, ValidationError
from pydantic_core import PydanticSerializationError

from cmo_agent_bridge.application.confirmation import (
    DESTRUCTIVE_CONFIRMATION_FORMAT,
    ConfirmationBinding,
    DestructiveConfirmationDescriptor,
    DestructiveTarget,
    IssuedConfirmation,
    destructive_confirmation_binding,
)
from cmo_agent_bridge.application.models import InvocationOutcome
from cmo_agent_bridge.application.ports import (
    CompatibilityPolicyPort,
    CompatibilityProbePort,
    LocalOperationPort,
    WallClockPort,
)
from cmo_agent_bridge.application.session_service import (
    PreparedReadAttempt,
    SessionActivation,
)
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.models import (
    BridgeStatusArgs,
    ConfirmedDeleteMissionWireArgs,
    ConfirmedDeleteUnitWireArgs,
    DeleteMissionArgs,
    DeleteResult,
    DeleteUnitArgs,
    DestructivePreviewResult,
    MissionResult,
    UnitResult,
)
from cmo_agent_bridge.operations.registry import (
    OperationContract,
    OperationRegistry,
    ResolvedInvocation,
)
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import (
    RuntimeSnapshot,
    Sha256,
    derive_runtime_tag,
    revalidate_runtime_snapshot,
)
from cmo_agent_bridge.transports.file_bridge.models import BridgeChannel, BridgeTransport


_ERROR_PAYLOAD_ADAPTER: TypeAdapter[dict[str, JsonValue]] = TypeAdapter(
    dict[str, JsonValue],
    config=ConfigDict(strict=True),
)
_JSON_VALUE_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(
    JsonValue,
    config=ConfigDict(strict=True),
)
_LOCAL_OPERATION_NAMES = frozenset({"bridge.prepare", "bridge.doctor", "bridge.uninstall"})
_DESTRUCTIVE_OPERATION_NAMES = frozenset({"unit.delete", "mission.delete"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_URLSAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_CONFIRMATION_LIFETIME_MS = 60_000
_SQLITE_INT_MAX = 2**63 - 1
_DESTRUCTIVE_IMPACTS = {
    "unit.delete": (
        "Permanently deletes the resolved unit from the scenario. "
        "The bridge cannot undo this action."
    ),
    "mission.delete": (
        "Permanently deletes the resolved mission from the scenario. "
        "The bridge cannot undo this action."
    ),
}
_DESTRUCTIVE_DESCRIPTOR_TYPE = DestructiveConfirmationDescriptor
_CONFIRMATION_BINDING_TYPE = ConfirmationBinding
_ISSUED_CONFIRMATION_TYPE = IssuedConfirmation
_DESTRUCTIVE_PREVIEW_TYPE = DestructivePreviewResult


class _ForeignArtifactError(BridgeError):
    pass


@dataclass(frozen=True, slots=True)
class _DestructiveReadResult:
    activation: SessionActivation
    command: ExchangeCommand
    artifact: ResponseArtifact


class _DestructiveIdentityLedger:
    def __init__(self, candidate: UUID, operation: str) -> None:
        self._candidate = candidate
        self._operation = operation
        self._visible_ids: set[UUID] = {candidate}
        self._request_id: UUID | None = None

    @property
    def request_id(self) -> UUID | None:
        return self._request_id

    def record_status(self, activation: SessionActivation) -> None:
        self._record(activation.status_request_id, "status request ID")

    def record_target_read(self, command: ExchangeCommand) -> None:
        self._record(command.request_id, "target-read request ID")
        self._request_id = command.request_id

    def record_delete(self, command: ExchangeCommand) -> None:
        self._record(command.request_id, "delete request ID")
        self._request_id = command.request_id

    def clear_request_id(self) -> None:
        self._request_id = None

    def _record(self, value: object, label: str) -> None:
        request_id = _require_uuid4(value, self._operation, label)
        if request_id in self._visible_ids:
            raise _orchestration_protocol_error(
                self._operation,
                f"{label} reused a visible destructive identity",
            )
        self._visible_ids.add(request_id)


class _SessionCoordinator(Protocol):
    @property
    def runtime_snapshot(self) -> RuntimeSnapshot: ...

    @property
    def root_key(self) -> Sha256: ...

    def reserve_activation_candidate(self) -> UUID: ...

    async def handshake(
        self,
        channel: BridgeChannel,
        *,
        accept_lineage_id: UUID | None = None,
        reserved_activation_candidate: UUID | None = None,
    ) -> SessionActivation: ...

    def prepare_exchange(
        self,
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> ExchangeCommand: ...

    async def prepare_read_attempt(
        self,
        channel: BridgeChannel,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> PreparedReadAttempt: ...

    async def reprepare_read_after_activation_mismatch(
        self,
        channel: BridgeChannel,
        prior: PreparedReadAttempt,
        response: ResponseArtifact,
    ) -> PreparedReadAttempt: ...


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


def _has_method(value: object, name: str, *, is_async: bool) -> bool:
    method = inspect.getattr_static(value, name)
    return callable(method) and inspect.iscoroutinefunction(method) is is_async


def _invalid_argument(message: str, details: dict[str, JsonValue] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message, details)


def _argument_validation_error(operation: str, error: ValidationError) -> BridgeError:
    return _invalid_argument(
        "operation arguments are invalid",
        {
            "operation": operation,
            "validation_errors": cast(
                JsonValue,
                json.loads(error.json(include_url=False, include_context=False)),
            ),
        },
    )


def _protocol_error(operation: str) -> BridgeError:
    return BridgeError(
        ErrorCode.PROTOCOL_ERROR,
        "operation result failed public validation",
        {"operation": operation},
    )


def _orchestration_protocol_error(operation: str, message: str) -> BridgeError:
    return BridgeError(
        ErrorCode.PROTOCOL_ERROR,
        message,
        {"operation": operation},
    )


def _foreign_artifact(operation: str) -> _ForeignArtifactError:
    return _ForeignArtifactError(
        ErrorCode.PROTOCOL_ERROR,
        "response artifact does not belong to the prepared command",
        {"operation": operation},
    )


def _canonical_json_equal(left: object, right: object) -> bool:
    try:
        return json.dumps(
            left,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ) == json.dumps(
            right,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, OverflowError, RecursionError):
        return False


def _validated_root_key(value: object) -> Sha256:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("root key must be an exact lowercase SHA-256")
    return value


def _require_uuid4(value: object, operation: str, label: str) -> UUID:
    if type(value) is not UUID or value.version != 4:
        raise _orchestration_protocol_error(operation, f"{label} is invalid")
    return value


def _revalidate_destructive_descriptor(
    value: object,
    operation: str,
) -> DestructiveConfirmationDescriptor:
    try:
        if type(value) is not _DESTRUCTIVE_DESCRIPTOR_TYPE:
            raise TypeError("destructive descriptor must be exact")
        descriptor = value
        return _DESTRUCTIVE_DESCRIPTOR_TYPE.model_validate(
            descriptor.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except (AttributeError, TypeError, ValidationError, ValueError) as error:
        raise _orchestration_protocol_error(
            operation,
            "destructive confirmation descriptor is invalid",
        ) from error


def _revalidate_confirmation_binding(
    value: object,
    operation: str,
) -> ConfirmationBinding:
    try:
        if type(value) is not _CONFIRMATION_BINDING_TYPE:
            raise TypeError("confirmation binding must be exact")
        binding = value
        return _CONFIRMATION_BINDING_TYPE.model_validate(
            binding.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except (AttributeError, TypeError, ValidationError, ValueError) as error:
        raise _orchestration_protocol_error(
            operation,
            "destructive confirmation binding is invalid",
        ) from error


def _revalidate_issued_confirmation(
    value: object,
    operation: str,
) -> IssuedConfirmation:
    try:
        if type(value) is not _ISSUED_CONFIRMATION_TYPE:
            raise TypeError("issued confirmation must be exact")
        issued = value
        return _ISSUED_CONFIRMATION_TYPE.model_validate(
            issued.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except (AttributeError, TypeError, ValidationError, ValueError) as error:
        raise _orchestration_protocol_error(
            operation,
            "confirmation store returned an invalid issuance",
        ) from error


def _is_canonical_confirmation_token(value: object) -> bool:
    if type(value) is not str or _URLSAFE_TOKEN_RE.fullmatch(value) is None:
        return False
    try:
        decoded = base64.b64decode(
            (value + "=").encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
    except (UnicodeEncodeError, binascii.Error, ValueError):
        return False
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    return len(decoded) == 32 and canonical == value


def _validated_preview_epoch(value: object, operation: str) -> int:
    if type(value) is not int or value < 0 or value > _SQLITE_INT_MAX - _CONFIRMATION_LIFETIME_MS:
        raise _orchestration_protocol_error(
            operation,
            "wall clock returned an invalid confirmation epoch",
        )
    return value


def _validated_confirmation_epoch(value: object, operation: str) -> int:
    if type(value) is not int or value < 0 or value > _SQLITE_INT_MAX:
        raise _orchestration_protocol_error(
            operation,
            "wall clock returned an invalid confirmation epoch",
        )
    return value


def _confirmation_denied() -> BridgeError:
    return BridgeError(ErrorCode.POLICY_DENIED, "confirmation denied")


def _redact_secret_substrings(value: str, secrets: tuple[str, ...]) -> str:
    redacted = value
    while True:
        prior = redacted
        for secret in secrets:
            redacted = redacted.replace(secret, "")
        if redacted == prior:
            return redacted


def _redact_confirmation_value(value: object, secrets: tuple[str, ...]) -> object:
    if isinstance(value, str):
        return _redact_secret_substrings(value, secrets)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {
            (
                _redact_secret_substrings(key, secrets) if isinstance(key, str) else key
            ): _redact_confirmation_value(item, secrets)
            for key, item in mapping.items()
        }
    if isinstance(value, list | tuple):
        sequence = cast(list[object] | tuple[object, ...], value)
        return [_redact_confirmation_value(item, secrets) for item in sequence]
    return value


def _sanitize_confirmation_error(
    error: BridgeError,
    sensitive_values: list[str],
) -> BridgeError:
    secrets = tuple(
        sorted(
            {value for value in sensitive_values if type(value) is str and value},
            key=len,
            reverse=True,
        )
    )
    if not secrets:
        return error
    try:
        message = _redact_secret_substrings(error.message, secrets)
        details = cast(
            dict[str, object],
            _redact_confirmation_value(error.details, secrets),
        )
    except (AttributeError, RecursionError, TypeError, ValueError):
        return BridgeError(error.code, "operation failed")
    if message == error.message and details == error.details:
        return error
    return BridgeError(error.code, message, details)


def _validated_confirmation_proof(
    token: object,
    proof: object,
    operation: str,
) -> Sha256:
    if (
        not _is_canonical_confirmation_token(token)
        or type(token) is not str
        or type(proof) is not str
        or _SHA256_RE.fullmatch(proof) is None
        or proof != hashlib.sha256(token.encode("utf-8")).hexdigest()
    ):
        raise _orchestration_protocol_error(
            operation,
            "confirmation store returned an invalid proof",
        )
    return proof


def _preview_expiry_utc(expires_at_ms: int, operation: str) -> datetime:
    seconds, milliseconds = divmod(expires_at_ms, 1000)
    try:
        return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(milliseconds=milliseconds)
    except (OSError, OverflowError, ValueError) as error:
        raise _orchestration_protocol_error(
            operation,
            "confirmation expiry is outside the supported UTC range",
        ) from error


def _validated_destructive_preview(
    value: object,
    operation: str,
) -> JsonValue:
    try:
        if type(value) is not _DESTRUCTIVE_PREVIEW_TYPE:
            raise TypeError("destructive preview result must be exact")
        preview = value
        validated = _DESTRUCTIVE_PREVIEW_TYPE.model_validate(
            preview.model_dump(mode="python", round_trip=True, warnings=False)
        )
        dumped = validated.model_dump(mode="json", warnings="error")
        result = _JSON_VALUE_ADAPTER.validate_python(dumped)
        json.dumps(
            result,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return result
    except (
        AttributeError,
        PydanticSerializationError,
        TypeError,
        ValidationError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise _protocol_error(operation) from error


def _round_trip_models(value: object) -> object:
    if isinstance(value, BaseModel):
        return _round_trip_models(value.model_dump(mode="python", round_trip=True, warnings=False))
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {key: _round_trip_models(item) for key, item in mapping.items()}
    if isinstance(value, list):
        sequence = cast(list[object], value)
        return [_round_trip_models(item) for item in sequence]
    if isinstance(value, tuple):
        sequence = cast(tuple[object, ...], value)
        return [_round_trip_models(item) for item in sequence]
    return value


def _validated_public_result(invocation: ResolvedInvocation, candidate: object) -> JsonValue:
    try:
        validated = invocation.public_result_adapter.validate_python(_round_trip_models(candidate))
        if not isinstance(validated, BaseModel):
            raise TypeError("public result adapter did not produce a Pydantic model")
        dumped = validated.model_dump(mode="json", warnings="error")
        json_value = _JSON_VALUE_ADAPTER.validate_python(dumped)
        json.dumps(
            json_value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return json_value
    except (
        PydanticSerializationError,
        TypeError,
        ValidationError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise _protocol_error(invocation.contract.name) from error


def _failure_outcome(request_id: UUID | None, error: BridgeError) -> InvocationOutcome:
    payload = _ERROR_PAYLOAD_ADAPTER.validate_python(
        {
            "code": error.code.value,
            "message": error.message,
            "details": error.details,
        }
    )
    json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=request_id,
        ok=False,
        result=None,
        error=payload,
    )


def _success_outcome(request_id: UUID | None, result: JsonValue) -> InvocationOutcome:
    return InvocationOutcome(
        protocol="cmo-agent-bridge/1",
        request_id=request_id,
        ok=True,
        result=result,
        error=None,
    )


class BridgeApplication:
    def __init__(
        self,
        *,
        registry: OperationRegistry,
        transport: BridgeTransport,
        sessions: _SessionCoordinator,
        policy: CompatibilityPolicyPort,
        confirmations: _ConfirmationStore,
        wall_clock: WallClockPort,
        local_operations: LocalOperationPort,
        compatibility_probe: CompatibilityProbePort,
        request_timeout_seconds: float,
    ) -> None:
        try:
            if type(registry) is not OperationRegistry:
                raise TypeError("registry must be exact")
            snapshot_descriptor = inspect.getattr_static(sessions, "runtime_snapshot")
            if type(snapshot_descriptor) is not property:
                raise TypeError("session snapshot must be a property")
            session_root_descriptor = inspect.getattr_static(sessions, "root_key")
            if type(session_root_descriptor) is not property:
                raise TypeError("session root key must be a property")
            for name, is_async in (
                ("reserve_activation_candidate", False),
                ("handshake", True),
                ("prepare_exchange", False),
                ("prepare_read_attempt", True),
                ("reprepare_read_after_activation_mismatch", True),
            ):
                if not _has_method(sessions, name, is_async=is_async):
                    raise TypeError(f"session coordinator {name} has the wrong shape")
            snapshot = revalidate_runtime_snapshot(sessions.runtime_snapshot)
            if registry.manifest_sha256 != snapshot.operation_manifest_sha256:
                raise ValueError("registry manifest does not match runtime snapshot")
            session_root_key = _validated_root_key(sessions.root_key)
            if not _has_method(transport, "session", is_async=False):
                raise TypeError("transport session must be synchronous")
            transport_root_descriptor = inspect.getattr_static(transport, "root_key")
            if type(transport_root_descriptor) is not property:
                raise TypeError("transport root key must be a property")
            transport_root_key = _validated_root_key(transport.root_key)
            if session_root_key != transport_root_key:
                raise ValueError("session and transport roots do not match")
            for name in ("ensure_allowed", "ensure_destructive_allowed"):
                if not _has_method(policy, name, is_async=False):
                    raise TypeError(f"policy {name} must be synchronous")
            for name in ("issue", "lookup_active", "consume"):
                if not _has_method(confirmations, name, is_async=False):
                    raise TypeError(f"confirmation store {name} must be synchronous")
            if not _has_method(wall_clock, "now_ms", is_async=False):
                raise TypeError("wall clock must be synchronous")
            if not _has_method(local_operations, "execute", is_async=True):
                raise TypeError("local operation execution must be asynchronous")
            if not _has_method(compatibility_probe, "execute", is_async=True):
                raise TypeError("compatibility probe execution must be asynchronous")
            if type(request_timeout_seconds) not in {int, float}:
                raise TypeError("request timeout must be numeric")
            timeout = float(request_timeout_seconds)
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("request timeout must be finite and strictly positive")
        except (AttributeError, TypeError, ValueError, ValidationError) as error:
            raise BridgeError(
                ErrorCode.INVALID_ARGUMENT,
                "bridge application dependencies are invalid",
            ) from error
        self._registry = registry
        self._transport = transport
        self._sessions = sessions
        self._runtime_snapshot = snapshot
        self._root_key = session_root_key
        self._policy = policy
        self._confirmations = confirmations
        self._wall_clock = wall_clock
        self._local_operations = local_operations
        self._compatibility_probe = compatibility_probe
        self._request_timeout_seconds = timeout

    async def execute(
        self,
        operation: str,
        arguments: Mapping[str, JsonValue],
        *,
        confirmation_token: str | None = None,
    ) -> InvocationOutcome:
        request_id: UUID | None = None
        try:
            contract, frozen_arguments, public_arguments = self._validate_public_input(
                operation,
                arguments,
            )
            name = contract.name
            if name == "compat.probe.step":
                raise _invalid_argument(
                    "operation is internal-only",
                    {"operation": name},
                )
            if name in _DESTRUCTIVE_OPERATION_NAMES:
                lookup_invocation = self._resolve_destructive_lookup(
                    contract,
                    public_arguments,
                )
                if confirmation_token is None:
                    return await self._execute_destructive_preview(
                        contract,
                        public_arguments,
                        lookup_invocation,
                    )
                return await self._execute_confirmed_destructive(
                    contract,
                    public_arguments,
                    lookup_invocation,
                    confirmation_token,
                )
            if confirmation_token is not None:
                raise _invalid_argument(
                    "confirmation token is not accepted for this operation",
                    {"operation": name},
                )
            if contract.confirmation_required or contract.base_class in {
                OperationClass.DESTRUCTIVE,
                OperationClass.RECONCILE,
            }:
                raise BridgeError(
                    ErrorCode.POLICY_DENIED,
                    "operation requires a dedicated safety workflow",
                    {
                        "operation": name,
                        "operation_class": contract.base_class.value,
                    },
                )
            if contract.target is ExecutionTarget.LOCAL:
                invocation = self._resolve_ordinary(
                    contract,
                    frozen_arguments,
                    public_arguments,
                )
                result = await self._execute_local(invocation)
                return _success_outcome(
                    None,
                    _validated_public_result(invocation, result),
                )
            if name == "bridge.status":
                if type(public_arguments) is not BridgeStatusArgs:
                    raise BridgeError(
                        ErrorCode.PROTOCOL_ERROR,
                        "status public arguments have an invalid shape",
                        {"operation": name},
                    )
                async with self._transport.session() as channel:
                    activation = await self._sessions.handshake(
                        channel,
                        accept_lineage_id=public_arguments.accept_lineage_id,
                    )
                    activation = self._revalidate_activation(activation, name)
                    request_id = activation.status_request_id
                    invocation = self._registry.resolve_invocation(
                        name,
                        frozen_arguments,
                        {"activation_candidate": activation.status.activation_id},
                    )
                    self._require_same_public_arguments(invocation, public_arguments)
                    self._policy.ensure_allowed(
                        status=activation.status,
                        invocation=invocation,
                        runtime_snapshot=self._runtime_snapshot,
                    )
                    return _success_outcome(
                        request_id,
                        _validated_public_result(invocation, activation.status),
                    )
            invocation = self._resolve_ordinary(
                contract,
                frozen_arguments,
                public_arguments,
            )
            if invocation.effective_class not in {
                OperationClass.READ,
                OperationClass.MUTATION,
            }:
                raise BridgeError(
                    ErrorCode.POLICY_DENIED,
                    "operation requires a dedicated safety workflow",
                    {
                        "operation": name,
                        "operation_class": invocation.effective_class.value,
                    },
                )
            async with self._transport.session() as channel:
                if invocation.effective_class is OperationClass.READ:
                    attempt = await self._sessions.prepare_read_attempt(
                        channel,
                        invocation,
                        timeout_seconds=self._request_timeout_seconds,
                    )
                    attempt = self._validate_read_attempt(
                        attempt,
                        invocation,
                        expected_rehandshakes=0,
                    )
                    request_id = attempt.command.request_id
                    self._policy.ensure_allowed(
                        status=attempt.activation.status,
                        invocation=invocation,
                        runtime_snapshot=self._runtime_snapshot,
                    )
                    artifact = await channel.exchange(attempt.command)
                    try:
                        artifact = self._validate_artifact(
                            attempt.command,
                            artifact,
                        )
                    except _ForeignArtifactError:
                        request_id = None
                        raise
                    if self._is_exact_activation_mismatch(artifact):
                        prior = attempt
                        attempt = await self._sessions.reprepare_read_after_activation_mismatch(
                            channel,
                            prior,
                            artifact,
                        )
                        attempt = self._validate_read_attempt(
                            attempt,
                            invocation,
                            expected_rehandshakes=1,
                            prior=prior,
                        )
                        request_id = attempt.command.request_id
                        self._policy.ensure_allowed(
                            status=attempt.activation.status,
                            invocation=invocation,
                            runtime_snapshot=self._runtime_snapshot,
                        )
                        artifact = await channel.exchange(attempt.command)
                        try:
                            artifact = self._validate_artifact(
                                attempt.command,
                                artifact,
                            )
                        except _ForeignArtifactError:
                            request_id = None
                            raise
                    return self._artifact_outcome(
                        invocation,
                        attempt.command.request_id,
                        artifact,
                    )
                activation = await self._sessions.handshake(channel)
                activation = self._revalidate_activation(activation, name)
                self._policy.ensure_allowed(
                    status=activation.status,
                    invocation=invocation,
                    runtime_snapshot=self._runtime_snapshot,
                )
                command = self._sessions.prepare_exchange(
                    activation,
                    invocation,
                    timeout_seconds=self._request_timeout_seconds,
                )
                command = self._validate_command(command, invocation, activation)
                request_id = command.request_id
                artifact = await channel.exchange(command)
                try:
                    artifact = self._validate_artifact(command, artifact)
                except _ForeignArtifactError:
                    request_id = None
                    raise
                return self._artifact_outcome(
                    invocation,
                    command.request_id,
                    artifact,
                )
        except BridgeError as error:
            return _failure_outcome(request_id, error)

    def _resolve_destructive_lookup(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
    ) -> ResolvedInvocation:
        operation = contract.name
        if operation == "unit.delete":
            if type(public_arguments) is not DeleteUnitArgs:
                raise _orchestration_protocol_error(
                    operation,
                    "unit delete arguments have an invalid public shape",
                )
            arguments = public_arguments
            lookup_name = "unit.get"
            lookup_arguments: dict[str, object] = {"unit_guid": arguments.unit_guid}
        elif operation == "mission.delete":
            if type(public_arguments) is not DeleteMissionArgs:
                raise _orchestration_protocol_error(
                    operation,
                    "mission delete arguments have an invalid public shape",
                )
            arguments = public_arguments
            lookup_name = "mission.get"
            lookup_arguments = {
                "side_guid": arguments.side_guid,
                "mission_guid": arguments.mission_guid,
            }
        else:
            raise _orchestration_protocol_error(
                operation,
                "destructive operation has no target lookup",
            )
        try:
            invocation = self._registry.resolve_invocation(lookup_name, lookup_arguments)
        except ValidationError as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive target lookup resolution failed",
            ) from error
        if type(invocation) is not ResolvedInvocation:
            raise _orchestration_protocol_error(
                operation,
                "destructive target lookup resolution returned an invalid invocation",
            )
        try:
            resolved_arguments = invocation.public_arguments.model_dump(
                mode="json",
                exclude_none=True,
                warnings="error",
            )
            wire_arguments = invocation.wire_arguments.model_dump(
                mode="json",
                exclude_none=True,
                warnings="error",
            )
        except (PydanticSerializationError, TypeError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive target lookup invocation is invalid",
            ) from error
        if (
            invocation.contract.name != lookup_name
            or invocation.contract.target is not ExecutionTarget.CMO
            or invocation.effective_class is not OperationClass.READ
            or not _canonical_json_equal(resolved_arguments, lookup_arguments)
            or not _canonical_json_equal(wire_arguments, lookup_arguments)
        ):
            raise _orchestration_protocol_error(
                operation,
                "destructive target lookup invocation is invalid",
            )
        return invocation

    async def _read_reserved_destructive_target(
        self,
        channel: BridgeChannel,
        contract: OperationContract,
        lookup_invocation: ResolvedInvocation,
        candidate: UUID,
        ledger: _DestructiveIdentityLedger,
        *,
        expected_lineage_id: UUID | None,
    ) -> _DestructiveReadResult:
        operation = contract.name
        for attempt_index in range(2):
            activation = await self._sessions.handshake(
                channel,
                reserved_activation_candidate=candidate,
            )
            activation = self._revalidate_activation(activation, operation)
            if activation.status.activation_id != candidate:
                raise _orchestration_protocol_error(
                    operation,
                    "session activation did not use the reserved candidate",
                )
            ledger.record_status(activation)
            if (
                expected_lineage_id is not None
                and activation.status.lineage_id != expected_lineage_id
            ):
                raise BridgeError(
                    ErrorCode.SCENARIO_CHANGED,
                    "confirmation belongs to a different scenario lineage",
                    {"observed_lineage_id": str(activation.status.lineage_id)},
                )
            self._policy.ensure_destructive_allowed(
                status=activation.status,
                contract=contract,
                runtime_snapshot=self._runtime_snapshot,
            )
            command = self._sessions.prepare_exchange(
                activation,
                lookup_invocation,
                timeout_seconds=self._request_timeout_seconds,
            )
            command = self._validate_command(
                command,
                lookup_invocation,
                activation,
            )
            ledger.record_target_read(command)
            artifact = await channel.exchange(command)
            try:
                artifact = self._validate_artifact(command, artifact)
            except _ForeignArtifactError:
                ledger.clear_request_id()
                raise
            if attempt_index == 0 and self._is_exact_activation_mismatch(artifact):
                continue
            return _DestructiveReadResult(
                activation=activation,
                command=command,
                artifact=artifact,
            )
        raise RuntimeError("bounded destructive target-read state machine did not terminate")

    async def _execute_destructive_preview(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
        lookup_invocation: ResolvedInvocation,
    ) -> InvocationOutcome:
        operation = contract.name
        ledger: _DestructiveIdentityLedger | None = None
        try:
            candidate = _require_uuid4(
                self._sessions.reserve_activation_candidate(),
                operation,
                "reserved activation candidate",
            )
            ledger = _DestructiveIdentityLedger(candidate, operation)
            async with self._transport.session() as channel:
                read = await self._read_reserved_destructive_target(
                    channel,
                    contract,
                    lookup_invocation,
                    candidate,
                    ledger,
                    expected_lineage_id=None,
                )
                target = self._resolved_destructive_target(
                    contract,
                    public_arguments,
                    lookup_invocation,
                    read.artifact,
                )
                try:
                    descriptor_value = DestructiveConfirmationDescriptor(
                        format=DESTRUCTIVE_CONFIRMATION_FORMAT,
                        root_key=self._root_key,
                        operation=cast(object, operation),  # type: ignore[arg-type]
                        public_arguments=cast(
                            dict[str, JsonValue],
                            public_arguments.model_dump(mode="json", warnings="error"),
                        ),
                        resolved_target=target,
                        scenario_lineage_id=read.activation.status.lineage_id,
                        reserved_activation_id=candidate,
                        release_id=self._runtime_snapshot.release_id,
                    )
                except (
                    PydanticSerializationError,
                    TypeError,
                    ValidationError,
                    ValueError,
                ) as error:
                    raise _orchestration_protocol_error(
                        operation,
                        "destructive confirmation descriptor is invalid",
                    ) from error
                descriptor = _revalidate_destructive_descriptor(
                    descriptor_value,
                    operation,
                )
                try:
                    binding_value = destructive_confirmation_binding(descriptor)
                except (AttributeError, TypeError, ValidationError, ValueError) as error:
                    raise _orchestration_protocol_error(
                        operation,
                        "destructive confirmation binding is invalid",
                    ) from error
                binding = _revalidate_confirmation_binding(binding_value, operation)
                now_ms = _validated_preview_epoch(self._wall_clock.now_ms(), operation)
                issued = _revalidate_issued_confirmation(
                    self._confirmations.issue(binding, now_ms=now_ms),
                    operation,
                )
                if (
                    not _is_canonical_confirmation_token(issued.token)
                    or issued.expires_at_ms != now_ms + _CONFIRMATION_LIFETIME_MS
                ):
                    raise _orchestration_protocol_error(
                        operation,
                        "confirmation store returned an invalid issuance",
                    )
                expires_at_utc = _preview_expiry_utc(
                    issued.expires_at_ms,
                    operation,
                )
                try:
                    preview_value = DestructivePreviewResult(
                        operation=cast(object, operation),  # type: ignore[arg-type]
                        target_guid=target.guid,
                        target_name=target.name,
                        target_type=target.type,
                        impact=_DESTRUCTIVE_IMPACTS[operation],
                        reserved_activation_candidate=candidate,
                        confirmation_token=issued.token,
                        expires_at_utc=expires_at_utc,
                    )
                except (TypeError, ValidationError, ValueError) as error:
                    raise _protocol_error(operation) from error
                result = _validated_destructive_preview(preview_value, operation)
                return _success_outcome(read.command.request_id, result)
        except BridgeError as error:
            return _failure_outcome(
                None if ledger is None else ledger.request_id,
                error,
            )

    async def _execute_confirmed_destructive(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
        lookup_invocation: ResolvedInvocation,
        confirmation_token: str,
    ) -> InvocationOutcome:
        operation = contract.name
        ledger: _DestructiveIdentityLedger | None = None
        sensitive_values = (
            [confirmation_token] if type(confirmation_token) is str and confirmation_token else []
        )
        try:
            async with self._transport.session() as channel:
                lookup_at_ms = _validated_confirmation_epoch(
                    self._wall_clock.now_ms(),
                    operation,
                )
                stored_value = self._confirmations.lookup_active(
                    confirmation_token,
                    now_ms=lookup_at_ms,
                )
                try:
                    stored_binding = _revalidate_confirmation_binding(
                        stored_value,
                        operation,
                    )
                except BridgeError as error:
                    raise _confirmation_denied() from error
                candidate = stored_binding.activation_id
                if (
                    stored_binding.root_key != self._root_key
                    or stored_binding.operation != operation
                    or stored_binding.binding_format != DESTRUCTIVE_CONFIRMATION_FORMAT
                    or type(candidate) is not UUID
                    or candidate.version != 4
                ):
                    raise _confirmation_denied()
                ledger = _DestructiveIdentityLedger(candidate, operation)
                read = await self._read_reserved_destructive_target(
                    channel,
                    contract,
                    lookup_invocation,
                    candidate,
                    ledger,
                    expected_lineage_id=stored_binding.lineage_id,
                )
                target = self._resolved_destructive_target(
                    contract,
                    public_arguments,
                    lookup_invocation,
                    read.artifact,
                )
                rebuilt_binding = self._build_destructive_binding(
                    contract,
                    public_arguments,
                    target,
                    read.activation,
                    candidate,
                )
                if rebuilt_binding != stored_binding:
                    raise _confirmation_denied()
                consume_at_ms = _validated_confirmation_epoch(
                    self._wall_clock.now_ms(),
                    operation,
                )
                proof = _validated_confirmation_proof(
                    confirmation_token,
                    self._confirmations.consume(
                        confirmation_token,
                        rebuilt_binding,
                        now_ms=consume_at_ms,
                    ),
                    operation,
                )
                sensitive_values.append(proof)
                delete_invocation = self._resolve_confirmed_destructive(
                    contract,
                    public_arguments,
                    proof,
                )
                delete_command = self._sessions.prepare_exchange(
                    read.activation,
                    delete_invocation,
                    timeout_seconds=self._request_timeout_seconds,
                )
                delete_command = self._validate_command(
                    delete_command,
                    delete_invocation,
                    read.activation,
                )
                ledger.record_delete(delete_command)
                delete_artifact = await channel.exchange(delete_command)
                try:
                    delete_artifact = self._validate_artifact(
                        delete_command,
                        delete_artifact,
                    )
                except _ForeignArtifactError:
                    ledger.clear_request_id()
                    raise
                return self._destructive_artifact_outcome(
                    delete_invocation,
                    delete_command.request_id,
                    delete_artifact,
                    target,
                )
        except BridgeError as error:
            return _failure_outcome(
                None if ledger is None else ledger.request_id,
                _sanitize_confirmation_error(error, sensitive_values),
            )

    def _build_destructive_binding(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
        target: DestructiveTarget,
        activation: SessionActivation,
        candidate: UUID,
    ) -> ConfirmationBinding:
        operation = contract.name
        try:
            descriptor_value = DestructiveConfirmationDescriptor(
                format=DESTRUCTIVE_CONFIRMATION_FORMAT,
                root_key=self._root_key,
                operation=cast(object, operation),  # type: ignore[arg-type]
                public_arguments=cast(
                    dict[str, JsonValue],
                    public_arguments.model_dump(mode="json", warnings="error"),
                ),
                resolved_target=target,
                scenario_lineage_id=activation.status.lineage_id,
                reserved_activation_id=candidate,
                release_id=self._runtime_snapshot.release_id,
            )
        except (
            PydanticSerializationError,
            TypeError,
            ValidationError,
            ValueError,
        ) as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive confirmation descriptor is invalid",
            ) from error
        descriptor = _revalidate_destructive_descriptor(descriptor_value, operation)
        try:
            binding_value = destructive_confirmation_binding(descriptor)
        except (AttributeError, TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive confirmation binding is invalid",
            ) from error
        return _revalidate_confirmation_binding(binding_value, operation)

    def _resolve_confirmed_destructive(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
        proof: Sha256,
    ) -> ResolvedInvocation:
        operation = contract.name
        try:
            public_json = cast(
                dict[str, JsonValue],
                public_arguments.model_dump(mode="json", warnings="error"),
            )
            invocation = self._registry.resolve_invocation(
                operation,
                public_json,
                {"confirmation_proof": proof},
            )
        except (PydanticSerializationError, ValidationError) as error:
            raise _orchestration_protocol_error(
                operation,
                "confirmed destructive resolution failed",
            ) from error
        if type(invocation) is not ResolvedInvocation:
            raise _orchestration_protocol_error(
                operation,
                "confirmed destructive resolution returned an invalid invocation",
            )
        try:
            resolved_public = invocation.public_arguments.model_dump(
                mode="json",
                warnings="error",
            )
            resolved_wire = invocation.wire_arguments.model_dump(
                mode="json",
                warnings="error",
            )
        except (PydanticSerializationError, TypeError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "confirmed destructive invocation is invalid",
            ) from error
        expected_wire = {**public_json, "confirmation_proof": proof}
        try:
            if operation == "unit.delete":
                exact_public = (
                    type(public_arguments) is DeleteUnitArgs
                    and type(invocation.public_arguments) is DeleteUnitArgs
                    and invocation.public_arguments == public_arguments
                )
                expected_wire_model: BaseModel = ConfirmedDeleteUnitWireArgs.model_validate(
                    expected_wire
                )
                exact_wire = (
                    type(invocation.wire_arguments) is ConfirmedDeleteUnitWireArgs
                    and invocation.wire_arguments == expected_wire_model
                )
            elif operation == "mission.delete":
                exact_public = (
                    type(public_arguments) is DeleteMissionArgs
                    and type(invocation.public_arguments) is DeleteMissionArgs
                    and invocation.public_arguments == public_arguments
                )
                expected_wire_model = ConfirmedDeleteMissionWireArgs.model_validate(expected_wire)
                exact_wire = (
                    type(invocation.wire_arguments) is ConfirmedDeleteMissionWireArgs
                    and invocation.wire_arguments == expected_wire_model
                )
            else:
                raise TypeError("operation has no confirmed destructive model")
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "confirmed destructive invocation is invalid",
            ) from error
        if (
            invocation.contract != contract
            or invocation.contract.target is not ExecutionTarget.CMO
            or invocation.effective_class is not OperationClass.DESTRUCTIVE
            or not exact_public
            or not exact_wire
            or not _canonical_json_equal(resolved_public, public_json)
            or not _canonical_json_equal(resolved_wire, expected_wire)
        ):
            raise _orchestration_protocol_error(
                operation,
                "confirmed destructive invocation is invalid",
            )
        return invocation

    def _destructive_artifact_outcome(
        self,
        invocation: ResolvedInvocation,
        request_id: UUID,
        artifact: ResponseArtifact,
        target: DestructiveTarget,
    ) -> InvocationOutcome:
        operation = invocation.contract.name
        accepted = artifact.accepted_response
        if not accepted.ok:
            error = accepted.error
            if error is None:
                raise _orchestration_protocol_error(
                    operation,
                    "failed destructive response lost its error",
                )
            raise BridgeError(error.code, "destructive operation failed")
        if accepted.result is None:
            raise _protocol_error(operation)
        validated_result = _validated_public_result(invocation, accepted.result)
        try:
            result = DeleteResult.model_validate(validated_result)
            expected_kind = "unit" if operation == "unit.delete" else "mission"
            if (
                type(result) is not DeleteResult
                or result.deleted_guid != target.guid
                or result.object_kind != expected_kind
            ):
                raise ValueError("destructive result does not match the bound target")
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive result is invalid",
            ) from error
        return _success_outcome(request_id, validated_result)

    def _resolved_destructive_target(
        self,
        contract: OperationContract,
        public_arguments: BaseModel,
        lookup_invocation: ResolvedInvocation,
        artifact: ResponseArtifact,
    ) -> DestructiveTarget:
        operation = contract.name
        accepted = artifact.accepted_response
        if not accepted.ok:
            error = accepted.error
            if error is None:
                raise _orchestration_protocol_error(
                    operation,
                    "failed target lookup lost its error",
                )
            raise BridgeError(error.code, error.message, dict(error.details))
        if accepted.result is None:
            raise _protocol_error(lookup_invocation.contract.name)
        validated_result = _validated_public_result(
            lookup_invocation,
            accepted.result,
        )
        try:
            if operation == "unit.delete":
                if type(public_arguments) is not DeleteUnitArgs:
                    raise TypeError("unit delete arguments must be exact")
                result = UnitResult.model_validate(validated_result)
                if type(result) is not UnitResult or result.guid != public_arguments.unit_guid:
                    raise ValueError("resolved unit GUID does not match delete arguments")
                return DestructiveTarget(
                    guid=result.guid,
                    name=result.name,
                    type=result.type,
                )
            if operation == "mission.delete":
                if type(public_arguments) is not DeleteMissionArgs:
                    raise TypeError("mission delete arguments must be exact")
                result = MissionResult.model_validate(validated_result)
                if (
                    type(result) is not MissionResult
                    or result.guid != public_arguments.mission_guid
                ):
                    raise ValueError("resolved mission GUID does not match delete arguments")
                return DestructiveTarget(
                    guid=result.guid,
                    name=result.name,
                    type=result.mission_class,
                )
            raise TypeError("destructive operation has no exact target result")
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "destructive target result is invalid",
            ) from error

    def _validate_public_input(
        self,
        operation: object,
        arguments: object,
    ) -> tuple[OperationContract, dict[str, object], BaseModel]:
        if type(operation) is not str or not operation:
            raise _invalid_argument("operation name is invalid")
        if not isinstance(arguments, Mapping):
            raise _invalid_argument(
                "operation arguments are invalid",
                {"operation": operation},
            )
        try:
            copied = dict(cast(Mapping[object, object], arguments))
        except Exception as error:
            raise _invalid_argument(
                "operation arguments are invalid",
                {"operation": operation},
            ) from error
        if any(type(key) is not str for key in copied):
            raise _invalid_argument(
                "operation arguments are invalid",
                {"operation": operation},
            )
        contract = self._registry.resolve(operation)
        try:
            frozen = cast(dict[str, object], _round_trip_models(copied))
        except (
            PydanticSerializationError,
            RecursionError,
            TypeError,
            ValueError,
        ) as error:
            raise _invalid_argument(
                "operation arguments are invalid",
                {"operation": operation},
            ) from error
        try:
            validated = contract.public_arguments_adapter.validate_python(frozen)
        except ValidationError as error:
            raise _argument_validation_error(contract.name, error) from error
        if not isinstance(validated, BaseModel):
            raise TypeError("public argument adapter did not produce a Pydantic model")
        try:
            json.dumps(
                validated.model_dump(mode="json", warnings="error"),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (
            PydanticSerializationError,
            RecursionError,
            TypeError,
            ValueError,
        ) as error:
            raise _invalid_argument(
                "operation arguments are invalid",
                {"operation": contract.name},
            ) from error
        return contract, frozen, validated

    def _resolve_ordinary(
        self,
        contract: OperationContract,
        frozen_arguments: Mapping[str, object],
        public_arguments: BaseModel,
    ) -> ResolvedInvocation:
        try:
            invocation = self._registry.resolve_invocation(contract.name, frozen_arguments)
        except ValidationError as error:
            raise _argument_validation_error(contract.name, error) from error
        if type(invocation) is not ResolvedInvocation:
            raise TypeError("operation registry did not produce an exact resolved invocation")
        self._require_same_public_arguments(invocation, public_arguments)
        return invocation

    @staticmethod
    def _require_same_public_arguments(
        invocation: ResolvedInvocation,
        public_arguments: BaseModel,
    ) -> None:
        resolved = invocation.public_arguments
        if type(resolved) is not type(public_arguments) or resolved != public_arguments:
            raise RuntimeError("resolved invocation changed validated public arguments")

    def _validate_read_attempt(
        self,
        value: object,
        invocation: ResolvedInvocation,
        *,
        expected_rehandshakes: int,
        prior: PreparedReadAttempt | None = None,
    ) -> PreparedReadAttempt:
        operation = invocation.contract.name
        if type(value) is not PreparedReadAttempt:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned an invalid read attempt",
            )
        attempt = value
        activation = self._revalidate_activation(attempt.activation, operation)
        try:
            validated = PreparedReadAttempt(
                activation=activation,
                command=attempt.command,
                rehandshakes_used=attempt.rehandshakes_used,
            )
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned an invalid read attempt",
            ) from error
        if validated.rehandshakes_used != expected_rehandshakes:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned a read attempt in the wrong recovery state",
            )
        self._validate_command(validated.command, invocation, activation)
        if prior is not None:
            identities = {
                prior.activation.status_request_id,
                prior.command.request_id,
                prior.activation.status.activation_id,
                validated.activation.status_request_id,
                validated.command.request_id,
                validated.activation.status.activation_id,
            }
            if len(identities) != 6:
                raise _orchestration_protocol_error(
                    operation,
                    "recovered read attempt did not use fresh identities",
                )
        return validated

    def _revalidate_activation(self, value: object, operation: str) -> SessionActivation:
        if type(value) is not SessionActivation:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned an invalid activation",
            )
        activation = value
        try:
            activation = SessionActivation.model_validate(
                activation.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned an invalid activation",
            ) from error
        try:
            snapshot = revalidate_runtime_snapshot(self._runtime_snapshot)
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator runtime snapshot is invalid",
            ) from error
        status = activation.status
        expected_identity = (
            (status.protocol, snapshot.protocol),
            (status.runtime_version, snapshot.runtime_version),
            (status.runtime_tag, snapshot.runtime_tag),
            (status.runtime_asset_sha256, snapshot.runtime_asset_sha256),
            (status.release_id, snapshot.release_id),
            (status.manifest_sha256, snapshot.operation_manifest_sha256),
        )
        if activation.status_request_id == status.activation_id or any(
            actual != expected for actual, expected in expected_identity
        ):
            raise _orchestration_protocol_error(
                operation,
                "session activation does not match the running runtime snapshot",
            )
        return activation

    def _validate_command(
        self,
        value: object,
        invocation: ResolvedInvocation,
        activation: SessionActivation,
    ) -> ExchangeCommand:
        operation = invocation.contract.name
        if type(value) is not ExchangeCommand:
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned an invalid exchange command",
            )
        command = value
        try:
            running_snapshot = revalidate_runtime_snapshot(self._runtime_snapshot)
            command_snapshot = revalidate_runtime_snapshot(command.runtime_snapshot)
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "exchange command has an invalid runtime snapshot",
            ) from error
        raw_body = command.body
        if type(raw_body) is not RequestBody:
            raise _orchestration_protocol_error(
                operation,
                "exchange command has an invalid request body",
            )
        try:
            body = RequestBody.model_validate(
                raw_body.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (TypeError, ValidationError, ValueError) as error:
            raise _orchestration_protocol_error(
                operation,
                "exchange command has an invalid request body",
            ) from error
        expected_arguments = invocation.wire_arguments.model_dump(mode="json")
        timeout = command.timeout
        expected_identity = (
            (body.protocol, running_snapshot.protocol),
            (body.runtime_version, running_snapshot.runtime_version),
            (body.runtime_tag, running_snapshot.runtime_tag),
            (body.runtime_asset_sha256, running_snapshot.runtime_asset_sha256),
            (body.release_id, running_snapshot.release_id),
            (
                body.operation_manifest_sha256,
                running_snapshot.operation_manifest_sha256,
            ),
        )
        if (
            type(command.request_id) is not UUID
            or command.request_id.version != 4
            or command.request_id == activation.status_request_id
            or command.request_id == activation.status.activation_id
            or type(command.invocation) is not ResolvedInvocation
            or command.invocation != invocation
            or body.operation != operation
            or not _canonical_json_equal(body.arguments, expected_arguments)
            or body.expected_lineage_id != activation.status.lineage_id
            or body.expected_activation_id != activation.status.activation_id
            or command_snapshot != running_snapshot
            or any(actual != expected for actual, expected in expected_identity)
            or type(timeout) not in {int, float}
            or not math.isfinite(float(timeout))
            or timeout <= 0
            or float(timeout).hex() != self._request_timeout_seconds.hex()
        ):
            raise _orchestration_protocol_error(
                operation,
                "session coordinator returned a semantically invalid exchange command",
            )
        return command

    def _validate_artifact(
        self,
        command: ExchangeCommand,
        value: object,
    ) -> ResponseArtifact:
        operation = command.invocation.contract.name
        if type(value) is not ResponseArtifact:
            raise _foreign_artifact(operation)
        artifact = value
        try:
            artifact = ResponseArtifact.model_validate(
                artifact.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (TypeError, ValidationError, ValueError) as error:
            raise _foreign_artifact(operation) from error
        accepted = artifact.accepted_response
        envelope = accepted.envelope
        response_error = envelope.error
        error_code = None if response_error is None else response_error.code
        lineage_exception = not envelope.ok and error_code is ErrorCode.SCENARIO_CHANGED
        manifest_exception = not envelope.ok and error_code is ErrorCode.MANIFEST_MISMATCH
        snapshot = command.runtime_snapshot
        try:
            reported_tag = derive_runtime_tag(
                envelope.bridge_version,
                envelope.runtime_asset_sha256,
            )
        except ValueError as error:
            raise _foreign_artifact(operation) from error
        expected_identity = (
            (envelope.protocol, snapshot.protocol),
            (envelope.bridge_version, snapshot.runtime_version),
            (envelope.runtime_tag, snapshot.runtime_tag),
            (envelope.runtime_asset_sha256, snapshot.runtime_asset_sha256),
            (envelope.release_id, snapshot.release_id),
            (
                envelope.operation_manifest_sha256,
                snapshot.operation_manifest_sha256,
            ),
        )
        if (
            accepted.delivery_kind != "request"
            or envelope.request_id != command.request_id
            or envelope.request_hash != request_sha256(command.body)
            or envelope.activation_id != command.body.expected_activation_id
            or (
                not lineage_exception
                and envelope.scenario_lineage_id != command.body.expected_lineage_id
            )
            or reported_tag != envelope.runtime_tag
            or (
                not manifest_exception
                and any(actual != expected for actual, expected in expected_identity)
            )
        ):
            raise _foreign_artifact(operation)
        if envelope.ok:
            if not isinstance(accepted.settlement, CompletedSettlement):
                raise _orchestration_protocol_error(
                    operation,
                    "successful response lacks completed settlement",
                )
            return artifact
        if response_error is None:
            raise _orchestration_protocol_error(
                operation,
                "failed response lost its error",
            )
        evidence = response_error.mutation_not_started
        if command.invocation.effective_class is OperationClass.READ:
            if evidence is not None:
                raise _orchestration_protocol_error(
                    operation,
                    "read response contains mutation-only evidence",
                )
            if manifest_exception:
                if accepted.settlement is not None:
                    raise _orchestration_protocol_error(
                        operation,
                        "manifest mismatch response cannot claim settlement",
                    )
            elif not isinstance(accepted.settlement, RejectedSettlement):
                raise _orchestration_protocol_error(
                    operation,
                    "failed read response lacks rejected settlement",
                )
        elif command.invocation.effective_class in {
            OperationClass.MUTATION,
            OperationClass.DESTRUCTIVE,
        }:
            if evidence is None:
                if accepted.settlement is not None:
                    raise _orchestration_protocol_error(
                        operation,
                        "failed mutation response cannot claim settlement",
                    )
            elif (
                not isinstance(accepted.settlement, RejectedSettlement)
                or evidence.request_id != command.request_id
                or evidence.request_hash != request_sha256(command.body)
                or evidence.operation != operation
            ):
                raise _orchestration_protocol_error(
                    operation,
                    "mutation-not-started evidence is not correlated to the command",
                )
        return artifact

    @staticmethod
    def _is_exact_activation_mismatch(artifact: ResponseArtifact) -> bool:
        accepted = artifact.accepted_response
        error = accepted.error
        return (
            not accepted.ok
            and error is not None
            and error.code is ErrorCode.ACTIVATION_MISMATCH
            and error.mutation_not_started is None
            and isinstance(accepted.settlement, RejectedSettlement)
        )

    @staticmethod
    def _artifact_outcome(
        invocation: ResolvedInvocation,
        request_id: UUID,
        artifact: ResponseArtifact,
    ) -> InvocationOutcome:
        accepted = artifact.accepted_response
        if accepted.ok:
            result = accepted.result
            if result is None:
                raise _protocol_error(invocation.contract.name)
            return _success_outcome(
                request_id,
                _validated_public_result(invocation, result),
            )
        error = accepted.error
        if error is None:
            raise _orchestration_protocol_error(
                invocation.contract.name,
                "failed response lost its error",
            )
        raise BridgeError(error.code, error.message, dict(error.details))

    async def _execute_local(self, invocation: ResolvedInvocation) -> object:
        name = invocation.contract.name
        arguments = invocation.public_arguments
        if name == "compat.probe":
            return await self._compatibility_probe.execute(arguments)  # type: ignore[arg-type]
        if name in _LOCAL_OPERATION_NAMES:
            return await self._local_operations.execute(name, arguments)  # type: ignore[arg-type]
        raise BridgeError(
            ErrorCode.PROTOCOL_ERROR,
            "local operation has no application route",
            {"operation": name},
        )
