from __future__ import annotations

import inspect
import json
import math
import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    InstanceOf,
    JsonValue,
    ValidationError,
    field_validator,
    model_validator,
)

from cmo_agent_bridge.application.ports import WallClockPort
from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import ExecutionTarget, OperationClass
from cmo_agent_bridge.operations.models import BridgeStatusResult, BridgeStatusWireArgs
from cmo_agent_bridge.operations.registry import OperationRegistry, ResolvedInvocation
from cmo_agent_bridge.protocol.canonical import request_sha256
from cmo_agent_bridge.protocol.models import ExchangeCommand, RequestBody
from cmo_agent_bridge.protocol.response_models import (
    CompletedSettlement,
    RejectedSettlement,
    ResponseArtifact,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, Sha256, revalidate_runtime_snapshot
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.transports.file_bridge.models import BridgeChannel
from cmo_agent_bridge.transports.file_bridge.process_guard import (
    ProcessInfo,
    windows_paths_equal,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _invalid_argument(message: str) -> BridgeError:
    return BridgeError(ErrorCode.INVALID_ARGUMENT, message)


def _state_conflict(message: str, details: dict[str, object] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.STATE_CONFLICT, message, details)


def _protocol_error(message: str) -> BridgeError:
    return BridgeError(ErrorCode.PROTOCOL_ERROR, message)


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _normalize_nested_model(value: object, field: str) -> object:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="python", round_trip=True, warnings=False)
    if not isinstance(value, Mapping):
        return value
    normalized = dict(cast(Mapping[str, object], value))
    nested = normalized.get(field)
    if isinstance(nested, BaseModel):
        normalized[field] = nested.model_dump(mode="python", round_trip=True, warnings=False)
    return normalized


class SessionScope(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    root_key: str
    command_exe: Path

    @field_validator("root_key", mode="before")
    @classmethod
    def validate_root_key(cls, value: object) -> object:
        if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
            raise ValueError("session scope root key must be lowercase SHA-256")
        return value

    @field_validator("command_exe", mode="before")
    @classmethod
    def validate_command_exe(cls, value: object) -> object:
        if (
            not isinstance(value, Path)
            or not value.is_absolute()
            or value.name.casefold() != "command.exe"
            or any(part in {".", ".."} for part in value.parts)
        ):
            raise ValueError(
                "session scope command executable must be an absolute Command.exe Path"
            )
        return value


class SessionActivation(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    status_request_id: UUID
    status: BridgeStatusResult
    response_artifact: ResponseArtifact

    @model_validator(mode="before")
    @classmethod
    def revalidate_nested_models(cls, value: object) -> object:
        normalized = _normalize_nested_model(value, "status")
        return _normalize_nested_model(normalized, "response_artifact")

    @field_validator("status_request_id")
    @classmethod
    def validate_status_request_id(cls, value: UUID) -> UUID:
        if type(value) is not UUID or value.version != 4:
            raise ValueError("status request ID must be an exact UUIDv4")
        return value

    @model_validator(mode="after")
    def validate_activation_identity(self) -> SessionActivation:
        accepted = self.response_artifact.accepted_response
        envelope = accepted.envelope
        status_json = self.status.model_dump(mode="json")
        if (
            accepted.delivery_kind != "request"
            or not envelope.ok
            or not isinstance(accepted.settlement, CompletedSettlement)
            or envelope.request_id != self.status_request_id
            or self.status.lineage_id != envelope.scenario_lineage_id
            or self.status.activation_id != envelope.activation_id
            or self.status.protocol != envelope.protocol
            or self.status.runtime_version != envelope.bridge_version
            or self.status.runtime_tag != envelope.runtime_tag
            or self.status.runtime_asset_sha256 != envelope.runtime_asset_sha256
            or self.status.release_id != envelope.release_id
            or self.status.manifest_sha256 != envelope.operation_manifest_sha256
            or _canonical_json_bytes(accepted.result) != _canonical_json_bytes(status_json)
        ):
            raise ValueError("session activation identity is inconsistent")
        return self


class PreparedReadAttempt(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        arbitrary_types_allowed=True,
    )

    activation: SessionActivation
    command: InstanceOf[ExchangeCommand]
    rehandshakes_used: Literal[0, 1]

    @model_validator(mode="before")
    @classmethod
    def revalidate_activation(cls, value: object) -> object:
        return _normalize_nested_model(value, "activation")

    @field_validator("command", mode="before")
    @classmethod
    def validate_exact_command(cls, value: object) -> object:
        if type(value) is not ExchangeCommand:
            raise ValueError("prepared read requires an exact exchange command")
        return value

    @model_validator(mode="after")
    def validate_read_attempt(self) -> PreparedReadAttempt:
        invocation = self.command.invocation
        if (
            type(invocation) is not ResolvedInvocation
            or invocation.contract.target is not ExecutionTarget.CMO
            or invocation.effective_class is not OperationClass.READ
            or self.command.body.expected_lineage_id != self.activation.status.lineage_id
            or self.command.body.expected_activation_id != self.activation.status.activation_id
        ):
            raise ValueError("prepared read command is not bound to its activation")
        return self


class SessionService:
    def __init__(
        self,
        *,
        scope: SessionScope,
        session_store: SessionStore,
        registry: OperationRegistry,
        runtime_snapshot: RuntimeSnapshot,
        wall_clock: WallClockPort,
        uuid4_source: Callable[[], UUID],
        status_timeout_seconds: float,
    ) -> None:
        try:
            if type(scope) is not SessionScope:
                raise TypeError("scope must be exact")
            validated_scope = SessionScope.model_validate(
                scope.model_dump(mode="python", round_trip=True, warnings=False)
            )
            if type(session_store) is not SessionStore:
                raise TypeError("session store must be exact")
            if type(registry) is not OperationRegistry:
                raise TypeError("operation registry must be exact")
            snapshot = revalidate_runtime_snapshot(runtime_snapshot)
            if registry.manifest_sha256 != snapshot.operation_manifest_sha256:
                raise ValueError("registry manifest does not match runtime snapshot")
            now_ms = inspect.getattr_static(wall_clock, "now_ms")
            if not callable(now_ms) or inspect.iscoroutinefunction(now_ms):
                raise TypeError("wall clock must expose synchronous now_ms")
            if not callable(uuid4_source) or inspect.iscoroutinefunction(uuid4_source):
                raise TypeError("UUID4 source must be synchronous")
            if type(status_timeout_seconds) not in {int, float}:
                raise TypeError("status timeout must be numeric")
            timeout = float(status_timeout_seconds)
            if not math.isfinite(timeout) or timeout <= 0:
                raise ValueError("status timeout must be finite and strictly positive")
        except (AttributeError, TypeError, ValueError, ValidationError) as error:
            raise _invalid_argument("session service dependencies are invalid") from error
        self._scope = validated_scope
        self._session_store = session_store
        self._registry = registry
        self._runtime_snapshot = snapshot
        self._wall_clock = wall_clock
        self._uuid4_source = uuid4_source
        self._status_timeout_seconds = timeout
        self._issued_request_ids: set[UUID] = set()
        self._issued_activation_candidates: set[UUID] = set()

    @property
    def runtime_snapshot(self) -> RuntimeSnapshot:
        return self._runtime_snapshot

    @property
    def root_key(self) -> Sha256:
        return self._scope.root_key

    def reserve_activation_candidate(self) -> UUID:
        return self._next_uuid4("reserved activation candidate", role="candidate")

    def prepare_exchange(
        self,
        activation: SessionActivation,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> ExchangeCommand:
        validated_activation = self._validated_activation(activation)
        timeout = self._validated_invocation_timeout(
            invocation,
            timeout_seconds,
            require_read=False,
        )
        request_id = self._next_uuid4("operation request ID", role="request")
        body = RequestBody(
            protocol=self._runtime_snapshot.protocol,
            release_id=self._runtime_snapshot.release_id,
            runtime_version=self._runtime_snapshot.runtime_version,
            runtime_tag=self._runtime_snapshot.runtime_tag,
            runtime_asset_sha256=self._runtime_snapshot.runtime_asset_sha256,
            expected_lineage_id=validated_activation.status.lineage_id,
            expected_activation_id=validated_activation.status.activation_id,
            operation_manifest_sha256=self._runtime_snapshot.operation_manifest_sha256,
            operation=invocation.contract.name,
            arguments=cast(
                dict[str, JsonValue],
                invocation.wire_arguments.model_dump(mode="json"),
            ),
        )
        return ExchangeCommand(
            request_id=request_id,
            body=body,
            invocation=invocation,
            runtime_snapshot=self._runtime_snapshot,
            timeout=timeout,
        )

    async def prepare_read_attempt(
        self,
        channel: BridgeChannel,
        invocation: ResolvedInvocation,
        *,
        timeout_seconds: float,
    ) -> PreparedReadAttempt:
        timeout = self._validated_invocation_timeout(
            invocation,
            timeout_seconds,
            require_read=True,
        )
        activation = await self.handshake(channel)
        command = self.prepare_exchange(
            activation,
            invocation,
            timeout_seconds=timeout,
        )
        return PreparedReadAttempt(
            activation=activation,
            command=command,
            rehandshakes_used=0,
        )

    async def reprepare_read_after_activation_mismatch(
        self,
        channel: BridgeChannel,
        prior: PreparedReadAttempt,
        response: ResponseArtifact,
    ) -> PreparedReadAttempt:
        validated_prior = self._validated_read_attempt(prior)
        if validated_prior.rehandshakes_used != 0:
            raise _invalid_argument("read activation mismatch may be re-handshaken only once")
        self._validate_activation_mismatch_response(
            validated_prior.command,
            response,
        )
        activation = await self.handshake(channel)
        command = self.prepare_exchange(
            activation,
            validated_prior.command.invocation,
            timeout_seconds=validated_prior.command.timeout,
        )
        return PreparedReadAttempt(
            activation=activation,
            command=command,
            rehandshakes_used=1,
        )

    async def handshake(
        self,
        channel: BridgeChannel,
        *,
        accept_lineage_id: UUID | None = None,
        reserved_activation_candidate: UUID | None = None,
    ) -> SessionActivation:
        if accept_lineage_id is not None and reserved_activation_candidate is not None:
            raise _invalid_argument(
                "lineage acceptance and a reserved activation candidate are mutually exclusive"
            )
        if reserved_activation_candidate is not None and (
            type(reserved_activation_candidate) is not UUID
            or reserved_activation_candidate.version != 4
        ):
            raise _invalid_argument("reserved activation candidate must be an exact UUIDv4")
        if reserved_activation_candidate is not None:
            if reserved_activation_candidate in self._issued_request_ids:
                raise _invalid_argument(
                    "reserved activation candidate collides with an issued request ID"
                )
            self._issued_activation_candidates.add(reserved_activation_candidate)
        if accept_lineage_id is not None and (
            type(accept_lineage_id) is not UUID or accept_lineage_id.version != 4
        ):
            raise _invalid_argument("accepted lineage ID must be an exact UUIDv4")
        process = self._validated_channel_process(channel)
        loaded = self._session_store.load(self._scope.root_key)
        continuing = (
            loaded is not None
            and loaded.process_pid == process.pid
            and loaded.process_create_time == process.create_time
        )
        if accept_lineage_id is not None and not continuing:
            raise _invalid_argument("lineage acceptance requires an existing continuing session")

        candidate = (
            reserved_activation_candidate
            if reserved_activation_candidate is not None
            else self._next_uuid4("activation candidate", role="candidate")
        )
        request_id = self._next_uuid4("status request ID", role="request")
        if candidate == request_id:
            raise _state_conflict("status request and activation candidate IDs were reused")
        command = self._status_command(
            request_id=request_id,
            activation_candidate=candidate,
            expected_lineage_id=(
                loaded.scenario_lineage_id if continuing and loaded is not None else None
            ),
            expected_activation_id=(
                loaded.activation_id if continuing and loaded is not None else None
            ),
            accept_lineage_id=accept_lineage_id,
        )
        artifact = await channel.exchange(command)
        try:
            status = self._extract_status(command, artifact)
        except BridgeError as error:
            if (
                error.code is ErrorCode.ACTIVATION_MISMATCH
                and continuing
                and loaded is not None
                and accept_lineage_id is None
            ):
                bootstrap_candidate = (
                    reserved_activation_candidate
                    if reserved_activation_candidate is not None
                    else self._next_uuid4("activation candidate", role="candidate")
                )
                bootstrap_request_id = self._next_uuid4("status request ID", role="request")
                if bootstrap_candidate == bootstrap_request_id:
                    raise _state_conflict("status request and activation candidate IDs were reused")
                bootstrap_command = self._status_command(
                    request_id=bootstrap_request_id,
                    activation_candidate=bootstrap_candidate,
                    expected_lineage_id=None,
                    expected_activation_id=None,
                    accept_lineage_id=None,
                )
                bootstrap_artifact = await channel.exchange(bootstrap_command)
                try:
                    bootstrap_status = self._extract_status(
                        bootstrap_command,
                        bootstrap_artifact,
                    )
                except BridgeError as bootstrap_error:
                    if bootstrap_error.code is ErrorCode.SCENARIO_CHANGED:
                        bootstrap_observed = (
                            bootstrap_artifact.accepted_response.envelope.scenario_lineage_id
                        )
                        raise self._scenario_changed_error(
                            bootstrap_error,
                            bootstrap_observed,
                        ) from bootstrap_error
                    raise
                if bootstrap_status.lineage_id != loaded.scenario_lineage_id:
                    raise BridgeError(
                        ErrorCode.SCENARIO_CHANGED,
                        "stale activation bootstrap reported a different scenario lineage",
                        {"observed_lineage_id": str(bootstrap_status.lineage_id)},
                    )
                self._persist(
                    bootstrap_status,
                    process,
                    expected_activation_id=loaded.activation_id,
                )
                return SessionActivation(
                    status_request_id=bootstrap_request_id,
                    status=bootstrap_status,
                    response_artifact=bootstrap_artifact,
                )
            if error.code is not ErrorCode.SCENARIO_CHANGED:
                raise
            observed = artifact.accepted_response.envelope.scenario_lineage_id
            if not continuing or loaded is None or accept_lineage_id != observed:
                raise self._scenario_changed_error(error, observed) from error
            accepted_guard = SessionRecord(
                root_key=self._scope.root_key,
                scenario_lineage_id=observed,
                activation_id=artifact.accepted_response.envelope.activation_id,
                build_number=loaded.build_number,
                runtime_snapshot=self._runtime_snapshot,
                process_pid=process.pid,
                process_create_time=process.create_time,
                validated_at_ms=loaded.validated_at_ms,
            )
            guard_replaced = self._session_store.conditional_replace(
                accepted_guard,
                expected_activation_id=loaded.activation_id,
            )
            if not guard_replaced:
                raise _state_conflict("lineage adoption lost its activation CAS") from error
            second_candidate = self._next_uuid4("activation candidate", role="candidate")
            second_request_id = self._next_uuid4("status request ID", role="request")
            if second_candidate == second_request_id:
                raise _state_conflict("status request and activation candidate IDs were reused")
            second_command = self._status_command(
                request_id=second_request_id,
                activation_candidate=second_candidate,
                expected_lineage_id=observed,
                expected_activation_id=artifact.accepted_response.envelope.activation_id,
                accept_lineage_id=observed,
            )
            second_artifact = await channel.exchange(second_command)
            try:
                second_status = self._extract_status(second_command, second_artifact)
            except BridgeError as second_error:
                if second_error.code is ErrorCode.SCENARIO_CHANGED:
                    second_observed = second_artifact.accepted_response.envelope.scenario_lineage_id
                    raise self._scenario_changed_error(
                        second_error,
                        second_observed,
                    ) from second_error
                raise
            self._persist(
                second_status,
                process,
                expected_activation_id=accepted_guard.activation_id,
            )
            return SessionActivation(
                status_request_id=second_request_id,
                status=second_status,
                response_artifact=second_artifact,
            )
        if accept_lineage_id is not None:
            raise _invalid_argument(
                "lineage acceptance requires an exact scenario-changed response"
            )
        self._persist(
            status,
            process,
            expected_activation_id=(None if loaded is None else loaded.activation_id),
        )
        return SessionActivation(
            status_request_id=request_id,
            status=status,
            response_artifact=artifact,
        )

    @staticmethod
    def _scenario_changed_error(error: BridgeError, observed: UUID) -> BridgeError:
        details = dict(error.details)
        details["observed_lineage_id"] = str(observed)
        return BridgeError(ErrorCode.SCENARIO_CHANGED, error.message, details)

    def _validated_channel_process(self, channel: BridgeChannel) -> ProcessInfo:
        try:
            process = channel.process_identity
        except (AttributeError, TypeError, ValueError) as error:
            raise _state_conflict("bridge channel process identity is unavailable") from error
        if (
            type(process) is not ProcessInfo
            or type(process.pid) is not int
            or process.pid <= 0
            or type(process.create_time) is not float
            or not math.isfinite(process.create_time)
            or process.create_time <= 0
            or not isinstance(cast(object, process.executable), Path)
        ):
            raise _state_conflict("bridge channel process identity is malformed")
        if not windows_paths_equal(process.executable, self._scope.command_exe):
            raise _state_conflict(
                "bridge channel process executable does not match the session scope",
                {
                    "channel_executable": str(process.executable),
                    "command_exe": str(self._scope.command_exe),
                },
            )
        return process

    def _validated_activation(self, value: SessionActivation) -> SessionActivation:
        try:
            if type(value) is not SessionActivation:
                raise TypeError("activation must be exact")
            candidate = SessionActivation.model_validate(
                value.model_dump(mode="python", round_trip=True, warnings=False)
            )
        except (AttributeError, TypeError, ValueError, ValidationError) as error:
            raise _invalid_argument("session activation is invalid") from error
        status = candidate.status
        expected_identity = (
            (status.protocol, self._runtime_snapshot.protocol),
            (status.runtime_version, self._runtime_snapshot.runtime_version),
            (status.runtime_tag, self._runtime_snapshot.runtime_tag),
            (status.runtime_asset_sha256, self._runtime_snapshot.runtime_asset_sha256),
            (status.release_id, self._runtime_snapshot.release_id),
            (status.manifest_sha256, self._runtime_snapshot.operation_manifest_sha256),
        )
        if any(actual != expected for actual, expected in expected_identity):
            raise _invalid_argument("session activation belongs to another runtime snapshot")
        return candidate

    def _validated_read_attempt(self, value: PreparedReadAttempt) -> PreparedReadAttempt:
        if type(value) is not PreparedReadAttempt:
            raise _invalid_argument("prior read attempt must be exact")
        command = value.command
        invocation = command.invocation if type(command) is ExchangeCommand else None
        if (
            type(value.rehandshakes_used) is not int
            or value.rehandshakes_used not in {0, 1}
            or type(invocation) is not ResolvedInvocation
            or invocation.contract.target is not ExecutionTarget.CMO
            or invocation.effective_class is not OperationClass.READ
            or type(command.request_id) is not UUID
            or command.request_id.version != 4
            or command.body.operation != invocation.contract.name
            or command.body.expected_lineage_id != value.activation.status.lineage_id
            or command.body.expected_activation_id != value.activation.status.activation_id
            or command.runtime_snapshot != self._runtime_snapshot
            or type(command.timeout) not in {int, float}
            or not math.isfinite(float(command.timeout))
            or command.timeout <= 0
        ):
            raise _invalid_argument("prior read attempt is invalid")
        self._validated_activation(value.activation)
        return value

    def _validate_activation_mismatch_response(
        self,
        command: ExchangeCommand,
        response: ResponseArtifact,
    ) -> None:
        if type(response) is not ResponseArtifact:
            raise _invalid_argument("read mismatch response must be an exact artifact")
        accepted = response.accepted_response
        envelope = accepted.envelope
        error = envelope.error
        snapshot = command.runtime_snapshot
        if (
            accepted.delivery_kind != "request"
            or envelope.ok
            or error is None
            or error.code is not ErrorCode.ACTIVATION_MISMATCH
            or error.mutation_not_started is not None
            or not isinstance(accepted.settlement, RejectedSettlement)
            or envelope.request_id != command.request_id
            or envelope.request_hash != request_sha256(command.body)
            or envelope.scenario_lineage_id != command.body.expected_lineage_id
            or envelope.activation_id != command.body.expected_activation_id
            or envelope.protocol != snapshot.protocol
            or envelope.bridge_version != snapshot.runtime_version
            or envelope.runtime_tag != snapshot.runtime_tag
            or envelope.runtime_asset_sha256 != snapshot.runtime_asset_sha256
            or envelope.release_id != snapshot.release_id
            or envelope.operation_manifest_sha256 != snapshot.operation_manifest_sha256
        ):
            raise _invalid_argument("response is not the prior read's exact activation mismatch")

    @staticmethod
    def _validated_invocation_timeout(
        invocation: ResolvedInvocation,
        timeout_seconds: float,
        *,
        require_read: bool,
    ) -> float:
        if (
            type(invocation) is not ResolvedInvocation
            or invocation.contract.target is not ExecutionTarget.CMO
        ):
            raise _invalid_argument("exchange requires an exact CMO invocation")
        if require_read and invocation.effective_class is not OperationClass.READ:
            raise _invalid_argument("read attempt requires effective READ invocation")
        if type(timeout_seconds) not in {int, float}:
            raise _invalid_argument("exchange timeout must be an exact int or float")
        timeout = float(timeout_seconds)
        if not math.isfinite(timeout) or timeout <= 0:
            raise _invalid_argument("exchange timeout must be finite and strictly positive")
        return timeout

    def _next_uuid4(
        self,
        description: str,
        *,
        role: Literal["request", "candidate"],
    ) -> UUID:
        try:
            value = self._uuid4_source()
        except Exception as error:
            raise _state_conflict(f"{description} generation failed") from error
        if type(value) is not UUID or value.version != 4:
            raise _state_conflict(f"{description} source did not return an exact UUIDv4")
        if value in self._issued_request_ids or value in self._issued_activation_candidates:
            raise _state_conflict(f"{description} source reused an issued identity")
        if role == "request":
            self._issued_request_ids.add(value)
        else:
            self._issued_activation_candidates.add(value)
        return value

    def _status_command(
        self,
        *,
        request_id: UUID,
        activation_candidate: UUID,
        expected_lineage_id: UUID | None,
        expected_activation_id: UUID | None,
        accept_lineage_id: UUID | None,
    ) -> ExchangeCommand:
        public_arguments: dict[str, object] = {}
        if accept_lineage_id is not None:
            public_arguments["accept_lineage_id"] = accept_lineage_id
        invocation = self._registry.resolve_invocation(
            "bridge.status",
            public_arguments,
            {"activation_candidate": activation_candidate},
        )
        body = RequestBody(
            protocol=self._runtime_snapshot.protocol,
            release_id=self._runtime_snapshot.release_id,
            runtime_version=self._runtime_snapshot.runtime_version,
            runtime_tag=self._runtime_snapshot.runtime_tag,
            runtime_asset_sha256=self._runtime_snapshot.runtime_asset_sha256,
            expected_lineage_id=expected_lineage_id,
            expected_activation_id=expected_activation_id,
            operation_manifest_sha256=self._runtime_snapshot.operation_manifest_sha256,
            operation="bridge.status",
            arguments=cast(
                dict[str, JsonValue],
                invocation.wire_arguments.model_dump(mode="json"),
            ),
        )
        return ExchangeCommand(
            request_id=request_id,
            body=body,
            invocation=invocation,
            runtime_snapshot=self._runtime_snapshot,
            timeout=self._status_timeout_seconds,
        )

    def _extract_status(
        self,
        command: ExchangeCommand,
        artifact: ResponseArtifact,
    ) -> BridgeStatusResult:
        if type(artifact) is not ResponseArtifact:
            raise _protocol_error("status exchange did not return an exact response artifact")
        accepted = artifact.accepted_response
        envelope = accepted.envelope
        if (
            accepted.delivery_kind != "request"
            or envelope.request_id != command.request_id
            or envelope.request_hash != request_sha256(command.body)
        ):
            raise _protocol_error("status response does not belong to its request")
        wire = command.invocation.wire_arguments
        if type(wire) is not BridgeStatusWireArgs:
            raise _protocol_error("status invocation lost its typed wire arguments")
        if envelope.activation_id != wire.activation_candidate:
            raise _protocol_error("status response activation does not match its candidate")
        response_error = envelope.error
        error_code = None if response_error is None else response_error.code
        manifest_exception = error_code is ErrorCode.MANIFEST_MISMATCH
        expected_identity = (
            (envelope.protocol, command.runtime_snapshot.protocol),
            (envelope.bridge_version, command.runtime_snapshot.runtime_version),
            (envelope.runtime_tag, command.runtime_snapshot.runtime_tag),
            (
                envelope.runtime_asset_sha256,
                command.runtime_snapshot.runtime_asset_sha256,
            ),
            (envelope.release_id, command.runtime_snapshot.release_id),
            (
                envelope.operation_manifest_sha256,
                command.runtime_snapshot.operation_manifest_sha256,
            ),
        )
        if not manifest_exception and any(
            actual != expected for actual, expected in expected_identity
        ):
            raise _protocol_error("status response runtime identity is not fully correlated")
        expected_lineage = command.body.expected_lineage_id
        if (
            expected_lineage is not None
            and error_code is not ErrorCode.SCENARIO_CHANGED
            and envelope.scenario_lineage_id != expected_lineage
        ):
            raise _protocol_error("status response changed the expected scenario lineage")
        if not envelope.ok:
            if response_error is None:
                raise _protocol_error("failed status response lost its error")
            if manifest_exception:
                if accepted.settlement is not None:
                    raise _protocol_error(
                        "manifest-mismatch status response cannot claim settlement"
                    )
            elif not isinstance(accepted.settlement, RejectedSettlement):
                raise _protocol_error("failed status response lacks rejected settlement")
            raise BridgeError(
                response_error.code,
                response_error.message,
                dict(response_error.details),
            )
        if not isinstance(accepted.settlement, CompletedSettlement):
            raise _protocol_error("successful status response lacks completed settlement")
        try:
            validated = command.invocation.result_adapter.validate_python(envelope.result)
        except (ValidationError, TypeError, ValueError) as error:
            raise _protocol_error("status result does not match the frozen schema") from error
        if type(validated) is not BridgeStatusResult:
            raise _protocol_error("status result adapter returned an unexpected type")
        status = validated
        snapshot = command.runtime_snapshot
        expected_identity = (
            (envelope.protocol, snapshot.protocol),
            (envelope.bridge_version, snapshot.runtime_version),
            (envelope.runtime_tag, snapshot.runtime_tag),
            (envelope.runtime_asset_sha256, snapshot.runtime_asset_sha256),
            (envelope.release_id, snapshot.release_id),
            (envelope.operation_manifest_sha256, snapshot.operation_manifest_sha256),
            (status.protocol, snapshot.protocol),
            (status.runtime_version, snapshot.runtime_version),
            (status.runtime_tag, snapshot.runtime_tag),
            (status.runtime_asset_sha256, snapshot.runtime_asset_sha256),
            (status.release_id, snapshot.release_id),
            (status.manifest_sha256, snapshot.operation_manifest_sha256),
            (status.lineage_id, envelope.scenario_lineage_id),
            (status.activation_id, envelope.activation_id),
        )
        if any(actual != expected for actual, expected in expected_identity):
            raise _protocol_error("status response identity is not fully correlated")
        if expected_lineage is not None and status.lineage_id != expected_lineage:
            raise _protocol_error("successful status changed the expected scenario lineage")
        return status

    def _persist(
        self,
        status: BridgeStatusResult,
        process: ProcessInfo,
        *,
        expected_activation_id: UUID | None,
    ) -> None:
        try:
            validated_at_ms = self._wall_clock.now_ms()
        except Exception as error:
            raise _state_conflict("session validation epoch could not be read") from error
        if type(validated_at_ms) is not int or validated_at_ms < 0:
            raise _state_conflict("session validation epoch must be a non-negative exact integer")
        persisted = self._session_store.conditional_replace(
            SessionRecord(
                root_key=self._scope.root_key,
                scenario_lineage_id=status.lineage_id,
                activation_id=status.activation_id,
                build_number=status.build,
                runtime_snapshot=self._runtime_snapshot,
                process_pid=process.pid,
                process_create_time=process.create_time,
                validated_at_ms=validated_at_ms,
            ),
            expected_activation_id=expected_activation_id,
        )
        if not persisted:
            raise _state_conflict("session persistence lost its activation CAS")
