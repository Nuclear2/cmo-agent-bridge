from __future__ import annotations

from dataclasses import dataclass

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError
from cmo_agent_bridge.operations.models import ReconcileCommitWireArgs
from cmo_agent_bridge.operations.registry import FrozenInvocation
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.manifest import ReleaseBinding
from cmo_agent_bridge.protocol.models import AllowedDelivery, RequestBody, ResponseExpectation
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import AcceptedResponse, ResponseArtifact, Settlement
from cmo_agent_bridge.protocol.runtime import revalidate_runtime_snapshot
from cmo_agent_bridge.state.models import (
    PendingExchange,
    PendingJournal,
    _canonical_json_bytes,  # pyright: ignore[reportPrivateUsage]
    _canonical_model_bytes,  # pyright: ignore[reportPrivateUsage]
    _parse_duplicate_free_json,  # pyright: ignore[reportPrivateUsage]
)


class DurableValidationError(ValueError):
    """An already-durable object cannot reconstruct its exact runtime semantics."""


@dataclass(frozen=True, slots=True)
class RevalidatedExchange:
    invocation: FrozenInvocation
    expectation: ResponseExpectation


@dataclass(frozen=True, slots=True)
class LoadedPendingJournal:
    journal: PendingJournal
    original: RevalidatedExchange
    reconcile_attempt: RevalidatedExchange | None


def _validated_exchange_model(exchange: PendingExchange) -> PendingExchange:
    if type(exchange) is not PendingExchange:
        raise DurableValidationError("pending exchange must be exact")
    try:
        return PendingExchange.model_validate(
            exchange.model_dump(mode="python", round_trip=True, warnings=False)
        )
    except (ValidationError, TypeError, ValueError, OverflowError, RecursionError) as error:
        raise DurableValidationError("pending exchange structure is invalid") from error


def revalidate_pending_exchange(
    exchange: PendingExchange,
    *,
    binding: ReleaseBinding,
) -> RevalidatedExchange:
    validated_exchange = _validated_exchange_model(exchange)
    try:
        snapshot = revalidate_runtime_snapshot(binding.snapshot)
        if binding.registry.manifest_sha256 != snapshot.operation_manifest_sha256:
            raise DurableValidationError("release binding manifest is inconsistent")
        if validated_exchange.runtime_snapshot != snapshot:
            raise DurableValidationError("pending exchange snapshot does not match release binding")

        raw_body = validated_exchange.body_json.encode("utf-8")
        parsed_body = _parse_duplicate_free_json(raw_body)
        body = RequestBody.model_validate(parsed_body)
        if canonical_body_bytes(body) != raw_body:
            raise DurableValidationError("pending body is not canonical")
        invocation = binding.registry.resolve_wire_invocation(body.operation, body.arguments)
        if invocation.contract.name != validated_exchange.operation:
            raise DurableValidationError("pending operation does not match frozen invocation")
        if invocation.effective_class is not validated_exchange.effective_class:
            raise DurableValidationError("pending effective class does not match frozen invocation")
        if invocation.result_schema.schema_id != validated_exchange.result_schema_id:
            raise DurableValidationError("pending result schema does not match frozen invocation")
        recovery_schema_id = (
            None if invocation.recovery_schema is None else invocation.recovery_schema.schema_id
        )
        if recovery_schema_id != validated_exchange.recovery_schema_id:
            raise DurableValidationError("pending recovery schema does not match frozen invocation")

        if validated_exchange.original_target_request_id is not None:
            wire_arguments = invocation.wire_arguments
            if not isinstance(wire_arguments, ReconcileCommitWireArgs):
                raise DurableValidationError("reconcile attempt does not contain commit arguments")
            if wire_arguments.request_id != validated_exchange.original_target_request_id:
                raise DurableValidationError("reconcile target ID does not match wire arguments")

        allowed = tuple(
            AllowedDelivery(
                delivery_id=intent.delivery_id,
                delivery_kind=intent.delivery_kind,
            )
            for intent in validated_exchange.delivery_intents
        )
        expectation = ResponseExpectation(
            request_id=validated_exchange.request_id,
            allowed_deliveries=allowed,
            request_hash=validated_exchange.request_hash,
            expected_lineage_id=validated_exchange.expected_lineage_id,
            expected_activation_id=validated_exchange.expected_activation_id,
            status_bootstrap=False,
            activation_candidate=None,
            runtime_snapshot=snapshot,
            invocation=invocation,
        )
        rebuilt = RevalidatedExchange(invocation=invocation, expectation=expectation)
        if validated_exchange.response_artifact is not None:
            revalidate_accepted_artifact(
                validated_exchange.response_artifact,
                validated_exchange.settlement,
                validated=rebuilt,
            )
        return rebuilt
    except DurableValidationError:
        raise
    except (
        BridgeError,
        ValidationError,
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise DurableValidationError("pending exchange semantics are invalid") from error


def revalidate_accepted_artifact(
    artifact: ResponseArtifact,
    settlement: Settlement | None,
    *,
    validated: RevalidatedExchange,
) -> ResponseArtifact:
    try:
        rebuilt_artifact = ResponseArtifact.model_validate(
            artifact.model_dump(mode="python", round_trip=True, warnings=False)
        )
        accepted_tree = rebuilt_artifact.accepted_response.model_dump(mode="json")
        inner = _canonical_json_bytes(
            rebuilt_artifact.accepted_response.envelope.model_dump(mode="json")
        )
        outer = _canonical_json_bytes({"Comments": inner.decode("utf-8")})
        reparsed = parse_inst_response(outer, validated.expectation)
        if _canonical_json_bytes(reparsed.model_dump(mode="json")) != _canonical_json_bytes(
            accepted_tree
        ):
            raise DurableValidationError("accepted response differs from semantic reconstruction")
        if _canonical_model_bytes(reparsed.settlement) != _canonical_model_bytes(settlement):
            raise DurableValidationError(
                "accepted response settlement does not match durable value"
            )
        return ResponseArtifact(
            filename=rebuilt_artifact.filename,
            sha256=rebuilt_artifact.sha256,
            size_bytes=rebuilt_artifact.size_bytes,
            accepted_at_ms=rebuilt_artifact.accepted_at_ms,
            accepted_response=AcceptedResponse.model_validate(
                reparsed.model_dump(mode="python", round_trip=True, warnings=False)
            ),
        )
    except DurableValidationError:
        raise
    except (
        BridgeError,
        ValidationError,
        TypeError,
        ValueError,
        OverflowError,
        RecursionError,
    ) as error:
        raise DurableValidationError("accepted response artifact semantics are invalid") from error
