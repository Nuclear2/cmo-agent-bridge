from __future__ import annotations

import json
import math
from typing import cast

from pydantic import (
    JsonValue,
    TypeAdapter,
    ValidationError,
)

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.kinds import OperationClass
from cmo_agent_bridge.operations.models import ReconcileCommitWireArgs
from cmo_agent_bridge.protocol.models import (
    CancelAckResult,
    DeliveryKind,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CancelledSettlement,
    CompletedSettlement,
    RejectedSettlement,
    ResponseCorrelation,
    ResponseEnvelope,
    Settlement,
)
from cmo_agent_bridge.protocol.runtime import derive_runtime_tag


def _protocol_error(message: str, details: dict[str, object] | None = None) -> BridgeError:
    return BridgeError(ErrorCode.PROTOCOL_ERROR, message, details)


class RetryableResponseJsonError(BridgeError):
    """A JSON decode failure that may represent an in-progress ExportInst write."""


def _retryable_json_error(description: str) -> RetryableResponseJsonError:
    return RetryableResponseJsonError(
        ErrorCode.PROTOCOL_ERROR,
        f"invalid {description} JSON",
    )


def _reject_constant(value: str) -> object:
    raise ValueError(f"invalid JSON constant: {value}")


def _finite_float(value: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError("JSON number is outside the finite float range")
    return parsed


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON member: {key}")
        value[key] = item
    return value


def _parse_json(raw: bytes | str, description: str) -> object:
    try:
        return cast(
            object,
            json.loads(
                raw,
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
                parse_float=_finite_float,
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise _retryable_json_error(description) from error


def _validation_errors(error: ValidationError) -> list[dict[str, object]]:
    return cast(
        list[dict[str, object]],
        error.errors(
            include_url=False,
            include_context=False,
            include_input=False,
        ),
    )


def _parse_inner_object(raw: bytes) -> dict[str, object]:
    outer = _parse_json(raw, "outer .inst")
    if not isinstance(outer, dict):
        raise _protocol_error("outer .inst JSON must be an object")
    outer_object = cast(dict[str, object], outer)
    comments = outer_object.get("Comments")
    if not isinstance(comments, str):
        raise _protocol_error("outer .inst JSON requires string Comments")

    inner = _parse_json(comments, "Comments envelope")
    if not isinstance(inner, dict):
        raise _protocol_error("Comments envelope JSON must be an object")
    return cast(dict[str, object], inner)


def _parse_correlation(inner: dict[str, object]) -> ResponseCorrelation:
    try:
        return ResponseCorrelation.model_validate(inner)
    except ValidationError as error:
        raise _protocol_error(
            "invalid response correlation fields",
            {"validation_errors": _validation_errors(error)},
        ) from error


def _parse_envelope(inner: dict[str, object]) -> ResponseEnvelope:
    try:
        return ResponseEnvelope.model_validate(inner)
    except ValidationError as error:
        raise _protocol_error(
            "invalid response envelope",
            {"validation_errors": _validation_errors(error)},
        ) from error


def _validated_delivery_kind(value: object) -> DeliveryKind:
    if value == "request":
        return "request"
    if value == "cancel":
        return "cancel"
    raise _protocol_error("response expectation has invalid delivery kind")


def _matching_delivery(
    correlation: ResponseCorrelation, expectation: ResponseExpectation
) -> DeliveryKind:
    if correlation.request_id != expectation.request_id:
        raise _protocol_error("response request ID does not match expectation")
    if correlation.request_hash != expectation.request_hash:
        raise _protocol_error("response request hash does not match expectation")

    matches = tuple(
        allowed
        for allowed in expectation.allowed_deliveries
        if allowed.delivery_id == correlation.delivery_id
    )
    if len(matches) != 1:
        raise _protocol_error("response delivery ID is not uniquely allowed")
    return _validated_delivery_kind(matches[0].delivery_kind)


def _validate_reported_runtime_tag(envelope: ResponseEnvelope) -> None:
    try:
        expected_tag = derive_runtime_tag(envelope.bridge_version, envelope.runtime_asset_sha256)
    except ValueError as error:
        raise _protocol_error(
            "response runtime tag has an invalid bridge version or asset digest"
        ) from error
    if envelope.runtime_tag != expected_tag:
        raise _protocol_error("response runtime tag does not match bridge version and asset digest")


def _validate_context(envelope: ResponseEnvelope, expectation: ResponseExpectation) -> None:
    operation = expectation.invocation.contract.name
    is_status = operation == "bridge.status"
    error_code = None if envelope.error is None else envelope.error.code
    lineage_exception = not envelope.ok and error_code is ErrorCode.SCENARIO_CHANGED
    manifest_exception = not envelope.ok and error_code is ErrorCode.MANIFEST_MISMATCH

    if expectation.status_bootstrap:
        if (
            not is_status
            or expectation.expected_lineage_id is not None
            or expectation.expected_activation_id is not None
            or expectation.activation_candidate is None
        ):
            raise _protocol_error("invalid bootstrap status expectation")
    elif is_status:
        if expectation.expected_lineage_id is None or expectation.activation_candidate is None:
            raise _protocol_error("known-lineage status expectation is incomplete")
        if (
            not lineage_exception
            and envelope.scenario_lineage_id != expectation.expected_lineage_id
        ):
            raise _protocol_error("response scenario lineage does not match expectation")
    else:
        if expectation.expected_lineage_id is None or expectation.expected_activation_id is None:
            raise _protocol_error("non-status response expectation requires lineage and activation")
        if (
            not lineage_exception
            and envelope.scenario_lineage_id != expectation.expected_lineage_id
        ):
            raise _protocol_error("response scenario lineage does not match expectation")

    expected_activation = (
        expectation.activation_candidate if is_status else expectation.expected_activation_id
    )
    if expected_activation is None or envelope.activation_id != expected_activation:
        raise _protocol_error("response activation does not match expectation")

    if manifest_exception:
        return

    snapshot = expectation.runtime_snapshot
    expected_identity = (
        ("runtime version", envelope.bridge_version, snapshot.runtime_version),
        (
            "runtime asset",
            envelope.runtime_asset_sha256,
            snapshot.runtime_asset_sha256,
        ),
        ("runtime tag", envelope.runtime_tag, snapshot.runtime_tag),
        ("release", envelope.release_id, snapshot.release_id),
        (
            "operation manifest",
            envelope.operation_manifest_sha256,
            snapshot.operation_manifest_sha256,
        ),
    )
    for label, reported, expected in expected_identity:
        if reported != expected:
            raise _protocol_error(f"response {label} does not match expectation")


def _validated_adapter_result(
    adapter: TypeAdapter[object], value: JsonValue, description: str
) -> JsonValue:
    try:
        validated = adapter.validate_python(value)
        dumped = adapter.dump_python(validated, mode="json")
        json.dumps(dumped, allow_nan=False)
        return cast(JsonValue, dumped)
    except (ValidationError, TypeError, ValueError, OverflowError, RecursionError) as error:
        raise _protocol_error(f"{description} does not match frozen operation schema") from error


def _validate_status_result(
    result: JsonValue,
    envelope: ResponseEnvelope,
    expectation: ResponseExpectation,
) -> None:
    if not isinstance(result, dict):
        raise _protocol_error("status result is not a JSON object")
    expected_identity = {
        "lineage_id": str(envelope.scenario_lineage_id),
        "activation_id": str(envelope.activation_id),
        "manifest_sha256": envelope.operation_manifest_sha256,
        "runtime_version": envelope.bridge_version,
        "runtime_tag": envelope.runtime_tag,
        "runtime_asset_sha256": envelope.runtime_asset_sha256,
        "release_id": envelope.release_id,
    }
    snapshot_identity = {
        "manifest_sha256": expectation.runtime_snapshot.operation_manifest_sha256,
        "runtime_version": expectation.runtime_snapshot.runtime_version,
        "runtime_tag": expectation.runtime_snapshot.runtime_tag,
        "runtime_asset_sha256": expectation.runtime_snapshot.runtime_asset_sha256,
        "release_id": expectation.runtime_snapshot.release_id,
    }
    for field, expected in expected_identity.items():
        if result.get(field) != expected:
            raise _protocol_error(f"status result {field} does not match response envelope")
        if field in snapshot_identity and result.get(field) != snapshot_identity[field]:
            raise _protocol_error(f"status result {field} does not match runtime snapshot")


def _validate_mutation_not_started_evidence(
    envelope: ResponseEnvelope,
    expectation: ResponseExpectation,
    delivery_kind: DeliveryKind,
) -> None:
    error = envelope.error
    evidence = None if error is None else error.mutation_not_started
    if evidence is None:
        return
    if envelope.ok:
        raise _protocol_error("mutation-not-started evidence is forbidden on success")
    if delivery_kind != "request":
        raise _protocol_error("mutation-not-started evidence is forbidden on cancel delivery")
    if expectation.invocation.effective_class not in {
        OperationClass.MUTATION,
        OperationClass.DESTRUCTIVE,
        OperationClass.RECONCILE,
    }:
        raise _protocol_error("mutation-not-started evidence is forbidden for safe operations")
    if evidence.request_id != envelope.request_id or evidence.request_id != expectation.request_id:
        raise _protocol_error("mutation-not-started evidence request ID does not match")
    if (
        evidence.request_hash != envelope.request_hash
        or evidence.request_hash != expectation.request_hash
    ):
        raise _protocol_error("mutation-not-started evidence request hash does not match")
    if evidence.operation != expectation.invocation.contract.name:
        raise _protocol_error("mutation-not-started evidence operation does not match")


def _classify_error_settlement(
    envelope: ResponseEnvelope,
    expectation: ResponseExpectation,
    delivery_kind: DeliveryKind,
) -> Settlement | None:
    error = envelope.error
    if error is None:
        raise _protocol_error("failed response has no error")
    if delivery_kind == "cancel" or error.code is ErrorCode.MANIFEST_MISMATCH:
        return None
    if expectation.invocation.effective_class in {
        OperationClass.STATUS,
        OperationClass.READ,
    }:
        return RejectedSettlement(state="rejected", error=error)
    if error.mutation_not_started is not None:
        return RejectedSettlement(state="rejected", error=error)
    return None


def _reconcile_commit_wire_arguments(
    expectation: ResponseExpectation,
) -> ReconcileCommitWireArgs | None:
    if (
        expectation.invocation.contract.name != "bridge.reconcile"
        or expectation.invocation.effective_class is not OperationClass.RECONCILE
    ):
        return None
    wire_arguments = expectation.invocation.wire_arguments
    if not isinstance(wire_arguments, ReconcileCommitWireArgs):
        raise _protocol_error("reconcile commit has invalid frozen wire arguments")
    return wire_arguments


def _validate_raw_reconcile_commit_result(
    result: JsonValue,
    expectation: ResponseExpectation,
) -> None:
    if _reconcile_commit_wire_arguments(expectation) is None:
        return
    if not isinstance(result, dict) or result.get("resolved") is not True:
        raise _protocol_error("reconcile result resolved flag must be exact JSON true")


def _validate_reconcile_commit_result(
    result: JsonValue,
    expectation: ResponseExpectation,
) -> None:
    wire_arguments = _reconcile_commit_wire_arguments(expectation)
    if wire_arguments is None:
        return
    if not isinstance(result, dict):
        raise _protocol_error("reconcile commit result is not an object")
    if result.get("request_id") != str(wire_arguments.request_id):
        raise _protocol_error("reconcile result request ID does not match frozen target")
    if result.get("disposition") != wire_arguments.disposition:
        raise _protocol_error("reconcile result disposition does not match frozen target")


def _rebuild_envelope_result(
    envelope: ResponseEnvelope,
    result: JsonValue,
) -> ResponseEnvelope:
    payload = envelope.model_dump(mode="json")
    payload["result"] = result
    try:
        return ResponseEnvelope.model_validate(payload)
    except ValidationError as error:
        raise _protocol_error("normalized response envelope is invalid") from error


def _validate_cancel_ack(
    envelope: ResponseEnvelope, expectation: ResponseExpectation
) -> tuple[CancelAckResult, JsonValue | None]:
    try:
        acknowledgement = CancelAckResult.model_validate(envelope.result)
    except ValidationError as error:
        raise _protocol_error("cancel acknowledgement does not match cancel schema") from error

    request_delivery_ids = {
        allowed.delivery_id
        for allowed in expectation.allowed_deliveries
        if allowed.delivery_kind == "request"
    }
    if acknowledgement.request_id != expectation.request_id:
        raise _protocol_error("cancel acknowledgement request ID does not match expectation")
    if acknowledgement.request_hash != expectation.request_hash:
        raise _protocol_error("cancel acknowledgement request hash does not match expectation")
    if acknowledgement.original_delivery_id not in request_delivery_ids:
        raise _protocol_error("cancel acknowledgement original delivery is not allowed")

    if acknowledgement.status == "cancelled":
        normalized = CancelAckResult.model_validate(acknowledgement.model_dump(mode="json"))
        return normalized, None
    recovery_adapter = expectation.invocation.recovery_adapter
    if recovery_adapter is None:
        raise _protocol_error("completed cancel acknowledgement has no recovery schema")
    _validate_raw_reconcile_commit_result(acknowledgement.result, expectation)
    nested_result = _validated_adapter_result(
        recovery_adapter,
        acknowledgement.result,
        "completed cancel result",
    )
    _validate_reconcile_commit_result(nested_result, expectation)
    acknowledgement_payload = acknowledgement.model_dump(mode="json")
    acknowledgement_payload["result"] = nested_result
    try:
        normalized = CancelAckResult.model_validate(acknowledgement_payload)
    except ValidationError as error:
        raise _protocol_error("normalized cancel acknowledgement is invalid") from error
    return normalized, nested_result


def _build_accepted_response(
    *,
    envelope: ResponseEnvelope,
    delivery_kind: DeliveryKind,
    settlement: Settlement | None,
    cancel_ack: CancelAckResult | None,
) -> AcceptedResponse:
    try:
        return AcceptedResponse(
            envelope=envelope,
            delivery_kind=delivery_kind,
            settlement=settlement,
            cancel_ack=cancel_ack,
        )
    except ValidationError as error:
        raise _protocol_error(
            "accepted response violates protocol invariants",
            {"validation_errors": _validation_errors(error)},
        ) from error


def parse_inst_response(raw: bytes, expectation: ResponseExpectation) -> AcceptedResponse:
    inner = _parse_inner_object(raw)
    correlation = _parse_correlation(inner)
    delivery_kind = _matching_delivery(correlation, expectation)
    envelope = _parse_envelope(inner)
    _validate_reported_runtime_tag(envelope)
    _validate_context(envelope, expectation)
    _validate_mutation_not_started_evidence(envelope, expectation, delivery_kind)

    if not envelope.ok:
        return _build_accepted_response(
            envelope=envelope,
            delivery_kind=delivery_kind,
            settlement=_classify_error_settlement(envelope, expectation, delivery_kind),
            cancel_ack=None,
        )

    if delivery_kind == "request":
        _validate_raw_reconcile_commit_result(envelope.result, expectation)
        result = _validated_adapter_result(
            expectation.invocation.result_adapter,
            envelope.result,
            "response result",
        )
        _validate_reconcile_commit_result(result, expectation)
        if expectation.invocation.contract.name == "bridge.status":
            _validate_status_result(result, envelope, expectation)
        normalized_envelope = _rebuild_envelope_result(envelope, result)
        return _build_accepted_response(
            envelope=normalized_envelope,
            delivery_kind=delivery_kind,
            settlement=CompletedSettlement(state="completed", result=result),
            cancel_ack=None,
        )

    acknowledgement, result = _validate_cancel_ack(envelope, expectation)
    normalized_ack_result = cast(JsonValue, acknowledgement.model_dump(mode="json"))
    normalized_envelope = _rebuild_envelope_result(envelope, normalized_ack_result)
    settlement: Settlement
    if acknowledgement.status == "cancelled":
        settlement = CancelledSettlement(state="cancelled")
    else:
        if result is None:
            raise _protocol_error("completed cancel acknowledgement has no normalized result")
        settlement = CompletedSettlement(state="completed", result=result)
    return _build_accepted_response(
        envelope=normalized_envelope,
        delivery_kind=delivery_kind,
        settlement=settlement,
        cancel_ack=acknowledgement,
    )
