from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import replace
from typing import Literal, cast
from uuid import UUID

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.errors import ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.models import (
    AllowedDelivery,
    CancelAckResult,
    DeliveryKind,
    ResponseExpectation,
)
from cmo_agent_bridge.protocol.response_models import (
    AcceptedResponse,
    CancelledSettlement,
    CompletedSettlement,
    MutationNotStartedEvidence,
    RejectedSettlement,
    ResponseArtifact,
    ResponseEnvelope,
    ResponseError,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


REQUEST_ID = UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
REQUEST_DELIVERY_ID = UUID("11111111-1111-4111-8111-111111111111")
CANCEL_DELIVERY_ID = UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
REQUEST_HASH = "a" * 64
RESULT = cast(JsonValue, {"items": [True, 1, 1.0], "label": "caf\u00e9"})
RUNTIME_SNAPSHOT = RuntimeSnapshot.create(
    runtime_version="0.1.0",
    runtime_asset_sha256="b" * 64,
    operation_manifest_sha256=MANIFEST_SHA256,
    host_contract_sha256="c" * 64,
    dependency_lock_sha256="d" * 64,
)
LITERAL_VECTOR_RUNTIME_SNAPSHOT = RuntimeSnapshot.create(
    runtime_version="0.1.0",
    runtime_asset_sha256="b" * 64,
    operation_manifest_sha256="09ebb6c0c266637178a21b78cab1da3b641dc74eeef76d3e2f6bb740bc665fc5",
    host_contract_sha256="c" * 64,
    dependency_lock_sha256="d" * 64,
)


def _error(*, details: dict[str, JsonValue] | None = None) -> ResponseError:
    return ResponseError(
        code=ErrorCode.CMO_LUA_ERROR,
        message="runtime rejected request",
        details={} if details is None else details,
    )


def _envelope(
    *,
    ok: bool,
    result: JsonValue = RESULT,
    error: ResponseError | None = None,
) -> ResponseEnvelope:
    return ResponseEnvelope(
        protocol="cmo-agent-bridge/1",
        request_id=REQUEST_ID,
        delivery_id=REQUEST_DELIVERY_ID,
        request_hash=REQUEST_HASH,
        ok=ok,
        result=result if ok else None,
        error=None if ok else (error or _error()),
        scenario_time="2026-07-10T13:00:00Z",
        scenario_lineage_id=LINEAGE_ID,
        activation_id=ACTIVATION_ID,
        operation_manifest_sha256=MANIFEST_SHA256,
        bridge_version=RUNTIME_SNAPSHOT.runtime_version,
        runtime_tag=RUNTIME_SNAPSHOT.runtime_tag,
        runtime_asset_sha256=RUNTIME_SNAPSHOT.runtime_asset_sha256,
        release_id=RUNTIME_SNAPSHOT.release_id,
    )


def _ack(status: Literal["cancelled", "completed"]) -> CancelAckResult:
    return CancelAckResult(
        request_id=REQUEST_ID,
        request_hash=REQUEST_HASH,
        original_delivery_id=REQUEST_DELIVERY_ID,
        status=status,
        result=None if status == "cancelled" else RESULT,
    )


def _settlement(name: str) -> object:
    if name == "none":
        return None
    if name == "completed":
        return CompletedSettlement(state="completed", result=RESULT)
    if name == "cancelled":
        return CancelledSettlement(state="cancelled")
    if name == "rejected":
        return RejectedSettlement(state="rejected", error=_error())
    raise AssertionError(name)


def _cancel_ack(name: str) -> CancelAckResult | None:
    if name == "none":
        return None
    return _ack(cast(Literal["cancelled", "completed"], name))


VALID_ACCEPTED_SHAPES = {
    ("request", "success", "completed", "none"),
    ("request", "error", "rejected", "none"),
    ("request", "error", "none", "none"),
    ("cancel", "success", "cancelled", "cancelled"),
    ("cancel", "success", "completed", "completed"),
    ("cancel", "error", "none", "none"),
}
ALL_ACCEPTED_SHAPES = tuple(
    itertools.product(
        ("request", "cancel"),
        ("success", "error"),
        ("none", "completed", "cancelled", "rejected"),
        ("none", "cancelled", "completed"),
    )
)


@pytest.mark.parametrize(
    ("delivery", "envelope_state", "settlement_name", "ack_name"),
    ALL_ACCEPTED_SHAPES,
    ids=("-".join(shape) for shape in ALL_ACCEPTED_SHAPES),
)
def test_accepted_response_exhaustive_shape_matrix(
    delivery: DeliveryKind,
    envelope_state: str,
    settlement_name: str,
    ack_name: str,
) -> None:
    acknowledgement = _cancel_ack(ack_name)
    if delivery == "cancel" and envelope_state == "success":
        envelope_ack = acknowledgement or _ack("cancelled")
        envelope = _envelope(ok=True, result=envelope_ack.model_dump(mode="json"))
    else:
        envelope = _envelope(ok=envelope_state == "success")
    arguments = {
        "envelope": envelope,
        "delivery_kind": delivery,
        "settlement": _settlement(settlement_name),
        "cancel_ack": acknowledgement,
    }
    shape = (delivery, envelope_state, settlement_name, ack_name)

    if shape in VALID_ACCEPTED_SHAPES:
        AcceptedResponse.model_validate(arguments)
    else:
        with pytest.raises(ValidationError):
            AcceptedResponse.model_validate(arguments)


def test_accepted_response_matrix_has_exactly_six_valid_shapes() -> None:
    assert len(ALL_ACCEPTED_SHAPES) == 48
    assert len(VALID_ACCEPTED_SHAPES) == 6


def test_completed_settlement_rejects_null_result() -> None:
    with pytest.raises(ValidationError):
        CompletedSettlement(state="completed", result=None)


@pytest.mark.parametrize(
    ("envelope_result", "settlement_result", "valid"),
    [
        ({"b": 2, "a": 1}, {"a": 1, "b": 2}, True),
        ({"value": True}, {"value": 1}, False),
        ({"value": 1}, {"value": 1.0}, False),
        ({"value": "\u00e9"}, {"value": "e\u0301"}, False),
    ],
)
def test_completed_result_uses_canonical_json_byte_equality(
    envelope_result: JsonValue,
    settlement_result: JsonValue,
    valid: bool,
) -> None:
    arguments = {
        "envelope": _envelope(ok=True, result=envelope_result),
        "delivery_kind": "request",
        "settlement": CompletedSettlement(state="completed", result=settlement_result),
        "cancel_ack": None,
    }
    if valid:
        AcceptedResponse.model_validate(arguments)
    else:
        with pytest.raises(ValidationError):
            AcceptedResponse.model_validate(arguments)


def test_rejected_error_uses_canonical_json_byte_equality() -> None:
    envelope_error = _error(details={"b": 2, "a": True})
    matching = _error(details={"a": True, "b": 2})
    accepted = AcceptedResponse(
        envelope=_envelope(ok=False, error=envelope_error),
        delivery_kind="request",
        settlement=RejectedSettlement(state="rejected", error=matching),
        cancel_ack=None,
    )
    assert isinstance(accepted.settlement, RejectedSettlement)

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=_envelope(ok=False, error=envelope_error),
            delivery_kind="request",
            settlement=RejectedSettlement(
                state="rejected",
                error=_error(details={"a": 1, "b": 2}),
            ),
            cancel_ack=None,
        )


def test_completed_cancel_nested_result_uses_canonical_json_byte_equality() -> None:
    acknowledgement = _ack("completed")
    envelope = _envelope(ok=True, result=acknowledgement.model_dump(mode="json"))
    AcceptedResponse(
        envelope=envelope,
        delivery_kind="cancel",
        settlement=CompletedSettlement(
            state="completed",
            result={"label": "caf\u00e9", "items": [True, 1, 1.0]},
        ),
        cancel_ack=acknowledgement,
    )

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=envelope,
            delivery_kind="cancel",
            settlement=CompletedSettlement(
                state="completed",
                result={"label": "cafe\u0301", "items": [True, 1, 1.0]},
            ),
            cancel_ack=acknowledgement,
        )


def test_accepted_response_durable_json_round_trip_revalidates_union() -> None:
    accepted = AcceptedResponse(
        envelope=_envelope(ok=True),
        delivery_kind="request",
        settlement=CompletedSettlement(state="completed", result=RESULT),
        cancel_ack=None,
    )

    reloaded = AcceptedResponse.model_validate_json(accepted.model_dump_json())

    assert reloaded == accepted
    assert isinstance(reloaded.settlement, CompletedSettlement)


def test_accepted_response_rejects_non_utf8_json_even_without_settlement() -> None:
    envelope = _envelope(ok=False, error=_error(details={"value": "\ud800"}))

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=envelope,
            delivery_kind="request",
            settlement=None,
            cancel_ack=None,
        )


def test_accepted_response_revalidates_forged_envelope_instance() -> None:
    forged = _envelope(ok=True).model_copy(update={"ok": 1})

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=forged,
            delivery_kind="request",
            settlement=CompletedSettlement(state="completed", result=RESULT),
            cancel_ack=None,
        )


def test_accepted_response_revalidates_forged_error_instance() -> None:
    forged_error = _error().model_copy(update={"code": "NOT_A_CODE"})
    forged_envelope = _envelope(ok=False).model_copy(update={"error": forged_error})

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=forged_envelope,
            delivery_kind="request",
            settlement=None,
            cancel_ack=None,
        )


def test_accepted_response_revalidates_forged_settlement_instance() -> None:
    forged = CompletedSettlement(state="completed", result=RESULT).model_copy(
        update={"state": "cancelled"}
    )

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=_envelope(ok=True),
            delivery_kind="request",
            settlement=forged,
            cancel_ack=None,
        )


def test_accepted_response_revalidates_forged_cancel_ack_instance() -> None:
    forged = _ack("completed").model_copy(update={"request_id": "not-a-uuid"})
    envelope = _envelope(ok=True, result=forged.model_dump(mode="json", warnings=False))

    with pytest.raises(ValidationError):
        AcceptedResponse(
            envelope=envelope,
            delivery_kind="cancel",
            settlement=CompletedSettlement(state="completed", result=RESULT),
            cancel_ack=forged,
        )


def _evidence(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 1,
        "stage": "dispatch_validation",
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "operation": "unit.assign_mission",
        "mutation_barrier_written": False,
        "execute_started": False,
    }
    value.update(changes)
    return value


@pytest.mark.parametrize("stage", ["dispatch_validation", "handler_preflight"])
def test_mutation_not_started_evidence_accepts_only_exact_strict_shape(stage: str) -> None:
    evidence = MutationNotStartedEvidence.model_validate(_evidence(stage=stage))
    assert evidence.schema_version == 1
    assert evidence.stage == stage
    assert evidence.mutation_barrier_written is False
    assert evidence.execute_started is False


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("schema_version", True),
        ("schema_version", 1.0),
        ("schema_version", 2),
        ("mutation_barrier_written", 0),
        ("mutation_barrier_written", True),
        ("execute_started", 0),
        ("execute_started", True),
        ("stage", "execute"),
        ("request_hash", b"a" * 64),
    ],
)
def test_mutation_not_started_evidence_rejects_scalar_and_literal_near_misses(
    field: str, wrong: object
) -> None:
    with pytest.raises(ValidationError):
        MutationNotStartedEvidence.model_validate(_evidence(**{field: wrong}))


def test_response_error_rejects_reserved_evidence_in_details() -> None:
    with pytest.raises(ValidationError):
        ResponseError(
            code=ErrorCode.CMO_LUA_ERROR,
            message="bad",
            details={"mutation_not_started": cast(JsonValue, _evidence())},
        )


def _accepted_request() -> AcceptedResponse:
    return AcceptedResponse(
        envelope=_envelope(ok=True),
        delivery_kind="request",
        settlement=CompletedSettlement(state="completed", result=RESULT),
        cancel_ack=None,
    )


def test_response_artifact_accepts_exact_filename_and_metadata() -> None:
    artifact = ResponseArtifact(
        filename=f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
        sha256="f" * 64,
        size_bytes=123,
        accepted_at_ms=456,
        accepted_response=_accepted_request(),
    )
    assert artifact.filename == f"CMOAgentBridge_Response_{REQUEST_ID}.inst"

    reloaded = ResponseArtifact.model_validate_json(artifact.model_dump_json())
    assert reloaded == artifact


@pytest.mark.parametrize(
    "filename",
    [
        f"CMOAgentBridge_Response_{{{REQUEST_ID}}}.inst",
        f"CMOAgentBridge_Response_{str(REQUEST_ID).upper()}.inst",
        f"CMOAgentBridge_Response_{REQUEST_ID.hex}.inst",
        f"CMOAgentBridge_Response_{UUID('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb')}.inst",
        f"CMOAgentBridge_Response_{REQUEST_ID}.INST",
        f"CMOAgentBridge_Response_{REQUEST_ID}.inst.bak",
        f"Other_Response_{REQUEST_ID}.inst",
        f"subdir/CMOAgentBridge_Response_{REQUEST_ID}.inst",
        f"../CMOAgentBridge_Response_{REQUEST_ID}.inst",
        f"C:\\CMOAgentBridge_Response_{REQUEST_ID}.inst",
    ],
)
def test_response_artifact_rejects_filename_near_misses(filename: str) -> None:
    with pytest.raises(ValidationError):
        ResponseArtifact(
            filename=filename,
            sha256="f" * 64,
            size_bytes=123,
            accepted_at_ms=456,
            accepted_response=_accepted_request(),
        )


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("size_bytes", True),
        ("size_bytes", 1.0),
        ("size_bytes", -1),
        ("accepted_at_ms", False),
        ("accepted_at_ms", 1.0),
        ("accepted_at_ms", -1),
    ],
)
def test_response_artifact_rejects_non_strict_or_negative_integers(
    field: str, wrong: object
) -> None:
    arguments: dict[str, object] = {
        "filename": f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
        "sha256": "f" * 64,
        "size_bytes": 123,
        "accepted_at_ms": 456,
        "accepted_response": _accepted_request(),
    }
    arguments[field] = wrong
    with pytest.raises(ValidationError):
        ResponseArtifact.model_validate(arguments)


def test_response_artifact_sha256_is_an_exact_string() -> None:
    with pytest.raises(ValidationError):
        ResponseArtifact.model_validate(
            {
                "filename": f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
                "sha256": b"f" * 64,
                "size_bytes": 123,
                "accepted_at_ms": 456,
                "accepted_response": _accepted_request(),
            }
        )


def test_response_artifact_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        ResponseArtifact.model_validate(
            {
                "filename": f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
                "sha256": "f" * 64,
                "size_bytes": 123,
                "accepted_at_ms": 456,
                "accepted_response": _accepted_request(),
                "raw_bytes": "forbidden",
            }
        )


def test_response_artifact_revalidates_forged_accepted_response_instance() -> None:
    forged = _accepted_request().model_copy(update={"delivery_kind": "bogus"})

    with pytest.raises(ValidationError):
        ResponseArtifact(
            filename=f"CMOAgentBridge_Response_{REQUEST_ID}.inst",
            sha256="f" * 64,
            size_bytes=123,
            accepted_at_ms=456,
            accepted_response=forged,
        )


def _expectation() -> ResponseExpectation:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )
    return ResponseExpectation(
        request_id=REQUEST_ID,
        allowed_deliveries=(AllowedDelivery(REQUEST_DELIVERY_ID, "request"),),
        request_hash=REQUEST_HASH,
        expected_lineage_id=LINEAGE_ID,
        expected_activation_id=ACTIVATION_ID,
        status_bootstrap=False,
        activation_candidate=None,
        runtime_snapshot=RUNTIME_SNAPSHOT,
        invocation=invocation,
    )


def test_response_expectation_accepts_resolved_and_frozen_invocations() -> None:
    resolved = _expectation()
    frozen = OPERATION_REGISTRY.resolve_wire_invocation(
        "unit.assign_mission",
        {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"},
    )

    copied = replace(resolved, invocation=frozen)

    assert copied.invocation is frozen


@pytest.mark.parametrize(
    "changes",
    [
        {"request_hash": "A" * 64},
        {"request_hash": "a" * 63},
        {"status_bootstrap": 1},
        {
            "allowed_deliveries": (
                AllowedDelivery(REQUEST_DELIVERY_ID, "request"),
                AllowedDelivery(REQUEST_DELIVERY_ID, "request"),
            )
        },
        {
            "allowed_deliveries": (
                AllowedDelivery(REQUEST_DELIVERY_ID, cast(DeliveryKind, "bogus")),
            )
        },
        {"invocation": object()},
    ],
)
def test_response_expectation_rejects_invalid_durable_boundary(changes: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(_expectation(), **changes)


def test_response_expectation_revalidates_runtime_snapshot() -> None:
    invalid = RUNTIME_SNAPSHOT.model_copy(update={"runtime_tag": "forged"})

    with pytest.raises(ValueError):
        replace(_expectation(), runtime_snapshot=invalid)


JOURNAL_HEADER_VECTOR = {
    "format": "cmo-agent-bridge/pending-journal",
    "header_version": 1,
    "root_key": "a" * 64,
    "required_release_id": LITERAL_VECTOR_RUNTIME_SNAPSHOT.release_id,
}
DELIVERY_INTENT_VECTOR = {
    "request_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    "delivery_id": "11111111-1111-4111-8111-111111111111",
    "delivery_kind": "request",
    "original_request_delivery_id": "11111111-1111-4111-8111-111111111111",
    "body_json": (
        '{"arguments":{},"expected_activation_id":"44444444-4444-4444-8444-444444444444",'
        '"expected_lineage_id":"33333333-3333-4333-8333-333333333333",'
        '"operation":"scenario.get","operation_manifest_sha256":"'
        + LITERAL_VECTOR_RUNTIME_SNAPSHOT.operation_manifest_sha256
        + '","protocol":"cmo-agent-bridge/1","release_id":"'
        + LITERAL_VECTOR_RUNTIME_SNAPSHOT.release_id
        + '","runtime_asset_sha256":"'
        + LITERAL_VECTOR_RUNTIME_SNAPSHOT.runtime_asset_sha256
        + '","runtime_tag":"'
        + LITERAL_VECTOR_RUNTIME_SNAPSHOT.runtime_tag
        + '","runtime_version":"0.1.0"}'
    ),
    "request_hash": "ed5d47fc0e099d5ee2fd358acd4f0ce3111dacdd0ef7085f2d1a0f9da60983cf",
    "runtime_snapshot": LITERAL_VECTOR_RUNTIME_SNAPSHOT.model_dump(mode="json"),
    "result_schema_id": "7e80762a13e8f43d481079585b09671b73706c2b94f1936cd6f59e2851c1b5df",
    "recovery_schema_id": None,
    "intended_at_ms": 1_752_142_800_000,
    "published_at_ms": None,
    "rendered_inbox_sha256": "e" * 64,
    "rendered_inbox_size_bytes": 512,
    "response_filename": "CMOAgentBridge_Response_aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee.inst",
}
CURRENT_EVIDENCE_VECTOR = {
    "format": "cmo-agent-bridge/reconcile-evidence/1",
    "journal_evidence": {
        "request_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "request_hash": "f" * 64,
        "operation": "unit.set",
        "effective_class": "mutation",
        "revision": 7,
        "state": "quarantined",
        "release_id": LITERAL_VECTOR_RUNTIME_SNAPSHOT.release_id,
        "result_schema_id": "1" * 64,
        "recovery_schema_id": "2" * 64,
        "response_artifact_sha256": "3" * 64,
        "settlement_state": None,
    },
    "barrier_evidence": {
        "request_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "request_hash": "f" * 64,
        "state": "indeterminate",
        "result_sha256": None,
    },
    "ledger_evidence": {
        "request_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "request_hash": "f" * 64,
        "state": "indeterminate",
        "result_sha256": None,
    },
    "quarantined": True,
}


def _canonical_vector(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


CURRENT_EVIDENCE_SHA256 = hashlib.sha256(
    _canonical_vector(CURRENT_EVIDENCE_VECTOR).encode("utf-8")
).hexdigest()
CONFIRMATION_BINDING_VECTOR = {
    "format": "cmo-agent-bridge/reconcile-confirmation/1",
    "root_key": "a" * 64,
    "original_request_id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    "original_request_hash": "f" * 64,
    "scenario_lineage_id": "33333333-3333-4333-8333-333333333333",
    "reserved_activation_id": "44444444-4444-4444-8444-444444444444",
    "original_journal_revision": 7,
    "current_evidence_sha256": CURRENT_EVIDENCE_SHA256,
    "allowed_dispositions": ["applied", "not_applied"],
}
CONTRACT_VECTOR_SHA256 = {
    "journal_header": "938f38c5425bb73d90f512ad06bb32d7255a0bbea59afb1a8b658104408b884d",
    "delivery_intent": "8737efc9ff3ef8d92bb0f6c18a1ef57fb3b67b339a6b9c14cff46e80ab424b42",
    "current_evidence": "86fdd0be678c213d8733ebc254c2ae0f5faed306ba025b7308cb72d1b145ff2a",
    "confirmation_binding": "e52b0ff21380b1cb98631933a34804674378e67ff1844e8ba97a61c63cad8f01",
}


def test_contract_literal_vectors_are_canonical() -> None:
    vectors = {
        "journal_header": JOURNAL_HEADER_VECTOR,
        "delivery_intent": DELIVERY_INTENT_VECTOR,
        "current_evidence": CURRENT_EVIDENCE_VECTOR,
        "confirmation_binding": CONFIRMATION_BINDING_VECTOR,
    }
    body_json = DELIVERY_INTENT_VECTOR["body_json"]
    request_hash = DELIVERY_INTENT_VECTOR["request_hash"]
    assert isinstance(body_json, str)
    assert hashlib.sha256(body_json.encode("utf-8")).hexdigest() == request_hash
    assert (
        DELIVERY_INTENT_VECTOR["original_request_delivery_id"]
        == DELIVERY_INTENT_VECTOR["delivery_id"]
    )
    RuntimeSnapshot.model_validate(DELIVERY_INTENT_VECTOR["runtime_snapshot"])
    assert CONFIRMATION_BINDING_VECTOR["current_evidence_sha256"] == CURRENT_EVIDENCE_SHA256
    dispositions = CONFIRMATION_BINDING_VECTOR["allowed_dispositions"]
    assert isinstance(dispositions, list)
    assert dispositions == sorted(set(dispositions))
    assert _canonical_vector(None) == "null"
    for name, vector in vectors.items():
        canonical = _canonical_vector(vector)
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        assert digest == CONTRACT_VECTOR_SHA256[name]
