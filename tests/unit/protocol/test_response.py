import json
from dataclasses import replace
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest
from pydantic import BaseModel, ConfigDict, TypeAdapter

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256
from cmo_agent_bridge.operations.registry import Adapter, OPERATION_REGISTRY
from cmo_agent_bridge.protocol.models import AllowedDelivery, DeliveryKind, ResponseExpectation
from cmo_agent_bridge.protocol.response import parse_inst_response
from cmo_agent_bridge.protocol.response_models import (
    CancelledSettlement,
    CompletedSettlement,
    RejectedSettlement,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


REQUEST_ID = UUID("11111111-1111-4111-8111-111111111111")
REQUEST_DELIVERY_ID = UUID("22222222-2222-4222-8222-222222222222")
LINEAGE_ID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_ID = UUID("44444444-4444-4444-8444-444444444444")
CANCEL_DELIVERY_ID = UUID("55555555-5555-4555-8555-555555555555")
OTHER_LINEAGE_ID = UUID("66666666-6666-4666-8666-666666666666")
REQUEST_HASH = "a" * 64
MANIFEST_HASH = MANIFEST_SHA256
RUNTIME_SNAPSHOT = RuntimeSnapshot.create(
    runtime_version="0.1.0",
    runtime_asset_sha256="b" * 64,
    operation_manifest_sha256=MANIFEST_HASH,
    host_contract_sha256="c" * 64,
    dependency_lock_sha256="d" * 64,
)


class RequestOnlyResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_value: int


class RecoveryOnlyResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    recovery_value: int


class DefaultedResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    normalized_value: int = 7


class StringResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    text: str


def _inst(envelope: dict[str, object]) -> bytes:
    return json.dumps(
        {"Comments": json.dumps(envelope, separators=(",", ":"))},
        separators=(",", ":"),
    ).encode()


def _outer_with_comments(comments: str) -> bytes:
    return json.dumps({"Comments": comments}, separators=(",", ":")).encode()


def _base_envelope(
    *,
    delivery_id: UUID = REQUEST_DELIVERY_ID,
    lineage_id: UUID = LINEAGE_ID,
    activation_id: UUID = ACTIVATION_ID,
    manifest_hash: str = MANIFEST_HASH,
) -> dict[str, object]:
    return {
        "protocol": "cmo-agent-bridge/1",
        "request_id": str(REQUEST_ID),
        "delivery_id": str(delivery_id),
        "request_hash": REQUEST_HASH,
        "ok": True,
        "result": {
            "unit_guid": "UNIT-1",
            "mission_guid": "MISSION-1",
            "escort": False,
        },
        "error": None,
        "scenario_time": "2026-07-10T13:00:00Z",
        "scenario_lineage_id": str(lineage_id),
        "activation_id": str(activation_id),
        "operation_manifest_sha256": manifest_hash,
        "bridge_version": "0.1.0",
        "runtime_tag": RUNTIME_SNAPSHOT.runtime_tag,
        "runtime_asset_sha256": RUNTIME_SNAPSHOT.runtime_asset_sha256,
        "release_id": RUNTIME_SNAPSHOT.release_id,
    }


def _error_envelope(code: str, **changes: object) -> dict[str, object]:
    envelope = _base_envelope()
    envelope.update(
        ok=False,
        result=None,
        error={"code": code, "message": "runtime rejected request", "details": {}},
    )
    envelope.update(changes)
    return envelope


def _status_result(lineage_id: UUID, activation_id: UUID) -> dict[str, object]:
    return {
        "protocol": "cmo-agent-bridge/1",
        "runtime_version": "0.1.0",
        "runtime_tag": RUNTIME_SNAPSHOT.runtime_tag,
        "runtime_asset_sha256": RUNTIME_SNAPSHOT.runtime_asset_sha256,
        "release_id": RUNTIME_SNAPSHOT.release_id,
        "build": 1868,
        "manifest_sha256": MANIFEST_HASH,
        "lineage_id": str(lineage_id),
        "activation_id": str(activation_id),
        "installed_event_names": ["Initialize", "Poll"],
        "installed_action_names": ["Initialize", "Poll"],
        "installed_trigger_names": ["Loaded", "Regular"],
        "pending_request_id": None,
        "quarantined": False,
        "paused_capability": None,
        "poll_interval_seconds": 5,
        "safe_payload_bytes": 4096,
        "verified_ledger_entries": 32,
        "effective_ledger_capacity": 32,
    }


def _not_started_evidence(operation: str, **changes: object) -> dict[str, object]:
    evidence: dict[str, object] = {
        "schema_version": 1,
        "stage": "dispatch_validation",
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "operation": operation,
        "mutation_barrier_written": False,
        "execute_started": False,
    }
    evidence.update(changes)
    return evidence


def _error_with_not_started(operation: str, **changes: object) -> dict[str, object]:
    envelope = _error_envelope("CMO_LUA_ERROR")
    error = cast(dict[str, object], envelope["error"])
    error["mutation_not_started"] = _not_started_evidence(operation, **changes)
    return envelope


def _assert_protocol_error(
    raw: bytes, expectation: ResponseExpectation, match: str | None = None
) -> BridgeError:
    with pytest.raises(BridgeError, match=match) as caught:
        parse_inst_response(raw, expectation)
    assert caught.value.code is ErrorCode.PROTOCOL_ERROR
    return caught.value


@pytest.fixture
def valid_inst_bytes() -> bytes:
    path = Path(__file__).parents[2] / "fixtures" / "valid_response.inst"
    return path.read_bytes()


@pytest.fixture
def expectation() -> ResponseExpectation:
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


@pytest.fixture
def bootstrap_expectation(expectation: ResponseExpectation) -> ResponseExpectation:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "bridge.status",
        {},
        {"activation_candidate": ACTIVATION_ID},
    )
    return replace(
        expectation,
        expected_lineage_id=None,
        expected_activation_id=None,
        status_bootstrap=True,
        activation_candidate=ACTIVATION_ID,
        invocation=invocation,
    )


@pytest.fixture
def cancel_expectation(expectation: ResponseExpectation) -> ResponseExpectation:
    return replace(
        expectation,
        allowed_deliveries=(
            AllowedDelivery(REQUEST_DELIVERY_ID, "request"),
            AllowedDelivery(CANCEL_DELIVERY_ID, "cancel"),
        ),
    )


def test_response_rejects_wrong_delivery(
    valid_inst_bytes: bytes, expectation: ResponseExpectation
) -> None:
    wrong = valid_inst_bytes.replace(
        str(REQUEST_DELIVERY_ID).encode(),
        b"00000000-0000-4000-8000-000000000000",
    )

    with pytest.raises(BridgeError, match="delivery"):
        parse_inst_response(wrong, expectation)


def test_scenario_changed_allows_only_lineage_exception(
    expectation: ResponseExpectation,
) -> None:
    response = parse_inst_response(
        _inst(
            _error_envelope(
                "SCENARIO_CHANGED",
                scenario_lineage_id=str(OTHER_LINEAGE_ID),
            )
        ),
        expectation,
    )

    assert response.error is not None
    assert response.error.code == "SCENARIO_CHANGED"


def test_bootstrap_status_accepts_new_lineage_but_exact_candidate(
    bootstrap_expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope["result"] = _status_result(LINEAGE_ID, ACTIVATION_ID)
    response = parse_inst_response(_inst(envelope), bootstrap_expectation)

    assert response.envelope.scenario_lineage_id == LINEAGE_ID
    assert response.envelope.activation_id == bootstrap_expectation.activation_candidate


def test_cancel_ack_uses_cancel_schema(cancel_expectation: ResponseExpectation) -> None:
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "cancelled",
        "result": None,
    }
    response = parse_inst_response(_inst(envelope), cancel_expectation)

    assert response.cancel_ack is not None
    assert response.cancel_ack.status == "cancelled"


def test_valid_fixture_uses_frozen_operation_adapter(
    valid_inst_bytes: bytes, expectation: ResponseExpectation
) -> None:
    response = parse_inst_response(valid_inst_bytes, expectation)

    assert response.ok is True
    assert response.result == {
        "unit_guid": "UNIT-1",
        "mission_guid": "MISSION-1",
        "escort": False,
    }


@pytest.mark.parametrize(
    "raw",
    [
        b'{"Comments":',
        b"[]",
        b"null",
        b'{"other":"value"}',
        b'{"Comments":null}',
        b'{"Comments":{}}',
        b'{"Comments":"[1,2,3]"}',
        b'{"Comments":"{\\"protocol\\":"}',
    ],
)
def test_malformed_outer_or_comments_is_protocol_error(
    raw: bytes, expectation: ResponseExpectation
) -> None:
    _assert_protocol_error(raw, expectation)


def test_duplicate_json_members_are_rejected(expectation: ResponseExpectation) -> None:
    duplicate_outer = b'{"Comments":"{}","Comments":"{}"}'
    duplicate_inner = b'{"Comments":"{\\"protocol\\":1,\\"protocol\\":2}"}'

    _assert_protocol_error(duplicate_outer, expectation)
    _assert_protocol_error(duplicate_inner, expectation)


def test_excessively_nested_comments_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    nested = "[" * 5_000 + "0" + "]" * 5_000
    raw = json.dumps({"Comments": nested}).encode()

    _assert_protocol_error(raw, expectation)


def test_non_finite_exponent_in_error_details_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    comments = json.dumps(
        _error_envelope("CMO_LUA_ERROR"),
        separators=(",", ":"),
    ).replace('"details":{}', '"details":{"value":1e999}')

    _assert_protocol_error(_outer_with_comments(comments), expectation)


def test_non_finite_exponent_in_operation_result_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "speed": 18.0},
    )
    mutation_expectation = replace(expectation, invocation=invocation)
    envelope = _base_envelope()
    envelope["result"] = {
        "unit_guid": "UNIT-1",
        "name": "Alpha",
        "speed": 18.0,
        "altitude": 1000.0,
        "heading": 90.0,
        "course": None,
    }
    comments = json.dumps(envelope, separators=(",", ":")).replace('"speed":18.0', '"speed":1e999')

    _assert_protocol_error(_outer_with_comments(comments), mutation_expectation)


@pytest.mark.parametrize("speed", ["1e999", "NaN", "Infinity", "-Infinity"])
def test_adapter_coercion_cannot_create_non_finite_request_result(
    speed: str,
    expectation: ResponseExpectation,
) -> None:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "speed": 18.0},
    )
    mutation_expectation = replace(expectation, invocation=invocation)
    envelope = _base_envelope()
    envelope["result"] = {
        "unit_guid": "UNIT-1",
        "name": "Alpha",
        "speed": speed,
        "altitude": 1000.0,
        "heading": 90.0,
        "course": None,
    }

    _assert_protocol_error(_inst(envelope), mutation_expectation)


def test_adapter_coercion_cannot_create_non_finite_completed_cancel_result(
    cancel_expectation: ResponseExpectation,
) -> None:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "unit.set",
        {"unit_guid": "UNIT-1", "speed": 18.0},
    )
    mutation_expectation = replace(cancel_expectation, invocation=invocation)
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {
            "unit_guid": "UNIT-1",
            "name": "Alpha",
            "speed": "Infinity",
            "altitude": 1000.0,
            "heading": 90.0,
            "course": None,
        },
    }

    _assert_protocol_error(_inst(envelope), mutation_expectation)


@pytest.mark.parametrize(
    "changes",
    [
        {"unexpected": True},
        {"ok": True, "result": None, "error": None},
        {
            "ok": True,
            "error": {"code": "CMO_LUA_ERROR", "message": "bad", "details": {}},
        },
        {"ok": False, "result": None, "error": None},
        {
            "ok": False,
            "result": {"unit_guid": "UNIT-1"},
            "error": {"code": "CMO_LUA_ERROR", "message": "bad", "details": {}},
        },
        {
            "ok": False,
            "result": None,
            "error": {"code": "NOT_A_CODE", "message": "bad", "details": {}},
        },
        {
            "ok": False,
            "result": None,
            "error": {
                "code": "CMO_LUA_ERROR",
                "message": "bad",
                "details": {},
                "extra": True,
            },
        },
    ],
)
def test_envelope_and_error_shapes_are_strict(
    changes: dict[str, object], expectation: ResponseExpectation
) -> None:
    envelope = _base_envelope()
    envelope.update(changes)
    _assert_protocol_error(_inst(envelope), expectation)


def test_protocol_error_details_remain_json_serializable(
    expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope.update(ok=True, result=None, error=None)

    failure = _assert_protocol_error(_inst(envelope), expectation)
    json.dumps(failure.to_payload(), allow_nan=False)


@pytest.mark.parametrize(
    ("field", "wrong", "message"),
    [
        ("request_id", "77777777-7777-4777-8777-777777777777", "request ID"),
        ("request_hash", "c" * 64, "request hash"),
        ("delivery_id", "77777777-7777-4777-8777-777777777777", "delivery ID"),
    ],
)
def test_primary_correlation_precedes_operation_result_validation(
    field: str,
    wrong: str,
    message: str,
    expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope[field] = wrong
    envelope["result"] = {"unit_guid": "missing-other-fields"}

    _assert_protocol_error(_inst(envelope), expectation, message)


def test_delivery_correlation_precedes_error_content_validation(
    expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope.update(
        delivery_id="77777777-7777-4777-8777-777777777777",
        ok=False,
        result=None,
        error={"not": "a response error"},
    )

    _assert_protocol_error(_inst(envelope), expectation, "delivery ID")


@pytest.mark.parametrize(
    ("field", "wrong", "message"),
    [
        ("scenario_lineage_id", str(OTHER_LINEAGE_ID), "lineage"),
        ("activation_id", "77777777-7777-4777-8777-777777777777", "activation"),
        ("operation_manifest_sha256", "c" * 64, "manifest"),
        ("bridge_version", "0.2.0", "runtime"),
        ("runtime_tag", f"0_1_0-{'c' * 64}", "runtime tag"),
        ("runtime_asset_sha256", "c" * 64, "runtime tag"),
        ("release_id", "c" * 64, "release"),
    ],
)
def test_success_requires_exact_context_correlation(
    field: str,
    wrong: str,
    message: str,
    expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope[field] = wrong
    _assert_protocol_error(_inst(envelope), expectation, message)


def test_manifest_mismatch_allows_the_complete_reported_runtime_identity_exception(
    expectation: ResponseExpectation,
) -> None:
    reported = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="c" * 64,
        operation_manifest_sha256="d" * 64,
        host_contract_sha256="e" * 64,
        dependency_lock_sha256="f" * 64,
    )
    accepted = _error_envelope(
        "MANIFEST_MISMATCH",
        operation_manifest_sha256=reported.operation_manifest_sha256,
        bridge_version=reported.runtime_version,
        runtime_tag=reported.runtime_tag,
        runtime_asset_sha256=reported.runtime_asset_sha256,
        release_id=reported.release_id,
    )
    response = parse_inst_response(_inst(accepted), expectation)
    assert response.error is not None
    assert response.error.code is ErrorCode.MANIFEST_MISMATCH

    wrong_lineage = dict(accepted, scenario_lineage_id=str(OTHER_LINEAGE_ID))
    _assert_protocol_error(_inst(wrong_lineage), expectation, "lineage")
    wrong_activation = dict(
        accepted,
        activation_id="77777777-7777-4777-8777-777777777777",
    )
    _assert_protocol_error(_inst(wrong_activation), expectation, "activation")


def test_manifest_mismatch_still_requires_reported_tag_self_consistency(
    expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope(
        "MANIFEST_MISMATCH",
        bridge_version="0.2.0",
        runtime_asset_sha256="c" * 64,
        runtime_tag=f"0_2_0-{'d' * 64}",
        release_id="e" * 64,
        operation_manifest_sha256="f" * 64,
    )

    _assert_protocol_error(_inst(envelope), expectation, "runtime tag")


def test_manifest_mismatch_cannot_bypass_primary_correlation(
    expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope(
        "MANIFEST_MISMATCH",
        request_id="77777777-7777-4777-8777-777777777777",
        runtime_tag="not-even-a-tag",
    )

    _assert_protocol_error(_inst(envelope), expectation, "request ID")


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"activation_id": "77777777-7777-4777-8777-777777777777"}, "activation"),
        ({"operation_manifest_sha256": "c" * 64}, "manifest"),
        (
            {
                "bridge_version": "0.2.0",
                "runtime_tag": f"0_2_0-{'b' * 64}",
            },
            "runtime",
        ),
        (
            {
                "runtime_asset_sha256": "c" * 64,
                "runtime_tag": f"0_1_0-{'c' * 64}",
            },
            "asset",
        ),
        ({"release_id": "c" * 64}, "release"),
    ],
)
def test_scenario_changed_exempts_only_lineage(
    changes: dict[str, object],
    message: str,
    expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope(
        "SCENARIO_CHANGED",
        scenario_lineage_id=str(OTHER_LINEAGE_ID),
    )
    envelope.update(changes)

    _assert_protocol_error(_inst(envelope), expectation, message)


def test_other_errors_do_not_relax_lineage_or_manifest(
    expectation: ResponseExpectation,
) -> None:
    wrong_lineage = _error_envelope("CMO_LUA_ERROR", scenario_lineage_id=str(OTHER_LINEAGE_ID))
    wrong_manifest = _error_envelope("CMO_LUA_ERROR", operation_manifest_sha256="c" * 64)

    _assert_protocol_error(_inst(wrong_lineage), expectation, "lineage")
    _assert_protocol_error(_inst(wrong_manifest), expectation, "manifest")


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        (
            {"bridge_version": "0.2.0", "runtime_tag": f"0_2_0-{'b' * 64}"},
            "runtime version",
        ),
        (
            {
                "runtime_asset_sha256": "c" * 64,
                "runtime_tag": f"0_1_0-{'c' * 64}",
            },
            "runtime asset",
        ),
        ({"release_id": "c" * 64}, "release"),
    ],
)
def test_other_errors_do_not_relax_runtime_identity(
    changes: dict[str, object], message: str, expectation: ResponseExpectation
) -> None:
    envelope = _error_envelope("CMO_LUA_ERROR", **changes)

    _assert_protocol_error(_inst(envelope), expectation, message)


def test_bootstrap_status_rejects_wrong_activation_candidate(
    bootstrap_expectation: ResponseExpectation,
) -> None:
    wrong_activation = UUID("77777777-7777-4777-8777-777777777777")
    envelope = _base_envelope(activation_id=wrong_activation)
    envelope["result"] = _status_result(LINEAGE_ID, wrong_activation)

    _assert_protocol_error(_inst(envelope), bootstrap_expectation, "activation")


@pytest.mark.parametrize(
    "changes",
    [
        {"lineage_id": str(OTHER_LINEAGE_ID)},
        {"activation_id": "77777777-7777-4777-8777-777777777777"},
        {"manifest_sha256": "c" * 64},
        {
            "runtime_version": "0.2.0",
            "runtime_tag": f"0_2_0-{'b' * 64}",
        },
        {
            "runtime_asset_sha256": "c" * 64,
            "runtime_tag": f"0_1_0-{'c' * 64}",
        },
        {"release_id": "c" * 64},
    ],
)
def test_status_result_identity_matches_correlated_envelope(
    changes: dict[str, str],
    bootstrap_expectation: ResponseExpectation,
) -> None:
    result = _status_result(LINEAGE_ID, ACTIVATION_ID)
    result.update(changes)
    envelope = _base_envelope()
    envelope["result"] = result

    _assert_protocol_error(_inst(envelope), bootstrap_expectation, "status result")


def test_only_status_can_use_bootstrap_correlation(
    expectation: ResponseExpectation,
) -> None:
    invalid = replace(
        expectation,
        expected_lineage_id=None,
        expected_activation_id=None,
        status_bootstrap=True,
        activation_candidate=ACTIVATION_ID,
    )
    _assert_protocol_error(_inst(_base_envelope()), invalid, "bootstrap")


def test_known_lineage_status_uses_candidate_not_old_activation(
    expectation: ResponseExpectation,
) -> None:
    old_activation = UUID("77777777-7777-4777-8777-777777777777")
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "bridge.status",
        {},
        {"activation_candidate": ACTIVATION_ID},
    )
    known = replace(
        expectation,
        expected_activation_id=old_activation,
        activation_candidate=ACTIVATION_ID,
        invocation=invocation,
    )
    envelope = _base_envelope()
    envelope["result"] = _status_result(LINEAGE_ID, ACTIVATION_ID)

    assert parse_inst_response(_inst(envelope), known).ok is True

    envelope["activation_id"] = str(old_activation)
    envelope["result"] = _status_result(LINEAGE_ID, old_activation)
    _assert_protocol_error(_inst(envelope), known, "activation")


def test_known_lineage_status_cross_checks_release_identity(
    expectation: ResponseExpectation,
) -> None:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "bridge.status",
        {},
        {"activation_candidate": ACTIVATION_ID},
    )
    known = replace(
        expectation,
        activation_candidate=ACTIVATION_ID,
        invocation=invocation,
    )
    envelope = _base_envelope()
    result = _status_result(LINEAGE_ID, ACTIVATION_ID)
    result["release_id"] = "c" * 64
    envelope["result"] = result

    _assert_protocol_error(_inst(envelope), known, "status result")


def test_mutation_result_schema_failure_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope()
    envelope["result"] = {"unit_guid": "UNIT-1", "mission_guid": "MISSION-1"}
    _assert_protocol_error(_inst(envelope), expectation, "frozen operation schema")


def test_original_request_delivery_during_cancel_wait_uses_request_schema(
    cancel_expectation: ResponseExpectation,
) -> None:
    response = parse_inst_response(_inst(_base_envelope()), cancel_expectation)
    assert response.delivery_kind == "request"
    assert response.cancel_ack is None
    assert response.result == {
        "unit_guid": "UNIT-1",
        "mission_guid": "MISSION-1",
        "escort": False,
    }


def test_completed_cancel_validates_and_returns_recovery_result(
    cancel_expectation: ResponseExpectation,
) -> None:
    original_result = {
        "unit_guid": "UNIT-1",
        "mission_guid": "MISSION-1",
        "escort": False,
    }
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": original_result,
    }

    response = parse_inst_response(_inst(envelope), cancel_expectation)
    assert response.cancel_ack is not None
    assert response.cancel_ack.status == "completed"
    assert response.result == original_result


def test_completed_cancel_schema_failure_is_protocol_error(
    cancel_expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {"unit_guid": "UNIT-1"},
    }
    _assert_protocol_error(_inst(envelope), cancel_expectation, "completed cancel result")


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": "77777777-7777-4777-8777-777777777777"},
        {"request_hash": "c" * 64},
        {"original_delivery_id": str(CANCEL_DELIVERY_ID)},
        {"original_delivery_id": "77777777-7777-4777-8777-777777777777"},
    ],
)
def test_cancel_ack_nested_correlation_is_exact(
    changes: dict[str, object], cancel_expectation: ResponseExpectation
) -> None:
    acknowledgement: dict[str, object] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "cancelled",
        "result": None,
    }
    acknowledgement.update(changes)
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = acknowledgement

    _assert_protocol_error(_inst(envelope), cancel_expectation, "cancel acknowledgement")


def test_cancel_in_progress_evidence_must_be_error_envelope(
    cancel_expectation: ResponseExpectation,
) -> None:
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "in_progress",
        "result": None,
    }
    _assert_protocol_error(_inst(envelope), cancel_expectation, "cancel schema")

    error = _error_envelope("INDETERMINATE_OUTCOME", delivery_id=str(CANCEL_DELIVERY_ID))
    response = parse_inst_response(_inst(error), cancel_expectation)
    assert response.error is not None
    assert response.error.code is ErrorCode.INDETERMINATE_OUTCOME
    assert response.cancel_ack is None


def test_completed_cancel_requires_frozen_recovery_adapter(
    cancel_expectation: ResponseExpectation,
) -> None:
    read_invocation = OPERATION_REGISTRY.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
    no_recovery = replace(cancel_expectation, invocation=read_invocation)
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {"some": "result"},
    }

    _assert_protocol_error(_inst(envelope), no_recovery, "no recovery schema")


def test_duplicate_allowed_delivery_is_rejected_as_ambiguous(
    expectation: ResponseExpectation,
) -> None:
    with pytest.raises(ValueError, match="unique"):
        replace(
            expectation,
            allowed_deliveries=(
                AllowedDelivery(REQUEST_DELIVERY_ID, "request"),
                AllowedDelivery(REQUEST_DELIVERY_ID, "cancel"),
            ),
        )


def test_invalid_allowed_delivery_kind_is_not_treated_as_cancel(
    cancel_expectation: ResponseExpectation,
) -> None:
    with pytest.raises(ValueError, match="delivery kind"):
        replace(
            cancel_expectation,
            allowed_deliveries=(
                AllowedDelivery(REQUEST_DELIVERY_ID, "request"),
                AllowedDelivery(CANCEL_DELIVERY_ID, cast(DeliveryKind, "bogus")),
            ),
        )


def test_request_and_cancel_use_distinct_frozen_adapters(
    cancel_expectation: ResponseExpectation,
) -> None:
    request_adapter = cast(Adapter, TypeAdapter(RequestOnlyResult))
    recovery_adapter = cast(Adapter, TypeAdapter(RecoveryOnlyResult))
    current_recovery = cancel_expectation.invocation.recovery_schema
    assert current_recovery is not None
    invocation = replace(
        cancel_expectation.invocation,
        result_schema=replace(
            cancel_expectation.invocation.result_schema,
            adapter=request_adapter,
        ),
        recovery_schema=replace(
            current_recovery,
            adapter=recovery_adapter,
        ),
    )
    synthetic = replace(cancel_expectation, invocation=invocation)

    request_envelope = _base_envelope()
    request_envelope["result"] = {"request_value": 7}
    assert parse_inst_response(_inst(request_envelope), synthetic).result == {"request_value": 7}

    cancel_envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    cancel_envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {"recovery_value": 9},
    }
    assert parse_inst_response(_inst(cancel_envelope), synthetic).result == {"recovery_value": 9}

    request_envelope["result"] = {"recovery_value": 9}
    _assert_protocol_error(_inst(request_envelope), synthetic)
    cast(dict[str, object], cancel_envelope["result"])["result"] = {"request_value": 7}
    _assert_protocol_error(_inst(cancel_envelope), synthetic)


def test_parser_rebuilds_request_envelope_and_completed_cancel_from_normalized_result(
    cancel_expectation: ResponseExpectation,
) -> None:
    adapter = cast(Adapter, TypeAdapter(DefaultedResult))
    recovery_schema = cancel_expectation.invocation.recovery_schema
    assert recovery_schema is not None
    invocation = replace(
        cancel_expectation.invocation,
        result_schema=replace(
            cancel_expectation.invocation.result_schema,
            adapter=adapter,
        ),
        recovery_schema=replace(recovery_schema, adapter=adapter),
    )
    current = replace(cancel_expectation, invocation=invocation)
    request_envelope = _base_envelope()
    request_envelope["result"] = {}
    cancel_envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    cancel_envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {},
    }

    request = parse_inst_response(_inst(request_envelope), current)
    cancel = parse_inst_response(_inst(cancel_envelope), current)

    normalized = {"normalized_value": 7}
    assert request.envelope.result == normalized
    assert request.result == normalized
    assert cancel.cancel_ack is not None
    assert cancel.cancel_ack.result == normalized
    assert cancel.result == normalized
    assert cast(dict[str, object], cancel.envelope.result)["result"] == normalized


def test_request_success_builds_completed_settlement_and_normalizes_envelope(
    valid_inst_bytes: bytes, expectation: ResponseExpectation
) -> None:
    response = parse_inst_response(valid_inst_bytes, expectation)

    assert isinstance(response.settlement, CompletedSettlement)
    assert response.settlement.result == response.envelope.result
    assert response.result == response.settlement.result


def test_read_and_status_errors_build_rejected_settlements(
    expectation: ResponseExpectation,
    bootstrap_expectation: ResponseExpectation,
) -> None:
    read_invocation = OPERATION_REGISTRY.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
    read_expectation = replace(expectation, invocation=read_invocation)

    for current in (read_expectation, bootstrap_expectation):
        response = parse_inst_response(_inst(_error_envelope("CMO_LUA_ERROR")), current)
        assert isinstance(response.settlement, RejectedSettlement)
        assert response.settlement.error == response.envelope.error


def test_mutation_error_settles_only_with_exact_not_started_evidence(
    expectation: ResponseExpectation,
) -> None:
    unsafe = parse_inst_response(_inst(_error_envelope("CMO_LUA_ERROR")), expectation)
    safe = parse_inst_response(_inst(_error_with_not_started("unit.assign_mission")), expectation)

    assert unsafe.settlement is None
    assert isinstance(safe.settlement, RejectedSettlement)


def test_dynamic_effective_mutation_accepts_exact_not_started_evidence(
    expectation: ResponseExpectation,
) -> None:
    invocation = OPERATION_REGISTRY.resolve_invocation("compat.probe.step", {"step": "dedupe"})
    dynamic_expectation = replace(expectation, invocation=invocation)
    envelope = _error_with_not_started("compat.probe.step")

    response = parse_inst_response(_inst(envelope), dynamic_expectation)

    assert isinstance(response.settlement, RejectedSettlement)


@pytest.mark.parametrize("code", list(ErrorCode))
@pytest.mark.parametrize("operation_kind", ["read", "mutation"])
def test_manifest_mismatch_is_the_only_error_code_that_suppresses_otherwise_safe_settlement(
    code: ErrorCode,
    operation_kind: str,
    expectation: ResponseExpectation,
) -> None:
    if operation_kind == "read":
        invocation = OPERATION_REGISTRY.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
        current = replace(expectation, invocation=invocation)
        envelope = _error_envelope(code.value)
    else:
        current = expectation
        envelope = _error_envelope(code.value)
        cast(dict[str, object], envelope["error"])["mutation_not_started"] = _not_started_evidence(
            "unit.assign_mission"
        )

    response = parse_inst_response(_inst(envelope), current)

    if code is ErrorCode.MANIFEST_MISMATCH:
        assert response.settlement is None
    else:
        assert isinstance(response.settlement, RejectedSettlement)


def test_manifest_mismatch_with_foreign_identity_and_exact_evidence_has_no_settlement(
    expectation: ResponseExpectation,
) -> None:
    reported = RuntimeSnapshot.create(
        runtime_version="0.2.0",
        runtime_asset_sha256="e" * 64,
        operation_manifest_sha256="f" * 64,
        host_contract_sha256="1" * 64,
        dependency_lock_sha256="2" * 64,
    )
    envelope = _error_envelope(
        "MANIFEST_MISMATCH",
        bridge_version=reported.runtime_version,
        runtime_tag=reported.runtime_tag,
        runtime_asset_sha256=reported.runtime_asset_sha256,
        operation_manifest_sha256=reported.operation_manifest_sha256,
        release_id=reported.release_id,
    )
    cast(dict[str, object], envelope["error"])["mutation_not_started"] = _not_started_evidence(
        "unit.assign_mission"
    )

    response = parse_inst_response(_inst(envelope), expectation)

    assert response.settlement is None


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": "77777777-7777-4777-8777-777777777777"},
        {"request_hash": "e" * 64},
        {"operation": "unit.set"},
    ],
)
def test_not_started_evidence_identity_mismatch_is_protocol_error(
    changes: dict[str, object], expectation: ResponseExpectation
) -> None:
    envelope = _error_with_not_started("unit.assign_mission")
    evidence = cast(
        dict[str, object], cast(dict[str, object], envelope["error"])["mutation_not_started"]
    )
    evidence.update(changes)

    _assert_protocol_error(_inst(envelope), expectation, "evidence")


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("schema_version", True),
        ("schema_version", 1.0),
        ("mutation_barrier_written", 0),
        ("execute_started", True),
        ("stage", "execute"),
    ],
)
def test_malformed_not_started_evidence_is_protocol_error(
    field: str, wrong: object, expectation: ResponseExpectation
) -> None:
    envelope = _error_with_not_started("unit.assign_mission", **{field: wrong})

    _assert_protocol_error(_inst(envelope), expectation, "envelope")


def test_not_started_evidence_is_forbidden_on_read_status_and_cancel(
    expectation: ResponseExpectation,
    bootstrap_expectation: ResponseExpectation,
    cancel_expectation: ResponseExpectation,
) -> None:
    read_invocation = OPERATION_REGISTRY.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
    read_expectation = replace(expectation, invocation=read_invocation)
    read = _error_with_not_started("unit.get")
    status = _error_with_not_started("bridge.status")
    cancel = _error_with_not_started("unit.assign_mission")
    cancel["delivery_id"] = str(CANCEL_DELIVERY_ID)

    _assert_protocol_error(_inst(read), read_expectation, "evidence")
    _assert_protocol_error(_inst(status), bootstrap_expectation, "evidence")
    _assert_protocol_error(_inst(cancel), cancel_expectation, "evidence")


def test_reserved_evidence_key_in_details_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope("CMO_LUA_ERROR")
    details = cast(dict[str, object], cast(dict[str, object], envelope["error"])["details"])
    details["mutation_not_started"] = _not_started_evidence("unit.assign_mission")

    _assert_protocol_error(_inst(envelope), expectation, "envelope")


def _reconcile_expectation(
    expectation: ResponseExpectation,
    *,
    target_id: UUID,
    disposition: str = "applied",
) -> ResponseExpectation:
    invocation = OPERATION_REGISTRY.resolve_invocation(
        "bridge.reconcile",
        {"request_id": str(target_id), "disposition": disposition},
        {"confirmation_proof": "e" * 64},
    )
    return replace(expectation, invocation=invocation)


def _reconcile_result(target_id: UUID, disposition: str = "applied") -> dict[str, object]:
    return {
        "request_id": str(target_id),
        "request_hash": "f" * 64,
        "disposition": disposition,
        "resolved": True,
    }


def test_reconcile_evidence_identifies_outer_attempt_not_original_target(
    expectation: ResponseExpectation,
) -> None:
    target_id = UUID("77777777-7777-4777-8777-777777777777")
    current = _reconcile_expectation(expectation, target_id=target_id)
    exact = _error_with_not_started("bridge.reconcile")
    wrong = _error_with_not_started("bridge.reconcile", request_id=str(target_id))

    assert isinstance(
        parse_inst_response(_inst(exact), current).settlement,
        RejectedSettlement,
    )
    _assert_protocol_error(_inst(wrong), current, "evidence")


@pytest.mark.parametrize(
    ("wrong_field", "wrong_value"),
    [
        ("request_id", "88888888-8888-4888-8888-888888888888"),
        ("disposition", "not_applied"),
    ],
)
def test_reconcile_request_result_matches_frozen_target_and_disposition(
    wrong_field: str,
    wrong_value: object,
    expectation: ResponseExpectation,
) -> None:
    target_id = UUID("77777777-7777-4777-8777-777777777777")
    current = _reconcile_expectation(expectation, target_id=target_id)
    valid = _base_envelope()
    valid["result"] = _reconcile_result(target_id)

    response = parse_inst_response(_inst(valid), current)
    assert isinstance(response.settlement, CompletedSettlement)

    invalid = _base_envelope()
    invalid_result = _reconcile_result(target_id)
    invalid_result[wrong_field] = wrong_value
    invalid["result"] = invalid_result
    _assert_protocol_error(_inst(invalid), current, "reconcile")


def test_reconcile_request_result_requires_exact_true_resolved_flag(
    expectation: ResponseExpectation,
) -> None:
    target_id = UUID("77777777-7777-4777-8777-777777777777")
    current = _reconcile_expectation(expectation, target_id=target_id)
    envelope = _base_envelope()
    result = _reconcile_result(target_id)
    result["resolved"] = 1
    envelope["result"] = result

    _assert_protocol_error(_inst(envelope), current, "reconcile")


@pytest.mark.parametrize(
    ("wrong_field", "wrong_value"),
    [
        ("request_id", "88888888-8888-4888-8888-888888888888"),
        ("disposition", "not_applied"),
    ],
)
def test_completed_cancel_reconcile_result_matches_frozen_target_and_disposition(
    wrong_field: str,
    wrong_value: object,
    cancel_expectation: ResponseExpectation,
) -> None:
    target_id = UUID("77777777-7777-4777-8777-777777777777")
    current = _reconcile_expectation(cancel_expectation, target_id=target_id)
    acknowledgement: dict[str, object] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": _reconcile_result(target_id),
    }
    valid = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    valid["result"] = acknowledgement

    response = parse_inst_response(_inst(valid), current)
    assert isinstance(response.settlement, CompletedSettlement)
    assert response.cancel_ack is not None
    assert response.cancel_ack.result == response.settlement.result

    invalid = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    bad_acknowledgement = dict(acknowledgement)
    bad_result = _reconcile_result(target_id)
    bad_result[wrong_field] = wrong_value
    bad_acknowledgement["result"] = bad_result
    invalid["result"] = bad_acknowledgement
    _assert_protocol_error(_inst(invalid), current, "reconcile")


def test_completed_cancel_reconcile_requires_exact_true_resolved_flag(
    cancel_expectation: ResponseExpectation,
) -> None:
    target_id = UUID("77777777-7777-4777-8777-777777777777")
    current = _reconcile_expectation(cancel_expectation, target_id=target_id)
    result = _reconcile_result(target_id)
    result["resolved"] = 1
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": result,
    }

    _assert_protocol_error(_inst(envelope), current, "reconcile")


def test_cancel_success_builds_cancelled_or_completed_settlement(
    cancel_expectation: ResponseExpectation,
) -> None:
    cancelled_envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    cancelled_envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "cancelled",
        "result": None,
    }
    completed_envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    completed_envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": _base_envelope()["result"],
    }

    cancelled = parse_inst_response(_inst(cancelled_envelope), cancel_expectation)
    completed = parse_inst_response(_inst(completed_envelope), cancel_expectation)

    assert isinstance(cancelled.settlement, CancelledSettlement)
    assert isinstance(completed.settlement, CompletedSettlement)


def test_cancel_error_has_no_settlement(
    cancel_expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope("INDETERMINATE_OUTCOME", delivery_id=str(CANCEL_DELIVERY_ID))

    response = parse_inst_response(_inst(envelope), cancel_expectation)

    assert response.settlement is None


def test_lone_surrogate_in_read_or_unsafe_mutation_error_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    envelope = _error_envelope("CMO_LUA_ERROR")
    error = cast(dict[str, object], envelope["error"])
    error["details"] = {"value": "\ud800"}
    read_invocation = OPERATION_REGISTRY.resolve_invocation("unit.get", {"unit_guid": "UNIT-1"})
    read_expectation = replace(expectation, invocation=read_invocation)

    for current in (read_expectation, expectation):
        failure = _assert_protocol_error(_inst(envelope), current)
        json.dumps(failure.to_payload(), ensure_ascii=False, allow_nan=False)


def test_lone_surrogate_in_success_result_is_protocol_error(
    expectation: ResponseExpectation,
) -> None:
    adapter = cast(Adapter, TypeAdapter(StringResult))
    invocation = replace(
        expectation.invocation,
        result_schema=replace(expectation.invocation.result_schema, adapter=adapter),
    )
    current = replace(expectation, invocation=invocation)
    envelope = _base_envelope()
    envelope["result"] = {"text": "\ud800"}

    _assert_protocol_error(_inst(envelope), current)


def test_lone_surrogate_in_completed_cancel_result_is_protocol_error(
    cancel_expectation: ResponseExpectation,
) -> None:
    adapter = cast(Adapter, TypeAdapter(StringResult))
    recovery_schema = cancel_expectation.invocation.recovery_schema
    assert recovery_schema is not None
    invocation = replace(
        cancel_expectation.invocation,
        recovery_schema=replace(recovery_schema, adapter=adapter),
    )
    current = replace(cancel_expectation, invocation=invocation)
    envelope = _base_envelope(delivery_id=CANCEL_DELIVERY_ID)
    envelope["result"] = {
        "request_id": str(REQUEST_ID),
        "request_hash": REQUEST_HASH,
        "original_delivery_id": str(REQUEST_DELIVERY_ID),
        "status": "completed",
        "result": {"text": "\ud800"},
    }

    _assert_protocol_error(_inst(envelope), current)
