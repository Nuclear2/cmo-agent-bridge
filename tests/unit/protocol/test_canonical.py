from uuid import uuid4

import pytest
from pydantic import JsonValue, ValidationError

from cmo_agent_bridge.protocol.canonical import (
    canonical_body_bytes,
    prepare_delivery,
    request_sha256,
)
from cmo_agent_bridge.protocol.models import RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


def runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="a" * 64,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def status_body(arguments: dict[str, JsonValue]) -> RequestBody:
    snapshot = runtime_snapshot()
    return RequestBody(
        protocol="cmo-agent-bridge/1",
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=None,
        expected_activation_id=None,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation="bridge.status",
        arguments=arguments,
    )


def test_canonical_bytes_ignore_mapping_order_and_are_utf8() -> None:
    assert canonical_body_bytes(status_body({"b": 2, "a": "桥"})) == (
        b'{"arguments":{"a":"\xe6\xa1\xa5","b":2},'
        b'"expected_activation_id":null,"expected_lineage_id":null,'
        b'"operation":"bridge.status","operation_manifest_sha256":"'
        + b"a" * 64
        + b'","protocol":"cmo-agent-bridge/1","release_id":"'
        + runtime_snapshot().release_id.encode()
        + b'","runtime_asset_sha256":"'
        + b"b" * 64
        + b'","runtime_tag":"0_1_0-'
        + b"b" * 64
        + b'","runtime_version":"0.1.0"}'
    )
    assert canonical_body_bytes(status_body({"b": 2, "a": 1})) == canonical_body_bytes(
        status_body({"a": 1, "b": 2})
    )


def test_delivery_ids_do_not_change_request_hash() -> None:
    body = status_body({"activation_candidate": str(uuid4())})
    first = prepare_delivery(body, request_id=uuid4(), delivery_id=uuid4(), delivery_kind="request")
    second = prepare_delivery(
        body, request_id=uuid4(), delivery_id=uuid4(), delivery_kind="request"
    )
    assert first.request_hash == second.request_hash == request_sha256(body)
    assert first.body_json == canonical_body_bytes(body)


def test_changing_any_argument_changes_request_hash() -> None:
    assert request_sha256(status_body({"value": 1})) != request_sha256(status_body({"value": 2}))


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("release_id", "e" * 64),
        ("runtime_version", "0.2.0"),
        ("runtime_tag", f"0_1_0-{'e' * 64}"),
        ("runtime_asset_sha256", "e" * 64),
        ("operation_manifest_sha256", "e" * 64),
    ],
)
def test_each_runtime_snapshot_wire_field_changes_request_hash(field: str, wrong: str) -> None:
    body = status_body({"value": 1})
    changed = body.model_copy(update={field: wrong})

    assert request_sha256(changed) != request_sha256(body)


def test_non_finite_number_is_rejected() -> None:
    with pytest.raises((ValidationError, ValueError)):
        canonical_body_bytes(status_body({"value": float("nan")}))
