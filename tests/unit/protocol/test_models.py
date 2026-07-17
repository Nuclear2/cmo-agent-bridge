from dataclasses import FrozenInstanceError
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.models import ExchangeCommand, PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


def runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="a" * 64,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def request_body(**overrides: object) -> RequestBody:
    snapshot = runtime_snapshot()
    values: dict[str, object] = {
        "protocol": "cmo-agent-bridge/1",
        "release_id": snapshot.release_id,
        "runtime_version": snapshot.runtime_version,
        "runtime_tag": snapshot.runtime_tag,
        "runtime_asset_sha256": snapshot.runtime_asset_sha256,
        "expected_lineage_id": None,
        "expected_activation_id": None,
        "operation_manifest_sha256": snapshot.operation_manifest_sha256,
        "operation": "scenario.get",
        "arguments": {},
    }
    values.update(overrides)
    return RequestBody.model_validate(values)


def test_request_body_has_the_exact_wire_fields() -> None:
    assert tuple(RequestBody.model_fields) == (
        "protocol",
        "release_id",
        "runtime_version",
        "runtime_tag",
        "runtime_asset_sha256",
        "expected_lineage_id",
        "expected_activation_id",
        "operation_manifest_sha256",
        "operation",
        "arguments",
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protocol", "cmo-agent-bridge/2"),
        ("release_id", "A" * 64),
        ("runtime_version", "01.2.3"),
        ("runtime_asset_sha256", "b" * 63),
        ("operation_manifest_sha256", "A" * 64),
        ("operation_manifest_sha256", "a" * 63),
        ("arguments", {"bad": object()}),
    ],
)
def test_request_body_rejects_invalid_wire_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        request_body(**{field: value})


def test_request_body_rejects_unknown_fields_and_is_frozen() -> None:
    with pytest.raises(ValidationError):
        request_body(unknown=True)

    body = request_body(expected_lineage_id=str(uuid4()))
    assert isinstance(body.expected_lineage_id, UUID)
    with pytest.raises(ValidationError):
        body.operation = "unit.list"


def test_prepared_delivery_is_an_immutable_value_object() -> None:
    prepared = PreparedDelivery(
        request_id=uuid4(),
        delivery_id=uuid4(),
        delivery_kind="request",
        request_hash="b" * 64,
        body_json=b"{}",
    )
    with pytest.raises(FrozenInstanceError):
        setattr(prepared, "request_hash", "c" * 64)


def test_exchange_command_accepts_only_a_matching_runtime_snapshot() -> None:
    snapshot = runtime_snapshot()
    body = request_body()
    invocation = OPERATION_REGISTRY.resolve_invocation("scenario.get", {})

    command = ExchangeCommand(
        request_id=uuid4(),
        body=body,
        invocation=invocation,
        runtime_snapshot=snapshot,
        timeout=30,
    )

    assert command.runtime_snapshot is snapshot


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("protocol", "cmo-agent-bridge/2"),
        ("release_id", "e" * 64),
        ("runtime_version", "0.2.0"),
        ("runtime_tag", f"0_1_0-{'e' * 64}"),
        ("runtime_asset_sha256", "e" * 64),
        ("operation_manifest_sha256", "e" * 64),
    ],
)
def test_exchange_command_rejects_every_body_snapshot_mismatch(field: str, wrong: str) -> None:
    snapshot = runtime_snapshot()
    invocation = OPERATION_REGISTRY.resolve_invocation("scenario.get", {})
    body = request_body().model_copy(update={field: wrong})

    with pytest.raises(ValueError, match=field):
        ExchangeCommand(
            request_id=uuid4(),
            body=body,
            invocation=invocation,
            runtime_snapshot=snapshot,
            timeout=30,
        )
