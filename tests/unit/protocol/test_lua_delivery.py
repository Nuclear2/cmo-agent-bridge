import hashlib
import inspect
import json
from collections.abc import Iterator
from dataclasses import replace
from typing import cast
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import (
    lua_byte_string,
    render_delivery_lua,
    render_idle_lua,
)
from cmo_agent_bridge.protocol.models import DeliveryKind, PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot


class InjectingStr(str):
    def __str__(self) -> str:
        return 'x"; print("INJECTED") --'

    def __format__(self, format_spec: str) -> str:
        return 'x"; print("INJECTED") --'


class InjectingUuid(UUID):
    def __str__(self) -> str:
        return 'x"; print("INJECTED") --'

    def __format__(self, format_spec: str) -> str:
        return 'x"; print("INJECTED") --'


class InjectingBytes(bytes):
    def __iter__(self) -> Iterator[int]:
        return iter(b'"; print("INJECTED") --')


def _body(runtime_snapshot: RuntimeSnapshot) -> RequestBody:
    return RequestBody(
        protocol=runtime_snapshot.protocol,
        release_id=runtime_snapshot.release_id,
        runtime_version=runtime_snapshot.runtime_version,
        runtime_tag=runtime_snapshot.runtime_tag,
        runtime_asset_sha256=runtime_snapshot.runtime_asset_sha256,
        expected_lineage_id=None,
        expected_activation_id=None,
        operation_manifest_sha256=runtime_snapshot.operation_manifest_sha256,
        operation="bridge.status",
        arguments={"text": '桥"\\\r\n\u0000'},
    )


@pytest.fixture
def prepared_delivery(runtime_snapshot: RuntimeSnapshot) -> PreparedDelivery:
    return prepare_delivery(
        _body(runtime_snapshot),
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        delivery_id=UUID("22222222-2222-4222-8222-222222222222"),
        delivery_kind="request",
    )


@pytest.fixture
def runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="c" * 64,
        host_contract_sha256="d" * 64,
        dependency_lock_sha256="e" * 64,
    )


def test_body_is_emitted_as_three_digit_byte_escapes(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    rendered = render_delivery_lua(prepared_delivery, runtime_snapshot)

    assert b'body_json = "\\123\\034' in rendered
    assert "\u6865".encode() not in rendered
    assert (
        f"CMOAgentBridge/versions/{runtime_snapshot.runtime_tag}/dispatcher.lua".encode()
        in rendered
    )


def test_byte_escape_is_exact_and_total() -> None:
    assert lua_byte_string(bytes([0, 10, 13, 34, 92, 127, 255])) == (
        '"\\000\\010\\013\\034\\092\\127\\255"'
    )


def test_idle_lua_is_exact() -> None:
    assert render_idle_lua() == b"CMO_AGENT_BRIDGE_DELIVERY = nil\nreturn true\n"


def test_renderer_rejects_untrusted_delivery_metadata(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    invalid_hash = replace(
        prepared_delivery,
        request_hash='"; print("INJECTED") --',
    )
    non_string_hash = replace(
        prepared_delivery,
        request_hash=cast(str, 123),
    )
    invalid_kind = replace(
        prepared_delivery,
        delivery_kind=cast(DeliveryKind, 'request"; print("INJECTED") --'),
    )
    invalid_request_id = replace(
        prepared_delivery,
        request_id=cast(UUID, '"; print("INJECTED") --'),
    )
    invalid_body = replace(
        prepared_delivery,
        body_json=cast(bytes, "not bytes"),
    )

    for invalid in (
        invalid_hash,
        non_string_hash,
        invalid_kind,
        invalid_request_id,
        invalid_body,
    ):
        with pytest.raises(BridgeError) as caught:
            render_delivery_lua(invalid, runtime_snapshot)
        assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    "field",
    ["request_id", "delivery_id", "delivery_kind", "request_hash", "body_json"],
)
def test_renderer_rejects_scalar_subclasses_before_formatting(
    field: str,
    prepared_delivery: PreparedDelivery,
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    original = getattr(prepared_delivery, field)
    if field in {"request_id", "delivery_id"}:
        malicious = InjectingUuid(hex=cast(UUID, original).hex)
    elif field == "body_json":
        malicious = InjectingBytes(cast(bytes, original))
    else:
        malicious = InjectingStr(cast(str, original))
    untrusted = replace(prepared_delivery, **{field: malicious})

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(untrusted, runtime_snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_renderer_has_fixed_dispatch_and_cleanup_flow(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    rendered = render_delivery_lua(prepared_delivery, runtime_snapshot)

    assert f'request_id = "{prepared_delivery.request_id}"'.encode() in rendered
    assert f'delivery_id = "{prepared_delivery.delivery_id}"'.encode() in rendered
    assert b'delivery_kind = "request"' in rendered
    assert f'request_hash = "{prepared_delivery.request_hash}"'.encode() in rendered
    assert f'runtime_version = "{runtime_snapshot.runtime_version}"'.encode() in rendered
    assert f'runtime_tag = "{runtime_snapshot.runtime_tag}"'.encode() in rendered
    assert f'runtime_asset_sha256 = "{runtime_snapshot.runtime_asset_sha256}"'.encode() in rendered
    assert f'release_id = "{runtime_snapshot.release_id}"'.encode() in rendered
    assert rendered.count(b"ScenEdit_RunScript") == 1
    assert rendered.count(b"CMO_AGENT_BRIDGE|DISPATCH_FAILED") == 1
    assert rendered.index(b"CMO_AGENT_BRIDGE_DELIVERY = nil", 1) > rendered.index(
        b"ScenEdit_RunScript"
    )


def test_renderer_accepts_only_an_immutable_snapshot_api() -> None:
    assert tuple(inspect.signature(render_delivery_lua).parameters) == (
        "delivery",
        "runtime_snapshot",
    )


def test_renderer_revalidates_snapshot_before_interpolation(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    untrusted = RuntimeSnapshot.model_construct(
        protocol=runtime_snapshot.protocol,
        runtime_version=runtime_snapshot.runtime_version,
        runtime_asset_sha256=runtime_snapshot.runtime_asset_sha256,
        operation_manifest_sha256=runtime_snapshot.operation_manifest_sha256,
        host_contract_sha256=runtime_snapshot.host_contract_sha256,
        dependency_lock_sha256=runtime_snapshot.dependency_lock_sha256,
        runtime_tag='../0_1_0"; print("INJECTED") --',
        release_id=runtime_snapshot.release_id,
    )

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(prepared_delivery, untrusted)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_renderer_rejects_body_hash_mismatch(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    mismatched = replace(prepared_delivery, request_hash="f" * 64)

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(mismatched, runtime_snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize("body_json", [b"{broken", b'{"unknown":true}'])
def test_renderer_rejects_invalid_request_body(
    body_json: bytes,
    prepared_delivery: PreparedDelivery,
    runtime_snapshot: RuntimeSnapshot,
) -> None:
    invalid = replace(
        prepared_delivery,
        body_json=body_json,
        request_hash=hashlib.sha256(body_json).hexdigest(),
    )

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(invalid, runtime_snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


def test_renderer_rejects_noncanonical_request_body(
    prepared_delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot
) -> None:
    decoded = json.loads(prepared_delivery.body_json)
    noncanonical = json.dumps(decoded, ensure_ascii=False, indent=2).encode()
    invalid = replace(
        prepared_delivery,
        body_json=noncanonical,
        request_hash=hashlib.sha256(noncanonical).hexdigest(),
    )

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(invalid, runtime_snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT


@pytest.mark.parametrize(
    ("field", "wrong"),
    [
        ("protocol", "cmo-agent-bridge/2"),
        ("release_id", "f" * 64),
        ("runtime_version", "0.2.0"),
        ("runtime_tag", f"0_1_0-{'f' * 64}"),
        ("runtime_asset_sha256", "f" * 64),
        ("operation_manifest_sha256", "f" * 64),
    ],
)
def test_renderer_rejects_every_body_snapshot_mismatch(
    field: str, wrong: str, runtime_snapshot: RuntimeSnapshot
) -> None:
    mismatched_body = _body(runtime_snapshot).model_copy(update={field: wrong})
    delivery = prepare_delivery(
        mismatched_body,
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        delivery_id=UUID("22222222-2222-4222-8222-222222222222"),
        delivery_kind="request",
    )

    with pytest.raises(BridgeError) as caught:
        render_delivery_lua(delivery, runtime_snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
