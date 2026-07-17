import hashlib
import hmac
import re
from uuid import UUID

from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import canonical_body_bytes
from cmo_agent_bridge.protocol.models import (
    PreparedDelivery,
    RequestBody,
    validate_body_runtime_snapshot,
)
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def lua_byte_string(raw: bytes) -> str:
    return '"' + "".join(f"\\{byte:03d}" for byte in raw) + '"'


def render_idle_lua() -> bytes:
    return b"CMO_AGENT_BRIDGE_DELIVERY = nil\nreturn true\n"


def _is_uuid(value: object) -> bool:
    return type(value) is UUID


def _is_bytes(value: object) -> bool:
    return type(value) is bytes


def _is_string(value: object) -> bool:
    return type(value) is str


def _validate_delivery(delivery: PreparedDelivery) -> PreparedDelivery:
    if type(delivery) is not PreparedDelivery:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "delivery must be a PreparedDelivery")
    if not _is_uuid(delivery.request_id) or not _is_uuid(delivery.delivery_id):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "delivery IDs must be UUIDs")
    if not _is_string(delivery.delivery_kind) or delivery.delivery_kind not in (
        "request",
        "cancel",
    ):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "invalid delivery kind")
    if not _is_string(delivery.request_hash) or _SHA256_RE.fullmatch(delivery.request_hash) is None:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "invalid request hash")
    if not _is_bytes(delivery.body_json):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "delivery body must be bytes")
    return PreparedDelivery(
        request_id=delivery.request_id,
        delivery_id=delivery.delivery_id,
        delivery_kind=delivery.delivery_kind,
        request_hash=delivery.request_hash,
        body_json=delivery.body_json,
    )


def _validate_runtime_snapshot(runtime_snapshot: object) -> RuntimeSnapshot:
    try:
        return revalidate_runtime_snapshot(runtime_snapshot)
    except (ValidationError, TypeError, ValueError) as error:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "invalid runtime snapshot") from error


def _validate_body_binding(delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot) -> None:
    actual_hash = hashlib.sha256(delivery.body_json).hexdigest()
    if not hmac.compare_digest(actual_hash, delivery.request_hash):
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "request hash does not match body bytes")
    try:
        body = RequestBody.model_validate_json(delivery.body_json)
    except (ValidationError, ValueError) as error:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "invalid request body") from error
    if canonical_body_bytes(body) != delivery.body_json:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, "request body is not canonical JSON")
    try:
        validate_body_runtime_snapshot(body, runtime_snapshot)
    except ValueError as error:
        raise BridgeError(ErrorCode.INVALID_ARGUMENT, str(error)) from error


def render_delivery_lua(delivery: PreparedDelivery, runtime_snapshot: RuntimeSnapshot) -> bytes:
    trusted_delivery = _validate_delivery(delivery)
    snapshot = _validate_runtime_snapshot(runtime_snapshot)
    _validate_body_binding(trusted_delivery, snapshot)
    rendered = (
        "CMO_AGENT_BRIDGE_DELIVERY = {\n"
        f'    request_id = "{trusted_delivery.request_id}",\n'
        f'    delivery_id = "{trusted_delivery.delivery_id}",\n'
        f'    delivery_kind = "{trusted_delivery.delivery_kind}",\n'
        f'    request_hash = "{trusted_delivery.request_hash}",\n'
        f'    runtime_version = "{snapshot.runtime_version}",\n'
        f'    runtime_tag = "{snapshot.runtime_tag}",\n'
        f'    runtime_asset_sha256 = "{snapshot.runtime_asset_sha256}",\n'
        f'    release_id = "{snapshot.release_id}",\n'
        f"    body_json = {lua_byte_string(trusted_delivery.body_json)},\n"
        "}\n"
        "local ok = pcall(ScenEdit_RunScript, "
        f'"CMOAgentBridge/versions/{snapshot.runtime_tag}/dispatcher.lua")\n'
        "CMO_AGENT_BRIDGE_DELIVERY = nil\n"
        "if not ok then\n"
        '    print("CMO_AGENT_BRIDGE|DISPATCH_FAILED")\n'
        "end\n"
        "return ok\n"
    )
    return rendered.encode("ascii")
