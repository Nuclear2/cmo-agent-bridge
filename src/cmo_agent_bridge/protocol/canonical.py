import hashlib
import json
from typing import Literal
from uuid import UUID

from cmo_agent_bridge.protocol.models import PreparedDelivery, RequestBody


def canonical_body_bytes(body: RequestBody) -> bytes:
    value = body.model_dump(mode="json", exclude_none=False)
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def request_sha256(body: RequestBody) -> str:
    return hashlib.sha256(canonical_body_bytes(body)).hexdigest()


def prepare_delivery(
    body: RequestBody,
    *,
    request_id: UUID,
    delivery_id: UUID,
    delivery_kind: Literal["request", "cancel"],
) -> PreparedDelivery:
    if delivery_kind not in ("request", "cancel"):
        raise ValueError(f"unsupported delivery kind: {delivery_kind}")
    body_json = canonical_body_bytes(body)
    return PreparedDelivery(
        request_id=request_id,
        delivery_id=delivery_id,
        delivery_kind=delivery_kind,
        request_hash=hashlib.sha256(body_json).hexdigest(),
        body_json=body_json,
    )
