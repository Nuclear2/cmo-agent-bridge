from __future__ import annotations

import hashlib
import json
import re
from typing import Annotated, Literal, cast

from pydantic import BaseModel, ConfigDict, StringConstraints, TypeAdapter, model_validator
from typing_extensions import Self


ProtocolVersion = Literal["cmo-agent-bridge/1"]
RuntimeVersion = Annotated[
    str,
    StringConstraints(pattern=r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$"),
]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_PROTOCOL_ADAPTER: TypeAdapter[ProtocolVersion] = TypeAdapter(
    ProtocolVersion, config=ConfigDict(strict=True)
)
_RUNTIME_VERSION_ADAPTER: TypeAdapter[RuntimeVersion] = TypeAdapter(
    RuntimeVersion, config=ConfigDict(strict=True)
)
_SHA256_ADAPTER: TypeAdapter[Sha256] = TypeAdapter(Sha256, config=ConfigDict(strict=True))
_RUNTIME_VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def derive_runtime_tag(runtime_version: object, runtime_asset_sha256: object) -> str:
    if (
        not isinstance(runtime_version, str)
        or _RUNTIME_VERSION_RE.fullmatch(runtime_version) is None
    ):
        raise ValueError("runtime version must be strict dotted MAJOR.MINOR.PATCH")
    if (
        not isinstance(runtime_asset_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", runtime_asset_sha256) is None
    ):
        raise ValueError("runtime asset digest must be lowercase SHA-256")
    return f"{runtime_version.replace('.', '_')}-{runtime_asset_sha256}"


def _release_payload(
    *,
    protocol: str,
    runtime_version: str,
    runtime_asset_sha256: str,
    operation_manifest_sha256: str,
    host_contract_sha256: str,
    dependency_lock_sha256: str,
) -> dict[str, str]:
    return {
        "dependency_lock_sha256": dependency_lock_sha256,
        "format": "cmo-agent-bridge/release/1",
        "host_contract_sha256": host_contract_sha256,
        "operation_manifest_sha256": operation_manifest_sha256,
        "protocol": protocol,
        "runtime_asset_sha256": runtime_asset_sha256,
        "runtime_version": runtime_version,
    }


def derive_release_id(
    *,
    protocol: str,
    runtime_version: str,
    runtime_asset_sha256: str,
    operation_manifest_sha256: str,
    host_contract_sha256: str,
    dependency_lock_sha256: str,
) -> str:
    payload = _release_payload(
        protocol=protocol,
        runtime_version=runtime_version,
        runtime_asset_sha256=runtime_asset_sha256,
        operation_manifest_sha256=operation_manifest_sha256,
        host_contract_sha256=host_contract_sha256,
        dependency_lock_sha256=dependency_lock_sha256,
    )
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class RuntimeSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    protocol: ProtocolVersion
    runtime_version: RuntimeVersion
    runtime_asset_sha256: Sha256
    operation_manifest_sha256: Sha256
    host_contract_sha256: Sha256
    dependency_lock_sha256: Sha256
    runtime_tag: str
    release_id: Sha256

    @classmethod
    def create(
        cls,
        *,
        runtime_version: str,
        runtime_asset_sha256: str,
        operation_manifest_sha256: str,
        host_contract_sha256: str,
        dependency_lock_sha256: str,
        protocol: ProtocolVersion = "cmo-agent-bridge/1",
    ) -> Self:
        validated_protocol = _PROTOCOL_ADAPTER.validate_python(protocol)
        validated_version = _RUNTIME_VERSION_ADAPTER.validate_python(runtime_version)
        validated_asset = _SHA256_ADAPTER.validate_python(runtime_asset_sha256)
        validated_manifest = _SHA256_ADAPTER.validate_python(operation_manifest_sha256)
        validated_host = _SHA256_ADAPTER.validate_python(host_contract_sha256)
        validated_lock = _SHA256_ADAPTER.validate_python(dependency_lock_sha256)
        tag = derive_runtime_tag(validated_version, validated_asset)
        release_id = derive_release_id(
            protocol=validated_protocol,
            runtime_version=validated_version,
            runtime_asset_sha256=validated_asset,
            operation_manifest_sha256=validated_manifest,
            host_contract_sha256=validated_host,
            dependency_lock_sha256=validated_lock,
        )
        return cls(
            protocol=validated_protocol,
            runtime_version=validated_version,
            runtime_asset_sha256=validated_asset,
            operation_manifest_sha256=validated_manifest,
            host_contract_sha256=validated_host,
            dependency_lock_sha256=validated_lock,
            runtime_tag=tag,
            release_id=release_id,
        )

    @model_validator(mode="after")
    def validate_derived_identity(self) -> Self:
        expected_tag = derive_runtime_tag(self.runtime_version, self.runtime_asset_sha256)
        if self.runtime_tag != expected_tag:
            raise ValueError("runtime_tag does not match runtime version and asset digest")
        expected_release_id = derive_release_id(
            protocol=self.protocol,
            runtime_version=self.runtime_version,
            runtime_asset_sha256=self.runtime_asset_sha256,
            operation_manifest_sha256=self.operation_manifest_sha256,
            host_contract_sha256=self.host_contract_sha256,
            dependency_lock_sha256=self.dependency_lock_sha256,
        )
        if self.release_id != expected_release_id:
            raise ValueError("release_id does not match the canonical release identity")
        return self


def _is_exact_snapshot(value: object) -> bool:
    return type(value) is RuntimeSnapshot


def _is_exact_string(value: object) -> bool:
    return type(value) is str


def revalidate_runtime_snapshot(value: object) -> RuntimeSnapshot:
    if not _is_exact_snapshot(value):
        raise TypeError("runtime snapshot must be a RuntimeSnapshot")
    trusted = cast(RuntimeSnapshot, value)
    snapshot = RuntimeSnapshot.model_validate(trusted.model_dump(mode="python"))
    if not _is_exact_snapshot(snapshot) or any(
        not _is_exact_string(getattr(snapshot, field)) for field in RuntimeSnapshot.model_fields
    ):
        raise TypeError("runtime snapshot must contain exact built-in strings")
    return snapshot
