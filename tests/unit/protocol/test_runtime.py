from typing import NotRequired, TypedDict, cast

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.protocol.runtime import (
    ProtocolVersion,
    RuntimeSnapshot,
    revalidate_runtime_snapshot,
)


RUNTIME_ASSET_SHA256 = "132178bbd19104e74cfab1d874c48223c9a0ef1b971d436450e192be5ab88982"
OPERATION_MANIFEST_SHA256 = "83c8f18fa0d6d92425d04573611447087b4d2a40915a569a4fba288350f45118"
HOST_CONTRACT_SHA256 = "3fbea240e4baee6d09b4d5701c8b48f6cea2eee03fdafe2166f9fddaf40c23fa"
DEPENDENCY_LOCK_SHA256 = "1c47be7a7dc1ff0f290c74aab25e42b01805e1d5eab90092be50cc3bcfa92bff"
RELEASE_ID = "6bce909f28eac5f8375abb35cb4dc5c1741f9de1dbb2a69692fbd4030907e63f"


class _RuntimeCreateArguments(TypedDict):
    runtime_version: str
    runtime_asset_sha256: str
    operation_manifest_sha256: str
    host_contract_sha256: str
    dependency_lock_sha256: str
    protocol: NotRequired[ProtocolVersion]


class InjectingStr(str):
    def __str__(self) -> str:
        return 'x"; print("INJECTED") --'

    def __format__(self, format_spec: str) -> str:
        return 'x"; print("INJECTED") --'


class InjectingSnapshot(RuntimeSnapshot):
    pass


def _snapshot(**changes: object) -> RuntimeSnapshot:
    values: dict[str, object] = {
        "runtime_version": "0.1.0",
        "runtime_asset_sha256": RUNTIME_ASSET_SHA256,
        "operation_manifest_sha256": OPERATION_MANIFEST_SHA256,
        "host_contract_sha256": HOST_CONTRACT_SHA256,
        "dependency_lock_sha256": DEPENDENCY_LOCK_SHA256,
    }
    values.update(changes)
    return RuntimeSnapshot.create(**cast(_RuntimeCreateArguments, values))


def test_runtime_snapshot_has_exact_frozen_identity_fields() -> None:
    assert tuple(RuntimeSnapshot.model_fields) == (
        "protocol",
        "runtime_version",
        "runtime_asset_sha256",
        "operation_manifest_sha256",
        "host_contract_sha256",
        "dependency_lock_sha256",
        "runtime_tag",
        "release_id",
    )

    snapshot = _snapshot()
    with pytest.raises(ValidationError):
        snapshot.runtime_version = "0.2.0"


def test_runtime_snapshot_matches_fixed_release_vector() -> None:
    snapshot = _snapshot()

    assert snapshot.protocol == "cmo-agent-bridge/1"
    assert snapshot.runtime_tag == f"0_1_0-{RUNTIME_ASSET_SHA256}"
    assert len(snapshot.runtime_tag) == len("0_1_0-") + 64
    assert snapshot.release_id == RELEASE_ID


def test_same_semver_with_different_assets_has_distinct_full_identities() -> None:
    first = _snapshot(runtime_asset_sha256="a" * 64)
    second = _snapshot(runtime_asset_sha256="b" * 64)

    assert first.runtime_tag == f"0_1_0-{'a' * 64}"
    assert second.runtime_tag == f"0_1_0-{'b' * 64}"
    assert first.runtime_tag != second.runtime_tag
    assert first.release_id != second.release_id


def test_runtime_version_changes_tag_and_release_id() -> None:
    first = _snapshot(runtime_version="0.1.0")
    second = _snapshot(runtime_version="0.2.0")

    assert first.runtime_tag != second.runtime_tag
    assert first.release_id != second.release_id


def test_operation_manifest_changes_release_but_not_runtime_tag() -> None:
    first = _snapshot(operation_manifest_sha256="a" * 64)
    second = _snapshot(operation_manifest_sha256="b" * 64)

    assert first.runtime_tag == second.runtime_tag
    assert first.release_id != second.release_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("host_contract_sha256", "c" * 64),
        ("dependency_lock_sha256", "d" * 64),
    ],
)
def test_host_or_raw_dependency_lock_digest_changes_release_id(field: str, value: str) -> None:
    assert _snapshot(**{field: value}).release_id != _snapshot().release_id


@pytest.mark.parametrize(
    "changes",
    [
        {"runtime_tag": f"0_1_0-{'f' * 64}"},
        {"release_id": "f" * 64},
    ],
)
def test_direct_construction_rejects_inconsistent_derived_fields(
    changes: dict[str, str],
) -> None:
    valid = _snapshot().model_dump()
    valid.update(changes)

    with pytest.raises(ValidationError):
        RuntimeSnapshot.model_validate(valid)


@pytest.mark.parametrize(
    "runtime_version",
    [
        "01.2.3",
        "1.02.3",
        "1.2.03",
        "1.2",
        "1.2.3.4",
        "1.2.3-alpha",
        "1.2.3+build",
        "../1.2.3",
        "1/2/3",
        "1\\2\\3",
        " 1.2.3",
        "1.2.3\n",
    ],
)
def test_runtime_snapshot_rejects_noncanonical_semver(runtime_version: str) -> None:
    with pytest.raises(ValidationError):
        _snapshot(runtime_version=runtime_version)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("runtime_asset_sha256", "A" * 64),
        ("runtime_asset_sha256", "a" * 63),
        ("operation_manifest_sha256", "not-a-hash"),
        ("host_contract_sha256", "0" * 65),
        ("dependency_lock_sha256", 123),
    ],
)
def test_runtime_snapshot_rejects_malformed_hashes(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        _snapshot(**{field: value})


def test_direct_construction_rejects_malformed_tag() -> None:
    valid = _snapshot().model_dump()
    valid["runtime_tag"] = "0_1_0/dispatcher"

    with pytest.raises(ValidationError):
        RuntimeSnapshot.model_validate(valid)


@pytest.mark.parametrize("field", tuple(RuntimeSnapshot.model_fields))
def test_snapshot_revalidation_never_preserves_string_subclasses(field: str) -> None:
    values = _snapshot().model_dump()
    values[field] = InjectingStr(values[field])
    untrusted = RuntimeSnapshot.model_construct(**values)

    normalized = revalidate_runtime_snapshot(untrusted)

    assert type(normalized) is RuntimeSnapshot
    assert all(type(getattr(normalized, name)) is str for name in RuntimeSnapshot.model_fields)


def test_snapshot_revalidation_rejects_model_subclass() -> None:
    untrusted = InjectingSnapshot.model_validate(_snapshot().model_dump())

    with pytest.raises(TypeError):
        revalidate_runtime_snapshot(untrusted)
