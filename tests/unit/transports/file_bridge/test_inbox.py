from dataclasses import replace
import inspect
from pathlib import Path
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.protocol.canonical import prepare_delivery
from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.models import PreparedDelivery, RequestBody
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.transports.file_bridge.inbox import InboxPublisher
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


def _paths(tmp_path: Path) -> FileBridgePaths:
    game_root = tmp_path / "CMO"
    game_root.mkdir()
    (game_root / "Command.exe").write_bytes(b"command")
    (game_root / "Lua").mkdir()
    (game_root / "ImportExport").mkdir()
    local_app_data = tmp_path / "LocalAppData"
    local_app_data.mkdir()
    paths = FileBridgePaths.build(game_root, local_app_data)
    paths.inbox.parent.mkdir(parents=True)
    return paths


def _delivery(snapshot: RuntimeSnapshot) -> PreparedDelivery:
    body = RequestBody(
        protocol=snapshot.protocol,
        release_id=snapshot.release_id,
        runtime_version=snapshot.runtime_version,
        runtime_tag=snapshot.runtime_tag,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        expected_lineage_id=None,
        expected_activation_id=None,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        operation="bridge.status",
        arguments={},
    )
    return prepare_delivery(
        body,
        request_id=UUID("550e8400-e29b-41d4-a716-446655440000"),
        delivery_id=UUID("91f9e65d-8560-42de-9233-e0559c8f2545"),
        delivery_kind="request",
    )


def _runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.1.0",
        runtime_asset_sha256="b" * 64,
        operation_manifest_sha256="c" * 64,
        host_contract_sha256="d" * 64,
        dependency_lock_sha256="e" * 64,
    )


def test_publisher_exposes_only_controlled_publish_methods() -> None:
    public_methods = {
        name
        for name, _member in inspect.getmembers(InboxPublisher, predicate=inspect.isfunction)
        if not name.startswith("_")
    }

    assert public_methods == {"publish_delivery", "publish_idle"}


def test_publish_delivery_writes_renderer_output_to_fixed_inbox(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    runtime_snapshot = _runtime_snapshot()
    delivery = _delivery(runtime_snapshot)
    publisher = InboxPublisher(paths, replace_retry_seconds=0)

    publisher.publish_delivery(delivery, runtime_snapshot=runtime_snapshot)

    assert paths.inbox.read_bytes() == render_delivery_lua(delivery, runtime_snapshot)
    assert set(paths.inbox.parent.iterdir()) == {paths.inbox}


def test_publish_idle_atomically_replaces_delivery(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    publisher = InboxPublisher(paths, replace_retry_seconds=0)
    snapshot = _runtime_snapshot()
    publisher.publish_delivery(_delivery(snapshot), runtime_snapshot=snapshot)

    publisher.publish_idle()

    assert paths.inbox.read_bytes() == render_idle_lua()
    assert set(paths.inbox.parent.iterdir()) == {paths.inbox}


def test_invalid_delivery_render_does_not_replace_existing_inbox(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.inbox.write_bytes(b"old")
    publisher = InboxPublisher(paths, replace_retry_seconds=0)
    snapshot = _runtime_snapshot()
    invalid_snapshot = RuntimeSnapshot.model_construct(
        protocol=snapshot.protocol,
        runtime_version=snapshot.runtime_version,
        runtime_asset_sha256=snapshot.runtime_asset_sha256,
        operation_manifest_sha256=snapshot.operation_manifest_sha256,
        host_contract_sha256=snapshot.host_contract_sha256,
        dependency_lock_sha256=snapshot.dependency_lock_sha256,
        runtime_tag="../dispatcher",
        release_id=snapshot.release_id,
    )

    with pytest.raises(BridgeError) as caught:
        publisher.publish_delivery(
            _delivery(_runtime_snapshot()), runtime_snapshot=invalid_snapshot
        )

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert paths.inbox.read_bytes() == b"old"


def test_body_hash_mismatch_does_not_replace_existing_inbox(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.inbox.write_bytes(b"old")
    snapshot = _runtime_snapshot()
    invalid = replace(_delivery(snapshot), request_hash="f" * 64)
    publisher = InboxPublisher(paths, replace_retry_seconds=0)

    with pytest.raises(BridgeError) as caught:
        publisher.publish_delivery(invalid, runtime_snapshot=snapshot)

    assert caught.value.code is ErrorCode.INVALID_ARGUMENT
    assert paths.inbox.read_bytes() == b"old"


def test_publisher_does_not_create_missing_inbox_parent(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.inbox.parent.rmdir()
    paths.lua_root.rmdir()
    publisher = InboxPublisher(paths, replace_retry_seconds=0)

    with pytest.raises(FileNotFoundError):
        publisher.publish_idle()

    assert not paths.lua_root.exists()
