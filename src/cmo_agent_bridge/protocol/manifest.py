import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.operations.generated_manifest import MANIFEST_SHA256, OPERATIONS
from cmo_agent_bridge.operations.registry import OperationRegistry
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot


def canonical_manifest_bytes(entries: Sequence[Mapping[str, object]] = OPERATIONS) -> bytes:
    return json.dumps(
        list(entries),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def operation_manifest_sha256(entries: Sequence[Mapping[str, object]] = OPERATIONS) -> str:
    return hashlib.sha256(canonical_manifest_bytes(entries)).hexdigest()


def verify_generated_manifest() -> None:
    actual = operation_manifest_sha256()
    if actual != MANIFEST_SHA256:
        raise RuntimeError(
            f"generated operation manifest hash mismatch: expected {MANIFEST_SHA256}, got {actual}"
        )


@dataclass(frozen=True, slots=True)
class ReleaseBinding:
    snapshot: RuntimeSnapshot
    registry: OperationRegistry


class ManifestCatalog:
    def __init__(self, running: ReleaseBinding) -> None:
        if type(running) is not ReleaseBinding:
            raise TypeError("running release binding must be exact")
        snapshot = revalidate_runtime_snapshot(running.snapshot)
        if type(running.registry) is not OperationRegistry:
            raise TypeError("running release registry must be exact")
        if running.registry.manifest_sha256 != snapshot.operation_manifest_sha256:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "running operation registry does not match the runtime snapshot",
                {
                    "release_id": snapshot.release_id,
                    "snapshot_manifest_sha256": snapshot.operation_manifest_sha256,
                    "registry_manifest_sha256": running.registry.manifest_sha256,
                },
            )
        self._running = ReleaseBinding(snapshot=snapshot, registry=running.registry)

    @property
    def running_release_id(self) -> str:
        return self._running.snapshot.release_id

    def resolve_running(self, required_release_id: str) -> ReleaseBinding:
        if type(required_release_id) is not str:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "required release ID must be an exact string",
            )
        if required_release_id != self.running_release_id:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "the pending journal requires a different bridge release",
                {
                    "required_release_id": required_release_id,
                    "running_release_id": self.running_release_id,
                },
            )
        try:
            snapshot = revalidate_runtime_snapshot(self._running.snapshot)
        except (TypeError, ValueError) as error:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "running release binding changed after catalog construction",
                {
                    "required_release_id": required_release_id,
                    "running_release_id": self.running_release_id,
                },
            ) from error
        if self._running.registry.manifest_sha256 != snapshot.operation_manifest_sha256:
            raise BridgeError(
                ErrorCode.MANIFEST_MISMATCH,
                "running release binding changed after catalog construction",
                {
                    "required_release_id": required_release_id,
                    "running_release_id": self.running_release_id,
                },
            )
        return ReleaseBinding(snapshot=snapshot, registry=self._running.registry)
