from __future__ import annotations

from dataclasses import dataclass

from cmo_agent_bridge.protocol.lua_delivery import render_delivery_lua, render_idle_lua
from cmo_agent_bridge.protocol.models import PreparedDelivery
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.transports.file_bridge.atomic_io import atomic_replace_bytes
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths


@dataclass(frozen=True, slots=True)
class InboxPublisher:
    paths: FileBridgePaths
    replace_retry_seconds: float

    def publish_delivery(
        self,
        delivery: PreparedDelivery,
        *,
        runtime_snapshot: RuntimeSnapshot,
    ) -> None:
        rendered = render_delivery_lua(delivery, runtime_snapshot)
        self._publish_bytes(rendered)

    def publish_idle(self) -> None:
        self._publish_bytes(render_idle_lua())

    def _publish_bytes(self, data: bytes) -> None:
        atomic_replace_bytes(
            self.paths.inbox,
            data,
            retry_seconds=self.replace_retry_seconds,
        )
