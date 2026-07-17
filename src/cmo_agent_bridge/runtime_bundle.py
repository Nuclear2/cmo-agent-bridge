from __future__ import annotations

import hashlib
from importlib.resources import files

from cmo_agent_bridge import __version__
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY
from cmo_agent_bridge.protocol.manifest import canonical_manifest_bytes
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot, revalidate_runtime_snapshot


_TEMPLATE_NAME = "dispatcher.lua.tmpl"
_HOST_CONTRACT_PREFIX = b"cmo-agent-bridge/host-contract/1\0"
_DEPENDENCY_CONTRACT = (
    b"python==3.12.*\nmcp>=1.28.1,<2\npydantic>=2.12,<3\npsutil>=7.2,<8\ntyper>=0.20,<1\n"
)
_PLACEHOLDERS = {
    b"@@PROTOCOL@@": "protocol",
    b"@@RUNTIME_VERSION@@": "runtime_version",
    b"@@RUNTIME_TAG@@": "runtime_tag",
    b"@@RUNTIME_ASSET_SHA256@@": "runtime_asset_sha256",
    b"@@RELEASE_ID@@": "release_id",
    b"@@OPERATION_MANIFEST_SHA256@@": "operation_manifest_sha256",
}


def dispatcher_template_bytes() -> bytes:
    return files("cmo_agent_bridge.runtime_assets").joinpath(_TEMPLATE_NAME).read_bytes()


def create_runtime_snapshot() -> RuntimeSnapshot:
    template = dispatcher_template_bytes()
    host_contract = _HOST_CONTRACT_PREFIX + canonical_manifest_bytes()
    return RuntimeSnapshot.create(
        runtime_version=__version__,
        runtime_asset_sha256=hashlib.sha256(template).hexdigest(),
        operation_manifest_sha256=OPERATION_REGISTRY.manifest_sha256,
        host_contract_sha256=hashlib.sha256(host_contract).hexdigest(),
        dependency_lock_sha256=hashlib.sha256(_DEPENDENCY_CONTRACT).hexdigest(),
    )


def render_dispatcher(snapshot: RuntimeSnapshot) -> bytes:
    validated = revalidate_runtime_snapshot(snapshot)
    rendered = dispatcher_template_bytes()
    for placeholder, field in _PLACEHOLDERS.items():
        value = getattr(validated, field)
        rendered = rendered.replace(placeholder, str(value).encode("ascii"))
    if b"@@" in rendered:
        raise RuntimeError("Lua dispatcher template contains an unresolved placeholder")
    return rendered
