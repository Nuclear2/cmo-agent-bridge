from __future__ import annotations

import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictInt

from cmo_agent_bridge.protocol.runtime import Sha256
from cmo_agent_bridge.state.models import PendingJournal


HOST_QUARANTINE_RESOLUTION_FORMAT = (
    "cmo-agent-bridge/host-quarantine-resolution/1"
)


class HostQuarantineResolutionMarker(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
    )

    format: Literal["cmo-agent-bridge/host-quarantine-resolution/1"]
    mode: Literal["host_only"]
    manual_evidence: Literal[True]
    root_key: Sha256
    required_release_id: Sha256
    request_id: UUID
    request_hash: Sha256
    original_journal_revision: StrictInt
    scenario_lineage_id: UUID
    original_activation_id: UUID
    disposition: Literal["applied", "not_applied"]
    resolved_at_ms: StrictInt


def canonical_host_quarantine_resolution(
    marker: HostQuarantineResolutionMarker,
) -> str:
    candidate = HostQuarantineResolutionMarker.model_validate(
        marker.model_dump(mode="python", round_trip=True, warnings=False)
    )
    return json.dumps(
        candidate.model_dump(mode="json", warnings="error"),
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def parse_host_quarantine_resolution(value: str) -> HostQuarantineResolutionMarker:
    if type(value) is not str:
        raise ValueError("host quarantine resolution must be an exact string")
    marker = HostQuarantineResolutionMarker.model_validate_json(value, strict=True)
    if canonical_host_quarantine_resolution(marker) != value:
        raise ValueError("host quarantine resolution is not canonical JSON")
    return marker


def marker_matches_pending_journal(
    marker: HostQuarantineResolutionMarker,
    journal: PendingJournal,
) -> bool:
    original = journal.original
    return (
        marker.root_key == journal.header.root_key
        and marker.required_release_id == journal.header.required_release_id
        and marker.request_id == original.request_id
        and marker.request_hash == original.request_hash
        and marker.original_journal_revision == original.revision
        and marker.scenario_lineage_id == original.expected_lineage_id
        and marker.original_activation_id == original.expected_activation_id
    )
