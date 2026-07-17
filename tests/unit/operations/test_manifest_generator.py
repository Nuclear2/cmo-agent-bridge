import copy
import importlib.util
import json
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import cast

import pytest
from pydantic import ValidationError

from cmo_agent_bridge.errors import BridgeError
from cmo_agent_bridge.operations import models
from cmo_agent_bridge.operations.kinds import ExecutionTarget
from cmo_agent_bridge.operations.registry import OPERATION_REGISTRY


ROOT = Path(__file__).parents[3]
SPEC = importlib.util.spec_from_file_location(
    "cmo_agent_bridge_generate_manifest", ROOT / "scripts" / "generate_manifest.py"
)
assert SPEC is not None and SPEC.loader is not None
generate_manifest: ModuleType = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(generate_manifest)


def test_schema_corpus_matches_registry_invocation_semantics() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    assert corpus["manifest_sha256"] == OPERATION_REGISTRY.manifest_sha256
    for case in corpus["cases"]:
        surface = case["surface"]
        if case["valid"]:
            invocation = OPERATION_REGISTRY.resolve_invocation(
                case["operation"], case["caller_arguments"], case["trusted_enrichment"]
            )
            assert invocation.wire_arguments.model_dump(mode="json") == case["wire_arguments"]
            contract = OPERATION_REGISTRY.resolve(case["operation"])
            if contract.target is ExecutionTarget.LOCAL:
                with pytest.raises(BridgeError):
                    OPERATION_REGISTRY.resolve_wire_invocation(
                        case["operation"], case["wire_arguments"]
                    )
            else:
                raw = OPERATION_REGISTRY.resolve_wire_invocation(
                    case["operation"], case["wire_arguments"]
                )
                assert raw.wire_arguments == invocation.wire_arguments
                assert raw.effective_class is invocation.effective_class
                assert raw.result_schema.schema_id == invocation.result_schema.schema_id
                assert (None if raw.recovery_schema is None else raw.recovery_schema.schema_id) == (
                    None
                    if invocation.recovery_schema is None
                    else invocation.recovery_schema.schema_id
                )
        else:
            if surface in {"registry-wire", "registry-public"}:
                with pytest.raises((ValidationError, BridgeError)):
                    OPERATION_REGISTRY.resolve_invocation(
                        case["operation"],
                        case["caller_arguments"],
                        case["trusted_enrichment"],
                    )
            if surface in {"registry-wire", "raw-wire"}:
                with pytest.raises((ValidationError, BridgeError)):
                    OPERATION_REGISTRY.resolve_wire_invocation(
                        case["operation"], case["wire_arguments"]
                    )


def test_schema_corpus_locks_reconcile_public_and_raw_null_boundaries() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    reconcile = {
        case["id"]: case for case in corpus["cases"] if case["operation"] == "bridge.reconcile"
    }
    expected = {
        "bridge.reconcile:valid-explicit-null-probe": ("registry-wire", True),
        "bridge.reconcile:invalid-public-caller-proof": ("registry-public", False),
        "bridge.reconcile:invalid-raw-explicit-null-all": ("raw-wire", False),
        "bridge.reconcile:invalid-raw-null-request": ("raw-wire", False),
        "bridge.reconcile:invalid-raw-null-disposition": ("raw-wire", False),
        "bridge.reconcile:invalid-raw-null-proof": ("raw-wire", False),
        "bridge.reconcile:invalid-raw-request-only": ("raw-wire", False),
        "bridge.reconcile:invalid-raw-proof-only": ("raw-wire", False),
    }
    assert {
        case_id: (reconcile[case_id]["surface"], reconcile[case_id]["valid"])
        for case_id in expected
        if case_id in reconcile
    } == expected


def test_schema_corpus_covers_every_named_invariant() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    covered = {
        case["invariant_id"]
        for case in corpus["cases"]
        if not case["valid"] and case.get("invariant_id") is not None
    }
    assert covered == generate_manifest.INVARIANT_IDS


def test_schema_corpus_covers_confirmed_reconcile_registry_wire_paths() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    confirmed = {
        case["id"]: case
        for case in corpus["cases"]
        if case["operation"] == "bridge.reconcile" and "confirmed" in case["id"]
    }
    expected_ids = {
        "bridge.reconcile:valid-confirmed-applied",
        "bridge.reconcile:valid-confirmed-not-applied",
        "bridge.reconcile:invalid-confirmed-missing-proof",
        "bridge.reconcile:invalid-confirmed-invalid-proof",
    }
    assert set(confirmed) == expected_ids

    request_id = "00000000-0000-0000-0000-000000000002"
    proof = "b" * 64
    for disposition in ("applied", "not_applied"):
        case = confirmed[f"bridge.reconcile:valid-confirmed-{disposition.replace('_', '-')}"]
        assert case == {
            "id": case["id"],
            "operation": "bridge.reconcile",
            "surface": "registry-wire",
            "valid": True,
            "invariant_id": None,
            "caller_arguments": {
                "request_id": request_id,
                "disposition": disposition,
            },
            "trusted_enrichment": {"confirmation_proof": proof},
            "wire_arguments": {
                "request_id": request_id,
                "disposition": disposition,
                "confirmation_proof": proof,
            },
        }

    missing = confirmed["bridge.reconcile:invalid-confirmed-missing-proof"]
    invalid = confirmed["bridge.reconcile:invalid-confirmed-invalid-proof"]
    assert missing["valid"] is False
    assert missing["surface"] == "registry-wire"
    assert missing["trusted_enrichment"] == {}
    assert invalid["valid"] is False
    assert invalid["surface"] == "registry-wire"
    assert invalid["trusted_enrichment"] == {"confirmation_proof": "g" * 64}


def test_list_ast_contains_the_exact_result_projection_allowlist() -> None:
    source = json.loads(generate_manifest.SOURCE_PATH.read_text(encoding="utf-8"))
    for entry in source:
        result_factory = entry["wire_result_factory"]
        if not result_factory.startswith("paged:"):
            continue
        result_name = result_factory.partition(":")[2]
        expected = list(getattr(models, result_name).model_fields)
        fields_ast = entry["arguments_ast"]["properties"]["fields"]
        assert fields_ast["items"]["enum"] == expected


def test_schema_corpus_preserves_new_location_zone_and_inherit_forms() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    cases = {case["id"]: case for case in corpus["cases"]}

    assert cases["unit.add:valid-2"]["wire_arguments"] == {
        "altitude": None,
        "base_guid": "BASE-1",
        "dbid": 1,
        "latitude": None,
        "loadout_dbid": None,
        "longitude": None,
        "name": "Based Alpha",
        "side_guid": "SIDE-1",
        "unit_type": "Aircraft",
    }
    mission_update = cases["mission.update:valid-2"]["wire_arguments"]
    assert mission_update["reference_point_guids"] == ["RP-3", "RP-1", "RP-2", "RP-1"]
    assert mission_update["prosecution_zone_reference_point_guids"] == []
    assert cases["doctrine.get:valid-2"]["wire_arguments"]["actual"] is True
    assert cases["doctrine.set:valid-2"]["wire_arguments"]["weapon_control_air"] == "inherit"
    assert (
        cases["doctrine.set:valid-3"]["wire_arguments"]["engage_opportunity_targets"] == "inherit"
    )
    assert cases["emcon.set:valid-2"]["wire_arguments"]["emcon"] == "Inherit"


def test_schema_corpus_covers_every_discriminated_valid_branch() -> None:
    corpus = json.loads((ROOT / "protocol" / "schema-corpus.json").read_text(encoding="utf-8"))
    valid = [case for case in corpus["cases"] if case["valid"]]
    mission_classes = {
        case["caller_arguments"]["details"]["mission_class"]
        for case in valid
        if case["operation"] == "mission.create"
    }
    probe_steps = {
        case["caller_arguments"]["step"]
        for case in valid
        if case["operation"] == "compat.probe.step"
    }
    uninstall_phases = {
        case["caller_arguments"]["phase"]
        for case in valid
        if case["operation"] == "bridge.uninstall"
    }
    compat_phases = {
        case["caller_arguments"]["phase"] for case in valid if case["operation"] == "compat.probe"
    }
    assert mission_classes == {
        "patrol",
        "support",
        "strike",
        "ferry",
        "mining",
        "mine_clearing",
        "cargo",
    }
    assert probe_steps == {
        "observational",
        "payload",
        "high-speed",
        "lineage",
        "key-value",
        "dedupe",
        "indeterminate",
        "ledger-capacity",
        "apply-profile",
    }
    assert uninstall_phases == {"command", "files"}
    assert compat_phases == {
        "automatic",
        "arm-paused-special-action",
        "collect-paused-special-action",
    }


def _replace_arguments_ref(source: list[dict[str, object]]) -> None:
    arguments_ast = cast(dict[str, object], source[0]["arguments_ast"])
    arguments_ast.update({"$ref": "#/bad"})


def _append_invariant(source: list[dict[str, object]]) -> None:
    invariant_ids = cast(list[str], source[0]["invariant_ids"])
    invariant_ids.append("execute_python")


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (_replace_arguments_ref, "unsupported"),
        (_append_invariant, "invariant"),
    ],
)
def test_generator_rejects_unsupported_schema_vocabulary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[list[dict[str, object]]], None],
    message: str,
) -> None:
    source = cast(
        list[dict[str, object]],
        json.loads(generate_manifest.SOURCE_PATH.read_text(encoding="utf-8")),
    )
    mutated = copy.deepcopy(source)
    assert callable(mutation)
    mutation(mutated)
    source_path = tmp_path / "operations.json"
    source_path.write_text(json.dumps(mutated), encoding="utf-8")
    monkeypatch.setattr(generate_manifest, "SOURCE_PATH", source_path)
    with pytest.raises(ValueError, match=message):
        generate_manifest.load_and_validate_source()
