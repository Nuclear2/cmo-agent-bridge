from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[2]
PLUGIN_ROOT = ROOT / "plugins" / "cmo-agent-bridge"
SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


def _load_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _project() -> dict[str, Any]:
    document = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return cast(dict[str, Any], document["project"])


def test_release_metadata_uses_one_version_and_public_identity() -> None:
    project = _project()
    version = cast(str, project["version"])
    assert SEMVER.fullmatch(version)
    assert project["readme"] == "README.md"
    assert project["requires-python"] == ">=3.12,<3.13"
    assert project["license"] == "MIT"
    assert project["license-files"] == ["LICENSE"]

    codex = _load_json(PLUGIN_ROOT / ".codex-plugin" / "plugin.json")
    claude = _load_json(PLUGIN_ROOT / ".claude-plugin" / "plugin.json")
    claude_marketplace = _load_json(ROOT / ".claude-plugin" / "marketplace.json")
    claude_entries = cast(list[dict[str, Any]], claude_marketplace["plugins"])

    assert codex["version"] == version
    assert claude["version"] == version
    assert claude_entries[0]["version"] == version
    for document in (codex, claude, claude_entries[0]):
        assert document["repository"] == "https://github.com/Nuclear2/cmo-agent-bridge"
        assert document["license"] == "MIT"


def test_codex_marketplace_has_installable_local_plugin() -> None:
    marketplace = _load_json(ROOT / ".agents" / "plugins" / "marketplace.json")
    entries = cast(list[dict[str, Any]], marketplace["plugins"])
    entry = entries[0]
    source = cast(dict[str, Any], entry["source"])
    policy = cast(dict[str, Any], entry["policy"])

    assert marketplace["name"] == "cmo-tools"
    assert len(entries) == 1
    assert entry["name"] == "cmo-agent-bridge"
    assert source == {
        "source": "local",
        "path": "./plugins/cmo-agent-bridge",
    }
    assert policy == {
        "installation": "AVAILABLE",
        "authentication": "ON_INSTALL",
    }
    assert entry["category"] == "Productivity"


def test_claude_marketplace_has_installable_local_plugin() -> None:
    marketplace = _load_json(ROOT / ".claude-plugin" / "marketplace.json")
    entries = cast(list[dict[str, Any]], marketplace["plugins"])
    entry = entries[0]

    assert marketplace["name"] == "cmo-tools"
    assert len(entries) == 1
    assert entry["name"] == "cmo-agent-bridge"
    assert entry["source"] == "./plugins/cmo-agent-bridge"


def test_framework_manifests_launch_one_pinned_release_server() -> None:
    version = cast(str, _project()["version"])
    wheel_url = (
        "https://github.com/Nuclear2/cmo-agent-bridge/releases/download/"
        f"v{version}/cmo_agent_bridge-{version}-py3-none-any.whl"
    )

    codex_config = _load_json(PLUGIN_ROOT / ".mcp.json")
    codex_servers = cast(dict[str, dict[str, Any]], codex_config["mcpServers"])
    codex_server = codex_servers["cmo-agent-bridge"]
    codex_args = cast(list[str], codex_server["args"])
    assert codex_server["command"] == "uvx"
    assert "cwd" not in codex_server
    assert codex_args[codex_args.index("--python") + 1] == "3.12"
    assert codex_args[codex_args.index("--from") + 1] == wheel_url
    assert codex_args[-2:] == ["cmo-bridge", "serve"]

    claude = _load_json(PLUGIN_ROOT / ".claude-plugin" / "plugin.json")
    codex = _load_json(PLUGIN_ROOT / ".codex-plugin" / "plugin.json")
    assert codex["mcpServers"] == "./.mcp.json"
    assert claude["mcpServers"] == "./.mcp.json"
    assert not list(PLUGIN_ROOT.rglob("*.whl"))


def test_packaged_skills_have_valid_minimal_frontmatter() -> None:
    skill_directories = sorted(path for path in (PLUGIN_ROOT / "skills").iterdir() if path.is_dir())
    assert skill_directories

    for skill_directory in skill_directories:
        skill_file = skill_directory / "SKILL.md"
        text = skill_file.read_text(encoding="utf-8")
        assert text.startswith("---\n")
        parts = text.split("---", 2)
        assert len(parts) == 3
        fields: dict[str, str] = {}
        for line in parts[1].splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.strip()] = value.strip().strip('"')
        assert fields["name"] == skill_directory.name
        assert fields["description"]
        assert parts[2].strip()


def test_distribution_metadata_never_embeds_a_development_worktree() -> None:
    metadata_paths = [
        ROOT / ".agents" / "plugins" / "marketplace.json",
        ROOT / ".claude-plugin" / "marketplace.json",
        PLUGIN_ROOT / ".codex-plugin" / "plugin.json",
        PLUGIN_ROOT / ".claude-plugin" / "plugin.json",
        PLUGIN_ROOT / ".mcp.json",
    ]
    combined = "\n".join(path.read_text(encoding="utf-8") for path in metadata_paths).lower()
    assert ".worktrees" not in combined
    assert "cmo-agent-bridge-implementation" not in combined


def test_repository_has_mit_license() -> None:
    license_text = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert license_text.startswith("MIT License\n")
    assert "Copyright (c) 2026 CMO Agent Bridge contributors" in license_text
