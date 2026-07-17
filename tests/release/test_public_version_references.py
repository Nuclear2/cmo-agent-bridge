from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[2]
PLUGIN_ROOT = ROOT / "plugins" / "cmo-agent-bridge"
VERSION_REFERENCE = re.compile(
    r"(?:releases/(?:download|tag)/v|--(?:branch|ref)\s+v|@v)"
    r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)"
)
INSTALLER_VERSION = re.compile(
    r"(?m)^\s*\[string\]\$Version\s*=\s*'"
    r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)'"
)
PUBLIC_INSTALLATION_FILES = (
    ROOT / "README.md",
    ROOT / "docs" / "quickstart.md",
    ROOT / "docs" / "installation.md",
    ROOT / "docs" / "frameworks" / "codex.md",
    ROOT / "docs" / "frameworks" / "claude-code.md",
    ROOT / "docs" / "frameworks" / "generic-mcp.md",
    PLUGIN_ROOT / "skills" / "operate-cmo" / "references" / "setup.md",
    PLUGIN_ROOT / ".mcp.json",
    PLUGIN_ROOT / ".codex-mcp.json",
)


def _load_json(path: Path) -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def _project_version() -> str:
    document = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = cast(dict[str, Any], document["project"])
    return cast(str, project["version"])


def test_public_installation_references_use_current_version() -> None:
    expected = {_project_version()}

    for path in PUBLIC_INSTALLATION_FILES:
        text = path.read_text(encoding="utf-8")
        versions = {match.group("version") for match in VERSION_REFERENCE.finditer(text)}
        assert versions, f"No pinned public release reference found in {path.relative_to(ROOT)}"
        assert versions == expected, (
            f"Stale public release reference in {path.relative_to(ROOT)}: {sorted(versions)}"
        )


def test_public_plugin_metadata_uses_current_version() -> None:
    version = _project_version()
    codex = _load_json(PLUGIN_ROOT / ".codex-plugin" / "plugin.json")
    claude = _load_json(PLUGIN_ROOT / ".claude-plugin" / "plugin.json")
    marketplace = _load_json(ROOT / ".claude-plugin" / "marketplace.json")
    entries = cast(list[dict[str, Any]], marketplace["plugins"])

    assert codex["version"] == version
    assert claude["version"] == version
    assert entries[0]["version"] == version


def test_codex_desktop_installer_defaults_to_current_version() -> None:
    installer = (ROOT / "scripts" / "install-codex-desktop.ps1").read_text(encoding="utf-8")
    match = INSTALLER_VERSION.search(installer)

    assert match is not None
    assert match.group("version") == _project_version()
