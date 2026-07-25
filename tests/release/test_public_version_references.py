from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any, cast


ROOT = Path(__file__).resolve().parents[2]
PLUGIN_ROOT = ROOT / "plugins" / "cmo-agent-bridge"
VERSION_REFERENCE = re.compile(
    r"(?:releases/(?:download|tag)/v|--branch\s+v)"
    r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)"
)
ARTIFACT_VERSION_REFERENCES = (
    re.compile(
        r"cmo_agent_bridge-"
        r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)"
        r"(?=-py3-none-any\.whl|\.tar\.gz)"
    ),
    re.compile(
        r"operate-cmo-skill-"
        r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)"
        r"(?=\.zip|[\"'])"
    ),
    re.compile(
        r"cmo-agent-bridge-plugin-"
        r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)"
        r"(?=\.zip|[\"'])"
    ),
)
INSTALLER_VERSION = re.compile(
    r"(?m)^\s*\[string\]\$Version\s*=\s*'"
    r"(?P<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)'"
)
PUBLIC_INSTALLATION_FILES = (
    ROOT / "README.md",
    ROOT / "docs" / "quickstart.md",
    ROOT / "docs" / "installation.md",
    ROOT / "docs" / "frameworks" / "README.md",
    ROOT / "docs" / "frameworks" / "codex.md",
    ROOT / "docs" / "frameworks" / "generic-mcp.md",
    PLUGIN_ROOT / "skills" / "operate-cmo" / "references" / "setup.md",
    PLUGIN_ROOT / ".mcp.json",
    PLUGIN_ROOT / ".claude-mcp.json",
)
PUBLIC_VERSIONED_ARTIFACT_FILES = {
    ROOT / "README.md",
    ROOT / "docs" / "quickstart.md",
    ROOT / "docs" / "installation.md",
    ROOT / "docs" / "frameworks" / "generic-mcp.md",
    PLUGIN_ROOT / "skills" / "operate-cmo" / "references" / "setup.md",
    PLUGIN_ROOT / ".mcp.json",
    PLUGIN_ROOT / ".claude-mcp.json",
}


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
        release_versions = {
            match.group("version") for match in VERSION_REFERENCE.finditer(text)
        }
        artifact_versions = {
            match.group("version")
            for pattern in ARTIFACT_VERSION_REFERENCES
            for match in pattern.finditer(text)
        }
        if path in PUBLIC_VERSIONED_ARTIFACT_FILES:
            assert artifact_versions, (
                f"No versioned release artifact found in {path.relative_to(ROOT)}"
            )
        assert release_versions, (
            f"No pinned public release reference found in {path.relative_to(ROOT)}"
        )
        observed = release_versions | artifact_versions
        assert observed == expected, (
            f"Stale public release or artifact reference in {path.relative_to(ROOT)}: "
            f"{sorted(observed)}"
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


def test_marketplace_installation_uses_stable_release_channel() -> None:
    codex_command = (
        "codex plugin marketplace add Nuclear2/cmo-agent-bridge --ref stable"
    )
    claude_command = "claude plugin marketplace add Nuclear2/cmo-agent-bridge@stable"
    codex_documents = (
        ROOT / "README.md",
        ROOT / "docs" / "installation.md",
        ROOT / "docs" / "frameworks" / "codex.md",
    )
    claude_documents = (
        ROOT / "README.md",
        ROOT / "docs" / "installation.md",
        ROOT / "docs" / "frameworks" / "claude-code.md",
    )

    for path in codex_documents:
        assert codex_command in path.read_text(encoding="utf-8")
    for path in claude_documents:
        assert claude_command in path.read_text(encoding="utf-8")

    marketplace_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (*codex_documents, *claude_documents)
    )
    assert not re.search(r"codex plugin marketplace add .*--ref v\d", marketplace_text)
    assert not re.search(r"claude plugin marketplace add .*@v\d", marketplace_text)


def test_release_workflow_advances_stable_after_publishing() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )
    publish = workflow.index("- name: Publish GitHub prerelease")
    advance = workflow.index("- name: Advance stable marketplace branch")

    assert publish < advance
    stable_step = workflow[advance:]
    assert 'git push origin "HEAD:refs/heads/stable"' in stable_step
    assert "git fetch origin stable" in stable_step
    assert "git merge-base --is-ancestor HEAD FETCH_HEAD" in stable_step
    assert "--force" not in stable_step
