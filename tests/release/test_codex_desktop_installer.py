from __future__ import annotations

import json
import shutil
import subprocess
import tomllib
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "scripts" / "install-codex-desktop.ps1"
VERSION = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]


def _powershell() -> str:
    executable = shutil.which("powershell") or shutil.which("pwsh")
    if executable is None:
        pytest.skip("PowerShell is required to test the Windows Desktop installer")
    return executable


def _write_bundle(
    path: Path,
    *,
    version: str = VERSION,
    marker: str = "first",
    include_codex_mcp: bool = True,
) -> Path:
    files = {
        "plugins/cmo-agent-bridge/.codex-plugin/plugin.json": json.dumps(
            {
                "name": "cmo-agent-bridge",
                "version": version,
                "mcpServers": "./.mcp.json",
                "skills": "./skills/",
            }
        ),
        "plugins/cmo-agent-bridge/skills/operate-cmo/SKILL.md": (
            "---\nname: operate-cmo\ndescription: Test skill.\n---\n\n# Test skill\n"
        ),
        "plugins/cmo-agent-bridge/content-marker.txt": marker,
    }
    if include_codex_mcp:
        files["plugins/cmo-agent-bridge/.mcp.json"] = json.dumps(
            {
                "mcpServers": {
                    "cmo-agent-bridge": {
                        "command": "uvx",
                        "args": ["cmo-bridge", "serve"],
                    }
                }
            }
        )

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for archive_path, content in files.items():
            archive.writestr(archive_path, content)
    return path


def _run_installer(bundle: Path, user_home: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            _powershell(),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(INSTALLER),
            "-Version",
            VERSION,
            "-BundlePath",
            str(bundle),
            "-UserHome",
            str(user_home),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
    )


def _marketplace_path(user_home: Path) -> Path:
    return user_home / ".agents" / "plugins" / "marketplace.json"


def _plugin_path(user_home: Path) -> Path:
    return user_home / ".codex" / "plugins" / "cmo-agent-bridge"


def test_installer_creates_desktop_plugin_and_personal_marketplace(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "plugin.zip")
    user_home = tmp_path / "home"

    result = _run_installer(bundle, user_home)

    assert result.returncode == 0, result.stderr
    plugin = _plugin_path(user_home)
    assert (plugin / ".codex-plugin" / "plugin.json").is_file()
    assert (plugin / ".mcp.json").is_file()
    assert (plugin / "skills" / "operate-cmo" / "SKILL.md").is_file()
    marketplace = json.loads(_marketplace_path(user_home).read_text(encoding="utf-8-sig"))
    assert marketplace["name"] == "personal"
    assert marketplace["interface"] == {"displayName": "Personal"}
    assert marketplace["plugins"] == [
        {
            "name": "cmo-agent-bridge",
            "source": {
                "source": "local",
                "path": "./.codex/plugins/cmo-agent-bridge",
            },
            "policy": {
                "installation": "AVAILABLE",
                "authentication": "ON_INSTALL",
            },
            "category": "Productivity",
        }
    ]
    assert "Restart ChatGPT Desktop" in result.stdout
    assert "does not install uv" in result.stdout


def test_installer_preserves_unrelated_marketplace_data(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "plugin.zip")
    user_home = tmp_path / "home"
    marketplace_path = _marketplace_path(user_home)
    marketplace_path.parent.mkdir(parents=True)
    marketplace_path.write_text(
        json.dumps(
            {
                "name": "my-marketplace",
                "interface": {"displayName": "个人插件", "accent": "blue"},
                "customTopLevel": {"keep": True, "label": "café—测试"},
                "plugins": [
                    {
                        "name": "other-plugin",
                        "source": {"source": "local", "path": "./other"},
                        "custom": 7,
                    },
                    {
                        "name": "cmo-agent-bridge",
                        "source": {"source": "local", "path": "./obsolete"},
                        "obsolete": True,
                    },
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = _run_installer(bundle, user_home)
    second_result = _run_installer(bundle, user_home)

    assert result.returncode == 0, result.stderr
    assert second_result.returncode == 0, second_result.stderr
    marketplace = json.loads(marketplace_path.read_text(encoding="utf-8-sig"))
    assert marketplace["name"] == "my-marketplace"
    assert marketplace["interface"] == {"displayName": "个人插件", "accent": "blue"}
    assert marketplace["customTopLevel"] == {"keep": True, "label": "café—测试"}
    assert marketplace["plugins"][0] == {
        "name": "other-plugin",
        "source": {"source": "local", "path": "./other"},
        "custom": 7,
    }
    matching = [
        plugin for plugin in marketplace["plugins"] if plugin.get("name") == "cmo-agent-bridge"
    ]
    assert len(matching) == 1
    assert matching[0]["source"]["path"] == "./.codex/plugins/cmo-agent-bridge"
    assert "obsolete" not in matching[0]


def test_installer_is_idempotent_and_replaces_only_its_managed_plugin(tmp_path: Path) -> None:
    first_bundle = _write_bundle(tmp_path / "first.zip", marker="first")
    second_bundle = _write_bundle(tmp_path / "second.zip", marker="second")
    user_home = tmp_path / "home"
    unrelated = user_home / ".codex" / "plugins" / "unrelated" / "keep.txt"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text("keep", encoding="utf-8")

    first_result = _run_installer(first_bundle, user_home)
    second_result = _run_installer(second_bundle, user_home)

    assert first_result.returncode == 0, first_result.stderr
    assert second_result.returncode == 0, second_result.stderr
    assert (_plugin_path(user_home) / "content-marker.txt").read_text() == "second"
    assert unrelated.read_text() == "keep"
    transaction_artifacts = list((user_home / ".codex" / "plugins").glob(".cmo-agent-bridge.*"))
    assert transaction_artifacts == []
    marketplace = json.loads(_marketplace_path(user_home).read_text(encoding="utf-8-sig"))
    assert sum(plugin.get("name") == "cmo-agent-bridge" for plugin in marketplace["plugins"]) == 1


def test_invalid_bundle_does_not_modify_existing_installation(tmp_path: Path) -> None:
    invalid_bundle = _write_bundle(
        tmp_path / "invalid.zip",
        marker="invalid",
        include_codex_mcp=False,
    )
    user_home = tmp_path / "home"
    existing_plugin = _plugin_path(user_home)
    existing_plugin.mkdir(parents=True)
    original_plugin = existing_plugin / "original.txt"
    original_plugin.write_text("untouched", encoding="utf-8")
    marketplace_path = _marketplace_path(user_home)
    marketplace_path.parent.mkdir(parents=True)
    original_marketplace = '{"name":"existing","plugins":[]}\n'
    marketplace_path.write_text(original_marketplace, encoding="utf-8")

    result = _run_installer(invalid_bundle, user_home)

    assert result.returncode != 0
    assert original_plugin.read_text(encoding="utf-8") == "untouched"
    assert not (existing_plugin / "content-marker.txt").exists()
    assert marketplace_path.read_text(encoding="utf-8") == original_marketplace
    transaction_artifacts = list((user_home / ".codex" / "plugins").glob(".cmo-agent-bridge.*"))
    assert transaction_artifacts == []


def test_invalid_marketplace_rolls_back_existing_plugin(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "plugin.zip", marker="replacement")
    user_home = tmp_path / "home"
    existing_plugin = _plugin_path(user_home)
    existing_plugin.mkdir(parents=True)
    original_plugin = existing_plugin / "original.txt"
    original_plugin.write_text("untouched", encoding="utf-8")
    marketplace_path = _marketplace_path(user_home)
    marketplace_path.parent.mkdir(parents=True)
    marketplace_path.write_text('"not an object"\n', encoding="utf-8")

    result = _run_installer(bundle, user_home)

    assert result.returncode != 0
    assert original_plugin.read_text(encoding="utf-8") == "untouched"
    assert not (existing_plugin / "content-marker.txt").exists()
    assert marketplace_path.read_text(encoding="utf-8") == '"not an object"\n'
