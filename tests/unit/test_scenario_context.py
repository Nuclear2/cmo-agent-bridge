from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from cmo_agent_bridge.scenario_context import (
    ScenarioContextProcessResult,
    ScenarioContextReader,
)


class _FakeRunner:
    def __init__(self, payload: object, *, returncode: int = 0, stderr: bytes = b"") -> None:
        self._result = ScenarioContextProcessResult(
            returncode=returncode,
            stdout=json.dumps(payload).encode(),
            stderr=stderr,
        )
        self.calls: list[tuple[tuple[str, ...], Path, float]] = []

    async def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> ScenarioContextProcessResult:
        self.calls.append((tuple(command), cwd, timeout_seconds))
        return self._result


def _root_and_scenario(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "CMO"
    root.mkdir()
    (root / "Command.exe").write_bytes(b"marker")
    scenario_directory = root / "Scenarios" / "Test"
    scenario_directory.mkdir(parents=True)
    scenario = scenario_directory / "mission.scen"
    scenario.write_bytes(b"marker")
    return root, scenario


def _success_payload(*, description: str = "<p>Situation</p>", briefing: str = "Orders") -> object:
    return {
        "ok": True,
        "scenario_description": description,
        "player_side_guid": "SIDE-1",
        "player_side_name": "Blue",
        "side_briefing": briefing,
        "scoring": {
            "major_defeat": -100,
            "minor_defeat": -50,
            "average": 0,
            "minor_victory": 50,
            "major_victory": 100,
        },
    }


async def test_reader_invokes_packaged_powershell_and_returns_context(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    runner = _FakeRunner(
        _success_payload(
            description="<h1>Situation</h1><p>Enemy airfield located.</p>",
            briefing="<p>Protect the carrier.</p><ul><li>Maintain CAP</li></ul>",
        )
    )
    reader = ScenarioContextReader(
        runner=runner,
        powershell_executable="powershell.exe",
    )

    result = await reader.read(
        game_root=root,
        file_name_path=str(scenario.parent),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.available is True
    assert result.scenario_description == "Situation\nEnemy airfield located."
    assert result.player_side_name == "Blue"
    assert result.side_briefing == "Protect the carrier.\n- Maintain CAP"
    assert result.scoring is not None
    assert result.scoring.major_victory == 100
    assert result.unavailable_reason is None
    command, cwd, timeout = runner.calls[0]
    assert command[0] == "powershell.exe"
    assert command[command.index("-ScenarioPath") + 1] == str(scenario.resolve())
    assert command[command.index("-PlayerSideGuid") + 1] == "SIDE-1"
    assert command[command.index("-File") + 1].endswith("scenario_context_reader.ps1")
    assert cwd == root.resolve()
    assert timeout == 30.0


async def test_reader_resolves_relative_scenario_directory_and_loaddoc(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    document_directory = scenario.parent / "files" / "Blue"
    document_directory.mkdir(parents=True)
    (document_directory / "Briefing.html").write_text(
        "<h2>Mission</h2><p>Destroy the hostile fighters.</p>",
        encoding="utf-8",
    )
    runner = _FakeRunner(
        _success_payload(briefing="<BODY>[LOADDOC]files\\Blue\\Briefing.html[/LOADDOC]</BODY>")
    )
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path="Scenarios\\Test",
        file_name="mission.scen",
        player_side_guid="SIDE-1",
    )

    assert result.available is True
    assert result.side_briefing == "Mission\nDestroy the hostile fighters."
    assert result.warnings == ()


async def test_reader_honors_declared_gbk_loaddoc_charset(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    document = scenario.parent / "Briefing.html"
    document.write_bytes(
        '<html><meta charset="gbk"><body><p>任务：保护机场。</p></body></html>'.encode("gbk")
    )
    runner = _FakeRunner(_success_payload(briefing="[LOADDOC]Briefing.html[/LOADDOC]"))
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path=str(scenario.parent),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.available is True
    assert result.side_briefing == "任务：保护机场。"


async def test_reader_accepts_directory_with_scenario_like_suffix(tmp_path: Path) -> None:
    root, _ = _root_and_scenario(tmp_path)
    directory = root / "Scenarios" / "Campaign.scen"
    directory.mkdir()
    scenario = directory / "mission.scen"
    scenario.write_bytes(b"marker")
    runner = _FakeRunner(_success_payload())
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path=str(directory),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.available is True
    command, _cwd, _timeout = runner.calls[0]
    assert command[command.index("-ScenarioPath") + 1] == str(scenario.resolve())


async def test_reader_rejects_url_and_traversal_loaddoc_references(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    outside = scenario.parent.parent / "secret.html"
    outside.write_text("SECRET", encoding="utf-8")
    runner = _FakeRunner(
        _success_payload(
            description="[LOADDOC]https://example.test/orders[/LOADDOC]",
            briefing="[LOADDOC]..\\secret.html[/LOADDOC]",
        )
    )
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path=str(scenario.parent),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.available is True
    assert result.scenario_description == "[External briefing document unavailable]"
    assert result.side_briefing == "[External briefing document unavailable]"
    assert len(result.warnings) == 2
    assert "rejected" in result.warnings[0]
    assert "could not be read" in result.warnings[1]
    assert "SECRET" not in (result.side_briefing or "")


async def test_reader_limits_each_text_and_marks_truncation(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    runner = _FakeRunner(_success_payload(description="abcdef", briefing="123456"))
    reader = ScenarioContextReader(
        runner=runner,
        powershell_executable="powershell.exe",
        max_text_characters=4,
    )

    result = await reader.read(
        game_root=root,
        file_name_path=str(scenario.parent),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.scenario_description == "abcd"
    assert result.side_briefing == "1234"
    assert result.description_truncated is True
    assert result.briefing_truncated is True


async def test_reader_returns_structured_unavailable_when_scenario_is_unsaved(
    tmp_path: Path,
) -> None:
    root, _ = _root_and_scenario(tmp_path)
    runner = _FakeRunner(_success_payload())
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path="",
        file_name="",
        player_side_guid="SIDE-1",
    )

    assert result.available is False
    assert result.unavailable_reason == "CMO did not report a saved scenario file name"
    assert runner.calls == []


async def test_reader_preserves_powershell_failure_as_unavailable(tmp_path: Path) -> None:
    root, scenario = _root_and_scenario(tmp_path)
    runner = _FakeRunner({"ok": False, "error": "player side not found"}, returncode=1)
    reader = ScenarioContextReader(runner=runner, powershell_executable="powershell.exe")

    result = await reader.read(
        game_root=root,
        file_name_path=str(scenario.parent),
        file_name=scenario.name,
        player_side_guid="SIDE-1",
    )

    assert result.available is False
    assert result.unavailable_reason == "player side not found"
    assert result.player_side_guid == "SIDE-1"


def test_packaged_script_deserializes_only_the_selected_side() -> None:
    script = (
        Path(__file__).parents[2]
        / "src"
        / "cmo_agent_bridge"
        / "host_assets"
        / "scenario_context_reader.ps1"
    ).read_text(encoding="utf-8")

    assert 'SelectNodes("Sides/Side")' in script
    assert 'SelectSingleNode("ID")' in script
    assert "$sideType::FromXML" in script
    assert script.count("::FromXML") == 1
    assert 'root.Name -ne "Scenario"' in script
    assert 'root.Name -ne "ContentScenario"' in script
