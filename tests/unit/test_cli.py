import pytest
from typer.testing import CliRunner

from cmo_agent_bridge import __version__
import cmo_agent_bridge.cli as cli_module
from cmo_agent_bridge.cli import app


def _forbid_eager_build(**_kwargs: object) -> None:
    raise AssertionError("eager runtime build")


def test_version_command_reports_package_version() -> None:
    result = CliRunner().invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == __version__


def test_serve_enters_stdio_without_eager_runtime_build(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[object] = []
    sentinel = object()

    def fake_manager(**_kwargs: object) -> object:
        return sentinel

    monkeypatch.setattr(cli_module, "McpRuntimeManager", fake_manager)
    monkeypatch.setattr(cli_module, "run_stdio", captured.append)
    monkeypatch.setattr(
        cli_module,
        "build_application_runtime",
        _forbid_eager_build,
    )

    result = CliRunner().invoke(app, ["serve"])

    assert result.exit_code == 0
    assert captured == [sentinel]
