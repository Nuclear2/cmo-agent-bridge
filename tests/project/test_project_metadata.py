import tomllib
from pathlib import Path
from typing import cast

from cmo_agent_bridge import __version__


def test_package_version() -> None:
    root = Path(__file__).resolve().parents[2]
    document = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    project = cast(dict[str, object], document["project"])
    assert __version__ == cast(str, project["version"])
