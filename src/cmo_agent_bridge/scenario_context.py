from __future__ import annotations

import asyncio
import codecs
import json
import os
import re
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from html.parser import HTMLParser
from importlib.resources import as_file, files
from pathlib import Path, PureWindowsPath
from typing import Protocol, cast


_LOADDOC_PATTERN = re.compile(r"\[LOADDOC\](.*?)\[/LOADDOC\]", re.IGNORECASE | re.DOTALL)
_URL_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*://")
_HTML_CHARSET_PATTERN = re.compile(rb"charset\s*=\s*['\"]?\s*([A-Za-z0-9._:-]+)", re.IGNORECASE)
_BLOCK_TAGS = frozenset(
    {
        "address",
        "article",
        "aside",
        "blockquote",
        "div",
        "dl",
        "dt",
        "dd",
        "fieldset",
        "figcaption",
        "figure",
        "footer",
        "form",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "hr",
        "main",
        "nav",
        "ol",
        "p",
        "pre",
        "section",
        "table",
        "tr",
        "ul",
    }
)
_IGNORED_TAGS = frozenset({"script", "style"})


@dataclass(frozen=True, slots=True)
class ScenarioScoring:
    major_defeat: int
    minor_defeat: int
    average: int
    minor_victory: int
    major_victory: int


@dataclass(frozen=True, slots=True)
class ScenarioContext:
    available: bool
    scenario_description: str | None
    player_side_guid: str
    player_side_name: str | None
    side_briefing: str | None
    scoring: ScenarioScoring | None
    description_truncated: bool
    briefing_truncated: bool
    warnings: tuple[str, ...]
    unavailable_reason: str | None

    @classmethod
    def unavailable(cls, *, player_side_guid: str, reason: str) -> ScenarioContext:
        return cls(
            available=False,
            scenario_description=None,
            player_side_guid=player_side_guid,
            player_side_name=None,
            side_briefing=None,
            scoring=None,
            description_truncated=False,
            briefing_truncated=False,
            warnings=(),
            unavailable_reason=reason,
        )


@dataclass(frozen=True, slots=True)
class ScenarioContextProcessResult:
    returncode: int
    stdout: bytes
    stderr: bytes


class ScenarioContextProcessRunner(Protocol):
    async def __call__(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        timeout_seconds: float,
    ) -> ScenarioContextProcessResult: ...


class ScenarioContextReaderPort(Protocol):
    async def read(
        self,
        *,
        game_root: Path,
        file_name_path: str,
        file_name: str,
        player_side_guid: str,
    ) -> ScenarioContext: ...


class _ReadableHtml(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._parts: list[str] = []
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag in _IGNORED_TAGS:
            self._ignored_depth += 1
            return
        if self._ignored_depth:
            return
        if tag == "li":
            self._line_break()
            self._parts.append("- ")
        elif tag == "br" or tag in _BLOCK_TAGS:
            self._line_break()
        elif tag in {"td", "th"} and self._parts:
            self._parts.append(" | ")
        elif tag == "img":
            alt = next((value for name, value in attrs if name.lower() == "alt"), None)
            if alt:
                self._parts.append(f"[Image: {alt.strip()}]")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in _IGNORED_TAGS:
            if self._ignored_depth:
                self._ignored_depth -= 1
            return
        if not self._ignored_depth and (tag == "li" or tag in _BLOCK_TAGS):
            self._line_break()

    def handle_data(self, data: str) -> None:
        if not self._ignored_depth:
            self._parts.append(data)

    def _line_break(self) -> None:
        if self._parts and not self._parts[-1].endswith("\n"):
            self._parts.append("\n")

    def text(self) -> str:
        value = "".join(self._parts).replace("\r\n", "\n").replace("\r", "\n")
        lines = [re.sub(r"[\t\f\v ]+", " ", line).strip() for line in value.split("\n")]
        value = "\n".join(lines)
        value = re.sub(r"\n{3,}", "\n\n", value)
        return value.strip().replace("\x00", "")


async def _run_process(
    command: Sequence[str],
    *,
    cwd: Path,
    timeout_seconds: float,
) -> ScenarioContextProcessResult:
    creationflags = cast(int, getattr(subprocess, "CREATE_NO_WINDOW", 0)) if os.name == "nt" else 0
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(cwd),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=creationflags,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout_seconds)
    except TimeoutError:
        process.kill()
        await process.communicate()
        raise
    return ScenarioContextProcessResult(
        returncode=process.returncode if process.returncode is not None else -1,
        stdout=stdout,
        stderr=stderr,
    )


def _decode_document(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        return data.decode("utf-16")
    declared = _HTML_CHARSET_PATTERN.search(data[:4096])
    candidates = [
        declared.group(1).decode("ascii") if declared is not None else None,
        "utf-8-sig",
        "mbcs" if os.name == "nt" else None,
        "gb18030",
        "cp1252",
    ]
    attempted: set[str] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            encoding = codecs.lookup(candidate).name
        except LookupError:
            continue
        if encoding in attempted:
            continue
        attempted.add(encoding)
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def html_to_text(value: str) -> str:
    parser = _ReadableHtml()
    parser.feed(value)
    parser.close()
    return parser.text()


def _is_external_reference(value: str) -> bool:
    normalized = value.strip()
    windows_path = PureWindowsPath(normalized)
    return bool(
        not normalized
        or _URL_PATTERN.match(normalized)
        or normalized.startswith(("//", "\\\\"))
        or windows_path.is_absolute()
        or windows_path.drive
    )


def _expand_loaddoc(
    value: str,
    *,
    scenario_directory: Path,
    max_document_bytes: int,
) -> tuple[str, tuple[str, ...]]:
    warnings: list[str] = []
    base = scenario_directory.resolve(strict=True)

    def replace(match: re.Match[str]) -> str:
        reference = match.group(1).strip()
        if _is_external_reference(reference):
            warnings.append(f"LOADDOC reference was rejected: {reference or '<empty>'}")
            return "[External briefing document unavailable]"
        try:
            candidate = (base / Path(PureWindowsPath(reference))).resolve(strict=True)
            candidate.relative_to(base)
            if not candidate.is_file():
                raise OSError("not a file")
            if candidate.stat().st_size > max_document_bytes:
                raise OSError("file is too large")
            return _decode_document(candidate.read_bytes())
        except (OSError, RuntimeError, ValueError):
            warnings.append(f"LOADDOC reference could not be read: {reference}")
            return "[External briefing document unavailable]"

    return _LOADDOC_PATTERN.sub(replace, value), tuple(warnings)


def _limit_text(value: str, maximum: int) -> tuple[str, bool]:
    if len(value) <= maximum:
        return value, False
    return value[:maximum].rstrip(), True


def _resolve_scenario_path(
    *,
    game_root: Path,
    file_name_path: str,
    file_name: str,
) -> Path:
    if not file_name.strip():
        raise ValueError("CMO did not report a saved scenario file name")
    reported_path = Path(file_name_path.strip()) if file_name_path.strip() else None
    reported_candidate = (
        reported_path
        if reported_path is None or reported_path.is_absolute()
        else game_root / reported_path
    )
    if (
        reported_path is not None
        and reported_path.suffix.lower() in {".scen", ".save"}
        and not (reported_candidate is not None and reported_candidate.is_dir())
    ):
        assert reported_candidate is not None
        scenario_path = reported_candidate
    else:
        directory = reported_path or game_root / "Scenarios"
        if not directory.is_absolute():
            directory = game_root / directory
        scenario_path = directory / Path(file_name.strip())
    try:
        resolved = scenario_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValueError("the active scenario file could not be resolved") from error
    if not resolved.is_file():
        raise ValueError("the active scenario path is not a file")
    return resolved


class ScenarioContextReader:
    """Read the active player's description and briefing from the saved .scen file."""

    def __init__(
        self,
        *,
        runner: ScenarioContextProcessRunner | None = None,
        powershell_executable: str | None = None,
        timeout_seconds: float = 30.0,
        max_text_characters: int = 24_000,
        max_document_bytes: int = 1_000_000,
    ) -> None:
        self._runner = runner or _run_process
        self._powershell_executable = powershell_executable
        self._timeout_seconds = timeout_seconds
        self._max_text_characters = max_text_characters
        self._max_document_bytes = max_document_bytes

    async def read(
        self,
        *,
        game_root: Path,
        file_name_path: str,
        file_name: str,
        player_side_guid: str,
    ) -> ScenarioContext:
        guid = player_side_guid.strip()
        if not guid:
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason="CMO did not report the live player side GUID",
            )
        try:
            root = game_root.resolve(strict=True)
            scenario_path = _resolve_scenario_path(
                game_root=root,
                file_name_path=file_name_path,
                file_name=file_name,
            )
        except (OSError, RuntimeError, ValueError) as error:
            return ScenarioContext.unavailable(player_side_guid=guid, reason=str(error))
        if not (root / "Command.exe").is_file():
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason="Command.exe was not found in the configured game root",
            )

        powershell = self._powershell_executable or shutil.which("powershell.exe")
        powershell = powershell or shutil.which("pwsh.exe")
        powershell = powershell or shutil.which("powershell") or shutil.which("pwsh")
        if powershell is None:
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason="PowerShell is unavailable on this Windows host",
            )

        script = files("cmo_agent_bridge.host_assets").joinpath("scenario_context_reader.ps1")
        try:
            with as_file(script) as script_path:
                process = await self._runner(
                    (
                        powershell,
                        "-NoLogo",
                        "-NoProfile",
                        "-NonInteractive",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        str(script_path),
                        "-GameRoot",
                        str(root),
                        "-ScenarioPath",
                        str(scenario_path),
                        "-PlayerSideGuid",
                        guid,
                    ),
                    cwd=root,
                    timeout_seconds=self._timeout_seconds,
                )
        except TimeoutError:
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason="reading the saved scenario briefing timed out",
            )
        except OSError as error:
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason=f"PowerShell could not read the saved scenario: {error}",
            )

        try:
            decoded_payload = cast(object, json.loads(process.stdout.decode("utf-8-sig")))
        except (UnicodeDecodeError, json.JSONDecodeError):
            stderr = process.stderr.decode("utf-8", errors="replace").strip()
            reason = stderr or "the scenario reader returned an invalid response"
            return ScenarioContext.unavailable(player_side_guid=guid, reason=reason)
        if not isinstance(decoded_payload, dict):
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason="the scenario reader returned an invalid response",
            )
        payload = cast(dict[object, object], decoded_payload)
        if payload.get("ok") is not True:
            error = payload.get("error")
            reason = error if isinstance(error, str) and error else "the scenario reader failed"
            return ScenarioContext.unavailable(player_side_guid=guid, reason=reason)

        try:
            raw_description = _required_string(payload, "scenario_description")
            raw_briefing = _required_string(payload, "side_briefing")
            side_name = _required_string(payload, "player_side_name")
            scoring_payload = payload["scoring"]
            if not isinstance(scoring_payload, dict):
                raise TypeError("scoring must be an object")
            scoring_values = cast(dict[object, object], scoring_payload)
            scoring = ScenarioScoring(
                major_defeat=_required_integer(scoring_values, "major_defeat"),
                minor_defeat=_required_integer(scoring_values, "minor_defeat"),
                average=_required_integer(scoring_values, "average"),
                minor_victory=_required_integer(scoring_values, "minor_victory"),
                major_victory=_required_integer(scoring_values, "major_victory"),
            )
        except (KeyError, TypeError) as error:
            return ScenarioContext.unavailable(
                player_side_guid=guid,
                reason=f"the scenario reader returned an invalid payload: {error}",
            )

        description, description_warnings = _expand_loaddoc(
            raw_description,
            scenario_directory=scenario_path.parent,
            max_document_bytes=self._max_document_bytes,
        )
        briefing, briefing_warnings = _expand_loaddoc(
            raw_briefing,
            scenario_directory=scenario_path.parent,
            max_document_bytes=self._max_document_bytes,
        )
        description, description_truncated = _limit_text(
            html_to_text(description), self._max_text_characters
        )
        briefing, briefing_truncated = _limit_text(
            html_to_text(briefing), self._max_text_characters
        )
        return ScenarioContext(
            available=True,
            scenario_description=description,
            player_side_guid=guid,
            player_side_name=side_name,
            side_briefing=briefing,
            scoring=scoring,
            description_truncated=description_truncated,
            briefing_truncated=briefing_truncated,
            warnings=description_warnings + briefing_warnings,
            unavailable_reason=None,
        )


def _required_string(payload: dict[object, object], name: str) -> str:
    value = payload[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _required_integer(payload: dict[object, object], name: str) -> int:
    value = payload[name]
    if type(value) is not int:
        raise TypeError(f"{name} must be an integer")
    return value
