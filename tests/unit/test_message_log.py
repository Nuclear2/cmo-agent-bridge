from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

import pytest

from cmo_agent_bridge.errors import BridgeError, ErrorCode
from cmo_agent_bridge.message_log import NativeMessageLogService
from cmo_agent_bridge.protocol.runtime import RuntimeSnapshot
from cmo_agent_bridge.state.session_store import SessionRecord, SessionStore
from cmo_agent_bridge.state.sqlite import StateDatabase
from cmo_agent_bridge.transports.file_bridge.paths import FileBridgePaths
from cmo_agent_bridge.transports.file_bridge.process_guard import ProcessInfo


@dataclass(frozen=True, slots=True)
class _Inspector:
    processes: tuple[ProcessInfo, ...]

    def matching_processes(self, command_exe: Path) -> tuple[ProcessInfo, ...]:
        del command_exe
        return self.processes


def _runtime_snapshot() -> RuntimeSnapshot:
    return RuntimeSnapshot.create(
        runtime_version="0.4.0",
        runtime_asset_sha256="a" * 64,
        operation_manifest_sha256="b" * 64,
        host_contract_sha256="c" * 64,
        dependency_lock_sha256="d" * 64,
    )


def _environment(
    tmp_path: Path,
    *,
    logging_enabled: bool = True,
    with_session: bool = True,
) -> tuple[NativeMessageLogService, SessionStore, Path, ProcessInfo]:
    game_root = tmp_path / "CMO"
    command_exe = game_root / "Command.exe"
    logs = game_root / "Logs"
    config = game_root / "Config"
    command_exe.parent.mkdir(parents=True)
    command_exe.write_bytes(b"MZ")
    logs.mkdir()
    config.mkdir()
    (config / "Command.ini").write_text(
        f"[Game Preferences]\nLogDebugInfoToFile = {logging_enabled}\n",
        encoding="ascii",
    )
    paths = FileBridgePaths(
        game_root=game_root.resolve(),
        root_key="e" * 64,
        command_exe=command_exe.resolve(),
        lua_root=game_root / "Lua" / "CMOAgentBridge",
        inbox=game_root / "Lua" / "CMOAgentBridge" / "inbox" / "request.lua",
        import_export=game_root / "ImportExport",
        lock_file=tmp_path / "state" / "root.lock",
        pending_file=tmp_path / "state" / "pending.json",
        sqlite_file=tmp_path / "state" / "state.sqlite3",
    )
    database = StateDatabase(paths.sqlite_file)
    database.initialize()
    sessions = SessionStore(database)
    process = ProcessInfo(
        pid=7300,
        create_time=datetime(2026, 7, 18, 23, 9, 21).timestamp(),
        executable=paths.command_exe,
    )
    if with_session:
        sessions.replace(
            SessionRecord(
                root_key=paths.root_key,
                scenario_lineage_id=UUID("11111111-1111-4111-8111-111111111111"),
                activation_id=UUID("22222222-2222-4222-8222-222222222222"),
                build_number=1868,
                runtime_snapshot=_runtime_snapshot(),
                process_pid=process.pid,
                process_create_time=process.create_time,
                validated_at_ms=1_000,
            )
        )
    service = NativeMessageLogService(
        paths=paths,
        session_store=sessions,
        process_inspector=_Inspector((process,)),
        tail_stability_seconds=0,
    )
    return service, sessions, logs, process


def _write_log(path: Path, records: list[str]) -> None:
    path.write_bytes(("\r\n\r\n".join(records) + "\r\n\r\n").encode("utf-8"))


def test_status_binds_strict_timestamp_log_to_process_start(tmp_path: Path) -> None:
    service, _sessions, logs, process = _environment(tmp_path)
    expected = logs / "2026-07-18_23.09.25.txt"
    _write_log(expected, ["2025/10/3 20:00:00 - [PRC] Ready"])
    (logs / "LuaHistory_2026-07-18.txt").write_text("newer", encoding="utf-8")
    (logs / "ExceptionLog_2026_07_18.txt").write_text("newest", encoding="utf-8")
    (logs / "2026-07-18_22.10.03.txt").write_text("old", encoding="utf-8")

    status = service.status()

    assert status.state == "ready"
    assert status.available is True
    assert status.requires_lua_poll is False
    assert status.process_pid == process.pid
    assert status.log_path == str(expected.resolve())
    assert status.candidate_count == 1


def test_status_does_not_bind_a_previous_process_log_from_two_seconds_earlier(
    tmp_path: Path,
) -> None:
    service, _sessions, logs, _process = _environment(tmp_path)
    old = logs / "2026-07-18_23.09.19.txt"
    expected = logs / "2026-07-18_23.09.25.txt"
    _write_log(old, ["2025/10/3 19:59:59 - [PRC] Previous process"])
    _write_log(expected, ["2025/10/3 20:00:00 - [PRC] Current process"])

    status = service.status()

    assert status.state == "ready"
    assert status.log_path == str(expected.resolve())
    assert status.candidate_count == 1


def test_status_reports_session_and_logging_preconditions(tmp_path: Path) -> None:
    unbound, _sessions, logs, _process = _environment(tmp_path / "unbound", with_session=False)
    _write_log(logs / "2026-07-18_23.09.25.txt", ["2025/10/3 20:00:00 - [PRC] Ready"])
    assert unbound.status().state == "session_unbound"

    disabled, _sessions, logs, _process = _environment(tmp_path / "disabled", logging_enabled=False)
    _write_log(logs / "2026-07-18_23.09.25.txt", ["2025/10/3 20:00:00 - [PRC] Ready"])
    assert disabled.status().state == "logging_disabled"

    missing, _sessions, _logs, _process = _environment(tmp_path / "missing")
    assert missing.status().state == "no_log_yet"


def test_forward_cursor_reads_only_selected_side_and_plain_html(tmp_path: Path) -> None:
    service, _sessions, logs, _process = _environment(tmp_path)
    log = logs / "2026-07-18_23.09.25.txt"
    _write_log(log, ["2025/10/3 20:00:00 - [PRC] Existing"])

    opened = service.read(side_name="PRC", start="now")
    assert opened.items == ()
    assert opened.read_through_offset == log.stat().st_size

    with log.open("ab") as stream:
        stream.write(
            (
                "2025/10/3 20:01:00 - [Taiwan] Enemy only\r\n\r\n"
                "2025/10/3 20:01:01 - [PRC] <BODY><P>入电：</P>"
                "<P>行动开始！</P></BODY>\r\n\r\n"
                "2025/10/3 20:01:02 - Unscoped detail\r\n\r\n"
            ).encode("utf-8")
        )

    result = service.read(side_name="PRC", cursor=opened.next_cursor)

    assert len(result.items) == 1
    assert result.items[0].side_name == "PRC"
    assert result.items[0].text == "入电：\n行动开始！"
    assert result.items[0].raw_text is None
    assert result.items[0].is_html is True
    assert result.suppressed_other_side == 1
    assert result.suppressed_unscoped == 1
    assert result.has_more is False

    empty = service.read(side_name="PRC", cursor=result.next_cursor)
    assert empty.items == ()


def test_incomplete_tail_is_replayed_after_record_finishes(tmp_path: Path) -> None:
    service, _sessions, logs, _process = _environment(tmp_path)
    log = logs / "2026-07-18_23.09.25.txt"
    _write_log(log, ["2025/10/3 20:00:00 - [PRC] Existing"])
    opened = service.read(side_name="PRC", start="now")

    with log.open("ab") as stream:
        stream.write("2025/10/3 20:02:00 - [PRC] Partial".encode("utf-8"))
    partial = service.read(side_name="PRC", cursor=opened.next_cursor)
    assert partial.items == ()
    assert partial.pending_partial_record is True
    assert partial.has_more is False
    assert partial.read_through_offset == opened.read_through_offset

    with log.open("ab") as stream:
        stream.write(b"\r\n\r\n")
    completed = service.read(side_name="PRC", cursor=partial.next_cursor)
    assert [item.text for item in completed.items] == ["Partial"]


def test_internal_blank_line_is_not_committed_when_file_grows_during_stability_probe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, _sessions, logs, _process = _environment(tmp_path)
    log = logs / "2026-07-18_23.09.25.txt"
    _write_log(log, ["2025/10/3 20:00:00 - [PRC] Existing"])
    opened = service.read(side_name="PRC", start="now")
    with log.open("ab") as stream:
        stream.write(("2025/10/3 20:02:00 - [PRC] <BODY><P>First</P>\r\n\r\n").encode("utf-8"))

    appended = False

    def finish_record(_seconds: float) -> None:
        nonlocal appended
        if appended:
            return
        appended = True
        with log.open("ab") as stream:
            stream.write("<P>Second</P></BODY>\r\n\r\n".encode("utf-8"))

    monkeypatch.setattr("cmo_agent_bridge.message_log.time.sleep", finish_record)

    partial = service.read(side_name="PRC", cursor=opened.next_cursor)
    assert partial.items == ()
    assert partial.pending_partial_record is True
    assert partial.read_through_offset == opened.read_through_offset

    completed = service.read(side_name="PRC", cursor=partial.next_cursor)
    assert len(completed.items) == 1
    assert "First" in completed.items[0].text
    assert "Second" in completed.items[0].text


def test_recent_tail_is_explicit_and_old_cursor_rejects_new_lineage(tmp_path: Path) -> None:
    service, sessions, logs, process = _environment(tmp_path)
    log = logs / "2026-07-18_23.09.25.txt"
    _write_log(
        log,
        [
            "2025/10/3 20:00:00 - [PRC] First",
            "2025/10/3 20:00:01 - [PRC] Second",
            "2025/10/3 20:00:02 - [PRC] Third",
        ],
    )

    recent = service.read(side_name="PRC", start="recent", page_size=2)
    assert [item.text for item in recent.items] == ["Second", "Third"]
    assert recent.pre_session_history_may_be_included is True
    assert recent.warnings

    sessions.replace(
        SessionRecord(
            root_key="e" * 64,
            scenario_lineage_id=UUID("33333333-3333-4333-8333-333333333333"),
            activation_id=UUID("44444444-4444-4444-8444-444444444444"),
            build_number=1868,
            runtime_snapshot=_runtime_snapshot(),
            process_pid=process.pid,
            process_create_time=process.create_time,
            validated_at_ms=2_000,
        )
    )

    with pytest.raises(BridgeError) as caught:
        service.read(side_name="PRC", cursor=recent.next_cursor)
    assert caught.value.code is ErrorCode.SCENARIO_CHANGED


def test_cursor_cannot_change_side_or_unscoped_filter(tmp_path: Path) -> None:
    service, _sessions, logs, _process = _environment(tmp_path)
    _write_log(logs / "2026-07-18_23.09.25.txt", ["2025/10/3 20:00:00 - [PRC] Ready"])
    opened = service.read(side_name="PRC", start="now")

    with pytest.raises(BridgeError) as side_error:
        service.read(side_name="Taiwan", cursor=opened.next_cursor)
    assert side_error.value.code is ErrorCode.INVALID_ARGUMENT

    with pytest.raises(BridgeError) as scope_error:
        service.read(
            side_name="PRC",
            cursor=opened.next_cursor,
            include_unscoped=True,
        )
    assert scope_error.value.code is ErrorCode.INVALID_ARGUMENT
