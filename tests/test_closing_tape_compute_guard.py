import json
import os
from datetime import datetime, timezone

import pytest

from backend.closing_tape.compute_guard import (
    find_active_capture_sessions,
    require_compute_window,
)


UTC = timezone.utc


def _status(
    tmp_path,
    *,
    observed_at: str,
    session_status="running",
    feed_status="running",
    pid=41400,
    tape_root=None,
):
    root = tape_root or tmp_path / "data" / "closing_tape"
    day = root / "2026-09-01"
    day.mkdir(parents=True)
    path = day / "status.json"
    path.write_text(
        json.dumps(
            {
                "observed_at_utc": observed_at,
                "pid": pid,
                "session": {
                    "session_id": "2026-09-01-124553Z",
                    "trading_date": "2026-09-01",
                    "status": session_status,
                },
                "feeds": [
                    {"feed_name": "opra_options", "status": feed_status}
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_compute_guard_detects_fresh_running_capture(tmp_path):
    _status(tmp_path, observed_at="2026-09-01T15:39:30+00:00")

    active = find_active_capture_sessions(
        tmp_path,
        now_utc=datetime(2026, 9, 1, 15, 40, tzinfo=UTC),
    )

    assert len(active) == 1
    assert active[0].session_id == "2026-09-01-124553Z"
    assert active[0].feed_names == ("opra_options",)


def test_compute_guard_ignores_stale_dead_or_terminal_status(tmp_path, monkeypatch):
    path = _status(tmp_path, observed_at="2026-09-01T15:30:00+00:00")
    now = datetime(2026, 9, 1, 15, 40, tzinfo=UTC)
    monkeypatch.setattr("backend.closing_tape.compute_guard._pid_is_running", lambda _pid: False)

    assert find_active_capture_sessions(tmp_path, now_utc=now) == ()

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["observed_at_utc"] = "2026-09-01T15:39:30+00:00"
    payload["session"]["status"] = "complete"
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert find_active_capture_sessions(tmp_path, now_utc=now) == ()


def test_compute_guard_retains_stale_running_status_when_pid_is_live(tmp_path, monkeypatch):
    _status(tmp_path, observed_at="2026-09-01T15:30:00+00:00", pid=999)
    monkeypatch.setattr("backend.closing_tape.compute_guard._pid_is_running", lambda pid: pid == 999)

    active = find_active_capture_sessions(
        tmp_path,
        now_utc=datetime(2026, 9, 1, 15, 40, tzinfo=UTC),
    )

    assert len(active) == 1
    assert active[0].pid == 999


def test_compute_guard_ignores_status_under_archive_catalog_root(
    tmp_path,
    monkeypatch,
):
    external_root = tmp_path / "external-tape"
    _status(
        tmp_path,
        observed_at="2020-09-01T15:39:30+00:00",
        pid=os.getpid(),
        tape_root=external_root,
    )
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(external_root))

    active = find_active_capture_sessions(
        tmp_path,
        now_utc=datetime(2026, 9, 1, 15, 40, tzinfo=UTC),
    )

    assert active == ()


def test_compute_guard_does_not_treat_missing_archive_as_live_state(
    tmp_path,
    monkeypatch,
):
    missing_root = tmp_path / "missing-tape"
    monkeypatch.setenv("CLOSING_TAPE_CATALOG_ROOTS", str(missing_root))

    assert find_active_capture_sessions(tmp_path) == ()

    assert not missing_root.exists()


def test_compute_window_requires_explicit_live_capture_override(tmp_path, monkeypatch):
    active = (
        type(
            "Capture",
            (),
            {
                "session_id": "s1",
                "trading_date": "2026-09-01",
            },
        )(),
    )
    monkeypatch.setattr(
        "backend.closing_tape.compute_guard.find_active_capture_sessions",
        lambda *_args, **_kwargs: active,
    )

    with pytest.raises(RuntimeError, match="refused while closing-tape capture is active: s1"):
        require_compute_window(tmp_path, operation="test compute")

    assert require_compute_window(
        tmp_path,
        operation="test compute",
        allow_active_capture=True,
    ) == active
