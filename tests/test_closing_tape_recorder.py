from datetime import date, datetime, timedelta, timezone

import pytest

from backend.closing_tape.config import build_session_config
from backend.closing_tape.recorder import RecorderLockError, SessionRecorder


UTC = timezone.utc


def test_recorder_lock_is_exclusive_and_recoverable(tmp_path):
    now = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=now,
        session_id="lock-test",
        stop_due_utc=now + timedelta(seconds=1),
    )
    first = SessionRecorder(config, client_factory=lambda **_: object())
    second = SessionRecorder(config, client_factory=lambda **_: object())

    first._acquire_lock()
    try:
        with pytest.raises(RecorderLockError):
            second._acquire_lock()
    finally:
        first._release_lock()

    second._acquire_lock()
    second._release_lock()


def test_recorder_refuses_to_start_without_key(tmp_path, monkeypatch):
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    )

    with pytest.raises(RuntimeError, match="DATABENTO_API_KEY"):
        SessionRecorder(config).run()
