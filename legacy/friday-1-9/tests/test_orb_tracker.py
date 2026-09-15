"""Tests for provider-independent opening-range tracking."""

import json
from datetime import datetime, timezone

from app.state.orb_tracker import ORB_SYMBOLS, ORBTracker


def _utc_timestamp(hour: int, minute: int) -> datetime:
    return datetime(2026, 8, 24, hour, minute, tzinfo=timezone.utc)


def test_all_prediction_symbols_are_reported_before_first_tick(tmp_path, monkeypatch):
    tracker = ORBTracker(export_dir=tmp_path)
    monkeypatch.setattr(
        "app.state.orb_tracker.now_et",
        lambda *_args: _utc_timestamp(13, 0).astimezone(),
    )

    assert set(tracker.get_all_orb_data()) == set(ORB_SYMBOLS)


def test_orb_uses_tick_timestamp_and_persists_once(tmp_path, monkeypatch):
    from app.utils.time_et import now_et as _real_now_et

    monkeypatch.setattr(
        "app.state.orb_tracker.now_et",
        lambda dt=None: _real_now_et(_utc_timestamp(9, 0)) if dt is None else _real_now_et(dt),
    )
    tracker = ORBTracker(export_dir=tmp_path)

    tracker.update_price("SPX", 6500.0, _utc_timestamp(13, 30))
    tracker.update_price("SPX", 6510.0, _utc_timestamp(14, 0))
    tracker.update_price("SPX", 6490.0, _utc_timestamp(14, 15))
    tracker.update_price("SPX", 6505.0, _utc_timestamp(14, 31))
    tracker.update_price("SPX", 6506.0, _utc_timestamp(14, 32))

    orb = tracker.get_orb_data("SPX")
    assert orb is not None
    assert orb.opening_price == 6500.0
    assert orb.orb_high == 6510.0
    assert orb.orb_low == 6490.0
    assert orb.orb_complete is True

    audit_path = tmp_path / "2026-08-24.ndjson"
    records = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["symbol"] == "SPX"


def test_completed_orb_is_restored_after_restart(tmp_path, monkeypatch):
    from app.utils.time_et import now_et as _real_now_et

    monkeypatch.setattr(
        "app.state.orb_tracker.now_et",
        lambda dt=None: _real_now_et(_utc_timestamp(9, 0)) if dt is None else _real_now_et(dt),
    )
    tracker = ORBTracker(export_dir=tmp_path)
    tracker.update_price("NDX", 24000.0, _utc_timestamp(13, 30))
    tracker.update_price("NDX", 24100.0, _utc_timestamp(14, 0))
    tracker.update_price("NDX", 24050.0, _utc_timestamp(14, 31))

    restored = ORBTracker(export_dir=tmp_path).get_orb_data("NDX")

    assert restored is not None
    assert restored.orb_complete is True
    assert restored.orb_high == 24100.0
