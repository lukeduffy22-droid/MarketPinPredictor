from datetime import datetime, timedelta, timezone
import json
import sqlite3
from types import SimpleNamespace

import pandas as pd

from backend.equity_context import EquityContextTracker

NOW = datetime(2026, 9, 10, 15, 0, tzinfo=timezone.utc)


def bar(symbol="SPY", stamp=None, close=100):
    return {"symbol": symbol, "ts_event": stamp or NOW - timedelta(minutes=1),
            "open": 100, "high": 110, "low": 90, "close": close, "volume": 10}


def test_disabled_snapshot_and_start_do_not_fetch_or_create_database(tmp_path):
    def forbidden(**kwargs):
        raise AssertionError("Unexpected provider request")
    path = tmp_path / "context.db"
    tracker = EquityContextTracker(symbols=("SPY", "QQQ"), db_path=path, fetcher=forbidden)
    assert tracker.start() is False
    assert tracker.poll_once(NOW) == 0
    assert tracker.snapshot(NOW)["status"] == "STAGED_DISABLED"
    assert tracker.history("SPX") == []
    assert not path.exists()


def test_capture_rejects_bad_and_incomplete_bars_and_retains_source_identity(tmp_path):
    tracker = EquityContextTracker(symbols=("SPY", "QQQ"), db_path=tmp_path / "context.db")
    rows = [bar(), bar("QQQ", close=float("nan")), bar("SPX"), bar(stamp=NOW),
            bar(stamp=NOW.replace(hour=12)), bar(close=-1)]
    assert tracker.ingest(rows, observed_at=NOW) == 1
    snap = tracker.snapshot(NOW)
    assert snap["symbols"]["SPY"]["fresh"] is True
    latest = snap["symbols"]["SPY"]["latest_bar"]
    assert latest["source_kind"] == "observed_etf_bar"
    assert latest["consolidated_sip"] is False
    assert latest["observed_at_utc"] == NOW.isoformat()
    assert snap["rejected_bar_count"] == 5
    assert tracker.ingest([bar()], observed_at=NOW + timedelta(minutes=1)) == 0
    assert tracker.snapshot(NOW + timedelta(minutes=5))["symbols"]["SPY"]["status"] == "STALE"
    with sqlite3.connect(tmp_path / "context.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM equity_context_bars").fetchone()[0] == 1
        payload = json.loads(conn.execute("SELECT payload_json FROM equity_context_bars").fetchone()[0])
        assert payload["source_revision"] == latest["source_revision"]


def test_one_batch_per_minute_and_entitlement_circuit_breaker():
    calls = []
    def denied(**kwargs):
        calls.append(kwargs)
        raise RuntimeError("403 license_not_found_unauthorized secret-api-key")
    tracker = EquityContextTracker(symbols=("SPY", "QQQ", "DIA", "IWM"), enabled=True, fetcher=denied)
    assert tracker.poll_once(NOW) == 0
    assert tracker.poll_once(NOW + timedelta(minutes=1)) == 0
    assert len(calls) == 1
    assert calls[0]["symbols"] == ["SPY", "QQQ", "DIA", "IWM"]
    assert calls[0]["schema"] == "ohlcv-1m"
    snap = tracker.snapshot(NOW)
    assert snap["status"] == "ENTITLEMENT_UNAVAILABLE"
    assert "secret-api-key" not in json.dumps(snap)
    tracker.poll_once(NOW + timedelta(minutes=30))
    assert len(calls) == 2


def test_out_of_order_revisions_have_stable_sorted_retention_and_append_only_storage(tmp_path):
    tracker = EquityContextTracker(symbols=("SPY",), db_path=tmp_path / "context.db", max_bars_per_symbol=2)
    assert tracker.ingest([bar(), bar(stamp=NOW - timedelta(minutes=3)),
                           bar(stamp=NOW - timedelta(minutes=2))], observed_at=NOW) == 3
    assert len(tracker.history("SPY")) == 2
    assert tracker.history("SPY")[-1]["close"] == 100
    assert tracker.ingest([bar(close=101)], observed_at=NOW) == 1
    assert tracker.history("SPY")[-1]["close"] == 101
    with sqlite3.connect(tmp_path / "context.db") as conn:
        assert conn.execute("SELECT COUNT(*) FROM equity_context_bars").fetchone()[0] == 4


def test_off_hours_polling_does_not_contact_provider():
    calls = []
    tracker = EquityContextTracker(symbols=("SPY",), enabled=True, fetcher=lambda **kwargs: calls.append(kwargs))
    assert tracker.poll_once(NOW.replace(hour=22)) == 0
    assert calls == []


def test_restored_forecast_history_preserves_observation_time_and_uses_bar_end(tmp_path):
    path = tmp_path / "context.db"
    initial = EquityContextTracker(symbols=("SPY",), db_path=path)
    initial.ingest([bar()], observed_at=NOW)
    restored = EquityContextTracker(symbols=("SPY",), db_path=path)
    assert restored.restore() == 1
    row = restored.forecast_history("SPY")[0]
    assert row["timestamp_utc"] == NOW.isoformat()
    assert row["source_bar_start_utc"] == (NOW - timedelta(minutes=1)).isoformat()
    assert row["available_at_utc"] == NOW.isoformat()
    assert row["subscription_epoch_id"] == initial.subscription_epoch_id
    assert row["price"] == row["close"] == 100
    assert row["source_verified"] is True
    assert row["universe_sha256"]
    assert row["volume_kind"] == "minute_bar_aggregate"
    assert "volume_price" not in row


def test_provider_revision_reversion_is_retained_as_a_new_observation(tmp_path):
    path = tmp_path / "context.db"
    tracker = EquityContextTracker(symbols=("SPY",), db_path=path)
    tracker.ingest([bar(close=100)], observed_at=NOW)
    tracker.ingest([bar(close=101)], observed_at=NOW + timedelta(minutes=1))
    tracker.ingest([bar(close=100)], observed_at=NOW + timedelta(minutes=2))
    restored = EquityContextTracker(symbols=("SPY",), db_path=path)
    assert restored.restore() == 1
    assert restored.history("SPY")[0]["close"] == 100
    assert restored.history("SPY")[0]["observed_at_utc"] == (NOW + timedelta(minutes=2)).isoformat()
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM equity_context_bars").fetchone()[0] == 3


def test_available_end_retry_excludes_provider_partial_minute(monkeypatch):
    calls = []
    def get_range(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("data_end_after_available_end: available up to '2026-09-10T14:59:30Z'")
        return SimpleNamespace(to_df=lambda: pd.DataFrame())
    monkeypatch.setattr("databento.Historical", lambda key: SimpleNamespace(timeseries=SimpleNamespace(get_range=get_range)))
    assert EquityContextTracker._fetch(dataset="EQUS.MINI", schema="ohlcv-1m", symbols=["SPY"],
                                       start="2026-09-10T14:57:00+00:00", end=NOW.isoformat()) == []
    assert calls[1]["end"] == "2026-09-10T14:59:00+00:00"
