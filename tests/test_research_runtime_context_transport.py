"""Runtime health transport failures must not erase readable retained evidence."""
import hashlib
import io
import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from backend import market_research_service as research

NOW = datetime(2026, 9, 10, 17, tzinfo=timezone.utc)
EPOCH = "a" * 64


def context():
    return {"subscription_epoch_id": EPOCH, "subscription_generation": 1,
            "handoff_status": "active", "runtime_context_stable": True,
            "symbol_status": {"SPX": {"usable_for_prediction": True}}}


def source_db(tmp_path):
    path = tmp_path / "retained.db"
    with sqlite3.connect(path) as con:
        con.execute("CREATE TABLE market_structure_observations (observation_id TEXT, symbol TEXT, trading_date TEXT, "
                    "source_timestamp_utc TEXT, captured_at_utc TEXT, reference_price REAL, provider TEXT, "
                    "spot_source TEXT, subscription_epoch_id TEXT, subscription_generation INTEGER, validation_status TEXT, "
                    "universe_sha256 TEXT, same_day_profile_available INTEGER, primary_expiration TEXT, gamma_pin REAL, net_gex REAL)")
        for i in range(40):
            ts = NOW - timedelta(minutes=39-i)
            con.execute("INSERT INTO market_structure_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (str(i), "SPX", "2026-09-10", ts.replace(tzinfo=None).isoformat(" "),
                         (ts+timedelta(seconds=1)).replace(tzinfo=None).isoformat(" "), 6000+i,
                         "databento", "databento_opra_put_call_parity", EPOCH, 1, "valid",
                         "b"*64, 1, "2026-09-10", 6040, 12))
        con.execute("CREATE TABLE eod_close_observations (id INTEGER PRIMARY KEY, symbol TEXT, trading_date TEXT, "
                    "official_close REAL, source TEXT, source_verified INTEGER, observed_at_utc TEXT, "
                    "ingested_at_utc TEXT, source_artifact_sha256 TEXT, correction_of_id INTEGER)")
        con.execute("INSERT INTO eod_close_observations VALUES (1,?,?,?,?,?,?,?,?,NULL)",
                    ("SPX", "2026-09-09", 6000, "official-test-fixture", 1,
                     "2026-09-09 20:01:00", "2026-09-09 20:01:00", "c"*64))
    return path


@pytest.mark.parametrize("error", [TimeoutError("health timeout"), OSError("health unavailable"),
                                   ValueError("RUNTIME_HEALTH_SAMPLE_STALE")])
def test_failed_bracketing_health_read_preserves_daily_evidence_and_abstains(tmp_path, error):
    path = source_db(tmp_path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    calls = []
    def reader():
        calls.append(None)
        if len(calls) == 2:
            raise error
        return context()
    service = research.MarketResearchService(path, tmp_path/"research.db", context_reader=reader, symbols=["SPX"])
    service.initialize_journal()
    state = service.collect_once(now=NOW)
    assert len(calls) == 2
    assert state["symbols"]["SPX"]["verified_daily_rows"] == 1
    assert state["symbols"]["SPX"]["retained_intraday_rows"] == 39
    assert all(f["predicted_close"] is None for f in state["symbols"]["SPX"]["forecasts"].values())
    assert state["symbols"]["SPX"]["directional_shift"]["status"] == "ABSTAIN"
    assert "RUNTIME_CONTEXT_CHANGED_OR_UNVERIFIED" in state["reasons"]
    assert "RUNTIME_CONTEXT_RECHECK_UNAVAILABLE:" + type(error).__name__ in state["reasons"]
    assert "RETAINED_EVIDENCE_UNAVAILABLE" not in state["reasons"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    with sqlite3.connect(service.journal_database) as con:
        assert con.execute("SELECT COUNT(*) FROM research_forecasts").fetchone()[0] == 0
    # A new successful bracket can recover; the first sample is never used as
    # a substitute for the failed recheck in the preceding capture.
    recovered = service.collect_once(now=NOW)
    assert len(calls) == 4
    assert recovered["symbols"]["SPX"]["forecasts"]["0"]["status"] == "RESEARCH_ONLY"
    assert recovered["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is not None


def test_failed_initial_health_read_keeps_official_history_but_no_live_estimate(tmp_path):
    path = source_db(tmp_path)
    def reader():
        raise TimeoutError("initial health timeout")
    service = research.MarketResearchService(path, tmp_path/"research.db", context_reader=reader, symbols=["SPX"])
    service.initialize_journal()
    state = service.collect_once(now=NOW)
    assert state["symbols"]["SPX"]["verified_daily_rows"] == 1
    assert state["symbols"]["SPX"]["retained_intraday_rows"] == 39
    assert state["symbols"]["SPX"]["forecasts"]["0"]["predicted_close"] is None
    assert "RUNTIME_CONTEXT_UNAVAILABLE:TimeoutError" in state["reasons"]
    assert "RETAINED_EVIDENCE_UNAVAILABLE" not in state["reasons"]


@pytest.mark.parametrize("payload,reason", [
    ([], "RUNTIME_HEALTH_PAYLOAD_INVALID"),
    (None, "RUNTIME_HEALTH_PAYLOAD_INVALID"),
    ({}, "RUNTIME_HEALTH_SAMPLE_INVALID"),
    ({"sampled_at_utc": "bad"}, "RUNTIME_HEALTH_SAMPLE_INVALID"),
])
def test_runtime_reader_rejects_malformed_samples_explicitly(monkeypatch, payload, reason):
    monkeypatch.setattr(research.urllib.request, "urlopen",
                        lambda *args, **kwargs: io.BytesIO(json.dumps(payload).encode()))
    with pytest.raises(ValueError, match=reason):
        research.read_runtime_context()


@pytest.mark.parametrize("offset", [-16, 60])
def test_runtime_reader_still_rejects_stale_and_future_samples(monkeypatch, offset):
    payload = {"sampled_at_utc": (datetime.now(timezone.utc)+timedelta(seconds=offset)).isoformat()}
    monkeypatch.setattr(research.urllib.request, "urlopen",
                        lambda *args, **kwargs: io.BytesIO(json.dumps(payload).encode()))
    with pytest.raises(ValueError, match="RUNTIME_HEALTH_SAMPLE_STALE"):
        research.read_runtime_context()


def test_runtime_reader_remains_bounded_and_preserves_timeout(monkeypatch):
    calls = []
    def request(url, *, timeout):
        calls.append((url, timeout))
        raise TimeoutError("health timeout")
    monkeypatch.setattr(research.urllib.request, "urlopen", request)
    with pytest.raises(TimeoutError):
        research.read_runtime_context()
    assert calls == [("http://127.0.0.1:8000/health/live", 2.0)]
