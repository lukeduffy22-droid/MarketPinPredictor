from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import time

import pytest

from backend.forecast_attempt_journal import append_attempt, read_attempts
from backend.market_research_service import MarketResearchService

NOW = datetime(2026, 10, 8, 19, 0, tzinfo=timezone.utc)


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///" + (tmp_path / "unused.db").as_posix())
    result = MarketResearchService(tmp_path/"source.db", tmp_path/"journal.db", symbols=["SPX", "NDX"])
    result.initialize_journal()
    return result


def capture_state():
    return {"as_of_utc": NOW.isoformat(), "reasons": [], "symbols": {
        "SPX": {"forecasts": {"0": {"status": "RESEARCH_ONLY", "selected_method": "last_price_v1",
                                   "forecast_id": "frozen", "reasons": ["unpromoted_research_candidate"]}}},
        "NDX": {"forecasts": {"0": {"status": "ABSTAIN", "reasons": ["latest_observation_invalid"]}}}}}


def test_success_abstention_and_failure_are_all_append_only_attempts(service, monkeypatch):
    state = capture_state()
    original = deepcopy(state)
    monkeypatch.setattr(service, "_collect_once", lambda **kw: deepcopy(state))
    result = service.collect_once(now=NOW)
    assert result["attempt_journal"]["status"] == "RECORDED"
    def failure(**kw): raise RuntimeError("private failure details")
    monkeypatch.setattr(service, "_collect_once", failure)
    with pytest.raises(RuntimeError): service.collect_once(now=NOW)
    audit = read_attempts(service.journal_database)
    assert audit["status"] == "READ_ONLY" and len(audit["attempts"]) == 2
    failed, captured = audit["attempts"]
    assert failed["status"] == "FAILED" and failed["reasons"] == ["RESEARCH_CAPTURE_FAILED"]
    assert all(f["status"] == "ERROR" for s in failed["symbols"].values() for f in s.values())
    assert captured["symbols"]["SPX"]["0"]["selected_method"] == "last_price_v1"
    assert captured["symbols"]["NDX"]["0"]["status"] == "ABSTAIN"
    assert "private failure" not in json.dumps(audit)
    assert state == original
    with sqlite3.connect(service.journal_database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM research_forecasts").fetchone()[0] == 0
        for statement in ("DELETE FROM research_forecast_attempts", "UPDATE research_forecast_attempts SET status='CAPTURED'"):
            with pytest.raises(sqlite3.IntegrityError, match="append only"):
                connection.execute(statement)


def test_attempt_hash_is_verifiable_and_duplicate_cannot_replace(service):
    args = dict(instance_id="instance", sequence=1, attempted_at_utc=NOW.isoformat(), symbols=["SPX"], state=capture_state())
    first = append_attempt(service.journal_database, **args)
    assert first["status"] == "RECORDED"
    second = append_attempt(service.journal_database, **args)
    assert second["status"] == "UNAVAILABLE"
    with sqlite3.connect(service.journal_database) as connection:
        rows = connection.execute("SELECT payload_sha256,payload_json FROM research_forecast_attempts").fetchall()
    assert len(rows) == 1 and hashlib.sha256(rows[0][1].encode()).hexdigest() == rows[0][0] == first["payload_sha256"]


def test_locked_attempt_append_is_bounded_and_visible_in_worker(service, monkeypatch):
    monkeypatch.setattr(service, "_collect_once", lambda **kw: capture_state())
    with sqlite3.connect(service.journal_database) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        result = service.collect_once(now=NOW)
        elapsed = time.monotonic()-started
    assert elapsed < 2
    assert result["attempt_journal"] == {"status": "UNAVAILABLE", "reason_code": "ATTEMPT_JOURNAL_LOCKED"}
    assert service._worker["attempt_journal"] == result["attempt_journal"]
    assert result["symbols"]["SPX"]["forecasts"]["0"]["status"] == "RESEARCH_ONLY"


def test_missing_journal_not_created_and_get_limit_is_bounded(tmp_path):
    path = tmp_path/"absent.db"
    assert read_attempts(path)["status"] == "UNAVAILABLE"
    result = append_attempt(path, instance_id="a", sequence=1, attempted_at_utc=NOW.isoformat(), symbols=["SPX"])
    assert result["status"] == "UNAVAILABLE" and not path.exists()
    with pytest.raises(ValueError): read_attempts(path, limit=51)


def test_corrupt_attempt_remains_rejected_without_hiding_valid_receipts(service):
    result = append_attempt(service.journal_database, instance_id="ok", sequence=1,
        attempted_at_utc=NOW.isoformat(), symbols=["SPX"], state=capture_state())
    with sqlite3.connect(service.journal_database) as connection:
        for key, raw in (("malformed", "[]"), ("oversized", "x" * (256*1024+1))):
            connection.execute("INSERT INTO research_forecast_attempts VALUES (?,?,?,?,?,?)",
                (key, NOW.isoformat(), NOW.isoformat(), "CAPTURED", hashlib.sha256(raw.encode()).hexdigest(), raw))
    audit = read_attempts(service.journal_database)
    assert audit["status"] == "INCOMPLETE" and audit["rejected_receipts"] == 2
    assert audit["examined_receipts"] == 3
    assert len(audit["attempts"]) == 1 and audit["attempts"][0]["attempt_id"] == result["attempt_id"]
