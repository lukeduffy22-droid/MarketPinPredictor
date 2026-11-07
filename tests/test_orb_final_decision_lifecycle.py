"""Offline lifecycle regression: committed raw inputs need a terminal decision.

Synthetic September 14 CBBO inputs and an isolated real SQLite/WAL journal.
"""
from datetime import timedelta
import hashlib
import json
import sqlite3
import threading
import time

import pytest
from sqlalchemy import text

import backend.database as database
import backend.databento_streamer as stream_module
from backend.quote_timing import CBBO_TIMING
from tests.test_opening_transition_stress import (
    september_14_open, _publish_mappings_and_quotes,
)
from tests.test_orb_cbbo_capture import _record


@pytest.fixture
def ready_lifecycle(september_14_open, monkeypatch):
    streamer, journal, engine, opening = september_14_open
    # Use the exact real saver so caller-owned budgets reach SQLite rather than
    # the legacy stress fixture's unbudgeted thread-observation wrapper.
    monkeypatch.setattr(journal, "reference_saver", database.save_orb_reference_sample)
    database._ensure_orb_cbbo_reference_schema()
    database._ensure_orb_cbbo_reference_immutability()
    database._ensure_orb_cbbo_reference_decision_schema()
    database._ensure_orb_cbbo_reference_decision_immutability()
    streamer.schema = "cbbo-1s"
    received_ns = int((opening + timedelta(seconds=1)).timestamp()) * 10**9
    for instrument in range(stream_module.CLOCK_SYNC_MIN_SAMPLES):
        assert streamer._record_processing_clock_sample(
            _record(instrument + 1, received_ns),
            processed_at_ns=received_ns + 20_000_000, generation=7,
        )
    assert streamer._processing_clock_telemetry()["status"] == "synchronized"
    for index, market in enumerate(("SPX", "NDX")):
        received = _publish_mappings_and_quotes(
            streamer, market, first_instrument_id=10_000 + index * 10_000,
        )
        for item in streamer._universe_records_by_market[market]:
            quote = streamer.quotes[item["symbol"]]
            quote.update(
                timestamp_contract_version=CBBO_TIMING, quote_schema="cbbo-1s",
                ts_recv_ns=received_ns, ts_index_ns=received_ns,
            )
        streamer._record_fresh_market_and_maybe_activate(
            market, generation=7, received_monotonic=received,
        )
    assert streamer.handoff_status == "active"
    clock = {"now": opening + timedelta(seconds=1, milliseconds=250)}
    return streamer, journal, engine, opening, clock


def _raw_fingerprint(engine):
    with engine.connect() as connection:
        rows = connection.execute(text(
            "SELECT * FROM orb_cbbo_reference_samples ORDER BY sample_id"
        )).mappings().all()
    encoded = json.dumps([dict(row) for row in rows], sort_keys=True, default=str).encode()
    return len(rows), hashlib.sha256(encoded).hexdigest()


def _decisions(engine):
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(
            "SELECT * FROM orb_cbbo_reference_sample_decisions ORDER BY sample_id"
        )).mappings()]


def test_failure_receipt_retains_pending_raw_witness_and_cleanup_diagnostics(tmp_path):
    streamer = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.audit_dir = tmp_path / "logs" / "audit"
    from datetime import datetime, timezone
    intended = datetime(2026, 9, 14, 13, 30, tzinfo=timezone.utc)
    result = {
        "recorded": True, "progress_eligible": False,
        "sample_id": "a" * 64,
        "reason": "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED",
        "progress_decision_pending": True, "progress_decision_recorded": False,
        "progress_decision_cleanup_attempted": True,
        "progress_decision_cleanup_timeout_seconds": .25,
        "progress_decision_cleanup_elapsed_seconds": .251,
        "progress_decision_cleanup_error": "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED",
    }
    expected = dict(result)
    streamer._record_opening_reference_failed_attempt(
        market="SPX", subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=7, intended_bucket_utc=intended, result=result,
    )
    receipt = tmp_path / "logs" / "opening_reference_attempts" / "2026-09-14.ndjson"
    saved = json.loads(receipt.read_text().splitlines()[0])
    for key in expected:
        assert saved[key] == expected[key]
    assert result["failure_evidence_recorded"] is True
    assert saved["usable_for_prediction"] is False


@pytest.mark.parametrize("market", ["SPX", "NDX"])
@pytest.mark.parametrize("expiry", ["bucket_close", "cancel_after_raw_commit"])
def test_raw_commit_then_capture_expiry_appends_one_ineligible_decision(
    ready_lifecycle, monkeypatch, market, expiry,
):
    streamer, journal, engine, opening, clock = ready_lifecycle
    cancelled = threading.Event()
    real_record = journal.record_reference
    raw_before = []

    def commit_then_expire(payload):
        result = real_record(payload)
        assert result["recorded"] is True, result.get("reason")
        raw_before.append(_raw_fingerprint(engine))
        assert raw_before[-1][0] == 1
        if expiry == "bucket_close":
            clock["now"] = opening + timedelta(seconds=5, milliseconds=10)
        else:
            cancelled.set()
        return result

    monkeypatch.setattr(journal, "record_reference", commit_then_expire)
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=lambda: clock["now"], journal=journal,
        attempt_cancel_event=cancelled,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert _raw_fingerprint(engine) == raw_before[0]
    decisions = _decisions(engine)
    assert len(decisions) == 1, result
    decision = decisions[0]
    assert decision["sample_id"] == result["sample_id"]
    assert decision["decision_status"] == "final"
    assert decision["progress_eligible"] == 0
    assert decision["reason"] in {
        "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED",
        "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET",
    }
    assert result["progress_decision_recorded"] is True
    assert result["progress_decision_pending"] is False
    assert database.load_orb_reference_samples(
        market, opening.date(), as_of_utc=opening + timedelta(seconds=10),
    ) == []
    # Finalization replay is immutable/idempotent; it must not add a second row.
    assert database.save_orb_reference_sample_decision({
        **decision, "progress_eligible": bool(decision["progress_eligible"]),
    }) is not None
    assert _decisions(engine) == decisions
    assert _raw_fingerprint(engine) == raw_before[0]


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_post_raw_validation_exception_keeps_witness_and_negative_final_decision(
    ready_lifecycle, monkeypatch, market,
):
    streamer, journal, engine, opening, clock = ready_lifecycle
    real_record = journal.record_reference
    real_context = streamer._opening_reference_context_is_current
    raw_before = []

    def record_then_mark_committed(payload):
        result = real_record(payload)
        assert result["recorded"] is True, result.get("reason")
        raw_before.append(_raw_fingerprint(engine))
        return result

    def fail_only_after_raw_commit(snapshot):
        if raw_before:
            raise RuntimeError("offline post-commit validation failure")
        return real_context(snapshot)

    monkeypatch.setattr(journal, "record_reference", record_then_mark_committed)
    monkeypatch.setattr(streamer, "_opening_reference_context_is_current", fail_only_after_raw_commit)
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=lambda: clock["now"], journal=journal,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["reason"] == "REFERENCE_POST_PERSIST_VALIDATION_FAILED"
    assert result["progress_decision_recorded"] is True
    assert result["progress_decision_pending"] is False
    assert len(raw_before) == 1
    assert _raw_fingerprint(engine) == raw_before[0]
    decisions = _decisions(engine)
    assert len(decisions) == 1
    assert decisions[0]["sample_id"] == result["sample_id"]
    assert decisions[0]["progress_eligible"] == 0
    assert decisions[0]["reason"] == "REFERENCE_POST_PERSIST_VALIDATION_FAILED"
    assert database.load_orb_reference_samples(
        market, opening.date(), as_of_utc=opening + timedelta(seconds=10),
    ) == []


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_clock_callback_failure_after_raw_commit_preserves_pending_witness(
    ready_lifecycle, monkeypatch, market,
):
    streamer, journal, engine, opening, clock = ready_lifecycle
    real_record = journal.record_reference
    raw_before = []

    def record_then_fail_clock(payload):
        result = real_record(payload)
        assert result["recorded"] is True, result.get("reason")
        raw_before.append(_raw_fingerprint(engine))
        return result

    def now_until_raw_commit():
        if raw_before:
            raise RuntimeError("offline post-commit clock callback failure")
        return clock["now"]

    monkeypatch.setattr(journal, "record_reference", record_then_fail_clock)
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=now_until_raw_commit, journal=journal,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["progress_decision_pending"] is True
    assert result["progress_decision_recorded"] is False
    assert result["progress_decision_cleanup_error"] == "REFERENCE_DECISION_TIMESTAMP_UNAVAILABLE"
    assert result["timing_metadata_unavailable"] is True
    assert "progress_decided_at_utc" not in result
    assert len(raw_before) == 1
    assert _raw_fingerprint(engine) == raw_before[0]
    assert _decisions(engine) == []
    assert database.load_orb_reference_samples(
        market, opening.date(), as_of_utc=opening + timedelta(seconds=10),
    ) == []


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_sqlite_busy_after_raw_commit_stays_pending_bounded_and_ineligible(
    ready_lifecycle, monkeypatch, market,
):
    streamer, journal, engine, opening, clock = ready_lifecycle
    real_record = journal.record_reference
    blockers = []
    raw_before = []

    def commit_then_hold_writer(payload):
        result = real_record(payload)
        assert result["recorded"] is True, result.get("reason")
        raw_before.append(_raw_fingerprint(engine))
        connection = sqlite3.connect(engine.url.database, timeout=0)
        connection.execute("BEGIN IMMEDIATE")
        blockers.append(connection)
        return result

    monkeypatch.setattr(journal, "record_reference", commit_then_hold_writer)
    started = time.monotonic()
    try:
        result = streamer._capture_opening_reference_for_bucket(
            market, opening, now_utc=lambda: clock["now"], journal=journal,
            attempt_deadline_monotonic=time.monotonic() + .25,
        )
        assert time.monotonic() - started < 1.5
        assert result["recorded"] is True
        assert result["progress_eligible"] is False
        assert result["progress_decision_pending"] is True
        assert result["progress_decision_recorded"] is False
        assert _decisions(engine) == []
    finally:
        for connection in blockers:
            connection.rollback()
            connection.close()
    # No deferred writer should later turn a rejected call into persistence.
    assert _decisions(engine) == []
    assert _raw_fingerprint(engine) == raw_before[0]


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_cancellation_after_successful_final_commit_preserves_historical_decision(
    ready_lifecycle, monkeypatch, market,
):
    streamer, journal, engine, opening, clock = ready_lifecycle
    cancelled = threading.Event()
    real_decision = journal.record_reference_decision
    preserved = []

    def decide_then_cancel(**options):
        result = real_decision(**options)
        assert result["recorded"] is True, result.get("reason")
        preserved.append(_decisions(engine))
        cancelled.set()
        return result

    monkeypatch.setattr(journal, "record_reference_decision", decide_then_cancel)
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=lambda: clock["now"], journal=journal,
        attempt_cancel_event=cancelled,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["progress_decision_recorded"] is True
    assert result["progress_decision_pending"] is False
    assert _decisions(engine) == preserved[0]
    assert len(preserved[0]) == 1
    assert preserved[0][0]["progress_eligible"] == 1
