"""Synthetic provider-schema rehearsal; this is not evidence of a live open."""
from datetime import timedelta

import databento_dbn as dbn
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DatabaseError

import backend.database as database
from backend.databento_streamer import DatabentoGammaStreamer, _record_timestamp_ns
from backend.quote_timing import CBBO_TIMING, UNDEF_TIMESTAMP
from tests.test_orb_e2e_rehearsal import (
    OPEN_UTC, TRADING_DATE, persistent_orb_journal, _build_streamer, _refresh_market_quotes,
)


def _record(instrument, recv, event=UNDEF_TIMESTAMP):
    return dbn.CBBOMsg(
        rtype=dbn.RType.CBBO_1S, publisher_id=1, instrument_id=instrument,
        ts_event=event, price=dbn.UNDEF_PRICE, size=0, side=dbn.Side.NONE,
        ts_recv=recv,
    )


def _quotes(streamer, at, event=None):
    _refresh_market_quotes(streamer, "SPX", at, 6500.0)
    recv = int(at.timestamp()) * 10**9
    for record in streamer._universe_records_by_market["SPX"]:
        quote = streamer.quotes[record["symbol"]]
        quote.update(timestamp_contract_version=CBBO_TIMING, quote_schema="cbbo-1s",
                     ts_event_ns=event, raw_ts_event_ns=UNDEF_TIMESTAMP if event is None else event,
                     ts_recv_ns=recv, ts_index_ns=recv)


@pytest.fixture
def cbbo_store(persistent_orb_journal, monkeypatch):
    journal, clock, engine = persistent_orb_journal
    database._ensure_orb_cbbo_reference_schema()
    database._ensure_orb_cbbo_reference_immutability()
    database._ensure_orb_cbbo_reference_decision_schema()
    database._ensure_orb_cbbo_reference_decision_immutability()
    streamer = _build_streamer(monkeypatch, epoch="c" * 64)
    streamer.is_running = True
    monkeypatch.setattr(streamer, "_processing_clock_telemetry",
                        DatabentoGammaStreamer._processing_clock_telemetry.__get__(streamer))
    recv = int((OPEN_UTC + timedelta(seconds=1)).timestamp()) * 10**9
    for i in range(50):
        assert streamer._record_processing_clock_sample(
            _record(i + 1, recv), processed_at_ns=recv + 50_000_000, generation=7)
    return streamer, journal, clock, engine


@pytest.mark.parametrize("last_trade", [None, -60, 0])
def test_cbbo_first_bucket_persists_without_requiring_a_new_trade(cbbo_store, last_trade):
    streamer, journal, clock, engine = cbbo_store
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    event = None if last_trade is None else int((OPEN_UTC + timedelta(seconds=last_trade)).timestamp()) * 10**9
    _quotes(streamer, observed, event)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)
    assert result["recorded"] and result["progress_eligible"], (result.get("reason"), result.get("opening_gate_evidence"))
    rows = database.load_orb_reference_samples("SPX", TRADING_DATE, as_of_utc=observed)
    assert len(rows) == 1
    assert rows[0]["timestamp_contract_version"] == CBBO_TIMING
    assert rows[0]["earliest_ts_event_ns"] == event
    assert rows[0]["trade_timestamp_count"] == (0 if event is None else 10)
    assert rows[0]["sample_timestamp_utc"] == OPEN_UTC.replace(tzinfo=None)
    assert len(database.load_orb_reference_snapshot_samples("SPX", TRADING_DATE, as_of_utc=observed)) == 1
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM orb_reference_samples")).scalar() == 0
        assert connection.execute(text("SELECT count(*) FROM orb_cbbo_reference_sample_decisions WHERE progress_eligible=1")).scalar() == 1
    for table in ("orb_cbbo_reference_samples", "orb_cbbo_reference_sample_decisions"):
        with pytest.raises(DatabaseError), engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {table}"))


def test_native_sdk_undefined_trade_preserves_the_interval_timestamp():
    recv = int((OPEN_UTC + timedelta(seconds=1)).timestamp()) * 10**9
    record = _record(1, recv)
    assert _record_timestamp_ns(record, "ts_event") is None
    assert _record_timestamp_ns(record, "ts_recv") == recv
    assert _record_timestamp_ns(record, "ts_index") == recv


def test_clock_counts_live_intervals_before_mapping_but_not_replay_or_duplicates(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"], subscription_epoch_id="a" * 64)
    streamer.active_generation = 7
    recv = int((OPEN_UTC + timedelta(seconds=1)).timestamp()) * 10**9
    for i in range(49):
        record = _record(i + 1, recv)
        assert streamer._record_processing_clock_sample(record, processed_at_ns=recv + 1, generation=7)
        assert not streamer._record_processing_clock_sample(record, processed_at_ns=recv + 1, generation=7)
    assert streamer._processing_clock_telemetry()["status"] == "unknown"
    assert streamer._processing_clock_telemetry()["reason"] == "INSUFFICIENT_LIVE_TIMING_SAMPLES"
    streamer._clock_replay_pending = True
    assert not streamer._record_processing_clock_sample(_record(50, recv), processed_at_ns=recv + 1, generation=7)
    streamer._clock_replay_pending = False
    assert not streamer._record_processing_clock_sample(_record(50, recv), processed_at_ns=recv + 1, generation=6)
    assert streamer._record_processing_clock_sample(_record(50, recv), processed_at_ns=recv + 1, generation=7)
    assert streamer._processing_clock_telemetry()["status"] == "synchronized"
    assert not streamer.quotes and not streamer.symbol_mappings
    streamer._reset_stream_telemetry()
    for i in range(50):
        assert streamer._record_processing_clock_sample(_record(i + 1, recv), processed_at_ns=recv - 100_000_000, generation=7)
    assert streamer._processing_clock_telemetry()["status"] == "unsynchronized"


@pytest.mark.parametrize("bad", ["old_interval", "future_interval", "old_trade", "unknown_contract"])
def test_cbbo_invalid_candidates_still_fail_closed(cbbo_store, bad):
    streamer, journal, clock, engine = cbbo_store
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    for quote in streamer.quotes.values():
        if bad == "unknown_contract":
            quote["timestamp_contract_version"] = "unknown"
        elif bad == "old_trade":
            quote["ts_event_ns"] = int((OPEN_UTC - timedelta(days=1)).timestamp()) * 10**9
        else:
            offset = -86400 if bad == "old_interval" else 5
            quote["ts_recv_ns"] += offset * 10**9
            quote["ts_index_ns"] = quote["ts_recv_ns"]
    result = streamer._capture_opening_reference_for_bucket("SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)
    assert not result["recorded"]
    assert result["reason"] == "COMPLETE_PAIR_MINIMUM_NOT_MET"


def test_full_hour_cbbo_windows_and_legacy_contract_do_not_merge(cbbo_store):
    streamer, journal, clock, engine = cbbo_store
    for i in range(720):
        bucket = OPEN_UTC + timedelta(seconds=5*i)
        observed = bucket + timedelta(seconds=1, milliseconds=100)
        _quotes(streamer, observed)
        result = streamer._capture_opening_reference_for_bucket("SPX", bucket, now_utc=lambda: observed, journal=journal)
        assert result["progress_eligible"], (result.get("reason"), result.get("opening_gate_evidence"))
    clock["now"] = OPEN_UTC + timedelta(minutes=60)
    state = journal.snapshot("SPX", as_of_utc=clock["now"])
    for name, count in (("5m", 60), ("15m", 180), ("30m", 360), ("60m", 720)):
        window = state["opening_ranges"][name]
        assert window["capture_status"] == "complete"
        assert window["capture_evidence"]["sample_count"] == count
        assert window["capture_evidence"]["expected_sample_count"] == count
    # A distinct legacy contract in the same bucket is retained, not silently
    # considered interchangeable with the new interval contract.
    observed = OPEN_UTC + timedelta(seconds=2)
    _refresh_market_quotes(streamer, "SPX", observed, 6500.0)
    result = streamer._capture_opening_reference_for_bucket("SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)
    assert result["recorded"], (result.get("reason"), result.get("opening_gate_evidence"))
    state = journal.snapshot("SPX", as_of_utc=clock["now"])
    assert state["opening_ranges"]["5m"]["capture_status"] == "partial"


def test_automatic_first_bucket_check_retains_hashed_readback_receipt(cbbo_store, tmp_path):
    import hashlib
    import json
    from pathlib import Path
    from backend.opening_capture_receipts import capture_receipt, due_checks
    streamer, journal, clock, engine = cbbo_store
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    assert streamer._capture_opening_reference_for_bucket(
        "SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)["progress_eligible"]
    at = OPEN_UTC + timedelta(seconds=5, milliseconds=100)
    key = next(iter(due_checks(at, set(), "c" * 64, 7)))
    result = capture_receipt(journal=journal, symbols=("SPX", "NDX"), key=key,
                             observed_at=at, context_is_current=lambda: True,
                             destination=tmp_path)
    assert result["symbols"]["SPX"]["passed"] is True
    first = result["symbols"]["SPX"]["capture_evidence"]["first_bucket_receipts"][0]
    assert first["decision_timestamp_utc"] == observed.isoformat().replace("+00:00", "Z")
    assert first["processing_clock_status"] == "synchronized"
    assert len(first["sample_id"]) == 64
    assert result["symbols"]["NDX"]["passed"] is False
    assert result["symbols"]["NDX"]["full_bucket_coverage"] is False
    receipt_path = Path(result["path"])
    raw = receipt_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == result["sha256"]
    assert json.loads(raw)["observed_at_utc"] == at.isoformat()
    assert not list(due_checks(at, {key}, "c" * 64, 7))
    assert not list(due_checks(OPEN_UTC + timedelta(seconds=4), set(), "c" * 64, 7))
    changed = capture_receipt(journal=journal, symbols=("SPX",), key=key,
                              observed_at=at, context_is_current=lambda: False,
                              destination=tmp_path)
    assert changed["symbols"]["SPX"]["classification"] == "NOT YET VERIFIED"
    assert changed["symbols"]["SPX"]["passed"] is False


def test_cbbo_late_commit_is_retained_without_opening_authority(cbbo_store, monkeypatch):
    streamer, journal, clock, engine = cbbo_store
    clock['now'] = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, clock['now'])
    original = journal.record_reference
    def delayed(payload):
        result = original(payload)
        clock['now'] = OPEN_UTC + timedelta(seconds=5, milliseconds=100)
        return result
    monkeypatch.setattr(journal, 'record_reference', delayed)
    result = streamer._capture_opening_reference_for_bucket(
        'SPX', OPEN_UTC, now_utc=lambda: clock['now'], journal=journal)
    assert result['recorded'] and not result['progress_eligible']
    assert result['reason'] == 'REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET'
    with engine.connect() as connection:
        assert connection.execute(text('SELECT count(*) FROM orb_cbbo_reference_samples')).scalar() == 1
        assert connection.execute(text('SELECT progress_eligible FROM orb_cbbo_reference_sample_decisions')).scalar() == 0
    assert not database.load_orb_reference_samples('SPX', TRADING_DATE)
    assert not database.load_orb_reference_snapshot_samples('SPX', TRADING_DATE)


def test_additive_migration_and_restart_preserve_legacy_evidence(persistent_orb_journal, monkeypatch):
    import hashlib
    journal, clock, engine = persistent_orb_journal
    streamer = _build_streamer(monkeypatch, epoch='d' * 64)
    streamer.is_running = True
    observed = OPEN_UTC + timedelta(milliseconds=100)
    _refresh_market_quotes(streamer, 'SPX', observed, 6500.)
    assert streamer._capture_opening_reference_for_bucket(
        'SPX', OPEN_UTC, now_utc=lambda: observed, journal=journal)['progress_eligible']
    database.OrbCbboReferenceSampleDecision.__table__.drop(engine)
    database.OrbCbboReferenceSample.__table__.drop(engine)
    def legacy_digest():
        with engine.connect() as connection:
            evidence = [connection.execute(text(f'SELECT * FROM {table} ORDER BY sample_id')).all()
                        for table in ('orb_reference_samples', 'orb_reference_sample_decisions')]
            guards = connection.execute(text("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name IN ('orb_reference_samples','orb_reference_sample_decisions') ORDER BY name")).all()
            return hashlib.sha256(repr((evidence, guards)).encode()).hexdigest()
    before = legacy_digest()
    for _ in range(2):
        database.init_db()
        assert legacy_digest() == before
        assert len(database.load_orb_reference_samples('SPX', TRADING_DATE)) == 1
        with engine.connect() as connection:
            assert connection.execute(text('SELECT count(*) FROM orb_cbbo_reference_samples')).scalar() == 0
            fk = connection.execute(text('PRAGMA foreign_key_list(orb_cbbo_reference_sample_decisions)')).first()
            assert fk[2] == 'orb_cbbo_reference_samples'


def test_new_store_inspector_uses_final_decisions_and_detects_missing_guard(cbbo_store):
    import sqlite3
    from tools.inspect_opening_capture import _inspect_cbbo_reference_store
    streamer, journal, clock, engine = cbbo_store
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    assert streamer._capture_opening_reference_for_bucket(
        'SPX', OPEN_UTC, now_utc=lambda: observed, journal=journal)['progress_eligible']
    raw = engine.raw_connection()
    try:
        connection = raw.driver_connection
        connection.row_factory = sqlite3.Row
        store, rows, decisions = _inspect_cbbo_reference_store(connection, TRADING_DATE)
        assert store['ready'] and len(rows) == 1
        assert decisions['SPX']['eligible_decision_count'] == 1
        assert rows[0]['timestamp_contract_version'] == CBBO_TIMING
        connection.execute('DROP TRIGGER orb_cbbo_reference_samples_no_delete')
        store, rows, decisions = _inspect_cbbo_reference_store(connection, TRADING_DATE)
        assert not store['ready'] and not rows
    finally:
        raw.close()
