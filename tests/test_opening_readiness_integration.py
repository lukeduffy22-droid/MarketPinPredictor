"""Real application caller paths for bounded opening diagnostics, offline only."""
from datetime import datetime, timezone
import json
from pathlib import Path

import databento_dbn as dbn
import pytest

import backend.databento_streamer as source_module
from backend.opening_readiness_timeline import BoundedOpeningTimelineWriter, OpeningReadinessTimeline
from tests.test_databento_iterator_startup import _sdk
from tests.test_opening_readiness_timeline import Clock, OPEN, OPEN_NS, mapping, quote, SYMBOLS
from tests.test_opening_transition_stress import september_14_open
from tests.test_orb_readiness_wakeup import (
    _install_real_scheduler, _clock_records, _quotes_and_handoff, _first_bucket_counts,
)


def _rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize("case", ["late_receipt", "reader_wait", "rejected"])
def test_actual_reader_records_distinct_ndx_boundaries(tmp_path, monkeypatch, case):
    clock = Clock()
    clock.set(0.5)
    monkeypatch.setattr(source_module, "DATA_DIR", tmp_path / "isolated-data")
    monkeypatch.setattr(source_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX", "NDX"])
    monkeypatch.setattr(source_module.time, "time_ns", lambda: clock.wall)
    monkeypatch.setattr(source_module.time, "monotonic_ns", lambda: clock.mono)
    streamer = source_module.DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.active_generation = 0
    streamer.api_key = "offline-test-only-not-a-key"
    streamer.live_symbols = list(SYMBOLS.values())
    streamer.is_running = True
    streamer.schema = "cbbo-1s"
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
        "cash_open_utc": OPEN.isoformat(),
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **_: None)
    monkeypatch.setattr(streamer, "_subscription_replay_start", lambda: OPEN.isoformat())
    monkeypatch.setattr(streamer, "_mark_stream_progress", lambda: None)

    def finished():
        if streamer.quote_records_seen >= 50:
            streamer.is_running = False
        return False
    monkeypatch.setattr(streamer, "_stream_stalled", finished)

    records = [mapping(i, "NDX" if i == 50 else "SPX") for i in range(1, 51)]
    records += [dbn.SystemMsg(ts_event=OPEN_NS, msg="Replay complete",
                             code=dbn.SystemCode.REPLAY_COMPLETED)]
    ndx_quote = quote(50, bid=3_000_000_000 if case == "rejected" else 1_000_000_000)
    heartbeat = dbn.SystemMsg(ts_event=OPEN_NS + 6 * 10**9, msg="Heartbeat",
                             code=dbn.SystemCode.HEARTBEAT)
    records += [quote(i) for i in range(1, 50)]
    if case != "late_receipt":
        records += [ndx_quote, heartbeat]
    with _sdk(records, close_after_burst=case != "late_receipt") as (client, queue, _trace, _cleanup):
        client.subscribe = lambda **_: None
        monkeypatch.setattr(source_module.db, "Live", lambda **_: client)
        protocol = client._session._protocol
        real_receive = protocol.received_record

        def advance(seconds):
            # Both SDK and reader stages share one monotonic virtual clock.
            clock.set(max((clock.wall - OPEN_NS) / 10**9, seconds))

        def receive(record):
            seconds = (
                5.4 if case == "late_receipt"
                and getattr(record, "instrument_id", None) == 50
                and hasattr(record, "bid_px_00") else 1.15
            )
            advance(seconds)
            return real_receive(record)
        monkeypatch.setattr(protocol, "received_record", receive)
        real_get = queue.get
        spx_consumed = 0
        late_injected = False

        def get(*args, **kwargs):
            nonlocal spx_consumed, late_injected
            if case == "late_receipt" and not late_injected and spx_consumed == 49:
                # SPX's initial batch has actually been processed by _run_live.
                # Only now does the in-memory provider deliver the sole NDX
                # quote through real SDK callbacks, before its next queue read.
                late_injected = True
                advance(5.4)
                protocol.received_record(ndx_quote)
                protocol.received_record(heartbeat)
                protocol.disconnected.set_result(None)
            record = real_get(*args, **kwargs)
            if hasattr(record, "bid_px_00"):
                if record.instrument_id != 50:
                    spx_consumed += 1
                    advance(1.3)
                elif case == "late_receipt":
                    advance(5.5)
                elif case == "reader_wait":
                    advance(4.0)
                else:
                    advance(1.3)
            return record
        monkeypatch.setattr(queue, "get", get)
        streamer._run_live()
        assert spx_consumed == 49
        assert late_injected is (case == "late_receipt")

    path = streamer.audit_dir.parent / "opening_readiness" / "2026-11-10.ndjson"
    rows = _rows(path)
    status = streamer._opening_timeline_writer.status()
    assert status["all_offered_events_acknowledged"], status
    assert streamer.quote_records_seen == 50
    assert streamer._processing_clock_telemetry()["sample_count"] == 50
    assert streamer._processing_clock_telemetry()["minimum_samples"] == 50
    ndx = [r for r in rows if r.get("family_at_observation") == "NDX"]
    received = next(r for r in ndx if r["kind"] == "first_live_sdk_quote")
    processed = next(r for r in ndx if r["kind"] == "first_reader_processed_quote")
    assert received["provider_interval_end_ns"] == OPEN_NS + 10**9
    assert processed["sdk_callback_received_at_ns"] == received["local_observed_at_ns"]
    if case == "late_receipt":
        assert received["local_observed_at_ns"] == OPEN_NS + 5_400_000_000
        assert processed["local_observed_at_ns"] == OPEN_NS + 5_500_000_000
        assert received["phase"] == "after_opening_bucket"
    elif case == "reader_wait":
        assert received["local_observed_at_ns"] == OPEN_NS + 1_150_000_000
        assert processed["local_observed_at_ns"] == OPEN_NS + 4_000_000_000
        assert processed["callback_to_reader_elapsed_ns"] == 2_850_000_000
    else:
        rejected = next(r for r in ndx if r["kind"] == "first_quote_rejection")
        assert rejected["details"]["reason"] == "CROSSED_QUOTE"
        assert streamer.handoff_status == "warming"
        assert "NDX" not in streamer._fresh_market_observations
        assert not any(r["kind"] == "first_cache_admitted_quote" for r in ndx)
    spx = [r for r in rows if r.get("family_at_observation") == "SPX"]
    spx_processed = next(r for r in spx if r["kind"] == "first_reader_processed_quote")
    spx_admitted = next(r for r in spx if r["kind"] == "first_cache_admitted_quote")
    assert spx_processed["local_observed_at_ns"] == OPEN_NS + 1_300_000_000
    assert spx_admitted["local_observed_at_ns"] == OPEN_NS + 1_300_000_000
    assert spx_admitted["local_observed_at_ns"] < OPEN_NS + 5 * 10**9
    assert spx_admitted["callback_to_stage_elapsed_ns"] == 150_000_000
    assert not any(r["kind"] == "qualified_quote_inputs" for r in rows)
    assert not any(r["kind"] == "orb_attempt_outcome" for r in rows)
    assert all(r.get("authority") == "DIAGNOSTIC_ONLY" for r in rows)
    assert streamer._opening_timeline_snapshot()["sdk_callback_registered"]
    # Preserve the real source-caller output in pytest's isolated artifact tree.
    (tmp_path / "Actual Reader Timeline.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    (tmp_path / "Actual Reader Writer Status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    (tmp_path / "Actual Reader Source Gate State.json").write_text(json.dumps({
        "case": case, "handoff_status": streamer.handoff_status,
        "processing_clock": streamer._processing_clock_telemetry(),
        "quote_records_seen": streamer.quote_records_seen,
        "cache_admitted_records": streamer.messages_received,
        "first_bucket_deadline_ns": OPEN_NS + 5 * 10**9,
        "qualified_pair_set_events": sum(r["kind"] == "qualified_quote_inputs" for r in rows),
        "orb_attempt_outcome_events": sum(r["kind"] == "orb_attempt_outcome" for r in rows),
        "scope": "actual reader/SDK paths with virtual offline timestamps; sampler not run",
    }, indent=2), encoding="utf-8")


def _attach_timeline(streamer, tmp_path, opening, clock):
    writer = BoundedOpeningTimelineWriter(tmp_path / "caller-timeline").start()
    observer = OpeningReadinessTimeline(
        writer, epoch_id=streamer.subscription_epoch_id, generation=7,
        cash_open_utc=opening,
        clock_ns=lambda: int(clock.utcnow().timestamp() * 10**9),
        monotonic_ns=lambda: int(clock.monotonic() * 10**9),
    )
    streamer._opening_timeline = observer
    streamer._opening_timeline_writer = writer
    streamer._opening_timeline_cash_open = opening
    return observer, writer


@pytest.mark.parametrize("ndx_ready", [True, False])
def test_real_sampler_logs_actual_cbbo_qualification_and_final_outcome(
    september_14_open, monkeypatch, tmp_path, ndx_ready,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, contract="cbbo")
    observer, writer = _attach_timeline(streamer, tmp_path, opening, clock)

    def ready():
        _clock_records(streamer, opening, 50)
        _quotes_and_handoff(streamer, "SPX", 10000)
        if ndx_ready:
            _quotes_and_handoff(streamer, "NDX", 20000)
    clock.schedule(4.8, "actual input readiness", ready)
    streamer._opening_reference_loop()
    streamer._audit_opening_timeline_dispatch(clock.utcnow())
    observer.retire("offline real sampler test complete")
    status = writer.close()
    assert status["all_offered_events_acknowledged"], status
    rows = _rows(tmp_path / "caller-timeline" / "2026-09-14.ndjson")
    outcomes = [r for r in rows if r["kind"] == "orb_attempt_outcome"]
    assert outcomes
    assert any(r["kind"] == "sampler_dispatch" for r in rows)
    if ndx_ready:
        assert _first_bucket_counts(engine, opening) == {"SPX": 1, "NDX": 1}
        assert {r["family_at_observation"] for r in rows
                if r["kind"] == "qualified_quote_inputs"} == {"SPX", "NDX"}
        accepted = [r for r in outcomes if r["details"]["accepted_final_decision"]]
        assert {r["family_at_observation"] for r in accepted} == {"SPX", "NDX"}
        assert all(r["details"]["progress_decision_recorded"] for r in accepted)
    else:
        assert _first_bucket_counts(engine, opening) == {}
        assert not any(r["kind"] == "qualified_quote_inputs" for r in rows)
        blocked = [r for r in outcomes if r["details"].get("reason") == "HANDOFF_NOT_ACTIVE"]
        assert {r["family_at_observation"] for r in blocked} == {"SPX", "NDX"}
        assert not any(r["details"]["accepted_final_decision"] for r in outcomes)
    assert all(not r.get("prediction_eligible") for r in rows)


def test_missing_dispatch_is_observed_without_inventing_an_upstream_cause(
    september_14_open, monkeypatch, tmp_path,
):
    streamer, journal, _engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening)
    observer, writer = _attach_timeline(streamer, tmp_path, opening, clock)
    clock.seconds = 5.2
    streamer._audit_opening_timeline_dispatch(clock.utcnow())
    streamer._audit_opening_timeline_dispatch(clock.utcnow())
    observer.retire("offline no-dispatch test")
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _rows(tmp_path / "caller-timeline" / "2026-09-14.ndjson")
    audits = [r for r in rows if r["kind"] == "first_bucket_dispatch_audit"]
    assert {r["family_at_observation"] for r in audits} == {"SPX", "NDX"}
    assert len(audits) == 2
    assert all(r["details"]["stage"] == "NO_DISPATCH_OBSERVED" for r in audits)
    assert all(r["details"]["upstream_cause"] == "UNKNOWN" for r in audits)
    assert not any(r["kind"] == "sampler_dispatch" for r in rows)
