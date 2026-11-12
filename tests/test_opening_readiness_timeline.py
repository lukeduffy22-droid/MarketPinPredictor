"""Actual observer/writer emissions; virtual timestamps are offline fixtures."""
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import threading
import time

import databento_dbn as dbn
import pytest

import backend.databento_streamer as stream_module
from backend.opening_readiness_timeline import (
    BoundedOpeningTimelineWriter, OpeningReadinessTimeline, UNKNOWN_UPSTREAM,
)
from tests.test_databento_iterator_startup import _sdk, _start


OPEN = datetime(2026, 11, 10, 14, 30, tzinfo=timezone.utc)
OPEN_NS = int(OPEN.timestamp()) * 10**9
SYMBOLS = {"SPX": "SPXW  261110C07350000", "NDX": "NDXP  261110C26000000"}


class Clock:
    def __init__(self):
        self.set(-1)

    def set(self, seconds):
        self.wall = OPEN_NS + int(seconds * 10**9)
        self.mono = 10**12 + int(seconds * 10**9)


def quote(iid, *, bid=1_000_000_000, ask=2_000_000_000, provider_seconds=1):
    return dbn.CBBOMsg(
        rtype=dbn.RType.CBBO_1S, publisher_id=1, instrument_id=iid,
        ts_event=dbn.UNDEF_TIMESTAMP, price=dbn.UNDEF_PRICE, size=0,
        side=dbn.Side.NONE, ts_recv=OPEN_NS + provider_seconds * 10**9,
        levels=dbn.ConsolidatedBidAskPair(bid_px=bid, ask_px=ask),
    )


def mapping(iid, family):
    return dbn.SymbolMappingMsg(
        publisher_id=1, instrument_id=iid, ts_event=OPEN_NS - 10**9,
        stype_in=dbn.SType.RAW_SYMBOL, stype_in_symbol=SYMBOLS[family],
        stype_out=dbn.SType.RAW_SYMBOL, stype_out_symbol=SYMBOLS[family],
        start_ts=OPEN_NS - 3600 * 10**9, end_ts=OPEN_NS + 86400 * 10**9,
    )


def observer(tmp_path, clock, **options):
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline")
    writer.start()
    ledger = OpeningReadinessTimeline(
        writer, epoch_id="offline-sdk-fixture", generation=7, cash_open_utc=OPEN,
        clock_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono, **options,
    )
    return ledger, writer


def events(tmp_path):
    return [
        json.loads(line) for line in
        (tmp_path / "timeline" / "2026-11-10.ndjson").read_text().splitlines()
    ]


@pytest.mark.parametrize(("case", "name"), [
    ("late_receipt", "SPX Ready - NDX Arrives Later.ndjson"),
    ("reader_queue", "SPX Ready - NDX Waits in Reader Queue.ndjson"),
    ("rejection", "SPX Ready - NDX Rejected.ndjson"),
])
def test_emit_distinguishable_real_sdk_callback_examples(tmp_path, monkeypatch, case, name):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX", "NDX"])
    source = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    source.active_generation = 7
    source.subscription_cutoff_monotonic = time.monotonic() - 1
    source.handoff_status = "warming"
    source._opening_readiness_timeline = None
    first = quote(1)
    second = quote(2, bid=3_000_000_000 if case == "rejection" else 1_000_000_000)

    def process(record, family):
        ledger.processed(record, family)
        bid = stream_module.normalize_price(record.bid_px_00)
        ask = stream_module.normalize_price(record.ask_px_00)
        if bid is None or ask is None or ask < bid:
            reason = "CROSSED_QUOTE" if bid is not None and ask is not None and ask < bid else "INVALID_QUOTE_PRICE"
            ledger.rejected(record, family, reason)
            return
        source._store_quote(SYMBOLS[family], {
            "generation": 7, "received_monotonic": time.monotonic(),
            "bid": bid, "ask": ask,
        })
        ledger.cache_admitted(record, family)
        source._record_fresh_market_and_maybe_activate(
            family, generation=7, received_monotonic=time.monotonic(),
        )
        ledger.qualified(family, stage="normalized_quote_prices",
                         details={"pair_set_evaluation": "NOT_EVALUATED",
                                  "semantics": "price fields valid; no ORB eligibility inferred"})

    clock.set(1.10)
    with _sdk([mapping(1, "SPX"), first]) as (client, _queue, _trace, _cleanup):
        # Actual public SDK registration, before actual Live.__iter__/start.
        client.add_callback(ledger.sdk_callback)
        iterator = _start(client)
        assert isinstance(next(iterator), dbn.SymbolMappingMsg)
        assert next(iterator).instrument_id == 1
        clock.set(1.20)
        process(first, "SPX")
        ledger.milestone("shared_handoff", details={
            "status": source.handoff_status, "missing_required_families": ["NDX"],
        })
        clock.set(5.40 if case == "late_receipt" else 1.15)
        client._session._protocol.received_record(mapping(2, "NDX"))
        client._session._protocol.received_record(second)
        clock.set(5.50 if case == "late_receipt" else 4.00 if case == "reader_queue" else 1.30)
        assert isinstance(next(iterator), dbn.SymbolMappingMsg)
        assert next(iterator).instrument_id == 2
        process(second, "NDX")
        ledger.milestone("shared_handoff", details={
            "status": source.handoff_status,
            "missing_required_families": [] if source.handoff_status == "active" else ["NDX"],
        })
        ledger.milestone("orb_outcome", family="SPX", details={
            "status": "NOT_ATTEMPTED_IN_THIS_SDK_OBSERVER_TEST",
            "recorded": False, "progress_eligible": False,
            "qualified_pair_set": "NOT_EVALUATED",
            "first_bucket_deadline_ns": OPEN_NS + 5 * 10**9,
        })
        ledger.milestone("orb_outcome", family="NDX", details={
            "status": "NOT_ATTEMPTED_IN_THIS_SDK_OBSERVER_TEST",
            "recorded": False, "progress_eligible": False,
            "qualified_pair_set": "NOT_EVALUATED",
            "first_bucket_deadline_ns": OPEN_NS + 5 * 10**9,
        })
        assert list(iterator) == []
    ledger.retire("offline fixture complete")
    status = writer.close()
    assert status["all_offered_events_acknowledged"], status
    rows = events(tmp_path)
    ndx_receipt = next(row for row in rows if row["kind"] == "first_live_sdk_quote" and row["family_at_observation"] == "NDX")
    ndx_processing = next(row for row in rows if row["kind"] == "first_reader_processed_quote" and row["family_at_observation"] == "NDX")
    assert ndx_receipt["provider_interval_end_ns"] == OPEN_NS + 10**9
    assert ndx_receipt["upstream_timing"] == UNKNOWN_UPSTREAM
    assert ndx_processing["sdk_callback_receipt_status"] == "OBSERVED_EVENT_RETAINED"
    if case == "late_receipt":
        assert ndx_receipt["sdk_callback_received_at_ns"] == OPEN_NS + 5_400_000_000
        assert ndx_receipt["phase"] == "after_opening_bucket"
        assert ndx_receipt["provider_interval_phase"] == "opening_bucket"
        assert ndx_processing["callback_to_reader_elapsed_ns"] == 100_000_000
    elif case == "reader_queue":
        assert ndx_receipt["sdk_callback_received_at_ns"] == OPEN_NS + 1_150_000_000
        assert ndx_processing["callback_to_reader_elapsed_ns"] == 2_850_000_000
    else:
        assert next(row for row in rows if row["kind"] == "first_quote_rejection")["details"]["reason"] == "CROSSED_QUOTE"
        assert source.handoff_status == "warming"
    assert not any(row.get("prediction_eligible") for row in rows)
    # Copy byte-for-byte real writer output, not generated example prose.
    shutil.copyfile(tmp_path / "timeline" / "2026-11-10.ndjson", tmp_path / name)
    (tmp_path / (name + ".status.json")).write_text(json.dumps(status, indent=2))
    assert ledger.snapshot()["callback_errors"] == 0


def test_delayed_mapping_links_validity_without_rewriting_unknown_receipt(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    clock.set(0.5)
    record = quote(99)
    ledger.sdk_callback(record)
    clock.set(1.5)
    ledger.mapped(99, SYMBOLS["NDX"], mapping_start_ns=OPEN_NS,
                  mapping_end_ns=OPEN_NS + 2 * 10**9, mapping_version="original")
    ledger.mapped(99, SYMBOLS["NDX"], mapping_start_ns=0,
                  mapping_end_ns=1, mapping_version="later")
    writer.close()
    rows = events(tmp_path)
    receipt = next(row for row in rows if row["kind"] == "first_live_sdk_quote")
    link = next(row for row in rows if row["kind"] == "reader_mapping_processed")["details"]["linked_receipts"][0]
    assert receipt["family_at_observation"] == "UNKNOWN"
    assert receipt["sdk_callback_received_at_ns"] == OPEN_NS + 500_000_000
    assert link["original_family_at_receipt"] == "UNKNOWN"
    assert link["mapping_version"] == "original"
    assert link["link_status"] == "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY"
    assert len([r for r in rows if r["kind"] == "reader_mapping_processed"]) == 1


@pytest.mark.parametrize(("start", "end", "expected"), [
    (None, None, "CANDIDATE_MAPPING_VALIDITY_UNKNOWN"),
    (0, 1, "REJECTED_PROVIDER_INTERVAL_OUTSIDE_MAPPING_VALIDITY"),
])
def test_uncertain_or_invalid_mapping_is_never_an_authoritative_link(tmp_path, start, end, expected):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    clock.set(1.2)
    ledger.sdk_callback(quote(99))
    ledger.mapped(99, SYMBOLS["NDX"], mapping_start_ns=start, mapping_end_ns=end)
    writer.close()
    link = next(r for r in events(tmp_path) if r["kind"] == "reader_mapping_processed")["details"]["linked_receipts"][0]
    assert link["link_status"] == expected


def test_preopen_and_opening_first_live_receipts_are_separate(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    for at in (-1, 1.2, 5.5):
        clock.set(at)
        ledger.sdk_callback(mapping(1, "SPX"))
        ledger.sdk_callback(quote(1))
    ledger.retire("reconnect")
    frozen = ledger.snapshot()
    ledger.sdk_callback(quote(2))
    assert ledger.snapshot()["emitted_first_event_keys"] == frozen["emitted_first_event_keys"]
    writer.close()
    receipts = [r for r in events(tmp_path) if r["kind"] == "first_live_sdk_quote"]
    assert [r["phase"] for r in receipts] == ["preopen", "opening_bucket", "after_opening_bucket"]
    assert any(r["kind"] == "generation_retired" for r in events(tmp_path))


def test_monotonic_queue_delay_survives_wall_clock_step(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    clock.set(1.5)
    ledger.sdk_callback(mapping(1, "SPX"))
    record = quote(1)
    ledger.sdk_callback(record)
    clock.wall -= 10**9
    clock.mono += 100_000_000
    ledger.processed(record, "SPX")
    writer.close()
    row = next(r for r in events(tmp_path) if r["kind"] == "first_reader_processed_quote")
    assert row["callback_to_reader_elapsed_ns"] == 100_000_000
    assert row["elapsed_time_status"] == "MONOTONIC"


def test_overflow_preserves_reserved_control_and_reports_missing_events(tmp_path):
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline", max_pending=1, critical_capacity=2)
    event = {"trading_date": "2026-11-10", "kind": "ordinary"}
    assert writer.offer(event)
    assert not writer.offer(event)
    assert writer.offer({"trading_date": "2026-11-10", "kind": "SPX_control"}, critical=True)
    writer.start()
    status = writer.close()
    rows = events(tmp_path)
    assert any(r["kind"] == "SPX_control" for r in rows)
    assert any(r["kind"] == "writer_overflow" for r in rows)
    assert status["dropped_events"] == 1
    assert status["all_offered_events_acknowledged"] is False


def test_failed_write_is_unknown_and_never_acknowledged_as_durable(tmp_path):
    def failed_write(_rows):
        raise OSError("offline simulated disk failure")
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline", write_batch=failed_write).start()
    writer.offer({"trading_date": "2026-11-10", "kind": "test"}, critical=True)
    status = writer.flush()
    writer.close()
    assert status["write_failures"] == 1
    assert status["write_outcome_unknown_events"] == 1
    assert status["all_offered_events_acknowledged"] is False
    assert status["last_write_error_type"] == "OSError"
    assert not (tmp_path / "timeline").exists()


def test_burst_is_bounded_and_known_family_control_survives_regular_event_cap(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock, max_instruments=8, max_events=2)
    clock.set(1.2)
    for iid in range(100):
        ledger.sdk_callback(quote(iid + 1))
    ledger.milestone("shared_handoff", details={"status": "warming", "missing": ["NDX"]})
    ledger.rejected(quote(1), "NDX", "CROSSED_QUOTE")
    ledger.retire("offline reconnect")
    writer.close()
    state = ledger.snapshot()
    assert state["retained_instrument_phase_receipts"] <= 8
    assert state["observer_overflow"] > 0
    rows = events(tmp_path)
    assert any(row["kind"] == "shared_handoff" for row in rows)
    assert any(row["kind"] == "first_quote_rejection" and row["family_at_observation"] == "NDX" for row in rows)
    assert any(row["kind"] == "generation_retired" for row in rows)
    assert len(ledger._counts) <= ledger.max_events + 256



def test_known_receipt_is_not_relabelled_unknown_and_coalesced_receipt_is_observed(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    clock.set(1.2)
    ledger.sdk_callback(mapping(1, "SPX"))
    ledger.sdk_callback(mapping(2, "SPX"))
    ledger.sdk_callback(quote(1))
    second = quote(2)
    ledger.sdk_callback(second)  # Same family first event is coalesced.
    clock.set(1.3)
    ledger.mapped(1, SYMBOLS["SPX"], family="SPX",
                  mapping_start_ns=OPEN_NS, mapping_end_ns=OPEN_NS + 86400 * 10**9)
    ledger.processed(second, "SPX")
    writer.close()
    rows = events(tmp_path)
    processed = next(r for r in rows if r["kind"] == "first_reader_processed_quote")
    assert processed["sdk_callback_receipt_status"] == "OBSERVED_NO_SEPARATE_EVENT"
    assert processed["sdk_callback_received_at_ns"] == OPEN_NS + 1_200_000_000
    mapping_row = next(r for r in rows if r["kind"] == "reader_mapping_processed")
    assert mapping_row["details"]["linked_receipts"] == []


def test_first_replay_and_first_live_callback_are_separate(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock, replay_pending=True)
    clock.set(1.1)
    ledger.sdk_callback(mapping(1, "SPX"))
    ledger.sdk_callback(quote(1))
    ledger.sdk_callback(dbn.SystemMsg(ts_event=OPEN_NS, msg="Replay complete",
                                    code=dbn.SystemCode.REPLAY_COMPLETED))
    ledger.sdk_callback(quote(1))
    writer.close()
    rows = events(tmp_path)
    assert len([r for r in rows if r["kind"] == "first_replay_sdk_quote"]) == 1
    assert len([r for r in rows if r["kind"] == "first_live_sdk_quote"]) == 1
    assert any(r["kind"] == "sdk_replay_completed" for r in rows)


def test_later_success_never_acknowledges_an_earlier_unknown_write_gap(tmp_path):
    calls = []

    def write(rows):
        calls.append(rows)
        if len(calls) == 1:
            raise OSError("offline first batch unknown")
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline", batch_size=1, write_batch=write).start()
    writer.offer({"trading_date": "2026-11-10", "kind": "first"}, critical=True)
    first = writer.flush()
    assert first["write_failures"] == 1
    writer.offer({"trading_date": "2026-11-10", "kind": "second"}, critical=True)
    final = writer.flush()
    writer.close()
    assert final["completed_write_attempt_order"] == 2
    assert final["last_successful_batch_order"] == 2
    assert final["fsync_acknowledged_order"] == 0
    assert final["write_outcome_unknown_events"] == 1
    assert final["all_offered_events_acknowledged"] is False


def test_close_timeout_reports_pending_and_never_blocks_the_reader(tmp_path):
    entered, release = threading.Event(), threading.Event()
    callers = []

    def write(_rows):
        callers.append(threading.current_thread().name)
        entered.set()
        assert release.wait(2)
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline", write_batch=write).start()
    assert writer.offer({"trading_date": "2026-11-10", "kind": "test"})
    assert entered.wait(1)
    state = writer.close(timeout=0)
    assert state["writer_alive"]
    assert state["all_offered_events_acknowledged"] is False
    assert callers == ["marketpin-opening-timeline"]
    release.set()
    assert writer.close(timeout=2)["all_offered_events_acknowledged"]



def test_unique_reason_burst_cannot_grow_counter_and_retirement_remains_explicit(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock, max_events=2)
    clock.set(1.2)
    record = quote(1)
    for index in range(1000):
        ledger.rejected(record, "NDX", f"OFFLINE_DISTINCT_REJECTION_{index}")
    ledger.retire("offline overflow test")
    state = ledger.snapshot()
    writer.close()
    assert len(ledger._counts) <= ledger.max_events + 256
    assert len(ledger._critical_seen) <= 257  # One reserved retirement marker.
    assert state["retired"] is True
    assert any(row["kind"] == "generation_retired" for row in state["critical_latest"])
    assert state["observer_overflow"] > 0


def test_cache_admission_duration_is_not_labelled_reader_queue_delay(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock)
    clock.set(1.1)
    ledger.sdk_callback(mapping(1, "SPX"))
    record = quote(1)
    ledger.sdk_callback(record)
    clock.set(1.2)
    ledger.processed(record, "SPX")
    clock.set(1.4)
    ledger.cache_admitted(record, "SPX")
    writer.close()
    rows = events(tmp_path)
    processed = next(r for r in rows if r["kind"] == "first_reader_processed_quote")
    admitted = next(r for r in rows if r["kind"] == "first_cache_admitted_quote")
    assert processed["callback_to_reader_elapsed_ns"] == 100_000_000
    assert admitted["callback_to_stage_elapsed_ns"] == 300_000_000
    assert "callback_to_reader_elapsed_ns" not in admitted
    assert admitted["cache_admitted_at_ns"] == OPEN_NS + 1_400_000_000


def test_timeline_module_is_bound_in_loaded_opening_source_contract():
    from backend.opening_code_fingerprint import OPENING_CRITICAL_SOURCE_PATHS
    assert "backend/opening_readiness_timeline.py" in OPENING_CRITICAL_SOURCE_PATHS



def test_compact_critical_summary_omits_raw_bytes_and_distinguishes_enqueue_fsync(tmp_path):
    clock = Clock()
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline")
    ledger = OpeningReadinessTimeline(
        writer, epoch_id="offline-compact", generation=7, cash_open_utc=OPEN,
        clock_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono,
    )
    clock.set(1.2)
    ledger.sdk_callback(mapping(2, "NDX"))
    ledger.sdk_callback(quote(2))
    ledger.rejected(quote(2), "NDX", "CROSSED_QUOTE")
    pending = ledger.snapshot()
    assert not pending["writer"]["writer_started"]
    assert len(pending["compact_critical_summary"]) <= 64
    assert all("raw_record_base64" not in row for row in pending["compact_critical_summary"])
    assert all(row["persistence_status"] == "ENQUEUED_PENDING_OR_UNKNOWN_NOT_FSYNC_ACKNOWLEDGED"
               for row in pending["compact_critical_summary"])
    writer.start()
    writer.close()
    acknowledged = ledger.snapshot()["compact_critical_summary"]
    assert all(row["persistence_status"] == "FSYNC_ACKNOWLEDGED" for row in acknowledged)
    assert any(row["details"].get("reason") == "CROSSED_QUOTE" for row in acknowledged)



def test_first_named_ndx_receipt_survives_exhausted_unmapped_scratch_budget(tmp_path):
    clock = Clock()
    ledger, writer = observer(tmp_path, clock, max_instruments=2)
    clock.set(1.2)
    ledger.sdk_callback(quote(1))
    ledger.sdk_callback(quote(2))
    ledger.sdk_callback(mapping(99, "NDX"))
    record = quote(99)
    ledger.sdk_callback(record)
    clock.set(1.4)
    ledger.processed(record, "NDX")
    writer.close()
    rows = events(tmp_path)
    receipt = next(r for r in rows if r["kind"] == "first_live_sdk_quote" and r["family_at_observation"] == "NDX")
    processed = next(r for r in rows if r["kind"] == "first_reader_processed_quote")
    assert processed["sdk_callback_receipt_for_record"] == receipt["event_id"]
    assert processed["callback_to_reader_elapsed_ns"] == 200_000_000
    assert ledger.snapshot()["retained_instrument_phase_receipts"] == 2
