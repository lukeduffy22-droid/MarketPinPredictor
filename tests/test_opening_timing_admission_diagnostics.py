"""Prospective timing evidence must preserve all existing admission decisions."""
from types import SimpleNamespace
import json

import pytest

import backend.databento_streamer as stream_module
from backend.opening_readiness_timeline import BoundedOpeningTimelineWriter, OpeningReadinessTimeline
from tests.test_opening_readiness_timeline import Clock, OPEN, OPEN_NS, SYMBOLS


def record(iid=1, **overrides):
    return SimpleNamespace(**{
        "instrument_id": iid, "ts_recv": OPEN_NS + 10**9, "ts_index": OPEN_NS + 10**9,
        "ts_event": None, "rtype": 192, "bid_px_00": 10**9, "ask_px_00": 2 * 10**9,
        **overrides,
    })


@pytest.fixture
def capture(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    source = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    source.active_generation = 7
    clock = Clock()
    clock.set(1.2)
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: clock.wall)
    writer = BoundedOpeningTimelineWriter(tmp_path / "timeline").start()
    observer = OpeningReadinessTimeline(
        writer, epoch_id=source.subscription_epoch_id, generation=7, cash_open_utc=OPEN,
        clock_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono,
    )
    source._opening_timeline = observer
    yield source, observer, writer, clock
    writer.close()


def counts(observer):
    return {(r["phase"], r["family_at_observation"], r["reason"]): r["observation_count"]
            for r in observer.snapshot()["timing_admission_counts"]}


@pytest.mark.parametrize(("reason", "changes", "generation", "replay"), [
    ("MISSING_TS_RECV", {"ts_recv": None}, 7, False),
    ("CBBO_RTYPE_MISMATCH", {"rtype": 193}, 7, False),
    ("CBBO_INTERVAL_UNALIGNED", {"ts_recv": OPEN_NS + 1}, 7, False),
    ("STALE_RECEIVE_LAG", {"ts_recv": OPEN_NS - 30 * 10**9}, 7, False),
    ("MISSING_INSTRUMENT_ID", {"instrument_id": None}, 7, False),
    ("GENERATION_MISMATCH", {}, 6, False),
    ("REPLAY_PENDING", {}, 7, True),
])
def test_each_exclusion_is_observed_without_adding_a_sample(capture, reason, changes, generation, replay):
    source, observer, writer, clock = capture
    source._clock_replay_pending = replay
    assert not source._record_processing_clock_sample(record(**changes), processed_at_ns=clock.wall, generation=generation)
    assert source._processing_clock_telemetry()["sample_count"] == 0
    assert counts(observer) == {("opening_bucket", "UNKNOWN", reason): 1}
    assert writer.flush()["all_offered_events_acknowledged"]


def test_49_and_50_sample_boundary_and_duplicate_evidence(capture):
    source, observer, _writer, clock = capture
    for iid in range(1, 50):
        assert source._record_processing_clock_sample(record(iid), processed_at_ns=clock.wall, generation=7)
    assert source._processing_clock_telemetry()["status"] == "unknown"
    assert not source._record_processing_clock_sample(record(1), processed_at_ns=clock.wall, generation=7)
    assert source._record_processing_clock_sample(record(50), processed_at_ns=clock.wall, generation=7)
    evidence = source._processing_clock_telemetry()
    assert evidence["minimum_samples"] == evidence["sample_count"] == 50
    assert evidence["status"] == "synchronized"
    assert evidence["establishes_opening_reference"] is False
    assert counts(observer) == {("opening_bucket", "UNKNOWN", "ADMITTED"): 50,
                                ("opening_bucket", "UNKNOWN", "DUPLICATE_RECORD"): 1}


def test_negative_lag_admission_does_not_establish_safe_clock(capture):
    source, observer, _writer, clock = capture
    clock.set(0.1)
    for iid in range(1, 51):
        assert source._record_processing_clock_sample(record(iid), processed_at_ns=clock.wall, generation=7)
    assert source._processing_clock_telemetry()["status"] == "unsynchronized"
    assert counts(observer)[("opening_bucket", "UNKNOWN", "ADMITTED")] == 50


def test_unknown_family_counts_are_not_rewritten_by_later_mapping(capture):
    source, observer, _writer, clock = capture
    source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    observer.mapped(1, SYMBOLS["SPX"])
    source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    assert counts(observer) == {("opening_bucket", "UNKNOWN", "ADMITTED"): 1,
                                ("opening_bucket", "SPX", "DUPLICATE_RECORD"): 1}


def test_retired_observer_and_replacement_identity_do_not_inherit_counts(capture):
    source, old, writer, clock = capture
    source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    old.retire("reconnect")
    source.active_generation = 8
    new = OpeningReadinessTimeline(writer, epoch_id=source.subscription_epoch_id, generation=8,
                                   cash_open_utc=OPEN, clock_ns=lambda: clock.wall)
    source._opening_timeline = new
    assert not source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    assert source._processing_clock_telemetry()["sample_count"] == 0
    assert counts(old)[("opening_bucket", "UNKNOWN", "ADMITTED")] == 1
    assert counts(new) == {("opening_bucket", "UNKNOWN", "GENERATION_MISMATCH"): 1}


def test_observer_failure_cannot_change_clock_admission_or_rejection(capture, monkeypatch):
    source, observer, _writer, clock = capture
    def broken(*_args, **_kwargs):
        raise OSError("synthetic instrumentation failure")
    monkeypatch.setattr(observer, "timing_admission", broken)
    for iid in range(1, 51):
        assert source._record_processing_clock_sample(record(iid), processed_at_ns=clock.wall, generation=7)
    assert source._processing_clock_telemetry()["status"] == "synchronized"
    assert not source._record_processing_clock_sample(record(ts_recv=None), processed_at_ns=clock.wall, generation=7)
    assert source._opening_timeline_error == "OSError"


def test_counts_stay_exact_when_event_retention_is_saturated(capture):
    source, observer, writer, clock = capture
    observer.max_events = 1
    observer.milestone("fill_normal_retention")
    # Force each reason to coalesce after its first event, not after one record.
    for _ in range(1000):
        source._record_processing_clock_sample(record(ts_recv=None), processed_at_ns=clock.wall, generation=7)
    for iid in range(1, 1001):
        source._record_processing_clock_sample(record(iid), processed_at_ns=clock.wall, generation=7)
    summary = observer.snapshot()
    assert len(summary["timing_admission_counts"]) == 2
    assert counts(observer)[("opening_bucket", "UNKNOWN", "MISSING_TS_RECV")] == 1000
    assert counts(observer)[("opening_bucket", "UNKNOWN", "ADMITTED")] == 1000
    assert summary["observer_overflow"] > 0
    assert writer.status()["offered_events"] < 10


def test_missing_price_still_retains_rejection_evidence(capture, tmp_path):
    _source, observer, writer, clock = capture
    observer.rejected(record(bid_px_00=None), "SPX", "INVALID_QUOTE_PRICE", observed_at_ns=clock.wall)
    assert writer.flush()["all_offered_events_acknowledged"]
    rows = [json.loads(line) for line in (tmp_path / "timeline" / "2026-11-10.ndjson").read_text().splitlines()]
    rejection = next(row for row in rows if row["kind"] == "first_quote_rejection")
    assert rejection["bid_px_raw"] is None
    assert rejection["details"]["reason"] == "INVALID_QUOTE_PRICE"


def test_generation_change_between_admission_and_observation_does_not_relabel_sample(capture, monkeypatch):
    source, old, writer, clock = capture
    original = source._observe_clock_admission
    source._opening_timeline_observers = {(old.epoch_id, 7): old}
    def replace_then_observe(*args, **kwargs):
        source.active_generation = 8
        new = OpeningReadinessTimeline(writer, epoch_id=source.subscription_epoch_id, generation=8,
                                       cash_open_utc=OPEN, clock_ns=lambda: clock.wall)
        source._opening_timeline = new
        original(*args, **kwargs)
    monkeypatch.setattr(source, "_observe_clock_admission", replace_then_observe)
    assert source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    assert source._processing_clock_telemetry()["sample_count"] == 0
    assert counts(source._opening_timeline) == {}
    assert writer.flush()["all_offered_events_acknowledged"]
    # Count milestones retain the original generation, even across reconnect.
    assert not any(row["kind"] == "provider_timing_count"
                   for row in source._opening_timeline.snapshot()["compact_critical_summary"])


def test_delayed_admission_is_observed_after_cutoff_without_relabeling_reader_time(capture, tmp_path):
    source, observer, writer, clock = capture
    reader_at = OPEN_NS + 4_900_000_000
    clock.set(5.1)
    assert source._record_processing_clock_sample(record(), processed_at_ns=reader_at, generation=7)
    assert counts(observer) == {("after_opening_bucket", "UNKNOWN", "ADMITTED"): 1}
    assert writer.flush()["all_offered_events_acknowledged"]
    rows = [json.loads(line) for line in (tmp_path / "timeline" / "2026-11-10.ndjson").read_text().splitlines()]
    row = next(row for row in rows if row["kind"] == "provider_timing_admission")
    assert row["reader_processed_at_ns"] == reader_at
    assert row["local_observed_at_ns"] == OPEN_NS + 5_100_000_000
    assert row["phase"] == "after_opening_bucket"


def test_epoch_replacement_between_admission_and_observation_cannot_inherit_result(capture, monkeypatch):
    source, old, writer, clock = capture
    source._opening_timeline_observers = {(old.epoch_id, 7): old}
    original = source._observe_clock_admission
    def replace_then_observe(*args, **kwargs):
        source.subscription_epoch_id = "replacement-offline-epoch"
        source._opening_timeline = OpeningReadinessTimeline(
            writer, epoch_id=source.subscription_epoch_id, generation=7,
            cash_open_utc=OPEN, clock_ns=lambda: clock.wall,
        )
        original(*args, **kwargs)
    monkeypatch.setattr(source, "_observe_clock_admission", replace_then_observe)
    assert source._record_processing_clock_sample(record(), processed_at_ns=clock.wall, generation=7)
    assert counts(source._opening_timeline) == {}
    assert not any(row["kind"] == "provider_timing_count"
                   for row in source._opening_timeline.snapshot()["compact_critical_summary"])


def test_actual_reader_observes_missing_timestamp(capture, monkeypatch):
    source, observer, _writer, clock = capture
    source.api_key = "offline-test-only"
    source.live_symbols = [SYMBOLS["SPX"]]
    source.is_running = True
    source.replay_minutes = 0
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX"])
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: clock.wall)
    monkeypatch.setattr(source, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True})
    monkeypatch.setattr(source, "_build_universe", lambda **_: None)
    monkeypatch.setattr(source, "_stream_stalled", lambda: False)
    def begin(_client, generation):
        observer.generation = generation
    monkeypatch.setattr(source, "_begin_opening_timeline", begin)
    class LocalClient:
        def __init__(self, **_kwargs): pass
        def subscribe(self, **_kwargs): pass
        def stop(self): pass
        def block_for_close(self, **_kwargs): pass
        def __iter__(self):
            yield record(ts_recv=None)
            source.is_running = False
    monkeypatch.setattr(stream_module.db, "Live", LocalClient)
    source._run_live()
    assert counts(observer)[("opening_bucket", "UNKNOWN", "MISSING_TS_RECV")] == 1
    assert source._processing_clock_telemetry()["sample_count"] == 0
