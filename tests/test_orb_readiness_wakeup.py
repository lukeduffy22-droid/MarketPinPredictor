"""A first-bucket readiness change must wake the existing sampler.

These tests use the real scheduler, capture gates and isolated SQLite journal.
Virtual time freezes during work: they prove scheduling opportunity, not live
provider availability or a guaranteed wall-clock persistence duration.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
import json

import databento_dbn as dbn
import pytest
from sqlalchemy import text

import backend.database as database
import backend.databento_streamer as stream_module
from backend.quote_timing import CBBO_TIMING, OPTIONAL_LAST_SALE, UNDEF_TIMESTAMP, last_sale_policy
from tests.test_opening_transition_stress import (
    september_14_open,
    _publish_mappings_and_quotes,
)
from tests.test_orb_cbbo_capture import _record


@pytest.fixture(autouse=True)
def isolated_streamer_paths(monkeypatch, tmp_path):
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")


class VirtualEvent:
    def __init__(self, clock, kind):
        self.clock = clock
        self.kind = kind
        self.flag = False

    def set(self):
        self.flag = True

    def clear(self):
        self.flag = False

    def is_set(self):
        return self.flag

    def wait(self, timeout=None):
        return self.clock.wait(self, timeout)


class VirtualOpening:
    def __init__(self, streamer, opening):
        self.streamer = streamer
        self.opening = opening
        self.seconds = 4.75
        self.callbacks = []
        self.trace = []
        self.wait_count = 0

    def utcnow(self):
        return stream_module.datetime.fromtimestamp(self.opening.timestamp() + self.seconds, tz=timezone.utc)

    def monotonic(self):
        return 1000.0 + self.seconds

    def schedule(self, seconds, label, function):
        self.callbacks.append((float(seconds), label, function))
        self.callbacks.sort(key=lambda item: item[0])

    def wait(self, event, timeout):
        self.wait_count += 1
        assert self.wait_count < 30, "Unconsumed readiness event spun the sampler"
        timeout = float(timeout or 0.0)
        assert 0.0 <= timeout <= 1.0
        self.trace.append({"wait": event.kind, "at": self.seconds, "timeout": timeout})
        if event.flag:
            return True
        target = self.seconds + timeout
        while self.callbacks and self.callbacks[0][0] <= target:
            when, label, function = self.callbacks.pop(0)
            self.seconds = max(self.seconds, when)
            self.trace.append({"callback": label, "at": self.seconds})
            function()
            if event.flag:
                return True
        self.seconds = target
        return event.flag

    def finish(self):
        self.streamer.is_running = False
        self.streamer._stop_event.set()
        self.streamer._orb_reference_wakeup_event.set()


def _install_real_scheduler(monkeypatch, streamer, journal, opening, contract="legacy"):
    for ensure in (
        database._ensure_orb_cbbo_reference_schema,
        database._ensure_orb_cbbo_reference_immutability,
        database._ensure_orb_cbbo_reference_decision_schema,
        database._ensure_orb_cbbo_reference_decision_immutability,
    ):
        ensure()
    streamer._test_wakeup_quote_contract = contract
    streamer._test_wakeup_opening = opening
    clock = VirtualOpening(streamer, opening)
    streamer.symbols = ["SPX", "NDX"]
    streamer._stop_event = VirtualEvent(clock, "stop")
    # Proposed interface only. Before the source fix the actual loop ignores it.
    streamer._orb_reference_wakeup_event = VirtualEvent(clock, "readiness")
    streamer.subscription_cutoff_monotonic = clock.monotonic() - 2.0
    streamer.is_running = True

    class VirtualDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            observed = clock.utcnow()
            return observed.astimezone(tz) if tz is not None else observed.replace(tzinfo=None)

    class SynchronousExecutor:
        def __init__(self, **options):
            self.options = options

        def submit(self, function, *args, **options):
            future = Future()
            if function.__name__ == "_capture_opening_reference_for_bucket":
                clock.trace.append({
                    "dispatch": args[0], "at": clock.seconds,
                    "bucket_seconds": (args[1] - opening).total_seconds(),
                })
            try:
                result = function(*args, **options)
            except BaseException as error:
                future.set_exception(error)
            else:
                future.set_result(result)
                if function.__name__ == "_capture_opening_reference_for_bucket":
                    clock.trace.append({
                        "result": args[0], "at": clock.seconds,
                        "recorded": result.get("recorded"),
                        "eligible": result.get("progress_eligible"),
                        "reason": result.get("reason"),
                    })
            return future

        def shutdown(self, **_options):
            pass

    monkeypatch.setattr(stream_module, "datetime", VirtualDateTime)
    monkeypatch.setattr(stream_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(stream_module, "ThreadPoolExecutor", SynchronousExecutor)
    monkeypatch.setattr("backend.market_structure.get_market_structure_journal", lambda: journal)
    monkeypatch.setattr("backend.opening_capture_receipts.due_checks", lambda *_: [])
    clock.schedule(5.25, "test end", clock.finish)
    return clock


def _clock_records(streamer, opening, count, *, first_instrument=1, unsafe=False, generation=7):
    recv = int(opening.timestamp()) * 10**9
    for instrument in range(first_instrument, first_instrument + count):
        streamer._record_processing_clock_sample(
            _record(instrument, recv),
            processed_at_ns=recv + (-1_000_000_000 if unsafe else 20_000_000),
            generation=generation,
        )


def _quotes_and_handoff(streamer, market, first_instrument, generation=7):
    received = _publish_mappings_and_quotes(
        streamer, market, first_instrument_id=first_instrument,
    )
    for record in streamer._universe_records_by_market[market]:
        quote = streamer.quotes[str(record["symbol"])]
        quote["generation"] = generation
        if streamer._test_wakeup_quote_contract == "cbbo":
            # The first interval ends at cash open +1s. The missing trade is
            # real optional metadata, never a synthesized opening trade.
            source_at = streamer._test_wakeup_opening + timedelta(seconds=1)
            recv = int(source_at.timestamp()) * 10**9
            quote.update(
                timestamp_contract_version=CBBO_TIMING, quote_schema="cbbo-1s",
                last_sale_validation_version=OPTIONAL_LAST_SALE,
                ts_event_ns=None, raw_ts_event_ns=UNDEF_TIMESTAMP,
                ts_recv_ns=recv, ts_index_ns=recv,
            )
    streamer._record_fresh_market_and_maybe_activate(
        market, generation=generation, received_monotonic=received,
    )


def _assert_actual_inputs_ready(streamer, opening, observed):
    telemetry = streamer._processing_clock_telemetry()
    assert telemetry["sample_count"] >= 50
    assert telemetry["status"] == "synchronized"
    assert streamer.handoff_status == "active"
    for market in ("SPX", "NDX"):
        snapshot, reason = streamer._snapshot_opening_reference_inputs(
            market, trading_date=opening.date(), captured_at_utc=observed,
        )
        assert reason is None, (market, reason)
        payload, reason = streamer._compute_opening_reference_payload(
            snapshot, sample_timestamp_utc=opening, captured_at_utc=observed,
        )
        assert reason is None, (market, reason)
        assert payload["paired_quote_count"] >= stream_module.MIN_PAIRED_QUOTES
        if streamer._test_wakeup_quote_contract == "cbbo":
            assert payload["timestamp_contract_version"] == CBBO_TIMING
            assert last_sale_policy(payload) == OPTIONAL_LAST_SALE
            assert payload["earliest_ts_event_ns"] is None
            assert payload["latest_ts_event_ns"] is None


def _first_bucket_counts(engine, opening):
    with engine.connect() as connection:
        rows = connection.execute(text("""
            SELECT s.symbol, count(*)
            FROM orb_reference_samples s
            JOIN orb_reference_sample_decisions d ON d.sample_id = s.sample_id
            WHERE julianday(s.sample_timestamp_utc) = julianday(:opening)
              AND d.progress_eligible = 1
            GROUP BY s.symbol
            UNION ALL
            SELECT s.symbol, count(*)
            FROM orb_cbbo_reference_samples s
            JOIN orb_cbbo_reference_sample_decisions d ON d.sample_id = s.sample_id
            WHERE julianday(s.sample_timestamp_utc) = julianday(:opening)
              AND d.progress_eligible = 1
            GROUP BY s.symbol
        """), {"opening": opening.isoformat()}).all()
        counts = {}
        for symbol, count in rows:
            counts[symbol] = counts.get(symbol, 0) + int(count)
        return counts


@pytest.mark.parametrize("contract", ["legacy", "cbbo"])
@pytest.mark.parametrize("last_gate", ["clock", "handoff"])
def test_readiness_at_480_wakes_real_sampler_before_first_bucket_closes(
    september_14_open, monkeypatch, last_gate, contract,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, contract)
    if last_gate == "clock":
        _quotes_and_handoff(streamer, "SPX", 10000)
        _quotes_and_handoff(streamer, "NDX", 20000)
        _clock_records(streamer, opening, 49)

        def complete_gate():
            _clock_records(streamer, opening, 1, first_instrument=50)
    else:
        _clock_records(streamer, opening, 50)
        _quotes_and_handoff(streamer, "SPX", 10000)

        def complete_gate():
            _quotes_and_handoff(streamer, "NDX", 20000)

    def ready():
        complete_gate()
        _assert_actual_inputs_ready(streamer, opening, clock.utcnow())

    streamer._orb_reference_wakeup_event.clear()
    clock.schedule(4.80, "last gate actually ready", ready)
    streamer._opening_reference_loop()
    counts = _first_bucket_counts(engine, opening)
    assert counts == {"SPX": 1, "NDX": 1}, json.dumps({
        "last_gate": last_gate, "counts": counts, "trace": clock.trace,
        "scope": "virtual-time scheduling opportunity, not live throughput",
    }, sort_keys=True)
    assert clock.wait_count < 15


def test_quote_pairs_later_than_handoff_remain_an_explicit_polling_limit(
    september_14_open, monkeypatch,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening)
    _clock_records(streamer, opening, 50)
    _quotes_and_handoff(streamer, "SPX", 10000)
    _quotes_and_handoff(streamer, "NDX", 20000)
    ndx_symbols = [str(row["symbol"]) for row in streamer._universe_records_by_market["NDX"]]
    for symbol in ndx_symbols[1:]:
        streamer.quotes.pop(symbol, None)
    assert streamer.handoff_status == "active"

    def pairs_arrive():
        # This bounded fix signals clock/handoff transitions only. There is
        # deliberately no per-quote wakeup; pairs can become ready afterward.
        _publish_mappings_and_quotes(streamer, "NDX", first_instrument_id=20000)
        _assert_actual_inputs_ready(streamer, opening, clock.utcnow())

    streamer._orb_reference_wakeup_event.clear()
    clock.schedule(4.80, "NDX pairs become available after handoff", pairs_arrive)
    streamer._opening_reference_loop()
    assert _first_bucket_counts(engine, opening) == {"SPX": 1}, json.dumps(clock.trace)
    assert not any(
        row.get("dispatch") == "NDX" and 4.8 <= row["at"] < 5.0
        for row in clock.trace
    )


@pytest.mark.parametrize("contract", ["legacy", "cbbo"])
@pytest.mark.parametrize("blocked", ["49_samples", "replay", "unsafe_clock", "missing_ndx", "wrong_generation", "late"])
def test_readiness_wakeup_never_bypasses_existing_capture_gates(
    september_14_open, monkeypatch, blocked, contract,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, contract)

    def deliver():
        generation = 6 if blocked == "wrong_generation" else 7
        streamer._clock_replay_pending = blocked == "replay"
        _clock_records(
            streamer, opening, 49 if blocked == "49_samples" else 50,
            unsafe=blocked == "unsafe_clock", generation=generation,
        )
        _quotes_and_handoff(streamer, "SPX", 10000, generation=generation)
        if blocked != "missing_ndx":
            _quotes_and_handoff(streamer, "NDX", 20000, generation=generation)
        # A spurious notification is deliberately injected. It grants no
        # authority and must not replace any timing/family/pair admission.
        streamer._orb_reference_wakeup_event.set()

    clock.schedule(5.0 if blocked == "late" else 4.8, blocked, deliver)
    streamer._opening_reference_loop()
    assert _first_bucket_counts(engine, opening) == {}, json.dumps(clock.trace)
    if blocked == "49_samples":
        assert streamer._processing_clock_telemetry()["sample_count"] == 49
    elif blocked == "replay":
        assert streamer._processing_clock_telemetry()["sample_count"] == 0
    elif blocked == "unsafe_clock":
        assert streamer._processing_clock_telemetry()["status"] == "unsynchronized"
    elif blocked in ("missing_ndx", "wrong_generation"):
        assert streamer.handoff_status == "warming"
        if blocked == "wrong_generation":
            assert streamer._processing_clock_telemetry()["sample_count"] == 0


def test_repeated_readiness_notifications_do_not_duplicate_eligible_first_bucket(
    september_14_open, monkeypatch,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening)

    def deliver():
        _clock_records(streamer, opening, 50)
        _quotes_and_handoff(streamer, "SPX", 10000)
        _quotes_and_handoff(streamer, "NDX", 20000)
        streamer._orb_reference_wakeup_event.set()

    clock.schedule(4.80, "ready", deliver)
    for at in (4.81, 4.82, 4.83):
        clock.schedule(at, "redundant notification", streamer._orb_reference_wakeup_event.set)
    streamer._opening_reference_loop()
    assert _first_bucket_counts(engine, opening) == {"SPX": 1, "NDX": 1}, json.dumps(clock.trace)
    assert clock.wait_count < 15


def test_stopping_fixture_interrupts_readiness_wait_without_another_poll(
    september_14_open, monkeypatch,
):
    streamer, journal, _engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening)
    clock.callbacks.clear()
    clock.schedule(4.81, "offline fixture stop", lambda: asyncio.run(streamer.stop()))
    streamer._opening_reference_loop()
    assert clock.seconds == pytest.approx(4.81), json.dumps(clock.trace)
    assert streamer._stop_event.is_set()
    assert streamer.handoff_status == "stopped"



def _interval_marker(opening, seconds=4):
    return dbn.SystemMsg(
        ts_event=int((opening + timedelta(seconds=seconds)).timestamp()) * 10**9,
        code=dbn.SystemCode.END_OF_INTERVAL, msg="cbbo-1s interval complete",
    )


def test_cbbo_pairs_after_handoff_wake_on_interval_completion(
    september_14_open, monkeypatch,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, "cbbo")
    _clock_records(streamer, opening, 50)
    _quotes_and_handoff(streamer, "SPX", 10000)
    _quotes_and_handoff(streamer, "NDX", 20000)
    ndx_symbols = [str(row["symbol"]) for row in streamer._universe_records_by_market["NDX"]]
    for symbol in ndx_symbols[1:]:
        streamer.quotes.pop(symbol, None)
    assert streamer.handoff_status == "active"

    def pairs_and_interval_complete():
        _quotes_and_handoff(streamer, "NDX", 20000)
        _assert_actual_inputs_ready(streamer, opening, clock.utcnow())
        # The pre-fix reader ignores END_OF_INTERVAL. The new handler is an
        # advisory wake only; the same actual capture checks and deadlines run.
        handler = getattr(streamer, "_record_orb_interval_completion", None)
        if handler is not None:
            handler(
                _interval_marker(opening), generation=7,
                processed_at_ns=int(clock.utcnow().timestamp() * 10**9),
            )

    streamer._orb_reference_wakeup_event.clear()
    clock.schedule(4.80, "NDX pairs and CBBO interval completion", pairs_and_interval_complete)
    streamer._opening_reference_loop()
    assert _first_bucket_counts(engine, opening) == {"SPX": 1, "NDX": 1}, json.dumps(clock.trace)


@pytest.mark.parametrize("invalid", [
    "replay", "wrong_generation", "unaligned", "future", "stale", "missing_timestamp",
    "wrong_code", "wrong_schema", "zero_generation",
])
def test_interval_completion_cannot_bypass_marker_or_capture_contract(
    september_14_open, monkeypatch, invalid,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, "cbbo")
    now_ns = int(clock.utcnow().timestamp() * 10**9)
    marker = _interval_marker(opening)
    generation = 7
    if invalid == "replay":
        streamer._clock_replay_pending = True
    elif invalid == "wrong_generation":
        generation = 6
    elif invalid == "unaligned":
        marker = dbn.SystemMsg(
            ts_event=int(opening.timestamp()) * 10**9 + 4_000_000_001,
            code=dbn.SystemCode.END_OF_INTERVAL, msg="unaligned",
        )
    elif invalid == "future":
        marker = _interval_marker(opening, 5)
    elif invalid == "stale":
        marker = _interval_marker(opening, -60)
    elif invalid == "missing_timestamp":
        marker = dbn.SystemMsg(
            ts_event=UNDEF_TIMESTAMP, code=dbn.SystemCode.END_OF_INTERVAL, msg="undefined",
        )
    elif invalid == "wrong_code":
        marker = dbn.SystemMsg(
            ts_event=int((opening + timedelta(seconds=4)).timestamp()) * 10**9,
            code=dbn.SystemCode.HEARTBEAT, msg="not an interval boundary",
        )
    elif invalid == "wrong_schema":
        streamer.schema = "tcbbo"
    elif invalid == "zero_generation":
        generation = 0

    assert not streamer._record_orb_interval_completion(
        marker, generation=generation, processed_at_ns=now_ns,
    )
    assert not streamer._orb_reference_wakeup_event.is_set()
    assert _first_bucket_counts(engine, opening) == {}
    assert streamer._processing_clock_telemetry()["status"] == "unknown"


def test_interval_completion_deduplicates_without_claiming_readiness(
    september_14_open, monkeypatch,
):
    streamer, journal, engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, "cbbo")
    marker = _interval_marker(opening)
    processed = int(clock.utcnow().timestamp() * 10**9)
    assert streamer._record_orb_interval_completion(
        marker, generation=7, processed_at_ns=processed,
    )
    assert streamer._orb_reference_wakeup_event.is_set()
    streamer._orb_reference_wakeup_event.clear()
    for _ in range(100):
        assert not streamer._record_orb_interval_completion(
            marker, generation=7, processed_at_ns=processed,
        )
    assert not streamer._orb_reference_wakeup_event.is_set()
    assert not streamer._record_orb_interval_completion(
        _interval_marker(opening, 3), generation=7, processed_at_ns=processed,
    )
    assert not streamer._orb_reference_wakeup_event.is_set()
    assert streamer._processing_clock_telemetry()["sample_count"] == 0
    assert streamer.handoff_status == "warming"
    assert not streamer._capture_opening_reference_for_bucket(
        "NDX", opening, now_utc=clock.utcnow, journal=journal,
    )["recorded"]
    assert _first_bucket_counts(engine, opening) == {}



def test_actual_reader_routes_native_interval_marker_without_granting_authority(
    monkeypatch,
):
    import time

    streamer = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.api_key = "offline-test-only"
    streamer.replay_minutes = 0
    streamer.is_running = True
    streamer.live_symbols = ["SPXW  261110C07375000", "NDXP  261110C25000000"]
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **_options: None)
    monkeypatch.setattr(streamer, "_prepare_live_subscription_stages", lambda: streamer.live_symbols)
    monkeypatch.setattr(streamer, "_activate_initial_subscription_stage", lambda _generation: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    marker_ns = time.time_ns() // 1_000_000_000 * 1_000_000_000
    marker = dbn.SystemMsg(
        ts_event=marker_ns, code=dbn.SystemCode.END_OF_INTERVAL, msg="cbbo-1s complete",
    )
    seen = []
    original = streamer._record_orb_interval_completion

    def observe(record, **options):
        result = original(record, **options)
        seen.append((record, options["generation"], result))
        return result

    monkeypatch.setattr(streamer, "_record_orb_interval_completion", observe)

    class InMemoryClient:
        def __init__(self, **_options):
            pass

        def subscribe(self, **_options):
            pass

        def __iter__(self):
            yield marker
            streamer.is_running = False

        def stop(self):
            pass

        def block_for_close(self, **_options):
            pass

    monkeypatch.setattr(stream_module.db, "Live", InMemoryClient)
    streamer._run_live()
    assert seen == [(marker, 1, True)]
    assert streamer._orb_reference_wakeup_event.is_set()
    assert streamer.messages_received == 0
    assert streamer._processing_clock_telemetry()["sample_count"] == 0
    assert streamer.handoff_status != "active"



def test_interval_completion_rebinds_same_provider_interval_to_new_generation(
    september_14_open, monkeypatch,
):
    streamer, journal, _engine, opening = september_14_open
    clock = _install_real_scheduler(monkeypatch, streamer, journal, opening, "cbbo")
    marker = _interval_marker(opening)
    processed = int(clock.utcnow().timestamp() * 10**9)
    assert streamer._record_orb_interval_completion(
        marker, generation=7, processed_at_ns=processed,
    )
    streamer._orb_reference_wakeup_event.clear()

    streamer.active_generation = 8
    assert not streamer._record_orb_interval_completion(
        marker, generation=7, processed_at_ns=processed,
    )
    assert not streamer._orb_reference_wakeup_event.is_set()
    assert streamer._record_orb_interval_completion(
        marker, generation=8, processed_at_ns=processed,
    )
    assert streamer._orb_reference_wakeup_event.is_set()
    streamer._orb_reference_wakeup_event.clear()
    assert not streamer._record_orb_interval_completion(
        marker, generation=8, processed_at_ns=processed,
    )
    assert not streamer._orb_reference_wakeup_event.is_set()
    assert streamer._processing_clock_telemetry()["sample_count"] == 0
    assert streamer.handoff_status == "warming"
