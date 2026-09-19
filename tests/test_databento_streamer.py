import time
import asyncio
import hashlib
import json
import logging
import threading
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.api.helpers import payload_is_usable_prediction, with_gex_level_semantics
from backend.databento_streamer import (
    DatabentoGammaStreamer,
    DatabentoMarketConfig,
    _active_pair_universe,
    _record_timestamp_ns,
    _timestamp_ns_to_utc_iso,
    _dataframe_records,
    black_scholes_gamma,
    black_scholes_price,
    build_full_oi_analytics,
    current_market_date,
    implied_volatility,
    infer_spot_from_pairs,
    gex_invariant_errors,
    expiration_blend_weights,
    live_subscription_window,
    max_pain,
    normalize_price,
    pin_competition_metrics,
    parse_raw_option_symbol,
    raw_option_symbol,
    zero_gamma_level,
)


@pytest.fixture(autouse=True)
def isolate_blocked_capture_receipts(monkeypatch, tmp_path):
    # Existing streamer tests simulate open sessions with default runtime paths.
    # Keep the new receipt sink entirely inside each test's temporary directory.
    from backend.capture_attempts import record_blocked_capture
    def isolated(streamer, reason, now):
        original = streamer.audit_dir
        streamer.audit_dir = tmp_path / 'logs' / 'audit'
        try:
            record_blocked_capture(streamer, reason, now)
        finally:
            streamer.audit_dir = original
    monkeypatch.setattr(DatabentoGammaStreamer, '_record_blocked_capture', isolated)
    from backend.capture_attempts import record_opening_reference_failure
    monkeypatch.setattr(
        'backend.capture_attempts.record_opening_reference_failure',
        lambda _audit_dir, failure: record_opening_reference_failure(
            tmp_path / 'logs' / 'audit', failure),
    )


def test_prediction_publication_guard_blocks_generation_transition():
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 3
    streamer.handoff_status = "active"
    attempted = threading.Event()
    completed = threading.Event()

    def _transition():
        attempted.set()
        streamer.active_generation = 4
        streamer.handoff_status = "warming"
        completed.set()

    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=3,
    ) as allowed:
        assert allowed is True
        thread = threading.Thread(target=_transition)
        thread.start()
        assert attempted.wait(1.0)
        assert not completed.wait(0.05)

    thread.join(timeout=1.0)
    assert completed.is_set()
    assert streamer.active_generation == 4
    assert streamer.handoff_status == "warming"


def test_pin_competition_metrics_exposes_near_tie_without_rewriting_primary():
    metrics = pin_competition_metrics(
        [(29370.0, 28.17), (29310.0, 27.86), (29300.0, 11.0)],
        contested_lead_ratio=0.10,
    )

    assert metrics["pin_runner_up_strike"] == 29310.0
    assert metrics["pin_lead_ratio"] == pytest.approx((28.17 - 27.86) / 28.17)
    assert metrics["pin_is_contested"] is True
    assert "PIN_CONTESTED" in metrics["pin_competition_reason"]


def test_pin_competition_metrics_marks_decisive_lead():
    metrics = pin_competition_metrics(
        [(7690.0, 100.0), (7665.0, 60.0)],
        contested_lead_ratio=0.10,
    )

    assert metrics["pin_lead_ratio"] == pytest.approx(0.4)
    assert metrics["pin_is_contested"] is False
    assert metrics["pin_competition_reason"] is None


def test_raw_option_symbol_padding_and_strike():
    symbol = raw_option_symbol("SPXW", date(2026, 6, 17), "C", 7400)
    assert symbol == "SPXW  260617C07400000"

    symbol = raw_option_symbol("NDXP", date(2026, 6, 17), "P", 30000)
    assert symbol == "NDXP  260617P30000000"


def test_parse_raw_option_symbol():
    parsed = parse_raw_option_symbol("SPXW  260617C07400000")
    assert parsed == {
        "expiration": date(2026, 6, 17),
        "option_type": "C",
        "strike": 7400.0,
    }

    assert parse_raw_option_symbol("bad-symbol") is None


@pytest.mark.parametrize(
    ("observed_utc", "expected_state", "expected_allowed"),
    [
        ("2026-09-04T12:44:59+00:00", "preopen_wait", False),
        ("2026-09-04T12:45:00+00:00", "preopen", True),
        ("2026-09-04T13:30:00+00:00", "regular_session", True),
        ("2026-09-04T19:58:00+00:00", "regular_session", True),
        ("2026-09-04T20:00:00+00:00", "post_close_research", True),
        ("2026-09-04T20:14:59+00:00", "post_close_research", True),
        ("2026-09-04T20:15:00+00:00", "post_close", False),
        ("2026-09-05T14:00:00+00:00", "non_trading_day", False),
        ("2026-09-07T14:00:00+00:00", "non_trading_day", False),
        ("2026-09-08T12:44:59+00:00", "preopen_wait", False),
        ("2026-09-08T12:45:00+00:00", "preopen", True),
        ("2026-09-08T13:30:00+00:00", "regular_session", True),
        ("2026-11-27T17:59:59+00:00", "regular_session", True),
        ("2026-11-27T18:00:00+00:00", "post_close_research", True),
        ("2026-11-27T18:15:00+00:00", "post_close", False),
    ],
)
def test_live_subscription_window_is_preopen_warm_and_close_bounded(
    observed_utc,
    expected_state,
    expected_allowed,
):
    window = live_subscription_window(datetime.fromisoformat(observed_utc))

    assert window["state"] == expected_state
    assert window["subscription_allowed"] is expected_allowed


def _opening_reference_streamer(
    observed_at: datetime,
    *,
    cross_series_legs: bool = False,
) -> DatabentoGammaStreamer:
    streamer = DatabentoGammaStreamer(["SPX"])
    generation = 7
    now_monotonic = time.monotonic()
    expiration = date.fromisoformat(observed_at.date().isoformat())
    source_ns = int((observed_at - timedelta(milliseconds=100)).timestamp() * 1_000_000_000)
    rows = []
    instrument_id = 1000
    for offset in range(5):
        strike = 6498.0 + offset
        for option_type in ("C", "P"):
            root = "SPX" if cross_series_legs and option_type == "P" else "SPXW"
            symbol = raw_option_symbol(root, expiration, option_type, strike)
            rows.append(
                {
                    "market": "SPX",
                    "symbol": symbol,
                    "strike": strike,
                    "option_type": option_type,
                    "expiration_date": expiration,
                    "open_interest": 1_000.0,
                }
            )
            mapping_version = hashlib.sha256(symbol.encode("utf-8")).hexdigest()
            streamer.quotes[symbol] = {
                "bid": 9.9,
                "ask": 10.1,
                "mid": 10.0,
                "received_monotonic": now_monotonic,
                "generation": generation,
                "instrument_id": instrument_id,
                "ts_event_ns": source_ns - 1_000_000,
                "ts_recv_ns": source_ns,
                "ts_index_ns": source_ns + 1_000_000,
                "provider_timestamp_order_valid": True,
                "mapping_version": mapping_version,
            }
            streamer.symbol_mappings[instrument_id] = {
                "raw_symbol": symbol,
                "mapping_version": mapping_version,
            }
            instrument_id += 1
    streamer.universe = pd.DataFrame(rows)
    streamer.full_universe = streamer.universe.copy()
    streamer._index_universe_metadata()
    streamer.active_generation = generation
    streamer.subscription_cutoff_monotonic = now_monotonic - 1.0
    streamer.handoff_status = "active"
    streamer.subscription_metadata = {
        "selected_universe_sha256": "a" * 64,
        "universe_provenance": {
            "trading_date": expiration.isoformat(),
            "is_fallback": False,
        },
        "markets": {
            "SPX": {
                "selected_expirations": [
                    {
                        "expiration": expiration.isoformat(),
                        "role": "primary",
                        "stage": 0,
                        "selected_strike_pairs": 5,
                    }
                ]
            }
        },
    }
    return streamer


def test_handoff_promotion_requires_only_configured_core_families(monkeypatch):
    monkeypatch.setattr(
        "backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS",
        ["SPX", "NDX"],
    )
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX", "RUT"])
    streamer.active_generation = 4
    streamer.handoff_status = "warming"

    assert streamer.required_handoff_symbols == ("SPX", "NDX")
    assert not streamer._record_fresh_market_and_maybe_activate("VIX")
    assert not streamer._record_fresh_market_and_maybe_activate("RUT")
    assert not streamer._record_fresh_market_and_maybe_activate("SPX")
    assert streamer.handoff_status == "warming"

    assert streamer._record_fresh_market_and_maybe_activate("NDX")
    assert streamer.handoff_status == "active"
    assert streamer._optional_family_canary_generation == 4


def test_handoff_promotion_fails_closed_without_required_family(monkeypatch):
    monkeypatch.setattr(
        "backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS",
        ["SPX", "NDX"],
    )
    missing_core = DatabentoGammaStreamer(["SPX", "VIX", "RUT"])
    missing_core.handoff_status = "warming"
    for market in ("SPX", "VIX", "RUT"):
        assert not missing_core._record_fresh_market_and_maybe_activate(market)
    assert missing_core.required_handoff_symbols == ("SPX", "NDX")
    assert missing_core.missing_required_handoff_symbols == ("NDX",)
    assert missing_core.handoff_status == "warming"
    assert "NDX" in str(missing_core.handoff_reason)

    monkeypatch.setattr(
        "backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS",
        ["SPX", "NOT_A_MARKET"],
    )
    unknown_required = DatabentoGammaStreamer(["SPX", "NDX", "VIX", "RUT"])
    unknown_required.handoff_status = "warming"
    for market in unknown_required.symbols:
        assert not unknown_required._record_fresh_market_and_maybe_activate(market)
    assert unknown_required.required_handoff_symbols == ("SPX", "NOT_A_MARKET")
    assert unknown_required.missing_required_handoff_symbols == ("NOT_A_MARKET",)
    assert unknown_required.handoff_status == "warming"
    assert "NOT_A_MARKET" in str(unknown_required.handoff_reason)

    monkeypatch.setattr(
        "backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS",
        [],
    )
    empty_required = DatabentoGammaStreamer(["SPX", "NDX"])
    empty_required.handoff_status = "warming"
    assert not empty_required._record_fresh_market_and_maybe_activate("SPX")
    assert empty_required.required_handoff_symbols == ()
    assert "No required handoff families" in str(empty_required.handoff_reason)


def test_opening_reference_sampler_runs_during_gex_backpressure_without_calculate_pin(
    monkeypatch,
):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    streamer._compute_suspended_until_monotonic = time.monotonic() + 600.0
    streamer._open_grace_until_monotonic = time.monotonic() + 600.0
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    monkeypatch.setattr(
        streamer,
        "_calculate_pin",
        lambda *_args, **_kwargs: pytest.fail("ORB sampler invoked full GEX"),
    )

    class Journal:
        payloads = []
        decisions = []

        def record_reference(self, payload):
            self.payloads.append(payload)
            return {
                "recorded": True,
                "reason": None,
                "sample_id": "d" * 64,
                "sample_timestamp_utc": payload["sample_timestamp_utc"],
                "source_timestamp_utc": payload["source_timestamp_utc"],
                "subscription_epoch_id": payload["subscription_epoch_id"],
                "subscription_generation": payload["subscription_generation"],
            }

        def record_reference_decision(self, **decision):
            self.decisions.append(decision)
            return {"recorded": True, **decision}

    journal = Journal()
    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=journal,
    )

    assert result["recorded"] is True
    assert result["subscription_generation"] == 7
    assert result["post_persist_context_current"] is True
    assert result["progress_eligible"] is True
    assert len(journal.payloads) == 1
    assert len(journal.decisions) == 1
    assert journal.decisions[0]["progress_eligible"] is True
    payload = journal.payloads[0]
    assert payload["paired_quote_count"] == 5
    assert payload["minimum_paired_quote_count"] == 5
    assert payload["subscription_generation"] == 7
    assert payload["universe_is_fallback"] is False
    assert payload["primary_expiration"] == "2026-09-08"
    assert payload["sample_timestamp_utc"].endswith("13:30:05+00:00")
    assert payload["source_timestamp_utc"].startswith("2026-09-08T13:30:04.900")


def test_opening_reference_sampler_rejects_generation_race(monkeypatch):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    original = streamer._compute_opening_reference_payload

    def race(*args, **kwargs):
        result = original(*args, **kwargs)
        streamer.active_generation += 1
        return result

    monkeypatch.setattr(streamer, "_compute_opening_reference_payload", race)

    class Journal:
        def record_reference(self, _payload):
            pytest.fail("generation-raced sample reached persistence")

    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=Journal(),
    )
    assert result == {
        "recorded": False,
        "reason": "REFERENCE_STATE_CHANGED_DURING_SAMPLE",
    }


def test_opening_reference_sampler_preserves_row_but_rejects_progress_after_persist_race(
    monkeypatch,
):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    persisted = []

    class Journal:
        decisions = []

        def record_reference(self, payload):
            assert streamer._fresh_quote_lock._is_owned() is False
            persisted.append(dict(payload))
            streamer.active_generation = 8
            return {
                "recorded": True,
                "reason": None,
                "sample_id": "e" * 64,
                "sample_timestamp_utc": payload["sample_timestamp_utc"],
                "source_timestamp_utc": payload["source_timestamp_utc"],
                "subscription_epoch_id": payload["subscription_epoch_id"],
                "subscription_generation": payload["subscription_generation"],
            }

        def record_reference_decision(self, **decision):
            self.decisions.append(decision)
            return {"recorded": True, **decision}

    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=Journal(),
    )

    assert len(persisted) == 1
    assert persisted[0]["subscription_generation"] == 7
    assert result["recorded"] is True
    assert result["subscription_generation"] == 7
    assert result["active_generation_after_persist"] == 8
    assert result["post_persist_context_current"] is False
    assert result["progress_eligible"] is False
    assert result["reason"] == "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST"
    assert Journal.decisions[0]["progress_eligible"] is False


def test_opening_reference_sampler_never_cross_pairs_same_strike_two_roots(monkeypatch):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed, cross_series_legs=True)
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )

    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=object(),
    )
    assert result == {"recorded": False, "reason": "COMPLETE_PAIR_MINIMUM_NOT_MET"}


def test_opening_reference_sampler_rejects_quote_from_prior_mapping(monkeypatch):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    instrument_id = min(streamer.symbol_mappings)
    streamer.symbol_mappings[instrument_id]["mapping_version"] = "f" * 64

    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=object(),
    )

    assert result == {"recorded": False, "reason": "COMPLETE_PAIR_MINIMUM_NOT_MET"}


def test_production_reference_loop_uses_post_lock_capture_clock(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7
    streamer.is_running = True
    calls = []
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    capture_finished = threading.Event()

    def capture(market, **kwargs):
        calls.append((market, kwargs))
        capture_finished.set()
        return {
            "recorded": True,
            "sample_timestamp_utc": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
            "subscription_generation": 7,
            "progress_eligible": True,
        }

    class StopAfterOneWait:
        def wait(self, timeout):
            assert timeout <= 1.0
            assert capture_finished.wait(timeout=1.0)
            streamer.is_running = False
            return True

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterOneWait()

    streamer._opening_reference_loop()

    assert len(calls) == 1
    assert calls[0][0] == "SPX"
    assert "observed_at_utc" not in calls[0][1]
    assert calls[0][1]["intended_bucket_utc"] == streamer._orb_reference_bucket(
        datetime.now(timezone.utc)
    )


def test_reference_loop_recomputes_bucket_deadline_and_generation_after_lock_contention(
    monkeypatch,
):
    opening_bucket = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    clock = {"now": opening_bucket + timedelta(milliseconds=250)}

    class ControlledDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            observed = clock["now"]
            return observed.astimezone(tz) if tz is not None else observed.replace(tzinfo=None)

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7
    streamer.is_running = True
    scheduler_reached_lock = threading.Event()
    holder_acquired_lock = threading.Event()
    capture_finished = threading.Event()
    holder_errors = []
    captures = []

    def hold_universe_lock_across_first_scheduler_clock():
        try:
            # Exercise real dispatch contention without manufacturing the
            # inverse universe -> publication order that production forbids.
            with (
                streamer._prediction_publication_lock,
                streamer._universe_index_lock,
            ):
                holder_acquired_lock.set()
                assert scheduler_reached_lock.wait(timeout=2.0)
                clock["now"] = opening_bucket + timedelta(seconds=2)
                streamer.active_generation = 8
        except BaseException as exc:
            holder_errors.append(exc)

    holder = threading.Thread(target=hold_universe_lock_across_first_scheduler_clock)
    holder.start()
    assert holder_acquired_lock.wait(timeout=2.0)

    window_calls = 0

    def regular_session(_observed=None):
        nonlocal window_calls
        window_calls += 1
        if window_calls == 1:
            scheduler_reached_lock.set()
        return {"state": "regular_session"}

    def capture(market, intended_bucket, **kwargs):
        captures.append((market, intended_bucket, kwargs))
        capture_finished.set()
        return {
            "recorded": True,
            "sample_timestamp_utc": intended_bucket.isoformat(),
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": kwargs[
                "expected_subscription_generation"
            ],
            "progress_eligible": True,
        }

    class StopAfterCapture:
        def wait(self, timeout):
            assert timeout <= 1.0
            assert capture_finished.wait(timeout=2.0)
            streamer.is_running = False
            return True

    monkeypatch.setattr("backend.databento_streamer.datetime", ControlledDateTime)
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window", regular_session
    )
    monkeypatch.setattr(
        "backend.databento_streamer.ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS", 4.0
    )
    monkeypatch.setattr(streamer, "_capture_opening_reference_for_bucket", capture)
    streamer._stop_event = StopAfterCapture()

    streamer._opening_reference_loop()
    holder.join(timeout=2.0)

    assert holder.is_alive() is False
    assert holder_errors == []
    assert len(captures) == 1
    market, intended_bucket, kwargs = captures[0]
    assert market == "SPX"
    assert intended_bucket == opening_bucket
    assert kwargs["attempt_deadline_utc"] == opening_bucket + timedelta(seconds=5)
    assert kwargs["expected_subscription_generation"] == 8


def test_reference_sampler_lifecycle_is_idempotent_and_joined(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    created = []
    telemetry = []

    class FakeThread:
        def __init__(self, *, target, daemon):
            self.target = target
            self.daemon = daemon
            self.started = False
            self.join_timeout = None
            created.append(self)

        def start(self):
            self.started = True

        def is_alive(self):
            return self.started and self.join_timeout is None

        def join(self, timeout=None):
            self.join_timeout = timeout

    def unexpected_db_init():
        pytest.fail("streamer start attempted hidden database initialization")

    monkeypatch.setattr("backend.databento_streamer.threading.Thread", FakeThread)
    monkeypatch.setattr("backend.database.init_db", unexpected_db_init)
    monkeypatch.setattr(
        streamer,
        "_install_provider_warning_telemetry",
        lambda: telemetry.append("installed"),
    )
    monkeypatch.setattr(
        streamer,
        "_remove_provider_warning_telemetry",
        lambda: telemetry.append("removed"),
    )

    asyncio.run(streamer.start())
    asyncio.run(streamer.start())

    assert len(created) == 4
    assert all(thread.started for thread in created)
    assert streamer.orb_reference_thread is created[-1]
    assert telemetry == ["installed"]

    streamer.handoff_status = "active"
    streamer.latest_pins["SPX"] = {"gamma_pin": 6500.0}
    asyncio.run(streamer.stop())

    assert all(thread.join_timeout == 2 for thread in created)
    assert streamer._stop_event.is_set()
    assert streamer.handoff_status == "stopped"
    assert streamer.get_subscription_context()["handoff_status"] == "stopped"
    assert streamer.get_latest_pin("SPX") is None
    assert telemetry == ["installed", "removed"]


def test_reference_sampler_shutdown_bounds_persist_barrier_wait(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    monkeypatch.setattr(
        "backend.databento_streamer.ORB_REFERENCE_SHUTDOWN_BARRIER_TIMEOUT_SECONDS",
        0.01,
    )
    assert streamer._orb_reference_persist_barrier.acquire(timeout=0.1)
    try:
        started = time.perf_counter()
        asyncio.run(streamer.stop())
        elapsed = time.perf_counter() - started
    finally:
        streamer._orb_reference_persist_barrier.release()

    assert elapsed < 0.5
    assert streamer.handoff_status == "stopped"


def test_reference_persist_barrier_finishes_save_before_stop_and_blocks_late_save(
    monkeypatch,
):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    save_started = threading.Event()
    release_save = threading.Event()
    stop_returned = threading.Event()
    ordering = []
    capture_result = {}

    class BlockingJournal:
        calls = 0

        def record_reference(self, payload):
            self.calls += 1
            ordering.append("save_started")
            save_started.set()
            assert release_save.wait(timeout=2.0)
            ordering.append("save_finished")
            return {
                "recorded": True,
                "reason": None,
                "sample_id": "d" * 64,
                "sample_timestamp_utc": payload["sample_timestamp_utc"],
                "source_timestamp_utc": payload["source_timestamp_utc"],
                "subscription_epoch_id": payload["subscription_epoch_id"],
                "subscription_generation": payload["subscription_generation"],
            }

        def record_reference_decision(self, **decision):
            return {"recorded": True, **decision}

    journal = BlockingJournal()

    def capture():
        capture_result.update(
            streamer._capture_opening_reference_once(
                "SPX",
                observed_at_utc=observed,
                journal=journal,
            )
        )

    def stop():
        asyncio.run(streamer.stop())
        ordering.append("stop_returned")
        stop_returned.set()

    capture_thread = threading.Thread(target=capture)
    capture_thread.start()
    assert save_started.wait(timeout=2.0)

    stop_thread = threading.Thread(target=stop)
    stop_thread.start()
    assert streamer._stop_event.wait(timeout=2.0)
    assert stop_returned.wait(timeout=0.05) is False

    release_save.set()
    capture_thread.join(timeout=2.0)
    stop_thread.join(timeout=2.0)

    assert capture_thread.is_alive() is False
    assert stop_thread.is_alive() is False
    assert ordering == ["save_started", "save_finished", "stop_returned"]
    assert capture_result["recorded"] is True
    assert capture_result["progress_eligible"] is False
    assert capture_result["reason"] == "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST"

    late_result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        journal=journal,
    )
    assert late_result == {
        "recorded": False,
        "reason": "REFERENCE_STREAMER_STOPPING",
        "progress_eligible": False,
    }
    assert journal.calls == 1


def test_reference_sampler_loop_recovers_after_transient_exception(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7
    streamer.is_running = True
    attempts = []
    attempt_condition = threading.Condition()
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    def capture(_market, **_kwargs):
        with attempt_condition:
            attempts.append(len(attempts) + 1)
            attempt_condition.notify_all()
        if len(attempts) == 1:
            raise RuntimeError("transient test failure")
        return {
            "recorded": True,
            "sample_timestamp_utc": "2026-09-08T13:30:05+00:00",
            "subscription_generation": 7,
            "progress_eligible": True,
        }

    class StopAfterTwoWaits:
        waits = 0

        def wait(self, timeout):
            assert timeout <= 1.0
            self.waits += 1
            # Yield to the worker so the sampler can reap the failed future;
            # retry dispatch happens on the following loop iteration.
            threading.Event().wait(0.05)
            if len(attempts) >= 2:
                streamer.is_running = False
            return False

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterTwoWaits()

    streamer._opening_reference_loop()

    assert attempts == [1, 2]
    assert streamer.last_orb_reference_results["SPX"]["recorded"] is True


def test_reference_sampler_times_out_hung_attempt_and_retries_next_bucket(
    monkeypatch,
):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7
    streamer.is_running = True
    first_bucket = datetime.now(timezone.utc).replace(microsecond=0)
    second_bucket = first_bucket + timedelta(seconds=5)
    bucket = {"value": first_bucket}
    monotonic = {"value": 1_000.0}
    first_started = threading.Event()
    release_first = threading.Event()
    first_returned = threading.Event()
    second_finished = threading.Event()
    attempts = []

    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )
    monkeypatch.setattr(
        "backend.databento_streamer.ORB_REFERENCE_ATTEMPT_TIMEOUT_SECONDS", 0.05
    )
    monkeypatch.setattr(
        "backend.databento_streamer.time.monotonic",
        lambda: monotonic["value"],
    )
    monkeypatch.setattr(
        streamer,
        "_orb_reference_bucket",
        lambda _observed: bucket["value"],
    )

    def capture(_market, intended_bucket, **_kwargs):
        attempts.append(intended_bucket)
        if len(attempts) == 1:
            first_started.set()
            assert release_first.wait(timeout=2.0)
            first_returned.set()
            return {"recorded": False, "reason": "LATE_TEST_RESULT"}
        second_finished.set()
        return {
            "recorded": True,
            "sample_timestamp_utc": second_bucket.isoformat(),
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": 7,
            "progress_eligible": True,
        }

    class DriveTwoBuckets:
        waits = 0

        def wait(self, timeout):
            assert timeout <= 1.0
            self.waits += 1
            if self.waits == 1:
                assert first_started.wait(timeout=2.0)
                monotonic["value"] += 0.10
                bucket["value"] = second_bucket
            else:
                assert second_finished.wait(timeout=2.0)
                streamer.is_running = False
            return False

    monkeypatch.setattr(streamer, "_capture_opening_reference_for_bucket", capture)
    streamer._stop_event = DriveTwoBuckets()

    started = time.perf_counter()
    streamer._opening_reference_loop()
    elapsed = time.perf_counter() - started

    assert elapsed < 1.0
    assert attempts == [first_bucket, second_bucket]
    assert any(
        failure["reason"] == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
        and failure["intended_bucket_utc"] == first_bucket.isoformat()
        for failure in streamer._orb_reference_failed_attempts
    )
    assert streamer.last_orb_reference_results["SPX"]["recorded"] is True
    assert streamer.last_orb_reference_results["SPX"]["sample_timestamp_utc"] == (
        second_bucket.isoformat()
    )

    release_first.set()
    assert first_returned.wait(timeout=2.0)


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    [
        ("deadline", "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"),
        ("epoch", "REFERENCE_SUBSCRIPTION_EPOCH_CHANGED_DURING_SAMPLE"),
        (
            "generation",
            "REFERENCE_SUBSCRIPTION_GENERATION_CHANGED_DURING_SAMPLE",
        ),
        ("context", "REFERENCE_STATE_CHANGED_DURING_SAMPLE"),
    ],
)
def test_reference_attempt_revalidates_authority_immediately_before_persist(
    monkeypatch,
    mutation,
    expected_reason,
):
    observed = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(observed)
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    expected_epoch_id = streamer.subscription_epoch_id
    expected_generation = int(streamer.active_generation)
    cancel_event = threading.Event()

    def mutate_after_snapshot(_snapshot, **_kwargs):
        if mutation == "deadline":
            cancel_event.set()
        elif mutation == "epoch":
            streamer.subscription_epoch_id = "b" * 64
        elif mutation == "generation":
            streamer.active_generation += 1
        else:
            streamer.subscription_metadata["selected_universe_sha256"] = "c" * 64
        return {"test_payload": True}, None

    monkeypatch.setattr(
        streamer,
        "_compute_opening_reference_payload",
        mutate_after_snapshot,
    )

    class Journal:
        calls = 0

        def record_reference(self, _payload):
            self.calls += 1
            pytest.fail("stale or timed-out ORB attempt reached persistence")

    journal = Journal()
    result = streamer._capture_opening_reference_once(
        "SPX",
        observed_at_utc=observed,
        intended_bucket_utc=observed,
        now_utc=lambda: observed,
        journal=journal,
        attempt_deadline_utc=observed + timedelta(seconds=4),
        attempt_deadline_monotonic=time.monotonic() + 4.0,
        attempt_cancel_event=cancel_event,
        expected_subscription_epoch_id=expected_epoch_id,
        expected_subscription_generation=expected_generation,
    )

    assert result["recorded"] is False
    assert result["reason"] == expected_reason
    assert journal.calls == 0


def test_reference_sampler_isolates_one_market_exception(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX", "RUT"])
    streamer.active_generation = 7
    streamer.is_running = True
    attempts = []
    all_attempted = threading.Event()
    attempts_lock = threading.Lock()
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    def capture(market, **_kwargs):
        with attempts_lock:
            attempts.append(market)
            if len(attempts) == 4:
                all_attempted.set()
        if market == "SPX":
            raise RuntimeError("isolated SPX failure")
        return {
            "recorded": True,
            "sample_timestamp_utc": "2026-09-08T13:30:05+00:00",
            "subscription_generation": 7,
            "progress_eligible": True,
        }

    class StopAfterOneWait:
        def wait(self, timeout):
            assert timeout <= 1.0
            assert all_attempted.wait(timeout=2.0)
            streamer.is_running = False
            return True

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterOneWait()

    streamer._opening_reference_loop()

    assert set(attempts) == {"SPX", "NDX", "VIX", "RUT"}
    assert len(attempts) == 4
    assert streamer.last_orb_reference_results["SPX"]["recorded"] is False
    assert streamer.last_orb_reference_results["SPX"]["reason"] == "SAMPLER_EXCEPTION"
    assert all(
        streamer.last_orb_reference_results[market]["recorded"] is True
        for market in ("NDX", "VIX", "RUT")
    )


def test_reference_sampler_retries_same_bucket_for_new_generation(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7
    streamer.is_running = True
    attempts = []
    attempt_condition = threading.Condition()
    sample_bucket = streamer._orb_reference_bucket(datetime.now(timezone.utc)).isoformat()
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    def capture(_market, **_kwargs):
        with attempt_condition:
            attempts.append(streamer.active_generation)
            attempt_condition.notify_all()
        if len(attempts) == 1:
            streamer.active_generation = 8
            return {
                "recorded": True,
                "reason": "REFERENCE_CONTEXT_CHANGED_AFTER_PERSIST",
                "sample_timestamp_utc": sample_bucket,
                "subscription_epoch_id": streamer.subscription_epoch_id,
                "subscription_generation": 7,
                "progress_eligible": False,
                "post_persist_context_current": False,
            }
        return {
            "recorded": True,
            "reason": None,
            "sample_timestamp_utc": sample_bucket,
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": 8,
            "progress_eligible": True,
            "post_persist_context_current": True,
        }

    class StopAfterTwoWaits:
        waits = 0

        def wait(self, timeout):
            assert timeout <= 1.0
            self.waits += 1
            with attempt_condition:
                assert attempt_condition.wait_for(
                    lambda: len(attempts) >= self.waits,
                    timeout=2.0,
                )
            if self.waits >= 2:
                streamer.is_running = False
            return False

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterTwoWaits()

    streamer._opening_reference_loop()

    assert attempts == [7, 8]
    assert [
        generation
        for _bucket, generation, _epoch_id in streamer._orb_reference_recent_buckets_by_market[
            "SPX"
        ]
    ] == [8]
    assert streamer.last_orb_reference_results["SPX"][
        "subscription_generation"
    ] == 8
    assert streamer.last_orb_reference_results["SPX"]["progress_eligible"] is True


def test_reference_sampler_dispatches_all_markets_before_slow_sibling_finishes(
    monkeypatch,
):
    markets = ("SPX", "NDX", "VIX", "RUT")
    streamer = DatabentoGammaStreamer(list(markets))
    streamer.active_generation = 7
    streamer.is_running = True
    started: set[str] = set()
    started_lock = threading.Lock()
    all_started = threading.Event()
    release_slow = threading.Event()
    slow_finished = threading.Event()
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    def capture(market, **_kwargs):
        with started_lock:
            started.add(market)
            if started == set(markets):
                all_started.set()
        if market == "SPX":
            assert release_slow.wait(timeout=2.0)
            slow_finished.set()
        return {"recorded": False, "reason": "TEST_DISPATCH_ONLY"}

    class StopAfterConcurrentDispatch:
        def wait(self, timeout):
            assert timeout <= 1.0
            assert all_started.wait(timeout=2.0)
            assert slow_finished.is_set() is False
            release_slow.set()
            assert slow_finished.wait(timeout=2.0)
            streamer.is_running = False
            return True

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterConcurrentDispatch()

    streamer._opening_reference_loop()

    assert started == set(markets)
    assert all(
        streamer.last_orb_reference_results[market]["reason"]
        == "TEST_DISPATCH_ONLY"
        for market in markets
    )


def test_reference_sampler_stops_dispatching_family_removed_by_canary_rollback(
    monkeypatch,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    markets = ("SPX", "NDX", "RUT")
    streamer = DatabentoGammaStreamer(list(markets))
    streamer.active_generation = 7
    streamer.is_running = True
    attempts = {market: 0 for market in markets}
    attempts_condition = threading.Condition()
    monkeypatch.setattr(
        "backend.databento_streamer.live_subscription_window",
        lambda _observed=None: {"state": "regular_session"},
    )

    def capture(market, **_kwargs):
        with attempts_condition:
            attempts[market] += 1
            attempts_condition.notify_all()
        return {"recorded": False, "reason": "TEST_REJECTED"}

    class StopAfterRollbackRetry:
        waits = 0

        def wait(self, timeout):
            assert timeout <= 1.0
            self.waits += 1
            with attempts_condition:
                if self.waits == 1:
                    assert attempts_condition.wait_for(
                        lambda: all(attempts[market] >= 1 for market in markets),
                        timeout=2.0,
                    )
                    decision = streamer.optional_family_canary.evaluate({})
                    assert streamer._apply_optional_family_canary_rollback(decision)
                else:
                    assert attempts_condition.wait_for(
                        lambda: attempts["SPX"] >= 2 and attempts["NDX"] >= 2,
                        timeout=2.0,
                    )
                    streamer.is_running = False
            return False

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    streamer._stop_event = StopAfterRollbackRetry()

    streamer._opening_reference_loop()

    assert streamer.symbols == ["SPX", "NDX"]
    assert attempts == {"SPX": 2, "NDX": 2, "RUT": 1}
    failures = list(streamer._orb_reference_failed_attempts)
    assert len(failures) == 5
    assert sum(failure["market"] == "RUT" for failure in failures) == 1
    assert all(
        {
            "reason",
            "intended_bucket_utc",
            "subscription_epoch_id",
            "subscription_generation",
            "attempt_started_at_utc",
            "attempt_completed_at_utc",
            "attempt_duration_seconds",
        }
        <= failure.keys()
        for failure in failures
    )


def test_reference_sampler_failed_attempt_history_is_structured_and_bounded(
    monkeypatch,
    caplog,
):
    monotonic_clock = [1_000.0]
    monkeypatch.setattr(
        "backend.databento_streamer.time.monotonic",
        lambda: monotonic_clock[0],
    )
    streamer = DatabentoGammaStreamer(["SPX"])
    epoch_id = streamer.subscription_epoch_id
    first_bucket = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    limit = streamer._orb_reference_failed_attempts.maxlen
    assert limit is not None
    caplog.set_level(logging.WARNING, logger="backend.databento_streamer")

    for index in range(limit + 2):
        bucket = first_bucket + timedelta(seconds=5 * index)
        streamer._record_opening_reference_failed_attempt(
            market="SPX",
            subscription_epoch_id=epoch_id,
            subscription_generation=7,
            intended_bucket_utc=bucket,
            result={
                "recorded": False,
                "reason": "TEST_REJECTED",
                "attempt_started_at_utc": (bucket + timedelta(milliseconds=10)).isoformat(),
                "attempt_completed_at_utc": (bucket + timedelta(milliseconds=20)).isoformat(),
                "attempt_duration_seconds": 0.01,
                "persist_lock_wait_seconds": 0.001,
                "persist_lock_held_seconds": 0.002,
            },
        )

    failures = list(streamer._orb_reference_failed_attempts)
    assert len(failures) == limit
    assert failures[0]["failure_sequence"] == 3
    assert failures[-1]["failure_sequence"] == limit + 2
    assert failures[-1] == {
        "event": "orb_reference_attempt_failed",
        "schema_version": "orb-reference-failed-attempt-v1",
        "market": "SPX",
        "reason": "TEST_REJECTED",
        "recorded": False,
        "progress_eligible": False,
        "intended_bucket_utc": (
            first_bucket + timedelta(seconds=5 * (limit + 1))
        ).isoformat(),
        "subscription_epoch_id": epoch_id,
        "subscription_generation": 7,
        "attempt_started_at_utc": (
            first_bucket
            + timedelta(seconds=5 * (limit + 1), milliseconds=10)
        ).isoformat(),
        "attempt_completed_at_utc": (
            first_bucket
            + timedelta(seconds=5 * (limit + 1), milliseconds=20)
        ).isoformat(),
        "attempt_duration_seconds": 0.01,
        "persist_lock_wait_seconds": 0.001,
        "persist_lock_held_seconds": 0.002,
        "failure_sequence": limit + 2,
    }
    assert streamer._orb_reference_failure_warning_suppressed_count == limit + 1
    assert len(caplog.records) == 1
    structured_message = caplog.records[0].getMessage().split(": ", 1)[1]
    warning = json.loads(structured_message)
    assert warning["event"] == "orb_reference_attempt_failure_summary"
    assert warning["failure_sequence"] == 1
    assert warning["intended_bucket_utc"] == first_bucket.isoformat()
    assert warning["suppressed_attempts_since_last_warning"] == 0
    assert warning["warning_interval_seconds"] == 60.0


def test_reference_sampler_failure_warning_summarizes_suppressed_attempts(
    monkeypatch,
    caplog,
):
    monotonic_clock = [1_000.0]
    monkeypatch.setattr(
        "backend.databento_streamer.time.monotonic",
        lambda: monotonic_clock[0],
    )
    streamer = DatabentoGammaStreamer(["VIX"])
    epoch_id = streamer.subscription_epoch_id
    first_bucket = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    caplog.set_level(logging.WARNING, logger="backend.databento_streamer")

    def reject(index):
        bucket = first_bucket + timedelta(seconds=5 * index)
        streamer._record_opening_reference_failed_attempt(
            market="VIX",
            subscription_epoch_id=epoch_id,
            subscription_generation=7,
            intended_bucket_utc=bucket,
            result={
                "recorded": False,
                "reason": "COMPLETE_PAIR_MINIMUM_NOT_MET",
                "attempt_started_at_utc": bucket.isoformat(),
                "attempt_completed_at_utc": (
                    bucket + timedelta(milliseconds=10)
                ).isoformat(),
                "attempt_duration_seconds": 0.01,
            },
        )

    reject(0)
    monotonic_clock[0] += 10.0
    reject(1)
    monotonic_clock[0] += 50.0
    reject(2)

    assert len(streamer._orb_reference_failed_attempts) == 3
    assert streamer._orb_reference_failure_warning_suppressed_count == 1
    assert len(caplog.records) == 2
    first_warning = json.loads(caplog.records[0].getMessage().split(": ", 1)[1])
    summary = json.loads(caplog.records[1].getMessage().split(": ", 1)[1])
    assert first_warning["suppressed_attempts_since_last_warning"] == 0
    assert summary["event"] == "orb_reference_attempt_failure_summary"
    assert summary["market"] == "VIX"
    assert summary["reason"] == "COMPLETE_PAIR_MINIMUM_NOT_MET"
    assert summary["failure_sequence"] == 3
    assert summary["suppressed_attempts_since_last_warning"] == 1
    assert summary["intended_bucket_utc"] == (
        first_bucket + timedelta(seconds=10)
    ).isoformat()


def test_reference_sampler_reports_persist_lock_timing_when_capture_rejects(
    monkeypatch,
):
    intended = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(intended + timedelta(milliseconds=100))
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    monkeypatch.setattr(streamer, "_opening_reference_context_is_current", lambda _snapshot: False)
    clock_values = iter(
        intended + timedelta(milliseconds=offset)
        for offset in (100, 200, 300, 400, 500)
    )

    result = streamer._capture_opening_reference_for_bucket(
        "SPX",
        intended,
        now_utc=lambda: next(clock_values),
        journal=object(),
    )

    assert result["recorded"] is False
    assert result["reason"] == "REFERENCE_STATE_CHANGED_DURING_SAMPLE"
    assert result["attempt_duration_seconds"] == pytest.approx(0.4)
    assert result["persist_lock_wait_seconds"] >= 0.0
    assert result["persist_lock_held_seconds"] >= 0.0


def test_opening_reference_capture_does_not_persist_after_intended_bucket(
    monkeypatch,
):
    intended = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = _opening_reference_streamer(
        intended + timedelta(milliseconds=200)
    )
    streamer.is_running = True
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "synchronized"},
    )
    clock_values = iter(
        (
            intended + timedelta(milliseconds=100),
            intended + timedelta(milliseconds=200),
            intended + timedelta(seconds=5, milliseconds=100),
        )
    )

    class Journal:
        def record_reference(self, _payload):
            pytest.fail("cross-bucket reference reached persistence")

    result = streamer._capture_opening_reference_once(
        "SPX",
        journal=Journal(),
        intended_bucket_utc=intended,
        now_utc=lambda: next(clock_values),
    )

    assert result == {
        "recorded": False,
        "reason": "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET",
        "progress_eligible": False,
    }


def test_cross_bucket_worker_result_cannot_satisfy_progress(monkeypatch):
    intended = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 7

    def capture(_market, **_kwargs):
        return {
            "recorded": True,
            "sample_timestamp_utc": intended.isoformat(),
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": 7,
            "progress_eligible": True,
        }

    clock_values = iter(
        (
            intended + timedelta(milliseconds=100),
            intended + timedelta(seconds=5, milliseconds=100),
        )
    )
    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)

    result = streamer._capture_opening_reference_for_bucket(
        "SPX",
        intended,
        now_utc=lambda: next(clock_values),
    )

    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["reason"] == "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"


def test_universe_index_rebuild_is_serialized_for_sampler_workers(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX", "NDX"])
    streamer._indexed_universe_frame_id = None
    streamer._indexed_universe_row_count = -1
    original_rebuild = streamer._index_universe_metadata_locked
    first_rebuild_entered = threading.Event()
    release_first_rebuild = threading.Event()
    calls = []
    errors = []

    def controlled_rebuild():
        calls.append(threading.get_ident())
        if len(calls) == 1:
            first_rebuild_entered.set()
            assert release_first_rebuild.wait(timeout=2.0)
        original_rebuild()

    def ensure_index():
        try:
            streamer._ensure_universe_index()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    monkeypatch.setattr(
        streamer,
        "_index_universe_metadata_locked",
        controlled_rebuild,
    )
    first = threading.Thread(target=ensure_index)
    second = threading.Thread(target=ensure_index)
    first.start()
    assert first_rebuild_entered.wait(timeout=2.0)
    second.start()
    threading.Event().wait(0.05)

    assert len(calls) == 1
    release_first_rebuild.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert first.is_alive() is False
    assert second.is_alive() is False
    assert errors == []
    assert len(calls) == 1


def test_gateway_timeout_at_close_parks_without_resubscribe_or_generation_loss(
    monkeypatch,
):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    post_close = {"state": "post_close", "subscription_allowed": False}
    windows = iter((allowed, post_close))
    clients = []
    waits = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.active_generation = 7
    streamer.is_running = True

    class StopAfterWait:
        def wait(self, timeout):
            waits.append(timeout)
            streamer.is_running = False
            return True

    class GatewayTimeoutClient:
        def __init__(self, **_kwargs):
            self.subscribe_calls = []
            self.stop_calls = 0
            clients.append(self)

        def subscribe(self, **kwargs):
            self.subscribe_calls.append(kwargs)

        def __iter__(self):
            final_pin = {"symbol": "SPX", "subscription_generation": 8}
            streamer.latest_pins["SPX"] = final_pin
            streamer.buffers["SPX"].append(final_pin)
            raise RuntimeError("Gateway timeout: 30 second(s) since last message")

        def stop(self):
            self.stop_calls += 1

    streamer._stop_event = StopAfterWait()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: next(windows))
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", GatewayTimeoutClient)

    streamer._run_live()

    assert len(clients) == 1
    assert len(clients[0].subscribe_calls) == 1
    assert clients[0].stop_calls == 1
    assert streamer.subscription_attempts == 1
    assert streamer.reconnect_attempts == 0
    assert streamer.active_generation == 8
    assert streamer.latest_pins["SPX"]["subscription_generation"] == 8
    assert streamer.buffers["SPX"][-1]["subscription_generation"] == 8
    assert streamer.handoff_status == "off_hours"
    assert waits and waits[0] <= 1.0


def test_gateway_timeout_during_subscription_window_still_reconnects(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    lifecycle_events = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class ReconnectingClient:
        def __init__(self, **_kwargs):
            self.index = len(clients)
            self.subscribe_calls = []
            self.stop_calls = 0
            clients.append(self)
            lifecycle_events.append(f"client-{self.index + 1}-created")

        def subscribe(self, **kwargs):
            self.subscribe_calls.append(kwargs)

        def __iter__(self):
            if self.index == 0:
                raise RuntimeError("Gateway timeout: 30 second(s) since last message")
            streamer.is_running = False
            return iter(())

        def stop(self):
            self.stop_calls += 1
            lifecycle_events.append(f"client-{self.index + 1}-stopped")

    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", ReconnectingClient)
    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep",
        lambda _seconds: lifecycle_events.append("backoff"),
    )

    streamer._run_live()

    assert len(clients) == 2
    assert all(len(client.subscribe_calls) == 1 for client in clients)
    assert all(client.stop_calls == 1 for client in clients)
    assert streamer.subscription_attempts == 2
    assert streamer.reconnect_attempts == 1
    assert streamer.last_reconnect_reason == (
        "Gateway timeout: 30 second(s) since last message"
    )
    assert streamer.active_generation == 2
    assert lifecycle_events.index("client-1-stopped") < lifecycle_events.index("backoff")
    assert lifecycle_events.index("client-1-stopped") < lifecycle_events.index(
        "client-2-created"
    )


def test_reconnect_retains_iterator_until_bounded_close_barrier(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    lifecycle_events = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class FailingIterator:
        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError("record processing failed")

        def __del__(self):
            lifecycle_events.append("iterator-released")

    class CloseAwareClient:
        def __init__(self, **kwargs):
            self.number = len(clients) + 1
            self.kwargs = kwargs
            clients.append(self)
            lifecycle_events.append(f"client-{self.number}-created")

        def subscribe(self, **_kwargs):
            lifecycle_events.append(f"client-{self.number}-subscribed")

        def __iter__(self):
            if self.number == 1:
                return FailingIterator()
            streamer.is_running = False
            return iter(())

        def stop(self):
            lifecycle_events.append(f"client-{self.number}-stop")

        def block_for_close(self, timeout):
            lifecycle_events.append(f"client-{self.number}-close-{timeout}")

    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", CloseAwareClient)
    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep",
        lambda seconds: lifecycle_events.append(f"backoff-{seconds}"),
    )

    streamer._run_live()

    assert len(clients) == 2
    assert all(client.kwargs["reconnect_policy"] == "none" for client in clients)
    first_stop = lifecycle_events.index("client-1-stop")
    first_barrier = lifecycle_events.index("client-1-close-5.0")
    iterator_release = lifecycle_events.index("iterator-released")
    first_backoff = lifecycle_events.index("backoff-3.0")
    second_create = lifecycle_events.index("client-2-created")
    assert first_stop < first_barrier < first_backoff < second_create
    # CPython may retain the ``for`` loop's internal iterator reference until
    # the frame advances, but it must never release it before the close barrier.
    assert first_barrier < iterator_release


def test_open_connection_limit_circuit_is_exact_bounded_and_health_visible(
    monkeypatch,
):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    cooldowns = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class CircuitStopEvent:
        def wait(self, timeout):
            cooldowns.append(timeout)
            if len(cooldowns) == 5:
                streamer.is_running = False
                return True
            return False

    class LimitRejectedClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.stop_calls = 0
            clients.append(self)

        def subscribe(self, **_kwargs):
            raise RuntimeError("USER HAS REACHED THEIR OPEN CONNECTION LIMIT")

        def stop(self):
            self.stop_calls += 1

        def block_for_close(self, timeout):
            raise AssertionError(
                f"pre-authentication close cannot be acknowledged: {timeout}"
            )

    streamer._stop_event = CircuitStopEvent()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", LimitRejectedClient)
    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep",
        lambda _seconds: pytest.fail("quota rejection used ordinary backoff"),
    )

    streamer._run_live()

    assert cooldowns == [60.0, 120.0, 240.0, 300.0, 300.0]
    assert len(clients) == 5
    assert all(client.stop_calls == 1 for client in clients)
    assert all(client.kwargs["reconnect_policy"] == "none" for client in clients)
    assert streamer.subscription_attempts == 0
    assert streamer.connection_limit_rejections_total == 5
    assert streamer.connection_limit_consecutive == 5
    assert streamer.connection_limit_circuit_state == "open"
    assert streamer.last_client_close_status == "pre_auth_close_unacknowledged"
    assert streamer._is_open_connection_limit_error(
        "prefix: User has reached their open connection limit"
    )
    assert not streamer._is_open_connection_limit_error(
        "user is near an open connections limit"
    )

    lifecycle = streamer._connection_lifecycle_health()
    health = streamer.get_health()
    for key, value in lifecycle.items():
        assert key in health
        if key == "connection_limit_cooldown_remaining_seconds":
            assert health[key] == pytest.approx(value, abs=0.1)
        else:
            assert health[key] == value
    assert health["connection_limit_retry_not_before_utc"] is not None


def test_connection_limit_cooldown_rechecks_subscription_window(monkeypatch):
    window = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    pauses = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class ClosingBellStopEvent:
        def wait(self, timeout):
            assert timeout == 60.0
            window.update(state="post_close", subscription_allowed=False)
            return False

    class LimitRejectedClient:
        def __init__(self, **_kwargs):
            clients.append(self)

        def subscribe(self, **_kwargs):
            raise RuntimeError("User has reached their open connection limit")

        def stop(self):
            return None

    def pause_outside_window(observed_window, **_kwargs):
        pauses.append(dict(observed_window))
        streamer.is_running = False

    streamer._stop_event = ClosingBellStopEvent()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: dict(window))
    monkeypatch.setattr(streamer, "_pause_outside_subscription_window", pause_outside_window)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", LimitRejectedClient)

    streamer._run_live()

    assert len(clients) == 1
    assert pauses and pauses[0]["state"] == "post_close"
    assert streamer.connection_limit_rejections_total == 1
    assert streamer.connection_limit_circuit_state == "closed"


def test_inconclusive_half_open_probe_rearms_before_another_client(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    cooldowns = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class CircuitStopEvent:
        def wait(self, timeout):
            cooldowns.append(timeout)
            return False

    class SequencedClient:
        def __init__(self, **_kwargs):
            self.number = len(clients) + 1
            clients.append(self)

        def subscribe(self, **_kwargs):
            if self.number == 1:
                raise RuntimeError(
                    "User has reached their open connection limit"
                )
            if self.number == 2:
                raise RuntimeError("temporary TLS failure")

        def __iter__(self):
            streamer.is_running = False
            return iter(())

        def stop(self):
            return None

        def block_for_close(self, timeout):
            assert timeout == 5.0

    streamer._stop_event = CircuitStopEvent()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", SequencedClient)
    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep",
        lambda _seconds: pytest.fail("half-open failure bypassed quota cooldown"),
    )

    streamer._run_live()

    assert len(clients) == 3
    assert cooldowns == [60.0, 60.0]
    assert streamer.connection_limit_rejections_total == 1
    assert streamer.connection_limit_consecutive == 0
    assert streamer.connection_limit_circuit_state == "closed"


def test_subscribed_failure_at_close_runs_barrier_before_off_hours_wait(monkeypatch):
    windows = iter(
        [
            {"state": "regular_session", "subscription_allowed": True},
            {"state": "post_close", "subscription_allowed": False},
        ]
    )
    events = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class ClosingBellFailureClient:
        def __init__(self, **_kwargs):
            return None

        def subscribe(self, **_kwargs):
            return None

        def __iter__(self):
            raise RuntimeError("stream failed at closing bell")

        def stop(self):
            events.append("stop")

        def block_for_close(self, timeout):
            events.append(f"barrier-{timeout}")

    def pause_outside_window(_window, **_kwargs):
        events.append("off-hours-wait")
        streamer.is_running = False

    monkeypatch.setattr(streamer, "_subscription_window", lambda: next(windows))
    monkeypatch.setattr(streamer, "_pause_outside_subscription_window", pause_outside_window)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", ClosingBellFailureClient)

    streamer._run_live()

    assert events == ["stop", "barrier-5.0", "off-hours-wait"]


def test_successful_quota_probe_closes_only_circuit_not_normal_backoff(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    normal_backoffs = []
    quota_cooldowns = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class CircuitStopEvent:
        def wait(self, timeout):
            quota_cooldowns.append(timeout)
            return False

    class SequencedClient:
        def __init__(self, **_kwargs):
            self.number = len(clients) + 1
            clients.append(self)

        def subscribe(self, **_kwargs):
            if self.number == 2:
                raise RuntimeError(
                    "User has reached their open connection limit"
                )

        def __iter__(self):
            if self.number == 1:
                raise RuntimeError("ordinary failure before quota rejection")
            if self.number == 3:
                raise RuntimeError("ordinary failure after quota recovery")
            streamer.is_running = False
            return iter(())

        def stop(self):
            return None

        def block_for_close(self, timeout):
            assert timeout == 5.0

    streamer._stop_event = CircuitStopEvent()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", SequencedClient)
    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep", normal_backoffs.append
    )

    streamer._run_live()

    assert len(clients) == 4
    assert quota_cooldowns == [60.0]
    # The accepted half-open probe closes the quota circuit, but only accepted
    # quote progress is permitted to forgive the existing reconnect delay.
    assert normal_backoffs == [3.0, 6.0]
    assert streamer.connection_limit_rejections_total == 1
    assert streamer.connection_limit_consecutive == 0
    assert streamer.connection_limit_circuit_state == "closed"


def test_rapid_post_subscribe_failures_escalate_and_bound_reconnect_delay(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    clients = []
    sleeps = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class RapidFailureClient:
        def __init__(self, **_kwargs):
            self.number = len(clients) + 1
            clients.append(self)

        def subscribe(self, **_kwargs):
            return None

        def __iter__(self):
            if self.number <= 4:
                raise RuntimeError(f"rapid failure {self.number}")
            streamer.is_running = False
            return iter(())

        def stop(self):
            return None

    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", RapidFailureClient)
    monkeypatch.setattr("backend.databento_streamer.RECONNECT_BASE_DELAY_SECONDS", 1.0)
    monkeypatch.setattr("backend.databento_streamer.RECONNECT_MAX_DELAY_SECONDS", 4.0)
    monkeypatch.setattr("backend.databento_streamer.time.sleep", sleeps.append)

    streamer._run_live()

    assert len(clients) == 5
    assert sleeps == [1.0, 2.0, 4.0, 4.0]


def test_sustained_accepted_quote_progress_resets_reconnect_delay(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    raw_symbol = "SPXW  260904C06500000"
    clients = []
    sleeps = []
    clock = {"now": 0.0}
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = [raw_symbol]
    streamer.is_running = True

    def mapping_record():
        record_type = type("SymbolMappingMsg", (), {})
        record = record_type()
        record.instrument_id = 123
        record.stype_out_symbol = raw_symbol
        record.ts_event = time.time_ns()
        record.ts_index = record.ts_event
        return record

    def quote_record():
        record_type = type("QuoteRecord", (), {})
        record = record_type()
        record.instrument_id = 123
        record.bid_px_00 = 1_000_000_000
        record.ask_px_00 = 2_000_000_000
        record.ts_event = time.time_ns()
        record.ts_recv = record.ts_event
        record.ts_index = record.ts_event
        return record

    class RecoveringClient:
        def __init__(self, **_kwargs):
            self.number = len(clients) + 1
            clients.append(self)

        def subscribe(self, **_kwargs):
            return None

        def __iter__(self):
            if self.number <= 2:
                raise RuntimeError(f"rapid failure {self.number}")
            if self.number == 3:
                yield mapping_record()
                clock["now"] = 100.0
                yield quote_record()
                clock["now"] = 111.0
                yield quote_record()
                raise RuntimeError("failure after sustained progress")
            streamer.is_running = False

        def stop(self):
            return None

    monkeypatch.setattr("backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS", ["SPX"])
    monkeypatch.setattr(streamer, "_subscription_window", lambda: allowed)
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.db.Live", RecoveringClient)
    monkeypatch.setattr("backend.databento_streamer.RECONNECT_BASE_DELAY_SECONDS", 1.0)
    monkeypatch.setattr("backend.databento_streamer.RECONNECT_MAX_DELAY_SECONDS", 8.0)
    monkeypatch.setattr("backend.databento_streamer.RECONNECT_HEALTHY_RESET_SECONDS", 10.0)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("backend.databento_streamer.time.sleep", sleeps.append)

    streamer._run_live()

    assert len(clients) == 4
    assert streamer.messages_received == 2
    assert sleeps == [1.0, 2.0, 1.0]


def test_clean_stream_end_at_close_parks_before_a_new_subscription(monkeypatch):
    allowed = {"state": "regular_session", "subscription_allowed": True}
    post_close = {"state": "post_close", "subscription_allowed": False}
    windows = iter((allowed, post_close, post_close))
    clients = []
    waits = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.replay_minutes = 0
    streamer.live_symbols = ["SPXW  260904C06500000"]
    streamer.is_running = True

    class StopAfterWait:
        def wait(self, timeout):
            waits.append(timeout)
            streamer.is_running = False
            return True

    class EndedClient:
        def __init__(self, **_kwargs):
            self.subscribe_calls = 0
            self.stop_calls = 0
            clients.append(self)

        def subscribe(self, **_kwargs):
            self.subscribe_calls += 1

        def __iter__(self):
            return iter(())

        def stop(self):
            self.stop_calls += 1

    streamer._stop_event = StopAfterWait()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: next(windows))
    monkeypatch.setattr(streamer, "_build_universe", lambda **_kwargs: None)
    monkeypatch.setattr("backend.databento_streamer.db.Live", EndedClient)

    streamer._run_live()

    assert len(clients) == 1
    assert clients[0].subscribe_calls == 1
    assert clients[0].stop_calls == 1
    assert streamer.subscription_attempts == 1
    assert streamer.reconnect_attempts == 0
    assert streamer.active_generation == 1
    assert streamer.handoff_status == "off_hours"
    assert waits and waits[0] <= 1.0


def test_health_exposes_off_hours_subscription_suppression(monkeypatch):
    post_close = {
        "state": "post_close",
        "subscription_allowed": False,
        "observed_at_utc": "2026-09-04T20:00:00+00:00",
        "trading_date": "2026-09-04",
        "connect_from_utc": "2026-09-04T12:45:00+00:00",
        "cash_open_utc": "2026-09-04T13:30:00+00:00",
        "cash_close_utc": "2026-09-04T20:00:00+00:00",
        "preopen_lead_seconds": 2700.0,
    }
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.active_generation = 8
    streamer.handoff_status = "off_hours"
    streamer.latest_pins["SPX"] = {"subscription_generation": 8}
    streamer._last_progress_monotonic = time.monotonic()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: post_close)

    health = streamer.get_health()

    assert health["websocket"] == "off_hours"
    assert health["subscription_session_state"] == "post_close"
    assert health["subscription_allowed"] is False
    assert health["subscription_suppressed"] is True
    assert health["active_generation"] == 8
    assert streamer.latest_pins["SPX"]["subscription_generation"] == 8


def test_health_valid_symbols_excludes_retained_cross_runtime_pins(monkeypatch):
    session = {
        "state": "regular_hours",
        "subscription_allowed": True,
        "observed_at_utc": "2026-09-04T15:00:00+00:00",
        "trading_date": "2026-09-04",
        "connect_from_utc": "2026-09-04T12:45:00+00:00",
        "cash_open_utc": "2026-09-04T13:30:00+00:00",
        "cash_close_utc": "2026-09-04T20:00:00+00:00",
        "preopen_lead_seconds": 2700.0,
    }
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.is_running = True
    streamer.active_generation = 8
    streamer.handoff_status = "active"
    streamer._last_progress_monotonic = time.monotonic()
    monkeypatch.setattr(streamer, "_subscription_window", lambda: session)
    streamer.latest_pins.update(
        {
            "SPX": {
                "subscription_epoch_id": streamer.subscription_epoch_id,
                "subscription_generation": 8,
            },
            "NDX": {
                "subscription_epoch_id": "f" * 64,
                "subscription_generation": 8,
            },
            "VIX": {
                "subscription_epoch_id": streamer.subscription_epoch_id,
                "subscription_generation": 7,
            },
        }
    )

    active = streamer.get_health()
    streamer.handoff_status = "stopped"
    stopped = streamer.get_health()

    assert active["valid_symbols"] == ["SPX"]
    assert active["retained_valid_symbols"] == ["NDX", "SPX", "VIX"]
    assert stopped["valid_symbols"] == []
    assert stopped["retained_valid_symbols"] == ["NDX", "SPX", "VIX"]


def test_normalize_price_handles_dbn_fixed_precision():
    assert normalize_price(7425.25) == 7425.25
    assert normalize_price(7425250000000) == 7425.25
    assert normalize_price(None) is None
    assert normalize_price(-1) is None


def test_startup_uses_bounded_prior_cache_before_historical_discovery(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.api_key = "fixture-key"
    calls = []
    monkeypatch.setattr(streamer, "_load_cached_universe", lambda _day: False)

    def load_prior(**kwargs):
        calls.append(kwargs)
        streamer._universe_built_monotonic = time.monotonic()
        return True

    monkeypatch.setattr(streamer, "_load_prior_cached_universe", load_prior)
    monkeypatch.setattr(
        "backend.databento_streamer.db.Historical",
        lambda _key: (_ for _ in ()).throw(AssertionError("historical discovery must not run")),
    )

    streamer._build_universe()

    assert len(calls) == 1
    assert "bounded prior-session scaffold" in calls[0]["reason"]


def _primary_pair_rows(market: str, expiration: date, count: int, *, incomplete: int = 0):
    root = "SPXW" if market == "SPX" else "NDXP"
    rows = []
    for index in range(count + incomplete):
        strike = 5000.0 + index
        sides = ("C", "P") if index < count else ("C",)
        for option_type in sides:
            rows.append({
                "market": market,
                "symbol": raw_option_symbol(root, expiration, option_type, strike),
                "expiration_date": expiration,
                "option_type": option_type,
                "strike": strike,
                "open_interest": 1.0,
            })
    return rows


def test_core_cache_admission_rejects_weak_core_and_collects_all_family_diagnostics(
    monkeypatch,
):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 3)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    trading_date = date(2026, 8, 26)
    frame = pd.DataFrame(
        _primary_pair_rows("SPX", trading_date, 1)
        + _primary_pair_rows("NDX", trading_date, 3)
        + _primary_pair_rows("RUT", trading_date + timedelta(days=1), 1)
    )

    assert not streamer._has_required_primary_pair_coverage(frame, trading_date=trading_date)
    assert streamer.primary_pair_admission_diagnostics["SPX"]["complete_pair_count"] == 1
    assert streamer.primary_pair_admission_diagnostics["SPX"]["minimum_pair_count"] == 3
    assert streamer.primary_pair_admission_diagnostics["SPX"]["reason"] == (
        "PRIMARY_PAIR_COVERAGE_INCOMPLETE"
    )
    assert streamer.primary_pair_admission_diagnostics["SPX"][
        "required_for_universe_admission"
    ] is True
    assert streamer.primary_pair_admission_diagnostics["NDX"]["passes"] is True
    rut_diagnostic = streamer.primary_pair_admission_diagnostics["RUT"]
    assert rut_diagnostic["passes"] is False
    assert rut_diagnostic["reason"] == "PRIMARY_PAIR_COVERAGE_INCOMPLETE"
    assert rut_diagnostic["minimum_pair_count"] >= 5
    assert rut_diagnostic["required_for_universe_admission"] is False


def test_current_cache_publishes_core_when_optional_families_are_unavailable(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(
        "backend.databento_streamer.DATABENTO_REQUIRED_SYMBOLS", ["SPX", "NDX"]
    )
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 8, 26)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX", "RUT"])
    streamer.cache_dir = tmp_path
    core_frame = pd.DataFrame(
        _primary_pair_rows("SPX", trading_date, 1)
        + _primary_pair_rows("NDX", trading_date, 1)
        + [
            {
                "market": "VIX",
                "symbol": raw_option_symbol(
                    "VIXW", trading_date, option_type, 18.0
                ),
                "expiration_date": trading_date,
                "option_type": option_type,
                "strike": 18.0,
                "open_interest": 1.0,
            }
            for option_type in ("C", "P")
        ]
    )
    core_frame.to_csv(streamer._cache_path(trading_date), index=False)

    assert streamer._load_cached_universe(trading_date) is True
    assert set(streamer.universe["market"]) == {"SPX", "NDX"}
    assert set(streamer.primary_pair_admission_diagnostics) == {
        "SPX",
        "NDX",
        "VIX",
        "RUT",
    }
    assert streamer.primary_pair_admission_diagnostics["SPX"]["passes"] is True
    assert streamer.primary_pair_admission_diagnostics["NDX"]["passes"] is True
    for market in ("VIX", "RUT"):
        diagnostic = streamer.primary_pair_admission_diagnostics[market]
        assert diagnostic["passes"] is False
        assert diagnostic["required_for_universe_admission"] is False
    assert streamer.primary_pair_admission_diagnostics["VIX"]["reason"] == (
        "NO_FORWARD_VIX_EXPIRATION_AVAILABLE"
    )
    assert streamer.primary_pair_admission_diagnostics["RUT"]["reason"] == (
        "NO_POSITIVE_OI_CONTRACTS"
    )

    unavailable = streamer.subscription_metadata[
        "optional_family_unavailable_reasons"
    ]
    assert unavailable == {
        "VIX": "NO_FORWARD_VIX_EXPIRATION_AVAILABLE",
        "RUT": "NO_POSITIVE_OI_CONTRACTS",
    }
    health = streamer.get_health()
    assert health["optional_family_unavailable_reasons"] == unavailable
    for market in ("VIX", "RUT"):
        status = health["market_subscription_status"][market]
        assert status["requested"] is True
        assert status["selected_contract_count"] == 0
        assert status["subscription_available"] is False
        assert status["required_for_universe_admission"] is False
        assert status["optional_family_unavailable_reason"] == unavailable[market]


def test_core_cache_admission_requires_pair_completeness_ratio(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 3)
    monkeypatch.setattr("backend.databento_streamer.MIN_CORE_PRIMARY_PAIR_COMPLETENESS_RATIO", 0.90)
    streamer = DatabentoGammaStreamer(["SPX", "NDX"])
    trading_date = date(2026, 8, 26)
    frame = pd.DataFrame(
        _primary_pair_rows("SPX", trading_date, 3, incomplete=1)
        + _primary_pair_rows("NDX", trading_date, 3)
    )

    assert not streamer._has_required_primary_pair_coverage(frame, trading_date=trading_date)
    assert streamer.primary_pair_admission_diagnostics["SPX"]["pair_completeness_ratio"] == 0.75


def test_active_pair_filter_keeps_zero_oi_counterpart_and_drops_zero_oi_pair():
    expiration = date(2026, 8, 26)
    rows = _primary_pair_rows("SPX", expiration, 2)
    rows[1]["open_interest"] = 0.0
    rows[2]["open_interest"] = 0.0
    rows[3]["open_interest"] = 0.0

    active = _active_pair_universe(pd.DataFrame(rows))

    assert len(active) == 2
    assert set(active["option_type"]) == {"C", "P"}
    assert sorted(active["open_interest"].tolist()) == [0.0, 1.0]


def test_rut_canary_rollback_removes_only_rut_and_requests_one_resubscribe(monkeypatch):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    stopped = []
    streamer.client = type("FakeClient", (), {"stop": lambda self: stopped.append(True)})()
    streamer.live_symbols = [
        raw_option_symbol("SPXW", date(2026, 8, 26), "C", 5000),
        raw_option_symbol("NDXP", date(2026, 8, 26), "C", 20000),
        raw_option_symbol("RUTW", date(2026, 8, 26), "C", 2500),
    ]
    streamer._symbol_to_market = {
        streamer.live_symbols[0]: "SPX",
        streamer.live_symbols[1]: "NDX",
        streamer.live_symbols[2]: "RUT",
    }
    decision = streamer.optional_family_canary.evaluate({
        "SPX": {"validation_is_valid": False, "data_age_seconds": 1.0, "receive_to_process_lag_p95_seconds": 0.1},
        "NDX": {"validation_is_valid": True, "data_age_seconds": 1.0, "receive_to_process_lag_p95_seconds": 0.1},
        "RUT": {"validation_is_valid": True, "data_age_seconds": 1.0, "receive_to_process_lag_p95_seconds": 0.1},
    })

    assert streamer._apply_optional_family_canary_rollback(decision)
    assert streamer.symbols == ["SPX", "NDX"]
    assert all("RUTW" not in symbol for symbol in streamer.live_symbols)
    assert stopped == [True]
    assert streamer.optional_family_rollback_evidence["resubscribe_requested"] is True
    assert streamer.optional_family_rollback_evidence["status"] == "requested"
    assert not streamer._apply_optional_family_canary_rollback(decision)
    assert stopped == [True, True]
    streamer.active_generation += 1
    streamer.handoff_status = "active"
    streamer._confirm_optional_family_canary_rollback()
    assert streamer.optional_family_rollback_evidence["status"] == "confirmed"
    assert streamer.optional_family_rollback_evidence["confirmed_generation"] == 1


def test_rut_canary_defers_optional_only_rollback_through_cash_session(monkeypatch):
    clock = [1000.0]
    current_window = {
        "state": "regular_session",
        "observed_at_utc": "2026-09-08T13:30:00+00:00",
        "cash_open_utc": "2026-09-08T13:30:00+00:00",
    }
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock[0])
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 3
    streamer.handoff_status = "active"
    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            family: {
                "validation_is_valid": True,
                "data_age_seconds": 1.0,
                "receive_to_process_lag_p95_seconds": 0.1,
                "primary_pair_coverage_ratio": 1.0,
                "fresh_quote_count": 100,
                "expected_quote_count": 100,
            }
            for family in ("SPX", "NDX")
        },
    )
    monkeypatch.setattr(
        streamer,
        "_subscription_window",
        lambda: current_window,
    )

    warming = streamer._optional_family_canary_decision()
    assert warming.state == "armed"
    assert warming.rollback_required is False
    assert streamer._optional_family_canary_warmup_remaining() == 180.0

    clock[0] += 181.0
    protected = streamer._optional_family_canary_decision()
    assert protected.state == "protected_opening_orb"
    assert protected.rollback_required is False
    assert "CANARY_MISSING:RUT" in protected.reasons

    generation_before = streamer.active_generation
    current_window["observed_at_utc"] = "2026-09-08T14:30:00+00:00"
    after_orb = streamer._optional_family_canary_decision()
    assert after_orb.state == "protected_cash_session"
    assert after_orb.rollback_required is False
    assert "CANARY_MISSING:RUT" in after_orb.reasons
    assert streamer.optional_family_canary_evaluation_evidence[
        "opening_orb_protection_active"
    ] is False
    assert streamer.optional_family_canary_evaluation_evidence[
        "cash_session_reconnect_protection_active"
    ] is True
    assert streamer._apply_optional_family_canary_rollback(after_orb) is False
    assert streamer.active_generation == generation_before

    current_window.update(
        state="postclose",
        observed_at_utc="2026-09-08T20:01:00+00:00",
    )
    postclose = streamer._optional_family_canary_decision()
    assert postclose.state == "deferred_off_hours"
    assert postclose.rollback_required is False
    assert streamer.optional_family_canary.status().state == "armed"


def test_rut_canary_preopen_never_consumes_or_latches_regular_session_warmup(
    monkeypatch,
):
    clock = [1000.0]
    current_window = {
        "state": "preopen",
        "observed_at_utc": "2026-09-08T13:00:00+00:00",
        "cash_open_utc": "2026-09-08T13:30:00+00:00",
    }
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock[0])
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 7
    streamer.handoff_status = "active"
    monkeypatch.setattr(streamer, "_subscription_window", lambda: current_window)
    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            family: {
                "validation_is_valid": True,
                "data_age_seconds": 1.0,
                "receive_to_process_lag_p95_seconds": 0.1,
                "primary_pair_coverage_ratio": 1.0,
                "fresh_quote_count": 100,
                "expected_quote_count": 100,
            }
            for family in ("SPX", "NDX")
        },
    )

    for _ in range(5):
        decision = streamer._optional_family_canary_decision()
        assert decision.state == "deferred_preopen"
        assert decision.rollback_required is False
        assert streamer._optional_family_canary_active_since_monotonic == 0.0
        clock[0] += 60.0

    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer.optional_family_canary_evaluation_evidence[
        "evaluation_deferred"
    ] is True

    current_window.update(
        state="regular_session",
        observed_at_utc="2026-09-08T13:30:00+00:00",
    )
    regular = streamer._optional_family_canary_decision()

    assert regular.state == "armed"
    assert streamer._optional_family_canary_warmup_remaining() == 180.0


@pytest.mark.parametrize(
    ("failure", "expected_reason"),
    [
        ("invalid", "CORE_INVALID:NDX"),
        ("freshness", "CORE_FRESHNESS_BUDGET:NDX"),
        ("lag", "CORE_LAG_BUDGET:NDX"),
        ("pairs", "CORE_PRIMARY_PAIR_COVERAGE:NDX"),
        ("quotes", "CORE_FRESH_QUOTE_COVERAGE:NDX"),
    ],
)
def test_rut_canary_warmup_does_not_mask_measured_core_failure(
    monkeypatch,
    failure,
    expected_reason,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 8
    streamer.handoff_status = "active"

    def healthy():
        return {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": 0.1,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 100,
            "expected_quote_count": 100,
        }

    observations = {"SPX": healthy(), "NDX": healthy()}
    if failure == "invalid":
        observations["NDX"]["validation_is_valid"] = False
    elif failure == "freshness":
        observations["NDX"]["data_age_seconds"] = 16.0
    elif failure == "lag":
        observations["NDX"]["receive_to_process_lag_p95_seconds"] = 3.0
    elif failure == "pairs":
        observations["NDX"]["primary_pair_coverage_ratio"] = 0.0
    elif failure == "quotes":
        observations["NDX"]["fresh_quote_count"] = 0
    monkeypatch.setattr(
        streamer, "_optional_family_canary_observations", lambda: observations
    )

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-08T13:30:30+00:00",
            "cash_open_utc": "2026-09-08T13:30:00+00:00",
        }
    )

    assert decision.state == "rolled_back"
    assert decision.rollback_required is True
    assert expected_reason in decision.reasons
    assert streamer.optional_family_canary_evaluation_evidence[
        "warmup_bypassed_for_measured_risk"
    ] is True
    assert streamer._apply_optional_family_canary_rollback(decision)
    assert streamer.symbols == ["SPX", "NDX"]


def test_rut_canary_warmup_does_not_mask_measured_compute_overload(monkeypatch):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 9
    streamer.handoff_status = "active"
    healthy_core = {
        family: {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": 0.1,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 100,
            "expected_quote_count": 100,
        }
        for family in ("SPX", "NDX")
    }
    monkeypatch.setattr(
        streamer, "_optional_family_canary_observations", lambda: healthy_core
    )
    monkeypatch.setattr(streamer, "_compute_backpressure_remaining", lambda: 5.0)

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-08T13:30:30+00:00",
            "cash_open_utc": "2026-09-08T13:30:00+00:00",
        }
    )

    assert decision.state == "rolled_back"
    assert decision.rollback_required is True
    assert "CANARY_MISSING:RUT" in decision.reasons
    assert streamer.optional_family_canary_evaluation_evidence[
        "compute_backpressure_active"
    ] is True
    assert streamer._apply_optional_family_canary_rollback(decision)
    assert streamer.symbols == ["SPX", "NDX"]


def test_rut_canary_initial_queue_backpressure_honors_warmup_with_no_observations(
    monkeypatch,
):
    clock = [1_000.0]
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock[0])
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 9
    streamer.handoff_status = "active"
    monkeypatch.setattr(streamer, "_optional_family_canary_observations", lambda: {})

    # Exact 2026-09-09 opening condition: one queue-full warning at 08:30:09,
    # followed by the first canary judgment eight seconds later while every
    # calculated family observation was still absent.
    streamer.record_provider_warning(
        "record queue is full; 130 record(s) to be processed"
    )
    clock[0] += 8.0
    window = {
        "state": "regular_session",
        "observed_at_utc": "2026-09-09T13:30:17+00:00",
        "cash_open_utc": "2026-09-09T13:30:00+00:00",
    }

    warming = streamer._optional_family_canary_decision(window)

    assert warming.state == "armed"
    assert warming.rollback_required is False
    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer.provider_queue_full_warnings == 1
    evidence = streamer.optional_family_canary_evaluation_evidence
    assert evidence["compute_backpressure_active"] is True
    assert evidence["provider_integrity_risk_active"] is False
    assert evidence["initial_queue_backpressure_with_no_observations"] is True
    assert evidence["warmup_remaining_seconds"] == 180.0
    assert evidence["reasons"] == ["REGULAR_SESSION_WARMUP_ACTIVE"]
    assert streamer._apply_optional_family_canary_rollback(warming) is False
    assert streamer.symbols == ["SPX", "NDX", "RUT"]

    clock[0] += 181.0
    expired = streamer._optional_family_canary_decision(window)
    assert expired.state == "rolled_back"
    assert expired.rollback_required is True
    assert expired.reasons == (
        "CORE_MISSING:SPX",
        "CORE_MISSING:NDX",
        "CANARY_MISSING:RUT",
    )


@pytest.mark.parametrize(
    "warning",
    [
        "slow client detected",
        "skipped 7 records",
    ],
)
def test_rut_canary_warmup_does_not_mask_provider_integrity_risk(
    monkeypatch, warning
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 10
    streamer.handoff_status = "active"
    monkeypatch.setattr(streamer, "_optional_family_canary_observations", lambda: {})
    streamer.record_provider_warning(warning)

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-09T13:30:17+00:00",
            "cash_open_utc": "2026-09-09T13:30:00+00:00",
        }
    )

    assert decision.state == "rolled_back"
    assert decision.rollback_required is True
    evidence = streamer.optional_family_canary_evaluation_evidence
    assert evidence["provider_integrity_risk_active"] is True
    assert evidence["initial_queue_backpressure_with_no_observations"] is False
    assert evidence["warmup_bypassed_for_measured_risk"] is True


def test_rut_canary_warmup_does_not_mask_measured_canary_lag(monkeypatch):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "180")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 10
    streamer.handoff_status = "active"

    def observation(lag):
        return {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": lag,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 100,
            "expected_quote_count": 100,
        }

    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            "SPX": observation(0.1),
            "NDX": observation(0.1),
            "RUT": observation(4.0),
        },
    )

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-08T13:30:30+00:00",
            "cash_open_utc": "2026-09-08T13:30:00+00:00",
        }
    )

    assert decision.state == "rolled_back"
    assert decision.rollback_required is True
    assert "CANARY_LAG_BUDGET:RUT" in decision.reasons
    assert streamer.optional_family_canary_evaluation_evidence[
        "warmup_bypassed_for_measured_risk"
    ] is True
    assert streamer.optional_family_canary_evaluation_evidence[
        "rut_lag_breach_measured"
    ] is True
    assert streamer._apply_optional_family_canary_rollback(decision)
    assert streamer.symbols == ["SPX", "NDX"]


def test_rut_canary_protects_optional_opening_deficiencies_but_not_lag_or_overload(
    monkeypatch,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "0")
    window = {
        "state": "regular_session",
        "observed_at_utc": "2026-09-08T13:33:00+00:00",
        "cash_open_utc": "2026-09-08T13:30:00+00:00",
    }

    def observation(*, valid=True, lag=0.1):
        return {
            "validation_is_valid": valid,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": lag,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 100,
            "expected_quote_count": 100,
        }

    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 11
    streamer.handoff_status = "active"
    observations = {
        "SPX": observation(),
        "NDX": observation(),
    }
    monkeypatch.setattr(
        streamer, "_optional_family_canary_observations", lambda: observations
    )

    missing_protected = streamer._optional_family_canary_decision(window)

    assert missing_protected.state == "protected_opening_orb"
    assert missing_protected.rollback_required is False
    assert "CANARY_MISSING:RUT" in missing_protected.reasons
    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer._rut_orb_reference_progress(
        datetime(2026, 9, 8, 13, 33, tzinfo=timezone.utc)
    )["advancing"] is False

    observations["RUT"] = observation(valid=False)
    protected = streamer._optional_family_canary_decision(window)

    assert protected.state == "protected_opening_orb"
    assert protected.rollback_required is False
    assert "CANARY_INVALID:RUT" in protected.reasons
    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer.optional_family_canary_evaluation_evidence[
        "opening_orb_protection_active"
    ] is True

    observations["RUT"] = observation(valid=False, lag=4.0)
    lagged = streamer._optional_family_canary_decision(window)
    assert lagged.state == "rolled_back"
    assert "CANARY_LAG_BUDGET:RUT" in lagged.reasons

    overloaded = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    overloaded.active_generation = 12
    overloaded.handoff_status = "active"
    overloaded._orb_reference_recent_buckets_by_market["RUT"].extend(
        [
            (
                datetime(2026, 9, 8, 13, 32, 55, tzinfo=timezone.utc),
                12,
                overloaded.subscription_epoch_id,
            ),
            (
                datetime(2026, 9, 8, 13, 33, 0, tzinfo=timezone.utc),
                12,
                overloaded.subscription_epoch_id,
            ),
        ]
    )
    monkeypatch.setattr(
        overloaded, "_optional_family_canary_observations", lambda: {
            "SPX": observation(),
            "NDX": observation(),
            "RUT": observation(valid=False),
        }
    )
    monkeypatch.setattr(overloaded, "_compute_backpressure_remaining", lambda: 5.0)

    overload_decision = overloaded._optional_family_canary_decision(window)
    assert overload_decision.state == "rolled_back"
    assert overload_decision.rollback_required is True


def test_rut_canary_protects_lag_unavailable_as_only_optional_opening_defect(
    monkeypatch,
):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "0")
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.active_generation = 14
    streamer.handoff_status = "active"

    def observation(lag):
        return {
            "validation_is_valid": True,
            "data_age_seconds": 1.0,
            "receive_to_process_lag_p95_seconds": lag,
            "primary_pair_coverage_ratio": 1.0,
            "fresh_quote_count": 100,
            "expected_quote_count": 100,
        }

    monkeypatch.setattr(
        streamer,
        "_optional_family_canary_observations",
        lambda: {
            "SPX": observation(0.1),
            "NDX": observation(0.1),
            "RUT": observation(None),
        },
    )

    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": "2026-09-08T13:35:00+00:00",
            "cash_open_utc": "2026-09-08T13:30:00+00:00",
        }
    )

    assert decision.state == "protected_opening_orb"
    assert decision.rollback_required is False
    assert decision.reasons == ("CANARY_LAG_UNAVAILABLE:RUT",)
    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer._apply_optional_family_canary_rollback(decision) is False
    assert streamer.active_generation == 14


def test_rut_canary_protects_real_invalid_snapshot_with_unavailable_lag(
    tmp_path,
    monkeypatch,
):
    import database as root_database

    observed = datetime(2026, 9, 8, 13, 33, tzinfo=timezone.utc)
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setenv("DATABENTO_RUT_CANARY_WARMUP_SECONDS", "0")
    monkeypatch.setattr(
        "backend.databento_streamer._utcnow_naive",
        lambda: observed.replace(tzinfo=None),
    )
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", lambda _row: None)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "RUT"])
    streamer.audit_dir = tmp_path / "audit"
    streamer.active_generation = 13
    streamer.handoff_status = "active"
    streamer.subscription_metadata.update(
        {
            "markets": {
                symbol: {
                    "selected_contract_count": 100,
                    "selected_expirations": [
                        {
                            "expiration": "2026-09-08",
                            "stage": 0,
                            "role": "primary",
                        }
                    ],
                }
                for symbol in ("SPX", "NDX", "RUT")
            }
        }
    )
    valid_core = {
        "validation_is_valid": True,
        "receive_to_process_lag_p95_seconds": 0.1,
        "primary_pair_coverage_ratio": 1.0,
    }
    for symbol in ("SPX", "NDX"):
        streamer.latest_pins[symbol] = dict(valid_core)
        streamer.last_message_time[symbol] = (
            observed - timedelta(seconds=1)
        ).replace(tzinfo=None)
        streamer._fresh_quote_counts_by_market[symbol] = 100

    streamer._write_invalid_snapshot("RUT", "calculation unavailable")
    streamer._fresh_quote_counts_by_market["RUT"] = 100
    streamer._orb_reference_recent_buckets_by_market["RUT"].extend(
        [
            (
                observed - timedelta(seconds=5),
                13,
                streamer.subscription_epoch_id,
            ),
            (observed, 13, streamer.subscription_epoch_id),
        ]
    )

    assert "RUT" not in streamer.last_message_time
    assert "receive_to_process_lag_p95_seconds" not in streamer.latest_invalid["RUT"]
    decision = streamer._optional_family_canary_decision(
        {
            "state": "regular_session",
            "observed_at_utc": observed.isoformat(),
            "cash_open_utc": "2026-09-08T13:30:00+00:00",
        }
    )

    assert decision.state == "protected_opening_orb"
    assert decision.rollback_required is False
    assert "CANARY_INVALID:RUT" in decision.reasons
    assert "CANARY_FRESHNESS_BUDGET:RUT" in decision.reasons
    assert "CANARY_LAG_UNAVAILABLE:RUT" in decision.reasons
    assert "CANARY_LAG_BUDGET:RUT" not in decision.reasons
    assert streamer.optional_family_canary.status().state == "armed"
    assert streamer.optional_family_canary_evaluation_evidence[
        "rut_lag_evidence_unavailable"
    ] is True
    assert streamer.optional_family_canary_evaluation_evidence[
        "rut_lag_breach_measured"
    ] is False


def test_implied_volatility_recovers_known_vol():
    spot = 100.0
    strike = 100.0
    years = 30 / 365
    true_vol = 0.25
    price = black_scholes_price(spot, strike, years, true_vol, "C")
    solved = implied_volatility(spot, strike, years, price, "C")
    assert solved is not None
    assert solved == pytest.approx(true_vol, rel=1e-4)


def test_gamma_is_positive_for_atm_option():
    gamma = black_scholes_gamma(100.0, 100.0, 30 / 365, 0.25)
    assert gamma > 0


def test_zero_gamma_interpolation():
    level = zero_gamma_level({100.0: 10.0, 110.0: -10.0})
    assert level == pytest.approx(105.0)


def test_max_pain_uses_open_interest_by_strike():
    chain = pd.DataFrame([
        {"strike": 95.0, "option_type": "C", "open_interest": 10},
        {"strike": 100.0, "option_type": "C", "open_interest": 100},
        {"strike": 105.0, "option_type": "C", "open_interest": 10},
        {"strike": 95.0, "option_type": "P", "open_interest": 10},
        {"strike": 100.0, "option_type": "P", "open_interest": 100},
        {"strike": 105.0, "option_type": "P", "open_interest": 10},
    ])
    assert max_pain(chain) == 100.0


def test_infer_spot_from_call_put_pairs():
    years = 1 / 365
    chain = pd.DataFrame([
        {"strike": 100.0, "option_type": "C", "mid": 3.0},
        {"strike": 100.0, "option_type": "P", "mid": 2.0},
        {"strike": 105.0, "option_type": "C", "mid": 1.2},
        {"strike": 105.0, "option_type": "P", "mid": 4.2},
    ])
    spot = infer_spot_from_pairs(chain, years)
    assert spot is not None
    assert 100.0 < spot < 103.0


def test_infer_spot_from_parity_recovers_black_scholes_spot():
    years = 30 / 365
    expected_spot = 100.0
    rows = []
    for strike in (95.0, 100.0, 105.0):
        rows.extend([
            {"strike": strike, "option_type": "C", "mid": black_scholes_price(expected_spot, strike, years, 0.25, "C")},
            {"strike": strike, "option_type": "P", "mid": black_scholes_price(expected_spot, strike, years, 0.25, "P")},
        ])

    assert infer_spot_from_pairs(pd.DataFrame(rows), years) == pytest.approx(expected_spot, rel=1e-9)


def _synthetic_gex_streamer(call_oi: float, put_oi: float) -> DatabentoGammaStreamer:
    """Build a deterministic five-pair chain without touching a live feed."""
    streamer = DatabentoGammaStreamer(["SPX"])
    expiration = current_market_date()
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    _add_synthetic_expiration(streamer, "SPX", expiration, call_oi, put_oi, now)
    streamer.subscription_metadata["markets"] = {
        "SPX": {
            "selected_contract_count": len(streamer.universe),
            "selected_expirations": [
                {
                    "expiration": expiration.isoformat(),
                    "stage": 0,
                    "role": "primary",
                    "selected_strike_pairs": 1,
                }
            ]
        }
    }
    streamer.subscription_metadata["selected_universe_sha256"] = "selected-test-universe"
    streamer.subscription_metadata["universe_provenance"] = {
        "source_sha256": "definition-test-universe",
        "is_fallback": False,
    }
    streamer.live_symbols = streamer.universe["symbol"].astype(str).tolist()
    return streamer


def _add_synthetic_expiration(
    streamer: DatabentoGammaStreamer,
    market: str,
    expiration: date,
    call_oi: float,
    put_oi: float,
    now: float,
    *,
    include_quotes: bool = True,
) -> None:
    roots = {"SPX": "SPXW", "NDX": "NDXP", "VIX": "VIXW"}
    rows = []
    for strike in (98.0, 99.0, 100.0, 101.0, 102.0):
        for option_type, open_interest in (("C", call_oi), ("P", put_oi)):
            symbol = raw_option_symbol(roots[market], expiration, option_type, strike)
            mid = black_scholes_price(100.0, strike, 1 / 365.0, 0.25, option_type)
            rows.append({
                "market": market,
                "symbol": symbol,
                "strike": strike,
                "option_type": option_type,
                "expiration_date": expiration,
                "open_interest": open_interest,
            })
            if include_quotes:
                event_ns = 1_777_000_000_000_000_000 + int(strike * 1_000_000)
                recv_ns = event_ns + 1_000_000
                streamer.quotes[symbol] = {
                    "bid": mid * 0.99,
                    "ask": mid * 1.01,
                    "mid": mid,
                    "received_monotonic": now,
                    "generation": 1,
                    "instrument_id": int(strike * 100) + (0 if option_type == "C" else 1),
                    "ts_event_ns": event_ns,
                    "ts_recv_ns": recv_ns,
                    "ts_index_ns": recv_ns,
                    "ts_event_utc": _timestamp_ns_to_utc_iso(event_ns),
                    "ts_recv_utc": _timestamp_ns_to_utc_iso(recv_ns),
                    "ts_index_utc": _timestamp_ns_to_utc_iso(recv_ns),
                    "processed_at_utc": _timestamp_ns_to_utc_iso(recv_ns + 50_000_000),
                    "receive_to_process_lag_seconds": 0.05,
                    "provider_timestamp_order_valid": True,
                    "mapping_version": f"test-mapping-{market}",
                    "mapping_ts_event_ns": event_ns - 1_000_000,
                    "mapping_start_ts_ns": event_ns - 2_000_000,
                    "mapping_end_ts_ns": event_ns + 86_400_000_000_000,
                }
    streamer.universe = pd.concat([streamer.universe, pd.DataFrame(rows)], ignore_index=True)


def _coverage_only_invalid_streamer() -> DatabentoGammaStreamer:
    """Build 30 calculated strikes from only five complete parity pairs."""
    streamer = DatabentoGammaStreamer(["SPX"])
    expiration = current_market_date()
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    rows = []
    strikes = [96.5 + index * (7.0 / 50.0) for index in range(51)]
    for index, strike in enumerate(strikes):
        for option_type in ("C", "P"):
            symbol = raw_option_symbol("SPXW", expiration, option_type, strike)
            rows.append(
                {
                    "market": "SPX",
                    "symbol": symbol,
                    "strike": strike,
                    "option_type": option_type,
                    "expiration_date": expiration,
                    "open_interest": 1_000.0,
                }
            )
            if index >= 30 or (option_type == "P" and index >= 5):
                continue
            mid = black_scholes_price(
                100.0,
                strike,
                1 / 365.0,
                0.25,
                option_type,
            )
            streamer.quotes[symbol] = {
                "bid": mid * 0.99,
                "ask": mid * 1.01,
                "mid": mid,
                "received_monotonic": now,
                "generation": 1,
                "mapping_version": "coverage-only-test",
            }
    streamer.universe = pd.DataFrame(rows)
    streamer.full_universe = streamer.universe.copy()
    streamer.subscription_metadata["markets"] = {
        "SPX": {
            "selected_contract_count": len(streamer.universe),
            "selected_expirations": [
                {
                    "expiration": expiration.isoformat(),
                    "stage": 0,
                    "role": "primary",
                    "selected_strike_pairs": 51,
                }
            ],
        }
    }
    streamer.subscription_metadata["selected_universe_sha256"] = (
        "coverage-only-selected"
    )
    streamer.subscription_metadata["universe_provenance"] = {
        "source_sha256": "coverage-only-definition",
        "is_fallback": False,
    }
    streamer.live_symbols = streamer.universe["symbol"].astype(str).tolist()
    return streamer


def test_databento_gex_uses_canonical_call_minus_put_sign(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    call_dominant = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)._calculate_pin("SPX")
    put_dominant = _synthetic_gex_streamer(call_oi=100, put_oi=2_000)._calculate_pin("SPX")

    assert call_dominant is not None
    assert call_dominant["net_gex"] > 0
    assert call_dominant["net_gex"] == pytest.approx(
        call_dominant["call_gex_total"] - call_dominant["put_gex_total"]
    )
    assert put_dominant is not None
    assert put_dominant["net_gex"] < 0
    assert put_dominant["net_gex"] == pytest.approx(
        put_dominant["call_gex_total"] - put_dominant["put_gex_total"]
    )


def test_point_in_time_lineage_is_retained_in_calculation_inputs(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)
    for quote in streamer.quotes.values():
        quote["received_monotonic"] = float(quote["received_monotonic"]) - 0.25

    result = streamer._calculate_pin("SPX", capture_inputs=True)

    assert result is not None
    assert result["latest_ts_event_ns"] is not None
    assert result["latest_ts_recv_ns"] >= result["latest_ts_event_ns"]
    assert result["observation_index_ns"] == result["latest_ts_recv_ns"]
    assert result["quote_age_seconds"] > 0
    assert result["receive_to_process_lag_p95_seconds"] == pytest.approx(0.05)
    assert result["expected_quote_count"] == len(streamer.universe)
    assert result["selected_primary_pair_count"] == 1
    assert result["expected_primary_pair_count"] == 5
    assert result["primary_pair_coverage_ratio"] == pytest.approx(1.0)
    assert result["contributing_crossed_quote_count"] == 0
    assert result["instrument_definition_version"] == "universe-sha256:definition-test-universe"
    assert result["symbol_mapping_version"]
    assert result["mapping_version_count"] == 1
    assert result["mapping_version_missing_count"] == 0
    inputs = result["_calculation_inputs"]
    assert inputs["input_schema_version"] == "gamma-inputs-v2-point-in-time"
    assert inputs["raw_fresh_chain_rows"]
    row = inputs["raw_fresh_chain_rows"][0]
    assert row["ts_event_ns"] <= row["ts_recv_ns"]
    assert row["mapping_version"] == "test-mapping-SPX"


def test_health_reports_backend_authoritative_market_subscription_status():
    streamer = _synthetic_gex_streamer(call_oi=120.0, put_oi=80.0)

    health = streamer.get_health()

    status = health["market_subscription_status"]["SPX"]
    assert status["requested"] is True
    assert status["selected_contract_count"] == len(streamer.universe)
    assert status["full_contract_count"] == 0
    assert status["selected_expirations"][0]["role"] == "primary"


def test_health_preserves_vix_forward_context_subscription_authority():
    streamer = DatabentoGammaStreamer(["VIX"])
    streamer.subscription_metadata["markets"] = {
        "VIX": {
            "selected_contract_count": 2,
            "full_contract_count": 2,
            "selected_expirations": [
                {
                    "expiration": "2026-09-16",
                    "stage": 0,
                    "role": "primary",
                    "authority": "vix_forward_expiration_context_only",
                    "context_only": True,
                    "same_day_authority": False,
                    "selection_basis": (
                        "vix_last_trading_day_precedes_settlement_date"
                    ),
                }
            ],
            "primary_expiration_authority": (
                "vix_forward_expiration_context_only"
            ),
            "primary_expiration_context_only": True,
            "primary_expiration_same_day_authority": False,
            "primary_expiration_selection_basis": (
                "vix_last_trading_day_precedes_settlement_date"
            ),
            "settlement_ineligible_contract_count": 96,
            "settlement_ineligible_expirations": [
                {
                    "expiration": "2026-09-09",
                    "contracts": 96,
                    "reason": "VIX_AM_SETTLED_LAST_TRADING_DAY_PASSED",
                }
            ],
        }
    }

    status = streamer.get_health()["market_subscription_status"]["VIX"]

    assert status["primary_expiration_authority"] == (
        "vix_forward_expiration_context_only"
    )
    assert status["primary_expiration_context_only"] is True
    assert status["primary_expiration_same_day_authority"] is False
    assert status["settlement_ineligible_contract_count"] == 96
    assert status["settlement_ineligible_expirations"][0]["reason"] == (
        "VIX_AM_SETTLED_LAST_TRADING_DAY_PASSED"
    )


def test_dbn_timestamp_helpers_reject_missing_and_unsigned_sentinel():
    class Record:
        ts_event = 1_777_000_000_000_000_000
        ts_recv = 2**64 - 1

    record = Record()
    assert _record_timestamp_ns(record, "ts_event") == record.ts_event
    assert _record_timestamp_ns(record, "ts_recv") is None
    assert _record_timestamp_ns(record, "missing") is None
    assert _timestamp_ns_to_utc_iso(record.ts_event).endswith("+00:00")


def test_dataframe_records_normalizes_nan_to_canonical_json_null():
    exact_ns = 1_787_665_219_000_000_000
    source = [{"ts_recv_ns": exact_ns, "mapping": None}, {"ts_recv_ns": None, "mapping": "v1"}]
    frame = pd.DataFrame(source)
    frame["ts_recv_ns"] = pd.array([row["ts_recv_ns"] for row in source], dtype="Int64")
    records = _dataframe_records(frame)

    assert records[0]["mapping"] is None
    assert records[0]["ts_recv_ns"] == exact_ns
    assert records[1]["ts_recv_ns"] is None
    assert "NaN" not in json.dumps(records, allow_nan=False)


def test_processing_clock_telemetry_separates_material_skew_from_jitter():
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer._receive_lag_window.extend([-0.010] * 49 + [0.025])

    within_tolerance = streamer._processing_clock_telemetry()

    assert within_tolerance["status"] == "synchronized"
    assert within_tolerance["material_negative_count"] == 0
    streamer._receive_lag_window.clear()
    streamer._receive_lag_window.extend([-0.250] * 49 + [0.025])

    skewed = streamer._processing_clock_telemetry()

    assert skewed["status"] == "unsynchronized"
    assert skewed["material_negative_ratio"] == pytest.approx(49 / 50)


def test_gex_invariant_errors_fail_closed_on_sign_reversal():
    assert gex_invariant_errors(10.0, 3.0, 13.0, 7.0) == []
    errors = gex_invariant_errors(10.0, 3.0, 13.0, -7.0)
    assert any("call_gex_total - put_gex_total" in error for error in errors)


def test_spx_missing_primary_quotes_never_promotes_shadow_expiration(monkeypatch):
    trading_date = date(2026, 8, 25)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    streamer = DatabentoGammaStreamer(["SPX"])
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    _add_synthetic_expiration(streamer, "SPX", trading_date, 2_000, 100, now, include_quotes=False)
    shadow_expiration = trading_date + timedelta(days=3)
    _add_synthetic_expiration(streamer, "SPX", shadow_expiration, 2_000, 100, now)
    streamer.subscription_metadata["markets"] = {
        "SPX": {
            "selected_expirations": [
                {"expiration": trading_date.isoformat(), "stage": 0, "role": "primary"},
                {"expiration": shadow_expiration.isoformat(), "stage": 1, "role": "1-3DTE"},
            ]
        }
    }

    assert streamer._calculate_pin("SPX") is None
    assert "PRIMARY_EXPIRATION_NO_CURRENT_QUOTES" in streamer.formula_validation_errors["SPX"]
    assert streamer.last_calculation_diagnostics["SPX"]["fresh_chain_rows"] == 10
    assert "PRIMARY_EXPIRATION_NO_CURRENT_QUOTES" in str(
        streamer.last_calculation_diagnostics["SPX"]["failure_reason"]
    )


def test_fresh_spx_0dte_remains_primary_when_shadow_is_available(monkeypatch):
    trading_date = date(2026, 8, 25)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = DatabentoGammaStreamer(["SPX"])
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    _add_synthetic_expiration(streamer, "SPX", trading_date, 2_000, 100, now)
    shadow_expiration = trading_date + timedelta(days=3)
    _add_synthetic_expiration(streamer, "SPX", shadow_expiration, 100, 2_000, now)
    streamer.subscription_metadata["markets"] = {
        "SPX": {
            "selected_expirations": [
                {"expiration": trading_date.isoformat(), "stage": 0, "role": "primary"},
                {"expiration": shadow_expiration.isoformat(), "stage": 1, "role": "1-3DTE"},
            ]
        }
    }
    streamer.subscription_metadata["markets"]["SPX"]["selected_expirations"][0]["selected_strike_pairs"] = 5

    result = streamer._calculate_pin("SPX")

    assert result is not None
    assert result["primary_expiration"] == trading_date.isoformat()
    assert result["same_day_profile_available"] is True
    assert result["same_day_target"] == result["primary_expiration_target"]
    assert {profile["expiration"] for profile in result["expiration_profiles"]} == {
        trading_date.isoformat(),
        shadow_expiration.isoformat(),
    }


def test_primary_pair_coverage_uses_the_calculation_band_not_far_tail_strikes(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)
    expiration = current_market_date()
    far_rows = []
    for strike in range(150, 250):
        for option_type in ("C", "P"):
            far_rows.append({
                "market": "SPX",
                "symbol": raw_option_symbol("SPXW", expiration, option_type, float(strike)),
                "strike": float(strike),
                "option_type": option_type,
                "expiration_date": expiration,
                "open_interest": 1.0,
            })
    streamer.universe = pd.concat(
        [streamer.universe, pd.DataFrame(far_rows)],
        ignore_index=True,
    )
    primary_plan = streamer.subscription_metadata["markets"]["SPX"]["selected_expirations"][0]
    primary_plan["selected_strike_pairs"] = 105

    result = streamer._calculate_pin("SPX")

    assert result is not None
    diagnostics = streamer.last_calculation_diagnostics["SPX"]
    assert diagnostics["selected_primary_pair_count"] == 105
    assert diagnostics["expected_primary_pair_count"] == 5
    assert diagnostics["paired_primary_pair_count"] == 5
    assert diagnostics["primary_pair_coverage_ratio"] == pytest.approx(1.0)


def test_low_primary_pair_coverage_retains_diagnostic_levels_but_stays_invalid(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)
    expiration = current_market_date()
    unquoted_rows = []
    for index in range(46):
        strike = 96.01 + index * 0.08
        for option_type in ("C", "P"):
            unquoted_rows.append(
                {
                    "market": "SPX",
                    "symbol": raw_option_symbol(
                        "SPXW", expiration, option_type, float(strike)
                    ),
                    "strike": float(strike),
                    "option_type": option_type,
                    "expiration_date": expiration,
                    "open_interest": 1.0,
                }
            )
    streamer.universe = pd.concat(
        [streamer.universe, pd.DataFrame(unquoted_rows)],
        ignore_index=True,
    )
    primary_plan = streamer.subscription_metadata["markets"]["SPX"][
        "selected_expirations"
    ][0]
    primary_plan["selected_strike_pairs"] = 51

    result = streamer._calculate_pin("SPX")

    assert result is not None
    assert result["validation_is_valid"] is False
    assert result["gamma_excluded_from_model"] is True
    assert result["price"] > 0.0
    assert result["gamma_pin"] > 0.0
    assert result["primary_pair_coverage_ratio"] < 0.1
    assert result["validation_failure_reasons"][0].startswith(
        "PRIMARY_PAIR_COVERAGE_LOW:"
    )
    assert result["formula_health_reasons"] == result["validation_failure_reasons"]
    assert "PRIMARY_PAIR_COVERAGE_LOW" in streamer.formula_validation_errors["SPX"]


def test_primary_pair_coverage_failure_alone_never_promotes_calculated_surface(
    monkeypatch,
):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _coverage_only_invalid_streamer()

    result = streamer._calculate_pin("SPX")

    assert result is not None
    assert result["strike_count"] == 30
    assert result["paired_quote_count"] == 5
    assert result["expected_primary_pair_count"] == 51
    assert result["validation_failure_reasons"] == [
        "PRIMARY_PAIR_COVERAGE_LOW: "
        f"{result['primary_pair_coverage_ratio']} < 0.1"
    ]
    assert result["validation_is_valid"] is False
    assert result["gamma_excluded_from_model"] is True
    assert result["price"] > 0.0
    assert result["gamma_pin"] > 0.0


def test_invalid_input_capture_uses_invalid_snapshot_cadence(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)

    suppressed = streamer._calculate_pin(
        "SPX",
        capture_inputs=True,
        capture_invalid_inputs=False,
    )
    captured = streamer._calculate_pin(
        "SPX",
        capture_inputs=False,
        capture_invalid_inputs=True,
    )

    assert suppressed is not None and suppressed["validation_is_valid"] is False
    assert "_calculation_inputs" not in suppressed
    assert captured is not None and captured["validation_is_valid"] is False
    assert "_calculation_inputs" in captured


def test_downstream_failure_preserves_prior_low_coverage_reason(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _coverage_only_invalid_streamer()
    streamer.handoff_status = "active"
    monkeypatch.setattr(
        streamer,
        "_processing_clock_telemetry",
        lambda: {"status": "unsynchronized"},
    )

    result = streamer._calculate_pin("SPX")

    assert result is None
    reason = streamer.formula_validation_errors["SPX"]
    assert "PRIMARY_PAIR_COVERAGE_LOW" in reason
    assert "PROCESSING_CLOCK_NOT_SYNCHRONIZED" in reason
    assert streamer.last_calculation_diagnostics["SPX"]["failure_reason"] == reason


def test_vix_planned_nearest_future_expiration_is_allowed(monkeypatch):
    trading_date = date(2026, 8, 25)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = DatabentoGammaStreamer(["VIX"])
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    primary_expiration = trading_date + timedelta(days=1)
    _add_synthetic_expiration(streamer, "VIX", primary_expiration, 2_000, 100, now)
    streamer.subscription_metadata["markets"] = {
        "VIX": {
            "selected_expirations": [
                {"expiration": primary_expiration.isoformat(), "stage": 0, "role": "primary"}
            ]
        }
    }

    result = streamer._calculate_pin("VIX")

    assert result is not None
    assert result["primary_expiration"] == primary_expiration.isoformat()
    assert result["same_day_profile_available"] is False
    assert result["same_day_target"] is None
    assert result["primary_expiration_target"] is not None
    assert result["primary_expiration_authority"] == (
        "vix_forward_expiration_context_only"
    )
    assert result["primary_expiration_context_only"] is True
    assert result["same_day_authority"] is False
    assert result["selected_target_mode"] == "primary_expiration_forward_context"


def test_vix_same_day_primary_is_rejected_by_planned_expiration_guard(monkeypatch):
    trading_date = date(2026, 8, 25)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    streamer = DatabentoGammaStreamer(["VIX"])
    streamer.subscription_metadata["markets"] = {
        "VIX": {
            "selected_expirations": [
                {
                    "expiration": trading_date.isoformat(),
                    "stage": 0,
                    "role": "primary",
                }
            ]
        }
    }

    primary, reason = streamer._planned_primary_expiration("VIX")

    assert primary is None
    assert reason is not None
    assert reason.startswith("VIX_AM_SETTLED_PRIMARY_NOT_FORWARD:")


def test_vix_same_day_primary_is_rejected_by_opening_reference_guard():
    trading_date = date(2026, 8, 25)
    streamer = DatabentoGammaStreamer(["VIX"])
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    streamer.subscription_metadata["markets"] = {
        "VIX": {
            "selected_expirations": [
                {
                    "expiration": trading_date.isoformat(),
                    "stage": 0,
                    "role": "primary",
                }
            ]
        }
    }

    snapshot, reason = streamer._snapshot_opening_reference_inputs(
        "VIX",
        trading_date=trading_date,
    )

    assert snapshot is None
    assert reason == "VIX_AM_SETTLED_PRIMARY_NOT_FORWARD"


def test_structured_invalid_vix_snapshot_exposes_diagnostic_public_aliases_and_abstains(
    monkeypatch,
):
    trading_date = date(2026, 8, 25)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = DatabentoGammaStreamer(["VIX"])
    now = time.monotonic()
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = now - 1.0
    primary_expiration = trading_date + timedelta(days=1)
    _add_synthetic_expiration(streamer, "VIX", primary_expiration, 2_000, 100, now)
    streamer.subscription_metadata["markets"] = {
        "VIX": {
            "selected_expirations": [
                {
                    "expiration": primary_expiration.isoformat(),
                    "stage": 0,
                    "role": "primary",
                }
            ]
        }
    }

    result = streamer._calculate_pin("VIX")

    assert result is not None
    assert result["price"] > 0.0
    assert result["validation_is_valid"] is False
    assert result["gamma_excluded_from_model"] is True
    assert result["validation_failure_reasons"][0] == (
        "CHAIN_TOO_THIN: Only 5 primary-expiration strikes (min: 30)"
    )

    payload = streamer._snapshot_payload(result)
    assert payload["price"] == pytest.approx(payload["spot_last"])
    assert payload["gamma_pin"] == payload["primary_gamma_pin_strike"]
    assert payload["max_pain"] == payload["max_pain_strike"]
    assert payload["validation_is_valid"] is False
    assert payload["validation_status"] == "invalid"
    assert payload["gamma_excluded_from_model"] is True
    assert payload["validation_failure_reasons"] == result["validation_failure_reasons"]
    assert payload["primary_expiration_authority"] == (
        "vix_forward_expiration_context_only"
    )
    assert payload["primary_expiration_context_only"] is True
    assert payload["same_day_authority"] is False
    assert payload["selected_target_mode"] == "primary_expiration_forward_context"
    assert payload_is_usable_prediction(payload) is False

    public_payload = with_gex_level_semantics(payload)
    assert public_payload["forecast_state"] == "ABSTAIN"
    assert public_payload["decision_grade"] is False
    assert public_payload["usable_for_prediction"] is False


def test_snapshot_payload_diagnostic_aliases_preserve_explicit_zero_and_none():
    streamer = DatabentoGammaStreamer(["VIX"])
    reasons = ["CHAIN_TOO_THIN: diagnostic fixture"]
    payload = streamer._snapshot_payload(
        {
            "symbol": "VIX",
            "spot_last": 0.0,
            "price": 99.0,
            "primary_gamma_pin_strike": 0.0,
            "gamma_pin": 100.0,
            "max_pain_strike": None,
            "max_pain": 101.0,
            "validation_is_valid": False,
            "validation_failure_reasons": reasons,
            "gamma_excluded_from_model": True,
        }
    )

    assert payload["spot_last"] == 0.0
    assert payload["price"] == 0.0
    assert payload["primary_gamma_pin_strike"] == 0.0
    assert payload["gamma_pin_strike"] == 0.0
    assert payload["gamma_pin"] == 0.0
    assert payload["max_pain_strike"] is None
    assert payload["max_pain"] is None
    assert payload["validation_is_valid"] is False
    assert payload["validation_failure_reasons"] == reasons
    assert payload["gamma_excluded_from_model"] is True
    assert payload["usable_for_prediction"] is False
    assert payload_is_usable_prediction(payload) is False


def test_expiration_blend_assigns_each_bucket_once():
    profiles = [
        {"bucket": "0DTE", "gross_gex": 10.0},
        {"bucket": "1-3DTE", "gross_gex": 100.0},
        {"bucket": "1-3DTE", "gross_gex": 1.0},
        {"bucket": "4-45DTE", "gross_gex": 20.0},
    ]
    weights = expiration_blend_weights(profiles)

    assert sum(weights) == pytest.approx(1.0)
    assert weights[0] == pytest.approx(0.7)
    assert weights[1] + weights[2] == pytest.approx(0.2)
    assert weights[3] == pytest.approx(0.1)
    assert weights[1] > weights[2]


def test_snapshot_payload_has_truthful_pregates_and_diagnostics(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)
    result = streamer._calculate_pin("SPX")

    assert result is not None
    payload = streamer._snapshot_payload(result)
    assert payload["timestamp"] == result["timestamp"]
    assert payload["zero_gamma"] == result["zero_gamma"]
    assert payload["positive_gex_wall"] == result["positive_gex_wall"]
    assert payload["negative_gex_wall"] == result["negative_gex_wall"]
    assert payload["strike_count"] == 5
    assert payload["nonzero_strike_count"] == 5
    assert payload["validation_is_valid"] is False
    assert payload["gamma_excluded_from_model"] is True
    assert "CHAIN_TOO_THIN" in payload["pregate_reason"]
    assert payload["gamma_by_distance"] is not None
    assert payload["truncation"] is not None
    assert payload["vol_regime"] in {"LOW", "MEDIUM", "HIGH"}
    assert payload["confidence"] is not None
    assert payload["dispersion_ratio"] is not None
    assert payload["inference_feature_schema_version"] == "databento-gex-structure-v1"
    assert payload["inference_features"] == result["inference_features"]
    assert payload["top_strike_concentration"] == pytest.approx(
        payload["top_strike_share"]
    )
    assert all(row["abs_gex"] == pytest.approx(abs(row["net_gex"])) for row in payload["top_strikes_by_abs_gex"])
    assert all(
        row["call_calculated_contracts"] + row["put_calculated_contracts"] > 0
        for row in payload["top_strikes_by_abs_gex"]
    )
    assert all(
        row["call_calculation_status"] in {"calculated", "excluded"}
        and row["put_calculation_status"] in {"calculated", "excluded"}
        and isinstance(row["call_exclusion_reasons"], list)
        and isinstance(row["put_exclusion_reasons"], list)
        for row in payload["top_strikes_by_abs_gex"]
    )
    assert payload["calculation_exclusion_counts"] == result[
        "calculation_exclusion_counts"
    ]


def test_structured_invalid_snapshot_is_audited_but_never_writes_gamma_pin(tmp_path, monkeypatch):
    import backend.database as backend_database
    import database as root_database

    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    audit_payloads = []
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audit_payloads.append)
    monkeypatch.setattr(root_database, "save_gamma_snapshot", lambda **_kwargs: pytest.fail("invalid snapshot wrote gamma pin"))
    monkeypatch.setattr(
        backend_database,
        "save_market_snapshot",
        lambda *_args, **_kwargs: pytest.fail(
            "invalid snapshot wrote generic market price"
        ),
    )
    streamer = _synthetic_gex_streamer(call_oi=2_000, put_oi=100)
    streamer.exports_dir = tmp_path / "exports"
    streamer.audit_dir = tmp_path / "audit"
    streamer.snapshot_interval = 0
    result = streamer._calculate_pin("SPX")

    assert result is not None and result["validation_is_valid"] is False
    streamer._write_snapshot(result)

    assert len(audit_payloads) == 1
    assert audit_payloads[0]["validation_is_valid"] is False
    assert audit_payloads[0]["spot_last"] == pytest.approx(result["price"])
    assert audit_payloads[0]["primary_gamma_pin_strike"] == pytest.approx(
        result["gamma_pin"]
    )
    assert audit_payloads[0]["usable_for_prediction"] is False
    assert not list((tmp_path / "exports").rglob("*.ndjson"))
    assert list((tmp_path / "audit").rglob("*.json"))


def test_invalid_snapshot_never_writes_gamma_pin_bucket(tmp_path, monkeypatch):
    import database as root_database

    audit_payloads = []
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audit_payloads.append)

    def fail_if_gamma_pin_is_written(**_kwargs):
        raise AssertionError("invalid snapshots must not write gamma_pin_snapshots")

    monkeypatch.setattr(root_database, "save_gamma_snapshot", fail_if_gamma_pin_is_written)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.exports_dir = tmp_path / "exports"
    streamer.audit_dir = tmp_path / "audit"
    streamer.snapshot_interval = 0
    streamer.subscription_metadata.update({
        "selected_universe_sha256": "selected-hash",
        "universe_provenance": {
            "mode": "prior_day_cache",
            "source_path": "C:/safe/cache.csv",
            "source_sha256": "source-hash",
            "is_fallback": True,
        },
        "markets": {
            "SPX": {
                "selected_expirations": [
                    {"expiration": "2026-08-25", "stage": 0, "role": "primary"}
                ]
            }
        },
    })
    prior_valid_time = datetime(2026, 8, 25, 13, 30)
    streamer.latest_pins["SPX"] = {"validation_is_valid": True, "price": 7500.0}
    streamer.last_message_time["SPX"] = prior_valid_time

    streamer._write_invalid_snapshot("SPX", "unit-test invalid input")

    assert len(audit_payloads) == 1
    assert audit_payloads[0]["validation_is_valid"] is False
    assert audit_payloads[0]["validation_failure_reasons"] == ["unit-test invalid input"]
    assert audit_payloads[0]["universe_sha256"] == "source-hash"
    assert audit_payloads[0]["selected_universe_sha256"] == "selected-hash"
    assert audit_payloads[0]["universe_provenance"]["is_fallback"] is True
    assert audit_payloads[0]["primary_expiration"] == "2026-08-25"
    assert audit_payloads[0]["processing_clock_telemetry"]["status"] == "unknown"
    assert "SPX" not in streamer.latest_pins
    assert streamer.last_message_time["SPX"] == prior_valid_time
    assert "SPX" in streamer.last_diagnostic_time
    assert "SPX" not in streamer._last_snapshot_write
    assert "SPX" in streamer._last_invalid_snapshot_write
    assert not list((tmp_path / "exports").rglob("*.ndjson"))
    assert list((tmp_path / "audit").rglob("*.json"))


def test_valid_snapshot_writes_canonical_export_without_using_invalid_throttle(
    tmp_path,
    monkeypatch,
):
    import backend.database as backend_database
    import database as root_database

    audit_payloads = []
    gamma_payloads = []
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audit_payloads.append)
    monkeypatch.setattr(
        root_database,
        "save_gamma_snapshot",
        lambda **payload: gamma_payloads.append(payload),
    )
    monkeypatch.setattr(backend_database, "save_market_snapshot", lambda *_args, **_kwargs: None)

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.exports_dir = tmp_path / "exports"
    streamer.audit_dir = tmp_path / "audit"
    streamer.snapshot_interval = 0
    streamer._last_invalid_snapshot_write["SPX"] = time.monotonic()
    payload = {
        "generated_at_utc": "2026-09-01T14:30:00+00:00",
        "timestamp_utc": "2026-09-01T14:30:00+00:00",
        "symbol": "SPX",
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "primary_gamma_pin_strike": 6500.0,
        "spot_last": 6498.0,
        "gross_gex": 10.0,
        "net_gex": 2.0,
    }
    monkeypatch.setattr(streamer, "_snapshot_payload", lambda _result: payload)

    streamer._write_snapshot({"symbol": "SPX", "price": 6498.0})

    export_files = list((tmp_path / "exports").rglob("*.ndjson"))
    assert len(export_files) == 1
    assert json.loads(export_files[0].read_text(encoding="utf-8"))["validation_is_valid"] is True
    assert len(audit_payloads) == 1
    assert len(gamma_payloads) == 1
    assert "SPX" in streamer._last_snapshot_write


def test_compute_loop_does_not_persist_diagnostics_outside_regular_hours(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.handoff_status = "active"
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: True)
    monkeypatch.setattr(
        streamer,
        "_calculate_pin",
        lambda *_args, **_kwargs: pytest.fail("closed-session calculation ran"),
    )

    streamer._compute_and_publish()

    assert streamer._last_snapshot_write == {}
    assert streamer._last_invalid_snapshot_write == {}


def test_compute_publish_requires_explicit_validation_and_model_eligibility(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.handoff_status = "active"
    streamer.active_generation = 1
    streamer.update_interval = 0
    result = {
        "symbol": "SPX",
        "timestamp": datetime.now(timezone.utc),
        "price": 7500.0,
        "likely_close": 7510.0,
        "gamma_pin": 7505.0,
    }
    monkeypatch.setattr(streamer, "_calculate_pin", lambda *_args, **_kwargs: result)
    written = []
    monkeypatch.setattr(
        streamer,
        "_write_snapshot",
        lambda value: written.append(streamer._snapshot_payload(value)),
    )
    streamer.callbacks.append(
        lambda _payload: pytest.fail("unproven result reached prediction callback")
    )

    streamer._compute_and_publish()

    assert "SPX" not in streamer.latest_pins
    payload = streamer.latest_invalid["SPX"]
    assert payload["validation_is_valid"] is False
    assert payload["gamma_excluded_from_model"] is True
    assert payload["usable_for_prediction"] is False
    assert payload["validation_failure_reasons"] == [
        "SOURCE_VALIDATION_PROOF_MISSING",
        "SOURCE_MODEL_ELIGIBILITY_PROOF_MISSING",
    ]
    assert written == [payload]
    assert "SPX" not in streamer.last_message_time
    assert "SPX" in streamer.last_diagnostic_time


def test_streamer_generates_configured_daily_symbols():
    streamer = DatabentoGammaStreamer(["SPX"])
    config = DatabentoMarketConfig("TEST", "TEST", 5, 7400, 7410)
    symbols = streamer._generate_symbols(config)
    assert len(symbols) == 6
    assert symbols[0].startswith("TEST  ")
    assert symbols[0].endswith("C07400000")
    assert symbols[1].endswith("P07400000")


def test_spx_and_ndx_generate_weekly_and_standard_roots(monkeypatch):
    market_date = date(2026, 9, 4)
    monkeypatch.setattr(
        "backend.databento_streamer.current_market_date", lambda: market_date
    )
    streamer = DatabentoGammaStreamer(["SPX", "NDX"])
    config = DatabentoMarketConfig("SPX", "SPXW", 5, 7400, 7400)
    symbols = streamer._generate_symbols(config)
    assert raw_option_symbol("SPXW", market_date, "C", 7400) in symbols
    assert raw_option_symbol("SPX", market_date, "C", 7400) in symbols


def test_vix_definition_parents_include_weekly_and_standard_roots():
    streamer = DatabentoGammaStreamer(["VIX"])
    config = DatabentoMarketConfig("VIX", "VIXW", 1, 20, 20, days_forward=14)
    assert streamer._definition_parents(config) == ["VIXW.OPT", "VIX.OPT"]


def test_incremental_fresh_quote_counts_age_without_scanning_cache(monkeypatch):
    clock = {"now": 100.0}
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("backend.databento_streamer.QUOTE_FRESHNESS_SECONDS", 10.0)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 2
    streamer.subscription_cutoff_monotonic = 90.0
    symbol = raw_option_symbol("SPXW", current_market_date(), "C", 100.0)
    streamer.universe = pd.DataFrame([{
        "market": "SPX", "symbol": symbol, "strike": 100.0, "option_type": "C",
        "expiration_date": current_market_date(), "open_interest": 100.0,
    }])
    streamer._ensure_universe_index()

    streamer._store_quote(symbol, {"received_monotonic": 100.0, "generation": 2})
    assert streamer._fresh_quote_counts() == {"SPX": 1}
    clock["now"] = 105.0
    streamer._store_quote(symbol, {"received_monotonic": 105.0, "generation": 2})
    assert streamer._fresh_quote_counts() == {"SPX": 1}

    class NoItemsDict(dict):
        def items(self):
            raise AssertionError("freshness counter scanned the quote cache")

    streamer.quotes = NoItemsDict(streamer.quotes)
    clock["now"] = 116.0
    assert streamer._fresh_quote_counts() == {"SPX": 0}
    streamer.active_generation = 3
    streamer.subscription_cutoff_monotonic = 200.0
    streamer._reset_fresh_quote_index()
    assert streamer._fresh_quote_counts() == {"SPX": 0}


def test_closed_to_open_transition_resets_progress_and_graces_watchdog(monkeypatch):
    clock = {"now": 100.0}
    phase = {"closed": True}
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: phase["closed"])
    monkeypatch.setattr("backend.databento_streamer.OPEN_TRANSITION_GRACE_SECONDS", 30.0)
    monkeypatch.setattr("backend.databento_streamer.STREAM_STALL_SECONDS", 20.0)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer._last_progress_monotonic = 1.0

    assert streamer._stream_stalled() is False
    assert streamer._last_progress_monotonic == 0.0
    phase["closed"] = False
    clock["now"] = 110.0
    assert streamer._stream_stalled() is False
    assert streamer._last_progress_monotonic == 110.0
    assert streamer._open_grace_until_monotonic == 140.0
    clock["now"] = 139.0
    assert streamer._stream_stalled() is False
    clock["now"] = 141.0
    assert streamer._stream_stalled() is True


def test_mid_session_cold_start_does_not_invent_an_open_transition(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    streamer = DatabentoGammaStreamer(["SPX"])

    assert streamer._watchdog_session_blocked() is False
    assert streamer._market_was_closed is False
    assert streamer._last_progress_monotonic == 100.0
    assert streamer._open_grace_until_monotonic == 0.0


def test_opening_watchdog_grace_does_not_suppress_unsuspended_compute(monkeypatch):
    clock = {"now": 1_010.0}
    calculations = []
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.handoff_status = "active"
    streamer._market_was_closed = False
    streamer._open_grace_until_monotonic = 1_040.0
    streamer._compute_suspended_until_monotonic = 0.0
    monkeypatch.setattr(
        streamer,
        "_calculate_pin",
        lambda market, **_kwargs: calculations.append(market),
    )
    invalid_writes = []
    monkeypatch.setattr(
        streamer,
        "_write_invalid_snapshot",
        lambda market, _reason: invalid_writes.append(market),
    )

    streamer._compute_and_publish()

    assert calculations == ["SPX"]
    assert invalid_writes == ["SPX"]
    assert streamer._open_grace_until_monotonic == 1_040.0


def test_watchdog_never_forces_universe_refresh_while_closed(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.client = FakeClient()
    streamer.handoff_status = "active"
    streamer._universe_built_monotonic = 1.0
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: True)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 10_000.0)
    monkeypatch.setattr("backend.databento_streamer.ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", True)

    def stop_after_one_check(_seconds):
        streamer.is_running = False

    monkeypatch.setattr("backend.databento_streamer.time.sleep", stop_after_one_check)
    streamer._watchdog_loop()

    assert streamer.client.stop_calls == 0
    assert streamer.reconnect_attempts == 0


def test_watchdog_requests_only_one_reconnect_until_stream_thread_resets_it(
    monkeypatch,
):
    class FakeClient:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.client = FakeClient()
    streamer.handoff_status = "active"
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: True)
    monkeypatch.setattr(
        "backend.databento_streamer.ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", False
    )
    sleeps = 0

    def stop_after_repeated_checks(_seconds):
        nonlocal sleeps
        sleeps += 1
        if sleeps >= 3:
            streamer.is_running = False

    monkeypatch.setattr(
        "backend.databento_streamer.time.sleep", stop_after_repeated_checks
    )

    streamer._watchdog_loop()

    assert streamer.client.stop_calls == 1
    assert streamer.reconnect_attempts == 1
    assert streamer._watchdog_stop_reason == (
        "Databento watchdog forced reconnect (stalled quote progress)"
    )


def test_watchdog_does_not_interrupt_client_while_subscription_is_in_flight(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.client = FakeClient()
    streamer.handoff_status = "subscribing"
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: True)
    monkeypatch.setattr(
        "backend.databento_streamer.ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", False
    )

    def stop_after_one_check(_seconds):
        streamer.is_running = False

    monkeypatch.setattr("backend.databento_streamer.time.sleep", stop_after_one_check)

    streamer._watchdog_loop()

    assert streamer.client.stop_calls == 0
    assert streamer.reconnect_attempts == 0
    assert streamer._watchdog_stop_reason is None


def test_watchdog_never_stops_a_replacement_client_from_stale_observation(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    original = FakeClient()
    replacement = FakeClient()
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.client = original
    streamer.handoff_status = "active"

    def replace_during_stall_check():
        with streamer._client_lock:
            streamer.client = replacement
        return True

    monkeypatch.setattr(streamer, "_stream_stalled", replace_during_stall_check)
    monkeypatch.setattr(streamer, "_universe_refresh_due", lambda: False)
    monkeypatch.setattr(
        "backend.databento_streamer.ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", False
    )

    def stop_after_one_check(_seconds):
        streamer.is_running = False

    monkeypatch.setattr("backend.databento_streamer.time.sleep", stop_after_one_check)

    streamer._watchdog_loop()

    assert original.stop_calls == 0
    assert replacement.stop_calls == 0
    assert streamer.reconnect_attempts == 0
    assert streamer._watchdog_stop_reason is None


def test_transport_reconnect_reuses_same_day_bounded_in_memory_universe(monkeypatch):
    trading_date = current_market_date()
    symbol = raw_option_symbol("SPXW", trading_date, "C", 100.0)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.universe = pd.DataFrame(
        [{
            "market": "SPX",
            "symbol": symbol,
            "strike": 100.0,
            "option_type": "C",
            "expiration_date": trading_date,
            "open_interest": 1.0,
        }]
    )
    streamer.live_symbols = [symbol]
    streamer.subscription_metadata = {
        "universe_provenance": {"trading_date": trading_date.isoformat()}
    }

    class UnexpectedHistorical:
        def __init__(self, _api_key):
            raise AssertionError("transport reconnect repeated historical discovery")

    monkeypatch.setattr("backend.databento_streamer.db.Historical", UnexpectedHistorical)

    streamer._build_universe()

    assert streamer.live_symbols == [symbol]


def test_transport_reconnect_promotes_staged_current_day_cache_over_fallback(
    tmp_path, monkeypatch
):
    trading_date = date(2026, 9, 8)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.cache_dir = tmp_path

    fallback_symbols = [
        raw_option_symbol("SPXW", trading_date, option_type, 7600.0)
        for option_type in ("C", "P")
    ]
    streamer.universe = pd.DataFrame(
        [
            {
                "market": "SPX",
                "symbol": symbol,
                "strike": 7600.0,
                "option_type": option_type,
                "expiration_date": trading_date,
                "open_interest": 1.0,
            }
            for symbol, option_type in zip(fallback_symbols, ("C", "P"))
        ]
    )
    streamer.live_symbols = fallback_symbols
    streamer.subscription_metadata = {
        "universe_provenance": {
            "trading_date": trading_date.isoformat(),
            "source_date": "2026-09-04",
            "is_fallback": True,
        }
    }

    current_symbols = [
        raw_option_symbol("SPXW", trading_date, option_type, 7605.0)
        for option_type in ("C", "P")
    ]
    pd.DataFrame(
        [
            {
                "market": "SPX",
                "symbol": symbol,
                "strike": 7605.0,
                "option_type": option_type,
                "expiration_date": trading_date,
                "open_interest": 2.0,
            }
            for symbol, option_type in zip(current_symbols, ("C", "P"))
        ]
    ).to_csv(streamer._cache_path(trading_date), index=False)

    class UnexpectedHistorical:
        def __init__(self, _api_key):
            raise AssertionError("cache promotion opened Historical")

    monkeypatch.setattr("backend.databento_streamer.db.Historical", UnexpectedHistorical)

    streamer._build_universe()

    assert set(streamer.live_symbols) == set(current_symbols)
    provenance = streamer.subscription_metadata["universe_provenance"]
    assert provenance["mode"] == "current_day_cache"
    assert provenance["source_date"] == trading_date.isoformat()
    assert provenance["is_fallback"] is False


def test_transport_reconnect_retains_fallback_when_staged_current_cache_is_invalid(
    tmp_path, monkeypatch
):
    trading_date = date(2026, 9, 8)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: trading_date)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer.cache_dir = tmp_path
    fallback_symbol = raw_option_symbol("SPXW", trading_date, "C", 7600.0)
    streamer.universe = pd.DataFrame(
        [
            {
                "market": "SPX",
                "symbol": fallback_symbol,
                "strike": 7600.0,
                "option_type": "C",
                "expiration_date": trading_date,
                "open_interest": 1.0,
            }
        ]
    )
    streamer.live_symbols = [fallback_symbol]
    fallback_provenance = {
        "trading_date": trading_date.isoformat(),
        "source_date": "2026-09-04",
        "is_fallback": True,
    }
    streamer.subscription_metadata = {"universe_provenance": fallback_provenance}
    pd.DataFrame(
        [
            {
                "market": "SPX",
                "symbol": raw_option_symbol("SPXW", trading_date, "C", 7605.0),
                "strike": 7605.0,
                "option_type": "C",
                "expiration_date": trading_date,
                "open_interest": 2.0,
            }
        ]
    ).to_csv(streamer._cache_path(trading_date), index=False)

    class UnexpectedHistorical:
        def __init__(self, _api_key):
            raise AssertionError("invalid cache fallback opened Historical")

    monkeypatch.setattr("backend.databento_streamer.db.Historical", UnexpectedHistorical)

    streamer._build_universe()

    assert streamer.live_symbols == [fallback_symbol]
    assert streamer.subscription_metadata["universe_provenance"] == fallback_provenance


def test_forced_universe_refresh_queries_provider_before_cache_fallback(monkeypatch):
    events = []
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "test-key"
    streamer._universe_built_monotonic = 1.0

    def fake_load_cache(_trading_date, *, ignore_refresh_flag=False):
        events.append(("cache", ignore_refresh_flag))
        return True

    monkeypatch.setattr(streamer, "_load_cached_universe", fake_load_cache)
    monkeypatch.setattr(
        streamer,
        "_load_prior_cached_universe",
        lambda **_kwargs: pytest.fail("same-day cache should be the forced-refresh fallback"),
    )
    monkeypatch.setattr(
        "backend.databento_streamer.db.Historical",
        lambda _api_key: events.append(("provider", False)) or object(),
    )
    monkeypatch.setattr(
        streamer,
        "_available_end",
        lambda *_args: (_ for _ in ()).throw(RuntimeError("provider unavailable")),
    )

    streamer._build_universe(force_refresh=True)

    assert events == [("provider", False), ("cache", True)]
    assert "provider unavailable" in (
        streamer.subscription_metadata["universe_provenance"]["refresh_failure_reason"]
    )


def test_partial_forced_refresh_never_overwrites_validated_current_plan(monkeypatch):
    trading_date = current_market_date()
    expiration = trading_date
    spx_rows = pd.DataFrame(
        [
            {
                "market": "SPX",
                "symbol": raw_option_symbol("SPXW", expiration, option_type, 7600.0),
                "strike": 7600.0,
                "option_type": option_type,
                "expiration_date": expiration,
            }
            for option_type in ("C", "P")
        ]
    )
    streamer = DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.api_key = "test-key"
    streamer._universe_built_monotonic = 1.0
    fallback_calls = []

    def fake_load_cache(_trading_date, *, ignore_refresh_flag=False):
        fallback_calls.append(ignore_refresh_flag)
        return True

    monkeypatch.setattr(streamer, "_load_cached_universe", fake_load_cache)
    monkeypatch.setattr("backend.databento_streamer.db.Historical", lambda _key: object())
    monkeypatch.setattr(
        streamer,
        "_available_end",
        lambda *_args: datetime.combine(
            trading_date, datetime.max.time(), tzinfo=timezone.utc
        ),
    )
    monkeypatch.setattr(
        streamer,
        "_fetch_definition_universe",
        lambda _historical, config, **_kwargs: (
            spx_rows.copy()
            if config.label == "SPX"
            else pd.DataFrame(columns=spx_rows.columns)
        ),
    )
    monkeypatch.setattr(
        streamer,
        "_fetch_open_interest",
        lambda _historical, symbols, **_kwargs: pd.DataFrame(
            {"symbol": symbols, "open_interest": [100.0] * len(symbols)}
        ),
    )
    monkeypatch.setattr(
        streamer,
        "_has_required_primary_pair_coverage",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(
        streamer,
        "_save_cached_universe",
        lambda *_args, **_kwargs: pytest.fail("partial refresh overwrote current cache"),
    )

    streamer._build_universe(force_refresh=True)

    assert fallback_calls == [True]
    assert "partial" in (
        streamer.subscription_metadata["universe_provenance"]["refresh_failure_reason"]
    )


def test_intraday_replay_is_initial_subscription_only_by_default(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.replay_minutes = 2
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: True)
    monkeypatch.setattr("backend.databento_streamer.REPLAY_ON_RECONNECT", False)

    initial_replay_start = streamer._subscription_replay_start()
    streamer._record_subscription_started(initial_replay_start)

    assert initial_replay_start is not None
    assert streamer.last_subscription_replay_start_utc == initial_replay_start
    assert streamer._subscription_replay_start() is None


def test_intraday_replay_on_reconnect_remains_explicitly_opt_in(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.replay_minutes = 2
    streamer.subscription_attempts = 1
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: True)
    monkeypatch.setattr("backend.databento_streamer.REPLAY_ON_RECONNECT", True)

    assert streamer._subscription_replay_start() is not None


def test_mid_session_cold_start_does_not_request_intraday_replay_by_default(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.replay_minutes = 2
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.REPLAY_DURING_REGULAR_SESSION", False)

    assert streamer.subscription_attempts == 0
    assert streamer._subscription_replay_start() is None


def test_watchdog_forces_weekday_trading_date_rollover_even_when_refresh_disabled(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.stop_calls = 0

        def stop(self):
            self.stop_calls += 1

    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.client = FakeClient()
    streamer.handoff_status = "active"
    streamer.subscription_metadata = {
        "universe_provenance": {"trading_date": "2026-08-31"}
    }
    monkeypatch.setattr(
        "backend.databento_streamer.current_market_date", lambda: date(2026, 9, 1)
    )
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    monkeypatch.setattr(streamer, "_universe_refresh_due", lambda: False)
    monkeypatch.setattr(
        "backend.databento_streamer.ALLOW_LIVE_UNIVERSE_REFRESH_RECONNECT", False
    )

    def stop_after_one_check(_seconds):
        streamer.is_running = False

    monkeypatch.setattr("backend.databento_streamer.time.sleep", stop_after_one_check)

    streamer._watchdog_loop()

    assert streamer.client.stop_calls == 1
    assert streamer._watchdog_stop_reason == (
        "Databento watchdog forced reconnect (trading date rollover)"
    )


def test_weekend_does_not_trigger_repeated_trading_date_rollovers(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.subscription_metadata = {
        "universe_provenance": {"trading_date": "2026-09-04"}
    }
    monkeypatch.setattr(
        "backend.databento_streamer.current_market_date", lambda: date(2026, 9, 5)
    )

    assert streamer._universe_trading_date_changed() is False


def test_market_holiday_does_not_trigger_trading_date_rollover(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.subscription_metadata = {
        "universe_provenance": {"trading_date": "2026-09-04"}
    }
    monkeypatch.setattr(
        "backend.databento_streamer.current_market_date", lambda: date(2026, 9, 7)
    )
    monkeypatch.setattr("backend.databento_streamer.is_holiday", lambda _date: True)

    assert streamer._universe_trading_date_changed() is False


def test_provider_warning_and_subscription_telemetry_is_persistent(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.quote_records_seen = 25
    streamer.provider_timestamp_missing_records = 2
    streamer.last_receive_to_process_lag_seconds = -2.5
    streamer.max_receive_to_process_lag_seconds = 2.5
    streamer._receive_lag_window.extend([-2.5] * 50)
    assert streamer._processing_clock_telemetry()["status"] == "unsynchronized"
    streamer._reset_stream_telemetry()
    assert streamer.quote_records_seen == 25
    assert streamer.provider_timestamp_missing_records == 2
    assert streamer.last_receive_to_process_lag_seconds is None
    assert streamer.max_receive_to_process_lag_seconds is None
    assert streamer._processing_clock_telemetry()["status"] == "unknown"

    streamer._install_provider_warning_telemetry()
    try:
        logging.getLogger("databento.live.session").warning(
            "record queue is full: 12,377 pending records"
        )
        logging.getLogger("databento.live.protocol").warning(
            "Slow client detected. Skipped 42 records"
        )
    finally:
        streamer._remove_provider_warning_telemetry()
    assert streamer.provider_queue_full_warnings == 1
    assert streamer.provider_pending_records_peak == 12_377
    assert streamer.provider_slow_client_warnings == 1
    assert streamer.provider_skipped_record_warnings == 1
    assert streamer.provider_skipped_records == 42

    streamer._record_subscription_started()
    assert streamer.subscription_attempts == 1
    assert streamer.last_subscription_utc is not None
    assert streamer.reconnect_attempts == 0
    assert streamer.last_reconnect_utc is None
    streamer._record_reconnect("unit-test reconnect")
    assert streamer.reconnect_attempts == 1
    assert streamer.last_reconnect_reason == "unit-test reconnect"


def test_provider_overload_pauses_compute_and_parses_sdk_queue_wording(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)

    streamer.record_provider_warning(
        "record queue is full; 2,884 record(s) to be processed"
    )

    assert streamer.provider_queue_full_warnings == 1
    assert streamer.provider_pending_records_peak == 2_884
    assert streamer._compute_suspended_until_monotonic > 100.0


def test_compute_worker_yields_during_provider_backpressure(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.is_running = True
    streamer.handoff_status = "active"
    streamer._market_was_closed = False
    streamer._compute_suspended_until_monotonic = 110.0
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)
    monkeypatch.setattr(
        streamer,
        "_calculate_pin",
        lambda *_args, **_kwargs: pytest.fail("calculation must yield to ingestion"),
    )

    streamer._compute_and_publish()


def test_queue_warning_during_one_market_stops_later_market_calculations(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.is_running = True
    streamer.handoff_status = "active"
    streamer._market_was_closed = False
    calculations = []
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)
    monkeypatch.setattr(streamer, "_write_invalid_snapshot", lambda *_args: None)

    def calculate(market, **_kwargs):
        calculations.append(market)
        if market == "SPX":
            streamer.record_provider_warning(
                "record queue is full; 500 record(s) to be processed"
            )
        return None

    monkeypatch.setattr(streamer, "_calculate_pin", calculate)

    streamer._compute_and_publish()

    assert calculations == ["SPX"]
    assert streamer.provider_queue_full_warnings == 1


def test_full_oi_max_pain_is_separate_from_fresh_quote_window(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=200.0, put_oi=100.0)
    expiration = current_market_date()
    extra_rows = []
    for option_type in ("C", "P"):
        extra_rows.append({
            "market": "SPX",
            "symbol": raw_option_symbol("SPXW", expiration, option_type, 80.0),
            "strike": 80.0,
            "option_type": option_type,
            "expiration_date": expiration,
            "open_interest": 1_000_000.0,
        })
    streamer.full_universe = pd.concat(
        [streamer.universe, pd.DataFrame(extra_rows)], ignore_index=True
    )
    provenance = {
        "source_sha256": "full-oi-test-hash",
        "source_date": expiration.isoformat(),
        "provider_statistics_end": f"{expiration.isoformat()}T12:00:00+00:00",
        "is_fallback": False,
    }
    streamer.oi_analytics_by_market = build_full_oi_analytics(
        streamer.full_universe, ["SPX"], as_of=expiration, provenance=provenance
    )

    result = streamer._calculate_pin("SPX")

    assert result is not None
    assert result["max_pain"] == 80.0
    assert result["max_pain_source"] == "full-oi-universe"
    assert result["max_pain_formula_version"] == "full-oi-max-pain-v1"
    assert result["max_pain_as_of"] == provenance["provider_statistics_end"]
    assert result["oi_analytics_provenance"]["universe_sha256"] == "full-oi-test-hash"
    assert result["fresh_quote_count"] == len(streamer.universe)


def test_calculation_uses_preindexed_records_without_dataframe_iterrows(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.validate_underlying", lambda *_args: {})
    streamer = _synthetic_gex_streamer(call_oi=200.0, put_oi=100.0)
    monkeypatch.setattr(
        pd.DataFrame,
        "iterrows",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("whole-chain pandas iterrows was called")
        ),
    )

    assert streamer._calculate_pin("SPX") is not None
