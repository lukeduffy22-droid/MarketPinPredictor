"""Offline reproductions of the October 26 opening failure paths."""
from datetime import datetime, timedelta, timezone
import threading
import time

from backend import databento_streamer as module
from backend.databento_streamer import DatabentoGammaStreamer
from tests.test_databento_staged_subscription import _build_staged_streamer


def test_canary_coverage_tracks_requested_stage_and_preserves_half_coverage_gate(monkeypatch):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer.active_generation = 1
    streamer._activate_initial_subscription_stage(1)
    for family in ("SPX", "NDX", "RUT"):
        streamer.latest_pins[family] = {
            "receive_to_process_lag_p95_seconds": 0.1,
            "primary_pair_coverage_ratio": 1.0,
        }
        streamer.last_message_time[family] = datetime.now(timezone.utc).replace(tzinfo=None)
        streamer._fresh_quote_counts_by_market[family] = streamer.subscription_stage_primary_counts[family]
    # One of two primary SPX contracts is fresh. The two deferred contracts
    # must not turn exactly 50% coverage into a false 25% breach.
    streamer._fresh_quote_counts_by_market["SPX"] = 1
    observations = streamer._optional_family_canary_observations()
    assert observations["SPX"]["expected_quote_count"] == 2
    assert streamer.optional_family_canary.preview(observations).rollback_required is False

    # Once shadows are actually subscribed, the same fresh count fails.
    streamer.active_live_symbols = list(streamer.live_symbols)
    observations = streamer._optional_family_canary_observations()
    assert observations["SPX"]["expected_quote_count"] == 4
    assert "CORE_FRESH_QUOTE_COVERAGE:SPX" in streamer.optional_family_canary.preview(observations).reasons


def test_canary_expires_cached_fresh_counts_before_evaluation(monkeypatch):
    streamer = _build_staged_streamer(monkeypatch)
    streamer._prepare_live_subscription_stages()
    streamer.active_generation = 1
    raw = next(s for s in streamer.active_live_symbols if streamer._market_for_raw_symbol(s) == "SPX")
    streamer._store_quote(raw, {"generation": 1, "received_monotonic": time.monotonic() - 31})
    streamer.latest_pins["SPX"] = {}
    streamer.last_message_time["SPX"] = datetime.now(timezone.utc).replace(tzinfo=None)
    assert streamer._fresh_quote_counts_by_market["SPX"] == 1
    assert streamer._optional_family_canary_observations()["SPX"]["fresh_quote_count"] == 0


def test_first_bucket_retries_late_arriving_evidence_without_backfill(monkeypatch):
    opening = datetime(2026, 10, 26, 13, 30, tzinfo=timezone.utc)
    clock = {"now": opening + timedelta(seconds=4.1)}

    class ClockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = clock["now"]
            return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(module, "datetime", ClockDateTime)
    monkeypatch.setattr(module, "live_subscription_window", lambda *_: {
        "state": "regular_session", "cash_open_utc": opening.isoformat(),
    })
    monkeypatch.setattr("backend.opening_capture_receipts.due_checks", lambda *_: [])
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 1
    streamer.is_running = True
    attempts = []
    completed = threading.Event()

    def capture(_market, **kwargs):
        observed = clock["now"]
        intended = kwargs["intended_bucket_utc"]
        eligible = opening + timedelta(seconds=4.7) <= observed < opening + timedelta(seconds=5)
        attempts.append((observed, intended, eligible))
        completed.set()
        if not eligible:
            return {"recorded": False, "reason": "PROCESSING_CLOCK_NOT_SYNCHRONIZED"}
        return {
            "recorded": True, "progress_eligible": True,
            "sample_timestamp_utc": intended.isoformat(),
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": 1,
        }

    class StopAfterOpening:
        def is_set(self):
            return False

        def wait(self, timeout):
            assert completed.wait(1), "sampler worker did not finish"
            # Reaping the future may require one further scheduler iteration.
            threading.Event().wait(0.005)
            clock["now"] += timedelta(seconds=timeout)
            if clock["now"] >= opening + timedelta(seconds=5):
                streamer.is_running = False
            return False

    monkeypatch.setattr(streamer, "_capture_opening_reference_once", capture)
    monkeypatch.setattr(streamer, "_record_opening_reference_failed_attempt", lambda **_: None)
    streamer._stop_event = StopAfterOpening()
    streamer._opening_reference_loop()
    assert any(eligible and intended == opening for _, intended, eligible in attempts)
    assert streamer._orb_reference_recent_buckets_by_market["SPX"][0][0] == opening
    assert all(observed < opening + timedelta(seconds=5) for observed, _, eligible in attempts if eligible)
