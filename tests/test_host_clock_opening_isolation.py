"""The measurement rollout must not weaken actual opening admission."""
from datetime import timedelta

from backend.host_clock_shadow import HostClockShadow
from tests.test_host_clock_shadow import GOOD
from tests.test_orb_cbbo_capture import cbbo_store, _quotes
from tests.test_orb_e2e_rehearsal import OPEN_UTC, persistent_orb_journal


def test_ready_host_shadow_cannot_admit_unknown_provider_clock(cbbo_store):
    streamer, journal, clock, engine = cbbo_store
    streamer._receive_lag_window.clear()
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    streamer._host_clock_shadow = observer
    telemetry = streamer._processing_clock_telemetry()
    assert telemetry["status"] == "unknown"
    assert telemetry["sample_count"] == 0 and telemetry["minimum_samples"] == 50
    assert telemetry["host_clock_shadow"]["candidate_clock_bootstrap_ready"]
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    result = streamer._capture_opening_reference_for_bucket("SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)
    assert result["recorded"] is False
    assert result["reason"] == "PROCESSING_CLOCK_NOT_SYNCHRONIZED"


def test_failed_host_shadow_cannot_block_valid_provider_capture(cbbo_store):
    streamer, journal, clock, engine = cbbo_store
    streamer._host_clock_shadow = HostClockShadow(query=lambda: GOOD)
    observed = OPEN_UTC + timedelta(seconds=1, milliseconds=100)
    _quotes(streamer, observed)
    result = streamer._capture_opening_reference_for_bucket("SPX", OPEN_UTC, now_utc=lambda: observed, journal=journal)
    assert result["recorded"] is True and result["progress_eligible"] is True


def test_observer_disabled_by_default(cbbo_store):
    streamer, *_ = cbbo_store
    assert streamer._host_clock_shadow is None


def test_explicit_opt_in_constructs_observer_without_starting_it(monkeypatch):
    from tests.test_orb_e2e_rehearsal import _build_streamer

    monkeypatch.setenv("MARKETPIN_HOST_CLOCK_SHADOW_ENABLED", "1")
    streamer = _build_streamer(monkeypatch, epoch="d" * 64)
    assert isinstance(streamer._host_clock_shadow, HostClockShadow)
    assert streamer._host_clock_shadow._thread is None
