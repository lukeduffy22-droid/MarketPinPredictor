"""Opening handoff must not hold the live reader behind forecast persistence."""
import threading
import time
from types import SimpleNamespace

import pytest

import backend.databento_streamer as stream_module


SYMBOLS = {"SPX": "SPXW  261007C07785000", "NDX": "NDXP  261007C30980000"}


def _warming_streamer(monkeypatch):
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX", "NDX"])
    streamer = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.active_generation = 1
    streamer.subscription_cutoff_monotonic = time.monotonic() - 1.0
    streamer.handoff_status = "warming"
    return streamer


def _retain_while_publication_busy(streamer, observations):
    completed = threading.Event()
    results = []

    def record():
        for symbol, generation, received in observations:
            results.append(streamer._record_fresh_market_and_maybe_activate(
                symbol, generation=generation, received_monotonic=received,
            ))
        completed.set()

    with streamer._prediction_publication_lock:
        worker = threading.Thread(target=record, daemon=True)
        worker.start()
        progressed = completed.wait(0.5)
    worker.join(2)
    assert progressed, "warming reader waited for unrelated publication"
    assert not worker.is_alive()
    assert results == [False] * len(observations)


def test_actual_warming_reader_keeps_collecting_quotes_and_clock_samples(monkeypatch):
    streamer = _warming_streamer(monkeypatch)
    streamer.active_generation = 0  # _run_live owns the first connection generation.
    streamer.api_key = "offline-test-only"
    streamer.replay_minutes = 0
    streamer.schema = "cbbo-1s"
    streamer.live_symbols = list(SYMBOLS.values())
    streamer.is_running = True
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **kwargs: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    ready, release_quotes, consumed, finish = (threading.Event() for _ in range(4))
    errors = []

    class FakeLocalClient:
        def __init__(self, **kwargs):
            pass

        def subscribe(self, **kwargs):
            assert kwargs["symbols"] == list(SYMBOLS.values())

        def __iter__(self):
            for instrument, symbol in enumerate(SYMBOLS.values(), 1):
                yield type("SymbolMappingMsg", (), {
                    "instrument_id": instrument, "stype_out_symbol": symbol,
                })()
            ready.set()
            assert release_quotes.wait(2)
            for instrument in (1, 2):
                recv = time.time_ns() // 1_000_000_000 * 1_000_000_000
                yield SimpleNamespace(
                    instrument_id=instrument, rtype=192, ts_recv=recv,
                    ts_index=recv, ts_event=recv,
                    bid_px_00=1_000_000_000, ask_px_00=2_000_000_000,
                )
            consumed.set()
            assert finish.wait(2)
            streamer.is_running = False

        def stop(self):
            pass

        def block_for_close(self, **kwargs):
            pass

    monkeypatch.setattr(stream_module.db, "Live", FakeLocalClient)

    def run():
        try:
            streamer._run_live()
        except BaseException as exc:
            errors.append(exc)

    reader = threading.Thread(target=run, daemon=True)
    reader.start()
    try:
        assert ready.wait(2)
        with streamer._prediction_publication_lock:
            release_quotes.set()
            assert consumed.wait(0.5), "cold handoff blocked the second quote"
            assert streamer.messages_received == 2
            assert streamer._processing_clock_telemetry()["sample_count"] == 2
            assert streamer.handoff_status == "warming"
            assert set(streamer._pending_fresh_market_observations) == {"SPX", "NDX"}
        # No further quote is necessary: the existing watchdog can drain both
        # sole family observations when the publication owner releases its guard.
        monkeypatch.setattr(streamer, "_maybe_promote_deferred_subscription_stage", lambda: None)
        monkeypatch.setattr(streamer, "_confirm_optional_family_canary_rollback", lambda: None)
        monkeypatch.setattr(streamer, "_optional_family_canary_decision", lambda: None)
        monkeypatch.setattr(streamer, "_apply_optional_family_canary_rollback", lambda _: None)
        monkeypatch.setattr(streamer, "_universe_trading_date_changed", lambda: False)
        monkeypatch.setattr(streamer, "_universe_refresh_due", lambda: False)
        monkeypatch.setattr(stream_module.time, "sleep", lambda _: setattr(streamer, "is_running", False))
        streamer._watchdog_loop()
        assert streamer.handoff_status == "active"
        assert streamer._fresh_markets_seen == {"SPX", "NDX"}
        assert not streamer._pending_fresh_market_observations
    finally:
        release_quotes.set()
        finish.set()
        reader.join(2)
        streamer.is_running = False
    assert not reader.is_alive()
    assert not errors
    assert streamer.reconnect_attempts == 0


@pytest.mark.parametrize("change", ["generation", "cutoff", "expired", "future", "terminal"])
def test_pending_observations_revalidate_before_handoff(monkeypatch, change):
    streamer = _warming_streamer(monkeypatch)
    received = time.monotonic()
    _retain_while_publication_busy(streamer, [(s, 1, received) for s in SYMBOLS])
    if change == "generation":
        streamer.active_generation = 2
    elif change == "cutoff":
        streamer.subscription_cutoff_monotonic = received + 0.001
    elif change == "expired":
        monkeypatch.setattr(stream_module.time, "monotonic", lambda: received + stream_module.QUOTE_FRESHNESS_SECONDS + 1)
    elif change == "future":
        monkeypatch.setattr(stream_module.time, "monotonic", lambda: received - 1)
    else:
        streamer.handoff_status = "degraded"
        streamer.handoff_reason = "provider transport failed"
    assert not streamer._try_activate_fresh_handoff()
    assert streamer.handoff_status != "active"


def test_pending_observation_keeps_newest_and_missing_core_still_blocks(monkeypatch):
    streamer = _warming_streamer(monkeypatch)
    received = time.monotonic()
    _retain_while_publication_busy(streamer, [("SPX", 1, received), ("SPX", 1, received - 0.1)])
    assert streamer._pending_fresh_market_observations["SPX"] == (1, received)
    assert not streamer._try_activate_fresh_handoff()
    assert streamer._fresh_market_observations["SPX"] == (1, received)
    assert streamer.handoff_status == "warming"
    assert streamer._record_fresh_market_and_maybe_activate("NDX", generation=1)


def test_timeout_recovery_retains_freshness_and_exact_reason_gate(monkeypatch):
    streamer = _warming_streamer(monkeypatch)
    streamer.handoff_status = "degraded"
    streamer.handoff_reason = "Fresh post-refresh quotes did not arrive before timeout"
    now = time.monotonic()
    streamer._fresh_markets_seen.add("SPX")
    streamer._fresh_market_observations["SPX"] = (1, now - stream_module.QUOTE_FRESHNESS_SECONDS - 1)
    _retain_while_publication_busy(streamer, [("NDX", 1, now)])
    assert not streamer._try_activate_fresh_handoff()
    assert streamer._record_fresh_market_and_maybe_activate("SPX", generation=1)


@pytest.mark.parametrize("change", ["expired", "generation", "cutoff"])
def test_warming_revalidates_previously_drained_required_family(monkeypatch, change):
    streamer = _warming_streamer(monkeypatch)
    received = time.monotonic()
    assert not streamer._record_fresh_market_and_maybe_activate(
        "SPX", generation=1, received_monotonic=received,
    )
    assert streamer._fresh_markets_seen == {"SPX"}
    generation = 1
    if change == "expired":
        observed = received + stream_module.QUOTE_FRESHNESS_SECONDS + 1
    else:
        observed = received + 0.2
        if change == "generation":
            generation = 2
            streamer.active_generation = generation
        else:
            streamer.subscription_cutoff_monotonic = received + 0.1
    monkeypatch.setattr(stream_module.time, "monotonic", lambda: observed)
    assert not streamer._record_fresh_market_and_maybe_activate(
        "NDX", generation=generation, received_monotonic=observed,
    )
    assert streamer.handoff_status == "warming"
    assert streamer._record_fresh_market_and_maybe_activate(
        "SPX", generation=generation, received_monotonic=observed,
    )


def test_subscription_reset_discards_pending_observations(monkeypatch):
    streamer = _warming_streamer(monkeypatch)
    _retain_while_publication_busy(streamer, [("SPX", 1, time.monotonic())])
    streamer._reset_fresh_quote_index()
    assert not streamer._pending_fresh_market_observations
