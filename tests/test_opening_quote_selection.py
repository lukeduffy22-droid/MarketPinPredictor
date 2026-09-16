"""An invalid candidate must not displace enough eligible parity pairs."""

from datetime import timedelta

import pytest

from tests.test_opening_transition_stress import (
    september_14_open, _publish_mappings_and_quotes,
)
import backend.databento_streamer as streamer_module


@pytest.mark.parametrize("market", ["SPX", "NDX"])
@pytest.mark.parametrize("invalid_time", ["prior_session", "stale_receive", "future_receive"])
def test_invalid_provider_timestamp_candidate_does_not_poison_valid_pairs(
    september_14_open, market, invalid_time,
):
    streamer, journal, _, opening = september_14_open
    streamer._receive_lag_window.extend([.01] * streamer_module.CLOCK_SYNC_MIN_SAMPLES)
    for index, symbol in enumerate(("SPX", "NDX")):
        received = _publish_mappings_and_quotes(streamer, symbol, first_instrument_id=10000 + index * 10000)
        streamer._record_fresh_market_and_maybe_activate(symbol, generation=7, received_monotonic=received)
    capture_at = opening + timedelta(milliseconds=250)
    snapshot, reason = streamer._snapshot_opening_reference_inputs(
        market, trading_date=opening.date(), captured_at_utc=capture_at)
    assert reason is None
    payload, reason = streamer._compute_opening_reference_payload(
        snapshot, sample_timestamp_utc=opening, captured_at_utc=capture_at)
    assert reason is None
    selected = payload["formula_inputs"]["pairs"][0]["call_symbol"]
    quote = dict(streamer.quotes[selected])
    if invalid_time == "prior_session":
        quote["ts_event_ns"] = int((opening - timedelta(days=1)).timestamp() * 1e9)
    else:
        offset = (-streamer_module.QUOTE_FRESHNESS_SECONDS - 1
                  if invalid_time == "stale_receive" else 1)
        quote["ts_recv_ns"] = int((capture_at + timedelta(seconds=offset)).timestamp() * 1e9)
        quote["ts_event_ns"] = quote["ts_recv_ns"] - 1
    streamer.quotes[selected] = quote
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=lambda: capture_at, journal=journal)
    assert result["recorded"] is True, result.get("reason")
    assert result["progress_eligible"] is True
    evidence = result["opening_gate_evidence"]
    assert sum(evidence["quote_rejections"].values()) == 1
    assert evidence["complete_pair_count"] == payload["paired_quote_count"] - 1


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_all_prior_session_candidates_still_fail_closed(september_14_open, market):
    streamer, journal, _, opening = september_14_open
    streamer._receive_lag_window.extend([.01] * streamer_module.CLOCK_SYNC_MIN_SAMPLES)
    for index, symbol in enumerate(("SPX", "NDX")):
        received = _publish_mappings_and_quotes(streamer, symbol, first_instrument_id=10000 + index * 10000)
        streamer._record_fresh_market_and_maybe_activate(symbol, generation=7, received_monotonic=received)
    for symbol, original in list(streamer.quotes.items()):
        streamer.quotes[symbol] = {**original, "ts_event_ns": int((opening - timedelta(days=1)).timestamp() * 1e9)}
    result = streamer._capture_opening_reference_for_bucket(
        market, opening, now_utc=lambda: opening + timedelta(milliseconds=250), journal=journal)
    assert result["recorded"] is False
    assert result["reason"] == "COMPLETE_PAIR_MINIMUM_NOT_MET"
    assert result["opening_gate_evidence"]["complete_pair_count"] == 0
