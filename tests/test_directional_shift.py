from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.directional_shift import evaluate_directional_shift


NOW = datetime(2026, 9, 10, 16, 0, tzinfo=timezone.utc)


def rows(symbol="SPX", *, reversal=True, direction="UP", **overrides):
    start = NOW - timedelta(minutes=15)
    result = []
    for index in range(31):
        # Established 10-minute decline then a 5-minute climb in independent
        # 30-second observations. Total directional change comfortably exceeds
        # the fixed uncalibrated research noise floor.
        price = 100 - min(index, 20) * 0.01
        price += max(index - 20, 0) * (0.02 if reversal else -0.01)
        if direction == "DOWN":
            price = 200 - price
        row = {
            "symbol": symbol, "source_timestamp_utc": start + timedelta(seconds=30 * index),
            "reference_price": price, "validation_status": "valid", "provider": "databento",
            "subscription_epoch_id": "a" * 64, "subscription_generation": 7,
            "active_generation": 7, "universe_sha256": "b" * 64,
            "spot_source": "databento_opra_put_call_parity",
            "primary_expiration": "2026-09-10", "same_day_profile_available": True,
            "observation_id": f"{symbol}-{index}",
        }
        row.update(overrides)
        result.append(row)
    return result


def evaluate(history, **kwargs):
    return evaluate_directional_shift("SPX", history, as_of_utc=NOW, **kwargs)


def test_confirmed_reversal_is_research_only_and_never_probability():
    data = rows()
    before = copy.deepcopy(data)
    result = evaluate(data)
    assert result["status"] == "CONFIRMED_SHIFT"
    assert result["baseline"]["direction"] == "DOWN"
    assert result["recent"]["direction"] == result["direction"] == "UP"
    assert result["authority"] == "RESEARCH_ONLY"
    assert result["prediction_eligible"] is False
    assert "probability" not in result
    assert result["recent"]["span_seconds"] >= 300
    assert all(window["span_seconds"] >= 120 for window in result["confirmation_windows"])
    assert result["cross_market"]["covered_families"] == 1
    assert "CROSS_MARKET_COVERAGE_LIMITED" in result["reason_codes"]
    assert data == before
    json.dumps(result, allow_nan=False)


def test_downward_reversal():
    result = evaluate(rows(direction="DOWN"))
    assert result["status"] == "CONFIRMED_SHIFT"
    assert result["direction"] == "DOWN"


def test_moving_up_alone_is_not_a_reversal():
    result = evaluate(rows(reversal=False, direction="DOWN"))
    assert result["status"] == "NO_SHIFT"
    assert result["direction"] is None


def test_flat_baseline_cannot_establish_reversal():
    data = rows()
    for row in data[:21]:
        row["reference_price"] = data[20]["reference_price"]
    result = evaluate(data)
    assert result["status"] == "NO_SHIFT"
    assert "NO_ESTABLISHED_OPPOSING_BASELINE" in result["reason_codes"]


def test_duplicates_and_repeated_calls_do_not_add_confirmation():
    data = rows()[-2:]
    result = evaluate(data * 40)
    assert result["status"] == "ABSTAIN"
    assert result["coverage"]["eligible_observations"] == 2
    assert evaluate(data * 40) == result


def test_new_timestamp_on_same_source_id_does_not_advance_evidence():
    data = rows()
    for row in data:
        row["observation_id"] = "same-provider-observation"
    result = evaluate(data)
    assert result["status"] == "ABSTAIN"
    assert result["reason_codes"] == ["LATEST_DISTINCT_OBSERVATION_STALE"]


def test_conflicting_duplicate_timestamp_fails_closed():
    data = rows()
    data.insert(-1, {**data[-1], "reference_price": 200})
    result = evaluate(data)
    assert result["status"] == "ABSTAIN"
    assert result["reason_codes"] == ["CONFLICTING_DUPLICATE_TIMESTAMP"]


@pytest.mark.parametrize("overrides,reason", [
    ({"validation_status": "invalid"}, "OBSERVATION_INVALID"),
    ({"valid": False}, "OBSERVATION_INVALID"),
    ({"is_fallback": True}, "NON_CURRENT_PROVENANCE"),
    ({"universe_provenance": {"is_fallback": True}}, "NON_CURRENT_PROVENANCE"),
    ({"oi_analytics_provenance": {"is_fallback": True}}, "NON_CURRENT_PROVENANCE"),
    ({"spot_source": "delayed_proxy"}, "NON_CURRENT_PROVENANCE"),
    ({"active_generation": 8}, "ACTIVE_GENERATION_MISMATCH"),
    ({"active_subscription_epoch_id": "c" * 64}, "ACTIVE_EPOCH_MISMATCH"),
    ({"source_verified": False}, "SOURCE_NOT_VERIFIED"),
    ({"subscription_epoch_id": ""}, "PROVENANCE_INCOMPLETE"),
    ({"subscription_generation": True}, "PROVENANCE_INCOMPLETE"),
    ({"same_day_profile_available": False}, "OPTION_FORWARD_CONTEXT_ONLY"),
    ({"primary_expiration": "2026-09-11"}, "OPTION_FORWARD_CONTEXT_ONLY"),
    ({"directional_base_eligible": False}, "DIRECTIONAL_BASE_INELIGIBLE"),
    ({"handoff_status": "warming"}, "HANDOFF_NOT_ACTIVE"),
])
def test_latest_ineligible_row_cannot_silently_fall_back(overrides, reason):
    data = rows()
    data[-1].update(overrides)
    result = evaluate(data)
    assert result["status"] == "ABSTAIN"
    assert reason in result["reason_codes"]


def test_recent_invalid_observation_and_generation_change_reset_baseline():
    for overrides in ({"validation_status": "invalid"}, {"subscription_generation": 6}):
        data = rows()
        data[20].update(overrides)
        assert evaluate(data)["status"] == "ABSTAIN"


def test_gap_in_baseline_is_insufficient_history():
    data = rows()
    del data[5:12]
    assert evaluate(data)["status"] == "ABSTAIN"


def test_fast_reversal_without_persistent_second_window_is_watch():
    data = rows()
    for row in data[28:]:
        row["reference_price"] = data[27]["reference_price"]
    result = evaluate(data)
    assert result["status"] == "WATCH"
    assert "REVERSAL_CONFIRMATION_PENDING" in result["reason_codes"]


def test_families_deduplicate_index_etf_pairs():
    histories = {symbol: rows(symbol) for symbol in ("SPY", "NDX", "QQQ", "DJI", "DIA", "RUT", "IWM")}
    result = evaluate(rows(), cross_market_history=histories)
    breadth = result["cross_market"]
    assert breadth["covered_families"] == breadth["up_families"] == 4
    assert breadth["coverage_fraction"] == 1.0
    assert len(breadth["families"]["sp500"]["members"]) == 2
    assert result["status"] == "CONFIRMED_SHIFT"


def test_conflicting_family_downgrades_shift_and_vix_is_not_directional_vote():
    result = evaluate(rows(), cross_market_history={"SPY": rows("SPY", direction="DOWN"), "VIX": rows("VIX")})
    assert result["status"] == "WATCH"
    assert "CROSS_MARKET_DIRECTION_CONFLICT" in result["reason_codes"]
    assert result["cross_market"]["families"]["sp500"]["conflict"] is True
    assert result["cross_market"]["covered_families"] == 1
    assert "VIX" in result["cross_market"]["excluded_symbols"]


def test_stale_cross_market_symbol_never_votes():
    result = evaluate(rows(), cross_market_history={"NDX": rows("NDX")[:-6]})
    assert result["cross_market"]["covered_families"] == 1
    assert result["cross_market"]["excluded_symbols"]["NDX"] == ["LATEST_OBSERVATION_STALE"]


def test_future_source_or_late_arrival_never_changes_as_of_signal():
    data = rows()
    future = {**data[-1], "reference_price": 150, "source_timestamp_utc": NOW + timedelta(seconds=1)}
    late = {**data[-1], "reference_price": 1, "captured_at_utc": NOW + timedelta(seconds=20)}
    original = evaluate(data)
    for appended in (future, late):
        replayed = evaluate(data + [appended])
        assert replayed["status"] == original["status"]
        assert replayed["direction"] == original["direction"]
        assert replayed["baseline"] == original["baseline"]
        assert replayed["recent"] == original["recent"]


@pytest.mark.parametrize("now", [
    datetime(2026, 9, 7, 16, tzinfo=timezone.utc),  # Labor Day
    datetime(2026, 9, 12, 16, tzinfo=timezone.utc),  # Weekend
    datetime(2026, 9, 10, 12, tzinfo=timezone.utc),  # Premarket
    datetime(2026, 9, 10, 21, tzinfo=timezone.utc),  # Postmarket
    datetime(2026, 11, 27, 19, tzinfo=timezone.utc),  # After early close
    datetime(2030, 9, 10, 16, tzinfo=timezone.utc),  # Unreviewed year
])
def test_outside_reviewed_regular_session_abstains(now):
    assert evaluate_directional_shift("SPX", rows(), as_of_utc=now)["status"] == "ABSTAIN"


def test_naive_as_of_timestamp_is_rejected():
    assert evaluate_directional_shift("SPX", rows(), as_of_utc=NOW.replace(tzinfo=None))["reason_codes"] == ["AS_OF_TIMESTAMP_INVALID"]


def test_retained_naive_utc_source_is_accepted_with_explicit_utc_field():
    data = rows()
    for row in data:
        row["source_timestamp_utc"] = row["source_timestamp_utc"].replace(tzinfo=None)
    assert evaluate(data)["status"] == "CONFIRMED_SHIFT"


@pytest.mark.parametrize("volume_kind", [None, "cumulative", "option_volume", "quote_count"])
def test_option_or_untyped_volume_is_never_underlying_vwap(volume_kind):
    result = evaluate(rows(volume=50, volume_price=100, volume_kind=volume_kind))
    assert result["recent"]["window_vwap"] is None


def test_only_matched_true_trade_interval_prices_and_volume_produce_vwap():
    data = rows("SPY", spot_source="databento_equs_trades", volume=10, volume_price=101, volume_kind="interval_traded")
    result = evaluate_directional_shift("SPY", data, as_of_utc=NOW)
    assert result["recent"]["window_vwap"] == 101
    data[-1]["volume_kind"] = "option_volume"
    assert evaluate_directional_shift("SPY", data, as_of_utc=NOW)["recent"]["window_vwap"] is None


def test_irregular_provider_timestamps_use_actual_pre_boundary_anchor():
    template = rows()[0]
    data = []
    # First event inside the requested 5-minute window is +2 seconds; its
    # preceding real event is -4 seconds. The final source interval is 4s.
    for index, seconds in enumerate([*range(-904, 0, 6), 0]):
        timestamp = NOW + timedelta(seconds=seconds)
        since_turn = seconds + 300
        price = 99.8 - since_turn * 0.0005 if since_turn <= 0 else 99.8 + since_turn * 0.001
        data.append({**template, "source_timestamp_utc": timestamp,
                     "reference_price": price, "observation_id": f"irregular-{index}"})
    result = evaluate(data)
    assert result["status"] == "CONFIRMED_SHIFT"
    assert result["direction"] == "UP"
    recent = result["recent"]
    assert recent["eligible"] is True
    assert recent["span_seconds"] == 304
    assert recent["boundary_anchor_age_seconds"] == 4
    assert recent["observed_start_utc"] == (NOW - timedelta(seconds=304)).isoformat()
    assert recent["maximum_gap_seconds"] == 6


def test_missing_or_stale_boundary_anchor_does_not_relax_elapsed_requirement():
    from backend.directional_shift import _window

    start = NOW - timedelta(seconds=300)
    points = [{"time": NOW + timedelta(seconds=seconds), "price": 100.0, "row": {}}
              for seconds in [*range(-298, 0, 6), 0]]
    for history in (points, [{"time": start - timedelta(seconds=121), "price": 100.0, "row": {}}] + points):
        result = _window(history, start, NOW, min_span=300)
        assert result["eligible"] is False
        assert result["span_seconds"] == 298
        assert result["boundary_anchor_age_seconds"] is None


def test_boundary_anchor_cannot_manufacture_confirmation_from_two_samples():
    from backend.directional_shift import _window

    start = NOW - timedelta(seconds=300)
    result = _window([
        {"time": start - timedelta(seconds=4), "price": 100.0, "row": {}},
        {"time": NOW, "price": 101.0, "row": {}},
    ], start, NOW, min_span=300)
    assert result["eligible"] is False
    assert result["observations"] == 2
    assert result["reason"] == "INSUFFICIENT_DISTINCT_OBSERVATIONS"
