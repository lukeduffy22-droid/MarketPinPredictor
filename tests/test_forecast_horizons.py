from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import json

import pytest

from app.utils.time_et import EARLY_CLOSE_ET_DATES
from backend.forecast_calendar import resolve_target_session, session_bounds, shift_session
from backend.forecast_horizons import evaluate_forecast, evaluate_methods

UTC = timezone.utc
AS_OF = datetime(2026, 9, 10, 18, 0, tzinfo=UTC)
EPOCH = "a" * 64


def spot(timestamp=AS_OF, price=100.0, **overrides):
    return {
        "symbol": "SPY", "timestamp_utc": timestamp.isoformat(), "spot": price,
        "valid": True, "source_verified": True, "source": "databento_eqs",
        "subscription_epoch_id": EPOCH, "subscription_generation": 2,
        **overrides,
    }


def series(as_of=AS_OF):
    return [spot(as_of - timedelta(minutes=30 - offset), 99 + offset / 30) for offset in range(31)]


def daily(day, price=100.0, **overrides):
    _, closed = session_bounds(day)
    return {
        "symbol": "SPY", "kind": "daily_close", "session_date": day.isoformat(),
        "timestamp_utc": closed.isoformat(), "available_at_utc": (closed + timedelta(minutes=2)).isoformat(),
        "official_close": price, "source": "official_test_fixture", "source_verified": True,
        "source_sha256": "b" * 64, "price_basis": "unadjusted", **overrides,
    }


def daily_history(n=110):
    last = date(2026, 9, 9)
    days = [shift_session(last, -offset) for offset in range(n - 1, -1, -1)]
    return [daily(day, 80 + index * 0.2 + (index % 3) * 0.01) for index, day in enumerate(days)]


def candidate(result, method):
    return next(row for row in result["candidates"] if row["method_id"] == method)


def test_calendar_counts_exchange_sessions_and_early_closes():
    result = resolve_target_session(datetime(2026, 9, 4, 18, tzinfo=UTC), 1)
    assert result["target_session_date"] == "2026-09-08"  # Labor Day excluded
    assert resolve_target_session(datetime(2026, 11, 25, 18, tzinfo=UTC), 1)["target_close_utc"] == "2026-11-27T18:00:00Z"
    assert session_bounds(date(2026, 7, 2))[1].hour == 20  # NYSE regular close
    assert "2026-07-02" not in EARLY_CLOSE_ET_DATES
    assert session_bounds(date(2027, 11, 26))[1].hour == 18
    assert session_bounds(date(2028, 7, 3))[1].hour == 17
    assert session_bounds(date(2028, 11, 24))[1].hour == 18
    assert resolve_target_session(datetime(2026, 9, 12, 18, tzinfo=UTC), 0)["target_session_date"] == "2026-09-14"


def test_calendar_unknown_year_and_closed_day_fail_closed():
    with pytest.raises(ValueError, match="not_a_trading_session"):
        session_bounds(date(2026, 9, 7))
    result = evaluate_forecast("SPY", [], as_of_utc=datetime(2029, 1, 2, 18, tzinfo=UTC))
    assert result["reasons"] == ["calendar_year_unsupported"]


@pytest.mark.parametrize("horizon", [-1, 11, 1.5, True, "2", None])
def test_invalid_horizons_are_not_coerced(horizon):
    with pytest.raises(ValueError, match="horizon_sessions"):
        evaluate_forecast("SPY", [], as_of_utc=AS_OF, horizon_sessions=horizon)


def test_missing_spot_abstains_without_manufacturing_forecast():
    result = evaluate_forecast("SPY", [], as_of_utc=AS_OF)
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None
    assert result["reasons"] == ["no_verified_current_session_spot"]


def test_eod_candidates_are_pure_research_and_deterministic():
    history = series()
    before = deepcopy(history)
    result = evaluate_forecast("spy", history, as_of_utc=AS_OF)
    assert history == before
    assert result == evaluate_forecast("SPY", history, as_of_utc=AS_OF)
    assert result["status"] == "RESEARCH_ONLY"
    assert result["decision_grade"] is False
    assert result["production_promotion_allowed"] is False
    assert candidate(result, "last_price_v1")["predicted_close"] == 100
    assert candidate(result, "eod_damped_momentum_v1")["predicted_close"] > 100
    assert candidate(result, "eod_sample_mean_reversion_v1")["predicted_close"] < 100
    assert result["selected_method"] == "eod_equal_candidate_blend_v1"
    assert len(result["forecast_id"]) == 64
    assert "confidence" not in result
    assert "probability_up" not in result
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("changes,reason", [
    ({"source_verified": False}, "unverified_source"),
    ({"source_verified": "true"}, "unverified_source"),
    ({"valid": False}, "invalid_observation"),
    ({"source": "yfinance_delayed"}, "non_direct_provenance"),
    ({"is_proxy": True}, "non_direct_provenance"),
    ({"subscription_epoch_id": None}, "subscription_identity_missing"),
    ({"subscription_generation": True}, "subscription_identity_missing"),
    ({"spot": float("nan")}, "invalid_price"),
    ({"spot": float("inf")}, "invalid_price"),
    ({"spot": -1}, "invalid_price"),
    ({"timestamp_utc": AS_OF.replace(tzinfo=None).isoformat()}, "timestamp_missing_or_naive"),
    ({"available_at_utc": (AS_OF + timedelta(seconds=1)).isoformat()}, "future_observation"),
])
def test_invalid_provenance_and_values_are_excluded(changes, reason):
    result = evaluate_forecast("SPY", [spot(**changes)], as_of_utc=AS_OF)
    assert result["status"] == "ABSTAIN"
    assert result["evidence"]["excluded_rows"][reason] == 1


def test_future_row_does_not_change_forecast_or_hide_current_price():
    base = evaluate_forecast("SPY", series(), as_of_utc=AS_OF)
    result = evaluate_forecast("SPY", series() + [spot(AS_OF + timedelta(minutes=1), 900)], as_of_utc=AS_OF)
    assert result["predicted_close"] == base["predicted_close"]
    assert result["evidence"]["excluded_rows"]["future_observation"] == 1


def test_generation_transition_does_not_compute_momentum_across_generations():
    rows = series()
    rows[-1]["subscription_generation"] = 3
    result = evaluate_forecast("SPY", rows, as_of_utc=AS_OF)
    assert result["evidence"]["current_generation_observations"] == 1
    assert result["selected_method"] == "last_price_v1"
    assert candidate(result, "eod_damped_momentum_v1")["status"] == "ABSTAIN"


def test_gapped_history_rejects_momentum_even_with_correct_endpoint_span():
    rows = series()
    result = evaluate_forecast("SPY", rows[:5] + rows[20:], as_of_utc=AS_OF)
    assert candidate(result, "eod_damped_momentum_v1")["status"] == "ABSTAIN"


def test_invalid_current_snapshot_cannot_fall_back_to_valid_history():
    result = evaluate_forecast("SPY", series(), current_snapshot=spot(valid=False), as_of_utc=AS_OF)
    assert result["predicted_close"] is None
    assert result["reasons"][0] == "current_snapshot_invalid"


def test_latest_invalid_observation_cannot_fall_back_even_without_snapshot():
    rows = [spot(AS_OF - timedelta(seconds=30)), spot(valid=False)]
    result = evaluate_forecast("SPY", rows, as_of_utc=AS_OF)
    assert result["predicted_close"] is None
    assert result["reasons"] == ["latest_observation_invalid"]


def test_earlier_invalid_record_does_not_poison_later_valid_generation():
    rows = [spot(AS_OF - timedelta(minutes=1), valid=False), spot(subscription_generation=3)]
    result = evaluate_forecast("SPY", rows, as_of_utc=AS_OF)
    assert result["status"] == "RESEARCH_ONLY"
    assert result["evidence"]["subscription_generation"] == 3


def test_duplicate_conflicting_spot_abstains():
    result = evaluate_forecast("SPY", [spot(), spot(price=101)], as_of_utc=AS_OF)
    assert result["reasons"] == ["conflicting_spot_at_same_timestamp"]


def test_stale_spot_and_postclose_are_not_new_forecasts():
    assert evaluate_forecast("SPY", [spot(AS_OF - timedelta(minutes=3))], as_of_utc=AS_OF)["reasons"] == ["stale_current_spot"]
    at_close = session_bounds(date(2026, 11, 27))[1]
    assert evaluate_forecast("SPY", [spot(at_close)], as_of_utc=at_close)["reasons"] == ["outside_regular_hours"]


def test_options_candidate_requires_exact_same_session_and_positive_gex():
    context = spot(options_context_valid=True, options_expiration_date="2026-09-10", gamma_pin=101, net_gex=1e9,
                   options_source_timestamp_utc=AS_OF.isoformat(), options_available_at_utc=AS_OF.isoformat())
    result = evaluate_forecast("SPY", series(), current_snapshot=context, as_of_utc=AS_OF)
    assert candidate(result, "eod_positive_gex_pin_context_v1")["predicted_close"] > 100
    context["options_expiration_date"] = "2026-09-11"
    assert candidate(evaluate_forecast("SPY", series(), current_snapshot=context, as_of_utc=AS_OF), "eod_positive_gex_pin_context_v1")["status"] == "ABSTAIN"


def test_stale_options_cannot_borrow_the_fresh_spot_timestamp():
    context = spot(options_context_valid=True, options_expiration_date="2026-09-10", gamma_pin=101, net_gex=1e9,
                   options_source_timestamp_utc=(AS_OF - timedelta(minutes=10)).isoformat(), options_available_at_utc=AS_OF.isoformat())
    result = evaluate_forecast("SPY", series(), current_snapshot=context, as_of_utc=AS_OF)
    assert candidate(result, "eod_positive_gex_pin_context_v1")["status"] == "ABSTAIN"


def test_multi_day_never_extrapolates_gamma_pin_without_verified_history():
    context = spot(options_context_valid=True, options_expiration_date="2026-09-10", gamma_pin=101, net_gex=1e9)
    result = evaluate_forecast("SPY", series(), current_snapshot=context, as_of_utc=AS_OF, horizon_sessions=3)
    assert result["predicted_close"] is None
    assert result["target_session_date"] == "2026-09-15"
    assert result["status"] == "ABSTAIN"
    assert all("pin" not in row["method_id"] for row in result["candidates"])


def test_multi_day_fits_only_verified_complete_horizon_windows_and_walks_forward():
    result = evaluate_forecast("SPY", [*daily_history(), spot()], as_of_utc=AS_OF, horizon_sessions=3)
    assert result["status"] == "RESEARCH_ONLY"
    assert result["predicted_close"] > 100
    assert result["evidence"]["horizon_return_samples"] == 107
    evaluation = result["evaluation"]
    assert evaluation["metrics"]["n"] > 0
    assert evaluation["production_promotion_allowed"] is False
    previous_target = None
    for fold in evaluation["folds"]:
        assert fold["train_labels_end_utc"][:10] < fold["origin_session"]
        assert fold["train_available_at_utc"] <= fold["origin_available_at_utc"]
        if previous_target:
            assert fold["origin_session"] > previous_target
        previous_target = fold["target_session"]


def test_daily_missing_session_is_not_collapsed_into_one_day():
    history = daily_history(63)
    history.pop(30)
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["evidence"]["horizon_return_samples"] == 60
    assert result["status"] == "RESEARCH_ONLY"
    history.pop(29)
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["status"] == "ABSTAIN"
    assert result["reasons"] == ["insufficient_verified_daily_history"]


def test_daily_explicit_invalid_flag_overrides_source_verified():
    history = daily_history(61)
    history[-1]["valid"] = False
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["reasons"] == ["daily_close_history_not_current"]


def test_first_reviewed_year_does_not_assume_previous_year_calendar():
    as_of = datetime(2024, 1, 2, 18, tzinfo=UTC)
    result = evaluate_forecast("SPY", [spot(as_of)], as_of_utc=as_of, horizon_sessions=1)
    assert result["reasons"] == ["daily_history_calendar_unsupported"]


def test_late_backfill_can_inform_today_but_not_invent_historical_forecasts():
    history = daily_history(110)
    for row in history:
        row["available_at_utc"] = AS_OF.isoformat()
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=3)
    assert result["status"] == "RESEARCH_ONLY"
    assert result["evaluation"]["status"] == "INSUFFICIENT_MATURED_HISTORY"
    assert result["evaluation"]["metrics"] is None


def test_delayed_or_unhashed_daily_close_is_not_used():
    history = daily_history(61)
    history[-1]["available_at_utc"] = (AS_OF + timedelta(hours=1)).isoformat()
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["reasons"] == ["daily_close_history_not_current"]
    history = daily_history(61)
    history[20].pop("source_sha256")
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["status"] == "ABSTAIN"
    assert result["evidence"]["excluded_rows"]["daily_close_provenance_incomplete"] == 1


def test_unadjusted_split_discontinuity_requires_review_instead_of_false_drift():
    history = daily_history(110)
    for row in history[50:]:
        row["official_close"] /= 2
    result = evaluate_forecast("SPY", [*history, spot()], as_of_utc=AS_OF, horizon_sessions=1)
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None
    assert result["reasons"] == ["daily_close_discontinuity_requires_review"]


def test_finite_extreme_spot_values_cannot_overflow_serialized_forecasts():
    rows = [spot(AS_OF - timedelta(minutes=30 - offset), 1e308) for offset in range(31)]
    result = evaluate_forecast("SPY", rows, as_of_utc=AS_OF)
    assert result["predicted_close"] == 1e308
    json.dumps(result, allow_nan=False)


def test_method_evaluator_waits_for_verified_matured_label_and_pairs_baseline():
    forecast = evaluate_forecast("SPY", series(), as_of_utc=AS_OF)
    label = daily(date(2026, 9, 10), 102)
    assert evaluate_methods([forecast], [label], as_of_utc=AS_OF)["status"] == "AWAITING_MATURED_OUTCOMES"
    result = evaluate_methods([forecast], [label], as_of_utc=AS_OF + timedelta(hours=3))
    baseline = next(row for row in result["reports"] if row["method_id"] == "last_price_v1")
    assert baseline["mae"] == 2
    assert baseline["baseline_mae"] == 2
    assert baseline["mae_skill_vs_last_price"] == 0
    assert baseline["n"] == 1
    assert result["production_promotion_allowed"] is False


def test_method_evaluator_purges_overlapping_windows_and_rejects_calendar_mismatch():
    first = evaluate_forecast("SPY", series(), as_of_utc=AS_OF)
    later_time = AS_OF + timedelta(minutes=1)
    second = evaluate_forecast("SPY", [spot(later_time)], as_of_utc=later_time)
    bad = deepcopy(first)
    bad["target_close_utc"] = "2026-09-10T19:00:00Z"
    result = evaluate_methods([first, second, bad], [daily(date(2026, 9, 10), 102)], as_of_utc=AS_OF + timedelta(hours=3))
    assert result["excluded"]["forecast_payload_changed"] == 1
    assert result["excluded"]["overlapping_test_horizon_purged"] == 1
    assert all(row["n"] == 1 for row in result["reports"])


def test_evaluation_rejects_changed_numeric_candidate_and_abstained_output():
    forecast = evaluate_forecast("SPY", series(), as_of_utc=AS_OF)
    forecast["candidates"][0]["predicted_close"] = 102
    abstained = evaluate_forecast("SPY", [spot()], as_of_utc=AS_OF, horizon_sessions=1)
    result = evaluate_methods([forecast, abstained], [daily(date(2026, 9, 10), 102)], as_of_utc=AS_OF + timedelta(hours=3))
    assert result["reports"] == []
    assert result["excluded"] == {"forecast_payload_changed": 1, "unissued_forecast": 1}


def test_evaluation_rejects_conflicting_same_origin_instead_of_choosing_order():
    first = evaluate_forecast("SPY", [spot(price=100)], as_of_utc=AS_OF)
    second = evaluate_forecast("SPY", [spot(price=101)], as_of_utc=AS_OF)
    outcomes = [daily(date(2026, 9, 10), 102)]
    left = evaluate_methods([first, second], outcomes, as_of_utc=AS_OF + timedelta(hours=3))
    right = evaluate_methods([second, first], outcomes, as_of_utc=AS_OF + timedelta(hours=3))
    assert left == right
    assert left["reports"] == []
    assert left["excluded"] == {"conflicting_forecast_at_same_origin": 1}


def test_common_market_structure_column_aliases():
    row = spot()
    row["source_timestamp_utc"] = row.pop("timestamp_utc")
    row["reference_price"] = row.pop("spot")
    row["validation_is_valid"] = row.pop("valid")
    result = evaluate_forecast("SPY", [row], as_of_utc=AS_OF)
    assert result["predicted_close"] == 100


@pytest.mark.parametrize("source_field", ["source", "spot_source"])
def test_vix_parity_remains_forward_context_even_with_same_day_flag(source_field):
    row = spot(symbol="VIX", same_day_profile_available=True, primary_expiration="2026-09-10")
    row[source_field] = "databento_opra_parity"
    result = evaluate_forecast("VIX", [row], as_of_utc=AS_OF, current_snapshot=row)
    assert result["status"] == "ABSTAIN"
    assert result["predicted_close"] is None
    assert "vix_parity_is_forward_context" in result["reasons"]


@pytest.mark.parametrize("fields", [
    {"same_day_profile_available": False, "primary_expiration": "2026-09-10"},
    {"same_day_profile_available": True, "primary_expiration": "2026-09-11"},
    {"same_day_profile_available": True},
    {"same_day_profile_available": True, "primary_expiration": "2026-09-10", "options_expiration_date": "2026-09-11"},
])
def test_parity_requires_verified_same_source_session_expiration(fields):
    row = spot(source="databento_opra_parity", **fields)
    result = evaluate_forecast("SPY", [row], as_of_utc=AS_OF)
    assert result["status"] == "ABSTAIN"
    assert result["evidence"]["excluded_rows"]["parity_same_session_profile_required"] == 1


@pytest.mark.parametrize("expiry_field", ["primary_expiration", "options_expiration_date"])
def test_verified_cash_session_parity_remains_eligible(expiry_field):
    row = spot(source="databento_opra_parity", same_day_profile_available=True, **{expiry_field: "2026-09-10"})
    result = evaluate_forecast("SPY", [row], as_of_utc=AS_OF)
    assert result["status"] == "RESEARCH_ONLY"
    assert result["predicted_close"] == 100


def test_observed_etf_does_not_require_options_profile():
    row = spot(source="databento_eq_us_mini", same_day_profile_available=False)
    result = evaluate_forecast("SPY", [row], as_of_utc=AS_OF)
    assert result["status"] == "RESEARCH_ONLY"


def test_momentum_and_reversion_report_actual_fifteen_minute_lookback():
    rows = series()[15:]
    result = evaluate_forecast("SPY", rows, as_of_utc=AS_OF)
    for method in ("eod_damped_momentum_v1", "eod_sample_mean_reversion_v1"):
        method_result = candidate(result, method)
        assert method_result["status"] == "RESEARCH_ONLY"
        assert method_result["lookback_seconds"] == 900
        assert "30m" not in method_result["equation"]
