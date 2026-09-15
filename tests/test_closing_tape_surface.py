from datetime import date

import pandas as pd
import pytest

from backend.closing_tape.surface import (
    MODEL_FEATURE_COLUMNS,
    SURFACE_FEATURE_VERSION,
    build_contract_surface_features,
    select_decision_horizon_features,
)


def _rows() -> pd.DataFrame:
    common = {
        "trading_date": "2026-08-25", "session_id": "s1", "feed_name": "opra_options",
        "family_root": "SPX", "minute_utc": "2026-08-25T15:00:00+00:00",
        "feature_available_at_utc": "2026-08-25T15:01:00+00:00",
        "cash_open_utc": "2026-08-25T13:30:00+00:00",
        "cash_close_utc": "2026-08-25T15:16:00+00:00",
        "reference_price": 100.0,
        "reference_price_timestamp_utc": "2026-08-25T15:00:55+00:00",
        "reference_price_age_seconds": 5.0, "capture_integrity_verified": True,
        "inference_method": "trade_price_vs_pretrade_nbbo", "inference_version": "1.0",
        "source_sha256": "a" * 64,
        "reference_price_method": "put-call-parity-v2-discounted-strike",
        "reference_price_is_estimate": True, "reference_price_provider": "databento",
        "reference_price_tier": "primary_marketpin_snapshot",
        "reference_subscription_epoch_id": "a" * 64,
        "reference_subscription_generation": 1,
        "reference_price_epoch_eligible": True,
        "reference_price_epoch_status": "eligible_current_epoch",
        "predicted_close": 101.5,
        "open_interest_available_at_utc": "2026-08-25T14:59:30+00:00",
        "nbbo_valid_count": 1, "quoted_spread_bps_sum": 100.0,
        "data_quality_flagged_count": 0,
    }
    return pd.DataFrame(
        [
            {
                **common, "raw_symbol": "SPX 260825C00100000", "expiration": date(2026, 8, 25),
                "option_type": "C", "strike": 100.0, "trade_count": 1,
                "volume": 10.0, "notional": 1000.0, "at_ask_count": 1,
                "at_bid_count": 0, "at_ask_notional": 1000.0, "at_bid_notional": 0.0,
                "open_interest": 100.0,
            },
            {
                **common, "raw_symbol": "SPX 260830P00110000", "expiration": date(2026, 8, 30),
                "option_type": "P", "strike": 110.0, "trade_count": 2,
                "volume": 20.0, "notional": 4000.0, "at_ask_count": 0,
                "at_bid_count": 2, "at_ask_notional": 0.0, "at_bid_notional": 4000.0,
                "open_interest": None,
            },
        ]
    )


def test_contract_surface_preserves_observed_and_inferred_semantics():
    surface = build_contract_surface_features(_rows())

    assert len(surface) == 1
    row = surface.iloc[0]
    assert row["surface_feature_version"] == SURFACE_FEATURE_VERSION
    assert len(row["feature_schema_hash"]) == 64
    assert bool(row["reference_price_is_estimate"])
    assert row["reference_subscription_epoch_id"] == "a" * 64
    assert row["reference_subscription_generation"] == 1
    assert row["predicted_close"] == pytest.approx(101.5)
    assert "predicted_close" not in MODEL_FEATURE_COLUMNS
    assert row["observed_contract_count"] == 2
    assert row["observed_trade_count"] == 3
    assert row["observed_volume"] == 30
    assert row["observed_call_volume"] == 10
    assert row["observed_put_volume"] == 20
    assert row["observed_0dte_volume"] == 10
    assert row["observed_1_7dte_volume"] == 20
    assert row["observed_atm_25bps_volume"] == 10
    assert row["observed_wing_gt100bps_volume"] == 20
    assert row["observed_contract_volume_hhi"] == pytest.approx(5 / 9)
    assert row["observed_strike_volume_hhi"] == pytest.approx(5 / 9)
    assert row["observed_traded_contract_oi_sum"] == 100
    assert row["observed_traded_contract_oi_coverage_ratio"] == pytest.approx(0.5)
    assert row["inferred_call_at_ask_notional"] == 1000
    assert row["inferred_put_at_bid_notional"] == 4000
    assert row["inferred_net_at_ask_minus_bid_notional"] == -3000
    assert row["observed_call_volume_share"] == pytest.approx(1 / 3)
    assert row["observed_0dte_volume_share"] == pytest.approx(1 / 3)
    assert row["observed_nbbo_coverage_ratio"] == pytest.approx(2 / 3)
    assert row["observed_avg_quoted_spread_bps"] == pytest.approx(100.0)
    assert row["inferred_at_ask_count_share"] == pytest.approx(1 / 3)
    assert row["inferred_net_at_ask_minus_bid_notional_ratio"] == pytest.approx(-0.6)
    assert row["family_is_spx"] == 1
    assert row["family_is_ndx"] == 0
    assert set(MODEL_FEATURE_COLUMNS) <= set(surface.columns)
    assert row["minutes_to_cash_close"] == 15

    selected = select_decision_horizon_features(surface, minutes_before_close=15)
    assert len(selected) == 1
    assert selected.iloc[0]["decision_horizon_minutes_before_close"] == 15
    assert select_decision_horizon_features(surface, minutes_before_close=14).empty


def test_contract_surface_rejects_future_reference_price_and_unverified_capture():
    future = _rows()
    future["reference_price_timestamp_utc"] = "2026-08-25T15:01:01+00:00"
    with pytest.raises(ValueError, match="future"):
        build_contract_surface_features(future)

    unverified = _rows()
    unverified.loc[0, "capture_integrity_verified"] = False
    with pytest.raises(ValueError, match="verified capture integrity"):
        build_contract_surface_features(unverified)

    future_oi = _rows()
    future_oi.loc[0, "open_interest_available_at_utc"] = (
        "2026-08-25T15:01:01+00:00"
    )
    with pytest.raises(ValueError, match="future"):
        build_contract_surface_features(future_oi)


def test_contract_surface_does_not_turn_missing_open_interest_into_zero():
    rows = _rows()
    rows["open_interest"] = None

    result = build_contract_surface_features(rows).iloc[0]

    assert pd.isna(result["observed_traded_contract_oi_sum"])
    assert result["observed_traded_contract_oi_coverage_ratio"] == 0


def test_contract_surface_rejects_mixed_reference_subscription_epochs():
    rows = _rows()
    rows.loc[1, "reference_subscription_epoch_id"] = "b" * 64

    with pytest.raises(ValueError, match="mixes inference or source provenance"):
        build_contract_surface_features(rows)
