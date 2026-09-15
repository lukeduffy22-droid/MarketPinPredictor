from __future__ import annotations

import pandas as pd

import backend.historical_context as historical_context
from backend.historical_context import get_historical_context, historical_adjustment


def test_historical_context_returns_supplied_market_history(monkeypatch):
    timestamps = pd.date_range(
        end=pd.Timestamp.now(tz="UTC").normalize(),
        periods=121,
        freq="D",
    )
    market = pd.concat([
        pd.DataFrame({
            "symbol": "SPX",
            "timestamp": timestamps,
            "close": [5000.0 + number for number in range(len(timestamps))],
            "history_file": "test-history.csv",
        }),
        pd.DataFrame({
            "symbol": "VIX",
            "timestamp": timestamps,
            "close": [20.0 + number / 100 for number in range(len(timestamps))],
            "history_file": "test-history.csv",
        }),
    ], ignore_index=True)
    monkeypatch.setattr(historical_context, "_load_market_history", lambda: market)
    monkeypatch.setattr(historical_context, "_load_gamma_history", pd.DataFrame)

    context = get_historical_context("SPX")

    assert context["symbol"] == "SPX"
    assert context["history_rows"] == 121
    assert context["history_files"] == ["test-history.csv"]
    assert context["history_age_days"] is not None
    assert context["history_is_fresh"] is True
    assert "realized_vol_20d" in context
    assert "vix_percentile" in context
    assert "historical_adjustment_eligible" in context


def test_historical_context_without_local_data_is_structured_and_ineligible(monkeypatch):
    empty_market = pd.DataFrame(columns=[
        "symbol",
        "timestamp",
        "close",
        "timeframe",
        "source",
        "history_file",
        "history_priority",
    ])
    monkeypatch.setattr(historical_context, "_load_market_history", lambda: empty_market)
    monkeypatch.setattr(historical_context, "_load_gamma_history", pd.DataFrame)

    context = get_historical_context("SPX")

    assert context["symbol"] == "SPX"
    assert context["history_rows"] == 0
    assert context["history_age_days"] is None
    assert context["history_is_fresh"] is False
    assert context["historical_adjustment_eligible"] is False


def test_historical_adjustment_is_bounded():
    context = {
        "return_5d": 0.20,
        "return_20d": 0.50,
        "realized_vol_20d": 0.2,
        "vix_percentile": 0.95,
        "gamma_expected_move_bias": 1000.0,
    }
    adjustment, signals = historical_adjustment("SPX", 7500.0, context)
    assert abs(adjustment) < 7500.0 * 0.01
    assert signals


def test_historical_adjustment_does_not_apply_vix_regime_to_vix():
    context = {"vix_percentile": 0.95}
    adjustment, signals = historical_adjustment("VIX", 20.0, context)
    assert adjustment == 0.0
    assert not any(signal["name"] == "historical_vix_regime" for signal in signals)


def test_stale_historical_context_is_excluded_from_live_target():
    context = {
        "historical_adjustment_eligible": False,
        "history_age_days": 174.0,
        "history_max_age_days": 10,
        "return_5d": 0.20,
        "return_20d": 0.50,
        "vix_percentile": 0.95,
    }

    adjustment, signals = historical_adjustment("SPX", 7500.0, context)

    assert adjustment == 0.0
    assert signals == [{
        "name": "historical_context_stale_excluded",
        "value": 174.0,
        "distance_points": 0.0,
        "weight": 0.0,
        "max_age_days": 10,
    }]
