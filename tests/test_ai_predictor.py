import pandas as pd
import pytest

from backend.ai_predictor import build_ai_prediction
from backend.api.helpers import pin_payload_for_symbol, with_gex_level_semantics
from backend.api.routers import gex as gex_router
from backend.calibration import calibration_adjustment

EPOCH_ID = "e" * 64


def base_payload(**overrides):
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 1,
        "price": 7500.0,
        "likely_close": 7510.0,
        "gamma_pin": 7505.0,
        "zero_gamma": 7495.0,
        "max_pain": 7520.0,
        "net_gex": 5000.0,
        "gross_gex": 20000.0,
        "contracts": 80,
        "quotes_cached": 200,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "usable_for_prediction": True,
    }
    payload.update(overrides)
    return payload


def test_api_semantics_keep_zero_gamma_compatibility_but_relabel_the_concept():
    payload = with_gex_level_semantics(
        {"zero_gamma_level": 7440.21, "zero_gamma_method": "linear_interpolation"}
    )

    assert payload["zero_gamma"] == pytest.approx(7440.21)
    assert payload["zero_gamma_level"] == pytest.approx(7440.21)
    assert payload["zero_gamma_label"] == "First strike-bucket GEX sign crossing"
    assert payload["zero_gamma_method"] == (
        "first_strike_bucket_sign_crossing_linear_interpolation"
    )
    assert payload["zero_gamma_source_method"] == "linear_interpolation"
    assert payload["zero_gamma_is_portfolio_spot_sweep"] is False
    assert payload["zero_gamma_semantics"]["not_a_close_target"] is True


def test_ai_prediction_is_usable_with_live_payload():
    result = build_ai_prediction("SPX", base_payload())
    assert result["usable"] is True
    assert result["symbol"] == "SPX"
    assert result["predicted_close"] > 7490
    assert result["confidence"] >= 55
    assert result["confidence_kind"] == "data_quality_heuristic"
    assert result["confidence_scale"] == "percent_0_100"
    assert result["confidence_calibrated"] is False
    assert result["signals"]
    assert result["pin_payload"]["symbol"] == "SPX"


def test_invalid_payload_is_not_usable():
    result = build_ai_prediction("RUT", {"validation_is_valid": False, "price": 0})
    assert result["usable"] is False
    assert "reason" in result
    assert result["confidence"] is None
    assert result["confidence_kind"] == "unavailable"


@pytest.mark.parametrize(
    "provenance_field",
    ["universe_provenance", "oi_analytics_provenance"],
)
def test_nested_fallback_payload_is_not_usable(provenance_field):
    result = build_ai_prediction(
        "SPX",
        base_payload(**{provenance_field: {"is_fallback": True}}),
    )

    assert result["usable"] is False
    assert result["confidence"] is None


def test_rising_vix_adds_bearish_adjustment_to_equity_index():
    payload = base_payload(likely_close=7500.0, gamma_pin=7500.0, zero_gamma=7500.0, max_pain=7500.0)
    vix_payload = {
        "symbol": "VIX",
        "price": 18.0,
        "likely_close": 20.0,
        "gamma_pin": 19.0,
        "contracts": 20,
        "quotes_cached": 100,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "usable_for_prediction": True,
    }
    result = build_ai_prediction("SPX", payload, vix_payload)
    assert result["usable"] is True
    assert result["vix_pressure"] > 0
    assert result["vix_adjustment"] < 0
    assert result["predicted_close"] < 7500.0


def test_vix_does_not_apply_vix_pressure_to_itself():
    payload = base_payload(symbol="VIX", price=18.0, likely_close=19.0, gamma_pin=18.0, max_pain=19.0)
    result = build_ai_prediction("VIX", payload, payload)
    assert result["usable"] is True
    assert result["vix_pressure"] is None
    assert result["vix_adjustment"] == 0.0


def test_disabled_or_missing_calibration_returns_no_adjustment():
    adjustment, signal = calibration_adjustment("UNKNOWN", base_payload())
    assert adjustment == 0.0
    assert signal is None


def test_pin_payload_uses_historical_fallback_when_market_is_closed(monkeypatch):
    class _ClosedMarketStreamer:
        def get_latest_pin(self, symbol):
            return None

        def get_latest_data(self, symbol, n=1):
            return []

    monkeypatch.setattr("backend.api.helpers.get_streamer", lambda: _ClosedMarketStreamer())
    monkeypatch.setattr("backend.api.helpers.market_is_closed", lambda: True)
    monkeypatch.setattr(
        "backend.api.helpers.get_historical_context",
        lambda _symbol: {
            "history_rows": 2,
            "history_is_fresh": True,
            "return_5d": 0.0,
            "vix_percentile": 0.5,
        },
    )
    monkeypatch.setattr(
        "backend.api.helpers.get_market_history",
        lambda: pd.DataFrame({"symbol": ["SPX"], "close": [7500.0]}),
    )

    payload = pin_payload_for_symbol("SPX")

    assert payload is not None
    assert payload["price"] > 0
    assert payload["predicted_close"] > 0
    assert payload["symbol"] == "SPX"
    assert payload["validation_is_valid"] is False
    assert payload["usable_for_prediction"] is False
    assert payload["historical_context_only"] is True
    assert payload["gamma_pin"] is None
    assert payload["max_pain"] is None


def test_pin_payload_preserves_zero_price_diagnostics_instead_of_substituting_history(monkeypatch):
    class _ZeroPriceStreamer:
        subscription_epoch_id = EPOCH_ID
        active_generation = 1
        handoff_status = "active"

        def get_latest_pin(self, symbol):
            return None

        def get_latest_data(self, symbol, n=1):
            return [{
                "symbol": symbol,
                "price": 0.0,
                "predicted_close": None,
                "validation_is_valid": False,
                "subscription_epoch_id": EPOCH_ID,
                "subscription_generation": 1,
            }]

    monkeypatch.setattr("backend.api.helpers.get_streamer", lambda: _ZeroPriceStreamer())
    monkeypatch.setattr("backend.api.helpers.market_is_closed", lambda: True)
    monkeypatch.setattr(
        "backend.api.helpers.get_historical_context",
        lambda _symbol: {
            "history_rows": 2,
            "history_is_fresh": True,
            "return_5d": 0.0,
            "vix_percentile": 0.5,
        },
    )

    payload = pin_payload_for_symbol("SPX")

    assert payload is not None
    assert payload.get("provider") != "historical-fallback"
    assert payload["price"] == 0.0
    assert payload["predicted_close"] is None
    assert payload["historical_context_only"] is False
    assert payload["validation_is_valid"] is False
    assert payload["usable_for_prediction"] is False
    assert payload["zero_gamma"] is None
    assert payload["forecast_state"] == "ABSTAIN"
    assert payload["zero_gamma_is_portfolio_spot_sweep"] is False


def test_stale_history_is_not_served_as_closed_market_price(monkeypatch):
    class _ClosedMarketStreamer:
        def get_latest_pin(self, symbol):
            return None

        def get_latest_data(self, symbol, n=1):
            return []

    monkeypatch.setattr("backend.api.helpers.get_streamer", lambda: _ClosedMarketStreamer())
    monkeypatch.setattr("backend.api.helpers.market_is_closed", lambda: True)
    monkeypatch.setattr(
        "backend.api.helpers.get_historical_context",
        lambda _symbol: {"history_rows": 1_000, "history_is_fresh": False, "history_age_days": 174.0},
    )

    assert pin_payload_for_symbol("SPX") is None


def test_invalid_live_payload_never_becomes_valid_historical_fallback(monkeypatch):
    class _InvalidLiveStreamer:
        subscription_epoch_id = EPOCH_ID
        active_generation = 1
        handoff_status = "active"

        def get_latest_pin(self, symbol):
            return None

        def get_latest_data(self, symbol, n=1):
            return [{
                "symbol": symbol,
                "price": 0.0,
                "validation_is_valid": False,
                "pregate_reason": "CHAIN_TOO_THIN",
                "subscription_epoch_id": EPOCH_ID,
                "subscription_generation": 1,
            }]

    monkeypatch.setattr("backend.api.helpers.get_streamer", lambda: _InvalidLiveStreamer())
    monkeypatch.setattr("backend.api.helpers.market_is_closed", lambda: False)

    payload = pin_payload_for_symbol("SPX")

    assert payload is not None
    assert payload["validation_is_valid"] is False
    assert payload.get("provider") != "historical-fallback"


def test_dashboard_get_does_not_synthesize_closed_market_fallback(monkeypatch):
    gex_router.workstation_state_store.reset()

    class _EmptyRuntime:
        subscription_epoch_id = EPOCH_ID
        active_generation = 1
        handoff_status = "active"

        @staticmethod
        def get_subscription_context():
            return {
                "subscription_epoch_id": EPOCH_ID,
                "subscription_generation": 1,
                "handoff_status": "active",
            }

        @staticmethod
        def get_latest_data(_symbol, n=1):
            return []

    monkeypatch.setattr(gex_router, "get_streamer", lambda: _EmptyRuntime())
    monkeypatch.setattr(
        gex_router,
        "save_prediction_snapshot",
        lambda *_args, **_kwargs: pytest.fail(
            "historical context must not be persisted as a valid live prediction"
        ),
    )

    result = gex_router._symbol_dashboard_payload("SPX")

    assert result["status"] == "unavailable"
    assert result["prediction"] is None
    assert result["forecast_state"] == "UNAVAILABLE"
    assert result["health"]["usable_for_prediction"] is False
