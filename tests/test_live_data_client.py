from datetime import datetime, timezone

import pytest
import requests

from app.services.live_data_client import (
    ZERO_GAMMA_DISPLAY_LABEL,
    _current_lifecycle_close_forecast,
    _current_promoted_row,
    _source_metadata,
    build_expiration_profile_view,
    fetch_closing_tape_readiness,
    fetch_closing_tape_status,
    fetch_databento_prediction,
    fetch_live_status_snapshot,
    fetch_promoted_closing_tape_predictions,
    live_pipeline_runtime_ready,
    live_pipeline_symbol_is_current,
)


CURRENT_EPOCH = "e" * 64


def _current_pipeline(symbol: str = "SPX") -> dict:
    return {
        "prediction_pipeline_ok": True,
        "handoff_status": "active",
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_epoch_valid": True,
        "epoch_mismatch_symbols": [],
        "generation_mismatch_symbols": [],
        "required_epoch_mismatch_symbols": [],
        "required_generation_mismatch_symbols": [],
        "symbol_status": {
            symbol: {
                "subscription_epoch_id": CURRENT_EPOCH,
                "active_subscription_epoch_id": CURRENT_EPOCH,
                "epoch_is_current": True,
                "subscription_generation": 7,
                "active_generation": 7,
                "generation_is_current": True,
                "usable_for_prediction": True,
                "is_stale": False,
                "fresh_quote_count": 12,
            }
        },
    }


def _lifecycle_identity_inputs():
    dashboard = {
        "status": "ready",
        "forecast_id": "forecast-7",
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_generation": 7,
    }
    prediction = {
        "predicted_close": 7510.0,
        "usable": True,
        "forecast_id": "forecast-7",
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_generation": 7,
    }
    pin_payload = {
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_generation": 7,
    }
    return dashboard, prediction, pin_payload


def _server_verified_promoted_batch(*, is_current_session: bool):
    families = ("SPX", "NDX", "RUT", "VIX", "SPY")
    rows = [
        {
            "family_root": family,
            "trading_date": "2026-09-03" if is_current_session else "2026-09-02",
            "prediction_lower": 99.0 + index,
            "predicted_level": 100.0 + index,
            "prediction_upper": 101.0 + index,
        }
        for index, family in enumerate(families)
    ]
    return {
        "available": True,
        "read_succeeded": True,
        "requested_trading_date": rows[0]["trading_date"],
        "current_trading_date": "2026-09-03",
        "is_current_session": is_current_session,
        "prediction_authority": {
            "tcbbo_promoted": True,
            "promotion_verified": True,
        },
        "rows": rows,
    }


def test_current_promoted_row_accepts_complete_current_validated_batch():
    payload = _server_verified_promoted_batch(is_current_session=True)

    row = _current_promoted_row(payload, "SPX")

    assert row is not None
    assert row["family_root"] == "SPX"
    assert row["trading_date"] == "2026-09-03"


def test_current_promoted_row_rejects_historical_batch():
    payload = _server_verified_promoted_batch(is_current_session=False)

    assert _current_promoted_row(payload, "SPX") is None


def test_source_metadata_marks_historical_fallback():
    source = _source_metadata({"provider": "historical-fallback"})

    assert source["is_fallback"] is True
    assert source["display_mode"] == "fallback"
    assert source["source_label"] == "Closed-market historical context only"


@pytest.mark.parametrize(
    "provenance_field",
    ["universe_provenance", "oi_analytics_provenance"],
)
def test_source_metadata_marks_nested_fallback(provenance_field):
    source = _source_metadata(
        {
            "provider": "databento",
            provenance_field: {"is_fallback": True},
        }
    )

    assert source["is_fallback"] is True
    assert source["display_mode"] == "fallback"
    assert source["source_label"] == "Closed-market historical context only"


def test_source_metadata_defaults_to_databento_live():
    source = _source_metadata({"provider": "databento"})

    assert source["is_fallback"] is False
    assert source["display_mode"] == "live"
    assert source["source_label"] == "Databento Live"


def test_source_metadata_prefers_prediction_authority_over_market_data_label():
    source = _source_metadata(
        {"provider": "databento", "source_label": "Databento Live"},
        {
            "prediction_authority": {
                "source_label": "Databento live GEX research estimate - not TCBBO promoted"
            }
        },
    )

    assert source["market_data_source_label"] == "Databento Live"
    assert source["source_label"].endswith("not TCBBO promoted")


def test_live_client_maps_total_gex_to_gross_exposure(monkeypatch):
    source_time = datetime.now(timezone.utc).isoformat()
    subscription_epoch_id = "e" * 64

    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "status": "live",
                "forecast_id": "forecast-12",
                "forecast_state": "RESEARCH_ONLY",
                "state_revision": 12,
                "event_id": "12",
                "prediction_snapshot_id": 44,
                "source_as_of_utc": source_time,
                "subscription_epoch_id": subscription_epoch_id,
                "subscription_generation": 7,
                "prediction": {
                    "predicted_close": 7510.0,
                    "confidence": 80.0,
                    "usable": True,
                    "forecast_id": "forecast-12",
                },
                "pin_payload": {
                    "symbol": "SPX",
                    "subscription_epoch_id": subscription_epoch_id,
                    "subscription_generation": 7,
                    "timestamp": source_time,
                    "price": 7500.0,
                    "likely_close": 7510.0,
                    "gamma_pin": 7505.0,
                    "pin_runner_up_strike": 7500.0,
                    "pin_lead_ratio": 0.04,
                    "pin_is_contested": True,
                    "pin_competition_reason": "PIN_CONTESTED: unit test",
                    "gross_gex": 125.0,
                    "net_gex": -5.0,
                    "zero_gamma_level": 7490.0,
                    "zero_gamma_method": "linear_interpolation",
                    "primary_expiration": "2026-08-26",
                    "selected_target_mode": "primary_expiration",
                    "subscription_profile": "near-term-shadow",
                    "expiration_profiles": [
                        {
                            "expiration": "2026-08-26",
                            "dte": 0,
                            "bucket": "0DTE",
                            "pin": 7505.0,
                            "max_pain": 7500.0,
                            "zero_gamma": 7490.0,
                            "gross_gex": 125.0,
                            "net_gex": -5.0,
                            "contracts": 50,
                        }
                    ],
                    "subscription_expirations": [
                        {
                            "expiration": "2026-08-26",
                            "stage": 0,
                            "role": "primary",
                            "contracts": 100,
                        }
                    ],
                    "validation_is_valid": True,
                    "top_strikes": [],
                },
            }

    monkeypatch.setattr(
        "app.services.live_data_client.requests.get",
        lambda *_args, **_kwargs: _Response(),
    )

    indicator_builder_calls = []

    def indicator_builder(frame):
        indicator_builder_calls.append(frame)
        raise AssertionError("indicator builder requires verified observed OHLCV bars")

    prediction, error, warming = fetch_databento_prediction(
        "SPX",
        "1D",
        indicator_builder,
        stale_after_seconds=30.0,
    )

    assert error is None and warming is None
    assert prediction is not None
    assert indicator_builder_calls == []
    assert prediction["df"].empty
    assert list(prediction["df"].columns) == [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert prediction["indicators_available"] is False
    assert prediction["indicator_status"] == "unavailable"
    assert prediction["indicator_provenance"] == {
        "status": "unavailable",
        "reason_code": "OBSERVED_INTRADAY_BARS_UNAVAILABLE",
        "reason": (
            "The Databento prediction payload does not include verified observed "
            "intraday OHLCV bars."
        ),
        "required_source_kind": "observed_intraday_ohlcv",
        "is_observed": False,
        "is_synthetic": False,
        "row_count": 0,
    }
    assert prediction["current_price"] == 7500.0
    assert prediction["predicted_price"] == 7510.0
    assert prediction["change_pct"] == pytest.approx(10.0 / 7500.0 * 100.0)
    assert prediction["gex_data"]["total_gex"] == 125.0
    assert prediction["gex_data"]["net_gex"] == -5.0
    assert prediction["gex_data"]["gex_unit"] == "raw_gamma_x_open_interest_x_100"
    assert prediction["gex_data"]["pin_runner_up_strike"] == 7500.0
    assert prediction["gex_data"]["pin_lead_ratio"] == 0.04
    assert prediction["gex_data"]["pin_is_contested"] is True
    assert prediction["gex_data"]["zero_gamma"] == 7490.0
    assert "direction" not in prediction["gex_data"]
    assert "pull_strength" not in prediction["gex_data"]
    assert prediction["gex_data"]["zero_gamma_label"] == ZERO_GAMMA_DISPLAY_LABEL
    assert prediction["gex_data"]["zero_gamma_is_portfolio_spot_sweep"] is False
    assert prediction["confidence"] == 80.0
    assert prediction["confidence_kind"] == "data_quality_heuristic"
    assert prediction["confidence_scale"] == "percent_0_100"
    assert prediction["confidence_calibrated"] is False
    assert prediction["model_type"] == "Backend estimator identity unavailable"
    assert prediction["model_version"] is None
    assert prediction["decision_grade"] is False
    assert prediction["prediction_mode"] == "backend_lifecycle"
    assert prediction["is_estimate"] is True
    assert prediction["tcbbo_promoted"] is False
    assert prediction["prediction_authority"]["authority_state"] == "research_only"
    assert prediction["source_label"].endswith("not TCBBO promoted")
    assert prediction["forecast_id"] == "forecast-12"
    assert prediction["forecast_state"] == "RESEARCH_ONLY"
    assert prediction["state_revision"] == 12
    assert prediction["event_id"] == "12"
    assert prediction["prediction_snapshot_id"] == 44
    assert prediction["source_as_of_utc"] == source_time
    assert prediction["subscription_epoch_id"] == subscription_epoch_id
    assert prediction["subscription_generation"] == 7
    assert prediction["primary_expiration"] == "2026-08-26"
    assert prediction["expiration_profiles"][0]["calculation_coverage_ratio"] == 0.5


def test_live_client_does_not_fabricate_missing_confidence(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "status": "live",
                "forecast_id": "forecast-no-confidence",
                "subscription_epoch_id": CURRENT_EPOCH,
                "subscription_generation": 7,
                "prediction": {
                    "predicted_close": 7510.0,
                    "usable": True,
                    "forecast_id": "forecast-no-confidence",
                },
                "pin_payload": {
                    "symbol": "SPX",
                    "subscription_epoch_id": CURRENT_EPOCH,
                    "subscription_generation": 7,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "price": 7500.0,
                    "likely_close": 7510.0,
                    "validation_is_valid": True,
                    "top_strikes": [],
                },
            }

    monkeypatch.setattr(
        "app.services.live_data_client.requests.get",
        lambda *_args, **_kwargs: _Response(),
    )

    prediction, error, warming = fetch_databento_prediction(
        "SPX", "1D", lambda frame: frame, stale_after_seconds=30.0
    )

    assert error is None and warming is None
    assert prediction is not None
    assert prediction["confidence"] is None
    assert prediction["confidence_kind"] == "unavailable"
    assert prediction["confidence_scale"] == "not_applicable"
    assert prediction["confidence_calibrated"] is False


def test_live_client_never_substitutes_gamma_pin_for_missing_close_forecast(
    monkeypatch,
):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "status": "ready",
                "forecast_id": "forecast-without-estimate",
                "subscription_epoch_id": CURRENT_EPOCH,
                "subscription_generation": 7,
                "prediction": {"predicted_close": None, "usable": False},
                "pin_payload": {
                    "symbol": "SPX",
                    "subscription_epoch_id": CURRENT_EPOCH,
                    "subscription_generation": 7,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "price": 7500.0,
                    "likely_close": 7510.0,
                    "predicted_close": 7515.0,
                    "gamma_pin": 7520.0,
                    "validation_is_valid": True,
                    "top_strikes": [],
                },
            }

    monkeypatch.setattr(
        "app.services.live_data_client.requests.get",
        lambda *_args, **_kwargs: _Response(),
    )

    prediction, error, warming = fetch_databento_prediction(
        "SPX", "1D", lambda frame: frame, stale_after_seconds=30.0
    )

    assert prediction is None
    assert error == "SPX: no current usable lifecycle or promoted close forecast"
    assert warming is None


@pytest.mark.parametrize(
    ("source_name", "field", "value"),
    [
        ("prediction", "subscription_epoch_id", "f" * 64),
        ("pin", "subscription_epoch_id", "f" * 64),
        ("prediction", "subscription_generation", 8),
        ("pin", "subscription_generation", 8),
        ("prediction", "forecast_id", "forecast-other"),
    ],
)
def test_lifecycle_close_forecast_rejects_cross_authority_identity_mismatch(
    source_name,
    field,
    value,
):
    dashboard, prediction, pin_payload = _lifecycle_identity_inputs()
    sources = {
        "dashboard": dashboard,
        "prediction": prediction,
        "pin": pin_payload,
    }
    sources[source_name][field] = value

    assert (
        _current_lifecycle_close_forecast(dashboard, prediction, pin_payload)
        is None
    )


def test_lifecycle_close_forecast_accepts_matching_current_identity():
    dashboard, prediction, pin_payload = _lifecycle_identity_inputs()

    assert _current_lifecycle_close_forecast(
        dashboard,
        prediction,
        pin_payload,
    ) == 7510.0


def test_expiration_view_preserves_exact_dates_and_explicit_missing_evidence():
    view = build_expiration_profile_view(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quote_age_seconds": 2.5,
            "validation_is_valid": True,
            "primary_expiration": "2026-08-26",
            "selected_target_mode": "primary_expiration",
            "subscription_profile": "near-term-shadow",
            "expiration_profiles": [
                {
                    "expiration": "2026-08-26",
                    "dte": 0,
                    "bucket": "0DTE",
                    "pin": 7500.0,
                    "max_pain": None,
                    "zero_gamma": 7480.0,
                    "gross_gex": 100.0,
                    "net_gex": 10.0,
                    "contracts": 25,
                },
                {
                    "expiration": "2026-08-27",
                    "dte": 1,
                    "bucket": "1-3DTE",
                    "pin": 7510.0,
                    "max_pain": 7520.0,
                    "zero_gamma": None,
                    "gross_gex": 80.0,
                    "net_gex": -4.0,
                    "contracts": 20,
                    "validation_is_valid": True,
                    "quote_coverage_ratio": 0.75,
                    "quote_age_seconds": 1.25,
                    "open_interest": 12345.0,
                    "open_interest_as_of_utc": "2026-08-25T20:00:00+00:00",
                },
            ],
            "subscription_expirations": [
                {"expiration": "2026-08-26", "stage": 0, "role": "primary", "contracts": 50},
                {"expiration": "2026-08-27", "stage": 1, "role": "next-listed", "contracts": 40},
                {"expiration": "2026-08-31", "stage": 2, "role": "4-45DTE", "contracts": 30},
            ],
            "universe_provenance": {
                "mode": "prior_cache_filtered",
                "source_date": "2026-08-25",
                "is_fallback": True,
                "source_sha256": "a" * 64,
                "provider_statistics_end": None,
            },
        },
        stale_after_seconds=30.0,
    )

    assert [row["expiration"] for row in view["rows"]] == [
        "2026-08-26", "2026-08-27", "2026-08-31"
    ]
    primary, tomorrow, missing = view["rows"]
    assert primary["validation_status"] == "valid_primary"
    assert primary["max_pain"] is None
    assert primary["calculation_coverage_ratio"] == 0.5
    assert tomorrow["validation_status"] == "valid_profile_context"
    assert tomorrow["validation_is_valid"] is True
    assert tomorrow["quote_coverage_ratio"] == 0.75
    assert tomorrow["freshness_scope"] == "profile"
    assert tomorrow["open_interest"] == 12345.0
    assert tomorrow["open_interest_as_of_utc"] == "2026-08-25T20:00:00+00:00"
    assert missing["validation_status"] == "unavailable"
    assert missing["gamma_pin"] is None
    assert missing["gross_gex"] is None
    assert missing["open_interest_as_of_utc"] is None
    assert view["open_interest_as_of_utc"] is None
    assert view["provenance"]["source_date"] == "2026-08-25"


def test_expiration_view_labels_valid_vix_primary_as_forward_context_not_0dte():
    expiration = "2026-09-16"
    view = build_expiration_profile_view(
        {
            "symbol": "VIX",
            "validation_is_valid": True,
            "primary_expiration": expiration,
            "primary_expiration_authority": (
                "vix_forward_expiration_context_only"
            ),
            "primary_expiration_context_only": True,
            "same_day_authority": False,
            "primary_expiration_selection_basis": (
                "vix_last_trading_day_precedes_settlement_date"
            ),
            "selected_target_mode": "primary_expiration_forward_context",
            "expiration_profiles": [
                {
                    "expiration": expiration,
                    "dte": 7,
                    "bucket": "4-45DTE",
                    "pin": 19.0,
                    "max_pain": 20.0,
                    "gross_gex": 100.0,
                    "net_gex": 10.0,
                    "contracts": 20,
                }
            ],
            "subscription_expirations": [
                {
                    "expiration": expiration,
                    "stage": 0,
                    "role": "primary",
                    "contracts": 40,
                    "authority": "vix_forward_expiration_context_only",
                    "context_only": True,
                    "same_day_authority": False,
                    "selection_basis": (
                        "vix_last_trading_day_precedes_settlement_date"
                    ),
                }
            ],
        }
    )

    primary = view["rows"][0]
    assert primary["validation_status"] == "valid_primary_context"
    assert primary["validation_is_valid"] is True
    assert primary["context_only"] is True
    assert primary["same_day_authority"] is False
    assert primary["authority"] == "vix_forward_expiration_context_only"
    assert primary["mode"] == (
        "primary forward context / primary_expiration_forward_context"
    )
    assert any(
        "not 0DTE authority" in reason
        for reason in primary["validation_reasons"]
    )
    assert view["primary_expiration_context_only"] is True


def test_expiration_view_does_not_mislabel_statistics_query_end_as_oi_asof():
    view = build_expiration_profile_view(
        {
            "primary_expiration": "2026-08-26",
            "validation_is_valid": False,
            "validation_failure_reasons": ["CHAIN_TOO_THIN"],
            "expiration_profiles": [],
            "subscription_expirations": [
                {"expiration": "2026-08-26", "role": "primary", "contracts": 100}
            ],
            "universe_provenance": {
                "source_date": "2026-08-25",
                "provider_statistics_end": "2026-08-25T20:00:00+00:00",
            },
        }
    )

    row = view["rows"][0]
    assert row["validation_status"] == "unavailable"
    assert row["open_interest_as_of_utc"] is None
    assert row["open_interest_source_end_utc"] == "2026-08-25T20:00:00+00:00"


def test_promoted_prediction_client_preserves_empty_not_zero(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return {"prediction_mode": "tcbbo_promoted", "is_estimate": True, "rows": []}

    monkeypatch.setattr(
        "app.services.live_data_client.requests.get", lambda *_args, **_kwargs: _Response()
    )
    payload = fetch_promoted_closing_tape_predictions()
    assert payload["rows"] == []
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["authority_state"] == "unavailable"
    assert payload["prediction_authority"]["tcbbo_promoted"] is False


def test_closing_tape_status_preserves_read_timeout(monkeypatch):
    def _timeout(*_args, **_kwargs):
        raise requests.exceptions.ReadTimeout("deliberate")

    monkeypatch.setattr("app.services.live_data_client.requests.get", _timeout)

    payload = fetch_closing_tape_status(timeout_seconds=2.5)

    assert payload["transport_ok"] is False
    assert payload["transport_error"] == "read_timeout"
    assert payload["read_succeeded"] is False
    assert payload["state"] == "unavailable"
    assert payload["reasons"] == ["response timed out after 2.5s"]


def test_closing_tape_readiness_transport_failure_is_unknown_not_zero(monkeypatch):
    class _Response:
        status_code = 503

    monkeypatch.setattr(
        "app.services.live_data_client.requests.get",
        lambda *_args, **_kwargs: _Response(),
    )

    payload = fetch_closing_tape_readiness()

    assert payload["transport_ok"] is False
    assert payload["transport_error"] == "http_error"
    assert payload["catalogs"] is None
    assert payload["sessions"] is None
    assert payload["unavailable_reason"] == "backend returned HTTP 503"


def test_live_pipeline_gate_requires_current_epoch_generation_and_symbol():
    pipeline = _current_pipeline()

    assert live_pipeline_runtime_ready(pipeline) is True
    assert live_pipeline_symbol_is_current(pipeline, "SPX") is True
    assert live_pipeline_symbol_is_current(pipeline, "NDX") is False

    pipeline["symbol_status"]["SPX"]["subscription_generation"] = 6
    assert live_pipeline_symbol_is_current(pipeline, "SPX") is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("handoff_status", "stopped"),
        ("subscription_epoch_id", "E" * 64),
        ("epoch_mismatch_symbols", ["SPX"]),
        ("generation_mismatch_symbols", ["SPX"]),
    ],
)
def test_live_pipeline_gate_fails_closed_for_runtime_mismatch(field, value):
    pipeline = _current_pipeline()
    pipeline[field] = value

    assert live_pipeline_runtime_ready(pipeline) is False
    assert live_pipeline_symbol_is_current(pipeline, "SPX") is False


def test_poll_live_status_preserves_authoritative_runtime_pipeline(monkeypatch):
    health = {
        "websocket": "active",
        "valid_symbols": ["SPX"],
        "prediction_pipeline_ok": True,
    }
    pipeline = _current_pipeline()
    universe = {"symbols_subscribed": 42}

    class _Response:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    def get(url, **_kwargs):
        if url.endswith("/events/live"):
            raise requests.exceptions.ConnectionError("SSE unavailable")
        if url.endswith("/health/live"):
            return _Response(200, pipeline)
        if url.endswith("/health"):
            return _Response(200, health)
        if url.endswith("/databento/universe"):
            return _Response(200, universe)
        raise AssertionError(url)

    monkeypatch.setattr("app.services.live_data_client.requests.get", get)

    snapshot = fetch_live_status_snapshot()

    assert snapshot["source"] == "poll"
    assert snapshot["health"] == health
    assert snapshot["pipeline"] == pipeline
    assert live_pipeline_symbol_is_current(snapshot["pipeline"], "SPX") is True


def test_poll_live_status_fails_closed_when_runtime_pipeline_is_unavailable(monkeypatch):
    health = {
        "websocket": "active",
        "valid_symbols": ["SPX"],
        "prediction_pipeline_ok": True,
        "handoff_status": "active",
        "subscription_epoch_id": CURRENT_EPOCH,
        "subscription_epoch_valid": True,
        "active_generation": 7,
    }

    class _Response:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    def get(url, **_kwargs):
        if url.endswith("/events/live"):
            raise requests.exceptions.ConnectionError("SSE unavailable")
        if url.endswith("/health/live"):
            return _Response(503, {})
        if url.endswith("/health"):
            return _Response(200, health)
        if url.endswith("/databento/universe"):
            return _Response(200, {})
        raise AssertionError(url)

    monkeypatch.setattr("app.services.live_data_client.requests.get", get)

    snapshot = fetch_live_status_snapshot()

    assert snapshot["pipeline"]["reported_prediction_pipeline_ok"] is True
    assert snapshot["pipeline"]["prediction_pipeline_ok"] is False
    assert snapshot["pipeline"]["runtime_context_available"] is False
    assert live_pipeline_symbol_is_current(snapshot["pipeline"], "SPX") is False
