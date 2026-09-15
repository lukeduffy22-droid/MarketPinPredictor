import asyncio
import hashlib
import json
from datetime import datetime, timezone

import pytest

from backend.api import helpers as api_helpers
from backend.api.routers import gex as gex_router
from backend.api.routers import health as health_router
from backend.api.routers import predict as predict_router
from backend.workstation import (
    PREDICTIVE_PIN_PAYLOAD_FIELDS,
    build_workstation_state,
    workstation_state_store,
)

EPOCH_ID = "e" * 64


class _ActiveRuntime:
    def __init__(
        self,
        *,
        epoch_id=EPOCH_ID,
        generation=9,
        handoff_status="active",
        rows=None,
        pin=None,
    ):
        self.subscription_epoch_id = epoch_id
        self.active_generation = generation
        self.handoff_status = handoff_status
        self.rows = list(rows or [])
        self.pin = pin

    def get_subscription_context(self):
        return {
            "subscription_epoch_id": self.subscription_epoch_id,
            "subscription_generation": self.active_generation,
            "handoff_status": self.handoff_status,
        }

    def get_latest_data(self, _symbol, n=1):
        assert n == 1
        return list(self.rows[-n:])

    def get_latest_pin(self, _symbol):
        return self.pin if self.handoff_status == "active" else None


@pytest.fixture(autouse=True)
def _isolated_workstation_store(monkeypatch):
    runtime = _ActiveRuntime()
    monkeypatch.setattr(gex_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr(predict_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr(health_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr("backend.api.helpers.get_streamer", lambda: runtime)
    workstation_state_store.reset()
    yield runtime
    workstation_state_store.reset()


def _publish_live_state():
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "predicted_close": 7511.0,
        "likely_close": 7510.0,
        "expected_close": 7509.0,
        "close_range_low": 7495.0,
        "close_range_high": 7525.0,
        "gamma_pin": 7505.0,
        "zero_gamma": 7490.0,
        "positive_gex_wall": 7520.0,
        "negative_gex_wall": 7485.0,
        "top_strike_share": 0.4,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    prediction = {
        "symbol": "SPX",
        "usable": True,
        "current_price": 7500.0,
        "predicted_close": 7511.0,
        "confidence": 70.0,
        "expected_move_points": 11.0,
        "expected_move_pct": 11.0 / 7500.0,
        "net_bias": "bullish",
        "signals": [
            {"name": "leaky-target", "value": 7511.0, "distance_points": 11.0}
        ],
        "feature_snapshot": {
            "likely_close": 7511.0,
            "anchor_inputs": {"target": 7511.0},
        },
        "vix_pressure": 0.2,
        "vix_adjustment": 4.0,
        "historical_adjustment": 7.0,
        "inference_time_ms": 1.25,
        "model_type": "Databento Quant Ensemble",
        "model_version": "test-v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "live_inference_feature_schema_version": "databento-gex-structure-v1",
        "live_inference_features": {
            "price": 7500.0,
            "volume": None,
            "zero_gamma_distance": -10.0 / 7500.0,
            "wall_asymmetry": 1.0 / 7.0,
            "top_strike_concentration": 0.4,
        },
    }
    origin = {
        "schema_version": "prediction-passport-v1",
        "origin_kind": "test",
        "origin_key": "purity",
    }
    forecast_id = hashlib.sha256(
        json.dumps(origin, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    passport = {
        "forecast_id": forecast_id,
        "symbol": "SPX",
        "origin": origin,
        "prediction_mode": "backend_lifecycle",
        "state": "RESEARCH_ONLY",
        "decision_grade": False,
        "provenance": {},
        "model": {},
        "prediction": {
            "point_estimate": 7511.0,
            "interval_lower": 7500.0,
            "interval_upper": 7520.0,
            "interval_target_coverage": 0.9,
        },
        "quality": {
            "validation_status": "valid",
            "state_reasons": ["RESEARCH_ONLY_NOT_DECISION_GRADE"],
            "missing_evidence": [],
        },
    }
    passport["record_sha256"] = hashlib.sha256(
        json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return workstation_state_store.publish(
        build_workstation_state(
            symbol="SPX",
            payload=payload,
            prediction=prediction,
            passport=passport,
        )
    )


def _assert_pin_payload_has_no_forecast_fields(payload):
    assert isinstance(payload, dict)
    assert PREDICTIVE_PIN_PAYLOAD_FIELDS.isdisjoint(payload)


def _complete_passport(label, *, state="RESEARCH_ONLY"):
    origin = {
        "schema_version": "prediction-passport-v1",
        "origin_kind": "test",
        "origin_key": label,
    }
    forecast_id = hashlib.sha256(
        json.dumps(origin, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    passport = {
        "forecast_id": forecast_id,
        "symbol": "SPX",
        "origin": origin,
        "prediction_mode": "backend_lifecycle",
        "state": state,
        "decision_grade": False,
        "provenance": {},
        "model": {},
        "prediction": {
            "point_estimate": 6555.0,
            "interval_lower": 6540.0,
            "interval_upper": 6570.0,
            "interval_target_coverage": 0.9,
        },
        "quality": {
            "validation_status": "valid" if state != "ABSTAIN" else "invalid",
            "state_reasons": [] if state != "ABSTAIN" else ["SOURCE_INVALID"],
            "missing_evidence": [],
        },
    }
    passport["record_sha256"] = hashlib.sha256(
        json.dumps(passport, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return passport


def _forecast_bearing_prediction():
    return {
        "symbol": "SPX",
        "usable": True,
        "predicted_close": 6555.0,
        "confidence": 88.0,
        "expected_move_points": 55.0,
        "expected_move_pct": 55.0 / 6500.0,
        "net_bias": "bullish",
        "signals": [
            {
                "name": "reconstructive-signal",
                "value": 6555.0,
                "weight": 1.0,
                "distance_points": 55.0,
            }
        ],
        "feature_snapshot": {
            "likely_close": 6555.0,
            "anchor_inputs": {"target": 6555.0},
            "vix_adjustment": 21.0,
            "historical_adjustment": 34.0,
        },
        "vix_pressure": 0.9,
        "vix_adjustment": 21.0,
        "historical_adjustment": 34.0,
        "drift_adjustment": 55.0,
        "model_type": "test-model",
        "model_version": "test-v1",
        "inference_device": "cpu",
        "inference_time_ms": 1.25,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _assert_nonlive_prediction_is_nonreconstructive(prediction):
    assert prediction["usable"] is False
    assert prediction["is_estimate"] is False
    assert prediction["predicted_close"] is None
    assert prediction["prediction_lower"] is None
    assert prediction["prediction_upper"] is None
    assert prediction["interval_target_coverage"] is None
    assert prediction["confidence"] is None
    assert prediction["signals"] == []
    assert prediction["feature_snapshot"] == {}
    assert {
        "expected_move_points",
        "expected_move_pct",
        "net_bias",
        "vix_pressure",
        "vix_adjustment",
        "historical_adjustment",
        "drift_adjustment",
        "anchor_inputs",
    }.isdisjoint(prediction)


def test_legacy_dashboard_reads_are_side_effect_free(monkeypatch):
    published = _publish_live_state()
    monkeypatch.setattr(
        gex_router,
        "save_prediction_snapshot",
        lambda *_args, **_kwargs: pytest.fail("GET must not persist"),
    )

    first = gex_router._symbol_dashboard_payload("SPX")
    second = gex_router._symbol_dashboard_payload("SPX")

    assert first == second
    assert first["status"] == "ready"
    assert first["prediction"]["usable"] is True
    assert first["prediction"]["predicted_close"] == pytest.approx(7511.0)
    _assert_pin_payload_has_no_forecast_fields(first["pin_payload"])
    assert workstation_state_store.sequence == published["sequence"] == 1


def test_prediction_get_reads_state_without_using_db_or_inference(monkeypatch):
    published = _publish_live_state()

    class _ForbiddenDb:
        def add(self, *_args, **_kwargs):
            raise AssertionError("GET must not add a database row")

        def commit(self):
            raise AssertionError("GET must not commit")

    monkeypatch.setattr(
        "backend.ai_predictor.build_ai_prediction",
        lambda *_args, **_kwargs: pytest.fail("GET must not run inference"),
    )
    monkeypatch.setattr(
        predict_router,
        "build_live_inference_features",
        lambda *_args, **_kwargs: pytest.fail("GET must not derive inference features"),
    )

    response = asyncio.run(
        predict_router._predict_close_impl("SPX", db=_ForbiddenDb())
    )
    raw = asyncio.run(
        predict_router._predict_close_impl("SPX", db=_ForbiddenDb(), raw=True)
    )

    assert response.predicted_close == pytest.approx(7511.0)
    assert raw["state_sequence"] == 1
    assert raw["forecast_id"] == hashlib.sha256(
        json.dumps(
            {
                "schema_version": "prediction-passport-v1",
                "origin_kind": "test",
                "origin_key": "purity",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    assert raw["forecast_state"] == "RESEARCH_ONLY"
    assert raw["decision_grade"] is False
    assert raw["live_inference_feature_schema_version"] == "databento-gex-structure-v1"
    assert raw["live_inference_features"]["zero_gamma_distance"] == pytest.approx(
        -10.0 / 7500.0
    )
    assert raw["live_inference_features"]["wall_asymmetry"] == pytest.approx(1.0 / 7.0)
    assert raw["live_inference_features"]["top_strike_concentration"] == pytest.approx(0.4)
    assert raw["predicted_close"] == pytest.approx(7511.0)
    _assert_pin_payload_has_no_forecast_fields(raw["pin_payload"])
    assert workstation_state_store.sequence == published["sequence"] == 1


def test_after_hours_flag_never_overrides_stale_passport_gate():
    state = _publish_live_state()
    state["status"] = "stale"
    state["prediction"]["usable"] = False
    state["prediction"]["predicted_close"] = None
    state["health"]["is_stale"] = True
    state["health"]["usable_for_prediction"] = False
    workstation_state_store.publish(state)

    with pytest.raises(predict_router.HTTPException) as exc_info:
        asyncio.run(
            predict_router._predict_close_impl(
                "SPX",
                allow_after_hours=True,
            )
        )
    raw = asyncio.run(
        predict_router._predict_close_impl(
            "SPX",
            allow_after_hours=True,
            raw=True,
        )
    )

    assert exc_info.value.status_code == 503
    assert "does not override" in str(exc_info.value.detail)
    assert raw["predicted_close"] is None
    assert raw["usable"] is False
    assert raw["after_hours_override_requested"] is True
    assert raw["after_hours_override_applied"] is False


def test_versioned_state_read_does_not_publish():
    published = _publish_live_state()

    first = asyncio.run(gex_router.get_workstation_state("SPX"))
    second = asyncio.run(gex_router.get_workstation_state("SPX"))

    assert first == second
    assert first["contract_version"] == "workstation-state.v1"
    assert first["state_revision"] == 1
    assert first["forecast_id"] is not None
    assert first["prediction"]["predicted_close"] == pytest.approx(7511.0)
    _assert_pin_payload_has_no_forecast_fields(first["pin_payload"])
    assert workstation_state_store.sequence == published["sequence"] == 1


def test_published_live_forecast_is_only_in_governed_prediction_across_reads(
    _isolated_workstation_store,
):
    published = _publish_live_state()
    _isolated_workstation_store.rows = [
        {
            **published["pin_payload"],
            "predicted_close": 7511.0,
            "likely_close": 7510.0,
            "expected_close": 7509.0,
            "close_range_low": 7495.0,
            "close_range_high": 7525.0,
        }
    ]

    latest = asyncio.run(gex_router.get_buffer_latest("SPX"))
    gex = asyncio.run(gex_router.get_gex("SPX"))
    dashboard = gex_router._symbol_dashboard_payload("SPX")
    versioned = asyncio.run(gex_router.get_workstation_state("SPX"))
    raw_prediction = asyncio.run(predict_router.predict_ai_compatible("SPX"))
    event = asyncio.run(health_router.workstation_event_snapshot())
    stream = asyncio.run(
        health_router.workstation_events(after_sequence=0, last_event_id=None)
    )

    async def first_stream_event():
        iterator = stream.body_iterator
        chunk = await anext(iterator)
        await iterator.aclose()
        return chunk

    raw_event = asyncio.run(first_stream_event())
    streamed = json.loads(raw_event.split("data: ", 1)[1])

    for pin_payload in (
        latest,
        gex,
        dashboard["pin_payload"],
        versioned["pin_payload"],
        raw_prediction["pin_payload"],
        event["states"][0]["pin_payload"],
        streamed["states"][0]["pin_payload"],
    ):
        _assert_pin_payload_has_no_forecast_fields(pin_payload)
    assert dashboard["prediction"]["predicted_close"] == pytest.approx(7511.0)
    assert versioned["prediction"]["predicted_close"] == pytest.approx(7511.0)
    assert raw_prediction["predicted_close"] == pytest.approx(7511.0)
    assert event["states"][0]["prediction"]["predicted_close"] == pytest.approx(
        7511.0
    )
    assert streamed["states"][0]["prediction"]["predicted_close"] == pytest.approx(
        7511.0
    )
    assert versioned["prediction"]["expected_move_points"] == pytest.approx(11.0)
    assert versioned["prediction"]["confidence"] == pytest.approx(70.0)
    assert versioned["prediction"]["signals"][0]["value"] == pytest.approx(7511.0)
    assert versioned["prediction"]["feature_snapshot"]["likely_close"] == pytest.approx(
        7511.0
    )


@pytest.mark.parametrize(
    ("case", "payload_overrides"),
    [
        ("warming", {}),
        (
            "invalid",
            {
                "validation_is_valid": False,
                "gamma_excluded_from_model": True,
                "validation_failure_reasons": ["SOURCE_INVALID"],
            },
        ),
        (
            "stale",
            {"timestamp": "2020-01-01T00:00:00+00:00"},
        ),
    ],
)
def test_published_nonlive_states_never_expose_pin_forecast_fields(
    case, payload_overrides
):
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "gamma_pin": 7505.0,
        "predicted_close": 7511.0,
        "likely_close": 7510.0,
        "expected_close": 7509.0,
        "close_range_low": 7495.0,
        "close_range_high": 7525.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
        **payload_overrides,
    }
    published = workstation_state_store.publish(
        build_workstation_state(symbol="SPX", payload=payload, prediction=None)
    )
    assert published["status"] == case

    dashboard = gex_router._symbol_dashboard_payload("SPX")
    versioned = asyncio.run(gex_router.get_workstation_state("SPX"))
    gex = asyncio.run(gex_router.get_gex("SPX"))
    raw_prediction = asyncio.run(predict_router.predict_ai_compatible("SPX"))
    event = asyncio.run(health_router.workstation_event_snapshot())

    for pin_payload in (
        published["pin_payload"],
        dashboard["pin_payload"],
        versioned["pin_payload"],
        gex,
        raw_prediction["pin_payload"],
        event["states"][0]["pin_payload"],
    ):
        _assert_pin_payload_has_no_forecast_fields(pin_payload)
    for prediction in (
        dashboard["prediction"],
        versioned["prediction"],
        event["states"][0]["prediction"],
    ):
        if prediction is not None:
            assert prediction["predicted_close"] is None
            assert prediction["usable"] is False
    assert raw_prediction["predicted_close"] is None


@pytest.mark.parametrize(
    ("case", "passport", "timestamp", "expected_status"),
    [
        (
            "abstain",
            _complete_passport("abstain-leak", state="ABSTAIN"),
            None,
            "invalid",
        ),
        (
            "incomplete-passport",
            {"forecast_id": "f" * 64},
            None,
            "invalid",
        ),
        (
            "stale",
            _complete_passport("stale-leak"),
            "2020-01-01T00:00:00+00:00",
            "stale",
        ),
    ],
)
def test_nonlive_prediction_cannot_reconstruct_forecast_across_read_surfaces(
    case, passport, timestamp, expected_status
):
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "price": 6500.0,
        "likely_close": 6555.0,
        "gamma_pin": 6510.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    built = build_workstation_state(
        symbol="SPX",
        payload=payload,
        prediction=_forecast_bearing_prediction(),
        passport=passport,
    )
    assert built["status"] == expected_status, case
    _assert_nonlive_prediction_is_nonreconstructive(built["prediction"])
    # Simulate an older/inconsistent producer handing publication a polluted
    # non-live projection; the store boundary must independently fail closed.
    built["prediction"].update(_forecast_bearing_prediction())
    published = workstation_state_store.publish(built)

    dashboard = gex_router._symbol_dashboard_payload("SPX")
    versioned = asyncio.run(gex_router.get_workstation_state("SPX"))
    raw_prediction = asyncio.run(predict_router.predict_ai_compatible("SPX"))
    snapshot = asyncio.run(health_router.workstation_event_snapshot())
    stream = asyncio.run(
        health_router.workstation_events(after_sequence=0, last_event_id=None)
    )

    async def first_stream_event():
        iterator = stream.body_iterator
        chunk = await anext(iterator)
        await iterator.aclose()
        return chunk

    streamed = json.loads(asyncio.run(first_stream_event()).split("data: ", 1)[1])

    for state in (
        published,
        dashboard,
        versioned,
        snapshot["states"][0],
        streamed["states"][0],
    ):
        assert state["status"] == expected_status
        _assert_pin_payload_has_no_forecast_fields(state["pin_payload"])
        _assert_nonlive_prediction_is_nonreconstructive(state["prediction"])
    _assert_pin_payload_has_no_forecast_fields(raw_prediction["pin_payload"])
    _assert_nonlive_prediction_is_nonreconstructive(raw_prediction)
    assert set(raw_prediction["live_inference_features"].values()) == {None}


def test_unknown_versioned_state_reads_are_stable_and_do_not_publish():
    first = asyncio.run(gex_router.get_workstation_state("SPX"))
    second = asyncio.run(gex_router.get_workstation_state("SPX"))

    assert first == second
    assert first["status"] == "unavailable"
    assert first["sequence"] == first["state_revision"] == 0
    assert workstation_state_store.sequence == 0


def test_unknown_ai_compatibility_read_uses_static_empty_feature_vector(monkeypatch):
    monkeypatch.setattr(
        predict_router,
        "build_live_inference_features",
        lambda *_args, **_kwargs: pytest.fail("GET must not derive inference features"),
    )

    first = asyncio.run(predict_router.predict_ai_compatible("SPX"))
    second = asyncio.run(predict_router.predict_ai_compatible("SPX"))

    assert first["live_inference_features"] == second["live_inference_features"]
    assert first["live_inference_features"] == {
        "price": None,
        "volume": None,
        "zero_gamma_distance": None,
        "wall_asymmetry": None,
        "top_strike_concentration": None,
    }
    assert workstation_state_store.sequence == 0


def test_unpublished_valid_live_payload_is_diagnostic_without_get_time_forecast(
    monkeypatch, _isolated_workstation_store
):
    source = {
        "symbol": "SPX",
        "provider": "databento",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price": 7500.0,
        "predicted_close": 7511.0,
        "likely_close": 7510.0,
        "gamma_pin": 7505.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    _isolated_workstation_store.rows = [source]

    def _forbidden(*_args, **_kwargs):
        raise AssertionError("GET must not invoke fallback history or prediction helpers")

    monkeypatch.setattr(api_helpers, "pin_payload_for_symbol", _forbidden)
    monkeypatch.setattr(api_helpers, "get_historical_context", _forbidden)
    monkeypatch.setattr(api_helpers, "get_market_history", _forbidden)
    monkeypatch.setattr("backend.ai_predictor.build_ai_prediction", _forbidden)

    payload = gex_router._symbol_dashboard_payload("SPX")
    gex_payload = asyncio.run(gex_router.get_gex("SPX"))

    assert payload["status"] == "warming"
    assert payload["prediction"] is None
    assert "predicted_close" not in payload["pin_payload"]
    assert "likely_close" not in payload["pin_payload"]
    assert gex_payload["forecast_state"] == "UNAVAILABLE"
    assert gex_payload["usable_for_prediction"] is False
    assert "predicted_close" not in gex_payload
    assert "likely_close" not in gex_payload
    assert workstation_state_store.sequence == 0


def test_generic_market_endpoint_rejects_structured_invalid_diagnostics(monkeypatch):
    class _InvalidBuffer:
        subscription_epoch_id = EPOCH_ID
        active_generation = 9
        handoff_status = "active"

        def get_latest_data(self, _symbol, n=1):
            assert n == 1
            return [
                {
                    "symbol": "SPX",
                    "price": 7500.0,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "validation_is_valid": False,
                    "gamma_excluded_from_model": True,
                    "subscription_epoch_id": EPOCH_ID,
                    "subscription_generation": 9,
                    "validation_failure_reasons": [
                        "PRIMARY_PAIR_COVERAGE_LOW: 0.08 < 0.1"
                    ],
                }
            ]

    monkeypatch.setattr(gex_router, "get_streamer", lambda: _InvalidBuffer())

    with pytest.raises(gex_router.HTTPException) as exc_info:
        asyncio.run(gex_router.get_market_data("SPX"))

    assert exc_info.value.status_code == 409
    assert "PRIMARY_PAIR_COVERAGE_LOW" in str(exc_info.value.detail)


def test_generic_market_endpoint_rejects_stale_current_runtime_quote(monkeypatch):
    observed = datetime.now(timezone.utc).isoformat()

    class _ValidBuffer:
        subscription_epoch_id = EPOCH_ID
        active_generation = 9
        handoff_status = "active"

        def get_latest_data(self, _symbol, n=1):
            assert n == 1
            return [
                {
                    "symbol": "SPX",
                    "price": 7500.0,
                    "timestamp_utc": observed,
                    "quote_age_seconds": 123.0,
                    "validation_is_valid": True,
                    "gamma_excluded_from_model": False,
                    "subscription_epoch_id": EPOCH_ID,
                    "subscription_generation": 9,
                }
            ]

    monkeypatch.setattr(gex_router, "get_streamer", lambda: _ValidBuffer())

    with pytest.raises(gex_router.HTTPException) as exc_info:
        asyncio.run(gex_router.get_market_data("SPX"))

    assert exc_info.value.status_code == 503
    assert "stale" in str(exc_info.value.detail).lower()


def test_generic_market_endpoint_accepts_fresh_current_runtime_quote(monkeypatch):
    observed = datetime.now(timezone.utc).isoformat()
    payload = {
        "symbol": "SPX",
        "price": 7500.0,
        "timestamp_utc": observed,
        "quote_age_seconds": 1.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    monkeypatch.setattr(
        gex_router,
        "get_streamer",
        lambda: _ActiveRuntime(rows=[payload]),
    )

    response = asyncio.run(gex_router.get_market_data("SPX"))

    assert response.price == pytest.approx(7500.0)
    assert response.data_age_seconds <= gex_router.LIVE_DATA_STALE_AFTER_SECONDS


def test_generic_market_endpoint_accepts_exact_freshness_boundary(monkeypatch):
    payload = {
        "symbol": "SPX",
        "price": 7500.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    monkeypatch.setattr(
        gex_router,
        "get_streamer",
        lambda: _ActiveRuntime(rows=[payload]),
    )
    monkeypatch.setattr(
        gex_router,
        "payload_data_age_seconds",
        lambda _payload: gex_router.LIVE_DATA_STALE_AFTER_SECONDS,
    )

    response = asyncio.run(gex_router.get_market_data("SPX"))

    assert response.price == pytest.approx(7500.0)
    assert response.data_age_seconds == gex_router.LIVE_DATA_STALE_AFTER_SECONDS


def test_buffer_latest_preserves_stale_payload_as_diagnostic_context(monkeypatch):
    payload = {
        "symbol": "SPX",
        "price": 7500.0,
        "predicted_close": 7511.0,
        "likely_close": 7510.0,
        "expected_close": 7509.0,
        "close_range_low": 7495.0,
        "close_range_high": 7525.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "quote_age_seconds": 123.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    monkeypatch.setattr(
        gex_router,
        "get_streamer",
        lambda: _ActiveRuntime(rows=[payload]),
    )

    response = asyncio.run(gex_router.get_buffer_latest("SPX"))

    assert response["price"] == pytest.approx(7500.0)
    assert response["usable_for_prediction"] is False
    assert response["diagnostic_only"] is True
    _assert_pin_payload_has_no_forecast_fields(response)


def test_stopped_runtime_never_falls_through_to_retained_raw_buffer(monkeypatch):
    retained = {
        "symbol": "SPX",
        "provider": "databento",
        "price": 6500.0,
        "gamma_pin": 6510.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
    }
    stopped = _ActiveRuntime(
        handoff_status="stopped",
        rows=[retained],
        pin=retained,
    )
    monkeypatch.setattr(api_helpers, "get_streamer", lambda: stopped)
    monkeypatch.setattr(api_helpers, "market_is_closed", lambda: False)
    monkeypatch.setattr(gex_router, "get_streamer", lambda: stopped)

    assert api_helpers.pin_payload_for_symbol("SPX") is None
    with pytest.raises(gex_router.HTTPException) as market_error:
        asyncio.run(gex_router.get_market_data("SPX"))
    with pytest.raises(gex_router.HTTPException) as buffer_error:
        asyncio.run(gex_router.get_buffer_latest("SPX"))
    with pytest.raises(gex_router.HTTPException) as gex_error:
        asyncio.run(gex_router.get_gex("SPX"))

    assert market_error.value.status_code == 503
    assert buffer_error.value.status_code == 503
    assert gex_error.value.status_code == 503
    assert "HANDOFF_NOT_ACTIVE:stopped" in str(market_error.value.detail)


@pytest.mark.parametrize(
    "identity_override",
    [
        {"subscription_epoch_id": "E" * 64},
        {"subscription_epoch_id": None},
        {"subscription_generation": 0},
        {"subscription_generation": None},
    ],
)
def test_market_endpoint_rejects_noncanonical_payload_runtime_identity(
    monkeypatch,
    identity_override,
):
    retained = {
        "symbol": "SPX",
        "price": 6500.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": EPOCH_ID,
        "subscription_generation": 9,
        **identity_override,
    }
    runtime = _ActiveRuntime(rows=[retained])
    monkeypatch.setattr(gex_router, "get_streamer", lambda: runtime)

    with pytest.raises(gex_router.HTTPException) as exc_info:
        asyncio.run(gex_router.get_market_data("SPX"))

    assert exc_info.value.status_code == 503
    assert "current-runtime" in str(exc_info.value.detail)


@pytest.mark.parametrize(
    ("runtime", "expected_reason"),
    [
        (_ActiveRuntime(handoff_status="stopped"), "HANDOFF_NOT_ACTIVE"),
        (_ActiveRuntime(epoch_id="f" * 64), "SUBSCRIPTION_EPOCH_MISMATCH"),
        (_ActiveRuntime(generation=10), "SUBSCRIPTION_GENERATION_MISMATCH"),
    ],
)
def test_retained_workstation_state_is_never_current_across_runtime_boundary(
    monkeypatch,
    runtime,
    expected_reason,
):
    _publish_live_state()
    monkeypatch.setattr(gex_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr(predict_router, "get_streamer", lambda: runtime)
    monkeypatch.setattr(health_router, "get_streamer", lambda: runtime)

    dashboard = gex_router._symbol_dashboard_payload("SPX")
    versioned = asyncio.run(gex_router.get_workstation_state("SPX"))
    raw_prediction = asyncio.run(predict_router.predict_ai_compatible("SPX"))
    event = asyncio.run(health_router.workstation_event_snapshot())
    stream = asyncio.run(
        health_router.workstation_events(after_sequence=0, last_event_id=None)
    )

    async def first_stream_event():
        iterator = stream.body_iterator
        chunk = await anext(iterator)
        await iterator.aclose()
        return chunk

    raw_event = asyncio.run(first_stream_event())
    streamed = json.loads(raw_event.split("data: ", 1)[1])

    with pytest.raises(predict_router.HTTPException) as typed_error:
        asyncio.run(predict_router._predict_close_impl("SPX"))
    with pytest.raises(predict_router.HTTPException) as overlay_error:
        asyncio.run(predict_router.get_close_overlay("SPX"))

    assert dashboard["status"] == "unavailable"
    assert dashboard["current_price"] is None
    assert dashboard["pin_payload"] is None
    assert versioned["status"] == "unavailable"
    assert versioned["current_price"] is None
    assert versioned["prediction"] is None
    assert versioned["pin_payload"] is None
    assert any(
        expected_reason in reason
        for reason in versioned["health"]["validation_failure_reasons"]
    )
    assert raw_prediction["state_status"] == "unavailable"
    assert raw_prediction["usable"] is False
    assert raw_prediction["current_price"] is None
    assert raw_prediction["predicted_close"] is None
    assert raw_prediction["pin_payload"] is None
    assert typed_error.value.status_code == 503
    assert overlay_error.value.status_code == 503
    assert len(event["states"]) == 1
    assert event["states"][0]["status"] == "unavailable"
    assert event["states"][0]["current_price"] is None
    assert event["states"][0]["pin_payload"] is None
    assert streamed["states"][0]["status"] == "unavailable"
    assert streamed["states"][0]["current_price"] is None
    assert streamed["states"][0]["pin_payload"] is None


@pytest.mark.parametrize("nested_identity", ["pin_payload", "prediction"])
def test_workstation_projection_rejects_nested_identity_disagreement(
    nested_identity,
):
    state = _publish_live_state()
    state[nested_identity]["subscription_epoch_id"] = "f" * 64
    state[nested_identity]["subscription_generation"] = 9
    workstation_state_store.publish(state)

    projected = asyncio.run(gex_router.get_workstation_state("SPX"))
    raw_prediction = asyncio.run(predict_router.predict_ai_compatible("SPX"))

    assert projected["status"] == "unavailable"
    assert projected["current_price"] is None
    assert projected["pin_payload"] is None
    assert any(
        reason.startswith(f"{nested_identity}:SUBSCRIPTION_EPOCH_MISMATCH")
        for reason in projected["health"]["validation_failure_reasons"]
    )
    assert raw_prediction["state_status"] == "unavailable"
    assert raw_prediction["current_price"] is None
    assert raw_prediction["predicted_close"] is None
    assert raw_prediction["pin_payload"] is None


def test_openapi_exposes_typed_state_and_event_snapshot_contracts():
    from backend.app import app

    schema = app.openapi()

    state_response = schema["paths"]["/v1/workstation/state/{symbol}"]["get"][
        "responses"
    ]["200"]["content"]["application/json"]["schema"]
    event_response = schema["paths"]["/v1/workstation/events/snapshot"]["get"][
        "responses"
    ]["200"]["content"]["application/json"]["schema"]
    assert state_response["$ref"].endswith("/WorkstationStateV1")
    assert event_response["$ref"].endswith("/WorkstationEventV1")
