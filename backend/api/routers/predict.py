"""Prediction and calibration endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from backend.api.bounded_runtime_read import bounded_runtime_endpoint

from backend.api.helpers import (
    normalize_symbol,
    runtime_bound_workstation_state,
)
from backend.api.schemas import PredictionResponse
from backend.calibration import load_calibration
from backend.config import LIVE_DATA_STALE_AFTER_SECONDS
from backend.inference import (
    GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
    build_live_inference_features,
)
from backend.prediction_authority import build_prediction_authority
from backend.streamer import get_streamer  # legacy monkeypatch compatibility
from backend.workstation import sanitize_pin_payload, workstation_state_store

router = APIRouter(tags=["prediction"])

_EMPTY_LIVE_INFERENCE_FEATURES = {
    "price": None,
    "volume": None,
    "zero_gamma_distance": None,
    "wall_asymmetry": None,
    "top_strike_concentration": None,
}


def _utc_iso() -> str:
    """Return an explicit UTC ISO-8601 timestamp for API payloads."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@router.get(
    "/predict/close-overlay",
    summary="Get close-overlay signals",
    description=(
        "Returns lifecycle-published close signals only while their process epoch and "
        "generation match the active streamer handoff."
    ),
)
@bounded_runtime_endpoint
def get_close_overlay(symbol: str):
    """Return lightweight close-bias signals for the Streamlit close predictor panel."""
    normalized = normalize_symbol(symbol)
    retained = workstation_state_store.get(normalized)
    if retained is None:
        raise HTTPException(status_code=404, detail=f"No close-overlay data available for {symbol}")
    state = runtime_bound_workstation_state(
        get_streamer(),
        retained,
        symbol=normalized,
    )
    age = (state.get("health") or {}).get("data_age_seconds")
    if state.get("status") == "stale":
        detail = "unknown" if age is None else f"{age:.1f}s"
        raise HTTPException(
            status_code=503,
            detail=(
                f"Close-overlay payload is stale for {symbol} (age={detail}, "
                f"limit={LIVE_DATA_STALE_AFTER_SECONDS:.1f}s)"
            ),
        )

    prediction = dict(state.get("prediction") or {})
    if (
        state.get("status") != "live"
        or not state.get("forecast_id")
        or not prediction.get("usable")
        or prediction.get("predicted_close") is None
        or prediction.get("prediction_lower") is None
        or prediction.get("prediction_upper") is None
    ):
        raise HTTPException(
            status_code=503,
            detail=f"No passport-backed close-overlay prediction is available for {symbol}",
        )
    current_price = float(state.get("current_price") or 0)
    expected_close = float(prediction["predicted_close"])
    interval_lower = float(prediction["prediction_lower"])
    interval_upper = float(prediction["prediction_upper"])
    if not interval_lower <= expected_close <= interval_upper:
        raise HTTPException(
            status_code=503,
            detail=f"Passport-backed close interval is malformed for {symbol}",
        )
    prediction_authority = dict(
        state.get("prediction_authority")
        or prediction.get("prediction_authority")
        or build_prediction_authority(
            prediction_mode=prediction.get("prediction_mode") or "backend_lifecycle",
            forecast_state=state.get("forecast_state"),
            numeric_available=True,
            decision_grade=bool(state.get("decision_grade")),
            promotion_verified=False,
        )
    )
    drift = expected_close - current_price
    net_bias = "bullish" if drift > 0 else "bearish" if drift < 0 else "neutral"

    return {
        "symbol": normalized,
        "current_price": current_price,
        "expected_close": expected_close,
        "close_range_low": interval_lower,
        "close_range_high": interval_upper,
        "interval_target_coverage": prediction.get("interval_target_coverage"),
        "signals": prediction.get("signals", []),
        "net_bias": net_bias,
        "confidence": float(prediction.get("confidence") or 0.0),
        "drift_adjustment": drift,
        "summary": prediction.get("summary") or "Using latest passport-backed forecast",
        "call_gex": prediction.get("call_gex"),
        "put_gex": prediction.get("put_gex"),
        "gex_ratio": prediction.get("gex_ratio"),
        "minutes_to_close": prediction.get("minutes_to_close"),
        "timestamp": prediction.get("timestamp"),
        "forecast_id": state.get("forecast_id"),
        "forecast_state": state.get("forecast_state"),
        "decision_grade": bool(prediction_authority.get("decision_grade")),
        "prediction_mode": prediction_authority["prediction_mode"],
        "is_estimate": bool(prediction_authority["is_estimate"]),
        "prediction_authority": prediction_authority,
    }


@bounded_runtime_endpoint
def _predict_close_impl(
    symbol: str,
    db: object | None = None,
    allow_after_hours: bool = False,
    raw: bool = False,
):
    """Serialize lifecycle-published prediction state without model or DB work."""
    symbol = normalize_symbol(symbol)
    retained = workstation_state_store.get(symbol)
    if retained is None:
        raise HTTPException(status_code=404, detail=f"No market data for {symbol}")
    state = runtime_bound_workstation_state(
        get_streamer(),
        retained,
        symbol=symbol,
    )

    prediction = dict(state.get("prediction") or {})
    current_data = sanitize_pin_payload(state.get("pin_payload")) or {}
    persisted_live_features = prediction.get("live_inference_features")
    live_inference_features = (
        dict(persisted_live_features)
        if isinstance(persisted_live_features, dict)
        else dict(_EMPTY_LIVE_INFERENCE_FEATURES)
    )
    state_status = str(state.get("status") or "unavailable")
    prediction_authority = dict(
        state.get("prediction_authority")
        or prediction.get("prediction_authority")
        or build_prediction_authority(
            prediction_mode=prediction.get("prediction_mode") or "backend_lifecycle",
            forecast_state=state.get("forecast_state"),
            numeric_available=bool(
                state_status == "live"
                and prediction.get("usable")
                and prediction.get("predicted_close") is not None
            ),
            decision_grade=bool(state.get("decision_grade")),
            promotion_verified=False,
        )
    )
    current_age = (state.get("health") or {}).get("data_age_seconds")
    if state_status == "stale" and not raw:
        detail = "unknown" if current_age is None else f"{float(current_age):.1f}s"
        override_note = (
            " The allow_after_hours compatibility flag does not override freshness "
            "or passport evidence gates."
            if allow_after_hours
            else ""
        )
        raise HTTPException(
            status_code=503,
            detail=(
                f"Lifecycle-published data is stale for {symbol} (age={detail}, "
                f"limit={LIVE_DATA_STALE_AFTER_SECONDS:.1f}s). "
                f"Wait for a fresh backend state.{override_note}"
            ),
        )

    prediction.update(
        {
            "symbol": symbol,
            "current_price": (
                prediction.get("current_price")
                if state_status != "unavailable"
                else None
            ),
            "predicted_close": (
                prediction.get("predicted_close")
                if state_status == "live"
                else None
            ),
            "pin_payload": (
                current_data if state.get("pin_payload") is not None else None
            ),
            "data_age_seconds": current_age,
            "state_status": state_status,
            "state_revision": state.get("state_revision"),
            "state_sequence": state.get("sequence"),
            "forecast_id": state.get("forecast_id"),
            "forecast_state": state.get("forecast_state"),
            "decision_grade": bool(prediction_authority.get("decision_grade")),
            "prediction_mode": prediction_authority["prediction_mode"],
            "is_estimate": bool(prediction_authority["is_estimate"]),
            "prediction_authority": prediction_authority,
            "prediction_snapshot_id": state.get("prediction_snapshot_id"),
            "subscription_epoch_id": state.get("subscription_epoch_id"),
            "subscription_generation": state.get("subscription_generation"),
            "live_inference_feature_schema_version": (
                prediction.get("live_inference_feature_schema_version")
                or GEX_INFERENCE_FEATURE_SCHEMA_VERSION
            ),
            "live_inference_features": live_inference_features,
            "usable": bool(prediction.get("usable")) and state_status == "live",
            # Kept for existing clients, but this flag never bypasses
            # provenance, runtime binding, freshness, or passport gates.
            "after_hours_override_requested": bool(allow_after_hours),
            "after_hours_override_applied": False,
        }
    )
    if raw:
        return prediction

    if (
        state_status != "live"
        or not prediction.get("usable")
        or not state.get("forecast_id")
    ):
        override_note = (
            "; allow_after_hours does not override lifecycle evidence gates"
            if allow_after_hours
            else ""
        )
        raise HTTPException(
            status_code=503,
            detail=(
                f"No passport-backed lifecycle prediction is available for {symbol} "
                f"({state_status}){override_note}"
            ),
        )
    current_price = prediction.get("current_price")
    predicted_close = prediction.get("predicted_close")
    if current_price is None or predicted_close is None:
        raise HTTPException(status_code=404, detail=f"No usable price for {symbol}")
    return PredictionResponse(
        symbol=symbol,
        current_price=float(current_price),
        predicted_close=float(predicted_close),
        confidence=float(prediction.get("confidence") or 0.0),
        inference_time_ms=float(prediction.get("inference_time_ms") or 0.0),
        timestamp=str(prediction.get("timestamp") or state.get("generated_at_utc") or _utc_iso()),
        decision_grade=bool(prediction_authority.get("decision_grade")),
        forecast_id=state.get("forecast_id"),
        forecast_state=state.get("forecast_state"),
        state_status=state_status,
        state_revision=state.get("state_revision"),
        state_sequence=state.get("sequence"),
        prediction_mode=str(prediction_authority["prediction_mode"]),
        is_estimate=bool(prediction_authority["is_estimate"]),
        prediction_authority=prediction_authority,
    )


@router.get(
    "/predict/close",
    response_model=PredictionResponse,
    summary="Predict close by query symbol",
    description="Reads the latest lifecycle-published close projection for query parameter `symbol`.",
)
async def predict_close_override(symbol: str, allow_after_hours: bool = False):
    """Get a passport-backed EOD prediction; the legacy flag cannot bypass evidence gates."""
    return await _predict_close_impl(symbol, allow_after_hours=allow_after_hours)


@router.get(
    "/predict/{symbol}",
    response_model=PredictionResponse,
    summary="Predict close by path symbol",
    description="Reads the latest lifecycle-published close projection for a path symbol.",
)
async def predict_close(symbol: str, allow_after_hours: bool = False):
    """Get a passport-backed EOD prediction without request-time inference."""
    return await _predict_close_impl(symbol, allow_after_hours=allow_after_hours)


@router.get(
    "/ai/predict/{symbol}",
    summary="Get AI-compatible prediction payload",
    description=(
        "Compatibility alias used by the Streamlit dashboard. Retained cross-runtime "
        "state is returned only as an unavailable, non-numeric projection."
    ),
)
async def predict_ai_compatible(symbol: str, allow_after_hours: bool = False):
    """Return the richer prediction payload expected by legacy frontend helpers."""
    try:
        return await _predict_close_impl(symbol, allow_after_hours=allow_after_hours, raw=True)
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        prediction_authority = build_prediction_authority(
            prediction_mode="backend_lifecycle",
            forecast_state="UNAVAILABLE",
            numeric_available=False,
        )
        return {
            "symbol": normalize_symbol(symbol),
            "current_price": None,
            "predicted_close": None,
            "confidence": 0.0,
            "inference_time_ms": 0.0,
            "timestamp": None,
            "generated_at_utc": _utc_iso(),
            "model_type": None,
            "provider": None,
            "source_label": "No lifecycle-published forecast",
            "is_fallback": False,
            "usable": False,
            "state_status": "unavailable",
            "forecast_id": None,
            "forecast_state": "UNAVAILABLE",
            "decision_grade": False,
            "prediction_mode": prediction_authority["prediction_mode"],
            "is_estimate": False,
            "prediction_authority": prediction_authority,
            "signals": [],
            "feature_snapshot": {},
            "live_inference_feature_schema_version": GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
            "live_inference_features": dict(_EMPTY_LIVE_INFERENCE_FEATURES),
            "pin_payload": None,
        }


@router.get(
    "/api/ai/calibration-report",
    summary="Get AI calibration report (legacy alias)",
    description="Legacy alias for /ai/calibration-report.",
    deprecated=True,
)
@router.get(
    "/ai/calibration-report",
    summary="Get AI calibration report",
    description="Returns calibration model metrics generated from historical data.",
)
async def get_ai_calibration_report():
    """Return calibration model metrics generated from historical Polygon-era data."""
    return load_calibration()
