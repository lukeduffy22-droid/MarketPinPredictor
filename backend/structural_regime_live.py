"""Adapt one guarded observation; never fetch providers or infer missing levels."""
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any, Mapping

from backend.config import LIVE_DATA_STALE_AFTER_SECONDS
from backend.forecast_calendar import ET, session_bounds
from backend.structural_regime import DEFAULT_CONFIG, GEX_UNIT, MODEL_VERSION, Snapshot, finite_number, predict_close
from backend.workstation import payload_has_fallback_provenance

FIELD = "structural_regime_prediction"


def parse_utc(value: Any) -> datetime | None:
    try:
        parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else None


def observation_time(payload: Mapping) -> datetime | None:
    for key in ("latest_ts_recv_utc", "observation_index_utc", "quote_timestamp_utc", "timestamp", "timestamp_utc"):
        if payload.get(key) is not None:
            return parse_utc(payload[key])
    return None


def abstention(*reasons: str) -> dict:
    return {
        "model_version": MODEL_VERSION, "status": "ABSTAIN", "reasons": list(reasons),
        "research_only": True, "decision_grade": False, "capture_state": "RESEARCH_ONLY",
        "confidence_calibrated": False, "confidence_kind": "uncalibrated_heuristic_score",
        "predicted_close": None, "band_low": None, "band_high": None, "confidence": None,
        "regime": None, "inputs": None,
    }


def build_structural_prediction(
    payload: Mapping, model: Mapping, *, vix_payload: Mapping | None = None,
    now_utc: datetime | None = None,
) -> dict:
    now = now_utc or datetime.now(timezone.utc)
    observed = observation_time(payload)
    symbol = str(payload.get("symbol") or "").upper()
    reasons = []
    if symbol not in {"SPX", "NDX", "RUT"}:
        reasons.append("UNSUPPORTED_CASH_INDEX")
    if payload.get("validation_is_valid") is not True or payload.get("gamma_excluded_from_model") is not False:
        reasons.append("SOURCE_VALIDATION_NOT_CONFIRMED")
    if payload_has_fallback_provenance(payload):
        reasons.append("NON_PRODUCTION_FALLBACK")
    if observed is None or not 0 <= (now - observed).total_seconds() <= LIVE_DATA_STALE_AFTER_SECONDS:
        reasons.append("SOURCE_TIMESTAMP_STALE_MISSING_OR_FUTURE")
    epoch, generation = payload.get("subscription_epoch_id"), payload.get("subscription_generation")
    if not re.fullmatch(r"[0-9a-f]{64}", str(epoch or "")) or type(generation) is not int or generation <= 0:
        reasons.append("SOURCE_RUNTIME_IDENTITY_MISSING")
    if not isinstance(payload.get("calculation_id"), str) or not payload["calculation_id"].strip():
        reasons.append("COMMITTED_CALCULATION_ID_MISSING")
    if model.get("usable") is not True:
        reasons.append("MODEL_LEAN_UNAVAILABLE")
    if observed is None:
        return abstention(*reasons)
    day = observed.astimezone(ET).date()
    try:
        opened, closed = session_bounds(day)
    except ValueError as exc:
        return abstention(*reasons, str(exc))
    if not opened <= observed <= now < closed:
        reasons.append("OUTSIDE_CASH_SESSION")
    if payload.get("primary_expiration") != day.isoformat() or payload.get("same_day_authority") is not True:
        reasons.append("SAME_DAY_EXPIRATION_NOT_CONFIRMED")
    if payload.get("gex_unit", GEX_UNIT) != GEX_UNIT:
        reasons.append("GEX_UNIT_UNSUPPORTED")
    if reasons:
        return abstention(*reasons)
    vix = vix_payload if isinstance(vix_payload, Mapping) else {}
    vix_time = observation_time(vix)
    try:
        finite_number(vix.get("price"), "vix_price", positive=True)
        vix_price_valid = True
    except ValueError:
        vix_price_valid = False
    vol_valid = bool(
        vix_price_valid and vix.get("symbol") == "VIX" and vix.get("validation_is_valid") is True
        and vix.get("gamma_excluded_from_model") is False
        and not payload_has_fallback_provenance(vix)
        and vix.get("subscription_epoch_id") == epoch
        and vix.get("subscription_generation") == generation
        and vix_time is not None and opened <= vix_time <= observed
        and (now - vix_time).total_seconds() <= LIVE_DATA_STALE_AFTER_SECONDS
    )
    elapsed = (observed - opened).total_seconds() / 60
    remaining = (closed - observed).total_seconds() / 60
    session = "OPENING" if elapsed < 60 else "CLOSING" if remaining <= 60 else "MIDDAY"
    try:
        snapshot = Snapshot(
            spot=payload.get("price", payload.get("spot_price")),
            gamma_pin=payload.get("gamma_pin"), max_pain=payload.get("max_pain"),
            zero_gamma=payload.get("zero_gamma"), net_gex=payload.get("net_gex"),
            model_lean=model.get("predicted_close"),
            time_of_day=(observed - opened) / (closed - opened),
            session_state=session, vol_valid=vol_valid, capture_state="RESEARCH_ONLY",
        )
        result = predict_close(snapshot).to_dict()
    except ValueError as exc:
        return abstention(str(exc))
    result.update(
        model_version=MODEL_VERSION, status="AVAILABLE", reasons=[], symbol=symbol,
        research_only=True, decision_grade=False, capture_state="RESEARCH_ONLY",
        confidence_calibrated=False, confidence_kind="uncalibrated_heuristic_score",
        confidence_label="high" if result["confidence"] >= 0.75 else "medium" if result["confidence"] >= 0.5 else "low",
        band_kind="structural_reference_band_not_prediction_interval",
        as_of_utc=observed.isoformat(), trading_date=day.isoformat(), target_close_utc=closed.isoformat(),
        subscription_epoch_id=epoch, subscription_generation=generation,
        calculation_id=payload["calculation_id"], model_lean_version=model.get("model_version"),
        source_binding="independent_committed_calculation.v1",
        zero_gamma_semantics="strike_curve_crossing_not_portfolio_spot_flip",
        gex_unit=GEX_UNIT, configuration=asdict(DEFAULT_CONFIG),
    )
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    result["prediction_id"] = hashlib.sha256(encoded).hexdigest()
    return result
