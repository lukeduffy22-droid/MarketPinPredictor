"""Shared helper utilities for backend API routers."""

from __future__ import annotations

import re
import math
from datetime import datetime, timezone
from typing import Any, Mapping

from backend.config import LIVE_DATA_STALE_AFTER_SECONDS, LIVE_DATA_WARN_AFTER_SECONDS
from backend.historical_context import get_historical_context, get_market_history
from backend.prediction_authority import build_prediction_authority
from backend.streamer import get_streamer
from backend.workstation import (
    payload_age_seconds as workstation_payload_age_seconds,
    payload_has_fallback_provenance,
    sanitize_workstation_state_pin_payload,
    unavailable_workstation_state,
)
from app.utils.market_time import market_is_closed


ZERO_GAMMA_DISPLAY_LABEL = "First strike-bucket GEX sign crossing"
ZERO_GAMMA_SEMANTICS_VERSION = "strike-bucket-first-crossing-v1"
SUBSCRIPTION_EPOCH_PATTERN = re.compile(r"[0-9a-f]{64}")


def _canonical_subscription_epoch(value: object) -> str | None:
    raw = str(value or "").strip()
    return raw if SUBSCRIPTION_EPOCH_PATTERN.fullmatch(raw) is not None else None


def _positive_subscription_generation(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        generation = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return generation if generation > 0 else None


def streamer_runtime_context(streamer: object) -> dict[str, object | None]:
    """Read one side-effect-free process/generation/handoff identity."""

    getter = getattr(streamer, "get_subscription_context", None)
    try:
        raw = dict(getter()) if callable(getter) else {
            "subscription_epoch_id": getattr(
                streamer, "subscription_epoch_id", None
            ),
            "subscription_generation": getattr(
                streamer, "active_generation", None
            ),
            "handoff_status": getattr(streamer, "handoff_status", None),
        }
    except Exception:
        raw = {}
    return {
        "subscription_epoch_id": raw.get("subscription_epoch_id"),
        "subscription_generation": raw.get("subscription_generation"),
        "handoff_status": str(raw.get("handoff_status") or "unknown")
        .strip()
        .lower(),
    }


def _runtime_binding_against_context(
    payload: Mapping[str, Any] | None,
    context: Mapping[str, object | None],
) -> dict[str, Any]:
    active_epoch = _canonical_subscription_epoch(
        context.get("subscription_epoch_id")
    )
    payload_epoch = _canonical_subscription_epoch(
        payload.get("subscription_epoch_id") if payload else None
    )
    active_generation = _positive_subscription_generation(
        context.get("subscription_generation")
    )
    payload_generation = _positive_subscription_generation(
        payload.get("subscription_generation") if payload else None
    )
    handoff_status = str(context.get("handoff_status") or "unknown").lower()
    reasons: list[str] = []
    if handoff_status != "active":
        reasons.append(f"ACTIVE_RUNTIME_HANDOFF_NOT_ACTIVE:{handoff_status}")
    if active_epoch is None:
        reasons.append("ACTIVE_SUBSCRIPTION_EPOCH_INVALID")
    if payload_epoch is None:
        reasons.append("PAYLOAD_SUBSCRIPTION_EPOCH_INVALID")
    elif active_epoch is not None and payload_epoch != active_epoch:
        reasons.append("SUBSCRIPTION_EPOCH_MISMATCH")
    if active_generation is None:
        reasons.append("ACTIVE_SUBSCRIPTION_GENERATION_INVALID")
    if payload_generation is None:
        reasons.append("PAYLOAD_SUBSCRIPTION_GENERATION_INVALID")
    elif (
        active_generation is not None
        and payload_generation != active_generation
    ):
        reasons.append("SUBSCRIPTION_GENERATION_MISMATCH")
    return {
        "eligible": not reasons,
        "payload_present": payload is not None,
        "failure_reasons": reasons,
        "active_subscription_epoch_id": active_epoch,
        "payload_subscription_epoch_id": payload_epoch,
        "active_subscription_generation": active_generation,
        "payload_subscription_generation": payload_generation,
        "handoff_status": handoff_status,
    }


def current_runtime_binding(
    streamer: object,
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Require one payload to match a stable active runtime identity exactly."""

    before = streamer_runtime_context(streamer)
    binding = _runtime_binding_against_context(payload, before)
    after = streamer_runtime_context(streamer)
    if after != before:
        reasons = list(binding["failure_reasons"])
        reasons.append("ACTIVE_RUNTIME_CONTEXT_CHANGED")
        binding.update(eligible=False, failure_reasons=reasons)
    return binding


def current_buffered_payload(
    streamer: object,
    symbol: str,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Read one raw buffer value without crossing a runtime identity change."""

    before = streamer_runtime_context(streamer)
    latest = streamer.get_latest_data(symbol, n=1)
    payload = latest[0].copy() if latest else None
    binding = _runtime_binding_against_context(payload, before)
    after = streamer_runtime_context(streamer)
    if after != before:
        reasons = list(binding["failure_reasons"])
        reasons.append("ACTIVE_RUNTIME_CONTEXT_CHANGED")
        binding.update(eligible=False, failure_reasons=reasons)
    return (payload if binding["eligible"] else None), binding


def _explicit_historical_workstation_context(state: Mapping[str, Any]) -> bool:
    payload = state.get("pin_payload")
    health = state.get("health")
    return bool(
        str(state.get("status") or "").lower() == "closed_context"
        and isinstance(payload, Mapping)
        and payload_has_fallback_provenance(payload)
        and isinstance(health, Mapping)
        and health.get("usable_for_prediction") is False
    )


def runtime_bound_workstation_state(
    streamer: object,
    state: Mapping[str, Any] | None,
    *,
    symbol: str,
) -> dict[str, Any]:
    """Project retained workstation state as current only under exact runtime binding."""

    normalized = normalize_symbol(symbol)
    if state is None:
        return unavailable_workstation_state(normalized)
    projected = sanitize_workstation_state_pin_payload(state)
    if _explicit_historical_workstation_context(projected):
        return projected

    runtime_before = streamer_runtime_context(streamer)
    binding = _runtime_binding_against_context(projected, runtime_before)
    payload = projected.get("pin_payload")
    nested_candidates: list[tuple[str, Mapping[str, Any]]] = []
    if isinstance(payload, Mapping):
        nested_candidates.append(("pin_payload", payload))
    prediction = projected.get("prediction")
    if isinstance(prediction, Mapping):
        if any(
            prediction.get(key) is not None
            for key in ("subscription_epoch_id", "subscription_generation")
        ):
            nested_candidates.append(("prediction", prediction))
        prediction_pin = prediction.get("pin_payload")
        if isinstance(prediction_pin, Mapping):
            nested_candidates.append(("prediction.pin_payload", prediction_pin))
        for key in ("provenance", "passport_provenance"):
            nested_identity = prediction.get(key)
            if isinstance(nested_identity, Mapping) and any(
                nested_identity.get(field) is not None
                for field in ("subscription_epoch_id", "subscription_generation")
            ):
                nested_candidates.append(
                    (f"prediction.{key}", nested_identity)
                )

    binding_reasons = list(binding["failure_reasons"])
    for label, candidate in nested_candidates:
        nested_binding = _runtime_binding_against_context(
            candidate, runtime_before
        )
        binding_reasons.extend(
            f"{label}:{reason}"
            for reason in nested_binding["failure_reasons"]
            if not reason.startswith("ACTIVE_")
        )
    runtime_after = streamer_runtime_context(streamer)
    if runtime_after != runtime_before:
        binding_reasons.append("ACTIVE_RUNTIME_CONTEXT_CHANGED")
    binding.update(
        eligible=not binding_reasons,
        failure_reasons=list(dict.fromkeys(binding_reasons)),
    )
    age = (
        workstation_payload_age_seconds(payload)
        if isinstance(payload, Mapping)
        else None
    )
    if binding["eligible"] and (
        str(projected.get("status") or "").lower() != "live"
        or (age is not None and age <= LIVE_DATA_STALE_AFTER_SECONDS)
    ):
        return projected

    unavailable = unavailable_workstation_state(normalized)
    for key in ("sequence", "state_revision", "event_id"):
        if projected.get(key) is not None:
            unavailable[key] = projected[key]
    reasons = list(binding["failure_reasons"])
    if binding["eligible"] and (
        age is None or age > LIVE_DATA_STALE_AFTER_SECONDS
    ):
        reasons.append("LIVE_DATA_STALE")
    unavailable["health"]["validation_failure_reasons"] = list(
        dict.fromkeys(reasons or ["ACTIVE_RUNTIME_BINDING_UNAVAILABLE"])
    )
    unavailable["health"]["runtime_binding_applied"] = True
    unavailable["health"]["active_handoff_status"] = binding[
        "handoff_status"
    ]
    return unavailable


def runtime_bound_workstation_event(
    streamer: object,
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed each retained state carried by a workstation API event."""

    projected = dict(event)
    states = event.get("states")
    projected["states"] = [
        runtime_bound_workstation_state(
            streamer,
            state,
            symbol=str(state.get("symbol") or ""),
        )
        for state in states
        if isinstance(state, Mapping)
    ] if isinstance(states, list) else []
    return projected


def with_gex_level_semantics(payload: dict[str, Any]) -> dict[str, Any]:
    """Attach honest API-facing semantics while preserving legacy field names."""
    decorated = payload.copy()
    crossing = (
        decorated.get("zero_gamma")
        if decorated.get("zero_gamma") is not None
        else decorated.get("zero_gamma_level")
    )
    decorated["zero_gamma"] = crossing
    decorated["zero_gamma_level"] = crossing
    source_method = decorated.get("zero_gamma_method")
    decorated["zero_gamma_source_method"] = source_method
    decorated["zero_gamma_method"] = "first_strike_bucket_sign_crossing_linear_interpolation"
    decorated["zero_gamma_label"] = ZERO_GAMMA_DISPLAY_LABEL
    decorated["zero_gamma_semantics_version"] = ZERO_GAMMA_SEMANTICS_VERSION
    decorated["zero_gamma_is_portfolio_spot_sweep"] = False
    decorated["zero_gamma_semantics"] = {
        "label": ZERO_GAMMA_DISPLAY_LABEL,
        "version": ZERO_GAMMA_SEMANTICS_VERSION,
        "legacy_compatibility_fields": ["zero_gamma", "zero_gamma_level"],
        "calculation": (
            "first strike-ordered adjacent net-GEX sign crossing, linearly interpolated"
        ),
        "not_a_portfolio_spot_sweep": True,
        "not_a_close_target": True,
    }
    fallback = payload_has_fallback_provenance(decorated)
    prediction_mode = (
        "closed_market_historical_context_only"
        if fallback
        else "backend_lifecycle"
    )
    structural_source_gate_passed = _payload_validation_gate_passed(decorated)
    age_seconds = payload_data_age_seconds(decorated)
    freshness_passed = bool(
        age_seconds is not None and age_seconds <= LIVE_DATA_STALE_AFTER_SECONDS
    )
    source_gate_passed = bool(structural_source_gate_passed and freshness_passed)
    prediction_shape_eligible = _payload_prediction_shape_is_eligible(decorated)
    structurally_eligible = _payload_prediction_structure_is_eligible(decorated)
    usable_numeric_forecast = bool(structurally_eligible and freshness_passed)
    forecast_state = (
        "ABSTAIN"
        if decorated.get("validation_is_valid") is False
        or decorated.get("gamma_excluded_from_model") is True
        or prediction_mode == "closed_market_historical_context_only"
        else "STALE"
        if prediction_shape_eligible
        and age_seconds is not None
        and age_seconds > LIVE_DATA_STALE_AFTER_SECONDS
        else "RESEARCH_ONLY"
        if usable_numeric_forecast
        else "UNAVAILABLE"
    )
    prediction_authority = build_prediction_authority(
        prediction_mode=prediction_mode,
        forecast_state=forecast_state,
        numeric_available=usable_numeric_forecast,
        decision_grade=False,
        promotion_verified=False,
    )
    decorated["prediction_mode"] = prediction_mode
    decorated["is_fallback"] = fallback
    decorated["historical_context_only"] = bool(
        decorated.get("historical_context_only") or fallback
    )
    analytical_levels_present = _has_analytical_level_evidence(decorated)
    decorated["analytical_levels_available"] = bool(
        decorated.get("analytical_levels_available", True)
        and source_gate_passed
        and analytical_levels_present
    )
    decorated["usable_for_prediction"] = usable_numeric_forecast
    decorated["diagnostic_only"] = bool(
        _has_diagnostic_evidence(decorated)
        and (not source_gate_passed or not usable_numeric_forecast)
    )
    decorated["forecast_state"] = forecast_state
    decorated["decision_grade"] = False
    decorated["is_estimate"] = bool(prediction_authority["is_estimate"])
    decorated["tcbbo_promoted"] = False
    decorated["prediction_authority"] = prediction_authority
    return decorated


def _finite_api_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_api_number(value: Any) -> float | None:
    number = _finite_api_number(value)
    return number if number is not None and number > 0.0 else None


def _has_analytical_level_evidence(payload: dict[str, Any]) -> bool:
    if any(
        _positive_api_number(payload.get(key)) is not None
        for key in ("gamma_pin", "max_pain", "zero_gamma")
    ):
        return True
    contracts = _finite_api_number(
        payload.get("contracts_count", payload.get("contracts"))
    )
    return bool(
        contracts is not None
        and contracts > 0.0
        and any(
            _finite_api_number(payload.get(key)) is not None
            for key in ("gross_gex", "net_gex")
        )
    )


def _has_diagnostic_evidence(payload: dict[str, Any]) -> bool:
    if any(
        _positive_api_number(payload.get(key)) is not None
        for key in (
            "price",
            "spot_last",
            "gamma_pin",
            "max_pain",
            "zero_gamma",
        )
    ):
        return True
    if _has_analytical_level_evidence(payload):
        return True
    return any(
        (_finite_api_number(payload.get(key)) or 0.0) > 0.0
        for key in (
            "fresh_quote_count",
            "paired_quote_count",
            "expected_primary_pair_count",
            "paired_primary_pair_count",
        )
    )


def normalize_symbol(symbol: str) -> str:
    """Normalize UI labels like 'S&P 500 (SPX)' to backend symbols."""
    if not symbol:
        return ""
    normalized = symbol.upper().strip()
    label_aliases = {
        "VIX (VOLATILITY)": "VIX",
        "VOLATILITY": "VIX",
    }
    if normalized in label_aliases:
        return label_aliases[normalized]
    if normalized.startswith("VIX"):
        return "VIX"
    match = re.search(r"\(([A-Z0-9]+)\)", normalized)
    if match:
        return match.group(1)
    return normalized.replace("I:", "")


def streamer_health(streamer) -> dict:
    if hasattr(streamer, "get_health"):
        return streamer.get_health()
    return {
        "provider": "polygon",
        "websocket": "active" if streamer.is_running else "stopped",
        "buffer_health": "healthy" if streamer.get_all_latest() else "warming",
        "messages_received": getattr(streamer, "messages_received", 0),
    }


def _historical_closed_market_fallback(symbol: str) -> dict | None:
    """Return historical context without fabricating live option-derived levels."""
    normalized = normalize_symbol(symbol)
    context = get_historical_context(normalized)
    if not context or not context.get("history_rows"):
        return None
    if context.get("history_is_fresh") is False:
        return None

    market_history = get_market_history()
    if market_history.empty or not {"symbol", "close"}.issubset(market_history.columns):
        return None
    symbol_history = market_history[market_history["symbol"].astype(str).str.upper().eq(normalized)].copy()
    if symbol_history.empty:
        return None

    last_close = float(symbol_history["close"].iloc[-1])
    if not (last_close > 0):
        return None

    momentum = float(context.get("return_5d") or 0.0)
    vix_percentile = context.get("vix_percentile")
    vix_bias = 0.0
    if isinstance(vix_percentile, (int, float)):
        vix_bias = float(vix_percentile - 0.5) * 0.002

    predicted_close = last_close * (1.0 + max(min(momentum * 0.35, 0.012), -0.012))
    predicted_close *= 1.0 - max(min(vix_bias, 0.0015), -0.0015)

    return with_gex_level_semantics({
        "symbol": normalized,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "price": last_close,
        "spot_price": last_close,
        "current_price": last_close,
        "predicted_close": predicted_close,
        "likely_close": predicted_close,
        "gamma_pin": None,
        "max_pain": None,
        "zero_gamma": None,
        "provider": "historical-fallback",
        "source_label": "Closed-market historical context only",
        "is_fallback": True,
        "display_mode": "historical_context_only",
        "prediction_mode": "closed_market_historical_context_only",
        "historical_context_only": True,
        "analytical_levels_available": False,
        "usable_for_prediction": False,
        "confidence": None,
        "validation_is_valid": False,
        "validation_failure_reasons": [
            "CLOSED_MARKET_HISTORICAL_CONTEXT_ONLY: live OPRA analytical levels are unavailable"
        ],
        "expiration_profiles": [],
        "subscription_expirations": [],
        "primary_expiration": None,
        "signals": [
            {"name": "historical_close_anchor", "value": last_close, "weight": 0.4, "distance_points": last_close - last_close},
            {"name": "historical_5d_momentum", "value": momentum, "weight": 0.2, "distance_points": predicted_close - last_close},
        ],
        "historical_context": context,
    })


def pin_payload_for_symbol(symbol: str) -> dict | None:
    streamer = get_streamer()
    normalized = normalize_symbol(symbol)
    pin_fetcher = getattr(streamer, "get_latest_pin", None)
    if callable(pin_fetcher):
        pin = pin_fetcher(normalized)
        if isinstance(pin, dict) and pin:
            binding = current_runtime_binding(streamer, pin)
            if not binding["eligible"]:
                return None
            payload = pin.copy()
            timestamp = payload.get("timestamp")
            if timestamp is not None and hasattr(timestamp, "isoformat"):
                payload["timestamp"] = timestamp.isoformat()
            return with_gex_level_semantics(payload)
    payload, binding = current_buffered_payload(streamer, normalized)
    if payload is not None:
        timestamp = payload.get("timestamp")
        if hasattr(timestamp, "isoformat"):
            payload["timestamp"] = timestamp.isoformat()
        if not payload_is_usable_prediction(payload):
            # Preserve the observed invalid/stale candidate and its exact
            # reasons. Closed-market history is only a fallback when no live
            # lifecycle row exists at all; it must not replace failed 0DTE
            # evidence with a more plausible-looking number.
            return with_gex_level_semantics(payload)
        return with_gex_level_semantics(payload)
    if binding.get("payload_present"):
        return None
    return _historical_closed_market_fallback(normalized) if market_is_closed() else None


def payload_is_usable_prediction(payload: dict | None) -> bool:
    if not _payload_prediction_structure_is_eligible(payload):
        return False
    age_seconds = payload_data_age_seconds(payload)
    return bool(
        age_seconds is not None and age_seconds <= LIVE_DATA_STALE_AFTER_SECONDS
    )


def _payload_validation_gate_passed(payload: dict | None) -> bool:
    if not payload:
        return False
    if payload_has_fallback_provenance(payload):
        return False
    if payload.get("validation_is_valid") is not True:
        return False
    if payload.get("gamma_excluded_from_model") is not False:
        return False
    return True


def _payload_prediction_shape_is_eligible(payload: dict | None) -> bool:
    """Return whether a payload contains a validated numeric close forecast.

    A gamma pin is an analytical level, not a forecast of the closing price, so
    it cannot satisfy this contract by itself.
    """

    if not _payload_validation_gate_passed(payload):
        return False
    assert payload is not None
    if _positive_api_number(payload.get("price")) is None:
        return False
    return any(
        _positive_api_number(payload.get(key)) is not None
        for key in ("likely_close", "predicted_close")
    )


def _payload_prediction_structure_is_eligible(payload: dict | None) -> bool:
    if not _payload_prediction_shape_is_eligible(payload):
        return False
    assert payload is not None
    if payload.get("usable_for_prediction") is False:
        return False
    return True


def parse_payload_timestamp(payload: dict | None) -> datetime | None:
    if not payload:
        return None
    value = (
        payload.get("timestamp")
        or payload.get("timestamp_utc")
        or payload.get("generated_at_utc")
    )
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def payload_data_age_seconds(payload: dict | None) -> float | None:
    if not payload:
        return None
    return workstation_payload_age_seconds(payload)


def payload_is_stale(payload: dict | None, stale_after_seconds: float = LIVE_DATA_STALE_AFTER_SECONDS) -> bool:
    age = payload_data_age_seconds(payload)
    if age is None:
        return True
    return age > stale_after_seconds


def payload_health(payload: dict | None, stale_after_seconds: float = LIVE_DATA_STALE_AFTER_SECONDS) -> dict:
    age = payload_data_age_seconds(payload)
    validation_is_valid = bool(
        payload and payload.get("validation_is_valid") is True
    )
    prediction_shape_eligible = _payload_prediction_shape_is_eligible(payload)
    structurally_eligible = _payload_prediction_structure_is_eligible(payload)
    usable_for_prediction = bool(
        structurally_eligible
        and age is not None
        and age <= stale_after_seconds
    )
    validation_failure_reasons = (
        list(payload.get("validation_failure_reasons") or []) if payload else []
    )
    if age is None:
        return {
            "status": "unknown",
            "data_age_seconds": None,
            "stale_after_seconds": stale_after_seconds,
            "warn_after_seconds": LIVE_DATA_WARN_AFTER_SECONDS,
            "is_stale": True,
            "validation_is_valid": validation_is_valid,
            "usable_for_prediction": False,
            "validation_failure_reasons": validation_failure_reasons,
        }
    status = (
        "invalid"
        if not prediction_shape_eligible
        else "stale"
        if age > stale_after_seconds
        else "invalid"
        if not structurally_eligible
        else "warning"
        if age > LIVE_DATA_WARN_AFTER_SECONDS
        else "fresh"
    )
    return {
        "status": status,
        "data_age_seconds": age,
        "stale_after_seconds": stale_after_seconds,
        "warn_after_seconds": LIVE_DATA_WARN_AFTER_SECONDS,
        "is_stale": age > stale_after_seconds,
        "validation_is_valid": validation_is_valid,
        "usable_for_prediction": usable_for_prediction,
        "validation_failure_reasons": validation_failure_reasons,
    }


def _to_float(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def build_inference_features(
    payload: dict | None,
    current_price: float | None = None,
) -> dict[str, float | None]:
    """Compatibility wrapper for the versioned live inference feature builder."""
    from backend.inference import build_live_inference_features

    return build_live_inference_features(payload, current_price=current_price)
