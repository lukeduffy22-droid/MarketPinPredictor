"""Lifecycle-owned materialized state for read-only workstation clients.

The workstation API reads this in-memory projection. Only lifecycle code calls
``publish``; reads return defensive copies and never run inference or write to
the database.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import threading
from collections import deque
from datetime import datetime, timezone
from typing import Any, Mapping

from backend.config import LIVE_DATA_STALE_AFTER_SECONDS, MARKET_DATA_PROVIDER
from backend.prediction_authority import build_prediction_authority

STATE_CONTRACT_VERSION = "workstation-state.v1"
EVENT_CONTRACT_VERSION = "workstation-events.v1"
_PASSPORT_PUBLIC_STATES = {
    "VALID",
    "RESEARCH",
    "RESEARCH_ONLY",
    "ABSTAIN",
    "STALE",
    "UNAVAILABLE",
}
PREDICTIVE_PIN_PAYLOAD_FIELDS = frozenset(
    {
        "predicted_close",
        "likely_close",
        "expected_close",
        "close_range_low",
        "close_range_high",
    }
)
_NONLIVE_PREDICTION_MODES = frozenset(
    {
        "backend_lifecycle",
        "backend_periodic",
        "tcbbo_promoted",
        "closed_market_historical_context_only",
    }
)


def _utc_iso(value: datetime | None = None) -> str:
    parsed = value or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _source_datetime(payload: Mapping[str, Any]) -> datetime | None:
    for key in (
        "latest_ts_recv_utc",
        "observation_index_utc",
        "quote_timestamp_utc",
        "timestamp",
        "timestamp_utc",
        "generated_at_utc",
    ):
        if payload.get(key) is not None:
            return _parse_timestamp(payload[key])
    return None


def source_timestamp(payload: Mapping[str, Any]) -> str | None:
    parsed = _source_datetime(payload)
    return _utc_iso(parsed) if parsed is not None else None


def payload_has_fallback_provenance(payload: Mapping[str, Any] | None) -> bool:
    """Return whether any authoritative source layer marks a payload fallback.

    Databento calculation payloads carry definition-universe and open-interest
    provenance as nested objects.  Consumers must not rely only on the legacy
    top-level fallback flag or a prior-session cache can be mistaken for a
    production-current observation.
    """

    if not isinstance(payload, Mapping):
        return False
    provider = str(payload.get("provider") or "").lower().strip()
    universe_provenance = payload.get("universe_provenance")
    universe_provenance = (
        universe_provenance if isinstance(universe_provenance, Mapping) else {}
    )
    oi_provenance = payload.get("oi_analytics_provenance")
    oi_provenance = oi_provenance if isinstance(oi_provenance, Mapping) else {}
    return bool(
        payload.get("is_fallback")
        or payload.get("historical_context_only")
        or provider == "historical-fallback"
        or universe_provenance.get("is_fallback")
        or oi_provenance.get("is_fallback")
    )


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _json_safe(value: Any) -> Any:
    """Return a deterministic JSON-safe copy for API and SSE serialization."""

    if isinstance(value, datetime):
        return _utc_iso(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def sanitize_pin_payload(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return raw market evidence without forecast-like compatibility numerics."""

    if not isinstance(payload, Mapping):
        return None
    sanitized = copy.deepcopy(dict(payload))
    for field in PREDICTIVE_PIN_PAYLOAD_FIELDS:
        sanitized.pop(field, None)
    return sanitized


def sanitize_workstation_state_pin_payload(
    state: Mapping[str, Any],
) -> dict[str, Any]:
    """Remove raw pin forecasts and fail closed any non-governed prediction."""

    sanitized = copy.deepcopy(dict(state))
    if "pin_payload" in sanitized:
        sanitized["pin_payload"] = sanitize_pin_payload(sanitized.get("pin_payload"))
    prediction = sanitized.get("prediction")
    if isinstance(prediction, Mapping):
        prediction_copy = dict(prediction)
        if "pin_payload" in prediction_copy:
            prediction_copy["pin_payload"] = sanitize_pin_payload(
                prediction_copy.get("pin_payload")
            )
        if _state_has_governed_live_prediction(sanitized, prediction_copy):
            sanitized["prediction"] = prediction_copy
        else:
            status = str(sanitized.get("status") or "unavailable").lower()
            was_inconsistent_live = status == "live"
            if status == "live":
                status = "invalid"
                sanitized["status"] = status
            retained_forecast_state = str(
                sanitized.get("forecast_state") or ""
            ).upper()
            forecast_state = (
                "ABSTAIN"
                if was_inconsistent_live
                else retained_forecast_state
                if retained_forecast_state in _PASSPORT_PUBLIC_STATES
                else {
                    "stale": "STALE",
                    "invalid": "ABSTAIN",
                    "closed_context": "ABSTAIN",
                }.get(status, "UNAVAILABLE")
            )
            raw_authority = sanitized.get("prediction_authority")
            raw_mode = (
                raw_authority.get("prediction_mode")
                if isinstance(raw_authority, Mapping)
                else None
            ) or prediction_copy.get("prediction_mode")
            mode = (
                str(raw_mode)
                if str(raw_mode or "") in _NONLIVE_PREDICTION_MODES
                else "backend_lifecycle"
            )
            safe_authority = build_prediction_authority(
                prediction_mode=mode,
                forecast_state=forecast_state,
                numeric_available=False,
                decision_grade=False,
                promotion_verified=False,
            )
            sanitized["forecast_state"] = forecast_state
            sanitized["decision_grade"] = False
            sanitized["prediction_authority"] = safe_authority
            health = sanitized.get("health")
            if isinstance(health, Mapping):
                health_copy = copy.deepcopy(dict(health))
                health_copy.update(
                    usable_for_prediction=False,
                    decision_grade=False,
                    forecast_state=forecast_state,
                    prediction_authority_state=safe_authority["authority_state"],
                )
                sanitized["health"] = health_copy
            sanitized["prediction"] = _nonlive_prediction_projection(
                prediction_copy, sanitized
            )
    return sanitized


def _is_sha256(value: Any) -> bool:
    candidate = str(value or "").strip()
    return bool(
        len(candidate) == 64
        and all(character in "0123456789abcdef" for character in candidate)
    )


def _state_has_governed_live_prediction(
    state: Mapping[str, Any], prediction: Mapping[str, Any]
) -> bool:
    health = state.get("health")
    authority = state.get("prediction_authority")
    return bool(
        str(state.get("status") or "").lower() == "live"
        and str(state.get("forecast_state") or "").upper()
        in {"VALID", "RESEARCH_ONLY"}
        and _is_sha256(state.get("forecast_id"))
        and prediction.get("usable") is True
        and isinstance(health, Mapping)
        and health.get("usable_for_prediction") is True
        and isinstance(authority, Mapping)
        and authority.get("is_estimate") is True
    )


def _nonlive_prediction_projection(
    prediction: Mapping[str, Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    """Keep bounded diagnostics while discarding every forecast-bearing field."""

    authority = state.get("prediction_authority")
    authority_copy = _json_safe(dict(authority)) if isinstance(authority, Mapping) else {}
    pin_payload = state.get("pin_payload")
    pin_payload = pin_payload if isinstance(pin_payload, Mapping) else {}
    return {
        "symbol": str(state.get("symbol") or prediction.get("symbol") or "").upper(),
        "timestamp": state.get("generated_at_utc"),
        "current_price": _finite(state.get("current_price")),
        "usable": False,
        "decision_grade": False,
        "forecast_state": state.get("forecast_state"),
        "prediction_mode": authority_copy.get("prediction_mode") or "backend_lifecycle",
        "is_estimate": False,
        "prediction_authority": authority_copy,
        "predicted_close": None,
        "prediction_lower": None,
        "prediction_upper": None,
        "interval_target_coverage": None,
        "confidence": None,
        "signals": [],
        "feature_snapshot": {},
        "gamma_pin": _finite(pin_payload.get("gamma_pin")),
        "zero_gamma": _finite(pin_payload.get("zero_gamma")),
        "max_pain": _finite(pin_payload.get("max_pain")),
        "prediction_snapshot_id": state.get("prediction_snapshot_id"),
    }


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _json_safe(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def passport_contract_is_complete(passport: Mapping[str, Any] | None) -> bool:
    """Require the persisted passport envelope before exposing a forecast.

    A caller-supplied forecast id or a partial adapter result is not evidence
    that an immutable passport was actually written. The lifecycle issuer
    returns all of these sections plus the database record hash.
    """

    if not isinstance(passport, Mapping):
        return False
    state = str(passport.get("state") or "").upper()
    if state not in _PASSPORT_PUBLIC_STATES:
        return False
    forecast_id = str(passport.get("forecast_id") or "").strip()
    if not _is_sha256(forecast_id):
        return False
    record_sha256 = str(passport.get("record_sha256") or "").strip()
    if not _is_sha256(record_sha256):
        return False
    if not all(
        isinstance(passport.get(key), Mapping)
        for key in ("origin", "provenance", "model", "prediction", "quality")
    ):
        return False

    origin = passport["origin"]
    identity = {
        "schema_version": origin.get("schema_version"),
        "origin_kind": origin.get("origin_kind"),
        "origin_key": origin.get("origin_key"),
    }
    if (
        identity["schema_version"] != "prediction-passport-v1"
        or not str(identity["origin_kind"] or "").strip()
        or not str(identity["origin_key"] or "").strip()
        or _canonical_sha256(identity) != forecast_id
    ):
        return False

    immutable_core = {
        key: value
        for key, value in passport.items()
        if key not in {"record_sha256", "created_at_utc", "outcome"}
    }
    return _canonical_sha256(immutable_core) == record_sha256


def payload_age_seconds(payload: Mapping[str, Any]) -> float | None:
    """Return conservative source age, advancing with wall-clock time."""

    ages: list[float] = []
    explicit = payload.get("quote_age_seconds")
    if explicit is None:
        explicit = payload.get("data_age_seconds")
    explicit_value = _finite(explicit)
    if explicit is not None:
        # A negative provider/source age is not "extra fresh" evidence.  It
        # means the observation clock is ahead of this process and freshness
        # cannot be established safely.
        if explicit_value is None or explicit_value < 0:
            return None
        ages.append(explicit_value)

    source = _source_datetime(payload)
    if source is not None:
        source_age = (datetime.now(timezone.utc) - source).total_seconds()
        if source_age < 0:
            return None
        ages.append(source_age)
    return max(ages) if ages else None


def payload_revision_key(payload: Mapping[str, Any]) -> tuple[Any, ...]:
    """Identify one source calculation without depending on object identity."""

    material = {
        "symbol": str(payload.get("symbol") or "").upper(),
        "subscription_epoch_id": payload.get("subscription_epoch_id"),
        "subscription_generation": payload.get("subscription_generation"),
        "source_as_of_utc": source_timestamp(payload),
        "calculation_id": payload.get("calculation_id"),
        "provider": payload.get("provider"),
        "price": payload.get("price", payload.get("spot_price")),
        "likely_close": payload.get("likely_close"),
        "gamma_pin": payload.get("gamma_pin"),
        "zero_gamma": payload.get("zero_gamma"),
        "max_pain": payload.get("max_pain"),
        "validation_is_valid": payload.get("validation_is_valid"),
        "validation_failure_reasons": payload.get("validation_failure_reasons"),
    }
    digest = hashlib.sha256(
        json.dumps(_json_safe(material), sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    return (
        material["symbol"],
        material["subscription_epoch_id"],
        material["subscription_generation"],
        material["source_as_of_utc"],
        material["calculation_id"],
        digest,
    )


def build_workstation_state(
    *,
    symbol: str,
    payload: Mapping[str, Any],
    prediction: Mapping[str, Any] | None,
    feature_snapshot: Mapping[str, Any] | None = None,
    forecast_id: str | None = None,
    passport: Mapping[str, Any] | None = None,
    prediction_snapshot_id: int | None = None,
) -> dict[str, Any]:
    """Build an unpublished state projection from one lifecycle observation."""

    normalized = symbol.upper()
    payload_copy = _json_safe(sanitize_pin_payload(payload) or {})
    provider = str(payload.get("provider") or MARKET_DATA_PROVIDER or "unknown")
    fallback = payload_has_fallback_provenance(payload)
    valid = bool(
        payload.get("validation_is_valid") is True
        and payload.get("gamma_excluded_from_model") is False
    )
    age = payload_age_seconds(payload)
    stale = age is None or age > LIVE_DATA_STALE_AFTER_SECONDS
    current = _finite(
        payload.get("price")
        if payload.get("price") is not None
        else payload.get("spot_price")
    )
    source_prediction = _json_safe(
        dict(prediction) if isinstance(prediction, Mapping) else {}
    )
    prediction_usable = bool(
        isinstance(prediction, Mapping) and prediction.get("usable") is True
    )

    passport_contract_complete = passport_contract_is_complete(passport)
    passport_copy = _json_safe(
        dict(passport) if isinstance(passport, Mapping) else {}
    )
    passport_binding_reasons: list[str] = []
    if passport_contract_complete:
        passport_symbol = str(passport_copy.get("symbol") or "").strip().upper()
        if passport_symbol != normalized:
            passport_binding_reasons.append("PASSPORT_SYMBOL_MISMATCH")
        passport_mode = str(passport_copy.get("prediction_mode") or "").strip()
        expected_mode = str(source_prediction.get("prediction_mode") or "").strip()
        if not passport_mode or (expected_mode and passport_mode != expected_mode):
            passport_binding_reasons.append("PASSPORT_PREDICTION_MODE_MISMATCH")
        origin = passport_copy.get("origin")
        origin_snapshot_id = origin.get("prediction_snapshot_id") if isinstance(origin, Mapping) else None
        if prediction_snapshot_id is not None:
            try:
                snapshot_matches = int(origin_snapshot_id) == int(prediction_snapshot_id)
            except (TypeError, ValueError, OverflowError):
                snapshot_matches = False
            if not snapshot_matches:
                passport_binding_reasons.append("PASSPORT_SNAPSHOT_ID_MISMATCH")
        if passport_binding_reasons:
            passport_contract_complete = False
    passport_state = str(passport_copy.get("state") or "").upper() or None
    if passport_state == "RESEARCH":
        passport_state = "RESEARCH_ONLY"
    if not passport_contract_complete:
        passport_state = None
    # Never let the compatibility ``forecast_id`` argument self-attest that a
    # persisted passport exists. Only a complete issuer result may expose it.
    passport_forecast_id = str(passport_copy.get("forecast_id") or "").strip()
    forecast_id = passport_forecast_id if passport_contract_complete else None
    if prediction_snapshot_id is None:
        origin = passport_copy.get("origin")
        if isinstance(origin, Mapping):
            prediction_snapshot_id = origin.get("prediction_snapshot_id")

    passport_quality = passport_copy.get("quality")
    passport_quality = (
        dict(passport_quality) if isinstance(passport_quality, Mapping) else {}
    )
    passport_prediction = passport_copy.get("prediction")
    passport_prediction = (
        dict(passport_prediction)
        if isinstance(passport_prediction, Mapping)
        else {}
    )
    passport_point_estimate = _finite(passport_prediction.get("point_estimate"))
    reasons = [str(reason) for reason in payload.get("validation_failure_reasons") or []]
    reasons.extend(passport_binding_reasons)
    if prediction is not None:
        reasons.extend(
            str(reason)
            for reason in prediction.get("validation_failure_reasons") or []
        )
    reasons.extend(str(reason) for reason in passport_quality.get("state_reasons") or [])
    if (
        source_timestamp(payload) is None
        and "SOURCE_TIMESTAMP_MISSING_OR_INVALID" not in reasons
    ):
        reasons.append("SOURCE_TIMESTAMP_MISSING_OR_INVALID")
    if stale and "LIVE_DATA_STALE" not in reasons:
        reasons.append("LIVE_DATA_STALE")
    if current is None or current <= 0:
        if "SOURCE_PRICE_MISSING_OR_INVALID" not in reasons:
            reasons.append("SOURCE_PRICE_MISSING_OR_INVALID")
    if prediction is not None and not prediction_usable:
        if "PREDICTION_UNUSABLE" not in reasons:
            reasons.append("PREDICTION_UNUSABLE")
    if prediction is not None and not passport_contract_complete:
        reasons.append("PASSPORT_INCOMPLETE_OR_UNVERIFIED")
    if (
        prediction is not None
        and passport_contract_complete
        and passport_state in {"VALID", "RESEARCH_ONLY"}
        and passport_point_estimate is None
    ):
        reasons.append("PASSPORT_POINT_ESTIMATE_MISSING_OR_INVALID")
    if fallback and "NON_PRODUCTION_FALLBACK" not in reasons:
        reasons.append("NON_PRODUCTION_FALLBACK")

    if fallback:
        status = "closed_context"
    elif passport_state == "UNAVAILABLE" or current is None or current <= 0:
        status = "unavailable"
    elif (
        not valid
        or passport_state == "ABSTAIN"
        or (
            prediction is not None
            and (
                not prediction_usable
                or not passport_contract_complete
                or passport_state not in {"VALID", "RESEARCH_ONLY"}
                or passport_point_estimate is None
            )
        )
    ):
        status = "invalid"
    elif stale or passport_state == "STALE":
        status = "stale"
    elif prediction is None:
        status = "warming"
    else:
        status = "live"

    if passport_state is None:
        if status == "stale":
            passport_state = "STALE"
        elif status in {"invalid", "closed_context"}:
            passport_state = "ABSTAIN"
        elif status == "unavailable":
            passport_state = "UNAVAILABLE"

    requested_decision_grade = bool(
        passport_copy.get("decision_grade")
        and passport_state == "VALID"
        and status == "live"
    )
    prediction_mode = str(
        passport_copy.get("prediction_mode")
        or source_prediction.get("prediction_mode")
        or "backend_lifecycle"
    )
    lifecycle_usable = bool(
        prediction_usable
        and passport_contract_complete
        and status == "live"
        and passport_state in {"VALID", "RESEARCH_ONLY"}
    )
    predicted_close = passport_point_estimate if lifecycle_usable else None
    usable = bool(lifecycle_usable and predicted_close is not None)
    if lifecycle_usable and predicted_close is None:
        reasons.append("PREDICTION_CLOSE_MISSING_OR_INVALID")

    # The generic workstation lifecycle is not proof that the immutable
    # closing-tape promotion ledger was satisfied. That trusted lane is exposed
    # separately by /closing-tape/production-predictions.
    prediction_authority = build_prediction_authority(
        prediction_mode=prediction_mode,
        forecast_state=passport_state,
        numeric_available=usable,
        decision_grade=requested_decision_grade,
        promotion_verified=False,
    )
    decision_grade = bool(prediction_authority["decision_grade"])

    prediction_view: dict[str, Any] | None = None
    if prediction is not None or status in {
        "stale",
        "invalid",
        "closed_context",
    }:
        prediction_view = dict(source_prediction)
        selected_features = (
            feature_snapshot
            if feature_snapshot is not None
            else prediction_view.get("feature_snapshot")
        )
        prediction_view.update(
            {
                "current_price": current,
                "usable": usable,
                "decision_grade": decision_grade,
                "forecast_state": passport_state,
                "prediction_mode": prediction_mode,
                "is_estimate": bool(prediction_authority["is_estimate"]),
                "prediction_authority": prediction_authority,
                "predicted_close": predicted_close if usable else None,
                "prediction_lower": (
                    _finite(passport_prediction.get("interval_lower")) if usable else None
                ),
                "prediction_upper": (
                    _finite(passport_prediction.get("interval_upper")) if usable else None
                ),
                "interval_target_coverage": (
                    _finite(passport_prediction.get("interval_target_coverage"))
                    if usable
                    else None
                ),
                "gamma_pin": _finite(payload.get("gamma_pin")),
                "zero_gamma": _finite(payload.get("zero_gamma")),
                "max_pain": _finite(payload.get("max_pain")),
                "feature_snapshot": _json_safe(dict(selected_features or {})),
                "prediction_snapshot_id": prediction_snapshot_id,
            }
        )

    state = {
        "contract_version": STATE_CONTRACT_VERSION,
        "sequence": 0,
        "state_revision": 0,
        "event_id": "0",
        "forecast_id": forecast_id,
        "forecast_state": passport_state,
        "decision_grade": decision_grade,
        "prediction_snapshot_id": prediction_snapshot_id,
        "symbol": normalized,
        "status": status,
        "generated_at_utc": _utc_iso(),
        "source_as_of_utc": source_timestamp(payload),
        "provider": provider,
        "subscription_epoch_id": payload.get("subscription_epoch_id"),
        "subscription_generation": payload.get("subscription_generation"),
        "current_price": current,
        "prediction_authority": prediction_authority,
        "prediction": prediction_view,
        "pin_payload": payload_copy,
        "health": {
            "usable_for_prediction": usable,
            "decision_grade": decision_grade,
            "forecast_state": passport_state,
            "prediction_authority_state": prediction_authority["authority_state"],
            "data_age_seconds": age,
            "stale_after_seconds": LIVE_DATA_STALE_AFTER_SECONDS,
            "is_stale": stale,
            "validation_is_valid": valid and not fallback,
            "validation_failure_reasons": list(dict.fromkeys(reasons)),
            "passport_validation_status": passport_quality.get("validation_status"),
            "missing_evidence": list(passport_quality.get("missing_evidence") or []),
        },
    }
    return sanitize_workstation_state_pin_payload(state)


def unavailable_workstation_state(symbol: str) -> dict[str, Any]:
    """Return a stable, explicitly unpublished read result."""

    prediction_authority = build_prediction_authority(
        prediction_mode="backend_lifecycle",
        forecast_state="UNAVAILABLE",
        numeric_available=False,
    )
    return {
        "contract_version": STATE_CONTRACT_VERSION,
        "sequence": 0,
        "state_revision": 0,
        "event_id": "0",
        "forecast_id": None,
        "forecast_state": "UNAVAILABLE",
        "decision_grade": False,
        "prediction_snapshot_id": None,
        "symbol": symbol.upper(),
        "status": "unavailable",
        "generated_at_utc": None,
        "source_as_of_utc": None,
        "provider": str(MARKET_DATA_PROVIDER or "unknown"),
        "subscription_epoch_id": None,
        "subscription_generation": None,
        "current_price": None,
        "prediction_authority": prediction_authority,
        "prediction": None,
        "pin_payload": None,
        "health": {
            "usable_for_prediction": False,
            "decision_grade": False,
            "forecast_state": "UNAVAILABLE",
            "prediction_authority_state": prediction_authority["authority_state"],
            "data_age_seconds": None,
            "stale_after_seconds": LIVE_DATA_STALE_AFTER_SECONDS,
            "is_stale": True,
            "validation_is_valid": False,
            "validation_failure_reasons": ["WAITING_FOR_LIFECYCLE_PUBLISH"],
        },
    }


def legacy_dashboard_payload(state: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt versioned state to the existing Streamlit dashboard shape."""

    result = sanitize_workstation_state_pin_payload(state)
    status = str(state.get("status") or "unavailable")
    result["status"] = (
        "ready"
        if status == "live"
        else "fallback"
        if status == "closed_context"
        else status
    )

    payload = dict(result.get("pin_payload") or {})
    market_data_source_label = str(
        payload.get("source_label")
        or (
            "Closed-market historical context only"
            if status == "closed_context"
            else "Databento Live"
            if state.get("provider") == "databento"
            else state.get("provider") or "Unavailable"
        )
    )
    prediction_authority = dict(state.get("prediction_authority") or {})
    prediction_source_label = str(
        market_data_source_label
        if status == "closed_context"
        else prediction_authority.get("source_label") or market_data_source_label
    )
    result["market_data_source_label"] = market_data_source_label
    result["prediction_authority"] = prediction_authority
    prediction = result.get("prediction")
    if prediction is not None:
        prediction.update(
            {
                "provider": state.get("provider"),
                "source_label": prediction_source_label,
                "market_data_source_label": market_data_source_label,
                "is_fallback": status == "closed_context",
                "current_price": state.get("current_price"),
                "decision_grade": bool(state.get("decision_grade")),
                "forecast_id": state.get("forecast_id"),
                "prediction_authority": prediction_authority,
                "prediction_mode": prediction_authority.get("prediction_mode"),
                "is_estimate": bool(prediction_authority.get("is_estimate")),
            }
        )
    health = dict(result.get("health") or {})
    health.update(
        {
            "provider": state.get("provider"),
            "source_label": market_data_source_label,
            "prediction_source_label": prediction_source_label,
            "market_data_source_label": market_data_source_label,
            "is_fallback": status == "closed_context",
            "historical_context_only": bool(payload.get("historical_context_only")),
            "decision_grade": bool(state.get("decision_grade")),
        }
    )
    result["health"] = health
    return result


class WorkstationStateStore:
    """Thread-safe state plus bounded resumable event history."""

    def __init__(self, *, history_size: int = 2048):
        self._condition = threading.Condition(threading.RLock())
        self._states: dict[str, dict[str, Any]] = {}
        self._sequence = 0
        self._history: deque[dict[str, Any]] = deque(
            maxlen=max(1, int(history_size))
        )

    @property
    def sequence(self) -> int:
        with self._condition:
            return self._sequence

    def _event(
        self,
        event_type: str,
        states: list[dict[str, Any]],
        *,
        requires_resync: bool,
        requested_after_sequence: int | None = None,
    ) -> dict[str, Any]:
        oldest = self._history[0]["sequence"] if self._history else None
        if oldest is None and event_type == "update":
            oldest = self._sequence
        return {
            "contract_version": EVENT_CONTRACT_VERSION,
            "event_type": event_type,
            "sequence": self._sequence,
            "event_id": str(self._sequence),
            "requires_resync": requires_resync,
            "requested_after_sequence": requested_after_sequence,
            "oldest_available_sequence": oldest,
            "latest_sequence": self._sequence,
            "generated_at_utc": _utc_iso(),
            "states": copy.deepcopy(states),
        }

    def publish(self, state: Mapping[str, Any]) -> dict[str, Any]:
        with self._condition:
            self._sequence += 1
            item = _json_safe(sanitize_workstation_state_pin_payload(state))
            item.update(
                sequence=self._sequence,
                state_revision=self._sequence,
                event_id=str(self._sequence),
            )
            self._states[str(item["symbol"]).upper()] = item
            self._history.append(
                self._event("update", [item], requires_resync=False)
            )
            self._condition.notify_all()
            return copy.deepcopy(item)

    def get(self, symbol: str) -> dict[str, Any] | None:
        with self._condition:
            value = self._states.get(symbol.upper())
            return copy.deepcopy(value) if value is not None else None

    def all(self) -> list[dict[str, Any]]:
        with self._condition:
            return [copy.deepcopy(self._states[key]) for key in sorted(self._states)]

    def snapshot_event(self, *, requires_resync: bool = False) -> dict[str, Any]:
        with self._condition:
            return self._event(
                "resync_required" if requires_resync else "snapshot",
                [self._states[key] for key in sorted(self._states)],
                requires_resync=requires_resync,
            )

    def heartbeat_event(self) -> dict[str, Any]:
        with self._condition:
            return self._event("heartbeat", [], requires_resync=False)

    def events_after(self, sequence: int | None) -> list[dict[str, Any]]:
        with self._condition:
            if sequence is None:
                return [self.snapshot_event()]
            oldest = self._history[0]["sequence"] if self._history else None
            cursor_invalid = sequence < 0 or sequence > self._sequence
            cursor_has_gap = bool(
                oldest is not None and sequence < int(oldest) - 1
            )
            empty_history_gap = oldest is None and sequence != self._sequence
            if cursor_invalid or cursor_has_gap or empty_history_gap:
                return [
                    self._event(
                        "resync_required",
                        [self._states[key] for key in sorted(self._states)],
                        requires_resync=True,
                        requested_after_sequence=sequence,
                    )
                ]
            return [
                copy.deepcopy(event)
                for event in self._history
                if event["sequence"] > sequence
            ]

    def wait_for_events(
        self, sequence: int, *, timeout_seconds: float = 10.0
    ) -> list[dict[str, Any]]:
        with self._condition:
            if self._sequence <= sequence:
                self._condition.wait(timeout=max(0.0, timeout_seconds))
            return self.events_after(sequence)

    def reset(self) -> None:
        with self._condition:
            self._states.clear()
            self._sequence = 0
            self._history.clear()
            self._condition.notify_all()

    reset_for_tests = reset


workstation_state_store = WorkstationStateStore()
