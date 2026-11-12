"""Request-time authority for immutable research forecasts; no I/O or model changes."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import math
import re

from backend.forecast_horizons import MAX_SPOT_AGE_SECONDS, _forecast_digest

SCHEMA = "marketpin-forecast-publication-gate.v1"
HEALTH_MAX_AGE_SECONDS = 15.0
_HASH = re.compile(r"[0-9a-f]{64}\Z")


def _stamp(value):
    try:
        result = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.tzinfo is not None else None
    except (TypeError, ValueError, AttributeError, OverflowError):
        return None


def _number(value):
    if type(value) not in (int, float):
        return None
    return float(value) if math.isfinite(value) else None


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _identity(value):
    value = _mapping(value)
    epoch, generation = value.get("subscription_epoch_id"), value.get("subscription_generation")
    if not isinstance(epoch, str) or not _HASH.fullmatch(epoch) or type(generation) is not int or generation < 1:
        return None
    return epoch, generation


def evaluate_publication_gate(forecasts, runtime_context, *, checked_at_utc):
    """Bind every publication to one coherent, bounded, current health observation.

    The forecast digest and issuance evidence remain unchanged. Current health
    may revoke permission without a subscription generation change.
    """
    now = _stamp(checked_at_utc)
    if now is None:
        raise ValueError("PUBLICATION_CHECK_TIMESTAMP_INVALID")
    context = _mapping(runtime_context)
    sampled = _stamp(context.get("sampled_at_utc"))
    identity = _identity(context)
    common = []
    if sampled is None or not 0 <= (now - sampled).total_seconds() <= HEALTH_MAX_AGE_SECONDS:
        common.append("CURRENT_RUNTIME_HEALTH_UNAVAILABLE_OR_STALE")
    if identity is None or context.get("runtime_context_stable") is not True or context.get("handoff_status") != "active":
        common.append("CURRENT_RUNTIME_IDENTITY_UNVERIFIED")
    if any(context.get(key) is not True for key in ("transport_ready", "stream_connected", "stream_progressing")):
        common.append("CURRENT_TRANSPORT_NOT_READY")
    clock = _mapping(context.get("processing_clock_telemetry"))
    if clock.get("status") != "synchronized" or clock.get("replay_pending") is True:
        common.append("CURRENT_CLOCK_NOT_SYNCHRONIZED")
    result = {"schema_version": SCHEMA, "checked_at_utc": now.isoformat(),
              "runtime_context": {"subscription_epoch_id": identity[0] if identity else None,
                                  "subscription_generation": identity[1] if identity else None},
              "reasons": common, "symbols": {}}
    for symbol, supplied in forecasts.items():
        forecast = _mapping(supplied)
        evidence = _mapping(forecast.get("evidence"))
        reasons = list(common)
        deadlines = []
        forecast_id = forecast.get("forecast_id")
        try:
            intact = isinstance(forecast_id, str) and forecast_id == _forecast_digest(forecast)
        except (TypeError, ValueError, KeyError, OverflowError):
            intact = False
        value = _number(forecast.get("predicted_close"))
        if forecast.get("status") != "RESEARCH_ONLY" or forecast.get("symbol") != symbol or value is None or value <= 0:
            reasons.append("ISSUED_FORECAST_INELIGIBLE")
        if not intact:
            reasons.append("ISSUED_FORECAST_ID_UNVERIFIED")
        candidates = forecast.get("candidates")
        selected = [row for row in candidates if isinstance(row, dict) and
                    row.get("method_id") == forecast.get("selected_method")] if isinstance(candidates, list) else []
        if (len(selected) != 1 or selected[0].get("status") != "RESEARCH_ONLY" or
                _number(selected[0].get("predicted_close")) != value or selected[0].get("prospective_only") is True):
            reasons.append("ISSUED_SELECTION_UNVERIFIED")
        if identity is None or _identity(evidence) != identity:
            reasons.append("FORECAST_RUNTIME_IDENTITY_MISMATCH")
        origin, source, available = (_stamp(forecast.get("as_of_utc")),
                                     _stamp(evidence.get("source_timestamp_utc")),
                                     _stamp(evidence.get("available_at_utc")))
        if (origin is None or source is None or available is None or
                not source <= available <= origin <= now):
            reasons.append("FORECAST_SOURCE_TIMESTAMP_UNVERIFIED")
        else:
            deadlines.extend((origin + timedelta(seconds=120), source + timedelta(seconds=MAX_SPOT_AGE_SECONDS)))
        opened, closed, target = (_stamp(forecast.get("session_open_utc")),
                                 _stamp(forecast.get("session_close_utc")),
                                 _stamp(forecast.get("target_close_utc")))
        if opened is None or closed is None or target is None or not opened <= now < closed or now >= target:
            reasons.append("CURRENT_CASH_SESSION_NOT_OPEN")
        else:
            deadlines.extend((closed, target))
        detail = _mapping(_mapping(context.get("symbol_status")).get(symbol))
        if (any(detail.get(key) is not True for key in
                ("usable_for_prediction", "validation_is_valid", "epoch_is_current", "generation_is_current"))
                or detail.get("is_stale") is not False or detail.get("validation_failure_reasons") != []):
            reasons.append("CURRENT_SYMBOL_INELIGIBLE")
        if (identity is None or _identity(detail) != identity or
                detail.get("active_subscription_epoch_id") != identity[0] or
                type(detail.get("active_generation")) is not int or detail.get("active_generation") != identity[1]):
            reasons.append("CURRENT_SYMBOL_IDENTITY_MISMATCH")
        age, stale = _number(detail.get("data_age_seconds")), _number(detail.get("stale_after_seconds"))
        count = detail.get("fresh_quote_count")
        if (sampled is None or age is None or stale is None or not 0 <= age < stale or
                type(count) is not int or count < 1):
            reasons.append("CURRENT_SYMBOL_QUOTES_UNVERIFIED")
        else:
            deadlines.append(sampled + timedelta(seconds=stale - age))
        if sampled is not None:
            deadlines.append(sampled + timedelta(seconds=HEALTH_MAX_AGE_SECONDS))
        deadline = min(deadlines) if deadlines else None
        if deadline is None or now >= deadline:
            reasons.append("CURRENT_PUBLICATION_EXPIRED")
        reasons = list(dict.fromkeys(reasons))
        result["symbols"][symbol] = {
            "status": "ABSTAIN" if reasons else "ELIGIBLE", "reasons": reasons,
            "source_forecast_id": forecast_id, "checked_at_utc": now.isoformat(),
            "valid_until_utc": deadline.isoformat() if deadline is not None and not reasons else None,
        }
    result["status"] = "ELIGIBLE" if result["symbols"] and all(
        row["status"] == "ELIGIBLE" for row in result["symbols"].values()) else "ABSTAIN"
    return result


def project_publication_item(item, forecast, retained_forecast, symbol_gate):
    """Withdraw current numbers while preserving issuance and measured history."""
    result = deepcopy(item)
    current = deepcopy(forecast)
    result["retained_forecast"] = deepcopy(retained_forecast)
    result["forecast"] = current
    if symbol_gate.get("status") == "ELIGIBLE":
        return result
    reasons = symbol_gate.get("reasons") or ["CURRENT_PUBLICATION_UNVERIFIED"]
    current.update(status="ABSTAIN", predicted_close=None, candidates=[])
    current["reasons"] = list(dict.fromkeys([*current.get("reasons", []), *reasons]))
    direction = result.get("directional_shift")
    if isinstance(direction, dict):
        direction.update(status="ABSTAIN", direction=None)
        direction["reason_codes"] = list(dict.fromkeys([*direction.get("reason_codes", []), *reasons]))
    learning = result.get("eod_learning")
    if isinstance(learning, dict):
        learning.update(status="ABSTAIN", disagreement=None)
        learning["reasons"] = list(dict.fromkeys([*learning.get("reasons", []), *reasons]))
        for comparison in learning.get("comparisons", []):
            comparison["current_estimate"] = None
        adaptive = learning.get("adaptive")
        if isinstance(adaptive, dict):
            adaptive.update(status="ABSTAIN", predicted_close=None, historical_error_band=None)
    return result
