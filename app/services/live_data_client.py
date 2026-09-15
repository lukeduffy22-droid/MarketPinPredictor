"""Shared Streamlit helpers for backend live-data access."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
from typing import Any, Callable

import pandas as pd
import requests

from app.services.sidebar_live_state import SidebarStateBatch, retained_prediction_matches_state
from backend.prediction_authority import build_prediction_authority
from backend.workstation import payload_has_fallback_provenance


ZERO_GAMMA_COMPATIBILITY_FIELD = "zero_gamma"
ZERO_GAMMA_DISPLAY_LABEL = "First strike-bucket GEX sign crossing"
ZERO_GAMMA_SEMANTICS_VERSION = "strike-bucket-first-crossing-v1"
_CANONICAL_SUBSCRIPTION_EPOCH = re.compile(r"^[0-9a-f]{64}$")


def _canonical_subscription_epoch(value: Any) -> str | None:
    candidate = str(value or "").strip()
    return candidate if _CANONICAL_SUBSCRIPTION_EPOCH.fullmatch(candidate) else None


def _positive_generation(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        generation = int(value)
    except (TypeError, ValueError):
        return None
    return generation if generation > 0 else None


def live_pipeline_runtime_ready(pipeline: Any) -> bool:
    """Require a complete current-runtime envelope before live UI wording."""

    if not isinstance(pipeline, dict):
        return False
    if pipeline.get("prediction_pipeline_ok") is not True:
        return False
    if pipeline.get("handoff_status") != "active":
        return False
    if pipeline.get("subscription_epoch_valid") is not True:
        return False
    if _canonical_subscription_epoch(pipeline.get("subscription_epoch_id")) is None:
        return False
    mismatch_fields = (
        "epoch_mismatch_symbols",
        "generation_mismatch_symbols",
        "required_epoch_mismatch_symbols",
        "required_generation_mismatch_symbols",
    )
    return not any(pipeline.get(field) for field in mismatch_fields)


def live_pipeline_symbol_is_current(pipeline: Any, symbol: str) -> bool:
    """Validate one symbol against the active epoch and generation."""

    if not live_pipeline_runtime_ready(pipeline):
        return False
    active_epoch = _canonical_subscription_epoch(pipeline.get("subscription_epoch_id"))
    symbol_status = pipeline.get("symbol_status")
    if not isinstance(symbol_status, dict):
        return False
    detail = symbol_status.get(str(symbol).strip().upper())
    if not isinstance(detail, dict):
        return False
    payload_epoch = _canonical_subscription_epoch(detail.get("subscription_epoch_id"))
    detail_active_epoch = _canonical_subscription_epoch(
        detail.get("active_subscription_epoch_id")
    )
    payload_generation = _positive_generation(detail.get("subscription_generation"))
    active_generation = _positive_generation(detail.get("active_generation"))
    fresh_quote_count = _positive_generation(detail.get("fresh_quote_count"))
    return bool(
        active_epoch
        and payload_epoch == active_epoch
        and detail_active_epoch == active_epoch
        and detail.get("epoch_is_current") is True
        and payload_generation is not None
        and payload_generation == active_generation
        and detail.get("generation_is_current") is True
        and detail.get("usable_for_prediction") is True
        and detail.get("is_stale") is False
        and fresh_quote_count is not None
    )


def _request_json_object(
    url: str,
    *,
    timeout_seconds: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Fetch one local JSON object without erasing transport failure semantics."""

    checked_at_utc = datetime.now(timezone.utc).isoformat()
    try:
        response = requests.get(url, timeout=timeout_seconds)
    except requests.exceptions.ConnectTimeout:
        error = "connect_timeout"
        message = f"connection timed out after {timeout_seconds:.1f}s"
    except requests.exceptions.ReadTimeout:
        error = "read_timeout"
        message = f"response timed out after {timeout_seconds:.1f}s"
    except requests.exceptions.ConnectionError:
        error = "connection_error"
        message = "backend connection failed"
    except requests.exceptions.Timeout:
        error = "timeout"
        message = f"request timed out after {timeout_seconds:.1f}s"
    except Exception as exc:  # noqa: BLE001 - preserve an explicit fail-closed state
        error = "unexpected_error"
        message = f"request failed ({type(exc).__name__})"
    else:
        if response.status_code != 200:
            return None, {
                "transport_ok": False,
                "transport_error": "http_error",
                "transport_message": f"backend returned HTTP {response.status_code}",
                "checked_at_utc": checked_at_utc,
            }
        try:
            payload = response.json()
        except (TypeError, ValueError):
            return None, {
                "transport_ok": False,
                "transport_error": "invalid_json",
                "transport_message": "backend response was not valid JSON",
                "checked_at_utc": checked_at_utc,
            }
        if not isinstance(payload, dict):
            return None, {
                "transport_ok": False,
                "transport_error": "invalid_contract",
                "transport_message": "backend response was not a JSON object",
                "checked_at_utc": checked_at_utc,
            }
        return payload, {
            "transport_ok": True,
            "transport_error": None,
            "transport_message": None,
            "checked_at_utc": checked_at_utc,
        }

    return None, {
        "transport_ok": False,
        "transport_error": error,
        "transport_message": message,
        "checked_at_utc": checked_at_utc,
    }


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _exact_expiration_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    try:
        return datetime.fromisoformat(text[:10]).date().isoformat()
    except (TypeError, ValueError):
        return None


def build_expiration_profile_view(
    payload: dict[str, Any],
    *,
    payload_age_seconds: float | None = None,
    stale_after_seconds: float | None = None,
) -> dict[str, Any]:
    """Normalize exact-date expiration evidence without inventing missing values.

    Non-primary profiles are analytical context. The current backend only applies
    its full validation gate to the primary expiration, so shadow expirations are
    deliberately reported as ``not_production_gated`` instead of being labeled
    valid. Quote age is shared payload-level evidence; it is not represented as a
    per-expiration measurement.
    """

    raw_profiles = payload.get("expiration_profiles")
    raw_subscriptions = payload.get("subscription_expirations")
    profiles = raw_profiles if isinstance(raw_profiles, list) else []
    subscriptions = raw_subscriptions if isinstance(raw_subscriptions, list) else []

    profile_by_expiration: dict[str, dict[str, Any]] = {}
    invalid_profile_entries = 0
    for profile in profiles:
        if not isinstance(profile, dict):
            invalid_profile_entries += 1
            continue
        expiration = _exact_expiration_date(profile.get("expiration"))
        if expiration is None:
            invalid_profile_entries += 1
            continue
        profile_by_expiration[expiration] = profile

    subscription_by_expiration: dict[str, dict[str, Any]] = {}
    invalid_subscription_entries = 0
    ordered_expirations: list[str] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            invalid_subscription_entries += 1
            continue
        expiration = _exact_expiration_date(subscription.get("expiration"))
        if expiration is None:
            invalid_subscription_entries += 1
            continue
        subscription_by_expiration[expiration] = subscription
        if expiration not in ordered_expirations:
            ordered_expirations.append(expiration)
    for expiration in sorted(profile_by_expiration):
        if expiration not in ordered_expirations:
            ordered_expirations.append(expiration)

    primary_expiration = _exact_expiration_date(payload.get("primary_expiration"))
    if primary_expiration and primary_expiration not in ordered_expirations:
        ordered_expirations.insert(0, primary_expiration)

    provenance = payload.get("universe_provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    open_interest_as_of_utc = (
        payload.get("open_interest_as_of_utc")
        or payload.get("oi_as_of_utc")
    )
    if open_interest_as_of_utc is not None:
        open_interest_as_of_utc = str(open_interest_as_of_utc)
    open_interest_source_end_utc = provenance.get("provider_statistics_end")
    if open_interest_source_end_utc is not None:
        open_interest_source_end_utc = str(open_interest_source_end_utc)

    quote_age = _finite_number(payload.get("quote_age_seconds"))
    if quote_age is None:
        quote_age = _finite_number(payload_age_seconds)
    if quote_age is None:
        freshness_status = "unknown"
    elif stale_after_seconds is not None and quote_age > float(stale_after_seconds):
        freshness_status = "stale"
    else:
        freshness_status = "fresh"

    payload_validity = payload.get("validation_is_valid")
    payload_failure_reasons = [
        str(reason) for reason in (payload.get("validation_failure_reasons") or []) if reason
    ]
    selected_target_mode = str(payload.get("selected_target_mode") or "unreported")
    payload_primary_authority = payload.get("primary_expiration_authority")
    payload_primary_context_only = (
        payload.get("primary_expiration_context_only") is True
    )
    payload_same_day_authority = payload.get("same_day_authority")
    payload_primary_selection_basis = payload.get(
        "primary_expiration_selection_basis"
    )
    rows: list[dict[str, Any]] = []
    for expiration in ordered_expirations:
        profile = profile_by_expiration.get(expiration)
        subscription = subscription_by_expiration.get(expiration, {})
        is_primary = expiration == primary_expiration
        role = str(subscription.get("role") or ("primary" if is_primary else "unreported"))
        authority = subscription.get("authority")
        context_only = subscription.get("context_only") is True
        same_day_authority = subscription.get("same_day_authority")
        selection_basis = subscription.get("selection_basis")
        if is_primary:
            authority = authority or payload_primary_authority
            context_only = context_only or payload_primary_context_only
            if same_day_authority is None:
                same_day_authority = payload_same_day_authority
            selection_basis = selection_basis or payload_primary_selection_basis
        calculated_contracts = _finite_number(profile.get("contracts")) if profile else None
        subscribed_contracts = _finite_number(subscription.get("contracts"))
        profile_quote_coverage = None
        profile_quote_age = None
        profile_open_interest = None
        profile_open_interest_as_of_utc = open_interest_as_of_utc
        profile_validation = None
        profile_failure_reasons: list[str] = []
        if profile:
            profile_quote_coverage = _finite_number(
                profile.get("quote_coverage_ratio")
                if profile.get("quote_coverage_ratio") is not None
                else profile.get("fresh_quote_coverage_ratio")
            )
            profile_quote_age = _finite_number(profile.get("quote_age_seconds"))
            profile_open_interest = _finite_number(profile.get("open_interest"))
            profile_open_interest_as_of_utc = (
                profile.get("open_interest_as_of_utc")
                or profile.get("oi_as_of_utc")
                or open_interest_as_of_utc
            )
            if profile_open_interest_as_of_utc is not None:
                profile_open_interest_as_of_utc = str(profile_open_interest_as_of_utc)
            profile_validation = profile.get("validation_is_valid")
            profile_failure_reasons = [
                str(reason)
                for reason in (profile.get("validation_failure_reasons") or [])
                if reason
            ]
        effective_quote_age = profile_quote_age if profile_quote_age is not None else quote_age
        if effective_quote_age is None:
            profile_freshness_status = "unknown"
        elif stale_after_seconds is not None and effective_quote_age > float(stale_after_seconds):
            profile_freshness_status = "stale"
        else:
            profile_freshness_status = "fresh"
        profile_freshness_scope = "profile" if profile_quote_age is not None else "shared_payload"
        calculation_coverage_ratio = None
        if (
            calculated_contracts is not None
            and subscribed_contracts is not None
            and subscribed_contracts > 0
        ):
            calculation_coverage_ratio = calculated_contracts / subscribed_contracts

        required_values = {
            "gamma pin": _finite_number(profile.get("pin")) if profile else None,
            "gross GEX": _finite_number(profile.get("gross_gex")) if profile else None,
            "net GEX": _finite_number(profile.get("net_gex")) if profile else None,
            "calculated contracts": calculated_contracts,
        }
        missing_fields = [name for name, value in required_values.items() if value is None]
        reasons: list[str] = []
        if profile is None:
            status = "unavailable"
            validation_is_valid: bool | None = False
            reasons.append("No calculated GEX profile was returned for this subscribed expiration")
        elif missing_fields:
            status = "invalid"
            validation_is_valid = False
            reasons.append("Missing profile evidence: " + ", ".join(missing_fields))
        elif profile_validation is False:
            status = "invalid"
            validation_is_valid = False
            reasons.extend(profile_failure_reasons or ["Expiration-profile validation failed"])
        elif is_primary and payload_validity is False:
            status = "invalid"
            validation_is_valid = False
            reasons.extend(payload_failure_reasons or ["Primary-expiration validation failed"])
        elif is_primary and context_only:
            status = (
                "valid_primary_context"
                if payload_validity is True
                else "primary_context_gate_unreported"
            )
            validation_is_valid = True if payload_validity is True else None
            reasons.append(
                "Forward-expiration analytical context; not 0DTE authority"
            )
            if payload_validity is not True:
                reasons.append("Primary-expiration validation state was not reported")
        elif is_primary:
            status = "valid_primary" if payload_validity is True else "primary_gate_unreported"
            validation_is_valid = True if payload_validity is True else None
            if payload_validity is not True:
                reasons.append("Primary-expiration validation state was not reported")
        elif profile_validation is True:
            status = "valid_profile_context"
            validation_is_valid = True
            reasons.append("Validated expiration profile; still analytical context, not a target")
        else:
            status = "calculated_context"
            validation_is_valid = None
            reasons.append("Analytical context only; no independent production validation gate")

        rows.append(
            {
                "expiration": expiration,
                "dte": _finite_number(profile.get("dte")) if profile else None,
                "bucket": str(profile.get("bucket")) if profile and profile.get("bucket") else None,
                "role": role,
                "stage": _finite_number(subscription.get("stage")),
                "mode": (
                    f"primary forward context / {selected_target_mode}"
                    if is_primary and context_only
                    else f"primary / {selected_target_mode}"
                    if is_primary
                    else "cross-expiration analytical context"
                ),
                "is_primary": is_primary,
                "authority": authority,
                "context_only": context_only,
                "same_day_authority": same_day_authority,
                "selection_basis": selection_basis,
                "validation_status": status,
                "validation_is_valid": validation_is_valid,
                "validation_reasons": reasons,
                "gamma_pin": required_values["gamma pin"],
                "max_pain": _finite_number(profile.get("max_pain")) if profile else None,
                "first_strike_bucket_gex_sign_crossing": (
                    _finite_number(profile.get("zero_gamma")) if profile else None
                ),
                "gross_gex": required_values["gross GEX"],
                "net_gex": required_values["net GEX"],
                "pin_abs_gex": _finite_number(profile.get("pin_abs_gex")) if profile else None,
                "blend_weight": _finite_number(profile.get("blend_weight")) if profile else None,
                "calculated_contracts": calculated_contracts,
                "subscribed_contracts": subscribed_contracts,
                "calculation_coverage_ratio": calculation_coverage_ratio,
                "quote_coverage_ratio": profile_quote_coverage,
                "quote_age_seconds": effective_quote_age,
                "freshness_status": profile_freshness_status,
                "freshness_scope": profile_freshness_scope,
                "open_interest": profile_open_interest,
                "open_interest_as_of_utc": profile_open_interest_as_of_utc,
                "open_interest_as_of_status": (
                    "reported" if profile_open_interest_as_of_utc else "unavailable"
                ),
                "open_interest_source_end_utc": open_interest_source_end_utc,
            }
        )

    return {
        "rows": rows,
        "primary_expiration": primary_expiration,
        "primary_expiration_authority": next(
            (
                row.get("authority")
                for row in rows
                if row.get("is_primary") is True
                and row.get("authority") is not None
            ),
            payload_primary_authority,
        ),
        "primary_expiration_context_only": any(
            row.get("is_primary") is True and row.get("context_only") is True
            for row in rows
        ) or payload_primary_context_only,
        "same_day_authority": next(
            (
                row.get("same_day_authority")
                for row in rows
                if row.get("is_primary") is True
                and row.get("same_day_authority") is not None
            ),
            payload_same_day_authority,
        ),
        "primary_expiration_selection_basis": next(
            (
                row.get("selection_basis")
                for row in rows
                if row.get("is_primary") is True
                and row.get("selection_basis") is not None
            ),
            payload_primary_selection_basis,
        ),
        "subscription_profile": payload.get("subscription_profile"),
        "selected_target_mode": selected_target_mode,
        "payload_timestamp": payload.get("timestamp") or payload.get("timestamp_utc"),
        "quote_age_seconds": quote_age,
        "freshness_status": freshness_status,
        "freshness_scope": "shared_payload",
        "open_interest_as_of_utc": open_interest_as_of_utc,
        "open_interest_source_end_utc": open_interest_source_end_utc,
        "provenance": {
            "mode": provenance.get("mode"),
            "source_date": provenance.get("source_date"),
            "is_fallback": provenance.get("is_fallback"),
            "reason": provenance.get("reason"),
            "source_sha256": provenance.get("source_sha256") or payload.get("universe_sha256"),
            "selected_universe_sha256": payload.get("selected_universe_sha256"),
        },
        "invalid_profile_entries": invalid_profile_entries,
        "invalid_subscription_entries": invalid_subscription_entries,
        "zero_gamma_label": ZERO_GAMMA_DISPLAY_LABEL,
        "zero_gamma_semantics_version": ZERO_GAMMA_SEMANTICS_VERSION,
    }


def fetch_closing_tape_status(timeout_seconds: float = 5.0) -> dict[str, Any]:
    """Fetch the read-only observed/inferred tape contract."""
    payload, transport = _request_json_object(
        "http://localhost:8000/closing-tape/status",
        timeout_seconds=timeout_seconds,
    )
    if payload is not None:
        return {**payload, **transport, "read_succeeded": True}
    reason = str(transport["transport_message"])
    return {
        **transport,
        "read_succeeded": False,
        "state": "unavailable",
        "usable_for_research": False,
        "reasons": [reason],
        "observed": {"available": False},
        "inferred": {"available": False},
        "model_gate": {"passed": False, "reason": "model evidence is unavailable"},
    }


def fetch_closing_tape_readiness(timeout_seconds: float = 15.0) -> dict[str, Any]:
    """Fetch the read-only cross-session evidence audit."""
    payload, transport = _request_json_object(
        "http://localhost:8000/closing-tape/readiness",
        timeout_seconds=timeout_seconds,
    )
    if payload is not None:
        return {**payload, **transport, "read_succeeded": True}
    return {
        **transport,
        "read_succeeded": False,
        "catalogs": None,
        "sessions": None,
        "eligible_sessions": None,
        "unique_verified_sources": None,
        "verified_close_sessions": None,
        "model_evidence_sessions": None,
        "model_evidence_sources": None,
        "paper_forecast_sessions": None,
        "capture_gate_sessions_required": 10,
        "model_sessions_required": 60,
        "paper_sessions_required": 20,
        "capture_gate_ready": False,
        "model_training_ready": False,
        "paper_evidence_ready": False,
        "unavailable_reason": str(transport["transport_message"]),
        "sessions_detail": [],
    }


def fetch_promoted_closing_tape_predictions(timeout_seconds: float = 5.0) -> dict[str, Any]:
    """Fetch immutable promoted estimates; unavailable never becomes a zero row."""
    payload, transport = _request_json_object(
        "http://localhost:8000/closing-tape/production-predictions",
        timeout_seconds=timeout_seconds,
    )
    if payload is not None and isinstance(payload.get("rows"), list):
        rows = payload["rows"]
        prediction_authority = payload.get("prediction_authority")
        if not isinstance(prediction_authority, dict):
            prediction_authority = build_prediction_authority(
                prediction_mode="tcbbo_promoted",
                forecast_state="VALID" if rows else "UNAVAILABLE",
                numeric_available=bool(rows),
                decision_grade=bool(rows),
                promotion_verified=bool(rows),
            )
        payload["prediction_authority"] = prediction_authority
        payload["is_estimate"] = bool(prediction_authority.get("is_estimate"))
        return {**payload, **transport}
    if payload is not None:
        transport = {
            **transport,
            "transport_ok": False,
            "transport_error": "invalid_contract",
            "transport_message": "backend response did not contain a prediction row list",
        }
    prediction_authority = build_prediction_authority(
        prediction_mode="tcbbo_promoted",
        forecast_state="UNAVAILABLE",
        numeric_available=False,
    )
    return {
        **transport,
        "read_succeeded": False,
        "prediction_mode": "tcbbo_promoted",
        "is_estimate": False,
        "prediction_authority": prediction_authority,
        "available": False,
        "rows": [],
        "reason": str(transport["transport_message"]),
    }


def _current_promoted_row(
    payload: dict[str, Any] | None,
    symbol: str,
) -> dict[str, Any] | None:
    """Select one row only from a complete, current, server-verified batch."""

    batch = payload or {}
    authority = batch.get("prediction_authority")
    rows = batch.get("rows")
    if (
        batch.get("available") is not True
        or batch.get("read_succeeded") is not True
        or batch.get("is_current_session") is not True
        or not isinstance(authority, dict)
        or authority.get("tcbbo_promoted") is not True
        or authority.get("promotion_verified") is not True
        or not isinstance(rows, list)
        or len(rows) != 5
    ):
        return None
    expected = {"SPX", "NDX", "RUT", "VIX", "SPY"}
    roots = {
        str(row.get("family_root") or "")
        for row in rows
        if isinstance(row, dict)
    }
    if roots != expected:
        return None
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and str(row.get("family_root") or "").upper() == symbol.upper()
    ]
    if len(matches) != 1:
        return None
    row = dict(matches[0])
    predicted = _finite_number(row.get("predicted_level"))
    lower = _finite_number(row.get("prediction_lower"))
    upper = _finite_number(row.get("prediction_upper"))
    if (
        predicted is None
        or lower is None
        or upper is None
        or not 0 < lower <= predicted <= upper
    ):
        return None
    return row


def fetch_live_status_snapshot(timeout_seconds: float = 1.2) -> dict[str, Any]:
    """Fetch one live status snapshot, preferring SSE stream and falling back to polling endpoints."""
    sse_url = "http://localhost:8000/events/live"
    try:
        with requests.get(sse_url, stream=True, timeout=(1.0, timeout_seconds)) as response:
            response.raise_for_status()
            for raw_line in response.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                line = str(raw_line).strip()
                if not line.startswith("data:"):
                    continue
                payload = json.loads(line[5:].strip())
                if isinstance(payload, dict):
                    payload["source"] = "sse"
                    return payload
    except Exception:
        pass

    health_data = {}
    pipeline_data = {}
    universe_data = {}
    try:
        health_resp = requests.get("http://localhost:8000/health", timeout=2)
        if health_resp.status_code == 200:
            health_data = health_resp.json()
    except Exception:
        health_data = {}

    try:
        pipeline_resp = requests.get(
            "http://localhost:8000/health/live", timeout=2
        )
        if pipeline_resp.status_code == 200:
            candidate = pipeline_resp.json()
            if isinstance(candidate, dict):
                pipeline_data = candidate
    except Exception:
        pipeline_data = {}

    if not pipeline_data:
        # The compact compatibility health response does not include the
        # per-symbol runtime identity needed to authorize a current label.
        # Preserve what it reported for diagnosis, but fail the live gate.
        pipeline_data = {
            "status": "unavailable",
            "prediction_pipeline_ok": False,
            "reported_prediction_pipeline_ok": health_data.get(
                "prediction_pipeline_ok"
            ),
            "handoff_status": health_data.get("handoff_status") or "unknown",
            "subscription_epoch_id": health_data.get("subscription_epoch_id"),
            "subscription_epoch_valid": bool(
                health_data.get("subscription_epoch_valid", False)
            ),
            "active_generation": health_data.get("active_generation"),
            "epoch_mismatch_symbols": health_data.get("epoch_mismatch_symbols")
            or [],
            "generation_mismatch_symbols": health_data.get(
                "generation_mismatch_symbols"
            )
            or [],
            "symbol_status": {},
            "runtime_context_available": False,
            "reason": "health_live_runtime_context_unavailable",
        }

    try:
        universe_resp = requests.get("http://localhost:8000/databento/universe", timeout=2)
        if universe_resp.status_code == 200:
            universe_data = universe_resp.json()
    except Exception:
        universe_data = {}

    return {
        "source": "poll",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "health": health_data,
        "pipeline": pipeline_data,
        "universe": universe_data,
    }


def market_session_status_from_health(payload: Any) -> dict[str, Any]:
    """Map the backend's holiday-aware subscription window to badge state."""

    if not isinstance(payload, dict):
        return {
            "state": "unavailable",
            "is_open": False,
            "session_state": None,
            "reason": "backend_live_session_state_unavailable",
        }
    session_state = str(payload.get("subscription_session_state") or "").strip()
    expected_subscription_allowed = {
        "non_trading_day": False,
        "preopen_wait": False,
        "preopen": True,
        "regular_session": True,
        "post_close": False,
    }
    if session_state not in expected_subscription_allowed:
        return {
            "state": "unavailable",
            "is_open": False,
            "session_state": session_state or None,
            "reason": "backend_live_session_state_unrecognized",
        }
    subscription_allowed = payload.get("subscription_allowed")
    if subscription_allowed is not expected_subscription_allowed[session_state]:
        return {
            "state": "unavailable",
            "is_open": False,
            "session_state": session_state,
            "reason": "backend_live_session_state_inconsistent",
        }
    is_open = session_state == "regular_session"
    return {
        "state": "open" if is_open else "closed",
        "is_open": is_open,
        "session_state": session_state,
        "reason": None,
    }


def fetch_backend_market_session_status(
    timeout_seconds: float = 1.2,
) -> dict[str, Any]:
    """Read the backend session authority; transport failure never becomes open."""

    payload, transport = _request_json_object(
        "http://localhost:8000/health/live",
        timeout_seconds=timeout_seconds,
    )
    if payload is None:
        return {
            "state": "unavailable",
            "is_open": False,
            "session_state": None,
            "reason": str(
                transport.get("transport_message")
                or "backend_live_session_state_unavailable"
            ),
        }
    return market_session_status_from_health(payload)


def _unavailable_indicator_surface() -> tuple[pd.DataFrame, dict[str, Any]]:
    """Return an explicit empty surface when observed OHLCV bars are unavailable."""
    frame = pd.DataFrame(
        {
            "timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
            "open": pd.Series(dtype="float64"),
            "high": pd.Series(dtype="float64"),
            "low": pd.Series(dtype="float64"),
            "close": pd.Series(dtype="float64"),
            "volume": pd.Series(dtype="float64"),
        }
    )
    provenance = {
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
    return frame, provenance


def _payload_age_seconds(payload: dict[str, Any]) -> float | None:
    timestamp = payload.get("timestamp")
    if isinstance(timestamp, datetime):
        parsed = timestamp
    elif isinstance(timestamp, str):
        try:
            parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())


def _current_lifecycle_close_forecast(
    dashboard_payload: dict[str, Any],
    prediction_payload: dict[str, Any] | None,
    pin_payload: dict[str, Any],
) -> float | None:
    """Accept a lifecycle close forecast only with its current-state identity."""

    prediction = prediction_payload if isinstance(prediction_payload, dict) else {}
    predicted_close = _finite_number(prediction.get("predicted_close"))
    raw_epochs = [
        source.get("subscription_epoch_id")
        for source in (dashboard_payload, prediction, pin_payload)
        if source.get("subscription_epoch_id") not in (None, "")
    ]
    epochs = [_canonical_subscription_epoch(value) for value in raw_epochs]
    raw_generations = [
        source.get("subscription_generation")
        for source in (dashboard_payload, prediction, pin_payload)
        if source.get("subscription_generation") not in (None, "")
    ]
    generations = [_positive_generation(value) for value in raw_generations]
    state_identity = dashboard_payload if dashboard_payload else prediction
    state_epoch = _canonical_subscription_epoch(
        state_identity.get("subscription_epoch_id")
    )
    state_generation = _positive_generation(
        state_identity.get("subscription_generation")
    )
    pin_epoch = _canonical_subscription_epoch(pin_payload.get("subscription_epoch_id"))
    pin_generation = _positive_generation(
        pin_payload.get("subscription_generation")
    )
    dashboard_forecast_id = str(dashboard_payload.get("forecast_id") or "").strip()
    prediction_forecast_id = str(prediction.get("forecast_id") or "").strip()
    forecast_id = dashboard_forecast_id or prediction_forecast_id
    identity_consistent = bool(
        state_epoch is not None
        and state_generation is not None
        and pin_epoch is not None
        and pin_generation is not None
        and raw_epochs
        and all(epoch is not None for epoch in epochs)
        and len(set(epochs)) == 1
        and raw_generations
        and all(generation is not None for generation in generations)
        and len(set(generations)) == 1
    )
    forecast_identity_consistent = bool(
        forecast_id
        and (
            not dashboard_payload
            or (
                dashboard_forecast_id
                and prediction_forecast_id
                and dashboard_forecast_id == prediction_forecast_id
            )
        )
    )
    dashboard_current = str(dashboard_payload.get("status") or "").lower() in {
        "ready",
        "live",
    }
    raw_current = str(prediction.get("state_status") or "").lower() == "live"
    if not (
        predicted_close is not None
        and predicted_close > 0
        and prediction.get("usable") is True
        and (dashboard_current or raw_current)
        and identity_consistent
        and forecast_identity_consistent
    ):
        return None
    return predicted_close


def _source_metadata(payload: dict[str, Any], ai_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    provider = str(payload.get("provider") or (ai_payload or {}).get("provider") or "databento")
    is_fallback = bool(
        payload_has_fallback_provenance(payload)
        or payload_has_fallback_provenance(ai_payload)
        or provider == "historical-fallback"
    )
    market_data_source_label = str(
        payload.get("source_label")
        or (
            "Closed-market historical context only"
            if is_fallback
            else "Databento Live"
        )
    )
    prediction_authority = (ai_payload or {}).get("prediction_authority")
    prediction_authority = (
        dict(prediction_authority)
        if isinstance(prediction_authority, dict)
        else {}
    )
    source_label = str(
        market_data_source_label
        if is_fallback
        else prediction_authority.get("source_label")
        or (ai_payload or {}).get("source_label")
        or market_data_source_label
    )
    return {
        "provider": provider,
        "is_fallback": is_fallback,
        "source_label": source_label,
        "market_data_source_label": market_data_source_label,
        "prediction_authority": prediction_authority,
        "display_mode": "fallback" if is_fallback else "live",
    }


def _confidence_metadata(ai_payload: dict[str, Any] | None) -> dict[str, Any]:
    """Preserve score semantics without fabricating a forecast probability."""
    payload = ai_payload or {}
    confidence = _finite_number(payload.get("confidence"))
    if confidence is None or not 0.0 <= confidence <= 100.0:
        return {
            "confidence": None,
            "confidence_kind": "unavailable",
            "confidence_scale": "not_applicable",
            "confidence_calibrated": False,
        }
    return {
        "confidence": confidence,
        # Older lifecycle payloads predate explicit metadata, but this field is
        # produced by the same deterministic data-quality scoring contract.
        "confidence_kind": str(
            payload.get("confidence_kind") or "data_quality_heuristic"
        ),
        "confidence_scale": str(
            payload.get("confidence_scale") or "percent_0_100"
        ),
        "confidence_calibrated": payload.get("confidence_calibrated") is True,
    }


def fetch_databento_prediction(
    symbol: str,
    timeframe: str,
    indicator_builder: Callable[[pd.DataFrame], pd.DataFrame],
    stale_after_seconds: float,
    promoted_predictions: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Fetch Databento-backed prediction payload and enforce freshness.

    ``indicator_builder`` remains in the public call contract for compatibility,
    but is only safe to invoke once a verified observed-bar payload is available.
    The current backend payload contains pin and estimate fields, not intraday
    OHLCV bars, so this function returns an explicit unavailable surface instead.
    """
    ai_payload = None
    dashboard_payload: dict[str, Any] = {}
    dashboard_status = None
    try:
        dashboard_response = requests.get(f"http://localhost:8000/dashboard/symbol/{symbol}", timeout=3)
        if dashboard_response.status_code == 200:
            dashboard_payload = dashboard_response.json()
            dashboard_status = dashboard_payload.get("status")
            ai_payload = dashboard_payload.get("prediction") or {}
            payload = dashboard_payload.get("pin_payload") or {}
        else:
            ai_response = requests.get(f"http://localhost:8000/ai/predict/{symbol}", timeout=3)
            if ai_response.status_code == 200:
                ai_payload = ai_response.json()
                payload = ai_payload.get("pin_payload") or {}
            else:
                response = requests.get(f"http://localhost:8000/gex/{symbol}", timeout=3)
                payload = response.json() if response.status_code == 200 else None
    except Exception as exc:  # noqa: BLE001
        return None, f"{symbol}: backend unavailable ({str(exc)[:50]})", None

    return build_databento_prediction_from_payloads(
        symbol=symbol,
        timeframe=timeframe,
        stale_after_seconds=stale_after_seconds,
        dashboard_payload=dashboard_payload,
        promoted_predictions=promoted_predictions,
        ai_payload=ai_payload,
        payload=payload,
    )


def build_databento_predictions_from_state_batch(
    batch: SidebarStateBatch,
    *,
    timeframe: str,
    stale_after_seconds: float,
    promoted_predictions: dict[str, Any] | None = None,
) -> tuple[dict[str, dict[str, Any]], tuple[str, ...]]:
    """Build and validate each forecast from the same captured state and health read."""

    predictions: dict[str, dict[str, Any]] = {}
    messages: list[str] = []
    for state in batch.results:
        if not state.usable:
            continue
        dashboard_payload = batch.dashboard_payloads.get(state.symbol)
        if dashboard_payload is None:
            messages.append(f"{state.symbol}: matching lifecycle response is unavailable")
            continue
        prediction, warning, info = build_databento_prediction_from_payloads(
            symbol=state.symbol,
            timeframe=timeframe,
            stale_after_seconds=stale_after_seconds,
            dashboard_payload=dict(dashboard_payload),
            promoted_predictions=promoted_predictions,
        )
        messages.extend(message for message in (warning, info) if message)
        if prediction is None:
            continue
        matches, reason = retained_prediction_matches_state(prediction, state)
        if matches:
            predictions[state.symbol] = prediction
        else:
            messages.append(f"{state.symbol}: {reason}")
    return predictions, tuple(messages)


def build_databento_prediction_from_payloads(
    *,
    symbol: str,
    timeframe: str,
    stale_after_seconds: float,
    dashboard_payload: dict[str, Any],
    promoted_predictions: dict[str, Any] | None = None,
    ai_payload: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    """Convert one captured lifecycle response without requesting a newer revision."""

    dashboard_status = dashboard_payload.get("status")
    if ai_payload is None:
        ai_payload = dashboard_payload.get("prediction") or {}
    if payload is None:
        payload = dashboard_payload.get("pin_payload") or {}

    if not payload:
        if dashboard_status == "warming":
            return None, None, f"{symbol}: Databento dashboard warming (waiting for first live payload)"
        return None, f"{symbol}: no Databento prediction cache yet", None

    if payload.get("validation_is_valid") is False:
        reasons = payload.get("validation_failure_reasons") or [payload.get("pregate_reason") or "No valid Databento pin yet"]
        return None, None, f"{symbol}: Databento diagnostics recorded, but no valid prediction yet ({reasons[0]})"

    age_seconds = _payload_age_seconds(payload)
    if age_seconds is None:
        return None, f"{symbol}: payload timestamp missing/invalid", None
    if age_seconds > stale_after_seconds:
        return None, f"{symbol}: live payload stale ({age_seconds:.1f}s > {stale_after_seconds:.1f}s)", None

    promoted_row = _current_promoted_row(promoted_predictions, symbol)
    current_price = _finite_number(payload.get("price"))
    lifecycle_predicted_price = _current_lifecycle_close_forecast(
        dashboard_payload,
        ai_payload,
        payload,
    )
    if current_price is None or current_price <= 0:
        return None, f"{symbol}: incomplete Databento payload", None
    if promoted_row is None and lifecycle_predicted_price is None:
        return (
            None,
            f"{symbol}: no current usable lifecycle or promoted close forecast",
            None,
        )

    current_price = float(current_price)
    predicted_price = float(
        promoted_row["predicted_level"]
        if promoted_row is not None
        else lifecycle_predicted_price
    )
    prediction_mode = str(
        (ai_payload or {}).get("prediction_mode") or "backend_lifecycle"
    )
    prediction_authority = (ai_payload or {}).get("prediction_authority")
    if not isinstance(prediction_authority, dict):
        prediction_authority = build_prediction_authority(
            prediction_mode=prediction_mode,
            forecast_state=(ai_payload or {}).get("forecast_state") or "RESEARCH_ONLY",
            numeric_available=True,
            decision_grade=(ai_payload or {}).get("decision_grade") is True,
            promotion_verified=False,
        )
    if promoted_row is not None:
        prediction_authority = dict(
            (promoted_predictions or {})["prediction_authority"]
        )
        prediction_mode = "tcbbo_promoted"
    source_payload = dict(ai_payload or {})
    source_payload["prediction_authority"] = prediction_authority
    source = _source_metadata(payload, source_payload)
    change_pct = ((predicted_price - current_price) / current_price) * 100
    confidence_metadata = _confidence_metadata(ai_payload)

    gamma_pin = payload.get("gamma_pin")
    max_pain_value = payload.get("max_pain")
    zero_gamma = (
        payload.get("zero_gamma")
        if payload.get("zero_gamma") is not None
        else payload.get("zero_gamma_level")
    )
    df, indicator_provenance = _unavailable_indicator_surface()
    top_strikes = payload.get("top_strikes") or []
    expiration_view = build_expiration_profile_view(
        payload,
        payload_age_seconds=age_seconds,
        stale_after_seconds=stale_after_seconds,
    )
    gamma_walls = pd.DataFrame(top_strikes)
    if not gamma_walls.empty:
        if "net_gex" not in gamma_walls.columns and "gex" in gamma_walls.columns:
            gamma_walls["net_gex"] = gamma_walls["gex"]
        if "total_gex" not in gamma_walls.columns and "net_gex" in gamma_walls.columns:
            gamma_walls["total_gex"] = gamma_walls["net_gex"].abs()
        if "days_to_expiry" not in gamma_walls.columns:
            gamma_walls["days_to_expiry"] = 0
        if "expiration_count" not in gamma_walls.columns:
            gamma_walls["expiration_count"] = 1

    return (
        {
            "ticker": symbol,
            "current_price": current_price,
            "predicted_price": predicted_price,
            **confidence_metadata,
            "df": df,
            "indicators_available": False,
            "indicator_status": "unavailable",
            "indicator_provenance": indicator_provenance,
            "change_pct": change_pct,
            "model_type": (
                "Promoted TCBBO model"
                if promoted_row is not None
                else
                (ai_payload or {}).get("model_type")
                or "Backend estimator identity unavailable"
            ),
            "model_version": (
                promoted_row.get("model_version")
                if promoted_row is not None
                else (ai_payload or {}).get("model_version")
            ),
            "model_artifact_sha256": (
                promoted_row.get("artifact_sha256")
                if promoted_row is not None
                else (ai_payload or {}).get("model_artifact_sha256")
            ),
            "feature_schema_version": (ai_payload or {}).get(
                "feature_schema_version"
            ),
            "feature_hash": (ai_payload or {}).get("feature_hash"),
            "forecast_id": (
                dashboard_payload.get("forecast_id")
                or (ai_payload or {}).get("forecast_id")
            ),
            "forecast_state": (
                "VALID"
                if promoted_row is not None
                else dashboard_payload.get("forecast_state")
                or (ai_payload or {}).get("forecast_state")
            ),
            "state_revision": dashboard_payload.get("state_revision"),
            "event_id": dashboard_payload.get("event_id"),
            "prediction_snapshot_id": dashboard_payload.get("prediction_snapshot_id"),
            "source_as_of_utc": (
                dashboard_payload.get("source_as_of_utc")
                or payload.get("timestamp")
                or payload.get("timestamp_utc")
            ),
            "decision_grade": prediction_authority.get("decision_grade") is True,
            "prediction_mode": str(prediction_authority["prediction_mode"]),
            "is_estimate": prediction_authority.get("is_estimate") is True,
            "tcbbo_promoted": prediction_authority.get("tcbbo_promoted") is True,
            "prediction_authority": prediction_authority,
            "prediction_lower": (
                promoted_row.get("prediction_lower")
                if promoted_row is not None
                else (ai_payload or {}).get("prediction_lower")
            ),
            "prediction_upper": (
                promoted_row.get("prediction_upper")
                if promoted_row is not None
                else (ai_payload or {}).get("prediction_upper")
            ),
            "interval_target_coverage": (
                promoted_row.get("interval_target_coverage")
                if promoted_row is not None
                else (ai_payload or {}).get("interval_target_coverage")
            ),
            "source_sha256": (
                promoted_row.get("source_sha256")
                if promoted_row is not None
                else None
            ),
            "feature_available_at_utc": (
                promoted_row.get("feature_available_at_utc")
                if promoted_row is not None
                else None
            ),
            "prediction_recorded_at_utc": (
                promoted_row.get("recorded_at_utc")
                if promoted_row is not None
                else None
            ),
            "prediction_trading_date": (
                promoted_row.get("trading_date")
                if promoted_row is not None
                else None
            ),
            "timeframe": timeframe,
            "has_vix": False,
            "data_source": source["display_mode"],
            "ticker_used": source["source_label"],
            "provider": source["provider"],
            "source_label": source["source_label"],
            "market_data_source_label": source["market_data_source_label"],
            "is_fallback": source["is_fallback"],
            "display_mode": source["display_mode"],
            "inference_device": (
                promoted_row.get("execution_device")
                if promoted_row is not None
                else (ai_payload or {}).get("inference_device", "cpu")
            ),
            "subscription_epoch_id": (
                dashboard_payload.get("subscription_epoch_id")
                or (ai_payload or {}).get("subscription_epoch_id")
                or payload.get("subscription_epoch_id")
            ),
            "subscription_generation": (
                dashboard_payload.get("subscription_generation")
                or (ai_payload or {}).get("subscription_generation")
                or payload.get("subscription_generation")
            ),
            "quote_age_seconds": (ai_payload or {}).get("quote_age_seconds") or payload.get("quote_age_seconds") or age_seconds,
            "active_contract_count": payload.get("contracts") or payload.get("contracts_count"),
            "fresh_quote_count": payload.get("fresh_quote_count"),
            "pin_payload": payload,
            "payload_age_seconds": age_seconds,
            "underlying_validation_status": payload.get("underlying_validation_status", "unavailable"),
            "underlying_proxy_symbol": payload.get("underlying_proxy_symbol"),
            "underlying_price": payload.get("underlying_price"),
            "underlying_timestamp_utc": payload.get("underlying_timestamp_utc"),
            "underlying_age_seconds": payload.get("underlying_age_seconds"),
            "underlying_divergence_points": payload.get("underlying_divergence_points"),
            "underlying_divergence_pct": payload.get("underlying_divergence_pct"),
            "underlying_validation_reason": payload.get("underlying_validation_reason"),
            "expiration_view": expiration_view,
            "expiration_profiles": expiration_view["rows"],
            "subscription_expirations": payload.get("subscription_expirations") or [],
            "primary_expiration": expiration_view["primary_expiration"],
            "primary_expiration_authority": expiration_view[
                "primary_expiration_authority"
            ],
            "primary_expiration_context_only": expiration_view[
                "primary_expiration_context_only"
            ],
            "same_day_authority": expiration_view["same_day_authority"],
            "primary_expiration_selection_basis": expiration_view[
                "primary_expiration_selection_basis"
            ],
            "gex_data": {
                "pin_strike": float(gamma_pin) if gamma_pin is not None else None,
                "pin_runner_up_strike": payload.get("pin_runner_up_strike"),
                "pin_lead_ratio": payload.get("pin_lead_ratio"),
                "pin_is_contested": bool(payload.get("pin_is_contested")),
                "pin_competition_reason": payload.get("pin_competition_reason"),
                "max_pain_strike": float(max_pain_value) if max_pain_value is not None else None,
                "zero_gamma": float(zero_gamma) if zero_gamma is not None else None,
                "zero_gamma_compatibility_field": ZERO_GAMMA_COMPATIBILITY_FIELD,
                "zero_gamma_label": str(
                    payload.get("zero_gamma_label") or ZERO_GAMMA_DISPLAY_LABEL
                ),
                "zero_gamma_method": payload.get("zero_gamma_method"),
                "zero_gamma_semantics_version": str(
                    payload.get("zero_gamma_semantics_version")
                    or ZERO_GAMMA_SEMANTICS_VERSION
                ),
                "zero_gamma_is_portfolio_spot_sweep": False,
                "total_gex": float(
                    payload.get("gross_gex")
                    if payload.get("gross_gex") is not None
                    else payload.get("total_gex_abs", 0)
                ),
                "net_gex": float(payload.get("net_gex", 0)),
                "gex_unit": "raw_gamma_x_open_interest_x_100",
                "summary": f"{(ai_payload or {}).get('model_type', 'Databento')} target ${predicted_price:,.0f}; net GEX {payload.get('net_gex', 0):+,.0f}",
                "ai_signals": (ai_payload or {}).get("signals", []),
                "ai_feature_snapshot": (ai_payload or {}).get("feature_snapshot", {}),
                "vix_pressure": (ai_payload or {}).get("vix_pressure"),
                "vix_adjustment": (ai_payload or {}).get("vix_adjustment"),
                "gamma_walls": gamma_walls,
                "gex_by_strike": gamma_walls,
                "positive_gex_wall": payload.get("positive_gex_wall"),
                "negative_gex_wall": payload.get("negative_gex_wall"),
                "options_root": "OPRA",
                "is_etf_proxy": False,
                "key_levels": [level for level in [gamma_pin, max_pain_value, zero_gamma] if level is not None],
                "expiration_view": expiration_view,
                "expiration_profiles": expiration_view["rows"],
                "subscription_expirations": payload.get("subscription_expirations") or [],
                "primary_expiration": expiration_view["primary_expiration"],
                "primary_expiration_authority": expiration_view[
                    "primary_expiration_authority"
                ],
                "primary_expiration_context_only": expiration_view[
                    "primary_expiration_context_only"
                ],
                "same_day_authority": expiration_view["same_day_authority"],
                "primary_expiration_selection_basis": expiration_view[
                    "primary_expiration_selection_basis"
                ],
            },
        },
        None,
        None,
    )
