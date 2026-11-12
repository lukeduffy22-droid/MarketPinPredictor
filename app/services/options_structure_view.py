"""Present independent option-chain evidence without inventing live gamma."""

from __future__ import annotations

from datetime import date, datetime, timezone
import math
import re
from typing import Any


def _positive(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def oi_max_pain_context(payload: dict[str, Any], expiration: str) -> dict[str, Any]:
    """Read one exact-expiration OI result independently of quote-based GEX.

    This never supplies a gamma pin, forecast, or validation approval. The
    provider statistics query cutoff is not the effective timestamp of OI.
    """
    unavailable = {"value": None, "source": None, "statistics_cutoff_utc": None}
    provenance = payload.get("universe_provenance")
    if not isinstance(provenance, dict):
        return unavailable
    source_date = provenance.get("source_date")
    try:
        canonical_date = date.fromisoformat(source_date).isoformat()
    except (TypeError, ValueError):
        return unavailable
    if not (
        source_date == canonical_date == provenance.get("trading_date")
        and provenance.get("is_fallback") is False
        and provenance.get("mode") == "current_day_cache"
        and provenance.get("cache_metadata_status") == "verified"
        and re.fullmatch(r"[0-9a-f]{64}", str(provenance.get("source_sha256") or ""))
        and payload.get("max_pain_source") == "full-oi-universe"
    ):
        return unavailable
    profiles = payload.get("oi_expiration_profiles")
    if not isinstance(profiles, list):
        return unavailable
    matches = [row for row in profiles if isinstance(row, dict)
               and row.get("expiration") == expiration]
    if len(matches) != 1:
        return unavailable
    row = matches[0]
    value = _positive(row.get("max_pain"))
    if value is None or any(_positive(row.get(key)) is None for key in
                            ("total_open_interest", "contracts", "strikes")):
        return unavailable
    return {
        "value": value,
        "source": "full-oi-universe",
        "statistics_cutoff_utc": provenance.get("provider_statistics_end"),
    }


def shadow_expiration_table_rows(view: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for row in view.get("rows") or []:
        if not isinstance(row, dict) or row.get("is_primary") is not False:
            continue
        gamma = _positive(row.get("gamma_pin"))
        pain = _positive(row.get("max_pain"))
        context = row.get("oi_max_pain_context") or {}
        independent_pain = _positive(context.get("value"))
        if pain is None:
            pain = independent_pain
        result.append({
            "Expiration": row.get("expiration"),
            "Gamma Pin": f"${gamma:,.0f}" if gamma is not None else "Unavailable",
            "Max Pain (OI context)": f"${pain:,.0f}" if pain is not None else "Unavailable",
            "Gamma status": str(row.get("validation_status") or "unreported").replace("_", " "),
            "Reason": "; ".join(row.get("validation_reasons") or []),
            "Max-pain source": context.get("source") if independent_pain is not None else
                "expiration profile" if pain is not None else "unavailable",
            "Statistics cutoff (UTC)": context.get("statistics_cutoff_utc")
                if independent_pain is not None else row.get("open_interest_source_end_utc"),
        })
    return result


def close_overlay_unavailable_message(status_code: int, payload: Any) -> str:
    """A rejected read is terminal for this request, never a background job."""
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if isinstance(detail, dict):
        reason = detail.get("reason")
        if isinstance(reason, str) and reason in {"RUNTIME_READ_BUSY", "RUNTIME_READ_TIMEOUT", "RUNTIME_READ_FAILED"}:
            return f"Close forecast read unavailable ({reason}); refresh to recheck."
    if isinstance(detail, str):
        if detail.startswith("No passport-backed close-overlay prediction"):
            return "No verified close forecast is available for this symbol. No forecast is computing in this panel."
        if detail.startswith("Close-overlay payload is stale"):
            return "Close forecast withheld because its source is stale; refresh after fresh evidence is published."
        if detail.startswith("Passport-backed close interval is malformed"):
            return "Close forecast withheld because its interval failed validation."
    if status_code == 429:
        return "Close forecast request was rate limited; refresh later."
    if status_code == 404:
        return "No close forecast has been published for this symbol."
    return f"Close forecast unavailable (HTTP {status_code}); refresh to recheck."


def expiration_gamma_unavailable_message(
    rows: list[dict[str, Any]], live_snapshot: Any = None, *, now: datetime | None = None,
) -> str:
    """Explain per-expiry evidence separately from the transport connection.

    Reuses the page's retained status read. No network access or subscriptions.
    Stage counts cover the entire selected universe, never a single expiration.
    """
    missing = [row for row in rows if isinstance(row, dict)
               and row.get("validation_status") in {"invalid", "unavailable"}]
    dates = list(dict.fromkeys(str(row.get("expiration")) for row in missing if row.get("expiration")))
    subject = ", ".join(dates) if dates else "one or more selected expirations"
    parts = [f"Usable gamma evidence is unavailable for {subject}."]
    snapshot = live_snapshot if isinstance(live_snapshot, dict) else {}
    health = snapshot.get("health") if isinstance(snapshot.get("health"), dict) else {}
    pipeline = snapshot.get("pipeline") if isinstance(snapshot.get("pipeline"), dict) else {}
    now = now or datetime.now(timezone.utc)
    try:
        stamp = datetime.fromisoformat(str(snapshot.get("timestamp_utc")).replace("Z", "+00:00"))
        fresh = stamp.tzinfo is not None and 0 <= (now - stamp).total_seconds() <= 15
    except (TypeError, ValueError):
        fresh = False
    if (fresh and health.get("provider", health.get("market_data_provider")) == "databento"
            and health.get("websocket") == "active" and pipeline.get("stream_connected") is True):
        parts.append("Databento's overall live connection was connected at the latest status read.")
    else:
        parts.append("This expiration-level warning does not establish the status of the overall Databento connection.")
    parts.append("An expiration can be planned while its contracts are still deferred, or its quotes can fail "
                 "freshness and coverage checks. Planning alone does not prove an active subscription.")
    staging = health.get("subscription_staging")
    epoch = pipeline.get("subscription_epoch_id")
    generation = pipeline.get("subscription_generation")
    if (fresh and isinstance(staging, dict) and isinstance(epoch, str)
            and re.fullmatch(r"[0-9a-f]{64}", epoch)
            and type(generation) is int and generation > 0
            and staging.get("subscription_epoch_id") == epoch
            and type(staging.get("subscription_generation")) is int
            and staging.get("subscription_generation") == generation):
        counts = [staging.get(key) for key in
                  ("full_selected_contract_count", "active_contract_count", "deferred_contract_count")]
        if all(type(value) is int and value >= 0 for value in counts) and counts[0] == counts[1] + counts[2]:
            parts.append(f"Across all markets: {counts[0]:,} contracts selected, {counts[1]:,} active, "
                         f"{counts[2]:,} deferred.")
        reasons = staging.get("promotion_reasons")
        if isinstance(reasons, list):
            codes = [reason for reason in reasons if isinstance(reason, str)]
            if codes:
                parts.append("Recorded staging blockers: " + "; ".join(dict.fromkeys(codes)) + ".")
    parts.append("Available open-interest max-pain context is shown separately. "
                 "Missing gamma is withheld; it is not replaced with a price forecast.")
    return " ".join(parts)
