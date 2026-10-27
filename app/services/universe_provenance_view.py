"""Fail-closed presentation metadata for option-universe provenance."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any


def _canonical_date(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def universe_provenance_view(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    """Describe universe authority without trusting only ``is_fallback``.

    ``universe_provenance.trading_date`` is the session/admission date while
    ``source_date`` is the actual cache-definition date.  A producer flag of
    ``is_fallback=false`` cannot make mismatched dates current-session data.
    This helper is presentation-only and never mutates the producer payload.
    """

    source_payload = payload if isinstance(payload, Mapping) else {}
    raw = source_payload.get("universe_provenance")
    provenance = raw if isinstance(raw, Mapping) else {}

    session_date = None
    for candidate in (
        source_payload.get("current_trading_date"),
        source_payload.get("session_trading_date"),
        source_payload.get("runtime_trading_date"),
        provenance.get("trading_date"),
        source_payload.get("trading_date"),
    ):
        session_date = _canonical_date(candidate)
        if session_date is not None:
            break

    source_date = _canonical_date(provenance.get("source_date"))
    reported_flag = provenance.get("is_fallback")
    reported_is_fallback = reported_flag if isinstance(reported_flag, bool) else None
    date_mismatch = bool(
        session_date is not None
        and source_date is not None
        and session_date != source_date
    )
    prior_session = bool(
        date_mismatch
        and source_date is not None
        and session_date is not None
        and source_date < session_date
    )
    effective_is_fallback = bool(reported_is_fallback is True or date_mismatch)

    warning = None
    if date_mismatch:
        relationship = (
            "predates"
            if prior_session
            else "does not match"
        )
        context = (
            "prior-session fallback"
            if prior_session
            else "mismatched-date fallback"
        )
        warning = (
            f"Universe source date {source_date} {relationship} runtime/session "
            f"trading date {session_date}. Treating it as non-authoritative "
            f"{context} context, not current-session data."
        )

    if date_mismatch:
        authority_status = (
            "non_authoritative_prior_session"
            if prior_session
            else "non_authoritative_date_mismatch"
        )
    elif reported_is_fallback is True:
        authority_status = "reported_fallback"
    elif (
        session_date is not None
        and source_date is not None
        and session_date == source_date
        and reported_is_fallback is False
    ):
        authority_status = "source_date_matches_session"
    else:
        authority_status = "unverified"

    return {
        "mode": provenance.get("mode"),
        "trading_date": session_date or provenance.get("trading_date"),
        "source_date": source_date or provenance.get("source_date"),
        "reported_is_fallback": reported_is_fallback,
        "is_fallback": effective_is_fallback,
        "date_mismatch": date_mismatch,
        "prior_session": prior_session,
        "authority_status": authority_status,
        "warning": warning,
        "reason": provenance.get("reason"),
        "source_sha256": provenance.get("source_sha256")
        or source_payload.get("universe_sha256"),
        "selected_universe_sha256": source_payload.get(
            "selected_universe_sha256"
        ),
    }
