"""Trust-preserving Streamlit view for sampled opening ranges."""

from __future__ import annotations

import math
from typing import Any, Mapping, MutableMapping, Sequence

import pandas as pd
import requests
import streamlit as st


ORB_ENDPOINT = "http://localhost:8000/v1/orb"
ORB_COLLECTION_SCHEMA_V2 = "marketpin-reference-orb.collection.v2"
ORB_SYMBOL_SCHEMA_V2 = "marketpin-reference-orb.v2"
ORB_FETCH_FAILURE_KEY = "_marketpin_orb_fetch_failure"
ORB_RUNTIME_FAILURE_REASONS = frozenset(
    {"RUNTIME_READ_BUSY", "RUNTIME_READ_TIMEOUT", "RUNTIME_READ_FAILED"}
)
LIVE_AUTO_REFRESH_INITIALIZED_KEY = "_live_autorefresh_initialized"
LIVE_AUTO_REFRESH_KEY = "auto_refresh_live"


def initialize_live_autorefresh(
    session_state: MutableMapping[str, Any],
) -> None:
    """Disable automatic refresh, including in sessions opened before this change."""

    session_state[LIVE_AUTO_REFRESH_KEY] = False
    session_state[LIVE_AUTO_REFRESH_INITIALIZED_KEY] = True


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _nonnegative_int(value: Any) -> int:
    number = _finite_float(value)
    if number is None or number < 0.0 or not number.is_integer():
        return 0
    return int(number)


def _positive_int(value: Any) -> int | None:
    number = _finite_float(value)
    if number is None or number <= 0.0 or not number.is_integer():
        return None
    return int(number)


def _canonical_subscription_epoch(value: Any) -> str | None:
    if not isinstance(value, str) or len(value) != 64:
        return None
    if any(character not in "0123456789abcdef" for character in value):
        return None
    return value


def _abbreviate_epoch(value: str | None) -> str:
    return f"{value[:8]}…{value[-8:]}" if value is not None else "unavailable"


def _live_envelope_context(
    payload: Mapping[str, Any],
) -> tuple[bool, str | None, int | None]:
    context = _as_mapping(payload.get("active_runtime_context"))
    active_epoch = _canonical_subscription_epoch(
        context.get("subscription_epoch_id")
    )
    active_generation = _positive_int(context.get("subscription_generation"))
    eligible = bool(
        payload.get("schema_version") == ORB_COLLECTION_SCHEMA_V2
        and payload.get("runtime_binding_applied") is True
        and payload.get("runtime_context_stable") is True
        and active_epoch is not None
        and active_generation is not None
        and context.get("handoff_status") == "active"
    )
    return eligible, active_epoch, active_generation


def _state_has_current_v2_identity(
    state: Mapping[str, Any],
    *,
    envelope_eligible: bool,
    active_epoch: str | None,
    active_generation: int | None,
) -> bool:
    provenance = _as_mapping(state.get("provenance"))
    reference = _as_mapping(state.get("reference_semantics"))
    return bool(
        envelope_eligible
        and state.get("schema_version") == ORB_SYMBOL_SCHEMA_V2
        and isinstance(reference.get("directional_base_eligible"), bool)
        and str(reference.get("kind") or "").strip()
        and provenance.get("runtime_binding_applied") is True
        and provenance.get("active_runtime_epoch_aligned") is True
        and _canonical_subscription_epoch(
            provenance.get("active_subscription_epoch_id")
        )
        == active_epoch
        and _positive_int(provenance.get("active_subscription_generation"))
        == active_generation
        and provenance.get("active_handoff_status") == "active"
    )


def _window_summary(value: Any, *, directional_allowed: bool) -> str:
    window = value if isinstance(value, Mapping) else {}
    status = str(window.get("capture_status") or "unavailable")
    low = window.get("orb_low")
    high = window.get("orb_high")
    breakout = (
        str(window.get("breakout_direction") or "unavailable")
        if directional_allowed
        else "context only"
    )
    if status == "complete" and isinstance(low, (int, float)) and isinstance(
        high, (int, float)
    ):
        return f"{float(low):,.2f}–{float(high):,.2f} | {breakout}"
    return status


def _orb_fetch_failure(reason: str, *, status_code: int | None = None) -> dict[str, Any]:
    """Return a non-signal UI diagnostic for a failed bounded ORB read."""

    return {
        ORB_FETCH_FAILURE_KEY: {
            "reason": reason,
            "status_code": status_code,
        }
    }


def _orb_http_failure(response: Any) -> dict[str, Any]:
    reason = "ORB_HTTP_ERROR"
    try:
        payload = response.json()
    except (ValueError, TypeError):
        payload = None
    if isinstance(payload, Mapping):
        detail = payload.get("detail")
        detail = detail if isinstance(detail, Mapping) else {}
        candidate = str(detail.get("reason") or "").strip().upper()
        if candidate in ORB_RUNTIME_FAILURE_REASONS:
            reason = candidate
    return _orb_fetch_failure(reason, status_code=int(response.status_code))


def fetch_opening_ranges(timeout_seconds: float = 1.5) -> dict[str, Any]:
    """Read ORB state while preserving a safe reason when the read is unavailable."""
    try:
        response = requests.get(ORB_ENDPOINT, timeout=timeout_seconds)
        if response.status_code == 200:
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        return _orb_http_failure(response)
    except requests.Timeout:
        return _orb_fetch_failure("ORB_REQUEST_TIMEOUT")
    except requests.ConnectionError:
        return _orb_fetch_failure("ORB_BACKEND_UNREACHABLE")
    except requests.RequestException:
        return _orb_fetch_failure("ORB_REQUEST_FAILED")
    except ValueError:
        return _orb_fetch_failure("ORB_RESPONSE_INVALID")


def opening_range_rows(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    """Flatten the versioned ORB contract for compact, auditable display."""
    envelope = payload if isinstance(payload, Mapping) else {}
    envelope_eligible, active_epoch, active_generation = _live_envelope_context(
        envelope
    )
    symbols = envelope.get("symbols") or {}
    if not isinstance(symbols, Mapping):
        return []
    rows: list[dict[str, Any]] = []
    for symbol, raw_state in symbols.items():
        state = _as_mapping(raw_state)
        evidence = _as_mapping(state.get("capture_evidence"))
        pin = _as_mapping(state.get("pin_behavior"))
        provenance = _as_mapping(state.get("provenance"))
        structure_status = str(
            provenance.get("structure_reference_status") or "unavailable"
        )
        reference = _as_mapping(state.get("reference_semantics"))
        state_identity_current = _state_has_current_v2_identity(
            state,
            envelope_eligible=envelope_eligible,
            active_epoch=active_epoch,
            active_generation=active_generation,
        )
        current_reference_visible = bool(
            state_identity_current and state.get("current_reference_fresh") is True
        )
        directional_visible = bool(
            current_reference_visible
            and reference.get("directional_base_eligible") is True
            and state.get("directional_evidence_eligible") is True
        )
        structure_is_current = bool(
            current_reference_visible and structure_status == "aligned"
        )
        windows = _as_mapping(state.get("opening_ranges"))
        capture_ratio = _finite_float(evidence.get("capture_ratio"))
        generations = provenance.get("subscription_generations")
        if not isinstance(generations, Sequence) or isinstance(
            generations, (str, bytes)
        ):
            generations = []
        warnings = state.get("warnings")
        if not isinstance(warnings, Sequence) or isinstance(warnings, (str, bytes)):
            warnings = []
        capture_status = str(state.get("capture_status") or "unavailable")
        if not state_identity_current:
            alignment = "context-only"
        elif capture_status in {
            "not_started",
            "awaiting_first_sample",
            "unavailable",
            "not_configured",
        }:
            alignment = "pending"
        elif provenance.get("current_vs_range_aligned") is True:
            alignment = "aligned"
        else:
            alignment = "reset/mixed"
        display_warnings = [str(item) for item in warnings]
        if not state_identity_current:
            display_warnings.append("ORB_V2_LIVE_CONTRACT_UNVERIFIED")
        rows.append(
            {
                "Symbol": str(symbol),
                "Capture": capture_status,
                "Provenance": alignment,
                "Reference": str(reference.get("kind") or "pending"),
                "ORB eligible": bool(
                    directional_visible
                ),
                "Pin alignment": str(
                    structure_status if state_identity_current else "context-only"
                ),
                "ORB + structure eligible": bool(
                    directional_visible
                    and structure_is_current
                    and state.get(
                        "combined_structure_directional_evidence_eligible"
                    ) is True
                ),
                "Samples": _nonnegative_int(evidence.get("sample_count")),
                "Coverage %": (
                    capture_ratio * 100.0 if capture_ratio is not None else None
                ),
                "Open": state.get("opening_price"),
                "ORB high": state.get("orb_high"),
                "ORB low": state.get("orb_low"),
                "Current": (
                    state.get("current_price") if current_reference_visible else None
                ),
                "5m ORB": _window_summary(
                    windows.get("5m"),
                    directional_allowed=bool(
                        current_reference_visible
                        and reference.get("directional_base_eligible") is True
                        and _as_mapping(windows.get("5m")).get(
                            "directional_evidence_eligible"
                        ) is True
                    ),
                ),
                "15m ORB": _window_summary(
                    windows.get("15m"),
                    directional_allowed=bool(
                        current_reference_visible
                        and reference.get("directional_base_eligible") is True
                        and _as_mapping(windows.get("15m")).get(
                            "directional_evidence_eligible"
                        ) is True
                    ),
                ),
                "30m ORB": _window_summary(
                    windows.get("30m"),
                    directional_allowed=bool(
                        current_reference_visible
                        and reference.get("directional_base_eligible") is True
                        and _as_mapping(windows.get("30m")).get(
                            "directional_evidence_eligible"
                        ) is True
                    ),
                ),
                "60m ORB": _window_summary(
                    windows.get("60m"),
                    directional_allowed=bool(
                        current_reference_visible
                        and reference.get("directional_base_eligible") is True
                        and _as_mapping(windows.get("60m")).get(
                            "directional_evidence_eligible"
                        ) is True
                    ),
                ),
                "Breakout": (
                    str(state.get("breakout_direction") or "unavailable")
                    if directional_visible
                    else "unavailable"
                ),
                "Gamma pin": pin.get("gamma_pin") if structure_is_current else None,
                "Pin move from open": (
                    pin.get("gamma_pin_change_from_open")
                    if structure_is_current
                    else None
                ),
                "Max pain": pin.get("max_pain") if structure_is_current else None,
                "Pain move from open": (
                    pin.get("max_pain_change_from_open")
                    if structure_is_current
                    else None
                ),
                "Generation": ", ".join(
                    str(value)
                    for value in generations
                ),
                "Epoch": _abbreviate_epoch(active_epoch),
                "Epoch full": active_epoch,
                "Epoch aligned": state_identity_current,
                "Live contract": (
                    "v2 current" if state_identity_current else "context only"
                ),
                "Latest source (UTC)": provenance.get("latest_source_timestamp_utc"),
                "Warnings": ", ".join(dict.fromkeys(display_warnings)),
            }
        )
    return rows


def render_opening_range_panel() -> None:
    """Render all configured markets plus explicit unavailable index canaries."""
    st.subheader("Opening Range & Pin Drift")
    st.caption(
        "5-, 15-, 30-, and 60-minute ranges begin at 09:30 ET. Prices are sampled "
        "from Databento OPRA put/call parity, not official exchange OHLC. VIX option "
        "parity is forward-like context and is never labeled as a spot breakout signal."
    )
    payload = fetch_opening_ranges()
    rows = opening_range_rows(payload)
    if not rows:
        failure = _as_mapping(payload.get(ORB_FETCH_FAILURE_KEY))
        if failure:
            reason = str(failure.get("reason") or "ORB_READ_UNAVAILABLE")
            status_code = failure.get("status_code")
            http_context = f"HTTP {status_code}; " if status_code is not None else ""
            st.error(
                "Opening-range evidence is unavailable because the bounded backend "
                f"read failed ({http_context}{reason}). No ORB values were inferred."
            )
            st.caption(
                "Use Refresh live status now after the backend read recovers. Retained "
                "or partial values remain non-directional context."
            )
            return
        st.error(
            "Opening-range evidence is unavailable: the backend endpoint could not "
            "provide usable symbol state."
        )
        return

    states = {row["Capture"] for row in rows}
    provenance_resets = any(row["Provenance"] == "reset/mixed" for row in rows)
    all_live_v2 = all(row["Live contract"] == "v2 current" for row in rows)
    if states == {"complete"} and not provenance_resets and all_live_v2:
        st.success("Opening ranges are complete for every displayed market.")
    elif not all_live_v2:
        st.warning(
            "Current ORB signals are withheld because at least one row lacks a stable, "
            "runtime-bound v2 process identity. Retained ranges are context only."
        )
    elif (
        "partial" in states
        or "unavailable" in states
        or "not_configured" in states
        or provenance_resets
    ):
        st.warning(
            "At least one opening range is partial, unavailable, not configured, or "
            "provenance-reset; "
            "those rows are not used as breakout signals."
        )
    elif "forming" in states or "awaiting_first_sample" in states:
        st.info("Opening ranges are forming; breakout classification remains disabled.")
    elif "not_started" in states:
        st.info("Opening-range capture has not started yet.")

    frame = pd.DataFrame(rows)
    st.dataframe(
        frame,
        width="stretch",
        hide_index=True,
        column_config={
            "Coverage %": st.column_config.ProgressColumn(
                "Coverage %",
                min_value=0.0,
                max_value=100.0,
                format="%.0f%%",
            ),
            "Open": st.column_config.NumberColumn(format="%.2f"),
            "ORB high": st.column_config.NumberColumn(format="%.2f"),
            "ORB low": st.column_config.NumberColumn(format="%.2f"),
            "Current": st.column_config.NumberColumn(format="%.2f"),
            "Gamma pin": st.column_config.NumberColumn(format="%.2f"),
            "Pin move from open": st.column_config.NumberColumn(format="%+.2f"),
            "Max pain": st.column_config.NumberColumn(format="%.2f"),
            "Pain move from open": st.column_config.NumberColumn(format="%+.2f"),
        },
    )
    st.caption(
        "RUT remains not configured unless its guarded canary is explicitly enabled and "
        "passes live coverage checks; missing ranges are never reconstructed from later prices. "
        "ORB eligible applies only to the sampled range. ORB + structure eligible also "
        "requires gamma/max-pain provenance to align with that reference. The abbreviated "
        "epoch is for scanning; Epoch full retains the complete process identity for audit."
    )
