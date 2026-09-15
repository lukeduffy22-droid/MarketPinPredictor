"""Render helpers for Streamlit live streaming status panel."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any

import pandas as pd
import requests
import streamlit as st
import numpy as np

from app.services.live_data_client import (
    fetch_closing_tape_readiness,
    fetch_closing_tape_status,
    fetch_promoted_closing_tape_predictions,
    fetch_live_status_snapshot,
    live_pipeline_runtime_ready,
    live_pipeline_symbol_is_current,
)


_CACHE_REFRESH_CONFIRMATION_KEY = "databento_cache_refresh_confirmation_pending"


def _snapshot_age_seconds(timestamp_utc: Any) -> float | None:
    if not isinstance(timestamp_utc, str) or not timestamp_utc:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp_utc.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds())


def _render_event_age_indicator(age_seconds: float | None) -> None:
    if age_seconds is None:
        st.caption("Event age: unknown")
        return
    if age_seconds <= 3:
        st.success(f"Event age: {age_seconds:.1f}s (live)")
    elif age_seconds <= 8:
        st.warning(f"Event age: {age_seconds:.1f}s (lagging)")
    else:
        st.error(f"Event age: {age_seconds:.1f}s (stale)")


def _pipeline_status_message(
    pipeline_ok: bool,
    valid_symbols: list[str],
) -> tuple[str, str]:
    """Return a current-runtime status without promoting retained symbols."""

    if pipeline_ok:
        return "success", "Prediction pipeline status: ready"
    return "warning", "Prediction pipeline status: ABSTAIN (current runtime is not eligible)"


def _current_valid_pin_symbols(
    pipeline: Any,
    valid_symbols: list[str],
) -> list[str]:
    """Filter retained producer keys through current runtime identity."""

    if not live_pipeline_runtime_ready(pipeline):
        return []
    return [
        str(symbol).strip().upper()
        for symbol in valid_symbols
        if live_pipeline_symbol_is_current(pipeline, str(symbol))
    ]


def _closing_tape_status_message(payload: dict[str, Any]) -> tuple[str, str]:
    if payload.get("transport_ok") is False:
        reason = str(
            payload.get("transport_message")
            or payload.get("transport_error")
            or (payload.get("reasons") or ["status request failed"])[0]
        )
        return "error", f"Capture status unavailable: {reason}"
    state = str(payload.get("state") or "unavailable").lower()
    if state == "complete" and payload.get("usable_for_research"):
        return "success", "Capture: complete and integrity-verified for research"
    if state == "running":
        return "info", "Capture: recording; final integrity is not yet known"
    reasons = payload.get("reasons") or []
    reason = str(reasons[0]) if reasons else "capture evidence is unavailable"
    return "warning", f"Capture: {state} ({reason})"


def _closing_tape_analysis_message(payload: dict[str, Any]) -> tuple[str, str]:
    """Describe forecast-analysis eligibility independently from raw capture."""

    if payload.get("transport_ok") is False:
        return (
            "error",
            "Close-minus-15 analysis: unknown because capture status could not be read",
        )

    session = payload.get("session") if isinstance(payload.get("session"), dict) else {}
    analysis = payload.get("analysis") if isinstance(payload.get("analysis"), dict) else {}
    decision_state = str(analysis.get("decision_state") or "").strip().lower()
    abstention_reasons = [
        str(reason)
        for reason in (analysis.get("abstention_reasons") or [])
        if str(reason).strip()
    ]
    session_error = str(session.get("error") or "").strip()

    if abstention_reasons or session_error:
        reason = (abstention_reasons or [session_error])[0]
        return "warning", f"Close-minus-15 analysis: abstained ({reason[:240]})"
    if decision_state in {
        "valid",
        "eligible",
        "available",
        "complete",
        "ready",
        "validated_research",
    }:
        return "success", "Close-minus-15 analysis: eligible research estimate recorded"
    if decision_state == "abstain":
        return "warning", "Close-minus-15 analysis: abstained (no eligible estimate)"
    if decision_state == "research_only":
        return "info", "Close-minus-15 analysis: recorded as research only"
    if analysis:
        label = decision_state or "recorded"
        return "info", f"Close-minus-15 analysis: {label}"
    if str(session.get("status") or "").lower() == "running":
        return "info", "Close-minus-15 analysis: pending while capture is running"
    return "info", "Close-minus-15 analysis: no eligible estimate is available"


def _evidence_progress_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "label": "Clean capture gate",
            "current": int(payload.get("unique_verified_sources") or 0),
            "required": int(payload.get("capture_gate_sessions_required") or 10),
            "ready": bool(payload.get("capture_gate_ready")),
        },
        {
            "label": "Labeled model-evidence gate",
            "current": int(payload.get("model_evidence_sessions") or 0),
            "required": int(payload.get("model_sessions_required") or 60),
            "ready": bool(payload.get("model_training_ready")),
        },
        {
            "label": "Paper/verified-close gate",
            "current": int(payload.get("paper_forecast_sessions") or 0),
            "required": int(payload.get("paper_sessions_required") or 20),
            "ready": bool(payload.get("paper_evidence_ready")),
        },
    ]


def _readiness_inventory_warning(payload: dict[str, Any]) -> str | None:
    unavailable = str(payload.get("unavailable_reason") or "").strip()
    if unavailable:
        return unavailable
    issues = tuple(payload.get("catalog_issues") or ())
    if not issues:
        return None
    return (
        "Catalog inventory is incomplete; readiness gates are disabled: "
        + "; ".join(map(str, issues[:3]))
        + ("; …" if len(issues) > 3 else "")
    )


def _tape_family_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Join observed facts and inference estimates without merging their semantics."""
    observed = payload.get("observed") or {}
    inferred = payload.get("inferred") or {}
    observed_rows = {
        str(row.get("family_root") or ""): row
        for row in (observed.get("by_family") or [])
        if isinstance(row, dict) and row.get("family_root")
    }
    inferred_rows = {
        str(row.get("family_root") or ""): row
        for row in (inferred.get("by_family") or [])
        if isinstance(row, dict) and row.get("family_root")
    }
    rows: list[dict[str, Any]] = []
    for family in sorted(set(observed_rows) | set(inferred_rows)):
        observed_row = observed_rows.get(family, {})
        inferred_row = inferred_rows.get(family, {})
        trades = int(observed_row.get("trade_records") or 0)
        valid_nbbo = int(observed_row.get("valid_pretrade_nbbo_records") or 0)
        at_ask = int(inferred_row.get("at_ask_count") or 0)
        at_bid = int(inferred_row.get("at_bid_count") or 0)
        inside = int(inferred_row.get("inside_count") or 0)
        unknown = int(inferred_row.get("unknown_count") or 0)
        classified_total = at_ask + at_bid + inside + unknown
        rows.append(
            {
                "Family": family,
                "Observed trades": trades,
                "Observed volume": float(observed_row.get("volume") or 0.0),
                "NBBO coverage": valid_nbbo / trades if trades else None,
                "Estimate alignment": classified_total / trades if trades else None,
                "At-ask estimate share": at_ask / classified_total if classified_total else None,
                "At-bid estimate share": at_bid / classified_total if classified_total else None,
                "Inside/unknown share": (
                    (inside + unknown) / classified_total if classified_total else None
                ),
                "At-ask minus bid estimate": (
                    (at_ask - at_bid) / classified_total if classified_total else None
                ),
                "Quality-flagged records": int(
                    observed_row.get("data_quality_flagged_records") or 0
                ),
                "Latest minute (UTC)": observed_row.get("last_minute_utc"),
            }
        )
    return rows


def _promoted_prediction_view(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate immutable promoted output before it reaches presentation."""
    if payload.get("available") is False:
        if (
            payload.get("transport_ok") is True
            and payload.get("read_succeeded") is True
        ):
            return {
                "state": "empty",
                "rows": [],
                "message": str(
                    payload.get("reason")
                    or "No promoted TCBBO close estimate exists; research gates remain active."
                ),
            }
        return {
            "state": "unavailable", "rows": [],
            "message": str(
                payload.get("reason")
                or payload.get("transport_message")
                or "promoted estimate evidence could not be read"
            ),
        }
    rows = payload.get("rows")
    if not isinstance(rows, list) or not rows:
        return {
            "state": "empty", "rows": [],
            "message": "No promoted TCBBO close estimates exist; the incumbent remains active.",
        }
    expected = {"SPX", "NDX", "RUT", "VIX", "SPY"}
    roots = {str(row.get("family_root") or "") for row in rows if isinstance(row, dict)}
    required = {
        "reference_price", "predicted_level", "feature_available_at_utc",
        "prediction_lower", "prediction_upper", "interval_target_coverage",
        "calibration_method", "calibration_evidence_sha256",
        "recorded_at_utc", "source_sha256", "model_version", "artifact_sha256",
        "execution_device", "prediction_mode", "is_estimate", "trading_date",
    }
    if (
        len(rows) != 5 or roots != expected
        or any(not isinstance(row, dict) or not required <= set(row) for row in rows)
        or any(row.get("prediction_mode") != "tcbbo_promoted" or row.get("is_estimate") is not True for row in rows)
    ):
        return {
            "state": "invalid", "rows": [],
            "message": "Promoted prediction evidence is partial or malformed and was hidden.",
        }
    provenance_fields = (
        "trading_date", "source_sha256", "model_version", "artifact_sha256",
        "calibration_evidence_sha256", "execution_device",
        "feature_available_at_utc", "recorded_at_utc",
    )
    if any(len({str(row[field]) for row in rows}) != 1 for field in provenance_fields):
        return {
            "state": "invalid", "rows": [],
            "message": "Promoted prediction batch mixes provenance and was hidden.",
        }
    display_rows = []
    try:
        for row in sorted(rows, key=lambda item: str(item["family_root"])):
            reference = float(row["reference_price"])
            predicted = float(row["predicted_level"])
            lower = float(row["prediction_lower"])
            upper = float(row["prediction_upper"])
            coverage = float(row["interval_target_coverage"])
            if (
                not np.isfinite([reference, predicted, lower, upper, coverage]).all()
                or not 0 < lower <= predicted <= upper or not 0 < coverage < 1
            ):
                raise ValueError("invalid price")
            display_rows.append(
                {
                    "Family": str(row["family_root"]),
                    "Reference": reference,
                    "Promoted estimate": predicted,
                    "Calibrated lower": lower,
                    "Calibrated upper": upper,
                    "Target coverage": coverage,
                    "Estimated move %": (predicted / reference - 1.0) * 100.0,
                    "Feature time (UTC)": str(row["feature_available_at_utc"]),
                    "Model": str(row["model_version"]),
                    "Device": str(row["execution_device"]).upper(),
                }
            )
    except (TypeError, ValueError):
        return {
            "state": "invalid", "rows": [],
            "message": "Promoted prediction prices are invalid and were hidden.",
        }
    first = rows[0]
    calibration_evidence_hash = str(first["calibration_evidence_sha256"]).lower()
    if not re.fullmatch(r"[0-9a-f]{64}", calibration_evidence_hash):
        return {
            "state": "invalid", "rows": [],
            "message": "Promoted calibration evidence identity is invalid and was hidden.",
        }
    return {
        "state": "available", "rows": display_rows,
        "message": "Evidence-promoted TCBBO close estimates (analytical estimates, not guarantees).",
        "trading_date": str(first["trading_date"]),
        "source_sha256": str(first["source_sha256"]),
        "artifact_sha256": str(first["artifact_sha256"]),
        "recorded_at_utc": str(first["recorded_at_utc"]),
        "calibration_method": str(first["calibration_method"]),
        "calibration_evidence_sha256": calibration_evidence_hash,
        "target_coverage": float(first["interval_target_coverage"]),
    }


@st.cache_data(ttl=60, show_spinner=False)
def _cached_closing_tape_readiness() -> dict[str, Any]:
    return fetch_closing_tape_readiness()


@st.cache_data(ttl=15, show_spinner=False)
def _cached_closing_tape_status() -> dict[str, Any]:
    return fetch_closing_tape_status()


@st.cache_data(ttl=15, show_spinner=False)
def _cached_promoted_closing_tape_predictions() -> dict[str, Any]:
    return fetch_promoted_closing_tape_predictions()


def render_closing_tape_evidence() -> None:
    """Render the full-width observed/inferred evidence and promotion surface."""
    with st.expander("Observed OPRA Tape and Inferred Flow", expanded=False):
        load_evidence = st.toggle(
            "Load retained TCBBO evidence",
            value=False,
            key="load_closing_tape_evidence",
            help=(
                "Loads read-only capture, inference, readiness, and promotion evidence. "
                "Leave off when this section is not being inspected."
            ),
        )
        if not load_evidence:
            st.caption(
                "Evidence loading is paused. Enable it to inspect immutable observed records, "
                "explicitly inferred flow, and research-promotion gates."
            )
            return
        if st.button(
            "Refresh retained TCBBO evidence",
            key="refresh_closing_tape_evidence",
            help="Refresh read-only capture, inference, and promotion evidence.",
        ):
            _cached_closing_tape_status.clear()
            _cached_closing_tape_readiness.clear()
            _cached_promoted_closing_tape_predictions.clear()

        payload = _cached_closing_tape_status()
        readiness = _cached_closing_tape_readiness()
        promoted = _promoted_prediction_view(
            _cached_promoted_closing_tape_predictions()
        )
        level, message = _closing_tape_status_message(payload)
        getattr(st, level)(message)
        analysis_level, analysis_message = _closing_tape_analysis_message(payload)
        getattr(st, analysis_level)(analysis_message)
        observed = payload.get("observed") or {}
        inferred = payload.get("inferred") or {}
        model_gate = payload.get("model_gate") or {}
        close_evidence = payload.get("close_evidence") or {}
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            raw_feed = observed.get("raw_feed") or {}
            value = raw_feed.get("trade_records") if observed.get("available") else None
            st.metric("Observed trades", f"{int(value):,}" if value is not None else "Unavailable")
        with col2:
            value = observed.get("minute_rows") if observed.get("available") else None
            st.metric("Observed minutes", f"{int(value):,}" if value is not None else "Unavailable")
        with col3:
            value = inferred.get("minute_rows") if inferred.get("available") else None
            st.metric("Inferred minutes", f"{int(value):,}" if value is not None else "Unavailable")
        with col4:
            value = observed.get("open_interest_rows") if observed.get("available") else None
            st.metric("Daily OI rows", f"{int(value):,}" if value is not None else "Unavailable")
        with col5:
            value = (
                observed.get("instrument_definition_observations")
                if observed.get("available") else None
            )
            st.metric("Observed definitions", f"{int(value):,}" if value is not None else "Unavailable")
        st.caption(
            "Observed = provider DBN records and reproducible arithmetic aggregates. "
            "Inferred = trade-price-versus-pre-trade-NBBO estimates; OPRA does not publish "
            "aggressor side or participant holdings."
        )
        tcbbo = observed.get("tcbbo_evidence") or {}
        if tcbbo.get("finalized"):
            records = int(tcbbo.get("records") or 0)
            valid_nbbo = int(tcbbo.get("valid_pretrade_nbbo_records") or 0)
            coverage = (100.0 * valid_nbbo / records) if records else 0.0
            st.caption(
                f"Finalized TCBBO evidence: {records:,} records; "
                f"{coverage:.2f}% valid pre-trade NBBO coverage; "
                f"actions {tcbbo.get('action_counts') or {}}. "
                "Flags and actions are observed provider fields, not side inference."
            )
        else:
            st.caption(
                "TCBBO timestamps, pre-trade NBBO, flags, and actions remain unfinalized "
                "until the DBN file closes and passes integrity inspection."
            )
        if int(observed.get("open_interest_observations") or 0) > 0:
            st.caption(
                f"Immutable daily OI updates: "
                f"{int(observed.get('open_interest_observations') or 0):,}; "
                f"latest-state projection: {int(observed.get('open_interest_rows') or 0):,} rows "
                f"across {int(observed.get('open_interest_families') or 0):,} families."
            )
        if int(observed.get("instrument_definition_observations") or 0) > 0:
            st.caption(
                "Immutable instrument definitions: "
                f"{int(observed.get('instrument_definition_observations') or 0):,}; "
                f"provider update actions {observed.get('instrument_definition_actions') or {}}. "
                "Each row retains its exact source DBN bytes, offset, and hashes."
            )
        if close_evidence.get("available"):
            st.caption(
                f"Verified production close evidence: {int(close_evidence.get('rows') or 0):,} observations "
                f"across {', '.join(close_evidence.get('families') or [])}."
            )
        else:
            st.caption(
                "Verified production close evidence: unavailable ("
                + str(close_evidence.get("reason") or "no verified observations")
                + ")."
            )
        methods = inferred.get("methods") or []
        if methods:
            st.caption(f"Inference method/version: {', '.join(map(str, methods))}")
        family_rows = _tape_family_rows(payload)
        if family_rows:
            st.markdown("**Live family tape coverage**")
            st.dataframe(
                pd.DataFrame(family_rows),
                hide_index=True,
                width="stretch",
                column_config={
                    "Observed trades": st.column_config.NumberColumn(format="%d"),
                    "Observed volume": st.column_config.NumberColumn(format="%.0f"),
                    "NBBO coverage": st.column_config.NumberColumn(
                        format="percent",
                        help="Observed share of trades retaining a valid pre-trade national best bid and offer.",
                    ),
                    "At-ask estimate share": st.column_config.NumberColumn(
                        format="percent",
                        help="Estimated from trade price versus pre-trade NBBO; not published aggressor side.",
                    ),
                    "At-bid estimate share": st.column_config.NumberColumn(
                        format="percent",
                        help="Estimated from trade price versus pre-trade NBBO; not published aggressor side.",
                    ),
                    "Inside/unknown share": st.column_config.NumberColumn(format="percent"),
                    "At-ask minus bid estimate": st.column_config.NumberColumn(
                        format="percent",
                        help="At-ask share minus at-bid share; an estimate, not participant positioning.",
                    ),
                    "Estimate alignment": st.column_config.NumberColumn(
                        format="percent",
                        help="Share of observed trades assigned to exactly one inferred location bucket; must be 100%.",
                    ),
                    "Quality-flagged records": st.column_config.NumberColumn(format="%d"),
                },
            )
            st.caption(
                "Observed trades, volume, NBBO coverage, and quality flags are reproducible facts. "
                "At-ask, at-bid, inside, and unknown shares are price-location estimates only; "
                "they are not published aggressor side or participant positioning."
            )
            alignment = inferred.get("classification_alignment") or {}
            if alignment.get("passed") is False:
                st.warning(
                    "Inferred location coverage is not aligned to observed trades; "
                    "the affected estimates are not research-usable."
                )
        reference_estimates = inferred.get("reference_estimates") or {}
        if reference_estimates.get("available"):
            st.caption(
                f"Inferred reference estimates: {int(reference_estimates.get('rows') or 0):,} rows "
                f"across {int(reference_estimates.get('families') or 0):,} families; "
                f"methods {', '.join(map(str, reference_estimates.get('methods') or []))}. "
                "These are calculated parity estimates, not observed underlying prints."
            )
        if model_gate.get("passed"):
            st.success("Frozen-model promotion gate passed; outputs remain decision support, not guarantees.")
        else:
            st.warning(
                "Prediction model gate: research only. "
                + str(model_gate.get("reason") or "Required out-of-sample evidence is unavailable.")
            )
        st.markdown("**Promoted TCBBO close estimates**")
        if promoted["state"] == "available":
            st.success(str(promoted["message"]))
            st.dataframe(
                pd.DataFrame(promoted["rows"]), hide_index=True, width="stretch",
                column_config={
                    "Reference": st.column_config.NumberColumn(format="%.2f"),
                    "Promoted estimate": st.column_config.NumberColumn(format="%.2f"),
                    "Calibrated lower": st.column_config.NumberColumn(format="%.2f"),
                    "Calibrated upper": st.column_config.NumberColumn(format="%.2f"),
                    "Target coverage": st.column_config.NumberColumn(format="percent"),
                    "Estimated move %": st.column_config.NumberColumn(format="%+.3f%%"),
                },
            )
            st.caption(
                f"Recorded {promoted['recorded_at_utc']} for {promoted['trading_date']}; "
                f"source DBN SHA-256 {promoted['source_sha256']}; "
                f"artifact SHA-256 {promoted['artifact_sha256']}. "
                f"Intervals target {promoted['target_coverage']:.0%} coverage using "
                f"{promoted['calibration_method']} from OOS evidence "
                f"{promoted['calibration_evidence_sha256']}; empirical coverage can differ. "
                "No trade instruction or guaranteed close is implied."
            )
        elif promoted["state"] == "empty":
            st.info(str(promoted["message"]))
        else:
            st.warning(str(promoted["message"]))
        st.markdown("**Evidence accumulation**")
        readiness_warning = _readiness_inventory_warning(readiness)
        readiness_unavailable = readiness_warning is not None
        if readiness_warning is not None:
            st.warning(readiness_warning)
        else:
            for row in _evidence_progress_rows(readiness):
                required = max(1, int(row["required"]))
                current = int(row["current"])
                st.caption(
                    f"{row['label']}: {current}/{required} independent sessions"
                    + (" — passed" if row["ready"] else " — not yet passed")
                )
                st.progress(min(1.0, current / required))
            st.caption(
                f"Retained catalogs: {int(readiness.get('catalogs') or 0):,}; "
                f"catalog sessions: {int(readiness.get('sessions') or 0):,}; "
                f"currently eligible: {int(readiness.get('eligible_sessions') or 0):,}."
            )
        cache_state = str(readiness.get("readiness_cache_state") or "unknown")
        if readiness.get("generated_at_utc"):
            st.caption(
                f"Readiness evidence scanned at {readiness['generated_at_utc']} "
                f"(evidence age {float(readiness.get('cache_age_seconds') or 0.0):.1f}s; "
                f"cache state {cache_state})."
            )
        excluded = [
            row for row in (readiness.get("sessions_detail") or [])
            if not row.get("eligible")
        ]
        if excluded:
            latest = excluded[-1]
            reasons = latest.get("reasons") or ["evidence contract is incomplete"]
            st.caption(
                f"Latest session {latest.get('session_id') or 'unknown'} is not training-eligible: "
                + "; ".join(map(str, reasons[:3]))
                + ("; …" if len(reasons) > 3 else "")
            )


def _request_databento_cache_refresh():
    """Submit a confirmed non-forced cache-maintenance request."""

    return requests.post(
        "http://localhost:8000/databento/refresh-cache",
        json={"confirm": True},
        timeout=5,
    )


def _render_databento_cache_maintenance() -> None:
    """Render a two-step cache deletion flow that never requests force mode."""

    pending = bool(st.session_state.get(_CACHE_REFRESH_CONFIRMATION_KEY, False))
    if not pending:
        if st.button(
            "🧹 Review Databento Cache Maintenance",
            type="secondary",
            help="Review the destructive action and readiness impact before confirming.",
        ):
            st.session_state[_CACHE_REFRESH_CONFIRMATION_KEY] = True
            pending = True

    if not pending:
        return

    st.error(
        "Destructive maintenance: this deletes the current-day OPRA universe cache. "
        "The API will refuse the request while the Databento streamer is active."
    )
    st.warning(
        "After an approved deletion, restart the backend and verify subscription coverage, "
        "fresh quotes, and prediction readiness before relying on dashboard values. "
        "This dashboard never sends the force-maintenance override."
    )

    confirm_clicked = st.button(
        "⚠️ Confirm Cache Deletion",
        type="secondary",
        help="Allowed only when the Databento streamer is inactive.",
    )
    cancel_clicked = st.button("Cancel Cache Maintenance", type="secondary")
    if cancel_clicked:
        st.session_state[_CACHE_REFRESH_CONFIRMATION_KEY] = False
        st.info("Cache maintenance cancelled; no request was sent.")
        return
    if not confirm_clicked:
        return

    # Reset before the request so every later attempt requires a fresh first step.
    st.session_state[_CACHE_REFRESH_CONFIRMATION_KEY] = False
    try:
        refresh_resp = _request_databento_cache_refresh()
        if refresh_resp.status_code == 200:
            st.success(
                refresh_resp.json().get(
                    "message",
                    "Cache cleared. Restart backend and verify readiness.",
                )
            )
        else:
            try:
                detail = refresh_resp.json().get("detail")
            except (AttributeError, ValueError):
                detail = None
            st.warning(str(detail or refresh_resp.text)[:240])
    except Exception as exc:
        st.error(f"Could not clear cache: {str(exc)[:80]}")


def render_backend_streaming_status() -> None:
    """Render backend streaming diagnostics using SSE-first unified snapshots."""
    st.subheader("Backend Streaming Status")

    try:
        live_snapshot = fetch_live_status_snapshot(timeout_seconds=1.2)
        health_data = (live_snapshot or {}).get("health") or {}
        universe_data = (live_snapshot or {}).get("universe") or {}
        pipeline_data = (live_snapshot or {}).get("pipeline") or {}
        snapshot_source = str((live_snapshot or {}).get("source") or "poll")
        snapshot_age = _snapshot_age_seconds((live_snapshot or {}).get("timestamp_utc"))

        if not health_data:
            st.warning("Backend not responding")
            return

        st.caption(f"Live status source: {snapshot_source.upper()}")
        _render_event_age_indicator(snapshot_age)

        ws_status = health_data.get("websocket", "unknown")
        last_error = health_data.get("last_error")

        status_col1, status_col2, status_col3, status_col4 = st.columns(4)
        with status_col1:
            if ws_status == "active":
                st.success("WebSocket: Connected")
            elif ws_status == "stopped":
                st.warning("Stream: stopped")
            else:
                st.info(f"WebSocket: {ws_status}")
        with status_col2:
            st.metric("Buffer Status", health_data.get("buffer_health", "unknown"))
        with status_col3:
            st.metric("Provider", health_data.get("market_data_provider", health_data.get("provider", "unknown")))
        with status_col4:
            st.metric("Quotes", health_data.get("quotes_cached", 0))

        detail_col1, detail_col2, detail_col3 = st.columns(3)
        with detail_col1:
            st.caption(f"Symbols subscribed: {health_data.get('symbols_subscribed', 0)}")
        with detail_col2:
            age = health_data.get("data_age_seconds")
            if ws_status != "active":
                st.caption("Data age: unavailable")
            elif age is None:
                st.caption("Data age: warming")
            else:
                st.caption(f"Data age: {float(age):.1f}s")
                if float(age) > 30:
                    st.warning("Databento cache is stale")
        with detail_col3:
            st.caption(f"Schema: {health_data.get('schema', 'N/A')}")

        if ws_status != "active":
            if last_error:
                st.warning(f"Databento stream unavailable: {last_error}")
                st.caption("If today is a U.S. market holiday or OPRA is closed, live pins will resume on the next trading session.")
            else:
                st.warning("Databento stream is stopped. Live OPRA may be closed or unavailable.")

        valid_symbols = health_data.get("valid_symbols") or []
        invalid_symbols = health_data.get("invalid_symbols") or []
        invalid_reasons = health_data.get("invalid_reasons") or {}
        current_valid_symbols = _current_valid_pin_symbols(
            pipeline_data,
            valid_symbols,
        )
        current_pipeline_ready = bool(current_valid_symbols) and live_pipeline_runtime_ready(
            pipeline_data
        )

        if pipeline_data:
            stale_symbols = pipeline_data.get("stale_symbols") or []
            last_tick_age_ms = pipeline_data.get("last_tick_age_ms")
            status_level, status_message = _pipeline_status_message(
                current_pipeline_ready,
                current_valid_symbols,
            )
            getattr(st, status_level)(status_message)
            if last_tick_age_ms is not None:
                st.caption(f"Last tick age: {float(last_tick_age_ms) / 1000.0:.1f}s")
            if stale_symbols:
                st.caption(f"Stale symbols: {', '.join(stale_symbols)}")
            required_symbols = pipeline_data.get("required_symbols") or []
            optional_degraded = pipeline_data.get("optional_degraded_symbols") or []
            if required_symbols:
                st.caption(f"Required prediction symbols: {', '.join(required_symbols)}")
            if optional_degraded:
                st.caption(
                    "Optional context degraded (does not veto required predictions): "
                    + ", ".join(optional_degraded)
                )

        subscribed_symbols = universe_data.get("subscribed_symbols") or health_data.get("subscribed_symbols") or []
        core_status = universe_data.get("core_symbol_status") or health_data.get("core_symbol_status") or {}
        root_contract_counts = universe_data.get("root_contract_counts") or health_data.get("root_contract_counts") or {}

        if current_valid_symbols:
            st.success(f"Current Databento pins: {', '.join(current_valid_symbols)}")
        elif valid_symbols:
            st.warning(
                "Retained Databento pin context only; current epoch/generation "
                f"eligibility is not established: {', '.join(valid_symbols)}"
            )
        else:
            if ws_status == "active":
                st.info("Current Databento pins: warming")
            else:
                st.info("Current Databento pins: unavailable until live OPRA data resumes")

        if invalid_symbols:
            with st.expander(f"Experimental/insufficient Databento symbols ({len(invalid_symbols)})", expanded=False):
                for symbol in invalid_symbols:
                    st.caption(f"{symbol}: {invalid_reasons.get(symbol, 'No valid pin yet')}")

        if core_status:
            st.markdown("**Core roots coverage (auto-tracked):**")
            core_rows = []
            for core in ("SPX", "NDX", "VIX", "RUT"):
                row = core_status.get(core, {})
                core_rows.append(
                    {
                        "Core": core,
                        "Requested": bool(row.get("requested")),
                        "Contracts Subscribed": int(row.get("contracts_subscribed") or 0),
                        "Aliases": ", ".join(row.get("aliases") or []),
                    }
                )
            st.dataframe(pd.DataFrame(core_rows), width="stretch", hide_index=True)

        if root_contract_counts:
            root_rows = [
                {"Root": root, "Contracts": int(count)}
                for root, count in sorted(root_contract_counts.items(), key=lambda item: item[1], reverse=True)
            ]
            with st.expander(f"Root contract counts ({len(root_rows)} roots)", expanded=False):
                st.dataframe(pd.DataFrame(root_rows), width="stretch", hide_index=True)

        if subscribed_symbols:
            with st.expander(f"All subscribed Databento symbols ({len(subscribed_symbols)})", expanded=False):
                pinned_core = [
                    s
                    for s in subscribed_symbols
                    if str(s).split()[0].strip().upper()
                    in {"SPXW", "SPX", "NDXP", "NDX", "VIXW", "VIX", "RUTW", "RUT", "MRUT"}
                ]
                if pinned_core:
                    st.markdown("**Pinned core-related contracts first:**")
                    st.code("\n".join(pinned_core[:150]), language="text")
                st.markdown("**Full subscription universe:**")
                st.code("\n".join(subscribed_symbols), language="text")

        _render_databento_cache_maintenance()
    except Exception as exc:
        st.error(f"Cannot reach backend: {str(exc)[:50]}")
