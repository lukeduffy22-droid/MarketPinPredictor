"""In-app advisor service for live market diagnostics and proposal generation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import requests

from app.services.live_data_client import (
    fetch_live_status_snapshot,
    live_pipeline_runtime_ready,
    live_pipeline_symbol_is_current,
)


def _safe_get(url: str, timeout: float = 2.0) -> dict[str, Any] | None:
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            payload = resp.json()
            if isinstance(payload, dict):
                return payload
    except Exception:
        return None
    return None


def fetch_advisor_context(symbols: list[str]) -> dict[str, Any]:
    """Fetch the current live context used to generate in-app advisor proposals."""
    requested_symbols = list(
        dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())
    )
    snapshot = fetch_live_status_snapshot(timeout_seconds=1.2)
    health_live = _safe_get("http://localhost:8000/health/live") or {}
    universe = _safe_get("http://localhost:8000/databento/universe") or {}

    dashboards: dict[str, dict[str, Any]] = {}
    for symbol in requested_symbols:
        payload = _safe_get(f"http://localhost:8000/dashboard/symbol/{symbol}")
        if payload is not None:
            dashboards[symbol] = payload

    pipeline = health_live or (snapshot or {}).get("pipeline") or {}

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "requested_symbols": requested_symbols,
        "snapshot": snapshot or {},
        "health": (snapshot or {}).get("health") or {},
        "pipeline": pipeline,
        "universe": universe or (snapshot or {}).get("universe") or {},
        "health_live": health_live,
        "dashboards": dashboards,
    }


def _advisor_live_eligibility(
    pipeline: Any,
    requested_symbols: list[str],
) -> tuple[bool, list[str], list[str]]:
    """Return current symbols and explicit reasons for advisor abstention."""

    normalized_symbols = list(
        dict.fromkeys(
            str(symbol).strip().upper()
            for symbol in requested_symbols
            if str(symbol).strip()
        )
    )
    current_symbols = [
        symbol
        for symbol in normalized_symbols
        if live_pipeline_symbol_is_current(pipeline, symbol)
    ]
    reasons: list[str] = []
    if not live_pipeline_runtime_ready(pipeline):
        if not isinstance(pipeline, dict) or pipeline.get("prediction_pipeline_ok") is not True:
            reasons.append("prediction pipeline is not ready")
        if not isinstance(pipeline, dict) or pipeline.get("handoff_status") != "active":
            reasons.append("stream handoff is not active")
        if isinstance(pipeline, dict) and (
            pipeline.get("epoch_mismatch_symbols")
            or pipeline.get("required_epoch_mismatch_symbols")
        ):
            reasons.append("subscription epoch mismatch is present")
        if isinstance(pipeline, dict) and (
            pipeline.get("generation_mismatch_symbols")
            or pipeline.get("required_generation_mismatch_symbols")
        ):
            reasons.append("subscription generation mismatch is present")
        if not reasons:
            reasons.append("current runtime identity is incomplete")
    if not normalized_symbols:
        reasons.append("no requested symbol evidence is available")
    else:
        ineligible = [
            symbol for symbol in normalized_symbols if symbol not in current_symbols
        ]
        if ineligible:
            reasons.append(
                "current symbol eligibility is missing for " + ", ".join(ineligible)
            )
    eligible = bool(
        normalized_symbols
        and live_pipeline_runtime_ready(pipeline)
        and len(current_symbols) == len(normalized_symbols)
    )
    return eligible, current_symbols, reasons


def build_advisor_report(context: dict[str, Any], previous_capabilities=None) -> dict[str, Any]:
    """Build a 3-tier recommendation set from live backend context."""
    health = context.get("health") or {}
    pipeline = context.get("pipeline") or {}
    universe = context.get("universe") or {}
    dashboards = context.get("dashboards") or {}
    requested_symbols = context.get("requested_symbols") or list(dashboards)

    websocket = str(health.get("websocket") or "unknown")
    buffer_health = str(health.get("buffer_health") or "unknown")
    symbols_subscribed = int(universe.get("symbols_subscribed") or health.get("symbols_subscribed") or 0)
    source = str((context.get("snapshot") or {}).get("source") or "poll").upper()
    last_tick_age_ms = pipeline.get("last_tick_age_ms")
    stale_symbols = pipeline.get("stale_symbols") or []
    invalid_symbols = health.get("invalid_symbols") or []

    dashboard_status_counts: dict[str, int] = {}
    for payload in dashboards.values():
        status = str(payload.get("status") or "unknown")
        dashboard_status_counts[status] = dashboard_status_counts.get(status, 0) + 1

    live_eligible, current_symbols, abstention_reasons = _advisor_live_eligibility(
        pipeline,
        requested_symbols,
    )
    # Contradictory health summaries must not produce a ready advisor header.
    if stale_symbols or invalid_symbols:
        live_eligible = False
        abstention_reasons.append('backend reports stale or invalid symbols; inspect per-symbol diagnostics')
    if websocket != 'active':
        live_eligible = False
        abstention_reasons.append('WebSocket state is ' + websocket)

    live_state = [
        (
            "Decision: CURRENT — active epoch/generation verified for "
            + ", ".join(current_symbols)
            if live_eligible
            else "Decision: ABSTAIN — " + "; ".join(abstention_reasons)
        ),
        f"Live source: {source}",
        f"WebSocket state: {websocket}",
        f"Buffer health: {buffer_health}",
        f"Subscribed contracts: {symbols_subscribed}",
    ]
    if last_tick_age_ms is not None:
        live_state.append(f"Last tick age: {float(last_tick_age_ms) / 1000.0:.1f}s")
    if stale_symbols:
        live_state.append(f"Stale symbols: {', '.join(stale_symbols)}")
    if invalid_symbols:
        live_state.append(f"Invalid symbols: {', '.join(invalid_symbols)}")
    if dashboard_status_counts:
        counts = ", ".join(f"{k}={v}" for k, v in sorted(dashboard_status_counts.items()))
        live_state.append(f"Dashboard statuses: {counts}")

    from app.services.advisor_workflow import inspect_capabilities, contextual_proposals, identity, proposal
    capabilities = inspect_capabilities()
    safe_quick_wins = contextual_proposals(context, capabilities)
    if previous_capabilities and previous_capabilities.get('fingerprint') != capabilities['fingerprint']:
        changed = [path for path, value in capabilities['source_hashes'].items()
                   if previous_capabilities.get('source_hashes', {}).get(path) != value]
        safe_quick_wins.append(proposal(
            'Verify changed advisor layout and functionality',
            'Check status visibility, retained-observation labels, and save/download behavior in the changed UI. Review source differences before proposing further edits.',
            'Source hashes changed: ' + ', '.join(changed), changed))
    medium_risk = []
    high_impact = []
    report_id = identity({"source": capabilities["fingerprint"], "context": context})

    return {
        "report_id": report_id,
        "capabilities": capabilities,
        "context_timestamp": context.get("timestamp_utc"),
        "live_state_heading": (
            "Current Live State" if live_eligible else "Live State Evidence — ABSTAIN"
        ),
        "live_state_eligible": live_eligible,
        "current_eligible_symbols": current_symbols,
        "live_state_abstention_reasons": abstention_reasons,
        "current_live_state": live_state,
        "safe_quick_wins": safe_quick_wins,
        "medium_risk_improvements": medium_risk,
        "high_impact_optional": high_impact,
        "approval_prompt": "Select proposals to approve for implementation planning (approval records choices only).",
    }
