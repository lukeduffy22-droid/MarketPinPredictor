"""Shared mutable API runtime state."""

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime, timezone

from app.utils.settings import settings


log = logging.getLogger("live_model_monitor")

last_prediction_ts = {s: 0.0 for s in ("SPX", "NDX", "DJI", "RUT")}
coefficients_cache = {}
model_metadata_cache = {}
_anomaly_lock = threading.Lock()
_last_anomaly_state = {}


def get_live_model_readiness(symbol: str, now_ts: float = None) -> dict:
    """Return whether every required live model input is trustworthy."""
    from app.ingest.options_websocket_stream import (
        get_options_data_provider,
        is_options_websocket_active,
    )
    from app.state.oi_cache import oi_cache
    from app.state.ring_buffers import FLOW_RINGS

    current_ts = time.time() if now_ts is None else now_ts
    metadata = model_metadata_cache.get(symbol, {})
    coefficients = coefficients_cache.get(symbol, {})
    gamma_coefficient_active = abs(
        float(coefficients.get("beta_gamma", 0.0) or 0.0)
    ) > 1e-12
    flow_coefficient_active = abs(
        float(coefficients.get("beta_flow", 0.0) or 0.0)
    ) > 1e-12
    coefficient_ready = (
        metadata.get("coefficients_source") == "database"
        and int(metadata.get("coefficients_sample_size") or 0) > 0
        and gamma_coefficient_active
        and flow_coefficient_active
    )

    oi_status = oi_cache.get_status(
        symbol,
        max_age_minutes=settings.max_oi_age_minutes,
    )
    options_provider = get_options_data_provider()
    options_stream_active = is_options_websocket_active()

    flow_ring = FLOW_RINGS.get(symbol)
    latest_flow = flow_ring.latest() if flow_ring else None
    flow_age_seconds = (
        max(0.0, current_ts - latest_flow[0])
        if latest_flow
        else None
    )
    flow_ready = (
        flow_age_seconds is not None
        and flow_age_seconds <= settings.max_options_flow_age_seconds
    )

    issues = []
    if not coefficient_ready:
        issues.append("calibrated_coefficients_unavailable")
    if not gamma_coefficient_active:
        issues.append("gamma_coefficient_inactive")
    if not flow_coefficient_active:
        issues.append("flow_coefficient_inactive")
    if options_provider != "polygon":
        issues.append("live_options_provider_unavailable")
    elif not options_stream_active:
        issues.append("options_stream_inactive")
    if oi_status["simulated"]:
        issues.append("simulated_oi_rejected")
    elif not oi_status["fresh"] or oi_status["strike_count"] == 0:
        issues.append("live_oi_unavailable")
    if not flow_ready:
        issues.append("live_options_flow_stale")

    return {
        "ready": not issues,
        "issues": issues,
        "coefficients": {
            "ready": coefficient_ready,
            "source": metadata.get("coefficients_source", "missing"),
            "sample_size": int(metadata.get("coefficients_sample_size") or 0),
            "gamma_active": gamma_coefficient_active,
            "flow_active": flow_coefficient_active,
        },
        "oi": oi_status,
        "options": {
            "provider": options_provider,
            "stream_active": options_stream_active,
            "flow_fresh": flow_ready,
            "flow_age_seconds": flow_age_seconds,
        },
    }


def record_runtime_anomaly(symbol: str, readiness: dict) -> None:
    """Append a deduplicated model-readiness anomaly or recovery event."""
    issues = tuple(readiness.get("issues", ()))
    previous_issues, last_recorded = _last_anomaly_state.get(
        symbol,
        (None, 0.0),
    )
    now_ts = time.time()
    heartbeat_due = bool(issues) and now_ts - last_recorded >= 300
    if issues == previous_issues and not heartbeat_due:
        return

    event_type = "live_model_anomaly" if issues else "live_model_recovered"
    payload = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "symbol": symbol,
        "issues": list(issues),
        "readiness": readiness,
    }
    log_dir = settings.live_anomaly_log_dir
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(
        log_dir,
        f"{datetime.now(timezone.utc).date().isoformat()}.ndjson",
    )
    with _anomaly_lock:
        with open(log_path, "a", encoding="utf-8") as anomaly_file:
            anomaly_file.write(json.dumps(payload, sort_keys=True) + "\n")
    _last_anomaly_state[symbol] = (issues, now_ts)
    log_method = log.error if issues else log.info
    log_method("%s for %s: %s", event_type, symbol, list(issues))


async def monitor_live_model_readiness() -> None:
    """Continuously audit and persist live model readiness during market hours."""
    from app.utils.time_et import is_regular_hours

    while True:
        try:
            if is_regular_hours(datetime.utcnow()):
                for symbol in last_prediction_ts:
                    readiness = get_live_model_readiness(symbol)
                    record_runtime_anomaly(symbol, readiness)
        except Exception:
            log.exception("Live model readiness monitor failed")
        await asyncio.sleep(settings.live_monitor_interval_seconds)
