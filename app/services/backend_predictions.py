"""Backend prediction client helpers for Streamlit."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests

from app.utils.settings import settings


def _parse_iso_timestamp(value: Any) -> Optional[datetime]:
    """Parse an ISO8601 timestamp into an aware datetime."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fetch_backend_health(base_url: str, symbol: str, timeout: int) -> Tuple[Dict[str, Any], Optional[str]]:
    """Fetch backend health so Streamlit can reject stale live data."""
    try:
        response = requests.get(f"{base_url}/health", timeout=max(2, min(timeout, 4)))
    except Exception as exc:
        return {}, f"Backend health unavailable: {exc}"

    if response.status_code != 200:
        return {}, f"Backend health failed: {response.text}"

    payload = response.json()
    symbol_status = (payload.get("symbols") or {}).get(symbol) or {}
    provider_status = payload.get("provider_status") or {}

    if payload.get("status") == "degraded" and not symbol_status.get("fresh", False):
        provider = payload.get("market_data_provider", "unknown")
        age = symbol_status.get("data_age_seconds")
        return payload, f"Backend data stale for {symbol} via {provider} (age={age}s)"

    last_success_at = _parse_iso_timestamp(provider_status.get("last_success_at"))
    if last_success_at is not None:
        provider_status["last_success_age_seconds"] = max(
            0.0,
            (datetime.now(timezone.utc) - last_success_at).total_seconds(),
        )
        payload["provider_status"] = provider_status

    return payload, None


def fetch_live_prediction(symbol: str, timeout: int = 8) -> Tuple[Optional[float], Optional[float], Dict[str, Any], Optional[str]]:
    """Fetch live prediction from FastAPI backend."""
    base_url = settings.backend_base_url.rstrip("/")
    health_payload, health_error = _fetch_backend_health(base_url, symbol, timeout)
    if health_error:
        return None, None, health_payload, health_error

    try:
        response = requests.get(
            f"{base_url}/predict/close",
            params={"symbol": symbol},
            timeout=timeout,
        )
    except Exception as exc:
        return None, None, {}, f"Backend unavailable: {exc}"

    if response.status_code != 200:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        return None, None, {}, f"Backend prediction failed: {detail}"

    payload = response.json()
    payload["market_data_provider"] = health_payload.get("market_data_provider")
    payload["provider_status"] = health_payload.get("provider_status") or {}
    payload["symbol_status"] = ((health_payload.get("symbols") or {}).get(symbol) or {})
    confidence_level = str(payload.get("confidence_level", "unknown")).lower()
    confidence_map = {"high": 85.0, "medium": 70.0, "low": 55.0, "unknown": 50.0}

    return (
        payload.get("predicted_close"),
        confidence_map.get(confidence_level, 50.0),
        payload,
        None,
    )
