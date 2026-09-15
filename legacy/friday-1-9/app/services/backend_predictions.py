"""Backend prediction client helpers for Streamlit."""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import requests

from app.utils.settings import settings


def fetch_live_prediction(symbol: str, timeout: int = 8) -> Tuple[Optional[float], Optional[float], Dict[str, Any], Optional[str]]:
    """Fetch live prediction from FastAPI backend."""
    base_url = settings.backend_base_url.rstrip("/")
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
    confidence_level = str(payload.get("confidence_level", "unknown")).lower()
    confidence_map = {"high": 85.0, "medium": 70.0, "low": 55.0, "unknown": 50.0}

    return (
        payload.get("predicted_close"),
        confidence_map.get(confidence_level, 50.0),
        payload,
        None,
    )
