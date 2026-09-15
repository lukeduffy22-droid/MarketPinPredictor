"""Trend research view for symbols with explicit trend metrics."""
from __future__ import annotations

import math
from datetime import datetime, timezone
import requests

RESEARCH_URL = "http://127.0.0.1:8001"


def analyze_trend(symbol: str, lookback_days: int = 30, *, requester=requests.get) -> dict:
    """Analyze trend direction, slope, and momentum.

    Returns a dict with keys:
    - trend_direction: "up", "down", or "sideways"
    - slope_pct: percentage slope over the lookback period
    - momentum_score: normalized momentum score (-1 to 1)
    """
    try:
        # Fetch historical data
        response = requester(
            RESEARCH_URL + "/v1/research/trend",
            params={"symbol": symbol, "lookback_days": lookback_days},
            timeout=3
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("schema_version") != "marketpin-market-research.v1":
            raise ValueError("Unknown research schema")

        # Extract trend data
        trend = payload.get("trend", {})
        prices = trend.get("prices", [])
        if len(prices) < 2:
            return {
                "trend_direction": "sideways",
                "slope_pct": 0.0,
                "momentum_score": 0.0
            }

        # Calculate slope (percent change)
        first = prices[0].get("close")
        last = prices[-1].get("close")
        if first is None or last is None:
            return {
                "trend_direction": "sideways",
                "slope_pct": 0.0,
                "momentum_score": 0.0
            }

        slope_pct = ((last - first) / first) * 100

        # Determine trend direction
        if slope_pct > 5:
            trend_direction = "up"
        elif slope_pct < -5:
            trend_direction = "down"
        else:
            trend_direction = "sideways"

        # Calculate momentum (normalized)
        momentum_score = min(max(slope_pct / 20, -1.0), 1.0)

        return {
            "trend_direction": trend_direction,
            "slope_pct": slope_pct,
            "momentum_score": momentum_score
        }
    except (requests.RequestException, ValueError, TypeError, AttributeError, KeyError):
        return {
            "trend_direction": "sideways",
            "slope_pct": 0.0,
            "momentum_score": 0.0
        }
