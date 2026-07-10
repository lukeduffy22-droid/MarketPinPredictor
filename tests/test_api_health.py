"""Tests for stable health endpoint behavior."""

import asyncio
import sys
import types
from datetime import datetime, timezone

from app.api import main as api_main


class FakeRing:
    """Minimal ring buffer stand-in for health endpoint tests."""

    def __init__(self, fresh: bool, latest_ts: int, length_seconds: int) -> None:
        self._fresh = fresh
        self.q = [(latest_ts, object())]
        self.length_seconds = length_seconds

    def is_fresh(self, max_age_seconds: int = 5) -> bool:
        """Return the configured freshness value."""
        return self._fresh


def test_health_check_reports_degraded_during_regular_hours(monkeypatch):
    """Stale data during market hours should degrade health."""
    # Use a fixed weekday timestamp that maps to regular US market hours.
    fixed_now = datetime(2025, 7, 10, 19, 30, tzinfo=timezone.utc)
    rings = {
        symbol: FakeRing(fresh=False, latest_ts=100, length_seconds=0)
        for symbol in ("SPX", "NDX", "DJI", "RUT")
    }

    monkeypatch.setattr(api_main, "INDEX_RINGS", rings)
    monkeypatch.setattr(api_main, "get_latest_price", lambda symbol: None)
    monkeypatch.setattr(api_main, "is_regular_hours", lambda dt: True)
    monkeypatch.setattr(api_main, "now_et", lambda: fixed_now)
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.rest_fallback",
        types.SimpleNamespace(
            is_rest_only_mode=lambda: True,
            get_market_data_provider=lambda: "databento",
        ),
    )

    result = asyncio.run(api_main.health_check())

    assert result["status"] == "degraded"
    assert result["market_data_provider"] == "databento"
    assert all(symbol_status["mode"] == "REST" for symbol_status in result["symbols"].values())


def test_health_check_reports_ok_outside_regular_hours(monkeypatch):
    """Stale data after hours should not degrade health."""
    # Use the same weekday after market close to verify after-hours behavior.
    fixed_now = datetime(2025, 7, 10, 22, 0, tzinfo=timezone.utc)
    rings = {
        symbol: FakeRing(fresh=False, latest_ts=100, length_seconds=0)
        for symbol in ("SPX", "NDX", "DJI", "RUT")
    }

    monkeypatch.setattr(api_main, "INDEX_RINGS", rings)
    monkeypatch.setattr(api_main, "get_latest_price", lambda symbol: None)
    monkeypatch.setattr(api_main, "is_regular_hours", lambda dt: False)
    monkeypatch.setattr(api_main, "now_et", lambda: fixed_now)
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.rest_fallback",
        types.SimpleNamespace(
            is_rest_only_mode=lambda: False,
            get_market_data_provider=lambda: "polygon",
        ),
    )

    result = asyncio.run(api_main.health_check())

    assert result["status"] == "ok"
    assert result["market_data_provider"] == "polygon"
    assert all(symbol_status["mode"] == "WebSocket" for symbol_status in result["symbols"].values())
