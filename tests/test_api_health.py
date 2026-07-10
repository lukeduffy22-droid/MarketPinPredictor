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
    fixed_now = datetime(2026, 7, 10, 19, 30, tzinfo=timezone.utc)
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
        types.SimpleNamespace(is_rest_only_mode=lambda: True),
    )

    result = asyncio.run(api_main.health_check())

    assert result["status"] == "degraded"
    assert all(symbol_status["mode"] == "REST" for symbol_status in result["symbols"].values())


def test_health_check_reports_ok_outside_regular_hours(monkeypatch):
    """Stale data after hours should not degrade health."""
    fixed_now = datetime(2026, 7, 10, 22, 0, tzinfo=timezone.utc)
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
        types.SimpleNamespace(is_rest_only_mode=lambda: False),
    )

    result = asyncio.run(api_main.health_check())

    assert result["status"] == "ok"
    assert all(symbol_status["mode"] == "WebSocket" for symbol_status in result["symbols"].values())
