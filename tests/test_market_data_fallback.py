"""Tests for provider-aware market data fallback startup."""

import asyncio
import sys
import types

from app.ingest import rest_fallback
from app.utils.settings import Settings


def test_settings_accept_polygon_api_key_env(monkeypatch):
    """Settings should honor the legacy Polygon environment variable name."""
    monkeypatch.delenv("Massive_API", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "polygon-test-key")

    configured = Settings()

    assert configured.polygon_api_key == "polygon-test-key"


def test_start_market_data_fallback_uses_polygon_when_databento_exits(monkeypatch):
    """Auto mode should fall back to Polygon if Databento cannot start."""
    calls = []

    async def fake_databento() -> None:
        calls.append("databento")

    async def fake_polygon() -> None:
        calls.append("polygon")

    monkeypatch.setattr(
        rest_fallback,
        "settings",
        types.SimpleNamespace(
            market_data_provider="auto",
            databento_api_key="databento-test-key",
            polygon_api_key="polygon-test-key",
        ),
    )
    monkeypatch.setattr(rest_fallback, "poll_polygon_rest", fake_polygon)
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.databento_fallback",
        types.SimpleNamespace(poll_databento_rest=fake_databento),
    )
    monkeypatch.setattr(rest_fallback, "CURRENT_MARKET_DATA_PROVIDER", "unknown")

    asyncio.run(rest_fallback.start_market_data_fallback())

    assert calls == ["databento", "polygon"]
    assert rest_fallback.get_market_data_provider() == "polygon"
