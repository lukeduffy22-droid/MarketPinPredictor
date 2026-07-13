"""Tests for provider-aware market data fallback startup."""

import asyncio
import sys
import types
from datetime import date

from app.ingest import databento_fallback, rest_fallback
from app.state import ring_buffers
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


def test_databento_poll_uses_timezone_aware_market_hours_check(monkeypatch):
    """Databento polling should pass a UTC-aware timestamp into market-hours checks."""
    monkeypatch.setattr(
        databento_fallback,
        "settings",
        types.SimpleNamespace(databento_api_key="databento-test-key"),
    )
    monkeypatch.setattr(
        databento_fallback,
        "_DatabentoPollClient",
        lambda api_key: types.SimpleNamespace(fetch_latest_prices=lambda now_utc: {}),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.utils.market_time",
        types.SimpleNamespace(market_is_closed=lambda: False, get_freeze_status=lambda: (False, "")),
    )

    def fake_is_regular_hours(dt):
        assert dt.tzinfo is not None
        raise asyncio.CancelledError()

    monkeypatch.setattr(databento_fallback, "is_regular_hours", fake_is_regular_hours)

    try:
        asyncio.run(databento_fallback.poll_databento_rest())
    except asyncio.CancelledError:
        pass


def test_update_session_vwap_resets_on_new_session(monkeypatch):
    """VWAP state should reset automatically when the ET trading session changes."""
    ring_buffers.reset_session_vwap(session_date=date(2026, 7, 10))
    ring_buffers.update_session_vwap("SPX", 100.0, 1.0)
    ring_buffers.update_session_vwap("SPX", 110.0, 1.0)
    assert ring_buffers.get_session_vwap("SPX") == 105.0

    monkeypatch.setattr(ring_buffers, "_current_session_date", lambda: date(2026, 7, 11))

    ring_buffers.update_session_vwap("SPX", 120.0, 1.0)

    assert ring_buffers.get_session_vwap("SPX") == 120.0
