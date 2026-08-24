"""Tests for Databento gamma API normalization."""

from datetime import datetime, timezone
import asyncio
import sys
import types

import pytest
from fastapi import HTTPException

from app.ingest import databento_gamma
from app.api.routes import predictions


class _FakeStreamer:
    def get_latest_pin(self, symbol):
        return {
            "symbol": symbol,
            "timestamp": datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc),
            "price": 6500.0,
            "gamma_pin": 6525.0,
            "zero_gamma": 6475.0,
            "gross_gex": 120.0,
            "net_gex": 30.0,
            "likely_anchor": "Max Gamma Pin",
            "top_strikes": [{"strike": 6525.0, "net_gex": 30.0}],
        }


def test_databento_gamma_state_is_normalized(monkeypatch):
    monkeypatch.setattr(databento_gamma, "_streamer", _FakeStreamer())

    state = databento_gamma.get_databento_gamma_state("spx")

    assert state["provider"] == "databento"
    assert state["pin_strike"] == 6525.0
    assert state["pull_strength"] == 25.0
    assert state["direction"] == "above"


def test_databento_gamma_fails_closed_for_unsupported_symbol(monkeypatch):
    monkeypatch.setattr(databento_gamma, "_streamer", _FakeStreamer())

    assert databento_gamma.get_databento_gamma_state("DJI") is None
    assert databento_gamma.get_databento_gamma_status("DJI") == {
        "provider": "databento",
        "supported": False,
        "ready": False,
    }


def test_gamma_route_uses_databento_and_fails_closed_when_warming(monkeypatch):
    monkeypatch.setattr(databento_gamma, "_streamer", None)
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.market_data_provider",
        "databento",
    )
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.options_data_provider",
        "none",
    )
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.databento_api_key",
        "key",
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(predictions.get_gamma_state("SPX"))

    assert exc_info.value.status_code == 503
    assert "Databento gamma state unavailable" in exc_info.value.detail


def test_gamma_route_preserves_polygon_path_for_hybrid_provider(monkeypatch):
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.market_data_provider",
        "databento",
    )
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.options_data_provider",
        "polygon",
    )
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.databento_api_key",
        "db-key",
    )
    monkeypatch.setattr(
        "app.ingest.provider_selection.settings.polygon_api_key",
        "polygon-key",
    )
    monkeypatch.setattr(
        predictions,
        "get_latest_price_with_fallback",
        lambda symbol, api_key: 6500.0,
    )

    fake_options_gamma = types.SimpleNamespace(
        get_gamma_analysis=lambda **_kwargs: {
            "pin_strike": 6525.0,
            "pull_strength": 1.0,
            "total_gex": 10.0,
            "net_gex": 2.0,
            "gamma_walls": [],
        }
    )
    monkeypatch.setitem(sys.modules, "options_gamma", fake_options_gamma)

    state = asyncio.run(predictions.get_gamma_state("SPX"))

    assert state["pin_strike"] == 6525.0
