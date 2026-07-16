"""Tests for gamma state behavior when options providers are unavailable."""

import asyncio

from app.api.routes import predictions


def test_gamma_state_returns_unavailable_when_options_provider_disabled(monkeypatch):
    """Gamma endpoint should fail closed when options provider is disabled."""
    monkeypatch.setattr(predictions.settings, "options_data_provider", "none")
    monkeypatch.setattr(
        predictions, "get_latest_price_with_fallback", lambda symbol, api_key: 5500.0
    )

    result = asyncio.run(predictions.get_gamma_state("SPX"))

    assert result["data_unavailable"] is True
    assert result["total_gex"] == 0.0
    assert result["net_gex"] == 0.0
    assert result["gamma_walls"] == []
    assert "options provider disabled" in result["summary"].lower()


def test_gamma_state_returns_unavailable_when_polygon_key_missing(monkeypatch):
    """Gamma endpoint should fail closed when polygon provider has no key."""
    monkeypatch.setattr(predictions.settings, "options_data_provider", "polygon")
    monkeypatch.setattr(predictions.settings, "polygon_api_key", "")
    monkeypatch.setattr(
        predictions, "get_latest_price_with_fallback", lambda symbol, api_key: 5500.0
    )

    result = asyncio.run(predictions.get_gamma_state("SPX"))

    assert result["data_unavailable"] is True
    assert result["total_gex"] == 0.0
    assert result["net_gex"] == 0.0
    assert result["gamma_walls"] == []
    assert "api key is missing" in result["summary"].lower()
