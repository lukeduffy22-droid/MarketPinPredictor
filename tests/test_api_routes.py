"""Smoke tests for refactored API route registration."""

import asyncio

from app.api.main import app
from app.api.routes import predictions
from app.state.ring_buffers import TRACKED_INDEX_SYMBOLS


class _FakeRing:
    def __init__(self):
        self.length_seconds = 2

    def latest(self):
        return (1234567890, object())

    def is_fresh(self, max_age_seconds: int = 5) -> bool:
        return True


def test_domain_routes_remain_registered():
    paths = set(app.openapi()["paths"])

    assert "/health" in paths
    assert "/predict/eod" in paths
    assert "/predict/ai-enhanced" in paths
    assert "/ai/status" in paths
    assert "/accuracy/ledger" in paths
    assert "/historical/snapshot/{symbol}/{date}" in paths
    assert "/api/predictions" in paths
    assert "/gamma/state" in paths
    assert "/buffer/latest/{symbol}" in paths
    assert "/models/live" in paths
    assert "/diagnostics/system" in paths


def test_buffer_latest_supports_tracked_symbols(monkeypatch):
    monkeypatch.setattr(predictions, "INDEX_RINGS", {symbol: _FakeRing() for symbol in TRACKED_INDEX_SYMBOLS})
    monkeypatch.setattr(predictions, "get_latest_price_with_fallback", lambda symbol, api_key=None: 19.75)

    for symbol in TRACKED_INDEX_SYMBOLS:
        result = asyncio.run(predictions.get_latest_buffered_price(symbol))

        assert result["symbol"] == symbol
        assert result["price"] == 19.75
        assert result["fresh"] is True
