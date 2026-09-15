"""Tests for websocket subscription diagnostics endpoint."""

import asyncio
import sys
import types

from app.api.routes import debug


def test_debug_subscriptions_returns_full_lists(monkeypatch):
    monkeypatch.setattr(debug, "now_et", lambda: types.SimpleNamespace(isoformat=lambda: "2026-01-01T00:00:00+00:00"))
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.websocket_stream",
        types.SimpleNamespace(
            get_websocket_subscription_state=lambda: {
                "stream": "index",
                "requested_count": 2,
                "requested_subscriptions": ["V.I:SPX", "V.I:NDX"],
                "confirmed_count": 1,
                "confirmed_subscriptions": ["V.I:SPX"],
            }
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.options_websocket_stream",
        types.SimpleNamespace(
            get_options_subscription_state=lambda: {
                "stream": "options",
                "requested_count": 421,
                "requested_subscriptions": [f"T.O:TEST{i}" for i in range(421)],
                "confirmed_count": 420,
                "confirmed_subscriptions": [f"T.O:TEST{i}" for i in range(420)],
            }
        ),
    )

    result = asyncio.run(debug.debug_subscriptions())

    assert result["index_stream"]["requested_count"] == 2
    assert result["options_stream"]["requested_count"] == 421
    assert len(result["options_stream"]["requested_subscriptions"]) == 421
    assert result["total_requested_subscriptions"] == 423
    assert result["total_confirmed_subscriptions"] == 421
