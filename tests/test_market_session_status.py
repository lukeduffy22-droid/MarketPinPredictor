import asyncio

import requests

from app.services.live_data_client import (
    fetch_backend_market_session_status,
    market_session_status_from_health,
)
from backend.api.routers import health as health_router


def test_labor_day_2026_backend_session_cannot_render_open():
    status = market_session_status_from_health(
        {
            "subscription_session_state": "non_trading_day",
            "subscription_allowed": False,
            "subscription_window": {
                "trading_date": "2026-09-07",
                "observed_at_utc": "2026-09-07T15:00:00+00:00",
            },
        }
    )

    assert status == {
        "state": "closed",
        "is_open": False,
        "session_state": "non_trading_day",
        "reason": None,
    }


def test_tuesday_regular_session_backend_authority_renders_open():
    status = market_session_status_from_health(
        {
            "subscription_session_state": "regular_session",
            "subscription_allowed": True,
            "subscription_window": {
                "trading_date": "2026-09-08",
                "observed_at_utc": "2026-09-08T15:00:00+00:00",
            },
        }
    )

    assert status == {
        "state": "open",
        "is_open": True,
        "session_state": "regular_session",
        "reason": None,
    }


def test_market_session_badge_fails_closed_when_backend_is_unavailable(monkeypatch):
    def fail(*_args, **_kwargs):
        raise requests.exceptions.ConnectionError("offline")

    monkeypatch.setattr("app.services.live_data_client.requests.get", fail)

    status = fetch_backend_market_session_status()

    assert status["state"] == "unavailable"
    assert status["is_open"] is False
    assert "connection failed" in status["reason"]


def test_health_live_exposes_subscription_session_authority(monkeypatch):
    streamer = object()
    session_window = {
        "state": "non_trading_day",
        "trading_date": "2026-09-07",
    }
    monkeypatch.setattr(health_router, "get_streamer", lambda: streamer)
    monkeypatch.setattr(
        health_router,
        "streamer_health",
        lambda _streamer: {
            "subscription_session_state": "non_trading_day",
            "subscription_allowed": False,
            "subscription_window": session_window,
        },
    )
    monkeypatch.setattr(
        health_router,
        "_live_pipeline_snapshot",
        lambda _streamer, _health: {"prediction_pipeline_ok": False},
    )
    monkeypatch.setattr(health_router, "inference_device", lambda: "cpu")
    monkeypatch.setattr(health_router, "runtime_control_health", lambda: {})

    payload = asyncio.run(health_router.health_live_status())

    assert payload["subscription_session_state"] == "non_trading_day"
    assert payload["subscription_allowed"] is False
    assert payload["subscription_window"] == session_window
