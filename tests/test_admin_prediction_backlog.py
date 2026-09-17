import asyncio
from datetime import date

import pytest
from fastapi import HTTPException

from backend.api.routers import admin as admin_router


def test_prediction_score_backlog_endpoint_is_read_only(monkeypatch):
    captured = {}

    def backlog(symbol, trading_date, *, limit):
        captured["symbol"] = symbol
        captured["trading_date"] = trading_date
        captured["limit"] = limit
        return {
            "read_only": True,
            "evidence_scope": "diagnostic_research_only",
            "groups": [],
        }

    monkeypatch.setattr(admin_router, "get_prediction_score_backlog", backlog)

    payload = asyncio.run(
        admin_router.prediction_score_backlog(
            symbol="spx",
            trading_date="2026-06-18",
            limit=25,
        )
    )

    assert payload["read_only"] is True
    assert captured == {
        "symbol": "SPX",
        "trading_date": date(2026, 6, 18),
        "limit": 25,
    }


def test_prediction_score_backlog_endpoint_rejects_invalid_inputs(monkeypatch):
    with pytest.raises(HTTPException) as date_error:
        asyncio.run(
            admin_router.prediction_score_backlog(
                symbol="SPX",
                trading_date="06/18/2026",
                limit=25,
            )
        )
    assert date_error.value.status_code == 400
    assert date_error.value.detail == "trading_date must be YYYY-MM-DD"

    monkeypatch.setattr(
        admin_router,
        "get_prediction_score_backlog",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            ValueError("limit must be between 1 and 5000")
        ),
    )
    with pytest.raises(HTTPException) as limit_error:
        asyncio.run(
            admin_router.prediction_score_backlog(
                symbol="SPX",
                trading_date="2026-06-18",
                limit=0,
            )
        )
    assert limit_error.value.status_code == 400
    assert "limit" in limit_error.value.detail
