"""Smoke tests for refactored API route registration."""

import asyncio
import sys
import types

from app.api.main import app
from app.api.routes import observability


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
    assert "/models/live" in paths
    assert "/diagnostics/system" in paths


def test_system_diagnostics_reports_runtime_provider(monkeypatch):
    """Diagnostics should expose live provider runtime status, not config intent only."""

    monkeypatch.setattr(observability, "snapshot", lambda name: {"current_mb": 1.0, "peak_mb": 2.0})
    monkeypatch.setitem(
        sys.modules,
        "torch",
        types.SimpleNamespace(
            cuda=types.SimpleNamespace(
                is_available=lambda: False,
                get_device_name=lambda index: None,
            )
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "app.ingest.rest_fallback",
        types.SimpleNamespace(
            get_market_data_status=lambda: {
                "selected_provider": "databento",
                "selection_reason": "default-databento",
                "running": True,
                "last_success_at": "2025-07-10T19:29:55Z",
            }
        ),
    )

    result = asyncio.run(observability.get_system_diagnostics())

    assert result["market_data_provider"] == "databento"
    assert result["market_data_status"]["selection_reason"] == "default-databento"
