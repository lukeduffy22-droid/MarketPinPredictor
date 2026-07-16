"""Smoke tests for refactored API route registration."""

from app.api.main import app


def test_domain_routes_remain_registered():
    paths = set(app.openapi()["paths"])

    assert "/health" in paths
    assert "/predict/eod" in paths
    assert "/predict/ai-enhanced" in paths
    assert "/ai/status" in paths
    assert "/accuracy/ledger" in paths
    assert "/debug/subscriptions" in paths
    assert "/historical/snapshot/{symbol}/{date}" in paths
    assert "/api/predictions" in paths
    assert "/gamma/state" in paths
    assert "/models/live" in paths
    assert "/diagnostics/system" in paths
