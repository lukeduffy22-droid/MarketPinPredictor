"""Tests for Streamlit backend prediction helper behavior."""

from app.services import backend_predictions


class DummyResponse:
    """Minimal requests.Response stand-in for backend helper tests."""

    def __init__(self, status_code, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


def test_fetch_live_prediction_rejects_stale_backend_health(monkeypatch):
    """Degraded backend health should stop Streamlit from trusting live predictions."""

    monkeypatch.setattr(backend_predictions.settings, "backend_base_url", "http://backend")

    def fake_get(url, params=None, timeout=None):
        if url.endswith("/health"):
            return DummyResponse(
                200,
                {
                    "status": "degraded",
                    "market_data_provider": "databento",
                    "provider_status": {"last_success_at": "2025-07-10T19:29:55Z"},
                    "symbols": {"SPX": {"fresh": False, "data_age_seconds": 11}},
                },
            )
        raise AssertionError("predict endpoint should not be called when health is stale")

    monkeypatch.setattr(backend_predictions.requests, "get", fake_get)

    predicted_price, confidence, payload, error = backend_predictions.fetch_live_prediction("SPX")

    assert predicted_price is None
    assert confidence is None
    assert payload["market_data_provider"] == "databento"
    assert error is not None
    assert "stale" in error.lower()


def test_fetch_live_prediction_enriches_payload_with_health(monkeypatch):
    """Healthy backend responses should carry provider diagnostics into Streamlit."""

    monkeypatch.setattr(backend_predictions.settings, "backend_base_url", "http://backend")

    def fake_get(url, params=None, timeout=None):
        if url.endswith("/health"):
            return DummyResponse(
                200,
                {
                    "status": "ok",
                    "market_data_provider": "databento",
                    "provider_status": {"last_success_at": "2025-07-10T19:29:55Z"},
                    "symbols": {"SPX": {"fresh": True, "data_age_seconds": 1}},
                },
            )
        if url.endswith("/predict/close"):
            return DummyResponse(
                200,
                {
                    "predicted_close": 6100.0,
                    "confidence_level": "high",
                    "current_price": 6090.0,
                },
            )
        raise AssertionError(f"Unexpected URL: {url}")

    monkeypatch.setattr(backend_predictions.requests, "get", fake_get)

    predicted_price, confidence, payload, error = backend_predictions.fetch_live_prediction("SPX")

    assert error is None
    assert predicted_price == 6100.0
    assert confidence == 85.0
    assert payload["market_data_provider"] == "databento"
    assert payload["symbol_status"]["data_age_seconds"] == 1
    assert "last_success_age_seconds" in payload["provider_status"]
