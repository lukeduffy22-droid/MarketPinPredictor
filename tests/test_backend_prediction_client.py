"""Tests for Streamlit backend prediction client."""

from app.services.backend_predictions import fetch_live_prediction


class _FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


def test_fetch_live_prediction_success(monkeypatch):
    def fake_get(*args, **kwargs):
        return _FakeResponse(
            200,
            {
                "predicted_close": 5100.0,
                "confidence_level": "high",
                "current_price": 5098.0,
            },
        )

    monkeypatch.setattr("app.services.backend_predictions.requests.get", fake_get)
    predicted, confidence, payload, error = fetch_live_prediction("SPX")

    assert error is None
    assert predicted == 5100.0
    assert confidence == 85.0
    assert payload["current_price"] == 5098.0


def test_fetch_live_prediction_http_failure(monkeypatch):
    def fake_get(*args, **kwargs):
        return _FakeResponse(503, {"detail": "stale data"})

    monkeypatch.setattr("app.services.backend_predictions.requests.get", fake_get)
    predicted, confidence, payload, error = fetch_live_prediction("SPX")

    assert predicted is None
    assert confidence is None
    assert payload == {}
    assert "stale data" in error
