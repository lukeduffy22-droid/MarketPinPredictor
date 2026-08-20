"""Tests for gamma state behavior when options providers are unavailable."""

import asyncio

import pytest
from fastapi import HTTPException

from app.api.routes import predictions


def test_gamma_state_raises_503_when_options_module_unavailable(monkeypatch):
    """Gamma endpoint should raise 503 when options_gamma module is unavailable."""
    monkeypatch.setattr(
        predictions, "get_latest_price_with_fallback", lambda symbol, api_key: 5500.0
    )

    def _raise_import(*args, **kwargs):
        raise ImportError("options_gamma not installed")

    import builtins
    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "options_gamma":
            raise ImportError("options_gamma not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(predictions.get_gamma_state("SPX"))

    assert exc_info.value.status_code == 503


def test_gamma_state_accepts_vix_symbol(monkeypatch):
    """Gamma endpoint should accept VIX as a valid live display symbol."""
    monkeypatch.setattr(
        predictions, "get_latest_price_with_fallback", lambda symbol, api_key: 19.75
    )

    import builtins
    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "options_gamma":
            raise ImportError("options_gamma not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(predictions.get_gamma_state("VIX"))

    # VIX is now a valid symbol - it should attempt gamma and raise 503, not 400
    assert exc_info.value.status_code == 503
