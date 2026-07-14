"""Tests for environment alias handling in settings."""

from app.utils.settings import Settings


def test_settings_default_to_databento_provider(monkeypatch):
    """Databento should be the default live provider configuration."""

    monkeypatch.delenv("MARKET_DATA_PROVIDER", raising=False)

    settings = Settings()

    assert settings.market_data_provider == "databento"


def test_settings_accept_polygon_api_key_alias(monkeypatch):
    """POLYGON_API_KEY should be accepted as a first-class alias."""

    monkeypatch.delenv("Massive_API", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "polygon-test-key")

    settings = Settings()

    assert settings.polygon_api_key == "polygon-test-key"
