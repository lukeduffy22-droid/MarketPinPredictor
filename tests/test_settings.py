"""Tests for environment-backed application settings."""

from app.utils.settings import Settings


def test_settings_loads_databento_key_from_environment(monkeypatch):
    """Databento key should be read from the documented environment variable."""
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test-key")
    monkeypatch.delenv("DATABENTO_KEY", raising=False)

    settings = Settings()

    assert settings.databento_api_key == "db-test-key"


def test_settings_loads_databento_key_from_legacy_environment(monkeypatch):
    """Databento key should support the shorter legacy environment variable."""
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)
    monkeypatch.setenv("DATABENTO_KEY", "db-legacy-key")

    settings = Settings()

    assert settings.databento_api_key == "db-legacy-key"


def test_settings_prefers_documented_databento_environment(monkeypatch):
    """Documented Databento variable should win over the legacy alias."""
    monkeypatch.setenv("DATABENTO_API_KEY", "db-test-key")
    monkeypatch.setenv("DATABENTO_KEY", "db-legacy-key")

    settings = Settings()

    assert settings.databento_api_key == "db-test-key"


def test_settings_loads_polygon_key_from_massive_environment(monkeypatch):
    """Polygon/Massive key should support the primary documented environment name."""
    monkeypatch.setenv("Massive_API", "massive-test-key")
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)

    settings = Settings()

    assert settings.polygon_api_key == "massive-test-key"


def test_settings_loads_polygon_key_from_polygon_environment(monkeypatch):
    """Polygon/Massive key should support the legacy Polygon environment name."""
    monkeypatch.delenv("Massive_API", raising=False)
    monkeypatch.setenv("POLYGON_API_KEY", "polygon-test-key")

    settings = Settings()

    assert settings.polygon_api_key == "polygon-test-key"


def test_settings_prefers_massive_environment(monkeypatch):
    """Primary Massive variable should win over the legacy Polygon alias."""
    monkeypatch.setenv("Massive_API", "massive-test-key")
    monkeypatch.setenv("POLYGON_API_KEY", "polygon-test-key")

    settings = Settings()

    assert settings.polygon_api_key == "massive-test-key"


def test_settings_loads_market_data_provider_from_environment(monkeypatch):
    """Market data provider override should be read from the environment."""
    monkeypatch.setenv("MARKET_DATA_PROVIDER", "databento")

    settings = Settings()

    assert settings.market_data_provider == "databento"
