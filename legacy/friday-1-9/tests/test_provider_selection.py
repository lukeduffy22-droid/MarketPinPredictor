"""Tests for provider selection contract helpers."""

from app.ingest import provider_selection


def test_resolve_index_provider_prefers_databento_in_auto(monkeypatch):
    monkeypatch.setattr(provider_selection.settings, "market_data_provider", "auto")
    monkeypatch.setattr(provider_selection.settings, "databento_api_key", "db_key")
    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "poly_key")

    assert provider_selection.resolve_index_data_provider() == "databento"


def test_resolve_index_provider_falls_back_to_databento_when_polygon_forced_without_key(monkeypatch):
    monkeypatch.setattr(provider_selection.settings, "market_data_provider", "polygon")
    monkeypatch.setattr(provider_selection.settings, "databento_api_key", "db_key")
    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "")

    assert provider_selection.resolve_index_data_provider() == "databento"


def test_resolve_index_provider_returns_none_when_no_keys(monkeypatch):
    monkeypatch.setattr(provider_selection.settings, "market_data_provider", "databento")
    monkeypatch.setattr(provider_selection.settings, "databento_api_key", "")
    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "")

    assert provider_selection.resolve_index_data_provider() == "none"


def test_should_start_polygon_index_stream_only_for_active_polygon_provider(monkeypatch):
    monkeypatch.setattr(provider_selection.settings, "market_data_provider", "polygon")
    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "poly_key")
    monkeypatch.setattr(provider_selection.settings, "databento_api_key", "")

    assert provider_selection.should_start_polygon_index_stream() is True

    monkeypatch.setattr(provider_selection.settings, "market_data_provider", "databento")
    monkeypatch.setattr(provider_selection.settings, "databento_api_key", "db_key")

    assert provider_selection.should_start_polygon_index_stream() is False


def test_resolve_options_provider(monkeypatch):
    monkeypatch.setattr(provider_selection.settings, "options_data_provider", "polygon")
    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "poly_key")
    assert provider_selection.resolve_options_data_provider() == "polygon"

    monkeypatch.setattr(provider_selection.settings, "polygon_api_key", "")
    assert provider_selection.resolve_options_data_provider() == "none"

    monkeypatch.setattr(provider_selection.settings, "options_data_provider", "none")
    assert provider_selection.resolve_options_data_provider() == "none"
