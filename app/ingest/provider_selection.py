"""Provider selection helpers for index and options data sources."""

from typing import Literal

from app.utils.settings import settings

IndexProvider = Literal["databento", "polygon", "none"]
OptionsProvider = Literal["polygon", "none"]


def resolve_index_data_provider() -> IndexProvider:
    """
    Resolve the active index data provider from configuration and available keys.

    Resolution behavior:
    - databento: use Databento when key exists; fallback to Polygon when available.
    - polygon: use Polygon when key exists; fallback to Databento when available.
    - auto/unknown: prefer Databento, then Polygon.
    """
    configured = (settings.market_data_provider or "auto").strip().lower()
    has_databento = bool(settings.databento_api_key)
    has_polygon = bool(settings.polygon_api_key)

    if configured == "databento":
        if has_databento:
            return "databento"
        if has_polygon:
            return "polygon"
        return "none"

    if configured == "polygon":
        if has_polygon:
            return "polygon"
        if has_databento:
            return "databento"
        return "none"

    if has_databento:
        return "databento"
    if has_polygon:
        return "polygon"
    return "none"


def should_start_polygon_index_stream() -> bool:
    """Return whether Polygon index websocket stream should be started."""
    return resolve_index_data_provider() == "polygon" and bool(settings.polygon_api_key)


def resolve_options_data_provider() -> OptionsProvider:
    """
    Resolve the active options provider.

    Only Polygon is currently supported for live options feed.
    """
    configured = (settings.options_data_provider or "none").strip().lower()
    if configured == "polygon" and settings.polygon_api_key:
        return "polygon"
    return "none"
