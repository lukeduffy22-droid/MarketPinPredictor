"""Tests for legacy exports retained in app.__init__."""

import pytest

from app import (
    get_live_market_data,
    get_market_data_with_key,
    get_multiple_quotes,
)


@pytest.mark.parametrize(
    ("func", "name"),
    [
        (get_live_market_data, "get_live_market_data"),
        (get_multiple_quotes, "get_multiple_quotes"),
        (get_market_data_with_key, "get_market_data_with_key"),
    ],
)
def test_legacy_export_raises_clear_error(func, name):
    """Legacy exports should fail with consistent migration guidance."""
    with pytest.deprecated_call():
        with pytest.raises(NotImplementedError, match=rf"{name} has been removed from app.__init__\.py"):
            func()
