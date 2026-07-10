"""Legacy application exports retained only for compatibility checks.

Active runtime entry points live in:
- ``app.api.main`` for the FastAPI application
- ``server.py`` for local API startup
- ``app.py`` for the Streamlit dashboard

The legacy functions below remain importable so stale callers fail with a
consistent message instead of an import error.
"""

import warnings

__all__ = [
    "get_live_market_data",
    "get_multiple_quotes",
    "get_market_data_with_key",
]


def _raise_removed_entrypoint(name: str) -> None:
    """Raise a consistent error for removed legacy entry points."""
    warnings.warn(
        f"{name} is deprecated as of 0.1.0 and will be removed in a future release.",
        DeprecationWarning,
        stacklevel=2,
    )
    raise NotImplementedError(
        f"{name} has been removed from app.__init__.py. "
        "Update your code to use the active API surface in app.api.main "
        "or remove this dependency."
    )


def get_live_market_data(*args, **kwargs):
    """
Legacy stub for get_live_market_data.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    _raise_removed_entrypoint("get_live_market_data")


def get_multiple_quotes(*args, **kwargs):
    """
Legacy stub for get_multiple_quotes.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    _raise_removed_entrypoint("get_multiple_quotes")


def get_market_data_with_key(*args, **kwargs):
    """
Legacy stub for get_market_data_with_key.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    _raise_removed_entrypoint("get_market_data_with_key")
