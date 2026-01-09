# Backend application package
"""
Legacy entry points for market data functionality.

The original implementations of the following functions have been removed
from this module:
  - get_live_market_data
  - get_multiple_quotes
  - get_market_data_with_key

If these functions are still needed, their implementations should live in a
more appropriate module (for example, a dedicated `market_data` module),
and this package can re-export them. As of now, they are intentionally
unimplemented and will raise NotImplementedError when called.
"""


def get_live_market_data(*args, **kwargs):
    """
Legacy stub for get_live_market_data.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    raise NotImplementedError(
        "get_live_market_data has been removed from app.__init__.py. "
        "Update your code to use the new market data API or remove this call."
    )


def get_multiple_quotes(*args, **kwargs):
    """
Legacy stub for get_multiple_quotes.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    raise NotImplementedError(
        "get_multiple_quotes has been removed from app.__init__.py. "
        "Update your code to use the new market data API or remove this call."
    )


def get_market_data_with_key(*args, **kwargs):
    """
Legacy stub for get_market_data_with_key.

This function has been removed from app.__init__.py. If your code still
depends on it, you should update the call site to import and use the
new implementation (if one exists) or remove the dependency entirely.
"""
    raise NotImplementedError(
        "get_market_data_with_key has been removed from app.__init__.py. "
        "Update your code to use the new market data API or remove this call."
    )
