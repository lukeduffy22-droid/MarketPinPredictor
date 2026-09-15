from backend.market_universe import (
    EQUITY_CONTEXT_SYMBOLS, MARKET_UNIVERSE, OPTION_ETF_SYMBOLS,
    market_universe_catalog, normalize_market_symbol, resolve_equity_context_symbols,
)
import pytest


def test_index_etf_families_never_authorize_price_conversion():
    catalog = {row["symbol"]: row for row in market_universe_catalog()}
    for index, etf in (("SPX", "SPY"), ("NDX", "QQQ"), ("DJI", "DIA"), ("RUT", "IWM")):
        assert catalog[index]["etf_equivalent"] == etf
        assert catalog[index]["cash_index_feed_supported"] is False
        assert catalog[index]["equity_symbol"] is None
        assert catalog[etf]["equity_symbol"] == etf
        assert not catalog[index]["price_conversion_allowed"]
    assert catalog["DJI"]["opra_root"] is None
    assert normalize_market_symbol("dij") == "DJI"
    assert normalize_market_symbol("^DJI") == "DJI"


def test_context_basket_is_bounded_and_rejects_index_substitution():
    assert len(EQUITY_CONTEXT_SYMBOLS) == 20
    assert resolve_equity_context_symbols("spy, SPY, qqq") == ("SPY", "QQQ")
    with pytest.raises(ValueError, match="Unsupported"):
        resolve_equity_context_symbols("SPX")
    for symbol in EQUITY_CONTEXT_SYMBOLS:
        assert MARKET_UNIVERSE[symbol].kind == "etf"
        assert (MARKET_UNIVERSE[symbol].opra_root is not None) == (symbol in OPTION_ETF_SYMBOLS)


def test_etf_option_underlying_validation_keeps_own_price_units():
    from backend.underlying_validator import PROXY_SYMBOLS
    for symbol in OPTION_ETF_SYMBOLS:
        assert PROXY_SYMBOLS[symbol] == symbol
