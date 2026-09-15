"""Explicit instrument identities and a bounded cross-market research universe.

An ETF is a separately priced instrument. Family links permit comparisons of
returns; they never authorize converting an ETF price into a cash-index level.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class MarketInstrument:
    symbol: str
    name: str
    kind: str
    family: str
    role: str
    opra_root: str | None = None
    equity_symbol: str | None = None
    etf_equivalent: str | None = None


_INDEXES = (
    MarketInstrument("SPX", "S&P 500", "index", "SPX", "large_cap", "SPXW", etf_equivalent="SPY"),
    MarketInstrument("NDX", "Nasdaq 100", "index", "NDX", "growth", "NDXP", etf_equivalent="QQQ"),
    MarketInstrument("DJI", "Dow Jones Industrial Average", "index", "DJI", "blue_chip", etf_equivalent="DIA"),
    MarketInstrument("RUT", "Russell 2000", "index", "RUT", "small_cap", "RUTW", etf_equivalent="IWM"),
    MarketInstrument("VIX", "Cboe Volatility Index", "index", "VIX", "volatility", "VIXW"),
)
_ETFS = (
    ("SPY", "S&P 500 ETF", "SPX", "large_cap"),
    ("QQQ", "Nasdaq 100 ETF", "NDX", "growth"),
    ("DIA", "Dow Jones Industrial Average ETF", "DJI", "blue_chip"),
    ("IWM", "Russell 2000 ETF", "RUT", "small_cap"),
    ("RSP", "S&P 500 Equal Weight ETF", "RSP", "equal_weight"),
    ("VTI", "Total US Stock Market ETF", "VTI", "broad_market"),
    ("MDY", "S&P MidCap 400 ETF", "MDY", "mid_cap"),
    ("XLK", "Technology Sector ETF", "XLK", "technology"),
    ("XLF", "Financial Sector ETF", "XLF", "financials"),
    ("XLY", "Consumer Discretionary Sector ETF", "XLY", "discretionary"),
    ("XLP", "Consumer Staples Sector ETF", "XLP", "staples"),
    ("XLE", "Energy Sector ETF", "XLE", "energy"),
    ("XLI", "Industrial Sector ETF", "XLI", "industrials"),
    ("XLV", "Health Care Sector ETF", "XLV", "healthcare"),
    ("XLU", "Utilities Sector ETF", "XLU", "utilities"),
    ("SMH", "Semiconductor ETF", "SMH", "semiconductors"),
    ("TLT", "Long Treasury Bond ETF", "TLT", "long_rates"),
    ("IEF", "Intermediate Treasury Bond ETF", "IEF", "intermediate_rates"),
    ("HYG", "High Yield Corporate Bond ETF", "HYG", "high_yield_credit"),
    ("LQD", "Investment Grade Corporate Bond ETF", "LQD", "investment_grade_credit"),
)
OPTION_ETF_SYMBOLS = ("SPY", "QQQ", "DIA", "IWM")
EQUITY_CONTEXT_SYMBOLS = tuple(row[0] for row in _ETFS)
MAX_EQUITY_CONTEXT_SYMBOLS = len(EQUITY_CONTEXT_SYMBOLS)
MARKET_UNIVERSE = {item.symbol: item for item in _INDEXES}
MARKET_UNIVERSE.update({
    symbol: MarketInstrument(symbol, name, "etf", family, role,
                             symbol if symbol in OPTION_ETF_SYMBOLS else None, symbol)
    for symbol, name, family, role in _ETFS
})
DEFAULT_TRACKED_SYMBOLS = tuple(MARKET_UNIVERSE)
EXPANDED_OPRA_SYMBOLS = ("SPX", "NDX", "VIX", "RUT", *OPTION_ETF_SYMBOLS)


def normalize_market_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper().removeprefix("^")
    return {"DIJ": "DJI", "DJIA": "DJI"}.get(normalized, normalized)


def resolve_equity_context_symbols(symbols) -> tuple[str, ...]:
    """Deduplicate an allowlisted ETF basket; never silently substitute indexes."""
    if isinstance(symbols, str):
        symbols = symbols.split(",")
    resolved = tuple(dict.fromkeys(normalize_market_symbol(value) for value in symbols if str(value).strip()))
    unsupported = set(resolved).difference(EQUITY_CONTEXT_SYMBOLS)
    if unsupported:
        raise ValueError("Unsupported equity context symbols: " + ", ".join(sorted(unsupported)))
    if len(resolved) > MAX_EQUITY_CONTEXT_SYMBOLS:
        raise ValueError("Equity context symbol budget exceeded")
    return resolved


def market_universe_catalog() -> list[dict]:
    return [
        {
            **asdict(item),
            "cash_index_feed_supported": False if item.kind == "index" else None,
            "price_conversion_allowed": False,
            "option_reference_kind": (
                "option_implied_reference" if item.opra_root else None
            ),
            "tracking_capability": (
                "equity_context" if item.equity_symbol else
                "option_implied_reference" if item.opra_root else "etf_context_only"
            ),
            "coverage_status": "NOT_YET_VERIFIED",
        }
        for item in MARKET_UNIVERSE.values()
    ]
