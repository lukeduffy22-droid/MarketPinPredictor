"""Lifecycle and API adapter for Databento OPRA gamma-pin streaming."""

import logging
from typing import Optional

from app.utils.settings import settings


log = logging.getLogger("databento_gamma")
SUPPORTED_GAMMA_SYMBOLS = ("SPX", "NDX", "RUT")
_streamer = None


async def start_databento_gamma_stream() -> None:
    """Start the singleton Databento OPRA stream when configured."""
    global _streamer
    if not settings.databento_api_key:
        log.error("DATABENTO_API_KEY unavailable; live gamma stream disabled")
        return
    if _streamer is None:
        from backend.databento_streamer import DatabentoGammaStreamer

        _streamer = DatabentoGammaStreamer(list(SUPPORTED_GAMMA_SYMBOLS))
    await _streamer.start()


async def stop_databento_gamma_stream() -> None:
    """Stop the singleton Databento OPRA stream."""
    if _streamer is not None:
        await _streamer.stop()


def get_databento_gamma_state(symbol: str) -> Optional[dict]:
    """Return a normalized backend gamma response for one symbol."""
    clean_symbol = symbol.upper()
    if _streamer is None or clean_symbol not in SUPPORTED_GAMMA_SYMBOLS:
        return None
    result = _streamer.get_latest_pin(clean_symbol)
    if not result:
        return None

    gross_gex = float(result.get("gross_gex") or 0.0)
    net_gex = float(result.get("net_gex") or 0.0)
    spot = float(result.get("price") or 0.0)
    pin = result.get("gamma_pin")
    return {
        "symbol": clean_symbol,
        "spot_price": spot,
        "pin_strike": pin,
        "pull_strength": abs(net_gex) / gross_gex * 100 if gross_gex else 0.0,
        "total_gex": gross_gex,
        "net_gex": net_gex,
        "zero_gamma": result.get("zero_gamma"),
        "direction": (
            "above" if pin is not None and pin > spot
            else "below" if pin is not None and pin < spot
            else "at"
        ),
        "summary": f"Live Databento OPRA gamma state ({result.get('likely_anchor', 'gamma pin')})",
        "is_etf_proxy": False,
        "options_root": clean_symbol,
        "gamma_walls": result.get("top_strikes") or [],
        "provider": "databento",
        "timestamp": str(result.get("timestamp") or ""),
    }


def get_databento_gamma_status(symbol: str) -> dict:
    """Return per-symbol Databento gamma readiness without fabricating data."""
    clean_symbol = symbol.upper()
    supported = clean_symbol in SUPPORTED_GAMMA_SYMBOLS
    state = get_databento_gamma_state(clean_symbol)
    return {
        "provider": "databento",
        "supported": supported,
        "ready": state is not None,
    }
