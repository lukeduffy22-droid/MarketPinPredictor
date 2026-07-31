"""
Databento fallback polling for index proxy prices.

Uses front-month continuous futures as liquid proxies:
- SPX <- ES.c.0
- NDX <- NQ.c.0
- DJI <- YM.c.0
- RUT <- RTY.c.0
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from app.state.ring_buffers import INDEX_RINGS, IndexTick, update_session_vwap
from app.utils.settings import settings
from app.utils.time_et import is_regular_hours

log = logging.getLogger("databento_fallback")

DATABENTO_POLL_INTERVAL = 1.0

# Databento futures proxies for major US indices
SYMBOL_MAP: Dict[str, Tuple[str, ...]] = {
    "SPX": ("ES.c.0",),
    "NDX": ("NQ.c.0",),
    "DJI": ("YM.c.0",),
    "RUT": ("RTY.c.0",),
    # VIX may be available through either the standard or mini volatility futures
    # contract in different environments, so try both continuous symbols.
    "VIX": ("VX.c.0", "VXM.c.0"),
}


class _DatabentoPollClient:
    """Thin wrapper to isolate Databento client usage and keep polling loop compact."""

    def __init__(self, api_key: str):
        try:
            import databento as db  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("databento package is not installed") from exc

        self._db = db
        self._client = db.Historical(api_key)

    def fetch_latest_prices(self, now_utc: datetime) -> Dict[str, float]:
        """
        Fetch latest close values for mapped proxy symbols.

        Uses short rolling windows to keep payloads and latency low.
        """
        start = (now_utc - timedelta(seconds=90)).isoformat()
        end = now_utc.isoformat()

        prices: Dict[str, float] = {}

        for index_symbol, proxy_symbols in SYMBOL_MAP.items():
            for proxy_symbol in proxy_symbols:
                try:
                    store = self._client.timeseries.get_range(
                        dataset="GLBX.MDP3",
                        schema="ohlcv-1s",
                        stype_in="continuous",
                        symbols=[proxy_symbol],
                        start=start,
                        end=end,
                    )

                    df = store.to_df()
                    if df is None or df.empty:
                        continue

                    # Databento frames generally expose "close" for OHLCV schemas.
                    close_series = df.get("close")
                    if close_series is None or close_series.empty:
                        continue

                    latest_close = float(close_series.iloc[-1])
                    if latest_close > 0:
                        prices[index_symbol] = latest_close
                        break
                except Exception as exc:
                    log.debug("Databento fetch failed for %s (%s): %s", index_symbol, proxy_symbol, exc)

        return prices


async def poll_databento_rest() -> None:
    """
    Poll Databento once per second during regular hours.

    FREEZE GUARD: Terminates when market closes.
    """
    api_key = settings.databento_api_key
    if not api_key:
        log.warning("DATABENTO_API_KEY not set, Databento polling disabled")
        return

    try:
        from app.utils.market_time import market_is_closed, get_freeze_status
        if market_is_closed():
            _, reason = get_freeze_status()
            log.info("Databento poller waiting for market open: %s", reason)
    except ImportError:
        pass

    try:
        client = _DatabentoPollClient(api_key)
    except Exception as exc:
        log.error("Databento client initialization failed: %s", exc)
        return

    log.info("Starting Databento polling (every %.1fs)", DATABENTO_POLL_INTERVAL)

    while True:
        try:
            from app.utils.market_time import market_is_closed as check_closed
            if check_closed():
                await asyncio.sleep(30)
                continue

            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(30)
                continue

            now_utc = datetime.now(timezone.utc)
            prices = client.fetch_latest_prices(now_utc)
            ts = int(time.time())

            for symbol, price in prices.items():
                tick = IndexTick(ts=ts, price=price, size=1.0)
                INDEX_RINGS[symbol].add(ts, tick)
                update_session_vwap(symbol, price, 1.0)

            await asyncio.sleep(DATABENTO_POLL_INTERVAL)
        except Exception as exc:
            log.error("Databento polling error: %s", exc)
            await asyncio.sleep(DATABENTO_POLL_INTERVAL)
