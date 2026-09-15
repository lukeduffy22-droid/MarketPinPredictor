"""Databento REST polling fallback for when primary streaming is unavailable."""

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List

from app.state.ring_buffers import (
    INDEX_RINGS,
    IndexTick,
    ensure_symbol_registered,
    update_session_vwap,
)
from app.utils.settings import settings
from app.utils.time_et import is_regular_hours

log = logging.getLogger("rest_fallback")

# Global flag to indicate WebSocket is connected
WEBSOCKET_CONNECTED = False

# Polling interval for near-real-time updates
REST_POLL_INTERVAL = 1.0

# Logging interval - log every N polls to reduce noise
LOG_EVERY_N_POLLS = 10
_poll_count = 0

_INDEX_PROXY_MAP = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "RUT": "IWM",
    "DJI": "DIA",
    "VIX": "UVXY",
}

_CORE_INDEX_SYMBOLS = {"SPX", "NDX", "DJI", "RUT", "VIX"}


def _utcnow_naive() -> datetime:
    """Return UTC now as naive datetime for compatibility with time utilities."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def set_websocket_connected(connected: bool):
    """Set WebSocket connection status."""
    global WEBSOCKET_CONNECTED
    WEBSOCKET_CONNECTED = connected


def is_rest_only_mode() -> bool:
    """Check if we're running in REST-only mode (WebSocket unavailable)."""
    return not WEBSOCKET_CONNECTED


def _fetch_databento_prices(databento_key: str, symbols: List[str]) -> Dict[str, float]:
    """Fetch latest prices for configured symbols using Databento minute bars."""
    try:
        import databento as db
        import pandas as pd
        from datetime import date, timedelta

        # Databento EQUS datasets are used only for directly supported non-index symbols.
        symbol_to_db = {sym: sym for sym in symbols if sym not in _CORE_INDEX_SYMBOLS}
        db_symbols = sorted(set(symbol_to_db.values()))
        if not db_symbols:
            return {}

        client = db.Historical(databento_key)
        start = (date.today() - timedelta(days=1)).isoformat()

        store = client.timeseries.get_range(
            dataset="EQUS.MINI",
            symbols=db_symbols,
            schema="ohlcv-1m",
            start=start,
            # Ensure we have enough rows to include all symbols across a full session.
            limit=max(2000, len(db_symbols) * 500),
        )
        df = store.to_df()
        if df is None or df.empty:
            return {}

        latest_by_db_symbol: Dict[str, float] = {}
        if "symbol" in df.columns:
            for db_symbol in db_symbols:
                sub = df[df["symbol"].astype(str) == db_symbol]
                if sub.empty:
                    continue
                latest = sub.iloc[-1]
                close_val = latest.get("close")
                if close_val is not None and pd.notna(close_val):
                    latest_by_db_symbol[db_symbol] = float(close_val)
        else:
            latest = df.iloc[-1]
            close_val = latest.get("close")
            if close_val is not None and pd.notna(close_val) and len(db_symbols) == 1:
                latest_by_db_symbol[db_symbols[0]] = float(close_val)

        prices: Dict[str, float] = {}
        for sym, db_symbol in symbol_to_db.items():
            price = latest_by_db_symbol.get(db_symbol)
            if price is not None:
                prices[sym] = price
        return prices

    except Exception as e:
        log.warning(f"Databento fetch failed: {e}")
        return {}


def _seed_rings_from_databento(databento_key: str, symbols: List[str], minutes: int = 360):
    """Backfill ring buffers with recent Databento 1-minute bars so predictors are warm on startup."""
    try:
        import databento as db
        import pandas as pd
        from datetime import date, timedelta

        # Only seed non-index symbols from Databento EQUS data.
        symbol_to_db = {sym: sym for sym in symbols if sym not in _CORE_INDEX_SYMBOLS}
        db_symbols = sorted(set(symbol_to_db.values()))
        if not db_symbols:
            return

        client = db.Historical(databento_key)
        start = (date.today() - timedelta(days=2)).isoformat()
        store = client.timeseries.get_range(
            dataset="EQUS.MINI",
            symbols=db_symbols,
            schema="ohlcv-1m",
            start=start,
            limit=max(4000, len(db_symbols) * 600),
        )
        df = store.to_df()
        if df is None or df.empty:
            return

        if "symbol" not in df.columns:
            return

        for sym, db_symbol in symbol_to_db.items():
            sub = df[df["symbol"].astype(str) == db_symbol]
            if sub.empty:
                continue

            # Databento often provides ts_event as index, not as a normal column.
            ts_col = "ts_event" if "ts_event" in sub.columns else ("ts_recv" if "ts_recv" in sub.columns else None)
            if "close" not in sub.columns:
                continue

            for _, row in sub.tail(minutes).iterrows():
                ts_val = row.get(ts_col) if ts_col is not None else row.name
                close_val = row.get("close")
                if close_val is None or pd.isna(close_val):
                    continue

                try:
                    ts = int(pd.Timestamp(str(ts_val)).timestamp())
                except Exception:
                    continue

                tick = IndexTick(ts=ts, price=float(close_val), size=1.0)
                INDEX_RINGS[sym].add(ts, tick)
                update_session_vwap(sym, float(close_val), 1.0)

        log.info("Databento ring buffer seeding completed")

    except Exception as e:
        log.warning(f"Databento ring buffer seed failed: {e}")


async def poll_polygon_rest():
    """
    Poll live prices every second.

    Name retained for backward compatibility with existing startup wiring.
    """
    global _poll_count

    try:
        from app.utils.market_time import market_is_closed, get_freeze_status

        if market_is_closed():
            _is_frozen, reason = get_freeze_status()
            log.warning(f"MARKET CLOSED - REST polling disabled: {reason}")
            log.warning("Live feeds disabled - use frozen snapshots only")
            return
    except ImportError:
        pass

    databento_key = (settings.databento_api_key or "").strip()

    if not databento_key:
        log.error("DATABENTO_API_KEY not set; Databento-only polling cannot start")
        return

    configured_symbols = settings.get_databento_symbols()
    for sym in configured_symbols:
        ensure_symbol_registered(sym)

    # Warm up buffers immediately so /predict/close is usable after restart.
    if databento_key:
        await asyncio.to_thread(_seed_rings_from_databento, databento_key, configured_symbols)

    log.info(f"Starting Databento REST polling (every {REST_POLL_INTERVAL}s)")

    failure_counts = {s: 0 for s in configured_symbols}
    max_failures = 10

    while True:
        try:
            try:
                from app.utils.market_time import market_is_closed as check_closed

                if check_closed():
                    log.warning("Market closed during REST polling - terminating feed")
                    return
            except ImportError:
                pass

            if not is_regular_hours(_utcnow_naive()):
                await asyncio.sleep(60)
                continue

            symbols_to_fetch = [s for s in configured_symbols if failure_counts[s] < max_failures]
            if not symbols_to_fetch:
                log.warning("All configured symbols exceeded failure threshold, waiting")
                await asyncio.sleep(30)
                continue

            ts = int(time.time())
            fetched_symbols = set()

            # Databento-first for premium plan symbols.
            db_prices: Dict[str, float] = {}
            if databento_key:
                db_prices = await asyncio.to_thread(_fetch_databento_prices, databento_key, symbols_to_fetch)

            if db_prices:
                _poll_count += 1
                should_log = (_poll_count % LOG_EVERY_N_POLLS == 0)
                if should_log:
                    log.info(f"Databento poll #{_poll_count} - {len(db_prices)} symbols")

                for symbol, price in db_prices.items():
                    tick = IndexTick(ts=ts, price=float(price), size=1.0)
                    INDEX_RINGS[symbol].add(ts, tick)
                    update_session_vwap(symbol, float(price), 1.0)
                    failure_counts[symbol] = 0
                    fetched_symbols.add(symbol)
                    if should_log:
                        log.info(f"{symbol}: ${price:.2f}")

            if fetched_symbols:
                for symbol in configured_symbols:
                    if symbol not in fetched_symbols and failure_counts[symbol] < max_failures:
                        failure_counts[symbol] += 1
                        if failure_counts[symbol] <= 3:
                            log.warning(f"No live data for {symbol} (failures: {failure_counts[symbol]})")
            else:
                log.warning("REST polling returned no results from Databento")
                for symbol in configured_symbols:
                    if failure_counts[symbol] < max_failures:
                        failure_counts[symbol] += 1

            await asyncio.sleep(REST_POLL_INTERVAL)

        except Exception as e:
            log.error(f"REST polling error: {e}")
            await asyncio.sleep(REST_POLL_INTERVAL)


async def load_cached_snapshots():
    """
    Secondary fallback: load cached index snapshots from database.
    Used when both WebSocket and live REST polling fail.
    """
    from typing import Any, cast

    from database import get_latest_gamma_snapshot

    index_symbols = ["SPX", "NDX", "DJI", "RUT"]

    log.info("Starting database fallback for cached snapshots")

    while True:
        try:
            if not is_regular_hours(_utcnow_naive()):
                await asyncio.sleep(60)
                continue

            for symbol in index_symbols:
                try:
                    snapshot = get_latest_gamma_snapshot(symbol)
                    if snapshot:
                        price = float(cast(Any, snapshot.spot_price))
                        ts = int(time.time())
                        tick = IndexTick(ts=ts, price=price, size=1.0)
                        INDEX_RINGS[symbol].add(ts, tick)
                        update_session_vwap(symbol, price, 1.0)
                except Exception as e:
                    log.error(f"Error loading cached snapshot for {symbol}: {e}")

            await asyncio.sleep(30.0)

        except Exception as e:
            log.error(f"Database fallback error: {e}")
            await asyncio.sleep(30.0)
