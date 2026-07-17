"""
REST fallback for when WebSocket connection fails.
Polls Polygon REST API for live prices when WebSocket is unavailable.
Also loads cached snapshots as a secondary fallback.

NOTE: Replit blocks WebSocket connections to socket.polygon.io (DNS resolution fails).
This REST polling is the primary data source in the Replit environment.
With premium Polygon subscription (unlimited API calls), we poll every 1 second
for near-real-time data.
"""
import asyncio
import logging
from datetime import datetime
import time
import os

from app.state.ring_buffers import INDEX_RINGS, update_session_vwap, IndexTick
from app.ingest.provider_selection import resolve_index_data_provider
from app.utils.time_et import is_regular_hours
from app.utils.settings import settings

log = logging.getLogger("rest_fallback")

# Global flag to indicate WebSocket is connected
WEBSOCKET_CONNECTED = False
CURRENT_MARKET_DATA_PROVIDER = "unknown"

# Polling interval - 1 second for near-real-time with premium subscription (unlimited API calls)
REST_POLL_INTERVAL = 1.0

# Logging interval - log every N polls to reduce noise
LOG_EVERY_N_POLLS = 10
_poll_count = 0

def set_websocket_connected(connected: bool):
    """Set WebSocket connection status"""
    global WEBSOCKET_CONNECTED
    WEBSOCKET_CONNECTED = connected

def is_rest_only_mode() -> bool:
    """Check if we're running in REST-only mode (WebSocket unavailable)"""
    return not WEBSOCKET_CONNECTED


def get_market_data_provider() -> str:
    """Return the currently selected fallback provider."""
    return CURRENT_MARKET_DATA_PROVIDER

async def poll_polygon_rest():
    """
    Poll Polygon REST API as backup when WebSocket is primary.
    With premium Polygon subscription (unlimited API calls), polls every 1 second.
    
    FREEZE GUARD: Disabled when market is closed.
    """
    global _poll_count
    
    try:
        from app.utils.market_time import market_is_closed, get_freeze_status
        if market_is_closed():
            _, reason = get_freeze_status()
            log.info(f"REST poller waiting for market open: {reason}")
    except ImportError:
        pass
    
    from polygon.rest import RESTClient
    
    api_key = os.getenv("Massive_API") or os.getenv("POLYGON_API_KEY")
    if not api_key:
        log.error("Massive_API not set, cannot poll REST API")
        return
    
    client = RESTClient(api_key)
    INDEX_SYMBOLS = ["SPX", "NDX", "DJI", "RUT"]
    
    log.info(f"Starting REST API polling (every {REST_POLL_INTERVAL}s - premium subscription, unlimited API calls)")
    
    # Track consecutive failures per symbol
    failure_counts = {s: 0 for s in INDEX_SYMBOLS}
    MAX_FAILURES = 10  # Stop retrying after 10 consecutive failures
    
    while True:
        try:
            from app.utils.market_time import market_is_closed as check_closed
            if check_closed():
                await asyncio.sleep(30)
                continue
            
            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(60)
                continue
            
            # Build list of tickers to fetch (excluding failed ones)
            tickers_to_fetch = [f"I:{s}" for s in INDEX_SYMBOLS if failure_counts[s] < MAX_FAILURES]
            
            if not tickers_to_fetch:
                log.warning("All index REST fetches have failed, waiting...")
                await asyncio.sleep(30)
                continue
            
            try:
                # Use get_snapshot_indices for batch fetching
                results = client.get_snapshot_indices(ticker_any_of=tickers_to_fetch)
                
                ts = int(time.time())
                fetched_symbols = set()
                
                # Convert iterator to list to avoid exhaustion issues
                results_list = list(results) if results else []
                
                if results_list:
                    _poll_count += 1
                    should_log = (_poll_count % LOG_EVERY_N_POLLS == 0)
                    
                    if should_log:
                        log.info(f"REST API poll #{_poll_count} - {len(results_list)} indices")
                    
                    for snapshot in results_list:
                        if hasattr(snapshot, 'ticker') and hasattr(snapshot, 'value') and snapshot.value:
                            # Extract symbol from ticker (e.g., "I:SPX" -> "SPX")
                            ticker = snapshot.ticker
                            symbol = ticker.replace("I:", "") if ticker.startswith("I:") else ticker
                            
                            if symbol in INDEX_SYMBOLS:
                                price = float(snapshot.value)
                                
                                # Create tick from REST data
                                tick = IndexTick(ts=ts, price=price, size=1.0)
                                
                                # Add to ring buffer
                                INDEX_RINGS[symbol].add(ts, tick)
                                update_session_vwap(symbol, price, 1.0)
                                
                                # Reset failure count on success
                                failure_counts[symbol] = 0
                                fetched_symbols.add(symbol)
                                
                                if should_log:
                                    log.info(f"{symbol}: ${price:.2f}")
                else:
                    log.warning("REST API returned no results")
                
                # Mark failures for symbols not in results
                for symbol in INDEX_SYMBOLS:
                    if symbol not in fetched_symbols and failure_counts[symbol] < MAX_FAILURES:
                        failure_counts[symbol] += 1
                        if failure_counts[symbol] <= 3:
                            log.warning(f"No REST data for {symbol} (failures: {failure_counts[symbol]})")
                            
            except Exception as e:
                log.warning(f"REST API batch error: {e}")
                # Increment all failure counts
                for symbol in INDEX_SYMBOLS:
                    if failure_counts[symbol] < MAX_FAILURES:
                        failure_counts[symbol] += 1
            
            # Poll at configured interval (1s with premium subscription)
            await asyncio.sleep(REST_POLL_INTERVAL)
            
        except Exception as e:
            log.error(f"REST polling error: {e}")
            await asyncio.sleep(REST_POLL_INTERVAL)


async def start_market_data_fallback():
    """
    Start the configured fallback poller.

    Provider precedence:
    1. explicit MARKET_DATA_PROVIDER (databento or polygon)
    2. auto mode: Databento when key exists, else Polygon
    """
    provider = resolve_index_data_provider()
    global CURRENT_MARKET_DATA_PROVIDER

    CURRENT_MARKET_DATA_PROVIDER = provider

    if provider == "databento":
        from app.ingest.databento_fallback import poll_databento_rest

        log.info("Fallback market data provider: Databento")
        await poll_databento_rest()
        return

    if provider == "polygon":
        log.info("Fallback market data provider: Polygon")
        await poll_polygon_rest()
        return

    log.error("No usable index data provider configured: missing Databento and Polygon credentials")

async def load_cached_snapshots():
    """
    Secondary fallback: Load cached index snapshots from database.
    Used when both WebSocket AND REST API fail.
    """
    from database import get_latest_gamma_snapshot
    
    INDEX_SYMBOLS = ["SPX", "NDX", "DJI", "RUT"]
    
    log.info("Starting database fallback for cached snapshots (WebSocket unavailable)")
    
    while True:
        try:
            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(60)
                continue
            
            # Load cached snapshots from database for each index
            for symbol in INDEX_SYMBOLS:
                try:
                    # Get latest stored gamma snapshot
                    snapshot = get_latest_gamma_snapshot(symbol)
                    
                    if snapshot:
                        # Use stored spot price as index value
                        price = snapshot.spot_price
                        ts = int(time.time())
                        
                        # Create tick from cached data
                        tick = IndexTick(ts=ts, price=price, size=1.0)
                        
                        # Add to ring buffer
                        INDEX_RINGS[symbol].add(ts, tick)
                        update_session_vwap(symbol, price, 1.0)
                        
                        log.debug(f"{symbol}: ${price:.2f} (cached snapshot, updated {snapshot.interval_timestamp})")
                        
                except Exception as e:
                    log.error(f"Error loading cached snapshot for {symbol}: {e}")
            
            # Refresh every 30 seconds (cached data, secondary fallback)
            await asyncio.sleep(30.0)
            
        except Exception as e:
            log.error(f"Database fallback error: {e}")
            await asyncio.sleep(30.0)
