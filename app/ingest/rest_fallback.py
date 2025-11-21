"""
Database fallback for when WebSocket connection fails.
Loads cached snapshots instead of polling REST API.
Primary live data comes from Polygon WebSocket streaming.
"""
import asyncio
import logging
from datetime import datetime
import time

from app.state.ring_buffers import INDEX_RINGS, update_session_vwap, IndexTick
from app.utils.time_et import is_regular_hours

log = logging.getLogger("rest_fallback")

async def load_cached_snapshots():
    """
    Fallback: Load cached index snapshots from database when WebSocket fails.
    This is a graceful degradation - not real-time but prevents complete data loss.
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
            
            # Refresh every 30 seconds (cached data, no need to hammer)
            await asyncio.sleep(30.0)
            
        except Exception as e:
            log.error(f"Database fallback error: {e}")
            await asyncio.sleep(30.0)
