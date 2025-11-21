"""
REST API fallback for live data when WebSocket fails.
Polls Polygon REST API every second to populate ring buffers.
"""
import asyncio
import logging
from datetime import datetime
import time
from polygon import RESTClient

from app.utils.settings import settings
from app.state.ring_buffers import INDEX_RINGS, update_session_vwap, IndexTick
from app.utils.time_et import is_regular_hours

log = logging.getLogger("rest_fallback")

async def poll_rest_data():
    """
    Fallback: Poll REST API for live quotes every second.
    Use when WebSocket connection fails.
    Uses direct index tickers (I:SPX, I:NDX, I:DJI, I:RUT).
    """
    client = RESTClient(settings.polygon_api_key)
    
    # Direct index tickers (requires indices subscription)
    INDEX_TICKERS = {
        "SPX": "I:SPX",
        "NDX": "I:NDX",
        "DJI": "I:DJI",
        "RUT": "I:RUT"
    }
    
    log.info("Starting REST API fallback for live data (using direct index tickers)")
    
    while True:
        try:
            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(60)
                continue
            
            # Poll each index directly using indices snapshot API
            for symbol, ticker in INDEX_TICKERS.items():
                try:
                    # Get index snapshot using get_snapshot_indices method
                    snapshot = client.get_snapshot_indices(ticker_any_of=ticker)
                    
                    # Response is a list - get first result
                    if snapshot and len(snapshot) > 0:
                        result = snapshot[0]
                        if hasattr(result, 'value'):
                            # Use the index value
                            price = result.value
                            
                            ts = int(time.time())
                            
                            # Create tick
                            tick = IndexTick(ts=ts, price=price, size=1.0)
                            
                            # Add to ring buffer
                            INDEX_RINGS[symbol].add(ts, tick)
                            update_session_vwap(symbol, price, 1.0)
                            
                            log.info(f"{symbol}: ${price:.2f} (real-time index data)")
                        
                except Exception as e:
                    log.error(f"Error polling {symbol} ({ticker}): {e}")
            
            # Poll every second
            await asyncio.sleep(1.0)
            
        except Exception as e:
            log.error(f"REST polling error: {e}")
            await asyncio.sleep(5.0)
