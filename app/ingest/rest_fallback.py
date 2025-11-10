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
    """
    client = RESTClient(settings.polygon_api_key)
    
    log.info("Starting REST API fallback for live data")
    
    while True:
        try:
            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(60)
                continue
            
            # Poll each index
            for symbol in ("SPX", "NDX", "DJI", "RUT"):
                try:
                    ticker = f"I:{symbol}"
                    
                    # Get latest quote
                    quote = client.get_last_quote(ticker)
                    
                    if quote and hasattr(quote, 'ask_price'):
                        # Use mid price
                        price = (quote.ask_price + quote.bid_price) / 2 if hasattr(quote, 'bid_price') else quote.ask_price
                        ts = int(time.time())
                        
                        # Create tick
                        tick = IndexTick(ts=ts, price=price, size=1.0)
                        
                        # Add to ring buffer
                        INDEX_RINGS[symbol].add(ts, tick)
                        update_session_vwap(symbol, price, 1.0)
                        
                        log.debug(f"{symbol}: ${price:.2f}")
                        
                except Exception as e:
                    log.error(f"Error polling {symbol}: {e}")
            
            # Poll every second
            await asyncio.sleep(1.0)
            
        except Exception as e:
            log.error(f"REST polling error: {e}")
            await asyncio.sleep(5.0)
