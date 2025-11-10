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
    Uses ETF proxies (SPY, QQQ, DIA, IWM) for index data.
    """
    client = RESTClient(settings.polygon_api_key)
    
    # ETF proxies for indices (works with most API tiers)
    ETF_PROXIES = {
        "SPX": "SPY",
        "NDX": "QQQ",
        "DJI": "DIA",
        "RUT": "IWM"
    }
    
    log.info("Starting REST API fallback for live data (using ETF proxies)")
    
    while True:
        try:
            if not is_regular_hours(datetime.utcnow()):
                await asyncio.sleep(60)
                continue
            
            # Poll each index via ETF proxy
            for symbol, etf in ETF_PROXIES.items():
                try:
                    # Get latest quote for ETF
                    quote = client.get_last_quote(etf)
                    
                    if quote and hasattr(quote, 'ask_price'):
                        # Use mid price
                        price = (quote.ask_price + quote.bid_price) / 2 if hasattr(quote, 'bid_price') else quote.ask_price
                        
                        # Scale ETF price to index (approximate)
                        # SPY ~= SPX/10, QQQ ~= NDX/10, DIA ~= DJI/100, IWM ~= RUT
                        scale_factors = {"SPX": 10, "NDX": 10, "DJI": 100, "RUT": 1}
                        scaled_price = price * scale_factors.get(symbol, 1)
                        
                        ts = int(time.time())
                        
                        # Create tick
                        tick = IndexTick(ts=ts, price=scaled_price, size=1.0)
                        
                        # Add to ring buffer
                        INDEX_RINGS[symbol].add(ts, tick)
                        update_session_vwap(symbol, scaled_price, 1.0)
                        
                        log.debug(f"{symbol} (via {etf}): ${scaled_price:.2f}")
                        
                except Exception as e:
                    log.error(f"Error polling {symbol} via {etf}: {e}")
            
            # Poll every second
            await asyncio.sleep(1.0)
            
        except Exception as e:
            log.error(f"REST polling error: {e}")
            await asyncio.sleep(5.0)
