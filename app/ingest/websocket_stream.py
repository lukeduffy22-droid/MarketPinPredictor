"""
WebSocket streaming with intelligent feed switching and reconnection logic.
Automatically switches between real-time and delayed feeds based on market hours.
"""
import asyncio
import logging
import random
from datetime import datetime
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Market

from app.utils.settings import settings
from app.utils.time_et import is_regular_hours
from app.ingest.websocket_aggregator import ingest_message

log = logging.getLogger("ws_stream")

class PolygonWebSocketStream:
    """
    Manages WebSocket connection with smart feed switching and reconnection.
    """
    
    def __init__(self):
        self.client = None
        self.reconnect_count = 0
        self.max_reconnects = settings.ws_max_reconnects
        self.running = False
    
    async def message_handler(self, msg: WebSocketMessage):
        """Handle incoming WebSocket messages"""
        try:
            # Convert to dict and ingest
            msg_dict = msg.to_dict() if hasattr(msg, "to_dict") else msg
            await ingest_message(msg_dict)
        except Exception as e:
            log.error(f"Error handling message: {e}")
    
    async def start(self):
        """Start WebSocket stream with automatic reconnection"""
        self.running = True
        
        while self.running and self.reconnect_count < self.max_reconnects:
            try:
                # Determine which feed to use
                dt_utc = datetime.utcnow()
                use_realtime = is_regular_hours(dt_utc)
                
                feed_type = "real-time" if use_realtime else "delayed"
                log.info(f"Starting {feed_type} WebSocket feed")
                
                # Create client
                self.client = WebSocketClient(
                    api_key=settings.polygon_api_key,
                    market=Market.Stocks,
                    feed="RealTime" if use_realtime else "Delayed"
                )
                
                # Subscribe to index aggregates and options trades
                self.client.subscribe(
                    "A.I:SPX",    # S&P 500 aggregates
                    "A.I:NDX",    # NASDAQ 100 aggregates
                    "A.I:DJI",    # Dow Jones aggregates
                    "A.I:RUT",    # Russell 2000 aggregates
                    "T.O:SPX*",   # SPX options trades (wildcard)
                    "T.O:NDX*",   # NDX options trades
                    "T.O:DJI*",   # DJI options trades
                    "T.O:RUT*"    # RUT options trades
                )
                
                # Run client
                await self.client.connect(self.message_handler)
                
            except Exception as e:
                log.error(f"WebSocket error: {e}")
                
                # Exponential backoff with jitter
                self.reconnect_count += 1
                backoff = min(60, settings.ws_backoff_base ** self.reconnect_count)
                jitter = backoff * settings.ws_backoff_jitter * random.random()
                wait_time = backoff + jitter
                
                log.info(f"Reconnecting in {wait_time:.1f}s (attempt {self.reconnect_count}/{self.max_reconnects})")
                await asyncio.sleep(wait_time)
        
        if self.reconnect_count >= self.max_reconnects:
            log.error("Max reconnection attempts reached")
    
    async def stop(self):
        """Stop WebSocket stream"""
        self.running = False
        if self.client:
            await self.client.close()
            log.info("WebSocket stream stopped")

# Global stream instance
_ws_stream = None

async def start_websocket_stream():
    """Start the global WebSocket stream"""
    global _ws_stream
    
    if _ws_stream is None:
        _ws_stream = PolygonWebSocketStream()
    
    await _ws_stream.start()

async def stop_websocket_stream():
    """Stop the global WebSocket stream"""
    global _ws_stream
    
    if _ws_stream:
        await _ws_stream.stop()
