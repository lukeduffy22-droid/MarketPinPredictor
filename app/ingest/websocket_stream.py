"""
WebSocket streaming with intelligent feed switching and reconnection logic.
Uses raw websockets library for maximum compatibility with Replit environment.
"""
import asyncio
import json
import logging
import random
from datetime import datetime
import os

import websockets

from app.utils.settings import settings
from app.utils.time_et import is_regular_hours
from app.ingest.websocket_aggregator import ingest_message
from app.ingest.rest_fallback import set_websocket_connected

log = logging.getLogger("ws_stream")

class PolygonWebSocketStream:
    """
    Manages WebSocket connection with smart feed switching and reconnection.
    Uses raw websockets library for Replit compatibility.
    """
    
    def __init__(self):
        self.ws = None
        self.reconnect_count = 0
        self.max_reconnects = settings.ws_max_reconnects
        self.running = False
        self.connected = False
        # Use Value updates (V.) instead of Aggregates (A.) - more widely available
        # V.I:XXX = real-time index value updates
        # A.I:XXX = aggregate bars - requires higher tier plan
        self.subscriptions = [
            "V.I:SPX",    # S&P 500 value updates
            "V.I:NDX",    # NASDAQ 100 value updates
            "V.I:DJI",    # Dow Jones value updates
            "V.I:RUT",    # Russell 2000 value updates
        ]
    
    async def _send(self, action: str, params: str = None):
        """Send a message to WebSocket"""
        msg = {"action": action}
        if params:
            msg["params"] = params
        await self.ws.send(json.dumps(msg))
    
    async def _handle_messages(self):
        """Handle incoming WebSocket messages"""
        async for message in self.ws:
            try:
                data = json.loads(message)
                
                if isinstance(data, list):
                    for msg in data:
                        await self._process_message(msg)
                else:
                    await self._process_message(data)
                    
            except json.JSONDecodeError as e:
                log.warning(f"Invalid JSON message: {e}")
            except Exception as e:
                log.error(f"Error processing message: {e}")
    
    async def _process_message(self, msg: dict):
        """Process a single message"""
        ev = msg.get("ev")
        
        if ev == "status":
            status = msg.get("status")
            message = msg.get("message", "")
            
            if status == "connected":
                log.info(f"WebSocket connected: {message}")
            elif status == "auth_success":
                log.info("WebSocket authentication successful - subscribing to channels")
                self.connected = True
                set_websocket_connected(True)  # Mark WebSocket as active
                # Subscribe to index aggregates
                await self._send("subscribe", ",".join(self.subscriptions))
            elif status == "auth_failed":
                log.error(f"WebSocket authentication failed: {message}")
                self.connected = False
            elif status == "success":
                log.info(f"Subscription confirmed: {message}")
            else:
                log.debug(f"Status message: {status} - {message}")
        
        elif ev == "V":
            # Index value update - primary feed type
            await ingest_message(msg)
        
        elif ev == "AM" or ev == "A":
            # Index aggregate message - pass to ingest
            await ingest_message(msg)
        
        elif ev == "T":
            # Trade message
            await ingest_message(msg)
        
        else:
            # Other message types - pass through
            if ev:
                await ingest_message(msg)
    
    async def start(self):
        """Start WebSocket stream with automatic reconnection"""
        self.running = True
        api_key = settings.polygon_api_key
        
        if not api_key:
            log.error("POLYGON_API_KEY not set, cannot start WebSocket")
            return
        
        while self.running and self.reconnect_count < self.max_reconnects:
            try:
                # Determine which feed to use
                dt_utc = datetime.utcnow()
                use_realtime = is_regular_hours(dt_utc)
                
                endpoint = "wss://socket.polygon.io/indices" if use_realtime else "wss://delayed.polygon.io/indices"
                feed_type = "real-time" if use_realtime else "delayed"
                
                log.info(f"Connecting to {feed_type} WebSocket: {endpoint}")
                
                async with websockets.connect(
                    endpoint,
                    ping_interval=30,
                    ping_timeout=30,
                    close_timeout=10
                ) as ws:
                    self.ws = ws
                    self.connected = False
                    self.reconnect_count = 0  # Reset on successful connect
                    
                    # Authenticate
                    log.info("Authenticating with Polygon...")
                    await self._send("auth", api_key)
                    
                    # Handle messages until connection closes
                    await self._handle_messages()
                    
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"WebSocket connection closed: {e}")
                set_websocket_connected(False)
            except Exception as e:
                log.error(f"WebSocket error: {type(e).__name__}: {e}")
                set_websocket_connected(False)
            
            if self.running:
                # Exponential backoff with jitter
                self.reconnect_count += 1
                backoff = min(60, settings.ws_backoff_base ** self.reconnect_count)
                jitter = backoff * settings.ws_backoff_jitter * random.random()
                wait_time = backoff + jitter
                
                log.info(f"Reconnecting in {wait_time:.1f}s (attempt {self.reconnect_count}/{self.max_reconnects})")
                await asyncio.sleep(wait_time)
        
        if self.reconnect_count >= self.max_reconnects:
            log.error("Max WebSocket reconnection attempts reached - falling back to REST")
    
    async def stop(self):
        """Stop WebSocket stream"""
        self.running = False
        if self.ws:
            await self.ws.close()
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
