"""
WebSocket streaming with intelligent feed switching and reconnection logic.
Uses raw websockets library for maximum compatibility with Replit environment.

SINGLETON PATTERN: Only one WebSocket connection per API key.
SUBSCRIPTION CACHE: Channels are only subscribed once per connection.
"""
import asyncio
import json
import logging
import random
import threading
from datetime import datetime
import os

import websockets

from app.utils.settings import settings
from app.utils.time_et import is_regular_hours
from app.ingest.websocket_aggregator import ingest_message
from app.ingest.rest_fallback import set_websocket_connected

log = logging.getLogger("ws_stream")

# Singleton lock to prevent race conditions
_singleton_lock = threading.Lock()

class PolygonWebSocketStream:
    """
    Manages WebSocket connection with smart feed switching and reconnection.
    Uses raw websockets library for Replit compatibility.
    
    SINGLETON: Only one instance should exist per API key.
    SUBSCRIPTION CACHE: Prevents duplicate subscribe messages.
    """
    
    def __init__(self):
        self.ws = None
        self.reconnect_count = 0
        self.max_reconnects = settings.ws_max_reconnects
        self.running = False
        self.connected = False
        self.authenticated = False  # Track auth state
        # Subscription cache - prevents duplicate subscriptions (Polygon 1008 error)
        self._subscribed_channels: set = set()
        # Use Value updates (V.) instead of Aggregates (A.) - more widely available
        # V.I:XXX = real-time index value updates
        # A.I:XXX = aggregate bars - requires higher tier plan
        self.subscriptions = [
            "V.I:SPX",    # S&P 500 value updates
            "V.I:NDX",    # NASDAQ 100 value updates
            "V.I:DJI",    # Dow Jones value updates
            "V.I:RUT",    # Russell 2000 value updates
            "V.I:VIX",    # Volatility index value updates
        ]
    
    def is_active(self) -> bool:
        """Check if WebSocket is already active (connected + authenticated + subscribed)"""
        return (
            self.ws is not None and 
            self.connected and 
            self.authenticated and 
            len(self._subscribed_channels) > 0
        )
    
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
                self.connected = True
                self.authenticated = True
                set_websocket_connected(True)  # Mark WebSocket as active
                
                # Subscribe only to channels not already subscribed (prevents 1008 error)
                channels_to_subscribe = [ch for ch in self.subscriptions if ch not in self._subscribed_channels]
                if channels_to_subscribe:
                    log.info(f"WebSocket authenticated - subscribing to {len(channels_to_subscribe)} new channels")
                    await self._send("subscribe", ",".join(channels_to_subscribe))
                else:
                    log.info("WebSocket authenticated - all channels already subscribed, skipping")
            elif status == "auth_failed":
                log.error(f"WebSocket authentication failed: {message}")
                self.connected = False
                self.authenticated = False
            elif status == "success":
                # Track successful subscriptions in cache
                if "subscribed to:" in message:
                    channel = message.replace("subscribed to: ", "").strip()
                    self._subscribed_channels.add(channel)
                log.info(f"Subscription confirmed: {message}")
            else:
                log.debug(f"Status message: {status} - {message}")
        
        elif ev == "V":
            # Index value update - primary feed type
            # Continuous freeze check
            try:
                from app.utils.market_time import market_is_closed
                if market_is_closed():
                    log.warning("Market closed — dropping incoming data")
                    self.running = False
                    return
            except ImportError:
                pass
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
        """Start WebSocket stream with automatic reconnection
        
        GUARD: Returns immediately if already active (connected + authenticated + subscribed).
        This prevents duplicate connections and the Polygon 1008 error.
        """
        # GUARD: Return immediately if already active
        if self.is_active():
            log.info("WebSocket already active - skipping connection (prevents 1008 error)")
            return
        
        # GUARD: Prevent concurrent start attempts
        if self.running:
            log.info("WebSocket start already in progress - skipping")
            return
        
        self.running = True
        api_key = settings.polygon_api_key
        
        if not api_key:
            log.error("Massive_API not set, cannot start WebSocket")
            self.running = False
            return
        
        try:
            from app.utils.market_time import market_is_closed, get_freeze_status
            if market_is_closed():
                is_frozen, reason = get_freeze_status()
                log.warning(f"MARKET CLOSED — WebSocket disabled: {reason}")
                log.warning("Live feeds disabled — use frozen snapshots only")
                self.running = False
                return
        except ImportError:
            pass
        
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
        """Stop WebSocket stream and reset state"""
        self.running = False
        self.connected = False
        self.authenticated = False
        # Keep subscription cache - prevents resubscribing on reconnect
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
            log.info("WebSocket stream stopped")

# SINGLETON: Global stream instance - only one can exist
_ws_stream: PolygonWebSocketStream = None

async def start_websocket_stream():
    """Start the global WebSocket stream (singleton pattern)
    
    SINGLETON: Only creates one instance. If already running, returns immediately.
    This function is safe to call multiple times without creating duplicates.
    """
    global _ws_stream
    
    with _singleton_lock:
        if _ws_stream is None:
            _ws_stream = PolygonWebSocketStream()
            log.info("Created singleton WebSocket instance")
        elif _ws_stream.is_active():
            log.info("WebSocket singleton already active - no action needed")
            return
    
    await _ws_stream.start()

async def stop_websocket_stream():
    """Stop the global WebSocket stream"""
    global _ws_stream
    
    if _ws_stream:
        await _ws_stream.stop()

def is_websocket_active() -> bool:
    """Check if the singleton WebSocket is currently active"""
    global _ws_stream
    return _ws_stream is not None and _ws_stream.is_active()


def get_websocket_subscription_state() -> dict:
    """Return current index websocket subscription state for diagnostics."""
    global _ws_stream
    if _ws_stream is None:
        return {
            "stream": "index",
            "running": False,
            "connected": False,
            "authenticated": False,
            "requested_count": 0,
            "requested_subscriptions": [],
            "confirmed_count": 0,
            "confirmed_subscriptions": [],
        }

    requested = sorted(list(_ws_stream.subscriptions))
    confirmed = sorted(list(_ws_stream._subscribed_channels))
    return {
        "stream": "index",
        "running": bool(_ws_stream.running),
        "connected": bool(_ws_stream.connected),
        "authenticated": bool(_ws_stream.authenticated),
        "requested_count": len(requested),
        "requested_subscriptions": requested,
        "confirmed_count": len(confirmed),
        "confirmed_subscriptions": confirmed,
    }
