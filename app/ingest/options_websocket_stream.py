"""
Options WebSocket streaming for real-time gamma exposure updates.
Uses raw websockets library for Replit compatibility.
"""
import asyncio
import json
import logging
import random
from datetime import datetime
from typing import Dict, Optional, Callable

import websockets
from polygon import RESTClient

from app.utils.settings import settings
from app.utils.time_et import is_regular_hours

log = logging.getLogger("options_ws_stream")

_gamma_update_callback: Optional[Callable] = None

class OptionsGammaTracker:
    """
    Tracks real-time options trades and updates gamma exposure calculations.
    """
    
    def __init__(self):
        self.trade_counts: Dict[str, int] = {s: 0 for s in ("SPX", "NDX", "DJI", "RUT")}
        self.volume_by_strike: Dict[str, Dict[int, float]] = {
            s: {} for s in ("SPX", "NDX", "DJI", "RUT")
        }
        self.last_update_ts: Dict[str, float] = {s: 0.0 for s in ("SPX", "NDX", "DJI", "RUT")}
    
    def process_trade(self, root: str, strike: int, is_call: bool, notional: float, ts: int):
        """
        Process an incoming options trade and update gamma tracking.
        """
        if root not in self.trade_counts:
            return
        
        self.trade_counts[root] += 1
        self.last_update_ts[root] = ts
        
        if strike not in self.volume_by_strike[root]:
            self.volume_by_strike[root][strike] = 0.0
        
        self.volume_by_strike[root][strike] += notional
    
    def get_hot_strikes(self, symbol: str, top_n: int = 5) -> list:
        """Get the most active strikes by notional volume"""
        volumes = self.volume_by_strike.get(symbol, {})
        if not volumes:
            return []
        
        sorted_strikes = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
        return sorted_strikes[:top_n]
    
    def reset_daily(self):
        """Reset trackers at market open"""
        for symbol in self.trade_counts:
            self.trade_counts[symbol] = 0
            self.volume_by_strike[symbol] = {}
            self.last_update_ts[symbol] = 0.0

_gamma_tracker = OptionsGammaTracker()

def get_gamma_tracker() -> OptionsGammaTracker:
    """Get the global gamma tracker instance"""
    return _gamma_tracker


class OptionsWebSocketStream:
    """
    Manages dedicated options WebSocket connection for real-time gamma tracking.
    Uses raw websockets library for Replit compatibility.
    """
    
    def __init__(self):
        self.ws = None
        self.reconnect_count = 0
        self.max_reconnects = settings.ws_max_reconnects
        self.running = False
        self.is_market_hours = False
        self._last_trade_time = 0
        self.subscriptions = [
            "T.O:SPX*",   # SPX options trades
            "T.O:NDX*",   # NDX options trades
            "T.O:SPXW*",  # SPX weekly options
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
                log.error(f"Error processing options message: {e}")
    
    async def _process_message(self, msg: dict):
        """Process a single message"""
        ev = msg.get("ev")
        
        if ev == "status":
            status = msg.get("status")
            message = msg.get("message", "")
            
            if status == "connected":
                log.info(f"Options WebSocket connected: {message}")
            elif status == "auth_success":
                log.info("Options WebSocket authentication successful - subscribing to channels")
                await self._send("subscribe", ",".join(self.subscriptions))
            elif status == "auth_failed":
                log.error(f"Options WebSocket authentication failed: {message}")
            elif status == "success":
                log.info(f"Options subscription confirmed: {message}")
            else:
                log.debug(f"Options status: {status} - {message}")
        
        elif ev == "T":
            await self._process_options_trade(msg)
        
        elif ev == "AM":
            await self._process_options_aggregate(msg)
    
    async def _process_options_trade(self, msg: dict):
        """
        Process options trade message and update gamma tracker.
        
        Polygon format: O:SPX241108C06000000
        """
        try:
            sym = msg.get("sym", "")
            if not sym.startswith("O:"):
                return
            
            parts = sym[2:]
            
            root = None
            for r in ("SPXW", "SPX", "NDX", "DJI", "RUT"):
                if parts.startswith(r):
                    root = "SPX" if r == "SPXW" else r
                    break
            
            if not root:
                return
            
            parts_after_root = parts[len(root if root != "SPX" else ("SPXW" if parts.startswith("SPXW") else "SPX")):]
            
            if len(parts_after_root) < 8:
                return
            
            cp_flag = parts_after_root[6] if len(parts_after_root) > 6 else None
            if cp_flag not in ("C", "P"):
                return
            
            is_call = (cp_flag == "C")
            
            strike_str = parts_after_root[7:]
            if not strike_str.isdigit():
                return
            strike = int(strike_str) // 1000
            
            price = msg.get("p", 0)
            size = msg.get("s", 1)
            ts = msg.get("t", 0) // 1000 if msg.get("t") else 0
            
            notional = price * size * 100
            
            _gamma_tracker.process_trade(root, strike, is_call, notional, ts)
            self._last_trade_time = ts
            
            if _gamma_update_callback:
                _gamma_update_callback(root, strike, is_call, notional)
                
        except Exception as e:
            log.warning(f"Failed to process options trade: {e}")
    
    async def _process_options_aggregate(self, msg: dict):
        """Process options minute aggregate (for volume tracking)"""
        pass
    
    async def start(self):
        """Start options WebSocket stream
        
        FREEZE GUARD: Disabled when market is closed.
        """
        self.running = True
        api_key = settings.polygon_api_key
        
        if not api_key:
            log.error("Massive_API not set, cannot start Options WebSocket")
            return
        
        try:
            from app.utils.market_time import market_is_closed, get_freeze_status
            if market_is_closed():
                is_frozen, reason = get_freeze_status()
                log.warning(f"MARKET CLOSED — Options WebSocket disabled: {reason}")
                return
        except ImportError:
            pass
        
        while self.running and self.reconnect_count < self.max_reconnects:
            try:
                from app.utils.market_time import market_is_closed as check_closed
                if check_closed():
                    log.warning("Market closed during Options WebSocket — terminating")
                    return
                
                dt_utc = datetime.utcnow()
                self.is_market_hours = is_regular_hours(dt_utc)
                
                if not self.is_market_hours:
                    log.info("Market closed - Options WebSocket waiting for market hours")
                    await asyncio.sleep(60)
                    continue
                
                endpoint = "wss://socket.polygon.io/options"
                log.info(f"Connecting to Options WebSocket: {endpoint}")
                
                async with websockets.connect(
                    endpoint,
                    ping_interval=30,
                    ping_timeout=30,
                    close_timeout=10
                ) as ws:
                    self.ws = ws
                    self.reconnect_count = 0
                    
                    log.info("Authenticating with Polygon Options...")
                    await self._send("auth", api_key)
                    
                    await self._handle_messages()
                    
            except websockets.exceptions.ConnectionClosed as e:
                log.warning(f"Options WebSocket connection closed: {e}")
            except Exception as e:
                log.error(f"Options WebSocket error: {type(e).__name__}: {e}")
            
            if self.running:
                self.reconnect_count += 1
                backoff = min(60, settings.ws_backoff_base ** self.reconnect_count)
                jitter = backoff * settings.ws_backoff_jitter * random.random()
                wait_time = backoff + jitter
                
                log.info(f"Options WS reconnecting in {wait_time:.1f}s (attempt {self.reconnect_count}/{self.max_reconnects})")
                await asyncio.sleep(wait_time)
        
        if self.reconnect_count >= self.max_reconnects:
            log.error("Options WebSocket max reconnection attempts reached")
    
    async def stop(self):
        """Stop options WebSocket stream"""
        self.running = False
        if self.ws:
            await self.ws.close()
            log.info("Options WebSocket stream stopped")


_options_stream = None

async def start_options_websocket_stream():
    """Start the global options WebSocket stream"""
    global _options_stream
    
    if _options_stream is None:
        _options_stream = OptionsWebSocketStream()
    
    await _options_stream.start()

async def stop_options_websocket_stream():
    """Stop the global options WebSocket stream"""
    global _options_stream
    
    if _options_stream:
        await _options_stream.stop()

def set_gamma_update_callback(callback: Callable):
    """Set callback for real-time gamma updates"""
    global _gamma_update_callback
    _gamma_update_callback = callback

async def fetch_eod_options_snapshot(symbol: str, spot_price: float) -> dict:
    """
    Fetch EOD options snapshot from Polygon REST API when market is closed.
    
    Returns gamma exposure data for display after hours.
    """
    try:
        client = RESTClient(settings.polygon_api_key)
        
        log.info(f"Fetching EOD options snapshot for {symbol}...")
        
        snapshot = client.list_snapshot_options_chain(symbol)
        
        options_data = []
        for contract in snapshot:
            try:
                if not hasattr(contract, 'details'):
                    continue
                
                details = contract.details
                strike = float(details.strike_price) if hasattr(details, 'strike_price') else 0
                option_type = details.contract_type.lower() if hasattr(details, 'contract_type') else ''
                
                if strike <= 0:
                    continue
                
                oi = int(contract.open_interest) if hasattr(contract, 'open_interest') and contract.open_interest else 0
                iv = float(contract.implied_volatility) if hasattr(contract, 'implied_volatility') and contract.implied_volatility else 0.25
                
                options_data.append({
                    'strike': strike,
                    'type': option_type,
                    'open_interest': oi,
                    'implied_volatility': iv
                })
                
            except Exception:
                continue
        
        log.info(f"Fetched {len(options_data)} option contracts for {symbol}")
        
        return {
            'symbol': symbol,
            'spot_price': spot_price,
            'contracts': options_data,
            'is_realtime': False,
            'source': 'EOD_SNAPSHOT'
        }
        
    except Exception as e:
        log.error(f"Failed to fetch EOD snapshot for {symbol}: {e}")
        return {
            'symbol': symbol,
            'spot_price': spot_price,
            'contracts': [],
            'is_realtime': False,
            'source': 'ERROR',
            'error': str(e)
        }
