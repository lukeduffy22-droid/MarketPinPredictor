"""
Options WebSocket streaming for real-time gamma exposure updates.
Connects to Polygon options WebSocket during market hours and falls back to REST EOD snapshots.
"""
import asyncio
import logging
import random
from datetime import datetime
from typing import Dict, Optional, Callable

from polygon import WebSocketClient, RESTClient
from polygon.websocket.models import WebSocketMessage, Market

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
    """
    
    def __init__(self):
        self.client = None
        self.reconnect_count = 0
        self.max_reconnects = settings.ws_max_reconnects
        self.running = False
        self.is_market_hours = False
        self._last_trade_time = 0
    
    async def message_handler(self, msg: WebSocketMessage):
        """Handle incoming options WebSocket messages"""
        try:
            msg_dict = msg.to_dict() if hasattr(msg, "to_dict") else msg
            ev = msg_dict.get("ev", "")
            
            if ev == "T":  # Options trade
                await self._process_options_trade(msg_dict)
            elif ev == "AM":  # Options minute aggregate
                await self._process_options_aggregate(msg_dict)
                
        except Exception as e:
            log.error(f"Error handling options message: {e}")
    
    async def _process_options_trade(self, msg: dict):
        """
        Process options trade message and update gamma tracker.
        
        Polygon format: O:SPX241108C06000000
        """
        try:
            sym = msg.get("sym", "")
            if not sym.startswith("O:"):
                return
            
            parts = sym[2:]  # Remove O:
            
            root = None
            for r in ("SPX", "NDX", "DJI", "RUT"):
                if parts.startswith(r):
                    root = r
                    break
            
            if not root:
                return
            
            cp_flag = parts[len(root)+6] if len(parts) > len(root)+6 else None
            if cp_flag not in ("C", "P"):
                return
            
            is_call = (cp_flag == "C")
            
            strike_str = parts[len(root)+7:]
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
        """Start options WebSocket stream"""
        self.running = True
        
        while self.running and self.reconnect_count < self.max_reconnects:
            try:
                dt_utc = datetime.utcnow()
                self.is_market_hours = is_regular_hours(dt_utc)
                
                if not self.is_market_hours:
                    log.info("Market closed - options WebSocket waiting for market hours")
                    await asyncio.sleep(60)
                    continue
                
                feed_type = "real-time" if self.is_market_hours else "delayed"
                log.info(f"Starting {feed_type} Options WebSocket feed (wss://socket.polygon.io/options)")
                
                self.client = WebSocketClient(
                    api_key=settings.polygon_api_key,
                    market=Market.Options,
                    feed="RealTime" if self.is_market_hours else "Delayed"
                )
                
                self.client.subscribe(
                    "T.O:SPX*",
                    "T.O:NDX*",
                    "T.O:DJI*",
                    "T.O:RUT*"
                )
                
                log.info("Connecting to Polygon Options WebSocket...")
                await self.client.connect(self.message_handler)
                log.info("Options WebSocket connected successfully")
                
            except Exception as e:
                log.error(f"Options WebSocket error: {e}")
                
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
        if self.client:
            await self.client.close()
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
