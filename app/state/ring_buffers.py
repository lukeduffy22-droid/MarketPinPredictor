"""
Per-symbol ring buffers for high-frequency index and options flow data.
Optimized for <200ms latency and minimal memory footprint.
"""
from collections import deque
from typing import Deque, Tuple, Any, Dict, Optional, NamedTuple
from dataclasses import dataclass
from datetime import date, datetime, timezone

class IndexTick(NamedTuple):
    """Normalized index price tick"""
    ts: int  # Unix timestamp (UTC)
    price: float
    size: float  # Treat as 1.0 if unavailable

@dataclass
class OptTrade:
    """Normalized options trade"""
    ts: int  # Unix timestamp (UTC)
    root: str  # SPX, NDX, DJI, RUT
    K: int  # Strike price
    is_call: bool
    exp: date  # Expiration date
    notional: float  # price * size
    aggressor: int  # +1 for buy, -1 for sell

class Ring1s:
    """Lock-free ring buffer with fixed capacity for 1-second aggregated data"""
    
    def __init__(self, capacity: int = 5400):  # 90 minutes
        self.q: Deque[Tuple[int, Any]] = deque(maxlen=capacity)
    
    def add(self, ts: int, v: Any):
        """Add timestamped value (auto-drops oldest)"""
        self.q.append((ts, v))
    
    def view(self) -> Deque[Tuple[int, Any]]:
        """Read-only view (do not mutate!)"""
        return self.q
    
    def latest(self) -> Optional[Tuple[int, Any]]:
        """Get most recent entry"""
        return self.q[-1] if self.q else None
    
    def is_fresh(self, max_age_seconds: int = 5) -> bool:
        """Check if latest data is within max_age_seconds"""
        if not self.q:
            return False
        import time
        latest_ts = self.q[-1][0]
        return (time.time() - latest_ts) <= max_age_seconds
    
    @property
    def length_seconds(self) -> int:
        """Number of seconds of data available"""
        return len(self.q)

# Backend-tracked live display symbols.
TRACKED_INDEX_SYMBOLS = ("SPX", "NDX", "DJI", "RUT", "VIX")
PREDICTION_SYMBOLS = ("SPX", "NDX", "DJI", "RUT")

# Global per-symbol ring buffers
INDEX_RINGS: Dict[str, Ring1s] = {
    s: Ring1s() for s in TRACKED_INDEX_SYMBOLS
}

FLOW_RINGS: Dict[str, Ring1s] = {
    r: Ring1s() for r in PREDICTION_SYMBOLS
}

# Session VWAP trackers (reset at market open)
_vwap_state: Dict[str, Dict[str, float]] = {
    s: {"sum_pv": 0.0, "sum_v": 0.0} for s in TRACKED_INDEX_SYMBOLS
}

def update_session_vwap(symbol: str, price: float, size: float = 1.0):
    """Update running VWAP calculation for symbol"""
    if symbol in _vwap_state:
        _vwap_state[symbol]["sum_pv"] += price * size
        _vwap_state[symbol]["sum_v"] += size


def record_live_index_tick(
    symbol: str,
    tick: IndexTick,
    *,
    update_vwap: bool = True,
) -> None:
    """Record a normalized live tick and update all session-derived state."""
    if symbol not in INDEX_RINGS or tick.price <= 0:
        return

    INDEX_RINGS[symbol].add(tick.ts, tick)
    if update_vwap:
        update_session_vwap(symbol, tick.price, tick.size)

    if symbol in PREDICTION_SYMBOLS:
        from app.state.orb_tracker import update_orb

        tick_time = datetime.fromtimestamp(tick.ts, tz=timezone.utc)
        update_orb(symbol, tick.price, tick_time)

def get_session_vwap(symbol: str) -> float:
    """Get current session VWAP for symbol"""
    state = _vwap_state.get(symbol, {})
    sum_v = state.get("sum_v", 0.0)
    if sum_v > 0:
        return state.get("sum_pv", 0.0) / sum_v
    
    # Fallback: compute from ring buffer
    ring = INDEX_RINGS.get(symbol)
    if not ring or not ring.q:
        return 0.0
    
    total_pv = sum(tick.price * tick.size for _, tick in ring.q if isinstance(tick, IndexTick))
    total_v = sum(tick.size for _, tick in ring.q if isinstance(tick, IndexTick))
    
    return total_pv / total_v if total_v > 0 else 0.0

def reset_session_vwap():
    """Reset VWAP trackers (call at market open)"""
    for symbol in _vwap_state:
        _vwap_state[symbol] = {"sum_pv": 0.0, "sum_v": 0.0}

def get_latest_price(symbol: str) -> Optional[float]:
    """Get latest price for symbol from ring buffer only"""
    ring = INDEX_RINGS.get(symbol)
    if ring:
        latest = ring.latest()
        if latest:
            _, tick = latest
            if isinstance(tick, IndexTick):
                return tick.price
            return tick  # Fallback for raw float
    return None

def get_latest_price_with_fallback(symbol: str, api_key: str = None) -> Optional[float]:
    """
    Get latest price for symbol with fallback sources for after-hours access.
    
    Priority:
    1. Ring buffer (real-time WebSocket data)
    2. Database (last gamma snapshot spot price)
    3. Polygon REST API (previous close)
    
    This ensures EOD data is available after hours for premium subscribers.
    """
    # Try ring buffer first (fastest, real-time)
    price = get_latest_price(symbol)
    if price is not None:
        return price
    
    # Fallback 1: Database - get last known spot price from gamma snapshot
    try:
        from database import get_latest_gamma_snapshot
        snapshot = get_latest_gamma_snapshot(symbol)
        if snapshot and snapshot.spot_price:
            return float(snapshot.spot_price)
    except Exception as e:
        import logging
        logging.getLogger("ring_buffers").warning(f"Database fallback failed for {symbol}: {e}")
    
    # Fallback 2: Polygon REST API - get previous close
    if api_key:
        try:
            from polygon.rest import RESTClient
            client = RESTClient(api_key)
            
            # Try to get the previous day's close for indices
            ticker = f"I:{symbol}"
            
            # Use previous close from snapshot endpoint
            response = client.get_previous_close(ticker)
            if response and hasattr(response, 'results') and response.results:
                result = response.results[0]
                if hasattr(result, 'close') and result.close:
                    return float(result.close)
        except Exception as e:
            import logging
            logging.getLogger("ring_buffers").warning(f"Polygon REST fallback failed for {symbol}: {e}")
    
    return None
