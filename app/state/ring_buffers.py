"""
Ring buffer state management for high-frequency trading data.
Uses lock-free deque for optimal performance during market hours.
"""
from collections import deque
from typing import Tuple, List, Optional, Dict
from datetime import datetime
import threading

class RingBuffer:
    """Lock-free ring buffer with fixed capacity"""
    
    def __init__(self, capacity: int = 5400):  # 90 minutes at 1-second resolution
        self.capacity = capacity
        self.buffer = deque(maxlen=capacity)
        self._lock = threading.RLock()  # For thread safety when needed
        
    def add(self, timestamp: float, value: float) -> None:
        """Add a new data point (automatically drops oldest if full)"""
        self.buffer.append((timestamp, value))
        
    def view(self) -> deque:
        """Get read-only view of buffer (do not mutate!)"""
        return self.buffer
        
    def get_last_n_seconds(self, n: int) -> List[Tuple[float, float]]:
        """Get data from last n seconds"""
        if not self.buffer:
            return []
        
        cutoff = self.buffer[-1][0] - n if self.buffer else 0
        return [(ts, val) for ts, val in self.buffer if ts >= cutoff]
    
    def get_latest(self) -> Optional[Tuple[float, float]]:
        """Get most recent data point"""
        return self.buffer[-1] if self.buffer else None
    
    def clear(self) -> None:
        """Clear all data"""
        with self._lock:
            self.buffer.clear()
    
    @property
    def size(self) -> int:
        """Current number of elements"""
        return len(self.buffer)


class OptionsFlowBuffer:
    """Specialized buffer for options flow data"""
    
    def __init__(self, capacity: int = 5400):
        self.capacity = capacity
        self.buffer = deque(maxlen=capacity)
        self._stats = {
            'total_volume': 0,
            'buy_volume': 0,
            'sell_volume': 0,
            'call_volume': 0,
            'put_volume': 0
        }
        
    def add(self, timestamp: float, data: Dict) -> None:
        """
        Add options trade data.
        
        Args:
            timestamp: Unix timestamp
            data: Dict with keys: symbol, strike, expiry, is_call, volume, side, notional
        """
        self.buffer.append((timestamp, data))
        
        # Update running statistics
        if data.get('side') == 'buy':
            self._stats['buy_volume'] += data.get('volume', 0)
        else:
            self._stats['sell_volume'] += data.get('volume', 0)
            
        if data.get('is_call'):
            self._stats['call_volume'] += data.get('volume', 0)
        else:
            self._stats['put_volume'] += data.get('volume', 0)
            
        self._stats['total_volume'] += data.get('volume', 0)
    
    def get_flow_last_n_seconds(self, n: int) -> List[Dict]:
        """Get options flow from last n seconds"""
        if not self.buffer:
            return []
            
        cutoff = self.buffer[-1][0] - n if self.buffer else 0
        return [data for ts, data in self.buffer if ts >= cutoff]
    
    def get_net_delta_flow(self, seconds: int = 60) -> float:
        """Calculate net delta-adjusted flow for last n seconds"""
        flow = self.get_flow_last_n_seconds(seconds)
        
        net = 0.0
        for trade in flow:
            # Delta-adjust the notional
            delta = trade.get('delta', 0.5)  # Default 0.5 for ATM
            side_mult = 1 if trade.get('side') == 'buy' else -1
            option_mult = 1 if trade.get('is_call') else -1
            
            net += side_mult * option_mult * delta * trade.get('notional', 0)
        
        return net
    
    @property
    def stats(self) -> Dict:
        """Get current statistics"""
        return self._stats.copy()


# Global ring buffers - shared across the application
BUFF_SECS = 5400  # 90 minutes

# Index price ticks
index_buffers = {
    'SPX': RingBuffer(BUFF_SECS),
    'NDX': RingBuffer(BUFF_SECS),
    'DJI': RingBuffer(BUFF_SECS),
    'RUT': RingBuffer(BUFF_SECS)
}

# Options flow buffers
options_buffers = {
    'SPX': OptionsFlowBuffer(BUFF_SECS),
    'NDX': OptionsFlowBuffer(BUFF_SECS),
    'DJI': OptionsFlowBuffer(BUFF_SECS),
    'RUT': OptionsFlowBuffer(BUFF_SECS)
}

# VWAP tracking
vwap_buffers = {
    'SPX': {'sum_pv': 0.0, 'sum_v': 0.0, 'vwap': 0.0},
    'NDX': {'sum_pv': 0.0, 'sum_v': 0.0, 'vwap': 0.0},
    'DJI': {'sum_pv': 0.0, 'sum_v': 0.0, 'vwap': 0.0},
    'RUT': {'sum_pv': 0.0, 'sum_v': 0.0, 'vwap': 0.0}
}

def update_vwap(symbol: str, price: float, volume: float = 1.0):
    """Update VWAP calculation for symbol"""
    if symbol in vwap_buffers:
        vwap_buffers[symbol]['sum_pv'] += price * volume
        vwap_buffers[symbol]['sum_v'] += volume
        if vwap_buffers[symbol]['sum_v'] > 0:
            vwap_buffers[symbol]['vwap'] = vwap_buffers[symbol]['sum_pv'] / vwap_buffers[symbol]['sum_v']

def reset_session_vwap():
    """Reset VWAP at market open"""
    for symbol in vwap_buffers:
        vwap_buffers[symbol] = {'sum_pv': 0.0, 'sum_v': 0.0, 'vwap': 0.0}

def get_latest_price(symbol: str) -> Optional[float]:
    """Get latest price for symbol"""
    if symbol in index_buffers:
        latest = index_buffers[symbol].get_latest()
        return latest[1] if latest else None
    return None

def get_session_vwap(symbol: str) -> float:
    """Get current session VWAP"""
    return vwap_buffers.get(symbol, {}).get('vwap', 0.0)