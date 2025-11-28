"""
Opening Range Breakout (ORB) Tracker

Tracks the 1-hour opening range (9:30-10:30 AM ET) for each trading day.
Provides ORB features for ML prediction enhancement:
- ORB High/Low levels
- Current position within range (0-1 scale)
- Breakout direction (bullish/bearish/inside)
- Range width as percentage of opening price
"""

from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, Optional, Tuple
import logging

from app.utils.time_et import now_et, open_time_et, ET

log = logging.getLogger("orb_tracker")

# ORB period: 9:30 AM - 10:30 AM ET (first 60 minutes)
ORB_DURATION_MINUTES = 60


@dataclass
class ORBData:
    """Opening Range Breakout data for a single trading day"""
    date: date
    symbol: str
    opening_price: Optional[float] = None
    orb_high: Optional[float] = None
    orb_low: Optional[float] = None
    orb_complete: bool = False
    tick_count: int = 0
    
    @property
    def range_width(self) -> float:
        """ORB range width in price terms"""
        if self.orb_high and self.orb_low:
            return self.orb_high - self.orb_low
        return 0.0
    
    @property
    def range_width_pct(self) -> float:
        """ORB range width as percentage of opening price"""
        if self.opening_price and self.opening_price > 0 and self.range_width > 0:
            return (self.range_width / self.opening_price) * 100
        return 0.0
    
    @property
    def midpoint(self) -> float:
        """ORB midpoint (50% level)"""
        if self.orb_high and self.orb_low:
            return (self.orb_high + self.orb_low) / 2
        return 0.0
    
    def position_in_range(self, current_price: float) -> float:
        """
        Returns position within the ORB range:
        - 0.0 = at ORB low
        - 0.5 = at midpoint
        - 1.0 = at ORB high
        - >1.0 = above ORB high (bullish breakout)
        - <0.0 = below ORB low (bearish breakout)
        """
        if not self.range_width or self.range_width == 0:
            return 0.5  # Default to neutral
        return (current_price - self.orb_low) / self.range_width
    
    def breakout_direction(self, current_price: float) -> str:
        """
        Determine breakout status:
        - 'bullish': Price above ORB high
        - 'bearish': Price below ORB low  
        - 'inside': Price within ORB range
        - 'unknown': ORB not complete
        """
        if not self.orb_complete:
            return 'forming'
        
        if current_price > self.orb_high:
            return 'bullish'
        elif current_price < self.orb_low:
            return 'bearish'
        else:
            return 'inside'
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for API/UI"""
        return {
            'date': self.date.isoformat(),
            'symbol': self.symbol,
            'opening_price': self.opening_price,
            'orb_high': self.orb_high,
            'orb_low': self.orb_low,
            'range_width': self.range_width,
            'range_width_pct': self.range_width_pct,
            'midpoint': self.midpoint,
            'orb_complete': self.orb_complete,
            'tick_count': self.tick_count
        }


@dataclass 
class ORBFeatures:
    """ORB-derived features for ML prediction"""
    position_in_range: float  # 0-1 scale, can exceed if breakout
    breakout_direction: str  # 'bullish', 'bearish', 'inside', 'forming'
    range_width_pct: float  # Range as % of opening price
    distance_to_high_pct: float  # Distance to ORB high as %
    distance_to_low_pct: float  # Distance to ORB low as %
    orb_complete: bool  # Whether ORB period is complete
    
    def to_dict(self) -> Dict:
        return {
            'position_in_range': self.position_in_range,
            'breakout_direction': self.breakout_direction,
            'range_width_pct': self.range_width_pct,
            'distance_to_high_pct': self.distance_to_high_pct,
            'distance_to_low_pct': self.distance_to_low_pct,
            'orb_complete': self.orb_complete
        }


class ORBTracker:
    """
    Tracks Opening Range Breakout for all symbols.
    Resets daily at market open.
    """
    
    def __init__(self):
        self._orb_data: Dict[str, ORBData] = {}
        self._last_reset_date: Optional[date] = None
    
    def _get_or_create_orb(self, symbol: str) -> ORBData:
        """Get or create ORB data for symbol, resetting if new trading day"""
        current_date = now_et().date()
        
        # Reset all ORB data at start of new trading day
        if self._last_reset_date != current_date:
            log.info(f"New trading day {current_date}, resetting ORB data")
            self._orb_data.clear()
            self._last_reset_date = current_date
        
        if symbol not in self._orb_data:
            self._orb_data[symbol] = ORBData(date=current_date, symbol=symbol)
        
        return self._orb_data[symbol]
    
    def _is_orb_period(self, dt: Optional[datetime] = None) -> bool:
        """Check if current time is within ORB period (9:30-10:30 AM ET)"""
        t = dt or now_et()
        market_open = open_time_et(t)
        orb_end = market_open.replace(
            hour=10, minute=30, second=0, microsecond=0
        )
        return market_open <= t <= orb_end
    
    def update_price(self, symbol: str, price: float, timestamp: Optional[datetime] = None):
        """
        Update ORB tracking with new price tick.
        Should be called for every price update during market hours.
        """
        if price <= 0:
            return
        
        t = timestamp or now_et()
        orb = self._get_or_create_orb(symbol)
        
        # Only update ORB high/low during ORB period (9:30-10:30 AM ET)
        if self._is_orb_period(t):
            orb.tick_count += 1
            
            # Set opening price from first tick
            if orb.opening_price is None:
                orb.opening_price = price
                orb.orb_high = price
                orb.orb_low = price
                log.info(f"{symbol} ORB started: Opening price ${price:.2f}")
            else:
                # Update high/low
                if price > orb.orb_high:
                    orb.orb_high = price
                if price < orb.orb_low:
                    orb.orb_low = price
        
        elif not orb.orb_complete and orb.orb_high is not None:
            # ORB period just ended - mark as complete
            orb.orb_complete = True
            log.info(
                f"{symbol} ORB complete: High=${orb.orb_high:.2f}, "
                f"Low=${orb.orb_low:.2f}, Range={orb.range_width_pct:.2f}%"
            )
    
    def get_orb_data(self, symbol: str) -> Optional[ORBData]:
        """Get current ORB data for symbol"""
        return self._orb_data.get(symbol)
    
    def get_orb_features(self, symbol: str, current_price: float) -> ORBFeatures:
        """
        Get ORB-derived features for ML model.
        Returns neutral features if ORB not yet formed.
        """
        orb = self._orb_data.get(symbol)
        
        if orb is None or orb.orb_high is None or orb.orb_low is None:
            # ORB not yet started
            return ORBFeatures(
                position_in_range=0.5,
                breakout_direction='forming',
                range_width_pct=0.0,
                distance_to_high_pct=0.0,
                distance_to_low_pct=0.0,
                orb_complete=False
            )
        
        # Calculate position and distances
        position = orb.position_in_range(current_price)
        
        distance_to_high = (orb.orb_high - current_price) / current_price * 100
        distance_to_low = (current_price - orb.orb_low) / current_price * 100
        
        return ORBFeatures(
            position_in_range=position,
            breakout_direction=orb.breakout_direction(current_price),
            range_width_pct=orb.range_width_pct,
            distance_to_high_pct=distance_to_high,
            distance_to_low_pct=distance_to_low,
            orb_complete=orb.orb_complete
        )
    
    def get_all_orb_data(self) -> Dict[str, Dict]:
        """Get ORB data for all tracked symbols"""
        return {
            symbol: orb.to_dict()
            for symbol, orb in self._orb_data.items()
        }
    
    def set_orb_from_historical(
        self, 
        symbol: str, 
        orb_high: float, 
        orb_low: float,
        opening_price: Optional[float] = None
    ):
        """
        Manually set ORB levels from historical data.
        Useful when starting mid-day or backtesting.
        """
        orb = self._get_or_create_orb(symbol)
        orb.orb_high = orb_high
        orb.orb_low = orb_low
        orb.opening_price = opening_price or orb_low
        orb.orb_complete = True
        log.info(f"{symbol} ORB set from historical: High=${orb_high:.2f}, Low=${orb_low:.2f}")


# Global singleton instance
_orb_tracker: Optional[ORBTracker] = None


def get_orb_tracker() -> ORBTracker:
    """Get or create the global ORB tracker instance"""
    global _orb_tracker
    if _orb_tracker is None:
        _orb_tracker = ORBTracker()
    return _orb_tracker


def update_orb(symbol: str, price: float, timestamp: Optional[datetime] = None):
    """Convenience function to update ORB from price tick"""
    get_orb_tracker().update_price(symbol, price, timestamp)


def get_orb_features(symbol: str, current_price: float) -> ORBFeatures:
    """Convenience function to get ORB features for ML"""
    return get_orb_tracker().get_orb_features(symbol, current_price)
