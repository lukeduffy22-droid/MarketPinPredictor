"""
Options open interest cache service.
Loads OI at 09:35 ET and 13:00 ET to avoid REST calls in prediction path.
"""
import logging
import os
from typing import Dict, Optional
from datetime import datetime, timedelta, date
from dataclasses import dataclass
import asyncio

log = logging.getLogger("oi_cache")

# CRITICAL: Simulated OI must NOT leak into production
# Default to True until real OI data source is wired
# Set ALLOW_SIMULATED_OI=false in production when real OI is available
ALLOW_SIMULATED_OI = os.environ.get("ALLOW_SIMULATED_OI", "true").lower() == "true"

@dataclass
class OISnapshot:
    """Options OI snapshot for a strike"""
    K: int  # Strike price
    oi_call: int  # Open interest for calls
    oi_put: int  # Open interest for puts
    iv_call: float  # Implied volatility for calls
    iv_put: float  # Implied volatility for puts
    exp: date  # Expiration date

class OICache:
    """
    Per-symbol OI cache refreshed at scheduled times.
    Prevents REST API calls during prediction path.
    """
    
    def __init__(self):
        self._data: Dict[str, Dict[int, OISnapshot]] = {
            s: {} for s in ("SPX", "NDX", "DJI", "RUT", "VIX")
        }
        self._last_refresh: Dict[str, Optional[datetime]] = {
            s: None for s in ("SPX", "NDX", "DJI", "RUT", "VIX")
        }
        self._is_simulated: Dict[str, bool] = {
            s: False for s in ("SPX", "NDX", "DJI", "RUT", "VIX")
        }
        self._refresh_in_progress = False
    
    async def refresh(self, symbol: str, polygon_client):
        """
        Refresh OI cache for symbol using Polygon API.
        Called at 09:35 ET and 13:00 ET only.
        """
        from app.utils.time_et import now_et
        
        if self._refresh_in_progress:
            log.warning(f"OI refresh already in progress for {symbol}")
            return
        
        self._refresh_in_progress = True
        
        try:
            log.info(f"Refreshing OI cache for {symbol}")
            
            # Get 0DTE options (expiring today)
            today = datetime.now().date()
            
            # Use simulated data for now (real data requires higher API tier)
            # In production, use: polygon_client.list_options_contracts()
            # IMPORTANT: Mark this data as simulated
            self._data[symbol] = self._generate_simulated_oi(symbol)
            self._is_simulated[symbol] = True  # Mark as simulated data
            
            self._last_refresh[symbol] = now_et()
            
            log.info(f"OI cache refreshed for {symbol}: {len(self._data[symbol])} strikes (SIMULATED)")
            
        except Exception as e:
            log.error(f"Failed to refresh OI cache for {symbol}: {e}")
        finally:
            self._refresh_in_progress = False
    
    def _generate_simulated_oi(self, symbol: str) -> Dict[int, OISnapshot]:
        """
        Generate simulated OI data for testing.
        Replace with real API calls when higher-tier access is available.
        """
        from app.state.ring_buffers import get_latest_price
        
        current_price = get_latest_price(symbol)
        if not current_price:
            # Use reasonable defaults
            defaults = {"SPX": 6000, "NDX": 21000, "DJI": 44000, "RUT": 2300}
            current_price = defaults.get(symbol, 6000)
        
        # Generate strikes around current price
        strikes = {}
        strike_step = 25 if symbol == "SPX" else 50
        
        for i in range(-10, 11):  # 20 strikes around ATM
            K = int(current_price + i * strike_step)
            
            # Distance from ATM affects OI (higher near ATM)
            dist = abs(i)
            oi_base = max(100, 1000 - dist * 50)
            
            strikes[K] = OISnapshot(
                K=K,
                oi_call=oi_base,
                oi_put=oi_base,
                iv_call=0.15 + dist * 0.01,  # IV smile
                iv_put=0.15 + dist * 0.01,
                exp=datetime.now().date()
            )
        
        return strikes
    
    def get_oi(self, symbol: str, strike: int) -> Optional[OISnapshot]:
        """Get OI snapshot for specific strike"""
        # Fail closed: don't return simulated OI in live mode
        if not ALLOW_SIMULATED_OI and self._is_simulated.get(symbol, False):
            log.warning(f"Simulated OI requested for {symbol} strike {strike} but ALLOW_SIMULATED_OI=false")
            return None
        return self._data.get(symbol, {}).get(strike)
    
    def get_all_strikes(self, symbol: str) -> Dict[int, OISnapshot]:
        """
        Get all strikes for symbol.
        
        IMPORTANT: Fails closed if simulated OI would be returned in live mode.
        Set ALLOW_SIMULATED_OI=true in environment for local testing only.
        """
        # Fail closed: don't return simulated OI in live mode
        if not ALLOW_SIMULATED_OI and self._is_simulated.get(symbol, False):
            log.warning(
                f"OI cache is returning simulated OI for {symbol}. "
                "Disable simulation or wire real OI for live runs. "
                "Set ALLOW_SIMULATED_OI=true for testing."
            )
            return {}  # Return empty dict to fail closed
        return self._data.get(symbol, {})
    
    def is_fresh(self, symbol: str, max_age_minutes: int = 120) -> bool:
        """Check if cache is fresh enough"""
        last = self._last_refresh.get(symbol)
        if not last:
            return False
        
        from app.utils.time_et import now_et
        age = (now_et() - last).total_seconds() / 60
        
        return age <= max_age_minutes

# Global OI cache instance
oi_cache = OICache()

async def schedule_oi_refresh():
    """
    Background task to refresh OI cache at 09:35 ET and 13:00 ET.
    Should be run as a FastAPI background task.
    """
    from app.utils.time_et import now_et
    from app.utils.settings import settings
    from polygon import RESTClient
    
    client = RESTClient(settings.polygon_api_key)
    
    while True:
        try:
            current = now_et()
            
            # Check if we should refresh (09:35 or 13:00 ET)
            should_refresh = False
            
            if current.hour == 9 and current.minute == 35:
                should_refresh = True
            elif current.hour == 13 and current.minute == 0:
                should_refresh = True
            
            if should_refresh:
                for symbol in ("SPX", "NDX", "DJI", "RUT"):
                    await oi_cache.refresh(symbol, client)
                
                # Wait 2 minutes to avoid double-refresh
                await asyncio.sleep(120)
            else:
                # Check every 30 seconds
                await asyncio.sleep(30)
                
        except Exception as e:
            log.error(f"Error in OI refresh scheduler: {e}")
            await asyncio.sleep(60)
