"""
Options open interest cache service.
Loads OI at 09:35 ET and 13:00 ET to avoid REST calls in prediction path.
"""
import logging
from typing import Dict, Optional
from datetime import datetime, date
from dataclasses import dataclass
import asyncio

log = logging.getLogger("oi_cache")

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

            snapshots = await asyncio.to_thread(
                self._load_live_chain,
                symbol,
                polygon_client,
            )
            if not snapshots:
                raise RuntimeError("live options chain contained no usable OI")

            self._data[symbol] = snapshots
            self._is_simulated[symbol] = False
            self._last_refresh[symbol] = now_et()

            log.info(
                "OI cache refreshed for %s: %d strikes (LIVE)",
                symbol,
                len(snapshots),
            )

        except Exception as e:
            log.error(f"Failed to refresh OI cache for {symbol}: {e}")
        finally:
            self._refresh_in_progress = False

    @staticmethod
    def _load_live_chain(symbol: str, polygon_client) -> Dict[int, OISnapshot]:
        """Load the nearest live options expiry into strike-level OI snapshots."""
        options_root = {"DJI": "DIA"}.get(symbol, symbol)
        today = date.today()
        by_expiry: Dict[date, Dict[int, OISnapshot]] = {}

        for contract in polygon_client.list_snapshot_options_chain(options_root):
            details = getattr(contract, "details", None)
            if details is None:
                continue

            try:
                expiry = date.fromisoformat(str(details.expiration_date))
                strike = int(round(float(details.strike_price)))
                contract_type = str(details.contract_type).lower()
            except (AttributeError, TypeError, ValueError):
                continue

            if expiry < today or contract_type not in ("call", "put"):
                continue

            open_interest = int(getattr(contract, "open_interest", 0) or 0)
            implied_volatility = float(
                getattr(contract, "implied_volatility", 0.0) or 0.0
            )
            if open_interest <= 0 or implied_volatility <= 0:
                continue

            expiry_data = by_expiry.setdefault(expiry, {})
            snapshot = expiry_data.setdefault(
                strike,
                OISnapshot(
                    K=strike,
                    oi_call=0,
                    oi_put=0,
                    iv_call=0.0,
                    iv_put=0.0,
                    exp=expiry,
                ),
            )
            if contract_type == "call":
                snapshot.oi_call += open_interest
                snapshot.iv_call = implied_volatility
            else:
                snapshot.oi_put += open_interest
                snapshot.iv_put = implied_volatility

        if not by_expiry:
            return {}

        nearest_expiry = min(by_expiry)
        return {
            strike: snapshot
            for strike, snapshot in by_expiry[nearest_expiry].items()
            if snapshot.iv_call > 0 and snapshot.iv_put > 0
        }
    
    def get_oi(self, symbol: str, strike: int) -> Optional[OISnapshot]:
        """Get OI snapshot for specific strike"""
        if self._is_simulated.get(symbol, False):
            log.warning(f"Simulated OI requested for {symbol} strike {strike}")
            return None
        return self._data.get(symbol, {}).get(strike)
    
    def get_all_strikes(self, symbol: str) -> Dict[int, OISnapshot]:
        """
        Get all strikes for symbol.
        
        IMPORTANT: Always fails closed if simulated OI would be returned.
        """
        if self._is_simulated.get(symbol, False):
            log.warning(
                f"OI cache is returning simulated OI for {symbol}. "
                "Gamma is unavailable until a live refresh succeeds."
            )
            return {}
        return self._data.get(symbol, {})
    
    def is_fresh(self, symbol: str, max_age_minutes: int = 120) -> bool:
        """Check if cache is fresh enough"""
        last = self._last_refresh.get(symbol)
        if not last:
            return False
        
        from app.utils.time_et import now_et
        age = (now_et() - last).total_seconds() / 60
        
        return age <= max_age_minutes

    def get_status(self, symbol: str, max_age_minutes: int = 120) -> dict:
        """Return provenance and freshness for a symbol's OI cache."""
        snapshots = self.get_all_strikes(symbol)
        last_refresh = self._last_refresh.get(symbol)
        return {
            "fresh": self.is_fresh(symbol, max_age_minutes=max_age_minutes),
            "simulated": self._is_simulated.get(symbol, False),
            "strike_count": len(snapshots),
            "last_refresh": last_refresh.isoformat() if last_refresh else None,
        }

# Global OI cache instance
oi_cache = OICache()

async def schedule_oi_refresh():
    """
    Background task to refresh OI cache at 09:35 ET and 13:00 ET.
    Should be run as a FastAPI background task.
    """
    from app.utils.time_et import now_et
    from app.utils.settings import settings
    from polygon.rest import RESTClient
    
    if not settings.polygon_api_key:
        log.error("Polygon API key unavailable; live OI refresh disabled")
        return

    client = RESTClient(settings.polygon_api_key)
    
    while True:
        try:
            current = now_et()
            
            scheduled_refresh = (
                (current.hour == 9 and current.minute == 35)
                or (current.hour == 13 and current.minute == 0)
            )
            market_open_refresh = (
                9 <= current.hour < 16
                and current.weekday() < 5
                and any(
                    not oi_cache.is_fresh(
                        symbol,
                        max_age_minutes=settings.max_oi_age_minutes,
                    )
                    for symbol in ("SPX", "NDX", "DJI", "RUT")
                )
            )
            should_refresh = scheduled_refresh or market_open_refresh
            
            if should_refresh:
                for symbol in ("SPX", "NDX", "DJI", "RUT"):
                    await oi_cache.refresh(symbol, client)
                
                # Avoid hammering the chain API after either success or failure.
                await asyncio.sleep(300)
            else:
                # Check every 30 seconds
                await asyncio.sleep(30)
                
        except Exception as e:
            log.error(f"Error in OI refresh scheduler: {e}")
            await asyncio.sleep(60)
