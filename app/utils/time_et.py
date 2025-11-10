"""
Eastern Time utilities with holiday and early-close support.
Critical for accurate minutes-to-close calculations during power hour.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except ImportError:
    # Fallback for systems without zoneinfo
    import pytz
    ET = pytz.timezone("US/Eastern")

# Regular market hours (ET)
REG_OPEN = (9, 30, 0)   # 09:30:00
REG_CLOSE = (16, 0, 0)  # 16:00:00

# Early close dates (1:00 PM ET) - Update annually
EARLY_CLOSE_ET_DATES = {
    # 2024
    "2024-07-03",  # Day before Independence Day
    "2024-11-29",  # Day after Thanksgiving
    "2024-12-24",  # Christmas Eve
    # 2025
    "2025-07-03",  # Day before Independence Day
    "2025-11-28",  # Day after Thanksgiving
    "2025-12-24",  # Christmas Eve
}

# Full market closures - Update annually
US_MARKET_HOLIDAYS = {
    # 2024
    "2024-01-01",  # New Year's Day
    "2024-01-15",  # MLK Day
    "2024-02-19",  # Presidents' Day
    "2024-03-29",  # Good Friday
    "2024-05-27",  # Memorial Day
    "2024-06-19",  # Juneteenth
    "2024-07-04",  # Independence Day
    "2024-09-02",  # Labor Day
    "2024-11-28",  # Thanksgiving
    "2024-12-25",  # Christmas
    # 2025
    "2025-01-01",  # New Year's Day
    "2025-01-20",  # MLK Day
    "2025-02-17",  # Presidents' Day
    "2025-04-18",  # Good Friday
    "2025-05-26",  # Memorial Day
    "2025-06-19",  # Juneteenth
    "2025-07-04",  # Independence Day
    "2025-09-01",  # Labor Day
    "2025-11-27",  # Thanksgiving
    "2025-12-25",  # Christmas
}

def now_et(dt_utc: Optional[datetime] = None) -> datetime:
    """Convert UTC datetime to ET, or get current ET time"""
    if dt_utc is None:
        return datetime.now(ET)
    return dt_utc.astimezone(ET)

def et_today(dt_et: Optional[datetime] = None) -> datetime:
    """Get midnight ET for given datetime or today"""
    d = dt_et or now_et()
    return d.replace(hour=0, minute=0, second=0, microsecond=0)

def is_market_holiday(d: Optional[datetime] = None) -> bool:
    """Check if given date is a market holiday"""
    base = et_today(d)
    return base.date().isoformat() in US_MARKET_HOLIDAYS

def is_early_close(d: Optional[datetime] = None) -> bool:
    """Check if given date has early close (1:00 PM ET)"""
    base = et_today(d)
    return base.date().isoformat() in EARLY_CLOSE_ET_DATES

def close_time_et(d: Optional[datetime] = None) -> datetime:
    """Get actual close time for given date (handles early close)"""
    base = et_today(d)
    
    if is_market_holiday(base):
        # No close time on holidays
        return base.replace(hour=0, minute=0, second=0)
    
    if is_early_close(base):
        return base.replace(hour=13, minute=0, second=0)
    
    h, m, s = REG_CLOSE
    return base.replace(hour=h, minute=m, second=s)

def open_time_et(d: Optional[datetime] = None) -> datetime:
    """Get market open time for given date"""
    base = et_today(d)
    
    if is_market_holiday(base):
        # No open time on holidays
        return base.replace(hour=0, minute=0, second=0)
    
    h, m, s = REG_OPEN
    return base.replace(hour=h, minute=m, second=s)

def minutes_to_close_et(dt_utc: datetime) -> int:
    """
    Calculate minutes until market close.
    Returns 0 if market is closed or after close.
    Critical for τ-based weight calculations.
    """
    t = now_et(dt_utc)
    
    # Check if market holiday
    if is_market_holiday(t):
        return 0
    
    ct = close_time_et(t)
    
    if t >= ct:
        return 0
    
    return int((ct - t).total_seconds() // 60)

def is_regular_hours(dt_utc: datetime) -> bool:
    """Check if given time is during regular trading hours"""
    t = now_et(dt_utc)
    
    # Check weekday
    if t.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Check holiday
    if is_market_holiday(t):
        return False
    
    ot = open_time_et(t)
    ct = close_time_et(t)
    
    return ot <= t <= ct

def is_power_hour(dt_utc: datetime) -> bool:
    """Check if in power hour (3:45-4:00 PM ET) - most critical time for predictions"""
    t = now_et(dt_utc)
    
    if not is_regular_hours(dt_utc):
        return False
    
    # Power hour: 15:45:00 to close
    power_start = t.replace(hour=15, minute=45, second=0, microsecond=0)
    ct = close_time_et(t)
    
    return power_start <= t <= ct

def get_cadence_ms(dt_utc: datetime) -> int:
    """
    Get appropriate recompute cadence based on time until close.
    Increases update frequency as market close approaches.
    """
    from app.utils.settings import settings
    
    tau = minutes_to_close_et(dt_utc)
    
    if tau <= 0:
        return 60000  # Market closed, slow refresh
    elif tau <= 15:  # 3:45-4:00 PM - power hour
        return settings.recompute_ms_1545_1600  # 15 seconds
    elif tau <= 30:  # 3:30-3:45 PM
        return settings.recompute_ms_1530_1545  # 30 seconds
    else:  # Before 3:30 PM
        return settings.recompute_ms_1500_1530  # 60 seconds
