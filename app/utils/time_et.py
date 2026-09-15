"""
Eastern Time utilities with holiday and early-close support.
Critical for accurate minutes-to-close calculations during power hour.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.utils.market_calendar import (
    full_closure_iso_dates,
    market_calendar_status,
)

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
    # 2026
    "2026-11-27",  # Day after Thanksgiving
    "2026-12-24",  # Christmas Eve
    # 2027-2028, verified against NYSE hours-calendars on 2026-09-10.
    # July 2, 2026 is a regular session, not an early close.
    "2027-11-26",
    "2028-07-03",
    "2028-11-24",
}

# Full closures are shared with the PowerShell scheduler through the reviewed
# calendar file. The compatibility set remains available to older callers.
US_MARKET_HOLIDAYS = set(full_closure_iso_dates())


def now_utc() -> datetime:
    """Return the current UTC time as a timezone-aware datetime."""
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    """Return an explicit UTC ISO-8601 timestamp with Z suffix."""
    value = dt or now_utc()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def format_clock_et(dt_utc: Optional[datetime] = None) -> str:
    """Return a short ET clock label for user-facing UI captions."""
    return now_et(dt_utc).strftime("%H:%M:%S ET")

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
    """Check full closure status, failing closed outside reviewed years."""
    base = et_today(d)
    status = market_calendar_status(base.date())
    return bool(
        not status["supported"]
        or status["reason"] in {"regular_full_closure", "one_off_full_closure"}
    )

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
