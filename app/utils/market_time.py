"""
Market Time Utilities - Hard Freeze Enforcement

This module provides authoritative market open/close detection for Eastern Time.
Used to enforce data freeze after market close - NO EXCEPTIONS.

Functions:
    now_et(): Current Eastern Time
    market_is_open(): True if within 9:30 AM - 4:00 PM ET (or early close time)
    market_is_closed(): Inverse of market_is_open()
    get_close_time_today(): Returns today's close time (accounting for early close)
    is_freeze_enforced(): True if post-close freeze is active
"""
from datetime import datetime, time as dt_time, date
from typing import Tuple
import pytz

ET = pytz.timezone("US/Eastern")

MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE_REGULAR = dt_time(16, 0)
MARKET_CLOSE_EARLY = dt_time(13, 0)

EARLY_CLOSE_DATES_2024 = {
    date(2024, 7, 3),    # Day before July 4th
    date(2024, 11, 29),  # Black Friday
    date(2024, 12, 24),  # Christmas Eve
}

EARLY_CLOSE_DATES_2025 = {
    date(2025, 7, 3),    # Day before July 4th
    date(2025, 11, 28),  # Black Friday
    date(2025, 12, 24),  # Christmas Eve
}

MARKET_HOLIDAYS_2024 = {
    date(2024, 1, 1),    # New Year's Day
    date(2024, 1, 15),   # MLK Day
    date(2024, 2, 19),   # Presidents Day
    date(2024, 3, 29),   # Good Friday
    date(2024, 5, 27),   # Memorial Day
    date(2024, 6, 19),   # Juneteenth
    date(2024, 7, 4),    # Independence Day
    date(2024, 9, 2),    # Labor Day
    date(2024, 11, 28),  # Thanksgiving
    date(2024, 12, 25),  # Christmas
}

MARKET_HOLIDAYS_2025 = {
    date(2025, 1, 1),    # New Year's Day
    date(2025, 1, 20),   # MLK Day
    date(2025, 2, 17),   # Presidents Day
    date(2025, 4, 18),   # Good Friday
    date(2025, 5, 26),   # Memorial Day
    date(2025, 6, 19),   # Juneteenth
    date(2025, 7, 4),    # Independence Day
    date(2025, 9, 1),    # Labor Day
    date(2025, 11, 27),  # Thanksgiving
    date(2025, 12, 25),  # Christmas
}

ALL_HOLIDAYS = MARKET_HOLIDAYS_2024 | MARKET_HOLIDAYS_2025
ALL_EARLY_CLOSE = EARLY_CLOSE_DATES_2024 | EARLY_CLOSE_DATES_2025


def now_et() -> datetime:
    """Get current time in Eastern timezone."""
    return datetime.now(ET)


def is_holiday(d: date = None) -> bool:
    """Check if given date is a market holiday."""
    if d is None:
        d = now_et().date()
    return d in ALL_HOLIDAYS


def is_early_close_day(d: date = None) -> bool:
    """Check if given date is an early close day (1 PM ET)."""
    if d is None:
        d = now_et().date()
    return d in ALL_EARLY_CLOSE


def get_close_time_today() -> dt_time:
    """Get today's market close time, accounting for early close days."""
    if is_early_close_day():
        return MARKET_CLOSE_EARLY
    return MARKET_CLOSE_REGULAR


def is_weekend(d: date = None) -> bool:
    """Check if given date is a weekend."""
    if d is None:
        d = now_et().date()
    return d.weekday() >= 5  # Saturday = 5, Sunday = 6


def market_is_open() -> bool:
    """
    Check if market is currently open.
    
    Returns True if:
    - Not a weekend
    - Not a holiday
    - Current time is between 9:30 AM and close time (4 PM or 1 PM for early close)
    """
    now = now_et()
    
    if is_weekend(now.date()):
        return False
    
    if is_holiday(now.date()):
        return False
    
    current_time = now.time()
    close_time = get_close_time_today()
    
    return MARKET_OPEN <= current_time < close_time


def market_is_closed() -> bool:
    """
    Check if market is currently closed.
    
    CRITICAL: When this returns True, ALL live data ingestion must stop.
    Only cached snapshots may be used.
    """
    return not market_is_open()


def is_freeze_enforced() -> bool:
    """
    Check if post-close data freeze is active.
    
    Freeze is enforced when:
    - Market has closed for the day (after close time)
    - It's a weekend
    - It's a holiday
    
    When freeze is enforced:
    - No WebSocket connections allowed
    - No REST API polling allowed
    - No snapshot writes allowed
    - Only last valid snapshot may be read
    """
    return market_is_closed()


def get_freeze_status() -> Tuple[bool, str]:
    """
    Get detailed freeze status with reason.
    
    Returns:
        (is_frozen, reason_string)
    """
    now = now_et()
    
    if is_weekend(now.date()):
        return True, f"Weekend ({now.strftime('%A')})"
    
    if is_holiday(now.date()):
        return True, "Market holiday"
    
    current_time = now.time()
    close_time = get_close_time_today()
    
    if current_time < MARKET_OPEN:
        return True, f"Pre-market (opens {MARKET_OPEN.strftime('%I:%M %p')} ET)"
    
    if current_time >= close_time:
        return True, f"Post-close freeze active (closed {close_time.strftime('%I:%M %p')} ET)"
    
    return False, f"Market open (closes {close_time.strftime('%I:%M %p')} ET)"


def minutes_until_close() -> int:
    """Get minutes until market close. Returns 0 if closed."""
    if market_is_closed():
        return 0
    
    now = now_et()
    close_time = get_close_time_today()
    close_dt = now.replace(hour=close_time.hour, minute=close_time.minute, second=0, microsecond=0)
    
    if now >= close_dt:
        return 0
    
    return int((close_dt - now).total_seconds() // 60)


def get_last_close_datetime() -> datetime:
    """Get the datetime of the most recent market close."""
    now = now_et()
    today = now.date()
    
    if market_is_open():
        yesterday = today
        for i in range(1, 10):
            check_date = date(today.year, today.month, today.day - i) if today.day > i else today
            try:
                from datetime import timedelta
                check_date = today - timedelta(days=i)
            except:
                continue
            if not is_weekend(check_date) and not is_holiday(check_date):
                close_time = MARKET_CLOSE_EARLY if check_date in ALL_EARLY_CLOSE else MARKET_CLOSE_REGULAR
                return ET.localize(datetime.combine(check_date, close_time))
    
    if now.time() >= get_close_time_today():
        close_time = get_close_time_today()
        return now.replace(hour=close_time.hour, minute=close_time.minute, second=0, microsecond=0)
    
    from datetime import timedelta
    for i in range(1, 10):
        check_date = today - timedelta(days=i)
        if not is_weekend(check_date) and not is_holiday(check_date):
            close_time = MARKET_CLOSE_EARLY if check_date in ALL_EARLY_CLOSE else MARKET_CLOSE_REGULAR
            return ET.localize(datetime.combine(check_date, close_time))
    
    return now
