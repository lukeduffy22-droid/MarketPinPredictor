"""Compatibility wrapper around app.utils.time_et."""

from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Tuple

from app.utils.time_et import close_time_et, is_early_close, is_market_holiday, is_regular_hours, now_et as _now_et

MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE_REGULAR = dt_time(16, 0)
MARKET_CLOSE_EARLY = dt_time(13, 0)


def now_et() -> datetime:
    """Get current time in Eastern timezone."""
    return _now_et()


def is_holiday(d: date | None = None) -> bool:
    """Check if given date is a market holiday."""
    if d is None:
        return is_market_holiday(now_et())
    return is_market_holiday(datetime(d.year, d.month, d.day, tzinfo=now_et().tzinfo))


def is_early_close_day(d: date | None = None) -> bool:
    """Check if given date is an early close day."""
    if d is None:
        return is_early_close(now_et())
    return is_early_close(datetime(d.year, d.month, d.day, tzinfo=now_et().tzinfo))


def get_close_time_today() -> dt_time:
    """Get today's market close time, accounting for early close days."""
    close_dt = close_time_et(now_et())
    return dt_time(close_dt.hour, close_dt.minute, close_dt.second)


def is_weekend(d: date | None = None) -> bool:
    """Check if given date is a weekend."""
    target = d or now_et().date()
    return target.weekday() >= 5


def market_is_open(at_utc: datetime | None = None) -> bool:
    """Check the reviewed U.S. cash-equity session at an explicit UTC instant."""
    candidate = at_utc or datetime.now(timezone.utc)
    if candidate.tzinfo is None:
        candidate = candidate.replace(tzinfo=timezone.utc)
    return is_regular_hours(candidate)


def market_is_closed() -> bool:
    """Check if market is currently closed."""
    return not market_is_open()


def is_freeze_enforced() -> bool:
    """Freeze is enforced whenever the market is closed."""
    return market_is_closed()


def get_freeze_status() -> Tuple[bool, str]:
    """Get detailed freeze status with reason."""
    now = now_et()

    if is_weekend(now.date()):
        return True, f"Weekend ({now.strftime('%A')})"

    if is_holiday(now.date()):
        return True, "Market holiday"

    close_time = get_close_time_today()
    current_time = now.time().replace(tzinfo=None)

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
    close_dt = close_time_et(now)
    return max(0, int((close_dt - now).total_seconds() // 60))


def get_last_close_datetime() -> datetime:
    """Get the datetime of the most recent market close."""
    now = now_et()
    today = now.date()

    if now.time().replace(tzinfo=None) >= get_close_time_today() and not is_weekend(today) and not is_holiday(today):
        return close_time_et(now)

    for days_back in range(1, 10):
        check_date = today - timedelta(days=days_back)
        if is_weekend(check_date) or is_holiday(check_date):
            continue
        check_dt = datetime(check_date.year, check_date.month, check_date.day, 12, 0, tzinfo=now.tzinfo)
        return close_time_et(check_dt)

    return close_time_et(now)
