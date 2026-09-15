"""Reviewed cash-session boundaries shared by research forecasts and shifts.

These are underlying cash closes, not the later trading cutoff for some
options. Full closures and early-close dates reuse the application's calendar.
No weekday-only fallback is permitted outside the reviewed years.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from numbers import Integral
from zoneinfo import ZoneInfo

from app.utils.market_calendar import market_calendar_status
from app.utils.time_et import close_time_et

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
EARLY_CLOSE_REVIEWED_YEARS = frozenset(range(2024, 2029))


def session_bounds(day: date) -> tuple[datetime, datetime]:
    """Return UTC open/close or raise for a closure/unreviewed calendar."""
    status = market_calendar_status(day)
    if not status["supported"] or day.year not in EARLY_CLOSE_REVIEWED_YEARS:
        raise ValueError("calendar_year_unsupported")
    if not status["market_open"]:
        raise ValueError("not_a_trading_session")
    local = datetime.combine(day, time(9, 30), tzinfo=ET)
    return local.astimezone(UTC), close_time_et(local).astimezone(UTC)


def shift_session(day: date, sessions: int) -> date:
    """Move by actual exchange sessions from an existing session date."""
    if isinstance(sessions, bool) or not isinstance(sessions, Integral):
        raise ValueError("session_offset_must_be_an_integer")
    session_bounds(day)
    direction = 1 if sessions >= 0 else -1
    remaining = abs(sessions)
    while remaining:
        day += timedelta(days=direction)
        status = market_calendar_status(day)
        if not status["supported"] or day.year not in EARLY_CLOSE_REVIEWED_YEARS:
            raise ValueError("calendar_year_unsupported")
        if status["market_open"]:
            remaining -= 1
    return day


def resolve_target_session(as_of_utc: datetime, horizon_sessions: int = 0) -> dict:
    """Horizon 0 is today's close; positive horizons count following sessions.

    On a closed date the anchor is the next trading session. An already closed
    session remains today's target, allowing callers to explicitly abstain
    rather than quietly reinterpret 'today' as tomorrow.
    """
    if not isinstance(as_of_utc, datetime) or as_of_utc.tzinfo is None:
        raise ValueError("as_of_utc_must_be_timezone_aware")
    if isinstance(horizon_sessions, bool) or not isinstance(horizon_sessions, Integral) or not 0 <= horizon_sessions <= 10:
        raise ValueError("horizon_sessions_must_be_0_to_10")
    day = as_of_utc.astimezone(ET).date()
    for _ in range(15):
        status = market_calendar_status(day)
        if not status["supported"]:
            raise ValueError("calendar_year_unsupported")
        if status["market_open"]:
            break
        day += timedelta(days=1)
    else:
        raise ValueError("next_session_unavailable")
    start, close = session_bounds(day)
    target_day = shift_session(day, int(horizon_sessions))
    _, target_close = session_bounds(target_day)
    return {
        "anchor_session_date": day.isoformat(),
        "session_open_utc": start.isoformat().replace("+00:00", "Z"),
        "session_close_utc": close.isoformat().replace("+00:00", "Z"),
        "target_session_date": target_day.isoformat(),
        "target_close_utc": target_close.isoformat().replace("+00:00", "Z"),
        "horizon_sessions": int(horizon_sessions),
        "calendar_authority": "https://www.nyse.com/trade/hours-calendars",
    }
