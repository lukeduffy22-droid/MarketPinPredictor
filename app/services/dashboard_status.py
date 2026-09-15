"""Presentation of session timing and diagnostic state without changing authority."""

from datetime import datetime, timezone

from app.utils.time_et import close_time_et, is_regular_hours, now_et


def diagnostic_state_label(state: str) -> str:
    # The API also uses closed_context for intraday fallback evidence. It does
    # not establish that the exchange is closed.
    if state in {"fallback", "closed_context"}:
        return "historical / fallback context"
    return state.replace("_", " ")


def near_close_seconds(at_utc: datetime | None = None) -> int | None:
    candidate = at_utc or datetime.now(timezone.utc)
    if candidate.tzinfo is None:
        raise ValueError("Session timing requires a timezone-aware timestamp")
    if not is_regular_hours(candidate):
        return None
    remaining = (close_time_et(now_et(candidate)) - candidate).total_seconds()
    return int(remaining) if 0 < remaining <= 900 else None


def near_close_caption(seconds: int) -> str:
    if seconds < 60:
        duration = "less than 1 minute"
    else:
        duration = f"{seconds // 60}m {seconds % 60:02d}s"
    return (
        f"Near-close research window: {duration} until the scheduled close "
        "at this page update. Automatic refresh is off."
    )
