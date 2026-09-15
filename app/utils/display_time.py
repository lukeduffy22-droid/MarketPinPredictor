"""Browser-local timestamp formatting for user-facing MarketPin views.

Persisted timestamps remain UTC and exchange-session calculations remain in
America/New_York.  This module only controls how UTC instants are presented to
the person viewing the Streamlit dashboard.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, tzinfo
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc


@dataclass(frozen=True)
class DisplayTimezone:
    """Resolved timezone plus enough provenance to label fallback behavior."""

    tzinfo: tzinfo
    name: str
    source: str


def resolve_display_timezone(
    browser_timezone: str | None,
    *,
    fallback_timezone: tzinfo | None = None,
) -> DisplayTimezone:
    """Resolve a DST-safe browser timezone, with an explicit UTC fallback.

    A browser's current numeric offset is intentionally not used for historical
    records because it cannot represent daylight-saving transitions.
    """

    timezone_name = str(browser_timezone or "").strip()
    if timezone_name:
        try:
            resolved = ZoneInfo(timezone_name)
            return DisplayTimezone(resolved, timezone_name, "browser")
        except (ZoneInfoNotFoundError, ValueError):
            pass

    resolved = fallback_timezone or UTC
    name = str(getattr(resolved, "key", None) or resolved or "UTC")
    return DisplayTimezone(resolved, name, "fallback")


def resolve_context_display_timezone(context: Any) -> DisplayTimezone:
    """Resolve the IANA timezone exposed by a Streamlit browser context."""

    try:
        browser_timezone = getattr(context, "timezone", None)
    except Exception:
        browser_timezone = None
    return resolve_display_timezone(browser_timezone)


def parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse a persisted UTC timestamp without changing the represented instant.

    Legacy database values can be naive even though their field names and
    storage contract are UTC, so naive values are explicitly interpreted as
    UTC. Malformed values remain unavailable rather than receiving a guessed
    clock time.
    """

    parsed: datetime
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_display_timestamp(
    value: Any,
    display_timezone: DisplayTimezone,
    *,
    format_string: str = "%b %d, %Y at %I:%M:%S %p %Z",
    unavailable: str = "N/A",
) -> str:
    """Format a stored UTC instant in the resolved viewer timezone."""

    parsed = parse_utc_timestamp(value)
    if parsed is None:
        return unavailable
    return parsed.astimezone(display_timezone.tzinfo).strftime(format_string)


def display_timezone_label(
    display_timezone: DisplayTimezone,
    at_utc: datetime | None = None,
) -> str:
    """Return an auditable IANA-zone/abbreviation label for the current viewer."""

    reference = parse_utc_timestamp(at_utc or datetime.now(UTC))
    local = reference.astimezone(display_timezone.tzinfo) if reference else None
    abbreviation = local.tzname() if local else None
    if abbreviation and abbreviation != display_timezone.name:
        return f"{display_timezone.name} ({abbreviation})"
    return abbreviation or display_timezone.name
