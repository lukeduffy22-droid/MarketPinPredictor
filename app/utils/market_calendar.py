"""Single-source U.S. cash-equity full-closure calendar.

Both the scheduled PowerShell launcher and Python runtime consume
``config/us_cash_equity_calendar.json``. Dates outside its reviewed year set
fail closed instead of silently reverting to a weekday-only calendar.
"""

from __future__ import annotations

import json
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Any

CALENDAR_SCHEMA_VERSION = "marketpin-us-cash-equity-calendar.v1"
CALENDAR_PATH = Path(__file__).resolve().parents[2] / "config" / "us_cash_equity_calendar.json"


def _invalid_calendar(error: str) -> dict[str, Any]:
    return {
        "valid": False,
        "error": error,
        "supported_years": frozenset(),
        "regular_full_closures": frozenset(),
        "one_off_full_closures": {},
    }


def _exact_iso_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


@lru_cache(maxsize=1)
def load_market_calendar() -> dict[str, Any]:
    try:
        payload = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return _invalid_calendar(f"{type(exc).__name__}: {exc}")
    if not isinstance(payload, dict):
        return _invalid_calendar("calendar_contract_invalid:root_type")
    required = {
        "schema_version",
        "supported_years",
        "regular_full_closures",
        "one_off_full_closures",
    }
    if not required.issubset(payload):
        return _invalid_calendar("calendar_contract_invalid:missing_property")
    if payload.get("schema_version") != CALENDAR_SCHEMA_VERSION:
        return _invalid_calendar("calendar_contract_invalid:schema")

    raw_years = payload["supported_years"]
    raw_regular = payload["regular_full_closures"]
    raw_one_off = payload["one_off_full_closures"]
    if (
        not isinstance(raw_years, list)
        or not raw_years
        or not isinstance(raw_regular, list)
        or not raw_regular
        or not isinstance(raw_one_off, dict)
    ):
        return _invalid_calendar("calendar_contract_invalid:property_type")
    if any(
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 2000 <= value <= 2100
        for value in raw_years
    ):
        return _invalid_calendar("calendar_contract_invalid:supported_year")
    if len(set(raw_years)) != len(raw_years):
        return _invalid_calendar("calendar_contract_invalid:duplicate_year")
    supported_years = frozenset(raw_years)

    parsed_regular = [_exact_iso_date(value) for value in raw_regular]
    if any(value is None for value in parsed_regular):
        return _invalid_calendar("calendar_contract_invalid:regular_date")
    regular_values = [str(value) for value in raw_regular]
    if len(set(regular_values)) != len(regular_values):
        return _invalid_calendar("calendar_contract_invalid:duplicate_date")
    if any(value.year not in supported_years for value in parsed_regular if value):
        return _invalid_calendar("calendar_contract_invalid:unsupported_date_year")
    if {value.year for value in parsed_regular if value} != set(supported_years):
        return _invalid_calendar("calendar_contract_invalid:year_without_holidays")

    one_off: dict[str, str] = {}
    for value, reason in raw_one_off.items():
        parsed = _exact_iso_date(value)
        if (
            parsed is None
            or parsed.year not in supported_years
            or not isinstance(reason, str)
            or not reason.strip()
            or value in regular_values
        ):
            return _invalid_calendar("calendar_contract_invalid:one_off_closure")
        one_off[value] = reason
    regular = frozenset(regular_values)
    return {
        "valid": True,
        "error": None,
        "supported_years": supported_years,
        "regular_full_closures": regular,
        "one_off_full_closures": one_off,
    }


def is_market_calendar_year_supported(year: int) -> bool:
    calendar = load_market_calendar()
    return bool(calendar["valid"] and int(year) in calendar["supported_years"])


def market_calendar_status(day) -> dict[str, Any]:
    """Return reviewed calendar status; unsupported years are closed."""

    iso_day = day.isoformat()
    calendar = load_market_calendar()
    if not calendar["valid"]:
        return {
            "date": iso_day,
            "supported": False,
            "market_open": False,
            "reason": "calendar_file_invalid",
        }
    if int(day.year) not in calendar["supported_years"]:
        return {
            "date": iso_day,
            "supported": False,
            "market_open": False,
            "reason": "calendar_year_unsupported",
        }
    if day.weekday() >= 5:
        return {
            "date": iso_day,
            "supported": True,
            "market_open": False,
            "reason": "weekend",
        }
    if iso_day in calendar["regular_full_closures"]:
        return {
            "date": iso_day,
            "supported": True,
            "market_open": False,
            "reason": "regular_full_closure",
        }
    if iso_day in calendar["one_off_full_closures"]:
        return {
            "date": iso_day,
            "supported": True,
            "market_open": False,
            "reason": "one_off_full_closure",
        }
    return {
        "date": iso_day,
        "supported": True,
        "market_open": True,
        "reason": "regular_session_date",
    }


def full_closure_iso_dates() -> frozenset[str]:
    calendar = load_market_calendar()
    return frozenset(calendar["regular_full_closures"]) | frozenset(
        calendar["one_off_full_closures"]
    )
