"""Optional earnings and inflation consensus adapters for the market-events display."""

from __future__ import annotations

import json
import math
import os
import re
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from time import monotonic
from zoneinfo import ZoneInfo

import requests

from backend.market_event_sources import TECH_CIKS

_UTC = timezone.utc
_NEW_YORK = ZoneInfo("America/New_York")
_FINNHUB_DOCS = "https://finnhub.io/docs/api/earnings-calendar"
_TE_CALENDAR = "https://tradingeconomics.com/united-states/calendar"
_TE_US = "https://api.tradingeconomics.com/calendar/country/united%20states"
_RETROSPECTIVE = "Finnhub supplied the adjusted figures after the release. The original estimate timestamp is unavailable."


def _clock(override):
    if override is None:
        return datetime.now(_UTC)
    if override.tzinfo is None:
        raise ValueError("A timezone-aware timestamp is required")
    return override.astimezone(_UTC)


def _time(value, *, naive_utc=False):
    if not isinstance(value, str) or "T" not in value:
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            return stamp.replace(tzinfo=_UTC) if naive_utc else None
        return stamp.astimezone(_UTC)
    except ValueError:
        return None


def _finite(value):
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _get_json(url, *, headers, params):
    with requests.get(url, headers=headers, params=params, timeout=(3, 5),
                      allow_redirects=False, stream=True) as response:
        if response.status_code != 200:
            raise ValueError(f"HTTP {response.status_code}")
        deadline = monotonic() + 7
        body = bytearray()
        for chunk in response.iter_content(65536):
            if len(body) + len(chunk) > 6_000_000 or monotonic() > deadline:
                raise ValueError("Provider response limit exceeded")
            body.extend(chunk)
        return json.loads(body)


def _status(source_id, status, detail, now):
    name, url = (("Finnhub earnings calendar", _FINNHUB_DOCS) if source_id == "finnhub_earnings"
                 else ("Trading Economics survey", _TE_CALENDAR))
    return {"id": source_id, "source_url": url, "name": name, "status": status,
            "detail": detail, "checked_at": _clock(now).isoformat()}


def _earnings_key(event, today):
    if event.get("category") != "earnings" or event.get("ticker") not in TECH_CIKS or event.get("form") != "8-K":
        return None
    publication = _time(event.get("published_at"))
    if publication is None or publication > today:
        return None
    day = publication.astimezone(_NEW_YORK).date()
    if today.astimezone(_NEW_YORK).date() - day > timedelta(days=14):
        return None
    return event["ticker"], day.isoformat()


def _period_matches(event, year, quarter):
    """Match the exhibit's quarter-end date against the issuer's SEC fiscal year-end."""
    fiscal_end = event.get("fiscal_year_end")
    if not isinstance(fiscal_end, str) or not re.fullmatch(r"[0-9]{4}", fiscal_end):
        return False
    try:
        reported = date.fromisoformat(event.get("period") or "")
        month, day = int(fiscal_end[:2]), int(fiscal_end[2:])
        if not 1 <= month <= 12 or not 1 <= day <= monthrange(2000, month)[1]:
            return False
        # Count back from the end of the provider's fiscal year, including year rollover.
        offset = month - 1 - 3 * (4 - quarter)
        expected_year, expected_month = year + offset // 12, offset % 12 + 1
        nominal = date(expected_year, expected_month, min(day, monthrange(expected_year, expected_month)[1]))
        # Weekly retail calendars shift around a nominal date, including 53-week years.
        filing_day = _time(event["published_at"]).astimezone(_NEW_YORK).date()
        return abs((reported - nominal).days) <= 7 and reported <= filing_day
    except (TypeError, ValueError):
        return False


def _earnings_figures(event, row, observed):
    year, quarter = row.get("year"), row.get("quarter")
    if type(year) is not int or not 2000 <= year <= observed.year + 1:
        return []
    if type(quarter) is not int or quarter not in range(1, 5) or not _period_matches(event, year, quarter):
        return []
    figures = []
    for key, metric, unit in (("eps", "Adjusted EPS", "USD/share"), ("revenue", "Adjusted revenue", "USD")):
        actual, expected = _finite(row.get(key + "Actual")), _finite(row.get(key + "Estimate"))
        if actual is None:
            continue
        note = _RETROSPECTIVE if expected is not None else "Finnhub supplied no analyst estimate."
        if key == "revenue" and expected is not None:
            note += " The revenue estimate includes Finnhub proprietary estimates."
        figures.append({"metric": metric, "unit": unit, "actual": actual, "expected": expected,
                        "period": f"FY{year} Q{quarter}", "source": "Finnhub", "source_url": _FINNHUB_DOCS,
                        "expected_source": "Finnhub" if expected is not None else None,
                        "expected_url": _FINNHUB_DOCS if expected is not None else None,
                        "expected_as_of": None, "estimate_vintage": "unverified", "as_of": observed.isoformat(),
                        "note": note})
    return figures


def enrich_earnings(events, *, api_key=None, now=None):
    """Attach provider-basis actuals/estimates only to unique same-issuer, same-day releases."""
    source_id = "finnhub_earnings"
    api_key = (os.getenv("FINNHUB_API_KEY", "") if api_key is None else api_key).strip()
    if not api_key:
        return events, _status(source_id, "unconfigured", "FINNHUB_API_KEY is unset.", now)
    start = _clock(now)
    filings = defaultdict(list)
    for index, event in enumerate(events):
        key = _earnings_key(event, start)
        if key:
            filings[key].append(index)
    if not filings:
        return events, _status(source_id, "idle", "No eligible recent US earnings disclosures.", now)
    days = [day for _, day in filings]
    try:
        data = _get_json("https://finnhub.io/api/v1/calendar/earnings",
                         headers={"X-Finnhub-Token": api_key}, params={"from": min(days), "to": max(days)})
        if not isinstance(data, dict) or not isinstance(data.get("earningsCalendar"), list):
            raise ValueError("Unexpected earnings calendar payload")
    except (requests.RequestException, ValueError):
        return events, _status(source_id, "error", "Calendar request failed. Check entitlement, quota and service availability.", now)
    observed = _clock(now)
    rows = defaultdict(list)
    for row in data["earningsCalendar"]:
        if isinstance(row, dict) and isinstance(row.get("symbol"), str) and isinstance(row.get("date"), str):
            rows[(row["symbol"], row["date"])].append(row)
    updated = list(events)
    count = 0
    for key, indices in filings.items():
        if len(indices) != 1 or len(rows[key]) != 1:
            continue
        figures = _earnings_figures(events[indices[0]], rows[key][0], observed)
        if not figures:
            continue
        index = indices[0]
        event = events[index]
        # Shape idempotently on repeated polls. The separate SEC provenance is preserved.
        existing = [figure for figure in event.get("figures", []) if figure.get("source") != "Finnhub"]
        updated[index] = {**event, "figures": existing + figures}
        count += 1
    return updated, _status(source_id, "ok", f"Matched {count} earnings disclosures to dated provider results.", now)


# Exact calendar event names exclude quarterly annualized rates and price index levels.
_MACRO_METRICS = {
    "Fed Interest Rate Decision": ("fed_policy", "Fed funds target upper"),
    "Inflation Rate MoM": ("cpi", "Headline CPI MoM"),
    "Inflation Rate YoY": ("cpi", "Headline CPI YoY"),
    "Core Inflation Rate MoM": ("cpi", "Core CPI MoM"),
    "Core Inflation Rate YoY": ("cpi", "Core CPI YoY"),
    "PCE Price Index MoM": ("pce", "Headline PCE MoM"),
    "PCE Price Index YoY": ("pce", "Headline PCE YoY"),
    "Core PCE Price Index MoM": ("pce", "Core PCE MoM"),
    "Core PCE Price Index YoY": ("pce", "Core PCE YoY"),
}


def _macro_record(row, observed):
    if not isinstance(row, dict) or row.get("Country") != "United States" or row.get("Actual") not in (None, ""):
        return None
    metric = _MACRO_METRICS.get(row.get("Event")) if isinstance(row.get("Event"), str) else None
    due = _time(row.get("Date"), naive_utc=True)  # TE documents the timezone as UTC.
    if not metric or not due or due <= observed or str(row.get("DateSpan")) != "0":
        return None
    forecast = str(row.get("Forecast", "")).strip()
    if not forecast.endswith("%") and row.get("Unit") != "%":
        return None
    value = _finite(forecast.removesuffix("%"))
    if value is None:
        return None
    category, label = metric
    url = row.get("URL")
    source = "Trading Economics economist survey"
    if category == "fed_policy":
        # The Sep 16 and Oct 28, 2026 TE decision tables report 4%; both FOMC
        # statements specify a 3.75–4% target range. Lower bound is not surveyed.
        if url != "/united-states/interest-rate":
            return None
        period = due.astimezone(_NEW_YORK).date().isoformat()
        source += " (single decision rate; historically matches the upper bound)"
    else:
        try:
            reference = date.fromisoformat(str(row.get("ReferenceDate", "")).split("T")[0])
        except ValueError:
            return None
        if reference >= due.date():
            return None
        period = reference.strftime("%Y-%m")
    source_url = ("https://tradingeconomics.com" + url
                  if isinstance(url, str) and re.fullmatch(r"/united-states/[a-z0-9-]+", url) else _TE_CALENDAR)
    return {"category": category, "period": period, "metric": label, "unit": "%",
            "expected": value, "expected_source": source,
            "expected_url": source_url, "expected_as_of": observed.isoformat()}


def fetch_upcoming_macro_consensus(*, api_key=None, now=None):
    """Collect available future US CPI, PCE and Fed decision surveys with the actual observation time."""
    source_id = "tradingeconomics_survey"
    api_key = (os.getenv("TRADINGECONOMICS_API_KEY", "") if api_key is None else api_key).strip()
    if not api_key:
        return [], _status(source_id, "unconfigured", "TRADINGECONOMICS_API_KEY is unset.", now)
    start = _clock(now).date()
    end = start + timedelta(days=35)
    try:
        rows = _get_json(f"{_TE_US}/{start}/{end}", headers={"Authorization": api_key}, params={"f": "json"})
        if not isinstance(rows, list):
            raise ValueError("Unexpected survey payload")
    except (requests.RequestException, ValueError):
        return [], _status(source_id, "error", "Survey request failed. Check entitlement, quota and service availability.", now)
    observed = _clock(now)
    grouped = defaultdict(list)
    for row in rows:
        record = _macro_record(row, observed)
        if record:
            grouped[record["category"], record["period"], record["metric"]].append(record)
    records = [items[0] for items in grouped.values() if len(items) == 1]
    return records, _status(source_id, "ok", f"Captured {len(records)} upcoming survey figures.", now)
