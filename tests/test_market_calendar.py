import json
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest

import app.utils.market_calendar as market_calendar
from app.utils.market_calendar import (
    CALENDAR_PATH,
    load_market_calendar,
    market_calendar_status,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POWERSHELL_MODULE = PROJECT_ROOT / "market_calendar.psm1"


def _powershell_calendar_statuses(
    start: date, end: date, *, calendar_path: Path = CALENDAR_PATH
):
    module_path = str(POWERSHELL_MODULE).replace("'", "''")
    calendar_path_text = str(calendar_path).replace("'", "''")
    script = f"""
$ErrorActionPreference = 'Stop'
Import-Module -Name '{module_path}' -Force -ErrorAction Stop
$day = [datetime]'{start.isoformat()}'
$end = [datetime]'{end.isoformat()}'
$rows = @()
while ($day -le $end) {{
    $status = Get-UsCashEquityMarketCalendarStatus -Candidate $day -CalendarPath '{calendar_path_text}'
    $rows += [pscustomobject]@{{
        date = [string]$status.Date
        supported = [bool]$status.Supported
        market_open = [bool]$status.MarketOpen
        reason = [string]$status.Reason
    }}
    $day = $day.AddDays(1)
}}
$rows | ConvertTo-Json -Depth 3 -Compress
"""
    result = subprocess.run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            script,
        ],
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    if isinstance(rows, dict):
        rows = [rows]
    return {row["date"]: row for row in rows}


def test_python_and_powershell_share_every_reviewed_calendar_day():
    calendar = load_market_calendar()
    assert calendar["valid"] is True
    start = date(min(calendar["supported_years"]), 1, 1)
    end = date(max(calendar["supported_years"]), 12, 31)
    powershell = _powershell_calendar_statuses(start, end)

    day = start
    while day <= end:
        python = market_calendar_status(day)
        assert powershell[day.isoformat()] == python
        day += timedelta(days=1)


def test_labor_day_tuesday_and_saturday_new_year_exception_are_explicit():
    assert market_calendar_status(date(2026, 9, 7)) == {
        "date": "2026-09-07",
        "supported": True,
        "market_open": False,
        "reason": "regular_full_closure",
    }
    assert market_calendar_status(date(2026, 9, 8)) == {
        "date": "2026-09-08",
        "supported": True,
        "market_open": True,
        "reason": "regular_session_date",
    }
    # NYSE explicitly does not observe Saturday 2028 New Year's Day on the
    # preceding year-end Friday.
    assert market_calendar_status(date(2027, 12, 31))["market_open"] is True


def test_one_off_closure_override_is_distinct_from_regular_holidays():
    payload = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    assert payload["one_off_full_closures"]["2025-01-09"].startswith(
        "National Day of Mourning"
    )
    assert "2025-01-09" not in payload["regular_full_closures"]
    assert market_calendar_status(date(2025, 1, 9)) == {
        "date": "2025-01-09",
        "supported": True,
        "market_open": False,
        "reason": "one_off_full_closure",
    }


def test_unreviewed_year_fails_closed_in_both_runtimes():
    unreviewed = date(max(load_market_calendar()["supported_years"]) + 1, 1, 2)
    python = market_calendar_status(unreviewed)
    powershell = _powershell_calendar_statuses(unreviewed, unreviewed)[
        unreviewed.isoformat()
    ]

    assert python == powershell
    assert python["supported"] is False
    assert python["market_open"] is False
    assert python["reason"] == "calendar_year_unsupported"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda payload: payload.pop("regular_full_closures"),
        lambda payload: payload.update({"supported_years": 2026}),
        lambda payload: payload.update({"regular_full_closures": []}),
        lambda payload: payload.update(
            {"regular_full_closures": ["2026-02-30"]}
        ),
        lambda payload: payload.update(
            {"one_off_full_closures": ["2025-01-09"]}
        ),
    ],
)
def test_incomplete_or_malformed_calendar_fails_closed_in_both_runtimes(
    tmp_path, monkeypatch, mutator
):
    payload = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    mutator(payload)
    damaged_path = tmp_path / "damaged_calendar.json"
    damaged_path.write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(market_calendar, "CALENDAR_PATH", damaged_path)
    market_calendar.load_market_calendar.cache_clear()
    try:
        python = market_calendar.market_calendar_status(date(2026, 9, 8))
        powershell = _powershell_calendar_statuses(
            date(2026, 9, 8),
            date(2026, 9, 8),
            calendar_path=damaged_path,
        )["2026-09-08"]
    finally:
        market_calendar.load_market_calendar.cache_clear()

    assert python["supported"] is False
    assert python["market_open"] is False
    assert python["reason"] == "calendar_file_invalid"
    assert powershell["supported"] is False
    assert powershell["market_open"] is False
    assert powershell["reason"] == "calendar_contract_invalid"
