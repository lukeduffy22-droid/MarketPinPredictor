from datetime import date, datetime, timezone

from app.utils import market_time
from app.utils.time_et import ET, format_clock_et, is_early_close, is_market_holiday, utc_iso
from backend.api.routers import predict


def test_utc_iso_returns_explicit_utc_suffix():
    value = utc_iso(datetime(2026, 7, 22, 19, 15, tzinfo=timezone.utc))
    assert value.endswith("Z")
    assert value.startswith("2026-07-22T19:15:00")


def test_format_clock_et_handles_dst_in_summer_and_winter():
    summer = datetime(2026, 7, 22, 19, 15, tzinfo=timezone.utc)
    winter = datetime(2026, 12, 22, 20, 15, tzinfo=timezone.utc)

    assert format_clock_et(summer) == "15:15:00 ET"
    assert format_clock_et(winter) == "15:15:00 ET"


def test_2026_market_calendar_entries_are_present():
    holiday_dt = datetime(2026, 7, 3, 12, 0, tzinfo=ET)
    early_close_dt = datetime(2026, 11, 27, 12, 0, tzinfo=ET)

    assert is_market_holiday(holiday_dt) is True
    assert is_early_close(early_close_dt) is True


def test_market_time_wrapper_uses_shared_calendar(monkeypatch):
    early_close_now = datetime(2026, 11, 27, 10, 0, tzinfo=ET)
    monkeypatch.setattr(market_time, "now_et", lambda: early_close_now)

    assert market_time.get_close_time_today().hour == 13
    assert market_time.is_holiday(date(2026, 7, 3)) is True


def test_prediction_router_returns_explicit_utc_timestamps():
    value = predict._utc_iso()
    assert value.endswith("Z")
