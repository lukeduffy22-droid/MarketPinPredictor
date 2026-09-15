from datetime import datetime, timezone

from app.utils.market_time import market_is_open


def test_market_status_is_closed_on_labor_day_2026():
    labor_day_mid_session_utc = datetime(
        2026, 9, 7, 15, 0, tzinfo=timezone.utc
    )

    assert market_is_open(labor_day_mid_session_utc) is False


def test_market_status_is_open_during_tuesday_regular_session():
    tuesday_mid_session_utc = datetime(
        2026, 9, 8, 15, 0, tzinfo=timezone.utc
    )

    assert market_is_open(tuesday_mid_session_utc) is True


def test_market_status_is_closed_after_early_close_cutoff():
    after_early_close_utc = datetime(
        2026, 11, 27, 18, 1, tzinfo=timezone.utc
    )

    assert market_is_open(after_early_close_utc) is False
