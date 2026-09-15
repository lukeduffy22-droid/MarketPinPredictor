from datetime import datetime, timezone

import pytest

from app.services.dashboard_status import (
    diagnostic_state_label, near_close_caption, near_close_seconds,
)


@pytest.mark.parametrize("stamp,expected", [
    ("2026-09-08T19:45:00+00:00", 900),
    ("2026-09-08T19:59:40+00:00", 20),
    ("2026-09-08T20:00:00+00:00", None),
    ("2026-09-08T19:44:59+00:00", None),
    ("2026-09-07T19:50:00+00:00", None),
    ("2026-09-12T19:50:00+00:00", None),
    ("2026-11-27T17:50:00+00:00", 600),
    ("2026-11-27T18:00:00+00:00", None),
])
def test_countdown_respects_session_boundaries(stamp, expected):
    assert near_close_seconds(datetime.fromisoformat(stamp)) == expected


def test_naive_clock_is_not_treated_as_exchange_time():
    with pytest.raises(ValueError):
        near_close_seconds(datetime(2026, 9, 8, 15, 50))


def test_last_minute_does_not_claim_fifteen_minutes_remain():
    assert "less than 1 minute" in near_close_caption(20)
    assert "page update" in near_close_caption(20)
    assert "2m 10s" in near_close_caption(130)


def test_fallback_label_does_not_claim_exchange_is_closed():
    assert diagnostic_state_label("closed_context") == "historical / fallback context"
    assert diagnostic_state_label("generation_mismatch") == "generation mismatch"
