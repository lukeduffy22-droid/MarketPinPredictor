from datetime import datetime, timedelta, timezone

from app.services.forecast_research_view import central_timestamp, forecast_rows, research_refresh_due

NOW = datetime(2026, 10, 5, 19, 59, 30, tzinfo=timezone.utc)


def test_refresh_only_rechecks_submitted_publication_every_fifteen_seconds():
    selected = {"SPX": 0, "NDX": 1}
    payload = {"selections": selected, "submitted_at_utc": NOW.isoformat()}
    assert not research_refresh_due(payload, selected, enabled=True, now=NOW + timedelta(seconds=14))
    assert research_refresh_due(payload, selected, enabled=True, now=NOW + timedelta(seconds=15))
    assert not research_refresh_due(payload, {"SPX": 1}, enabled=True, now=NOW + timedelta(minutes=5))
    assert not research_refresh_due(payload, selected, enabled=False, now=NOW + timedelta(minutes=5))
    assert not research_refresh_due(None, selected, enabled=True, now=NOW)


def test_eod_estimate_is_withheld_at_close_even_when_capture_is_fresh():
    from tests.test_live_api_demo import payload as demo_payload
    captured = demo_payload("SPX", now=NOW)
    item = captured["symbols"]["SPX"]
    item["forecast"].update(target_close_utc="2026-10-05T20:00:00Z", predicted_close=7700)
    item["capture"] = captured.copy()
    item["publication_gate"] = captured["publication_gate"]
    payload = {"symbols": {"SPX": item}}
    assert forecast_rows(payload, now=NOW)[0]["Research close"] == 7700
    closed = forecast_rows(payload, now=NOW + timedelta(seconds=30))[0]
    assert closed["Research close"] is None
    assert "TARGET_SESSION_HAS_CLOSED" in closed["Reason"]


def test_missing_target_time_and_stale_capture_fail_closed():
    payload = {"symbols": {"SPX": {"forecast": {"status": "RESEARCH_ONLY", "horizon_sessions": 0,
        "as_of_utc": NOW.isoformat(), "predicted_close": 7700}}}}
    assert forecast_rows(payload, now=NOW)[0]["Research close"] is None
    assert "STALE" == forecast_rows(payload, now=NOW + timedelta(minutes=3))[0]["Status"]


def test_central_display_handles_dst_and_never_changes_source_timestamp():
    stamp = "2026-10-05T19:59:30Z"
    assert central_timestamp(stamp) == "Oct 05, 2026 02:59:30 PM CDT"
    assert central_timestamp("2026-12-01T19:00:00Z") == "Dec 01, 2026 01:00:00 PM CST"
    assert central_timestamp("2026-12-01T19:00:00") == "Time unavailable"
    assert stamp == "2026-10-05T19:59:30Z"
