from datetime import datetime, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from app.utils.display_time import (
    display_timezone_label,
    format_display_timestamp,
    parse_utc_timestamp,
    resolve_context_display_timezone,
    resolve_display_timezone,
)


def test_browser_timezone_formats_central_summer_and_winter_with_dst_labels():
    central = resolve_display_timezone("America/Chicago")

    assert format_display_timestamp(
        "2026-07-22T19:15:00Z", central
    ) == "Jul 22, 2026 at 02:15:00 PM CDT"
    assert format_display_timestamp(
        "2026-12-22T20:15:00+00:00", central
    ) == "Dec 22, 2026 at 02:15:00 PM CST"


def test_browser_timezone_change_updates_the_same_utc_instant_without_storage_change():
    timestamp_utc = "2026-07-22T19:15:00Z"
    central = resolve_display_timezone("America/Chicago")
    eastern = resolve_display_timezone("America/New_York")

    assert format_display_timestamp(timestamp_utc, central).endswith(
        "02:15:00 PM CDT"
    )
    assert format_display_timestamp(timestamp_utc, eastern).endswith(
        "03:15:00 PM EDT"
    )
    assert timestamp_utc == "2026-07-22T19:15:00Z"


def test_streamlit_browser_context_drives_the_display_timezone():
    central = resolve_context_display_timezone(
        SimpleNamespace(timezone="America/Chicago")
    )

    assert central.source == "browser"
    assert central.name == "America/Chicago"
    assert format_display_timestamp(
        "2026-09-01T07:41:32Z", central
    ).endswith("02:41:32 AM CDT")


def test_fall_back_repeated_hour_preserves_the_two_distinct_instants():
    central = resolve_display_timezone("America/Chicago")

    first = format_display_timestamp(
        "2026-11-01T06:30:00Z",
        central,
        format_string="%I:%M %p %Z %z",
    )
    second = format_display_timestamp(
        "2026-11-01T07:30:00Z",
        central,
        format_string="%I:%M %p %Z %z",
    )

    assert first == "01:30 AM CDT -0500"
    assert second == "01:30 AM CST -0600"


def test_invalid_or_missing_browser_zone_falls_back_to_explicit_utc():
    invalid = resolve_display_timezone("Not/A_Real_Zone")
    missing = resolve_display_timezone(None)

    assert invalid.source == "fallback"
    assert invalid.name == "UTC"
    assert missing.name == "UTC"
    assert format_display_timestamp(
        "2026-09-01T07:41:32Z", invalid
    ).endswith("07:41:32 AM UTC")


def test_legacy_naive_and_offset_timestamps_normalize_to_the_same_utc_instant():
    naive = parse_utc_timestamp(datetime(2026, 9, 1, 7, 41, 32))
    offset = parse_utc_timestamp("2026-09-01T02:41:32-05:00")

    assert naive == datetime(2026, 9, 1, 7, 41, 32, tzinfo=timezone.utc)
    assert offset == naive


def test_timezone_label_names_the_iana_zone_and_historical_abbreviation():
    central = resolve_display_timezone("America/Chicago")

    assert display_timezone_label(
        central,
        datetime(2026, 12, 22, 20, 15, tzinfo=ZoneInfo("UTC")),
    ) == "America/Chicago (CST)"
