import json
from datetime import date, timedelta

from app.utils.display_time import resolve_display_timezone
from app.utils.snapshot_history import (
    available_local_dates,
    available_local_dates_for_symbol,
    load_snapshots_for_local_day,
    local_day_utc_bounds,
    partition_snapshot_evidence,
    utc_partition_dates_for_local_day,
)


def _write_records(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )


def test_central_calendar_day_merges_records_across_utc_partitions(tmp_path):
    central = resolve_display_timezone("America/Chicago")
    exports = tmp_path / "exports"
    _write_records(
        exports / "SPX" / "2026-08-31.ndjson",
        [
            {"id": "before-utc-midnight", "generated_at_utc": "2026-08-31T23:59:26Z"},
            # Payload/write midnight races are tolerated by adjacent-file reads.
            {"id": "adjacent-file-race", "generated_at_utc": "2026-09-01T00:00:10Z"},
        ],
    )
    _write_records(
        exports / "SPX" / "2026-09-01.ndjson",
        [
            {"id": "after-utc-midnight", "generated_at_utc": "2026-09-01T00:00:26Z"},
            {"id": "local-day-end", "generated_at_utc": "2026-09-01T04:59:59Z"},
            {"id": "next-local-day", "generated_at_utc": "2026-09-01T05:00:00Z"},
        ],
    )

    august_31 = load_snapshots_for_local_day(
        exports, "SPX", "2026-08-31", central
    )
    september_1 = load_snapshots_for_local_day(
        exports, "SPX", "2026-09-01", central
    )

    assert [record["id"] for record in august_31.records] == [
        "before-utc-midnight",
        "adjacent-file-race",
        "after-utc-midnight",
        "local-day-end",
    ]
    assert [record["id"] for record in september_1.records] == ["next-local-day"]
    assert august_31.records[0]["generated_at_utc"] == "2026-08-31T23:59:26Z"


def test_local_day_bounds_are_dst_safe_for_23_and_25_hour_days():
    central = resolve_display_timezone("America/Chicago")

    spring_start, spring_end = local_day_utc_bounds("2026-03-08", central)
    fall_start, fall_end = local_day_utc_bounds("2026-11-01", central)

    assert spring_end - spring_start == timedelta(hours=23)
    assert fall_end - fall_start == timedelta(hours=25)


def test_partition_catalog_maps_utc_files_to_possible_local_dates():
    central = resolve_display_timezone("America/Chicago")

    assert available_local_dates(["2026-09-01"], central) == [
        "2026-09-01",
        "2026-08-31",
    ]
    assert "2026-09-01" in utc_partition_dates_for_local_day(
        date(2026, 8, 31), central
    )


def test_actual_date_catalog_uses_file_timestamp_bounds_not_utc_midnight(tmp_path):
    central = resolve_display_timezone("America/Chicago")
    exports = tmp_path / "exports"
    _write_records(
        exports / "SPX" / "2026-08-31.ndjson",
        [
            {"id": "first", "generated_at_utc": "2026-08-31T17:10:26Z"},
            {"id": "last", "generated_at_utc": "2026-08-31T23:59:26Z"},
        ],
    )

    assert available_local_dates_for_symbol(exports, "SPX", central) == [
        "2026-08-31"
    ]


def test_malformed_timestamps_are_excluded_and_reported(tmp_path):
    central = resolve_display_timezone("America/Chicago")
    exports = tmp_path / "exports"
    _write_records(
        exports / "VIX" / "2026-09-01.ndjson",
        [
            {"id": "bad", "generated_at_utc": "not-a-time"},
            {
                "id": "good",
                "generated_at_utc": "not-a-time",
                "timestamp_utc": "2026-09-01T02:00:00Z",
            },
        ],
    )

    selection = load_snapshots_for_local_day(
        exports, "VIX", "2026-08-31", central
    )

    assert [record["id"] for record in selection.records] == ["good"]
    assert selection.malformed_timestamp_records == 1


def test_snapshot_evidence_keeps_invalid_zero_gex_out_of_usable_records():
    evidence = partition_snapshot_evidence(
        [
            {
                "id": "valid",
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
                "subscription_epoch_id": "a" * 64,
                "subscription_generation": 1,
            },
            {
                "id": "legacy-epochless",
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
                "subscription_generation": 1,
            },
            {
                "id": "clock-failed",
                "validation_is_valid": False,
                "gamma_excluded_from_model": True,
                "gross_gex": 0,
            },
            {
                "id": "excluded",
                "validation_is_valid": True,
                "gamma_excluded_from_model": True,
            },
        ]
    )

    assert [record["id"] for record in evidence.usable_records] == ["valid"]
    assert [record["id"] for record in evidence.historical_records] == [
        "legacy-epochless"
    ]
    assert [record["id"] for record in evidence.diagnostic_records] == [
        "clock-failed",
        "excluded",
    ]
