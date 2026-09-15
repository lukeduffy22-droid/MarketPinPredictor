import sqlite3
from datetime import date, datetime, timezone

import pytest

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape.contracts import HISTORICAL_SOURCE_KIND


UTC = timezone.utc


def test_observed_and_inferred_minute_truths_are_separate(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")

    with sqlite3.connect(catalog.path) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        observed = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_observed_minute)")
        }
        inferred = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_inferred_minute_flow)")
        }
        feed_status = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_feed_status)")
        }

    assert "tape_observed_minute" in tables
    assert "tape_inferred_minute_flow" in tables
    assert "tape_observed_contract_minute" in tables
    assert "tape_inferred_contract_minute_flow" in tables
    assert "tape_inferred_reference_minute" in tables
    assert "at_ask_volume" not in observed
    assert {
        "nbbo_valid_count", "quoted_spread_sum", "trade_to_mid_abs_sum",
        "receive_lag_ns_max", "data_quality_flagged_count", "flag_last_count",
    } <= observed
    assert {"inference_method", "inference_version", "source_sha256", "at_ask_volume"} <= inferred
    assert {
        "source_kind",
        "evidence_contract_version",
        "operational_counters_applicable",
        "source_manifest_path",
        "source_components_json",
        "provisional_tcbbo_records",
        "provisional_tcbbo_timestamped_records",
        "provisional_tcbbo_valid_nbbo_records",
        "tcbbo_records",
    } <= feed_status
    with sqlite3.connect(catalog.path) as connection:
        contract_observed = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_observed_contract_minute)")
        }
        contract_inferred = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_inferred_contract_minute_flow)")
        }
    assert {
        "raw_symbol", "expiration", "option_type", "strike", "nbbo_valid_count",
        "last_pretrade_midpoint", "last_nbbo_event_ns",
    } <= contract_observed
    assert "at_ask_count" not in contract_observed
    assert {"raw_symbol", "expiration", "option_type", "strike", "at_ask_count"} <= contract_inferred
    with sqlite3.connect(catalog.path) as connection:
        reference = {
            row[1] for row in connection.execute("PRAGMA table_info(tape_inferred_reference_minute)")
        }
    assert {
        "reference_method", "reference_version", "estimated_price", "pair_count",
        "dispersion_bps", "parameters_json", "source_sha256",
    } <= reference


def test_observed_microstructure_columns_migrate_additively(tmp_path):
    path = tmp_path / "old.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE tape_observed_minute (session_id TEXT, feed_name TEXT, "
            "family_root TEXT, minute_utc TEXT, updated_at_utc TEXT)"
        )
    TapeCatalog(path)
    with sqlite3.connect(path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(tape_observed_minute)")}
    assert {"nbbo_valid_count", "quoted_spread_bps_sum", "receive_lag_ns_sum"} <= columns


def test_provisional_tcbbo_counters_persist_without_claiming_terminal_truth(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="provisional",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    catalog.register_feed(
        config.session_id, config.feeds[0], config.output_dir / "running.dbn"
    )
    catalog.update_feed_status(
        config.session_id,
        config.feeds[0].name,
        {
            "status": "running",
            "provisional_tcbbo_records": 100,
            "provisional_tcbbo_timestamped_records": 99,
            "provisional_tcbbo_valid_nbbo_records": 98,
        },
    )

    with catalog.connect(read_only=True) as connection:
        row = connection.execute(
            """
            SELECT provisional_tcbbo_records,
                   provisional_tcbbo_timestamped_records,
                   provisional_tcbbo_valid_nbbo_records,
                   tcbbo_records
            FROM tape_feed_status
            WHERE session_id=? AND feed_name=?
            """,
            (config.session_id, config.feeds[0].name),
        ).fetchone()

    assert dict(row) == {
        "provisional_tcbbo_records": 100,
        "provisional_tcbbo_timestamped_records": 99,
        "provisional_tcbbo_valid_nbbo_records": 98,
        "tcbbo_records": 0,
    }


def test_feed_registration_rejects_mismatched_source_contract(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="mismatched-contract",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)

    with pytest.raises(ValueError, match="does not match source_kind"):
        catalog.register_feed(
            config.session_id,
            config.feeds[0],
            config.output_dir / "manifest.v2.json",
            source_kind=HISTORICAL_SOURCE_KIND,
        )


def test_instrument_projection_rebuilds_from_latest_immutable_definition(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")
    common = {
        "session_id": "historical",
        "feed_name": "opra_options",
        "source_sha256": "a" * 64,
        "record_bytes": 520,
        "ts_event_ns": 1,
        "ts_recv_ns": 2,
        "option_type": "C",
        "expiration": "2026-08-27",
        "strike": 7000.0,
        "raw_record_sha256": "b" * 64,
        "raw_record": b"definition",
        "ingested_at_utc": "2026-08-27T00:00:00+00:00",
    }
    catalog.append_instrument_definition_observations(
        [
            {
                **common,
                "observation_key": "a" * 64 + ":100",
                "record_offset": 100,
                "instrument_id": 1,
                "raw_symbol": "SPXW  260827C07000000",
                "family_root": "SPX",
                "security_update_action": "A",
            },
            {
                **common,
                "observation_key": "a" * 64 + ":200",
                "record_offset": 200,
                "instrument_id": 1,
                "raw_symbol": "SPXW  260827C07000000",
                "family_root": "SPX",
                "security_update_action": "D",
            },
            {
                **common,
                "observation_key": "a" * 64 + ":300",
                "record_offset": 300,
                "instrument_id": 2,
                "raw_symbol": "NDXP  260827C25000000",
                "family_root": "NDX",
                "strike": 25000.0,
                "security_update_action": "A",
            },
        ]
    )

    count = catalog.rebuild_instruments_from_definition_observations(
        session_id="historical",
        feed_name="opra_options",
        source_sha256="a" * 64,
    )

    assert count == 1
    with catalog.connect(read_only=True) as connection:
        rows = connection.execute(
            "SELECT instrument_id, family_root FROM tape_instruments"
        ).fetchall()
    assert [tuple(row) for row in rows] == [(2, "NDX")]


def test_analysis_cutoff_persists_exact_barrier_sequence(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")

    catalog.record_analysis_cutoff(
        session_id="session-1",
        feed_name="opra_options",
        horizon_id="cash-close-minus-15m-v1",
        captured_at_utc="2026-08-25T19:45:00+00:00",
        event_cutoff_utc="2026-08-25T19:45:00+00:00",
        cutoff_bytes=1234,
        record_sequence=99,
        processed_sequence=99,
        prefix_sha256="a" * 64,
        last_trade_event_ns=1_787_686_500_000_000_000,
    )

    with catalog.connect(read_only=True) as connection:
        row = connection.execute(
            "SELECT record_sequence, processed_sequence FROM tape_analysis_cutoffs"
        ).fetchone()

    assert dict(row) == {"record_sequence": 99, "processed_sequence": 99}


def test_exclusive_lock_recovery_marks_orphaned_session_incomplete(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")
    first = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="orphaned",
    )
    catalog.start_session(first)
    catalog.register_feed("orphaned", first.feeds[0], tmp_path / "orphaned.dbn")
    catalog.update_feed_status("orphaned", first.feeds[0].name, {"status": "running"})

    recovered = catalog.abandon_running_sessions(recovery_session_id="replacement")

    assert recovered == ["orphaned"]
    with catalog.connect(read_only=True) as connection:
        session = connection.execute(
            "SELECT status, completed_at_utc, error FROM tape_sessions WHERE session_id='orphaned'"
        ).fetchone()
        feed = connection.execute(
            "SELECT status, complete, ended_at_utc, error FROM tape_feed_status WHERE session_id='orphaned'"
        ).fetchone()
    assert session["status"] == "incomplete"
    assert session["completed_at_utc"]
    assert "recovered by replacement" in session["error"]
    assert dict(feed) | {"ended_at_utc": bool(feed["ended_at_utc"])} == {
        "status": "incomplete",
        "complete": 0,
        "ended_at_utc": True,
        "error": "orphaned recorder session recovered by replacement",
    }
