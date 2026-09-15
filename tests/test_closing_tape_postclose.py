import json
import os
from datetime import date, datetime, timedelta, timezone

from backend.closing_tape.contracts import (
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
)
from backend.closing_tape.postclose import postclose_finalize_decision
from backend.closing_tape.replay import REPLAY_DECODER_VERSION
from tests.test_closing_tape_readiness import _complete_catalog


UTC = timezone.utc


def _incomplete_audit_catalog(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    with catalog.connect() as connection:
        connection.execute("DELETE FROM tape_finalization_runs")
    source = config.output_dir / f"complete-{config.session_id}.dbn"
    source_hash = "d" * 64
    prior_error = "retained recorder failure"
    prior_gaps = '[{"derived_callback_drop_records":"1"}]'
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            "status": "stopped",
            "complete": 0,
            "file_bytes": source.stat().st_size,
            "sha256": source_hash,
            "error": prior_error,
            "gaps_json": prior_gaps,
        },
    )
    with catalog.connect(read_only=True) as connection:
        session = dict(
            connection.execute(
                "SELECT * FROM tape_sessions WHERE session_id=?", (config.session_id,)
            ).fetchone()
        )
        prior_feed = dict(
            connection.execute(
                "SELECT * FROM tape_feed_status WHERE session_id=?", (config.session_id,)
            ).fetchone()
        )
    config.status_path.write_text(
        json.dumps({"session": session, "feeds": [prior_feed]}), encoding="utf-8"
    )
    attempted_at = datetime(2026, 8, 25, 20, 22, tzinfo=UTC)
    retained_mtime = (attempted_at - timedelta(seconds=5)).timestamp()
    os.utime(source, (retained_mtime, retained_mtime))
    os.utime(config.status_path, (retained_mtime, retained_mtime))
    integrity = {
        "sha256": source_hash,
        "file_bytes": source.stat().st_size,
        "records_seen": 1000,
        "trade_records": 1000,
        "tcbbo_records": 1000,
        "tcbbo_timestamped_records": 1000,
        "tcbbo_valid_nbbo_records": 990,
        "tcbbo_flagged_records": 0,
        "mapping_records": 0,
        "statistics_records": 5,
        "definition_records": 5,
        "first_event_ns": None,
        "last_event_ns": None,
        "last_receive_ns": None,
        "subscription_acks": 3,
        "replay_completed": 3,
    }
    issues = ["catalog records slow-reader or derived-queue loss"]
    report = {
        "session_id": config.session_id,
        "feed_name": "opra_options",
        "source_path": str(source.resolve()),
        "source_sha256": source_hash,
        "complete": False,
        "issues": issues,
        "operational_warnings": [],
        "integrity": integrity,
        "performance": {"replay_decoder_version": REPLAY_DECODER_VERSION},
    }
    catalog.record_finalization_run(
        {
            "run_key": "e" * 64,
            "session_id": config.session_id,
            "feed_name": "opra_options",
            "source_sha256": source_hash,
            "evidence_contract_version": prior_feed["evidence_contract_version"],
            "attempted_at_utc": attempted_at.isoformat(),
            "complete": 0,
            "issues_json": json.dumps(issues, separators=(",", ":")),
            "prior_status": "stopped",
            "prior_error": prior_error,
            "prior_gaps_json": prior_gaps,
            "report_json": json.dumps(report, sort_keys=True, separators=(",", ":")),
        }
    )
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            **integrity,
            "status": "incomplete",
            "complete": 0,
            "error": "; ".join(issues),
            "gaps_json": json.dumps(
                [{"offline_finalization_issue": issues[0]}], separators=(",", ":")
            ),
        },
    )
    return config, catalog, source


def test_postclose_finalizer_is_read_only_before_stop_time(tmp_path):
    decision = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 10, tzinfo=UTC),
    )

    assert decision["action"] == "not_due"
    assert not (tmp_path / "data").exists()


def test_postclose_finalizer_noops_for_matching_passing_audit(tmp_path):
    config, _catalog = _complete_catalog(tmp_path)

    decision = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )

    assert decision["action"] == "already_finalized"
    assert decision["session_id"] == config.session_id


def test_postclose_finalizer_requests_upgrade_when_audit_is_missing(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    with catalog.connect() as connection:
        connection.execute("DELETE FROM tape_finalization_runs")

    decision = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )

    assert decision == {"action": "finalize", "session_id": config.session_id}


def test_postclose_finalizer_reuses_unchanged_incomplete_audit(tmp_path):
    config, _catalog, _source = _incomplete_audit_catalog(tmp_path)

    decision = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )

    assert decision["action"] == "incomplete"
    assert decision["session_id"] == config.session_id
    assert decision["deduplicated"] is True
    assert decision["issues"] == ["catalog records slow-reader or derived-queue loss"]


def test_postclose_finalizer_retries_when_incomplete_evidence_changes(tmp_path):
    config, catalog, source = _incomplete_audit_catalog(tmp_path)

    source.write_bytes(source.read_bytes() + b"changed")
    changed_source = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )
    assert changed_source == {"action": "finalize", "session_id": config.session_id}

    config, catalog, source = _incomplete_audit_catalog(tmp_path / "status-change")
    config.status_path.write_text(
        config.status_path.read_text(encoding="utf-8") + " ", encoding="utf-8"
    )
    changed_status = postclose_finalize_decision(
        tmp_path / "status-change",
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )
    assert changed_status == {"action": "finalize", "session_id": config.session_id}

    config, catalog, _source = _incomplete_audit_catalog(tmp_path / "feed-change")
    catalog.update_feed_status(config.session_id, "opra_options", {"error": "changed"})
    changed_feed = postclose_finalize_decision(
        tmp_path / "feed-change",
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )
    assert changed_feed == {"action": "finalize", "session_id": config.session_id}


def test_postclose_recognizes_passing_historical_import_audit(tmp_path):
    config, catalog = _complete_catalog(tmp_path)
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            "source_kind": HISTORICAL_SOURCE_KIND,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "operational_counters_applicable": 0,
        },
    )
    catalog.record_finalization_run(
        {
            "run_key": "historical-postclose",
            "session_id": config.session_id,
            "feed_name": "opra_options",
            "source_sha256": "a" * 64,
            "evidence_contract_version": HISTORICAL_EVIDENCE_CONTRACT_VERSION,
            "attempted_at_utc": "2026-08-25T20:22:00+00:00",
            "complete": 1,
            "issues_json": "[]",
            "report_json": "{}",
        }
    )

    decision = postclose_finalize_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 20, 25, tzinfo=UTC),
    )

    assert decision["action"] == "already_finalized"
    assert decision["session_id"] == config.session_id
