import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape.contracts import (
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
)
from backend.closing_tape.finalize import (
    _monitor_pause_reasons,
    _recoverable_monitor_pause_reasons,
    finalize_closed_session,
)
from backend.closing_tape.live_recorder import ProcessFileLock


UTC = timezone.utc


def test_only_monitor_starvation_is_recoverable_from_raw_rebuild():
    reason = (
        "recorder monitor paused for 153.2 seconds "
        "(expected no more than 30.0); system sleep or a severe scheduler stall is likely"
    )
    monitor_only = {
        "slow_reader_warnings": 1,
        "gaps_json": json.dumps([{"derived_incomplete": reason}]),
    }
    provider_warning = {"slow_reader_warnings": 1, "gaps_json": "[]"}
    callback_drop = {
        "slow_reader_warnings": 1,
        "gaps_json": json.dumps([{"derived_callback_drop_records": "1"}]),
    }
    current_monitor_only = {
        "slow_reader_warnings": 0,
        "gaps_json": json.dumps([{"operational_warning": reason}]),
    }
    current_monitor_with_provider_loss = {
        "slow_reader_warnings": 1,
        "gaps_json": json.dumps([{"operational_warning": reason}]),
    }

    assert _recoverable_monitor_pause_reasons(monitor_only) == (reason,)
    assert _recoverable_monitor_pause_reasons(current_monitor_only) == (reason,)
    assert _monitor_pause_reasons(current_monitor_with_provider_loss) == (reason,)
    assert _recoverable_monitor_pause_reasons(current_monitor_with_provider_loss) == ()
    assert _recoverable_monitor_pause_reasons(provider_warning) == ()
    assert _recoverable_monitor_pause_reasons(callback_drop) == ()


def test_closing_analysis_contract_persistence_import_boundary_is_available():
    """Regression for the retained close-minus-15 analyzer ImportError."""
    from backend.closing_analysis import run_closing_analysis
    from backend.closing_tape.replay import persist_contract_minute_rows

    class _Catalog:
        def __init__(self):
            self.observed = None
            self.inferred = None

        def upsert_observed_contract_minutes(self, rows):
            self.observed = rows

        def upsert_inferred_contract_minute_flow(self, rows):
            self.inferred = rows

    catalog = _Catalog()
    persist_contract_minute_rows(catalog, [{"observed": 1}], [{"inferred": 1}])

    assert callable(run_closing_analysis)
    assert catalog.observed == [{"observed": 1}]
    assert catalog.inferred == [{"inferred": 1}]


def _catalog_session(tmp_path, *, dbn_path=None):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="offline-finalize",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    catalog.register_feed(
        config.session_id,
        config.feeds[0],
        dbn_path or (config.output_dir / "raw.dbn"),
    )
    return config, catalog


def test_offline_finalizer_refuses_active_recorder_lock(tmp_path):
    config, _catalog = _catalog_session(tmp_path)

    with ProcessFileLock(config.lock_path):
        with pytest.raises(RuntimeError, match="already running"):
            finalize_closed_session(
                tmp_path,
                trading_date="2026-08-25",
                session_id=config.session_id,
            )


def test_offline_finalizer_rejects_catalog_path_outside_day_directory(tmp_path):
    outside = tmp_path / "outside.dbn"
    outside.write_bytes(b"not a DBN")
    config, _catalog = _catalog_session(tmp_path, dbn_path=outside)

    with pytest.raises(ValueError, match="escapes the trading-day directory"):
        finalize_closed_session(
            tmp_path,
            trading_date="2026-08-25",
            session_id=config.session_id,
        )


def test_live_finalizer_refuses_historical_source_contract(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 21, 0, tzinfo=UTC),
        session_id="historical",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    catalog.register_feed(
        config.session_id,
        config.feeds[0],
        config.output_dir / "manifest.v2.json",
        source_kind=HISTORICAL_SOURCE_KIND,
        evidence_contract_version=HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        operational_counters_applicable=False,
        source_manifest_path=config.output_dir / "manifest.v2.json",
    )

    with pytest.raises(ValueError, match="cannot process historical evidence"):
        finalize_closed_session(
            tmp_path,
            trading_date="2026-08-25",
            session_id=config.session_id,
        )


def test_offline_finalizer_does_not_promote_schema_incomplete_probe(tmp_path):
    sample = Path(__file__).parents[1] / ".codex_tmp" / "opra_probe_5d4993deeb574657a5908a74fe5fa7bd.dbn"
    if not sample.exists():
        pytest.skip("local OPRA probe is unavailable")
    config, catalog = _catalog_session(tmp_path)
    destination = config.output_dir / "raw.dbn"
    shutil.copyfile(sample, destination)
    catalog.update_feed_status(
        config.session_id,
        config.feeds[0].name,
        {
            "trade_records": 1250,
            "status": "stopped",
            "error": "prior recorder evidence",
            "gaps_json": '[{"prior_gap":"preserve me"}]',
        },
    )

    report = finalize_closed_session(
        tmp_path,
        trading_date="2026-08-25",
        session_id=config.session_id,
        now=datetime(2026, 8, 25, 16, 0, tzinfo=UTC),
    )

    assert report["complete"] is False
    assert any("schema replay incomplete" in issue for issue in report["issues"])
    assert "statistics subscription produced no records" in report["issues"]
    assert "definition subscription produced no records" in report["issues"]
    assert "cash session has not closed; full-session finalization is premature" in report["issues"]
    with catalog.connect(read_only=True) as connection:
        feed = connection.execute(
            "SELECT status, complete, sha256 FROM tape_feed_status WHERE session_id=?",
            (config.session_id,),
        ).fetchone()
    assert dict(feed) == {
        "status": "incomplete",
        "complete": 0,
        "sha256": report["source_sha256"],
    }
    with catalog.connect(read_only=True) as connection:
        audit = connection.execute(
            "SELECT * FROM tape_finalization_runs WHERE session_id=?",
            (config.session_id,),
        ).fetchone()
    assert audit["prior_status"] == "stopped"
    assert audit["prior_error"] == "prior recorder evidence"
    assert audit["prior_gaps_json"] == '[{"prior_gap":"preserve me"}]'
    assert json.loads(audit["report_json"])["source_sha256"] == report["source_sha256"]
    assert report["performance"]["stage_seconds"]["integrity"] >= 0
    assert report["performance"]["stage_seconds"]["pre_audit_total"] >= 0

    finalize_closed_session(
        tmp_path,
        trading_date="2026-08-25",
        session_id=config.session_id,
        now=datetime(2026, 8, 25, 16, 0, tzinfo=UTC),
    )
    with catalog.connect(read_only=True) as connection:
        audit_count = connection.execute(
            "SELECT COUNT(*) FROM tape_finalization_runs WHERE session_id=?",
            (config.session_id,),
        ).fetchone()[0]
    assert audit_count == 1
