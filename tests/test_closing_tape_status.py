from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
import pandas as pd

import backend.closing_tape.surface_artifact as surface_artifact_module
from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape.config import _market_times
from backend.closing_tape.contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
)
from backend.closing_tape.status import MODEL_MANIFEST_CONTRACT_VERSION
from backend.closing_tape.model_artifact import MODEL_ARTIFACT_FORMAT
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH
from backend.closing_tape.paper_evidence import (
    LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
    PAPER_CAMPAIGN_COVERAGE_CONTRACT_VERSION,
    PAPER_PREFIX_REPLAY_CONTRACT_VERSION,
    build_paper_feature_payload,
)
from backend.closing_tape.surface_artifact import (
    SURFACE_REPLAY_VERIFICATION_CONTRACT,
    canonical_replay_receipt_sha256,
    write_research_surface_artifact,
)
from backend.closing_tape.status import _model_gate, closing_tape_status
from backend.closing_tape.calibration import DEPLOYMENT_CALIBRATION_METHOD
from backend.closing_tape.candidate import (
    CANDIDATE_PACKAGE_CONTRACT_VERSION,
    PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION,
)
from backend.closing_tape.promotion import (
    record_promotion_decision,
    write_promoted_model_manifest,
)


UTC = timezone.utc


def _install_synthetic_promotion_source_replay(monkeypatch):
    real_inspect_dbn = surface_artifact_module.inspect_dbn
    real_build_surface = surface_artifact_module.build_research_surface_dataset

    def inspect_dbn(path, **kwargs):
        source = Path(path)
        payload = source.read_bytes()
        if payload.startswith(b"retained-source-"):
            return SimpleNamespace(
                local_file_intact=True,
                incomplete_reasons=(),
                sha256=hashlib.sha256(payload).hexdigest(),
                file_bytes=len(payload),
                records_seen=1,
                trade_records=1,
                tcbbo_records=1,
                tcbbo_timestamped_records=1,
                tcbbo_valid_nbbo_records=1,
                definition_records=0,
                statistics_records=0,
                subscription_acks=1,
                replay_completed=1,
                last_trade_event_ns=None,
            )
        return real_inspect_dbn(path, **kwargs)

    monkeypatch.setattr(surface_artifact_module, "inspect_dbn", inspect_dbn)

    def build_surface(catalog_paths, market_db_path, **kwargs):
        catalogs = tuple(Path(value).resolve() for value in catalog_paths)
        if catalogs:
            project_root = catalogs[0].parents[3]
            retained = project_root / "models" / "promotion_evidence" / "surfaces"
            candidate_path = project_root / "models" / "candidate.json"
            if candidate_path.is_file():
                candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
                surface_hash = candidate["training"]["surface_artifact_sha256"]
                artifact = retained / f"{surface_hash}.parquet"
                if artifact.is_file():
                    return pd.read_parquet(artifact, engine="pyarrow"), SimpleNamespace(
                        to_dict=lambda: {}
                    )
        return real_build_surface(catalog_paths, market_db_path, **kwargs)

    monkeypatch.setattr(
        surface_artifact_module,
        "build_research_surface_dataset",
        build_surface,
    )


@pytest.fixture(autouse=True)
def _decode_synthetic_promotion_sources(monkeypatch):
    _install_synthetic_promotion_source_replay(monkeypatch)


def test_missing_catalog_is_unavailable_not_zero(tmp_path):
    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert status["state"] == "unavailable"
    assert not status["catalog_available"]
    assert not status["usable_for_research"]
    assert status["observed"]["available"] is False
    assert status["inferred"]["available"] is False
    assert status["close_evidence"]["available"] is False


def test_status_reads_close_evidence_from_the_explicit_configured_database(tmp_path):
    configured_database = tmp_path / "configured" / "market.db"
    configured_database.parent.mkdir(parents=True)
    artifact_bytes = b'{"official_close":6500.25,"symbol":"SPX"}\n'
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    artifact_path = (
        tmp_path
        / "data"
        / "verified_close_sources"
        / "2026-08-25"
        / "SPX"
        / f"{artifact_sha256}.json"
    )
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(artifact_bytes)
    with sqlite3.connect(configured_database) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                official_close REAL NOT NULL,
                source_verified INTEGER NOT NULL,
                source_artifact_sha256 TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,
                correction_of_id INTEGER
            )
            """
        )
        connection.execute(
            """
            INSERT INTO eod_close_observations
                (symbol, trading_date, official_close, source_verified,
                 source_artifact_sha256, observed_at_utc, correction_of_id)
            VALUES ('SPX', '2026-08-25', 6500.25, 1, ?,
                    '2026-08-25T21:01:00+00:00', NULL)
            """,
            (artifact_sha256,),
        )

    status = closing_tape_status(
        tmp_path,
        trading_day=date(2026, 8, 25),
        market_db_path=configured_database,
    )

    assert status["close_evidence"]["available"] is True
    assert status["close_evidence"]["rows"] == 1
    assert status["close_evidence"]["families"] == ["SPX"]
    assert not (tmp_path / "data" / "market_data.db").exists()

    artifact_path.unlink()
    unavailable = closing_tape_status(
        tmp_path,
        trading_day=date(2026, 8, 25),
        market_db_path=configured_database,
    )
    assert unavailable["close_evidence"]["available"] is False
    assert "artifact" in unavailable["close_evidence"]["reason"]


def test_status_fails_closed_for_forked_verified_close_lineage(tmp_path):
    database = tmp_path / "market.db"
    artifacts = []
    for index in range(3):
        payload = f"official-close-{index}".encode()
        digest = hashlib.sha256(payload).hexdigest()
        path = (
            tmp_path
            / "data"
            / "verified_close_sources"
            / "2026-08-25"
            / "SPX"
            / f"{digest}.txt"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        artifacts.append(digest)
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY, symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL, official_close REAL NOT NULL,
                source_verified INTEGER NOT NULL,
                source_artifact_sha256 TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL, correction_of_id INTEGER
            )
            """
        )
        connection.executemany(
            "INSERT INTO eod_close_observations VALUES (?,?,?,?,?,?,?,?)",
            [
                (1, "SPX", "2026-08-25", 6500.0, 1, artifacts[0],
                 "2026-08-25T21:01:00+00:00", None),
                (2, "SPX", "2026-08-25", 6501.0, 1, artifacts[1],
                 "2026-08-25T21:02:00+00:00", 1),
                (3, "SPX", "2026-08-25", 6502.0, 1, artifacts[2],
                 "2026-08-25T21:03:00+00:00", 1),
            ],
        )

    status = closing_tape_status(
        tmp_path,
        trading_day=date(2026, 8, 25),
        market_db_path=database,
    )

    assert status["close_evidence"]["available"] is False
    assert "forked" in status["close_evidence"]["reason"]


def test_status_default_trading_day_uses_new_york_date_at_utc_rollover(
    tmp_path, monkeypatch
):
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            instant = datetime(2026, 9, 8, 1, 0, tzinfo=UTC)
            return instant.astimezone(tz) if tz is not None else instant.replace(tzinfo=None)

    monkeypatch.setattr("backend.closing_tape.status.datetime", FrozenDateTime)

    status = closing_tape_status(tmp_path)

    assert status["trading_date"] == "2026-09-07"


def test_catalog_without_feed_or_features_is_degraded(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="empty",
    )
    TapeCatalog(config.catalog_path).start_session(config)

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert status["state"] == "degraded"
    assert "required OPRA options feed is missing" in status["reasons"]
    assert "observed minute aggregates are unavailable" in status["reasons"]
    assert not status["model_gate"]["passed"]


def test_completed_feed_without_tcbbo_evidence_fails_closed(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="legacy-complete",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    catalog.register_feed(
        config.session_id,
        config.feeds[0],
        config.output_dir / "legacy.dbn",
    )
    catalog.update_feed_status(
        config.session_id,
        config.feeds[0].name,
        {"status": "complete", "complete": 1, "trade_records": 100, "sha256": "a" * 64},
    )
    catalog.finish_session(config.session_id, "complete")

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert status["state"] == "degraded"
    assert "completed OPRA feed has no verified TCBBO records" in status["reasons"]
    assert status["observed"]["tcbbo_evidence"]["records"] == 0
    assert not status["usable_for_research"]


def test_historical_status_marks_live_operational_counters_not_applicable(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 21, 0, tzinfo=UTC),
        session_id="historical",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    manifest = config.output_dir / "manifest.v2.json"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("{}", encoding="utf-8")
    catalog.register_feed(
        config.session_id,
        config.feeds[0],
        manifest,
        source_kind=HISTORICAL_SOURCE_KIND,
        evidence_contract_version=HISTORICAL_EVIDENCE_CONTRACT_VERSION,
        operational_counters_applicable=False,
        source_manifest_path=manifest,
    )
    catalog.update_feed_status(
        config.session_id,
        "opra_options",
        {
            "status": "complete",
            "complete": 1,
            "trade_records": 100,
            "tcbbo_records": 100,
            "tcbbo_timestamped_records": 100,
            "tcbbo_valid_nbbo_records": 99,
            "reconnect_count": 7,
            "subscription_acks": 9,
            "expected_subscription_acks": 9,
            "replay_completed": 9,
            "sha256": "a" * 64,
        },
    )
    catalog.finish_session(config.session_id, "complete")

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))
    raw_feed = status["observed"]["raw_feed"]

    assert raw_feed["operational_counter_semantics"] == (
        "not_applicable_to_historical_bundle"
    )
    assert raw_feed["reconnect_count"] is None
    assert raw_feed["subscription_acks"] is None
    assert raw_feed["replay_completed"] is None


def test_running_status_separates_observed_and_inferred_family_totals(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="running-family-totals",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    feed = config.feeds[0]
    catalog.register_feed(config.session_id, feed, config.output_dir / "running.dbn")
    observed_key = {
        "session_id": config.session_id,
        "feed_name": feed.name,
        "family_root": "SPX",
        "minute_utc": "2026-08-25T19:45:00+00:00",
    }
    catalog.upsert_observed_minutes([
        {
            **observed_key,
            "asset_class": "options",
            "trade_count": 4,
            "volume": 10,
            "notional": 2500,
            "call_count": 2,
            "put_count": 2,
            "call_volume": 5,
            "put_volume": 5,
            "call_premium": 1250,
            "put_premium": 1250,
            "first_price": 2.5,
            "high_price": 2.5,
            "low_price": 2.5,
            "last_price": 2.5,
            "price_volume_sum": 25,
            "largest_trade_size": 4,
            "largest_trade_notional": 1000,
            "nbbo_valid_count": 3,
            "data_quality_flagged_count": 1,
            "updated_at_utc": "2026-08-25T19:46:00+00:00",
        }
    ])
    catalog.upsert_inferred_minute_flow([
        {
            **observed_key,
            "inference_method": "trade_price_vs_pretrade_nbbo",
            "inference_version": "1.0",
            "source_sha256": "pending",
            "at_ask_count": 1,
            "at_bid_count": 1,
            "inside_count": 1,
            "unknown_count": 1,
            "at_ask_volume": 2,
            "at_bid_volume": 3,
            "inside_volume": 4,
            "unknown_volume": 1,
            "at_ask_notional": 500,
            "at_bid_notional": 750,
            "inside_notional": 1000,
            "unknown_notional": 250,
            "call_at_ask_notional": 500,
            "call_at_bid_notional": 0,
            "put_at_ask_notional": 0,
            "put_at_bid_notional": 750,
            "updated_at_utc": "2026-08-25T19:46:00+00:00",
        }
    ])

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    observed = status["observed"]["by_family"][0]
    inferred = status["inferred"]["by_family"][0]
    assert observed["family_root"] == "SPX"
    assert observed["trade_records"] == 4
    assert observed["valid_pretrade_nbbo_records"] == 3
    assert observed["data_quality_flagged_records"] == 1
    assert inferred["family_root"] == "SPX"
    assert inferred["classified_records"] == 4
    assert inferred["at_ask_count"] == inferred["at_bid_count"] == 1
    assert inferred["inside_count"] == inferred["unknown_count"] == 1
    assert status["observed"]["family_integrity"]["passed"] is True
    assert status["inferred"]["classification_alignment"]["passed"] is True


def test_running_status_degrades_on_inference_alignment_mismatch(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="misaligned-family-totals",
    )
    catalog = TapeCatalog(config.catalog_path)
    catalog.start_session(config)
    feed = config.feeds[0]
    catalog.register_feed(config.session_id, feed, config.output_dir / "running.dbn")
    key = {
        "session_id": config.session_id,
        "feed_name": feed.name,
        "family_root": "SPX",
        "minute_utc": "2026-08-25T19:45:00+00:00",
    }
    catalog.upsert_observed_minutes([{**key, "asset_class": "options", "trade_count": 2,
        "volume": 2, "notional": 2, "call_count": 1, "put_count": 1,
        "call_volume": 1, "put_volume": 1, "call_premium": 1, "put_premium": 1,
        "first_price": 1, "high_price": 1, "low_price": 1, "last_price": 1,
        "price_volume_sum": 2, "largest_trade_size": 1,
        "largest_trade_notional": 1, "nbbo_valid_count": 2,
        "updated_at_utc": "2026-08-25T19:46:00+00:00"}])
    catalog.upsert_inferred_minute_flow([{**key,
        "inference_method": "trade_price_vs_pretrade_nbbo", "inference_version": "1.0",
        "source_sha256": "pending", "at_ask_count": 1, "at_bid_count": 0,
        "inside_count": 0, "unknown_count": 0, "at_ask_volume": 1,
        "at_bid_volume": 0, "inside_volume": 0, "unknown_volume": 0,
        "at_ask_notional": 1, "at_bid_notional": 0, "inside_notional": 0,
        "unknown_notional": 0, "call_at_ask_notional": 1,
        "call_at_bid_notional": 0, "put_at_ask_notional": 0,
        "put_at_bid_notional": 0, "updated_at_utc": "2026-08-25T19:46:00+00:00"}])

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert status["state"] == "degraded"
    assert status["inferred"]["classification_alignment"]["passed"] is False
    assert status["inferred"]["classification_alignment"]["mismatched_families"] == ["SPX"]
    assert "SPX inferred location coverage 1/2 observed trades" in status["reasons"]


def _passing_model_manifest(tmp_path, *, include_approval=True, version="test-1"):
    models = tmp_path / "models"
    models.mkdir(exist_ok=True)
    artifact = models / "candidate.json"
    families = ["SPX", "NDX", "RUT", "VIX", "SPY"]
    paper_days = []
    candidate_day = date(2026, 7, 2)
    while len(paper_days) < 20:
        try:
            _market_times(candidate_day)
        except ValueError:
            pass
        else:
            paper_days.append(candidate_day)
        candidate_day += timedelta(days=1)
    source_days = list(paper_days)
    candidate_source_day = date(2026, 2, 2)
    while len(source_days) < 60:
        try:
            _market_times(candidate_source_day)
        except ValueError:
            pass
        else:
            if candidate_source_day not in source_days:
                source_days.append(candidate_source_day)
        candidate_source_day += timedelta(days=1)
    source_records = []
    surface_rows = []
    for index, trading_day in enumerate(source_days):
        config = build_session_config(
            tmp_path,
            trading_day=trading_day,
            now=datetime.combine(
                trading_day, datetime.min.time(), tzinfo=UTC
            ) + timedelta(hours=15),
            session_id=f"surface-{version}-{index}",
        )
        catalog = config.catalog_path
        source = config.output_dir / "opra.dbn"
        source.parent.mkdir(parents=True, exist_ok=True)
        source_bytes = f"retained-source-{version}-{index}".encode()
        source.write_bytes(source_bytes)
        source_hash = hashlib.sha256(source_bytes).hexdigest()
        catalog_store = TapeCatalog(catalog)
        catalog_store.start_session(config)
        feed = config.feeds[0]
        catalog_store.register_feed(config.session_id, feed, source)
        catalog_store.update_feed_status(
            config.session_id,
            feed.name,
            {
                "status": "complete",
                "complete": 1,
                "sha256": source_hash,
                "records_seen": 1,
                "trade_records": 1,
                "tcbbo_records": 1,
                "tcbbo_timestamped_records": 1,
                "tcbbo_valid_nbbo_records": 1,
                "file_bytes": len(source_bytes),
                "subscription_acks": 1,
                "expected_subscription_acks": 1,
                "replay_completed": 1,
            },
        )
        catalog_store.record_finalization_run(
            {
                "run_key": f"final-{version}-{index}",
                "session_id": config.session_id,
                "feed_name": feed.name,
                "source_sha256": source_hash,
                "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
                "attempted_at_utc": datetime.now(UTC).isoformat(),
                "complete": 1,
                "issues_json": "[]",
                "report_json": json.dumps(
                    {
                        "complete": True,
                        "source_sha256": source_hash,
                        "integrity": {
                            "sha256": source_hash,
                            "file_bytes": len(source_bytes),
                        },
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            }
        )
        catalog_store.finish_session(config.session_id, "complete")
        source_records.append(
            {
                "trading_date": trading_day.isoformat(),
                "session_id": config.session_id,
                "feed_name": feed.name,
                "source_kind": "databento_live",
                "source_sha256": source_hash,
                "source_path": str(source.resolve()),
                "source_bytes": len(source_bytes),
                "records_seen": 1,
                "tcbbo_records": 1,
                "catalog_path": str(catalog.resolve()),
            }
        )
        minute = datetime.combine(
            trading_day, datetime.min.time(), tzinfo=UTC
        ) + timedelta(hours=19, minutes=45)
        surface_row = {
            "trading_date": trading_day.isoformat(),
            "session_id": config.session_id,
            "feed_name": "opra_options",
            "family_root": "SPX",
            "minute_utc": minute.isoformat(),
            "feature_available_at_utc": minute.isoformat(),
            "source_sha256": source_hash,
            "capture_integrity_verified": True,
            "inference_method": "test",
            "inference_version": "v1",
            "reference_price_tier": "underlying_trade",
            "reference_subscription_epoch_id": f"epoch-{index}",
            "reference_subscription_generation": 1,
            "reference_price_epoch_eligible": True,
            "reference_price_epoch_status": "eligible",
            "surface_feature_version": "test-v1",
            "feature_schema_hash": MODEL_FEATURE_CONTRACT_HASH,
        }
        for column in MODEL_FEATURE_COLUMNS:
            surface_row.setdefault(column, 0.0)
        surface_rows.append(surface_row)
    hashes = sorted(item["source_sha256"] for item in source_records)
    surface_dir = models / "promotion_evidence" / "surfaces"
    frozen_surface = write_research_surface_artifact(
        pd.DataFrame(surface_rows),
        artifact_dir=surface_dir,
        report={},
        max_price_age_seconds=90.0,
    )
    ordered_source_records = sorted(
        source_records,
        key=lambda item: (
            item["trading_date"], item["session_id"], item["feed_name"],
            item["source_sha256"],
        ),
    )
    replay_receipt = {
        "contract_version": SURFACE_REPLAY_VERIFICATION_CONTRACT,
        "surface_artifact_sha256": frozen_surface.artifact_sha256,
        "semantic_equality_verified": True,
        "max_price_age_seconds": 90.0,
        "selected_sessions": [
            {
                "trading_date": item["trading_date"],
                "session_id": item["session_id"],
                "feed_name": item["feed_name"],
                "source_sha256": item["source_sha256"],
            }
            for item in ordered_source_records
        ],
        "selected_catalog_paths": sorted(
            {item["catalog_path"] for item in ordered_source_records}, key=str.casefold
        ),
        "source_evidence": ordered_source_records,
        "replay_surface_report": {},
    }
    surface_replay_hash = canonical_replay_receipt_sha256(replay_receipt)
    replay_dir = models / "promotion_evidence" / "surface_replays"
    replay_dir.mkdir(parents=True, exist_ok=True)
    replay_bytes = (
        json.dumps(
            replay_receipt,
            sort_keys=True,
            indent=2,
            allow_nan=False,
            separators=(",", ": "),
        )
        + "\n"
    ).encode()
    (replay_dir / f"{surface_replay_hash}.json").write_bytes(replay_bytes)

    artifact_payload = {
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "preprocessing": {
            "imputer_median": [0.0] * len(MODEL_FEATURE_COLUMNS),
            "scaler_mean": [0.0] * len(MODEL_FEATURE_COLUMNS),
            "scaler_scale": [1.0] * len(MODEL_FEATURE_COLUMNS),
        },
        "target": {"mean": 0.0, "scale": 1.0},
        "training": {
            "rows": 300, "epochs": 3, "seed": 17, "device": "cpu",
            "source_sha256s": hashes,
            "label_source_artifact_sha256s": [],
            "surface_artifact_sha256": frozen_surface.artifact_sha256,
            "surface_replay_receipt_sha256": surface_replay_hash,
        },
        "layers": [
            {"weight": [[0.0] * len(MODEL_FEATURE_COLUMNS)], "bias": [0.0]}
        ],
    }

    activated_at = "2026-07-01T19:00:00+00:00"
    activation_registered_at = "2026-07-01T19:00:01+00:00"
    paper_rows = []
    replay_receipts = []
    coverage_opportunities = []
    paper_label_hashes = []
    pending_opportunities = []
    for index, trading_day in enumerate(paper_days):
        source_record = source_records[index]
        _cash_open, analysis_due, _cash_close, _stop = _market_times(trading_day)
        feature_at = analysis_due.astimezone(UTC)
        live_prefix = {
            "contract_version": LIVE_PREFIX_RECEIPT_CONTRACT_VERSION,
            "prefix_sha256": source_record["source_sha256"],
            "cutoff_bytes": source_record["source_bytes"],
            "session_id": source_record["session_id"],
            "feed_name": "opra_options",
            "horizon_id": "cash-close-minus-15m-v1",
            "trading_date": trading_day.isoformat(),
            "feature_available_at_utc": feature_at.isoformat(),
            "catalog_path": source_record["catalog_path"],
            "source_path": source_record["source_path"],
        }
        payload_hashes = []
        opportunity_rows = []
        for family_index, family in enumerate(families):
            reference = 100.0 + family_index
            forecast_key = hashlib.sha256(
                f"{version}:{trading_day}:{family}".encode()
            ).hexdigest()
            forecast_identity = {
                "forecast_key": forecast_key,
                "model_version": version,
                "artifact_sha256": "pending",
                "source_sha256": source_record["source_sha256"],
                "session_id": source_record["session_id"],
                "family_root": family,
                "trading_date": trading_day.isoformat(),
                "decision_horizon_minutes": 15,
                "feature_available_at_utc": feature_at.isoformat(),
                "reference_price": reference,
                "incumbent_predicted_close": reference + 2.0,
                "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
            }
            opportunity_rows.append((family, forecast_identity, reference))
        pending_opportunities.append((trading_day, live_prefix, opportunity_rows))

    # Close artifacts are part of the frozen model provenance, so finalize them
    # before serializing the model and the feature payload identities.
    vault = tmp_path / "data" / "verified_close_sources"
    for trading_day, _live_prefix, opportunity_rows in pending_opportunities:
        for family, _identity, _reference in opportunity_rows:
            close_bytes = f"official-close-{version}-{trading_day}-{family}".encode()
            close_hash = hashlib.sha256(close_bytes).hexdigest()
            close_path = vault / trading_day.isoformat() / family / f"{close_hash}.txt"
            close_path.parent.mkdir(parents=True, exist_ok=True)
            close_path.write_bytes(close_bytes)
            paper_label_hashes.append(close_hash)
    label_hashes = sorted(paper_label_hashes)
    artifact_payload["training"]["label_source_artifact_sha256s"] = label_hashes
    artifact.write_text(json.dumps(artifact_payload), encoding="utf-8")
    artifact_sha256 = hashlib.sha256(artifact.read_bytes()).hexdigest()

    deployment_calibration = {
        "method": DEPLOYMENT_CALIBRATION_METHOD,
        "alpha": 0.1,
        "target_coverage": 0.9,
        "asof_utc": "2026-07-01T18:00:00+00:00",
        "model_version": version,
        "artifact_sha256": artifact_sha256,
        "eligible_rows": 100,
        "excluded_future_rows": 0,
        "evidence_sha256": "c" * 64,
        "source_sha256s": hashes[:20],
        "label_source_artifact_sha256s": label_hashes,
        "family_radii": [
            {
                "family_root": family,
                "rows": 20,
                "sessions": 20,
                "radius_log_return": 0.01,
            }
            for family in families
        ],
    }
    candidate_package = {
        "contract_version": CANDIDATE_PACKAGE_CONTRACT_VERSION,
        "version": version,
        "artifact_path": artifact.name,
        "artifact_sha256": artifact_sha256,
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "training_rows": 300,
        "training_epochs": 3,
        "training_device": "cpu",
        "source_sha256s": hashes,
        "label_source_artifact_sha256s": label_hashes,
        "surface_artifact_sha256": frozen_surface.artifact_sha256,
        "surface_replay_receipt_sha256": surface_replay_hash,
        "deployment_calibration": deployment_calibration,
    }
    candidate_package_bytes = (
        json.dumps(
            candidate_package, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    candidate_package_sha256 = hashlib.sha256(candidate_package_bytes).hexdigest()
    candidate_package_dir = models / "candidate_packages"
    candidate_package_dir.mkdir(exist_ok=True)
    (candidate_package_dir / f"{candidate_package_sha256}.json").write_bytes(
        candidate_package_bytes
    )
    activation = {
        "contract_version": PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION,
        "activated_at_utc": activated_at,
        "candidate_package_path": f"candidate_packages/{candidate_package_sha256}.json",
        "candidate_package_sha256": candidate_package_sha256,
        "model_version": version,
        "artifact_sha256": artifact_sha256,
    }
    activation_bytes = (
        json.dumps(
            activation, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    activation_sha256 = hashlib.sha256(activation_bytes).hexdigest()
    activation_dir = models / "paper_activations"
    activation_dir.mkdir(exist_ok=True)
    (activation_dir / f"{activation_sha256}.json").write_bytes(activation_bytes)

    close_hash_iterator = iter(paper_label_hashes)
    for trading_day, live_prefix, opportunity_rows in pending_opportunities:
        payload_hashes = []
        built_rows = []
        for family, forecast_identity, reference in opportunity_rows:
            forecast_identity["artifact_sha256"] = artifact_sha256
            feature_json, feature_hash = build_paper_feature_payload(
                {column: 0.0 for column in MODEL_FEATURE_COLUMNS},
                forecast_identity=forecast_identity,
                live_prefix_receipt=live_prefix,
            )
            payload_hashes.append(feature_hash)
            built_rows.append(
                {
                    "forecast_key": forecast_identity["forecast_key"],
                    "session_id": forecast_identity["session_id"],
                    "trading_date": trading_day.isoformat(),
                    "family_root": family,
                    "feature_available_at_utc": forecast_identity[
                        "feature_available_at_utc"
                    ],
                    "recorded_at_utc": (
                        datetime.fromisoformat(
                            forecast_identity["feature_available_at_utc"]
                        )
                        + timedelta(seconds=1)
                    ).isoformat(),
                    "decision_horizon_minutes": 15,
                    "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
                    "source_sha256": forecast_identity["source_sha256"],
                    "model_version": version,
                    "artifact_sha256": artifact_sha256,
                    "activation_receipt_sha256": activation_sha256,
                    "candidate_package_sha256": candidate_package_sha256,
                    "activated_at_utc": activated_at,
                    "activation_registered_at_utc": activation_registered_at,
                    "feature_payload_sha256": feature_hash,
                    "feature_payload_json": feature_json,
                    "prefix_replay_receipt_sha256": "pending",
                    "campaign_coverage_receipt_sha256": "pending",
                    "close_source_artifact_sha256": next(close_hash_iterator),
                    "reference_price": reference,
                    "candidate_predicted_log_return": 0.0,
                    "candidate_predicted_close": reference,
                    "incumbent_predicted_close": reference + 2.0,
                    "official_close": reference + 0.5,
                }
            )
        replay_receipt = {
            "contract_version": PAPER_PREFIX_REPLAY_CONTRACT_VERSION,
            "model_version": version,
            "artifact_sha256": artifact_sha256,
            "catalog_path": live_prefix["catalog_path"],
            "source_path": live_prefix["source_path"],
            "session_id": live_prefix["session_id"],
            "feed_name": live_prefix["feed_name"],
            "horizon_id": live_prefix["horizon_id"],
            "trading_date": live_prefix["trading_date"],
            "feature_available_at_utc": live_prefix["feature_available_at_utc"],
            "prefix_sha256": live_prefix["prefix_sha256"],
            "cutoff_bytes": live_prefix["cutoff_bytes"],
            "record_sequence": 1,
            "processed_sequence": 1,
            "last_trade_event_ns": None,
            "final_source_sha256": live_prefix["prefix_sha256"],
            "final_source_bytes": live_prefix["cutoff_bytes"],
            "expected_subscription_acks": 1,
            "feature_payload_sha256s": sorted(payload_hashes),
            "families": sorted(families),
            "exact_semantics_verified": True,
        }
        replay_receipt_bytes = (
            json.dumps(
                replay_receipt,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
            + b"\n"
        )
        replay_hash = hashlib.sha256(replay_receipt_bytes).hexdigest()
        replay_receipts.append((replay_hash, replay_receipt_bytes))
        for row in built_rows:
            row["prefix_replay_receipt_sha256"] = replay_hash
            paper_rows.append(row)
        coverage_opportunities.append(
            {
                "trading_date": live_prefix["trading_date"],
                "session_id": live_prefix["session_id"],
                "feed_name": live_prefix["feed_name"],
                "horizon_id": live_prefix["horizon_id"],
                "prefix_sha256": live_prefix["prefix_sha256"],
                "feature_payload_sha256s": sorted(payload_hashes),
            }
        )
    coverage_receipt = {
        "contract_version": PAPER_CAMPAIGN_COVERAGE_CONTRACT_VERSION,
        "activated_at_utc": activated_at,
        "latest_counted_trading_date": max(paper_days).isoformat(),
        "families": sorted(families),
        "opportunities": coverage_opportunities,
    }
    coverage_bytes = (
        json.dumps(
            coverage_receipt,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        + b"\n"
    )
    coverage_hash = hashlib.sha256(coverage_bytes).hexdigest()
    for row in paper_rows:
        row["campaign_coverage_receipt_sha256"] = coverage_hash
    paper_rows.sort(
        key=lambda item: (
            item["trading_date"], item["family_root"], item["session_id"]
        )
    )
    paper_evidence_bytes = json.dumps(
        paper_rows, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    paper_evidence_hash = hashlib.sha256(paper_evidence_bytes).hexdigest()
    paper_root = models / "promotion_evidence" / "paper"
    for category in ("rows", "prefix_replays", "campaign_coverage"):
        (paper_root / category).mkdir(parents=True, exist_ok=True)
    (paper_root / "rows" / f"{paper_evidence_hash}.json").write_bytes(
        paper_evidence_bytes
    )
    for replay_hash, replay_receipt_bytes in replay_receipts:
        (paper_root / "prefix_replays" / f"{replay_hash}.json").write_bytes(
            replay_receipt_bytes
        )
    (paper_root / "campaign_coverage" / f"{coverage_hash}.json").write_bytes(
        coverage_bytes
    )

    manifest = {
        "enabled": True,
        "version": version,
        "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
        "model_manifest_contract_version": MODEL_MANIFEST_CONTRACT_VERSION,
        "complete_sessions": 60,
        "purged_folds": 55,
        "paper_sessions": 20,
        "paper_candidate_mae": 0.5,
        "paper_incumbent_mae": 1.5,
        "paper_family_metrics": [
            {
                "family_root": family, "rows": 20, "sessions": 20,
                "candidate_mae": 0.5, "incumbent_mae": 1.5,
            }
            for family in families
        ],
        "oos_improvement_pct": 3.0,
        "oos_improvement_ci_low_pct": 0.5,
        "held_out_rows": 100,
        "incumbent_rows": 100,
        "held_out_sessions": 20,
        "incumbent_sessions": 20,
        "torch_improvement_over_incumbent_pct": 3.0,
        "torch_improvement_over_incumbent_ci_low_pct": 0.5,
        "feature_schema_hash": MODEL_FEATURE_CONTRACT_HASH,
        "execution_device": "cpu",
        "inference_batch_rows": 5,
        "source_sha256s": hashes,
        "label_source_artifact_sha256s": label_hashes,
        "paper_close_source_artifact_sha256s": paper_label_hashes,
        "paper_evidence_sha256": paper_evidence_hash,
        "paper_activation_receipt_sha256s": [activation_sha256],
        "paper_prefix_replay_receipt_sha256s": [
            value[0] for value in replay_receipts
        ],
        "paper_final_tape_source_sha256s": [
            source_records[value]["source_sha256"] for value in range(20)
        ],
        "paper_campaign_opportunities": 20,
        "paper_campaign_coverage_receipt_sha256": coverage_hash,
        "artifact_path": artifact.name,
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "artifact_sha256": artifact_sha256,
        "surface_artifact_sha256": frozen_surface.artifact_sha256,
        "surface_replay_receipt_sha256": surface_replay_hash,
        "family_metrics": [
            {
                "family_root": family,
                "sessions": 20,
                "candidate_mae": 0.8,
                "ridge_mae": 1.0,
                "persistence_mae": 1.1,
                "incumbent_mae": 1.05,
                "rows": 20,
                "incumbent_rows": 20,
            }
            for family in families
        ],
        "calibration_metrics": [
            {"family_root": family, "sessions": 20, "coverage_error": 0.02}
            for family in families
        ],
        "deployment_calibration": deployment_calibration,
        "regime_metrics": [
            {
                "family_root": family,
                "volatility_regime": regime,
                "sessions": 5,
                "candidate_mae": 0.8,
                "ridge_mae": 1.0,
            }
            for family in families
            for regime in ("calm", "normal", "stressed")
        ],
    }
    if include_approval:
        manifest["promotion_approval"] = record_promotion_decision(
            tmp_path,
            manifest,
            approved_by="test-promotion-operator",
            decision="APPROVE",
            approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
        )
    return manifest


def test_model_gate_verifies_artifact_family_calibration_and_regimes(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert status["model_gate"]["passed"]
    assert status["model_gate"]["artifact_verified"]
    assert status["model_gate"]["artifact_surface_provenance_matches"]
    assert status["model_gate"]["paper_prefix_replay_verified"]
    assert status["model_gate"]["paper_campaign_opportunities"] == 20
    assert status["model_gate"]["regime_slices_verified"] == 15


def _approve_and_write_manifest(tmp_path, manifest):
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )


def _missing_hashes(label, count):
    return [
        hashlib.sha256(f"missing-{label}-{index}".encode()).hexdigest()
        for index in range(count)
    ]


def test_model_gate_and_runtime_reject_nonexistent_retained_paper_hashes(tmp_path):
    from backend.closing_tape.production import load_promoted_model_runtime

    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    manifest["paper_evidence_sha256"] = _missing_hashes("rows", 1)[0]
    manifest["paper_prefix_replay_receipt_sha256s"] = _missing_hashes(
        "prefix", 20
    )
    manifest["paper_final_tape_source_sha256s"] = _missing_hashes("final", 20)
    manifest["paper_close_source_artifact_sha256s"] = _missing_hashes(
        "close", 100
    )
    manifest["paper_campaign_coverage_receipt_sha256"] = _missing_hashes(
        "coverage", 1
    )[0]
    _approve_and_write_manifest(tmp_path, manifest)

    gate = _model_gate(tmp_path)

    assert gate["passed"] is False
    assert gate["paper_prefix_replay_verified"] is False
    assert "retained paper evidence is invalid" in str(gate["reason"])
    with pytest.raises(RuntimeError, match="retained paper evidence is invalid"):
        load_promoted_model_runtime(tmp_path)


def test_model_gate_replays_retained_paper_predictions_with_actual_model(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    row_path = (
        tmp_path
        / "models"
        / "promotion_evidence"
        / "paper"
        / "rows"
        / f"{manifest['paper_evidence_sha256']}.json"
    )
    rows = json.loads(row_path.read_text(encoding="utf-8"))
    rows[0]["candidate_predicted_log_return"] = 0.01
    rows[0]["candidate_predicted_close"] = rows[0]["reference_price"] * math.exp(
        0.01
    )
    row_bytes = json.dumps(
        rows, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    row_hash = hashlib.sha256(row_bytes).hexdigest()
    row_path.with_name(f"{row_hash}.json").write_bytes(row_bytes)
    manifest["paper_evidence_sha256"] = row_hash
    _approve_and_write_manifest(tmp_path, manifest)

    gate = _model_gate(tmp_path)

    assert gate["passed"] is False
    assert "paper prediction does not reproduce" in str(gate["reason"])


def test_model_gate_binds_campaign_coverage_to_real_activation(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    paper_root = tmp_path / "models" / "promotion_evidence" / "paper"
    coverage_path = (
        paper_root
        / "campaign_coverage"
        / f"{manifest['paper_campaign_coverage_receipt_sha256']}.json"
    )
    coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
    coverage["activated_at_utc"] = "2026-06-30T19:00:00+00:00"
    coverage_bytes = (
        json.dumps(
            coverage, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    coverage_hash = hashlib.sha256(coverage_bytes).hexdigest()
    coverage_path.with_name(f"{coverage_hash}.json").write_bytes(coverage_bytes)
    row_path = (
        paper_root / "rows" / f"{manifest['paper_evidence_sha256']}.json"
    )
    rows = json.loads(row_path.read_text(encoding="utf-8"))
    for row in rows:
        row["campaign_coverage_receipt_sha256"] = coverage_hash
    row_bytes = json.dumps(
        rows, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    row_hash = hashlib.sha256(row_bytes).hexdigest()
    row_path.with_name(f"{row_hash}.json").write_bytes(row_bytes)
    manifest["paper_campaign_coverage_receipt_sha256"] = coverage_hash
    manifest["paper_evidence_sha256"] = row_hash
    _approve_and_write_manifest(tmp_path, manifest)

    gate = _model_gate(tmp_path)

    assert gate["passed"] is False
    assert "coverage is bound to another activation" in str(gate["reason"])


def test_model_gate_and_runtime_reject_a_truthy_non_boolean_enabled_value(tmp_path):
    from backend.closing_tape.production import load_promoted_model_runtime

    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    manifest["enabled"] = "false"
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )

    gate = _model_gate(tmp_path)

    assert gate["passed"] is False
    assert gate["enabled"] is False
    assert "manifest is not enabled" in str(gate["reason"])
    with pytest.raises(RuntimeError, match="manifest is not enabled"):
        load_promoted_model_runtime(tmp_path)


def test_model_gate_rejects_artifact_provenance_mismatch(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    artifact_path = tmp_path / "models" / "candidate.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["training"]["source_sha256s"] = ["f" * 64]
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    artifact_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    manifest["artifact_sha256"] = artifact_hash
    manifest["deployment_calibration"]["artifact_sha256"] = artifact_hash
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert not status["model_gate"]["artifact_verified"]
    assert not status["model_gate"]["artifact_dbn_provenance_matches"]
    assert "artifact DBN provenance" in status["model_gate"]["reason"]


def test_model_gate_rejects_surface_replay_provenance_mismatch(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    manifest["surface_replay_receipt_sha256"] = "e" * 64
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert not status["model_gate"]["artifact_verified"]
    assert not status["model_gate"]["artifact_surface_provenance_matches"]
    assert "surface replay provenance" in status["model_gate"]["reason"]


def test_promoted_manifest_publish_is_atomic_gate_verified_and_conflict_safe(tmp_path):
    manifest = _passing_model_manifest(tmp_path)

    path = write_promoted_model_manifest(tmp_path, manifest)

    assert path.name == "closing_tape_model.json"
    assert closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))["model_gate"]["passed"]
    assert write_promoted_model_manifest(tmp_path, manifest) == path
    changed = dict(manifest, publication_note="different but still gate-valid")
    changed["promotion_approval"] = record_promotion_decision(
        tmp_path,
        changed,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, 0, 1, tzinfo=UTC),
    )
    with pytest.raises(FileExistsError, match="explicit replacement"):
        write_promoted_model_manifest(tmp_path, changed)
    assert json.loads(path.read_text(encoding="utf-8"))["version"] == "test-1"


def test_promoted_manifest_publish_leaves_no_live_file_when_gate_fails(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest.pop("deployment_calibration")

    with pytest.raises(ValueError, match="failed runtime gate"):
        write_promoted_model_manifest(tmp_path, manifest)

    assert not (tmp_path / "models" / "closing_tape_model.json").exists()
    assert not list((tmp_path / "models").glob("*.tmp"))


def test_model_gate_requires_frozen_deployment_calibration(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest.pop("deployment_calibration")
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "deployment conformal calibration" in status["model_gate"]["reason"]


def test_model_gate_rejects_invalid_deployment_evidence_identity(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest["deployment_calibration"]["evidence_sha256"] = "invalid"
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "OOS evidence hash" in status["model_gate"]["reason"]


def test_model_gate_requires_one_paper_candidate_activation_receipt(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest["paper_activation_receipt_sha256s"] = []
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "candidate activation receipt" in status["model_gate"]["reason"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("paper_prefix_replay_receipt_sha256s", []),
        ("paper_prefix_replay_receipt_sha256s", ["a" * 64] * 20),
        ("paper_final_tape_source_sha256s", ["invalid"] * 20),
        ("paper_campaign_opportunities", 19),
        ("paper_campaign_coverage_receipt_sha256", "invalid"),
    ],
)
def test_model_gate_rejects_incomplete_paper_prefix_replay_proof(
    tmp_path, field, value
):
    manifest = _passing_model_manifest(tmp_path)
    manifest[field] = value
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert not status["model_gate"]["paper_prefix_replay_verified"]
    assert "prefix replay" in status["model_gate"]["reason"]


def test_model_gate_rehashes_paper_candidate_activation_receipt(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    activation_hash = manifest["paper_activation_receipt_sha256s"][0]
    activation_path = (
        tmp_path / "models" / "paper_activations" / f"{activation_hash}.json"
    )
    activation_path.write_bytes(activation_path.read_bytes() + b" ")
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert not status["model_gate"]["paper_activation_verified"]
    assert "activation receipt" in status["model_gate"]["reason"]


def test_model_gate_rehashes_activated_candidate_package(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    activation_hash = manifest["paper_activation_receipt_sha256s"][0]
    activation = json.loads(
        (
            tmp_path
            / "models"
            / "paper_activations"
            / f"{activation_hash}.json"
        ).read_text(encoding="utf-8")
    )
    package_path = (
        tmp_path / "models" / activation["candidate_package_path"]
    )
    package_path.write_bytes(package_path.read_bytes() + b" ")
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert not status["model_gate"]["paper_activation_verified"]
    assert "candidate package" in status["model_gate"]["reason"]


@pytest.mark.parametrize("changed_field", ["evidence_sha256", "radius_log_return"])
def test_model_gate_binds_deployment_calibration_to_activated_candidate(
    tmp_path, changed_field
):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    if changed_field == "evidence_sha256":
        manifest["deployment_calibration"]["evidence_sha256"] = "e" * 64
    else:
        manifest["deployment_calibration"]["family_radii"][0][
            "radius_log_return"
        ] = 0.02
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert status["model_gate"]["paper_activation_verified"]
    assert (
        "deployment calibration does not match the activated candidate package"
        in status["model_gate"]["reason"]
    )


def test_model_gate_rejects_missing_or_unbound_close_artifact_evidence(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    manifest["deployment_calibration"]["label_source_artifact_sha256s"] = ["f" * 64]
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-promotion-operator",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 26, tzinfo=UTC),
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "calibration close artifacts" in status["model_gate"]["reason"]


def test_model_gate_rejects_aggregate_gain_that_hides_family_or_hash_failure(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    next(
        row for row in manifest["family_metrics"] if row["family_root"] == "VIX"
    )["candidate_mae"] = 1.2
    (tmp_path / "models" / "candidate.json").write_bytes(b"tampered")
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "VIX MAE does not beat ridge" in status["model_gate"]["reason"]
    assert "artifact SHA-256 does not match" in status["model_gate"]["reason"]


def test_model_gate_rejects_missing_or_underperforming_incumbent_evidence(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest["incumbent_rows"] = 99
    manifest["torch_improvement_over_incumbent_ci_low_pct"] = -0.1
    manifest["family_metrics"][0]["candidate_mae"] = 1.1
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    reason = status["model_gate"]["reason"]
    assert "coverage 99/100 held-out rows" in reason
    assert "versus incumbent does not exclude zero" in reason
    assert "SPX MAE does not beat incumbent" in reason


def test_model_gate_fails_closed_on_malformed_numeric_manifest_fields(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest["complete_sessions"] = "not-a-number"
    manifest["family_metrics"][0]["sessions"] = None
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "complete_sessions 0 < 60" in status["model_gate"]["reason"]


def test_model_gate_rejects_wrong_feature_contract(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest["feature_schema_hash"] = "a" * 64
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    assert "does not match the running feature contract" in status["model_gate"]["reason"]


def test_cuda_model_gate_requires_speed_and_numerical_equivalence(tmp_path):
    manifest = _passing_model_manifest(tmp_path)
    manifest.update(
        execution_device="cuda",
        cuda_speedup_vs_cpu=0.9,
        cuda_benchmark_rows=100,
        inference_batch_rows=5,
        cuda_max_abs_prediction_difference=0.01,
        cuda_prediction_tolerance=0.001,
    )
    (tmp_path / "models" / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    status = closing_tape_status(tmp_path, trading_day=date(2026, 8, 25))

    assert not status["model_gate"]["passed"]
    reason = status["model_gate"]["reason"]
    assert "no measured speedup" in reason
    assert "do not match deployed inference batch" in reason
    assert "exceed the declared numerical tolerance" in reason
import hashlib
import json
