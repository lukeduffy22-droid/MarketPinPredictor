import json
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import sessionmaker

import backend.database as database_module
from backend.database import (
    _trading_date,
    get_prediction_score_backlog,
    get_prediction_score_summary,
    init_db,
    save_gamma_calculation_inputs,
    save_prediction_snapshot,
    score_prediction_snapshots,
    upsert_eod_close,
    upsert_verified_eod_close,
    upsert_verified_eod_close_bundle,
)


def test_prediction_trading_date_uses_eastern_session_date():
    assert _trading_date(datetime(2026, 8, 25, 1, 0, tzinfo=timezone.utc)) == date(2026, 8, 24)


def _minimal_prediction(**overrides):
    prediction = {
        "symbol": "SPX",
        "provider": "test",
        "timestamp": "2026-06-18T15:30:00Z",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 101.0,
        "quote_timestamp_utc": "2026-06-18T15:29:59Z",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 3,
        "pin_payload": {"price": 100.0},
    }
    prediction.update(overrides)
    return prediction


@pytest.fixture
def isolated_prediction_store(tmp_path, monkeypatch):
    """Keep persistence tests out of the configured live market database."""
    database_path = tmp_path / "prediction-test.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    test_engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
    )
    test_session = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    export_dir = tmp_path / "exports" / "predictions"
    monkeypatch.setattr(database_module, "DATABASE_URL", database_url)
    monkeypatch.setattr(database_module, "engine", test_engine)
    monkeypatch.setattr(database_module, "SessionLocal", test_session)
    monkeypatch.setattr(database_module, "PREDICTION_EXPORT_DIR", export_dir)
    yield export_dir
    test_engine.dispose()


def test_prediction_snapshot_can_be_scored_with_eod_close(isolated_prediction_store):
    init_db()
    symbol = "SPX"
    calculation_id = "11111111-1111-1111-1111-111111111111"
    assert save_gamma_calculation_inputs(
        {
            "calculation_id": calculation_id,
            "symbol": symbol,
            "calculated_at_utc": "2026-06-18T15:29:59Z",
            "provider": "test",
            "status": "valid",
            "formula_version": "test-v1",
        },
        {"input_schema_version": "gamma-inputs-v1", "rows": [{"strike": 100.0}]},
    ) is not None
    prediction = {
        "symbol": symbol,
        "provider": "test",
        "model_version": "test_v1",
        "model_type": "unit-test",
        "timestamp": "2026-06-18T15:30:00",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 102.0,
        "confidence": 75.0,
        "expected_move_points": 2.0,
        "expected_move_pct": 2.0,
        "net_bias": "bullish",
        "feature_snapshot": {"spot": 100.0},
        "signals": [{"name": "test", "distance_points": 2.0}],
        "pin_payload": {"price": 100.0, "calculation_id": calculation_id},
        "inference_device": "cuda:0",
        "quote_timestamp_utc": "2026-06-18T15:29:58",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 3,
        "quote_age_seconds": 1.5,
        "active_contract_count": 120,
        "fresh_quote_count": 42,
        "gamma_pin": 101.0,
        "max_pain": 102.0,
        "zero_gamma": 99.0,
        "gross_gex": 1000.0,
        "call_gex": 600.0,
        "put_gex": 400.0,
    }

    snapshot = save_prediction_snapshot(prediction, prediction_mode="unit")
    assert snapshot is not None
    assert snapshot.symbol == symbol
    assert snapshot.inference_device == "cuda:0"
    assert snapshot.subscription_epoch_id == "e" * 64
    assert snapshot.subscription_generation == 3
    assert snapshot.fresh_quote_count == 42

    close = upsert_verified_eod_close(
        symbol, date(2026, 6, 18), 101.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )
    assert close is not None
    assert close.official_close == 101.0

    score = score_prediction_snapshots(
        symbol,
        date(2026, 6, 18),
        prediction_ids=[snapshot.id],
        selection_evidence_sha256="c" * 64,
    )
    assert score["scored"] >= 1
    assert score["evidence_scope"] == "diagnostic_research_only"
    assert score["training_eligible"] is False
    assert score["selection_evidence_sha256"] == "c" * 64
    assert score["selection_evidence_bound"] is True
    repeated = score_prediction_snapshots(
        symbol,
        date(2026, 6, 18),
        prediction_ids=[snapshot.id],
        selection_evidence_sha256="c" * 64,
    )
    assert repeated["close_observation_id"] == score["close_observation_id"]
    assert repeated["selection_evidence_sha256"] == "c" * 64
    conflict = score_prediction_snapshots(
        symbol,
        date(2026, 6, 18),
        prediction_ids=[snapshot.id],
        selection_evidence_sha256="d" * 64,
    )
    assert conflict["scored"] == 0
    assert conflict["reason"] == "selection_evidence_conflict"

    summary = get_prediction_score_summary(symbol, date(2026, 6, 18))
    assert summary["metric_scope"] == "performance_claim_eligible_only"
    assert summary["status"] == "no_claim_eligible_scores"
    assert summary["count"] == 0
    assert summary["mae"] is None
    assert summary["direction_hit_rate"] is None
    assert summary["excluded_non_claim_eligible_count"] == 1
    assert summary["diagnostic_research_only"]["count"] == 1
    assert summary["diagnostic_research_only"]["mae"] == pytest.approx(1.0)
    ledger_path = Path(isolated_prediction_store) / "accuracy_ledger" / "2026-06-18.ndjson"
    assert ledger_path.exists()
    assert symbol in ledger_path.read_text(encoding="utf-8")
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 1
    ledger_row = json.loads(ledger_path.read_text(encoding="utf-8").splitlines()[0])
    assert ledger_row["evidence_scope"] == "diagnostic_research_only"
    assert ledger_row["training_eligible"] is False
    assert ledger_row["performance_claim_eligible"] is False
    assert ledger_row["selection_evidence_sha256"] == "c" * 64
    with database_module.SessionLocal() as session:
        score_row = session.query(database_module.PredictionAccuracyObservation).one()
        assert score_row.evidence_scope == "diagnostic_research_only"
        assert score_row.training_eligible is False
    with pytest.raises(Exception, match="prediction accuracy observations are immutable"):
        with database_module.engine.begin() as connection:
            connection.execute(text(
                "INSERT OR REPLACE INTO prediction_accuracy_observations "
                "SELECT * FROM prediction_accuracy_observations"
            ))

    upsert_verified_eod_close(
        symbol, date(2026, 6, 18), 100.5, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close-correction",
        source_artifact_sha256="b" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 5, tzinfo=timezone.utc),
        correction_of_id=score["close_observation_id"],
    )
    corrected = score_prediction_snapshots(
        symbol,
        date(2026, 6, 18),
        prediction_ids=[snapshot.id],
        selection_evidence_sha256="c" * 64,
    )
    assert corrected["close_observation_id"] != score["close_observation_id"]
    with database_module.SessionLocal() as session:
        assert session.query(database_module.PredictionAccuracyObservation).count() == 2
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 2
    corrected_summary = get_prediction_score_summary(symbol, date(2026, 6, 18))
    assert corrected_summary["count"] == 0
    assert corrected_summary["excluded_non_claim_eligible_count"] == 1
    assert corrected_summary["diagnostic_research_only"]["count"] == 1
    assert corrected_summary["diagnostic_research_only"]["mae"] == pytest.approx(1.5)


def test_unverified_close_cannot_create_authoritative_accuracy(isolated_prediction_store):
    init_db()
    trading_day = date(2026, 6, 18)
    upsert_eod_close("SPX", trading_day, 6500.0, "manual-unverified")

    result = score_prediction_snapshots("SPX", trading_day, prediction_ids=[999])

    assert result["scored"] == 0
    assert result["reason"] == "missing_verified_eod_close"
    with database_module.SessionLocal() as session:
        assert session.query(database_module.PredictionAccuracyObservation).count() == 0
    assert not (Path(isolated_prediction_store) / "accuracy_ledger" / "2026-06-18.ndjson").exists()


def test_scoring_refuses_implicit_broad_intraday_selection(isolated_prediction_store):
    init_db()
    trading_day = date(2026, 6, 18)
    for minute in (30, 31):
        save_prediction_snapshot(
            {
                "symbol": "SPX", "provider": "test", "model_version": "test_v1",
                "model_type": "unit-test", "timestamp": f"2026-06-18T15:{minute}:00",
                "usable": True, "current_price": 100.0, "predicted_close": 101.0,
                "confidence": 70.0, "feature_snapshot": {}, "signals": [],
                "pin_payload": {},
            },
            prediction_mode="unit",
        )
    upsert_verified_eod_close(
        "SPX", trading_day, 101.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )

    result = score_prediction_snapshots("SPX", trading_day)

    assert result["scored"] == 0
    assert result["reason"] == "explicit_prediction_selection_required"
    with database_module.SessionLocal() as session:
        assert session.query(database_module.PredictionAccuracyObservation).count() == 0
        assert session.query(database_module.PredictionSnapshot).filter(
            database_module.PredictionSnapshot.scored_at_utc.isnot(None)
        ).count() == 0


def test_score_backlog_lists_only_verified_close_explicit_score_requests(
    isolated_prediction_store,
):
    init_db()
    trading_day = date(2026, 6, 18)
    calculation_id = "22222222-2222-2222-2222-222222222222"
    assert save_gamma_calculation_inputs(
        {
            "calculation_id": calculation_id,
            "symbol": "SPX",
            "calculated_at_utc": "2026-06-18T15:29:59Z",
            "provider": "test",
            "status": "valid",
            "formula_version": "test-v1",
        },
        {"input_schema_version": "gamma-inputs-v1", "rows": [{"strike": 100.0}]},
    ) is not None
    scoreable = save_prediction_snapshot(
        {
            "symbol": "SPX", "provider": "test", "model_version": "test_v1",
            "model_type": "unit-test", "timestamp": "2026-06-18T15:30:00",
            "usable": True, "current_price": 100.0, "predicted_close": 101.0,
            "confidence": 70.0, "feature_snapshot": {}, "signals": [],
            "pin_payload": {"calculation_id": calculation_id},
        },
        prediction_mode="unit",
    )
    blocked = save_prediction_snapshot(
        {
            "symbol": "SPX", "provider": "test", "model_version": "test_v1",
            "model_type": "unit-test", "timestamp": "2026-06-18T15:31:00",
            "usable": True, "current_price": 100.0, "predicted_close": 102.0,
            "confidence": 70.0, "feature_snapshot": {}, "signals": [],
            "pin_payload": {},
        },
        prediction_mode="unit",
    )
    upsert_verified_eod_close(
        "SPX", trading_day, 101.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )

    backlog = get_prediction_score_backlog("SPX", trading_day)

    assert backlog["read_only"] is True
    assert backlog["evidence_scope"] == "diagnostic_research_only"
    assert backlog["training_eligible"] is False
    assert backlog["performance_claim_eligible"] is False
    assert backlog["totals"] == {
        "scoreable": 1,
        "already_scored": 0,
        "blocked": 1,
        "candidate_predictions": 2,
    }
    group = backlog["groups"][0]
    assert group["scoreable_prediction_ids"] == [scoreable.id]
    assert group["blocked_predictions"] == [{
        "prediction_id": blocked.id,
        "reason": "missing_calculation_id",
    }]
    score_request = group["score_request"]
    assert score_request["writes_diagnostic_scores_only"] is True
    assert score_request["prediction_ids"] == [scoreable.id]
    assert len(score_request["selection_evidence_sha256"]) == 64
    with database_module.SessionLocal() as session:
        assert session.query(database_module.PredictionAccuracyObservation).count() == 0

    score = score_prediction_snapshots(
        "SPX",
        trading_day,
        prediction_ids=score_request["prediction_ids"],
        selection_evidence_sha256=score_request["selection_evidence_sha256"],
    )
    assert score["scored"] == 1
    assert score["training_eligible"] is False

    scored_backlog = get_prediction_score_backlog("SPX", trading_day)
    scored_group = scored_backlog["groups"][0]
    assert scored_group["scoreable_prediction_ids"] == []
    assert scored_group["already_scored_prediction_ids"] == [scoreable.id]
    assert scored_backlog["totals"]["already_scored"] == 1


def test_explicit_scoring_rejects_missing_calculation_lineage(isolated_prediction_store):
    init_db()
    trading_day = date(2026, 6, 18)
    snapshot = save_prediction_snapshot(
        {
            "symbol": "SPX", "provider": "test", "model_version": "test_v1",
            "model_type": "unit-test", "timestamp": "2026-06-18T15:30:00",
            "usable": True, "current_price": 100.0, "predicted_close": 101.0,
            "confidence": 70.0, "feature_snapshot": {}, "signals": [],
            "pin_payload": {"price": 100.0},
        },
        prediction_mode="unit-no-lineage",
    )
    upsert_verified_eod_close(
        "SPX", trading_day, 101.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )

    result = score_prediction_snapshots(
        "SPX",
        trading_day,
        prediction_ids=[snapshot.id],
    )

    assert result["scored"] == 0
    assert result["reason"] == "prediction_selection_not_eligible"
    assert result["ineligible_reasons"][snapshot.id] == "missing_calculation_id"


def test_eod_close_observations_are_append_only_idempotent_and_link_corrections(
    isolated_prediction_store,
):
    init_db()
    symbol = "SPX"
    trading_day = date(2026, 8, 25)

    upsert_eod_close(
        symbol,
        trading_day,
        6500.0,
        "manual-unverified",
        source_reference="operator-ticket-1",
        observed_at_utc=datetime(2026, 8, 25, 21, 1, tzinfo=timezone.utc),
    )
    upsert_eod_close(
        symbol,
        trading_day,
        6500.0,
        "manual-unverified",
        source_reference="operator-ticket-1",
        observed_at_utc=datetime(2026, 8, 25, 21, 2, tzinfo=timezone.utc),
    )
    with database_module.SessionLocal() as session:
        first_rows = session.query(database_module.EODCloseObservation).all()
        assert len(first_rows) == 1
        first_id = first_rows[0].id

    projection = upsert_eod_close(
        symbol,
        trading_day,
        6501.25,
        "manual-unverified",
        source_reference="operator-ticket-2",
        observed_at_utc=datetime(2026, 8, 25, 21, 3, tzinfo=timezone.utc),
    )

    with database_module.SessionLocal() as session:
        rows = session.query(database_module.EODCloseObservation).order_by(
            database_module.EODCloseObservation.id
        ).all()
        assert len(rows) == 2
        assert rows[0].official_close == 6500.0
        assert rows[1].official_close == 6501.25
        assert rows[1].correction_of_id == first_id
        assert rows[0].source_reference == "operator-ticket-1"
        assert not rows[0].source_verified
    assert projection.official_close == 6501.25


def test_verified_close_observations_are_database_immutable(isolated_prediction_store):
    init_db()
    trading_day = date(2026, 6, 18)
    upsert_verified_eod_close(
        "SPX", trading_day, 101.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )
    with database_module.engine.begin() as connection:
        close_id = connection.execute(text("SELECT id FROM eod_close_observations")).scalar_one()
    with pytest.raises(Exception, match="eod close observations are immutable"):
        with database_module.engine.begin() as connection:
            connection.execute(text(
                "UPDATE eod_close_observations SET official_close=102 WHERE id=:id"
            ), {"id": close_id})
    with pytest.raises(Exception, match="eod close observations are immutable"):
        with database_module.engine.begin() as connection:
            connection.execute(
                text("DELETE FROM eod_close_observations WHERE id=:id"),
                {"id": close_id},
            )
    with pytest.raises(Exception, match="eod close observations are immutable"):
        with database_module.engine.begin() as connection:
            connection.execute(text(
                "INSERT OR REPLACE INTO eod_close_observations "
                "SELECT * FROM eod_close_observations WHERE id=:id"
            ), {"id": close_id})


def test_verified_close_corrections_require_one_linear_authoritative_chain(
    isolated_prediction_store,
):
    init_db()
    trading_day = date(2026, 8, 25)
    observed = datetime(2026, 8, 25, 21, 1, tzinfo=timezone.utc)
    upsert_verified_eod_close(
        "SPX", trading_day, 6500.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=observed,
    )
    with database_module.SessionLocal() as session:
        root_id = session.query(database_module.EODCloseObservation.id).scalar()

    with pytest.raises(ValueError, match="explicit correction_of_id"):
        upsert_verified_eod_close(
            "SPX", trading_day, 6501.0, "sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            source_artifact_sha256="b" * 64,
            observed_at_utc=observed,
        )
    with pytest.raises(ValueError, match="current authoritative"):
        upsert_verified_eod_close(
            "SPX", trading_day, 6501.0, "sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            source_artifact_sha256="b" * 64,
            observed_at_utc=observed,
            correction_of_id=999,
        )
    upsert_verified_eod_close(
        "SPX", trading_day, 6501.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="b" * 64,
        observed_at_utc=observed,
        correction_of_id=root_id,
    )
    with pytest.raises(ValueError, match="current authoritative"):
        upsert_verified_eod_close(
            "SPX", trading_day, 6502.0, "sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            source_artifact_sha256="c" * 64,
            observed_at_utc=observed,
            correction_of_id=root_id,
        )

    projection = upsert_verified_eod_close(
        "SPX", trading_day, 6500.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=observed,
    )
    assert projection.official_close == 6501.0


@pytest.mark.parametrize(
    ("observed", "reason"),
    [
        (
            datetime(2026, 8, 25, 19, 59, tzinfo=timezone.utc),
            "official cash-session close",
        ),
        (
            datetime(2099, 8, 25, 21, 1, tzinfo=timezone.utc),
            "future clock skew",
        ),
    ],
)
def test_verified_close_writer_rejects_invalid_observation_chronology(
    isolated_prediction_store, observed, reason
):
    init_db()
    with pytest.raises(ValueError, match=reason):
        upsert_verified_eod_close(
            "SPX", date(2026, 8, 25), 6500.0, "sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            source_artifact_sha256="a" * 64,
            observed_at_utc=observed,
        )


def test_verified_close_path_requires_symbol_authority_and_reference(isolated_prediction_store):
    init_db()
    observed = datetime(2026, 8, 25, 21, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError, match="not an approved"):
        upsert_verified_eod_close(
            "SPX", date(2026, 8, 25), 6500.0, "nasdaq-official",
            source_reference="https://www.nasdaq.com/close", observed_at_utc=observed,
            source_artifact_sha256="a" * 64,
        )
    with pytest.raises(ValueError, match="source_reference"):
        upsert_verified_eod_close(
            "SPX", date(2026, 8, 25), 6500.0, "sp-global-official",
            source_reference="", source_artifact_sha256="a" * 64, observed_at_utc=observed,
        )
    with pytest.raises(ValueError, match="include a timezone"):
        upsert_verified_eod_close(
            "SPX", date(2026, 8, 25), 6500.0, "sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            source_artifact_sha256="a" * 64,
            observed_at_utc=datetime(2026, 8, 25, 21, 1),
        )

    upsert_verified_eod_close(
        "SPX", date(2026, 8, 25), 6500.0, "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close", observed_at_utc=observed,
        source_artifact_sha256="a" * 64,
    )
    with database_module.SessionLocal() as session:
        row = session.query(database_module.EODCloseObservation).one()
        assert row.source_verified
        assert row.source == "sp-global-official"
        assert row.source_artifact_sha256 == "a" * 64


def test_verified_close_bundle_rolls_back_every_family_on_mid_bundle_failure(
    isolated_prediction_store,
):
    init_db()
    observed = datetime(2026, 8, 25, 21, 1, tzinfo=timezone.utc)
    payloads = [
        {
            "symbol": "SPX", "trading_date": date(2026, 8, 25),
            "official_close": 6500.0, "source": "sp-global-official",
            "source_reference": "https://www.spglobal.com/spdji/official-close",
            "source_artifact_sha256": "a" * 64, "observed_at_utc": observed,
        },
        {
            "symbol": "NDX", "trading_date": date(2026, 8, 25),
            "official_close": 24000.0, "source": "nasdaq-official",
            "source_reference": "https://www.nasdaq.com/official-close",
            "source_artifact_sha256": "b" * 64, "observed_at_utc": observed,
            "correction_of_id": 999,
        },
    ]

    with pytest.raises(ValueError, match="correction_of_id"):
        upsert_verified_eod_close_bundle(payloads)

    with database_module.SessionLocal() as session:
        assert session.query(database_module.EODCloseObservation).count() == 0
        assert session.query(database_module.EODClose).count() == 0


def test_prediction_snapshot_preserves_zero_metrics_and_audit_fields(isolated_prediction_store):
    init_db()
    symbol = f"T{uuid4().hex[:6]}".upper()
    prediction = {
        "symbol": symbol,
        "provider": "test",
        "model_version": "test_v2",
        "model_type": "unit-test",
        "timestamp": "2026-08-25T23:30:00-07:00",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 100.0,
        "confidence": 50.0,
        "feature_schema_version": "features-v2",
        "feature_hash": "a" * 64,
        "feature_snapshot": {"spot": 100.0},
        "signals": [],
        "data_age_seconds": 0.0,
        "quote_timestamp_utc": "2026-08-25T23:29:59-07:00",
        "subscription_generation": 0,
        "quote_age_seconds": 0.0,
        "active_contract_count": 0,
        "fresh_quote_count": 0,
        "gamma_pin": 0.0,
        "max_pain": 0.0,
        "zero_gamma": 0.0,
        "gross_gex": 0.0,
        "net_gex": 0.0,
        "call_gex": 0.0,
        "put_gex": 0.0,
        "pin_payload": {},
    }

    snapshot = save_prediction_snapshot(prediction, prediction_mode="unit-zeroes")

    assert snapshot is not None
    assert snapshot.timestamp_utc == datetime(2026, 8, 26, 6, 30)
    assert snapshot.trading_date == date(2026, 8, 26)
    assert snapshot.feature_schema_version == "features-v2"
    assert snapshot.feature_hash == "a" * 64
    assert snapshot.data_age_seconds == 0.0
    assert snapshot.subscription_generation == 0
    assert snapshot.quote_age_seconds == 0.0
    assert snapshot.active_contract_count == 0
    assert snapshot.fresh_quote_count == 0
    assert snapshot.gamma_pin == 0.0
    assert snapshot.max_pain == 0.0
    assert snapshot.zero_gamma == 0.0
    assert snapshot.gross_gex == 0.0
    assert snapshot.net_gex == 0.0
    assert snapshot.call_gex == 0.0
    assert snapshot.put_gex == 0.0


def test_ndjson_export_failure_does_not_block_sqlite_persistence(
    isolated_prediction_store,
    monkeypatch,
):
    init_db()
    monkeypatch.setattr(
        database_module,
        "_append_prediction_jsonl",
        lambda _payload: (_ for _ in ()).throw(OSError("disk unavailable")),
    )
    prediction = {
        "symbol": f"T{uuid4().hex[:6]}".upper(),
        "timestamp": "2026-08-25T15:30:00Z",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 101.0,
        "quote_timestamp_utc": "2026-08-25T15:29:59Z",
        "subscription_generation": 1,
        "pin_payload": {},
    }

    snapshot = save_prediction_snapshot(prediction, prediction_mode="unit-export-failure")

    assert snapshot is not None
    assert snapshot.id is not None


def test_source_quote_persistence_is_idempotent(isolated_prediction_store):
    init_db()
    prediction = {
        "symbol": f"T{uuid4().hex[:6]}".upper(),
        "timestamp": "2026-08-25T15:30:00Z",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 101.0,
        "quote_timestamp_utc": "2026-08-25T15:29:59Z",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 2,
        "pin_payload": {},
    }

    first = save_prediction_snapshot(prediction, prediction_mode="unit-idempotent")
    second = save_prediction_snapshot(prediction, prediction_mode="unit-idempotent")

    assert first is not None
    assert second is not None
    assert second.id == first.id

    restarted = save_prediction_snapshot(
        {**prediction, "subscription_epoch_id": "f" * 64},
        prediction_mode="unit-idempotent",
    )
    assert restarted is not None
    assert restarted.id != first.id

    with database_module.engine.begin() as connection:
        with pytest.raises(DatabaseError):
            connection.execute(
                text(
                    "UPDATE prediction_snapshots "
                    "SET subscription_epoch_id = :epoch WHERE id = :snapshot_id"
                ),
                {"epoch": "a" * 64, "snapshot_id": first.id},
            )


def test_init_db_upgrades_existing_prediction_snapshot_metrics(tmp_path, monkeypatch):
    database_path = tmp_path / "legacy-predictions.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    test_engine = create_engine(
        database_url,
        connect_args={"check_same_thread": False},
    )
    test_session = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    with test_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE prediction_snapshots (
                id INTEGER PRIMARY KEY,
                symbol VARCHAR(16) NOT NULL,
                timestamp_utc DATETIME NOT NULL,
                trading_date DATE NOT NULL,
                prediction_mode VARCHAR(40),
                subscription_generation INTEGER,
                quote_timestamp_utc DATETIME
            )
        """))
        connection.execute(text("""
            INSERT INTO prediction_snapshots (
                symbol, timestamp_utc, trading_date, prediction_mode,
                subscription_generation, quote_timestamp_utc
            ) VALUES (
                'SPX', '2026-08-25 15:30:00', '2026-08-25',
                'legacy', 1, '2026-08-25 15:29:59'
            )
        """))
    monkeypatch.setattr(database_module, "DATABASE_URL", database_url)
    monkeypatch.setattr(database_module, "engine", test_engine)
    monkeypatch.setattr(database_module, "SessionLocal", test_session)

    init_db()

    inspector = inspect(test_engine)
    tables = set(inspector.get_table_names())
    columns = {column["name"] for column in inspector.get_columns("prediction_snapshots")}
    index_rows = inspector.get_indexes("prediction_snapshots")
    indexes = {index["name"] for index in index_rows}
    with test_engine.connect() as connection:
        row_count = connection.execute(text("SELECT COUNT(*) FROM prediction_snapshots")).scalar_one()
        passport_count = connection.execute(text("SELECT COUNT(*) FROM prediction_passports")).scalar_one()
    test_engine.dispose()

    assert row_count == 1
    assert "prediction_passports" in tables
    assert passport_count == 0
    assert {
        "feature_schema_version",
        "feature_hash",
        "data_age_seconds",
        "subscription_epoch_id",
        "net_gex",
    }.issubset(columns)
    assert "uix_prediction_snapshot_source_quote" in indexes
    source_quote_index = next(
        index
        for index in index_rows
        if index["name"] == "uix_prediction_snapshot_source_quote"
    )
    assert source_quote_index["column_names"] == [
        "symbol",
        "trading_date",
        "prediction_mode",
        "subscription_epoch_id",
        "subscription_generation",
        "quote_timestamp_utc",
    ]


def test_snapshot_missing_usable_is_never_marked_valid(isolated_prediction_store):
    init_db()
    prediction = _minimal_prediction()
    prediction.pop("usable")

    snapshot = save_prediction_snapshot(prediction, prediction_mode="missing-usable")

    assert snapshot is not None
    assert snapshot.is_valid is False


def test_publication_guard_change_rolls_back_snapshot_and_export(
    isolated_prediction_store,
):
    init_db()
    guard_calls = 0

    def _guard():
        nonlocal guard_calls
        guard_calls += 1
        return guard_calls < 3

    snapshot = save_prediction_snapshot(
        _minimal_prediction(),
        prediction_mode="guard-race",
        publication_guard=_guard,
    )

    assert snapshot is None
    with database_module.SessionLocal() as session:
        assert session.query(database_module.PredictionSnapshot).count() == 0
    assert not list(Path(isolated_prediction_store).rglob("*.ndjson"))

def test_init_db_upgrades_existing_close_observation_provenance(tmp_path, monkeypatch):
    database_path = tmp_path / "legacy-closes.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    test_engine = create_engine(database_url, connect_args={"check_same_thread": False})
    test_session = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    with test_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY,
                observation_key VARCHAR(64) NOT NULL,
                symbol VARCHAR(16) NOT NULL,
                trading_date DATE NOT NULL,
                official_close FLOAT NOT NULL,
                source VARCHAR(80) NOT NULL,
                source_reference VARCHAR(500),
                source_verified BOOLEAN NOT NULL,
                observed_at_utc DATETIME NOT NULL,
                ingested_at_utc DATETIME NOT NULL,
                correction_of_id INTEGER
            )
        """))
    monkeypatch.setattr(database_module, "DATABASE_URL", database_url)
    monkeypatch.setattr(database_module, "engine", test_engine)
    monkeypatch.setattr(database_module, "SessionLocal", test_session)

    init_db()
    init_db()  # additive migration and scope guard are idempotent

    columns = {
        column["name"] for column in inspect(test_engine).get_columns("eod_close_observations")
    }
    test_engine.dispose()
    assert "source_artifact_sha256" in columns


def test_init_db_classifies_legacy_accuracy_rows_as_diagnostic_only(tmp_path, monkeypatch):
    database_path = tmp_path / "legacy-accuracy.db"
    database_url = f"sqlite:///{database_path.as_posix()}"
    test_engine = create_engine(database_url, connect_args={"check_same_thread": False})
    test_session = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    with test_engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE prediction_accuracy_observations (
                id INTEGER PRIMARY KEY
            )
        """))
        connection.execute(text(
            "INSERT INTO prediction_accuracy_observations (id) VALUES (1)"
        ))
    monkeypatch.setattr(database_module, "DATABASE_URL", database_url)
    monkeypatch.setattr(database_module, "engine", test_engine)
    monkeypatch.setattr(database_module, "SessionLocal", test_session)

    init_db()

    columns = {
        column["name"]
        for column in inspect(test_engine).get_columns("prediction_accuracy_observations")
    }
    with test_engine.connect() as connection:
        migrated = connection.execute(text("""
            SELECT evidence_scope, training_eligible, selection_evidence_sha256
            FROM prediction_accuracy_observations
            WHERE id=1
        """)).mappings().one()
    assert {
        "evidence_scope", "training_eligible", "selection_evidence_sha256"
    }.issubset(columns)
    assert migrated["evidence_scope"] == "diagnostic_research_only"
    assert migrated["training_eligible"] == 0
    assert migrated["selection_evidence_sha256"] is None

    invalid_rows = [
        (2, "paper_evaluation", 0),
        (3, "diagnostic_research_only", 1),
    ]
    for row_id, evidence_scope, training_eligible in invalid_rows:
        with pytest.raises(Exception, match="must remain diagnostic-only"):
            with test_engine.begin() as connection:
                connection.execute(text("""
                    INSERT INTO prediction_accuracy_observations (
                        id, evidence_scope, training_eligible
                    ) VALUES (:id, :scope, :eligible)
                """), {
                    "id": row_id,
                    "scope": evidence_scope,
                    "eligible": training_eligible,
                })
    test_engine.dispose()
