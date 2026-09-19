from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3

from backend.research.shadow_formula import (
    NAIVE_LAST_PRICE_V1,
    PIN_CONTEXT_LINEAR_V1,
    PointInTimeObservation,
    ShadowFormulaEngine,
    ShadowPredictionJournal,
)
from tools import evaluate_shadow_formulas as tool


UTC = timezone.utc


def _observation(*, day_offset: int = 0, symbol: str = "NDX") -> PointInTimeObservation:
    event = datetime(2026, 8, 25, 18, 0, tzinfo=UTC) + timedelta(days=day_offset)
    received = event + timedelta(milliseconds=1)
    index = received + timedelta(milliseconds=1)
    processed = index + timedelta(milliseconds=1)
    features = {
        "spot",
        "gamma_pin",
        "zero_gamma",
        "gross_gex",
        "net_gex",
        "top_strike_share",
        "spot_return_5m",
    }
    return PointInTimeObservation(
        symbol=symbol,
        provider="databento",
        ts_event_utc=event,
        ts_recv_utc=received,
        observation_index_utc=index,
        processed_at_utc=processed,
        feature_asof_utc={name: index for name in features},
        calculation_id=f"calc-{symbol}-{day_offset}",
        universe_sha256="a" * 64,
        selected_universe_sha256="b" * 64,
        subscription_epoch_id="e" * 64,
        subscription_generation=1,
        instrument_definition_version="definition-v1",
        symbol_mapping_version="mapping-v1",
        spot=29_000.0,
        gamma_pin=29_025.0,
        zero_gamma=28_975.0,
        gross_gex=1_000.0,
        net_gex=-250.0,
        top_strike_share=0.2,
        spot_return_5m=0.001,
        crossed_quote_count=0,
        fresh_quote_count=100,
        expected_quote_count=200,
        paired_quote_count=50,
        quote_age_seconds=1.0,
        source_confidence=0.8,
        expiration_days=0.0,
        validation_is_valid=True,
    )


def test_shadow_evaluation_is_read_only_auditable_and_never_promotes(tmp_path):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    engine = ShadowFormulaEngine()
    predictions = [
        engine.evaluate(_observation(), NAIVE_LAST_PRICE_V1),
        engine.evaluate(_observation(), PIN_CONTEXT_LINEAR_V1),
    ]
    for prediction in predictions:
        assert journal.save(prediction)
        journal.score_outcome(
            prediction.prediction_id,
            realized_price=29_010.0,
            realized_timestamp_utc=prediction.target_timestamp_utc,
        )

    before = journal_path.stat().st_mtime_ns
    report = tool.evaluate(journal_path)

    assert journal_path.stat().st_mtime_ns == before
    assert report["sqlite_quick_check"] == "ok"
    assert report["integrity_checks"]["production_signal_replaced_rows"] == 0
    assert report["integrity_checks"]["same_bar_or_early_outcome_rows"] == 0
    assert report["integrity_checks"]["lookahead_feature_timestamp_instances"] == 0
    assert report["paired_candidate_vs_baseline"][0]["common_scored_timestamps"] == 1
    assert report["paired_candidate_vs_baseline"][0]["promotion_supported"] is False
    assert report["walk_forward_validation"]["executed"] is False
    assert report["walk_forward_validation"]["independent_session_count"] == 1
    assert report["walk_forward_validation"]["status"] == "insufficient_evidence"
    assert "1 independent CT trading sessions" in report["walk_forward_validation"]["reason"]
    assert report["decision"]["state"] == "SHADOW_ONLY"
    assert len(report["logical_snapshot_sha256"]) == 64


def test_shadow_evaluation_cli_writes_rerunnable_json_and_markdown(tmp_path, capsys):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    prediction = ShadowFormulaEngine().evaluate(_observation(), NAIVE_LAST_PRICE_V1)
    journal.save(prediction)
    journal.score_outcome(
        prediction.prediction_id,
        realized_price=29_005.0,
        realized_timestamp_utc=prediction.target_timestamp_utc,
    )
    output_json = tmp_path / "report.json"
    output_markdown = tmp_path / "report.md"

    assert tool.main([
        "--journal", str(journal_path),
        "--output-json", str(output_json),
        "--output-markdown", str(output_markdown),
    ]) == 0

    assert '"promotion_supported": false' in output_json.read_text(encoding="utf-8")
    assert "SHADOW ONLY" in output_markdown.read_text(encoding="utf-8")
    assert '"artifact_schema_version"' in capsys.readouterr().out


def test_shadow_walk_forward_status_uses_actual_multi_session_counts(tmp_path):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    engine = ShadowFormulaEngine()
    for day_offset in range(2):
        for formula in (NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1):
            prediction = engine.evaluate(_observation(day_offset=day_offset), formula)
            assert journal.save(prediction)
            journal.score_outcome(
                prediction.prediction_id,
                realized_price=29_010.0 + day_offset,
                realized_timestamp_utc=prediction.target_timestamp_utc,
            )

    report = tool.evaluate(journal_path)
    status = report["walk_forward_validation"]

    assert status["executed"] is False
    assert status["fit_performed"] is False
    assert status["promotion_supported"] is False
    assert status["independent_session_count"] == 2
    assert status["paired_scored_independent_session_count"] == 2
    assert status["paired_scored_sessions_by_symbol"] == {"SPX": 0, "NDX": 2}
    assert "2 independent CT trading sessions" in status["reason"]
    assert "fewer than two" not in status["reason"]


def test_shadow_walk_forward_status_reports_ready_without_fitting_or_promotion(tmp_path):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    engine = ShadowFormulaEngine()
    for day_offset in range(10):
        for symbol in ("SPX", "NDX"):
            for formula in (NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1):
                prediction = engine.evaluate(
                    _observation(day_offset=day_offset, symbol=symbol), formula
                )
                assert journal.save(prediction)
                journal.score_outcome(
                    prediction.prediction_id,
                    realized_price=29_010.0 + day_offset,
                    realized_timestamp_utc=prediction.target_timestamp_utc,
                )

    report = tool.evaluate(journal_path)
    status = report["walk_forward_validation"]

    assert status["status"] == "ready_for_explicit_purged_evaluator"
    assert status["evidence_gate_passed"] is True
    assert status["executed"] is False
    assert status["fit_performed"] is False
    assert status["promotion_supported"] is False
    assert status["independent_session_count"] == 10
    assert status["paired_scored_sessions_by_symbol"] == {"SPX": 10, "NDX": 10}
    assert report["decision"]["promotion_supported"] is False
    chronological = report["chronological_candidate_evaluation"]
    assert chronological["executed"] is True
    assert chronological["fit_performed"] is False
    assert chronological["held_out_session_count"] == 5
    assert chronological["promotion_supported"] is False
    assert chronological["candidate"] == {
        "formula_id": "shadow-pin-context-no-zero-gamma",
        "formula_version": "0.2.0-preregistered",
        "fitted": False,
        "promotion_allowed": False,
    }
    assert chronological["held_out_metrics"]["v2_no_zero_gamma"]["n"] == 10
    assert report["decision"]["frozen_benchmark_disposition"] == (
        "REJECTED_PROMOTION_CANDIDATE"
    )


def test_shadow_evaluator_never_pairs_different_process_epochs(tmp_path):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    engine = ShadowFormulaEngine()
    baseline = engine.evaluate(_observation(), NAIVE_LAST_PRICE_V1)
    candidate = engine.evaluate(
        replace(
            _observation(),
            subscription_epoch_id="f" * 64,
            calculation_id="different-process",
        ),
        PIN_CONTEXT_LINEAR_V1,
    )
    for prediction in (baseline, candidate):
        assert journal.save(prediction)
        journal.score_outcome(
            prediction.prediction_id,
            realized_price=29_010.0,
            realized_timestamp_utc=prediction.target_timestamp_utc,
        )

    report = tool.evaluate(journal_path)

    assert report["paired_candidate_vs_baseline"] == []
    assert report["walk_forward_validation"][
        "paired_scored_independent_session_count"
    ] == 0


def test_shadow_evaluator_excludes_legacy_epochless_rows_from_evidence(tmp_path):
    journal_path = tmp_path / "shadow.db"
    journal = ShadowPredictionJournal(journal_path)
    engine = ShadowFormulaEngine()
    for formula in (NAIVE_LAST_PRICE_V1, PIN_CONTEXT_LINEAR_V1):
        prediction = engine.evaluate(_observation(), formula)
        assert journal.save(prediction)
        journal.score_outcome(
            prediction.prediction_id,
            realized_price=29_010.0,
            realized_timestamp_utc=prediction.target_timestamp_utc,
        )
    with sqlite3.connect(journal_path) as connection:
        rows = connection.execute(
            "SELECT prediction_id, provenance_json FROM shadow_prediction_journal"
        ).fetchall()
        for prediction_id, raw_provenance in rows:
            provenance = json.loads(raw_provenance)
            provenance.pop("subscription_epoch_id", None)
            connection.execute(
                "UPDATE shadow_prediction_journal SET provenance_json=? "
                "WHERE prediction_id=?",
                (json.dumps(provenance, sort_keys=True), prediction_id),
            )

    report = tool.evaluate(journal_path)

    assert report["artifact_schema_version"] == "marketpin-shadow-evaluation-v3"
    assert report["integrity_checks"][
        "missing_or_invalid_process_identity_rows"
    ] == 2
    assert report["paired_candidate_vs_baseline"] == []
    assert report["walk_forward_validation"]["independent_session_count"] == 0
    assert all(
        summary["provenance_eligible_rows"] == 0
        for summary in report["formula_symbol_results"]
    )
