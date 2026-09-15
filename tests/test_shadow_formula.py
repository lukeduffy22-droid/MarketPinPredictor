import math
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from backend.research.shadow_formula import (
    NAIVE_LAST_PRICE_V1,
    PIN_CONTEXT_LINEAR_V1,
    PointInTimeObservation,
    ShadowFormulaEngine,
    ShadowPredictionJournal,
    observation_from_databento_payload,
)


UTC = timezone.utc


def _observation(**overrides):
    event = datetime(2026, 8, 25, 13, 30, 0, 100_000, tzinfo=UTC)
    received = event + timedelta(milliseconds=2)
    index = received + timedelta(milliseconds=20)
    processed = index + timedelta(milliseconds=80)
    feature_names = {
        "spot",
        "gamma_pin",
        "zero_gamma",
        "gross_gex",
        "net_gex",
        "top_strike_share",
        "spot_return_5m",
    }
    values = dict(
        symbol="SPX",
        provider="databento",
        ts_event_utc=event,
        ts_recv_utc=received,
        observation_index_utc=index,
        processed_at_utc=processed,
        feature_asof_utc={name: index for name in feature_names},
        calculation_id="calc-001",
        universe_sha256="a" * 64,
        selected_universe_sha256="b" * 64,
        subscription_epoch_id="e" * 64,
        subscription_generation=7,
        instrument_definition_version="opra-definition-2026-08-25",
        symbol_mapping_version="opra-parent-map-v1",
        spot=6500.0,
        gamma_pin=6513.0,
        zero_gamma=6487.0,
        gross_gex=1_000_000.0,
        net_gex=250_000.0,
        top_strike_share=0.25,
        spot_return_5m=0.001,
        coherent_bid=6499.5,
        coherent_ask=6500.5,
        crossed_quote_count=0,
        fresh_quote_count=300,
        expected_quote_count=400,
        paired_quote_count=150,
        quote_age_seconds=0.5,
        source_confidence=0.8,
        expiration_days=0.0,
        validation_is_valid=True,
        validation_failure_reasons=(),
        provenance={"source_path": "data/databento_cache/redacted.csv"},
    )
    values.update(overrides)
    return PointInTimeObservation(**values)


def test_preregistered_formula_has_exact_auditable_equation_and_is_shadow_only():
    prediction = ShadowFormulaEngine().evaluate(_observation())

    # pin gap=20bps, zero gap=-20bps, momentum=10bps,
    # concentration=5bps, balance interaction=5bps.
    expected_delta_bps = 0.10 * 20 + 0.03 * -20 + 0.12 * 10 + 0.08 * 5 + 0.05 * 5
    assert prediction.abstained is False
    assert prediction.mode == "shadow"
    assert prediction.production_signal_replaced is False
    assert prediction.formula.promotion_allowed is False
    assert prediction.formula.fitted is False
    assert prediction.predicted_delta_bps == pytest.approx(expected_delta_bps)
    assert prediction.predicted_price == pytest.approx(6500 * (1 + expected_delta_bps / 10_000))
    assert prediction.confidence < 0.70
    assert prediction.confidence_kind == "data-quality heuristic; not forecast calibration"
    assert "predicted_price(t+300s)" in prediction.formula.equation
    assert prediction.target_timestamp_utc - prediction.prediction_timestamp_utc == timedelta(seconds=300)
    with pytest.raises(TypeError):
        prediction.formula.coefficients["pin_gap_bps"] = 99.0


def test_naive_baseline_is_no_change_but_keeps_same_data_guardrails():
    prediction = ShadowFormulaEngine().evaluate(_observation(), NAIVE_LAST_PRICE_V1)
    assert not prediction.abstained
    assert prediction.predicted_price == 6500.0
    assert prediction.predicted_delta_points == 0.0
    assert prediction.predicted_delta_bps == 0.0


@pytest.mark.parametrize(
    ("overrides", "expected_reason"),
    [
        ({"crossed_quote_count": 1}, "CROSSED_QUOTES_PRESENT"),
        ({"coherent_bid": 6501.0, "coherent_ask": 6500.0}, "CROSSED_COHERENT_BID_ASK"),
        ({"quote_age_seconds": 11.0}, "STALE_QUOTES"),
        ({"processed_at_utc": datetime(2026, 8, 25, 13, 30, 8, tzinfo=UTC)}, "RECEIVE_TO_PROCESSING_BACKLOG"),
        ({"fresh_quote_count": 10}, "INSUFFICIENT_FRESH_QUOTES"),
        ({"gross_gex": 10.0, "net_gex": 11.0}, "GEX_INVARIANT_FAILED"),
        ({"validation_is_valid": False}, "SOURCE_VALIDATION_FAILED"),
        ({"subscription_epoch_id": None}, "MISSING_PROVENANCE:subscription_epoch_id"),
        ({"subscription_epoch_id": "E" * 64}, "INVALID_PROVENANCE:subscription_epoch_id"),
        ({"symbol_mapping_version": None}, "MISSING_PROVENANCE:symbol_mapping_version"),
    ],
)
def test_bad_or_incomplete_inputs_fail_closed(overrides, expected_reason):
    prediction = ShadowFormulaEngine().evaluate(_observation(**overrides))
    assert prediction.abstained
    assert expected_reason in prediction.abstention_reasons
    assert prediction.predicted_price is None
    assert prediction.predicted_delta_bps is None
    assert prediction.confidence == 0.0


def test_future_feature_is_explicit_lookahead_abstention():
    observation = _observation()
    future = observation.observation_index_utc + timedelta(microseconds=1)
    asof = dict(observation.feature_asof_utc)
    asof["gamma_pin"] = future
    prediction = ShadowFormulaEngine().evaluate(replace(observation, feature_asof_utc=asof))
    assert prediction.abstained
    assert "LOOKAHEAD_FEATURE:gamma_pin" in prediction.abstention_reasons


def test_event_after_receive_is_rejected():
    observation = _observation()
    prediction = ShadowFormulaEngine().evaluate(
        replace(observation, ts_event_utc=observation.ts_recv_utc + timedelta(seconds=1))
    )
    assert prediction.abstained
    assert "TS_EVENT_AFTER_TS_RECV" in prediction.abstention_reasons


def test_missing_processing_time_is_not_silently_replaced_in_audit_record():
    prediction = ShadowFormulaEngine().evaluate(_observation(processed_at_utc=None))
    assert prediction.abstained
    assert "MISSING_OR_NAIVE_PROCESSED_AT" in prediction.abstention_reasons
    assert prediction.processed_at_utc is None
    assert prediction.as_dict()["processed_at_utc"] is None


def test_sequence_orders_by_index_and_marks_duplicate_timestamp():
    first = _observation(calculation_id="later", observation_index_utc=datetime(2026, 8, 25, 13, 31, tzinfo=UTC), processed_at_utc=datetime(2026, 8, 25, 13, 31, 0, 10_000, tzinfo=UTC), ts_recv_utc=datetime(2026, 8, 25, 13, 30, 59, 990_000, tzinfo=UTC), feature_asof_utc={name: datetime(2026, 8, 25, 13, 31, tzinfo=UTC) for name in _observation().feature_asof_utc})
    earlier = _observation(calculation_id="earlier")
    duplicate = replace(earlier, calculation_id="duplicate")

    results = ShadowFormulaEngine().evaluate_many(
        [first, earlier, duplicate], formulas=(NAIVE_LAST_PRICE_V1,)
    )
    assert [item.provenance["calculation_id"] for item in results] == ["earlier", "duplicate", "later"]
    assert not results[0].abstained
    assert results[1].abstained
    assert "DUPLICATE_SYMBOL_INDEX_TIMESTAMP" in results[1].abstention_reasons


def test_sequence_duplicate_identity_does_not_cross_process_epochs():
    first = _observation(calculation_id="epoch-one", subscription_epoch_id="d" * 64)
    restarted = replace(
        first,
        calculation_id="epoch-two",
        subscription_epoch_id="e" * 64,
    )

    results = ShadowFormulaEngine().evaluate_many(
        [first, restarted], formulas=(NAIVE_LAST_PRICE_V1,)
    )

    assert len(results) == 2
    assert all(
        "DUPLICATE_SYMBOL_INDEX_TIMESTAMP" not in result.abstention_reasons
        for result in results
    )
    assert results[0].prediction_id != results[1].prediction_id


def test_payload_adapter_never_guesses_timing_mapping_momentum_or_freshness():
    base = _observation()
    payload = {
        "symbol": "SPX",
        "provider": "databento",
        "calculation_id": "calc-002",
        "universe_sha256": "c" * 64,
        "selected_universe_sha256": "d" * 64,
        "subscription_epoch_id": "f" * 64,
        "subscription_generation": 8,
        "price": 6500,
        "gamma_pin": 6510,
        "zero_gamma": 6490,
        "gross_gex": 1000,
        "net_gex": 200,
        "top_strike_share": 0.2,
        "market_bid": 6499,
        "market_ask": 6501,
        "fresh_quote_count": 300,
        "paired_quote_count": 150,
        # This field is hard-coded to zero in today's aggregate output and must
        # not be trusted by the research adapter.
        "quote_age_seconds": 0,
        "confidence": 0.7,
        "expirations_min_days": 0,
        "validation_is_valid": True,
        "universe_provenance": {"cache_kind": "current_day"},
    }
    observation = observation_from_databento_payload(
        payload,
        ts_event_utc=base.ts_event_utc,
        ts_recv_utc=base.ts_recv_utc,
        observation_index_utc=base.observation_index_utc,
        processed_at_utc=base.processed_at_utc,
        feature_asof_utc=base.feature_asof_utc,
        spot_return_5m=0.001,
        trusted_quote_age_seconds=2.5,
        fresh_quote_count=300,
        paired_quote_count=150,
        crossed_quote_count=0,
        expected_quote_count=400,
        instrument_definition_version="definitions-v1",
        symbol_mapping_version="mapping-v1",
    )
    prediction = ShadowFormulaEngine().evaluate(observation)
    assert not prediction.abstained
    assert prediction.provenance["subscription_epoch_id"] == "f" * 64
    assert prediction.provenance["cache_kind"] == "current_day"
    assert prediction.raw_feature_snapshot["spot_return_5m"] == 0.001
    assert prediction.raw_feature_snapshot["quote_age_seconds"] == 2.5
    assert prediction.raw_feature_snapshot["coherent_bid"] is None
    assert prediction.raw_feature_snapshot["coherent_ask"] is None


def test_research_journal_is_idempotent_and_scores_only_after_horizon(tmp_path):
    prediction = ShadowFormulaEngine().evaluate(_observation())
    journal_path = tmp_path / "shadow_research.db"
    journal = ShadowPredictionJournal(journal_path)

    assert not journal_path.exists()  # construction is side-effect free
    assert journal.save(prediction) is True
    assert journal.save(prediction) is False
    row = journal.fetch(prediction.prediction_id)
    assert row["mode"] == "shadow"
    assert row["production_signal_replaced"] == 0
    assert row["realized_price"] is None
    assert len(row["record_sha256"]) == 64

    with pytest.raises(ValueError, match="precedes"):
        journal.score_outcome(
            prediction.prediction_id,
            realized_price=6502.0,
            realized_timestamp_utc=prediction.target_timestamp_utc - timedelta(microseconds=1),
        )

    score = journal.score_outcome(
        prediction.prediction_id,
        realized_price=6502.0,
        realized_timestamp_utc=prediction.target_timestamp_utc,
    )
    assert score["error_points"] == pytest.approx(prediction.predicted_price - 6502.0)
    assert math.isclose(score["absolute_error_points"], abs(score["error_points"]))
    with pytest.raises(ValueError, match="already recorded"):
        journal.score_outcome(
            prediction.prediction_id,
            realized_price=6503.0,
            realized_timestamp_utc=prediction.target_timestamp_utc + timedelta(seconds=1),
        )
    with sqlite3.connect(journal_path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM shadow_prediction_journal").fetchone()[0] == 1
