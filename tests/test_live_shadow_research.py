import sqlite3
from datetime import datetime, timedelta, timezone

from backend.research.live_shadow import LiveShadowResearchRecorder
from app.services.shadow_research_view import load_shadow_research_state


UTC = timezone.utc


def _payload(index_at: datetime, *, calculation_id: str, spot: float = 100.0) -> dict:
    return {
        "symbol": "SPX",
        "provider": "databento",
        "calculation_id": calculation_id,
        "price": spot,
        "gamma_pin": 101.0,
        "zero_gamma": 99.0,
        "gross_gex": 1000.0,
        "net_gex": 250.0,
        "top_strike_share": 0.15,
        "confidence": 0.8,
        "expirations_min_days": 0,
        "validation_is_valid": True,
        "validation_failure_reasons": [],
        "universe_sha256": "definition-hash",
        "selected_universe_sha256": "selected-hash",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 7,
        "instrument_definition_version": "universe-sha256:definition-hash",
        "symbol_mapping_version": "mapping-set-hash",
        "mapping_version_missing_count": 0,
        "latest_ts_event_utc": (index_at - timedelta(seconds=1)).isoformat(),
        "latest_ts_recv_utc": (index_at - timedelta(milliseconds=10)).isoformat(),
        "observation_index_utc": index_at.isoformat(),
        "quote_age_max_seconds": 2.0,
        "fresh_quote_count": 80,
        "expected_quote_count": 100,
        "paired_quote_count": 40,
        "primary_pair_coverage_ratio": 1.0,
        "processing_clock_telemetry": {"status": "synchronized"},
        "contributing_crossed_quote_count": 0,
        "provider_timestamp_order_errors": 0,
        "universe_provenance": {
            "mode": "unit-test",
            "subscription_profile": "near-term-shadow",
        },
    }


def test_live_shadow_records_baseline_candidate_and_matured_outcome(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=True)
    first_index = datetime(2026, 8, 25, 14, 30, tzinfo=UTC)

    first = recorder.record(
        _payload(first_index, calculation_id="first", spot=100.0),
        processed_at_utc=first_index + timedelta(milliseconds=50),
    )

    assert first.error is None
    assert first.saved_count == 2
    by_formula = {prediction.formula.formula_id: prediction for prediction in first.predictions}
    assert by_formula["shadow-naive-last-price"].abstained is False
    assert by_formula["shadow-pin-context-linear"].abstained is True
    assert "MISSING_OR_NONFINITE_FEATURE:spot_return_5m" in by_formula[
        "shadow-pin-context-linear"
    ].abstention_reasons

    second_index = first_index + timedelta(seconds=301)
    second = recorder.record(
        _payload(second_index, calculation_id="second", spot=101.0),
        processed_at_utc=second_index + timedelta(milliseconds=50),
    )

    assert second.error is None
    assert second.saved_count == 2
    assert second.scored_count == 1
    candidate = next(
        prediction
        for prediction in second.predictions
        if prediction.formula.formula_id == "shadow-pin-context-linear"
    )
    assert candidate.abstained is False
    assert candidate.mode == "shadow"
    assert candidate.production_signal_replaced is False
    assert candidate.predicted_price is not None

    state = load_shadow_research_state(path)
    assert state["summary"]["journal_rows"] == 4
    assert state["summary"]["eligible_forecasts"] == 3
    assert state["summary"]["abstentions"] == 1
    assert state["summary"]["scored_outcomes"] == 1
    assert state["summary"]["production_replacements"] == 0


def test_live_shadow_fails_closed_on_negative_receive_lag(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=True)
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)

    result = recorder.record(
        _payload(index_at, calculation_id="clock-skew"),
        processed_at_utc=index_at - timedelta(milliseconds=200),
    )

    assert result.saved_count == 2
    assert all(prediction.abstained for prediction in result.predictions)
    assert all(
        "NEGATIVE_RECEIVE_TO_PROCESSING_LAG" in prediction.abstention_reasons
        for prediction in result.predictions
    )
    assert all(prediction.predicted_price is None for prediction in result.predictions)


def test_live_shadow_fails_closed_on_low_primary_pair_coverage(tmp_path):
    recorder = LiveShadowResearchRecorder(journal_path=tmp_path / "shadow.db", enabled=True)
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    payload = _payload(index_at, calculation_id="thin-primary-pairs")
    payload["primary_pair_coverage_ratio"] = 0.05

    result = recorder.record(
        payload,
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert all(prediction.abstained for prediction in result.predictions)
    assert all(
        any(reason.startswith("INSUFFICIENT_PRIMARY_PAIR_COVERAGE") for reason in prediction.abstention_reasons)
        for prediction in result.predictions
    )


def test_live_shadow_fails_closed_when_processing_clock_is_unsynchronized(tmp_path):
    recorder = LiveShadowResearchRecorder(journal_path=tmp_path / "shadow.db", enabled=True)
    index_at = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    payload = _payload(index_at, calculation_id="unsynchronized-clock")
    payload["processing_clock_telemetry"] = {"status": "unsynchronized"}

    result = recorder.record(
        payload,
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert all(prediction.abstained for prediction in result.predictions)
    assert all(
        "PROCESSING_CLOCK_UNSYNCHRONIZED" in prediction.abstention_reasons
        for prediction in result.predictions
    )


def test_contested_pin_abstains_pin_formula_but_preserves_naive_baseline(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=True)
    first_index = datetime(2026, 8, 25, 15, 0, tzinfo=UTC)
    recorder.record(
        _payload(first_index, calculation_id="warmup", spot=100.0),
        processed_at_utc=first_index + timedelta(milliseconds=50),
    )
    payload = _payload(
        first_index + timedelta(seconds=301),
        calculation_id="contested",
        spot=101.0,
    )
    payload.update({
        "pin_is_contested": True,
        "pin_competition_reason": "PIN_CONTESTED: 101 leads 102 by 2.0%",
    })

    result = recorder.record(
        payload,
        processed_at_utc=first_index + timedelta(seconds=301, milliseconds=50),
    )

    by_formula = {prediction.formula.formula_id: prediction for prediction in result.predictions}
    assert by_formula["shadow-naive-last-price"].abstained is False
    assert by_formula["shadow-pin-context-linear"].abstained is True
    assert any(
        reason.startswith("PIN_CONTESTED")
        for reason in by_formula["shadow-pin-context-linear"].abstention_reasons
    )


def test_duplicate_symbol_index_is_preserved_as_abstention(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=True)
    index_at = datetime(2026, 8, 25, 15, 10, tzinfo=UTC)
    payload = _payload(index_at, calculation_id="duplicate")
    recorder.record(payload, processed_at_utc=index_at + timedelta(milliseconds=50))

    duplicate = recorder.record(
        payload,
        processed_at_utc=index_at + timedelta(seconds=1),
    )

    assert duplicate.saved_count == 2
    assert all(prediction.abstained for prediction in duplicate.predictions)
    assert all(
        "DUPLICATE_SYMBOL_INDEX_TIMESTAMP" in prediction.abstention_reasons
        for prediction in duplicate.predictions
    )
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM shadow_prediction_journal").fetchone()[0] == 4


def test_process_restart_resets_momentum_duplicate_and_outcome_alignment(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=True)
    first_index = datetime(2026, 8, 25, 15, 10, tzinfo=UTC)
    first_payload = _payload(first_index, calculation_id="old-process", spot=100.0)
    recorder.record(
        first_payload,
        processed_at_utc=first_index + timedelta(milliseconds=50),
    )

    same_index_new_process = {
        **first_payload,
        "calculation_id": "new-process-same-index",
        "subscription_epoch_id": "f" * 64,
    }
    same_index = recorder.record(
        same_index_new_process,
        processed_at_utc=first_index + timedelta(seconds=1),
    )
    assert same_index.saved_count == 2
    assert all(
        "DUPLICATE_SYMBOL_INDEX_TIMESTAMP" not in prediction.abstention_reasons
        for prediction in same_index.predictions
    )

    later_index = first_index + timedelta(seconds=301)
    restarted = recorder.record(
        _payload(later_index, calculation_id="new-process-later", spot=101.0)
        | {"subscription_epoch_id": "a" * 64},
        processed_at_utc=later_index + timedelta(milliseconds=50),
    )

    assert restarted.scored_count == 0
    candidate = next(
        prediction
        for prediction in restarted.predictions
        if prediction.formula.formula_id == "shadow-pin-context-linear"
    )
    assert candidate.abstained is True
    assert "MISSING_OR_NONFINITE_FEATURE:spot_return_5m" in candidate.abstention_reasons
    with sqlite3.connect(path) as connection:
        old_scored = connection.execute(
            "SELECT COUNT(*) FROM shadow_prediction_journal "
            "WHERE json_extract(provenance_json, '$.subscription_epoch_id') = ? "
            "AND realized_price IS NOT NULL",
            ("e" * 64,),
        ).fetchone()[0]
    assert old_scored == 0


def test_research_write_failure_never_escapes_to_production_callback(tmp_path, monkeypatch):
    recorder = LiveShadowResearchRecorder(journal_path=tmp_path / "shadow.db", enabled=True)
    index_at = datetime(2026, 8, 25, 15, 20, tzinfo=UTC)

    monkeypatch.setattr(recorder.journal, "save", lambda _prediction: (_ for _ in ()).throw(OSError("disk")))
    result = recorder.record(
        _payload(index_at, calculation_id="write-failure"),
        processed_at_utc=index_at + timedelta(milliseconds=50),
    )

    assert result.error == "OSError: disk"
    assert result.saved_count == 0


def test_disabled_shadow_research_does_not_create_database(tmp_path):
    path = tmp_path / "shadow.db"
    recorder = LiveShadowResearchRecorder(journal_path=path, enabled=False)

    result = recorder.record(
        _payload(datetime(2026, 8, 25, 15, 30, tzinfo=UTC), calculation_id="disabled")
    )

    assert result.skipped_reason == "SHADOW_RESEARCH_DISABLED"
    assert not path.exists()


def test_read_only_shadow_view_does_not_create_missing_database(tmp_path):
    path = tmp_path / "missing.db"

    state = load_shadow_research_state(path)

    assert state["present"] is False
    assert state["rows"] == []
    assert not path.exists()
