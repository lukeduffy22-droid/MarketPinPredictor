import sqlite3
import math
from datetime import date, datetime, timedelta, timezone

import pytest

from backend.closing_tape import governance, production
from backend.closing_tape.governance import (
    build_closing_tape_scorecard,
    finish_forecast_attempt,
    load_governed_promoted_close_predictions,
    start_forecast_attempt,
)
from backend.closing_tape.production import record_promoted_close_predictions
from tests.test_closing_tape_production import _predictions


UTC = timezone.utc
DAY = date(2026, 8, 25)
FEATURE_TIME = datetime(2026, 8, 25, 19, 45, tzinfo=UTC)


def _start(path, *, session_id="s1", model_version="candidate-1"):
    return start_forecast_attempt(
        path,
        trading_day=DAY,
        session_id=session_id,
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=FEATURE_TIME,
        recorded_at_utc=FEATURE_TIME,
        model_version=model_version,
        artifact_sha256="a" * 64 if model_version else None,
        source_sha256="b" * 64,
    )


def test_started_attempt_without_terminal_event_is_unresolved(tmp_path):
    database = tmp_path / "market.db"
    attempt_key = _start(database)

    scorecard = build_closing_tape_scorecard(
        database, model_version="candidate-1", minimum_scored_sessions=1
    )

    assert len(attempt_key) == 64
    assert scorecard["attempts"] == {
        "opportunities": 1,
        "predicted": 0,
        "abstained": 0,
        "unresolved": 1,
        "invalid_predicted": 0,
        "availability_rate": 0.0,
        "abstention_rate": 0.0,
        "abstention_reasons": {},
        "scope": "production batch opportunities; family and regime filters apply to metrics",
    }
    assert not scorecard["available"]
    assert scorecard["overall"]["candidate_mae"] is None


def test_failed_attempt_persists_abstention_and_is_append_only(tmp_path):
    database = tmp_path / "market.db"
    attempt_key = _start(database)
    terminal_key = finish_forecast_attempt(
        database,
        attempt_key,
        event_type="ABSTAIN",
        reasons=("INCOMPLETE_FAMILY_BATCH", "STALE_REFERENCE"),
        recorded_at_utc=FEATURE_TIME + timedelta(seconds=1),
        feature_hash="c" * 64,
    )
    assert terminal_key == finish_forecast_attempt(
        database,
        attempt_key,
        event_type="ABSTAIN",
        reasons=("INCOMPLETE_FAMILY_BATCH", "STALE_REFERENCE"),
        recorded_at_utc=FEATURE_TIME + timedelta(seconds=2),
        feature_hash="c" * 64,
    )

    scorecard = build_closing_tape_scorecard(
        database, model_version="candidate-1", minimum_scored_sessions=1
    )
    assert scorecard["attempts"]["opportunities"] == 1
    assert scorecard["attempts"]["abstained"] == 1
    assert scorecard["attempts"]["availability_rate"] == 0.0
    assert scorecard["attempts"]["abstention_reasons"] == {
        "INCOMPLETE_FAMILY_BATCH": 1,
        "STALE_REFERENCE": 1,
    }

    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE closing_tape_forecast_events SET event_type='PREDICTED'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM closing_tape_forecast_events")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "INSERT OR REPLACE INTO closing_tape_forecast_events "
                "SELECT * FROM closing_tape_forecast_events WHERE event_type='ATTEMPT_STARTED'"
            )


def test_attempt_cannot_be_created_after_its_target_close(tmp_path):
    database = tmp_path / "market.db"

    with pytest.raises(ValueError, match="must start before its target close"):
        start_forecast_attempt(
            database,
            trading_day=DAY,
            session_id="late-attempt",
            prediction_mode="tcbbo_promoted",
            decision_horizon_minutes=15,
            feature_available_at_utc=FEATURE_TIME,
            recorded_at_utc=FEATURE_TIME + timedelta(minutes=15),
            model_version="candidate-1",
            artifact_sha256="a" * 64,
            source_sha256="b" * 64,
        )

    assert not database.exists()


def test_scorecard_rejects_a_corrupted_attempt_record(tmp_path):
    database = tmp_path / "market.db"
    _start(database)
    with sqlite3.connect(database) as connection:
        connection.execute("DROP TRIGGER closing_tape_forecast_events_no_update")
        connection.execute(
            "UPDATE closing_tape_forecast_events SET prediction_mode='tampered'"
        )
    governance.initialize_governance_ledger(database)

    with pytest.raises(RuntimeError, match="forecast event record hash is invalid"):
        build_closing_tape_scorecard(
            database,
            model_version="candidate-1",
            minimum_scored_sessions=1,
        )


def test_governance_schema_rejects_a_missing_event_type_check(tmp_path):
    database = tmp_path / "missing-check.db"
    expected = """event_type TEXT NOT NULL CHECK (
        event_type IN ('ATTEMPT_STARTED','PREDICTED','ABSTAIN')
    )"""
    weakened = governance.GOVERNANCE_SCHEMA.replace(
        expected,
        "event_type TEXT NOT NULL",
    )
    assert weakened != governance.GOVERNANCE_SCHEMA
    with sqlite3.connect(database) as connection:
        connection.executescript(weakened)

    with pytest.raises(RuntimeError, match="schema is incompatible"):
        governance.initialize_governance_ledger(database)


def test_successful_attempt_is_family_complete_idempotent_and_terminal(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    predictions = _predictions(tmp_path, monkeypatch)
    feature_time = datetime.fromisoformat(predictions[0].feature_available_at_utc)
    attempt_key = start_forecast_attempt(
        database,
        trading_day=DAY,
        session_id="s1",
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=feature_time,
        recorded_at_utc=feature_time,
        model_version=predictions.runtime.version,
        artifact_sha256=predictions.runtime.artifact_sha256,
        source_sha256=predictions[0].source_sha256,
    )
    keys = record_promoted_close_predictions(
        database,
        predictions,
        runtime=predictions.runtime,
        trading_day=DAY,
        session_id="s1",
        recorded_at_utc=feature_time + timedelta(seconds=1),
    )

    first = finish_forecast_attempt(
        database,
        attempt_key,
        event_type="PREDICTED",
        prediction_keys=keys,
        recorded_at_utc=feature_time + timedelta(seconds=2),
        prediction_batch=predictions,
    )
    repeated = finish_forecast_attempt(
        database,
        attempt_key,
        event_type="PREDICTED",
        prediction_keys=reversed(keys),
        recorded_at_utc=feature_time + timedelta(minutes=30),
        prediction_batch=predictions,
    )

    assert repeated == first
    scorecard = build_closing_tape_scorecard(
        database,
        model_version=predictions.runtime.version,
        minimum_scored_sessions=1,
        project_root=tmp_path / "runtime",
    )
    assert scorecard["attempts"]["predicted"] == 1
    assert scorecard["attempts"]["availability_rate"] == 1.0
    governed_rows = load_governed_promoted_close_predictions(
        database,
        project_root=tmp_path / "runtime",
        trading_day=DAY,
    )
    assert len(governed_rows) == 5
    assert all(row["forecast_id"] == row["prediction_key"] for row in governed_rows)
    assert all(row["decision_grade"] is True for row in governed_rows)
    assert all(
        row["replay_status"] == "DETERMINISTIC_REPLAY_VERIFIED"
        and row["deterministic_replay_verified"] is True
        and row["replay_log_return_absolute_error"]
        <= row["replay_log_return_absolute_tolerance"]
        and row["replay_max_level_absolute_error"]
        <= row["replay_level_absolute_tolerance"]
        for row in governed_rows
    )
    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE closing_tape_promoted_prediction_governance "
                "SET volatility_regime='stressed'"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "DELETE FROM closing_tape_promoted_prediction_governance"
            )
    with pytest.raises(ValueError, match="conflicting terminal"):
        finish_forecast_attempt(
            database,
            attempt_key,
            event_type="ABSTAIN",
            reasons=("LATE_FAILURE",),
            recorded_at_utc=FEATURE_TIME + timedelta(seconds=3),
        )


def test_partial_prediction_batch_cannot_be_recorded_as_success(tmp_path):
    database = tmp_path / "market.db"
    attempt_key = _start(database)

    with pytest.raises(ValueError, match="every production-family"):
        finish_forecast_attempt(
            database,
            attempt_key,
            event_type="PREDICTED",
            prediction_keys=("SPX", "NDX", "RUT", "VIX"),
            recorded_at_utc=FEATURE_TIME + timedelta(seconds=1),
        )


def test_five_arbitrary_keys_cannot_masquerade_as_a_predicted_batch(tmp_path):
    database = tmp_path / "market.db"
    attempt_key = _start(database)

    with pytest.raises(ValueError, match="loader-authorized"):
        finish_forecast_attempt(
            database,
            attempt_key,
            event_type="PREDICTED",
            prediction_keys=tuple(f"fabricated-{index}" for index in range(5)),
            recorded_at_utc=FEATURE_TIME + timedelta(seconds=1),
        )


def test_model_filter_does_not_count_an_unidentified_attempt(tmp_path):
    database = tmp_path / "market.db"
    _start(database, session_id="unidentified", model_version=None)

    scorecard = build_closing_tape_scorecard(
        database, model_version="candidate-1", minimum_scored_sessions=1
    )

    assert scorecard["attempts"]["opportunities"] == 0
    assert scorecard["attempts"]["availability_rate"] is None


def test_governance_connections_are_closed_after_commit(monkeypatch, tmp_path):
    real_connect = sqlite3.connect
    opened = []

    class TrackingConnection(sqlite3.Connection):
        was_closed = False

        def close(self):
            self.was_closed = True
            return super().close()

    def tracked_connect(*args, **kwargs):
        kwargs["factory"] = TrackingConnection
        connection = real_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(governance.sqlite3, "connect", tracked_connect)

    _start(tmp_path / "market.db")

    assert opened
    assert all(connection.was_closed for connection in opened)


def test_predicted_terminal_rejects_output_that_fails_deterministic_replay(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    issued = _predictions(tmp_path, monkeypatch)
    changed = list(issued)
    index = next(i for i, item in enumerate(changed) if item.family_root == "SPX")
    original = changed[index]
    changed_return = original.predicted_log_return + 1e-3
    radius = original.interval_radius_log_return
    changed[index] = production.PromotedPrediction(
        **{
            **original.to_dict(),
            "predicted_log_return": changed_return,
            "predicted_level": original.reference_price * math.exp(changed_return),
            "prediction_lower": original.reference_price
            * math.exp(changed_return - radius),
            "prediction_upper": original.reference_price
            * math.exp(changed_return + radius),
        }
    )
    mismatched = production._build_promoted_prediction_batch(
        tuple(changed),
        issued.runtime,
        feature_hash=issued.feature_hash,
        model_feature_rows_by_family=issued.model_feature_rows_by_family,
        volatility_regime_by_family=issued.volatility_regime_by_family,
    )
    feature_time = datetime.fromisoformat(mismatched[0].feature_available_at_utc)
    attempt_key = start_forecast_attempt(
        database,
        trading_day=DAY,
        session_id="replay-mismatch",
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=feature_time,
        recorded_at_utc=feature_time,
        model_version=mismatched.runtime.version,
        artifact_sha256=mismatched.runtime.artifact_sha256,
        source_sha256=mismatched[0].source_sha256,
    )
    keys = record_promoted_close_predictions(
        database,
        mismatched,
        runtime=mismatched.runtime,
        trading_day=DAY,
        session_id="replay-mismatch",
        recorded_at_utc=feature_time + timedelta(seconds=1),
    )

    with pytest.raises(ValueError, match="deterministic replay mismatch for SPX"):
        finish_forecast_attempt(
            database,
            attempt_key,
            event_type="PREDICTED",
            prediction_keys=keys,
            prediction_batch=mismatched,
            recorded_at_utc=feature_time + timedelta(seconds=2),
        )

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM closing_tape_promoted_prediction_governance"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT COUNT(*) FROM closing_tape_forecast_events "
            "WHERE event_type='PREDICTED'"
        ).fetchone()[0] == 0


def test_predicted_terminal_rejects_tampered_retained_feature_rows(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    issued = _predictions(tmp_path, monkeypatch)
    feature_time = datetime.fromisoformat(issued[0].feature_available_at_utc)
    attempt_key = start_forecast_attempt(
        database,
        trading_day=DAY,
        session_id="feature-tamper",
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=feature_time,
        recorded_at_utc=feature_time,
        model_version=issued.runtime.version,
        artifact_sha256=issued.runtime.artifact_sha256,
        source_sha256=issued[0].source_sha256,
    )
    keys = record_promoted_close_predictions(
        database,
        issued,
        runtime=issued.runtime,
        trading_day=DAY,
        session_id="feature-tamper",
        recorded_at_utc=feature_time + timedelta(seconds=1),
    )
    tampered = list(issued.model_feature_rows_by_family)
    family, values = tampered[0]
    tampered[0] = (family, (values[0] + 1.0, *values[1:]))
    object.__setattr__(issued, "model_feature_rows_by_family", tuple(tampered))

    with pytest.raises(ValueError, match="do not match feature hash"):
        finish_forecast_attempt(
            database,
            attempt_key,
            event_type="PREDICTED",
            prediction_keys=keys,
            prediction_batch=issued,
            recorded_at_utc=feature_time + timedelta(seconds=2),
        )
