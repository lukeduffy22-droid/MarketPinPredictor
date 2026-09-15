import hashlib
import math
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest
import pandas as pd

from backend.closing_tape import governance
from backend.closing_tape.dataset import PRODUCTION_FAMILIES
from backend.closing_tape.governance import (
    build_closing_tape_scorecard,
    finish_forecast_attempt,
    initialize_governance_ledger,
    score_promoted_prediction_outcomes,
    start_forecast_attempt,
)
from backend.closing_tape.production import (
    PROMOTED_PREDICTION_COLUMNS,
    PROMOTED_PREDICTION_SCHEMA,
    predict_promoted_close,
    record_promoted_close_predictions,
)
from tests.test_closing_tape_production import _five_family_surface, _runtime


UTC = timezone.utc
MODEL_VERSION = "scorecard-candidate-1"
ARTIFACT_SHA256 = "a" * 64
CALIBRATION_SHA256 = "b" * 64
SOURCE_SHA256 = "c" * 64
CLOSE_ARTIFACT_SHA256 = "d" * 64


def _initialize_database(path):
    with sqlite3.connect(path) as connection:
        connection.executescript(PROMOTED_PREDICTION_SCHEMA)
        connection.executescript(
            """
            CREATE TABLE eod_close_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                observation_key TEXT UNIQUE NOT NULL,
                symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL,
                official_close REAL NOT NULL,
                source TEXT NOT NULL,
                source_reference TEXT,
                source_artifact_sha256 TEXT,
                source_verified INTEGER NOT NULL,
                observed_at_utc TEXT NOT NULL,
                ingested_at_utc TEXT NOT NULL,
                correction_of_id INTEGER
            );
            """
        )


def _insert_prediction(
    path,
    *,
    trading_day,
    reference_price,
    predicted_level,
    lower,
    upper,
    artifact_sha256=ARTIFACT_SHA256,
):
    feature_time = datetime.combine(
        trading_day, datetime.min.time(), tzinfo=UTC
    ) + timedelta(hours=19, minutes=45)
    prediction_key = f"SPX-{trading_day.isoformat()}-{artifact_sha256[:8]}"
    values = {
        "prediction_key": prediction_key,
        "trading_date": trading_day.isoformat(),
        "session_id": f"session-{trading_day.isoformat()}",
        "family_root": "SPX",
        "decision_horizon_minutes": 15,
        "feature_available_at_utc": feature_time.isoformat(),
        "recorded_at_utc": (feature_time + timedelta(seconds=1)).isoformat(),
        "reference_price": reference_price,
        "predicted_log_return": math.log(predicted_level / reference_price),
        "predicted_level": predicted_level,
        "prediction_lower": lower,
        "prediction_upper": upper,
        "interval_radius_log_return": 0.02,
        "interval_alpha": 0.1,
        "interval_target_coverage": 0.9,
        "calibration_method": "frozen-oos-absolute-log-residual-conformal-v1",
        "calibration_evidence_sha256": CALIBRATION_SHA256,
        "source_sha256": SOURCE_SHA256,
        "model_version": MODEL_VERSION,
        "artifact_sha256": artifact_sha256,
        "execution_device": "cpu",
        "prediction_mode": "tcbbo_promoted",
        "is_estimate": 1,
    }
    with sqlite3.connect(path) as connection:
        connection.execute(
            f"INSERT INTO promoted_close_predictions "
            f"({','.join(PROMOTED_PREDICTION_COLUMNS)}) "
            f"VALUES ({','.join('?' for _ in PROMOTED_PREDICTION_COLUMNS)})",
            tuple(values[column] for column in PROMOTED_PREDICTION_COLUMNS),
        )
    return prediction_key, feature_time


def _insert_close(
    path,
    *,
    trading_day,
    official_close,
    source_verified=True,
    observation_suffix="original",
    observed_offset_minutes=0,
    correction_of_id=None,
    observed_at_utc=None,
):
    observed_at = observed_at_utc or (
        datetime.combine(trading_day, datetime.min.time(), tzinfo=UTC)
        + timedelta(hours=22, minutes=observed_offset_minutes)
    )
    artifact_bytes = (
        f"SPX|{trading_day.isoformat()}|{official_close}|{observation_suffix}"
    ).encode("utf-8")
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    artifact_path = (
        path.parent
        / "runtime"
        / "data"
        / "verified_close_sources"
        / trading_day.isoformat()
        / "SPX"
        / f"{artifact_sha256}.txt"
    )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(artifact_bytes)
    with sqlite3.connect(path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO eod_close_observations (
                observation_key, symbol, trading_date, official_close, source,
                source_reference, source_artifact_sha256, source_verified,
                observed_at_utc, ingested_at_utc, correction_of_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                f"SPX-{trading_day.isoformat()}-{observation_suffix}",
                "SPX",
                trading_day.isoformat(),
                official_close,
                "sp-global-official",
                f"https://www.spglobal.com/spdji/en/id-{trading_day.isoformat()}",
                artifact_sha256,
                int(source_verified),
                observed_at.isoformat(),
                observed_at.isoformat(),
                correction_of_id,
            ),
        )
        return int(cursor.lastrowid)


def _record_attempt(path, *, trading_day, feature_time, state, reason=None):
    assert state == "ABSTAIN"
    attempt_key = start_forecast_attempt(
        path,
        trading_day=trading_day,
        session_id=f"attempt-{trading_day.isoformat()}-{state}",
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=feature_time,
        recorded_at_utc=feature_time,
        model_version=MODEL_VERSION,
        artifact_sha256=ARTIFACT_SHA256,
        source_sha256=SOURCE_SHA256,
    )
    finish_forecast_attempt(
        path,
        attempt_key,
        event_type=state,
        reasons=(reason,) if reason else (),
        recorded_at_utc=feature_time + timedelta(seconds=2),
    )


def _insert_forged_score(path, **changes):
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        original = connection.execute(
            "SELECT * FROM promoted_prediction_accuracy_observations "
            "ORDER BY close_observation_id LIMIT 1"
        ).fetchone()
        assert original is not None
        payload = dict(original)
        payload.update(changes)
        payload["score_key"] = __import__("hashlib").sha256(
            f"{payload['prediction_key']}:{payload['close_observation_id']}".encode(
                "utf-8"
            )
        ).hexdigest()
        payload["record_sha256"] = governance._hash(
            {
                column: payload[column]
                for column in governance.PROMOTED_SCORE_COLUMNS
                if column != "record_sha256"
            }
        )
        connection.execute(
            "INSERT INTO promoted_prediction_accuracy_observations "
            f"({','.join(governance.PROMOTED_SCORE_COLUMNS)}) "
            f"VALUES ({','.join('?' for _ in governance.PROMOTED_SCORE_COLUMNS)})",
            tuple(payload[column] for column in governance.PROMOTED_SCORE_COLUMNS),
        )


def _issue_governed_batch(
    path,
    *,
    tmp_path,
    monkeypatch,
    trading_day,
    reference_price,
    model_version=MODEL_VERSION,
):
    runtime = _runtime(tmp_path, monkeypatch, model_version)
    surface = _five_family_surface().copy()
    shift = pd.Timedelta(days=(trading_day - date(2026, 8, 25)).days)
    for field in ("feature_available_at_utc", "minute_utc", "cash_close_utc"):
        surface[field] = pd.to_datetime(surface[field], utc=True) + shift
    surface["trading_date"] = trading_day.isoformat()
    surface["session_id"] = f"session-{trading_day.isoformat()}-{model_version}"
    surface.loc[surface["family_root"] == "SPX", "reference_price"] = reference_price
    surface["volatility_regime"] = "normal"
    batch = predict_promoted_close(surface, runtime)
    feature_time = datetime.fromisoformat(batch[0].feature_available_at_utc)
    attempt_key = start_forecast_attempt(
        path,
        trading_day=trading_day,
        session_id=f"session-{trading_day.isoformat()}-{model_version}",
        prediction_mode="tcbbo_promoted",
        decision_horizon_minutes=15,
        feature_available_at_utc=feature_time,
        recorded_at_utc=feature_time,
        model_version=runtime.version,
        artifact_sha256=runtime.artifact_sha256,
        source_sha256=batch[0].source_sha256,
    )
    keys = record_promoted_close_predictions(
        path,
        batch,
        runtime=runtime,
        trading_day=trading_day,
        session_id=f"session-{trading_day.isoformat()}-{model_version}",
        recorded_at_utc=feature_time + timedelta(seconds=1),
    )
    finish_forecast_attempt(
        path,
        attempt_key,
        event_type="PREDICTED",
        prediction_keys=keys,
        recorded_at_utc=feature_time + timedelta(seconds=2),
        prediction_batch=batch,
    )
    spx_key = next(
        key for key, item in zip(keys, sorted(batch, key=lambda value: value.family_root))
        if item.family_root == "SPX"
    )
    spx_prediction = next(item for item in batch if item.family_root == "SPX")
    return spx_key, feature_time, spx_prediction


def test_scorecard_compares_persistence_covers_inclusive_intervals_and_counts_abstentions(
    tmp_path,
    monkeypatch,
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    first_day = date(2026, 8, 24)
    second_day = date(2026, 8, 25)
    _, first_feature, first_prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=first_day,
        reference_price=95.0,
    )
    _, second_feature, second_prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=second_day,
        reference_price=105.0,
    )
    first_actual = first_prediction.predicted_level
    second_actual = second_prediction.prediction_upper
    _insert_close(database, trading_day=first_day, official_close=first_actual)
    _insert_close(database, trading_day=second_day, official_close=second_actual)
    abstain_day = date(2026, 8, 26)
    _record_attempt(
        database,
        trading_day=abstain_day,
        feature_time=datetime(2026, 8, 26, 19, 45, tzinfo=UTC),
        state="ABSTAIN",
        reason="STALE_REFERENCE",
    )

    assert score_promoted_prediction_outcomes(
        database, trading_day=first_day, project_root=tmp_path / "runtime"
    )["inserted"] == 1
    assert score_promoted_prediction_outcomes(
        database, trading_day=second_day, project_root=tmp_path / "runtime"
    )["inserted"] == 1
    scorecard = build_closing_tape_scorecard(
        database,
        model_version=MODEL_VERSION,
        family_root="SPX",
        decision_horizon_minutes=15,
        minimum_scored_sessions=2,
        project_root=tmp_path / "runtime",
    )

    assert scorecard["available"]
    assert scorecard["attempts"]["opportunities"] == 3
    assert scorecard["attempts"]["predicted"] == 2
    assert scorecard["attempts"]["abstained"] == 1
    assert scorecard["attempts"]["availability_rate"] == pytest.approx(2 / 3)
    assert scorecard["attempts"]["abstention_reasons"] == {"STALE_REFERENCE": 1}
    assert scorecard["overall"]["scored_rows"] == 2
    expected_candidate_mae = (
        abs(first_prediction.predicted_level - first_actual)
        + abs(second_prediction.predicted_level - second_actual)
    ) / 2
    expected_persistence_mae = (
        abs(first_prediction.reference_price - first_actual)
        + abs(second_prediction.reference_price - second_actual)
    ) / 2
    assert scorecard["overall"]["candidate_mae"] == pytest.approx(
        expected_candidate_mae
    )
    assert scorecard["overall"]["persistence_mae"] == pytest.approx(
        expected_persistence_mae
    )
    assert scorecard["overall"]["candidate_mae_improvement_pct"] == pytest.approx(
        (expected_persistence_mae - expected_candidate_mae)
        / expected_persistence_mae
        * 100.0
    )
    assert scorecard["overall"]["candidate_direction_hit_rate"] == 1.0
    assert scorecard["overall"]["persistence_direction_hit_rate"] == 0.0
    assert scorecard["overall"]["interval_target_coverage"] == 0.9
    assert scorecard["overall"]["interval_empirical_coverage"] == 1.0
    assert scorecard["overall"]["interval_coverage_error"] == pytest.approx(0.1)
    assert scorecard["outcome_resolution"] == {
        "eligible_forecasts": 2,
        "resolved_forecasts": 2,
        "unresolved_forecasts": 0,
        "resolution_rate": 1.0,
    }
    assert next(
        row for row in scorecard["by_regime"] if row["volatility_regime"] == "normal"
    )["candidate_mae"] == pytest.approx(expected_candidate_mae)


def test_scorecard_returns_null_metrics_below_minimum_evidence(tmp_path, monkeypatch):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _, _, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    score_promoted_prediction_outcomes(
        database, trading_day=trading_day, project_root=tmp_path / "runtime"
    )

    scorecard = build_closing_tape_scorecard(
        database,
        model_version=MODEL_VERSION,
        family_root="SPX",
        minimum_scored_sessions=2,
        project_root=tmp_path / "runtime",
    )

    assert not scorecard["available"]
    assert scorecard["overall"]["scored_sessions"] == 1
    assert scorecard["overall"]["candidate_mae"] is None
    assert scorecard["overall"]["persistence_mae"] is None
    assert scorecard["overall"]["interval_empirical_coverage"] is None


@pytest.mark.parametrize(
    ("observed_at_utc", "reason"),
    [
        (
            datetime(2026, 8, 25, 19, 59, tzinfo=UTC),
            "official cash-session close",
        ),
        (
            datetime(2099, 8, 25, 21, 1, tzinfo=UTC),
            "future clock skew",
        ),
    ],
)
def test_scoring_revalidates_verified_close_chronology_from_raw_ledger(
    tmp_path, monkeypatch, observed_at_utc, reason
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _key, _feature_time, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
        observed_at_utc=observed_at_utc,
    )

    with pytest.raises(RuntimeError, match=reason):
        score_promoted_prediction_outcomes(
            database,
            trading_day=trading_day,
            project_root=tmp_path / "runtime",
        )


def test_unverified_close_cannot_enter_promoted_score_ledger(tmp_path, monkeypatch):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _, _, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
        source_verified=False,
    )

    result = score_promoted_prediction_outcomes(
        database, trading_day=trading_day, project_root=tmp_path / "runtime"
    )

    assert result["scored"] == 0
    assert result["reason"] == "prediction_batch_not_scoreable"
    assert set(result["ineligible_reasons"].values()) == {"missing_verified_close"}
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM promoted_prediction_accuracy_observations"
        ).fetchone()[0] == 0


def test_close_correction_appends_new_score_and_scorecard_uses_latest_evidence(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _, _, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=95.0,
    )
    original_id = _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    first = score_promoted_prediction_outcomes(
        database, trading_day=trading_day, project_root=tmp_path / "runtime"
    )
    correction_id = _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level + 1.0,
        observation_suffix="correction",
        observed_offset_minutes=5,
        correction_of_id=original_id,
    )
    second = score_promoted_prediction_outcomes(
        database, trading_day=trading_day, project_root=tmp_path / "runtime"
    )

    assert first["inserted"] == 1
    assert second["inserted"] == 1
    assert second["close_observation_ids"] == [correction_id]
    scorecard = build_closing_tape_scorecard(
        database,
        model_version=MODEL_VERSION,
        family_root="SPX",
        minimum_scored_sessions=1,
        project_root=tmp_path / "runtime",
    )
    assert scorecard["overall"]["candidate_mae"] == pytest.approx(1.0)
    assert scorecard["overall"]["persistence_mae"] == pytest.approx(
        abs(prediction.predicted_level + 1.0 - prediction.reference_price)
    )
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM promoted_prediction_accuracy_observations"
        ).fetchone()[0] == 2
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE promoted_prediction_accuracy_observations SET actual_close=0"
            )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM promoted_prediction_accuracy_observations")
        promoted_columns = tuple(
            row[1]
            for row in connection.execute("PRAGMA table_info(promoted_close_predictions)")
        )
    assert promoted_columns == PROMOTED_PREDICTION_COLUMNS


@pytest.mark.parametrize(
    ("corruption", "reason"),
    [
        ("unlinked", "exactly one causal root"),
        ("missing-parent", "missing, mismatched, or non-causal"),
        ("fork", "lineage forks"),
    ],
)
def test_scoring_rejects_corrupted_verified_close_correction_graphs(
    tmp_path, monkeypatch, corruption, reason
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _key, _feature_time, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    root_id = _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    if corruption == "unlinked":
        _insert_close(
            database,
            trading_day=trading_day,
            official_close=prediction.predicted_level + 1.0,
            observation_suffix="unlinked",
        )
    elif corruption == "missing-parent":
        _insert_close(
            database,
            trading_day=trading_day,
            official_close=prediction.predicted_level + 1.0,
            observation_suffix="missing-parent",
            correction_of_id=999,
        )
    else:
        _insert_close(
            database,
            trading_day=trading_day,
            official_close=prediction.predicted_level + 1.0,
            observation_suffix="first-correction",
            correction_of_id=root_id,
        )
        _insert_close(
            database,
            trading_day=trading_day,
            official_close=prediction.predicted_level + 2.0,
            observation_suffix="forked-correction",
            correction_of_id=root_id,
        )

    with pytest.raises(RuntimeError, match=reason):
        score_promoted_prediction_outcomes(
            database,
            trading_day=trading_day,
            project_root=tmp_path / "runtime",
        )


def test_scorecard_suppresses_a_score_superseded_by_an_unscored_correction(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _key, _feature_time, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=95.0,
    )
    original_id = _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    score_promoted_prediction_outcomes(
        database,
        trading_day=trading_day,
        project_root=tmp_path / "runtime",
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level + 1.0,
        observation_suffix="unscored-correction",
        observed_offset_minutes=5,
        correction_of_id=original_id,
    )

    scorecard = build_closing_tape_scorecard(
        database,
        model_version=MODEL_VERSION,
        family_root="SPX",
        minimum_scored_sessions=1,
        project_root=tmp_path / "runtime",
    )

    assert scorecard["available"] is False
    assert scorecard["overall"]["scored_rows"] == 0
    assert scorecard["overall"]["candidate_mae"] is None
    assert scorecard["outcome_resolution"] == {
        "eligible_forecasts": 1,
        "resolved_forecasts": 0,
        "unresolved_forecasts": 1,
        "resolution_rate": 0.0,
    }


def test_scorecard_rejects_self_hashed_score_for_nonexistent_close(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _key, _feature_time, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    score_promoted_prediction_outcomes(
        database,
        trading_day=trading_day,
        project_root=tmp_path / "runtime",
    )
    _insert_forged_score(database, close_observation_id=999_999)

    with pytest.raises(RuntimeError, match="missing verified close observation"):
        build_closing_tape_scorecard(
            database,
            family_root="SPX",
            minimum_scored_sessions=1,
            project_root=tmp_path / "runtime",
        )


def test_scorecard_rejects_score_misbound_to_another_verified_symbol(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    _initialize_database(database)
    trading_day = date(2026, 8, 25)
    _key, _feature_time, prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=trading_day,
        reference_price=100.0,
    )
    _insert_close(
        database,
        trading_day=trading_day,
        official_close=prediction.predicted_level,
    )
    score_promoted_prediction_outcomes(
        database,
        trading_day=trading_day,
        project_root=tmp_path / "runtime",
    )
    observed_at = datetime(2026, 8, 25, 22, 5, tzinfo=UTC).isoformat()
    with sqlite3.connect(database) as connection:
        other_close_id = connection.execute(
            """
            INSERT INTO eod_close_observations (
                observation_key, symbol, trading_date, official_close, source,
                source_reference, source_artifact_sha256, source_verified,
                observed_at_utc, ingested_at_utc, correction_of_id
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                "NDX-2026-08-25-other",
                "NDX",
                trading_day.isoformat(),
                24_000.5,
                "nasdaq-official",
                "https://www.nasdaq.com/market-activity/index/ndx",
                "e" * 64,
                1,
                observed_at,
                observed_at,
                None,
            ),
        ).lastrowid
    _insert_forged_score(database, close_observation_id=int(other_close_id))

    with pytest.raises(RuntimeError, match="close identity conflicts"):
        build_closing_tape_scorecard(
            database,
            family_root="SPX",
            minimum_scored_sessions=1,
            project_root=tmp_path / "runtime",
        )


def test_governance_schema_requires_keys_uniqueness_not_null_and_guards(tmp_path):
    columns = ",".join(f"{column} TEXT" for column in governance.FORECAST_EVENT_COLUMNS)
    initializer_database = tmp_path / "initializer.db"
    with sqlite3.connect(initializer_database) as connection:
        connection.execute(f"CREATE TABLE closing_tape_forecast_events ({columns})")

    with pytest.raises(RuntimeError, match="schema is incompatible"):
        initialize_governance_ledger(initializer_database)

    reader_database = tmp_path / "reader.db"
    with sqlite3.connect(reader_database) as connection:
        connection.execute(f"CREATE TABLE closing_tape_forecast_events ({columns})")

    with pytest.raises(RuntimeError, match="schema is incompatible"):
        build_closing_tape_scorecard(reader_database, minimum_scored_sessions=1)


def test_scorecard_refuses_to_aggregate_mixed_artifact_identity(tmp_path, monkeypatch):
    database = tmp_path / "market.db"
    _initialize_database(database)
    first_day = date(2026, 8, 24)
    second_day = date(2026, 8, 25)
    _, _, first_prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=first_day,
        reference_price=100.0,
    )
    _, _, second_prediction = _issue_governed_batch(
        database,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        trading_day=second_day,
        reference_price=100.0,
        model_version="scorecard-candidate-2",
    )
    _insert_close(
        database,
        trading_day=first_day,
        official_close=first_prediction.predicted_level,
    )
    _insert_close(
        database,
        trading_day=second_day,
        official_close=second_prediction.predicted_level,
    )
    score_promoted_prediction_outcomes(
        database, trading_day=first_day, project_root=tmp_path / "runtime"
    )
    score_promoted_prediction_outcomes(
        database, trading_day=second_day, project_root=tmp_path / "runtime"
    )

    scorecard = build_closing_tape_scorecard(
        database,
        family_root="SPX",
        minimum_scored_sessions=2,
        project_root=tmp_path / "runtime",
    )

    assert not scorecard["available"]
    assert "mixes model, artifact, or calibration identity" in scorecard["reason"]
    assert scorecard["overall"]["candidate_mae"] is None
