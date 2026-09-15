import hashlib
import json
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from backend.closing_tape import production
from backend.closing_tape.production import (
    PromotedModelRuntime,
    load_promoted_close_predictions,
    load_promoted_model_runtime,
    predict_promoted_close,
    record_promoted_close_predictions,
    validate_promoted_close_prediction_batch,
)
from backend.closing_tape.promotion import record_promotion_decision
from backend.closing_tape.surface import build_contract_surface_features
from tests.test_closing_tape_model_artifact import _payload
from tests.test_closing_tape_surface import _rows


FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")
UTC = timezone.utc


def _runtime(tmp_path, monkeypatch, version="v1"):
    root = tmp_path / "runtime"
    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)
    artifact_text = json.dumps(_payload(), sort_keys=True, separators=(",", ":"))
    artifact_path = models / "artifact.json"
    artifact_path.write_text(artifact_text, encoding="utf-8")
    artifact_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    manifest = {
        "version": version,
        "evidence_contract_version": "test-evidence-v1",
        "feature_schema_hash": _payload()["feature_contract_hash"],
        "artifact_path": artifact_path.name,
        "artifact_sha256": artifact_hash,
        "execution_device": "cpu",
        "inference_batch_rows": 5,
        "deployment_calibration": {
            "method": "family_absolute_residual_conformal_v1",
            "evidence_sha256": "c" * 64,
            "model_version": version,
            "artifact_sha256": artifact_hash,
            "alpha": 0.1,
            "target_coverage": 0.9,
            "asof_utc": "2026-08-24T20:00:00+00:00",
            "family_radii": [
                {"family_root": family, "radius_log_return": 0.01}
                for family in FAMILIES
            ],
        },
    }
    approval = record_promotion_decision(
        root,
        manifest,
        approved_by="test-risk-owner",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 24, 21, 0, tzinfo=UTC),
    )
    manifest["promotion_approval"] = approval
    (models / "closing_tape_model.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    monkeypatch.setattr(
        production,
        "_model_gate",
        lambda checked_root, **kwargs: {
            "passed": checked_root.resolve() == root.resolve(),
            "reason": None,
            "manifest_sha256": hashlib.sha256(
                kwargs["manifest_bytes"]
            ).hexdigest(),
            "promotion_approval": {
                "proposal_sha256": approval["proposal_sha256"],
                "receipt_sha256": approval["receipt_sha256"],
            },
        },
    )
    return load_promoted_model_runtime(root)


def _five_family_surface():
    frames = []
    for index, family in enumerate(FAMILIES):
        frame = _rows()
        frame["family_root"] = family
        frame["raw_symbol"] = frame["raw_symbol"].str.replace("SPX", family)
        frame["reference_price"] = 100.0 + index
        frames.append(frame)
    return build_contract_surface_features(pd.concat(frames, ignore_index=True))


def test_promoted_prediction_is_exact_family_complete_and_provenanced(
    tmp_path, monkeypatch
):
    runtime = _runtime(tmp_path, monkeypatch, "tcbbo-v1")

    predictions = predict_promoted_close(_five_family_surface(), runtime)

    assert [item.family_root for item in predictions] == sorted(FAMILIES)
    assert all(item.model_version == "tcbbo-v1" for item in predictions)
    assert all(item.source_sha256 == "a" * 64 for item in predictions)
    assert all(item.prediction_mode == "tcbbo_promoted" and item.is_estimate for item in predictions)
    assert all(item.predicted_level > item.reference_price for item in predictions)
    assert all(item.prediction_lower < item.predicted_level < item.prediction_upper for item in predictions)
    assert all(item.calibration_evidence_sha256 == "c" * 64 for item in predictions)


def test_promoted_prediction_rejects_partial_or_unverified_input(
    tmp_path, monkeypatch
):
    runtime = _runtime(tmp_path, monkeypatch)
    partial = _five_family_surface().query("family_root != 'VIX'")
    with pytest.raises(ValueError, match="exact-horizon row per family"):
        predict_promoted_close(partial, runtime)

    unverified = _five_family_surface()
    unverified.loc[unverified["family_root"] == "SPX", "capture_integrity_verified"] = False
    with pytest.raises(ValueError, match="integrity-verified"):
        predict_promoted_close(unverified, runtime)

    string_flags = _five_family_surface()
    string_flags["capture_integrity_verified"] = "False"
    with pytest.raises(ValueError, match="integrity-verified"):
        predict_promoted_close(string_flags, runtime)


def test_promoted_runtime_and_prediction_writes_reject_fabricated_authority(
    tmp_path, monkeypatch
):
    fabricated_runtime = object.__new__(PromotedModelRuntime)
    with pytest.raises(ValueError, match="loader-issued promoted model runtime"):
        predict_promoted_close(_five_family_surface(), fabricated_runtime)

    authorized = _predictions(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="loader-issued promoted model runtime"):
        record_promoted_close_predictions(
            tmp_path / "fabricated-runtime.db",
            tuple(authorized),
            runtime=fabricated_runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="loader-authorized prediction batch"):
        record_promoted_close_predictions(
            tmp_path / "fabricated.db",
            tuple(authorized),
            runtime=authorized.runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )

    changed = list(authorized)
    changed[0] = production.PromotedPrediction(
        **{**changed[0].to_dict(), "model_version": "fabricated"}
    )
    mismatched = production._build_promoted_prediction_batch(
        tuple(changed), authorized.runtime
    )
    with pytest.raises(ValueError, match="does not match its runtime"):
        record_promoted_close_predictions(
            tmp_path / "mismatched.db",
            mismatched,
            runtime=authorized.runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )

    other_runtime = _runtime(tmp_path, monkeypatch, "other-version")
    with pytest.raises(ValueError, match="does not match its runtime"):
        record_promoted_close_predictions(
            tmp_path / "wrong-runtime.db",
            authorized,
            runtime=other_runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )


def test_production_runtime_rechecks_manifest_and_artifact_after_gate(tmp_path, monkeypatch):
    models = tmp_path / "models"
    models.mkdir()
    artifact_text = json.dumps(_payload(), sort_keys=True, separators=(",", ":"))
    artifact_path = models / "artifact.json"
    artifact_path.write_text(artifact_text, encoding="utf-8")
    artifact_hash = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    manifest = {
        "version": "v1",
        "evidence_contract_version": "test-evidence-v1",
        "feature_schema_hash": _payload()["feature_contract_hash"],
        "artifact_path": "artifact.json",
        "artifact_sha256": artifact_hash,
        "execution_device": "cpu",
        "inference_batch_rows": 5,
        "deployment_calibration": {
            "method": "family_absolute_residual_conformal_v1",
            "evidence_sha256": "c" * 64,
            "model_version": "v1", "artifact_sha256": artifact_hash,
            "alpha": 0.1, "target_coverage": 0.9,
            "asof_utc": "2026-08-24T20:00:00+00:00",
            "family_radii": [
                {"family_root": family, "radius_log_return": 0.01}
                for family in FAMILIES
            ],
        },
    }
    approval = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="test-risk-owner",
        decision="APPROVE",
        approved_at_utc=datetime(2026, 8, 24, 21, 0, tzinfo=UTC),
    )
    manifest["promotion_approval"] = approval
    (models / "closing_tape_model.json").write_text(json.dumps(manifest), encoding="utf-8")
    monkeypatch.setattr(
        production,
        "_model_gate",
        lambda _root, **kwargs: {
            "passed": True,
            "reason": None,
            "manifest_sha256": hashlib.sha256(
                kwargs["manifest_bytes"]
            ).hexdigest(),
            "promotion_approval": {
                "proposal_sha256": approval["proposal_sha256"],
                "receipt_sha256": approval["receipt_sha256"],
            },
        },
    )

    runtime = load_promoted_model_runtime(tmp_path)
    assert runtime.version == "v1"
    assert runtime.artifact_sha256 == artifact_hash

    artifact_path.write_text(artifact_text + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="changed after gate validation"):
        load_promoted_model_runtime(tmp_path)


def test_production_runtime_parses_the_exact_manifest_snapshot_passed_to_gate(
    tmp_path, monkeypatch
):
    original_runtime = _runtime(tmp_path, monkeypatch, "approved-v1")
    root = tmp_path / "runtime"
    manifest_path = root / "models" / "closing_tape_model.json"
    approved_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    approved_bytes = manifest_path.read_bytes()

    def swap_after_snapshot(_root, *, manifest_path, manifest_bytes):
        assert manifest_bytes == approved_bytes
        replacement = dict(approved_manifest)
        replacement["version"] = "unapproved-replacement"
        manifest_path.write_text(json.dumps(replacement), encoding="utf-8")
        return {
            "passed": True,
            "reason": None,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "promotion_approval": {
                "proposal_sha256": approved_manifest["promotion_approval"][
                    "proposal_sha256"
                ],
                "receipt_sha256": approved_manifest["promotion_approval"][
                    "receipt_sha256"
                ],
            },
        }

    monkeypatch.setattr(production, "_model_gate", swap_after_snapshot)

    loaded = load_promoted_model_runtime(root)

    assert loaded.version == original_runtime.version == "approved-v1"
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["version"] == (
        "unapproved-replacement"
    )


def test_production_runtime_refuses_unpromoted_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(
        production, "_model_gate",
        lambda _root: {"passed": False, "reason": "paper evidence incomplete"},
    )
    with pytest.raises(RuntimeError, match="paper evidence incomplete"):
        load_promoted_model_runtime(tmp_path)


def _predictions(tmp_path, monkeypatch):
    return predict_promoted_close(
        _five_family_surface(), _runtime(tmp_path, monkeypatch)
    )


@pytest.fixture
def valid_promoted_rows(tmp_path, monkeypatch):
    database = tmp_path / "validated-market.db"
    predictions = _predictions(tmp_path, monkeypatch)
    record_promoted_close_predictions(
        database,
        predictions,
        runtime=predictions.runtime,
        trading_day=date(2026, 8, 25),
        session_id="validated-session",
        recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
    )
    return load_promoted_close_predictions(
        database, trading_day=date(2026, 8, 25)
    )


def test_promoted_batch_validator_accepts_exact_five_family_identity(
    valid_promoted_rows,
):
    validate_promoted_close_prediction_batch(valid_promoted_rows)

    assert {row["family_root"] for row in valid_promoted_rows} == set(FAMILIES)


def test_promoted_batch_validator_rejects_missing_or_duplicate_family(
    valid_promoted_rows,
):
    with pytest.raises(RuntimeError, match="exactly five production-family rows"):
        validate_promoted_close_prediction_batch(valid_promoted_rows[:-1])

    duplicate = [dict(row) for row in valid_promoted_rows]
    duplicate[-1]["family_root"] = duplicate[0]["family_root"]
    with pytest.raises(RuntimeError, match="exactly the five production families"):
        validate_promoted_close_prediction_batch(duplicate)


def test_promoted_batch_validator_rejects_mixed_provenance(valid_promoted_rows):
    mixed = [dict(row) for row in valid_promoted_rows]
    mixed[0]["session_id"] = "different-session"

    with pytest.raises(RuntimeError, match="share one complete provenance set"):
        validate_promoted_close_prediction_batch(mixed)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    (
        ("prediction_key", "A" * 64),
        ("calibration_evidence_sha256", "c" * 63),
        ("source_sha256", "not-a-sha256"),
        ("artifact_sha256", "B" * 64),
    ),
)
def test_promoted_batch_validator_rejects_malformed_hashes(
    valid_promoted_rows,
    field,
    invalid_value,
):
    invalid = [dict(row) for row in valid_promoted_rows]
    invalid[0][field] = invalid_value

    with pytest.raises(RuntimeError, match=rf"{field} must be a lowercase SHA-256"):
        validate_promoted_close_prediction_batch(invalid)


def test_promoted_batch_validator_recomputes_prediction_interval(
    valid_promoted_rows,
):
    inconsistent = [dict(row) for row in valid_promoted_rows]
    row = inconsistent[0]
    row["predicted_level"] = (
        float(row["predicted_level"]) + float(row["prediction_upper"])
    ) / 2.0

    with pytest.raises(RuntimeError, match="inconsistent with its log returns"):
        validate_promoted_close_prediction_batch(inconsistent)


@pytest.mark.parametrize(
    "recorded_time_position",
    (
        "before_features",
        "at_target_close",
    ),
)
def test_promoted_batch_validator_rejects_out_of_window_recorded_time(
    valid_promoted_rows,
    recorded_time_position,
):
    invalid = [dict(row) for row in valid_promoted_rows]
    feature_time = datetime.fromisoformat(invalid[0]["feature_available_at_utc"])
    if recorded_time_position == "before_features":
        recorded_time = feature_time - timedelta(seconds=1)
    else:
        recorded_time = feature_time + timedelta(
            minutes=int(invalid[0]["decision_horizon_minutes"])
        )
    for row in invalid:
        row["recorded_at_utc"] = recorded_time.isoformat()

    with pytest.raises(
        RuntimeError,
        match="must follow feature availability and precede target close",
    ):
        validate_promoted_close_prediction_batch(invalid)


def test_promoted_batch_validator_recomputes_prediction_key(valid_promoted_rows):
    invalid = [dict(row) for row in valid_promoted_rows]
    invalid[0]["prediction_key"] = "d" * 64

    with pytest.raises(RuntimeError, match="prediction_key does not match row identity"):
        validate_promoted_close_prediction_batch(invalid)


def test_promoted_predictions_are_atomic_immutable_and_idempotent(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    predictions = _predictions(tmp_path, monkeypatch)
    first = record_promoted_close_predictions(
        database, predictions, runtime=predictions.runtime,
        trading_day=date(2026, 8, 25), session_id="s1",
        recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
    )
    second = record_promoted_close_predictions(
        database, predictions, runtime=predictions.runtime,
        trading_day=date(2026, 8, 25), session_id="s1",
        recorded_at_utc=datetime(2026, 8, 25, 15, 3, tzinfo=UTC),
    )

    assert first == second
    rows = load_promoted_close_predictions(database, trading_day=date(2026, 8, 25))
    assert len(rows) == 5
    assert {row["family_root"] for row in rows} == set(FAMILIES)
    assert all(row["is_estimate"] and row["prediction_mode"] == "tcbbo_promoted" for row in rows)
    assert all(row["prediction_lower"] < row["predicted_level"] < row["prediction_upper"] for row in rows)
    assert all(row["calibration_evidence_sha256"] == "c" * 64 for row in rows)
    with sqlite3.connect(database) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("UPDATE promoted_close_predictions SET predicted_level=0")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute("DELETE FROM promoted_close_predictions")
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            connection.execute(
                "INSERT OR REPLACE INTO promoted_close_predictions "
                "SELECT * FROM promoted_close_predictions WHERE family_root='SPX'"
            )


def test_promoted_prediction_ledger_rejects_an_exact_column_only_schema(
    tmp_path, monkeypatch
):
    database = tmp_path / "column-only.db"
    definitions = ",".join(
        f"{column} TEXT" for column in production.PROMOTED_PREDICTION_COLUMNS
    )
    with sqlite3.connect(database) as connection:
        connection.execute(f"CREATE TABLE promoted_close_predictions ({definitions})")

    with pytest.raises(RuntimeError, match="ledger schema is incompatible"):
        load_promoted_close_predictions(database, trading_day=date(2026, 8, 25))

    predictions = _predictions(tmp_path, monkeypatch)
    with pytest.raises(RuntimeError, match="ledger schema is incompatible"):
        record_promoted_close_predictions(
            database,
            predictions,
            runtime=predictions.runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )


def test_promoted_prediction_writer_rejects_a_pre_feature_timestamp(
    tmp_path, monkeypatch
):
    database = tmp_path / "pre-feature.db"
    predictions = _predictions(tmp_path, monkeypatch)
    feature_time = datetime.fromisoformat(predictions[0].feature_available_at_utc)

    with pytest.raises(ValueError, match="before its features"):
        record_promoted_close_predictions(
            database,
            predictions,
            runtime=predictions.runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=feature_time - timedelta(seconds=1),
        )

    assert not database.exists()


def test_promoted_prediction_ledger_rejects_conflict_and_post_close_write(
    tmp_path, monkeypatch
):
    database = tmp_path / "market.db"
    predictions = _predictions(tmp_path, monkeypatch)
    record_promoted_close_predictions(
        database, predictions, runtime=predictions.runtime,
        trading_day=date(2026, 8, 25), session_id="s1",
        recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
    )
    changed = list(predictions)
    changed[0] = production.PromotedPrediction(
        **{**changed[0].to_dict(), "predicted_level": changed[0].predicted_level + 1.0}
    )
    changed_batch = production._build_promoted_prediction_batch(
        tuple(changed), predictions.runtime
    )
    with pytest.raises(ValueError, match="conflicting immutable"):
        record_promoted_close_predictions(
            database, changed_batch, runtime=predictions.runtime,
            trading_day=date(2026, 8, 25), session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 3, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="before its target close"):
        record_promoted_close_predictions(
            tmp_path / "late.db", predictions, runtime=predictions.runtime,
            trading_day=date(2026, 8, 25), session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 16, tzinfo=UTC),
        )


def test_promoted_prediction_ledger_rejects_invalid_interval(
    tmp_path, monkeypatch
):
    authorized = _predictions(tmp_path, monkeypatch)
    predictions = list(authorized)
    predictions[0] = production.PromotedPrediction(
        **{**predictions[0].to_dict(), "prediction_lower": predictions[0].predicted_level + 1.0}
    )
    invalid_batch = production._build_promoted_prediction_batch(
        tuple(predictions), authorized.runtime
    )

    with pytest.raises(ValueError, match="calibrated interval is invalid"):
        record_promoted_close_predictions(
            tmp_path / "invalid.db", invalid_batch, runtime=authorized.runtime,
            trading_day=date(2026, 8, 25), session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )


def test_legacy_immutable_ledger_requires_explicit_reviewed_migration(
    tmp_path, monkeypatch
):
    database = tmp_path / "legacy.db"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE promoted_close_predictions (prediction_key TEXT PRIMARY KEY)"
        )

    with pytest.raises(RuntimeError, match="explicit reviewed migration"):
        predictions = _predictions(tmp_path, monkeypatch)
        record_promoted_close_predictions(
            database, predictions, runtime=predictions.runtime,
            trading_day=date(2026, 8, 25),
            session_id="s1",
            recorded_at_utc=datetime(2026, 8, 25, 15, 2, tzinfo=UTC),
        )
    with pytest.raises(RuntimeError, match="explicit reviewed migration"):
        load_promoted_close_predictions(database, trading_day=date(2026, 8, 25))
    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT COUNT(*) FROM promoted_close_predictions"
        ).fetchone()[0] == 0
