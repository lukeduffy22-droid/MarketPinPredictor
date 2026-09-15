from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import backend.database as database
import backend.prediction_passport as passport_module
from backend.api.schemas import ForecastPassportDetailV1, ForecastPassportListV1
from backend.ai_predictor import build_ai_prediction
from backend.prediction_passport import (
    PassportConflictError,
    PassportIntegrityError,
    issue_prediction_passport,
    issue_snapshot_passport,
    list_prediction_passport_summaries,
    list_prediction_passports,
    passport_detail_envelope,
    read_prediction_passport,
    replay_prediction_passport,
)


@pytest.fixture
def passport_store(tmp_path, monkeypatch):
    path = tmp_path / "passports.db"
    url = f"sqlite:///{path.as_posix()}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    sessions = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(database, "DATABASE_URL", url)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr(database, "SessionLocal", sessions)
    monkeypatch.setattr(database, "PREDICTION_EXPORT_DIR", tmp_path / "exports")
    database.init_db()
    yield engine
    engine.dispose()


def _prediction(*, calculation_id: str, usable: bool = True) -> dict:
    source = {
        "symbol": "SPX",
        "provider": "test",
        "price": 100.0,
        "likely_close": 101.0,
        "gamma_pin": 101.5,
        "zero_gamma": 99.0,
        "max_pain": 100.5,
        "net_gex": 20.0,
        "gross_gex": 40.0,
        "contracts": 120,
        "quotes_cached": 100,
        "fresh_quote_count": 80,
        "quote_age_seconds": 0.5,
        "timestamp": "2026-06-18T15:29:59Z",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 7,
        "calculation_id": calculation_id,
        "validation_is_valid": usable,
        "gamma_excluded_from_model": not usable,
        "gex_formula_version": "gex-v1",
    }
    result = build_ai_prediction("SPX", source)
    result.update(
        {
            "timestamp": "2026-06-18T15:30:00Z",
            "quote_timestamp_utc": source["timestamp"],
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 7,
            "quote_age_seconds": 0.5,
            "active_contract_count": 120,
            "fresh_quote_count": 80,
            "confidence_scale": "percent",
            "confidence_kind": "data_quality_heuristic",
            "pin_payload": source,
        }
    )
    return result


def _calculation(calculation_id: str) -> None:
    saved = database.save_gamma_calculation_inputs(
        {
            "calculation_id": calculation_id,
            "symbol": "SPX",
            "calculated_at_utc": "2026-06-18T15:29:59Z",
            "provider": "test",
            "subscription_epoch_id": "e" * 64,
            "status": "valid",
            "formula_version": "gex-v1",
            "universe_sha256": "b" * 64,
        },
        {
            "input_schema_version": "gamma-inputs-v1",
            "subscription_epoch_id": "e" * 64,
            "rows": [{"strike": 100.0}],
        },
    )
    assert saved is not None


def test_research_passport_is_immutable_replayable_and_idempotent(
    passport_store, monkeypatch
):
    calculation_id = "11111111-1111-1111-1111-111111111111"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    snapshot = database.save_prediction_snapshot(prediction, prediction_mode="backend_periodic")

    first = issue_snapshot_passport(snapshot.id, prediction=prediction)
    second = issue_snapshot_passport(snapshot.id, prediction=prediction)

    assert first["forecast_id"] == second["forecast_id"]
    assert first["state"] == "RESEARCH_ONLY"
    assert first["decision_grade"] is False
    assert first["provenance"]["calculation_input_sha256"]
    assert first["model"]["model_artifact_sha256"]
    assert first["replay"]["status"] == "VERIFIED"
    explicit = replay_prediction_passport(first["forecast_id"])
    assert explicit["forecast_id"] == first["forecast_id"]
    assert explicit["record_sha256"] == first["record_sha256"]
    assert explicit["replay"]["status"] == "VERIFIED"
    monkeypatch.setattr(
        "backend.prediction_passport.model_artifact_sha256", lambda: "f" * 64
    )
    unavailable = replay_prediction_passport(first["forecast_id"])
    assert unavailable["replay"]["status"] == "UNAVAILABLE"
    assert read_prediction_passport(first["forecast_id"])["replay"]["status"] == (
        "VERIFIED"
    )
    with pytest.raises(Exception, match="prediction passports are immutable"):
        with passport_store.begin() as connection:
            connection.execute(text(
                "UPDATE prediction_passports SET state='valid' WHERE passport_id=:id"
            ), {"id": first["forecast_id"]})
    with pytest.raises(Exception, match="prediction passports are immutable"):
        with passport_store.begin() as connection:
            connection.execute(
                text("DELETE FROM prediction_passports WHERE passport_id=:id"),
                {"id": first["forecast_id"]},
            )
    with pytest.raises(Exception, match="prediction passports are immutable"):
        with passport_store.begin() as connection:
            connection.execute(
                text(
                    "INSERT OR REPLACE INTO prediction_passports "
                    "SELECT * FROM prediction_passports WHERE passport_id=:id"
                ),
                {"id": first["forecast_id"]},
            )


def test_decision_grade_incomplete_evidence_becomes_numeric_free_abstention(passport_store):
    prediction = {
        "symbol": "SPX",
        "timestamp": "2026-06-18T15:30:00Z",
        "provider": "test",
        "usable": True,
        "current_price": 100.0,
        "predicted_close": 101.0,
        "model_version": "unproven-v1",
        "model_type": "unit-test",
        "feature_snapshot": {"spot": 100.0},
        "pin_payload": {"price": 100.0},
    }

    passport = issue_prediction_passport(
        prediction,
        prediction_mode="unit",
        origin_key="incomplete",
        requested_state="valid",
        decision_grade=True,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["prediction"]["point_estimate"] is None
    assert "calibrated_interval" not in passport["prediction"]
    assert "calculation_id" in passport["quality"]["missing_evidence"]
    assert any(reason.startswith("MISSING_EVIDENCE:") for reason in passport["quality"]["state_reasons"])


def test_unapproved_or_unresolved_production_evidence_abstains(passport_store):
    calculation_id = "22222222-2222-2222-2222-222222222222"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    prediction.update(
        {
            "prediction_lower": prediction["predicted_close"] - 1.0,
            "prediction_upper": prediction["predicted_close"] + 1.0,
            "interval_target_coverage": 0.9,
            "calibration_method": "test-conformal",
            "calibration_evidence_sha256": "c" * 64,
        }
    )

    passport = issue_prediction_passport(
        prediction,
        prediction_mode="promoted-test",
        origin_key="complete",
        requested_state="valid",
        decision_grade=True,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["decision_grade"] is False
    assert passport["prediction"]["point_estimate"] is None
    assert passport["prediction"]["interval_lower"] is None
    assert passport["prediction"]["interval_upper"] is None
    assert "approved_production_manifest" in passport["quality"]["missing_evidence"]
    assert "resolved_calibration_evidence" in passport["quality"]["missing_evidence"]
    assert "verified_calculation_lineage" in passport["quality"]["missing_evidence"]


def test_requested_valid_cannot_bypass_missing_evidence(passport_store):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "model_version": "unproven-v1",
            "feature_snapshot": {"spot": 100.0},
        },
        prediction_mode="unit",
        origin_key="valid-bypass",
        requested_state="valid",
        decision_grade=False,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["decision_grade"] is False
    assert passport["prediction"]["point_estimate"] is None
    assert passport["quality"]["missing_evidence"]


@pytest.mark.parametrize("state", ["abstain", "stale", "unavailable"])
def test_terminal_nonvalid_states_never_expose_numeric_forecasts(passport_store, state):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "prediction_lower": 99.0,
            "prediction_upper": 102.0,
        },
        prediction_mode="unit",
        origin_key=f"state-{state}",
        requested_state=state,
    )

    assert passport["state"] == state.upper()
    assert passport["prediction"]["point_estimate"] is None
    assert passport["prediction"]["interval_lower"] is None
    assert passport["prediction"]["interval_upper"] is None
    assert passport["quality"]["state_reasons"]


def test_invalid_hash_evidence_cannot_satisfy_valid_passport(passport_store):
    calculation_id = "66666666-6666-6666-6666-666666666666"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    prediction.update(
        {
            "model_artifact_sha256": "not-a-sha256",
            "prediction_lower": prediction["predicted_close"] - 1.0,
            "prediction_upper": prediction["predicted_close"] + 1.0,
            "interval_target_coverage": 0.9,
            "calibration_method": "test-conformal",
            "calibration_evidence_sha256": "also-not-a-sha256",
        }
    )

    passport = issue_prediction_passport(
        prediction,
        prediction_mode="unit",
        origin_key="invalid-hashes",
        requested_state="valid",
        decision_grade=True,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["model"]["model_artifact_sha256"] is None
    assert "valid_model_artifact_sha256" in passport["quality"]["missing_evidence"]
    assert "valid_calibration_evidence_sha256" in passport["quality"]["missing_evidence"]


def test_fallback_can_never_masquerade_as_production(passport_store):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "provider": "historical-fallback",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "model_type": "pin-payload-fallback",
            "model_version": "fallback-1.0",
            "pin_payload": {"price": 100.0, "is_fallback": True},
        },
        prediction_mode="legacy",
        origin_key="fallback",
        requested_state="valid",
        decision_grade=True,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["prediction"]["point_estimate"] is None
    assert "NON_PRODUCTION_FALLBACK" in passport["quality"]["state_reasons"]


@pytest.mark.parametrize(
    "provenance_field",
    ["universe_provenance", "oi_analytics_provenance"],
)
def test_nested_fallback_can_never_masquerade_as_production(
    passport_store,
    provenance_field,
):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "provider": "databento",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "model_type": "Databento Quant Ensemble",
            "model_version": "test-v1",
            "pin_payload": {
                "provider": "databento",
                "price": 100.0,
                "validation_is_valid": True,
                provenance_field: {"is_fallback": True},
            },
        },
        prediction_mode="backend_lifecycle",
        origin_key=f"nested-fallback-{provenance_field}",
        requested_state="valid",
        decision_grade=True,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["prediction"]["point_estimate"] is None
    assert "NON_PRODUCTION_FALLBACK" in passport["quality"]["state_reasons"]


def test_conflicting_immutable_origin_is_rejected(passport_store):
    calculation_id = "33333333-3333-3333-3333-333333333333"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    issue_prediction_passport(prediction, prediction_mode="unit", origin_key="same-origin")
    prediction["predicted_close"] += 1.0

    with pytest.raises(PassportConflictError):
        issue_prediction_passport(prediction, prediction_mode="unit", origin_key="same-origin")


def test_snapshot_passport_rejects_forecast_content_from_another_snapshot(passport_store):
    calculation_id = "88888888-8888-8888-8888-888888888888"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    snapshot = database.save_prediction_snapshot(prediction, prediction_mode="unit")
    conflicting = {**prediction, "predicted_close": prediction["predicted_close"] + 5.0}

    with pytest.raises(PassportConflictError, match="predicted_close"):
        issue_snapshot_passport(snapshot.id, prediction=conflicting)

    with database.SessionLocal() as session:
        assert session.query(database.PredictionPassport).count() == 0


def test_read_detects_canonical_payload_tampering(passport_store):
    calculation_id = "44444444-4444-4444-4444-444444444444"
    _calculation(calculation_id)
    issued = issue_prediction_passport(
        _prediction(calculation_id=calculation_id),
        prediction_mode="unit",
        origin_key="tamper",
    )
    with passport_store.begin() as connection:
        connection.execute(text("DROP TRIGGER prediction_passports_no_update"))
        connection.execute(
            text("UPDATE prediction_passports SET canonical_payload_json=:payload WHERE passport_id=:id"),
            {"payload": json.dumps({"forecast_id": issued["forecast_id"]}), "id": issued["forecast_id"]},
        )

    with pytest.raises(PassportIntegrityError):
        read_prediction_passport(issued["forecast_id"])


def test_read_detects_indexed_relational_mirror_tampering(passport_store):
    calculation_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    _calculation(calculation_id)
    issued = issue_prediction_passport(
        _prediction(calculation_id=calculation_id),
        prediction_mode="unit",
        origin_key="relational-tamper",
    )
    with passport_store.begin() as connection:
        connection.execute(text("DROP TRIGGER prediction_passports_no_update"))
        connection.execute(
            text("UPDATE prediction_passports SET symbol='NDX' WHERE passport_id=:id"),
            {"id": issued["forecast_id"]},
        )

    with pytest.raises(PassportIntegrityError, match="symbol mirror mismatch"):
        read_prediction_passport(issued["forecast_id"])


def test_historical_row_without_replay_remains_hash_exact_and_read_only(
    passport_store,
    monkeypatch,
):
    calculation_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    _calculation(calculation_id)
    issued = issue_prediction_passport(
        _prediction(calculation_id=calculation_id),
        prediction_mode="unit",
        origin_key="historical-no-replay",
    )
    with passport_store.begin() as connection:
        connection.execute(text("DROP TRIGGER prediction_passports_no_update"))
        canonical = connection.execute(
            text(
                "SELECT canonical_payload_json FROM prediction_passports "
                "WHERE passport_id=:id"
            ),
            {"id": issued["forecast_id"]},
        ).scalar_one()
        payload = json.loads(canonical)
        payload.pop("replay")
        payload["quality"].pop("source_validation_is_valid")
        historical_canonical = passport_module._canonical_json(payload)
        historical_hash = passport_module._sha256_text(historical_canonical)
        connection.execute(
            text(
                "UPDATE prediction_passports SET canonical_payload_json=:payload, "
                "record_sha256=:record_hash WHERE passport_id=:id"
            ),
            {
                "payload": historical_canonical,
                "record_hash": historical_hash,
                "id": issued["forecast_id"],
            },
        )

    monkeypatch.setattr(
        passport_module,
        "replay_ai_prediction",
        lambda _features: (_ for _ in ()).throw(AssertionError("GET must not replay")),
    )
    read_back = read_prediction_passport(issued["forecast_id"])
    detail = ForecastPassportDetailV1.model_validate(passport_detail_envelope(read_back))

    assert "replay" not in read_back
    assert detail.passport.replay is None
    assert "replay" not in detail.model_dump(exclude_unset=True)["passport"]


def test_invalid_source_status_and_malformed_interval_abstain(passport_store):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "quote_timestamp_utc": "2026-06-18T15:29:59Z",
            "provider": "test",
            "usable": True,
            "validation_status": "invalid",
            "current_price": 100.0,
            "predicted_close": 101.0,
            "prediction_lower": 102.0,
            "prediction_upper": 99.0,
        },
        prediction_mode="unit",
        origin_key="invalid-status-interval",
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["prediction"]["point_estimate"] is None
    assert "SOURCE_VALIDATION_FAILED" in passport["quality"]["state_reasons"]
    assert "INVALID_PREDICTION_INTERVAL" in passport["quality"]["state_reasons"]


def test_stale_data_age_cannot_leave_a_numeric_research_forecast(passport_store):
    passport = issue_prediction_passport(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "quote_timestamp_utc": "2026-06-18T15:29:59Z",
            "provider": "test",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "data_age_seconds": 10_000.0,
        },
        prediction_mode="unit",
        origin_key="stale-data-age",
    )

    assert passport["state"] == "STALE"
    assert passport["prediction"]["point_estimate"] is None


def test_verified_outcome_and_baseline_join_do_not_mutate_passport(passport_store):
    calculation_id = "55555555-5555-5555-5555-555555555555"
    _calculation(calculation_id)
    prediction = _prediction(calculation_id=calculation_id)
    snapshot = database.save_prediction_snapshot(prediction, prediction_mode="unit")
    passport = issue_snapshot_passport(snapshot.id, prediction=prediction)
    database.upsert_verified_eod_close(
        "SPX",
        date(2026, 6, 18),
        100.5,
        "sp-global-official",
        source_reference="https://www.spglobal.com/spdji/official-close",
        source_artifact_sha256="a" * 64,
        observed_at_utc=datetime(2026, 6, 18, 21, 1, tzinfo=timezone.utc),
    )
    result = database.score_prediction_snapshots(
        "SPX", date(2026, 6, 18), prediction_ids=[snapshot.id]
    )
    assert result["scored"] == 1

    after = read_prediction_passport(passport["forecast_id"])

    assert after["record_sha256"] == passport["record_sha256"]
    assert after["outcome"]["actual_close"] == 100.5
    assert after["outcome"]["close_verified"] is True
    assert after["outcome"]["evidence_scope"] == "diagnostic_research_only"
    assert after["outcome"]["training_eligible"] is False
    assert after["outcome"]["performance_claim_eligible"] is False
    assert after["outcome"]["baseline_absolute_error_points"] == pytest.approx(0.5)
    assert after["outcome"]["baseline_lift_points"] is not None


def test_init_db_does_not_backfill_legacy_snapshots(passport_store):
    database.save_prediction_snapshot(
        {
            "symbol": "SPX",
            "timestamp": "2026-06-18T15:30:00Z",
            "usable": True,
            "current_price": 100.0,
            "predicted_close": 101.0,
            "pin_payload": {},
        },
        prediction_mode="legacy",
    )
    database.init_db()
    with database.SessionLocal() as session:
        assert session.query(database.PredictionSnapshot).count() == 1
        assert session.query(database.PredictionPassport).count() == 0


def test_read_and_list_results_are_api_serializable(passport_store):
    calculation_id = "77777777-7777-7777-7777-777777777777"
    _calculation(calculation_id)
    issued = issue_prediction_passport(
        _prediction(calculation_id=calculation_id),
        prediction_mode="unit",
        origin_key="serializable",
    )

    read_back = read_prediction_passport(issued["forecast_id"])
    listed = list_prediction_passports(symbol="spx", state="research", limit=10)
    summaries = list_prediction_passport_summaries(
        symbol="spx", state="research_only", limit=10
    )
    detail = passport_detail_envelope(read_back)

    assert json.loads(json.dumps(read_back))["forecast_id"] == issued["forecast_id"]
    assert [item["forecast_id"] for item in json.loads(json.dumps(listed))] == [
        issued["forecast_id"]
    ]
    assert ForecastPassportDetailV1.model_validate(detail).record_sha256 == (
        issued["record_sha256"]
    )
    assert ForecastPassportListV1.model_validate({"items": summaries}).items[
        0
    ].forecast_id == issued["forecast_id"]

    # Records issued before source_validation_is_valid was captured remain
    # readable without inferring evidence that was never stored.
    historical_detail = json.loads(json.dumps(detail))
    historical_detail["passport"]["quality"].pop("source_validation_is_valid")
    assert ForecastPassportDetailV1.model_validate(
        historical_detail
    ).passport.quality.source_validation_is_valid is None


def test_issuance_requires_a_timestamp_for_deterministic_identity(passport_store):
    with pytest.raises(ValueError, match="valid prediction timestamp"):
        issue_prediction_passport(
            {"symbol": "SPX", "usable": False},
            prediction_mode="unit",
            origin_key="missing-time",
            requested_state="unavailable",
        )


@pytest.mark.parametrize(
    ("target_timestamp", "reason_prefix"),
    [
        ("2026-06-18T16:00:00Z", "NON_SESSION_TARGET:"),
        ("2026-06-19T20:00:00Z", "NON_SESSION_TARGET:"),
        ("not-a-timestamp", "INVALID_TARGET_TIMESTAMP"),
    ],
)
def test_non_official_close_target_abstains_and_cannot_relabel_target(
    passport_store, target_timestamp, reason_prefix
):
    passport = issue_prediction_passport(
        _prediction(calculation_id="target-test"),
        prediction_mode="unit",
        origin_key=f"invalid-target-{target_timestamp}",
        target_timestamp_utc=target_timestamp,
    )

    assert passport["state"] == "ABSTAIN"
    assert passport["prediction"]["point_estimate"] is None
    assert passport["target"]["target_timestamp_utc"] == "2026-06-18T20:00:00+00:00"
    assert any(
        reason.startswith(reason_prefix)
        for reason in passport["quality"]["state_reasons"]
    )


def test_future_prediction_timestamp_is_rejected_before_issuance(
    passport_store, monkeypatch
):
    monkeypatch.setattr(
        passport_module,
        "_issuance_now_utc",
        lambda: datetime(2026, 6, 18, 15, 0, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError, match="prediction timestamp cannot be in the future"):
        issue_prediction_passport(
            _prediction(calculation_id="future-prediction"),
            prediction_mode="unit",
            origin_key="future-prediction",
        )

    with database.SessionLocal() as session:
        assert session.query(database.PredictionPassport).count() == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"symbol": "NDX"},
        {
            "timestamp": "2026-06-19T15:30:00Z",
            "quote_timestamp_utc": "2026-06-19T15:29:59Z",
        },
    ],
)
def test_snapshot_passport_rejects_cross_symbol_or_date_binding(
    passport_store, overrides
):
    prediction = _prediction(calculation_id="snapshot-binding")
    snapshot = database.save_prediction_snapshot(
        prediction, prediction_mode="backend_periodic"
    )
    supplied = {**prediction, **overrides}

    with pytest.raises(PassportConflictError, match="prediction_snapshot_id conflicts"):
        issue_snapshot_passport(snapshot.id, prediction=supplied)


def test_direct_issuer_cannot_attach_arbitrary_snapshot_id(passport_store):
    prediction = _prediction(calculation_id="direct-binding")
    snapshot = database.save_prediction_snapshot(
        prediction, prediction_mode="backend_periodic"
    )

    with pytest.raises(ValueError, match="wrapper-private"):
        issue_prediction_passport(
            prediction,
            prediction_mode="unit",
            prediction_snapshot_id=snapshot.id,
        )
