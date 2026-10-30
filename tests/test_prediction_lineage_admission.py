from datetime import datetime
import json
import zlib

import pytest
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

import backend.database as db


@pytest.fixture
def store(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{(tmp_path / 'lineage.sqlite').as_posix()}")
    monkeypatch.setattr(db, "engine", engine)
    monkeypatch.setattr(db, "SessionLocal", sessionmaker(bind=engine))
    monkeypatch.setattr(db, "PREDICTION_EXPORT_DIR", tmp_path / "exports")
    db.Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


def source():
    return {"symbol": "SPX", "provider": "databento", "calculation_id": "calc-1",
            "subscription_epoch_id": "e" * 64, "subscription_generation": 2,
            "timestamp": "2026-10-27T15:00:00Z", "price": 100.0,
            "latest_ts_recv_utc": "2026-10-27T14:59:59Z",
            "validation_is_valid": True, "gamma_excluded_from_model": False,
            "usable_for_prediction": True}


def retain(s, *, run_changes=None, input_changes=None, corrupt=False):
    payload = {"calculation_id": s["calculation_id"], "symbol": s["symbol"],
               "calculated_at_utc": s["timestamp"],
               "subscription_epoch_id": s["subscription_epoch_id"],
               "subscription_generation": s["subscription_generation"],
               "input_schema_version": "gamma-inputs-v2-point-in-time",
               "parameters": {"contract_multiplier": 100.0},
               "output_summary": {"calculation_id": s["calculation_id"], "price": s["price"],
                                  "symbol": s["symbol"], "subscription_epoch_id": s["subscription_epoch_id"],
                                  "subscription_generation": s["subscription_generation"],
                                  "validation_is_valid": True, "gamma_excluded_from_model": False},
               "rejection_counts": {},
               "raw_fresh_chain_rows": [{"symbol": "SPXW  261027C00100000"}],
               "calculated_gex_rows": [{"gamma": 0.1, "open_interest": 10, "gex": 100.0}]}
    payload.update(input_changes or {})
    run = {"calculation_id": s["calculation_id"], "symbol": s["symbol"],
           "provider": s["provider"], "calculated_at_utc": s["timestamp"],
           "subscription_epoch_id": s["subscription_epoch_id"],
           "subscription_generation": s["subscription_generation"], "status": "valid",
           "formula_version": "databento-gex-v2-call-minus-put", "spot_price": s["price"],
           "chain_row_count": 1, "gex_row_count": 1}
    run.update(run_changes or {})
    if not corrupt:
        return db.save_gamma_calculation_inputs(run, payload)
    # A corrupt retained fixture is inserted as-is, not repaired by the reader.
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    blob = zlib.compress(raw)
    with db.SessionLocal.begin() as session:
        record = db.GammaCalculationRun(
            **{k: v for k, v in run.items() if k != "calculated_at_utc"},
            calculated_at_utc=datetime(2026, 10, 27, 15), trading_date=datetime(2026, 10, 27).date(),
            input_schema_version=payload["input_schema_version"],
        )
        session.add(record)
        session.flush()
        session.add(db.GammaCalculationInputBlob(
            calculation_run_id=record.id, encoding="canonical-json+zlib-v1",
            payload_sha256="0" * 64, uncompressed_bytes=len(raw),
            compressed_bytes=len(blob), payload=blob,
        ))


def prediction(s):
    return {"symbol": "SPX", "provider": "databento", "usable": True,
            "timestamp": "2026-10-27T15:00:01Z", "current_price": 100.0,
            "predicted_close": 101.0, "model_version": "databento_quant_ensemble_v1",
            "quote_timestamp_utc": s["latest_ts_recv_utc"],
            "subscription_epoch_id": s["subscription_epoch_id"],
            "subscription_generation": s["subscription_generation"], "pin_payload": s}


def count_predictions():
    with db.SessionLocal() as session:
        return session.scalar(select(func.count()).select_from(db.PredictionSnapshot))


def test_exact_retained_inputs_admit_forecast_and_repeat_is_idempotent(store):
    s = source()
    receipt = retain(s)
    verified = db.verify_prediction_calculation_lineage(s, symbol="SPX", predicted_at_utc="2026-10-27T15:00:01Z")
    assert verified["valid"] and verified["payload_sha256"] == receipt["payload_sha256"]
    first = db.save_prediction_snapshot(prediction(s), "backend_periodic")
    second = db.save_prediction_snapshot(prediction(s), "backend_periodic")
    assert first.id == second.id and count_predictions() == 1
    lines = (db.PREDICTION_EXPORT_DIR / "2026-10-27.ndjson").read_text().splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["source_payload"]["calculation_id"] == "calc-1"


@pytest.mark.parametrize("changes,reason", [
    ({"calculation_id": None}, "CALCULATION_ID_MISSING"),
    ({"calculation_id": "unretained"}, "CALCULATION_INPUTS_NOT_RETAINED"),
    ({"symbol": "NDX"}, "CALCULATION_SYMBOL_MISMATCH"),
    ({"subscription_epoch_id": "f" * 64}, "CALCULATION_PROCESS_IDENTITY_MISMATCH"),
    ({"subscription_generation": 3}, "CALCULATION_PROCESS_IDENTITY_MISMATCH"),
    ({"subscription_generation": True}, "CALCULATION_PROCESS_IDENTITY_INVALID"),
    ({"timestamp": "2026-10-27T15:00:00.000001Z"}, "CALCULATION_TIMESTAMP_MISMATCH"),
    ({"price": 102.0}, "CALCULATION_REFERENCE_MISMATCH"),
    ({"provider": "other"}, "CALCULATION_PROVIDER_MISMATCH"),
    ({"calculation_input_sha256": "f" * 64}, "CALCULATION_INPUT_HASH_MISMATCH"),
    ({"latest_ts_recv_utc": "2026-10-27T15:00:02Z"}, "SOURCE_TIMESTAMP_AFTER_PREDICTION"),
    ({"latest_ts_recv_utc": "not-a-timestamp"}, "SOURCE_TIMESTAMP_INVALID"),
    ({"validation_is_valid": False}, "CALCULATION_SOURCE_NOT_USABLE"),
    ({"gamma_excluded_from_model": True}, "CALCULATION_SOURCE_NOT_USABLE"),
    ({"gross_gex": 500.0}, "CALCULATION_OUTPUT_MISMATCH:gross_gex"),
    ({"usable_for_prediction": False}, "CALCULATION_SOURCE_NOT_USABLE"),
    ({"universe_provenance": {"is_fallback": True}}, "CALCULATION_SOURCE_NOT_USABLE"),
])
def test_claimed_or_cross_bound_identity_cannot_create_prediction(store, changes, reason):
    original = source()
    retain(original)
    changed = {**original, **changes, "calculation_inputs_persisted": True}
    result = db.verify_prediction_calculation_lineage(changed, symbol="SPX", predicted_at_utc="2026-10-27T15:00:01Z")
    assert not result["valid"] and reason in result["reasons"]
    assert db.save_prediction_snapshot(prediction(changed), "backend_periodic") is None
    assert count_predictions() == 0 and not db.PREDICTION_EXPORT_DIR.exists()


def test_future_calculation_and_wrong_session_are_rejected(store):
    s = source()
    retain(s)
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T14:59:59Z")
    assert "CALCULATION_AFTER_PREDICTION" in result["reasons"]
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-28T15:00:01Z")
    assert "CALCULATION_SYMBOL_OR_SESSION_MISMATCH" in result["reasons"]


def test_invalid_calculation_cannot_be_accepted_even_with_retained_bytes(store):
    s = source()
    retain(s, run_changes={"status": "invalid"})
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None
    assert count_predictions() == 0


def test_corrupt_blob_is_rejected_without_changing_retained_bytes(store):
    s = source()
    retain(s, corrupt=True)
    with db.SessionLocal() as session:
        before = session.query(db.GammaCalculationInputBlob).one().payload
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T15:00:01Z")
    assert "CALCULATION_INPUT_INTEGRITY_FAILED" in result["reasons"]
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None
    with db.SessionLocal() as session:
        assert session.query(db.GammaCalculationInputBlob).one().payload == before
    assert count_predictions() == 0


def test_content_identity_is_verified_after_hash_not_merely_nonempty_id(store):
    s = source()
    retain(s, input_changes={"symbol": "NDX"})
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T15:00:01Z")
    assert "CALCULATION_INPUT_IDENTITY_MISMATCH" in result["reasons"]
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None


def test_top_level_generation_or_reference_cannot_disagree_with_verified_source(store):
    s = source()
    retain(s)
    for changes in [{"subscription_generation": 3}, {"subscription_generation": 2.5},
                    {"subscription_generation": True}, {"provider": "other"},
                    {"is_fallback": True}, {"usable_for_prediction": False},
                    {"current_price": 102.0}, {"quote_timestamp_utc": "2026-10-27T15:00:02Z"},
                    {"gamma_pin": 110.0}, {"net_gex": 500.0}]:
        assert db.save_prediction_snapshot({**prediction(s), **changes}, "backend_periodic") is None
    assert count_predictions() == 0


def test_same_quote_does_not_relabel_historical_model_or_feature_revision(store):
    s = source()
    retain(s)
    initial = prediction(s)
    saved = db.save_prediction_snapshot(initial, "backend_periodic")
    assert saved is not None
    for changes in [{"predicted_close": 102.0}, {"model_version": "databento_changed"}, {"feature_hash": "a" * 64}]:
        assert db.save_prediction_snapshot({**initial, **changes}, "backend_periodic") is None
    with db.SessionLocal() as session:
        original = session.query(db.PredictionSnapshot).one()
        assert original.predicted_close == 101.0 and original.model_version == initial["model_version"]


@pytest.mark.parametrize("changes,reason", [
    ({"input_schema_version": "different"}, "CALCULATION_REPLAY_INPUTS_INCOMPLETE"),
    ({"parameters": None}, "CALCULATION_REPLAY_INPUTS_INCOMPLETE"),
    ({"calculated_gex_rows": []}, "CALCULATION_REPLAY_INPUTS_INCOMPLETE"),
    ({"output_summary": {"price": 999.0}}, "CALCULATION_INPUT_SUMMARY_MISMATCH"),
])
def test_hash_valid_incomplete_replay_input_is_not_a_valid_receipt(store, changes, reason):
    s = source()
    retain(s, input_changes=changes)
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T15:00:01Z")
    assert not result["valid"] and reason in result["reasons"]
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None


def test_blob_schema_cannot_disagree_with_retained_run_metadata(store):
    s = source()
    retain(s)
    with db.SessionLocal.begin() as session:
        session.query(db.GammaCalculationRun).one().input_schema_version = "wrong-schema-fixture"
    checked = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T15:00:01Z")
    assert "CALCULATION_INPUT_SCHEMA_MISMATCH" in checked["reasons"]


@pytest.mark.parametrize("changes", [
    {"symbol": "NDX"}, {"subscription_epoch_id": "f" * 64},
    {"subscription_generation": 3}, {"validation_is_valid": False},
    {"gamma_excluded_from_model": True}, {"gross_gex": 999.0},
])
def test_hash_valid_summary_cannot_contradict_its_retained_run(store, changes):
    s = source()
    summary = {"calculation_id": s["calculation_id"], "price": s["price"],
               "symbol": s["symbol"], "subscription_epoch_id": s["subscription_epoch_id"],
               "subscription_generation": 2, "validation_is_valid": True,
               "gamma_excluded_from_model": False, **changes}
    retain(s, input_changes={"output_summary": summary})
    result = db.verify_prediction_calculation_lineage(s, predicted_at_utc="2026-10-27T15:00:01Z")
    assert not result["valid"]
    assert any(r.startswith("CALCULATION_INPUT_SUMMARY_MISMATCH") for r in result["reasons"])


def test_guard_cannot_replace_verified_bytes_by_mutating_callers_dictionary(store):
    s = source()
    retain(s)
    p = prediction(s)
    calls = 0

    def guard():
        nonlocal calls
        calls += 1
        if calls > 1:
            p["pin_payload"]["calculation_id"] = "not-retained"
            p["current_price"] = 500.0
        return True

    stored = db.save_prediction_snapshot(p, "backend_periodic", publication_guard=guard)
    assert stored is not None
    assert stored.current_price == 100.0
    assert json.loads(stored.source_payload_json)["calculation_id"] == "calc-1"


@pytest.mark.parametrize("timestamp", [None, "bad-timestamp"])
def test_live_issuance_time_is_required_not_invented_from_current_clock(store, timestamp, monkeypatch):
    s = source()
    retain(s)
    monkeypatch.setattr(db, "_naive_utc", lambda *_: datetime(2026, 10, 27, 15, 0, 1))
    assert db.save_prediction_snapshot({**prediction(s), "timestamp": timestamp}, "backend_periodic") is None
    assert count_predictions() == 0 and not db.PREDICTION_EXPORT_DIR.exists()


def test_failed_sqlite_insert_exports_only_an_unissued_attempt(store, monkeypatch):
    s = source()
    retain(s)
    real_sessions = db.SessionLocal

    class FailingInsert:
        def __init__(self):
            self.inner = real_sessions()

        def __getattr__(self, key):
            return getattr(self.inner, key)

        def add(self, value):
            raise RuntimeError("offline injected persistence failure")

    monkeypatch.setattr(db, "SessionLocal", FailingInsert)
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None
    monkeypatch.setattr(db, "SessionLocal", real_sessions)
    assert count_predictions() == 0
    payload = json.loads((db.PREDICTION_EXPORT_DIR / "2026-10-27.ndjson").read_text())
    assert payload["is_valid"] is False and payload["usable_for_prediction"] is False
    assert payload["persistence_status"] == "uncommitted"
    assert payload["evidence_role"] == "unissued_prediction_attempt"


def test_commit_success_refresh_failure_does_not_claim_sqlite_was_uncommitted(store, monkeypatch):
    s = source()
    retain(s)
    real_sessions = db.SessionLocal

    class FailingRefresh:
        def __init__(self):
            self.inner = real_sessions()

        def __getattr__(self, key):
            return getattr(self.inner, key)

        def refresh(self, value):
            raise RuntimeError("offline injected refresh failure after commit")

    monkeypatch.setattr(db, "SessionLocal", FailingRefresh)
    assert db.save_prediction_snapshot(prediction(s), "backend_periodic") is None
    monkeypatch.setattr(db, "SessionLocal", real_sessions)
    assert count_predictions() == 1
    payload = json.loads((db.PREDICTION_EXPORT_DIR / "2026-10-27.ndjson").read_text())
    assert payload["persistence_status"] == "committed_publication_unavailable"
    assert payload["usable_for_prediction"] is False


def test_old_unbound_duplicate_is_preserved_but_never_returned_as_new_success(store):
    s = source()
    retain(s)
    p = prediction(s)
    historic_source = {**s, "calculation_id": None}
    encoded = json.dumps(historic_source)
    with db.SessionLocal.begin() as session:
        session.add(db.PredictionSnapshot(
            symbol="SPX", timestamp_utc=datetime(2026, 10, 27, 15, 0, 1),
            trading_date=datetime(2026, 10, 27).date(), provider="databento",
            model_version=p["model_version"], prediction_mode="backend_periodic",
            is_valid=True, current_price=100.0, predicted_close=101.0,
            source_payload_json=encoded, subscription_epoch_id="e" * 64,
            subscription_generation=2, quote_timestamp_utc=datetime(2026, 10, 27, 14, 59, 59),
        ))
    assert db.save_prediction_snapshot(p, "backend_periodic") is None
    with db.SessionLocal() as session:
        record = session.query(db.PredictionSnapshot).one()
        assert record.source_payload_json == encoded and record.predicted_close == 101.0
    assert not db.PREDICTION_EXPORT_DIR.exists()
