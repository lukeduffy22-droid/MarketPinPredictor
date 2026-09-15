from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import sessionmaker

import backend.database as database


def _isolated_store(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'gamma-inputs.db'}", connect_args={"check_same_thread": False})
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr(database, "SessionLocal", session_factory)
    database.Base.metadata.create_all(bind=engine)
    database._ensure_capture_epoch_columns()
    database._ensure_gamma_calculation_immutability()
    return engine, session_factory


def test_gamma_input_capture_round_trips_and_is_append_only(tmp_path, monkeypatch):
    engine, session_factory = _isolated_store(tmp_path, monkeypatch)
    run = {
        "calculation_id": "11111111-1111-1111-1111-111111111111",
        "symbol": "SPX",
        "calculated_at_utc": datetime(2026, 8, 24, 14, 30, tzinfo=timezone.utc),
        "provider": "databento",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 2,
        "status": "valid",
        "formula_version": "test-gex-v1",
        "target_formula_version": "test-target-v1",
        "spot_formula_version": "test-spot-v1",
        "risk_free_rate": 0.05,
        "contract_multiplier": 100.0,
        "spot_price": 7500.0,
        "gamma_pin": 7510.0,
        "selected_target": 7508.0,
        "primary_expiration_target": 7510.0,
        "multi_expiration_target": 7508.0,
        "chain_row_count": 2,
        "gex_row_count": 2,
    }
    inputs = {
        "input_schema_version": "gamma-inputs-v1",
        "subscription_epoch_id": "e" * 64,
        "subscription_generation": 2,
        "raw_fresh_chain_rows": [
            {"symbol": "SPXW  260824C07500000", "strike": 7500.0, "option_type": "C", "mid": 12.5},
            {"symbol": "SPXW  260824P07500000", "strike": 7500.0, "option_type": "P", "mid": 11.5},
        ],
        "parameters": {"risk_free_rate": 0.05},
    }

    metadata = database.save_gamma_calculation_inputs(run, inputs)
    assert metadata is not None
    assert metadata["compressed_bytes"] < metadata["uncompressed_bytes"]
    assert database.load_gamma_calculation_inputs(run["calculation_id"]) == inputs

    with engine.begin() as connection:
        for statement in (
            "UPDATE gamma_calculation_runs SET subscription_epoch_id = :epoch",
            "DELETE FROM gamma_calculation_runs",
            "UPDATE gamma_calculation_input_blobs SET payload_sha256 = :epoch",
            "DELETE FROM gamma_calculation_input_blobs",
        ):
            with pytest.raises(DatabaseError):
                connection.execute(text(statement), {"epoch": "f" * 64})

    assert database.save_gamma_calculation_inputs(
        run,
        {
            "input_schema_version": "gamma-inputs-v1",
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 2,
            "changed": True,
        },
    ) is None
    session = session_factory()
    try:
        assert session.query(database.GammaCalculationRun).count() == 1
        assert session.query(database.GammaCalculationInputBlob).count() == 1
    finally:
        session.close()
        engine.dispose()


def test_gamma_input_capture_rejects_non_finite_json(tmp_path, monkeypatch):
    engine, session_factory = _isolated_store(tmp_path, monkeypatch)
    result = database.save_gamma_calculation_inputs(
        {
            "calculation_id": "22222222-2222-2222-2222-222222222222",
            "symbol": "SPX",
            "provider": "databento",
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 2,
            "status": "invalid",
            "formula_version": "test",
            "chain_row_count": 1,
            "gex_row_count": 0,
        },
        {
            "input_schema_version": "gamma-inputs-v1",
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 2,
            "bad": float("nan"),
        },
    )

    assert result is None
    session = session_factory()
    try:
        assert session.query(database.GammaCalculationRun).count() == 0
        assert session.query(database.GammaCalculationInputBlob).count() == 0
    finally:
        session.close()
        engine.dispose()


def test_databento_gamma_input_capture_rejects_generation_mismatch(
    tmp_path, monkeypatch
):
    engine, session_factory = _isolated_store(tmp_path, monkeypatch)
    result = database.save_gamma_calculation_inputs(
        {
            "calculation_id": "33333333-3333-3333-3333-333333333333",
            "symbol": "SPX",
            "provider": "databento",
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 2,
            "status": "valid",
            "formula_version": "test",
        },
        {
            "input_schema_version": "gamma-inputs-v2-point-in-time",
            "subscription_epoch_id": "e" * 64,
            "subscription_generation": 3,
            "rows": [],
        },
    )

    assert result is None
    session = session_factory()
    try:
        assert session.query(database.GammaCalculationRun).count() == 0
        assert session.query(database.GammaCalculationInputBlob).count() == 0
    finally:
        session.close()
        engine.dispose()
