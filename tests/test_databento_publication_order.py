"""Regression coverage for lifecycle reads racing calculation input commits."""

from datetime import datetime, timezone
import json

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import backend.database as backend_database
import database as root_database
from backend.databento_streamer import DatabentoGammaStreamer


@pytest.fixture
def publication_case(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'publication.db'}")
    backend_database.Base.metadata.create_all(
        engine,
        tables=[
            backend_database.GammaCalculationRun.__table__,
            backend_database.GammaCalculationInputBlob.__table__,
        ],
    )
    monkeypatch.setattr(backend_database, "SessionLocal", sessionmaker(bind=engine))
    monkeypatch.setattr(backend_database, "save_market_snapshot", lambda *a, **kw: None)
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(root_database, "save_gamma_snapshot", lambda *a, **kw: None)
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 100.0)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    streamer.update_interval = 0
    streamer.snapshot_interval = 60
    streamer.exports_dir = tmp_path / "exports"
    streamer.audit_dir = tmp_path / "audit"
    result = {
        "symbol": "SPX",
        "timestamp": datetime.now(timezone.utc),
        "price": 7500.0,
        "gamma_pin": 7510.0,
        "likely_close": 7510.0,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "validation_failure_reasons": [],
        "subscription_epoch_id": streamer.subscription_epoch_id,
        "subscription_generation": 1,
        "calculation_id": "11111111-1111-1111-1111-111111111111",
    }
    result["_calculation_inputs"] = {
        "calculation_id": result["calculation_id"],
        "subscription_epoch_id": streamer.subscription_epoch_id,
        "subscription_generation": 1,
        "rows": [{"strike": 7500.0}],
    }
    monkeypatch.setattr(streamer, "_calculate_pin", lambda *a, **kw: result)
    yield streamer, result
    engine.dispose()


@pytest.mark.parametrize("valid", [True, False])
def test_calculation_is_invisible_until_run_and_inputs_commit(
    publication_case, monkeypatch, valid
):
    streamer, result = publication_case
    result["validation_is_valid"] = valid
    result["gamma_excluded_from_model"] = not valid
    original_save = backend_database.save_gamma_calculation_inputs
    inspected = []

    def save_with_concurrent_lifecycle_read(run, inputs):
        # This is the exact read boundary the independently polling lifecycle
        # previously reached before save_gamma_calculation_inputs returned.
        assert "SPX" not in streamer.get_all_latest()
        assert "SPX" not in streamer.latest_pins
        metadata = original_save(run, inputs)
        assert metadata is not None
        assert backend_database.load_gamma_calculation_inputs(run["calculation_id"]) == inputs
        assert "SPX" not in streamer.get_all_latest()
        inspected.append(run["calculation_id"])
        return metadata

    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", save_with_concurrent_lifecycle_read)
    streamer._compute_and_publish()

    assert inspected == [result["calculation_id"]]
    assert streamer.get_all_latest()["SPX"]["calculation_id"] == result["calculation_id"]
    assert backend_database.load_gamma_calculation_inputs(result["calculation_id"]) == result["_calculation_inputs"]


@pytest.mark.parametrize("failure", ["none", "exception", "wrong_id"])
def test_failed_input_persistence_publishes_only_unusable_diagnostics(
    publication_case, monkeypatch, failure
):
    streamer, result = publication_case

    def fail_save(*args):
        if failure == "exception":
            raise RuntimeError("simulated disk failure")
        if failure == "wrong_id":
            return {"calculation_id": "different-calculation", "payload_sha256": "a" * 64}
        return None

    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", fail_save)
    audits = []
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audits.append)
    monkeypatch.setattr(
        backend_database,
        "save_market_snapshot",
        lambda *a, **kw: pytest.fail("failed persistence wrote usable market snapshot"),
    )
    monkeypatch.setattr(
        root_database,
        "save_gamma_snapshot",
        lambda *a, **kw: pytest.fail("failed persistence wrote usable gamma snapshot"),
    )
    streamer.callbacks.append(lambda value: pytest.fail("failed persistence reached prediction callback"))
    streamer._compute_and_publish()

    assert "SPX" not in streamer.latest_pins
    payload = streamer.get_all_latest()["SPX"]
    assert not payload.get("calculation_id")
    assert payload["validation_is_valid"] is False
    assert payload["usable_for_prediction"] is False
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" in payload["validation_failure_reasons"]
    assert backend_database.load_gamma_calculation_inputs(result["calculation_id"]) is None
    assert not list(streamer.exports_dir.rglob("*.ndjson"))
    disk_audits = [json.loads(path.read_text()) for path in streamer.audit_dir.rglob("*.json")]
    assert len(audits) == len(disk_audits) == 1
    for audit in [*audits, *disk_audits]:
        assert not audit.get("calculation_id")
        assert audit["validation_is_valid"] is False
        assert audit["validation_status"] == "invalid"
        assert audit["gamma_excluded_from_model"] is True
        assert audit["usable_for_prediction"] is False
        assert "CALCULATION_INPUT_PERSISTENCE_FAILED" in audit["validation_failure_reasons"]


def test_failed_due_capture_retries_before_success_throttle_and_stays_abstained(
    publication_case, monkeypatch
):
    streamer, template = publication_case
    monotonic_clock = [100.0]
    monkeypatch.setattr(
        "backend.databento_streamer.time.monotonic",
        lambda: monotonic_clock[0],
    )
    capture_flags = []
    generated_inputs = []
    calculation_sequence = [
        "21111111-1111-1111-1111-111111111111",
        "31111111-1111-1111-1111-111111111111",
    ]

    def calculate(_market, *, capture_inputs, capture_invalid_inputs):
        capture_flags.append(capture_inputs)
        result = {
            key: value
            for key, value in template.items()
            if key not in {"calculation_id", "_calculation_inputs"}
        }
        if capture_inputs:
            calculation_id = calculation_sequence[len(capture_flags) - 1]
            result["calculation_id"] = calculation_id
            result["_calculation_inputs"] = {
                "calculation_id": calculation_id,
                "subscription_epoch_id": streamer.subscription_epoch_id,
                "subscription_generation": 1,
                "rows": [{"strike": 7500.0}],
            }
            generated_inputs.append(result["_calculation_inputs"])
        return result

    original_save = backend_database.save_gamma_calculation_inputs
    save_attempts = []

    def fail_once_then_commit(run, inputs):
        save_attempts.append(run["calculation_id"])
        if len(save_attempts) == 1:
            return None
        return original_save(run, inputs)

    monkeypatch.setattr(streamer, "_calculate_pin", calculate)
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        fail_once_then_commit,
    )

    streamer._compute_and_publish()

    first = streamer.get_all_latest()["SPX"]
    assert first["validation_is_valid"] is False
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" in first[
        "validation_failure_reasons"
    ]
    assert "SPX" not in streamer._last_snapshot_write

    monotonic_clock[0] = 105.0
    streamer._compute_and_publish()

    assert capture_flags == [True, True]
    assert save_attempts == calculation_sequence
    assert streamer._last_snapshot_write["SPX"] == 105.0
    second = streamer.get_all_latest()["SPX"]
    assert second["validation_is_valid"] is True
    assert second["calculation_id"] == calculation_sequence[1]
    assert backend_database.load_gamma_calculation_inputs(
        calculation_sequence[1]
    ) == generated_inputs[1]


def test_due_capture_without_lineage_is_diagnostic_and_retries(
    publication_case, monkeypatch
):
    streamer, template = publication_case
    result = {
        key: value
        for key, value in template.items()
        if key not in {"calculation_id", "_calculation_inputs"}
    }
    monkeypatch.setattr(streamer, "_calculate_pin", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        lambda *_args: pytest.fail("lineage-free capture reached persistence"),
    )

    streamer._compute_and_publish()

    payload = streamer.get_all_latest()["SPX"]
    assert payload["validation_is_valid"] is False
    assert payload["usable_for_prediction"] is False
    assert "CALCULATION_INPUT_CAPTURE_MISSING" in payload[
        "validation_failure_reasons"
    ]
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" not in payload[
        "validation_failure_reasons"
    ]
    assert "SPX" not in streamer._last_snapshot_write


def test_already_invalid_due_result_keeps_source_reason_without_false_persistence(
    publication_case, monkeypatch
):
    streamer, template = publication_case
    result = {
        key: value
        for key, value in template.items()
        if key not in {"calculation_id", "_calculation_inputs"}
    }
    result["validation_is_valid"] = False
    result["gamma_excluded_from_model"] = True
    result["usable_for_prediction"] = False
    result["validation_failure_reasons"] = ["PRIMARY_PAIR_COVERAGE_LOW"]
    monkeypatch.setattr(streamer, "_calculate_pin", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        lambda *_args: pytest.fail("already-invalid result reached valid-input persistence"),
    )

    streamer._compute_and_publish()

    payload = streamer.get_all_latest()["SPX"]
    assert payload["validation_is_valid"] is False
    assert payload["validation_failure_reasons"] == ["PRIMARY_PAIR_COVERAGE_LOW"]
    assert "CALCULATION_INPUT_CAPTURE_MISSING" not in payload[
        "validation_failure_reasons"
    ]
    assert "CALCULATION_INPUT_PERSISTENCE_FAILED" not in payload[
        "validation_failure_reasons"
    ]


def test_failed_invalid_due_input_persistence_strips_lineage_without_retrying(
    publication_case, monkeypatch
):
    streamer, template = publication_case
    monotonic_clock = [100.0]
    monkeypatch.setattr(
        "backend.databento_streamer.time.monotonic",
        lambda: monotonic_clock[0],
    )
    capture_flags = []
    save_attempts = []
    calculation_id = "41111111-1111-1111-1111-111111111111"

    def calculate(_market, *, capture_inputs, capture_invalid_inputs):
        capture_flags.append((capture_inputs, capture_invalid_inputs))
        result = {
            key: value
            for key, value in template.items()
            if key not in {"calculation_id", "_calculation_inputs"}
        }
        result["validation_is_valid"] = False
        result["gamma_excluded_from_model"] = True
        result["usable_for_prediction"] = False
        result["validation_failure_reasons"] = ["PRIMARY_PAIR_COVERAGE_LOW"]
        if capture_invalid_inputs:
            result["calculation_id"] = calculation_id
            result["_calculation_inputs"] = {
                "calculation_id": calculation_id,
                "subscription_epoch_id": streamer.subscription_epoch_id,
                "subscription_generation": 1,
                "rows": [{"strike": 7500.0}],
            }
        return result

    def fail_save(run, _inputs):
        save_attempts.append(run["calculation_id"])
        return None

    audits = []
    monkeypatch.setattr(streamer, "_calculate_pin", calculate)
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        fail_save,
    )
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audits.append)

    streamer._compute_and_publish()

    first = streamer.get_all_latest()["SPX"]
    assert not first.get("calculation_id")
    assert first["validation_failure_reasons"] == [
        "PRIMARY_PAIR_COVERAGE_LOW",
        "CALCULATION_INPUT_PERSISTENCE_FAILED",
    ]
    assert len(audits) == 1
    assert not audits[0].get("calculation_id")
    assert audits[0]["validation_failure_reasons"] == first[
        "validation_failure_reasons"
    ]
    assert streamer._last_invalid_snapshot_write["SPX"] == 100.0
    assert "SPX" not in streamer._last_snapshot_write

    monotonic_clock[0] = 105.0
    streamer._compute_and_publish()

    assert capture_flags == [(True, True), (True, False)]
    assert save_attempts == [calculation_id]
    assert streamer._last_invalid_snapshot_write["SPX"] == 100.0


def test_due_capture_with_partial_lineage_strips_uncommitted_id(
    publication_case, monkeypatch
):
    streamer, template = publication_case
    result = dict(template)
    result.pop("_calculation_inputs")
    audits = []
    monkeypatch.setattr(streamer, "_calculate_pin", lambda *_args, **_kwargs: result)
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        lambda *_args: pytest.fail("partial lineage reached persistence"),
    )
    monkeypatch.setattr(root_database, "save_audit_snapshot_to_db", audits.append)

    streamer._compute_and_publish()

    payload = streamer.get_all_latest()["SPX"]
    assert not payload.get("calculation_id")
    assert payload["validation_failure_reasons"] == [
        "CALCULATION_INPUT_CAPTURE_MISSING"
    ]
    assert len(audits) == 1
    assert not audits[0].get("calculation_id")
    assert audits[0]["validation_failure_reasons"] == payload[
        "validation_failure_reasons"
    ]
    assert "SPX" not in streamer._last_snapshot_write


def test_handoff_during_commit_does_not_publish_previous_generation(
    publication_case, monkeypatch
):
    streamer, result = publication_case
    original_save = backend_database.save_gamma_calculation_inputs

    def commit_then_handoff(run, inputs):
        metadata = original_save(run, inputs)
        streamer.active_generation = 2
        return metadata

    monkeypatch.setattr(backend_database, "save_gamma_calculation_inputs", commit_then_handoff)
    streamer._compute_and_publish()

    assert "SPX" not in streamer.get_all_latest()
    assert backend_database.load_gamma_calculation_inputs(result["calculation_id"]) is not None


def test_noncapture_cycle_keeps_existing_publication_without_claiming_lineage(
    publication_case, monkeypatch
):
    streamer, result = publication_case
    result.pop("calculation_id")
    result.pop("_calculation_inputs")
    streamer._last_snapshot_write["SPX"] = 99.0
    monkeypatch.setattr(
        backend_database,
        "save_gamma_calculation_inputs",
        lambda *a: pytest.fail("noncapture cycle tried to persist inputs"),
    )
    streamer._compute_and_publish()

    payload = streamer.latest_pins["SPX"]
    assert "calculation_id" not in payload
    assert payload["validation_is_valid"] is True
