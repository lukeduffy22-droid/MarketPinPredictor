"""Synthetic, offline fixtures for the read-only lineage and coverage CLI."""
import hashlib
import json
from pathlib import Path
import sqlite3
import zlib

import pytest

from tools.audit_calculation_lineage import (
    MAX_BLOB_BYTES, audit_database, canonical, digest, main, write_assessment,
)

START = "2026-10-27T13:30:00Z"
END = "2026-10-27T13:33:00Z"
EPOCH = "a" * 64


@pytest.fixture
def retained(tmp_path):
    folder = tmp_path / "source-data"
    folder.mkdir()
    path = folder / "market.sqlite"
    c = sqlite3.connect(path)
    c.executescript("""
        CREATE TABLE prediction_snapshots (
            id INTEGER PRIMARY KEY, symbol TEXT, trading_date TEXT,
            timestamp_utc TEXT, model_version TEXT, provider TEXT,
            subscription_epoch_id TEXT, subscription_generation INTEGER,
            source_payload_json TEXT);
        CREATE INDEX day_predictions ON prediction_snapshots(trading_date,symbol);
        CREATE TABLE prediction_passports (
            passport_id TEXT PRIMARY KEY, prediction_snapshot_id INTEGER,
            symbol TEXT, prediction_timestamp_utc TEXT, target_kind TEXT,
            target_trading_date TEXT, model_version TEXT,
            subscription_epoch_id TEXT, subscription_generation INTEGER,
            calculation_id TEXT, calculation_input_sha256 TEXT);
        CREATE TABLE gamma_calculation_runs (
            id INTEGER PRIMARY KEY, calculation_id TEXT, symbol TEXT,
            trading_date TEXT, calculated_at_utc TEXT, provider TEXT,
            status TEXT, subscription_epoch_id TEXT, subscription_generation INTEGER);
        CREATE INDEX day_runs ON gamma_calculation_runs(trading_date,symbol);
        CREATE TABLE gamma_calculation_input_blobs (
            calculation_run_id INTEGER PRIMARY KEY, encoding TEXT,
            payload_sha256 TEXT, uncompressed_bytes INTEGER,
            compressed_bytes INTEGER, payload BLOB);
    """)
    stamp = "2026-10-27 13:30:10.000000"
    payload = {
        "input_schema_version": "gamma-inputs-v2-point-in-time",
        "calculation_id": "calc-1", "symbol": "SPX", "calculated_at_utc": stamp,
        "subscription_epoch_id": EPOCH, "subscription_generation": 1,
        "parameters": {}, "rejection_counts": {},
        "raw_fresh_chain_rows": [{"symbol": "SPXW  261027C07700000"}],
        "calculated_gex_rows": [{"symbol": "SPXW  261027C07700000"}],
        "output_summary": {"calculation_id": "calc-1", "symbol": "SPX",
                           "subscription_epoch_id": EPOCH, "subscription_generation": 1,
                           "validation_is_valid": True, "gamma_excluded_from_model": False},
    }
    raw = canonical(payload)
    hashed = hashlib.sha256(raw).hexdigest()
    compressed = zlib.compress(raw)
    c.execute("INSERT INTO gamma_calculation_runs VALUES (?,?,?,?,?,?,?,?,?)",
              (1, "calc-1", "SPX", "2026-10-27", stamp, "databento", "valid", EPOCH, 1))
    c.execute("INSERT INTO gamma_calculation_runs VALUES (?,?,?,?,?,?,?,?,?)",
              (2, "invalid-1", "SPX", "2026-10-27", "2026-10-27 13:31:10.000000", "databento", "invalid", EPOCH, 1))
    c.execute("INSERT INTO gamma_calculation_input_blobs VALUES (?,?,?,?,?,?)",
              (1, "canonical-json+zlib-v1", hashed, len(raw), len(compressed), compressed))
    c.execute("INSERT INTO prediction_snapshots VALUES (?,?,?,?,?,?,?,?,?)",
              (11, "SPX", "2026-10-27", "2026-10-27 13:30:11.000000", "model-1", "databento", EPOCH, 1,
               json.dumps({"calculation_id": "calc-1", "symbol": "SPX",
                           "subscription_epoch_id": EPOCH, "subscription_generation": 1})))
    c.execute("INSERT INTO prediction_snapshots VALUES (?,?,?,?,?,?,?,?,?)",
              (12, "SPX", "2026-10-27", "2026-10-27 13:31:11.000000", "model-1", "databento", EPOCH, 1, "{}"))
    c.execute("INSERT INTO prediction_passports VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              ("pass-1", 11, "SPX", "2026-10-27 13:30:11.000000", "official_cash_close", "2026-10-27", "model-1", EPOCH, 1, "calc-1", hashed))
    c.execute("INSERT INTO prediction_passports VALUES (?,?,?,?,?,?,?,?,?,?,?)",
              ("pass-2", 12, "SPX", "2026-10-27 13:31:11.000000", "official_cash_close", "2026-10-27", "model-1", EPOCH, 1, None, None))
    c.commit()
    c.close()
    return path


def audit(path, **kwargs):
    return audit_database(path, "2026-10-27", START, END, ["SPX"], **kwargs)


def mutate(path, sql, args=()):
    with sqlite3.connect(path) as c:
        c.execute(sql, args)


def test_exact_lineage_and_missing_invalid_minute_states_are_separate(retained):
    before = retained.read_bytes()
    result = audit(retained)
    assert result["complete"] and result["status"] == "COMPLETE"
    assert result["prediction_lineage_summary"]["SPX"] == {"VERIFIED_LINEAGE": 1, "UNRESOLVED_LINEAGE": 1}
    assert result["minute_status_summary"]["SPX"] == {"VALID_RUN_PRESENT": 1, "INVALID_ONLY": 1, "NO_RUN": 1}
    assert result["predictions"][1]["calculation_id"] is None
    assert "SOURCE_CALCULATION_ID_MISSING" in result["predictions"][1]["reasons"]
    assert result["training_eligibility"] == result["economic_capture_completeness"] == "NOT_ASSESSED"
    assert retained.read_bytes() == before


def test_missing_id_cannot_be_repaired_by_nearby_calculation(retained):
    result = audit(retained)
    missing = next(r for r in result["predictions"] if r["prediction_id"] == 12)
    assert missing["status"] == "UNRESOLVED_LINEAGE"
    assert missing["calculation_id"] is None
    assert "CALCULATION_RUN_MISSING_OR_AMBIGUOUS" in missing["reasons"]


@pytest.mark.parametrize("sql,reason", [
    ("UPDATE prediction_snapshots SET model_version=NULL WHERE id=11", "PREDICTION_MODEL_IDENTITY_MISSING"),
    ("UPDATE prediction_snapshots SET subscription_epoch_id=NULL WHERE id=11", "PREDICTION_SUBSCRIPTION_IDENTITY_INVALID"),
    ("UPDATE prediction_snapshots SET subscription_generation=0 WHERE id=11", "PREDICTION_SUBSCRIPTION_IDENTITY_INVALID"),
    ("UPDATE prediction_passports SET symbol='NDX' WHERE passport_id='pass-1'", "PASSPORT_MISSING_OR_AMBIGUOUS"),
    ("UPDATE prediction_passports SET calculation_input_sha256='wrong' WHERE passport_id='pass-1'", "PASSPORT_INPUT_HASH_MISMATCH"),
    ("UPDATE gamma_calculation_runs SET subscription_generation=2 WHERE id=1", "CALCULATION_RUN_IDENTITY_OR_STATUS_MISMATCH"),
    ("UPDATE gamma_calculation_runs SET calculated_at_utc='2026-10-27 13:30:12' WHERE id=1", "CALCULATION_AFTER_ISSUANCE"),
    ("UPDATE gamma_calculation_input_blobs SET payload_sha256='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' WHERE calculation_run_id=1", "CALCULATION_BLOB_HASH_MISMATCH"),
    ("DELETE FROM gamma_calculation_input_blobs WHERE calculation_run_id=1", "CALCULATION_BLOB_MISSING"),
])
def test_identity_time_hash_and_missing_blob_fail_closed(retained, sql, reason):
    mutate(retained, sql)
    first = audit(retained)["predictions"][0]
    assert first["status"] == "UNRESOLVED_LINEAGE"
    assert reason in first["reasons"]


def test_ambiguous_passport_does_not_multiply_or_admit_prediction(retained):
    mutate(retained, "INSERT INTO prediction_passports SELECT 'duplicate',prediction_snapshot_id,symbol,prediction_timestamp_utc,target_kind,target_trading_date,model_version,subscription_epoch_id,subscription_generation,calculation_id,calculation_input_sha256 FROM prediction_passports WHERE passport_id='pass-1'")
    result = audit(retained)
    assert len(result["predictions"]) == 2
    assert "PASSPORT_MISSING_OR_AMBIGUOUS" in result["predictions"][0]["reasons"]


def test_oversized_blob_is_not_decompressed(retained):
    mutate(retained, "UPDATE gamma_calculation_input_blobs SET uncompressed_bytes=?", (MAX_BLOB_BYTES + 1,))
    assert "CALCULATION_BLOB_SIZE_INVALID" in audit(retained)["predictions"][0]["reasons"]


def test_resource_ceiling_and_missing_schema_return_no_partial_success(retained):
    result = audit(retained, max_rows=1)
    assert not result["complete"] and result["blocked_reason"].startswith("ROW_BUDGET_EXCEEDED")
    assert "predictions" not in result and "calculation_minutes" not in result
    mutate(retained, "DROP TABLE prediction_passports")
    result = audit(retained)
    assert not result["complete"] and "REQUIRED_TABLE_MISSING" in result["blocked_reason"]


def test_whole_operation_deadline_returns_explicit_blocked(retained, monkeypatch):
    values = iter([0, 100])
    monkeypatch.setattr("tools.audit_calculation_lineage.time.monotonic", lambda: next(values, 100))
    result = audit(retained, max_seconds=1)
    assert not result["complete"] and result["blocked_reason"] == "QUERY_BUDGET_EXCEEDED"
    assert "predictions" not in result


def test_attempt_receipts_do_not_create_valid_calculations_or_copy_prices(retained, tmp_path):
    attempts = tmp_path / "attempt-source" / "attempts.ndjson"
    attempts.parent.mkdir()
    attempts.write_text(json.dumps({"symbol": "SPX", "generated_at_utc": "2026-10-27T13:32:10Z",
                                    "price": 99999, "validation_is_valid": False,
                                    "validation_failure_reasons": ["COMPUTE_SUSPENDED"]}) + "\n")
    result = audit(retained, capture_attempts=[attempts])
    assert result["minute_status_summary"]["SPX"]["NO_RUN"] == 1
    assert result["retained_attempts"][0]["reasons"] == ["COMPUTE_SUSPENDED"]
    assert "price" not in result["retained_attempts"][0]


def test_malformed_attempt_does_not_leave_partial_success_cohort(retained, tmp_path):
    attempts = tmp_path / "bad.ndjson"
    attempts.write_text("not json\n")
    result = audit(retained, capture_attempts=[attempts])
    assert not result["complete"] and "ATTEMPT_RECORD_MALFORMED" in result["blocked_reason"]
    assert "predictions" not in result


@pytest.mark.parametrize("raw,reason", [(b"[]\n", "ATTEMPT_RECORD_MALFORMED"), (b"\xff", "ATTEMPT_FILE_ENCODING_INVALID")])
def test_malformed_attempt_shapes_and_encoding_are_explicit(retained, tmp_path, raw, reason):
    attempts = tmp_path / "bad-shape.ndjson"
    attempts.write_bytes(raw)
    result = audit(retained, capture_attempts=[attempts])
    assert not result["complete"] and reason in result["blocked_reason"]


def test_output_manifest_is_hash_bound_exclusive_and_external(retained, tmp_path):
    result = audit(retained)
    output = tmp_path / "audit-output"
    manifest = write_assessment(output, result, source_database=retained)
    file = manifest["files"][0]
    assert hashlib.sha256((output / file["path"]).read_bytes()).hexdigest() == file["sha256"]
    assert manifest["manifest_sha256"] == digest({k: v for k, v in manifest.items() if k != "manifest_sha256"})
    assert result["report_sha256"] == digest({k: v for k, v in result.items() if k != "report_sha256"})
    with pytest.raises(FileExistsError):
        write_assessment(output, result, source_database=retained)
    with pytest.raises(ValueError, match="outside"):
        write_assessment(retained.parent / "audit", result, source_database=retained)


def test_cli_requires_aware_utc_and_minute_bounds(retained, tmp_path):
    for start in ["2026-10-27T13:30:00", "2026-10-27T08:30:00-05:00", "2026-10-27T13:30:01Z"]:
        with pytest.raises(ValueError):
            audit_database(retained, "2026-10-27", start, END, ["SPX"])
    assert main(["--database", str(retained), "--trading-date", "2026-10-27", "--start-utc", START,
                 "--end-utc", END, "--symbols", "SPX", "--output", str(tmp_path / "cli-output")]) == 0


def test_wal_source_is_read_without_checkpointing_or_altering_records(retained):
    writer = sqlite3.connect(retained)
    try:
        assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        writer.execute("UPDATE prediction_snapshots SET model_version='changed' WHERE id=12")
        writer.commit()
        wal = Path(str(retained) + "-wal")
        before = (retained.read_bytes(), wal.read_bytes())
        result = audit(retained)
        assert result["complete"]
        assert (retained.read_bytes(), wal.read_bytes()) == before
    finally:
        writer.close()
