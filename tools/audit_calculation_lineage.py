"""Bounded, read-only calculation lineage and cycle-coverage evidence.

This command imports no application modules, creates no schema and performs no
repair. It reports retained calculation opportunities, not economic feed
completeness or training eligibility. UTC database columns follow their declared
UTC storage contract, including the application's offset-free SQLite values.
CLI window timestamps must carry an explicit UTC offset.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import time
import zlib

SCHEMA = "marketpin-calculation-lineage-audit.v1"
MAX_BLOB_BYTES = 8 * 1024 * 1024
MAX_RESULT_BYTES = 128 * 1024 * 1024
REQUIRED_COLUMNS = {
    "prediction_snapshots": {
        "id", "symbol", "trading_date", "timestamp_utc", "model_version",
        "provider", "subscription_epoch_id", "subscription_generation", "source_payload_json",
    },
    "prediction_passports": {
        "passport_id", "prediction_snapshot_id", "symbol", "prediction_timestamp_utc",
        "target_kind", "target_trading_date", "model_version", "subscription_epoch_id",
        "subscription_generation", "calculation_id", "calculation_input_sha256",
    },
    "gamma_calculation_runs": {
        "id", "calculation_id", "symbol", "trading_date", "calculated_at_utc",
        "provider", "status", "subscription_epoch_id", "subscription_generation",
    },
    "gamma_calculation_input_blobs": {
        "calculation_run_id", "encoding", "payload_sha256", "uncompressed_bytes",
        "compressed_bytes", "payload",
    },
}


class AuditBlocked(Exception):
    """A resource or evidence boundary prevents a complete assessment."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def utc(value, *, allow_naive=False):
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        if not allow_naive:
            raise ValueError("UTC window timestamps require an explicit offset")
        parsed = parsed.replace(tzinfo=timezone.utc)
    if not allow_naive and parsed.utcoffset() != timedelta(0):
        raise ValueError("Window timestamps must be expressed in UTC")
    return parsed.astimezone(timezone.utc)


def db_utc(value):
    """Normalize a declared *_utc field, never the host's local timezone."""
    return utc(value, allow_naive=True)


def iso(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def file_identity(path):
    stat = path.stat()
    return {"path": str(path), "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}


def _blob_payload(blob):
    if blob is None:
        return None, "CALCULATION_BLOB_MISSING"
    if blob["encoding"] != "canonical-json+zlib-v1":
        return None, "CALCULATION_BLOB_ENCODING_UNSUPPORTED"
    if (type(blob["uncompressed_bytes"]) is not int or type(blob["compressed_bytes"]) is not int
            or not 0 < blob["uncompressed_bytes"] <= MAX_BLOB_BYTES
            or not 0 < blob["compressed_bytes"] <= MAX_BLOB_BYTES):
        return None, "CALCULATION_BLOB_SIZE_INVALID"
    if not re.fullmatch(r"[0-9a-f]{64}", str(blob["payload_sha256"] or "")):
        return None, "CALCULATION_BLOB_HASH_INVALID"
    try:
        compressed = bytes(blob["payload"])
        if len(compressed) != blob["compressed_bytes"]:
            return None, "CALCULATION_BLOB_SIZE_MISMATCH"
        decoder = zlib.decompressobj()
        raw = decoder.decompress(compressed, MAX_BLOB_BYTES + 1)
        if (len(raw) > MAX_BLOB_BYTES or not decoder.eof or decoder.unused_data
                or decoder.unconsumed_tail or len(raw) != blob["uncompressed_bytes"]):
            return None, "CALCULATION_BLOB_STREAM_INVALID"
        if hashlib.sha256(raw).hexdigest() != blob["payload_sha256"]:
            return None, "CALCULATION_BLOB_HASH_MISMATCH"
        payload = json.loads(raw)
        if not isinstance(payload, dict) or canonical(payload) != raw:
            return None, "CALCULATION_BLOB_NONCANONICAL"
        return payload, None
    except (TypeError, ValueError, UnicodeDecodeError, zlib.error, OverflowError):
        return None, "CALCULATION_BLOB_MALFORMED"


def assess_prediction(prediction, passports, runs_by_id, blobs_by_run):
    """Resolve exact identities only; no timestamp-nearest or historical repair."""
    reasons = []
    if not isinstance(prediction["model_version"], str) or not prediction["model_version"].strip():
        reasons.append("PREDICTION_MODEL_IDENTITY_MISSING")
    if (not re.fullmatch(r"[0-9a-f]{64}", str(prediction["subscription_epoch_id"] or ""))
            or type(prediction["subscription_generation"]) is not int
            or prediction["subscription_generation"] <= 0):
        reasons.append("PREDICTION_SUBSCRIPTION_IDENTITY_INVALID")
    if prediction["provider"] != "databento":
        reasons.append("CALCULATION_PROVIDER_UNSUPPORTED")
    try:
        source = json.loads(prediction["source_payload_json"] or "{}")
        if not isinstance(source, dict):
            raise ValueError("payload not an object")
    except (TypeError, ValueError):
        source = {}
        reasons.append("SOURCE_PAYLOAD_MALFORMED")
    source_id = str(source.get("calculation_id") or "").strip()
    if not source_id:
        reasons.append("SOURCE_CALCULATION_ID_MISSING")
    if (source.get("symbol") != prediction["symbol"]
            or source.get("subscription_epoch_id") != prediction["subscription_epoch_id"]
            or source.get("subscription_generation") != prediction["subscription_generation"]):
        reasons.append("SOURCE_SUBSCRIPTION_IDENTITY_MISSING_OR_MISMATCH")
    exact = [p for p in passports if (
        p["prediction_snapshot_id"] == prediction["id"]
        and p["symbol"] == prediction["symbol"]
        and p["model_version"] == prediction["model_version"]
        and p["prediction_timestamp_utc"] == prediction["timestamp_utc"]
        and p["target_kind"] == "official_cash_close"
        and p["target_trading_date"] == prediction["trading_date"]
    )]
    if len(exact) != 1:
        reasons.append("PASSPORT_MISSING_OR_AMBIGUOUS")
        passport = None
    else:
        passport = exact[0]
        if not passport["calculation_id"]:
            reasons.append("PASSPORT_CALCULATION_ID_MISSING")
        elif passport["calculation_id"] != source_id:
            reasons.append("SOURCE_PASSPORT_CALCULATION_ID_MISMATCH")
        if not passport["calculation_input_sha256"]:
            reasons.append("PASSPORT_INPUT_HASH_MISSING")
    runs = runs_by_id.get(source_id, []) if source_id else []
    run = runs[0] if len(runs) == 1 else None
    payload_hash = None
    if run is None:
        reasons.append("CALCULATION_RUN_MISSING_OR_AMBIGUOUS")
    else:
        if (run["symbol"] != prediction["symbol"] or run["trading_date"] != prediction["trading_date"]
                or run["provider"] != prediction["provider"] or run["status"] != "valid"
                or run["subscription_epoch_id"] != prediction["subscription_epoch_id"]
                or run["subscription_generation"] != prediction["subscription_generation"]):
            reasons.append("CALCULATION_RUN_IDENTITY_OR_STATUS_MISMATCH")
        try:
            if db_utc(run["calculated_at_utc"]) > db_utc(prediction["timestamp_utc"]):
                reasons.append("CALCULATION_AFTER_ISSUANCE")
        except (ValueError, TypeError, OverflowError):
            reasons.append("CALCULATION_OR_ISSUANCE_TIME_INVALID")
        blob = blobs_by_run.get(run["id"])
        payload, failure = _blob_payload(blob)
        if failure:
            reasons.append(failure)
        else:
            payload_hash = blob["payload_sha256"]
            summary = payload.get("output_summary")
            summary = summary if isinstance(summary, dict) else {}
            if (payload.get("input_schema_version") != "gamma-inputs-v2-point-in-time"
                    or not isinstance(payload.get("parameters"), dict)
                    or not isinstance(payload.get("rejection_counts"), dict)
                    or not isinstance(payload.get("raw_fresh_chain_rows"), list)
                    or not payload.get("raw_fresh_chain_rows")
                    or not isinstance(payload.get("calculated_gex_rows"), list)
                    or not payload.get("calculated_gex_rows")
                    or payload.get("calculation_id") != source_id
                    or payload.get("symbol") != prediction["symbol"]
                    or payload.get("subscription_epoch_id") != prediction["subscription_epoch_id"]
                    or payload.get("subscription_generation") != prediction["subscription_generation"]
                    or summary.get("calculation_id") != source_id
                    or summary.get("symbol") != prediction["symbol"]
                    or summary.get("subscription_epoch_id") != prediction["subscription_epoch_id"]
                    or summary.get("subscription_generation") != prediction["subscription_generation"]
                    or summary.get("validation_is_valid") is not True
                    or summary.get("gamma_excluded_from_model") is not False):
                reasons.append("CALCULATION_INPUT_IDENTITY_OR_STATUS_MISMATCH")
            try:
                if db_utc(payload["calculated_at_utc"]) != db_utc(run["calculated_at_utc"]):
                    reasons.append("CALCULATION_INPUT_TIME_MISMATCH")
            except (KeyError, ValueError, TypeError, OverflowError):
                reasons.append("CALCULATION_INPUT_TIME_INVALID")
            if passport and passport["calculation_input_sha256"] != payload_hash:
                reasons.append("PASSPORT_INPUT_HASH_MISMATCH")
        if passport and (
            passport["subscription_epoch_id"] != prediction["subscription_epoch_id"]
            or passport["subscription_generation"] != prediction["subscription_generation"]
        ):
            reasons.append("PASSPORT_SUBSCRIPTION_IDENTITY_MISMATCH")
    return {"prediction_id": prediction["id"], "symbol": prediction["symbol"],
            "issued_at_utc": prediction["timestamp_utc"], "model_version": prediction["model_version"],
            "calculation_id": source_id or None, "input_sha256": payload_hash,
            "status": "VERIFIED_LINEAGE" if not reasons else "UNRESOLVED_LINEAGE",
            "reasons": list(dict.fromkeys(reasons))}


def audit_database(database, trading_date, start_utc, end_utc, symbols, *,
                   max_seconds=15.0, max_rows=100_000, capture_attempts=()):
    """Return one coherent snapshot or an explicit blocked/partial assessment."""
    path = Path(database).resolve()
    if not path.is_file():
        raise ValueError("Existing database file required")
    day = date.fromisoformat(str(trading_date)).isoformat()
    start, end = utc(start_utc), utc(end_utc)
    if end <= start or end - start > timedelta(days=1):
        raise ValueError("Window must be positive and at most one day")
    if start.second or start.microsecond or end.second or end.microsecond:
        raise ValueError("Window bounds must align to complete UTC minutes")
    selected = tuple(dict.fromkeys(str(s).strip().upper() for s in symbols))
    if not selected or len(selected) > 40 or any(not re.fullmatch(r"[A-Z0-9]{1,16}", s) for s in selected):
        raise ValueError("One to forty normalized instruments required")
    if not 0 < max_seconds <= 300 or type(max_rows) is not int or not 1 <= max_rows <= 1_000_000:
        raise ValueError("Resource ceiling invalid")
    began = time.monotonic()
    report = {"schema_version": SCHEMA, "status": "BLOCKED", "complete": False,
              "scope": "retained calculation lineage and cycle coverage only",
              "training_eligibility": "NOT_ASSESSED", "economic_capture_completeness": "NOT_ASSESSED",
              "database": file_identity(path), "trading_date": day,
              "window_start_utc": iso(start), "window_end_utc_exclusive": iso(end),
              "symbols": list(selected), "expected_minutes_per_symbol": int((end - start).total_seconds() // 60),
              "timestamp_storage_contract": "declared *_utc database columns are UTC, including offset-free values",
              "resource_limits": {"max_seconds": max_seconds, "max_rows_per_query": max_rows,
                                  "max_blob_bytes": MAX_BLOB_BYTES, "max_result_bytes_per_query": MAX_RESULT_BYTES},
              "queries": [], "issues": []}

    def budget():
        if time.monotonic() - began > max_seconds:
            raise AuditBlocked("QUERY_BUDGET_EXCEEDED")

    def read(conn, label, sql, params=()):
        budget()
        entry = {"label": label, "sql": sql, "parameters": list(params)}
        report["queries"].append(entry)
        try:
            cursor = conn.execute(sql, params)
            rows, byte_count = [], 0
            for row in cursor:
                budget()
                byte_count += sum(len(v) if isinstance(v, (bytes, str)) else 16 for v in row)
                if byte_count > MAX_RESULT_BYTES:
                    raise AuditBlocked("RESULT_BYTE_BUDGET_EXCEEDED:" + label)
                rows.append(row)
                if len(rows) > max_rows:
                    raise AuditBlocked("ROW_BUDGET_EXCEEDED:" + label)
        except sqlite3.OperationalError as exc:
            if "interrupted" in str(exc).lower():
                raise AuditBlocked("QUERY_BUDGET_EXCEEDED") from exc
            raise
        if len(rows) > max_rows:
            raise AuditBlocked("ROW_BUDGET_EXCEEDED:" + label)
        entry["row_count"] = len(rows)
        # Blob content is bound by its declared digest and independently verified below.
        evidence = [{k: (hashlib.sha256(bytes(v)).hexdigest() if isinstance(v, bytes) else v)
                     for k, v in dict(row).items()} for row in rows]
        entry["result_sha256"] = digest(evidence)
        return [dict(row) for row in rows]

    connection = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=min(1, max_seconds))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.set_progress_handler(lambda: int(time.monotonic() - began > max_seconds), 1000)
        connection.execute("BEGIN")
        tables = {r["name"] for r in read(connection, "table discovery", "SELECT name FROM sqlite_master WHERE type='table'")}
        report["snapshot_opened_at_utc"] = iso(datetime.now(timezone.utc))
        for table, required in REQUIRED_COLUMNS.items():
            if table not in tables:
                raise AuditBlocked("REQUIRED_TABLE_MISSING:" + table)
            cols = read(connection, table + " schema", 'PRAGMA table_info("' + table + '")')
            missing = sorted(required - {r["name"] for r in cols})
            if missing:
                raise AuditBlocked("REQUIRED_COLUMNS_MISSING:" + table + ":" + ",".join(missing))
        placeholders = ",".join("?" for _ in selected)
        params = (day, *selected)
        # Indexed date/symbol constraints bound the cohort; normalize UTC formatting in Python.
        projection = ",".join(sorted(REQUIRED_COLUMNS["prediction_snapshots"] - {"source_payload_json"}))
        projection += ",CASE WHEN length(CAST(source_payload_json AS BLOB))<=? THEN source_payload_json ELSE NULL END source_payload_json"
        preds = read(connection, "prediction cohort", "SELECT " + projection + " FROM prediction_snapshots WHERE trading_date=? AND symbol IN (" + placeholders + ")", (MAX_BLOB_BYTES, *params))
        predictions = []
        for row in preds:
            try:
                if start <= db_utc(row["timestamp_utc"]) < end:
                    predictions.append(row)
            except (TypeError, ValueError, OverflowError):
                report["issues"].append({"prediction_id": row["id"], "reason": "ISSUANCE_TIME_INVALID"})
        runs = read(connection, "calculation cohort", "SELECT " + ",".join(sorted(REQUIRED_COLUMNS["gamma_calculation_runs"])) + " FROM gamma_calculation_runs WHERE trading_date=? AND symbol IN (" + placeholders + ")", params)
        passports = read(connection, "exact passport candidates", "SELECT " + ",".join("p." + column for column in sorted(REQUIRED_COLUMNS["prediction_passports"])) + " FROM prediction_passports p JOIN prediction_snapshots s ON s.id=p.prediction_snapshot_id WHERE s.trading_date=? AND s.symbol IN (" + placeholders + ")", params)
        run_map = defaultdict(list)
        for r in runs:
            run_map[r["calculation_id"]].append(r)
        passport_map = defaultdict(list)
        for p in passports:
            passport_map[p["prediction_snapshot_id"]].append(p)
        class BlobReader:
            def get(self, run_id):
                # Load one bounded blob at a time, never a full day's binary payloads.
                rows = read(connection, "calculation input blob", "SELECT calculation_run_id,encoding,payload_sha256,uncompressed_bytes,compressed_bytes,CASE WHEN length(payload)<=? THEN payload ELSE NULL END payload FROM gamma_calculation_input_blobs WHERE calculation_run_id=?", (MAX_BLOB_BYTES, run_id))
                return rows[0] if len(rows) == 1 else None

        blob_map = BlobReader()
        assessments = []
        for p in predictions:
            budget()
            assessments.append(assess_prediction(p, passport_map[p["id"]], run_map, blob_map))
        by_minute = defaultdict(list)
        malformed_runs = []
        for r in runs:
            try:
                at = db_utc(r["calculated_at_utc"])
                if start <= at < end:
                    by_minute[(r["symbol"], iso(at.replace(second=0, microsecond=0)))].append(r)
            except (TypeError, ValueError, OverflowError):
                malformed_runs.append(r["id"])
        minutes = []
        for symbol in selected:
            at = start
            while at < end:
                values = by_minute[(symbol, iso(at))]
                valid = sum(v["status"] == "valid" for v in values)
                status = "VALID_RUN_PRESENT" if valid else "INVALID_ONLY" if values else "NO_RUN"
                minutes.append({"symbol": symbol, "minute_utc": iso(at), "status": status,
                                "run_count": len(values), "valid_run_count": valid,
                                "invalid_run_count": len(values) - valid})
                at += timedelta(minutes=1)
        report.update(predictions=assessments, calculation_minutes=minutes,
                      malformed_calculation_ids=malformed_runs,
                      prediction_lineage_summary={s: dict(Counter(a["status"] for a in assessments if a["symbol"] == s)) for s in selected},
                      minute_status_summary={s: dict(Counter(a["status"] for a in minutes if a["symbol"] == s)) for s in selected},
                      source_prediction_count=len(predictions), source_calculation_count=len(runs),
                      date_filtered_prediction_rows=len(preds),
                      unplaced_prediction_count=len(report["issues"]),
                      unplaced_calculation_count=len(malformed_runs))
        attempt_rows = []
        for raw_path in capture_attempts:
            budget()
            attempt_path = Path(raw_path).resolve()
            if not attempt_path.is_file() or attempt_path.stat().st_size > MAX_BLOB_BYTES:
                raise AuditBlocked("ATTEMPT_FILE_MISSING_OR_TOO_LARGE:" + str(attempt_path))
            raw = attempt_path.read_bytes()
            report.setdefault("attempt_sources", []).append({"path": str(attempt_path), "sha256": hashlib.sha256(raw).hexdigest()})
            try:
                lines = raw.decode("utf-8-sig").splitlines()
            except UnicodeDecodeError as exc:
                raise AuditBlocked("ATTEMPT_FILE_ENCODING_INVALID:" + str(attempt_path)) from exc
            for number, line in enumerate(lines, 1):
                budget()
                try:
                    value = json.loads(line)
                    at = db_utc(value.get("generated_at_utc") or value.get("attempted_at_utc"))
                    if value.get("symbol") not in selected or not start <= at < end:
                        continue
                    # Receipts remain attempts; never use a price or count them as valid calculations.
                    attempt_rows.append({"source": str(attempt_path), "line": number, "symbol": value["symbol"],
                                         "attempted_at_utc": iso(at), "validation_is_valid": value.get("validation_is_valid"),
                                         "reasons": value.get("validation_failure_reasons") or value.get("reasons") or [],
                                         "evidence_role": "retained_attempt_not_calculation"})
                    if len(attempt_rows) > max_rows:
                        raise AuditBlocked("ROW_BUDGET_EXCEEDED:attempts")
                except (ValueError, TypeError, KeyError, AttributeError) as exc:
                    raise AuditBlocked("ATTEMPT_RECORD_MALFORMED:" + str(number)) from exc
        report["retained_attempts"] = attempt_rows
        report.update(status="COMPLETE", complete=True,
                      interpretation="Complete for the declared query scope; not feed completeness, model promotion or training approval")
    except (AuditBlocked, sqlite3.Error, OSError) as exc:
        report.update(status="BLOCKED", complete=False, blocked_reason=str(exc))
        # Never expose a partial successful lineage cohort after an interrupted assessment.
        for key in ("predictions", "calculation_minutes", "prediction_lineage_summary", "minute_status_summary", "retained_attempts"):
            report.pop(key, None)
    finally:
        if connection is not None:
            connection.rollback()
            connection.close()
    report["elapsed_seconds"] = time.monotonic() - began
    report["report_sha256"] = digest(report)
    return report


def write_assessment(output, report, *, source_database):
    """Create a new exclusive audit directory, never overwrite source artifacts."""
    output, database = Path(output).resolve(), Path(source_database).resolve()
    if output == database or database.is_relative_to(output) or output.is_relative_to(database.parent):
        raise ValueError("Audit output must be outside the source database directory")
    if any(output.is_relative_to(Path(item["path"]).parent) for item in report.get("attempt_sources", [])):
        raise ValueError("Audit output must be outside source attempt directories")
    output.mkdir(parents=True, exist_ok=False)
    report_path = output / "lineage-assessment.json"
    raw = json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False).encode() + b"\n"
    report_path.write_bytes(raw)
    manifest = {"schema_version": SCHEMA, "created_at_utc": iso(datetime.now(timezone.utc)),
                "status": report["status"], "source_database": str(database),
                "files": [{"path": report_path.name, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}],
                "authority": "audit_only_no_backfill_no_training_approval"}
    manifest["manifest_sha256"] = digest(manifest)
    (output / "coverage-manifest.json").write_bytes(canonical(manifest) + b"\n")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--start-utc", required=True)
    parser.add_argument("--end-utc", required=True, help="Exclusive, complete-minute UTC bound")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--capture-attempts", type=Path, action="append", default=[])
    parser.add_argument("--max-seconds", type=float, default=15)
    parser.add_argument("--max-rows", type=int, default=100_000)
    parser.add_argument("--output", type=Path, required=True, help="New audit directory; never an application data directory")
    args = parser.parse_args(argv)
    try:
        report = audit_database(args.database, args.trading_date, args.start_utc, args.end_utc, args.symbols,
                                max_seconds=args.max_seconds, max_rows=args.max_rows, capture_attempts=args.capture_attempts)
        write_assessment(args.output, report, source_database=args.database)
    except (ValueError, OSError) as exc:
        parser.error(str(exc))
    print(json.dumps({"status": report["status"], "complete": report["complete"],
                      "output": str(args.output.resolve()), "blocked_reason": report.get("blocked_reason")}))
    return 0 if report["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
