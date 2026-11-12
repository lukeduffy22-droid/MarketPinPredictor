"""Append-only scheduled research attempts, separate from issued forecasts/scores."""
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time

SCHEMA = "marketpin-forecast-attempt.v1"
MAX_PAYLOAD_BYTES = 256 * 1024


def initialize_attempt_journal(connection):
    connection.execute("CREATE TABLE IF NOT EXISTS research_forecast_attempts ("
        "attempt_id TEXT PRIMARY KEY, attempted_at_utc TEXT NOT NULL, completed_at_utc TEXT NOT NULL, "
        "status TEXT NOT NULL, payload_sha256 TEXT NOT NULL, payload_json TEXT NOT NULL)")
    connection.execute("CREATE INDEX IF NOT EXISTS research_forecast_attempts_time "
                       "ON research_forecast_attempts(attempted_at_utc)")
    for action in ("UPDATE", "DELETE"):
        connection.execute(f"CREATE TRIGGER IF NOT EXISTS research_forecast_attempts_no_{action.lower()} "
            f"BEFORE {action} ON research_forecast_attempts BEGIN "
            "SELECT RAISE(ABORT, 'research attempts are append only'); END")


def _reasons(value):
    if not isinstance(value, list) or len(value) > 64 or any(not isinstance(v, str) or len(v) > 256 for v in value):
        raise ValueError("ATTEMPT_REASON_METADATA_INVALID")
    return list(dict.fromkeys(value))


def append_attempt(path, *, instance_id, sequence, attempted_at_utc, symbols,
                   state=None, error_code=None, loaded_source_sha256=None):
    """One compact event per completed scheduled attempt, including abstentions.

    Never creates a missing journal during capture or modifies issued forecasts.
    Persistence failures are returned explicitly instead of becoming lost success.
    """
    try:
        if not isinstance(instance_id, str) or not instance_id or type(sequence) is not int or sequence < 1:
            raise ValueError("ATTEMPT_IDENTITY_INVALID")
        if not 1 <= len(symbols) <= 40:
            raise ValueError("ATTEMPT_SYMBOLS_INVALID")
        completed = datetime.now(timezone.utc).isoformat()
        state = state or {}
        entries = {}
        for symbol in symbols:
            item = state.get("symbols", {}).get(symbol, {})
            forecasts = item.get("forecasts", {})
            if len(forecasts) > 11:
                raise ValueError("ATTEMPT_HORIZONS_INVALID")
            entries[symbol] = {}
            for horizon in range(11):
                forecast = forecasts.get(str(horizon), {})
                entries[symbol][str(horizon)] = {
                    "status": forecast.get("status", "ERROR" if error_code else "ABSTAIN"),
                    "forecast_id": forecast.get("forecast_id"),
                    "selected_method": forecast.get("selected_method"),
                    "reasons": _reasons(forecast.get("reasons", [error_code or "FORECAST_RESULT_MISSING"])),
                }
        payload = {"schema_version": SCHEMA, "instance_id": instance_id, "sequence": sequence,
                   "attempted_at_utc": attempted_at_utc, "completed_at_utc": completed,
                   "status": "FAILED" if error_code else "CAPTURED",
                   "loaded_source_sha256": loaded_source_sha256,
                   "runtime_context": state.get("runtime_context"), "symbols": entries,
                   "reasons": _reasons([error_code] if error_code else state.get("reasons", []))}
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(raw.encode()) > MAX_PAYLOAD_BYTES:
            raise ValueError("ATTEMPT_PAYLOAD_TOO_LARGE")
        key = hashlib.sha256(f"{instance_id}\n{sequence}".encode()).hexdigest()
        digest = hashlib.sha256(raw.encode()).hexdigest()
        deadline = time.monotonic() + 1.0
        uri = Path(path).resolve().as_uri() + "?mode=rw"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.25)) as connection:
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            connection.execute("INSERT INTO research_forecast_attempts VALUES (?,?,?,?,?,?)",
                (key, attempted_at_utc, completed, payload["status"], digest, raw))
            connection.commit()
        return {"status": "RECORDED", "attempt_id": key, "payload_sha256": digest,
                "completed_at_utc": completed}
    except (sqlite3.Error, OSError, ValueError, TypeError, AttributeError, OverflowError) as exc:
        reason = ("ATTEMPT_JOURNAL_LOCKED" if isinstance(exc, sqlite3.OperationalError)
                  and "locked" in str(exc).lower() else "ATTEMPT_JOURNAL_APPEND_FAILED")
        return {"status": "UNAVAILABLE", "reason_code": reason}


def read_attempts(path, *, limit=20):
    """Bounded query-only receipt read; exposes all attempts, not just successes."""
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError("Attempt limit must be 1..50")
    deadline = time.monotonic() + 1.0
    try:
        uri = Path(path).resolve().as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=0.25)) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            rows = connection.execute("SELECT attempt_id,payload_sha256,"
                "CASE WHEN length(CAST(payload_json AS BLOB)) <= ? THEN payload_json ELSE NULL END "
                "FROM research_forecast_attempts ORDER BY attempted_at_utc DESC, rowid DESC LIMIT ?",
                (MAX_PAYLOAD_BYTES, limit)).fetchall()
        attempts, rejected = [], 0
        for key, digest, raw in rows:
            if not isinstance(raw, str) or len(raw.encode()) > MAX_PAYLOAD_BYTES or hashlib.sha256(raw.encode()).hexdigest() != digest:
                rejected += 1
                continue
            try:
                payload = json.loads(raw)
                expected = hashlib.sha256(f"{payload['instance_id']}\n{payload['sequence']}".encode()).hexdigest()
                valid = expected == key and payload.get("schema_version") == SCHEMA
            except (ValueError, TypeError, KeyError):
                valid = False
            if not valid:
                rejected += 1
                continue
            attempts.append({"attempt_id": key, "payload_sha256": digest, **payload})
        return {"schema_version": SCHEMA, "status": "READ_ONLY" if not rejected else "INCOMPLETE",
                "attempts": attempts, "rejected_receipts": rejected, "limit": limit,
                "examined_receipts": len(rows),
                "scope": "recent scheduled attempts; not a full-session availability rate"}
    except (sqlite3.Error, OSError, ValueError, TypeError, KeyError):
        return {"schema_version": SCHEMA, "status": "UNAVAILABLE", "attempts": [],
                "reason_code": "ATTEMPT_JOURNAL_READ_FAILED"}
