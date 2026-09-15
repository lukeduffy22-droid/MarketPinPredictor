"""Read-only, deterministic assembly of MarketPin monitor commit requests.

The scan ledger deliberately owns durability rather than collection.  This
module closes the boundary between live read authorities and that ledger: it
collects no market data of its own, writes nothing, and never invokes the
commit helper.  A policy confirmation is formed only from the current
source-aligned candidate and a candidate stored by a distinct, receipt-backed
prior substantive scan.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import sqlite3
import statistics
import time
import zlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from app.utils.market_calendar import market_calendar_status
from app.utils.time_et import close_time_et, open_time_et
from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
    validate_cadence_evidence,
)
from backend.monitor_policy import INPUT_SCHEMA as POLICY_INPUT_SCHEMA
from backend.monitor_scan_ledger import (
    COMMIT_RECEIPT_EVENT_TYPE,
    MonitorScanLedgerError,
    _journal_index,
    _read_journal_snapshot,
    _validated_ledger_state,
    validate_scan_wrapper,
    with_policy_input_hashes,
    with_scan_event_id,
)


COLLECTOR_SCHEMA = "marketpin-monitor-scan-collector.v1"
DATA_QUALITY_ID_SCHEMA = "marketpin-monitor-data-quality-condition.v1"
TRANSPORT_HEALTH_SCHEMA = "marketpin-monitor-transport-health.v1"
TRANSPORT_DATA_QUALITY_SCHEMA = (
    "marketpin-monitor-shared-transport-deterioration.v1"
)
TRANSPORT_SCOPE = "shared_databento_transport"
MONITORED_SYMBOLS = ("SPX", "NDX")
CONTEXT_SYMBOLS = ("VIX", "RUT")
AUDIT_MAX_AGE_SECONDS = 180.0
SOURCE_TIMESTAMP_TOLERANCE_SECONDS = 2.0
CADENCE_SECONDS = {"NORMAL": 900, "ELEVATED": 300}
# Keep the collector's candidate window identical to the deterministic policy
# and durable-ledger contract: confirmations need at least four minutes of
# separation and may arrive up to five minutes after their intended cadence.
# A quarter-hour wake can legitimately follow a catch-up scan by less than ten
# minutes, so a narrower collector-only window would emit a reseed request that
# the ledger correctly rejects as unnecessary.
CONFIRMATION_MIN_GAP_SECONDS = 4 * 60
CONFIRMATION_GAP_GRACE_SECONDS = 5 * 60
CONFIRMATION_GAP_BOUNDS = {
    mode: (
        float(CONFIRMATION_MIN_GAP_SECONDS),
        float(cadence_seconds + CONFIRMATION_GAP_GRACE_SECONDS),
    )
    for mode, cadence_seconds in CADENCE_SECONDS.items()
}
CANONICAL_MAX_PAIN_SOURCES = {
    "full-oi-universe",
    "databento-statistics-open-interest-full-universe",
}
CANONICAL_MAX_PAIN_FORMULA_VERSION = "full-oi-max-pain-v1"
# The live lifecycle persists the deterministic ensemble under this exact
# identity.  Keep these literals local so the read-only collector does not
# import torch (via ``backend.ai_predictor``) merely to validate SQLite rows.
CANONICAL_PREDICTION_MODE = "backend_periodic"
CANONICAL_PREDICTION_MODEL_VERSION = "databento_quant_ensemble_v1"
CANONICAL_PREDICTION_MODEL_TYPE = "Databento Quant Ensemble"
CANONICAL_PREDICTION_FEATURE_SCHEMA_VERSION = "databento-close-features-2.0"
PREDICTION_PRICE_CONSISTENCY_ABS_TOLERANCE = 0.01
SUPPORTED_PRICE_CHANGE_SCHEMA = "marketpin-monitor-supported-price-change.v1"
PRICE_CHANGE_15M_WINDOW_SECONDS = 15 * 60
PRICE_CHANGE_15M_TOLERANCE_SECONDS = 2 * 60
GAMMA_INPUT_SCHEMA_VERSION = "gamma-inputs-v2-point-in-time"
MAX_GAMMA_INPUT_COMPRESSED_BYTES = 4 * 1024 * 1024
MAX_GAMMA_INPUT_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
FUTURE_EXPIRATION_CONTEXT_SCHEMA = (
    "marketpin-monitor-future-expiration-context.v1"
)
MAX_FUTURE_EXPIRATION_SOURCE_PROFILES = 64
MAX_FUTURE_EXPIRATION_PROFILES = 8
MAX_FUTURE_EXPIRATION_CONTEXT_BYTES = 32 * 1024
MAX_FUTURE_EXPIRATION_TEXT_LENGTH = 256
REQUIRED_PERSISTENCE_TABLES = (
    "gamma_calculation_runs",
    "gamma_calculation_input_blobs",
    "gamma_audit_snapshots",
    "gamma_pin_snapshots",
    "prediction_snapshots",
    "market_snapshots",
    "market_structure_observations",
    "orb_reference_samples",
    "orb_reference_sample_decisions",
)
TIME_COLUMNS = (
    "timestamp_utc",
    "calculated_at_utc",
    "generated_at_utc",
    "interval_timestamp",
    "created_at_utc",
    "created_at",
    "source_timestamp_utc",
    "sample_timestamp_utc",
    "trading_date",
)
TRIGGER_FIELDS = (
    "half_threshold_gamma_pin_move",
    "contested_pin_leadership",
    "spot_near_or_crossed_gamma_pin",
    "spot_near_or_crossed_zero_gamma",
    "spot_near_or_crossed_gex_wall",
    "normalized_net_gex_near_or_crossed_zero",
    "forecast_bias_awaiting_confirmation",
    "high_volatility_regime",
)
TRANSPORT_COUNTER_FIELDS = (
    "reconnect_attempts",
    "provider_queue_full_warnings",
    "provider_slow_client_warnings",
    "provider_skipped_record_warnings",
    "provider_skipped_records",
    "provider_pending_records_peak",
)
TRANSPORT_TRIGGER_COUNTER_FIELDS = TRANSPORT_COUNTER_FIELDS[:-1]
TRANSPORT_ISSUE_BY_COUNTER = {
    "reconnect_attempts": "TRANSPORT_RECONNECT_ATTEMPTS_INCREASED",
    "provider_queue_full_warnings": "TRANSPORT_QUEUE_FULL_WARNINGS_INCREASED",
    "provider_slow_client_warnings": "TRANSPORT_SLOW_CLIENT_WARNINGS_INCREASED",
    "provider_skipped_record_warnings": (
        "TRANSPORT_SKIPPED_RECORD_WARNINGS_INCREASED"
    ),
    "provider_skipped_records": "TRANSPORT_SKIPPED_RECORDS_INCREASED",
}

_CT = ZoneInfo("America/Chicago")
_ET = ZoneInfo("America/New_York")
_UTC = timezone.utc
_SHA256 = re.compile(r"[0-9a-f]{64}")

# These conditions are an expected consequence of the deliberately suppressed
# live subscription outside the reviewed cash session. They remain visible in
# each symbol's eligibility diagnostics, but must not masquerade as a new
# regular-hours data-quality condition or extend adaptive cadence. Provenance,
# identity, audit, persistence, GEX-integrity, and database failures are
# intentionally absent from this allowlist and therefore remain active.
_OUTSIDE_REGULAR_SESSION_LIVE_ISSUES = frozenset(
    {
        "SESSION_PHASE_NOT_REGULAR",
        "HEALTH_LIVE_UNAVAILABLE",
        "HEALTH_STREAM_CONNECTED_FALSE",
        "HEALTH_STREAM_PROGRESSING_FALSE",
        "HEALTH_TRANSPORT_READY_FALSE",
        "HEALTH_COLLECTION_READY_FALSE",
        "HEALTH_CALCULATION_READY_FALSE",
        "HEALTH_PREDICTION_PIPELINE_OK_FALSE",
        "HEALTH_HANDOFF_NOT_ACTIVE",
        "HEALTH_MESSAGES_NOT_ADVANCING",
        "HEALTH_SYMBOL_STATUS_MISSING",
        "HEALTH_SYMBOL_NOT_USABLE",
        "HEALTH_SYMBOL_STALE",
        "HEALTH_SYMBOL_ZERO_FRESH_QUOTES",
        "WORKSTATION_UNAVAILABLE",
        "WORKSTATION_PIN_PAYLOAD_MISSING",
        "WORKSTATION_VALIDATION_FAILED",
        "WORKSTATION_NOT_LIVE",
        "GEX_ENDPOINT_UNAVAILABLE",
        "AUDIT_NOT_FRESH",
    }
)
_NON_DATA_QUALITY_ELIGIBILITY_REASONS = frozenset(
    {"confirmation_history_seeding"}
)


class MonitorScanCollectorError(ValueError):
    """The read authorities cannot safely produce a commit request."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(_UTC)


def _database_utc(value: Any) -> datetime | None:
    """Parse a timestamp from a SQLite ``*_utc`` column.

    MarketPin's persisted UTC columns intentionally use SQLite's conventional
    timezone-naive ``YYYY-MM-DD HH:MM:SS.ffffff`` representation.  External
    evidence remains required to carry an explicit offset through ``_utc``;
    only values read from a database column named as UTC may use this parser.
    """

    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=_UTC)
    return parsed.astimezone(_UTC)


def _utc_iso(value: datetime) -> str:
    return value.astimezone(_UTC).isoformat()


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _positive(value: Any) -> float | None:
    result = _finite(value)
    return result if result is not None and result > 0.0 else None


def _positive_int(value: Any) -> int | None:
    return value if type(value) is int and value > 0 else None


def _nonnegative_int(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _same_number(left: Any, right: Any) -> bool:
    a = _finite(left)
    b = _finite(right)
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-7)


def _endpoint_payload(endpoint: Any) -> Mapping[str, Any] | None:
    if not isinstance(endpoint, Mapping) or endpoint.get("ok") is not True:
        return None
    payload = endpoint.get("payload")
    return payload if isinstance(payload, Mapping) else None


def _fetch_json(url: str, timeout_seconds: float) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read().decode("utf-8", errors="strict")
            payload = json.loads(raw)
            if not isinstance(payload, Mapping):
                raise ValueError("response JSON is not an object")
            return {
                "ok": 200 <= int(response.status) < 300,
                "status_code": int(response.status),
                "payload": dict(payload),
            }
    except (HTTPError, URLError, TimeoutError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        return {"ok": False, "error": f"{type(exc).__name__}:{exc}"[:300]}


def _latest_audit(project_root: Path, symbol: str) -> dict[str, Any]:
    directory = project_root / "logs" / "audit" / symbol
    try:
        files = list(directory.glob("*.json")) if directory.is_dir() else []
    except OSError as exc:
        return {"present": False, "error": f"{type(exc).__name__}:{exc}"[:300]}
    if not files:
        return {"present": False}
    path = max(files, key=lambda item: (item.stat().st_mtime_ns, item.name))
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise ValueError("audit JSON is not an object")
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        return {
            "present": True,
            "path": str(path.resolve()),
            "error": f"{type(exc).__name__}:{exc}"[:300],
        }
    return {"present": True, "path": str(path.resolve()), "payload": dict(payload)}


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    result: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            result[key] = hashlib.sha256(value).hexdigest()
        elif isinstance(value, (str, int, float, bool)) or value is None:
            result[key] = value
        else:
            result[key] = str(value)
    return result


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")')}


def _table_summary(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    columns = _table_columns(connection, table)
    row_count = int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
    time_column = next((name for name in TIME_COLUMNS if name in columns), None)
    latest = None
    if time_column is not None:
        latest = connection.execute(
            f'SELECT MAX("{time_column}") FROM "{table}"'
        ).fetchone()[0]
    return {"row_count": row_count, "latest_column": time_column, "latest_value": latest}


def _gamma_input_blob_evidence(
    blob: sqlite3.Row | None,
    *,
    gamma_run: Mapping[str, Any],
    structure: Mapping[str, Any],
    symbol: str,
    session_date: str,
) -> dict[str, Any] | None:
    """Verify a compressed calculation input and return bounded evidence."""

    if blob is None:
        return None
    result: dict[str, Any] = {
        "calculation_run_id": blob["calculation_run_id"],
        "encoding": blob["encoding"],
        "payload_sha256": blob["payload_sha256"],
        "uncompressed_bytes": blob["uncompressed_bytes"],
        "compressed_bytes": blob["compressed_bytes"],
        "payload_integrity_verified": False,
        "payload_lineage_verified": False,
        "validation_reason": "GAMMA_CALCULATION_INPUT_INTEGRITY_INVALID",
    }
    compressed_size = _positive_int(blob["compressed_bytes"])
    uncompressed_size = _positive_int(blob["uncompressed_bytes"])
    payload_sha256 = str(blob["payload_sha256"] or "")
    try:
        compressed = bytes(blob["payload"])
    except (TypeError, ValueError):
        return result
    if (
        blob["encoding"] != "canonical-json+zlib-v1"
        or compressed_size is None
        or uncompressed_size is None
        or compressed_size > MAX_GAMMA_INPUT_COMPRESSED_BYTES
        or uncompressed_size > MAX_GAMMA_INPUT_UNCOMPRESSED_BYTES
        or compressed_size != len(compressed)
        or _SHA256.fullmatch(payload_sha256) is None
    ):
        return result
    try:
        decompressor = zlib.decompressobj()
        canonical = decompressor.decompress(
            compressed, MAX_GAMMA_INPUT_UNCOMPRESSED_BYTES + 1
        )
        remaining = MAX_GAMMA_INPUT_UNCOMPRESSED_BYTES + 1 - len(canonical)
        if remaining <= 0:
            return result
        canonical += decompressor.flush(remaining)
        compressed_stream_complete = bool(
            decompressor.eof
            and not decompressor.unused_data
            and not decompressor.unconsumed_tail
        )
        payload = json.loads(canonical.decode("utf-8"))
        canonical_payload_matches = (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
            == canonical
        )
    except (
        TypeError,
        ValueError,
        UnicodeDecodeError,
        zlib.error,
        json.JSONDecodeError,
    ):
        return result
    if (
        len(canonical) != uncompressed_size
        or hashlib.sha256(canonical).hexdigest() != payload_sha256
        or not compressed_stream_complete
        or not canonical_payload_matches
        or not isinstance(payload, Mapping)
    ):
        return result
    result["payload_integrity_verified"] = True
    result["validation_reason"] = "GAMMA_CALCULATION_INPUT_SCHEMA_INVALID"
    raw_rows = payload.get("raw_fresh_chain_rows")
    calculated_rows = payload.get("calculated_gex_rows")
    summary = payload.get("output_summary")
    if not (
        payload.get("input_schema_version") == GAMMA_INPUT_SCHEMA_VERSION
        and isinstance(payload.get("calculated_at_utc"), str)
        and isinstance(raw_rows, list)
        and all(isinstance(row, Mapping) for row in raw_rows)
        and isinstance(calculated_rows, list)
        and all(isinstance(row, Mapping) for row in calculated_rows)
        and isinstance(payload.get("parameters"), Mapping)
        and isinstance(payload.get("rejection_counts"), Mapping)
        and isinstance(summary, Mapping)
    ):
        return result
    assert isinstance(summary, Mapping)
    provenance = summary.get("universe_provenance")
    provenance = provenance if isinstance(provenance, Mapping) else {}
    run_epoch = str(gamma_run.get("subscription_epoch_id") or "")
    run_generation = _positive_int(gamma_run.get("subscription_generation"))
    run_calculated = _database_utc(gamma_run.get("calculated_at_utc"))
    payload_calculated = _database_utc(payload.get("calculated_at_utc"))
    run_source_universe = str(gamma_run.get("universe_sha256") or "")
    selected_universe = str(structure.get("universe_sha256") or "")
    result["validation_reason"] = "GAMMA_CALCULATION_INPUT_LINEAGE_MISMATCH"
    if (
        gamma_run.get("input_schema_version") != GAMMA_INPUT_SCHEMA_VERSION
        or str(payload.get("calculation_id") or "")
        != str(gamma_run.get("calculation_id") or "")
        or str(payload.get("symbol") or "") != symbol
        or payload.get("subscription_epoch_id") != run_epoch
        or _positive_int(payload.get("subscription_generation")) != run_generation
        or run_calculated is None
        or payload_calculated != run_calculated
        or str(summary.get("calculation_id") or "")
        != str(gamma_run.get("calculation_id") or "")
        or str(summary.get("symbol") or "") != symbol
        or summary.get("subscription_epoch_id") != run_epoch
        or _positive_int(summary.get("subscription_generation")) != run_generation
        or summary.get("validation_is_valid") is not True
        or summary.get("gamma_excluded_from_model") is not False
        or not _same_number(summary.get("price"), structure.get("reference_price"))
        or not _same_number(summary.get("gamma_pin"), structure.get("gamma_pin"))
        or not _same_number(summary.get("max_pain"), structure.get("max_pain"))
        or str(summary.get("primary_expiration") or "") != session_date
        or summary.get("same_day_profile_available") is not True
        or summary.get("selected_universe_sha256") != selected_universe
        or summary.get("universe_sha256") != run_source_universe
        or provenance.get("source_sha256") != run_source_universe
        or provenance.get("is_fallback") is not False
        or str(provenance.get("trading_date") or "") != session_date
        or str(provenance.get("source_date") or "") != session_date
    ):
        return result
    result["payload_lineage_verified"] = True
    result["validation_reason"] = None
    return result


def _database_evidence(
    database_path: Path,
    *,
    session_date: str,
    audits: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if not database_path.is_file():
        return {"present": False, "path": str(database_path.resolve())}
    uri = f"file:{database_path.resolve().as_posix()}?mode=ro"
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
    except sqlite3.Error as exc:
        return {
            "present": True,
            "path": str(database_path.resolve()),
            "error": f"{type(exc).__name__}:{exc}"[:300],
        }
    try:
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        quick_check = str(connection.execute("PRAGMA quick_check").fetchone()[0])
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        summaries = {
            table: _table_summary(connection, table)
            for table in REQUIRED_PERSISTENCE_TABLES
            if table in tables
        }
        symbols: dict[str, Any] = {}
        for symbol in MONITORED_SYMBOLS:
            audit_entry = audits.get(symbol)
            audit = (
                audit_entry.get("payload")
                if isinstance(audit_entry, Mapping)
                and isinstance(audit_entry.get("payload"), Mapping)
                else {}
            )
            calculation_id = str(audit.get("calculation_id") or "").strip()
            structure = None
            gamma_run = None
            input_blob = None
            prediction = None
            if calculation_id and "market_structure_observations" in tables:
                structure = _row_dict(
                    connection.execute(
                        "SELECT * FROM market_structure_observations "
                        "WHERE symbol=? AND trading_date=? AND calculation_id=? "
                        "ORDER BY captured_at_utc DESC, observation_id DESC LIMIT 1",
                        (symbol, session_date, calculation_id),
                    ).fetchone()
                )
            if calculation_id and "gamma_calculation_runs" in tables:
                gamma_run = _row_dict(
                    connection.execute(
                        "SELECT * FROM gamma_calculation_runs "
                        "WHERE symbol=? AND trading_date=? AND calculation_id=? "
                        "ORDER BY id DESC LIMIT 1",
                        (symbol, session_date, calculation_id),
                    ).fetchone()
                )
            if gamma_run is not None and "gamma_calculation_input_blobs" in tables:
                input_blob = _gamma_input_blob_evidence(
                    connection.execute(
                        "SELECT calculation_run_id, encoding, payload_sha256, "
                        "uncompressed_bytes, compressed_bytes, payload "
                        "FROM gamma_calculation_input_blobs WHERE calculation_run_id=?",
                        (gamma_run.get("id"),),
                    ).fetchone(),
                    gamma_run=gamma_run,
                    structure=structure or {},
                    symbol=symbol,
                    session_date=session_date,
                )
            if "prediction_snapshots" in tables:
                prediction = _row_dict(
                    connection.execute(
                        "SELECT symbol, trading_date, timestamp_utc, quote_timestamp_utc, provider, "
                        "model_version, model_type, prediction_mode, feature_schema_version, "
                        "subscription_epoch_id, subscription_generation, is_valid, "
                        "validation_status, current_price, predicted_close, "
                        "expected_move_pct, net_bias, gamma_pin, max_pain, zero_gamma, "
                        "gross_gex, net_gex FROM prediction_snapshots "
                        "WHERE symbol=? AND trading_date=? AND prediction_mode=? "
                        "AND model_version=? AND model_type=? AND feature_schema_version=? "
                        "ORDER BY timestamp_utc DESC, id DESC LIMIT 1",
                        (
                            symbol,
                            session_date,
                            CANONICAL_PREDICTION_MODE,
                            CANONICAL_PREDICTION_MODEL_VERSION,
                            CANONICAL_PREDICTION_MODEL_TYPE,
                            CANONICAL_PREDICTION_FEATURE_SCHEMA_VERSION,
                        ),
                    ).fetchone()
                )
            symbols[symbol] = {
                "market_structure": structure,
                "gamma_calculation_run": gamma_run,
                "gamma_calculation_input_blob": input_blob,
                "prediction_snapshot": prediction,
            }
        return {
            "present": True,
            "path": str(database_path.resolve()),
            "journal_mode": journal_mode,
            "quick_check": quick_check,
            "tables_present": sorted(tables),
            "table_summaries": summaries,
            "symbols": symbols,
        }
    except sqlite3.Error as exc:
        return {
            "present": True,
            "path": str(database_path.resolve()),
            "error": f"{type(exc).__name__}:{exc}"[:300],
        }
    finally:
        connection.close()


def acquire_live_evidence(
    *,
    project_root: Path,
    backend_url: str,
    database_path: Path,
    session_date: str,
    sample_seconds: float = 5.0,
    timeout_seconds: float = 5.0,
) -> dict[str, Any]:
    """Acquire bounded read authorities without starting or mutating anything."""

    base = backend_url.rstrip("/")
    timeout = max(0.2, min(float(timeout_seconds), 10.0))
    sample = max(0.0, min(float(sample_seconds), 10.0))
    first = _fetch_json(base + "/health/live", timeout)
    if sample > 0.0:
        time.sleep(sample)
    second = _fetch_json(base + "/health/live", timeout)
    overview = _fetch_json(base + "/health", timeout)
    workstation = {
        symbol: _fetch_json(base + f"/v1/workstation/state/{symbol}", timeout)
        for symbol in MONITORED_SYMBOLS
    }
    gex = {
        symbol: _fetch_json(base + f"/gex/{symbol}", timeout)
        for symbol in MONITORED_SYMBOLS
    }
    orb = _fetch_json(base + "/v1/orb", timeout)
    audits = {symbol: _latest_audit(project_root, symbol) for symbol in MONITORED_SYMBOLS}
    database = _database_evidence(
        database_path,
        session_date=session_date,
        audits=audits,
    )
    return {
        "schema_version": COLLECTOR_SCHEMA,
        "observed_at_utc": _utc_iso(datetime.now(_UTC)),
        "health_first": first,
        "health_second": second,
        "health_overview": overview,
        "workstation": workstation,
        "gex": gex,
        "orb": orb,
        "audits": audits,
        "database": database,
        "sample_seconds": sample,
    }


def load_monitor_context(
    *, state_path: Path, journal_dir: Path, session_date: str
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Mapping[str, Any]]]:
    """Read and validate monitor state plus the complete current journal."""

    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MonitorScanCollectorError(
            f"state_read_failed:{type(exc).__name__}:{exc}"
        ) from exc
    if not isinstance(state, Mapping):
        raise MonitorScanCollectorError("state_must_be_an_object")
    state = copy.deepcopy(dict(state))
    if state.get("session_date") != session_date:
        raise MonitorScanCollectorError("state_session_date_mismatch_prepare_required")
    journal_path = journal_dir / f"{session_date}.jsonl"
    try:
        records, raw = _read_journal_snapshot(journal_path)
        by_id, _ = _journal_index(records)
        _validated_ledger_state(state.get("monitor_scan_ledger"), state, by_id, raw)
    except (OSError, MonitorScanLedgerError) as exc:
        raise MonitorScanCollectorError(f"monitor_context_invalid:{exc}") from exc
    return state, records, by_id


def _health_issues(evidence: Mapping[str, Any]) -> list[str]:
    first = _endpoint_payload(evidence.get("health_first"))
    second = _endpoint_payload(evidence.get("health_second"))
    if first is None or second is None:
        return ["HEALTH_LIVE_UNAVAILABLE"]
    issues: list[str] = []
    required_true = (
        "stream_connected",
        "stream_progressing",
        "transport_ready",
        "runtime_context_stable",
    )
    issues.extend(
        f"HEALTH_{field.upper()}_FALSE"
        for field in required_true
        if second.get(field) is not True
    )
    if str(second.get("provider") or "").lower() != "databento":
        issues.append("HEALTH_PROVIDER_NOT_DATABENTO")
    if str(second.get("handoff_status") or "").lower() != "active":
        issues.append("HEALTH_HANDOFF_NOT_ACTIVE")
    first_epoch = str(first.get("subscription_epoch_id") or "")
    second_epoch = str(second.get("subscription_epoch_id") or "")
    if _SHA256.fullmatch(second_epoch) is None:
        issues.append("HEALTH_EPOCH_INVALID")
    elif first_epoch != second_epoch:
        issues.append("HEALTH_EPOCH_CHANGED_DURING_SAMPLE")
    first_generation = _positive_int(first.get("subscription_generation"))
    second_generation = _positive_int(second.get("subscription_generation"))
    if second_generation is None:
        issues.append("HEALTH_GENERATION_INVALID")
    elif first_generation != second_generation:
        issues.append("HEALTH_GENERATION_CHANGED_DURING_SAMPLE")
    first_messages = _finite(first.get("messages_received"))
    second_messages = _finite(second.get("messages_received"))
    if first_messages is None or second_messages is None or second_messages <= first_messages:
        issues.append("HEALTH_MESSAGES_NOT_ADVANCING")
    return sorted(set(issues))


def _symbol_status_issues(health: Mapping[str, Any], symbol: str) -> list[str]:
    statuses = health.get("symbol_status")
    status = statuses.get(symbol) if isinstance(statuses, Mapping) else None
    if not isinstance(status, Mapping):
        return ["HEALTH_SYMBOL_STATUS_MISSING"]
    issues: list[str] = []
    if status.get("usable_for_prediction") is not True:
        issues.append("HEALTH_SYMBOL_NOT_USABLE")
    if status.get("is_stale") is True:
        issues.append("HEALTH_SYMBOL_STALE")
    if status.get("epoch_is_current") is not True:
        issues.append("HEALTH_SYMBOL_EPOCH_MISMATCH")
    if status.get("generation_is_current") is not True:
        issues.append("HEALTH_SYMBOL_GENERATION_MISMATCH")
    if (_positive(status.get("fresh_quote_count")) or 0.0) <= 0.0:
        issues.append("HEALTH_SYMBOL_ZERO_FRESH_QUOTES")
    return issues


def _audit_source_time(audit: Mapping[str, Any]) -> datetime | None:
    for field in (
        "latest_ts_recv_utc",
        "observation_index_utc",
        "timestamp_utc",
        "generated_at_utc",
    ):
        parsed = _utc(audit.get(field))
        if parsed is not None:
            return parsed
    return None


def _same_text(values: Sequence[Any], *, sha256: bool = False) -> str | None:
    strings = [str(value or "").strip() for value in values]
    if not strings or any(not value for value in strings) or len(set(strings)) != 1:
        return None
    if sha256 and _SHA256.fullmatch(strings[0]) is None:
        return None
    return strings[0]


def _future_expiration_context(
    audit: Mapping[str, Any] | None,
    candidate: Mapping[str, Any] | None,
    *,
    session_date: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Project bounded, research-only future profiles from the current audit.

    This deliberately runs *after* the canonical candidate gate. Rejected
    shadow rows are diagnostic only and can never weaken or populate the
    production candidate used by the directional policy.
    """

    diagnostic: dict[str, Any] = {
        "schema_version": FUTURE_EXPIRATION_CONTEXT_SCHEMA,
        "source_field": "expiration_profiles",
        "context_only": True,
        "status": "unavailable",
        "abstained": True,
        "retained_count": 0,
        "excluded_primary_count": 0,
        "rejected_count": 0,
        "rejection_reasons": [],
    }
    if audit is None or candidate is None:
        diagnostic["rejection_reasons"] = ["canonical_current_audit_ineligible"]
        return [], diagnostic

    raw_profiles = audit.get("expiration_profiles")
    if raw_profiles is None:
        diagnostic["status"] = "not_present"
        diagnostic["rejection_reasons"] = ["source_field_not_present"]
        return [], diagnostic
    if not isinstance(raw_profiles, list):
        diagnostic["rejected_count"] = 1
        diagnostic["rejection_reasons"] = ["source_profiles_not_an_array"]
        return [], diagnostic
    if len(raw_profiles) > MAX_FUTURE_EXPIRATION_SOURCE_PROFILES:
        diagnostic["rejected_count"] = len(raw_profiles)
        diagnostic["rejection_reasons"] = ["source_profile_count_exceeds_limit"]
        return [], diagnostic

    required_text = {
        "calculation_id": candidate.get("calculation_id"),
        "subscription_epoch_id": candidate.get("subscription_epoch_id"),
        "gex_formula_version": candidate.get("gex_formula_version"),
        "universe_sha256": candidate.get("universe_sha256"),
        "source_timestamp_utc": candidate.get("source_timestamp_utc"),
    }
    if any(
        not isinstance(value, str)
        or not value
        or len(value) > MAX_FUTURE_EXPIRATION_TEXT_LENGTH
        for value in required_text.values()
    ):
        diagnostic["rejected_count"] = len(raw_profiles)
        diagnostic["rejection_reasons"] = ["source_identity_not_bounded"]
        return [], diagnostic

    try:
        session_day = date.fromisoformat(session_date)
    except ValueError:
        diagnostic["rejected_count"] = len(raw_profiles)
        diagnostic["rejection_reasons"] = ["session_date_invalid"]
        return [], diagnostic

    reasons: set[str] = set()
    excluded_primary = 0
    rejected = 0
    projected_by_expiration: dict[str, dict[str, Any]] = {}
    for raw_profile in raw_profiles:
        if not isinstance(raw_profile, Mapping):
            rejected += 1
            reasons.add("profile_not_an_object")
            continue
        expiration = raw_profile.get("expiration")
        try:
            expiration_day = date.fromisoformat(str(expiration))
        except ValueError:
            rejected += 1
            reasons.add("expiration_invalid")
            continue
        if not isinstance(expiration, str) or expiration_day.isoformat() != expiration:
            rejected += 1
            reasons.add("expiration_not_canonical")
            continue
        if expiration_day == session_day:
            excluded_primary += 1
            continue
        if expiration_day < session_day:
            rejected += 1
            reasons.add("expiration_not_future")
            continue
        if expiration in projected_by_expiration:
            rejected += 1
            reasons.add("duplicate_expiration")
            continue

        pin_present = raw_profile.get("pin") is not None
        max_pain_present = raw_profile.get("max_pain") is not None
        pin = _positive(raw_profile.get("pin")) if pin_present else None
        max_pain = (
            _positive(raw_profile.get("max_pain")) if max_pain_present else None
        )
        if (pin_present and pin is None) or (max_pain_present and max_pain is None):
            rejected += 1
            reasons.add("profile_price_not_positive_finite")
            continue
        if pin is None and max_pain is None:
            rejected += 1
            reasons.add("profile_has_no_pin_or_max_pain")
            continue

        optional_alignment = (
            ("provider", "databento", lambda value: str(value).lower()),
            ("calculation_id", required_text["calculation_id"], str),
            (
                "subscription_epoch_id",
                required_text["subscription_epoch_id"],
                str,
            ),
            (
                "subscription_generation",
                candidate.get("subscription_generation"),
                lambda value: value,
            ),
            (
                "gex_formula_version",
                required_text["gex_formula_version"],
                str,
            ),
        )
        if any(
            field in raw_profile and normalizer(raw_profile.get(field)) != expected
            for field, expected, normalizer in optional_alignment
        ):
            rejected += 1
            reasons.add("profile_source_identity_mismatch")
            continue
        profile_universe = raw_profile.get(
            "selected_universe_sha256", raw_profile.get("universe_sha256")
        )
        if profile_universe is not None and profile_universe != required_text[
            "universe_sha256"
        ]:
            rejected += 1
            reasons.add("profile_universe_mismatch")
            continue
        profile_source_time = raw_profile.get("source_timestamp_utc")
        if profile_source_time is not None:
            parsed_source_time = _utc(profile_source_time)
            expected_source_time = _utc(required_text["source_timestamp_utc"])
            if (
                parsed_source_time is None
                or expected_source_time is None
                or abs((parsed_source_time - expected_source_time).total_seconds())
                > SOURCE_TIMESTAMP_TOLERANCE_SECONDS
            ):
                rejected += 1
                reasons.add("profile_source_timestamp_mismatch")
                continue

        projected: dict[str, Any] = {
            "expiration": expiration,
            "days_to_expiration": (expiration_day - session_day).days,
            "context_only": True,
            "role": "future_expiration_context_only",
            "source": "current_eligible_databento_audit.expiration_profiles",
            "source_timestamp_utc": required_text["source_timestamp_utc"],
            "calculation_id": required_text["calculation_id"],
            "subscription_epoch_id": required_text["subscription_epoch_id"],
            "subscription_generation": candidate.get("subscription_generation"),
            "gex_formula_version": required_text["gex_formula_version"],
            "universe_sha256": required_text["universe_sha256"],
            "universe_is_fallback": False,
        }
        if pin is not None:
            projected["gamma_pin"] = pin
        if max_pain is not None:
            row_source = raw_profile.get("max_pain_source")
            row_as_of = raw_profile.get("max_pain_as_of")
            top_source = candidate.get("max_pain_source")
            top_as_of = candidate.get("max_pain_as_of")
            top_formula = candidate.get("max_pain_formula_version")
            row_formula = raw_profile.get("max_pain_formula_version", top_formula)
            if not (
                isinstance(row_source, str)
                and row_source in CANONICAL_MAX_PAIN_SOURCES
                and row_source == top_source
                and row_as_of == session_date
                and row_as_of == top_as_of
                and row_formula == CANONICAL_MAX_PAIN_FORMULA_VERSION
                and row_formula == top_formula
            ):
                rejected += 1
                reasons.add("max_pain_provenance_mismatch")
                continue
            projected.update(
                {
                    "max_pain": max_pain,
                    "max_pain_source": row_source,
                    "max_pain_as_of": row_as_of,
                    "max_pain_formula_version": row_formula,
                }
            )
        projected_by_expiration[expiration] = projected

    projected_profiles = [
        projected_by_expiration[key] for key in sorted(projected_by_expiration)
    ]
    if len(projected_profiles) > MAX_FUTURE_EXPIRATION_PROFILES:
        rejected += len(projected_profiles) - MAX_FUTURE_EXPIRATION_PROFILES
        reasons.add("output_profile_limit_applied")
        projected_profiles = projected_profiles[:MAX_FUTURE_EXPIRATION_PROFILES]
    try:
        serialized_size = len(_canonical_bytes(projected_profiles))
    except (TypeError, ValueError):
        serialized_size = MAX_FUTURE_EXPIRATION_CONTEXT_BYTES + 1
    if serialized_size > MAX_FUTURE_EXPIRATION_CONTEXT_BYTES:
        rejected += len(projected_profiles)
        reasons.add("output_json_size_exceeds_limit")
        projected_profiles = []
        serialized_size = 2

    diagnostic.update(
        {
            "status": (
                "available"
                if projected_profiles and rejected == 0
                else "partial"
                if projected_profiles
                else "unavailable"
            ),
            "abstained": bool(rejected or not projected_profiles),
            "retained_count": len(projected_profiles),
            "excluded_primary_count": excluded_primary,
            "rejected_count": rejected,
            "rejection_reasons": sorted(reasons),
            "serialized_bytes": serialized_size,
        }
    )
    return projected_profiles, diagnostic


def _candidate_observation(
    symbol: str,
    evidence: Mapping[str, Any],
    *,
    observed_at: datetime,
    global_health_issues: Sequence[str],
) -> tuple[dict[str, Any] | None, list[str], dict[str, Any]]:
    issues = list(global_health_issues)
    session_date = observed_at.astimezone(_CT).date().isoformat()
    health = _endpoint_payload(evidence.get("health_second"))
    workstation_endpoint = (evidence.get("workstation") or {}).get(symbol)
    gex_endpoint = (evidence.get("gex") or {}).get(symbol)
    workstation = _endpoint_payload(workstation_endpoint)
    gex = _endpoint_payload(gex_endpoint)
    audit_entry = (evidence.get("audits") or {}).get(symbol)
    audit = (
        audit_entry.get("payload")
        if isinstance(audit_entry, Mapping)
        and isinstance(audit_entry.get("payload"), Mapping)
        else None
    )
    database = evidence.get("database")
    database_symbol = (
        (database.get("symbols") or {}).get(symbol)
        if isinstance(database, Mapping)
        else None
    )
    structure = (
        database_symbol.get("market_structure")
        if isinstance(database_symbol, Mapping)
        and isinstance(database_symbol.get("market_structure"), Mapping)
        else None
    )
    gamma_run = (
        database_symbol.get("gamma_calculation_run")
        if isinstance(database_symbol, Mapping)
        and isinstance(database_symbol.get("gamma_calculation_run"), Mapping)
        else None
    )
    input_blob = (
        database_symbol.get("gamma_calculation_input_blob")
        if isinstance(database_symbol, Mapping)
        and isinstance(database_symbol.get("gamma_calculation_input_blob"), Mapping)
        else None
    )
    prediction = (
        database_symbol.get("prediction_snapshot")
        if isinstance(database_symbol, Mapping)
        and isinstance(database_symbol.get("prediction_snapshot"), Mapping)
        else None
    )
    diagnostic = {
        "audit_path": audit_entry.get("path") if isinstance(audit_entry, Mapping) else None,
        "calculation_id": audit.get("calculation_id") if isinstance(audit, Mapping) else None,
    }
    if health is None:
        issues.append("HEALTH_LIVE_UNAVAILABLE")
    else:
        issues.extend(_symbol_status_issues(health, symbol))
    if workstation is None:
        issues.append("WORKSTATION_UNAVAILABLE")
        pin = None
    else:
        pin = workstation.get("pin_payload")
        if not isinstance(pin, Mapping):
            issues.append("WORKSTATION_PIN_PAYLOAD_MISSING")
            pin = None
        workstation_health = workstation.get("health")
        if not isinstance(workstation_health, Mapping) or workstation_health.get(
            "validation_is_valid"
        ) is not True:
            issues.append("WORKSTATION_VALIDATION_FAILED")
        if workstation.get("status") != "live":
            issues.append("WORKSTATION_NOT_LIVE")
    if gex is None:
        issues.append("GEX_ENDPOINT_UNAVAILABLE")
    if audit is None:
        issues.append("AUDIT_UNAVAILABLE")
    database_readable = bool(
        isinstance(database, Mapping)
        and database.get("present") is True
        and not database.get("error")
    )
    if not isinstance(database, Mapping) or database.get("present") is not True:
        issues.append("DATABASE_UNAVAILABLE")
    elif database.get("error"):
        issues.append("DATABASE_READ_FAILED")
    else:
        if str(database.get("journal_mode") or "").lower() != "wal":
            issues.append("DATABASE_NOT_WAL")
        if str(database.get("quick_check") or "").lower() != "ok":
            issues.append("DATABASE_QUICK_CHECK_FAILED")
        present = set(database.get("tables_present") or [])
        missing = set(REQUIRED_PERSISTENCE_TABLES).difference(present)
        if missing:
            issues.append("DATABASE_REQUIRED_TABLES_MISSING")
        if structure is None:
            issues.append("MARKET_STRUCTURE_EXACT_ROW_MISSING")
        if gamma_run is None:
            issues.append("GAMMA_RUN_EXACT_ROW_MISSING")
        if input_blob is None:
            issues.append("GAMMA_INPUT_BLOB_EXACT_ROW_MISSING")
    if not database_readable or any(
        value is None
        for value in (health, workstation, pin, gex, audit, structure, gamma_run, input_blob)
    ):
        return None, sorted(set(issues)), diagnostic

    assert isinstance(health, Mapping)
    assert isinstance(workstation, Mapping)
    assert isinstance(pin, Mapping)
    assert isinstance(gex, Mapping)
    assert isinstance(audit, Mapping)
    assert isinstance(structure, Mapping)
    assert isinstance(gamma_run, Mapping)
    assert isinstance(input_blob, Mapping)

    # Each HTTP authority carries its own stable symbol/provider identity.
    # Validate those fields independently so a routing/cache mix-up cannot be
    # hidden merely because process-level epoch and generation values match.
    if str(workstation.get("symbol") or "").upper() != symbol:
        issues.append("WORKSTATION_SYMBOL_MISMATCH")
    if str(workstation.get("provider") or "").lower() != "databento":
        issues.append("WORKSTATION_PROVIDER_NOT_DATABENTO")
    if str(pin.get("symbol") or "").upper() != symbol:
        issues.append("PIN_PAYLOAD_SYMBOL_MISMATCH")
    if str(pin.get("provider") or "").lower() != "databento":
        issues.append("PIN_PAYLOAD_PROVIDER_NOT_DATABENTO")
    if str(gex.get("symbol") or "").upper() != symbol:
        issues.append("GEX_ENDPOINT_SYMBOL_MISMATCH")
    if str(gex.get("provider") or "").lower() != "databento":
        issues.append("GEX_ENDPOINT_PROVIDER_NOT_DATABENTO")

    if str(audit.get("symbol") or "").upper() != symbol:
        issues.append("AUDIT_SYMBOL_MISMATCH")
    if audit.get("validation_is_valid") is not True:
        issues.append("AUDIT_VALIDATION_FAILED")
    if audit.get("gamma_excluded_from_model") is not False:
        issues.append("AUDIT_GAMMA_EXCLUDED")
    if str(audit.get("provider") or "").lower() != "databento":
        issues.append("AUDIT_PROVIDER_NOT_DATABENTO")
    mandated_provenance_layers = (
        ("audit.universe_provenance", audit.get("universe_provenance")),
        ("audit.oi_analytics_provenance", audit.get("oi_analytics_provenance")),
        ("pin.universe_provenance", pin.get("universe_provenance")),
        ("pin.oi_analytics_provenance", pin.get("oi_analytics_provenance")),
        ("gex.universe_provenance", gex.get("universe_provenance")),
        ("gex.oi_analytics_provenance", gex.get("oi_analytics_provenance")),
    )
    invalid_provenance_layers = [
        name
        for name, layer in mandated_provenance_layers
        if not isinstance(layer, Mapping) or layer.get("is_fallback") is not False
    ]
    if invalid_provenance_layers:
        issues.append("FALLBACK_PROVENANCE")
        diagnostic["invalid_provenance_layers"] = invalid_provenance_layers

    if str(structure.get("symbol") or "").upper() != symbol:
        issues.append("MARKET_STRUCTURE_SYMBOL_MISMATCH")
    if str(structure.get("trading_date") or "") != session_date:
        issues.append("MARKET_STRUCTURE_SESSION_DATE_MISMATCH")
    if str(structure.get("provider") or "").lower() != "databento":
        issues.append("MARKET_STRUCTURE_PROVIDER_NOT_DATABENTO")
    if str(structure.get("validation_status") or "").lower() != "valid":
        issues.append("MARKET_STRUCTURE_VALIDATION_FAILED")
    if str(gamma_run.get("symbol") or "").upper() != symbol:
        issues.append("GAMMA_RUN_SYMBOL_MISMATCH")
    if str(gamma_run.get("trading_date") or "") != session_date:
        issues.append("GAMMA_RUN_SESSION_DATE_MISMATCH")
    if str(gamma_run.get("provider") or "").lower() != "databento":
        issues.append("GAMMA_RUN_PROVIDER_NOT_DATABENTO")
    if str(gamma_run.get("status") or "").lower() != "valid":
        issues.append("GAMMA_RUN_STATUS_INVALID")
    gamma_run_id = _positive_int(gamma_run.get("id"))
    input_blob_run_id = _positive_int(input_blob.get("calculation_run_id"))
    input_blob_valid = bool(
        gamma_run_id is not None
        and input_blob_run_id == gamma_run_id
        and input_blob.get("encoding") == "canonical-json+zlib-v1"
        and _SHA256.fullmatch(str(input_blob.get("payload_sha256") or ""))
        is not None
        and _positive_int(input_blob.get("uncompressed_bytes")) is not None
        and _positive_int(input_blob.get("compressed_bytes")) is not None
        and input_blob.get("payload_integrity_verified") is True
        and input_blob.get("payload_lineage_verified") is True
        and input_blob.get("validation_reason") is None
    )
    if not input_blob_valid:
        issues.append("GAMMA_INPUT_BLOB_INVALID")

    epoch = _same_text(
        (
            health.get("subscription_epoch_id"),
            workstation.get("subscription_epoch_id"),
            pin.get("subscription_epoch_id"),
            gex.get("subscription_epoch_id"),
            audit.get("subscription_epoch_id"),
            structure.get("subscription_epoch_id"),
            gamma_run.get("subscription_epoch_id"),
        ),
        sha256=True,
    )
    if epoch is None:
        issues.append("SUBSCRIPTION_EPOCH_ALIGNMENT_FAILED")
    generations = (
        health.get("subscription_generation"),
        workstation.get("subscription_generation"),
        pin.get("subscription_generation"),
        gex.get("subscription_generation"),
        audit.get("subscription_generation"),
        structure.get("subscription_generation"),
        gamma_run.get("subscription_generation"),
    )
    generation_values = [_positive_int(value) for value in generations]
    generation = generation_values[0] if generation_values and len(set(generation_values)) == 1 else None
    if generation is None:
        issues.append("SUBSCRIPTION_GENERATION_ALIGNMENT_FAILED")
    universe = _same_text(
        (
            pin.get("selected_universe_sha256"),
            gex.get("selected_universe_sha256"),
            audit.get("selected_universe_sha256"),
            structure.get("universe_sha256"),
        ),
        sha256=True,
    )
    if universe is None:
        issues.append("SELECTED_UNIVERSE_ALIGNMENT_FAILED")
    primary_expiration = _same_text(
        (
            pin.get("primary_expiration"),
            gex.get("primary_expiration"),
            audit.get("primary_expiration"),
            structure.get("primary_expiration"),
        )
    )
    if primary_expiration is None:
        issues.append("PRIMARY_EXPIRATION_ALIGNMENT_FAILED")
    elif primary_expiration != session_date:
        issues.append("PRIMARY_EXPIRATION_NOT_CURRENT_SESSION")
    api_same_day_flags = (
        pin.get("same_day_profile_available"),
        gex.get("same_day_profile_available"),
        audit.get("same_day_profile_available"),
    )
    structure_same_day = structure.get("same_day_profile_available")
    if any(value is not True for value in api_same_day_flags) or not (
        structure_same_day is True
        or (type(structure_same_day) is int and structure_same_day == 1)
    ):
        issues.append("SAME_DAY_PROFILE_UNAVAILABLE")
    if str(audit.get("max_pain_as_of") or "") != session_date:
        issues.append("MAX_PAIN_AS_OF_NOT_CURRENT_SESSION")
    formula = _same_text(
        (
            pin.get("gex_formula_version"),
            gex.get("gex_formula_version"),
            audit.get("gex_formula_version"),
            gamma_run.get("formula_version"),
        )
    )
    if formula is None:
        issues.append("GEX_FORMULA_ALIGNMENT_FAILED")
    calculation_id = _same_text(
        (
            audit.get("calculation_id"),
            structure.get("calculation_id"),
            gamma_run.get("calculation_id"),
        )
    )
    if calculation_id is None:
        issues.append("CALCULATION_ID_ALIGNMENT_FAILED")
    for endpoint_payload in (pin, gex):
        endpoint_calculation = str(endpoint_payload.get("calculation_id") or "").strip()
        if endpoint_calculation and calculation_id and endpoint_calculation != calculation_id:
            issues.append("CURRENT_ENDPOINT_CALCULATION_DIFFERS_FROM_AUDIT")

    source_time = _audit_source_time(audit)
    structure_source = _database_utc(structure.get("source_timestamp_utc"))
    generated = _utc(audit.get("generated_at_utc") or audit.get("timestamp_utc"))
    if source_time is None or structure_source is None:
        issues.append("SOURCE_TIMESTAMP_ALIGNMENT_FAILED")
    elif abs((source_time - structure_source).total_seconds()) > SOURCE_TIMESTAMP_TOLERANCE_SECONDS:
        issues.append("SOURCE_TIMESTAMP_ALIGNMENT_FAILED")
    if source_time is not None:
        source_age = (observed_at - source_time).total_seconds()
        diagnostic["source_age_seconds"] = source_age
        if source_time.astimezone(_CT).date().isoformat() != session_date:
            issues.append("SOURCE_SESSION_DATE_MISMATCH")
        if (
            source_age < -SOURCE_TIMESTAMP_TOLERANCE_SECONDS
            or source_age > AUDIT_MAX_AGE_SECONDS
        ):
            issues.append("SOURCE_NOT_FRESH")
    if generated is None:
        issues.append("AUDIT_TIMESTAMP_INVALID")
    else:
        age = (observed_at - generated).total_seconds()
        diagnostic["audit_age_seconds"] = age
        if generated.astimezone(_CT).date().isoformat() != session_date:
            issues.append("AUDIT_SESSION_DATE_MISMATCH")
        if age < -SOURCE_TIMESTAMP_TOLERANCE_SECONDS or age > AUDIT_MAX_AGE_SECONDS:
            issues.append("AUDIT_NOT_FRESH")

    value_pairs = (
        ("SPOT", audit.get("spot_last", audit.get("price")), structure.get("reference_price"), gamma_run.get("spot_price")),
        ("GAMMA_PIN", audit.get("gamma_pin"), structure.get("gamma_pin"), gamma_run.get("gamma_pin")),
        ("MAX_PAIN", audit.get("max_pain"), structure.get("max_pain"), gamma_run.get("max_pain")),
        ("ZERO_GAMMA", audit.get("zero_gamma"), structure.get("zero_gamma"), gamma_run.get("zero_gamma")),
        ("GROSS_GEX", audit.get("gross_gex"), structure.get("gross_gex"), gamma_run.get("gross_gex")),
        ("NET_GEX", audit.get("net_gex"), structure.get("net_gex"), gamma_run.get("net_gex")),
    )
    for name, *values in value_pairs:
        if not all(_same_number(values[0], value) for value in values[1:]):
            issues.append(f"{name}_PERSISTENCE_MISMATCH")
    call_gex = _finite(audit.get("call_gex_total"))
    put_gex = _finite(audit.get("put_gex_total"))
    gross_gex = _finite(audit.get("gross_gex"))
    net_gex = _finite(audit.get("net_gex"))
    if None in (call_gex, put_gex, gross_gex, net_gex):
        issues.append("GEX_INVARIANTS_UNPROVEN")
    else:
        assert call_gex is not None and put_gex is not None
        assert gross_gex is not None and net_gex is not None
        tolerance = 1e-6 * max(1.0, abs(call_gex), abs(put_gex), abs(gross_gex), abs(net_gex))
        if not (
            gross_gex + tolerance >= abs(net_gex)
            and abs(gross_gex - (call_gex + put_gex)) <= tolerance
            and abs(net_gex - (call_gex - put_gex)) <= tolerance
        ):
            issues.append("GEX_INVARIANTS_FAILED")
    spot = _positive(audit.get("spot_last", audit.get("price")))
    gamma_pin = _positive(audit.get("gamma_pin"))
    positive_wall = _positive(audit.get("positive_gex_wall"))
    negative_wall = _positive(audit.get("negative_gex_wall"))
    if spot is None:
        issues.append("SPOT_MISSING")
    if gamma_pin is None:
        issues.append("GAMMA_PIN_MISSING")
    if positive_wall is None or negative_wall is None:
        issues.append("AUDIT_GEX_WALLS_MISSING")
    max_pain = _positive(audit.get("max_pain"))
    max_pain_source = str(audit.get("max_pain_source") or "").strip()
    if max_pain is not None and max_pain_source not in CANONICAL_MAX_PAIN_SOURCES:
        issues.append("MAX_PAIN_SOURCE_INVALID")
    if max_pain is not None and audit.get(
        "max_pain_formula_version"
    ) != CANONICAL_MAX_PAIN_FORMULA_VERSION:
        issues.append("MAX_PAIN_FORMULA_VERSION_INVALID")
    for name, value in (
        ("MAX_PAIN", audit.get("max_pain")),
        ("ZERO_GAMMA", audit.get("zero_gamma")),
    ):
        if value is not None and _positive(value) is None:
            issues.append(f"{name}_PRICE_INVALID")
    pin_is_contested = audit.get("pin_is_contested")
    if type(pin_is_contested) is not bool:
        issues.append("PIN_CONTEST_STATE_MISSING")
    observation_id = str(structure.get("observation_id") or "")
    if _SHA256.fullmatch(observation_id) is None:
        issues.append("OBSERVATION_ID_INVALID")

    issues = sorted(set(issues))
    if issues:
        return None, issues, diagnostic
    assert source_time is not None
    assert epoch is not None and generation is not None
    assert universe is not None and primary_expiration is not None and formula is not None
    assert calculation_id is not None and spot is not None and gamma_pin is not None
    assert gross_gex is not None and net_gex is not None
    normalized_net_gex = net_gex / gross_gex if gross_gex > 0.0 else None
    if normalized_net_gex is None or not math.isfinite(normalized_net_gex):
        return None, ["NORMALIZED_NET_GEX_UNAVAILABLE"], diagnostic
    top_strikes = audit.get("top_strikes_by_abs_gex")
    top_strikes = copy.deepcopy(top_strikes) if isinstance(top_strikes, list) else []
    candidate = {
        "observation_id": observation_id,
        "observed_at_utc": _utc_iso(source_time),
        "symbol": symbol,
        "eligible": True,
        "provider": "databento",
        "subscription_generation": generation,
        "subscription_epoch_id": epoch,
        "primary_expiration": primary_expiration,
        "gex_formula_version": formula,
        "universe_sha256": universe,
        "universe_is_fallback": False,
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "spot": spot,
        "gamma_pin": gamma_pin,
        "pin_is_contested": pin_is_contested,
        "pin_lead_ratio": _finite(audit.get("pin_lead_ratio")),
        "top_strikes_by_abs_gex": top_strikes,
        "max_pain": max_pain,
        "max_pain_source": max_pain_source or None,
        "max_pain_formula_version": audit.get("max_pain_formula_version"),
        "max_pain_as_of": audit.get("max_pain_as_of"),
        "zero_gamma": _positive(audit.get("zero_gamma")),
        "positive_gex_wall": positive_wall,
        "negative_gex_wall": negative_wall,
        "normalized_net_gex": normalized_net_gex,
        "volatility_regime": str(audit.get("vol_regime") or "").upper() or None,
        "calculation_id": calculation_id,
        "source_timestamp_utc": _utc_iso(source_time),
    }
    if prediction is not None:
        prediction_quote_time = _database_utc(prediction.get("quote_timestamp_utc"))
        prediction_time = _database_utc(prediction.get("timestamp_utc"))
        prediction_bias = str(prediction.get("net_bias") or "").lower()
        expected_move_pct = _finite(prediction.get("expected_move_pct"))
        current_price = _positive(prediction.get("current_price"))
        predicted_close = _positive(prediction.get("predicted_close"))
        prediction_reasons: list[str] = []
        if str(prediction.get("symbol") or "") != symbol:
            prediction_reasons.append("symbol_mismatch")
        if str(prediction.get("trading_date") or "") != session_date:
            prediction_reasons.append("trading_date_mismatch")
        if str(prediction.get("provider") or "").lower() != "databento":
            prediction_reasons.append("provider_not_databento")
        if prediction.get("prediction_mode") != CANONICAL_PREDICTION_MODE:
            prediction_reasons.append("prediction_mode_not_canonical")
        if prediction.get("model_version") != CANONICAL_PREDICTION_MODEL_VERSION:
            prediction_reasons.append("model_version_not_canonical")
        if prediction.get("model_type") != CANONICAL_PREDICTION_MODEL_TYPE:
            prediction_reasons.append("model_type_not_canonical")
        if (
            prediction.get("feature_schema_version")
            != CANONICAL_PREDICTION_FEATURE_SCHEMA_VERSION
        ):
            prediction_reasons.append("feature_schema_version_not_canonical")
        if prediction.get("subscription_epoch_id") != epoch:
            prediction_reasons.append("subscription_epoch_mismatch")
        if _positive_int(prediction.get("subscription_generation")) != generation:
            prediction_reasons.append("subscription_generation_mismatch")
        if prediction.get("is_valid") not in (True, 1):
            prediction_reasons.append("prediction_not_valid")
        if str(prediction.get("validation_status") or "").lower() != "valid":
            prediction_reasons.append("validation_status_not_valid")
        if current_price is None:
            prediction_reasons.append("current_price_not_positive_finite")
        if predicted_close is None:
            prediction_reasons.append("predicted_close_not_positive_finite")
        if expected_move_pct is None:
            prediction_reasons.append("expected_move_pct_not_finite")
        if prediction_quote_time is None:
            prediction_reasons.append("quote_timestamp_invalid")
        else:
            prediction_quote_age = (observed_at - prediction_quote_time).total_seconds()
            diagnostic["prediction_quote_age_seconds"] = prediction_quote_age
            if prediction_quote_time.astimezone(_CT).date().isoformat() != session_date:
                prediction_reasons.append("quote_session_date_mismatch")
            if (
                prediction_quote_age < -SOURCE_TIMESTAMP_TOLERANCE_SECONDS
                or prediction_quote_age > AUDIT_MAX_AGE_SECONDS
            ):
                prediction_reasons.append("quote_not_fresh")
            if (
                abs((prediction_quote_time - source_time).total_seconds())
                > SOURCE_TIMESTAMP_TOLERANCE_SECONDS
            ):
                prediction_reasons.append("quote_source_timestamp_mismatch")
        if prediction_time is None:
            prediction_reasons.append("prediction_timestamp_invalid")
        else:
            if prediction_time.astimezone(_CT).date().isoformat() != session_date:
                prediction_reasons.append("prediction_session_date_mismatch")
            if (prediction_time - observed_at).total_seconds() > SOURCE_TIMESTAMP_TOLERANCE_SECONDS:
                prediction_reasons.append("prediction_timestamp_in_future")
            if prediction_quote_time is not None and prediction_time < prediction_quote_time:
                prediction_reasons.append("prediction_before_quote")
        prediction_bias_agrees_with_move = bool(
            expected_move_pct is not None
            and (
                (prediction_bias == "bullish" and expected_move_pct > 0.0)
                or (prediction_bias == "bearish" and expected_move_pct < 0.0)
            )
        )
        if prediction_bias not in {"bullish", "bearish"}:
            prediction_reasons.append("prediction_bias_invalid")
        elif not prediction_bias_agrees_with_move:
            prediction_reasons.append("prediction_bias_move_disagreement")
        if (
            current_price is not None
            and predicted_close is not None
            and expected_move_pct is not None
            and not math.isclose(
                predicted_close,
                current_price * (1.0 + expected_move_pct / 100.0),
                rel_tol=1e-9,
                abs_tol=PREDICTION_PRICE_CONSISTENCY_ABS_TOLERANCE,
            )
        ):
            prediction_reasons.append("predicted_close_expected_move_inconsistent")
        for field in ("gamma_pin", "max_pain", "zero_gamma"):
            value = prediction.get(field)
            if value is not None and _positive(value) is None:
                prediction_reasons.append(f"{field}_not_positive_finite")
        prediction_values = (
            (prediction.get("current_price"), spot),
            (prediction.get("gamma_pin"), gamma_pin),
            (prediction.get("max_pain"), max_pain),
            (prediction.get("zero_gamma"), audit.get("zero_gamma")),
            (prediction.get("gross_gex"), gross_gex),
            (prediction.get("net_gex"), net_gex),
        )
        if not all(_same_number(left, right) for left, right in prediction_values):
            prediction_reasons.append("prediction_input_values_mismatch")
        prediction_reasons = sorted(set(prediction_reasons))
        prediction_aligned = not prediction_reasons
        diagnostic["prediction_context_status"] = (
            "aligned" if prediction_aligned else "unavailable_or_misaligned"
        )
        diagnostic["prediction_context_reasons"] = prediction_reasons
        if prediction_aligned:
            assert predicted_close is not None and expected_move_pct is not None
            assert prediction_time is not None and prediction_quote_time is not None
            candidate.update(
                {
                    "forecast_bias": prediction_bias,
                    "expected_move_pct": expected_move_pct,
                    "predicted_close": predicted_close,
                    "prediction_timestamp_utc": _utc_iso(prediction_time),
                    "prediction_quote_timestamp_utc": _utc_iso(
                        prediction_quote_time
                    ),
                }
            )
    return candidate, [], diagnostic


def _receipt_backed_scans(
    records: Sequence[Mapping[str, Any]], by_id: Mapping[str, Mapping[str, Any]]
) -> list[Mapping[str, Any]]:
    parent_ids = {
        str(record.get("parent_scan_event_id") or "")
        for record in records
        if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
    }
    scans = [
        by_id[event_id]
        for event_id in parent_ids
        if event_id in by_id and by_id[event_id].get("event_type") == "substantive_scan"
    ]
    return sorted(scans, key=lambda row: (str(row.get("observed_at_utc") or ""), str(row.get("event_id") or "")))


def _required_transport_counter(
    payload: Mapping[str, Any], field: str, *, source: str
) -> int:
    value = _nonnegative_int(payload.get(field))
    if value is None:
        raise MonitorScanCollectorError(
            f"transport_health_{source}_{field}_must_be_nonnegative_integer"
        )
    return value


def _transport_counter_snapshot(
    payload: Mapping[str, Any], *, source: str
) -> dict[str, int]:
    return {
        field: _required_transport_counter(payload, field, source=source)
        for field in TRANSPORT_COUNTER_FIELDS
    }


def _prior_regular_scan(
    *,
    session_date: str,
    current_scan_time: datetime,
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    candidates: list[Mapping[str, Any]] = []
    for scan in _receipt_backed_scans(records, by_id):
        observed = _utc(scan.get("observed_at_utc"))
        if (
            scan.get("session_date") == session_date
            and scan.get("phase") == "regular-session"
            and observed is not None
            and observed < current_scan_time
        ):
            candidates.append(scan)
    return candidates[-1] if candidates else None


def _transport_health_snapshot(
    evidence: Mapping[str, Any],
    *,
    phase: str,
    session_date: str,
    observed_at: datetime,
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build a compact, receipt-baselined shared-stream counter snapshot."""

    source_paths = {
        "live_first": "/health/live",
        "live_second": "/health/live",
        "overview": "/health",
    }
    if phase != "regular-session":
        return {
            "schema_version": TRANSPORT_HEALTH_SCHEMA,
            "scope": TRANSPORT_SCOPE,
            "evaluation_status": "outside_regular_session",
            "source_paths": source_paths,
            "subscription_epoch_id": None,
            "active_generation": None,
            "baseline_status": "not_evaluated",
            "baseline_scan_event_id": None,
            "baseline_observed_at_utc": None,
            "baseline_counters": None,
            "current_counters": None,
            "counter_deltas": None,
            "acquisition_reconnect": None,
            "acquisition_generations": None,
            "provider_context": None,
        }

    first = _endpoint_payload(evidence.get("health_first"))
    second = _endpoint_payload(evidence.get("health_second"))
    if first is None or second is None:
        # The existing live-health gate already makes both shared-stream
        # candidates unavailable. Preserve that durable diagnostic scan rather
        # than manufacturing counter values from an incomplete acquisition.
        return {
            "schema_version": TRANSPORT_HEALTH_SCHEMA,
            "scope": TRANSPORT_SCOPE,
            "evaluation_status": "live_health_unavailable",
            "source_paths": source_paths,
            "subscription_epoch_id": None,
            "active_generation": None,
            "baseline_status": "not_evaluated",
            "baseline_scan_event_id": None,
            "baseline_observed_at_utc": None,
            "baseline_counters": None,
            "current_counters": None,
            "counter_deltas": None,
            "acquisition_reconnect": None,
            "acquisition_generations": None,
            "provider_context": None,
        }
    overview = _endpoint_payload(evidence.get("health_overview"))
    if overview is None:
        raise MonitorScanCollectorError("transport_health_overview_unavailable")

    epochs = (
        str(first.get("subscription_epoch_id") or ""),
        str(second.get("subscription_epoch_id") or ""),
        str(overview.get("subscription_epoch_id") or ""),
    )
    if any(_SHA256.fullmatch(epoch) is None for epoch in epochs):
        raise MonitorScanCollectorError("transport_health_epoch_invalid")
    if len(set(epochs)) != 1:
        raise MonitorScanCollectorError(
            "transport_health_epoch_mismatch_during_acquisition"
        )
    epoch = epochs[0]

    generations = (
        _positive_int(first.get("subscription_generation")),
        _positive_int(second.get("subscription_generation")),
        _positive_int(overview.get("active_generation")),
    )
    if any(generation is None for generation in generations):
        raise MonitorScanCollectorError("transport_health_generation_invalid")
    first_generation, second_generation, overview_generation = generations
    assert first_generation is not None
    assert second_generation is not None
    assert overview_generation is not None
    if not first_generation <= second_generation <= overview_generation:
        raise MonitorScanCollectorError(
            "transport_health_generation_regressed_during_acquisition"
        )

    first_reconnects = _required_transport_counter(
        first, "reconnect_attempts", source="live_first"
    )
    second_reconnects = _required_transport_counter(
        second, "reconnect_attempts", source="live_second"
    )
    current = _transport_counter_snapshot(overview, source="overview")
    overview_reconnects = current["reconnect_attempts"]
    if not first_reconnects <= second_reconnects <= overview_reconnects:
        raise MonitorScanCollectorError(
            "transport_health_reconnect_attempts_regressed_during_acquisition"
        )
    if (
        len(set(generations)) != 1
        and overview_reconnects == first_reconnects
    ):
        raise MonitorScanCollectorError(
            "transport_health_generation_changed_without_reconnect"
        )

    raw_backpressure = overview.get("compute_backpressure_remaining_seconds")
    backpressure = _finite(raw_backpressure)
    if backpressure is None or backpressure < 0.0:
        raise MonitorScanCollectorError(
            "transport_health_compute_backpressure_remaining_seconds_invalid"
        )

    prior_scan = _prior_regular_scan(
        session_date=session_date,
        current_scan_time=observed_at,
        records=records,
        by_id=by_id,
    )
    baseline: dict[str, int] | None = None
    baseline_scan_event_id: str | None = None
    baseline_observed_at_utc: str | None = None
    baseline_status = "no_prior_receipt"
    if prior_scan is not None:
        prior_transport = (
            (prior_scan.get("evidence") or {}).get("transport_health")
            if isinstance(prior_scan.get("evidence"), Mapping)
            else None
        )
        if prior_transport is None:
            baseline_status = "prior_receipt_has_no_transport_snapshot"
        elif not isinstance(prior_transport, Mapping):
            raise MonitorScanCollectorError(
                "transport_health_prior_snapshot_must_be_an_object"
            )
        elif prior_transport.get("schema_version") != TRANSPORT_HEALTH_SCHEMA:
            raise MonitorScanCollectorError(
                "transport_health_prior_snapshot_schema_invalid"
            )
        elif prior_transport.get("scope") != TRANSPORT_SCOPE:
            raise MonitorScanCollectorError(
                "transport_health_prior_snapshot_scope_invalid"
            )
        elif prior_transport.get("current_counters") is None:
            if prior_transport.get("evaluation_status") not in {
                "live_health_unavailable",
                "outside_regular_session",
            }:
                raise MonitorScanCollectorError(
                    "transport_health_prior_snapshot_counters_missing"
                )
            baseline_status = "prior_transport_snapshot_unavailable"
        else:
            prior_epoch = str(prior_transport.get("subscription_epoch_id") or "")
            if _SHA256.fullmatch(prior_epoch) is None:
                raise MonitorScanCollectorError(
                    "transport_health_prior_snapshot_epoch_invalid"
                )
            if prior_epoch != epoch:
                baseline_status = "subscription_epoch_changed"
            else:
                raw_baseline = prior_transport.get("current_counters")
                if not isinstance(raw_baseline, Mapping):
                    raise MonitorScanCollectorError(
                        "transport_health_prior_counters_must_be_an_object"
                    )
                baseline = _transport_counter_snapshot(
                    raw_baseline, source="prior_receipt"
                )
                for field in TRANSPORT_COUNTER_FIELDS:
                    if current[field] < baseline[field]:
                        raise MonitorScanCollectorError(
                            f"transport_health_{field}_regressed_within_epoch"
                        )
                if first_reconnects < baseline["reconnect_attempts"]:
                    raise MonitorScanCollectorError(
                        "transport_health_reconnect_attempts_regressed_before_sample"
                    )
                baseline_scan_event_id = str(prior_scan.get("event_id") or "")
                if _SHA256.fullmatch(baseline_scan_event_id) is None:
                    raise MonitorScanCollectorError(
                        "transport_health_baseline_scan_event_id_invalid"
                    )
                baseline_observed = _utc(prior_scan.get("observed_at_utc"))
                if baseline_observed is None:
                    raise MonitorScanCollectorError(
                        "transport_health_baseline_observed_at_invalid"
                    )
                baseline_observed_at_utc = _utc_iso(baseline_observed)
                baseline_status = "same_epoch_receipt"

    deltas = {
        field: current[field] - baseline[field] if baseline is not None else None
        for field in TRANSPORT_COUNTER_FIELDS
    }
    return {
        "schema_version": TRANSPORT_HEALTH_SCHEMA,
        "scope": TRANSPORT_SCOPE,
        "evaluation_status": "compared" if baseline is not None else "seeded",
        "source_paths": source_paths,
        "subscription_epoch_id": epoch,
        "active_generation": overview_generation,
        "baseline_status": baseline_status,
        "baseline_scan_event_id": baseline_scan_event_id,
        "baseline_observed_at_utc": baseline_observed_at_utc,
        "baseline_counters": baseline,
        "current_counters": current,
        "counter_deltas": deltas,
        "acquisition_reconnect": {
            "first": first_reconnects,
            "second": second_reconnects,
            "overview": overview_reconnects,
            "delta": overview_reconnects - first_reconnects,
        },
        "acquisition_generations": {
            "first": first_generation,
            "second": second_generation,
            "overview": overview_generation,
        },
        "provider_context": {
            "compute_backpressure_remaining_seconds": backpressure,
            "last_reconnect_utc": overview.get("last_reconnect_utc"),
            "last_provider_warning_utc": overview.get(
                "last_provider_warning_utc"
            ),
        },
    }


def _transport_deterioration_event(
    *, session_date: str, snapshot: Mapping[str, Any]
) -> dict[str, Any] | None:
    if snapshot.get("evaluation_status") not in {"seeded", "compared"}:
        return None
    current = snapshot.get("current_counters")
    if not isinstance(current, Mapping):
        return None
    baseline = snapshot.get("baseline_counters")
    deltas = snapshot.get("counter_deltas")
    transitions: dict[str, dict[str, int]] = {}
    if isinstance(baseline, Mapping) and isinstance(deltas, Mapping):
        for field in TRANSPORT_TRIGGER_COUNTER_FIELDS:
            delta = _nonnegative_int(deltas.get(field))
            previous = _nonnegative_int(baseline.get(field))
            observed = _nonnegative_int(current.get(field))
            if delta is None or previous is None or observed is None:
                raise MonitorScanCollectorError(
                    "transport_health_validated_delta_shape_invalid"
                )
            if delta > 0:
                transitions[field] = {
                    "previous": previous,
                    "current": observed,
                    "delta": delta,
                }
    else:
        acquisition = snapshot.get("acquisition_reconnect")
        if isinstance(acquisition, Mapping):
            delta = _nonnegative_int(acquisition.get("delta"))
            previous = _nonnegative_int(acquisition.get("first"))
            observed = _nonnegative_int(acquisition.get("overview"))
            if delta is None or previous is None or observed is None:
                raise MonitorScanCollectorError(
                    "transport_health_validated_acquisition_delta_shape_invalid"
                )
            if delta > 0:
                transitions["reconnect_attempts"] = {
                    "previous": previous,
                    "current": observed,
                    "delta": delta,
                }
    if not transitions:
        return None
    material = {
        "schema_version": TRANSPORT_DATA_QUALITY_SCHEMA,
        "session_date": session_date,
        "scope": TRANSPORT_SCOPE,
        "subscription_epoch_id": snapshot.get("subscription_epoch_id"),
        "baseline_scan_event_id": snapshot.get("baseline_scan_event_id"),
        "counter_transitions": transitions,
    }
    return {
        "schema_version": TRANSPORT_DATA_QUALITY_SCHEMA,
        "event_id": _sha256(material),
        "scope": TRANSPORT_SCOPE,
        "subscription_epoch_id": snapshot.get("subscription_epoch_id"),
        "issues": sorted(
            TRANSPORT_ISSUE_BY_COUNTER[field] for field in transitions
        ),
        "baseline_scan_event_id": snapshot.get("baseline_scan_event_id"),
        "counter_transitions": transitions,
    }


def _prior_candidate(
    symbol: str,
    *,
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    current_scan_time: datetime,
    mode: str,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    scans = [
        scan
        for scan in _receipt_backed_scans(records, by_id)
        if scan.get("session_date") == current_scan_time.astimezone(_CT).date().isoformat()
        and (_utc(scan.get("observed_at_utc")) or current_scan_time) < current_scan_time
    ]
    if not scans:
        return None, None, "confirmation_history_seeding"
    prior_scan = scans[-1]
    prior_time = _utc(prior_scan.get("observed_at_utc"))
    if prior_time is None:
        return None, None, "PRIOR_SCHEDULED_SCAN_TIME_INVALID"
    gap = (current_scan_time - prior_time).total_seconds()
    minimum, maximum = CONFIRMATION_GAP_BOUNDS[mode]
    if not minimum <= gap <= maximum:
        return None, str(prior_scan.get("event_id") or ""), "PRIOR_SCHEDULED_SCAN_GAP_INVALID"
    symbol_payload = (prior_scan.get("symbols") or {}).get(symbol)
    if not isinstance(symbol_payload, Mapping):
        return None, str(prior_scan.get("event_id") or ""), "PRIOR_SCHEDULED_SYMBOL_MISSING"
    candidate = symbol_payload.get("candidate_policy_observation")
    if not isinstance(candidate, Mapping):
        candidate = symbol_payload.get("policy_observation")
    if not isinstance(candidate, Mapping) or candidate.get("eligible") is not True:
        return None, str(prior_scan.get("event_id") or ""), "PRIOR_SCHEDULED_OBSERVATION_UNAVAILABLE"
    return copy.deepcopy(dict(candidate)), str(prior_scan.get("event_id") or ""), None


def _provenance_aligned(previous: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    fields = (
        "symbol",
        "provider",
        "subscription_generation",
        "subscription_epoch_id",
        "primary_expiration",
        "gex_formula_version",
        "universe_sha256",
        "universe_is_fallback",
    )
    return all(previous.get(field) == current.get(field) for field in fields)


def _supported_price_change_15m(
    symbol: str,
    current: Mapping[str, Any],
    *,
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    """Derive a trailing 15-minute move from receipt-backed same-session truth.

    The normal/elevated monitor cadence is not itself evidence of a 15-minute
    interval. Select the closest committed source observation inside a tight
    two-minute tolerance and retain the actual interval plus the exact ledger
    anchor so the policy never has to infer or relabel a shorter move.
    """

    current_time = _utc(current.get("observed_at_utc"))
    current_spot = _positive(current.get("spot"))
    current_observation_id = str(current.get("observation_id") or "")
    if (
        current_time is None
        or current_spot is None
        or _SHA256.fullmatch(current_observation_id) is None
    ):
        return None
    session_date = current_time.astimezone(_CT).date().isoformat()
    minimum_gap = PRICE_CHANGE_15M_WINDOW_SECONDS - PRICE_CHANGE_15M_TOLERANCE_SECONDS
    maximum_gap = PRICE_CHANGE_15M_WINDOW_SECONDS + PRICE_CHANGE_15M_TOLERANCE_SECONDS
    baselines: list[tuple[float, float, str, Mapping[str, Any], datetime]] = []
    for scan in _receipt_backed_scans(records, by_id):
        scan_event_id = str(scan.get("event_id") or "")
        if (
            _SHA256.fullmatch(scan_event_id) is None
            or scan.get("session_date") != session_date
            or scan.get("phase") != "regular-session"
        ):
            continue
        symbol_payload = (scan.get("symbols") or {}).get(symbol)
        if not isinstance(symbol_payload, Mapping):
            continue
        baseline = symbol_payload.get("candidate_policy_observation")
        if not isinstance(baseline, Mapping):
            baseline = symbol_payload.get("policy_observation")
        if (
            not isinstance(baseline, Mapping)
            or baseline.get("eligible") is not True
            or not _provenance_aligned(baseline, current)
        ):
            continue
        baseline_time = _utc(baseline.get("observed_at_utc"))
        baseline_spot = _positive(baseline.get("spot"))
        baseline_observation_id = str(baseline.get("observation_id") or "")
        if (
            baseline_time is None
            or baseline_spot is None
            or _SHA256.fullmatch(baseline_observation_id) is None
            or baseline_time.astimezone(_CT).date().isoformat() != session_date
        ):
            continue
        gap_seconds = (current_time - baseline_time).total_seconds()
        if not minimum_gap <= gap_seconds <= maximum_gap:
            continue
        baselines.append(
            (
                abs(gap_seconds - PRICE_CHANGE_15M_WINDOW_SECONDS),
                -baseline_time.timestamp(),
                scan_event_id,
                baseline,
                baseline_time,
            )
        )
    if not baselines:
        return None
    _, _, scan_event_id, baseline, baseline_time = min(
        baselines, key=lambda row: (row[0], row[1], row[2])
    )
    baseline_spot = _positive(baseline.get("spot"))
    assert baseline_spot is not None
    actual_interval_seconds = (current_time - baseline_time).total_seconds()
    price_change_pct = (current_spot - baseline_spot) / baseline_spot * 100.0
    if not math.isfinite(price_change_pct):
        return None
    return {
        "schema_version": SUPPORTED_PRICE_CHANGE_SCHEMA,
        "window_seconds": PRICE_CHANGE_15M_WINDOW_SECONDS,
        "actual_interval_seconds": actual_interval_seconds,
        "pct": price_change_pct,
        "baseline_scan_event_id": scan_event_id,
        "baseline_observation_id": str(baseline.get("observation_id")),
        "baseline_observed_at_utc": _utc_iso(baseline_time),
        "baseline_spot": baseline_spot,
        "current_observation_id": current_observation_id,
        "current_observed_at_utc": _utc_iso(current_time),
        "current_spot": current_spot,
    }


def _top_spacing(observations: Sequence[Mapping[str, Any]]) -> float | None:
    spacings: list[float] = []
    for observation in observations:
        raw = observation.get("top_strikes_by_abs_gex")
        strikes = sorted(
            {
                value
                for row in raw
                if isinstance(row, Mapping)
                and (value := _finite(row.get("strike"))) is not None
            }
        ) if isinstance(raw, list) else []
        spacings.extend(
            right - left
            for left, right in zip(strikes, strikes[1:])
            if right > left
        )
    return float(statistics.median(spacings)) if spacings else None


def _crossed(previous: Mapping[str, Any], current: Mapping[str, Any], field: str) -> bool:
    values = (
        _finite(previous.get("spot")),
        _finite(previous.get(field)),
        _finite(current.get("spot")),
        _finite(current.get(field)),
    )
    if any(value is None for value in values):
        return False
    previous_spot, previous_level, current_spot, current_level = values
    assert previous_spot is not None and previous_level is not None
    assert current_spot is not None and current_level is not None
    return (previous_spot - previous_level) * (current_spot - current_level) < 0.0


def _near(current: Mapping[str, Any], field: str) -> bool:
    spot = _positive(current.get("spot"))
    level = _positive(current.get(field))
    return bool(spot is not None and level is not None and abs(spot - level) / spot <= 0.001)


def _forecast_direction(observation: Mapping[str, Any]) -> str | None:
    bias = str(observation.get("forecast_bias") or "").lower()
    expected = _finite(observation.get("expected_move_pct"))
    if bias not in {"bullish", "bearish"} or expected is None or abs(expected) < 0.10:
        return None
    if (expected > 0.0) != (bias == "bullish"):
        return None
    return bias


def _trigger_matrix(previous: Mapping[str, Any], current: Mapping[str, Any], symbol: str) -> dict[str, bool]:
    spot = _positive(current.get("spot"))
    previous_pin = _positive(previous.get("gamma_pin"))
    current_pin = _positive(current.get("gamma_pin"))
    spacing = _top_spacing((previous, current))
    fallback = 20.0 if symbol == "SPX" else 75.0
    threshold = (
        max(0.0015 * spot, 2.0 * spacing)
        if spot is not None and spacing is not None
        else fallback
    )
    gex_previous = _finite(previous.get("normalized_net_gex"))
    gex_current = _finite(current.get("normalized_net_gex"))
    gex_near_or_crossed = bool(
        gex_current is not None
        and (
            abs(gex_current) <= 0.10
            or (gex_previous is not None and gex_previous * gex_current < 0.0)
        )
    )
    prior_forecast = _forecast_direction(previous)
    current_forecast = _forecast_direction(current)
    return {
        "half_threshold_gamma_pin_move": bool(
            previous_pin is not None
            and current_pin is not None
            and abs(current_pin - previous_pin) >= threshold / 2.0
        ),
        "contested_pin_leadership": bool(
            current.get("pin_is_contested") is True
            or (
                _finite(current.get("pin_lead_ratio")) is not None
                and float(current["pin_lead_ratio"]) <= 0.10
            )
        ),
        "spot_near_or_crossed_gamma_pin": _near(current, "gamma_pin")
        or _crossed(previous, current, "gamma_pin"),
        "spot_near_or_crossed_zero_gamma": _near(current, "zero_gamma")
        or _crossed(previous, current, "zero_gamma"),
        "spot_near_or_crossed_gex_wall": any(
            _near(current, field) or _crossed(previous, current, field)
            for field in ("positive_gex_wall", "negative_gex_wall")
        ),
        "normalized_net_gex_near_or_crossed_zero": gex_near_or_crossed,
        "forecast_bias_awaiting_confirmation": bool(
            current_forecast is not None and current_forecast != prior_forecast
        ),
        "high_volatility_regime": str(current.get("volatility_regime") or "").upper()
        == "HIGH",
    }


def _orb_breakout(evidence: Mapping[str, Any], symbol: str, candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    payload = _endpoint_payload(evidence.get("orb"))
    if payload is None:
        return None
    symbols = payload.get("symbols")
    state = symbols.get(symbol) if isinstance(symbols, Mapping) else None
    if not isinstance(state, Mapping):
        return None
    ranges = state.get("opening_ranges")
    if not isinstance(ranges, Mapping):
        return None
    for window in ("15m", "5m"):
        row = ranges.get(window)
        if not isinstance(row, Mapping):
            continue
        provenance = row.get("provenance")
        direction = str(row.get("breakout_direction") or "").lower()
        if (
            direction in {"bullish", "bearish"}
            and row.get("directional_evidence_eligible") is True
            and row.get("current_reference_fresh") is True
            and isinstance(provenance, Mapping)
            and provenance.get("range_provenance_aligned") is True
            and provenance.get("current_vs_range_aligned") is True
            and provenance.get("structure_vs_reference_aligned") is True
            and state.get("provenance", {}).get("active_subscription_epoch_id")
            == candidate.get("subscription_epoch_id")
            and state.get("provenance", {}).get("active_subscription_generation")
            == candidate.get("subscription_generation")
        ):
            return {
                "window": window,
                "direction": direction,
                "directional_evidence_eligible": True,
                "current_reference_fresh": True,
            }
    return None


def _data_quality_event_id(session_date: str, symbol: str, issues: Sequence[str]) -> str:
    return _sha256(
        {
            "schema_version": DATA_QUALITY_ID_SCHEMA,
            "session_date": session_date,
            "symbol": symbol,
            "issues": sorted(set(issues)),
        }
    )


def _seen_data_quality_ids(records: Sequence[Mapping[str, Any]]) -> set[str]:
    return {
        str(event_id)
        for record in records
        if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
        and isinstance(record.get("cadence_decision"), Mapping)
        for event_id in record["cadence_decision"].get(
            "active_data_quality_event_ids", []
        )
        if isinstance(event_id, str) and event_id
    }


def _phase(observed_ct: datetime) -> str:
    observed_et = observed_ct.astimezone(_ET)
    calendar = market_calendar_status(observed_et.date())
    if calendar.get("supported") is not True or calendar.get("market_open") is not True:
        return "off-hours"
    if observed_et < open_time_et(observed_et):
        return "pre-market"
    if observed_et < close_time_et(observed_et):
        return "regular-session"
    return "post-close"


def _active_data_quality_issues(phase: str, issues: Sequence[str]) -> list[str]:
    normalized = sorted(
        set(issues).difference(_NON_DATA_QUALITY_ELIGIBILITY_REASONS)
    )
    if phase == "regular-session":
        return normalized
    return [
        issue
        for issue in normalized
        if issue not in _OUTSIDE_REGULAR_SESSION_LIVE_ISSUES
    ]


def build_commit_request(
    *,
    evidence: Mapping[str, Any],
    state: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Build and preflight the exact JSON request consumed by the commit CLI."""

    if evidence.get("schema_version") != COLLECTOR_SCHEMA:
        raise MonitorScanCollectorError("evidence_schema_invalid")
    observed_utc = _utc(evidence.get("observed_at_utc"))
    if observed_utc is None:
        raise MonitorScanCollectorError("evidence_observed_at_utc_invalid")
    observed_ct = observed_utc.astimezone(_CT)
    session_date = observed_ct.date().isoformat()
    if state.get("session_date") != session_date:
        raise MonitorScanCollectorError("state_session_date_mismatch_prepare_required")
    mode = str(state.get("mode") or "")
    if mode not in CADENCE_SECONDS:
        raise MonitorScanCollectorError("state_cadence_mode_invalid")
    phase = _phase(observed_ct)

    global_health_issues = _health_issues(evidence)
    symbol_payloads: dict[str, Any] = {}
    policy_inputs: dict[str, Any] = {}
    flags: dict[str, dict[str, bool]] = {}
    data_quality_events: list[dict[str, Any]] = []
    transport_health = _transport_health_snapshot(
        evidence,
        phase=phase,
        session_date=session_date,
        observed_at=observed_utc,
        records=records,
        by_id=by_id,
    )
    transport_event = _transport_deterioration_event(
        session_date=session_date,
        snapshot=transport_health,
    )
    if transport_event is not None:
        data_quality_events.append(transport_event)

    for symbol in MONITORED_SYMBOLS:
        candidate, source_issues, diagnostic = _candidate_observation(
            symbol,
            evidence,
            observed_at=observed_utc,
            global_health_issues=global_health_issues,
        )
        audit_entry = (evidence.get("audits") or {}).get(symbol)
        audit = (
            audit_entry.get("payload")
            if isinstance(audit_entry, Mapping)
            and isinstance(audit_entry.get("payload"), Mapping)
            else None
        )
        future_profiles, future_diagnostic = _future_expiration_context(
            audit,
            candidate,
            session_date=session_date,
        )
        diagnostic["future_expiration_context"] = future_diagnostic
        payload: dict[str, Any] = {
            "eligible": False,
            "eligibility_reasons": list(source_issues),
            "collector_diagnostic": diagnostic,
            "future_expiration_profiles": future_profiles,
        }
        symbol_issues = list(source_issues)
        if phase != "regular-session":
            symbol_issues.append("SESSION_PHASE_NOT_REGULAR")
        previous = None
        prior_scan_id = None
        history_issue = None
        if candidate is not None and phase == "regular-session":
            price_change_15m = _supported_price_change_15m(
                symbol,
                candidate,
                records=records,
                by_id=by_id,
            )
            if price_change_15m is not None:
                candidate["supported_price_changes"] = {"15m": price_change_15m}
            payload["candidate_policy_observation"] = copy.deepcopy(candidate)
            previous, prior_scan_id, history_issue = _prior_candidate(
                symbol,
                records=records,
                by_id=by_id,
                current_scan_time=observed_utc,
                mode=mode,
            )
            if history_issue is not None:
                symbol_issues.append(history_issue)
                # The durable ledger requires an explicit marker whenever a
                # valid current candidate must seed/reseed confirmation history
                # instead of being policy-eligible on this scan.
                if history_issue != "confirmation_history_seeding":
                    symbol_issues.append("confirmation_history_seeding")
            elif previous is not None and not _provenance_aligned(previous, candidate):
                symbol_issues.append("PRIOR_SCHEDULED_PROVENANCE_MISMATCH")
            elif previous is not None:
                previous_source = _utc(previous.get("observed_at_utc"))
                current_source = _utc(candidate.get("observed_at_utc"))
                if (
                    previous_source is None
                    or current_source is None
                    or current_source <= previous_source
                    or previous.get("calculation_id") == candidate.get("calculation_id")
                    or previous.get("observation_id") == candidate.get("observation_id")
                ):
                    symbol_issues.append("PRIOR_SCHEDULED_OBSERVATION_NOT_ADVANCING")
                else:
                    orb = _orb_breakout(evidence, symbol, candidate)
                    if orb is not None:
                        candidate["orb_breakout"] = orb
                        payload["candidate_policy_observation"] = copy.deepcopy(candidate)
                    payload.update(
                        {
                            "eligible": True,
                            "eligibility_reasons": [],
                            "policy_observation": copy.deepcopy(candidate),
                            "prior_confirmation_scan_event_id": prior_scan_id,
                        }
                    )
                    policy_inputs[symbol] = {
                        "schema_version": POLICY_INPUT_SCHEMA,
                        "observations": [copy.deepcopy(previous), copy.deepcopy(candidate)],
                        "confirmation_cadence_seconds": CADENCE_SECONDS[mode],
                        "prior_scan_event_id": prior_scan_id,
                    }
                    flags[symbol] = _trigger_matrix(previous, candidate, symbol)
        # The ledger treats every persisted candidate that did not become a
        # policy input as confirmation history for a future substantive scan.
        # This includes provenance resets and non-advancing calculation IDs,
        # not only an empty or unusable prior-scan history.
        if (
            "candidate_policy_observation" in payload
            and payload["eligible"] is False
        ):
            symbol_issues.append("confirmation_history_seeding")
        payload["eligibility_reasons"] = sorted(set(symbol_issues))
        if payload["eligible"] is not True:
            flags[symbol] = {field: False for field in TRIGGER_FIELDS}
        active_symbol_issues = _active_data_quality_issues(phase, symbol_issues)
        suppressed_symbol_issues = sorted(
            set(symbol_issues).difference(active_symbol_issues)
        )
        if suppressed_symbol_issues:
            diagnostic["phase_suppressed_data_quality_issues"] = suppressed_symbol_issues
        if active_symbol_issues:
            event_id = _data_quality_event_id(
                session_date, symbol, active_symbol_issues
            )
            data_quality_events.append(
                {
                    "event_id": event_id,
                    "symbol": symbol,
                    "issues": active_symbol_issues,
                }
            )
        symbol_payloads[symbol] = payload

    active_ids = sorted(event["event_id"] for event in data_quality_events)
    seen_ids = _seen_data_quality_ids(records)
    new_ids = sorted(set(active_ids).difference(seen_ids))
    adaptive_evidence = {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": active_ids,
        "new_data_quality_event_ids": new_ids,
        "symbols": flags,
    }
    validate_cadence_evidence(adaptive_evidence)

    orb_payload = _endpoint_payload(evidence.get("orb"))
    for symbol in CONTEXT_SYMBOLS:
        symbol_payloads[symbol] = {
            "eligible": False,
            "context_only": True,
            "orb_available": bool(
                isinstance(orb_payload, Mapping)
                and isinstance(orb_payload.get("symbols"), Mapping)
                and isinstance(orb_payload["symbols"].get(symbol), Mapping)
            ),
        }
    first_health = _endpoint_payload(evidence.get("health_first")) or {}
    second_health = _endpoint_payload(evidence.get("health_second")) or {}
    first_messages = _finite(first_health.get("messages_received"))
    second_messages = _finite(second_health.get("messages_received"))
    message_delta = (
        second_messages - first_messages
        if first_messages is not None and second_messages is not None
        else None
    )
    database = evidence.get("database") if isinstance(evidence.get("database"), Mapping) else {}
    scan = {
        "schema_version": 2,
        "event_type": "substantive_scan",
        "observed_at_ct": observed_ct.isoformat(),
        "observed_at_utc": _utc_iso(observed_utc),
        "session_date": session_date,
        "phase": phase,
        "cadence": {
            "mode": mode,
            "substantive": True,
            "adaptive_evidence": adaptive_evidence,
        },
        "evidence": {
            "collector_schema_version": COLLECTOR_SCHEMA,
            "sample_seconds": evidence.get("sample_seconds"),
            "message_delta": message_delta,
            "transport_health": transport_health,
            "data_quality_events": data_quality_events,
            "database": {
                "path": database.get("path"),
                "journal_mode": database.get("journal_mode"),
                "quick_check": database.get("quick_check"),
                "table_summaries": copy.deepcopy(database.get("table_summaries") or {}),
            },
            "orb_endpoint_ok": _endpoint_payload(evidence.get("orb")) is not None,
        },
        "symbols": symbol_payloads,
        "alerts": [],
        "directional_interpretation": (
            "POLICY_CONTROLLED"
            if any(symbol_payloads[symbol]["eligible"] for symbol in MONITORED_SYMBOLS)
            else "ABSTAIN"
        ),
        "research_hypotheses": [],
    }
    request = {"scan": scan, "policy_inputs": policy_inputs}
    # Exercise the exact pre-commit normalization and wrapper validator without
    # touching the journal or state.
    prepared = with_scan_event_id(with_policy_input_hashes(scan, policy_inputs))
    validate_scan_wrapper(prepared)
    _canonical_bytes(request)
    return request


def collect_commit_request(
    *,
    project_root: Path,
    backend_url: str,
    database_path: Path,
    state_path: Path,
    journal_dir: Path,
    sample_seconds: float = 5.0,
    timeout_seconds: float = 5.0,
    evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Acquire or consume injected evidence and return one exact commit request."""

    supplied = copy.deepcopy(dict(evidence)) if isinstance(evidence, Mapping) else None
    observed = _utc(supplied.get("observed_at_utc")) if supplied is not None else datetime.now(_UTC)
    if observed is None:
        raise MonitorScanCollectorError("evidence_observed_at_utc_invalid")
    session_date = observed.astimezone(_CT).date().isoformat()
    state, records, by_id = load_monitor_context(
        state_path=state_path,
        journal_dir=journal_dir,
        session_date=session_date,
    )
    live_evidence = supplied or acquire_live_evidence(
        project_root=project_root,
        backend_url=backend_url,
        database_path=database_path,
        session_date=session_date,
        sample_seconds=sample_seconds,
        timeout_seconds=timeout_seconds,
    )
    # Acquisition can cross midnight/session boundaries; never relabel it.
    acquired_time = _utc(live_evidence.get("observed_at_utc"))
    if acquired_time is None or acquired_time.astimezone(_CT).date().isoformat() != session_date:
        raise MonitorScanCollectorError("acquisition_crossed_session_boundary")
    return build_commit_request(
        evidence=live_evidence,
        state=state,
        records=records,
        by_id=by_id,
    )
