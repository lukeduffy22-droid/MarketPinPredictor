from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Mapping, Sequence

import numpy as np

from .close_reconciliation import load_verified_close_parent_overrides
from .close_evidence import (
    resolve_verified_close_artifact,
    validate_official_close_reference,
    validate_source_artifact_sha256,
    validate_verified_close_observed_at,
)
from .dataset import PRODUCTION_FAMILIES
from .research import _session_block_improvement_interval


UTC = timezone.utc
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FORECAST_EVENT_TYPES = frozenset({"ATTEMPT_STARTED", "PREDICTED", "ABSTAIN"})
VOLATILITY_REGIMES = ("calm", "normal", "stressed", "unavailable")

FORECAST_EVENT_COLUMNS = (
    "event_key",
    "attempt_key",
    "event_type",
    "trading_date",
    "session_id",
    "prediction_mode",
    "model_version",
    "artifact_sha256",
    "decision_horizon_minutes",
    "feature_available_at_utc",
    "recorded_at_utc",
    "source_sha256",
    "feature_hash",
    "prediction_keys_json",
    "reasons_json",
    "record_sha256",
)

PROMOTED_SCORE_COLUMNS = (
    "score_key",
    "prediction_key",
    "close_observation_id",
    "family_root",
    "trading_date",
    "decision_horizon_minutes",
    "feature_available_at_utc",
    "model_version",
    "artifact_sha256",
    "calibration_evidence_sha256",
    "source_sha256",
    "reference_price",
    "predicted_level",
    "prediction_lower",
    "prediction_upper",
    "interval_target_coverage",
    "actual_close",
    "error_points",
    "absolute_error_points",
    "squared_error_points",
    "direction_hit",
    "persistence_error_points",
    "persistence_absolute_error_points",
    "persistence_squared_error_points",
    "persistence_direction_hit",
    "interval_covered",
    "interval_width_points",
    "interval_width_pct",
    "close_source",
    "close_source_reference",
    "close_source_artifact_sha256",
    "scored_at_utc",
    "record_sha256",
)

PROMOTED_GOVERNANCE_COLUMNS = (
    "prediction_key",
    "attempt_key",
    "family_root",
    "volatility_regime",
    "feature_hash",
    "feature_contract_sha256",
    "feature_values_json",
    "model_version",
    "artifact_sha256",
    "calibration_evidence_sha256",
    "source_sha256",
    "promotion_approval_contract_version",
    "promotion_proposal_sha256",
    "promotion_approval_receipt_sha256",
    "replay_identity_sha256",
    "replay_status",
    "replayed_log_return",
    "replay_log_return_absolute_error",
    "replay_log_return_absolute_tolerance",
    "replay_max_level_absolute_error",
    "replay_level_absolute_tolerance",
    "replay_verified_at_utc",
    "replay_result_sha256",
    "registered_at_utc",
    "record_sha256",
)

GOVERNANCE_SCHEMA = """
CREATE TABLE IF NOT EXISTS closing_tape_forecast_events (
    event_key TEXT PRIMARY KEY NOT NULL,
    attempt_key TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (
        event_type IN ('ATTEMPT_STARTED','PREDICTED','ABSTAIN')
    ),
    trading_date TEXT NOT NULL,
    session_id TEXT NOT NULL,
    prediction_mode TEXT NOT NULL,
    model_version TEXT,
    artifact_sha256 TEXT,
    decision_horizon_minutes INTEGER NOT NULL,
    feature_available_at_utc TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    source_sha256 TEXT,
    feature_hash TEXT,
    prediction_keys_json TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    record_sha256 TEXT NOT NULL UNIQUE,
    UNIQUE(attempt_key, event_type)
);
CREATE INDEX IF NOT EXISTS idx_closing_tape_forecast_events_date_mode
    ON closing_tape_forecast_events(
        trading_date, prediction_mode, model_version, decision_horizon_minutes
    );
CREATE TRIGGER IF NOT EXISTS closing_tape_forecast_events_no_update
BEFORE UPDATE ON closing_tape_forecast_events
BEGIN SELECT RAISE(ABORT, 'closing-tape forecast events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_forecast_events_no_delete
BEFORE DELETE ON closing_tape_forecast_events
BEGIN SELECT RAISE(ABORT, 'closing-tape forecast events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_forecast_events_no_replace
BEFORE INSERT ON closing_tape_forecast_events
WHEN EXISTS (
    SELECT 1 FROM closing_tape_forecast_events
    WHERE event_key=NEW.event_key
       OR record_sha256=NEW.record_sha256
       OR (
           attempt_key=NEW.attempt_key
           AND event_type=NEW.event_type
       )
)
BEGIN SELECT RAISE(ABORT, 'closing-tape forecast events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_forecast_terminal_requires_start
BEFORE INSERT ON closing_tape_forecast_events
WHEN NEW.event_type <> 'ATTEMPT_STARTED'
 AND NOT EXISTS (
    SELECT 1 FROM closing_tape_forecast_events
    WHERE attempt_key=NEW.attempt_key AND event_type='ATTEMPT_STARTED'
 )
BEGIN SELECT RAISE(ABORT, 'closing-tape forecast terminal event has no start'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_forecast_one_terminal
BEFORE INSERT ON closing_tape_forecast_events
WHEN NEW.event_type IN ('PREDICTED','ABSTAIN')
 AND EXISTS (
    SELECT 1 FROM closing_tape_forecast_events
    WHERE attempt_key=NEW.attempt_key AND event_type IN ('PREDICTED','ABSTAIN')
 )
BEGIN SELECT RAISE(ABORT, 'closing-tape forecast attempt already has a terminal event'); END;

CREATE TABLE IF NOT EXISTS closing_tape_promoted_prediction_governance (
    prediction_key TEXT PRIMARY KEY NOT NULL,
    attempt_key TEXT NOT NULL,
    family_root TEXT NOT NULL,
    volatility_regime TEXT NOT NULL CHECK (
        volatility_regime IN ('calm','normal','stressed','unavailable')
    ),
    feature_hash TEXT NOT NULL,
    feature_contract_sha256 TEXT NOT NULL,
    feature_values_json TEXT NOT NULL,
    model_version TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    calibration_evidence_sha256 TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    promotion_approval_contract_version TEXT NOT NULL,
    promotion_proposal_sha256 TEXT NOT NULL,
    promotion_approval_receipt_sha256 TEXT NOT NULL,
    replay_identity_sha256 TEXT NOT NULL,
    replay_status TEXT NOT NULL CHECK (
        replay_status='DETERMINISTIC_REPLAY_VERIFIED'
    ),
    replayed_log_return REAL NOT NULL,
    replay_log_return_absolute_error REAL NOT NULL,
    replay_log_return_absolute_tolerance REAL NOT NULL,
    replay_max_level_absolute_error REAL NOT NULL,
    replay_level_absolute_tolerance REAL NOT NULL,
    replay_verified_at_utc TEXT NOT NULL,
    replay_result_sha256 TEXT NOT NULL,
    registered_at_utc TEXT NOT NULL,
    record_sha256 TEXT NOT NULL UNIQUE,
    UNIQUE(attempt_key, family_root)
);
CREATE INDEX IF NOT EXISTS idx_closing_tape_promoted_governance_attempt
    ON closing_tape_promoted_prediction_governance(attempt_key, family_root);
CREATE INDEX IF NOT EXISTS idx_closing_tape_promoted_governance_model_regime
    ON closing_tape_promoted_prediction_governance(
        model_version, volatility_regime, family_root
    );
CREATE TRIGGER IF NOT EXISTS closing_tape_promoted_governance_no_update
BEFORE UPDATE ON closing_tape_promoted_prediction_governance
BEGIN SELECT RAISE(ABORT, 'closing-tape promoted governance is append-only'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_promoted_governance_no_delete
BEFORE DELETE ON closing_tape_promoted_prediction_governance
BEGIN SELECT RAISE(ABORT, 'closing-tape promoted governance is append-only'); END;
CREATE TRIGGER IF NOT EXISTS closing_tape_promoted_governance_no_replace
BEFORE INSERT ON closing_tape_promoted_prediction_governance
WHEN EXISTS (
    SELECT 1 FROM closing_tape_promoted_prediction_governance
    WHERE prediction_key=NEW.prediction_key
       OR record_sha256=NEW.record_sha256
       OR (
           attempt_key=NEW.attempt_key
           AND family_root=NEW.family_root
       )
)
BEGIN SELECT RAISE(ABORT, 'closing-tape promoted governance is append-only'); END;

CREATE TABLE IF NOT EXISTS promoted_prediction_accuracy_observations (
    score_key TEXT PRIMARY KEY NOT NULL,
    prediction_key TEXT NOT NULL,
    close_observation_id INTEGER NOT NULL,
    family_root TEXT NOT NULL,
    trading_date TEXT NOT NULL,
    decision_horizon_minutes INTEGER NOT NULL,
    feature_available_at_utc TEXT NOT NULL,
    model_version TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    calibration_evidence_sha256 TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    reference_price REAL NOT NULL,
    predicted_level REAL NOT NULL,
    prediction_lower REAL NOT NULL,
    prediction_upper REAL NOT NULL,
    interval_target_coverage REAL NOT NULL,
    actual_close REAL NOT NULL,
    error_points REAL NOT NULL,
    absolute_error_points REAL NOT NULL,
    squared_error_points REAL NOT NULL,
    direction_hit INTEGER NOT NULL,
    persistence_error_points REAL NOT NULL,
    persistence_absolute_error_points REAL NOT NULL,
    persistence_squared_error_points REAL NOT NULL,
    persistence_direction_hit INTEGER NOT NULL,
    interval_covered INTEGER NOT NULL,
    interval_width_points REAL NOT NULL,
    interval_width_pct REAL NOT NULL,
    close_source TEXT NOT NULL,
    close_source_reference TEXT NOT NULL,
    close_source_artifact_sha256 TEXT NOT NULL,
    scored_at_utc TEXT NOT NULL,
    record_sha256 TEXT NOT NULL UNIQUE,
    UNIQUE(prediction_key, close_observation_id)
);
CREATE INDEX IF NOT EXISTS idx_promoted_prediction_accuracy_model_date
    ON promoted_prediction_accuracy_observations(
        model_version, trading_date, family_root, decision_horizon_minutes
    );
CREATE TRIGGER IF NOT EXISTS promoted_prediction_accuracy_no_update
BEFORE UPDATE ON promoted_prediction_accuracy_observations
BEGIN SELECT RAISE(ABORT, 'promoted prediction accuracy is append-only'); END;
CREATE TRIGGER IF NOT EXISTS promoted_prediction_accuracy_no_delete
BEFORE DELETE ON promoted_prediction_accuracy_observations
BEGIN SELECT RAISE(ABORT, 'promoted prediction accuracy is append-only'); END;
CREATE TRIGGER IF NOT EXISTS promoted_prediction_accuracy_no_replace
BEFORE INSERT ON promoted_prediction_accuracy_observations
WHEN EXISTS (
    SELECT 1 FROM promoted_prediction_accuracy_observations
    WHERE score_key=NEW.score_key
       OR record_sha256=NEW.record_sha256
       OR (
           prediction_key=NEW.prediction_key
           AND close_observation_id=NEW.close_observation_id
       )
)
BEGIN SELECT RAISE(ABORT, 'promoted prediction accuracy is append-only'); END;
"""

GOVERNANCE_TABLE_CONTRACTS = {
    "closing_tape_forecast_events": {
        "columns": FORECAST_EVENT_COLUMNS,
        "primary_key": ("event_key",),
        "nullable": frozenset(
            {"model_version", "artifact_sha256", "source_sha256", "feature_hash"}
        ),
        "unique": frozenset(
            {("record_sha256",), ("attempt_key", "event_type")}
        ),
        "triggers": frozenset(
            {
                "closing_tape_forecast_events_no_update",
                "closing_tape_forecast_events_no_delete",
                "closing_tape_forecast_events_no_replace",
                "closing_tape_forecast_terminal_requires_start",
                "closing_tape_forecast_one_terminal",
            }
        ),
    },
    "closing_tape_promoted_prediction_governance": {
        "columns": PROMOTED_GOVERNANCE_COLUMNS,
        "primary_key": ("prediction_key",),
        "nullable": frozenset(),
        "unique": frozenset(
            {("record_sha256",), ("attempt_key", "family_root")}
        ),
        "triggers": frozenset(
            {
                "closing_tape_promoted_governance_no_update",
                "closing_tape_promoted_governance_no_delete",
                "closing_tape_promoted_governance_no_replace",
            }
        ),
    },
    "promoted_prediction_accuracy_observations": {
        "columns": PROMOTED_SCORE_COLUMNS,
        "primary_key": ("score_key",),
        "nullable": frozenset(),
        "unique": frozenset(
            {("record_sha256",), ("prediction_key", "close_observation_id")}
        ),
        "triggers": frozenset(
            {
                "promoted_prediction_accuracy_no_update",
                "promoted_prediction_accuracy_no_delete",
                "promoted_prediction_accuracy_no_replace",
            }
        ),
    },
}


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError("governance evidence must be strict canonical JSON") from exc


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _utc_iso(value: datetime | str) -> str:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if timestamp.tzinfo is None:
        raise ValueError("governance timestamps must be timezone-aware")
    return timestamp.astimezone(UTC).isoformat()


def _optional_sha256(value: str | None, *, field: str) -> str | None:
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().lower()
    if not SHA256_PATTERN.fullmatch(normalized):
        raise ValueError(f"{field} must be a 64-character hexadecimal SHA-256")
    return normalized


@contextmanager
def _connect(
    path: Path,
    *,
    read_only: bool = False,
) -> Iterator[sqlite3.Connection]:
    if read_only:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10.0)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, timeout=10.0)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        with connection:
            yield connection
    finally:
        connection.close()


def _table_columns(connection: sqlite3.Connection, table: str) -> tuple[str, ...]:
    return tuple(str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})"))


def _normalized_schema_sql(value: str) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    normalized = normalized.replace("create table if not exists", "create table")
    normalized = normalized.replace("create trigger if not exists", "create trigger")
    return normalized.rstrip(";")


def _declared_governance_table_sql() -> dict[str, str]:
    declared: dict[str, str] = {}
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s+(\(.*?\n\);)",
        re.DOTALL,
    )
    for match in pattern.finditer(GOVERNANCE_SCHEMA):
        name, body = match.groups()
        declared[name] = _normalized_schema_sql(f"CREATE TABLE {name} {body}")
    return declared


def _declared_governance_trigger_sql() -> dict[str, str]:
    declared: dict[str, str] = {}
    pattern = re.compile(
        r"CREATE TRIGGER IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s+(.*?END;)",
        re.DOTALL,
    )
    for match in pattern.finditer(GOVERNANCE_SCHEMA):
        name, body = match.groups()
        declared[name] = _normalized_schema_sql(f"CREATE TRIGGER {name} {body}")
    return declared


GOVERNANCE_TABLE_SQL = _declared_governance_table_sql()
GOVERNANCE_TRIGGER_SQL = _declared_governance_trigger_sql()


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _require_governance_table_schema(
    connection: sqlite3.Connection,
    table: str,
) -> None:
    contract = GOVERNANCE_TABLE_CONTRACTS[table]
    table_info = connection.execute(f"PRAGMA table_info({table})").fetchall()
    columns = tuple(str(row[1]) for row in table_info)
    primary_key = tuple(
        str(row[1])
        for row in sorted(
            (row for row in table_info if int(row[5]) > 0),
            key=lambda row: int(row[5]),
        )
    )
    not_null = frozenset(str(row[1]) for row in table_info if bool(row[3]))
    expected_columns = contract["columns"]
    expected_not_null = frozenset(expected_columns) - contract["nullable"]
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    table_sql = _normalized_schema_sql(str(table_row[0] or "")) if table_row else ""

    unique_constraints: set[tuple[str, ...]] = set()
    invalid_partial_unique = False
    for index in connection.execute(f"PRAGMA index_list({table})").fetchall():
        if not bool(index[2]) or str(index[3]) == "pk":
            continue
        if bool(index[4]):
            invalid_partial_unique = True
        index_name = str(index[1]).replace('"', '""')
        unique_constraints.add(
            tuple(
                str(row[2])
                for row in connection.execute(
                    f'PRAGMA index_info("{index_name}")'
                ).fetchall()
            )
        )

    trigger_rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
        (table,),
    ).fetchall()
    trigger_sql = {
        str(row[0]): _normalized_schema_sql(str(row[1] or "")) for row in trigger_rows
    }
    expected_triggers = contract["triggers"]
    trigger_set_matches = frozenset(trigger_sql) == expected_triggers
    trigger_sql_matches = trigger_set_matches and all(
        trigger_sql[name] == GOVERNANCE_TRIGGER_SQL.get(name)
        for name in expected_triggers
    )

    if (
        columns != expected_columns
        or primary_key != contract["primary_key"]
        or not_null != expected_not_null
        or frozenset(unique_constraints) != contract["unique"]
        or invalid_partial_unique
        or table_sql != GOVERNANCE_TABLE_SQL.get(table)
        or not trigger_sql_matches
    ):
        raise RuntimeError(
            f"{table} schema is incompatible; governance evidence requires an explicit reviewed migration"
        )


def _require_governance_schema(connection: sqlite3.Connection) -> None:
    for table in GOVERNANCE_TABLE_CONTRACTS:
        _require_governance_table_schema(connection, table)


def initialize_governance_ledger(market_db_path: str | Path) -> None:
    """Create only the companion governance ledgers and their immutability guards."""
    with _connect(Path(market_db_path)) as connection:
        connection.executescript(GOVERNANCE_SCHEMA)
        _require_governance_schema(connection)


def _event_semantics(row: sqlite3.Row | dict[str, object]) -> dict[str, object]:
    return {
        key: row[key]
        for key in FORECAST_EVENT_COLUMNS
        if key not in {"event_key", "recorded_at_utc", "record_sha256"}
    }


def _append_event(connection: sqlite3.Connection, payload: dict[str, object]) -> str:
    event_key = str(payload["event_key"])
    existing = connection.execute(
        "SELECT * FROM closing_tape_forecast_events WHERE event_key=?", (event_key,)
    ).fetchone()
    if existing is not None:
        _verify_forecast_event_row(existing)
        if _event_semantics(existing) != _event_semantics(payload):
            raise ValueError("conflicting immutable closing-tape forecast event")
        return event_key
    columns = FORECAST_EVENT_COLUMNS
    placeholders = ",".join("?" for _ in columns)
    connection.execute(
        f"INSERT INTO closing_tape_forecast_events ({','.join(columns)}) VALUES ({placeholders})",
        tuple(payload[column] for column in columns),
    )
    return event_key


def _binding_semantics(row: Mapping[str, object]) -> dict[str, object]:
    return {
        key: row[key]
        for key in PROMOTED_GOVERNANCE_COLUMNS
        if key not in {"registered_at_utc", "record_sha256"}
    }


def _append_governance_binding(
    connection: sqlite3.Connection,
    payload: dict[str, object],
) -> None:
    existing = connection.execute(
        """
        SELECT * FROM closing_tape_promoted_prediction_governance
        WHERE prediction_key=?
        """,
        (payload["prediction_key"],),
    ).fetchone()
    if existing is not None:
        _verify_governance_binding_row(existing)
        if _binding_semantics(existing) != _binding_semantics(payload):
            raise ValueError("conflicting immutable promoted prediction governance binding")
        return
    placeholders = ",".join("?" for _ in PROMOTED_GOVERNANCE_COLUMNS)
    connection.execute(
        f"INSERT INTO closing_tape_promoted_prediction_governance "
        f"({','.join(PROMOTED_GOVERNANCE_COLUMNS)}) VALUES ({placeholders})",
        tuple(payload[column] for column in PROMOTED_GOVERNANCE_COLUMNS),
    )


def _prediction_replay_identity(
    row: Mapping[str, object],
    *,
    feature_hash: str,
    proposal_sha256: str,
    receipt_sha256: str,
) -> str:
    from .production import PROMOTED_PREDICTION_COLUMNS

    return _hash(
        {
            "prediction": {
                column: row[column] for column in PROMOTED_PREDICTION_COLUMNS
            },
            "feature_hash": feature_hash,
            "promotion_proposal_sha256": proposal_sha256,
            "promotion_approval_receipt_sha256": receipt_sha256,
        }
    )


def _replay_result_evidence(row: Mapping[str, object]) -> dict[str, object]:
    return {
        key: row[key]
        for key in (
            "prediction_key",
            "replay_identity_sha256",
            "feature_hash",
            "feature_contract_sha256",
            "feature_values_json",
            "replay_status",
            "replayed_log_return",
            "replay_log_return_absolute_error",
            "replay_log_return_absolute_tolerance",
            "replay_max_level_absolute_error",
            "replay_level_absolute_tolerance",
            "replay_verified_at_utc",
        )
    }


def _validate_and_register_predicted_batch(
    connection: sqlite3.Connection,
    *,
    start: sqlite3.Row,
    attempt_key: str,
    prediction_keys: Sequence[str],
    prediction_batch: object,
    registered_at_utc: str,
) -> tuple[str, str, str, str]:
    """Validate persisted rows against loader-issued authority and bind provenance."""
    from .production import (
        REPLAY_LOG_RETURN_ABSOLUTE_TOLERANCE,
        require_promoted_prediction_batch,
        validate_promoted_close_prediction_batch_against_runtime,
        verify_promoted_prediction_batch_replay,
    )
    from .promotion import PROMOTION_APPROVAL_CONTRACT_VERSION
    from .surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH

    require_promoted_prediction_batch(prediction_batch)  # type: ignore[arg-type]
    runtime = prediction_batch.runtime  # type: ignore[attr-defined]
    placeholders = ",".join("?" for _ in prediction_keys)
    rows = connection.execute(
        f"SELECT * FROM promoted_close_predictions "
        f"WHERE prediction_key IN ({placeholders}) ORDER BY family_root",
        tuple(prediction_keys),
    ).fetchall()
    if len(rows) != len(PRODUCTION_FAMILIES):
        raise ValueError("predicted terminal keys are not a registered full production batch")
    found_keys = {str(row["prediction_key"]) for row in rows}
    if found_keys != set(prediction_keys):
        raise ValueError("predicted terminal keys do not match persisted predictions")
    row_payloads = [dict(row) for row in rows]
    validate_promoted_close_prediction_batch_against_runtime(row_payloads, runtime)

    if str(start["prediction_mode"]) != "tcbbo_promoted":
        raise ValueError("predicted governance terminal requires tcbbo_promoted mode")
    expected_feature_time = _utc_iso(str(start["feature_available_at_utc"]))
    expected_identity = {
        "trading_date": str(start["trading_date"]),
        "session_id": str(start["session_id"]),
        "decision_horizon_minutes": int(start["decision_horizon_minutes"]),
        "feature_available_at_utc": expected_feature_time,
    }
    for row in rows:
        actual_identity = {
            "trading_date": str(row["trading_date"]),
            "session_id": str(row["session_id"]),
            "decision_horizon_minutes": int(row["decision_horizon_minutes"]),
            "feature_available_at_utc": _utc_iso(str(row["feature_available_at_utc"])),
        }
        if actual_identity != expected_identity:
            raise ValueError("persisted prediction identity conflicts with its started attempt")

    predictions_by_family = {
        str(item.family_root).upper(): item for item in prediction_batch  # type: ignore[union-attr]
    }
    rows_by_family = {str(row["family_root"]).upper(): row for row in rows}
    if set(predictions_by_family) != set(PRODUCTION_FAMILIES):
        raise ValueError("loader-authorized prediction batch is not family-complete")
    for family, item in predictions_by_family.items():
        row = rows_by_family[family]
        for field, expected in item.to_dict().items():
            actual = bool(row[field]) if field == "is_estimate" else row[field]
            if isinstance(expected, float):
                if not math.isclose(float(actual), expected, rel_tol=0.0, abs_tol=1e-12):
                    raise ValueError(
                        f"persisted {family} prediction differs from loader-authorized batch"
                    )
            elif actual != expected:
                raise ValueError(
                    f"persisted {family} prediction differs from loader-authorized batch"
                )

    # A prediction is not decision-grade merely because its identity is
    # intact. Re-run the loader-verified artifact against the exact retained
    # five-row feature input before registering a PREDICTED terminal.
    replay_results = {
        result.family_root: result
        for result in verify_promoted_prediction_batch_replay(prediction_batch)  # type: ignore[arg-type]
    }
    feature_rows = dict(
        prediction_batch.model_feature_rows_by_family  # type: ignore[attr-defined]
    )
    if (
        set(replay_results) != set(PRODUCTION_FAMILIES)
        or set(feature_rows) != set(PRODUCTION_FAMILIES)
    ):
        raise ValueError("deterministic replay evidence is not family-complete")

    feature_hash = _optional_sha256(
        str(prediction_batch.feature_hash),  # type: ignore[attr-defined]
        field="feature_hash",
    )
    if feature_hash is None:
        raise ValueError("loader-authorized prediction batch feature hash is missing")
    approval_contract = str(runtime.promotion_approval_contract_version or "").strip()
    proposal_hash = _optional_sha256(
        str(runtime.promotion_proposal_sha256), field="promotion_proposal_sha256"
    )
    receipt_hash = _optional_sha256(
        str(runtime.promotion_approval_receipt_sha256),
        field="promotion_approval_receipt_sha256",
    )
    if (
        approval_contract != PROMOTION_APPROVAL_CONTRACT_VERSION
        or proposal_hash is None
        or receipt_hash is None
    ):
        raise ValueError("loader-authorized runtime lacks valid human approval identity")
    regimes = dict(prediction_batch.volatility_regime_by_family)  # type: ignore[attr-defined]
    if set(regimes) != set(PRODUCTION_FAMILIES) or any(
        regime not in VOLATILITY_REGIMES for regime in regimes.values()
    ):
        raise ValueError("loader-authorized prediction regime context is incomplete")

    shared_sources = {str(row["source_sha256"]).lower() for row in rows}
    if len(shared_sources) != 1:
        raise ValueError("promoted prediction batch does not share source provenance")
    source_hash = next(iter(shared_sources))
    for row in rows:
        family = str(row["family_root"]).upper()
        replay_identity = _prediction_replay_identity(
            row,
            feature_hash=feature_hash,
            proposal_sha256=proposal_hash,
            receipt_sha256=receipt_hash,
        )
        replay = replay_results[family]
        feature_values = feature_rows[family]
        feature_values_json = _canonical_json(
            [float(value).hex() for value in feature_values]
        )
        if len(feature_values) != len(MODEL_FEATURE_COLUMNS):
            raise ValueError(f"retained model feature row is incomplete for {family}")
        payload: dict[str, object] = {
            "prediction_key": str(row["prediction_key"]),
            "attempt_key": attempt_key,
            "family_root": family,
            "volatility_regime": regimes[family],
            "feature_hash": feature_hash,
            "feature_contract_sha256": MODEL_FEATURE_CONTRACT_HASH,
            "feature_values_json": feature_values_json,
            "model_version": str(row["model_version"]),
            "artifact_sha256": str(row["artifact_sha256"]).lower(),
            "calibration_evidence_sha256": str(
                row["calibration_evidence_sha256"]
            ).lower(),
            "source_sha256": source_hash,
            "promotion_approval_contract_version": approval_contract,
            "promotion_proposal_sha256": proposal_hash,
            "promotion_approval_receipt_sha256": receipt_hash,
            "replay_identity_sha256": replay_identity,
            "replay_status": "DETERMINISTIC_REPLAY_VERIFIED",
            "replayed_log_return": replay.replayed_log_return,
            "replay_log_return_absolute_error": replay.log_return_absolute_error,
            "replay_log_return_absolute_tolerance": (
                REPLAY_LOG_RETURN_ABSOLUTE_TOLERANCE
            ),
            "replay_max_level_absolute_error": replay.max_level_absolute_error,
            "replay_level_absolute_tolerance": replay.level_absolute_tolerance,
            "replay_verified_at_utc": registered_at_utc,
            "registered_at_utc": registered_at_utc,
        }
        payload["replay_result_sha256"] = _hash(_replay_result_evidence(payload))
        payload["record_sha256"] = _hash(
            {
                key: payload[key]
                for key in PROMOTED_GOVERNANCE_COLUMNS
                if key != "record_sha256"
            }
        )
        _append_governance_binding(connection, payload)
    return (
        str(runtime.version),
        str(runtime.artifact_sha256).lower(),
        source_hash,
        feature_hash,
    )


def start_forecast_attempt(
    market_db_path: str | Path,
    *,
    trading_day: date,
    session_id: str,
    prediction_mode: str,
    decision_horizon_minutes: int,
    feature_available_at_utc: datetime,
    recorded_at_utc: datetime | None = None,
    model_version: str | None = None,
    artifact_sha256: str | None = None,
    source_sha256: str | None = None,
    feature_hash: str | None = None,
) -> str:
    """Append the durable denominator event before replay or inference begins."""
    normalized_session = str(session_id or "").strip()
    normalized_mode = str(prediction_mode or "").strip()
    if not normalized_session or not normalized_mode or decision_horizon_minutes < 1:
        raise ValueError("session, prediction mode, and positive decision horizon are required")
    feature_time = _utc_iso(feature_available_at_utc)
    recorded_time = _utc_iso(recorded_at_utc or datetime.now(UTC))
    feature_timestamp = datetime.fromisoformat(feature_time)
    recorded_timestamp = datetime.fromisoformat(recorded_time)
    if recorded_timestamp < feature_timestamp:
        raise ValueError("forecast attempt cannot start before features are available")
    if recorded_timestamp >= feature_timestamp + timedelta(
        minutes=int(decision_horizon_minutes)
    ):
        raise ValueError("forecast attempt must start before its target close")
    artifact_hash = _optional_sha256(artifact_sha256, field="artifact_sha256")
    source_hash = _optional_sha256(source_sha256, field="source_sha256")
    normalized_feature_hash = _optional_sha256(feature_hash, field="feature_hash")
    identity = {
        "trading_date": trading_day.isoformat(),
        "session_id": normalized_session,
        "prediction_mode": normalized_mode,
        "decision_horizon_minutes": int(decision_horizon_minutes),
        "feature_available_at_utc": feature_time,
    }
    attempt_key = _hash(identity)
    event_key = _hash({"attempt_key": attempt_key, "event_type": "ATTEMPT_STARTED"})
    payload: dict[str, object] = {
        "event_key": event_key,
        "attempt_key": attempt_key,
        "event_type": "ATTEMPT_STARTED",
        **identity,
        "model_version": str(model_version).strip() if model_version else None,
        "artifact_sha256": artifact_hash,
        "recorded_at_utc": recorded_time,
        "source_sha256": source_hash,
        "feature_hash": normalized_feature_hash,
        "prediction_keys_json": "[]",
        "reasons_json": "[]",
    }
    payload["record_sha256"] = _hash(
        {key: payload[key] for key in FORECAST_EVENT_COLUMNS if key != "record_sha256"}
    )
    initialize_governance_ledger(market_db_path)
    with _connect(Path(market_db_path)) as connection:
        _require_governance_schema(connection)
        _append_event(connection, payload)
    return attempt_key


def finish_forecast_attempt(
    market_db_path: str | Path,
    attempt_key: str,
    *,
    event_type: str,
    prediction_keys: Sequence[str] = (),
    reasons: Sequence[str] = (),
    recorded_at_utc: datetime | None = None,
    model_version: str | None = None,
    artifact_sha256: str | None = None,
    source_sha256: str | None = None,
    feature_hash: str | None = None,
    prediction_batch: object | None = None,
) -> str:
    """Append exactly one terminal PREDICTED or ABSTAIN event for an attempt."""
    terminal = str(event_type).upper()
    if terminal not in {"PREDICTED", "ABSTAIN"}:
        raise ValueError("terminal event_type must be PREDICTED or ABSTAIN")
    keys = tuple(sorted({str(value).strip() for value in prediction_keys if str(value).strip()}))
    normalized_reasons = tuple(dict.fromkeys(str(value).strip() for value in reasons if str(value).strip()))
    if terminal == "PREDICTED" and len(keys) != len(PRODUCTION_FAMILIES):
        raise ValueError("a predicted closing-tape batch requires every production-family prediction key")
    if terminal == "ABSTAIN" and (keys or not normalized_reasons):
        raise ValueError("an abstention requires reasons and cannot contain prediction keys")
    if terminal == "PREDICTED" and normalized_reasons:
        raise ValueError("a predicted event cannot contain abstention reasons")
    if terminal == "PREDICTED" and prediction_batch is None:
        raise ValueError(
            "a predicted terminal requires its loader-authorized promoted prediction batch"
        )
    if terminal == "ABSTAIN" and prediction_batch is not None:
        raise ValueError("an abstention cannot carry a promoted prediction batch")

    path = Path(market_db_path)
    initialize_governance_ledger(path)
    with _connect(path) as connection:
        _require_governance_schema(connection)
        start = connection.execute(
            """
            SELECT * FROM closing_tape_forecast_events
            WHERE attempt_key=? AND event_type='ATTEMPT_STARTED'
            """,
            (attempt_key,),
        ).fetchone()
        if start is None:
            raise KeyError(attempt_key)
        _verify_forecast_event_row(start)
        existing_terminal = connection.execute(
            """
            SELECT * FROM closing_tape_forecast_events
            WHERE attempt_key=? AND event_type IN ('PREDICTED','ABSTAIN')
            """,
            (attempt_key,),
        ).fetchone()
        if existing_terminal is not None:
            _verify_forecast_event_row(existing_terminal)
        event_key = _hash({"attempt_key": attempt_key, "event_type": terminal})
        if (
            existing_terminal is not None
            and str(existing_terminal["event_key"]) != event_key
        ):
            raise ValueError(
                "closing-tape forecast attempt already has a conflicting terminal event"
            )

        def resolved_text(field: str, supplied: str | None) -> str | None:
            existing_value = str(start[field]).strip() if start[field] is not None else None
            supplied_value = str(supplied).strip() if supplied is not None and str(supplied).strip() else None
            if existing_value and supplied_value and existing_value != supplied_value:
                raise ValueError(f"terminal {field} conflicts with the started attempt")
            return supplied_value or existing_value

        # A retry may occur after the target close. Reuse the immutable first
        # terminal timestamp so an otherwise identical retry remains
        # idempotent without pretending the forecast was issued later.
        recorded_time = (
            str(existing_terminal["recorded_at_utc"])
            if existing_terminal is not None
            else _utc_iso(recorded_at_utc or datetime.now(UTC))
        )
        if datetime.fromisoformat(recorded_time) < datetime.fromisoformat(str(start["recorded_at_utc"])):
            raise ValueError("terminal forecast event cannot predate its start")
        if terminal == "PREDICTED" and datetime.fromisoformat(recorded_time) >= (
            datetime.fromisoformat(_utc_iso(str(start["feature_available_at_utc"])))
            + timedelta(minutes=int(start["decision_horizon_minutes"]))
        ):
            raise ValueError("predicted terminal event must be recorded before its target close")
        if terminal == "PREDICTED":
            batch_model, batch_artifact, batch_source, batch_feature = (
                _validate_and_register_predicted_batch(
                    connection,
                    start=start,
                    attempt_key=attempt_key,
                    prediction_keys=keys,
                    prediction_batch=prediction_batch,
                    registered_at_utc=recorded_time,
                )
            )
            for field, supplied, expected in (
                ("model_version", model_version, batch_model),
                ("artifact_sha256", artifact_sha256, batch_artifact),
                ("source_sha256", source_sha256, batch_source),
                ("feature_hash", feature_hash, batch_feature),
            ):
                if supplied is not None and str(supplied).strip().lower() != expected.lower():
                    raise ValueError(f"terminal {field} conflicts with the authorized batch")
            model_version = batch_model
            artifact_sha256 = batch_artifact
            source_sha256 = batch_source
            feature_hash = batch_feature

        resolved_artifact = _optional_sha256(
            resolved_text("artifact_sha256", artifact_sha256), field="artifact_sha256"
        )
        resolved_source = _optional_sha256(
            resolved_text("source_sha256", source_sha256), field="source_sha256"
        )
        resolved_feature = _optional_sha256(
            resolved_text("feature_hash", feature_hash), field="feature_hash"
        )
        payload: dict[str, object] = {
            "event_key": event_key,
            "attempt_key": attempt_key,
            "event_type": terminal,
            "trading_date": start["trading_date"],
            "session_id": start["session_id"],
            "prediction_mode": start["prediction_mode"],
            "model_version": resolved_text("model_version", model_version),
            "artifact_sha256": resolved_artifact,
            "decision_horizon_minutes": start["decision_horizon_minutes"],
            "feature_available_at_utc": start["feature_available_at_utc"],
            "recorded_at_utc": recorded_time,
            "source_sha256": resolved_source,
            "feature_hash": resolved_feature,
            "prediction_keys_json": _canonical_json(keys),
            "reasons_json": _canonical_json(normalized_reasons),
        }
        payload["record_sha256"] = _hash(
            {key: payload[key] for key in FORECAST_EVENT_COLUMNS if key != "record_sha256"}
        )
        if existing_terminal is not None:
            if _event_semantics(existing_terminal) != _event_semantics(payload):
                raise ValueError("closing-tape forecast attempt already has a conflicting terminal event")
            return event_key
        return _append_event(connection, payload)


def _verify_governance_binding_row(binding: Mapping[str, object]) -> None:
    expected = _hash(
        {
            key: binding[key]
            for key in PROMOTED_GOVERNANCE_COLUMNS
            if key != "record_sha256"
        }
    )
    if str(binding["record_sha256"]) != expected:
        raise ValueError("promoted governance binding record hash is invalid")


def _verify_forecast_event_row(event: Mapping[str, object]) -> None:
    expected = _hash(
        {
            key: event[key]
            for key in FORECAST_EVENT_COLUMNS
            if key != "record_sha256"
        }
    )
    if str(event["record_sha256"]) != expected:
        raise ValueError("closing-tape forecast event record hash is invalid")


def _retained_model_feature_values(binding: Mapping[str, object]) -> tuple[float, ...]:
    from .surface import MODEL_FEATURE_COLUMNS

    try:
        encoded = json.loads(str(binding["feature_values_json"]))
    except json.JSONDecodeError as exc:
        raise ValueError("retained promoted model features are malformed") from exc
    if (
        not isinstance(encoded, list)
        or len(encoded) != len(MODEL_FEATURE_COLUMNS)
        or any(not isinstance(value, str) for value in encoded)
    ):
        raise ValueError("retained promoted model feature row is incomplete")
    try:
        values = tuple(float.fromhex(value) for value in encoded)
    except ValueError as exc:
        raise ValueError("retained promoted model feature value is invalid") from exc
    if not np.isfinite(np.asarray(values, dtype=float)).all() or any(
        value.hex() != encoded[index] for index, value in enumerate(values)
    ):
        raise ValueError("retained promoted model feature value is not canonical")
    return values


def _verify_persisted_replay_result(
    binding: Mapping[str, object],
    prediction: Mapping[str, object],
) -> None:
    if str(binding["replay_status"]) != "DETERMINISTIC_REPLAY_VERIFIED":
        raise ValueError("promoted prediction lacks deterministic replay verification")
    if str(binding["replay_result_sha256"]) != _hash(
        _replay_result_evidence(binding)
    ):
        raise ValueError("promoted deterministic replay result hash is invalid")
    numeric = np.asarray(
        [
            binding["replayed_log_return"],
            binding["replay_log_return_absolute_error"],
            binding["replay_log_return_absolute_tolerance"],
            binding["replay_max_level_absolute_error"],
            binding["replay_level_absolute_tolerance"],
        ],
        dtype=float,
    )
    if not np.isfinite(numeric).all() or (numeric[1:] < 0).any():
        raise ValueError("promoted deterministic replay evidence is non-finite")
    if numeric[1] > numeric[2] or numeric[3] > numeric[4]:
        raise ValueError("promoted deterministic replay exceeded its declared tolerance")
    if not math.isclose(
        float(binding["replayed_log_return"]),
        float(prediction["predicted_log_return"]),
        rel_tol=0.0,
        abs_tol=float(binding["replay_log_return_absolute_tolerance"]),
    ):
        raise ValueError("promoted replay output conflicts with persisted prediction")
    if _utc_iso(str(binding["replay_verified_at_utc"])) != _utc_iso(
        str(binding["registered_at_utc"])
    ):
        raise ValueError("promoted replay verification time is inconsistent")


def _validate_approval_binding(
    project_root: str | Path,
    binding: Mapping[str, object],
    prediction: Mapping[str, object],
) -> None:
    from .promotion import (
        PROMOTION_APPROVAL_CONTRACT_VERSION,
        load_promotion_approval_receipt,
    )

    if (
        str(binding["promotion_approval_contract_version"])
        != PROMOTION_APPROVAL_CONTRACT_VERSION
    ):
        raise ValueError("promoted prediction approval contract is unsupported")
    receipt = load_promotion_approval_receipt(
        project_root,
        str(binding["promotion_approval_receipt_sha256"]),
    )
    expected = {
        "proposal_sha256": str(binding["promotion_proposal_sha256"]).lower(),
        "model_version": str(prediction["model_version"]),
        "artifact_sha256": str(prediction["artifact_sha256"]).lower(),
        "calibration_evidence_sha256": str(
            prediction["calibration_evidence_sha256"]
        ).lower(),
    }
    for field, value in expected.items():
        actual = str(receipt.get(field) or "")
        if field.endswith("sha256"):
            actual = actual.lower()
        if actual != value:
            raise ValueError(
                f"promoted prediction approval receipt {field} does not match issuance"
            )


def _governed_prediction_groups(
    connection: sqlite3.Connection,
    *,
    trading_day: date | None = None,
    prediction_keys: Sequence[str] = (),
) -> list[tuple[sqlite3.Row, list[sqlite3.Row], list[sqlite3.Row]]]:
    tables = _tables(connection)
    required = {
        "promoted_close_predictions",
        "closing_tape_forecast_events",
        "closing_tape_promoted_prediction_governance",
    }
    if not required <= tables:
        return []
    _require_governance_table_schema(connection, "closing_tape_forecast_events")
    _require_governance_table_schema(
        connection,
        "closing_tape_promoted_prediction_governance",
    )
    clauses: list[str] = ["e.event_type='PREDICTED'"]
    parameters: list[object] = []
    if trading_day is not None:
        clauses.append("e.trading_date=?")
        parameters.append(trading_day.isoformat())
    if prediction_keys:
        placeholders = ",".join("?" for _ in prediction_keys)
        clauses.append(f"g.prediction_key IN ({placeholders})")
        parameters.extend(prediction_keys)
    attempts = connection.execute(
        f"""
        SELECT DISTINCT e.*
        FROM closing_tape_forecast_events e
        JOIN closing_tape_promoted_prediction_governance g
          ON g.attempt_key=e.attempt_key
        WHERE {' AND '.join(clauses)}
        ORDER BY e.trading_date, e.recorded_at_utc, e.attempt_key
        """,
        tuple(parameters),
    ).fetchall()
    groups: list[tuple[sqlite3.Row, list[sqlite3.Row], list[sqlite3.Row]]] = []
    requested = set(prediction_keys)
    for event in attempts:
        bindings = connection.execute(
            """
            SELECT * FROM closing_tape_promoted_prediction_governance
            WHERE attempt_key=? ORDER BY family_root
            """,
            (event["attempt_key"],),
        ).fetchall()
        keys = tuple(str(binding["prediction_key"]) for binding in bindings)
        if requested and not requested.intersection(keys):
            continue
        placeholders = ",".join("?" for _ in keys)
        predictions = connection.execute(
            f"SELECT * FROM promoted_close_predictions "
            f"WHERE prediction_key IN ({placeholders}) ORDER BY family_root",
            keys,
        ).fetchall() if keys else []
        groups.append((event, list(bindings), list(predictions)))
    return groups


def _validated_governed_rows(
    connection: sqlite3.Connection,
    *,
    project_root: str | Path,
    trading_day: date | None = None,
    prediction_keys: Sequence[str] = (),
) -> list[dict[str, object]]:
    from .production import (
        promoted_feature_evidence_sha256,
        validate_promoted_close_prediction_batch,
    )
    from .surface import MODEL_FEATURE_CONTRACT_HASH

    selected: list[dict[str, object]] = []
    requested = set(prediction_keys)
    for event, bindings, predictions in _governed_prediction_groups(
        connection,
        trading_day=trading_day,
        prediction_keys=prediction_keys,
    ):
        _verify_forecast_event_row(event)
        if len(bindings) != len(PRODUCTION_FAMILIES) or len(predictions) != len(
            PRODUCTION_FAMILIES
        ):
            raise ValueError("governed promoted prediction batch is incomplete")
        event_keys = json.loads(str(event["prediction_keys_json"]))
        if not isinstance(event_keys, list):
            raise ValueError("governed promoted prediction key evidence is malformed")
        binding_keys = {str(binding["prediction_key"]) for binding in bindings}
        prediction_keys_in_rows = {
            str(prediction["prediction_key"]) for prediction in predictions
        }
        if set(map(str, event_keys)) != binding_keys or binding_keys != prediction_keys_in_rows:
            raise ValueError("governed promoted prediction identities do not agree")
        validate_promoted_close_prediction_batch([dict(row) for row in predictions])
        bindings_by_key = {
            str(binding["prediction_key"]): binding for binding in bindings
        }
        feature_rows: list[tuple[str, tuple[float, ...]]] = []
        for prediction in predictions:
            key = str(prediction["prediction_key"])
            binding = bindings_by_key[key]
            _verify_governance_binding_row(binding)
            if str(binding["feature_contract_sha256"]) != MODEL_FEATURE_CONTRACT_HASH:
                raise ValueError("promoted model feature contract hash is invalid")
            feature_rows.append(
                (
                    str(prediction["family_root"]).upper(),
                    _retained_model_feature_values(binding),
                )
            )
            for field in (
                "family_root", "model_version", "artifact_sha256",
                "calibration_evidence_sha256", "source_sha256",
            ):
                if str(binding[field]) != str(prediction[field]):
                    raise ValueError(
                        f"governance binding {field} conflicts with promoted prediction"
                    )
            expected_replay = _prediction_replay_identity(
                prediction,
                feature_hash=str(binding["feature_hash"]),
                proposal_sha256=str(binding["promotion_proposal_sha256"]),
                receipt_sha256=str(binding["promotion_approval_receipt_sha256"]),
            )
            if str(binding["replay_identity_sha256"]) != expected_replay:
                raise ValueError("promoted prediction replay identity is invalid")
            _verify_persisted_replay_result(binding, prediction)
            _validate_approval_binding(project_root, binding, prediction)
            if requested and key not in requested:
                continue
            payload = dict(prediction)
            payload.update(
                forecast_id=key,
                validation_state="VALID",
                decision_grade=True,
                attempt_key=str(binding["attempt_key"]),
                feature_hash=str(binding["feature_hash"]),
                volatility_regime=str(binding["volatility_regime"]),
                promotion_approval_contract_version=str(
                    binding["promotion_approval_contract_version"]
                ),
                promotion_proposal_sha256=str(
                    binding["promotion_proposal_sha256"]
                ),
                promotion_approval_receipt_sha256=str(
                    binding["promotion_approval_receipt_sha256"]
                ),
                replay_identity_sha256=str(binding["replay_identity_sha256"]),
                replay_status=str(binding["replay_status"]),
                deterministic_replay_verified=True,
                replayed_log_return=float(binding["replayed_log_return"]),
                replay_log_return_absolute_error=float(
                    binding["replay_log_return_absolute_error"]
                ),
                replay_log_return_absolute_tolerance=float(
                    binding["replay_log_return_absolute_tolerance"]
                ),
                replay_max_level_absolute_error=float(
                    binding["replay_max_level_absolute_error"]
                ),
                replay_level_absolute_tolerance=float(
                    binding["replay_level_absolute_tolerance"]
                ),
                replay_verified_at_utc=str(binding["replay_verified_at_utc"]),
                replay_result_sha256=str(binding["replay_result_sha256"]),
                governance_record_sha256=str(binding["record_sha256"]),
            )
            payload["is_estimate"] = bool(payload["is_estimate"])
            selected.append(payload)
        expected_feature_hash = promoted_feature_evidence_sha256(
            [dict(row) for row in predictions],
            feature_rows,
        )
        binding_feature_hashes = {
            str(binding["feature_hash"]) for binding in bindings
        }
        if binding_feature_hashes != {expected_feature_hash}:
            raise ValueError("retained promoted model inputs do not match feature hash")
    if requested:
        found = {str(row["prediction_key"]) for row in selected}
        if found != requested:
            raise ValueError(
                "one or more selected predictions are not registered decision-grade forecasts"
            )
    return selected


def load_governed_promoted_close_predictions(
    market_db_path: str | Path,
    *,
    project_root: str | Path,
    trading_day: date | None = None,
) -> list[dict[str, object]]:
    """Read only terminal, approval-bound production forecasts; never infer or write."""
    path = Path(market_db_path)
    if not path.is_file():
        return []
    with _connect(path, read_only=True) as connection:
        selected_day = trading_day
        if selected_day is None:
            tables = _tables(connection)
            if "closing_tape_forecast_events" not in tables:
                return []
            _require_governance_table_schema(
                connection,
                "closing_tape_forecast_events",
            )
            latest = connection.execute(
                """
                SELECT MAX(trading_date) FROM closing_tape_forecast_events
                WHERE event_type='PREDICTED' AND prediction_mode='tcbbo_promoted'
                """
            ).fetchone()[0]
            if latest is None:
                return []
            selected_day = date.fromisoformat(str(latest))
        return _validated_governed_rows(
            connection,
            project_root=project_root,
            trading_day=selected_day,
        )


def _sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _score_record(
    prediction: sqlite3.Row,
    close: sqlite3.Row,
    *,
    scored_at_utc: str,
) -> dict[str, object]:
    family = str(prediction["family_root"]).upper()
    close_source = str(close["source"] or "").lower()
    close_reference = validate_official_close_reference(
        family, close_source, str(close["source_reference"] or "")
    )
    close_artifact = validate_source_artifact_sha256(str(close["source_artifact_sha256"] or ""))
    artifact_hash = _optional_sha256(str(prediction["artifact_sha256"]), field="artifact_sha256")
    calibration_hash = _optional_sha256(
        str(prediction["calibration_evidence_sha256"]), field="calibration_evidence_sha256"
    )
    source_hash = _optional_sha256(str(prediction["source_sha256"]), field="source_sha256")
    numeric = np.asarray(
        [
            prediction["reference_price"], prediction["predicted_level"],
            prediction["prediction_lower"], prediction["prediction_upper"],
            prediction["interval_target_coverage"], close["official_close"],
        ],
        dtype=float,
    )
    if not np.isfinite(numeric).all():
        raise ValueError(f"{family} promoted prediction or close contains non-finite evidence")
    reference, predicted, lower, upper, target_coverage, actual = map(float, numeric)
    if not 0 < lower <= predicted <= upper or reference <= 0 or actual <= 0:
        raise ValueError(f"{family} promoted prediction or close is outside its valid domain")
    if not 0 < target_coverage < 1:
        raise ValueError(f"{family} interval target coverage is invalid")
    feature_time = datetime.fromisoformat(
        _utc_iso(str(prediction["feature_available_at_utc"]))
    )
    target_time = feature_time.timestamp() + int(
        prediction["decision_horizon_minutes"]
    ) * 60
    observed_time = validate_verified_close_observed_at(
        str(close["trading_date"]),
        _utc_iso(str(close["observed_at_utc"])),
    )
    if observed_time.timestamp() < target_time:
        raise ValueError(f"{family} verified close was observed before the forecast horizon")
    error = predicted - actual
    persistence_error = reference - actual
    actual_direction = _sign(actual - reference)
    interval_width = upper - lower
    payload: dict[str, object] = {
        "score_key": hashlib.sha256(
            f"{prediction['prediction_key']}:{close['id']}".encode("utf-8")
        ).hexdigest(),
        "prediction_key": prediction["prediction_key"],
        "close_observation_id": int(close["id"]),
        "family_root": family,
        "trading_date": str(prediction["trading_date"]),
        "decision_horizon_minutes": int(prediction["decision_horizon_minutes"]),
        "feature_available_at_utc": str(prediction["feature_available_at_utc"]),
        "model_version": str(prediction["model_version"]),
        "artifact_sha256": artifact_hash,
        "calibration_evidence_sha256": calibration_hash,
        "source_sha256": source_hash,
        "reference_price": reference,
        "predicted_level": predicted,
        "prediction_lower": lower,
        "prediction_upper": upper,
        "interval_target_coverage": target_coverage,
        "actual_close": actual,
        "error_points": error,
        "absolute_error_points": abs(error),
        "squared_error_points": error * error,
        "direction_hit": int(_sign(predicted - reference) == actual_direction),
        "persistence_error_points": persistence_error,
        "persistence_absolute_error_points": abs(persistence_error),
        "persistence_squared_error_points": persistence_error * persistence_error,
        "persistence_direction_hit": int(actual_direction == 0),
        "interval_covered": int(lower <= actual <= upper),
        "interval_width_points": interval_width,
        "interval_width_pct": interval_width / reference * 100.0,
        "close_source": close_source,
        "close_source_reference": close_reference,
        "close_source_artifact_sha256": close_artifact,
        "scored_at_utc": scored_at_utc,
    }
    payload["record_sha256"] = _hash(payload)
    return payload


def _reproduce_score_row(
    connection: sqlite3.Connection,
    score: Mapping[str, object],
    governed_by_key: Mapping[str, Mapping[str, object]],
) -> Mapping[str, object]:
    try:
        expected_record_hash = _hash(
            {
                key: score[key]
                for key in PROMOTED_SCORE_COLUMNS
                if key != "record_sha256"
            }
        )
        close_observation_id = int(score["close_observation_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            "promoted prediction score identity is malformed"
        ) from exc
    if str(score["record_sha256"]) != expected_record_hash:
        raise RuntimeError("promoted prediction score record hash is invalid")

    prediction_key = str(score["prediction_key"])
    governed = governed_by_key.get(prediction_key)
    if governed is None:
        raise RuntimeError(
            "promoted prediction score is not bound to a governed prediction"
        )
    close = connection.execute(
        "SELECT * FROM eod_close_observations WHERE id=?",
        (close_observation_id,),
    ).fetchone()
    if close is None:
        raise RuntimeError(
            "promoted prediction score references a missing verified close observation"
        )
    try:
        source_verified = int(close["source_verified"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "promoted prediction score close verification state is malformed"
        ) from exc
    if source_verified != 1:
        raise RuntimeError(
            "promoted prediction score references an unverified close observation"
        )
    if (
        str(close["symbol"]).upper() != str(governed["family_root"]).upper()
        or str(close["trading_date"]) != str(governed["trading_date"])
    ):
        raise RuntimeError(
            "promoted prediction score close identity conflicts with its governed prediction"
        )
    try:
        reproduced = _score_record(
            governed,  # type: ignore[arg-type]
            close,
            scored_at_utc=str(score["scored_at_utc"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"promoted prediction score cannot be reproduced from retained evidence: {exc}"
        ) from exc
    compared_columns = tuple(
        column
        for column in PROMOTED_SCORE_COLUMNS
        if column not in {"scored_at_utc", "record_sha256"}
    )
    mismatched = [
        column
        for column in compared_columns
        if score[column] != reproduced[column]
    ]
    if mismatched:
        raise RuntimeError(
            "promoted prediction score does not reproduce from its governed prediction "
            f"and verified close: {','.join(mismatched)}"
        )
    return governed


def _latest_verified_close_for_prediction(
    connection: sqlite3.Connection,
    prediction: Mapping[str, object],
) -> sqlite3.Row | None:
    family = str(prediction["family_root"]).upper()
    trading_date = str(prediction["trading_date"])
    try:
        parent_overrides = load_verified_close_parent_overrides(connection)
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"verified close reconciliation is invalid: {exc}") from exc
    observations = connection.execute(
        "SELECT * FROM eod_close_observations "
        "WHERE symbol=? AND trading_date=? ORDER BY id",
        (family, trading_date),
    ).fetchall()
    by_id = {int(row["id"]): row for row in observations}
    verified: list[sqlite3.Row] = []
    roots: list[sqlite3.Row] = []
    children: dict[int, sqlite3.Row] = {}
    for row in observations:
        try:
            is_verified = int(row["source_verified"]) == 1
            correction_of_id = (
                int(parent_overrides.get(int(row["id"]), row["correction_of_id"]))
                if int(row["id"]) in parent_overrides or row["correction_of_id"] is not None
                else None
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("verified close lineage is malformed") from exc
        if correction_of_id is not None:
            prior = by_id.get(correction_of_id)
            if (
                prior is None
                or correction_of_id >= int(row["id"])
                or str(prior["symbol"]).upper() != family
                or str(prior["trading_date"]) != trading_date
                or int(prior["source_verified"]) != 1
            ):
                raise RuntimeError(
                    "verified close correction lineage is missing, mismatched, or non-causal"
                )
        if is_verified:
            verified.append(row)
            if correction_of_id is None:
                roots.append(row)
            else:
                if correction_of_id in children:
                    raise RuntimeError("verified close correction lineage forks")
                children[correction_of_id] = row
    if not verified:
        return None
    if len(roots) != 1:
        raise RuntimeError("verified close observations require exactly one causal root")
    latest = roots[0]
    visited = {int(latest["id"])}
    while int(latest["id"]) in children:
        latest = children[int(latest["id"])]
        if int(latest["id"]) in visited:
            raise RuntimeError("verified close correction lineage cycles")
        visited.add(int(latest["id"]))
    if visited != {int(row["id"]) for row in verified}:
        raise RuntimeError("verified close correction lineage is disconnected")
    try:
        _score_record(
            prediction,  # type: ignore[arg-type]
            latest,
            scored_at_utc="authoritative-close-validation",
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"latest verified close cannot be validated for {family}: {exc}"
        ) from exc
    return latest


def _require_retained_close_artifact(
    project_root: Path,
    close: Mapping[str, object],
) -> None:
    try:
        resolve_verified_close_artifact(
            project_root / "data" / "verified_close_sources",
            trading_date=str(close["trading_date"]),
            symbol=str(close["symbol"]),
            source_artifact_sha256=str(close["source_artifact_sha256"]),
        )
    except (KeyError, OSError, ValueError) as exc:
        raise RuntimeError(
            f"verified close artifact is missing or invalid: {exc}"
        ) from exc


def score_promoted_prediction_outcomes(
    market_db_path: str | Path,
    *,
    prediction_keys: Sequence[str] | None = None,
    trading_day: date | None = None,
    scored_at_utc: datetime | None = None,
    project_root: str | Path | None = None,
) -> dict[str, object]:
    """Append verified-close outcomes for an explicit key set or trading date."""
    selected_keys = tuple(sorted({str(value).strip() for value in (prediction_keys or ()) if str(value).strip()}))
    if not selected_keys and trading_day is None:
        return {"scored": 0, "reason": "prediction_keys_or_trading_date_required"}
    path = Path(market_db_path)
    if not path.is_file():
        return {"scored": 0, "reason": "market_database_missing"}
    initialize_governance_ledger(path)
    with _connect(path) as connection:
        _require_governance_schema(connection)
        tables = _tables(connection)
        required_tables = {
            "promoted_close_predictions",
            "eod_close_observations",
            "closing_tape_forecast_events",
            "closing_tape_promoted_prediction_governance",
        }
        if not required_tables <= tables:
            return {
                "scored": 0,
                "reason": "required_prediction_or_close_evidence_missing",
                "missing_tables": sorted(required_tables - tables),
            }
        root = (
            Path(project_root).resolve()
            if project_root is not None
            else path.resolve().parent.parent
        )
        predictions = _validated_governed_rows(
            connection,
            project_root=root,
            trading_day=None if selected_keys else trading_day,
            prediction_keys=selected_keys,
        )
        if not predictions:
            return {"scored": 0, "reason": "no_governed_promoted_predictions_selected"}

        scored_time = _utc_iso(scored_at_utc or datetime.now(UTC))
        payloads: list[dict[str, object]] = []
        ineligible: dict[str, str] = {}
        for prediction in predictions:
            close = _latest_verified_close_for_prediction(connection, prediction)
            if close is None:
                ineligible[str(prediction["prediction_key"])] = "missing_verified_close"
                continue
            _require_retained_close_artifact(root, close)
            try:
                payloads.append(_score_record(prediction, close, scored_at_utc=scored_time))
            except (ValueError, TypeError, KeyError) as exc:
                ineligible[str(prediction["prediction_key"])] = str(exc)
        if ineligible and not payloads:
            return {
                "scored": 0,
                "reason": "prediction_batch_not_scoreable",
                "ineligible_reasons": ineligible,
            }

        inserted = 0
        for payload in payloads:
            existing = connection.execute(
                "SELECT * FROM promoted_prediction_accuracy_observations WHERE score_key=?",
                (payload["score_key"],),
            ).fetchone()
            if existing is not None:
                expected_existing_hash = _hash(
                    {
                        key: existing[key]
                        for key in PROMOTED_SCORE_COLUMNS
                        if key != "record_sha256"
                    }
                )
                if str(existing["record_sha256"]) != expected_existing_hash:
                    raise ValueError("promoted prediction score record hash is invalid")
                comparable = {
                    key: payload[key]
                    for key in PROMOTED_SCORE_COLUMNS
                    if key not in {"scored_at_utc", "record_sha256"}
                }
                stored = {key: existing[key] for key in comparable}
                if stored != comparable:
                    raise ValueError("conflicting immutable promoted prediction score")
                continue
            placeholders = ",".join("?" for _ in PROMOTED_SCORE_COLUMNS)
            connection.execute(
                f"INSERT INTO promoted_prediction_accuracy_observations "
                f"({','.join(PROMOTED_SCORE_COLUMNS)}) VALUES ({placeholders})",
                tuple(payload[column] for column in PROMOTED_SCORE_COLUMNS),
            )
            inserted += 1
        return {
            "scored": len(payloads),
            "inserted": inserted,
            "reason": "partial_verified_outcomes" if ineligible else None,
            "prediction_keys": [str(payload["prediction_key"]) for payload in payloads],
            "close_observation_ids": sorted(
                {int(payload["close_observation_id"]) for payload in payloads}
            ),
            "ineligible_reasons": ineligible,
        }


def _empty_metric(reason: str, *, rows: int = 0, sessions: int = 0) -> dict[str, object]:
    return {
        "metrics_available": False,
        "reason": reason,
        "scored_rows": rows,
        "scored_sessions": sessions,
        "candidate_mae": None,
        "candidate_rmse": None,
        "candidate_direction_hit_rate": None,
        "persistence_mae": None,
        "persistence_rmse": None,
        "persistence_direction_hit_rate": None,
        "candidate_mae_improvement_pct": None,
        "candidate_mae_improvement_ci_low_pct": None,
        "candidate_mae_improvement_ci_high_pct": None,
        "interval_target_coverage": None,
        "interval_empirical_coverage": None,
        "interval_coverage_error": None,
        "mean_interval_width_points": None,
        "mean_interval_width_pct": None,
    }


def _metric_summary(
    rows: Sequence[dict[str, object]],
    *,
    minimum_scored_sessions: int,
) -> dict[str, object]:
    sessions = len({str(row["trading_date"]) for row in rows})
    if sessions < minimum_scored_sessions:
        return _empty_metric(
            f"scored sessions {sessions} < minimum {minimum_scored_sessions}",
            rows=len(rows),
            sessions=sessions,
        )
    families = {str(row["family_root"]) for row in rows}
    if len(families) > 1:
        return _empty_metric(
            "raw point-error aggregation across different symbols is suppressed",
            rows=len(rows),
            sessions=sessions,
        )
    identities = {
        (
            str(row["model_version"]), str(row["artifact_sha256"]),
            str(row["calibration_evidence_sha256"]),
            float(row["interval_target_coverage"]),
        )
        for row in rows
    }
    if len(identities) != 1:
        return _empty_metric(
            "score evidence mixes model, artifact, or calibration identity",
            rows=len(rows),
            sessions=sessions,
        )
    actual = np.asarray([float(row["actual_close"]) for row in rows], dtype=float)
    candidate = np.asarray([float(row["predicted_level"]) for row in rows], dtype=float)
    persistence = np.asarray([float(row["reference_price"]) for row in rows], dtype=float)
    blocks = np.asarray([str(row["trading_date"]) for row in rows])
    candidate_abs = np.abs(candidate - actual)
    persistence_abs = np.abs(persistence - actual)
    candidate_mae = float(np.mean(candidate_abs))
    persistence_mae = float(np.mean(persistence_abs))
    improvement = (
        (persistence_mae - candidate_mae) / persistence_mae * 100.0
        if persistence_mae > 0 else None
    )
    ci_low, ci_high = _session_block_improvement_interval(
        actual, candidate, persistence, blocks, seed=1877
    )
    if not math.isfinite(ci_low) or not math.isfinite(ci_high):
        ci_low, ci_high = None, None
    target_coverage = next(iter(identities))[3]
    empirical_coverage = float(np.mean([int(row["interval_covered"]) for row in rows]))
    return {
        "metrics_available": True,
        "reason": None,
        "scored_rows": len(rows),
        "scored_sessions": sessions,
        "candidate_mae": candidate_mae,
        "candidate_rmse": float(math.sqrt(np.mean(np.square(candidate - actual)))),
        "candidate_direction_hit_rate": float(np.mean([int(row["direction_hit"]) for row in rows])),
        "persistence_mae": persistence_mae,
        "persistence_rmse": float(math.sqrt(np.mean(np.square(persistence - actual)))),
        "persistence_direction_hit_rate": float(
            np.mean([int(row["persistence_direction_hit"]) for row in rows])
        ),
        "candidate_mae_improvement_pct": improvement,
        "candidate_mae_improvement_ci_low_pct": ci_low,
        "candidate_mae_improvement_ci_high_pct": ci_high,
        "interval_target_coverage": target_coverage,
        "interval_empirical_coverage": empirical_coverage,
        "interval_coverage_error": abs(empirical_coverage - target_coverage),
        "mean_interval_width_points": float(
            np.mean([float(row["interval_width_points"]) for row in rows])
        ),
        "mean_interval_width_pct": float(
            np.mean([float(row["interval_width_pct"]) for row in rows])
        ),
    }


def _attempt_summary(
    rows: Sequence[sqlite3.Row],
    *,
    model_version: str | None,
    decision_horizon_minutes: int | None,
    governed_prediction_keys: set[str],
) -> dict[str, object]:
    grouped: dict[str, dict[str, sqlite3.Row]] = {}
    for row in rows:
        _verify_forecast_event_row(row)
        if str(row["prediction_mode"]) != "tcbbo_promoted":
            continue
        if decision_horizon_minutes is not None and int(row["decision_horizon_minutes"]) != decision_horizon_minutes:
            continue
        grouped.setdefault(str(row["attempt_key"]), {})[str(row["event_type"])] = row
    states: list[tuple[str, sqlite3.Row, sqlite3.Row | None]] = []
    for events in grouped.values():
        started = events.get("ATTEMPT_STARTED")
        if started is None:
            continue
        terminal = events.get("PREDICTED") or events.get("ABSTAIN")
        effective = terminal or started
        if model_version is not None and str(effective["model_version"] or "") != model_version:
            continue
        state = str(terminal["event_type"]) if terminal is not None else "UNRESOLVED"
        if state == "PREDICTED" and terminal is not None:
            try:
                keys = json.loads(str(terminal["prediction_keys_json"]))
            except json.JSONDecodeError:
                keys = None
            if (
                not isinstance(keys, list)
                or len(keys) != len(PRODUCTION_FAMILIES)
                or not set(map(str, keys)) <= governed_prediction_keys
            ):
                state = "INVALID_PREDICTED"
        states.append((state, started, terminal))
    counts = Counter(state for state, _started, _terminal in states)
    reasons: Counter[str] = Counter()
    for state, _started, terminal in states:
        if state != "ABSTAIN" or terminal is None:
            continue
        try:
            values = json.loads(str(terminal["reasons_json"]))
        except json.JSONDecodeError:
            values = ["MALFORMED_ABSTENTION_REASONS"]
        reasons.update(str(value) for value in values)
    opportunities = len(states)
    predicted = counts["PREDICTED"]
    abstained = counts["ABSTAIN"]
    unresolved = counts["UNRESOLVED"]
    invalid_predicted = counts["INVALID_PREDICTED"]
    return {
        "opportunities": opportunities,
        "predicted": predicted,
        "abstained": abstained,
        "unresolved": unresolved,
        "invalid_predicted": invalid_predicted,
        "availability_rate": predicted / opportunities if opportunities else None,
        "abstention_rate": abstained / opportunities if opportunities else None,
        "abstention_reasons": dict(sorted(reasons.items())),
        "scope": "production batch opportunities; family and regime filters apply to metrics",
    }


def build_closing_tape_scorecard(
    market_db_path: str | Path,
    *,
    model_version: str | None = None,
    family_root: str | None = None,
    decision_horizon_minutes: int | None = None,
    volatility_regime: str | None = None,
    minimum_scored_sessions: int = 5,
    project_root: str | Path | None = None,
) -> dict[str, object]:
    """Build a read-only, correction-aware production forecast scorecard."""
    if minimum_scored_sessions < 1:
        raise ValueError("minimum_scored_sessions must be positive")
    if decision_horizon_minutes is not None and decision_horizon_minutes < 1:
        raise ValueError("decision_horizon_minutes must be positive")
    family = str(family_root).upper() if family_root else None
    if family is not None and family not in PRODUCTION_FAMILIES:
        raise ValueError(f"unsupported production family: {family}")
    regime = str(volatility_regime).strip().lower() if volatility_regime else None
    if regime is not None and regime not in VOLATILITY_REGIMES:
        raise ValueError(
            "volatility_regime must be calm, normal, stressed, or unavailable"
        )
    path = Path(market_db_path)
    base: dict[str, object] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "filters": {
            "model_version": model_version,
            "family_root": family,
            "decision_horizon_minutes": decision_horizon_minutes,
            "volatility_regime": regime,
            "minimum_scored_sessions": minimum_scored_sessions,
        },
        "prediction_mode": "tcbbo_promoted",
    }
    if not path.is_file():
        return {
            **base,
            "available": False,
            "reason": "market database is missing",
            "attempts": {
                "opportunities": 0, "predicted": 0, "abstained": 0,
                "unresolved": 0, "invalid_predicted": 0,
                "availability_rate": None, "abstention_rate": None,
                "abstention_reasons": {},
                "scope": "production batch opportunities; family and regime filters apply to metrics",
            },
            "overall": _empty_metric("market database is missing"),
            "by_family": [],
            "by_symbol": [],
            "by_horizon": [],
            "by_regime": [],
            "by_model": [],
            "by_slice": [],
            "outcome_resolution": {
                "eligible_forecasts": 0,
                "resolved_forecasts": 0,
                "unresolved_forecasts": 0,
                "resolution_rate": None,
            },
        }
    with _connect(path, read_only=True) as connection:
        tables = _tables(connection)
        for governance_table in GOVERNANCE_TABLE_CONTRACTS:
            if governance_table in tables:
                _require_governance_table_schema(connection, governance_table)
        event_rows: list[sqlite3.Row] = []
        score_rows: list[dict[str, object]] = []
        root = (
            Path(project_root).resolve()
            if project_root is not None
            else path.resolve().parent.parent
        )
        try:
            governed_rows = _validated_governed_rows(
                connection,
                project_root=root,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"closing-tape governance evidence is invalid: {exc}"
            ) from exc
        governed_by_key = {
            str(row["prediction_key"]): row for row in governed_rows
        }
        if "closing_tape_forecast_events" in tables:
            event_rows = connection.execute(
                "SELECT * FROM closing_tape_forecast_events ORDER BY recorded_at_utc, event_key"
            ).fetchall()
        if "promoted_prediction_accuracy_observations" in tables:
            raw_scores = connection.execute(
                "SELECT * FROM promoted_prediction_accuracy_observations ORDER BY trading_date, score_key"
            ).fetchall()
            if raw_scores and "eod_close_observations" not in tables:
                raise RuntimeError(
                    "promoted prediction scores exist without verified close evidence"
                )
            latest: dict[str, sqlite3.Row] = {}
            for row in raw_scores:
                close = connection.execute(
                    "SELECT * FROM eod_close_observations WHERE id=?",
                    (int(row["close_observation_id"]),),
                ).fetchone()
                if close is None:
                    raise RuntimeError(
                        "promoted prediction score references a missing verified close observation"
                    )
                _reproduce_score_row(connection, row, governed_by_key)
                _require_retained_close_artifact(root, close)
                key = str(row["prediction_key"])
                prior = latest.get(key)
                if prior is None or int(row["close_observation_id"]) > int(prior["close_observation_id"]):
                    latest[key] = row
            for row in latest.values():
                governed = governed_by_key.get(str(row["prediction_key"]))
                if governed is None:
                    continue
                authoritative_close = _latest_verified_close_for_prediction(
                    connection,
                    governed,
                )
                if (
                    authoritative_close is None
                    or int(row["close_observation_id"])
                    != int(authoritative_close["id"])
                ):
                    continue
                _require_retained_close_artifact(root, authoritative_close)
                if model_version is not None and str(row["model_version"]) != model_version:
                    continue
                if family is not None and str(row["family_root"]) != family:
                    continue
                if decision_horizon_minutes is not None and int(row["decision_horizon_minutes"]) != decision_horizon_minutes:
                    continue
                if regime is not None and str(governed["volatility_regime"]) != regime:
                    continue
                payload = dict(row)
                payload["volatility_regime"] = str(governed["volatility_regime"])
                score_rows.append(payload)
    filtered_governed = [
        row
        for row in governed_rows
        if (model_version is None or str(row["model_version"]) == model_version)
        and (family is None or str(row["family_root"]) == family)
        and (
            decision_horizon_minutes is None
            or int(row["decision_horizon_minutes"]) == decision_horizon_minutes
        )
        and (regime is None or str(row["volatility_regime"]) == regime)
    ]
    try:
        attempts = _attempt_summary(
            event_rows,
            model_version=model_version,
            decision_horizon_minutes=decision_horizon_minutes,
            governed_prediction_keys=set(governed_by_key),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"closing-tape governance evidence is invalid: {exc}"
        ) from exc
    overall = _metric_summary(score_rows, minimum_scored_sessions=minimum_scored_sessions)
    roots = [family] if family else sorted(PRODUCTION_FAMILIES)
    by_family = [
        {
            "family_root": root,
            "symbol": root,
            **_metric_summary(
                [row for row in score_rows if str(row["family_root"]) == root],
                minimum_scored_sessions=minimum_scored_sessions,
            ),
        }
        for root in roots
    ]

    horizons = sorted(
        {
            int(row["decision_horizon_minutes"])
            for row in filtered_governed
        }
        | (
            {decision_horizon_minutes}
            if decision_horizon_minutes is not None else set()
        )
    )
    by_horizon = [
        {
            "decision_horizon_minutes": horizon,
            **_metric_summary(
                [
                    row for row in score_rows
                    if int(row["decision_horizon_minutes"]) == horizon
                ],
                minimum_scored_sessions=minimum_scored_sessions,
            ),
        }
        for horizon in horizons
    ]
    regimes = [regime] if regime else list(VOLATILITY_REGIMES)
    by_regime = [
        {
            "volatility_regime": value,
            **_metric_summary(
                [
                    row for row in score_rows
                    if str(row["volatility_regime"]) == value
                ],
                minimum_scored_sessions=minimum_scored_sessions,
            ),
        }
        for value in regimes
    ]
    versions = sorted(
        {str(row["model_version"]) for row in filtered_governed}
        | ({model_version} if model_version else set())
    )
    by_model = [
        {
            "model_version": version,
            **_metric_summary(
                [row for row in score_rows if str(row["model_version"]) == version],
                minimum_scored_sessions=minimum_scored_sessions,
            ),
        }
        for version in versions
    ]
    slice_identities = sorted(
        {
            (
                str(row["family_root"]),
                int(row["decision_horizon_minutes"]),
                str(row["volatility_regime"]),
                str(row["model_version"]),
                str(row["artifact_sha256"]),
            )
            for row in filtered_governed
        }
    )
    by_slice = []
    for symbol, horizon, slice_regime, version, artifact in slice_identities:
        rows = [
            row
            for row in score_rows
            if str(row["family_root"]) == symbol
            and int(row["decision_horizon_minutes"]) == horizon
            and str(row["volatility_regime"]) == slice_regime
            and str(row["model_version"]) == version
            and str(row["artifact_sha256"]) == artifact
        ]
        by_slice.append(
            {
                "symbol": symbol,
                "family_root": symbol,
                "decision_horizon_minutes": horizon,
                "volatility_regime": slice_regime,
                "model_version": version,
                "artifact_sha256": artifact,
                **_metric_summary(
                    rows,
                    minimum_scored_sessions=minimum_scored_sessions,
                ),
            }
        )

    resolved_keys = {str(row["prediction_key"]) for row in score_rows}
    eligible_keys = {str(row["prediction_key"]) for row in filtered_governed}
    resolved = len(eligible_keys & resolved_keys)
    outcome_resolution = {
        "eligible_forecasts": len(eligible_keys),
        "resolved_forecasts": resolved,
        "unresolved_forecasts": len(eligible_keys) - resolved,
        "resolution_rate": resolved / len(eligible_keys) if eligible_keys else None,
    }
    slice_metrics_available = any(
        bool(item["metrics_available"]) for item in by_slice
    )
    reason = None if (overall["metrics_available"] or slice_metrics_available) else str(overall["reason"])
    if not event_rows and not governed_rows:
        reason = "closing-tape governance evidence is not initialized"
        overall = _empty_metric(reason)
    elif not filtered_governed:
        reason = "no governed promoted forecasts match the requested filters"
        overall = _empty_metric(reason)
    elif not score_rows:
        reason = "verified outcomes have not resolved for the selected forecasts"
        overall = _empty_metric(reason)
    return {
        **base,
        "available": bool(overall["metrics_available"] or slice_metrics_available),
        "reason": reason,
        "attempts": attempts,
        "overall": overall,
        "by_family": by_family,
        "by_symbol": by_family,
        "by_horizon": by_horizon,
        "by_regime": by_regime,
        "by_model": by_model,
        "by_slice": by_slice,
        "outcome_resolution": outcome_resolution,
    }
