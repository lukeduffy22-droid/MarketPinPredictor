"""Append-only research observations and official-close comparison receipts."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

from backend.forecast_calendar import session_bounds
from backend.structural_regime_live import parse_utc

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "structural_regime.db"


def _encode(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class StructuralRegimeJournal:
    def __init__(self, path: str | Path = DEFAULT_PATH):
        self.path = Path(path)

    def _connect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=0.25)
        connection.row_factory = sqlite3.Row
        connection.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                prediction_id TEXT PRIMARY KEY, symbol TEXT NOT NULL,
                trading_date TEXT NOT NULL, model_version TEXT NOT NULL,
                as_of_utc TEXT NOT NULL, recorded_at_utc TEXT NOT NULL,
                prediction_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS final_observation ON observations
                (symbol, trading_date, model_version, as_of_utc);
            CREATE TABLE IF NOT EXISTS outcomes (
                outcome_id TEXT PRIMARY KEY, prediction_id TEXT NOT NULL,
                recorded_at_utc TEXT NOT NULL, outcome_json TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS observations_no_update BEFORE UPDATE ON observations
                BEGIN SELECT RAISE(ABORT, 'append_only'); END;
            CREATE TRIGGER IF NOT EXISTS observations_no_delete BEFORE DELETE ON observations
                BEGIN SELECT RAISE(ABORT, 'append_only'); END;
            CREATE TRIGGER IF NOT EXISTS outcomes_no_update BEFORE UPDATE ON outcomes
                BEGIN SELECT RAISE(ABORT, 'append_only'); END;
            CREATE TRIGGER IF NOT EXISTS outcomes_no_delete BEFORE DELETE ON outcomes
                BEGIN SELECT RAISE(ABORT, 'append_only'); END;
        """)
        return connection

    def record(self, prediction: dict, *, now_utc: datetime | None = None) -> bool:
        if prediction.get("status") != "AVAILABLE":
            return False
        now = now_utc or datetime.now(timezone.utc)
        observed, close = parse_utc(prediction.get("as_of_utc")), parse_utc(prediction.get("target_close_utc"))
        if observed is None or close is None or not observed <= now < close:
            raise ValueError("research_observation_must_be_recorded_before_close")
        body = {key: value for key, value in prediction.items() if key not in {"prediction_id", "journal_status"}}
        if hashlib.sha256(_encode(body).encode()).hexdigest() != prediction.get("prediction_id"):
            raise ValueError("structural_prediction_hash_mismatch")
        connection = self._connect()
        try:
            with connection:
                result = connection.execute(
                    "INSERT OR IGNORE INTO observations VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (prediction["prediction_id"], prediction["symbol"], prediction["trading_date"],
                     prediction["model_version"], observed.isoformat(), now.isoformat(),
                     _encode({**prediction, "journal_status": "RECORDED"})),
                )
                return result.rowcount == 1
        finally:
            connection.close()

    def record_verified_close(self, row, *, now_utc: datetime | None = None) -> dict:
        """Called only after the official importer validates and commits its bundle.

        ``row`` is its VerifiedCloseInput, including retained artifact bytes.
        Corrections append another receipt; predictions and prior outcomes stay intact.
        """
        if not self.path.exists():
            return {"recorded": 0, "reason": "no_structural_observations"}
        from backend.closing_tape.close_evidence import (
            validate_official_close_reference, validate_source_artifact_sha256,
            validate_verified_close_observed_at,
        )
        from backend.structural_regime import finite_number

        now = now_utc or datetime.now(timezone.utc)
        validate_official_close_reference(row.symbol, row.source, row.source_reference)
        digest = validate_source_artifact_sha256(row.source_artifact_sha256)
        if hashlib.sha256(Path(row.source_artifact_path).read_bytes()).hexdigest() != digest:
            raise ValueError("verified_close_artifact_hash_mismatch")
        observed_close = validate_verified_close_observed_at(row.trading_date, row.observed_at_utc, now_utc=now)
        actual = finite_number(row.official_close, "official_close", positive=True)
        _, closed = session_bounds(row.trading_date)
        if now < closed:
            raise ValueError("session_not_complete")
        connection = self._connect()
        try:
            # Choose the final prospectively recorded observation for each version.
            rows = connection.execute("""
                SELECT * FROM observations WHERE symbol=? AND trading_date=?
                ORDER BY as_of_utc DESC, recorded_at_utc DESC, prediction_id DESC
            """, (row.symbol, row.trading_date.isoformat())).fetchall()
            versions, count = set(), 0
            with connection:
                for observation in rows:
                    version = observation["model_version"]
                    if version in versions:
                        continue
                    prediction = json.loads(observation["prediction_json"])
                    source_time = parse_utc(observation["as_of_utc"])
                    recorded_time = parse_utc(observation["recorded_at_utc"])
                    if source_time is None or recorded_time is None or not source_time <= recorded_time < closed:
                        continue
                    versions.add(version)
                    receipt = {
                        "prediction_id": observation["prediction_id"], "prediction": prediction,
                        "actual_official_close": actual, "source": row.source,
                        "source_reference": row.source_reference, "source_artifact_sha256": digest,
                        "close_observed_at_utc": observed_close.isoformat(),
                        "correction_of_id": row.correction_of_id,
                        "error_points": prediction["predicted_close"] - actual,
                        "absolute_error_points": abs(prediction["predicted_close"] - actual),
                        "actual_inside_structural_band": prediction["band_low"] <= actual <= prediction["band_high"],
                        "research_only": True, "promotion_eligible": False,
                    }
                    encoded = _encode(receipt)
                    outcome_id = hashlib.sha256(encoded.encode()).hexdigest()
                    count += connection.execute(
                        "INSERT OR IGNORE INTO outcomes VALUES (?, ?, ?, ?)",
                        (outcome_id, observation["prediction_id"], now.isoformat(), encoded),
                    ).rowcount
            return {"recorded": count, "reason": "verified_close_comparison" if rows else "no_structural_observations"}
        finally:
            connection.close()
