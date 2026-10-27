"""Deterministic, receipt-backed selection for post-close diagnostic scoring."""

from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from backend.closing_tape.config import (
    _market_times,
    official_close_publication_cutoff_utc,
)
from backend.database_target import configured_market_database_path


UTC = timezone.utc
SCHEMA_VERSION = "marketpin-prediction-score-selection.v1"
DEFAULT_SYMBOLS = ("SPX", "NDX", "RUT", "DJI")
VERIFIED_CLOSE_SCORE_SYMBOLS = frozenset({"SPX", "NDX", "RUT"})


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _parse_utc(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _selection_directory(project_root: Path, trading_day: date) -> Path:
    return (
        project_root
        / "data"
        / "prediction_score_selections"
        / trading_day.isoformat()
    )


def _validated_existing_receipt(directory: Path) -> tuple[dict[str, Any], Path, str] | None:
    paths = sorted(directory.glob("*.json")) if directory.is_dir() else []
    if not paths:
        return None
    if len(paths) != 1:
        raise RuntimeError("multiple immutable prediction-selection receipts exist")
    path = paths[0]
    raw = path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if path.stem != digest:
        raise RuntimeError("prediction-selection receipt filename does not match its SHA-256")
    payload = json.loads(raw.decode("utf-8"))
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("prediction-selection receipt schema is unsupported")
    return payload, path, digest


def _eligible_snapshot(
    connection: sqlite3.Connection,
    *,
    symbol: str,
    trading_day: date,
    cash_close_utc: datetime,
) -> dict[str, Any] | None:
    rows = connection.execute(
        """
        SELECT id, symbol, timestamp_utc, current_price, predicted_close,
               prediction_mode, model_version, source_payload_json
        FROM prediction_snapshots
        WHERE symbol=? AND trading_date=? AND is_valid=1
              AND predicted_close IS NOT NULL AND current_price IS NOT NULL
        ORDER BY timestamp_utc DESC, id DESC
        """,
        (symbol, trading_day.isoformat()),
    )
    for row in rows:
        timestamp = _parse_utc(row["timestamp_utc"])
        if timestamp > cash_close_utc:
            continue
        try:
            current_price = float(row["current_price"])
            predicted_close = float(row["predicted_close"])
            source_payload = json.loads(row["source_payload_json"] or "{}")
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if not (
            math.isfinite(current_price)
            and current_price > 0
            and math.isfinite(predicted_close)
            and predicted_close > 0
            and isinstance(source_payload, dict)
        ):
            continue
        calculation_id = str(source_payload.get("calculation_id") or "").strip()
        if not calculation_id:
            continue
        lineage = connection.execute(
            """
            SELECT r.calculation_id
            FROM gamma_calculation_runs AS r
            JOIN gamma_calculation_input_blobs AS b
              ON b.calculation_run_id=r.id
            WHERE r.calculation_id=? AND r.symbol=? AND r.trading_date=?
                  AND r.status='valid'
            LIMIT 1
            """,
            (calculation_id, symbol, trading_day.isoformat()),
        ).fetchone()
        if lineage is None:
            continue
        return {
            "prediction_id": int(row["id"]),
            "symbol": symbol,
            "prediction_timestamp_utc": timestamp.isoformat(),
            "current_price": current_price,
            "predicted_close": predicted_close,
            "prediction_mode": str(row["prediction_mode"] or ""),
            "model_version": str(row["model_version"] or ""),
            "calculation_id": calculation_id,
        }
    return None


def prepare_prediction_score_selection(
    project_root: str | Path,
    trading_day: date,
    *,
    observed_at_utc: datetime | None = None,
    symbols: Iterable[str] = DEFAULT_SYMBOLS,
) -> dict[str, Any]:
    """Freeze the last lineage-valid pre-close forecast before close publication."""
    root = Path(project_root).resolve()
    database_path = configured_market_database_path(root, require_canonical=True)
    observed = (observed_at_utc or datetime.now(UTC)).astimezone(UTC)
    _cash_open, _analysis_due, cash_close, _stop = _market_times(trading_day)
    publication_cutoff = official_close_publication_cutoff_utc(trading_day)
    if observed < cash_close:
        raise RuntimeError("prediction selection is blocked until the cash session closes")

    directory = _selection_directory(root, trading_day)
    existing = _validated_existing_receipt(directory)
    if existing is not None:
        payload, path, digest = existing
        if payload.get("trading_date") != trading_day.isoformat():
            raise RuntimeError("prediction-selection receipt trading date mismatch")
        try:
            selected_at = _parse_utc(payload["selected_at_utc"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "prediction-selection receipt has an invalid selection timestamp"
            ) from exc
        if selected_at < cash_close or selected_at >= publication_cutoff:
            raise RuntimeError(
                "prediction-selection receipt was not created within the approved window"
            )
        return {"receipt": payload, "receipt_path": str(path), "receipt_sha256": digest}

    if observed >= publication_cutoff:
        raise RuntimeError(
            "prediction selection is blocked at or after the official publication cutoff"
        )

    normalized_symbols = tuple(
        dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip())
    )
    if not normalized_symbols:
        raise ValueError("at least one selection symbol is required")
    uri = f"file:{database_path.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        required = {
            "prediction_snapshots",
            "gamma_calculation_runs",
            "gamma_calculation_input_blobs",
        }
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        missing = sorted(required - tables)
        if missing:
            raise RuntimeError("selection database schema is incomplete: " + ",".join(missing))
        selections: list[dict[str, Any]] = []
        statuses: list[dict[str, str]] = []
        for symbol in normalized_symbols:
            if symbol not in VERIFIED_CLOSE_SCORE_SYMBOLS:
                statuses.append(
                    {
                        "symbol": symbol,
                        "status": "UNSUPPORTED_VERIFIED_CLOSE_SCORING",
                        "reason": (
                            "DJI has no approved official-close adapter"
                            if symbol == "DJI"
                            else "symbol is outside the diagnostic close-scoring contract"
                        ),
                    }
                )
                continue
            selected = _eligible_snapshot(
                connection,
                symbol=symbol,
                trading_day=trading_day,
                cash_close_utc=cash_close,
            )
            if selected is None:
                statuses.append(
                    {
                        "symbol": symbol,
                        "status": "NO_ELIGIBLE_PREDICTION",
                        "reason": "no valid pre-close prediction with retained calculation lineage",
                    }
                )
            else:
                selections.append(selected)
                statuses.append({"symbol": symbol, "status": "SELECTED", "reason": ""})
    finally:
        connection.close()

    receipt = {
        "schema_version": SCHEMA_VERSION,
        "trading_date": trading_day.isoformat(),
        "policy": "latest_lineage_valid_prediction_at_or_before_cash_close.v1",
        "cash_close_utc": cash_close.isoformat(),
        "selected_at_utc": observed.isoformat(),
        "database_path": str(database_path),
        "symbols_requested": list(normalized_symbols),
        "selections": selections,
        "statuses": statuses,
        "research_only": True,
        "training_eligible": False,
        "performance_claim_eligible": False,
    }
    rendered = _canonical_bytes(receipt)
    digest = hashlib.sha256(rendered).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{digest}.json"
    temporary = directory / f".{digest}.{uuid.uuid4().hex}.tmp"
    try:
        with temporary.open("xb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()
    return {"receipt": receipt, "receipt_path": str(path), "receipt_sha256": digest}


def load_prediction_score_selection(
    project_root: str | Path, trading_day: date
) -> dict[str, Any]:
    root = Path(project_root).resolve()
    existing = _validated_existing_receipt(_selection_directory(root, trading_day))
    if existing is None:
        raise FileNotFoundError("prediction-selection receipt is missing")
    payload, path, digest = existing
    if payload.get("trading_date") != trading_day.isoformat():
        raise RuntimeError("prediction-selection receipt trading date mismatch")
    return {"receipt": payload, "receipt_path": str(path), "receipt_sha256": digest}
