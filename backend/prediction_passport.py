"""Immutable forecast passports and replay/outcome projections.

The passport is the decision-grade boundary.  Legacy prediction snapshots remain
available for compatibility, but a snapshot is not silently upgraded into a
passport when required point-in-time evidence is absent.
"""
from __future__ import annotations

import hashlib
import json
import math
import zlib
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.exc import IntegrityError, OperationalError

import backend.database as database
from backend.ai_predictor import (
    MODEL_VERSION,
    model_artifact_sha256,
    replay_ai_prediction,
)
from backend.closing_tape.config import _market_times
from backend.config import LIVE_DATA_STALE_AFTER_SECONDS
from backend.workstation import payload_has_fallback_provenance


PASSPORT_SCHEMA_VERSION = "prediction-passport-v1"
PASSPORT_STATES = {"valid", "research", "abstain", "stale", "unavailable"}
MAX_PREDICTION_CLOCK_SKEW_SECONDS = 5.0
PASSPORT_SQLITE_WRITE_TIMEOUT_MS = 250
PASSPORT_SQLITE_WRITE_ATTEMPTS = 2
_HEX_DIGITS = frozenset("0123456789abcdef")
_SNAPSHOT_ISSUER_TOKEN = object()
FORMULA_VERSION_FIELDS = (
    "gex_formula_version",
    "target_formula_version",
    "spot_formula_version",
    "max_pain_formula_version",
    "pin_competition_formula_version",
    "zero_gamma_semantics_version",
)


class PassportConflictError(ValueError):
    """Raised when an immutable forecast identity is retried with new content."""


class PassportIntegrityError(ValueError):
    """Raised when stored canonical passport content no longer matches its hash."""


class PassportPublicationRejected(RuntimeError):
    """Raised when runtime identity changes before an immutable commit."""


def _require_publication_allowed(
    publication_guard: Callable[[], bool] | None,
) -> None:
    if publication_guard is None:
        return
    try:
        allowed = publication_guard() is True
    except Exception as exc:
        raise PassportPublicationRejected("publication guard failed") from exc
    if not allowed:
        raise PassportPublicationRejected("publication guard rejected persistence")


def _sqlite_busy(exc: OperationalError) -> bool:
    """Only lock contention is retryable; malformed SQL/I/O errors are not."""
    code = getattr(exc.orig, "sqlite_errorcode", None)
    return bool(
        (isinstance(code, int) and (code & 0xFF) in {5, 6})
        or str(exc.orig).lower() in {"database is locked", "database table is locked"}
    )


def _persist_passport_bounded(
    values: Mapping[str, Any],
    *,
    publication_guard: Callable[[], bool] | None,
) -> dict[str, Any]:
    """Publish immutable content in a short, bounded SQLite write transaction.

    Retained evidence is assembled before entering this function. Acquire the
    SQLite writer reservation before reading the identity to avoid upgrading a
    stale read snapshot. Each attempt owns its connection until its original
    busy timeout is restored; connection-local settings never leak to the pool.
    This bounds SQLite lock waiting, not disk I/O, model work, or total latency.
    """
    for attempt in range(PASSPORT_SQLITE_WRITE_ATTEMPTS):
        _require_publication_allowed(publication_guard)
        with database.engine.connect() as connection:
            sqlite = connection.dialect.name == "sqlite"
            raw = connection.connection.driver_connection if sqlite else None
            original_timeout = None
            try:
                if raw is not None:
                    cursor = raw.cursor()
                    try:
                        original_timeout = int(cursor.execute("PRAGMA busy_timeout").fetchone()[0])
                        cursor.execute(f"PRAGMA busy_timeout={PASSPORT_SQLITE_WRITE_TIMEOUT_MS}")
                    finally:
                        cursor.close()
                with database.SessionLocal(bind=connection) as write_session:
                    if sqlite:
                        write_session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                    _require_publication_allowed(publication_guard)
                    existing = write_session.query(database.PredictionPassport).filter(
                        database.PredictionPassport.origin_kind == values["origin_kind"],
                        database.PredictionPassport.origin_key == values["origin_key"],
                    ).first()
                    if existing is not None:
                        if existing.record_sha256 != values["record_sha256"]:
                            raise PassportConflictError("immutable passport origin has conflicting content")
                        _require_publication_allowed(publication_guard)
                        return read_prediction_passport(existing.forecast_id, session=write_session)
                    write_session.add(database.PredictionPassport(**dict(values)))
                    write_session.flush()
                    _require_publication_allowed(publication_guard)
                    write_session.commit()
                    return read_prediction_passport(str(values["forecast_id"]), session=write_session)
            except OperationalError as exc:
                connection.rollback()
                if not sqlite or not _sqlite_busy(exc) or attempt + 1 >= PASSPORT_SQLITE_WRITE_ATTEMPTS:
                    raise
            finally:
                if raw is not None and original_timeout is not None:
                    cursor = raw.cursor()
                    try:
                        cursor.execute(f"PRAGMA busy_timeout={original_timeout}")
                    except Exception:
                        # A broken connection must not return to the pool with
                        # the temporary timeout or an unresolved transaction.
                        connection.invalidate()
                        raise
                    finally:
                        cursor.close()
    raise RuntimeError("passport write retry budget exhausted")


def _issuance_now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("passport payload contains a non-finite number")
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        return _to_json_value(value.item())
    if isinstance(value, Mapping):
        return {str(key): _to_json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_json_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_to_json_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ),
        )
    raise TypeError(f"unsupported passport JSON value: {type(value).__name__}")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        _to_json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_hash(value: Mapping[str, Any]) -> str:
    return _sha256_text(_canonical_json(value))


def _sha256_evidence(value: Any) -> tuple[str | None, bool]:
    """Return normalized SHA-256 evidence and whether supplied evidence was invalid."""
    if value is None:
        return None, False
    normalized = str(value).strip().lower()
    if not normalized:
        return None, False
    if len(normalized) != 64 or any(character not in _HEX_DIGITS for character in normalized):
        return None, True
    return normalized, False


def _subscription_epoch_evidence(value: Any) -> tuple[str | None, bool]:
    """Validate the canonical process epoch without normalizing its identity."""

    if value is None:
        return None, False
    candidate = str(value).strip()
    if not candidate:
        return None, False
    if len(candidate) != 64 or any(character not in _HEX_DIGITS for character in candidate):
        return None, True
    return candidate, False


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        numeric = _finite(value)
        if numeric is None:
            return None
        try:
            parsed = datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _nonnegative_int(value: Any) -> tuple[int | None, bool]:
    """Return a normalized non-negative integer and whether evidence was malformed."""

    if value is None:
        return None, False
    if isinstance(value, bool):
        return None, True
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None, True
    try:
        if float(value) != float(number) or number < 0:
            return None, True
    except (TypeError, ValueError, OverflowError):
        return None, True
    return number, False


def _target_close(trading_day: date) -> datetime:
    _open, _analysis, close, _stop = _market_times(trading_day)
    return close.astimezone(timezone.utc)


def _state_reasons(prediction: Mapping[str, Any], source: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for candidate in (
        prediction.get("abstention_reasons"),
        prediction.get("validation_failure_reasons"),
        source.get("validation_failure_reasons"),
    ):
        if isinstance(candidate, str) and candidate.strip():
            values.append(candidate.strip())
        elif isinstance(candidate, Iterable) and not isinstance(candidate, Mapping):
            values.extend(str(item).strip() for item in candidate if str(item).strip())
    reason = prediction.get("reason") or source.get("pregate_reason")
    if reason:
        values.append(str(reason).strip())
    return list(dict.fromkeys(item for item in values if item))


def _gamma_lineage(session, calculation_id: str | None) -> dict[str, Any]:
    """Load and cryptographically verify the persisted gamma input lineage."""

    evidence: dict[str, Any] = {
        "run": None,
        "universe_sha256": None,
        "calculation_input_sha256": None,
        "input_payload": None,
        "errors": [],
    }
    if not calculation_id:
        return evidence
    run = session.query(database.GammaCalculationRun).filter(
        database.GammaCalculationRun.calculation_id == calculation_id,
    ).first()
    if run is None:
        evidence["errors"].append("CALCULATION_RUN_NOT_FOUND")
        return evidence
    evidence["run"] = run
    evidence["universe_sha256"] = run.universe_sha256
    blob = session.query(database.GammaCalculationInputBlob).filter(
        database.GammaCalculationInputBlob.calculation_run_id == run.id,
    ).first()
    if blob is None:
        evidence["errors"].append("CALCULATION_INPUT_BLOB_NOT_FOUND")
        return evidence
    evidence["calculation_input_sha256"] = blob.payload_sha256
    try:
        if blob.encoding != "canonical-json+zlib-v1":
            raise ValueError("unsupported input encoding")
        if int(blob.compressed_bytes) != len(blob.payload):
            raise ValueError("compressed input size mismatch")
        canonical = zlib.decompress(blob.payload)
        if int(blob.uncompressed_bytes) != len(canonical):
            raise ValueError("uncompressed input size mismatch")
        if hashlib.sha256(canonical).hexdigest() != str(blob.payload_sha256).lower():
            raise ValueError("input payload hash mismatch")
        decoded = json.loads(canonical.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise ValueError("input payload is not an object")
        evidence["input_payload"] = decoded
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError, zlib.error):
        evidence["errors"].append("CALCULATION_INPUT_BLOB_INVALID")
    return evidence


def _same_number(left: Any, right: Any) -> bool:
    first = _finite(left)
    second = _finite(right)
    return bool(
        first is not None
        and second is not None
        and math.isclose(first, second, rel_tol=1e-10, abs_tol=1e-9)
    )


def _lineage_validation_errors(
    lineage: Mapping[str, Any],
    *,
    symbol: str,
    trading_day: date,
    provider: str | None,
    subscription_epoch_id: str | None,
    generation: int | None,
    predicted_at: datetime,
    source: Mapping[str, Any],
    reference_price: float | None,
) -> list[str]:
    errors = [str(item) for item in lineage.get("errors") or []]
    run = lineage.get("run")
    if run is None:
        return errors
    if str(run.status or "").lower() != "valid":
        errors.append("CALCULATION_RUN_NOT_VALID")
    if str(run.symbol or "").upper() != symbol:
        errors.append("CALCULATION_SYMBOL_MISMATCH")
    if run.trading_date != trading_day:
        errors.append("CALCULATION_TRADING_DATE_MISMATCH")
    if provider and str(run.provider or "").lower() != provider.lower():
        errors.append("CALCULATION_PROVIDER_MISMATCH")
    if (
        subscription_epoch_id is not None
        and run.subscription_epoch_id != subscription_epoch_id
    ):
        errors.append("CALCULATION_SUBSCRIPTION_EPOCH_MISMATCH")
    if generation is not None and run.subscription_generation != generation:
        errors.append("CALCULATION_GENERATION_MISMATCH")
    calculated_at = _timestamp(run.calculated_at_utc)
    if calculated_at is None:
        errors.append("CALCULATION_TIMESTAMP_MISSING")
    elif calculated_at > predicted_at:
        errors.append("CALCULATION_AFTER_PREDICTION")
    if int(run.chain_row_count or 0) <= 0 or int(run.gex_row_count or 0) <= 0:
        errors.append("CALCULATION_CHAIN_EVIDENCE_EMPTY")
    if reference_price is not None and not _same_number(run.spot_price, reference_price):
        errors.append("CALCULATION_REFERENCE_PRICE_MISMATCH")
    comparisons = (
        ("gamma_pin", run.gamma_pin),
        ("zero_gamma", run.zero_gamma),
        ("max_pain", run.max_pain),
        ("gross_gex", run.gross_gex),
        ("net_gex", run.net_gex),
    )
    for field, persisted in comparisons:
        supplied = source.get(field)
        if supplied is not None and persisted is not None and not _same_number(supplied, persisted):
            errors.append(f"CALCULATION_{field.upper()}_MISMATCH")
    formula = source.get("gex_formula_version") or source.get("formula_version")
    if formula and str(run.formula_version or "") != str(formula):
        errors.append("CALCULATION_FORMULA_VERSION_MISMATCH")
    return list(dict.fromkeys(errors))


def _calibration_validation_errors(
    evidence: Mapping[str, Any] | None,
    *,
    symbol: str,
    model_version: str | None,
    horizon_seconds: int,
    regime: str,
    predicted_at: datetime,
    interval_target_coverage: float | None,
) -> list[str]:
    if not evidence:
        return ["CALIBRATION_EVIDENCE_NOT_RESOLVED"]
    errors: list[str] = []
    if str(evidence.get("symbol") or "").upper() != symbol:
        errors.append("CALIBRATION_SYMBOL_MISMATCH")
    if str(evidence.get("model_version") or "") != str(model_version or ""):
        errors.append("CALIBRATION_MODEL_VERSION_MISMATCH")
    try:
        if int(evidence.get("horizon_seconds")) != horizon_seconds:
            errors.append("CALIBRATION_HORIZON_MISMATCH")
    except (TypeError, ValueError):
        errors.append("CALIBRATION_HORIZON_MISSING")
    if str(evidence.get("regime") or "") != regime:
        errors.append("CALIBRATION_REGIME_MISMATCH")
    asof = _timestamp(evidence.get("asof_utc"))
    if asof is None or asof > predicted_at:
        errors.append("CALIBRATION_ASOF_INVALID")
    sample_size = _finite(evidence.get("sample_size"))
    candidate_mae = _finite(evidence.get("oos_mae"))
    baseline_mae = _finite(evidence.get("persistence_mae"))
    coverage = _finite(evidence.get("interval_empirical_coverage"))
    improvement_ci_low = _finite(evidence.get("improvement_ci_low_pct"))
    if sample_size is None or sample_size < 60 or not float(sample_size).is_integer():
        errors.append("CALIBRATION_SAMPLE_SIZE_INSUFFICIENT")
    if candidate_mae is None or candidate_mae < 0:
        errors.append("CALIBRATION_OOS_MAE_INVALID")
    if baseline_mae is None or baseline_mae <= 0:
        errors.append("CALIBRATION_BASELINE_MAE_INVALID")
    if improvement_ci_low is None or improvement_ci_low <= 0:
        errors.append("CALIBRATION_IMPROVEMENT_NOT_CONFIDENTLY_POSITIVE")
    if coverage is None or not 0 <= coverage <= 1:
        errors.append("CALIBRATION_COVERAGE_INVALID")
    elif (
        interval_target_coverage is not None
        and abs(coverage - interval_target_coverage) > 0.10
    ):
        errors.append("CALIBRATION_COVERAGE_OUTSIDE_TOLERANCE")
    return list(dict.fromkeys(errors))


def _snapshot_prediction(snapshot: database.PredictionSnapshot) -> dict[str, Any]:
    def decoded(value: str | None, default: Any) -> Any:
        try:
            result = json.loads(value or "")
        except (json.JSONDecodeError, TypeError):
            return default
        return result

    source = decoded(snapshot.source_payload_json, {})
    return {
        "symbol": snapshot.symbol,
        "timestamp": snapshot.timestamp_utc,
        "provider": snapshot.provider,
        "model_version": snapshot.model_version,
        "model_type": snapshot.model_type,
        "usable": snapshot.is_valid,
        "validation_status": snapshot.validation_status,
        "current_price": snapshot.current_price,
        "predicted_close": snapshot.predicted_close,
        "confidence": snapshot.confidence,
        "feature_snapshot": decoded(snapshot.feature_snapshot_json, {}),
        "feature_schema_version": snapshot.feature_schema_version,
        "feature_hash": snapshot.feature_hash,
        "signals": decoded(snapshot.signals_json, []),
        "pin_payload": source,
        "quote_timestamp_utc": snapshot.quote_timestamp_utc,
        "subscription_epoch_id": snapshot.subscription_epoch_id,
        "subscription_generation": snapshot.subscription_generation,
        "data_age_seconds": snapshot.data_age_seconds,
        "quote_age_seconds": snapshot.quote_age_seconds,
        "active_contract_count": snapshot.active_contract_count,
        "fresh_quote_count": snapshot.fresh_quote_count,
        "inference_device": snapshot.inference_device,
    }


def _validate_snapshot_binding(
    snapshot_payload: Mapping[str, Any],
    supplied_prediction: Mapping[str, Any],
) -> None:
    """Reject attaching a caller-supplied forecast to a different snapshot."""

    def mismatch(field: str) -> None:
        raise PassportConflictError(
            f"prediction_snapshot_id conflicts with supplied {field}"
        )

    for field in (
        "provider",
        "model_version",
        "model_type",
        "feature_schema_version",
        "feature_hash",
    ):
        supplied = supplied_prediction.get(field)
        stored = snapshot_payload.get(field)
        if supplied is not None and stored is not None and str(supplied) != str(stored):
            mismatch(field)

    supplied_symbol = supplied_prediction.get("symbol")
    stored_symbol = snapshot_payload.get("symbol")
    if (
        supplied_symbol is not None
        and str(supplied_symbol).strip().upper() != str(stored_symbol).strip().upper()
    ):
        mismatch("symbol")

    for field in ("timestamp", "quote_timestamp_utc"):
        supplied = supplied_prediction.get(field)
        stored = snapshot_payload.get(field)
        if supplied is not None and stored is not None and _timestamp(supplied) != _timestamp(stored):
            mismatch(field)

    supplied_generation = supplied_prediction.get("subscription_generation")
    stored_generation = snapshot_payload.get("subscription_generation")
    if supplied_generation is not None and stored_generation is not None:
        try:
            generation_matches = int(supplied_generation) == int(stored_generation)
        except (TypeError, ValueError):
            generation_matches = False
        if not generation_matches:
            mismatch("subscription_generation")

    supplied_epoch_id = supplied_prediction.get("subscription_epoch_id")
    stored_epoch_id = snapshot_payload.get("subscription_epoch_id")
    if supplied_epoch_id is not None and stored_epoch_id is not None:
        if str(supplied_epoch_id).strip() != str(stored_epoch_id).strip():
            mismatch("subscription_epoch_id")

    for field in ("current_price", "predicted_close"):
        supplied = supplied_prediction.get(field)
        stored = snapshot_payload.get(field)
        if supplied is None or stored is None:
            continue
        supplied_number = _finite(supplied)
        stored_number = _finite(stored)
        if (
            supplied_number is None
            or stored_number is None
            or not math.isclose(supplied_number, stored_number, rel_tol=1e-12, abs_tol=1e-12)
        ):
            mismatch(field)

    supplied_features = supplied_prediction.get("feature_snapshot")
    stored_features = snapshot_payload.get("feature_snapshot")
    if isinstance(supplied_features, Mapping) and isinstance(stored_features, Mapping):
        if _canonical_json(supplied_features) != _canonical_json(stored_features):
            mismatch("feature_snapshot")

    supplied_source = supplied_prediction.get("pin_payload")
    stored_source = snapshot_payload.get("pin_payload")
    if isinstance(supplied_source, Mapping) and isinstance(stored_source, Mapping):
        supplied_calculation_id = supplied_source.get("calculation_id")
        stored_calculation_id = stored_source.get("calculation_id")
        if (
            supplied_calculation_id is not None
            and stored_calculation_id is not None
            and str(supplied_calculation_id) != str(stored_calculation_id)
        ):
            mismatch("pin_payload.calculation_id")


def issue_prediction_passport(
    prediction: Mapping[str, Any],
    *,
    prediction_mode: str,
    prediction_snapshot_id: int | None = None,
    origin_kind: str = "live_prediction",
    origin_key: str | None = None,
    requested_state: str | None = None,
    decision_grade: bool = False,
    target_timestamp_utc: datetime | str | None = None,
    publication_guard: Callable[[], bool] | None = None,
    _snapshot_issuer_token: object | None = None,
) -> dict[str, Any]:
    """Issue one immutable passport, degrading incomplete claims explicitly.

    Any ``valid`` request with incomplete provenance becomes ``abstain``.
    A non-production heuristic with otherwise usable evidence becomes
    ``research``.  Neither path invents missing interval or artifact evidence.
    """
    if prediction_snapshot_id is not None and _snapshot_issuer_token is not _SNAPSHOT_ISSUER_TOKEN:
        raise ValueError(
            "prediction_snapshot_id is wrapper-private; use issue_snapshot_passport"
        )
    _require_publication_allowed(publication_guard)
    source = prediction.get("pin_payload")
    source = source if isinstance(source, Mapping) else {}
    symbol = str(prediction.get("symbol") or source.get("symbol") or "").strip().upper()
    if not symbol:
        raise ValueError("prediction symbol is required")
    normalized_mode = str(prediction_mode or "").strip()
    if not normalized_mode:
        raise ValueError("prediction_mode is required")
    normalized_origin_kind = str(origin_kind or "").strip()
    if not normalized_origin_kind:
        raise ValueError("origin_kind is required")
    raw_predicted_at = prediction.get("timestamp")
    parsed_predicted_at = _timestamp(raw_predicted_at)
    if parsed_predicted_at is None:
        raise ValueError("a valid prediction timestamp is required for deterministic issuance")
    predicted_at = parsed_predicted_at
    if predicted_at > _issuance_now_utc() + timedelta(
        seconds=MAX_PREDICTION_CLOCK_SKEW_SECONDS
    ):
        raise ValueError("prediction timestamp cannot be in the future")
    predicted_trading_day = predicted_at.astimezone(ZoneInfo("America/New_York")).date()
    raw_target_at = target_timestamp_utc
    parsed_target_at = _timestamp(raw_target_at)
    target_error = None
    try:
        official_target_at = _target_close(predicted_trading_day)
    except ValueError as exc:
        official_target_at = predicted_at
        target_error = str(exc)
    target_at = official_target_at
    if raw_target_at is not None and parsed_target_at is not None:
        if parsed_target_at != official_target_at:
            target_error = "target_timestamp_utc does not equal the official cash close"
    trading_day = target_at.astimezone(ZoneInfo("America/New_York")).date()
    horizon_seconds = max(0, int((target_at - predicted_at).total_seconds()))

    provider = str(prediction.get("provider") or source.get("provider") or "").strip() or None
    raw_quote_at = _first_not_none(
        prediction.get("quote_timestamp_utc"),
        source.get("latest_ts_recv_utc"),
        source.get("observation_index_utc"),
        source.get("timestamp"),
    )
    quote_at = _timestamp(raw_quote_at)
    generation_value = prediction.get("subscription_generation")
    if generation_value is None:
        generation_value = source.get("subscription_generation")
    generation, generation_invalid = _nonnegative_int(generation_value)
    raw_epoch_values = [
        value
        for value in (
            prediction.get("subscription_epoch_id"),
            source.get("subscription_epoch_id"),
        )
        if value is not None
    ]
    raw_epoch_id = str(raw_epoch_values[0]).strip() if raw_epoch_values else None
    subscription_epoch_id, subscription_epoch_invalid = (
        _subscription_epoch_evidence(raw_epoch_id)
    )
    if any(str(value).strip() != raw_epoch_id for value in raw_epoch_values[1:]):
        subscription_epoch_id = None
        subscription_epoch_invalid = True
    calculation_id = str(source.get("calculation_id") or "").strip() or None

    feature_snapshot_value = prediction.get("feature_snapshot")
    feature_snapshot_invalid = bool(
        feature_snapshot_value is not None
        and not isinstance(feature_snapshot_value, Mapping)
    )
    feature_snapshot = (
        dict(feature_snapshot_value)
        if isinstance(feature_snapshot_value, Mapping)
        else {}
    )
    computed_feature_hash = _canonical_hash(feature_snapshot) if feature_snapshot else None
    supplied_feature_hash, feature_hash_invalid = _sha256_evidence(prediction.get("feature_hash"))
    feature_hash = supplied_feature_hash or computed_feature_hash
    feature_hash_conflict = bool(
        supplied_feature_hash and computed_feature_hash and supplied_feature_hash != computed_feature_hash
    )
    source_payload_sha256 = _canonical_hash(source) if source else None

    reference_price = _finite(_first_not_none(prediction.get("current_price"), source.get("price")))
    predicted_close = _finite(
        _first_not_none(prediction.get("predicted_close"), prediction.get("predicted_value"))
    )
    lower = _finite(prediction.get("prediction_lower"))
    upper = _finite(prediction.get("prediction_upper"))
    interval_target = _finite(prediction.get("interval_target_coverage"))
    reference_price_invalid = reference_price is None or reference_price <= 0
    predicted_close_invalid = predicted_close is None or predicted_close <= 0
    if reference_price_invalid:
        reference_price = None
    if predicted_close_invalid:
        predicted_close = None
    supplied_interval = (
        prediction.get("prediction_lower") is not None
        or prediction.get("prediction_upper") is not None
    )
    interval_invalid = bool(
        supplied_interval
        and (
            lower is None
            or upper is None
            or predicted_close is None
            or lower <= 0
            or lower > predicted_close
            or predicted_close > upper
        )
    )
    if interval_invalid:
        lower = upper = None
    calibration_evidence_value = prediction.get("calibration_evidence")
    calibration_evidence = (
        dict(calibration_evidence_value)
        if isinstance(calibration_evidence_value, Mapping)
        else None
    )
    computed_calibration_hash = (
        _canonical_hash(calibration_evidence) if calibration_evidence else None
    )
    supplied_calibration_hash, calibration_hash_invalid = _sha256_evidence(
        prediction.get("calibration_evidence_sha256")
    )
    calibration_hash = supplied_calibration_hash or computed_calibration_hash
    calibration_hash_conflict = bool(
        supplied_calibration_hash
        and computed_calibration_hash
        and supplied_calibration_hash != computed_calibration_hash
    )
    model_artifact_hash, model_artifact_hash_invalid = _sha256_evidence(
        prediction.get("model_artifact_sha256")
    )
    model_type = str(prediction.get("model_type") or "").strip() or None
    model_version = str(prediction.get("model_version") or "").strip() or None
    supplied_quote_age = _finite(
        _first_not_none(prediction.get("quote_age_seconds"), source.get("quote_age_seconds"))
    )
    supplied_quote_age_invalid = supplied_quote_age is not None and supplied_quote_age < 0
    derived_quote_age = (
        (predicted_at - quote_at).total_seconds() if quote_at is not None else None
    )
    quote_age_candidates = [
        max(0.0, value)
        for value in (supplied_quote_age, derived_quote_age)
        if value is not None and value >= 0
    ]
    quote_age = max(quote_age_candidates) if quote_age_candidates else None
    data_age = _finite(prediction.get("data_age_seconds"))
    data_age_invalid = data_age is not None and data_age < 0
    if data_age_invalid:
        data_age = None
    active_contract_count, active_contract_count_invalid = _nonnegative_int(
        _first_not_none(
            prediction.get("active_contract_count"),
            source.get("contracts"),
            source.get("contracts_count"),
        )
    )
    fresh_quote_count, fresh_quote_count_invalid = _nonnegative_int(
        _first_not_none(
            prediction.get("fresh_quote_count"),
            source.get("fresh_quote_count"),
        )
    )
    validation_status = str(
        prediction.get("validation_status")
        or source.get("validation_status")
        or ""
    ).strip().lower()
    source_validation_value = _first_not_none(
        prediction.get("validation_is_valid"),
        source.get("validation_is_valid"),
    )
    source_validation_is_valid = source_validation_value is True
    source_validation_invalid = (
        source_validation_value is not None
        and not isinstance(source_validation_value, bool)
    )
    regime = (
        str(prediction.get("regime") or source.get("regime") or "unclassified").strip()
        or "unclassified"
    )
    fallback = bool(
        prediction.get("is_fallback")
        or payload_has_fallback_provenance(source)
        or provider == "historical-fallback"
        or model_type == "pin-payload-fallback"
        or model_version == "fallback-1.0"
    )
    usable_value = prediction.get("usable")
    usable = (
        usable_value is True
        and source_validation_value is not False
        and validation_status != "invalid"
        and not reference_price_invalid
        and not predicted_close_invalid
        and not fallback
    )
    reasons = _state_reasons(prediction, source)
    if target_error:
        reasons.append(f"NON_SESSION_TARGET: {target_error}")
    if raw_target_at is not None and parsed_target_at is None:
        reasons.append("INVALID_TARGET_TIMESTAMP")
    if raw_quote_at is not None and quote_at is None:
        reasons.append("INVALID_SOURCE_TIMESTAMP")
    if generation_invalid:
        reasons.append("INVALID_SUBSCRIPTION_GENERATION")
    if feature_snapshot_invalid:
        reasons.append("INVALID_FEATURE_SNAPSHOT")
    if feature_hash_invalid:
        reasons.append("INVALID_EVIDENCE_SHA256:feature_hash")
    if feature_hash_conflict:
        reasons.append("FEATURE_HASH_MISMATCH")
    if model_artifact_hash_invalid:
        reasons.append("INVALID_EVIDENCE_SHA256:model_artifact_sha256")
    if calibration_hash_invalid:
        reasons.append("INVALID_EVIDENCE_SHA256:calibration_evidence_sha256")
    if calibration_hash_conflict:
        reasons.append("CALIBRATION_EVIDENCE_HASH_MISMATCH")
    if derived_quote_age is not None and derived_quote_age < 0:
        reasons.append("SOURCE_TIMESTAMP_AFTER_PREDICTION")
    if supplied_quote_age_invalid:
        reasons.append("INVALID_QUOTE_AGE")
    if data_age_invalid:
        reasons.append("INVALID_DATA_AGE")
    if source_validation_value is False or validation_status == "invalid":
        reasons.append("SOURCE_VALIDATION_FAILED")
    if source_validation_invalid:
        reasons.append("INVALID_SOURCE_VALIDATION_FLAG")
    if usable_value is not True:
        reasons.append("PREDICTION_NOT_DECLARED_USABLE")
    if reference_price_invalid:
        reasons.append("INVALID_REFERENCE_PRICE")
    if predicted_close_invalid:
        reasons.append("INVALID_POINT_ESTIMATE")
    if interval_invalid:
        reasons.append("INVALID_PREDICTION_INTERVAL")
    if active_contract_count_invalid:
        reasons.append("INVALID_ACTIVE_CONTRACT_COUNT")
    if fresh_quote_count_invalid:
        reasons.append("INVALID_FRESH_QUOTE_COUNT")
    if subscription_epoch_invalid:
        reasons.append("INVALID_SUBSCRIPTION_EPOCH_ID")

    requested = str(requested_state or "").strip().lower() or None
    if requested is not None and requested not in PASSPORT_STATES:
        raise ValueError(f"unsupported passport state: {requested}")
    stale = bool(prediction.get("stale")) or any(
        age is not None and age > LIVE_DATA_STALE_AFTER_SECONDS
        for age in (quote_age, data_age)
    )
    structural_abstention = bool(
        raw_target_at is not None and parsed_target_at is None
        or target_error
        or predicted_at >= target_at
        or raw_quote_at is not None and quote_at is None
        or derived_quote_age is not None and derived_quote_age < 0
        or supplied_quote_age_invalid
        or data_age_invalid
        or source_validation_invalid
        or interval_invalid
        or generation_invalid
    )
    if fallback:
        state = "abstain"
        reasons.append("NON_PRODUCTION_FALLBACK")
    elif requested in {"abstain", "stale", "unavailable"}:
        state = requested
    elif not usable or structural_abstention:
        state = "abstain"
    elif stale:
        state = "stale"
        reasons.append("SOURCE_STALE")
    elif requested:
        state = requested
    else:
        state = "valid" if decision_grade else "research"

    session = database.SessionLocal()
    try:
        _require_publication_allowed(publication_guard)
        snapshot = None
        if prediction_snapshot_id is not None:
            snapshot = session.query(database.PredictionSnapshot).filter(
                database.PredictionSnapshot.id == int(prediction_snapshot_id)
            ).first()
            if snapshot is None:
                raise ValueError("prediction_snapshot_id does not exist")
            _validate_snapshot_binding(_snapshot_prediction(snapshot), prediction)
        lineage = _gamma_lineage(session, calculation_id)
        universe_hash, universe_hash_invalid = _sha256_evidence(
            lineage.get("universe_sha256")
        )
        calculation_input_hash, calculation_input_hash_invalid = _sha256_evidence(
            lineage.get("calculation_input_sha256")
        )
        lineage_errors = _lineage_validation_errors(
            lineage,
            symbol=symbol,
            trading_day=predicted_trading_day,
            provider=provider,
            subscription_epoch_id=subscription_epoch_id,
            generation=generation,
            predicted_at=predicted_at,
            source=source,
            reference_price=reference_price,
        )
        reasons.extend(lineage_errors)
        if universe_hash_invalid:
            reasons.append("INVALID_EVIDENCE_SHA256:universe_sha256")
        if calculation_input_hash_invalid:
            reasons.append("INVALID_EVIDENCE_SHA256:calculation_input_sha256")

        formula_versions = {
            key: source.get(key)
            for key in FORMULA_VERSION_FIELDS
            if source.get(key) is not None and str(source.get(key)).strip()
        }
        run = lineage.get("run")
        run_calculated_at = (
            _timestamp(run.calculated_at_utc) if run is not None else None
        )
        if run is not None:
            formula_versions.setdefault("gex_formula_version", run.formula_version)
            if run.target_formula_version:
                formula_versions.setdefault(
                    "target_formula_version", run.target_formula_version
                )
            if run.spot_formula_version:
                formula_versions.setdefault("spot_formula_version", run.spot_formula_version)
        feature_schema_version = (
            prediction.get("feature_schema_version") or feature_snapshot.get("schema_version")
        )
        calibration_method = str(prediction.get("calibration_method") or "").strip() or None
        current_model_hash = model_artifact_sha256() if model_version == MODEL_VERSION else None
        model_artifact_resolved = bool(
            current_model_hash
            and model_artifact_hash
            and current_model_hash == model_artifact_hash
        )
        if model_artifact_hash and not model_artifact_resolved:
            reasons.append("MODEL_ARTIFACT_NOT_RESOLVED")
        calibration_errors = _calibration_validation_errors(
            calibration_evidence,
            symbol=symbol,
            model_version=model_version,
            horizon_seconds=horizon_seconds,
            regime=regime,
            predicted_at=predicted_at,
            interval_target_coverage=interval_target,
        )
        if state == "valid":
            reasons.extend(calibration_errors)

        required = {
            "symbol": symbol or None,
            "prediction_mode": normalized_mode,
            "prediction_timestamp": parsed_predicted_at,
            "target_timestamp": None
            if raw_target_at is not None and parsed_target_at is None
            else target_at,
            "provider": provider,
            "source_timestamp": quote_at,
            "source_freshness": (
                True
                if quote_age is not None
                and quote_age <= LIVE_DATA_STALE_AFTER_SECONDS
                and (derived_quote_age is None or derived_quote_age >= 0)
                and (data_age is None or data_age <= LIVE_DATA_STALE_AFTER_SECONDS)
                else None
            ),
            "source_validation": (
                True
                if source_validation_is_valid and validation_status in {"valid", "ready"}
                else None
            ),
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": generation,
            "calculation_id": calculation_id,
            "calculation_input_sha256": calculation_input_hash,
            "universe_sha256": universe_hash,
            "feature_hash": feature_hash,
            "feature_schema_version": feature_schema_version,
            "model_version": model_version,
            "model_artifact_sha256": model_artifact_hash,
            "resolved_model_artifact": True if model_artifact_resolved else None,
            "reference_price": reference_price if reference_price and reference_price > 0 else None,
            "predicted_close": predicted_close if predicted_close and predicted_close > 0 else None,
            "prediction_interval": (
                True if lower is not None and predicted_close is not None and upper is not None
                and 0 < lower <= predicted_close <= upper else None
            ),
            "interval_target_coverage": (
                interval_target
                if interval_target is not None and 0 < interval_target <= 1
                else None
            ),
            "calibration_method": calibration_method,
            "calibration_evidence_sha256": calibration_hash,
            "resolved_calibration_evidence": (
                True
                if calibration_hash
                and computed_calibration_hash == calibration_hash
                and not calibration_errors
                else None
            ),
            "formula_versions": formula_versions or None,
            # Decision-grade issuance is deliberately unavailable until a
            # promoted-model adapter supplies a verified human approval.
            "approved_production_manifest": None,
        }
        missing = [name for name, value in required.items() if value is None]
        if feature_hash_conflict or feature_hash_invalid:
            missing.append("matching_feature_hash")
        if model_artifact_hash_invalid:
            missing.append("valid_model_artifact_sha256")
        if calibration_hash_invalid or calibration_hash_conflict:
            missing.append("valid_calibration_evidence_sha256")
        if calibration_errors:
            missing.append("resolved_calibration_evidence")
        if not model_artifact_resolved:
            missing.append("resolved_model_artifact")
        if lineage_errors:
            missing.append("verified_calculation_lineage")
        if universe_hash_invalid:
            missing.append("valid_universe_sha256")
        if calculation_input_hash_invalid:
            missing.append("valid_calculation_input_sha256")
        if predicted_at >= target_at:
            missing.append("pre_target_issue_time")
        missing = list(dict.fromkeys(missing))
        if state == "valid" and missing:
            state = "abstain"
            reasons.extend(f"MISSING_EVIDENCE:{name}" for name in missing)
            predicted_close = lower = upper = None
        if state in {"abstain", "stale", "unavailable"}:
            # Non-valid states never expose a numeric primary forecast.
            predicted_close = lower = upper = None
        if not reasons and state == "research":
            reasons.append("RESEARCH_ONLY_NOT_DECISION_GRADE")
        elif not reasons and state != "valid":
            reasons.append(f"STATE_{state.upper()}")

        confidence = _finite(prediction.get("confidence"))
        confidence_scale = str(prediction.get("confidence_scale") or "unknown")
        confidence_kind = str(
            prediction.get("confidence_kind")
            or ("data_quality_heuristic" if confidence is not None else "unavailable")
        )
        drivers_value = prediction.get("signals")
        if drivers_value is None:
            drivers: list[Any] = []
        elif isinstance(drivers_value, (list, tuple)):
            drivers = list(drivers_value)
        else:
            drivers = []
            reasons.append("INVALID_DRIVER_EVIDENCE")
        state_reasons = list(dict.fromkeys(reasons))
        if origin_key is not None:
            origin_key_value = str(origin_key).strip()
            if not origin_key_value:
                raise ValueError("origin_key cannot be blank when supplied")
        elif prediction_snapshot_id is not None:
            origin_key_value = str(prediction_snapshot_id)
        else:
            origin_key_value = "|".join(
                (
                    symbol,
                    predicted_at.isoformat(),
                    str(subscription_epoch_id),
                    str(generation),
                    quote_at.isoformat() if quote_at else "missing-source-time",
                    normalized_mode,
                )
            )
        if len(normalized_origin_kind) > 40 or len(origin_key_value) > 128:
            raise ValueError("passport origin identity exceeds schema limits")
        identity = {
            "schema_version": PASSPORT_SCHEMA_VERSION,
            "origin_kind": normalized_origin_kind,
            "origin_key": origin_key_value,
        }
        forecast_id = _canonical_hash(identity)
        payload = {
            "schema_version": PASSPORT_SCHEMA_VERSION,
            "forecast_id": forecast_id,
            "origin": {**identity, "prediction_snapshot_id": prediction_snapshot_id},
            "symbol": symbol,
            "prediction_mode": normalized_mode,
            "state": "RESEARCH_ONLY" if state == "research" else state.upper(),
            "decision_grade": bool(decision_grade and state == "valid"),
            "target": {
                "kind": "official_cash_close",
                "trading_date": trading_day.isoformat(),
                "prediction_timestamp_utc": predicted_at.isoformat(),
                "target_timestamp_utc": target_at.isoformat(),
                "horizon_seconds": horizon_seconds,
            },
            "provenance": {
                "provider": provider,
                "quote_timestamp_utc": quote_at.isoformat() if quote_at else None,
                "subscription_epoch_id": subscription_epoch_id,
                "subscription_generation": generation,
                "calculation_id": calculation_id,
                "calculation_status": str(run.status) if run is not None else None,
                "calculated_at_utc": (
                    run_calculated_at.isoformat() if run_calculated_at is not None else None
                ),
                "input_schema_version": (
                    str(run.input_schema_version) if run is not None else None
                ),
                "calculation_input_sha256": calculation_input_hash,
                "source_payload_sha256": source_payload_sha256,
                "universe_sha256": universe_hash,
            },
            "model": {
                "model_version": model_version,
                "model_type": model_type,
                "model_artifact_sha256": model_artifact_hash,
                "feature_schema_version": feature_schema_version,
                "feature_hash": feature_hash,
                "formula_versions": formula_versions,
                "inference_device": prediction.get("inference_device"),
                "calibration_method": calibration_method,
                "calibration_evidence_sha256": calibration_hash,
                "calibration_evidence": calibration_evidence,
            },
            "prediction": {
                "reference_price": reference_price,
                "point_estimate": predicted_close,
                "interval_lower": lower,
                "interval_upper": upper,
                "interval_target_coverage": interval_target,
                "confidence_raw": confidence,
                "confidence_scale": confidence_scale,
                "confidence_kind": confidence_kind,
                "baseline": {"name": "persistence", "value": reference_price},
            },
            "quality": {
                "validation_status": validation_status
                or ("valid" if source_validation_is_valid and usable else "invalid"),
                "source_validation_is_valid": source_validation_is_valid,
                "data_age_seconds": data_age,
                "quote_age_seconds": quote_age,
                "active_contract_count": active_contract_count,
                "fresh_quote_count": fresh_quote_count,
                "regime": regime,
                "state_reasons": state_reasons,
                "missing_evidence": missing,
            },
            "drivers": drivers,
            "feature_snapshot": feature_snapshot,
        }
        payload["replay"] = _replay_projection(payload)
        canonical = _canonical_json(payload)
        record_hash = _sha256_text(canonical)
        existing = session.query(database.PredictionPassport).filter(
            database.PredictionPassport.origin_kind == normalized_origin_kind,
            database.PredictionPassport.origin_key == origin_key_value,
        ).first()
        if existing is not None:
            if existing.record_sha256 != record_hash:
                raise PassportConflictError("immutable passport origin has conflicting content")
            _require_publication_allowed(publication_guard)
            return read_prediction_passport(existing.forecast_id, session=session)

        row = database.PredictionPassport(
            passport_id=forecast_id,
            forecast_id=forecast_id,
            schema_version=PASSPORT_SCHEMA_VERSION,
            origin_kind=normalized_origin_kind,
            origin_key=origin_key_value,
            prediction_snapshot_id=prediction_snapshot_id,
            symbol=symbol,
            prediction_mode=normalized_mode,
            state=state,
            target_kind="official_cash_close",
            target_trading_date=trading_day,
            prediction_timestamp_utc=_naive_utc(predicted_at),
            target_timestamp_utc=_naive_utc(target_at),
            horizon_seconds=horizon_seconds,
            provider=provider,
            quote_timestamp_utc=_naive_utc(quote_at) if quote_at else None,
            subscription_epoch_id=subscription_epoch_id,
            subscription_generation=generation,
            calculation_id=calculation_id,
            calculation_input_sha256=calculation_input_hash,
            source_payload_sha256=source_payload_sha256,
            universe_sha256=universe_hash,
            model_version=model_version,
            model_type=model_type,
            model_artifact_sha256=model_artifact_hash,
            feature_schema_version=payload["model"]["feature_schema_version"],
            feature_hash=feature_hash,
            formula_versions_json=_canonical_json(formula_versions),
            inference_device=prediction.get("inference_device"),
            reference_price=reference_price,
            predicted_close=predicted_close,
            prediction_lower=lower,
            prediction_upper=upper,
            confidence_raw=confidence,
            confidence_scale=confidence_scale,
            confidence_kind=confidence_kind,
            interval_target_coverage=interval_target,
            calibration_method=calibration_method,
            calibration_evidence_sha256=calibration_hash,
            validation_status=str(payload["quality"]["validation_status"]),
            data_age_seconds=payload["quality"]["data_age_seconds"],
            quote_age_seconds=quote_age,
            active_contract_count=payload["quality"]["active_contract_count"],
            fresh_quote_count=payload["quality"]["fresh_quote_count"],
            state_reasons_json=_canonical_json({"items": state_reasons}),
            missing_evidence_json=_canonical_json({"items": missing}),
            provenance_json=_canonical_json(payload["provenance"]),
            canonical_payload_json=canonical,
            record_sha256=record_hash,
        )
        values = {
            key: value for key, value in vars(row).items()
            if key != "_sa_instance_state"
        }
        # Release the lineage/identity read transaction before acquiring the
        # writer. All content and its immutable digest are already fixed.
        session.close()
        return _persist_passport_bounded(values, publication_guard=publication_guard)
    except IntegrityError as exc:
        session.rollback()
        existing = session.query(database.PredictionPassport).filter(
            database.PredictionPassport.forecast_id == forecast_id
        ).first()
        if existing is not None and existing.record_sha256 == record_hash:
            _require_publication_allowed(publication_guard)
            return read_prediction_passport(existing.forecast_id, session=session)
        raise PassportConflictError("passport identity collision or conflicting retry") from exc
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def issue_snapshot_passport(
    prediction_snapshot_id: int,
    *,
    prediction: Mapping[str, Any] | None = None,
    decision_grade: bool = False,
    requested_state: str | None = None,
    publication_guard: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    _require_publication_allowed(publication_guard)
    session = database.SessionLocal()
    try:
        snapshot = session.query(database.PredictionSnapshot).filter(
            database.PredictionSnapshot.id == int(prediction_snapshot_id)
        ).first()
        if snapshot is None:
            raise KeyError(prediction_snapshot_id)
        snapshot_payload = _snapshot_prediction(snapshot)
        if prediction is not None:
            _validate_snapshot_binding(snapshot_payload, prediction)
            # Stored snapshot fields are authoritative. The supplied record may
            # only add evidence the compatibility snapshot schema does not hold.
            payload = {**dict(prediction), **snapshot_payload}
        else:
            payload = snapshot_payload
        mode = snapshot.prediction_mode or "legacy"
        _require_publication_allowed(publication_guard)
    finally:
        session.close()
    return issue_prediction_passport(
        payload,
        prediction_mode=mode,
        prediction_snapshot_id=prediction_snapshot_id,
        origin_kind="prediction_snapshot",
        origin_key=str(prediction_snapshot_id),
        requested_state=requested_state,
        decision_grade=decision_grade,
        publication_guard=publication_guard,
        _snapshot_issuer_token=_SNAPSHOT_ISSUER_TOKEN,
    )


def _latest_outcome(session, snapshot_id: int | None, passport: dict[str, Any]) -> dict[str, Any] | None:
    if snapshot_id is None:
        return None
    snapshot = session.query(database.PredictionSnapshot).filter(
        database.PredictionSnapshot.id == int(snapshot_id)
    ).first()
    target = passport.get("target")
    origin = passport.get("origin")
    if (
        snapshot is None
        or str(snapshot.symbol).upper() != str(passport.get("symbol") or "").upper()
        or not isinstance(target, Mapping)
        or str(target.get("trading_date") or "") != snapshot.trading_date.isoformat()
        or not isinstance(origin, Mapping)
        or origin.get("prediction_snapshot_id") != int(snapshot_id)
    ):
        raise PassportIntegrityError("passport outcome snapshot binding is invalid")
    reference = _finite(passport.get("prediction", {}).get("reference_price"))
    point = _finite(passport.get("prediction", {}).get("point_estimate"))
    if reference is not None and not math.isclose(
        reference, float(snapshot.current_price), rel_tol=1e-12, abs_tol=1e-12
    ):
        raise PassportIntegrityError("passport outcome reference price binding is invalid")
    if point is not None and not math.isclose(
        point, float(snapshot.predicted_close), rel_tol=1e-12, abs_tol=1e-12
    ):
        raise PassportIntegrityError("passport outcome point estimate binding is invalid")
    row = session.query(database.PredictionAccuracyObservation).filter(
        database.PredictionAccuracyObservation.prediction_id == int(snapshot_id)
    ).order_by(database.PredictionAccuracyObservation.close_observation_id.desc()).first()
    if row is None:
        return None
    lower = _finite(passport.get("prediction", {}).get("interval_lower"))
    upper = _finite(passport.get("prediction", {}).get("interval_upper"))
    baseline_error = abs(reference - row.actual_close) if reference is not None else None
    model_error = abs(point - row.actual_close) if point is not None else None
    scored_at = _timestamp(row.scored_at_utc)
    if scored_at is None:
        raise PassportIntegrityError("passport outcome timestamp is invalid")
    return {
        "score_key": row.score_key,
        "close_observation_id": row.close_observation_id,
        # ``verified`` is retained for API compatibility and refers only to the
        # official-close evidence, not to model-validation eligibility.
        "verified": True,
        "close_verified": True,
        "evidence_scope": row.evidence_scope,
        "training_eligible": bool(row.training_eligible),
        "performance_claim_eligible": False,
        "actual_close": row.actual_close,
        "error_points": row.error_points,
        "absolute_error_points": model_error,
        "baseline_absolute_error_points": baseline_error,
        "baseline_lift_points": (
            baseline_error - model_error
            if baseline_error is not None and model_error is not None else None
        ),
        "interval_covered": (
            lower <= row.actual_close <= upper
            if lower is not None and upper is not None else None
        ),
        "close_source": row.close_source,
        "close_source_reference": row.close_source_reference,
        "close_source_artifact_sha256": row.close_source_artifact_sha256,
        "scored_at_utc": scored_at.isoformat(),
    }


def _replay_projection(passport: dict[str, Any]) -> dict[str, Any]:
    point = _finite(passport.get("prediction", {}).get("point_estimate"))
    features = passport.get("feature_snapshot")
    if point is None or not isinstance(features, dict):
        return {"status": "UNAVAILABLE", "reason": "point estimate or replay inputs unavailable"}
    model = passport.get("model")
    model = model if isinstance(model, dict) else {}
    if model.get("model_version") != MODEL_VERSION:
        return {
            "status": "UNAVAILABLE",
            "reason": "the stored model version has no registered replay adapter",
        }
    stored_artifact = str(model.get("model_artifact_sha256") or "").lower()
    if not stored_artifact or stored_artifact != model_artifact_sha256():
        return {
            "status": "UNAVAILABLE",
            "reason": "the stored model artifact is not available in this runtime",
        }
    try:
        replayed = replay_ai_prediction(features)
    except ValueError as exc:
        return {"status": "UNAVAILABLE", "reason": str(exc)}
    tolerance = max(1e-9, abs(point) * 1e-10)
    error = replayed - point
    return {
        "status": "VERIFIED" if abs(error) <= tolerance else "MISMATCH",
        "replayed_point_estimate": replayed,
        "stored_point_estimate": point,
        "error_points": error,
        "tolerance_points": tolerance,
    }


def replay_prediction_passport(forecast_id: str) -> dict[str, Any]:
    """Explicitly replay a forecast without mutating its immutable passport."""

    passport = read_prediction_passport(forecast_id)
    return {
        "forecast_id": passport["forecast_id"],
        "record_sha256": passport["record_sha256"],
        "replay": _replay_projection(passport),
    }


def _required_object(document: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = document.get(key)
    if not isinstance(value, Mapping):
        raise PassportIntegrityError(f"passport {key} evidence is missing")
    return value


def _decoded_object(value: str, *, field: str) -> Mapping[str, Any]:
    try:
        decoded = json.loads(value)
    except (json.JSONDecodeError, TypeError) as exc:
        raise PassportIntegrityError(f"passport {field} mirror is invalid") from exc
    if not isinstance(decoded, Mapping):
        raise PassportIntegrityError(f"passport {field} mirror is not an object")
    return decoded


def _optional_numbers_match(left: Any, right: Any) -> bool:
    if left is None and right is None:
        return True
    return _same_number(left, right)


def _verify_relational_mirror(
    row: database.PredictionPassport,
    passport: Mapping[str, Any],
) -> None:
    """Detect corruption in indexed/mirrored columns used to select passports."""

    origin = _required_object(passport, "origin")
    target = _required_object(passport, "target")
    provenance = _required_object(passport, "provenance")
    model = _required_object(passport, "model")
    prediction = _required_object(passport, "prediction")
    quality = _required_object(passport, "quality")

    external_state = str(passport.get("state") or "").upper()
    payload_state = "research" if external_state in {"RESEARCH", "RESEARCH_ONLY"} else external_state.lower()
    if (
        passport.get("schema_version") != PASSPORT_SCHEMA_VERSION
        or origin.get("schema_version") != PASSPORT_SCHEMA_VERSION
        or payload_state not in PASSPORT_STATES
        or not isinstance(passport.get("decision_grade"), bool)
        or (passport.get("decision_grade") and payload_state != "valid")
        or not isinstance(passport.get("feature_snapshot"), Mapping)
        or not isinstance(passport.get("drivers"), list)
        or not isinstance(quality.get("state_reasons"), list)
        or not isinstance(quality.get("missing_evidence"), list)
    ):
        raise PassportIntegrityError("passport canonical structure is invalid")
    text_pairs = {
        "schema_version": (row.schema_version, passport.get("schema_version")),
        "origin_kind": (row.origin_kind, origin.get("origin_kind")),
        "origin_key": (row.origin_key, origin.get("origin_key")),
        "symbol": (row.symbol, passport.get("symbol")),
        "prediction_mode": (row.prediction_mode, passport.get("prediction_mode")),
        "state": (row.state, payload_state),
        "target_kind": (row.target_kind, target.get("kind")),
        "provider": (row.provider, provenance.get("provider")),
        "subscription_epoch_id": (
            row.subscription_epoch_id,
            provenance.get("subscription_epoch_id"),
        ),
        "calculation_id": (row.calculation_id, provenance.get("calculation_id")),
        "calculation_input_sha256": (
            row.calculation_input_sha256,
            provenance.get("calculation_input_sha256"),
        ),
        "source_payload_sha256": (
            row.source_payload_sha256,
            provenance.get("source_payload_sha256"),
        ),
        "universe_sha256": (row.universe_sha256, provenance.get("universe_sha256")),
        "model_version": (row.model_version, model.get("model_version")),
        "model_type": (row.model_type, model.get("model_type")),
        "model_artifact_sha256": (
            row.model_artifact_sha256,
            model.get("model_artifact_sha256"),
        ),
        "feature_schema_version": (
            row.feature_schema_version,
            model.get("feature_schema_version"),
        ),
        "feature_hash": (row.feature_hash, model.get("feature_hash")),
        "inference_device": (row.inference_device, model.get("inference_device")),
        "confidence_scale": (row.confidence_scale, prediction.get("confidence_scale")),
        "confidence_kind": (row.confidence_kind, prediction.get("confidence_kind")),
        "calibration_method": (row.calibration_method, model.get("calibration_method")),
        "calibration_evidence_sha256": (
            row.calibration_evidence_sha256,
            model.get("calibration_evidence_sha256"),
        ),
        "validation_status": (row.validation_status, quality.get("validation_status")),
    }
    for field, (stored, canonical) in text_pairs.items():
        if stored != canonical:
            raise PassportIntegrityError(f"passport {field} mirror mismatch")

    integer_pairs = {
        "prediction_snapshot_id": (
            row.prediction_snapshot_id,
            origin.get("prediction_snapshot_id"),
        ),
        "horizon_seconds": (row.horizon_seconds, target.get("horizon_seconds")),
        "subscription_generation": (
            row.subscription_generation,
            provenance.get("subscription_generation"),
        ),
        "active_contract_count": (
            row.active_contract_count,
            quality.get("active_contract_count"),
        ),
        "fresh_quote_count": (row.fresh_quote_count, quality.get("fresh_quote_count")),
    }
    for field, (stored, canonical) in integer_pairs.items():
        if stored is None and canonical is None:
            continue
        if isinstance(canonical, bool):
            raise PassportIntegrityError(f"passport {field} mirror mismatch")
        try:
            matches = int(stored) == int(canonical) and float(canonical) == int(canonical)
        except (TypeError, ValueError, OverflowError):
            matches = False
        if not matches:
            raise PassportIntegrityError(f"passport {field} mirror mismatch")

    date_value = target.get("trading_date")
    if not isinstance(date_value, str) or row.target_trading_date.isoformat() != date_value:
        raise PassportIntegrityError("passport target_trading_date mirror mismatch")
    timestamp_pairs = {
        "prediction_timestamp_utc": (
            row.prediction_timestamp_utc,
            target.get("prediction_timestamp_utc"),
        ),
        "target_timestamp_utc": (
            row.target_timestamp_utc,
            target.get("target_timestamp_utc"),
        ),
        "quote_timestamp_utc": (
            row.quote_timestamp_utc,
            provenance.get("quote_timestamp_utc"),
        ),
    }
    for field, (stored, canonical) in timestamp_pairs.items():
        if stored is None and canonical is None:
            continue
        if _timestamp(stored) is None or _timestamp(stored) != _timestamp(canonical):
            raise PassportIntegrityError(f"passport {field} mirror mismatch")

    number_pairs = {
        "reference_price": (row.reference_price, prediction.get("reference_price")),
        "predicted_close": (row.predicted_close, prediction.get("point_estimate")),
        "prediction_lower": (row.prediction_lower, prediction.get("interval_lower")),
        "prediction_upper": (row.prediction_upper, prediction.get("interval_upper")),
        "confidence_raw": (row.confidence_raw, prediction.get("confidence_raw")),
        "interval_target_coverage": (
            row.interval_target_coverage,
            prediction.get("interval_target_coverage"),
        ),
        "data_age_seconds": (row.data_age_seconds, quality.get("data_age_seconds")),
        "quote_age_seconds": (row.quote_age_seconds, quality.get("quote_age_seconds")),
    }
    for field, (stored, canonical) in number_pairs.items():
        if not _optional_numbers_match(stored, canonical):
            raise PassportIntegrityError(f"passport {field} mirror mismatch")

    json_pairs = {
        "formula_versions_json": (
            _decoded_object(row.formula_versions_json, field="formula_versions_json"),
            model.get("formula_versions"),
        ),
        "state_reasons_json": (
            _decoded_object(row.state_reasons_json, field="state_reasons_json"),
            {"items": quality.get("state_reasons")},
        ),
        "missing_evidence_json": (
            _decoded_object(row.missing_evidence_json, field="missing_evidence_json"),
            {"items": quality.get("missing_evidence")},
        ),
        "provenance_json": (
            _decoded_object(row.provenance_json, field="provenance_json"),
            provenance,
        ),
    }
    for field, (stored, canonical) in json_pairs.items():
        if not isinstance(canonical, Mapping) or _canonical_json(stored) != _canonical_json(canonical):
            raise PassportIntegrityError(f"passport {field} mirror mismatch")


def _verified_passport_core(row: database.PredictionPassport) -> dict[str, Any]:
    try:
        passport = json.loads(row.canonical_payload_json)
    except json.JSONDecodeError as exc:
        raise PassportIntegrityError("passport canonical payload is not valid JSON") from exc
    if not isinstance(passport, dict):
        raise PassportIntegrityError("passport canonical payload is not an object")
    if (
        len(str(row.record_sha256 or "")) != 64
        or any(character not in _HEX_DIGITS for character in str(row.record_sha256 or ""))
        or _canonical_hash(passport) != row.record_sha256
    ):
        raise PassportIntegrityError("passport record hash mismatch")
    origin = passport.get("origin")
    if not isinstance(origin, dict):
        raise PassportIntegrityError("passport origin identity is missing")
    identity = {
        "schema_version": origin.get("schema_version"),
        "origin_kind": origin.get("origin_kind"),
        "origin_key": origin.get("origin_key"),
    }
    expected_forecast_id = _canonical_hash(identity)
    if (
        row.passport_id != row.forecast_id
        or passport.get("forecast_id") != row.forecast_id
        or expected_forecast_id != row.forecast_id
    ):
        raise PassportIntegrityError("passport forecast identity mismatch")
    _verify_relational_mirror(row, passport)
    return passport


def passport_detail_envelope(passport: Mapping[str, Any]) -> dict[str, Any]:
    """Separate immutable core content from the current outcome projection."""

    core = {
        key: value
        for key, value in passport.items()
        if key not in {"record_sha256", "created_at_utc", "outcome"}
    }
    return {
        "passport": core,
        "record_sha256": passport["record_sha256"],
        "created_at_utc": passport["created_at_utc"],
        "outcome": passport.get("outcome"),
    }


def read_prediction_passport(
    forecast_id: str,
    *,
    session=None,
) -> dict[str, Any]:
    owns_session = session is None
    current = session or database.SessionLocal()
    try:
        row = current.query(database.PredictionPassport).filter(
            database.PredictionPassport.forecast_id == str(forecast_id)
        ).first()
        if row is None:
            raise KeyError(forecast_id)
        passport = _verified_passport_core(row)
        result = dict(passport)
        result["record_sha256"] = row.record_sha256
        created_at = _timestamp(row.created_at_utc)
        if created_at is None:
            raise PassportIntegrityError("passport creation timestamp is invalid")
        result["created_at_utc"] = created_at.isoformat()
        result["outcome"] = _latest_outcome(current, row.prediction_snapshot_id, passport)
        return result
    finally:
        if owns_session:
            current.close()


def list_prediction_passports(
    *,
    symbol: str | None = None,
    state: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    session = database.SessionLocal()
    try:
        query = session.query(database.PredictionPassport)
        if symbol:
            query = query.filter(database.PredictionPassport.symbol == symbol.upper())
        if state:
            normalized = state.lower()
            if normalized == "research_only":
                normalized = "research"
            if normalized not in PASSPORT_STATES:
                raise ValueError("invalid passport state")
            query = query.filter(database.PredictionPassport.state == normalized)
        rows = query.order_by(
            database.PredictionPassport.prediction_timestamp_utc.desc()
        ).limit(limit).all()
        return [read_prediction_passport(row.forecast_id, session=session) for row in rows]
    finally:
        session.close()


def list_prediction_passport_summaries(
    *,
    symbol: str | None = None,
    state: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """List compact immutable summaries with one query and no outcome joins."""

    if limit < 1 or limit > 1000:
        raise ValueError("limit must be between 1 and 1000")
    session = database.SessionLocal()
    try:
        query = session.query(database.PredictionPassport)
        if symbol:
            query = query.filter(database.PredictionPassport.symbol == symbol.upper())
        if state:
            normalized = state.lower()
            if normalized == "research_only":
                normalized = "research"
            if normalized not in PASSPORT_STATES:
                raise ValueError("invalid passport state")
            query = query.filter(database.PredictionPassport.state == normalized)
        rows = query.order_by(
            database.PredictionPassport.prediction_timestamp_utc.desc()
        ).limit(limit).all()
        items: list[dict[str, Any]] = []
        for row in rows:
            core = _verified_passport_core(row)
            created_at = _timestamp(row.created_at_utc)
            if created_at is None:
                raise PassportIntegrityError("passport creation timestamp is invalid")
            target = core.get("target") or {}
            provenance = core.get("provenance") or {}
            model = core.get("model") or {}
            forecast = core.get("prediction") or {}
            quality = core.get("quality") or {}
            items.append(
                {
                    "forecast_id": core["forecast_id"],
                    "symbol": core["symbol"],
                    "state": core["state"],
                    "decision_grade": bool(core["decision_grade"]),
                    "prediction_mode": core["prediction_mode"],
                    "prediction_timestamp_utc": target["prediction_timestamp_utc"],
                    "target_timestamp_utc": target["target_timestamp_utc"],
                    "horizon_seconds": int(target["horizon_seconds"]),
                    "provider": provenance.get("provider"),
                    "model_version": model.get("model_version"),
                    "point_estimate": forecast.get("point_estimate"),
                    "missing_evidence": list(quality.get("missing_evidence") or []),
                    "record_sha256": row.record_sha256,
                    "created_at_utc": created_at.isoformat(),
                }
            )
        return items
    finally:
        session.close()
