from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from .dataset import PRODUCTION_FAMILIES
from .model_artifact import FrozenModelArtifact, load_frozen_model_artifact_bytes
from .status import _model_gate
from .surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    select_decision_horizon_features,
)


MAX_MANIFEST_BYTES = 1024 * 1024
UTC = timezone.utc
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PROMOTED_RUNTIME_TOKEN = object()
_PROMOTED_BATCH_TOKEN = object()
REPLAY_LOG_RETURN_ABSOLUTE_TOLERANCE = 1e-7
REPLAY_LEVEL_RELATIVE_TOLERANCE = 1e-7

PROMOTED_PREDICTION_COLUMNS = (
    "prediction_key", "trading_date", "session_id", "family_root",
    "decision_horizon_minutes", "feature_available_at_utc", "recorded_at_utc",
    "reference_price", "predicted_log_return", "predicted_level",
    "prediction_lower", "prediction_upper", "interval_radius_log_return",
    "interval_alpha", "interval_target_coverage", "calibration_method",
    "calibration_evidence_sha256", "source_sha256", "model_version",
    "artifact_sha256", "execution_device", "prediction_mode", "is_estimate",
)

PROMOTED_PREDICTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS promoted_close_predictions (
    prediction_key TEXT PRIMARY KEY NOT NULL,
    trading_date TEXT NOT NULL,
    session_id TEXT NOT NULL,
    family_root TEXT NOT NULL,
    decision_horizon_minutes INTEGER NOT NULL,
    feature_available_at_utc TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    reference_price REAL NOT NULL,
    predicted_log_return REAL NOT NULL,
    predicted_level REAL NOT NULL,
    prediction_lower REAL NOT NULL,
    prediction_upper REAL NOT NULL,
    interval_radius_log_return REAL NOT NULL,
    interval_alpha REAL NOT NULL,
    interval_target_coverage REAL NOT NULL,
    calibration_method TEXT NOT NULL,
    calibration_evidence_sha256 TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    model_version TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    execution_device TEXT NOT NULL,
    prediction_mode TEXT NOT NULL,
    is_estimate INTEGER NOT NULL CHECK (is_estimate=1),
    UNIQUE(model_version, artifact_sha256, family_root, trading_date,
           decision_horizon_minutes)
);
CREATE TRIGGER IF NOT EXISTS promoted_close_predictions_no_update
BEFORE UPDATE ON promoted_close_predictions
BEGIN SELECT RAISE(ABORT, 'promoted close predictions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS promoted_close_predictions_no_delete
BEFORE DELETE ON promoted_close_predictions
BEGIN SELECT RAISE(ABORT, 'promoted close predictions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS promoted_close_predictions_no_replace
BEFORE INSERT ON promoted_close_predictions
WHEN EXISTS (
    SELECT 1 FROM promoted_close_predictions
    WHERE prediction_key=NEW.prediction_key
       OR (
           model_version=NEW.model_version
           AND artifact_sha256=NEW.artifact_sha256
           AND family_root=NEW.family_root
           AND trading_date=NEW.trading_date
           AND decision_horizon_minutes=NEW.decision_horizon_minutes
       )
)
BEGIN SELECT RAISE(ABORT, 'promoted close predictions are immutable'); END;
"""


def _normalized_schema_sql(value: str) -> str:
    normalized = re.sub(r"\s+", " ", str(value or "").strip()).lower()
    normalized = normalized.replace("create table if not exists", "create table")
    normalized = normalized.replace("create trigger if not exists", "create trigger")
    return normalized.rstrip(";")


_PROMOTED_TABLE_SQL = _normalized_schema_sql(
    PROMOTED_PREDICTION_SCHEMA.split("CREATE TRIGGER", 1)[0]
)
_PROMOTED_TRIGGER_SQL = {
    match.group(1): _normalized_schema_sql(
        f"CREATE TRIGGER {match.group(1)} {match.group(2)}"
    )
    for match in re.finditer(
        r"CREATE TRIGGER IF NOT EXISTS\s+([a-zA-Z0-9_]+)\s+(.*?END;)",
        PROMOTED_PREDICTION_SCHEMA,
        re.DOTALL,
    )
}


@dataclass(frozen=True, init=False)
class PromotedModelRuntime:
    version: str
    artifact_sha256: str
    execution_device: str
    inference_batch_rows: int
    artifact: FrozenModelArtifact
    calibration_method: str
    calibration_evidence_sha256: str
    interval_alpha: float
    interval_target_coverage: float
    family_radius_log_return: tuple[tuple[str, float], ...]
    promotion_approval_contract_version: str
    promotion_proposal_sha256: str
    promotion_approval_receipt_sha256: str
    _authority_token: object = field(repr=False, compare=False)


@dataclass(frozen=True)
class PromotedPrediction:
    family_root: str
    reference_price: float
    predicted_log_return: float
    predicted_level: float
    prediction_lower: float
    prediction_upper: float
    interval_radius_log_return: float
    interval_alpha: float
    interval_target_coverage: float
    calibration_method: str
    calibration_evidence_sha256: str
    feature_available_at_utc: str
    source_sha256: str
    model_version: str
    artifact_sha256: str
    execution_device: str
    prediction_mode: str = "tcbbo_promoted"
    is_estimate: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, init=False)
class PromotedPredictionBatch(Sequence[PromotedPrediction]):
    predictions: tuple[PromotedPrediction, ...]
    runtime: PromotedModelRuntime
    feature_hash: str
    model_feature_rows_by_family: tuple[tuple[str, tuple[float, ...]], ...]
    volatility_regime_by_family: tuple[tuple[str, str], ...]
    _authority_token: object = field(repr=False, compare=False)

    def __len__(self) -> int:
        return len(self.predictions)

    def __iter__(self):
        return iter(self.predictions)

    def __getitem__(self, index):
        return self.predictions[index]


@dataclass(frozen=True)
class PromotedReplayResult:
    family_root: str
    replayed_log_return: float
    log_return_absolute_error: float
    max_level_absolute_error: float
    level_absolute_tolerance: float


def _build_promoted_model_runtime(**values: object) -> PromotedModelRuntime:
    runtime = object.__new__(PromotedModelRuntime)
    for name, value in values.items():
        object.__setattr__(runtime, name, value)
    object.__setattr__(runtime, "_authority_token", _PROMOTED_RUNTIME_TOKEN)
    return runtime


def require_promoted_model_runtime(runtime: PromotedModelRuntime) -> None:
    if (
        type(runtime) is not PromotedModelRuntime
        or getattr(runtime, "_authority_token", None) is not _PROMOTED_RUNTIME_TOKEN
    ):
        raise ValueError("a loader-issued promoted model runtime is required")


def _build_promoted_prediction_batch(
    predictions: tuple[PromotedPrediction, ...],
    runtime: PromotedModelRuntime,
    *,
    feature_hash: str | None = None,
    model_feature_rows_by_family: Sequence[tuple[str, Sequence[float]]] = (),
    volatility_regime_by_family: Sequence[tuple[str, str]] = (),
) -> PromotedPredictionBatch:
    require_promoted_model_runtime(runtime)
    normalized_feature_rows = tuple(
        sorted(
            (
                str(family).strip().upper(),
                tuple(float(value) for value in values),
            )
            for family, values in model_feature_rows_by_family
        )
    )
    if normalized_feature_rows:
        if (
            len(normalized_feature_rows) != len(PRODUCTION_FAMILIES)
            or {family for family, _values in normalized_feature_rows}
            != set(PRODUCTION_FAMILIES)
        ):
            raise ValueError(
                "promoted prediction model features must cover every production family"
            )
        if any(
            len(values) != len(MODEL_FEATURE_COLUMNS)
            or not np.isfinite(np.asarray(values, dtype=float)).all()
            for _family, values in normalized_feature_rows
        ):
            raise ValueError(
                "promoted prediction model feature rows are incomplete or non-finite"
            )
    normalized_feature_hash = str(feature_hash or "").strip().lower()
    if normalized_feature_rows:
        calculated_feature_hash = promoted_feature_evidence_sha256(
            predictions,
            normalized_feature_rows,
        )
        if normalized_feature_hash and normalized_feature_hash != calculated_feature_hash:
            raise ValueError(
                "promoted prediction feature hash does not match retained model inputs"
            )
        normalized_feature_hash = calculated_feature_hash
    elif not SHA256_PATTERN.fullmatch(normalized_feature_hash):
        # This private builder is retained for narrow mutation tests. Real
        # production batches always receive a hash of their exact selected
        # point-in-time feature rows from ``predict_promoted_close``.
        normalized_feature_hash = hashlib.sha256(
            json.dumps(
                [item.to_dict() for item in predictions],
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
    normalized_regimes = tuple(
        sorted(
            (
                str(family).strip().upper(),
                str(regime).strip().lower(),
            )
            for family, regime in volatility_regime_by_family
        )
    )
    if normalized_regimes:
        if {family for family, _regime in normalized_regimes} != set(PRODUCTION_FAMILIES):
            raise ValueError("promoted prediction regime context must cover every production family")
        if any(regime not in {"calm", "normal", "stressed", "unavailable"} for _family, regime in normalized_regimes):
            raise ValueError("promoted prediction volatility regime is invalid")
    else:
        normalized_regimes = tuple(
            (family, "unavailable") for family in sorted(PRODUCTION_FAMILIES)
        )
    batch = object.__new__(PromotedPredictionBatch)
    object.__setattr__(batch, "predictions", predictions)
    object.__setattr__(batch, "runtime", runtime)
    object.__setattr__(batch, "feature_hash", normalized_feature_hash)
    object.__setattr__(
        batch, "model_feature_rows_by_family", normalized_feature_rows
    )
    object.__setattr__(batch, "volatility_regime_by_family", normalized_regimes)
    object.__setattr__(batch, "_authority_token", _PROMOTED_BATCH_TOKEN)
    return batch


def require_promoted_prediction_batch(batch: PromotedPredictionBatch) -> None:
    if (
        type(batch) is not PromotedPredictionBatch
        or getattr(batch, "_authority_token", None) is not _PROMOTED_BATCH_TOKEN
    ):
        raise ValueError("a loader-authorized promoted prediction batch is required")
    require_promoted_model_runtime(batch.runtime)
    if not SHA256_PATTERN.fullmatch(str(batch.feature_hash or "").lower()):
        raise ValueError("promoted prediction batch feature hash is invalid")


def promoted_feature_evidence_sha256(
    predictions: Sequence[PromotedPrediction | Mapping[str, object]],
    model_feature_rows_by_family: Sequence[tuple[str, Sequence[float]]],
) -> str:
    """Hash the exact ordered model inputs and their point-in-time identity."""
    def field(item: PromotedPrediction | Mapping[str, object], name: str) -> object:
        return item[name] if isinstance(item, Mapping) else getattr(item, name)

    predictions_by_family = {
        str(field(item, "family_root")).upper(): item for item in predictions
    }
    feature_rows = {
        str(family).upper(): tuple(float(value) for value in values)
        for family, values in model_feature_rows_by_family
    }
    if (
        set(predictions_by_family) != set(PRODUCTION_FAMILIES)
        or set(feature_rows) != set(PRODUCTION_FAMILIES)
    ):
        raise ValueError("promoted feature evidence must cover every production family")
    evidence: list[dict[str, object]] = []
    for family in sorted(PRODUCTION_FAMILIES):
        item = predictions_by_family[family]
        values = feature_rows[family]
        if len(values) != len(MODEL_FEATURE_COLUMNS) or not np.isfinite(
            np.asarray(values, dtype=float)
        ).all():
            raise ValueError(
                f"promoted feature evidence is incomplete or non-finite for {family}"
            )
        row: dict[str, object] = {
            "family_root": family,
            "feature_available_at_utc": _utc_iso(
                field(item, "feature_available_at_utc")  # type: ignore[arg-type]
            ),
            "reference_price_hex": float(field(item, "reference_price")).hex(),
            "source_sha256": str(field(item, "source_sha256")).lower(),
        }
        row.update(
            {
                column: float(value).hex()
                for column, value in zip(MODEL_FEATURE_COLUMNS, values)
            }
        )
        evidence.append(row)
    return hashlib.sha256(
        json.dumps(
            evidence,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def verify_promoted_prediction_batch_replay(
    batch: PromotedPredictionBatch,
) -> tuple[PromotedReplayResult, ...]:
    """Rerun exact retained inputs and reject any output that exceeds tolerance."""
    require_promoted_prediction_batch(batch)
    feature_rows = tuple(batch.model_feature_rows_by_family)
    if len(feature_rows) != len(PRODUCTION_FAMILIES):
        raise ValueError(
            "promoted prediction batch lacks exact retained model feature rows"
        )
    calculated_hash = promoted_feature_evidence_sha256(batch, feature_rows)
    if calculated_hash != str(batch.feature_hash).lower():
        raise ValueError(
            "promoted prediction retained model inputs do not match feature hash"
        )
    values_by_family = dict(feature_rows)
    ordered_families = tuple(sorted(PRODUCTION_FAMILIES))
    frame = pd.DataFrame(
        [values_by_family[family] for family in ordered_families],
        columns=MODEL_FEATURE_COLUMNS,
    )
    replayed = batch.runtime.artifact.predict_log_return(
        frame,
        device=batch.runtime.execution_device,
    )
    if replayed.shape != (len(ordered_families),) or not np.isfinite(replayed).all():
        raise ValueError("promoted deterministic replay returned invalid outputs")
    predictions_by_family = {
        str(item.family_root).upper(): item for item in batch
    }
    radii = dict(batch.runtime.family_radius_log_return)
    if (
        len(predictions_by_family) != len(PRODUCTION_FAMILIES)
        or set(predictions_by_family) != set(PRODUCTION_FAMILIES)
        or set(radii) != set(PRODUCTION_FAMILIES)
    ):
        raise ValueError("promoted deterministic replay batch is not family-complete")
    results: list[PromotedReplayResult] = []
    for index, family in enumerate(ordered_families):
        item = predictions_by_family[family]
        replayed_return = float(replayed[index])
        log_error = abs(replayed_return - float(item.predicted_log_return))
        radius = float(radii[family])
        reference = float(item.reference_price)
        expected_values = (
            reference * math.exp(replayed_return),
            reference * math.exp(replayed_return - radius),
            reference * math.exp(replayed_return + radius),
        )
        recorded_values = (
            float(item.predicted_level),
            float(item.prediction_lower),
            float(item.prediction_upper),
        )
        level_errors = tuple(
            abs(recorded - expected)
            for recorded, expected in zip(recorded_values, expected_values)
        )
        level_tolerance = max(
            1e-9,
            max(abs(value) for value in expected_values)
            * REPLAY_LEVEL_RELATIVE_TOLERANCE,
        )
        if (
            log_error > REPLAY_LOG_RETURN_ABSOLUTE_TOLERANCE
            or max(level_errors) > level_tolerance
        ):
            raise ValueError(
                f"promoted prediction deterministic replay mismatch for {family}"
            )
        results.append(
            PromotedReplayResult(
                family_root=family,
                replayed_log_return=replayed_return,
                log_return_absolute_error=log_error,
                max_level_absolute_error=max(level_errors),
                level_absolute_tolerance=level_tolerance,
            )
        )
    return tuple(results)


def load_promoted_model_runtime(project_root: str | Path) -> PromotedModelRuntime:
    """Load only a manifest that passes the complete production evidence gate."""
    root = Path(project_root).resolve()
    manifest_path = root / "models" / "closing_tape_model.json"
    try:
        manifest_bytes = manifest_path.read_bytes()
    except FileNotFoundError:
        gate = _model_gate(root)
        raise RuntimeError(
            f"closing-tape production model is not promoted: {gate.get('reason')}"
        ) from None
    size = len(manifest_bytes)
    if size <= 0 or size > MAX_MANIFEST_BYTES:
        raise ValueError("production model manifest size is outside the allowed range")
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    gate = _model_gate(
        root,
        manifest_path=manifest_path,
        manifest_bytes=manifest_bytes,
    )
    if not gate.get("passed"):
        raise RuntimeError(f"closing-tape production model is not promoted: {gate.get('reason')}")
    if str(gate.get("manifest_sha256") or "").lower() != manifest_sha256:
        raise ValueError("production model gate did not validate the loaded manifest snapshot")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("production model manifest root must be an object")
    if manifest.get("feature_schema_hash") != MODEL_FEATURE_CONTRACT_HASH:
        raise ValueError("production manifest feature contract changed after gate validation")
    approval_reference = manifest.get("promotion_approval")
    gate_approval = gate.get("promotion_approval")
    if not isinstance(approval_reference, dict) or not isinstance(gate_approval, dict):
        raise ValueError("production manifest approval changed after gate validation")
    approval_contract = str(approval_reference.get("contract_version") or "").strip()
    proposal_hash = str(gate_approval.get("proposal_sha256") or "").strip().lower()
    receipt_hash = str(gate_approval.get("receipt_sha256") or "").strip().lower()
    if (
        not approval_contract
        or not SHA256_PATTERN.fullmatch(proposal_hash)
        or not SHA256_PATTERN.fullmatch(receipt_hash)
        or str(approval_reference.get("proposal_sha256") or "").lower() != proposal_hash
        or str(approval_reference.get("receipt_sha256") or "").lower() != receipt_hash
    ):
        raise ValueError("production approval identity changed after gate validation")

    models_dir = (root / "models").resolve()
    artifact_path = (models_dir / str(manifest["artifact_path"])).resolve()
    artifact_path.relative_to(models_dir)
    expected_hash = str(manifest["artifact_sha256"]).lower()
    artifact_bytes = artifact_path.read_bytes()
    actual_hash = hashlib.sha256(artifact_bytes).hexdigest()
    if actual_hash != expected_hash:
        raise ValueError("production artifact changed after gate validation")
    artifact = load_frozen_model_artifact_bytes(artifact_bytes)
    device = str(manifest.get("execution_device") or "cpu").lower()
    if device == "cuda":
        import torch

        if not torch.cuda.is_available():
            raise RuntimeError("promoted CUDA execution is unavailable; refusing silent CPU fallback")
    deployment = manifest["deployment_calibration"]
    family_radii = tuple(
        sorted(
            (str(row["family_root"]), float(row["radius_log_return"]))
            for row in deployment["family_radii"]
        )
    )
    return _build_promoted_model_runtime(
        version=str(manifest["version"]),
        artifact_sha256=expected_hash,
        execution_device=device,
        inference_batch_rows=int(manifest["inference_batch_rows"]),
        artifact=artifact,
        calibration_method=str(deployment["method"]),
        calibration_evidence_sha256=str(deployment["evidence_sha256"]).lower(),
        interval_alpha=float(deployment["alpha"]),
        interval_target_coverage=float(deployment["target_coverage"]),
        family_radius_log_return=family_radii,
        promotion_approval_contract_version=approval_contract,
        promotion_proposal_sha256=proposal_hash,
        promotion_approval_receipt_sha256=receipt_hash,
    )


def predict_promoted_close(
    surface: pd.DataFrame,
    runtime: PromotedModelRuntime,
    *,
    decision_horizon_minutes: int = 15,
) -> PromotedPredictionBatch:
    """Predict an exact-horizon close only from complete verified family evidence."""
    require_promoted_model_runtime(runtime)
    selected = select_decision_horizon_features(
        surface, minutes_before_close=decision_horizon_minutes
    ).copy()
    expected = set(PRODUCTION_FAMILIES)
    roots = set(selected["family_root"].astype(str)) if not selected.empty else set()
    counts = selected["family_root"].value_counts().to_dict() if not selected.empty else {}
    if roots != expected or any(int(counts.get(family, 0)) != 1 for family in expected):
        raise ValueError("production inference requires exactly one exact-horizon row per family")
    if len(selected) != runtime.inference_batch_rows:
        raise ValueError(
            f"production batch rows {len(selected)} do not match promoted workload "
            f"{runtime.inference_batch_rows}"
        )
    if "capture_integrity_verified" not in selected:
        raise ValueError("production inference requires integrity-verified capture rows")
    integrity_flags = selected["capture_integrity_verified"]
    if (
        not pd.api.types.is_bool_dtype(integrity_flags.dtype)
        or integrity_flags.isna().any()
        or not integrity_flags.all()
    ):
        raise ValueError("production inference requires integrity-verified capture rows")
    references = pd.to_numeric(selected["reference_price"], errors="coerce").to_numpy(float)
    if not np.isfinite(references).all() or (references <= 0).any():
        raise ValueError("production inference requires finite positive reference prices")
    source_hashes = selected["source_sha256"].astype(str).str.lower()
    if not source_hashes.str.fullmatch(r"[0-9a-f]{64}").all() or source_hashes.nunique() != 1:
        raise ValueError("production family rows must share one valid source SHA-256")

    selected = selected.sort_values("family_root").reset_index(drop=True)
    references = pd.to_numeric(selected["reference_price"], errors="coerce").to_numpy(float)
    predicted_returns = runtime.artifact.predict_log_return(
        selected, device=runtime.execution_device
    )
    predicted_levels = references * np.exp(predicted_returns)
    if not np.isfinite(predicted_levels).all() or (predicted_levels <= 0).any():
        raise ValueError("promoted model produced invalid predicted levels")
    radii = dict(runtime.family_radius_log_return)
    if set(radii) != expected or any(not np.isfinite(value) or value < 0 for value in radii.values()):
        raise ValueError("promoted runtime has incomplete calibrated family radii")
    predictions = tuple(
        PromotedPrediction(
            family_root=str(row.family_root),
            reference_price=float(references[index]),
            predicted_log_return=float(predicted_returns[index]),
            predicted_level=float(predicted_levels[index]),
            prediction_lower=float(
                references[index] * np.exp(predicted_returns[index] - radii[str(row.family_root)])
            ),
            prediction_upper=float(
                references[index] * np.exp(predicted_returns[index] + radii[str(row.family_root)])
            ),
            interval_radius_log_return=float(radii[str(row.family_root)]),
            interval_alpha=runtime.interval_alpha,
            interval_target_coverage=runtime.interval_target_coverage,
            calibration_method=runtime.calibration_method,
            calibration_evidence_sha256=runtime.calibration_evidence_sha256,
            feature_available_at_utc=pd.Timestamp(row.feature_available_at_utc).isoformat(),
            source_sha256=str(row.source_sha256).lower(),
            model_version=runtime.version,
            artifact_sha256=runtime.artifact_sha256,
            execution_device=runtime.execution_device,
        )
        for index, row in enumerate(selected.itertuples(index=False))
    )
    model_feature_rows: list[tuple[str, tuple[float, ...]]] = []
    regimes: list[tuple[str, str]] = []
    for row in selected.itertuples(index=False):
        family = str(row.family_root).upper()
        model_values: list[float] = []
        for column in MODEL_FEATURE_COLUMNS:
            number = float(getattr(row, column))
            if not math.isfinite(number):
                raise ValueError(f"production feature {column} is non-finite")
            model_values.append(number)
        model_feature_rows.append((family, tuple(model_values)))
        raw_regime = getattr(row, "volatility_regime", None)
        if raw_regime is None or pd.isna(raw_regime):
            regime = "unavailable"
        else:
            regime = str(raw_regime).strip().lower()
            if regime not in {"calm", "normal", "stressed"}:
                raise ValueError(f"production volatility regime is invalid for {family}")
        regimes.append((family, regime))
    feature_hash = promoted_feature_evidence_sha256(
        predictions,
        model_feature_rows,
    )
    return _build_promoted_prediction_batch(
        predictions,
        runtime,
        feature_hash=feature_hash,
        model_feature_rows_by_family=model_feature_rows,
        volatility_regime_by_family=regimes,
    )


def _utc_iso(value: datetime | str) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        raise ValueError("prediction timestamps must be timezone-aware")
    return timestamp.tz_convert("UTC").isoformat()


def _invalid_promoted_batch(reason: str) -> None:
    raise RuntimeError(f"promoted prediction batch is invalid: {reason}")


def _batch_text(row: Mapping[str, object], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        _invalid_promoted_batch(f"{field} must be a nonempty string")
    return value


def _batch_number(row: Mapping[str, object], field: str) -> float:
    value = row.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _invalid_promoted_batch(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        _invalid_promoted_batch(f"{field} must be finite")
    return number


def _batch_utc_timestamp(row: Mapping[str, object], field: str) -> str:
    raw = _batch_text(row, field)
    try:
        canonical = _utc_iso(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        _invalid_promoted_batch(f"{field} must be a timezone-aware timestamp: {exc}")
    if raw != canonical:
        _invalid_promoted_batch(f"{field} must use canonical UTC representation")
    return canonical


def validate_promoted_close_prediction_batch(
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Fail closed unless persisted rows form one complete immutable batch."""

    batch = tuple(rows)
    expected_families = set(PRODUCTION_FAMILIES)
    if len(batch) != len(expected_families):
        _invalid_promoted_batch("exactly five production-family rows are required")

    normalized: list[dict[str, object]] = []
    for row in batch:
        if not isinstance(row, Mapping):
            _invalid_promoted_batch("every row must be a mapping")
        family_root = _batch_text(row, "family_root")
        trading_date = _batch_text(row, "trading_date")
        try:
            parsed_date = date.fromisoformat(trading_date)
        except ValueError as exc:
            _invalid_promoted_batch(f"trading_date is invalid: {exc}")
        if parsed_date.isoformat() != trading_date:
            _invalid_promoted_batch("trading_date must use canonical ISO format")

        horizon = row.get("decision_horizon_minutes")
        if isinstance(horizon, bool) or not isinstance(horizon, int) or horizon < 1:
            _invalid_promoted_batch("decision_horizon_minutes must be a positive integer")

        hashes: dict[str, str] = {}
        for field in (
            "prediction_key",
            "calibration_evidence_sha256",
            "source_sha256",
            "artifact_sha256",
        ):
            value = _batch_text(row, field)
            if not SHA256_PATTERN.fullmatch(value):
                _invalid_promoted_batch(f"{field} must be a lowercase SHA-256")
            hashes[field] = value

        prediction_mode = _batch_text(row, "prediction_mode")
        if prediction_mode != "tcbbo_promoted":
            _invalid_promoted_batch("prediction_mode must be tcbbo_promoted")
        is_estimate = row.get("is_estimate")
        if isinstance(is_estimate, bool):
            estimate_value = int(is_estimate)
        elif isinstance(is_estimate, int):
            estimate_value = is_estimate
        else:
            _invalid_promoted_batch("is_estimate must be the integer estimate marker")
        if estimate_value != 1:
            _invalid_promoted_batch("promoted outputs must remain labeled estimates")

        execution_device = _batch_text(row, "execution_device")
        if execution_device not in {"cpu", "cuda"}:
            _invalid_promoted_batch("execution_device must be cpu or cuda")

        reference_price = _batch_number(row, "reference_price")
        predicted_log_return = _batch_number(row, "predicted_log_return")
        predicted_level = _batch_number(row, "predicted_level")
        prediction_lower = _batch_number(row, "prediction_lower")
        prediction_upper = _batch_number(row, "prediction_upper")
        interval_radius = _batch_number(row, "interval_radius_log_return")
        interval_alpha = _batch_number(row, "interval_alpha")
        interval_target = _batch_number(row, "interval_target_coverage")
        if (
            reference_price <= 0
            or not 0 < prediction_lower <= predicted_level <= prediction_upper
            or interval_radius < 0
            or not 0 < interval_alpha < 1
            or abs(interval_target - (1.0 - interval_alpha)) > 1e-12
        ):
            _invalid_promoted_batch("numeric prediction interval is invalid")
        try:
            expected_level = reference_price * math.exp(predicted_log_return)
            expected_lower = reference_price * math.exp(
                predicted_log_return - interval_radius
            )
            expected_upper = reference_price * math.exp(
                predicted_log_return + interval_radius
            )
        except OverflowError:
            _invalid_promoted_batch("numeric prediction interval overflows")
        if not all(
            math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
            for actual, expected in (
                (predicted_level, expected_level),
                (prediction_lower, expected_lower),
                (prediction_upper, expected_upper),
            )
        ):
            _invalid_promoted_batch(
                "numeric prediction interval is inconsistent with its log returns"
            )

        feature_time = _batch_utc_timestamp(row, "feature_available_at_utc")
        recorded_time = _batch_utc_timestamp(row, "recorded_at_utc")
        feature_timestamp = pd.Timestamp(feature_time)
        recorded_timestamp = pd.Timestamp(recorded_time)
        if not (
            feature_timestamp
            <= recorded_timestamp
            < feature_timestamp + pd.Timedelta(minutes=horizon)
        ):
            _invalid_promoted_batch(
                "recorded_at_utc must follow feature availability and precede target close"
            )

        normalized.append(
            {
                "prediction_key": hashes["prediction_key"],
                "trading_date": trading_date,
                "session_id": _batch_text(row, "session_id"),
                "family_root": family_root,
                "decision_horizon_minutes": horizon,
                "feature_available_at_utc": feature_time,
                "recorded_at_utc": recorded_time,
                "source_sha256": hashes["source_sha256"],
                "model_version": _batch_text(row, "model_version"),
                "artifact_sha256": hashes["artifact_sha256"],
                "calibration_method": _batch_text(row, "calibration_method"),
                "calibration_evidence_sha256": hashes[
                    "calibration_evidence_sha256"
                ],
                "interval_alpha": interval_alpha,
                "interval_target_coverage": interval_target,
                "execution_device": execution_device,
            }
        )

    families = [str(row["family_root"]) for row in normalized]
    if set(families) != expected_families or len(set(families)) != len(families):
        _invalid_promoted_batch("exactly the five production families are required")

    provenance_fields = (
        "trading_date",
        "session_id",
        "decision_horizon_minutes",
        "feature_available_at_utc",
        "recorded_at_utc",
        "source_sha256",
        "model_version",
        "artifact_sha256",
        "calibration_method",
        "calibration_evidence_sha256",
        "interval_alpha",
        "interval_target_coverage",
        "execution_device",
    )
    provenance_sets = {
        tuple(row[field] for field in provenance_fields) for row in normalized
    }
    if len(provenance_sets) != 1:
        _invalid_promoted_batch("rows do not share one complete provenance set")

    for row in normalized:
        identity = "|".join(
            (
                str(row["model_version"]),
                str(row["artifact_sha256"]),
                str(row["calibration_evidence_sha256"]),
                str(row["source_sha256"]),
                str(row["session_id"]),
                str(row["family_root"]),
                str(row["trading_date"]),
                str(row["decision_horizon_minutes"]),
                str(row["feature_available_at_utc"]),
            )
        )
        expected_key = hashlib.sha256(identity.encode("utf-8")).hexdigest()
        if row["prediction_key"] != expected_key:
            _invalid_promoted_batch(
                f"prediction_key does not match row identity for {row['family_root']}"
            )


def validate_promoted_close_prediction_batch_against_runtime(
    rows: Sequence[Mapping[str, object]],
    runtime: PromotedModelRuntime,
) -> None:
    """Bind persisted rows to the currently loader-verified promoted runtime."""

    require_promoted_model_runtime(runtime)
    validate_promoted_close_prediction_batch(rows)
    if len(rows) != runtime.inference_batch_rows:
        _invalid_promoted_batch(
            "persisted rows do not match the promoted runtime batch size"
        )
    expected_radii = dict(runtime.family_radius_log_return)
    for row in rows:
        family = _batch_text(row, "family_root")
        if (
            _batch_text(row, "model_version") != runtime.version
            or _batch_text(row, "artifact_sha256") != runtime.artifact_sha256
            or _batch_text(row, "execution_device") != runtime.execution_device
            or _batch_text(row, "calibration_method") != runtime.calibration_method
            or _batch_text(row, "calibration_evidence_sha256")
            != runtime.calibration_evidence_sha256
            or not math.isclose(
                _batch_number(row, "interval_alpha"),
                runtime.interval_alpha,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or not math.isclose(
                _batch_number(row, "interval_target_coverage"),
                runtime.interval_target_coverage,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or family not in expected_radii
            or not math.isclose(
                _batch_number(row, "interval_radius_log_return"),
                expected_radii[family],
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            _invalid_promoted_batch(
                f"persisted {family} row does not match the promoted runtime"
            )


def _require_promoted_prediction_schema(connection: sqlite3.Connection) -> None:
    table_info = connection.execute(
        "PRAGMA table_info(promoted_close_predictions)"
    ).fetchall()
    columns = tuple(str(row[1]) for row in table_info)
    primary_key = tuple(
        str(row[1])
        for row in sorted(
            (row for row in table_info if int(row[5]) > 0),
            key=lambda row: int(row[5]),
        )
    )
    not_null = frozenset(str(row[1]) for row in table_info if bool(row[3]))
    unique_constraints: set[tuple[str, ...]] = set()
    invalid_partial_unique = False
    for index in connection.execute(
        "PRAGMA index_list(promoted_close_predictions)"
    ).fetchall():
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
    table_row = connection.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name='promoted_close_predictions'"
    ).fetchone()
    table_sql = _normalized_schema_sql(str(table_row[0] or "")) if table_row else ""
    trigger_rows = connection.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='trigger' AND tbl_name='promoted_close_predictions'"
    ).fetchall()
    trigger_sql = {
        str(row[0]): _normalized_schema_sql(str(row[1] or ""))
        for row in trigger_rows
    }
    expected_unique = {
        (
            "model_version",
            "artifact_sha256",
            "family_root",
            "trading_date",
            "decision_horizon_minutes",
        )
    }
    if (
        columns != PROMOTED_PREDICTION_COLUMNS
        or primary_key != ("prediction_key",)
        or not_null != frozenset(PROMOTED_PREDICTION_COLUMNS)
        or unique_constraints != expected_unique
        or invalid_partial_unique
        or table_sql != _PROMOTED_TABLE_SQL
        or trigger_sql.keys() != _PROMOTED_TRIGGER_SQL.keys()
        or any(
            trigger_sql[name] != expected_sql
            for name, expected_sql in _PROMOTED_TRIGGER_SQL.items()
        )
    ):
        raise RuntimeError(
            "promoted prediction ledger schema is incompatible; immutable evidence "
            "requires an explicit reviewed migration"
        )


def record_promoted_close_predictions(
    market_db_path: str | Path,
    predictions: PromotedPredictionBatch,
    *,
    runtime: PromotedModelRuntime,
    trading_day: date,
    session_id: str,
    decision_horizon_minutes: int = 15,
    recorded_at_utc: datetime | None = None,
) -> tuple[str, ...]:
    """Append one loader-authorized, immutable, pre-close five-family batch."""
    require_promoted_model_runtime(runtime)
    if (
        type(predictions) is not PromotedPredictionBatch
        or getattr(predictions, "_authority_token", None) is not _PROMOTED_BATCH_TOKEN
    ):
        raise ValueError(
            "promoted prediction writes require a loader-authorized prediction batch"
        )
    require_promoted_model_runtime(predictions.runtime)
    if decision_horizon_minutes < 1 or not session_id:
        raise ValueError("prediction session and horizon are required")
    expected = set(PRODUCTION_FAMILIES)
    if len(predictions) != len(expected) or {item.family_root for item in predictions} != expected:
        raise ValueError("promoted prediction batch must contain exactly the production families")
    expected_radii = dict(runtime.family_radius_log_return)
    for item in predictions:
        if (
            item.model_version != runtime.version
            or item.artifact_sha256 != runtime.artifact_sha256
            or item.execution_device != runtime.execution_device
            or item.calibration_method != runtime.calibration_method
            or item.calibration_evidence_sha256
            != runtime.calibration_evidence_sha256
            or not math.isclose(
                float(item.interval_alpha),
                float(runtime.interval_alpha),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or not math.isclose(
                float(item.interval_target_coverage),
                float(runtime.interval_target_coverage),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
            or item.family_root not in expected_radii
            or not math.isclose(
                float(item.interval_radius_log_return),
                float(expected_radii[item.family_root]),
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise ValueError(
                f"promoted prediction identity does not match its runtime: {item.family_root}"
            )
    source_hashes = {item.source_sha256 for item in predictions}
    versions = {item.model_version for item in predictions}
    artifacts = {item.artifact_sha256 for item in predictions}
    devices = {item.execution_device for item in predictions}
    calibration_methods = {item.calibration_method for item in predictions}
    calibration_evidence_hashes = {
        item.calibration_evidence_sha256 for item in predictions
    }
    interval_alphas = {item.interval_alpha for item in predictions}
    interval_targets = {item.interval_target_coverage for item in predictions}
    feature_times = {_utc_iso(item.feature_available_at_utc) for item in predictions}
    if any(
        len(values) != 1 for values in (
            source_hashes, versions, artifacts, devices, calibration_methods,
            calibration_evidence_hashes, interval_alphas, interval_targets,
            feature_times,
        )
    ):
        raise ValueError("promoted prediction batch mixes provenance or feature times")
    if any(not item.is_estimate or item.prediction_mode != "tcbbo_promoted" for item in predictions):
        raise ValueError("promoted outputs must remain explicitly labeled estimates")
    if any(
        not pd.Series([item.calibration_evidence_sha256]).str.fullmatch(
            r"[0-9a-f]{64}"
        ).iloc[0]
        for item in predictions
    ):
        raise ValueError("promoted prediction calibration evidence hash is invalid")
    if any(
        not np.isfinite(
            [item.prediction_lower, item.predicted_level, item.prediction_upper,
             item.interval_radius_log_return, item.interval_alpha,
             item.interval_target_coverage]
        ).all()
        or not 0 < item.prediction_lower <= item.predicted_level <= item.prediction_upper
        or item.interval_radius_log_return < 0
        or not 0 < item.interval_alpha < 1
        or abs(item.interval_target_coverage - (1.0 - item.interval_alpha)) > 1e-12
        for item in predictions
    ):
        raise ValueError("promoted prediction calibrated interval is invalid")
    now = recorded_at_utc or datetime.now(UTC)
    recorded_iso = _utc_iso(now)
    feature_time = pd.Timestamp(next(iter(feature_times)))
    recorded_time = pd.Timestamp(recorded_iso)
    if recorded_time < feature_time:
        raise ValueError("promoted close prediction cannot be recorded before its features")
    if recorded_time >= feature_time + pd.Timedelta(minutes=decision_horizon_minutes):
        raise ValueError("promoted close prediction must be recorded before its target close")

    rows: list[dict[str, object]] = []
    for item in sorted(predictions, key=lambda value: value.family_root):
        identity = "|".join(
            (
                item.model_version, item.artifact_sha256,
                item.calibration_evidence_sha256, item.source_sha256,
                session_id, item.family_root, trading_day.isoformat(),
                str(decision_horizon_minutes), _utc_iso(item.feature_available_at_utc),
            )
        )
        rows.append(
            {
                "prediction_key": hashlib.sha256(identity.encode("utf-8")).hexdigest(),
                "trading_date": trading_day.isoformat(), "session_id": session_id,
                "family_root": item.family_root,
                "decision_horizon_minutes": decision_horizon_minutes,
                "feature_available_at_utc": _utc_iso(item.feature_available_at_utc),
                "recorded_at_utc": recorded_iso, "reference_price": item.reference_price,
                "predicted_log_return": item.predicted_log_return,
                "predicted_level": item.predicted_level,
                "prediction_lower": item.prediction_lower,
                "prediction_upper": item.prediction_upper,
                "interval_radius_log_return": item.interval_radius_log_return,
                "interval_alpha": item.interval_alpha,
                "interval_target_coverage": item.interval_target_coverage,
                "calibration_method": item.calibration_method,
                "calibration_evidence_sha256": item.calibration_evidence_sha256,
                "source_sha256": item.source_sha256,
                "model_version": item.model_version, "artifact_sha256": item.artifact_sha256,
                "execution_device": item.execution_device,
                "prediction_mode": item.prediction_mode, "is_estimate": 1,
            }
        )
    path = Path(market_db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = tuple(rows[0])
    if columns != PROMOTED_PREDICTION_COLUMNS:
        raise RuntimeError("promoted prediction row contract does not match the ledger schema")
    compare_columns = tuple(column for column in columns if column != "recorded_at_utc")
    with sqlite3.connect(path, timeout=10.0) as connection:
        connection.row_factory = sqlite3.Row
        connection.executescript(PROMOTED_PREDICTION_SCHEMA)
        _require_promoted_prediction_schema(connection)
        for row in rows:
            existing = connection.execute(
                "SELECT * FROM promoted_close_predictions WHERE prediction_key=?",
                (row["prediction_key"],),
            ).fetchone()
            if existing is not None:
                if any(existing[column] != row[column] for column in compare_columns):
                    raise ValueError(f"conflicting immutable promoted prediction: {row['family_root']}")
                continue
            placeholders = ",".join("?" for _ in columns)
            connection.execute(
                f"INSERT INTO promoted_close_predictions ({','.join(columns)}) VALUES ({placeholders})",
                tuple(row[column] for column in columns),
            )
    return tuple(str(row["prediction_key"]) for row in rows)


def load_promoted_close_predictions(
    market_db_path: str | Path,
    *,
    trading_day: date | None = None,
) -> list[dict[str, object]]:
    """Read immutable promoted estimates without initializing or mutating storage."""
    path = Path(market_db_path)
    if not path.is_file():
        return []
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10.0) as connection:
        connection.row_factory = sqlite3.Row
        present = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='promoted_close_predictions'"
        ).fetchone()
        if present is None:
            return []
        _require_promoted_prediction_schema(connection)
        if trading_day is None:
            latest = connection.execute(
                "SELECT MAX(trading_date) FROM promoted_close_predictions"
            ).fetchone()[0]
            if latest is None:
                return []
            value = str(latest)
        else:
            value = trading_day.isoformat()
        rows = connection.execute(
            """
            SELECT * FROM promoted_close_predictions
            WHERE trading_date=? ORDER BY family_root
            """,
            (value,),
        ).fetchall()
    result = [dict(row) for row in rows]
    if result:
        validate_promoted_close_prediction_batch(result)
    for row in result:
        row["is_estimate"] = bool(row["is_estimate"])
    return result
