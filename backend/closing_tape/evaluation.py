from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from typing import Any, Iterable

import pandas as pd

from .dataset import PRODUCTION_FAMILIES
from .surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH
from .surface_artifact import (
    ReplayVerifiedResearchSurfaceArtifact,
    dataframe_semantic_sha256,
    require_replay_verified_surface_identity,
)
from .training import prepare_close_training_dataset


_GUARDED_EVALUATION_TOKEN = object()


@dataclass(frozen=True, init=False)
class GuardedModelEvaluationResult:
    trained: bool
    device_requested: str
    device_effective: str
    model_feature_contract_hash: str
    model_feature_columns: tuple[str, ...]
    surface_artifact_sha256: str | None
    surface_replay_receipt_sha256: str | None
    readiness: dict[str, object]
    evaluation: dict[str, object] | None
    reasons: tuple[str, ...]
    evaluation_report: object | None
    labeled_training_frame: pd.DataFrame | None
    replay_verified_surface: ReplayVerifiedResearchSurfaceArtifact | None
    labeled_training_frame_sha256: str | None
    evaluation_report_sha256: str | None
    _verification_token: object = field(repr=False, compare=False)

    def to_dict(self) -> dict[str, object]:
        return {
            "trained": self.trained,
            "device_requested": self.device_requested,
            "device_effective": self.device_effective,
            "model_feature_contract_hash": self.model_feature_contract_hash,
            "model_feature_columns": self.model_feature_columns,
            "surface_artifact_sha256": self.surface_artifact_sha256,
            "surface_replay_receipt_sha256": self.surface_replay_receipt_sha256,
            "labeled_training_frame_sha256": self.labeled_training_frame_sha256,
            "evaluation_report_sha256": self.evaluation_report_sha256,
            "readiness": self.readiness,
            "evaluation": self.evaluation,
            "reasons": self.reasons,
        }


def _canonical_evaluation_report_value(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): _canonical_evaluation_report_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_canonical_evaluation_report_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            return {"nonfinite_float": "nan"}
        return {
            "nonfinite_float": (
                "positive_infinity" if value > 0 else "negative_infinity"
            )
        }
    return value


def _evaluation_report_sha256(report: object) -> str:
    try:
        payload = _canonical_evaluation_report_value(
            report.to_dict()  # type: ignore[attr-defined]
        )
        serialized = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("evaluation report cannot be hashed deterministically") from exc
    return hashlib.sha256(serialized).hexdigest()


def _build_guarded_model_evaluation_result(
    *,
    trained: bool,
    device_requested: str,
    device_effective: str,
    surface_artifact_sha256: str | None,
    surface_replay_receipt_sha256: str | None,
    readiness: dict[str, object],
    evaluation: dict[str, object] | None,
    reasons: tuple[str, ...],
    evaluation_report: object | None,
    labeled_training_frame: pd.DataFrame | None,
    replay_verified_surface: ReplayVerifiedResearchSurfaceArtifact | None,
) -> GuardedModelEvaluationResult:
    result = object.__new__(GuardedModelEvaluationResult)
    values = {
        "trained": bool(trained),
        "device_requested": device_requested,
        "device_effective": device_effective,
        "model_feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "model_feature_columns": MODEL_FEATURE_COLUMNS,
        "surface_artifact_sha256": surface_artifact_sha256,
        "surface_replay_receipt_sha256": surface_replay_receipt_sha256,
        "readiness": readiness,
        "evaluation": evaluation,
        "reasons": tuple(reasons),
        "evaluation_report": evaluation_report,
        "labeled_training_frame": labeled_training_frame,
        "replay_verified_surface": replay_verified_surface,
        "labeled_training_frame_sha256": (
            dataframe_semantic_sha256(labeled_training_frame)
            if labeled_training_frame is not None
            else None
        ),
        "evaluation_report_sha256": (
            _evaluation_report_sha256(evaluation_report)
            if evaluation_report is not None
            else None
        ),
        "_verification_token": _GUARDED_EVALUATION_TOKEN,
    }
    for name, value in values.items():
        object.__setattr__(result, name, value)
    return result


def require_guarded_model_evaluation_result(
    result: GuardedModelEvaluationResult,
) -> None:
    """Reject fabricated or post-evaluation-mutated model evidence."""

    if (
        type(result) is not GuardedModelEvaluationResult
        or getattr(result, "_verification_token", None) is not _GUARDED_EVALUATION_TOKEN
    ):
        raise ValueError("candidate packaging requires a loader-issued guarded evaluation result")
    if (
        result.model_feature_contract_hash != MODEL_FEATURE_CONTRACT_HASH
        or tuple(result.model_feature_columns) != MODEL_FEATURE_COLUMNS
    ):
        raise ValueError("guarded evaluation feature contract changed after evaluation")
    if result.replay_verified_surface is None:
        if (
            result.surface_artifact_sha256 is not None
            or result.surface_replay_receipt_sha256 is not None
        ):
            raise ValueError("guarded evaluation has inconsistent surface replay identity")
    else:
        identity = require_replay_verified_surface_identity(
            result.replay_verified_surface
        )
        if (
            result.surface_artifact_sha256 != identity.surface_artifact_sha256
            or result.surface_replay_receipt_sha256
            != identity.surface_replay_receipt_sha256
        ):
            raise ValueError("guarded evaluation surface replay identity changed")
    if not result.trained:
        if any(
            value is not None
            for value in (
                result.evaluation,
                result.evaluation_report,
                result.labeled_training_frame,
                result.evaluation_report_sha256,
                result.labeled_training_frame_sha256,
            )
        ):
            raise ValueError("untrained guarded evaluation contains trained evidence")
        return
    from .torch_research import TorchWalkForwardReport

    if type(result.evaluation_report) is not TorchWalkForwardReport:
        raise ValueError("guarded evaluation report is not authoritative")
    if not isinstance(result.labeled_training_frame, pd.DataFrame):
        raise ValueError("guarded evaluation labeled frame is missing")
    if (
        result.labeled_training_frame_sha256
        != dataframe_semantic_sha256(result.labeled_training_frame)
    ):
        raise ValueError("guarded evaluation labeled frame changed after evaluation")
    report_hash = _evaluation_report_sha256(result.evaluation_report)
    if result.evaluation_report_sha256 != report_hash:
        raise ValueError("guarded evaluation report changed after evaluation")
    if result.evaluation != result.evaluation_report.to_dict():
        raise ValueError("guarded evaluation summary does not match its report")


def _evaluate_torch(frame: pd.DataFrame, **kwargs: Any):
    # Keep torch/CUDA imports behind the evidence gate.
    from .torch_research import evaluate_torch_walk_forward

    return evaluate_torch_walk_forward(frame, **kwargs)


def evaluate_close_models_if_ready(
    surface: pd.DataFrame | ReplayVerifiedResearchSurfaceArtifact,
    closes: pd.DataFrame,
    *,
    expected_families: Iterable[str] = PRODUCTION_FAMILIES,
    minutes_before_close: int = 15,
    minimum_sessions: int = 60,
    minimum_family_sessions: int = 10,
    min_train_sessions: int = 40,
    test_sessions: int = 1,
    validation_sessions: int = 5,
    device: str = "auto",
    max_epochs: int = 100,
    patience: int = 12,
    batch_size: int = 2048,
) -> GuardedModelEvaluationResult:
    """Run identical ridge/CUDA walk-forward evaluation only after readiness passes."""
    requested_device = str(device).lower()
    if requested_device not in {"auto", "cpu", "cuda"}:
        raise ValueError("device must be auto, cpu, or cuda")
    replay_surface = (
        surface if isinstance(surface, ReplayVerifiedResearchSurfaceArtifact) else None
    )
    if replay_surface is not None:
        surface_identity = require_replay_verified_surface_identity(replay_surface)
        surface_frame = replay_surface.frame
    elif isinstance(surface, pd.DataFrame):
        surface_identity = None
        surface_frame = surface
    else:
        raise ValueError("surface must be a DataFrame or replay-verified artifact")
    if requested_device == "cuda" and surface_identity is None:
        raise ValueError("CUDA evaluation requires a replay-verified surface artifact")
    effective_device = (
        "cpu"
        if requested_device == "auto" and surface_identity is None
        else requested_device
    )
    labeled, readiness = prepare_close_training_dataset(
        surface_frame,
        closes,
        minutes_before_close=minutes_before_close,
        minimum_sessions=minimum_sessions,
        minimum_family_sessions=minimum_family_sessions,
        expected_families=expected_families,
    )
    if not readiness.ready:
        return _build_guarded_model_evaluation_result(
            trained=False,
            device_requested=requested_device,
            device_effective=effective_device,
            surface_artifact_sha256=(
                surface_identity.surface_artifact_sha256
                if surface_identity is not None else None
            ),
            surface_replay_receipt_sha256=(
                surface_identity.surface_replay_receipt_sha256
                if surface_identity is not None else None
            ),
            readiness=readiness.to_dict(), evaluation=None,
            reasons=readiness.reasons, evaluation_report=None,
            labeled_training_frame=None,
            replay_verified_surface=replay_surface,
        )
    if min_train_sessions + validation_sessions >= minimum_sessions:
        raise ValueError(
            "min_train_sessions plus validation_sessions must leave held-out sessions"
        )
    report = _evaluate_torch(
        labeled,
        feature_columns=MODEL_FEATURE_COLUMNS,
        horizon_minutes=minutes_before_close,
        min_train_sessions=min_train_sessions,
        test_sessions=test_sessions,
        validation_sessions=validation_sessions,
        device=effective_device,
        max_epochs=max_epochs,
        patience=patience,
        batch_size=batch_size,
        minimum_complete_sessions=minimum_sessions,
        minimum_sessions_per_family=minimum_family_sessions,
    )
    from .torch_research import TorchWalkForwardReport

    if type(report) is not TorchWalkForwardReport:
        raise ValueError("torch evaluator returned a non-authoritative report")
    return _build_guarded_model_evaluation_result(
        trained=True,
        device_requested=requested_device,
        device_effective=effective_device,
        surface_artifact_sha256=(
            surface_identity.surface_artifact_sha256
            if surface_identity is not None else None
        ),
        surface_replay_receipt_sha256=(
            surface_identity.surface_replay_receipt_sha256
            if surface_identity is not None else None
        ),
        readiness=readiness.to_dict(), evaluation=report.to_dict(),
        reasons=tuple(report.promotion_reasons), evaluation_report=report,
        labeled_training_frame=labeled.copy(),
        replay_verified_surface=replay_surface,
    )
