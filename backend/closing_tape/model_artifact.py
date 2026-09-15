from __future__ import annotations

import json
import hashlib
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH


MODEL_ARTIFACT_FORMAT = "marketpin-closing-mlp-json-v3"
MODEL_MANIFEST_CONTRACT_VERSION = "closing-tape-model-v6"
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True)
class FrozenModelBuild:
    json_text: str
    sha256: str
    training_rows: int
    epochs: int
    training_device: str
    source_sha256s: tuple[str, ...]
    label_source_artifact_sha256s: tuple[str, ...]
    surface_artifact_sha256: str
    surface_replay_receipt_sha256: str

    def write(self, path: str | Path) -> Path:
        destination = Path(path).resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        temporary.write_text(self.json_text, encoding="utf-8", newline="\n")
        temporary.replace(destination)
        loaded = load_frozen_model_artifact(destination)
        if loaded.feature_columns != MODEL_FEATURE_COLUMNS:
            raise ValueError("written artifact failed feature-contract verification")
        if loaded.source_sha256s != self.source_sha256s:
            raise ValueError("written artifact failed DBN provenance verification")
        if (
            loaded.label_source_artifact_sha256s
            != self.label_source_artifact_sha256s
        ):
            raise ValueError("written artifact failed close provenance verification")
        if (
            loaded.surface_artifact_sha256 != self.surface_artifact_sha256
            or loaded.surface_replay_receipt_sha256
            != self.surface_replay_receipt_sha256
        ):
            raise ValueError("written artifact failed surface replay provenance verification")
        actual = hashlib.sha256(destination.read_bytes()).hexdigest()
        if actual != self.sha256:
            raise ValueError("written artifact SHA-256 does not match the build")
        return destination


@dataclass(frozen=True)
class FrozenInferenceBenchmark:
    rows: int
    repeats: int
    cpu_median_milliseconds: float
    cuda_available: bool
    cuda_median_milliseconds: float | None
    cuda_speedup_vs_cpu: float | None
    max_abs_prediction_difference: float | None
    prediction_tolerance: float
    numerically_equivalent: bool | None


@dataclass(frozen=True)
class FrozenModelArtifact:
    feature_columns: tuple[str, ...]
    hidden_units: tuple[int, ...]
    imputer_median: np.ndarray
    scaler_mean: np.ndarray
    scaler_scale: np.ndarray
    target_mean: float
    target_scale: float
    training_rows: int
    training_epochs: int
    training_seed: int
    training_device: str
    source_sha256s: tuple[str, ...]
    label_source_artifact_sha256s: tuple[str, ...]
    surface_artifact_sha256: str
    surface_replay_receipt_sha256: str
    layers: tuple[tuple[np.ndarray, np.ndarray], ...]

    def predict_log_return(self, frame: pd.DataFrame, *, device: str = "cpu") -> np.ndarray:
        """Predict from the declared feature order; never infer order from the caller."""
        missing = [column for column in self.feature_columns if column not in frame.columns]
        if missing:
            raise ValueError(f"model input columns missing: {', '.join(missing)}")
        values = frame[list(self.feature_columns)].apply(pd.to_numeric, errors="coerce").to_numpy(
            dtype=np.float32
        )
        if values.ndim != 2:
            raise ValueError("model input must be a two-dimensional table")
        values = np.where(np.isfinite(values), values, self.imputer_median)
        values = (values - self.scaler_mean) / self.scaler_scale

        import torch

        selected = device.lower()
        if selected not in {"cpu", "cuda"}:
            raise ValueError("device must be cpu or cuda")
        if selected == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA was requested but is unavailable")
        tensor = torch.from_numpy(values.astype(np.float32, copy=False)).to(selected)
        with torch.no_grad():
            for index, (weight, bias) in enumerate(self.layers):
                tensor = torch.nn.functional.linear(
                    tensor,
                    torch.from_numpy(weight).to(selected),
                    torch.from_numpy(bias).to(selected),
                )
                if index < len(self.layers) - 1:
                    tensor = torch.nn.functional.gelu(tensor)
            result = tensor.squeeze(-1).cpu().numpy().astype(np.float64)
        result = result * self.target_scale + self.target_mean
        if not np.isfinite(result).all():
            raise ValueError("frozen model produced a non-finite prediction")
        return result


def _finite_vector(payload: dict[str, Any], key: str, size: int) -> np.ndarray:
    value = np.asarray(payload.get(key), dtype=np.float32)
    if value.shape != (size,) or not np.isfinite(value).all():
        raise ValueError(f"artifact {key} must contain {size} finite values")
    return value


def _canonical_sha256s(payload: dict[str, Any], key: str) -> tuple[str, ...]:
    raw = payload.get(key)
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"artifact training {key} must be a non-empty list")
    values = tuple(str(value).lower() for value in raw)
    if values != tuple(sorted(set(values))) or any(
        not re.fullmatch(r"[0-9a-f]{64}", value) for value in values
    ):
        raise ValueError(
            f"artifact training {key} must contain sorted unique SHA-256 values"
        )
    return values


def _canonical_sha256(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"artifact training {key} must be a SHA-256 value")
    return value


def parse_frozen_model_artifact(payload: object) -> FrozenModelArtifact:
    if not isinstance(payload, dict):
        raise ValueError("model artifact root must be an object")
    if payload.get("artifact_format") != MODEL_ARTIFACT_FORMAT:
        raise ValueError("unsupported model artifact format")
    if payload.get("feature_contract_hash") != MODEL_FEATURE_CONTRACT_HASH:
        raise ValueError("artifact feature contract does not match the running app")
    features = tuple(str(value) for value in payload.get("feature_columns") or ())
    if features != MODEL_FEATURE_COLUMNS:
        raise ValueError("artifact feature columns or order do not match the running app")
    feature_count = len(features)
    preprocessing = payload.get("preprocessing")
    target = payload.get("target")
    training = payload.get("training")
    if (
        not isinstance(preprocessing, dict)
        or not isinstance(target, dict)
        or not isinstance(training, dict)
    ):
        raise ValueError("artifact preprocessing, target, and training metadata are required")
    median = _finite_vector(preprocessing, "imputer_median", feature_count)
    mean = _finite_vector(preprocessing, "scaler_mean", feature_count)
    scale = _finite_vector(preprocessing, "scaler_scale", feature_count)
    if (scale <= 0).any():
        raise ValueError("artifact scaler_scale values must be positive")
    target_mean = float(target.get("mean"))
    target_scale = float(target.get("scale"))
    if not np.isfinite([target_mean, target_scale]).all() or target_scale <= 0:
        raise ValueError("artifact target mean/scale must be finite and scale positive")
    try:
        training_rows = int(training.get("rows"))
        training_epochs = int(training.get("epochs"))
        training_seed = int(training.get("seed"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("artifact training counters are invalid") from exc
    training_device = str(training.get("device") or "").lower()
    if training_rows < 1 or training_epochs < 1 or training_device not in {"cpu", "cuda"}:
        raise ValueError("artifact training rows, epochs, or device are invalid")
    source_sha256s = _canonical_sha256s(training, "source_sha256s")
    label_source_artifact_sha256s = _canonical_sha256s(
        training, "label_source_artifact_sha256s"
    )
    surface_artifact_sha256 = _canonical_sha256(
        training, "surface_artifact_sha256"
    )
    surface_replay_receipt_sha256 = _canonical_sha256(
        training, "surface_replay_receipt_sha256"
    )

    raw_layers = payload.get("layers")
    if not isinstance(raw_layers, list) or not raw_layers:
        raise ValueError("artifact must contain at least one linear layer")
    layers: list[tuple[np.ndarray, np.ndarray]] = []
    input_size = feature_count
    hidden_units: list[int] = []
    for index, layer in enumerate(raw_layers):
        if not isinstance(layer, dict):
            raise ValueError("artifact layer must be an object")
        weight = np.asarray(layer.get("weight"), dtype=np.float32)
        bias = np.asarray(layer.get("bias"), dtype=np.float32)
        if weight.ndim != 2 or weight.shape[1] != input_size:
            raise ValueError(f"artifact layer {index} input dimension is invalid")
        if bias.shape != (weight.shape[0],):
            raise ValueError(f"artifact layer {index} bias dimension is invalid")
        if not np.isfinite(weight).all() or not np.isfinite(bias).all():
            raise ValueError(f"artifact layer {index} contains non-finite weights")
        is_output = index == len(raw_layers) - 1
        if is_output and weight.shape[0] != 1:
            raise ValueError("artifact output layer must have one output")
        if not is_output:
            hidden_units.append(int(weight.shape[0]))
        layers.append((weight, bias))
        input_size = int(weight.shape[0])
    return FrozenModelArtifact(
        feature_columns=features,
        hidden_units=tuple(hidden_units),
        imputer_median=median,
        scaler_mean=mean,
        scaler_scale=scale,
        target_mean=target_mean,
        target_scale=target_scale,
        training_rows=training_rows,
        training_epochs=training_epochs,
        training_seed=training_seed,
        training_device=training_device,
        source_sha256s=source_sha256s,
        label_source_artifact_sha256s=label_source_artifact_sha256s,
        surface_artifact_sha256=surface_artifact_sha256,
        surface_replay_receipt_sha256=surface_replay_receipt_sha256,
        layers=tuple(layers),
    )


def load_frozen_model_artifact_bytes(artifact_bytes: bytes) -> FrozenModelArtifact:
    """Parse one bounded immutable artifact byte snapshot."""
    if not isinstance(artifact_bytes, bytes):
        raise TypeError("model artifact snapshot must be bytes")
    size = len(artifact_bytes)
    if size <= 0 or size > MAX_ARTIFACT_BYTES:
        raise ValueError(f"model artifact size {size} is outside the allowed range")
    try:
        payload = json.loads(artifact_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("model artifact is not valid UTF-8 JSON") from exc
    return parse_frozen_model_artifact(payload)


def load_frozen_model_artifact(path: str | Path) -> FrozenModelArtifact:
    """Read once, then validate and parse the same immutable byte snapshot."""
    return load_frozen_model_artifact_bytes(Path(path).read_bytes())


def fit_promoted_frozen_model(
    frame: pd.DataFrame,
    evaluation_report: object,
    *,
    surface_artifact_sha256: str,
    surface_replay_receipt_sha256: str,
    device: str | None = None,
    seed: int = 17,
    learning_rate: float = 1e-3,
) -> FrozenModelBuild:
    """Final-fit a deterministic artifact only after walk-forward promotion.

    This packages a future-facing model from all currently labeled evidence.
    It does not create or enable a production manifest.
    """
    if not bool(getattr(evaluation_report, "promoted", False)):
        raise ValueError("walk-forward evaluation did not authorize candidate packaging")
    surface_artifact_hash = str(surface_artifact_sha256 or "").lower()
    surface_replay_hash = str(surface_replay_receipt_sha256 or "").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", surface_artifact_hash) or not re.fullmatch(
        r"[0-9a-f]{64}", surface_replay_hash
    ):
        raise ValueError("final-fit surface replay provenance is missing or invalid")
    if tuple(getattr(evaluation_report, "features", ())) != MODEL_FEATURE_COLUMNS:
        raise ValueError("evaluation feature contract does not match the running app")
    hidden_units = tuple(int(value) for value in getattr(evaluation_report, "hidden_units", ()))
    if not hidden_units or any(value < 1 for value in hidden_units):
        raise ValueError("evaluation report has no valid hidden-unit architecture")
    folds = tuple(getattr(evaluation_report, "folds", ()))
    fold_epochs = [int(getattr(fold, "epochs_trained", 0)) for fold in folds]
    if not fold_epochs or any(value < 1 for value in fold_epochs):
        raise ValueError("evaluation report has no valid early-stopping epoch evidence")
    epochs = max(1, int(round(float(np.median(fold_epochs)))))
    missing = [
        column
        for column in (
            *MODEL_FEATURE_COLUMNS,
            "target_log_return",
            "source_sha256",
            "close_source_artifact_sha256",
        )
        if column not in frame
    ]
    if missing:
        raise ValueError(f"final-fit columns missing: {', '.join(missing)}")
    frame_label_hashes = {
        str(value).lower() for value in frame["close_source_artifact_sha256"].unique()
    }
    report_label_hashes = {
        str(value).lower()
        for value in getattr(evaluation_report, "label_source_artifact_sha256s", ())
    }
    if (
        not frame_label_hashes
        or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in frame_label_hashes)
        or frame_label_hashes != report_label_hashes
    ):
        raise ValueError(
            "final-fit verified-close artifact hashes do not match walk-forward evidence"
        )
    frame_source_hashes = {
        str(value).lower() for value in frame["source_sha256"].unique()
    }
    report_source_hashes = {
        str(value).lower()
        for value in getattr(evaluation_report, "source_sha256s", ())
    }
    if (
        not frame_source_hashes
        or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in frame_source_hashes)
        or frame_source_hashes != report_source_hashes
    ):
        raise ValueError("final-fit DBN source hashes do not match walk-forward evidence")
    raw = frame[list(MODEL_FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce").to_numpy(
        dtype=np.float64
    )
    target = pd.to_numeric(frame["target_log_return"], errors="coerce").to_numpy(dtype=np.float64)
    if not len(raw) or not np.isfinite(target).all():
        raise ValueError("final-fit targets must be non-empty and finite")
    with np.errstate(all="ignore"):
        median = np.nanmedian(np.where(np.isfinite(raw), raw, np.nan), axis=0)
    if not np.isfinite(median).all():
        raise ValueError("every final-fit feature must have at least one finite value")
    values = np.where(np.isfinite(raw), raw, median)
    mean = np.mean(values, axis=0)
    scale = np.std(values, axis=0)
    scale = np.where(np.isfinite(scale) & (scale > 1e-12), scale, 1.0)
    normalized = ((values - mean) / scale).astype(np.float32)
    target_mean = float(np.mean(target))
    target_scale = float(np.std(target))
    if not np.isfinite(target_scale) or target_scale < 1e-12:
        target_scale = 1.0
    normalized_target = ((target - target_mean) / target_scale).astype(np.float32)

    import torch

    selected = (device or str(getattr(evaluation_report, "device", "cpu"))).lower()
    if selected not in {"cpu", "cuda"}:
        raise ValueError("device must be cpu or cuda")
    if selected == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but is unavailable")
    torch.manual_seed(seed)
    if selected == "cuda":
        torch.cuda.manual_seed_all(seed)
    dimensions = (len(MODEL_FEATURE_COLUMNS), *hidden_units, 1)
    modules: list[torch.nn.Module] = []
    for index in range(len(dimensions) - 1):
        modules.append(torch.nn.Linear(dimensions[index], dimensions[index + 1]))
        if index < len(dimensions) - 2:
            modules.append(torch.nn.GELU())
    model = torch.nn.Sequential(*modules).to(selected)
    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate, weight_decay=1e-4)
    x_tensor = torch.from_numpy(normalized).to(selected)
    y_tensor = torch.from_numpy(normalized_target[:, None]).to(selected)
    for _epoch in range(epochs):
        model.train()
        optimizer.zero_grad(set_to_none=True)
        loss = torch.nn.functional.smooth_l1_loss(model(x_tensor), y_tensor)
        loss.backward()
        optimizer.step()
    linear_layers = [module for module in model if isinstance(module, torch.nn.Linear)]
    payload = {
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "preprocessing": {
            "imputer_median": median.tolist(),
            "scaler_mean": mean.tolist(),
            "scaler_scale": scale.tolist(),
        },
        "target": {"mean": target_mean, "scale": target_scale},
        "layers": [
            {
                "weight": layer.weight.detach().cpu().numpy().astype(float).tolist(),
                "bias": layer.bias.detach().cpu().numpy().astype(float).tolist(),
            }
            for layer in linear_layers
        ],
        "training": {
            "rows": len(frame), "epochs": epochs, "seed": seed,
            "device": selected,
            "source_sha256s": sorted(frame_source_hashes),
            "label_source_artifact_sha256s": sorted(frame_label_hashes),
            "surface_artifact_sha256": surface_artifact_hash,
            "surface_replay_receipt_sha256": surface_replay_hash,
        },
    }
    json_text = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    if len(json_text.encode("utf-8")) > MAX_ARTIFACT_BYTES:
        raise ValueError("frozen model artifact exceeds the allowed size")
    parse_frozen_model_artifact(payload)
    return FrozenModelBuild(
        json_text=json_text,
        sha256=hashlib.sha256(json_text.encode("utf-8")).hexdigest(),
        training_rows=len(frame),
        epochs=epochs,
        training_device=selected,
        source_sha256s=tuple(sorted(frame_source_hashes)),
        label_source_artifact_sha256s=tuple(sorted(frame_label_hashes)),
        surface_artifact_sha256=surface_artifact_hash,
        surface_replay_receipt_sha256=surface_replay_hash,
    )


def benchmark_frozen_model_inference(
    artifact: FrozenModelArtifact,
    frame: pd.DataFrame,
    *,
    repeats: int = 20,
    warmup_repeats: int = 3,
    prediction_tolerance: float = 1e-6,
    compare_cuda: bool = True,
) -> FrozenInferenceBenchmark:
    """Measure the same end-to-end artifact call on CPU and CUDA.

    DataFrame coercion, imputation, scaling, host/device transfer, model math,
    and result transfer are included because all are paid by live inference.
    """
    if frame.empty or repeats < 1 or warmup_repeats < 0:
        raise ValueError("benchmark needs rows, positive repeats, and nonnegative warmup")
    if not np.isfinite(prediction_tolerance) or prediction_tolerance < 0:
        raise ValueError("prediction_tolerance must be finite and nonnegative")

    def measure(device: str) -> tuple[np.ndarray, float]:
        prediction = np.empty(0, dtype=float)
        for _ in range(warmup_repeats):
            prediction = artifact.predict_log_return(frame, device=device)
        durations = []
        for _ in range(repeats):
            started = time.perf_counter()
            prediction = artifact.predict_log_return(frame, device=device)
            durations.append((time.perf_counter() - started) * 1000.0)
        return prediction, float(np.median(durations))

    cpu_prediction, cpu_ms = measure("cpu")
    import torch

    cuda_available = bool(torch.cuda.is_available())
    if not compare_cuda or not cuda_available:
        return FrozenInferenceBenchmark(
            rows=len(frame), repeats=repeats, cpu_median_milliseconds=cpu_ms,
            cuda_available=cuda_available, cuda_median_milliseconds=None,
            cuda_speedup_vs_cpu=None, max_abs_prediction_difference=None,
            prediction_tolerance=prediction_tolerance, numerically_equivalent=None,
        )
    cuda_prediction, cuda_ms = measure("cuda")
    difference = float(np.max(np.abs(cpu_prediction - cuda_prediction)))
    return FrozenInferenceBenchmark(
        rows=len(frame), repeats=repeats, cpu_median_milliseconds=cpu_ms,
        cuda_available=True, cuda_median_milliseconds=cuda_ms,
        cuda_speedup_vs_cpu=cpu_ms / cuda_ms if cuda_ms > 0 else None,
        max_abs_prediction_difference=difference,
        prediction_tolerance=prediction_tolerance,
        numerically_equivalent=difference <= prediction_tolerance,
    )
