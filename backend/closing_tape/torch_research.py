from __future__ import annotations

import copy
from dataclasses import asdict, dataclass
from typing import Sequence

import numpy as np
import pandas as pd

from .research import (
    FORBIDDEN_FEATURE_TOKENS,
    REQUIRED_PROVENANCE,
    _independent_session_keys,
    _metrics,
    _session_block_improvement_interval,
    _validate_columns,
    purged_session_walk_forward,
)


@dataclass(frozen=True)
class TorchFoldMetric:
    fold: int
    device: str
    epochs_trained: int
    train_sessions: int
    validation_sessions: int
    test_sessions: int
    train_rows: int
    validation_rows: int
    test_rows: int
    test_start_utc: str
    test_end_utc: str
    torch_mae: float
    ridge_mae: float
    zero_return_mae: float
    torch_rmse: float
    ridge_rmse: float
    zero_return_rmse: float
    torch_directional_accuracy: float
    ridge_directional_accuracy: float
    zero_return_directional_accuracy: float
    incumbent_mae: float
    incumbent_rmse: float
    incumbent_directional_accuracy: float
    incumbent_rows: int


@dataclass(frozen=True)
class TorchFamilyMetric:
    family_root: str
    rows: int
    sessions: int
    torch_mae: float
    ridge_mae: float
    zero_return_mae: float
    torch_directional_accuracy: float
    ridge_directional_accuracy: float
    zero_return_directional_accuracy: float
    incumbent_mae: float
    incumbent_directional_accuracy: float
    incumbent_rows: int


@dataclass(frozen=True)
class TorchOutOfSamplePrediction:
    session_id: str
    trading_date: str
    family_root: str
    source_sha256: str
    close_source_artifact_sha256: str
    feature_available_at_utc: str
    label_available_at_utc: str
    reference_price: float
    target_log_return: float
    torch_predicted_log_return: float
    ridge_predicted_log_return: float
    persistence_predicted_log_return: float
    incumbent_predicted_log_return: float | None


@dataclass(frozen=True)
class TorchWalkForwardReport:
    horizon_minutes: int
    features: tuple[str, ...]
    device: str
    hidden_units: tuple[int, int]
    complete_sessions: int
    source_files: int
    source_sha256s: tuple[str, ...]
    label_source_artifact_sha256s: tuple[str, ...]
    held_out_rows: int
    held_out_sessions: int
    folds: tuple[TorchFoldMetric, ...]
    family_metrics: tuple[TorchFamilyMetric, ...]
    out_of_sample_predictions: tuple[TorchOutOfSamplePrediction, ...]
    torch_mae: float
    ridge_mae: float
    zero_return_mae: float
    incumbent_mae: float
    incumbent_rmse: float
    incumbent_directional_accuracy: float
    incumbent_rows: int
    incumbent_sessions: int
    torch_improvement_over_ridge_pct: float
    torch_improvement_over_zero_pct: float
    torch_improvement_over_ridge_ci_low_pct: float
    torch_improvement_over_ridge_ci_high_pct: float
    torch_improvement_over_zero_ci_low_pct: float
    torch_improvement_over_zero_ci_high_pct: float
    torch_improvement_over_incumbent_pct: float
    torch_improvement_over_incumbent_ci_low_pct: float
    torch_improvement_over_incumbent_ci_high_pct: float
    torch_rmse: float
    ridge_rmse: float
    zero_return_rmse: float
    torch_directional_accuracy: float
    ridge_directional_accuracy: float
    zero_return_directional_accuracy: float
    promoted: bool
    promotion_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _choose_device(requested: str) -> str:
    import torch

    value = requested.lower()
    if value == "auto":
        return "cuda" if torch.cuda.is_available() else "cpu"
    if value not in {"cpu", "cuda"}:
        raise ValueError("device must be auto, cpu, or cuda")
    if value == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but is unavailable")
    return value


def evaluate_torch_walk_forward(
    frame: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    horizon_minutes: int,
    min_train_sessions: int = 5,
    test_sessions: int = 1,
    validation_sessions: int = 1,
    device: str = "auto",
    hidden_units: tuple[int, int] = (64, 32),
    max_epochs: int = 50,
    patience: int = 8,
    batch_size: int = 1024,
    learning_rate: float = 1e-3,
    minimum_complete_sessions: int = 60,
    minimum_sessions_per_family: int = 10,
    minimum_improvement_pct: float = 2.0,
    incumbent_prediction_column: str = "marketpin_predicted_log_return",
    seed: int = 17,
) -> TorchWalkForwardReport:
    """Evaluate a deterministic MLP against ridge, persistence, and the live incumbent."""
    if not feature_columns:
        raise ValueError("feature_columns must be explicit and non-empty")
    unsafe = [
        column for column in feature_columns
        if any(token in column.lower() for token in FORBIDDEN_FEATURE_TOKENS)
    ]
    if unsafe:
        raise ValueError(f"future/label columns cannot be model features: {', '.join(unsafe)}")
    if validation_sessions < 1 or max_epochs < 1 or patience < 1 or batch_size < 1:
        raise ValueError("validation, epoch, patience, and batch sizes must be positive")
    if min_train_sessions < 1 or test_sessions < 1:
        raise ValueError("train and test session counts must be positive")
    if validation_sessions >= min_train_sessions:
        raise ValueError("validation_sessions must be smaller than min_train_sessions")
    if minimum_complete_sessions < 1 or minimum_sessions_per_family < 1:
        raise ValueError("minimum session requirements must be positive")
    requested_device = str(device).strip().lower()
    if requested_device not in {"auto", "cpu", "cuda"}:
        raise ValueError("device must be auto, cpu, or cuda")
    _validate_columns(
        frame,
        [
            *REQUIRED_PROVENANCE,
            "target_log_return",
            "label_available_at_utc",
            "close_source_artifact_sha256",
            *feature_columns,
        ],
    )
    if not frame["capture_integrity_verified"].fillna(False).astype(bool).all():
        raise ValueError("all model rows must come from integrity-verified captures")
    label_artifact_hashes = tuple(
        sorted(frame["close_source_artifact_sha256"].astype(str).str.lower().unique())
    )
    if not label_artifact_hashes or any(
        not pd.Series([value]).str.fullmatch(r"[0-9a-f]{64}").iloc[0]
        for value in label_artifact_hashes
    ):
        raise ValueError("all model rows must retain a valid verified-close artifact SHA-256")

    ordered = frame.sort_values(["feature_available_at_utc", "session_id", "family_root"]).reset_index(drop=True)
    evidence_sessions = _independent_session_keys(ordered)
    complete_sessions = int(evidence_sessions.nunique())
    if complete_sessions < minimum_complete_sessions:
        raise ValueError(
            "insufficient integrity-verified independent sessions before model fitting: "
            f"{complete_sessions} available, {minimum_complete_sessions} required"
        )
    if complete_sessions < min_train_sessions + test_sessions:
        raise ValueError(
            "insufficient independent sessions for one purged train/validation/test fold "
            "before model fitting"
        )

    source_sha256s = tuple(
        sorted(ordered["source_sha256"].astype(str).str.lower().unique())
    )
    if not source_sha256s or any(
        not pd.Series([value]).str.fullmatch(r"[0-9a-f]{64}").iloc[0]
        for value in source_sha256s
    ):
        raise ValueError("all model rows must retain a valid finalized DBN SHA-256")
    source_files = len(source_sha256s)
    if source_files < minimum_complete_sessions:
        raise ValueError(
            "insufficient independent source evidence before model fitting: "
            f"{source_files} source hashes available, {minimum_complete_sessions} required"
        )

    family_session_counts = (
        pd.DataFrame(
            {
                "family_root": ordered["family_root"].astype(str),
                "evidence_session": evidence_sessions.astype(str),
            }
        )
        .groupby("family_root")["evidence_session"]
        .nunique()
    )
    insufficient_families = {
        str(family): int(count)
        for family, count in family_session_counts.items()
        if int(count) < minimum_sessions_per_family
    }
    if insufficient_families:
        details = ", ".join(
            f"{family}={count}" for family, count in sorted(insufficient_families.items())
        )
        raise ValueError(
            "insufficient per-family session evidence before model fitting: "
            f"{details}; {minimum_sessions_per_family} required"
        )

    # Heavy model imports, CUDA discovery, preprocessing fits, and optimizer
    # construction are intentionally below every evidence-sufficiency gate.
    # Calling this public evaluator directly must be as fail-closed as using
    # the higher-level training-readiness wrapper.
    import torch
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    selected_device = _choose_device(requested_device)
    raw_x = ordered[list(feature_columns)].apply(pd.to_numeric, errors="coerce")
    y = ordered["target_log_return"].to_numpy(dtype=np.float64)
    incumbent_series = (
        ordered[incumbent_prediction_column]
        if incumbent_prediction_column in ordered.columns
        else pd.Series(np.nan, index=ordered.index)
    )
    incumbent_values = pd.to_numeric(incumbent_series, errors="coerce").to_numpy(dtype=np.float64)
    folds: list[TorchFoldMetric] = []
    all_actual: list[np.ndarray] = []
    all_torch: list[np.ndarray] = []
    all_ridge: list[np.ndarray] = []
    all_zero: list[np.ndarray] = []
    all_incumbent: list[np.ndarray] = []
    all_families: list[np.ndarray] = []
    all_sessions: list[np.ndarray] = []
    out_of_sample_predictions: list[TorchOutOfSamplePrediction] = []

    for fold_number, (outer_train, test) in enumerate(
        purged_session_walk_forward(
            ordered,
            min_train_sessions=min_train_sessions,
            test_sessions=test_sessions,
        ),
        start=1,
    ):
        training_frame = ordered.iloc[outer_train]
        session_starts = (
            training_frame.assign(_evidence_session=evidence_sessions.iloc[outer_train].to_numpy())
            .groupby("_evidence_session")["feature_available_at_utc"]
            .min().sort_values()
        )
        if len(session_starts) <= validation_sessions:
            continue
        validation_ids = set(session_starts.index[-validation_sessions:])
        validation_mask = evidence_sessions.isin(validation_ids)
        validation_start = ordered.loc[validation_mask, "feature_available_at_utc"].min()
        inner_train_mask = (
            ordered.index.isin(outer_train)
            & ~validation_mask
            & (ordered["label_available_at_utc"] < validation_start)
        )
        train = np.flatnonzero(inner_train_mask)
        validation = np.flatnonzero(validation_mask.to_numpy() & ordered.index.isin(outer_train))
        if not len(train) or not len(validation):
            continue

        preprocessor = Pipeline(
            [("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())]
        )
        x_train = preprocessor.fit_transform(raw_x.iloc[train]).astype("float32")
        x_validation = preprocessor.transform(raw_x.iloc[validation]).astype("float32")
        x_test = preprocessor.transform(raw_x.iloc[test]).astype("float32")
        y_mean = float(np.mean(y[train]))
        y_scale = float(np.std(y[train]))
        if not np.isfinite(y_scale) or y_scale < 1e-12:
            y_scale = 1.0
        y_train = ((y[train] - y_mean) / y_scale).astype("float32")
        y_validation = ((y[validation] - y_mean) / y_scale).astype("float32")

        torch.manual_seed(seed + fold_number)
        if selected_device == "cuda":
            torch.cuda.manual_seed_all(seed + fold_number)
        model = torch.nn.Sequential(
            torch.nn.Linear(len(feature_columns), hidden_units[0]),
            torch.nn.GELU(),
            torch.nn.Linear(hidden_units[0], hidden_units[1]),
            torch.nn.GELU(),
            torch.nn.Linear(hidden_units[1], 1),
        ).to(selected_device)
        optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate, weight_decay=1e-4)
        train_dataset = torch.utils.data.TensorDataset(
            torch.from_numpy(x_train), torch.from_numpy(y_train[:, None])
        )
        generator = torch.Generator().manual_seed(seed + fold_number)
        loader = torch.utils.data.DataLoader(
            train_dataset,
            batch_size=min(batch_size, len(train_dataset)),
            shuffle=True,
            generator=generator,
        )
        validation_x = torch.from_numpy(x_validation).to(selected_device)
        validation_y = torch.from_numpy(y_validation[:, None]).to(selected_device)
        best_state = copy.deepcopy(model.state_dict())
        best_validation = float("inf")
        stale_epochs = 0
        epochs_trained = 0
        for epoch in range(max_epochs):
            model.train()
            for batch_x, batch_y in loader:
                batch_x = batch_x.to(selected_device)
                batch_y = batch_y.to(selected_device)
                optimizer.zero_grad(set_to_none=True)
                loss = torch.nn.functional.smooth_l1_loss(model(batch_x), batch_y)
                loss.backward()
                optimizer.step()
            model.eval()
            with torch.no_grad():
                validation_mae = float(
                    torch.mean(torch.abs(model(validation_x) - validation_y)).detach().cpu()
                )
            epochs_trained = epoch + 1
            if validation_mae < best_validation - 1e-7:
                best_validation = validation_mae
                best_state = copy.deepcopy(model.state_dict())
                stale_epochs = 0
            else:
                stale_epochs += 1
                if stale_epochs >= patience:
                    break
        model.load_state_dict(best_state)
        model.eval()
        with torch.no_grad():
            predicted_scaled = model(torch.from_numpy(x_test).to(selected_device)).cpu().numpy().ravel()
        torch_prediction = predicted_scaled.astype(np.float64) * y_scale + y_mean

        ridge = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("ridge", Ridge(alpha=1.0)),
            ]
        )
        ridge.fit(raw_x.iloc[train], y[train])
        ridge_prediction = ridge.predict(raw_x.iloc[test])
        zero_prediction = np.zeros(len(test), dtype=float)
        torch_metrics = _metrics(y[test], torch_prediction)
        ridge_metrics = _metrics(y[test], ridge_prediction)
        zero_metrics = _metrics(y[test], zero_prediction)
        fold_incumbent = incumbent_values[test]
        incumbent_mask = np.isfinite(fold_incumbent)
        incumbent_metrics = (
            _metrics(y[test][incumbent_mask], fold_incumbent[incumbent_mask])
            if incumbent_mask.any() else (float("nan"), float("nan"), float("nan"))
        )
        folds.append(
            TorchFoldMetric(
                fold=fold_number,
                device=selected_device,
                epochs_trained=epochs_trained,
                train_sessions=int(evidence_sessions.iloc[train].nunique()),
                validation_sessions=int(evidence_sessions.iloc[validation].nunique()),
                test_sessions=int(evidence_sessions.iloc[test].nunique()),
                train_rows=len(train), validation_rows=len(validation), test_rows=len(test),
                test_start_utc=ordered.iloc[test]["feature_available_at_utc"].min().isoformat(),
                test_end_utc=ordered.iloc[test]["feature_available_at_utc"].max().isoformat(),
                torch_mae=torch_metrics[0], ridge_mae=ridge_metrics[0], zero_return_mae=zero_metrics[0],
                torch_rmse=torch_metrics[1], ridge_rmse=ridge_metrics[1], zero_return_rmse=zero_metrics[1],
                torch_directional_accuracy=torch_metrics[2],
                ridge_directional_accuracy=ridge_metrics[2],
                zero_return_directional_accuracy=zero_metrics[2],
                incumbent_mae=incumbent_metrics[0],
                incumbent_rmse=incumbent_metrics[1],
                incumbent_directional_accuracy=incumbent_metrics[2],
                incumbent_rows=int(incumbent_mask.sum()),
            )
        )
        all_actual.append(y[test])
        all_torch.append(torch_prediction)
        all_ridge.append(ridge_prediction)
        all_zero.append(zero_prediction)
        all_incumbent.append(fold_incumbent)
        all_families.append(ordered.iloc[test]["family_root"].astype(str).to_numpy())
        all_sessions.append(evidence_sessions.iloc[test].astype(str).to_numpy())
        for position, row_index in enumerate(test):
            row = ordered.iloc[row_index]
            incumbent_value = float(fold_incumbent[position])
            feature_time = pd.Timestamp(row["feature_available_at_utc"])
            trading_date = str(row.get("trading_date", feature_time.date().isoformat()))
            out_of_sample_predictions.append(
                TorchOutOfSamplePrediction(
                    session_id=str(row["session_id"]),
                    trading_date=trading_date,
                    family_root=str(row["family_root"]),
                    source_sha256=str(row["source_sha256"]).lower(),
                    close_source_artifact_sha256=str(
                        row["close_source_artifact_sha256"]
                    ).lower(),
                    feature_available_at_utc=feature_time.isoformat(),
                    label_available_at_utc=pd.Timestamp(
                        row["label_available_at_utc"]
                    ).isoformat(),
                    reference_price=float(row["reference_price"]),
                    target_log_return=float(y[row_index]),
                    torch_predicted_log_return=float(torch_prediction[position]),
                    ridge_predicted_log_return=float(ridge_prediction[position]),
                    persistence_predicted_log_return=0.0,
                    incumbent_predicted_log_return=(
                        incumbent_value if np.isfinite(incumbent_value) else None
                    ),
                )
            )

    if not folds:
        raise ValueError("not enough complete sessions for one train/validation/test fold")
    actual = np.concatenate(all_actual)
    torch_prediction = np.concatenate(all_torch)
    ridge_prediction = np.concatenate(all_ridge)
    zero_prediction = np.concatenate(all_zero)
    incumbent_prediction = np.concatenate(all_incumbent)
    families = np.concatenate(all_families)
    sessions = np.concatenate(all_sessions)
    torch_metrics = _metrics(actual, torch_prediction)
    ridge_metrics = _metrics(actual, ridge_prediction)
    zero_metrics = _metrics(actual, zero_prediction)
    incumbent_mask = np.isfinite(incumbent_prediction)
    incumbent_metrics = (
        _metrics(actual[incumbent_mask], incumbent_prediction[incumbent_mask])
        if incumbent_mask.any() else (float("nan"), float("nan"), float("nan"))
    )
    torch_incumbent_metrics = (
        _metrics(actual[incumbent_mask], torch_prediction[incumbent_mask])
        if incumbent_mask.any() else (float("nan"), float("nan"), float("nan"))
    )

    def improvement(baseline: float) -> float:
        return (baseline - torch_metrics[0]) / baseline * 100.0 if baseline > 0 else float("-inf")

    ridge_improvement = improvement(ridge_metrics[0])
    zero_improvement = improvement(zero_metrics[0])
    incumbent_improvement = (
        (incumbent_metrics[0] - torch_incumbent_metrics[0]) / incumbent_metrics[0] * 100.0
        if np.isfinite(incumbent_metrics[0]) and incumbent_metrics[0] > 0 else float("-inf")
    )
    ridge_ci_low, ridge_ci_high = _session_block_improvement_interval(
        actual, torch_prediction, ridge_prediction, sessions, seed=1733
    )
    zero_ci_low, zero_ci_high = _session_block_improvement_interval(
        actual, torch_prediction, zero_prediction, sessions, seed=1741
    )
    if incumbent_mask.any():
        incumbent_ci_low, incumbent_ci_high = _session_block_improvement_interval(
            actual[incumbent_mask], torch_prediction[incumbent_mask],
            incumbent_prediction[incumbent_mask], sessions[incumbent_mask], seed=1753
        )
    else:
        incumbent_ci_low, incumbent_ci_high = float("nan"), float("nan")
    reasons: list[str] = []
    family_metrics: list[TorchFamilyMetric] = []
    for family_root in sorted(set(families)):
        mask = families == family_root
        torch_family = _metrics(actual[mask], torch_prediction[mask])
        ridge_family = _metrics(actual[mask], ridge_prediction[mask])
        zero_family = _metrics(actual[mask], zero_prediction[mask])
        family_incumbent_mask = mask & incumbent_mask
        incumbent_family = (
            _metrics(actual[family_incumbent_mask], incumbent_prediction[family_incumbent_mask])
            if family_incumbent_mask.any() else (float("nan"), float("nan"), float("nan"))
        )
        family_metrics.append(
            TorchFamilyMetric(
                family_root=family_root,
                rows=int(mask.sum()),
                sessions=len(set(sessions[mask])),
                torch_mae=torch_family[0],
                ridge_mae=ridge_family[0],
                zero_return_mae=zero_family[0],
                torch_directional_accuracy=torch_family[2],
                ridge_directional_accuracy=ridge_family[2],
                zero_return_directional_accuracy=zero_family[2],
                incumbent_mae=incumbent_family[0],
                incumbent_directional_accuracy=incumbent_family[2],
                incumbent_rows=int(family_incumbent_mask.sum()),
            )
        )
    if len(folds) < 5:
        reasons.append(f"only {len(folds)} walk-forward folds; at least 5 required")
    if ridge_improvement < minimum_improvement_pct:
        reasons.append(
            f"MLP improvement over ridge {ridge_improvement:.3f}% is below {minimum_improvement_pct:.3f}%"
        )
    if zero_improvement < minimum_improvement_pct:
        reasons.append(
            f"MLP improvement over persistence {zero_improvement:.3f}% is below {minimum_improvement_pct:.3f}%"
        )
    if int(incumbent_mask.sum()) != len(actual):
        reasons.append(
            f"incumbent timestamp-aligned coverage is {int(incumbent_mask.sum())}/{len(actual)} held-out rows"
        )
    elif incumbent_improvement < minimum_improvement_pct:
        reasons.append(
            f"MLP improvement over incumbent {incumbent_improvement:.3f}% is below {minimum_improvement_pct:.3f}%"
        )
    if not np.isfinite(ridge_ci_low) or ridge_ci_low <= 0:
        reasons.append("95% session-block MLP improvement interval versus ridge does not exclude zero")
    if not np.isfinite(zero_ci_low) or zero_ci_low <= 0:
        reasons.append("95% session-block MLP improvement interval versus persistence does not exclude zero")
    if not np.isfinite(incumbent_ci_low) or incumbent_ci_low <= 0:
        reasons.append("95% session-block MLP improvement interval versus incumbent does not exclude zero")
    if torch_metrics[2] <= ridge_metrics[2]:
        reasons.append("MLP directional accuracy did not beat ridge")
    for metric in family_metrics:
        if metric.torch_mae >= metric.ridge_mae:
            reasons.append(f"{metric.family_root} MLP MAE did not beat ridge")
        if metric.torch_mae >= metric.zero_return_mae:
            reasons.append(f"{metric.family_root} MLP MAE did not beat persistence")
        if metric.incumbent_rows != metric.rows:
            reasons.append(f"{metric.family_root} incumbent coverage is incomplete")
        elif metric.torch_mae >= metric.incumbent_mae:
            reasons.append(f"{metric.family_root} MLP MAE did not beat incumbent")
    return TorchWalkForwardReport(
        horizon_minutes=horizon_minutes,
        features=tuple(feature_columns),
        device=selected_device,
        hidden_units=hidden_units,
        complete_sessions=complete_sessions,
        source_files=source_files,
        source_sha256s=source_sha256s,
        label_source_artifact_sha256s=label_artifact_hashes,
        held_out_rows=len(actual),
        held_out_sessions=len(set(sessions)),
        folds=tuple(folds),
        family_metrics=tuple(family_metrics),
        out_of_sample_predictions=tuple(out_of_sample_predictions),
        torch_mae=torch_metrics[0], ridge_mae=ridge_metrics[0], zero_return_mae=zero_metrics[0],
        incumbent_mae=incumbent_metrics[0], incumbent_rmse=incumbent_metrics[1],
        incumbent_directional_accuracy=incumbent_metrics[2],
        incumbent_rows=int(incumbent_mask.sum()),
        incumbent_sessions=len(set(sessions[incumbent_mask])),
        torch_improvement_over_ridge_pct=ridge_improvement,
        torch_improvement_over_zero_pct=zero_improvement,
        torch_improvement_over_ridge_ci_low_pct=ridge_ci_low,
        torch_improvement_over_ridge_ci_high_pct=ridge_ci_high,
        torch_improvement_over_zero_ci_low_pct=zero_ci_low,
        torch_improvement_over_zero_ci_high_pct=zero_ci_high,
        torch_improvement_over_incumbent_pct=incumbent_improvement,
        torch_improvement_over_incumbent_ci_low_pct=incumbent_ci_low,
        torch_improvement_over_incumbent_ci_high_pct=incumbent_ci_high,
        torch_rmse=torch_metrics[1], ridge_rmse=ridge_metrics[1], zero_return_rmse=zero_metrics[1],
        torch_directional_accuracy=torch_metrics[2],
        ridge_directional_accuracy=ridge_metrics[2],
        zero_return_directional_accuracy=zero_metrics[2],
        promoted=not reasons,
        promotion_reasons=tuple(reasons),
    )
