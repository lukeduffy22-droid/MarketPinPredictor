from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Iterator, Sequence

import numpy as np
import pandas as pd


REQUIRED_PROVENANCE = (
    "session_id",
    "family_root",
    "minute_utc",
    "reference_price",
    "inference_method",
    "inference_version",
    "source_sha256",
    "capture_integrity_verified",
)
FORBIDDEN_FEATURE_TOKENS = ("target", "future", "label", "available_at")


@dataclass(frozen=True)
class FoldMetric:
    fold: int
    train_sessions: int
    test_sessions: int
    train_rows: int
    test_rows: int
    test_start_utc: str
    test_end_utc: str
    candidate_mae: float
    candidate_rmse: float
    candidate_directional_accuracy: float
    zero_return_mae: float
    zero_return_rmse: float
    zero_return_directional_accuracy: float


@dataclass(frozen=True)
class FamilyMetric:
    family_root: str
    rows: int
    sessions: int
    candidate_mae: float
    candidate_rmse: float
    candidate_directional_accuracy: float
    zero_return_mae: float
    zero_return_rmse: float
    zero_return_directional_accuracy: float


@dataclass(frozen=True)
class WalkForwardReport:
    horizon_minutes: int
    embargo_seconds: float
    independent_sessions: int
    features: tuple[str, ...]
    folds: tuple[FoldMetric, ...]
    family_metrics: tuple[FamilyMetric, ...]
    candidate_mae: float
    zero_return_mae: float
    mae_improvement_pct: float
    mae_improvement_ci_low_pct: float
    mae_improvement_ci_high_pct: float
    candidate_rmse: float
    zero_return_rmse: float
    candidate_directional_accuracy: float
    zero_return_directional_accuracy: float
    promoted: bool
    promotion_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class PrecomputedPredictionMetric:
    name: str
    rows: int
    sessions: int
    mae: float
    rmse: float
    directional_accuracy: float


@dataclass(frozen=True)
class PrecomputedFamilyMetric:
    name: str
    family_root: str
    rows: int
    sessions: int
    mae: float
    rmse: float
    directional_accuracy: float


@dataclass(frozen=True)
class PrecomputedComparisonReport:
    target: str
    metrics: tuple[PrecomputedPredictionMetric, ...]
    family_metrics: tuple[PrecomputedFamilyMetric, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _validate_columns(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    missing = [column for column in columns if column not in frame.columns]
    if missing:
        raise ValueError(f"missing required columns: {', '.join(missing)}")


def build_forward_return_labels(frame: pd.DataFrame, *, horizon_minutes: int) -> pd.DataFrame:
    """Attach exact-horizon log-return labels without crossing sessions.

    `minute_utc` is the feature as-of timestamp. The label is marked available
    only at the exact future timestamp, allowing downstream splitters to purge
    any row whose outcome was not known before a test fold began.
    """
    if horizon_minutes <= 0:
        raise ValueError("horizon_minutes must be positive")
    _validate_columns(frame, REQUIRED_PROVENANCE)
    if not frame["capture_integrity_verified"].fillna(False).astype(bool).all():
        raise ValueError("all research rows must come from integrity-verified captures")
    result = frame.copy()
    result["minute_utc"] = pd.to_datetime(result["minute_utc"], utc=True)
    if result.duplicated(["session_id", "family_root", "minute_utc"]).any():
        raise ValueError("duplicate session/family/minute feature rows")
    if (pd.to_numeric(result["reference_price"], errors="coerce") <= 0).any():
        raise ValueError("reference_price must be finite and positive")

    # A [14:30, 14:31) minute aggregate first becomes knowable at 14:31.
    # Never timestamp it at the bucket open for model or split purposes.
    result["feature_available_at_utc"] = result["minute_utc"] + pd.Timedelta(minutes=1)
    horizon = pd.Timedelta(minutes=horizon_minutes)
    result["target_time_utc"] = result["feature_available_at_utc"] + horizon
    future = result[
        ["session_id", "family_root", "feature_available_at_utc", "reference_price"]
    ].rename(
        columns={"feature_available_at_utc": "target_time_utc", "reference_price": "target_price"}
    )
    result = result.merge(
        future,
        on=["session_id", "family_root", "target_time_utc"],
        how="left",
        validate="one_to_one",
    )
    result = result[result["target_price"].notna()].copy()
    result["target_log_return"] = np.log(result["target_price"] / result["reference_price"])
    result["label_available_at_utc"] = result["target_time_utc"]
    return result.sort_values(["minute_utc", "session_id", "family_root"]).reset_index(drop=True)


def build_session_close_labels(frame: pd.DataFrame, closes: pd.DataFrame) -> pd.DataFrame:
    """Attach scored close outcomes while preserving when each label became known."""
    _validate_columns(frame, REQUIRED_PROVENANCE)
    _validate_columns(
        closes,
        ("family_root", "trading_date", "actual_close", "close_label_available_at_utc"),
    )
    result = frame.copy()
    result["minute_utc"] = pd.to_datetime(result["minute_utc"], utc=True)
    result["feature_available_at_utc"] = result["minute_utc"] + pd.Timedelta(minutes=1)
    close_frame = closes.copy()
    close_frame["close_label_available_at_utc"] = pd.to_datetime(
        close_frame["close_label_available_at_utc"], utc=True
    )
    result = result.merge(
        close_frame,
        on=["family_root", "trading_date"],
        how="inner",
        validate="many_to_one",
    )
    result = result[
        (pd.to_numeric(result["actual_close"], errors="coerce") > 0)
        & (result["close_label_available_at_utc"] > result["feature_available_at_utc"])
    ].copy()
    result["target_price"] = result["actual_close"]
    result["target_time_utc"] = result["close_label_available_at_utc"]
    result["label_available_at_utc"] = result["close_label_available_at_utc"]
    result["target_log_return"] = np.log(result["actual_close"] / result["reference_price"])
    if "predicted_close" in result.columns:
        predicted = pd.to_numeric(result["predicted_close"], errors="coerce")
        result["marketpin_predicted_log_return"] = np.log(predicted / result["reference_price"])
        result.loc[
            ~np.isfinite(result["marketpin_predicted_log_return"]),
            "marketpin_predicted_log_return",
        ] = np.nan
    return result.sort_values(["feature_available_at_utc", "session_id", "family_root"]).reset_index(drop=True)


def compare_precomputed_predictions(
    frame: pd.DataFrame,
    *,
    prediction_columns: Sequence[str],
    target_column: str = "target_log_return",
) -> PrecomputedComparisonReport:
    """Score timestamp-aligned live predictions without fitting or hindsight."""
    if not prediction_columns:
        raise ValueError("prediction_columns must be non-empty")
    _validate_columns(frame, ["session_id", "family_root", target_column, *prediction_columns])
    evidence_sessions = _independent_session_keys(frame)
    metrics: list[PrecomputedPredictionMetric] = []
    family_metrics: list[PrecomputedFamilyMetric] = []
    target = pd.to_numeric(frame[target_column], errors="coerce")
    for column in prediction_columns:
        predicted = pd.to_numeric(frame[column], errors="coerce")
        eligible = target.notna() & predicted.notna() & np.isfinite(target) & np.isfinite(predicted)
        if not eligible.any():
            continue
        actual_values = target[eligible].to_numpy(dtype=float)
        predicted_values = predicted[eligible].to_numpy(dtype=float)
        values = _metrics(actual_values, predicted_values)
        metrics.append(
            PrecomputedPredictionMetric(
                name=column,
                rows=int(eligible.sum()),
                sessions=int(evidence_sessions.loc[eligible].nunique()),
                mae=values[0],
                rmse=values[1],
                directional_accuracy=values[2],
            )
        )
        for family_root, family_frame in frame.loc[eligible].groupby("family_root", sort=True):
            family_actual = pd.to_numeric(family_frame[target_column], errors="coerce").to_numpy(dtype=float)
            family_predicted = pd.to_numeric(family_frame[column], errors="coerce").to_numpy(dtype=float)
            family_values = _metrics(family_actual, family_predicted)
            family_metrics.append(
                PrecomputedFamilyMetric(
                    name=column,
                    family_root=str(family_root),
                    rows=len(family_frame),
                    sessions=int(evidence_sessions.loc[family_frame.index].nunique()),
                    mae=family_values[0],
                    rmse=family_values[1],
                    directional_accuracy=family_values[2],
                )
            )
    if not metrics:
        raise ValueError("no finite aligned predictions are available")
    return PrecomputedComparisonReport(
        target=target_column,
        metrics=tuple(metrics),
        family_metrics=tuple(family_metrics),
    )


def _independent_session_keys(frame: pd.DataFrame) -> pd.Series:
    """Return chronological evidence blocks, rejecting same-day capture retries.

    Historical unit fixtures without ``trading_date`` retain their explicit
    session IDs. Production research surfaces always carry the trading date,
    which is the independent market outcome and close-label unit.
    """
    _validate_columns(frame, ("session_id",))
    if "trading_date" not in frame.columns:
        return frame["session_id"].astype(str)
    dates = pd.to_datetime(frame["trading_date"], errors="coerce").dt.date.astype("string")
    if dates.isna().any():
        raise ValueError("trading_date must be valid for independent-session evidence")
    mapping = pd.DataFrame(
        {"trading_date": dates, "session_id": frame["session_id"].astype(str)}
    ).drop_duplicates()
    ambiguous = mapping.groupby("trading_date")["session_id"].nunique()
    ambiguous_dates = ambiguous[ambiguous > 1].index.astype(str).tolist()
    if ambiguous_dates:
        raise ValueError(
            "multiple capture sessions share a trading date: " + ", ".join(ambiguous_dates)
        )
    return dates


def purged_session_walk_forward(
    frame: pd.DataFrame,
    *,
    min_train_sessions: int,
    test_sessions: int = 1,
    embargo_seconds: float = 300.0,
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """Yield chronological folds with label purging and a fixed time embargo."""
    if min_train_sessions < 1 or test_sessions < 1:
        raise ValueError("session counts must be positive")
    if embargo_seconds < 0:
        raise ValueError("embargo_seconds must be non-negative")
    _validate_columns(
        frame,
        ("session_id", "minute_utc", "feature_available_at_utc", "label_available_at_utc"),
    )
    evidence_sessions = _independent_session_keys(frame)
    working = frame.assign(_evidence_session=evidence_sessions)
    ordered = (
        working.groupby("_evidence_session", sort=False)["minute_utc"]
        .min()
        .sort_values()
        .index.tolist()
    )
    for start in range(min_train_sessions, len(ordered) - test_sessions + 1, test_sessions):
        train_ids = set(ordered[:start])
        test_ids = set(ordered[start : start + test_sessions])
        test_mask = working["_evidence_session"].isin(test_ids)
        test_start = working.loc[test_mask, "feature_available_at_utc"].min()
        embargo_cutoff = test_start - timedelta(seconds=float(embargo_seconds))
        train_mask = working["_evidence_session"].isin(train_ids) & (
            working["label_available_at_utc"] < embargo_cutoff
        )
        train_index = np.flatnonzero(train_mask.to_numpy())
        test_index = np.flatnonzero(test_mask.to_numpy())
        if len(train_index) and len(test_index):
            yield train_index, test_index


def _metrics(actual: np.ndarray, predicted: np.ndarray) -> tuple[float, float, float]:
    error = predicted - actual
    return (
        float(np.mean(np.abs(error))),
        float(math.sqrt(np.mean(np.square(error)))),
        float(np.mean(np.sign(predicted) == np.sign(actual))),
    )


def _session_block_improvement_interval(
    actual: np.ndarray,
    candidate: np.ndarray,
    baseline: np.ndarray,
    sessions: np.ndarray,
    *,
    iterations: int = 2000,
    seed: int = 1729,
) -> tuple[float, float]:
    """Return a deterministic 95% CI from whole-session bootstrap resamples."""
    unique_sessions = np.asarray(sorted(set(sessions.astype(str))))
    if len(unique_sessions) < 2:
        return float("nan"), float("nan")
    indices = {value: np.flatnonzero(sessions.astype(str) == value) for value in unique_sessions}
    rng = np.random.default_rng(seed)
    improvements: list[float] = []
    for _ in range(iterations):
        sampled = rng.choice(unique_sessions, size=len(unique_sessions), replace=True)
        selected = np.concatenate([indices[value] for value in sampled])
        candidate_mae = float(np.mean(np.abs(candidate[selected] - actual[selected])))
        baseline_mae = float(np.mean(np.abs(baseline[selected] - actual[selected])))
        if baseline_mae > 0:
            improvements.append((baseline_mae - candidate_mae) / baseline_mae * 100.0)
    if not improvements:
        return float("nan"), float("nan")
    low, high = np.quantile(np.asarray(improvements), [0.025, 0.975])
    return float(low), float(high)


def evaluate_ridge_walk_forward(
    frame: pd.DataFrame,
    *,
    feature_columns: Sequence[str],
    horizon_minutes: int,
    min_train_sessions: int = 5,
    test_sessions: int = 1,
    embargo_seconds: float = 300.0,
    minimum_independent_sessions: int = 10,
    minimum_sessions_per_family: int = 5,
    minimum_improvement_pct: float = 2.0,
) -> WalkForwardReport:
    """Compare a CPU ridge baseline with zero-return persistence out of sample."""
    if not feature_columns:
        raise ValueError("feature_columns must be explicit and non-empty")
    unsafe = [
        column for column in feature_columns
        if any(token in column.lower() for token in FORBIDDEN_FEATURE_TOKENS)
    ]
    if unsafe:
        raise ValueError(f"future/label columns cannot be model features: {', '.join(unsafe)}")
    _validate_columns(frame, [*REQUIRED_PROVENANCE, "target_log_return", "label_available_at_utc", *feature_columns])

    ordered = frame.sort_values(["feature_available_at_utc", "session_id", "family_root"]).reset_index(drop=True)
    evidence_sessions = _independent_session_keys(ordered)
    independent_session_count = int(evidence_sessions.nunique())
    if independent_session_count < minimum_independent_sessions:
        raise ValueError(
            f"insufficient independent-session evidence before model fitting: "
            f"{independent_session_count} available, {minimum_independent_sessions} required"
        )
    family_session_counts = (
        pd.DataFrame({"family_root": ordered["family_root"].astype(str), "session": evidence_sessions})
        .groupby("family_root")["session"]
        .nunique()
    )
    insufficient_families = {
        family: int(count)
        for family, count in family_session_counts.items()
        if int(count) < minimum_sessions_per_family
    }
    if insufficient_families:
        details = ", ".join(f"{family}={count}" for family, count in sorted(insufficient_families.items()))
        raise ValueError(
            f"insufficient per-family session evidence before model fitting: {details}; "
            f"{minimum_sessions_per_family} required"
        )

    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    x = ordered[list(feature_columns)].apply(pd.to_numeric, errors="coerce")
    y = ordered["target_log_return"].to_numpy(dtype=float)
    fold_metrics: list[FoldMetric] = []
    all_actual: list[np.ndarray] = []
    all_candidate: list[np.ndarray] = []
    all_zero: list[np.ndarray] = []
    all_families: list[np.ndarray] = []
    all_sessions: list[np.ndarray] = []
    for fold_number, (train, test) in enumerate(
        purged_session_walk_forward(
            ordered,
            min_train_sessions=min_train_sessions,
            test_sessions=test_sessions,
            embargo_seconds=embargo_seconds,
        ),
        start=1,
    ):
        model = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("ridge", Ridge(alpha=1.0)),
            ]
        )
        model.fit(x.iloc[train], y[train])
        candidate = model.predict(x.iloc[test])
        zero = np.zeros(len(test), dtype=float)
        candidate_metrics = _metrics(y[test], candidate)
        zero_metrics = _metrics(y[test], zero)
        train_sessions_count = int(evidence_sessions.iloc[train].nunique())
        test_sessions_count = int(evidence_sessions.iloc[test].nunique())
        fold_metrics.append(
            FoldMetric(
                fold=fold_number,
                train_sessions=train_sessions_count,
                test_sessions=test_sessions_count,
                train_rows=len(train),
                test_rows=len(test),
                test_start_utc=ordered.iloc[test]["feature_available_at_utc"].min().isoformat(),
                test_end_utc=ordered.iloc[test]["feature_available_at_utc"].max().isoformat(),
                candidate_mae=candidate_metrics[0],
                candidate_rmse=candidate_metrics[1],
                candidate_directional_accuracy=candidate_metrics[2],
                zero_return_mae=zero_metrics[0],
                zero_return_rmse=zero_metrics[1],
                zero_return_directional_accuracy=zero_metrics[2],
            )
        )
        all_actual.append(y[test])
        all_candidate.append(candidate)
        all_zero.append(zero)
        all_families.append(ordered.iloc[test]["family_root"].astype(str).to_numpy())
        all_sessions.append(evidence_sessions.iloc[test].astype(str).to_numpy())

    if not fold_metrics:
        raise ValueError("not enough complete sessions for one walk-forward fold")
    actual = np.concatenate(all_actual)
    candidate = np.concatenate(all_candidate)
    zero = np.concatenate(all_zero)
    families = np.concatenate(all_families)
    sessions = np.concatenate(all_sessions)
    candidate_metrics = _metrics(actual, candidate)
    zero_metrics = _metrics(actual, zero)
    improvement = (
        (zero_metrics[0] - candidate_metrics[0]) / zero_metrics[0] * 100.0
        if zero_metrics[0] > 0 else float("-inf")
    )
    improvement_ci_low, improvement_ci_high = _session_block_improvement_interval(
        actual, candidate, zero, sessions
    )
    reasons: list[str] = []
    family_metrics: list[FamilyMetric] = []
    for family_root in sorted(set(families)):
        mask = families == family_root
        candidate_family = _metrics(actual[mask], candidate[mask])
        zero_family = _metrics(actual[mask], zero[mask])
        family_metrics.append(
            FamilyMetric(
                family_root=family_root,
                rows=int(mask.sum()),
                sessions=len(set(sessions[mask])),
                candidate_mae=candidate_family[0],
                candidate_rmse=candidate_family[1],
                candidate_directional_accuracy=candidate_family[2],
                zero_return_mae=zero_family[0],
                zero_return_rmse=zero_family[1],
                zero_return_directional_accuracy=zero_family[2],
            )
        )
    if len(fold_metrics) < 5:
        reasons.append(f"only {len(fold_metrics)} walk-forward folds; at least 5 required")
    if improvement < minimum_improvement_pct:
        reasons.append(
            f"MAE improvement {improvement:.3f}% is below {minimum_improvement_pct:.3f}%"
        )
    if not np.isfinite(improvement_ci_low) or improvement_ci_low <= 0:
        reasons.append("95% session-block MAE improvement interval does not exclude zero")
    if candidate_metrics[2] <= zero_metrics[2]:
        reasons.append("directional accuracy did not beat zero-return persistence")
    for metric in family_metrics:
        if metric.candidate_mae >= metric.zero_return_mae:
            reasons.append(f"{metric.family_root} MAE did not beat zero-return persistence")
    return WalkForwardReport(
        horizon_minutes=horizon_minutes,
        embargo_seconds=float(embargo_seconds),
        independent_sessions=independent_session_count,
        features=tuple(feature_columns),
        folds=tuple(fold_metrics),
        family_metrics=tuple(family_metrics),
        candidate_mae=candidate_metrics[0],
        zero_return_mae=zero_metrics[0],
        mae_improvement_pct=improvement,
        mae_improvement_ci_low_pct=improvement_ci_low,
        mae_improvement_ci_high_pct=improvement_ci_high,
        candidate_rmse=candidate_metrics[1],
        zero_return_rmse=zero_metrics[1],
        candidate_directional_accuracy=candidate_metrics[2],
        zero_return_directional_accuracy=zero_metrics[2],
        promoted=not reasons,
        promotion_reasons=tuple(reasons),
    )
