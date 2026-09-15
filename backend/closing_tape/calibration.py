from __future__ import annotations

import math
import re
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime

import numpy as np
import pandas as pd

from .research import _independent_session_keys, _validate_columns


DEPLOYMENT_CALIBRATION_METHOD = "family_absolute_residual_conformal_v1"
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class CalibrationMetric:
    family_root: str
    rows: int
    sessions: int
    target_coverage: float
    empirical_coverage: float
    coverage_error: float
    mean_interval_width: float
    mean_absolute_error: float


@dataclass(frozen=True)
class OnlineConformalReport:
    prediction_column: str
    target_column: str
    alpha: float
    minimum_calibration_sessions: int
    calibration_window_sessions: int | None
    eligible_rows: int
    uncalibrated_rows: int
    overall: CalibrationMetric
    family_metrics: tuple[CalibrationMetric, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class DeploymentFamilyRadius:
    family_root: str
    rows: int
    sessions: int
    radius_log_return: float


@dataclass(frozen=True)
class DeploymentConformalCalibration:
    method: str
    alpha: float
    target_coverage: float
    asof_utc: str
    model_version: str
    artifact_sha256: str
    eligible_rows: int
    excluded_future_rows: int
    evidence_sha256: str
    source_sha256s: tuple[str, ...]
    label_source_artifact_sha256s: tuple[str, ...]
    family_radii: tuple[DeploymentFamilyRadius, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def bind_candidate_oos_predictions(
    evaluation_report: object,
    *,
    model_version: str,
    artifact_sha256: str,
) -> pd.DataFrame:
    """Bind retained leakage-safe fold predictions to one frozen candidate identity."""
    version = str(model_version).strip()
    artifact_hash = str(artifact_sha256).lower()
    if not version or not SHA256_PATTERN.fullmatch(artifact_hash):
        raise ValueError("candidate model version and artifact SHA-256 are required")
    if not bool(getattr(evaluation_report, "promoted", False)):
        raise ValueError("only a promoted walk-forward evaluation can be candidate-bound")
    predictions = tuple(getattr(evaluation_report, "out_of_sample_predictions", ()))
    if not predictions:
        raise ValueError("walk-forward evaluation retained no out-of-sample predictions")
    frame = pd.DataFrame.from_records([asdict(item) for item in predictions])
    if frame.duplicated(["trading_date", "family_root"]).any():
        raise ValueError("walk-forward evaluation has duplicate family/date predictions")
    frame["model_version"] = version
    frame["artifact_sha256"] = artifact_hash
    frame["prediction_is_out_of_sample"] = True
    return frame


def fit_deployment_conformal_calibration(
    frame: pd.DataFrame,
    *,
    prediction_column: str,
    asof_utc: datetime,
    target_column: str = "target_log_return",
    alpha: float = 0.1,
    minimum_family_sessions: int = 20,
    expected_families: tuple[str, ...] = ("SPX", "NDX", "RUT", "VIX", "SPY"),
) -> DeploymentConformalCalibration:
    """Fit fixed deployment radii from candidate-specific OOS residuals only."""
    if not 0 < alpha < 1 or minimum_family_sessions < 2:
        raise ValueError("deployment alpha and minimum family sessions are invalid")
    observed_asof = pd.Timestamp(asof_utc)
    if observed_asof.tzinfo is None:
        raise ValueError("deployment calibration asof_utc must be timezone-aware")
    observed_asof = observed_asof.tz_convert("UTC")
    required = (
        "session_id", "trading_date", "family_root", "label_available_at_utc",
        "source_sha256", "close_source_artifact_sha256",
        "model_version", "artifact_sha256",
        "prediction_is_out_of_sample", prediction_column, target_column,
    )
    _validate_columns(frame, required)
    if frame.empty:
        raise ValueError("deployment calibration requires out-of-sample rows")
    _independent_session_keys(frame)
    if not frame["prediction_is_out_of_sample"].fillna(False).astype(bool).all():
        raise ValueError("deployment calibration accepts only out-of-sample predictions")
    versions = frame["model_version"].astype(str).unique()
    artifacts = frame["artifact_sha256"].astype(str).str.lower().unique()
    if len(versions) != 1 or not versions[0] or len(artifacts) != 1:
        raise ValueError("deployment calibration mixes candidate identity")
    if not pd.Series(artifacts).str.fullmatch(r"[0-9a-f]{64}").all():
        raise ValueError("deployment calibration artifact SHA-256 is invalid")
    label_times = pd.to_datetime(frame["label_available_at_utc"], utc=True, errors="coerce")
    if label_times.isna().any():
        raise ValueError("deployment calibration label availability is invalid")
    eligible = frame.loc[label_times <= observed_asof].copy()
    excluded = len(frame) - len(eligible)
    if eligible.empty:
        raise ValueError("no out-of-sample labels were available by calibration as-of time")
    roots = tuple(dict.fromkeys(str(value).upper() for value in expected_families))
    if set(eligible["family_root"].astype(str)) != set(roots):
        raise ValueError("deployment calibration must contain exactly the production families")
    if eligible.duplicated(["trading_date", "family_root"]).any():
        raise ValueError("deployment calibration has duplicate family rows for a trading date")
    sources = tuple(sorted(eligible["source_sha256"].astype(str).str.lower().unique()))
    if any(not pd.Series([value]).str.fullmatch(r"[0-9a-f]{64}").iloc[0] for value in sources):
        raise ValueError("deployment calibration source SHA-256 is invalid")
    independent_dates = int(eligible["trading_date"].nunique())
    if len(sources) < independent_dates:
        raise ValueError("deployment calibration source hashes do not cover each trading date")
    label_artifacts = tuple(
        sorted(
            eligible["close_source_artifact_sha256"]
            .astype(str)
            .str.lower()
            .unique()
        )
    )
    if not label_artifacts or any(
        not SHA256_PATTERN.fullmatch(value) for value in label_artifacts
    ):
        raise ValueError("deployment calibration close artifact SHA-256 is invalid")
    eligible[prediction_column] = pd.to_numeric(eligible[prediction_column], errors="coerce")
    eligible[target_column] = pd.to_numeric(eligible[target_column], errors="coerce")
    if not np.isfinite(eligible[[prediction_column, target_column]].to_numpy(float)).all():
        raise ValueError("deployment calibration residuals are not finite")
    evidence_records = []
    for _index, row in eligible.sort_values(
        ["trading_date", "family_root", "session_id"]
    ).iterrows():
        evidence_records.append(
            {
                "session_id": str(row["session_id"]),
                "trading_date": str(row["trading_date"]),
                "family_root": str(row["family_root"]),
                "label_available_at_utc": pd.Timestamp(
                    row["label_available_at_utc"]
                ).tz_convert("UTC").isoformat(),
                "source_sha256": str(row["source_sha256"]).lower(),
                "close_source_artifact_sha256": str(
                    row["close_source_artifact_sha256"]
                ).lower(),
                "model_version": str(row["model_version"]),
                "artifact_sha256": str(row["artifact_sha256"]).lower(),
                "prediction": float(row[prediction_column]),
                "target": float(row[target_column]),
            }
        )
    evidence_json = json.dumps(
        evidence_records, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    evidence_sha256 = hashlib.sha256(evidence_json).hexdigest()

    family_radii: list[DeploymentFamilyRadius] = []
    for family in roots:
        group = eligible[eligible["family_root"].astype(str) == family]
        sessions = int(group["trading_date"].nunique())
        if sessions < minimum_family_sessions:
            raise ValueError(
                f"{family} deployment calibration sessions {sessions} < {minimum_family_sessions}"
            )
        actual = pd.to_numeric(group[target_column], errors="coerce").to_numpy(float)
        predicted = pd.to_numeric(group[prediction_column], errors="coerce").to_numpy(float)
        if not np.isfinite(actual).all() or not np.isfinite(predicted).all():
            raise ValueError(f"{family} deployment calibration residuals are not finite")
        residuals = np.abs(actual - predicted)
        quantile = min(1.0, math.ceil((len(residuals) + 1) * (1 - alpha)) / len(residuals))
        radius = float(np.quantile(residuals, quantile, method="higher"))
        if not np.isfinite(radius) or radius < 0:
            raise ValueError(f"{family} deployment calibration radius is invalid")
        family_radii.append(DeploymentFamilyRadius(family, len(group), sessions, radius))
    return DeploymentConformalCalibration(
        method=DEPLOYMENT_CALIBRATION_METHOD,
        alpha=float(alpha), target_coverage=float(1 - alpha),
        asof_utc=observed_asof.isoformat(), model_version=str(versions[0]),
        artifact_sha256=str(artifacts[0]), eligible_rows=len(eligible),
        excluded_future_rows=excluded, evidence_sha256=evidence_sha256,
        source_sha256s=sources,
        label_source_artifact_sha256s=label_artifacts,
        family_radii=tuple(family_radii),
    )


def build_online_conformal_intervals(
    frame: pd.DataFrame,
    *,
    prediction_column: str,
    target_column: str = "target_log_return",
    alpha: float = 0.1,
    minimum_calibration_sessions: int = 5,
    calibration_window_sessions: int | None = 20,
) -> pd.DataFrame:
    """Attach intervals using only labeled residuals from earlier sessions.

    Each option family is calibrated independently. Rows remain in the result
    when insufficient history exists, but their interval fields are NaN so a
    caller cannot mistake an uncalibrated estimate for a validated range.
    """
    if not 0 < alpha < 1:
        raise ValueError("alpha must be between zero and one")
    if minimum_calibration_sessions < 1:
        raise ValueError("minimum_calibration_sessions must be positive")
    if calibration_window_sessions is not None and calibration_window_sessions < 1:
        raise ValueError("calibration_window_sessions must be positive or None")
    required = (
        "session_id",
        "family_root",
        "feature_available_at_utc",
        "label_available_at_utc",
        prediction_column,
        target_column,
    )
    _validate_columns(frame, required)
    result = frame.copy()
    result["_evidence_session"] = _independent_session_keys(result)
    result["feature_available_at_utc"] = pd.to_datetime(
        result["feature_available_at_utc"], utc=True
    )
    result["label_available_at_utc"] = pd.to_datetime(
        result["label_available_at_utc"], utc=True
    )
    result[prediction_column] = pd.to_numeric(result[prediction_column], errors="coerce")
    result[target_column] = pd.to_numeric(result[target_column], errors="coerce")
    result["interval_radius"] = np.nan
    result["prediction_lower"] = np.nan
    result["prediction_upper"] = np.nan
    result["interval_calibration_sessions"] = 0
    result["interval_calibration_rows"] = 0

    for _family, family_frame in result.groupby("family_root", sort=True):
        session_starts = (
            family_frame.groupby("_evidence_session")["feature_available_at_utc"].min().sort_values()
        )
        ordered_sessions = session_starts.index.astype(str).tolist()
        for position, session_id in enumerate(ordered_sessions):
            current_index = family_frame.index[family_frame["_evidence_session"].astype(str) == session_id]
            test_start = result.loc[current_index, "feature_available_at_utc"].min()
            prior_ids = ordered_sessions[:position]
            if calibration_window_sessions is not None:
                prior_ids = prior_ids[-calibration_window_sessions:]
            history = family_frame[
                family_frame["_evidence_session"].astype(str).isin(prior_ids)
                & (family_frame["label_available_at_utc"] < test_start)
            ]
            finite = (
                np.isfinite(history[prediction_column])
                & np.isfinite(history[target_column])
            )
            history = history[finite]
            history_sessions = int(history["_evidence_session"].nunique())
            if history_sessions < minimum_calibration_sessions or history.empty:
                continue
            residuals = np.abs(
                history[target_column].to_numpy(dtype=float)
                - history[prediction_column].to_numpy(dtype=float)
            )
            quantile = min(1.0, math.ceil((len(residuals) + 1) * (1 - alpha)) / len(residuals))
            radius = float(np.quantile(residuals, quantile, method="higher"))
            prediction = result.loc[current_index, prediction_column]
            result.loc[current_index, "interval_radius"] = radius
            result.loc[current_index, "prediction_lower"] = prediction - radius
            result.loc[current_index, "prediction_upper"] = prediction + radius
            result.loc[current_index, "interval_calibration_sessions"] = history_sessions
            result.loc[current_index, "interval_calibration_rows"] = len(history)
    return result.drop(columns="_evidence_session").sort_values(
        ["feature_available_at_utc", "session_id", "family_root"]
    ).reset_index(drop=True)


def _metric(frame: pd.DataFrame, *, alpha: float, target_column: str) -> CalibrationMetric:
    actual = frame[target_column].to_numpy(dtype=float)
    prediction = frame["_prediction"].to_numpy(dtype=float)
    covered = (actual >= frame["prediction_lower"].to_numpy(dtype=float)) & (
        actual <= frame["prediction_upper"].to_numpy(dtype=float)
    )
    empirical = float(np.mean(covered))
    target = 1.0 - alpha
    roots = sorted(frame["family_root"].astype(str).unique())
    return CalibrationMetric(
        family_root=roots[0] if len(roots) == 1 else "ALL",
        rows=len(frame),
        sessions=int(_independent_session_keys(frame).nunique()),
        target_coverage=target,
        empirical_coverage=empirical,
        coverage_error=abs(empirical - target),
        mean_interval_width=float(
            np.mean(frame["prediction_upper"] - frame["prediction_lower"])
        ),
        mean_absolute_error=float(np.mean(np.abs(prediction - actual))),
    )


def evaluate_online_conformal_intervals(
    frame: pd.DataFrame,
    *,
    prediction_column: str,
    target_column: str = "target_log_return",
    alpha: float = 0.1,
    minimum_calibration_sessions: int = 5,
    calibration_window_sessions: int | None = 20,
) -> OnlineConformalReport:
    intervals = build_online_conformal_intervals(
        frame,
        prediction_column=prediction_column,
        target_column=target_column,
        alpha=alpha,
        minimum_calibration_sessions=minimum_calibration_sessions,
        calibration_window_sessions=calibration_window_sessions,
    )
    calibrated = intervals[
        np.isfinite(intervals["prediction_lower"])
        & np.isfinite(intervals["prediction_upper"])
        & np.isfinite(intervals[prediction_column])
        & np.isfinite(intervals[target_column])
    ].copy()
    if calibrated.empty:
        raise ValueError("not enough prior labeled sessions to calibrate an interval")
    calibrated["_prediction"] = calibrated[prediction_column]
    families = tuple(
        _metric(group, alpha=alpha, target_column=target_column)
        for _root, group in calibrated.groupby("family_root", sort=True)
    )
    return OnlineConformalReport(
        prediction_column=prediction_column,
        target_column=target_column,
        alpha=alpha,
        minimum_calibration_sessions=minimum_calibration_sessions,
        calibration_window_sessions=calibration_window_sessions,
        eligible_rows=len(calibrated),
        uncalibrated_rows=len(intervals) - len(calibrated),
        overall=_metric(calibrated, alpha=alpha, target_column=target_column),
        family_metrics=families,
    )
