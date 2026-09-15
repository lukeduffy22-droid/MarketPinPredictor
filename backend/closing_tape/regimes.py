from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from .research import _independent_session_keys, _metrics, _validate_columns


@dataclass(frozen=True)
class RegimeMetric:
    family_root: str
    volatility_regime: str
    rows: int
    sessions: int
    mae: float
    rmse: float
    directional_accuracy: float


@dataclass(frozen=True)
class RegimeEvaluationReport:
    prediction_column: str
    target_column: str
    lookback_minutes: int
    minimum_history_sessions: int
    metrics: tuple[RegimeMetric, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_online_volatility_regimes(
    frame: pd.DataFrame,
    *,
    lookback_minutes: int = 15,
    minimum_history_sessions: int = 5,
    history_window_sessions: int | None = 20,
) -> pd.DataFrame:
    """Classify observed trailing volatility using prior-session thresholds."""
    if lookback_minutes < 2:
        raise ValueError("lookback_minutes must be at least two")
    if minimum_history_sessions < 1:
        raise ValueError("minimum_history_sessions must be positive")
    if history_window_sessions is not None and history_window_sessions < 1:
        raise ValueError("history_window_sessions must be positive or None")
    _validate_columns(
        frame,
        ("session_id", "family_root", "feature_available_at_utc", "reference_price"),
    )
    result = frame.copy()
    result["_evidence_session"] = _independent_session_keys(result)
    result["feature_available_at_utc"] = pd.to_datetime(
        result["feature_available_at_utc"], utc=True
    )
    result["reference_price"] = pd.to_numeric(result["reference_price"], errors="coerce")
    result = result.sort_values(
        ["family_root", "session_id", "feature_available_at_utc"]
    ).reset_index(drop=True)
    if (~np.isfinite(result["reference_price"]) | (result["reference_price"] <= 0)).any():
        raise ValueError("reference_price must be finite and positive")
    log_price = np.log(result["reference_price"])
    result["_observed_log_return"] = log_price.groupby(
        [result["family_root"], result["session_id"]]
    ).diff()
    result["trailing_realized_volatility"] = (
        result.groupby(["family_root", "session_id"])["_observed_log_return"]
        .rolling(lookback_minutes, min_periods=lookback_minutes)
        .apply(lambda values: float(np.sqrt(np.mean(np.square(values)))), raw=True)
        .reset_index(level=[0, 1], drop=True)
    )
    result["volatility_regime"] = pd.Series(pd.NA, index=result.index, dtype="string")
    result["regime_low_threshold"] = np.nan
    result["regime_high_threshold"] = np.nan
    result["regime_history_sessions"] = 0

    for _family, family_frame in result.groupby("family_root", sort=True):
        starts = (
            family_frame.groupby("_evidence_session")["feature_available_at_utc"].min().sort_values()
        )
        ordered_sessions = starts.index.astype(str).tolist()
        for position, session_id in enumerate(ordered_sessions):
            prior_ids = ordered_sessions[:position]
            if history_window_sessions is not None:
                prior_ids = prior_ids[-history_window_sessions:]
            history = family_frame[
                family_frame["_evidence_session"].astype(str).isin(prior_ids)
                & np.isfinite(family_frame["trailing_realized_volatility"])
            ]
            history_sessions = int(history["_evidence_session"].nunique())
            if history_sessions < minimum_history_sessions or history.empty:
                continue
            low, high = np.quantile(history["trailing_realized_volatility"], [1 / 3, 2 / 3])
            current = family_frame.index[family_frame["_evidence_session"].astype(str) == session_id]
            volatility = result.loc[current, "trailing_realized_volatility"]
            regimes = pd.Series(pd.NA, index=current, dtype="string")
            finite = np.isfinite(volatility)
            regimes.loc[finite & (volatility <= low)] = "calm"
            regimes.loc[finite & (volatility > low) & (volatility <= high)] = "normal"
            regimes.loc[finite & (volatility > high)] = "stressed"
            result.loc[current, "volatility_regime"] = regimes
            result.loc[current, "regime_low_threshold"] = float(low)
            result.loc[current, "regime_high_threshold"] = float(high)
            result.loc[current, "regime_history_sessions"] = history_sessions
    return result.drop(columns=["_observed_log_return", "_evidence_session"]).sort_values(
        ["feature_available_at_utc", "session_id", "family_root"]
    ).reset_index(drop=True)


def evaluate_predictions_by_regime(
    frame: pd.DataFrame,
    *,
    prediction_column: str,
    target_column: str = "target_log_return",
    lookback_minutes: int = 15,
    minimum_history_sessions: int = 5,
    history_window_sessions: int | None = 20,
) -> RegimeEvaluationReport:
    _validate_columns(frame, (prediction_column, target_column))
    classified = build_online_volatility_regimes(
        frame,
        lookback_minutes=lookback_minutes,
        minimum_history_sessions=minimum_history_sessions,
        history_window_sessions=history_window_sessions,
    )
    classified[prediction_column] = pd.to_numeric(
        classified[prediction_column], errors="coerce"
    )
    classified[target_column] = pd.to_numeric(classified[target_column], errors="coerce")
    eligible = classified[
        classified["volatility_regime"].notna()
        & np.isfinite(classified[prediction_column])
        & np.isfinite(classified[target_column])
    ]
    if eligible.empty:
        raise ValueError("not enough point-in-time volatility history for regime evaluation")
    metrics: list[RegimeMetric] = []
    for (family_root, regime), group in eligible.groupby(
        ["family_root", "volatility_regime"], sort=True, observed=True
    ):
        values = _metrics(
            group[target_column].to_numpy(dtype=float),
            group[prediction_column].to_numpy(dtype=float),
        )
        metrics.append(
            RegimeMetric(
                family_root=str(family_root),
                volatility_regime=str(regime),
                rows=len(group),
                sessions=int(_independent_session_keys(group).nunique()),
                mae=values[0],
                rmse=values[1],
                directional_accuracy=values[2],
            )
        )
    return RegimeEvaluationReport(
        prediction_column=prediction_column,
        target_column=target_column,
        lookback_minutes=lookback_minutes,
        minimum_history_sessions=minimum_history_sessions,
        metrics=tuple(metrics),
    )
