import numpy as np
import pytest

from backend.closing_tape.regimes import (
    build_online_volatility_regimes,
    evaluate_predictions_by_regime,
)
from backend.closing_tape.research import build_forward_return_labels
from tests.test_closing_tape_research import _features


def _predictions():
    labeled = build_forward_return_labels(_features(session_count=14, minutes=25), horizon_minutes=1)
    labeled["candidate_prediction"] = labeled["flow_signal"] * 0.95
    return labeled


def test_regime_thresholds_do_not_use_future_sessions():
    frame = _predictions()
    before = build_online_volatility_regimes(
        frame, lookback_minutes=5, minimum_history_sessions=5
    )
    changed = frame.copy()
    mask = changed["session_id"] == "s13"
    changed.loc[mask, "reference_price"] *= np.linspace(1.0, 2.0, mask.sum())
    after = build_online_volatility_regimes(
        changed, lookback_minutes=5, minimum_history_sessions=5
    )

    columns = ["regime_low_threshold", "regime_high_threshold", "volatility_regime"]
    assert before[before["session_id"] == "s12"][columns].equals(
        after[after["session_id"] == "s12"][columns]
    )
    assert before[before["session_id"] == "s00"]["volatility_regime"].isna().all()


def test_regime_report_is_family_and_state_explicit():
    report = evaluate_predictions_by_regime(
        _predictions(),
        prediction_column="candidate_prediction",
        lookback_minutes=5,
        minimum_history_sessions=5,
    )

    assert report.metrics
    assert {metric.family_root for metric in report.metrics} == {"SPX"}
    assert {metric.volatility_regime for metric in report.metrics} <= {
        "calm", "normal", "stressed"
    }
    assert all(metric.rows > 0 for metric in report.metrics)


def test_regimes_reject_same_day_capture_retries():
    frame = _predictions()
    frame["trading_date"] = frame["minute_utc"].dt.date.astype(str)
    frame.loc[frame["session_id"] == "s01", "trading_date"] = "2026-07-01"

    with pytest.raises(ValueError, match="multiple capture sessions share a trading date"):
        build_online_volatility_regimes(
            frame, lookback_minutes=5, minimum_history_sessions=1,
        )
