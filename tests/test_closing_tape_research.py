from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from backend.closing_tape.research import (
    build_forward_return_labels,
    build_session_close_labels,
    compare_precomputed_predictions,
    evaluate_ridge_walk_forward,
    purged_session_walk_forward,
)


UTC = timezone.utc


def _features(session_count=12, minutes=20):
    rows = []
    origin = datetime(2026, 7, 1, 14, 30, tzinfo=UTC)
    for session in range(session_count):
        start = origin + timedelta(days=session)
        price = 6000.0
        for minute in range(minutes):
            signal = ((session * minutes + minute) % 7 - 3) / 10_000.0
            rows.append(
                {
                    "session_id": f"s{session:02d}",
                    "family_root": "SPX",
                    "minute_utc": start + timedelta(minutes=minute),
                    "reference_price": price,
                    "flow_signal": signal,
                    "inference_method": "trade_price_vs_pretrade_nbbo",
                    "inference_version": "1.0",
                    "source_sha256": f"{session:064x}",
                    "capture_integrity_verified": True,
                }
            )
            price *= np.exp(signal)
    return pd.DataFrame(rows)


def test_labels_use_exact_future_timestamp_and_never_cross_session():
    labeled = build_forward_return_labels(_features(session_count=2, minutes=5), horizon_minutes=2)

    assert len(labeled) == 6
    assert (labeled["feature_available_at_utc"] - labeled["minute_utc"] == pd.Timedelta(minutes=1)).all()
    assert (
        labeled["target_time_utc"] - labeled["feature_available_at_utc"]
        == pd.Timedelta(minutes=2)
    ).all()
    assert (labeled["label_available_at_utc"] == labeled["target_time_utc"]).all()
    assert np.isfinite(labeled["target_log_return"]).all()


def test_walk_forward_is_chronological_and_purges_unavailable_labels():
    labeled = build_forward_return_labels(_features(), horizon_minutes=1)

    for train, test in purged_session_walk_forward(labeled, min_train_sessions=5):
        assert set(labeled.iloc[train]["session_id"]).isdisjoint(labeled.iloc[test]["session_id"])
        assert (
            labeled.iloc[train]["label_available_at_utc"].max()
            < labeled.iloc[test]["feature_available_at_utc"].min() - pd.Timedelta(minutes=5)
        )


def test_walk_forward_embargo_is_configurable_and_rejects_negative_values():
    labeled = build_forward_return_labels(_features(), horizon_minutes=1)
    train, test = next(
        purged_session_walk_forward(
            labeled,
            min_train_sessions=5,
            embargo_seconds=900,
        )
    )

    assert (
        labeled.iloc[train]["label_available_at_utc"].max()
        < labeled.iloc[test]["feature_available_at_utc"].min() - pd.Timedelta(minutes=15)
    )
    with pytest.raises(ValueError, match="embargo_seconds"):
        list(
            purged_session_walk_forward(
                labeled,
                min_train_sessions=5,
                embargo_seconds=-1,
            )
        )


def test_walk_forward_rejects_same_day_capture_retries():
    features = _features(session_count=3)
    features["trading_date"] = features["minute_utc"].dt.date.astype(str)
    features.loc[features["session_id"] == "s01", "trading_date"] = "2026-07-01"
    labeled = build_forward_return_labels(features, horizon_minutes=1)

    with pytest.raises(ValueError, match="multiple capture sessions share a trading date"):
        list(purged_session_walk_forward(labeled, min_train_sessions=1))


def test_cpu_baseline_report_is_out_of_sample_and_auditable():
    labeled = build_forward_return_labels(_features(), horizon_minutes=1)

    report = evaluate_ridge_walk_forward(
        labeled,
        feature_columns=["flow_signal"],
        horizon_minutes=1,
        min_train_sessions=5,
    )

    assert len(report.folds) == 7
    assert report.embargo_seconds == 300.0
    assert report.independent_sessions == 12
    assert report.candidate_mae < report.zero_return_mae
    assert report.mae_improvement_pct > 2.0
    assert report.mae_improvement_ci_low_pct > 0
    assert report.mae_improvement_ci_high_pct >= report.mae_improvement_ci_low_pct
    assert [metric.family_root for metric in report.family_metrics] == ["SPX"]
    assert report.family_metrics[0].candidate_mae < report.family_metrics[0].zero_return_mae
    assert report.promoted


def test_multi_session_gate_abstains_before_model_fit(monkeypatch):
    labeled = build_forward_return_labels(
        _features(session_count=7),
        horizon_minutes=1,
    )

    from sklearn.pipeline import Pipeline

    fit_calls = []
    monkeypatch.setattr(Pipeline, "fit", lambda *args, **kwargs: fit_calls.append(args))
    with pytest.raises(ValueError, match="before model fitting"):
        evaluate_ridge_walk_forward(
            labeled,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            min_train_sessions=5,
        )
    assert fit_calls == []


def test_future_or_label_columns_are_rejected_as_features():
    labeled = build_forward_return_labels(_features(), horizon_minutes=1)

    with pytest.raises(ValueError, match="future/label"):
        evaluate_ridge_walk_forward(
            labeled,
            feature_columns=["target_price"],
            horizon_minutes=1,
            min_train_sessions=5,
        )


def test_family_regression_blocks_aggregate_promotion():
    spx = _features()
    good = []
    for family_root in ("SPX", "NDX", "RUT", "SPY"):
        family = spx.copy()
        family["family_root"] = family_root
        good.append(family)
    vix = spx.copy()
    vix["family_root"] = "VIX"
    vix["reference_price"] = 20.0
    labeled = build_forward_return_labels(pd.concat([*good, vix], ignore_index=True), horizon_minutes=1)

    report = evaluate_ridge_walk_forward(
        labeled,
        feature_columns=["flow_signal"],
        horizon_minutes=1,
        min_train_sessions=5,
    )

    assert report.mae_improvement_pct > 2.0
    assert not report.promoted
    assert any("VIX MAE did not beat" in reason for reason in report.promotion_reasons)


def test_close_labels_compare_marketpin_on_the_same_end_of_session_target():
    features = _features(session_count=2, minutes=5)
    features["trading_date"] = features["minute_utc"].dt.date.astype(str)
    features["predicted_close"] = features["reference_price"] * 1.0002
    closes = pd.DataFrame(
        [
            {
                "family_root": "SPX",
                "trading_date": day,
                "actual_close": group.iloc[-1]["reference_price"],
                "close_label_available_at_utc": group.iloc[-1]["minute_utc"] + timedelta(minutes=2),
            }
            for day, group in features.groupby("trading_date")
        ]
    )

    labeled = build_session_close_labels(features, closes)
    report = compare_precomputed_predictions(
        labeled,
        prediction_columns=["marketpin_predicted_log_return"],
    )

    assert len(labeled) == 10
    assert report.metrics[0].rows == 10
    assert report.metrics[0].sessions == 2
    assert report.metrics[0].mae >= 0
    assert len(report.family_metrics) == 1
    assert report.family_metrics[0].family_root == "SPX"
