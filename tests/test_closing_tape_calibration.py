import numpy as np
import pandas as pd
import pytest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from backend.closing_tape.calibration import (
    bind_candidate_oos_predictions,
    build_online_conformal_intervals,
    evaluate_online_conformal_intervals,
    fit_deployment_conformal_calibration,
)
from backend.closing_tape.research import build_forward_return_labels
from backend.closing_tape.torch_research import TorchOutOfSamplePrediction
from tests.test_closing_tape_research import _features


def _predictions(session_count=14):
    labeled = build_forward_return_labels(
        _features(session_count=session_count, minutes=20), horizon_minutes=1
    )
    labeled["candidate_prediction"] = labeled["flow_signal"] * 0.95
    return labeled


def test_online_intervals_use_only_prior_labeled_sessions():
    frame = _predictions()
    before = build_online_conformal_intervals(
        frame,
        prediction_column="candidate_prediction",
        minimum_calibration_sessions=5,
    )
    changed = frame.copy()
    changed.loc[changed["session_id"] == "s13", "target_log_return"] += 1.0
    after = build_online_conformal_intervals(
        changed,
        prediction_column="candidate_prediction",
        minimum_calibration_sessions=5,
    )

    earlier = before[before["session_id"] == "s12"]["interval_radius"].to_numpy()
    earlier_after = after[after["session_id"] == "s12"]["interval_radius"].to_numpy()
    assert np.array_equal(earlier, earlier_after)
    assert before[before["session_id"] == "s00"]["interval_radius"].isna().all()
    assert before[before["session_id"] == "s05"]["interval_radius"].notna().all()


def test_online_conformal_report_is_calibrated_and_family_explicit():
    report = evaluate_online_conformal_intervals(
        _predictions(),
        prediction_column="candidate_prediction",
        alpha=0.1,
        minimum_calibration_sessions=5,
    )

    assert report.eligible_rows > 0
    assert report.uncalibrated_rows > 0
    assert report.overall.target_coverage == 0.9
    assert report.overall.empirical_coverage >= 0.8
    assert [metric.family_root for metric in report.family_metrics] == ["SPX"]


def test_online_conformal_refuses_to_invent_range_without_history():
    try:
        evaluate_online_conformal_intervals(
            _predictions(session_count=4),
            prediction_column="candidate_prediction",
            minimum_calibration_sessions=5,
        )
    except ValueError as exc:
        assert "not enough prior labeled sessions" in str(exc)
    else:
        raise AssertionError("uncalibrated interval was reported as valid")


def test_calibration_rejects_same_day_capture_retries():
    frame = _predictions()
    frame["trading_date"] = frame["minute_utc"].dt.date.astype(str)
    frame.loc[frame["session_id"] == "s01", "trading_date"] = "2026-07-01"

    with pytest.raises(ValueError, match="multiple capture sessions share a trading date"):
        build_online_conformal_intervals(
            frame, prediction_column="candidate_prediction",
            minimum_calibration_sessions=1,
        )


def _deployment_rows(session_count=21):
    rows = []
    origin = datetime(2026, 6, 1, 20, 1, tzinfo=timezone.utc)
    for session in range(session_count):
        day = (origin + timedelta(days=session)).date().isoformat()
        for family_index, family in enumerate(("SPX", "NDX", "RUT", "VIX", "SPY")):
            error = (session + family_index + 1) / 10000.0
            rows.append(
                {
                    "session_id": f"s{session}", "trading_date": day,
                    "family_root": family,
                    "label_available_at_utc": origin + timedelta(days=session),
                    "source_sha256": f"{session + 1:064x}",
                    "close_source_artifact_sha256": f"{1000 + session * 5 + family_index:064x}",
                    "model_version": "candidate-v1", "artifact_sha256": "a" * 64,
                    "prediction_is_out_of_sample": True,
                    "candidate_prediction": 0.001,
                    "target_log_return": 0.001 + error,
                }
            )
    return pd.DataFrame(rows)


def test_deployment_calibration_is_family_specific_oos_and_asof_bounded():
    frame = _deployment_rows()
    report = fit_deployment_conformal_calibration(
        frame, prediction_column="candidate_prediction",
        asof_utc=datetime(2026, 6, 20, 20, 1, tzinfo=timezone.utc),
        minimum_family_sessions=20,
    )

    assert report.model_version == "candidate-v1"
    assert report.target_coverage == 0.9
    assert report.eligible_rows == 100
    assert report.excluded_future_rows == 5
    assert len(report.label_source_artifact_sha256s) == 100
    assert len(report.family_radii) == 5
    assert all(item.sessions == 20 and item.radius_log_return > 0 for item in report.family_radii)
    shuffled = fit_deployment_conformal_calibration(
        frame.sample(frac=1.0, random_state=17),
        prediction_column="candidate_prediction",
        asof_utc=datetime(2026, 6, 20, 20, 1, tzinfo=timezone.utc),
        minimum_family_sessions=20,
    )
    assert len(report.evidence_sha256) == 64
    assert shuffled.evidence_sha256 == report.evidence_sha256
    changed = frame.copy()
    changed.loc[0, "target_log_return"] += 0.0001
    changed_report = fit_deployment_conformal_calibration(
        changed, prediction_column="candidate_prediction",
        asof_utc=datetime(2026, 6, 20, 20, 1, tzinfo=timezone.utc),
        minimum_family_sessions=20,
    )
    assert changed_report.evidence_sha256 != report.evidence_sha256
    changed_artifact = frame.copy()
    changed_artifact.loc[0, "close_source_artifact_sha256"] = "f" * 64
    changed_artifact_report = fit_deployment_conformal_calibration(
        changed_artifact, prediction_column="candidate_prediction",
        asof_utc=datetime(2026, 6, 20, 20, 1, tzinfo=timezone.utc),
        minimum_family_sessions=20,
    )
    assert changed_artifact_report.evidence_sha256 != report.evidence_sha256


def test_deployment_calibration_rejects_in_sample_or_candidate_mixing():
    frame = _deployment_rows()
    frame.loc[0, "prediction_is_out_of_sample"] = False
    with pytest.raises(ValueError, match="only out-of-sample"):
        fit_deployment_conformal_calibration(
            frame, prediction_column="candidate_prediction",
            asof_utc=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )

    mixed = _deployment_rows()
    mixed.loc[0, "model_version"] = "other"
    with pytest.raises(ValueError, match="mixes candidate identity"):
        fit_deployment_conformal_calibration(
            mixed, prediction_column="candidate_prediction",
            asof_utc=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )


def test_deployment_calibration_rejects_duplicate_same_day_family_label():
    frame = _deployment_rows()
    duplicate = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate family rows"):
        fit_deployment_conformal_calibration(
            duplicate, prediction_column="candidate_prediction",
            asof_utc=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )


def test_walk_forward_predictions_bind_directly_to_frozen_candidate_calibration():
    predictions = []
    origin = datetime(2026, 6, 1, 19, 45, tzinfo=timezone.utc)
    for session in range(2):
        for family_index, family in enumerate(("SPX", "NDX", "RUT", "VIX", "SPY")):
            predictions.append(
                TorchOutOfSamplePrediction(
                    session_id=f"s{session}",
                    trading_date=(origin + timedelta(days=session)).date().isoformat(),
                    family_root=family,
                    source_sha256=f"{session + 1:064x}",
                    close_source_artifact_sha256=f"{1000 + session * 5 + family_index:064x}",
                    feature_available_at_utc=(origin + timedelta(days=session)).isoformat(),
                    label_available_at_utc=(origin + timedelta(days=session, minutes=15)).isoformat(),
                    reference_price=100.0 + family_index,
                    target_log_return=0.002 + session / 1000,
                    torch_predicted_log_return=0.001,
                    ridge_predicted_log_return=0.0,
                    persistence_predicted_log_return=0.0,
                    incumbent_predicted_log_return=0.0005,
                )
            )
    evaluation = SimpleNamespace(
        promoted=True, out_of_sample_predictions=tuple(predictions)
    )

    bound = bind_candidate_oos_predictions(
        evaluation, model_version="candidate-v1", artifact_sha256="a" * 64
    )
    calibration = fit_deployment_conformal_calibration(
        bound, prediction_column="torch_predicted_log_return",
        asof_utc=origin + timedelta(days=2), minimum_family_sessions=2,
    )

    assert bound["prediction_is_out_of_sample"].all()
    assert set(bound["model_version"]) == {"candidate-v1"}
    assert bound["close_source_artifact_sha256"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert calibration.eligible_rows == 10
    assert len(calibration.family_radii) == 5


def test_candidate_binding_rejects_unpromoted_evaluation():
    with pytest.raises(ValueError, match="only a promoted"):
        bind_candidate_oos_predictions(
            SimpleNamespace(promoted=False, out_of_sample_predictions=()),
            model_version="candidate-v1", artifact_sha256="a" * 64,
        )
