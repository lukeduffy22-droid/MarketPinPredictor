import pytest

from backend.closing_tape.research import build_forward_return_labels
from backend.closing_tape.torch_research import evaluate_torch_walk_forward
from tests.test_closing_tape_research import _features


def _retain_close_artifacts(frame):
    result = frame.copy()
    session_numbers = result["session_id"].astype(str).str.removeprefix("s").astype(int)
    result["close_source_artifact_sha256"] = session_numbers.map(
        lambda value: f"{value + 1000:064x}"
    )
    return result


def test_torch_walk_forward_uses_inner_validation_after_explicit_session_gate():
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(), horizon_minutes=1)
    )
    labeled["marketpin_predicted_log_return"] = 0.0

    report = evaluate_torch_walk_forward(
        labeled,
        feature_columns=["flow_signal"],
        horizon_minutes=1,
        min_train_sessions=5,
        validation_sessions=1,
        device="cpu",
        hidden_units=(8, 4),
        max_epochs=8,
        patience=2,
        batch_size=128,
        minimum_complete_sessions=10,
        minimum_sessions_per_family=10,
    )

    assert len(report.folds) == 7
    assert all(fold.validation_sessions == 1 for fold in report.folds)
    assert all(fold.train_sessions + fold.validation_sessions < 12 for fold in report.folds)
    assert report.complete_sessions == 12
    assert len(report.source_sha256s) == 12
    assert report.device == "cpu"
    assert report.hidden_units == (8, 4)
    assert [metric.family_root for metric in report.family_metrics] == ["SPX"]
    assert report.torch_improvement_over_ridge_ci_high_pct >= report.torch_improvement_over_ridge_ci_low_pct
    assert report.incumbent_rows > 0
    assert report.incumbent_rows == report.held_out_rows
    assert report.incumbent_sessions == report.held_out_sessions
    assert len(report.out_of_sample_predictions) == report.held_out_rows
    assert all(
        row.source_sha256
        and row.close_source_artifact_sha256
        and row.feature_available_at_utc
        and row.label_available_at_utc
        for row in report.out_of_sample_predictions
    )
    assert report.label_source_artifact_sha256s
    assert report.torch_improvement_over_incumbent_ci_high_pct >= report.torch_improvement_over_incumbent_ci_low_pct
    assert not report.promoted


def test_torch_walk_forward_does_not_import_or_fit_when_session_evidence_is_insufficient(
    monkeypatch,
):
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(session_count=12), horizon_minutes=1)
    )
    labeled["marketpin_predicted_log_return"] = 0.0

    def forbidden_import(name, *_args, **_kwargs):
        if name == "torch" or name.startswith("sklearn"):
            raise AssertionError(f"model dependency imported before evidence gate: {name}")
        return original_import(name, *_args, **_kwargs)

    import builtins

    original_import = builtins.__import__
    monkeypatch.setattr(builtins, "__import__", forbidden_import)

    with pytest.raises(ValueError, match="before model fitting"):
        evaluate_torch_walk_forward(
            labeled,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            device="cuda",
        )


def test_torch_walk_forward_checks_source_and_family_sessions_before_model_import(
    monkeypatch,
):
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(session_count=12), horizon_minutes=1)
    )
    labeled["marketpin_predicted_log_return"] = 0.0
    monkeypatch.setattr(
        "backend.closing_tape.torch_research._choose_device",
        lambda _device: (_ for _ in ()).throw(AssertionError("device/model path reached")),
    )

    same_source = labeled.copy()
    same_source["source_sha256"] = "a" * 64
    with pytest.raises(ValueError, match="independent source evidence before model fitting"):
        evaluate_torch_walk_forward(
            same_source,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            minimum_complete_sessions=10,
            minimum_sessions_per_family=5,
        )

    thin_family = labeled.copy()
    thin_family.loc[thin_family["session_id"].isin(["s00", "s01"]), "family_root"] = "NDX"
    with pytest.raises(ValueError, match="per-family session evidence before model fitting"):
        evaluate_torch_walk_forward(
            thin_family,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            minimum_complete_sessions=10,
            minimum_sessions_per_family=5,
        )


def test_torch_walk_forward_rejects_future_feature():
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(), horizon_minutes=1)
    )

    with pytest.raises(ValueError, match="future/label"):
        evaluate_torch_walk_forward(
            labeled,
            feature_columns=["target_price"],
            horizon_minutes=1,
            device="cpu",
        )


def test_torch_walk_forward_rejects_same_day_capture_retries():
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(session_count=3), horizon_minutes=1)
    )
    labeled["trading_date"] = labeled["minute_utc"].dt.date.astype(str)
    labeled.loc[labeled["session_id"] == "s01", "trading_date"] = "2026-07-01"

    with pytest.raises(ValueError, match="multiple capture sessions share a trading date"):
        evaluate_torch_walk_forward(
            labeled, feature_columns=["flow_signal"], horizon_minutes=1,
            min_train_sessions=2, device="cpu", max_epochs=1,
        )


def test_torch_walk_forward_rejects_missing_label_artifact_before_model_import(monkeypatch):
    labeled = build_forward_return_labels(_features(session_count=12), horizon_minutes=1)
    monkeypatch.setattr(
        "backend.closing_tape.torch_research._choose_device",
        lambda _device: (_ for _ in ()).throw(AssertionError("device/model path reached")),
    )

    with pytest.raises(ValueError, match="close_source_artifact_sha256"):
        evaluate_torch_walk_forward(
            labeled,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            minimum_complete_sessions=10,
            minimum_sessions_per_family=5,
        )


def test_torch_walk_forward_rejects_invalid_dbn_hash_before_model_import(monkeypatch):
    labeled = _retain_close_artifacts(
        build_forward_return_labels(_features(session_count=12), horizon_minutes=1)
    )
    labeled.loc[labeled["session_id"] == "s00", "source_sha256"] = "invalid"
    monkeypatch.setattr(
        "backend.closing_tape.torch_research._choose_device",
        lambda _device: (_ for _ in ()).throw(AssertionError("device/model path reached")),
    )

    with pytest.raises(ValueError, match="finalized DBN SHA-256"):
        evaluate_torch_walk_forward(
            labeled,
            feature_columns=["flow_signal"],
            horizon_minutes=1,
            minimum_complete_sessions=10,
            minimum_sessions_per_family=5,
        )
