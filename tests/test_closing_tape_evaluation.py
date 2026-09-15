import pandas as pd
import pytest

from backend.closing_tape import evaluation
from backend.closing_tape.surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    build_contract_surface_features,
)
from tests.closing_tape_authority_helpers import (
    issued_replay_surface,
    make_torch_report,
)
from tests.test_closing_tape_surface import _rows
from tests.test_closing_tape_training import _closes


def test_guarded_evaluation_does_not_touch_torch_when_not_ready(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("torch evaluator must not be called")

    monkeypatch.setattr(evaluation, "_evaluate_torch", forbidden)

    result = evaluation.evaluate_close_models_if_ready(
        pd.DataFrame(),
        _closes(),
        device="auto",
    )

    assert not result.trained
    assert result.evaluation is None
    assert result.evaluation_report is None
    assert result.labeled_training_frame is None
    assert result.model_feature_contract_hash == MODEL_FEATURE_CONTRACT_HASH
    assert result.device_requested == "auto"
    assert result.device_effective == "cpu"
    assert result.surface_artifact_sha256 is None
    assert result.surface_replay_receipt_sha256 is None
    assert "no eligible research surface rows" in result.reasons


def test_guarded_evaluation_rejects_explicit_cuda_without_replay_proof(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("training readiness must not be touched")

    monkeypatch.setattr(evaluation, "prepare_close_training_dataset", forbidden)

    with pytest.raises(ValueError, match="replay-verified surface"):
        evaluation.evaluate_close_models_if_ready(
            build_contract_surface_features(_rows()), _closes(), device="cuda"
        )


def test_guarded_evaluation_forces_unverified_auto_to_cpu(monkeypatch):
    captured = {}

    def fake(_frame, **kwargs):
        captured.update(kwargs)
        return make_torch_report(device="cpu")

    monkeypatch.setattr(evaluation, "_evaluate_torch", fake)
    result = evaluation.evaluate_close_models_if_ready(
        build_contract_surface_features(_rows()),
        _closes(),
        expected_families=("SPX",),
        minimum_sessions=1,
        minimum_family_sessions=1,
        min_train_sessions=0,
        validation_sessions=0,
        device="auto",
    )

    assert result.trained
    assert captured["device"] == "cpu"
    assert result.device_requested == "auto"
    assert result.device_effective == "cpu"


def test_guarded_evaluation_passes_identical_frozen_features_to_cuda(
    tmp_path, monkeypatch
):
    captured = {}

    def fake(frame, **kwargs):
        captured["rows"] = len(frame)
        captured.update(kwargs)
        return make_torch_report(promoted=False, device="cuda")

    monkeypatch.setattr(evaluation, "_evaluate_torch", fake)
    surface = issued_replay_surface(
        build_contract_surface_features(_rows()), tmp_path, monkeypatch
    )

    result = evaluation.evaluate_close_models_if_ready(
        surface,
        _closes(),
        expected_families=("SPX",),
        minimum_sessions=1,
        minimum_family_sessions=1,
        min_train_sessions=0,
        validation_sessions=0,
        device="cuda",
    )

    assert result.trained
    assert captured["rows"] == 1
    assert captured["feature_columns"] == MODEL_FEATURE_COLUMNS
    assert captured["device"] == "cuda"
    assert captured["horizon_minutes"] == 15
    assert result.evaluation == result.evaluation_report.to_dict()
    assert result.evaluation_report is not None
    assert result.evaluation_report.to_dict() == result.evaluation
    assert result.labeled_training_frame is not None
    assert len(result.labeled_training_frame) == 1
    assert result.replay_verified_surface is surface
    assert result.surface_artifact_sha256 == surface.artifact_sha256
    assert result.surface_replay_receipt_sha256 == surface.replay_receipt_sha256
    assert result.to_dict()["surface_replay_receipt_sha256"] == surface.replay_receipt_sha256
    assert "evaluation_report" not in result.to_dict()
    assert "labeled_training_frame" not in result.to_dict()


def test_guarded_evaluation_rehashes_replay_receipt_before_cuda(
    tmp_path, monkeypatch
):
    surface = issued_replay_surface(
        build_contract_surface_features(_rows()), tmp_path, monkeypatch
    )
    surface.replay_verification["semantic_equality_verified"] = False

    with pytest.raises(ValueError, match="receipt identity"):
        evaluation.evaluate_close_models_if_ready(surface, _closes(), device="cuda")
