from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.closing_tape import evaluation
from backend.closing_tape.candidate import (
    load_candidate_package_receipt,
    load_paper_candidate_activation_receipt,
    package_promoted_candidate,
    write_paper_candidate_descriptor,
)
from backend.closing_tape.surface import (
    MODEL_FEATURE_COLUMNS,
    MODEL_FEATURE_CONTRACT_HASH,
    build_contract_surface_features,
)
from backend.closing_tape.evaluation import GuardedModelEvaluationResult
from tests.closing_tape_authority_helpers import (
    issued_replay_surface,
    make_torch_report,
)
from backend.closing_tape.torch_research import TorchOutOfSamplePrediction
from tests.test_closing_tape_surface import _rows


UTC = timezone.utc
FAMILIES = ("SPX", "NDX", "RUT", "VIX", "SPY")


def _evaluation_result(
    tmp_path,
    monkeypatch,
    *,
    promoted=True,
    replay_verified=True,
):
    rows = []
    for row_number in range(24):
        row = {
            column: float(row_number + feature_number) / 100.0
            for feature_number, column in enumerate(MODEL_FEATURE_COLUMNS)
        }
        row["target_log_return"] = (row_number - 12) / 1000.0
        row["source_sha256"] = f"{row_number + 1:064x}"
        row["close_source_artifact_sha256"] = f"{1000 + row_number:064x}"
        rows.append(row)
    origin = datetime(2026, 6, 1, 19, 45, tzinfo=UTC)
    predictions = []
    for session in range(2):
        for family_index, family in enumerate(FAMILIES):
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
    report = make_torch_report(
        promoted=promoted,
        source_sha256s=tuple(row["source_sha256"] for row in rows),
        label_source_artifact_sha256s=tuple(
            row["close_source_artifact_sha256"] for row in rows
        ),
        predictions=tuple(predictions),
    )
    labeled = pd.DataFrame(rows)

    class Ready:
        ready = True
        reasons = ()

        def to_dict(self):
            return {"ready": True}

    monkeypatch.setattr(
        evaluation,
        "prepare_close_training_dataset",
        lambda *_args, **_kwargs: (labeled.copy(), Ready()),
    )
    monkeypatch.setattr(evaluation, "_evaluate_torch", lambda *_args, **_kwargs: report)
    surface = build_contract_surface_features(_rows())
    evaluation_surface = (
        issued_replay_surface(surface, tmp_path, monkeypatch)
        if replay_verified
        else surface
    )
    return evaluation.evaluate_close_models_if_ready(
        evaluation_surface,
        pd.DataFrame(),
        minimum_sessions=2,
        minimum_family_sessions=1,
        min_train_sessions=0,
        validation_sessions=0,
        device="cpu",
    )


def test_promoted_candidate_packages_artifact_and_bound_calibration(
    tmp_path, monkeypatch
):
    destination = tmp_path / "models" / "candidate-v1.json"

    package = package_promoted_candidate(
        _evaluation_result(tmp_path, monkeypatch),
        version="candidate-v1", artifact_path=destination,
        calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
        minimum_family_sessions=2,
    )

    assert destination.is_file()
    assert package.artifact_sha256 == package.deployment_calibration.artifact_sha256
    assert package.version == package.deployment_calibration.model_version
    assert package.training_rows == 24
    assert len(package.source_sha256s) == 24
    assert len(package.label_source_artifact_sha256s) == 24
    assert len(package.surface_artifact_sha256) == 64
    assert len(package.surface_replay_receipt_sha256) == 64
    assert len(package.deployment_calibration.family_radii) == 5
    assert len(package.deployment_calibration.label_source_artifact_sha256s) == 10
    receipt_path = destination.parent / "candidate_packages" / (
        f"{package.package_receipt_sha256}.json"
    )
    assert receipt_path.resolve() == Path(package.package_receipt_path)
    receipt = load_candidate_package_receipt(receipt_path)
    assert receipt["artifact_path"] == destination.name
    assert receipt["artifact_sha256"] == package.artifact_sha256
    assert tuple(receipt["source_sha256s"]) == package.source_sha256s
    assert receipt["surface_artifact_sha256"] == package.surface_artifact_sha256
    assert (
        receipt["surface_replay_receipt_sha256"]
        == package.surface_replay_receipt_sha256
    )
    descriptor = write_paper_candidate_descriptor(tmp_path, package)
    assert descriptor.is_file()
    descriptor_payload = json.loads(descriptor.read_text(encoding="utf-8"))
    activation = load_paper_candidate_activation_receipt(
        destination.parent / descriptor_payload["activation_receipt_path"]
    )
    assert activation["candidate_package_sha256"] == package.package_receipt_sha256
    assert activation["artifact_sha256"] == package.artifact_sha256


def test_candidate_packaging_writes_nothing_when_evidence_fails(
    tmp_path, monkeypatch
):
    destination = tmp_path / "candidate-v1.json"
    with pytest.raises(ValueError, match="sessions 2 < 3"):
        package_promoted_candidate(
            _evaluation_result(tmp_path, monkeypatch),
            version="candidate-v1", artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=3,
        )
    assert not destination.exists()

    with pytest.raises(ValueError, match="did not promote"):
        package_promoted_candidate(
            _evaluation_result(tmp_path, monkeypatch, promoted=False), version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )


def test_candidate_packaging_requires_live_rehashed_replay_proof(
    tmp_path, monkeypatch
):
    destination = tmp_path / "candidate-v1.json"
    with pytest.raises(ValueError, match="guarded evaluation result"):
        package_promoted_candidate(
            SimpleNamespace(trained=True),
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    with pytest.raises(ValueError, match="guarded evaluation result"):
        package_promoted_candidate(
            object.__new__(GuardedModelEvaluationResult),
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    with pytest.raises(ValueError, match="replay-verified surface evidence"):
        package_promoted_candidate(
            _evaluation_result(tmp_path, monkeypatch, replay_verified=False),
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    assert not destination.exists()

    tampered = _evaluation_result(tmp_path, monkeypatch)
    tampered.replay_verified_surface.replay_verification[
        "semantic_equality_verified"
    ] = False
    with pytest.raises(ValueError, match="authoritative guarded evaluation"):
        package_promoted_candidate(
            tampered,
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    assert not destination.exists()

    changed_summary = _evaluation_result(tmp_path, monkeypatch)
    changed_summary.evaluation["promoted"] = False
    with pytest.raises(ValueError, match="authoritative guarded evaluation"):
        package_promoted_candidate(
            changed_summary,
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    assert not destination.exists()

    mutated = _evaluation_result(tmp_path, monkeypatch)
    mutated.labeled_training_frame.loc[
        mutated.labeled_training_frame.index[0], "target_log_return"
    ] += 1.0
    with pytest.raises(ValueError, match="authoritative guarded evaluation"):
        package_promoted_candidate(
            mutated,
            version="candidate-v1",
            artifact_path=destination,
            calibration_asof_utc=datetime(2026, 6, 3, tzinfo=UTC),
            minimum_family_sessions=2,
        )
    assert not destination.exists()
