import copy
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.closing_tape.model_artifact import (
    MODEL_ARTIFACT_FORMAT,
    benchmark_frozen_model_inference,
    fit_promoted_frozen_model,
    load_frozen_model_artifact,
    parse_frozen_model_artifact,
)
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH


SURFACE_ARTIFACT_SHA256 = "c" * 64
SURFACE_REPLAY_RECEIPT_SHA256 = "d" * 64


def _payload():
    count = len(MODEL_FEATURE_COLUMNS)
    return {
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
        "feature_columns": list(MODEL_FEATURE_COLUMNS),
        "preprocessing": {
            "imputer_median": [0.0] * count,
            "scaler_mean": [0.0] * count,
            "scaler_scale": [1.0] * count,
        },
        "target": {"mean": 0.25, "scale": 2.0},
        "training": {
            "rows": 1, "epochs": 1, "seed": 17, "device": "cpu",
            "source_sha256s": ["a" * 64],
            "label_source_artifact_sha256s": ["b" * 64],
            "surface_artifact_sha256": SURFACE_ARTIFACT_SHA256,
            "surface_replay_receipt_sha256": SURFACE_REPLAY_RECEIPT_SHA256,
        },
        "layers": [
            {"weight": [[0.0] * count, [0.0] * count], "bias": [0.0, 0.0]},
            {"weight": [[0.0, 0.0]], "bias": [0.5]},
        ],
    }


def test_safe_json_artifact_predicts_in_declared_order_and_imputes():
    artifact = parse_frozen_model_artifact(_payload())
    frame = pd.DataFrame(
        [{column: (np.nan if index == 0 else float(index)) for index, column in enumerate(MODEL_FEATURE_COLUMNS)}]
    )

    prediction = artifact.predict_log_return(frame, device="cpu")

    assert prediction.tolist() == pytest.approx([1.25])
    assert artifact.feature_columns == MODEL_FEATURE_COLUMNS
    assert artifact.source_sha256s == ("a" * 64,)
    assert artifact.label_source_artifact_sha256s == ("b" * 64,)
    assert artifact.surface_artifact_sha256 == SURFACE_ARTIFACT_SHA256
    assert artifact.surface_replay_receipt_sha256 == SURFACE_REPLAY_RECEIPT_SHA256


def test_artifact_rejects_feature_contract_or_weight_dimension_drift():
    wrong_order = _payload()
    wrong_order["feature_columns"] = list(reversed(MODEL_FEATURE_COLUMNS))
    with pytest.raises(ValueError, match="columns or order"):
        parse_frozen_model_artifact(wrong_order)

    wrong_weight = copy.deepcopy(_payload())
    wrong_weight["layers"][0]["weight"] = [[0.0]]
    with pytest.raises(ValueError, match="input dimension"):
        parse_frozen_model_artifact(wrong_weight)


def test_artifact_predict_rejects_missing_inputs():
    artifact = parse_frozen_model_artifact(_payload())
    with pytest.raises(ValueError, match="model input columns missing"):
        artifact.predict_log_return(pd.DataFrame({MODEL_FEATURE_COLUMNS[0]: [1.0]}))


def test_artifact_rejects_missing_or_noncanonical_training_provenance():
    missing = _payload()
    missing["training"].pop("source_sha256s")
    with pytest.raises(ValueError, match="source_sha256s"):
        parse_frozen_model_artifact(missing)

    unsorted = _payload()
    unsorted["training"]["source_sha256s"] = ["b" * 64, "a" * 64]
    with pytest.raises(ValueError, match="sorted unique"):
        parse_frozen_model_artifact(unsorted)

    missing_surface = _payload()
    missing_surface["training"].pop("surface_replay_receipt_sha256")
    with pytest.raises(ValueError, match="surface_replay_receipt_sha256"):
        parse_frozen_model_artifact(missing_surface)


def _promoted_report(promoted=True, label_hashes=(), source_hashes=()):
    return SimpleNamespace(
        promoted=promoted,
        features=MODEL_FEATURE_COLUMNS,
        hidden_units=(4, 2),
        folds=(SimpleNamespace(epochs_trained=2), SimpleNamespace(epochs_trained=4)),
        device="cpu",
        source_sha256s=tuple(source_hashes),
        label_source_artifact_sha256s=tuple(label_hashes),
    )


def test_promoted_final_fit_is_deterministic_writable_and_loadable(tmp_path):
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
    frame = pd.DataFrame(rows)
    label_hashes = tuple(frame["close_source_artifact_sha256"])
    source_hashes = tuple(frame["source_sha256"])
    first = fit_promoted_frozen_model(
        frame,
        _promoted_report(label_hashes=label_hashes, source_hashes=source_hashes),
        surface_artifact_sha256=SURFACE_ARTIFACT_SHA256,
        surface_replay_receipt_sha256=SURFACE_REPLAY_RECEIPT_SHA256,
        seed=31,
    )
    second = fit_promoted_frozen_model(
        frame,
        _promoted_report(label_hashes=label_hashes, source_hashes=source_hashes),
        surface_artifact_sha256=SURFACE_ARTIFACT_SHA256,
        surface_replay_receipt_sha256=SURFACE_REPLAY_RECEIPT_SHA256,
        seed=31,
    )

    assert first.sha256 == second.sha256
    assert first.json_text == second.json_text
    assert first.epochs == 3
    path = first.write(tmp_path / "candidate.json")
    loaded = load_frozen_model_artifact(path)
    assert loaded.source_sha256s == tuple(sorted(source_hashes))
    assert loaded.label_source_artifact_sha256s == tuple(sorted(label_hashes))
    assert loaded.surface_artifact_sha256 == SURFACE_ARTIFACT_SHA256
    assert loaded.surface_replay_receipt_sha256 == SURFACE_REPLAY_RECEIPT_SHA256
    prediction = loaded.predict_log_return(frame.iloc[:3], device="cpu")
    assert prediction.shape == (3,)
    assert np.isfinite(prediction).all()


def test_final_fit_refuses_unpromoted_evaluation():
    with pytest.raises(ValueError, match="did not authorize"):
        fit_promoted_frozen_model(
            pd.DataFrame(),
            _promoted_report(promoted=False),
            surface_artifact_sha256=SURFACE_ARTIFACT_SHA256,
            surface_replay_receipt_sha256=SURFACE_REPLAY_RECEIPT_SHA256,
        )


def test_final_fit_rejects_missing_surface_replay_provenance():
    with pytest.raises(ValueError, match="surface replay provenance"):
        fit_promoted_frozen_model(
            pd.DataFrame(),
            _promoted_report(),
            surface_artifact_sha256="invalid",
            surface_replay_receipt_sha256=SURFACE_REPLAY_RECEIPT_SHA256,
        )


def test_inference_benchmark_measures_end_to_end_cpu_path():
    artifact = parse_frozen_model_artifact(_payload())
    frame = pd.DataFrame(
        [{column: float(index) for index, column in enumerate(MODEL_FEATURE_COLUMNS)}]
        * 5
    )

    report = benchmark_frozen_model_inference(
        artifact, frame, repeats=2, warmup_repeats=1, compare_cuda=False
    )

    assert report.rows == 5
    assert report.repeats == 2
    assert report.cpu_median_milliseconds > 0
    assert report.cuda_median_milliseconds is None
    assert report.numerically_equivalent is None
