import logging
from types import SimpleNamespace

import pytest
import torch

from backend import inference
from backend.databento_streamer import gamma_structure_inference_features


def test_live_feature_builder_adds_gamma_structure_without_fabricating_volume():
    payload = {
        "spot_last": 100.0,
        "zero_gamma_level": 102.0,
        "positive_gex_wall": 105.0,
        "negative_gex_wall": 98.0,
        "gross_gex": 1_000.0,
        "top_strikes_by_abs_gex": [{"abs_gex": 400.0}],
    }

    features = inference.build_live_inference_features(payload)

    assert features["price"] == pytest.approx(100.0)
    assert features["volume"] is None
    assert features["zero_gamma_distance"] == pytest.approx(0.02)
    assert features["wall_asymmetry"] == pytest.approx(3.0 / 7.0)
    assert features["top_strike_concentration"] == pytest.approx(0.4)


def test_live_feature_builder_preserves_missing_structure_as_missing():
    features = inference.build_live_inference_features({"spot_last": 100.0})

    assert features["price"] == 100.0
    assert features["volume"] is None
    assert features["zero_gamma_distance"] is None
    assert features["wall_asymmetry"] is None
    assert features["top_strike_concentration"] is None


def test_streamer_and_inference_feature_formulas_match():
    streamer_features = gamma_structure_inference_features(
        spot=100.0,
        zero_gamma=102.0,
        positive_gex_wall=105.0,
        negative_gex_wall=98.0,
        top_strike_concentration=0.4,
    )
    inference_features = inference.build_live_inference_features(
        {
            "spot_last": 100.0,
            "inference_features": streamer_features,
        }
    )

    for name, expected in streamer_features.items():
        assert inference_features[name] == pytest.approx(expected)


def test_prepare_features_abstains_instead_of_imputing_missing_gex():
    engine = object.__new__(inference.CUDAInferenceEngine)
    engine.feature_names = {
        "SPX": ["price", "volume", "zero_gamma_distance"]
    }
    engine.scalers = {"SPX": {}}
    engine.device = torch.device("cpu")

    prepared = engine._prepare_features(
        "SPX",
        {"price": 100.0, "volume": 1_000.0, "zero_gamma_distance": None},
    )

    assert prepared is None


@pytest.mark.parametrize("device_type", ["cpu", "cuda"])
def test_no_artifact_log_names_the_actual_runtime(monkeypatch, tmp_path, caplog, device_type):
    engine = SimpleNamespace(
        device=torch.device(device_type),
        models={},
        warmup=lambda: pytest.fail("no-artifact startup must not warm up"),
    )
    monkeypatch.setattr(inference, "get_inference_engine", lambda: engine)
    monkeypatch.setattr(inference, "MODELS_DIR", tmp_path)

    with caplog.at_level(logging.WARNING, logger=inference.__name__):
        loaded = inference.load_all_models()

    assert loaded == []
    assert f"{device_type.upper()} inference runtime initialized" in caplog.text
    assert "trained-model inference remains disabled" in caplog.text
