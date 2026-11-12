import io
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
        note_missing_model=lambda *_args: None,
    )
    monkeypatch.setattr(inference, "get_inference_engine", lambda: engine)
    monkeypatch.setattr(inference, "MODELS_DIR", tmp_path)

    with caplog.at_level(logging.WARNING, logger=inference.__name__):
        loaded = inference.load_all_models(enabled=True)

    assert loaded == []
    assert f"{device_type.upper()} inference runtime initialized" in caplog.text
    assert "trained-model inference remains disabled" in caplog.text


def _checkpoint(*, include_output_contract: bool) -> dict:
    model = inference.GammaPredictorModel(input_dim=2, hidden_dims=[4])
    checkpoint = {
        "config": {"input_dim": 2, "hidden_dims": [4]},
        "features": ["price", "zero_gamma_distance"],
        "feature_schema_version": inference.GEX_INFERENCE_FEATURE_SCHEMA_VERSION,
        "scaler": {
            "kind": "standard",
            "mean": [100.0, 0.0],
            "std": [10.0, 0.1],
        },
        "model_state_dict": model.state_dict(),
    }
    if include_output_contract:
        checkpoint["output_contract"] = {
            "schema_version": inference.CUDA_SHADOW_OUTPUT_CONTRACT_VERSION,
            "kind": "future_price",
            "units": "index_points",
            "prediction_horizon_seconds": 300,
            "research_only": True,
            "promotion_allowed": False,
            "max_abs_move_bps": 250.0,
        }
    return checkpoint


def test_model_loader_rejects_missing_output_contract_with_evidence(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(_checkpoint(include_output_contract=False), path)
    engine = inference.CUDAInferenceEngine()

    loaded = engine.load_model("SPX", path)

    assert loaded is False
    assert "SPX" not in engine.models
    evidence = engine.get_model_load_evidence("SPX")
    assert evidence["status"] == "REJECTED"
    assert evidence["research_only"] is True
    assert evidence["promotion_allowed"] is False
    assert evidence["artifact_sha256"]
    assert "output contract" in evidence["error"]


def test_contract_complete_model_loads_shadow_only_and_reports_evidence(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(_checkpoint(include_output_contract=True), path)
    engine = inference.CUDAInferenceEngine()

    loaded = engine.load_model("SPX", path)

    assert loaded is True
    evidence = engine.get_model_load_evidence("SPX")
    assert evidence["status"] == "LOADED_SHADOW_ONLY"
    assert evidence["research_only"] is True
    assert evidence["promotion_allowed"] is False
    assert evidence["output_contract"]["kind"] == "future_price"
    result = engine.predict(
        "spx", {"price": 100.0, "zero_gamma_distance": 0.01}
    )
    assert result is not None
    assert result["research_only"] is True
    assert result["promotion_allowed"] is False
    assert result["artifact_sha256"] == evidence["artifact_sha256"]


def test_model_loader_rejects_ambiguous_distance_output_contract(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    checkpoint = _checkpoint(include_output_contract=True)
    checkpoint["output_contract"]["kind"] = "distance_to_gamma_pin"
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(checkpoint, path)
    engine = inference.CUDAInferenceEngine()

    assert engine.load_model("SPX", path) is False
    assert "SPX" not in engine.models
    assert "output kind is unsupported" in engine.get_model_load_evidence("SPX")["error"]


def test_model_loader_rejects_nonfinite_state_dict_weights(monkeypatch, tmp_path):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    checkpoint = _checkpoint(include_output_contract=True)
    name = next(iter(checkpoint["model_state_dict"]))
    corrupted = checkpoint["model_state_dict"][name].clone()
    corrupted.reshape(-1)[0] = float("nan")
    checkpoint["model_state_dict"][name] = corrupted
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(checkpoint, path)
    engine = inference.CUDAInferenceEngine()

    assert engine.load_model("SPX", path) is False
    assert "SPX" not in engine.models
    assert "non-finite weights" in engine.get_model_load_evidence("SPX")["error"]


def test_model_loader_hashes_and_deserializes_one_buffer_with_weights_only(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(_checkpoint(include_output_contract=True), path)
    original_load = torch.load
    observed = {}

    def guarded_load(source, *args, **kwargs):
        observed["source"] = source
        observed["weights_only"] = kwargs.get("weights_only")
        return original_load(source, *args, **kwargs)

    monkeypatch.setattr(inference.torch, "load", guarded_load)
    engine = inference.CUDAInferenceEngine()

    assert engine.load_model("SPX", path) is True
    assert isinstance(observed["source"], io.BytesIO)
    assert observed["weights_only"] is True
    assert engine.get_model_load_evidence("SPX")["artifact_sha256"]


def test_missing_artifact_clears_previously_loaded_singleton_state(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    engine = inference.CUDAInferenceEngine()
    engine.models["SPX"] = object()
    engine.scalers["SPX"] = {"kind": "identity"}
    engine.feature_names["SPX"] = ["price"]
    engine.feature_schema_versions["SPX"] = "old"
    engine.output_contracts["SPX"] = {"kind": "future_price"}
    engine.model_artifact_sha256["SPX"] = "a" * 64

    engine.note_missing_model("spx", tmp_path / "missing.pt")

    assert "SPX" not in engine.models
    assert "SPX" not in engine.scalers
    assert "SPX" not in engine.feature_names
    assert "SPX" not in engine.feature_schema_versions
    assert "SPX" not in engine.output_contracts
    assert "SPX" not in engine.model_artifact_sha256
    assert engine.get_model_load_evidence("SPX")["status"] == "MISSING"


@pytest.mark.parametrize("enabled", [False, True])
def test_discovery_without_cuda_validates_without_admitting_or_warming(
    monkeypatch, tmp_path, enabled
):
    monkeypatch.setattr(inference, "CUDA_ENABLED", False)
    path = tmp_path / "gamma_model_SPX.pt"
    torch.save(_checkpoint(include_output_contract=True), path)
    engine = inference.CUDAInferenceEngine()
    monkeypatch.setattr(inference, "get_inference_engine", lambda: engine)
    monkeypatch.setattr(inference, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(
        engine,
        "warmup",
        lambda: pytest.fail("disabled artifact discovery must not warm a model"),
    )

    loaded = inference.load_all_models(enabled=enabled)

    assert loaded == []
    assert engine.models == {}
    evidence = engine.get_model_load_evidence("SPX")
    assert evidence["status"] == "DISABLED_NOT_LOADED"
    assert evidence["artifact_sha256"]


def test_cuda_discovery_failure_falls_back_to_cpu_with_evidence(monkeypatch):
    monkeypatch.setattr(inference, "CUDA_ENABLED", True)
    monkeypatch.setattr(inference.torch.cuda, "is_available", lambda: True)

    def fail_device_name(_index):
        raise RuntimeError("driver unavailable in session")

    monkeypatch.setattr(inference.torch.cuda, "get_device_name", fail_device_name)

    engine = inference.CUDAInferenceEngine()

    assert engine.device == torch.device("cpu")
    assert engine.device_setup_status == "CUDA_SETUP_FAILED_CPU_FALLBACK"
    assert engine.device_setup_error == {
        "error_type": "RuntimeError",
        "error": "driver unavailable in session",
    }
