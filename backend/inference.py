"""
CUDA-accelerated real-time prediction engine
Target: <100ms inference latency
"""
import hashlib
import io
import os

import torch
import torch.nn as nn
import numpy as np
import logging
import math
import time
from pathlib import Path
from typing import Any, Mapping, Optional, Dict
import json

from backend.config import (
    CUDA_ENABLED, MODELS_DIR, INFERENCE_BATCH_SIZE,
    MODEL_PRECISION, MAX_INFERENCE_LATENCY_MS
)

logger = logging.getLogger(__name__)


GEX_INFERENCE_FEATURE_SCHEMA_VERSION = "databento-gex-structure-v1"
CUDA_SHADOW_OUTPUT_CONTRACT_VERSION = "marketpin-cuda-shadow-output-v1"
CUDA_SHADOW_PREDICTION_HORIZON_SECONDS = 300
CUDA_SHADOW_ALLOWED_OUTPUT_KINDS = frozenset({"future_price"})
CUDA_SHADOW_MAX_ABS_MOVE_BPS_HARD_CEILING = 500.0
CUDA_SHADOW_AUTOLOAD_SYMBOLS = ("SPX", "NDX")
MAX_CUDA_SHADOW_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_CUDA_SHADOW_PARAMETERS = 5_000_000
MAX_CUDA_SHADOW_HIDDEN_LAYERS = 6
MAX_CUDA_SHADOW_HIDDEN_WIDTH = 2_048
BASE_INFERENCE_FEATURE_NAMES = ("price", "volume")
GEX_STRUCTURE_FEATURE_NAMES = (
    "zero_gamma_distance",
    "wall_asymmetry",
    "top_strike_concentration",
)
SUPPORTED_LIVE_INFERENCE_FEATURE_NAMES = (
    *BASE_INFERENCE_FEATURE_NAMES,
    *GEX_STRUCTURE_FEATURE_NAMES,
)


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _first_finite(source: Mapping[str, Any], *names: str) -> float | None:
    for name in names:
        value = _finite_float(source.get(name))
        if value is not None:
            return value
    return None


def build_live_inference_features(
    payload: Mapping[str, Any] | None,
    *,
    current_price: float | None = None,
) -> dict[str, float | None]:
    """Build the point-in-time feature vector declared by live model artifacts.

    Missing GEX structure remains ``None`` rather than being encoded as zero:
    zero is a real crossing distance/asymmetry/concentration observation. The
    inference engine therefore abstains when an artifact requires evidence the
    current payload does not contain.
    """
    source = dict(payload or {})
    embedded = source.get("inference_features")
    embedded = dict(embedded) if isinstance(embedded, Mapping) else {}

    price = _finite_float(current_price)
    if price is None:
        price = _first_finite(source, "price", "spot_last", "spot_price")
    if price is not None and price <= 0:
        price = None

    volume = _first_finite(source, "volume", "market_volume")
    if volume is not None and volume < 0:
        volume = None

    zero_gamma_distance = _first_finite(
        embedded,
        "zero_gamma_distance",
        "gex_crossing_distance",
    )
    if zero_gamma_distance is None:
        zero_gamma_distance = _first_finite(
            source,
            "zero_gamma_distance",
            "gex_crossing_distance",
        )
    if zero_gamma_distance is None and price is not None:
        crossing = _first_finite(source, "zero_gamma", "zero_gamma_level")
        if crossing is not None:
            zero_gamma_distance = (crossing - price) / price

    wall_asymmetry = _first_finite(embedded, "wall_asymmetry")
    if wall_asymmetry is None:
        wall_asymmetry = _first_finite(source, "wall_asymmetry")
    if wall_asymmetry is None and price is not None:
        positive_wall = _first_finite(source, "positive_gex_wall")
        negative_wall = _first_finite(source, "negative_gex_wall")
        if positive_wall is not None and negative_wall is not None:
            positive_distance = abs(positive_wall - price) / price
            negative_distance = abs(price - negative_wall) / price
            distance_sum = positive_distance + negative_distance
            if distance_sum > 0:
                wall_asymmetry = (
                    positive_distance - negative_distance
                ) / distance_sum

    top_strike_concentration = _first_finite(
        embedded,
        "top_strike_concentration",
    )
    if top_strike_concentration is None:
        top_strike_concentration = _first_finite(
            source,
            "top_strike_concentration",
            "top_strike_share",
        )
    if top_strike_concentration is None:
        gross_gex = _first_finite(source, "gross_gex", "total_gex_abs")
        top_strikes = source.get("top_strikes") or source.get(
            "top_strikes_by_abs_gex"
        )
        if gross_gex is not None and gross_gex > 0 and isinstance(top_strikes, list):
            for item in top_strikes:
                if not isinstance(item, Mapping):
                    continue
                top_abs_gex = _first_finite(item, "abs_gex")
                if top_abs_gex is None:
                    top_net_gex = _first_finite(item, "net_gex", "gex")
                    top_abs_gex = abs(top_net_gex) if top_net_gex is not None else None
                if top_abs_gex is not None:
                    top_strike_concentration = top_abs_gex / gross_gex
                    break

    if wall_asymmetry is not None and not -1.0 <= wall_asymmetry <= 1.0:
        wall_asymmetry = None
    if (
        top_strike_concentration is not None
        and not 0.0 <= top_strike_concentration <= 1.0
    ):
        top_strike_concentration = None

    return {
        "price": price,
        "volume": volume,
        "zero_gamma_distance": zero_gamma_distance,
        "wall_asymmetry": wall_asymmetry,
        "top_strike_concentration": top_strike_concentration,
    }


class GammaPredictorModel(nn.Module):
    """Optimized neural network for gamma-based EOD prediction"""

    def __init__(self, input_dim: int = 10, hidden_dims: list = None):
        super().__init__()

        if hidden_dims is None:
            hidden_dims = [128, 64, 32]

        layers = []
        prev_dim = input_dim

        for hidden_dim in hidden_dims:
            layers.extend([
                nn.Linear(prev_dim, hidden_dim),
                nn.LayerNorm(hidden_dim),
                nn.ReLU(),
                nn.Dropout(0.2)
            ])
            prev_dim = hidden_dim

        layers.append(nn.Linear(prev_dim, 1))

        self.network = nn.Sequential(*layers)

    def forward(self, x):
        return self.network(x)


class CUDAInferenceEngine:
    """High-performance CUDA inference engine"""

    def __init__(self):
        self.device_setup_status = "INITIALIZING"
        self.device_setup_error: dict[str, str] | None = None
        self.device = self._setup_device()
        self.models: Dict[str, torch.nn.Module] = {}
        self.scalers: Dict[str, dict] = {}
        self.feature_names: Dict[str, list] = {}
        self.feature_schema_versions: Dict[str, str] = {}
        self.output_contracts: Dict[str, dict] = {}
        self.model_artifact_sha256: Dict[str, str] = {}
        self.model_load_evidence: Dict[str, dict] = {}

        # Performance metrics
        self.inference_times = []
        self.total_predictions = 0

        logger.info(f"Inference engine initialized on {self.device}")

    def _setup_device(self) -> torch.device:
        """Setup CUDA device with optimizations"""
        if not CUDA_ENABLED:
            self.device_setup_status = "CUDA_DISABLED_BY_CONFIG"
            logger.warning("CUDA not available, using CPU")
            return torch.device("cpu")
        try:
            if not torch.cuda.is_available():
                self.device_setup_status = "CUDA_UNAVAILABLE"
                logger.warning("CUDA not available, using CPU")
                return torch.device("cpu")
            device_name = torch.cuda.get_device_name(0)
            device = torch.device("cuda:0")

            # Enable TF32 for faster matmul on Ampere+ GPUs
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True

            # Enable cudnn benchmarking for fixed input sizes
            torch.backends.cudnn.benchmark = True
            self.device_setup_status = "CUDA_READY"
            logger.info(f"✅ CUDA enabled: {device_name}")
            return device
        except Exception as exc:
            self.device_setup_status = "CUDA_SETUP_FAILED_CPU_FALLBACK"
            self.device_setup_error = {
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            logger.warning(
                "CUDA setup failed; inference runtime is falling back to CPU: %s: %s",
                type(exc).__name__,
                exc,
            )
            return torch.device("cpu")

    def _record_model_evidence(
        self,
        symbol: str,
        *,
        status: str,
        model_path: Path,
        artifact_sha256: str | None = None,
        error: Exception | None = None,
        feature_names: list[str] | None = None,
        feature_schema_version: str | None = None,
        output_contract: Mapping[str, Any] | None = None,
        validation_device: str | None = None,
    ) -> None:
        """Retain explicit research-only artifact acceptance/rejection evidence."""
        normalized = str(symbol or "").strip().upper()
        self.model_load_evidence[normalized] = {
            "symbol": normalized,
            "status": status,
            "research_only": True,
            "promotion_allowed": False,
            # Runtime capability is not evidence that disabled inspection ran
            # inference on that device.  Keep validation and execution explicit.
            "runtime_device": str(self.device),
            "validation_device": validation_device,
            "execution_device": (
                str(self.device) if status == "LOADED_SHADOW_ONLY" else None
            ),
            "device_setup_status": self.device_setup_status,
            "device_setup_error": dict(self.device_setup_error or {}),
            "model_path": str(model_path.resolve()),
            "artifact_sha256": artifact_sha256,
            "feature_names": list(feature_names or ()),
            "feature_schema_version": feature_schema_version,
            "output_contract": dict(output_contract or {}),
            "error_type": type(error).__name__ if error is not None else None,
            "error": str(error) if error is not None else None,
        }

    def _clear_model_state(self, symbol: str) -> str:
        normalized = str(symbol or "").strip().upper()
        self.models.pop(normalized, None)
        self.scalers.pop(normalized, None)
        self.feature_names.pop(normalized, None)
        self.feature_schema_versions.pop(normalized, None)
        self.output_contracts.pop(normalized, None)
        self.model_artifact_sha256.pop(normalized, None)
        return normalized

    def note_missing_model(self, symbol: str, model_path: Path) -> None:
        """Record absence without treating an unavailable artifact as loaded."""
        symbol = self._clear_model_state(symbol)
        self._record_model_evidence(
            symbol,
            status="MISSING",
            model_path=model_path,
            error=FileNotFoundError(f"model artifact not found: {model_path}"),
        )

    def get_model_load_evidence(self, symbol: str | None = None) -> dict:
        """Return a copy of artifact evidence suitable for research provenance."""
        if symbol is not None:
            return dict(self.model_load_evidence.get(str(symbol).upper(), {}))
        return {
            key: dict(value)
            for key, value in sorted(self.model_load_evidence.items())
        }

    @staticmethod
    def _validated_scaler(checkpoint: Mapping[str, Any], input_dim: int) -> dict:
        scaler = checkpoint.get("scaler")
        if not isinstance(scaler, Mapping):
            raise ValueError("checkpoint is missing its scaler contract")
        kind = str(scaler.get("kind") or "standard").strip().lower()
        if kind == "identity":
            return {"kind": "identity"}
        if kind != "standard":
            raise ValueError(f"unsupported scaler kind: {kind or 'missing'}")
        mean = list(scaler.get("mean") or ())
        std = list(scaler.get("std") or ())
        if len(mean) != input_dim or len(std) != input_dim:
            raise ValueError(
                "checkpoint scaler width does not match config.input_dim: "
                f"mean={len(mean)}, std={len(std)}, input_dim={input_dim}"
            )
        parsed_mean = [_finite_float(value) for value in mean]
        parsed_std = [_finite_float(value) for value in std]
        if any(value is None for value in parsed_mean):
            raise ValueError("checkpoint scaler mean contains non-finite values")
        if any(value is None or value <= 0 for value in parsed_std):
            raise ValueError("checkpoint scaler std must contain positive finite values")
        return {"kind": "standard", "mean": parsed_mean, "std": parsed_std}

    @staticmethod
    def _validated_output_contract(checkpoint: Mapping[str, Any]) -> dict:
        contract = checkpoint.get("output_contract")
        if not isinstance(contract, Mapping):
            raise ValueError("checkpoint is missing its output contract")
        normalized = {
            "schema_version": str(contract.get("schema_version") or ""),
            "kind": str(contract.get("kind") or ""),
            "units": str(contract.get("units") or ""),
            "prediction_horizon_seconds": int(
                contract.get("prediction_horizon_seconds") or 0
            ),
            "research_only": contract.get("research_only") is True,
            "promotion_allowed": contract.get("promotion_allowed") is True,
            "max_abs_move_bps": _finite_float(contract.get("max_abs_move_bps")),
        }
        if normalized["schema_version"] != CUDA_SHADOW_OUTPUT_CONTRACT_VERSION:
            raise ValueError("checkpoint output contract version is unsupported")
        if normalized["kind"] not in CUDA_SHADOW_ALLOWED_OUTPUT_KINDS:
            raise ValueError("checkpoint output kind is unsupported")
        if normalized["units"] != "index_points":
            raise ValueError("checkpoint output units must be index_points")
        if (
            normalized["prediction_horizon_seconds"]
            != CUDA_SHADOW_PREDICTION_HORIZON_SECONDS
        ):
            raise ValueError(
                "checkpoint prediction horizon must be exactly "
                f"{CUDA_SHADOW_PREDICTION_HORIZON_SECONDS} seconds"
            )
        if normalized["research_only"] is not True:
            raise ValueError("checkpoint must declare research_only=true")
        if normalized["promotion_allowed"] is not False:
            raise ValueError("checkpoint must declare promotion_allowed=false")
        max_abs_move_bps = normalized["max_abs_move_bps"]
        if (
            max_abs_move_bps is None
            or max_abs_move_bps <= 0
            or max_abs_move_bps > CUDA_SHADOW_MAX_ABS_MOVE_BPS_HARD_CEILING
        ):
            raise ValueError(
                "checkpoint max_abs_move_bps must be positive and no greater than "
                f"{CUDA_SHADOW_MAX_ABS_MOVE_BPS_HARD_CEILING}"
            )
        return normalized

    def _read_validated_checkpoint(
        self,
        model_path: Path,
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        validated = evidence if evidence is not None else {}
        size = model_path.stat().st_size
        if size <= 0 or size > MAX_CUDA_SHADOW_ARTIFACT_BYTES:
            raise ValueError(
                "checkpoint byte size is outside the research safety bound: "
                f"{size} not in 1..{MAX_CUDA_SHADOW_ARTIFACT_BYTES}"
            )
        # Hash and deserialize the same bounded byte sequence.  Always decode on
        # CPU; device transfer occurs only after every contract and shape passes.
        artifact_bytes = model_path.read_bytes()
        if len(artifact_bytes) != size or len(artifact_bytes) > MAX_CUDA_SHADOW_ARTIFACT_BYTES:
            raise ValueError("checkpoint changed while it was being read")
        artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
        validated["artifact_sha256"] = artifact_sha256
        checkpoint = torch.load(
            io.BytesIO(artifact_bytes),
            map_location="cpu",
            weights_only=True,
        )
        if not isinstance(checkpoint, Mapping):
            raise ValueError("checkpoint root must be a mapping")
        model_config = checkpoint.get("config", {})
        if not isinstance(model_config, Mapping):
            raise ValueError("checkpoint config must be a mapping")
        input_dim = int(model_config.get("input_dim", 0))
        if not 1 <= input_dim <= len(SUPPORTED_LIVE_INFERENCE_FEATURE_NAMES):
            raise ValueError("checkpoint input_dim is outside the live feature bound")
        raw_hidden_dims = model_config.get("hidden_dims", [128, 64, 32])
        if not isinstance(raw_hidden_dims, (list, tuple)):
            raise ValueError("checkpoint hidden_dims must be an ordered sequence")
        hidden_dims = [int(value) for value in raw_hidden_dims]
        if not hidden_dims or len(hidden_dims) > MAX_CUDA_SHADOW_HIDDEN_LAYERS:
            raise ValueError("checkpoint hidden layer count is outside the safety bound")
        if any(value <= 0 or value > MAX_CUDA_SHADOW_HIDDEN_WIDTH for value in hidden_dims):
            raise ValueError("checkpoint hidden width is outside the safety bound")
        layer_dims = [input_dim, *hidden_dims, 1]
        linear_parameters = sum(
            left * right + right
            for left, right in zip(layer_dims, layer_dims[1:])
        )
        layer_norm_parameters = sum(2 * width for width in hidden_dims)
        if linear_parameters + layer_norm_parameters > MAX_CUDA_SHADOW_PARAMETERS:
            raise ValueError("checkpoint architecture exceeds the parameter safety bound")

        feature_names = [str(name) for name in (checkpoint.get("features") or [])]
        if not feature_names:
            raise ValueError("checkpoint is missing its ordered feature contract")
        if len(feature_names) != input_dim:
            raise ValueError(
                "checkpoint feature count does not match config.input_dim: "
                f"{len(feature_names)} != {input_dim}"
            )
        if len(set(feature_names)) != len(feature_names):
            raise ValueError("checkpoint ordered feature contract contains duplicates")
        unsupported_features = sorted(
            set(feature_names) - set(SUPPORTED_LIVE_INFERENCE_FEATURE_NAMES)
        )
        if unsupported_features:
            raise ValueError(
                "checkpoint requests unsupported live features: "
                + ", ".join(unsupported_features)
            )
        feature_schema_version = str(
            checkpoint.get("feature_schema_version") or ""
        )
        if feature_schema_version != GEX_INFERENCE_FEATURE_SCHEMA_VERSION:
            raise ValueError(
                "checkpoint feature schema does not match the live payload contract"
            )
        scaler = self._validated_scaler(checkpoint, input_dim)
        output_contract = self._validated_output_contract(checkpoint)
        state_dict = checkpoint.get("model_state_dict")
        if not isinstance(state_dict, Mapping):
            raise ValueError("checkpoint is missing model_state_dict")
        total_tensor_numel = 0
        for name, tensor in state_dict.items():
            if not isinstance(name, str) or not isinstance(tensor, torch.Tensor):
                raise ValueError("checkpoint state_dict must map names to tensors")
            if tensor.layout != torch.strided or not tensor.is_floating_point():
                raise ValueError(
                    "checkpoint state_dict tensors must be floating strided tensors"
                )
            total_tensor_numel += int(tensor.numel())
            if total_tensor_numel > MAX_CUDA_SHADOW_PARAMETERS:
                raise ValueError("checkpoint tensor count exceeds the safety bound")
            if not bool(torch.isfinite(tensor).all().item()):
                raise ValueError("checkpoint state_dict contains non-finite weights")
        if total_tensor_numel <= 0:
            raise ValueError("checkpoint state_dict contains no parameters")
        validated.update({
            "input_dim": input_dim,
            "hidden_dims": hidden_dims,
            "feature_names": feature_names,
            "feature_schema_version": feature_schema_version,
            "scaler": scaler,
            "output_contract": output_contract,
            "state_dict": state_dict,
        })
        return validated

    def inspect_model(self, symbol: str, model_path: Path | None = None) -> bool:
        """Validate a disabled artifact on CPU without admitting or warming it."""
        symbol = self._clear_model_state(symbol)
        model_path = Path(
            model_path or (MODELS_DIR / f"gamma_model_{symbol}.pt")
        )
        if not model_path.exists():
            self.note_missing_model(symbol, model_path)
            return False
        validated: dict[str, Any] = {}
        try:
            self._read_validated_checkpoint(model_path, validated)
            model = GammaPredictorModel(
                input_dim=validated["input_dim"],
                hidden_dims=validated["hidden_dims"],
            )
            model.load_state_dict(validated["state_dict"])
            model.eval()
            self._record_model_evidence(
                symbol,
                status="DISABLED_NOT_LOADED",
                model_path=model_path,
                artifact_sha256=validated["artifact_sha256"],
                feature_names=validated["feature_names"],
                feature_schema_version=validated["feature_schema_version"],
                output_contract=validated["output_contract"],
                validation_device="cpu",
            )
            return True
        except Exception as exc:
            self._clear_model_state(symbol)
            self._record_model_evidence(
                symbol,
                status="REJECTED",
                model_path=model_path,
                artifact_sha256=validated.get("artifact_sha256"),
                error=exc,
                feature_names=validated.get("feature_names"),
                feature_schema_version=validated.get("feature_schema_version"),
                output_contract=validated.get("output_contract"),
                validation_device="cpu",
            )
            logger.error("Error inspecting model for %s: %s", symbol, exc)
            return False

    def load_model(self, symbol: str, model_path: Path = None) -> bool:
        """Load a contract-complete research-only artifact for shadow inference."""
        symbol = self._clear_model_state(symbol)
        if model_path is None:
            model_path = MODELS_DIR / f"gamma_model_{symbol}.pt"
        model_path = Path(model_path)
        validated: dict[str, Any] = {}
        try:
            if not model_path.exists():
                logger.warning(f"Model not found: {model_path}")
                self.note_missing_model(symbol, model_path)
                return False
            self._read_validated_checkpoint(model_path, validated)
            model = GammaPredictorModel(
                input_dim=validated["input_dim"],
                hidden_dims=validated["hidden_dims"],
            )
            model.load_state_dict(validated["state_dict"])
            model.to(self.device)
            model.eval()

            # Use half precision if configured
            if MODEL_PRECISION == "fp16" and self.device.type == "cuda":
                model = model.half()

            self.models[symbol] = model
            self.scalers[symbol] = validated["scaler"]
            self.feature_names[symbol] = validated["feature_names"]
            self.feature_schema_versions[symbol] = validated["feature_schema_version"]
            self.output_contracts[symbol] = validated["output_contract"]
            self.model_artifact_sha256[symbol] = validated["artifact_sha256"]
            self._record_model_evidence(
                symbol,
                status="LOADED_SHADOW_ONLY",
                model_path=model_path,
                artifact_sha256=validated["artifact_sha256"],
                feature_names=validated["feature_names"],
                feature_schema_version=validated["feature_schema_version"],
                output_contract=validated["output_contract"],
                validation_device="cpu",
            )

            logger.info(f"✅ Loaded model for {symbol}: {model_path.name}")
            return True

        except Exception as e:
            self._clear_model_state(symbol)
            self._record_model_evidence(
                symbol,
                status="REJECTED",
                model_path=model_path,
                artifact_sha256=validated.get("artifact_sha256"),
                error=e,
                feature_names=validated.get("feature_names"),
                feature_schema_version=validated.get("feature_schema_version"),
                output_contract=validated.get("output_contract"),
                validation_device="cpu",
            )
            logger.error(f"Error loading model for {symbol}: {e}")
            return False

    def _prepare_features(self, symbol: str, features: Dict[str, float]) -> Optional[torch.Tensor]:
        """Prepare and normalize features for inference"""
        try:
            # Extract feature vector in correct order
            feature_names = self.feature_names.get(symbol, [])
            if not feature_names:
                logger.error(f"No feature names found for {symbol}")
                return None

            feature_vector = []
            for name in feature_names:
                value = _finite_float(features.get(name))
                if value is None:
                    logger.warning(
                        "Inference abstained for %s: missing or non-finite feature %s",
                        symbol,
                        name,
                    )
                    return None
                feature_vector.append(value)

            # Convert to tensor
            x = torch.tensor([feature_vector], dtype=torch.float32)

            # Apply normalization
            scaler = self.scalers.get(symbol)
            if scaler and scaler.get("kind") == "standard":
                mean = scaler.get("mean", [])
                std = scaler.get("std", [])
                x = (x - torch.tensor(mean)) / torch.tensor(std)

            # Move to device and convert precision
            x = x.to(self.device)
            if MODEL_PRECISION == "fp16" and self.device.type == "cuda":
                x = x.half()

            return x

        except Exception as e:
            logger.error(f"Error preparing features: {e}")
            return None

    @torch.no_grad()
    def predict(self, symbol: str, features: Dict[str, float]) -> Optional[Dict]:
        """
        Run real-time prediction

        Args:
            symbol: Index symbol
            features: Dictionary of feature values

        Returns:
            Prediction dict with value, confidence, and latency
        """
        start_time = time.perf_counter()
        symbol = str(symbol or "").strip().upper()

        try:
            # Check if model exists
            if symbol not in self.models:
                logger.error(f"No model loaded for {symbol}")
                return None

            # Prepare input
            x = self._prepare_features(symbol, features)
            if x is None:
                return None

            # Run inference
            model = self.models[symbol]
            output = model(x)

            # Convert to scalar
            prediction = output.cpu().float().item()

            # Calculate latency
            latency_ms = (time.perf_counter() - start_time) * 1000
            self.inference_times.append(latency_ms)
            self.total_predictions += 1

            # Warn if too slow
            if latency_ms > MAX_INFERENCE_LATENCY_MS:
                logger.warning(f"Slow inference: {latency_ms:.1f}ms (target: {MAX_INFERENCE_LATENCY_MS}ms)")

            return {
                "symbol": symbol,
                "predicted_value": prediction,
                "confidence": self._estimate_confidence(symbol, features),
                "inference_time_ms": latency_ms,
                "timestamp": time.time(),
                "feature_schema_version": self.feature_schema_versions.get(symbol),
                "output_contract": dict(self.output_contracts.get(symbol, {})),
                "artifact_sha256": self.model_artifact_sha256.get(symbol),
                "inference_device": str(self.device),
                "research_only": True,
                "promotion_allowed": False,
                "feature_snapshot": {
                    name: _finite_float(features.get(name))
                    for name in self.feature_names.get(symbol, [])
                },
            }

        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return None

    def predict_from_live_payload(
        self,
        symbol: str,
        payload: Mapping[str, Any] | None,
        *,
        current_price: float | None = None,
    ) -> Optional[Dict]:
        """Run artifact-declared inference from a Databento pin payload."""
        features = build_live_inference_features(
            payload,
            current_price=current_price,
        )
        return self.predict(symbol.upper(), features)

    def _estimate_confidence(self, symbol: str, features: Dict) -> float:
        """Estimate prediction confidence based on feature quality"""
        # Simple heuristic - can be improved with uncertainty estimation
        confidence = 0.8

        # Reduce confidence if features are missing
        feature_names = self.feature_names.get(symbol, [])
        missing = sum(1 for f in feature_names if features.get(f) is None)
        confidence -= missing * 0.05

        return max(0.0, min(1.0, confidence))

    def get_stats(self) -> Dict:
        """Get performance statistics"""
        if not self.inference_times:
            return {}

        recent = self.inference_times[-100:]  # Last 100 predictions

        return {
            "total_predictions": self.total_predictions,
            "avg_latency_ms": np.mean(recent),
            "p50_latency_ms": np.percentile(recent, 50),
            "p95_latency_ms": np.percentile(recent, 95),
            "p99_latency_ms": np.percentile(recent, 99),
            "max_latency_ms": np.max(recent),
            "models_loaded": list(self.models.keys())
        }

    def warmup(self):
        """Warmup models with dummy predictions"""
        logger.info("Warming up inference engine...")

        for symbol in self.models.keys():
            # Create dummy features
            feature_names = self.feature_names.get(symbol, [])
            dummy_features = {name: 0.5 for name in feature_names}

            # Run a few predictions
            for _ in range(10):
                self.predict(symbol, dummy_features)

        logger.info("✅ Warmup complete")


# Global inference engine
_engine: Optional[CUDAInferenceEngine] = None


def get_inference_engine() -> CUDAInferenceEngine:
    """Get or create global inference engine"""
    global _engine
    if _engine is None:
        _engine = CUDAInferenceEngine()
    return _engine


def load_all_models(*, enabled: bool | None = None) -> list[str]:
    """Audit artifacts by default; admit/warm them only after explicit opt-in."""
    engine = get_inference_engine()
    requested_enabled = (
        _env_enabled("CUDA_SHADOW_RESEARCH_ENABLED", False)
        if enabled is None
        else bool(enabled)
    )
    execution_enabled = requested_enabled and str(
        getattr(engine, "device", "cpu")
    ).startswith("cuda")
    rejected = []
    validated_disabled = []
    for symbol in CUDA_SHADOW_AUTOLOAD_SYMBOLS:
        model_path = MODELS_DIR / f"gamma_model_{symbol}.pt"
        if model_path.exists():
            if execution_enabled:
                accepted = engine.load_model(symbol, model_path)
            else:
                inspect_model = getattr(engine, "inspect_model", None)
                accepted = bool(
                    inspect_model(symbol, model_path)
                    if callable(inspect_model)
                    else False
                )
                if accepted:
                    validated_disabled.append(symbol)
            if not accepted:
                rejected.append(symbol)
        else:
            note_missing = getattr(engine, "note_missing_model", None)
            if callable(note_missing):
                note_missing(symbol, model_path)

    loaded = sorted(engine.models) if execution_enabled else []
    if execution_enabled and loaded:
        engine.warmup()
    elif not execution_enabled and validated_disabled:
        logger.info(
            "Validated shadow artifacts for %s, but CUDA shadow execution is disabled; "
            "no model was admitted or warmed",
            ", ".join(validated_disabled),
        )
    elif rejected:
        logger.warning(
            "Trained-model artifacts exist for %s but none passed loading validation; "
            "trained-model inference remains disabled. See the per-symbol errors.",
            ", ".join(rejected),
        )
    elif requested_enabled:
        runtime_name = "CUDA" if engine.device.type == "cuda" else "CPU"
        logger.warning(
            "%s inference runtime initialized, but no gamma_model_*.pt artifacts are "
            "installed; trained-model inference remains disabled",
            runtime_name,
        )
    else:
        logger.info(
            "CUDA shadow execution is disabled and no valid artifacts were admitted"
        )
    return loaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test inference engine
    engine = CUDAInferenceEngine()

    # Create dummy model for testing
    model = GammaPredictorModel(input_dim=5)
    model.to(engine.device)
    model.eval()

    engine.models["TEST"] = model
    engine.feature_names["TEST"] = ["f1", "f2", "f3", "f4", "f5"]

    # Test prediction
    features = {"f1": 1.0, "f2": 2.0, "f3": 3.0, "f4": 4.0, "f5": 5.0}
    result = engine.predict("TEST", features)

    if result:
        print(f"✅ Prediction: {result['predicted_value']:.4f}")
        print(f"⚡ Latency: {result['inference_time_ms']:.2f}ms")
