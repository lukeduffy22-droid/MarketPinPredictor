"""
CUDA-accelerated real-time prediction engine
Target: <100ms inference latency
"""
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
        self.device = self._setup_device()
        self.models: Dict[str, torch.nn.Module] = {}
        self.scalers: Dict[str, dict] = {}
        self.feature_names: Dict[str, list] = {}
        self.feature_schema_versions: Dict[str, str] = {}

        # Performance metrics
        self.inference_times = []
        self.total_predictions = 0

        logger.info(f"Inference engine initialized on {self.device}")

    def _setup_device(self) -> torch.device:
        """Setup CUDA device with optimizations"""
        if CUDA_ENABLED and torch.cuda.is_available():
            device = torch.device("cuda:0")

            # Enable TF32 for faster matmul on Ampere+ GPUs
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True

            # Enable cudnn benchmarking for fixed input sizes
            torch.backends.cudnn.benchmark = True

            logger.info(f"✅ CUDA enabled: {torch.cuda.get_device_name(0)}")
            return device
        else:
            logger.warning("CUDA not available, using CPU")
            return torch.device("cpu")

    def load_model(self, symbol: str, model_path: Path = None) -> bool:
        """Load pretrained model for a symbol"""
        try:
            if model_path is None:
                model_path = MODELS_DIR / f"gamma_model_{symbol}.pt"

            if not model_path.exists():
                logger.warning(f"Model not found: {model_path}")
                return False

            # Load checkpoint
            checkpoint = torch.load(model_path, map_location=self.device)

            # Create model
            model_config = checkpoint.get("config", {})
            input_dim = int(model_config.get("input_dim", 10))
            feature_names = list(checkpoint.get("features") or [])
            if not feature_names:
                raise ValueError("checkpoint is missing its ordered feature contract")
            if len(feature_names) != input_dim:
                raise ValueError(
                    "checkpoint feature count does not match config.input_dim: "
                    f"{len(feature_names)} != {input_dim}"
                )
            model = GammaPredictorModel(
                input_dim=input_dim,
                hidden_dims=model_config.get("hidden_dims", [128, 64, 32])
            )

            model.load_state_dict(checkpoint["model_state_dict"])
            model.to(self.device)
            model.eval()

            # Use half precision if configured
            if MODEL_PRECISION == "fp16" and self.device.type == "cuda":
                model = model.half()

            self.models[symbol] = model
            self.scalers[symbol] = checkpoint.get("scaler", {})
            self.feature_names[symbol] = feature_names
            self.feature_schema_versions[symbol] = str(
                checkpoint.get("feature_schema_version") or "legacy-checkpoint"
            )

            logger.info(f"✅ Loaded model for {symbol}: {model_path.name}")
            return True

        except Exception as e:
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
            if scaler:
                mean = scaler.get("mean", [])
                std = scaler.get("std", [])
                if mean and std:
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


def load_all_models() -> list[str]:
    """Load available trained artifacts without implying that CUDA alone is a model."""
    engine = get_inference_engine()

    for symbol in ["SPX", "NDX", "RUT", "DJI"]:
        model_path = MODELS_DIR / f"gamma_model_{symbol}.pt"
        if model_path.exists():
            engine.load_model(symbol, model_path)

    loaded = sorted(engine.models)
    if loaded:
        engine.warmup()
    else:
        runtime_name = "CUDA" if engine.device.type == "cuda" else "CPU"
        logger.warning(
            "%s inference runtime initialized, but no gamma_model_*.pt artifacts are "
            "installed; trained-model inference remains disabled",
            runtime_name,
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
