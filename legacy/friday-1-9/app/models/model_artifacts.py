"""Helpers for standardized model artifact metadata."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from app.utils.settings import settings


def _artifacts_root() -> Path:
    root = Path(settings.model_artifacts_dir)
    if root.is_absolute():
        return root
    return Path.cwd() / root


def load_gamma_model_artifact(symbol: str) -> Dict[str, Any]:
    """Load standardized artifact metadata for the offline gamma model."""
    clean_symbol = symbol.upper().replace("I:", "")
    base = _artifacts_root()
    model_path = base / f"gamma_model_{clean_symbol}.pt"
    meta_path = base / f"gamma_model_{clean_symbol}_meta.json"

    metadata: Dict[str, Any] = {
        "artifact_schema_version": "gamma-artifact-v1",
        "symbol": clean_symbol,
        "model_path": str(model_path),
        "meta_path": str(meta_path),
        "model_exists": model_path.exists(),
        "meta_exists": meta_path.exists(),
        "feature_columns": [],
        "scaling_present": False,
        "training_device": "unknown",
    }

    if not meta_path.exists():
        return metadata

    try:
        raw = json.loads(meta_path.read_text())
    except (OSError, json.JSONDecodeError):
        return metadata

    metadata.update(
        {
            "feature_columns": raw.get("feature_columns", []),
            "target": raw.get("target"),
            "rows_total": raw.get("rows_total"),
            "rows_train": raw.get("rows_train"),
            "rows_validation": raw.get("rows_validation"),
            "best_validation_mse": raw.get("best_validation_mse"),
            "training_device": raw.get("device", "unknown"),
            "scaling_present": bool(raw.get("scaling")),
        }
    )
    return metadata
