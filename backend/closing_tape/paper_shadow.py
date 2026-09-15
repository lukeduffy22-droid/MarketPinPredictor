from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from .candidate import (
    PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION,
    SHA256_PATTERN,
    load_candidate_package_receipt,
    load_paper_candidate_activation_receipt,
)
from .dataset import PRODUCTION_FAMILIES
from .model_artifact import load_frozen_model_artifact
from .paper import record_paper_candidate_activation, record_paper_forecast
from .surface import select_decision_horizon_features


@dataclass(frozen=True)
class PaperShadowResult:
    configured: bool
    recorded: int
    model_version: str | None
    artifact_sha256: str | None
    forecast_keys: tuple[str, ...]
    reasons: tuple[str, ...]
    production_recorded: int = 0
    production_keys: tuple[str, ...] = ()
    production_reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def record_paper_shadow_predictions(
    surface: pd.DataFrame,
    *,
    project_root: str | Path,
    market_db_path: str | Path,
    trading_day: date,
    session_id: str,
    live_prefix_receipt: Mapping[str, object],
    decision_horizon_minutes: int = 15,
) -> PaperShadowResult:
    root = Path(project_root).resolve()
    descriptor_path = root / "models" / "closing_tape_paper_candidate.json"
    if not descriptor_path.is_file():
        return PaperShadowResult(False, 0, None, None, (), ("no paper candidate descriptor",))
    try:
        descriptor = json.loads(descriptor_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return PaperShadowResult(True, 0, None, None, (), (f"invalid paper descriptor: {type(exc).__name__}",))
    models_dir = (root / "models").resolve()
    expected_descriptor_fields = {
        "contract_version",
        "enabled_for_paper",
        "activation_receipt_path",
        "activation_receipt_sha256",
    }
    if not isinstance(descriptor, dict) or set(descriptor) != expected_descriptor_fields:
        return PaperShadowResult(True, 0, None, None, (), ("paper descriptor fields do not match its contract",))
    if descriptor.get("contract_version") != PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION:
        return PaperShadowResult(True, 0, None, None, (), ("paper descriptor contract is unsupported",))
    if descriptor.get("enabled_for_paper") is not True:
        return PaperShadowResult(True, 0, None, None, (), ("paper candidate is not enabled",))
    activation_hash = str(
        descriptor.get("activation_receipt_sha256") or ""
    ).lower()
    activation_relative = Path(
        str(descriptor.get("activation_receipt_path") or "")
    )
    activation_dir = (models_dir / "paper_activations").resolve()
    activation_path = (models_dir / activation_relative).resolve()
    if (
        not SHA256_PATTERN.fullmatch(activation_hash)
        or activation_relative.as_posix()
        != f"paper_activations/{activation_hash}.json"
        or activation_path.parent != activation_dir
        or not activation_path.is_file()
    ):
        return PaperShadowResult(
            True, 0, None, None, (),
            ("paper activation receipt identity is invalid",),
        )
    try:
        activation_bytes = activation_path.read_bytes()
    except OSError as exc:
        return PaperShadowResult(
            True, 0, None, None, (),
            (f"paper activation receipt is unreadable: {exc}",),
        )
    if hashlib.sha256(activation_bytes).hexdigest() != activation_hash:
        return PaperShadowResult(
            True, 0, None, None, (), ("paper activation receipt hash mismatch",)
        )
    try:
        activation = load_paper_candidate_activation_receipt(activation_path)
    except (OSError, ValueError) as exc:
        return PaperShadowResult(
            True, 0, None, None, (),
            (f"paper activation receipt is invalid: {exc}",),
        )
    receipt_hash = str(activation["candidate_package_sha256"]).lower()
    receipt_relative = Path(str(activation["candidate_package_path"]))
    receipts_dir = (models_dir / "candidate_packages").resolve()
    receipt_path = (models_dir / receipt_relative).resolve()
    if (
        not SHA256_PATTERN.fullmatch(receipt_hash)
        or receipt_relative.as_posix() != f"candidate_packages/{receipt_hash}.json"
        or receipt_path.parent != receipts_dir
        or not receipt_path.is_file()
    ):
        return PaperShadowResult(True, 0, None, None, (), ("paper candidate receipt identity is invalid",))
    try:
        receipt_bytes = receipt_path.read_bytes()
    except OSError as exc:
        return PaperShadowResult(True, 0, None, None, (), (f"paper candidate receipt is unreadable: {exc}",))
    if hashlib.sha256(receipt_bytes).hexdigest() != receipt_hash:
        return PaperShadowResult(True, 0, None, None, (), ("paper candidate receipt hash mismatch",))
    try:
        receipt = load_candidate_package_receipt(receipt_path)
    except (OSError, ValueError) as exc:
        return PaperShadowResult(True, 0, None, None, (), (f"paper candidate receipt is invalid: {exc}",))
    version = str(receipt["version"])
    artifact_hash = str(receipt["artifact_sha256"]).lower()
    if (
        str(activation["model_version"]) != version
        or str(activation["artifact_sha256"]).lower() != artifact_hash
    ):
        return PaperShadowResult(
            True, 0, version, artifact_hash, (),
            ("paper activation does not match the candidate package",),
        )
    artifact_path = (models_dir / str(receipt["artifact_path"])).resolve()
    try:
        artifact_path.relative_to(models_dir)
    except ValueError:
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact escapes models directory",))
    if not artifact_path.is_file():
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact is missing",))
    if hashlib.sha256(artifact_path.read_bytes()).hexdigest() != artifact_hash:
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact hash mismatch",))
    try:
        artifact = load_frozen_model_artifact(artifact_path)
    except (OSError, ValueError) as exc:
        return PaperShadowResult(True, 0, version, artifact_hash, (), (f"paper artifact is not loadable: {exc}",))
    if artifact.source_sha256s != tuple(receipt["source_sha256s"]):
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact DBN provenance mismatch",))
    if artifact.label_source_artifact_sha256s != tuple(
        receipt["label_source_artifact_sha256s"]
    ):
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact close provenance mismatch",))
    if (
        artifact.surface_artifact_sha256
        != str(receipt["surface_artifact_sha256"])
        or artifact.surface_replay_receipt_sha256
        != str(receipt["surface_replay_receipt_sha256"])
    ):
        return PaperShadowResult(
            True, 0, version, artifact_hash, (),
            ("paper artifact surface replay provenance mismatch",),
        )
    if (
        artifact.training_rows != int(receipt["training_rows"])
        or artifact.training_epochs != int(receipt["training_epochs"])
        or artifact.training_device != str(receipt["training_device"])
    ):
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("paper artifact training provenance mismatch",))

    selected = select_decision_horizon_features(
        surface, minutes_before_close=decision_horizon_minutes
    )
    expected = set(PRODUCTION_FAMILIES)
    roots = set(selected["family_root"].astype(str)) if not selected.empty else set()
    counts = selected["family_root"].value_counts().to_dict() if not selected.empty else {}
    if roots != expected or any(int(counts.get(family, 0)) != 1 for family in expected):
        return PaperShadowResult(
            True, 0, version, artifact_hash, (),
            ("exact-horizon surface must contain exactly one row for every production family",),
        )
    activated_at = datetime.fromisoformat(
        str(activation["activated_at_utc"])
    )
    feature_times = pd.to_datetime(
        selected["feature_available_at_utc"], utc=True, errors="coerce"
    )
    if feature_times.isna().any() or any(
        activated_at > value.to_pydatetime() for value in feature_times
    ):
        return PaperShadowResult(
            True, 0, version, artifact_hash, (),
            ("paper surface predates candidate activation",),
        )
    if "predicted_close" not in selected.columns:
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("incumbent prediction is unavailable",))
    incumbent = pd.to_numeric(selected["predicted_close"], errors="coerce")
    reference = pd.to_numeric(selected["reference_price"], errors="coerce")
    if not np.isfinite(incumbent).all() or not np.isfinite(reference).all():
        return PaperShadowResult(True, 0, version, artifact_hash, (), ("incumbent or reference price is unavailable",))
    try:
        record_paper_candidate_activation(
            market_db_path,
            activation_receipt_sha256=activation_hash,
            model_version=version,
            artifact_sha256=artifact_hash,
            candidate_package_sha256=receipt_hash,
            activated_at_utc=activated_at,
        )
    except ValueError as exc:
        return PaperShadowResult(
            True, 0, version, artifact_hash, (),
            (f"paper activation ledger rejected the candidate: {exc}",),
        )
    predictions = artifact.predict_log_return(selected, device="cpu")
    keys = []
    for index, row in selected.reset_index(drop=True).iterrows():
        keys.append(
            record_paper_forecast(
                market_db_path, model_version=version, artifact_sha256=artifact_hash,
                source_sha256=str(row["source_sha256"]), session_id=session_id,
                family_root=str(row["family_root"]), trading_day=trading_day,
                decision_horizon_minutes=decision_horizon_minutes,
                feature_available_at_utc=pd.Timestamp(row["feature_available_at_utc"]).to_pydatetime(),
                reference_price=float(reference.iloc[index]),
                candidate_predicted_log_return=float(predictions[index]),
                incumbent_predicted_close=float(incumbent.iloc[index]),
                model_features=row,
                live_prefix_receipt=live_prefix_receipt,
            )
        )
    return PaperShadowResult(True, len(keys), version, artifact_hash, tuple(keys), ())
