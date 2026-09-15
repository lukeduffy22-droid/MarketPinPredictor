from __future__ import annotations

import hashlib
import json
import math
import os
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .calibration import (
    DEPLOYMENT_CALIBRATION_METHOD,
    DeploymentConformalCalibration,
    bind_candidate_oos_predictions,
    fit_deployment_conformal_calibration,
)
from .dataset import PRODUCTION_FAMILIES
from .evaluation import (
    GuardedModelEvaluationResult,
    require_guarded_model_evaluation_result,
)
from .model_artifact import (
    MODEL_ARTIFACT_FORMAT,
    FrozenModelBuild,
    fit_promoted_frozen_model,
    load_frozen_model_artifact,
)
from .surface_artifact import (
    require_replay_verified_surface_identity,
    retain_surface_promotion_evidence,
)


CANDIDATE_PACKAGE_CONTRACT_VERSION = "closing-tape-candidate-package-v2"
PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION = "closing-tape-paper-candidate-v3"
PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION = (
    "closing-tape-paper-candidate-activation-v1"
)
MAX_CANDIDATE_RECEIPT_BYTES = 2 * 1024 * 1024
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
UTC = timezone.utc


@dataclass(frozen=True)
class FrozenCandidatePackage:
    version: str
    artifact_path: str
    artifact_sha256: str
    training_rows: int
    training_epochs: int
    training_device: str
    source_sha256s: tuple[str, ...]
    label_source_artifact_sha256s: tuple[str, ...]
    surface_artifact_sha256: str
    surface_replay_receipt_sha256: str
    package_receipt_path: str
    package_receipt_sha256: str
    deployment_calibration: DeploymentConformalCalibration

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8") + b"\n"
    except (TypeError, ValueError) as exc:
        raise ValueError("candidate package must be strict canonical JSON") from exc


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _canonical_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("paper activation timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _canonical_hashes(value: object, *, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or not value:
        raise ValueError(f"candidate package {field} must be a non-empty list")
    hashes = tuple(str(item).lower() for item in value)
    if hashes != tuple(sorted(set(hashes))) or any(
        not SHA256_PATTERN.fullmatch(item) for item in hashes
    ):
        raise ValueError(
            f"candidate package {field} must contain sorted unique SHA-256 values"
        )
    return hashes


def parse_candidate_package_receipt(payload: object) -> dict[str, object]:
    """Validate one immutable candidate receipt without loading its artifact."""
    if not isinstance(payload, dict):
        raise ValueError("candidate package receipt root must be an object")
    expected = {
        "contract_version", "version", "artifact_path", "artifact_sha256",
        "artifact_format", "training_rows", "training_epochs", "training_device",
        "source_sha256s", "label_source_artifact_sha256s",
        "surface_artifact_sha256", "surface_replay_receipt_sha256",
        "deployment_calibration",
    }
    if set(payload) != expected:
        raise ValueError("candidate package receipt fields do not match its contract")
    if payload.get("contract_version") != CANDIDATE_PACKAGE_CONTRACT_VERSION:
        raise ValueError("candidate package receipt contract is unsupported")
    version = str(payload.get("version") or "").strip()
    artifact_hash = str(payload.get("artifact_sha256") or "").lower()
    artifact_value = str(payload.get("artifact_path") or "").strip()
    artifact_relative = Path(artifact_value)
    if (
        not version
        or not SHA256_PATTERN.fullmatch(artifact_hash)
        or not artifact_value
        or artifact_relative.is_absolute()
        or artifact_relative.parent != Path(".")
        or ".." in artifact_relative.parts
    ):
        raise ValueError("candidate package artifact identity is invalid")
    if payload.get("artifact_format") != MODEL_ARTIFACT_FORMAT:
        raise ValueError("candidate package artifact format is unsupported")
    try:
        training_rows = int(payload.get("training_rows"))
        training_epochs = int(payload.get("training_epochs"))
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("candidate package training counters are invalid") from exc
    training_device = str(payload.get("training_device") or "").lower()
    if training_rows < 1 or training_epochs < 1 or training_device not in {"cpu", "cuda"}:
        raise ValueError("candidate package training evidence is invalid")
    source_hashes = _canonical_hashes(payload.get("source_sha256s"), field="source_sha256s")
    label_hashes = _canonical_hashes(
        payload.get("label_source_artifact_sha256s"),
        field="label_source_artifact_sha256s",
    )
    surface_artifact_hash = str(
        payload.get("surface_artifact_sha256") or ""
    ).lower()
    surface_replay_hash = str(
        payload.get("surface_replay_receipt_sha256") or ""
    ).lower()
    if not SHA256_PATTERN.fullmatch(
        surface_artifact_hash
    ) or not SHA256_PATTERN.fullmatch(surface_replay_hash):
        raise ValueError("candidate package surface replay provenance is invalid")
    deployment = payload.get("deployment_calibration")
    if not isinstance(deployment, dict):
        raise ValueError("candidate package deployment calibration is missing")
    expected_deployment_fields = {
        "method", "alpha", "target_coverage", "asof_utc", "model_version",
        "artifact_sha256", "eligible_rows", "excluded_future_rows",
        "evidence_sha256", "source_sha256s",
        "label_source_artifact_sha256s", "family_radii",
    }
    if set(deployment) != expected_deployment_fields:
        raise ValueError("candidate package calibration fields do not match its contract")
    if deployment.get("method") != DEPLOYMENT_CALIBRATION_METHOD:
        raise ValueError("candidate package calibration method is unsupported")
    if str(deployment.get("model_version") or "") != version:
        raise ValueError("candidate package calibration model version does not match")
    if str(deployment.get("artifact_sha256") or "").lower() != artifact_hash:
        raise ValueError("candidate package calibration artifact hash does not match")
    if not SHA256_PATTERN.fullmatch(
        str(deployment.get("evidence_sha256") or "").lower()
    ):
        raise ValueError("candidate package calibration evidence SHA-256 is invalid")
    calibration_sources = _canonical_hashes(
        deployment.get("source_sha256s"), field="deployment source_sha256s"
    )
    calibration_labels = _canonical_hashes(
        deployment.get("label_source_artifact_sha256s"),
        field="deployment label_source_artifact_sha256s",
    )
    if not set(calibration_sources) <= set(source_hashes):
        raise ValueError("candidate calibration DBN sources are outside training evidence")
    if not set(calibration_labels) <= set(label_hashes):
        raise ValueError("candidate calibration close sources are outside training evidence")
    try:
        alpha = float(deployment.get("alpha"))
        target_coverage = float(deployment.get("target_coverage"))
        eligible_rows = int(deployment.get("eligible_rows"))
        excluded_future_rows = int(deployment.get("excluded_future_rows"))
        asof_utc = datetime.fromisoformat(
            str(deployment.get("asof_utc") or "").replace("Z", "+00:00")
        )
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("candidate package calibration counters are invalid") from exc
    if (
        not math.isfinite(alpha)
        or not math.isfinite(target_coverage)
        or not 0 < alpha < 1
        or not math.isclose(target_coverage, 1 - alpha, abs_tol=1e-12)
        or eligible_rows < len(PRODUCTION_FAMILIES)
        or excluded_future_rows < 0
        or asof_utc.tzinfo is None
    ):
        raise ValueError("candidate package calibration evidence is invalid")
    radii = deployment.get("family_radii")
    if (
        not isinstance(radii, (list, tuple))
        or len(radii) != len(PRODUCTION_FAMILIES)
        or any(
            not isinstance(item, dict)
            or set(item) != {"family_root", "rows", "sessions", "radius_log_return"}
            for item in radii
        )
    ):
        raise ValueError("candidate package calibration families are incomplete")
    family_roots: set[str] = set()
    radius_rows = 0
    for item in radii:
        family = str(item["family_root"] or "").upper()
        try:
            rows = int(item["rows"])
            sessions = int(item["sessions"])
            radius = float(item["radius_log_return"])
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("candidate package calibration radius is invalid") from exc
        if rows < 1 or sessions < 1 or sessions > rows or not math.isfinite(radius) or radius < 0:
            raise ValueError("candidate package calibration radius is invalid")
        family_roots.add(family)
        radius_rows += rows
    if family_roots != set(PRODUCTION_FAMILIES) or radius_rows != eligible_rows:
        raise ValueError("candidate package calibration families are incomplete")
    return payload


def load_candidate_package_receipt(path: str | Path) -> dict[str, object]:
    receipt_path = Path(path)
    size = receipt_path.stat().st_size
    if size <= 0 or size > MAX_CANDIDATE_RECEIPT_BYTES:
        raise ValueError("candidate package receipt size is outside the allowed range")
    try:
        receipt_bytes = receipt_path.read_bytes()
        payload = json.loads(receipt_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("candidate package receipt is not valid UTF-8 JSON") from exc
    validated = parse_candidate_package_receipt(payload)
    if receipt_bytes != _canonical_json_bytes(validated):
        raise ValueError("candidate package receipt is not canonical JSON")
    return validated


def parse_paper_candidate_activation_receipt(
    payload: object,
) -> dict[str, object]:
    """Validate one content-addressed, future-only paper activation receipt."""

    if not isinstance(payload, dict):
        raise ValueError("paper activation receipt root must be an object")
    expected = {
        "contract_version",
        "activated_at_utc",
        "candidate_package_path",
        "candidate_package_sha256",
        "model_version",
        "artifact_sha256",
    }
    if set(payload) != expected:
        raise ValueError("paper activation receipt fields do not match its contract")
    if (
        payload.get("contract_version")
        != PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION
    ):
        raise ValueError("paper activation receipt contract is unsupported")
    package_hash = str(payload.get("candidate_package_sha256") or "").lower()
    artifact_hash = str(payload.get("artifact_sha256") or "").lower()
    package_path = str(payload.get("candidate_package_path") or "")
    if (
        not SHA256_PATTERN.fullmatch(package_hash)
        or not SHA256_PATTERN.fullmatch(artifact_hash)
        or package_path != f"candidate_packages/{package_hash}.json"
        or not str(payload.get("model_version") or "").strip()
    ):
        raise ValueError("paper activation candidate identity is invalid")
    activated_raw = str(payload.get("activated_at_utc") or "")
    try:
        activated = datetime.fromisoformat(activated_raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("paper activation timestamp is invalid") from exc
    if activated.tzinfo is None or activated_raw != _canonical_utc(activated):
        raise ValueError("paper activation timestamp must be canonical UTC")
    return payload


def load_paper_candidate_activation_receipt(
    path: str | Path,
) -> dict[str, object]:
    receipt_path = Path(path)
    size = receipt_path.stat().st_size
    if size <= 0 or size > MAX_CANDIDATE_RECEIPT_BYTES:
        raise ValueError("paper activation receipt size is outside the allowed range")
    try:
        receipt_bytes = receipt_path.read_bytes()
        payload = json.loads(receipt_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("paper activation receipt is not valid UTF-8 JSON") from exc
    validated = parse_paper_candidate_activation_receipt(payload)
    if receipt_bytes != _canonical_json_bytes(validated):
        raise ValueError("paper activation receipt is not canonical JSON")
    return validated


def write_paper_candidate_descriptor(
    project_root: str | Path,
    package: FrozenCandidatePackage,
    *,
    allow_replace: bool = False,
) -> Path:
    """Explicitly activate one immutable candidate receipt for paper forecasts."""
    models_dir = (Path(project_root).resolve() / "models").resolve()
    receipts_dir = (models_dir / "candidate_packages").resolve()
    receipt_path = Path(package.package_receipt_path).resolve()
    try:
        receipt_path.relative_to(receipts_dir)
    except ValueError as exc:
        raise ValueError("candidate package receipt is outside the project models directory") from exc
    receipt_hash = str(package.package_receipt_sha256).lower()
    if (
        receipt_path.parent != receipts_dir
        or receipt_path.name != f"{receipt_hash}.json"
        or not SHA256_PATTERN.fullmatch(receipt_hash)
        or not receipt_path.is_file()
    ):
        raise ValueError("candidate package receipt identity is invalid")
    receipt_bytes = receipt_path.read_bytes()
    if hashlib.sha256(receipt_bytes).hexdigest() != receipt_hash:
        raise ValueError("candidate package receipt hash does not match")
    receipt = load_candidate_package_receipt(receipt_path)
    artifact_path = (models_dir / str(receipt["artifact_path"])).resolve()
    if artifact_path != Path(package.artifact_path).resolve():
        raise ValueError("candidate package artifact path does not match the receipt")
    if str(receipt["version"]) != package.version:
        raise ValueError("candidate package version does not match the receipt")
    if str(receipt["artifact_sha256"]).lower() != package.artifact_sha256.lower():
        raise ValueError("candidate package artifact hash does not match the receipt")
    if not artifact_path.is_file():
        raise ValueError("candidate package artifact is missing")
    if hashlib.sha256(artifact_path.read_bytes()).hexdigest() != package.artifact_sha256:
        raise ValueError("candidate package artifact bytes do not match the receipt")
    artifact = load_frozen_model_artifact(artifact_path)
    if (
        artifact.training_rows != int(receipt["training_rows"])
        or artifact.training_epochs != int(receipt["training_epochs"])
        or artifact.training_device != str(receipt["training_device"])
        or artifact.source_sha256s != tuple(receipt["source_sha256s"])
        or artifact.label_source_artifact_sha256s
        != tuple(receipt["label_source_artifact_sha256s"])
        or artifact.surface_artifact_sha256
        != str(receipt["surface_artifact_sha256"])
        or artifact.surface_replay_receipt_sha256
        != str(receipt["surface_replay_receipt_sha256"])
        or artifact.surface_artifact_sha256 != package.surface_artifact_sha256
        or artifact.surface_replay_receipt_sha256
        != package.surface_replay_receipt_sha256
    ):
        raise ValueError("candidate package artifact provenance does not match the receipt")
    destination = models_dir / "closing_tape_paper_candidate.json"
    if destination.exists():
        try:
            current_descriptor = json.loads(destination.read_text(encoding="utf-8"))
            expected_descriptor_fields = {
                "contract_version",
                "enabled_for_paper",
                "activation_receipt_path",
                "activation_receipt_sha256",
            }
            if (
                not isinstance(current_descriptor, dict)
                or set(current_descriptor) != expected_descriptor_fields
            ):
                raise ValueError("paper candidate descriptor fields are invalid")
            activation_hash = str(
                current_descriptor.get("activation_receipt_sha256") or ""
            ).lower()
            activation_relative = str(
                current_descriptor.get("activation_receipt_path") or ""
            )
            activation_path = (models_dir / activation_relative).resolve()
            activation_dir = (models_dir / "paper_activations").resolve()
            if (
                not SHA256_PATTERN.fullmatch(activation_hash)
                or activation_relative
                != f"paper_activations/{activation_hash}.json"
                or activation_path.parent != activation_dir
            ):
                raise ValueError("paper candidate activation identity is invalid")
            current_activation = load_paper_candidate_activation_receipt(
                activation_path
            )
            same_candidate = (
                current_descriptor.get("contract_version")
                == PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION
                and current_descriptor.get("enabled_for_paper") is True
                and activation_relative
                == f"paper_activations/{activation_hash}.json"
                and hashlib.sha256(activation_path.read_bytes()).hexdigest()
                == activation_hash
                and current_activation["candidate_package_sha256"]
                == receipt_hash
                and current_activation["model_version"] == package.version
                and current_activation["artifact_sha256"]
                == package.artifact_sha256
            )
        except (AttributeError, KeyError, OSError, ValueError):
            same_candidate = False
        if same_candidate:
            return destination
        if not allow_replace:
            raise FileExistsError(
                "a different paper candidate descriptor exists; explicit replacement is required"
            )
    activation = {
        "contract_version": PAPER_CANDIDATE_ACTIVATION_CONTRACT_VERSION,
        "activated_at_utc": _canonical_utc(_utc_now()),
        "candidate_package_path": f"candidate_packages/{receipt_path.name}",
        "candidate_package_sha256": receipt_hash,
        "model_version": package.version,
        "artifact_sha256": package.artifact_sha256,
    }
    activation_bytes = _canonical_json_bytes(activation)
    activation_hash = hashlib.sha256(activation_bytes).hexdigest()
    activation_dir = models_dir / "paper_activations"
    activation_dir.mkdir(parents=True, exist_ok=True)
    activation_path = activation_dir / f"{activation_hash}.json"
    if activation_path.exists():
        if activation_path.read_bytes() != activation_bytes:
            raise ValueError("paper activation receipt hash collision")
    else:
        try:
            with activation_path.open("xb") as stream:
                stream.write(activation_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        except FileExistsError:
            if activation_path.read_bytes() != activation_bytes:
                raise ValueError("paper activation receipt hash collision") from None
    descriptor = {
        "contract_version": PAPER_CANDIDATE_DESCRIPTOR_CONTRACT_VERSION,
        "enabled_for_paper": True,
        "activation_receipt_path": f"paper_activations/{activation_path.name}",
        "activation_receipt_sha256": activation_hash,
    }
    serialized = json.dumps(descriptor, sort_keys=True, indent=2, allow_nan=False) + "\n"
    temporary = destination.with_name(
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def package_promoted_candidate(
    evaluation_result: GuardedModelEvaluationResult,
    *,
    version: str,
    artifact_path: str | Path,
    calibration_asof_utc: datetime,
    device: str | None = None,
    alpha: float = 0.1,
    minimum_family_sessions: int = 20,
    allow_replace: bool = False,
) -> FrozenCandidatePackage:
    """Freeze one promoted evaluation and its OOS deployment calibration.

    This writes a candidate artifact only. It does not publish or enable the
    production manifest, which still requires paper and regime evidence.
    """
    try:
        require_guarded_model_evaluation_result(evaluation_result)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "candidate packaging requires an authoritative guarded evaluation result"
        ) from exc
    report = evaluation_result.evaluation_report
    frame = getattr(evaluation_result, "labeled_training_frame", None)
    if not bool(getattr(evaluation_result, "trained", False)) or report is None or frame is None:
        raise ValueError("guarded evaluation has no reusable trained evidence")
    replay_surface = getattr(evaluation_result, "replay_verified_surface", None)
    try:
        surface_identity = require_replay_verified_surface_identity(replay_surface)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "candidate packaging requires independently replay-verified surface evidence"
        ) from exc
    if (
        str(getattr(evaluation_result, "surface_artifact_sha256", "")).lower()
        != surface_identity.surface_artifact_sha256
        or str(
            getattr(evaluation_result, "surface_replay_receipt_sha256", "")
        ).lower()
        != surface_identity.surface_replay_receipt_sha256
    ):
        raise ValueError("guarded evaluation surface replay identity is inconsistent")
    if not bool(getattr(report, "promoted", False)):
        raise ValueError("walk-forward evaluation did not promote the candidate")
    candidate_version = str(version).strip()
    if not candidate_version:
        raise ValueError("candidate version is required")

    build: FrozenModelBuild = fit_promoted_frozen_model(
        frame,
        report,
        surface_artifact_sha256=surface_identity.surface_artifact_sha256,
        surface_replay_receipt_sha256=(
            surface_identity.surface_replay_receipt_sha256
        ),
        device=device,
    )
    bound = bind_candidate_oos_predictions(
        report, model_version=candidate_version, artifact_sha256=build.sha256
    )
    calibration = fit_deployment_conformal_calibration(
        bound,
        prediction_column="torch_predicted_log_return",
        asof_utc=calibration_asof_utc,
        alpha=alpha,
        minimum_family_sessions=minimum_family_sessions,
        expected_families=tuple(PRODUCTION_FAMILIES),
    )
    evaluation_label_artifacts = {
        str(value).lower()
        for value in getattr(report, "label_source_artifact_sha256s", ())
    }
    if not set(calibration.label_source_artifact_sha256s) <= evaluation_label_artifacts:
        raise ValueError(
            "candidate calibration close artifacts are outside walk-forward evidence"
        )

    destination = Path(artifact_path).resolve()
    if destination.exists():
        with destination.open("rb") as handle:
            current_hash = hashlib.file_digest(handle, "sha256").hexdigest()
        if current_hash != build.sha256 and not allow_replace:
            raise FileExistsError(
                "a different frozen candidate artifact already exists; explicit replacement is required"
            )
    written = build.write(destination)
    retain_surface_promotion_evidence(
        replay_surface,
        models_dir=destination.parent,
    )
    receipt_payload = {
        "contract_version": CANDIDATE_PACKAGE_CONTRACT_VERSION,
        "version": candidate_version,
        "artifact_path": written.name,
        "artifact_sha256": build.sha256,
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "training_rows": build.training_rows,
        "training_epochs": build.epochs,
        "training_device": build.training_device,
        "source_sha256s": list(build.source_sha256s),
        "label_source_artifact_sha256s": list(
            build.label_source_artifact_sha256s
        ),
        "surface_artifact_sha256": build.surface_artifact_sha256,
        "surface_replay_receipt_sha256": (
            build.surface_replay_receipt_sha256
        ),
        "deployment_calibration": calibration.to_dict(),
    }
    parse_candidate_package_receipt(receipt_payload)
    receipt_bytes = _canonical_json_bytes(receipt_payload)
    receipt_hash = hashlib.sha256(receipt_bytes).hexdigest()
    receipts_dir = destination.parent / "candidate_packages"
    receipts_dir.mkdir(parents=True, exist_ok=True)
    receipt_path = receipts_dir / f"{receipt_hash}.json"
    try:
        with receipt_path.open("xb") as handle:
            handle.write(receipt_bytes)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if receipt_path.read_bytes() != receipt_bytes:
            raise ValueError("conflicting immutable candidate package receipt")
    return FrozenCandidatePackage(
        version=candidate_version,
        artifact_path=str(written),
        artifact_sha256=build.sha256,
        training_rows=build.training_rows,
        training_epochs=build.epochs,
        training_device=build.training_device,
        source_sha256s=build.source_sha256s,
        label_source_artifact_sha256s=build.label_source_artifact_sha256s,
        surface_artifact_sha256=build.surface_artifact_sha256,
        surface_replay_receipt_sha256=build.surface_replay_receipt_sha256,
        package_receipt_path=str(receipt_path.resolve()),
        package_receipt_sha256=receipt_hash,
        deployment_calibration=calibration,
    )
