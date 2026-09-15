from __future__ import annotations

import json
import os
import re
import math
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from .dataset import PRODUCTION_FAMILIES
from .calibration import DEPLOYMENT_CALIBRATION_METHOD
from .contracts import EVIDENCE_CONTRACT_VERSION
from .model_artifact import (
    MODEL_ARTIFACT_FORMAT,
    MODEL_MANIFEST_CONTRACT_VERSION,
    FrozenInferenceBenchmark,
)
from .surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH


SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
PROMOTION_APPROVAL_CONTRACT_VERSION = "closing-tape-promotion-approval-v1"
MAX_PROMOTION_APPROVAL_BYTES = 64 * 1024
MAX_PROMOTION_APPROVAL_FUTURE_SKEW = timedelta(minutes=5)


def _canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("promotion evidence must be strict canonical JSON") from exc


def promotion_proposal_sha256(manifest: dict[str, object]) -> str:
    """Hash the exact proposed manifest while excluding only its approval reference."""
    if not isinstance(manifest, dict):
        raise ValueError("promotion manifest root must be an object")
    proposal = dict(manifest)
    proposal.pop("promotion_approval", None)
    return hashlib.sha256(_canonical_json_bytes(proposal)).hexdigest()


def _approval_identity(manifest: dict[str, object]) -> dict[str, str]:
    deployment = manifest.get("deployment_calibration")
    if not isinstance(deployment, dict):
        raise ValueError("deployment calibration is required before an approval decision")
    version = str(manifest.get("version") or "").strip()
    artifact_hash = str(manifest.get("artifact_sha256") or "").strip().lower()
    calibration_hash = str(deployment.get("evidence_sha256") or "").strip().lower()
    evidence_version = str(manifest.get("evidence_contract_version") or "").strip()
    feature_hash = str(manifest.get("feature_schema_hash") or "").strip().lower()
    if not version or not evidence_version:
        raise ValueError("model version and evidence contract are required for approval")
    for field, value in (
        ("artifact_sha256", artifact_hash),
        ("calibration_evidence_sha256", calibration_hash),
        ("feature_schema_hash", feature_hash),
    ):
        if not SHA256_PATTERN.fullmatch(value):
            raise ValueError(f"{field} is missing or invalid")
    return {
        "model_version": version,
        "artifact_sha256": artifact_hash,
        "calibration_evidence_sha256": calibration_hash,
        "evidence_contract_version": evidence_version,
        "feature_schema_hash": feature_hash,
    }


def _approval_time(value: datetime | None) -> str:
    observed = value or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError("promotion approval timestamp must be timezone-aware")
    canonical = observed.astimezone(timezone.utc)
    if canonical > datetime.now(timezone.utc) + MAX_PROMOTION_APPROVAL_FUTURE_SKEW:
        raise ValueError("promotion approval timestamp is unreasonably in the future")
    return canonical.isoformat()


def load_promotion_approval_receipt(
    project_root: str | Path,
    receipt_sha256: str,
) -> dict[str, object]:
    """Load and cryptographically verify one immutable approval receipt."""
    receipt_hash = str(receipt_sha256 or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(receipt_hash):
        raise ValueError("promotion approval receipt SHA-256 is invalid")
    approvals_dir = (
        Path(project_root).resolve() / "models" / "promotion_approvals"
    ).resolve()
    receipt_path = (approvals_dir / f"{receipt_hash}.json").resolve()
    receipt_path.relative_to(approvals_dir)
    if receipt_path.parent != approvals_dir or not receipt_path.is_file():
        raise ValueError("promotion approval receipt file is missing")
    size = receipt_path.stat().st_size
    if size <= 0 or size > MAX_PROMOTION_APPROVAL_BYTES:
        raise ValueError("promotion approval receipt size is outside the allowed range")
    receipt_bytes = receipt_path.read_bytes()
    if hashlib.sha256(receipt_bytes).hexdigest() != receipt_hash:
        raise ValueError("promotion approval receipt content hash does not match")
    try:
        receipt = json.loads(receipt_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("promotion approval receipt is not valid JSON") from exc
    if not isinstance(receipt, dict):
        raise ValueError("promotion approval receipt root must be an object")
    expected_keys = {
        "contract_version", "decision", "approved_by", "approved_at_utc",
        "proposal_sha256", "model_version", "artifact_sha256",
        "calibration_evidence_sha256", "evidence_contract_version",
        "feature_schema_hash",
    }
    if set(receipt) != expected_keys:
        raise ValueError("promotion approval receipt fields do not match its contract")
    if receipt.get("contract_version") != PROMOTION_APPROVAL_CONTRACT_VERSION:
        raise ValueError("promotion approval receipt contract is unsupported")
    if receipt.get("decision") != "APPROVE":
        raise ValueError("promotion decision is not APPROVE")
    if not str(receipt.get("approved_by") or "").strip():
        raise ValueError("promotion approval operator identity is missing")
    for field in (
        "proposal_sha256", "artifact_sha256", "calibration_evidence_sha256",
        "feature_schema_hash",
    ):
        value = str(receipt.get(field) or "").strip().lower()
        if not SHA256_PATTERN.fullmatch(value):
            raise ValueError(f"promotion approval {field} is invalid")
    approved_at = datetime.fromisoformat(
        str(receipt.get("approved_at_utc") or "").replace("Z", "+00:00")
    )
    if approved_at.tzinfo is None:
        raise ValueError("promotion approval timestamp must be timezone-aware")
    if (
        approved_at.astimezone(timezone.utc)
        > datetime.now(timezone.utc) + MAX_PROMOTION_APPROVAL_FUTURE_SKEW
    ):
        raise ValueError("promotion approval timestamp is unreasonably in the future")
    return dict(receipt)


def record_promotion_decision(
    project_root: str | Path,
    manifest: dict[str, object],
    *,
    approved_by: str,
    decision: str,
    approved_at_utc: datetime | None = None,
) -> dict[str, str]:
    """Write one immutable, content-addressed human promotion decision receipt.

    This records an explicit local attestation; it does not authenticate the
    supplied operator identity. Publication remains a separate operation.
    """
    operator = str(approved_by or "").strip()
    normalized_decision = str(decision or "").strip().upper()
    if not operator or len(operator) > 200:
        raise ValueError("approved_by must identify the local approving operator")
    if normalized_decision not in {"APPROVE", "REJECT"}:
        raise ValueError("promotion decision must be APPROVE or REJECT")
    identity = _approval_identity(manifest)
    approved_at = _approval_time(approved_at_utc)
    deployment = manifest["deployment_calibration"]
    calibration_asof = datetime.fromisoformat(
        str(deployment.get("asof_utc") or "").replace("Z", "+00:00")
    )
    if calibration_asof.tzinfo is None:
        raise ValueError("deployment calibration as-of timestamp must be timezone-aware")
    if datetime.fromisoformat(approved_at) < calibration_asof.astimezone(timezone.utc):
        raise ValueError("promotion decision cannot predate deployment calibration evidence")
    proposal_hash = promotion_proposal_sha256(manifest)
    receipt = {
        "contract_version": PROMOTION_APPROVAL_CONTRACT_VERSION,
        "decision": normalized_decision,
        "approved_by": operator,
        "approved_at_utc": approved_at,
        "proposal_sha256": proposal_hash,
        **identity,
    }
    receipt_bytes = _canonical_json_bytes(receipt) + b"\n"
    receipt_hash = hashlib.sha256(receipt_bytes).hexdigest()
    approvals_dir = Path(project_root).resolve() / "models" / "promotion_approvals"
    approvals_dir.mkdir(parents=True, exist_ok=True)
    destination = approvals_dir / f"{receipt_hash}.json"
    try:
        with destination.open("xb") as handle:
            handle.write(receipt_bytes)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        if destination.read_bytes() != receipt_bytes:
            raise ValueError("conflicting immutable promotion approval receipt")
    return {
        "contract_version": PROMOTION_APPROVAL_CONTRACT_VERSION,
        "proposal_sha256": proposal_hash,
        "receipt_path": f"promotion_approvals/{destination.name}",
        "receipt_sha256": receipt_hash,
    }


def validate_promotion_approval(
    project_root: str | Path,
    manifest: dict[str, object],
) -> dict[str, object]:
    """Validate that the manifest is bound to one explicit APPROVE receipt."""
    reference = manifest.get("promotion_approval")
    if not isinstance(reference, dict):
        return {"passed": False, "reason": "human promotion approval receipt is missing"}
    try:
        if reference.get("contract_version") != PROMOTION_APPROVAL_CONTRACT_VERSION:
            raise ValueError("promotion approval contract is missing or unsupported")
        proposal_hash = promotion_proposal_sha256(manifest)
        if str(reference.get("proposal_sha256") or "").lower() != proposal_hash:
            raise ValueError("promotion approval does not match the proposed manifest")
        receipt_hash = str(reference.get("receipt_sha256") or "").lower()
        if not SHA256_PATTERN.fullmatch(receipt_hash):
            raise ValueError("promotion approval receipt SHA-256 is invalid")
        models_dir = (Path(project_root).resolve() / "models").resolve()
        approvals_dir = (models_dir / "promotion_approvals").resolve()
        receipt_path = (models_dir / str(reference.get("receipt_path") or "")).resolve()
        receipt_path.relative_to(approvals_dir)
        if receipt_path.parent != approvals_dir or receipt_path.name != f"{receipt_hash}.json":
            raise ValueError("promotion approval receipt path is not content-addressed")
        if not receipt_path.is_file():
            raise ValueError("promotion approval receipt file is missing")
        size = receipt_path.stat().st_size
        if size <= 0 or size > MAX_PROMOTION_APPROVAL_BYTES:
            raise ValueError("promotion approval receipt size is outside the allowed range")
        receipt = load_promotion_approval_receipt(project_root, receipt_hash)
        if str(receipt.get("proposal_sha256") or "").lower() != proposal_hash:
            raise ValueError("promotion approval receipt is bound to another manifest")
        identity = _approval_identity(manifest)
        for field, expected in identity.items():
            if str(receipt.get(field) or "").lower() != expected.lower():
                raise ValueError(f"promotion approval {field} does not match the manifest")
        approved_at = datetime.fromisoformat(
            str(receipt.get("approved_at_utc") or "").replace("Z", "+00:00")
        )
        if approved_at.tzinfo is None:
            raise ValueError("promotion approval timestamp must be timezone-aware")
        if (
            approved_at.astimezone(timezone.utc)
            > datetime.now(timezone.utc) + MAX_PROMOTION_APPROVAL_FUTURE_SKEW
        ):
            raise ValueError("promotion approval timestamp is unreasonably in the future")
        deployment = manifest.get("deployment_calibration") or {}
        calibration_asof = datetime.fromisoformat(
            str(deployment.get("asof_utc") or "").replace("Z", "+00:00")
        )
        if calibration_asof.tzinfo is None or approved_at < calibration_asof:
            raise ValueError("promotion approval predates deployment calibration evidence")
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        return {"passed": False, "reason": str(exc)}
    return {
        "passed": True,
        "reason": None,
        "proposal_sha256": proposal_hash,
        "receipt_sha256": receipt_hash,
        "approved_by": str(receipt["approved_by"]),
        "approved_at_utc": approved_at.astimezone(timezone.utc).isoformat(),
    }


def write_promoted_model_manifest(
    project_root: str | Path,
    manifest: dict[str, object],
    *,
    allow_replace: bool = False,
) -> Path:
    """Atomically publish only a manifest that passes the complete runtime gate."""
    root = Path(project_root).resolve()
    models = root / "models"
    models.mkdir(parents=True, exist_ok=True)
    destination = models / "closing_tape_model.json"
    try:
        text = json.dumps(manifest, sort_keys=True, indent=2, allow_nan=False) + "\n"
    except (TypeError, ValueError) as exc:
        raise ValueError("promoted model manifest is not strict JSON") from exc
    snapshot = text.encode("utf-8")
    snapshot_sha256 = hashlib.sha256(snapshot).hexdigest()
    temporary = models / (
        f".{destination.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    with temporary.open("xb") as stream:
        stream.write(snapshot)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        from .status import _model_gate

        gate = _model_gate(
            root,
            manifest_path=temporary,
            manifest_bytes=snapshot,
        )
        if not bool(gate.get("passed")):
            raise ValueError(f"promoted model manifest failed runtime gate: {gate.get('reason')}")
        if str(gate.get("manifest_sha256") or "").lower() != snapshot_sha256:
            raise ValueError("runtime gate did not validate the publication byte snapshot")
        if destination.exists():
            current = destination.read_bytes()
            proposed = snapshot
            if current == proposed:
                return destination
            if not allow_replace:
                raise FileExistsError(
                    "a different promoted model manifest already exists; explicit replacement is required"
                )
        if allow_replace:
            os.replace(temporary, destination)
        else:
            try:
                # A same-filesystem hard link is an atomic create-if-absent.
                # It prevents two same-process publishers from both observing
                # a missing destination and silently replacing one another.
                os.link(temporary, destination)
            except FileExistsError:
                current = destination.read_bytes()
                proposed = snapshot
                if current != proposed:
                    raise FileExistsError(
                        "a different promoted model manifest already exists; "
                        "explicit replacement is required"
                    ) from None
            else:
                temporary.unlink()
    finally:
        if temporary.exists():
            temporary.unlink()
    return destination


def approve_and_write_promoted_model_manifest(
    project_root: str | Path,
    manifest: dict[str, object],
    *,
    approved_by: str,
    expected_proposal_sha256: str,
    approved_at_utc: datetime | None = None,
    allow_replace: bool = False,
) -> Path:
    """Content-confirm, approve, and publish a manifest as separate human action.

    Callers must present the proposal hash they reviewed. This prevents a
    background challenger or stale UI action from approving different bytes.
    """
    if not isinstance(manifest, dict):
        raise ValueError("promotion manifest root must be an object")
    proposed = dict(manifest)
    proposed.pop("promotion_approval", None)
    actual_proposal = promotion_proposal_sha256(proposed)
    expected = str(expected_proposal_sha256 or "").strip().lower()
    if not SHA256_PATTERN.fullmatch(expected) or expected != actual_proposal:
        raise ValueError("operator-confirmed proposal SHA-256 does not match the manifest")
    approved = dict(proposed)
    approved["promotion_approval"] = record_promotion_decision(
        project_root,
        proposed,
        approved_by=approved_by,
        decision="APPROVE",
        approved_at_utc=approved_at_utc,
    )
    return write_promoted_model_manifest(
        project_root,
        approved,
        allow_replace=allow_replace,
    )


def build_promoted_model_manifest(
    *,
    evaluation_report: object,
    calibration_report: object,
    deployment_calibration: object,
    candidate_regime_report: object,
    ridge_regime_report: object,
    source_sha256s: Iterable[str],
    artifact_path: str,
    artifact_sha256: str,
    surface_artifact_sha256: str,
    surface_replay_receipt_sha256: str,
    paper_report: object,
    version: str,
    execution_device: str = "cpu",
    inference_batch_rows: int = 5,
    inference_benchmark: FrozenInferenceBenchmark | None = None,
) -> dict[str, object]:
    """Derive an enabled manifest from connected, already-passing evidence."""
    if not bool(getattr(evaluation_report, "promoted", False)):
        raise ValueError("walk-forward evaluation is not promoted")
    if tuple(getattr(evaluation_report, "features", ())) != MODEL_FEATURE_COLUMNS:
        raise ValueError("evaluation feature evidence does not match the running contract")
    complete_sessions = int(getattr(evaluation_report, "complete_sessions", 0))
    folds = tuple(getattr(evaluation_report, "folds", ()))
    if complete_sessions < 60 or len(folds) < 5:
        raise ValueError("model promotion requires 60 sessions and at least 5 folds")
    paper_sessions = int(getattr(paper_report, "complete_sessions", 0))
    if paper_sessions < 20:
        raise ValueError("model promotion requires at least 20 paper sessions")
    hashes = sorted({str(value).lower() for value in source_sha256s})
    if len(hashes) < 60 or any(not SHA256_PATTERN.fullmatch(value) for value in hashes):
        raise ValueError("model promotion requires at least 60 unique valid source hashes")
    evaluation_source_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(evaluation_report, "source_sha256s", ())
        }
    )
    if hashes != evaluation_source_hashes:
        raise ValueError(
            "promotion DBN source hashes do not match walk-forward evidence"
        )
    label_artifact_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(
                evaluation_report, "label_source_artifact_sha256s", ()
            )
        }
    )
    if not label_artifact_hashes or any(
        not SHA256_PATTERN.fullmatch(value) for value in label_artifact_hashes
    ):
        raise ValueError(
            "model promotion requires valid verified-close artifact hashes"
        )
    artifact_hash = str(artifact_sha256).lower()
    if not artifact_path or not SHA256_PATTERN.fullmatch(artifact_hash):
        raise ValueError("artifact path and SHA-256 are required")
    surface_artifact_hash = str(surface_artifact_sha256 or "").lower()
    surface_replay_hash = str(surface_replay_receipt_sha256 or "").lower()
    if not SHA256_PATTERN.fullmatch(
        surface_artifact_hash
    ) or not SHA256_PATTERN.fullmatch(surface_replay_hash):
        raise ValueError("surface artifact and replay receipt SHA-256 are required")
    if str(getattr(paper_report, "model_version", "")) != version:
        raise ValueError("paper evidence model version does not match the manifest")
    if str(getattr(paper_report, "artifact_sha256", "")).lower() != artifact_hash:
        raise ValueError("paper evidence artifact hash does not match the manifest")
    paper_label_artifact_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(
                paper_report, "close_source_artifact_sha256s", ()
            )
        }
    )
    if not paper_label_artifact_hashes or any(
        not SHA256_PATTERN.fullmatch(value) for value in paper_label_artifact_hashes
    ):
        raise ValueError("paper evidence verified-close artifact hashes are missing or invalid")
    paper_evidence_hash = str(
        getattr(paper_report, "evidence_sha256", "")
    ).lower()
    if not SHA256_PATTERN.fullmatch(paper_evidence_hash):
        raise ValueError("paper evidence SHA-256 is missing or invalid")
    paper_activation_receipt_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(
                paper_report, "activation_receipt_sha256s", ()
            )
        }
    )
    if len(paper_activation_receipt_hashes) != 1 or any(
        not SHA256_PATTERN.fullmatch(value)
        for value in paper_activation_receipt_hashes
    ):
        raise ValueError(
            "paper evidence must bind exactly one valid candidate activation receipt"
        )
    paper_prefix_replay_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(
                paper_report, "prefix_replay_receipt_sha256s", ()
            )
        }
    )
    paper_final_tape_hashes = sorted(
        {
            str(value).lower()
            for value in getattr(paper_report, "final_tape_source_sha256s", ())
        }
    )
    campaign_opportunities = int(
        getattr(paper_report, "campaign_opportunities", 0)
    )
    campaign_coverage_hash = str(
        getattr(paper_report, "campaign_coverage_receipt_sha256", "")
    ).lower()
    if (
        campaign_opportunities < paper_sessions
        or len(paper_prefix_replay_hashes) != campaign_opportunities
        or len(paper_final_tape_hashes) != campaign_opportunities
        or any(
            not SHA256_PATTERN.fullmatch(value)
            for value in (*paper_prefix_replay_hashes, *paper_final_tape_hashes)
        )
        or not SHA256_PATTERN.fullmatch(campaign_coverage_hash)
    ):
        raise ValueError(
            "paper evidence has incomplete prefix replay or campaign coverage proof"
        )

    families = {str(value).upper() for value in PRODUCTION_FAMILIES}
    family_metrics = {str(item.family_root).upper(): item for item in evaluation_report.family_metrics}
    if set(family_metrics) != families:
        raise ValueError("evaluation must contain exactly the production families")
    calibration_metrics = {
        str(item.family_root).upper(): item for item in calibration_report.family_metrics
    }
    if set(calibration_metrics) != families:
        raise ValueError("calibration must contain exactly the production families")
    if str(getattr(deployment_calibration, "method", "")) != DEPLOYMENT_CALIBRATION_METHOD:
        raise ValueError("deployment calibration method is missing or unsupported")
    if str(getattr(deployment_calibration, "model_version", "")) != version:
        raise ValueError("deployment calibration model version does not match the manifest")
    if str(getattr(deployment_calibration, "artifact_sha256", "")).lower() != artifact_hash:
        raise ValueError("deployment calibration artifact hash does not match the manifest")
    deployment_alpha = float(getattr(deployment_calibration, "alpha", float("nan")))
    deployment_target = float(getattr(deployment_calibration, "target_coverage", float("nan")))
    if not math.isfinite(deployment_alpha) or not 0 < deployment_alpha < 1:
        raise ValueError("deployment calibration alpha is invalid")
    if not math.isclose(deployment_target, 1.0 - deployment_alpha, abs_tol=1e-12):
        raise ValueError("deployment calibration target coverage is inconsistent")
    try:
        deployment_eligible_rows = int(
            getattr(deployment_calibration, "eligible_rows")
        )
        deployment_excluded_future_rows = int(
            getattr(deployment_calibration, "excluded_future_rows")
        )
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise ValueError("deployment calibration row counters are invalid") from exc
    if (
        deployment_eligible_rows < len(families)
        or deployment_excluded_future_rows < 0
    ):
        raise ValueError("deployment calibration row counters are invalid")
    deployment_radii = {
        str(item.family_root).upper(): item
        for item in getattr(deployment_calibration, "family_radii", ())
    }
    if set(deployment_radii) != families:
        raise ValueError("deployment calibration must contain every production family")
    for family, item in deployment_radii.items():
        radius = float(item.radius_log_return)
        if int(item.sessions) < 20 or not math.isfinite(radius) or radius < 0:
            raise ValueError(f"{family} deployment calibration evidence is insufficient")
    if (
        sum(int(item.rows) for item in deployment_radii.values())
        != deployment_eligible_rows
    ):
        raise ValueError("deployment calibration row counters do not match family evidence")
    calibration_sources = {
        str(value).lower() for value in getattr(deployment_calibration, "source_sha256s", ())
    }
    if len(calibration_sources) < 20 or not calibration_sources <= set(hashes):
        raise ValueError("deployment calibration sources are incomplete or outside training evidence")
    calibration_label_artifacts = {
        str(value).lower()
        for value in getattr(
            deployment_calibration, "label_source_artifact_sha256s", ()
        )
    }
    if (
        not calibration_label_artifacts
        or any(
            not SHA256_PATTERN.fullmatch(value)
            for value in calibration_label_artifacts
        )
        or not calibration_label_artifacts <= set(label_artifact_hashes)
    ):
        raise ValueError(
            "deployment calibration close artifacts are incomplete or outside training evidence"
        )
    calibration_evidence_hash = str(
        getattr(deployment_calibration, "evidence_sha256", "")
    ).lower()
    if not SHA256_PATTERN.fullmatch(calibration_evidence_hash):
        raise ValueError("deployment calibration OOS evidence hash is missing or invalid")
    paper_metrics = {str(item.family_root).upper(): item for item in paper_report.family_metrics}
    if set(paper_metrics) != families:
        raise ValueError("paper evidence must contain exactly the production families")
    if int(getattr(paper_report, "eligible_rows", 0)) != paper_sessions * len(families):
        raise ValueError("paper evidence is not complete for every family/session")
    if float(paper_report.candidate_mae) >= float(paper_report.incumbent_mae):
        raise ValueError("paper candidate MAE does not beat the incumbent")
    for family, metric in paper_metrics.items():
        if int(metric.sessions) < 20 or float(metric.candidate_mae) >= float(metric.incumbent_mae):
            raise ValueError(f"{family} paper evidence does not beat the incumbent for 20 sessions")
    candidate_regimes = {
        (str(item.family_root).upper(), str(item.volatility_regime)): item
        for item in candidate_regime_report.metrics
    }
    ridge_regimes = {
        (str(item.family_root).upper(), str(item.volatility_regime)): item
        for item in ridge_regime_report.metrics
    }
    required_regimes = {
        (family, regime)
        for family in families
        for regime in ("calm", "normal", "stressed")
    }
    if set(candidate_regimes) != required_regimes or set(ridge_regimes) != required_regimes:
        raise ValueError("candidate and ridge reports must contain every family/regime slice")

    selected_device = execution_device.lower()
    if selected_device not in {"cpu", "cuda"} or inference_batch_rows < 1:
        raise ValueError("execution device or deployed batch size is invalid")
    benchmark_fields: dict[str, object] = {}
    if selected_device == "cuda":
        if inference_benchmark is None:
            raise ValueError("CUDA promotion requires an artifact inference benchmark")
        if (
            inference_benchmark.rows != inference_batch_rows
            or inference_benchmark.cuda_speedup_vs_cpu is None
            or inference_benchmark.cuda_speedup_vs_cpu <= 1.0
            or not inference_benchmark.numerically_equivalent
        ):
            raise ValueError("CUDA benchmark does not pass the deployed workload gate")
        benchmark_fields = {
            "cuda_speedup_vs_cpu": inference_benchmark.cuda_speedup_vs_cpu,
            "cuda_benchmark_rows": inference_benchmark.rows,
            "cuda_max_abs_prediction_difference": inference_benchmark.max_abs_prediction_difference,
            "cuda_prediction_tolerance": inference_benchmark.prediction_tolerance,
        }

    return {
        "enabled": True,
        "version": version,
        "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
        "model_manifest_contract_version": MODEL_MANIFEST_CONTRACT_VERSION,
        "feature_schema_hash": MODEL_FEATURE_CONTRACT_HASH,
        "complete_sessions": complete_sessions,
        "purged_folds": len(folds),
        "paper_sessions": int(paper_sessions),
        "paper_candidate_mae": float(paper_report.candidate_mae),
        "paper_incumbent_mae": float(paper_report.incumbent_mae),
        "paper_family_metrics": [
            {
                "family_root": family,
                "rows": int(metric.rows),
                "sessions": int(metric.sessions),
                "candidate_mae": float(metric.candidate_mae),
                "incumbent_mae": float(metric.incumbent_mae),
            }
            for family, metric in sorted(paper_metrics.items())
        ],
        "held_out_rows": int(evaluation_report.held_out_rows),
        "held_out_sessions": int(evaluation_report.held_out_sessions),
        "incumbent_rows": int(evaluation_report.incumbent_rows),
        "incumbent_sessions": int(evaluation_report.incumbent_sessions),
        "oos_improvement_pct": float(evaluation_report.torch_improvement_over_ridge_pct),
        "oos_improvement_ci_low_pct": float(evaluation_report.torch_improvement_over_ridge_ci_low_pct),
        "torch_improvement_over_incumbent_pct": float(
            evaluation_report.torch_improvement_over_incumbent_pct
        ),
        "torch_improvement_over_incumbent_ci_low_pct": float(
            evaluation_report.torch_improvement_over_incumbent_ci_low_pct
        ),
        "source_sha256s": hashes,
        "label_source_artifact_sha256s": label_artifact_hashes,
        "paper_close_source_artifact_sha256s": paper_label_artifact_hashes,
        "paper_evidence_sha256": paper_evidence_hash,
        "paper_activation_receipt_sha256s": paper_activation_receipt_hashes,
        "paper_prefix_replay_receipt_sha256s": paper_prefix_replay_hashes,
        "paper_final_tape_source_sha256s": paper_final_tape_hashes,
        "paper_campaign_opportunities": campaign_opportunities,
        "paper_campaign_coverage_receipt_sha256": campaign_coverage_hash,
        "artifact_path": artifact_path,
        "artifact_sha256": artifact_hash,
        "artifact_format": MODEL_ARTIFACT_FORMAT,
        "surface_artifact_sha256": surface_artifact_hash,
        "surface_replay_receipt_sha256": surface_replay_hash,
        "execution_device": selected_device,
        "inference_batch_rows": int(inference_batch_rows),
        **benchmark_fields,
        "family_metrics": [
            {
                "family_root": family,
                "rows": int(metric.rows),
                "sessions": int(metric.sessions),
                "candidate_mae": float(metric.torch_mae),
                "ridge_mae": float(metric.ridge_mae),
                "persistence_mae": float(metric.zero_return_mae),
                "incumbent_mae": float(metric.incumbent_mae),
                "incumbent_rows": int(metric.incumbent_rows),
            }
            for family, metric in sorted(family_metrics.items())
        ],
        "calibration_metrics": [
            {
                "family_root": family,
                "sessions": int(metric.sessions),
                "coverage_error": float(metric.coverage_error),
            }
            for family, metric in sorted(calibration_metrics.items())
        ],
        "deployment_calibration": {
            "method": DEPLOYMENT_CALIBRATION_METHOD,
            "model_version": version,
            "artifact_sha256": artifact_hash,
            "alpha": deployment_alpha,
            "target_coverage": deployment_target,
            "asof_utc": str(getattr(deployment_calibration, "asof_utc", "")),
            "eligible_rows": deployment_eligible_rows,
            "excluded_future_rows": deployment_excluded_future_rows,
            "evidence_sha256": calibration_evidence_hash,
            "source_sha256s": sorted(calibration_sources),
            "label_source_artifact_sha256s": sorted(calibration_label_artifacts),
            "family_radii": [
                {
                    "family_root": family,
                    "rows": int(item.rows),
                    "sessions": int(item.sessions),
                    "radius_log_return": float(item.radius_log_return),
                }
                for family, item in sorted(deployment_radii.items())
            ],
        },
        "regime_metrics": [
            {
                "family_root": family,
                "volatility_regime": regime,
                "sessions": int(candidate_regimes[(family, regime)].sessions),
                "candidate_mae": float(candidate_regimes[(family, regime)].mae),
                "ridge_mae": float(ridge_regimes[(family, regime)].mae),
            }
            for family, regime in sorted(required_regimes)
        ],
    }
