from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from backend.database_target import configured_market_database_path

from .contracts import EVIDENCE_CONTRACT_VERSION
from .calibration import DEPLOYMENT_CALIBRATION_METHOD
from .model_artifact import (
    MODEL_ARTIFACT_FORMAT,
    MODEL_MANIFEST_CONTRACT_VERSION,
    load_frozen_model_artifact_bytes,
)
from .close_reconciliation import load_verified_close_parent_overrides
from .close_evidence import (
    resolve_verified_close_artifact,
    resolve_verified_close_artifacts_by_hashes,
    validate_verified_close_observed_at,
)
from .paper_evidence import (
    resolve_paper_candidate,
    validate_retained_paper_promotion_evidence,
)
from .promotion import validate_promotion_approval
from .surface import MODEL_FEATURE_CONTRACT_HASH
from .surface_artifact import validate_retained_surface_promotion_evidence


UTC = timezone.utc
MARKET_TIMEZONE = ZoneInfo("America/New_York")
PRODUCTION_FAMILIES = {"SPX", "NDX", "RUT", "VIX", "SPY"}
VOLATILITY_REGIMES = {"calm", "normal", "stressed"}
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_MODEL_MANIFEST_BYTES = 1024 * 1024


def _number(value: object, default: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result == result and abs(result) != float("inf") else default


def _integer(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _normalized_deployment_calibration(
    value: object,
) -> dict[str, object] | None:
    """Return one strict, order-independent deployment calibration identity."""

    expected_fields = {
        "method",
        "alpha",
        "target_coverage",
        "asof_utc",
        "model_version",
        "artifact_sha256",
        "eligible_rows",
        "excluded_future_rows",
        "evidence_sha256",
        "source_sha256s",
        "label_source_artifact_sha256s",
        "family_radii",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        return None
    try:
        alpha = float(value["alpha"])
        target = float(value["target_coverage"])
        eligible_rows = int(value["eligible_rows"])
        excluded_future_rows = int(value["excluded_future_rows"])
        asof = datetime.fromisoformat(
            str(value["asof_utc"]).replace("Z", "+00:00")
        )
    except (TypeError, ValueError, OverflowError):
        return None
    if (
        isinstance(value["eligible_rows"], bool)
        or isinstance(value["excluded_future_rows"], bool)
        or not math.isfinite(alpha)
        or not math.isfinite(target)
        or not 0 < alpha < 1
        or not math.isclose(target, 1.0 - alpha, abs_tol=1e-12)
        or eligible_rows < len(PRODUCTION_FAMILIES)
        or excluded_future_rows < 0
        or asof.tzinfo is None
    ):
        return None

    artifact_hash = str(value["artifact_sha256"] or "").lower()
    evidence_hash = str(value["evidence_sha256"] or "").lower()
    raw_sources = value["source_sha256s"]
    raw_labels = value["label_source_artifact_sha256s"]
    if not isinstance(raw_sources, (list, tuple)) or not isinstance(
        raw_labels, (list, tuple)
    ):
        return None
    sources = tuple(str(item).lower() for item in raw_sources)
    labels = tuple(str(item).lower() for item in raw_labels)
    if (
        not SHA256_PATTERN.fullmatch(artifact_hash)
        or not SHA256_PATTERN.fullmatch(evidence_hash)
        or not sources
        or not labels
        or len(set(sources)) != len(sources)
        or len(set(labels)) != len(labels)
        or any(not SHA256_PATTERN.fullmatch(item) for item in (*sources, *labels))
    ):
        return None

    raw_radii = value["family_radii"]
    if not isinstance(raw_radii, (list, tuple)) or len(raw_radii) != len(
        PRODUCTION_FAMILIES
    ):
        return None
    radii: dict[str, tuple[int, int, float]] = {}
    for item in raw_radii:
        if not isinstance(item, dict) or set(item) != {
            "family_root",
            "rows",
            "sessions",
            "radius_log_return",
        }:
            return None
        try:
            rows = int(item["rows"])
            sessions = int(item["sessions"])
            radius = float(item["radius_log_return"])
        except (TypeError, ValueError, OverflowError):
            return None
        family = str(item["family_root"] or "").upper()
        if (
            isinstance(item["rows"], bool)
            or isinstance(item["sessions"], bool)
            or family in radii
            or rows < 1
            or sessions < 1
            or sessions > rows
            or not math.isfinite(radius)
            or radius < 0
        ):
            return None
        radii[family] = (rows, sessions, radius)
    if set(radii) != PRODUCTION_FAMILIES or sum(
        item[0] for item in radii.values()
    ) != eligible_rows:
        return None
    return {
        "method": str(value["method"] or ""),
        "alpha": alpha,
        "target_coverage": target,
        "asof_utc": asof.astimezone(UTC).isoformat(),
        "model_version": str(value["model_version"] or ""),
        "artifact_sha256": artifact_hash,
        "eligible_rows": eligible_rows,
        "excluded_future_rows": excluded_future_rows,
        "evidence_sha256": evidence_hash,
        "source_sha256s": tuple(sorted(sources)),
        "label_source_artifact_sha256s": tuple(sorted(labels)),
        "family_radii": tuple(
            (family, *radii[family]) for family in sorted(radii)
        ),
    }


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True, timeout=10.0)
    connection.row_factory = sqlite3.Row
    return connection


def _model_gate(
    project_root: Path,
    manifest_path: Path | None = None,
    *,
    manifest_bytes: bytes | None = None,
    market_db_path: str | Path | None = None,
) -> dict[str, object]:
    path = manifest_path or (project_root / "models" / "closing_tape_model.json")
    if manifest_bytes is None and not path.is_file():
        return {
            "passed": False,
            "enabled": False,
            "reason": "no frozen closing-tape model manifest",
        }
    try:
        snapshot = path.read_bytes() if manifest_bytes is None else manifest_bytes
        if not isinstance(snapshot, bytes):
            raise TypeError("manifest snapshot must be bytes")
        if len(snapshot) <= 0 or len(snapshot) > MAX_MODEL_MANIFEST_BYTES:
            raise ValueError(
                f"model manifest size {len(snapshot)} is outside the allowed range"
            )
        manifest = json.loads(snapshot.decode("utf-8"))
    except Exception as exc:
        return {"passed": False, "enabled": False, "reason": f"invalid model manifest: {type(exc).__name__}"}
    manifest_sha256 = hashlib.sha256(snapshot).hexdigest()
    if not isinstance(manifest, dict):
        return {
            "passed": False,
            "enabled": False,
            "reason": "invalid model manifest: root must be an object",
        }
    requirements = {"complete_sessions": 60, "purged_folds": 5, "paper_sessions": 20}
    reasons: list[str] = []
    approval = validate_promotion_approval(project_root, manifest)
    if not bool(approval.get("passed")):
        reasons.append(str(approval.get("reason") or "human promotion approval is invalid"))
    if manifest.get("enabled") is not True:
        reasons.append("manifest is not enabled")
    if manifest.get("evidence_contract_version") != EVIDENCE_CONTRACT_VERSION:
        reasons.append("unsupported or missing evidence contract version")
    if manifest.get("model_manifest_contract_version") != MODEL_MANIFEST_CONTRACT_VERSION:
        reasons.append("unsupported or missing model manifest contract version")
    for field, minimum in requirements.items():
        value = _integer(manifest.get(field))
        if value < minimum:
            reasons.append(f"{field} {value} < {minimum}")
    if _number(manifest.get("paper_candidate_mae"), float("inf")) >= _number(
        manifest.get("paper_incumbent_mae"), 0.0
    ):
        reasons.append("paper candidate MAE does not beat the incumbent")
    paper_family_rows = {
        str(row.get("family_root")): row
        for row in (manifest.get("paper_family_metrics") or [])
        if isinstance(row, dict)
    }
    for family in sorted(PRODUCTION_FAMILIES):
        row = paper_family_rows.get(family)
        if row is None:
            reasons.append(f"{family} paper evidence is missing")
            continue
        if _integer(row.get("sessions")) < 20:
            reasons.append(f"{family} has fewer than 20 paper sessions")
        if _integer(row.get("rows")) < _integer(row.get("sessions")):
            reasons.append(f"{family} paper rows are incomplete")
        if _number(row.get("candidate_mae"), float("inf")) >= _number(
            row.get("incumbent_mae"), 0.0
        ):
            reasons.append(f"{family} paper MAE does not beat incumbent")
    if _number(manifest.get("oos_improvement_pct"), 0.0) < 2.0:
        reasons.append("out-of-sample improvement is below 2 percent")
    if _number(manifest.get("oos_improvement_ci_low_pct"), 0.0) <= 0:
        reasons.append("out-of-sample improvement confidence interval does not exclude zero")
    held_out_rows = _integer(manifest.get("held_out_rows"))
    incumbent_rows = _integer(manifest.get("incumbent_rows"))
    held_out_sessions = _integer(manifest.get("held_out_sessions"))
    incumbent_sessions = _integer(manifest.get("incumbent_sessions"))
    if held_out_rows <= 0 or incumbent_rows != held_out_rows:
        reasons.append(
            f"incumbent timestamp-aligned coverage {incumbent_rows}/{held_out_rows} held-out rows"
        )
    if held_out_sessions <= 0 or incumbent_sessions != held_out_sessions:
        reasons.append(
            f"incumbent timestamp-aligned coverage {incumbent_sessions}/{held_out_sessions} held-out sessions"
        )
    if _number(manifest.get("torch_improvement_over_incumbent_pct"), 0.0) < 2.0:
        reasons.append("out-of-sample improvement over incumbent is below 2 percent")
    if _number(manifest.get("torch_improvement_over_incumbent_ci_low_pct"), 0.0) <= 0:
        reasons.append("improvement confidence interval versus incumbent does not exclude zero")

    feature_schema_hash = str(manifest.get("feature_schema_hash") or "").lower()
    if not SHA256_PATTERN.fullmatch(feature_schema_hash):
        reasons.append("feature schema hash is missing or invalid")
    elif feature_schema_hash != MODEL_FEATURE_CONTRACT_HASH:
        reasons.append("feature schema hash does not match the running feature contract")

    surface_artifact_hash = str(
        manifest.get("surface_artifact_sha256") or ""
    ).lower()
    surface_replay_hash = str(
        manifest.get("surface_replay_receipt_sha256") or ""
    ).lower()
    if not SHA256_PATTERN.fullmatch(
        surface_artifact_hash
    ) or not SHA256_PATTERN.fullmatch(surface_replay_hash):
        reasons.append("surface artifact or replay receipt SHA-256 is missing or invalid")

    execution_device = str(manifest.get("execution_device") or "cpu").lower()
    if execution_device not in {"cpu", "cuda"}:
        reasons.append("execution device must be cpu or cuda")
    if execution_device == "cuda":
        speedup = _number(manifest.get("cuda_speedup_vs_cpu"), 0.0)
        tolerance = _number(manifest.get("cuda_max_abs_prediction_difference"), float("inf"))
        allowed_tolerance = _number(manifest.get("cuda_prediction_tolerance"), -1.0)
        benchmark_rows = _integer(manifest.get("cuda_benchmark_rows"))
        inference_batch_rows = _integer(manifest.get("inference_batch_rows"))
        if speedup <= 1.0:
            reasons.append("CUDA inference has no measured speedup over CPU")
        if benchmark_rows < 1:
            reasons.append("CUDA inference benchmark has no rows")
        if inference_batch_rows < 1 or benchmark_rows != inference_batch_rows:
            reasons.append(
                f"CUDA benchmark rows {benchmark_rows} do not match deployed inference batch {inference_batch_rows}"
            )
        if allowed_tolerance < 0 or tolerance > allowed_tolerance:
            reasons.append("CUDA and CPU predictions exceed the declared numerical tolerance")
    raw_source_hashes = [
        str(value).lower() for value in (manifest.get("source_sha256s") or [])
    ]
    source_hashes = set(raw_source_hashes)
    source_identities_valid = not (
        len(source_hashes) < requirements["complete_sessions"]
        or len(source_hashes) != len(raw_source_hashes)
        or any(not SHA256_PATTERN.fullmatch(value) for value in raw_source_hashes)
    )
    if not source_identities_valid:
        reasons.append(
            f"verified source hashes {len(source_hashes)} < {requirements['complete_sessions']}"
        )
    raw_label_artifacts = [
        str(value).lower()
        for value in (manifest.get("label_source_artifact_sha256s") or [])
    ]
    label_artifacts = set(raw_label_artifacts)
    training_label_identities_valid = not (
        not raw_label_artifacts or any(
        not SHA256_PATTERN.fullmatch(value) for value in raw_label_artifacts
        ) or len(label_artifacts) != len(raw_label_artifacts)
    )
    if not training_label_identities_valid:
        reasons.append("training verified-close artifact hashes are missing or invalid")
    raw_paper_label_artifacts = [
        str(value).lower()
        for value in (manifest.get("paper_close_source_artifact_sha256s") or [])
    ]
    paper_label_artifacts = set(raw_paper_label_artifacts)
    paper_label_identities_valid = not (
        not raw_paper_label_artifacts or any(
        not SHA256_PATTERN.fullmatch(value) for value in raw_paper_label_artifacts
        ) or len(paper_label_artifacts) != len(raw_paper_label_artifacts)
    )
    if not paper_label_identities_valid:
        reasons.append("paper verified-close artifact hashes are missing or invalid")
    if not SHA256_PATTERN.fullmatch(
        str(manifest.get("paper_evidence_sha256") or "").lower()
    ):
        reasons.append("paper row-level evidence SHA-256 is missing or invalid")
    paper_sessions = _integer(manifest.get("paper_sessions"))
    paper_campaign_opportunities = _integer(
        manifest.get("paper_campaign_opportunities")
    )
    paper_prefix_replay_hashes = [
        str(value).lower()
        for value in (manifest.get("paper_prefix_replay_receipt_sha256s") or [])
    ]
    paper_final_tape_hashes = [
        str(value).lower()
        for value in (manifest.get("paper_final_tape_source_sha256s") or [])
    ]
    paper_campaign_coverage_hash = str(
        manifest.get("paper_campaign_coverage_receipt_sha256") or ""
    ).lower()
    paper_receipt_identities_valid = (
        paper_campaign_opportunities >= paper_sessions
        and paper_campaign_opportunities > 0
        and len(paper_prefix_replay_hashes) == paper_campaign_opportunities
        and len(set(paper_prefix_replay_hashes)) == paper_campaign_opportunities
        and len(paper_final_tape_hashes) == paper_campaign_opportunities
        and len(set(paper_final_tape_hashes)) == paper_campaign_opportunities
        and all(
            SHA256_PATTERN.fullmatch(value)
            for value in (*paper_prefix_replay_hashes, *paper_final_tape_hashes)
        )
        and SHA256_PATTERN.fullmatch(paper_campaign_coverage_hash) is not None
    )
    paper_prefix_replay_verified = False
    if not paper_receipt_identities_valid:
        reasons.append(
            "paper prefix replay or complete-campaign coverage proof is missing or invalid"
        )
    paper_activation_receipt_hashes = [
        str(value).lower()
        for value in (manifest.get("paper_activation_receipt_sha256s") or [])
    ]
    models_dir = (project_root / "models").resolve()
    training_close_artifacts_verified = False
    if training_label_identities_valid:
        try:
            resolve_verified_close_artifacts_by_hashes(
                project_root / "data" / "verified_close_sources",
                source_artifact_sha256s=tuple(sorted(label_artifacts)),
            )
        except (OSError, ValueError) as exc:
            reasons.append(f"training verified-close artifact is unavailable: {exc}")
        else:
            training_close_artifacts_verified = True
    artifact_value = manifest.get("artifact_path")
    artifact_hash = str(manifest.get("artifact_sha256") or "").lower()
    paper_activation_verified = False
    activation_hash = ""
    activated_candidate_calibration: dict[str, object] | None = None
    if len(paper_activation_receipt_hashes) != 1 or any(
        not SHA256_PATTERN.fullmatch(value)
        for value in paper_activation_receipt_hashes
    ):
        reasons.append(
            "paper candidate activation receipt SHA-256 is missing, duplicated, or invalid"
        )
    else:
        activation_hash = paper_activation_receipt_hashes[0]
        try:
            resolved_candidate = resolve_paper_candidate(
                project_root,
                activation_receipt_sha256=activation_hash,
                candidate_package_sha256=None,
                model_version=str(manifest.get("version") or ""),
                artifact_sha256=artifact_hash,
                activated_at_utc=None,
            )
        except (OSError, ValueError) as exc:
            reasons.append(
                f"paper candidate activation receipt is invalid: {exc}"
            )
        else:
            activation = resolved_candidate.activation_receipt
            activated_candidate_calibration = _normalized_deployment_calibration(
                resolved_candidate.candidate_package_receipt.get(
                    "deployment_calibration"
                )
            )
            paper_activation_verified = (
                str(activation.get("model_version") or "")
                == str(manifest.get("version") or "")
                and str(activation.get("artifact_sha256") or "").lower()
                == artifact_hash
                and resolved_candidate.artifact_sha256 == artifact_hash
                and activated_candidate_calibration is not None
            )
            if not paper_activation_verified:
                reasons.append(
                    "paper candidate activation chain does not bind the promoted model artifact"
                )

    artifact_ok = False
    artifact_loadable = False
    artifact_dbn_provenance_matches = False
    artifact_close_provenance_matches = False
    artifact_surface_provenance_matches = False
    surface_evidence_verified = False
    verified_source_files = 0
    if not artifact_value or not SHA256_PATTERN.fullmatch(artifact_hash):
        reasons.append("model artifact path or SHA-256 is missing or invalid")
    else:
        artifact_path = (models_dir / str(artifact_value)).resolve()
        try:
            artifact_path.relative_to(models_dir)
        except ValueError:
            reasons.append("model artifact path escapes the models directory")
        else:
            if not artifact_path.is_file():
                reasons.append("model artifact is missing")
            else:
                artifact_bytes = artifact_path.read_bytes()
                actual_hash = hashlib.sha256(artifact_bytes).hexdigest()
                artifact_ok = actual_hash == artifact_hash
                if not artifact_ok:
                    reasons.append("model artifact SHA-256 does not match the manifest")
                elif manifest.get("artifact_format") != MODEL_ARTIFACT_FORMAT:
                    reasons.append("manifest artifact format is missing or unsupported")
                else:
                    try:
                        loaded_artifact = load_frozen_model_artifact_bytes(
                            artifact_bytes
                        )
                    except (OSError, ValueError) as exc:
                        reasons.append(f"model artifact is not loadable: {exc}")
                    else:
                        artifact_loadable = True
                        artifact_dbn_provenance_matches = (
                            set(loaded_artifact.source_sha256s) == source_hashes
                        )
                        artifact_close_provenance_matches = (
                            set(loaded_artifact.label_source_artifact_sha256s)
                            == label_artifacts
                        )
                        artifact_surface_provenance_matches = (
                            loaded_artifact.surface_artifact_sha256
                            == surface_artifact_hash
                            and loaded_artifact.surface_replay_receipt_sha256
                            == surface_replay_hash
                        )
                        if not artifact_dbn_provenance_matches:
                            reasons.append(
                                "model artifact DBN provenance does not match manifest"
                            )
                        if not artifact_close_provenance_matches:
                            reasons.append(
                                "model artifact close provenance does not match manifest"
                            )
                        if not artifact_surface_provenance_matches:
                            reasons.append(
                                "model artifact surface replay provenance does not match manifest"
                            )

    if (
        source_identities_valid
        and SHA256_PATTERN.fullmatch(surface_artifact_hash)
        and SHA256_PATTERN.fullmatch(surface_replay_hash)
    ):
        try:
            surface_proof = validate_retained_surface_promotion_evidence(
                project_root,
                surface_artifact_sha256=surface_artifact_hash,
                surface_replay_receipt_sha256=surface_replay_hash,
                source_sha256s=tuple(sorted(source_hashes)),
                market_db_path=(
                    Path(market_db_path).resolve()
                    if market_db_path is not None
                    else configured_market_database_path(project_root)
                ),
            )
        except (OSError, ValueError) as exc:
            reasons.append(f"retained surface/source evidence is invalid: {exc}")
        else:
            verified_source_files = int(surface_proof["source_files"])
            surface_evidence_verified = (
                verified_source_files >= requirements["complete_sessions"]
            )
            if not surface_evidence_verified:
                reasons.append("retained surface/source evidence has too few source files")

    if (
        paper_receipt_identities_valid
        and paper_label_identities_valid
        and paper_activation_verified
    ):
        try:
            paper_proof = validate_retained_paper_promotion_evidence(
                project_root,
                model_version=str(manifest.get("version") or ""),
                artifact_sha256=artifact_hash,
                activation_receipt_sha256=activation_hash,
                row_evidence_sha256=str(
                    manifest.get("paper_evidence_sha256") or ""
                ).lower(),
                prefix_replay_receipt_sha256s=tuple(paper_prefix_replay_hashes),
                final_tape_source_sha256s=tuple(paper_final_tape_hashes),
                campaign_coverage_receipt_sha256=paper_campaign_coverage_hash,
                campaign_opportunities=paper_campaign_opportunities,
                close_source_artifact_sha256s=tuple(
                    sorted(paper_label_artifacts)
                ),
                paper_sessions=paper_sessions,
                candidate_mae=_number(
                    manifest.get("paper_candidate_mae"), float("nan")
                ),
                incumbent_mae=_number(
                    manifest.get("paper_incumbent_mae"), float("nan")
                ),
                family_metrics=paper_family_rows,
            )
        except (OSError, ValueError) as exc:
            reasons.append(f"retained paper evidence is invalid: {exc}")
        else:
            paper_prefix_replay_verified = (
                int(paper_proof["opportunities"]) == paper_campaign_opportunities
                and int(paper_proof["sessions"]) == paper_sessions
            )
            if not paper_prefix_replay_verified:
                reasons.append("retained paper evidence counters do not match manifest")

    family_rows = {
        str(row.get("family_root")): row
        for row in (manifest.get("family_metrics") or [])
        if isinstance(row, dict)
    }
    for family in sorted(PRODUCTION_FAMILIES):
        row = family_rows.get(family)
        if row is None:
            reasons.append(f"{family} held-out family evidence is missing")
            continue
        if _integer(row.get("sessions")) < 20:
            reasons.append(f"{family} has fewer than 20 held-out sessions")
        if _number(row.get("candidate_mae"), float("inf")) >= _number(
            row.get("ridge_mae"), 0.0
        ):
            reasons.append(f"{family} MAE does not beat ridge")
        if _number(row.get("candidate_mae"), float("inf")) >= _number(
            row.get("persistence_mae"), 0.0
        ):
            reasons.append(f"{family} MAE does not beat persistence")
        if _integer(row.get("incumbent_rows")) != _integer(row.get("rows")):
            reasons.append(f"{family} incumbent held-out coverage is incomplete")
        if _number(row.get("candidate_mae"), float("inf")) >= _number(
            row.get("incumbent_mae"), 0.0
        ):
            reasons.append(f"{family} MAE does not beat incumbent")

    calibration_rows = {
        str(row.get("family_root")): row
        for row in (manifest.get("calibration_metrics") or [])
        if isinstance(row, dict)
    }
    for family in sorted(PRODUCTION_FAMILIES):
        row = calibration_rows.get(family)
        if row is None:
            reasons.append(f"{family} calibration evidence is missing")
            continue
        if _integer(row.get("sessions")) < 20:
            reasons.append(f"{family} calibration has fewer than 20 sessions")
        if _number(row.get("coverage_error"), float("inf")) > 0.05:
            reasons.append(f"{family} interval coverage error exceeds 5 percent")
    deployment = manifest.get("deployment_calibration")
    deployment_rows: dict[str, dict[str, object]] = {}
    if not isinstance(deployment, dict):
        reasons.append("deployment conformal calibration is missing")
    else:
        normalized_deployment = _normalized_deployment_calibration(deployment)
        if normalized_deployment is None:
            reasons.append("deployment calibration contract is invalid")
        elif (
            activated_candidate_calibration is not None
            and normalized_deployment != activated_candidate_calibration
        ):
            reasons.append(
                "deployment calibration does not match the activated candidate package"
            )
        if str(deployment.get("model_version") or "") != str(manifest.get("version") or ""):
            reasons.append("deployment calibration model version does not match manifest")
        if str(deployment.get("artifact_sha256") or "").lower() != artifact_hash:
            reasons.append("deployment calibration artifact hash does not match manifest")
        if deployment.get("method") != DEPLOYMENT_CALIBRATION_METHOD:
            reasons.append("deployment conformal calibration method is unsupported")
        if not SHA256_PATTERN.fullmatch(
            str(deployment.get("evidence_sha256") or "").lower()
        ):
            reasons.append("deployment calibration OOS evidence hash is missing or invalid")
        alpha = _number(deployment.get("alpha"), -1.0)
        target = _number(deployment.get("target_coverage"), -1.0)
        if not 0 < alpha < 1 or abs(target - (1.0 - alpha)) > 1e-12:
            reasons.append("deployment conformal coverage contract is invalid")
        deployment_sources = {
            str(value).lower() for value in (deployment.get("source_sha256s") or [])
            if SHA256_PATTERN.fullmatch(str(value).lower())
        }
        if len(deployment_sources) < 20 or not deployment_sources <= source_hashes:
            reasons.append("deployment calibration sources are incomplete or unverified")
        raw_deployment_label_artifacts = [
            str(value).lower()
            for value in (deployment.get("label_source_artifact_sha256s") or [])
        ]
        deployment_label_artifacts = set(raw_deployment_label_artifacts)
        if (
            not raw_deployment_label_artifacts
            or any(
                not SHA256_PATTERN.fullmatch(value)
                for value in raw_deployment_label_artifacts
            )
            or not deployment_label_artifacts <= label_artifacts
        ):
            reasons.append(
                "deployment calibration close artifacts are incomplete or unverified"
            )
        deployment_rows = {
            str(row.get("family_root")): row
            for row in (deployment.get("family_radii") or []) if isinstance(row, dict)
        }
        for family in sorted(PRODUCTION_FAMILIES):
            row = deployment_rows.get(family)
            if row is None or _integer(row.get("sessions")) < 20:
                reasons.append(f"{family} deployment calibration evidence is missing")
            elif _number(row.get("radius_log_return"), -1.0) < 0:
                reasons.append(f"{family} deployment calibration radius is invalid")

    regime_keys = {
        (str(row.get("family_root")), str(row.get("volatility_regime")))
        for row in (manifest.get("regime_metrics") or [])
        if isinstance(row, dict)
        and _integer(row.get("sessions")) >= 5
        and _number(row.get("candidate_mae"), float("inf"))
        < _number(row.get("ridge_mae"), 0.0)
    }
    missing_regimes = sorted(
        (family, regime)
        for family in PRODUCTION_FAMILIES
        for regime in VOLATILITY_REGIMES
        if (family, regime) not in regime_keys
    )
    if missing_regimes:
        reasons.append(
            "missing passing family/regime evidence: "
            + ", ".join(f"{family}/{regime}" for family, regime in missing_regimes)
        )
    return {
        "passed": not reasons,
        "enabled": manifest.get("enabled") is True,
        "reason": "; ".join(reasons) or None,
        "manifest_sha256": manifest_sha256,
        "version": manifest.get("version"),
        "complete_sessions": _integer(manifest.get("complete_sessions")),
        "purged_folds": _integer(manifest.get("purged_folds")),
        "paper_sessions": _integer(manifest.get("paper_sessions")),
        "paper_campaign_opportunities": paper_campaign_opportunities,
        "paper_prefix_replay_verified": paper_prefix_replay_verified,
        "paper_prefix_replay_receipt_sha256s": paper_prefix_replay_hashes,
        "paper_final_tape_source_sha256s": paper_final_tape_hashes,
        "paper_campaign_coverage_receipt_sha256": (
            paper_campaign_coverage_hash
            if SHA256_PATTERN.fullmatch(paper_campaign_coverage_hash)
            else None
        ),
        "paper_families_verified": sorted(PRODUCTION_FAMILIES & set(paper_family_rows)),
        "oos_improvement_pct": _number(manifest.get("oos_improvement_pct"), 0.0),
        "oos_improvement_ci_low_pct": _number(
            manifest.get("oos_improvement_ci_low_pct"), 0.0
        ),
        "torch_improvement_over_incumbent_pct": _number(
            manifest.get("torch_improvement_over_incumbent_pct"), 0.0
        ),
        "torch_improvement_over_incumbent_ci_low_pct": _number(
            manifest.get("torch_improvement_over_incumbent_ci_low_pct"), 0.0
        ),
        "incumbent_rows": incumbent_rows,
        "held_out_rows": held_out_rows,
        "incumbent_sessions": incumbent_sessions,
        "held_out_sessions": held_out_sessions,
        "evidence_contract_version": manifest.get("evidence_contract_version"),
        "model_manifest_contract_version": manifest.get("model_manifest_contract_version"),
        "feature_contract_matches": feature_schema_hash == MODEL_FEATURE_CONTRACT_HASH,
        "execution_device": execution_device,
        "inference_batch_rows": _integer(manifest.get("inference_batch_rows")),
        "artifact_verified": (
            artifact_ok
            and artifact_loadable
            and artifact_dbn_provenance_matches
            and artifact_close_provenance_matches
            and artifact_surface_provenance_matches
            and training_close_artifacts_verified
            and surface_evidence_verified
        ),
        "artifact_loadable": artifact_loadable,
        "artifact_dbn_provenance_matches": artifact_dbn_provenance_matches,
        "artifact_close_provenance_matches": artifact_close_provenance_matches,
        "artifact_surface_provenance_matches": artifact_surface_provenance_matches,
        "training_close_artifacts_verified": training_close_artifacts_verified,
        "surface_evidence_verified": surface_evidence_verified,
        "surface_artifact_sha256": surface_artifact_hash or None,
        "surface_replay_receipt_sha256": surface_replay_hash or None,
        "paper_activation_verified": paper_activation_verified,
        "verified_source_files": verified_source_files,
        "families_verified": sorted(PRODUCTION_FAMILIES & set(family_rows)),
        "calibrated_families": sorted(PRODUCTION_FAMILIES & set(calibration_rows)),
        "deployment_calibrated_families": sorted(PRODUCTION_FAMILIES & set(deployment_rows)),
        "regime_slices_verified": len(regime_keys),
        "promotion_approval_verified": bool(approval.get("passed")),
        "promotion_approval": {
            "proposal_sha256": approval.get("proposal_sha256"),
            "receipt_sha256": approval.get("receipt_sha256"),
            "approved_by": approval.get("approved_by"),
            "approved_at_utc": approval.get("approved_at_utc"),
        } if approval.get("passed") else None,
    }


def _verified_close_evidence(
    market_path: Path,
    *,
    project_root: Path,
) -> dict[str, object]:
    if not market_path.is_file():
        return {"available": False, "rows": 0, "families": [], "reason": "market database is missing"}
    with _connect(market_path) as connection:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='eod_close_observations'"
        ).fetchone()
        if not table:
            return {
                "available": False,
                "rows": 0,
                "families": [],
                "reason": "immutable close-observation ledger is not initialized",
            }
        columns = {
            str(row[1]) for row in connection.execute(
                "PRAGMA table_info(eod_close_observations)"
            )
        }
        required_close_columns = {
            "id", "symbol", "trading_date", "source_artifact_sha256",
            "source_verified", "observed_at_utc", "correction_of_id",
        }
        if not required_close_columns <= columns:
            return {
                "available": False, "rows": 0, "families": [],
                "reason": "verified close ledger lacks immutable provenance or lineage fields",
            }
        evidence_rows = connection.execute(
            """
            SELECT id, symbol, trading_date, source_artifact_sha256,
                   observed_at_utc, correction_of_id
            FROM eod_close_observations
            WHERE source_verified=1 AND symbol IN ('SPX','NDX','RUT','VIX','SPY')
            ORDER BY symbol, trading_date, id
            """
        ).fetchall()
    if not evidence_rows:
        return {
            "available": False,
            "rows": 0,
            "sessions": 0,
            "families": [],
            "by_family": [],
            "reason": "no source-verified production closes are recorded",
        }
    authoritative_tips: list[sqlite3.Row] = []
    try:
        with _connect(market_path) as connection:
            parent_overrides = load_verified_close_parent_overrides(connection)
        grouped: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in evidence_rows:
            grouped.setdefault(
                (str(row["symbol"]).upper(), str(row["trading_date"])), []
            ).append(row)
        for (symbol, trading_date), rows in grouped.items():
            by_id = {int(row["id"]): row for row in rows}
            roots: list[sqlite3.Row] = []
            children: dict[int, sqlite3.Row] = {}
            for row in rows:
                raw_observed = str(row["observed_at_utc"] or "")
                observed = datetime.fromisoformat(raw_observed.replace("Z", "+00:00"))
                if observed.tzinfo is None:
                    observed = observed.replace(tzinfo=UTC)
                validate_verified_close_observed_at(
                    trading_date,
                    observed,
                )
                parent_id = (
                    int(parent_overrides.get(int(row["id"]), row["correction_of_id"]))
                    if int(row["id"]) in parent_overrides or row["correction_of_id"] is not None
                    else None
                )
                if parent_id is None:
                    roots.append(row)
                else:
                    if (
                        parent_id not in by_id
                        or parent_id >= int(row["id"])
                        or parent_id in children
                    ):
                        raise ValueError(
                            "verified close correction lineage is missing, non-causal, or forked"
                        )
                    children[parent_id] = row
                resolve_verified_close_artifact(
                    project_root / "data" / "verified_close_sources",
                    trading_date=trading_date,
                    symbol=symbol,
                    source_artifact_sha256=str(row["source_artifact_sha256"]),
                )
            if len(roots) != 1:
                raise ValueError(
                    "verified close observations require exactly one causal root"
                )
            tip = roots[0]
            visited = {int(tip["id"])}
            while int(tip["id"]) in children:
                tip = children[int(tip["id"])]
                if int(tip["id"]) in visited:
                    raise ValueError("verified close correction lineage cycles")
                visited.add(int(tip["id"]))
            if visited != set(by_id):
                raise ValueError("verified close correction lineage is disconnected")
            authoritative_tips.append(tip)
    except (OSError, TypeError, ValueError) as exc:
        return {
            "available": False,
            "rows": 0,
            "sessions": 0,
            "families": [],
            "by_family": [],
            "reason": f"verified close evidence is unavailable: {exc}",
        }
    by_family: list[dict[str, object]] = []
    for symbol in sorted({str(row["symbol"]) for row in authoritative_tips}):
        selected = [
            row for row in authoritative_tips if str(row["symbol"]) == symbol
        ]
        by_family.append(
            {
                "symbol": symbol,
                "observations": len(selected),
                "sessions": len({str(row["trading_date"]) for row in selected}),
            }
        )
    return {
        "available": True,
        "rows": len(authoritative_tips),
        "ledger_rows": len(evidence_rows),
        "lineage_reconciliations": len(parent_overrides),
        "sessions": sum(int(row["sessions"]) for row in by_family),
        "families": [str(row["symbol"]) for row in by_family],
        "by_family": by_family,
        "reason": None,
    }


def closing_tape_status(
    project_root: str | Path,
    *,
    trading_day: date | None = None,
    market_db_path: str | Path | None = None,
) -> dict[str, object]:
    """Return an auditable summary without treating missing evidence as zero."""
    root = Path(project_root).resolve()
    market_path = (
        Path(market_db_path).resolve()
        if market_db_path is not None
        else (root / "data" / "market_data.db").resolve()
    )
    day = trading_day or datetime.now(MARKET_TIMEZONE).date()
    catalog_path = root / "data" / "closing_tape" / day.isoformat() / "closing_tape.sqlite"
    model_gate = _model_gate(root, market_db_path=market_path)
    close_evidence = _verified_close_evidence(market_path, project_root=root)
    base: dict[str, object] = {
        "asof_utc": datetime.now(UTC).isoformat(),
        "trading_date": day.isoformat(),
        "catalog_available": catalog_path.is_file(),
        "observed": {
            "semantics": "provider records and reproducible arithmetic aggregates",
            "available": False,
        },
        "inferred": {
            "semantics": "estimated trade location or positioning; not aggressor side or holdings",
            "available": False,
        },
        "model_gate": model_gate,
        "close_evidence": close_evidence,
    }
    if not catalog_path.is_file():
        base.update(
            state="unavailable",
            usable_for_research=False,
            reasons=["closing-tape catalog is missing for the requested trading date"],
        )
        return base

    with _connect(catalog_path) as connection:
        tables = {
            str(row[0]) for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        session = connection.execute(
            "SELECT * FROM tape_sessions WHERE trading_date=? ORDER BY created_at_utc DESC LIMIT 1",
            (day.isoformat(),),
        ).fetchone()
        if session is None:
            base.update(
                state="unavailable",
                usable_for_research=False,
                reasons=["closing-tape catalog contains no session for the requested trading date"],
            )
            return base
        session_id = str(session["session_id"])
        feeds = connection.execute(
            "SELECT * FROM tape_feed_status WHERE session_id=? ORDER BY feed_name",
            (session_id,),
        ).fetchall()
        observed = connection.execute(
            """
            SELECT COUNT(*) minute_rows, COUNT(DISTINCT family_root) families,
                   COALESCE(SUM(trade_count), 0) derived_trade_count,
                   MIN(minute_utc) first_minute_utc, MAX(minute_utc) last_minute_utc
            FROM tape_observed_minute WHERE session_id=?
            """,
            (session_id,),
        ).fetchone()
        inferred = connection.execute(
            """
            SELECT COUNT(*) minute_rows, COUNT(DISTINCT family_root) families,
                   COUNT(DISTINCT inference_method || ':' || inference_version) method_versions,
                   GROUP_CONCAT(DISTINCT inference_method || ':' || inference_version) methods
            FROM tape_inferred_minute_flow WHERE session_id=?
            """,
            (session_id,),
        ).fetchone()
        observed_by_family = connection.execute(
            """
            SELECT family_root,
                   COUNT(*) minute_rows,
                   COALESCE(SUM(trade_count), 0) trade_records,
                   COALESCE(SUM(volume), 0) volume,
                   COALESCE(SUM(notional), 0) premium_notional,
                   COALESCE(SUM(nbbo_valid_count), 0) valid_pretrade_nbbo_records,
                   COALESCE(SUM(data_quality_flagged_count), 0) data_quality_flagged_records,
                   MIN(minute_utc) first_minute_utc,
                   MAX(minute_utc) last_minute_utc
            FROM tape_observed_minute
            WHERE session_id=? AND feed_name='opra_options'
            GROUP BY family_root ORDER BY family_root
            """,
            (session_id,),
        ).fetchall()
        inferred_by_family = connection.execute(
            """
            SELECT family_root,
                   COUNT(*) minute_rows,
                   COALESCE(SUM(at_ask_count), 0) at_ask_count,
                   COALESCE(SUM(at_bid_count), 0) at_bid_count,
                   COALESCE(SUM(inside_count), 0) inside_count,
                   COALESCE(SUM(unknown_count), 0) unknown_count,
                   COALESCE(SUM(at_ask_count + at_bid_count + inside_count + unknown_count), 0)
                       classified_records,
                   COALESCE(SUM(at_ask_volume), 0) at_ask_volume,
                   COALESCE(SUM(at_bid_volume), 0) at_bid_volume,
                   COALESCE(SUM(inside_volume), 0) inside_volume,
                   COALESCE(SUM(unknown_volume), 0) unknown_volume,
                   COALESCE(SUM(at_ask_notional), 0) at_ask_notional,
                   COALESCE(SUM(at_bid_notional), 0) at_bid_notional,
                   MIN(minute_utc) first_minute_utc,
                   MAX(minute_utc) last_minute_utc
            FROM tape_inferred_minute_flow
            WHERE session_id=? AND feed_name='opra_options'
            GROUP BY family_root ORDER BY family_root
            """,
            (session_id,),
        ).fetchall()
        observed_family_payload = [dict(row) for row in observed_by_family]
        inferred_family_payload = [dict(row) for row in inferred_by_family]
        observed_family_map = {
            str(row["family_root"]): row for row in observed_family_payload
        }
        inferred_family_map = {
            str(row["family_root"]): row for row in inferred_family_payload
        }
        family_integrity_reasons: list[str] = []
        inference_mismatches: list[str] = []
        nbbo_mismatches: list[str] = []
        for family in sorted(set(observed_family_map) | set(inferred_family_map)):
            observed_row = observed_family_map.get(family, {})
            inferred_row = inferred_family_map.get(family, {})
            trades = int(observed_row.get("trade_records") or 0)
            classified = int(inferred_row.get("classified_records") or 0)
            valid_nbbo = int(observed_row.get("valid_pretrade_nbbo_records") or 0)
            if trades != classified:
                inference_mismatches.append(family)
                family_integrity_reasons.append(
                    f"{family} inferred location coverage {classified}/{trades} observed trades"
                )
            if valid_nbbo < 0 or valid_nbbo > trades:
                nbbo_mismatches.append(family)
                family_integrity_reasons.append(
                    f"{family} valid pre-trade NBBO count {valid_nbbo} exceeds observed trades {trades}"
                )
        contract_observed_rows = (
            connection.execute(
                "SELECT COUNT(*) FROM tape_observed_contract_minute WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if "tape_observed_contract_minute" in tables else 0
        )
        contract_inferred_rows = (
            connection.execute(
                "SELECT COUNT(*) FROM tape_inferred_contract_minute_flow WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if "tape_inferred_contract_minute_flow" in tables else 0
        )
        inferred_references = (
            connection.execute(
                """
                SELECT COUNT(*) rows, COUNT(DISTINCT family_root) families,
                       GROUP_CONCAT(DISTINCT reference_method || ':' || reference_version) methods,
                       MAX(available_at_utc) latest_available_at_utc
                FROM tape_inferred_reference_minute WHERE session_id=?
                """,
                (session_id,),
            ).fetchone()
            if "tape_inferred_reference_minute" in tables else None
        )
        open_interest_rows = connection.execute(
            "SELECT COUNT(*) FROM tape_open_interest WHERE session_id=?",
            (session_id,),
        ).fetchone()[0]
        oi_observation_table = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type='table' AND name='tape_open_interest_observations'
            """
        ).fetchone()
        open_interest_observations = (
            connection.execute(
                "SELECT COUNT(*) FROM tape_open_interest_observations WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if oi_observation_table else 0
        )
        open_interest_families = connection.execute(
            """
            SELECT COUNT(DISTINCT family_root) FROM tape_open_interest
            WHERE session_id=? AND family_root IS NOT NULL
            """,
            (session_id,),
        ).fetchone()[0]
        definition_observations = (
            connection.execute(
                "SELECT COUNT(*) FROM tape_instrument_definition_observations WHERE session_id=?",
                (session_id,),
            ).fetchone()[0]
            if "tape_instrument_definition_observations" in tables else 0
        )
        definition_actions = (
            {
                str(row[0]): int(row[1])
                for row in connection.execute(
                    """
                    SELECT security_update_action, COUNT(*)
                    FROM tape_instrument_definition_observations
                    WHERE session_id=? GROUP BY security_update_action
                    """,
                    (session_id,),
                )
            }
            if "tape_instrument_definition_observations" in tables else {}
        )
        analysis = connection.execute(
            """
            SELECT decision_state, asof_utc, feature_hash, report_json
            FROM closing_analysis_runs WHERE session_id=?
            ORDER BY created_at_utc DESC LIMIT 1
            """,
            (session_id,),
        ).fetchone()

    feed_payload = [dict(row) for row in feeds]
    for feed in feed_payload:
        applicable = bool(int(feed.get("operational_counters_applicable", 1) or 0))
        feed["operational_counter_semantics"] = (
            "observed_live_session_counters"
            if applicable
            else "not_applicable_to_historical_bundle"
        )
        if not applicable:
            for name in (
                "reconnect_count",
                "slow_reader_warnings",
                "provider_error_count",
                "subscription_acks",
                "expected_subscription_acks",
                "replay_completed",
                "callback_queue_depth",
            ):
                if name in feed:
                    feed[name] = None
    required = next((feed for feed in feed_payload if feed["feed_name"] == "opra_options"), None)
    reasons: list[str] = []
    if required is None:
        reasons.append("required OPRA options feed is missing")
    else:
        required_keys = set(required.keys())

        def feed_int(name: str) -> int:
            return int(required[name] or 0) if name in required_keys else 0

        if required["status"] not in {"running", "complete"}:
            reasons.append(f"required OPRA feed status is {required['status']}")
        if int(required["trade_records"] or 0) <= 0:
            reasons.append("required OPRA feed has no trade records")
        if required["status"] == "complete":
            tcbbo_records = feed_int("tcbbo_records")
            if tcbbo_records <= 0:
                reasons.append("completed OPRA feed has no verified TCBBO records")
            elif feed_int("tcbbo_timestamped_records") != tcbbo_records:
                reasons.append("completed OPRA feed has TCBBO records without both timestamps")
            elif feed_int("tcbbo_valid_nbbo_records") / tcbbo_records < 0.95:
                reasons.append("completed OPRA feed has insufficient valid pre-trade NBBO coverage")
        if int(required["reconnect_count"] or 0) > 0:
            reasons.append("required OPRA feed recorded a reconnect gap")
        if int(required["slow_reader_warnings"] or 0) > 0:
            reasons.append("required OPRA feed reported slow-reader or derived-queue loss")
        if required["status"] == "complete" and not int(required["complete"] or 0):
            reasons.append("required OPRA feed failed final integrity verification")
    observed_payload = {
        "semantics": base["observed"]["semantics"],
        "available": bool(observed["minute_rows"]),
        "raw_feed": required,
        "minute_rows": int(observed["minute_rows"] or 0),
        "families": int(observed["families"] or 0),
        "derived_trade_count": int(observed["derived_trade_count"] or 0),
        "first_minute_utc": observed["first_minute_utc"],
        "last_minute_utc": observed["last_minute_utc"],
        "contract_minute_rows": int(contract_observed_rows or 0),
        "open_interest_rows": int(open_interest_rows or 0),
        "open_interest_observations": int(open_interest_observations or 0),
        "open_interest_families": int(open_interest_families or 0),
        "instrument_definition_observations": int(definition_observations or 0),
        "instrument_definition_actions": definition_actions,
        "by_family": observed_family_payload,
        "family_integrity": {
            "passed": not nbbo_mismatches,
            "invalid_nbbo_families": nbbo_mismatches,
        },
        "tcbbo_evidence": {
            "finalized": bool(required and required["status"] == "complete"),
            "records": feed_int("tcbbo_records") if required else 0,
            "timestamped_records": (
                feed_int("tcbbo_timestamped_records") if required else 0
            ),
            "valid_pretrade_nbbo_records": (
                feed_int("tcbbo_valid_nbbo_records") if required else 0
            ),
            "flagged_records": feed_int("tcbbo_flagged_records") if required else 0,
            "action_counts": (
                json.loads(str(required["tcbbo_action_counts_json"] or "{}"))
                if required and "tcbbo_action_counts_json" in required_keys else {}
            ),
        },
    }
    inferred_payload = {
        "semantics": base["inferred"]["semantics"],
        "available": bool(inferred["minute_rows"]),
        "minute_rows": int(inferred["minute_rows"] or 0),
        "families": int(inferred["families"] or 0),
        "method_versions": int(inferred["method_versions"] or 0),
        "methods": str(inferred["methods"] or "").split(",") if inferred["methods"] else [],
        "contract_minute_rows": int(contract_inferred_rows or 0),
        "by_family": inferred_family_payload,
        "classification_alignment": {
            "passed": bool(observed_family_payload) and not inference_mismatches,
            "mismatched_families": inference_mismatches,
            "semantics": "each observed trade maps to one inferred price-location bucket",
        },
        "reference_estimates": {
            "available": bool(inferred_references and inferred_references["rows"]),
            "rows": int(inferred_references["rows"] or 0) if inferred_references else 0,
            "families": int(inferred_references["families"] or 0) if inferred_references else 0,
            "methods": (
                str(inferred_references["methods"] or "").split(",")
                if inferred_references and inferred_references["methods"] else []
            ),
            "latest_available_at_utc": (
                inferred_references["latest_available_at_utc"] if inferred_references else None
            ),
            "semantics": "calculated reference estimates; not observed underlying prints",
        },
    }
    if not observed_payload["available"]:
        reasons.append("observed minute aggregates are unavailable")
    reasons.extend(family_integrity_reasons)
    if required is not None and required["status"] == "complete":
        expected_definitions = int(required["definition_records"] or 0)
        if expected_definitions <= 0:
            reasons.append("completed OPRA feed has no instrument-definition evidence")
        elif int(definition_observations or 0) != expected_definitions:
            reasons.append(
                "immutable instrument-definition observations are incomplete "
                f"({int(definition_observations or 0)}/{expected_definitions})"
            )
    if not inferred_payload["available"]:
        reasons.append("inferred minute estimates are unavailable")
    usable = not reasons and required is not None and required["status"] == "complete"
    if required is not None and required["status"] == "running" and not reasons:
        state = "running"
    elif usable:
        state = "complete"
    else:
        state = "degraded"
    analysis_payload = None
    if analysis is not None:
        report = json.loads(analysis["report_json"])
        analysis_payload = {
            "decision_state": analysis["decision_state"],
            "asof_utc": analysis["asof_utc"],
            "feature_hash": analysis["feature_hash"],
            "use_gate": report.get("use_gate"),
            "abstention_reasons": report.get("abstention_reasons") or [],
        }
    base.update(
        state=state,
        usable_for_research=usable,
        reasons=list(dict.fromkeys(reasons)),
        session={
            "session_id": session_id,
            "status": session["status"],
            "created_at_utc": session["created_at_utc"],
            "completed_at_utc": session["completed_at_utc"],
            "error": session["error"],
        },
        feeds=feed_payload,
        observed=observed_payload,
        inferred=inferred_payload,
        analysis=analysis_payload,
    )
    return base
