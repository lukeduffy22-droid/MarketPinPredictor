"""Fail-closed inspection for retired ``gamma_model_*.pt`` bundles.

The old CSV trainer wrote a bare PyTorch state dict and a loosely coupled JSON
sidecar.  Those files may still be useful as historical research evidence, but
they do not carry the immutable source, label, replay, feature-contract, or
walk-forward evidence required by the governed closing-tape artifact path.

This module deliberately does *not* convert or load a legacy bundle for live
inference.  It reads each source once, uses PyTorch's weights-only unpickler,
and reports whether the historical tensor/sidecar structure is internally
coherent.  A coherent legacy bundle remains ``RESEARCH_ONLY`` and conversion
remains refused.
"""

from __future__ import annotations

import hashlib
import io
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


AUDIT_CONTRACT_VERSION = "marketpin-legacy-gamma-artifact-audit.v1"
MAX_CHECKPOINT_BYTES = 128 * 1024 * 1024
MAX_SIDECAR_BYTES = 1024 * 1024
LEGACY_LINEAR_KEYS = (
    "net.0.weight",
    "net.0.bias",
    "net.2.weight",
    "net.2.bias",
    "net.4.weight",
    "net.4.bias",
)


@dataclass(frozen=True)
class LegacyGammaArtifactAudit:
    contract_version: str
    symbol: str
    checkpoint_path: str
    checkpoint_present: bool
    checkpoint_sha256: str | None
    checkpoint_bytes: int | None
    checkpoint_format: str
    tensor_contract_valid: bool
    tensor_shapes: dict[str, list[int]]
    metadata_path: str
    metadata_present: bool
    metadata_sha256: str | None
    metadata_contract_valid: bool
    metadata_features: tuple[str, ...]
    metadata_target: str | None
    metadata_rows_total: int | None
    training_report_path: str
    training_report_present: bool
    training_report_sha256: str | None
    training_report_exit_code: int | None
    training_report_authorizes_symbol: bool
    loadable_by_current_live_inference: bool
    migration_permitted: bool
    authority: str
    promotion_eligible: bool
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class LegacyModelDirectoryAudit:
    contract_version: str
    authority: str
    migration_permitted: bool
    promotion_eligible: bool
    readiness_source: str
    readiness_current: bool
    readiness_snapshot_path: str
    readiness_snapshot_present: bool
    readiness_snapshot_sha256: str | None
    readiness_generated_at_utc: str | None
    model_evidence_sessions: int | None
    model_sessions_required: int | None
    paper_forecast_sessions: int | None
    paper_sessions_required: int | None
    model_training_ready: bool
    paper_evidence_ready: bool
    symbols: dict[str, dict[str, object]]
    reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _read_bounded(path: Path, *, limit: int, kind: str) -> bytes:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ValueError(f"{kind} cannot be stat'ed: {exc}") from exc
    if size <= 0 or size > limit:
        raise ValueError(f"{kind} size {size} is outside the allowed range")
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ValueError(f"{kind} cannot be read: {exc}") from exc
    if len(payload) != size:
        raise ValueError(f"{kind} changed while it was being read")
    return payload


def _load_json_snapshot(path: Path) -> tuple[dict[str, Any], str]:
    raw = _read_bounded(path, limit=MAX_SIDECAR_BYTES, kind="JSON sidecar")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("JSON sidecar is not valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON sidecar root must be an object")
    return payload, digest


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _finite_vector(value: object, *, length: int, positive: bool = False) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return False
    if len(value) != length:
        return False
    try:
        values = [float(item) for item in value]
    except (TypeError, ValueError, OverflowError):
        return False
    return all(math.isfinite(item) and (not positive or item > 0) for item in values)


def _load_weights_only_snapshot(path: Path) -> tuple[object, str, int]:
    raw = _read_bounded(path, limit=MAX_CHECKPOINT_BYTES, kind="PyTorch checkpoint")
    digest = hashlib.sha256(raw).hexdigest()
    try:
        import torch

        payload = torch.load(io.BytesIO(raw), map_location="cpu", weights_only=True)
    except Exception as exc:  # noqa: BLE001 - surface bounded loader rejection as evidence
        raise ValueError(f"weights-only checkpoint load failed: {exc}") from exc
    return payload, digest, len(raw)


def _audit_legacy_state_dict(payload: object) -> tuple[str, bool, dict[str, list[int]], list[str]]:
    reasons: list[str] = []
    if not isinstance(payload, Mapping):
        return "unsupported", False, {}, ["checkpoint root is not a mapping"]

    raw_keys = tuple(str(key) for key in payload.keys())
    if "model_state_dict" in payload:
        return (
            "wrapped-checkpoint",
            False,
            {},
            [
                "checkpoint is not a retired raw state dict; validate it with its own declared contract"
            ],
        )
    if set(raw_keys) != set(LEGACY_LINEAR_KEYS):
        return (
            "unknown-state-dict",
            False,
            {},
            ["checkpoint keys do not match the retired three-layer MLP contract"],
        )

    try:
        import torch

        tensor_shapes: dict[str, list[int]] = {}
        for key in LEGACY_LINEAR_KEYS:
            tensor = payload[key]
            if not isinstance(tensor, torch.Tensor):
                reasons.append(f"checkpoint value {key} is not a tensor")
                continue
            tensor_shapes[key] = list(tensor.shape)
            if tensor.layout != torch.strided:
                reasons.append(f"checkpoint tensor {key} is not strided")
            if not tensor.is_floating_point():
                reasons.append(f"checkpoint tensor {key} is not floating point")
            elif not bool(torch.isfinite(tensor).all().item()):
                reasons.append(f"checkpoint tensor {key} contains non-finite values")
    except Exception as exc:  # noqa: BLE001 - report structural failures without loading live
        return "legacy-raw-state-dict", False, {}, [f"tensor inspection failed: {exc}"]

    if reasons:
        return "legacy-raw-state-dict", False, tensor_shapes, reasons

    weights = [payload[f"net.{index}.weight"] for index in (0, 2, 4)]
    biases = [payload[f"net.{index}.bias"] for index in (0, 2, 4)]
    expected_shapes = (
        weights[0].ndim == 2,
        weights[1].ndim == 2,
        weights[2].ndim == 2,
        biases[0].shape == (weights[0].shape[0],),
        biases[1].shape == (weights[1].shape[0],),
        biases[2].shape == (weights[2].shape[0],),
        weights[1].shape[1] == weights[0].shape[0],
        weights[2].shape[1] == weights[1].shape[0],
        weights[2].shape[0] == 1,
    )
    if not all(expected_shapes):
        reasons.append("linear tensor dimensions do not form a three-layer scalar-output MLP")
    return "legacy-raw-state-dict", not reasons, tensor_shapes, reasons


def audit_legacy_gamma_artifact(
    checkpoint_path: str | Path,
    *,
    symbol: str,
    metadata_path: str | Path | None = None,
    training_report_path: str | Path | None = None,
) -> LegacyGammaArtifactAudit:
    """Inspect one retired artifact bundle without creating a migrated model."""

    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    checkpoint = Path(checkpoint_path).resolve()
    metadata = Path(
        metadata_path
        if metadata_path is not None
        else checkpoint.with_name(f"{checkpoint.stem}_meta.json")
    ).resolve()
    report = Path(
        training_report_path
        if training_report_path is not None
        else checkpoint.parent / "training_report.json"
    ).resolve()

    reasons: list[str] = []
    checkpoint_present = checkpoint.is_file()
    checkpoint_hash: str | None = None
    checkpoint_bytes: int | None = None
    checkpoint_format = "missing"
    tensor_contract_valid = False
    tensor_shapes: dict[str, list[int]] = {}
    input_dim: int | None = None
    if not checkpoint_present:
        reasons.append("checkpoint file is missing")
    else:
        try:
            payload, checkpoint_hash, checkpoint_bytes = _load_weights_only_snapshot(checkpoint)
            (
                checkpoint_format,
                tensor_contract_valid,
                tensor_shapes,
                tensor_reasons,
            ) = _audit_legacy_state_dict(payload)
            reasons.extend(tensor_reasons)
            first_shape = tensor_shapes.get("net.0.weight") or []
            if len(first_shape) == 2:
                input_dim = int(first_shape[1])
        except ValueError as exc:
            checkpoint_format = "unreadable"
            reasons.append(str(exc))

    metadata_present = metadata.is_file()
    metadata_hash: str | None = None
    metadata_contract_valid = False
    metadata_features: tuple[str, ...] = ()
    metadata_target: str | None = None
    metadata_rows_total: int | None = None
    metadata_payload: dict[str, Any] = {}
    if not metadata_present:
        reasons.append("metadata sidecar is missing")
    else:
        try:
            metadata_payload, metadata_hash = _load_json_snapshot(metadata)
            raw_features = metadata_payload.get("feature_columns")
            if isinstance(raw_features, list) and all(
                isinstance(item, str) and item.strip() for item in raw_features
            ):
                metadata_features = tuple(item.strip() for item in raw_features)
            metadata_target_value = metadata_payload.get("target")
            if isinstance(metadata_target_value, str) and metadata_target_value.strip():
                metadata_target = metadata_target_value.strip()
            metadata_rows_total = _optional_int(metadata_payload.get("rows_total"))
            metadata_symbol = str(
                metadata_payload.get("index_name")
                or metadata_payload.get("symbol")
                or ""
            ).strip().upper()
            scaling = metadata_payload.get("scaling")
            scaling_valid = False
            if isinstance(scaling, Mapping) and metadata_features:
                scaling_valid = _finite_vector(
                    scaling.get("mean"), length=len(metadata_features)
                ) and _finite_vector(
                    scaling.get("std"), length=len(metadata_features), positive=True
                )
            metadata_contract_valid = bool(
                metadata_features
                and len(set(metadata_features)) == len(metadata_features)
                and metadata_target
                and metadata_symbol == normalized_symbol
                and input_dim == len(metadata_features)
                and scaling_valid
            )
            if not metadata_contract_valid:
                reasons.append(
                    "metadata feature, symbol, scaler, or input-dimension contract is invalid"
                )
        except ValueError as exc:
            reasons.append(str(exc))

    report_present = report.is_file()
    report_hash: str | None = None
    report_exit_code: int | None = None
    report_authorizes_symbol = False
    if not report_present:
        reasons.append("training report is missing")
    else:
        try:
            report_payload, report_hash = _load_json_snapshot(report)
            report_exit_code = _optional_int(report_payload.get("exit_code"))
            trained_symbols_raw = report_payload.get("trained_symbols")
            trained_symbols = {
                str(item).strip().upper()
                for item in trained_symbols_raw
                if isinstance(item, str)
            } if isinstance(trained_symbols_raw, list) else set()
            report_authorizes_symbol = bool(
                report_exit_code == 0
                and normalized_symbol in trained_symbols
                and report_payload.get("include_invalid") is False
            )
            if report_exit_code != 0:
                reasons.append("training report records a failed training run")
            if normalized_symbol not in trained_symbols:
                reasons.append("training report does not list the symbol as trained")
            if report_payload.get("include_invalid") is not False:
                reasons.append("training report does not prove invalid rows were excluded")
            minimum = _optional_int(report_payload.get("min_train_rows"))
            if (
                minimum is not None
                and metadata_rows_total is not None
                and metadata_rows_total < minimum
            ):
                reasons.append(
                    f"metadata rows_total={metadata_rows_total} is below training minimum={minimum}"
                )
        except ValueError as exc:
            reasons.append(str(exc))

    if checkpoint_format == "legacy-raw-state-dict":
        reasons.append("legacy checkpoint has no ordered live feature contract wrapper")
        reasons.append("legacy checkpoint is not bound to its sidecars by SHA-256")
        reasons.append(
            "legacy bundle has no immutable DBN, verified-close, surface, or replay provenance"
        )
        reasons.append("legacy bundle has no purged walk-forward or paper evidence")
    elif checkpoint_format == "wrapped-checkpoint":
        reasons.append("wrapped checkpoints are outside this retired-artifact audit")

    # Intentionally fail closed.  Internal tensor coherence is not authority to
    # manufacture a current artifact, auto-load a model, or claim promotion.
    return LegacyGammaArtifactAudit(
        contract_version=AUDIT_CONTRACT_VERSION,
        symbol=normalized_symbol,
        checkpoint_path=str(checkpoint),
        checkpoint_present=checkpoint_present,
        checkpoint_sha256=checkpoint_hash,
        checkpoint_bytes=checkpoint_bytes,
        checkpoint_format=checkpoint_format,
        tensor_contract_valid=tensor_contract_valid,
        tensor_shapes=tensor_shapes,
        metadata_path=str(metadata),
        metadata_present=metadata_present,
        metadata_sha256=metadata_hash,
        metadata_contract_valid=metadata_contract_valid,
        metadata_features=metadata_features,
        metadata_target=metadata_target,
        metadata_rows_total=metadata_rows_total,
        training_report_path=str(report),
        training_report_present=report_present,
        training_report_sha256=report_hash,
        training_report_exit_code=report_exit_code,
        training_report_authorizes_symbol=report_authorizes_symbol,
        loadable_by_current_live_inference=False,
        migration_permitted=False,
        authority="RESEARCH_ONLY",
        promotion_eligible=False,
        reasons=tuple(dict.fromkeys(reasons)),
    )


def audit_legacy_model_directory(
    project_root: str | Path,
    *,
    symbols: Sequence[str] = ("SPX", "NDX"),
    readiness_snapshot_path: str | Path | None = None,
    refresh_readiness: bool = True,
) -> LegacyModelDirectoryAudit:
    """Audit legacy bundles and current evidence counters for required symbols."""

    root = Path(project_root).resolve()
    models = root / "models"
    snapshot = Path(
        readiness_snapshot_path
        if readiness_snapshot_path is not None
        else root / "data" / "closing_tape" / "readiness_snapshot.json"
    ).resolve()
    readiness_present = snapshot.is_file()
    readiness_hash: str | None = None
    snapshot_payload: dict[str, Any] = {}
    reasons: list[str] = []
    if readiness_present:
        try:
            snapshot_payload, readiness_hash = _load_json_snapshot(snapshot)
        except ValueError as exc:
            reasons.append(str(exc))
    readiness = snapshot_payload
    readiness_source = "cached-snapshot"
    if refresh_readiness:
        try:
            from backend.closing_tape.readiness import audit_training_readiness

            readiness = audit_training_readiness(root).to_dict()
            readiness_source = "live-read-only"
        except Exception as exc:  # noqa: BLE001 - fail closed to labeled cache evidence
            reasons.append(
                "current read-only readiness audit failed; using labeled cached snapshot: "
                f"{type(exc).__name__}: {exc}"
            )
    if not readiness:
        reasons.append("closing-tape readiness evidence is missing")

    model_evidence = _optional_int(readiness.get("model_evidence_sessions"))
    model_required = _optional_int(readiness.get("model_sessions_required"))
    paper_evidence = _optional_int(readiness.get("paper_forecast_sessions"))
    paper_required = _optional_int(readiness.get("paper_sessions_required"))
    model_ready = readiness.get("model_training_ready") is True
    paper_ready = readiness.get("paper_evidence_ready") is True
    readiness_label = (
        "current read-only readiness"
        if readiness_source == "live-read-only"
        else "cached readiness snapshot"
    )
    if not model_ready:
        reasons.append(f"{readiness_label} reports model training readiness=false")
    if not paper_ready:
        reasons.append(f"{readiness_label} reports paper evidence readiness=false")
    if model_evidence is not None and model_required is not None and model_evidence < model_required:
        reasons.append(
            f"{readiness_label} model evidence sessions {model_evidence}/{model_required}"
        )
    if paper_evidence is not None and paper_required is not None and paper_evidence < paper_required:
        reasons.append(
            f"{readiness_label} paper forecast sessions {paper_evidence}/{paper_required}"
        )

    symbol_reports: dict[str, dict[str, object]] = {}
    normalized_symbols: list[str] = []
    for raw_symbol in symbols:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or symbol in normalized_symbols:
            continue
        normalized_symbols.append(symbol)
        audit = audit_legacy_gamma_artifact(
            models / f"gamma_model_{symbol}.pt",
            symbol=symbol,
            metadata_path=models / f"gamma_model_{symbol}_meta.json",
            training_report_path=models / "training_report.json",
        )
        symbol_reports[symbol] = audit.to_dict()
        if not audit.checkpoint_present:
            reasons.append(f"{symbol} legacy checkpoint is missing")
        elif not audit.migration_permitted:
            reasons.append(f"{symbol} legacy checkpoint migration is refused")

    return LegacyModelDirectoryAudit(
        contract_version=AUDIT_CONTRACT_VERSION,
        authority="RESEARCH_ONLY",
        migration_permitted=False,
        promotion_eligible=False,
        readiness_source=readiness_source,
        readiness_current=readiness_source == "live-read-only",
        readiness_snapshot_path=str(snapshot),
        readiness_snapshot_present=readiness_present,
        readiness_snapshot_sha256=readiness_hash,
        readiness_generated_at_utc=(
            str(readiness.get("generated_at_utc"))
            if readiness.get("generated_at_utc") is not None
            else None
        ),
        model_evidence_sessions=model_evidence,
        model_sessions_required=model_required,
        paper_forecast_sessions=paper_evidence,
        paper_sessions_required=paper_required,
        model_training_ready=model_ready,
        paper_evidence_ready=paper_ready,
        symbols=symbol_reports,
        reasons=tuple(dict.fromkeys(reasons)),
    )

