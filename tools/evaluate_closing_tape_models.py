from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.dataset import load_scored_marketpin_closes
from backend.closing_tape.catalog_discovery import select_closing_tape_catalogs
from backend.closing_tape.compute_guard import require_compute_window
from backend.closing_tape.evaluation import evaluate_close_models_if_ready
from backend.closing_tape.candidate import (
    package_promoted_candidate,
    write_paper_candidate_descriptor,
)
from backend.closing_tape.pipeline import build_research_surface_dataset
from backend.closing_tape.surface import MODEL_FEATURE_COLUMNS, MODEL_FEATURE_CONTRACT_HASH
from backend.closing_tape.surface_artifact import (
    load_replay_verified_research_surface_artifact,
)
from backend.closing_tape.training import audit_close_label_preflight


def _emit_payload(
    payload: dict[str, object],
    *,
    output: Path | None,
    strict_json: bool,
) -> None:
    serialized = json.dumps(
        payload, sort_keys=True, indent=2, allow_nan=not strict_json
    ) + "\n"
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(serialized, encoding="utf-8", newline="\n")
            temporary.replace(output)
        finally:
            if temporary.exists():
                temporary.unlink()
    print(serialized, end="")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Evidence-gated ridge and CUDA MLP evaluation for MarketPin closing tape"
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--catalog", action="append", default=[])
    parser.add_argument("--market-db")
    parser.add_argument(
        "--surface-manifest",
        help=(
            "load a content-addressed surface only after retained-source "
            "verification and exact catalog replay"
        ),
    )
    parser.add_argument(
        "--allow-active-capture",
        action="store_true",
        help="explicitly allow surface loading and CUDA evaluation during live capture",
    )
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--max-epochs", type=int, default=100)
    parser.add_argument("--patience", type=int, default=12)
    parser.add_argument("--batch-size", type=int, default=2048)
    parser.add_argument("--output-json")
    parser.add_argument("--package-version")
    parser.add_argument("--candidate-artifact")
    parser.add_argument(
        "--calibration-asof-utc",
        help="Timezone-aware ISO timestamp; defaults to the current UTC time",
    )
    parser.add_argument("--allow-artifact-replace", action="store_true")
    parser.add_argument(
        "--enable-paper-candidate",
        action="store_true",
        help="explicitly publish the packaged immutable receipt for paper forecasts",
    )
    parser.add_argument("--allow-paper-descriptor-replace", action="store_true")
    parser.add_argument("--allow-output-replace", action="store_true")
    args = parser.parse_args(argv)
    if args.allow_paper_descriptor_replace and not args.enable_paper_candidate:
        parser.error(
            "--allow-paper-descriptor-replace requires --enable-paper-candidate"
        )
    packaging_requested = bool(
        args.package_version
        or args.candidate_artifact
        or args.calibration_asof_utc
        or args.enable_paper_candidate
        or args.allow_paper_descriptor_replace
    )
    if packaging_requested:
        if not args.package_version or not args.candidate_artifact:
            parser.error("--package-version and --candidate-artifact are both required for packaging")
        if not args.output_json:
            parser.error("--output-json is required to preserve packaged candidate evidence")
        if not args.surface_manifest:
            parser.error(
                "packaging requires --surface-manifest so retained sources can be replay verified"
            )
    if args.device == "cuda" and not args.surface_manifest:
        parser.error(
            "CUDA evaluation requires --surface-manifest so retained sources can be replay verified"
        )
    root = Path(args.project_root).resolve()
    if args.enable_paper_candidate:
        candidate_destination = Path(args.candidate_artifact).resolve()
        if candidate_destination.parent != (root / "models").resolve():
            parser.error(
                "--enable-paper-candidate requires --candidate-artifact directly under project models/"
            )
    output = Path(args.output_json).resolve() if args.output_json else None
    if output is not None and output.exists() and not args.allow_output_replace:
        parser.error(
            "output evidence already exists; use --allow-output-replace for explicit replacement"
        )
    catalog_selection = select_closing_tape_catalogs(
        root,
        explicit_catalogs=args.catalog,
    )
    if catalog_selection.issues:
        parser.error(
            "catalog selection failed: "
            + "; ".join(catalog_selection.issues)
        )
    catalogs = list(catalog_selection.catalog_paths)
    catalog_selection_payload = {
        "mode": "explicit" if args.catalog else "configured_roots",
        **catalog_selection.to_dict(),
    }
    market_db = Path(args.market_db).resolve() if args.market_db else root / "data" / "market_data.db"
    closes = load_scored_marketpin_closes(
        market_db,
        verified_artifact_root=root / "data" / "verified_close_sources",
    )
    label_preflight = audit_close_label_preflight(closes)
    if not label_preflight.ready:
        preflight_payload = label_preflight.to_dict()
        payload = {
            "label_preflight": preflight_payload,
            "catalog_selection": catalog_selection_payload,
            "surface": {
                "skipped": True,
                "catalog_count": len(catalogs),
                "reason": "verified-close preflight failed before TCBBO contract scan",
            },
            "model_evaluation": {
                "trained": False,
                "device_requested": args.device,
                "model_feature_contract_hash": MODEL_FEATURE_CONTRACT_HASH,
                "model_feature_columns": MODEL_FEATURE_COLUMNS,
                "readiness": preflight_payload,
                "evaluation": None,
                "reasons": label_preflight.reasons,
            },
        }
        _emit_payload(payload, output=output, strict_json=packaging_requested)
        return 2

    try:
        active_captures = require_compute_window(
            root,
            operation="closing-tape model evaluation",
            allow_active_capture=args.allow_active_capture,
        )
    except RuntimeError as exc:
        parser.error(str(exc))
    if args.surface_manifest:
        frozen_surface = load_replay_verified_research_surface_artifact(
            args.surface_manifest,
            project_root=root,
            market_db_path=market_db,
            explicit_catalogs=args.catalog,
        )
        surface = frozen_surface.frame
        evaluation_surface = frozen_surface
        catalog_selection_payload = {
            "mode": "explicit" if args.catalog else "configured_roots",
            **frozen_surface.catalog_selection.to_dict(),
        }
        surface_payload = {
            **frozen_surface.manifest,
            "loaded_from_artifact": True,
            "replay_verified": True,
            "artifact_sha256": frozen_surface.artifact_sha256,
            "surface_artifact_sha256": frozen_surface.artifact_sha256,
            "surface_replay_receipt_sha256": (
                frozen_surface.replay_receipt_sha256
            ),
            "artifact_path": str(frozen_surface.parquet_path),
            "manifest_path": str(frozen_surface.manifest_path),
            "replay_verification": frozen_surface.replay_verification,
        }
    else:
        surface, surface_report = build_research_surface_dataset(catalogs, market_db)
        evaluation_surface = surface
        surface_payload = surface_report.to_dict()
    evaluation = evaluate_close_models_if_ready(
        evaluation_surface,
        closes,
        device=args.device,
        max_epochs=args.max_epochs,
        patience=args.patience,
        batch_size=args.batch_size,
    )
    payload = {
        "catalog_selection": catalog_selection_payload,
        "surface": surface_payload,
        "model_evaluation": evaluation.to_dict(),
        "compute_window": {
            "active_capture_override": bool(args.allow_active_capture),
            "active_captures": [item.to_dict() for item in active_captures],
            "device_requested": args.device,
            "device_effective": evaluation.device_effective,
        },
    }
    if packaging_requested:
        try:
            json.dumps(payload, sort_keys=True, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "candidate evaluation evidence is not strict finite JSON"
            ) from exc
        if args.calibration_asof_utc:
            try:
                calibration_asof = datetime.fromisoformat(
                    args.calibration_asof_utc.replace("Z", "+00:00")
                )
            except ValueError as exc:
                parser.error(f"invalid --calibration-asof-utc: {exc}")
            if calibration_asof.tzinfo is None:
                parser.error("--calibration-asof-utc must include a timezone")
        else:
            calibration_asof = datetime.now(timezone.utc)
        package = package_promoted_candidate(
            evaluation,
            version=args.package_version,
            artifact_path=Path(args.candidate_artifact).resolve(),
            calibration_asof_utc=calibration_asof,
            device=(evaluation.evaluation_report.device if evaluation.evaluation_report else None),
            allow_replace=args.allow_artifact_replace,
        )
        payload["candidate_package"] = package.to_dict()
        if args.enable_paper_candidate:
            descriptor = write_paper_candidate_descriptor(
                root,
                package,
                allow_replace=args.allow_paper_descriptor_replace,
            )
            payload["paper_candidate_descriptor"] = str(descriptor)
    _emit_payload(payload, output=output, strict_json=packaging_requested)
    return 0 if evaluation.trained else 2


if __name__ == "__main__":
    raise SystemExit(main())
