from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.pipeline import build_research_surface_dataset
from backend.closing_tape.catalog_discovery import select_closing_tape_catalogs
from backend.closing_tape.dataset import load_scored_marketpin_closes
from backend.closing_tape.compute_guard import require_compute_window
from backend.closing_tape.surface_artifact import write_research_surface_artifact
from backend.closing_tape.training import prepare_close_training_dataset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build an integrity-gated MarketPin contract-surface research dataset"
    )
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--catalog", action="append", default=[])
    parser.add_argument("--market-db")
    parser.add_argument("--max-price-age-seconds", type=float, default=90.0)
    parser.add_argument("--output-parquet")
    parser.add_argument("--output-manifest")
    parser.add_argument(
        "--artifact-dir",
        help="write a content-addressed, re-loadable Parquet surface and strict manifest",
    )
    parser.add_argument(
        "--allow-active-capture",
        action="store_true",
        help="explicitly allow CPU-heavy surface construction during a live recorder session",
    )
    args = parser.parse_args(argv)
    root = Path(args.project_root).resolve()
    try:
        active_captures = require_compute_window(
            root,
            operation="research-surface construction",
            allow_active_capture=args.allow_active_capture,
        )
    except RuntimeError as exc:
        parser.error(str(exc))
    catalog_selection = select_closing_tape_catalogs(
        root,
        explicit_catalogs=args.catalog,
    )
    if catalog_selection.issues:
        parser.error(
            "catalog selection failed: " + "; ".join(catalog_selection.issues)
        )
    catalogs = list(catalog_selection.catalog_paths)
    market_db = Path(args.market_db).resolve() if args.market_db else root / "data" / "market_data.db"
    surface, report = build_research_surface_dataset(
        catalogs, market_db, max_price_age_seconds=args.max_price_age_seconds
    )
    payload = report.to_dict()
    payload["catalog_selection"] = {
        "mode": "explicit" if args.catalog else "configured_roots",
        **catalog_selection.to_dict(),
    }
    payload["compute_window"] = {
        "active_capture_override": bool(args.allow_active_capture),
        "active_captures": [item.to_dict() for item in active_captures],
    }
    closes = load_scored_marketpin_closes(
        market_db,
        verified_artifact_root=root / "data" / "verified_close_sources",
    )
    _labeled, training_readiness = prepare_close_training_dataset(surface, closes)
    payload["close_training_readiness"] = training_readiness.to_dict()
    if args.artifact_dir:
        frozen = write_research_surface_artifact(
            surface,
            artifact_dir=Path(args.artifact_dir),
            report=report,
            max_price_age_seconds=args.max_price_age_seconds,
        )
        payload["frozen_surface_artifact"] = {
            "artifact_sha256": frozen.artifact_sha256,
            "artifact_path": str(frozen.parquet_path),
            "manifest_path": str(frozen.manifest_path),
            "artifact_bytes": frozen.manifest["artifact_bytes"],
        }
    if args.output_parquet:
        output = Path(args.output_parquet).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        surface.to_parquet(output, index=False)
        payload["output_parquet"] = str(output)
        payload["output_bytes"] = output.stat().st_size
    if args.output_manifest:
        manifest = Path(args.output_manifest).resolve()
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
    print(json.dumps(payload, sort_keys=True, indent=2))
    return 0 if report.surface_rows else 2


if __name__ == "__main__":
    raise SystemExit(main())
