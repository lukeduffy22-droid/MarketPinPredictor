"""Orchestrate full-history dataset build and multi-symbol CUDA training."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import torch

# Ensure repository root is importable even when script is launched via absolute path.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from build_full_gamma_dataset import build_dataset
except ModuleNotFoundError:
    # Backward-compatible fallback for older script/module naming.
    from build_full_dataset import build_dataset

from train_gamma_model import train_model, estimate_trainable_rows
from app.utils.settings import settings
from app.utils.time_et import now_et, is_regular_hours


def _candidate_snapshot_roots(primary: str) -> list[Path]:
    candidates = [
        Path(primary),
        Path("./logs/audit_validated"),
        Path("./logs/audit"),
        Path("./exports"),
        Path("./clean_app/data/snapshots_raw"),
    ]

    # Keep insertion order, remove duplicates.
    deduped = []
    seen = set()
    for c in candidates:
        key = str(c.resolve()) if c.exists() else str(c)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    return deduped


def main() -> int:
    parser = argparse.ArgumentParser(description="Train institutional models on full historical data")
    parser.add_argument("--exports-dir", default="./exports", help="Root NDJSON export directory")
    parser.add_argument("--dataset-csv", default="./data/full_gamma_history.csv", help="Unified dataset output")
    parser.add_argument("--symbols", default="SPX,NDX,DJI,RUT", help="Comma-separated symbols")
    parser.add_argument("--rebuild", action="store_true", help="Rebuild unified dataset from NDJSON")
    parser.add_argument("--include-invalid", action="store_true", help="Include invalid snapshots in training corpus")
    parser.add_argument("--epochs", type=int, default=60)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--patience", type=int, default=8)
    parser.add_argument("--validation-split", type=float, default=0.2)
    parser.add_argument("--min-train-rows", type=int, default=100, help="Minimum usable rows required to train a symbol")
    parser.add_argument("--report-path", default="./models/training_report.json", help="Machine-readable training report output")
    args = parser.parse_args()

    dataset_path = Path(args.dataset_csv)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)

    failures = []
    skipped = []
    trained = []

    def write_report(exit_code: int, source: str | None = None) -> None:
        report_path = Path(args.report_path)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        now_utc = datetime.utcnow()
        market_open = is_regular_hours(now_utc)
        provider = (settings.market_data_provider or "auto").strip().lower()
        market_frozen, freeze_reason = __import__("app.utils.market_time", fromlist=["get_freeze_status"]).get_freeze_status()

        if provider == "polygon":
            databento_status = "disabled_by_config"
        elif not settings.databento_api_key:
            if settings.polygon_api_key and market_open:
                databento_status = "fallback_polygon_active"
            else:
                databento_status = "disabled_by_config"
        elif market_open:
            databento_status = "available"
        elif market_frozen:
            databento_status = "paused_closed"
        else:
            databento_status = "paused_closed"

        report = {
            "exit_code": exit_code,
            "timestamp_utc": now_utc.isoformat() + "Z",
            "timestamp_et": now_et(now_utc).isoformat(),
            "market_status": "open" if market_open else "closed",
            "market_data_provider": provider,
            "databento_status": databento_status,
            "freeze_reason": freeze_reason,
            "dataset_csv": str(dataset_path),
            "dataset_source": source,
            "include_invalid": bool(args.include_invalid),
            "min_train_rows": int(args.min_train_rows),
            "trained_symbols": trained,
            "skipped_symbols": [{"symbol": s, "reason": r} for s, r in skipped],
            "failed_symbols": [{"symbol": s, "reason": r} for s, r in failures],
        }
        with open(report_path, "w", encoding="utf-8") as fp:
            json.dump(report, fp, indent=2)
        print(f"Training report saved: {report_path}")

    selected_root = None
    df = None

    if args.rebuild or not dataset_path.exists():
        for root in _candidate_snapshot_roots(args.exports_dir):
            print(f"Building unified dataset from {root}")
            df = build_dataset(str(root), include_invalid=args.include_invalid)
            if not df.empty:
                selected_root = root
                break

        if df.empty and not args.include_invalid:
            print("No validated rows found. Retrying with invalid snapshots included...")
            for root in _candidate_snapshot_roots(args.exports_dir):
                print(f"Rebuilding dataset from {root} (include_invalid=True)")
                df = build_dataset(str(root), include_invalid=True)
                if not df.empty:
                    selected_root = root
                    break

        if df.empty:
            print("No data found; aborting")
            print("Checked snapshot roots:")
            for root in _candidate_snapshot_roots(args.exports_dir):
                print(f" - {root}")
            write_report(1)
            return 1

        print(f"Using snapshot source: {selected_root}")
        df.to_csv(dataset_path, index=False)
        print(f"Saved dataset: {dataset_path} ({len(df)} rows)")
    else:
        print(f"Using existing dataset: {dataset_path}")
        df = pd.read_csv(dataset_path)
        selected_root = "existing_dataset"

    print(f"CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"CUDA device: {torch.cuda.get_device_name(0)}")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    models_dir = Path("./models")
    models_dir.mkdir(parents=True, exist_ok=True)

    for symbol in symbols:
        usable_rows, reason = estimate_trainable_rows(df, symbol, include_invalid=args.include_invalid)
        if usable_rows < args.min_train_rows:
            skip_reason = (
                f"usable_rows={usable_rows} below min_train_rows={args.min_train_rows}; "
                f"precheck={reason}"
            )
            skipped.append((symbol, skip_reason))
            print(f"\n⚠️ Skipping {symbol}: {skip_reason}")
            continue

        print(f"\n=== Training {symbol} ===")
        model_out = models_dir / f"gamma_model_{symbol}.pt"
        meta_out = models_dir / f"gamma_model_{symbol}_meta.json"

        try:
            train_model(
                data_path=str(dataset_path),
                index_name=symbol,
                model_out=str(model_out),
                meta_out=str(meta_out),
                batch_size=args.batch_size,
                epochs=args.epochs,
                learning_rate=args.lr,
                validation_split=args.validation_split,
                early_stopping_patience=args.patience,
                max_rows=0,
                include_invalid=args.include_invalid,
            )
            trained.append(symbol)
        except Exception as exc:
            failures.append((symbol, str(exc)))
            print(f"Training failed for {symbol}: {exc}")

    if skipped:
        print("\nSymbols skipped (precheck):")
        for symbol, reason in skipped:
            print(f" - {symbol}: {reason}")

    if failures:
        print("\nTraining finished with failures:")
        for symbol, err in failures:
            print(f" - {symbol}: {err}")
        write_report(1, str(selected_root) if selected_root is not None else None)
        return 1

    if not trained:
        print("\nNo symbols were trained. Adjust --min-train-rows or include more data.")
        write_report(1, str(selected_root) if selected_root is not None else None)
        return 1

    print(f"\nTraining complete. Trained symbols: {', '.join(trained)}")
    if skipped:
        print("Skipped symbols were explicitly reported above.")
    write_report(0, str(selected_root) if selected_root is not None else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
