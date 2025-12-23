#!/usr/bin/env python3
# run_eod_gamma_pipeline.py
"""
End-of-Day Gamma Pipeline Orchestrator

This script runs the full EOD workflow:
1. Build dataset from NDJSON snapshots
2. Train models for each index
3. Run predictions
4. Validate predictions (if actuals available)
5. Bundle all artifacts into a ZIP

Usage:
    python run_eod_gamma_pipeline.py <date> [--skip-train] [--skip-validate]
    
Examples:
    python run_eod_gamma_pipeline.py 2025-12-22
    python run_eod_gamma_pipeline.py 2025-12-22 --skip-train
"""

import sys
import os
import argparse
import subprocess
import zipfile
from datetime import datetime

INDEXES = ["SPX", "NDX", "RUT"]
EXPORTS_DIR = "./exports"
ARTIFACTS_DIR = "./artifacts"

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")

def run_command(cmd, description):
    """Run a shell command and return success status."""
    log(f"Running: {description}")
    log(f"Command: {' '.join(cmd)}", level="DEBUG")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        log(f"FAILED: {description}", level="ERROR")
        log(f"stderr: {result.stderr}", level="ERROR")
        return False
    
    if result.stdout:
        for line in result.stdout.strip().split('\n')[-5:]:
            log(f"  {line}")
    
    return True

def step_build_dataset(date, output_csv):
    """Step 1: Build unified dataset from NDJSON files."""
    log("="*60)
    log("STEP 1: Building Dataset")
    log("="*60)
    
    files_found = []
    for symbol in INDEXES:
        symbol_dir = os.path.join(EXPORTS_DIR, symbol)
        ndjson_file = os.path.join(symbol_dir, f"{date}.ndjson")
        if os.path.exists(ndjson_file):
            files_found.append(ndjson_file)
            log(f"  Found: {ndjson_file}")
    
    if not files_found:
        log(f"No NDJSON files found for date {date}", level="ERROR")
        return False
    
    cmd = ["python", "build_full_gamma_dataset.py", EXPORTS_DIR, output_csv, date]
    return run_command(cmd, f"Building dataset -> {output_csv}")

def step_train_models(dataset_csv):
    """Step 2: Train models for each index."""
    log("="*60)
    log("STEP 2: Training Models")
    log("="*60)
    
    success = True
    for symbol in INDEXES:
        cmd = ["python", "train_gamma_model.py", dataset_csv, symbol]
        if not run_command(cmd, f"Training model for {symbol}"):
            log(f"Training failed for {symbol}", level="WARN")
            success = False
    
    return success

def step_run_predictions(dataset_csv, date):
    """Step 3: Run predictions for each index."""
    log("="*60)
    log("STEP 3: Running Predictions")
    log("="*60)
    
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    predictions = {}
    
    for symbol in INDEXES:
        model_file = f"gamma_model_{symbol}.pt"
        if not os.path.exists(model_file):
            log(f"Model not found: {model_file}", level="WARN")
            continue
        
        output_csv = os.path.join(ARTIFACTS_DIR, f"predictions_{symbol}_{date}.csv")
        cmd = ["python", "predict_gamma_model.py", dataset_csv, symbol, output_csv]
        
        if run_command(cmd, f"Predictions for {symbol}"):
            predictions[symbol] = output_csv
    
    return predictions

def step_validate_predictions(predictions, date, actuals_csv=None):
    """Step 4: Validate predictions (if actuals available)."""
    log("="*60)
    log("STEP 4: Validating Predictions")
    log("="*60)
    
    if not predictions:
        log("No predictions to validate", level="WARN")
        return True
    
    if actuals_csv and os.path.exists(actuals_csv):
        log(f"Using actuals file: {actuals_csv}")
        for symbol, pred_file in predictions.items():
            if not os.path.exists(pred_file):
                continue
            
            output_report = os.path.join(ARTIFACTS_DIR, f"validation_{symbol}_{date}.csv")
            cmd = ["python", "validate_gamma_predictions.py", pred_file, actuals_csv, output_report]
            run_command(cmd, f"Validating {symbol}")
    else:
        log("No actuals file provided.", level="WARN")
        log("To validate, provide --actuals <actuals.csv> with columns: timestamp_utc, symbol, actual_close", level="INFO")
        log("Or add 'actual_close' column directly to prediction files.", level="INFO")
    
    return True

def step_bundle_artifacts(date, dataset_csv, predictions):
    """Step 5: Bundle all artifacts into a ZIP."""
    log("="*60)
    log("STEP 5: Bundling Artifacts")
    log("="*60)
    
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    zip_path = os.path.join(ARTIFACTS_DIR, f"gamma_pipeline_{date}.zip")
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(dataset_csv):
            zf.write(dataset_csv, os.path.basename(dataset_csv))
            log(f"  Added: {dataset_csv}")
        
        for symbol in INDEXES:
            model_file = f"gamma_model_{symbol}.pt"
            meta_file = f"gamma_model_{symbol}_meta.json"
            
            if os.path.exists(model_file):
                zf.write(model_file, model_file)
                log(f"  Added: {model_file}")
            if os.path.exists(meta_file):
                zf.write(meta_file, meta_file)
                log(f"  Added: {meta_file}")
        
        for symbol, pred_file in predictions.items():
            if os.path.exists(pred_file):
                zf.write(pred_file, os.path.basename(pred_file))
                log(f"  Added: {pred_file}")
        
        for symbol in INDEXES:
            symbol_dir = os.path.join(EXPORTS_DIR, symbol)
            ndjson_file = os.path.join(symbol_dir, f"{date}.ndjson")
            if os.path.exists(ndjson_file):
                zf.write(ndjson_file, f"ndjson/{symbol}_{date}.ndjson")
                log(f"  Added: {ndjson_file}")
    
    log(f"Bundle saved: {zip_path}")
    return zip_path

def main():
    parser = argparse.ArgumentParser(description="EOD Gamma Pipeline Orchestrator")
    parser.add_argument("date", help="Date in YYYY-MM-DD format")
    parser.add_argument("--skip-train", action="store_true", help="Skip model training")
    parser.add_argument("--skip-validate", action="store_true", help="Skip validation")
    parser.add_argument("--actuals", help="CSV file with actual close prices (columns: timestamp_utc, symbol, actual_close)")
    args = parser.parse_args()
    
    date = args.date
    dataset_csv = os.path.join(ARTIFACTS_DIR, f"all_indices_{date}.csv")
    
    log("="*60)
    log(f"EOD GAMMA PIPELINE - {date}")
    log("="*60)
    
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    
    if not step_build_dataset(date, dataset_csv):
        log("Pipeline failed at Step 1", level="ERROR")
        return 1
    
    if not args.skip_train:
        if not step_train_models(dataset_csv):
            log("Some models failed to train", level="WARN")
    else:
        log("Skipping training (--skip-train)")
    
    predictions = step_run_predictions(dataset_csv, date)
    
    if not args.skip_validate:
        step_validate_predictions(predictions, date, args.actuals)
    else:
        log("Skipping validation (--skip-validate)")
    
    zip_path = step_bundle_artifacts(date, dataset_csv, predictions)
    
    log("="*60)
    log("PIPELINE COMPLETE")
    log(f"Artifacts: {zip_path}")
    log("="*60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
