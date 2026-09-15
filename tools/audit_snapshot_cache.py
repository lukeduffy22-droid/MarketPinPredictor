"""Offline, research-only acceptance gate for the dashboard snapshot cache.

Runs the exact app.py cache helpers in isolated Streamlit AppTest workers. No
backend, feed, model, live dashboard restart, or database write is involved.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.utils.display_time import parse_utc_timestamp, resolve_display_timezone
from app.utils.snapshot_history import (
    snapshot_research_export_fields,
    utc_partition_dates_for_local_day,
)

HELPERS = (
    "_cached_local_snapshot_selection_payload",
    "_cached_local_snapshot_selection",
)


def digest(value):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=True, allow_nan=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_hash(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def input_manifest(config):
    root = Path(config["exports_root"]).resolve()
    symbol = config["symbol"]
    if not symbol or Path(symbol).name != symbol or symbol in {".", ".."}:
        raise ValueError("symbol must be a single directory name")
    zone = resolve_display_timezone(config["timezone"])
    if zone.name != config["timezone"] or zone.source != "browser":
        raise ValueError("a valid explicit IANA timezone is required")
    manifest = []
    for day in utc_partition_dates_for_local_day(config["local_date"], zone):
        path = root / symbol / f"{day}.ndjson"
        # Include absent candidates: creating a partition must invalidate a run.
        entry = {"path": path.relative_to(root).as_posix(), "exists": path.exists()}
        if entry["exists"]:
            entry.update(size_bytes=path.stat().st_size, sha256=file_hash(path))
        manifest.append(entry)
    return manifest


def primitive_payload(value):
    if value is None or type(value) in (str, int, float, bool):
        return True
    if type(value) in (tuple, list):
        return all(primitive_payload(item) for item in value)
    if type(value) is dict:
        return all(type(key) is str and primitive_payload(item)
                   for key, item in value.items())
    return False


def selection_receipt(selection, config, manifest):
    root = Path(config["exports_root"]).resolve()
    sources = [Path(path).resolve().relative_to(root).as_posix()
               for path in selection.source_files]
    policy = {key: config[key] for key in ("symbol", "local_date", "timezone")}
    identity = {
        "contract": "snapshot-selection-cache.v1", "configuration": policy,
        "input_files": manifest, "source_files": sources,
        "records": selection.records,
        "malformed_timestamp_records": selection.malformed_timestamp_records,
    }
    timestamps = [parse_utc_timestamp(row.get("generated_at_utc")) or
                  parse_utc_timestamp(row.get("timestamp_utc"))
                  for row in selection.records]
    authority = [snapshot_research_export_fields(row) for row in selection.records]
    return {
        # Content identity for this selection, not a forecast/passport ID.
        "snapshot_id": digest(identity),
        "input_file_hashes": manifest,
        "selected_timestamp_utc": timestamps[-1].isoformat() if timestamps else None,
        "first_timestamp_utc": timestamps[0].isoformat() if timestamps else None,
        "row_count": len(selection.records),
        "malformed_timestamp_records": selection.malformed_timestamp_records,
        "source_files": sources,
        "provenance_counts": dict(sorted(Counter(
            row["export_provenance_status"] for row in authority).items())),
        "research_only": all(row["research_only"] is True for row in authority),
        "current_live_eligible_count": sum(row["current_live_eligible"] is not False
                                           for row in authority),
        "decision_grade": False,
    }


def helper_source(app_path):
    source = Path(app_path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    nodes = {node.name: node for node in tree.body
             if isinstance(node, ast.FunctionDef) and node.name in HELPERS}
    if set(nodes) != set(HELPERS):
        raise ValueError("production snapshot helpers missing")
    decorated, wrapper = (nodes[name] for name in HELPERS)
    if (len(decorated.decorator_list) != 1 or
            not isinstance(decorated.decorator_list[0], ast.Call) or
            ast.unparse(decorated.decorator_list[0].func) != "st.cache_data" or
            wrapper.decorator_list):
        raise ValueError("cache must use st.cache_data with uncached reconstruction")
    return "\n\n".join(ast.unparse(nodes[name]) for name in HELPERS)


def worker(config, result_path, phase):
    from streamlit.testing.v1 import AppTest
    helpers = helper_source(config["app_path"])
    # Only the two production helpers execute. Importing the complete dashboard
    # would initialize unrelated UI/network/model code and is deliberately avoided.
    source = f'''
import importlib
from typing import Any
import streamlit as st
import app.utils.snapshot_history as history
from app.utils.display_time import resolve_display_timezone
from tools.audit_snapshot_cache import primitive_payload, selection_receipt
history = importlib.reload(history)
SnapshotSelection = history.SnapshotSelection
def load_snapshots_for_local_day(*args):
    st.session_state["disk_loads"] = st.session_state.get("disk_loads", 0) + 1
    return history.load_snapshots_for_local_day(*args)
{helpers}
config = {config!r}
args = (config["exports_root"], config["symbol"], config["local_date"], config["timezone"])
selection = _cached_local_snapshot_selection(*args)
payload = _cached_local_snapshot_selection_payload(*args)
st.session_state["receipt"] = selection_receipt(selection, config, config["manifest"])
st.session_state["primitive_payload"] = primitive_payload(payload)
st.session_state["current_class"] = type(selection) is history.SnapshotSelection
'''
    app = AppTest.from_string(source, default_timeout=60)
    runs = []
    for number in range(4 if phase == "warm" else 1):
        before = input_manifest(config)
        app.run()
        after = input_manifest(config)
        errors = [str(exc.message) for exc in app.exception]
        run = {
            "phase": ("initial" if number == 0 else f"rerun_{number}")
                     if phase == "warm" else "cold_restart",
            "pid": os.getpid(), "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "errors": errors, "inputs_unchanged": before == after == config["manifest"],
            "manifest_before": before, "manifest_after": after,
        }
        if not errors:
            run.update(receipt=app.session_state["receipt"],
                       disk_loads=app.session_state["disk_loads"],
                       primitive_payload=app.session_state["primitive_payload"],
                       current_class=app.session_state["current_class"])
        runs.append(run)
    Path(result_path).write_text(json.dumps(runs, indent=2), encoding="utf-8")


def evaluate_runs(runs):
    expected = ["initial", "rerun_1", "rerun_2", "rerun_3", "cold_restart"]
    reasons = []
    if [run.get("phase") for run in runs] != expected:
        reasons.append("MISSING_REQUIRED_RUNS")
    if any(run.get("errors") for run in runs):
        reasons.append("STREAMLIT_ERROR")
    if any(not run.get("inputs_unchanged") for run in runs):
        reasons.append("INPUT_FILES_CHANGED")
    receipts = [run.get("receipt") for run in runs]
    if not receipts or any(receipt is None for receipt in receipts):
        reasons.append("MISSING_SELECTION_EVIDENCE")
    else:
        if any(receipt != receipts[0] for receipt in receipts):
            reasons.append("SELECTION_CHANGED")
        if any(receipt["row_count"] <= 0 or not receipt["selected_timestamp_utc"]
               or not receipt["source_files"] for receipt in receipts):
            reasons.append("EMPTY_SELECTION")
        if any(receipt["research_only"] is not True or
               receipt["decision_grade"] is not False or
               receipt["current_live_eligible_count"] != 0 for receipt in receipts):
            reasons.append("RESEARCH_BOUNDARY_VIOLATION")
    if any(not run.get("primitive_payload") or not run.get("current_class")
           for run in runs):
        reasons.append("UNSAFE_CACHE_PAYLOAD_OR_STALE_CLASS")
    if any(run.get("disk_loads") != 1 for run in runs):
        reasons.append("WARM_CACHE_HITS_NOT_PROVEN")
    if len(runs) == 5 and (any(type(run.get("pid")) is not int or run["pid"] <= 0
                              for run in runs) or
                          len({run.get("pid") for run in runs[:4]}) != 1 or
                          runs[-1].get("pid") == runs[0].get("pid")):
        reasons.append("COLD_PROCESS_NOT_PROVEN")
    return reasons


def audit(config):
    config = dict(config)
    config["exports_root"] = str(Path(config["exports_root"]).resolve())
    config["app_path"] = str(PROJECT_ROOT / "app.py")
    config["manifest"] = input_manifest(config)
    source_paths = [PROJECT_ROOT / "app.py", PROJECT_ROOT / "app/utils/snapshot_history.py",
                    PROJECT_ROOT / "app/utils/display_time.py",
                    PROJECT_ROOT / "backend/workstation.py", PROJECT_ROOT / "backend/config.py",
                    Path(__file__).resolve()]
    sources = {str(path.relative_to(PROJECT_ROOT)): file_hash(path) for path in source_paths}
    # Validate structure before starting any worker.
    helper_source(config["app_path"])
    runs, failures = [], []
    with tempfile.TemporaryDirectory(prefix="snapshot-cache-gate-") as scratch:
        scratch = Path(scratch)
        config_path = scratch / "config.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        for phase in ("warm", "cold"):
            result_path = scratch / f"{phase}.json"
            process = subprocess.run(
                [sys.executable, "-B", str(Path(__file__).resolve()), "--worker", phase,
                 "--config", str(config_path), "--output", str(result_path)],
                cwd=PROJECT_ROOT, capture_output=True, text=True, timeout=180,
                env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            )
            if process.returncode or not result_path.is_file():
                failures.append({"phase": phase, "returncode": process.returncode,
                                 "stderr": process.stderr[-8000:]})
            else:
                runs.extend(json.loads(result_path.read_text(encoding="utf-8")))
    reasons = evaluate_runs(runs)
    if failures:
        reasons.append("WORKER_FAILED")
    if input_manifest(config) != config["manifest"]:
        reasons.append("INPUT_FILES_CHANGED")
    if any(file_hash(PROJECT_ROOT / path) != value for path, value in sources.items()):
        reasons.append("SOURCE_CHANGED_DURING_AUDIT")
    import streamlit
    return {
        "gate": "deterministic_snapshot_cache.v1",
        "status": "FAIL" if reasons else "PASS",
        "failure_reasons": sorted(set(reasons)),
        "research_only": True, "decision_grade": False,
        "scope": "Offline production cache helpers; no live readiness or predictive validation claim",
        "snapshot_id_definition": "SHA256 of canonical ordered selection, configuration, input manifest and diagnostics; not a forecast ID",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python": sys.version, "python_executable": sys.executable,
        "streamlit": streamlit.__version__, "configuration": config,
        "source_sha256": sources, "runs": runs, "worker_failures": failures,
        "streamlit_error_count": sum(len(run["errors"]) for run in runs),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exports-root")
    parser.add_argument("--symbol", default="SPX")
    parser.add_argument("--local-date")
    parser.add_argument("--timezone", default="America/Chicago")
    parser.add_argument("--output", required=True)
    parser.add_argument("--worker", choices=("warm", "cold"), help=argparse.SUPPRESS)
    parser.add_argument("--config", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.worker:
        worker(json.loads(Path(args.config).read_text(encoding="utf-8")),
               args.output, args.worker)
        return 0
    if not args.exports_root or not args.local_date:
        parser.error("--exports-root and --local-date are required")
    try:
        result = audit({"exports_root": args.exports_root, "symbol": args.symbol,
                        "local_date": args.local_date, "timezone": args.timezone})
    except Exception as exc:
        result = {"gate": "deterministic_snapshot_cache.v1", "status": "FAIL",
                  "failure_reasons": ["AUDIT_ERROR"], "error": str(exc),
                  "research_only": True, "decision_grade": False}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({"status": result["status"], "failure_reasons": result["failure_reasons"],
                      "evidence": str(output.resolve())}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
