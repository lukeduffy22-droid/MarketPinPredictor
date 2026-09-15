"""Acceptance evidence for the offline production Streamlit cache gate."""

import ast
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

import app.utils.snapshot_history as history
from app.utils.display_time import resolve_display_timezone
from tools import audit_snapshot_cache as gate


def _write_sample(root):
    symbol_dir = root / "SPX"
    symbol_dir.mkdir(parents=True)
    identity = {
        "validation_is_valid": True,
        "gamma_excluded_from_model": False,
        "subscription_epoch_id": "a" * 64,
        "subscription_generation": 7,
    }
    partitions = {
        "2026-09-11": [
            {"id": "before-day", "timestamp_utc": "2026-09-11T04:59:59Z"},
            {"id": "invalid", "timestamp_utc": "2026-09-11T18:00:00Z",
             "validation_is_valid": False, "gamma_excluded_from_model": True},
            {"id": "legacy", "timestamp_utc": "2026-09-11T05:00:00Z",
             "validation_is_valid": True, "gamma_excluded_from_model": False},
            {"id": "equal-research", "generated_at_utc": "2026-09-11T14:00:00Z",
             **identity, "research_only": False, "current_live_eligible": True},
            {"id": "missing-timestamp"},
            {"id": "bad-timestamps", "generated_at_utc": "invalid",
             "timestamp_utc": "also invalid"},
        ],
        "2026-09-12": [
            {"id": "equal-fallback", "generated_at_utc": "invalid",
             "timestamp_utc": "2026-09-11T14:00:00Z", **identity,
             "is_fallback": True},
            {"id": "final-research", "timestamp_utc": "2026-09-12T04:59:59Z",
             **identity},
            {"id": "after-day", "timestamp_utc": "2026-09-12T05:00:00Z"},
        ],
    }
    for day, records in partitions.items():
        (symbol_dir / f"{day}.ndjson").write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in records)
            + "\nnot-json\n[]\n", encoding="utf-8",
        )
    return {
        "exports_root": str(root), "symbol": "SPX",
        "local_date": "2026-09-11", "timezone": "America/Chicago",
    }


@pytest.fixture(scope="module")
def sample(tmp_path_factory):
    return _write_sample(tmp_path_factory.mktemp("cache-evidence"))


@pytest.fixture(scope="module")
def observed_audit(sample):
    # Execute the real decorated production functions in two separate workers.
    return gate.audit(sample)


def test_real_cache_hits_and_cold_restart_preserve_exact_selection(sample, observed_audit):
    assert observed_audit["status"] == "PASS", observed_audit
    assert observed_audit["failure_reasons"] == []
    runs = observed_audit["runs"]
    assert [run["phase"] for run in runs] == [
        "initial", "rerun_1", "rerun_2", "rerun_3", "cold_restart",
    ]
    assert len({run["pid"] for run in runs[:4]}) == 1
    assert runs[-1]["pid"] != runs[0]["pid"]
    assert all(run["disk_loads"] == 1 for run in runs)
    assert all(run["primitive_payload"] and run["current_class"] for run in runs)
    assert all(run["inputs_unchanged"] and not run["errors"] for run in runs)
    receipt = runs[0]["receipt"]
    assert all(run["receipt"] == receipt for run in runs)
    assert receipt["first_timestamp_utc"] == "2026-09-11T05:00:00+00:00"
    assert receipt["selected_timestamp_utc"] == "2026-09-12T04:59:59+00:00"
    assert receipt["row_count"] == 5
    assert receipt["malformed_timestamp_records"] == 2
    assert receipt["source_files"] == ["SPX/2026-09-11.ndjson", "SPX/2026-09-12.ndjson"]
    assert receipt["provenance_counts"] == {
        "diagnostic_invalid_snapshot": 1,
        "historical_unverified_subscription_identity": 1,
        "research_fallback_provenance": 1,
        "research_recorded_subscription_identity": 2,
    }
    assert receipt["research_only"] is True
    assert receipt["decision_grade"] is False
    assert receipt["current_live_eligible_count"] == 0
    assert observed_audit["research_only"] is True
    assert observed_audit["decision_grade"] is False
    manifest = receipt["input_file_hashes"]
    assert [entry["path"] for entry in manifest] == [
        f"SPX/2026-09-{day}.ndjson" for day in (10, 11, 12, 13)
    ]
    assert [entry["exists"] for entry in manifest] == [False, True, True, False]
    for entry in manifest:
        if entry["exists"]:
            data = (Path(sample["exports_root"]) / entry["path"]).read_bytes()
            assert entry["size_bytes"] == len(data)
            assert entry["sha256"] == hashlib.sha256(data).hexdigest()


def test_receipt_identity_includes_stable_tie_order_and_record_contents(sample, observed_audit):
    selection = history.load_snapshots_for_local_day(
        sample["exports_root"], sample["symbol"], sample["local_date"],
        resolve_display_timezone(sample["timezone"]),
    )
    assert [row["id"] for row in selection.records] == [
        "legacy", "equal-research", "equal-fallback", "invalid", "final-research",
    ]
    manifest = gate.input_manifest(sample)
    receipt = gate.selection_receipt(selection, sample, manifest)
    assert receipt == observed_audit["runs"][0]["receipt"]
    records = list(deepcopy(selection.records))
    records[1], records[2] = records[2], records[1]
    reordered = history.SnapshotSelection(tuple(records), selection.source_files, 2)
    reordered_receipt = gate.selection_receipt(reordered, sample, manifest)
    assert reordered_receipt["row_count"] == receipt["row_count"]
    assert reordered_receipt["selected_timestamp_utc"] == receipt["selected_timestamp_utc"]
    assert reordered_receipt["snapshot_id"] != receipt["snapshot_id"]
    records = list(deepcopy(selection.records))
    records[0]["id"] = "LEGACY"
    changed = history.SnapshotSelection(tuple(records), selection.source_files, 2)
    assert gate.selection_receipt(changed, sample, manifest)["snapshot_id"] != receipt["snapshot_id"]


def test_equal_size_and_mtime_input_change_fails_real_audit(tmp_path, monkeypatch):
    config = _write_sample(tmp_path / "exports")
    path = tmp_path / "exports" / "SPX" / "2026-09-11.ndjson"
    original = path.read_bytes()
    stat = path.stat()
    before = gate.input_manifest(config)
    real_run = gate.subprocess.run
    mutated = False

    def run_then_mutate(*args, **kwargs):
        nonlocal mutated
        process = real_run(*args, **kwargs)
        if not mutated:
            assert process.returncode == 0, process.stderr
            replacement = original.replace(b'"legacy"', b'"LEGACY"')
            assert replacement != original and len(replacement) == len(original)
            path.write_bytes(replacement)
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))
            mutated = True
        return process

    monkeypatch.setattr(gate.subprocess, "run", run_then_mutate)
    result = gate.audit(config)
    assert path.stat().st_size == stat.st_size
    assert path.stat().st_mtime_ns == stat.st_mtime_ns
    assert gate.input_manifest(config) != before
    assert result["status"] == "FAIL"
    assert "INPUT_FILES_CHANGED" in result["failure_reasons"]
    assert all(run["inputs_unchanged"] for run in result["runs"][:4])
    assert result["runs"][-1]["inputs_unchanged"] is False


def test_empty_input_cannot_pass_by_repeating_the_same_empty_selection(tmp_path):
    result = gate.audit({
        "exports_root": str(tmp_path), "symbol": "SPX",
        "local_date": "2026-09-11", "timezone": "America/Chicago",
    })
    assert result["status"] == "FAIL"
    assert "EMPTY_SELECTION" in result["failure_reasons"]


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("snapshot_id", "different-selection", "SELECTION_CHANGED"),
    ("research_only", False, "RESEARCH_BOUNDARY_VIOLATION"),
    ("decision_grade", True, "RESEARCH_BOUNDARY_VIOLATION"),
    ("current_live_eligible_count", 1, "RESEARCH_BOUNDARY_VIOLATION"),
])
def test_selection_and_authority_fail_closed(observed_audit, field, value, reason):
    runs = deepcopy(observed_audit["runs"])
    runs[-1]["receipt"][field] = value
    assert reason in gate.evaluate_runs(runs)


@pytest.mark.parametrize(("field", "value", "reason"), [
    ("primitive_payload", False, "UNSAFE_CACHE_PAYLOAD_OR_STALE_CLASS"),
    ("current_class", False, "UNSAFE_CACHE_PAYLOAD_OR_STALE_CLASS"),
    ("disk_loads", 2, "WARM_CACHE_HITS_NOT_PROVEN"),
    ("inputs_unchanged", False, "INPUT_FILES_CHANGED"),
    ("errors", ["UnserializableReturnValueError"], "STREAMLIT_ERROR"),
])
def test_execution_evidence_fail_closed(observed_audit, field, value, reason):
    runs = deepcopy(observed_audit["runs"])
    runs[2][field] = value
    assert reason in gate.evaluate_runs(runs)


@pytest.mark.parametrize("cold_pid", [None, "missing", "same-process"])
def test_cold_restart_requires_recorded_distinct_process(observed_audit, cold_pid):
    runs = deepcopy(observed_audit["runs"])
    if cold_pid == "missing":
        runs[-1].pop("pid")
    else:
        runs[-1]["pid"] = runs[0]["pid"] if cold_pid == "same-process" else None
    assert "COLD_PROCESS_NOT_PROVEN" in gate.evaluate_runs(runs)


def test_missing_required_rerun_is_not_accepted(observed_audit):
    runs = deepcopy(observed_audit["runs"])
    del runs[2]
    assert "MISSING_REQUIRED_RUNS" in gate.evaluate_runs(runs)


def test_real_streamlit_rejects_reintroduced_stale_dataclass_cache(tmp_path, sample):
    # Reintroduce the old return type in a temporary copy of the real helpers.
    # Reload while the original selection is still referenced reproduces the
    # source-reload class-identity mismatch without touching the live app.
    tree = ast.parse(gate.helper_source(gate.PROJECT_ROOT / "app.py"))
    cached, wrapper = tree.body
    assert isinstance(cached.body[-1], ast.Return)
    cached.body[-1:] = ast.parse("importlib.reload(history)\nreturn selection").body
    wrapper.body = ast.parse(
        "return _cached_local_snapshot_selection_payload("
        "exports_root, symbol, local_date, timezone_name)"
    ).body
    copied_app = tmp_path / "regressed_app.py"
    copied_app.write_text(ast.unparse(ast.fix_missing_locations(tree)), encoding="utf-8")
    config = dict(sample, app_path=str(copied_app), manifest=gate.input_manifest(sample))
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    result_path = tmp_path / "regression.json"
    process = gate.subprocess.run(
        [sys.executable, "-B", str(Path(gate.__file__).resolve()),
         "--worker", "warm", "--config", str(config_path), "--output", str(result_path)],
        cwd=gate.PROJECT_ROOT, capture_output=True, text=True, timeout=90,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
    )
    assert process.returncode == 0, process.stderr
    runs = json.loads(result_path.read_text(encoding="utf-8"))
    errors = "\n".join(error for run in runs for error in run["errors"])
    assert "serialize" in errors.lower() and "SnapshotSelection" in errors
    assert "STREAMLIT_ERROR" in gate.evaluate_runs(runs)
    assert "MISSING_SELECTION_EVIDENCE" in gate.evaluate_runs(runs)
