import ast
import importlib
import pickle
from pathlib import Path
from typing import Any

import pytest

import app.utils.snapshot_history as snapshot_history


class _FakeStreamlit:
    @staticmethod
    def cache_data(**_kwargs):
        return lambda function: function


def _load_snapshot_cache_helpers(selection, selection_type):
    app_path = Path(__file__).resolve().parents[1] / "app.py"
    module = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    helper_names = {
        "_cached_local_snapshot_selection_payload",
        "_cached_local_snapshot_selection",
    }
    helpers = [
        node
        for node in module.body
        if isinstance(node, ast.FunctionDef) and node.name in helper_names
    ]
    assert {node.name for node in helpers} == helper_names

    namespace = {
        "Any": Any,
        "SnapshotSelection": selection_type,
        "load_snapshots_for_local_day": lambda *_args: selection,
        "resolve_display_timezone": lambda name: name,
        "st": _FakeStreamlit(),
    }
    exec(
        compile(ast.Module(body=helpers, type_ignores=[]), str(app_path), "exec"),
        namespace,
    )
    return namespace


def test_streamlit_snapshot_cache_serializes_reload_stable_payload():
    stale_selection = snapshot_history.SnapshotSelection(
        records=(
            {
                "id": "valid-post-close",
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
                "subscription_epoch_id": "a" * 64,
                "subscription_generation": 1,
            },
            {
                "id": "diagnostic-post-close",
                "validation_is_valid": False,
                "gamma_excluded_from_model": True,
            },
        ),
        source_files=("exports/SPX/2026-09-04.ndjson",),
        malformed_timestamp_records=1,
    )
    current_module = importlib.reload(snapshot_history)
    helpers = _load_snapshot_cache_helpers(
        stale_selection,
        current_module.SnapshotSelection,
    )

    with pytest.raises(pickle.PicklingError, match="not the same object"):
        pickle.dumps(stale_selection)

    payload = helpers["_cached_local_snapshot_selection_payload"](
        "exports",
        "SPX",
        "2026-09-04",
        "America/Chicago",
    )
    assert pickle.loads(pickle.dumps(payload)) == (
        stale_selection.records,
        stale_selection.source_files,
        stale_selection.malformed_timestamp_records,
    )

    rehydrated = helpers["_cached_local_snapshot_selection"](
        "exports",
        "SPX",
        "2026-09-04",
        "America/Chicago",
    )
    assert type(rehydrated) is current_module.SnapshotSelection
    assert rehydrated.records == stale_selection.records
    assert rehydrated.source_files == stale_selection.source_files
    assert rehydrated.malformed_timestamp_records == 1

    evidence = current_module.partition_snapshot_evidence(rehydrated.records)
    assert [record["id"] for record in evidence.usable_records] == [
        "valid-post-close"
    ]
    assert [record["id"] for record in evidence.diagnostic_records] == [
        "diagnostic-post-close"
    ]
