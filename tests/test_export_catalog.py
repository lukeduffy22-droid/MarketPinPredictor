import json

from app.utils.export_catalog import (
    is_gamma_snapshot_file,
    list_gamma_snapshot_symbols,
)


def _write_ndjson(path, records):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def test_catalog_only_returns_gamma_snapshot_folders(tmp_path):
    snapshot = {
        "snapshot_version": "databento-live-1.0",
        "symbol": "SPX",
        "validation_is_valid": True,
    }
    invalid_snapshot = {
        "gex_formula_version": "databento-gex-v2-call-minus-put",
        "symbol": "VIX",
        "validation_is_valid": False,
    }
    prediction = {
        "symbol": "SPX",
        "is_valid": True,
        "predicted_close": 7000.0,
    }

    _write_ndjson(tmp_path / "SPX" / "2026-08-25.ndjson", [snapshot])
    _write_ndjson(tmp_path / "VIX" / "2026-08-25.ndjson", [invalid_snapshot])
    _write_ndjson(tmp_path / "predictions" / "2026-08-25.ndjson", [prediction])
    (tmp_path / "dashboard_screenshots").mkdir()

    assert list_gamma_snapshot_symbols(tmp_path) == ["SPX", "VIX"]


def test_file_detection_skips_corrupt_prefix_before_snapshot(tmp_path):
    path = tmp_path / "NDX" / "2026-08-25.ndjson"
    path.parent.mkdir(parents=True)
    path.write_text(
        "not-json\n"
        + json.dumps(
            {
                "snapshot_version": "canonical-v2",
                "symbol": "NDX",
                "validation_is_valid": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )

    assert is_gamma_snapshot_file(path) is True


def test_file_detection_rejects_prediction_schema(tmp_path):
    path = tmp_path / "predictions" / "2026-08-25.ndjson"
    _write_ndjson(path, [{"symbol": "NDX", "is_valid": True}])

    assert is_gamma_snapshot_file(path) is False
