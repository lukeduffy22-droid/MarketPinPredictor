import json

from build_full_gamma_dataset import build_dataset


def test_build_dataset_excludes_invalid_snapshots_by_default(tmp_path):
    symbol_dir = tmp_path / "SPX"
    symbol_dir.mkdir()
    snapshots = [
        {
            "symbol": "SPX",
            "timestamp_utc": "2026-08-21T13:30:00Z",
            "spot_last": 0,
            "primary_gamma_pin_strike": 0,
            "validation_is_valid": False,
        },
        {
            "symbol": "SPX",
            "timestamp_utc": "2026-08-21T13:31:00Z",
            "spot_last": 7000,
            "primary_gamma_pin_strike": 7005,
            "validation_is_valid": True,
            "gross_gex": 100,
            "net_gex": 20,
        },
    ]
    (symbol_dir / "2026-08-21.ndjson").write_text(
        "\n".join(json.dumps(snapshot) for snapshot in snapshots),
        encoding="utf-8",
    )

    result = build_dataset(str(tmp_path), date_filter="2026-08-21")

    assert len(result) == 1
    assert float(result.iloc[0]["spot"]) == 7000


def test_build_dataset_can_include_invalid_snapshots_for_diagnostics(tmp_path):
    symbol_dir = tmp_path / "SPX"
    symbol_dir.mkdir()
    snapshot = {
        "symbol": "SPX",
        "timestamp_utc": "2026-08-21T13:30:00Z",
        "spot_last": 0,
        "primary_gamma_pin_strike": 0,
        "validation_is_valid": False,
    }
    (symbol_dir / "2026-08-21.ndjson").write_text(json.dumps(snapshot), encoding="utf-8")

    result = build_dataset(str(tmp_path), date_filter="2026-08-21", include_invalid=True)

    assert len(result) == 1
    assert float(result.iloc[0]["spot"]) == 0
