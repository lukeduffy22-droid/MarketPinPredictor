import json

import pandas as pd
import pytest

from tools.compare_expiration_targets import evaluate, load_snapshots


def test_load_snapshots_filters_invalid_stale_and_duplicates(tmp_path):
    symbol_dir = tmp_path / "SPX"
    symbol_dir.mkdir()
    rows = [
        {"symbol": "SPX", "timestamp_utc": "2026-08-21T15:00:00Z", "spot_last": 100, "same_day_target": 101, "multi_expiration_target": 102, "validation_is_valid": True, "contracts_count": 10, "subscription_epoch_id": "a" * 64, "subscription_generation": 1, "quote_age_seconds": 1},
        {"symbol": "SPX", "timestamp_utc": "2026-08-21T15:00:00Z", "spot_last": 100, "same_day_target": 101, "multi_expiration_target": 102, "validation_is_valid": True, "contracts_count": 10, "subscription_epoch_id": "a" * 64, "subscription_generation": 1, "quote_age_seconds": 1},
        {"symbol": "SPX", "timestamp_utc": "2026-08-21T15:01:00Z", "spot_last": 0, "same_day_target": 101, "multi_expiration_target": 102, "validation_is_valid": False, "contracts_count": 0},
        {"symbol": "SPX", "timestamp_utc": "2026-08-21T15:02:00Z", "spot_last": 100, "same_day_target": 101, "multi_expiration_target": 102, "validation_is_valid": True, "contracts_count": 10, "quote_age_seconds": 60},
    ]
    (symbol_dir / "2026-08-21.ndjson").write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    result = load_snapshots(tmp_path)

    assert len(result) == 1


def test_load_snapshots_separates_process_epochs_and_rejects_legacy_rows(tmp_path):
    symbol_dir = tmp_path / "SPX"
    symbol_dir.mkdir()
    common = {
        "symbol": "SPX", "timestamp_utc": "2026-08-21T15:00:00Z",
        "spot_last": 100, "same_day_target": 101, "multi_expiration_target": 102,
        "validation_is_valid": True, "contracts_count": 10,
        "subscription_generation": 1, "quote_age_seconds": 1,
    }
    rows = [
        {**common, "subscription_epoch_id": "a" * 64},
        {**common, "subscription_epoch_id": "b" * 64},
        common,
        {**common, "subscription_epoch_id": "A" * 64},
    ]
    (symbol_dir / "2026-08-21.ndjson").write_text(
        "\n".join(json.dumps(row) for row in rows), encoding="utf-8"
    )

    result = load_snapshots(tmp_path)

    assert len(result) == 2
    assert set(result["subscription_epoch_id"]) == {"a" * 64, "b" * 64}


def test_load_snapshots_rejects_conflicting_values_within_epoch_identity(tmp_path):
    symbol_dir = tmp_path / "SPX"
    symbol_dir.mkdir()
    common = {
        "symbol": "SPX", "timestamp_utc": "2026-08-21T15:00:00Z",
        "spot_last": 100, "same_day_target": 101,
        "validation_is_valid": True, "contracts_count": 10,
        "subscription_epoch_id": "a" * 64, "subscription_generation": 1,
        "quote_age_seconds": 1,
    }
    rows = [
        {**common, "multi_expiration_target": 102},
        {**common, "multi_expiration_target": 103},
    ]
    (symbol_dir / "2026-08-21.ndjson").write_text(
        "\n".join(json.dumps(row) for row in rows), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="conflicting snapshots"):
        load_snapshots(tmp_path)


def test_evaluate_recommends_baseline_when_multi_is_not_better():
    joined = pd.DataFrame([
        {"symbol": "SPX", "spot": 100, "same_day_target": 101, "multi_expiration_target": 103, "actual_close": 102, "pin_dispersion": 2.0, "confidence": 80, "subscription_epoch_id": "a" * 64, "subscription_generation": 1},
        {"symbol": "SPX", "spot": 100, "same_day_target": 101, "multi_expiration_target": 104, "actual_close": 102, "pin_dispersion": 2.0, "confidence": 80, "subscription_epoch_id": "b" * 64, "subscription_generation": 1},
    ])

    report = evaluate(joined)

    assert report["symbols"]["SPX"]["same_day"]["mae"] == 1.0
    assert report["symbols"]["SPX"]["multi_expiration"]["mae"] == 1.5
    assert report["symbols"]["SPX"]["subscription_identities"] == [
        {"subscription_epoch_id": "a" * 64, "subscription_generation": 1},
        {"subscription_epoch_id": "b" * 64, "subscription_generation": 1},
    ]
    assert report["deployment"]["recommendation"] == "keep_0DTE_baseline"
