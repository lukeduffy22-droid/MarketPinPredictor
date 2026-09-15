import pandas as pd

from backend.closing_tape.surface import build_contract_surface_features
from backend.closing_tape.training import (
    audit_close_label_preflight,
    prepare_close_training_dataset,
)
from tests.test_closing_tape_surface import _rows


def _closes() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "family_root": "SPX", "trading_date": "2026-08-25",
                "actual_close": 101.0,
                "close_label_available_at_utc": "2026-08-25T21:01:00Z",
                "close_source_artifact_sha256": "c" * 64,
            }
        ]
    )


def test_close_label_preflight_requires_complete_family_bundles():
    partial = pd.DataFrame(
        [
            {"family_root": "SPX", "trading_date": "2026-08-25"},
            {"family_root": "NDX", "trading_date": "2026-08-25"},
            {"family_root": "SPX", "trading_date": "2026-08-26"},
        ]
    )

    report = audit_close_label_preflight(
        partial,
        minimum_sessions=1,
        expected_families=("SPX", "NDX", "VIX"),
    )

    assert not report.ready
    assert report.verified_rows == 3
    assert report.complete_bundle_sessions == 0
    assert report.family_coverage[0].verified_sessions == 2
    assert "complete verified close bundles 0 < 1" in report.reasons


def test_close_label_preflight_passes_only_at_requested_session_count():
    closes = pd.DataFrame(
        [
            {"family_root": family, "trading_date": trading_date}
            for trading_date in ("2026-08-25", "2026-08-26")
            for family in ("SPX", "NDX")
        ]
    )

    report = audit_close_label_preflight(
        closes,
        minimum_sessions=2,
        expected_families=("SPX", "NDX"),
    )

    assert report.ready
    assert report.complete_bundle_sessions == 2
    assert report.reasons == ()


def test_close_training_readiness_uses_exact_horizon_and_frozen_features():
    surface = build_contract_surface_features(_rows())

    labeled, report = prepare_close_training_dataset(
        surface,
        _closes(),
        expected_families=("SPX",),
        minimum_sessions=1,
        minimum_family_sessions=1,
    )

    assert report.ready
    assert report.decision_horizon_minutes_before_close == 15
    assert report.labeled_sessions == 1
    assert report.source_hashes == 1
    assert report.incumbent_rows == 1
    assert report.incumbent_sessions == 1
    assert len(labeled) == 1
    assert labeled.iloc[0]["target_price"] == 101.0
    assert all(item.finite_rows == 1 for item in report.feature_coverage)


def test_close_training_readiness_requires_timestamp_aligned_incumbent():
    rows = _rows().drop(columns=["predicted_close"])
    surface = build_contract_surface_features(rows)

    _labeled, report = prepare_close_training_dataset(
        surface, _closes(), expected_families=("SPX",),
        minimum_sessions=1, minimum_family_sessions=1,
    )

    assert not report.ready
    assert "timestamp-aligned incumbent predictions 0 < labeled rows 1" in report.reasons


def test_close_training_readiness_reports_missing_families_and_labels():
    surface = build_contract_surface_features(_rows())

    _labeled, report = prepare_close_training_dataset(
        surface,
        pd.DataFrame(
            columns=[
                "family_root", "trading_date", "actual_close",
                "close_label_available_at_utc",
            ]
        ),
        minimum_sessions=60,
        minimum_family_sessions=10,
    )

    assert not report.ready
    assert any("no verified official closes" in reason for reason in report.reasons)
    coverage = {item.family_root: item for item in report.family_coverage}
    assert "no exact-horizon surface rows" in coverage["NDX"].reasons
    assert "labeled sessions 0 < 10" in coverage["SPX"].reasons


def test_close_training_readiness_rejects_wrong_horizon_without_nearby_fallback():
    surface = build_contract_surface_features(_rows())

    labeled, report = prepare_close_training_dataset(
        surface,
        _closes(),
        minutes_before_close=14,
        expected_families=("SPX",),
        minimum_sessions=1,
        minimum_family_sessions=1,
    )

    assert labeled.empty
    assert not report.ready
    assert "no rows exist at the exact decision horizon" in report.reasons


def test_close_training_rejects_multiple_captures_from_one_trading_day():
    surface = build_contract_surface_features(_rows())
    retry = surface.copy()
    retry["session_id"] = "same-day-retry"
    retry["source_sha256"] = "b" * 64

    _labeled, report = prepare_close_training_dataset(
        pd.concat([surface, retry], ignore_index=True),
        _closes(), expected_families=("SPX",),
        minimum_sessions=1, minimum_family_sessions=1,
    )

    assert not report.ready
    assert report.labeled_sessions == 1
    assert any("multiple eligible captures share a trading date" in reason for reason in report.reasons)


def test_close_training_rejects_missing_or_invalid_label_artifact_hash():
    surface = build_contract_surface_features(_rows())
    missing = _closes().drop(columns=["close_source_artifact_sha256"])
    _labeled, missing_report = prepare_close_training_dataset(
        surface, missing, expected_families=("SPX",),
        minimum_sessions=1, minimum_family_sessions=1,
    )
    invalid = _closes()
    invalid.loc[0, "close_source_artifact_sha256"] = "invalid"
    _labeled, invalid_report = prepare_close_training_dataset(
        surface, invalid, expected_families=("SPX",),
        minimum_sessions=1, minimum_family_sessions=1,
    )

    assert not missing_report.ready
    assert "verified close artifact SHA-256 column is missing" in missing_report.reasons
    assert not invalid_report.ready
    assert "verified close artifact SHA-256 values are invalid" in invalid_report.reasons
