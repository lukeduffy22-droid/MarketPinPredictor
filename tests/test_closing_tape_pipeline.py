import pandas as pd

from backend.closing_tape import pipeline


def _contract_rows() -> pd.DataFrame:
    common = {
        "trading_date": "2026-08-25", "session_id": "s1", "feed_name": "opra_options",
        "minute_utc": pd.Timestamp("2026-08-25T15:00:00Z"),
        "cash_open_utc": pd.Timestamp("2026-08-25T13:30:00Z"),
        "cash_close_utc": pd.Timestamp("2026-08-25T20:00:00Z"),
        "expiration": pd.Timestamp("2026-08-25"), "option_type": "C", "strike": 100.0,
        "trade_count": 1, "volume": 10.0, "notional": 1000.0,
        "nbbo_valid_count": 1, "quoted_spread_bps_sum": 100.0,
        "data_quality_flagged_count": 0, "at_ask_count": 1, "at_bid_count": 0,
        "at_ask_notional": 1000.0, "at_bid_notional": 0.0, "open_interest": 50.0,
        "open_interest_available_at_utc": pd.Timestamp("2026-08-25T14:59:30Z"),
        "capture_integrity_verified": True,
        "inference_method": "trade_price_vs_pretrade_nbbo", "inference_version": "1.0",
        "source_sha256": "a" * 64, "last_pretrade_midpoint": 5.0,
        "last_nbbo_event_ns": int(pd.Timestamp("2026-08-25T15:00:50Z").value),
    }
    return pd.DataFrame(
        [
            {**common, "family_root": "SPX", "raw_symbol": "SPX   260825C00100000"},
            {**common, "family_root": "RUT", "raw_symbol": "RUTW  260825C00100000"},
        ]
    )


def _price(family: str, value: float, method: str) -> dict[str, object]:
    return {
        "family_root": family, "trading_date": "2026-08-25",
        "timestamp_utc": pd.Timestamp("2026-08-25T15:00:55Z"),
        "quote_timestamp_utc": pd.Timestamp("2026-08-25T15:00:55Z"),
        "current_price": value, "provider": "databento",
        "reference_price_method": method, "reference_price_is_estimate": True,
        "reference_price_provider": "databento",
        "reference_subscription_epoch_id": "a" * 64,
        "reference_subscription_generation": 1,
        "reference_price_epoch_eligible": True,
        "reference_price_epoch_status": "eligible_current_epoch",
    }


def test_research_pipeline_reports_primary_fallback_and_exclusions(monkeypatch, tmp_path):
    monkeypatch.setattr(pipeline, "load_complete_contract_tape_features", lambda _paths: _contract_rows())
    monkeypatch.setattr(
        pipeline, "load_marketpin_reference_prices",
        lambda _path: pd.DataFrame([_price("SPX", 100.0, "primary-parity")]),
    )
    fallback = _price("RUT", 100.0, "tcbbo-parity")
    fallback.pop("reference_subscription_epoch_id")
    fallback.pop("reference_subscription_generation")
    fallback.pop("reference_price_epoch_eligible")
    fallback.pop("reference_price_epoch_status")
    fallback.update(session_id="s1", minute_utc=pd.Timestamp("2026-08-25T15:00:00Z"), source_sha256="a" * 64)
    monkeypatch.setattr(
        pipeline, "estimate_tcbbo_parity_reference_prices",
        lambda _contracts: pd.DataFrame([fallback]),
    )

    surface, report = pipeline.build_research_surface_dataset(
        [tmp_path / "one.sqlite"], tmp_path / "market.sqlite"
    )

    assert len(surface) == 2
    assert report.surface_rows == 2
    assert report.sessions == 1
    assert len(report.feature_schema_hash or "") == 64
    assert len(report.model_feature_contract_hash) == 64
    assert "observed_nbbo_coverage_ratio" in report.model_feature_columns
    assert "inferred_at_ask_count_share" in report.model_feature_columns
    coverage = {row.family_root: row for row in report.family_coverage}
    assert coverage["SPX"].primary_matches == 1
    assert coverage["SPX"].fallback_matches == 0
    assert coverage["RUT"].fallback_matches == 1
    assert coverage["VIX"].exclusion_reasons == ("no eligible finalized contract rows",)


def test_research_pipeline_fails_closed_with_no_eligible_catalogs(monkeypatch, tmp_path):
    monkeypatch.setattr(
        pipeline, "load_complete_contract_tape_features", lambda _paths: pd.DataFrame()
    )

    surface, report = pipeline.build_research_surface_dataset(
        [tmp_path / "missing.sqlite"], tmp_path / "market.sqlite"
    )

    assert surface.empty
    assert report.surface_rows == 0
    assert all(row.exclusion_reasons for row in report.family_coverage)
