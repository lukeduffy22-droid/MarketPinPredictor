import pandas as pd
import pytest

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.parity import (
    PARITY_REFERENCE_METHOD,
    estimate_tcbbo_parity_reference_prices,
    persist_parity_reference_prices,
)


def _contracts(*, family: str = "RUT", pair_gap_seconds: float = 1.0) -> pd.DataFrame:
    rows = []
    minute = pd.Timestamp("2026-08-25T15:00:00Z")
    base_ns = int(pd.Timestamp("2026-08-25T15:00:30Z").value)
    for index, strike in enumerate((1980, 1990, 2000, 2010, 2020)):
        for kind, midpoint, offset in (
            ("C", 100.0 + 2000.0 - strike, 0.0),
            ("P", 100.0, pair_gap_seconds),
        ):
            root = "RUTW" if family == "RUT" else family
            rows.append(
                {
                    "trading_date": "2026-08-25", "session_id": "s1",
                    "family_root": family,
                    "raw_symbol": f"{root:<6}260825{kind}{strike * 1000:08d}",
                    "expiration": "2026-08-25", "option_type": kind,
                    "strike": float(strike), "minute_utc": minute,
                    "last_pretrade_midpoint": midpoint,
                    "last_nbbo_event_ns": base_ns + index * 10_000_000 + int(offset * 1e9),
                    "capture_integrity_verified": True, "source_sha256": "a" * 64,
                }
            )
    return pd.DataFrame(rows)


def test_parity_reference_is_timestamp_gated_and_explicitly_estimated():
    result = estimate_tcbbo_parity_reference_prices(_contracts())

    assert len(result) == 1
    row = result.iloc[0]
    assert row["family_root"] == "RUT"
    assert row["current_price"] == pytest.approx(2000.0)
    assert row["parity_pair_count"] == 5
    assert row["parity_dispersion_bps"] == pytest.approx(0.0)
    assert row["reference_price_method"] == PARITY_REFERENCE_METHOD
    assert bool(row["reference_price_is_estimate"])
    assert row["timestamp_utc"] < pd.Timestamp("2026-08-25T15:01:00Z")


def test_parity_reference_fails_closed_for_old_pairs_thin_pairs_and_vix():
    assert estimate_tcbbo_parity_reference_prices(
        _contracts(pair_gap_seconds=6.0), max_pair_age_seconds=5.0
    ).empty
    assert estimate_tcbbo_parity_reference_prices(
        _contracts().iloc[:-2], minimum_pairs=5
    ).empty
    assert estimate_tcbbo_parity_reference_prices(_contracts(family="VIX")).empty

    unverified = _contracts()
    unverified.loc[0, "capture_integrity_verified"] = False
    with pytest.raises(ValueError, match="verified capture integrity"):
        estimate_tcbbo_parity_reference_prices(unverified)


def test_parity_reference_persists_only_as_inferred_evidence(tmp_path):
    catalog = TapeCatalog(tmp_path / "catalog.sqlite")
    prices = estimate_tcbbo_parity_reference_prices(_contracts())

    persist_parity_reference_prices(
        catalog,
        prices,
        parameters={"minimum_pairs": 5, "max_pair_age_seconds": 5.0},
    )

    with catalog.connect(read_only=True) as connection:
        row = connection.execute("SELECT * FROM tape_inferred_reference_minute").fetchone()
        observed_columns = {
            item[1] for item in connection.execute("PRAGMA table_info(tape_observed_contract_minute)")
        }
    assert row["family_root"] == "RUT"
    assert row["estimated_price"] == pytest.approx(2000.0)
    assert row["pair_count"] == 5
    assert row["reference_method"].endswith("estimate-v1")
    assert "estimated_price" not in observed_columns
