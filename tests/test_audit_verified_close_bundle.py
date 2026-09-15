import hashlib
from datetime import date
from pathlib import Path

from tools.audit_verified_close_bundle import audit_close_bundle_csv


HEADER = (
    "symbol,trading_date,official_close,source,source_reference,"
    "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
)


def _candidate(
    *,
    symbol: str,
    trading_date: str,
    official_close: float,
    source: str,
    source_reference: str,
    artifact: Path,
) -> str:
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    return (
        f"{symbol},{trading_date},{official_close},{source},{source_reference},"
        f"{digest},{artifact.name},2026-08-29T06:15:00Z\n"
    )


def test_audit_reports_valid_and_missing_families_without_ingestion(tmp_path: Path):
    vix = tmp_path / "VIX_History.csv"
    vix.write_text(
        "DATE,OPEN,HIGH,LOW,CLOSE\n08/28/2026,14.10,14.60,14.00,14.43\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "partial.csv"
    manifest.write_text(
        HEADER
        + _candidate(
            symbol="VIX",
            trading_date="2026-08-28",
            official_close=14.43,
            source="cboe-official",
            source_reference="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
            artifact=vix,
        ),
        encoding="utf-8",
    )

    report = audit_close_bundle_csv(manifest, trading_date=date(2026, 8, 28))

    assert report["bundle_status"] == "INCOMPLETE"
    assert report["ready_for_ingestion"] is False
    assert report["valid_symbols"] == ["VIX"]
    assert report["missing_symbols"] == ["NDX", "RUT", "SPX", "SPY"]
    assert report["families"]["VIX"]["status"] == "SEMANTICALLY_VALID"


def test_audit_distinguishes_stale_publication_from_missing_family(tmp_path: Path):
    vix = tmp_path / "VIX_History.csv"
    vix.write_text(
        "DATE,OPEN,HIGH,LOW,CLOSE\n08/27/2026,15.10,15.60,15.00,15.43\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "stale.csv"
    manifest.write_text(
        HEADER
        + _candidate(
            symbol="VIX",
            trading_date="2026-08-28",
            official_close=15.43,
            source="cboe-official",
            source_reference="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
            artifact=vix,
        ),
        encoding="utf-8",
    )

    report = audit_close_bundle_csv(manifest, trading_date=date(2026, 8, 28))

    assert report["bundle_status"] == "INCOMPLETE"
    assert report["stale_symbols"] == ["VIX"]
    assert "VIX" not in report["missing_symbols"]
    assert report["families"]["VIX"]["status"] == "STALE"


def test_audit_marks_same_date_wrong_close_invalid_not_stale(tmp_path: Path):
    vix = tmp_path / "VIX_History.csv"
    vix.write_text(
        "DATE,OPEN,HIGH,LOW,CLOSE\n08/28/2026,14.10,14.60,14.00,14.43\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "wrong-close.csv"
    manifest.write_text(
        HEADER
        + _candidate(
            symbol="VIX",
            trading_date="2026-08-28",
            official_close=99.99,
            source="cboe-official",
            source_reference="https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv",
            artifact=vix,
        ),
        encoding="utf-8",
    )

    report = audit_close_bundle_csv(manifest, trading_date=date(2026, 8, 28))

    assert report["bundle_status"] == "INVALID"
    assert report["invalid_symbols"] == ["VIX"]
    assert report["stale_symbols"] == []
    assert report["families"]["VIX"]["status"] == "INVALID"


def test_audit_marks_semantically_valid_core_bundle_ready(tmp_path: Path):
    spx = tmp_path / "spx.txt"
    spx.write_text("S&P 500 official close 2026-08-28 6500.25", encoding="utf-8")
    ndx = tmp_path / "ndx.json"
    ndx.write_text(
        '{"data":{"rows":[{"symbol":"NDX","date":"2026-08-28","close":"29433.43"}]}}',
        encoding="utf-8",
    )
    manifest = tmp_path / "core.csv"
    manifest.write_text(
        HEADER
        + _candidate(
            symbol="SPX",
            trading_date="2026-08-28",
            official_close=6500.25,
            source="sp-global-official",
            source_reference="https://www.spglobal.com/spdji/official-close",
            artifact=spx,
        )
        + _candidate(
            symbol="NDX",
            trading_date="2026-08-28",
            official_close=29433.43,
            source="nasdaq-official",
            source_reference="https://www.nasdaq.com/market-activity/index/ndx",
            artifact=ndx,
        ),
        encoding="utf-8",
    )

    report = audit_close_bundle_csv(
        manifest,
        trading_date=date(2026, 8, 28),
        bundle_profile="core",
    )

    assert report["bundle_status"] == "READY"
    assert report["ready_for_ingestion"] is True
    assert report["valid_symbols"] == ["NDX", "SPX"]
    assert report["missing_symbols"] == []
