import json
from datetime import date, datetime, timezone

import pytest

from tools import fetch_verified_close_bundle as tool


UTC = timezone.utc
TRADING_DAY = date(2026, 8, 25)


def test_extracts_structured_official_close_values(tmp_path, monkeypatch):
    spx = tmp_path / "spx.html"
    spx.write_text(
        '<div>S&amp;P 500<span class="indices-price-value">7,677.28</span>'
        '<span>As of Aug 25, 2026 05:37 PM EDT</span></div>',
        encoding="utf-8",
    )
    ndx = tmp_path / "ndx.json"
    ndx.write_text(
        json.dumps(
            {
                "data": {
                    "tradesTable": {
                        "rows": [{"date": "08/25/2026", "close": "29,209.23"}]
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    vix = tmp_path / "vix.csv"
    vix.write_text("DATE,OPEN,HIGH,LOW,CLOSE\n08/25/2026,15,16,14,15.45\n")
    spy = tmp_path / "spy.json"
    spy.write_text(
        json.dumps(
            {
                "quoteHistory": {
                    "symbol": "SPY",
                    "historyList": [{"date": "2026/08/25", "close": "765.91"}],
                }
            }
        ),
        encoding="utf-8",
    )
    rut = tmp_path / "rut.pdf"
    rut.write_bytes(b"test fixture")
    monkeypatch.setattr(
        tool,
        "_pdf_text",
        lambda _path: (
            "Russell indexes\nDaily Values\n"
            "Russell 2000 Index 3000 3020 2990 3010.125 10 0.3\n"
            "August 25 2026 Page 1 of 4"
        ),
    )

    assert tool.extract_official_close(spx, symbol="SPX", trading_day=TRADING_DAY) == 7677.28
    assert tool.extract_official_close(ndx, symbol="NDX", trading_day=TRADING_DAY) == 29209.23
    assert tool.extract_official_close(rut, symbol="RUT", trading_day=TRADING_DAY) == 3010.125
    assert tool.extract_official_close(vix, symbol="VIX", trading_day=TRADING_DAY) == 15.45
    assert tool.extract_official_close(spy, symbol="SPY", trading_day=TRADING_DAY) == 765.91


def test_extracts_exact_cboe_spx_and_rut_history_rows(tmp_path):
    spx = tmp_path / "SPX_History.csv"
    spx.write_text("DATE,SPX\n08/25/2026,7677.280000\n", encoding="utf-8")
    rut = tmp_path / "RUT_History.csv"
    rut.write_text("DATE,RUT\n08/25/2026,3010.125000\n", encoding="utf-8")

    assert tool.extract_official_close(
        spx, symbol="SPX", trading_day=TRADING_DAY
    ) == 7677.28
    assert tool.extract_official_close(
        rut, symbol="RUT", trading_day=TRADING_DAY
    ) == 3010.125


def test_cboe_history_extractor_rejects_reversed_columns(tmp_path):
    spx = tmp_path / "SPX_History.csv"
    spx.write_text("SPX,DATE\n7677.280000,08/25/2026\n", encoding="utf-8")

    with pytest.raises(ValueError, match="exactly DATE and SPX columns"):
        tool.extract_official_close(spx, symbol="SPX", trading_day=TRADING_DAY)


def test_bundle_fetch_is_blocked_before_publication_window(tmp_path, monkeypatch):
    called = False

    def forbidden(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("official endpoints must not be fetched before publication")

    monkeypatch.setattr(tool, "fetch_official_artifact", forbidden)
    with pytest.raises(RuntimeError, match="blocked before"):
        tool.fetch_verified_close_bundle(
            project_root=tmp_path,
            trading_day=TRADING_DAY,
            now_utc=datetime(2026, 8, 25, 19, 0, tzinfo=UTC),
        )
    assert not called
    assert not (tmp_path / "data" / "verified_close_sources").exists()


def test_bundle_fetch_preserves_existing_candidate_before_network(tmp_path, monkeypatch):
    candidate = (
        tmp_path / "data" / "verified_close_sources" / "2026-08-25"
        / "candidate_bundle.csv"
    )
    candidate.parent.mkdir(parents=True)
    candidate.write_text("preserve me\n", encoding="utf-8")
    called = False

    def forbidden(**_kwargs):
        nonlocal called
        called = True
        raise AssertionError("network fetch must not run before output conflict is resolved")

    monkeypatch.setattr(tool, "fetch_official_artifact", forbidden)
    with pytest.raises(FileExistsError, match="already exists"):
        tool.fetch_verified_close_bundle(
            project_root=tmp_path,
            trading_day=TRADING_DAY,
            now_utc=datetime(2026, 8, 25, 23, 1, tzinfo=UTC),
        )

    assert not called
    assert candidate.read_text(encoding="utf-8") == "preserve me\n"


def test_bundle_fetch_writes_audited_candidate_without_ingesting(tmp_path, monkeypatch):
    closes = {"SPX": 7677.28, "NDX": 29209.23, "RUT": 3010.125,
              "VIX": 15.45, "SPY": 765.91}

    def fetch(**kwargs):
        symbol = kwargs["symbol"]
        directory = (
            tmp_path / "data" / "verified_close_sources"
            / TRADING_DAY.isoformat() / symbol
        )
        directory.mkdir(parents=True, exist_ok=True)
        artifact = directory / f"{symbol.lower()}.txt"
        artifact.write_text(symbol, encoding="utf-8")
        return {
            "symbol": symbol,
            "trading_date": TRADING_DAY.isoformat(),
            "source": kwargs["source"],
            "source_reference": kwargs["source_reference"],
            "final_url": kwargs["source_reference"],
            "source_artifact_sha256": symbol.lower().ljust(64, "0"),
            "source_artifact_path": str(artifact),
            "artifact_bytes": artifact.stat().st_size,
            "content_type": "text/plain",
            "retrieved_at_utc": "2026-08-25T23:01:00+00:00",
        }

    monkeypatch.setattr(tool, "fetch_official_artifact", fetch)
    monkeypatch.setattr(
        tool,
        "extract_official_close",
        lambda _path, *, symbol, trading_day: closes[symbol],
    )
    monkeypatch.setattr(tool, "validate_official_artifact_semantics", lambda _row: None)
    monkeypatch.setattr(
        tool,
        "audit_close_bundle_csv",
        lambda *_args, **_kwargs: {
            "ready_for_ingestion": True,
            "bundle_status": "READY",
            "valid_symbols": sorted(closes),
        },
    )

    payload = tool.fetch_verified_close_bundle(
        project_root=tmp_path,
        trading_day=TRADING_DAY,
        now_utc=datetime(2026, 8, 25, 23, 1, tzinfo=UTC),
    )

    candidate = tmp_path / "data" / "verified_close_sources" / "2026-08-25" / "candidate_bundle.csv"
    assert candidate.is_file()
    text = candidate.read_text(encoding="utf-8")
    assert "SPX,2026-08-25,7677.28,cboe-official" in text
    assert "RUT,2026-08-25,3010.125,cboe-official" in text
    assert "SPY,2026-08-25,765.91,nyse-arca-official" in text
    assert payload["audit"]["bundle_status"] == "READY"
    assert payload["database_ingested"] is False
