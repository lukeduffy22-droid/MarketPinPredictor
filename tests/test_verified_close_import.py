import hashlib
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.engine import make_url

import tools.ingest_verified_closes as ingest_module
from tools.ingest_verified_closes import (
    canonical_database_url,
    ingest,
    read_verified_close_csv,
    reconcile_governed_prediction_outcomes,
    stage_source_artifacts,
    validate_close_bundles,
)


def _artifact(tmp_path: Path) -> tuple[Path, str]:
    path = tmp_path / "official-source.csv"
    path.write_bytes(b"date,close\n2026-08-25,6500.25\n")
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _spx_artifact(tmp_path: Path, *, trading_date="August 25, 2026", close="6,500.25"):
    path = tmp_path / "spx-official.html"
    path.write_text(
        f"<html><body><h1>S&amp;P 500</h1><p>{trading_date}</p><p>Close {close}</p></body></html>",
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _ndx_artifact(tmp_path: Path, *, trading_date="2026-08-25", close="24000.50"):
    path = tmp_path / "ndx-official.json"
    path.write_text(
        '{"data":{"rows":[{"symbol":"NDX","date":"%s","close":"%s"}]}}'
        % (trading_date, close),
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _vix_artifact(tmp_path: Path, *, trading_date="08/25/2026", close="15.450000"):
    path = tmp_path / "VIX_History.csv"
    path.write_text(
        f"DATE,OPEN,HIGH,LOW,CLOSE\n{trading_date},15.710000,16.300000,15.130000,{close}\n",
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _cboe_index_artifact(
    tmp_path: Path,
    *,
    symbol: str,
    trading_date: str = "08/27/2026",
    close: str,
    columns: str | None = None,
):
    path = tmp_path / f"{symbol}_History.csv"
    header = columns or f"DATE,{symbol}"
    path.write_text(
        f"{header}\n{trading_date},{close}\n",
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def _spy_artifact(
    tmp_path: Path,
    *,
    trading_date="2026/08/25",
    close="765.91",
    symbol="SPY",
    exchange="ARCX",
):
    path = tmp_path / "spy-official.json"
    path.write_text(
        '{"quote":{"exchg":"%s","desc":"STATE STREET SPDR S&P 500 ETF"},'
        '"quoteHistory":{"symbol":"%s","historyList":'
        '[{"date":"%s","open":"766.16","high":"766.78",'
        '"low":"763.0501","close":"%s","volume":"27422263"}]}}'
        % (exchange, symbol, trading_date, close),
        encoding="utf-8",
    )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_verified_close_csv_validates_all_rows_before_ingestion(tmp_path: Path):
    artifact, digest = _spx_artifact(tmp_path)
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc,correction_of_id\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{digest},{artifact.name},2026-08-25T21:01:00Z,\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)

    assert len(rows) == 1
    assert rows[0].symbol == "SPX"
    assert rows[0].official_close == 6500.25
    assert rows[0].source_artifact_sha256 == digest
    assert rows[0].source_artifact_path == artifact.resolve()
    assert rows[0].observed_at_utc.tzinfo is not None


@pytest.mark.parametrize(
    ("observed_at_utc", "reason"),
    [
        ("2026-08-25T19:59:00Z", "official cash-session close"),
        ("2099-08-25T21:01:00Z", "future clock skew"),
    ],
)
def test_verified_close_csv_rejects_false_observation_chronology(
    tmp_path: Path, observed_at_utc: str, reason: str
):
    artifact, digest = _spx_artifact(tmp_path)
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,"
        "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,"
        f"https://www.spglobal.com/spdji/official-close,{digest},"
        f"{artifact.name},{observed_at_utc}\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=reason):
        read_verified_close_csv(path)


def test_core_bundle_accepts_exact_spx_and_ndx_session(tmp_path: Path):
    spx, spx_hash = _spx_artifact(tmp_path)
    ndx, ndx_hash = _ndx_artifact(tmp_path)
    path = tmp_path / "core-closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{spx_hash},{spx.name},2026-08-25T21:01:00Z\n"
        f"NDX,2026-08-25,24000.50,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{ndx_hash},{ndx.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)
    validate_close_bundles(rows, bundle_profile="core")
    with pytest.raises(ValueError, match="missing RUT,SPY,VIX"):
        validate_close_bundles(rows)


def test_spx_artifact_rejects_stale_trading_date(tmp_path: Path):
    artifact, digest = _spx_artifact(tmp_path, trading_date="August 24, 2026")
    path = tmp_path / "stale-spx.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{digest},{artifact.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requested trading date"):
        read_verified_close_csv(path)


def test_ndx_artifact_rejects_wrong_close(tmp_path: Path):
    artifact, digest = _ndx_artifact(tmp_path, close="23999.00")
    path = tmp_path / "wrong-ndx.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"NDX,2026-08-25,24000.50,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{digest},{artifact.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no matching symbol/date/close"):
        read_verified_close_csv(path)


def test_ndx_artifact_accepts_parent_symbol_and_child_trade_row(tmp_path: Path):
    artifact = tmp_path / "ndx-parent-child.json"
    official_payload = (
        '{"data":{"symbol":"NDX","totalRecords":1,"tradesTable":{"asOf":null,'
        '"headers":{"date":"Date","close":"Close/Last","volume":"Volume",'
        '"open":"Open","high":"High","low":"Low"},"rows":[{"date":"08/25/2026",'
        '"close":"29,209.23","volume":"--","open":"29,231.18","high":"29,338.77",'
        '"low":"29,077.72"}]}},"message":null,"status":{"rCode":200,"bCodeMessage":null,'
        '"developerMessage":null}}'
    )
    artifact.write_text(official_payload, encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    assert digest == "09abfacb60d65dc868951bfa6ae505f2b2fa16d73607f7b48772f6e46f58114c"
    path = tmp_path / "ndx-parent-child.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"NDX,2026-08-25,29209.23,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{digest},{artifact.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)

    assert rows[0].symbol == "NDX"
    assert rows[0].official_close == 29209.23


def test_vix_artifact_rejects_wrong_close(tmp_path: Path):
    artifact, digest = _vix_artifact(tmp_path, close="15.440000")
    path = tmp_path / "wrong-vix.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv,{digest},{artifact.name},2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no matching date/close"):
        read_verified_close_csv(path)


def test_rut_artifact_accepts_official_daily_values_row(tmp_path: Path, monkeypatch):
    artifact = tmp_path / "russell_real_time_value_daily.pdf"
    artifact.write_bytes(b"synthetic-pdf-bytes")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    monkeypatch.setattr(
        "tools.ingest_verified_closes._pdf_text",
        lambda _path: (
            "Russell indexes\nDaily Values\n"
            "Russell 2000\ufffd Index 3005.90 3019.40 3000.91 3014.339419 8.44 0.28\n"
            "August 27 2026 Page 1 of 4"
        ),
    )
    path = tmp_path / "rut.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"RUT,2026-08-27,3014.339419,ftse-russell-official,https://research.ftserussell.com/products/data/russell_real_time_value_daily.pdf,{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)

    assert rows[0].symbol == "RUT"
    assert rows[0].official_close == 3014.339419


def test_rut_artifact_does_not_accept_value_index_decoy(tmp_path: Path, monkeypatch):
    artifact = tmp_path / "russell_real_time_value_daily.pdf"
    artifact.write_bytes(b"synthetic-pdf-bytes")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    monkeypatch.setattr(
        "tools.ingest_verified_closes._pdf_text",
        lambda _path: (
            "Russell indexes\nDaily Values\n"
            "Russell 2000\ufffd Index 3005.90 3019.40 3000.91 3014.339419 8.44 0.28\n"
            "Russell 2000\ufffd Value Index 3351.43 3358.58 3335.74 3354.428245 3.00 0.09\n"
            "August 27 2026 Page 1 of 4"
        ),
    )
    path = tmp_path / "rut-decoy.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"RUT,2026-08-27,3354.428245,ftse-russell-official,https://research.ftserussell.com/products/data/russell_real_time_value_daily.pdf,{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no matching Russell 2000 date/close row"):
        read_verified_close_csv(path)


@pytest.mark.parametrize(
    ("symbol", "close"),
    (("SPX", "7730.990000"), ("RUT", "3014.339400")),
)
def test_cboe_index_history_accepts_exact_spx_and_rut_rows(
    tmp_path: Path, symbol: str, close: str
):
    artifact, digest = _cboe_index_artifact(
        tmp_path, symbol=symbol, close=close
    )
    path = tmp_path / f"{symbol.lower()}-cboe.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,"
        "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"{symbol},2026-08-27,{close},cboe-official,"
        f"https://cdn.cboe.com/api/global/us_indices/daily_prices/{symbol}_History.csv,"
        f"{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)

    assert rows[0].symbol == symbol
    assert rows[0].official_close == float(close)


def test_cboe_rut_history_rejects_performance_series_decoy(tmp_path: Path):
    artifact, digest = _cboe_index_artifact(
        tmp_path,
        symbol="RUT",
        close="7491.392688",
        columns="DATE,Value_Without_Dividends__USD_",
    )
    path = tmp_path / "rut-performance-decoy.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,"
        "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        "RUT,2026-08-27,7491.392688,cboe-official,"
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/RUT_History.csv,"
        f"{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exactly DATE and RUT columns"):
        read_verified_close_csv(path)


def test_cboe_index_history_rejects_reversed_columns(tmp_path: Path):
    artifact, digest = _cboe_index_artifact(
        tmp_path,
        symbol="SPX",
        close="7730.990000",
        columns="SPX,DATE",
    )
    path = tmp_path / "spx-reversed-columns.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,"
        "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        "SPX,2026-08-27,7730.99,cboe-official,"
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/SPX_History.csv,"
        f"{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exactly DATE and SPX columns"):
        read_verified_close_csv(path)


def test_cboe_spx_history_rejects_noncanonical_endpoint(tmp_path: Path):
    artifact, digest = _cboe_index_artifact(
        tmp_path, symbol="SPX", close="7730.990000"
    )
    path = tmp_path / "spx-wrong-endpoint.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,"
        "source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        "SPX,2026-08-27,7730.99,cboe-official,"
        "https://cdn.cboe.com/api/global/us_indices/daily_prices/RUT_History.csv,"
        f"{digest},{artifact.name},2026-08-28T12:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exact Cboe history CSV endpoint"):
        read_verified_close_csv(path)


def test_spy_artifact_accepts_exact_nyse_arca_history_record(tmp_path: Path):
    artifact, digest = _spy_artifact(tmp_path)
    path = tmp_path / "spy.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPY,2026-08-25,765.91,nyse-arca-official,https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY,{digest},{artifact.name},2026-08-25T22:00:00Z\n",
        encoding="utf-8",
    )

    rows = read_verified_close_csv(path)

    assert rows[0].symbol == "SPY"
    assert rows[0].official_close == 765.91


def test_spy_artifact_rejects_wrong_close(tmp_path: Path):
    artifact, digest = _spy_artifact(tmp_path, close="765.90")
    path = tmp_path / "spy-wrong-close.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPY,2026-08-25,765.91,nyse-arca-official,https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY,{digest},{artifact.name},2026-08-25T22:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no matching NYSE Arca date/close"):
        read_verified_close_csv(path)


def test_spy_artifact_rejects_non_arca_identity(tmp_path: Path):
    artifact, digest = _spy_artifact(tmp_path, exchange="XNYS")
    path = tmp_path / "spy-wrong-exchange.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPY,2026-08-25,765.91,nyse-arca-official,https://www.nyse.com/api/nyseservice/v1/quotes?symbol=SPY,{digest},{artifact.name},2026-08-25T22:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="NYSE Arca listing"):
        read_verified_close_csv(path)


def test_spy_artifact_rejects_generic_nyse_reference(tmp_path: Path):
    artifact, digest = _spy_artifact(tmp_path)
    path = tmp_path / "spy-generic-reference.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPY,2026-08-25,765.91,nyse-arca-official,https://www.nyse.com/market-data/reference,{digest},{artifact.name},2026-08-25T22:00:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="exact NYSE SPY quote JSON endpoint"):
        read_verified_close_csv(path)


def test_ingest_does_not_score_without_explicit_selection(tmp_path: Path, monkeypatch):
    spx, spx_hash = _spx_artifact(tmp_path)
    ndx, ndx_hash = _ndx_artifact(tmp_path)
    path = tmp_path / "core-closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{spx_hash},{spx.name},2026-08-25T21:01:00Z\n"
        f"NDX,2026-08-25,24000.50,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{ndx_hash},{ndx.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )
    import backend.database as database_module
    events = []
    database_url = canonical_database_url(tmp_path)
    bound_engine = SimpleNamespace(url=make_url(database_url))

    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setattr(database_module, "engine", bound_engine)
    monkeypatch.setattr(
        database_module,
        "SessionLocal",
        SimpleNamespace(kw={"bind": bound_engine}),
    )

    monkeypatch.setattr(database_module, "init_db", lambda: None)
    monkeypatch.setattr(
        database_module,
        "upsert_verified_eod_close_bundle",
        lambda payloads: (
            events.append("close_bundle_committed"),
            [SimpleNamespace(official_close=item["official_close"]) for item in payloads],
        )[1],
    )
    monkeypatch.setattr(
        database_module,
        "score_prediction_snapshots",
        lambda *args, **kwargs: pytest.fail("implicit scoring must not occur"),
    )
    monkeypatch.setattr(
        "tools.ingest_verified_closes.reconcile_governed_prediction_outcomes",
        lambda _root, days, *, market_db_path: (
            events.append("governance_reconciled"),
            {
                day.isoformat(): {
                    "scored": 0,
                    "reason": "no_governed_promoted_predictions_selected",
                }
                for day in days
            },
        )[1],
    )

    results = ingest(
        read_verified_close_csv(path),
        project_root=tmp_path,
        bundle_profile="core",
    )

    assert [item["scored"] for item in results] == [0, 0]
    assert all(item["scored"] == 0 for item in results)
    assert events == ["close_bundle_committed", "governance_reconciled"]
    assert all(
        item["governed_forecast_scoring"]["reason"]
        == "no_governed_promoted_predictions_selected"
        for item in results
    )


def test_governed_reconciliation_treats_absent_predictions_as_non_error(
    tmp_path: Path, monkeypatch
):
    import backend.closing_tape.governance as governance

    monkeypatch.setattr(
        governance,
        "score_promoted_prediction_outcomes",
        lambda *_args, **_kwargs: {
            "scored": 0,
            "reason": "no_governed_promoted_predictions_selected",
        },
    )

    reports = reconcile_governed_prediction_outcomes(
        tmp_path,
        {date(2026, 8, 25)},
    )

    assert reports == {
        "2026-08-25": {
            "scored": 0,
            "reason": "no_governed_promoted_predictions_selected",
        }
    }


def test_governed_reconciliation_fails_visibly_on_partial_scoring(
    tmp_path: Path, monkeypatch
):
    import backend.closing_tape.governance as governance

    monkeypatch.setattr(
        governance,
        "score_promoted_prediction_outcomes",
        lambda *_args, **_kwargs: {
            "scored": 1,
            "reason": "partial_verified_outcomes",
        },
    )

    with pytest.raises(RuntimeError, match="governed forecast scoring was incomplete"):
        reconcile_governed_prediction_outcomes(
            tmp_path,
            {date(2026, 8, 25)},
        )


def test_verified_close_csv_rejects_wrong_authority(tmp_path: Path):
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,nasdaq-official,https://www.nasdaq.com/official-close,{'a' * 64},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="not approved"):
        read_verified_close_csv(path)


def test_verified_close_import_targets_canonical_runtime_database(tmp_path: Path):
    assert canonical_database_url(tmp_path).endswith("/data/market_data.db")


def test_ingest_fails_before_writes_when_database_was_preimported_for_another_path(
    tmp_path: Path, monkeypatch
):
    spx, spx_hash = _spx_artifact(tmp_path)
    ndx, ndx_hash = _ndx_artifact(tmp_path)
    path = tmp_path / "core-closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{spx_hash},{spx.name},2026-08-25T21:01:00Z\n"
        f"NDX,2026-08-25,24000.50,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{ndx_hash},{ndx.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )
    rows = read_verified_close_csv(path)
    database_a = tmp_path / "database-a.db"
    database_b = tmp_path / "database-b.db"
    engine_a = SimpleNamespace(
        url=make_url(f"sqlite:///{database_a.as_posix()}")
    )
    import backend.database as database_module

    monkeypatch.setattr(database_module, "engine", engine_a)
    monkeypatch.setattr(
        database_module,
        "SessionLocal",
        SimpleNamespace(kw={"bind": engine_a}),
    )
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{database_b.as_posix()}")
    monkeypatch.setattr(
        "tools.ingest_verified_closes.stage_source_artifacts",
        lambda *_args, **_kwargs: pytest.fail(
            "database mismatch must fail before artifact staging"
        ),
    )

    with pytest.raises(RuntimeError, match="database target mismatch"):
        ingest(rows, project_root=tmp_path, bundle_profile="core")

    assert not database_a.exists()
    assert not database_b.exists()


def test_verified_close_bundle_refuses_partial_session(tmp_path: Path):
    artifact, digest = _vix_artifact(tmp_path)
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/vix-history.csv,{digest},{artifact.name},2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing NDX,RUT,SPX,SPY"):
        validate_close_bundles(read_verified_close_csv(path))


def test_partial_correction_requires_explicit_lineage(tmp_path: Path):
    artifact, digest = _vix_artifact(tmp_path)
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc,correction_of_id\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/vix-history.csv,{digest},{artifact.name},2026-08-25T21:30:00Z,\n",
        encoding="utf-8",
    )
    rows = read_verified_close_csv(path)

    with pytest.raises(ValueError, match="explicit correction_of_id"):
        validate_close_bundles(rows, allow_partial_correction=True)


def test_verified_close_csv_rejects_reference_on_wrong_domain(tmp_path: Path):
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,observed_at_utc\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://example.com/vix.csv,{'a' * 64},2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="official cboe.com domain"):
        read_verified_close_csv(path)


def test_verified_close_csv_rejects_missing_artifact_hash(tmp_path: Path):
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,observed_at_utc\n"
        "VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/vix.csv,,2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="64-character hexadecimal"):
        read_verified_close_csv(path)


def test_verified_close_csv_rejects_artifact_hash_mismatch(tmp_path: Path):
    artifact, _digest = _artifact(tmp_path)
    path = tmp_path / "closes.csv"
    path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/vix.csv,{'a' * 64},{artifact.name},2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="bytes do not match"):
        read_verified_close_csv(path)


def test_source_artifact_vault_is_content_addressed_and_idempotent(tmp_path: Path):
    artifact, digest = _vix_artifact(tmp_path)
    csv_path = tmp_path / "closes.csv"
    csv_path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"VIX,2026-08-25,15.45,cboe-official,https://cdn.cboe.com/vix.csv,{digest},{artifact.name},2026-08-25T21:30:00Z\n",
        encoding="utf-8",
    )
    rows = read_verified_close_csv(csv_path)

    first = stage_source_artifacts(rows, project_root=tmp_path)
    second = stage_source_artifacts(rows, project_root=tmp_path)
    destination = first[(rows[0].trading_date, "VIX", digest)]

    assert first == second
    assert destination.read_bytes() == artifact.read_bytes()
    assert destination.name == f"{digest}.csv"


def test_ingest_revalidates_the_hash_bound_staged_artifact_before_close_commit(
    tmp_path: Path, monkeypatch
):
    valid_spx, _valid_spx_hash = _spx_artifact(tmp_path)
    valid_spx_bytes = valid_spx.read_bytes()
    invalid_spx = tmp_path / "spx-invalid.html"
    invalid_spx_bytes = b"unrelated rendered page without an official close"
    invalid_spx.write_bytes(invalid_spx_bytes)
    invalid_spx_hash = hashlib.sha256(invalid_spx_bytes).hexdigest()
    ndx, ndx_hash = _ndx_artifact(tmp_path)
    csv_path = tmp_path / "swapped-core-closes.csv"
    csv_path.write_text(
        "symbol,trading_date,official_close,source,source_reference,source_artifact_sha256,source_artifact_path,observed_at_utc\n"
        f"SPX,2026-08-25,6500.25,sp-global-official,https://www.spglobal.com/spdji/official-close,{invalid_spx_hash},{invalid_spx.name},2026-08-25T21:01:00Z\n"
        f"NDX,2026-08-25,24000.50,nasdaq-official,https://www.nasdaq.com/market-activity/index/ndx,{ndx_hash},{ndx.name},2026-08-25T21:01:00Z\n",
        encoding="utf-8",
    )
    real_validate = ingest_module.validate_official_artifact_semantics
    swapped_once = False

    def validate_with_coordinated_swap(row):
        nonlocal swapped_once
        if row.symbol == "SPX" and row.source_artifact_path == invalid_spx and not swapped_once:
            swapped_once = True
            invalid_spx.write_bytes(valid_spx_bytes)
            try:
                return real_validate(row)
            finally:
                invalid_spx.write_bytes(invalid_spx_bytes)
        return real_validate(row)

    monkeypatch.setattr(
        ingest_module,
        "validate_official_artifact_semantics",
        validate_with_coordinated_swap,
    )
    rows = read_verified_close_csv(csv_path)
    monkeypatch.setattr(
        ingest_module,
        "_require_configured_database_bindings",
        lambda _path: SimpleNamespace(
            init_db=lambda: None,
            score_prediction_snapshots=lambda *_args, **_kwargs: pytest.fail(
                "score writes must not begin"
            ),
            upsert_verified_eod_close_bundle=lambda *_args, **_kwargs: pytest.fail(
                "close commit must not begin"
            ),
        ),
    )

    with pytest.raises(ValueError, match="SPX official artifact"):
        ingest(rows, project_root=tmp_path / "project", bundle_profile="core")

    assert swapped_once
