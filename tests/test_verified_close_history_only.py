"""History maintenance must not rescore forecasts or reconcile capture state."""
import hashlib
from datetime import date, datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import tools.ingest_verified_closes as importer


@pytest.fixture
def close_rows(tmp_path):
    artifacts = {
        "SPX": ('spx.html', '<h1>S&amp;P 500</h1>August 25, 2026 Close 6500.25',
                '6500.25', 'sp-global-official', 'https://www.spglobal.com/spdji/official-close'),
        "NDX": ('ndx.json', '{"symbol":"NDX","date":"2026-08-25","close":"24000.50"}',
                '24000.50', 'nasdaq-official', 'https://www.nasdaq.com/market-activity/index/ndx'),
    }
    rows = []
    for symbol, (name, content, close, source, reference) in artifacts.items():
        artifact = tmp_path / name
        artifact.write_text(content, encoding="utf-8")
        rows.append(importer.parse_verified_close_record({
            "symbol": symbol, "trading_date": "2026-08-25", "official_close": close,
            "source": source, "source_reference": reference,
            "source_artifact_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            "source_artifact_path": str(artifact), "observed_at_utc": "2026-08-25T21:01:00Z",
        }, source_path=tmp_path / "input.csv"))
    return rows


@pytest.fixture
def scratch_database(tmp_path, monkeypatch):
    database_path = tmp_path / "data" / "market_data.db"
    database_path.parent.mkdir()
    url = importer.canonical_database_url(tmp_path)
    monkeypatch.setenv("DATABASE_URL", url)
    import backend.database as database

    engine = create_engine(url)
    monkeypatch.setattr(database, "engine", engine)
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=engine))
    database.Base.metadata.create_all(engine)
    with database.SessionLocal() as session:
        session.add(database.PredictionSnapshot(
            symbol="SPX", timestamp_utc=datetime(2026, 8, 25, 19, 59),
            trading_date=date(2026, 8, 25), current_price=6500, predicted_close=6501,
            actual_close=6502, error_points=-1, scored_at_utc=datetime(2026, 8, 25, 21),
        ))
        session.commit()

    def forbidden(*_args, **_kwargs):
        pytest.fail("history-only mode invoked scoring or general database initialization")

    monkeypatch.setattr(database, "init_db", forbidden)
    monkeypatch.setattr(database, "score_prediction_snapshots", forbidden)
    monkeypatch.setattr(importer, "reconcile_governed_prediction_outcomes", forbidden)
    yield engine
    engine.dispose()


def _table_rows(engine, table):
    with engine.connect() as connection:
        return [tuple(row) for row in connection.execute(text(f"SELECT * FROM {table} ORDER BY id"))]


def test_history_only_import_preserves_scores_and_repeat_is_noop(tmp_path, close_rows, scratch_database):
    engine = scratch_database
    predictions = _table_rows(engine, "prediction_snapshots")
    first = importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core", history_only=True)
    ledger = _table_rows(engine, "eod_close_observations")
    closes = _table_rows(engine, "eod_closes")
    assert len(ledger) == len(closes) == 2
    artifacts = {Path(row["source_artifact_path"]): Path(row["source_artifact_path"]).stat().st_mtime_ns
                 for row in first}
    assert all(row["history_only"] and row["score"] == {"scored": 0, "reason": "history_only"}
               and row["governed_forecast_scoring"] == {"scored": 0, "reason": "history_only"}
               for row in first)
    second = importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core", history_only=True)
    assert second == first
    assert _table_rows(engine, "prediction_snapshots") == predictions
    assert _table_rows(engine, "eod_close_observations") == ledger
    assert _table_rows(engine, "eod_closes") == closes
    assert _table_rows(engine, "prediction_accuracy_observations") == []
    assert all(path.stat().st_mtime_ns == mtime for path, mtime in artifacts.items())
    for row, result in zip(close_rows, first, strict=True):
        assert hashlib.sha256(Path(result["source_artifact_path"]).read_bytes()).hexdigest() == row.source_artifact_sha256


@pytest.mark.parametrize("selection", [
    {"score_prediction_ids_by_symbol": {"SPX": [1]}},
    {"selection_evidence_sha256_by_symbol": {"SPX": "a" * 64}},
])
def test_history_only_rejects_scoring_before_any_write(tmp_path, close_rows, selection):
    with pytest.raises(ValueError, match="history-only import cannot include"):
        importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core",
                        history_only=True, **selection)
    assert not (tmp_path / "data").exists()


def test_history_only_requires_initialized_schema_before_staging(tmp_path, close_rows, scratch_database):
    with scratch_database.begin() as connection:
        connection.execute(text("DROP TABLE eod_close_observations"))
    with pytest.raises(RuntimeError, match="requires initialized close schema"):
        importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core", history_only=True)
    assert not (tmp_path / "data" / "verified_close_sources").exists()


def test_history_only_missing_database_does_not_create_it(tmp_path, close_rows, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", importer.canonical_database_url(tmp_path))
    with pytest.raises(RuntimeError, match="requires an existing initialized"):
        importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core", history_only=True)
    assert not (tmp_path / "data").exists()


def test_settlement_does_not_initialize_or_reconcile_capture_schema(
    tmp_path, close_rows, scratch_database, monkeypatch,
):
    # scratch_database forbids init_db and scoring without an explicit selection.
    monkeypatch.setattr(importer, "reconcile_governed_prediction_outcomes",
        lambda root, days, **kwargs: {day.isoformat(): {"scored": 0} for day in days})
    result = importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core",
                             initialize_schema=False)
    assert len(result) == 2
    assert all(row["history_only"] is False for row in result)


def test_settlement_rejects_incomplete_schema_without_repairing_it(
    tmp_path, close_rows, scratch_database,
):
    with scratch_database.begin() as connection:
        connection.execute(text("DROP TABLE prediction_passports"))
    with pytest.raises(RuntimeError, match="settlement requires initialized close schema"):
        importer.ingest(close_rows, project_root=tmp_path, bundle_profile="core",
                        initialize_schema=False)
    assert not (tmp_path / "data" / "verified_close_sources").exists()


@pytest.mark.parametrize("args", [
    ["--score-prediction", "SPX:1"], ["--selection-evidence", "SPX:" + "a" * 64],
])
def test_cli_rejects_history_only_scoring_before_reading_csv(args, monkeypatch):
    monkeypatch.setattr(importer, "read_verified_close_csv", lambda *_: pytest.fail("must reject before reading"))
    with pytest.raises(SystemExit) as error:
        importer.main(["nonexistent.csv", "--history-only", *args])
    assert error.value.code == 2


def test_cli_forwards_explicit_history_mode(monkeypatch):
    captured = {}
    monkeypatch.setattr(importer, "read_verified_close_csv", lambda *_: [])

    def ingest(_rows, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(importer, "ingest", ingest)
    assert importer.main(["candidate.csv", "--history-only"]) == 0
    assert captured["history_only"] is True
