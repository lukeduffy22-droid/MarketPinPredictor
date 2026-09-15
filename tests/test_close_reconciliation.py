import hashlib
import json
import sqlite3

import pytest

from backend.closing_tape.close_reconciliation import (
    TABLE, load_verified_close_parent_overrides, reconcile_duplicate_close_roots,
)
from backend.closing_tape.status import _verified_close_evidence


@pytest.fixture
def ledger(tmp_path):
    path = tmp_path / "market.db"
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("CREATE TABLE eod_close_observations (id INTEGER PRIMARY KEY, "
        "symbol TEXT, trading_date TEXT, official_close REAL, source TEXT, source_reference TEXT, "
        "source_verified INTEGER, observed_at_utc TEXT, correction_of_id INTEGER, source_artifact_sha256 TEXT)")
    for ident in (1, 2):
        raw = f"official close evidence {ident}".encode()
        digest = hashlib.sha256(raw).hexdigest()
        artifact = tmp_path / "data" / "verified_close_sources" / "2026-08-25" / "SPX" / f"{digest}.txt"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_bytes(raw)
        connection.execute("INSERT INTO eod_close_observations VALUES (?,?,?,?,?,?,?,?,?,?)",
            (ident, "SPX", "2026-08-25", 6500.0, "cboe-official", "https://cdn.cboe.com/close.csv",
             1, f"2026-08-25T21:0{ident}:00+00:00", None, digest))
    connection.commit()
    yield connection, path, tmp_path
    connection.close()


def repair(connection, root, pairs=((2, 1),)):
    return reconcile_duplicate_close_roots(connection, pairs, project_root=root,
        reason="Verified equal-price corroboration; preserve both original observations")


def test_explicit_receipt_repairs_status_without_modifying_originals(ledger):
    connection, path, root = ledger
    before = [dict(r) for r in connection.execute("SELECT * FROM eod_close_observations ORDER BY id")]
    assert _verified_close_evidence(path, project_root=root)["available"] is False
    assert repair(connection, root) == 1
    connection.commit()
    assert load_verified_close_parent_overrides(connection) == {2: 1}
    assert [dict(r) for r in connection.execute("SELECT * FROM eod_close_observations ORDER BY id")] == before
    status = _verified_close_evidence(path, project_root=root)
    assert status["available"] is True
    assert status["rows"] == 1
    assert status["ledger_rows"] == 2
    assert status["lineage_reconciliations"] == 1
    assert repair(connection, root) == 0


@pytest.mark.parametrize("column,value", [
    ("official_close", 6501), ("symbol", "NDX"), ("trading_date", "2026-08-26"),
    ("source_verified", 0), ("source_reference", "https://example.com/fake"),
    ("correction_of_id", 1), ("observed_at_utc", "2026-08-25T21:00:00+00:00"),
])
def test_conflicts_and_invalid_evidence_cannot_be_reconciled(ledger, column, value):
    connection, _, root = ledger
    connection.execute(f"UPDATE eod_close_observations SET {column}=? WHERE id=2", (value,))
    with pytest.raises(ValueError):
        repair(connection, root)
    assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone()


def test_missing_or_tampered_artifact_prevents_repair(ledger):
    connection, _, root = ledger
    artifact = next((root / "data" / "verified_close_sources" / "2026-08-25" / "SPX").glob("*.txt"))
    artifact.write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA-256"):
        repair(connection, root)


@pytest.mark.parametrize("pair", [(2.0, 1), (2, True), (2, "1"), (-2, 1), (2, 0)])
def test_reconciliation_rejects_non_integer_or_nonpositive_ids(ledger, pair):
    connection, _, root = ledger
    with pytest.raises(ValueError, match="positive integers"):
        repair(connection, root, pairs=(pair,))
    assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name=?", (TABLE,)).fetchone()


@pytest.mark.parametrize("sql", [
    f"DELETE FROM {TABLE}", f"UPDATE {TABLE} SET parent_observation_id=9",
    f"INSERT OR REPLACE INTO {TABLE} SELECT * FROM {TABLE}",
])
def test_receipts_are_append_only(ledger, sql):
    connection, _, root = ledger
    repair(connection, root)
    connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        connection.execute(sql)


def test_receipt_does_not_authorize_mutated_original(ledger):
    connection, _, root = ledger
    repair(connection, root)
    connection.execute("UPDATE eod_close_observations SET official_close=6501 WHERE id=2")
    with pytest.raises(ValueError):
        load_verified_close_parent_overrides(connection)


def test_repair_that_would_fork_an_existing_chain_is_rejected(ledger):
    connection, _, root = ledger
    connection.execute("INSERT INTO eod_close_observations SELECT 3,symbol,trading_date,official_close,"
        "source,source_reference,source_verified,'2026-08-25T21:03:00+00:00',1,source_artifact_sha256 "
        "FROM eod_close_observations WHERE id=1")
    with pytest.raises(ValueError, match="forked"):
        repair(connection, root)


def test_invalid_receipt_hash_fails_closed(ledger):
    connection, _, root = ledger
    repair(connection, root)
    connection.execute(f"DROP TRIGGER {TABLE}_no_update")
    connection.execute(f"UPDATE {TABLE} SET receipt_json='{{}}'")
    with pytest.raises(ValueError, match="receipt structure"):
        load_verified_close_parent_overrides(connection)


def test_sqlalchemy_connection_reads_the_same_receipt(ledger):
    from sqlalchemy import create_engine
    connection, path, root = ledger
    repair(connection, root)
    connection.commit()
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    try:
        with engine.connect() as sqlalchemy_connection:
            assert load_verified_close_parent_overrides(sqlalchemy_connection) == {2: 1}
    finally:
        engine.dispose()


@pytest.mark.parametrize("document", [[], None, {"version": 1}])
def test_malformed_receipt_returns_unavailable_status(ledger, document):
    connection, path, root = ledger
    repair(connection, root)
    connection.execute(f"DROP TRIGGER {TABLE}_no_update")
    connection.execute(f"UPDATE {TABLE} SET receipt_json=?", (json.dumps(document),))
    connection.commit()
    status = _verified_close_evidence(path, project_root=root)
    assert status["available"] is False
    assert "receipt structure" in status["reason"]


def test_governed_scoring_uses_reconciled_tip_and_later_real_correction(tmp_path, monkeypatch):
    from datetime import date
    from backend.closing_tape.governance import score_promoted_prediction_outcomes
    from tests.test_closing_tape_scorecard import (
        _initialize_database, _insert_close, _issue_governed_batch,
    )
    path = tmp_path / "market.db"
    _initialize_database(path)
    day = date(2026, 8, 25)
    _, _, prediction = _issue_governed_batch(path, tmp_path=tmp_path, monkeypatch=monkeypatch,
        trading_day=day, reference_price=100.0)
    first = _insert_close(path, trading_day=day, official_close=prediction.predicted_level)
    second = _insert_close(path, trading_day=day, official_close=prediction.predicted_level,
        observation_suffix="corroborating", observed_offset_minutes=1)
    root = tmp_path / "runtime"
    with pytest.raises(RuntimeError, match="exactly one causal root"):
        score_promoted_prediction_outcomes(path, trading_day=day, project_root=root)
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        repair(connection, root, pairs=((second, first),))
    result = score_promoted_prediction_outcomes(path, trading_day=day, project_root=root)
    assert result["close_observation_ids"] == [second]
    third = _insert_close(path, trading_day=day, official_close=prediction.predicted_level + 1,
        observation_suffix="real-correction", observed_offset_minutes=2, correction_of_id=second)
    result = score_promoted_prediction_outcomes(path, trading_day=day, project_root=root)
    assert result["close_observation_ids"] == [third]


def test_database_writer_requires_reconciled_tip_for_subsequent_close(tmp_path, monkeypatch):
    from datetime import date, datetime, timezone
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from backend import database
    path = tmp_path / "writer.db"
    engine = create_engine(f"sqlite:///{path.as_posix()}")
    sessions = sessionmaker(bind=engine)
    monkeypatch.setattr(database, "SessionLocal", sessions)
    database.EODClose.__table__.create(engine)
    database.EODCloseObservation.__table__.create(engine)
    try:
        with sessions.begin() as session:
            for ident in (1, 2):
                raw = f"close {ident}".encode()
                digest = hashlib.sha256(raw).hexdigest()
                artifact = tmp_path / "data" / "verified_close_sources" / "2026-08-25" / "SPX" / f"{digest}.txt"
                artifact.parent.mkdir(parents=True, exist_ok=True)
                artifact.write_bytes(raw)
                session.add(database.EODCloseObservation(id=ident, observation_key=str(ident) * 64,
                    symbol="SPX", trading_date=date(2026, 8, 25), official_close=6500.0,
                    source="cboe-official", source_reference="https://cdn.cboe.com/close.csv",
                    source_artifact_sha256=digest, source_verified=True,
                    observed_at_utc=datetime(2026, 8, 25, 21, ident)))
        with sqlite3.connect(path) as connection:
            connection.row_factory = sqlite3.Row
            repair(connection, tmp_path)
        kwargs = dict(source_reference="https://cdn.cboe.com/close.csv",
            source_artifact_sha256="c" * 64, observed_at_utc=datetime(2026, 8, 25, 21, 3, tzinfo=timezone.utc))
        with pytest.raises(ValueError, match="current authoritative"):
            database.upsert_verified_eod_close("SPX", date(2026, 8, 25), 6501, "cboe-official",
                correction_of_id=1, **kwargs)
        result = database.upsert_verified_eod_close("SPX", date(2026, 8, 25), 6501, "cboe-official",
            correction_of_id=2, **kwargs)
        assert result.official_close == 6501
        with sessions() as session:
            assert session.query(database.EODCloseObservation).count() == 3
    finally:
        engine.dispose()
