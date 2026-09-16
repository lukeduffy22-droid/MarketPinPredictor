import sqlite3
from datetime import date

import pytest

from tools import install_orb_read_index as installer


@pytest.fixture
def retained_database(tmp_path):
    path = tmp_path / "retained.db"
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE orb_reference_samples (
            sample_id TEXT PRIMARY KEY, symbol TEXT, trading_date TEXT,
            sample_timestamp_utc TEXT, captured_at_utc TEXT, provider TEXT,
            subscription_epoch_id TEXT, subscription_generation INTEGER,
            universe_sha256 TEXT, formula_inputs_json TEXT)""")
        connection.execute("""INSERT INTO orb_reference_samples VALUES
            ('a', 'SPX', '2026-09-16', '13:30', '13:30', 'databento',
             'epoch', 1, 'universe', '{"audit":"preserve exactly"}')""")
    return path


def test_index_install_preserves_evidence_and_is_idempotent(retained_database):
    with sqlite3.connect(retained_database) as connection:
        before = connection.execute("SELECT * FROM orb_reference_samples").fetchall()
    assert installer.install(retained_database)["status"] == "planned"
    assert installer.install(retained_database, apply=True)["status"] == "installed"
    assert installer.install(retained_database, apply=True)["status"] == "already_present"
    with sqlite3.connect(retained_database) as connection:
        assert connection.execute("SELECT * FROM orb_reference_samples").fetchall() == before


def test_busy_writer_defers_without_installing(retained_database):
    with sqlite3.connect(retained_database) as writer:
        writer.execute("BEGIN IMMEDIATE")
        result = installer.install(retained_database, apply=True)
        assert result["status"] == "deferred"
        writer.rollback()
    assert installer.install(retained_database)["status"] == "planned"


def test_interrupted_build_rolls_back(retained_database, monkeypatch):
    with sqlite3.connect(retained_database) as connection:
        connection.executemany(
            "INSERT INTO orb_reference_samples(sample_id) VALUES (?)",
            [(str(i),) for i in range(2000)],
        )
    ticks = iter([0.0, 2.0, 2.0])
    monkeypatch.setattr(installer.time, "perf_counter", lambda: next(ticks))
    assert installer.install(retained_database, apply=True)["status"] == "deferred"
    assert installer.install(retained_database)["status"] == "planned"
    with sqlite3.connect(retained_database) as connection:
        assert connection.execute("SELECT count(*) FROM orb_reference_samples").fetchone()[0] == 2001


def test_missing_database_is_never_created(tmp_path):
    missing = tmp_path / "missing.db"
    with pytest.raises(ValueError, match="already exist"):
        installer.install(missing, apply=True)
    assert not missing.exists()


def test_date_scoped_index_serves_parameterized_live_query_only_for_its_date(retained_database):
    day = date(2026, 9, 16)
    assert installer.install(retained_database, apply=True, trading_date=day)["status"] == "installed"
    assert installer.install(retained_database, apply=True, trading_date=day)["status"] == "already_present"
    with sqlite3.connect(retained_database) as connection:
        query = """EXPLAIN QUERY PLAN SELECT sample_id,sample_timestamp_utc,provider,
            subscription_epoch_id,subscription_generation,universe_sha256
            FROM orb_reference_samples WHERE symbol=? AND trading_date=?
            AND sample_timestamp_utc<=? AND captured_at_utc<=?
            ORDER BY sample_timestamp_utc,sample_id"""
        plan = connection.execute(query, ('SPX', day.isoformat(), '23:59', '23:59')).fetchall()
        assert any('COVERING INDEX idx_orb_reference_snapshot_metadata_20260916' in r[3] for r in plan)
        assert not any('TEMP B-TREE' in r[3] for r in plan)
        other_day = connection.execute(query, ('SPX', '2026-09-15', '23:59', '23:59')).fetchall()
        assert not any('snapshot_metadata_20260916' in r[3] for r in other_day)
