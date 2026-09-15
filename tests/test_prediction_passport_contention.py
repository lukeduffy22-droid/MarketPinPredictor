"""Real SQLite writer contention on temporary files; never the runtime DB."""

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import OperationalError

import backend.database as database
import backend.prediction_passport as passport
from tests.test_prediction_passport import _prediction, passport_store


def _issue(key="bounded-contention", guard=None):
    return passport.issue_prediction_passport(
        _prediction(calculation_id="no-lineage"),
        prediction_mode="unit", origin_key=key, publication_guard=guard,
    )


def _write_lock(engine):
    with engine.connect() as connection:
        connection.exec_driver_sql("PRAGMA journal_mode=WAL")
    blocker = engine.connect()
    blocker.exec_driver_sql("BEGIN IMMEDIATE")
    return blocker


def test_contended_sqlite_write_has_two_bounded_attempts_and_restores_timeout(passport_store, monkeypatch):
    monkeypatch.setattr(passport, "PASSPORT_SQLITE_WRITE_TIMEOUT_MS", 25)
    attempts = []
    blocker = _write_lock(passport_store)

    def before_execute(connection, cursor, statement, parameters, context, executemany):
        if statement == "BEGIN IMMEDIATE":
            raw = connection.connection.driver_connection
            attempts.append(raw.execute("PRAGMA busy_timeout").fetchone()[0])

    event.listen(passport_store, "before_cursor_execute", before_execute)
    try:
        with pytest.raises(OperationalError, match="database is locked"):
            _issue()
    finally:
        event.remove(passport_store, "before_cursor_execute", before_execute)
        blocker.rollback()
        blocker.close()
    assert attempts == [25, 25]
    with passport_store.connect() as connection:
        assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 5000
        assert connection.execute(text("SELECT COUNT(*) FROM prediction_passports")).scalar_one() == 0


def test_released_lock_retries_once_and_preserves_immutable_idempotency(passport_store, monkeypatch):
    monkeypatch.setattr(passport, "PASSPORT_SQLITE_WRITE_TIMEOUT_MS", 25)
    blocker = _write_lock(passport_store)
    failures = []

    def release_after_failed_attempt(context):
        if context.statement == "BEGIN IMMEDIATE":
            failures.append(str(context.original_exception))
            blocker.rollback()

    event.listen(passport_store, "handle_error", release_after_failed_attempt)
    try:
        issued = _issue()
    finally:
        event.remove(passport_store, "handle_error", release_after_failed_attempt)
        blocker.close()
    assert failures == ["database is locked"]
    retried = _issue()
    assert issued["forecast_id"] == retried["forecast_id"]
    assert issued["record_sha256"] == retried["record_sha256"]
    with passport_store.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM prediction_passports")).scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 5000


def test_generation_guard_is_rechecked_before_retry_after_lock_release(passport_store, monkeypatch):
    monkeypatch.setattr(passport, "PASSPORT_SQLITE_WRITE_TIMEOUT_MS", 25)
    blocker = _write_lock(passport_store)
    state = {"allowed": True}

    def change_generation_after_failed_attempt(context):
        if context.statement == "BEGIN IMMEDIATE":
            state["allowed"] = False
            blocker.rollback()

    event.listen(passport_store, "handle_error", change_generation_after_failed_attempt)
    try:
        with pytest.raises(passport.PassportPublicationRejected):
            _issue(guard=lambda: state["allowed"])
    finally:
        event.remove(passport_store, "handle_error", change_generation_after_failed_attempt)
        blocker.close()
    with database.SessionLocal() as session:
        assert session.query(database.PredictionPassport).count() == 0


def test_generation_guard_after_flush_rolls_back_before_commit(passport_store):
    state = {"allowed": True}

    def change_generation_after_insert(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO prediction_passports"):
            state["allowed"] = False

    event.listen(passport_store, "after_cursor_execute", change_generation_after_insert)
    try:
        with pytest.raises(passport.PassportPublicationRejected):
            _issue(guard=lambda: state["allowed"])
    finally:
        event.remove(passport_store, "after_cursor_execute", change_generation_after_insert)
    with database.SessionLocal() as session:
        assert session.query(database.PredictionPassport).count() == 0


def test_non_lock_operational_error_is_not_retried():
    assert passport._sqlite_busy(OperationalError("SELECT", {}, Exception("no such table"))) is False
