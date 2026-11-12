"""Offline fault injection for the caller-owned ORB persistence deadline."""

from datetime import datetime, timedelta, timezone
import math
import sqlite3
import threading
import time

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

import backend.database as database
from backend.market_structure import MarketStructureJournal
from tests.test_databento_streamer import _opening_reference_streamer
from tests.test_market_structure_orb import _payload, _reference_payload


OBSERVED = datetime(2026, 9, 8, 13, 30, 5, tzinfo=timezone.utc)


@pytest.fixture
def orb_db(tmp_path, monkeypatch):
    path = tmp_path / "orb.sqlite"
    engine = create_engine(f"sqlite:///{path.as_posix()}", pool_size=1, max_overflow=0, pool_timeout=1)
    database.Base.metadata.create_all(engine)
    with engine.connect() as conn:
        conn.exec_driver_sql("PRAGMA journal_mode=WAL")
    monkeypatch.setattr(database, "SessionLocal", sessionmaker(bind=engine))
    try:
        yield path, engine
    finally:
        engine.dispose()


def sample():
    rows = []
    journal = MarketStructureJournal(reference_saver=lambda row: rows.append(row) or row)
    result = journal.record_reference(_reference_payload(
        _payload(OBSERVED, 6500, primary_expiration=OBSERVED.date().isoformat())
    ))
    assert result["recorded"] is True
    return rows[0]


def decision(raw):
    return {
        "sample_id": raw["sample_id"],
        "sample_timestamp_utc": OBSERVED,
        "intended_bucket_utc": OBSERVED,
        "attempt_completed_at_utc": OBSERVED + timedelta(milliseconds=100),
        "progress_eligible": True,
        "reason": None,
    }


def budget(seconds=1, cancel=None):
    return database.OrbPersistenceBudget(time.monotonic() + seconds, cancellation_reason=cancel)


def counts(engine):
    with engine.connect() as conn:
        return (
            conn.execute(text("SELECT count(*) FROM orb_reference_samples")).scalar_one(),
            conn.execute(text("SELECT count(*) FROM orb_reference_sample_decisions")).scalar_one(),
        )


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_budget_cannot_start_a_write(orb_db, deadline):
    bound = database.OrbPersistenceBudget(deadline)
    assert database.save_orb_reference_sample(sample(), persistence_budget=bound) is None
    assert bound.reason() == "REFERENCE_PERSISTENCE_DEADLINE_INVALID"
    assert counts(orb_db[1]) == (0, 0)


def test_cancellation_probe_exception_fails_closed(orb_db):
    def broken_probe():
        raise RuntimeError("clock unavailable")
    bound = budget(cancel=broken_probe)
    assert database.save_orb_reference_sample(sample(), persistence_budget=bound) is None
    assert bound.reason() == "REFERENCE_PERSISTENCE_CANCELLATION_CHECK_FAILED"
    assert counts(orb_db[1]) == (0, 0)


def test_bounded_save_is_idempotent_and_needs_no_shared_pool_checkout(orb_db):
    raw = sample()
    # Occupy the entire normal pool. The ORB write must not wait one second
    # for pool checkout or refresh its row after committing.
    with orb_db[1].connect():
        started = time.monotonic()
        first = database.save_orb_reference_sample(raw, persistence_budget=budget(.5))
        assert first is not None
        assert first.sample_id == raw["sample_id"]
        assert time.monotonic() - started < .5
        assert database.save_orb_reference_sample(raw, persistence_budget=budget()) is not None
        assert database.save_orb_reference_sample_decision(decision(raw), persistence_budget=budget()) is not None
        assert database.save_orb_reference_sample_decision(decision(raw), persistence_budget=budget()) is not None
    assert counts(orb_db[1]) == (1, 1)


def test_sqlite_writer_contention_rejects_without_a_late_background_write(orb_db):
    raw = sample()
    blocking = sqlite3.connect(orb_db[0], timeout=0)
    try:
        blocking.execute("BEGIN IMMEDIATE")
        bound = budget(.1)
        started = time.monotonic()
        assert database.save_orb_reference_sample(raw, persistence_budget=bound) is None
        assert time.monotonic() - started < .35
        assert bound.reason() in {"REFERENCE_PERSISTENCE_BUSY", "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"}
    finally:
        blocking.rollback()
        blocking.close()
    assert counts(orb_db[1]) == (0, 0)
    # A later valid caller can write; the rejected call left no transaction or
    # thread waiting for the SQLite lock to clear.
    assert database.save_orb_reference_sample(raw, persistence_budget=budget()) is not None
    assert counts(orb_db[1]) == (1, 0)


def test_sqlite_vm_work_is_interrupted_by_budget(orb_db):
    bound = budget(.03)
    session = database._orb_reference_write_session(bound)
    started = time.monotonic()
    try:
        with pytest.raises(Exception, match="interrupted|REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"):
            session.execute(text("WITH RECURSIVE x(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM x WHERE n<100000000) SELECT sum(n) FROM x"))
    finally:
        database._close_orb_reference_write_session(session)
    assert time.monotonic() - started < .5


@pytest.mark.parametrize("stage", ["raw", "decision"])
def test_cancel_after_flush_rolls_back_before_commit(orb_db, monkeypatch, stage):
    raw = sample()
    if stage == "decision":
        assert database.save_orb_reference_sample(raw, persistence_budget=budget()) is not None
    cancelled = threading.Event()
    real_session = database._orb_reference_write_session

    def session_with_cancellation(bound):
        session = real_session(bound)
        event.listen(session, "after_flush", lambda *_: cancelled.set())
        return session

    monkeypatch.setattr(database, "_orb_reference_write_session", session_with_cancellation)
    bound = budget(cancel=lambda: "REFERENCE_STREAMER_STOPPING" if cancelled.is_set() else None)
    saver = database.save_orb_reference_sample if stage == "raw" else database.save_orb_reference_sample_decision
    assert saver(raw if stage == "raw" else decision(raw), persistence_budget=bound) is None
    assert bound.reason() == "REFERENCE_STREAMER_STOPPING"
    assert counts(orb_db[1]) == ((0, 0) if stage == "raw" else (1, 0))


def ready_streamer(monkeypatch):
    streamer = _opening_reference_streamer(OBSERVED + timedelta(milliseconds=100))
    streamer.is_running = True
    monkeypatch.setattr(streamer, "_processing_clock_telemetry", lambda: {"status": "synchronized"})
    return streamer


def test_raw_commit_returning_after_cancel_gets_ineligible_audit_decision(orb_db, monkeypatch):
    streamer = ready_streamer(monkeypatch)
    cancelled = threading.Event()
    real_session = database._orb_reference_write_session

    def delayed_commit_return(bound):
        session = real_session(bound)
        commit = session.commit
        def commit_then_cancel():
            commit()
            cancelled.set()  # Simulates a deadline/stop detected at DB commit return.
        session.commit = commit_then_cancel
        return session

    monkeypatch.setattr(database, "_orb_reference_write_session", delayed_commit_return)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
        journal=MarketStructureJournal(), attempt_cancel_event=cancelled,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["progress_decision_pending"] is False
    assert result["progress_decision_recorded"] is True
    assert counts(orb_db[1]) == (1, 1)
    with orb_db[1].connect() as connection:
        assert connection.execute(text(
            "SELECT progress_eligible FROM orb_reference_sample_decisions"
        )).scalar_one() == 0


def test_delayed_dialect_commit_cannot_commit_expired_decision(orb_db, monkeypatch):
    raw = sample()
    assert database.save_orb_reference_sample(raw, persistence_budget=budget()) is not None
    real_session = database._orb_reference_write_session
    def delayed_dialect(bound):
        session = real_session(bound)
        dialect = session.info["orb_bounded_engine"].dialect
        do_commit = dialect.do_commit
        def delayed_commit(connection):
            time.sleep(.08)  # After SQLAlchemy's commit event, before DBAPI.
            do_commit(connection)
        dialect.do_commit = delayed_commit
        return session
    monkeypatch.setattr(database, "_orb_reference_write_session", delayed_dialect)
    bound = budget(.05)
    assert database.save_orb_reference_sample_decision(decision(raw), persistence_budget=bound) is None
    assert bound.reason() == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
    assert counts(orb_db[1]) == (1, 0)


def test_commit_authorizer_allows_rollback_but_denies_expired_sql_commit(orb_db):
    cancelled = threading.Event()
    bound = budget(cancel=lambda: "REFERENCE_STREAMER_STOPPING" if cancelled.is_set() else None)
    session = database._orb_reference_write_session(bound)
    try:
        raw_connection = session.connection().connection.driver_connection
        raw_connection.execute("BEGIN")
        cancelled.set()
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            raw_connection.execute("COMMIT")
        raw_connection.rollback()
        assert not raw_connection.in_transaction
    finally:
        database._close_orb_reference_write_session(session)


def test_decision_commit_return_after_cancel_has_no_current_runtime_authority(orb_db, monkeypatch):
    streamer = ready_streamer(monkeypatch)
    cancelled = threading.Event()
    real_session = database._orb_reference_write_session
    commits = []
    def cancel_after_decision_commit(bound):
        session = real_session(bound)
        commit = session.commit
        def commit_then_cancel():
            commit()
            commits.append(True)
            if len(commits) == 2:
                cancelled.set()
        session.commit = commit_then_cancel
        return session
    monkeypatch.setattr(database, "_orb_reference_write_session", cancel_after_decision_commit)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
        journal=MarketStructureJournal(), attempt_cancel_event=cancelled,
    )
    assert result["recorded"] is True
    assert result["progress_eligible"] is False
    assert result["progress_decision_recorded"] is True
    assert result["progress_decision_pending"] is False
    assert result["progress_decision_returned_after_budget"] is True
    assert counts(orb_db[1]) == (1, 1)  # Preserve the historical post-raw decision.


def test_held_barrier_times_out_without_admitting_a_late_writer(monkeypatch):
    streamer = ready_streamer(monkeypatch)
    streamer._orb_reference_persist_barrier.acquire()
    started = time.monotonic()
    try:
        result = streamer._capture_opening_reference_for_bucket(
            "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
            journal=object(), attempt_deadline_monotonic=time.monotonic() + .08,
        )
    finally:
        streamer._orb_reference_persist_barrier.release()
    assert time.monotonic() - started < .4
    assert result["recorded"] is False
    assert result["reason"] == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
    assert result["persist_lock_held_seconds"] == 0
    assert result["capture_rejection_stage"] == "persist_barrier_wait"
    assert result["capture_stage_elapsed_seconds"]["persist_barrier_wait"] >= .04


@pytest.mark.parametrize("stop", [False, True])
def test_cancel_or_shutdown_interrupts_waiter_while_barrier_stays_held(monkeypatch, stop):
    streamer = ready_streamer(monkeypatch)
    cancelled = threading.Event()
    result = {}
    streamer._orb_reference_persist_barrier.acquire()
    def capture():
        result.update(streamer._capture_opening_reference_for_bucket(
            "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
            journal=object(), attempt_cancel_event=cancelled,
        ))
    worker = threading.Thread(target=capture)
    worker.start()
    try:
        time.sleep(.03)
        (streamer._stop_event if stop else cancelled).set()
        worker.join(.5)
        assert not worker.is_alive()
        assert result["recorded"] is False
        assert result["reason"] == ("REFERENCE_STREAMER_STOPPING" if stop else "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED")
    finally:
        streamer._orb_reference_persist_barrier.release()
        worker.join(1)


def test_bucket_change_on_lock_admission_rejects_and_releases_lock(monkeypatch):
    streamer = ready_streamer(monkeypatch)
    now = [OBSERVED + timedelta(milliseconds=100)]
    class LateLock:
        released = False
        def acquire(self, timeout):
            now[0] = OBSERVED + timedelta(seconds=5)
            return True
        def release(self):
            self.released = True
    lock = LateLock()
    streamer._orb_reference_persist_barrier = lock
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OBSERVED, now_utc=lambda: now[0], journal=object(),
    )
    assert lock.released
    assert result["recorded"] is False
    assert result["progress_eligible"] is False
    assert result["reason"] in {"REFERENCE_ATTEMPT_DEADLINE_EXCEEDED", "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET"}


@pytest.mark.parametrize("phase", ["initial", "recheck", "post_raw"])
@pytest.mark.parametrize("lock_name", ["_universe_index_lock", "_fresh_quote_lock", "_clock_evidence_lock", "_runtime_context_lock"])
def test_all_capture_lock_waits_share_budget_and_release_ownership(orb_db, monkeypatch, phase, lock_name):
    streamer = ready_streamer(monkeypatch)
    # Exercise the real provider-clock evidence lock and unchanged thresholds.
    streamer._receive_lag_window.extend([.01] * 50)
    monkeypatch.setattr(streamer, "_processing_clock_telemetry",
                        type(streamer)._processing_clock_telemetry.__get__(streamer))
    held, release = threading.Event(), threading.Event()
    lock = getattr(streamer, lock_name)
    def hold_lock():
        with lock:
            held.set()
            release.wait(2)
    holder = threading.Thread(target=hold_lock)
    def start_holder():
        holder.start()
        assert held.wait(.5)
        active_budget = getattr(streamer._orb_reference_capture_context, "budget", None)
        if active_budget is not None:
            # Start the lock fault's short deadline after the preceding real
            # disk commit, so slow temp storage cannot test the wrong phase.
            active_budget.deadline_monotonic = time.monotonic() + .08
    journal = MarketStructureJournal()
    if phase == "initial":
        start_holder()
    elif phase == "recheck":
        original = streamer._compute_opening_reference_payload
        def compute_then_hold(*args, **kwargs):
            result = original(*args, **kwargs)
            start_holder()
            return result
        monkeypatch.setattr(streamer, "_compute_opening_reference_payload", compute_then_hold)
    else:
        original = journal.record_reference
        def persist_then_hold(payload):
            result = original(payload)
            assert result["recorded"] is True
            start_holder()
            return result
        monkeypatch.setattr(journal, "record_reference", persist_then_hold)
    started = time.monotonic()
    try:
        result = streamer._capture_opening_reference_for_bucket(
            "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
            journal=journal, attempt_deadline_monotonic=time.monotonic() + (.15 if phase == "initial" else 2),
        )
        assert time.monotonic() - started < 2.5
        assert result["reason"] == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
        assert result["progress_eligible"] is False
        assert result["recorded"] is (phase == "post_raw")
        lock_label = {"_prediction_publication_lock": "publication", "_universe_index_lock": "universe",
                      "_fresh_quote_lock": "quote", "_clock_evidence_lock": "provider_clock",
                      "_runtime_context_lock": "runtime_context"}[lock_name]
        expected_stage = ({"recheck": "pre_persist_context", "post_raw": "post_persist_context"}.get(phase)
                          or ("input_snapshot" if lock_name in {"_universe_index_lock", "_fresh_quote_lock"}
                              else "initial_checks"))
        assert result["capture_rejection_stage"] == expected_stage
        assert result["capture_rejected_lock"] == lock_label
        assert result["capture_rejected_lock_acquired"] is False
        assert result["capture_lock_wait_seconds"][lock_label] >= .04
        raw_count, decision_count = counts(orb_db[1])
        assert raw_count == (1 if phase == "post_raw" else 0)
        if phase == "post_raw":
            assert result["progress_decision_cleanup_attempted"] is True
            assert 0 <= decision_count <= 1
            if decision_count:
                with orb_db[1].connect() as connection:
                    assert connection.execute(text(
                        "SELECT progress_eligible FROM orb_reference_sample_decisions"
                    )).scalar_one() == 0
                assert result["progress_decision_recorded"] is True
                assert result["progress_decision_pending"] is False
            else:
                # This fault measures state-lock interruption, not a guarantee
                # that real storage completes its separate bounded audit append.
                assert result["progress_decision_recorded"] is False
                assert result["progress_decision_pending"] is True
                assert result["progress_decision_cleanup_error"] in {
                    "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED", "REFERENCE_PERSISTENCE_BUSY",
                    "REFERENCE_PERSISTENCE_FAILED", "REFERENCE_PROGRESS_DECISION_PERSISTENCE_FAILED",
                }
            assert result["progress_decision_cleanup_timeout_seconds"] == .25
            assert math.isfinite(result["progress_decision_cleanup_elapsed_seconds"])
            assert result["progress_decision_cleanup_elapsed_seconds"] >= 0.0
        else:
            assert decision_count == 0
        assert getattr(streamer._orb_reference_capture_context, "budget", None) is None
        assert streamer._orb_reference_persist_barrier.acquire(timeout=.1)
        streamer._orb_reference_persist_barrier.release()
    finally:
        release.set()
        if holder.ident is not None:
            holder.join(1)
    # No subset of the nested locks or caller-thread context leaked on timeout.
    for name in ("_prediction_publication_lock", "_universe_index_lock", "_fresh_quote_lock", "_clock_evidence_lock", "_runtime_context_lock"):
        candidate = getattr(streamer, name)
        assert candidate.acquire(timeout=.1)
        candidate.release()
    assert counts(orb_db[1]) == (raw_count, decision_count)


def test_runtime_context_lock_wait_is_bounded_with_explicit_identity(monkeypatch):
    streamer = ready_streamer(monkeypatch)
    held, release = threading.Event(), threading.Event()
    def hold_lock():
        with streamer._runtime_context_lock:
            held.set()
            release.wait(2)
    holder = threading.Thread(target=hold_lock)
    holder.start()
    assert held.wait(.5)
    started = time.monotonic()
    try:
        result = streamer._capture_opening_reference_for_bucket(
            "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
            journal=object(), expected_subscription_generation=7,
            attempt_deadline_monotonic=time.monotonic() + .08,
        )
        assert time.monotonic() - started < .5
        assert not result["recorded"] and not result["progress_eligible"]
        assert result["reason"] == "REFERENCE_ATTEMPT_DEADLINE_EXCEEDED"
    finally:
        release.set()
        holder.join(1)


def test_bounded_capture_omits_only_host_shadow_diagnostic_lock(orb_db, monkeypatch):
    streamer = ready_streamer(monkeypatch)
    streamer._receive_lag_window.extend([.01] * 50)
    monkeypatch.setattr(streamer, "_processing_clock_telemetry",
                        type(streamer)._processing_clock_telemetry.__get__(streamer))
    class Observer:
        calls = 0
        def snapshot(self, evidence):
            self.calls += 1
            return {"diagnostic": True}
    observer = Observer()
    streamer._host_clock_shadow = observer
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
        journal=MarketStructureJournal(),
    )
    assert result["recorded"] and result["progress_eligible"]
    assert observer.calls == 0
    assert streamer._processing_clock_telemetry()["status"] == "synchronized"
    assert observer.calls == 1


@pytest.mark.parametrize("stage", ["raw_persistence", "decision_persistence"])
def test_sqlite_contention_stage_is_retained_in_existing_failure_receipt(orb_db, monkeypatch, stage):
    streamer = ready_streamer(monkeypatch)
    journal = MarketStructureJournal()
    blocking = sqlite3.connect(orb_db[0], timeout=0)
    if stage == "raw_persistence":
        blocking.execute("BEGIN IMMEDIATE")
    else:
        original = journal.record_reference
        def persist_then_block(payload):
            result = original(payload)
            assert result["recorded"] is True
            blocking.execute("BEGIN IMMEDIATE")
            return result
        monkeypatch.setattr(journal, "record_reference", persist_then_block)
    try:
        result = streamer._capture_opening_reference_for_bucket(
            "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100), journal=journal,
        )
        assert result["recorded"] is (stage == "decision_persistence")
        assert not result["progress_eligible"]
        assert result["capture_rejection_stage"] == stage
        assert result["capture_stage_elapsed_seconds"][stage] >= 0
        receipts = []
        monkeypatch.setattr("backend.capture_attempts.record_opening_reference_failure",
                            lambda _directory, payload: receipts.append(dict(payload)))
        streamer._record_opening_reference_failed_attempt(
            market="SPX", subscription_epoch_id=streamer.subscription_epoch_id,
            subscription_generation=7, intended_bucket_utc=OBSERVED, result=result,
        )
        assert len(receipts) == 1
        assert receipts[0]["capture_rejection_stage"] == stage
        assert receipts[0]["capture_stage_elapsed_seconds"] == result["capture_stage_elapsed_seconds"]
        assert receipts[0]["capture_lock_wait_seconds"] == result["capture_lock_wait_seconds"]
    finally:
        blocking.rollback()
        blocking.close()
    assert counts(orb_db[1]) == ((0, 0) if stage == "raw_persistence" else (1, 0))


def test_successful_stage_timings_do_not_change_sample_or_decision(orb_db, monkeypatch):
    streamer = ready_streamer(monkeypatch)
    result = streamer._capture_opening_reference_for_bucket(
        "SPX", OBSERVED, now_utc=lambda: OBSERVED + timedelta(milliseconds=100),
        journal=MarketStructureJournal(),
    )
    assert result["recorded"] and result["progress_eligible"]
    assert "capture_rejection_stage" not in result
    assert "capture_rejected_lock" not in result
    assert set(result["capture_stage_elapsed_seconds"]) == {
        "initial_checks", "input_snapshot", "parity_calculation", "persist_barrier_wait",
        "pre_persist_context", "raw_persistence", "post_persist_context", "decision_persistence", "result_validation",
    }
    assert all(value >= 0 for value in result["capture_stage_elapsed_seconds"].values())
    assert result["capture_completed_stage"] == "result_validation"
    assert counts(orb_db[1]) == (1, 1)
