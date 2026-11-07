"""An unrelated forecast publication cannot consume an ORB capture bucket.

The dispatch test proves submission only. The separate capture tests use the
real capture path, provider-clock classifier and temporary SQLite journal.
"""
from concurrent.futures import Future
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import threading
import time

import pytest
from sqlalchemy import text

import backend.databento_streamer as stream_module
from tests.test_opening_transition_stress import (
    september_14_open,
    _publish_mappings_and_quotes,
)
from tests.test_orb_cbbo_capture import _record


@contextmanager
def held_prediction_publication(streamer):
    entered, release = threading.Event(), threading.Event()
    failures = []
    epoch, generation = streamer.subscription_epoch_id, streamer.active_generation

    def publish():
        try:
            with streamer.prediction_publication_guard(
                subscription_epoch_id=epoch,
                subscription_generation=generation,
            ) as allowed:
                if not allowed:
                    raise AssertionError("The real forecast guard rejected fixture identity")
                entered.set()
                if not release.wait(5):
                    raise AssertionError("Test did not release its publication holder")
        except BaseException as error:
            failures.append(error)
            entered.set()

    holder = threading.Thread(target=publish, daemon=True)
    holder.start()
    try:
        assert entered.wait(1)
        assert not failures
        yield holder, release
    finally:
        release.set()
        holder.join(2)
        assert not holder.is_alive()
        assert not failures


def ready_opening(streamer, opening):
    recv = int(opening.timestamp()) * 10**9
    for instrument in range(stream_module.CLOCK_SYNC_MIN_SAMPLES):
        assert streamer._record_processing_clock_sample(
            _record(instrument + 1, recv),
            processed_at_ns=recv + 20_000_000,
            generation=7,
        )
    assert streamer._processing_clock_telemetry()["sample_count"] == 50
    for index, market in enumerate(("SPX", "NDX")):
        received = _publish_mappings_and_quotes(
            streamer, market, first_instrument_id=10000 + index * 10000,
        )
        streamer._record_fresh_market_and_maybe_activate(
            market, generation=7, received_monotonic=received,
        )
    assert streamer.handoff_status == "active"


@pytest.mark.parametrize("market", ["SPX", "NDX"])
def test_actual_orb_capture_and_final_decision_persist_while_forecast_guard_is_held(
    september_14_open, market,
):
    streamer, journal, engine, opening = september_14_open
    ready_opening(streamer, opening)
    observed = opening + timedelta(milliseconds=250)
    with held_prediction_publication(streamer) as (holder, release):
        started = time.monotonic()
        result = streamer._capture_opening_reference_for_bucket(
            market, opening, now_utc=lambda: observed, journal=journal,
            attempt_deadline_utc=observed + timedelta(seconds=1),
            attempt_deadline_monotonic=started + 1,
            expected_subscription_epoch_id=streamer.subscription_epoch_id,
            expected_subscription_generation=7,
        )
        elapsed = time.monotonic() - started
        assert elapsed < 1.25
        assert holder.is_alive() and not release.is_set()
        assert result["recorded"] is True, result
        assert result["progress_eligible"] is True, result
        assert result["opening_gate_evidence"]["processing_clock"]["sample_count"] == 50
        assert result["opening_gate_evidence"]["complete_pair_count"] >= stream_module.MIN_PAIRED_QUOTES
        with engine.connect() as connection:
            assert connection.execute(text("SELECT count(*) FROM orb_reference_samples")).scalar_one() == 1
            assert connection.execute(text("SELECT count(*) FROM orb_reference_sample_decisions WHERE progress_eligible=1")).scalar_one() == 1
        assert not release.is_set()  # Both durable rows exist before forecast release.


def test_actual_sampler_dispatches_configured_markets_while_forecast_guard_is_held(
    september_14_open, monkeypatch,
):
    streamer, _journal, _engine, opening = september_14_open
    ready_opening(streamer, opening)
    observed = opening + timedelta(milliseconds=250)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed.astimezone(tz) if tz is not None else observed.replace(tzinfo=None)

    submissions, executors = [], []
    exited = threading.Event()

    class RecordingExecutor:
        def __init__(self, **options):
            self.options = options
            self.stopped = False
            executors.append(self)

        def submit(self, function, *args, **options):
            # Deliberately do not execute capture: this test proves dispatch,
            # while the preceding tests prove the durable capture behavior.
            submissions.append((function, args, options))
            return Future()

        def shutdown(self, **_options):
            self.stopped = True

    class StopAfterFirstPoll:
        def wait(self, timeout=None):
            streamer.is_running = False
            return True

        def is_set(self):
            return False

    monkeypatch.setattr(stream_module, "datetime", FixedDateTime)
    monkeypatch.setattr(stream_module, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr("backend.opening_capture_receipts.due_checks", lambda *_: [])
    streamer._stop_event = StopAfterFirstPoll()

    def dispatch():
        try:
            streamer._opening_reference_loop()
        finally:
            exited.set()

    worker = threading.Thread(target=dispatch, daemon=True)
    with held_prediction_publication(streamer) as (holder, release):
        worker.start()
        completed_while_held = exited.wait(.75)
        assert holder.is_alive() and not release.is_set()
    worker.join(2)
    assert not worker.is_alive()
    assert completed_while_held, "Actual sampler dispatch waited for the unrelated forecast guard"
    assert {args[0] for _, args, _ in submissions} == set(streamer.symbols)
    assert all(function.__self__ is streamer and function.__name__ == "_capture_opening_reference_for_bucket" for function, _, _ in submissions)
    assert all(args[1] == opening for _, args, _ in submissions)
    assert all(options["expected_subscription_generation"] == 7 for _, _, options in submissions)
    assert len(executors) == 2 and all(executor.stopped for executor in executors)
