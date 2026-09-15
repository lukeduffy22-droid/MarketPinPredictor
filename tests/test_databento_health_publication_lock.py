"""Health sampling must not carry publication waits into quote ingestion."""

import asyncio
from datetime import date
import threading

import pytest

from tests.test_databento_reader_publication_lock import _mapping, _quote, _streamer, SYMBOL
import backend.databento_streamer as stream_module


class _PauseAfterHealthPublicationSample:
    """Schedule a publisher between health's first and second identity reads."""

    def __init__(self):
        self.lock = threading.RLock()
        self.depth = threading.local()
        self.first_sample_released = threading.Event()
        self.resume_health = threading.Event()
        self.health_waiting_again = threading.Event()
        self.paused_once = False

    def __enter__(self):
        is_health = threading.current_thread().name == "health-sample"
        if is_health and self.paused_once:
            self.health_waiting_again.set()
        self.lock.acquire()
        self.depth.value = getattr(self.depth, "value", 0) + 1
        return self

    def __exit__(self, *_args):
        self.depth.value -= 1
        self.lock.release()
        if (
            threading.current_thread().name == "health-sample"
            and self.depth.value == 0
            and not self.paused_once
        ):
            self.paused_once = True
            self.first_sample_released.set()
            assert self.resume_health.wait(3.0)


def test_actual_health_sample_cannot_block_reader_behind_delayed_publication(monkeypatch):
    streamer = _streamer(monkeypatch)
    publication_lock = _PauseAfterHealthPublicationSample()
    streamer._prediction_publication_lock = publication_lock
    first_consumed = threading.Event()
    release_second = threading.Event()
    second_stored = threading.Event()
    captured = []
    errors = []
    health_results = []
    original_store = streamer._store_quote

    def record_store(symbol, payload):
        original_store(symbol, payload)
        captured.append(dict(payload))
        if len(captured) == 2:
            second_stored.set()

    monkeypatch.setattr(streamer, "_store_quote", record_store)

    class FakeLocalClient:
        def __init__(self, **_kwargs):
            pass

        def subscribe(self, **kwargs):
            assert kwargs["symbols"] == [SYMBOL]

        def __iter__(self):
            yield _mapping()
            yield _quote()
            first_consumed.set()
            assert release_second.wait(3.0)
            yield _quote()
            streamer.is_running = False

        def stop(self):
            pass

    monkeypatch.setattr(stream_module.db, "Live", FakeLocalClient)

    def run_reader():
        try:
            streamer._run_live()
        except BaseException as exc:
            errors.append(exc)

    def run_health():
        try:
            health_results.append(streamer.get_health())
        except BaseException as exc:
            errors.append(exc)

    reader = threading.Thread(target=run_reader, name="quote-reader", daemon=True)
    health = threading.Thread(target=run_health, name="health-sample", daemon=True)
    reader.start()
    try:
        assert first_consumed.wait(3.0)
        assert streamer.handoff_status == "active"
        health.start()
        assert publication_lock.first_sample_released.wait(3.0)
        with streamer.prediction_publication_guard(
            subscription_epoch_id=streamer.subscription_epoch_id,
            subscription_generation=1,
        ) as allowed:
            assert allowed
            publication_lock.resume_health.set()
            assert publication_lock.health_waiting_again.wait(3.0)
            quote_lock_available = streamer._fresh_quote_lock.acquire(blocking=False)
            if quote_lock_available:
                streamer._fresh_quote_lock.release()
            release_second.set()
            reader_progressed = second_stored.wait(0.3)
    finally:
        publication_lock.resume_health.set()
        release_second.set()
        streamer.is_running = False
        reader.join(timeout=3.0)
        if health.ident is not None:
            health.join(timeout=3.0)

    assert not reader.is_alive() and not health.is_alive()
    assert not errors
    assert health_results
    assert quote_lock_available, "health held the quote lock while waiting for publication"
    assert reader_progressed, "health made delayed publication block the actual quote reader"
    assert [payload["generation"] for payload in captured] == [1, 1]


@pytest.mark.parametrize("operation", ["orb_snapshot", "orb_recheck", "stop"])
def test_other_identity_reads_wait_for_publication_before_taking_quote_lock(monkeypatch, operation):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    waiting_for_publication = threading.Event()
    completed = threading.Event()
    errors = []

    class ObservedPublicationLock:
        def __init__(self):
            self.lock = threading.RLock()

        def __enter__(self):
            if threading.current_thread().name == "identity-worker":
                waiting_for_publication.set()
            self.lock.acquire()
            return self

        def __exit__(self, *_args):
            self.lock.release()

    streamer._prediction_publication_lock = ObservedPublicationLock()

    def run_operation():
        try:
            if operation == "orb_snapshot":
                streamer._snapshot_opening_reference_inputs("SPX", trading_date=date(2026, 9, 8))
            elif operation == "orb_recheck":
                streamer._opening_reference_context_is_current({
                    "market": "SPX", "subscription_epoch_id": streamer.subscription_epoch_id,
                    "generation": 1, "context_identity": "obsolete-context",
                })
            else:
                asyncio.run(streamer.stop())
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    worker = threading.Thread(target=run_operation, name="identity-worker", daemon=True)
    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=1,
    ) as allowed:
        assert allowed
        worker.start()
        assert waiting_for_publication.wait(3.0)
        quote_lock_available = streamer._fresh_quote_lock.acquire(blocking=False)
        if quote_lock_available:
            streamer._fresh_quote_lock.release()
        assert not completed.is_set()
    worker.join(timeout=3.0)
    assert not worker.is_alive()
    assert not errors
    assert quote_lock_available, f"{operation} held the quote lock while waiting for publication"


def test_orb_scheduler_dispatch_takes_publication_before_universe(monkeypatch):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 1
    order_state = threading.local()
    inversion_observed = threading.Event()
    dispatched = threading.Event()

    class TrackedLock:
        def __init__(self, name):
            self.name = name
            self.lock = threading.RLock()

        def __enter__(self):
            depth_name = f"{self.name}_depth"
            depth = getattr(order_state, depth_name, 0)
            if (
                self.name == "publication"
                and depth == 0
                and getattr(order_state, "universe_depth", 0) > 0
            ):
                inversion_observed.set()
            self.lock.acquire()
            setattr(order_state, depth_name, depth + 1)
            return self

        def __exit__(self, *_args):
            depth_name = f"{self.name}_depth"
            setattr(order_state, depth_name, getattr(order_state, depth_name) - 1)
            self.lock.release()

    class StopAfterDispatch:
        def is_set(self):
            return False

        def wait(self, timeout=None):
            del timeout
            streamer.is_running = False
            return True

    streamer._prediction_publication_lock = TrackedLock("publication")
    streamer._universe_index_lock = TrackedLock("universe")
    streamer._stop_event = StopAfterDispatch()
    monkeypatch.setattr(
        stream_module,
        "live_subscription_window",
        lambda _observed: {"state": "regular_session"},
    )
    monkeypatch.setattr(
        streamer,
        "_capture_opening_reference_for_bucket",
        lambda *_args, **_kwargs: (
            dispatched.set()
            or {"recorded": False, "reason": "OFFLINE_SCHEDULER_TEST"}
        ),
    )

    sampler = threading.Thread(
        target=streamer._opening_reference_loop,
        name="orb-scheduler-order-test",
        daemon=True,
    )
    sampler.start()
    sampler.join(timeout=3.0)

    assert not sampler.is_alive()
    assert dispatched.wait(1.0)
    assert not inversion_observed.is_set(), (
        "ORB scheduler acquired publication while already holding universe"
    )
