import threading
import time
from types import SimpleNamespace

import backend.databento_streamer as stream_module


SYMBOL = "SPXW  260908C06500000"


def _mapping():
    now = time.time_ns()
    return type("SymbolMappingMsg", (), {
        "instrument_id": 123,
        "stype_out_symbol": SYMBOL,
        "ts_event": now,
        "ts_index": now,
    })()


def _quote():
    now = time.time_ns()
    return SimpleNamespace(
        instrument_id=123, bid_px_00=1_000_000_000, ask_px_00=2_000_000_000,
        ts_event=now, ts_recv=now, ts_index=now,
    )


def _streamer(monkeypatch):
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX"])
    streamer = stream_module.DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "offline-test-only"
    streamer.replay_minutes = 0
    streamer.live_symbols = [SYMBOL]
    streamer.is_running = True
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **kwargs: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    return streamer


def test_actual_reader_progresses_while_publication_persistence_is_delayed(monkeypatch):
    streamer = _streamer(monkeypatch)
    first_consumed = threading.Event()
    release_second = threading.Event()
    second_stored = threading.Event()
    reader_errors = []
    captured = []
    original_store = streamer._store_quote

    def record_store(symbol, payload):
        original_store(symbol, payload)
        captured.append(dict(payload))
        if len(captured) == 2:
            second_stored.set()

    monkeypatch.setattr(streamer, "_store_quote", record_store)

    class FakeLocalClient:
        def __init__(self, **kwargs):
            pass

        def subscribe(self, **kwargs):
            assert kwargs["symbols"] == [SYMBOL]

        def __iter__(self):
            yield _mapping()
            yield _quote()
            first_consumed.set()
            assert release_second.wait(2.0)
            yield _quote()
            streamer.is_running = False

        def stop(self):
            pass

    monkeypatch.setattr(stream_module.db, "Live", FakeLocalClient)

    def run_reader():
        try:
            streamer._run_live()
        except BaseException as error:
            reader_errors.append(error)

    reader = threading.Thread(target=run_reader, daemon=True)
    reader.start()
    try:
        assert first_consumed.wait(2.0)
        assert streamer.handoff_status == "active"
        with streamer.prediction_publication_guard(
            subscription_epoch_id=streamer.subscription_epoch_id,
            subscription_generation=1,
        ) as allowed:
            assert allowed
            release_second.set()
            progressed_during_publication = second_stored.wait(0.3)
    finally:
        release_second.set()
        reader.join(timeout=2.0)
        streamer.is_running = False
    assert not reader.is_alive()
    assert not reader_errors
    assert progressed_during_publication, "publication persistence blocked the actual quote reader"
    assert streamer.messages_received == 2
    assert [payload["generation"] for payload in captured] == [1, 1]
    assert streamer.active_generation == 1
    assert streamer.provider_queue_full_warnings == 0
    assert streamer.reconnect_attempts == 0


def test_reconnect_stamps_each_quote_with_its_own_client_generation(monkeypatch):
    streamer = _streamer(monkeypatch)
    clients = []
    captured = []
    original_store = streamer._store_quote

    def record_store(symbol, payload):
        original_store(symbol, payload)
        captured.append(dict(payload))

    monkeypatch.setattr(streamer, "_store_quote", record_store)

    class FakeLocalClient:
        def __init__(self, **kwargs):
            self.number = len(clients) + 1
            clients.append(self)

        def subscribe(self, **kwargs):
            assert kwargs["symbols"] == [SYMBOL]

        def __iter__(self):
            yield _mapping()
            yield _quote()
            if self.number == 1:
                raise ConnectionResetError("synthetic reconnect for generation test")
            streamer.is_running = False

        def stop(self):
            pass

    monkeypatch.setattr(stream_module.db, "Live", FakeLocalClient)
    monkeypatch.setattr(stream_module.time, "sleep", lambda seconds: None)
    streamer._run_live()
    assert len(clients) == 2
    assert streamer.messages_received == 2
    assert [payload["generation"] for payload in captured] == [1, 2]
    assert streamer.quotes[SYMBOL]["generation"] == 2
    assert not streamer._quote_is_current(captured[0])
    assert streamer._quote_is_current(captured[1])
    assert streamer._fresh_quote_counts() == {"SPX": 1}
    assert streamer.provider_queue_full_warnings == 0
    assert streamer.reconnect_attempts == 1


def test_publication_barrier_still_serializes_generation_change(monkeypatch):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 4
    streamer.handoff_status = "active"
    attempted = threading.Event()
    completed = threading.Event()

    def change_generation():
        attempted.set()
        streamer.active_generation = 5
        streamer.handoff_status = "warming"
        completed.set()

    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=4,
    ) as allowed:
        assert allowed
        thread = threading.Thread(target=change_generation)
        thread.start()
        assert attempted.wait(1.0)
        assert not completed.wait(0.05)
    thread.join(timeout=1.0)
    assert completed.is_set()
    assert streamer.active_generation == 5
    assert streamer.handoff_status == "warming"
    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=4,
    ) as stale_allowed:
        assert not stale_allowed
