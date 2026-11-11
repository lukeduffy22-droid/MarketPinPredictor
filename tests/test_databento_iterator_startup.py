"""SDK startup must retain the first burst without changing admission gates."""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import contextmanager
import gc
from threading import Lock
from types import SimpleNamespace

import databento_dbn as dbn
from databento.live.client import Live
from databento.live.session import DBNQueue, LiveSession, SessionMetadata, _SessionProtocol
import pytest

import backend.databento_streamer as stream_module


RECEIVE_NS = 1793973198 * 10**9
SPX_SYMBOL = "SPXW  261106C07350000"
NDX_SYMBOL = "NDXP  261106C26000000"


def _quote(instrument):
    return dbn.CBBOMsg(
        rtype=dbn.RType.CBBO_1S, publisher_id=1, instrument_id=instrument,
        ts_event=dbn.UNDEF_TIMESTAMP, price=dbn.UNDEF_PRICE, size=0,
        side=dbn.Side.NONE, ts_recv=RECEIVE_NS,
        levels=dbn.ConsolidatedBidAskPair(
            bid_px=1_000_000_000, ask_px=2_000_000_000,
        ),
    )


def _mapping(instrument, symbol):
    return dbn.SymbolMappingMsg(
        publisher_id=1, instrument_id=instrument, ts_event=RECEIVE_NS,
        stype_in=dbn.SType.RAW_SYMBOL, stype_in_symbol=symbol,
        stype_out=dbn.SType.RAW_SYMBOL, stype_out_symbol=symbol,
        start_ts=RECEIVE_NS - 60 * 10**9, end_ts=RECEIVE_NS + 60 * 10**9,
    )


def _clock():
    # Constructor-free collector: no directories, credentials or app lifecycle.
    collector = stream_module.DatabentoGammaStreamer.__new__(
        stream_module.DatabentoGammaStreamer,
    )
    collector.schema = "cbbo-1s"
    collector._clock_evidence_lock = Lock()
    collector._receive_lag_window = deque(maxlen=2048)
    collector._clock_record_keys = {}
    collector._active_generation = 7
    collector._clock_replay_pending = False
    return collector


def _start(client):
    # Before the correction this deliberately runs the production expression.
    return getattr(stream_module, "_start_live_iterator", iter)(client)


@contextmanager
def _sdk(records, *, failure=None, close_after_burst=True):
    # The SDK creates Futures from the current thread's loop. Earlier callers
    # of asyncio.run may have cleared it; own a loop for the entire fixture,
    # including construction failures, then restore the caller's loop.
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None
    real_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(real_loop)
    try:
        with _sdk_session(
            records, real_loop=real_loop, failure=failure,
            close_after_burst=close_after_burst,
        ) as sdk:
            yield sdk
    finally:
        real_loop.close()
        asyncio.set_event_loop(
            previous_loop if previous_loop is not None and not previous_loop.is_closed() else None
        )


@contextmanager
def _sdk_session(records, *, real_loop, failure=None, close_after_burst=True):
    trace = []
    cleanup = {"closed": 0, "terminated": 0, "stopped": 0}

    class ImmediateLoop:
        def call_soon_threadsafe(self, callback, *args):
            # Legal ordering: the receive thread processes the first burst
            # before LiveSession.start returns to the consumer thread.
            callback(*args)

        def create_task(self, coroutine):
            coroutine.close()  # No real heartbeat task or provider connection.
            return SimpleNamespace(cancel=lambda: None)

        def __getattr__(self, name):
            return getattr(real_loop, name)

    loop = ImmediateLoop()
    session = LiveSession(loop, api_key="offline-test-only-not-a-key")
    protocol = _SessionProtocol(
        api_key="offline-test-only-not-a-key", dataset="OPRA.PILLAR",
        dbn_queue=session._dbn_queue, user_streams=session._user_streams,
        user_callbacks=session._user_callbacks,
        loop=loop, metadata=SessionMetadata(), heartbeat_interval_s=5,
    )

    class InMemoryTransport:
        def write(self, _data):
            trace.append({"start_sent": True,
                          "queue_enabled": session._dbn_queue.is_enabled()})
            if failure:
                raise failure
            for record in records:
                protocol.received_record(record)
            if close_after_burst:
                protocol.disconnected.set_result(None)

        def is_reading(self):
            return True

    transport = InMemoryTransport()
    protocol.connection_made(transport)
    session._protocol = protocol
    session._transport = transport
    session._dataset = "OPRA.PILLAR"
    client = Live.__new__(Live)  # No SDK constructor/thread/socket.
    client._session = session
    client.block_for_close = lambda **_: cleanup.__setitem__(
        "closed", cleanup["closed"] + 1,
    )
    client.terminate = lambda: cleanup.__setitem__(
        "terminated", cleanup["terminated"] + 1,
    )
    client.stop = lambda: cleanup.__setitem__("stopped", cleanup["stopped"] + 1)
    yield client, session._dbn_queue, trace, cleanup


def _consume_clock(records, *, replay=False, generation=7, unsafe=False):
    collector = _clock()
    collector._clock_replay_pending = replay
    with _sdk(records) as (client, queue, trace, cleanup):
        iterator = _start(client)
        consumed = list(iterator)
        for record in consumed:
            collector._record_processing_clock_sample(
                record,
                processed_at_ns=RECEIVE_NS + (
                    -1_000_000_000 if unsafe else 20_000_000
                ),
                generation=generation,
            )
        del iterator
        gc.collect()
        assert queue.is_enabled() is False
        assert cleanup["closed"] == 1
    return consumed, collector._processing_clock_telemetry(), trace


def test_first_fifty_native_records_survive_actual_sdk_startup():
    records = [_quote(i) for i in range(1, 51)]
    consumed, telemetry, trace = _consume_clock(records)
    assert [bytes(record) for record in consumed] == [
        bytes(record) for record in records
    ]
    assert trace == [{"start_sent": True, "queue_enabled": True}]
    assert telemetry["sample_count"] == telemetry["minimum_samples"] == 50
    assert telemetry["status"] == "synchronized"
    assert telemetry["establishes_opening_reference"] is False


def test_first_mapping_replay_boundary_and_only_ndx_quote_are_not_lost():
    records = [
        _mapping(1, SPX_SYMBOL),
        _mapping(2, NDX_SYMBOL),
        dbn.SystemMsg(ts_event=RECEIVE_NS, msg="Replay completed", code=dbn.SystemCode.REPLAY_COMPLETED),
        _quote(2),
    ]
    collector = _clock()
    collector._clock_replay_pending = True
    mappings = {}
    families = set()
    with _sdk(records) as (client, queue, trace, _cleanup):
        iterator = _start(client)
        consumed = list(iterator)
        for record in consumed:
            if isinstance(record, dbn.SymbolMappingMsg):
                mappings[record.instrument_id] = record.stype_out_symbol
            elif isinstance(record, dbn.SystemMsg) and record.code == 3:
                collector._clock_replay_pending = False
            else:
                collector._record_processing_clock_sample(
                    record, processed_at_ns=RECEIVE_NS + 20_000_000,
                    generation=7,
                )
                families.add(mappings[record.instrument_id].strip().split()[0])
        del iterator
        gc.collect()
        assert not queue.is_enabled()
    assert [bytes(row) for row in consumed] == [bytes(row) for row in records]
    assert mappings == {1: SPX_SYMBOL, 2: NDX_SYMBOL}
    assert families == {"NDXP"}
    assert collector._clock_replay_pending is False
    assert collector._processing_clock_telemetry()["sample_count"] == 1
    assert trace[0]["queue_enabled"] is True


@pytest.mark.parametrize(
    ("count", "replay", "generation", "unsafe", "samples", "status"),
    [
        (49, False, 7, False, 49, "unknown"),
        (50, True, 7, False, 0, "unknown"),
        (50, False, 6, False, 0, "unknown"),
        (50, False, 7, True, 50, "unsynchronized"),
    ],
)
def test_retention_does_not_weaken_provider_clock_gates(
    count, replay, generation, unsafe, samples, status,
):
    records = [_quote(i) for i in range(1, count + 1)]
    consumed, telemetry, _trace = _consume_clock(
        records, replay=replay, generation=generation, unsafe=unsafe,
    )
    assert len(consumed) == count
    assert telemetry["minimum_samples"] == 50
    assert telemetry["sample_count"] == samples
    assert telemetry["status"] == status
    assert telemetry["establishes_opening_reference"] is False


def test_non_sdk_double_keeps_ordinary_iteration():
    class LocalDouble:
        def __iter__(self):
            return iter(("offline", "only"))
    assert list(_start(LocalDouble())) == ["offline", "only"]


@pytest.mark.parametrize("state", ["already_streaming", "already_enabled"])
def test_active_or_foreign_queue_is_not_modified(monkeypatch, state):
    with _sdk([]) as (client, queue, _trace, _cleanup):
        started = []
        monkeypatch.setattr(client, "start", lambda: started.append(True))
        if state == "already_streaming":
            client._session._protocol._started = True
        else:
            queue.enable()
        enabled_before = queue.is_enabled()
        with pytest.raises(RuntimeError, match="DATABENTO_ITERATOR_QUEUE"):
            _start(client)
        assert started == []
        assert queue.is_enabled() is enabled_before


@pytest.mark.parametrize("missing", [
    "session", "queue", "is_streaming", "enable", "disable", "is_enabled",
])
def test_private_contract_drift_fails_before_start(monkeypatch, missing):
    with _sdk([]) as (client, queue, _trace, _cleanup):
        started = []
        monkeypatch.setattr(client, "start", lambda: started.append(True))
        if missing == "session":
            client._session = None
        elif missing == "queue":
            client._session._dbn_queue = None
        elif missing == "is_streaming":
            monkeypatch.setattr(client._session, "is_streaming", None)
        else:
            monkeypatch.setattr(queue, missing, None)
        with pytest.raises(RuntimeError, match="DATABENTO_ITERATOR_QUEUE"):
            _start(client)
        assert started == []


def test_failed_iterator_start_disables_only_the_armed_queue(monkeypatch):
    with _sdk([]) as (client, queue, _trace, _cleanup):
        def failed_iteration(_client):
            assert queue.is_enabled()
            raise OSError("offline simulated start failure")
        monkeypatch.setattr(Live, "__iter__", failed_iteration)
        with pytest.raises(OSError, match="simulated start failure"):
            _start(client)
        assert queue.is_enabled() is False


def test_partial_enable_failure_disables_owned_queue(monkeypatch):
    with _sdk([]) as (client, queue, _trace, _cleanup):
        def failed_enable():
            DBNQueue.enable(queue)
            raise OSError("offline enable failure")
        monkeypatch.setattr(queue, "enable", failed_enable)
        with pytest.raises(OSError, match="enable failure"):
            _start(client)
        assert queue.is_enabled() is False


def test_iterator_ownership_drift_disables_owned_queue(monkeypatch):
    with _sdk([]) as (client, queue, _trace, _cleanup):
        monkeypatch.setattr(Live, "__iter__", lambda _client: iter(()))
        with pytest.raises(RuntimeError, match="DATABENTO_ITERATOR_QUEUE"):
            _start(client)
        assert queue.is_enabled() is False



@pytest.mark.parametrize("include_ndx", [True, False])
def test_actual_reader_preserves_startup_records_and_requires_ndx(
    monkeypatch, tmp_path, include_ndx,
):
    """Real SDK start/iteration feeds the real application reader, offline."""
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX", "NDX"])
    streamer = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    streamer.active_generation = 0
    streamer.api_key = "offline-test-only"
    streamer.live_symbols = [SPX_SYMBOL, NDX_SYMBOL]
    streamer.is_running = True
    streamer.schema = "cbbo-1s"
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **_: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    monkeypatch.setattr(
        streamer, "_subscription_replay_start",
        lambda: "2026-11-06T13:51:18+00:00",
    )
    monkeypatch.setattr(
        stream_module.time, "time_ns", lambda: RECEIVE_NS + 20_000_000,
    )
    mapping_symbols = [
        NDX_SYMBOL if include_ndx and instrument == 50 else SPX_SYMBOL
        for instrument in range(1, 51)
    ]
    records = [
        _mapping(instrument, symbol)
        for instrument, symbol in enumerate(mapping_symbols, 1)
    ]
    records += [dbn.SystemMsg(
        ts_event=RECEIVE_NS, msg="Replay completed",
        code=dbn.SystemCode.REPLAY_COMPLETED,
    )]
    # The sole NDX observation, when present, arrives in this initial burst.
    records += [_quote(instrument) for instrument in range(1, 51)]
    with _sdk(records) as (client, queue, trace, cleanup):
        subscriptions = []
        client.subscribe = lambda **options: subscriptions.append(options)
        monkeypatch.setattr(stream_module.db, "Live", lambda **_: client)

        def progress():
            if streamer.messages_received == 50:
                streamer.is_running = False
        monkeypatch.setattr(streamer, "_mark_stream_progress", progress)
        streamer._run_live()

        assert streamer.messages_received == 50
        assert streamer.quote_records_seen == 50
        assert streamer.unmapped_quote_records == 0
        assert len(streamer.symbol_mappings) == 50
        assert streamer._clock_replay_pending is False
        telemetry = streamer._processing_clock_telemetry()
        assert telemetry["sample_count"] == 50
        assert telemetry["minimum_samples"] == 50
        assert telemetry["status"] == "synchronized"
        assert streamer.handoff_status == ("active" if include_ndx else "warming")
        assert ("NDX" in streamer._fresh_market_observations) is include_ndx
        assert (NDX_SYMBOL in streamer.quotes) is include_ndx
        assert trace == [{"start_sent": True, "queue_enabled": True}]
        assert subscriptions[0]["schema"] == "cbbo-1s"
        assert subscriptions[0]["start"] == "2026-11-06T13:51:18+00:00"
        assert queue.is_enabled() is False
        assert cleanup["closed"] >= 1
        assert cleanup["stopped"] == 1
        assert streamer.last_client_close_status == "sdk_close_barrier_returned"
