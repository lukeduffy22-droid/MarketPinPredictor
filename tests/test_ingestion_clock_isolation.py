"""The live reader must not queue behind unrelated prediction persistence."""
import threading
from types import SimpleNamespace

from backend.databento_streamer import DatabentoGammaStreamer
from tests.test_orb_cbbo_capture import _record


def test_clock_sampling_progresses_while_prediction_publication_is_blocked():
    streamer = DatabentoGammaStreamer(['SPX'])
    streamer.active_generation = 7
    recv = 1790775001000000000
    entered, completed = threading.Event(), threading.Event()
    accepted = []

    def consume():
        entered.set()
        for i in range(50):
            accepted.append(streamer._record_processing_clock_sample(
                _record(i + 1, recv), processed_at_ns=recv + 1, generation=7))
        completed.set()

    with streamer._prediction_publication_lock:
        reader = threading.Thread(target=consume)
        reader.start()
        assert entered.wait(1)
        progressed_under_contention = completed.wait(0.25)
    reader.join(2)
    assert not reader.is_alive()
    assert progressed_under_contention
    assert all(accepted) and len(accepted) == 50
    assert streamer._processing_clock_telemetry()['status'] == 'synchronized'


def test_generation_transition_invalidates_clock_evidence():
    streamer = DatabentoGammaStreamer(['SPX'])
    streamer.active_generation = 7
    recv = 1790775001000000000
    for i in range(50):
        assert streamer._record_processing_clock_sample(_record(i + 1, recv), processed_at_ns=recv + 1, generation=7)
    streamer.active_generation = 8
    assert streamer._processing_clock_telemetry()['status'] == 'unknown'
    assert not streamer._record_processing_clock_sample(_record(99, recv), processed_at_ns=recv + 1, generation=7)
    assert streamer._record_processing_clock_sample(_record(99, recv), processed_at_ns=recv + 1, generation=8)
    assert streamer._processing_clock_telemetry()['sample_count'] == 1


def test_actual_cbbo_reader_keeps_metadata_and_progresses_during_publication(monkeypatch):
    import tests.test_databento_reader_publication_lock as reader_cases
    recv = 1790775001000000000
    monkeypatch.setattr(reader_cases, '_quote', lambda: SimpleNamespace(
        instrument_id=123, bid_px_00=1_000_000_000, ask_px_00=2_000_000_000,
        rtype=192, ts_event=recv - 86400 * 10**9, ts_recv=recv, ts_index=recv))
    original = reader_cases._streamer
    created = []
    def create(patch):
        streamer = original(patch)
        streamer.schema = 'cbbo-1s'
        created.append(streamer)
        return streamer
    monkeypatch.setattr(reader_cases, '_streamer', create)
    reader_cases.test_actual_reader_progresses_while_publication_persistence_is_delayed(monkeypatch)
    quote = created[0].quotes[reader_cases.SYMBOL]
    assert quote['provider_timestamp_order_valid'] is True
    assert quote['last_sale_validation_version'] == 'cbbo-last-sale-metadata.v2'
    assert quote['raw_ts_event_ns'] == quote['ts_event_ns'] == recv - 86400 * 10**9

