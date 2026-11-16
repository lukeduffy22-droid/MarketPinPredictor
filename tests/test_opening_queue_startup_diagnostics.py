"""Observed SDK startup ordering with actual SDK queues and offline delivery."""
from __future__ import annotations

import gc

from databento.live.client import Live
import pytest

import backend.databento_streamer as stream_module
from tests.test_databento_iterator_startup import _quote, _sdk


def test_diagnostics_prove_queue_enabled_before_first_sdk_delivery(monkeypatch):
    observations = []
    timestamps = iter(range(100, 200))
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: next(timestamps))
    monkeypatch.setattr(stream_module.time, "monotonic_ns", lambda: next(timestamps))
    records = [_quote(i) for i in range(1, 51)]
    with _sdk(records) as (client, queue, trace, _cleanup):
        deliveries = []
        client.add_callback(lambda record: deliveries.append({
            "record": bytes(record),
            "stages_before_delivery": [row["stage"] for row in observations],
        }))
        iterator = stream_module._start_live_iterator(
            client, on_diagnostic=observations.append,
        )
        consumed = list(iterator)
        del iterator
        gc.collect()
        assert not queue.is_enabled()
    assert [bytes(row) for row in consumed] == [bytes(row) for row in records]
    assert trace == [{"start_sent": True, "queue_enabled": True}]
    assert len(deliveries) == 50
    assert all(row["stages_before_delivery"] == [
        "queue_ownership_established", "queue_enable_returned",
        "queue_enabled_before_iterator_start",
    ] for row in deliveries)
    assert [row["stage"] for row in observations] == [
        "queue_ownership_established", "queue_enable_returned",
        "queue_enabled_before_iterator_start", "iterator_start_returned",
        "iterator_ownership_verified",
    ]
    assert observations[0]["queue_enabled"] is False
    assert observations[2]["queue_enabled"] is True
    assert all(row["authority"] == "DIAGNOSTIC_ONLY" for row in observations)
    assert [row["startup_observed_monotonic_ns"] for row in observations] == sorted(
        row["startup_observed_monotonic_ns"] for row in observations
    )
    assert all("startup_observed_at_ns" in row for row in observations)


@pytest.mark.parametrize("failure", [False, True])
def test_observer_failure_does_not_change_startup_or_original_exception(monkeypatch, failure):
    def broken_observer(_event):
        raise ValueError("diagnostics unavailable")

    with _sdk([_quote(1)]) as (client, queue, _trace, _cleanup):
        original = OSError("private transport detail must not be logged")
        if failure:
            def failed_start(_client):
                raise original
            monkeypatch.setattr(Live, "__iter__", failed_start)
            with pytest.raises(OSError) as captured:
                stream_module._start_live_iterator(client, on_diagnostic=broken_observer)
            assert captured.value is original
            assert queue.is_enabled() is False
        else:
            iterator = stream_module._start_live_iterator(client, on_diagnostic=broken_observer)
            assert len(list(iterator)) == 1
            del iterator
            gc.collect()
            assert queue.is_enabled() is False


@pytest.mark.parametrize("state", ["already_streaming", "already_enabled"])
def test_queue_rejection_is_observed_without_altering_foreign_queue(state):
    observations = []
    with _sdk([]) as (client, queue, trace, _cleanup):
        if state == "already_streaming":
            client._session._protocol._started = True
        else:
            queue.enable()
        was_enabled = queue.is_enabled()
        with pytest.raises(RuntimeError, match="DATABENTO_ITERATOR_QUEUE_ALREADY_IN_USE"):
            stream_module._start_live_iterator(client, on_diagnostic=observations.append)
        assert queue.is_enabled() is was_enabled
        assert trace == []
    assert len(observations) == 1
    assert observations[0]["stage"] == "queue_ownership_rejected"
    assert observations[0]["owned_queue"] is False
    assert observations[0]["queue_enabled"] == (None if state == "already_streaming" else True)


def test_failed_start_reports_stage_and_cleanup_without_exception_text(monkeypatch):
    observations = []
    with _sdk([]) as (client, queue, _trace, _cleanup):
        def failed_start(_client):
            raise OSError("secret transport error text")
        monkeypatch.setattr(Live, "__iter__", failed_start)
        with pytest.raises(OSError):
            stream_module._start_live_iterator(client, on_diagnostic=observations.append)
        assert queue.is_enabled() is False
    assert observations[-2]["stage"] == "startup_failed"
    assert observations[-2]["failed_stage"] == "iterator_start"
    assert observations[-2]["error_type"] == "OSError"
    assert observations[-1]["stage"] == "queue_disable_returned_after_failure"
    assert "secret" not in repr(observations)


def test_non_sdk_client_is_explicitly_outside_queue_contract():
    observations = []
    assert list(stream_module._start_live_iterator(
        [1, 2], on_diagnostic=observations.append,
    )) == [1, 2]
    assert observations[0]["stage"] == "not_applicable_non_sdk_client"
    assert observations[0]["queue_contract_supported"] is False


def test_production_reader_binds_startup_observations_to_subscription_generation(monkeypatch, tmp_path):
    from tests.test_databento_iterator_startup import RECEIVE_NS, SPX_SYMBOL, _mapping

    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    monkeypatch.setattr(stream_module, "DATABENTO_REQUIRED_SYMBOLS", ["SPX"])
    streamer = stream_module.DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 0
    streamer.api_key = "offline-test-only"
    streamer.live_symbols = [SPX_SYMBOL]
    streamer.is_running = True
    streamer.schema = "cbbo-1s"
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {
        "state": "regular_session", "subscription_allowed": True,
    })
    monkeypatch.setattr(streamer, "_build_universe", lambda **_: None)
    monkeypatch.setattr(streamer, "_stream_stalled", lambda: False)
    monkeypatch.setattr(streamer, "_subscription_replay_start", lambda: None)
    monkeypatch.setattr(stream_module.time, "time_ns", lambda: RECEIVE_NS + 20_000_000)
    observations = []
    monkeypatch.setattr(streamer, "_opening_timeline_note", lambda kind, **fields:
                        observations.append({"kind": kind, **fields}))

    def progress():
        if streamer.messages_received == 1:
            streamer.is_running = False
    monkeypatch.setattr(streamer, "_mark_stream_progress", progress)
    with _sdk([_mapping(1, SPX_SYMBOL), _quote(1)]) as (client, _queue, _trace, _cleanup):
        client.subscribe = lambda **_: None
        monkeypatch.setattr(stream_module.db, "Live", lambda **_: client)
        streamer._run_live()
    startup = [row for row in observations if row["kind"] == "sdk_iterator_startup"]
    assert streamer.messages_received == 1
    assert len(startup) == 5
    assert all(row["expected_generation"] == 1 for row in startup)
    assert all(row["expected_epoch_id"] == streamer.subscription_epoch_id for row in startup)
    assert startup[2]["details"]["stage"] == "queue_enabled_before_iterator_start"
