"""Offline concurrency proof for lock-bound health and event reads."""

import asyncio
import json
import threading
import time
from types import SimpleNamespace

from fastapi import FastAPI, HTTPException
import httpx
import pytest

from backend.api import bounded_runtime_read as bounded
from backend.api.routers import gex, health, orb, predict


async def _until_set(event):
    async def wait():
        while not event.is_set():
            await asyncio.sleep(0.001)
    await asyncio.wait_for(wait(), timeout=1.0)


def test_health_admission_is_independent_of_saturated_projection_workers(monkeypatch):
    projections = bounded.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.5)
    health_reads = bounded.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.15)
    monkeypatch.setattr(bounded, "runtime_reads", projections)
    monkeypatch.setattr(health, "_runtime_reads", health_reads)
    release = threading.Event()
    entered = [threading.Event(), threading.Event()]

    def blocked(index):
        entered[index].set()
        assert release.wait(3)

    async def run():
        jobs = [asyncio.create_task(projections.run(i, lambda i=i: blocked(i)))
                for i in range(2)]
        try:
            for event in entered:
                await _until_set(event)
            assert await health._read_or_503("live-health", lambda: {"fresh": True}) == {"fresh": True}
            # Independent admission must not bypass an actual health lock stall.
            with pytest.raises(HTTPException) as failure:
                await health._read_or_503("blocked-health", lambda: release.wait(3))
            assert failure.value.status_code == 503
            assert failure.value.detail["usable_for_prediction"] is False
        finally:
            release.set()
            await asyncio.gather(*jobs, return_exceptions=True)

    try:
        asyncio.run(run())
    finally:
        release.set()
        projections.close()
        health_reads.close()


@pytest.fixture
def read_service(monkeypatch):
    service = bounded.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.15)
    monkeypatch.setattr(bounded, "runtime_reads", service)
    monkeypatch.setattr(health, "_runtime_reads", service)
    yield service
    service.close()


def test_timeouts_and_cancellations_keep_capacity_until_actual_worker_completes(read_service):
    release = threading.Event()
    entered = threading.Event()
    calls = []

    def blocked():
        calls.append("blocked")
        entered.set()
        assert release.wait(3.0)
        return "finished"

    async def run():
        first = asyncio.create_task(read_service.run("same", blocked))
        await _until_set(entered)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        failures = await asyncio.gather(
            *(read_service.run("same", blocked) for _ in range(40)),
            return_exceptions=True,
        )
        assert all(isinstance(exc, bounded.RuntimeReadUnavailable) for exc in failures)
        assert all(str(exc) == "RUNTIME_READ_TIMEOUT" for exc in failures)
        for _ in range(20):
            with pytest.raises(bounded.RuntimeReadUnavailable, match="RUNTIME_READ_TIMEOUT"):
                await read_service.run("same", blocked)
        assert calls == ["blocked"]

        second = asyncio.create_task(read_service.run("other", blocked))
        await asyncio.sleep(0.02)
        with pytest.raises(bounded.RuntimeReadUnavailable, match="RUNTIME_READ_BUSY"):
            await read_service.run("third", lambda: pytest.fail("queued excess worker"))
        assert len(calls) == 2
        release.set()
        with pytest.raises(bounded.RuntimeReadUnavailable, match="RUNTIME_READ_TIMEOUT"):
            await second
        assert await read_service.run("fresh", lambda: "new observation") == "new observation"

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_completed_read_is_not_reused_before_delayed_callback_cleanup():
    service = bounded.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.5)
    reader_entered = threading.Event()
    release_reader = threading.Event()
    callback_entered = threading.Event()
    release_callback = threading.Event()
    calls = []
    original_finished = service._finished

    def delayed_finished(key, read):
        if not callback_entered.is_set():
            callback_entered.set()
            assert release_callback.wait(3.0)
        original_finished(key, read)

    service._finished = delayed_finished

    def read_sequence():
        calls.append(len(calls) + 1)
        if len(calls) == 1:
            reader_entered.set()
            assert release_reader.wait(3.0)
        return calls[-1]

    async def run():
        pending_first = asyncio.create_task(service.run("same", read_sequence))
        await _until_set(reader_entered)
        release_reader.set()
        first = await pending_first
        await _until_set(callback_entered)
        second = await service.run("same", read_sequence)
        assert (first, second) == (1, 2)
        assert calls == [1, 2]

    try:
        asyncio.run(run())
    finally:
        release_reader.set()
        release_callback.set()
        service.close()


def test_completed_other_key_releases_capacity_before_delayed_callback_cleanup():
    service = bounded.BoundedRuntimeReads(max_workers=1, timeout_seconds=0.5)
    reader_entered = threading.Event()
    release_reader = threading.Event()
    callback_entered = threading.Event()
    release_callback = threading.Event()
    original_finished = service._finished

    def delayed_finished(key, read):
        callback_entered.set()
        assert release_callback.wait(3.0)
        original_finished(key, read)

    service._finished = delayed_finished

    def first_reader():
        reader_entered.set()
        assert release_reader.wait(3.0)
        return "first"

    async def run():
        pending_first = asyncio.create_task(service.run("completed", first_reader))
        await _until_set(reader_entered)
        release_reader.set()
        assert await pending_first == "first"
        await _until_set(callback_entered)
        waiting = asyncio.create_task(service.run("different", lambda: "second"))
        await asyncio.sleep(0.02)
        with service._lock:
            assert "completed" not in service._reads
            assert "different" in service._reads
        release_callback.set()
        assert await waiting == "second"

    try:
        asyncio.run(run())
    finally:
        release_reader.set()
        release_callback.set()
        service.close()


def test_concurrent_callers_still_coalesce_one_in_flight_read():
    service = bounded.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.5)
    entered = threading.Event()
    release = threading.Event()
    calls = []

    def blocked_read():
        calls.append(threading.get_ident())
        entered.set()
        assert release.wait(3.0)
        return "one-current-snapshot"

    async def run():
        pending = [
            asyncio.create_task(service.run("same", blocked_read))
            for _ in range(20)
        ]
        await _until_set(entered)
        await asyncio.sleep(0.02)
        assert len(calls) == 1
        release.set()
        assert await asyncio.gather(*pending) == ["one-current-snapshot"] * 20

    try:
        asyncio.run(run())
    finally:
        release.set()
        service.close()


@pytest.mark.parametrize("path", [
    "/dashboard/symbol/SPX", "/predict/close-overlay?symbol=SPX", "/ai/predict/SPX",
    "/orb?symbols=SPX", "/v1/orb/SPX",
])
def test_lock_bound_gex_and_prediction_routes_do_not_block_unrelated_requests(read_service, monkeypatch, path):
    release = threading.Event()
    entered = threading.Event()

    def blocked(*_args, **_kwargs):
        entered.set()
        assert release.wait(3.0)
        return {}

    monkeypatch.setattr(gex, "get_streamer", lambda: object())
    monkeypatch.setattr(predict, "get_streamer", lambda: object())
    monkeypatch.setattr(orb, "get_streamer", lambda: object())
    monkeypatch.setattr(orb, "get_market_structure_journal", lambda: object())
    monkeypatch.setattr(orb, "_runtime_context", blocked)
    monkeypatch.setattr(gex.workstation_state_store, "get", lambda _symbol: {"status": "live"})
    monkeypatch.setattr(gex, "runtime_bound_workstation_state", blocked)
    monkeypatch.setattr(predict, "runtime_bound_workstation_state", blocked)
    application = FastAPI()
    for router in (health.router, gex.router, predict.router, orb.router):
        application.include_router(router)

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://offline") as client:
            waiting = asyncio.create_task(client.get(path))
            await _until_set(entered)
            assert (await asyncio.wait_for(client.get("/symbols"), timeout=0.08)).status_code == 200
            response = await waiting
            assert response.status_code == 503
            assert response.json()["detail"]["reason"] == "RUNTIME_READ_TIMEOUT"

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_healthy_symbol_health_and_sse_burst_waits_for_bounded_capacity(read_service, monkeypatch):
    active = 0
    peak = 0
    counter_lock = threading.Lock()

    def briefly_read(result):
        nonlocal active, peak
        with counter_lock:
            active += 1
            peak = max(peak, active)
        try:
            time.sleep(0.015)
            return result
        finally:
            with counter_lock:
                active -= 1

    monkeypatch.setattr(gex, "_symbol_dashboard_payload", lambda symbol: briefly_read({"symbol": symbol}))
    monkeypatch.setattr(health, "_read_live_health_sample", lambda: briefly_read({
        "health": {}, "pipeline": {
            "transport_ready": True, "collection_ready": True,
            "calculation_ready": True, "prediction_pipeline_ok": True,
            "runtime_context_stable": True,
        }, "stream_active": True, "quant_inference_device": "cpu",
        "runtime_controls": {}, "sampled_at_utc": "2026-09-09T13:00:00+00:00",
    }))
    application = FastAPI()
    application.include_router(health.router)
    application.include_router(gex.router)

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://offline") as client:
            streams = [await health.events_live() for _ in range(3)]
            iterators = [stream.body_iterator for stream in streams]
            urls = [f"/dashboard/symbol/{symbol}" for symbol in ("SPX", "NDX", "VIX", "RUT")]
            responses = await asyncio.gather(
                *(client.get(url) for url in [*urls, "/health", "/health/live"]),
                *(anext(iterator) for iterator in iterators),
            )
            assert all(response.status_code == 200 for response in responses[:6])
            assert all(chunk.startswith("event: snapshot\n") for chunk in responses[6:])
            assert 1 <= peak <= 2
            for iterator in iterators:
                await iterator.aclose()

    asyncio.run(run())


def test_orb_collection_and_health_remain_responsive_during_persistence_pressure(
    read_service, monkeypatch
):
    class Streamer:
        symbols = ["SPX", "NDX", "VIX", "RUT"]

        @staticmethod
        def get_subscription_context():
            return {
                "subscription_epoch_id": "a" * 64,
                "subscription_generation": 7,
                "handoff_status": "active",
            }

    class Journal:
        @staticmethod
        def snapshot(symbol, **_kwargs):
            # ORB has its own one-second serial budget. Health must still
            # complete within its shorter budget while the projection runs.
            time.sleep(0.06)
            return {"symbol": symbol, "capture_status": "unavailable"}

    monkeypatch.setattr(orb, "get_streamer", lambda: Streamer())
    monkeypatch.setattr(orb, "get_market_structure_journal", lambda: Journal())
    monkeypatch.setattr(
        health,
        "_read_live_health_sample",
        lambda: {
            "health": {},
            "pipeline": {
                "transport_ready": False,
                "collection_ready": False,
                "calculation_ready": False,
                "prediction_pipeline_ok": False,
                "runtime_context_stable": True,
            },
            "stream_active": False,
            "quant_inference_device": "cpu",
            "runtime_controls": {},
            "sampled_at_utc": "2026-09-14T19:45:16+00:00",
        },
    )
    application = FastAPI()
    application.include_router(health.router)
    application.include_router(orb.router)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application),
            base_url="http://offline",
        ) as client:
            started = time.perf_counter()
            pending_orb = asyncio.create_task(client.get("/v1/orb"))
            health_response = await client.get("/health/live")
            health_elapsed = time.perf_counter() - started
            orb_response = await pending_orb
            elapsed = time.perf_counter() - started
        assert orb_response.status_code == 200
        assert health_response.status_code == 200
        assert orb_response.json()["runtime_context_stable"] is True
        assert set(orb_response.json()["symbols"]) == {"SPX", "NDX", "VIX", "RUT"}
        assert health_elapsed < 0.15
        assert elapsed < 1.0

    asyncio.run(run())


def test_startup_health_progresses_during_coalesced_sse_burst(read_service, monkeypatch):
    import torch

    release = threading.Event()
    entered = threading.Event()
    calls = []

    def blocked():
        calls.append(threading.get_ident())
        entered.set()
        assert release.wait(3.0)
        return {}

    monkeypatch.setattr(health, "_read_live_health_sample", blocked)
    monkeypatch.setattr(health, "get_streamer", lambda: SimpleNamespace(is_running=False))
    monkeypatch.setattr(health, "get_inference_engine", lambda: SimpleNamespace(get_stats=lambda: {"models_loaded": []}))
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    application = FastAPI()
    application.include_router(health.router)

    async def run():
        streams = [await health.events_live() for _ in range(20)]
        iterators = [stream.body_iterator for stream in streams]
        pending = [asyncio.create_task(anext(iterator)) for iterator in iterators]
        await _until_set(entered)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://offline") as client:
            startup = await asyncio.wait_for(client.get("/"), timeout=0.08)
        assert startup.status_code == 200
        assert startup.json()["streaming_active"] is False
        assert all(not task.done() for task in pending)
        chunks = await asyncio.gather(*pending)
        assert all(chunk.startswith("event: unavailable\n") for chunk in chunks)
        assert len(calls) == 1
        release.set()
        for iterator in iterators:
            await iterator.aclose()

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_worker_failure_is_unavailable_and_releases_capacity_without_leaking_details(read_service):
    def failed():
        raise ValueError("private worker internals")

    async def run():
        with pytest.raises(HTTPException) as captured:
            await bounded.read_runtime_or_503("failed", failed)
        assert captured.value.status_code == 503
        assert captured.value.detail["reason"] == "RUNTIME_READ_FAILED"
        assert "private" not in json.dumps(captured.value.detail)
        assert await read_service.run("failed", lambda: "recovered") == "recovered"

    asyncio.run(run())


def test_unrelated_http_request_progresses_during_blocked_health_and_universe(read_service, monkeypatch):
    release = threading.Event()
    entered = threading.Event()
    calls = []

    class Streamer:
        is_running = False

        def get_health(self):
            calls.append(threading.get_ident())
            entered.set()
            assert release.wait(3.0)
            return {"provider": "databento", "symbols_requested": ["SPX"]}

        def get_all_latest(self):
            return {}

    streamer = Streamer()
    monkeypatch.setattr(health, "get_streamer", lambda: streamer)
    monkeypatch.setattr(gex, "get_streamer", lambda: streamer)
    monkeypatch.setattr(health, "inference_device", lambda: "cpu")
    monkeypatch.setattr(health, "runtime_control_health", lambda: {})
    application = FastAPI()
    application.include_router(health.router)
    application.include_router(gex.router)

    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://offline") as client:
            blocked_request = asyncio.create_task(client.get("/health/live"))
            await _until_set(entered)
            symbols = await asyncio.wait_for(client.get("/symbols"), timeout=0.08)
            assert symbols.status_code == 200
            assert not blocked_request.done()
            coalesced = asyncio.create_task(client.get("/health"))
            universe = asyncio.create_task(client.get("/databento/universe"))
            responses = await asyncio.gather(blocked_request, coalesced, universe)
            assert all(response.status_code == 503 for response in responses)
            for response in responses:
                detail = response.json()["detail"]
                assert detail["reason"] == "RUNTIME_READ_TIMEOUT"
                assert detail["usable_for_prediction"] is False
                assert detail["prediction_pipeline_ok"] is False
                assert detail["checked_at_utc"]
            assert len(calls) == 2  # One shared health sample; one universe sample.
            assert all(thread_id != threading.get_ident() for thread_id in calls)
            release.set()
            await asyncio.sleep(0.03)
            recovered = await client.get("/health/live")
            assert recovered.status_code == 200
            assert recovered.json()["prediction_pipeline_ok"] is False
            assert recovered.json()["sampled_at_utc"]

    try:
        asyncio.run(run())
    finally:
        release.set()


@pytest.mark.parametrize("stream_kind", ["health", "workstation"])
def test_sse_reports_unavailable_without_advancing_cursor_when_runtime_read_waits(read_service, monkeypatch, stream_kind):
    release = threading.Event()
    entered = threading.Event()
    cursors = []

    def blocked(*_args):
        entered.set()
        assert release.wait(3.0)
        return {"sequence": 9, "event_type": "update", "states": []}

    monkeypatch.setattr(health, "_read_live_health_sample", blocked)
    monkeypatch.setattr(health, "get_streamer", lambda: object())
    monkeypatch.setattr(health, "runtime_bound_workstation_event", blocked)

    class Store:
        def events_after(self, cursor):
            cursors.append(cursor)
            return [{"sequence": 9, "event_type": "update", "states": []}]

    monkeypatch.setattr(health, "workstation_state_store", Store())

    async def run():
        response = (
            await health.events_live() if stream_kind == "health"
            else await health.workstation_events(after_sequence=8, last_event_id=None)
        )
        iterator = response.body_iterator
        pending = asyncio.create_task(anext(iterator))
        await _until_set(entered)
        assert (await health.get_available_symbols())["symbols"]
        chunk = await asyncio.wait_for(pending, timeout=0.3)
        assert chunk.startswith("event: unavailable\n")
        assert "id:" not in chunk
        payload = json.loads(chunk.split("data: ", 1)[1])
        assert payload["usable_for_prediction"] is False
        assert "states" not in payload and "symbol_status" not in payload
        if stream_kind == "workstation":
            assert cursors == [8]
        release.set()
        await iterator.aclose()

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_workstation_snapshot_moves_runtime_binding_off_event_loop(read_service, monkeypatch):
    release = threading.Event()
    entered = threading.Event()
    monkeypatch.setattr(health, "get_streamer", lambda: object())

    def blocked(*_args):
        entered.set()
        assert release.wait(3.0)
        return {}

    monkeypatch.setattr(health, "runtime_bound_workstation_event", blocked)

    async def run():
        snapshot = asyncio.create_task(health.workstation_event_snapshot())
        await _until_set(entered)
        assert (await health.get_available_symbols())["symbols"]
        with pytest.raises(HTTPException) as captured:
            await snapshot
        assert captured.value.status_code == 503
        assert captured.value.detail["reason"] == "RUNTIME_READ_TIMEOUT"

    try:
        asyncio.run(run())
    finally:
        release.set()


def test_idle_workstation_polling_preserves_heartbeat_cadence_and_event_cursor(read_service, monkeypatch):
    cursors = []

    class Store:
        def events_after(self, cursor):
            cursors.append(cursor)
            if len(cursors) < 3:
                return []
            return [{"sequence": 9, "event_type": "update", "states": []}]

        def heartbeat_event(self):
            pytest.fail("Idle polling must not emit a heartbeat before ten seconds")

    monkeypatch.setattr(health, "workstation_state_store", Store())
    monkeypatch.setattr(health, "get_streamer", lambda: object())

    async def run():
        response = await health.workstation_events(after_sequence=8, last_event_id=None)
        iterator = response.body_iterator
        try:
            chunk = await asyncio.wait_for(anext(iterator), timeout=2.0)
            assert chunk.startswith("id: 9\nevent: update\n")
            assert cursors == [8, 8, 8]
        finally:
            await iterator.aclose()

    asyncio.run(run())
