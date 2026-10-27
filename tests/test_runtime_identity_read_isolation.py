"""Live reads remain independent of a slow forecast commit, with identity fencing."""

import asyncio
from datetime import datetime, timedelta, timezone
import threading

from fastapi import FastAPI
import httpx
import pytest

from backend.api import bounded_runtime_read
from backend.api.helpers import current_runtime_binding
from backend.api.routers import gex, predict
from backend.workstation import WorkstationStateStore, build_workstation_state
from tests.test_databento_reader_publication_lock import _streamer
from tests.test_workstation_state import _passport, _payload, _prediction


@pytest.mark.parametrize("read", ["context", "generation", "handoff", "health"])
def test_live_identity_reads_complete_while_forecast_persistence_is_blocked(monkeypatch, read):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    completed = threading.Event()
    results = []
    errors = []

    def reader():
        try:
            if read == "context":
                results.append(streamer.get_subscription_context())
            elif read == "generation":
                results.append(streamer.active_generation)
            elif read == "handoff":
                results.append(streamer.handoff_status)
            else:
                results.append(streamer.get_health())
        except BaseException as exc:
            errors.append(exc)
        finally:
            completed.set()

    worker = threading.Thread(target=reader, daemon=True)
    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=1,
    ) as allowed:
        assert allowed
        worker.start()
        completed_during_persistence = completed.wait(0.5)
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert not errors
    assert completed_during_persistence, f"{read} waited for forecast persistence"
    if read == "context":
        assert results[0] == {
            "subscription_epoch_id": streamer.subscription_epoch_id,
            "subscription_generation": 1,
            "handoff_status": "active",
        }
    elif read == "generation":
        assert results == [1]
    elif read == "handoff":
        assert results == ["active"]
    else:
        assert results[0]["active_generation"] == 1
        assert results[0]["handoff_status"] == "active"


def test_generation_change_still_waits_for_commit_and_invalidates_old_payload(monkeypatch):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    payload = {
        "subscription_epoch_id": streamer.subscription_epoch_id,
        "subscription_generation": 1,
    }
    attempted = threading.Event()
    changed = threading.Event()

    def handoff():
        attempted.set()
        streamer.active_generation = 2
        streamer.handoff_status = "warming"
        changed.set()

    worker = threading.Thread(target=handoff, daemon=True)
    with streamer.prediction_publication_guard(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=1,
    ) as allowed:
        assert allowed
        worker.start()
        assert attempted.wait(1)
        assert not changed.wait(0.05)
        assert current_runtime_binding(streamer, payload)["eligible"] is True
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert changed.is_set()
    binding = current_runtime_binding(streamer, payload)
    assert binding["eligible"] is False
    assert "SUBSCRIPTION_GENERATION_MISMATCH" in binding["failure_reasons"]
    assert "ACTIVE_RUNTIME_HANDOFF_NOT_ACTIVE:warming" in binding["failure_reasons"]


@pytest.mark.parametrize("failure", [None, "stale", "generation", "invalid"])
def test_api_reads_during_commit_preserve_freshness_generation_and_validation_gates(monkeypatch, failure):
    streamer = _streamer(monkeypatch)
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    payload = _payload(
        subscription_epoch_id=streamer.subscription_epoch_id,
        subscription_generation=2 if failure == "generation" else 1,
    )
    if failure == "stale":
        payload["timestamp"] = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    if failure == "invalid":
        payload.update(validation_is_valid=False, gamma_excluded_from_model=True)
    store = WorkstationStateStore()
    store.publish(build_workstation_state(
        symbol="SPX", payload=payload, prediction=_prediction(), passport=_passport(),
    ))
    service = bounded_runtime_read.BoundedRuntimeReads(max_workers=2, timeout_seconds=0.15)
    monkeypatch.setattr(bounded_runtime_read, "runtime_reads", service)
    for module in (gex, predict):
        monkeypatch.setattr(module, "get_streamer", lambda: streamer)
        monkeypatch.setattr(module, "workstation_state_store", store)
    app = FastAPI()
    app.include_router(gex.router)
    app.include_router(predict.router)

    async def requests():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            return await asyncio.gather(
                client.get("/v1/workstation/state/SPX"),
                client.get("/predict/SPX"),
            )

    try:
        with streamer.prediction_publication_guard(
            subscription_epoch_id=streamer.subscription_epoch_id,
            subscription_generation=1,
        ) as allowed:
            assert allowed
            state_response, prediction_response = asyncio.run(requests())
        assert state_response.status_code == 200
        state = state_response.json()
        if failure is None:
            assert state["status"] == "live"
            assert prediction_response.status_code == 200
            assert prediction_response.json()["predicted_close"] == 7511.0
        else:
            assert state["status"] != "live"
            assert state["health"]["usable_for_prediction"] is False
            assert prediction_response.status_code == 503
            assert "RUNTIME_READ_TIMEOUT" not in prediction_response.text
    finally:
        service.close()
