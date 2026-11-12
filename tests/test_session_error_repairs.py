"""Offline regressions for failures observed during the October 1 session."""
import asyncio
from contextlib import suppress
from datetime import timedelta
import threading
import time
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest
from sqlalchemy.exc import OperationalError

from app.utils.gamma_wall_evidence import format_gamma_walls, normalize_gamma_walls
from backend.api import lifecycle
from backend.closing_tape import live_recorder
from tests.test_closing_tape_live_recorder import _feed, HarmlessRecord
from tests.test_workstation_state import _payload, _ActiveStreamer, _passport


def test_gamma_wall_table_serializes_mixed_known_and_missing_sides():
    source = pd.DataFrame([
        {"strike": 30500, "call_gex": 40, "put_gex": None},
        {"strike": 30525, "call_gex": 50, "put_gex": 20},
        {"strike": 30550, "call_gex": 30, "put_gex": 0, "put_calculated_contracts": 1},
    ])
    original = source.copy(deep=True)
    table = pa.Table.from_pandas(format_gamma_walls(source, lambda x: f"{x:.2f}"))
    assert table["put_gex"].to_pylist() == ["N/A", "20.00", "0.00"]
    assert table["Total GEX"].to_pylist() == ["N/A", "70.00", "30.00"]
    pd.testing.assert_frame_equal(source, original)
    assert pd.isna(normalize_gamma_walls(source).loc[0, "put_gex"])


def test_frozen_unready_prefix_does_not_wait_for_impossible_future_evidence(tmp_path, monkeypatch):
    feed = _feed(tmp_path)
    feed.analysis_pause_target = feed.processed_sequence = 1
    feed.worker_paused = True
    monkeypatch.setattr(feed.catalog, "update_feed_status", lambda *args: None)
    monkeypatch.setattr(live_recorder.time, "sleep", lambda _: pytest.fail("waiting on frozen evidence"))
    assert feed.wait_for_analysis_ready(timeout_seconds=30) is False
    assert any("historical replay has not completed" in str(x) for x in feed.aggregator.derived_gaps)


def test_readiness_exception_releases_callback_pause_and_prevents_rearming(tmp_path, monkeypatch):
    feed = _feed(tmp_path)
    feed.analysis_pause_target = 0
    recorder = live_recorder.StreamingSessionRecorder(feed.config)
    recorder.feeds = [feed]
    monkeypatch.setattr(feed, "wait_for_analysis_ready", lambda **_: (_ for _ in ()).throw(OSError("catalog locked")))
    monkeypatch.setattr(recorder, "write_status", lambda: None)
    monkeypatch.setattr(live_recorder, "utcnow", lambda: feed.config.analysis_due_utc + timedelta(seconds=1))
    recorder._analysis()
    assert "catalog locked" in recorder.analysis_error
    assert feed.analysis_pause_target is None
    feed._enqueue_record(HarmlessRecord())
    assert feed.analysis_pause_target is None


def test_capture_only_feed_continues_processing_after_analysis_deadline(tmp_path, monkeypatch):
    feed = _feed(tmp_path)
    feed.analysis_enabled = False
    monkeypatch.setattr(live_recorder, "utcnow", lambda: feed.config.analysis_due_utc + timedelta(seconds=1))
    feed._enqueue_record(HarmlessRecord())
    worker = threading.Thread(target=feed._aggregate_worker)
    worker.start()
    try:
        with feed.sequence_condition:
            assert feed.sequence_condition.wait_for(lambda: feed.processed_sequence == 1, timeout=2)
        assert feed.analysis_pause_target is None
        assert feed.aggregator.records_seen == 1
    finally:
        feed.worker_stop.set()
        feed.release_analysis_snapshot()
        worker.join(timeout=2)
    assert not worker.is_alive()


@pytest.mark.parametrize("failures,expected", [(1, 2), (10, 3)])
def test_same_revision_retries_busy_passport_with_bounded_budget(monkeypatch, failures, expected):
    monkeypatch.setattr(lifecycle, "PUBLICATION_RETRY_SECONDS", 0.05)
    payload = _payload()
    calls = []
    class Coordinator:
        publication_retry_symbols = set()
        def process_payload(self, value):
            calls.append(value)
            if len(calls) <= failures:
                self.publication_retry_symbols.add("SPX")
            else:
                self.publication_retry_symbols.clear()
    async def exercise():
        task = asyncio.create_task(lifecycle.run_workstation_state_loop(
            SimpleNamespace(get_all_latest=lambda: {"SPX": payload}), Coordinator(), poll_seconds=0.05))
        try:
            deadline = time.monotonic() + 2
            while len(calls) < expected and time.monotonic() < deadline:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.18)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
    asyncio.run(exercise())
    assert len(calls) == expected
    assert all(row == payload for row in calls)


def test_only_sqlite_contention_marks_passport_retry_and_success_clears_it():
    coordinator = lifecycle.WorkstationPredictionCoordinator(
        streamer=_ActiveStreamer(), shadow_research=None, capture_seconds=60)
    for exc in (ValueError("bad contract"), OperationalError("", {}, Exception("no such table"))):
        coordinator._remember_retryable_failure(exc, "SPX")
    assert not coordinator.publication_retry_symbols
    coordinator._remember_retryable_failure(OperationalError("", {}, Exception("database is locked")), "SPX")
    assert coordinator.publication_retry_symbols == {"SPX"}
    coordinator._remember_passport(symbol="SPX", data=_payload(), passport=_passport())
    assert not coordinator.publication_retry_symbols


def test_rejected_existing_model_is_not_reported_as_missing(tmp_path, monkeypatch, caplog):
    from backend import inference
    (tmp_path / "gamma_model_SPX.pt").write_bytes(b"rejected fixture")
    engine = SimpleNamespace(
        device="cuda:0",
        models={},
        load_model=lambda *_: False,
    )
    monkeypatch.setattr(inference, "get_inference_engine", lambda: engine)
    monkeypatch.setattr(inference, "MODELS_DIR", tmp_path)
    assert inference.load_all_models(enabled=True) == []
    assert "SPX but none passed loading validation" in caplog.text
    assert "no gamma_model_*.pt artifacts are installed" not in caplog.text
