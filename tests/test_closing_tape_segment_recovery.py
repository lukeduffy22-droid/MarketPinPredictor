from datetime import date, datetime
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

from backend.closing_tape import live_recorder as lr
from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import UTC, build_session_config
from backend.closing_tape.recovery import transport_failure_kind


NOW = datetime(2026, 10, 8, 17, 0, tzinfo=UTC)


def config(tmp_path):
    return build_session_config(tmp_path, trading_day=date(2026, 10, 8), now=NOW,
                                session_id="original", option_parents=["SPX.OPT"])


@pytest.mark.parametrize("error,kind", [
    (TimeoutError("receive timeout"), "transport_timeout"),
    (RuntimeError("Gateway timeout: 15 second(s) since last message"), "gateway_timeout"),
    (OSError("getaddrinfo failed"), "dns_failure"),
    (OSError("[WinError 10054] reset"), "connection_lost"),
    (OSError("disk I/O error"), None),
    (RuntimeError("connection limit reached"), None),
    (RuntimeError("403 unauthorized live data license"), None),
    (RuntimeError("queue full"), None),
    (RuntimeError("raw DBN stream write failed"), None),
])
def test_only_transient_transport_failures_retry(error, kind):
    assert transport_failure_kind(error) == kind


def fake_feeds(monkeypatch, *, failure_kind="gateway_timeout", closed=True, always_fail=False):
    class FakeFeed:
        instances = []

        def __init__(self, cfg, feed, catalog, callback, **kwargs):
            self.config, self.feed, self.catalog = cfg, feed, catalog
            self.kwargs = kwargs
            self.failure = None
            self.transport_failure_kind = None
            self.capture_resources_closed = closed
            self.path = cfg.output_dir / f"{feed.name}.{cfg.session_id}.dbn"
            self.instances.append(self)

        def start(self):
            # The coordinator must retain the exclusive OS lease across retries.
            with pytest.raises(RuntimeError, match="already running"):
                with lr.ProcessFileLock(self.config.lock_path):
                    pass
            with self.path.open("xb") as stream:
                stream.write(self.config.session_id.encode())
            self.catalog.register_feed(self.config.session_id, self.feed, self.path)
            first = len(self.instances) == 1
            if first or always_fail:
                self.failure = "original transport error"
                self.transport_failure_kind = failure_kind
            self.catalog.update_feed_status(self.config.session_id, self.feed.name, {
                "status": "incomplete", "complete": 0, "ended_at_utc": NOW.isoformat(),
                "file_bytes": self.path.stat().st_size,
                "sha256": hashlib.sha256(self.path.read_bytes()).hexdigest(),
                "error": self.failure or "live-only partial segment",
            })

        def is_alive(self):
            return False

        def join(self):
            pass

        def _status_snapshot(self):
            return {"slow_reader_warnings": 0, "provider_error_count": 0}

    monkeypatch.setattr(lr, "FeedThread", FakeFeed)
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    monkeypatch.setattr(lr, "RECOVERY_DELAYS_SECONDS", (0.0, 0.0))
    monkeypatch.setattr(lr.shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * 1024**3))
    return FakeFeed


def test_recovery_is_new_partial_session_and_preserves_original(tmp_path, monkeypatch):
    feeds = fake_feeds(monkeypatch)
    checks = []
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=True, parquet=False,
        recovery_guard=lambda: checks.append(True) or {"allowed": True})
    assert recorder.run() == 2
    assert len(feeds.instances) == 2
    old, new = feeds.instances
    assert old.path.read_bytes() == b"original"
    assert new.path != old.path
    assert new.kwargs["analysis_enabled"] is False
    context = new.kwargs["recovery_context"]
    assert context["previous_session_id"] == "original"
    assert context["gap_end_utc"] is None
    assert context["historical_replay_requested"] is False
    assert context["continuity"] == "unverified_gap_not_backfilled"
    assert checks == [True]
    status = json.loads(recorder.config.status_path.read_text())
    assert status["runtime"]["loaded_source"] == json.loads(lr._LOADED_RECORDER_SOURCE_IDENTITY_JSON)
    with recorder.catalog.connect(read_only=True) as connection:
        sessions = connection.execute("SELECT session_id,status,error FROM tape_sessions ORDER BY created_at_utc").fetchall()
        original = connection.execute("SELECT sha256,complete,error FROM tape_feed_status WHERE session_id='original'").fetchone()
    assert len(sessions) == 2 and all(row["status"] == "incomplete" for row in sessions)
    assert original["sha256"] == hashlib.sha256(b"original").hexdigest()
    assert original["complete"] == 0 and original["error"] == "original transport error"
    receipts = list((old.config.output_dir / "recovery_receipts").glob("*.json"))
    assert len(receipts) == 3
    for path in receipts:
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() in path.name
        assert json.loads(raw)["historical_records_modified"] is False
        assert json.loads(raw)["recorder_source_identity"] == status["runtime"]["loaded_source"]


@pytest.mark.parametrize("kind,closed", [(None, True), ("gateway_timeout", False)])
def test_local_failure_or_unknown_cleanup_never_reconnects(tmp_path, monkeypatch, kind, closed):
    feeds = fake_feeds(monkeypatch, failure_kind=kind, closed=closed)
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False,
        recovery_guard=lambda: pytest.fail("must not probe/reconnect"))
    assert recorder.run() == 2
    assert len(feeds.instances) == 1


def test_primary_guard_denial_is_bounded_and_preserves_original(tmp_path, monkeypatch):
    feeds = fake_feeds(monkeypatch)
    checks = []
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False,
        recovery_guard=lambda: checks.append(True) or {"allowed": False, "reason": "CLOCK_UNSAFE"})
    assert recorder.run() == 2
    assert len(checks) == 2 and len(feeds.instances) == 1
    assert recorder.recovery_state["state"] == "exhausted_or_stopped"


def test_missing_receipt_blocks_recovery(tmp_path, monkeypatch):
    feeds = fake_feeds(monkeypatch)
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False,
        recovery_guard=lambda: pytest.fail("receipt failed before guard"))
    def fail_receipt(*args, **kwargs):
        raise OSError("disk I/O error")
    monkeypatch.setattr(lr, "write_recovery_receipt", fail_receipt)
    assert recorder.run() == 2
    assert len(feeds.instances) == 1
    assert recorder.recovery_state["state"] == "failed_closed"


def test_default_recovery_guard_protects_opening_capture(tmp_path, monkeypatch):
    from backend.closing_tape import start_gate

    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False)
    monkeypatch.setattr(lr, "utcnow", lambda: recorder.config.cash_open_utc)
    monkeypatch.setattr(start_gate, "probe_primary_capture_readiness",
                        lambda **_: pytest.fail("opening protection must precede runtime probe"))
    assert recorder._probe_recovery_guard()["reason"] == "PRIMARY_OPENING_CAPTURE_PROTECTED"


def test_maximum_two_new_segments_even_when_transport_keeps_failing(tmp_path, monkeypatch):
    feeds = fake_feeds(monkeypatch, always_fail=True)
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False,
        recovery_guard=lambda: {"allowed": True})
    assert recorder.run() == 2
    assert len(feeds.instances) == 3
    assert recorder.recovery_segment_count == 2
    assert recorder.recovery_state == {
        "state": "blocked", "reason": "RECOVERY_SEGMENT_BUDGET_EXHAUSTED"}


@pytest.mark.parametrize("stop", ["deadline", "shutdown", "disk"])
def test_recovery_rechecks_stops_after_wait_before_connection(tmp_path, monkeypatch, stop):
    feeds = fake_feeds(monkeypatch)
    recorder = lr.StreamingSessionRecorder(config(tmp_path), run_analysis=False,
        recovery_guard=lambda: {"allowed": True})
    original_wait = recorder.shutdown.wait
    def wait(_):
        if stop == "deadline":
            monkeypatch.setattr(lr, "utcnow", lambda: recorder.config.cash_close_utc)
        elif stop == "shutdown":
            recorder.shutdown.set()
        else:
            monkeypatch.setattr(lr.shutil, "disk_usage", lambda _: SimpleNamespace(free=0))
        return original_wait(0)
    monkeypatch.setattr(recorder.shutdown, "wait", wait)
    assert recorder.run() == 2
    assert len(feeds.instances) == 1


def test_actual_feed_recovery_omits_all_replay_starts_and_remains_partial(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    calls = []
    class Client:
        def __init__(self, **kwargs):
            assert kwargs["reconnect_policy"] == "none"
        def subscribe(self, **kwargs): calls.append(kwargs)
        def add_stream(self, stream, **kwargs): self.stream = stream
        def add_callback(self, *args, **kwargs): pass
        def add_reconnect_callback(self, *args, **kwargs): pass
        def start(self): pass
        def block_for_close(self, **kwargs): pass
        def is_connected(self): return False
        def stop(self): pass
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    guards = []
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None, live_factory=Client,
        parquet=False, analysis_enabled=False, recovery_context={"kind": "live_only_partial_recovery"},
        recovery_guard=lambda: guards.append(True) or {"allowed": True})
    feed.run()
    assert guards == [True] and len(calls) == 3
    assert all("start" not in call and "snapshot" not in call for call in calls)
    assert feed.capture_resources_closed
    with catalog.connect(read_only=True) as connection:
        row = connection.execute("SELECT complete,gaps_json,source_components_json FROM tape_feed_status").fetchone()
    assert row["complete"] == 0
    assert "live_only_recovery" in row["gaps_json"]
    assert "live_only_partial_recovery" in row["source_components_json"]


def test_recorder_source_identity_includes_recovery_and_retains_import_snapshot(tmp_path, monkeypatch):
    files = ("backend/closing_tape/live_recorder.py", "backend/closing_tape/recovery.py")
    for relative in files:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(relative.encode())
    identity = lr._capture_recorder_source_identity(tmp_path)
    for relative in files:
        assert identity["files"][relative]["sha256"] == hashlib.sha256(relative.encode()).hexdigest()
    (tmp_path / files[-1]).write_bytes(b"changed later")
    assert identity["files"][files[-1]]["sha256"] == hashlib.sha256(files[-1].encode()).hexdigest()
    assert identity["current_on_disk_recomputed"] is False
    assert set(json.loads(lr._LOADED_RECORDER_SOURCE_IDENTITY_JSON)["files"]) == set(files)


@pytest.mark.parametrize("stop_stage", ["factory", "subscribe", "add_stream"])
def test_recovery_cannot_start_when_client_setup_crosses_cash_close(tmp_path, monkeypatch, stop_stage):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    observed = [NOW]
    monkeypatch.setattr(lr, "utcnow", lambda: observed[0])
    subscriptions = []
    class Client:
        def __init__(self, **kwargs):
            if stop_stage == "factory": observed[0] = cfg.cash_close_utc
        def subscribe(self, **kwargs):
            subscriptions.append(kwargs)
            if stop_stage == "subscribe": observed[0] = cfg.cash_close_utc
        def add_stream(self, stream, **kwargs):
            if stop_stage == "add_stream": observed[0] = cfg.cash_close_utc
        def add_callback(self, *args, **kwargs): pass
        def add_reconnect_callback(self, *args, **kwargs): pass
        def start(self): pytest.fail("must not start beyond cash-close admission deadline")
        def stop(self): pass
        def block_for_close(self, **kwargs): pass
        def is_connected(self): return False
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None, live_factory=Client,
        parquet=False, recovery_context={"kind": "live_only_partial_recovery"},
        recovery_guard=lambda: {"allowed": True})
    feed.run()
    assert feed.capture_resources_closed
    assert feed.transport_failure_kind is None
    if stop_stage == "factory": assert not subscriptions
    if stop_stage == "subscribe": assert len(subscriptions) == 1


def test_actual_recovery_feed_guard_failure_never_creates_client(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None,
        live_factory=lambda **_: pytest.fail("guard denial must not connect"), parquet=False,
        recovery_context={"kind": "live_only_partial_recovery"},
        recovery_guard=lambda: {"allowed": False})
    feed.run()
    assert feed.transport_failure_kind is None
    assert not feed.path.exists()


def test_shutdown_during_guard_prevents_new_live_connection(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    def guard():
        feed.stop(reason="requested during preflight")
        return {"allowed": True}
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None,
        live_factory=lambda **_: pytest.fail("shutdown must prevent new connection"), parquet=False,
        recovery_context={"kind": "live_only_partial_recovery"}, recovery_guard=guard)
    feed.run()
    assert feed.transport_failure_kind is None
    assert not feed.path.exists()


def test_terminal_local_failure_clears_earlier_transport_retry_flag(tmp_path, monkeypatch):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None, parquet=False)
    def fail_after_transport():
        feed.transport_failure_kind = "gateway_timeout"
        feed.failure = "original transport failure"
        raise OSError("disk I/O error while persisting terminal status")
    monkeypatch.setattr(feed, "_run_impl", fail_after_transport)
    feed.run()
    assert feed.transport_failure_kind is None
    assert feed.failure == "original transport failure"


@pytest.mark.parametrize("blocked_writer", ["worker", "monitor", "client", "raw_stream"])
def test_failed_cleanup_retains_os_owner_without_closing_or_finalizing(tmp_path, monkeypatch, blocked_writer):
    cfg = replace(config(tmp_path), flush_seconds=0.001)
    release = threading.Event()
    entered = threading.Event()
    clients = []
    inspected = []
    def held_writer(_feed):
        entered.set()
        release.wait()
    if blocked_writer == "worker":
        monkeypatch.setattr(lr.FeedThread, "_aggregate_worker", held_writer)
    elif blocked_writer == "monitor":
        monkeypatch.setattr(lr.FeedThread, "_monitor", held_writer)
    else:
        entered.set()
    if blocked_writer == "raw_stream":
        real_open = Path.open
        class UnclosedStream:
            def __init__(self, stream): self.stream = stream
            def write(self, data): return self.stream.write(data)
            def flush(self): return self.stream.flush()
            def close(self): raise OSError("raw stream close failed")
            @property
            def closed(self):
                if release.is_set(): self.stream.close()
                return self.stream.closed
        def open_path(path, *args, **kwargs):
            stream = real_open(path, *args, **kwargs)
            return UnclosedStream(stream) if path.suffix == ".dbn" and args[0] == "xb" else stream
        monkeypatch.setattr(Path, "open", open_path)
    class Client:
        def __init__(self, **kwargs): clients.append(self)
        def subscribe(self, **kwargs): pass
        def add_stream(self, stream, **kwargs): self.stream = stream
        def add_callback(self, *args, **kwargs): pass
        def add_reconnect_callback(self, *args, **kwargs): pass
        def start(self): self.stream.write(b"retained partial fixture")
        def stop(self): pass
        def block_for_close(self, **kwargs): assert entered.wait(2)
        def is_connected(self): return blocked_writer == "client" and not release.is_set()
    def inspect_after_stop(*args, **kwargs):
        assert release.is_set(), "must not hash while a capture writer can change evidence"
        inspected.append(True)
        raise ValueError("fixture deliberately is not a DBN tape")
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    monkeypatch.setattr(lr, "TAPE_AGGREGATE_DRAIN_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(lr, "TAPE_MONITOR_DRAIN_MINIMUM_SECONDS", 0.01)
    monkeypatch.setattr(lr, "TAPE_CLEANUP_OWNERSHIP_POLL_SECONDS", 0.01)
    monkeypatch.setattr(lr, "inspect_dbn", inspect_after_stop)
    recorder = lr.StreamingSessionRecorder(cfg, live_factory=Client, run_analysis=False, parquet=False,
        recovery_guard=lambda: pytest.fail("fatal cleanup must not recover"))
    real_wait = recorder.shutdown.wait
    monkeypatch.setattr(recorder.shutdown, "wait", lambda timeout: real_wait(min(timeout, 0.01)))
    results = []
    runner = threading.Thread(target=lambda: results.append(recorder.run()))
    runner.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if recorder.feeds and recorder.feeds[0].cleanup_state["state"] == "failed_cleanup_ownership_retained":
                break
            time.sleep(0.01)
        feed = recorder.feeds[0]
        assert feed.cleanup_state["code"] == "FAILED_CLEANUP_OWNERSHIP_RETAINED"
        assert feed.transport_failure_kind is None
        original_failure = feed.failure
        assert runner.is_alive() and not clients[0].stream.closed and not inspected
        with pytest.raises(RuntimeError, match="already running"):
            with lr.ProcessFileLock(cfg.lock_path): pass
        with recorder.catalog.connect(read_only=True) as connection:
            session = connection.execute("SELECT status,completed_at_utc FROM tape_sessions").fetchone()
            status = connection.execute("SELECT sha256,ended_at_utc FROM tape_feed_status").fetchone()
        assert session["status"] == "running" and session["completed_at_utc"] is None
        assert status["sha256"] is None and status["ended_at_utc"] is None
        recorder.stop("operator shutdown during failed cleanup")
        time.sleep(0.03)
        assert runner.is_alive() and not clients[0].stream.closed and not inspected
        assert feed.failure == original_failure
        with pytest.raises(RuntimeError, match="already running"):
            with lr.ProcessFileLock(cfg.lock_path): pass
    finally:
        release.set()
        runner.join(timeout=5)
    assert not runner.is_alive() and results == [2]
    assert clients[0].stream.closed and inspected == [True]
    assert len(clients) == 1 and feed.cleanup_state["state"] == "cleanup_released_incomplete"
    assert feed.cleanup_state["evidence_remains_incomplete"] is True
    assert feed.cleanup_state["raw_finalization_allowed"] is True
    with recorder.catalog.connect(read_only=True) as connection:
        final = connection.execute("SELECT complete,gaps_json FROM tape_feed_status").fetchone()
    assert final["complete"] == 0 and "FAILED_CLEANUP_OWNERSHIP_RETAINED" in final["gaps_json"]
    assert list((cfg.output_dir / "recovery_receipts").glob("*.json"))


@pytest.mark.parametrize("integrity_error", [False, True])
def test_actual_feed_transport_exception_closes_writer_before_recovery(tmp_path, monkeypatch, integrity_error):
    cfg = config(tmp_path)
    cfg.output_dir.mkdir(parents=True)
    catalog = TapeCatalog(cfg.catalog_path)
    catalog.start_session(cfg)
    clients = []
    class FailedClient:
        def __init__(self, **kwargs): self.connected = True; clients.append(self)
        def subscribe(self, **kwargs): pass
        def add_stream(self, stream, **kwargs): self.stream = stream
        def add_callback(self, *args, **kwargs): pass
        def add_reconnect_callback(self, *args, **kwargs): pass
        def start(self):
            if integrity_error:
                self.stream.write(b"partial fixture")
        def block_for_close(self, **kwargs):
            self.connected = False
            raise RuntimeError("Gateway timeout: 15 second(s) since last message")
        def is_connected(self): return self.connected
        def stop(self): self.connected = False
    monkeypatch.setattr(lr, "utcnow", lambda: NOW)
    if integrity_error:
        def fail_integrity(*args, **kwargs):
            raise OSError("disk I/O error")
        monkeypatch.setattr(lr, "inspect_dbn", fail_integrity)
    feed = lr.FeedThread(cfg, cfg.feeds[0], catalog, lambda: None,
                        live_factory=FailedClient, parquet=False, analysis_enabled=False)
    feed.run()
    assert feed.transport_failure_kind == (None if integrity_error else "gateway_timeout")
    assert feed.capture_resources_closed
    assert clients[0].stream.closed
    assert "Gateway timeout" in feed.failure
