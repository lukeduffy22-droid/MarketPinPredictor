import json
import threading
import time
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import databento_dbn as dbn
import pytest

from backend.closing_tape.catalog import TapeCatalog
from backend.closing_tape.config import build_session_config
from backend.closing_tape import live_recorder
from backend.closing_tape.live_recorder import (
    FeedThread,
    LiveAggregator,
    MinuteBucket,
    ProcessPriorityControl,
    StreamingSessionRecorder,
    SystemSleepGuard,
    excessive_unmapped_trade_ratio,
    infer_execution_location,
    monitor_pause_reason,
    provisional_tcbbo_integrity_issues,
    retry_sqlite_busy,
    tcbbo_integrity_issues,
)


UTC = timezone.utc


class HarmlessRecord:
    ts_event = 1_787_670_000_000_000_000

    @staticmethod
    def record_size():
        return 16

    def __bytes__(self):
        return b"r" * self.record_size()


def test_process_priority_normalizes_inherited_below_normal():
    observed = iter((0x00004000, 0x00000020))
    calls = []
    control = ProcessPriorityControl(
        platform_name="nt",
        current_process=lambda: "current-process",
        getter=lambda _handle: next(observed),
        setter=lambda handle, priority: calls.append((handle, priority)) or 1,
    )

    assert control.normalize()
    assert calls == [("current-process", 0x00000020)]
    assert control.to_dict() == {
        "platform": "nt",
        "requested": True,
        "active": True,
        "mode": "normal_priority_class",
        "before": "below_normal",
        "before_code": 0x00004000,
        "after": "normal",
        "after_code": 0x00000020,
        "error": None,
    }


def test_process_priority_failure_is_visible():
    control = ProcessPriorityControl(
        platform_name="nt",
        current_process=lambda: "current-process",
        getter=lambda _handle: 0x00004000,
        setter=lambda _handle, _priority: 0,
    )

    assert not control.normalize()
    assert not control.active
    assert control.error == "SetPriorityClass returned 0"


def test_process_priority_leaves_normal_process_unchanged():
    calls = []
    control = ProcessPriorityControl(
        platform_name="nt",
        current_process=lambda: "current-process",
        getter=lambda _handle: 0x00000020,
        setter=lambda handle, priority: calls.append((handle, priority)) or 1,
    )

    assert control.normalize()
    assert calls == []
    assert control.before_code == 0x00000020
    assert control.after_code == 0x00000020


def test_sqlite_busy_retry_recovers_without_hiding_contention(monkeypatch):
    attempts = []
    delays = []

    def operation():
        attempts.append(len(attempts) + 1)
        if len(attempts) < 3:
            raise live_recorder.sqlite3.OperationalError("database is locked")
        return 42

    monkeypatch.setattr(live_recorder.time, "sleep", delays.append)

    assert retry_sqlite_busy(operation, attempts=3, delay_seconds=0.5) == 42
    assert attempts == [1, 2, 3]
    assert delays == [0.5, 0.5]


def test_sqlite_busy_retry_does_not_retry_unrelated_operational_error(
    monkeypatch,
):
    delays = []
    monkeypatch.setattr(live_recorder.time, "sleep", delays.append)

    with pytest.raises(live_recorder.sqlite3.OperationalError, match="disk I/O"):
        retry_sqlite_busy(
            lambda: (_ for _ in ()).throw(
                live_recorder.sqlite3.OperationalError("disk I/O error")
            )
        )

    assert delays == []


def test_status_heartbeat_persistent_sqlite_lock_is_best_effort(tmp_path, monkeypatch):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="heartbeat-locked",
    )
    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    attempts = []

    def locked_status():
        attempts.append(True)
        raise live_recorder.sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(recorder, "write_status", locked_status)
    monkeypatch.setattr(live_recorder.time, "sleep", lambda _seconds: None)

    assert recorder._write_status_heartbeat() is False
    assert len(attempts) == 3
    assert recorder.status_heartbeat_requested.is_set()
    assert not recorder.shutdown.is_set()


def test_status_heartbeat_non_lock_sqlite_error_remains_fatal(tmp_path, monkeypatch):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="heartbeat-io-error",
    )
    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    monkeypatch.setattr(
        recorder,
        "write_status",
        lambda: (_ for _ in ()).throw(
            live_recorder.sqlite3.OperationalError("disk I/O error")
        ),
    )

    with pytest.raises(live_recorder.sqlite3.OperationalError, match="disk I/O"):
        recorder._write_status_heartbeat()


def test_run_survives_locked_heartbeats_but_final_status_stays_fail_closed(
    tmp_path, monkeypatch
):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="heartbeat-run",
        stop_due_utc=datetime.now(UTC) + timedelta(minutes=5),
    )

    class FinishedFeed:
        instances = []

        def __init__(self, feed_config, feed, catalog, status_callback, **_kwargs):
            self.config = feed_config
            self.feed = feed
            self.catalog = catalog
            self.status_callback = status_callback
            self.failure = None
            self.alive_checks = 0
            self.started = False
            self.joined = False
            self.instances.append(self)

        def start(self):
            self.started = True
            # Feed threads may request a status refresh while their monitor is
            # persisting. This callback must only enqueue/defer the real write.
            self.status_callback()
            self.catalog.register_feed(
                self.config.session_id,
                self.feed,
                self.config.output_dir / "finished.dbn",
            )
            self.catalog.update_feed_status(
                self.config.session_id,
                self.feed.name,
                {"status": "complete", "complete": 1},
            )

        def is_alive(self):
            self.alive_checks += 1
            return self.alive_checks == 1

        def join(self):
            self.joined = True

    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    recorder.shutdown = SimpleNamespace(is_set=lambda: False, wait=lambda _timeout: False)
    status_attempts = []

    def locked_status():
        status_attempts.append(True)
        raise live_recorder.sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(live_recorder, "FeedThread", FinishedFeed)
    monkeypatch.setattr(recorder, "write_status", locked_status)
    monkeypatch.setattr(live_recorder.time, "sleep", lambda _seconds: None)

    assert recorder.run() == 2
    assert len(status_attempts) == 7
    assert FinishedFeed.instances[0].started
    assert FinishedFeed.instances[0].joined
    assert recorder.status_heartbeat_requested.is_set()
    with recorder.catalog.connect(read_only=True) as connection:
        session = connection.execute(
            "SELECT status, error FROM tape_sessions WHERE session_id=?",
            (config.session_id,),
        ).fetchone()
    assert dict(session) == {"status": "complete", "error": None}


def _feed(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    )
    feed = FeedThread(config, config.feeds[0], TapeCatalog(config.catalog_path), lambda: None, parquet=False)
    feed.path.parent.mkdir(parents=True, exist_ok=True)
    # The SDK writes DBN metadata and the current record before dispatching the
    # callback. Model that ordering so metadata-byte discovery is realistic.
    feed.path.write_bytes(b"m" * 128 + b"r" * HarmlessRecord.record_size())
    return feed


def test_feed_monitor_status_callback_failure_does_not_latch_capture_failure(
    tmp_path,
):
    feed = _feed(tmp_path)

    class OneTickStop:
        def __init__(self):
            self.calls = 0

        def wait(self, _timeout):
            self.calls += 1
            return self.calls > 1

    feed.monitor_stop = OneTickStop()
    feed.status_callback = lambda: (_ for _ in ()).throw(
        live_recorder.sqlite3.OperationalError("database is locked")
    )

    feed._monitor()

    assert feed.failure is None
    gaps = json.loads(str(feed.aggregator.status().get("gaps_json") or "[]"))
    assert not any("live aggregate persistence failed" in str(gap) for gap in gaps)


def test_optional_tape_client_never_auto_reconnects(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        stop_due_utc=datetime.now(UTC) + timedelta(minutes=5),
    )
    observed_kwargs = {}

    class OneShotClient:
        def subscribe(self, **_kwargs):
            return None

        def add_stream(self, *_args, **_kwargs):
            return None

        def add_callback(self, *_args, **_kwargs):
            return None

        def add_reconnect_callback(self, *_args, **_kwargs):
            return None

        def start(self):
            return None

        def block_for_close(self):
            return None

    def live_factory(**kwargs):
        observed_kwargs.update(kwargs)
        return OneShotClient()

    feed = FeedThread(
        config,
        config.feeds[0],
        TapeCatalog(config.catalog_path),
        lambda: None,
        parquet=False,
        live_factory=live_factory,
    )

    feed._run_impl()

    assert observed_kwargs["reconnect_policy"] == "none"


def test_subscribe_failure_stops_exact_tape_client_before_finalization(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        stop_due_utc=datetime.now(UTC) + timedelta(minutes=5),
    )

    class SubscribeFailureClient:
        def __init__(self):
            self.stop_calls = 0
            self.close_timeouts = []

        def subscribe(self, **_kwargs):
            raise RuntimeError("synthetic subscribe failure")

        def stop(self):
            self.stop_calls += 1

        def block_for_close(self, *, timeout):
            self.close_timeouts.append(timeout)

    client = SubscribeFailureClient()
    feed = FeedThread(
        config,
        config.feeds[0],
        TapeCatalog(config.catalog_path),
        lambda: None,
        parquet=False,
        live_factory=lambda **_kwargs: client,
    )

    feed._run_impl()

    assert client.stop_calls == 1
    assert client.close_timeouts == [
        live_recorder.TAPE_LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS
    ]
    assert feed.failure == "RuntimeError: synthetic subscribe failure"


def test_tape_cleanup_errors_do_not_mask_original_start_failure(tmp_path):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        stop_due_utc=datetime.now(UTC) + timedelta(minutes=5),
    )

    class StartFailureClient:
        def subscribe(self, **_kwargs):
            return None

        def add_stream(self, *_args, **_kwargs):
            return None

        def add_callback(self, *_args, **_kwargs):
            return None

        def add_reconnect_callback(self, *_args, **_kwargs):
            return None

        def start(self):
            raise RuntimeError("synthetic start failure")

        def stop(self):
            raise OSError("synthetic stop failure")

        def block_for_close(self, *, timeout):
            assert timeout == live_recorder.TAPE_LIVE_CLIENT_CLOSE_TIMEOUT_SECONDS
            raise TimeoutError("synthetic close timeout")

    feed = FeedThread(
        config,
        config.feeds[0],
        TapeCatalog(config.catalog_path),
        lambda: None,
        parquet=False,
        live_factory=lambda **_kwargs: StartFailureClient(),
    )

    feed._run_impl()

    assert feed.failure == "RuntimeError: synthetic start failure"


def test_paper_shadow_failure_is_visible_but_not_canonical_analysis_failure(
    tmp_path, monkeypatch
):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    )
    recorder = StreamingSessionRecorder(config, run_analysis=True, parquet=False)
    feed = SimpleNamespace(path=tmp_path / "live.dbn", feed=SimpleNamespace(name="options"))

    def fail_shadow(**_kwargs):
        raise RuntimeError("candidate evidence failed")

    monkeypatch.setattr(
        "backend.closing_tape.live_shadow.run_live_prefix_paper_shadow", fail_shadow
    )
    recorder._run_paper_shadow(
        feed, {"cutoff_bytes": 100, "prefix_sha256": "a" * 64}
    )

    assert recorder.analysis_error is None
    assert recorder.paper_shadow_result == {
        "configured": True,
        "recorded": 0,
        "reasons": ["RuntimeError: candidate evidence failed"],
    }


def test_paper_shadow_uses_the_session_configured_market_database(
    tmp_path, monkeypatch
):
    configured_database = tmp_path / "configured" / "market.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{configured_database}")
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    )
    recorder = StreamingSessionRecorder(config, run_analysis=True, parquet=False)
    feed = SimpleNamespace(path=tmp_path / "live.dbn", feed=SimpleNamespace(name="options"))
    captured = {}

    def observe_shadow(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(to_dict=lambda: {"configured": False, "recorded": 0})

    monkeypatch.setattr(
        "backend.closing_tape.live_shadow.run_live_prefix_paper_shadow",
        observe_shadow,
    )
    recorder._run_paper_shadow(
        feed, {"cutoff_bytes": 100, "prefix_sha256": "a" * 64}
    )

    assert config.market_db_path == configured_database.resolve()
    assert captured["market_db_path"] == configured_database.resolve()


def test_reader_callback_only_enqueues(tmp_path):
    feed = _feed(tmp_path)

    feed._enqueue_record(HarmlessRecord())

    assert feed.aggregator.records_seen == 0
    assert feed.record_queue.qsize() == 1


def test_analysis_barrier_freezes_exact_processed_prefix(tmp_path):
    feed = _feed(tmp_path)
    feed._enqueue_record(HarmlessRecord())
    worker = threading.Thread(target=feed._aggregate_worker)
    worker.start()

    barrier = feed.freeze_analysis_snapshot(timeout_seconds=2.0)

    assert barrier == (1, 144)
    assert feed.processed_sequence == 1
    assert feed.worker_paused

    # Raw callbacks may continue during prefix hashing, but the derived snapshot
    # must remain fixed at the exact target sequence until explicitly released.
    feed.path.write_bytes(feed.path.read_bytes() + b"r" * HarmlessRecord.record_size())
    feed._enqueue_record(HarmlessRecord())
    time.sleep(0.05)
    assert feed.processed_sequence == 1

    feed.release_analysis_snapshot()
    deadline = time.monotonic() + 2.0
    while feed.processed_sequence < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    feed.worker_stop.set()
    with feed.sequence_condition:
        feed.sequence_condition.notify_all()
    worker.join(timeout=2.0)

    assert feed.processed_sequence == 2
    assert not worker.is_alive()


def test_analysis_barrier_excludes_first_callback_observed_after_deadline(
    tmp_path, monkeypatch
):
    feed = _feed(tmp_path)
    before = feed.config.analysis_due_utc.replace(microsecond=0) - timedelta(seconds=1)
    after = feed.config.analysis_due_utc.replace(microsecond=0) + timedelta(seconds=1)
    observed = iter((before, after))
    monkeypatch.setattr(live_recorder, "utcnow", lambda: next(observed))

    feed._enqueue_record(HarmlessRecord())
    feed.path.write_bytes(feed.path.read_bytes() + b"r" * HarmlessRecord.record_size())
    feed._enqueue_record(HarmlessRecord())
    worker = threading.Thread(target=feed._aggregate_worker)
    worker.start()

    barrier = feed.freeze_analysis_snapshot(timeout_seconds=2.0)

    assert barrier == (1, 144)
    assert feed.analysis_deadline_sequence == 1
    assert feed.processed_sequence == 1
    assert feed.worker_paused
    assert feed.record_queue.qsize() == 1

    feed.release_analysis_snapshot()
    deadline = time.monotonic() + 2.0
    while feed.processed_sequence < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    feed.worker_stop.set()
    with feed.sequence_condition:
        feed.sequence_condition.notify_all()
    worker.join(timeout=2.0)

    assert feed.processed_sequence == 2
    assert not worker.is_alive()


def test_analysis_cutoff_rejects_post_horizon_trade_event(tmp_path, monkeypatch):
    feed = _feed(tmp_path)
    feed.analysis_pause_target = 1
    feed.worker_paused = True
    feed.processed_sequence = 1
    cutoff_ns = int(feed.config.analysis_due_utc.timestamp() * 1_000_000_000)
    monkeypatch.setattr(
        feed,
        "_status_snapshot",
        lambda: {"last_trade_event_ns": cutoff_ns + 1, "callback_queue_depth": 0},
    )

    with pytest.raises(RuntimeError, match="post-horizon trade event"):
        feed.capture_analysis_cutoff(record_sequence=1, cutoff_bytes=144)


def test_queue_overflow_marks_derived_features_incomplete(tmp_path):
    feed = _feed(tmp_path)
    feed.record_queue = __import__("queue").Queue(maxsize=1)

    feed._enqueue_record(HarmlessRecord())
    feed._enqueue_record(HarmlessRecord())

    status = feed.aggregator.status()
    assert feed.record_queue.qsize() == 1
    assert status["slow_reader_warnings"] == 1
    assert "derived_callback_drop_records" in status["gaps_json"]


def test_replay_and_live_nbbo_tolerance_semantics_match():
    assert infer_execution_location(1.995, 1.90, 2.00) == "at_ask"
    assert infer_execution_location(1.904, 1.90, 2.00) == "at_bid"
    assert infer_execution_location(1.95, 1.90, 2.00) == "inside"


def test_monitor_pause_is_retained_without_claiming_provider_or_queue_loss():
    aggregator = LiveAggregator(
        "session",
        SimpleNamespace(name="opra_options"),
        catalog=None,
    )
    reason = "recorder monitor paused for 153.2 seconds"

    aggregator.record_operational_warning(reason)
    status = aggregator.status()

    assert status["slow_reader_warnings"] == 0
    assert status["provider_error_count"] == 0
    assert status["error"] is None
    assert json.loads(status["gaps_json"]) == [
        {"operational_warning": reason}
    ]


def test_observed_microstructure_sums_do_not_fabricate_missing_nbbo():
    bucket = MinuteBucket("session", "opra_options", "SPX", "2026-08-25T19:00:00+00:00", "options")
    bucket.add(
        price=2.0, size=3, notional=600, option_type="C", location="inside",
        event_ns=100, receive_ns=350, instrument_id=1,
        bid=1.8, ask=2.2, bid_size=10, ask_size=12, receive_lag_ns=250, flags=4,
    )
    bucket.add(
        price=1.0, size=1, notional=100, option_type="P", location="unknown",
        event_ns=200, receive_ns=None, instrument_id=2,
        receive_lag_ns=None,
    )

    observed = bucket.observed_row()
    inferred = bucket.inferred_row()
    assert observed["nbbo_valid_count"] == 1
    assert observed["quoted_spread_sum"] == pytest.approx(0.4)
    assert observed["quoted_spread_bps_sum"] == pytest.approx(2000.0)
    assert observed["trade_to_mid_abs_sum"] == pytest.approx(0.0)
    assert observed["bid_size_sum"] == 10
    assert observed["ask_size_sum"] == 12
    assert observed["receive_lag_ns_sum"] == 250
    assert observed["receive_lag_ns_max"] == 250
    assert observed["flag_maybe_bad_book_count"] == 1
    assert observed["data_quality_flagged_count"] == 1
    assert observed["flag_last_count"] == 0
    assert "at_ask_count" not in observed
    assert inferred["inside_count"] == 1
    assert inferred["unknown_count"] == 1


def test_live_tcbbo_counters_are_explicitly_provisional():
    aggregator = LiveAggregator(
        "session", SimpleNamespace(name="opra_options", asset_class="options",
                                   contract_multiplier=100.0), catalog=None
    )
    aggregator.instruments[1] = {
        "raw_symbol": "SPXW  260825C07600000",
        "family_root": "SPX",
        "option_type": "C",
    }
    aggregator(
        SimpleNamespace(
            instrument_id=1, ts_event=1_787_686_500_000_000_000,
            ts_recv=1_787_686_500_000_000_100, price=2.0, size=1,
            bid_px_00=1.9, ask_px_00=2.1, bid_sz_00=10, ask_sz_00=12,
            flags=0,
        )
    )
    aggregator(
        SimpleNamespace(
            instrument_id=1, ts_event=1_787_686_500_000_000_200,
            price=2.0, size=1, bid_px_00=2.2, ask_px_00=2.1,
            bid_sz_00=10, ask_sz_00=12, flags=0,
        )
    )

    status = aggregator.status()
    assert status["provisional_tcbbo_records"] == 2
    assert status["provisional_tcbbo_timestamped_records"] == 1
    assert status["provisional_tcbbo_valid_nbbo_records"] == 1
    assert status["trade_records"] == 2


def test_terminal_tcbbo_scan_must_reconcile_with_provisional_callbacks():
    status = {
        "provisional_tcbbo_records": 100,
        "provisional_tcbbo_timestamped_records": 100,
        "provisional_tcbbo_valid_nbbo_records": 99,
    }
    matching = SimpleNamespace(
        tcbbo_records=100,
        tcbbo_timestamped_records=100,
        tcbbo_valid_nbbo_records=99,
    )
    assert provisional_tcbbo_integrity_issues(status, matching) == []

    mismatched = SimpleNamespace(
        tcbbo_records=101,
        tcbbo_timestamped_records=100,
        tcbbo_valid_nbbo_records=98,
    )
    issues = provisional_tcbbo_integrity_issues(status, mismatched)
    assert len(issues) == 2
    assert "TCBBO record count" in issues[0]
    assert "valid pre-trade NBBO record count" in issues[1]


def test_minute_first_and_last_prices_ignore_callback_arrival_order():
    trades = [
        dict(price=3.0, size=1, notional=300, event_ns=300, receive_ns=330,
             instrument_id=3, option_type="C", location="inside"),
        dict(price=1.0, size=1, notional=100, event_ns=100, receive_ns=130,
             instrument_id=1, option_type="C", location="inside"),
        dict(price=2.0, size=1, notional=200, event_ns=200, receive_ns=230,
             instrument_id=2, option_type="C", location="inside"),
    ]
    forward = MinuteBucket(
        "forward", "opra_options", "SPX", "2026-08-25T19:00:00+00:00", "options"
    )
    reversed_bucket = MinuteBucket(
        "reverse", "opra_options", "SPX", "2026-08-25T19:00:00+00:00", "options"
    )
    for trade in trades:
        forward.add(**trade)
    for trade in reversed(trades):
        reversed_bucket.add(**trade)

    assert forward.first_price == reversed_bucket.first_price == 1.0
    assert forward.last_price == reversed_bucket.last_price == 3.0


def test_tcbbo_completion_gate_requires_timestamps_and_nbbo_coverage():
    assert not tcbbo_integrity_issues(
        SimpleNamespace(
            tcbbo_records=100,
            tcbbo_timestamped_records=100,
            tcbbo_valid_nbbo_records=95,
        )
    )
    issues = tcbbo_integrity_issues(
        SimpleNamespace(
            tcbbo_records=100,
            tcbbo_timestamped_records=99,
            tcbbo_valid_nbbo_records=94,
        )
    )
    assert len(issues) == 2
    assert tcbbo_integrity_issues(SimpleNamespace(tcbbo_records=0)) == [
        "TCBBO subscription produced no TCBBO records"
    ]


def test_terminal_completeness_rejects_excessive_unmapped_trade_ratio():
    assert not excessive_unmapped_trade_ratio(5, 1_000)
    assert excessive_unmapped_trade_ratio(6, 1_000)


def test_monitor_pause_threshold_is_explicit():
    assert monitor_pause_reason(30.0, 10.0) is None
    reason = monitor_pause_reason(31.0, 10.0)
    assert reason is not None
    assert "system sleep" in reason


def test_monitor_pause_is_retained_as_operational_warning(tmp_path, monkeypatch):
    feed = _feed(tmp_path)

    class OneMonitorCycle:
        calls = 0

        def wait(self, _timeout):
            self.calls += 1
            return self.calls > 1

    feed.monitor_stop = OneMonitorCycle()
    ticks = iter((100.0, 131.0, 132.0))
    monkeypatch.setattr(live_recorder.time, "monotonic", lambda: next(ticks))

    feed._monitor()

    status = feed.aggregator.status()
    assert status["slow_reader_warnings"] == 0
    gaps = json.loads(status["gaps_json"])
    assert len(gaps) == 1
    assert "recorder monitor paused" in gaps[0]["operational_warning"]


def test_windows_sleep_guard_is_transient_and_releases():
    flags = []
    guard = SystemSleepGuard(platform_name="nt", setter=lambda value: flags.append(value) or 1)

    with guard:
        assert guard.active
        assert guard.to_dict()["requested"] is True

    assert not guard.active
    assert flags == [
        live_recorder.ES_CONTINUOUS | live_recorder.ES_SYSTEM_REQUIRED,
        live_recorder.ES_CONTINUOUS,
    ]


def test_sleep_guard_is_noop_outside_windows():
    guard = SystemSleepGuard(platform_name="posix", setter=lambda _value: pytest.fail("setter called"))

    assert not guard.activate()
    assert guard.to_dict()["mode"] == "not_applicable"


def test_windows_sleep_guard_failure_is_visible_without_claiming_protection():
    guard = SystemSleepGuard(platform_name="nt", setter=lambda _value: 0)

    assert not guard.activate()
    assert not guard.active
    assert guard.to_dict()["error"] == "SetThreadExecutionState returned 0"


def test_absolute_stop_deadline_survives_relative_timer_pause(tmp_path, monkeypatch):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
    )
    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    stop_calls = []
    feed = SimpleNamespace(is_alive=lambda: True, stop=lambda: stop_calls.append(True))
    monkeypatch.setattr(live_recorder, "utcnow", lambda: config.stop_due_utc)

    assert recorder._stop_feeds_at_absolute_deadline([feed])
    assert stop_calls == [True]


def test_opra_open_interest_uses_event_time_when_reference_time_is_undefined(tmp_path):
    feed = _feed(tmp_path)
    instrument_id = 123
    event_ns = 1_787_653_800_613_943_845
    feed.aggregator.instruments[instrument_id] = {
        "raw_symbol": "SPXW  260825C07600000",
        "family_root": "SPX",
    }
    record = dbn.StatMsg(
        publisher_id=61,
        instrument_id=instrument_id,
        ts_event=event_ns,
        ts_recv=event_ns + 1,
        ts_ref=2**64 - 1,
        price=0,
        quantity=1_288,
        stat_type=dbn.StatType.OPEN_INTEREST,
        update_action=dbn.StatUpdateAction.NEW,
    )

    feed.aggregator._statistic(record)

    item = feed.aggregator.open_interest[instrument_id]
    assert item["open_interest"] == 1_288.0
    assert item["asof_utc"] == datetime.fromtimestamp(event_ns / 1e9, tz=UTC).isoformat()


def test_status_reader_lock_is_retried_without_stopping_capture(tmp_path, monkeypatch):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="status-retry",
    )
    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    assert recorder.catalog is None
    recorder.catalog = TapeCatalog(config.catalog_path)
    recorder.catalog.start_session(config)
    real_replace = live_recorder.os.replace
    attempts = []

    def intermittently_locked(source, destination):
        attempts.append((source, destination))
        if len(attempts) < 3:
            raise PermissionError("simulated Windows reader lock")
        real_replace(source, destination)

    monkeypatch.setattr(live_recorder.os, "replace", intermittently_locked)
    monkeypatch.setattr(live_recorder.time, "sleep", lambda _: None)

    recorder.write_status()

    assert len(attempts) == 3
    assert config.status_path.exists()
    payload = json.loads(config.status_path.read_text(encoding="utf-8"))
    assert payload["runtime"]["python_executable"]
    assert isinstance(payload["runtime"]["venv_active"], bool)
    assert payload["process_priority"]["mode"] in {
        "normal_priority_class",
        "not_applicable",
    }
    assert payload["sleep_prevention"]["limitation"].startswith("does not override")
    assert attempts[0][0] != config.status_path.with_suffix(".tmp")
    assert not attempts[0][0].exists()


def test_persistent_status_reader_lock_does_not_kill_capture(tmp_path, monkeypatch):
    config = build_session_config(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 15, 0, tzinfo=UTC),
        session_id="status-locked",
    )
    recorder = StreamingSessionRecorder(config, run_analysis=False, parquet=False)
    recorder.catalog = TapeCatalog(config.catalog_path)
    recorder.catalog.start_session(config)
    temporary_paths = []

    def always_locked(source, _destination):
        temporary_paths.append(source)
        raise PermissionError("simulated persistent Windows reader lock")

    monkeypatch.setattr(live_recorder.os, "replace", always_locked)
    monkeypatch.setattr(live_recorder.time, "sleep", lambda _: None)

    recorder.write_status()

    assert len(temporary_paths) == 20
    assert not temporary_paths[0].exists()
