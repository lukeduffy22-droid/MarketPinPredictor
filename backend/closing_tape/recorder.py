from __future__ import annotations

import json
import os
import shutil
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .catalog import TapeCatalog
from .config import FeedSpec, SessionConfig
from .integrity import inspect_dbn


UTC = timezone.utc


class RecorderLockError(RuntimeError):
    pass


class SessionRecorder:
    """Write provider DBN on the client stream without per-record analytics."""

    def __init__(
        self,
        config: SessionConfig,
        *,
        client_factory: Callable[..., object] | None = None,
        api_key: str | None = None,
    ):
        self.config = config
        self.catalog = TapeCatalog(config.catalog_path)
        self.api_key = api_key or os.getenv("DATABENTO_API_KEY")
        self._client_factory = client_factory
        self._lock_fd: int | None = None

    def _new_client(self):
        if self._client_factory is not None:
            return self._client_factory(
                key=self.api_key,
                reconnect_policy="none",
                slow_reader_behavior="warn",
            )
        import databento as db

        return db.Live(
            key=self.api_key,
            reconnect_policy="none",
            slow_reader_behavior="warn",
        )

    def _acquire_lock(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._lock_fd = os.open(
                self.config.lock_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RecorderLockError(f"recorder lock already exists: {self.config.lock_path}") from exc
        os.write(self._lock_fd, json.dumps({"pid": os.getpid(), "session_id": self.config.session_id}).encode())

    def _release_lock(self) -> None:
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None
        self.config.lock_path.unlink(missing_ok=True)

    def _check_capacity(self) -> None:
        free = shutil.disk_usage(self.config.output_dir).free
        if free < self.config.min_free_bytes:
            raise OSError(f"insufficient disk space: {free} bytes free; {self.config.min_free_bytes} required")

    def _record_feed(self, feed: FeedSpec, errors: list[str]) -> None:
        dbn_path = self.config.output_dir / f"{feed.name}-{self.config.session_id}.dbn"
        self.catalog.register_feed(self.config.session_id, feed, dbn_path)
        client = self._new_client()
        try:
            client.add_stream(dbn_path)
            for subscription in feed.subscriptions:
                client.subscribe(
                    dataset=feed.dataset,
                    schema=subscription.schema,
                    symbols=list(subscription.symbols),
                    stype_in=subscription.stype_in,
                    start=subscription.start_utc,
                )
            self.catalog.update_feed_status(
                self.config.session_id,
                feed.name,
                {"status": "running", "started_at_utc": datetime.now(UTC).isoformat()},
            )
            client.start()
            remaining = max(0.0, (self.config.stop_due_utc - datetime.now(UTC)).total_seconds())
            client.block_for_close(timeout=remaining)
            report = inspect_dbn(
                dbn_path,
                require_trades=feed.required,
                expected_subscription_acks=len(feed.subscriptions),
            )
            session_issues: list[str] = []
            if report.subscription_acks < len(feed.subscriptions):
                session_issues.append("subscription acknowledgements incomplete")
            if report.replay_completed < len(feed.subscriptions):
                session_issues.append("schema replay incomplete")
            if any(item.schema == "statistics" for item in feed.subscriptions) and report.statistics_records <= 0:
                session_issues.append("statistics coverage missing")
            if any(item.schema == "definition" for item in feed.subscriptions) and report.definition_records <= 0:
                session_issues.append("definition coverage missing")
            close_cutoff_ns = int((self.config.cash_close_utc.timestamp() - 300) * 1e9)
            if datetime.now(UTC) >= self.config.cash_close_utc and int(report.last_event_ns or 0) < close_cutoff_ns:
                session_issues.append("tape did not advance to the close window")
            session_complete = report.local_file_intact and not session_issues
            self.catalog.update_feed_status(
                self.config.session_id,
                feed.name,
                {
                    "status": "complete" if session_complete else "incomplete",
                    "ended_at_utc": datetime.now(UTC).isoformat(),
                    "records_seen": report.records_seen,
                    "trade_records": report.trade_records,
                    "mapping_records": report.mapping_records,
                    "statistics_records": report.statistics_records,
                    "definition_records": report.definition_records,
                    "first_event_ns": report.first_event_ns,
                    "last_event_ns": report.last_event_ns,
                    "last_receive_ns": report.last_receive_ns,
                    "file_bytes": report.file_bytes,
                    "slow_reader_warnings": report.slow_reader_warnings,
                    "subscription_acks": report.subscription_acks,
                    "expected_subscription_acks": len(feed.subscriptions),
                    "replay_completed": report.replay_completed,
                    "gaps_json": json.dumps([*report.incomplete_reasons, *session_issues]),
                    "complete": int(session_complete),
                    "sha256": report.sha256,
                },
            )
            if feed.required and not session_complete:
                errors.append(
                    f"{feed.name}: {', '.join([*report.incomplete_reasons, *session_issues])}"
                )
        except Exception as exc:
            errors.append(f"{feed.name}: {type(exc).__name__}: {exc}")
            self.catalog.update_feed_status(
                self.config.session_id,
                feed.name,
                {"status": "failed", "ended_at_utc": datetime.now(UTC).isoformat(), "error": str(exc)},
            )

    def run(self) -> bool:
        if not self.api_key and self._client_factory is None:
            raise RuntimeError("DATABENTO_API_KEY is required")
        self._acquire_lock()
        errors: list[str] = []
        try:
            self._check_capacity()
            self.catalog.start_session(self.config)
            threads = [
                threading.Thread(target=self._record_feed, args=(feed, errors), name=f"tape-{feed.name}")
                for feed in self.config.feeds
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            success = not errors
            self.catalog.finish_session(
                self.config.session_id,
                "complete" if success else "incomplete",
                "; ".join(errors) or None,
            )
            return success
        finally:
            self._release_lock()
