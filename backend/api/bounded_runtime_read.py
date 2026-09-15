"""Bound synchronous runtime reads without occupying the API event loop."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
import inspect
import math
import threading
import time
from typing import Callable, Hashable, TypeVar

from fastapi import HTTPException


T = TypeVar("T")


class RuntimeReadUnavailable(RuntimeError):
    """A read could not establish current runtime state within its budget."""


@dataclass(frozen=True)
class _Read:
    future: Future
    deadline: float


class BoundedRuntimeReads:
    """Coalesce identical reads; admit at most ``max_workers`` unfinished jobs.

    A caller's cancellation or deadline cannot stop synchronous lock/I/O work.
    Capacity is therefore released by actual worker completion, never by the
    caller. Completed results are not cached or presented as a newer sample.
    """

    def __init__(self, *, max_workers: int = 2, timeout_seconds: float = 1.0):
        if max_workers < 1 or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("Runtime read limits must be positive")
        self._max_workers = max_workers
        self._timeout_seconds = timeout_seconds
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="runtime-read",
        )
        self._lock = threading.Lock()
        self._reads: dict[Hashable, _Read] = {}

    def _finished(self, key: Hashable, read: _Read) -> None:
        with self._lock:
            if self._reads.get(key) is read:
                self._reads.pop(key)

    async def run(self, key: Hashable, reader: Callable[[], T]) -> T:
        admission_deadline = time.monotonic() + self._timeout_seconds
        created = False
        while True:
            with self._lock:
                # A worker marks its Future done before its completion callback
                # can acquire this lock and remove the entry.  Never let a new
                # request mistake that cleanup window for occupied capacity or
                # reuse a completed (therefore older) runtime snapshot.
                for completed_key, completed_read in tuple(self._reads.items()):
                    if (
                        completed_read.future.done()
                        and self._reads.get(completed_key) is completed_read
                    ):
                        self._reads.pop(completed_key, None)
                read = self._reads.get(key)
                if read is None and len(self._reads) < self._max_workers:
                    read = _Read(self._executor.submit(reader), admission_deadline)
                    self._reads[key] = read
                    created = True
            if read is not None:
                break
            remaining = admission_deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeReadUnavailable("RUNTIME_READ_BUSY")
            # Wait in the existing request coroutine, without submitting or
            # creating any queued/background task for excess requests.
            await asyncio.sleep(min(0.01, remaining))
        # add_done_callback may run immediately; do not register under _lock.
        if created:
            read.future.add_done_callback(lambda _future: self._finished(key, read))

        # Polling a concurrent Future adds no cancellation-linked wrapper or
        # abandoned asyncio task per HTTP/SSE caller when a worker stays stuck.
        while not read.future.done():
            remaining = read.deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeReadUnavailable("RUNTIME_READ_TIMEOUT")
            await asyncio.sleep(min(0.01, remaining))
        if time.monotonic() > read.deadline:
            raise RuntimeReadUnavailable("RUNTIME_READ_TIMEOUT")
        try:
            return read.future.result()
        except HTTPException:
            raise  # Preserve existing 404/409/503 and their evidence reasons.
        except Exception as exc:
            raise RuntimeReadUnavailable("RUNTIME_READ_FAILED") from exc

    def close(self) -> None:
        """Join completed test/service workers; callers must release test locks."""
        self._executor.shutdown(wait=True, cancel_futures=True)


runtime_reads = BoundedRuntimeReads(max_workers=2, timeout_seconds=1.0)


def runtime_read_unavailable_payload(exc: RuntimeReadUnavailable) -> dict:
    return {
        "status": "unavailable", "reason": str(exc),
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "prediction_pipeline_ok": False, "runtime_context_stable": False,
        "usable_for_prediction": False,
    }


async def read_runtime_or_503(key: Hashable, reader: Callable[[], T]) -> T:
    try:
        return await runtime_reads.run(key, reader)
    except RuntimeReadUnavailable as exc:
        raise HTTPException(
            status_code=503, detail=runtime_read_unavailable_payload(exc),
            headers={"Retry-After": "1"},
        ) from exc


def bounded_runtime_endpoint(reader):
    """Make an otherwise synchronous runtime projection an isolated async API."""
    @wraps(reader)
    async def endpoint(*args, **kwargs):
        key = (reader.__module__, reader.__qualname__, args, tuple(sorted(kwargs.items())))
        return await read_runtime_or_503(key, lambda: reader(*args, **kwargs))
    # FastAPI resolves postponed annotations against the endpoint globals.
    # Evaluate in the original module, where its date/schema types are defined.
    endpoint.__signature__ = inspect.signature(reader, eval_str=True)
    return endpoint
