"""Independent Windows clock evidence; deliberately has NO admission authority.

This is the measurement stage of a new opening-clock policy. Provider evidence
keeps its existing meaning and 50-sample threshold. A candidate host result must
never be substituted for ``processing_clock_status`` in a persisted sample.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, replace
import ctypes
import hashlib
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Callable
import uuid


@dataclass(frozen=True)
class ShadowPolicy:
    # Proposed measurement limits, not certified absolute clock accuracy.
    evidence_ttl_seconds: float = 15.0
    maximum_sync_age_seconds: float = 128.0
    uncertainty_budget_seconds: float = 0.050
    maximum_wall_step_seconds: float = 0.010
    holdover_drift_per_second: float = 15e-6


@dataclass(frozen=True)
class HostState:
    leap: int
    stratum: int
    sync_error: int
    state_machine: int
    source: str
    poll_seconds: int
    last_good_sync_age_seconds: float
    root_delay_seconds: float
    root_dispersion_seconds: float
    phase_offset_seconds: float


@dataclass(frozen=True)
class Observation:
    process_id: int
    instance_id: str
    wall_start_ns: int
    monotonic_start_ns: int
    wall_end_ns: int
    monotonic_end_ns: int
    state: HostState | None
    reason: str | None
    raw_sha256: str
    maximum_clock_read_span_seconds: float = 0.0
    local_clock_basis: str = "GetSystemTimePreciseAsFileTime/QueryPerformanceCounter"


if os.name == "nt":
    _precise_time = ctypes.WinDLL("kernel32", use_last_error=True).GetSystemTimePreciseAsFileTime
    _precise_time.argtypes = [ctypes.POINTER(ctypes.c_ulonglong)]
    _precise_time.restype = None


def read_clock_pair() -> tuple[int, int, float]:
    """UTC plus bracketed high-resolution monotonic time, never GetTickCount64.

    Python 3.11 time.time/monotonic report 15.625 ms resolution on this Windows
    host. That is too coarse to test a 10 ms discontinuity. Resolution is still
    distinct from UTC accuracy; W32Time evidence is evaluated separately.
    """
    before = time.perf_counter_ns()
    if os.name == "nt":
        file_time = ctypes.c_ulonglong()
        _precise_time(ctypes.byref(file_time))
        wall = (file_time.value - 116444736000000000) * 100
    else:
        wall = time.time_ns()
    after = time.perf_counter_ns()
    return wall, (before + after) // 2, (after - before) / 1e9


def parse_verbose_status(raw: str) -> HostState:
    """Fail closed on missing, duplicate, localized or malformed fields."""
    def field(label: str, pattern: str) -> str:
        lines = re.findall(r"(?m)^\s*" + re.escape(label) + r":([^\r\n]*)$", raw)
        if len(lines) != 1:
            raise ValueError("missing or duplicate field: " + label)
        match = re.fullmatch(pattern, lines[0].strip())
        if match is None:
            raise ValueError("malformed field: " + label)
        return match.group(1)

    def integer(label: str) -> int:
        return int(field(label, r"(\d+)(?:\s*\([^\r\n]*\))?"))

    def seconds(label: str) -> float:
        value = float(field(label, r"([+-]?(?:\d+(?:\.\d*)?|\.\d+))s"))
        if not math.isfinite(value):
            raise ValueError("nonfinite field: " + label)
        return value

    return HostState(
        leap=integer("Leap Indicator"), stratum=integer("Stratum"),
        sync_error=integer("Last Sync Error"), state_machine=integer("State Machine"),
        source=field("Source", r"(.+)"),
        poll_seconds=int(field("Poll Interval", r"\d+\s*\((\d+)s\)")),
        last_good_sync_age_seconds=seconds("Time since Last Good Sync Time"),
        root_delay_seconds=seconds("Root Delay"),
        root_dispersion_seconds=seconds("Root Dispersion"),
        phase_offset_seconds=seconds("Phase Offset"),
    )


def query_windows_time() -> str:
    """Bounded, local, read-only query; never resync or contact a provider."""
    if os.name != "nt":
        raise OSError("WINDOWS_TIME_UNAVAILABLE")
    executable = Path(os.environ.get("SystemRoot", r"C:\Windows")) / "System32/w32tm.exe"
    result = subprocess.run(
        [str(executable), "/query", "/status", "/verbose"],
        capture_output=True, text=True, errors="replace", timeout=1.0,
        check=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    return result.stdout


def inspect_observation(
    observation: Observation | None, *, instance_id: str, process_id: int,
    wall_ns: int, monotonic_ns: int, policy: ShadowPolicy = ShadowPolicy(),
) -> dict[str, object]:
    result: dict[str, object] = {
        "status": "unavailable", "reason": "NO_HOST_OBSERVATION",
        "policy": asdict(policy), "absolute_accuracy_verified": False,
    }
    if observation is None:
        return result
    result["observation"] = asdict(observation)
    def reject(reason: str) -> dict[str, object]:
        result.update(status="not_ready", reason=reason)
        return result
    if observation.instance_id != instance_id or observation.process_id != process_id:
        return reject("HOST_OBSERVATION_OWNER_MISMATCH")
    if observation.maximum_clock_read_span_seconds > 0.001:
        return reject("LOCAL_CLOCK_READ_TOO_SLOW")
    if observation.reason:
        return reject(observation.reason)
    state = observation.state
    if state is None:
        return reject("HOST_STATUS_MISSING")
    query_elapsed = (observation.monotonic_end_ns - observation.monotonic_start_ns) / 1e9
    age = (monotonic_ns - observation.monotonic_end_ns) / 1e9
    result.update(age_seconds=age, query_elapsed_seconds=query_elapsed)
    if query_elapsed < 0 or age < 0:
        return reject("MONOTONIC_CLOCK_REGRESSED")
    if query_elapsed > 1.5 or age > policy.evidence_ttl_seconds:
        return reject("HOST_OBSERVATION_EXPIRED")
    query_step = (observation.wall_end_ns - observation.wall_start_ns) / 1e9 - query_elapsed
    since_step = (wall_ns - observation.wall_end_ns) / 1e9 - age
    result.update(query_wall_step_seconds=query_step, since_query_wall_step_seconds=since_step)
    if max(abs(query_step), abs(since_step)) > policy.maximum_wall_step_seconds:
        return reject("WALL_MONOTONIC_DIVERGENCE")
    if state.leap != 0 or not 1 <= state.stratum <= 15 or state.sync_error != 0 or state.state_machine != 2:
        return reject("WINDOWS_TIME_NOT_SYNCHRONIZED")
    source = state.source.lower().strip()
    if any(token in source for token in ("local", "free-running", "cmos", "vm ic", "0.0.0.0", "127.0.0.1", "::1")):
        return reject("HOST_REFERENCE_NOT_EXTERNAL")
    if state.poll_seconds <= 0 or state.last_good_sync_age_seconds < 0:
        return reject("INVALID_SYNC_AGE")
    sync_age = state.last_good_sync_age_seconds + query_elapsed + age
    if sync_age > min(2 * state.poll_seconds, policy.maximum_sync_age_seconds):
        return reject("HOST_SYNC_TOO_OLD")
    if min(state.root_delay_seconds, state.root_dispersion_seconds) < 0:
        return reject("INVALID_UNCERTAINTY")
    # Reported NTP metrics + phase offset + a proposed post-query holdover margin.
    # This is an engineering screening estimate, NOT an independent measurement
    # of true UTC error. Shadow results cannot authorize capture.
    estimate = (state.root_dispersion_seconds + state.root_delay_seconds / 2
                + abs(state.phase_offset_seconds)
                + (query_elapsed + age) * policy.holdover_drift_per_second)
    result["reported_uncertainty_estimate_seconds"] = estimate
    if estimate > policy.uncertainty_budget_seconds:
        return reject("HOST_REPORTED_UNCERTAINTY_TOO_LARGE")
    result.update(status="candidate_ready", reason=None)
    return result


def compare_clock_evidence(host: dict[str, object], provider: dict[str, object]) -> dict[str, object]:
    """A counterfactual clock-only decision, never an opening eligibility vote."""
    reason = "PROVIDER_EVIDENCE_NOT_BOOTSTRAPPABLE"
    count = provider.get("sample_count")
    valid_count = type(count) is int and 0 <= count < 50
    if provider.get("replay_pending") is not False:
        reason = "REPLAY_OR_REPLAY_STATE_UNKNOWN"
    elif type(provider.get("material_negative_count")) is not int:
        reason = "PROVIDER_CONFLICT_EVIDENCE_MISSING"
    elif provider["material_negative_count"] != 0:
        reason = "MATERIAL_NEGATIVE_PROVIDER_LAG_VETO"
    elif provider.get("status") == "synchronized":
        reason = "PROVIDER_ALREADY_READY"
    elif host.get("status") != "candidate_ready":
        reason = str(host.get("reason") or "HOST_NOT_READY")
    elif (provider.get("status") == "unknown" and valid_count
          and provider.get("minimum_samples") == 50
          and provider.get("reason") == "INSUFFICIENT_LIVE_TIMING_SAMPLES"):
        reason = "HOST_READY_PROVIDER_STILL_WARMING"
    return {
        "schema_version": "marketpin-host-clock-shadow.v1",
        "authority": "diagnostic_only", "live_admission_changed": False,
        "candidate_clock_bootstrap_ready": reason == "HOST_READY_PROVIDER_STILL_WARMING",
        "reason": reason, "host": host,
        "provider_status": provider.get("status"),
        "provider_sample_count": count,
        "opening_eligibility": "not_evaluated",
    }


class HostClockShadow:
    """Optional background observer. Reads never execute a command or await I/O."""
    def __init__(self, query: Callable[[], str] = query_windows_time):
        self._query = query
        self._instance_id = uuid.uuid4().hex
        self._observation: Observation | None = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def sample_once(self) -> None:
        wall_start, mono_start, span_start = read_clock_pair()
        state, raw, reason = None, "", None
        try:
            raw = self._query()
            state = parse_verbose_status(raw)
        except subprocess.TimeoutExpired:
            reason = "HOST_QUERY_TIMEOUT"
        except (OSError, subprocess.SubprocessError):
            reason = "HOST_QUERY_FAILED"
        except (ValueError, TypeError):
            reason = "HOST_STATUS_UNPARSEABLE"
        wall_end, mono_end, span_end = read_clock_pair()
        observation = Observation(os.getpid(), self._instance_id, wall_start, mono_start,
                                  wall_end, mono_end, state, reason,
                                  hashlib.sha256(raw.encode("utf-8")).hexdigest(),
                                  max(span_start, span_end))
        with self._lock:
            previous = self._observation
            if reason is None and previous is not None:
                step = ((wall_end - previous.wall_end_ns)
                        - (mono_end - previous.monotonic_end_ns)) / 1e9
                if abs(step) > ShadowPolicy().maximum_wall_step_seconds:
                    observation = replace(observation, reason="WALL_MONOTONIC_DIVERGENCE")
                elif previous.state is not None and state.source != previous.state.source:
                    observation = replace(observation, reason="HOST_REFERENCE_CHANGED")
            # A failed query replaces good evidence; there is no stale success
            # fallback, on-disk cache, or reuse across observer instances.
            self._observation = observation

    def snapshot(self, provider: dict[str, object]) -> dict[str, object]:
        with self._lock:
            observation = self._observation
        wall, monotonic, span = read_clock_pair()
        host = inspect_observation(observation, instance_id=self._instance_id,
                                   process_id=os.getpid(), wall_ns=wall,
                                   monotonic_ns=monotonic)
        if span > 0.001:
            host.update(status="not_ready", reason="LOCAL_CLOCK_READ_TOO_SLOW")
        return compare_clock_evidence(host, provider)

    def _run(self) -> None:
        while not self._stop.is_set():
            self.sample_once()
            self._stop.wait(5.0)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        with self._lock:
            self._observation = None
            self._instance_id = uuid.uuid4().hex
        self._thread = threading.Thread(target=self._run, name="host-clock-shadow", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        with self._lock:
            self._observation = None
