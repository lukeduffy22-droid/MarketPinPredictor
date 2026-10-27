"""Offline clock failure injection; no provider connection or live database."""
from dataclasses import replace
import os
import subprocess
import threading

import pytest

from backend.host_clock_shadow import (
    HostClockShadow, Observation, compare_clock_evidence, inspect_observation,
    parse_verbose_status, query_windows_time,
)


GOOD = """Leap Indicator: 0(no warning)
Stratum: 2 (secondary reference - syncd by (S)NTP)
Root Delay: 0.024s
Root Dispersion: 0.023s
Source: 216.239.35.8,0x8
Poll Interval: 6 (64s)
Phase Offset: -0.0001s
State Machine: 2 (Sync)
Last Sync Error: 0 (The command completed successfully.)
Time since Last Good Sync Time: 30.0s
"""


def _observation():
    return Observation(42, "instance", 1_000_000_000_000, 5_000_000_000,
                       1_000_020_000_000, 5_020_000_000, parse_verbose_status(GOOD), None, "a" * 64)


def _inspect(observation=None, *, age=0.0, wall_step=0.0, **kwargs):
    observation = observation or _observation()
    return inspect_observation(observation, instance_id="instance", process_id=42,
                               wall_ns=observation.wall_end_ns + int((age + wall_step) * 1e9),
                               monotonic_ns=observation.monotonic_end_ns + int(age * 1e9), **kwargs)


def _provider(count=0, **changes):
    return {"status": "unknown", "sample_count": count, "minimum_samples": 50,
            "material_negative_count": 0, "replay_pending": False,
            "reason": "INSUFFICIENT_LIVE_TIMING_SAMPLES", **changes}


@pytest.mark.parametrize("count", [0, 2, 3, 49])
def test_host_readiness_does_not_depend_on_quote_arrivals(count):
    result = compare_clock_evidence(_inspect(), _provider(count))
    assert result["candidate_clock_bootstrap_ready"]
    assert result["provider_status"] == "unknown"
    assert result["authority"] == "diagnostic_only"
    assert not result["live_admission_changed"]
    assert result["opening_eligibility"] == "not_evaluated"
    assert result["host"]["absolute_accuracy_verified"] is False


@pytest.mark.parametrize("field,value,reason", [
    ("leap", 3, "WINDOWS_TIME_NOT_SYNCHRONIZED"),
    ("leap", 1, "WINDOWS_TIME_NOT_SYNCHRONIZED"),
    ("stratum", 16, "WINDOWS_TIME_NOT_SYNCHRONIZED"),
    ("sync_error", 5, "WINDOWS_TIME_NOT_SYNCHRONIZED"),
    ("state_machine", 1, "WINDOWS_TIME_NOT_SYNCHRONIZED"),
    ("source", "Local CMOS Clock", "HOST_REFERENCE_NOT_EXTERNAL"),
    ("source", "Free-running System Clock", "HOST_REFERENCE_NOT_EXTERNAL"),
    ("last_good_sync_age_seconds", 129, "HOST_SYNC_TOO_OLD"),
    ("last_good_sync_age_seconds", -1, "INVALID_SYNC_AGE"),
    ("poll_seconds", 0, "INVALID_SYNC_AGE"),
    ("root_delay_seconds", -0.01, "INVALID_UNCERTAINTY"),
    ("root_dispersion_seconds", -0.01, "INVALID_UNCERTAINTY"),
    ("root_dispersion_seconds", 0.2, "HOST_REPORTED_UNCERTAINTY_TOO_LARGE"),
    ("phase_offset_seconds", -0.1, "HOST_REPORTED_UNCERTAINTY_TOO_LARGE"),
])
def test_untrustworthy_host_evidence_fails_closed(field, value, reason):
    observation = _observation()
    result = _inspect(replace(observation, state=replace(observation.state, **{field: value})))
    assert result["status"] == "not_ready"
    assert result["reason"] == reason


@pytest.mark.parametrize("age,step,reason", [
    (15.001, 0, "HOST_OBSERVATION_EXPIRED"),
    (300, 0, "HOST_OBSERVATION_EXPIRED"),
    (-0.1, 0, "MONOTONIC_CLOCK_REGRESSED"),
    (1, 0.011, "WALL_MONOTONIC_DIVERGENCE"),
    (1, -0.011, "WALL_MONOTONIC_DIVERGENCE"),
])
def test_stale_suspended_or_stepped_clock_rejected(age, step, reason):
    assert _inspect(age=age, wall_step=step)["reason"] == reason


def test_jump_during_query_and_different_process_cannot_be_reused():
    obs = _observation()
    assert _inspect(replace(obs, wall_start_ns=obs.wall_start_ns - 20_000_000))["reason"] == "WALL_MONOTONIC_DIVERGENCE"
    assert _inspect(replace(obs, process_id=43))["reason"] == "HOST_OBSERVATION_OWNER_MISMATCH"
    assert _inspect(replace(obs, instance_id="another"))["reason"] == "HOST_OBSERVATION_OWNER_MISMATCH"
    assert _inspect(replace(obs, maximum_clock_read_span_seconds=0.002))["reason"] == "LOCAL_CLOCK_READ_TOO_SLOW"


@pytest.mark.skipif(os.name != "nt", reason="Windows precise-clock implementation")
def test_coarse_python_windows_clocks_are_never_used(monkeypatch):
    import time

    monkeypatch.setattr(time, "time_ns", lambda: pytest.fail("coarse wall clock used"))
    monkeypatch.setattr(time, "monotonic_ns", lambda: pytest.fail("GetTickCount64 used"))
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    assert observer.snapshot(_provider())["candidate_clock_bootstrap_ready"]


def test_step_between_queries_is_retained_then_requires_fresh_stable_query(monkeypatch):
    import backend.host_clock_shadow as clocks

    pairs = iter([(1000000000000, 5000000000, 0), (1000010000000, 5010000000, 0),
                  (1005050000000, 10000000000, 0), (1005060000000, 10010000000, 0),
                  (1005060000000, 10010000000, 0),
                  (1010050000000, 15000000000, 0), (1010060000000, 15010000000, 0),
                  (1010060000000, 15010000000, 0)])
    monkeypatch.setattr(clocks, "read_clock_pair", lambda: next(pairs))
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    observer.sample_once()
    assert observer.snapshot(_provider())["host"]["reason"] == "WALL_MONOTONIC_DIVERGENCE"
    observer.sample_once()
    assert observer.snapshot(_provider())["candidate_clock_bootstrap_ready"]


@pytest.mark.parametrize("old,new", [
    ("0.024s", "nans"), ("0.023s", "infs"), ("0.023s", "0,023s"),
    ("Root Delay:", "Root Delay Missing:"), ("Source: 216.239.35.8,0x8", "Source:"),
    ("State Machine:", "Estado:"),
])
def test_malformed_or_localized_status_rejected(old, new):
    with pytest.raises(ValueError):
        parse_verbose_status(GOOD.replace(old, new))


def test_duplicate_fields_rejected():
    with pytest.raises(ValueError):
        parse_verbose_status(GOOD + "Root Delay: 0.0s\n")


@pytest.mark.parametrize("changes", [
    {"replay_pending": True}, {"replay_pending": None}, {"material_negative_count": 1},
    {"material_negative_count": None}, {"status": "unsynchronized"},
    {"status": "synchronized", "sample_count": 50},
    {"sample_count": 50}, {"sample_count": -1}, {"sample_count": True},
    {"minimum_samples": 49}, {"reason": "UNKNOWN"},
])
def test_host_cannot_mask_provider_conflicts_replay_or_malformed_evidence(changes):
    assert not compare_clock_evidence(_inspect(), _provider(**changes))["candidate_clock_bootstrap_ready"]


def test_missing_host_evidence_not_ready():
    observer = HostClockShadow(query=lambda: GOOD)
    assert observer.snapshot(_provider())["host"]["reason"] == "NO_HOST_OBSERVATION"


@pytest.mark.parametrize("error,reason", [
    (subprocess.TimeoutExpired("w32tm", 1), "HOST_QUERY_TIMEOUT"),
    (OSError("permission denied"), "HOST_QUERY_FAILED"),
    (ValueError("parse failure"), "HOST_STATUS_UNPARSEABLE"),
])
def test_failed_refresh_replaces_prior_good_evidence(error, reason):
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    assert observer.snapshot(_provider())["candidate_clock_bootstrap_ready"]
    def fail():
        raise error
    observer._query = fail
    observer.sample_once()
    assert observer.snapshot(_provider())["host"]["reason"] == reason


def test_snapshot_never_executes_query_and_source_changes_revoke_readiness():
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    observer._query = lambda: GOOD.replace("216.239.35.8", "216.239.35.12")
    observer.sample_once()
    assert observer.snapshot(_provider())["host"]["reason"] == "HOST_REFERENCE_CHANGED"
    observer.sample_once()
    observer._query = lambda: pytest.fail("snapshot must perform no I/O")
    assert observer.snapshot(_provider())["candidate_clock_bootstrap_ready"]
    observer.stop()
    assert observer.snapshot(_provider())["host"]["reason"] == "NO_HOST_OBSERVATION"


@pytest.mark.skipif(os.name != "nt", reason="Windows local status query")
def test_query_uses_only_bounded_hidden_local_read(monkeypatch):
    def run(command, **kwargs):
        assert command[1:] == ["/query", "/status", "/verbose"]
        assert kwargs["timeout"] == 1.0 and kwargs["check"]
        assert kwargs["creationflags"] == subprocess.CREATE_NO_WINDOW
        return subprocess.CompletedProcess(command, 0, GOOD, "")
    monkeypatch.setattr(subprocess, "run", run)
    assert query_windows_time() == GOOD


def test_background_start_discards_old_evidence_and_stop_clears_state():
    observer = HostClockShadow(query=lambda: GOOD)
    observer.sample_once()
    entered, release = threading.Event(), threading.Event()
    def query():
        entered.set()
        assert release.wait(2)
        return GOOD
    observer._query = query
    try:
        observer.start()
        assert entered.wait(2)
        assert observer.snapshot(_provider())["host"]["reason"] == "NO_HOST_OBSERVATION"
    finally:
        release.set()
        observer.stop()
    assert not observer._thread.is_alive()
    assert observer.snapshot(_provider())["host"]["reason"] == "NO_HOST_OBSERVATION"
