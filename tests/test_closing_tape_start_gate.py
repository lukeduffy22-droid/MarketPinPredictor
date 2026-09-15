from datetime import date, datetime, timezone
from io import BytesIO
import json
from urllib.error import HTTPError

import pytest

from backend.closing_tape import start_gate
from backend.closing_tape.start_gate import (
    primary_capture_start_decision,
    recorder_start_decision,
)


UTC = timezone.utc
EPOCH = "e" * 64


def _ready_primary_payloads():
    health = {
        "provider": "databento",
        "websocket": "active",
        "handoff_status": "active",
        "subscription_session_state": "regular_session",
        "streaming_active": True,
        "stream_progressing": True,
        "runtime_context_stable": True,
        "subscription_allowed": True,
        "subscription_suppressed": False,
        "subscription_epoch_id": EPOCH,
        "active_generation": 7,
        "symbols_requested": ["SPX", "NDX", "VIX", "RUT"],
        "provider_queue_full_warnings": 0,
        "provider_slow_client_warnings": 0,
        "provider_skipped_record_warnings": 0,
        "provider_skipped_records": 0,
        "reconnect_attempts": 0,
        "connection_limit_rejections_total": 0,
        "connection_limit_consecutive": 0,
        "connection_limit_circuit_state": "closed",
        "connection_limit_retry_not_before_utc": None,
        "connection_limit_cooldown_remaining_seconds": 0.0,
        "last_client_close_status": "not_attempted",
        "last_client_close_elapsed_seconds": None,
        "pre_auth_transport_guard_status": "installed",
        "pre_auth_transport_aborts_total": 0,
        "pre_auth_transport_abort_failures_total": 0,
        "last_pre_auth_transport_event": None,
        "last_pre_auth_transport_reason": None,
        "last_pre_auth_transport_event_utc": None,
        "last_error": None,
        "core_symbol_status": {
            symbol: {"requested": True, "contracts_subscribed": 200}
            for symbol in ("SPX", "NDX", "VIX", "RUT")
        },
        "subscription_staging": {
            "state": "full_active",
            "primary_contract_counts": {
                symbol: 200 for symbol in ("SPX", "NDX", "VIX", "RUT")
            },
        },
        "orb_reference_sampler": {"thread_alive": True, "interval_seconds": 5},
    }
    live = {
        "provider": "databento",
        "stream_connected": True,
        "stream_progressing": True,
        "collection_ready": True,
        "all_configured_collection_ready": True,
        "calculation_ready": True,
        "prediction_pipeline_ok": True,
        "runtime_context_stable": True,
        "handoff_status": "active",
        "subscription_epoch_id": EPOCH,
        "subscription_generation": 7,
        "active_generation": 7,
        "last_error": None,
    }
    opening_start = "2026-08-25T13:30:00+00:00"
    symbols = {}
    for symbol in ("SPX", "NDX", "VIX", "RUT"):
        windows = {}
        for window_name, minutes, samples in (("5m", 5, 60), ("60m", 60, 720)):
            end_minute = 35 if window_name == "5m" else 30
            end_hour = 13 if window_name == "5m" else 14
            windows[window_name] = {
                "duration_minutes": minutes,
                "range_start_utc": opening_start,
                "range_end_utc": (
                    f"2026-08-25T{end_hour:02d}:{end_minute:02d}:00+00:00"
                ),
                "capture_status": "complete",
                "orb_complete": True,
                "clock_status": "closed",
                "current_reference_fresh": True,
                "opening_price": 7_500.0,
                "orb_high": 7_550.0,
                "orb_low": 7_450.0,
                "current_price": 7_510.0,
                "capture_evidence": {
                    "sample_count": samples,
                    "expected_sample_count": samples,
                    "capture_ratio": 1.0,
                    "opening_bucket_present": True,
                    "first_sample_lag_seconds": 0.0,
                    "end_gap_seconds": 5.0,
                    "max_gap_seconds": 5.0,
                },
            }
        symbols[symbol] = {
            "configured": True,
            "trading_date": "2026-08-25",
            "opening_ranges": windows,
            "directional_evidence_eligible": symbol in {"SPX", "NDX"},
            "combined_structure_directional_evidence_eligible": symbol
            in {"SPX", "NDX"},
            "pin_behavior": {
                "level_availability_status": (
                    "available" if symbol in {"SPX", "NDX"} else "unavailable"
                ),
                "gamma_pin": 7_525.0 if symbol in {"SPX", "NDX"} else None,
                "max_pain": 7_500.0 if symbol in {"SPX", "NDX"} else None,
            },
            "last_known_reference": {"runtime_aligned": True},
            "provenance": {
                "runtime_binding_applied": True,
                "range_provenance_aligned": True,
                "current_vs_range_aligned": True,
                "active_runtime_epoch_aligned": True,
                "active_subscription_epoch_id": EPOCH,
                "active_subscription_generation": 7,
            },
        }
    orb = {
        "schema_version": "marketpin-reference-orb.collection.v2",
        "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
        "requested_symbols": ["SPX", "NDX", "VIX", "RUT"],
        "runtime_binding_applied": True,
        "runtime_context_stable": True,
        "active_runtime_context": {
            "subscription_epoch_id": EPOCH,
            "subscription_generation": 7,
            "handoff_status": "active",
        },
        "symbols": symbols,
    }
    return health, live, orb


def test_primary_capture_gate_accepts_clean_all_four_exact_opening_ranges():
    health, live, orb = _ready_primary_payloads()

    decision = primary_capture_start_decision(
        health_payload=health,
        live_payload=live,
        orb_payload=orb,
        trading_day=date(2026, 8, 25),
    )

    assert decision["allowed"] is True
    assert decision["state"] == "primary_capture_ready"
    assert decision["issues"] == []


@pytest.mark.parametrize(
    ("mutate", "expected_issue"),
    [
        (
            lambda health, _live, _orb: health.__setitem__(
                "provider_queue_full_warnings", 1
            ),
            "PRIMARY_HEALTH_PROVIDER_QUEUE_FULL_WARNINGS_NONZERO",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "reconnect_attempts", 1
            ),
            "PRIMARY_HEALTH_RECONNECT_ATTEMPTS_NONZERO",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "connection_limit_rejections_total", 1
            ),
            "PRIMARY_HEALTH_CONNECTION_LIMIT_REJECTIONS_TOTAL_NONZERO",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "connection_limit_circuit_state", "open"
            ),
            "PRIMARY_CONNECTION_LIMIT_CIRCUIT_NOT_CLOSED",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "pre_auth_transport_guard_status", "unavailable"
            ),
            "PRIMARY_PRE_AUTH_TRANSPORT_GUARD_NOT_INSTALLED",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "pre_auth_transport_abort_failures_total", 1
            ),
            "PRIMARY_HEALTH_PRE_AUTH_TRANSPORT_ABORT_FAILURES_TOTAL_NONZERO",
        ),
        (
            lambda health, _live, _orb: health.__setitem__(
                "last_client_close_status", "pre_auth_close_unacknowledged"
            ),
            "PRIMARY_CLIENT_CLOSE_LIFECYCLE_NOT_CLEAN",
        ),
        (
            lambda health, _live, _orb: health["subscription_staging"].__setitem__(
                "state", "primary_active"
            ),
            "PRIMARY_STAGED_SUBSCRIPTION_NOT_FULL_ACTIVE",
        ),
        (
            lambda _health, _live, orb: orb["symbols"]["RUT"]["opening_ranges"][
                "60m"
            ].__setitem__("capture_status", "partial"),
            "PRIMARY_ORB_60M_INCOMPLETE:RUT",
        ),
    ],
)
def test_primary_capture_gate_defers_on_pressure_or_incomplete_orb(
    mutate, expected_issue
):
    health, live, orb = _ready_primary_payloads()
    mutate(health, live, orb)

    decision = primary_capture_start_decision(
        health_payload=health,
        live_payload=live,
        orb_payload=orb,
        trading_day=date(2026, 8, 25),
    )

    assert decision["allowed"] is False
    assert decision["state"] == "primary_capture_not_ready"
    assert expected_issue in decision["issues"]


def test_cli_primary_readiness_failure_is_retryable_defer(monkeypatch, capsys):
    monkeypatch.setattr(
        start_gate,
        "recorder_start_decision",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "state": "allowed",
            "reason": None,
            "trading_date": "2026-08-25",
        },
    )
    monkeypatch.setattr(
        start_gate,
        "probe_primary_capture_readiness",
        lambda **_kwargs: {
            "allowed": False,
            "state": "primary_capture_not_ready",
            "reason": "PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:TimeoutError",
            "issues": ["PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:TimeoutError"],
        },
    )

    exit_code = start_gate.main(
        [
            "--project-root",
            ".",
            "--trading-date",
            "2026-08-25",
            "--require-primary-ready",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 4
    assert payload["allowed"] is False
    assert payload["state"] == "primary_capture_not_ready"


def test_cli_rechecks_time_gate_after_successful_endpoint_probe(monkeypatch, capsys):
    decisions = iter(
        (
            {
                "allowed": True,
                "state": "allowed",
                "reason": None,
                "trading_date": "2026-08-25",
            },
            {
                "allowed": False,
                "state": "too_late",
                "reason": "close-minus-15 analysis cutoff has passed",
                "trading_date": "2026-08-25",
            },
        )
    )
    monkeypatch.setattr(
        start_gate,
        "recorder_start_decision",
        lambda *_args, **_kwargs: next(decisions),
    )
    monkeypatch.setattr(
        start_gate,
        "probe_primary_capture_readiness",
        lambda **_kwargs: {
            "allowed": True,
            "state": "primary_capture_ready",
            "reason": None,
            "issues": [],
        },
    )

    exit_code = start_gate.main(
        [
            "--project-root",
            ".",
            "--trading-date",
            "2026-08-25",
            "--require-primary-ready",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 3
    assert payload["allowed"] is False
    assert payload["state"] == "too_late"
    assert payload["primary_capture"]["state"] == "primary_capture_ready"


def test_cli_without_primary_flag_preserves_pure_owner_gate(monkeypatch, capsys):
    monkeypatch.setattr(
        start_gate,
        "recorder_start_decision",
        lambda *_args, **_kwargs: {
            "allowed": True,
            "state": "allowed",
            "reason": None,
            "trading_date": "2026-08-25",
        },
    )
    monkeypatch.setattr(
        start_gate,
        "probe_primary_capture_readiness",
        lambda **_kwargs: pytest.fail("opt-in primary probe ran without its flag"),
    )

    exit_code = start_gate.main(
        ["--project-root", ".", "--trading-date", "2026-08-25"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["allowed"] is True
    assert "primary_capture" not in payload


def test_primary_probe_rejects_non_loopback_url_without_fetching(monkeypatch):
    monkeypatch.setattr(
        start_gate,
        "_fetch_json",
        lambda *_args: pytest.fail("non-loopback URL reached endpoint fetch"),
    )

    decision = start_gate.probe_primary_capture_readiness(
        backend_url="https://example.com",
        timeout_seconds=2.0,
        trading_day=date(2026, 8, 25),
    )

    assert decision["allowed"] is False
    assert decision["state"] == "primary_capture_not_ready"
    assert decision["issues"] == [
        "PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:ValueError"
    ]


class _ProbeClock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _runtime_timeout_error():
    return HTTPError(
        "http://127.0.0.1:8000/v1/orb", 503, "unavailable", {},
        BytesIO(json.dumps({"detail": {"reason": "RUNTIME_READ_TIMEOUT"}}).encode()),
    )


def test_primary_probe_retries_transient_orb_read_without_bypassing_gates(monkeypatch):
    payloads = _ready_primary_payloads()
    payloads[0]["provider_queue_full_warnings"] = 1
    probe_clock = _ProbeClock()
    monkeypatch.setattr(start_gate, "clock", probe_clock)
    calls = []

    def fetch(url, timeout):
        calls.append((url, timeout))
        if url.endswith("/health"):
            return payloads[0]
        if url.endswith("/health/live"):
            return payloads[1]
        if len(calls) == 3:
            probe_clock.now += 1.0
            raise _runtime_timeout_error()
        return payloads[2]

    monkeypatch.setattr(start_gate, "_fetch_json", fetch)
    result = start_gate.probe_primary_capture_readiness(
        backend_url="http://127.0.0.1:8000", timeout_seconds=2,
        trading_day=date(2026, 8, 25),
    )

    assert result["allowed"] is False
    assert "PRIMARY_HEALTH_PROVIDER_QUEUE_FULL_WARNINGS_NONZERO" in result["issues"]
    assert [row["endpoint"] for row in result["endpoint_checks"]] == ["/health", "/health/live", "/v1/orb"]
    assert result["endpoint_checks"][-1]["attempts"] == [
        {"status": "unavailable", "failure": "HTTP_503:RUNTIME_READ_TIMEOUT"},
        {"status": "available"},
    ]
    assert calls[-1][1] == pytest.approx(1.0)
    assert len(calls) == 4


def test_primary_probe_failure_identifies_endpoint_and_sanitized_status(monkeypatch):
    payloads = _ready_primary_payloads()
    monkeypatch.setattr(start_gate, "clock", _ProbeClock())
    calls = []

    def fetch(url, timeout):
        calls.append(url)
        if url.endswith("/health"):
            return payloads[0]
        if url.endswith("/health/live"):
            return payloads[1]
        raise _runtime_timeout_error()

    monkeypatch.setattr(start_gate, "_fetch_json", fetch)
    result = start_gate.probe_primary_capture_readiness(
        backend_url="http://127.0.0.1:8000", timeout_seconds=2,
        trading_day=date(2026, 8, 25),
    )

    assert result["allowed"] is False
    assert result["issues"] == ["PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:/v1/orb:HTTP_503:RUNTIME_READ_TIMEOUT"]
    assert len(result["endpoint_checks"][-1]["attempts"]) == 2
    assert len(calls) == 4


def test_endpoint_timeout_retry_shares_three_second_budget(monkeypatch):
    probe_clock = _ProbeClock()
    monkeypatch.setattr(start_gate, "clock", probe_clock)
    timeouts = []

    def fetch(_url, timeout):
        timeouts.append(timeout)
        probe_clock.now += timeout
        raise TimeoutError("private exception text must not enter diagnostics")

    monkeypatch.setattr(start_gate, "_fetch_json", fetch)
    payload, check = start_gate._probe_endpoint("http://127.0.0.1:8000/health", 2)

    assert payload is None
    assert timeouts == pytest.approx([2.0, 0.95])
    assert check["elapsed_seconds"] == 3.0
    assert "private" not in json.dumps(check)


@pytest.mark.parametrize("failure", [ValueError("bad JSON"), HTTPError("http://127.0.0.1", 302, "redirect", {}, BytesIO())])
def test_endpoint_does_not_retry_invalid_payload_or_redirect(monkeypatch, failure):
    calls = []

    def fetch(_url, _timeout):
        calls.append(True)
        raise failure

    monkeypatch.setattr(start_gate, "_fetch_json", fetch)
    payload, check = start_gate._probe_endpoint("http://127.0.0.1:8000/health", 2)

    assert payload is None
    assert check["status"] == "unavailable"
    assert len(calls) == 1


def test_readiness_fetch_disables_proxies_and_redirects(monkeypatch):
    handlers_seen = []

    class Response(BytesIO):
        status = 200

    class Opener:
        def open(self, request, timeout):
            assert request.full_url == "http://127.0.0.1:8000/health"
            assert timeout == 2
            return Response(b'{"status":"healthy"}')

    def build(*handlers):
        handlers_seen.extend(handlers)
        return Opener()

    monkeypatch.setattr(start_gate, "build_opener", build)
    result = start_gate._fetch_json("http://127.0.0.1:8000/health", 2)

    assert result == {"status": "healthy"}
    assert handlers_seen[0].proxies == {}
    assert handlers_seen[1].redirect_request(None, None, 302, None, {}, "https://example.com") is None


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -float("inf")])
def test_primary_probe_rejects_nonfinite_timeout_without_fetching(monkeypatch, timeout):
    monkeypatch.setattr(start_gate, "_fetch_json", lambda *_args: pytest.fail("invalid timeout reached HTTP"))
    result = start_gate.probe_primary_capture_readiness(
        backend_url="http://127.0.0.1:8000", timeout_seconds=timeout,
        trading_day=date(2026, 8, 25),
    )
    assert result["allowed"] is False
    assert result["issues"] == ["PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:ValueError"]


def test_start_gate_defers_during_opening_orb_protection_window(tmp_path):
    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 14, 34, 59, tzinfo=UTC),
    )

    assert decision["allowed"] is False
    assert decision["state"] == "not_yet_due"
    assert decision["start_not_before_utc"] == "2026-08-25T14:35:00+00:00"
    assert "opening gamma" in decision["reason"]


def test_start_gate_allows_launch_after_opening_orb_and_before_close_minus_15(tmp_path):
    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 14, 35, tzinfo=UTC),
    )

    assert decision["allowed"] is True
    assert decision["state"] == "allowed"
    assert decision["analysis_due_utc"] == "2026-08-25T19:45:00+00:00"
    assert decision["prior_nonempty_dbn_count"] == 0


def test_start_gate_blocks_automatic_replay_when_prior_same_day_dbn_exists(tmp_path):
    day_dir = tmp_path / "data" / "closing_tape" / "2026-08-25"
    day_dir.mkdir(parents=True)
    (day_dir / "opra_options.orphaned.dbn").write_bytes(b"DBN prior evidence")

    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 19, 42, 38, tzinfo=UTC),
    )

    assert decision["allowed"] is False
    assert decision["state"] == "recovery_replay_blocked"
    assert decision["prior_nonempty_dbn_count"] == 1
    assert "automatic cash-open replay recovery is blocked" in decision["reason"]


def test_start_gate_ignores_zero_byte_dbn_from_failed_first_attempt(tmp_path):
    day_dir = tmp_path / "data" / "closing_tape" / "2026-08-25"
    day_dir.mkdir(parents=True)
    (day_dir / "empty-attempt.dbn").touch()

    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 14, 35, tzinfo=UTC),
    )

    assert decision["allowed"] is True
    assert decision["state"] == "allowed"
    assert decision["prior_nonempty_dbn_count"] == 0


def test_start_gate_refuses_partial_launch_at_analysis_cutoff(tmp_path):
    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 25),
        now=datetime(2026, 8, 25, 19, 45, tzinfo=UTC),
    )

    assert decision["allowed"] is False
    assert decision["state"] == "too_late"
    assert decision["reason"] == "close-minus-15 analysis cutoff has passed"


def test_start_gate_refuses_non_session_day(tmp_path):
    decision = recorder_start_decision(
        tmp_path,
        trading_day=date(2026, 8, 29),
        now=datetime(2026, 8, 29, 15, 0, tzinfo=UTC),
    )

    assert decision["allowed"] is False
    assert "not a configured US cash-market session" in decision["reason"]


def test_start_gate_rejects_naive_operator_time(tmp_path):
    with pytest.raises(ValueError, match="timezone-aware"):
        recorder_start_decision(
            tmp_path,
            trading_day=date(2026, 8, 25),
            now=datetime(2026, 8, 25, 15, 0),
        )
