from __future__ import annotations

import argparse
import json
import math
import time as clock
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .config import ET, UTC, build_session_config


# The primary live backend owns the opening hour. The closing-tape stream can
# request a same-day replay from the cash open, so starting it immediately
# after the 60-minute ORB is complete preserves the full tape without making a
# second broad OPRA client compete with opening gamma/ORB capture.
AUTOMATIC_START_NOT_BEFORE_ET = time(10, 35)
PRIMARY_CAPTURE_FAMILIES = ("SPX", "NDX", "VIX", "RUT")
PRIMARY_CAPTURE_ORB_WINDOWS = {"5m": 60, "60m": 720}
PRIMARY_READINESS_TIMEOUT_SECONDS = 2.0
PRIMARY_READINESS_MAX_TIMEOUT_SECONDS = 3.0
PRIMARY_READINESS_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
PRIMARY_READINESS_TRANSIENT_REASONS = {
    "RUNTIME_READ_BUSY", "RUNTIME_READ_TIMEOUT", "RUNTIME_READ_FAILED"
}


def _sha256_identity(value: object) -> bool:
    candidate = str(value or "").strip()
    return bool(
        len(candidate) == 64
        and all(character in "0123456789abcdef" for character in candidate)
    )


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _symbol_set(value: object) -> set[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return set()
    return {str(symbol).strip().upper() for symbol in value if str(symbol).strip()}


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        candidate = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return candidate if math.isfinite(candidate) else None


def _aware_datetime(value: object) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo is not None else None


def primary_capture_start_decision(
    *,
    health_payload: object,
    live_payload: object,
    orb_payload: object,
    trading_day: date,
) -> dict[str, object]:
    """Validate the already-running primary before an optional OPRA client starts.

    This function is pure: callers supply retained HTTP payloads and it neither
    opens a provider connection nor mutates runtime or database state.
    """

    health = _mapping(health_payload)
    live = _mapping(live_payload)
    orb = _mapping(orb_payload)
    issues: list[str] = []

    epoch = health.get("subscription_epoch_id")
    generation = health.get("active_generation")
    if not _sha256_identity(epoch):
        issues.append("PRIMARY_SUBSCRIPTION_EPOCH_INVALID")
    if type(generation) is not int or generation <= 0:
        issues.append("PRIMARY_SUBSCRIPTION_GENERATION_INVALID")

    expected_health = {
        "provider": "databento",
        "websocket": "active",
        "handoff_status": "active",
        "subscription_session_state": "regular_session",
    }
    for field, expected in expected_health.items():
        if str(health.get(field) or "").strip().lower() != expected:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_INVALID")
    for field in (
        "streaming_active",
        "stream_progressing",
        "runtime_context_stable",
        "subscription_allowed",
    ):
        if health.get(field) is not True:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_NOT_READY")
    if health.get("subscription_suppressed") is not False:
        issues.append("PRIMARY_HEALTH_SUBSCRIPTION_SUPPRESSED")
    if str(health.get("last_error") or "").strip():
        issues.append("PRIMARY_HEALTH_LAST_ERROR_PRESENT")
    for field in (
        "provider_queue_full_warnings",
        "provider_slow_client_warnings",
        "provider_skipped_record_warnings",
        "provider_skipped_records",
        "reconnect_attempts",
        "connection_limit_rejections_total",
        "connection_limit_consecutive",
        "pre_auth_transport_aborts_total",
        "pre_auth_transport_abort_failures_total",
    ):
        if type(health.get(field)) is not int or health.get(field) != 0:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_NONZERO")
    if str(health.get("connection_limit_circuit_state") or "").lower() != "closed":
        issues.append("PRIMARY_CONNECTION_LIMIT_CIRCUIT_NOT_CLOSED")
    if health.get("connection_limit_retry_not_before_utc") is not None:
        issues.append("PRIMARY_CONNECTION_LIMIT_RETRY_PENDING")
    cooldown = _finite(health.get("connection_limit_cooldown_remaining_seconds"))
    if cooldown is None or cooldown != 0.0:
        issues.append("PRIMARY_CONNECTION_LIMIT_COOLDOWN_ACTIVE_OR_INVALID")
    if health.get("pre_auth_transport_guard_status") != "installed":
        issues.append("PRIMARY_PRE_AUTH_TRANSPORT_GUARD_NOT_INSTALLED")
    if health.get("last_client_close_status") != "not_attempted":
        issues.append("PRIMARY_CLIENT_CLOSE_LIFECYCLE_NOT_CLEAN")
    if health.get("last_client_close_elapsed_seconds") is not None:
        issues.append("PRIMARY_CLIENT_CLOSE_ELAPSED_UNEXPECTED")
    for field in (
        "last_pre_auth_transport_event",
        "last_pre_auth_transport_reason",
        "last_pre_auth_transport_event_utc",
    ):
        if health.get(field) is not None:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_PRESENT")

    required = set(PRIMARY_CAPTURE_FAMILIES)
    if not required.issubset(_symbol_set(health.get("symbols_requested"))):
        issues.append("PRIMARY_HEALTH_REQUIRED_FAMILIES_MISSING")
    family_status = _mapping(health.get("core_symbol_status"))
    for symbol in PRIMARY_CAPTURE_FAMILIES:
        status = _mapping(family_status.get(symbol))
        if (
            status.get("requested") is not True
            or type(status.get("contracts_subscribed")) is not int
            or int(status.get("contracts_subscribed") or 0) <= 0
        ):
            issues.append(f"PRIMARY_FAMILY_SUBSCRIPTION_NOT_READY:{symbol}")

    staging = _mapping(health.get("subscription_staging"))
    if str(staging.get("state") or "").lower() != "full_active":
        issues.append("PRIMARY_STAGED_SUBSCRIPTION_NOT_FULL_ACTIVE")
    primary_counts = _mapping(staging.get("primary_contract_counts"))
    for symbol in PRIMARY_CAPTURE_FAMILIES:
        if type(primary_counts.get(symbol)) is not int or int(
            primary_counts.get(symbol) or 0
        ) <= 0:
            issues.append(f"PRIMARY_STAGED_FAMILY_MISSING:{symbol}")

    sampler = _mapping(health.get("orb_reference_sampler"))
    if sampler.get("thread_alive") is not True or sampler.get("interval_seconds") != 5:
        issues.append("PRIMARY_ORB_SAMPLER_NOT_READY")

    expected_live_true = (
        "stream_connected",
        "stream_progressing",
        "collection_ready",
        "all_configured_collection_ready",
        "calculation_ready",
        "prediction_pipeline_ok",
        "runtime_context_stable",
    )
    for field in expected_live_true:
        if live.get(field) is not True:
            issues.append(f"PRIMARY_LIVE_{field.upper()}_NOT_READY")
    if str(live.get("provider") or "").lower() != "databento":
        issues.append("PRIMARY_LIVE_PROVIDER_INVALID")
    if str(live.get("handoff_status") or "").lower() != "active":
        issues.append("PRIMARY_LIVE_HANDOFF_NOT_ACTIVE")
    if str(live.get("last_error") or "").strip():
        issues.append("PRIMARY_LIVE_LAST_ERROR_PRESENT")
    if (
        live.get("subscription_epoch_id") != epoch
        or live.get("subscription_generation") != generation
        or live.get("active_generation") != generation
    ):
        issues.append("PRIMARY_LIVE_RUNTIME_IDENTITY_MISMATCH")

    context = _mapping(orb.get("active_runtime_context"))
    if (
        orb.get("schema_version") != "marketpin-reference-orb.collection.v2"
        or orb.get("runtime_binding_applied") is not True
        or orb.get("runtime_context_stable") is not True
        or context.get("subscription_epoch_id") != epoch
        or context.get("subscription_generation") != generation
        or str(context.get("handoff_status") or "").lower() != "active"
    ):
        issues.append("PRIMARY_ORB_RUNTIME_IDENTITY_MISMATCH")
    if not required.issubset(_symbol_set(orb.get("configured_symbols"))):
        issues.append("PRIMARY_ORB_CONFIGURED_FAMILIES_MISSING")
    if not required.issubset(_symbol_set(orb.get("requested_symbols"))):
        issues.append("PRIMARY_ORB_REQUESTED_FAMILIES_MISSING")

    symbols = _mapping(orb.get("symbols"))
    for symbol in PRIMARY_CAPTURE_FAMILIES:
        state = _mapping(symbols.get(symbol))
        if not state:
            issues.append(f"PRIMARY_ORB_SYMBOL_MISSING:{symbol}")
            continue
        if state.get("trading_date") != trading_day.isoformat() or state.get(
            "configured"
        ) is not True:
            issues.append(f"PRIMARY_ORB_SESSION_INVALID:{symbol}")
        provenance = _mapping(state.get("provenance"))
        if (
            provenance.get("runtime_binding_applied") is not True
            or provenance.get("range_provenance_aligned") is not True
            or provenance.get("current_vs_range_aligned") is not True
            or provenance.get("active_runtime_epoch_aligned") is not True
            or provenance.get("active_subscription_epoch_id") != epoch
            or provenance.get("active_subscription_generation") != generation
        ):
            issues.append(f"PRIMARY_ORB_PROVENANCE_INVALID:{symbol}")
        if _mapping(state.get("last_known_reference")).get("runtime_aligned") is not True:
            issues.append(f"PRIMARY_ORB_CURRENT_REFERENCE_UNALIGNED:{symbol}")
        if symbol in {"SPX", "NDX"}:
            pin = _mapping(state.get("pin_behavior"))
            if (
                state.get("directional_evidence_eligible") is not True
                or state.get("combined_structure_directional_evidence_eligible")
                is not True
                or pin.get("level_availability_status") != "available"
                or (_finite(pin.get("gamma_pin")) or 0.0) <= 0.0
                or (_finite(pin.get("max_pain")) or 0.0) <= 0.0
            ):
                issues.append(f"PRIMARY_DIRECTIONAL_PIN_STATE_NOT_READY:{symbol}")

        windows = _mapping(state.get("opening_ranges"))
        for window_name, expected_samples in PRIMARY_CAPTURE_ORB_WINDOWS.items():
            window = _mapping(windows.get(window_name))
            capture = _mapping(window.get("capture_evidence"))
            sample_count = capture.get("sample_count")
            recorded_expected = capture.get("expected_sample_count")
            ratio = _finite(capture.get("capture_ratio"))
            first_lag = _finite(capture.get("first_sample_lag_seconds"))
            end_gap = _finite(capture.get("end_gap_seconds"))
            max_gap = _finite(capture.get("max_gap_seconds"))
            range_start = _aware_datetime(window.get("range_start_utc"))
            range_end = _aware_datetime(window.get("range_end_utc"))
            expected_start = datetime.combine(
                trading_day, time(9, 30), tzinfo=ET
            ).astimezone(UTC)
            # Avoid importing a second calendar implementation: the expected
            # duration is already encoded by the contract's exact sample count.
            expected_end = expected_start + timedelta(minutes=int(window_name[:-1]))
            coherent_ratio = (
                sample_count / expected_samples
                if type(sample_count) is int and expected_samples > 0
                else None
            )
            opening_price = _finite(window.get("opening_price"))
            orb_high = _finite(window.get("orb_high"))
            orb_low = _finite(window.get("orb_low"))
            current_price = _finite(window.get("current_price"))
            price_evidence_ok = bool(
                opening_price is not None
                and opening_price > 0.0
                and orb_high is not None
                and orb_high > 0.0
                and orb_low is not None
                and orb_low > 0.0
                and current_price is not None
                and current_price > 0.0
                and orb_low <= opening_price <= orb_high
            )
            timing_ok = bool(
                range_start is not None
                and range_end is not None
                and range_start.astimezone(UTC) == expected_start
                and range_end.astimezone(UTC) == expected_end
                and first_lag == 0.0
                and end_gap is not None
                and 0.0 <= end_gap <= 30.0
                and max_gap is not None
                and 0.0 <= max_gap <= 30.0
            )
            capture_ok = bool(
                window.get("capture_status") == "complete"
                and window.get("duration_minutes") == int(window_name[:-1])
                and window.get("orb_complete") is True
                and window.get("clock_status") == "closed"
                and window.get("current_reference_fresh") is True
                and type(sample_count) is int
                and type(recorded_expected) is int
                and recorded_expected == expected_samples
                and 0 < sample_count <= expected_samples
                and ratio is not None
                and ratio >= 0.95
                and ratio <= 1.0
                and coherent_ratio is not None
                and math.isclose(ratio, coherent_ratio, rel_tol=0.0, abs_tol=1e-12)
                and capture.get("opening_bucket_present") is True
                and price_evidence_ok
                and timing_ok
            )
            if not capture_ok:
                issues.append(f"PRIMARY_ORB_{window_name.upper()}_INCOMPLETE:{symbol}")

    issues = list(dict.fromkeys(issues))
    return {
        "allowed": not issues,
        "state": "primary_capture_ready" if not issues else "primary_capture_not_ready",
        "reason": None if not issues else "; ".join(issues),
        "issues": issues,
        "trading_date": trading_day.isoformat(),
        "required_families": list(PRIMARY_CAPTURE_FAMILIES),
        "required_orb_windows": list(PRIMARY_CAPTURE_ORB_WINDOWS),
        "subscription_epoch_id": epoch if _sha256_identity(epoch) else None,
        "subscription_generation": generation if type(generation) is int else None,
    }


def _validated_loopback_base_url(value: str) -> str:
    parsed = urlsplit(str(value).strip())
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("backend URL must be a plain loopback HTTP origin")
    return str(value).rstrip("/")


class _NoReadinessRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _fetch_json(url: str, timeout_seconds: float) -> dict[str, object]:
    request = Request(url, headers={"Accept": "application/json"})
    # A local readiness read must stay local, regardless of shell proxy state.
    opener = build_opener(ProxyHandler({}), _NoReadinessRedirects())
    with opener.open(request, timeout=timeout_seconds) as response:
        status = int(getattr(response, "status", 200))
        if status != 200:
            raise RuntimeError(f"HTTP {status}")
        body = response.read(PRIMARY_READINESS_MAX_RESPONSE_BYTES + 1)
    if len(body) > PRIMARY_READINESS_MAX_RESPONSE_BYTES:
        raise ValueError("endpoint payload exceeds bounded response size")
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("endpoint payload must be a JSON object")
    return payload


def _readiness_failure(exc: Exception) -> tuple[str, bool, float]:
    """Keep diagnostics useful without echoing URLs, bodies, or exception text."""
    if isinstance(exc, HTTPError):
        failure = f"HTTP_{exc.code}"
        if exc.code == 503:
            try:
                body = json.loads(exc.read(4096))
                reason = _mapping(_mapping(body).get("detail")).get("reason")
                if reason in PRIMARY_READINESS_TRANSIENT_REASONS:
                    failure += f":{reason}"
            except (OSError, ValueError, TypeError):
                pass
        exc.close()
        return failure, exc.code == 503, 1.0
    timeout = isinstance(exc, TimeoutError) or (
        isinstance(exc, URLError) and isinstance(exc.reason, TimeoutError)
    )
    return "TimeoutError" if timeout else type(exc).__name__, timeout, 0.05


def _probe_endpoint(url: str, timeout: float) -> tuple[dict[str, object] | None, dict[str, object]]:
    started = clock.monotonic()
    deadline = started + PRIMARY_READINESS_MAX_TIMEOUT_SECONDS
    attempts: list[dict[str, object]] = []
    payload = None
    for attempt in range(2):
        remaining = deadline - clock.monotonic()
        if remaining <= 0:
            break
        try:
            payload = _fetch_json(url, min(timeout, remaining))
            attempts.append({"status": "available"})
            break
        except Exception as exc:
            failure, retryable, retry_delay = _readiness_failure(exc)
            attempts.append({"status": "unavailable", "failure": failure})
            # Only transient reads retry, at most once, inside the original
            # three-second cap. Do not add parallel work to the two-worker
            # runtime-read guard, and never retry a failed evidence gate.
            if attempt or not retryable or deadline - clock.monotonic() <= retry_delay:
                break
            clock.sleep(retry_delay)
    return payload, {
        "endpoint": urlsplit(url).path,
        "status": "available" if payload is not None else "unavailable",
        "elapsed_seconds": round(clock.monotonic() - started, 3),
        "attempts": attempts,
    }


def probe_primary_capture_readiness(
    *,
    backend_url: str,
    timeout_seconds: float,
    trading_day: date,
) -> dict[str, object]:
    """Read sequential local projections with at most one bounded transient retry."""

    try:
        origin = _validated_loopback_base_url(backend_url)
        requested_timeout = float(timeout_seconds)
        if not math.isfinite(requested_timeout):
            raise ValueError("readiness timeout must be finite")
        timeout = max(
            0.2,
            min(requested_timeout, PRIMARY_READINESS_MAX_TIMEOUT_SECONDS),
        )
    except Exception as exc:
        issue = f"PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:{type(exc).__name__}"
        return {
            "allowed": False,
            "state": "primary_capture_not_ready",
            "reason": issue,
            "issues": [issue],
            "trading_date": trading_day.isoformat(),
            "required_families": list(PRIMARY_CAPTURE_FAMILIES),
            "required_orb_windows": list(PRIMARY_CAPTURE_ORB_WINDOWS),
        }
    payloads = []
    endpoint_checks = []
    for path in ("/health", "/health/live", "/v1/orb?symbols=SPX%2CNDX%2CVIX%2CRUT"):
        payload, check = _probe_endpoint(origin + path, timeout)
        endpoint_checks.append(check)
        if payload is None:
            last_attempt = check["attempts"][-1] if check["attempts"] else {}
            failure = last_attempt.get("failure", "TimeoutError")
            issue = f"PRIMARY_READINESS_ENDPOINT_UNAVAILABLE:{check['endpoint']}:{failure}"
            return {
                "allowed": False,
                "state": "primary_capture_not_ready",
                "reason": issue,
                "issues": [issue],
                "trading_date": trading_day.isoformat(),
                "required_families": list(PRIMARY_CAPTURE_FAMILIES),
                "required_orb_windows": list(PRIMARY_CAPTURE_ORB_WINDOWS),
                "endpoint_checks": endpoint_checks,
            }
        payloads.append(payload)
    decision = primary_capture_start_decision(
        health_payload=payloads[0],
        live_payload=payloads[1],
        orb_payload=payloads[2],
        trading_day=trading_day,
    )
    return {**decision, "endpoint_checks": endpoint_checks}


def recorder_start_decision(
    project_root: str | Path,
    *,
    trading_day: date,
    now: datetime | None = None,
) -> dict[str, object]:
    """Return a fail-closed decision for starting a complete daily recorder.

    A launch before the opening-range protection boundary is deferred because
    the recorder can replay from the cash open. A launch at or after the
    close-minus-15 analysis horizon cannot satisfy the session contract and
    would create a misleading partial session. The launcher checks for an
    active recorder before reaching this gate, so a non-empty same-day DBN here
    is orphaned evidence: automatically replaying the full session again could
    starve the primary gamma/ORB stream. Existing processes remain protected by
    the process lock and supervisor.
    """
    observed = now or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    try:
        config = build_session_config(
            project_root,
            trading_day=trading_day,
            now=observed,
            session_id="start-gate",
        )
    except ValueError as exc:
        return {
            "allowed": False,
            "reason": str(exc),
            "trading_date": trading_day.isoformat(),
            "observed_at_utc": observed.astimezone(UTC).isoformat(),
        }
    observed_utc = observed.astimezone(UTC)
    start_not_before_utc = datetime.combine(
        trading_day,
        AUTOMATIC_START_NOT_BEFORE_ET,
        tzinfo=ET,
    ).astimezone(UTC)
    prior_dbn_evidence = tuple(
        path
        for path in sorted(config.output_dir.glob("*.dbn"))
        if path.is_file() and path.stat().st_size > 0
    )
    # Dated operator hold: Sep 8 primary acceptance failed. Do not introduce
    # a competing broad replay; keep the normal post-cutoff finalizer path.
    if trading_day == date(2026, 9, 8) and observed_utc < config.analysis_due_utc:
        state = "optional_research_held"
        reason = "Sep 8 primary acceptance failed; optional broad OPRA replay held for this session"
        start_not_before_utc = config.stop_due_utc
    elif observed_utc < start_not_before_utc:
        state = "not_yet_due"
        reason = "opening gamma and 60-minute ORB protection window is active"
    elif observed_utc >= config.analysis_due_utc:
        state = "too_late"
        reason = "close-minus-15 analysis cutoff has passed"
    elif prior_dbn_evidence:
        state = "recovery_replay_blocked"
        reason = (
            "non-empty same-day DBN evidence already exists; automatic cash-open "
            "replay recovery is blocked to protect the primary gamma and ORB stream"
        )
    else:
        state = "allowed"
        reason = None
    return {
        "allowed": state == "allowed",
        "state": state,
        "reason": reason,
        "trading_date": trading_day.isoformat(),
        "observed_at_utc": observed_utc.isoformat(),
        "start_not_before_utc": start_not_before_utc.isoformat(),
        "analysis_due_utc": config.analysis_due_utc.isoformat(),
        "stop_due_utc": config.stop_due_utc.isoformat(),
        "prior_nonempty_dbn_count": len(prior_dbn_evidence),
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Check whether a complete closing-tape session can start")
    result.add_argument("--project-root", required=True)
    result.add_argument("--trading-date", required=True)
    result.add_argument(
        "--require-primary-ready",
        action="store_true",
        help=(
            "Require the existing loopback backend and exact all-four 5m/60m "
            "ORB evidence before authorizing the optional recorder."
        ),
    )
    result.add_argument(
        "--backend-url",
        default="http://127.0.0.1:8000",
        help="Loopback backend origin used only with --require-primary-ready.",
    )
    result.add_argument(
        "--timeout-seconds",
        type=float,
        default=PRIMARY_READINESS_TIMEOUT_SECONDS,
        help="Per-attempt timeout, bounded to 0.2-3.0 seconds; one transient retry shares a 3-second endpoint budget.",
    )
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    trading_day = date.fromisoformat(args.trading_date)
    decision = recorder_start_decision(
        args.project_root,
        trading_day=trading_day,
    )
    if decision["allowed"] and args.require_primary_ready:
        primary = probe_primary_capture_readiness(
            backend_url=args.backend_url,
            timeout_seconds=args.timeout_seconds,
            trading_day=trading_day,
        )
        decision = {**decision, "primary_capture": primary}
        if not primary["allowed"]:
            decision.update(
                allowed=False,
                state="primary_capture_not_ready",
                reason=primary["reason"],
            )
        else:
            # Endpoint reads are bounded but can straddle the close-minus-15
            # boundary. Re-evaluate the pure owner/time gate after I/O so a
            # pre-cutoff decision cannot authorize a post-cutoff process.
            post_probe = recorder_start_decision(
                args.project_root,
                trading_day=trading_day,
            )
            if not post_probe["allowed"]:
                decision = {**post_probe, "primary_capture": primary}
    print(json.dumps(decision, sort_keys=True, separators=(",", ":")))
    if decision["allowed"]:
        return 0
    return (
        4
        if decision.get("state")
        in {
            "not_yet_due",
            "optional_research_held",
            "primary_capture_not_ready",
        }
        else 3
    )


if __name__ == "__main__":
    raise SystemExit(main())
