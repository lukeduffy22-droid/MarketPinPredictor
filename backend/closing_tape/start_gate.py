from __future__ import annotations

import argparse
import json
import math
import time as clock
from datetime import date, datetime, time
from pathlib import Path
from typing import Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from .config import ET, UTC, build_session_config


# The primary live backend owns the opening hour. The closing-tape stream can
# request a same-day replay from the cash open, so starting after the opening
# protection window preserves the full tape without making a second broad OPRA
# client compete with opening capture. Forecast and ORB validity are deliberately
# not recorder prerequisites: invalid predictions must abstain, but they must not
# prevent immutable source evidence from being retained.
AUTOMATIC_START_NOT_BEFORE_ET = time(10, 35)
PRIMARY_CAPTURE_FAMILIES = ("SPX", "NDX", "VIX", "RUT")
PRIMARY_CAPTURE_SAFE_STAGING_STATES = {
    "primary_active",
    "full_active",
    "full_active_integrity_warning",
    "frozen",
}
MAX_AUTOMATIC_CAPTURE_ATTEMPTS = 2
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


def primary_capture_start_decision(
    *,
    health_payload: object,
    live_payload: object,
    orb_payload: object | None = None,
    trading_day: date,
) -> dict[str, object]:
    """Validate transport safety before an independent evidence recorder starts.

    This function is pure: callers supply retained HTTP payloads and it neither
    opens a provider connection nor mutates runtime or database state. Forecast,
    calculation, quote-pair, and ORB readiness remain downstream analysis gates.
    """

    health = _mapping(health_payload)
    live = _mapping(live_payload)
    issues: list[str] = []
    diagnostics: list[str] = []

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
    # Skips are evidence loss and remain blocking. Cumulative recovered warning,
    # reconnect, and rejection counters are retained as diagnostics; requiring
    # them to remain zero forever made one recovered event suppress the day's
    # closing tape even after transport had returned to a safe state.
    for field in (
        "provider_skipped_record_warnings",
        "provider_skipped_records",
        "connection_limit_consecutive",
        "pre_auth_transport_aborts_total",
        "pre_auth_transport_abort_failures_total",
    ):
        if type(health.get(field)) is not int or health.get(field) != 0:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_NONZERO")
    for field in (
        "provider_queue_full_warnings",
        "provider_slow_client_warnings",
        "reconnect_attempts",
        "connection_limit_rejections_total",
    ):
        value = health.get(field)
        if type(value) is not int or value < 0:
            issues.append(f"PRIMARY_HEALTH_{field.upper()}_INVALID")
        elif value:
            diagnostics.append(f"PRIMARY_HEALTH_{field.upper()}_HISTORICAL_NONZERO")
    if str(health.get("connection_limit_circuit_state") or "").lower() != "closed":
        issues.append("PRIMARY_CONNECTION_LIMIT_CIRCUIT_NOT_CLOSED")
    if health.get("connection_limit_retry_not_before_utc") is not None:
        issues.append("PRIMARY_CONNECTION_LIMIT_RETRY_PENDING")
    cooldown = _finite(health.get("connection_limit_cooldown_remaining_seconds"))
    if cooldown is None or cooldown != 0.0:
        issues.append("PRIMARY_CONNECTION_LIMIT_COOLDOWN_ACTIVE_OR_INVALID")
    if health.get("pre_auth_transport_guard_status") != "installed":
        issues.append("PRIMARY_PRE_AUTH_TRANSPORT_GUARD_NOT_INSTALLED")
    backpressure = _finite(health.get("compute_backpressure_remaining_seconds"))
    if backpressure is None or backpressure < 0.0:
        issues.append("PRIMARY_COMPUTE_BACKPRESSURE_INVALID")
    elif backpressure > 0.0:
        issues.append("PRIMARY_COMPUTE_BACKPRESSURE_ACTIVE")

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
    staging_state = str(staging.get("state") or "").lower()
    if staging_state not in PRIMARY_CAPTURE_SAFE_STAGING_STATES:
        issues.append("PRIMARY_STAGED_SUBSCRIPTION_STATE_UNSAFE")
    elif staging_state != "full_active":
        diagnostics.append(f"PRIMARY_STAGED_SUBSCRIPTION_DEGRADED:{staging_state}")
    primary_counts = _mapping(staging.get("primary_contract_counts"))
    for symbol in PRIMARY_CAPTURE_FAMILIES:
        if type(primary_counts.get(symbol)) is not int or int(
            primary_counts.get(symbol) or 0
        ) <= 0:
            issues.append(f"PRIMARY_STAGED_FAMILY_MISSING:{symbol}")

    expected_live_true = (
        "stream_connected",
        "stream_progressing",
        "collection_ready",
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

    for field in (
        "all_configured_collection_ready",
        "calculation_ready",
        "prediction_pipeline_ok",
    ):
        if live.get(field) is not True:
            diagnostics.append(f"PRIMARY_LIVE_{field.upper()}_NOT_READY")

    issues = list(dict.fromkeys(issues))
    diagnostics = list(dict.fromkeys(diagnostics))
    return {
        "allowed": not issues,
        "state": "primary_capture_ready" if not issues else "primary_capture_not_ready",
        "reason": None if not issues else "; ".join(issues),
        "issues": issues,
        "diagnostics": diagnostics,
        "readiness_scope": "transport_and_primary_subscription_only",
        "trading_date": trading_day.isoformat(),
        "required_families": list(PRIMARY_CAPTURE_FAMILIES),
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
            "readiness_scope": "transport_and_primary_subscription_only",
        }
    payloads = []
    endpoint_checks = []
    for path in ("/health", "/health/live"):
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
                "readiness_scope": "transport_and_primary_subscription_only",
                "endpoint_checks": endpoint_checks,
            }
        payloads.append(payload)
    decision = primary_capture_start_decision(
        health_payload=payloads[0],
        live_payload=payloads[1],
        orb_payload=None,
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
    the recorder can replay from the cash open. The close-minus-15 boundary
    controls analysis eligibility, not immutable capture. A bounded second
    attempt is permitted when an earlier non-empty DBN exists; process locks,
    transport readiness, and the hard stop boundary still protect the primary.
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
    elif observed_utc >= config.stop_due_utc:
        state = "too_late"
        reason = "closing-tape stop boundary has passed"
    elif len(prior_dbn_evidence) >= MAX_AUTOMATIC_CAPTURE_ATTEMPTS:
        state = "recovery_attempt_limit_reached"
        reason = (
            "automatic closing-tape recovery attempt limit reached; preserve "
            "existing DBN evidence and require operator review"
        )
    elif prior_dbn_evidence:
        state = "allowed_recovery"
        reason = "one bounded recovery replay is allowed before the hard stop boundary"
    else:
        state = "allowed"
        reason = None
    analysis_eligible = bool(observed_utc < config.analysis_due_utc)
    capture_mode = "capture_and_analyze" if analysis_eligible else "capture_only"
    return {
        "allowed": state in {"allowed", "allowed_recovery"},
        "state": state,
        "reason": reason,
        "trading_date": trading_day.isoformat(),
        "observed_at_utc": observed_utc.isoformat(),
        "start_not_before_utc": start_not_before_utc.isoformat(),
        "analysis_due_utc": config.analysis_due_utc.isoformat(),
        "stop_due_utc": config.stop_due_utc.isoformat(),
        "prior_nonempty_dbn_count": len(prior_dbn_evidence),
        "maximum_automatic_capture_attempts": MAX_AUTOMATIC_CAPTURE_ATTEMPTS,
        "analysis_eligible": analysis_eligible,
        "capture_mode": capture_mode,
    }


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Check whether a complete closing-tape session can start")
    result.add_argument("--project-root", required=True)
    result.add_argument("--trading-date", required=True)
    result.add_argument(
        "--require-primary-ready",
        action="store_true",
        help=(
            "Require safe transport and active primary family subscriptions "
            "before authorizing the optional recorder."
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
            # Endpoint reads are bounded but can straddle the hard stop or move
            # the launch from analysis-eligible to capture-only. Re-evaluate
            # the owner/time gate after I/O before authorizing the process.
            post_probe = recorder_start_decision(
                args.project_root,
                trading_day=trading_day,
            )
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
