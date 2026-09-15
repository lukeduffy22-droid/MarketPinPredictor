"""Truthful, bounded reads for the Streamlit sidebar market-state controls.

The sidebar must not infer readiness from the presence of numeric fields.  This
module consumes the lifecycle-owned dashboard contract and one compact health
sample, then releases numeric values only after validity, freshness, and the
active process epoch plus subscription generation have all been verified.
"""

import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence

import requests

# Keep annotations eager in this module. Streamlit's source watcher can evict a
# watched module from ``sys.modules`` while a rerun is importing it. Python 3.11
# dataclasses resolve postponed (string) annotations through that registry, so
# ``from __future__ import annotations`` makes the decorator crash during that
# narrow hot-reload window. Every annotation below is safe to evaluate eagerly.


WORKSTATION_CONTRACT_VERSION = "workstation-state.v1"
OPTIONAL_SYMBOLS = frozenset({"VIX"})
SUBSCRIPTION_EPOCH_HEX_LENGTH = 64


@dataclass(frozen=True)
class DiagnosticSymbolEvidence:
    """Curated numbers that may be inspected but never treated as usable state."""

    scope: str
    spot: float | None = None
    gamma_pin: float | None = None
    max_pain: float | None = None
    zero_gamma: float | None = None
    gross_gex: float | None = None
    net_gex: float | None = None
    contracts_count: int | None = None
    fresh_quote_count: int | None = None
    paired_quote_count: int | None = None
    expected_primary_pair_count: int | None = None
    paired_primary_pair_count: int | None = None
    primary_pair_coverage_ratio: float | None = None
    primary_expiration: str | None = None
    max_pain_source: str | None = None
    max_pain_as_of: str | None = None
    calculation_id: str | None = None


@dataclass(frozen=True)
class SidebarSymbolState:
    """One fail-closed symbol projection suitable for sidebar rendering."""

    symbol: str
    state: str
    lifecycle_status: str | None
    usable: bool
    reason: str
    failure_reasons: tuple[str, ...]
    checked_at_utc: str
    source_as_of_utc: str | None = None
    generated_at_utc: str | None = None
    provider: str | None = None
    data_age_seconds: float | None = None
    stale_after_seconds: float | None = None
    subscription_epoch_id: str | None = None
    active_subscription_epoch_id: str | None = None
    epoch_is_current: bool | None = None
    active_handoff_status: str | None = None
    subscription_generation: int | None = None
    active_generation: int | None = None
    state_revision: int | None = None
    event_id: str | None = None
    forecast_id: str | None = None
    prediction_snapshot_id: int | None = None
    current_price: float | None = None
    prediction_usable: bool = False
    predicted_close: float | None = None
    gamma_pin: float | None = None
    max_pain: float | None = None
    positive_gex_wall: float | None = None
    negative_gex_wall: float | None = None
    market_data_source_label: str | None = None
    prediction_source_label: str | None = None
    prediction_is_estimate: bool = False
    tcbbo_promoted: bool = False
    optional: bool = False
    diagnostic_evidence: DiagnosticSymbolEvidence | None = None


@dataclass(frozen=True)
class SidebarStateBatch:
    """One concurrent sidebar read and its single generation-health sample."""

    results: tuple[SidebarSymbolState, ...]
    checked_at_utc: str
    health_state: str
    health_reason: str | None
    dashboard_payloads: Mapping[str, Mapping[str, Any]] = field(default_factory=dict)

    @property
    def usable_results(self) -> tuple[SidebarSymbolState, ...]:
        return tuple(result for result in self.results if result.usable)

    @property
    def failed_results(self) -> tuple[SidebarSymbolState, ...]:
        return tuple(result for result in self.results if not result.usable)


@dataclass(frozen=True)
class CacheReloadSummary:
    level: str
    message: str


@dataclass(frozen=True)
class _HttpResult:
    state: str
    payload: Mapping[str, Any] | None = None
    reason: str | None = None
    status_code: int | None = None


HttpGetter = Callable[..., Any]


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _nonnegative_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None


def _canonical_subscription_epoch(value: Any) -> str | None:
    """Accept only the backend's exact lowercase 64-hex process identity."""

    if not isinstance(value, str) or len(value) != SUBSCRIPTION_EPOCH_HEX_LENGTH:
        return None
    if any(character not in "0123456789abcdef" for character in value):
        return None
    return value


def _failure_reasons(payload: Mapping[str, Any]) -> tuple[str, ...]:
    health = payload.get("health")
    health = health if isinstance(health, Mapping) else {}
    values = health.get("validation_failure_reasons") or []
    if isinstance(values, str):
        values = [values]
    reasons = [str(value).strip() for value in values if str(value).strip()]
    return tuple(dict.fromkeys(reasons))


def _positive_finite(value: Any) -> float | None:
    number = _finite(value)
    return number if number is not None and number > 0.0 else None


def _diagnostic_symbol_evidence(
    payload: Mapping[str, Any],
) -> DiagnosticSymbolEvidence | None:
    """Expose a narrow diagnostic projection without releasing a prediction.

    Invalid rows often retain useful evidence such as an OI-only max-pain level
    or a fully calculated but rejected GEX surface.  Zero placeholders are
    deliberately converted to ``None`` so the UI cannot make a failed
    calculation look like a real market value.
    """

    pin_payload = payload.get("pin_payload")
    if not isinstance(pin_payload, Mapping) or not pin_payload:
        return None

    contracts_count = _nonnegative_int(
        pin_payload.get("contracts")
        if pin_payload.get("contracts") is not None
        else pin_payload.get("contracts_count")
    )
    spot = _positive_finite(
        pin_payload.get("price")
        if pin_payload.get("price") is not None
        else pin_payload.get("spot_last")
    )
    gamma_pin = _positive_finite(
        pin_payload.get("gamma_pin")
        if pin_payload.get("gamma_pin") is not None
        else pin_payload.get("primary_gamma_pin_strike")
    )
    max_pain = _positive_finite(
        pin_payload.get("max_pain")
        if pin_payload.get("max_pain") is not None
        else pin_payload.get("max_pain_strike")
    )
    zero_gamma = _positive_finite(
        pin_payload.get("zero_gamma")
        if pin_payload.get("zero_gamma") is not None
        else pin_payload.get("zero_gamma_level")
    )

    # A hard-stop audit writes zero GEX placeholders with zero contracts. Only
    # release exposure values when at least one calculated contract survived.
    has_calculated_chain = bool(contracts_count and contracts_count > 0)
    gross_gex = _finite(pin_payload.get("gross_gex")) if has_calculated_chain else None
    net_gex = _finite(pin_payload.get("net_gex")) if has_calculated_chain else None
    primary_expiration = str(pin_payload.get("primary_expiration") or "").strip() or None
    max_pain_source = str(pin_payload.get("max_pain_source") or "").strip() or None
    max_pain_as_of = str(pin_payload.get("max_pain_as_of") or "").strip() or None
    calculation_id = str(pin_payload.get("calculation_id") or "").strip() or None
    lifecycle_status = str(payload.get("status") or "").strip().lower()
    historical_context_only = bool(
        lifecycle_status in {"fallback", "closed_context"}
        or pin_payload.get("historical_context_only") is True
        or pin_payload.get("is_fallback") is True
        or str(pin_payload.get("provider") or "").lower() == "historical-fallback"
    )

    if historical_context_only:
        scope = "historical_context_only"
    elif spot is not None or gamma_pin is not None or has_calculated_chain:
        scope = "partial_live_calculation"
    elif max_pain is not None:
        scope = "open_interest_context_only"
    else:
        scope = "status_only"

    evidence = DiagnosticSymbolEvidence(
        scope=scope,
        spot=spot,
        gamma_pin=gamma_pin,
        max_pain=max_pain,
        zero_gamma=zero_gamma,
        gross_gex=gross_gex,
        net_gex=net_gex,
        contracts_count=contracts_count,
        fresh_quote_count=_nonnegative_int(pin_payload.get("fresh_quote_count")),
        paired_quote_count=_nonnegative_int(pin_payload.get("paired_quote_count")),
        expected_primary_pair_count=_nonnegative_int(
            pin_payload.get("expected_primary_pair_count")
        ),
        paired_primary_pair_count=_nonnegative_int(
            pin_payload.get("paired_primary_pair_count")
        ),
        primary_pair_coverage_ratio=_finite(
            pin_payload.get("primary_pair_coverage_ratio")
        ),
        primary_expiration=primary_expiration,
        max_pain_source=max_pain_source,
        max_pain_as_of=max_pain_as_of,
        calculation_id=calculation_id,
    )
    has_evidence = any(
        value is not None
        for value in (
            evidence.spot,
            evidence.gamma_pin,
            evidence.max_pain,
            evidence.zero_gamma,
            evidence.gross_gex,
            evidence.net_gex,
            evidence.contracts_count,
            evidence.fresh_quote_count,
            evidence.paired_quote_count,
            evidence.expected_primary_pair_count,
            evidence.paired_primary_pair_count,
            evidence.primary_pair_coverage_ratio,
            evidence.primary_expiration,
            evidence.max_pain_as_of,
            evidence.calculation_id,
        )
    )
    return evidence if has_evidence else None


def _http_detail(response: Any) -> str | None:
    try:
        payload = response.json()
    except (TypeError, ValueError):
        payload = None
    if isinstance(payload, Mapping):
        detail = payload.get("detail")
        if detail is not None:
            return str(detail)
    text = str(getattr(response, "text", "") or "").strip()
    return text or None


def _fetch_json(url: str, *, timeout_seconds: float, get: HttpGetter) -> _HttpResult:
    try:
        response = get(url, timeout=timeout_seconds)
    except requests.exceptions.Timeout as exc:
        detail = str(exc).strip()
        return _HttpResult(
            state="timeout",
            reason="Request timed out" + (f": {detail}" if detail else ""),
        )
    except requests.exceptions.ConnectionError as exc:
        detail = str(exc).strip()
        return _HttpResult(
            state="connection_error",
            reason="Connection failed" + (f": {detail}" if detail else ""),
        )
    except requests.exceptions.RequestException as exc:
        detail = str(exc).strip()
        return _HttpResult(
            state="request_error",
            reason="Request failed" + (f": {detail}" if detail else ""),
        )

    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code != 200:
        detail = _http_detail(response)
        return _HttpResult(
            state="http_error",
            status_code=status_code,
            reason=f"HTTP {status_code}" + (f": {detail}" if detail else ""),
        )
    try:
        payload = response.json()
    except (TypeError, ValueError) as exc:
        return _HttpResult(state="malformed", reason=f"Invalid JSON: {exc}")
    if not isinstance(payload, Mapping):
        return _HttpResult(
            state="malformed",
            reason="Response JSON is not an object",
        )
    return _HttpResult(state="ok", payload=payload, status_code=status_code)


def _health_subscription_context(
    health_payload: Mapping[str, Any] | None,
    symbol: str,
) -> tuple[int | None, bool | None, str | None, bool | None, str | None]:
    if not isinstance(health_payload, Mapping):
        return None, None, None, None, None
    symbol_status = health_payload.get("symbol_status")
    symbol_status = symbol_status if isinstance(symbol_status, Mapping) else {}
    detail = symbol_status.get(symbol)
    detail = detail if isinstance(detail, Mapping) else {}
    active_generation = _positive_int(detail.get("active_generation"))
    generation_is_current = detail.get("generation_is_current")
    if not isinstance(generation_is_current, bool):
        generation_is_current = None
    active_epoch_id = _canonical_subscription_epoch(
        detail.get("active_subscription_epoch_id")
    )
    if active_epoch_id is None:
        active_epoch_id = _canonical_subscription_epoch(
            health_payload.get("subscription_epoch_id")
        )
    epoch_is_current = detail.get("epoch_is_current")
    if not isinstance(epoch_is_current, bool):
        epoch_is_current = None
    handoff_status = health_payload.get("handoff_status")
    if not isinstance(handoff_status, str) or not handoff_status:
        handoff_status = None

    if active_generation is None:
        for candidate in symbol_status.values():
            if isinstance(candidate, Mapping):
                active_generation = _positive_int(candidate.get("active_generation"))
                if active_generation is not None:
                    break
    return (
        active_generation,
        generation_is_current,
        active_epoch_id,
        epoch_is_current,
        handoff_status,
    )


def _unusable_state(
    *,
    symbol: str,
    state: str,
    reason: str,
    checked_at_utc: str,
    optional: bool,
    lifecycle_status: str | None = None,
    failure_reasons: tuple[str, ...] = (),
    source_as_of_utc: str | None = None,
    generated_at_utc: str | None = None,
    provider: str | None = None,
    data_age_seconds: float | None = None,
    stale_after_seconds: float | None = None,
    subscription_epoch_id: str | None = None,
    active_subscription_epoch_id: str | None = None,
    epoch_is_current: bool | None = None,
    active_handoff_status: str | None = None,
    subscription_generation: int | None = None,
    active_generation: int | None = None,
    state_revision: int | None = None,
    event_id: str | None = None,
    forecast_id: str | None = None,
    prediction_snapshot_id: int | None = None,
    diagnostic_evidence: DiagnosticSymbolEvidence | None = None,
) -> SidebarSymbolState:
    return SidebarSymbolState(
        symbol=symbol,
        state=state,
        lifecycle_status=lifecycle_status,
        usable=False,
        reason=reason,
        failure_reasons=failure_reasons,
        checked_at_utc=checked_at_utc,
        source_as_of_utc=source_as_of_utc,
        generated_at_utc=generated_at_utc,
        provider=provider,
        data_age_seconds=data_age_seconds,
        stale_after_seconds=stale_after_seconds,
        subscription_epoch_id=subscription_epoch_id,
        active_subscription_epoch_id=active_subscription_epoch_id,
        epoch_is_current=epoch_is_current,
        active_handoff_status=active_handoff_status,
        subscription_generation=subscription_generation,
        active_generation=active_generation,
        state_revision=state_revision,
        event_id=event_id,
        forecast_id=forecast_id,
        prediction_snapshot_id=prediction_snapshot_id,
        optional=optional,
        diagnostic_evidence=diagnostic_evidence,
    )


def classify_sidebar_symbol_state(
    symbol: str,
    response: _HttpResult,
    *,
    health_response: _HttpResult,
    checked_at: datetime,
) -> SidebarSymbolState:
    """Validate one dashboard contract and redact numbers unless it is current."""

    symbol = str(symbol).upper().strip()
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    else:
        checked_at = checked_at.astimezone(timezone.utc)
    optional = symbol in OPTIONAL_SYMBOLS
    checked_at_utc = _utc_iso(checked_at)
    if response.state != "ok" or response.payload is None:
        return _unusable_state(
            symbol=symbol,
            state=response.state,
            reason=response.reason or "Dashboard state request failed",
            checked_at_utc=checked_at_utc,
            optional=optional,
        )

    payload = response.payload
    lifecycle_status = str(payload.get("status") or "").strip().lower() or None
    failure_reasons = _failure_reasons(payload)
    source_as_of_utc = (
        str(payload.get("source_as_of_utc"))
        if payload.get("source_as_of_utc") is not None
        else None
    )
    generated_at_utc = (
        str(payload.get("generated_at_utc"))
        if payload.get("generated_at_utc") is not None
        else None
    )
    provider = str(payload.get("provider") or "").strip() or None
    subscription_epoch_id = _canonical_subscription_epoch(
        payload.get("subscription_epoch_id")
    )
    subscription_generation = _positive_int(payload.get("subscription_generation"))
    state_revision = _nonnegative_int(payload.get("state_revision"))
    event_id = str(payload.get("event_id") or "").strip() or None
    forecast_id = str(payload.get("forecast_id") or "").strip() or None
    prediction_snapshot_id = _positive_int(payload.get("prediction_snapshot_id"))
    health = payload.get("health")
    health = health if isinstance(health, Mapping) else {}
    stale_after_seconds = _finite(health.get("stale_after_seconds"))
    reported_age_seconds = _finite(health.get("data_age_seconds"))
    source_datetime = _parse_utc(source_as_of_utc)
    dynamic_age_seconds = (
        max(0.0, (checked_at.astimezone(timezone.utc) - source_datetime).total_seconds())
        if source_datetime is not None
        else None
    )
    ages = [
        age
        for age in (reported_age_seconds, dynamic_age_seconds)
        if age is not None
    ]
    data_age_seconds = max(ages) if ages else None
    (
        active_generation,
        health_generation_is_current,
        active_subscription_epoch_id,
        health_epoch_is_current,
        health_handoff_status,
    ) = _health_subscription_context(health_response.payload, symbol)

    common = {
        "symbol": symbol,
        "checked_at_utc": checked_at_utc,
        "optional": optional,
        "lifecycle_status": lifecycle_status,
        "failure_reasons": failure_reasons,
        "source_as_of_utc": source_as_of_utc,
        "generated_at_utc": generated_at_utc,
        "provider": provider,
        "data_age_seconds": data_age_seconds,
        "stale_after_seconds": stale_after_seconds,
        "subscription_epoch_id": subscription_epoch_id,
        "active_subscription_epoch_id": active_subscription_epoch_id,
        "epoch_is_current": health_epoch_is_current,
        "active_handoff_status": health_handoff_status,
        "subscription_generation": subscription_generation,
        "active_generation": active_generation,
        "state_revision": state_revision,
        "event_id": event_id,
        "forecast_id": forecast_id,
        "prediction_snapshot_id": prediction_snapshot_id,
    }

    if payload.get("contract_version") != WORKSTATION_CONTRACT_VERSION:
        return _unusable_state(
            state="malformed",
            reason=(
                "Unsupported dashboard contract: "
                f"{payload.get('contract_version') or 'missing'}"
            ),
            **common,
        )
    if str(payload.get("symbol") or "").upper() != symbol:
        return _unusable_state(
            state="malformed",
            reason=f"Dashboard returned state for {payload.get('symbol') or 'no symbol'}",
            **common,
        )

    diagnostic_evidence = _diagnostic_symbol_evidence(payload)
    common["diagnostic_evidence"] = diagnostic_evidence

    if lifecycle_status in {"fallback", "closed_context"}:
        reasons = failure_reasons or ("CLOSED_MARKET_HISTORICAL_CONTEXT_ONLY",)
        return _unusable_state(
            state="closed_context",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )

    epoch_reason = any(
        reason.startswith("SUBSCRIPTION_EPOCH_MISMATCH")
        for reason in failure_reasons
    )
    explicit_epoch_mismatch = bool(
        health_epoch_is_current is False
        or (
            subscription_epoch_id is not None
            and active_subscription_epoch_id is not None
            and subscription_epoch_id != active_subscription_epoch_id
        )
    )
    if epoch_reason or explicit_epoch_mismatch:
        reasons = failure_reasons or (
            "SUBSCRIPTION_EPOCH_MISMATCH: "
            f"payload={subscription_epoch_id}, active={active_subscription_epoch_id}",
        )
        return _unusable_state(
            state="epoch_mismatch",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )

    generation_reason = any(
        reason.startswith("SUBSCRIPTION_GENERATION_MISMATCH")
        for reason in failure_reasons
    )
    explicit_generation_mismatch = (
        health_generation_is_current is False
        or (
            active_generation is not None
            and subscription_generation is not None
            and active_generation != subscription_generation
        )
    )
    if generation_reason or explicit_generation_mismatch:
        reasons = failure_reasons or (
            "SUBSCRIPTION_GENERATION_MISMATCH: "
            f"payload={subscription_generation}, active={active_generation}",
        )
        return _unusable_state(
            state="generation_mismatch",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )

    if lifecycle_status in {"invalid"} or health.get("validation_is_valid") is not True:
        reasons = failure_reasons or ("LIFECYCLE_VALIDATION_FAILED",)
        return _unusable_state(
            state="invalid",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if lifecycle_status in {"stale"}:
        reasons = failure_reasons or ("LIVE_DATA_STALE",)
        return _unusable_state(
            state="stale",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if lifecycle_status in {"warming"}:
        reasons = failure_reasons or ("WAITING_FOR_LIFECYCLE_PUBLISH",)
        return _unusable_state(
            state="warming",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if lifecycle_status in {"unavailable", None}:
        reasons = failure_reasons or ("LIFECYCLE_STATE_UNAVAILABLE",)
        return _unusable_state(
            state="unavailable",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if lifecycle_status not in {"ready", "live"}:
        return _unusable_state(
            state="malformed",
            reason=f"Unknown lifecycle status: {lifecycle_status}",
            **common,
        )

    if health.get("is_stale") is not False:
        reasons = failure_reasons or ("LIFECYCLE_FRESHNESS_NOT_VERIFIED",)
        return _unusable_state(
            state="stale",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if source_datetime is None:
        reasons = failure_reasons or ("SOURCE_TIMESTAMP_MISSING_OR_INVALID",)
        return _unusable_state(
            state="invalid",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )
    if stale_after_seconds is None or stale_after_seconds <= 0:
        return _unusable_state(
            state="malformed",
            reason="Freshness threshold is missing or invalid",
            **common,
        )
    if data_age_seconds is None or data_age_seconds > stale_after_seconds:
        reason = (
            "LIVE_DATA_STALE: "
            f"age={data_age_seconds if data_age_seconds is not None else 'unknown'}s, "
            f"limit={stale_after_seconds}s"
        )
        return _unusable_state(state="stale", reason=reason, **common)

    if health_response.state != "ok":
        return _unusable_state(
            state="generation_unverified",
            reason=(
                "ACTIVE_GENERATION_UNAVAILABLE: "
                f"health {health_response.state}"
                + (f" ({health_response.reason})" if health_response.reason else "")
            ),
            **common,
        )
    if health_handoff_status != "active":
        return _unusable_state(
            state="handoff_unready",
            reason=(
                "ACTIVE_HANDOFF_UNAVAILABLE: health handoff_status must be exactly "
                f"active, got {health_handoff_status or 'missing'}"
            ),
            **common,
        )
    if (
        subscription_epoch_id is None
        or active_subscription_epoch_id is None
        or health_epoch_is_current is not True
    ):
        return _unusable_state(
            state="epoch_unverified",
            reason=(
                "ACTIVE_SUBSCRIPTION_EPOCH_UNAVAILABLE: lifecycle and health must "
                "carry the same canonical lowercase 64-hex epoch with "
                "epoch_is_current=true"
            ),
            **common,
        )
    if active_generation is None or subscription_generation is None:
        return _unusable_state(
            state="generation_unverified",
            reason=(
                "ACTIVE_GENERATION_UNAVAILABLE: lifecycle or health generation missing"
            ),
            **common,
        )

    current_price = _finite(payload.get("current_price"))
    if current_price is None or current_price <= 0:
        reasons = failure_reasons or ("SOURCE_PRICE_MISSING_OR_INVALID",)
        return _unusable_state(
            state="invalid",
            reason="; ".join(reasons),
            **{**common, "failure_reasons": reasons},
        )

    pin_payload = payload.get("pin_payload")
    pin_payload = pin_payload if isinstance(pin_payload, Mapping) else {}
    prediction = payload.get("prediction")
    prediction = prediction if isinstance(prediction, Mapping) else {}
    authority = payload.get("prediction_authority")
    authority = authority if isinstance(authority, Mapping) else {}
    market_source = str(
        payload.get("market_data_source_label")
        or health.get("market_data_source_label")
        or provider
        or "Unknown source"
    )
    prediction_source = str(
        prediction.get("source_label")
        or health.get("prediction_source_label")
        or authority.get("source_label")
        or market_source
    )
    predicted_close = _finite(prediction.get("predicted_close"))
    prediction_usable = bool(
        prediction.get("usable") is True
        and health.get("usable_for_prediction") is True
        and predicted_close is not None
        and predicted_close > 0
    )

    return SidebarSymbolState(
        symbol=symbol,
        state="ready",
        lifecycle_status=lifecycle_status,
        usable=True,
        reason="Lifecycle state is valid, current-epoch, current-generation, and fresh",
        failure_reasons=(),
        checked_at_utc=checked_at_utc,
        source_as_of_utc=source_as_of_utc,
        generated_at_utc=generated_at_utc,
        provider=provider,
        data_age_seconds=data_age_seconds,
        stale_after_seconds=stale_after_seconds,
        subscription_epoch_id=subscription_epoch_id,
        active_subscription_epoch_id=active_subscription_epoch_id,
        epoch_is_current=True,
        active_handoff_status=health_handoff_status,
        subscription_generation=subscription_generation,
        active_generation=active_generation,
        state_revision=state_revision,
        event_id=event_id,
        forecast_id=forecast_id,
        prediction_snapshot_id=prediction_snapshot_id,
        current_price=current_price,
        prediction_usable=prediction_usable,
        predicted_close=predicted_close if prediction_usable else None,
        gamma_pin=_finite(
            pin_payload.get("gamma_pin")
            if pin_payload.get("gamma_pin") is not None
            else pin_payload.get("primary_gamma_pin_strike")
        ),
        max_pain=_finite(
            pin_payload.get("max_pain")
            if pin_payload.get("max_pain") is not None
            else pin_payload.get("max_pain_strike")
        ),
        positive_gex_wall=_finite(pin_payload.get("positive_gex_wall")),
        negative_gex_wall=_finite(pin_payload.get("negative_gex_wall")),
        market_data_source_label=market_source,
        prediction_source_label=prediction_source,
        prediction_is_estimate=bool(
            prediction.get("is_estimate") or authority.get("is_estimate")
        ),
        tcbbo_promoted=authority.get("tcbbo_promoted") is True,
        optional=optional,
    )


def retained_prediction_matches_state(
    prediction: Mapping[str, Any],
    state: SidebarSymbolState,
) -> tuple[bool, str]:
    """Require a retained forecast to match the current immutable state identity."""

    symbol = str(prediction.get("ticker") or "").upper().strip()
    if not symbol or symbol != state.symbol:
        return False, "Retained forecast symbol does not match current lifecycle state"
    if not state.usable:
        return False, state.reason

    retained_epoch_id = _canonical_subscription_epoch(
        prediction.get("subscription_epoch_id")
    )
    if (
        retained_epoch_id is None
        or state.subscription_epoch_id is None
        or state.active_subscription_epoch_id is None
        or state.epoch_is_current is not True
        or retained_epoch_id != state.subscription_epoch_id
        or retained_epoch_id != state.active_subscription_epoch_id
    ):
        return False, "Retained forecast subscription epoch is not current"

    retained_generation = _positive_int(prediction.get("subscription_generation"))
    if (
        retained_generation is None
        or state.subscription_generation is None
        or state.active_generation is None
        or retained_generation != state.subscription_generation
        or retained_generation != state.active_generation
    ):
        return False, "Retained forecast subscription generation is not current"

    retained_revision = _nonnegative_int(prediction.get("state_revision"))
    if (
        retained_revision is None
        or state.state_revision is None
        or retained_revision != state.state_revision
    ):
        return False, "Retained forecast state revision does not match current state"

    retained_pin_payload = prediction.get("pin_payload")
    retained_pin_payload = (
        retained_pin_payload if isinstance(retained_pin_payload, Mapping) else {}
    )
    retained_source = (
        prediction.get("source_as_of_utc")
        or retained_pin_payload.get("timestamp")
        or retained_pin_payload.get("timestamp_utc")
    )
    retained_source_dt = _parse_utc(retained_source)
    current_source_dt = _parse_utc(state.source_as_of_utc)
    if (
        retained_source_dt is None
        or current_source_dt is None
        or retained_source_dt != current_source_dt
    ):
        return False, "Retained forecast source timestamp does not match current state"

    retained_forecast_id = str(prediction.get("forecast_id") or "").strip() or None
    if retained_forecast_id != state.forecast_id:
        return False, "Retained forecast ID does not match current state"

    if prediction.get("tcbbo_promoted") is True:
        authority = prediction.get("prediction_authority")
        authority = authority if isinstance(authority, Mapping) else {}
        identity_fields = (
            prediction.get("source_sha256"),
            prediction.get("model_version"),
            prediction.get("model_artifact_sha256"),
            prediction.get("feature_available_at_utc"),
            prediction.get("prediction_recorded_at_utc"),
            prediction.get("prediction_trading_date"),
        )
        if (
            authority.get("tcbbo_promoted") is not True
            or authority.get("promotion_verified") is not True
            or any(not str(value or "").strip() for value in identity_fields)
        ):
            return False, "Promoted forecast identity or promotion evidence is incomplete"
        return True, "Promoted forecast and current market-state identities match"

    retained_close = _finite(prediction.get("predicted_price"))
    if (
        not state.prediction_usable
        or retained_close is None
        or retained_close <= 0
        or state.predicted_close is None
        or not math.isclose(retained_close, state.predicted_close, rel_tol=0.0, abs_tol=1e-9)
    ):
        return False, "Retained forecast value is not the current usable lifecycle forecast"
    return True, "Retained forecast identity matches current lifecycle state"


def fetch_sidebar_symbol_states(
    symbols: Sequence[str],
    *,
    base_url: str = "http://localhost:8000",
    timeout_seconds: float = 2.0,
    get: HttpGetter | None = None,
    checked_at: datetime | None = None,
) -> SidebarStateBatch:
    """Read all selected states and health concurrently within one timeout window."""

    ordered_symbols = tuple(
        dict.fromkeys(str(symbol).upper().strip() for symbol in symbols if str(symbol).strip())
    )
    checked_at = checked_at or datetime.now(timezone.utc)
    getter = get or requests.get
    if not ordered_symbols:
        return SidebarStateBatch(
            results=(),
            checked_at_utc=_utc_iso(checked_at),
            health_state="not_requested",
            health_reason=None,
        )

    base_url = base_url.rstrip("/")
    urls = {
        symbol: f"{base_url}/dashboard/symbol/{symbol}"
        for symbol in ordered_symbols
    }
    health_url = f"{base_url}/health/live"
    responses: dict[str, _HttpResult] = {}
    with ThreadPoolExecutor(max_workers=min(8, len(ordered_symbols) + 1)) as executor:
        futures = {
            executor.submit(
                _fetch_json,
                url,
                timeout_seconds=timeout_seconds,
                get=getter,
            ): key
            for key, url in {**urls, "__health__": health_url}.items()
        }
        for future in as_completed(futures):
            responses[futures[future]] = future.result()

    health_response = responses["__health__"]
    results = tuple(
        classify_sidebar_symbol_state(
            symbol,
            responses[symbol],
            health_response=health_response,
            checked_at=checked_at,
        )
        for symbol in ordered_symbols
    )
    return SidebarStateBatch(
        results=results,
        checked_at_utc=_utc_iso(checked_at),
        health_state=health_response.state,
        health_reason=health_response.reason,
        dashboard_payloads={
            symbol: response.payload
            for symbol, response in responses.items()
            if symbol != "__health__" and response.state == "ok"
            and response.payload is not None
        },
    )


def summarize_cache_reload(batch: SidebarStateBatch) -> CacheReloadSummary:
    """Summarize reload truthfully while treating VIX as optional context."""

    results = batch.results
    if not results:
        return CacheReloadSummary("error", "No symbols were selected for reload")

    required = tuple(result for result in results if not result.optional)
    optional = tuple(result for result in results if result.optional)
    required_ready = tuple(result for result in required if result.usable)
    optional_ready = tuple(result for result in optional if result.usable)
    optional_failed = tuple(result for result in optional if not result.usable)

    if required:
        if not required_ready:
            return CacheReloadSummary(
                "error",
                f"No required symbol reloaded ({len(required)} failed); no values displayed",
            )
        if len(required_ready) != len(required):
            return CacheReloadSummary(
                "warning",
                f"Partial reload: {len(required_ready)}/{len(required)} required symbols are current",
            )
        optional_suffix = (
            f"; optional VIX unavailable ({optional_failed[0].state})"
            if optional_failed
            else ""
        )
        return CacheReloadSummary(
            "success",
            f"Reloaded {len(required_ready)}/{len(required)} required current states"
            f"{optional_suffix}",
        )

    if len(optional_ready) == len(optional):
        return CacheReloadSummary(
            "success",
            f"Reloaded {len(optional_ready)}/{len(optional)} optional current states",
        )
    if optional_ready:
        return CacheReloadSummary(
            "warning",
            f"Partial optional reload: {len(optional_ready)}/{len(optional)} are current",
        )
    return CacheReloadSummary(
        "error",
        f"No selected symbol reloaded ({len(optional)} optional state failed)",
    )
