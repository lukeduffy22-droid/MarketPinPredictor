"""Deterministic, side-effect-free policy for MarketPin monitor alerts.

The heartbeat remains responsible for collecting read-only evidence.  This module
only evaluates a normalized JSON payload and returns events plus the next small
policy state.  It deliberately does not read HTTP endpoints, files, or databases.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from datetime import date, datetime
from statistics import median
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


INPUT_SCHEMA = "marketpin-monitor-policy.input.v2"
OUTPUT_SCHEMA = "marketpin-monitor-policy.result.v2"

_ALIGNMENT_FIELDS = (
    "symbol",
    "provider",
    "subscription_generation",
    "subscription_epoch_id",
    "primary_expiration",
    "gex_formula_version",
    "universe_sha256",
)
_DIRECTIONAL_PROVENANCE_FIELDS = _ALIGNMENT_FIELDS + ("universe_is_fallback",)
_LEVEL_PROVENANCE_FIELDS = {
    "gamma_pin": {
        "symbol",
        "provider",
        "subscription_generation",
        "subscription_epoch_id",
        "primary_expiration",
        "universe_sha256",
        "universe_is_fallback",
        "formula_version",
    },
    "max_pain": {
        "symbol",
        "provider",
        "subscription_generation",
        "subscription_epoch_id",
        "primary_expiration",
        "universe_sha256",
        "universe_is_fallback",
        "formula_version",
        "source",
        "as_of",
    },
}
_DIRECTIONS = {"bullish", "bearish"}
_MONITORED_SYMBOLS = {"SPX", "NDX"}
_SESSION_TIMEZONE = ZoneInfo("America/Chicago")
_FULL_OI_SOURCES = {
    "full-oi-universe",
    "databento-statistics-open-interest-full-universe",
}
_NORMAL_CONFIRMATION_CADENCE_SECONDS = 15 * 60
_ELEVATED_CONFIRMATION_CADENCE_SECONDS = 5 * 60
_SUPPORTED_CONFIRMATION_CADENCE_SECONDS = {
    _NORMAL_CONFIRMATION_CADENCE_SECONDS,
    _ELEVATED_CONFIRMATION_CADENCE_SECONDS,
}
_CONFIRMATION_MIN_GAP_SECONDS = 4 * 60
_CONFIRMATION_GAP_GRACE_SECONDS = 5 * 60
_SUPPORTED_PRICE_CHANGE_SCHEMA = "marketpin-monitor-supported-price-change.v1"
_PRICE_CHANGE_15M_WINDOW_SECONDS = 15 * 60
_PRICE_CHANGE_15M_TOLERANCE_SECONDS = 2 * 60


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _truth(value: Any) -> bool:
    return value is True


def _canonical_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _observation_id(observation: Mapping[str, Any]) -> str:
    explicit = str(observation.get("observation_id") or "").strip()
    if explicit:
        return explicit
    material = {
        key: observation.get(key)
        for key in (
            "symbol",
            "observed_at_utc",
            "subscription_generation",
            "subscription_epoch_id",
            "primary_expiration",
            "gex_formula_version",
            "universe_sha256",
            "spot",
            "gamma_pin",
            "max_pain",
            "zero_gamma",
            "normalized_net_gex",
        )
    }
    return _canonical_hash(material)


def _event_id(event_type: str, symbol: str, material: Mapping[str, Any]) -> str:
    return _canonical_hash(
        {
            "event_schema": "marketpin-monitor-event.v3",
            "event_type": event_type,
            "symbol": symbol,
            "material": material,
        }
    )


def _level_transition_predecessor(
    level: str, prior: Mapping[str, Any]
) -> dict[str, str]:
    """Identify the state edge before a level transition.

    Confirmation observations are evidence for an event, not its identity. If
    the caller appended an event but crashed before persisting ``next_state``,
    the same old state may be evaluated against a later confirmation pair. A
    state-derived predecessor keeps that replay on the same semantic edge. A
    successfully persisted event becomes the predecessor for a later genuine
    reversal or repeated cycle, giving that transition a new identity.
    """

    last_event_id = str(prior.get("last_event_id") or "").strip()
    if last_event_id:
        return {"kind": "event", "id": last_event_id}
    baseline_material = {
        "level": level,
        "value": _finite(prior.get("value")),
        "provenance": prior.get("provenance"),
    }
    return {
        "kind": "baseline",
        "id": _canonical_hash(
            {
                "event_schema": "marketpin-monitor-level-baseline.v2",
                "material": baseline_material,
            }
        ),
    }


def _directional_transition_predecessor(
    latch: Mapping[str, Any],
    session_date: str | None,
    alignment_provenance: Mapping[str, Any],
) -> dict[str, str | None]:
    last_event_id = str(latch.get("last_event_id") or "").strip()
    if last_event_id:
        return {"kind": "event", "id": last_event_id}
    return {
        "kind": "session_arm",
        "id": _canonical_hash(
            {
                "event_schema": "marketpin-monitor-directional-arm.v2",
                "session_date": session_date,
                "active_direction": str(latch.get("active_direction") or "").lower()
                or None,
                "alignment_provenance": dict(alignment_provenance),
            }
        ),
    }


def _valid_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    offset = parsed.utcoffset()
    return parsed.tzinfo is not None and offset is not None and offset.total_seconds() == 0


def _parsed_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _append_valid_timestamps(target: list[datetime], value: Any) -> None:
    values = value if isinstance(value, list) else [value]
    for candidate in values:
        if _valid_timestamp(candidate):
            target.append(_parsed_timestamp(str(candidate)))


def _policy_evidence_timestamps(state: Mapping[str, Any]) -> list[datetime]:
    """Return every valid timestamp carried by persisted policy evidence."""

    candidates: list[datetime] = []
    levels = state.get("levels")
    if isinstance(levels, Mapping):
        for raw_level in levels.values():
            if not isinstance(raw_level, Mapping):
                continue
            if raw_level.get("historical_context_only") is True:
                continue
            _append_valid_timestamps(candidates, raw_level.get("confirmed_at_utc"))
            _append_valid_timestamps(
                candidates, raw_level.get("confirmation_observed_at_utc")
            )

    latch = state.get("directional_latch")
    if isinstance(latch, Mapping):
        for field in (
            "last_alert_confirmed_at_utc",
            "last_alert_confirmation_observed_at_utc",
            "last_rearmed_at_utc",
            "last_rearm_observed_at_utc",
        ):
            _append_valid_timestamps(candidates, latch.get(field))
        pending = latch.get("pending_rearm_observations")
        if isinstance(pending, list):
            for row in pending:
                if isinstance(row, Mapping):
                    _append_valid_timestamps(candidates, row.get("observed_at_utc"))
    return candidates


def _canonical_session_date(value: Any) -> date | None:
    if not isinstance(value, str) or re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) is None:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.isoformat() == value else None


def _resolved_policy_watermark(
    state: Mapping[str, Any],
) -> tuple[datetime | None, list[str]]:
    """Reconcile the explicit watermark with all richer persisted evidence."""

    issues: list[str] = []
    inferred_timestamps = _policy_evidence_timestamps(state)
    inferred_watermark = max(inferred_timestamps) if inferred_timestamps else None
    explicit_raw = state.get("last_accepted_observed_at_utc")
    explicit_watermark: datetime | None = None
    if explicit_raw not in (None, ""):
        if not _valid_timestamp(explicit_raw):
            issues.append("policy_state.last_accepted_observed_at_utc_invalid")
        else:
            explicit_watermark = _parsed_timestamp(str(explicit_raw))

    stored_session = _canonical_session_date(state.get("session_date"))
    if stored_session is not None:
        if (
            explicit_watermark is not None
            and explicit_watermark.astimezone(_SESSION_TIMEZONE).date()
            != stored_session
        ):
            issues.append("policy_state_explicit_watermark_session_mismatch")
        if any(
            timestamp.astimezone(_SESSION_TIMEZONE).date() != stored_session
            for timestamp in inferred_timestamps
        ):
            issues.append("policy_state_inferred_watermark_session_mismatch")

    if (
        explicit_watermark is not None
        and inferred_watermark is not None
        and explicit_watermark < inferred_watermark
    ):
        issues.append("policy_state_watermark_precedes_persisted_evidence")

    candidates = [
        candidate
        for candidate in (explicit_watermark, inferred_watermark)
        if candidate is not None
    ]
    return (max(candidates) if candidates else None), issues


def _policy_temporal_issues(
    state: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]
) -> list[str]:
    """Reject replay, rollback, and malformed persisted temporal state."""

    current_session = _session_date_ct(observations[-1])
    if current_session is None:
        return []

    issues: list[str] = []
    prior_session_raw = state.get("session_date")
    prior_session: date | None = None
    if "session_date" in state:
        prior_session = _canonical_session_date(prior_session_raw)
        if prior_session is None:
            issues.append("policy_state.session_date_invalid")
    current_session_date = date.fromisoformat(current_session)
    if prior_session is not None and current_session_date < prior_session:
        issues.append("policy_state_session_rollback_rejected")

    watermark, watermark_issues = _resolved_policy_watermark(state)
    issues.extend(watermark_issues)

    latest_observation = _parsed_timestamp(str(observations[-1]["observed_at_utc"]))
    if watermark is not None and latest_observation <= watermark:
        issues.append("latest_observation_not_after_policy_watermark")
    return issues


def _session_date_ct(observation: Mapping[str, Any]) -> str | None:
    value = observation.get("observed_at_utc")
    if not _valid_timestamp(value):
        return None
    return _parsed_timestamp(str(value)).astimezone(_SESSION_TIMEZONE).date().isoformat()


def _alignment_provenance(observation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: copy.deepcopy(observation.get(field))
        for field in _DIRECTIONAL_PROVENANCE_FIELDS
    }


def _observation_marker(observation: Mapping[str, Any]) -> dict[str, Any]:
    observed_at_utc = str(observation.get("observed_at_utc") or "")
    observed_at_ct = _parsed_timestamp(observed_at_utc).astimezone(
        _SESSION_TIMEZONE
    ).isoformat()
    return {
        "observation_id": _observation_id(observation),
        "observed_at_utc": observed_at_utc,
        "observed_at_ct": observed_at_ct,
        "alignment_provenance": _alignment_provenance(observation),
    }


def _confirmation_metadata(
    observations: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    markers = [_observation_marker(row) for row in observations[-2:]]
    current = markers[-1]
    return {
        "confirmation_observation_ids": [row["observation_id"] for row in markers],
        "confirmation_observed_at_utc": [row["observed_at_utc"] for row in markers],
        "confirmation_observed_at_ct": [row["observed_at_ct"] for row in markers],
        "confirmed_at_utc": current["observed_at_utc"],
        "confirmed_at_ct": current["observed_at_ct"],
    }


def _confirmed_level_state(
    *,
    value: float,
    provenance: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    status: str,
    prior_provenance: Any,
    last_event_id: str | None = None,
    threshold_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    current_provenance = copy.deepcopy(dict(provenance))
    result: dict[str, Any] = {
        "value": value,
        "provenance": current_provenance,
        "prior_provenance": copy.deepcopy(prior_provenance),
        "current_provenance": copy.deepcopy(current_provenance),
        "status": status,
        **_confirmation_metadata(observations),
    }
    if last_event_id:
        result["last_event_id"] = last_event_id
    if threshold_evidence is not None:
        result["threshold_evidence"] = copy.deepcopy(dict(threshold_evidence))
    return result


def _retain_existing_level_evidence(
    prior: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """Backfill missing confirmation metadata without erasing alert evidence."""

    retained = copy.deepcopy(dict(prior))
    for key, value in _confirmation_metadata(observations).items():
        retained.setdefault(key, value)
    current_provenance = retained.get("provenance")
    retained.setdefault("prior_provenance", copy.deepcopy(current_provenance))
    retained.setdefault("current_provenance", copy.deepcopy(current_provenance))
    return retained


def policy_observation_issues(
    observation: Mapping[str, Any], *, prefix: str = "observation"
) -> list[str]:
    """Return fail-closed eligibility issues for one normalized observation.

    The scan ledger also uses this validator for a seed observation before a
    two-scan confirmation pair exists.  Keeping the predicate here prevents a
    seed-only scan from weakening the evaluator's source/provenance gates.
    """

    issues: list[str] = []
    if not _truth(observation.get("eligible")):
        issues.append(f"{prefix}.eligible_must_be_true")
    if observation.get("validation_is_valid") is not True:
        issues.append(f"{prefix}.validation_is_valid_must_be_true")
    if observation.get("gamma_excluded_from_model") is not False:
        issues.append(f"{prefix}.gamma_excluded_from_model_must_be_false")
    if observation.get("universe_is_fallback") is not False:
        issues.append(f"{prefix}.universe_is_fallback_must_be_false")
    if not _valid_timestamp(observation.get("observed_at_utc")):
        issues.append(f"{prefix}.observed_at_utc_missing_or_invalid")
    for field in _ALIGNMENT_FIELDS:
        value = observation.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            issues.append(f"{prefix}.{field}_missing")
    raw_generation = observation.get("subscription_generation")
    if type(raw_generation) is not int or raw_generation <= 0:
        issues.append(f"{prefix}.subscription_generation_must_be_positive")
    subscription_epoch_id = observation.get("subscription_epoch_id")
    if (
        not isinstance(subscription_epoch_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None
    ):
        issues.append(f"{prefix}.subscription_epoch_id_must_be_sha256")
    if observation.get("provider") != "databento":
        issues.append(f"{prefix}.provider_must_be_databento")
    if observation.get("symbol") not in _MONITORED_SYMBOLS:
        issues.append(f"{prefix}.symbol_must_be_spx_or_ndx")
    universe = observation.get("universe_sha256")
    if not isinstance(universe, str) or re.fullmatch(r"[0-9a-f]{64}", universe) is None:
        issues.append(f"{prefix}.universe_sha256_must_be_sha256")
    max_pain_source = observation.get("max_pain_source")
    if (
        _finite(observation.get("max_pain")) is not None
        and (
            not isinstance(max_pain_source, str)
            or max_pain_source not in _FULL_OI_SOURCES
        )
    ):
        issues.append(f"{prefix}.max_pain_source_must_be_canonical_full_oi")
    return issues


def _eligibility_issues(observation: Mapping[str, Any], index: int) -> list[str]:
    return policy_observation_issues(
        observation, prefix=f"observations[{index}]"
    )


def _alignment_issues(observations: Sequence[Mapping[str, Any]]) -> list[str]:
    issues: list[str] = []
    for field in _ALIGNMENT_FIELDS:
        values = {str(observation.get(field)) for observation in observations}
        if len(values) != 1:
            issues.append(f"alignment_mismatch:{field}")
    raw_times = [str(observation.get("observed_at_utc") or "") for observation in observations]
    if all(_valid_timestamp(value) for value in raw_times):
        times = [_parsed_timestamp(value) for value in raw_times]
        if times != sorted(times) or len(set(times)) != len(times):
            issues.append("observations_must_be_unique_and_chronological")
        observation_ids = [_observation_id(observation) for observation in observations]
        if len(set(observation_ids)) != len(observation_ids):
            issues.append("observations_must_have_unique_ids")
        session_dates = {_session_date_ct(observation) for observation in observations}
        if len(session_dates) != 1:
            issues.append("alignment_mismatch:session_date_ct")
    return issues


def _confirmation_gap_policy(
    payload: Mapping[str, Any], observations: Sequence[Mapping[str, Any]]
) -> tuple[int | None, int, int | None, float | None, list[str]]:
    """Require a recent pair instead of treating any same-day rows as consecutive.

    The optional cadence is the collector's intended scan cadence. Omitting it
    keeps backward compatibility with the normal 15-minute monitor. A fixed
    five-minute grace accommodates scheduler/runtime jitter without allowing a
    long monitoring outage to bridge an alert confirmation.
    """

    raw_cadence = payload.get(
        "confirmation_cadence_seconds", _NORMAL_CONFIRMATION_CADENCE_SECONDS
    )
    if (
        type(raw_cadence) is not int
        or raw_cadence not in _SUPPORTED_CONFIRMATION_CADENCE_SECONDS
    ):
        return (
            None,
            _CONFIRMATION_MIN_GAP_SECONDS,
            None,
            None,
            ["confirmation_cadence_seconds_must_be_300_or_900"],
        )

    maximum_gap = raw_cadence + _CONFIRMATION_GAP_GRACE_SECONDS
    if len(observations) < 2:
        return (
            raw_cadence,
            _CONFIRMATION_MIN_GAP_SECONDS,
            maximum_gap,
            None,
            [],
        )
    raw_times = [
        str(observation.get("observed_at_utc") or "")
        for observation in observations[-2:]
    ]
    if not all(_valid_timestamp(value) for value in raw_times):
        return (
            raw_cadence,
            _CONFIRMATION_MIN_GAP_SECONDS,
            maximum_gap,
            None,
            [],
        )

    timestamps = [_parsed_timestamp(value) for value in raw_times]
    actual_gap = (timestamps[-1] - timestamps[-2]).total_seconds()
    issues: list[str] = []
    if actual_gap < _CONFIRMATION_MIN_GAP_SECONDS:
        issues.append("confirmation_gap_below_minimum_seconds")
    if actual_gap > maximum_gap:
        issues.append("confirmation_gap_exceeds_maximum_seconds")
    return (
        raw_cadence,
        _CONFIRMATION_MIN_GAP_SECONDS,
        maximum_gap,
        actual_gap,
        issues,
    )


def _level_provenance(observation: Mapping[str, Any], level: str) -> dict[str, Any]:
    base = {
        "symbol": str(observation.get("symbol") or "").upper(),
        "provider": observation.get("provider"),
        "subscription_generation": observation.get("subscription_generation"),
        "subscription_epoch_id": observation.get("subscription_epoch_id"),
        "primary_expiration": observation.get("primary_expiration"),
        "universe_sha256": observation.get("universe_sha256"),
        "universe_is_fallback": observation.get("universe_is_fallback"),
    }
    if level == "max_pain":
        base.update(
            {
                "formula_version": observation.get("max_pain_formula_version"),
                "source": observation.get("max_pain_source"),
                "as_of": observation.get("max_pain_as_of"),
            }
        )
    else:
        base["formula_version"] = observation.get("gex_formula_version")
    return base


def _level_provenance_complete(provenance: Any, level: str) -> bool:
    required = _LEVEL_PROVENANCE_FIELDS.get(level)
    if not isinstance(provenance, Mapping) or required is None:
        return False
    if set(provenance) != required:
        return False
    if provenance.get("symbol") not in _MONITORED_SYMBOLS:
        return False
    if provenance.get("provider") != "databento":
        return False
    if provenance.get("universe_is_fallback") is not False:
        return False
    generation = provenance.get("subscription_generation")
    if type(generation) is not int or generation <= 0:
        return False
    for field in ("subscription_epoch_id", "universe_sha256"):
        value = provenance.get(field)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            return False
    for field in ("primary_expiration", "formula_version"):
        value = provenance.get(field)
        if not isinstance(value, str) or not value:
            return False
    if level == "max_pain":
        if provenance.get("source") not in _FULL_OI_SOURCES:
            return False
        as_of = provenance.get("as_of")
        if not isinstance(as_of, str) or not as_of:
            return False
    return True


def _rollover_level_state(
    level_state: Mapping[str, Any], prior_session_date: str | None
) -> dict[str, Any]:
    """Carry only complete prior levels as historical comparison evidence."""

    carried: dict[str, Any] = {}
    for level in _LEVEL_PROVENANCE_FIELDS:
        row = level_state.get(level)
        if not isinstance(row, Mapping) or _finite(row.get("value")) is None:
            continue
        provenance = row.get("provenance")
        if not _level_provenance_complete(provenance, level):
            continue
        retained = copy.deepcopy(dict(row))
        retained.update(
            {
                "historical_context_only": True,
                "prior_session_date": prior_session_date,
            }
        )
        carried[level] = retained
    return carried


def _same_number(left: Any, right: Any, *, tolerance: float = 1e-9) -> bool:
    left_number = _finite(left)
    right_number = _finite(right)
    return bool(
        left_number is not None
        and right_number is not None
        and math.isclose(left_number, right_number, rel_tol=0.0, abs_tol=tolerance)
    )


def _top_strikes_by_observation(
    observations: Sequence[Mapping[str, Any]],
) -> list[list[float]]:
    result: list[list[float]] = []
    for observation in observations:
        raw = observation.get("top_strikes_by_abs_gex") or []
        strikes = sorted(
            {
                strike
                for item in raw
                if isinstance(item, Mapping)
                and (strike := _finite(item.get("strike"))) is not None
            }
        )
        result.append(strikes)
    return result


def _top_strike_spacing(observations: Sequence[Mapping[str, Any]]) -> float | None:
    spacings: list[float] = []
    for strikes in _top_strikes_by_observation(observations):
        spacings.extend(
            right - left
            for left, right in zip(strikes, strikes[1:])
            if right > left
        )
    return float(median(spacings)) if spacings else None


def _price_change_15m_pct(observation: Mapping[str, Any]) -> float | None:
    supported = observation.get("supported_price_changes")
    if isinstance(supported, Mapping):
        window = supported.get("15m")
        if isinstance(window, Mapping):
            start_time = window.get("baseline_observed_at_utc")
            end_time = window.get("current_observed_at_utc")
            observation_time = observation.get("observed_at_utc")
            start_price = _finite(window.get("baseline_spot"))
            end_price = _finite(window.get("current_spot"))
            observation_spot = _finite(observation.get("spot"))
            pct = _finite(window.get("pct"))
            actual_interval = _finite(window.get("actual_interval_seconds"))
            baseline_scan_id = str(window.get("baseline_scan_event_id") or "")
            baseline_observation_id = str(
                window.get("baseline_observation_id") or ""
            )
            current_observation_id = str(
                window.get("current_observation_id") or ""
            )
            timestamps_valid = all(
                _valid_timestamp(value)
                for value in (start_time, end_time, observation_time)
            )
            if timestamps_valid:
                start = _parsed_timestamp(str(start_time))
                end = _parsed_timestamp(str(end_time))
                computed_interval = (end - start).total_seconds()
                same_session = (
                    start.astimezone(_SESSION_TIMEZONE).date()
                    == end.astimezone(_SESSION_TIMEZONE).date()
                )
            else:
                computed_interval = None
                same_session = False
            if (
                window.get("schema_version") == _SUPPORTED_PRICE_CHANGE_SCHEMA
                and window.get("window_seconds") == _PRICE_CHANGE_15M_WINDOW_SECONDS
                and actual_interval is not None
                and computed_interval is not None
                and math.isclose(
                    actual_interval, computed_interval, rel_tol=0.0, abs_tol=1e-9
                )
                and (
                    _PRICE_CHANGE_15M_WINDOW_SECONDS
                    - _PRICE_CHANGE_15M_TOLERANCE_SECONDS
                    <= actual_interval
                    <= _PRICE_CHANGE_15M_WINDOW_SECONDS
                    + _PRICE_CHANGE_15M_TOLERANCE_SECONDS
                )
                and same_session
                and end_time == observation_time
                and start_price is not None
                and start_price > 0.0
                and end_price is not None
                and end_price > 0.0
                and observation_spot is not None
                and math.isclose(
                    end_price, observation_spot, rel_tol=1e-12, abs_tol=1e-9
                )
                and pct is not None
                and math.isclose(
                    pct,
                    (end_price - start_price) / start_price * 100.0,
                    rel_tol=1e-12,
                    abs_tol=1e-9,
                )
                and re.fullmatch(r"[0-9a-f]{64}", baseline_scan_id) is not None
                and re.fullmatch(r"[0-9a-f]{64}", baseline_observation_id)
                is not None
                and current_observation_id == _observation_id(observation)
            ):
                return pct
            return None
    # Backward-compatible input for already-durable v2 observations. New
    # collector output uses the self-validating receipt-backed structure above.
    return _finite(observation.get("price_change_15m_pct"))


def _forecast_direction(observation: Mapping[str, Any]) -> str | None:
    bias = str(observation.get("forecast_bias") or "").lower()
    if bias not in _DIRECTIONS:
        return None
    expected_move = _finite(observation.get("expected_move_pct"))
    if expected_move is not None and abs(expected_move) >= 0.10:
        if (expected_move > 0 and bias == "bullish") or (
            expected_move < 0 and bias == "bearish"
        ):
            return bias
    spot = _finite(observation.get("spot"))
    lower = _finite(observation.get("prediction_lower"))
    upper = _finite(observation.get("prediction_upper"))
    if spot is not None and lower is not None and lower > spot and bias == "bullish":
        return bias
    if spot is not None and upper is not None and upper < spot and bias == "bearish":
        return bias
    return None


def _price_confirmations(
    observations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    previous, current = observations[-2:]
    evidence: list[dict[str, Any]] = []

    forecast_directions = [_forecast_direction(previous), _forecast_direction(current)]
    if (
        forecast_directions[0] is not None
        and forecast_directions[1] is not None
        and forecast_directions[0] != forecast_directions[1]
    ):
        evidence.append(
            {
                "category": "price",
                "type": "forecast_bias_flip",
                "direction": forecast_directions[1],
                "previous_direction": forecast_directions[0],
                "transition_observation_ids": [
                    _observation_id(previous),
                    _observation_id(current),
                ],
            }
        )

    changes = [_price_change_15m_pct(previous), _price_change_15m_pct(current)]
    if all(change is not None and abs(change) >= 0.20 for change in changes):
        directions = ["bullish" if change > 0 else "bearish" for change in changes]
        if len(set(directions)) == 1:
            evidence.append(
                {
                    "category": "price",
                    "type": "persistent_15m_price_move",
                    "direction": directions[0],
                }
            )

    orb_rows = [previous.get("orb_breakout"), current.get("orb_breakout")]
    if all(isinstance(row, Mapping) for row in orb_rows):
        directions = [str(row.get("direction") or "").lower() for row in orb_rows]
        eligible = all(
            row.get("directional_evidence_eligible") is True
            and row.get("current_reference_fresh") is True
            for row in orb_rows
        )
        if eligible and directions[0] in _DIRECTIONS and len(set(directions)) == 1:
            evidence.append(
                {
                    "category": "price",
                    "type": "persistent_orb_breakout",
                    "direction": directions[0],
                }
            )
    return evidence


def _cross_direction(
    previous: Mapping[str, Any], current: Mapping[str, Any], field: str
) -> str | None:
    previous_spot = _finite(previous.get("spot"))
    current_spot = _finite(current.get("spot"))
    previous_level = _finite(previous.get(field))
    current_level = _finite(current.get(field))
    if None in {previous_spot, current_spot, previous_level, current_level}:
        return None
    previous_distance = previous_spot - previous_level
    current_distance = current_spot - current_level
    if previous_distance == 0 or current_distance == 0:
        return None
    if previous_distance * current_distance < 0:
        return "bullish" if current_distance > 0 else "bearish"
    return None


def _structure_confirmations(
    observations: Sequence[Mapping[str, Any]],
    gamma_event: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    previous, current = observations[-2:]
    evidence: list[dict[str, Any]] = []
    if gamma_event is not None:
        direction = "bullish" if gamma_event["new_value"] > gamma_event["old_value"] else "bearish"
        evidence.append(
            {
                "category": "structure",
                "type": "confirmed_significant_gamma_pin_shift",
                "direction": direction,
                "event_id": gamma_event["event_id"],
            }
        )

    previous_gex = _finite(previous.get("normalized_net_gex"))
    current_gex = _finite(current.get("normalized_net_gex"))
    if previous_gex is not None and current_gex is not None:
        sign_change = previous_gex * current_gex < 0
        magnitude_gate = min(abs(previous_gex), abs(current_gex)) >= 0.10
        delta_gate = abs(current_gex - previous_gex) >= 0.25
        if (sign_change and magnitude_gate) or delta_gate:
            evidence.append(
                {
                    "category": "structure",
                    "type": "normalized_net_gex_regime_change",
                    "direction": "bullish" if current_gex > previous_gex else "bearish",
                    "previous": previous_gex,
                    "current": current_gex,
                }
            )

    for field in ("zero_gamma", "positive_gex_wall", "negative_gex_wall"):
        direction = _cross_direction(previous, current, field)
        if direction:
            evidence.append(
                {
                    "category": "structure",
                    "type": f"spot_cross_{field}",
                    "direction": direction,
                }
            )

    previous_flow = previous.get("research_flow")
    current_flow = current.get("research_flow")
    if isinstance(previous_flow, Mapping) and isinstance(current_flow, Mapping):
        raw_scores = [previous_flow.get("score"), current_flow.get("score")]
        scores = [
            _finite(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
            for value in raw_scores
        ]
        previous_score, current_score = scores
        raw_shares = [
            previous_flow.get("classified_share"),
            current_flow.get("classified_share"),
        ]
        raw_minimums = [
            previous_flow.get("minimum_classified_share"),
            current_flow.get("minimum_classified_share"),
        ]
        shares = [
            _finite(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
            for value in raw_shares
        ]
        minimums = [
            _finite(value)
            if isinstance(value, (int, float)) and not isinstance(value, bool)
            else None
            for value in raw_minimums
        ]
        adequate = all(
            share is not None
            and minimum is not None
            and 0.0 <= share <= 1.0
            and 0.0 < minimum <= 1.0
            and share >= minimum
            for share, minimum in zip(shares, minimums)
        )
        scores_valid = all(
            score is not None and -1.0 <= score <= 1.0 for score in scores
        )
        explicitly_eligible = all(
            row.get("fresh") is True and row.get("research_only") is True
            for row in (previous_flow, current_flow)
        )
        if (
            adequate
            and scores_valid
            and explicitly_eligible
            and previous_score is not None
            and current_score is not None
        ):
            if previous_score <= 0.15 < current_score:
                direction = "bullish"
            elif previous_score >= -0.15 > current_score:
                direction = "bearish"
            else:
                direction = None
            if direction:
                evidence.append(
                    {
                        "category": "structure",
                        "type": "classified_research_flow_threshold_cross",
                        "direction": direction,
                    }
                )

    # Deliberately absent: merely remaining above or below a pin/wall is a
    # static relationship, not a new options-structure confirmation.
    return evidence


def _confirmed_level_value(
    observations: Sequence[Mapping[str, Any]], field: str
) -> float | None:
    previous, current = observations[-2:]
    if not _same_number(previous.get(field), current.get(field)):
        return None
    return _finite(current.get(field))


def _max_pain_event(
    observations: Sequence[Mapping[str, Any]],
    level_state: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    previous, current = observations[-2:]
    value = _confirmed_level_value(observations, "max_pain")
    suppressions: list[str] = []
    if value is None:
        return None, None, ["max_pain_not_confirmed_across_two_observations"]
    sources = {row.get("max_pain_source") for row in observations[-2:]}
    if len(sources) != 1 or not sources.issubset(_FULL_OI_SOURCES):
        return None, None, ["max_pain_source_not_aligned_full_oi"]
    provenance = _level_provenance(current, "max_pain")
    if any(provenance.get(field) in (None, "") for field in ("formula_version", "as_of")):
        return None, None, ["max_pain_provenance_incomplete"]
    if _level_provenance(previous, "max_pain") != provenance:
        return None, None, ["max_pain_provenance_not_aligned_between_confirmations"]

    prior = level_state.get("max_pain")
    if not isinstance(prior, Mapping) or _finite(prior.get("value")) is None:
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=None,
        ), ["max_pain_baseline_initialized_without_alert"]
    old_value = float(prior["value"])
    prior_provenance = prior.get("provenance")
    if not _level_provenance_complete(prior_provenance, "max_pain"):
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=None,
        ), ["max_pain_prior_provenance_incomplete_rebaselined"]
    same_provenance = bool(
        isinstance(prior_provenance, Mapping)
        and dict(prior_provenance) == provenance
    )
    if _same_number(old_value, value):
        if same_provenance:
            return None, _retain_existing_level_evidence(prior, observations), [
                "max_pain_unchanged_from_last_alerted_or_baseline"
            ]
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=prior_provenance,
            last_event_id=str(prior.get("last_event_id") or "").strip() or None,
        ), ["max_pain_unchanged_value_provenance_rebaselined"]
    event_type = "MAX_PAIN_CHANGE" if same_provenance else "MAX_PAIN_PROVENANCE_RESET"
    confirmation = _confirmation_metadata(observations)
    material = {
        "session_date": _session_date_ct(current),
        "old_value": old_value,
        "new_value": value,
        "prior_provenance": prior_provenance,
        "current_provenance": provenance,
        "predecessor": _level_transition_predecessor("max_pain", prior),
    }
    event = {
        "type": event_type,
        "symbol": str(current.get("symbol")),
        "event_id": _event_id(event_type, str(current.get("symbol")), material),
        "event_identity_schema": "marketpin-monitor-event.v3",
        "predecessor": material["predecessor"],
        "old_value": old_value,
        "new_value": value,
        "change_points": value - old_value,
        "change_pct": ((value - old_value) / old_value * 100.0) if old_value else None,
        "provenance_reset": event_type.endswith("PROVENANCE_RESET"),
        **confirmation,
        "prior_provenance": prior_provenance,
        "current_provenance": provenance,
    }
    next_level = _confirmed_level_state(
        value=value,
        provenance=provenance,
        observations=observations,
        status="alerted",
        prior_provenance=prior_provenance,
        last_event_id=event["event_id"],
    )
    return event, next_level, suppressions


def _gamma_pin_event(
    observations: Sequence[Mapping[str, Any]],
    level_state: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, list[str]]:
    previous, current = observations[-2:]
    value = _confirmed_level_value(observations, "gamma_pin")
    if value is None:
        return None, None, ["gamma_pin_not_confirmed_across_two_observations"]
    provenance = _level_provenance(current, "gamma_pin")
    if _level_provenance(previous, "gamma_pin") != provenance:
        return None, None, ["gamma_pin_provenance_not_aligned_between_confirmations"]
    prior = level_state.get("gamma_pin")
    if not isinstance(prior, Mapping) or _finite(prior.get("value")) is None:
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=None,
        ), ["gamma_pin_baseline_initialized_without_alert"]
    old_value = float(prior["value"])
    prior_provenance = prior.get("provenance")
    if not _level_provenance_complete(prior_provenance, "gamma_pin"):
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=None,
        ), ["gamma_pin_prior_provenance_incomplete_rebaselined"]
    same_provenance = bool(
        isinstance(prior_provenance, Mapping)
        and dict(prior_provenance) == provenance
    )
    if _same_number(old_value, value):
        if same_provenance:
            return None, _retain_existing_level_evidence(prior, observations), [
                "gamma_pin_unchanged_from_last_alerted_or_baseline"
            ]
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=prior_provenance,
            last_event_id=str(prior.get("last_event_id") or "").strip() or None,
        ), ["gamma_pin_unchanged_value_provenance_rebaselined"]
    if not same_provenance:
        return None, _confirmed_level_state(
            value=value,
            provenance=provenance,
            observations=observations,
            status="baseline",
            prior_provenance=prior_provenance,
            last_event_id=str(prior.get("last_event_id") or "").strip() or None,
        ), ["gamma_pin_provenance_reset_requires_new_baseline"]

    spot_by_observation = [_finite(row.get("spot")) for row in observations[-2:]]
    spot = spot_by_observation[-1]
    top_strikes = _top_strikes_by_observation(observations[-2:])
    spacing = _top_strike_spacing(observations[-2:])
    fallback = 20.0 if str(current.get("symbol") or "").upper() == "SPX" else 75.0
    spot_component = abs(spot) * 0.0015 if spot is not None else 0.0
    if spacing is not None:
        components = [spot_component, 2.0 * spacing]
        threshold_source = "spot_and_observed_top_strike_spacing"
    else:
        components = [spot_component, fallback]
        threshold_source = "spot_and_conservative_spacing_fallback"
    threshold = max(components)
    change = value - old_value
    crossed_spot = bool(
        spot is not None and (old_value - spot) * (value - spot) <= 0 and old_value != value
    )
    zero_gamma_by_observation = [
        _finite(row.get("zero_gamma")) for row in observations[-2:]
    ]
    zero_gamma = zero_gamma_by_observation[-1]
    crossed_zero_gamma = bool(
        zero_gamma is not None
        and (old_value - zero_gamma) * (value - zero_gamma) <= 0
        and old_value != value
    )
    significant = abs(change) >= threshold or crossed_spot or crossed_zero_gamma
    if not significant:
        return None, dict(prior), ["gamma_pin_change_below_significant_threshold"]

    lead_ratios = [_finite(row.get("pin_lead_ratio")) for row in observations[-2:]]
    contested_flags = [
        row.get("pin_is_contested") is True for row in observations[-2:]
    ]
    contested = any(contested_flags) or any(
        ratio is None or ratio <= 0.10 for ratio in lead_ratios
    )
    spot_crossed_new_pin = _cross_direction(previous, current, "gamma_pin") is not None
    if contested and not spot_crossed_new_pin:
        return None, dict(prior), ["gamma_pin_shift_suppressed_contested_leadership"]

    threshold_evidence = {
        "calculation_version": "gamma-pin-significance-v1",
        "threshold_points": threshold,
        "threshold_source": threshold_source,
        "spot": spot,
        "spot_by_observation": spot_by_observation,
        "spot_component_points": spot_component,
        "top_strikes_by_observation": top_strikes,
        "median_top_strike_spacing": spacing,
        "spacing_component_points": 2.0 * spacing if spacing is not None else None,
        "conservative_fallback_points": fallback,
        "fallback_used": spacing is None,
        "components_points": components,
        "change_points": change,
        "absolute_change_points": abs(change),
        "absolute_change_qualified": abs(change) >= threshold,
        "old_pin_value": old_value,
        "new_pin_value": value,
        "zero_gamma": zero_gamma,
        "zero_gamma_by_observation": zero_gamma_by_observation,
        "crossed_spot": crossed_spot,
        "crossed_zero_gamma": crossed_zero_gamma,
        "pin_lead_ratios": lead_ratios,
        "pin_is_contested_by_observation": contested_flags,
        "pin_contest_threshold": 0.10,
        "pin_is_contested": contested,
        "spot_crossed_new_pin": spot_crossed_new_pin,
        "significant": significant,
    }
    confirmation = _confirmation_metadata(observations)
    material = {
        "session_date": _session_date_ct(current),
        "old_value": old_value,
        "new_value": value,
        "prior_provenance": prior_provenance,
        "current_provenance": provenance,
        "predecessor": _level_transition_predecessor("gamma_pin", prior),
    }
    event = {
        "type": "GAMMA_PIN_SHIFT",
        "symbol": str(current.get("symbol")),
        "event_id": _event_id("GAMMA_PIN_SHIFT", str(current.get("symbol")), material),
        "event_identity_schema": "marketpin-monitor-event.v3",
        "predecessor": material["predecessor"],
        "old_value": old_value,
        "new_value": value,
        "change_points": change,
        "threshold_points": threshold,
        "threshold_source": threshold_source,
        "median_top_strike_spacing": spacing,
        "crossed_spot": crossed_spot,
        "crossed_zero_gamma": crossed_zero_gamma,
        "pin_is_contested": contested,
        "threshold_evidence": threshold_evidence,
        **confirmation,
        "prior_provenance": prior_provenance,
        "current_provenance": provenance,
    }
    next_level = _confirmed_level_state(
        value=value,
        provenance=provenance,
        observations=observations,
        status="alerted",
        prior_provenance=prior_provenance,
        last_event_id=event["event_id"],
        threshold_evidence=threshold_evidence,
    )
    return event, next_level, []


def _directional_event(
    observations: Sequence[Mapping[str, Any]],
    price: Sequence[Mapping[str, Any]],
    structure: Sequence[Mapping[str, Any]],
    latch: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, dict[str, Any], list[str], bool]:
    current = observations[-1]
    current_id = _observation_id(current)
    current_alignment = _alignment_provenance(current)
    raw_latch_alignment = latch.get("alignment_provenance")
    latch_alignment = (
        dict(raw_latch_alignment)
        if isinstance(raw_latch_alignment, Mapping)
        else None
    )
    alignment_matches = latch_alignment == current_alignment
    raw_pending = latch.get("pending_rearm_observations")
    pending_rearm = (
        [
            copy.deepcopy(dict(row))
            for row in raw_pending
            if isinstance(row, Mapping)
            and str(row.get("observation_id") or "").strip()
            and _valid_timestamp(row.get("observed_at_utc"))
            and isinstance(row.get("alignment_provenance"), Mapping)
            and dict(row["alignment_provenance"]) == current_alignment
        ]
        if isinstance(raw_pending, list) and alignment_matches
        else []
    )
    next_latch = copy.deepcopy(dict(latch))
    next_latch.update(
        {
            "active_direction": latch.get("active_direction"),
            "neutral_eligible_scan_count": len(pending_rearm),
            "pending_rearm_observations": pending_rearm,
            "last_processed_observation_id": latch.get(
                "last_processed_observation_id"
            ),
            "last_processed_alignment_provenance": copy.deepcopy(
                latch.get("last_processed_alignment_provenance")
            ),
            "last_event_id": latch.get("last_event_id"),
            "alignment_provenance": copy.deepcopy(current_alignment),
        }
    )
    suppressions: list[str] = []
    if latch_alignment is not None and not alignment_matches:
        suppressions.append("directional_alignment_provenance_rebaselined")
    price_directions = {str(item.get("direction")) for item in price}
    structure_directions = {str(item.get("direction")) for item in structure}
    all_directions = price_directions | structure_directions
    candidate = (
        next(iter(all_directions))
        if len(all_directions) == 1 and price_directions and structure_directions
        else None
    )
    if len(all_directions) > 1:
        suppressions.append("directional_conflict_between_confirmations")
    elif not price_directions:
        suppressions.append("directional_missing_price_confirmation")
    elif not structure_directions:
        suppressions.append("directional_missing_structure_confirmation")

    rearmed = False
    active = str(next_latch.get("active_direction") or "").lower() or None
    already_processed = bool(
        next_latch.get("last_processed_observation_id") == current_id
        and isinstance(
            next_latch.get("last_processed_alignment_provenance"), Mapping
        )
        and dict(next_latch["last_processed_alignment_provenance"])
        == current_alignment
    )
    if candidate is None:
        active_still_present = bool(active and active in all_directions)
        if active and not active_still_present and not already_processed:
            marker = _observation_marker(current)
            if all(
                row.get("observation_id") != marker["observation_id"]
                for row in pending_rearm
            ):
                pending_rearm.append(marker)
            next_latch["pending_rearm_observations"] = pending_rearm
            next_latch["neutral_eligible_scan_count"] = len(pending_rearm)
            if len(pending_rearm) >= 2:
                rearm_observations = pending_rearm[-2:]
                next_latch["active_direction"] = None
                next_latch["neutral_eligible_scan_count"] = 0
                next_latch["pending_rearm_observations"] = []
                next_latch["last_rearm_observation_ids"] = [
                    row["observation_id"] for row in rearm_observations
                ]
                next_latch["last_rearm_observed_at_utc"] = [
                    row["observed_at_utc"] for row in rearm_observations
                ]
                next_latch["last_rearm_observed_at_ct"] = [
                    row["observed_at_ct"] for row in rearm_observations
                ]
                next_latch["last_rearm_alignment_provenance"] = copy.deepcopy(
                    current_alignment
                )
                next_latch["last_rearmed_at_utc"] = rearm_observations[-1][
                    "observed_at_utc"
                ]
                next_latch["last_rearmed_at_ct"] = rearm_observations[-1][
                    "observed_at_ct"
                ]
                rearmed = True
        elif active_still_present:
            next_latch["neutral_eligible_scan_count"] = 0
            next_latch["pending_rearm_observations"] = []
        elif not active:
            next_latch["neutral_eligible_scan_count"] = 0
            next_latch["pending_rearm_observations"] = []
        next_latch["last_processed_observation_id"] = current_id
        next_latch["last_processed_alignment_provenance"] = copy.deepcopy(
            current_alignment
        )
        return None, next_latch, suppressions, rearmed

    if active == candidate:
        next_latch["neutral_eligible_scan_count"] = 0
        next_latch["pending_rearm_observations"] = []
        next_latch["last_processed_observation_id"] = current_id
        next_latch["last_processed_alignment_provenance"] = copy.deepcopy(
            current_alignment
        )
        suppressions.append("directional_same_direction_not_rearmed")
        return None, next_latch, suppressions, rearmed

    confirmation = _confirmation_metadata(observations)
    material = {
        "session_date": _session_date_ct(current),
        "direction": candidate,
        "alignment_provenance": current_alignment,
        "predecessor": _directional_transition_predecessor(
            latch, _session_date_ct(current), current_alignment
        ),
    }
    event = {
        "type": "DIRECTIONAL_SHIFT",
        "symbol": str(current.get("symbol")),
        "event_id": _event_id("DIRECTIONAL_SHIFT", str(current.get("symbol")), material),
        "event_identity_schema": "marketpin-monitor-event.v3",
        "predecessor": material["predecessor"],
        "direction": candidate,
        "alignment_provenance": copy.deepcopy(current_alignment),
        "price_confirmations": list(price),
        "structure_confirmations": list(structure),
        **confirmation,
        "analytical_estimate_only": True,
    }
    next_latch.update(
        {
            "active_direction": candidate,
            "neutral_eligible_scan_count": 0,
            "pending_rearm_observations": [],
            "last_processed_observation_id": current_id,
            "last_processed_alignment_provenance": copy.deepcopy(
                current_alignment
            ),
            "last_event_id": event["event_id"],
            "alignment_provenance": copy.deepcopy(current_alignment),
            "last_alert_confirmation_observation_ids": confirmation[
                "confirmation_observation_ids"
            ],
            "last_alert_confirmation_observed_at_utc": confirmation[
                "confirmation_observed_at_utc"
            ],
            "last_alert_confirmation_observed_at_ct": confirmation[
                "confirmation_observed_at_ct"
            ],
            "last_alert_confirmed_at_utc": confirmation["confirmed_at_utc"],
            "last_alert_confirmed_at_ct": confirmation["confirmed_at_ct"],
        }
    )
    return event, next_latch, suppressions, rearmed


_EVENT_TYPES = {
    "MAX_PAIN_CHANGE",
    "MAX_PAIN_PROVENANCE_RESET",
    "GAMMA_PIN_SHIFT",
    "DIRECTIONAL_SHIFT",
}
_CONFIRMATION_FIELDS = (
    "confirmation_observation_ids",
    "confirmation_observed_at_utc",
    "confirmation_observed_at_ct",
    "confirmed_at_utc",
    "confirmed_at_ct",
)


def _valid_aware_timestamp(value: Any) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _durable_event_receipts(
    payload: Mapping[str, Any],
) -> tuple[dict[str, Mapping[str, Any]], list[str]]:
    """Parse optional journal-backed helper events without performing I/O."""

    raw_receipts = payload.get("durable_event_receipts")
    if raw_receipts is None:
        return {}, []
    if not isinstance(raw_receipts, list):
        return {}, ["durable_event_receipts_must_be_an_array"]

    receipts: dict[str, Mapping[str, Any]] = {}
    issues: list[str] = []
    for index, raw_receipt in enumerate(raw_receipts):
        prefix = f"durable_event_receipts[{index}]"
        if not isinstance(raw_receipt, Mapping):
            issues.append(f"{prefix}.must_be_an_object")
            continue
        event_id = raw_receipt.get("event_id")
        if not isinstance(event_id, str) or re.fullmatch(r"[0-9a-f]{64}", event_id) is None:
            issues.append(f"{prefix}.event_id_must_be_canonical_sha256")
            continue
        if event_id in receipts:
            issues.append(f"{prefix}.event_id_must_be_unique")
            continue
        if raw_receipt.get("type") not in _EVENT_TYPES:
            issues.append(f"{prefix}.type_invalid")
        if raw_receipt.get("symbol") not in _MONITORED_SYMBOLS:
            issues.append(f"{prefix}.symbol_invalid")
        receipts[event_id] = raw_receipt
    return receipts, issues


def _receipt_confirmation_issues(
    receipt: Mapping[str, Any], prefix: str
) -> list[str]:
    issues: list[str] = []
    observation_ids = receipt.get("confirmation_observation_ids")
    utc_times = receipt.get("confirmation_observed_at_utc")
    ct_times = receipt.get("confirmation_observed_at_ct")
    if (
        not isinstance(observation_ids, list)
        or len(observation_ids) != 2
        or any(not isinstance(value, str) or not value for value in observation_ids)
        or len(set(observation_ids)) != 2
    ):
        issues.append(f"{prefix}.confirmation_observation_ids_invalid")
    if (
        not isinstance(utc_times, list)
        or len(utc_times) != 2
        or not all(_valid_timestamp(value) for value in utc_times)
    ):
        issues.append(f"{prefix}.confirmation_observed_at_utc_invalid")
    if (
        not isinstance(ct_times, list)
        or len(ct_times) != 2
        or not all(_valid_aware_timestamp(value) for value in ct_times)
    ):
        issues.append(f"{prefix}.confirmation_observed_at_ct_invalid")

    if not _valid_timestamp(receipt.get("confirmed_at_utc")):
        issues.append(f"{prefix}.confirmed_at_utc_invalid")
    if not _valid_aware_timestamp(receipt.get("confirmed_at_ct")):
        issues.append(f"{prefix}.confirmed_at_ct_invalid")

    if not issues:
        parsed_utc = [_parsed_timestamp(value) for value in utc_times]
        parsed_ct = [datetime.fromisoformat(value) for value in ct_times]
        if parsed_utc != sorted(parsed_utc) or len(set(parsed_utc)) != 2:
            issues.append(f"{prefix}.confirmation_times_must_be_chronological")
        if any(
            ct.astimezone(ZoneInfo("UTC")) != utc
            for ct, utc in zip(parsed_ct, parsed_utc)
        ):
            issues.append(f"{prefix}.confirmation_ct_utc_mismatch")
        canonical_ct = [
            utc.astimezone(_SESSION_TIMEZONE).isoformat() for utc in parsed_utc
        ]
        if ct_times != canonical_ct:
            issues.append(f"{prefix}.confirmation_ct_must_be_canonical_chicago_time")
        if receipt.get("confirmed_at_utc") != utc_times[-1]:
            issues.append(f"{prefix}.confirmed_at_utc_must_equal_last_confirmation")
        if receipt.get("confirmed_at_ct") != ct_times[-1]:
            issues.append(f"{prefix}.confirmed_at_ct_must_equal_last_confirmation")
    return issues


def _receipt_temporal_compatibility_issues(
    receipt: Mapping[str, Any],
    event: Mapping[str, Any],
    prefix: str,
    prior_watermark: datetime | None,
) -> list[str]:
    """Require durable evidence to precede the retry and agree where it overlaps."""

    issues: list[str] = []
    receipt_ids = receipt.get("confirmation_observation_ids")
    receipt_times = receipt.get("confirmation_observed_at_utc")
    event_ids = event.get("confirmation_observation_ids")
    event_times = event.get("confirmation_observed_at_utc")
    if not all(
        isinstance(values, list) and len(values) == 2
        for values in (receipt_ids, receipt_times, event_ids, event_times)
    ) or not all(
        _valid_timestamp(value)
        for values in (receipt_times, event_times)
        for value in values
    ):
        return issues

    receipt_pairs = dict(zip(receipt_ids, receipt_times))
    event_pairs = dict(zip(event_ids, event_times))
    for observation_id in receipt_pairs.keys() & event_pairs.keys():
        if receipt_pairs[observation_id] != event_pairs[observation_id]:
            issues.append(f"{prefix}.overlapping_observation_id_time_mismatch")

    receipt_by_time = dict(zip(receipt_times, receipt_ids))
    event_by_time = dict(zip(event_times, event_ids))
    for observed_at in receipt_by_time.keys() & event_by_time.keys():
        if receipt_by_time[observed_at] != event_by_time[observed_at]:
            issues.append(f"{prefix}.overlapping_observation_time_id_mismatch")

    receipt_last = _parsed_timestamp(str(receipt_times[-1]))
    event_last = _parsed_timestamp(str(event_times[-1]))
    if receipt_last > event_last:
        issues.append(f"{prefix}.confirmation_cannot_be_after_retry_observation")
    if prior_watermark is not None and receipt_last <= prior_watermark:
        issues.append(f"{prefix}.confirmation_not_after_policy_watermark")

    receipt_sessions = {
        _parsed_timestamp(str(value)).astimezone(_SESSION_TIMEZONE).date()
        for value in receipt_times
    }
    event_sessions = {
        _parsed_timestamp(str(value)).astimezone(_SESSION_TIMEZONE).date()
        for value in event_times
    }
    if len(receipt_sessions) != 1 or receipt_sessions != event_sessions:
        issues.append(f"{prefix}.confirmation_session_mismatch")
    return list(dict.fromkeys(issues))


def _canonical_optional_number(value: Any) -> bool:
    return value is None or (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and _finite(value) is not None
    )


def _receipt_level_provenance_issues(
    provenance: Any,
    level_name: str,
    symbol: str,
    prefix: str,
    field_name: str = "current_provenance",
) -> list[str]:
    """Require receipt provenance to be a complete canonical live provenance."""

    if not isinstance(provenance, Mapping):
        return [f"{prefix}.{field_name}_missing"]

    issues: list[str] = []
    required_fields = {
        "symbol",
        "provider",
        "subscription_generation",
        "subscription_epoch_id",
        "primary_expiration",
        "universe_sha256",
        "universe_is_fallback",
        "formula_version",
    }
    if level_name == "max_pain":
        required_fields.update({"source", "as_of"})
    if set(provenance) != required_fields:
        issues.append(f"{prefix}.{field_name}_fields_invalid")
    if provenance.get("symbol") != symbol:
        issues.append(f"{prefix}.{field_name}_symbol_mismatch")
    if provenance.get("provider") != "databento":
        issues.append(f"{prefix}.{field_name}_provider_invalid")
    generation = provenance.get("subscription_generation")
    if type(generation) is not int or generation <= 0:
        issues.append(f"{prefix}.{field_name}_generation_invalid")
    subscription_epoch_id = provenance.get("subscription_epoch_id")
    if (
        not isinstance(subscription_epoch_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None
    ):
        issues.append(f"{prefix}.{field_name}_epoch_invalid")
    expiration = provenance.get("primary_expiration")
    if not isinstance(expiration, str) or not expiration:
        issues.append(f"{prefix}.{field_name}_expiration_missing")
    universe = provenance.get("universe_sha256")
    if not isinstance(universe, str) or re.fullmatch(r"[0-9a-f]{64}", universe) is None:
        issues.append(f"{prefix}.{field_name}_universe_invalid")
    if provenance.get("universe_is_fallback") is not False:
        issues.append(f"{prefix}.{field_name}_fallback_invalid")
    formula = provenance.get("formula_version")
    if not isinstance(formula, str) or not formula:
        issues.append(f"{prefix}.{field_name}_formula_missing")

    if level_name == "max_pain":
        if provenance.get("source") not in _FULL_OI_SOURCES:
            issues.append(f"{prefix}.{field_name}_source_invalid")
        as_of = provenance.get("as_of")
        if not isinstance(as_of, str) or not as_of:
            issues.append(f"{prefix}.{field_name}_as_of_missing")
    return issues


def _receipt_session_date(receipt: Mapping[str, Any]) -> str | None:
    utc_times = receipt.get("confirmation_observed_at_utc")
    if (
        not isinstance(utc_times, list)
        or len(utc_times) != 2
        or not all(_valid_timestamp(value) for value in utc_times)
    ):
        return None
    return (
        _parsed_timestamp(str(utc_times[-1]))
        .astimezone(_SESSION_TIMEZONE)
        .date()
        .isoformat()
    )


def _gamma_threshold_receipt_issues(
    receipt: Mapping[str, Any], event: Mapping[str, Any], prefix: str
) -> list[str]:
    """Rebuild gamma significance evidence and require the receipt to match it."""

    threshold = receipt.get("threshold_evidence")
    if not isinstance(threshold, Mapping):
        return [f"{prefix}.threshold_evidence_missing"]

    spot_rows = threshold.get("spot_by_observation")
    zero_gamma_rows = threshold.get("zero_gamma_by_observation")
    top_strikes = threshold.get("top_strikes_by_observation")
    lead_ratios = threshold.get("pin_lead_ratios")
    contested_flags = threshold.get("pin_is_contested_by_observation")
    if (
        not isinstance(spot_rows, list)
        or len(spot_rows) != 2
        or not all(_canonical_optional_number(value) for value in spot_rows)
        or not isinstance(zero_gamma_rows, list)
        or len(zero_gamma_rows) != 2
        or not all(_canonical_optional_number(value) for value in zero_gamma_rows)
        or not isinstance(top_strikes, list)
        or len(top_strikes) != 2
        or not all(isinstance(row, list) for row in top_strikes)
        or not all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and _finite(value) is not None
            for row in top_strikes
            for value in row
        )
        or not isinstance(lead_ratios, list)
        or len(lead_ratios) != 2
        or not all(_canonical_optional_number(value) for value in lead_ratios)
        or not isinstance(contested_flags, list)
        or len(contested_flags) != 2
        or not all(isinstance(value, bool) for value in contested_flags)
    ):
        return [f"{prefix}.threshold_evidence_shape_invalid"]

    receipt_ids = receipt.get("confirmation_observation_ids")
    receipt_times = receipt.get("confirmation_observed_at_utc")
    prior_provenance = receipt.get("prior_provenance")
    current_provenance = receipt.get("current_provenance")
    predecessor = receipt.get("predecessor")
    if not (
        isinstance(receipt_ids, list)
        and len(receipt_ids) == 2
        and all(isinstance(value, str) and value for value in receipt_ids)
        and isinstance(receipt_times, list)
        and len(receipt_times) == 2
        and all(_valid_timestamp(value) for value in receipt_times)
        and isinstance(prior_provenance, Mapping)
        and isinstance(current_provenance, Mapping)
        and isinstance(predecessor, Mapping)
    ):
        return [f"{prefix}.threshold_evidence_context_invalid"]

    observations: list[dict[str, Any]] = []
    for index in range(2):
        observations.append(
            {
                "observation_id": receipt_ids[index],
                "observed_at_utc": receipt_times[index],
                "symbol": current_provenance.get("symbol"),
                "provider": current_provenance.get("provider"),
                "subscription_generation": current_provenance.get(
                    "subscription_generation"
                ),
                "subscription_epoch_id": current_provenance.get(
                    "subscription_epoch_id"
                ),
                "primary_expiration": current_provenance.get("primary_expiration"),
                "universe_sha256": current_provenance.get("universe_sha256"),
                "universe_is_fallback": current_provenance.get(
                    "universe_is_fallback"
                ),
                "gex_formula_version": current_provenance.get("formula_version"),
                "gamma_pin": receipt.get("new_value"),
                "spot": spot_rows[index],
                "zero_gamma": zero_gamma_rows[index],
                "top_strikes_by_abs_gex": [
                    {"strike": strike} for strike in top_strikes[index]
                ],
                "pin_lead_ratio": lead_ratios[index],
                "pin_is_contested": contested_flags[index],
            }
        )

    prior_level: dict[str, Any] = {
        "value": receipt.get("old_value"),
        "provenance": copy.deepcopy(dict(prior_provenance)),
    }
    if predecessor.get("kind") == "event":
        prior_level["last_event_id"] = predecessor.get("id")
    rebuilt, _, _ = _gamma_pin_event(
        observations, {"gamma_pin": prior_level}
    )
    if rebuilt is None:
        return [f"{prefix}.threshold_evidence_not_self_consistent"]
    if dict(threshold) != rebuilt.get("threshold_evidence"):
        return [f"{prefix}.threshold_evidence_not_self_consistent"]

    for field in (
        "threshold_points",
        "threshold_source",
        "median_top_strike_spacing",
        "crossed_spot",
        "crossed_zero_gamma",
        "pin_is_contested",
    ):
        if receipt.get(field) != threshold.get(field):
            return [f"{prefix}.{field}_must_match_threshold_evidence"]
    return []


def _directional_provenance_issues(
    provenance: Any, symbol: str, prefix: str
) -> list[str]:
    if not isinstance(provenance, Mapping):
        return [f"{prefix}.alignment_provenance_missing"]

    issues: list[str] = []
    if set(provenance) != set(_DIRECTIONAL_PROVENANCE_FIELDS):
        issues.append(f"{prefix}.alignment_provenance_fields_invalid")
    if provenance.get("symbol") != symbol:
        issues.append(f"{prefix}.alignment_provenance_symbol_mismatch")
    if provenance.get("provider") != "databento":
        issues.append(f"{prefix}.alignment_provenance_provider_invalid")
    generation = provenance.get("subscription_generation")
    if type(generation) is not int or generation <= 0:
        issues.append(f"{prefix}.alignment_provenance_generation_invalid")
    for field in ("subscription_epoch_id", "universe_sha256"):
        value = provenance.get(field)
        if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
            issues.append(f"{prefix}.alignment_provenance_{field}_invalid")
    for field in ("primary_expiration", "gex_formula_version"):
        value = provenance.get(field)
        if not isinstance(value, str) or not value:
            issues.append(f"{prefix}.alignment_provenance_{field}_missing")
    if provenance.get("universe_is_fallback") is not False:
        issues.append(f"{prefix}.alignment_provenance_fallback_invalid")
    return issues


def _directional_receipt_evidence_issues(
    receipt: Mapping[str, Any], prefix: str
) -> list[str]:
    direction = receipt.get("direction")
    confirmation_ids = receipt.get("confirmation_observation_ids")
    price = receipt.get("price_confirmations")
    structure = receipt.get("structure_confirmations")
    issues: list[str] = []
    issues.extend(
        _directional_provenance_issues(
            receipt.get("alignment_provenance"),
            str(receipt.get("symbol") or ""),
            prefix,
        )
    )
    if receipt.get("analytical_estimate_only") is not True:
        issues.append(f"{prefix}.analytical_estimate_only_must_be_true")
    allowed_price = {
        "forecast_bias_flip",
        "persistent_15m_price_move",
        "persistent_orb_breakout",
    }
    allowed_structure = {
        "confirmed_significant_gamma_pin_shift",
        "normalized_net_gex_regime_change",
        "spot_cross_zero_gamma",
        "spot_cross_positive_gex_wall",
        "spot_cross_negative_gex_wall",
        "classified_research_flow_threshold_cross",
    }
    for field, values, category, allowed in (
        ("price_confirmations", price, "price", allowed_price),
        ("structure_confirmations", structure, "structure", allowed_structure),
    ):
        if not isinstance(values, list) or not values:
            issues.append(f"{prefix}.{field}_must_be_nonempty")
            continue
        for index, row in enumerate(values):
            if not isinstance(row, Mapping):
                issues.append(f"{prefix}.{field}[{index}]_must_be_an_object")
                continue
            if (
                row.get("category") != category
                or row.get("type") not in allowed
                or row.get("direction") != direction
            ):
                issues.append(f"{prefix}.{field}[{index}]_invalid")
                continue
            evidence_type = row.get("type")
            if evidence_type == "forecast_bias_flip":
                transition_ids = row.get("transition_observation_ids")
                if (
                    row.get("previous_direction") not in _DIRECTIONS
                    or row.get("previous_direction") == direction
                    or transition_ids != confirmation_ids
                ):
                    issues.append(f"{prefix}.{field}[{index}]_incomplete")
            elif evidence_type == "confirmed_significant_gamma_pin_shift":
                gamma_event_id = row.get("event_id")
                if (
                    not isinstance(gamma_event_id, str)
                    or re.fullmatch(r"[0-9a-f]{64}", gamma_event_id) is None
                ):
                    issues.append(f"{prefix}.{field}[{index}]_incomplete")
            elif evidence_type == "normalized_net_gex_regime_change":
                previous_value = row.get("previous")
                current_value = row.get("current")
                previous_number = _finite(previous_value)
                current_number = _finite(current_value)
                exact_live_gate = bool(
                    previous_number is not None
                    and current_number is not None
                    and (
                        (
                            previous_number * current_number < 0
                            and min(abs(previous_number), abs(current_number)) >= 0.10
                        )
                        or abs(current_number - previous_number) >= 0.25
                    )
                )
                if (
                    not isinstance(previous_value, (int, float))
                    or isinstance(previous_value, bool)
                    or previous_number is None
                    or not isinstance(current_value, (int, float))
                    or isinstance(current_value, bool)
                    or current_number is None
                    or not exact_live_gate
                    or (
                        direction == "bullish"
                        and not current_number > previous_number
                    )
                    or (
                        direction == "bearish"
                        and not current_number < previous_number
                    )
                ):
                    issues.append(f"{prefix}.{field}[{index}]_incomplete")
    return issues


def _receipt_match_issues(
    receipt: Mapping[str, Any],
    event: Mapping[str, Any],
    prior_watermark: datetime | None,
) -> list[str]:
    prefix = f"durable_event_receipt:{event['event_id']}"
    issues = _receipt_confirmation_issues(receipt, prefix)
    for field in ("event_id", "type", "symbol"):
        if receipt.get(field) != event.get(field):
            issues.append(f"{prefix}.{field}_mismatch")
    if receipt.get("event_identity_schema") != "marketpin-monitor-event.v3":
        issues.append(f"{prefix}.event_identity_schema_mismatch")
    if receipt.get("predecessor") != event.get("predecessor"):
        issues.append(f"{prefix}.predecessor_mismatch")
    if not issues:
        issues.extend(
            _receipt_temporal_compatibility_issues(
                receipt, event, prefix, prior_watermark
            )
        )

    event_type = str(event.get("type") or "")
    if event_type in {"MAX_PAIN_CHANGE", "MAX_PAIN_PROVENANCE_RESET", "GAMMA_PIN_SHIFT"}:
        for field in ("old_value", "new_value"):
            if not _same_number(receipt.get(field), event.get(field)):
                issues.append(f"{prefix}.{field}_mismatch")
        if not _same_number(
            receipt.get("change_points"),
            float(event.get("new_value")) - float(event.get("old_value")),
        ):
            issues.append(f"{prefix}.change_points_mismatch")
        if event_type in {"MAX_PAIN_CHANGE", "MAX_PAIN_PROVENANCE_RESET"}:
            expected_pct = (
                (float(event["new_value"]) - float(event["old_value"]))
                / float(event["old_value"])
                * 100.0
                if event.get("old_value")
                else None
            )
            if expected_pct is None:
                if receipt.get("change_pct") is not None:
                    issues.append(f"{prefix}.change_pct_mismatch")
            elif not _same_number(receipt.get("change_pct"), expected_pct):
                issues.append(f"{prefix}.change_pct_mismatch")
            if receipt.get("provenance_reset") is not event_type.endswith(
                "PROVENANCE_RESET"
            ):
                issues.append(f"{prefix}.provenance_reset_mismatch")
        for field in ("prior_provenance", "current_provenance"):
            receipt_value = receipt.get(field)
            event_value = event.get(field)
            if isinstance(receipt_value, Mapping) and isinstance(event_value, Mapping):
                matches = dict(receipt_value) == dict(event_value)
            else:
                matches = receipt_value == event_value
            if not matches:
                issues.append(f"{prefix}.{field}_mismatch")
    elif event_type == "DIRECTIONAL_SHIFT":
        if receipt.get("direction") != event.get("direction"):
            issues.append(f"{prefix}.direction_mismatch")
        if receipt.get("alignment_provenance") != event.get(
            "alignment_provenance"
        ):
            issues.append(f"{prefix}.alignment_provenance_mismatch")
        issues.extend(_directional_receipt_evidence_issues(receipt, prefix))

    if event_type == "GAMMA_PIN_SHIFT" and not issues:
        issues.extend(_gamma_threshold_receipt_issues(receipt, event, prefix))
    return issues


def _expected_event_for_receipt(
    receipt: Mapping[str, Any],
    observations: Sequence[Mapping[str, Any]],
    level_state: Mapping[str, Any],
    latch: Mapping[str, Any],
) -> tuple[dict[str, Any] | None, str | None, list[str]]:
    """Reconstruct a receipt's semantic edge from the persisted pre-event state."""

    event_id = str(receipt.get("event_id") or "")
    prefix = f"durable_event_receipt:{event_id}"
    event_type = str(receipt.get("type") or "")
    current = observations[-1]
    symbol = str(current.get("symbol") or "")
    session_date = _receipt_session_date(receipt) or _session_date_ct(current)
    confirmation = _confirmation_metadata(observations)
    issues: list[str] = []

    if event_type in {"MAX_PAIN_CHANGE", "MAX_PAIN_PROVENANCE_RESET"}:
        level_name = "max_pain"
    elif event_type == "GAMMA_PIN_SHIFT":
        level_name = "gamma_pin"
    elif event_type == "DIRECTIONAL_SHIFT":
        direction = receipt.get("direction")
        if direction not in _DIRECTIONS:
            return None, None, [f"{prefix}.direction_invalid"]
        alignment_provenance_raw = receipt.get("alignment_provenance")
        issues.extend(
            _directional_provenance_issues(
                alignment_provenance_raw, symbol, prefix
            )
        )
        if not isinstance(alignment_provenance_raw, Mapping):
            return None, None, issues
        alignment_provenance = copy.deepcopy(dict(alignment_provenance_raw))
        active = str(latch.get("active_direction") or "").lower() or None
        if active == direction:
            issues.append(f"{prefix}.direction_was_already_latched")
        predecessor = _directional_transition_predecessor(
            latch, session_date, alignment_provenance
        )
        material = {
            "session_date": session_date,
            "direction": direction,
            "alignment_provenance": alignment_provenance,
            "predecessor": predecessor,
        }
        expected = {
            "type": "DIRECTIONAL_SHIFT",
            "symbol": symbol,
            "event_id": _event_id("DIRECTIONAL_SHIFT", symbol, material),
            "event_identity_schema": "marketpin-monitor-event.v3",
            "predecessor": predecessor,
            "direction": direction,
            "alignment_provenance": alignment_provenance,
            **confirmation,
        }
        return expected, None, issues
    else:
        return None, None, [f"{prefix}.type_invalid"]

    prior = level_state.get(level_name)
    if not isinstance(prior, Mapping) or _finite(prior.get("value")) is None:
        return None, level_name, [f"{prefix}.prior_level_state_missing"]
    prior_provenance = prior.get("provenance")
    if not isinstance(prior_provenance, Mapping):
        return None, level_name, [f"{prefix}.prior_level_provenance_missing"]
    issues.extend(
        _receipt_level_provenance_issues(
            prior_provenance,
            level_name,
            symbol,
            prefix,
            field_name="prior_provenance",
        )
    )

    raw_value = receipt.get("new_value")
    if (
        not isinstance(raw_value, (int, float))
        or isinstance(raw_value, bool)
        or (value := _finite(raw_value)) is None
    ):
        return None, level_name, [f"{prefix}.new_value_invalid"]
    current_provenance_raw = receipt.get("current_provenance")
    issues.extend(
        _receipt_level_provenance_issues(
            current_provenance_raw, level_name, symbol, prefix
        )
    )
    if not isinstance(current_provenance_raw, Mapping):
        return None, level_name, issues
    provenance_fields = (
        "symbol",
        "provider",
        "subscription_generation",
        "subscription_epoch_id",
        "primary_expiration",
        "universe_sha256",
        "universe_is_fallback",
        "formula_version",
    )
    if level_name == "max_pain":
        provenance_fields += ("source", "as_of")
    current_provenance = {
        field: copy.deepcopy(current_provenance_raw.get(field))
        for field in provenance_fields
    }

    prior_provenance_dict = dict(prior_provenance)
    same_provenance = prior_provenance_dict == current_provenance
    if level_name == "gamma_pin" and not same_provenance:
        issues.append(f"{prefix}.gamma_provenance_requires_silent_rebaseline")
        expected_type = "GAMMA_PIN_SHIFT"
    elif level_name == "gamma_pin":
        expected_type = "GAMMA_PIN_SHIFT"
    else:
        expected_type = (
            "MAX_PAIN_CHANGE" if same_provenance else "MAX_PAIN_PROVENANCE_RESET"
        )

    old_value = float(prior["value"])
    if _same_number(old_value, value):
        issues.append(f"{prefix}.level_value_unchanged_could_not_emit_event")
    predecessor = _level_transition_predecessor(level_name, prior)
    material = {
        "session_date": session_date,
        "old_value": old_value,
        "new_value": value,
        "prior_provenance": prior_provenance_dict,
        "current_provenance": current_provenance,
        "predecessor": predecessor,
    }
    expected = {
        "type": expected_type,
        "symbol": symbol,
        "event_id": _event_id(expected_type, symbol, material),
        "event_identity_schema": "marketpin-monitor-event.v3",
        "predecessor": predecessor,
        "old_value": old_value,
        "new_value": value,
        "change_points": value - old_value if value is not None else None,
        **confirmation,
        "prior_provenance": prior_provenance_dict,
        "current_provenance": current_provenance,
    }
    if level_name == "max_pain":
        expected.update(
            {
                "change_pct": (
                    ((value - old_value) / old_value * 100.0)
                    if old_value and value is not None
                    else None
                ),
                "provenance_reset": expected_type.endswith("PROVENANCE_RESET"),
            }
        )
    return expected, level_name, issues


def _hydrate_durable_event_receipts(
    observations: Sequence[Mapping[str, Any]],
    level_state: Mapping[str, Any],
    latch: Mapping[str, Any],
    receipts: Mapping[str, Mapping[str, Any]],
    prior_watermark: datetime | None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], list[str]]:
    """Chronologically validate and hydrate journaled edges before scanning."""

    original_levels = copy.deepcopy(dict(level_state))
    original_latch = copy.deepcopy(dict(latch))
    hydrated_levels = copy.deepcopy(original_levels)
    hydrated_latch = copy.deepcopy(original_latch)
    hydrated_events: list[dict[str, Any]] = []
    issues: list[str] = []
    last_confirmation_by_slot: dict[str, datetime] = {}
    event_order = {
        "MAX_PAIN_CHANGE": 0,
        "MAX_PAIN_PROVENANCE_RESET": 0,
        "GAMMA_PIN_SHIFT": 1,
        "DIRECTIONAL_SHIFT": 2,
    }

    def receipt_order(
        row: Mapping[str, Any],
    ) -> tuple[int, datetime, int, str]:
        confirmed_at = row.get("confirmed_at_utc")
        timestamp_valid = _valid_timestamp(confirmed_at)
        confirmed_time = (
            _parsed_timestamp(str(confirmed_at))
            if timestamp_valid
            else datetime.max.replace(tzinfo=ZoneInfo("UTC"))
        )
        return (
            0 if timestamp_valid else 1,
            confirmed_time,
            event_order.get(str(row.get("type") or ""), 99),
            str(row.get("event_id") or ""),
        )

    ordered_receipts = sorted(
        receipts.values(),
        key=receipt_order,
    )
    for receipt in ordered_receipts:
        event_type = str(receipt.get("type") or "")
        if event_type in {"MAX_PAIN_CHANGE", "MAX_PAIN_PROVENANCE_RESET"}:
            logical_slot = "max_pain"
        elif event_type == "GAMMA_PIN_SHIFT":
            logical_slot = "gamma_pin"
        else:
            logical_slot = "directional_latch"

        expected, level_name, semantic_issues = _expected_event_for_receipt(
            receipt, observations, hydrated_levels, hydrated_latch
        )
        receipt_issues = list(semantic_issues)
        if expected is None:
            issues.extend(receipt_issues)
            continue
        receipt_issues.extend(
            _receipt_match_issues(receipt, expected, prior_watermark)
        )
        confirmed_at = receipt.get("confirmed_at_utc")
        if _valid_timestamp(confirmed_at):
            confirmation_time = _parsed_timestamp(str(confirmed_at))
            previous_confirmation = last_confirmation_by_slot.get(logical_slot)
            if (
                previous_confirmation is not None
                and confirmation_time <= previous_confirmation
            ):
                receipt_issues.append(
                    f"durable_event_receipts.{logical_slot}_confirmation_must_advance"
                )
        else:
            confirmation_time = None
        if receipt_issues:
            issues.extend(receipt_issues)
            continue

        event = copy.deepcopy(dict(receipt))
        event["durable_receipt_applied"] = True
        if level_name is not None:
            level = {
                "value": expected["new_value"],
                "provenance": copy.deepcopy(expected["current_provenance"]),
                "prior_provenance": copy.deepcopy(expected["prior_provenance"]),
                "current_provenance": copy.deepcopy(expected["current_provenance"]),
                "status": "alerted",
                "last_event_id": event["event_id"],
            }
            for field in _CONFIRMATION_FIELDS:
                level[field] = copy.deepcopy(receipt[field])
            if event_type == "GAMMA_PIN_SHIFT":
                level["threshold_evidence"] = copy.deepcopy(
                    receipt["threshold_evidence"]
                )
            hydrated_levels[level_name] = level
        else:
            hydrated_latch.update(
                {
                    "active_direction": receipt["direction"],
                    "neutral_eligible_scan_count": 0,
                    "pending_rearm_observations": [],
                    "last_processed_observation_id": receipt[
                        "confirmation_observation_ids"
                    ][-1],
                    "last_processed_alignment_provenance": copy.deepcopy(
                        receipt["alignment_provenance"]
                    ),
                    "last_event_id": receipt["event_id"],
                    "alignment_provenance": copy.deepcopy(
                        receipt["alignment_provenance"]
                    ),
                    "last_alert_confirmation_observation_ids": copy.deepcopy(
                        receipt["confirmation_observation_ids"]
                    ),
                    "last_alert_confirmation_observed_at_utc": copy.deepcopy(
                        receipt["confirmation_observed_at_utc"]
                    ),
                    "last_alert_confirmation_observed_at_ct": copy.deepcopy(
                        receipt["confirmation_observed_at_ct"]
                    ),
                    "last_alert_confirmed_at_utc": receipt["confirmed_at_utc"],
                    "last_alert_confirmed_at_ct": receipt["confirmed_at_ct"],
                }
            )
        hydrated_events.append(event)
        if confirmation_time is not None:
            last_confirmation_by_slot[logical_slot] = confirmation_time
    if issues:
        return [], original_levels, original_latch, list(dict.fromkeys(issues))
    return hydrated_events, hydrated_levels, hydrated_latch, []


def evaluate_monitor_policy(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate normalized observations without performing any I/O.

    The caller must persist ``next_state`` only after it has appended the result
    to its own journal. Re-evaluating the same semantic transition from an
    unchanged prior state yields identical event IDs even if the confirmation
    pair has advanced. On an append-before-state-write retry, the caller may
    provide the exact previously journaled helper event in
    ``durable_event_receipts``; matching receipts restore the durable evidence
    into ``next_state`` and mismatches fail closed.
    """

    issues: list[str] = []
    durable_receipts, receipt_input_issues = _durable_event_receipts(payload)
    issues.extend(receipt_input_issues)
    if payload.get("schema_version") != INPUT_SCHEMA:
        issues.append(f"schema_version_must_equal:{INPUT_SCHEMA}")
    raw_observations = payload.get("observations")
    if not isinstance(raw_observations, list) or len(raw_observations) < 2:
        issues.append("at_least_two_consecutive_observations_required")
        observations: list[Mapping[str, Any]] = []
    else:
        observations = [row for row in raw_observations if isinstance(row, Mapping)]
        if len(observations) != len(raw_observations):
            issues.append("observations_must_be_objects")
    if observations:
        for index, observation in enumerate(observations[-2:], start=len(observations) - 2):
            issues.extend(_eligibility_issues(observation, index))
        issues.extend(_alignment_issues(observations[-2:]))
    (
        confirmation_cadence_seconds,
        confirmation_min_gap_seconds,
        confirmation_max_gap_seconds,
        confirmation_gap_seconds,
        confirmation_gap_issues,
    ) = _confirmation_gap_policy(payload, observations[-2:])
    issues.extend(confirmation_gap_issues)

    policy_state = payload.get("policy_state")
    state = copy.deepcopy(dict(policy_state)) if isinstance(policy_state, Mapping) else {}
    if not issues:
        issues.extend(_policy_temporal_issues(state, observations[-2:]))
    level_state = state.get("levels")
    level_state = copy.deepcopy(dict(level_state)) if isinstance(level_state, Mapping) else {}
    latch = state.get("directional_latch")
    latch = copy.deepcopy(dict(latch)) if isinstance(latch, Mapping) else {}
    result: dict[str, Any] = {
        "schema_version": OUTPUT_SCHEMA,
        "accepted": not issues,
        "issues": issues,
        "events": [],
        "suppressions": [],
        "confirmations": {"price": [], "structure": []},
        "directional_rearmed": False,
        "confirmation_cadence_seconds": confirmation_cadence_seconds,
        "confirmation_min_gap_seconds": confirmation_min_gap_seconds,
        "confirmation_gap_grace_seconds": _CONFIRMATION_GAP_GRACE_SECONDS,
        "confirmation_max_gap_seconds": confirmation_max_gap_seconds,
        "confirmation_gap_seconds": confirmation_gap_seconds,
        "session_date": None,
        "policy_state_reset_for_session_rollover": False,
        "prior_policy_session_date": state.get("session_date"),
        # A rejected evaluation must be a true no-op. Preserve the caller's
        # complete state (including its session date and extension fields)
        # rather than returning a partially normalized replacement.
        "next_state": copy.deepcopy(state),
    }
    if issues:
        return result

    session_date = _session_date_ct(observations[-1])
    prior_policy_session_date = state.get("session_date") or None
    current_session_date = _canonical_session_date(session_date)
    prior_session_date = _canonical_session_date(prior_policy_session_date)
    reset_for_rollover = bool(
        prior_session_date is not None
        and current_session_date is not None
        and prior_session_date < current_session_date
    )
    if reset_for_rollover:
        level_state = _rollover_level_state(level_state, prior_policy_session_date)
        latch = {}
    result.update(
        {
            "session_date": session_date,
            "policy_state_reset_for_session_rollover": reset_for_rollover,
            "prior_policy_session_date": prior_policy_session_date,
        }
    )

    prior_watermark, _ = _resolved_policy_watermark(state)
    recovered_events, level_state, latch, receipt_match_issues = (
        _hydrate_durable_event_receipts(
            observations,
            level_state,
            latch,
            durable_receipts,
            prior_watermark,
        )
    )
    if receipt_match_issues:
        issues.extend(receipt_match_issues)
        result.update(
            {
                "accepted": False,
                "issues": issues,
                "events": [],
                "next_state": copy.deepcopy(state),
            }
        )
        return result

    max_event, next_max, max_suppressions = _max_pain_event(observations, level_state)
    if next_max is not None:
        level_state["max_pain"] = next_max
    gamma_event, next_gamma, gamma_suppressions = _gamma_pin_event(observations, level_state)
    if next_gamma is not None:
        level_state["gamma_pin"] = next_gamma
    price = _price_confirmations(observations)
    structure = _structure_confirmations(observations, gamma_event)
    directional_event, next_latch, directional_suppressions, rearmed = _directional_event(
        observations, price, structure, latch
    )

    generated_events = [
        event for event in (max_event, gamma_event, directional_event) if event
    ]
    recovered_ids = {event["event_id"] for event in recovered_events}
    events = recovered_events + [
        event for event in generated_events if event["event_id"] not in recovered_ids
    ]

    accepted_next_state = copy.deepcopy(state)
    accepted_next_state.update(
        {
            "session_date": session_date,
            "last_accepted_observed_at_utc": observations[-1]["observed_at_utc"],
            "levels": level_state,
            "directional_latch": next_latch,
        }
    )
    result.update(
        {
            "events": events,
            "suppressions": list(
                dict.fromkeys(max_suppressions + gamma_suppressions + directional_suppressions)
            ),
            "confirmations": {"price": price, "structure": structure},
            "directional_rearmed": rearmed,
            "next_state": accepted_next_state,
        }
    )
    return result
