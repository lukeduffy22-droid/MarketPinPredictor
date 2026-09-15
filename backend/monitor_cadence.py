"""Deterministic adaptive cadence state for the MarketPin monitor.

The collector supplies explicit, hash-bound evidence for non-policy cadence
triggers.  Deterministic policy alerts are supplied by the scan-commit helper.
This module performs no I/O; the scan ledger owns locking, journaling, crash
recovery, and the atomic state replacement.
"""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


CADENCE_EVIDENCE_SCHEMA = "marketpin-monitor-cadence-evidence.v1"
CADENCE_DECISION_SCHEMA = "marketpin-monitor-cadence-decision.v1"
CADENCE_STATE_SCHEMA = "marketpin-monitor-cadence-state.v1"
CADENCE_TRIGGER_CALCULATION_VERSION = "marketpin-monitor-cadence-triggers.v1"
ELEVATED_MINIMUM_SECONDS = 30 * 60
ELEVATED_STABLE_SCANS_REQUIRED = 3
ELEVATED_SCAN_MIN_GAP_SECONDS = 4 * 60
# Allow bounded scheduler jitter, but do not let a missed five-minute wake count
# as a consecutive elevated scan.
ELEVATED_SCAN_MAX_GAP_SECONDS = 7 * 60

_CHICAGO = ZoneInfo("America/Chicago")
_UTC = ZoneInfo("UTC")
_MONITORED_SYMBOLS = ("SPX", "NDX")
_CADENCE_STATE_FIELDS = (
    "mode",
    "mode_reason",
    "elevated_since_ct",
    "elevated_minimum_until_ct",
    "stable_elevated_scan_count",
)
_CADENCE_STATE_SNAPSHOT_FIELDS = (
    "schema_version",
    "session_date",
    *_CADENCE_STATE_FIELDS,
)
_SYMBOL_TRIGGER_FIELDS = (
    "half_threshold_gamma_pin_move",
    "contested_pin_leadership",
    "spot_near_or_crossed_gamma_pin",
    "spot_near_or_crossed_zero_gamma",
    "spot_near_or_crossed_gex_wall",
    "normalized_net_gex_near_or_crossed_zero",
    "forecast_bias_awaiting_confirmation",
    "high_volatility_regime",
)
_POLICY_ALERT_TYPES = {
    "MAX_PAIN_CHANGE",
    "MAX_PAIN_PROVENANCE_RESET",
    "GAMMA_PIN_SHIFT",
    "DIRECTIONAL_SHIFT",
}
_TRANSITIONS = {
    "LEGACY_PRESERVE",
    "HOLD_NORMAL",
    "ENTER_ELEVATED",
    "EXTEND_ELEVATED",
    "HOLD_ELEVATED_MINIMUM",
    "HOLD_ELEVATED_INELIGIBLE",
    "HOLD_ELEVATED_DATA_QUALITY",
    "HOLD_ELEVATED_NONCONSECUTIVE",
    "COUNT_STABLE_ELEVATED_SCAN",
    "RETURN_NORMAL",
}


class MonitorCadenceError(ValueError):
    """A fail-closed cadence evidence or state error."""


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _parsed_aware(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise MonitorCadenceError(f"{field}_missing_or_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorCadenceError(f"{field}_missing_or_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MonitorCadenceError(f"{field}_must_be_timezone_aware")
    return parsed


def _parsed_ct(value: Any, field: str, *, session_date: str) -> datetime:
    parsed = _parsed_aware(value, field)
    chicago = parsed.astimezone(_CHICAGO)
    if parsed.utcoffset() != chicago.utcoffset():
        raise MonitorCadenceError(f"{field}_must_use_chicago_offset")
    if chicago.date().isoformat() != session_date:
        raise MonitorCadenceError(f"{field}_session_date_mismatch")
    return chicago


def cadence_state_snapshot(
    state: Mapping[str, Any], *, require_complete: bool
) -> dict[str, Any]:
    """Return the cadence-owned state subset and validate production state."""

    if not isinstance(state, Mapping):
        raise MonitorCadenceError("cadence_state_must_be_an_object")
    raw_session_date = state.get("session_date")
    try:
        parsed_session_date = date.fromisoformat(str(raw_session_date))
    except ValueError as exc:
        raise MonitorCadenceError("cadence_state_session_date_invalid") from exc
    if parsed_session_date.isoformat() != raw_session_date:
        raise MonitorCadenceError("cadence_state_session_date_invalid")
    snapshot = {
        "schema_version": CADENCE_STATE_SCHEMA,
        "session_date": raw_session_date,
        **{
            field: copy.deepcopy(state.get(field))
            for field in _CADENCE_STATE_FIELDS
        },
    }
    mode = snapshot["mode"]
    if mode not in {"NORMAL", "ELEVATED"}:
        raise MonitorCadenceError("cadence_state_mode_invalid")
    if not require_complete:
        return snapshot

    missing = [field for field in _CADENCE_STATE_FIELDS if field not in state]
    if missing:
        raise MonitorCadenceError(
            "cadence_state_fields_missing:" + ",".join(missing)
        )
    reason = snapshot["mode_reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise MonitorCadenceError("cadence_state_mode_reason_invalid")
    stable_count = snapshot["stable_elevated_scan_count"]
    if type(stable_count) is not int or stable_count < 0:
        raise MonitorCadenceError("cadence_state_stable_count_invalid")
    session_date = str(raw_session_date)

    if mode == "NORMAL":
        if (
            snapshot["elevated_since_ct"] is not None
            or snapshot["elevated_minimum_until_ct"] is not None
            or stable_count != 0
        ):
            raise MonitorCadenceError("cadence_state_normal_fields_invalid")
    else:
        since = _parsed_ct(
            snapshot["elevated_since_ct"],
            "cadence_state.elevated_since_ct",
            session_date=session_date,
        )
        minimum = _parsed_ct(
            snapshot["elevated_minimum_until_ct"],
            "cadence_state.elevated_minimum_until_ct",
            session_date=session_date,
        )
        if minimum < since:
            raise MonitorCadenceError("cadence_state_minimum_precedes_since")
        if stable_count >= ELEVATED_STABLE_SCANS_REQUIRED:
            raise MonitorCadenceError("cadence_state_stable_count_should_have_demoted")
    return snapshot


def cadence_state_sha256(state: Mapping[str, Any], *, require_complete: bool) -> str:
    return _canonical_hash(
        cadence_state_snapshot(state, require_complete=require_complete)
    )


def validate_cadence_evidence(value: Any) -> dict[str, Any]:
    """Validate the caller-controlled, complete non-policy trigger matrix."""

    if not isinstance(value, Mapping):
        raise MonitorCadenceError("cadence_evidence_must_be_an_object")
    evidence = copy.deepcopy(dict(value))
    if set(evidence) != {
        "schema_version",
        "trigger_calculation_version",
        "active_data_quality_event_ids",
        "new_data_quality_event_ids",
        "symbols",
    }:
        raise MonitorCadenceError("cadence_evidence_fields_invalid")
    if evidence.get("schema_version") != CADENCE_EVIDENCE_SCHEMA:
        raise MonitorCadenceError("cadence_evidence_schema_invalid")
    if (
        evidence.get("trigger_calculation_version")
        != CADENCE_TRIGGER_CALCULATION_VERSION
    ):
        raise MonitorCadenceError(
            "cadence_evidence_trigger_calculation_version_invalid"
        )
    for field in (
        "active_data_quality_event_ids",
        "new_data_quality_event_ids",
    ):
        event_ids = evidence.get(field)
        if (
            not isinstance(event_ids, list)
            or len(event_ids) > 16
            or any(
                not isinstance(event_id, str)
                or not event_id.strip()
                or len(event_id) > 256
                for event_id in event_ids
            )
            or len(set(event_ids)) != len(event_ids)
        ):
            raise MonitorCadenceError(
                f"cadence_{field}_invalid"
            )
        if event_ids != sorted(event_ids):
            raise MonitorCadenceError(f"cadence_{field}_must_be_sorted")
    if not set(evidence["new_data_quality_event_ids"]).issubset(
        set(evidence["active_data_quality_event_ids"])
    ):
        raise MonitorCadenceError(
            "cadence_new_data_quality_events_must_be_active"
        )
    symbols = evidence.get("symbols")
    if not isinstance(symbols, Mapping) or set(symbols) != set(_MONITORED_SYMBOLS):
        raise MonitorCadenceError("cadence_evidence_symbols_invalid")
    for symbol in _MONITORED_SYMBOLS:
        payload = symbols.get(symbol)
        if not isinstance(payload, Mapping) or set(payload) != set(
            _SYMBOL_TRIGGER_FIELDS
        ):
            raise MonitorCadenceError(
                f"cadence_evidence.{symbol}.trigger_fields_invalid"
            )
        for field in _SYMBOL_TRIGGER_FIELDS:
            if type(payload.get(field)) is not bool:
                raise MonitorCadenceError(
                    f"cadence_evidence.{symbol}.{field}_must_be_boolean"
                )
    _canonical_json_bytes(evidence)
    return evidence


def _explicit_trigger_reasons(evidence: Mapping[str, Any]) -> list[str]:
    reasons = [
        f"NEW_DATA_QUALITY_EVENT:{event_id}"
        for event_id in evidence["new_data_quality_event_ids"]
    ]
    for symbol in _MONITORED_SYMBOLS:
        payload = evidence["symbols"][symbol]
        for field in _SYMBOL_TRIGGER_FIELDS:
            if payload[field] is True:
                reasons.append(f"{symbol}:{field.upper()}")
    return reasons


def _policy_trigger_reasons(policy_events: Sequence[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for event in policy_events:
        if not isinstance(event, Mapping):
            raise MonitorCadenceError("cadence_policy_event_invalid")
        event_type = event.get("type")
        symbol = event.get("symbol")
        event_id = event.get("event_id")
        if event_type not in _POLICY_ALERT_TYPES:
            raise MonitorCadenceError("cadence_policy_event_type_invalid")
        if symbol not in _MONITORED_SYMBOLS:
            raise MonitorCadenceError("cadence_policy_event_symbol_invalid")
        if not isinstance(event_id, str) or not event_id:
            raise MonitorCadenceError("cadence_policy_event_id_invalid")
        reasons.append(f"POLICY_ALERT:{event_type}:{symbol}:{event_id}")
    return reasons


def _mode_reason(prefix: str, reasons: Sequence[str]) -> str:
    if not reasons:
        return prefix
    return prefix + ":" + ",".join(reasons)


def evaluate_cadence_transition(
    *,
    state: Mapping[str, Any],
    scan: Mapping[str, Any],
    policy_events: Sequence[Mapping[str, Any]],
    require_explicit_evidence: bool,
    previously_seen_data_quality_event_ids: Sequence[str] = (),
) -> dict[str, Any]:
    """Return a replayable cadence decision without performing any I/O."""

    cadence = scan.get("cadence")
    if not isinstance(cadence, Mapping):
        raise MonitorCadenceError("scan_cadence_must_be_an_object")
    raw_evidence = cadence.get("adaptive_evidence")
    if raw_evidence is None:
        if require_explicit_evidence:
            raise MonitorCadenceError("scan_cadence_adaptive_evidence_required")
        pre_state = cadence_state_snapshot(state, require_complete=False)
        decision = {
            "schema_version": CADENCE_DECISION_SCHEMA,
            "evidence_mode": "legacy_compatibility",
            "evidence_sha256": _canonical_hash(
                {
                    "schema_version": CADENCE_EVIDENCE_SCHEMA,
                    "trigger_calculation_version": (
                        CADENCE_TRIGGER_CALCULATION_VERSION
                    ),
                    "compatibility": "legacy_fixture_omitted",
                }
            ),
            "trigger_reasons": [],
            "active_data_quality_event_ids": [],
            "new_data_quality_event_ids": [],
            "fully_eligible": all(
                isinstance(scan.get("symbols", {}).get(symbol), Mapping)
                and scan["symbols"][symbol].get("eligible") is True
                for symbol in _MONITORED_SYMBOLS
            ),
            "prior_scan_gap_seconds": None,
            "elevated_five_minute_scan": False,
            "transition": "LEGACY_PRESERVE",
            "pre_state": pre_state,
            "next_state": copy.deepcopy(pre_state),
        }
        validate_cadence_decision(decision)
        return decision

    evidence = validate_cadence_evidence(raw_evidence)
    if isinstance(previously_seen_data_quality_event_ids, (str, bytes)) or any(
        not isinstance(event_id, str) or not event_id
        for event_id in previously_seen_data_quality_event_ids
    ):
        raise MonitorCadenceError(
            "cadence_previously_seen_data_quality_event_ids_invalid"
        )
    previously_seen_ids = set(previously_seen_data_quality_event_ids)
    if len(previously_seen_ids) != len(previously_seen_data_quality_event_ids):
        raise MonitorCadenceError(
            "cadence_previously_seen_data_quality_event_ids_invalid"
        )
    repeated_new_ids = sorted(
        set(evidence["new_data_quality_event_ids"]) & previously_seen_ids
    )
    if repeated_new_ids:
        raise MonitorCadenceError(
            "cadence_new_data_quality_event_already_seen:"
            + ",".join(repeated_new_ids)
        )
    expected_new_ids = sorted(
        set(evidence["active_data_quality_event_ids"]) - previously_seen_ids
    )
    if evidence["new_data_quality_event_ids"] != expected_new_ids:
        raise MonitorCadenceError(
            "cadence_new_data_quality_event_ids_not_derived_from_active"
        )
    pre_state = cadence_state_snapshot(state, require_complete=True)
    observed_ct = _parsed_ct(
        scan.get("observed_at_ct"),
        "scan.observed_at_ct",
        session_date=str(scan.get("session_date") or ""),
    )
    observed_utc = _parsed_aware(scan.get("observed_at_utc"), "scan.observed_at_utc")
    if observed_utc.astimezone(_UTC) != observed_ct.astimezone(_UTC):
        raise MonitorCadenceError("scan_cadence_observed_time_mismatch")

    symbols = scan.get("symbols")
    fully_eligible = bool(
        isinstance(symbols, Mapping)
        and all(
            isinstance(symbols.get(symbol), Mapping)
            and symbols[symbol].get("eligible") is True
            for symbol in _MONITORED_SYMBOLS
        )
    )
    assert isinstance(symbols, Mapping)
    for symbol in _MONITORED_SYMBOLS:
        symbol_eligible = (
            isinstance(symbols.get(symbol), Mapping)
            and symbols[symbol].get("eligible") is True
        )
        trigger_payload = evidence["symbols"][symbol]
        if not symbol_eligible and any(trigger_payload.values()):
            raise MonitorCadenceError(
                f"cadence_evidence.{symbol}.ineligible_symbol_trigger_claim"
            )
        if symbol_eligible:
            observation = symbols[symbol].get("policy_observation")
            if not isinstance(observation, Mapping):
                raise MonitorCadenceError(
                    f"cadence_evidence.{symbol}.policy_observation_missing"
                )
            contested = observation.get("pin_is_contested") is True
            lead_ratio = observation.get("pin_lead_ratio")
            weak_leader = (
                type(lead_ratio) in {int, float}
                and float(lead_ratio) <= 0.10
            )
            if (contested or weak_leader) and not trigger_payload[
                "contested_pin_leadership"
            ]:
                raise MonitorCadenceError(
                    f"cadence_evidence.{symbol}.contested_pin_trigger_omitted"
                )

    reasons = _policy_trigger_reasons(policy_events) + _explicit_trigger_reasons(
        evidence
    )
    reasons = list(dict.fromkeys(reasons))
    prior_scan_gap_seconds: float | None = None
    prior_scan_raw = state.get("last_scan_utc")
    if prior_scan_raw is not None:
        prior_scan = _parsed_aware(prior_scan_raw, "state.last_scan_utc").astimezone(
            _UTC
        )
        prior_scan_gap_seconds = (
            observed_utc.astimezone(_UTC) - prior_scan
        ).total_seconds()
    elevated_five_minute_scan = bool(
        cadence.get("mode") == "ELEVATED"
        and prior_scan_gap_seconds is not None
        and ELEVATED_SCAN_MIN_GAP_SECONDS
        <= prior_scan_gap_seconds
        <= ELEVATED_SCAN_MAX_GAP_SECONDS
    )

    next_state = copy.deepcopy(pre_state)
    transition: str
    if reasons:
        minimum = observed_ct + timedelta(seconds=ELEVATED_MINIMUM_SECONDS)
        if pre_state["mode"] == "NORMAL":
            next_state.update(
                {
                    "mode": "ELEVATED",
                    "mode_reason": _mode_reason("cadence_trigger", reasons),
                    "elevated_since_ct": observed_ct.isoformat(),
                    "elevated_minimum_until_ct": minimum.isoformat(),
                    "stable_elevated_scan_count": 0,
                }
            )
            transition = "ENTER_ELEVATED"
        else:
            current_minimum = _parsed_ct(
                pre_state["elevated_minimum_until_ct"],
                "cadence_state.elevated_minimum_until_ct",
                session_date=str(scan.get("session_date") or ""),
            )
            next_state.update(
                {
                    "mode_reason": _mode_reason("cadence_trigger", reasons),
                    "elevated_minimum_until_ct": max(
                        current_minimum, minimum
                    ).isoformat(),
                    "stable_elevated_scan_count": 0,
                }
            )
            transition = "EXTEND_ELEVATED"
    elif pre_state["mode"] == "NORMAL":
        next_state.update(
            {
                "elevated_since_ct": None,
                "elevated_minimum_until_ct": None,
                "stable_elevated_scan_count": 0,
            }
        )
        transition = "HOLD_NORMAL"
    else:
        minimum = _parsed_ct(
            pre_state["elevated_minimum_until_ct"],
            "cadence_state.elevated_minimum_until_ct",
            session_date=str(scan.get("session_date") or ""),
        )
        if observed_ct < minimum:
            next_state["stable_elevated_scan_count"] = 0
            transition = "HOLD_ELEVATED_MINIMUM"
        elif not fully_eligible:
            next_state["stable_elevated_scan_count"] = 0
            transition = "HOLD_ELEVATED_INELIGIBLE"
        elif evidence["active_data_quality_event_ids"]:
            next_state["stable_elevated_scan_count"] = 0
            transition = "HOLD_ELEVATED_DATA_QUALITY"
        elif not elevated_five_minute_scan:
            next_state["stable_elevated_scan_count"] = 0
            transition = "HOLD_ELEVATED_NONCONSECUTIVE"
        else:
            stable_count = int(pre_state["stable_elevated_scan_count"]) + 1
            if stable_count >= ELEVATED_STABLE_SCANS_REQUIRED:
                next_state.update(
                    {
                        "mode": "NORMAL",
                        "mode_reason": "three_stable_elevated_scans_after_minimum",
                        "elevated_since_ct": None,
                        "elevated_minimum_until_ct": None,
                        "stable_elevated_scan_count": 0,
                    }
                )
                transition = "RETURN_NORMAL"
            else:
                next_state.update(
                    {
                        "mode_reason": (
                            f"stable_elevated_scan_{stable_count}_of_"
                            f"{ELEVATED_STABLE_SCANS_REQUIRED}"
                        ),
                        "stable_elevated_scan_count": stable_count,
                    }
                )
                transition = "COUNT_STABLE_ELEVATED_SCAN"

    decision = {
        "schema_version": CADENCE_DECISION_SCHEMA,
        "evidence_mode": "explicit",
        "evidence_sha256": _canonical_hash(evidence),
        "trigger_reasons": reasons,
        "active_data_quality_event_ids": copy.deepcopy(
            evidence["active_data_quality_event_ids"]
        ),
        "new_data_quality_event_ids": copy.deepcopy(
            evidence["new_data_quality_event_ids"]
        ),
        "fully_eligible": fully_eligible,
        "prior_scan_gap_seconds": prior_scan_gap_seconds,
        "elevated_five_minute_scan": elevated_five_minute_scan,
        "transition": transition,
        "pre_state": pre_state,
        "next_state": next_state,
    }
    validate_cadence_decision(decision)
    return decision


def validate_cadence_decision(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise MonitorCadenceError("cadence_decision_must_be_an_object")
    decision = copy.deepcopy(dict(value))
    if set(decision) != {
        "schema_version",
        "evidence_mode",
        "evidence_sha256",
        "trigger_reasons",
        "active_data_quality_event_ids",
        "new_data_quality_event_ids",
        "fully_eligible",
        "prior_scan_gap_seconds",
        "elevated_five_minute_scan",
        "transition",
        "pre_state",
        "next_state",
    }:
        raise MonitorCadenceError("cadence_decision_fields_invalid")
    if decision.get("schema_version") != CADENCE_DECISION_SCHEMA:
        raise MonitorCadenceError("cadence_decision_schema_invalid")
    if decision.get("evidence_mode") not in {"explicit", "legacy_compatibility"}:
        raise MonitorCadenceError("cadence_decision_evidence_mode_invalid")
    evidence_hash = decision.get("evidence_sha256")
    if (
        not isinstance(evidence_hash, str)
        or len(evidence_hash) != 64
        or any(char not in "0123456789abcdef" for char in evidence_hash)
    ):
        raise MonitorCadenceError("cadence_decision_evidence_hash_invalid")
    reasons = decision.get("trigger_reasons")
    if (
        not isinstance(reasons, list)
        or any(not isinstance(reason, str) or not reason for reason in reasons)
        or len(set(reasons)) != len(reasons)
    ):
        raise MonitorCadenceError("cadence_decision_trigger_reasons_invalid")
    for field in (
        "active_data_quality_event_ids",
        "new_data_quality_event_ids",
    ):
        quality_ids = decision.get(field)
        if (
            not isinstance(quality_ids, list)
            or len(quality_ids) > 16
            or any(
                not isinstance(event_id, str)
                or not event_id
                or len(event_id) > 256
                for event_id in quality_ids
            )
            or len(set(quality_ids)) != len(quality_ids)
            or quality_ids != sorted(quality_ids)
        ):
            raise MonitorCadenceError(f"cadence_decision_{field}_invalid")
    if not set(decision["new_data_quality_event_ids"]).issubset(
        set(decision["active_data_quality_event_ids"])
    ):
        raise MonitorCadenceError(
            "cadence_decision_new_data_quality_events_must_be_active"
        )
    if type(decision.get("fully_eligible")) is not bool:
        raise MonitorCadenceError("cadence_decision_fully_eligible_invalid")
    gap = decision.get("prior_scan_gap_seconds")
    if gap is not None and (type(gap) not in {int, float} or gap <= 0):
        raise MonitorCadenceError("cadence_decision_prior_scan_gap_invalid")
    if type(decision.get("elevated_five_minute_scan")) is not bool:
        raise MonitorCadenceError("cadence_decision_five_minute_flag_invalid")
    if decision.get("transition") not in _TRANSITIONS:
        raise MonitorCadenceError("cadence_decision_transition_invalid")
    for field in ("pre_state", "next_state"):
        snapshot = decision.get(field)
        if not isinstance(snapshot, Mapping) or set(snapshot) != set(
            _CADENCE_STATE_SNAPSHOT_FIELDS
        ):
            raise MonitorCadenceError(f"cadence_decision_{field}_invalid")
    _canonical_json_bytes(decision)
    return decision


def cadence_decision_state_hashes(
    decision: Mapping[str, Any],
) -> tuple[str, str]:
    validated = validate_cadence_decision(decision)
    return (
        _canonical_hash(validated["pre_state"]),
        _canonical_hash(validated["next_state"]),
    )


def apply_cadence_decision(
    state: Mapping[str, Any], decision: Mapping[str, Any]
) -> dict[str, Any]:
    """Apply only cadence-owned fields; legacy fixtures remain byte-semantic."""

    validated = validate_cadence_decision(decision)
    result = copy.deepcopy(dict(state))
    if validated["evidence_mode"] == "legacy_compatibility":
        return result
    for field in _CADENCE_STATE_FIELDS:
        result[field] = copy.deepcopy(validated["next_state"][field])
    return result


__all__ = [
    "CADENCE_DECISION_SCHEMA",
    "CADENCE_EVIDENCE_SCHEMA",
    "CADENCE_STATE_SCHEMA",
    "CADENCE_TRIGGER_CALCULATION_VERSION",
    "ELEVATED_MINIMUM_SECONDS",
    "ELEVATED_SCAN_MAX_GAP_SECONDS",
    "ELEVATED_SCAN_MIN_GAP_SECONDS",
    "ELEVATED_STABLE_SCANS_REQUIRED",
    "MonitorCadenceError",
    "apply_cadence_decision",
    "cadence_decision_state_hashes",
    "cadence_state_sha256",
    "cadence_state_snapshot",
    "evaluate_cadence_transition",
    "validate_cadence_decision",
    "validate_cadence_evidence",
]
