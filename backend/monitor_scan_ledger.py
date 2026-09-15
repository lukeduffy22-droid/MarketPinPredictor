"""Durable commit boundary for MarketPin substantive monitor scans.

The market-data collector remains read-only.  This module accepts an already
normalized stable-schema scan, evaluates only symbols the collector marked
eligible, appends the scan and complete helper-event receipts durably, and only
then replaces monitor state.  An append-before-state crash is recovered from
the exact helper events already present in the journal.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import tempfile
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.monitor_cadence import (
    MonitorCadenceError,
    apply_cadence_decision,
    cadence_decision_state_hashes,
    cadence_state_sha256,
    cadence_state_snapshot,
    evaluate_cadence_transition,
    validate_cadence_decision,
    validate_cadence_evidence,
)
from backend.monitor_policy import (
    INPUT_SCHEMA as POLICY_INPUT_SCHEMA,
    OUTPUT_SCHEMA as POLICY_OUTPUT_SCHEMA,
    evaluate_monitor_policy,
    policy_observation_issues,
)


RESULT_SCHEMA = "marketpin-monitor-scan-commit.result.v1"
SCAN_EVENT_SCHEMA = "marketpin-monitor-substantive-scan.v1"
COMMIT_RECEIPT_EVENT_SCHEMA = "marketpin-monitor-substantive-scan-commit.v2"
COMMIT_RECEIPT_EVENT_TYPE = "substantive_scan_commit"
NOTIFICATION_ACK_EVENT_SCHEMA = "marketpin-monitor-policy-notification-ack.v1"
NOTIFICATION_ACK_EVENT_TYPE = "policy_notification_ack"
SCAN_WRAPPER_SCHEMA = 2

_CHICAGO = ZoneInfo("America/Chicago")
_UTC = ZoneInfo("UTC")
_MONITORED_SYMBOLS = ("SPX", "NDX")
_POLICY_EVENT_TYPES = {
    "MAX_PAIN_CHANGE",
    "MAX_PAIN_PROVENANCE_RESET",
    "GAMMA_PIN_SHIFT",
    "DIRECTIONAL_SHIFT",
}
_MONITOR_TRANSACTION_EVENT_TYPES = {
    "substantive_scan",
    COMMIT_RECEIPT_EVENT_TYPE,
    NOTIFICATION_ACK_EVENT_TYPE,
    *_POLICY_EVENT_TYPES,
}
_CADENCE_SECONDS_BY_MODE = {"NORMAL": 15 * 60, "ELEVATED": 5 * 60}
_CONFIRMATION_MIN_GAP_SECONDS = 4 * 60
_CONFIRMATION_MAX_GAP_SECONDS_BY_MODE = {
    "NORMAL": 20 * 60,
    "ELEVATED": 10 * 60,
}
_CONFIRMATION_ALIGNMENT_FIELDS = (
    "symbol",
    "provider",
    "subscription_generation",
    "subscription_epoch_id",
    "primary_expiration",
    "gex_formula_version",
    "universe_sha256",
    "universe_is_fallback",
)
_SCAN_POLICY_OBSERVATION_FIELDS = (
    "observation_id",
    "observed_at_utc",
    "symbol",
    "eligible",
    "provider",
    "subscription_generation",
    "subscription_epoch_id",
    "primary_expiration",
    "gex_formula_version",
    "universe_sha256",
    "universe_is_fallback",
    "validation_is_valid",
    "gamma_excluded_from_model",
    "spot",
    "gamma_pin",
    "pin_is_contested",
    "pin_lead_ratio",
    "top_strikes_by_abs_gex",
    "max_pain",
    "max_pain_source",
    "max_pain_formula_version",
    "max_pain_as_of",
    "zero_gamma",
    "positive_gex_wall",
    "negative_gex_wall",
    "normalized_net_gex",
)
_REQUIRED_WRAPPER_FIELDS = {
    "schema_version",
    "event_id",
    "event_type",
    "observed_at_ct",
    "observed_at_utc",
    "session_date",
    "phase",
    "cadence",
    "evidence",
    "symbols",
    "alerts",
    "directional_interpretation",
    "research_hypotheses",
}
_COMMIT_RECEIPT_FIELDS = {
    "schema_version",
    "event_id",
    "event_type",
    "session_date",
    "observed_at_ct",
    "observed_at_utc",
    "phase",
    "cadence",
    "evidence",
    "symbols",
    "alerts",
    "directional_interpretation",
    "research_hypotheses",
    "parent_scan_event_id",
    "ordered_policy_event_ids",
    "ordered_policy_records",
    "pre_policy_evaluator_sha256",
    "next_policy_evaluator_sha256",
    "pre_cadence_state_sha256",
    "next_cadence_state_sha256",
    "cadence_decision",
    "predecessor",
}
_NOTIFICATION_ACK_FIELDS = _REQUIRED_WRAPPER_FIELDS | {
    "acked_policy_events",
    "delivery_proof",
    "pending_notification_event_ids_before",
    "pending_notification_event_ids_after",
    "previous_ack",
    "scan_commit_anchor",
}


class MonitorScanLedgerError(RuntimeError):
    """A fail-closed scan validation, journal, or state error."""


def _canonical_json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _canonical_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()


def _policy_event_hash(event_type: str, symbol: str, material: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        {
            "event_schema": "marketpin-monitor-event.v3",
            "event_type": event_type,
            "symbol": symbol,
            "material": material,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _commit_receipt_event_id(receipt: Mapping[str, Any]) -> str:
    material = copy.deepcopy(dict(receipt))
    material.pop("event_id", None)
    return _canonical_hash(
        {
            "event_schema": COMMIT_RECEIPT_EVENT_SCHEMA,
            "receipt": material,
        }
    )


def _build_commit_receipt(
    *,
    scan: Mapping[str, Any],
    ordered_policy_event_ids: Sequence[str],
    ordered_policy_records: Sequence[Mapping[str, Any]],
    pre_policy_evaluator_sha256: str,
    next_policy_evaluator_sha256: str,
    cadence_decision: Mapping[str, Any],
    predecessor: Mapping[str, Any],
) -> dict[str, Any]:
    pre_cadence_sha256, next_cadence_sha256 = cadence_decision_state_hashes(
        cadence_decision
    )
    receipt = {
        "schema_version": SCAN_WRAPPER_SCHEMA,
        "event_type": COMMIT_RECEIPT_EVENT_TYPE,
        "session_date": scan["session_date"],
        "observed_at_ct": scan["observed_at_ct"],
        "observed_at_utc": scan["observed_at_utc"],
        "phase": scan["phase"],
        "cadence": copy.deepcopy(scan["cadence"]),
        "evidence": copy.deepcopy(scan["evidence"]),
        "symbols": {},
        "alerts": [],
        "directional_interpretation": "POLICY_COMMIT_RECEIPT",
        "research_hypotheses": [],
        "parent_scan_event_id": scan["event_id"],
        "ordered_policy_event_ids": list(ordered_policy_event_ids),
        "ordered_policy_records": [
            copy.deepcopy(dict(record)) for record in ordered_policy_records
        ],
        "pre_policy_evaluator_sha256": pre_policy_evaluator_sha256,
        "next_policy_evaluator_sha256": next_policy_evaluator_sha256,
        "pre_cadence_state_sha256": pre_cadence_sha256,
        "next_cadence_state_sha256": next_cadence_sha256,
        "cadence_decision": copy.deepcopy(dict(cadence_decision)),
        "predecessor": copy.deepcopy(dict(predecessor)),
    }
    receipt["event_id"] = _commit_receipt_event_id(receipt)
    return receipt


def _validate_commit_receipt(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise MonitorScanLedgerError("commit_receipt_must_be_an_object")
    receipt = copy.deepcopy(dict(record))
    if set(receipt) != _COMMIT_RECEIPT_FIELDS:
        raise MonitorScanLedgerError("commit_receipt_fields_invalid")
    if receipt.get("schema_version") != SCAN_WRAPPER_SCHEMA:
        raise MonitorScanLedgerError("commit_receipt_schema_version_invalid")
    if receipt.get("event_type") != COMMIT_RECEIPT_EVENT_TYPE:
        raise MonitorScanLedgerError("commit_receipt_event_type_invalid")
    _validate_common_wrapper(receipt)
    if receipt.get("alerts") != []:
        raise MonitorScanLedgerError("commit_receipt_alerts_must_be_empty")
    if receipt.get("symbols") != {}:
        raise MonitorScanLedgerError("commit_receipt_symbols_must_be_empty")
    if receipt.get("directional_interpretation") != "POLICY_COMMIT_RECEIPT":
        raise MonitorScanLedgerError("commit_receipt_directional_interpretation_invalid")
    if receipt.get("research_hypotheses") != []:
        raise MonitorScanLedgerError("commit_receipt_research_hypotheses_must_be_empty")
    if not _is_sha256(receipt.get("parent_scan_event_id")):
        raise MonitorScanLedgerError("commit_receipt_parent_scan_event_id_invalid")
    ordered_ids = receipt.get("ordered_policy_event_ids")
    if (
        not isinstance(ordered_ids, list)
        or any(not _is_sha256(event_id) for event_id in ordered_ids)
        or len(set(ordered_ids)) != len(ordered_ids)
    ):
        raise MonitorScanLedgerError("commit_receipt_policy_event_ids_invalid")
    ordered_records = receipt.get("ordered_policy_records")
    if not isinstance(ordered_records, list) or len(ordered_records) != len(
        ordered_ids
    ):
        raise MonitorScanLedgerError("commit_receipt_policy_records_invalid")
    committed_ids: list[str] = []
    for commitment in ordered_records:
        if not isinstance(commitment, Mapping) or set(commitment) != {
            "event_id",
            "record_sha256",
        }:
            raise MonitorScanLedgerError("commit_receipt_policy_record_invalid")
        if not _is_sha256(commitment.get("event_id")) or not _is_sha256(
            commitment.get("record_sha256")
        ):
            raise MonitorScanLedgerError("commit_receipt_policy_record_invalid")
        committed_ids.append(str(commitment["event_id"]))
    if committed_ids != ordered_ids:
        raise MonitorScanLedgerError("commit_receipt_policy_record_order_mismatch")
    for field in (
        "pre_policy_evaluator_sha256",
        "next_policy_evaluator_sha256",
        "pre_cadence_state_sha256",
        "next_cadence_state_sha256",
    ):
        if not _is_sha256(receipt.get(field)):
            raise MonitorScanLedgerError(f"commit_receipt_{field}_invalid")
    try:
        cadence_decision = validate_cadence_decision(
            receipt.get("cadence_decision")
        )
        expected_pre_cadence, expected_next_cadence = (
            cadence_decision_state_hashes(cadence_decision)
        )
    except MonitorCadenceError as exc:
        raise MonitorScanLedgerError(f"commit_receipt_cadence_decision_invalid:{exc}") from exc
    if receipt.get("pre_cadence_state_sha256") != expected_pre_cadence:
        raise MonitorScanLedgerError("commit_receipt_pre_cadence_state_hash_mismatch")
    if receipt.get("next_cadence_state_sha256") != expected_next_cadence:
        raise MonitorScanLedgerError("commit_receipt_next_cadence_state_hash_mismatch")
    predecessor = receipt.get("predecessor")
    if not isinstance(predecessor, Mapping) or set(predecessor) != {
        "kind",
        "event_id",
        "record_sha256",
    }:
        raise MonitorScanLedgerError("commit_receipt_predecessor_invalid")
    kind = predecessor.get("kind")
    predecessor_id = predecessor.get("event_id")
    predecessor_hash = predecessor.get("record_sha256")
    if kind in {COMMIT_RECEIPT_EVENT_TYPE, "session_rollover"}:
        if not isinstance(predecessor_id, str) or not predecessor_id:
            raise MonitorScanLedgerError("commit_receipt_predecessor_event_id_invalid")
    elif kind == "legacy_session_start":
        if predecessor_id is not None:
            raise MonitorScanLedgerError("commit_receipt_legacy_predecessor_id_invalid")
    else:
        raise MonitorScanLedgerError("commit_receipt_predecessor_kind_invalid")
    if not _is_sha256(predecessor_hash):
        raise MonitorScanLedgerError("commit_receipt_predecessor_hash_invalid")
    if not _is_sha256(receipt.get("event_id")):
        raise MonitorScanLedgerError("commit_receipt_event_id_invalid")
    if receipt["event_id"] != _commit_receipt_event_id(receipt):
        raise MonitorScanLedgerError("commit_receipt_event_id_mismatch")
    _canonical_json_bytes(receipt)
    return receipt


def _notification_ack_event_id(receipt: Mapping[str, Any]) -> str:
    material = copy.deepcopy(dict(receipt))
    material.pop("event_id", None)
    return _canonical_hash(
        {
            "event_schema": NOTIFICATION_ACK_EVENT_SCHEMA,
            "ack": material,
        }
    )


def _validate_notification_delivery_proof(proof: Any) -> dict[str, Any]:
    if not isinstance(proof, Mapping) or set(proof) != {
        "proof_type",
        "conversation_history_sha256",
        "prior_final_delivered_at_utc",
    }:
        raise MonitorScanLedgerError("notification_ack_delivery_proof_invalid")
    normalized = copy.deepcopy(dict(proof))
    if normalized.get("proof_type") != "prior_heartbeat_final_delivered":
        raise MonitorScanLedgerError("notification_ack_delivery_proof_type_invalid")
    if not _is_sha256(normalized.get("conversation_history_sha256")):
        raise MonitorScanLedgerError("notification_ack_history_sha256_invalid")
    delivered = _parsed_aware(
        normalized.get("prior_final_delivered_at_utc"),
        "notification_ack.prior_final_delivered_at_utc",
    )
    if delivered.utcoffset() != _UTC.utcoffset(delivered):
        raise MonitorScanLedgerError(
            "notification_ack_prior_final_delivered_at_must_be_utc"
        )
    _canonical_json_bytes(normalized)
    return normalized


def _build_notification_ack(
    *,
    session_date: str,
    observed_at_utc: datetime,
    state_mode: str,
    acked_policy_events: Sequence[Mapping[str, Any]],
    delivery_proof: Mapping[str, Any],
    pending_before: Sequence[str],
    pending_after: Sequence[str],
    previous_ack: Mapping[str, Any] | None,
    scan_commit_anchor: Mapping[str, Any],
) -> dict[str, Any]:
    observed_utc = observed_at_utc.astimezone(_UTC)
    observed_utc_text = observed_utc.isoformat().replace("+00:00", "Z")
    proof = _validate_notification_delivery_proof(delivery_proof)
    receipt = {
        "schema_version": SCAN_WRAPPER_SCHEMA,
        "event_type": NOTIFICATION_ACK_EVENT_TYPE,
        "observed_at_ct": observed_utc.astimezone(_CHICAGO).isoformat(),
        "observed_at_utc": observed_utc_text,
        "session_date": session_date,
        "phase": "notification_ack",
        "cadence": {
            "mode": state_mode,
            "substantive": False,
            "reason": "prior_heartbeat_delivery_proven",
        },
        "evidence": {"delivery_proof_sha256": _canonical_hash(proof)},
        "symbols": {},
        "alerts": [],
        "directional_interpretation": "NOTIFICATION_DELIVERY_ACK",
        "research_hypotheses": [],
        "acked_policy_events": [
            copy.deepcopy(dict(commitment)) for commitment in acked_policy_events
        ],
        "delivery_proof": proof,
        "pending_notification_event_ids_before": list(pending_before),
        "pending_notification_event_ids_after": list(pending_after),
        "previous_ack": (
            copy.deepcopy(dict(previous_ack)) if previous_ack is not None else None
        ),
        "scan_commit_anchor": copy.deepcopy(dict(scan_commit_anchor)),
    }
    receipt["event_id"] = _notification_ack_event_id(receipt)
    return receipt


def _validate_notification_ack(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping):
        raise MonitorScanLedgerError("notification_ack_must_be_an_object")
    receipt = copy.deepcopy(dict(record))
    if set(receipt) != _NOTIFICATION_ACK_FIELDS:
        raise MonitorScanLedgerError("notification_ack_fields_invalid")
    _validate_common_wrapper(receipt, allow_observed_after_session=True)
    if receipt.get("event_type") != NOTIFICATION_ACK_EVENT_TYPE:
        raise MonitorScanLedgerError("notification_ack_event_type_invalid")
    if receipt.get("phase") != "notification_ack":
        raise MonitorScanLedgerError("notification_ack_phase_invalid")
    cadence = receipt.get("cadence")
    if (
        not isinstance(cadence, Mapping)
        or cadence.get("mode") not in _CADENCE_SECONDS_BY_MODE
        or cadence.get("substantive") is not False
        or cadence.get("reason") != "prior_heartbeat_delivery_proven"
    ):
        raise MonitorScanLedgerError("notification_ack_cadence_invalid")
    if receipt.get("symbols") != {} or receipt.get("alerts") != []:
        raise MonitorScanLedgerError("notification_ack_must_not_carry_signals")
    if receipt.get("directional_interpretation") != "NOTIFICATION_DELIVERY_ACK":
        raise MonitorScanLedgerError(
            "notification_ack_directional_interpretation_invalid"
        )
    if receipt.get("research_hypotheses") != []:
        raise MonitorScanLedgerError(
            "notification_ack_research_hypotheses_must_be_empty"
        )
    proof = _validate_notification_delivery_proof(receipt.get("delivery_proof"))
    if receipt.get("evidence") != {
        "delivery_proof_sha256": _canonical_hash(proof)
    }:
        raise MonitorScanLedgerError("notification_ack_delivery_proof_hash_mismatch")
    delivered = _parsed_aware(
        proof["prior_final_delivered_at_utc"],
        "notification_ack.prior_final_delivered_at_utc",
    ).astimezone(_UTC)
    acknowledged = _parsed_aware(
        receipt.get("observed_at_utc"), "notification_ack.observed_at_utc"
    ).astimezone(_UTC)
    if delivered >= acknowledged:
        raise MonitorScanLedgerError("notification_ack_precedes_delivery_proof")

    before = receipt.get("pending_notification_event_ids_before")
    after = receipt.get("pending_notification_event_ids_after")
    if (
        not isinstance(before, list)
        or not isinstance(after, list)
        or any(not _is_sha256(event_id) for event_id in before + after)
        or len(set(before)) != len(before)
        or len(set(after)) != len(after)
    ):
        raise MonitorScanLedgerError("notification_ack_pending_ids_invalid")
    acked = receipt.get("acked_policy_events")
    if not isinstance(acked, list) or not acked:
        raise MonitorScanLedgerError("notification_ack_policy_events_invalid")
    acked_ids: list[str] = []
    for commitment in acked:
        if not isinstance(commitment, Mapping) or set(commitment) != {
            "event_id",
            "record_sha256",
            "parent_scan_event_id",
            "commit_receipt_event_id",
            "commit_receipt_sha256",
        }:
            raise MonitorScanLedgerError(
                "notification_ack_policy_event_commitment_invalid"
            )
        if any(
            not _is_sha256(commitment.get(field))
            for field in (
                "event_id",
                "record_sha256",
                "parent_scan_event_id",
                "commit_receipt_event_id",
                "commit_receipt_sha256",
            )
        ):
            raise MonitorScanLedgerError(
                "notification_ack_policy_event_commitment_invalid"
            )
        acked_ids.append(str(commitment["event_id"]))
    if len(set(acked_ids)) != len(acked_ids):
        raise MonitorScanLedgerError("notification_ack_duplicate_policy_event_id")
    if acked_ids != [event_id for event_id in before if event_id in set(acked_ids)]:
        raise MonitorScanLedgerError("notification_ack_policy_event_order_invalid")
    if after != [event_id for event_id in before if event_id not in set(acked_ids)]:
        raise MonitorScanLedgerError("notification_ack_pending_transition_invalid")

    for field in ("scan_commit_anchor",):
        anchor = receipt.get(field)
        if not isinstance(anchor, Mapping) or set(anchor) != {
            "event_id",
            "record_sha256",
        } or not all(_is_sha256(anchor.get(key)) for key in anchor):
            raise MonitorScanLedgerError("notification_ack_scan_anchor_invalid")
    previous = receipt.get("previous_ack")
    if previous is not None and (
        not isinstance(previous, Mapping)
        or set(previous) != {"event_id", "record_sha256"}
        or not all(_is_sha256(previous.get(key)) for key in previous)
    ):
        raise MonitorScanLedgerError("notification_ack_previous_ack_invalid")
    if not _is_sha256(receipt.get("event_id")):
        raise MonitorScanLedgerError("notification_ack_event_id_invalid")
    if receipt["event_id"] != _notification_ack_event_id(receipt):
        raise MonitorScanLedgerError("notification_ack_event_id_mismatch")
    _canonical_json_bytes(receipt)
    return receipt


def _validate_notification_ack_causal_order(
    record: Mapping[str, Any],
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    latest_commit: Mapping[str, Any],
) -> None:
    """Prove that delivery and acknowledgement follow their durable evidence."""

    receipt = _validate_notification_ack(record)
    delivered_at = _parsed_aware(
        receipt["delivery_proof"]["prior_final_delivered_at_utc"],
        "notification_ack.prior_final_delivered_at_utc",
    ).astimezone(_UTC)
    acknowledged_at = _parsed_aware(
        receipt["observed_at_utc"], "notification_ack.observed_at_utc"
    ).astimezone(_UTC)

    scan_anchor = receipt["scan_commit_anchor"]
    if (
        scan_anchor.get("event_id") != latest_commit.get("event_id")
        or scan_anchor.get("record_sha256") != _canonical_hash(latest_commit)
    ):
        raise MonitorScanLedgerError("notification_ack_scan_anchor_mismatch")
    latest_parent_id = str(latest_commit.get("parent_scan_event_id") or "")
    latest_parent = by_id.get(latest_parent_id)
    if not isinstance(latest_parent, Mapping):
        raise MonitorScanLedgerError("notification_ack_scan_anchor_mismatch")
    latest_scan_at = _parsed_aware(
        latest_parent.get("observed_at_utc"),
        "notification_ack.scan_anchor_observed_at_utc",
    ).astimezone(_UTC)
    if acknowledged_at <= latest_scan_at:
        raise MonitorScanLedgerError(
            "notification_ack_must_follow_scan_commit_anchor"
        )

    for commitment in receipt["acked_policy_events"]:
        event_id = str(commitment["event_id"])
        wrapper = by_id.get(event_id)
        parent = (
            by_id.get(str(wrapper.get("parent_scan_event_id") or ""))
            if isinstance(wrapper, Mapping)
            else None
        )
        if (
            not isinstance(wrapper, Mapping)
            or not isinstance(wrapper.get("helper_event"), Mapping)
            or not isinstance(parent, Mapping)
        ):
            raise MonitorScanLedgerError(
                "notification_ack_policy_commitment_mismatch:" + event_id
            )
        helper_confirmed_at = _parsed_aware(
            wrapper["helper_event"].get("confirmed_at_utc"),
            "notification_ack.helper_confirmed_at_utc",
        ).astimezone(_UTC)
        if delivered_at < helper_confirmed_at:
            raise MonitorScanLedgerError(
                "notification_ack_delivery_precedes_policy_wrapper:" + event_id
            )
        parent_scan_at = _parsed_aware(
            parent.get("observed_at_utc"),
            "notification_ack.parent_scan_observed_at_utc",
        ).astimezone(_UTC)
        if delivered_at < parent_scan_at:
            raise MonitorScanLedgerError(
                "notification_ack_delivery_precedes_parent_scan:" + event_id
            )


def scan_event_id(scan: Mapping[str, Any]) -> str:
    """Return the canonical identity for a stable substantive-scan wrapper."""

    material = copy.deepcopy(dict(scan))
    material.pop("event_id", None)
    return _canonical_hash(
        {
            "event_schema": SCAN_EVENT_SCHEMA,
            "scan": material,
        }
    )


def with_scan_event_id(scan: Mapping[str, Any]) -> dict[str, Any]:
    """Copy a scan and populate its canonical event ID when absent."""

    prepared = copy.deepcopy(dict(scan))
    if not prepared.get("event_id"):
        prepared["event_id"] = scan_event_id(prepared)
    return prepared


def _sanitized_policy_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise MonitorScanLedgerError("policy_input_must_be_an_object")
    sanitized = copy.deepcopy(dict(payload))
    sanitized.pop("policy_state", None)
    sanitized.pop("durable_event_receipts", None)
    return sanitized


def policy_input_sha256(payload: Mapping[str, Any]) -> str:
    """Hash the complete caller-controlled policy evidence payload."""

    return _canonical_hash(_sanitized_policy_input(payload))


def with_policy_input_hashes(
    scan: Mapping[str, Any],
    policy_inputs: Mapping[str, Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Copy a scan and fill missing eligible-symbol policy commitments."""

    prepared = copy.deepcopy(dict(scan))
    symbols = prepared.get("symbols")
    if not isinstance(symbols, Mapping) or not isinstance(policy_inputs, Mapping):
        return prepared
    for symbol in _MONITORED_SYMBOLS:
        symbol_payload = symbols.get(symbol)
        policy_payload = policy_inputs.get(symbol)
        if (
            isinstance(symbol_payload, dict)
            and symbol_payload.get("eligible") is True
            and "policy_input_sha256" not in symbol_payload
            and isinstance(policy_payload, Mapping)
        ):
            symbol_payload["policy_input_sha256"] = policy_input_sha256(
                policy_payload
            )
    return prepared


def _parsed_aware(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise MonitorScanLedgerError(f"{field}_missing_or_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorScanLedgerError(f"{field}_missing_or_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MonitorScanLedgerError(f"{field}_must_be_timezone_aware")
    return parsed


def _canonical_session_date(value: Any) -> str:
    if not isinstance(value, str):
        raise MonitorScanLedgerError("session_date_invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise MonitorScanLedgerError("session_date_invalid") from exc
    if parsed.isoformat() != value:
        raise MonitorScanLedgerError("session_date_not_canonical")
    return value


def _validate_future_profiles(symbol_payload: Mapping[str, Any]) -> None:
    policy_observation = symbol_payload.get("policy_observation")
    candidate_observation = symbol_payload.get("candidate_policy_observation")
    primary = symbol_payload.get("primary_expiration")
    if primary is None and isinstance(policy_observation, Mapping):
        primary = policy_observation.get("primary_expiration")
    if primary is None and isinstance(candidate_observation, Mapping):
        primary = candidate_observation.get("primary_expiration")
    for field in ("future_expiration_profiles", "shadow_expiration_profiles"):
        if field not in symbol_payload:
            continue
        profiles = symbol_payload[field]
        if not isinstance(profiles, list):
            raise MonitorScanLedgerError(f"symbols.{field}_must_be_an_array")
        for index, raw_profile in enumerate(profiles):
            prefix = f"symbols.{field}[{index}]"
            if not isinstance(raw_profile, Mapping):
                raise MonitorScanLedgerError(f"{prefix}_must_be_an_object")
            expiration = raw_profile.get("expiration")
            try:
                canonical_expiration = date.fromisoformat(str(expiration)).isoformat()
            except ValueError as exc:
                raise MonitorScanLedgerError(f"{prefix}.expiration_invalid") from exc
            if canonical_expiration != expiration:
                raise MonitorScanLedgerError(f"{prefix}.expiration_not_canonical")
            if primary and expiration == primary:
                raise MonitorScanLedgerError(f"{prefix}.must_not_repeat_primary_expiration")
            context_only = raw_profile.get("context_only") is True
            context_role = str(raw_profile.get("role") or "") in {
                "shadow_context_only",
                "future_expiration_context_only",
            }
            if not (context_only or context_role):
                raise MonitorScanLedgerError(f"{prefix}.context_only_required")


def _validate_candidate_policy_observation(
    *,
    symbol: str,
    symbol_payload: Mapping[str, Any],
    scan_observed_at: datetime,
    scan_session_date: str,
    scan_cadence_mode: str,
) -> dict[str, Any] | None:
    raw_candidate = symbol_payload.get("candidate_policy_observation")
    if raw_candidate is None:
        return None
    if not isinstance(raw_candidate, Mapping):
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation_must_be_an_object"
        )
    candidate = copy.deepcopy(dict(raw_candidate))
    issues = policy_observation_issues(
        candidate,
        prefix=f"symbols.{symbol}.candidate_policy_observation",
    )
    if issues:
        raise MonitorScanLedgerError(",".join(issues))
    if candidate.get("symbol") != symbol:
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation_symbol_mismatch"
        )
    observed = _parsed_aware(
        candidate.get("observed_at_utc"),
        f"symbols.{symbol}.candidate_policy_observation.observed_at_utc",
    )
    if observed.utcoffset() != _UTC.utcoffset(observed):
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation.observed_at_utc_must_be_utc"
        )
    observed_utc = observed.astimezone(_UTC)
    scan_utc = scan_observed_at.astimezone(_UTC)
    if observed_utc > scan_utc:
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation_after_scan"
        )
    if observed_utc.astimezone(_CHICAGO).date().isoformat() != scan_session_date:
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation_session_mismatch"
        )
    if (scan_utc - observed_utc).total_seconds() > _CADENCE_SECONDS_BY_MODE[
        scan_cadence_mode
    ]:
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.candidate_policy_observation_stale_for_scan_cadence"
        )
    return candidate


def _validate_common_wrapper(
    record: Mapping[str, Any], *, allow_observed_after_session: bool = False
) -> None:
    missing = sorted(_REQUIRED_WRAPPER_FIELDS - set(record))
    if missing:
        raise MonitorScanLedgerError(
            "wrapper_required_fields_missing:" + ",".join(missing)
        )
    if record.get("schema_version") != SCAN_WRAPPER_SCHEMA:
        raise MonitorScanLedgerError("wrapper_schema_version_must_equal_2")
    event_id = record.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        raise MonitorScanLedgerError("wrapper_event_id_missing")
    session_date = _canonical_session_date(record.get("session_date"))
    observed_utc = _parsed_aware(record.get("observed_at_utc"), "observed_at_utc")
    observed_ct = _parsed_aware(record.get("observed_at_ct"), "observed_at_ct")
    if observed_utc.utcoffset() != _UTC.utcoffset(observed_utc):
        raise MonitorScanLedgerError("observed_at_utc_must_be_utc")
    chicago_observed = observed_ct.astimezone(_CHICAGO)
    if observed_ct.utcoffset() != chicago_observed.utcoffset():
        raise MonitorScanLedgerError("observed_at_ct_must_use_chicago_offset")
    if observed_ct.astimezone(_UTC) != observed_utc.astimezone(_UTC):
        raise MonitorScanLedgerError("observed_at_ct_utc_mismatch")
    observed_session_date = chicago_observed.date().isoformat()
    if observed_session_date != session_date:
        if not allow_observed_after_session or observed_session_date < session_date:
            raise MonitorScanLedgerError("observed_at_ct_session_date_mismatch")
    if not isinstance(record.get("phase"), str) or not record["phase"].strip():
        raise MonitorScanLedgerError("wrapper_phase_invalid")
    if not isinstance(record.get("cadence"), Mapping):
        raise MonitorScanLedgerError("wrapper_cadence_must_be_an_object")
    if not isinstance(record.get("evidence"), (Mapping, list)):
        raise MonitorScanLedgerError("wrapper_evidence_must_be_an_object_or_array")
    if not isinstance(record.get("symbols"), Mapping):
        raise MonitorScanLedgerError("wrapper_symbols_must_be_an_object")
    if not isinstance(record.get("alerts"), list):
        raise MonitorScanLedgerError("wrapper_alerts_must_be_an_array")
    if not isinstance(record.get("directional_interpretation"), str):
        raise MonitorScanLedgerError("wrapper_directional_interpretation_invalid")
    if not isinstance(record.get("research_hypotheses"), list):
        raise MonitorScanLedgerError("wrapper_research_hypotheses_must_be_an_array")
    # This also rejects NaN/Infinity and values that cannot be durably encoded.
    _canonical_json_bytes(record)


def validate_scan_wrapper(scan: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and return an isolated stable-schema substantive scan."""

    if not isinstance(scan, Mapping):
        raise MonitorScanLedgerError("scan_must_be_an_object")
    normalized = copy.deepcopy(dict(scan))
    _validate_common_wrapper(normalized)
    if normalized.get("event_type") != "substantive_scan":
        raise MonitorScanLedgerError("scan_event_type_must_equal_substantive_scan")
    if not re.fullmatch(r"[0-9a-f]{64}", str(normalized.get("event_id") or "")):
        raise MonitorScanLedgerError("scan_event_id_must_be_canonical_sha256")
    if normalized["event_id"] != scan_event_id(normalized):
        raise MonitorScanLedgerError("scan_event_id_mismatch")
    if normalized["alerts"]:
        raise MonitorScanLedgerError("scan_alerts_must_be_empty")
    normalized["cadence"] = copy.deepcopy(dict(normalized["cadence"]))
    cadence_mode = normalized["cadence"].get("mode")
    if cadence_mode not in _CADENCE_SECONDS_BY_MODE:
        raise MonitorScanLedgerError("scan_cadence_mode_invalid")
    if normalized["cadence"].get("substantive") is not True:
        raise MonitorScanLedgerError("scan_cadence_substantive_must_be_true")
    if "adaptive_evidence" in normalized["cadence"]:
        try:
            normalized["cadence"]["adaptive_evidence"] = (
                validate_cadence_evidence(
                    normalized["cadence"]["adaptive_evidence"]
                )
            )
        except MonitorCadenceError as exc:
            raise MonitorScanLedgerError(str(exc)) from exc
    scan_observed_at = _parsed_aware(
        normalized["observed_at_utc"], "observed_at_utc"
    )
    scan_session_date = str(normalized["session_date"])
    for symbol in _MONITORED_SYMBOLS:
        if symbol not in normalized["symbols"]:
            raise MonitorScanLedgerError(f"symbols.{symbol}_required")
    eligible_monitored_symbols: list[str] = []
    for raw_symbol, raw_payload in normalized["symbols"].items():
        if not isinstance(raw_symbol, str) or not isinstance(raw_payload, Mapping):
            raise MonitorScanLedgerError("scan_symbol_payload_invalid")
        if "eligible" not in raw_payload or type(raw_payload.get("eligible")) is not bool:
            raise MonitorScanLedgerError(
                f"symbols.{raw_symbol}.eligible_must_be_boolean"
            )
        candidate = _validate_candidate_policy_observation(
            symbol=raw_symbol,
            symbol_payload=raw_payload,
            scan_observed_at=scan_observed_at,
            scan_session_date=scan_session_date,
            scan_cadence_mode=str(cadence_mode),
        )
        prior_confirmation_scan_event_id = raw_payload.get(
            "prior_confirmation_scan_event_id"
        )
        if prior_confirmation_scan_event_id is not None and not _is_sha256(
            prior_confirmation_scan_event_id
        ):
            raise MonitorScanLedgerError(
                f"symbols.{raw_symbol}.prior_confirmation_scan_event_id_invalid"
            )
        eligibility_reasons = raw_payload.get("eligibility_reasons")
        if candidate is not None and raw_payload["eligible"] is False:
            if (
                not isinstance(eligibility_reasons, list)
                or "confirmation_history_seeding" not in eligibility_reasons
            ):
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.candidate_requires_confirmation_history_seeding"
                )
            if prior_confirmation_scan_event_id is not None:
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.seed_must_not_claim_prior_confirmation_scan"
                )
        if (
            candidate is None
            and isinstance(eligibility_reasons, list)
            and "confirmation_history_seeding" in eligibility_reasons
        ):
            raise MonitorScanLedgerError(
                f"symbols.{raw_symbol}.confirmation_history_seed_candidate_required"
            )
        if raw_symbol in _MONITORED_SYMBOLS and raw_payload["eligible"] is True:
            eligible_monitored_symbols.append(raw_symbol)
            policy_observation = raw_payload.get("policy_observation")
            if not isinstance(policy_observation, Mapping):
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.policy_observation_required_when_eligible"
                )
            if policy_observation.get("symbol") != raw_symbol:
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.policy_observation_symbol_mismatch"
                )
            if policy_observation.get("eligible") is not True:
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.policy_observation_must_be_eligible"
                )
            if candidate is not None and _canonical_json_bytes(
                policy_observation
            ) != _canonical_json_bytes(candidate):
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.candidate_policy_observation_mismatch"
                )
            policy_hash = raw_payload.get("policy_input_sha256")
            if (
                not isinstance(policy_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", policy_hash) is None
            ):
                raise MonitorScanLedgerError(
                    f"symbols.{raw_symbol}.policy_input_sha256_invalid"
                )
            for field in _SCAN_POLICY_OBSERVATION_FIELDS:
                if field in raw_payload and raw_payload[field] != policy_observation.get(
                    field
                ):
                    raise MonitorScanLedgerError(
                        f"symbols.{raw_symbol}.{field}_policy_observation_mismatch"
                    )
        _validate_future_profiles(raw_payload)
    if (
        not eligible_monitored_symbols
        and normalized["directional_interpretation"] != "ABSTAIN"
    ):
        raise MonitorScanLedgerError(
            "diagnostic_scan_directional_interpretation_must_equal_ABSTAIN"
        )
    if (
        eligible_monitored_symbols
        and normalized["directional_interpretation"] != "POLICY_CONTROLLED"
    ):
        raise MonitorScanLedgerError(
            "eligible_scan_directional_interpretation_must_equal_POLICY_CONTROLLED"
        )
    return normalized


def _validate_helper_event(event: Mapping[str, Any]) -> None:
    if not isinstance(event, Mapping):
        raise MonitorScanLedgerError("helper_event_must_be_an_object")
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or re.fullmatch(r"[0-9a-f]{64}", event_id) is None:
        raise MonitorScanLedgerError("helper_event_id_must_be_canonical_sha256")
    if event.get("type") not in _POLICY_EVENT_TYPES:
        raise MonitorScanLedgerError("helper_event_type_invalid")
    if event.get("symbol") not in _MONITORED_SYMBOLS:
        raise MonitorScanLedgerError("helper_event_symbol_invalid")
    if event.get("event_identity_schema") != "marketpin-monitor-event.v3":
        raise MonitorScanLedgerError("helper_event_identity_schema_invalid")
    observation_ids = event.get("confirmation_observation_ids")
    utc_times = event.get("confirmation_observed_at_utc")
    ct_times = event.get("confirmation_observed_at_ct")
    if (
        not isinstance(observation_ids, list)
        or len(observation_ids) != 2
        or len(set(str(value) for value in observation_ids)) != 2
        or any(not isinstance(value, str) or not value for value in observation_ids)
    ):
        raise MonitorScanLedgerError("helper_event_confirmation_ids_invalid")
    if not isinstance(utc_times, list) or len(utc_times) != 2:
        raise MonitorScanLedgerError("helper_event_confirmation_utc_invalid")
    if not isinstance(ct_times, list) or len(ct_times) != 2:
        raise MonitorScanLedgerError("helper_event_confirmation_ct_invalid")
    parsed_utc = [
        _parsed_aware(value, "helper_event.confirmation_observed_at_utc").astimezone(
            _UTC
        )
        for value in utc_times
    ]
    parsed_ct = [
        _parsed_aware(value, "helper_event.confirmation_observed_at_ct")
        for value in ct_times
    ]
    if parsed_utc != sorted(parsed_utc) or len(set(parsed_utc)) != 2:
        raise MonitorScanLedgerError("helper_event_confirmation_times_not_chronological")
    for utc_value, ct_value in zip(parsed_utc, parsed_ct):
        chicago_value = utc_value.astimezone(_CHICAGO)
        if ct_value.astimezone(_UTC) != utc_value:
            raise MonitorScanLedgerError("helper_event_confirmation_ct_utc_mismatch")
        if ct_value.utcoffset() != chicago_value.utcoffset():
            raise MonitorScanLedgerError("helper_event_confirmation_ct_offset_invalid")
    if event.get("confirmed_at_utc") != utc_times[-1]:
        raise MonitorScanLedgerError("helper_event_confirmed_utc_mismatch")
    if event.get("confirmed_at_ct") != ct_times[-1]:
        raise MonitorScanLedgerError("helper_event_confirmed_ct_mismatch")
    _canonical_json_bytes(event)


def _expected_helper_event_id(
    event: Mapping[str, Any], session_date: str
) -> str:
    event_type = str(event.get("type") or "")
    symbol = str(event.get("symbol") or "")
    if event_type in {
        "MAX_PAIN_CHANGE",
        "MAX_PAIN_PROVENANCE_RESET",
        "GAMMA_PIN_SHIFT",
    }:
        material = {
            "session_date": session_date,
            "old_value": event.get("old_value"),
            "new_value": event.get("new_value"),
            "prior_provenance": event.get("prior_provenance"),
            "current_provenance": event.get("current_provenance"),
            "predecessor": event.get("predecessor"),
        }
    else:
        material = {
            "session_date": session_date,
            "direction": event.get("direction"),
            "alignment_provenance": event.get("alignment_provenance"),
            "predecessor": event.get("predecessor"),
        }
    return _policy_event_hash(event_type, symbol, material)


def _validate_alert_wrapper(record: Mapping[str, Any]) -> dict[str, Any]:
    _validate_common_wrapper(record)
    helper_event = record.get("helper_event")
    if not isinstance(helper_event, Mapping):
        raise MonitorScanLedgerError("policy_alert_wrapper_helper_event_missing")
    _validate_helper_event(helper_event)
    if record.get("event_id") != helper_event.get("event_id"):
        raise MonitorScanLedgerError("policy_alert_wrapper_event_id_mismatch")
    if record.get("event_type") != helper_event.get("type"):
        raise MonitorScanLedgerError("policy_alert_wrapper_event_type_mismatch")
    if record.get("alerts") != [helper_event]:
        raise MonitorScanLedgerError("policy_alert_wrapper_alerts_mismatch")
    symbol = helper_event["symbol"]
    if symbol not in record.get("symbols", {}):
        raise MonitorScanLedgerError("policy_alert_wrapper_symbol_missing")
    wrapper_symbol_payload = record["symbols"][symbol]
    if not isinstance(wrapper_symbol_payload, Mapping):
        raise MonitorScanLedgerError("policy_alert_wrapper_symbol_payload_invalid")
    if wrapper_symbol_payload.get("eligible") is not True:
        raise MonitorScanLedgerError("policy_alert_wrapper_symbol_must_be_eligible")
    if record.get("observed_at_utc") != helper_event.get("confirmed_at_utc"):
        raise MonitorScanLedgerError("policy_alert_wrapper_utc_mismatch")
    if record.get("observed_at_ct") != helper_event.get("confirmed_at_ct"):
        raise MonitorScanLedgerError("policy_alert_wrapper_ct_mismatch")
    if helper_event.get("event_id") != _expected_helper_event_id(
        helper_event, str(record.get("session_date") or "")
    ):
        raise MonitorScanLedgerError("helper_event_semantic_event_id_mismatch")
    parent_scan_event_id = record.get("parent_scan_event_id")
    if (
        not isinstance(parent_scan_event_id, str)
        or re.fullmatch(r"[0-9a-f]{64}", parent_scan_event_id) is None
    ):
        raise MonitorScanLedgerError("policy_alert_wrapper_parent_scan_id_invalid")
    return copy.deepcopy(dict(record))


def _validate_alert_parent(
    wrapper: Mapping[str, Any], by_id: Mapping[str, Mapping[str, Any]]
) -> None:
    parent_id = str(wrapper.get("parent_scan_event_id") or "")
    parent = by_id.get(parent_id)
    if not isinstance(parent, Mapping) or parent.get("event_type") != "substantive_scan":
        raise MonitorScanLedgerError("policy_alert_wrapper_parent_scan_missing")
    helper = wrapper["helper_event"]
    symbol = str(helper["symbol"])
    if set(wrapper["symbols"]) != {symbol}:
        raise MonitorScanLedgerError("policy_alert_wrapper_symbols_mismatch")
    if wrapper["symbols"][symbol] != parent["symbols"].get(symbol):
        raise MonitorScanLedgerError("policy_alert_wrapper_parent_symbol_mismatch")
    for field in (
        "session_date",
        "phase",
        "cadence",
        "evidence",
        "research_hypotheses",
    ):
        if wrapper.get(field) != parent.get(field):
            raise MonitorScanLedgerError(
                f"policy_alert_wrapper_parent_{field}_mismatch"
            )
    confirmed = _parsed_aware(
        helper.get("confirmed_at_utc"), "helper_event.confirmed_at_utc"
    ).astimezone(_UTC)
    parent_observed = _parsed_aware(
        parent.get("observed_at_utc"), "parent_scan.observed_at_utc"
    ).astimezone(_UTC)
    if confirmed > parent_observed:
        raise MonitorScanLedgerError("policy_alert_wrapper_after_parent_scan")


def _append_jsonl_durable(path: Path, record: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _canonical_json_bytes(record)
    descriptor = os.open(
        path,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_BINARY", 0),
        0o600,
    )
    original_size = os.fstat(descriptor).st_size
    bytes_written = 0
    try:
        view = memoryview(encoded)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("journal append made no progress")
            bytes_written += written
            view = view[written:]
        os.fsync(descriptor)
    except BaseException:
        # A short write followed by failure must not strand a malformed JSONL
        # tail.  Only roll back bytes that can still be proven to be ours; a
        # concurrent size change is evidence of an external writer and is left
        # intact so the next snapshot fails closed instead of erasing it.
        try:
            if os.fstat(descriptor).st_size == original_size + bytes_written:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
        except OSError:
            pass
        raise
    finally:
        os.close(descriptor)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        encoded = _canonical_json_bytes(payload, pretty=True)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


@contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+b")
    locked = False
    try:
        if path.stat().st_size == 0:
            handle.write(b"0")
            handle.flush()
            os.fsync(handle.fileno())
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, BlockingIOError) as exc:
        handle.close()
        raise MonitorScanLedgerError("monitor_scan_lock_unavailable") from exc
    locked = True
    try:
        yield
    finally:
        if locked:
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def _load_state(path: Path) -> tuple[dict[str, Any], bytes]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise MonitorScanLedgerError(
            f"state_read_failed:{type(exc).__name__}"
        ) from exc
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MonitorScanLedgerError("state_malformed") from exc
    if not isinstance(decoded, dict):
        raise MonitorScanLedgerError("state_must_be_an_object")
    if decoded.get("schema_version") != 2:
        raise MonitorScanLedgerError("state_schema_version_must_equal_2")
    if not isinstance(decoded.get("policy_evaluator"), Mapping):
        raise MonitorScanLedgerError("state_policy_evaluator_must_be_an_object")
    _canonical_session_date(decoded.get("session_date"))
    _canonical_json_bytes(decoded)
    return decoded, raw


def _decode_journal_records(raw: bytes) -> list[dict[str, Any]]:
    try:
        lines = raw.decode("utf-8").splitlines()
    except UnicodeError as exc:
        raise MonitorScanLedgerError("journal_read_failed:UnicodeError") from exc
    records: list[dict[str, Any]] = []
    for index, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MonitorScanLedgerError(f"journal_line_{index}_malformed") from exc
        if not isinstance(record, dict):
            raise MonitorScanLedgerError(f"journal_line_{index}_must_be_an_object")
        records.append(record)
    return records


def _is_monitor_transaction_record(record: Mapping[str, Any]) -> bool:
    """Identify records owned by the scan/alert/ack transaction protocols."""

    return bool(
        record.get("helper_event") is not None
        or record.get("event_type") in _MONITOR_TRANSACTION_EVENT_TYPES
    )


def _read_journal_snapshot(path: Path) -> tuple[list[dict[str, Any]], bytes]:
    if not path.exists():
        return [], b""
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise MonitorScanLedgerError(
            f"journal_read_failed:{type(exc).__name__}"
        ) from exc
    if raw and not raw.endswith(b"\n"):
        raise MonitorScanLedgerError("journal_trailing_record_not_terminated")
    records = _decode_journal_records(raw)
    return records, raw


def _read_journal_exact_retry_snapshot(
    path: Path,
) -> tuple[list[dict[str, Any]], bytes, bytes | None, bytes]:
    """Read complete records and preserve a possible hard-kill tail for proof."""

    if not path.exists():
        return [], b"", None, b""
    try:
        full_raw = path.read_bytes()
    except OSError as exc:
        raise MonitorScanLedgerError(
            f"journal_read_failed:{type(exc).__name__}"
        ) from exc
    if not full_raw or full_raw.endswith(b"\n"):
        return _decode_journal_records(full_raw), full_raw, None, full_raw
    last_newline = full_raw.rfind(b"\n")
    complete_size = last_newline + 1
    complete_raw = full_raw[:complete_size]
    tail = full_raw[complete_size:]
    return _decode_journal_records(complete_raw), complete_raw, tail, full_raw


def _truncate_exact_partial_tail(
    path: Path, *, expected_full_raw: bytes, complete_size: int
) -> None:
    descriptor = os.open(
        path,
        os.O_RDWR | getattr(os, "O_BINARY", 0),
    )
    try:
        current_size = os.fstat(descriptor).st_size
        if current_size != len(expected_full_raw):
            raise MonitorScanLedgerError("journal_changed_during_tail_recovery")
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed = bytearray()
        while len(observed) < current_size:
            chunk = os.read(descriptor, current_size - len(observed))
            if not chunk:
                break
            observed.extend(chunk)
        if bytes(observed) != expected_full_raw:
            raise MonitorScanLedgerError("journal_changed_during_tail_recovery")
        os.ftruncate(descriptor, complete_size)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_journal(path: Path) -> list[dict[str, Any]]:
    return _read_journal_snapshot(path)[0]


def _journal_index(
    records: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], dict[str, list[dict[str, Any]]]]:
    by_id: dict[str, Mapping[str, Any]] = {}
    positions: dict[str, int] = {}
    receipts = {symbol: [] for symbol in _MONITORED_SYMBOLS}
    policy_wrappers: list[Mapping[str, Any]] = []
    commit_receipts: list[Mapping[str, Any]] = []
    notification_acks: list[Mapping[str, Any]] = []
    for position, record in enumerate(records):
        if (
            record.get("event_type") in _MONITOR_TRANSACTION_EVENT_TYPES
            and record.get("schema_version") != SCAN_WRAPPER_SCHEMA
        ):
            raise MonitorScanLedgerError(
                "journal_transaction_schema_invalid:"
                + str(record.get("event_type"))
            )
        event_id = record.get("event_id")
        if isinstance(event_id, str) and event_id:
            if event_id in by_id:
                raise MonitorScanLedgerError(f"journal_duplicate_event_id:{event_id}")
            by_id[event_id] = record
            positions[event_id] = position
        helper_event = record.get("helper_event")
        policy_shaped = (
            record.get("schema_version") == SCAN_WRAPPER_SCHEMA
            and record.get("event_type") in _POLICY_EVENT_TYPES
        )
        if helper_event is not None or policy_shaped:
            wrapper = _validate_alert_wrapper(record)
            policy_wrappers.append(wrapper)
            receipts[wrapper["helper_event"]["symbol"]].append(
                wrapper["helper_event"]
            )
        elif (
            record.get("schema_version") == SCAN_WRAPPER_SCHEMA
            and record.get("event_type") == "substantive_scan"
        ):
            validate_scan_wrapper(record)
        elif (
            record.get("schema_version") == SCAN_WRAPPER_SCHEMA
            and record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
        ):
            commit_receipts.append(_validate_commit_receipt(record))
        elif (
            record.get("schema_version") == SCAN_WRAPPER_SCHEMA
            and record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
        ):
            notification_acks.append(_validate_notification_ack(record))
        elif record.get("schema_version") == SCAN_WRAPPER_SCHEMA:
            # Operational/research records have event-specific, extensible
            # schemas.  Keep them indexed for global ID-collision detection but
            # never treat them as policy receipts merely because they are v2.
            if not isinstance(record.get("event_type"), str) or not record.get(
                "event_type"
            ):
                raise MonitorScanLedgerError(
                    "journal_non_policy_v2_event_type_invalid"
                )
            _canonical_session_date(record.get("session_date"))
            if "alerts" in record and not isinstance(record.get("alerts"), list):
                raise MonitorScanLedgerError(
                    "journal_non_policy_v2_alerts_must_be_an_array"
                )
    for wrapper in policy_wrappers:
        _validate_alert_parent(wrapper, by_id)
        parent_id = str(wrapper["parent_scan_event_id"])
        wrapper_id = str(wrapper["event_id"])
        if positions[parent_id] >= positions[wrapper_id]:
            raise MonitorScanLedgerError(
                "policy_alert_wrapper_precedes_parent_scan:" + wrapper_id
            )

    commit_by_parent: dict[str, Mapping[str, Any]] = {}
    previous_commit: Mapping[str, Any] | None = None
    previously_seen_data_quality_event_ids: set[str] = set()
    for receipt in commit_receipts:
        receipt_id = str(receipt["event_id"])
        parent_id = str(receipt["parent_scan_event_id"])
        parent = by_id.get(parent_id)
        if not isinstance(parent, Mapping) or parent.get("event_type") != "substantive_scan":
            raise MonitorScanLedgerError("commit_receipt_parent_scan_missing")
        if parent_id in commit_by_parent:
            raise MonitorScanLedgerError("duplicate_commit_receipt_for_parent_scan")
        commit_by_parent[parent_id] = receipt
        if positions[parent_id] >= positions[receipt_id]:
            raise MonitorScanLedgerError("commit_receipt_precedes_parent_scan")
        if receipt.get("session_date") != parent.get("session_date"):
            raise MonitorScanLedgerError("commit_receipt_parent_session_mismatch")
        if receipt.get("observed_at_ct") != parent.get("observed_at_ct"):
            raise MonitorScanLedgerError("commit_receipt_parent_ct_time_mismatch")
        if receipt.get("observed_at_utc") != parent.get("observed_at_utc"):
            raise MonitorScanLedgerError("commit_receipt_parent_time_mismatch")
        for field in ("phase", "cadence", "evidence"):
            if receipt.get(field) != parent.get(field):
                raise MonitorScanLedgerError(
                    f"commit_receipt_parent_{field}_mismatch"
                )

        ordered_ids = list(receipt["ordered_policy_event_ids"])
        physical_ids = [
            str(wrapper["event_id"])
            for wrapper in policy_wrappers
            if wrapper.get("parent_scan_event_id") == parent_id
        ]
        if physical_ids != ordered_ids:
            raise MonitorScanLedgerError("commit_receipt_policy_event_order_mismatch")
        for policy_event_id, commitment in zip(
            ordered_ids, receipt["ordered_policy_records"]
        ):
            policy_wrapper = by_id.get(policy_event_id)
            if (
                not isinstance(policy_wrapper, Mapping)
                or policy_wrapper.get("parent_scan_event_id") != parent_id
            ):
                raise MonitorScanLedgerError("commit_receipt_policy_wrapper_mismatch")
            if not (
                positions[parent_id]
                < positions[policy_event_id]
                < positions[receipt_id]
            ):
                raise MonitorScanLedgerError(
                    "commit_receipt_policy_wrapper_order_invalid"
                )
            if commitment.get("record_sha256") != _canonical_hash(policy_wrapper):
                raise MonitorScanLedgerError(
                    "commit_receipt_policy_wrapper_hash_mismatch:"
                    + policy_event_id
                )

        predecessor = receipt["predecessor"]
        prior_scan_utc: str | None = None
        require_explicit_cadence = False
        if previous_commit is None:
            if predecessor.get("kind") == COMMIT_RECEIPT_EVENT_TYPE:
                raise MonitorScanLedgerError("first_commit_receipt_predecessor_invalid")
            if predecessor.get("kind") == "session_rollover":
                require_explicit_cadence = True
                boundary_id = str(predecessor.get("event_id") or "")
                boundary = by_id.get(boundary_id)
                if (
                    not isinstance(boundary, Mapping)
                    or boundary.get("event_type") != "session_rollover"
                    or positions[boundary_id] >= positions[parent_id]
                    or predecessor.get("record_sha256") != _canonical_hash(boundary)
                ):
                    raise MonitorScanLedgerError(
                        "commit_receipt_rollover_boundary_mismatch"
                    )
                if (
                    _is_sha256(boundary.get("policy_evaluator_sha256"))
                    and receipt.get("pre_policy_evaluator_sha256")
                    != boundary.get("policy_evaluator_sha256")
                ):
                    raise MonitorScanLedgerError(
                        "commit_receipt_rollover_policy_hash_mismatch"
                    )
                expected_rollover_cadence_hash = cadence_state_sha256(
                    {
                        "session_date": parent.get("session_date"),
                        "mode": "NORMAL",
                        "mode_reason": "session_rollover",
                        "elevated_since_ct": None,
                        "elevated_minimum_until_ct": None,
                        "stable_elevated_scan_count": 0,
                    },
                    require_complete=True,
                )
                if (
                    receipt.get("pre_cadence_state_sha256")
                    != expected_rollover_cadence_hash
                ):
                    raise MonitorScanLedgerError(
                        "commit_receipt_rollover_cadence_hash_mismatch"
                    )
            elif receipt["cadence_decision"].get("evidence_mode") != (
                "legacy_compatibility"
            ):
                raise MonitorScanLedgerError(
                    "first_explicit_cadence_requires_session_rollover"
                )
        else:
            if (
                predecessor.get("kind") != COMMIT_RECEIPT_EVENT_TYPE
                or predecessor.get("event_id") != previous_commit.get("event_id")
                or predecessor.get("record_sha256") != _canonical_hash(previous_commit)
                or positions[str(previous_commit["event_id"])] >= positions[receipt_id]
            ):
                raise MonitorScanLedgerError("commit_receipt_chain_mismatch")
            if (
                receipt.get("pre_policy_evaluator_sha256")
                != previous_commit.get("next_policy_evaluator_sha256")
            ):
                raise MonitorScanLedgerError("commit_receipt_policy_hash_chain_mismatch")
            if (
                receipt.get("pre_cadence_state_sha256")
                != previous_commit.get("next_cadence_state_sha256")
            ):
                raise MonitorScanLedgerError(
                    "commit_receipt_cadence_hash_chain_mismatch"
                )
            prior_parent = by_id.get(
                str(previous_commit.get("parent_scan_event_id") or "")
            )
            if not isinstance(prior_parent, Mapping):
                raise MonitorScanLedgerError(
                    "commit_receipt_prior_parent_scan_missing"
                )
            prior_scan_utc = str(prior_parent.get("observed_at_utc") or "")
            require_explicit_cadence = (
                previous_commit["cadence_decision"].get("evidence_mode")
                == "explicit"
            )

        replay_state = copy.deepcopy(dict(receipt["cadence_decision"]["pre_state"]))
        replay_state["last_scan_utc"] = prior_scan_utc
        cadence_policy_events = [
            copy.deepcopy(dict(by_id[event_id]["helper_event"]))
            for event_id in receipt["ordered_policy_event_ids"]
        ]
        try:
            expected_cadence_decision = evaluate_cadence_transition(
                state=replay_state,
                scan=parent,
                policy_events=cadence_policy_events,
                require_explicit_evidence=require_explicit_cadence,
                previously_seen_data_quality_event_ids=sorted(
                    previously_seen_data_quality_event_ids
                ),
            )
        except MonitorCadenceError as exc:
            raise MonitorScanLedgerError(
                f"commit_receipt_cadence_replay_invalid:{exc}"
            ) from exc
        if _canonical_json_bytes(expected_cadence_decision) != _canonical_json_bytes(
            receipt["cadence_decision"]
        ):
            raise MonitorScanLedgerError("commit_receipt_cadence_replay_mismatch")
        if (
            previous_commit is not None
            and previous_commit["cadence_decision"].get("evidence_mode")
            != receipt["cadence_decision"].get("evidence_mode")
        ):
            raise MonitorScanLedgerError(
                "commit_receipt_cadence_evidence_mode_changed"
            )
        previously_seen_data_quality_event_ids.update(
            receipt["cadence_decision"]["active_data_quality_event_ids"]
        )
        previous_commit = receipt

    ack_by_id = {str(record["event_id"]): record for record in notification_acks}
    previous_ack: Mapping[str, Any] | None = None
    latest_commit: Mapping[str, Any] | None = None
    pending_notification_ids: list[str] = []
    commit_by_policy_id: dict[str, Mapping[str, Any]] = {}
    for record in records:
        event_type = record.get("event_type")
        if event_type == COMMIT_RECEIPT_EVENT_TYPE:
            latest_commit = by_id[str(record["event_id"])]
            for policy_event_id in latest_commit["ordered_policy_event_ids"]:
                if policy_event_id in commit_by_policy_id:
                    raise MonitorScanLedgerError(
                        "policy_event_committed_by_multiple_scan_receipts"
                    )
                commit_by_policy_id[str(policy_event_id)] = latest_commit
                pending_notification_ids.append(str(policy_event_id))
            continue
        if event_type != NOTIFICATION_ACK_EVENT_TYPE:
            continue
        ack = ack_by_id[str(record["event_id"])]
        ack_id = str(ack["event_id"])
        if latest_commit is None:
            raise MonitorScanLedgerError("notification_ack_without_scan_commit")
        scan_anchor = ack["scan_commit_anchor"]
        if (
            scan_anchor.get("event_id") != latest_commit.get("event_id")
            or scan_anchor.get("record_sha256") != _canonical_hash(latest_commit)
            or positions[str(latest_commit["event_id"])] >= positions[ack_id]
        ):
            raise MonitorScanLedgerError("notification_ack_scan_anchor_mismatch")
        _validate_notification_ack_causal_order(
            ack,
            by_id=by_id,
            latest_commit=latest_commit,
        )
        expected_previous = (
            None
            if previous_ack is None
            else {
                "event_id": previous_ack["event_id"],
                "record_sha256": _canonical_hash(previous_ack),
            }
        )
        if _canonical_json_bytes(ack.get("previous_ack")) != _canonical_json_bytes(
            expected_previous
        ):
            raise MonitorScanLedgerError("notification_ack_chain_mismatch")
        if ack["pending_notification_event_ids_before"] != pending_notification_ids:
            raise MonitorScanLedgerError("notification_ack_pending_before_mismatch")
        delivered_at = _parsed_aware(
            ack["delivery_proof"]["prior_final_delivered_at_utc"],
            "notification_ack.prior_final_delivered_at_utc",
        ).astimezone(_UTC)
        for commitment in ack["acked_policy_events"]:
            policy_event_id = str(commitment["event_id"])
            wrapper = by_id.get(policy_event_id)
            commit = commit_by_policy_id.get(policy_event_id)
            if (
                not isinstance(wrapper, Mapping)
                or not isinstance(wrapper.get("helper_event"), Mapping)
                or commit is None
                or positions[policy_event_id] >= positions[ack_id]
                or positions[str(commit["event_id"])] >= positions[ack_id]
                or commitment.get("record_sha256") != _canonical_hash(wrapper)
                or commitment.get("parent_scan_event_id")
                != wrapper.get("parent_scan_event_id")
                or commitment.get("commit_receipt_event_id")
                != commit.get("event_id")
                or commitment.get("commit_receipt_sha256")
                != _canonical_hash(commit)
            ):
                raise MonitorScanLedgerError(
                    "notification_ack_policy_commitment_mismatch:"
                    + policy_event_id
                )
            helper_confirmed = _parsed_aware(
                wrapper["helper_event"].get("confirmed_at_utc"),
                "notification_ack.helper_confirmed_at_utc",
            ).astimezone(_UTC)
            if delivered_at < helper_confirmed:
                raise MonitorScanLedgerError(
                    "notification_ack_delivery_precedes_policy_wrapper:"
                    + policy_event_id
                )
        pending_notification_ids = list(
            ack["pending_notification_event_ids_after"]
        )
        previous_ack = ack

    for symbol in receipts:
        receipts[symbol].sort(
            key=lambda row: (
                str(row.get("confirmed_at_utc") or ""),
                str(row.get("event_id") or ""),
            )
        )
    return by_id, receipts


def _validated_ledger_state(
    raw_ledger_state: Any,
    state: Mapping[str, Any],
    by_id: Mapping[str, Mapping[str, Any]],
    journal_raw: bytes,
) -> dict[str, Any]:
    if raw_ledger_state is None:
        return {}
    if not isinstance(raw_ledger_state, Mapping):
        raise MonitorScanLedgerError("monitor_scan_ledger_must_be_an_object")
    ledger_state = copy.deepcopy(dict(raw_ledger_state))
    if not ledger_state:
        return ledger_state
    if ledger_state.get("schema_version") != RESULT_SCHEMA:
        raise MonitorScanLedgerError("monitor_scan_ledger_schema_invalid")
    ledger_session = _canonical_session_date(ledger_state.get("session_date"))
    state_session = _canonical_session_date(state.get("session_date"))
    if ledger_session != state_session:
        if ledger_session < state_session:
            # The rollover helper preserves unknown state extensions.  An
            # older-session ledger namespace is therefore a historical anchor,
            # not a receipt for the newly selected session journal.
            return {}
        raise MonitorScanLedgerError("monitor_scan_ledger_session_mismatch")
    scan_id = ledger_state.get("last_committed_scan_event_id")
    if not isinstance(scan_id, str) or re.fullmatch(r"[0-9a-f]{64}", scan_id) is None:
        raise MonitorScanLedgerError("monitor_scan_ledger_scan_event_id_invalid")
    scan_record = by_id.get(scan_id)
    if (
        not isinstance(scan_record, Mapping)
        or scan_record.get("event_type") != "substantive_scan"
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_scan_receipt_missing")
    committed_at = ledger_state.get("last_committed_observed_at_utc")
    if committed_at != scan_record.get("observed_at_utc"):
        raise MonitorScanLedgerError("monitor_scan_ledger_scan_time_mismatch")
    if state.get("last_scan_utc") != committed_at:
        raise MonitorScanLedgerError("monitor_scan_ledger_state_time_mismatch")

    policy_state_hash = ledger_state.get("policy_evaluator_sha256")
    if (
        not isinstance(policy_state_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", policy_state_hash) is None
        or policy_state_hash != _canonical_hash(state.get("policy_evaluator"))
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_policy_state_hash_mismatch")

    commit_receipt_id = ledger_state.get("last_commit_receipt_event_id")
    commit_receipt_hash = ledger_state.get("last_commit_receipt_sha256")
    if not _is_sha256(commit_receipt_id) or not _is_sha256(commit_receipt_hash):
        raise MonitorScanLedgerError("monitor_scan_ledger_commit_receipt_anchor_invalid")
    commit_receipt = by_id.get(str(commit_receipt_id))
    if (
        not isinstance(commit_receipt, Mapping)
        or commit_receipt.get("event_type") != COMMIT_RECEIPT_EVENT_TYPE
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_commit_receipt_missing")
    if _canonical_hash(commit_receipt) != commit_receipt_hash:
        raise MonitorScanLedgerError("monitor_scan_ledger_commit_receipt_tampered")
    if commit_receipt.get("parent_scan_event_id") != scan_id:
        raise MonitorScanLedgerError("monitor_scan_ledger_commit_parent_mismatch")
    if commit_receipt.get("next_policy_evaluator_sha256") != policy_state_hash:
        raise MonitorScanLedgerError("monitor_scan_ledger_commit_policy_hash_mismatch")
    cadence_is_explicit = (
        commit_receipt["cadence_decision"].get("evidence_mode") == "explicit"
    )
    try:
        current_cadence_hash = cadence_state_sha256(
            state, require_complete=cadence_is_explicit
        )
    except MonitorCadenceError as exc:
        raise MonitorScanLedgerError(
            f"monitor_scan_ledger_cadence_state_invalid:{exc}"
        ) from exc
    if ledger_state.get("cadence_state_sha256") != current_cadence_hash:
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_cadence_state_hash_mismatch"
        )
    if (
        commit_receipt.get("next_cadence_state_sha256")
        != current_cadence_hash
    ):
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_commit_cadence_hash_mismatch"
        )

    committed_receipt_ids = ledger_state.get("committed_receipt_event_ids")
    if (
        not isinstance(committed_receipt_ids, list)
        or not committed_receipt_ids
        or any(not _is_sha256(event_id) for event_id in committed_receipt_ids)
        or len(set(committed_receipt_ids)) != len(committed_receipt_ids)
        or committed_receipt_ids[-1] != commit_receipt_id
    ):
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_committed_receipt_ids_invalid"
        )
    journal_receipt_ids = [
        event_id
        for event_id, record in by_id.items()
        if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
    ]
    if journal_receipt_ids[: len(committed_receipt_ids)] != committed_receipt_ids:
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_committed_receipt_order_mismatch"
        )
    committed_receipts = [by_id[event_id] for event_id in committed_receipt_ids]

    committed_scan_ids = ledger_state.get("committed_scan_event_ids")
    if (
        not isinstance(committed_scan_ids, list)
        or not committed_scan_ids
        or any(
            not isinstance(event_id, str)
            or re.fullmatch(r"[0-9a-f]{64}", event_id) is None
            for event_id in committed_scan_ids
        )
        or len(set(committed_scan_ids)) != len(committed_scan_ids)
        or committed_scan_ids[-1] != scan_id
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_committed_scan_ids_invalid")
    receipt_scan_ids = [
        str(receipt["parent_scan_event_id"]) for receipt in committed_receipts
    ]
    if receipt_scan_ids != committed_scan_ids:
        raise MonitorScanLedgerError("monitor_scan_ledger_committed_scan_order_mismatch")
    for event_id in committed_scan_ids:
        record = by_id.get(event_id)
        if not isinstance(record, Mapping) or record.get("event_type") != "substantive_scan":
            raise MonitorScanLedgerError("monitor_scan_ledger_committed_scan_missing")

    committed_policy_ids = ledger_state.get("committed_policy_event_ids")
    if (
        not isinstance(committed_policy_ids, list)
        or any(not isinstance(event_id, str) for event_id in committed_policy_ids)
        or len(set(committed_policy_ids)) != len(committed_policy_ids)
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_committed_policy_ids_invalid")
    receipt_committed_policy_ids = [
        str(event_id)
        for receipt in committed_receipts
        for event_id in receipt["ordered_policy_event_ids"]
    ]
    if receipt_committed_policy_ids != committed_policy_ids:
        raise MonitorScanLedgerError("monitor_scan_ledger_committed_policy_order_mismatch")

    event_ids = ledger_state.get("last_committed_policy_event_ids")
    hashes = ledger_state.get("last_committed_policy_record_sha256")
    if (
        not isinstance(event_ids, list)
        or any(not isinstance(event_id, str) for event_id in event_ids)
        or len(set(event_ids)) != len(event_ids)
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_policy_event_ids_invalid")
    if not isinstance(hashes, Mapping) or set(hashes) != set(event_ids):
        raise MonitorScanLedgerError("monitor_scan_ledger_policy_hashes_invalid")
    if not set(event_ids).issubset(set(committed_policy_ids)):
        raise MonitorScanLedgerError("monitor_scan_ledger_last_policy_ids_uncommitted")
    if event_ids != list(commit_receipt["ordered_policy_event_ids"]):
        raise MonitorScanLedgerError("monitor_scan_ledger_last_policy_ids_receipt_mismatch")
    for event_id in event_ids:
        record = by_id.get(event_id)
        if not isinstance(record, Mapping) or not isinstance(
            record.get("helper_event"), Mapping
        ):
            raise MonitorScanLedgerError("committed_scan_policy_receipt_missing")
        stored_hash = hashes.get(event_id)
        if (
            not isinstance(stored_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", stored_hash) is None
            or stored_hash != _canonical_hash(record)
        ):
            raise MonitorScanLedgerError("committed_scan_policy_receipt_tampered")

    committed_ack_ids = ledger_state.get("committed_notification_ack_event_ids")
    if (
        not isinstance(committed_ack_ids, list)
        or any(not _is_sha256(event_id) for event_id in committed_ack_ids)
        or len(set(committed_ack_ids)) != len(committed_ack_ids)
    ):
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_notification_ack_ids_invalid"
        )
    journal_ack_ids = [
        event_id
        for event_id, record in by_id.items()
        if record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
    ]
    if journal_ack_ids[: len(committed_ack_ids)] != committed_ack_ids:
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_notification_ack_order_mismatch"
        )
    last_ack_id = ledger_state.get("last_notification_ack_event_id")
    last_ack_hash = ledger_state.get("last_notification_ack_sha256")
    if committed_ack_ids:
        if last_ack_id != committed_ack_ids[-1] or not _is_sha256(last_ack_hash):
            raise MonitorScanLedgerError(
                "monitor_scan_ledger_notification_ack_anchor_invalid"
            )
        last_ack = by_id.get(str(last_ack_id))
        if (
            not isinstance(last_ack, Mapping)
            or last_ack.get("event_type") != NOTIFICATION_ACK_EVENT_TYPE
            or _canonical_hash(last_ack) != last_ack_hash
        ):
            raise MonitorScanLedgerError(
                "monitor_scan_ledger_notification_ack_anchor_tampered"
            )
    elif last_ack_id is not None or last_ack_hash is not None:
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_notification_ack_anchor_unexpected"
        )

    pending_ids = ledger_state.get("pending_notification_event_ids")
    if (
        not isinstance(pending_ids, list)
        or any(not _is_sha256(event_id) for event_id in pending_ids)
        or len(set(pending_ids)) != len(pending_ids)
    ):
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_pending_notification_ids_invalid"
        )
    reflected_receipt_id_set = set(committed_receipt_ids)
    reflected_ack_id_set = set(committed_ack_ids)
    replayed_pending: list[str] = []
    for record in by_id.values():
        record_id = str(record.get("event_id") or "")
        if (
            record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
            and record_id in reflected_receipt_id_set
        ):
            replayed_pending.extend(
                str(event_id) for event_id in record["ordered_policy_event_ids"]
            )
        elif (
            record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
            and record_id in reflected_ack_id_set
        ):
            if record["pending_notification_event_ids_before"] != replayed_pending:
                raise MonitorScanLedgerError(
                    "monitor_scan_ledger_notification_ack_replay_mismatch"
                )
            replayed_pending = list(
                record["pending_notification_event_ids_after"]
            )
    if replayed_pending != pending_ids:
        raise MonitorScanLedgerError(
            "monitor_scan_ledger_pending_notification_replay_mismatch"
        )

    committed_size = ledger_state.get("committed_journal_size")
    committed_hash = ledger_state.get("committed_journal_sha256")
    if (
        type(committed_size) is not int
        or committed_size < 0
        or committed_size > len(journal_raw)
        or not isinstance(committed_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", committed_hash) is None
        or hashlib.sha256(journal_raw[:committed_size]).hexdigest() != committed_hash
    ):
        raise MonitorScanLedgerError("monitor_scan_ledger_journal_anchor_mismatch")
    return ledger_state


def _validate_first_scan_rollover_policy_seed(
    state: Mapping[str, Any], ledger_state: Mapping[str, Any]
) -> None:
    if ledger_state:
        return
    prior_reference = state.get("prior_session_reference")
    if not isinstance(prior_reference, Mapping):
        return
    if "policy_evaluator_at_rollover" not in prior_reference:
        return
    archived = prior_reference.get("policy_evaluator_at_rollover")
    archived_hash = prior_reference.get("policy_evaluator_sha256")
    if not isinstance(archived, Mapping):
        raise MonitorScanLedgerError(
            "prior_session_reference.policy_evaluator_at_rollover_invalid"
        )
    if (
        not isinstance(archived_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", archived_hash) is None
        or archived_hash != _canonical_hash(archived)
    ):
        raise MonitorScanLedgerError(
            "prior_session_reference.policy_evaluator_sha256_mismatch"
        )
    if _canonical_json_bytes(state.get("policy_evaluator")) != _canonical_json_bytes(
        archived
    ):
        raise MonitorScanLedgerError(
            "first_scan_policy_evaluator_differs_from_rollover_archive"
        )


def _validate_first_scan_rollover_cadence_seed(
    state: Mapping[str, Any], ledger_state: Mapping[str, Any]
) -> None:
    if ledger_state or state.get("session_rollover_event_id") is None:
        return
    try:
        observed = cadence_state_snapshot(state, require_complete=True)
        expected_hash = cadence_state_sha256(
            {
                "session_date": state.get("session_date"),
                "mode": "NORMAL",
                "mode_reason": "session_rollover",
                "elevated_since_ct": None,
                "elevated_minimum_until_ct": None,
                "stable_elevated_scan_count": 0,
            },
            require_complete=True,
        )
    except MonitorCadenceError as exc:
        raise MonitorScanLedgerError(
            f"first_scan_rollover_cadence_state_invalid:{exc}"
        ) from exc
    if _canonical_hash(observed) != expected_hash:
        raise MonitorScanLedgerError(
            "first_scan_cadence_state_differs_from_rollover_reset"
        )


def _commit_predecessor(
    *,
    state: Mapping[str, Any],
    ledger_state: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    journal_dir: Path,
) -> dict[str, Any]:
    """Return a validated receipt-chain predecessor for the next commit."""

    if ledger_state:
        receipt_id = ledger_state.get("last_commit_receipt_event_id")
        receipt_hash = ledger_state.get("last_commit_receipt_sha256")
        if not _is_sha256(receipt_id) or not _is_sha256(receipt_hash):
            raise MonitorScanLedgerError(
                "monitor_scan_ledger_commit_receipt_anchor_invalid"
            )
        return {
            "kind": COMMIT_RECEIPT_EVENT_TYPE,
            "event_id": receipt_id,
            "record_sha256": receipt_hash,
        }

    _validate_first_scan_rollover_policy_seed(state, ledger_state)
    _validate_first_scan_rollover_cadence_seed(state, ledger_state)
    rollover_event_id = state.get("session_rollover_event_id")
    if rollover_event_id is not None:
        if not isinstance(rollover_event_id, str) or not rollover_event_id:
            raise MonitorScanLedgerError("session_rollover_event_id_invalid")
        matching = [
            record for record in records if record.get("event_id") == rollover_event_id
        ]
        if len(matching) != 1:
            raise MonitorScanLedgerError(
                "first_scan_session_rollover_receipt_missing_or_duplicate"
            )
        rollover_event = matching[0]
        if rollover_event.get("event_type") != "session_rollover":
            raise MonitorScanLedgerError(
                "first_scan_session_rollover_event_type_invalid"
            )
        rollover_index = list(records).index(rollover_event)
        if any(
            record.get("schema_version") != SCAN_WRAPPER_SCHEMA
            or record.get("session_date") != state.get("session_date")
            or record.get("phase") != "clock_preflight"
            or record.get("event_type") not in {"clock_preflight", "data_quality"}
            for record in records[:rollover_index]
        ):
            raise MonitorScanLedgerError(
                "first_scan_signal_record_precedes_session_rollover"
            )
        try:
            # Runtime import avoids monitor_session_rollover's module-load
            # dependency on this module while sharing its full receipt audit.
            from backend.monitor_session_rollover import (
                _validate_current_event_receipt,
            )

            _validate_current_event_receipt(
                rollover_event,
                state=state,
                journal_dir=journal_dir,
                target=str(state.get("session_date") or ""),
                event_id=rollover_event_id,
            )
        except Exception as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            raise MonitorScanLedgerError(
                "first_scan_session_rollover_receipt_invalid:" + str(exc)
            ) from exc
        return {
            "kind": "session_rollover",
            "event_id": rollover_event_id,
            "record_sha256": _canonical_hash(rollover_event),
        }

    prior_reference = state.get("prior_session_reference")
    rollover_style_reference = (
        isinstance(prior_reference, Mapping)
        and (
            "policy_evaluator_at_rollover" in prior_reference
            or prior_reference.get("session_date") != state.get("session_date")
        )
    )
    if rollover_style_reference or any(
        record.get("event_type") == "session_rollover" for record in records
    ):
        # A rolled session may never silently fall back to the fixture/legacy
        # starting boundary merely because its state pointer was removed.  The
        # current journal and prior-session archive independently prove that a
        # rollover receipt is required.
        raise MonitorScanLedgerError(
            "first_scan_session_rollover_event_id_missing"
        )

    # Unit fixtures and pre-rollover legacy sessions may not have a rollover
    # record.  Their first receipt is still bound to the exact starting policy
    # state; current production rollover states always take the branch above.
    return {
        "kind": "legacy_session_start",
        "event_id": None,
        "record_sha256": _canonical_hash(
            {
                "session_date": state.get("session_date"),
                "policy_evaluator": state.get("policy_evaluator"),
            }
        ),
    }


def _revalidate_expected_journal(
    path: Path, expected_records: Sequence[Mapping[str, Any]]
) -> tuple[bytes, dict[str, Mapping[str, Any]]]:
    records, raw = _read_journal_snapshot(path)
    if list(records) != [dict(record) for record in expected_records]:
        raise MonitorScanLedgerError("journal_changed_during_scan_commit")
    by_id, _receipts = _journal_index(records)
    return raw, by_id


def _event_slot(event_type: str) -> str:
    if event_type in {"MAX_PAIN_CHANGE", "MAX_PAIN_PROVENANCE_RESET"}:
        return "max_pain"
    if event_type == "GAMMA_PIN_SHIFT":
        return "gamma_pin"
    return "directional_latch"


def _slot_state(policy_state: Mapping[str, Any], slot: str) -> Mapping[str, Any]:
    if slot == "directional_latch":
        raw = policy_state.get("directional_latch")
    else:
        levels = policy_state.get("levels")
        raw = levels.get(slot) if isinstance(levels, Mapping) else None
    return raw if isinstance(raw, Mapping) else {}


def _slot_confirmation_time(slot_state: Mapping[str, Any]) -> datetime | None:
    for field in (
        "last_alert_confirmed_at_utc",
        "confirmed_at_utc",
    ):
        value = slot_state.get(field)
        if value:
            try:
                return _parsed_aware(value, field).astimezone(_UTC)
            except MonitorScanLedgerError:
                return None
    values = slot_state.get("confirmation_observed_at_utc")
    if isinstance(values, list) and values:
        try:
            return _parsed_aware(values[-1], "confirmation_observed_at_utc").astimezone(
                _UTC
            )
        except MonitorScanLedgerError:
            return None
    return None


def _unresolved_receipts(
    policy_state: Mapping[str, Any], receipts: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    by_slot: dict[str, list[Mapping[str, Any]]] = {
        "max_pain": [],
        "gamma_pin": [],
        "directional_latch": [],
    }
    for receipt in receipts:
        by_slot[_event_slot(str(receipt.get("type") or ""))].append(receipt)

    unresolved: list[dict[str, Any]] = []
    watermark: datetime | None = None
    raw_watermark = policy_state.get("last_accepted_observed_at_utc")
    if raw_watermark:
        watermark = _parsed_aware(
            raw_watermark, "policy_state.last_accepted_observed_at_utc"
        ).astimezone(_UTC)

    for slot, rows in by_slot.items():
        state_for_slot = _slot_state(policy_state, slot)
        last_event_id = str(state_for_slot.get("last_event_id") or "")
        slot_time = _slot_confirmation_time(state_for_slot)
        ids = [str(row.get("event_id") or "") for row in rows]
        if last_event_id and last_event_id in ids:
            candidates = rows[ids.index(last_event_id) + 1 :]
        elif slot_time is not None:
            candidates = [
                row
                for row in rows
                if _parsed_aware(row.get("confirmed_at_utc"), "confirmed_at_utc")
                .astimezone(_UTC)
                > slot_time
            ]
        else:
            candidates = list(rows)
        for row in candidates:
            confirmed = _parsed_aware(
                row.get("confirmed_at_utc"), "confirmed_at_utc"
            ).astimezone(_UTC)
            if watermark is not None and confirmed <= watermark:
                raise MonitorScanLedgerError(
                    "unreflected_receipt_not_after_policy_watermark:"
                    + str(row.get("event_id") or "")
                )
            unresolved.append(copy.deepcopy(dict(row)))
    unresolved.sort(
        key=lambda row: (
            str(row.get("confirmed_at_utc") or ""),
            str(row.get("event_id") or ""),
        )
    )
    return unresolved


def _scan_candidate(
    scan: Mapping[str, Any] | None, symbol: str
) -> Mapping[str, Any] | None:
    if not isinstance(scan, Mapping):
        return None
    symbols = scan.get("symbols")
    symbol_payload = symbols.get(symbol) if isinstance(symbols, Mapping) else None
    candidate = (
        symbol_payload.get("candidate_policy_observation")
        if isinstance(symbol_payload, Mapping)
        else None
    )
    return candidate if isinstance(candidate, Mapping) else None


def _confirmation_candidate_pair_status(
    prior_candidate: Mapping[str, Any] | None,
    current_candidate: Mapping[str, Any] | None,
    *,
    cadence_mode: str,
) -> tuple[bool, str | None, float | None]:
    if prior_candidate is None:
        return False, "prior_candidate_missing", None
    if current_candidate is None:
        return False, "current_candidate_missing", None
    if any(
        prior_candidate.get(field) != current_candidate.get(field)
        for field in _CONFIRMATION_ALIGNMENT_FIELDS
    ):
        return False, "confirmation_candidates_not_aligned", None
    prior_time = _parsed_aware(
        prior_candidate.get("observed_at_utc"),
        "prior_candidate.observed_at_utc",
    ).astimezone(_UTC)
    current_time = _parsed_aware(
        current_candidate.get("observed_at_utc"),
        "current_candidate.observed_at_utc",
    ).astimezone(_UTC)
    if prior_time.astimezone(_CHICAGO).date() != current_time.astimezone(
        _CHICAGO
    ).date():
        return False, "confirmation_candidates_session_mismatch", None
    gap_seconds = (current_time - prior_time).total_seconds()
    if gap_seconds < _CONFIRMATION_MIN_GAP_SECONDS:
        return False, "confirmation_gap_below_minimum_seconds", gap_seconds
    if gap_seconds > _CONFIRMATION_MAX_GAP_SECONDS_BY_MODE[cadence_mode]:
        return False, "confirmation_gap_exceeds_maximum_seconds", gap_seconds
    return True, None, gap_seconds


def _confirmation_contract_active(
    *,
    scan: Mapping[str, Any],
    prior_scan: Mapping[str, Any] | None,
    records: Sequence[Mapping[str, Any]],
) -> bool:
    session_date = scan.get("session_date")
    if any(
        record.get("session_date") == session_date
        and record.get("event_type") == "session_rollover"
        for record in records
    ):
        return True
    for candidate_scan in (scan, prior_scan):
        if not isinstance(candidate_scan, Mapping):
            continue
        symbols = candidate_scan.get("symbols")
        if not isinstance(symbols, Mapping):
            continue
        if any(
            isinstance(symbols.get(symbol), Mapping)
            and (
                "candidate_policy_observation" in symbols[symbol]
                or "prior_confirmation_scan_event_id" in symbols[symbol]
            )
            for symbol in _MONITORED_SYMBOLS
        ):
            return True
    return False


def _validate_confirmation_history_contract(
    *,
    scan: Mapping[str, Any],
    policy_inputs: Mapping[str, Mapping[str, Any]],
    ledger_state: Mapping[str, Any],
    by_id: Mapping[str, Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    prior_scan_id = ledger_state.get("last_committed_scan_event_id")
    prior_scan = by_id.get(str(prior_scan_id)) if prior_scan_id else None
    active = _confirmation_contract_active(
        scan=scan, prior_scan=prior_scan, records=records
    )
    if not active:
        return {}

    symbols = scan["symbols"]
    eligible_symbols = {
        symbol
        for symbol in _MONITORED_SYMBOLS
        if isinstance(symbols.get(symbol), Mapping)
        and symbols[symbol].get("eligible") is True
    }
    unexpected_policy_symbols = sorted(set(policy_inputs) - eligible_symbols)
    if unexpected_policy_symbols:
        raise MonitorScanLedgerError(
            "policy_inputs_not_allowed_for_ineligible_or_unknown_symbols:"
            + ",".join(unexpected_policy_symbols)
        )

    contexts: dict[str, dict[str, Any]] = {}
    cadence_mode = str(scan["cadence"]["mode"])
    for symbol in _MONITORED_SYMBOLS:
        symbol_payload = symbols[symbol]
        assert isinstance(symbol_payload, Mapping)
        current_candidate = _scan_candidate(scan, symbol)
        prior_candidate = _scan_candidate(prior_scan, symbol)
        ready, reason, gap_seconds = _confirmation_candidate_pair_status(
            prior_candidate,
            current_candidate,
            cadence_mode=cadence_mode,
        )
        eligible = symbol in eligible_symbols
        if not eligible:
            if current_candidate is not None and ready:
                raise MonitorScanLedgerError(
                    f"symbols.{symbol}.confirmation_history_seeding_not_required"
                )
            continue

        if current_candidate is None:
            raise MonitorScanLedgerError(
                f"symbols.{symbol}.candidate_policy_observation_required_when_eligible"
            )
        if not ready:
            raise MonitorScanLedgerError(f"symbols.{symbol}.{reason}")
        if not _is_sha256(prior_scan_id):
            raise MonitorScanLedgerError(
                f"symbols.{symbol}.prior_confirmation_scan_missing"
            )
        if symbol_payload.get("prior_confirmation_scan_event_id") != prior_scan_id:
            raise MonitorScanLedgerError(
                f"symbols.{symbol}.prior_confirmation_scan_event_id_mismatch"
            )
        raw_policy_payload = policy_inputs.get(symbol)
        if isinstance(raw_policy_payload, Mapping):
            supplied_prior_id = raw_policy_payload.get("prior_scan_event_id")
            if supplied_prior_id is not None and supplied_prior_id != prior_scan_id:
                raise MonitorScanLedgerError(
                    f"policy_inputs.{symbol}.prior_scan_event_id_mismatch"
                )
        contexts[symbol] = {
            "prior_scan_event_id": str(prior_scan_id),
            "prior_candidate": copy.deepcopy(dict(prior_candidate)),
            "current_candidate": copy.deepcopy(dict(current_candidate)),
            "confirmation_gap_seconds": gap_seconds,
        }
    return contexts


def _validate_policy_payload(
    symbol: str,
    payload: Mapping[str, Any],
    scan_observed_at: datetime,
    scan_session_date: str,
    scan_cadence_mode: str,
    scan_symbol_payload: Mapping[str, Any],
    confirmation_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise MonitorScanLedgerError(f"policy_inputs.{symbol}_must_be_an_object")
    normalized = copy.deepcopy(dict(payload))
    if normalized.get("schema_version") != POLICY_INPUT_SCHEMA:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.schema_version_invalid"
        )
    observations = normalized.get("observations")
    if not isinstance(observations, list) or len(observations) != 2:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.observations_must_have_exactly_two"
        )
    for index, observation in enumerate(observations):
        if not isinstance(observation, Mapping):
            raise MonitorScanLedgerError(
                f"policy_inputs.{symbol}.observations[{index}]_invalid"
            )
        if observation.get("symbol") != symbol:
            raise MonitorScanLedgerError(
                f"policy_inputs.{symbol}.observations[{index}].symbol_mismatch"
            )
        if observation.get("eligible") is not True:
            raise MonitorScanLedgerError(
                f"policy_inputs.{symbol}.observations[{index}].eligible_must_be_true"
            )
    if confirmation_context is not None:
        prior_candidate = confirmation_context["prior_candidate"]
        current_candidate = confirmation_context["current_candidate"]
        if _canonical_json_bytes(observations[0]) != _canonical_json_bytes(
            prior_candidate
        ):
            raise MonitorScanLedgerError(
                f"policy_inputs.{symbol}.prior_observation_not_anchored_to_previous_committed_scan"
            )
        if _canonical_json_bytes(observations[-1]) != _canonical_json_bytes(
            current_candidate
        ):
            raise MonitorScanLedgerError(
                f"policy_inputs.{symbol}.latest_observation_candidate_mismatch"
            )
    latest = _parsed_aware(
        observations[-1].get("observed_at_utc"),
        f"policy_inputs.{symbol}.latest_observed_at_utc",
    )
    latest_utc = latest.astimezone(_UTC)
    scan_utc = scan_observed_at.astimezone(_UTC)
    if latest.utcoffset() != _UTC.utcoffset(latest):
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.latest_observed_at_utc_must_be_utc"
        )
    if latest_utc > scan_utc:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.latest_observation_after_scan"
        )
    if latest_utc.astimezone(_CHICAGO).date().isoformat() != scan_session_date:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.latest_observation_session_mismatch"
        )
    expected_cadence = _CADENCE_SECONDS_BY_MODE[scan_cadence_mode]
    if normalized.get("confirmation_cadence_seconds") != expected_cadence:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.confirmation_cadence_scan_mode_mismatch"
        )
    age_seconds = (scan_utc - latest_utc).total_seconds()
    if age_seconds > expected_cadence:
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.latest_observation_stale_for_scan_cadence"
        )
    scan_policy_observation = scan_symbol_payload.get("policy_observation")
    if not isinstance(scan_policy_observation, Mapping):
        raise MonitorScanLedgerError(
            f"symbols.{symbol}.policy_observation_required_when_eligible"
        )
    if _canonical_json_bytes(scan_policy_observation) != _canonical_json_bytes(
        observations[-1]
    ):
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.latest_observation_scan_payload_mismatch"
        )
    normalized = _sanitized_policy_input(normalized)
    if policy_input_sha256(normalized) != scan_symbol_payload.get(
        "policy_input_sha256"
    ):
        raise MonitorScanLedgerError(
            f"policy_inputs.{symbol}.sha256_scan_payload_mismatch"
        )
    return normalized


def _validate_policy_result(symbol: str, result: Any) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise MonitorScanLedgerError(f"policy_result.{symbol}_must_be_an_object")
    normalized = copy.deepcopy(dict(result))
    if normalized.get("schema_version") != POLICY_OUTPUT_SCHEMA:
        raise MonitorScanLedgerError(
            f"policy_result.{symbol}.schema_version_invalid"
        )
    if normalized.get("accepted") is not True:
        issues = normalized.get("issues")
        suffix = ",".join(str(value) for value in issues) if isinstance(issues, list) else "rejected"
        raise MonitorScanLedgerError(f"policy_result.{symbol}_rejected:{suffix}")
    if not isinstance(normalized.get("next_state"), Mapping):
        raise MonitorScanLedgerError(f"policy_result.{symbol}.next_state_invalid")
    events = normalized.get("events")
    if not isinstance(events, list):
        raise MonitorScanLedgerError(f"policy_result.{symbol}.events_invalid")
    seen: set[str] = set()
    for event in events:
        _validate_helper_event(event)
        if event.get("symbol") != symbol:
            raise MonitorScanLedgerError(f"policy_result.{symbol}.event_symbol_mismatch")
        event_id = str(event["event_id"])
        if event_id in seen:
            raise MonitorScanLedgerError(f"policy_result.{symbol}.duplicate_event_id")
        seen.add(event_id)
    _canonical_json_bytes(normalized)
    return normalized


def _build_alert_wrapper(
    scan: Mapping[str, Any], event: Mapping[str, Any]
) -> dict[str, Any]:
    helper_event = copy.deepcopy(dict(event))
    helper_event.pop("durable_receipt_applied", None)
    symbol = str(helper_event["symbol"])
    direction = helper_event.get("direction")
    interpretation = str(helper_event["type"])
    if direction:
        interpretation += f"_{str(direction).upper()}"
    wrapper = {
        "schema_version": SCAN_WRAPPER_SCHEMA,
        "event_id": helper_event["event_id"],
        "event_type": helper_event["type"],
        "parent_scan_event_id": scan["event_id"],
        "observed_at_ct": helper_event["confirmed_at_ct"],
        "observed_at_utc": helper_event["confirmed_at_utc"],
        "session_date": scan["session_date"],
        "phase": scan["phase"],
        "cadence": copy.deepcopy(scan["cadence"]),
        "evidence": copy.deepcopy(scan["evidence"]),
        "symbols": {symbol: copy.deepcopy(scan["symbols"][symbol])},
        "alerts": [copy.deepcopy(helper_event)],
        "directional_interpretation": interpretation,
        "research_hypotheses": copy.deepcopy(scan["research_hypotheses"]),
        "helper_event": helper_event,
    }
    return _validate_alert_wrapper(wrapper)


def _base_result(scan: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "scan_event_id": scan.get("event_id") if isinstance(scan, Mapping) else None,
        "commit_receipt_event_id": None,
        "commit_receipt_appended": False,
        "cadence_decision": None,
        "cadence_transition_newly_durable": False,
        "partial_journal_tail_recovered": False,
        "records_appended": 0,
        "event_ids_appended": [],
        "event_ids_recovered": [],
        "event_ids_pending_notification": [],
        "policy_symbols_evaluated": [],
        "diagnostic_symbols_retained": [],
        "state_updated": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def commit_monitor_scan(
    *,
    scan: Mapping[str, Any],
    policy_inputs: Mapping[str, Mapping[str, Any]] | None,
    state_path: Path,
    journal_dir: Path,
    evaluator: Callable[[Mapping[str, Any]], Mapping[str, Any]] = evaluate_monitor_policy,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Commit one scan and its deterministic alerts with receipt-first recovery."""

    result = _base_result(scan)
    state_replace_succeeded = False
    try:
        normalized_scan = validate_scan_wrapper(scan)
        result["scan_event_id"] = normalized_scan["event_id"]
        if policy_inputs is None:
            normalized_policy_inputs: Mapping[str, Mapping[str, Any]] = {}
        elif isinstance(policy_inputs, Mapping):
            normalized_policy_inputs = policy_inputs
        else:
            raise MonitorScanLedgerError("policy_inputs_must_be_an_object")
        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()
        journal_path = journal_dir / f"{normalized_scan['session_date']}.jsonl"
        result["journal_path"] = str(journal_path)

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            if state.get("session_date") != normalized_scan["session_date"]:
                raise MonitorScanLedgerError("state_session_date_mismatch")
            state_mode = state.get("mode")
            if state_mode not in _CADENCE_SECONDS_BY_MODE:
                raise MonitorScanLedgerError("state_mode_must_be_NORMAL_or_ELEVATED")
            (
                records,
                journal_raw,
                partial_journal_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            by_id, _receipts_by_symbol = _journal_index(records)
            existing_scan = by_id.get(normalized_scan["event_id"])
            if existing_scan is not None and dict(existing_scan) != normalized_scan:
                raise MonitorScanLedgerError("existing_scan_event_id_payload_mismatch")
            if (
                existing_scan is None
                and normalized_scan["cadence"].get("mode") != state_mode
            ):
                raise MonitorScanLedgerError("scan_cadence_state_mode_mismatch")

            for symbol in _MONITORED_SYMBOLS:
                raw_symbol_payload = normalized_scan["symbols"].get(symbol)
                if (
                    isinstance(raw_symbol_payload, Mapping)
                    and raw_symbol_payload.get("eligible") is True
                    and symbol in normalized_policy_inputs
                ):
                    supplied_policy_payload = normalized_policy_inputs[symbol]
                    if not isinstance(supplied_policy_payload, Mapping):
                        raise MonitorScanLedgerError(
                            f"policy_inputs.{symbol}_must_be_an_object"
                        )
                    if policy_input_sha256(
                        supplied_policy_payload
                    ) != raw_symbol_payload.get("policy_input_sha256"):
                        raise MonitorScanLedgerError(
                            f"policy_inputs.{symbol}.sha256_scan_payload_mismatch"
                        )

            ledger_state = _validated_ledger_state(
                state.get("monitor_scan_ledger"), state, by_id, journal_raw
            )
            if (
                existing_scan is not None
                and ledger_state.get("last_committed_scan_event_id")
                != normalized_scan["event_id"]
                and normalized_scan["cadence"].get("mode") != state_mode
            ):
                raise MonitorScanLedgerError("scan_cadence_state_mode_mismatch")
            predecessor = _commit_predecessor(
                state=state,
                ledger_state=ledger_state,
                records=records,
                journal_dir=journal_dir,
            )
            journal_scans = [
                record
                for record in by_id.values()
                if record.get("event_type") == "substantive_scan"
            ]
            scan_times = [
                _parsed_aware(
                    record.get("observed_at_utc"),
                    "journal.scan.observed_at_utc",
                ).astimezone(_UTC)
                for record in journal_scans
            ]
            if scan_times != sorted(scan_times) or len(set(scan_times)) != len(
                scan_times
            ):
                raise MonitorScanLedgerError(
                    "journal_substantive_scans_out_of_order"
                )
            committed_scan_ids = set(
                ledger_state.get("committed_scan_event_ids", [])
            )
            orphan_scans = [
                record
                for record in journal_scans
                if record.get("event_id") not in committed_scan_ids
            ]
            if len(orphan_scans) > 1:
                raise MonitorScanLedgerError(
                    "multiple_unreflected_substantive_scans"
                )
            orphan_scan = orphan_scans[0] if orphan_scans else None
            if (
                orphan_scan is not None
                and orphan_scan.get("event_id") != normalized_scan["event_id"]
            ):
                raise MonitorScanLedgerError(
                    "unreflected_substantive_scan_requires_exact_retry:"
                    + str(orphan_scan.get("event_id") or "")
                )

            committed_policy_ids = set(
                ledger_state.get("committed_policy_event_ids", [])
            )
            committed_ack_ids = set(
                ledger_state.get("committed_notification_ack_event_ids", [])
            )
            unreflected_ack_ids = [
                event_id
                for event_id, record in by_id.items()
                if record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
                and event_id not in committed_ack_ids
            ]
            if unreflected_ack_ids:
                if len(unreflected_ack_ids) > 1:
                    raise MonitorScanLedgerError(
                        "multiple_unreflected_notification_acks"
                    )
                raise MonitorScanLedgerError(
                    "unreflected_notification_ack_requires_exact_recovery:"
                    + unreflected_ack_ids[0]
                )
            committed_receipt_ids = set(
                ledger_state.get("committed_receipt_event_ids", [])
            )
            unreflected_commit_receipts = [
                record
                for event_id, record in by_id.items()
                if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
                and event_id not in committed_receipt_ids
            ]
            if len(unreflected_commit_receipts) > 1:
                raise MonitorScanLedgerError(
                    "multiple_unreflected_commit_receipts"
                )
            unreflected_commit_receipt = (
                unreflected_commit_receipts[0]
                if unreflected_commit_receipts
                else None
            )
            if (
                unreflected_commit_receipt is not None
                and (
                    orphan_scan is None
                    or unreflected_commit_receipt.get("parent_scan_event_id")
                    != orphan_scan.get("event_id")
                )
            ):
                raise MonitorScanLedgerError(
                    "unreflected_commit_receipt_without_matching_orphan_scan"
                )
            unreflected_policy_ids = {
                event_id
                for event_id, record in by_id.items()
                if isinstance(record.get("helper_event"), Mapping)
                and event_id not in committed_policy_ids
            }
            if unreflected_policy_ids:
                if orphan_scan is None or any(
                    by_id[event_id].get("parent_scan_event_id")
                    != orphan_scan.get("event_id")
                    for event_id in unreflected_policy_ids
                ):
                    raise MonitorScanLedgerError(
                        "unreflected_policy_wrapper_without_matching_orphan_scan"
                    )
            if (
                existing_scan is not None
                and ledger_state.get("last_committed_scan_event_id")
                == normalized_scan["event_id"]
            ):
                if partial_journal_tail is not None:
                    raise MonitorScanLedgerError(
                        "journal_trailing_record_not_terminated"
                    )
                result.update(
                    {
                        "accepted": True,
                        "action": "already_committed",
                        "commit_receipt_event_id": ledger_state.get(
                            "last_commit_receipt_event_id"
                        ),
                        "cadence_decision": copy.deepcopy(
                            by_id[
                                str(ledger_state["last_commit_receipt_event_id"])
                            ]["cadence_decision"]
                        ),
                        "cadence_transition_newly_durable": False,
                        "event_ids_pending_notification": list(
                            ledger_state.get("pending_notification_event_ids", [])
                        ),
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result

            confirmation_contexts = _validate_confirmation_history_contract(
                scan=normalized_scan,
                policy_inputs=normalized_policy_inputs,
                ledger_state=ledger_state,
                by_id=by_id,
                records=records,
            )

            scan_observed_at = _parsed_aware(
                normalized_scan["observed_at_utc"], "observed_at_utc"
            ).astimezone(_UTC)
            prior_scan_raw = state.get("last_scan_utc")
            if prior_scan_raw:
                prior_scan = _parsed_aware(
                    prior_scan_raw, "state.last_scan_utc"
                ).astimezone(_UTC)
                if orphan_scan is not None:
                    orphan_time = _parsed_aware(
                        orphan_scan.get("observed_at_utc"),
                        "orphan_scan.observed_at_utc",
                    ).astimezone(_UTC)
                    if orphan_time <= prior_scan:
                        raise MonitorScanLedgerError(
                            "unreflected_substantive_scan_out_of_order"
                        )
                if scan_observed_at <= prior_scan:
                    raise MonitorScanLedgerError(
                        "scan_observed_at_not_after_state_last_scan"
                    )

            for record in records:
                if record.get("event_type") != "substantive_scan":
                    continue
                recorded_at = _parsed_aware(
                    record.get("observed_at_utc"),
                    "journal.scan.observed_at_utc",
                ).astimezone(_UTC)
                if recorded_at > scan_observed_at or (
                    recorded_at == scan_observed_at
                    and record.get("event_id") != normalized_scan["event_id"]
                ):
                    raise MonitorScanLedgerError(
                        "scan_observed_at_not_after_latest_journal_scan"
                    )

            policy_states = state.get("policy_evaluator")
            assert isinstance(policy_states, Mapping)
            next_policy_states = copy.deepcopy(dict(policy_states))
            generated_events: list[dict[str, Any]] = []
            recovered_ids: set[str] = set()

            for symbol in _MONITORED_SYMBOLS:
                raw_symbol_payload = normalized_scan["symbols"].get(symbol)
                eligible = bool(
                    isinstance(raw_symbol_payload, Mapping)
                    and raw_symbol_payload.get("eligible") is True
                )
                if not eligible:
                    continue
                raw_policy_payload = normalized_policy_inputs.get(symbol)
                if raw_policy_payload is None:
                    raise MonitorScanLedgerError(
                        f"policy_inputs.{symbol}_required_for_eligible_symbol"
                    )
                prepared = _validate_policy_payload(
                    symbol,
                    raw_policy_payload,
                    scan_observed_at,
                    normalized_scan["session_date"],
                    str(normalized_scan["cadence"]["mode"]),
                    raw_symbol_payload,
                    confirmation_contexts.get(symbol),
                )
                raw_policy_state = policy_states.get(symbol)
                policy_state = (
                    copy.deepcopy(dict(raw_policy_state))
                    if isinstance(raw_policy_state, Mapping)
                    else {}
                )
                prepared["policy_state"] = policy_state
                # An orphan transaction still has the unchanged atomic
                # pre-state.  Regenerate the exact deterministic result from
                # hash-bound observations only; journal contents are verified
                # outputs, never evaluator inputs that can steer next_state.
                prepared.pop("durable_event_receipts", None)
                evaluated = _validate_policy_result(symbol, evaluator(prepared))
                result["policy_symbols_evaluated"].append(symbol)
                next_policy_states[symbol] = copy.deepcopy(
                    dict(evaluated["next_state"])
                )
                generated_events.extend(copy.deepcopy(evaluated["events"]))

            seen_result_ids: set[str] = set()
            ordered_result_ids: list[str] = []
            new_wrappers: list[dict[str, Any]] = []
            for event in generated_events:
                event_id = str(event["event_id"])
                if event_id in seen_result_ids:
                    continue
                seen_result_ids.add(event_id)
                ordered_result_ids.append(event_id)
                if event_id == normalized_scan["event_id"]:
                    raise MonitorScanLedgerError(
                        "policy_event_id_collides_with_parent_scan"
                    )
                if event_id in by_id:
                    existing_wrapper = by_id[event_id]
                    existing_helper = existing_wrapper.get("helper_event")
                    expected_helper = copy.deepcopy(dict(event))
                    expected_helper.pop("durable_receipt_applied", None)
                    if not isinstance(existing_helper, Mapping):
                        raise MonitorScanLedgerError(
                            "policy_event_id_collision_with_nonreceipt:"
                            + event_id
                        )
                    if (
                        existing_wrapper.get("parent_scan_event_id")
                        != normalized_scan["event_id"]
                    ):
                        raise MonitorScanLedgerError(
                            "existing_policy_receipt_parent_scan_mismatch:"
                            + event_id
                        )
                    if dict(existing_helper) != expected_helper:
                        raise MonitorScanLedgerError(
                            "existing_policy_receipt_payload_mismatch:" + event_id
                        )
                    expected_wrapper = _build_alert_wrapper(
                        normalized_scan, expected_helper
                    )
                    if _canonical_json_bytes(
                        existing_wrapper
                    ) != _canonical_json_bytes(expected_wrapper):
                        raise MonitorScanLedgerError(
                            "existing_policy_wrapper_payload_mismatch:"
                            + event_id
                        )
                    recovered_ids.add(event_id)
                    continue
                if event.get("durable_receipt_applied") is True:
                    raise MonitorScanLedgerError(
                        "recovered_helper_event_missing_from_journal"
                    )
                wrapper = _build_alert_wrapper(normalized_scan, event)
                _validate_alert_parent(
                    wrapper,
                    {normalized_scan["event_id"]: normalized_scan},
                )
                new_wrappers.append(wrapper)

            if not recovered_ids.issubset(seen_result_ids):
                raise MonitorScanLedgerError(
                    "recovered_receipt_not_returned_by_policy"
                )
            if not unreflected_policy_ids.issubset(recovered_ids):
                raise MonitorScanLedgerError(
                    "unreflected_policy_wrapper_not_causally_recovered"
                )
            _canonical_json_bytes(next_policy_states)

            existing_parent_policy_ids = [
                event_id
                for event_id, record in by_id.items()
                if isinstance(record.get("helper_event"), Mapping)
                and record.get("parent_scan_event_id") == normalized_scan["event_id"]
            ]
            if (
                ordered_result_ids[: len(existing_parent_policy_ids)]
                != existing_parent_policy_ids
            ):
                raise MonitorScanLedgerError(
                    "existing_policy_receipts_not_deterministic_prefix"
                )

            policy_wrappers_for_receipt: dict[str, Mapping[str, Any]] = {
                event_id: by_id[event_id]
                for event_id in existing_parent_policy_ids
            }
            policy_wrappers_for_receipt.update(
                {str(wrapper["event_id"]): wrapper for wrapper in new_wrappers}
            )
            if set(policy_wrappers_for_receipt) != set(ordered_result_ids):
                raise MonitorScanLedgerError(
                    "commit_receipt_policy_wrapper_set_mismatch"
                )
            ordered_policy_records = [
                {
                    "event_id": event_id,
                    "record_sha256": _canonical_hash(
                        policy_wrappers_for_receipt[event_id]
                    ),
                }
                for event_id in ordered_result_ids
            ]

            cadence_policy_events = [
                copy.deepcopy(
                    dict(policy_wrappers_for_receipt[event_id]["helper_event"])
                )
                for event_id in ordered_result_ids
            ]
            require_explicit_cadence = predecessor.get("kind") == "session_rollover"
            previous_cadence_evidence_mode: str | None = None
            if ledger_state:
                previous_receipt_id = str(
                    ledger_state.get("last_commit_receipt_event_id") or ""
                )
                previous_receipt = by_id.get(previous_receipt_id)
                if not isinstance(previous_receipt, Mapping):
                    raise MonitorScanLedgerError(
                        "monitor_scan_ledger_commit_receipt_missing"
                    )
                require_explicit_cadence = (
                    previous_receipt["cadence_decision"].get("evidence_mode")
                    == "explicit"
                )
                previous_cadence_evidence_mode = str(
                    previous_receipt["cadence_decision"].get("evidence_mode") or ""
                )
            previously_seen_data_quality_event_ids = sorted(
                {
                    str(event_id)
                    for record in by_id.values()
                    if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
                    and record.get("parent_scan_event_id")
                    != normalized_scan["event_id"]
                    for event_id in record["cadence_decision"].get(
                        "active_data_quality_event_ids", []
                    )
                }
            )
            try:
                cadence_decision = evaluate_cadence_transition(
                    state=state,
                    scan=normalized_scan,
                    policy_events=cadence_policy_events,
                    require_explicit_evidence=require_explicit_cadence,
                    previously_seen_data_quality_event_ids=(
                        previously_seen_data_quality_event_ids
                    ),
                )
            except MonitorCadenceError as exc:
                raise MonitorScanLedgerError(str(exc)) from exc
            if (
                cadence_decision["evidence_mode"] == "explicit"
                and predecessor.get("kind") == "legacy_session_start"
            ):
                raise MonitorScanLedgerError(
                    "first_explicit_cadence_requires_session_rollover"
                )
            if (
                previous_cadence_evidence_mode is not None
                and cadence_decision["evidence_mode"]
                != previous_cadence_evidence_mode
            ):
                raise MonitorScanLedgerError(
                    "cadence_evidence_mode_change_requires_session_rollover"
                )
            result["cadence_decision"] = copy.deepcopy(cadence_decision)

            commit_receipt = _build_commit_receipt(
                scan=normalized_scan,
                ordered_policy_event_ids=ordered_result_ids,
                ordered_policy_records=ordered_policy_records,
                pre_policy_evaluator_sha256=_canonical_hash(policy_states),
                next_policy_evaluator_sha256=_canonical_hash(next_policy_states),
                cadence_decision=cadence_decision,
                predecessor=predecessor,
            )
            _validate_commit_receipt(commit_receipt)
            result["commit_receipt_event_id"] = commit_receipt["event_id"]
            existing_receipts_for_scan = [
                record
                for record in by_id.values()
                if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
                and record.get("parent_scan_event_id") == normalized_scan["event_id"]
            ]
            if len(existing_receipts_for_scan) > 1:
                raise MonitorScanLedgerError(
                    "duplicate_commit_receipt_for_parent_scan"
                )
            existing_commit_receipt = (
                existing_receipts_for_scan[0]
                if existing_receipts_for_scan
                else None
            )
            if (
                existing_commit_receipt is not None
                and _canonical_json_bytes(existing_commit_receipt)
                != _canonical_json_bytes(commit_receipt)
            ):
                raise MonitorScanLedgerError(
                    "existing_commit_receipt_payload_mismatch"
                )
            collision = by_id.get(commit_receipt["event_id"])
            if collision is not None and _canonical_json_bytes(
                collision
            ) != _canonical_json_bytes(commit_receipt):
                raise MonitorScanLedgerError(
                    "commit_receipt_event_id_collision"
                )

            if partial_journal_tail is not None:
                committed_size = ledger_state.get("committed_journal_size", 0)
                if (
                    type(committed_size) is not int
                    or committed_size < 0
                    or committed_size > len(journal_raw)
                    or len(journal_raw) < committed_size
                ):
                    raise MonitorScanLedgerError(
                        "partial_journal_tail_committed_boundary_invalid"
                    )
                if ledger_state:
                    committed_prefix = journal_raw[:committed_size]
                    if committed_prefix and not committed_prefix.endswith(b"\n"):
                        raise MonitorScanLedgerError(
                            "partial_journal_tail_committed_boundary_invalid"
                        )
                    committed_record_count = len(
                        _decode_journal_records(committed_prefix)
                    )
                elif existing_scan is not None:
                    committed_record_count = list(records).index(existing_scan)
                else:
                    committed_record_count = len(records)

                transaction_records: list[Mapping[str, Any]] = [normalized_scan]
                transaction_records.extend(
                    policy_wrappers_for_receipt[event_id]
                    for event_id in ordered_result_ids
                )
                transaction_records.append(commit_receipt)
                complete_suffix = list(records[committed_record_count:])
                # Other monitor writers may have durably appended complete,
                # validated operational/research records after the last scan
                # receipt.  They are not part of this interrupted transaction
                # and must remain untouched.  The first transaction-owned
                # record still has to start an exact prefix; owned records may
                # never be skipped.
                transaction_start = next(
                    (
                        index
                        for index, record in enumerate(complete_suffix)
                        if _is_monitor_transaction_record(record)
                    ),
                    len(complete_suffix),
                )
                durable_suffix = complete_suffix[transaction_start:]
                if len(durable_suffix) >= len(transaction_records) or any(
                    _canonical_json_bytes(observed)
                    != _canonical_json_bytes(expected)
                    for observed, expected in zip(
                        durable_suffix, transaction_records
                    )
                ):
                    raise MonitorScanLedgerError(
                        "partial_journal_complete_suffix_not_exact_transaction_prefix"
                    )
                next_record = transaction_records[len(durable_suffix)]
                next_record_bytes = _canonical_json_bytes(next_record)
                if (
                    not partial_journal_tail
                    or len(partial_journal_tail) > len(next_record_bytes)
                    or not next_record_bytes.startswith(partial_journal_tail)
                ):
                    raise MonitorScanLedgerError(
                        "partial_journal_tail_not_exact_transaction_prefix"
                    )
                try:
                    if state_path.read_bytes() != original_state_bytes:
                        raise MonitorScanLedgerError(
                            "state_changed_during_tail_recovery"
                        )
                except OSError as exc:
                    raise MonitorScanLedgerError(
                        f"state_revalidation_failed:{type(exc).__name__}"
                    ) from exc
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True

            result["diagnostic_symbols_retained"] = [
                symbol
                for symbol in _MONITORED_SYMBOLS
                if isinstance(normalized_scan["symbols"].get(symbol), Mapping)
                and normalized_scan["symbols"][symbol].get("eligible") is not True
            ]

            expected_records: list[Mapping[str, Any]] = list(records)
            if existing_scan is None:
                _append_jsonl_durable(journal_path, normalized_scan)
                result["records_appended"] += 1
                result["commit_phase"] = "journal_durable"
                expected_records.append(normalized_scan)
                by_id[normalized_scan["event_id"]] = normalized_scan
                if failpoint is not None:
                    failpoint("after_scan_append")
            else:
                result["commit_phase"] = "journal_durable"
            for wrapper in new_wrappers:
                _append_jsonl_durable(journal_path, wrapper)
                result["records_appended"] += 1
                result["commit_phase"] = "journal_durable"
                result["event_ids_appended"].append(wrapper["event_id"])
                expected_records.append(wrapper)
                by_id[wrapper["event_id"]] = wrapper
                if failpoint is not None:
                    failpoint("after_event_append")

            if existing_commit_receipt is None:
                _append_jsonl_durable(journal_path, commit_receipt)
                result["records_appended"] += 1
                result["commit_phase"] = "journal_durable"
                result["commit_receipt_appended"] = True
                expected_records.append(commit_receipt)
                by_id[commit_receipt["event_id"]] = commit_receipt
                if failpoint is not None:
                    failpoint("after_commit_receipt_append")

            if failpoint is not None:
                failpoint("before_state_replace")
            try:
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorScanLedgerError("state_changed_during_scan_commit")
            except OSError as exc:
                raise MonitorScanLedgerError(
                    f"state_revalidation_failed:{type(exc).__name__}"
                ) from exc
            committed_journal_raw, committed_by_id = _revalidate_expected_journal(
                journal_path, expected_records
            )

            committed_policy_hashes = {
                event_id: _canonical_hash(committed_by_id[event_id])
                for event_id in ordered_result_ids
            }
            committed_receipt_event_ids = list(
                ledger_state.get("committed_receipt_event_ids", [])
            )
            committed_receipt_event_ids.append(commit_receipt["event_id"])
            committed_scan_event_ids = list(
                ledger_state.get("committed_scan_event_ids", [])
            )
            committed_scan_event_ids.append(normalized_scan["event_id"])
            committed_policy_event_ids = list(
                ledger_state.get("committed_policy_event_ids", [])
            )
            committed_policy_event_ids.extend(ordered_result_ids)
            pending_notification_event_ids = list(
                ledger_state.get("pending_notification_event_ids", [])
            )
            if any(
                event_id in pending_notification_event_ids
                for event_id in ordered_result_ids
            ):
                raise MonitorScanLedgerError(
                    "policy_event_already_pending_notification"
                )
            pending_notification_event_ids.extend(ordered_result_ids)

            next_state = apply_cadence_decision(state, cadence_decision)
            next_state["policy_evaluator"] = next_policy_states
            next_state["last_scan_ct"] = normalized_scan["observed_at_ct"]
            next_state["last_scan_utc"] = normalized_scan["observed_at_utc"]
            eligible_symbols = list(result["policy_symbols_evaluated"])
            next_state["last_scan_result"] = (
                "eligible" if eligible_symbols else "diagnostic_unavailable"
            )
            if eligible_symbols:
                next_state["last_eligible_scan_ct"] = normalized_scan[
                    "observed_at_ct"
                ]
            next_state["updated_at_ct"] = normalized_scan["observed_at_ct"]
            next_state["updated_at_utc"] = normalized_scan["observed_at_utc"]
            next_ledger_state = copy.deepcopy(dict(ledger_state))
            next_ledger_state.update(
                {
                    "schema_version": RESULT_SCHEMA,
                    "session_date": normalized_scan["session_date"],
                    "last_committed_scan_event_id": normalized_scan["event_id"],
                    "last_committed_observed_at_utc": normalized_scan[
                        "observed_at_utc"
                    ],
                    "last_committed_policy_event_ids": ordered_result_ids,
                    "last_committed_policy_record_sha256": committed_policy_hashes,
                    "last_commit_receipt_event_id": commit_receipt["event_id"],
                    "last_commit_receipt_sha256": _canonical_hash(commit_receipt),
                    "committed_receipt_event_ids": committed_receipt_event_ids,
                    "committed_scan_event_ids": committed_scan_event_ids,
                    "committed_policy_event_ids": committed_policy_event_ids,
                    "pending_notification_event_ids": pending_notification_event_ids,
                    "committed_notification_ack_event_ids": list(
                        ledger_state.get(
                            "committed_notification_ack_event_ids", []
                        )
                    ),
                    "last_notification_ack_event_id": ledger_state.get(
                        "last_notification_ack_event_id"
                    ),
                    "last_notification_ack_sha256": ledger_state.get(
                        "last_notification_ack_sha256"
                    ),
                    "policy_evaluator_sha256": _canonical_hash(next_policy_states),
                    "cadence_state_sha256": cadence_state_sha256(
                        next_state,
                        require_complete=(
                            cadence_decision["evidence_mode"] == "explicit"
                        ),
                    ),
                    "committed_journal_size": len(committed_journal_raw),
                    "committed_journal_sha256": hashlib.sha256(
                        committed_journal_raw
                    ).hexdigest(),
                }
            )
            next_state["monitor_scan_ledger"] = next_ledger_state
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted_after_error, _raw_after_error = _load_state(state_path)
                    state_replace_succeeded = persisted_after_error == next_state
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _persisted_raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorScanLedgerError("state_postcondition_failed")
            _revalidate_expected_journal(journal_path, expected_records)

            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_committed"
                        if recovered_ids
                        or existing_scan is not None
                        or result["partial_journal_tail_recovered"]
                        else "committed"
                    ),
                    "event_ids_recovered": sorted(recovered_ids),
                    "event_ids_pending_notification": pending_notification_event_ids,
                    "cadence_transition_newly_durable": bool(
                        result["commit_receipt_appended"]
                        and cadence_decision["transition"]
                        in {
                            "ENTER_ELEVATED",
                            "EXTEND_ELEVATED",
                            "RETURN_NORMAL",
                        }
                    ),
                    "state_updated": True,
                    "commit_phase": "committed",
                    "issues": [],
                }
            )
            return result

        # Share the existing rollover lock: both operations replace the same
        # state file and append into the same session journal namespace.
        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (
        MonitorScanLedgerError,
        OSError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        result.update(
            {
                "accepted": False,
                "action": "retry_required" if state_replace_succeeded else "abstain",
                "state_updated": state_replace_succeeded,
                "commit_phase": (
                    "post_state_replace_uncertain"
                    if state_replace_succeeded
                    else result.get("commit_phase", "not_started")
                ),
                "issues": [
                    str(exc)
                    if isinstance(exc, MonitorScanLedgerError)
                    else f"scan_commit_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


__all__ = [
    "COMMIT_RECEIPT_EVENT_SCHEMA",
    "COMMIT_RECEIPT_EVENT_TYPE",
    "MonitorScanLedgerError",
    "NOTIFICATION_ACK_EVENT_SCHEMA",
    "NOTIFICATION_ACK_EVENT_TYPE",
    "RESULT_SCHEMA",
    "SCAN_EVENT_SCHEMA",
    "SCAN_WRAPPER_SCHEMA",
    "commit_monitor_scan",
    "policy_input_sha256",
    "scan_event_id",
    "validate_scan_wrapper",
    "with_scan_event_id",
    "with_policy_input_hashes",
]
