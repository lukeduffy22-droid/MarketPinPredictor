"""Durable delivery boundary for receipt-backed data-quality notifications.

The scan ledger owns substantive scans and cadence decisions.  This module is
deliberately separate from the policy notification outbox: it may project only
``cadence_decision.new_data_quality_event_ids`` from already reflected scan
commit receipts.  Notification wrappers and acknowledgements are appended and
fsynced before their projection replaces ``state.data_quality_notification_outbox``.

Sessions which predate this owner are an explicit compatibility boundary.  An
absent namespace is never auto-armed and historical cadence decisions are
never retroactively turned into pending notifications.  Session rollover arms
the namespace for the new target session.
"""

from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.monitor_scan_ledger import (
    COMMIT_RECEIPT_EVENT_TYPE,
    NOTIFICATION_ACK_EVENT_TYPE,
    MonitorScanLedgerError,
    SCAN_WRAPPER_SCHEMA,
    _append_jsonl_durable,
    _atomic_write_json,
    _canonical_hash,
    _canonical_json_bytes,
    _canonical_session_date,
    _exclusive_lock,
    _is_sha256,
    _journal_index,
    _load_state,
    _parsed_aware,
    _read_journal_exact_retry_snapshot,
    _revalidate_expected_journal,
    _truncate_exact_partial_tail,
    _validated_ledger_state,
    _validate_notification_delivery_proof,
    _UTC,
)


OUTBOX_STATE_SCHEMA = "marketpin-monitor-data-quality-notification-outbox.v1"
NOTIFICATION_EVENT_SCHEMA = "marketpin-monitor-data-quality-notification.v1"
NOTIFICATION_EVENT_TYPE = "data_quality_notification"
ACK_EVENT_SCHEMA = "marketpin-monitor-data-quality-notification-ack.v1"
ACK_EVENT_TYPE = "data_quality_notification_ack"
SYNC_RESULT_SCHEMA = "marketpin-monitor-data-quality-notification-sync.result.v1"
ACK_RESULT_SCHEMA = "marketpin-monitor-data-quality-notification-ack.result.v1"

ARMED = "armed_at_session_rollover"
LEGACY_PRE_ACTIVATION = "legacy_pre_activation"

_CHICAGO = ZoneInfo("America/Chicago")
_MONITORED_SYMBOLS = {"SPX", "NDX"}
_VALID_MODES = {"NORMAL", "ELEVATED"}
_COMMON_WRAPPER_FIELDS = {
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
_NOTIFICATION_FIELDS = _COMMON_WRAPPER_FIELDS | {
    "parent_scan_event_id",
    "parent_scan_sha256",
    "commit_receipt_event_id",
    "commit_receipt_sha256",
    "data_quality_event_id",
    "data_quality_descriptor",
    "data_quality_descriptor_sha256",
}
_ACK_FIELDS = _COMMON_WRAPPER_FIELDS | {
    "acked_notifications",
    "delivery_proof",
    "pending_notification_event_ids_before",
    "pending_notification_event_ids_after",
    "previous_ack",
    "scan_commit_anchor",
}
_OUTBOX_FIELDS = {
    "schema_version",
    "session_date",
    "activation_status",
    "activation_rollover_event_id",
    "last_processed_commit_receipt_event_id",
    "last_processed_commit_receipt_sha256",
    "source_data_quality_event_ids",
    "committed_notification_event_ids",
    "committed_notification_record_sha256",
    "pending_notification_event_ids",
    "committed_notification_ack_event_ids",
    "last_notification_event_id",
    "last_notification_sha256",
    "last_notification_ack_event_id",
    "last_notification_ack_sha256",
}


class MonitorDataQualityOutboxError(RuntimeError):
    """A fail-closed data-quality notification projection error."""


def legacy_pre_activation_outbox(session_date: str) -> dict[str, Any]:
    """Return the explicit, immutable marker for a session predating activation."""

    session = _canonical_session_date(session_date)
    return {
        "schema_version": OUTBOX_STATE_SCHEMA,
        "session_date": session,
        "activation_status": LEGACY_PRE_ACTIVATION,
        "activation_rollover_event_id": None,
        "last_processed_commit_receipt_event_id": None,
        "last_processed_commit_receipt_sha256": None,
        "source_data_quality_event_ids": [],
        "committed_notification_event_ids": [],
        "committed_notification_record_sha256": {},
        "pending_notification_event_ids": [],
        "committed_notification_ack_event_ids": [],
        "last_notification_event_id": None,
        "last_notification_sha256": None,
        "last_notification_ack_event_id": None,
        "last_notification_ack_sha256": None,
    }


def armed_outbox(session_date: str, rollover_event_id: str) -> dict[str, Any]:
    """Return a clean outbox armed by the target session's rollover receipt."""

    session = _canonical_session_date(session_date)
    if not isinstance(rollover_event_id, str) or not rollover_event_id:
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_activation_rollover_event_id_invalid"
        )
    result = legacy_pre_activation_outbox(session)
    result.update(
        {
            "activation_status": ARMED,
            "activation_rollover_event_id": rollover_event_id,
        }
    )
    return result


def _base_sync_result(session_date: Any = None) -> dict[str, Any]:
    return {
        "schema_version": SYNC_RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "event_ids_appended": [],
        "event_ids_recovered": [],
        # None means source/state authority was not validated.  Once it is,
        # every outcome returns the complete pending set, never merely deltas.
        "event_ids_pending_notification": None,
        "last_processed_commit_receipt_event_id": None,
        "partial_journal_tail_recovered": False,
        "state_updated": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def _base_ack_result(session_date: Any = None, event_ids: Any = None) -> dict[str, Any]:
    requested = list(event_ids) if isinstance(event_ids, list) else []
    return {
        "schema_version": ACK_RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "ack_event_id": None,
        "ack_appended": False,
        "requested_event_ids": requested,
        "event_ids_acknowledged": [],
        "event_ids_pending_notification": None,
        "partial_journal_tail_recovered": False,
        "state_updated": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def _normalized_observed_at(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MonitorDataQualityOutboxError(
            "observed_at_utc_must_be_timezone_aware"
        )
    return value.astimezone(_UTC)


def _normalized_event_ids(event_ids: Sequence[str]) -> list[str]:
    if isinstance(event_ids, (str, bytes)) or not isinstance(event_ids, Sequence):
        raise MonitorDataQualityOutboxError("event_ids_must_be_an_array")
    normalized = list(event_ids)
    if (
        not normalized
        or any(not _is_sha256(event_id) for event_id in normalized)
        or len(set(normalized)) != len(normalized)
    ):
        raise MonitorDataQualityOutboxError(
            "event_ids_must_be_unique_canonical_sha256_values"
        )
    return normalized


def _validate_common_wrapper(
    record: Mapping[str, Any], *, allow_after_session: bool = False
) -> None:
    if record.get("schema_version") != SCAN_WRAPPER_SCHEMA:
        raise MonitorDataQualityOutboxError("wrapper_schema_version_must_equal_2")
    if not _is_sha256(record.get("event_id")):
        raise MonitorDataQualityOutboxError("wrapper_event_id_invalid")
    session = _canonical_session_date(record.get("session_date"))
    observed_utc = _parsed_aware(
        record.get("observed_at_utc"), "data_quality_wrapper.observed_at_utc"
    )
    observed_ct = _parsed_aware(
        record.get("observed_at_ct"), "data_quality_wrapper.observed_at_ct"
    )
    if observed_utc.utcoffset() != _UTC.utcoffset(observed_utc):
        raise MonitorDataQualityOutboxError("wrapper_observed_at_utc_must_be_utc")
    chicago = observed_ct.astimezone(_CHICAGO)
    if observed_ct.utcoffset() != chicago.utcoffset():
        raise MonitorDataQualityOutboxError(
            "wrapper_observed_at_ct_must_use_chicago_offset"
        )
    if observed_ct.astimezone(_UTC) != observed_utc.astimezone(_UTC):
        raise MonitorDataQualityOutboxError("wrapper_observed_time_mismatch")
    observed_session = chicago.date().isoformat()
    if observed_session != session and (
        not allow_after_session or observed_session < session
    ):
        raise MonitorDataQualityOutboxError("wrapper_observed_session_mismatch")
    if not isinstance(record.get("phase"), str) or not record["phase"]:
        raise MonitorDataQualityOutboxError("wrapper_phase_invalid")
    if not isinstance(record.get("cadence"), Mapping):
        raise MonitorDataQualityOutboxError("wrapper_cadence_invalid")
    if not isinstance(record.get("evidence"), Mapping):
        raise MonitorDataQualityOutboxError("wrapper_evidence_invalid")
    if not isinstance(record.get("symbols"), Mapping):
        raise MonitorDataQualityOutboxError("wrapper_symbols_invalid")
    if not isinstance(record.get("alerts"), list):
        raise MonitorDataQualityOutboxError("wrapper_alerts_invalid")
    if not isinstance(record.get("directional_interpretation"), str):
        raise MonitorDataQualityOutboxError(
            "wrapper_directional_interpretation_invalid"
        )
    if not isinstance(record.get("research_hypotheses"), list):
        raise MonitorDataQualityOutboxError("wrapper_research_hypotheses_invalid")
    _canonical_json_bytes(record)


def _validate_descriptor(
    value: Any, *, event_id: str, session_date: str
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise MonitorDataQualityOutboxError(
            "data_quality_descriptor_must_be_an_object"
        )
    descriptor = copy.deepcopy(dict(value))
    if descriptor.get("event_id") != event_id or not _is_sha256(event_id):
        raise MonitorDataQualityOutboxError("data_quality_descriptor_event_id_invalid")
    issues = descriptor.get("issues")
    if (
        not isinstance(issues, list)
        or not issues
        or any(not isinstance(issue, str) or not issue for issue in issues)
        or issues != sorted(set(issues))
    ):
        raise MonitorDataQualityOutboxError("data_quality_descriptor_issues_invalid")
    symbol = descriptor.get("symbol")
    scope = descriptor.get("scope")
    if (symbol is None) == (scope is None):
        raise MonitorDataQualityOutboxError(
            "data_quality_descriptor_requires_exactly_one_symbol_or_scope"
        )
    if symbol is not None and symbol not in _MONITORED_SYMBOLS:
        raise MonitorDataQualityOutboxError("data_quality_descriptor_symbol_invalid")
    if scope is not None and (not isinstance(scope, str) or not scope):
        raise MonitorDataQualityOutboxError("data_quality_descriptor_scope_invalid")
    if "session_date" in descriptor and descriptor.get("session_date") != session_date:
        raise MonitorDataQualityOutboxError(
            "data_quality_descriptor_session_date_mismatch"
        )
    if "helper_event" in descriptor or any(
        key.startswith("policy_") for key in descriptor
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_descriptor_policy_shape_forbidden"
        )
    _canonical_json_bytes(descriptor)
    return descriptor


def _notification_event_id(record: Mapping[str, Any]) -> str:
    material = copy.deepcopy(dict(record))
    material.pop("event_id", None)
    return _canonical_hash(
        {"event_schema": NOTIFICATION_EVENT_SCHEMA, "notification": material}
    )


def _build_notification(
    *,
    scan: Mapping[str, Any],
    commit_receipt: Mapping[str, Any],
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_descriptor = _validate_descriptor(
        descriptor,
        event_id=str(descriptor.get("event_id") or ""),
        session_date=str(scan.get("session_date") or ""),
    )
    descriptor_hash = _canonical_hash(normalized_descriptor)
    cadence = scan.get("cadence")
    if not isinstance(cadence, Mapping) or cadence.get("mode") not in _VALID_MODES:
        raise MonitorDataQualityOutboxError(
            "data_quality_source_scan_cadence_invalid"
        )
    record = {
        "schema_version": SCAN_WRAPPER_SCHEMA,
        "event_type": NOTIFICATION_EVENT_TYPE,
        "observed_at_ct": scan["observed_at_ct"],
        "observed_at_utc": scan["observed_at_utc"],
        "session_date": scan["session_date"],
        "phase": scan["phase"],
        "cadence": {
            "mode": cadence["mode"],
            "substantive": False,
            "reason": "receipt_backed_new_data_quality_event",
        },
        "evidence": {
            "source": "cadence_decision.new_data_quality_event_ids",
            "data_quality_descriptor_sha256": descriptor_hash,
        },
        "symbols": {},
        "alerts": ["DATA_QUALITY"],
        "directional_interpretation": "DATA_QUALITY_ONLY",
        "research_hypotheses": [],
        "parent_scan_event_id": scan["event_id"],
        "parent_scan_sha256": _canonical_hash(scan),
        "commit_receipt_event_id": commit_receipt["event_id"],
        "commit_receipt_sha256": _canonical_hash(commit_receipt),
        "data_quality_event_id": normalized_descriptor["event_id"],
        "data_quality_descriptor": normalized_descriptor,
        "data_quality_descriptor_sha256": descriptor_hash,
    }
    record["event_id"] = _notification_event_id(record)
    return _validate_notification(record)


def _validate_notification(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping) or set(record) != _NOTIFICATION_FIELDS:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_fields_invalid"
        )
    normalized = copy.deepcopy(dict(record))
    _validate_common_wrapper(normalized)
    if normalized.get("event_type") != NOTIFICATION_EVENT_TYPE:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_event_type_invalid"
        )
    if normalized.get("alerts") != ["DATA_QUALITY"]:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_alerts_invalid"
        )
    if normalized.get("directional_interpretation") != "DATA_QUALITY_ONLY":
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_directional_interpretation_invalid"
        )
    if normalized.get("symbols") != {} or normalized.get("research_hypotheses") != []:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_must_not_carry_market_signal"
        )
    cadence = normalized.get("cadence")
    if cadence != {
        "mode": cadence.get("mode") if isinstance(cadence, Mapping) else None,
        "substantive": False,
        "reason": "receipt_backed_new_data_quality_event",
    } or cadence.get("mode") not in _VALID_MODES:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_cadence_invalid"
        )
    descriptor = _validate_descriptor(
        normalized.get("data_quality_descriptor"),
        event_id=str(normalized.get("data_quality_event_id") or ""),
        session_date=str(normalized.get("session_date") or ""),
    )
    descriptor_hash = _canonical_hash(descriptor)
    if (
        normalized.get("data_quality_descriptor_sha256") != descriptor_hash
        or normalized.get("evidence")
        != {
            "source": "cadence_decision.new_data_quality_event_ids",
            "data_quality_descriptor_sha256": descriptor_hash,
        }
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_descriptor_hash_mismatch"
        )
    for field in (
        "parent_scan_event_id",
        "parent_scan_sha256",
        "commit_receipt_event_id",
        "commit_receipt_sha256",
    ):
        if not _is_sha256(normalized.get(field)):
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_source_anchor_invalid"
            )
    if normalized["event_id"] != _notification_event_id(normalized):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_event_id_mismatch"
        )
    return normalized


def _notification_commitment(record: Mapping[str, Any]) -> dict[str, Any]:
    notification = _validate_notification(record)
    return {
        "event_id": notification["event_id"],
        "record_sha256": _canonical_hash(notification),
        "data_quality_event_id": notification["data_quality_event_id"],
        "data_quality_descriptor_sha256": notification[
            "data_quality_descriptor_sha256"
        ],
        "parent_scan_event_id": notification["parent_scan_event_id"],
        "parent_scan_sha256": notification["parent_scan_sha256"],
        "commit_receipt_event_id": notification["commit_receipt_event_id"],
        "commit_receipt_sha256": notification["commit_receipt_sha256"],
    }


def _ack_event_id(record: Mapping[str, Any]) -> str:
    material = copy.deepcopy(dict(record))
    material.pop("event_id", None)
    return _canonical_hash({"event_schema": ACK_EVENT_SCHEMA, "ack": material})


def _build_ack(
    *,
    session_date: str,
    observed_at_utc: datetime,
    state_mode: str,
    notifications: Sequence[Mapping[str, Any]],
    delivery_proof: Mapping[str, Any],
    pending_before: Sequence[str],
    pending_after: Sequence[str],
    previous_ack: Mapping[str, Any] | None,
    scan_commit_anchor: Mapping[str, Any],
) -> dict[str, Any]:
    observed_utc = observed_at_utc.astimezone(_UTC)
    proof = _validate_notification_delivery_proof(delivery_proof)
    record = {
        "schema_version": SCAN_WRAPPER_SCHEMA,
        "event_type": ACK_EVENT_TYPE,
        "observed_at_ct": observed_utc.astimezone(_CHICAGO).isoformat(),
        "observed_at_utc": observed_utc.isoformat().replace("+00:00", "Z"),
        "session_date": session_date,
        "phase": "data_quality_notification_ack",
        "cadence": {
            "mode": state_mode,
            "substantive": False,
            "reason": "prior_heartbeat_delivery_proven",
        },
        "evidence": {"delivery_proof_sha256": _canonical_hash(proof)},
        "symbols": {},
        "alerts": [],
        "directional_interpretation": "DATA_QUALITY_NOTIFICATION_ACK",
        "research_hypotheses": [],
        "acked_notifications": [
            _notification_commitment(notification)
            for notification in notifications
        ],
        "delivery_proof": proof,
        "pending_notification_event_ids_before": list(pending_before),
        "pending_notification_event_ids_after": list(pending_after),
        "previous_ack": (
            copy.deepcopy(dict(previous_ack)) if previous_ack is not None else None
        ),
        "scan_commit_anchor": copy.deepcopy(dict(scan_commit_anchor)),
    }
    record["event_id"] = _ack_event_id(record)
    return _validate_ack(record)


def _validate_ack(record: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(record, Mapping) or set(record) != _ACK_FIELDS:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_fields_invalid"
        )
    normalized = copy.deepcopy(dict(record))
    _validate_common_wrapper(normalized, allow_after_session=True)
    if normalized.get("event_type") != ACK_EVENT_TYPE:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_event_type_invalid"
        )
    if normalized.get("phase") != "data_quality_notification_ack":
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_phase_invalid"
        )
    cadence = normalized.get("cadence")
    if cadence != {
        "mode": cadence.get("mode") if isinstance(cadence, Mapping) else None,
        "substantive": False,
        "reason": "prior_heartbeat_delivery_proven",
    } or cadence.get("mode") not in _VALID_MODES:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_cadence_invalid"
        )
    if (
        normalized.get("symbols") != {}
        or normalized.get("alerts") != []
        or normalized.get("research_hypotheses") != []
        or normalized.get("directional_interpretation")
        != "DATA_QUALITY_NOTIFICATION_ACK"
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_must_not_carry_market_signal"
        )
    proof = _validate_notification_delivery_proof(normalized.get("delivery_proof"))
    if normalized.get("evidence") != {
        "delivery_proof_sha256": _canonical_hash(proof)
    }:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_delivery_proof_hash_mismatch"
        )
    delivered = _parsed_aware(
        proof["prior_final_delivered_at_utc"],
        "data_quality_notification_ack.prior_final_delivered_at_utc",
    ).astimezone(_UTC)
    acknowledged = _parsed_aware(
        normalized.get("observed_at_utc"),
        "data_quality_notification_ack.observed_at_utc",
    ).astimezone(_UTC)
    if delivered >= acknowledged:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_must_follow_prior_final_delivery"
        )
    before = normalized.get("pending_notification_event_ids_before")
    after = normalized.get("pending_notification_event_ids_after")
    if (
        not isinstance(before, list)
        or not isinstance(after, list)
        or any(not _is_sha256(value) for value in before + after)
        or len(set(before)) != len(before)
        or len(set(after)) != len(after)
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_pending_ids_invalid"
        )
    commitments = normalized.get("acked_notifications")
    if not isinstance(commitments, list) or not commitments:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_commitments_invalid"
        )
    commitment_fields = {
        "event_id",
        "record_sha256",
        "data_quality_event_id",
        "data_quality_descriptor_sha256",
        "parent_scan_event_id",
        "parent_scan_sha256",
        "commit_receipt_event_id",
        "commit_receipt_sha256",
    }
    acked_ids: list[str] = []
    for commitment in commitments:
        if (
            not isinstance(commitment, Mapping)
            or set(commitment) != commitment_fields
            or any(not _is_sha256(commitment.get(field)) for field in commitment_fields)
        ):
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_commitment_invalid"
            )
        acked_ids.append(str(commitment["event_id"]))
    if len(set(acked_ids)) != len(acked_ids):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_duplicate_event_id"
        )
    if acked_ids != [value for value in before if value in set(acked_ids)]:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_event_order_invalid"
        )
    if after != [value for value in before if value not in set(acked_ids)]:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_pending_transition_invalid"
        )
    for field in ("scan_commit_anchor", "previous_ack"):
        anchor = normalized.get(field)
        if anchor is None and field == "previous_ack":
            continue
        if (
            not isinstance(anchor, Mapping)
            or set(anchor) != {"event_id", "record_sha256"}
            or not all(_is_sha256(anchor.get(key)) for key in anchor)
        ):
            raise MonitorDataQualityOutboxError(
                f"data_quality_notification_ack_{field}_invalid"
            )
    if normalized["event_id"] != _ack_event_id(normalized):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_event_id_mismatch"
        )
    return normalized


def _scan_transaction_issue(
    *, by_id: Mapping[str, Mapping[str, Any]], ledger: Mapping[str, Any]
) -> str | None:
    committed_scans = set(ledger.get("committed_scan_event_ids", []))
    committed_policy = set(ledger.get("committed_policy_event_ids", []))
    committed_receipts = set(ledger.get("committed_receipt_event_ids", []))
    committed_policy_acks = set(
        ledger.get("committed_notification_ack_event_ids", [])
    )
    for event_id, record in by_id.items():
        if record.get("event_type") == "substantive_scan" and event_id not in committed_scans:
            return "unreflected_substantive_scan_requires_exact_retry:" + event_id
        if isinstance(record.get("helper_event"), Mapping) and event_id not in committed_policy:
            return "unreflected_policy_wrapper_requires_scan_retry:" + event_id
        if record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE and event_id not in committed_receipts:
            return "unreflected_scan_commit_requires_exact_retry:" + event_id
        if record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE and event_id not in committed_policy_acks:
            return "unreflected_policy_ack_requires_exact_retry:" + event_id
    return None


def _source_notifications(
    *,
    state: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    ledger: Mapping[str, Any],
    activation_rollover_event_id: str,
) -> tuple[list[dict[str, Any]], Mapping[str, Any] | None]:
    positions = {
        str(record.get("event_id")): index
        for index, record in enumerate(records)
        if isinstance(record.get("event_id"), str)
    }
    activation = by_id.get(activation_rollover_event_id)
    if (
        not isinstance(activation, Mapping)
        or activation.get("event_type") != "session_rollover"
        or activation.get("session_date") != state.get("session_date")
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_activation_rollover_receipt_missing"
        )
    activation_position = positions[activation_rollover_event_id]
    expected: list[dict[str, Any]] = []
    seen_source_ids: set[str] = set()
    latest_commit: Mapping[str, Any] | None = None
    for receipt_id in ledger.get("committed_receipt_event_ids", []):
        receipt = by_id.get(str(receipt_id))
        if not isinstance(receipt, Mapping):
            raise MonitorDataQualityOutboxError(
                "data_quality_source_commit_receipt_missing"
            )
        if positions[str(receipt_id)] <= activation_position:
            raise MonitorDataQualityOutboxError(
                "data_quality_source_commit_precedes_activation"
            )
        latest_commit = receipt
        scan = by_id.get(str(receipt.get("parent_scan_event_id") or ""))
        if not isinstance(scan, Mapping) or scan.get("event_type") != "substantive_scan":
            raise MonitorDataQualityOutboxError(
                "data_quality_source_parent_scan_missing"
            )
        decision = receipt.get("cadence_decision")
        if not isinstance(decision, Mapping) or decision.get("evidence_mode") != "explicit":
            raise MonitorDataQualityOutboxError(
                "data_quality_source_requires_explicit_cadence_receipt"
            )
        active_ids = decision.get("active_data_quality_event_ids")
        new_ids = decision.get("new_data_quality_event_ids")
        if not isinstance(active_ids, list) or not isinstance(new_ids, list):
            raise MonitorDataQualityOutboxError(
                "data_quality_source_event_ids_invalid"
            )
        if new_ids != sorted(set(new_ids)):
            raise MonitorDataQualityOutboxError(
                "data_quality_source_new_event_ids_order_or_uniqueness_invalid"
            )
        evidence = scan.get("evidence")
        descriptors_raw = (
            evidence.get("data_quality_events")
            if isinstance(evidence, Mapping)
            else None
        )
        if descriptors_raw is None and not active_ids and not new_ids:
            # Transitional explicit-cadence fixtures and older collectors may
            # omit the diagnostic array only when the receipt proves that no
            # DQ condition was active or new.
            descriptors_raw = []
        if not isinstance(descriptors_raw, list):
            raise MonitorDataQualityOutboxError(
                "data_quality_source_descriptor_array_missing"
            )
        descriptors: dict[str, dict[str, Any]] = {}
        for raw in descriptors_raw:
            if not isinstance(raw, Mapping):
                raise MonitorDataQualityOutboxError(
                    "data_quality_source_descriptor_invalid"
                )
            event_id = str(raw.get("event_id") or "")
            descriptor = _validate_descriptor(
                raw,
                event_id=event_id,
                session_date=str(scan.get("session_date") or ""),
            )
            if event_id in descriptors:
                raise MonitorDataQualityOutboxError(
                    "data_quality_source_descriptor_duplicate:" + event_id
                )
            descriptors[event_id] = descriptor
        if not isinstance(active_ids, list) or set(active_ids) != set(descriptors):
            raise MonitorDataQualityOutboxError(
                "data_quality_source_active_descriptor_set_mismatch"
            )
        for source_id in new_ids:
            if not _is_sha256(source_id) or source_id not in descriptors:
                raise MonitorDataQualityOutboxError(
                    "data_quality_source_new_descriptor_missing:" + str(source_id)
                )
            if source_id in seen_source_ids:
                raise MonitorDataQualityOutboxError(
                    "data_quality_source_new_event_repeated:" + source_id
                )
            seen_source_ids.add(source_id)
            expected.append(
                _build_notification(
                    scan=scan,
                    commit_receipt=receipt,
                    descriptor=descriptors[source_id],
                )
            )
    return expected, latest_commit


def _validate_ack_causal_order(
    ack: Mapping[str, Any],
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    notifications_by_id: Mapping[str, Mapping[str, Any]],
    latest_commit: Mapping[str, Any],
) -> None:
    normalized = _validate_ack(ack)
    anchor = normalized["scan_commit_anchor"]
    if (
        anchor.get("event_id") != latest_commit.get("event_id")
        or anchor.get("record_sha256") != _canonical_hash(latest_commit)
    ):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_scan_anchor_mismatch"
        )
    parent = by_id.get(str(latest_commit.get("parent_scan_event_id") or ""))
    if not isinstance(parent, Mapping):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_scan_anchor_mismatch"
        )
    delivered = _parsed_aware(
        normalized["delivery_proof"]["prior_final_delivered_at_utc"],
        "data_quality_notification_ack.prior_final_delivered_at_utc",
    ).astimezone(_UTC)
    acknowledged = _parsed_aware(
        normalized["observed_at_utc"],
        "data_quality_notification_ack.observed_at_utc",
    ).astimezone(_UTC)
    latest_scan_at = _parsed_aware(
        parent.get("observed_at_utc"),
        "data_quality_notification_ack.scan_anchor_observed_at_utc",
    ).astimezone(_UTC)
    if acknowledged <= latest_scan_at:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_ack_must_follow_scan_commit_anchor"
        )
    for commitment in normalized["acked_notifications"]:
        event_id = str(commitment["event_id"])
        notification = notifications_by_id.get(event_id)
        if (
            not isinstance(notification, Mapping)
            or commitment != _notification_commitment(notification)
        ):
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_commitment_mismatch:" + event_id
            )
        source_scan = by_id.get(str(notification["parent_scan_event_id"]))
        source_commit = by_id.get(str(notification["commit_receipt_event_id"]))
        if (
            not isinstance(source_scan, Mapping)
            or not isinstance(source_commit, Mapping)
            or notification["parent_scan_sha256"] != _canonical_hash(source_scan)
            or notification["commit_receipt_sha256"] != _canonical_hash(source_commit)
        ):
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_source_anchor_mismatch:" + event_id
            )
        source_time = _parsed_aware(
            notification["observed_at_utc"],
            "data_quality_notification.observed_at_utc",
        ).astimezone(_UTC)
        if delivered < source_time:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_delivery_precedes_notification:"
                + event_id
            )


def _owned_journal_replay(
    *,
    records: Sequence[Mapping[str, Any]],
    by_id: Mapping[str, Mapping[str, Any]],
    expected_notifications: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    expected_ids = [str(record["event_id"]) for record in expected_notifications]
    expected_by_id = {
        str(record["event_id"]): record for record in expected_notifications
    }
    journal_notifications: list[dict[str, Any]] = []
    journal_acks: list[dict[str, Any]] = []
    pending: list[str] = []
    previous_ack: Mapping[str, Any] | None = None
    latest_commit: Mapping[str, Any] | None = None
    notification_positions: dict[str, int] = {}
    positions = {
        str(record.get("event_id")): index
        for index, record in enumerate(records)
        if isinstance(record.get("event_id"), str)
    }
    for position, raw in enumerate(records):
        event_type = raw.get("event_type")
        if event_type == COMMIT_RECEIPT_EVENT_TYPE:
            latest_commit = raw
            continue
        if event_type == NOTIFICATION_EVENT_TYPE:
            notification = _validate_notification(raw)
            event_id = str(notification["event_id"])
            expected = expected_by_id.get(event_id)
            if expected is None or _canonical_json_bytes(notification) != _canonical_json_bytes(expected):
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_not_exact_source_projection:"
                    + event_id
                )
            parent_position = positions.get(
                str(notification["parent_scan_event_id"])
            )
            commit_position = positions.get(
                str(notification["commit_receipt_event_id"])
            )
            if (
                parent_position is None
                or commit_position is None
                or not parent_position < commit_position < position
            ):
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_must_follow_parent_and_commit_receipt:"
                    + event_id
                )
            journal_notifications.append(notification)
            notification_positions[event_id] = position
            pending.append(event_id)
            continue
        if event_type != ACK_EVENT_TYPE:
            continue
        ack = _validate_ack(raw)
        ack_id = str(ack["event_id"])
        if latest_commit is None:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_without_scan_commit"
            )
        notifications_by_id = {
            str(record["event_id"]): record for record in journal_notifications
        }
        _validate_ack_causal_order(
            ack,
            by_id=by_id,
            notifications_by_id=notifications_by_id,
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
        if ack.get("previous_ack") != expected_previous:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_chain_mismatch"
            )
        if ack["pending_notification_event_ids_before"] != pending:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_pending_before_mismatch"
            )
        for commitment in ack["acked_notifications"]:
            event_id = str(commitment["event_id"])
            if notification_positions.get(event_id, position) >= position:
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_ack_precedes_notification:"
                    + event_id
                )
        pending = list(ack["pending_notification_event_ids_after"])
        journal_acks.append(ack)
        previous_ack = ack
    journal_ids = [str(record["event_id"]) for record in journal_notifications]
    if journal_ids != expected_ids[: len(journal_ids)]:
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_journal_not_exact_source_prefix"
        )
    return {
        "notifications": journal_notifications,
        "acks": journal_acks,
        "pending": pending,
    }


def _state_for_prefix(
    *,
    session_date: str,
    activation_rollover_event_id: str,
    records: Sequence[Mapping[str, Any]],
    notification_ids: Sequence[str],
    ack_ids: Sequence[str],
    processed_commit_receipt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    notification_set = set(notification_ids)
    ack_set = set(ack_ids)
    notifications: list[Mapping[str, Any]] = []
    acks: list[Mapping[str, Any]] = []
    pending: list[str] = []
    for record in records:
        event_id = str(record.get("event_id") or "")
        if record.get("event_type") == NOTIFICATION_EVENT_TYPE and event_id in notification_set:
            notifications.append(record)
            pending.append(event_id)
        elif record.get("event_type") == ACK_EVENT_TYPE and event_id in ack_set:
            if record.get("pending_notification_event_ids_before") != pending:
                raise MonitorDataQualityOutboxError(
                    "data_quality_outbox_state_ack_replay_mismatch"
                )
            pending = list(record["pending_notification_event_ids_after"])
            acks.append(record)
    observed_notification_ids = [str(record["event_id"]) for record in notifications]
    observed_ack_ids = [str(record["event_id"]) for record in acks]
    if observed_notification_ids != list(notification_ids):
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_state_notification_order_invalid"
        )
    if observed_ack_ids != list(ack_ids):
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_state_ack_order_invalid"
        )
    return {
        "schema_version": OUTBOX_STATE_SCHEMA,
        "session_date": session_date,
        "activation_status": ARMED,
        "activation_rollover_event_id": activation_rollover_event_id,
        "last_processed_commit_receipt_event_id": (
            processed_commit_receipt.get("event_id")
            if processed_commit_receipt is not None
            else None
        ),
        "last_processed_commit_receipt_sha256": (
            _canonical_hash(processed_commit_receipt)
            if processed_commit_receipt is not None
            else None
        ),
        "source_data_quality_event_ids": [
            str(record["data_quality_event_id"]) for record in notifications
        ],
        "committed_notification_event_ids": observed_notification_ids,
        "committed_notification_record_sha256": {
            str(record["event_id"]): _canonical_hash(record)
            for record in notifications
        },
        "pending_notification_event_ids": pending,
        "committed_notification_ack_event_ids": observed_ack_ids,
        "last_notification_event_id": (
            observed_notification_ids[-1] if observed_notification_ids else None
        ),
        "last_notification_sha256": (
            _canonical_hash(notifications[-1]) if notifications else None
        ),
        "last_notification_ack_event_id": (
            observed_ack_ids[-1] if observed_ack_ids else None
        ),
        "last_notification_ack_sha256": (
            _canonical_hash(acks[-1]) if acks else None
        ),
    }


def _validate_outbox_state(
    raw: Any,
    *,
    session_date: str,
    activation_rollover_event_id: str,
    records: Sequence[Mapping[str, Any]],
    replay: Mapping[str, Any],
    by_id: Mapping[str, Mapping[str, Any]],
    ledger: Mapping[str, Any],
    expected_notifications: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != _OUTBOX_FIELDS:
        raise MonitorDataQualityOutboxError("data_quality_outbox_state_fields_invalid")
    normalized = copy.deepcopy(dict(raw))
    if (
        normalized.get("schema_version") != OUTBOX_STATE_SCHEMA
        or normalized.get("session_date") != session_date
        or normalized.get("activation_status") != ARMED
        or normalized.get("activation_rollover_event_id")
        != activation_rollover_event_id
    ):
        raise MonitorDataQualityOutboxError("data_quality_outbox_state_identity_invalid")
    notification_ids = normalized.get("committed_notification_event_ids")
    ack_ids = normalized.get("committed_notification_ack_event_ids")
    if (
        not isinstance(notification_ids, list)
        or any(not _is_sha256(value) for value in notification_ids)
        or len(set(notification_ids)) != len(notification_ids)
        or not isinstance(ack_ids, list)
        or any(not _is_sha256(value) for value in ack_ids)
        or len(set(ack_ids)) != len(ack_ids)
    ):
        raise MonitorDataQualityOutboxError("data_quality_outbox_state_ids_invalid")
    journal_notification_ids = [
        str(record["event_id"]) for record in replay["notifications"]
    ]
    journal_ack_ids = [str(record["event_id"]) for record in replay["acks"]]
    if notification_ids != journal_notification_ids[: len(notification_ids)]:
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_state_notification_not_journal_prefix"
        )
    if ack_ids != journal_ack_ids[: len(ack_ids)]:
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_state_ack_not_journal_prefix"
        )
    processed_id = normalized.get("last_processed_commit_receipt_event_id")
    processed_hash = normalized.get("last_processed_commit_receipt_sha256")
    receipt_ids = list(ledger.get("committed_receipt_event_ids", []))
    processed_receipt: Mapping[str, Any] | None = None
    if processed_id is None:
        if processed_hash is not None:
            raise MonitorDataQualityOutboxError(
                "data_quality_outbox_processed_commit_anchor_invalid"
            )
        processed_receipt_ids: set[str] = set()
    else:
        if (
            not _is_sha256(processed_id)
            or not _is_sha256(processed_hash)
            or processed_id not in receipt_ids
        ):
            raise MonitorDataQualityOutboxError(
                "data_quality_outbox_processed_commit_anchor_invalid"
            )
        processed_receipt = by_id.get(str(processed_id))
        if (
            not isinstance(processed_receipt, Mapping)
            or _canonical_hash(processed_receipt) != processed_hash
        ):
            raise MonitorDataQualityOutboxError(
                "data_quality_outbox_processed_commit_anchor_tampered"
            )
        processed_receipt_ids = set(
            receipt_ids[: receipt_ids.index(str(processed_id)) + 1]
        )
    expected_processed_notification_ids = [
        str(record["event_id"])
        for record in expected_notifications
        if record.get("commit_receipt_event_id") in processed_receipt_ids
    ]
    if notification_ids != expected_processed_notification_ids:
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_processed_commit_projection_mismatch"
        )
    expected = _state_for_prefix(
        session_date=session_date,
        activation_rollover_event_id=activation_rollover_event_id,
        records=records,
        notification_ids=notification_ids,
        ack_ids=ack_ids,
        processed_commit_receipt=processed_receipt,
    )
    if _canonical_json_bytes(normalized) != _canonical_json_bytes(expected):
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_state_replay_mismatch"
        )
    return normalized


def _context(
    *,
    state: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    journal_raw: bytes,
) -> dict[str, Any]:
    session = _canonical_session_date(state.get("session_date"))
    by_id, _policy_receipts = _journal_index(records)
    ledger = _validated_ledger_state(
        state.get("monitor_scan_ledger"), state, by_id, journal_raw
    )
    raw_outbox = state.get("data_quality_notification_outbox")
    owned_records = [
        record
        for record in records
        if record.get("event_type") in {NOTIFICATION_EVENT_TYPE, ACK_EVENT_TYPE}
    ]
    if raw_outbox is None:
        if owned_records:
            raise MonitorDataQualityOutboxError(
                "legacy_pre_activation_session_has_data_quality_outbox_records"
            )
        return {
            "session_date": session,
            "activation_status": LEGACY_PRE_ACTIVATION,
            "outbox": legacy_pre_activation_outbox(session),
            "records": records,
            "by_id": by_id,
            "ledger": ledger,
            "expected_notifications": [],
            "latest_commit": None,
            "replay": {"notifications": [], "acks": [], "pending": []},
        }
    if not isinstance(raw_outbox, Mapping):
        raise MonitorDataQualityOutboxError(
            "data_quality_notification_outbox_must_be_an_object"
        )
    if raw_outbox.get("activation_status") == LEGACY_PRE_ACTIVATION:
        expected_legacy = legacy_pre_activation_outbox(session)
        if (
            _canonical_json_bytes(raw_outbox) != _canonical_json_bytes(expected_legacy)
            or owned_records
        ):
            raise MonitorDataQualityOutboxError(
                "legacy_pre_activation_data_quality_outbox_invalid"
            )
        return {
            "session_date": session,
            "activation_status": LEGACY_PRE_ACTIVATION,
            "outbox": expected_legacy,
            "records": records,
            "by_id": by_id,
            "ledger": ledger,
            "expected_notifications": [],
            "latest_commit": None,
            "replay": {"notifications": [], "acks": [], "pending": []},
        }
    activation_id = raw_outbox.get("activation_rollover_event_id")
    if raw_outbox.get("activation_status") != ARMED or not isinstance(activation_id, str):
        raise MonitorDataQualityOutboxError(
            "data_quality_outbox_activation_status_invalid"
        )
    issue = _scan_transaction_issue(by_id=by_id, ledger=ledger)
    if issue:
        raise MonitorDataQualityOutboxError(issue)
    expected_notifications, latest_commit = _source_notifications(
        state=state,
        records=records,
        by_id=by_id,
        ledger=ledger,
        activation_rollover_event_id=activation_id,
    )
    replay = _owned_journal_replay(
        records=records,
        by_id=by_id,
        expected_notifications=expected_notifications,
    )
    outbox = _validate_outbox_state(
        raw_outbox,
        session_date=session,
        activation_rollover_event_id=activation_id,
        records=records,
        replay=replay,
        by_id=by_id,
        ledger=ledger,
        expected_notifications=expected_notifications,
    )
    return {
        "session_date": session,
        "activation_status": ARMED,
        "activation_rollover_event_id": activation_id,
        "outbox": outbox,
        "records": records,
        "by_id": by_id,
        "ledger": ledger,
        "expected_notifications": expected_notifications,
        "latest_commit": latest_commit,
        "replay": replay,
    }


def sync_data_quality_notifications(
    *,
    session_date: str,
    state_path: Path,
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Append every missing receipt-backed DQ wrapper, then reflect state."""

    result = _base_sync_result(session_date)
    state_replace_succeeded = False
    try:
        session = _canonical_session_date(session_date)
        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()
        journal_path = journal_dir / f"{session}.jsonl"
        result["journal_path"] = str(journal_path)

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            if state.get("session_date") != session:
                raise MonitorDataQualityOutboxError(
                    "data_quality_sync_state_session_mismatch"
                )
            (
                records,
                journal_raw,
                partial_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            context = _context(state=state, records=records, journal_raw=journal_raw)
            if context["activation_status"] == LEGACY_PRE_ACTIVATION:
                result.update(
                    {
                        "accepted": True,
                        "action": "legacy_pre_activation_noop",
                        "event_ids_pending_notification": [],
                        "commit_phase": "complete",
                        "issues": [],
                    }
                )
                return result

            replay = context["replay"]
            outbox = context["outbox"]
            latest_processed_commit = context["latest_commit"]
            result["last_processed_commit_receipt_event_id"] = (
                latest_processed_commit.get("event_id")
                if isinstance(latest_processed_commit, Mapping)
                else None
            )
            result["event_ids_pending_notification"] = list(
                outbox["pending_notification_event_ids"]
            )
            journal_ack_ids = [str(record["event_id"]) for record in replay["acks"]]
            state_ack_ids = list(outbox["committed_notification_ack_event_ids"])
            if journal_ack_ids != state_ack_ids:
                raise MonitorDataQualityOutboxError(
                    "unreflected_data_quality_notification_ack_requires_exact_retry:"
                    + journal_ack_ids[len(state_ack_ids)]
                )

            expected = context["expected_notifications"]
            journal_notifications = list(replay["notifications"])
            journal_ids = [str(record["event_id"]) for record in journal_notifications]
            state_ids = list(outbox["committed_notification_event_ids"])
            if state_ids != journal_ids[: len(state_ids)]:
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_state_not_journal_prefix"
                )
            ahead_ids = journal_ids[len(state_ids):]
            result["event_ids_recovered"] = list(ahead_ids)
            anticipated_records: list[Mapping[str, Any]] = list(records)
            anticipated_records.extend(expected[len(journal_notifications):])
            anticipated_outbox = _state_for_prefix(
                session_date=session,
                activation_rollover_event_id=context[
                    "activation_rollover_event_id"
                ],
                records=anticipated_records,
                notification_ids=[str(record["event_id"]) for record in expected],
                ack_ids=state_ack_ids,
                processed_commit_receipt=latest_processed_commit,
            )
            # The sync contract always surfaces the complete projected pending
            # set once source authority is validated, including when a later
            # append/state failpoint makes an exact retry necessary.
            result["event_ids_pending_notification"] = list(
                anticipated_outbox["pending_notification_event_ids"]
            )
            if partial_tail is not None:
                if len(journal_notifications) >= len(expected):
                    raise MonitorDataQualityOutboxError(
                        "data_quality_notification_partial_tail_has_no_owned_next_record"
                    )
                next_record = expected[len(journal_notifications)]
                next_bytes = _canonical_json_bytes(next_record)
                if not partial_tail or not next_bytes.startswith(partial_tail):
                    raise MonitorDataQualityOutboxError(
                        "data_quality_notification_partial_tail_not_exact_prefix"
                    )
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorDataQualityOutboxError(
                        "state_changed_during_data_quality_tail_recovery"
                    )
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True

            expected_records: list[Mapping[str, Any]] = list(records)
            for notification in expected[len(journal_notifications):]:
                _append_jsonl_durable(journal_path, notification)
                expected_records.append(notification)
                journal_notifications.append(notification)
                result["event_ids_appended"].append(notification["event_id"])
                result["commit_phase"] = "journal_durable"
                if failpoint is not None:
                    failpoint("after_notification_append")

            all_notification_ids = [
                str(record["event_id"]) for record in journal_notifications
            ]
            next_outbox = _state_for_prefix(
                session_date=session,
                activation_rollover_event_id=context[
                    "activation_rollover_event_id"
                ],
                records=expected_records,
                notification_ids=all_notification_ids,
                ack_ids=state_ack_ids,
                processed_commit_receipt=latest_processed_commit,
            )
            result["event_ids_pending_notification"] = list(
                next_outbox["pending_notification_event_ids"]
            )
            if (
                next_outbox == outbox
                and not result["partial_journal_tail_recovered"]
            ):
                result.update(
                    {
                        "accepted": True,
                        "action": "already_synchronized",
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result

            if failpoint is not None:
                failpoint("before_state_replace")
            if state_path.read_bytes() != original_state_bytes:
                raise MonitorDataQualityOutboxError(
                    "state_changed_during_data_quality_sync"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            next_state = copy.deepcopy(state)
            next_state["data_quality_notification_outbox"] = next_outbox
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted, _raw = _load_state(state_path)
                    state_replace_succeeded = persisted == next_state
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorDataQualityOutboxError(
                    "data_quality_sync_state_postcondition_failed"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_synchronized"
                        if ahead_ids or result["partial_journal_tail_recovered"]
                        else "synchronized"
                    ),
                    "state_updated": True,
                    "commit_phase": "committed",
                    "issues": [],
                }
            )
            return result

        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (
        MonitorDataQualityOutboxError,
        MonitorScanLedgerError,
        OSError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        durable = result.get("commit_phase") == "journal_durable"
        result.update(
            {
                "accepted": False,
                "action": (
                    "retry_required"
                    if state_replace_succeeded or durable
                    else "abstain"
                ),
                "state_updated": state_replace_succeeded,
                "commit_phase": (
                    "post_state_replace_uncertain"
                    if state_replace_succeeded
                    else result.get("commit_phase", "not_started")
                ),
                "issues": [
                    str(exc)
                    if isinstance(
                        exc,
                        (MonitorDataQualityOutboxError, MonitorScanLedgerError),
                    )
                    else f"data_quality_sync_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


def ack_data_quality_notifications(
    *,
    session_date: str,
    event_ids: Sequence[str],
    observed_at_utc: datetime,
    delivery_proof: Mapping[str, Any],
    state_path: Path,
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Acknowledge exact, prior-turn-delivered DQ notification wrappers."""

    result = _base_ack_result(
        session_date,
        list(event_ids)
        if isinstance(event_ids, Sequence) and not isinstance(event_ids, (str, bytes))
        else [],
    )
    state_replace_succeeded = False
    try:
        session = _canonical_session_date(session_date)
        requested_ids = _normalized_event_ids(event_ids)
        acknowledged_at = _normalized_observed_at(observed_at_utc)
        try:
            proof = _validate_notification_delivery_proof(delivery_proof)
        except MonitorScanLedgerError as exc:
            raise MonitorDataQualityOutboxError(str(exc)) from exc
        delivered_at = _parsed_aware(
            proof["prior_final_delivered_at_utc"],
            "data_quality_notification_ack.prior_final_delivered_at_utc",
        ).astimezone(_UTC)
        if delivered_at >= acknowledged_at:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_must_follow_prior_final_delivery"
            )
        if acknowledged_at.astimezone(_CHICAGO).date().isoformat() < session:
            raise MonitorDataQualityOutboxError(
                "data_quality_notification_ack_time_before_market_session"
            )
        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()
        journal_path = journal_dir / f"{session}.jsonl"
        result["journal_path"] = str(journal_path)

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            if state.get("session_date") != session:
                raise MonitorDataQualityOutboxError(
                    "data_quality_ack_state_session_mismatch"
                )
            state_mode = state.get("mode")
            if state_mode not in _VALID_MODES:
                raise MonitorDataQualityOutboxError(
                    "state_mode_must_be_NORMAL_or_ELEVATED"
                )
            (
                records,
                journal_raw,
                partial_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            context = _context(state=state, records=records, journal_raw=journal_raw)
            if context["activation_status"] != ARMED:
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_outbox_not_armed"
                )
            outbox = context["outbox"]
            replay = context["replay"]
            result["event_ids_pending_notification"] = list(
                outbox["pending_notification_event_ids"]
            )
            expected_ids = [
                str(record["event_id"])
                for record in context["expected_notifications"]
            ]
            journal_notification_ids = [
                str(record["event_id"]) for record in replay["notifications"]
            ]
            state_notification_ids = list(
                outbox["committed_notification_event_ids"]
            )
            if (
                journal_notification_ids != expected_ids
                or state_notification_ids != expected_ids
            ):
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_sync_required_before_ack"
                )
            committed_ack_ids = list(
                outbox["committed_notification_ack_event_ids"]
            )
            journal_acks = list(replay["acks"])
            if len(journal_acks) > len(committed_ack_ids) + 1:
                raise MonitorDataQualityOutboxError(
                    "multiple_unreflected_data_quality_notification_acks"
                )
            unreflected_ack = (
                journal_acks[len(committed_ack_ids)]
                if len(journal_acks) > len(committed_ack_ids)
                else None
            )
            pending_before = list(outbox["pending_notification_event_ids"])
            pending_set = set(requested_ids)
            matching_committed = [
                ack
                for ack in journal_acks[: len(committed_ack_ids)]
                if [
                    str(item["event_id"])
                    for item in ack["acked_notifications"]
                ]
                == requested_ids
                and ack.get("observed_at_utc")
                == acknowledged_at.isoformat().replace("+00:00", "Z")
                and _canonical_json_bytes(ack.get("delivery_proof"))
                == _canonical_json_bytes(proof)
            ]
            if len(matching_committed) > 1:
                raise MonitorDataQualityOutboxError(
                    "duplicate_exact_data_quality_notification_ack"
                )
            if matching_committed:
                if unreflected_ack is not None or partial_tail is not None:
                    raise MonitorDataQualityOutboxError(
                        "unreflected_data_quality_notification_ack_requires_exact_retry"
                    )
                result.update(
                    {
                        "accepted": True,
                        "action": "already_acknowledged",
                        "ack_event_id": matching_committed[0]["event_id"],
                        "event_ids_acknowledged": requested_ids,
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result
            if any(event_id not in pending_before for event_id in requested_ids):
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_ack_event_id_not_pending"
                )
            if requested_ids != [
                event_id for event_id in pending_before if event_id in pending_set
            ]:
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_ack_event_ids_out_of_pending_order"
                )
            notifications_by_id = {
                str(record["event_id"]): record for record in replay["notifications"]
            }
            selected = [notifications_by_id[event_id] for event_id in requested_ids]
            pending_after = [
                event_id for event_id in pending_before if event_id not in pending_set
            ]
            previous_ack = (
                {
                    "event_id": journal_acks[len(committed_ack_ids) - 1]["event_id"],
                    "record_sha256": _canonical_hash(
                        journal_acks[len(committed_ack_ids) - 1]
                    ),
                }
                if committed_ack_ids
                else None
            )
            latest_commit = context["latest_commit"]
            if not isinstance(latest_commit, Mapping):
                raise MonitorDataQualityOutboxError(
                    "data_quality_notification_ack_requires_scan_commit"
                )
            scan_anchor = {
                "event_id": latest_commit["event_id"],
                "record_sha256": _canonical_hash(latest_commit),
            }
            expected_ack = _build_ack(
                session_date=session,
                observed_at_utc=acknowledged_at,
                state_mode=(
                    str(unreflected_ack["cadence"]["mode"])
                    if unreflected_ack is not None
                    else str(state_mode)
                ),
                notifications=selected,
                delivery_proof=proof,
                pending_before=pending_before,
                pending_after=pending_after,
                previous_ack=previous_ack,
                scan_commit_anchor=scan_anchor,
            )
            _validate_ack_causal_order(
                expected_ack,
                by_id=context["by_id"],
                notifications_by_id=notifications_by_id,
                latest_commit=latest_commit,
            )
            result["ack_event_id"] = expected_ack["event_id"]
            if unreflected_ack is not None and _canonical_json_bytes(
                unreflected_ack
            ) != _canonical_json_bytes(expected_ack):
                raise MonitorDataQualityOutboxError(
                    "unreflected_data_quality_notification_ack_requires_exact_recovery:"
                    + str(unreflected_ack["event_id"])
                )
            if partial_tail is not None:
                expected_bytes = _canonical_json_bytes(expected_ack)
                if not partial_tail or not expected_bytes.startswith(partial_tail):
                    raise MonitorDataQualityOutboxError(
                        "data_quality_notification_ack_partial_tail_not_exact_prefix"
                    )
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorDataQualityOutboxError(
                        "state_changed_during_data_quality_ack_tail_recovery"
                    )
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True
            expected_records: list[Mapping[str, Any]] = list(records)
            if unreflected_ack is None:
                _append_jsonl_durable(journal_path, expected_ack)
                expected_records.append(expected_ack)
                result["ack_appended"] = True
                result["commit_phase"] = "journal_durable"
                if failpoint is not None:
                    failpoint("after_ack_append")
            else:
                result["commit_phase"] = "journal_durable"
            if failpoint is not None:
                failpoint("before_state_replace")
            if state_path.read_bytes() != original_state_bytes:
                raise MonitorDataQualityOutboxError(
                    "state_changed_during_data_quality_ack"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            next_ack_ids = committed_ack_ids + [str(expected_ack["event_id"])]
            next_outbox = _state_for_prefix(
                session_date=session,
                activation_rollover_event_id=context[
                    "activation_rollover_event_id"
                ],
                records=expected_records,
                notification_ids=state_notification_ids,
                ack_ids=next_ack_ids,
                processed_commit_receipt=context["latest_commit"],
            )
            next_state = copy.deepcopy(state)
            next_state["data_quality_notification_outbox"] = next_outbox
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted, _raw = _load_state(state_path)
                    state_replace_succeeded = persisted == next_state
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            result["event_ids_acknowledged"] = requested_ids
            result["event_ids_pending_notification"] = list(
                next_outbox["pending_notification_event_ids"]
            )
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorDataQualityOutboxError(
                    "data_quality_ack_state_postcondition_failed"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_acknowledged"
                        if unreflected_ack is not None
                        or result["partial_journal_tail_recovered"]
                        else "acknowledged"
                    ),
                    "state_updated": True,
                    "commit_phase": "committed",
                    "issues": [],
                }
            )
            return result

        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (
        MonitorDataQualityOutboxError,
        MonitorScanLedgerError,
        OSError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        durable = result.get("commit_phase") == "journal_durable"
        result.update(
            {
                "accepted": False,
                "action": (
                    "retry_required"
                    if state_replace_succeeded or durable
                    else "abstain"
                ),
                "state_updated": state_replace_succeeded,
                "commit_phase": (
                    "post_state_replace_uncertain"
                    if state_replace_succeeded
                    else result.get("commit_phase", "not_started")
                ),
                "issues": [
                    str(exc)
                    if isinstance(
                        exc,
                        (MonitorDataQualityOutboxError, MonitorScanLedgerError),
                    )
                    else f"data_quality_ack_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


def validate_data_quality_notification_outbox_receipt_replay(
    state: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    journal_raw: bytes,
    *,
    require_notifications_acknowledged: bool,
    rollover_observed_at_utc: datetime | None = None,
) -> dict[str, Any]:
    """Validate the complete outbox projection for session rollover.

    An absent/legacy namespace is a deliberate pre-activation boundary.  Once
    armed, every source condition must have a reflected wrapper, every ACK must
    be reflected, and rollover may additionally require an empty pending set.
    """

    try:
        context = _context(state=state, records=records, journal_raw=journal_raw)
        if context["activation_status"] == LEGACY_PRE_ACTIVATION:
            return copy.deepcopy(context["outbox"])
        expected_ids = [
            str(record["event_id"])
            for record in context["expected_notifications"]
        ]
        replay = context["replay"]
        outbox = context["outbox"]
        journal_ids = [str(record["event_id"]) for record in replay["notifications"]]
        state_ids = list(outbox["committed_notification_event_ids"])
        if journal_ids != state_ids:
            if state_ids == journal_ids[: len(state_ids)]:
                raise MonitorDataQualityOutboxError(
                    "prior_session_unreflected_data_quality_notification:"
                    + journal_ids[len(state_ids)]
                )
            raise MonitorDataQualityOutboxError(
                "prior_session_data_quality_notification_projection_mismatch"
            )
        if state_ids != expected_ids:
            raise MonitorDataQualityOutboxError(
                "prior_session_data_quality_notification_sync_required"
            )
        latest_commit = context["latest_commit"]
        latest_commit_id = (
            latest_commit.get("event_id")
            if isinstance(latest_commit, Mapping)
            else None
        )
        if outbox.get("last_processed_commit_receipt_event_id") != latest_commit_id:
            raise MonitorDataQualityOutboxError(
                "prior_session_data_quality_commit_receipt_not_processed:"
                + str(latest_commit_id or "none")
            )
        journal_ack_ids = [str(record["event_id"]) for record in replay["acks"]]
        if journal_ack_ids != outbox["committed_notification_ack_event_ids"]:
            raise MonitorDataQualityOutboxError(
                "prior_session_unreflected_data_quality_notification_ack"
            )
        pending = list(outbox["pending_notification_event_ids"])
        if require_notifications_acknowledged and pending:
            raise MonitorDataQualityOutboxError(
                "prior_session_pending_data_quality_notifications:"
                + ",".join(pending)
            )
        if rollover_observed_at_utc is not None:
            rollover_at = _normalized_observed_at(rollover_observed_at_utc)
            for ack in replay["acks"]:
                ack_at = _parsed_aware(
                    ack.get("observed_at_utc"),
                    "data_quality_notification_ack.observed_at_utc",
                ).astimezone(_UTC)
                if ack_at >= rollover_at:
                    raise MonitorDataQualityOutboxError(
                        "prior_session_data_quality_ack_not_before_rollover:"
                        + str(ack["event_id"])
                    )
        return copy.deepcopy(outbox)
    except MonitorScanLedgerError as exc:
        raise MonitorDataQualityOutboxError(str(exc)) from exc


__all__ = [
    "ACK_EVENT_SCHEMA",
    "ACK_EVENT_TYPE",
    "ACK_RESULT_SCHEMA",
    "ARMED",
    "LEGACY_PRE_ACTIVATION",
    "MonitorDataQualityOutboxError",
    "NOTIFICATION_EVENT_SCHEMA",
    "NOTIFICATION_EVENT_TYPE",
    "OUTBOX_STATE_SCHEMA",
    "SYNC_RESULT_SCHEMA",
    "ack_data_quality_notifications",
    "armed_outbox",
    "legacy_pre_activation_outbox",
    "sync_data_quality_notifications",
    "validate_data_quality_notification_outbox_receipt_replay",
]
