"""Durable acknowledgement boundary for MarketPin policy notifications.

Policy wrappers remain pending after scan commit.  A later heartbeat may call
this module only when its conversation history proves the prior notification
final was delivered.  The acknowledgement is journaled and fsynced before the
pending IDs are removed atomically from monitor state.
"""

from __future__ import annotations

import copy
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.monitor_scan_ledger import (
    COMMIT_RECEIPT_EVENT_TYPE,
    NOTIFICATION_ACK_EVENT_TYPE,
    MonitorScanLedgerError,
    _append_jsonl_durable,
    _atomic_write_json,
    _build_notification_ack,
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
    _validate_notification_ack_causal_order,
    _validate_notification_delivery_proof,
    _UTC,
)


RESULT_SCHEMA = "marketpin-monitor-notification-ack.result.v1"
_VALID_MODES = {"NORMAL", "ELEVATED"}
_CHICAGO = ZoneInfo("America/Chicago")


class MonitorNotificationOutboxError(RuntimeError):
    """A fail-closed notification acknowledgement error."""


def _base_result(*, session_date: Any = None, event_ids: Any = None) -> dict[str, Any]:
    requested_ids = list(event_ids) if isinstance(event_ids, list) else []
    return {
        "schema_version": RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "ack_event_id": None,
        "ack_appended": False,
        "partial_journal_tail_recovered": False,
        "requested_event_ids": requested_ids,
        "event_ids_acknowledged": [],
        # None means the durable outbox could not yet be validated.  Never
        # report an empty list merely because an acknowledgement was rejected.
        "event_ids_pending_notification": None,
        "state_updated": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def _normalized_event_ids(event_ids: Sequence[str]) -> list[str]:
    if isinstance(event_ids, (str, bytes)) or not isinstance(event_ids, Sequence):
        raise MonitorNotificationOutboxError("event_ids_must_be_an_array")
    normalized = list(event_ids)
    if (
        not normalized
        or any(not _is_sha256(event_id) for event_id in normalized)
        or len(set(normalized)) != len(normalized)
    ):
        raise MonitorNotificationOutboxError(
            "event_ids_must_be_unique_canonical_sha256_values"
        )
    return normalized


def _normalized_observed_at(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MonitorNotificationOutboxError(
            "observed_at_utc_must_be_timezone_aware"
        )
    return value.astimezone(_UTC)


def _raise_scan_error(exc: MonitorScanLedgerError) -> None:
    raise MonitorNotificationOutboxError(str(exc)) from exc


def _policy_commitment(
    event_id: str,
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    committed_receipt_ids: Sequence[str],
) -> dict[str, str]:
    wrapper = by_id.get(event_id)
    if not isinstance(wrapper, Mapping) or not isinstance(
        wrapper.get("helper_event"), Mapping
    ):
        raise MonitorNotificationOutboxError(
            "pending_notification_wrapper_missing:" + event_id
        )
    matching_receipts = [
        by_id[receipt_id]
        for receipt_id in committed_receipt_ids
        if event_id in by_id[receipt_id]["ordered_policy_event_ids"]
    ]
    if len(matching_receipts) != 1:
        raise MonitorNotificationOutboxError(
            "pending_notification_commit_receipt_invalid:" + event_id
        )
    receipt = matching_receipts[0]
    return {
        "event_id": event_id,
        "record_sha256": _canonical_hash(wrapper),
        "parent_scan_event_id": str(wrapper["parent_scan_event_id"]),
        "commit_receipt_event_id": str(receipt["event_id"]),
        "commit_receipt_sha256": _canonical_hash(receipt),
    }


def _request_matches_ack(
    record: Mapping[str, Any],
    *,
    session_date: str,
    observed_at_utc: str,
    event_ids: Sequence[str],
    delivery_proof: Mapping[str, Any],
) -> bool:
    return bool(
        record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
        and record.get("session_date") == session_date
        and record.get("observed_at_utc") == observed_at_utc
        and [
            commitment.get("event_id")
            for commitment in record.get("acked_policy_events", [])
            if isinstance(commitment, Mapping)
        ]
        == list(event_ids)
        and _canonical_json_bytes(record.get("delivery_proof"))
        == _canonical_json_bytes(delivery_proof)
    )


def _unreflected_scan_transaction_issue(
    *,
    by_id: Mapping[str, Mapping[str, Any]],
    ledger: Mapping[str, Any],
) -> str | None:
    committed_scan_ids = set(ledger.get("committed_scan_event_ids", []))
    committed_policy_ids = set(ledger.get("committed_policy_event_ids", []))
    committed_receipt_ids = set(ledger.get("committed_receipt_event_ids", []))
    for event_id, record in by_id.items():
        if (
            record.get("event_type") == "substantive_scan"
            and event_id not in committed_scan_ids
        ):
            return "unreflected_substantive_scan_requires_exact_retry:" + event_id
        if (
            isinstance(record.get("helper_event"), Mapping)
            and event_id not in committed_policy_ids
        ):
            return "unreflected_policy_wrapper_requires_scan_retry:" + event_id
        if (
            record.get("event_type") == COMMIT_RECEIPT_EVENT_TYPE
            and event_id not in committed_receipt_ids
        ):
            return "unreflected_scan_commit_requires_exact_retry:" + event_id
    return None


def ack_monitor_notifications(
    *,
    session_date: str,
    event_ids: Sequence[str],
    observed_at_utc: datetime,
    delivery_proof: Mapping[str, Any],
    state_path: Path,
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Durably acknowledge a proven-delivered set of pending policy wrappers."""

    result = _base_result(
        session_date=session_date,
        event_ids=(
            list(event_ids)
            if isinstance(event_ids, Sequence)
            and not isinstance(event_ids, (str, bytes))
            else []
        ),
    )
    state_replace_succeeded = False
    try:
        normalized_session = _canonical_session_date(session_date)
        normalized_ids = _normalized_event_ids(event_ids)
        acknowledged_at = _normalized_observed_at(observed_at_utc)
        acknowledged_text = acknowledged_at.isoformat().replace("+00:00", "Z")
        try:
            normalized_proof = _validate_notification_delivery_proof(delivery_proof)
        except MonitorScanLedgerError as exc:
            _raise_scan_error(exc)
        delivered_at = _parsed_aware(
            normalized_proof["prior_final_delivered_at_utc"],
            "notification_ack.prior_final_delivered_at_utc",
        ).astimezone(_UTC)
        if delivered_at >= acknowledged_at:
            raise MonitorNotificationOutboxError(
                "acknowledgement_must_follow_prior_final_delivery"
            )
        if acknowledged_at.astimezone(_CHICAGO).date().isoformat() < normalized_session:
            raise MonitorNotificationOutboxError(
                "acknowledgement_time_before_market_session"
            )

        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()
        journal_path = journal_dir / f"{normalized_session}.jsonl"
        result["journal_path"] = str(journal_path)

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            if state.get("session_date") != normalized_session:
                raise MonitorNotificationOutboxError(
                    "notification_ack_state_session_mismatch"
                )
            state_mode = state.get("mode")
            if state_mode not in _VALID_MODES:
                raise MonitorNotificationOutboxError(
                    "state_mode_must_be_NORMAL_or_ELEVATED"
                )
            (
                records,
                journal_raw,
                partial_journal_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            try:
                by_id, _receipts = _journal_index(records)
                ledger = _validated_ledger_state(
                    state.get("monitor_scan_ledger"), state, by_id, journal_raw
                )
            except MonitorScanLedgerError as exc:
                _raise_scan_error(exc)
            if not ledger:
                raise MonitorNotificationOutboxError(
                    "notification_ack_requires_committed_scan_ledger"
                )
            result["event_ids_pending_notification"] = list(
                ledger["pending_notification_event_ids"]
            )
            scan_issue = _unreflected_scan_transaction_issue(
                by_id=by_id, ledger=ledger
            )
            if scan_issue:
                raise MonitorNotificationOutboxError(scan_issue)

            committed_ack_ids = list(
                ledger.get("committed_notification_ack_event_ids", [])
            )
            committed_ack_id_set = set(committed_ack_ids)
            unreflected_ack_records = [
                record
                for event_id, record in by_id.items()
                if record.get("event_type") == NOTIFICATION_ACK_EVENT_TYPE
                and event_id not in committed_ack_id_set
            ]
            if len(unreflected_ack_records) > 1:
                raise MonitorNotificationOutboxError(
                    "multiple_unreflected_notification_acks"
                )
            matching = [
                record
                for record in by_id.values()
                if _request_matches_ack(
                    record,
                    session_date=normalized_session,
                    observed_at_utc=acknowledged_text,
                    event_ids=normalized_ids,
                    delivery_proof=normalized_proof,
                )
            ]
            if len(matching) > 1:
                raise MonitorNotificationOutboxError(
                    "duplicate_exact_notification_ack"
                )
            if matching and str(matching[0]["event_id"]) in committed_ack_id_set:
                if unreflected_ack_records:
                    raise MonitorNotificationOutboxError(
                        "unreflected_notification_ack_requires_exact_recovery:"
                        + str(unreflected_ack_records[0]["event_id"])
                    )
                # A committed acknowledgement cannot own bytes written after
                # its durable boundary.  Returning success here would bless a
                # malformed tail and let later rollover seal it.  Leave the
                # bytes untouched so the transaction that owns them must
                # recover them (or fail closed) through its exact retry path.
                if partial_journal_tail is not None:
                    raise MonitorNotificationOutboxError(
                        "partial_journal_tail_after_committed_notification_ack"
                    )
                result.update(
                    {
                        "accepted": True,
                        "action": "already_acknowledged",
                        "ack_event_id": matching[0]["event_id"],
                        "event_ids_acknowledged": normalized_ids,
                        "event_ids_pending_notification": list(
                            ledger["pending_notification_event_ids"]
                        ),
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result

            pending_before = list(ledger["pending_notification_event_ids"])
            if any(event_id not in pending_before for event_id in normalized_ids):
                raise MonitorNotificationOutboxError(
                    "notification_ack_event_id_not_pending"
                )
            if normalized_ids != [
                event_id for event_id in pending_before if event_id in set(normalized_ids)
            ]:
                raise MonitorNotificationOutboxError(
                    "notification_ack_event_ids_out_of_pending_order"
                )
            commitments = [
                _policy_commitment(
                    event_id,
                    by_id=by_id,
                    committed_receipt_ids=ledger["committed_receipt_event_ids"],
                )
                for event_id in normalized_ids
            ]
            pending_after = [
                event_id for event_id in pending_before if event_id not in set(normalized_ids)
            ]
            existing_ack = (
                unreflected_ack_records[0] if unreflected_ack_records else None
            )
            previous_ack = None
            if committed_ack_ids:
                prior = by_id[committed_ack_ids[-1]]
                previous_ack = {
                    "event_id": prior["event_id"],
                    "record_sha256": _canonical_hash(prior),
                }
            commit_receipt = by_id[str(ledger["last_commit_receipt_event_id"])]
            scan_anchor = {
                "event_id": commit_receipt["event_id"],
                "record_sha256": _canonical_hash(commit_receipt),
            }
            expected_ack = _build_notification_ack(
                session_date=normalized_session,
                observed_at_utc=acknowledged_at,
                # A complete orphan acknowledgement is already a validated,
                # durable record.  Its cadence mode is part of that original
                # transaction even if an external state writer changed the
                # live mode before the exact recovery attempt.
                state_mode=(
                    str(existing_ack["cadence"]["mode"])
                    if existing_ack is not None
                    else str(state_mode)
                ),
                acked_policy_events=commitments,
                delivery_proof=normalized_proof,
                pending_before=pending_before,
                pending_after=pending_after,
                previous_ack=previous_ack,
                scan_commit_anchor=scan_anchor,
            )
            try:
                _validate_notification_ack_causal_order(
                    expected_ack,
                    by_id=by_id,
                    latest_commit=commit_receipt,
                )
            except MonitorScanLedgerError as exc:
                _raise_scan_error(exc)
            result["ack_event_id"] = expected_ack["event_id"]

            if partial_journal_tail is not None:
                committed_size = ledger.get("committed_journal_size")
                if (
                    type(committed_size) is not int
                    or committed_size < 0
                    or committed_size > len(journal_raw)
                    or not _canonical_json_bytes(expected_ack).startswith(
                        partial_journal_tail
                    )
                ):
                    raise MonitorNotificationOutboxError(
                        "partial_notification_ack_tail_not_exact_prefix"
                    )
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorNotificationOutboxError(
                        "state_changed_during_notification_ack_tail_recovery"
                    )
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True

            if existing_ack is not None and _canonical_json_bytes(
                existing_ack
            ) != _canonical_json_bytes(expected_ack):
                raise MonitorNotificationOutboxError(
                    "unreflected_notification_ack_requires_exact_recovery:"
                    + str(existing_ack["event_id"])
                )
            collision = by_id.get(str(expected_ack["event_id"]))
            if collision is not None and _canonical_json_bytes(
                collision
            ) != _canonical_json_bytes(expected_ack):
                raise MonitorNotificationOutboxError(
                    "notification_ack_event_id_collision"
                )

            expected_records: list[Mapping[str, Any]] = list(records)
            if existing_ack is None:
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
                raise MonitorNotificationOutboxError(
                    "state_changed_during_notification_ack"
                )
            committed_raw, _committed_by_id = _revalidate_expected_journal(
                journal_path, expected_records
            )
            next_state = copy.deepcopy(state)
            next_ledger = copy.deepcopy(dict(ledger))
            next_ack_ids = list(committed_ack_ids)
            next_ack_ids.append(str(expected_ack["event_id"]))
            next_ledger.update(
                {
                    "pending_notification_event_ids": pending_after,
                    "committed_notification_ack_event_ids": next_ack_ids,
                    "last_notification_ack_event_id": expected_ack["event_id"],
                    "last_notification_ack_sha256": _canonical_hash(expected_ack),
                    "committed_journal_size": len(committed_raw),
                    "committed_journal_sha256": hashlib.sha256(
                        committed_raw
                    ).hexdigest(),
                }
            )
            next_state["monitor_scan_ledger"] = next_ledger
            next_state["notification_outbox_updated_at_utc"] = acknowledged_text
            next_state["notification_outbox_updated_at_ct"] = expected_ack[
                "observed_at_ct"
            ]
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted_after_error, _raw = _load_state(state_path)
                    state_replace_succeeded = persisted_after_error == next_state
                    if state_replace_succeeded:
                        result["event_ids_acknowledged"] = normalized_ids
                        result["event_ids_pending_notification"] = pending_after
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            result["event_ids_acknowledged"] = normalized_ids
            result["event_ids_pending_notification"] = pending_after
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorNotificationOutboxError(
                    "notification_ack_state_postcondition_failed"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_acknowledged"
                        if existing_ack is not None
                        or result["partial_journal_tail_recovered"]
                        else "acknowledged"
                    ),
                    "event_ids_acknowledged": normalized_ids,
                    "event_ids_pending_notification": pending_after,
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
        MonitorNotificationOutboxError,
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
                    if isinstance(
                        exc,
                        (MonitorNotificationOutboxError, MonitorScanLedgerError),
                    )
                    else f"notification_ack_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


__all__ = [
    "MonitorNotificationOutboxError",
    "RESULT_SCHEMA",
    "ack_monitor_notifications",
]
