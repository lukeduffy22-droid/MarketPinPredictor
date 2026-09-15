"""Pure validation for prior-heartbeat completed-final delivery evidence.

The durable notification owners intentionally persist only the compact
``delivery_proof`` object.  This module closes the caller-side gap between a
completed assistant final and that object without reading Codex-owned state:
the caller supplies a normalized completed-final record, and this module
derives the acknowledgement request from its exact UTF-8 text.

This is not an authentication boundary for conversation history.  The caller
remains responsible for obtaining the record from actual completed history.
It is, however, a fail-closed normalization boundary: incomplete finals,
missing or reordered event IDs, ambiguous markers, and acknowledgement times
which do not strictly follow final completion cannot produce a request.
"""

from __future__ import annotations

import hashlib
import re
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence


COMPLETED_FINAL_SCHEMA = "marketpin-monitor-completed-final.v1"
DELIVERY_PROOF_TYPE = "prior_heartbeat_final_delivered"

_COMPLETED_FINAL_FIELDS = {
    "schema_version",
    "turn_id",
    "role",
    "status",
    "completed_at_utc",
    "final_text",
}
_DELIVERY_MARKER_PREFIX = "Audit delivery marker: "
_DELIVERY_MARKER = re.compile(
    re.escape(_DELIVERY_MARKER_PREFIX)
    + r"(?P<inline_code>`?)"
    r"(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?Z)(?P=inline_code)\."
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class MonitorCompletedFinalDeliveryError(ValueError):
    """A completed-final record cannot authorize an acknowledgement request."""


def _canonical_session_date(value: Any) -> str:
    if not isinstance(value, str):
        raise MonitorCompletedFinalDeliveryError("session_date_invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise MonitorCompletedFinalDeliveryError("session_date_invalid") from exc
    if parsed.isoformat() != value:
        raise MonitorCompletedFinalDeliveryError("session_date_invalid")
    return value


def _utc_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise MonitorCompletedFinalDeliveryError(f"{field}_must_be_utc")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise MonitorCompletedFinalDeliveryError(f"{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise MonitorCompletedFinalDeliveryError(f"{field}_must_be_utc")
    return parsed.astimezone(timezone.utc)


def _canonical_utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _event_ids(value: Any) -> list[str]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise MonitorCompletedFinalDeliveryError("event_ids_must_be_an_array")
    normalized = list(value)
    if (
        not normalized
        or any(
            not isinstance(event_id, str) or not _SHA256.fullmatch(event_id)
            for event_id in normalized
        )
        or len(set(normalized)) != len(normalized)
    ):
        raise MonitorCompletedFinalDeliveryError(
            "event_ids_must_be_unique_canonical_sha256_values"
        )
    return normalized


def _event_id_position(final_text: str, event_id: str) -> int:
    matches = list(
        re.finditer(
            rf"(?<![0-9a-f]){re.escape(event_id)}(?![0-9a-f])",
            final_text,
        )
    )
    if len(matches) != 1:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_event_id_occurrence_invalid:" + event_id
        )
    return matches[0].start()


def build_data_quality_ack_request(
    *,
    session_date: str,
    event_ids: Sequence[str],
    observed_at_utc: str,
    completed_final: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the exact DQ ACK request from a normalized completed final.

    ``final_text`` is encoded directly with UTF-8 and is never normalized or
    newline-translated.  The acknowledgement timestamp must be strictly later
    than the completed-final timestamp, preserving the two-turn contract even
    when the embedded delivery marker was generated shortly before completion.
    """

    session = _canonical_session_date(session_date)
    normalized_ids = _event_ids(event_ids)
    acknowledged_at = _utc_timestamp(observed_at_utc, "observed_at_utc")

    if (
        not isinstance(completed_final, Mapping)
        or set(completed_final) != _COMPLETED_FINAL_FIELDS
    ):
        raise MonitorCompletedFinalDeliveryError("completed_final_fields_invalid")
    if completed_final.get("schema_version") != COMPLETED_FINAL_SCHEMA:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_schema_version_invalid"
        )
    if (
        not isinstance(completed_final.get("turn_id"), str)
        or not completed_final["turn_id"]
    ):
        raise MonitorCompletedFinalDeliveryError("completed_final_turn_id_invalid")
    if completed_final.get("role") != "assistant":
        raise MonitorCompletedFinalDeliveryError("completed_final_role_invalid")
    if completed_final.get("status") != "completed":
        raise MonitorCompletedFinalDeliveryError("completed_final_status_invalid")

    completed_at_text = completed_final.get("completed_at_utc")
    completed_at = _utc_timestamp(
        completed_at_text, "completed_final_completed_at_utc"
    )
    if acknowledged_at <= completed_at:
        raise MonitorCompletedFinalDeliveryError(
            "acknowledgement_must_strictly_follow_completed_final"
        )

    final_text = completed_final.get("final_text")
    if not isinstance(final_text, str) or not final_text:
        raise MonitorCompletedFinalDeliveryError("completed_final_text_invalid")
    try:
        final_bytes = final_text.encode("utf-8", errors="strict")
    except UnicodeEncodeError as exc:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_text_not_valid_utf8"
        ) from exc

    if final_text.count(_DELIVERY_MARKER_PREFIX) != 1:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_delivery_marker_must_appear_exactly_once"
        )
    markers = list(_DELIVERY_MARKER.finditer(final_text))
    if len(markers) != 1:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_delivery_marker_format_invalid"
        )
    marker_text = markers[0].group("timestamp")
    marker_at = _utc_timestamp(
        marker_text, "completed_final_delivery_marker_at_utc"
    )
    if marker_at > completed_at:
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_delivery_marker_after_completion"
        )

    positions = [
        _event_id_position(final_text, event_id) for event_id in normalized_ids
    ]
    if positions != sorted(positions):
        raise MonitorCompletedFinalDeliveryError(
            "completed_final_event_ids_not_in_pending_order"
        )

    return {
        "session_date": session,
        "event_ids": normalized_ids,
        "observed_at_utc": _canonical_utc_text(acknowledged_at),
        "delivery_proof": {
            "proof_type": DELIVERY_PROOF_TYPE,
            "conversation_history_sha256": hashlib.sha256(final_bytes).hexdigest(),
            "prior_final_delivered_at_utc": marker_text,
        },
    }
