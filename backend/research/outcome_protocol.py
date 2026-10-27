"""Pure eligibility checks for prospective reference-price shadow outcomes.

The protocol is a measurement rule, not a fitted model or calibrated interval.
Historical rows are classified at read time and are never rewritten.
"""
from datetime import datetime, timezone
import math

PROTOCOL_ID = "shadow-reference-outcome-30s.v1"
TARGET_TOLERANCE_SECONDS = 30.0


def _utc(value):
    try:
        value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.astimezone(timezone.utc) if value.tzinfo is not None else None
    except (TypeError, ValueError, OverflowError):
        return None


def assess_outcome(row, realized_timestamp_utc=None, *, tolerance_seconds=TARGET_TOLERANCE_SECONDS):
    """Return explicit timing validity; missing persistence evidence fails closed."""
    if not math.isfinite(tolerance_seconds) or tolerance_seconds < 0:
        raise ValueError("invalid outcome tolerance")
    origin = _utc(row.get("prediction_timestamp_utc"))
    target = _utc(row.get("target_timestamp_utc"))
    created = _utc(row.get("created_at_utc"))
    realized = _utc(realized_timestamp_utc or row.get("realized_timestamp_utc"))
    reasons = []
    if None in (origin, target, created, realized):
        reasons.append("OUTCOME_TIMING_UNVERIFIED")
    else:
        if not origin <= created < target:
            reasons.append("PREDICTION_NOT_PERSISTED_BEFORE_TARGET")
        if realized < target:
            reasons.append("OUTCOME_PRECEDES_TARGET")
        elif (realized - target).total_seconds() > tolerance_seconds:
            reasons.append("OUTCOME_AFTER_TARGET_TOLERANCE")
    return {"protocol_id": PROTOCOL_ID, "status": "QUARANTINED" if reasons else "ELIGIBLE",
            "reasons": reasons, "tolerance_seconds": tolerance_seconds,
            "target_lag_seconds": (realized - target).total_seconds() if realized and target else None,
            "actual_horizon_seconds": (realized - origin).total_seconds() if realized and origin else None,
            "outcome_kind": "option_derived_reference_not_official_close"}


def register_outcome_sql(connection):
    """Share exact timestamp semantics with bounded SQLite aggregate readers."""
    def eligible(origin, target, created, realized):
        row = dict(prediction_timestamp_utc=origin, target_timestamp_utc=target,
                   created_at_utc=created, realized_timestamp_utc=realized)
        return int(assess_outcome(row)["status"] == "ELIGIBLE")
    connection.create_function("outcome_eligible", 4, eligible, deterministic=True)
