"""Receipt-first owner for session-scoped opening-acceptance milestones.

The opening inspector remains read-only.  This module accepts one compact
inspector report, validates the milestone it proves, appends a deterministic
non-policy receipt, and only then replaces the monitor state.  It shares the
rollover/scan lock and can recover a complete receipt left durable before an
interrupted state replacement.
"""

from __future__ import annotations

import copy
import hashlib
import math
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from app.utils.market_calendar import market_calendar_status
from app.utils.market_time import is_early_close_day
from backend.monitor_scan_ledger import (
    MonitorScanLedgerError,
    _append_jsonl_durable,
    _atomic_write_json,
    _canonical_hash,
    _canonical_json_bytes,
    _exclusive_lock,
    _journal_index,
    _load_state,
    _read_journal_exact_retry_snapshot,
    _revalidate_expected_journal,
    _truncate_exact_partial_tail,
    _validate_notification_delivery_proof,
    _validated_ledger_state,
)
from backend.monitor_notification_outbox import _unreflected_scan_transaction_issue
from backend.opening_code_fingerprint import (
    LOADED_CODE_CAPTURE_SEMANTICS,
    LOADED_CODE_FINGERPRINT_SCHEMA,
    LOADED_CODE_HASH_ALGORITHM,
    OPENING_CRITICAL_SOURCE_PATHS,
)
from backend.monitor_session_rollover import (
    MonitorSessionRolloverError,
    _validate_current_event_receipt,
)


RESULT_SCHEMA = "marketpin-monitor-opening-acceptance.result.v1"
EVENT_SCHEMA = "marketpin-monitor-opening-acceptance-milestone.v1"
STATE_RECEIPT_SCHEMA = "marketpin-monitor-opening-acceptance-receipt.v1"
EVENT_TYPE = "opening_acceptance_milestone"
ACK_RESULT_SCHEMA = "marketpin-monitor-opening-acceptance-notification-ack.result.v1"
ACK_EVENT_SCHEMA = "marketpin-monitor-opening-acceptance-notification-ack.v1"
ACK_EVENT_TYPE = "opening_acceptance_notification_ack"

_CT = ZoneInfo("America/Chicago")
_UTC = timezone.utc
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_OPENING_SYMBOLS = ("SPX", "NDX", "VIX", "RUT")
_CORE_SYMBOLS = ("SPX", "NDX")
_VALID_MODES = {"NORMAL", "ELEVATED"}
_MAX_COMPACT_REPORT_BYTES = 128 * 1024
_CAPTURE_RATIO_FLOOR = 0.95
_MAX_STRUCTURE_AGE_SECONDS = 90.0
_MAX_SUBSCRIPTION_WINDOW_AGE_SECONDS = 15.0
_RUT_CONTRACT_SUBSCRIPTION_CAP = 3_200
_CORE_CONTRACT_SUBSCRIPTION_CAP = 3_600
_CONNECTION_LIFECYCLE_ZERO_COUNTERS = (
    "reconnect_attempts",
    "connection_limit_rejections_total",
    "connection_limit_consecutive",
    "pre_auth_transport_aborts_total",
    "pre_auth_transport_abort_failures_total",
)
_CONNECTION_LIFECYCLE_COMPACT_FIELDS = (
    *_CONNECTION_LIFECYCLE_ZERO_COUNTERS,
    "connection_limit_circuit_state",
    "connection_limit_retry_not_before_utc",
    "connection_limit_cooldown_remaining_seconds",
    "last_client_close_status",
    "last_client_close_elapsed_seconds",
    "pre_auth_transport_guard_status",
    "last_pre_auth_transport_event",
    "last_pre_auth_transport_reason",
    "last_pre_auth_transport_event_utc",
)
# The staged-subscription producer exposes primary *contract* counts only after
# proving that every retained strike has both a call and a put.  Opening
# acceptance v1 therefore converts that count back to complete pairs and keeps
# the sampler's five-pair formula floor as an independent receipt requirement.
_ORB_MIN_COMPLETE_PRIMARY_PAIRS_V1 = 5
_ORB_SCOPE_EXCLUDED_EXACT_ISSUES = {
    "LIVE_GATE_FAILED:calculation_ready",
    "LIVE_GATE_FAILED:prediction_pipeline_ok",
    "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE",
    "RUT_CANARY_CASH_SESSION_PROTECTION_ACTIVE",
}
_ORB_SCOPE_EXCLUDED_ISSUE_CODES = {
    "VALID_GEX_STRUCTURE_NOT_ADVANCING",
    "VALID_GEX_STRUCTURE_STALE",
    "GEX_STRUCTURE_SOURCE_TIMESTAMP_FUTURE",
    "GEX_STRUCTURE_CAPTURE_TIMESTAMP_MISSING",
    "GEX_STRUCTURE_CAPTURE_TIMESTAMP_FUTURE",
    "GEX_STRUCTURE_CAPTURE_STALE",
    "GEX_STRUCTURE_PROVIDER_INVALID",
    "GEX_STRUCTURE_VALIDATION_INVALID",
    "GEX_STRUCTURE_REFERENCE_PRICE_INVALID",
    "GAMMA_PIN_UNAVAILABLE",
    "MAX_PAIN_UNAVAILABLE",
    "GEX_STRUCTURE_CALCULATION_ID_MISSING",
    "GEX_STRUCTURE_UNIVERSE_INVALID",
    "GEX_STRUCTURE_PRIMARY_EXPIRATION_NOT_SAME_DAY",
    "GEX_STRUCTURE_SAME_DAY_PROFILE_UNAVAILABLE",
    "GEX_STRUCTURE_SUBSCRIPTION_EPOCH_MISMATCH",
    "GEX_STRUCTURE_SUBSCRIPTION_GENERATION_MISMATCH",
    "GEX_CALCULATION_BOUND_STRUCTURE_MISSING",
    "GEX_CALCULATION_BOUND_STRUCTURE_STALE",
    "GEX_CALCULATION_BOUND_STRUCTURE_INVALID",
    "GEX_CALCULATION_BOUND_IDENTITY_MISMATCH",
    "GEX_CALCULATION_LINEAGE_INVALID",
    "ORB_PIN_LEVELS_UNAVAILABLE",
    "ORB_GAMMA_PIN_UNAVAILABLE",
    "ORB_MAX_PAIN_UNAVAILABLE",
    "ORB_STRUCTURE_REFERENCE_NOT_ALIGNED",
    "ORB_STRUCTURE_LEVELS_UNAVAILABLE",
    "ORB_STRUCTURE_GAMMA_PIN_UNAVAILABLE",
    "ORB_STRUCTURE_MAX_PAIN_UNAVAILABLE",
    "ORB_STRUCTURE_REFERENCE_STALE",
    "ORB_PROVENANCE_STRUCTURE_NOT_ALIGNED",
    "ORB_PROVENANCE_STRUCTURE_MISMATCH",
    "ORB_PROVENANCE_STRUCTURE_STALE",
    "ORB_PROVENANCE_PIN_LEVELS_UNAVAILABLE",
    "ORB_PRIMARY_EXPIRATION_NOT_SAME_DAY",
    "ORB_SAME_DAY_PROFILE_UNAVAILABLE",
    "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_15m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_30m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_60m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
}
_GAMMA_SCOPE_CORE_ORB_ONLY_SYMBOL_CODES = {
    "ORB_REFERENCE_DECISION_PENDING",
    "ORB_REFERENCE_NOT_ADVANCING",
    "ORB_REFERENCE_CAPTURE_RATIO_LOW",
    "ORB_REFERENCE_5S_EVIDENCE_STALE",
    "ORB_REFERENCE_OPENING_BUCKET_MISSING",
    "ORB_REFERENCE_EPOCH_INVALID",
    "ORB_REFERENCE_EPOCH_MIXED",
    "ORB_REFERENCE_SUBSCRIPTION_EPOCH_MISMATCH",
    "ORB_REFERENCE_GENERATION_INVALID",
    "ORB_REFERENCE_GENERATION_MIXED",
    "ORB_REFERENCE_SUBSCRIPTION_GENERATION_MISMATCH",
    "ORB_5m_INCOMPLETE",
    "ORB_15m_INCOMPLETE",
    "ORB_30m_INCOMPLETE",
    "ORB_60m_INCOMPLETE",
    "ORB_5m_CAPTURE_RATIO_LOW",
    "ORB_15m_CAPTURE_RATIO_LOW",
    "ORB_30m_CAPTURE_RATIO_LOW",
    "ORB_60m_CAPTURE_RATIO_LOW",
    "ORB_5m_NOT_DIRECTIONAL_ELIGIBLE",
    "ORB_15m_NOT_DIRECTIONAL_ELIGIBLE",
    "ORB_30m_NOT_DIRECTIONAL_ELIGIBLE",
    "ORB_60m_NOT_DIRECTIONAL_ELIGIBLE",
    "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_15m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_30m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
    "ORB_60m_COMBINED_STRUCTURE_NOT_ELIGIBLE",
}
_GAMMA_SCOPE_OPTIONAL_SYMBOL_CODES = {
    "SUBSCRIPTION_ROOT_MISSING",
    *_GAMMA_SCOPE_CORE_ORB_ONLY_SYMBOL_CODES,
    "ORB_SYMBOL_RUNTIME_BINDING_MISSING",
    "ORB_SYMBOL_RUNTIME_EPOCH_MISMATCH",
    "ORB_SYMBOL_RUNTIME_GENERATION_MISMATCH",
    "ORB_SYMBOL_RANGE_GENERATION_MISMATCH",
    "ORB_SYMBOL_HANDOFF_NOT_ACTIVE",
    "ORB_SYMBOL_NOT_CONFIGURED",
    "ORB_5m_INCOMPLETE",
    "ORB_15m_INCOMPLETE",
    "ORB_30m_INCOMPLETE",
    "ORB_60m_INCOMPLETE",
    "ORB_5m_CAPTURE_RATIO_LOW",
    "ORB_15m_CAPTURE_RATIO_LOW",
    "ORB_30m_CAPTURE_RATIO_LOW",
    "ORB_60m_CAPTURE_RATIO_LOW",
}
_GAMMA_SCOPE_EXCLUDED_EXACT_ISSUES = {
    "ORB_REFERENCE_DECISION_TABLE_MISSING",
    "ORB_REFERENCE_DECISION_SCHEMA_INCOMPATIBLE",
    "ORB_REFERENCE_DECISION_IMMUTABILITY_TRIGGERS_MISSING",
    "ORB_REFERENCE_DECISION_IDENTITY_INDEX_MISSING",
    "RUT_ORB_CONTEXT_ONLY",
    "RUT_ORB_CLASSIFICATION_MISMATCH",
    "VIX_ORB_AUTHORITY_INVALID",
    "RUT_CANARY_PREOPEN_DEFERRAL_DURING_SESSION",
    "RUT_CANARY_DEFERRAL_EVIDENCE_MISSING",
    "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE",
    "RUT_CANARY_PROTECTION_EVIDENCE_MISSING",
    "RUT_CANARY_CASH_PROTECTION_EVIDENCE_MISSING",
    "RUT_CANARY_CASH_SESSION_PROTECTION_ACTIVE",
    "RUT_CANARY_STATE_INVALID:blocked",
    "RUT_CANARY_STATE_INVALID:deferred_off_hours",
    "RUT_CANARY_STATE_INVALID:disabled",
    "RUT_CANARY_STATE_INVALID:rolled_back",
    "RUT_CANARY_STATE_INVALID:unreported",
}
_COMPACT_REPORT_FIELDS = {
    "schema_version",
    "output_mode",
    "state",
    "observed_at_ct",
    "observed_at_utc",
    "issues",
    "warnings",
    "due_orb_windows",
    "due_orb_window",
    "clock",
    "backend_health",
    "backend_live_health",
    "dashboard_health",
    "orb",
    "database",
    "scheduled_tasks",
    "read_only",
    "authorities",
    "notes",
    "acceptance_scope",
    "out_of_scope_issues",
}


def _is_orb_scope_excluded_issue(issue: str) -> bool:
    if issue == "RUT_ORB_CONTEXT_ONLY":
        return True
    if issue in _ORB_SCOPE_EXCLUDED_EXACT_ISSUES:
        return True
    parts = issue.split(":")
    return (
        len(parts) == 2
        and parts[0] in _ORB_SCOPE_EXCLUDED_ISSUE_CODES
        and parts[1] in _CORE_SYMBOLS
    )


def _is_gamma_scope_excluded_issue(issue: str) -> bool:
    if issue in _GAMMA_SCOPE_EXCLUDED_EXACT_ISSUES:
        return True
    parts = issue.split(":")
    if len(parts) != 2:
        return False
    code, symbol = parts
    if symbol in _CORE_SYMBOLS:
        return code in _GAMMA_SCOPE_CORE_ORB_ONLY_SYMBOL_CODES
    return symbol in {"VIX", "RUT"} and code in _GAMMA_SCOPE_OPTIONAL_SYMBOL_CODES

_MILESTONES: tuple[tuple[str, str, str, time], ...] = (
    (
        "startup",
        "startup_milestone_notified",
        "startup_milestone",
        time(7, 55),
    ),
    (
        "first_eligible_gamma_capture",
        "first_eligible_gamma_capture_notified",
        "first_eligible_gamma_capture",
        time(8, 30),
    ),
    (
        "complete_5m_orb",
        "first_complete_5m_orb_notified",
        "first_complete_5m_orb",
        time(8, 35),
    ),
    (
        "complete_60m_orb",
        "final_complete_60m_orb_notified",
        "final_complete_60m_orb",
        time(9, 30),
    ),
)
_MILESTONE_BY_NAME = {item[0]: item for item in _MILESTONES}
_MILESTONE_DEPENDENCIES = {
    "startup": (),
    "first_eligible_gamma_capture": (),
    "complete_5m_orb": ("startup",),
    "complete_60m_orb": ("startup", "complete_5m_orb"),
}
_OWNED_BASE_FLAGS = {
    "startup_milestone_notified": False,
    "first_eligible_gamma_capture_notified": False,
    "first_complete_5m_orb_notified": False,
    "final_complete_60m_orb_notified": False,
    "temporary_acceptance_checks_complete": False,
    "pending_notification_event_ids": [],
    "committed_notification_ack_event_ids": [],
}


class MonitorOpeningAcceptanceError(RuntimeError):
    """A fail-closed opening milestone validation or persistence error."""


def _base_result(*, milestone: Any = None, session_date: Any = None) -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "milestone": milestone,
        "event_id": None,
        "event_appended": False,
        "partial_journal_tail_recovered": False,
        "state_updated": False,
        "notification_required": False,
        "notification_status": None,
        "pending_notification_event_ids": None,
        "temporary_acceptance_checks_complete": False,
        "commit_phase": "not_started",
        "issues": [],
    }


def _base_ack_result(*, session_date: Any = None) -> dict[str, Any]:
    return {
        "schema_version": ACK_RESULT_SCHEMA,
        "accepted": False,
        "action": "abstain",
        "session_date": session_date,
        "ack_event_id": None,
        "ack_appended": False,
        "partial_journal_tail_recovered": False,
        "state_updated": False,
        "event_ids_acknowledged": [],
        "pending_notification_event_ids": None,
        "commit_phase": "not_started",
        "issues": [],
    }


def _canonical_session_date(value: Any) -> str:
    if not isinstance(value, str) or len(value) != 10:
        raise MonitorOpeningAcceptanceError("session_date_invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise MonitorOpeningAcceptanceError("session_date_invalid") from exc
    if parsed.isoformat() != value:
        raise MonitorOpeningAcceptanceError("session_date_invalid")
    return value


def _parse_aware(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise MonitorOpeningAcceptanceError(f"{field}_invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MonitorOpeningAcceptanceError(f"{field}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MonitorOpeningAcceptanceError(f"{field}_must_be_timezone_aware")
    return parsed


def _latest_session_journal_observed(
    records: Sequence[Mapping[str, Any]], *, session_date: str, field: str
) -> datetime | None:
    latest: datetime | None = None
    for record in records:
        if record.get("session_date") != session_date:
            continue
        raw_observed = record.get("observed_at_utc")
        if raw_observed is None:
            continue
        observed = _parse_aware(raw_observed, field).astimezone(_UTC)
        if latest is None or observed > latest:
            latest = observed
    return latest


def _is_sha256(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _positive_finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0.0 else None


def _mapping(value: Any, issue: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MonitorOpeningAcceptanceError(issue)
    return value


def _subscription_staging_issues(
    health_payload: Mapping[str, Any],
    requested_symbols: set[str],
) -> list[str]:
    """Validate the optional all-primary-then-shadow health contract.

    Older full-subscription runtimes did not publish ``subscription_staging``;
    their existing count checks remain authoritative.  Once a runtime declares
    the staged contract, every identity and count is fail closed so an
    intentional primary-only opening stage is distinguishable from lost
    contracts.
    """

    requested_orb_symbols = requested_symbols.intersection(_OPENING_SYMBOLS)
    raw_stage = health_payload.get("subscription_staging")
    if raw_stage is None:
        return []
    if not isinstance(raw_stage, Mapping):
        return ["SUBSCRIPTION_STAGING_NOT_MAPPING"]

    issues: list[str] = []
    state = str(raw_stage.get("state") or "").strip().lower()
    active_stage = str(raw_stage.get("active_stage") or "").strip().lower()
    deferred_stage = raw_stage.get("deferred_stage")
    allowed_states = {"primary_active", "full_active"}
    if raw_stage.get("mode") != "all-primary-then-same-session-shadow":
        issues.append("SUBSCRIPTION_STAGING_MODE_INVALID")
    if state not in allowed_states:
        issues.append(f"SUBSCRIPTION_STAGING_STATE_INVALID:{state or 'missing'}")
    if raw_stage.get("same_client_additive_subscription") is not True:
        issues.append("SUBSCRIPTION_STAGING_CLIENT_MODE_INVALID")
    if raw_stage.get("intraday_replay_for_deferred_stage") is not False:
        issues.append("SUBSCRIPTION_STAGING_REPLAY_MODE_INVALID")

    def count(mapping: Mapping[str, Any], field: str) -> int | None:
        value = mapping.get(field)
        return value if type(value) is int and value >= 0 else None

    metadata_value = health_payload.get("subscription_metadata")
    metadata = metadata_value if isinstance(metadata_value, Mapping) else {}
    if not isinstance(metadata_value, Mapping):
        issues.append("SUBSCRIPTION_STAGING_METADATA_MISSING")
    selected_count = count(metadata, "selected_contract_count")
    stage_full = count(raw_stage, "full_selected_contract_count")
    stage_active = count(raw_stage, "active_contract_count")
    stage_deferred = count(raw_stage, "deferred_contract_count")
    health_selected = health_payload.get("symbols_selected")
    health_active = health_payload.get("symbols_subscribed")
    if (
        selected_count is None
        or stage_full is None
        or stage_active is None
        or stage_deferred is None
        or type(health_selected) is not int
        or type(health_active) is not int
        or selected_count <= 0
        or stage_full != selected_count
        or health_selected != selected_count
        or health_active != stage_active
        or stage_active <= 0
        or stage_active + stage_deferred != stage_full
    ):
        issues.append("SUBSCRIPTION_STAGING_COUNT_MISMATCH")

    metadata_hash = metadata.get("selected_universe_sha256")
    if (
        not _is_sha256(metadata_hash)
        or raw_stage.get("full_selected_universe_sha256") != metadata_hash
    ):
        issues.append("SUBSCRIPTION_STAGING_HASH_MISMATCH")
    if raw_stage.get("subscription_epoch_id") != health_payload.get(
        "subscription_epoch_id"
    ):
        issues.append("SUBSCRIPTION_STAGING_EPOCH_MISMATCH")
    stage_generation = raw_stage.get("subscription_generation")
    active_generation = health_payload.get("active_generation")
    if (
        type(stage_generation) is not int
        or type(active_generation) is not int
        or stage_generation <= 0
        or stage_generation != active_generation
    ):
        issues.append("SUBSCRIPTION_STAGING_GENERATION_MISMATCH")

    stage_requested = raw_stage.get("requested_orb_families")
    if not isinstance(stage_requested, Sequence) or isinstance(
        stage_requested, (str, bytes)
    ):
        stage_requested_set: set[str] = set()
        issues.append("SUBSCRIPTION_STAGING_REQUESTED_FAMILIES_INVALID")
    else:
        normalized_requested = [
            str(symbol).strip().upper() for symbol in stage_requested
        ]
        stage_requested_set = set(normalized_requested)
        if (
            len(stage_requested_set) != len(normalized_requested)
            or stage_requested_set != requested_orb_symbols
        ):
            missing = sorted(requested_orb_symbols - stage_requested_set)
            suffix = ",".join(missing) if missing else "set_mismatch"
            issues.append(
                f"SUBSCRIPTION_STAGING_REQUESTED_FAMILIES_MISSING:{suffix}"
            )

    primary_counts = raw_stage.get("primary_contract_counts")
    primary_counts = primary_counts if isinstance(primary_counts, Mapping) else {}
    market_statuses = health_payload.get("market_subscription_status")
    market_statuses = (
        market_statuses if isinstance(market_statuses, Mapping) else {}
    )
    market_plans = metadata.get("markets")
    market_plans = market_plans if isinstance(market_plans, Mapping) else {}
    family_statuses = health_payload.get("core_symbol_status")
    family_statuses = (
        family_statuses if isinstance(family_statuses, Mapping) else {}
    )
    aggregate_active = 0
    aggregate_deferred = 0
    aggregate_selected = 0
    complete_family_counts = True
    for symbol in sorted(requested_orb_symbols):
        market_status = market_statuses.get(symbol)
        market_plan = market_plans.get(symbol)
        family_status = family_statuses.get(symbol)
        if (
            not isinstance(market_status, Mapping)
            or not isinstance(market_plan, Mapping)
            or not isinstance(family_status, Mapping)
        ):
            issues.append(f"SUBSCRIPTION_STAGING_FAMILY_MISSING:{symbol}")
            complete_family_counts = False
            continue
        selected_for_family = count(market_status, "selected_contract_count")
        active_for_family = count(market_status, "active_contract_count")
        deferred_for_family = count(market_status, "deferred_contract_count")
        planned_selected = count(market_plan, "selected_contract_count")
        reported_active = count(family_status, "contracts_subscribed")
        primary_for_family = primary_counts.get(symbol)
        if (
            market_status.get("requested") is not True
            or selected_for_family is None
            or active_for_family is None
            or deferred_for_family is None
            or planned_selected is None
            or reported_active is None
            or selected_for_family <= 0
            or active_for_family <= 0
            or selected_for_family != planned_selected
            or reported_active != active_for_family
            or active_for_family + deferred_for_family != selected_for_family
        ):
            issues.append(f"SUBSCRIPTION_STAGING_FAMILY_COUNT_MISMATCH:{symbol}")
            complete_family_counts = False
            continue
        if type(primary_for_family) is not int or primary_for_family <= 0:
            issues.append(f"SUBSCRIPTION_STAGING_PRIMARY_COUNT_INVALID:{symbol}")
        elif primary_for_family % 2 != 0:
            issues.append(
                f"SUBSCRIPTION_STAGING_PRIMARY_PAIR_EVIDENCE_INVALID:{symbol}"
            )
        elif primary_for_family // 2 < _ORB_MIN_COMPLETE_PRIMARY_PAIRS_V1:
            issues.append(
                "SUBSCRIPTION_STAGING_ORB_PRIMARY_PAIR_MINIMUM_NOT_MET:"
                f"{symbol}:{primary_for_family // 2}"
                f"<{_ORB_MIN_COMPLETE_PRIMARY_PAIRS_V1}"
            )
        elif state == "primary_active" and primary_for_family != active_for_family:
            issues.append(f"SUBSCRIPTION_STAGING_PRIMARY_COUNT_MISMATCH:{symbol}")
        elif state == "full_active" and primary_for_family > active_for_family:
            issues.append(f"SUBSCRIPTION_STAGING_PRIMARY_COUNT_MISMATCH:{symbol}")
        aggregate_selected += selected_for_family
        aggregate_active += active_for_family
        aggregate_deferred += deferred_for_family

    if set(primary_counts) != requested_orb_symbols:
        issues.append("SUBSCRIPTION_STAGING_PRIMARY_FAMILY_SET_MISMATCH")
    if complete_family_counts and (
        aggregate_selected != stage_full
        or aggregate_active != stage_active
        or aggregate_deferred != stage_deferred
    ):
        issues.append("SUBSCRIPTION_STAGING_AGGREGATE_COUNT_MISMATCH")

    if state == "primary_active":
        if active_stage != "primary" or deferred_stage != "shadow":
            issues.append("SUBSCRIPTION_STAGING_PRIMARY_SHAPE_INVALID")
        if stage_deferred is not None and stage_deferred <= 0:
            issues.append("SUBSCRIPTION_STAGING_PRIMARY_SHAPE_INVALID")
    elif state == "full_active":
        if active_stage != "full" or deferred_stage is not None:
            issues.append("SUBSCRIPTION_STAGING_FULL_SHAPE_INVALID")
        if stage_deferred != 0 or stage_active != stage_full:
            issues.append("SUBSCRIPTION_STAGING_FULL_SHAPE_INVALID")

    return list(dict.fromkeys(issues))


def _validate_loaded_code_fingerprint(
    health_payload: Mapping[str, Any],
    *,
    observed_utc: datetime,
    session_date: str,
    project_root: Path = _PROJECT_ROOT,
) -> None:
    """Bind a live health payload to today's exact opening-critical source."""

    fingerprint = _mapping(
        health_payload.get("loaded_code_fingerprint"),
        "loaded_code_fingerprint_missing",
    )
    if set(fingerprint) != {
        "schema_version",
        "capture_semantics",
        "captured_at_utc",
        "hash_algorithm",
        "current_on_disk_recomputed",
        "current_on_disk_comparison",
        "files",
    }:
        raise MonitorOpeningAcceptanceError("loaded_code_fingerprint_fields_invalid")
    if (
        fingerprint.get("schema_version") != LOADED_CODE_FINGERPRINT_SCHEMA
        or fingerprint.get("capture_semantics") != LOADED_CODE_CAPTURE_SEMANTICS
        or fingerprint.get("hash_algorithm") != LOADED_CODE_HASH_ALGORITHM
        or fingerprint.get("current_on_disk_recomputed") is not False
        or fingerprint.get("current_on_disk_comparison")
        != "not_performed_by_health_endpoint"
    ):
        raise MonitorOpeningAcceptanceError("loaded_code_fingerprint_contract_invalid")

    captured = _parse_aware(
        fingerprint.get("captured_at_utc"),
        "loaded_code_fingerprint_captured_at_utc",
    )
    captured_utc = captured.astimezone(_UTC)
    if (
        captured.utcoffset() != timedelta(0)
        or fingerprint.get("captured_at_utc") != captured_utc.isoformat()
    ):
        raise MonitorOpeningAcceptanceError(
            "loaded_code_fingerprint_captured_at_utc_invalid"
        )
    if captured_utc > observed_utc.astimezone(_UTC):
        raise MonitorOpeningAcceptanceError("loaded_code_fingerprint_from_future")
    if captured_utc.astimezone(_CT).date().isoformat() != session_date:
        raise MonitorOpeningAcceptanceError(
            "loaded_code_fingerprint_not_current_session"
        )

    files = _mapping(
        fingerprint.get("files"), "loaded_code_fingerprint_files_missing"
    )
    if set(files) != set(OPENING_CRITICAL_SOURCE_PATHS):
        raise MonitorOpeningAcceptanceError(
            "loaded_code_fingerprint_source_set_invalid"
        )
    root = project_root.resolve()
    for relative_path in OPENING_CRITICAL_SOURCE_PATHS:
        source = _mapping(
            files.get(relative_path),
            f"loaded_code_fingerprint_source_invalid:{relative_path}",
        )
        if set(source) != {"loaded_at_startup_sha256"} or not _is_sha256(
            source.get("loaded_at_startup_sha256")
        ):
            raise MonitorOpeningAcceptanceError(
                f"loaded_code_fingerprint_source_invalid:{relative_path}"
            )
        candidate = (root / relative_path).resolve()
        try:
            candidate.relative_to(root)
            current_sha256 = hashlib.sha256(candidate.read_bytes()).hexdigest()
        except (OSError, ValueError) as exc:
            raise MonitorOpeningAcceptanceError(
                f"loaded_code_fingerprint_source_unreadable:{relative_path}"
            ) from exc
        if source.get("loaded_at_startup_sha256") != current_sha256:
            raise MonitorOpeningAcceptanceError(
                f"loaded_code_fingerprint_source_mismatch:{relative_path}"
            )


def _validate_connection_lifecycle_health(health_payload: Mapping[str, Any]) -> None:
    """Require a clean, guarded first Databento connection for opening proof."""

    for field in _CONNECTION_LIFECYCLE_ZERO_COUNTERS:
        if type(health_payload.get(field)) is not int or health_payload.get(field) != 0:
            raise MonitorOpeningAcceptanceError(
                f"connection_lifecycle_invalid:{field}"
            )
    if str(health_payload.get("connection_limit_circuit_state") or "").lower() != (
        "closed"
    ):
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:connection_limit_circuit_state"
        )
    if health_payload.get("connection_limit_retry_not_before_utc") is not None:
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:connection_limit_retry_not_before_utc"
        )
    cooldown = health_payload.get("connection_limit_cooldown_remaining_seconds")
    if (
        isinstance(cooldown, bool)
        or not isinstance(cooldown, (int, float))
        or not math.isfinite(float(cooldown))
        or float(cooldown) != 0.0
    ):
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:connection_limit_cooldown_remaining_seconds"
        )
    if health_payload.get("pre_auth_transport_guard_status") != "installed":
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:pre_auth_transport_guard_status"
        )
    if health_payload.get("last_client_close_status") != "not_attempted":
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:last_client_close_status"
        )
    if health_payload.get("last_client_close_elapsed_seconds") is not None:
        raise MonitorOpeningAcceptanceError(
            "connection_lifecycle_invalid:last_client_close_elapsed_seconds"
        )
    for field in (
        "last_pre_auth_transport_event",
        "last_pre_auth_transport_reason",
        "last_pre_auth_transport_event_utc",
    ):
        if health_payload.get(field) is not None:
            raise MonitorOpeningAcceptanceError(
                f"connection_lifecycle_invalid:{field}"
            )


def _expected_due_windows(observed_ct: datetime) -> list[str]:
    local_time = observed_ct.timetz().replace(tzinfo=None)
    return [
        name
        for boundary, name in (
            (time(8, 35), "5m"),
            (time(8, 45), "15m"),
            (time(9, 0), "30m"),
            (time(9, 30), "60m"),
        )
        if local_time >= boundary
    ]


def _bounded_nonnegative(value: Any, maximum: float) -> bool:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(parsed) and 0.0 <= parsed <= maximum


def _validate_task_evidence(report: Mapping[str, Any], observed_ct: datetime) -> None:
    clock = _mapping(report.get("clock"), "clock_report_missing")
    if (
        clock.get("applicable") is not True
        or clock.get("windows_time_synchronized") is not True
        or clock.get("leap_indicator") == 3
        or clock.get("synchronized") is False
    ):
        raise MonitorOpeningAcceptanceError("processing_clock_not_synchronized")
    if clock.get("external_offset_verified") is True:
        maximum = clock.get("maximum_allowed_absolute_offset_seconds")
        try:
            maximum_value = float(maximum)
            measured = abs(float(clock.get("median_offset_seconds")))
        except (TypeError, ValueError) as exc:
            raise MonitorOpeningAcceptanceError(
                "processing_clock_offset_invalid"
            ) from exc
        if (
            not math.isfinite(maximum_value)
            or maximum_value <= 0.0
            or maximum_value > 0.25
            or not math.isfinite(measured)
            or measured > maximum_value
        ):
            raise MonitorOpeningAcceptanceError(
                "processing_clock_offset_exceeded"
            )
    elif clock.get("synchronized") is not None:
        raise MonitorOpeningAcceptanceError("processing_clock_evidence_invalid")

    scheduled = _mapping(
        report.get("scheduled_tasks"), "scheduled_tasks_report_missing"
    )
    if scheduled.get("applicable") is not True or scheduled.get("error"):
        raise MonitorOpeningAcceptanceError("scheduled_task_inspection_invalid")
    tasks = _mapping(scheduled.get("tasks"), "scheduled_tasks_missing")
    required_true = (
        "present",
        "enabled",
        "wake_to_run",
        "task_path_valid",
        "principal_system_account",
        "logon_type_service_account",
        "action_count_valid",
        "action_shape_valid",
        "executable_matches_system32_powershell",
        "working_directory_matches_project",
        "arguments_match_contract",
        "trigger_contract_valid",
        "multiple_instances_ignore_new",
        "execution_limit_valid",
        "battery_policy_valid",
        "restart_policy_valid",
        "start_when_available_policy_valid",
        "launch_script_matches_project",
        "rut_canary_requested",
    )
    task_run_boundaries = {
        "MarketPinPredictor_AutoStart": time(7, 44),
        "MarketPinPredictor_Watchdog": time(7, 49),
    }
    for name in ("MarketPinPredictor_AutoStart", "MarketPinPredictor_Watchdog"):
        task = _mapping(tasks.get(name), f"scheduled_task_missing:{name}")
        if any(task.get(field) is not True for field in required_true):
            raise MonitorOpeningAcceptanceError(f"scheduled_task_contract_invalid:{name}")
        if str(task.get("run_level") or "").casefold() != "highest":
            raise MonitorOpeningAcceptanceError(f"scheduled_task_run_level_invalid:{name}")
        boundary = task_run_boundaries[name]
        if observed_ct.timetz().replace(tzinfo=None) >= boundary:
            last_run = _parse_aware(
                task.get("last_run_time"), f"scheduled_task_last_run_time:{name}"
            ).astimezone(_CT)
            if (
                last_run.date() != observed_ct.date()
                or last_run.timetz().replace(tzinfo=None) < boundary
                or last_run > observed_ct
            ):
                raise MonitorOpeningAcceptanceError(
                    f"scheduled_task_not_current:{name}"
                )
        if str(task.get("state") or "").casefold() != "running":
            try:
                last_result = int(task.get("last_result"))
            except (TypeError, ValueError) as exc:
                raise MonitorOpeningAcceptanceError(
                    f"scheduled_task_last_result_invalid:{name}"
                ) from exc
            if last_result != 0:
                raise MonitorOpeningAcceptanceError(
                    f"scheduled_task_last_run_failed:{name}"
                )

    startup = tasks["MarketPinPredictor_AutoStart"]
    watchdog = tasks["MarketPinPredictor_Watchdog"]
    if (
        startup.get("start_when_available") is not True
        or startup.get("clock_sync_skipped") is not False
    ):
        raise MonitorOpeningAcceptanceError("autostart_task_contract_invalid")
    if (
        watchdog.get("start_when_available") is not False
        or watchdog.get("clock_sync_skipped") is not True
    ):
        raise MonitorOpeningAcceptanceError("watchdog_task_contract_invalid")


def _validate_database_contract(
    report: Mapping[str, Any],
    *,
    require_orb_decision_contract: bool = True,
) -> None:
    database = _mapping(report.get("database"), "database_report_missing")
    if (
        database.get("present") is not True
        or database.get("error")
        or str(database.get("journal_mode") or "").lower() != "wal"
        or str(database.get("quick_check") or "").lower() != "ok"
    ):
        raise MonitorOpeningAcceptanceError("database_health_invalid")
    for field in (
        "market_structure_table_present",
        "orb_reference_table_present",
    ):
        if database.get(field) is not True:
            raise MonitorOpeningAcceptanceError(f"database_contract_invalid:{field}")
    for field in (
        "missing_market_structure_columns",
        "missing_market_structure_triggers",
        "missing_orb_reference_columns",
        "missing_orb_reference_triggers",
        "missing_orb_reference_indexes",
    ):
        if database.get(field) != []:
            raise MonitorOpeningAcceptanceError(f"database_contract_invalid:{field}")
    count_contracts = [
        ("market_structure_column_count", "expected_market_structure_column_count"),
        ("orb_reference_column_count", "expected_orb_reference_column_count"),
    ]
    if require_orb_decision_contract:
        _validate_orb_reference_decision_contract(report)
    for actual_field, expected_field in count_contracts:
        actual = database.get(actual_field)
        expected = database.get(expected_field)
        if (
            type(actual) is not int
            or type(expected) is not int
            or expected <= 0
            or actual < expected
        ):
            raise MonitorOpeningAcceptanceError(
                f"database_contract_invalid:{actual_field}"
            )


def _validate_orb_reference_decision_contract(report: Mapping[str, Any]) -> None:
    """Require the post-launch decision sidecar only for ORB acceptance."""
    database = _mapping(report.get("database"), "database_report_missing")
    if database.get("orb_reference_decision_table_present") is not True:
        raise MonitorOpeningAcceptanceError(
            "database_contract_invalid:orb_reference_decision_table_present"
        )
    for field in (
        "missing_orb_reference_decision_columns",
        "missing_orb_reference_decision_triggers",
        "missing_orb_reference_decision_indexes",
    ):
        if database.get(field) != []:
            raise MonitorOpeningAcceptanceError(
                f"database_contract_invalid:{field}"
            )
    actual = database.get("orb_reference_decision_column_count")
    expected = database.get("expected_orb_reference_decision_column_count")
    if (
        type(actual) is not int
        or type(expected) is not int
        or expected <= 0
        or actual < expected
    ):
        raise MonitorOpeningAcceptanceError(
            "database_contract_invalid:orb_reference_decision_column_count"
        )


def _validate_startup_evidence(
    report: Mapping[str, Any],
    *,
    observed_ct: datetime,
    session_date: str,
    required_families: Sequence[str] | None = None,
    allow_orb_protected_canary: bool = False,
    require_orb_decision_contract: bool = True,
) -> None:
    _validate_task_evidence(report, observed_ct)
    _validate_database_contract(
        report,
        require_orb_decision_contract=require_orb_decision_contract,
    )
    health = _mapping(report.get("backend_health"), "backend_health_missing")
    live = _mapping(report.get("backend_live_health"), "backend_live_health_missing")
    if health.get("ok") is not True:
        raise MonitorOpeningAcceptanceError("backend_health_not_ok")
    if live.get("ok") is not True:
        raise MonitorOpeningAcceptanceError("backend_live_health_not_ok")
    if _mapping(report.get("dashboard_health"), "dashboard_health_missing").get(
        "ok"
    ) is not True:
        raise MonitorOpeningAcceptanceError("dashboard_health_not_ok")
    if _mapping(report.get("orb"), "orb_report_missing").get("ok") is not True:
        raise MonitorOpeningAcceptanceError("orb_endpoint_not_ok")

    orb_payload = _mapping(
        _mapping(report.get("orb"), "orb_report_missing").get("payload"),
        "orb_payload_missing",
    )
    configured = orb_payload.get("configured_symbols")
    requested = orb_payload.get("requested_symbols")
    if (
        not isinstance(configured, Sequence)
        or isinstance(configured, (str, bytes))
        or not isinstance(requested, Sequence)
        or isinstance(requested, (str, bytes))
    ):
        raise MonitorOpeningAcceptanceError("orb_startup_symbol_sets_invalid")
    configured_set = {str(symbol).strip().upper() for symbol in configured}
    requested_set = {str(symbol).strip().upper() for symbol in requested}
    health_payload = _mapping(health.get("payload"), "backend_health_payload_missing")
    _validate_loaded_code_fingerprint(
        health_payload,
        observed_utc=observed_ct.astimezone(_UTC),
        session_date=session_date,
    )
    runtime_controls = _mapping(
        health_payload.get("runtime_controls"), "runtime_controls_missing"
    )
    sleep_prevention = _mapping(
        runtime_controls.get("sleep_prevention"), "sleep_prevention_missing"
    )
    if (
        sleep_prevention.get("requested") is not True
        or sleep_prevention.get("active") is not True
    ):
        raise MonitorOpeningAcceptanceError("sleep_prevention_inactive")
    expected_window_state = (
        "preopen"
        if observed_ct.timetz().replace(tzinfo=None) < time(8, 30)
        else "regular_session"
    )
    if (
        str(health_payload.get("provider") or "").lower() != "databento"
        or str(health_payload.get("websocket") or "").lower() != "active"
        or health_payload.get("subscription_session_state")
        != expected_window_state
        or health_payload.get("subscription_allowed") is not True
        or health_payload.get("subscription_suppressed") is not False
    ):
        raise MonitorOpeningAcceptanceError("subscription_runtime_window_invalid")
    for field in (
        "provider_queue_full_warnings",
        "provider_slow_client_warnings",
        "provider_skipped_record_warnings",
        "provider_skipped_records",
    ):
        if type(health_payload.get(field)) is not int or health_payload.get(field) != 0:
            raise MonitorOpeningAcceptanceError(f"provider_overload_invalid:{field}")
    _validate_connection_lifecycle_health(health_payload)
    health_requested = health_payload.get("symbols_requested")
    if (
        not isinstance(health_requested, Sequence)
        or isinstance(health_requested, (str, bytes))
    ):
        raise MonitorOpeningAcceptanceError("health_requested_symbols_invalid")
    health_requested_set = {
        str(symbol).strip().upper() for symbol in health_requested
    }
    if (
        len(health_requested_set) != len(health_requested)
        or not health_requested_set.issubset(set(_OPENING_SYMBOLS))
    ):
        raise MonitorOpeningAcceptanceError("health_requested_symbols_invalid")
    if required_families is None:
        required_symbols = {"SPX", "NDX", "VIX"}
        rut_orb_active = "RUT" in configured_set or "RUT" in requested_set
        if (
            observed_ct.timetz().replace(tzinfo=None) >= time(8, 25)
            or rut_orb_active
        ):
            required_symbols.add("RUT")
    else:
        required_symbols = {
            str(symbol).strip().upper() for symbol in required_families
        }
        if not required_symbols or not required_symbols.issubset(
            set(_OPENING_SYMBOLS)
        ):
            raise MonitorOpeningAcceptanceError("required_families_invalid")
    missing_configured = sorted(required_symbols - configured_set)
    if missing_configured:
        raise MonitorOpeningAcceptanceError(
            "orb_startup_configured_symbols_missing:"
            + ",".join(missing_configured)
        )
    missing_requested = sorted(required_symbols - requested_set)
    if missing_requested:
        raise MonitorOpeningAcceptanceError(
            "orb_startup_requested_symbols_missing:" + ",".join(missing_requested)
        )
    symbols = _mapping(orb_payload.get("symbols"), "orb_symbols_missing")
    missing_evidence = sorted(
        symbol
        for symbol in required_symbols
        if not isinstance(symbols.get(symbol), Mapping)
    )
    if missing_evidence:
        raise MonitorOpeningAcceptanceError(
            "orb_startup_symbol_evidence_missing:" + ",".join(missing_evidence)
        )
    required_health_symbols = set(required_symbols)
    missing_health = sorted(required_health_symbols - health_requested_set)
    if missing_health:
        raise MonitorOpeningAcceptanceError(
            "health_requested_symbols_missing:" + ",".join(missing_health)
        )
    strict_family_completeness = (
        required_families is None or required_symbols == set(_OPENING_SYMBOLS)
    )
    canary_profile = health_payload.get("optional_family_canary")
    canary_profile = canary_profile if isinstance(canary_profile, Mapping) else {}
    canary_profile_state = str(canary_profile.get("state") or "").lower()
    rut_profile_active = (
        "RUT" in (health_requested_set | configured_set | requested_set)
        or canary_profile_state not in {"", "disabled"}
    )
    expected_contract_cap = (
        _RUT_CONTRACT_SUBSCRIPTION_CAP
        if rut_profile_active
        else _CORE_CONTRACT_SUBSCRIPTION_CAP
    )
    subscribed = health_payload.get("symbols_subscribed")
    if (
        type(subscribed) is not int
        or subscribed <= 0
        or subscribed > expected_contract_cap
    ):
        raise MonitorOpeningAcceptanceError("subscription_count_invalid")
    family_status = _mapping(
        health_payload.get("core_symbol_status"),
        "core_symbol_status_missing",
    )
    epoch = health_payload.get("subscription_epoch_id")
    if not _is_sha256(epoch):
        raise MonitorOpeningAcceptanceError("subscription_epoch_invalid")
    sampler = _mapping(
        health_payload.get("orb_reference_sampler"),
        "orb_reference_sampler_missing",
    )
    if sampler.get("thread_alive") is not True or sampler.get("interval_seconds") != 5:
        raise MonitorOpeningAcceptanceError("orb_reference_sampler_invalid")
    window = _mapping(
        health_payload.get("subscription_window"), "subscription_window_missing"
    )
    window_observed = _parse_aware(
        window.get("observed_at_utc"), "subscription_window_observed_at_utc"
    ).astimezone(_UTC)
    window_age = (observed_ct.astimezone(_UTC) - window_observed).total_seconds()
    if (
        window.get("trading_date") != session_date
        or window.get("state") != expected_window_state
        or window.get("subscription_allowed") is not True
        or not 0.0 <= window_age <= _MAX_SUBSCRIPTION_WINDOW_AGE_SECONDS
    ):
        raise MonitorOpeningAcceptanceError("subscription_window_session_mismatch")
    provenance = _mapping(
        health_payload.get("universe_provenance"), "universe_provenance_missing"
    )
    metadata = _mapping(
        health_payload.get("subscription_metadata"),
        "subscription_metadata_missing",
    )
    metadata_provenance = _mapping(
        metadata.get("universe_provenance"),
        "subscription_metadata_universe_provenance_missing",
    )
    bounds = _mapping(
        health_payload.get("subscription_bounds"), "subscription_bounds_missing"
    )
    selected_count = metadata.get("selected_contract_count")
    full_count = metadata.get("full_contract_count")
    source_rows = provenance.get("source_rows")
    cap = bounds.get("max_subscription_contracts")
    provenance_fields = (
        "mode",
        "trading_date",
        "source_date",
        "source_sha256",
        "source_rows",
        "is_fallback",
    )
    if (
        health_payload.get("universe_fallback_active") is not False
        or provenance.get("mode") != "current_day_cache"
        or provenance.get("is_fallback") is not False
        or provenance.get("trading_date") != session_date
        or provenance.get("source_date") != session_date
        or not _is_sha256(provenance.get("source_sha256"))
        or type(source_rows) is not int
        or source_rows <= 0
        or any(
            metadata_provenance.get(field) != provenance.get(field)
            for field in provenance_fields
        )
        or not _is_sha256(metadata.get("selected_universe_sha256"))
        or type(selected_count) is not int
        or selected_count <= 0
        or type(full_count) is not int
        or full_count <= 0
        or type(cap) is not int
        or cap != expected_contract_cap
        or selected_count > cap
        or selected_count > source_rows
        or selected_count > full_count
        or metadata.get("reservation_shortfall_pairs") != 0
    ):
        raise MonitorOpeningAcceptanceError("current_day_universe_invalid")
    markets = _mapping(
        metadata.get("markets"), "subscription_metadata_markets_missing"
    )
    staging_issues = _subscription_staging_issues(
        health_payload, health_requested_set
    )
    if staging_issues:
        raise MonitorOpeningAcceptanceError(
            "subscription_staging_invalid:" + staging_issues[0]
        )
    staging = health_payload.get("subscription_staging")
    primary_stage_active = bool(
        isinstance(staging, Mapping)
        and str(staging.get("state") or "").strip().lower() == "primary_active"
    )
    subscribed_by_family: dict[str, int] = {}
    selected_by_family: dict[str, int] = {}
    producer_counts_complete = True
    for symbol in sorted(health_requested_set):
        status_value = family_status.get(symbol)
        market_value = markets.get(symbol)
        family_required = strict_family_completeness or symbol in required_symbols
        if not isinstance(status_value, Mapping) or not isinstance(
            market_value, Mapping
        ):
            if family_required:
                missing = (
                    f"core_symbol_status_missing:{symbol}"
                    if not isinstance(status_value, Mapping)
                    else f"subscription_market_missing:{symbol}"
                )
                raise MonitorOpeningAcceptanceError(missing)
            producer_counts_complete = False
            continue
        status = status_value
        market = market_value
        subscribed_for_family = status.get("contracts_subscribed")
        selected_for_family = market.get("selected_contract_count")
        if (
            status.get("requested") is not True
            or type(subscribed_for_family) is not int
            or subscribed_for_family < 0
            or subscribed_for_family > expected_contract_cap
            or type(selected_for_family) is not int
            or selected_for_family < 0
            or selected_for_family > expected_contract_cap
            or market.get("market_reservation_shortfall_pairs") != 0
        ):
            raise MonitorOpeningAcceptanceError(
                f"subscription_family_contract_invalid:{symbol}"
            )
        if subscribed_for_family == 0 or selected_for_family == 0:
            if family_required:
                raise MonitorOpeningAcceptanceError(
                    f"core_symbol_subscription_invalid:{symbol}"
                )
            if subscribed_for_family != selected_for_family:
                raise MonitorOpeningAcceptanceError(
                    f"subscription_family_count_mismatch:{symbol}"
                )
            producer_counts_complete = False
            continue
        subscribed_by_family[symbol] = int(subscribed_for_family)
        selected_by_family[symbol] = int(selected_for_family)
        if subscribed_for_family != selected_for_family and not primary_stage_active:
            raise MonitorOpeningAcceptanceError(
                f"subscription_family_count_mismatch:{symbol}"
            )
        if symbol in _CORE_SYMBOLS and (
            type(market.get("primary_reserved_pairs_retained")) is not int
            or market.get("primary_reserved_pairs_retained") < 100
            or type(market.get("next_listed_reserved_pairs_retained")) is not int
            or market.get("next_listed_reserved_pairs_retained") < 50
        ):
            raise MonitorOpeningAcceptanceError(
                f"subscription_market_reservation_invalid:{symbol}"
            )
    required_selected_total = sum(
        selected_by_family[symbol] for symbol in required_symbols
    )
    if (
        (not primary_stage_active and subscribed != selected_count)
        or required_selected_total > selected_count
    ):
        raise MonitorOpeningAcceptanceError("subscription_global_count_mismatch")
    if primary_stage_active and subscribed != sum(subscribed_by_family.values()):
        raise MonitorOpeningAcceptanceError("subscription_global_count_mismatch")
    if (
        strict_family_completeness or producer_counts_complete
    ) and selected_count != sum(selected_by_family.values()):
        raise MonitorOpeningAcceptanceError("subscription_global_count_mismatch")

    if required_families is None or "RUT" in required_symbols:
        canary = _mapping(
            health_payload.get("optional_family_canary"),
            "optional_family_canary_missing",
        )
        rut_active = "RUT" in health_requested_set
        valid_canary_states = (
            {"armed", "observing"}
            if rut_active
            else {"blocked", "disabled", "deferred_preopen"}
        )
        if rut_active and observed_ct.timetz().replace(tzinfo=None) < time(8, 30):
            valid_canary_states.add("deferred_preopen")
        if rut_active and allow_orb_protected_canary:
            valid_canary_states.update(
                {"protected_opening_orb", "protected_cash_session"}
            )
        canary_state = str(canary.get("state") or "").lower()
        if (
            canary.get("rollback_required") is not False
            or canary_state not in valid_canary_states
        ):
            raise MonitorOpeningAcceptanceError("optional_family_canary_invalid")
        if canary_state in {"protected_opening_orb", "protected_cash_session"}:
            canary_evidence = _mapping(
                canary.get("evaluation_evidence"),
                "optional_family_canary_evidence_missing",
            )
            if (
                canary_evidence.get("decision_state") != canary_state
                or canary_evidence.get("cash_session_reconnect_protection_active")
                is not True
                or (
                    canary_state == "protected_opening_orb"
                    and canary_evidence.get("opening_orb_protection_active")
                    is not True
                )
            ):
                raise MonitorOpeningAcceptanceError(
                    "optional_family_canary_protection_invalid"
                )


def _validate_runtime_identity(
    report: Mapping[str, Any],
    *,
    session_date: str,
    observed_ct: datetime,
    required_families: Sequence[str],
    require_calculation_gates: bool = True,
) -> tuple[str, int]:
    _validate_startup_evidence(
        report,
        observed_ct=observed_ct,
        session_date=session_date,
        required_families=required_families,
        allow_orb_protected_canary=not require_calculation_gates,
        require_orb_decision_contract=not require_calculation_gates,
    )
    health = _mapping(report.get("backend_health"), "backend_health_missing")
    live = _mapping(report.get("backend_live_health"), "backend_live_health_missing")
    health_payload = _mapping(health.get("payload"), "backend_health_payload_missing")
    epoch = health_payload.get("subscription_epoch_id")
    generation = health_payload.get("active_generation")
    if type(generation) is not int or generation <= 0:
        raise MonitorOpeningAcceptanceError("subscription_generation_invalid")
    if str(health_payload.get("handoff_status") or "").lower() != "active":
        raise MonitorOpeningAcceptanceError("backend_handoff_not_active")

    live_payload = _mapping(live.get("payload"), "backend_live_payload_missing")
    if live_payload.get("subscription_epoch_id") != epoch:
        raise MonitorOpeningAcceptanceError("live_subscription_epoch_mismatch")
    if live_payload.get("subscription_generation") != generation:
        raise MonitorOpeningAcceptanceError("live_subscription_generation_mismatch")
    if live_payload.get("active_generation") != generation:
        raise MonitorOpeningAcceptanceError("live_active_generation_mismatch")
    if str(live_payload.get("handoff_status") or "").lower() != "active":
        raise MonitorOpeningAcceptanceError("live_handoff_not_active")
    required_live_gates = [
        "stream_connected",
        "stream_progressing",
        "collection_ready",
    ]
    if require_calculation_gates:
        required_live_gates.extend(("calculation_ready", "prediction_pipeline_ok"))
    for field in required_live_gates:
        if live_payload.get(field) is not True:
            raise MonitorOpeningAcceptanceError(f"live_gate_failed:{field}")
    processing_clock = _mapping(
        health_payload.get("processing_clock_telemetry"),
        "backend_processing_clock_missing",
    )
    if processing_clock.get("status") != "synchronized":
        raise MonitorOpeningAcceptanceError(
            "backend_processing_clock_not_synchronized"
        )

    orb_payload = _mapping(
        _mapping(report.get("orb"), "orb_report_missing").get("payload"),
        "orb_payload_missing",
    )
    context = _mapping(
        orb_payload.get("active_runtime_context"), "orb_runtime_context_missing"
    )
    if (
        orb_payload.get("runtime_binding_applied") is not True
        or orb_payload.get("runtime_context_stable") is not True
        or context.get("subscription_epoch_id") != epoch
        or context.get("subscription_generation") != generation
        or str(context.get("handoff_status") or "").lower() != "active"
    ):
        raise MonitorOpeningAcceptanceError("orb_runtime_context_invalid")

    return str(epoch), int(generation)


def _validate_first_eligible_gamma_evidence(
    row: Mapping[str, Any],
    *,
    symbol: str,
    session_date: str,
    observed_utc: datetime,
) -> tuple[tuple[str, str, int, str, str, str, bool], datetime]:
    first = _mapping(
        row.get("first_eligible_calculation_bound"),
        f"market_structure_first_eligible_calculation_missing:{symbol}",
    )
    if (
        type(first.get("candidate_count")) is not int
        or int(first["candidate_count"]) <= 0
        or first.get("selection_status") != "verified"
        or first.get("selection_reason") is not None
    ):
        raise MonitorOpeningAcceptanceError(
            f"market_structure_first_eligible_calculation_invalid:{symbol}"
    )
    calculation_id = str(first.get("calculation_id") or "").strip()
    observation_id = str(first.get("observation_id") or "").strip()
    first_epoch = first.get("subscription_epoch_id")
    first_generation = first.get("subscription_generation")
    first_universe = first.get("universe_sha256")
    first_trading_date = first.get("trading_date")
    primary_expiration = first.get("primary_expiration")
    same_day = first.get("same_day_profile_available")
    if (
        not calculation_id
        or not observation_id
        or str(first.get("provider") or "").lower() != "databento"
        or str(first.get("validation_status") or "").lower() != "valid"
        or not _is_sha256(first_epoch)
        or type(first_generation) is not int
        or int(first_generation) <= 0
        or not _is_sha256(first_universe)
        or first_trading_date != session_date
        or primary_expiration != session_date
        or same_day is not True
        or _positive_finite(first.get("reference_price")) is None
        or _positive_finite(first.get("gamma_pin")) is None
        or _positive_finite(first.get("max_pain")) is None
    ):
        raise MonitorOpeningAcceptanceError(
            f"market_structure_first_eligible_calculation_invalid:{symbol}"
        )

    lineage = _mapping(
        first.get("lineage"),
        f"market_structure_first_eligible_lineage_missing:{symbol}",
    )
    if (
        lineage.get("status") != "verified"
        or lineage.get("reason") is not None
        or lineage.get("calculation_id") != calculation_id
        or lineage.get("gamma_run_present") is not True
        or lineage.get("input_blob_present") is not True
        or lineage.get("payload_integrity_verified") is not True
        or not _is_sha256(lineage.get("payload_sha256"))
        or not _bounded_nonnegative(
            lineage.get("source_to_run_seconds"), _MAX_STRUCTURE_AGE_SECONDS
        )
        or not _bounded_nonnegative(
            lineage.get("run_to_capture_seconds"), _MAX_STRUCTURE_AGE_SECONDS
        )
    ):
        raise MonitorOpeningAcceptanceError(
            f"market_structure_first_eligible_lineage_invalid:{symbol}"
        )
    try:
        source_utc = _parse_aware(
            first.get("source_timestamp_utc"), "first_eligible_source_timestamp_utc"
        ).astimezone(_UTC)
        captured_utc = _parse_aware(
            first.get("captured_at_utc"), "first_eligible_captured_at_utc"
        ).astimezone(_UTC)
        run_utc = _parse_aware(
            lineage.get("run_calculated_at_utc"),
            "first_eligible_run_calculated_at_utc",
        ).astimezone(_UTC)
        pair_completed_utc = _parse_aware(
            first.get("pair_completed_at_utc"),
            "first_eligible_pair_completed_at_utc",
        ).astimezone(_UTC)
    except MonitorOpeningAcceptanceError as exc:
        raise MonitorOpeningAcceptanceError(
            f"market_structure_first_eligible_timestamp_invalid:{symbol}"
        ) from exc

    session_day = date.fromisoformat(session_date)
    cash_open_utc = datetime.combine(session_day, time(8, 30), tzinfo=_CT).astimezone(
        _UTC
    )
    deadline_utc = cash_open_utc + timedelta(seconds=_MAX_STRUCTURE_AGE_SECONDS)
    source_to_run_seconds = (run_utc - source_utc).total_seconds()
    run_to_capture_seconds = (captured_utc - run_utc).total_seconds()
    if (
        not cash_open_utc
        <= source_utc
        <= run_utc
        <= captured_utc
        <= pair_completed_utc
        <= observed_utc
        or not math.isclose(
            float(lineage["source_to_run_seconds"]),
            source_to_run_seconds,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
        or not math.isclose(
            float(lineage["run_to_capture_seconds"]),
            run_to_capture_seconds,
            rel_tol=0.0,
            abs_tol=1e-6,
        )
    ):
        raise MonitorOpeningAcceptanceError(
            f"market_structure_first_eligible_timestamp_invalid:{symbol}"
        )
    if run_utc > deadline_utc:
        raise MonitorOpeningAcceptanceError(
            f"first_eligible_gamma_capture_late:{symbol}"
        )
    return (
        (
            "databento",
            str(first_epoch),
            int(first_generation),
            str(first_universe),
            str(first_trading_date),
            str(primary_expiration),
            bool(same_day),
        ),
        pair_completed_utc,
    )


def _validate_gamma_evidence(
    report: Mapping[str, Any], *, session_date: str, epoch: str, generation: int
) -> None:
    health_payload = _mapping(
        _mapping(report.get("backend_health"), "backend_health_missing").get(
            "payload"
        ),
        "backend_health_payload_missing",
    )
    selected_universe = _mapping(
        health_payload.get("subscription_metadata"),
        "subscription_metadata_missing",
    ).get("selected_universe_sha256")
    if not _is_sha256(selected_universe):
        raise MonitorOpeningAcceptanceError("selected_universe_invalid")
    database = _mapping(report.get("database"), "database_report_missing")
    rows = _mapping(
        database.get("market_structure_rows"),
        "market_structure_rows_missing",
    )
    orb_payload = _mapping(
        _mapping(report.get("orb"), "orb_report_missing").get("payload"),
        "orb_payload_missing",
    )
    symbols = _mapping(orb_payload.get("symbols"), "orb_symbols_missing")
    observed_utc = _parse_aware(
        report.get("observed_at_utc"), "observed_at_utc"
    ).astimezone(_UTC)
    first_pair_identity: tuple[str, str, int, str, str, str, bool] | None = None
    first_pair_completed_utc: datetime | None = None
    for symbol in _CORE_SYMBOLS:
        row = _mapping(rows.get(symbol), f"market_structure_row_missing:{symbol}")
        if int(row.get("row_count") or 0) <= 0:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_row_missing:{symbol}"
            )
        if str(row.get("latest_provider") or "").lower() != "databento":
            raise MonitorOpeningAcceptanceError(
                f"market_structure_provider_invalid:{symbol}"
            )
        if str(row.get("latest_validation_status") or "").lower() != "valid":
            raise MonitorOpeningAcceptanceError(
                f"market_structure_validation_invalid:{symbol}"
            )
        if row.get("latest_primary_expiration") != session_date:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_expiration_mismatch:{symbol}"
            )
        if row.get("latest_same_day_profile_available") is not True:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_same_day_missing:{symbol}"
            )
        if row.get("latest_subscription_epoch_id") != epoch:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_epoch_mismatch:{symbol}"
            )
        if row.get("latest_subscription_generation") != generation:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_generation_mismatch:{symbol}"
            )
        row_gamma_pin = _positive_finite(row.get("latest_gamma_pin"))
        row_max_pain = _positive_finite(row.get("latest_max_pain"))
        if row_gamma_pin is None:
            raise MonitorOpeningAcceptanceError(f"gamma_pin_unavailable:{symbol}")
        if row_max_pain is None:
            raise MonitorOpeningAcceptanceError(f"max_pain_unavailable:{symbol}")
        if (
            not _bounded_nonnegative(
                row.get("latest_source_age_seconds"), _MAX_STRUCTURE_AGE_SECONDS
            )
            or not _bounded_nonnegative(
                row.get("latest_capture_age_seconds"), _MAX_STRUCTURE_AGE_SECONDS
            )
            or _positive_finite(row.get("latest_reference_price")) is None
            or row.get("latest_universe_sha256") != selected_universe
        ):
            raise MonitorOpeningAcceptanceError(
                f"market_structure_freshness_invalid:{symbol}"
            )
        pair_identity, pair_completed_utc = _validate_first_eligible_gamma_evidence(
            row,
            symbol=symbol,
            session_date=session_date,
            observed_utc=observed_utc,
        )
        if first_pair_identity is None:
            first_pair_identity = pair_identity
            first_pair_completed_utc = pair_completed_utc
        elif (
            pair_identity != first_pair_identity
            or pair_completed_utc != first_pair_completed_utc
        ):
            raise MonitorOpeningAcceptanceError(
                "first_eligible_gamma_capture_identity_mismatch"
            )

        symbol_state = _mapping(symbols.get(symbol), f"orb_symbol_missing:{symbol}")
        pin = _mapping(
            symbol_state.get("pin_behavior"), f"orb_pin_behavior_missing:{symbol}"
        )
        provenance = _mapping(
            symbol_state.get("provenance"), f"orb_provenance_missing:{symbol}"
        )
        semantics = _mapping(
            symbol_state.get("reference_semantics"),
            f"orb_reference_semantics_missing:{symbol}",
        )
        last_structure = _mapping(
            symbol_state.get("last_known_structure"),
            f"orb_last_known_structure_missing:{symbol}",
        )
        calculation_bound = _mapping(
            row.get("latest_calculation_bound"),
            f"market_structure_calculation_bound_missing:{symbol}",
        )
        if int(calculation_bound.get("row_count") or 0) <= 0:
            raise MonitorOpeningAcceptanceError(
                f"market_structure_calculation_bound_missing:{symbol}"
            )
        calculation_id = str(
            calculation_bound.get("calculation_id") or ""
        ).strip()
        bound_gamma = _positive_finite(calculation_bound.get("gamma_pin"))
        bound_max_pain = _positive_finite(calculation_bound.get("max_pain"))
        if (
            not calculation_id
            or str(calculation_bound.get("provider") or "").lower()
            != "databento"
            or str(calculation_bound.get("validation_status") or "").lower()
            != "valid"
            or calculation_bound.get("subscription_epoch_id") != epoch
            or calculation_bound.get("subscription_generation") != generation
            or calculation_bound.get("universe_sha256") != selected_universe
            or calculation_bound.get("primary_expiration") != session_date
            or calculation_bound.get("same_day_profile_available") is not True
            or _positive_finite(calculation_bound.get("reference_price")) is None
            or bound_gamma is None
            or bound_max_pain is None
            or not _bounded_nonnegative(
                calculation_bound.get("source_age_seconds"),
                _MAX_STRUCTURE_AGE_SECONDS,
            )
            or not _bounded_nonnegative(
                calculation_bound.get("capture_age_seconds"),
                _MAX_STRUCTURE_AGE_SECONDS,
            )
            or not _bounded_nonnegative(
                calculation_bound.get("lag_from_latest_source_seconds"),
                _MAX_STRUCTURE_AGE_SECONDS,
            )
        ):
            raise MonitorOpeningAcceptanceError(
                f"market_structure_calculation_bound_invalid:{symbol}"
            )
        lineage = _mapping(
            calculation_bound.get("lineage"),
            f"market_structure_calculation_lineage_missing:{symbol}",
        )
        if (
            lineage.get("status") != "verified"
            or lineage.get("reason") is not None
            or lineage.get("calculation_id") != calculation_id
            or lineage.get("gamma_run_present") is not True
            or lineage.get("input_blob_present") is not True
            or lineage.get("payload_integrity_verified") is not True
            or not _is_sha256(lineage.get("payload_sha256"))
            or not _bounded_nonnegative(
                lineage.get("run_age_seconds"), _MAX_STRUCTURE_AGE_SECONDS
            )
        ):
            raise MonitorOpeningAcceptanceError(
                f"market_structure_calculation_lineage_invalid:{symbol}"
            )
        orb_calculation_bound = _mapping(
            symbol_state.get("last_calculation_bound_structure"),
            f"orb_calculation_bound_structure_missing:{symbol}",
        )
        if (
            orb_calculation_bound.get("status") != "aligned"
            or orb_calculation_bound.get("level_availability_status")
            != "available"
            or orb_calculation_bound.get("calculation_id") != calculation_id
            or orb_calculation_bound.get("provider") != "databento"
            or orb_calculation_bound.get("subscription_epoch_id") != epoch
            or orb_calculation_bound.get("subscription_generation") != generation
            or orb_calculation_bound.get("universe_sha256") != selected_universe
            or orb_calculation_bound.get("primary_expiration") != session_date
            or orb_calculation_bound.get("same_day_profile_available") is not True
            or orb_calculation_bound.get("current_provenance_aligned") is not True
            or orb_calculation_bound.get("runtime_aligned") is not True
            or orb_calculation_bound.get("evidence_eligible") is not True
            or _positive_finite(orb_calculation_bound.get("gamma_pin"))
            != bound_gamma
            or _positive_finite(orb_calculation_bound.get("max_pain"))
            != bound_max_pain
            or orb_calculation_bound.get("source_timestamp_utc")
            != calculation_bound.get("source_timestamp_utc")
            or orb_calculation_bound.get("captured_at_utc")
            != calculation_bound.get("captured_at_utc")
            or not _bounded_nonnegative(
                orb_calculation_bound.get("age_seconds"),
                _MAX_STRUCTURE_AGE_SECONDS,
            )
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_calculation_bound_structure_invalid:{symbol}"
            )
        if pin.get("level_availability_status") != "available":
            raise MonitorOpeningAcceptanceError(f"orb_pin_levels_unavailable:{symbol}")
        pin_gamma = _positive_finite(pin.get("gamma_pin"))
        pin_max_pain = _positive_finite(pin.get("max_pain"))
        if pin_gamma is None:
            raise MonitorOpeningAcceptanceError(f"orb_gamma_pin_unavailable:{symbol}")
        if pin_max_pain is None:
            raise MonitorOpeningAcceptanceError(f"orb_max_pain_unavailable:{symbol}")
        last_gamma = _positive_finite(last_structure.get("gamma_pin"))
        last_max_pain = _positive_finite(last_structure.get("max_pain"))
        current_calculation_id = (
            str(row.get("latest_calculation_id") or "").strip() or None
        )
        last_calculation_id = (
            str(last_structure.get("calculation_id") or "").strip() or None
        )
        if (
            last_structure.get("status") != "aligned"
            or last_structure.get("level_availability_status") != "available"
            or last_gamma is None
            or last_max_pain is None
            or last_calculation_id != current_calculation_id
            or row_gamma_pin != pin_gamma
            or row_gamma_pin != last_gamma
            or row_max_pain != pin_max_pain
            or row_max_pain != last_max_pain
            or not _bounded_nonnegative(
                last_structure.get("age_seconds"), _MAX_STRUCTURE_AGE_SECONDS
            )
            or provenance.get("runtime_binding_applied") is not True
            or provenance.get("active_runtime_epoch_aligned") is not True
            or provenance.get("active_subscription_epoch_id") != epoch
            or provenance.get("active_subscription_generation") != generation
            or provenance.get("subscription_generations") != [generation]
            or str(provenance.get("active_handoff_status") or "").lower()
            != "active"
            or provenance.get("structure_vs_reference_aligned") is not True
            or provenance.get("structure_reference_status") != "aligned"
            or provenance.get("structure_reference_fresh") is not True
            or not _bounded_nonnegative(
                provenance.get("structure_reference_age_seconds"),
                _MAX_STRUCTURE_AGE_SECONDS,
            )
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_structure_provenance_invalid:{symbol}"
            )
        if (
            semantics.get("primary_expiration") != session_date
            or semantics.get("same_day_profile_available") is not True
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_structure_expiration_invalid:{symbol}"
            )
    if first_pair_completed_utc is None:
        raise MonitorOpeningAcceptanceError(
            "market_structure_first_eligible_pair_completion_missing"
        )
    session_day = date.fromisoformat(session_date)
    cash_open_utc = datetime.combine(
        session_day, time(8, 30), tzinfo=_CT
    ).astimezone(_UTC)
    deadline_utc = cash_open_utc + timedelta(seconds=_MAX_STRUCTURE_AGE_SECONDS)
    if first_pair_completed_utc > deadline_utc:
        raise MonitorOpeningAcceptanceError(
            "first_eligible_gamma_capture_late:PAIR"
        )


def _validate_orb_window(
    report: Mapping[str, Any],
    *,
    window_name: str,
    session_date: str,
    epoch: str,
    generation: int,
) -> None:
    _validate_orb_reference_decision_contract(report)
    orb_payload = _mapping(
        _mapping(report.get("orb"), "orb_report_missing").get("payload"),
        "orb_payload_missing",
    )
    configured = orb_payload.get("configured_symbols")
    if not isinstance(configured, Sequence) or isinstance(configured, (str, bytes)):
        raise MonitorOpeningAcceptanceError("orb_configured_symbols_invalid")
    configured_set = {str(symbol).upper() for symbol in configured}
    missing = sorted(set(_OPENING_SYMBOLS) - configured_set)
    if missing:
        raise MonitorOpeningAcceptanceError(
            "orb_required_symbols_missing:" + ",".join(missing)
        )
    symbols = _mapping(orb_payload.get("symbols"), "orb_symbols_missing")
    reference_rows = _mapping(
        _mapping(report.get("database"), "database_report_missing").get(
            "orb_reference_rows"
        ),
        "orb_reference_rows_missing",
    )
    reference_decisions = _mapping(
        _mapping(report.get("database"), "database_report_missing").get(
            "orb_reference_progress_decisions"
        ),
        "orb_reference_progress_decisions_missing",
    )
    for symbol in _OPENING_SYMBOLS:
        symbol_state = _mapping(symbols.get(symbol), f"orb_symbol_missing:{symbol}")
        windows = _mapping(
            symbol_state.get("opening_ranges"), f"orb_ranges_missing:{symbol}"
        )
        window = _mapping(
            windows.get(window_name), f"orb_{window_name}_missing:{symbol}"
        )
        duration_minutes = int(window_name[:-1])
        expected_start = datetime.combine(
            date.fromisoformat(session_date), time(8, 30), tzinfo=_CT
        ).astimezone(_UTC)
        expected_end = expected_start + timedelta(minutes=duration_minutes)
        if (
            type(window.get("duration_minutes")) is not int
            or window.get("duration_minutes") != duration_minutes
            or _parse_aware(
                window.get("range_start_utc"),
                f"orb_{window_name}_range_start_utc:{symbol}",
            ).astimezone(_UTC)
            != expected_start
            or _parse_aware(
                window.get("range_end_utc"),
                f"orb_{window_name}_range_end_utc:{symbol}",
            ).astimezone(_UTC)
            != expected_end
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_window_identity_invalid:{symbol}"
            )
        if (
            window.get("capture_status") != "complete"
            or window.get("orb_complete") is not True
            or window.get("clock_status") != "closed"
            or window.get("current_reference_fresh") is not True
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_incomplete:{symbol}"
            )
        capture = _mapping(
            window.get("capture_evidence"),
            f"orb_{window_name}_capture_evidence_missing:{symbol}",
        )
        sample_count = capture.get("sample_count")
        expected_sample_count = capture.get("expected_sample_count")
        final_expected_count = duration_minutes * 60 // 5
        if (
            type(sample_count) is not int
            or type(expected_sample_count) is not int
            or sample_count <= 1
            or expected_sample_count != final_expected_count
            or sample_count > expected_sample_count
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_capture_counts_invalid:{symbol}"
            )
        try:
            ratio = float(capture.get("capture_ratio"))
        except (TypeError, ValueError) as exc:
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_capture_ratio_invalid:{symbol}"
            ) from exc
        coherent_ratio = sample_count / expected_sample_count
        if (
            not math.isfinite(ratio)
            or ratio < _CAPTURE_RATIO_FLOOR
            or ratio > 1.0
            or not math.isclose(ratio, coherent_ratio, rel_tol=0.0, abs_tol=1e-12)
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_capture_ratio_low:{symbol}"
            )
        if capture.get("opening_bucket_present") is not True:
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_opening_bucket_missing:{symbol}"
            )
        try:
            first_lag = float(capture.get("first_sample_lag_seconds"))
            end_gap = float(capture.get("end_gap_seconds"))
            max_gap = float(capture.get("max_gap_seconds"))
        except (TypeError, ValueError) as exc:
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_capture_timing_invalid:{symbol}"
            ) from exc
        if (
            not math.isfinite(first_lag)
            or first_lag != 0.0
            or not math.isfinite(end_gap)
            or not 0.0 <= end_gap <= 30.0
            or not math.isfinite(max_gap)
            or not 0.0 <= max_gap <= 30.0
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_capture_timing_invalid:{symbol}"
            )
        opening_price = _positive_finite(window.get("opening_price"))
        orb_high = _positive_finite(window.get("orb_high"))
        orb_low = _positive_finite(window.get("orb_low"))
        current_price = _positive_finite(window.get("current_price"))
        if (
            opening_price is None
            or orb_high is None
            or orb_low is None
            or current_price is None
            or orb_low > orb_high
            or not (orb_low <= opening_price <= orb_high)
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_{window_name}_price_evidence_invalid:{symbol}"
            )
        if symbol in _CORE_SYMBOLS:
            if window.get("directional_evidence_eligible") is not True:
                raise MonitorOpeningAcceptanceError(
                    f"orb_{window_name}_directional_ineligible:{symbol}"
                )
        elif symbol == "VIX":
            if window.get("directional_evidence_eligible") is not False:
                raise MonitorOpeningAcceptanceError("vix_orb_authority_invalid")
        else:
            semantics = _mapping(
                symbol_state.get("reference_semantics"),
                "rut_reference_semantics_missing",
            )
            semantics_kind = semantics.get("kind")
            primary_expiration = semantics.get("primary_expiration")
            rut_context_issue_present = "RUT_ORB_CONTEXT_ONLY" in set(
                report.get("out_of_scope_issues") or []
            )
            if (
                semantics.get("source") != "databento_opra_put_call_parity"
                or semantics.get("authority") != "research_reference_only"
            ):
                raise MonitorOpeningAcceptanceError(
                    "rut_orb_classification_invalid"
                )
            if semantics_kind == "same_day_index_option_parity":
                if (
                    window.get("directional_evidence_eligible") is not True
                    or semantics.get("directional_base_eligible") is not True
                    or primary_expiration != session_date
                    or semantics.get("same_day_profile_available") is not True
                    or semantics.get("limitation")
                    != (
                        "Same-day index-option parity is a sampled reference, "
                        "not official exchange OHLC."
                    )
                    or rut_context_issue_present
                ):
                    raise MonitorOpeningAcceptanceError(
                        "rut_orb_classification_invalid"
                    )
            elif semantics_kind == "non_same_day_index_option_forward_context":
                try:
                    primary_expiration_date = date.fromisoformat(primary_expiration)
                except (TypeError, ValueError) as exc:
                    raise MonitorOpeningAcceptanceError(
                        "rut_orb_classification_invalid"
                    ) from exc
                if (
                    semantics.get("directional_base_eligible") is not False
                    or semantics.get("same_day_profile_available") is not False
                    or primary_expiration_date <= date.fromisoformat(session_date)
                    or semantics.get("limitation")
                    != (
                        "The primary option expiry is not same-day, so its "
                        "parity level is context only."
                    )
                    or not rut_context_issue_present
                ):
                    raise MonitorOpeningAcceptanceError(
                        "rut_orb_classification_invalid"
                    )
                if window.get("directional_evidence_eligible") is not False:
                    raise MonitorOpeningAcceptanceError(
                        "rut_orb_directional_promotion_invalid"
                    )
            else:
                raise MonitorOpeningAcceptanceError(
                    "rut_orb_classification_invalid"
                )

        provenance = _mapping(
            symbol_state.get("provenance"), f"orb_provenance_missing:{symbol}"
        )
        if (
            provenance.get("range_provenance_aligned") is not True
            or provenance.get("current_vs_range_aligned") is not True
            or provenance.get("active_runtime_epoch_aligned") is not True
            or provenance.get("active_subscription_epoch_id") != epoch
            or provenance.get("active_subscription_generation") != generation
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_runtime_provenance_invalid:{symbol}"
            )
        row = _mapping(
            reference_rows.get(symbol), f"orb_reference_row_missing:{symbol}"
        )
        decision_row = _mapping(
            reference_decisions.get(symbol),
            f"orb_reference_decision_row_missing:{symbol}",
        )
        decision_counts = tuple(
            decision_row.get(field)
            for field in (
                "raw_row_count",
                "pending_decision_count",
                "ineligible_decision_count",
                "eligible_decision_count",
            )
        )
        if any(type(value) is not int or value < 0 for value in decision_counts):
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_decision_counts_invalid:{symbol}"
            )
        raw_count, pending_count, ineligible_count, eligible_count = decision_counts
        if pending_count != 0:
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_decision_pending:{symbol}"
            )
        if (
            raw_count != pending_count + ineligible_count + eligible_count
            or eligible_count != int(row.get("row_count") or 0)
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_decision_counts_mismatch:{symbol}"
            )
        if (
            int(row.get("row_count") or 0) <= 0
            or row.get("opening_bucket_present") is not True
            or row.get("advancing_5s_evidence") is not True
            or row.get("provider_source_timestamps_advancing") is not True
            or type(row.get("invalid_subscription_epoch_row_count")) is not int
            or row.get("invalid_subscription_epoch_row_count") != 0
            or row.get("mixed_subscription_epoch_rows") is not False
            or row.get("subscription_epoch_ids") != [epoch]
            or row.get("latest_subscription_epoch_id") != epoch
            or type(row.get("invalid_subscription_generation_row_count"))
            is not int
            or row.get("invalid_subscription_generation_row_count") != 0
            or row.get("mixed_subscription_generation_rows") is not False
            or row.get("subscription_generations") != [generation]
            or row.get("latest_subscription_generation") != generation
            or str(row.get("latest_provider") or "").lower() != "databento"
            or str(row.get("latest_validation_status") or "").lower() != "valid"
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_row_invalid:{symbol}"
            )
        try:
            persisted_ratio = float(row.get("opening_capture_ratio"))
        except (TypeError, ValueError) as exc:
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_capture_ratio_invalid:{symbol}"
            ) from exc
        if (
            not math.isfinite(persisted_ratio)
            or persisted_ratio < _CAPTURE_RATIO_FLOOR
            or persisted_ratio > 1.0
        ):
            raise MonitorOpeningAcceptanceError(
                f"orb_reference_capture_ratio_low:{symbol}"
            )
        if symbol == "RUT" and (
            row.get("latest_primary_expiration")
            != semantics.get("primary_expiration")
            or row.get("latest_same_day_profile_available")
            is not semantics.get("same_day_profile_available")
        ):
            raise MonitorOpeningAcceptanceError(
                "rut_orb_classification_mismatch"
            )


def _normalize_report(
    raw_report: Mapping[str, Any], *, milestone: str
) -> tuple[dict[str, Any], datetime, str]:
    if not isinstance(raw_report, Mapping):
        raise MonitorOpeningAcceptanceError("inspector_report_must_be_an_object")
    try:
        encoded = _canonical_json_bytes(raw_report)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MonitorOpeningAcceptanceError("inspector_report_not_canonical_json") from exc
    if len(encoded) > _MAX_COMPACT_REPORT_BYTES:
        raise MonitorOpeningAcceptanceError("inspector_report_exceeds_size_limit")
    normalized = copy.deepcopy(dict(raw_report))
    if set(normalized) != _COMPACT_REPORT_FIELDS:
        raise MonitorOpeningAcceptanceError("inspector_compact_fields_invalid")
    if normalized.get("schema_version") != "marketpin-opening-readiness.v1":
        raise MonitorOpeningAcceptanceError("inspector_schema_invalid")
    if normalized.get("output_mode") != "compact":
        raise MonitorOpeningAcceptanceError("inspector_output_must_be_compact")
    if normalized.get("read_only") is not True:
        raise MonitorOpeningAcceptanceError("inspector_report_not_read_only")
    if normalized.get("state") != "ready":
        raise MonitorOpeningAcceptanceError("inspector_state_not_ready")
    if normalized.get("issues") != []:
        raise MonitorOpeningAcceptanceError("inspector_issues_not_empty")
    if normalized.get("acceptance_scope") != milestone:
        raise MonitorOpeningAcceptanceError("inspector_acceptance_scope_mismatch")
    out_of_scope = normalized.get("out_of_scope_issues")
    if (
        not isinstance(out_of_scope, list)
        or len(out_of_scope) > 64
        or any(
            not isinstance(issue, str) or not issue
            for issue in out_of_scope
        )
    ):
        raise MonitorOpeningAcceptanceError("inspector_out_of_scope_issues_invalid")
    if len(set(out_of_scope)) != len(out_of_scope):
        raise MonitorOpeningAcceptanceError("inspector_out_of_scope_issues_invalid")
    if milestone == "first_eligible_gamma_capture":
        if any(
            not _is_gamma_scope_excluded_issue(issue)
            for issue in out_of_scope
        ):
            raise MonitorOpeningAcceptanceError(
                "inspector_out_of_scope_issue_invalid"
            )
    elif milestone not in {"complete_5m_orb", "complete_60m_orb"}:
        if out_of_scope:
            raise MonitorOpeningAcceptanceError(
                "inspector_out_of_scope_issues_not_allowed"
            )
    elif any(not _is_orb_scope_excluded_issue(issue) for issue in out_of_scope):
        raise MonitorOpeningAcceptanceError("inspector_out_of_scope_issue_invalid")
    for field in ("warnings", "notes"):
        values = normalized.get(field)
        if (
            not isinstance(values, list)
            or len(values) > 64
            or any(
                not isinstance(item, str) or len(item) > 160
                for item in values
            )
        ):
            raise MonitorOpeningAcceptanceError(f"inspector_{field}_invalid")
    authorities = _mapping(
        normalized.get("authorities"), "inspector_authorities_missing"
    )
    if (
        authorities.get("backend_health") != "/health"
        or authorities.get("backend_live_health") != "/health/live"
        or authorities.get("orb") != "/v1/orb"
        or not str(authorities.get("database") or "").strip()
    ):
        raise MonitorOpeningAcceptanceError("inspector_authorities_invalid")

    observed_ct = _parse_aware(normalized.get("observed_at_ct"), "observed_at_ct")
    observed_utc = _parse_aware(
        normalized.get("observed_at_utc"), "observed_at_utc"
    ).astimezone(_UTC)
    if observed_ct.astimezone(_UTC) != observed_utc:
        raise MonitorOpeningAcceptanceError("inspector_timestamp_mismatch")
    observed_ct = observed_utc.astimezone(_CT)
    session_date = observed_ct.date().isoformat()
    calendar = market_calendar_status(observed_ct.date())
    if calendar.get("supported") is not True:
        raise MonitorOpeningAcceptanceError("market_calendar_unverified")
    if calendar.get("market_open") is not True:
        raise MonitorOpeningAcceptanceError("milestone_requires_open_session_date")

    spec = _MILESTONE_BY_NAME[milestone]
    local_time = observed_ct.timetz().replace(tzinfo=None)
    close_time = time(12, 0) if is_early_close_day(observed_ct.date()) else time(15, 0)
    if local_time < spec[3]:
        raise MonitorOpeningAcceptanceError(f"milestone_not_due:{milestone}")
    if local_time >= close_time:
        raise MonitorOpeningAcceptanceError("milestone_evidence_must_precede_cash_close")
    expected_due = _expected_due_windows(observed_ct)
    if normalized.get("due_orb_windows") != expected_due:
        raise MonitorOpeningAcceptanceError("inspector_due_windows_mismatch")
    expected_latest = expected_due[-1] if expected_due else None
    if normalized.get("due_orb_window") != expected_latest:
        raise MonitorOpeningAcceptanceError("inspector_due_window_mismatch")

    if milestone == "startup":
        _validate_startup_evidence(
            normalized, observed_ct=observed_ct, session_date=session_date
        )
    else:
        epoch, generation = _validate_runtime_identity(
            normalized,
            session_date=session_date,
            observed_ct=observed_ct,
            required_families=(
                _CORE_SYMBOLS
                if milestone == "first_eligible_gamma_capture"
                else _OPENING_SYMBOLS
            ),
            require_calculation_gates=(
                milestone == "first_eligible_gamma_capture"
            ),
        )
    if milestone == "first_eligible_gamma_capture":
        _validate_gamma_evidence(
            normalized,
            session_date=session_date,
            epoch=epoch,
            generation=generation,
        )
    if milestone == "complete_5m_orb":
        _validate_orb_window(
            normalized,
            window_name="5m",
            session_date=session_date,
            epoch=epoch,
            generation=generation,
        )
    elif milestone == "complete_60m_orb":
        for window_name in ("5m", "15m", "30m", "60m"):
            _validate_orb_window(
                normalized,
                window_name=window_name,
                session_date=session_date,
                epoch=epoch,
                generation=generation,
            )

    normalized["observed_at_ct"] = observed_ct.isoformat()
    normalized["observed_at_utc"] = observed_utc.isoformat().replace(
        "+00:00", "Z"
    )
    return normalized, observed_utc, session_date


def _selected(source: Any, fields: Sequence[str]) -> dict[str, Any]:
    if not isinstance(source, Mapping):
        return {}
    return {
        field: copy.deepcopy(source[field])
        for field in fields
        if field in source
    }


def _bounded_report_projection(report: Mapping[str, Any]) -> dict[str, Any]:
    """Keep only validated acceptance evidence; never journal caller extensions."""

    health = _mapping(report.get("backend_health"), "backend_health_missing")
    health_payload = _mapping(health.get("payload"), "backend_health_payload_missing")
    live = _mapping(report.get("backend_live_health"), "backend_live_health_missing")
    live_payload = _mapping(live.get("payload"), "backend_live_payload_missing")
    orb = _mapping(report.get("orb"), "orb_report_missing")
    orb_payload = _mapping(orb.get("payload"), "orb_payload_missing")
    raw_symbols = _mapping(orb_payload.get("symbols"), "orb_symbols_missing")
    due_windows = list(report.get("due_orb_windows") or [])
    symbols: dict[str, Any] = {}
    for symbol in _OPENING_SYMBOLS:
        if not isinstance(raw_symbols.get(symbol), Mapping):
            continue
        raw_symbol = _mapping(raw_symbols[symbol], f"orb_symbol_missing:{symbol}")
        ranges = _mapping(
            raw_symbol.get("opening_ranges"), f"orb_ranges_missing:{symbol}"
        )
        symbols[symbol] = {
            "reference_semantics": _selected(
                raw_symbol.get("reference_semantics"),
                (
                    "kind",
                    "source",
                    "authority",
                    "primary_expiration",
                    "same_day_profile_available",
                    "directional_base_eligible",
                    "limitation",
                ),
            ),
            "pin_behavior": _selected(
                raw_symbol.get("pin_behavior"),
                ("level_availability_status", "gamma_pin", "max_pain"),
            ),
            "last_known_structure": _selected(
                raw_symbol.get("last_known_structure"),
                (
                    "status",
                    "level_availability_status",
                    "age_seconds",
                    "maximum_current_age_seconds",
                    "gamma_pin",
                    "max_pain",
                    "zero_gamma",
                    "calculation_id",
                ),
            ),
            "last_calculation_bound_structure": _selected(
                raw_symbol.get("last_calculation_bound_structure"),
                (
                    "status",
                    "level_availability_status",
                    "source_timestamp_utc",
                    "captured_at_utc",
                    "freshness_timestamp_utc",
                    "age_seconds",
                    "source_age_seconds",
                    "capture_age_seconds",
                    "maximum_current_age_seconds",
                    "reference_price",
                    "gamma_pin",
                    "max_pain",
                    "zero_gamma",
                    "calculation_id",
                    "provider",
                    "subscription_epoch_id",
                    "subscription_generation",
                    "universe_sha256",
                    "primary_expiration",
                    "same_day_profile_available",
                    "current_provenance_aligned",
                    "runtime_aligned",
                    "evidence_eligible",
                ),
            ),
            "provenance": _selected(
                raw_symbol.get("provenance"),
                (
                    "runtime_binding_applied",
                    "active_runtime_epoch_aligned",
                    "active_subscription_epoch_id",
                    "active_subscription_generation",
                    "subscription_generations",
                    "active_handoff_status",
                    "range_provenance_aligned",
                    "current_vs_range_aligned",
                    "structure_vs_reference_aligned",
                    "structure_reference_status",
                    "structure_reference_fresh",
                    "structure_reference_age_seconds",
                    "structure_reference_max_age_seconds",
                ),
            ),
            "opening_ranges": {
                window_name: {
                    **_selected(
                        ranges.get(window_name),
                        (
                            "duration_minutes",
                            "range_start_utc",
                            "range_end_utc",
                            "clock_status",
                            "capture_status",
                            "orb_complete",
                            "opening_price",
                            "orb_high",
                            "orb_low",
                            "current_price",
                            "current_reference_fresh",
                            "directional_evidence_eligible",
                            "combined_structure_directional_evidence_eligible",
                        ),
                    ),
                    "capture_evidence": _selected(
                        (ranges.get(window_name) or {}).get("capture_evidence"),
                        (
                            "sample_count",
                            "expected_sample_count",
                            "capture_ratio",
                            "opening_bucket_present",
                            "first_sample_lag_seconds",
                            "end_gap_seconds",
                            "max_gap_seconds",
                            "current_reference_age_seconds",
                        ),
                    ),
                }
                for window_name in due_windows
                if isinstance(ranges.get(window_name), Mapping)
            },
        }

    database = _mapping(report.get("database"), "database_report_missing")
    structure_rows = database.get("market_structure_rows") or {}
    reference_rows = database.get("orb_reference_rows") or {}
    scheduled = _mapping(
        report.get("scheduled_tasks"), "scheduled_tasks_report_missing"
    )
    raw_tasks = scheduled.get("tasks") or {}
    task_fields = (
        "present",
        "enabled",
        "state",
        "last_result",
        "wake_to_run",
        "task_path_valid",
        "principal_system_account",
        "logon_type_service_account",
        "action_count_valid",
        "action_shape_valid",
        "executable_matches_system32_powershell",
        "working_directory_matches_project",
        "arguments_match_contract",
        "trigger_contract_valid",
        "multiple_instances_ignore_new",
        "execution_limit_valid",
        "battery_policy_valid",
        "restart_policy_valid",
        "start_when_available_policy_valid",
        "launch_script_matches_project",
        "rut_canary_requested",
        "run_level",
        "start_when_available",
        "clock_sync_skipped",
        "last_run_time",
    )
    return {
        "schema_version": report["schema_version"],
        "output_mode": "compact",
        "state": report["state"],
        "observed_at_ct": report["observed_at_ct"],
        "observed_at_utc": report["observed_at_utc"],
        "issues": [],
        "acceptance_scope": report["acceptance_scope"],
        "out_of_scope_issues": list(report["out_of_scope_issues"]),
        "warnings": list(report.get("warnings", [])),
        "due_orb_windows": due_windows,
        "due_orb_window": report.get("due_orb_window"),
        "clock": _selected(
            report.get("clock"),
            (
                "applicable",
                "status",
                "synchronized",
                "windows_time_synchronized",
                "external_offset_verified",
                "leap_indicator",
                "median_offset_seconds",
                "maximum_allowed_absolute_offset_seconds",
            ),
        ),
        "backend_health": {
            **_selected(health, ("ok", "status_code")),
            "payload": {
                **_selected(
                    health_payload,
                    (
                        "provider",
                        "websocket",
                        "subscription_session_state",
                        "subscription_allowed",
                        "subscription_suppressed",
                        "subscription_epoch_id",
                        "active_generation",
                        "handoff_status",
                        "universe_fallback_active",
                        "symbols_requested",
                        "symbols_subscribed",
                        "symbols_selected",
                        "provider_queue_full_warnings",
                        "provider_slow_client_warnings",
                        "provider_skipped_record_warnings",
                        "provider_skipped_records",
                        *_CONNECTION_LIFECYCLE_COMPACT_FIELDS,
                    ),
                ),
                "processing_clock_telemetry": _selected(
                    health_payload.get("processing_clock_telemetry"),
                    ("status",),
                ),
                "loaded_code_fingerprint": copy.deepcopy(
                    health_payload.get("loaded_code_fingerprint")
                ),
                "runtime_controls": {
                    "sleep_prevention": _selected(
                        (health_payload.get("runtime_controls") or {}).get(
                            "sleep_prevention"
                        ),
                        ("requested", "active"),
                    )
                },
                "universe_provenance": _selected(
                    health_payload.get("universe_provenance"),
                    (
                        "mode",
                        "trading_date",
                        "source_date",
                        "source_sha256",
                        "source_rows",
                        "is_fallback",
                    ),
                ),
                "subscription_metadata": {
                    **_selected(
                        health_payload.get("subscription_metadata"),
                        (
                            "full_contract_count",
                            "selected_contract_count",
                            "selected_universe_sha256",
                            "reservation_shortfall_pairs",
                        ),
                    ),
                    "universe_provenance": _selected(
                        (health_payload.get("subscription_metadata") or {}).get(
                            "universe_provenance"
                        ),
                        (
                            "mode",
                            "trading_date",
                            "source_date",
                            "source_sha256",
                            "source_rows",
                            "is_fallback",
                        ),
                    ),
                    "markets": {
                        symbol: _selected(
                            (
                                (
                                    health_payload.get("subscription_metadata")
                                    or {}
                                ).get("markets")
                                or {}
                            ).get(symbol),
                            (
                                "selected_contract_count",
                                "market_reservation_shortfall_pairs",
                                "primary_reserved_pairs_retained",
                                "next_listed_reserved_pairs_retained",
                            ),
                        )
                        for symbol in _OPENING_SYMBOLS
                        if isinstance(
                            (
                                (
                                    health_payload.get("subscription_metadata")
                                    or {}
                                ).get("markets")
                                or {}
                            ).get(symbol),
                            Mapping,
                        )
                    },
                },
                "core_symbol_status": {
                    symbol: _selected(
                        (health_payload.get("core_symbol_status") or {}).get(
                            symbol
                        ),
                        ("requested", "contracts_subscribed"),
                    )
                    for symbol in _OPENING_SYMBOLS
                    if isinstance(
                        (health_payload.get("core_symbol_status") or {}).get(
                            symbol
                        ),
                        Mapping,
                    )
                },
                **(
                    {
                        "subscription_staging": _selected(
                            health_payload.get("subscription_staging"),
                            (
                                "mode",
                                "state",
                                "active_stage",
                                "deferred_stage",
                                "full_selected_contract_count",
                                "active_contract_count",
                                "deferred_contract_count",
                                "requested_orb_families",
                                "primary_contract_counts",
                                "subscription_epoch_id",
                                "subscription_generation",
                                "full_selected_universe_sha256",
                                "same_client_additive_subscription",
                                "intraday_replay_for_deferred_stage",
                                "promotion_eligible",
                                "promotion_reasons",
                                "additive_request_sent",
                            ),
                        )
                    }
                    if "subscription_staging" in health_payload
                    else {}
                ),
                **(
                    {
                        "market_subscription_status": {
                            symbol: _selected(
                                (
                                    health_payload.get(
                                        "market_subscription_status"
                                    )
                                    or {}
                                ).get(symbol),
                                (
                                    "requested",
                                    "selected_contract_count",
                                    "active_contract_count",
                                    "deferred_contract_count",
                                ),
                            )
                            for symbol in _OPENING_SYMBOLS
                            if isinstance(
                                (
                                    health_payload.get(
                                        "market_subscription_status"
                                    )
                                    or {}
                                ).get(symbol),
                                Mapping,
                            )
                        }
                    }
                    if "market_subscription_status" in health_payload
                    else {}
                ),
                "optional_family_canary": {
                    **_selected(
                        health_payload.get("optional_family_canary"),
                        ("state", "rollback_required"),
                    ),
                    "evaluation_evidence": _selected(
                        (
                            health_payload.get("optional_family_canary") or {}
                        ).get("evaluation_evidence"),
                        (
                            "decision_state",
                            "opening_orb_protection_active",
                            "cash_session_reconnect_protection_active",
                        ),
                    ),
                },
                "subscription_bounds": _selected(
                    health_payload.get("subscription_bounds"),
                    ("max_subscription_contracts",),
                ),
                "subscription_window": _selected(
                    health_payload.get("subscription_window"),
                    (
                        "state",
                        "subscription_allowed",
                        "observed_at_utc",
                        "trading_date",
                    ),
                ),
                "orb_reference_sampler": _selected(
                    health_payload.get("orb_reference_sampler"),
                    ("thread_alive", "interval_seconds", "last_bucket_utc"),
                ),
            },
        },
        "backend_live_health": {
            **_selected(live, ("ok", "status_code")),
            "payload": _selected(
                live_payload,
                (
                    "subscription_epoch_id",
                    "subscription_generation",
                    "active_generation",
                    "handoff_status",
                    "stream_connected",
                    "stream_progressing",
                    "collection_ready",
                    "calculation_ready",
                    "prediction_pipeline_ok",
                ),
            ),
        },
        "dashboard_health": _selected(
            report.get("dashboard_health"), ("ok", "status_code")
        ),
        "orb": {
            **_selected(orb, ("ok", "status_code")),
            "payload": {
                **_selected(
                    orb_payload,
                    (
                        "schema_version",
                        "configured_symbols",
                        "requested_symbols",
                        "runtime_binding_applied",
                        "runtime_context_stable",
                    ),
                ),
                "active_runtime_context": _selected(
                    orb_payload.get("active_runtime_context"),
                    (
                        "subscription_epoch_id",
                        "subscription_generation",
                        "handoff_status",
                    ),
                ),
                "symbols": symbols,
            },
        },
        "database": {
            **_selected(
                database,
                (
                    "present",
                    "path",
                    "journal_mode",
                    "quick_check",
                    "market_structure_table_present",
                    "missing_market_structure_columns",
                    "missing_market_structure_triggers",
                    "market_structure_column_count",
                    "expected_market_structure_column_count",
                    "orb_reference_table_present",
                    "missing_orb_reference_columns",
                    "missing_orb_reference_triggers",
                    "missing_orb_reference_indexes",
                    "orb_reference_column_count",
                    "expected_orb_reference_column_count",
                    "orb_reference_decision_table_present",
                    "missing_orb_reference_decision_columns",
                    "missing_orb_reference_decision_triggers",
                    "missing_orb_reference_decision_indexes",
                    "orb_reference_decision_column_count",
                    "expected_orb_reference_decision_column_count",
                    "orb_reference_progress_decisions",
                ),
            ),
            "market_structure_rows": {
                symbol: _selected(
                    structure_rows.get(symbol),
                    (
                        "row_count",
                        "latest_source_age_seconds",
                        "latest_capture_age_seconds",
                        "latest_provider",
                        "latest_subscription_epoch_id",
                        "latest_subscription_generation",
                        "latest_calculation_id",
                        "latest_reference_price",
                        "latest_gamma_pin",
                        "latest_max_pain",
                        "latest_primary_expiration",
                        "latest_same_day_profile_available",
                        "latest_universe_sha256",
                        "latest_validation_status",
                        "latest_calculation_bound",
                        "first_eligible_calculation_bound",
                    ),
                )
                for symbol in _CORE_SYMBOLS
            },
            "orb_reference_rows": {
                symbol: _selected(
                    reference_rows.get(symbol),
                    (
                        "row_count",
                        "opening_bucket_present",
                        "opening_capture_ratio",
                        "advancing_5s_evidence",
                        "provider_source_timestamps_advancing",
                        "latest_subscription_epoch_id",
                        "subscription_epoch_ids",
                        "invalid_subscription_epoch_row_count",
                        "mixed_subscription_epoch_rows",
                        "latest_subscription_generation",
                        "subscription_generations",
                        "invalid_subscription_generation_row_count",
                        "mixed_subscription_generation_rows",
                        "latest_provider",
                        "latest_validation_status",
                        "latest_primary_expiration",
                        "latest_same_day_profile_available",
                    ),
                )
                for symbol in _OPENING_SYMBOLS
            },
        },
        "scheduled_tasks": {
            "applicable": scheduled.get("applicable"),
            "tasks": {
                name: _selected(raw_tasks.get(name), task_fields)
                for name in (
                    "MarketPinPredictor_AutoStart",
                    "MarketPinPredictor_Watchdog",
                )
            },
        },
        "read_only": True,
        "authorities": _selected(
            report.get("authorities"),
            ("backend_health", "backend_live_health", "orb", "database"),
        ),
        "notes": list(report.get("notes", [])),
    }


def milestone_event_id(session_date: str, milestone: str) -> str:
    """Return the stable identity for one session/milestone pair."""

    normalized_session = _canonical_session_date(session_date)
    if milestone not in _MILESTONE_BY_NAME:
        raise MonitorOpeningAcceptanceError("milestone_invalid")
    return _canonical_hash(
        {
            "event_schema": EVENT_SCHEMA,
            "event_type": EVENT_TYPE,
            "session_date": normalized_session,
            "milestone": milestone,
        }
    )


def _owned_projection(opening: Mapping[str, Any], *, session_date: str) -> dict[str, Any]:
    projection: dict[str, Any] = {"session_date": session_date}
    for _name, flag, detail, _due in _MILESTONES:
        value = opening.get(flag)
        if type(value) is not bool:
            raise MonitorOpeningAcceptanceError(f"opening_flag_invalid:{flag}")
        projection[flag] = value
        if detail in opening:
            raw_detail = opening.get(detail)
            if not isinstance(raw_detail, Mapping):
                raise MonitorOpeningAcceptanceError(
                    f"opening_receipt_invalid:{detail}"
                )
            projection[detail] = copy.deepcopy(dict(raw_detail))
    temporary = opening.get("temporary_acceptance_checks_complete")
    if type(temporary) is not bool:
        raise MonitorOpeningAcceptanceError(
            "opening_flag_invalid:temporary_acceptance_checks_complete"
        )
    projection["temporary_acceptance_checks_complete"] = temporary
    if "temporary_acceptance_outcome" in opening:
        projection["temporary_acceptance_outcome"] = opening.get(
            "temporary_acceptance_outcome"
        )
    for field in (
        "pending_notification_event_ids",
        "committed_notification_ack_event_ids",
    ):
        value = opening.get(field, [])
        if (
            not isinstance(value, list)
            or any(not _is_sha256(item) for item in value)
            or len(set(value)) != len(value)
        ):
            raise MonitorOpeningAcceptanceError(f"opening_{field}_invalid")
        projection[field] = list(value)
    for field in (
        "last_notification_ack_event_id",
        "last_notification_ack_sha256",
    ):
        if field in opening:
            value = opening.get(field)
            if not _is_sha256(value):
                raise MonitorOpeningAcceptanceError(f"opening_{field}_invalid")
            projection[field] = value
    return projection


def _reset_projection(session_date: str) -> dict[str, Any]:
    return {"session_date": session_date, **copy.deepcopy(_OWNED_BASE_FLAGS)}


def _receipt_detail(event: Mapping[str, Any]) -> dict[str, Any]:
    milestone = str(event["milestone"])
    return {
        "schema_version": STATE_RECEIPT_SCHEMA,
        "event_id": event["event_id"],
        "event_sha256": _canonical_hash(event),
        "bounded_inspector_evidence_sha256": event["evidence"][
            "bounded_inspector_evidence_sha256"
        ],
        "source_compact_report_sha256": event["evidence"][
            "source_compact_report_sha256"
        ],
        "observed_at_ct": event["observed_at_ct"],
        "observed_at_utc": event["observed_at_utc"],
        "dedupe_key": f"{event['session_date']}:{milestone}",
        "committed": True,
        "notification_status": "pending",
        "notified": False,
    }


def _apply_event(
    opening: Mapping[str, Any], event: Mapping[str, Any]
) -> dict[str, Any]:
    result = copy.deepcopy(dict(opening))
    milestone = str(event["milestone"])
    _name, flag, detail, _due = _MILESTONE_BY_NAME[milestone]
    result[flag] = False
    result[detail] = _receipt_detail(event)
    pending = list(result.get("pending_notification_event_ids", []))
    event_id = str(event["event_id"])
    if event_id in pending:
        raise MonitorOpeningAcceptanceError(
            "opening_notification_already_pending:" + event_id
        )
    pending.append(event_id)
    result["pending_notification_event_ids"] = pending
    result.setdefault("committed_notification_ack_event_ids", [])
    result["updated_at_ct"] = event["observed_at_ct"]
    result["updated_at_utc"] = event["observed_at_utc"]
    acceptance_complete = all(
        detail in result
        for detail in (
            "first_eligible_gamma_capture",
            "final_complete_60m_orb",
        )
    )
    result["temporary_acceptance_checks_complete"] = acceptance_complete
    if acceptance_complete:
        result["temporary_acceptance_outcome"] = "complete_60m_orb"
    else:
        result.pop("temporary_acceptance_outcome", None)
    return result


def _event_completes_temporary_acceptance(
    pre_opening: Mapping[str, Any], milestone: str
) -> bool:
    details = {
        "first_eligible_gamma_capture": (
            "first_eligible_gamma_capture" in pre_opening
            or milestone == "first_eligible_gamma_capture"
        ),
        "complete_60m_orb": (
            "final_complete_60m_orb" in pre_opening
            or milestone == "complete_60m_orb"
        ),
    }
    return all(details.values())


def _ack_event_id(event: Mapping[str, Any]) -> str:
    material = copy.deepcopy(dict(event))
    material.pop("event_id", None)
    return _canonical_hash({"event_schema": ACK_EVENT_SCHEMA, "ack": material})


def _build_ack_event(
    *,
    session_date: str,
    observed_at_utc: datetime,
    mode: str,
    milestone_records: Sequence[Mapping[str, Any]],
    delivery_proof: Mapping[str, Any],
    pre_opening: Mapping[str, Any],
) -> dict[str, Any]:
    observed_utc = observed_at_utc.astimezone(_UTC)
    pending_before = list(pre_opening.get("pending_notification_event_ids", []))
    acked_ids = [str(record["event_id"]) for record in milestone_records]
    pending_after = [item for item in pending_before if item not in set(acked_ids)]
    proof = _validate_notification_delivery_proof(delivery_proof)
    event = {
        "schema_version": 2,
        "event_schema": ACK_EVENT_SCHEMA,
        "event_type": ACK_EVENT_TYPE,
        "observed_at_ct": observed_utc.astimezone(_CT).isoformat(),
        "observed_at_utc": observed_utc.isoformat().replace("+00:00", "Z"),
        "session_date": session_date,
        "phase": "notification_ack",
        "cadence": {
            "mode": mode,
            "substantive": False,
            "reason": "prior_heartbeat_opening_delivery_proven",
        },
        "evidence": {"delivery_proof_sha256": _canonical_hash(proof)},
        "symbols": {},
        "alerts": [],
        "directional_interpretation": "OPENING_NOTIFICATION_DELIVERY_ACK",
        "research_hypotheses": [],
        "acked_milestone_events": [
            {
                "event_id": record["event_id"],
                "record_sha256": _canonical_hash(record),
            }
            for record in milestone_records
        ],
        "delivery_proof": proof,
        "pending_notification_event_ids_before": pending_before,
        "pending_notification_event_ids_after": pending_after,
        "pre_opening_acceptance_sha256": _canonical_hash(pre_opening),
        "append_before_state_required": True,
    }
    event["event_id"] = _ack_event_id(event)
    return event


def _validate_ack_event(
    event: Mapping[str, Any],
    *,
    session_date: str,
    pre_opening: Mapping[str, Any],
    milestone_by_id: Mapping[str, Mapping[str, Any]],
) -> None:
    required = {
        "schema_version",
        "event_schema",
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
        "acked_milestone_events",
        "delivery_proof",
        "pending_notification_event_ids_before",
        "pending_notification_event_ids_after",
        "pre_opening_acceptance_sha256",
        "append_before_state_required",
    }
    if set(event) != required:
        raise MonitorOpeningAcceptanceError("opening_ack_fields_invalid")
    if (
        event.get("schema_version") != 2
        or event.get("event_schema") != ACK_EVENT_SCHEMA
        or event.get("event_type") != ACK_EVENT_TYPE
        or event.get("session_date") != session_date
        or event.get("phase") != "notification_ack"
        or event.get("event_id") != _ack_event_id(event)
        or event.get("pre_opening_acceptance_sha256")
        != _canonical_hash(pre_opening)
        or event.get("append_before_state_required") is not True
        or event.get("symbols") != {}
        or event.get("alerts") != []
        or event.get("research_hypotheses") != []
        or event.get("directional_interpretation")
        != "OPENING_NOTIFICATION_DELIVERY_ACK"
    ):
        raise MonitorOpeningAcceptanceError("opening_ack_identity_invalid")
    cadence = _mapping(event.get("cadence"), "opening_ack_cadence_invalid")
    if (
        cadence.get("mode") not in _VALID_MODES
        or cadence.get("substantive") is not False
        or cadence.get("reason")
        != "prior_heartbeat_opening_delivery_proven"
    ):
        raise MonitorOpeningAcceptanceError("opening_ack_cadence_invalid")
    try:
        proof = _validate_notification_delivery_proof(event.get("delivery_proof"))
    except MonitorScanLedgerError as exc:
        raise MonitorOpeningAcceptanceError(str(exc)) from exc
    if event.get("evidence") != {
        "delivery_proof_sha256": _canonical_hash(proof)
    }:
        raise MonitorOpeningAcceptanceError("opening_ack_delivery_proof_mismatch")
    acknowledged = _parse_aware(
        event.get("observed_at_utc"), "opening_ack.observed_at_utc"
    ).astimezone(_UTC)
    delivered = _parse_aware(
        proof.get("prior_final_delivered_at_utc"),
        "opening_ack.prior_final_delivered_at_utc",
    ).astimezone(_UTC)
    if delivered >= acknowledged:
        raise MonitorOpeningAcceptanceError("opening_ack_precedes_delivery_proof")
    if event.get("observed_at_ct") != acknowledged.astimezone(_CT).isoformat():
        raise MonitorOpeningAcceptanceError("opening_ack_timestamp_mismatch")

    pending_before = list(pre_opening.get("pending_notification_event_ids", []))
    commitments = event.get("acked_milestone_events")
    if not isinstance(commitments, list) or not commitments:
        raise MonitorOpeningAcceptanceError("opening_ack_commitments_invalid")
    acked_ids: list[str] = []
    for commitment in commitments:
        if not isinstance(commitment, Mapping) or set(commitment) != {
            "event_id",
            "record_sha256",
        }:
            raise MonitorOpeningAcceptanceError("opening_ack_commitment_invalid")
        event_id = str(commitment.get("event_id") or "")
        milestone = milestone_by_id.get(event_id)
        if (
            not isinstance(milestone, Mapping)
            or commitment.get("record_sha256") != _canonical_hash(milestone)
        ):
            raise MonitorOpeningAcceptanceError(
                "opening_ack_milestone_commitment_mismatch:" + event_id
            )
        milestone_time = _parse_aware(
            milestone.get("observed_at_utc"), "opening_ack.milestone_observed_at_utc"
        ).astimezone(_UTC)
        if delivered < milestone_time:
            raise MonitorOpeningAcceptanceError(
                "opening_ack_delivery_precedes_milestone:" + event_id
            )
        acked_ids.append(event_id)
    if acked_ids != pending_before[: len(acked_ids)]:
        raise MonitorOpeningAcceptanceError("opening_ack_pending_order_invalid")
    expected_after = pending_before[len(acked_ids) :]
    if (
        event.get("pending_notification_event_ids_before") != pending_before
        or event.get("pending_notification_event_ids_after") != expected_after
    ):
        raise MonitorOpeningAcceptanceError("opening_ack_pending_state_mismatch")


def _apply_ack(
    opening: Mapping[str, Any], event: Mapping[str, Any]
) -> dict[str, Any]:
    result = copy.deepcopy(dict(opening))
    acked_ids = [
        str(item["event_id"]) for item in event["acked_milestone_events"]
    ]
    for event_id in acked_ids:
        matched = False
        for _name, flag, detail, _due in _MILESTONES:
            receipt = result.get(detail)
            if not isinstance(receipt, Mapping) or receipt.get("event_id") != event_id:
                continue
            next_receipt = copy.deepcopy(dict(receipt))
            next_receipt["notification_status"] = "acknowledged"
            next_receipt["notified"] = True
            next_receipt["notification_ack_event_id"] = event["event_id"]
            result[detail] = next_receipt
            result[flag] = True
            matched = True
            break
        if not matched:
            raise MonitorOpeningAcceptanceError(
                "opening_ack_state_receipt_missing:" + event_id
            )
    result["pending_notification_event_ids"] = list(
        event["pending_notification_event_ids_after"]
    )
    ack_ids = list(result.get("committed_notification_ack_event_ids", []))
    ack_ids.append(str(event["event_id"]))
    result["committed_notification_ack_event_ids"] = ack_ids
    result["last_notification_ack_event_id"] = event["event_id"]
    result["last_notification_ack_sha256"] = _canonical_hash(event)
    result["updated_at_ct"] = event["observed_at_ct"]
    result["updated_at_utc"] = event["observed_at_utc"]
    return result


def _event_symbols(report: Mapping[str, Any], milestone: str) -> dict[str, Any]:
    if milestone == "startup":
        return {}
    symbols = (
        ((report.get("orb") or {}).get("payload") or {}).get("symbols") or {}
    )
    if milestone == "first_eligible_gamma_capture":
        return {
            symbol: {
                "pin_behavior": copy.deepcopy(
                    dict((symbols.get(symbol) or {}).get("pin_behavior") or {})
                ),
                "provenance": copy.deepcopy(
                    dict((symbols.get(symbol) or {}).get("provenance") or {})
                ),
            }
            for symbol in _CORE_SYMBOLS
        }
    window_name = "5m" if milestone == "complete_5m_orb" else "60m"
    return {
        symbol: copy.deepcopy(
            dict(
                (((symbols.get(symbol) or {}).get("opening_ranges") or {}).get(
                    window_name
                )
                or {})
            )
        )
        for symbol in _OPENING_SYMBOLS
    }


def _build_event(
    *,
    milestone: str,
    report: Mapping[str, Any],
    observed_utc: datetime,
    session_date: str,
    mode: str,
    pre_opening: Mapping[str, Any],
    prior_event_ids: Sequence[str],
) -> dict[str, Any]:
    observed_ct = observed_utc.astimezone(_CT)
    bounded_report = _bounded_report_projection(report)
    return {
        "schema_version": 2,
        "event_schema": EVENT_SCHEMA,
        "event_id": milestone_event_id(session_date, milestone),
        "event_type": EVENT_TYPE,
        "milestone": milestone,
        "observed_at_ct": observed_ct.isoformat(),
        "observed_at_utc": observed_utc.isoformat().replace("+00:00", "Z"),
        "session_date": session_date,
        "phase": (
            "preopen_acceptance" if milestone == "startup" else "regular-session"
        ),
        "cadence": {
            "mode": mode,
            "substantive": False,
            "reason": "opening_acceptance_milestone",
        },
        "evidence": {
            "source": "tools/inspect_opening_capture.py",
            "source_compact_report_sha256": _canonical_hash(report),
            "bounded_inspector_evidence_sha256": _canonical_hash(
                bounded_report
            ),
            "bounded_inspector_evidence": bounded_report,
        },
        "symbols": _event_symbols(bounded_report, milestone),
        "alerts": [],
        "directional_interpretation": "OPENING_ACCEPTANCE_ONLY_NO_MARKET_SIGNAL",
        "research_hypotheses": [],
        "prior_milestone_event_ids": list(prior_event_ids),
        "pre_opening_acceptance_sha256": _canonical_hash(pre_opening),
        "append_before_state_required": True,
        "temporary_acceptance_terminal": _event_completes_temporary_acceptance(
            pre_opening, milestone
        ),
    }


def _validate_event(
    event: Mapping[str, Any],
    *,
    expected_milestone: str,
    session_date: str,
    pre_opening: Mapping[str, Any],
    prior_event_ids: Sequence[str],
) -> None:
    required = {
        "schema_version",
        "event_schema",
        "event_id",
        "event_type",
        "milestone",
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
        "prior_milestone_event_ids",
        "pre_opening_acceptance_sha256",
        "append_before_state_required",
        "temporary_acceptance_terminal",
    }
    if set(event) != required:
        raise MonitorOpeningAcceptanceError("milestone_event_fields_invalid")
    if (
        event.get("schema_version") != 2
        or event.get("event_schema") != EVENT_SCHEMA
        or event.get("event_type") != EVENT_TYPE
        or event.get("milestone") != expected_milestone
        or event.get("session_date") != session_date
        or event.get("event_id")
        != milestone_event_id(session_date, expected_milestone)
        or event.get("prior_milestone_event_ids") != list(prior_event_ids)
        or event.get("pre_opening_acceptance_sha256")
        != _canonical_hash(pre_opening)
        or event.get("append_before_state_required") is not True
        or event.get("temporary_acceptance_terminal")
        is not _event_completes_temporary_acceptance(
            pre_opening, expected_milestone
        )
    ):
        raise MonitorOpeningAcceptanceError("milestone_event_identity_invalid")
    cadence = _mapping(event.get("cadence"), "milestone_event_cadence_invalid")
    if (
        cadence.get("mode") not in _VALID_MODES
        or cadence.get("substantive") is not False
        or cadence.get("reason") != "opening_acceptance_milestone"
    ):
        raise MonitorOpeningAcceptanceError("milestone_event_cadence_invalid")
    evidence = _mapping(event.get("evidence"), "milestone_event_evidence_invalid")
    if set(evidence) != {
        "source",
        "source_compact_report_sha256",
        "bounded_inspector_evidence_sha256",
        "bounded_inspector_evidence",
    }:
        raise MonitorOpeningAcceptanceError("milestone_event_evidence_invalid")
    report = _mapping(
        evidence.get("bounded_inspector_evidence"),
        "milestone_event_report_missing",
    )
    normalized, observed_utc, report_session = _normalize_report(
        report, milestone=expected_milestone
    )
    if (
        report_session != session_date
        or evidence.get("source") != "tools/inspect_opening_capture.py"
        or not _is_sha256(evidence.get("source_compact_report_sha256"))
        or evidence.get("bounded_inspector_evidence_sha256")
        != _canonical_hash(normalized)
        or event.get("observed_at_utc")
        != observed_utc.isoformat().replace("+00:00", "Z")
        or event.get("observed_at_ct") != observed_utc.astimezone(_CT).isoformat()
        or event.get("phase")
        != (
            "preopen_acceptance"
            if expected_milestone == "startup"
            else "regular-session"
        )
        or event.get("alerts") != []
        or event.get("research_hypotheses") != []
        or event.get("directional_interpretation")
        != "OPENING_ACCEPTANCE_ONLY_NO_MARKET_SIGNAL"
        or event.get("symbols") != _event_symbols(normalized, expected_milestone)
    ):
        raise MonitorOpeningAcceptanceError("milestone_event_evidence_mismatch")


def _validate_rollover_boundary(
    *,
    state: Mapping[str, Any],
    by_id: Mapping[str, Mapping[str, Any]],
    journal_dir: Path,
    session_date: str,
    records: Sequence[Mapping[str, Any]],
) -> None:
    event_id = state.get("session_rollover_event_id")
    event = by_id.get(str(event_id or ""))
    if not isinstance(event_id, str) or not isinstance(event, Mapping):
        raise MonitorOpeningAcceptanceError("current_session_rollover_receipt_missing")
    try:
        _validate_current_event_receipt(
            event,
            state=state,
            journal_dir=journal_dir,
            target=session_date,
            event_id=event_id,
        )
    except MonitorSessionRolloverError as exc:
        raise MonitorOpeningAcceptanceError(
            f"current_session_rollover_receipt_invalid:{exc}"
        ) from exc
    rollover_positions = [
        index
        for index, record in enumerate(records)
        if record.get("event_id") == event_id
    ]
    if len(rollover_positions) != 1:
        raise MonitorOpeningAcceptanceError(
            "current_session_rollover_receipt_missing_or_duplicate"
        )
    rollover_position = rollover_positions[0]
    if any(
        index <= rollover_position
        for index, record in enumerate(records)
        if record.get("event_type") in {EVENT_TYPE, ACK_EVENT_TYPE}
    ):
        raise MonitorOpeningAcceptanceError(
            "opening_receipt_must_follow_session_rollover"
        )


def _replay_opening_receipts(
    *,
    records: Sequence[Mapping[str, Any]],
    state_opening: Mapping[str, Any],
    session_date: str,
) -> tuple[
    dict[str, Any],
    list[Mapping[str, Any]],
    int,
    list[Mapping[str, Any]],
]:
    relevant_records: list[Mapping[str, Any]] = []
    milestone_records: list[Mapping[str, Any]] = []
    milestone_by_id: dict[str, Mapping[str, Any]] = {}
    replay_states = [_reset_projection(session_date)]
    latest_journal_observed: datetime | None = None
    for record in records:
        event_type = record.get("event_type")
        record_observed: datetime | None = None
        if record.get("session_date") == session_date and record.get(
            "observed_at_utc"
        ) is not None:
            record_observed = _parse_aware(
                record.get("observed_at_utc"),
                "opening_receipt.journal_observed_at_utc",
            ).astimezone(_UTC)
        if (
            event_type in {EVENT_TYPE, ACK_EVENT_TYPE}
            and latest_journal_observed is not None
            and record_observed is not None
            and record_observed <= latest_journal_observed
        ):
            raise MonitorOpeningAcceptanceError(
                (
                    "opening_milestone_not_after_latest_journal_record"
                    if event_type == EVENT_TYPE
                    else "opening_ack_not_after_latest_journal_record"
                )
            )
        if event_type == EVENT_TYPE:
            milestone_name = record.get("milestone")
            if milestone_name not in _MILESTONE_BY_NAME:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_name_invalid"
                )
            if any(
                prior.get("milestone") == milestone_name
                for prior in milestone_records
            ):
                raise MonitorOpeningAcceptanceError(
                    "duplicate_opening_milestone_receipt:" + str(milestone_name)
                )
            committed_names = {
                str(prior["milestone"]) for prior in milestone_records
            }
            missing_dependencies = [
                dependency
                for dependency in _MILESTONE_DEPENDENCIES[str(milestone_name)]
                if dependency not in committed_names
            ]
            if missing_dependencies:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_prerequisite_missing:"
                    + missing_dependencies[0]
                )
            if len(milestone_records) >= len(_MILESTONES):
                raise MonitorOpeningAcceptanceError(
                    "too_many_opening_milestone_receipts"
                )
            _validate_event(
                record,
                expected_milestone=str(milestone_name),
                session_date=session_date,
                pre_opening=replay_states[-1],
                prior_event_ids=[
                    str(prior["event_id"]) for prior in milestone_records
                ],
            )
            event_id = str(record["event_id"])
            milestone_records.append(record)
            milestone_by_id[event_id] = record
            next_projection = _owned_projection(
                _apply_event(replay_states[-1], record),
                session_date=session_date,
            )
        elif event_type == ACK_EVENT_TYPE:
            _validate_ack_event(
                record,
                session_date=session_date,
                pre_opening=replay_states[-1],
                milestone_by_id=milestone_by_id,
            )
            next_projection = _owned_projection(
                _apply_ack(replay_states[-1], record),
                session_date=session_date,
            )
        else:
            if record_observed is not None and (
                latest_journal_observed is None
                or record_observed > latest_journal_observed
            ):
                latest_journal_observed = record_observed
            continue
        relevant_records.append(record)
        replay_states.append(next_projection)
        if record_observed is not None and (
            latest_journal_observed is None
            or record_observed > latest_journal_observed
        ):
            latest_journal_observed = record_observed

    actual = _owned_projection(state_opening, session_date=session_date)
    matching_prefixes = [
        index for index, candidate in enumerate(replay_states) if candidate == actual
    ]
    if len(matching_prefixes) != 1:
        raise MonitorOpeningAcceptanceError(
            "opening_acceptance_state_receipt_mismatch"
        )
    reflected_count = matching_prefixes[0]
    replay_committed = replay_states[reflected_count]
    if len(relevant_records) - reflected_count > 1:
        raise MonitorOpeningAcceptanceError(
            "multiple_unreflected_opening_receipts"
        )
    return replay_committed, relevant_records, reflected_count, milestone_records


def validate_opening_acceptance_receipt_replay(
    *,
    records: Sequence[Mapping[str, Any]],
    state_opening: Mapping[str, Any],
    session_date: str,
    require_notifications_acknowledged: bool,
) -> None:
    """Apply the owner's full semantics before rollover discards the namespace."""

    replay, relevant_records, reflected_count, _milestones = (
        _replay_opening_receipts(
            records=records,
            state_opening=state_opening,
            session_date=session_date,
        )
    )
    if reflected_count != len(relevant_records):
        orphan = relevant_records[reflected_count]
        issue = (
            "prior_session_unreflected_opening_milestone:"
            if orphan.get("event_type") == EVENT_TYPE
            else "prior_session_unreflected_opening_notification_ack:"
        )
        raise MonitorOpeningAcceptanceError(
            issue + str(orphan.get("event_id") or "")
        )
    pending = list(replay.get("pending_notification_event_ids", []))
    if require_notifications_acknowledged and pending:
        raise MonitorOpeningAcceptanceError(
            "prior_session_opening_notification_pending:" + pending[0]
        )


def commit_opening_acceptance_milestone(
    *,
    milestone: str,
    inspector_report: Mapping[str, Any],
    state_path: Path,
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Commit one proven opening milestone without any direct heartbeat edit."""

    result = _base_result(milestone=milestone)
    state_replace_succeeded = False
    try:
        if milestone not in _MILESTONE_BY_NAME:
            raise MonitorOpeningAcceptanceError("milestone_invalid")
        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            session_date = _canonical_session_date(state.get("session_date"))
            result["session_date"] = session_date
            result["event_id"] = milestone_event_id(session_date, milestone)
            journal_path = journal_dir / f"{session_date}.jsonl"
            result["journal_path"] = str(journal_path)
            opening = _mapping(
                state.get("opening_acceptance"),
                "state_opening_acceptance_missing",
            )
            if opening.get("session_date") != session_date:
                raise MonitorOpeningAcceptanceError(
                    "opening_acceptance_session_mismatch"
                )
            if state.get("mode") not in _VALID_MODES:
                raise MonitorOpeningAcceptanceError(
                    "state_mode_must_be_NORMAL_or_ELEVATED"
                )
            calendar = market_calendar_status(date.fromisoformat(session_date))
            if calendar.get("supported") is not True:
                raise MonitorOpeningAcceptanceError("market_calendar_unverified")
            if calendar.get("market_open") is not True:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_requires_open_session"
                )

            (
                records,
                journal_raw,
                partial_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            by_id, _receipts = _journal_index(records)
            ledger = _validated_ledger_state(
                state.get("monitor_scan_ledger"), state, by_id, journal_raw
            )
            unreflected_scan_issue = _unreflected_scan_transaction_issue(
                by_id=by_id, ledger=ledger
            )
            if unreflected_scan_issue is not None:
                raise MonitorOpeningAcceptanceError(unreflected_scan_issue)
            _validate_rollover_boundary(
                state=state,
                by_id=by_id,
                journal_dir=journal_dir,
                session_date=session_date,
                records=records,
            )
            (
                replay,
                relevant_records,
                reflected_count,
                milestone_records,
            ) = _replay_opening_receipts(
                records=records,
                state_opening=opening,
                session_date=session_date,
            )
            reflected_milestones = [
                record
                for record in relevant_records[:reflected_count]
                if record.get("event_type") == EVENT_TYPE
            ]
            existing_orphan = (
                relevant_records[reflected_count]
                if len(relevant_records) > reflected_count
                else None
            )
            committed_by_name = {
                str(record["milestone"]): record
                for record in reflected_milestones
            }
            if milestone in committed_by_name:
                if existing_orphan is not None:
                    raise MonitorOpeningAcceptanceError(
                        "unreflected_opening_receipt_requires_exact_recovery:"
                        + str(existing_orphan.get("event_id") or "")
                    )
                if partial_tail is not None:
                    raise MonitorOpeningAcceptanceError(
                        "partial_journal_tail_after_committed_opening_milestone"
                    )
                committed_event = committed_by_name[milestone]
                pending = list(
                    replay.get("pending_notification_event_ids", [])
                )
                notification_pending = committed_event["event_id"] in pending
                result.update(
                    {
                        "accepted": True,
                        "action": (
                            "notification_pending"
                            if notification_pending
                            else "already_committed"
                        ),
                        "event_id": committed_event["event_id"],
                        "notification_required": notification_pending,
                        "notification_status": (
                            "pending" if notification_pending else "acknowledged"
                        ),
                        "pending_notification_event_ids": pending,
                        "temporary_acceptance_checks_complete": bool(
                            opening.get("temporary_acceptance_checks_complete")
                        ),
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result
            missing_dependencies = [
                dependency
                for dependency in _MILESTONE_DEPENDENCIES[milestone]
                if dependency not in committed_by_name
            ]
            if missing_dependencies:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_prerequisite_missing:"
                    + missing_dependencies[0]
                )

            if existing_orphan is not None and (
                existing_orphan.get("event_type") != EVENT_TYPE
                or existing_orphan.get("milestone") != milestone
            ):
                raise MonitorOpeningAcceptanceError(
                    "unreflected_opening_receipt_requires_exact_recovery:"
                    + str(existing_orphan.get("event_id") or "")
                )

            expected_event: Mapping[str, Any]
            if existing_orphan is not None:
                expected_event = existing_orphan
                result["event_id"] = expected_event["event_id"]
            else:
                report, observed_utc, report_session = _normalize_report(
                    inspector_report, milestone=milestone
                )
                if report_session != session_date:
                    raise MonitorOpeningAcceptanceError(
                        "inspector_report_session_mismatch"
                    )
                latest_record_time = _latest_session_journal_observed(
                    records,
                    session_date=session_date,
                    field="opening_milestone.latest_journal_observed_at_utc",
                )
                if (
                    latest_record_time is not None
                    and observed_utc <= latest_record_time
                ):
                    raise MonitorOpeningAcceptanceError(
                        "opening_milestone_not_after_latest_journal_record"
                    )
                expected_event = _build_event(
                    milestone=milestone,
                    report=report,
                    observed_utc=observed_utc,
                    session_date=session_date,
                    mode=str(state["mode"]),
                    pre_opening=replay,
                    prior_event_ids=[
                        str(record["event_id"])
                        for record in reflected_milestones
                    ],
                )
                _validate_event(
                    expected_event,
                    expected_milestone=milestone,
                    session_date=session_date,
                    pre_opening=replay,
                    prior_event_ids=[
                        str(record["event_id"])
                        for record in reflected_milestones
                    ],
                )

            collision = by_id.get(str(expected_event["event_id"]))
            if collision is not None and collision is not existing_orphan:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_event_id_collision"
                )

            if partial_tail is not None:
                if existing_orphan is not None or not _canonical_json_bytes(
                    expected_event
                ).startswith(partial_tail):
                    raise MonitorOpeningAcceptanceError(
                        "partial_journal_tail_not_exact_opening_milestone_prefix"
                    )
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorOpeningAcceptanceError(
                        "state_changed_during_opening_tail_recovery"
                    )
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True

            expected_records: list[Mapping[str, Any]] = list(records)
            if existing_orphan is None:
                _append_jsonl_durable(journal_path, expected_event)
                expected_records.append(expected_event)
                result["event_appended"] = True
                result["commit_phase"] = "journal_durable"
                if failpoint is not None:
                    failpoint("after_event_append")
            else:
                result["commit_phase"] = "journal_durable"

            if failpoint is not None:
                failpoint("before_state_replace")
            if state_path.read_bytes() != original_state_bytes:
                raise MonitorOpeningAcceptanceError(
                    "state_changed_during_opening_milestone_commit"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            next_state = copy.deepcopy(state)
            next_opening = _apply_event(opening, expected_event)
            next_state["opening_acceptance"] = next_opening
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted_after_error, _raw = _load_state(state_path)
                    state_replace_succeeded = persisted_after_error == next_state
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorOpeningAcceptanceError(
                    "opening_milestone_state_postcondition_failed"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_committed"
                        if existing_orphan is not None
                        or result["partial_journal_tail_recovered"]
                        else "committed"
                    ),
                    "state_updated": True,
                    "notification_required": True,
                    "notification_status": "pending",
                    "pending_notification_event_ids": list(
                        next_opening.get("pending_notification_event_ids", [])
                    ),
                    "temporary_acceptance_checks_complete": bool(
                        next_opening.get("temporary_acceptance_checks_complete")
                    ),
                    "commit_phase": "committed",
                    "issues": [],
                }
            )
            return result

        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (
        MonitorOpeningAcceptanceError,
        MonitorScanLedgerError,
        MonitorSessionRolloverError,
        OSError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        journal_durable = result.get("commit_phase") == "journal_durable"
        result.update(
            {
                "accepted": False,
                "action": (
                    "retry_required"
                    if state_replace_succeeded or journal_durable
                    else "abstain"
                ),
                "state_updated": state_replace_succeeded,
                "notification_required": False,
                "commit_phase": (
                    "post_state_replace_uncertain"
                    if state_replace_succeeded
                    else result.get("commit_phase", "not_started")
                ),
                "issues": [
                    str(exc)
                    if isinstance(
                        exc,
                        (
                            MonitorOpeningAcceptanceError,
                            MonitorScanLedgerError,
                            MonitorSessionRolloverError,
                        ),
                    )
                    else f"opening_milestone_commit_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


def ack_opening_acceptance_notifications(
    *,
    session_date: str,
    event_ids: Sequence[str],
    observed_at_utc: datetime,
    delivery_proof: Mapping[str, Any],
    state_path: Path,
    journal_dir: Path,
    failpoint: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Acknowledge milestone alerts only after prior-final delivery evidence."""

    result = _base_ack_result(session_date=session_date)
    state_replace_succeeded = False
    try:
        normalized_session = _canonical_session_date(session_date)
        if (
            not isinstance(event_ids, Sequence)
            or isinstance(event_ids, (str, bytes))
            or not event_ids
        ):
            raise MonitorOpeningAcceptanceError("opening_ack_event_ids_invalid")
        normalized_ids = [str(event_id) for event_id in event_ids]
        if (
            any(not _is_sha256(event_id) for event_id in normalized_ids)
            or len(set(normalized_ids)) != len(normalized_ids)
        ):
            raise MonitorOpeningAcceptanceError("opening_ack_event_ids_invalid")
        if (
            not isinstance(observed_at_utc, datetime)
            or observed_at_utc.tzinfo is None
            or observed_at_utc.utcoffset() is None
        ):
            raise MonitorOpeningAcceptanceError("opening_ack_observed_at_invalid")
        acknowledged_at = observed_at_utc.astimezone(_UTC)
        if acknowledged_at.utcoffset() != _UTC.utcoffset(acknowledged_at):
            raise MonitorOpeningAcceptanceError("opening_ack_observed_at_must_be_utc")
        try:
            normalized_proof = _validate_notification_delivery_proof(delivery_proof)
        except MonitorScanLedgerError as exc:
            raise MonitorOpeningAcceptanceError(str(exc)) from exc
        delivered_at = _parse_aware(
            normalized_proof["prior_final_delivered_at_utc"],
            "opening_ack.prior_final_delivered_at_utc",
        ).astimezone(_UTC)
        if delivered_at >= acknowledged_at:
            raise MonitorOpeningAcceptanceError(
                "acknowledgement_must_follow_prior_final_delivery"
            )
        if acknowledged_at.astimezone(_CT).date().isoformat() < normalized_session:
            raise MonitorOpeningAcceptanceError(
                "acknowledgement_time_before_market_session"
            )
        state_path = Path(state_path).resolve()
        journal_dir = Path(journal_dir).resolve()

        def execute_locked() -> dict[str, Any]:
            nonlocal state_replace_succeeded
            state, original_state_bytes = _load_state(state_path)
            state_session = _canonical_session_date(state.get("session_date"))
            if state_session != normalized_session:
                raise MonitorOpeningAcceptanceError(
                    "opening_ack_state_session_mismatch"
                )
            result["session_date"] = state_session
            journal_path = journal_dir / f"{state_session}.jsonl"
            result["journal_path"] = str(journal_path)
            opening = _mapping(
                state.get("opening_acceptance"),
                "state_opening_acceptance_missing",
            )
            if opening.get("session_date") != state_session:
                raise MonitorOpeningAcceptanceError(
                    "opening_acceptance_session_mismatch"
                )
            mode = state.get("mode")
            if mode not in _VALID_MODES:
                raise MonitorOpeningAcceptanceError(
                    "state_mode_must_be_NORMAL_or_ELEVATED"
                )
            (
                records,
                journal_raw,
                partial_tail,
                original_journal_raw,
            ) = _read_journal_exact_retry_snapshot(journal_path)
            by_id, _receipts = _journal_index(records)
            ledger = _validated_ledger_state(
                state.get("monitor_scan_ledger"), state, by_id, journal_raw
            )
            unreflected_scan_issue = _unreflected_scan_transaction_issue(
                by_id=by_id, ledger=ledger
            )
            if unreflected_scan_issue is not None:
                raise MonitorOpeningAcceptanceError(unreflected_scan_issue)
            _validate_rollover_boundary(
                state=state,
                by_id=by_id,
                journal_dir=journal_dir,
                session_date=state_session,
                records=records,
            )
            replay, relevant_records, reflected_count, milestone_records = (
                _replay_opening_receipts(
                    records=records,
                    state_opening=opening,
                    session_date=state_session,
                )
            )
            reflected_records = relevant_records[:reflected_count]
            reflected_ack_records = [
                record
                for record in reflected_records
                if record.get("event_type") == ACK_EVENT_TYPE
            ]
            existing_orphan = (
                relevant_records[reflected_count]
                if len(relevant_records) > reflected_count
                else None
            )
            existing_exact = next(
                (
                    record
                    for record in reflected_ack_records
                    if [
                        str(item["event_id"])
                        for item in record.get("acked_milestone_events", [])
                    ]
                    == normalized_ids
                    and record.get("delivery_proof") == normalized_proof
                    and record.get("observed_at_utc")
                    == acknowledged_at.isoformat().replace("+00:00", "Z")
                ),
                None,
            )
            if existing_exact is not None:
                if existing_orphan is not None:
                    raise MonitorOpeningAcceptanceError(
                        "unreflected_opening_receipt_requires_exact_recovery:"
                        + str(existing_orphan.get("event_id") or "")
                    )
                if partial_tail is not None:
                    raise MonitorOpeningAcceptanceError(
                        "partial_journal_tail_after_committed_opening_ack"
                    )
                result.update(
                    {
                        "accepted": True,
                        "action": "already_acknowledged",
                        "ack_event_id": existing_exact["event_id"],
                        "event_ids_acknowledged": [],
                        "pending_notification_event_ids": list(
                            replay.get("pending_notification_event_ids", [])
                        ),
                        "commit_phase": "committed",
                        "issues": [],
                    }
                )
                return result

            if existing_orphan is not None and existing_orphan.get(
                "event_type"
            ) != ACK_EVENT_TYPE:
                raise MonitorOpeningAcceptanceError(
                    "unreflected_opening_receipt_requires_exact_recovery:"
                    + str(existing_orphan.get("event_id") or "")
                )
            if existing_orphan is None:
                latest_record_time = _latest_session_journal_observed(
                    records,
                    session_date=state_session,
                    field="opening_ack.latest_journal_observed_at_utc",
                )
                if (
                    latest_record_time is not None
                    and acknowledged_at <= latest_record_time
                ):
                    raise MonitorOpeningAcceptanceError(
                        "opening_ack_not_after_latest_journal_record"
                    )

            pending_before = list(
                replay.get("pending_notification_event_ids", [])
            )
            if normalized_ids != pending_before[: len(normalized_ids)]:
                raise MonitorOpeningAcceptanceError(
                    "opening_ack_event_ids_not_pending_in_order"
                )
            milestone_by_id = {
                str(record["event_id"]): record for record in milestone_records
            }
            try:
                acked_milestones = [milestone_by_id[event_id] for event_id in normalized_ids]
            except KeyError as exc:
                raise MonitorOpeningAcceptanceError(
                    "opening_ack_milestone_receipt_missing:" + str(exc.args[0])
                ) from exc
            expected_ack = _build_ack_event(
                session_date=state_session,
                observed_at_utc=acknowledged_at,
                mode=str(mode),
                milestone_records=acked_milestones,
                delivery_proof=normalized_proof,
                pre_opening=replay,
            )
            _validate_ack_event(
                expected_ack,
                session_date=state_session,
                pre_opening=replay,
                milestone_by_id=milestone_by_id,
            )
            result["ack_event_id"] = expected_ack["event_id"]

            if existing_orphan is not None and _canonical_json_bytes(
                existing_orphan
            ) != _canonical_json_bytes(expected_ack):
                raise MonitorOpeningAcceptanceError(
                    "unreflected_opening_ack_requires_exact_recovery:"
                    + str(existing_orphan.get("event_id") or "")
                )
            collision = by_id.get(str(expected_ack["event_id"]))
            if collision is not None and collision is not existing_orphan:
                raise MonitorOpeningAcceptanceError(
                    "opening_ack_event_id_collision"
                )
            if partial_tail is not None:
                if existing_orphan is not None or not _canonical_json_bytes(
                    expected_ack
                ).startswith(partial_tail):
                    raise MonitorOpeningAcceptanceError(
                        "partial_journal_tail_not_exact_opening_ack_prefix"
                    )
                if state_path.read_bytes() != original_state_bytes:
                    raise MonitorOpeningAcceptanceError(
                        "state_changed_during_opening_ack_tail_recovery"
                    )
                _truncate_exact_partial_tail(
                    journal_path,
                    expected_full_raw=original_journal_raw,
                    complete_size=len(journal_raw),
                )
                result["partial_journal_tail_recovered"] = True

            expected_records: list[Mapping[str, Any]] = list(records)
            if existing_orphan is None:
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
                raise MonitorOpeningAcceptanceError(
                    "state_changed_during_opening_ack"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            next_state = copy.deepcopy(state)
            next_opening = _apply_ack(opening, expected_ack)
            next_state["opening_acceptance"] = next_opening
            try:
                _atomic_write_json(state_path, next_state)
            except (OSError, TypeError, ValueError, OverflowError):
                try:
                    persisted_after_error, _raw = _load_state(state_path)
                    state_replace_succeeded = persisted_after_error == next_state
                except (MonitorScanLedgerError, OSError):
                    pass
                raise
            state_replace_succeeded = True
            if failpoint is not None:
                failpoint("after_state_replace")
            persisted, _raw = _load_state(state_path)
            if persisted != next_state:
                raise MonitorOpeningAcceptanceError(
                    "opening_ack_state_postcondition_failed"
                )
            _revalidate_expected_journal(journal_path, expected_records)
            result.update(
                {
                    "accepted": True,
                    "action": (
                        "recovered_and_acknowledged"
                        if existing_orphan is not None
                        or result["partial_journal_tail_recovered"]
                        else "acknowledged"
                    ),
                    "state_updated": True,
                    "event_ids_acknowledged": normalized_ids,
                    "pending_notification_event_ids": list(
                        next_opening.get("pending_notification_event_ids", [])
                    ),
                    "commit_phase": "committed",
                    "issues": [],
                }
            )
            return result

        lock_path = state_path.with_name(f"{state_path.name}.rollover.lock")
        with _exclusive_lock(lock_path):
            return execute_locked()
    except (
        MonitorOpeningAcceptanceError,
        MonitorScanLedgerError,
        MonitorSessionRolloverError,
        OSError,
        TypeError,
        ValueError,
        OverflowError,
    ) as exc:
        journal_durable = result.get("commit_phase") == "journal_durable"
        result.update(
            {
                "accepted": False,
                "action": (
                    "retry_required"
                    if state_replace_succeeded or journal_durable
                    else "abstain"
                ),
                "state_updated": state_replace_succeeded,
                "event_ids_acknowledged": [],
                "commit_phase": (
                    "post_state_replace_uncertain"
                    if state_replace_succeeded
                    else result.get("commit_phase", "not_started")
                ),
                "issues": [
                    str(exc)
                    if isinstance(
                        exc,
                        (
                            MonitorOpeningAcceptanceError,
                            MonitorScanLedgerError,
                            MonitorSessionRolloverError,
                        ),
                    )
                    else f"opening_ack_failed:{type(exc).__name__}:{exc}"
                ],
            }
        )
        return result


__all__ = [
    "ACK_EVENT_SCHEMA",
    "ACK_EVENT_TYPE",
    "ACK_RESULT_SCHEMA",
    "EVENT_SCHEMA",
    "EVENT_TYPE",
    "MonitorOpeningAcceptanceError",
    "RESULT_SCHEMA",
    "ack_opening_acceptance_notifications",
    "commit_opening_acceptance_milestone",
    "milestone_event_id",
]
