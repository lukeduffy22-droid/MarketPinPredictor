"""Append-only live structure journal and sampled opening-range projection.

The active Databento backend derives its reference price from OPRA put/call
parity.  This module records each valid calculation revision and builds an ORB
from those retained samples.  It never labels the result as official exchange
OHLC and never fills a missing opening range from a later price.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from backend.database import (
    load_orb_reference_snapshot_samples,
    load_market_structure_observations,
    save_orb_reference_sample,
    save_orb_reference_sample_decision,
    save_market_structure_observation,
)
from backend.workstation import (
    payload_has_fallback_provenance,
    payload_revision_key,
    source_timestamp,
)

logger = logging.getLogger(__name__)

EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
ORB_START_ET = time(9, 30)
ORB_END_ET = time(10, 30)
REGULAR_CLOSE_ET = time(16, 0)
ORB_SCHEMA_VERSION = "marketpin-reference-orb.v2"
ORB_WINDOWS_MINUTES = (5, 15, 30, 60)
ORB_REFERENCE_CADENCE_SECONDS = max(
    1,
    int(os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")),
)
ORB_REFERENCE_QUOTE_FRESHNESS_SECONDS = max(
    0.1,
    float(os.getenv("DATABENTO_QUOTE_FRESHNESS_SECONDS", "10")),
)
ORB_MIN_CAPTURE_RATIO = min(
    1.0,
    max(0.90, float(os.getenv("DATABENTO_ORB_MIN_CAPTURE_RATIO", "0.95"))),
)
ORB_CURRENT_REFERENCE_MAX_AGE_SECONDS = max(
    float(ORB_REFERENCE_CADENCE_SECONDS),
    float(os.getenv("DATABENTO_ORB_CURRENT_MAX_AGE_SECONDS", "15")),
)
MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS = max(
    1.0,
    float(os.getenv("DATABENTO_MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS", "90")),
)
OPTION_SYMBOL_RE = re.compile(
    r"^(?P<root>[A-Z]+)\s+(?P<yymmdd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$"
)


def _reference_semantics(
    symbol: str,
    latest: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Classify option-parity price authority without promoting it to spot OHLC."""

    normalized = str(symbol or "").upper().strip()
    same_day = latest.get("same_day_profile_available") if latest else None
    primary_expiration = latest.get("primary_expiration") if latest else None
    if normalized == "VIX":
        kind = "vix_option_forward_context"
        directional_base_eligible = False
        limitation = (
            "VIX option parity is forward-like expiry context, not the official VIX spot index."
        )
    elif same_day is True:
        kind = "same_day_index_option_parity"
        directional_base_eligible = True
        limitation = (
            "Same-day index-option parity is a sampled reference, not official exchange OHLC."
        )
    elif same_day is False:
        kind = "non_same_day_index_option_forward_context"
        directional_base_eligible = False
        limitation = (
            "The primary option expiry is not same-day, so its parity level is context only."
        )
    else:
        kind = "pending_live_expiration_classification"
        directional_base_eligible = False
        limitation = "Live primary-expiration authority has not yet been established."
    return {
        "kind": kind,
        "source": "databento_opra_put_call_parity",
        "authority": "research_reference_only",
        "primary_expiration": primary_expiration,
        "same_day_profile_available": same_day,
        "directional_base_eligible": directional_base_eligible,
        "limitation": limitation,
    }


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _et_boundary(trading_date: date, boundary: time) -> datetime:
    return datetime.combine(trading_date, boundary, tzinfo=EASTERN).astimezone(UTC)


def _change(current: float | None, reference: float | None) -> float | None:
    if current is None or reference is None:
        return None
    return current - reference


def _integer(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _change_count(values: Sequence[float | None]) -> int:
    cleaned = [value for value in values if value is not None]
    return sum(1 for previous, current in zip(cleaned, cleaned[1:]) if current != previous)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _provenance_identity(
    row: Mapping[str, Any],
) -> tuple[str | None, str | None, int | None, str | None, str | None]:
    provider = str(row.get("provider") or "").lower().strip() or None
    subscription_epoch_id = (
        str(row.get("subscription_epoch_id") or "").lower().strip() or None
    )
    generation = _integer(row.get("subscription_generation"))
    universe_sha256 = str(row.get("universe_sha256") or "").lower().strip() or None
    primary_expiration = str(row.get("primary_expiration") or "").strip() or None
    return (
        provider,
        subscription_epoch_id,
        generation,
        universe_sha256,
        primary_expiration,
    )


def _complete_provenance_identity(row: Mapping[str, Any] | None) -> bool:
    return bool(
        row is not None
        and all(value is not None for value in _provenance_identity(row))
    )


def _runtime_identity_aligned(
    row: Mapping[str, Any] | None,
    *,
    active_subscription_epoch_id: str | None,
    active_subscription_generation: int | None,
    active_handoff_status: str | None,
) -> bool:
    """Bind a live view to its process while retaining offline replay support."""

    binding_requested = any(
        value is not None
        for value in (
            active_subscription_epoch_id,
            active_subscription_generation,
            active_handoff_status,
        )
    )
    if not binding_requested:
        return True
    epoch_id = str(active_subscription_epoch_id or "").strip()
    generation = _integer(active_subscription_generation)
    return bool(
        re.fullmatch(r"[0-9a-f]{64}", epoch_id) is not None
        and generation is not None
        and generation > 0
        and active_handoff_status == "active"
        and _complete_provenance_identity(row)
        and str(row.get("subscription_epoch_id")) == epoch_id
        and _integer(row.get("subscription_generation")) == generation
    )


def _orb_reference_provenance_identity(
    row: Mapping[str, Any],
) -> tuple[
    str | None,
    str | None,
    int | None,
    str | None,
    str | None,
    str | None,
    float | None,
    str | None,
]:
    (
        provider,
        subscription_epoch_id,
        generation,
        universe_sha256,
        primary_expiration,
    ) = _provenance_identity(row)
    formula_version = str(row.get("spot_formula_version") or "").strip() or None
    risk_free_rate = _finite(row.get("risk_free_rate"))
    mapping_version = str(row.get("symbol_mapping_version") or "").strip() or None
    return (
        provider,
        subscription_epoch_id,
        generation,
        universe_sha256,
        primary_expiration,
        formula_version,
        risk_free_rate,
        mapping_version,
    )


def _opening_window_projection(
    rows: Sequence[Mapping[str, Any]],
    *,
    latest: Mapping[str, Any] | None,
    start_utc: datetime,
    end_utc: datetime,
    as_of_utc: datetime,
    expected_cadence_seconds: float,
    configured: bool,
    active_subscription_epoch_id: str | None,
    active_subscription_generation: int | None,
    active_handoff_status: str | None,
) -> dict[str, Any]:
    """Project one sampled ORB window without inventing boundary evidence."""

    runtime_binding_applied = any(
        value is not None
        for value in (
            active_subscription_epoch_id,
            active_subscription_generation,
            active_handoff_status,
        )
    )
    raw_rows = [
        row for row in rows if start_utc <= row["_source_time"] < end_utc
    ]
    rows_by_source_time: dict[datetime, Mapping[str, Any]] = {}
    for row in raw_rows:
        rows_by_source_time[row["_source_time"]] = row
    range_rows = [rows_by_source_time[key] for key in sorted(rows_by_source_time)]

    if as_of_utc < start_utc:
        clock_status = "not_started"
    elif as_of_utc < end_utc:
        clock_status = "forming"
    else:
        clock_status = "closed"

    sample_count = len(range_rows)
    opening_price = range_rows[0]["_price"] if range_rows else None
    orb_high = max((row["_price"] for row in range_rows), default=None)
    orb_low = min((row["_price"] for row in range_rows), default=None)
    midpoint = (
        (orb_high + orb_low) / 2.0
        if orb_high is not None and orb_low is not None
        else None
    )
    range_width = (
        orb_high - orb_low
        if orb_high is not None and orb_low is not None
        else None
    )
    range_width_pct = (
        (range_width / opening_price) * 100.0
        if range_width is not None and opening_price is not None and opening_price > 0.0
        else None
    )

    elapsed_end = min(
        max(as_of_utc, start_utc),
        end_utc - timedelta(microseconds=1),
    )
    elapsed_seconds = max(0.0, (elapsed_end - start_utc).total_seconds())
    expected_samples = (
        int(elapsed_seconds // expected_cadence_seconds) + 1
        if as_of_utc >= start_utc
        else 0
    )
    capture_ratio = (
        min(1.0, sample_count / expected_samples) if expected_samples > 0 else None
    )
    first_sample_utc = range_rows[0]["_source_time"] if range_rows else None
    last_sample_utc = range_rows[-1]["_source_time"] if range_rows else None
    first_sample_lag_seconds = (
        (first_sample_utc - start_utc).total_seconds()
        if first_sample_utc is not None
        else None
    )
    opening_bucket_present = first_sample_utc == start_utc
    end_gap_seconds = (
        (end_utc - last_sample_utc).total_seconds()
        if last_sample_utc is not None
        else None
    )
    gaps = [
        (current["_source_time"] - previous["_source_time"]).total_seconds()
        for previous, current in zip(range_rows, range_rows[1:])
    ]
    max_gap_seconds = max(gaps) if gaps else None
    tolerance = max(30.0, expected_cadence_seconds * 3.0)
    boundary_coverage = bool(
        range_rows
        and opening_bucket_present
        and first_sample_lag_seconds is not None
        and first_sample_lag_seconds <= tolerance
        and end_gap_seconds is not None
        and end_gap_seconds <= tolerance
    )
    gap_coverage = max_gap_seconds is None or max_gap_seconds <= tolerance
    # A reconnect/restart can put two provenance identities in one cadence
    # bucket. Deduplication must not hide that mixed-state evidence.
    range_identities = {_orb_reference_provenance_identity(row) for row in raw_rows}
    range_identity = next(iter(range_identities)) if len(range_identities) == 1 else None
    range_provenance_aligned = bool(
        range_identity is not None and all(value is not None for value in range_identity)
    )
    latest_identity = _orb_reference_provenance_identity(latest) if latest else None
    active_runtime_epoch_aligned = bool(
        runtime_binding_applied
        and _runtime_identity_aligned(
            latest,
            active_subscription_epoch_id=active_subscription_epoch_id,
            active_subscription_generation=active_subscription_generation,
            active_handoff_status=active_handoff_status,
        )
    )
    current_vs_range_aligned = bool(
        range_provenance_aligned
        and latest_identity is not None
        and latest_identity == range_identity
        and _runtime_identity_aligned(
            latest,
            active_subscription_epoch_id=active_subscription_epoch_id,
            active_subscription_generation=active_subscription_generation,
            active_handoff_status=active_handoff_status,
        )
    )

    if not configured:
        capture_status = "not_configured"
    elif clock_status == "not_started":
        capture_status = "not_started"
    elif not range_rows:
        capture_status = (
            "awaiting_first_sample" if clock_status == "forming" else "unavailable"
        )
    elif not range_provenance_aligned:
        capture_status = "partial"
    elif clock_status == "forming":
        capture_status = "forming"
    elif (
        boundary_coverage
        and gap_coverage
        and capture_ratio is not None
        and capture_ratio >= ORB_MIN_CAPTURE_RATIO
    ):
        capture_status = "complete"
    else:
        capture_status = "partial"

    last_known_reference_price = latest["_price"] if latest else None
    current_price = (
        last_known_reference_price
        if not runtime_binding_applied or active_runtime_epoch_aligned
        else None
    )
    latest_knowledge_time = (
        _parse_utc(latest.get("captured_at_utc")) if latest else None
    )
    if latest_knowledge_time is None and latest:
        latest_knowledge_time = latest["_source_time"]
    current_reference_age_seconds = (
        max(0.0, (as_of_utc - latest_knowledge_time).total_seconds())
        if latest_knowledge_time is not None
        else None
    )
    current_reference_fresh = bool(
        current_reference_age_seconds is not None
        and current_reference_age_seconds <= ORB_CURRENT_REFERENCE_MAX_AGE_SECONDS
        and (not runtime_binding_applied or active_runtime_epoch_aligned)
    )
    if capture_status == "forming":
        breakout_direction = "forming"
    elif (
        capture_status != "complete"
        or current_price is None
        or not current_vs_range_aligned
        or not current_reference_fresh
    ):
        breakout_direction = "unavailable"
    elif current_price > float(orb_high):
        breakout_direction = "bullish"
    elif current_price < float(orb_low):
        breakout_direction = "bearish"
    else:
        breakout_direction = "inside"

    warnings: list[str] = []
    if capture_status == "partial":
        warnings.append("OPENING_RANGE_CAPTURE_PARTIAL")
    elif capture_status == "unavailable":
        warnings.append("OPENING_RANGE_NOT_CAPTURED")
    if not range_provenance_aligned and range_rows:
        warnings.append("OPENING_RANGE_PROVENANCE_MIXED")
    if capture_status == "complete" and not current_vs_range_aligned:
        warnings.append("CURRENT_PROVENANCE_DIFFERS_FROM_ORB")
    if latest and runtime_binding_applied and not active_runtime_epoch_aligned:
        warnings.append("CURRENT_RUNTIME_PROVENANCE_MISMATCH")
    if latest and not current_reference_fresh:
        warnings.append("CURRENT_REFERENCE_STALE")

    duration_minutes = int((end_utc - start_utc).total_seconds() // 60)
    return {
        "duration_minutes": duration_minutes,
        "range_start_utc": _utc_iso(start_utc),
        "range_end_utc": _utc_iso(end_utc),
        "clock_status": clock_status,
        "capture_status": capture_status,
        "orb_complete": capture_status == "complete",
        "opening_price": opening_price,
        "orb_high": orb_high,
        "orb_low": orb_low,
        "midpoint": midpoint,
        "range_width": range_width,
        "range_width_pct": range_width_pct,
        "current_price": current_price,
        "current_reference_fresh": current_reference_fresh,
        "last_known_reference": {
            "reference_price": last_known_reference_price,
            "runtime_aligned": (
                active_runtime_epoch_aligned if runtime_binding_applied else None
            ),
        },
        "breakout_direction": breakout_direction,
        "position_in_range": (
            (current_price - orb_low) / range_width
            if current_price is not None
            and orb_low is not None
            and range_width is not None
            and range_width > 0.0
            else None
        ),
        "capture_evidence": {
            "sample_count": sample_count,
            "raw_observation_count": len(raw_rows),
            "duplicate_source_timestamp_count": len(raw_rows) - sample_count,
            "expected_sample_count": expected_samples,
            "capture_ratio": capture_ratio,
            "minimum_complete_capture_ratio": ORB_MIN_CAPTURE_RATIO,
            "expected_cadence_seconds": expected_cadence_seconds,
            "first_sample_utc": _utc_iso(first_sample_utc),
            "opening_bucket_present": opening_bucket_present,
            "last_range_sample_utc": _utc_iso(last_sample_utc),
            "first_sample_lag_seconds": first_sample_lag_seconds,
            "end_gap_seconds": end_gap_seconds,
            "max_gap_seconds": max_gap_seconds,
            "current_reference_age_seconds": current_reference_age_seconds,
            "current_reference_max_age_seconds": ORB_CURRENT_REFERENCE_MAX_AGE_SECONDS,
        },
        "provenance": {
            "range_provenance_aligned": range_provenance_aligned,
            "current_vs_range_aligned": current_vs_range_aligned,
            "runtime_binding_applied": runtime_binding_applied,
            "active_runtime_epoch_aligned": active_runtime_epoch_aligned,
        },
        "warnings": warnings,
    }


class MarketStructureJournal:
    """Record validated revisions and project current ORB/pin-drift state."""

    def __init__(
        self,
        *,
        saver: Callable[[Mapping[str, Any]], Any] = save_market_structure_observation,
        loader: Callable[..., list[dict[str, Any]]] = load_market_structure_observations,
        reference_saver: Callable[[Mapping[str, Any]], Any] = save_orb_reference_sample,
        reference_loader: Callable[..., list[dict[str, Any]]] = (
            load_orb_reference_snapshot_samples
        ),
        reference_decision_saver: Callable[[Mapping[str, Any]], Any] | None = None,
        now_utc: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        expected_cadence_seconds: float | None = None,
    ) -> None:
        self.saver = saver
        self.loader = loader
        self.reference_saver = reference_saver
        self.reference_loader = reference_loader
        if reference_decision_saver is not None:
            self.reference_decision_saver = reference_decision_saver
        elif reference_saver is save_orb_reference_sample:
            self.reference_decision_saver = save_orb_reference_sample_decision
        else:
            # In-memory/unit-test journals retain the eligibility marker on the
            # saved mapping itself.  Never let an injected saver write a
            # decision into the runtime database by accident.
            self.reference_decision_saver = lambda decision: dict(decision)
        self.now_utc = now_utc
        configured_cadence = expected_cadence_seconds
        if configured_cadence is None:
            configured_cadence = _finite(
                os.getenv("DATABENTO_ORB_REFERENCE_INTERVAL_SECONDS", "5")
            )
        self.expected_cadence_seconds = max(1.0, float(configured_cadence or 5.0))

    def record(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Persist one valid regular-session revision; reject unsafe inputs."""
        symbol = str(payload.get("symbol") or "").upper().strip()
        if not symbol:
            return {"recorded": False, "reason": "SYMBOL_MISSING"}

        provider = str(payload.get("provider") or "unknown").lower().strip()
        if provider != "databento":
            return {"recorded": False, "reason": "PROVIDER_NOT_DATABENTO"}
        subscription_epoch_id = str(
            payload.get("subscription_epoch_id") or ""
        ).strip()
        if re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None:
            return {"recorded": False, "reason": "SUBSCRIPTION_EPOCH_INVALID"}
        universe_provenance = _mapping(payload.get("universe_provenance"))
        if payload_has_fallback_provenance(payload):
            return {"recorded": False, "reason": "FALLBACK_PROVENANCE"}
        if payload.get("validation_is_valid") is not True:
            return {"recorded": False, "reason": "PAYLOAD_INVALID"}
        if "gamma_excluded_from_model" not in payload:
            return {
                "recorded": False,
                "reason": "GAMMA_MODEL_ELIGIBILITY_UNPROVEN",
            }
        if payload.get("gamma_excluded_from_model") is not False:
            return {"recorded": False, "reason": "GAMMA_EXCLUDED_FROM_MODEL"}
        generation = _integer(payload.get("subscription_generation"))
        if generation is None or generation <= 0:
            return {"recorded": False, "reason": "SUBSCRIPTION_GENERATION_INVALID"}
        universe_sha256 = str(
            payload.get("selected_universe_sha256")
            or payload.get("universe_sha256")
            or universe_provenance.get("source_sha256")
            or ""
        ).lower().strip()
        if re.fullmatch(r"[0-9a-f]{64}", universe_sha256) is None:
            return {"recorded": False, "reason": "UNIVERSE_PROVENANCE_INVALID"}

        reference_price = _finite(
            payload.get("price")
            if payload.get("price") is not None
            else payload.get("spot_price")
        )
        if reference_price is None or reference_price <= 0.0:
            return {"recorded": False, "reason": "REFERENCE_PRICE_INVALID"}

        source_timestamp_utc = _parse_utc(source_timestamp(payload))
        if source_timestamp_utc is None:
            return {"recorded": False, "reason": "SOURCE_TIMESTAMP_MISSING"}
        source_et = source_timestamp_utc.astimezone(EASTERN)
        if not (ORB_START_ET <= source_et.time() <= REGULAR_CLOSE_ET):
            return {"recorded": False, "reason": "OUTSIDE_REGULAR_SESSION"}

        revision_key = payload_revision_key(payload)
        identity = json.dumps(
            revision_key,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        observation_id = hashlib.sha256(identity).hexdigest()
        observation = {
            "observation_id": observation_id,
            "symbol": symbol,
            "trading_date": source_et.date(),
            "source_timestamp_utc": source_timestamp_utc,
            "provider": provider,
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": generation,
            "calculation_id": payload.get("calculation_id"),
            "reference_price": reference_price,
            "spot_source": payload.get("spot_source")
            or (
                "databento_opra_put_call_parity"
                if provider == "databento"
                else "unreported"
            ),
            "gamma_pin": _finite(payload.get("gamma_pin")),
            "max_pain": _finite(payload.get("max_pain")),
            "zero_gamma": _finite(payload.get("zero_gamma")),
            "pin_lead_ratio": _finite(payload.get("pin_lead_ratio")),
            "pin_is_contested": payload.get("pin_is_contested"),
            "gross_gex": _finite(payload.get("gross_gex")),
            "net_gex": _finite(payload.get("net_gex")),
            "primary_expiration": payload.get("primary_expiration"),
            "same_day_profile_available": payload.get("same_day_profile_available"),
            "universe_sha256": universe_sha256,
            "validation_status": "valid",
        }
        saved = self.saver(observation)
        return {
            "recorded": saved is not None,
            "reason": None if saved is not None else "PERSISTENCE_FAILED",
            "observation_id": observation_id,
            "symbol": symbol,
            "source_timestamp_utc": _utc_iso(source_timestamp_utc),
        }

    def record_reference(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        """Persist one independently sampled, provenance-complete ORB reference."""
        defer_progress_decision = payload.get("_defer_progress_decision") is True
        symbol = str(payload.get("symbol") or "").upper().strip()
        if not symbol:
            return {"recorded": False, "reason": "SYMBOL_MISSING"}
        if str(payload.get("provider") or "").lower().strip() != "databento":
            return {"recorded": False, "reason": "PROVIDER_NOT_DATABENTO"}
        subscription_epoch_id = str(
            payload.get("subscription_epoch_id") or ""
        ).strip()
        if re.fullmatch(r"[0-9a-f]{64}", subscription_epoch_id) is None:
            return {"recorded": False, "reason": "SUBSCRIPTION_EPOCH_INVALID"}
        if payload_has_fallback_provenance(payload) or payload.get("universe_is_fallback") is not False:
            return {"recorded": False, "reason": "FALLBACK_PROVENANCE"}
        if str(payload.get("processing_clock_status") or "") != "synchronized":
            return {"recorded": False, "reason": "PROCESSING_CLOCK_NOT_SYNCHRONIZED"}
        if payload.get("timestamp_order_valid") is not True:
            return {"recorded": False, "reason": "PROVIDER_TIMESTAMP_ORDER_INVALID"}
        if str(payload.get("handoff_status") or "") != "active":
            return {"recorded": False, "reason": "HANDOFF_NOT_ACTIVE"}
        if payload.get("generation_state_unchanged") is not True:
            return {"recorded": False, "reason": "GENERATION_CHANGED_DURING_SAMPLE"}
        if payload.get("universe_state_unchanged") is not True:
            return {"recorded": False, "reason": "UNIVERSE_CHANGED_DURING_SAMPLE"}

        generation = _integer(payload.get("subscription_generation"))
        active_generation = _integer(payload.get("active_generation"))
        if generation is None or generation <= 0 or active_generation != generation:
            return {"recorded": False, "reason": "SUBSCRIPTION_GENERATION_INVALID"}
        universe_sha256 = str(payload.get("universe_sha256") or "").lower().strip()
        if re.fullmatch(r"[0-9a-f]{64}", universe_sha256) is None:
            return {"recorded": False, "reason": "UNIVERSE_PROVENANCE_INVALID"}

        primary_expiration = str(payload.get("primary_expiration") or "").strip()
        planned_primary = str(payload.get("planned_primary_expiration") or "").strip()
        try:
            primary_date = date.fromisoformat(primary_expiration)
        except ValueError:
            primary_date = None
        if primary_date is None or planned_primary != primary_expiration:
            return {"recorded": False, "reason": "PRIMARY_EXPIRATION_NOT_PLANNED"}

        paired_quote_count = _integer(payload.get("paired_quote_count"))
        minimum_pair_count = _integer(payload.get("minimum_paired_quote_count"))
        if (
            paired_quote_count is None
            or minimum_pair_count is None
            or minimum_pair_count <= 0
            or paired_quote_count < minimum_pair_count
        ):
            return {"recorded": False, "reason": "COMPLETE_PAIR_MINIMUM_NOT_MET"}

        reference_price = _finite(payload.get("reference_price"))
        if reference_price is None or reference_price <= 0.0:
            return {"recorded": False, "reason": "REFERENCE_PRICE_INVALID"}
        if str(payload.get("spot_source") or "") != "databento_opra_put_call_parity":
            return {"recorded": False, "reason": "REFERENCE_SOURCE_INVALID"}
        spot_formula_version = str(payload.get("spot_formula_version") or "").strip()
        risk_free_rate = _finite(payload.get("risk_free_rate"))
        time_to_expiration_years = _finite(payload.get("time_to_expiration_years"))
        contributing_pair_count = _integer(payload.get("contributing_pair_count"))
        contributing_quote_count = _integer(payload.get("contributing_quote_count"))
        formula_inputs = payload.get("formula_inputs")
        pair_identity_sha256 = str(
            payload.get("pair_identity_sha256") or ""
        ).lower().strip()
        symbol_mapping_version = str(
            payload.get("symbol_mapping_version") or ""
        ).lower().strip()
        if (
            not spot_formula_version
            or risk_free_rate is None
            or time_to_expiration_years is None
            or time_to_expiration_years <= 0.0
            or contributing_pair_count is None
            or contributing_pair_count <= 0
            or contributing_pair_count > paired_quote_count
            or contributing_quote_count != contributing_pair_count * 2
            or not isinstance(formula_inputs, Mapping)
            or formula_inputs.get("formula_version") != spot_formula_version
            or _finite(formula_inputs.get("risk_free_rate")) != risk_free_rate
            or _finite(formula_inputs.get("time_to_expiration_years"))
            != time_to_expiration_years
            or re.fullmatch(r"[0-9a-f]{64}", pair_identity_sha256) is None
            or re.fullmatch(r"[0-9a-f]{64}", symbol_mapping_version) is None
        ):
            return {"recorded": False, "reason": "REFERENCE_FORMULA_PROVENANCE_INVALID"}
        formula_pairs = formula_inputs.get("pairs")
        if not isinstance(formula_pairs, list) or len(formula_pairs) != contributing_pair_count:
            return {"recorded": False, "reason": "REFERENCE_FORMULA_INPUTS_INVALID"}
        canonical_pairs = json.dumps(
            formula_pairs,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        if hashlib.sha256(canonical_pairs).hexdigest() != pair_identity_sha256:
            return {"recorded": False, "reason": "PAIR_IDENTITY_HASH_MISMATCH"}
        mapping_versions: list[str] = []
        replay_values: list[float] = []
        for pair in formula_pairs:
            if not isinstance(pair, Mapping):
                return {"recorded": False, "reason": "REFERENCE_FORMULA_INPUTS_INVALID"}
            pair_identity = pair.get("pair_identity")
            if not isinstance(pair_identity, list) or len(pair_identity) < 3:
                return {"recorded": False, "reason": "OPTION_SERIES_IDENTITY_MISSING"}
            option_root = str(pair_identity[0] or "").upper().strip()
            call_symbol = str(pair.get("call_symbol") or "").upper().strip()
            put_symbol = str(pair.get("put_symbol") or "").upper().strip()
            strike = _finite(pair.get("strike"))
            call_mid = _finite(pair.get("call_mid"))
            put_mid = _finite(pair.get("put_mid"))
            call_mapping = str(pair.get("call_mapping_version") or "").lower().strip()
            put_mapping = str(pair.get("put_mapping_version") or "").lower().strip()
            call_match = OPTION_SYMBOL_RE.match(call_symbol)
            put_match = OPTION_SYMBOL_RE.match(put_symbol)
            if (
                not option_root
                or str(pair_identity[1]) != primary_expiration
                or strike is None
                or strike <= 0.0
                or _finite(pair_identity[2]) != strike
                or call_match is None
                or put_match is None
                or call_match.group("root") != option_root
                or put_match.group("root") != option_root
                or call_match.group("cp") != "C"
                or put_match.group("cp") != "P"
                or call_match.group("yymmdd") != primary_date.strftime("%y%m%d")
                or put_match.group("yymmdd") != primary_date.strftime("%y%m%d")
                or int(call_match.group("strike")) / 1000.0 != strike
                or int(put_match.group("strike")) / 1000.0 != strike
                or call_mid is None
                or call_mid <= 0.0
                or put_mid is None
                or put_mid <= 0.0
                or re.fullmatch(r"[0-9a-f]{64}", call_mapping) is None
                or re.fullmatch(r"[0-9a-f]{64}", put_mapping) is None
            ):
                return {"recorded": False, "reason": "OPTION_SERIES_IDENTITY_MISSING"}
            mapping_versions.extend((call_mapping, put_mapping))
            replay_values.append(
                call_mid
                - put_mid
                + strike * math.exp(-risk_free_rate * time_to_expiration_years)
            )
        replay_price = sorted(replay_values)[len(replay_values) // 2]
        if len(replay_values) % 2 == 0:
            middle = len(replay_values) // 2
            replay_price = (
                sorted(replay_values)[middle - 1] + sorted(replay_values)[middle]
            ) / 2.0
        if not math.isclose(reference_price, replay_price, rel_tol=1e-12, abs_tol=1e-9):
            return {"recorded": False, "reason": "REFERENCE_FORMULA_REPLAY_MISMATCH"}
        # Individual mapping versions are bound into ``pair_identity_sha256``.
        # ``symbol_mapping_version`` is the stable hash of the complete selected
        # subscription mapping set and is part of range provenance continuity.

        sample_time = _parse_utc(payload.get("sample_timestamp_utc"))
        source_time = _parse_utc(payload.get("source_timestamp_utc"))
        captured_at = _parse_utc(payload.get("captured_at_utc"))
        if sample_time is None or source_time is None or captured_at is None:
            return {"recorded": False, "reason": "REFERENCE_TIMESTAMP_MISSING"}
        if (
            sample_time.microsecond != 0
            or int(sample_time.timestamp()) % ORB_REFERENCE_CADENCE_SECONDS != 0
        ):
            return {"recorded": False, "reason": "SAMPLE_BUCKET_INVALID"}
        sample_et = sample_time.astimezone(EASTERN)
        source_et = source_time.astimezone(EASTERN)
        if (
            sample_et.date() != source_et.date()
            or not (ORB_START_ET <= sample_et.time() < REGULAR_CLOSE_ET)
            or not (ORB_START_ET <= source_et.time() < REGULAR_CLOSE_ET)
        ):
            return {"recorded": False, "reason": "OUTSIDE_REGULAR_SESSION"}
        capture_offset = (captured_at - sample_time).total_seconds()
        if not 0.0 <= capture_offset < ORB_REFERENCE_CADENCE_SECONDS:
            return {"recorded": False, "reason": "SAMPLE_BUCKET_CAPTURE_MISMATCH"}
        source_age_seconds = (captured_at - source_time).total_seconds()
        if source_age_seconds < -0.05:
            return {"recorded": False, "reason": "SOURCE_TIMESTAMP_IN_FUTURE"}
        if source_age_seconds > ORB_REFERENCE_QUOTE_FRESHNESS_SECONDS:
            return {"recorded": False, "reason": "SOURCE_TIMESTAMP_STALE"}

        earliest_ts_event_ns = _integer(payload.get("earliest_ts_event_ns"))
        latest_ts_event_ns = _integer(payload.get("latest_ts_event_ns"))
        earliest_ts_recv_ns = _integer(payload.get("earliest_ts_recv_ns"))
        latest_ts_recv_ns = _integer(payload.get("latest_ts_recv_ns"))
        observation_index_ns = _integer(payload.get("observation_index_ns"))
        if (
            earliest_ts_event_ns is None
            or latest_ts_event_ns is None
            or earliest_ts_recv_ns is None
            or latest_ts_recv_ns is None
            or observation_index_ns is None
            or min(
                earliest_ts_event_ns,
                latest_ts_event_ns,
                earliest_ts_recv_ns,
                latest_ts_recv_ns,
                observation_index_ns,
            )
            <= 0
            or earliest_ts_event_ns > latest_ts_event_ns
            or earliest_ts_recv_ns > latest_ts_recv_ns
            or latest_ts_event_ns > latest_ts_recv_ns
        ):
            return {"recorded": False, "reason": "PROVIDER_TIMESTAMP_ORDER_INVALID"}
        recv_time = datetime.fromtimestamp(latest_ts_recv_ns / 1_000_000_000, tz=UTC)
        if abs((recv_time - source_time).total_seconds()) > 0.000002:
            return {"recorded": False, "reason": "SOURCE_TIMESTAMP_MISMATCH"}
        reported_source_age = _finite(payload.get("source_quote_age_seconds"))
        maximum_source_age = _finite(
            payload.get("maximum_source_quote_age_seconds")
        )
        timestamp_span = _finite(payload.get("source_timestamp_span_seconds"))
        quote_freshness_limit = _finite(
            payload.get("quote_freshness_limit_seconds")
        )
        expected_maximum_age = (
            captured_at.timestamp() - earliest_ts_recv_ns / 1_000_000_000
        )
        expected_span = (latest_ts_recv_ns - earliest_ts_recv_ns) / 1_000_000_000
        if (
            reported_source_age is None
            or maximum_source_age is None
            or timestamp_span is None
            or quote_freshness_limit is None
            or not math.isclose(
                quote_freshness_limit,
                ORB_REFERENCE_QUOTE_FRESHNESS_SECONDS,
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            or not math.isclose(reported_source_age, source_age_seconds, abs_tol=0.000002)
            or not math.isclose(maximum_source_age, expected_maximum_age, abs_tol=0.000002)
            or not math.isclose(timestamp_span, expected_span, abs_tol=0.000002)
            or reported_source_age < -0.05
            or maximum_source_age < reported_source_age
            or maximum_source_age > ORB_REFERENCE_QUOTE_FRESHNESS_SECONDS
        ):
            return {"recorded": False, "reason": "SOURCE_TIMESTAMP_EVIDENCE_INVALID"}

        same_day = payload.get("same_day_profile_available")
        if not isinstance(same_day, bool) or same_day is not (primary_date == sample_et.date()):
            return {"recorded": False, "reason": "EXPIRATION_CLASSIFICATION_INVALID"}

        identity = json.dumps(
            {
                "symbol": symbol,
                "sample_timestamp_utc": _utc_iso(sample_time),
                "subscription_epoch_id": subscription_epoch_id,
                "subscription_generation": generation,
                "universe_sha256": universe_sha256,
                "primary_expiration": primary_expiration,
                "spot_formula_version": spot_formula_version,
                "risk_free_rate": risk_free_rate,
                "symbol_mapping_version": symbol_mapping_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        sample_id = hashlib.sha256(identity).hexdigest()
        sample = {
            "sample_id": sample_id,
            "symbol": symbol,
            "trading_date": sample_et.date(),
            "sample_timestamp_utc": sample_time,
            "source_timestamp_utc": source_time,
            "captured_at_utc": captured_at,
            "provider": "databento",
            "subscription_epoch_id": subscription_epoch_id,
            "subscription_generation": generation,
            "reference_price": reference_price,
            "spot_source": "databento_opra_put_call_parity",
            "spot_formula_version": spot_formula_version,
            "risk_free_rate": risk_free_rate,
            "time_to_expiration_years": time_to_expiration_years,
            "primary_expiration": primary_expiration,
            "same_day_profile_available": same_day,
            "universe_sha256": universe_sha256,
            "universe_is_fallback": False,
            "paired_quote_count": paired_quote_count,
            "minimum_paired_quote_count": minimum_pair_count,
            "contributing_pair_count": contributing_pair_count,
            "contributing_quote_count": contributing_quote_count,
            "earliest_ts_event_ns": earliest_ts_event_ns,
            "latest_ts_event_ns": latest_ts_event_ns,
            "earliest_ts_recv_ns": earliest_ts_recv_ns,
            "latest_ts_recv_ns": latest_ts_recv_ns,
            "observation_index_ns": observation_index_ns,
            "source_quote_age_seconds": reported_source_age,
            "maximum_source_quote_age_seconds": maximum_source_age,
            "source_timestamp_span_seconds": timestamp_span,
            "quote_freshness_limit_seconds": quote_freshness_limit,
            "pair_identity_sha256": pair_identity_sha256,
            "symbol_mapping_version": symbol_mapping_version,
            "formula_inputs_json": formula_inputs,
            "processing_clock_status": "synchronized",
            "timestamp_order_valid": True,
            "handoff_status": "active",
            "generation_state_unchanged": True,
            "universe_state_unchanged": True,
            "validation_status": "valid",
            # Custom in-memory loaders consume this marker directly.  The
            # database saver persists the authoritative immutable sidecar
            # instead and ignores this non-column field.
            "progress_eligible": not defer_progress_decision,
        }
        saved = self.reference_saver(sample)
        result = {
            "recorded": saved is not None,
            "reason": None if saved is not None else "PERSISTENCE_FAILED",
            "sample_id": sample_id,
            "sample_timestamp_utc": _utc_iso(sample_time),
            "source_timestamp_utc": _utc_iso(source_time),
            "subscription_epoch_id": subscription_epoch_id if saved is not None else None,
            "subscription_generation": generation if saved is not None else None,
            "progress_eligible": False,
            "progress_decision_pending": bool(
                saved is not None and defer_progress_decision
            ),
        }
        if saved is not None and not defer_progress_decision:
            decision = self.record_reference_decision(
                sample_id=sample_id,
                sample_timestamp_utc=sample_time,
                intended_bucket_utc=sample_time,
                attempt_completed_at_utc=captured_at,
                progress_eligible=True,
                reason=None,
            )
            if decision.get("recorded") is True:
                result["progress_eligible"] = True
                result["progress_decision_pending"] = False
            else:
                result["reason"] = "REFERENCE_PROGRESS_DECISION_PERSISTENCE_FAILED"
        return result

    def record_reference_decision(
        self,
        *,
        sample_id: str,
        sample_timestamp_utc: Any,
        intended_bucket_utc: Any,
        attempt_completed_at_utc: Any,
        progress_eligible: bool,
        reason: str | None,
    ) -> dict[str, Any]:
        """Append the one final, immutable progress decision for a raw sample."""
        decision = {
            "sample_id": str(sample_id or "").lower().strip(),
            "sample_timestamp_utc": sample_timestamp_utc,
            "intended_bucket_utc": intended_bucket_utc,
            "attempt_completed_at_utc": attempt_completed_at_utc,
            "progress_eligible": progress_eligible,
            "reason": reason,
            "decision_status": "final",
        }
        saved = self.reference_decision_saver(decision)
        return {
            "recorded": saved is not None,
            "reason": (
                None
                if saved is not None
                else "REFERENCE_PROGRESS_DECISION_PERSISTENCE_FAILED"
            ),
            **decision,
        }

    def snapshot(
        self,
        symbol: str,
        *,
        trading_date: date | None = None,
        as_of_utc: datetime | None = None,
        configured: bool = True,
        active_subscription_epoch_id: str | None = None,
        active_subscription_generation: int | None = None,
        active_handoff_status: str | None = None,
    ) -> dict[str, Any]:
        """Build one fail-closed ORB and pin-drift view from retained evidence."""
        normalized = str(symbol or "").upper().strip()
        as_of = _parse_utc(as_of_utc) or _parse_utc(self.now_utc())
        assert as_of is not None
        target_date = trading_date or as_of.astimezone(EASTERN).date()
        start_utc = _et_boundary(target_date, ORB_START_ET)
        end_utc = _et_boundary(target_date, ORB_END_ET)
        close_utc = _et_boundary(target_date, REGULAR_CLOSE_ET)
        runtime_binding_applied = any(
            value is not None
            for value in (
                active_subscription_epoch_id,
                active_subscription_generation,
                active_handoff_status,
            )
        )

        rows = self.reference_loader(normalized, target_date, as_of_utc=as_of)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            # Production loaders always project the immutable decision sidecar.
            # Requiring the marker here also prevents a custom/legacy loader
            # from accidentally promoting a pending raw row.
            if row.get("progress_eligible") is not True:
                continue
            source_time = _parse_utc(row.get("sample_timestamp_utc"))
            price = _finite(row.get("reference_price"))
            if source_time is None or price is None or price <= 0.0:
                continue
            normalized_rows.append({**row, "_source_time": source_time, "_price": price})
        normalized_rows.sort(
            key=lambda row: (
                row["_source_time"],
                _parse_utc(row.get("captured_at_utc")) or row["_source_time"],
                str(row.get("sample_id") or ""),
            )
        )
        structure_rows: list[dict[str, Any]] = []
        for row in self.loader(normalized, target_date, as_of_utc=as_of):
            source_time = _parse_utc(row.get("source_timestamp_utc"))
            if source_time is not None:
                structure_rows.append({**row, "_source_time": source_time})
        structure_rows.sort(
            key=lambda row: (row["_source_time"], str(row.get("observation_id") or ""))
        )
        raw_range_rows = [
            row for row in normalized_rows if start_utc <= row["_source_time"] < end_utc
        ]
        # One sampler bucket is one price sample. Provider timestamps remain
        # retained separately, so an unchanged but still-fresh quote does not
        # make the 5-second cadence appear missing.
        range_by_source_time: dict[datetime, dict[str, Any]] = {}
        for row in raw_range_rows:
            range_by_source_time[row["_source_time"]] = row
        range_rows = [range_by_source_time[key] for key in sorted(range_by_source_time)]
        latest = normalized_rows[-1] if normalized_rows else None

        if as_of < start_utc:
            clock_status = "not_started"
        elif as_of < end_utc:
            clock_status = "forming"
        else:
            clock_status = "closed"

        sample_count = len(range_rows)
        opening_price = range_rows[0]["_price"] if range_rows else None
        orb_high = max((row["_price"] for row in range_rows), default=None)
        orb_low = min((row["_price"] for row in range_rows), default=None)
        midpoint = (
            (orb_high + orb_low) / 2.0
            if orb_high is not None and orb_low is not None
            else None
        )
        range_width = (
            orb_high - orb_low
            if orb_high is not None and orb_low is not None
            else None
        )
        range_width_pct = (
            (range_width / opening_price) * 100.0
            if range_width is not None and opening_price is not None and opening_price > 0.0
            else None
        )

        elapsed_end = min(
            max(as_of, start_utc),
            end_utc - timedelta(microseconds=1),
        )
        elapsed_seconds = max(0.0, (elapsed_end - start_utc).total_seconds())
        expected_samples = (
            int(elapsed_seconds // self.expected_cadence_seconds) + 1
            if as_of >= start_utc
            else 0
        )
        capture_ratio = (
            min(1.0, sample_count / expected_samples) if expected_samples > 0 else None
        )
        first_sample_utc = range_rows[0]["_source_time"] if range_rows else None
        last_sample_utc = range_rows[-1]["_source_time"] if range_rows else None
        first_sample_lag_seconds = (
            (first_sample_utc - start_utc).total_seconds()
            if first_sample_utc is not None
            else None
        )
        opening_bucket_present = first_sample_utc == start_utc
        end_gap_seconds = (
            (end_utc - last_sample_utc).total_seconds()
            if last_sample_utc is not None
            else None
        )
        gaps = [
            (current["_source_time"] - previous["_source_time"]).total_seconds()
            for previous, current in zip(range_rows, range_rows[1:])
        ]
        max_gap_seconds = max(gaps) if gaps else None
        tolerance = max(30.0, self.expected_cadence_seconds * 3.0)
        boundary_coverage = bool(
            range_rows
            and opening_bucket_present
            and first_sample_lag_seconds is not None
            and first_sample_lag_seconds <= tolerance
            and end_gap_seconds is not None
            and end_gap_seconds <= tolerance
        )
        gap_coverage = max_gap_seconds is None or max_gap_seconds <= tolerance
        range_identities = {
            _orb_reference_provenance_identity(row) for row in raw_range_rows
        }
        range_identity = next(iter(range_identities)) if len(range_identities) == 1 else None
        range_provenance_aligned = bool(
            range_identity is not None and all(value is not None for value in range_identity)
        )
        latest_identity = (
            _orb_reference_provenance_identity(latest) if latest else None
        )
        current_vs_range_aligned = bool(
            range_provenance_aligned
            and latest_identity is not None
            and latest_identity == range_identity
            and _runtime_identity_aligned(
                latest,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
        )
        active_runtime_epoch_aligned = bool(
            runtime_binding_applied
            and _runtime_identity_aligned(
                latest,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
        )

        if not configured:
            capture_status = "not_configured"
        elif clock_status == "not_started":
            capture_status = "not_started"
        elif not range_rows:
            capture_status = "awaiting_first_sample" if clock_status == "forming" else "unavailable"
        elif not range_provenance_aligned:
            capture_status = "partial"
        elif clock_status == "forming":
            capture_status = "forming"
        elif (
            boundary_coverage
            and gap_coverage
            and capture_ratio is not None
            and capture_ratio >= ORB_MIN_CAPTURE_RATIO
        ):
            capture_status = "complete"
        else:
            capture_status = "partial"

        last_known_reference_price = latest["_price"] if latest else None
        current_price = (
            last_known_reference_price
            if not runtime_binding_applied or active_runtime_epoch_aligned
            else None
        )
        latest_knowledge_time = (
            _parse_utc(latest.get("captured_at_utc")) if latest else None
        )
        if latest_knowledge_time is None and latest:
            latest_knowledge_time = latest["_source_time"]
        current_reference_age_seconds = (
            max(0.0, (as_of - latest_knowledge_time).total_seconds())
            if latest_knowledge_time is not None
            else None
        )
        current_reference_fresh = bool(
            current_reference_age_seconds is not None
            and current_reference_age_seconds
            <= ORB_CURRENT_REFERENCE_MAX_AGE_SECONDS
            and (not runtime_binding_applied or active_runtime_epoch_aligned)
        )
        if capture_status == "forming":
            breakout_direction = "forming"
        elif (
            capture_status != "complete"
            or current_price is None
            or not current_vs_range_aligned
            or not current_reference_fresh
        ):
            breakout_direction = "unavailable"
        elif current_price > float(orb_high):
            breakout_direction = "bullish"
        elif current_price < float(orb_low):
            breakout_direction = "bearish"
        else:
            breakout_direction = "inside"

        latest_structure = structure_rows[-1] if structure_rows else None
        latest_structure_identity = (
            _provenance_identity(latest_structure) if latest_structure else None
        )
        structure_vs_reference_aligned = bool(
            latest_structure_identity is not None
            and latest is not None
            and _complete_provenance_identity(latest_structure)
            and _complete_provenance_identity(latest)
            and latest_structure_identity == _provenance_identity(latest)
            and _runtime_identity_aligned(
                latest_structure,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
            and _runtime_identity_aligned(
                latest,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
        )
        latest_structure_captured_at = (
            _parse_utc(latest_structure.get("captured_at_utc"))
            if latest_structure
            else None
        )
        latest_structure_freshness_time = (
            latest_structure["_source_time"] if latest_structure else None
        )
        structure_reference_age_seconds = (
            (as_of - latest_structure_freshness_time).total_seconds()
            if latest_structure_freshness_time is not None
            else None
        )
        structure_reference_fresh = bool(
            structure_reference_age_seconds is not None
            and 0.0
            <= structure_reference_age_seconds
            <= MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
        )
        if latest_structure is None or latest is None:
            structure_reference_status = "unavailable"
        elif not structure_vs_reference_aligned:
            structure_reference_status = "mismatch"
        elif not structure_reference_fresh:
            structure_reference_status = "stale"
        else:
            structure_reference_status = "aligned"

        calculation_bound_rows = [
            row
            for row in structure_rows
            if latest_structure_identity is not None
            and _complete_provenance_identity(latest_structure)
            and _complete_provenance_identity(row)
            and _provenance_identity(row) == latest_structure_identity
            and row.get("same_day_profile_available")
            is latest_structure.get("same_day_profile_available")
            and str(row.get("validation_status") or "").lower().strip() == "valid"
            and str(row.get("calculation_id") or "").strip()
        ]
        last_calculation_bound = (
            calculation_bound_rows[-1] if calculation_bound_rows else None
        )
        any_calculation_bound_row = any(
            str(row.get("calculation_id") or "").strip() for row in structure_rows
        )
        calculation_bound_source_time = (
            last_calculation_bound["_source_time"]
            if last_calculation_bound is not None
            else None
        )
        calculation_bound_captured_at = (
            _parse_utc(last_calculation_bound.get("captured_at_utc"))
            if last_calculation_bound is not None
            else None
        )
        calculation_bound_freshness_time = (
            min(calculation_bound_source_time, calculation_bound_captured_at)
            if calculation_bound_source_time is not None
            and calculation_bound_captured_at is not None
            else calculation_bound_source_time or calculation_bound_captured_at
        )
        calculation_bound_source_age_seconds = (
            (as_of - calculation_bound_source_time).total_seconds()
            if calculation_bound_source_time is not None
            else None
        )
        calculation_bound_capture_age_seconds = (
            (as_of - calculation_bound_captured_at).total_seconds()
            if calculation_bound_captured_at is not None
            else None
        )
        calculation_bound_age_seconds = (
            (as_of - calculation_bound_freshness_time).total_seconds()
            if calculation_bound_freshness_time is not None
            else None
        )
        calculation_bound_reference_aligned = bool(
            last_calculation_bound is not None
            and latest is not None
            and _complete_provenance_identity(latest)
            and _provenance_identity(last_calculation_bound)
            == _provenance_identity(latest)
            and last_calculation_bound.get("same_day_profile_available")
            is latest.get("same_day_profile_available")
        )
        calculation_bound_runtime_aligned = bool(
            last_calculation_bound is not None
            and latest is not None
            and _runtime_identity_aligned(
                last_calculation_bound,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
            and _runtime_identity_aligned(
                latest,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
        )
        calculation_bound_fresh = bool(
            calculation_bound_source_age_seconds is not None
            and calculation_bound_capture_age_seconds is not None
            and 0.0
            <= calculation_bound_source_age_seconds
            <= MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
            and 0.0
            <= calculation_bound_capture_age_seconds
            <= MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
        )
        if last_calculation_bound is None:
            calculation_bound_status = (
                "mismatch"
                if any_calculation_bound_row
                and latest_structure_identity is not None
                and _complete_provenance_identity(latest_structure)
                and latest is not None
                else "unavailable"
            )
        elif not (
            calculation_bound_reference_aligned
            and calculation_bound_runtime_aligned
        ):
            calculation_bound_status = "mismatch"
        elif not calculation_bound_fresh:
            calculation_bound_status = "stale"
        else:
            calculation_bound_status = "aligned"
        calculation_bound_gamma = (
            _finite(last_calculation_bound.get("gamma_pin"))
            if last_calculation_bound is not None
            else None
        )
        calculation_bound_pain = (
            _finite(last_calculation_bound.get("max_pain"))
            if last_calculation_bound is not None
            else None
        )
        if calculation_bound_gamma is not None and calculation_bound_pain is not None:
            calculation_bound_level_availability = "available"
        elif calculation_bound_gamma is not None or calculation_bound_pain is not None:
            calculation_bound_level_availability = "partial"
        else:
            calculation_bound_level_availability = "unavailable"
        comparison_rows = (
            [
                row
                for row in structure_rows
                if _provenance_identity(row) == latest_structure_identity
            ]
            if latest_structure_identity is not None
            and _complete_provenance_identity(latest_structure)
            else []
        )
        opening_structure_rows = [
            row
            for row in comparison_rows
            if start_utc <= row["_source_time"] < end_utc
        ]
        gamma_values = [
            value
            for row in comparison_rows
            if (value := _finite(row.get("gamma_pin"))) is not None
        ]
        pain_values = [
            value
            for row in comparison_rows
            if (value := _finite(row.get("max_pain"))) is not None
        ]
        prior_gamma_values = [
            value
            for row in comparison_rows[:-1]
            if (value := _finite(row.get("gamma_pin"))) is not None
        ]
        current_gamma = (
            _finite(latest_structure.get("gamma_pin"))
            if latest_structure
            else None
        )
        previous_gamma = prior_gamma_values[-1] if prior_gamma_values else None
        opening_gamma = next(
            (
                _finite(row.get("gamma_pin"))
                for row in opening_structure_rows
                if _finite(row.get("gamma_pin")) is not None
            ),
            None,
        )
        prior_pain_values = [
            value
            for row in comparison_rows[:-1]
            if (value := _finite(row.get("max_pain"))) is not None
        ]
        current_pain = (
            _finite(latest_structure.get("max_pain"))
            if latest_structure
            else None
        )
        previous_pain = prior_pain_values[-1] if prior_pain_values else None
        opening_pain = next(
            (
                _finite(row.get("max_pain"))
                for row in opening_structure_rows
                if _finite(row.get("max_pain")) is not None
            ),
            None,
        )
        generations = sorted(
            {
                generation
                for row in normalized_rows
                if (generation := _integer(row.get("subscription_generation"))) is not None
            }
        )
        subscription_epoch_ids = sorted(
            {
                str(row.get("subscription_epoch_id"))
                for row in normalized_rows
                if row.get("subscription_epoch_id")
            }
        )
        providers = sorted(
            {str(row.get("provider")) for row in normalized_rows if row.get("provider")}
        )
        universe_hashes = sorted(
            {str(row.get("universe_sha256")) for row in normalized_rows if row.get("universe_sha256")}
        )
        comparison_reset = bool(
            latest_structure_identity is not None
            and any(
                _provenance_identity(row) != latest_structure_identity
                for row in structure_rows
            )
        )
        if current_gamma is not None and current_pain is not None:
            current_pin_level_availability = "available"
        elif current_gamma is not None or current_pain is not None:
            current_pin_level_availability = "partial"
        else:
            current_pin_level_availability = "unavailable"
        current_pin_levels_available = current_pin_level_availability == "available"
        warnings: list[str] = []
        if capture_status == "partial":
            warnings.append("OPENING_RANGE_CAPTURE_PARTIAL")
        elif capture_status == "unavailable":
            warnings.append("OPENING_RANGE_NOT_CAPTURED")
        if len(generations) > 1:
            warnings.append("MULTIPLE_SUBSCRIPTION_GENERATIONS")
        if not range_provenance_aligned and range_rows:
            warnings.append("OPENING_RANGE_PROVENANCE_MIXED")
        if capture_status == "complete" and not current_vs_range_aligned:
            warnings.append("CURRENT_PROVENANCE_DIFFERS_FROM_ORB")
        if latest and runtime_binding_applied and not active_runtime_epoch_aligned:
            warnings.append("CURRENT_RUNTIME_PROVENANCE_MISMATCH")
        if latest and not current_reference_fresh:
            warnings.append("CURRENT_REFERENCE_STALE")
        if structure_reference_status == "mismatch":
            warnings.append("STRUCTURE_REFERENCE_PROVENANCE_MISMATCH")
        elif structure_reference_status == "stale":
            warnings.append("STRUCTURE_REFERENCE_STALE")
        if latest_structure and current_pin_level_availability == "unavailable":
            warnings.append("CURRENT_PIN_LEVELS_UNAVAILABLE")
        elif latest_structure and current_pin_level_availability == "partial":
            warnings.append("CURRENT_PIN_LEVELS_PARTIAL")

        reference_semantics = _reference_semantics(normalized, latest)
        if normalized == "VIX":
            warnings.append("VIX_OPTION_PARITY_FORWARD_CONTEXT_NOT_SPOT")
        elif latest and latest.get("same_day_profile_available") is not True:
            warnings.append("NON_SAME_DAY_PRIMARY_EXPIRATION_CONTEXT_ONLY")

        opening_ranges = {
            f"{minutes}m": _opening_window_projection(
                normalized_rows,
                latest=latest,
                start_utc=start_utc,
                end_utc=start_utc + timedelta(minutes=minutes),
                as_of_utc=as_of,
                expected_cadence_seconds=self.expected_cadence_seconds,
                configured=configured,
                active_subscription_epoch_id=active_subscription_epoch_id,
                active_subscription_generation=active_subscription_generation,
                active_handoff_status=active_handoff_status,
            )
            for minutes in ORB_WINDOWS_MINUTES
        }
        for window in opening_ranges.values():
            window["reference_semantics"] = dict(reference_semantics)
            window["directional_evidence_eligible"] = bool(
                reference_semantics["directional_base_eligible"]
                and window["capture_status"] == "complete"
                and window["provenance"]["current_vs_range_aligned"]
                and window["current_reference_fresh"]
            )
            window["combined_structure_directional_evidence_eligible"] = bool(
                window["directional_evidence_eligible"]
                and structure_vs_reference_aligned
                and structure_reference_fresh
                and current_pin_levels_available
            )
            window["provenance"]["structure_vs_reference_aligned"] = (
                structure_vs_reference_aligned
            )
            window["provenance"]["structure_reference_status"] = (
                structure_reference_status
            )
            window["provenance"]["structure_reference_fresh"] = (
                structure_reference_fresh
            )
            window["provenance"]["structure_reference_age_seconds"] = (
                structure_reference_age_seconds
            )
            window["provenance"]["structure_reference_max_age_seconds"] = (
                MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
            )
            window["provenance"]["current_pin_level_availability"] = (
                current_pin_level_availability
            )
            if structure_reference_status == "mismatch":
                window["warnings"].append(
                    "STRUCTURE_REFERENCE_PROVENANCE_MISMATCH"
                )
            elif structure_reference_status == "stale":
                window["warnings"].append("STRUCTURE_REFERENCE_STALE")
            if latest_structure and current_pin_level_availability == "unavailable":
                window["warnings"].append("CURRENT_PIN_LEVELS_UNAVAILABLE")
            elif latest_structure and current_pin_level_availability == "partial":
                window["warnings"].append("CURRENT_PIN_LEVELS_PARTIAL")
            if normalized == "VIX":
                window["warnings"].append(
                    "VIX_OPTION_PARITY_FORWARD_CONTEXT_NOT_SPOT"
                )
            elif latest and latest.get("same_day_profile_available") is not True:
                window["warnings"].append(
                    "NON_SAME_DAY_PRIMARY_EXPIRATION_CONTEXT_ONLY"
                )

        structure_is_current = structure_reference_status == "aligned"
        visible_current_gamma = current_gamma if structure_is_current else None
        visible_current_pain = current_pain if structure_is_current else None

        return {
            "schema_version": ORB_SCHEMA_VERSION,
            "symbol": normalized,
            "trading_date": target_date.isoformat(),
            "configured": bool(configured),
            "range_definition": {
                "timezone": "America/New_York",
                "start_et": "09:30:00",
                "end_et_exclusive": "10:30:00",
                "available_windows_minutes": list(ORB_WINDOWS_MINUTES),
                "reference": "sampled MarketPin option-parity reference price",
            },
            "reference_semantics": reference_semantics,
            "as_of_utc": _utc_iso(as_of),
            "range_start_utc": _utc_iso(start_utc),
            "range_end_utc": _utc_iso(end_utc),
            "regular_close_utc": _utc_iso(close_utc),
            "clock_status": clock_status,
            "capture_status": capture_status,
            "orb_complete": capture_status == "complete",
            "opening_price": opening_price,
            "orb_high": orb_high,
            "orb_low": orb_low,
            "midpoint": midpoint,
            "range_width": range_width,
            "range_width_pct": range_width_pct,
            "current_price": current_price,
            "current_reference_fresh": current_reference_fresh,
            "last_known_reference": {
                "reference_price": last_known_reference_price,
                "sample_id": latest.get("sample_id") if latest else None,
                "subscription_epoch_id": (
                    latest.get("subscription_epoch_id") if latest else None
                ),
                "subscription_generation": (
                    _integer(latest.get("subscription_generation"))
                    if latest
                    else None
                ),
                "runtime_aligned": (
                    active_runtime_epoch_aligned
                    if runtime_binding_applied
                    else None
                ),
            },
            "breakout_direction": breakout_direction,
            "directional_evidence_eligible": bool(
                reference_semantics["directional_base_eligible"]
                and capture_status == "complete"
                and current_vs_range_aligned
                and current_reference_fresh
            ),
            "combined_structure_directional_evidence_eligible": bool(
                reference_semantics["directional_base_eligible"]
                and capture_status == "complete"
                and current_vs_range_aligned
                and current_reference_fresh
                and structure_vs_reference_aligned
                and structure_reference_fresh
                and current_pin_levels_available
            ),
            "position_in_range": (
                (current_price - orb_low) / range_width
                if current_price is not None
                and orb_low is not None
                and range_width is not None
                and range_width > 0.0
                else None
            ),
            "opening_ranges": opening_ranges,
            "capture_evidence": {
                "sample_count": sample_count,
                "raw_observation_count": len(raw_range_rows),
                "duplicate_source_timestamp_count": len(raw_range_rows) - sample_count,
                "expected_sample_count": expected_samples,
                "capture_ratio": capture_ratio,
                "minimum_complete_capture_ratio": ORB_MIN_CAPTURE_RATIO,
                "expected_cadence_seconds": self.expected_cadence_seconds,
                "first_sample_utc": _utc_iso(first_sample_utc),
                "opening_bucket_present": opening_bucket_present,
                "last_range_sample_utc": _utc_iso(last_sample_utc),
                "first_sample_lag_seconds": first_sample_lag_seconds,
                "end_gap_seconds": end_gap_seconds,
                "max_gap_seconds": max_gap_seconds,
                "current_reference_age_seconds": current_reference_age_seconds,
                "current_reference_max_age_seconds": ORB_CURRENT_REFERENCE_MAX_AGE_SECONDS,
            },
            "pin_behavior": {
                "gamma_pin": visible_current_gamma,
                "previous_gamma_pin": previous_gamma,
                "gamma_pin_change_last": _change(
                    visible_current_gamma, previous_gamma
                ),
                "opening_gamma_pin": opening_gamma,
                "gamma_pin_change_from_open": _change(
                    visible_current_gamma, opening_gamma
                ),
                "gamma_pin_change_count": _change_count(gamma_values),
                "max_pain": visible_current_pain,
                "previous_max_pain": previous_pain,
                "max_pain_change_last": _change(
                    visible_current_pain, previous_pain
                ),
                "opening_max_pain": opening_pain,
                "max_pain_change_from_open": _change(
                    visible_current_pain, opening_pain
                ),
                "max_pain_change_count": _change_count(pain_values),
                "comparison_scope": "latest_aligned_provenance_only",
                "comparison_reset": comparison_reset,
                "level_availability_status": current_pin_level_availability,
                "current_level_policy": (
                    "newest_structure_observation_only_no_carry_forward"
                ),
                "zero_gamma": (
                    _finite(latest_structure.get("zero_gamma"))
                    if latest_structure and structure_is_current
                    else None
                ),
                "pin_lead_ratio": (
                    _finite(latest_structure.get("pin_lead_ratio"))
                    if latest_structure and structure_is_current
                    else None
                ),
                "pin_is_contested": (
                    latest_structure.get("pin_is_contested")
                    if latest_structure and structure_is_current
                    else None
                ),
            },
            "last_known_structure": {
                "status": structure_reference_status,
                "level_availability_status": current_pin_level_availability,
                "source_timestamp_utc": (
                    _utc_iso(latest_structure["_source_time"])
                    if latest_structure
                    else None
                ),
                "captured_at_utc": (
                    _utc_iso(latest_structure_captured_at)
                    if latest_structure
                    else None
                ),
                "freshness_timestamp_utc": (
                    _utc_iso(latest_structure_freshness_time)
                    if latest_structure
                    else None
                ),
                "age_seconds": structure_reference_age_seconds,
                "maximum_current_age_seconds": (
                    MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
                ),
                "gamma_pin": current_gamma,
                "max_pain": current_pain,
                "zero_gamma": (
                    _finite(latest_structure.get("zero_gamma"))
                    if latest_structure
                    else None
                ),
                "calculation_id": (
                    latest_structure.get("calculation_id")
                    if latest_structure
                    else None
                ),
            },
            "last_calculation_bound_structure": {
                "status": calculation_bound_status,
                "level_availability_status": (
                    calculation_bound_level_availability
                ),
                "source_timestamp_utc": (
                    _utc_iso(calculation_bound_source_time)
                    if last_calculation_bound is not None
                    else None
                ),
                "captured_at_utc": (
                    _utc_iso(calculation_bound_captured_at)
                    if last_calculation_bound is not None
                    else None
                ),
                "freshness_timestamp_utc": (
                    _utc_iso(calculation_bound_freshness_time)
                    if last_calculation_bound is not None
                    else None
                ),
                "age_seconds": calculation_bound_age_seconds,
                "source_age_seconds": calculation_bound_source_age_seconds,
                "capture_age_seconds": calculation_bound_capture_age_seconds,
                "maximum_current_age_seconds": (
                    MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
                ),
                "reference_price": (
                    _finite(last_calculation_bound.get("reference_price"))
                    if last_calculation_bound is not None
                    else None
                ),
                "gamma_pin": calculation_bound_gamma,
                "max_pain": calculation_bound_pain,
                "zero_gamma": (
                    _finite(last_calculation_bound.get("zero_gamma"))
                    if last_calculation_bound is not None
                    else None
                ),
                "calculation_id": (
                    str(last_calculation_bound.get("calculation_id") or "").strip()
                    if last_calculation_bound is not None
                    else None
                ),
                "provider": (
                    last_calculation_bound.get("provider")
                    if last_calculation_bound is not None
                    else None
                ),
                "subscription_epoch_id": (
                    last_calculation_bound.get("subscription_epoch_id")
                    if last_calculation_bound is not None
                    else None
                ),
                "subscription_generation": (
                    _integer(last_calculation_bound.get("subscription_generation"))
                    if last_calculation_bound is not None
                    else None
                ),
                "universe_sha256": (
                    last_calculation_bound.get("universe_sha256")
                    if last_calculation_bound is not None
                    else None
                ),
                "primary_expiration": (
                    last_calculation_bound.get("primary_expiration")
                    if last_calculation_bound is not None
                    else None
                ),
                "same_day_profile_available": (
                    last_calculation_bound.get("same_day_profile_available")
                    if last_calculation_bound is not None
                    else None
                ),
                "current_provenance_aligned": (
                    calculation_bound_reference_aligned
                ),
                "runtime_aligned": calculation_bound_runtime_aligned,
                "evidence_eligible": bool(
                    calculation_bound_status == "aligned"
                    and calculation_bound_level_availability == "available"
                ),
            },
            "provenance": {
                "providers": providers,
                "spot_source": latest.get("spot_source") if latest else None,
                "subscription_epoch_ids": subscription_epoch_ids,
                "subscription_generations": generations,
                "universe_sha256_values": universe_hashes,
                "runtime_binding_applied": runtime_binding_applied,
                "active_runtime_epoch_aligned": active_runtime_epoch_aligned,
                "active_subscription_epoch_id": active_subscription_epoch_id,
                "active_subscription_generation": active_subscription_generation,
                "active_handoff_status": active_handoff_status,
                "range_provenance_aligned": range_provenance_aligned,
                "current_vs_range_aligned": current_vs_range_aligned,
                "structure_vs_reference_aligned": structure_vs_reference_aligned,
                "structure_reference_status": structure_reference_status,
                "structure_reference_fresh": structure_reference_fresh,
                "structure_reference_age_seconds": structure_reference_age_seconds,
                "structure_reference_max_age_seconds": (
                    MARKET_STRUCTURE_CURRENT_MAX_AGE_SECONDS
                ),
                "current_pin_level_availability": (
                    current_pin_level_availability
                ),
                "latest_source_timestamp_utc": _utc_iso(latest["_source_time"]) if latest else None,
                "latest_reference_sample_id": latest.get("sample_id") if latest else None,
                "latest_reference_subscription_epoch_id": (
                    latest.get("subscription_epoch_id") if latest else None
                ),
                "latest_calculation_id": (
                    latest_structure.get("calculation_id")
                    if latest_structure
                    else None
                ),
                "primary_expiration": latest.get("primary_expiration") if latest else None,
                "same_day_profile_available": latest.get("same_day_profile_available") if latest else None,
                "structure_subscription_generation": (
                    _integer(latest_structure.get("subscription_generation"))
                    if latest_structure
                    else None
                ),
                "structure_subscription_epoch_id": (
                    latest_structure.get("subscription_epoch_id")
                    if latest_structure
                    else None
                ),
                "structure_universe_sha256": (
                    latest_structure.get("universe_sha256")
                    if latest_structure
                    else None
                ),
            },
            "warnings": warnings,
            "limitations": [
                "Derived from sampled Databento OPRA put/call-parity reference prices; not official exchange OHLC.",
                str(reference_semantics["limitation"]),
                "A partial or unavailable range is never backfilled from a later price.",
            ],
        }


_journal: MarketStructureJournal | None = None


def get_market_structure_journal() -> MarketStructureJournal:
    global _journal
    if _journal is None:
        _journal = MarketStructureJournal()
    return _journal


async def run_market_structure_capture_loop(
    streamer: Any,
    journal: MarketStructureJournal,
    *,
    poll_seconds: float = 0.1,
) -> None:
    """Persist every distinct valid live revision without blocking ingestion."""
    seen: dict[str, tuple[Any, ...]] = {}
    while True:
        latest_by_symbol = streamer.get_all_latest()
        for raw_symbol, raw_payload in latest_by_symbol.items():
            if not isinstance(raw_payload, Mapping):
                continue
            payload = dict(raw_payload)
            payload.setdefault("symbol", str(raw_symbol).upper())
            symbol = str(payload.get("symbol") or raw_symbol).upper()
            revision = payload_revision_key(payload)
            if seen.get(symbol) == revision:
                continue
            try:
                result = await asyncio.to_thread(journal.record, payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Market structure capture failed for %s", symbol)
                continue
            if result.get("reason") != "PERSISTENCE_FAILED":
                seen[symbol] = revision
        await asyncio.sleep(max(0.05, float(poll_seconds)))
