"""Pure eligibility metadata for newly written raw calculation captures.

Formula validation remains research evidence. An eligible capture is only an
input to subsequent lifecycle checks; this metadata never authorizes a live
forecast or makes an archived observation current again.
"""
from __future__ import annotations

import math
import re
from numbers import Integral
from typing import Any, Mapping

from backend.workstation import payload_has_fallback_provenance


def _positive_finite(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return False
    return math.isfinite(number) and number > 0


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def finalize_snapshot_export_payload(
    payload: Mapping[str, Any],
    *,
    source: Mapping[str, Any],
    calculation_persisted: bool,
    subscription_context: Mapping[str, Any],
) -> dict[str, Any]:
    """Fail closed on eligibility without rewriting calculation evidence.

    The caller supplies the actual commit acknowledgment and one coherent
    runtime context captured after that commit. Source identities are compared,
    never replaced by the current epoch or generation.
    """
    reasons = []
    if payload.get("validation_is_valid") is not True:
        reasons.append("SOURCE_VALIDATION_FAILED")
    if payload.get("gamma_excluded_from_model") is not False:
        reasons.append("GAMMA_EXCLUDED_FROM_MODEL")
    if (
        payload.get("usable_for_prediction") is not True
        or source.get("usable_for_prediction", True) is not True
    ):
        reasons.append("SOURCE_NOT_DECLARED_USABLE")
    fallback = payload_has_fallback_provenance(source) or payload_has_fallback_provenance(payload)
    if fallback:
        reasons.append("NON_PRODUCTION_FALLBACK")
    if not _positive_finite(payload.get("spot_last")) or not _positive_finite(payload.get("likely_close")):
        reasons.append("INVALID_POINT_ESTIMATE")

    calculation_id = source.get("calculation_id")
    if (
        not isinstance(calculation_id, str)
        or not calculation_id.strip()
        or payload.get("calculation_id") != calculation_id
        or calculation_persisted is not True
    ):
        reasons.append("CALCULATION_INPUT_PERSISTENCE_UNVERIFIED")

    epoch = source.get("subscription_epoch_id")
    if (
        not isinstance(epoch, str)
        or re.fullmatch(r"[0-9a-f]{64}", epoch) is None
        or epoch != payload.get("subscription_epoch_id")
        or epoch != subscription_context.get("subscription_epoch_id")
    ):
        reasons.append("SUBSCRIPTION_EPOCH_MISMATCH_OR_MISSING")
    generation = source.get("subscription_generation")
    active_generation = subscription_context.get("subscription_generation")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, Integral)
        or generation <= 0
        or isinstance(active_generation, bool)
        or not isinstance(active_generation, Integral)
        or generation != active_generation
        or generation != payload.get("subscription_generation")
    ):
        reasons.append("SUBSCRIPTION_GENERATION_MISMATCH_OR_MISSING")
    if subscription_context.get("handoff_status") != "active":
        reasons.append("SUBSCRIPTION_NOT_ACTIVE")

    calculation_inputs = _mapping(source.get("_calculation_inputs"))
    parameters = _mapping(calculation_inputs.get("parameters"))
    oi_provenance = _mapping(
        payload.get("oi_analytics_provenance")
        or source.get("oi_analytics_provenance")
    )
    quote_freshness_limit = parameters.get("quote_freshness_seconds")
    provider_statistics_end = oi_provenance.get("provider_statistics_end")
    replay_missing_evidence = []
    if not _positive_finite(quote_freshness_limit):
        replay_missing_evidence.append("quote_freshness_limit_seconds")
        quote_freshness_limit = None
    if not isinstance(provider_statistics_end, str) or not provider_statistics_end.strip():
        replay_missing_evidence.append("provider_statistics_end")
        provider_statistics_end = None

    timestamp_utc = payload.get("timestamp_utc") or payload.get("generated_at_utc")
    legacy_timestamp = payload.get("timestamp")

    return {
        **payload,
        "timestamp": timestamp_utc or legacy_timestamp,
        "producer_timestamp_legacy": legacy_timestamp,
        "timestamp_semantics": "aware_utc_canonical_with_legacy_preserved",
        "quote_freshness_limit_seconds": quote_freshness_limit,
        "open_interest_provider_statistics_end_utc": provider_statistics_end,
        "transport_counter_scope": "shared_databento_stream",
        "transport_counter_semantics": "cumulative_process_totals_not_symbol_counts",
        "replay_evidence_complete": not replay_missing_evidence,
        "replay_missing_evidence": replay_missing_evidence,
        "usable_for_prediction": not reasons,
        "is_fallback": bool(fallback),
        "calculation_inputs_persisted": calculation_persisted is True,
        "prediction_eligibility_scope": "capture_input_at_write_requires_lifecycle_revalidation",
        "prediction_eligibility_reasons": reasons,
    }
