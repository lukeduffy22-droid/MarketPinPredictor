from __future__ import annotations

from typing import Any


PREDICTION_AUTHORITY_CONTRACT_VERSION = "prediction-authority.v1"
TCBBO_PROMOTED_MODE = "tcbbo_promoted"


def build_prediction_authority(
    *,
    prediction_mode: Any,
    forecast_state: Any,
    numeric_available: bool,
    decision_grade: bool = False,
    promotion_verified: bool = False,
) -> dict[str, object]:
    """Classify one forecast without conflating legacy and TCBBO evidence."""

    mode = str(prediction_mode or "unknown").strip() or "unknown"
    state = str(forecast_state or "UNAVAILABLE").strip().upper() or "UNAVAILABLE"
    promoted_tcbbo = bool(
        numeric_available
        and promotion_verified
        and mode == TCBBO_PROMOTED_MODE
    )
    effective_decision_grade = bool(
        promoted_tcbbo and decision_grade and state == "VALID"
    )

    if not numeric_available:
        authority_state = {
            "ABSTAIN": "abstain",
            "STALE": "stale",
        }.get(state, "unavailable")
    elif promoted_tcbbo:
        authority_state = "tcbbo_promoted_estimate"
    else:
        authority_state = "research_only"

    if not numeric_available:
        source_label = (
            "No promoted TCBBO estimate available"
            if mode == TCBBO_PROMOTED_MODE
            else "No usable lifecycle forecast - not TCBBO promoted"
        )
        observed_basis = "none_available"
        inferred_basis = "none_available"
        limitations = (
            "no_numeric_estimate_available",
            "not_decision_grade",
        )
    elif promoted_tcbbo:
        source_label = "Promoted TCBBO estimate"
        observed_basis = "immutable_opra_tcbbo_trade_and_pretrade_nbbo"
        inferred_basis = "versioned_trade_location_and_positioning_features"
        limitations = (
            "analytical_estimate_not_guaranteed_close",
            "opra_does_not_publish_aggressor_side_or_holdings",
        )
    else:
        source_label = (
            "Unverified TCBBO-mode research output"
            if mode == TCBBO_PROMOTED_MODE
            else "Databento live GEX research estimate - not TCBBO promoted"
        )
        observed_basis = "live_option_quote_and_open_interest_snapshot"
        inferred_basis = "legacy_gex_close_overlay_heuristic"
        limitations = (
            "not_tcbbo_promoted",
            "not_decision_grade",
            "confidence_is_not_a_calibrated_probability",
        )

    return {
        "contract_version": PREDICTION_AUTHORITY_CONTRACT_VERSION,
        "authority_state": authority_state,
        "prediction_mode": mode,
        "forecast_state": state,
        "tcbbo_promoted": promoted_tcbbo,
        "promotion_verified": bool(promoted_tcbbo),
        "decision_grade": effective_decision_grade,
        "is_estimate": bool(numeric_available),
        "observed_basis": observed_basis,
        "inferred_basis": inferred_basis,
        "source_label": source_label,
        "limitations": list(limitations),
    }
