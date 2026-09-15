from datetime import datetime, timedelta, timezone

from backend.prediction_authority import build_prediction_authority
from backend.api.helpers import payload_health, with_gex_level_semantics


def test_legacy_numeric_forecast_is_explicitly_research_only():
    authority = build_prediction_authority(
        prediction_mode="backend_lifecycle",
        forecast_state="RESEARCH_ONLY",
        numeric_available=True,
        decision_grade=True,
        promotion_verified=True,
    )

    assert authority["authority_state"] == "research_only"
    assert authority["tcbbo_promoted"] is False
    assert authority["decision_grade"] is False
    assert authority["is_estimate"] is True
    assert "not TCBBO promoted" in authority["source_label"]
    assert authority["observed_basis"] != "immutable_opra_tcbbo_trade_and_pretrade_nbbo"


def test_tcbbo_authority_requires_verified_promotion():
    unverified = build_prediction_authority(
        prediction_mode="tcbbo_promoted",
        forecast_state="VALID",
        numeric_available=True,
        decision_grade=True,
        promotion_verified=False,
    )
    promoted = build_prediction_authority(
        prediction_mode="tcbbo_promoted",
        forecast_state="VALID",
        numeric_available=True,
        decision_grade=True,
        promotion_verified=True,
    )

    assert unverified["authority_state"] == "research_only"
    assert unverified["tcbbo_promoted"] is False
    assert promoted["authority_state"] == "tcbbo_promoted_estimate"
    assert promoted["tcbbo_promoted"] is True
    assert promoted["decision_grade"] is True
    assert promoted["observed_basis"] == "immutable_opra_tcbbo_trade_and_pretrade_nbbo"


def test_non_numeric_state_never_claims_an_estimate():
    authority = build_prediction_authority(
        prediction_mode="backend_lifecycle",
        forecast_state="STALE",
        numeric_available=False,
    )

    assert authority["authority_state"] == "stale"
    assert authority["is_estimate"] is False
    assert authority["decision_grade"] is False


def test_raw_gex_semantics_are_research_only_not_promoted():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "likely_close": 7510.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
        }
    )

    assert payload["prediction_mode"] == "backend_lifecycle"
    assert payload["is_estimate"] is True
    assert payload["tcbbo_promoted"] is False
    assert payload["decision_grade"] is False
    assert payload["prediction_authority"]["authority_state"] == "research_only"


def test_nested_fallback_gex_semantics_abstain_and_are_not_usable():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "likely_close": 7510.0,
            "validation_is_valid": True,
            "universe_provenance": {"is_fallback": True},
        }
    )

    assert payload["prediction_mode"] == "closed_market_historical_context_only"
    assert payload["forecast_state"] == "ABSTAIN"
    assert payload["is_fallback"] is True
    assert payload["historical_context_only"] is True
    assert payload["analytical_levels_available"] is False
    assert payload["usable_for_prediction"] is False
    assert payload["prediction_authority"]["authority_state"] == "abstain"


def test_invalid_gex_semantics_never_claim_usable_analytical_levels():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "gamma_pin": 7510.0,
            "max_pain": 7490.0,
            "validation_is_valid": False,
            "gamma_excluded_from_model": True,
        }
    )

    assert payload["forecast_state"] == "ABSTAIN"
    assert payload["analytical_levels_available"] is False
    assert payload["usable_for_prediction"] is False
    assert payload["diagnostic_only"] is True
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["authority_state"] == "abstain"


def test_missing_validation_proof_is_unavailable_not_implicitly_usable():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "gamma_pin": 7510.0,
        }
    )

    assert payload["forecast_state"] == "UNAVAILABLE"
    assert payload["analytical_levels_available"] is False
    assert payload["usable_for_prediction"] is False
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["authority_state"] == "unavailable"


def test_explicit_upstream_ineligibility_cannot_be_redecorated_as_usable():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "likely_close": 7510.0,
            "gamma_pin": 7505.0,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "usable_for_prediction": False,
        }
    )

    assert payload["forecast_state"] == "UNAVAILABLE"
    assert payload["usable_for_prediction"] is False
    assert payload["is_estimate"] is False


def test_gamma_pin_alone_is_an_analytical_level_not_a_close_forecast():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "gamma_pin": 7510.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
        }
    )

    assert payload["analytical_levels_available"] is True
    assert payload["forecast_state"] == "UNAVAILABLE"
    assert payload["usable_for_prediction"] is False
    assert payload["diagnostic_only"] is True
    assert payload["is_estimate"] is False


def test_zero_only_invalid_shell_is_not_claimed_as_numeric_diagnostic_data():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 0.0,
            "spot_last": 0.0,
            "gamma_pin": 0.0,
            "max_pain": None,
            "gross_gex": 0.0,
            "net_gex": 0.0,
            "contracts_count": 0,
            "paired_quote_count": 0,
            "validation_is_valid": False,
            "gamma_excluded_from_model": True,
        }
    )

    assert payload["forecast_state"] == "ABSTAIN"
    assert payload["diagnostic_only"] is False
    assert payload["analytical_levels_available"] is False
    assert payload["usable_for_prediction"] is False


def test_stale_valid_payload_is_labeled_stale_and_not_usable():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "likely_close": 7510.0,
            "gamma_pin": 7505.0,
            "timestamp": (
                datetime.now(timezone.utc) - timedelta(minutes=5)
            ).isoformat(),
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
        }
    )

    assert payload["forecast_state"] == "STALE"
    assert payload["usable_for_prediction"] is False
    assert payload["analytical_levels_available"] is False
    assert payload["is_estimate"] is False
    assert payload["prediction_authority"]["authority_state"] == "stale"
    assert payload_health(payload)["status"] == "stale"


def test_explicit_quote_age_can_make_a_recent_calculation_timestamp_stale():
    payload = with_gex_level_semantics(
        {
            "provider": "databento",
            "price": 7500.0,
            "likely_close": 7510.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "quote_age_seconds": 300.0,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
        }
    )

    assert payload["forecast_state"] == "STALE"
    assert payload["usable_for_prediction"] is False
    assert payload_health(payload)["status"] == "stale"
