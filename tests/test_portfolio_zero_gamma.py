from datetime import datetime, timezone

import numpy as np
import pytest

from backend.research.portfolio_zero_gamma import (
    PORTFOLIO_ZERO_GAMMA_SHADOW_VERSION,
    portfolio_zero_gamma_from_calculation_inputs,
    portfolio_zero_gamma_spot_sweep,
    sign_crossings,
)


def test_shadow_spot_sweep_finds_portfolio_crossing_without_promoting_it():
    result = portfolio_zero_gamma_spot_sweep(
        [
            {
                "strike": 90.0,
                "option_type": "C",
                "iv": 0.25,
                "open_interest": 100.0,
                "expiration_date": "2026-08-28",
            },
            {
                "strike": 110.0,
                "option_type": "P",
                "iv": 0.25,
                "open_interest": 100.0,
                "expiration_date": "2026-08-28",
            },
        ],
        reference_spot=100.0,
        as_of_utc=datetime(2026, 8, 25, 16, tzinfo=timezone.utc),
        span_pct=0.20,
        grid_points=801,
        risk_free_rate=0.0,
    )

    assert result.formula_version == PORTFOLIO_ZERO_GAMMA_SHADOW_VERSION
    assert result.mode == "shadow_research"
    assert result.promoted is False
    assert result.calculation_status == "crossing_found"
    assert result.contracts_used == 2
    assert result.selected_crossing is not None
    assert 95.0 < result.selected_crossing < 105.0
    assert result.selected_crossing == min(
        result.crossing_levels, key=lambda value: abs(value - result.reference_spot)
    )
    assert len(result.spots) == 801
    assert len(result.signed_portfolio_gamma_1pct) == 801
    assert any("open interest is not direct evidence" in item for item in result.assumptions)


def test_shadow_spot_sweep_rejects_missing_contract_evidence_instead_of_imputing():
    result = portfolio_zero_gamma_spot_sweep(
        [{"strike": 100.0, "option_type": "C", "open_interest": 10.0}],
        reference_spot=100.0,
        as_of_utc="2026-08-25T16:00:00+00:00",
    )

    assert result.calculation_status == "no_usable_contracts"
    assert result.selected_crossing is None
    assert result.crossing_levels == ()
    assert result.spots == ()
    assert any("iv" in reason for reason in result.rejection_reasons)


def test_shadow_spot_sweep_reports_no_crossing_in_bounded_grid():
    result = portfolio_zero_gamma_spot_sweep(
        [
            {
                "strike": 100.0,
                "option_type": "C",
                "iv": 0.20,
                "open_interest": 100.0,
                "years_to_expiration": 0.05,
            }
        ],
        reference_spot=100.0,
    )

    assert result.calculation_status == "no_crossing_in_grid"
    assert result.selected_crossing is None
    assert result.crossing_levels == ()
    assert all(value > 0 for value in result.signed_portfolio_gamma_1pct)


def test_sign_crossings_returns_all_crossings_with_linear_interpolation():
    crossings = sign_crossings(
        np.asarray([90.0, 95.0, 100.0, 105.0, 110.0]),
        np.asarray([-1.0, 1.0, -1.0, 1.0, -1.0]),
    )

    assert crossings == pytest.approx((92.5, 97.5, 102.5, 107.5))


def test_persisted_calculation_input_adapter_uses_recorded_iv_oi_and_asof():
    result = portfolio_zero_gamma_from_calculation_inputs(
        {
            "input_schema_version": "gamma-inputs-v2-point-in-time",
            "calculated_at_utc": "2026-08-25T16:00:00+00:00",
            "calculated_gex_rows": [
                {
                    "strike": 90.0,
                    "option_type": "C",
                    "iv": 0.25,
                    "open_interest": 100.0,
                    "expiration_date": "2026-08-28",
                },
                {
                    "strike": 110.0,
                    "option_type": "P",
                    "iv": 0.25,
                    "open_interest": 100.0,
                    "expiration_date": "2026-08-28",
                },
            ],
            "parameters": {"risk_free_rate": 0.0, "contract_multiplier": 100.0},
            "output_summary": {"price": 100.0},
        },
        span_pct=0.2,
        grid_points=801,
    )

    assert result.calculation_status == "crossing_found"
    assert result.contracts_used == 2
    assert result.promoted is False
