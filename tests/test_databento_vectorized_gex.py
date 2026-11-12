import numpy as np
import pytest

from backend.databento_streamer import (
    CONTRACT_MULTIPLIER,
    MIN_TTE_DAYS,
    batch_iv_gamma_gex,
    black_scholes_gamma,
    black_scholes_price,
    implied_volatility,
)


def test_batch_matches_scalar_iv_and_gamma_for_calls_puts_and_near_expiry():
    spot = 100.0
    strikes = np.array([96.0, 99.0, 100.0, 103.0, 105.0, 100.0])
    years = np.array([MIN_TTE_DAYS, 1 / 365, 3 / 365, 7 / 365, 30 / 365, MIN_TTE_DAYS])
    option_types = np.array(["C", "P", "C", "P", "C", "P"])
    open_interest = np.array([10.0, 25.0, 50.0, 75.0, 100.0, 125.0])
    true_vols = np.array([0.18, 0.23, 0.31, 0.42, 0.55, 0.27])
    mids = np.array([
        black_scholes_price(spot, strike, year, vol, option_type)
        for strike, year, vol, option_type in zip(strikes, years, true_vols, option_types)
    ])

    result = batch_iv_gamma_gex(
        spot,
        strikes,
        years,
        mids,
        option_types,
        open_interest,
    )

    assert result["valid_mask"].tolist() == [True] * len(strikes)
    for index, (strike, year, mid, option_type, oi) in enumerate(
        zip(strikes, years, mids, option_types, open_interest)
    ):
        scalar_iv = implied_volatility(spot, strike, year, mid, option_type)
        assert scalar_iv is not None
        scalar_gamma = black_scholes_gamma(spot, strike, year, scalar_iv)
        expected_sign = 1.0 if option_type == "C" else -1.0
        assert result["iv"][index] == pytest.approx(scalar_iv, rel=2e-10, abs=2e-12)
        # Deep ITM, minimum-TTE rows make gamma extremely sensitive to the
        # scalar Brent solver's stopping point. The batch solver remains within
        # a few parts per billion while converging more tightly on the IV root.
        assert result["gamma"][index] == pytest.approx(scalar_gamma, rel=1e-8, abs=1e-11)
        assert result["gex"][index] == pytest.approx(
            expected_sign * scalar_gamma * oi * CONTRACT_MULTIPLIER,
            rel=1e-8,
            abs=1e-11,
        )


def test_batch_rejects_invalid_quotes_and_unbracketed_iv_rows():
    spot = 100.0
    valid_mid = black_scholes_price(spot, 100.0, 1 / 365, 0.25, "C")
    result = batch_iv_gamma_gex(
        spot,
        strikes=np.array([100.0, 100.0, 100.0, 0.0, 100.0, 100.0, 100.0]),
        years=np.array([1 / 365, 1 / 365, 1 / 365, 1 / 365, 0.0, 1 / 365, 1 / 365]),
        mids=np.array([valid_mid, 0.0, np.nan, valid_mid, valid_mid, valid_mid, 200.0]),
        option_types=np.array(["C", "C", "C", "C", "C", "X", "C"]),
        open_interest=np.ones(7),
    )

    assert result["valid_mask"].tolist() == [True, False, False, False, False, False, False]
    assert result["rejection_reason"].tolist() == [
        "valid",
        "price_not_above_intrinsic",
        "nonfinite_input",
        "invalid_strike",
        "invalid_time_to_expiry",
        "invalid_option_type",
        "iv_root_not_bracketed",
    ]
    for field in ("iv", "gamma", "gex"):
        assert np.isfinite(result[field][0])
        assert np.isnan(result[field][1:]).all()


def test_batch_preserves_canonical_call_positive_put_negative_gex():
    spot = 100.0
    strikes = np.array([100.0, 100.0])
    years = np.array([7 / 365, 7 / 365])
    option_types = np.array(["C", "P"])
    open_interest = np.array([250.0, 250.0])
    mids = np.array([
        black_scholes_price(spot, 100.0, 7 / 365, 0.30, "C"),
        black_scholes_price(spot, 100.0, 7 / 365, 0.30, "P"),
    ])

    result = batch_iv_gamma_gex(
        spot,
        strikes,
        years,
        mids,
        option_types,
        open_interest,
    )

    assert result["valid_mask"].all()
    assert result["gamma"][0] == pytest.approx(result["gamma"][1], rel=1e-12)
    assert result["gex"][0] > 0.0
    assert result["gex"][1] < 0.0
    assert result["gex"][0] == pytest.approx(-result["gex"][1], rel=1e-12)


def test_batch_rejects_nonpositive_spot_and_mismatched_shapes():
    invalid_spot = batch_iv_gamma_gex(0.0, [100.0], [1 / 365], [1.0], ["C"], [1.0])
    assert invalid_spot["valid_mask"].tolist() == [False]
    assert invalid_spot["rejection_reason"].tolist() == ["invalid_spot"]
    assert np.isnan(invalid_spot["iv"]).all()

    with pytest.raises(ValueError, match="equal lengths"):
        batch_iv_gamma_gex(100.0, [100.0, 101.0], [1 / 365], [1.0], ["C"], [1.0])

    with pytest.raises(ValueError, match="one-dimensional"):
        batch_iv_gamma_gex(
            100.0,
            [[100.0]],
            [[1 / 365]],
            [[1.0]],
            [["C"]],
            [[1.0]],
        )
