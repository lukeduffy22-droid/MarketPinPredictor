"""Regression coverage for full-size PM-settled Russell closing signals."""
from datetime import date, timedelta

import pandas as pd

from backend import databento_streamer as stream


def contracts(root, day, market="RUT", open_interest=10):
    return [{
        "market": market, "symbol": stream.raw_option_symbol(root, day, side, strike),
        "expiration_date": day, "option_type": side, "strike": strike,
        "open_interest": open_interest,
    } for strike in range(2700, 2750, 5) for side in ("C", "P")]


def test_rut_uses_only_pm_expiry_for_live_selection_oi_and_parity(monkeypatch):
    day = date(2026, 11, 20)
    monkeypatch.setattr(stream, "current_market_date", lambda: day)
    frame = pd.DataFrame(contracts("RUT", day, open_interest=200_000)
                         + contracts("RUTW", day) + contracts("MRUT", day, market="MRUT"))
    streamer = stream.DatabentoGammaStreamer(["RUT", "MRUT"])
    streamer._apply_subscription_profile(frame, as_of=day)
    plan = streamer.subscription_metadata["markets"]["RUT"]
    assert plan["settlement_ineligible_expirations"] == [{
        "expiration": day.isoformat(), "contracts": 20, "reason": stream.RUT_SETTLEMENT_INELIGIBLE_REASON,
    }]
    assert plan["primary_pair_admission"]["complete_pair_count"] == 10
    assert plan["primary_expiration_same_day_authority"] is True
    assert set(streamer.universe.loc[streamer.universe.market == "RUT", "symbol"]) == {
        row["symbol"] for row in contracts("RUTW", day)
    }
    assert streamer.oi_analytics_by_market["RUT"]["expirations"][0]["total_open_interest"] == 200
    assert streamer._planned_primary_expiration("RUT") == (day, None)

    # A retained or legacy selected frame with mixed settlement cannot authorize a current pin.
    streamer.universe = frame
    assert streamer._planned_primary_expiration("RUT") == (None, stream.RUT_SETTLEMENT_INELIGIBLE_REASON)
    streamer.active_generation = 1
    streamer.handoff_status = "active"
    assert streamer._snapshot_opening_reference_inputs("RUT", trading_date=day) == (
        None, stream.RUT_SETTLEMENT_INELIGIBLE_REASON,
    )


def test_rut_future_series_cannot_replace_todays_pm_signal(monkeypatch):
    day = date(2026, 11, 20)
    monkeypatch.setattr(stream, "current_market_date", lambda: day)
    frame = pd.DataFrame(contracts("RUT", day) + contracts("RUTW", day + timedelta(days=3)))
    streamer = stream.DatabentoGammaStreamer(["RUT"])
    streamer._apply_subscription_profile(frame, as_of=day)
    plan = streamer.subscription_metadata["markets"]["RUT"]
    assert plan["primary_expiration_context_only"] is True
    assert plan["primary_expiration_same_day_authority"] is False
    expiry, error = streamer._planned_primary_expiration("RUT")
    assert expiry is None and error.startswith("PRIMARY_EXPIRATION_NOT_SAME_DAY")


def test_am_only_or_mini_rows_never_authorize_a_full_size_rut_close():
    day = date(2026, 11, 20)
    frame = pd.DataFrame(contracts("RUT", day) + contracts("MRUT", day) + contracts("IWM", day))
    streamer = stream.DatabentoGammaStreamer(["RUT"])
    streamer._apply_subscription_profile(frame, as_of=day)
    plan = streamer.subscription_metadata["markets"]["RUT"]
    assert plan["selected_contract_count"] == 0
    assert plan["primary_pair_admission"]["reason"] == stream.RUT_SETTLEMENT_INELIGIBLE_REASON
    assert plan["primary_expiration_unavailable_reason"] == stream.RUT_SETTLEMENT_INELIGIBLE_REASON
    assert not streamer.oi_analytics_by_market
