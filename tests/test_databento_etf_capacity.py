from datetime import date, timedelta

import pandas as pd

from backend.databento_streamer import MARKETS, raw_option_symbol, select_subscription_universe


def _universe(symbols):
    rows = []
    day = date(2026, 9, 10)
    for symbol in symbols:
        for offset in (0, 1, 7):
            for strike in range(100, 160):
                for cp in ("C", "P"):
                    rows.append({"market": symbol, "symbol": raw_option_symbol(MARKETS[symbol].daily_root, day + timedelta(days=offset), cp, strike),
                                 "expiration_date": day + timedelta(days=offset), "option_type": cp,
                                 "strike": strike, "open_interest": 100})
    return pd.DataFrame(rows)


def test_etf_addition_preserves_existing_core_allocation_even_when_requested_first(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 400)
    monkeypatch.setattr("backend.databento_streamer.MAX_CONTRACTS_PER_MARKET", 400)
    core = ["SPX", "NDX", "VIX"]
    extra = ["SPY", "QQQ", "DIA", "IWM"]
    base, _ = select_subscription_universe(_universe(core), core, as_of=date(2026, 9, 10))
    expanded, metadata = select_subscription_universe(_universe(core + extra), extra + core, as_of=date(2026, 9, 10))
    assert set(expanded.loc[expanded["market"].isin(core), "symbol"]) == set(base["symbol"])
    assert len(expanded) <= 400
    assert metadata["selected_contract_count"] == len(expanded)
    assert all(set(group["option_type"]) == {"C", "P"}
               for _, group in expanded.groupby(["market", "expiration_date", "strike"]))


def test_etfs_use_residual_budget_and_have_independent_raw_option_roots(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 1000)
    symbols = ["SPX", "NDX", "DIA", "IWM"]
    frame = _universe(symbols).query("strike < 110")
    selected, metadata = select_subscription_universe(frame, symbols, as_of=date(2026, 9, 10))
    assert {"DIA", "IWM"}.issubset(set(selected["market"]))
    assert "DJI" not in MARKETS
    assert metadata["selected_contract_count"] <= 1000
