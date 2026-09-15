from app.services.databento_symbol_catalog import (
    CANARY_CANDIDATE_SYMBOLS,
    active_canary_symbols,
    selectable_databento_symbols,
)
from backend.config import DATABENTO_SUPPORTED_SYMBOLS
from backend.databento_streamer import MARKETS


def test_symbol_catalog_keeps_stable_set_when_backend_is_unavailable():
    assert selectable_databento_symbols({}) == ("SPX", "NDX", "VIX")
    assert active_canary_symbols({}) == ()


def test_spy_and_qqq_are_supported_end_to_end_catalog_entries():
    for symbol in ("SPY", "QQQ", "DIA", "IWM"):
        assert symbol in CANARY_CANDIDATE_SYMBOLS
        assert symbol in DATABENTO_SUPPORTED_SYMBOLS
        assert MARKETS[symbol].daily_root == symbol


def test_symbol_catalog_exposes_only_canaries_with_selected_contracts():
    payload = {
        "market_subscription_status": {
            "RUT": {"selected_contract_count": 240},
            "SPY": {"selected_contract_count": 320},
            "QQQ": {"selected_contract_count": 280},
            "XSP": {"selected_contract_count": 0},
            "NOT_SUPPORTED": {"selected_contract_count": 999},
        }
    }

    assert selectable_databento_symbols(payload) == (
        "SPX", "NDX", "VIX", "SPY", "QQQ", "RUT"
    )
    assert active_canary_symbols(payload) == ("SPY", "QQQ", "RUT")


def test_symbol_catalog_fails_closed_on_malformed_contract_counts():
    payload = {
        "market_subscription_status": {
            "RUT": {"selected_contract_count": "not-a-count"},
            "XSP": None,
        }
    }

    assert selectable_databento_symbols(payload) == ("SPX", "NDX", "VIX")
