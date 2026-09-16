from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from backend.databento_streamer import (
    DatabentoGammaStreamer,
    DatabentoMarketConfig,
    raw_option_symbol,
    select_subscription_universe,
)


class _DefinitionResult:
    def __init__(self, frame):
        self._frame = frame

    def to_df(self):
        return self._frame


class _Timeseries:
    def __init__(self, frame):
        self.frame = frame
        self.calls = []

    def get_range(self, **kwargs):
        self.calls.append(dict(kwargs))
        return _DefinitionResult(self.frame)


class _Historical:
    def __init__(self, frame):
        self.timeseries = _Timeseries(frame)
        self.metadata = SimpleNamespace()


def test_definition_discovery_keeps_strikes_outside_legacy_bounds(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "symbol": "SPXW  260821C06800000",
                "expiration": "2026-08-21",
                "instrument_class": "C",
                "strike_price": 6800.0,
            },
            {
                "symbol": "SPXW  260821P06800000",
                "expiration": "2026-08-21",
                "instrument_class": "P",
                "strike_price": 6800.0,
            },
            {
                "symbol": "SPXW  270101C06800000",
                "expiration": "2027-01-01",
                "instrument_class": "C",
                "strike_price": 6800.0,
            },
            {
                "symbol": "SPXW  270101P06800000",
                "expiration": "2027-01-01",
                "instrument_class": "P",
                "strike_price": 6800.0,
            },
        ]
    )
    streamer = DatabentoGammaStreamer(["SPX"])
    config = DatabentoMarketConfig("SPX", "SPXW", 5, 7350, 7550)
    monkeypatch.setattr(streamer, "_available_end", lambda historical, schema: datetime(2026, 8, 22, tzinfo=timezone.utc))

    historical = _Historical(frame)
    result = streamer._fetch_definition_universe(
        historical,
        config,
        trading_date=date(2026, 8, 21),
        available_end=datetime(2026, 8, 22, tzinfo=timezone.utc),
    )

    assert len(result) == 2
    assert set(result["strike"]) == {6800.0}
    discovery = streamer.definition_discovery_metadata["SPX"]
    assert discovery["expiration_cutoff"] == "2026-10-05"
    assert discovery["contracts_dropped_outside_expiration_window"] == 2
    assert discovery["filter_stage"] == "before-open-interest-requests"
    assert discovery["query_mode"] == "historical-parent-unbounded"
    assert all("limit" not in call for call in historical.timeseries.calls)


def test_universe_refresh_due_uses_configured_interval(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer._universe_built_monotonic = 100.0
    monkeypatch.setattr("backend.databento_streamer.UNIVERSE_REFRESH_SECONDS", 10.0)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 111.0)
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    streamer._market_was_closed = False

    assert streamer._universe_refresh_due() is True


def test_pre_refresh_quotes_are_rejected(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.active_generation = 2
    streamer.subscription_cutoff_monotonic = 100.0
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 105.0)

    assert streamer._quote_is_current({"generation": 1, "received_monotonic": 104.0}) is False
    assert streamer._quote_is_current({"generation": 2, "received_monotonic": 99.0}) is False
    assert streamer._quote_is_current({"generation": 2, "received_monotonic": 104.0}) is True


def test_handoff_does_not_serve_previous_pin():
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.handoff_status = "warming"
    streamer.latest_pins["SPX"] = {"price": 100.0}

    assert streamer.get_latest_pin("SPX") is None


def test_gex_field_contract_is_symbol_agnostic():
    strike = {"gex": -5.0, "call_gex": 10.0, "put_gex": 15.0, "net_gex": -5.0, "total_gex": 25.0}

    assert strike["net_gex"] == strike["gex"]
    assert strike["total_gex"] == strike["call_gex"] + strike["put_gex"]
    assert strike["total_gex"] != abs(strike["net_gex"])


def _subscription_fixture() -> pd.DataFrame:
    rows = []
    expiries = {
        "SPX": [("2026-08-24", 100), ("2026-08-25", 10), ("2026-08-26", 20), ("2026-08-28", 5)],
        "NDX": [("2026-08-24", 100), ("2026-08-25", 10), ("2026-08-26", 20), ("2026-08-28", 5)],
        "VIX": [("2026-08-26", 100), ("2026-09-02", 5)],
    }
    roots = {"SPX": "SPXW", "NDX": "NDXP", "VIX": "VIXW"}
    strikes = {"SPX": 7600, "NDX": 29000, "VIX": 18}
    for market, expiration_values in expiries.items():
        for expiration_text, open_interest in expiration_values:
            expiration = date.fromisoformat(expiration_text)
            for option_type in ("C", "P"):
                rows.append({
                    "market": market,
                    "symbol": f"{roots[market]}-{expiration_text}-{option_type}",
                    "expiration_date": expiration,
                    "option_type": option_type,
                    "strike": strikes[market],
                    "open_interest": open_interest,
                })
    return pd.DataFrame(rows)


def test_near_term_subscription_keeps_complete_representative_expiries_and_interleaves_markets():
    selected, metadata = select_subscription_universe(
        _subscription_fixture(),
        ["SPX", "NDX", "VIX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 24),
    )

    expected_expiries = {
        "SPX": {date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 28)},
        "NDX": {date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 28)},
        "VIX": {date(2026, 8, 26), date(2026, 9, 2)},
    }
    for market, expirations in expected_expiries.items():
        market_rows = selected[selected["market"] == market]
        assert set(market_rows["expiration_date"]) == expirations
        assert market_rows.groupby("expiration_date")["option_type"].apply(set).eq({"C", "P"}).all()

    assert selected.head(3)["market"].tolist() == ["SPX", "NDX", "VIX"]
    assert metadata["full_contract_count"] == len(_subscription_fixture())
    assert metadata["selected_contract_count"] == len(selected)
    assert metadata["selected_universe_sha256"]
    assert metadata["markets"]["SPX"]["selected_expirations"][1]["role"] == "next-listed"


def test_subscription_selector_drops_expired_contracts():
    selected, metadata = select_subscription_universe(
        _subscription_fixture(),
        ["SPX", "NDX", "VIX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 25),
    )

    assert (selected["expiration_date"] >= date(2026, 8, 25)).all()
    assert metadata["expired_contract_count"] == 4


@pytest.mark.parametrize("profile", ["primary-only", "near-term-shadow", "full"])
def test_vix_same_day_settlement_is_excluded_and_next_expiry_is_forward_context(
    profile,
):
    trading_date = date(2026, 9, 9)
    rows = []
    for expiration in (
        trading_date,
        trading_date + timedelta(days=7),
        trading_date + timedelta(days=14),
    ):
        for option_type in ("C", "P"):
            rows.append(
                {
                    "market": "VIX",
                    "symbol": f"VIXW-{expiration.isoformat()}-{option_type}",
                    "expiration_date": expiration,
                    "option_type": option_type,
                    "strike": 18.0,
                    "open_interest": 100.0,
                }
            )

    selected, metadata = select_subscription_universe(
        pd.DataFrame(rows),
        ["VIX"],
        profile=profile,
        as_of=trading_date,
    )

    assert trading_date not in set(selected["expiration_date"])
    plan = metadata["markets"]["VIX"]["selected_expirations"]
    assert plan[0]["expiration"] == (trading_date + timedelta(days=7)).isoformat()
    assert plan[0]["stage"] == 0
    assert plan[0]["role"] == "primary"
    assert plan[0]["authority"] == "vix_forward_expiration_context_only"
    assert plan[0]["context_only"] is True
    assert plan[0]["same_day_authority"] is False
    assert plan[0]["selection_basis"] == (
        "vix_last_trading_day_precedes_settlement_date"
    )
    market = metadata["markets"]["VIX"]
    assert market["settlement_ineligible_contract_count"] == 2
    assert market["settlement_ineligible_expirations"] == [
        {
            "expiration": trading_date.isoformat(),
            "contracts": 2,
            "reason": "VIX_AM_SETTLED_LAST_TRADING_DAY_PASSED",
        }
    ]
    assert metadata["settlement_ineligible_contract_count"] == 2


def test_vix_with_only_same_day_settlement_fails_closed_without_index_error():
    trading_date = date(2026, 9, 9)
    rows = [
        {
            "market": "VIX",
            "symbol": f"VIXW-{trading_date.isoformat()}-{option_type}",
            "expiration_date": trading_date,
            "option_type": option_type,
            "strike": 18.0,
            "open_interest": 100.0,
        }
        for option_type in ("C", "P")
    ]

    selected, metadata = select_subscription_universe(
        pd.DataFrame(rows),
        ["VIX"],
        profile="near-term-shadow",
        as_of=trading_date,
    )

    assert selected.empty
    market = metadata["markets"]["VIX"]
    assert market["selected_expirations"] == []
    assert market["selected_contract_count"] == 0
    assert market["primary_expiration_unavailable_reason"] == (
        "NO_FORWARD_VIX_EXPIRATION_AVAILABLE"
    )
    assert market["primary_expiration_authority"] == (
        "vix_forward_expiration_context_only"
    )
    assert market["primary_expiration_context_only"] is True
    admission = DatabentoGammaStreamer(["VIX"])._primary_pair_admission_diagnostic(
        pd.DataFrame(rows),
        market="VIX",
        trading_date=trading_date,
    )
    assert admission["passes"] is False
    assert admission["reason"] == "NO_FORWARD_VIX_EXPIRATION_AVAILABLE"


def test_vix_primary_pair_admission_ignores_same_day_settlement_series():
    trading_date = date(2026, 9, 9)
    rows = []
    for expiration, strike_count in (
        (trading_date, 3),
        (trading_date + timedelta(days=7), 1),
    ):
        for strike_offset in range(strike_count):
            for option_type in ("C", "P"):
                strike = 18.0 + strike_offset
                rows.append(
                    {
                        "market": "VIX",
                        "symbol": (
                            f"VIXW-{expiration.isoformat()}-{strike:g}-{option_type}"
                        ),
                        "expiration_date": expiration,
                        "option_type": option_type,
                        "strike": strike,
                        "open_interest": 100.0,
                    }
                )

    diagnostic = DatabentoGammaStreamer(["VIX"])._primary_pair_admission_diagnostic(
        pd.DataFrame(rows),
        market="VIX",
        trading_date=trading_date,
    )

    assert diagnostic["passes"] is False
    assert diagnostic["reason"] == "PRIMARY_PAIR_COVERAGE_INCOMPLETE"
    assert diagnostic["minimum_pair_count"] >= 5
    assert diagnostic["primary_expiration"] == (
        trading_date + timedelta(days=7)
    ).isoformat()
    assert diagnostic["complete_pair_count"] == 1


def test_primary_in_later_bucket_is_not_duplicated_or_relabelled():
    rows = []
    for expiration in (date(2026, 8, 29), date(2026, 9, 2)):
        for option_type in ("C", "P"):
            rows.append({
                "market": "VIX",
                "symbol": f"VIXW-{expiration.isoformat()}-{option_type}",
                "expiration_date": expiration,
                "option_type": option_type,
                "strike": 18.0,
                "open_interest": 100.0,
            })

    selected, metadata = select_subscription_universe(
        pd.DataFrame(rows),
        ["VIX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 25),
    )

    plan = metadata["markets"]["VIX"]["selected_expirations"]
    assert [entry["expiration"] for entry in plan] == ["2026-08-29", "2026-09-02"]
    assert plan[0]["stage"] == 0 and plan[0]["role"] == "primary"
    assert selected["symbol"].is_unique


def test_prior_cache_fallback_is_filtered_auditable_and_read_only(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.ALLOW_PRIOR_UNIVERSE_FALLBACK", True)
    monkeypatch.setattr("backend.databento_streamer.UNIVERSE_FALLBACK_MAX_AGE_DAYS", 4)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.cache_dir = tmp_path
    cache_path = streamer._cache_path(date(2026, 8, 24))
    _subscription_fixture().to_csv(cache_path, index=False)
    original_bytes = cache_path.read_bytes()

    loaded = streamer._load_prior_cached_universe(
        trading_date=date(2026, 8, 25),
        reason="provider publication lag",
        provider_definition_end=datetime(2026, 8, 25, tzinfo=timezone.utc),
        provider_statistics_end=datetime(2026, 8, 25, tzinfo=timezone.utc),
    )

    assert loaded is True
    assert cache_path.read_bytes() == original_bytes
    assert (streamer.universe["expiration_date"] >= date(2026, 8, 25)).all()
    assert set(streamer.universe["market"]) == {"SPX", "NDX", "VIX"}
    provenance = streamer.subscription_metadata["universe_provenance"]
    assert provenance["mode"] == "prior_cache_filtered"
    assert provenance["is_fallback"] is True
    assert provenance["source_path"] == str(cache_path)
    assert provenance["source_sha256"]
    assert provenance["dropped_rows"] == 4


def test_prior_cache_fallback_allows_missing_optional_market(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.ALLOW_PRIOR_UNIVERSE_FALLBACK", True)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.cache_dir = tmp_path
    frame = _subscription_fixture()
    frame = frame[frame["market"] != "VIX"].copy()
    frame.to_csv(streamer._cache_path(date(2026, 8, 24)), index=False)

    assert streamer._load_prior_cached_universe(
        trading_date=date(2026, 8, 25),
        reason="provider publication lag",
    ) is True
    assert set(streamer.universe["market"]) == {"SPX", "NDX"}
    assert streamer.subscription_metadata["optional_family_unavailable_reasons"] == {
        "VIX": "NO_POSITIVE_OI_CONTRACTS"
    }


def test_prior_cache_fallback_rejects_incomplete_required_primary_coverage(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("backend.databento_streamer.ALLOW_PRIOR_UNIVERSE_FALLBACK", True)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.cache_dir = tmp_path
    frame = _subscription_fixture()
    frame = frame[
        ~(
            (frame["market"] == "NDX")
            & (frame["expiration_date"] == date(2026, 8, 25))
            & (frame["option_type"] == "P")
        )
    ].copy()
    frame.to_csv(streamer._cache_path(date(2026, 8, 24)), index=False)

    assert streamer._load_prior_cached_universe(
        trading_date=date(2026, 8, 25),
        reason="provider publication lag",
    ) is False


def test_build_universe_uses_fallback_without_zero_length_provider_requests(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.ALLOW_PRIOR_UNIVERSE_FALLBACK", True)
    streamer = DatabentoGammaStreamer(["SPX", "NDX", "VIX"])
    streamer.api_key = "unit-test-key"
    streamer.cache_dir = tmp_path
    _subscription_fixture().to_csv(streamer._cache_path(date(2026, 8, 24)), index=False)
    monkeypatch.setattr("backend.databento_streamer.current_market_date", lambda: date(2026, 8, 25))
    monkeypatch.setattr("backend.databento_streamer.db.Historical", lambda _key: object())
    monkeypatch.setattr(
        streamer,
        "_available_end",
        lambda _historical, _schema: datetime(2026, 8, 25, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        streamer,
        "_fetch_definition_universe",
        lambda *_args, **_kwargs: pytest.fail("zero-length definition request was made"),
    )
    monkeypatch.setattr(
        streamer,
        "_fetch_open_interest",
        lambda *_args, **_kwargs: pytest.fail("zero-length statistics request was made"),
    )

    streamer._build_universe()

    assert streamer.subscription_metadata["universe_provenance"]["is_fallback"] is True


def test_refresh_interval_returns_to_normal_after_current_day_discovery(monkeypatch):
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer._universe_built_monotonic = 100.0
    monkeypatch.setattr("backend.databento_streamer.UNIVERSE_FALLBACK_REFRESH_SECONDS", 5.0)
    monkeypatch.setattr("backend.databento_streamer.UNIVERSE_REFRESH_SECONDS", 20.0)
    monkeypatch.setattr("backend.databento_streamer.time.monotonic", lambda: 110.0)
    monkeypatch.setattr("backend.databento_streamer.market_is_closed", lambda: False)
    streamer._market_was_closed = False
    streamer.subscription_metadata["universe_provenance"] = {"is_fallback": True}
    assert streamer._universe_refresh_due() is True

    streamer.subscription_metadata["universe_provenance"] = {"is_fallback": False}
    assert streamer._universe_refresh_due() is False


def test_full_subscription_profile_keeps_every_unique_contract():
    fixture = _subscription_fixture()
    selected, metadata = select_subscription_universe(
        fixture,
        ["SPX", "NDX", "VIX"],
        profile="full",
        as_of=date(2026, 8, 24),
    )

    assert len(selected) == len(fixture)
    assert selected["symbol"].is_unique
    assert metadata["selected_contract_count"] == len(fixture)


def _many_pair_universe(markets=("SPX",), pair_count=8) -> pd.DataFrame:
    rows = []
    roots = {"SPX": "SPXW", "NDX": "NDXP", "VIX": "VIXW"}
    for market in markets:
        expiry_offsets = (1, 2) if market == "VIX" else (0, 1)
        for expiry_offset in expiry_offsets:
            expiration = date(2026, 8, 26) + timedelta(days=expiry_offset)
            for pair_index in range(pair_count):
                strike = 50_000.0 + pair_index * 25.0
                for option_type in ("C", "P"):
                    rows.append({
                        "market": market,
                        "symbol": f"{roots[market]}-{expiration}-{strike:g}-{option_type}",
                        "expiration_date": expiration,
                        "option_type": option_type,
                        "strike": strike,
                        "open_interest": float(1_000 - pair_index * 10),
                    })
    return pd.DataFrame(rows)


def test_subscription_caps_are_dynamic_auditable_and_pair_complete(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.PRIMARY_MAX_STRIKE_PAIRS", 3)
    monkeypatch.setattr("backend.databento_streamer.SHADOW_MAX_STRIKE_PAIRS", 2)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 2)
    monkeypatch.setattr("backend.databento_streamer.MIN_NEXT_LISTED_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.MAX_CONTRACTS_PER_MARKET", 10)
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 10)

    selected, metadata = select_subscription_universe(
        _many_pair_universe(("NDX",)),
        ["NDX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 26),
    )

    assert len(selected) == 10
    assert selected["strike"].min() >= 50_000.0  # no legacy/static NDX strike bound
    coverage = selected.groupby(["expiration_date", "strike"])["option_type"].apply(set)
    assert coverage.eq({"C", "P"}).all()
    plan = metadata["markets"]["NDX"]["selected_expirations"]
    assert [(entry["role"], entry["selected_strike_pairs"]) for entry in plan] == [
        ("primary", 3),
        ("next-listed", 2),
    ]
    assert metadata["bounds"]["pair_selection_method"].startswith("complete-pairs")


def test_fifty_pair_shadow_bound_preserves_primary_and_required_next_reservation(
    monkeypatch,
):
    monkeypatch.setattr("backend.databento_streamer.PRIMARY_MAX_STRIKE_PAIRS", 600)
    monkeypatch.setattr("backend.databento_streamer.SHADOW_MAX_STRIKE_PAIRS", 50)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 100)
    monkeypatch.setattr("backend.databento_streamer.MIN_NEXT_LISTED_STRIKE_PAIRS", 50)
    monkeypatch.setattr("backend.databento_streamer.MAX_CONTRACTS_PER_MARKET", 2_000)
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 3_600)

    selected, metadata = select_subscription_universe(
        _many_pair_universe(("SPX",), pair_count=80),
        ["SPX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 26),
    )

    plan = metadata["markets"]["SPX"]["selected_expirations"]
    assert [(entry["role"], entry["selected_strike_pairs"]) for entry in plan] == [
        ("primary", 80),
        ("next-listed", 50),
    ]
    assert metadata["markets"]["SPX"]["primary_reserved_pairs_retained"] == 80
    assert metadata["markets"]["SPX"]["next_listed_reserved_pairs_retained"] == 50
    assert metadata["reservation_shortfall_pairs"] == 0
    assert metadata["global_cap_applied"] is False
    coverage = selected.groupby(["expiration_date", "strike"])["option_type"].apply(set)
    assert coverage.eq({"C", "P"}).all()


def test_global_cap_reserves_primary_and_next_listed_pairs_for_every_market(monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.PRIMARY_MAX_STRIKE_PAIRS", 4)
    monkeypatch.setattr("backend.databento_streamer.SHADOW_MAX_STRIKE_PAIRS", 3)
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 2)
    monkeypatch.setattr("backend.databento_streamer.MIN_NEXT_LISTED_STRIKE_PAIRS", 1)
    monkeypatch.setattr("backend.databento_streamer.MAX_CONTRACTS_PER_MARKET", 20)
    monkeypatch.setattr("backend.databento_streamer.MAX_SUBSCRIPTION_CONTRACTS", 18)

    selected, metadata = select_subscription_universe(
        _many_pair_universe(("SPX", "NDX", "VIX")),
        ["SPX", "NDX", "VIX"],
        profile="near-term-shadow",
        as_of=date(2026, 8, 26),
    )

    assert len(selected) == 18
    assert metadata["global_cap_applied"] is True
    assert metadata["reservation_shortfall_pairs"] == 0
    for market in ("SPX", "NDX", "VIX"):
        plan = metadata["markets"][market]
        assert plan["primary_reserved_pairs_retained"] == 2
        assert plan["next_listed_reserved_pairs_retained"] == 1
        market_rows = selected[selected["market"] == market]
        coverage = market_rows.groupby(["expiration_date", "strike"])["option_type"].apply(set)
        assert coverage.eq({"C", "P"}).all()


def test_current_day_universe_can_be_staged_atomically_without_live_handoff(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 8, 26)
    current_call = "SPXW  260826C05000000"
    current_put = "SPXW  260826P05000000"
    far_call = "SPXW  270101C05000000"
    far_put = "SPXW  270101P05000000"
    definitions = pd.DataFrame([
        {"symbol": current_call, "expiration": "2026-08-26", "instrument_class": "C", "strike_price": 5000.0},
        {"symbol": current_put, "expiration": "2026-08-26", "instrument_class": "P", "strike_price": 5000.0},
        {"symbol": far_call, "expiration": "2027-01-01", "instrument_class": "C", "strike_price": 5000.0},
        {"symbol": far_put, "expiration": "2027-01-01", "instrument_class": "P", "strike_price": 5000.0},
    ])
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "unit-test-key"
    streamer.cache_dir = tmp_path
    streamer.live_symbols = ["existing-live-symbol"]
    streamer.active_generation = 7
    streamer._universe_built_monotonic = 123.0
    captured_oi_symbols = []
    historical = _Historical(definitions)
    monkeypatch.setattr("backend.databento_streamer.db.Historical", lambda _key: historical)
    monkeypatch.setattr(
        streamer,
        "_available_end",
        lambda _historical, _schema: datetime(2026, 8, 27, tzinfo=timezone.utc),
    )

    def fake_open_interest(_historical, _config, symbols, **_kwargs):
        captured_oi_symbols.extend(symbols)
        return pd.DataFrame({"symbol": symbols, "open_interest": [100.0] * len(symbols)})

    monkeypatch.setattr(streamer, "_fetch_parent_open_interest", fake_open_interest)

    provenance = streamer.stage_current_day_universe_cache(trading_date=trading_date)

    assert set(captured_oi_symbols) == {current_call, current_put}
    assert far_call not in captured_oi_symbols and far_put not in captured_oi_symbols
    assert provenance["mode"] == "databento_discovery_staged"
    assert provenance["applied_to_live_subscription"] is False
    assert provenance["provider_definition_end"]
    assert provenance["provider_statistics_end"]
    assert provenance["source_sha256"]
    assert Path(provenance["source_path"]).exists()
    assert streamer.live_symbols == ["existing-live-symbol"]
    assert streamer.active_generation == 7
    assert streamer._universe_built_monotonic == 123.0
    assert not list(tmp_path.glob("*.tmp"))


def test_bounded_stager_reuses_completed_market_fragment_before_atomic_publish(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 9, 8)
    request_end = datetime(2026, 9, 9, tzinfo=timezone.utc)

    def family_frame(market, root, strike):
        return pd.DataFrame(
            [
                {
                    "market": market,
                    "symbol": raw_option_symbol(root, trading_date, option_type, strike),
                    "expiration_date": trading_date,
                    "option_type": option_type,
                    "strike": strike,
                }
                for option_type in ("C", "P")
            ]
        )

    first = DatabentoGammaStreamer(["SPX", "NDX"])
    first.cache_dir = tmp_path

    def interrupted_definition(_historical, config, **_kwargs):
        if config.label == "NDX":
            raise KeyboardInterrupt("bounded preparer was terminated")
        first.definition_discovery_metadata[config.label] = {
            "trading_date": trading_date.isoformat(),
            "status": "ready",
        }
        return family_frame("SPX", "SPXW", 7600.0)

    monkeypatch.setattr(first, "_fetch_definition_universe", interrupted_definition)
    monkeypatch.setattr(
        first,
        "_fetch_parent_open_interest",
        lambda _historical, _config, symbols, **_kwargs: pd.DataFrame(
            {"symbol": symbols, "open_interest": [100.0] * len(symbols)}
        ),
    )

    with pytest.raises(KeyboardInterrupt, match="bounded preparer"):
        first._stage_provider_universe_cache(
            historical=object(),
            source_date=trading_date,
            admission_date=trading_date,
            definition_end=request_end,
            statistics_end=request_end,
            is_fallback=False,
        )

    assert first._staged_market_cache_path(trading_date, "SPX").exists()
    assert not first._cache_path(trading_date).exists()

    second = DatabentoGammaStreamer(["SPX", "NDX"])
    second.cache_dir = tmp_path
    fetched_markets = []

    def resumed_definition(_historical, config, **_kwargs):
        fetched_markets.append(config.label)
        if config.label == "SPX":
            pytest.fail("completed SPX family was fetched again")
        second.definition_discovery_metadata[config.label] = {
            "trading_date": trading_date.isoformat(),
            "status": "ready",
        }
        return family_frame("NDX", "NDXP", 25000.0)

    monkeypatch.setattr(second, "_fetch_definition_universe", resumed_definition)
    monkeypatch.setattr(
        second,
        "_fetch_parent_open_interest",
        lambda _historical, _config, symbols, **_kwargs: pd.DataFrame(
            {"symbol": symbols, "open_interest": [100.0] * len(symbols)}
        ),
    )

    provenance = second._stage_provider_universe_cache(
        historical=object(),
        source_date=trading_date,
        admission_date=trading_date,
        definition_end=request_end,
        statistics_end=request_end,
        is_fallback=False,
    )

    assert fetched_markets == ["NDX"]
    assert provenance["mode"] == "databento_discovery_staged"
    assert provenance["is_fallback"] is False
    assert second.definition_discovery_metadata["SPX"]["status"] == (
        "reused_staged_fragment"
    )
    aggregate = pd.read_csv(second._cache_path(trading_date))
    assert set(aggregate["market"]) == {"SPX", "NDX"}
    assert not set(aggregate.columns).intersection(
        {"_stage_fragment_version", "_stage_source_date", "_stage_market"}
    )
    assert not list(tmp_path.glob("*.tmp"))


def test_provider_prior_session_cache_uses_exact_source_day_and_stays_fallback(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    target_date = date(2026, 8, 26)
    source_date = date(2026, 8, 25)
    current_call = "SPXW  260826C05000000"
    current_put = "SPXW  260826P05000000"
    definitions = pd.DataFrame([
        {
            "symbol": current_call,
            "expiration": "2026-08-26",
            "instrument_class": "C",
            "strike_price": 5000.0,
        },
        {
            "symbol": current_put,
            "expiration": "2026-08-26",
            "instrument_class": "P",
            "strike_price": 5000.0,
        },
    ])
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.api_key = "unit-test-key"
    streamer.cache_dir = tmp_path
    historical = _Historical(definitions)
    monkeypatch.setattr("backend.databento_streamer.db.Historical", lambda _key: historical)
    monkeypatch.setattr(
        streamer,
        "_available_end",
        lambda _historical, _schema: datetime(2026, 8, 26, 7, 10, tzinfo=timezone.utc),
    )

    def fake_open_interest(_historical, _config, symbols, **_kwargs):
        assert _kwargs["trading_date"] == source_date
        assert _kwargs["available_end"] == datetime(2026, 8, 26, tzinfo=timezone.utc)
        return pd.DataFrame({"symbol": symbols, "open_interest": [100.0] * len(symbols)})

    monkeypatch.setattr(streamer, "_fetch_parent_open_interest", fake_open_interest)

    provenance = streamer.stage_prior_session_universe_cache(trading_date=target_date)

    assert provenance["mode"] == "databento_prior_session_discovery_staged"
    assert provenance["trading_date"] == target_date.isoformat()
    assert provenance["source_date"] == source_date.isoformat()
    assert provenance["is_fallback"] is True
    assert provenance["applied_to_live_subscription"] is False
    assert Path(provenance["source_path"]).name.startswith("opra_universe_2026-08-25_")
    assert Path(provenance["source_path"]).exists()
    assert all(call["start"] == source_date.isoformat() for call in historical.timeseries.calls)
    assert all(
        call["end"] == datetime(2026, 8, 26, tzinfo=timezone.utc)
        for call in historical.timeseries.calls
    )
    assert all("limit" not in call for call in historical.timeseries.calls)


def test_staged_open_interest_uses_bounded_parent_statistics_requests():
    candidate_call = "SPXW  260826C05000000"
    candidate_put = "SPXW  260826P05000000"
    unrelated = "SPXW  260826C06000000"
    statistics = pd.DataFrame([
        {"symbol": candidate_call, "stat_type": 9, "quantity": 100.0},
        {"symbol": candidate_put, "stat_type": 9, "quantity": 90.0},
        {"symbol": unrelated, "stat_type": 9, "quantity": 1_000.0},
        {"symbol": candidate_call, "stat_type": 1, "quantity": 9_999.0},
    ])
    historical = _Historical(statistics)
    streamer = DatabentoGammaStreamer(["SPX"])
    config = DatabentoMarketConfig("SPX", "SPXW", 5, 4900, 5100)
    request_end = datetime(2026, 8, 26, tzinfo=timezone.utc)

    result = streamer._fetch_parent_open_interest(
        historical,
        config,
        [candidate_call, candidate_put],
        trading_date=date(2026, 8, 25),
        available_end=request_end,
    )

    assert set(result["symbol"]) == {candidate_call, candidate_put}
    assert dict(zip(result["symbol"], result["open_interest"])) == {
        candidate_call: 100.0,
        candidate_put: 90.0,
    }
    assert len(historical.timeseries.calls) == 2
    assert {call["symbols"] for call in historical.timeseries.calls} == {
        "SPXW.OPT",
        "SPX.OPT",
    }
    for call in historical.timeseries.calls:
        assert call["dataset"] == "OPRA.PILLAR"
        assert call["schema"] == "statistics"
        assert call["stype_in"] == "parent"
        assert call["start"] == "2026-08-25"
        assert call["end"] == request_end
        assert "limit" not in call


def test_current_day_cache_rejects_calls_and_puts_on_different_primary_strikes(tmp_path):
    trading_date = date(2026, 8, 26)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.cache_dir = tmp_path
    pd.DataFrame([
        {
            "market": "SPX", "symbol": "SPXW-call", "expiration_date": trading_date,
            "option_type": "C", "strike": 5000.0, "open_interest": 100.0,
        },
        {
            "market": "SPX", "symbol": "SPXW-put", "expiration_date": trading_date,
            "option_type": "P", "strike": 5005.0, "open_interest": 100.0,
        },
    ]).to_csv(streamer._cache_path(trading_date), index=False)

    assert streamer._load_cached_universe(trading_date) is False


def test_subscription_never_cross_pairs_different_option_roots():
    trading_date = date(2026, 8, 26)
    frame = pd.DataFrame([
        {
            "market": "SPX", "symbol": "SPXW  260826C05000000",
            "expiration_date": trading_date, "option_type": "C", "strike": 5000.0,
            "open_interest": 100.0,
        },
        {
            "market": "SPX", "symbol": "SPX   260826P05000000",
            "expiration_date": trading_date, "option_type": "P", "strike": 5000.0,
            "open_interest": 100.0,
        },
    ])

    selected, metadata = select_subscription_universe(
        frame, ["SPX"], profile="primary-only", as_of=trading_date
    )

    assert selected.empty
    assert metadata["selected_contract_count"] == 0


def test_valid_current_day_cache_remains_usable_when_refresh_is_recommended(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 8, 26)
    streamer = DatabentoGammaStreamer(["SPX"])
    streamer.cache_dir = tmp_path
    cache_path = streamer._cache_path(trading_date)
    pd.DataFrame([
        {
            "market": "SPX", "symbol": "SPXW  260826C05000000",
            "expiration_date": trading_date, "option_type": "C", "strike": 5000.0,
            "open_interest": 100.0,
        },
        {
            "market": "SPX", "symbol": "SPXW  260826P05000000",
            "expiration_date": trading_date, "option_type": "P", "strike": 5000.0,
            "open_interest": 100.0,
        },
    ]).to_csv(cache_path, index=False)
    monkeypatch.setattr("backend.databento_streamer.UNIVERSE_REFRESH_SECONDS", 10.0)
    monkeypatch.setattr(
        "backend.databento_streamer.time.time", lambda: cache_path.stat().st_mtime + 60.0
    )

    assert streamer._load_cached_universe(trading_date) is True
    provenance = streamer.subscription_metadata["universe_provenance"]
    assert provenance["mode"] == "current_day_cache"
    assert provenance["refresh_recommended"] is True


def test_current_day_cache_restores_hash_bound_provider_cutoffs(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 8, 26)
    definition_end = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
    statistics_end = datetime(2026, 8, 26, 11, 0, tzinfo=timezone.utc)
    frame = pd.DataFrame([
        {
            "market": "SPX", "symbol": "SPXW  260826C05000000",
            "expiration_date": trading_date, "option_type": "C", "strike": 5000.0,
            "open_interest": 100.0,
        },
        {
            "market": "SPX", "symbol": "SPXW  260826P05000000",
            "expiration_date": trading_date, "option_type": "P", "strike": 5000.0,
            "open_interest": 100.0,
        },
    ])
    writer = DatabentoGammaStreamer(["SPX"])
    writer.cache_dir = tmp_path
    cache_path = writer._save_cached_universe(
        frame,
        trading_date=trading_date,
        provider_definition_end=definition_end,
        provider_statistics_end=statistics_end,
    )
    assert cache_path is not None

    reader = DatabentoGammaStreamer(["SPX"])
    reader.cache_dir = tmp_path
    assert reader._load_cached_universe(trading_date) is True

    provenance = reader.subscription_metadata["universe_provenance"]
    assert provenance["cache_metadata_status"] == "verified"
    assert provenance["provider_definition_end"] == definition_end.isoformat()
    assert provenance["provider_statistics_end"] == statistics_end.isoformat()


def test_current_day_cache_does_not_trust_tampered_cutoff_sidecar(tmp_path, monkeypatch):
    monkeypatch.setattr("backend.databento_streamer.MIN_PRIMARY_STRIKE_PAIRS", 1)
    trading_date = date(2026, 8, 26)
    frame = pd.DataFrame([
        {
            "market": "SPX", "symbol": "SPXW  260826C05000000",
            "expiration_date": trading_date, "option_type": "C", "strike": 5000.0,
            "open_interest": 100.0,
        },
        {
            "market": "SPX", "symbol": "SPXW  260826P05000000",
            "expiration_date": trading_date, "option_type": "P", "strike": 5000.0,
            "open_interest": 100.0,
        },
    ])
    writer = DatabentoGammaStreamer(["SPX"])
    writer.cache_dir = tmp_path
    cache_path = writer._save_cached_universe(
        frame,
        trading_date=trading_date,
        provider_definition_end=datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc),
        provider_statistics_end=datetime(2026, 8, 26, 11, 0, tzinfo=timezone.utc),
    )
    assert cache_path is not None
    writer._cache_metadata_path(cache_path).write_text("{}", encoding="utf-8")

    reader = DatabentoGammaStreamer(["SPX"])
    reader.cache_dir = tmp_path
    assert reader._load_cached_universe(trading_date) is True

    provenance = reader.subscription_metadata["universe_provenance"]
    assert provenance["cache_metadata_status"] == "missing_or_invalid"
    assert provenance["provider_statistics_end"] is None
