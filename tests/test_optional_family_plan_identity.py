from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import date, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

import backend.databento_streamer as module


DAY = date(2026, 10, 7)


@pytest.fixture
def full_stage(monkeypatch, tmp_path):
    monkeypatch.setenv("DATABENTO_RUT_CANARY_ENABLED", "1")
    monkeypatch.setattr(module, "DATA_DIR", tmp_path)
    monkeypatch.setattr(module, "current_market_date", lambda: DAY)
    monkeypatch.setattr(module, "MIN_PRIMARY_STRIKE_PAIRS", 1)
    monkeypatch.setattr(module, "MIN_NEXT_LISTED_STRIKE_PAIRS", 1)
    monkeypatch.setattr(module, "MIN_PAIRED_QUOTES", 1)
    monkeypatch.setattr(module, "MAX_SUBSCRIPTION_CONTRACTS", 48)
    rows = []
    for family, root, strike in (
        ("SPX", "SPXW", 7700), ("NDX", "NDXP", 31000),
        ("VIX", "VIXW", 20), ("RUT", "RUTW", 2800),
    ):
        primary = DAY + timedelta(days=7) if family == "VIX" else DAY
        for expiry in (primary, primary + timedelta(days=1)):
            for offset in range(6):
                for side in ("C", "P"):
                    rows.append({
                        "market": family,
                        "symbol": module.raw_option_symbol(root, expiry, side, strike + offset),
                        "expiration_date": expiry, "strike": float(strike + offset),
                        "option_type": side, "open_interest": 100.0,
                    })
    streamer = module.DatabentoGammaStreamer(["SPX", "NDX", "VIX", "RUT"])
    streamer.subscription_profile = "near-term-shadow"
    streamer._apply_subscription_profile(
        pd.DataFrame(rows), as_of=DAY,
        provenance={"mode": "test", "trading_date": DAY.isoformat(),
                    "source_date": DAY.isoformat(), "source_sha256": "f" * 64,
                    "is_fallback": False},
    )
    streamer._prepare_live_subscription_stages()
    streamer.active_generation = 7
    streamer._activate_initial_subscription_stage(7)
    # Exercise the state reached after the additive request succeeds.
    streamer.subscription_stage_state = "full_active"
    streamer.subscription_stage_active = "full"
    streamer.active_live_symbols = list(streamer.live_symbols)
    streamer.deferred_live_symbols = []
    streamer.handoff_status = "active"
    stopped = []
    streamer.client = SimpleNamespace(stop=lambda: stopped.append(True))
    return streamer, stopped


def rollback(streamer):
    return streamer._apply_optional_family_canary_rollback(
        SimpleNamespace(rollback_required=True, reasons=("CORE_FRESH_QUOTE_COVERAGE:SPX",))
    )


def test_rollback_preserves_retained_plan_and_reconnects_offline(full_stage, monkeypatch):
    streamer, stopped = full_stage
    metadata_before = streamer.subscription_metadata
    preserved_metadata = deepcopy(metadata_before)
    source_before = streamer.full_universe.copy()
    expected_frame = streamer.universe[streamer.universe["market"] != "RUT"].copy()
    expected_symbols = expected_frame["symbol"].tolist()
    epoch = streamer.subscription_epoch_id
    generation = streamer.active_generation
    assert metadata_before["global_cap_applied"]

    assert rollback(streamer)
    assert stopped == [True]
    assert streamer.handoff_status == "handoff"
    assert streamer.live_symbols == expected_symbols
    pd.testing.assert_frame_equal(streamer.universe, expected_frame)
    pd.testing.assert_frame_equal(streamer.full_universe, source_before)
    assert metadata_before == preserved_metadata
    assert streamer.subscription_metadata["universe_provenance"] == metadata_before["universe_provenance"]
    assert streamer.subscription_metadata["full_contract_count"] == metadata_before["full_contract_count"]
    assert streamer.subscription_metadata["selected_contract_count"] == len(expected_symbols)
    retained_hash = hashlib.sha256("\n".join(expected_symbols).encode()).hexdigest()
    assert streamer.subscription_metadata["selected_universe_sha256"] == retained_hash
    assert retained_hash != metadata_before["selected_universe_sha256"]
    assert streamer.optional_family_rollback_evidence["previous_selected_universe_sha256"] == metadata_before["selected_universe_sha256"]
    assert streamer.optional_family_rollback_evidence["remaining_selected_universe_sha256"] == retained_hash
    for name in ("markets", "primary_pair_admission", "definition_discovery"):
        assert "RUT" not in streamer.subscription_metadata[name]
    for family in ("SPX", "NDX", "VIX"):
        assert streamer.subscription_metadata["markets"][family] == metadata_before["markets"][family]
    assert streamer.subscription_metadata["reservation_pairs_requested"] == 6
    assert streamer.subscription_metadata["reservation_pairs_retained"] == 6
    assert streamer.subscription_metadata["reservation_shortfall_pairs"] == 0
    assert streamer.active_generation == generation
    assert streamer.subscription_epoch_id == epoch

    initial = streamer._prepare_live_subscription_stages()
    assert streamer.subscription_stage_selected_universe_sha256 == retained_hash
    assert set(initial) | set(streamer.deferred_live_symbols) == set(expected_symbols)
    assert set(streamer.subscription_stage_primary_counts) == {"SPX", "NDX", "VIX"}
    assert streamer.active_generation == generation

    calls = []
    class FakeLive:
        def __init__(self, **kwargs):
            pass

        def subscribe(self, **kwargs):
            calls.append(kwargs)

        def __iter__(self):
            streamer.is_running = False
            return iter(())

        def stop(self):
            pass

        def block_for_close(self, timeout):
            pass

    monkeypatch.setattr(module.db, "Live", FakeLive)
    monkeypatch.setattr(streamer, "_build_universe", lambda **kwargs: None)
    monkeypatch.setattr(streamer, "_subscription_window", lambda: {"state": "regular_session", "subscription_allowed": True})
    streamer.api_key = "offline-test-key"
    streamer.replay_minutes = 0
    streamer.is_running = True
    streamer._run_live()
    assert len(calls) == 1
    assert calls[0]["symbols"] == initial
    assert "start" not in calls[0]
    assert streamer.live_symbols == expected_symbols
    assert streamer.active_generation == generation + 1
    assert streamer.subscription_epoch_id == epoch
    assert streamer.subscription_metadata["selected_universe_sha256"] == retained_hash


def test_rollback_does_not_repair_preexisting_corrupt_hash(full_stage):
    streamer, stopped = full_stage
    streamer.subscription_metadata["selected_universe_sha256"] = "0" * 64
    original_symbols = list(streamer.live_symbols)
    original_frame = streamer.universe.copy()
    with pytest.raises(RuntimeError, match="FULL_SELECTED_UNIVERSE_HASH_MISMATCH"):
        rollback(streamer)
    assert streamer.live_symbols == original_symbols
    pd.testing.assert_frame_equal(streamer.universe, original_frame)
    assert streamer.symbols == ["SPX", "NDX", "VIX", "RUT"]
    assert stopped == []


def test_tampering_after_rollback_still_blocks_subscription(full_stage):
    streamer, _ = full_stage
    assert rollback(streamer)
    streamer.subscription_metadata["selected_universe_sha256"] = "0" * 64
    with pytest.raises(RuntimeError, match="FULL_SELECTED_UNIVERSE_HASH_MISMATCH"):
        streamer._prepare_live_subscription_stages()
    assert streamer.subscription_stage_state == "blocked"
    assert streamer.active_live_symbols == []


@pytest.mark.parametrize("missing_frame", [True, False])
def test_rollback_requires_frame_to_match_prior_identity(full_stage, missing_frame):
    streamer, stopped = full_stage
    metadata = deepcopy(streamer.subscription_metadata)
    symbols = list(streamer.live_symbols)
    streamer.universe = streamer.universe.iloc[0:0 if missing_frame else -1].copy()
    reason = "FRAME_MISSING" if missing_frame else "SYMBOL_SET_MISMATCH"
    with pytest.raises(RuntimeError, match=reason):
        rollback(streamer)
    assert streamer.subscription_metadata == metadata
    assert streamer.live_symbols == symbols
    assert stopped == []


@pytest.mark.parametrize("corruption", ["strike", "market_index", "duplicate", "row_order"])
def test_rollback_validates_candidate_before_mutating_shared_state(full_stage, corruption):
    streamer, stopped = full_stage
    if corruption == "strike":
        streamer.universe["strike"] = streamer.universe["strike"].astype(object)
        index = streamer.universe.index[streamer.universe["market"] == "SPX"][0]
        streamer.universe.at[index, "strike"] = "invalid"
    elif corruption == "market_index":
        index = streamer.universe.index[streamer.universe["market"] == "RUT"][0]
        # The old symbol index still says RUT: these candidate views disagree.
        streamer.universe.at[index, "market"] = "SPX"
    elif corruption == "duplicate":
        streamer.universe = pd.concat([streamer.universe, streamer.universe.iloc[:1]])
    else:
        streamer.universe = streamer.universe.iloc[::-1].copy()
    field_names = (
        "symbols", "live_symbols", "universe", "subscription_metadata",
        "_universe_records_by_market", "_paired_strikes_by_market_expiration",
        "_symbol_to_market",
    )
    old_fields = {name: getattr(streamer, name) for name in field_names}
    with pytest.raises((ValueError, RuntimeError)):
        rollback(streamer)
    assert all(getattr(streamer, name) is value for name, value in old_fields.items())
    assert streamer.handoff_status == "active"
    assert streamer.optional_family_rollback_evidence is None
    assert stopped == []


def test_core_rollback_overrides_primary_stage_protection(full_stage):
    streamer, stopped = full_stage
    streamer.subscription_stage_state = "primary_active"
    assert rollback(streamer) is True
    assert "RUT" not in streamer.symbols
    assert all(
        streamer._market_for_raw_symbol(symbol) != "RUT"
        for symbol in streamer.live_symbols
    )
    assert stopped == [True]
