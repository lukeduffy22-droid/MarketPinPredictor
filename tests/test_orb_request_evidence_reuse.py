"""Rollover retries reread authority while sharing only request-local retained inputs."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import sqlite3

import pytest

from backend.api.routers import orb
from backend.market_structure import MarketStructureJournal
from tests.test_market_structure_orb import _memory_journal, _payload, _record_both

NOW = datetime(2026, 9, 4, 14, tzinfo=timezone.utc)
EPOCH = "d" * 64


def binding(generation=7):
    return dict(active_subscription_epoch_id=EPOCH,
                active_subscription_generation=generation, active_handoff_status="active")


def empty_journal(calls):
    def references(symbol, day, *, as_of_utc):
        calls.append(("references", symbol, day, as_of_utc))
        return []
    def structures(symbol, day, *, as_of_utc):
        calls.append(("structures", symbol, day, as_of_utc))
        return []
    return MarketStructureJournal(loader=structures, reference_loader=references, now_utc=lambda: NOW)


@pytest.mark.parametrize("generations,stable", [([7,7], True), ([7,8,8], True), ([7,8,9], False)])
def test_rollover_projects_each_symbol_from_one_retained_read(monkeypatch, generations, stable):
    calls = []
    journal = empty_journal(calls)
    states = iter(generations)
    class Streamer:
        def get_subscription_context(self):
            return dict(subscription_epoch_id=EPOCH, subscription_generation=next(states),
                        handoff_status="active")
    class Clock:
        @staticmethod
        def now(tz):
            return NOW
    monkeypatch.setattr(orb, "datetime", Clock)
    symbols = ("SPX", "NDX", "VIX", "RUT")
    snapshots, context, actual_stable = orb._live_snapshots(journal, Streamer(), symbols, configured=symbols)
    assert actual_stable is stable
    # Old code performs 16/24 loader calls for one/two rollovers; fixed code 8.
    assert len(calls) == len(symbols) * 2
    assert {stamp for _, _, _, stamp in calls} == {NOW}
    final_binding = binding(generations[-1])
    if not stable:
        final_binding["active_handoff_status"] = "context_changed"
    for symbol in symbols:
        assert snapshots[symbol] == journal.snapshot(symbol, as_of_utc=NOW, **final_binding)
        assert snapshots[symbol]["current_price"] is None


def test_reused_inputs_recompute_generation_and_do_not_share_mutable_output():
    journal, _ = _memory_journal(now=NOW)
    assert _record_both(journal, _payload(NOW, 6501))["recorded"]
    reader = journal.snapshot_reader()
    live = reader("SPX", as_of_utc=NOW, **binding())
    assert live["current_price"] == 6501
    expected = deepcopy(live)
    live["warnings"].append("caller mutation")
    live["opening_ranges"]["5m"]["warnings"].append("nested mutation")
    assert reader("SPX", as_of_utc=NOW, **binding()) == expected
    changed = reader("SPX", as_of_utc=NOW, **binding(8))
    assert changed["current_price"] is None
    assert "CURRENT_RUNTIME_PROVENANCE_MISMATCH" in changed["warnings"]
    assert changed == journal.snapshot("SPX", as_of_utc=NOW, **binding(8))


def test_new_symbol_asof_and_request_reload_persistence():
    calls = []
    journal = empty_journal(calls)
    reader = journal.snapshot_reader()
    reader("SPX", as_of_utc=NOW, **binding())
    reader("spx", as_of_utc=NOW, **binding())
    assert len(calls) == 2
    reader("NDX", as_of_utc=NOW, **binding())
    reader("SPX", as_of_utc=NOW+timedelta(seconds=1), **binding())
    journal.snapshot_reader()("SPX", as_of_utc=NOW, **binding())
    assert len(calls) == 8


def test_changed_asof_cannot_reuse_freshness_from_prior_projection():
    journal, _ = _memory_journal(now=NOW)
    assert _record_both(journal, _payload(NOW, 6501))["recorded"]
    reader = journal.snapshot_reader()
    fresh = reader("SPX", as_of_utc=NOW, **binding())
    later = NOW + timedelta(hours=2)
    stale = reader("SPX", as_of_utc=later, **binding())
    assert stale == journal.snapshot("SPX", as_of_utc=later, **binding())
    assert "CURRENT_REFERENCE_STALE" in stale["warnings"]
    assert fresh["warnings"] != stale["warnings"]


def test_failed_loader_is_not_cached_as_empty_or_valid_evidence():
    attempts = []
    def references(*args, **kwargs):
        attempts.append(None)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        return []
    journal = MarketStructureJournal(loader=lambda *a, **k: [], reference_loader=references)
    reader = journal.snapshot_reader()
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        reader("SPX", as_of_utc=NOW, **binding())
    recovered = reader("SPX", as_of_utc=NOW, **binding())
    assert len(attempts) == 2
    assert recovered["current_price"] is None
