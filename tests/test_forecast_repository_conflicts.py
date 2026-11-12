"""Reject ambiguous retained references without rewriting or picking a winner."""
import hashlib
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from backend.forecast_repository import load_research_history
from backend.forecast_horizons import evaluate_forecast
from backend.directional_shift import evaluate_directional_shift

NOW = datetime(2026, 9, 23, 16, 0, tzinfo=timezone.utc)
EPOCH = "a" * 64
CONTEXT = {"subscription_epoch_id": EPOCH, "subscription_generation": 1,
           "handoff_status": "active", "runtime_context_stable": True,
           "symbol_status": {"SPX": {"usable_for_prediction": True}}}


def make_source(tmp_path, *, duplicate_index, same_price=False, available=None, epoch=EPOCH):
    path = tmp_path / "source.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE market_structure_observations (observation_id TEXT, symbol TEXT, "
                     "trading_date TEXT, source_timestamp_utc TEXT, captured_at_utc TEXT, "
                     "reference_price REAL, provider TEXT, spot_source TEXT, subscription_epoch_id TEXT, "
                     "subscription_generation INTEGER, validation_status TEXT, universe_sha256 TEXT, "
                     "same_day_profile_available INTEGER, primary_expiration TEXT, gamma_pin REAL)")
        for i in range(40):
            stamp = NOW - timedelta(minutes=40-i)
            fields = (str(i), "SPX", "2026-09-23", stamp.replace(tzinfo=None).isoformat(" "),
                      (stamp+timedelta(seconds=1)).replace(tzinfo=None).isoformat(" "), 7700+i,
                      "databento", "databento_opra_put_call_parity", EPOCH, 1, "valid", "b"*64, 1,
                      "2026-09-23", 7740)
            conn.execute("INSERT INTO market_structure_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", fields)
            if i == duplicate_index:
                duplicate = list(fields)
                duplicate[0] = "conflict"
                duplicate[4] = (available or stamp+timedelta(seconds=6)).replace(tzinfo=None).isoformat(" ")
                duplicate[5] += 0 if same_price else .001438
                duplicate[8] = epoch
                conn.execute("INSERT INTO market_structure_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", duplicate)
    return path


def load(path, **kwargs):
    return load_research_history(path, ["SPX"], as_of_utc=NOW,
                                 runtime_context=kwargs.get("context", CONTEXT))


def test_old_conflict_rejects_both_preserves_source_and_later_eligibility(tmp_path):
    path = make_source(tmp_path, duplicate_index=3)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    histories, diagnostics = load(path)
    rows = histories["SPX"]
    rejected = [row for row in rows if not row["valid"]]
    assert len(rows) == 41 and {row["observation_id"] for row in rejected} == {"3", "conflict"}
    assert all(row["validation_status"] == "conflicting_source_observations" and
               row["options_context_valid"] is False for row in rejected)
    assert len(diagnostics) == 1 and "CONFLICTING_SOURCE_OBSERVATIONS" in diagnostics[0]
    forecast = evaluate_forecast("SPX", rows, as_of_utc=NOW, current_snapshot=rows[-1])
    assert forecast["status"] == "RESEARCH_ONLY"
    assert forecast["evidence"]["excluded_rows"]["invalid_observation"] == 2
    direction = evaluate_directional_shift("SPX", rows, as_of_utc=NOW)
    assert "CONFLICTING_DUPLICATE_TIMESTAMP" not in direction["reason_codes"]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_conflict_at_latest_instant_still_abstains(tmp_path):
    path = make_source(tmp_path, duplicate_index=39)
    histories, diagnostics = load(path)
    rows = histories["SPX"]
    forecast = evaluate_forecast("SPX", rows, as_of_utc=NOW, current_snapshot=rows[-1])
    assert diagnostics and forecast["status"] == "ABSTAIN"
    assert forecast["predicted_close"] is None
    assert forecast["reasons"][0] == "current_snapshot_invalid"
    assert evaluate_directional_shift("SPX", rows, as_of_utc=NOW)["status"] == "ABSTAIN"


@pytest.mark.parametrize("kwargs", [
    {"same_price": True}, {"available": NOW+timedelta(seconds=1)}, {"epoch": "c"*64},
])
def test_identical_future_or_foreign_epoch_duplicate_cannot_poison_current_input(tmp_path, kwargs):
    path = make_source(tmp_path, duplicate_index=3, **kwargs)
    histories, diagnostics = load(path)
    assert not diagnostics
    assert not any(row.get("research_rejection_reason") for row in histories["SPX"])
    assert histories["SPX"][-1]["valid"]


def test_unverified_runtime_never_becomes_eligible(tmp_path):
    path = make_source(tmp_path, duplicate_index=3)
    rows, _ = load(path, context={**CONTEXT, "runtime_context_stable": False})
    assert all(not row["valid"] for row in rows["SPX"])
