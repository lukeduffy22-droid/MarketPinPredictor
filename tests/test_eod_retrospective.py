from copy import deepcopy
from datetime import timedelta

import pytest

from backend.eod_retrospective import score_completed_eod
from backend.forecast_horizons import _forecast_digest
from tests.test_eod_forecast_learning import history


def test_after_close_scoring_needs_no_current_forecast_and_counts_sessions_once():
    current, forecasts, closes = history(3)
    before = deepcopy((forecasts, closes))
    result = score_completed_eod(forecasts * 20, {"SPY": closes}, symbols=["SPY"],
                                as_of_utc=current["as_of_utc"])
    assert result["independent_sessions"] == {"SPY": 3}
    assert len(result["scored_forecasts"]) == 3
    candidate = next(r for r in result["comparisons"] if r["method_id"] == "eod_damped_momentum_v1")
    assert candidate["sessions"] == 3 and candidate["mae_points"] == 0
    assert candidate["baseline_mae_points"] == 1
    assert not result["production_promotion_allowed"] and not result["accuracy_improvement_verified"]
    assert (forecasts, closes) == before


@pytest.mark.parametrize("bad", ["unverified", "future", "conflict", "tamper", "origin_conflict"])
def test_invalid_close_or_forecast_never_scores(bad):
    current, forecasts, closes = history(1)
    if bad == "unverified":
        closes[0]["source_verified"] = False
    elif bad == "future":
        closes[0]["available_at_utc"] = current["target_close_utc"]
    elif bad == "conflict":
        closes.append({**closes[0], "official_close": 102.})
    elif bad == "tamper":
        forecasts[0]["candidates"][1]["predicted_close"] = 123.
    else:
        duplicate = deepcopy(forecasts[0])
        duplicate["candidates"][1]["predicted_close"] = 123.
        duplicate["forecast_id"] = _forecast_digest(duplicate)
        forecasts.append(duplicate)
    result = score_completed_eod(forecasts, {"SPY": closes}, symbols=["SPY"], as_of_utc=current["as_of_utc"])
    assert not result["scored_forecasts"]


def test_postclose_or_legacy_issuance_stays_in_diagnostic_cohort():
    current, forecasts, closes = history(2)
    forecasts[0]["journal_receipt"]["persisted_at_utc"] = None
    forecasts[1]["journal_receipt"]["persisted_at_utc"] = forecasts[1]["target_close_utc"]
    result = score_completed_eod(forecasts, {"SPY": closes}, symbols=["SPY"], as_of_utc=current["as_of_utc"])
    assert len(result["scored_forecasts"]) == 2
    assert all(r["cohort"] == "unverified_issuance_diagnostic" for r in result["comparisons"])


def test_earliest_origin_not_best_error_is_selected():
    current, forecasts, closes = history(1)
    duplicate = deepcopy(forecasts[0])
    from datetime import datetime
    origin = datetime.fromisoformat(duplicate["as_of_utc"].replace("Z", "+00:00"))
    duplicate["as_of_utc"] = (origin + timedelta(seconds=30)).isoformat()
    duplicate["journal_receipt"]["persisted_at_utc"] = (origin + timedelta(seconds=31)).isoformat()
    duplicate["candidates"][1]["predicted_close"] = 500.
    duplicate["forecast_id"] = _forecast_digest(duplicate)
    result = score_completed_eod([duplicate, *forecasts], {"SPY": closes}, symbols=["SPY"],
                                as_of_utc=current["as_of_utc"])
    assert result["scored_forecasts"][0]["forecast_id"] == forecasts[0]["forecast_id"]


def test_current_date_is_readable_after_close_without_exposing_future_outcomes(tmp_path):
    import hashlib
    import json
    import sqlite3
    from backend.eod_forecast_learning import read_frozen_eod
    from datetime import datetime

    _, forecasts, closes = history(1)
    f = forecasts[0]
    raw = json.dumps({"forecast": f, "captured_at_utc": f["as_of_utc"]}, sort_keys=True, separators=(",", ":"))
    path = tmp_path / "journal.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE research_forecasts(forecast_id TEXT,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        db.execute("INSERT INTO research_forecasts VALUES(?,?,?,?,?,?)", (
            hashlib.sha256(raw.encode()).hexdigest(), "SPY", f["as_of_utc"], 0, f["target_session_date"], raw))
    now = datetime.fromisoformat(closes[0]["available_at_utc"]) + timedelta(seconds=1)
    default_rows, _ = read_frozen_eod(path, ["SPY"], as_of_utc=now)
    assert not default_rows  # live learning keeps its existing historical scope
    rows, _ = read_frozen_eod(path, ["SPY"], as_of_utc=now, include_current_session=True)
    result = score_completed_eod(rows, {"SPY": closes}, symbols=["SPY"], as_of_utc=now)
    assert len(result["scored_forecasts"]) == 1
    before_close = score_completed_eod(rows, {"SPY": closes}, symbols=["SPY"], as_of_utc=f["as_of_utc"])
    assert not before_close["scored_forecasts"]
