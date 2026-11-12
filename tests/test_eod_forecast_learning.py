from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from backend.eod_forecast_learning import analyze_eod_forecast, read_frozen_eod
from backend.forecast_calendar import session_bounds, shift_session
from backend.forecast_horizons import _forecast_digest, evaluate_forecast
from app.services.forecast_research_view import eod_comparison_rows

UTC = timezone.utc


def forecast(day, minutes=60, delta=0):
    _, close = session_bounds(day)
    origin = close - timedelta(minutes=minutes)
    spot = {"symbol": "SPY", "timestamp_utc": origin.isoformat(), "available_at_utc": origin.isoformat(),
            "spot": 100., "valid": True, "source_verified": True, "source": "databento_eqs",
            "subscription_epoch_id": "a" * 64, "subscription_generation": 1}
    result = evaluate_forecast("SPY", [spot], as_of_utc=origin, horizon_sessions=0)
    result["candidates"] = [{"method_id": method, "predicted_close": value,
        "status": "RESEARCH_ONLY", "decision_grade": False, "reasons": []}
        for method, value in (("last_price_v1", 100.), ("eod_damped_momentum_v1", 101. + delta),
                              ("eod_sample_mean_reversion_v1", 101. + delta))]
    result["forecast_id"] = _forecast_digest(result)
    result["journal_receipt"] = {"binding_verified": True,
                                 "persisted_at_utc": (origin + timedelta(seconds=1)).isoformat()}
    return result


def close(day, price=101.):
    _, end = session_bounds(day)
    return {"kind": "daily_close", "symbol": "SPY", "session_date": day.isoformat(),
            "timestamp_utc": end.isoformat(), "available_at_utc": (end + timedelta(minutes=10)).isoformat(),
            "official_close": price, "source_verified": True, "source_sha256": "b" * 64,
            "source": "official_test_fixture", "price_basis": "unadjusted"}


def history(n=55):
    start = date(2026, 6, 1)
    days = [shift_session(start, i) for i in range(n)]
    return forecast(shift_session(start, n)), [forecast(d) for d in days], [close(d) for d in days]


def evaluate(current, forecasts, outcomes):
    return analyze_eod_forecast(current, forecasts, outcomes, as_of_utc=current["as_of_utc"])


def test_causal_replay_unlocks_separate_challenger_without_mutating_forecasts():
    current, forecasts, outcomes = history()
    before = deepcopy((current, forecasts, outcomes))
    result = evaluate(current, forecasts, outcomes)
    adaptive = result["adaptive"]
    assert result["independent_sessions"] == 55
    assert adaptive["test_sessions"] == 35
    assert adaptive["status"] == "RESEARCH_ONLY"
    assert 100.49 < adaptive["predicted_close"] < 100.51
    assert adaptive["weights"]["last_price_v1"] >= .5
    assert adaptive["replay"][0]["training_sessions"] == 20
    assert adaptive["historical_error_band"]["coverage_verified"] is False
    assert not result["production_promotion_allowed"] and not result["accuracy_improvement_verified"]
    assert (current, forecasts, outcomes) == before


def test_repeated_intraday_rows_and_other_leads_do_not_inflate_sessions():
    current, forecasts, outcomes = history(3)
    extra = forecast(date.fromisoformat(forecasts[0]["target_session_date"]), minutes=59)
    wrong_lead = forecast(date.fromisoformat(forecasts[1]["target_session_date"]), minutes=5)
    result = evaluate(current, forecasts * 20 + [extra, wrong_lead], outcomes)
    assert result["independent_sessions"] == 3
    assert result["adaptive"]["predicted_close"] is None
    assert result["adaptive"]["historical_error_band"] is None


def test_late_close_backfill_cannot_enter_earlier_training_windows():
    current, forecasts, outcomes = history()
    for outcome in outcomes:
        outcome["available_at_utc"] = current["as_of_utc"]
    result = evaluate(current, forecasts, outcomes)
    assert result["independent_sessions"] == 55  # retrospective comparisons are possible
    assert result["adaptive"]["test_sessions"] == 0  # historical fitting was not possible
    assert result["adaptive"]["predicted_close"] is None


@pytest.mark.parametrize("bad", ["unverified", "future", "conflict", "tamper"])
def test_bad_closes_and_changed_forecasts_are_excluded(bad):
    current, forecasts, outcomes = history(1)
    if bad == "unverified":
        outcomes[0]["source_verified"] = False
    elif bad == "future":
        outcomes[0]["available_at_utc"] = (datetime.fromisoformat(current["as_of_utc"].replace("Z", "+00:00")) + timedelta(days=1)).isoformat()
    elif bad == "conflict":
        outcomes.append({**outcomes[0], "official_close": 102.})
    else:
        forecasts[0]["candidates"][1]["predicted_close"] = 123.
    result = evaluate(current, forecasts, outcomes)
    assert result["independent_sessions"] == 0


def test_same_origin_different_receipts_fail_closed():
    current, forecasts, outcomes = history(1)
    contradictory = forecast(date.fromisoformat(forecasts[0]["target_session_date"]), delta=1)
    result = evaluate(current, forecasts + [contradictory], outcomes)
    assert result["independent_sessions"] == 0
    assert result["excluded"]["conflicting_forecast_at_same_origin"] == 1


def test_worse_candidate_does_not_pass_baseline_gate():
    current, forecasts, outcomes = history()
    for row in outcomes:
        row["official_close"] = 100.
    result = evaluate(current, forecasts, outcomes)
    assert result["adaptive"]["predicted_close"] is None
    assert "NO_STABLE_IMPROVEMENT_OVER_LAST_PRICE" in result["adaptive"]["reasons"]


def test_new_equal_blend_composition_does_not_borrow_old_blend_scores():
    current, forecasts, outcomes = history(1)
    for f, components in ((current, ["last_price_v1"]), (forecasts[0], ["last_price_v1", "eod_damped_momentum_v1"])):
        f["candidates"].append({"method_id": "blend", "predicted_close": 100.5,
                                "status": "RESEARCH_ONLY", "components": components})
        f["forecast_id"] = _forecast_digest(f)
    assert evaluate(current, forecasts, outcomes)["independent_sessions"] == 0


def test_prospective_challenger_preserves_legacy_comparisons_and_adaptive_weights():
    current, forecasts, outcomes = history()
    original = deepcopy(current)
    before = evaluate(current, forecasts, outcomes)
    # Deliberately use a recognized component id, absent from the old fixture,
    # to prove the prospective marker also excludes adaptive weighting.
    method = "eod_positive_gex_pin_context_v1"
    current["candidates"].append({"method_id": method, "predicted_close": 120,
        "status": "RESEARCH_ONLY", "prospective_only": True, "default_selection_eligible": False})
    current["forecast_id"] = _forecast_digest(current)
    after = evaluate(current, forecasts, outcomes)
    assert after["independent_sessions"] == before["independent_sessions"] == 55
    assert after["comparisons"] == before["comparisons"]
    assert after["adaptive"] == before["adaptive"]
    assert method not in after["adaptive"]["weights"]
    assert after["prospective_methods_excluded_from_shared_cohort"] == [method]
    assert current["selected_method"] == original["selected_method"]
    assert current["predicted_close"] == original["predicted_close"]


def test_reader_verifies_receipts_and_never_writes_source(tmp_path):
    current, forecasts, outcomes = history(1)
    p = tmp_path / "journal.db"
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE research_forecasts(forecast_id TEXT,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        f = forecasts[0]
        raw = json.dumps({"forecast": f, "captured_at_utc": f["as_of_utc"]}, sort_keys=True, separators=(",", ":"))
        key = hashlib.sha256(raw.encode()).hexdigest()
        row = (key, "SPY", f["as_of_utc"], 0, f["target_session_date"], raw)
        c.execute("INSERT INTO research_forecasts VALUES (?,?,?,?,?,?)", row)
        c.execute("INSERT INTO research_forecasts VALUES (?,?,?,?,?,?)", ("wrong", *row[1:]))
    before = p.read_bytes()
    read, audit = read_frozen_eod(p, ["SPY"], as_of_utc=current["as_of_utc"])
    assert read[0]["forecast_id"] == forecasts[0]["forecast_id"]
    assert read[0]["journal_receipt"]["persisted_at_utc"] is None
    assert audit["rejected"] == {"JOURNAL_RECEIPT_INVALID": 1}
    assert p.read_bytes() == before
    read, audit = read_frozen_eod(p, ["SPY"], as_of_utc=current["as_of_utc"], limit=1)
    assert not read and audit["status"] == "UNAVAILABLE"


def test_reader_preserves_all_forecasts_and_order_without_sorting_payloads_in_sql(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as module

    current, forecasts, _ = history(3)
    path = tmp_path / "journal.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE research_forecasts(forecast_id TEXT,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        for f in (forecasts[1], forecasts[0], forecasts[2], current):
            payload = {"forecast": f, "captured_at_utc": f["as_of_utc"], "retained_context": "x" * 100000}
            raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            conn.execute("INSERT INTO research_forecasts VALUES(?,?,?,?,?,?)", (
                hashlib.sha256(raw.encode()).hexdigest(), "SPY", f["as_of_utc"], 0,
                f["target_session_date"], raw))
    before = path.read_bytes()
    statements = []
    connect = sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(module.sqlite3, "connect", traced_connect)
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"], limit=3)
    assert audit["status"] == "READ_ONLY"
    assert [r["forecast_id"] for r in rows] == [r["forecast_id"] for r in reversed(forecasts)]
    assert not any("ORDER BY" in statement.upper() for statement in statements)
    assert path.read_bytes() == before
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"], limit=2)
    assert rows == [] and audit["reasons"] == ["EVALUATION_SCAN_LIMIT_REACHED"]


def test_reader_timeout_discards_partial_results_and_reports_specific_cause(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as module

    path = tmp_path / "journal.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE research_forecasts(forecast_id TEXT,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        conn.executemany("INSERT INTO research_forecasts VALUES(?,?,?,?,?,?)", [
            (str(i), "SPY", "2026-06-01T19:00:00+00:00", 0, "2026-06-01", "{}") for i in range(1000)])
    before = path.read_bytes()
    times = iter([0.0])
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: next(times, 4.0)))
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc="2026-06-02T19:00:00+00:00")
    assert rows == [] and audit["status"] == "UNAVAILABLE"
    assert audit["reasons"] == ["EVALUATION_JOURNAL_READ_FAILED", "EVALUATION_JOURNAL_READ_TIMEOUT"]
    assert path.read_bytes() == before


def test_reader_rejects_malformed_origins_without_losing_valid_rows(tmp_path):
    current, forecasts, _ = history(2)
    path = tmp_path / "journal.db"
    with sqlite3.connect(path) as conn:
        # No affinity on origin also exercises a non-text legacy/corrupt value.
        conn.execute("CREATE TABLE research_forecasts(forecast_id TEXT,symbol TEXT,as_of_utc,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        for origin in (None, 123, b"2026-06-01T19:00:00+00:00", "not-a-time", *[f["as_of_utc"] for f in forecasts]):
            f = next((f for f in forecasts if f["as_of_utc"] == origin), forecasts[0])
            payload = {"forecast": f, "captured_at_utc": f["as_of_utc"]}
            if origin is None:
                payload = {"forecast": {**f, "as_of_utc": None}, "captured_at_utc": None}
            raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            conn.execute("INSERT INTO research_forecasts VALUES(?,?,?,?,?,?)", (
                hashlib.sha256(raw.encode()).hexdigest(), "SPY", origin, 0, f["target_session_date"], raw))
    before = path.read_bytes()
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"])
    assert audit["status"] == "READ_ONLY"
    assert audit["rejected"] == {"JOURNAL_RECEIPT_INVALID": 4}
    assert [f["forecast_id"] for f in rows] == [f["forecast_id"] for f in reversed(forecasts)]
    assert path.read_bytes() == before


def test_reader_locked_journal_stays_unavailable_and_reports_specific_cause(tmp_path):
    path = tmp_path / "journal.db"
    with sqlite3.connect(path) as blocker:
        blocker.execute("CREATE TABLE research_forecasts(forecast_id TEXT)")
        blocker.commit()
        blocker.execute("BEGIN EXCLUSIVE")
        try:
            rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc="2026-06-02T19:00:00+00:00")
        finally:
            blocker.rollback()
    assert rows == [] and audit["status"] == "UNAVAILABLE"
    assert audit["reasons"] == ["EVALUATION_JOURNAL_READ_FAILED", "EVALUATION_JOURNAL_LOCKED"]


def test_failed_learning_read_retries_next_collection_then_caches_success(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as learning_module
    import backend.market_research_service as service_module

    calls = []

    def read_journal(*args, **kwargs):
        calls.append(kwargs["as_of_utc"])
        return ([], {"status": "UNAVAILABLE", "reasons": ["EVALUATION_JOURNAL_READ_FAILED"]}) if len(calls) == 1 else (
            [], {"status": "READ_ONLY", "frozen_forecasts": 0, "rejected": {}})

    monkeypatch.setattr(learning_module, "read_frozen_eod", read_journal)
    monkeypatch.setattr(service_module, "load_research_history", lambda *a, **k: ({"SPY": []}, []))
    service = service_module.MarketResearchService(
        tmp_path / "source.db", tmp_path / "journal.db", symbols=["SPY"], context_reader=lambda: {})
    monkeypatch.setattr(service, "_persist", lambda state: None)
    now = datetime(2026, 6, 2, 19, tzinfo=UTC)
    failed = service.collect_once(now=now)["symbols"]["SPY"]["eod_learning"]
    assert failed["journal_read"]["status"] == "UNAVAILABLE"
    assert failed["adaptive"]["predicted_close"] is None
    assert "EVALUATION_JOURNAL_READ_INCOMPLETE" in failed["adaptive"]["reasons"]
    recovered = service.collect_once(now=now + timedelta(seconds=60))["symbols"]["SPY"]["eod_learning"]
    assert recovered["journal_read"]["status"] == "READ_ONLY"
    assert "EVALUATION_JOURNAL_READ_INCOMPLETE" not in recovered["adaptive"].get("reasons", [])
    # Recovery of the journal does not override absent current market evidence.
    assert recovered["status"] == "ABSTAIN" and recovered["adaptive"]["predicted_close"] is None
    service.collect_once(now=now + timedelta(seconds=120))
    assert len(calls) == 2


def test_display_requires_current_exact_forecast_and_correct_horizon():
    current, forecasts, outcomes = history(1)
    now = datetime.fromisoformat(current["as_of_utc"].replace("Z", "+00:00"))
    learning = evaluate(current, forecasts, outcomes)
    identity = {k: current["evidence"][k] for k in ("subscription_epoch_id", "subscription_generation")}
    gate = {"schema_version": "marketpin-forecast-publication-gate.v1", "status": "ELIGIBLE",
        "checked_at_utc": now.isoformat(), "runtime_context": identity,
        "symbols": {"SPY": {"status": "ELIGIBLE", "source_forecast_id": current["forecast_id"], "reasons": [],
            "valid_until_utc": (now+timedelta(seconds=15)).isoformat()}}}
    payload = {"symbols": {"SPY": {"forecast": current, "eod_learning": learning, "publication_gate": gate,
        "capture": {"status": "RESEARCH_ONLY", "as_of_utc": now.isoformat(), "runtime_context": identity}}}}
    assert len(eod_comparison_rows(payload, now=now)) == 3
    assert not eod_comparison_rows(payload, now=now + timedelta(seconds=121))
    learning["source_forecast_id"] = "different"
    assert not eod_comparison_rows(payload, now=now)
    learning["source_forecast_id"] = current["forecast_id"]
    current["horizon_sessions"] = 1
    assert not eod_comparison_rows(payload, now=now)


def test_after_close_and_stale_current_forecasts_withhold_all_estimates():
    current, forecasts, outcomes = history(1)
    result = analyze_eod_forecast(current, forecasts, outcomes, as_of_utc=current["target_close_utc"])
    assert result["status"] == "ABSTAIN" and not result["comparisons"]
    assert result["adaptive"]["predicted_close"] is None


@pytest.mark.parametrize("timing", [None, "late"])
def test_legacy_or_postclose_receipts_never_train_adaptive_estimate(timing):
    current, forecasts, outcomes = history()
    for f in forecasts:
        f["journal_receipt"]["persisted_at_utc"] = f["target_close_utc"] if timing == "late" else None
    result = evaluate(current, forecasts, outcomes)
    assert result["independent_sessions"] == 55
    assert result["verified_issuance_sessions"] == 0
    assert result["adaptive"]["predicted_close"] is None


def test_stopped_worker_withholds_comparison_and_adaptive_values(tmp_path):
    from backend.market_research_service import MarketResearchService
    current, forecasts, outcomes = history()
    service = MarketResearchService(tmp_path / "source.db", tmp_path / "journal.db", symbols=["SPY"])
    learning = evaluate(current, forecasts, outcomes)
    service._state = {"status": "RESEARCH_ONLY", "as_of_utc": current["as_of_utc"], "reasons": [],
        "symbols": {"SPY": {"forecasts": {"0": current}, "eod_learning": learning,
                             "directional_shift": {"status": "RESEARCH_ONLY"}}}}
    service._stop.set()
    observed = service.snapshot(now=current["as_of_utc"])["symbols"]["SPY"]["eod_learning"]
    assert observed["status"] == "ABSTAIN"
    assert all(r["current_estimate"] is None for r in observed["comparisons"])
    assert observed["adaptive"]["predicted_close"] is None
    assert observed["adaptive"]["historical_error_band"] is None
    assert learning["adaptive"]["predicted_close"] is not None  # retained evidence unchanged


def _indexed_journal(tmp_path, monkeypatch):
    import backend.market_research_service as service_module
    day = date(2026, 6, 1)
    first = forecast(day, minutes=60)
    clock = [datetime.fromisoformat(first["as_of_utc"].replace("Z", "+00:00")) + timedelta(seconds=1)]
    class Clock:
        @staticmethod
        def now(tz=None): return clock[0]
    monkeypatch.setattr(service_module, "datetime", Clock)
    service = service_module.MarketResearchService(tmp_path / "source.db", tmp_path / "journal.db", symbols=["SPY"])
    service.initialize_journal()
    def persist(f):
        service._persist({"as_of_utc": f["as_of_utc"], "symbols": {"SPY": {
            "forecasts": {"0": f}, "directional_shift": {}, "evidence_sha256": "a"*64,
            "eod_learning": {"retained_detail": "x"*300000}}}})
    persist(first)
    return service, clock, persist, first


def test_live_writer_freezes_first_fixed_lead_and_separates_prospective_evaluation(tmp_path, monkeypatch):
    from backend.eod_retrospective import score_completed_eod
    service, clock, persist, first = _indexed_journal(tmp_path, monkeypatch)
    second = forecast(date(2026, 6, 1), minutes=59, delta=10)
    clock[0] += timedelta(seconds=60)
    persist(second)
    rows, audit = read_frozen_eod(service.journal_database, ["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    assert audit["status"] == "READ_ONLY" and len(rows) == 2
    selected = [r for r in rows if r["prospective_selection"]]
    assert len(selected) == 1 and selected[0]["forecast_id"] == first["forecast_id"]
    assert selected[0]["prospective_selection"]["lead_minutes"] == 60
    scored = score_completed_eod(rows, {"SPY": [close(date(2026, 6, 1))]}, symbols=["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    prospective = [r for r in scored["scored_forecasts"] if r["cohort"] == "prospective_fixed_lead"]
    assert len(prospective) == 1 and prospective[0]["forecast_id"] == first["forecast_id"]
    again, audit = read_frozen_eod(service.journal_database, ["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    assert audit["verified_cache_hits"] == 2
    # Returned nested values cannot poison the cached content-addressed object.
    again[0]["candidates"][0]["predicted_close"] = 999
    clean, _ = read_frozen_eod(service.journal_database, ["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    assert clean[0]["candidates"][0]["predicted_close"] == 100


def test_eod_immutable_guards_block_replace_but_allow_identical_replay(tmp_path, monkeypatch):
    from backend.eod_journal_index import IMMUTABLE_COLUMNS, guards_verified
    service, _, _, _ = _indexed_journal(tmp_path, monkeypatch)
    with sqlite3.connect(service.journal_database) as conn:
        assert guards_verified(conn)
        assert conn.execute("PRAGMA recursive_triggers").fetchone()[0] == 0
        for table, (_, columns) in IMMUTABLE_COLUMNS.items():
            row = conn.execute(f"SELECT * FROM {table}").fetchone()
            query = f"INSERT OR IGNORE INTO {table} VALUES ({','.join('?' for _ in row)})"
            conn.execute(query, row)
            changed = list(row)
            changed[-1] = str(changed[-1]) + "changed"
            with pytest.raises(sqlite3.IntegrityError, match="replacement denied"):
                conn.execute(query.replace("OR IGNORE", "OR REPLACE"), changed)


def test_same_name_dummy_guard_invalidates_cache_and_tampered_projection_is_rejected(tmp_path, monkeypatch):
    from backend.eod_journal_index import binding
    service, _, _, _ = _indexed_journal(tmp_path, monkeypatch)
    args = (service.journal_database, ["SPY"])
    assert read_frozen_eod(*args, as_of_utc="2026-06-02T19:00:00Z")[0]
    with sqlite3.connect(service.journal_database) as conn:
        conn.execute("DROP TRIGGER research_eod_index_no_update")
        conn.execute("CREATE TRIGGER research_eod_index_no_update BEFORE UPDATE ON research_eod_index BEGIN SELECT 1; END")
        key, raw = conn.execute("SELECT forecast_id,forecast_json FROM research_eod_index").fetchone()
        committed = conn.execute("SELECT committed_at_utc FROM research_forecast_receipts").fetchone()[0]
        altered = json.loads(raw); altered["candidates"][0]["predicted_close"] = 999
        raw = json.dumps(altered, sort_keys=True, separators=(",", ":"))
        sha = hashlib.sha256(raw.encode()).hexdigest()
        conn.execute("UPDATE research_eod_index SET forecast_json=?,forecast_sha256=?,binding_sha256=?",
                     (raw, sha, binding(key, sha, committed)))
    rows, audit = read_frozen_eod(*args, as_of_utc="2026-06-02T19:00:00Z")
    assert not rows and audit["verified_cache_hits"] == 0
    assert audit["rejected"] == {"JOURNAL_RECEIPT_INVALID": 1}


def test_python_verification_budget_also_fails_closed(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as module
    service, _, _, _ = _indexed_journal(tmp_path, monkeypatch)
    clock = iter([0., 0., 0., 4.])
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: next(clock, 4.)))
    rows, audit = read_frozen_eod(service.journal_database, ["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    assert rows == [] and audit["status"] == "UNAVAILABLE"
    assert "EVALUATION_JOURNAL_READ_TIMEOUT" in audit["reasons"]


def test_session_uncertainty_uses_sessions_not_number_of_intraday_forecasts():
    from backend.research.evaluation_statistics import paired_session_summary
    rows = [{"session": "one", "prediction": 101., "baseline": 100., "outcome": 101.}] * 100
    rows += [{"session": "two", "prediction": 99., "baseline": 100., "outcome": 101.}]
    result = paired_session_summary(rows)
    assert result["paired_outcomes"] == 101 and result["independent_session_count"] == 2
    assert result["mae_points"] == 1. and result["skill_vs_baseline"] == 0.
    assert result["uncertainty"]["coverage_verified"] is False
    assert result["performance_claim_eligible"] is False


def test_large_guarded_journal_resumes_cold_verification_then_reuses_content(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as module
    from backend.eod_journal_index import initialize_eod_index
    current, forecasts, _ = history(1)
    path = tmp_path / "large.db"
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE research_forecasts(forecast_id TEXT PRIMARY KEY,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        conn.execute("CREATE TABLE research_forecast_receipts(forecast_id TEXT PRIMARY KEY,committed_at_utc TEXT,receipt_sha256 TEXT)")
        initialize_eod_index(conn)
        for index in range(200):
            f = forecasts[0]
            raw = json.dumps({"forecast": f, "captured_at_utc": f["as_of_utc"],
                              "retained_context": "x"*65536, "sequence": index}, sort_keys=True, separators=(",", ":"))
            key = hashlib.sha256(raw.encode()).hexdigest()
            committed = f["journal_receipt"]["persisted_at_utc"]
            conn.execute("INSERT INTO research_forecasts VALUES (?,?,?,?,?,?)", (key, "SPY", f["as_of_utc"], 0, f["target_session_date"], raw))
            conn.execute("INSERT INTO research_forecast_receipts VALUES (?,?,?)", (key, committed,
                hashlib.sha256((key + "\n" + committed).encode()).hexdigest()))
    loads = json.loads
    parsed = [0]
    def counted(raw):
        parsed[0] += 1
        return loads(raw)
    monkeypatch.setattr(module.json, "loads", counted)
    # Deterministically exhaust the entire Python+SQL budget on payload ten.
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: 4. if parsed[0] >= 10 else 0.))
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"])
    assert rows == [] and audit["status"] == "UNAVAILABLE" and audit["verified_cache_entries"] == 9
    monkeypatch.setattr(module, "time", SimpleNamespace(monotonic=lambda: 0.))
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"])
    assert len(rows) == 200 and audit["complete_cohort"] and audit["verified_cache_hits"] == 9
    parsed[0] = 0
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc=current["as_of_utc"])
    assert len(rows) == 200 and audit["verified_cache_hits"] == 200 and parsed[0] == 0


def test_cache_byte_and_entry_bounds_never_return_partial_cohort(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as module
    service, _, _, _ = _indexed_journal(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "MAX_CACHE_BYTES", 1)
    rows, audit = read_frozen_eod(service.journal_database, ["SPY"], as_of_utc="2026-06-02T19:00:00Z")
    assert rows == [] and audit["reasons"] == ["EVALUATION_COHORT_MEMORY_LIMIT"]
    assert audit["complete_cohort"] is False
    monkeypatch.setattr(module, "MAX_CACHE_BYTES", 100000)
    monkeypatch.setattr(module, "MAX_CACHE_ENTRIES", 2)
    cache = module._VerifiedCache()
    for index in range(3): cache.remember(index, {"candidate": index})
    assert list(cache) == [1, 2] and cache.byte_size <= module.MAX_CACHE_BYTES


def test_metadata_bucket_filter_is_complete_across_dst_and_early_close(tmp_path):
    path = tmp_path / "calendar.db"
    dates = [date(2026, 10, 27), date(2026, 11, 25), date(2026, 11, 27)]
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE research_forecasts(forecast_id TEXT PRIMARY KEY,symbol TEXT,as_of_utc TEXT,horizon_sessions INTEGER,target_session_date TEXT,payload_json TEXT)")
        for day in dates:
            for minutes in (60, 59, 45):
                f = forecast(day, minutes=minutes)
                raw = json.dumps({"forecast": f, "captured_at_utc": f["as_of_utc"]}, sort_keys=True, separators=(",", ":"))
                conn.execute("INSERT INTO research_forecasts VALUES(?,?,?,?,?,?)", (hashlib.sha256(raw.encode()).hexdigest(),
                    "SPY", f["as_of_utc"], 0, f["target_session_date"], raw))
    before = path.read_bytes()
    rows, audit = read_frozen_eod(path, ["SPY"], as_of_utc="2026-11-30T19:00:00Z", lead_bucket=4)
    assert len(rows) == 6 and audit["complete_cohort"] and audit["lead_bucket_minutes"] == 60
    assert {r["target_session_date"] for r in rows} == {d.isoformat() for d in dates}
    assert {datetime.fromisoformat(r["target_close_utc"].replace("Z", "+00:00")).hour for r in rows} == {18, 20, 21}
    assert path.read_bytes() == before


def test_live_evaluation_cache_refreshes_on_lead_bucket_transition(tmp_path, monkeypatch):
    import backend.eod_forecast_learning as learning
    import backend.market_research_service as module
    buckets = []
    def read(*args, **kwargs):
        buckets.append(kwargs["lead_bucket"])
        return [], {"status": "READ_ONLY"}
    monkeypatch.setattr(learning, "read_frozen_eod", read)
    monkeypatch.setattr(module, "load_research_history", lambda *a, **k: ({"SPY": []}, []))
    service = module.MarketResearchService(tmp_path / "source.db", tmp_path / "journal.db", symbols=["SPY"], context_reader=lambda: {})
    monkeypatch.setattr(service, "_persist", lambda state: None)
    service.collect_once(now=datetime(2026, 6, 2, 19, 14, 59, tzinfo=UTC))
    service.collect_once(now=datetime(2026, 6, 2, 19, 15, 1, tzinfo=UTC))
    assert buckets == [4, 3]
