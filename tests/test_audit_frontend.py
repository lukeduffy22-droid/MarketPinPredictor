"""Offline rendering and evidence-boundary regression tests."""
import ast
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

import pytest
from streamlit.testing.v1 import AppTest

from app.services.evidence_stages import capture_evidence_stages, evidence_cache_context
from app.services import shadow_research_view as shadow


@pytest.fixture(autouse=True)
def offline_only(monkeypatch):
    import requests
    def blocked(*args, **kwargs):
        raise AssertionError("UI fixture must not contact a live endpoint")
    monkeypatch.setattr(requests.sessions.Session, "request", blocked)
    shadow._cached_shadow_state.clear()


@pytest.fixture
def journal(tmp_path):
    path = tmp_path / "shadow.db"
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE shadow_prediction_journal (
            prediction_id TEXT, symbol TEXT, formula_id TEXT, formula_version TEXT,
            prediction_timestamp_utc TEXT, target_timestamp_utc TEXT, created_at_utc TEXT,
            horizon_seconds INTEGER, spot REAL, predicted_price REAL, abstained INTEGER,
            abstention_reasons_json TEXT, realized_price REAL, realized_timestamp_utc TEXT,
            absolute_error_points REAL, direction_hit INTEGER, production_signal_replaced INTEGER,
            confidence REAL, equation TEXT)""")
        for name, realized in (("on-time", "14:05:30"), ("late", "14:05:31"), ("pending", None)):
            conn.execute("INSERT INTO shadow_prediction_journal VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                name, "SPX", "baseline", "v1", "2026-10-27T14:00:00Z", "2026-10-27T14:05:00Z",
                "2026-10-27T14:00:01Z", 300, 100, 101, 0, "[]", 102 if realized else None,
                "2026-10-27T" + realized + "Z" if realized else None, 1 if realized else None,
                1 if realized else None, 0, .75, "reference + drift"))
    return path


def test_outcome_summary_and_history_share_target_protocol(journal):
    before = journal.read_bytes()
    state = shadow.load_shadow_research_state(journal)
    assert state["summary"]["eligible_forecasts"] == 3
    assert state["summary"]["scored_outcomes"] == 1
    assert state["summary"]["quarantined_outcomes"] == 1
    assert [r["prediction_id"] for r in shadow.load_shadow_history(journal, status="Scored")["rows"]] == ["on-time"]
    quarantined = shadow.load_shadow_history(journal, status="Quarantined outcomes")["rows"]
    display = shadow.shadow_history_display_rows(quarantined)[0]
    assert display["Status"] == "QUARANTINED"
    assert display["Target lag (seconds)"] == 31
    assert display["Absolute error (points)"] is None
    assert display["Direction correct"] is None
    assert "OUTCOME_AFTER_TARGET_TOLERANCE" in display["Reason"]
    assert journal.read_bytes() == before


def test_summary_projects_only_visible_columns_and_reuses_inventory(journal):
    with sqlite3.connect(journal) as conn:
        conn.execute("ALTER TABLE shadow_prediction_journal ADD COLUMN large_payload_json TEXT")
        conn.execute("UPDATE shadow_prediction_journal SET large_payload_json=?", ('x' * 100000,))
    state = shadow.load_shadow_research_state(journal)
    assert "large_payload_json" not in state["rows"][0]
    assert state["symbols"] == ["SPX"] and state["formulas"] == ["baseline"]
    page = shadow.load_shadow_history(journal, include_inventory=False)
    assert page["total"] == 3 and len(page["rows"]) == 3
    assert page["symbols"] == [] and page["formulas"] == []


def test_missing_persistence_timestamp_never_counts_as_scored(journal):
    with sqlite3.connect(journal) as conn:
        conn.execute("UPDATE shadow_prediction_journal SET created_at_utc=NULL")
    assert shadow.load_shadow_research_state(journal)["summary"]["scored_outcomes"] == 0
    assert shadow.load_shadow_history(journal, status="Scored")["total"] == 0


@pytest.mark.parametrize("reported_status", ["QUARANTINED", "ELIGIBLE"])
def test_side_table_quarantine_is_visible_without_rewriting_forecast(journal, reported_status):
    with sqlite3.connect(journal) as conn:
        conn.execute("CREATE TABLE shadow_outcome_observations (prediction_id TEXT PRIMARY KEY, status TEXT, observed_reference_price REAL, observed_at_utc TEXT)")
        conn.execute("INSERT INTO shadow_outcome_observations VALUES ('pending',?,102,'2026-10-27T14:07:00Z')", (reported_status,))
    before = journal.read_bytes()
    assert shadow.load_shadow_research_state(journal)["summary"]["quarantined_outcomes"] == 2
    assert shadow.load_shadow_history(journal, status="Unscored forecasts")["total"] == 0
    result = shadow.load_shadow_history(journal, status="Quarantined outcomes", search="pending")
    row = shadow.shadow_history_display_rows(result["rows"])[0]
    assert row["Observed outcome"] == 102 and row["Status"] == "QUARANTINED"
    assert row["Absolute error (points)"] is None and row["Target lag (seconds)"] == 120
    assert journal.read_bytes() == before


def test_cache_invalidates_for_runtime_and_wal_changes(monkeypatch, journal):
    reads = []
    monkeypatch.setattr(shadow, "load_shadow_research_state", lambda path, limit: reads.append(path) or {"rows": []})
    source = str(journal)
    fingerprint = shadow._journal_identity(source)
    for _ in range(2):
        shadow._cached_shadow_state(source, fingerprint, ("epoch", 1), 100)
    assert len(reads) == 1
    shadow._cached_shadow_state(source, fingerprint, ("epoch", 2), 100)
    assert len(reads) == 2
    Path(source + "-wal").write_bytes(b"fixture-change")
    shadow._cached_shadow_state(source, shadow._journal_identity(source), ("epoch", 2), 100)
    assert len(reads) == 3


def test_slow_read_is_unavailable_not_empty_success(monkeypatch, journal):
    original = shadow._prepare_read
    monkeypatch.setattr(shadow, "_prepare_read", lambda conn: original(conn, budget_seconds=-1))
    with sqlite3.connect(journal) as conn:
        for _ in range(10):
            conn.execute("INSERT INTO shadow_prediction_journal SELECT * FROM shadow_prediction_journal")
    state = shadow.load_shadow_research_state(journal)
    assert state["error"] == "SHADOW_READ_TIMEOUT"
    assert state["summary"] == {}


def test_requested_evidence_stages_do_not_promote_complete_capture():
    rows = capture_evidence_stages({"catalog_available": True, "state": "complete",
        "observed": {"available": True, "tcbbo_evidence": {"finalized": True}},
        "usable_for_research": False}, {"model_evidence_sessions": 0, "model_training_ready": False})
    states = {r["Stage"]: r["Status"] for r in rows}
    assert states == {"Captured": "Verified", "Finalized": "Verified", "Validated": "Not established",
                      "Labeled": "0 sessions", "Model eligible": "Not established"}
    failed = capture_evidence_stages({"transport_ok": False, "observed": {"available": True}}, {})
    assert all(row["Status"] == "Unknown" for row in failed)
    assert evidence_cache_context({"subscription_generation": 1}, "2026-10-27") != evidence_cache_context({"subscription_generation": 2}, "2026-10-27")


@pytest.mark.parametrize("score", [None, True, float("nan"), float("inf"), "bad", -1, 2])
def test_missing_or_malformed_quality_is_not_zero_confidence(score):
    assert shadow._quality_score(score) == "Unavailable"
    assert shadow._reason_list("malformed") == ["MALFORMED_RETAINED_REASONS"]


def test_inactive_sidebar_panels_do_not_execute_and_refresh_reads_once(monkeypatch, journal):
    reads = []
    original = shadow.load_shadow_research_state
    def counted(path, *, limit):
        reads.append(path)
        return original(path, limit=limit)
    monkeypatch.setattr(shadow, "load_shadow_research_state", counted)
    app = AppTest.from_string(
        "from app.services.market_view_navigation import select_market_view\n"
        "from app.services.shadow_research_view import render_shadow_research_panel\n"
        "view = select_market_view()\n"
        f"if view == 'Shadow Research': render_shadow_research_panel({str(journal)!r})\n"
    ).run()
    assert not app.exception and not reads
    app.radio(key="market_data_view").set_value("Shadow Research").run()
    assert not app.exception and len(reads) == 1
    assert any("Outcome coverage: 1/3" in item.value for item in app.caption)
    assert "Data-quality score (uncalibrated)" in app.dataframe[0].value.columns
    assert not app.text_input
    app.button(key="refresh_shadow_evidence").click().run()
    assert not app.exception and len(reads) == 2
    app.radio(key="market_data_view").set_value("Advisor").run()
    assert not app.exception and len(reads) == 2 and not app.dataframe


def test_real_dashboard_wires_detail_reads_under_selected_view_and_no_double_refresh():
    source = (Path(__file__).resolve().parents[1] / "app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    branches = {node.test.comparators[0].value: node for node in ast.walk(tree)
                if isinstance(node, ast.If) and isinstance(node.test, ast.Compare)
                and isinstance(node.test.left, ast.Name) and node.test.left.id == "market_view"}
    for view, function in (("Live Streaming", "render_backend_streaming_status"),
                           ("Shadow Research", "render_shadow_research_panel"),
                           ("Frozen Gamma (Audit)", "load_last_valid_snapshot")):
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)
                 and isinstance(node.func, ast.Name) and node.func.id == function]
        assert calls and all(call in list(ast.walk(branches[view])) for call in calls)
    start = source.index('st.button("Refresh live status now"')
    end = source.index("render_backend_streaming_status", start)
    assert "st.rerun" not in source[start:end]


def test_retained_forecast_fix_preserves_vintage_and_rejects_future_or_invalid(tmp_path):
    from app.services.retained_forecasts import read_retained_forecasts
    path = tmp_path / "market.db"
    epoch = "a" * 64
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE prediction_snapshots (
            id INTEGER PRIMARY KEY, symbol TEXT, timestamp_utc TEXT, predicted_close REAL,
            subscription_epoch_id TEXT, subscription_generation INTEGER, source_payload_json TEXT,
            trading_date TEXT, provider TEXT, prediction_mode TEXT, is_valid INTEGER, validation_status TEXT)""")
        for number, stamp, valid in ((1, "14:30:00", True), (2, "14:30:30", False), (3, "14:32:00", True)):
            conn.execute("INSERT INTO prediction_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", (
                number, "SPX", "2026-10-27T" + stamp + "Z", 7000+number, epoch, 7,
                json.dumps({"symbol": "SPX", "validation_is_valid": valid, "gamma_excluded_from_model": False,
                            "subscription_epoch_id": epoch, "subscription_generation": 7}),
                "2026-10-27", "databento", "backend_periodic", 1, "valid"))
    before = path.read_bytes()
    result = read_retained_forecasts(path, ["SPX"], datetime(2026,10,27,14,31,tzinfo=timezone.utc))
    assert result["error"] is None and len(result["rows"]) == 1
    assert result["rows"][0]["snapshot_id"] == 1
    assert result["rows"][0]["age_seconds"] == 60
    assert result["rows"][0]["timestamp_utc"] == "2026-10-27T14:30:00Z"
    assert path.read_bytes() == before
    app = AppTest.from_string(
        "from datetime import datetime, timezone\n"
        "from pathlib import Path\n"
        "from app.services.retained_forecasts import render_retained_forecasts\n"
        f"render_retained_forecasts({{}},symbols=('SPX',),database_path=Path({str(path)!r}),now=datetime(2026,10,27,14,31,tzinfo=timezone.utc))"
    ).run()
    assert not app.exception
    assert app.metric[0].label == "SPX last accepted research close"
    assert any("live updates paused or unverified" in item.value for item in app.warning)
    assert any("not current validated forecasts" in item.value for item in app.caption)
