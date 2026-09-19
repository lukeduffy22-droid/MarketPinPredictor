from datetime import date
import json
import sqlite3

import pytest
from streamlit.testing.v1 import AppTest

from app.services.shadow_research_view import load_shadow_history, shadow_history_display_rows


@pytest.fixture
def journal(tmp_path):
    path = tmp_path / "shadow.db"
    with sqlite3.connect(path) as connection:
        connection.execute("""CREATE TABLE shadow_prediction_journal (
            prediction_id TEXT PRIMARY KEY, symbol TEXT, formula_id TEXT, formula_version TEXT,
            prediction_timestamp_utc TEXT, target_timestamp_utc TEXT, horizon_seconds INTEGER,
            spot REAL, predicted_price REAL, abstained INTEGER, abstention_reasons_json TEXT,
            realized_price REAL, realized_timestamp_utc TEXT, absolute_error_points REAL,
            direction_hit INTEGER)""")
        for i in range(125):
            abstained, scored = i % 3 == 0, i % 3 == 1
            day = "2026-09-16" if i < 5 else "2026-09-17"
            connection.execute("INSERT INTO shadow_prediction_journal VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                f"forecast-{i:03}", "SPX" if i % 2 == 0 else "NDX",
                "baseline" if i % 2 == 0 else "candidate", "v1",
                day + "T14:00:00+00:00", day + "T14:05:00+00:00", 300,
                100, None if abstained else 101, int(abstained),
                json.dumps(["MISSING_100%_QUOTE"] if abstained else []),
                102 if scored else None, day + "T14:05:01+00:00" if scored else None,
                1 if scored else None, 1 if scored else None,
            ))
    return path


def test_search_finds_old_forecast_outside_latest_hundred_without_writes(journal):
    before = journal.read_bytes()
    result = load_shadow_history(journal, search="forecast-001")
    assert result["total"] == 1
    assert result["rows"][0]["prediction_id"] == "forecast-001"
    assert journal.read_bytes() == before


def test_stable_pages_have_no_missing_or_repeated_rows_at_equal_timestamps(journal):
    rows = []
    for page in range(1, 4):
        result = load_shadow_history(journal, page=page)
        assert result["total"] == 125
        assert result["pages"] == 3
        rows.extend(row["prediction_id"] for row in result["rows"])
    assert len(rows) == len(set(rows)) == 125
    assert load_shadow_history(journal, page=999)["page"] == 3


def test_combined_filters_dates_and_literal_search(journal):
    result = load_shadow_history(journal, symbol="NDX", formula_id="candidate", status="Scored",
                                 start_date=date(2026, 9, 16), end_date=date(2026, 9, 16))
    assert [row["prediction_id"] for row in result["rows"]] == ["forecast-001"]
    assert load_shadow_history(journal, search="%_", status="Abstentions")["total"] == 42
    assert load_shadow_history(journal, search="' OR 1=1 --")["total"] == 0
    assert load_shadow_history(journal, status="Eligible forecasts")["total"] == 83
    assert load_shadow_history(journal, status="Unscored forecasts")["total"] == 41
    with pytest.raises(ValueError, match="Start date"):
        load_shadow_history(journal, start_date=date(2026, 9, 17), end_date=date(2026, 9, 16))


def test_display_keeps_abstentions_and_unscored_outcomes_blank(journal):
    abstention = shadow_history_display_rows(load_shadow_history(journal, search="forecast-000")["rows"])[0]
    assert abstention["Status"] == "ABSTAIN"
    assert abstention["Forecast price"] is None
    assert abstention["Observed outcome"] is None
    unscored = shadow_history_display_rows(load_shadow_history(journal, search="forecast-002")["rows"])[0]
    assert unscored["Status"] == "UNSCORED"
    assert unscored["Direction correct"] is None
    assert unscored["Horizon (minutes)"] == 5


def test_missing_journal_is_not_created(tmp_path):
    path = tmp_path / "absent.db"
    assert load_shadow_history(path)["rows"] == []
    assert not path.exists()


def test_frontend_search_pagination_and_filter_reset(journal):
    app = AppTest.from_string(
        "from app.services.shadow_research_view import render_shadow_history\n"
        f"render_shadow_history({str(journal)!r}, symbols=['SPX', 'NDX'], formulas=['baseline', 'candidate'])"
    ).run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 50
    app.number_input(key="shadow_history_page").set_value(3).run()
    assert not app.exception
    assert len(app.dataframe[0].value) == 25
    app.text_input(key="shadow_history_search").set_value("forecast-001").run()
    assert not app.exception
    assert app.number_input(key="shadow_history_page").value == 1
    assert app.dataframe[0].value.iloc[0]["Forecast ID"] == "forecast-001"
    app.selectbox(key="shadow_history_status").select("Abstentions").run()
    assert not app.exception
    assert any("No shadow journal rows" in item.value for item in app.info)
