"""Read-only Streamlit view for the isolated shadow-prediction journal."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import streamlit as st


DEFAULT_SHADOW_DB = Path(__file__).resolve().parents[2] / "data" / "shadow_research.db"

HISTORY_STATUSES = ("All", "Eligible forecasts", "Scored", "Unscored forecasts", "Abstentions")


def load_shadow_history(
    path: str | Path = DEFAULT_SHADOW_DB, *, search: str = "",
    symbol: str = "", formula_id: str = "", status: str = "All",
    start_date: date | None = None, end_date: date | None = None,
    page: int = 1, page_size: int = 50,
) -> dict[str, Any]:
    """Search the whole retained journal, returning only a bounded page, read-only."""
    if status not in HISTORY_STATUSES:
        raise ValueError("Unknown shadow history status")
    if start_date and end_date and start_date > end_date:
        raise ValueError("Start date must be on or before end date")
    page_size = min(200, max(1, int(page_size)))
    empty = {"rows": [], "total": 0, "page": 1, "pages": 1,
             "page_size": page_size, "symbols": [], "formulas": []}
    database_path = Path(path).resolve()
    if not database_path.is_file():
        return empty
    connection = sqlite3.connect(database_path.as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    try:
        # Keep count and page consistent while the live journal is appending.
        connection.execute("BEGIN")
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='shadow_prediction_journal'").fetchone() is None:
            return empty
        clauses, params = [], []
        for column, value in (("symbol", symbol), ("formula_id", formula_id)):
            if value:
                clauses.append(f"{column} = ?")
                params.append(value)
        status_clause = {
            "Eligible forecasts": "abstained = 0",
            "Scored": "abstained = 0 AND realized_price IS NOT NULL",
            "Unscored forecasts": "abstained = 0 AND realized_price IS NULL",
            "Abstentions": "abstained = 1",
        }.get(status)
        if status_clause:
            clauses.append(status_clause)
        if start_date:
            clauses.append("prediction_timestamp_utc >= ?")
            params.append(start_date.isoformat())
        if end_date:
            clauses.append("prediction_timestamp_utc < ?")
            params.append((end_date + timedelta(days=1)).isoformat())
        if search.strip():
            # instr treats %, _, quotes and other search text literally.
            clauses.append("(" + " OR ".join(
                f"instr(lower(COALESCE({column}, '')), lower(?)) > 0"
                for column in ("prediction_id", "symbol", "formula_id", "formula_version",
                               "abstention_reasons_json")
            ) + ")")
            params.extend([search.strip()] * 5)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        total = connection.execute(
            "SELECT COUNT(*) FROM shadow_prediction_journal" + where, params
        ).fetchone()[0]
        pages = max(1, (total + page_size - 1) // page_size)
        page = min(pages, max(1, int(page)))
        rows = connection.execute(
            """SELECT prediction_id, symbol, formula_id, formula_version,
                      prediction_timestamp_utc, target_timestamp_utc, horizon_seconds,
                      spot, predicted_price, abstained, abstention_reasons_json,
                      realized_price, realized_timestamp_utc, absolute_error_points, direction_hit
               FROM shadow_prediction_journal""" + where +
            " ORDER BY prediction_timestamp_utc DESC, prediction_id LIMIT ? OFFSET ?",
            [*params, page_size, (page - 1) * page_size],
        ).fetchall()
        return {"rows": [dict(row) for row in rows], "total": total,
                "page": page, "pages": pages, "page_size": page_size,
                "symbols": [r[0] for r in connection.execute(
                    "SELECT DISTINCT symbol FROM shadow_prediction_journal ORDER BY symbol")],
                "formulas": [r[0] for r in connection.execute(
                    "SELECT DISTINCT formula_id FROM shadow_prediction_journal ORDER BY formula_id")]}
    finally:
        connection.close()


def shadow_history_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        abstained = bool(row["abstained"])
        scored = not abstained and row["realized_price"] is not None
        result.append({
            "Symbol": row["symbol"],
            "Formula": f"{row['formula_id']}:{row['formula_version']}",
            "Forecast time (UTC)": row["prediction_timestamp_utc"],
            "Target time (UTC)": row["target_timestamp_utc"],
            "Horizon (minutes)": row["horizon_seconds"] / 60,
            "Status": "ABSTAIN" if abstained else "SCORED" if scored else "UNSCORED",
            "Spot at forecast": row["spot"],
            "Forecast price": None if abstained else row["predicted_price"],
            "Observed outcome": row["realized_price"] if scored else None,
            "Outcome time (UTC)": row["realized_timestamp_utc"] if scored else None,
            "Absolute error (points)": row["absolute_error_points"] if scored else None,
            "Direction correct": bool(row["direction_hit"]) if scored and row["direction_hit"] is not None else None,
            "Reason": "; ".join(json.loads(row["abstention_reasons_json"] or "[]")),
            "Forecast ID": row["prediction_id"],
        })
    return result


def render_shadow_history(path: str | Path, *, symbols: list[str], formulas: list[str]) -> None:
    with st.expander("Search shadow forecast history", expanded=False):
        st.caption("Search the full retained journal. Historical research only; these are not current signals. "
                   "Each target time is measured from the original forecast time. All dates and times are UTC.")
        search = st.text_input("Search forecast ID, formula, symbol or abstention reason", key="shadow_history_search")
        columns = st.columns(3)
        symbol = columns[0].selectbox("History symbol", ["All", *symbols], key="shadow_history_symbol")
        formula = columns[1].selectbox("History formula", ["All", *formulas], key="shadow_history_formula")
        status = columns[2].selectbox("History status", HISTORY_STATUSES, key="shadow_history_status")
        columns = st.columns(3)
        start = columns[0].date_input("Forecast start date (UTC)", value=None, key="shadow_history_start")
        end = columns[1].date_input("Forecast end date (UTC)", value=None, key="shadow_history_end")
        size = columns[2].selectbox("Rows per page", [25, 50, 100], index=1, key="shadow_history_size")
        signature = (str(path), search, symbol, formula, status, start, end, size)
        if st.session_state.get("shadow_history_filters") != signature:
            st.session_state["shadow_history_page"] = 1
            st.session_state["shadow_history_filters"] = signature
        if start and end and start > end:
            st.warning("Start date must be on or before end date.")
            return
        try:
            history = load_shadow_history(
                path, search=search, symbol="" if symbol == "All" else symbol,
                formula_id="" if formula == "All" else formula, status=status,
                start_date=start, end_date=end, page_size=size,
                page=st.session_state.get("shadow_history_page", 1),
            )
        except sqlite3.Error:
            st.warning("Shadow history is temporarily unavailable. Refresh to try again.")
            return
        st.session_state["shadow_history_page"] = history["page"]
        st.number_input("History page", min_value=1, max_value=history["pages"],
                        step=1, key="shadow_history_page")
        if not history["rows"]:
            st.info("No shadow journal rows match these filters.")
            return
        first = (history["page"] - 1) * size + 1
        st.caption(f"{history['total']:,} matching rows · Showing {first:,}–{first + len(history['rows']) - 1:,} "
                   f"· Page {history['page']} of {history['pages']}")
        st.dataframe(shadow_history_display_rows(history["rows"]), hide_index=True, width="stretch")
        st.caption("Scored means an outcome was recorded, not that the forecast was correct. "
                   "Unscored forecasts may be awaiting their target time or lack a matching outcome.")


def load_shadow_research_state(
    path: str | Path = DEFAULT_SHADOW_DB,
    *,
    limit: int = 100,
) -> dict[str, Any]:
    """Load journal evidence without creating or mutating the database."""
    database_path = Path(path)
    if not database_path.exists():
        return {"present": False, "path": str(database_path), "summary": {}, "rows": []}

    connection = sqlite3.connect(f"file:{database_path.resolve().as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_prediction_journal'"
        ).fetchone()
        if table is None:
            return {"present": True, "path": str(database_path), "summary": {}, "rows": []}
        summary_row = connection.execute(
            """
            SELECT
                COUNT(*) AS journal_rows,
                SUM(CASE WHEN abstained = 0 THEN 1 ELSE 0 END) AS eligible_forecasts,
                SUM(CASE WHEN abstained = 1 THEN 1 ELSE 0 END) AS abstentions,
                SUM(CASE WHEN realized_price IS NOT NULL THEN 1 ELSE 0 END) AS scored_outcomes,
                SUM(CASE WHEN production_signal_replaced != 0 THEN 1 ELSE 0 END) AS production_replacements,
                MAX(prediction_timestamp_utc) AS latest_prediction_utc
            FROM shadow_prediction_journal
            """
        ).fetchone()
        rows = connection.execute(
            """
            SELECT *
            FROM shadow_prediction_journal
            ORDER BY prediction_timestamp_utc DESC, symbol, formula_id
            LIMIT ?
            """,
            (max(1, int(limit)),),
        ).fetchall()
        return {
            "present": True,
            "path": str(database_path),
            "summary": dict(summary_row) if summary_row else {},
            "rows": [dict(row) for row in rows],
            "symbols": [r[0] for r in connection.execute(
                "SELECT DISTINCT symbol FROM shadow_prediction_journal ORDER BY symbol")],
            "formulas": [r[0] for r in connection.execute(
                "SELECT DISTINCT formula_id FROM shadow_prediction_journal ORDER BY formula_id")],
        }
    finally:
        connection.close()


def _latest_by_formula_and_symbol(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("symbol") or ""),
            str(row.get("formula_id") or ""),
            str(row.get("formula_version") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        latest.append(row)
    return latest


def render_shadow_research_panel(path: str | Path = DEFAULT_SHADOW_DB) -> None:
    """Render clearly labeled shadow evidence; never expose it as production."""
    st.divider()
    st.subheader("🧪 Shadow Formula Research — No Production Impact")
    st.warning(
        "Research only: these five-minute formulas cannot replace the primary signal, "
        "place orders, trigger trades, or promote themselves. Invalid inputs produce an abstention."
    )
    state = load_shadow_research_state(path)
    if not state["present"] or not state["rows"]:
        st.info(
            "No live shadow journal records are available yet. The offline formula contract remains "
            "preregistered and will begin logging only after the backend runs this source version."
        )
        return

    summary = state["summary"]
    metric_columns = st.columns(4)
    metric_columns[0].metric("Journal Rows", int(summary.get("journal_rows") or 0))
    metric_columns[1].metric("Eligible Forecasts", int(summary.get("eligible_forecasts") or 0))
    metric_columns[2].metric("Abstentions", int(summary.get("abstentions") or 0))
    metric_columns[3].metric("Scored Outcomes", int(summary.get("scored_outcomes") or 0))
    if int(summary.get("production_replacements") or 0):
        st.error("Safety invariant failed: a shadow row reports production replacement.")

    display_rows = []
    latest_rows = _latest_by_formula_and_symbol(state["rows"])
    for row in latest_rows:
        abstained = bool(row.get("abstained"))
        reasons = json.loads(row.get("abstention_reasons_json") or "[]")
        predicted = row.get("predicted_price")
        confidence = float(row.get("confidence") or 0.0)
        display_rows.append({
            "Symbol": row.get("symbol"),
            "Formula": f"{row.get('formula_id')}:{row.get('formula_version')}",
            "Observed UTC": row.get("prediction_timestamp_utc"),
            "Spot": row.get("spot"),
            "Shadow 5m Price": None if abstained else predicted,
            "Status": "ABSTAIN" if abstained else "SHADOW ONLY",
            "Data-quality confidence": f"{confidence * 100:.1f}%",
            "Reason": "; ".join(str(reason) for reason in reasons) if reasons else "Eligible",
        })
    st.dataframe(display_rows, hide_index=True, width="stretch")
    st.caption(
        "Confidence is a capped data-quality heuristic, not calibrated forecast probability. "
        "The candidate is preregistered, unfitted, and requires later walk-forward evidence plus explicit approval."
    )

    render_shadow_history(path, symbols=state["symbols"], formulas=state["formulas"])

    with st.expander("Formula equations and versions", expanded=False):
        formulas: dict[tuple[str, str], dict[str, Any]] = {}
        for row in latest_rows:
            key = (str(row.get("formula_id")), str(row.get("formula_version")))
            formulas.setdefault(key, row)
        for (formula_id, version), row in formulas.items():
            st.markdown(f"**{formula_id}:{version}**")
            st.code(str(row.get("equation") or "Equation unavailable"), language="text")
