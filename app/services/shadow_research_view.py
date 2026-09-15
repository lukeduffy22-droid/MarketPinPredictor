"""Read-only Streamlit view for the isolated shadow-prediction journal."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import streamlit as st


DEFAULT_SHADOW_DB = Path(__file__).resolve().parents[2] / "data" / "shadow_research.db"


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

    with st.expander("Formula equations and versions", expanded=False):
        formulas: dict[tuple[str, str], dict[str, Any]] = {}
        for row in latest_rows:
            key = (str(row.get("formula_id")), str(row.get("formula_version")))
            formulas.setdefault(key, row)
        for (formula_id, version), row in formulas.items():
            st.markdown(f"**{formula_id}:{version}**")
            st.code(str(row.get("equation") or "Equation unavailable"), language="text")
