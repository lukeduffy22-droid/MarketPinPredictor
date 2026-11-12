"""Read-only Streamlit view for the isolated shadow-prediction journal."""

from __future__ import annotations

import json
import math
import sqlite3
import time
from datetime import datetime, timezone
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import streamlit as st
from backend.research.outcome_protocol import assess_outcome, register_outcome_sql
from app.services.evidence_stages import evidence_cache_context


DEFAULT_SHADOW_DB = Path(__file__).resolve().parents[2] / "data" / "shadow_research.db"

HISTORY_STATUSES = ("All", "Eligible forecasts", "Scored", "Quarantined outcomes", "Unscored forecasts", "Abstentions")


def _prepare_read(connection, *, budget_seconds=2.5):
    """Bound both lock wait and computation without touching the journal."""
    deadline = time.monotonic() + budget_seconds
    connection.execute("PRAGMA query_only=ON")
    connection.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
    register_outcome_sql(connection)
    connection.execute("BEGIN")


def _score_expression(connection):
    columns = {row[1] for row in connection.execute("PRAGMA table_info(shadow_prediction_journal)")}
    created = "created_at_utc" if "created_at_utc" in columns else "NULL"
    return created, f"outcome_eligible(prediction_timestamp_utc,target_timestamp_utc,{created},realized_timestamp_utc)"


def _has_outcome_observations(connection):
    return connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_outcome_observations'").fetchone() is not None


def _reason_list(raw):
    try:
        values = json.loads(raw or "[]")
        if isinstance(values, list) and all(isinstance(value, str) for value in values):
            return values
    except (ValueError, TypeError):
        pass
    return ["MALFORMED_RETAINED_REASONS"]


def _quality_score(value):
    if isinstance(value, bool):
        return "Unavailable"
    try:
        score = float(value)
        return f"{score * 100:.1f}/100" if math.isfinite(score) and 0 <= score <= 1 else "Unavailable"
    except (ValueError, TypeError):
        return "Unavailable"


def _journal_identity(path):
    """Include WAL and inode replacement; path or size alone misses appends."""
    result = []
    for candidate in (Path(path), Path(str(path) + "-wal")):
        try:
            info = candidate.stat()
            result.append((info.st_ino, info.st_size, info.st_mtime_ns))
        except OSError:
            result.append(None)
    return tuple(result)


@st.cache_data(ttl=15, max_entries=32, show_spinner=False)
def _cached_shadow_state(path, source_identity, runtime_identity, limit):
    # Identity arguments intentionally participate in the cache key. This is
    # retained evidence only; a cached score never establishes live eligibility.
    return load_shadow_research_state(path, limit=limit)


def load_shadow_history(
    path: str | Path = DEFAULT_SHADOW_DB, *, search: str = "",
    symbol: str = "", formula_id: str = "", status: str = "All",
    start_date: date | None = None, end_date: date | None = None,
    page: int = 1, page_size: int = 50, include_inventory: bool = True,
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
    connection = sqlite3.connect(database_path.as_uri() + "?mode=ro", uri=True, timeout=0.1)
    connection.row_factory = sqlite3.Row
    try:
        # Keep count and page consistent while the live journal is appending.
        _prepare_read(connection)
        if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' "
                              "AND name='shadow_prediction_journal'").fetchone() is None:
            return empty
        created, _ = _score_expression(connection)
        created = "p." + created if created != "NULL" else created
        has_observations = _has_outcome_observations(connection)
        source = "shadow_prediction_journal p"
        if has_observations:
            source += " LEFT JOIN shadow_outcome_observations o ON o.prediction_id=p.prediction_id"
        price = "COALESCE(p.realized_price,o.observed_reference_price)" if has_observations else "p.realized_price"
        stamp = "COALESCE(p.realized_timestamp_utc,o.observed_at_utc)" if has_observations else "p.realized_timestamp_utc"
        score_expression = f"outcome_eligible(p.prediction_timestamp_utc,p.target_timestamp_utc,{created},{stamp})"
        clauses, params = [], []
        for column, value in (("symbol", symbol), ("formula_id", formula_id)):
            if value:
                clauses.append(f"p.{column} = ?")
                params.append(value)
        status_clause = {
            "Eligible forecasts": "abstained = 0",
            "Scored": f"abstained = 0 AND {price} IS NOT NULL AND {score_expression}=1",
            "Quarantined outcomes": f"abstained = 0 AND {price} IS NOT NULL AND {score_expression}=0",
            "Unscored forecasts": f"abstained = 0 AND {price} IS NULL",
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
                f"instr(lower(COALESCE(p.{column}, '')), lower(?)) > 0"
                for column in ("prediction_id", "symbol", "formula_id", "formula_version",
                               "abstention_reasons_json")
            ) + ")")
            params.extend([search.strip()] * 5)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        total = connection.execute(
            "SELECT COUNT(*) FROM " + source + where, params
        ).fetchone()[0]
        pages = max(1, (total + page_size - 1) // page_size)
        page = min(pages, max(1, int(page)))
        rows = connection.execute(
            f"""SELECT p.prediction_id, symbol, formula_id, formula_version, {created} AS created_at_utc,
                      prediction_timestamp_utc, target_timestamp_utc, horizon_seconds,
                      spot, predicted_price, abstained, abstention_reasons_json,
                      {price} AS realized_price, {stamp} AS realized_timestamp_utc, absolute_error_points, direction_hit
               FROM """ + source + where +
            " ORDER BY prediction_timestamp_utc DESC, p.prediction_id LIMIT ? OFFSET ?",
            [*params, page_size, (page - 1) * page_size],
        ).fetchall()
        return {"rows": [dict(row) for row in rows], "total": total,
                "page": page, "pages": pages, "page_size": page_size,
                "symbols": [r[0] for r in connection.execute(
                    "SELECT DISTINCT symbol FROM shadow_prediction_journal ORDER BY symbol")] if include_inventory else [],
                "formulas": [r[0] for r in connection.execute(
                    "SELECT DISTINCT formula_id FROM shadow_prediction_journal ORDER BY formula_id")] if include_inventory else []}
    finally:
        connection.close()


def shadow_history_display_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        abstained = bool(row["abstained"])
        observed = not abstained and row["realized_price"] is not None
        assessment = assess_outcome(row) if observed else None
        scored = observed and assessment["status"] == "ELIGIBLE"
        result.append({
            "Symbol": row["symbol"],
            "Formula": f"{row['formula_id']}:{row['formula_version']}",
            "Forecast time (UTC)": row["prediction_timestamp_utc"],
            "Target time (UTC)": row["target_timestamp_utc"],
            "Horizon (minutes)": row["horizon_seconds"] / 60,
            "Status": "ABSTAIN" if abstained else "SCORED" if scored else "QUARANTINED" if observed else "UNSCORED",
            "Spot at forecast": row["spot"],
            "Forecast price": None if abstained else row["predicted_price"],
            "Observed outcome": row["realized_price"] if observed else None,
            "Outcome kind": "Option-derived reference; not an official close",
            "Outcome time (UTC)": row["realized_timestamp_utc"] if observed else None,
            "Target lag (seconds)": assessment["target_lag_seconds"] if assessment else None,
            "Absolute error (points)": row["absolute_error_points"] if scored else None,
            "Direction correct": bool(row["direction_hit"]) if scored and row["direction_hit"] is not None else None,
            "Reason": "; ".join([*_reason_list(row["abstention_reasons_json"]),
                                   *(assessment["reasons"] if assessment else [])]),
            "Forecast ID": row["prediction_id"],
        })
    return result


def render_shadow_history(path: str | Path, *, symbols: list[str], formulas: list[str]) -> None:
    with st.expander("Search shadow forecast history", expanded=False):
        if not st.toggle("Load shadow history", value=False, key="load_shadow_history"):
            st.caption("History queries are paused until enabled.")
            return
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
                include_inventory=False,  # The summary already supplied the full inventory.
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
        st.caption("Scored requires persistence before the target and a reference observation within 30 seconds after it. "
                   "Quarantined outcomes are excluded from accuracy. These option-derived references are not official index closes. "
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

    connection = None
    try:
        connection = sqlite3.connect(database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)
        connection.row_factory = sqlite3.Row
        _prepare_read(connection)
        table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='shadow_prediction_journal'"
        ).fetchone()
        if table is None:
            return {"present": True, "path": str(database_path), "summary": {}, "rows": []}
        created, score_expression = _score_expression(connection)
        summary_row = connection.execute(
            f"""
            SELECT
                COUNT(*) AS journal_rows,
                SUM(CASE WHEN abstained = 0 THEN 1 ELSE 0 END) AS eligible_forecasts,
                SUM(CASE WHEN abstained = 1 THEN 1 ELSE 0 END) AS abstentions,
                SUM(CASE WHEN abstained=0 AND realized_price IS NOT NULL AND {score_expression}=1 THEN 1 ELSE 0 END) AS scored_outcomes,
                SUM(CASE WHEN abstained=0 AND realized_price IS NOT NULL THEN 1 ELSE 0 END) AS observed_outcomes,
                SUM(CASE WHEN production_signal_replaced != 0 THEN 1 ELSE 0 END) AS production_replacements,
                MAX(prediction_timestamp_utc) AS latest_prediction_utc,
                json_group_array(DISTINCT symbol) AS symbols_json,
                json_group_array(DISTINCT formula_id) AS formulas_json
            FROM shadow_prediction_journal
            """
        ).fetchone()
        summary = dict(summary_row) if summary_row else {}
        summary["quarantined_outcomes"] = int(summary.get("observed_outcomes") or 0) - int(summary.get("scored_outcomes") or 0)
        # New quarantines preserve the original journal's NULL outcome. Count
        # those observations separately without double-counting legacy scores.
        if _has_outcome_observations(connection):
            created_column = "p." + created if created != "NULL" else created
            extra = connection.execute("SELECT COUNT(*) FROM shadow_outcome_observations o "
                "JOIN shadow_prediction_journal p ON p.prediction_id=o.prediction_id "
                "WHERE p.abstained=0 AND p.realized_price IS NULL AND "
                f"outcome_eligible(p.prediction_timestamp_utc,p.target_timestamp_utc,{created_column},o.observed_at_utc)=0").fetchone()[0]
            summary["quarantined_outcomes"] = int(summary.get("quarantined_outcomes") or 0) + extra
        symbols = sorted(value for value in json.loads(summary.pop("symbols_json") or "[]") if isinstance(value, str))
        formulas = sorted(value for value in json.loads(summary.pop("formulas_json") or "[]") if isinstance(value, str))
        rows = connection.execute(
            """
            SELECT prediction_id, symbol, formula_id, formula_version,
                   prediction_timestamp_utc, spot, predicted_price, abstained,
                   abstention_reasons_json, confidence, equation
            FROM shadow_prediction_journal
            ORDER BY prediction_timestamp_utc DESC, symbol, formula_id
            LIMIT ?
            """,
            (min(200, max(1, int(limit))),),
        ).fetchall()
        return {
            "present": True,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "path": str(database_path),
            "summary": summary,
            "rows": [dict(row) for row in rows],
            "symbols": symbols,
            "formulas": formulas,
        }
    except sqlite3.Error as exc:
        return {"present": True, "path": str(database_path), "summary": {}, "rows": [],
                "error": "SHADOW_READ_TIMEOUT" if "interrupt" in str(exc).lower() else "SHADOW_READ_UNAVAILABLE"}
    finally:
        if connection is not None:
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


def render_shadow_research_panel(path: str | Path = DEFAULT_SHADOW_DB, *, runtime_context=None) -> None:
    """Render clearly labeled shadow evidence; never expose it as production."""
    st.divider()
    st.subheader("🧪 Shadow Formula Research — No Production Impact")
    st.warning(
        "Research only: these five-minute formulas cannot replace the primary signal, "
        "place orders, trigger trades, or promote themselves. Invalid inputs produce an abstention."
    )
    runtime = runtime_context if isinstance(runtime_context, dict) else {}
    runtime_identity = evidence_cache_context(runtime, datetime.now(timezone.utc).date())
    if st.button("Refresh shadow evidence", key="refresh_shadow_evidence"):
        _cached_shadow_state.clear()
    source = str(Path(path).resolve())
    state = _cached_shadow_state(source, _journal_identity(source), runtime_identity, 100)
    if state.get("error"):
        st.warning(f"Retained shadow evidence is unavailable ({state['error']}). No zero counts or accuracy are inferred.")
        return
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
    eligible = int(summary.get("eligible_forecasts") or 0)
    scored = int(summary.get("scored_outcomes") or 0)
    st.caption(f"Outcome coverage: {scored:,}/{eligible:,} eligible forecasts · "
               f"Quarantined recorded outcomes: {int(summary.get('quarantined_outcomes') or 0):,}. "
               "Scored outcomes use a 30-second target tolerance and an option-derived reference, not an official close.")
    st.caption(f"Retained evidence observed at {state.get('observed_at_utc', 'unavailable')} · "
               "Cache expires after 15 seconds and is invalidated by database, WAL or runtime generation changes.")
    if int(summary.get("production_replacements") or 0):
        st.error("Safety invariant failed: a shadow row reports production replacement.")

    display_rows = []
    latest_rows = _latest_by_formula_and_symbol(state["rows"])
    for row in latest_rows:
        abstained = bool(row.get("abstained"))
        reasons = _reason_list(row.get("abstention_reasons_json"))
        predicted = row.get("predicted_price")
        display_rows.append({
            "Symbol": row.get("symbol"),
            "Formula": f"{row.get('formula_id')}:{row.get('formula_version')}",
            "Observed UTC": row.get("prediction_timestamp_utc"),
            "Spot": row.get("spot"),
            "Shadow 5m Price": None if abstained else predicted,
            "Status": "ABSTAIN" if abstained else "SHADOW ONLY",
            "Data-quality score (uncalibrated)": _quality_score(row.get("confidence")),
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
