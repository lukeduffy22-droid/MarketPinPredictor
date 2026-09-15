from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Mapping

from .config import UTC, build_session_config
from .contracts import (
    EVIDENCE_CONTRACT_VERSION,
    HISTORICAL_SOURCE_KIND,
    LIVE_SOURCE_KIND,
    evidence_contract_for_source_kind,
)
from .finalize import finalize_closed_session
from .replay import REPLAY_DECODER_VERSION


def _unchanged_incomplete_audit(
    *,
    config,
    session: sqlite3.Row,
    feed: sqlite3.Row,
    audit: sqlite3.Row,
) -> dict[str, object] | None:
    """Reuse an incomplete audit only while all cheap retained evidence is unchanged."""
    try:
        attempted_at = datetime.fromisoformat(str(audit["attempted_at_utc"]))
        if attempted_at.tzinfo is None:
            return None
        attempted_ns = int(attempted_at.timestamp() * 1_000_000_000)
        report = json.loads(str(audit["report_json"]))
        issues = json.loads(str(audit["issues_json"]))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(report, Mapping) or not isinstance(issues, list):
        return None
    integrity = report.get("integrity")
    performance = report.get("performance")
    if not isinstance(integrity, Mapping) or not isinstance(performance, Mapping):
        return None
    source_hash = str(audit["source_sha256"] or "")
    if (
        bool(report.get("complete"))
        or str(report.get("session_id") or "") != str(session["session_id"])
        or str(report.get("feed_name") or "") != "opra_options"
        or str(report.get("source_sha256") or "") != source_hash
        or str(integrity.get("sha256") or "") != source_hash
        or str(feed["sha256"] or "") != source_hash
        or str(performance.get("replay_decoder_version") or "")
        != REPLAY_DECODER_VERSION
    ):
        return None

    source = Path(str(feed["dbn_path"])).resolve()
    try:
        source_stat = source.stat()
    except OSError:
        return None
    if (
        not source.is_file()
        or source_stat.st_size != int(integrity.get("file_bytes") or -1)
        or source_stat.st_size != int(feed["file_bytes"] or -1)
        or source_stat.st_mtime_ns > attempted_ns
        or Path(str(report.get("source_path") or "")).resolve() != source
    ):
        return None

    # status.json is the recorder's retained terminal artifact. A later write is
    # new evidence even when the DBN byte count and prior hash happen to match.
    status_path = Path(config.status_path).resolve()
    try:
        status_stat = status_path.stat()
        status_payload = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if not status_path.is_file() or status_stat.st_mtime_ns > attempted_ns:
        return None
    if not isinstance(status_payload, Mapping):
        return None
    status_session = status_payload.get("session")
    status_feeds = status_payload.get("feeds")
    if not isinstance(status_session, Mapping) or not isinstance(status_feeds, list):
        return None
    if str(status_session.get("session_id") or "") != str(session["session_id"]):
        return None
    status_feed = next(
        (
            item
            for item in status_feeds
            if isinstance(item, Mapping) and item.get("feed_name") == "opra_options"
        ),
        None,
    )
    if status_feed is None:
        return None
    if (
        Path(str(status_feed.get("dbn_path") or "")).resolve() != source
        or status_feed.get("status") != audit["prior_status"]
        or status_feed.get("error") != audit["prior_error"]
        or status_feed.get("gaps_json") != audit["prior_gaps_json"]
    ):
        return None

    expected_error = "; ".join(str(issue) for issue in issues) if issues else None
    warnings = report.get("operational_warnings")
    if not isinstance(warnings, list):
        return None
    expected_gaps = json.dumps(
        [
            *({"operational_warning": str(warning)} for warning in warnings),
            *({"offline_finalization_issue": str(issue)} for issue in issues),
        ],
        separators=(",", ":"),
    )
    if (
        str(feed["status"] or "") != "incomplete"
        or bool(feed["complete"])
        or feed["error"] != expected_error
        or str(feed["gaps_json"] or "") != expected_gaps
    ):
        return None
    for field in (
        "records_seen",
        "trade_records",
        "tcbbo_records",
        "tcbbo_timestamped_records",
        "tcbbo_valid_nbbo_records",
        "tcbbo_flagged_records",
        "mapping_records",
        "statistics_records",
        "definition_records",
        "first_event_ns",
        "last_event_ns",
        "last_receive_ns",
        "subscription_acks",
        "replay_completed",
        "file_bytes",
    ):
        if feed[field] != integrity.get(field):
            return None
    return {
        "action": "incomplete",
        "session_id": str(session["session_id"]),
        "source_sha256": source_hash,
        "complete": False,
        "issues": [str(issue) for issue in issues],
        "reason": "unchanged incomplete post-close evidence already audited",
        "deduplicated": True,
        "attempted_at_utc": str(audit["attempted_at_utc"]),
    }


def postclose_finalize_decision(
    project_root: str | Path,
    *,
    trading_day: date,
    now: datetime | None = None,
) -> dict[str, object]:
    root = Path(project_root).resolve()
    observed = now or datetime.now(UTC)
    if observed.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    observed = observed.astimezone(UTC)
    try:
        config = build_session_config(root, trading_day=trading_day, now=observed)
    except ValueError as exc:
        return {"action": "not_applicable", "reason": str(exc)}
    if observed < config.stop_due_utc:
        return {
            "action": "not_due",
            "reason": "configured options-session stop time has not passed",
            "stop_due_utc": config.stop_due_utc.isoformat(),
        }
    catalog_path = config.catalog_path
    if not catalog_path.is_file():
        return {"action": "missing", "reason": "closing-tape catalog is missing"}
    connection = sqlite3.connect(
        f"file:{catalog_path.resolve()}?mode=ro", uri=True, timeout=10.0
    )
    connection.row_factory = sqlite3.Row
    try:
        session = connection.execute(
            """
            SELECT * FROM tape_sessions
            WHERE trading_date=? ORDER BY created_at_utc DESC LIMIT 1
            """,
            (trading_day.isoformat(),),
        ).fetchone()
        if session is None:
            return {"action": "missing", "reason": "no session exists for the trading date"}
        session_id = str(session["session_id"])
        feed = connection.execute(
            """
            SELECT * FROM tape_feed_status
            WHERE session_id=? AND feed_name='opra_options'
            """,
            (session_id,),
        ).fetchone()
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        feed_columns = set(feed.keys()) if feed is not None else set()
        source_kind = (
            str(feed["source_kind"] or LIVE_SOURCE_KIND).lower()
            if feed is not None and "source_kind" in feed_columns
            else LIVE_SOURCE_KIND
        )
        try:
            expected_contract = evidence_contract_for_source_kind(source_kind)
        except ValueError as exc:
            return {"action": "blocked", "session_id": session_id, "reason": str(exc)}
        evidence_contract = (
            str(feed["evidence_contract_version"] or "")
            if feed is not None and "evidence_contract_version" in feed_columns
            else EVIDENCE_CONTRACT_VERSION
        )
        if evidence_contract != expected_contract:
            return {
                "action": "blocked",
                "session_id": session_id,
                "reason": "evidence contract does not match source kind",
            }
        if feed is not None and feed["sha256"] and "tape_finalization_runs" in tables:
            audit = connection.execute(
                """
                SELECT 1 FROM tape_finalization_runs
                WHERE session_id=? AND feed_name='opra_options' AND source_sha256=?
                  AND evidence_contract_version=? AND complete=1 LIMIT 1
                """,
                (session_id, str(feed["sha256"]), evidence_contract),
            ).fetchone()
            if audit is not None:
                return {
                    "action": "already_finalized",
                    "session_id": session_id,
                    "source_sha256": str(feed["sha256"]),
                }
        if source_kind == HISTORICAL_SOURCE_KIND:
            return {
                "action": "historical_incomplete",
                "session_id": session_id,
                "reason": "historical evidence requires the guarded bundle importer",
            }
        if feed is not None and feed["sha256"] and "tape_finalization_runs" in tables:
            incomplete_audits = connection.execute(
                """
                SELECT * FROM tape_finalization_runs
                WHERE session_id=? AND feed_name='opra_options' AND source_sha256=?
                  AND evidence_contract_version=? AND complete=0
                ORDER BY attempted_at_utc DESC
                """,
                (session_id, str(feed["sha256"]), evidence_contract),
            ).fetchall()
            for audit in incomplete_audits:
                unchanged = _unchanged_incomplete_audit(
                    config=config,
                    session=session,
                    feed=feed,
                    audit=audit,
                )
                if unchanged is not None:
                    return unchanged
        return {"action": "finalize", "session_id": session_id}
    finally:
        connection.close()


def finalize_closed_session_if_due(
    project_root: str | Path,
    *,
    trading_day: date,
    now: datetime | None = None,
) -> dict[str, object]:
    decision = postclose_finalize_decision(
        project_root, trading_day=trading_day, now=now
    )
    if decision["action"] != "finalize":
        return decision
    report = finalize_closed_session(
        project_root,
        trading_date=trading_day.isoformat(),
        session_id=str(decision["session_id"]),
        rebuild_minutes=True,
        now=now,
    )
    return {
        "action": "finalized" if report["complete"] else "incomplete",
        **report,
    }
