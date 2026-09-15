from __future__ import annotations

import hashlib
import json
from time import perf_counter
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from .catalog import TapeCatalog
from .config import UTC
from .contracts import EVIDENCE_CONTRACT_VERSION, LIVE_SOURCE_KIND
from .definition_replay import replay_instrument_definitions
from .integrity import inspect_dbn
from .live_recorder import (
    FAMILY_ROOTS,
    ProcessFileLock,
    excessive_unmapped_trade_ratio,
    tcbbo_integrity_issues,
)
from .oi_replay import replay_open_interest
from .parity import estimate_tcbbo_parity_reference_prices, persist_parity_reference_prices
from .replay import REPLAY_DECODER_VERSION, build_minute_rows, persist_minute_rows


PARITY_PARAMETERS = {
    "minimum_pairs": 5,
    "max_pair_age_seconds": 5.0,
    "annual_rate": 0.0,
    "settlement_hour_utc": 20,
    "excluded_families": ["VIX"],
}
MONITOR_PAUSE_PREFIX = "recorder monitor paused for "


def _postclose_retry_file_identity(
    path: str | Path, *, include_sha256: bool = False
) -> dict[str, object]:
    """Return a cheap generation identity for retained post-close evidence."""
    resolved = Path(path).resolve()
    try:
        stat = resolved.stat()
    except OSError:
        return {"path": str(resolved), "exists": False}
    identity: dict[str, object] = {
        "path": str(resolved),
        "exists": resolved.is_file(),
        "file_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    if include_sha256 and identity["exists"]:
        digest = hashlib.sha256()
        with resolved.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
        identity["sha256"] = digest.hexdigest()
    return identity


def _monitor_pause_reasons(feed: Mapping[str, object]) -> tuple[str, ...]:
    """Extract monitor-pause warnings from both recorder evidence formats."""
    try:
        gaps = json.loads(str(feed["gaps_json"] or "[]"))
    except json.JSONDecodeError:
        return ()
    if not isinstance(gaps, list):
        return ()
    reasons: list[str] = []
    for gap in gaps:
        if not isinstance(gap, dict):
            continue
        for key in ("operational_warning", "derived_incomplete"):
            if set(gap) != {key}:
                continue
            reason = str(gap.get(key) or "")
            if reason.startswith(MONITOR_PAUSE_PREFIX):
                reasons.append(reason)
    return tuple(dict.fromkeys(reasons))


def _recoverable_monitor_pause_reasons(feed: Mapping[str, object]) -> tuple[str, ...]:
    """Identify local monitor starvation that did not assert raw/provider loss."""
    warning_count = int(feed["slow_reader_warnings"] or 0)
    try:
        gaps = json.loads(str(feed["gaps_json"] or "[]"))
    except json.JSONDecodeError:
        return ()
    if not isinstance(gaps, list):
        return ()
    current_reasons: list[str] = []
    legacy_reasons: list[str] = []
    for gap in gaps:
        if not isinstance(gap, dict):
            return ()
        if set(gap) == {"operational_warning"}:
            reason = str(gap.get("operational_warning") or "")
            destination = current_reasons
        elif set(gap) == {"derived_incomplete"}:
            reason = str(gap.get("derived_incomplete") or "")
            destination = legacy_reasons
        else:
            return ()
        if not reason.startswith(MONITOR_PAUSE_PREFIX):
            return ()
        destination.append(reason)
    # Provider slow-reader messages increment the counter without entering the
    # gap list. Legacy monitor pauses did increment it, while current recorder
    # operational warnings deliberately do not. Any mismatch stays fail-closed.
    if len(legacy_reasons) != warning_count:
        return ()
    return tuple(dict.fromkeys([*current_reasons, *legacy_reasons]))


def _persist_contract_minute_rows(catalog, observed, inferred) -> None:
    """Use the stable catalog boundary when a recorder cached an older replay module."""
    catalog.upsert_observed_contract_minutes(observed)
    catalog.upsert_inferred_contract_minute_flow(inferred)


def _expected_roots(config: dict[str, Any], feed_name: str) -> set[str]:
    roots: set[str] = set()
    for feed in config.get("feeds") or []:
        if str(feed.get("name")) != feed_name:
            continue
        for subscription in feed.get("subscriptions") or []:
            if str(subscription.get("schema")) not in {"tcbbo", "tbbo", "trades"}:
                continue
            for symbol in subscription.get("symbols") or []:
                parent = str(symbol).upper().split(".", 1)[0]
                roots.add(FAMILY_ROOTS.get(parent, parent))
    return roots


def _feed_schemas(config: dict[str, Any], feed_name: str) -> list[str]:
    for feed in config.get("feeds") or []:
        if str(feed.get("name")) == feed_name:
            return [str(item.get("schema")) for item in feed.get("subscriptions") or []]
    return []


def finalize_closed_session(
    project_root: str | Path,
    *,
    trading_date: str,
    session_id: str | None = None,
    feed_name: str = "opra_options",
    rebuild_minutes: bool = False,
    now: datetime | None = None,
) -> dict[str, object]:
    """Upgrade a closed tape to the current evidence contract, idempotently."""
    root = Path(project_root).resolve()
    observed_at = now or datetime.now(UTC)
    if observed_at.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    observed_at = observed_at.astimezone(UTC)
    day_dir = root / "data" / "closing_tape" / trading_date
    catalog_path = day_dir / "closing_tape.sqlite"
    lock_path = day_dir / "recorder.lock"
    if not catalog_path.is_file():
        raise FileNotFoundError(catalog_path)

    with ProcessFileLock(lock_path):
        finalization_started = perf_counter()
        stage_seconds: dict[str, float] = {}
        catalog = TapeCatalog(catalog_path)
        with catalog.connect(read_only=True) as connection:
            session = connection.execute(
                """
                SELECT * FROM tape_sessions
                WHERE trading_date=? AND (? IS NULL OR session_id=?)
                ORDER BY created_at_utc DESC LIMIT 1
                """,
                (trading_date, session_id, session_id),
            ).fetchone()
            if session is None:
                raise ValueError("requested closing-tape session does not exist")
            selected_session = str(session["session_id"])
            feed = connection.execute(
                "SELECT * FROM tape_feed_status WHERE session_id=? AND feed_name=?",
                (selected_session, feed_name),
            ).fetchone()
            if feed is None:
                raise ValueError("requested closing-tape feed does not exist")
            source_kind = (
                str(feed["source_kind"] or LIVE_SOURCE_KIND).lower()
                if "source_kind" in feed.keys()
                else LIVE_SOURCE_KIND
            )
            if source_kind != LIVE_SOURCE_KIND:
                raise ValueError(
                    "live DBN finalizer cannot process historical evidence; "
                    "use the guarded historical bundle importer"
                )
        config = json.loads(str(session["config_json"]))
        schemas = _feed_schemas(config, feed_name)
        expected_roots = _expected_roots(config, feed_name)
        expected_acks = len(schemas)
        monitor_pause_warnings = _monitor_pause_reasons(feed)
        recoverable_monitor_pauses = _recoverable_monitor_pause_reasons(feed)
        source = Path(str(feed["dbn_path"])).resolve()
        try:
            source.relative_to(day_dir.resolve())
        except ValueError as exc:
            raise ValueError("catalog DBN path escapes the trading-day directory") from exc
        stage_started = perf_counter()
        integrity = inspect_dbn(
            source,
            require_trades=True,
            require_tcbbo="tcbbo" in schemas,
            expected_subscription_acks=expected_acks,
        )
        stage_seconds["integrity"] = perf_counter() - stage_started
        issues = list(integrity.incomplete_reasons)
        if integrity.subscription_acks < expected_acks:
            issues.append(
                f"subscription acknowledgements incomplete ({integrity.subscription_acks}/{expected_acks})"
            )
        if integrity.replay_completed < expected_acks:
            issues.append(
                f"schema replay incomplete ({integrity.replay_completed}/{expected_acks})"
            )
        if "tcbbo" in schemas:
            issues.extend(tcbbo_integrity_issues(integrity))
        if "statistics" in schemas and integrity.statistics_records <= 0:
            issues.append("statistics subscription produced no records")
        if "definition" in schemas and integrity.definition_records <= 0:
            issues.append("definition subscription produced no records")
        if int(feed["reconnect_count"] or 0) > 0:
            issues.append("catalog records reconnect gaps")
        if (
            int(feed["slow_reader_warnings"] or 0) > 0
            and not recoverable_monitor_pauses
        ):
            issues.append("catalog records slow-reader or derived-queue loss")
        if int(feed["provider_error_count"] or 0) > 0:
            issues.append("catalog records provider errors")
        if excessive_unmapped_trade_ratio(
            int(feed["unmapped_trade_records"] or 0),
            int(feed["trade_records"] or integrity.trade_records),
        ):
            issues.append("catalog unmapped-trade ratio exceeds 0.5 percent")

        definition_summary: dict[str, object] | None = None
        if "definition" in schemas and integrity.definition_records > 0:
            stage_started = perf_counter()
            definition_summary = replay_instrument_definitions(
                source,
                catalog=catalog,
                session_id=selected_session,
                feed_name=feed_name,
                source_sha256=integrity.sha256,
            )
            stage_seconds["definition_replay"] = perf_counter() - stage_started
            if int(definition_summary["records"] or 0) != int(integrity.definition_records):
                issues.append(
                    "instrument-definition replay count does not match capture integrity "
                    f"({definition_summary['records']}/{integrity.definition_records})"
                )

        oi_summary: dict[str, object] | None = None
        if "statistics" in schemas and integrity.statistics_records > 0:
            stage_started = perf_counter()
            oi_summary = replay_open_interest(
                source,
                catalog=catalog,
                session_id=selected_session,
                feed_name=feed_name,
                source_sha256=integrity.sha256,
            )
            stage_seconds["open_interest_replay"] = perf_counter() - stage_started
            if int(oi_summary["records"] or 0) <= 0:
                issues.append("no replayable open-interest observations")
            if int(oi_summary["unmapped_records"] or 0) > 0:
                issues.append(
                    "open-interest replay contains unmapped observations "
                    f"({oi_summary['unmapped_records']}/{oi_summary['records']})"
                )

        if rebuild_minutes:
            stage_started = perf_counter()
            (
                observed_rows, inferred_rows, contract_observed_rows,
                contract_inferred_rows, feature_hash,
            ) = build_minute_rows(
                source,
                session_id=selected_session,
                feed_name=feed_name,
                verified_source_sha256=integrity.sha256,
                verified_integrity_report=integrity,
                include_contract_rows=True,
            )
            stage_seconds["minute_build"] = perf_counter() - stage_started
            stage_started = perf_counter()
            persist_minute_rows(catalog, observed_rows, inferred_rows)
            _persist_contract_minute_rows(
                catalog, contract_observed_rows, contract_inferred_rows
            )
            stage_seconds["minute_persist"] = perf_counter() - stage_started
            contract_frame = pd.DataFrame(contract_observed_rows)
            contract_frame["trading_date"] = trading_date
            contract_frame["capture_integrity_verified"] = True
            contract_frame["source_sha256"] = integrity.sha256
            stage_started = perf_counter()
            parity_prices = estimate_tcbbo_parity_reference_prices(
                contract_frame,
                minimum_pairs=int(PARITY_PARAMETERS["minimum_pairs"]),
                max_pair_age_seconds=float(PARITY_PARAMETERS["max_pair_age_seconds"]),
                annual_rate=float(PARITY_PARAMETERS["annual_rate"]),
                settlement_hour_utc=int(PARITY_PARAMETERS["settlement_hour_utc"]),
                excluded_families=tuple(PARITY_PARAMETERS["excluded_families"]),
            )
            stage_seconds["parity_compute"] = perf_counter() - stage_started
            stage_started = perf_counter()
            persist_parity_reference_prices(
                catalog, parity_prices, feed_name=feed_name, parameters=PARITY_PARAMETERS
            )
            stage_seconds["parity_persist"] = perf_counter() - stage_started
            parity_summary = {
                "rows": int(len(parity_prices)),
                "families": sorted(parity_prices["family_root"].unique().tolist())
                if not parity_prices.empty else [],
            }
        else:
            feature_hash = None
            with catalog.connect(read_only=True) as connection:
                observed_count = connection.execute(
                    "SELECT COUNT(*) FROM tape_observed_minute WHERE session_id=? AND feed_name=?",
                    (selected_session, feed_name),
                ).fetchone()[0]
                inferred_count = connection.execute(
                    "SELECT COUNT(*) FROM tape_inferred_minute_flow WHERE session_id=? AND feed_name=?",
                    (selected_session, feed_name),
                ).fetchone()[0]
                contract_observed_count = connection.execute(
                    "SELECT COUNT(*) FROM tape_observed_contract_minute WHERE session_id=? AND feed_name=?",
                    (selected_session, feed_name),
                ).fetchone()[0]
                contract_inferred_count = connection.execute(
                    "SELECT COUNT(*) FROM tape_inferred_contract_minute_flow WHERE session_id=? AND feed_name=?",
                    (selected_session, feed_name),
                ).fetchone()[0]
                parity_rows = connection.execute(
                    """
                    SELECT family_root, COUNT(*) AS rows
                    FROM tape_inferred_reference_minute
                    WHERE session_id=? AND feed_name=?
                    GROUP BY family_root ORDER BY family_root
                    """,
                    (selected_session, feed_name),
                ).fetchall()
            parity_summary = {
                "rows": int(sum(int(row[1]) for row in parity_rows)),
                "families": [str(row[0]) for row in parity_rows],
            }
            if not observed_count or not inferred_count or not contract_observed_count or not contract_inferred_count:
                issues.append("minute evidence is missing; explicit rebuild is required")

        with catalog.connect() as connection:
            connection.execute(
                """
                UPDATE tape_inferred_minute_flow SET source_sha256=?
                WHERE session_id=? AND feed_name=?
                """,
                (integrity.sha256, selected_session, feed_name),
            )
            connection.execute(
                """
                UPDATE tape_inferred_contract_minute_flow SET source_sha256=?
                WHERE session_id=? AND feed_name=?
                """,
                (integrity.sha256, selected_session, feed_name),
            )
            roots_with_trades = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT DISTINCT family_root FROM tape_observed_minute
                    WHERE session_id=? AND feed_name=? AND trade_count>0
                    """,
                    (selected_session, feed_name),
                )
            }
            roots_with_oi = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT DISTINCT family_root FROM tape_open_interest
                    WHERE session_id=? AND feed_name=? AND family_root IS NOT NULL
                    """,
                    (selected_session, feed_name),
                )
            }
        for family in sorted(expected_roots - roots_with_trades):
            issues.append(f"{family} has no observed minute trade evidence")
        if "statistics" in schemas:
            for family in sorted(expected_roots - roots_with_oi):
                issues.append(f"{family} has no daily open-interest projection")

        cash_close = datetime.fromisoformat(str(config["cash_close_utc"]))
        if cash_close.tzinfo is None:
            cash_close = cash_close.replace(tzinfo=UTC)
        close_cutoff_ns = int((cash_close.timestamp() - 300) * 1e9)
        if observed_at < cash_close:
            issues.append("cash session has not closed; full-session finalization is premature")
        elif int(integrity.last_event_ns or 0) < close_cutoff_ns:
            issues.append("tape did not advance to the final five-minute close window")

        issues = list(dict.fromkeys(issues))
        complete = integrity.local_file_intact and not issues
        elapsed_before_audit = perf_counter() - finalization_started
        stage_seconds["pre_audit_total"] = elapsed_before_audit
        report = {
            "session_id": selected_session,
            "feed_name": feed_name,
            "source_path": str(source),
            "source_sha256": integrity.sha256,
            "complete": complete,
            "issues": issues,
            "operational_warnings": list(monitor_pause_warnings),
            "integrity": integrity.to_dict(),
            "instrument_definitions": definition_summary,
            "open_interest": oi_summary,
            "feature_hash": feature_hash,
            "inferred_parity_references": parity_summary,
            "performance": {
                "replay_decoder_version": REPLAY_DECODER_VERSION,
                "stage_seconds": {
                    key: round(value, 6) for key, value in stage_seconds.items()
                },
                "tcbbo_records_per_second": round(
                    integrity.tcbbo_records / stage_seconds["minute_build"], 3
                ) if stage_seconds.get("minute_build", 0) > 0 else None,
                "oi_records_per_second": round(
                    integrity.statistics_records / stage_seconds["open_interest_replay"], 3
                ) if stage_seconds.get("open_interest_replay", 0) > 0 else None,
                "definition_records_per_second": round(
                    integrity.definition_records / stage_seconds["definition_replay"], 3
                ) if stage_seconds.get("definition_replay", 0) > 0 else None,
                "redundant_integrity_scan_avoided": bool(rebuild_minutes),
            },
        }
        run_identity = {
            "session_id": selected_session,
            "feed_name": feed_name,
            "source_sha256": integrity.sha256,
            "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
            "replay_decoder_version": REPLAY_DECODER_VERSION,
            "rebuild_minutes": rebuild_minutes,
            "complete": complete,
            "issues": issues,
            "operational_warnings": list(monitor_pause_warnings),
            # A changed raw file, terminal status artifact, or catalog status
            # must produce a new audit key even when the resulting issue text
            # and raw SHA happen to be unchanged.
            "retry_evidence": {
                "source": _postclose_retry_file_identity(source),
                "status_artifact": _postclose_retry_file_identity(
                    config.get("status_path", day_dir / "status.json"),
                    include_sha256=True,
                ),
                "recorder_evidence": {
                    key: feed[key]
                    for key in (
                        "dataset",
                        "schemas_json",
                        "symbols_json",
                        "dbn_path",
                        "source_kind",
                        "evidence_contract_version",
                        "operational_counters_applicable",
                        "source_manifest_path",
                        "source_components_json",
                        "started_at_utc",
                        "provisional_tcbbo_records",
                        "provisional_tcbbo_timestamped_records",
                        "provisional_tcbbo_valid_nbbo_records",
                        "unmapped_trade_records",
                        "reconnect_count",
                        "provider_error_count",
                        "last_trade_event_ns",
                        "root_trade_watermarks_json",
                        "root_trade_counts_json",
                        "callback_queue_depth",
                    )
                    if key in feed.keys()
                },
            },
        }
        run_key = hashlib.sha256(
            json.dumps(run_identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        catalog.record_finalization_run(
            {
                "run_key": run_key,
                "session_id": selected_session,
                "feed_name": feed_name,
                "source_sha256": integrity.sha256,
                "evidence_contract_version": EVIDENCE_CONTRACT_VERSION,
                "attempted_at_utc": observed_at.isoformat(),
                "complete": int(complete),
                "issues_json": json.dumps(issues, separators=(",", ":")),
                "prior_status": feed["status"],
                "prior_error": feed["error"],
                "prior_gaps_json": feed["gaps_json"],
                "report_json": json.dumps(report, sort_keys=True, separators=(",", ":")),
            }
        )
        catalog.update_feed_status(
            selected_session,
            feed_name,
            {
                "status": "complete" if complete else "incomplete",
                "ended_at_utc": observed_at.isoformat(),
                "records_seen": integrity.records_seen,
                "trade_records": integrity.trade_records,
                "tcbbo_records": integrity.tcbbo_records,
                "tcbbo_timestamped_records": integrity.tcbbo_timestamped_records,
                "tcbbo_valid_nbbo_records": integrity.tcbbo_valid_nbbo_records,
                "tcbbo_flagged_records": integrity.tcbbo_flagged_records,
                "tcbbo_action_counts_json": json.dumps(dict(integrity.tcbbo_action_counts)),
                "mapping_records": integrity.mapping_records,
                "statistics_records": integrity.statistics_records,
                "definition_records": integrity.definition_records,
                "first_event_ns": integrity.first_event_ns,
                "last_event_ns": integrity.last_event_ns,
                "last_receive_ns": integrity.last_receive_ns,
                "subscription_acks": integrity.subscription_acks,
                "expected_subscription_acks": expected_acks,
                "replay_completed": integrity.replay_completed,
                "slow_reader_warnings": (
                    integrity.slow_reader_warnings
                    if recoverable_monitor_pauses
                    else max(
                        int(feed["slow_reader_warnings"] or 0),
                        int(integrity.slow_reader_warnings or 0),
                    )
                ),
                "file_bytes": integrity.file_bytes,
                "sha256": integrity.sha256,
                "complete": int(complete),
                "gaps_json": json.dumps(
                    [
                        *(
                            {"operational_warning": warning}
                            for warning in monitor_pause_warnings
                        ),
                        *(
                            {"offline_finalization_issue": issue}
                            for issue in issues
                        ),
                    ],
                    separators=(",", ":"),
                ),
                "error": "; ".join(issues) if issues else None,
            },
        )
        return report
