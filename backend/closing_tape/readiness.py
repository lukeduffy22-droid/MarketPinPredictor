from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .catalog_discovery import discover_closing_tape_catalogs
from .sqlite_io import sqlite_read_only_uri
from .close_evidence import (
    OFFICIAL_REFERENCE_HOSTS_BY_SOURCE,
    VERIFIED_CLOSE_SOURCES_BY_SYMBOL,
    resolve_verified_close_artifact,
    validate_official_close_reference,
)
from .contracts import (
    EVIDENCE_CONTRACT_VERSION,
    LIVE_SOURCE_KIND,
    evidence_contract_for_source_kind,
)


UTC = timezone.utc
PRODUCTION_FAMILIES = set(VERIFIED_CLOSE_SOURCES_BY_SYMBOL)
CAPTURE_GATE_SESSIONS_REQUIRED = 10
MODEL_SESSIONS_REQUIRED = 60
PAPER_SESSIONS_REQUIRED = 20


@dataclass(frozen=True)
class SessionReadiness:
    trading_date: str
    session_id: str
    catalog_path: str
    feed_status: str | None
    source_sha256: str | None
    eligible: bool
    reasons: tuple[str, ...]
    observed_families: tuple[str, ...]
    inferred_families: tuple[str, ...]
    open_interest_families: tuple[str, ...]
    observed_minutes: int
    inferred_minutes: int


@dataclass(frozen=True)
class CloseLabelRequirement:
    family_root: str
    approved_sources: tuple[str, ...]
    approved_reference_hosts: tuple[str, ...]


@dataclass(frozen=True)
class CloseLabelWorkItem:
    trading_date: str
    session_id: str
    source_sha256: str
    verified_families: tuple[str, ...]
    missing_families: tuple[str, ...]
    missing_requirements: tuple[CloseLabelRequirement, ...]


@dataclass(frozen=True)
class TrainingReadinessReport:
    generated_at_utc: str
    catalogs: int
    catalog_roots: tuple[str, ...]
    catalog_paths: tuple[str, ...]
    catalog_resolution_id: str
    catalog_issues: tuple[str, ...]
    sessions: int
    eligible_sessions: int
    unique_verified_sources: int
    capture_gate_sessions_required: int
    model_sessions_required: int
    paper_sessions_required: int
    verified_close_sessions: int
    model_evidence_sessions: int
    model_evidence_sources: int
    paper_forecast_sessions: int
    capture_gate_ready: bool
    model_training_ready: bool
    paper_evidence_ready: bool
    verified_close_artifact_issues: tuple[str, ...]
    label_work_queue: tuple[CloseLabelWorkItem, ...]
    sessions_detail: tuple[SessionReadiness, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        sqlite_read_only_uri(path),
        uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _families(
    connection: sqlite3.Connection, table: str, session_id: str, feed_name: str
) -> tuple[str, ...]:
    rows = connection.execute(
        f"""
        SELECT DISTINCT family_root FROM {table}
        WHERE session_id=? AND feed_name=? AND family_root IS NOT NULL
        ORDER BY family_root
        """,
        (session_id, feed_name),
    ).fetchall()
    return tuple(str(row[0]) for row in rows)


def _source_observation_count(
    connection: sqlite3.Connection,
    table: str,
    source_sha256: str | None,
) -> int:
    """Count immutable replay evidence through its content-addressed index.

    Both observation ledgers enforce ``UNIQUE(source_sha256, record_offset)``.
    The finalized DBN hash therefore identifies the exact observation set more
    strongly than the mutable session/feed projection, and this predicate uses
    the existing covering index without reading large raw-record BLOB pages.
    """
    if not source_sha256:
        return 0
    if table not in {
        "tape_open_interest_observations",
        "tape_instrument_definition_observations",
    }:
        raise ValueError(f"unsupported immutable observation table: {table}")
    row = connection.execute(
        f"SELECT COUNT(*) FROM {table} WHERE source_sha256=?",
        (source_sha256,),
    ).fetchone()
    return int(row[0] if row else 0)


def _audit_session(
    connection: sqlite3.Connection,
    *,
    catalog_path: Path,
    session: sqlite3.Row,
    tables: set[str],
) -> SessionReadiness:
    session_id = str(session["session_id"])
    trading_date = str(session["trading_date"])
    reasons: list[str] = []
    if catalog_path.parent.name != trading_date:
        reasons.append(
            "catalog partition does not match session trading date "
            f"({catalog_path.parent.name}/{trading_date})"
        )
    required_tables = {
        "tape_feed_status",
        "tape_observed_minute",
        "tape_inferred_minute_flow",
        "tape_open_interest",
    }
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        reasons.append("missing evidence tables: " + ", ".join(missing_tables))
        return SessionReadiness(
            trading_date, session_id, str(catalog_path), None, None, False,
            tuple(reasons), (), (), (), 0, 0,
        )
    feed_columns = _columns(connection, "tape_feed_status")
    required_columns = {
        "tcbbo_records", "tcbbo_timestamped_records", "tcbbo_valid_nbbo_records",
        "provider_error_count", "sha256",
    }
    if missing_columns := sorted(required_columns - feed_columns):
        reasons.append("missing feed evidence columns: " + ", ".join(missing_columns))
        return SessionReadiness(
            trading_date, session_id, str(catalog_path), None, None, False,
            tuple(reasons), (), (), (), 0, 0,
        )
    feed = connection.execute(
        "SELECT * FROM tape_feed_status WHERE session_id=? AND feed_name='opra_options'",
        (session_id,),
    ).fetchone()
    if feed is None:
        reasons.append("required OPRA options feed is missing")
        return SessionReadiness(
            trading_date, session_id, str(catalog_path), None, None, False,
            tuple(reasons), (), (), (), 0, 0,
        )
    source_hash = str(feed["sha256"] or "") or None
    source_kind = (
        str(feed["source_kind"] or LIVE_SOURCE_KIND).lower()
        if "source_kind" in feed_columns
        else LIVE_SOURCE_KIND
    )
    evidence_contract_version = (
        str(feed["evidence_contract_version"] or "")
        if "evidence_contract_version" in feed_columns
        else EVIDENCE_CONTRACT_VERSION
    )
    try:
        expected_evidence_contract = evidence_contract_for_source_kind(source_kind)
    except ValueError as exc:
        reasons.append(str(exc))
        expected_evidence_contract = ""
    if evidence_contract_version != expected_evidence_contract:
        reasons.append(
            "evidence contract does not match source kind "
            f"({source_kind}:{evidence_contract_version or '<missing>'})"
        )
    operational_counters_applicable = (
        bool(int(feed["operational_counters_applicable"] or 0))
        if "operational_counters_applicable" in feed_columns
        else True
    )
    if str(feed["status"]) != "complete" or int(feed["complete"] or 0) != 1:
        reasons.append(f"feed is not finalized complete (status={feed['status']})")
    operational_columns = (
        ("reconnect_count", "reconnect gaps"),
        ("slow_reader_warnings", "slow-reader loss"),
        ("provider_error_count", "provider errors"),
    ) if operational_counters_applicable else ()
    for column, label in (
        *operational_columns,
        ("unmapped_trade_records", "unmapped trades"),
    ):
        if int(feed[column] or 0) > 0:
            reasons.append(f"feed records {label}")
    tcbbo = int(feed["tcbbo_records"] or 0)
    if tcbbo <= 0:
        reasons.append("no verified TCBBO records")
    elif int(feed["tcbbo_timestamped_records"] or 0) != tcbbo:
        reasons.append("not every TCBBO record has event and receive timestamps")
    elif int(feed["tcbbo_valid_nbbo_records"] or 0) / tcbbo < 0.95:
        reasons.append("valid pre-trade NBBO coverage is below 95 percent")
    if operational_counters_applicable:
        if int(feed["subscription_acks"] or 0) < int(feed["expected_subscription_acks"] or 0):
            reasons.append("subscription acknowledgements are incomplete")
        if int(feed["replay_completed"] or 0) < int(feed["expected_subscription_acks"] or 0):
            reasons.append("schema replay is incomplete")
    if not source_hash or len(source_hash) != 64:
        reasons.append("final source SHA-256 is missing or invalid")
    finalization = (
        connection.execute(
            """
            SELECT 1 FROM tape_finalization_runs
            WHERE session_id=? AND feed_name='opra_options' AND source_sha256=?
              AND evidence_contract_version=? AND complete=1
            LIMIT 1
            """,
            (session_id, source_hash, evidence_contract_version),
        ).fetchone()
        if "tape_finalization_runs" in tables else None
    )
    if finalization is None:
        reasons.append(
            "no passing immutable "
            f"{evidence_contract_version or '<missing>'} finalization audit"
        )
    source_path = Path(str(feed["dbn_path"]))
    if not source_path.is_file():
        reasons.append("source evidence file is missing")

    observed_families = _families(
        connection, "tape_observed_minute", session_id, "opra_options"
    )
    inferred_families = _families(
        connection, "tape_inferred_minute_flow", session_id, "opra_options"
    )
    oi_families = _families(connection, "tape_open_interest", session_id, "opra_options")
    observed_minutes = int(
        connection.execute(
            "SELECT COUNT(*) FROM tape_observed_minute WHERE session_id=? AND feed_name='opra_options'",
            (session_id,),
        ).fetchone()[0]
    )
    inferred_minutes = int(
        connection.execute(
            "SELECT COUNT(*) FROM tape_inferred_minute_flow WHERE session_id=? AND feed_name='opra_options'",
            (session_id,),
        ).fetchone()[0]
    )
    for label, values in (
        ("observed minute", set(observed_families)),
        ("inferred minute", set(inferred_families)),
        ("open-interest", set(oi_families)),
    ):
        missing = sorted(PRODUCTION_FAMILIES - values)
        if missing:
            reasons.append(f"{label} evidence missing families: {', '.join(missing)}")
    mismatched = connection.execute(
        """
        SELECT COUNT(*) FROM tape_inferred_minute_flow
        WHERE session_id=? AND feed_name='opra_options' AND source_sha256<>?
        """,
        (session_id, source_hash),
    ).fetchone()[0]
    if int(mismatched):
        reasons.append("inferred minute rows are not aligned to the final source hash")
    oi_observations = (
        _source_observation_count(
            connection, "tape_open_interest_observations", source_hash
        )
        if "tape_open_interest_observations" in tables else 0
    )
    if int(oi_observations) <= 0:
        reasons.append("immutable open-interest observations are unavailable")
    definition_observations = (
        _source_observation_count(
            connection, "tape_instrument_definition_observations", source_hash
        )
        if "tape_instrument_definition_observations" in tables else 0
    )
    expected_definitions = int(feed["definition_records"] or 0)
    if expected_definitions <= 0:
        reasons.append("capture contains no instrument-definition observations")
    elif int(definition_observations) != expected_definitions:
        reasons.append(
            "immutable instrument-definition observations are incomplete "
            f"({int(definition_observations)}/{expected_definitions})"
        )
    return SessionReadiness(
        trading_date=trading_date,
        session_id=session_id,
        catalog_path=str(catalog_path),
        feed_status=str(feed["status"]),
        source_sha256=source_hash,
        eligible=not reasons,
        reasons=tuple(dict.fromkeys(reasons)),
        observed_families=observed_families,
        inferred_families=inferred_families,
        open_interest_families=oi_families,
        observed_minutes=observed_minutes,
        inferred_minutes=inferred_minutes,
    )


def _verified_close_coverage(
    project_root: Path,
    market_db_path: Path,
) -> tuple[dict[str, set[str]], tuple[str, ...]]:
    path = market_db_path
    if not path.is_file():
        return {}, ()
    with _connect(path) as connection:
        if "eod_close_observations" not in _tables(connection):
            return {}, ()
        columns = {
            str(row[1]) for row in connection.execute(
                "PRAGMA table_info(eod_close_observations)"
            )
        }
        required = {
            "id", "symbol", "trading_date", "official_close", "source",
            "source_reference", "source_verified", "observed_at_utc",
            "source_artifact_sha256",
        }
        if not required <= columns:
            return {}, ()
        latest: dict[tuple[str, str], sqlite3.Row] = {}
        for row in connection.execute(
            """
            SELECT id, trading_date, symbol, source_artifact_sha256,
                   source, source_reference
            FROM eod_close_observations
            WHERE source_verified=1 AND official_close>0
              AND length(source_artifact_sha256)=64
              AND source_artifact_sha256 NOT GLOB '*[^0-9a-f]*'
              AND symbol IN ('SPX','NDX','RUT','VIX','SPY')
            ORDER BY trading_date, symbol, observed_at_utc, id
            """
        ):
            latest[(str(row[1]), str(row[2]))] = row
    coverage: dict[str, set[str]] = {}
    issues: list[str] = []
    vault_root = project_root / "data" / "verified_close_sources"
    for (trading_date, symbol), row in sorted(latest.items()):
        try:
            validate_official_close_reference(
                symbol, str(row[4]), str(row[5] or "")
            )
            resolve_verified_close_artifact(
                vault_root,
                trading_date=trading_date,
                symbol=symbol,
                source_artifact_sha256=str(row[3]),
            )
        except (OSError, ValueError) as exc:
            issues.append(f"{trading_date}/{symbol}: {exc}")
            continue
        coverage.setdefault(trading_date, set()).add(symbol)
    return coverage, tuple(issues)


def _paper_forecast_sessions(market_db_path: Path) -> int:
    path = market_db_path
    if not path.is_file():
        return 0
    with _connect(path) as connection:
        tables = _tables(connection)
        if not {
            "paper_close_forecasts",
            "paper_candidate_activations",
            "paper_forecast_feature_payloads",
            "eod_close_observations",
        } <= tables:
            return 0
        row = connection.execute(
            """
            WITH latest_closes AS (
                SELECT symbol, trading_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol, trading_date
                           ORDER BY observed_at_utc DESC, id DESC
                       ) AS rank_value
                FROM eod_close_observations
                WHERE source_verified=1 AND official_close>0
            ), complete_dates AS (
                SELECT forecasts.model_version, forecasts.artifact_sha256,
                       activations.activation_receipt_sha256,
                       forecasts.trading_date
                FROM paper_close_forecasts forecasts
                JOIN paper_candidate_activations activations
                  ON activations.model_version=forecasts.model_version
                 AND activations.artifact_sha256=forecasts.artifact_sha256
                JOIN paper_forecast_feature_payloads feature_payloads
                  ON feature_payloads.forecast_key=forecasts.forecast_key
                JOIN latest_closes closes
                  ON closes.symbol=forecasts.family_root
                 AND closes.trading_date=forecasts.trading_date
                 AND closes.rank_value=1
                WHERE forecasts.family_root IN ('SPX','NDX','RUT','VIX','SPY')
                  AND activations.activated_at_utc <= forecasts.feature_available_at_utc
                  AND activations.registered_at_utc <= forecasts.recorded_at_utc
                  AND forecasts.recorded_at_utc >= forecasts.feature_available_at_utc
                  AND (
                      JULIANDAY(forecasts.recorded_at_utc)
                      - JULIANDAY(forecasts.feature_available_at_utc)
                  ) * 86400.0 <= 90.0
                  AND forecasts.decision_horizon_minutes=15
                  AND LENGTH(feature_payloads.feature_payload_sha256)=64
                  AND feature_payloads.feature_payload_sha256=
                      LOWER(feature_payloads.feature_payload_sha256)
                  AND feature_payloads.feature_payload_sha256
                      NOT GLOB '*[^0-9a-f]*'
                GROUP BY forecasts.model_version, forecasts.artifact_sha256,
                         activations.activation_receipt_sha256,
                         forecasts.trading_date
                HAVING COUNT(*)=5
                   AND COUNT(DISTINCT forecasts.family_root)=5
                   AND COUNT(DISTINCT forecasts.artifact_sha256)=1
                   AND COUNT(DISTINCT activations.activation_receipt_sha256)=1
                   AND COUNT(DISTINCT feature_payloads.feature_payload_sha256)=5
            ), model_counts AS (
                SELECT model_version, artifact_sha256,
                       activation_receipt_sha256,
                       COUNT(*) AS session_count
                FROM complete_dates
                GROUP BY model_version, artifact_sha256,
                         activation_receipt_sha256
            )
            SELECT COALESCE(MAX(session_count), 0) FROM model_counts
            """
        ).fetchone()
        return int(row[0] if row else 0)


def audit_training_readiness(
    project_root: str | Path,
    *,
    catalog_roots: Iterable[str | Path] | None = None,
    market_db_path: str | Path | None = None,
) -> TrainingReadinessReport:
    """Scan retained catalogs read-only and explain model-evidence eligibility."""
    root = Path(project_root).resolve()
    market_path = (
        Path(market_db_path).resolve()
        if market_db_path is not None
        else (root / "data" / "market_data.db").resolve()
    )
    discovery = discover_closing_tape_catalogs(
        root,
        configured_roots=catalog_roots,
    )
    catalogs = discovery.catalog_paths
    catalog_issues = list(discovery.issues)
    detail: list[SessionReadiness] = []
    for catalog_path in catalogs:
        try:
            with _connect(catalog_path) as connection:
                tables = _tables(connection)
                if "tape_sessions" not in tables:
                    catalog_issues.append(
                        f"catalog has no tape_sessions table: {catalog_path}"
                    )
                    continue
                sessions = connection.execute(
                    "SELECT * FROM tape_sessions "
                    "ORDER BY trading_date, created_at_utc, session_id"
                ).fetchall()
                for session in sessions:
                    detail.append(
                        _audit_session(
                            connection,
                            catalog_path=catalog_path,
                            session=session,
                            tables=tables,
                        )
                    )
        except (OSError, sqlite3.DatabaseError, KeyError, TypeError, ValueError) as exc:
            catalog_issues.append(
                "catalog audit failed "
                f"({catalog_path}): {type(exc).__name__}: {exc}"
            )
    eligible_by_day: dict[str, list[int]] = {}
    for index, item in enumerate(detail):
        if item.eligible:
            eligible_by_day.setdefault(item.trading_date, []).append(index)
    for trading_date, indices in eligible_by_day.items():
        if len(indices) > 1:
            reason = (
                "multiple eligible captures share this trading date; "
                "canonical independent evidence is ambiguous"
            )
            for index in indices:
                item = detail[index]
                detail[index] = replace(
                    item, eligible=False, reasons=(*item.reasons, reason)
                )
    eligible = [item for item in detail if item.eligible]
    unique_sources = {item.source_sha256 for item in eligible if item.source_sha256}
    eligible_dates = {item.trading_date for item in eligible}
    verified_close_coverage, verified_close_artifact_issues = (
        _verified_close_coverage(root, market_path)
    )
    verified_close_dates = {
        trading_date
        for trading_date, families in verified_close_coverage.items()
        if PRODUCTION_FAMILIES <= families
    }
    model_evidence_dates = eligible_dates & verified_close_dates
    eligible_source_by_date = {
        item.trading_date: item.source_sha256
        for item in eligible
        if item.source_sha256
    }
    model_evidence_sources = {
        eligible_source_by_date[trading_date]
        for trading_date in model_evidence_dates
        if trading_date in eligible_source_by_date
    }
    eligible_by_date = {item.trading_date: item for item in eligible}
    label_work_queue = tuple(
        CloseLabelWorkItem(
            trading_date=trading_date,
            session_id=eligible_by_date[trading_date].session_id,
            source_sha256=str(eligible_by_date[trading_date].source_sha256),
            verified_families=tuple(
                sorted(verified_close_coverage.get(trading_date, set()))
            ),
            missing_families=tuple(
                sorted(
                    PRODUCTION_FAMILIES
                    - verified_close_coverage.get(trading_date, set())
                )
            ),
            missing_requirements=tuple(
                CloseLabelRequirement(
                    family_root=family,
                    approved_sources=tuple(
                        sorted(VERIFIED_CLOSE_SOURCES_BY_SYMBOL[family])
                    ),
                    approved_reference_hosts=tuple(
                        sorted(
                            {
                                host
                                for source in VERIFIED_CLOSE_SOURCES_BY_SYMBOL[family]
                                for host in OFFICIAL_REFERENCE_HOSTS_BY_SOURCE[source]
                            }
                        )
                    ),
                )
                for family in sorted(
                    PRODUCTION_FAMILIES
                    - verified_close_coverage.get(trading_date, set())
                )
            ),
        )
        for trading_date in sorted(eligible_dates - verified_close_dates)
    )
    paper_sessions = _paper_forecast_sessions(market_path)
    catalog_inventory_complete = not catalog_issues
    return TrainingReadinessReport(
        generated_at_utc=datetime.now(UTC).isoformat(),
        catalogs=len(catalogs),
        catalog_roots=discovery.catalog_roots,
        catalog_paths=tuple(str(path) for path in catalogs),
        catalog_resolution_id=discovery.resolution_id,
        catalog_issues=tuple(dict.fromkeys(catalog_issues)),
        sessions=len(detail),
        eligible_sessions=len(eligible_dates),
        unique_verified_sources=len(unique_sources),
        capture_gate_sessions_required=CAPTURE_GATE_SESSIONS_REQUIRED,
        model_sessions_required=MODEL_SESSIONS_REQUIRED,
        paper_sessions_required=PAPER_SESSIONS_REQUIRED,
        verified_close_sessions=len(verified_close_dates),
        model_evidence_sessions=len(model_evidence_dates),
        model_evidence_sources=len(model_evidence_sources),
        paper_forecast_sessions=paper_sessions,
        capture_gate_ready=(
            catalog_inventory_complete
            and len(eligible_dates) >= CAPTURE_GATE_SESSIONS_REQUIRED
            and len(unique_sources) >= CAPTURE_GATE_SESSIONS_REQUIRED
        ),
        model_training_ready=(
            catalog_inventory_complete
            and len(model_evidence_dates) >= MODEL_SESSIONS_REQUIRED
            and len(model_evidence_sources) >= MODEL_SESSIONS_REQUIRED
        ),
        paper_evidence_ready=(
            catalog_inventory_complete
            and paper_sessions >= PAPER_SESSIONS_REQUIRED
        ),
        verified_close_artifact_issues=verified_close_artifact_issues,
        label_work_queue=label_work_queue,
        sessions_detail=tuple(detail),
    )
