"""Source and SQLite contract preflight for opening acceptance.

The default strict phase and bootstrap-eligibility phase are read-only.  The
explicit bootstrap phase is the sole exception: after repeating every source,
fingerprint, configured-target, and SQLite-integrity gate, it may run the
canonical additive ``backend.database.init_db`` migration exactly once and
then requires the strict contract to pass.  It never starts the application
lifecycle or a provider session.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "marketpin-opening-acceptance-preflight.v2"
PREFLIGHT_PHASES = (
    "strict",
    "bootstrap-eligibility",
    "bootstrap",
    "fingerprint",
)
REQUIRED_FILES = (
    "market_app_supervisor.psm1",
    "market_calendar.psm1",
    "config/us_cash_equity_calendar.json",
    "start_market_day.ps1",
    "ensure_market_app.ps1",
    "start_databento_app.ps1",
    "start_closing_tape.ps1",
    "server.py",
    ".streamlit/config.toml",
    "app.py",
    "app/services/dashboard_status.py",
    "app/services/databento_symbol_catalog.py",
    "app/services/live_data_client.py",
    "app/services/live_panel_view.py",
    "app/services/orb_view.py",
    "app/services/retained_parity_view.py",
    "app/services/sidebar_live_state.py",
    "app/utils/market_calendar.py",
    "app/utils/market_time.py",
    "app/utils/opra_parity_history.py",
    "app/utils/snapshot_history.py",
    "app/utils/time_et.py",
    "backend/ai_predictor.py",
    "backend/app.py",
    "backend/api/helpers.py",
    "backend/api/bounded_runtime_read.py",
    "backend/api/lifecycle.py",
    "backend/api/schemas.py",
    "backend/api/routers/gex.py",
    "backend/api/routers/health.py",
    "backend/api/routers/orb.py",
    "backend/api/routers/predict.py",
    "backend/config.py",
    "backend/closing_tape/catalog.py",
    "backend/closing_tape/config.py",
    "backend/closing_tape/live_recorder.py",
    "backend/closing_tape/start_gate.py",
    "backend/database.py",
    "backend/database_target.py",
    "backend/databento_streamer.py",
    "backend/historical_context.py",
    "backend/inference.py",
    "backend/market_structure.py",
    "backend/monitor_completed_final_delivery.py",
    "backend/monitor_data_quality_outbox.py",
    "backend/monitor_notification_outbox.py",
    "backend/monitor_opening_acceptance.py",
    "backend/monitor_scan_ledger.py",
    "backend/monitor_session_rollover.py",
    "backend/opening_code_fingerprint.py",
    "backend/optional_family_canary.py",
    "backend/prediction_authority.py",
    "backend/prediction_passport.py",
    "backend/research/live_shadow.py",
    "backend/research/shadow_formula.py",
    "backend/runtime_controls.py",
    "backend/snapshot_export.py",
    "backend/streamer.py",
    "backend/underlying_validator.py",
    "backend/workstation.py",
    "tools/ack_market_monitor_opening_acceptance.py",
    "tools/build_market_monitor_data_quality_ack_request.py",
    "tools/commit_market_monitor_opening_acceptance.py",
    "tools/inspect_opening_capture.py",
    "tools/preflight_opening_acceptance.py",
    "tools/prepare_databento_universe_cache.py",
    "tools/prepare_market_monitor_session.py",
)
REQUIRED_MODULES = (
    "app.services.sidebar_live_state",
    "app.services.orb_view",
    "app.utils.market_calendar",
    "app.utils.market_time",
    "app.utils.time_et",
    "backend.ai_predictor",
    "backend.app",
    "backend.closing_tape.catalog",
    "backend.closing_tape.config",
    "backend.closing_tape.live_recorder",
    "backend.closing_tape.start_gate",
    "backend.api.helpers",
    "backend.api.bounded_runtime_read",
    "backend.api.lifecycle",
    "backend.api.schemas",
    "backend.api.routers.gex",
    "backend.api.routers.health",
    "backend.api.routers.orb",
    "backend.api.routers.predict",
    "backend.config",
    "backend.database",
    "backend.database_target",
    "backend.databento_streamer",
    "backend.historical_context",
    "backend.inference",
    "backend.market_structure",
    "backend.monitor_completed_final_delivery",
    "backend.monitor_data_quality_outbox",
    "backend.monitor_notification_outbox",
    "backend.monitor_scan_ledger",
    "backend.monitor_session_rollover",
    "backend.monitor_opening_acceptance",
    "backend.opening_code_fingerprint",
    "backend.optional_family_canary",
    "backend.prediction_authority",
    "backend.prediction_passport",
    "backend.research.live_shadow",
    "backend.research.shadow_formula",
    "backend.runtime_controls",
    "backend.snapshot_export",
    "backend.streamer",
    "backend.underlying_validator",
    "backend.workstation",
    "tools.ack_market_monitor_opening_acceptance",
    "tools.build_market_monitor_data_quality_ack_request",
    "tools.commit_market_monitor_opening_acceptance",
    "tools.inspect_opening_capture",
    "tools.preflight_opening_acceptance",
    "tools.prepare_databento_universe_cache",
    "tools.prepare_market_monitor_session",
)
OPENING_ACCEPTANCE_CALLABLE_CONTRACTS = {
    "backend.monitor_session_rollover": ("prepare_monitor_session",),
    "backend.monitor_opening_acceptance": (
        "commit_opening_acceptance_milestone",
        "ack_opening_acceptance_notifications",
        "validate_opening_acceptance_receipt_replay",
    ),
    "tools.ack_market_monitor_opening_acceptance": ("main",),
    "tools.commit_market_monitor_opening_acceptance": ("main",),
    "tools.inspect_opening_capture": ("main",),
    "tools.prepare_market_monitor_session": ("main",),
}
REQUIRED_ROUTES = {
    "/health",
    "/health/live",
    "/orb",
    "/orb/{symbol}",
    "/v1/orb",
    "/v1/orb/{symbol}",
}
REQUIRED_MARKET_STRUCTURE_COLUMNS = {
    "observation_id",
    "symbol",
    "trading_date",
    "source_timestamp_utc",
    "captured_at_utc",
    "provider",
    "subscription_epoch_id",
    "subscription_generation",
    "calculation_id",
    "reference_price",
    "gamma_pin",
    "max_pain",
    "primary_expiration",
    "same_day_profile_available",
    "universe_sha256",
    "validation_status",
}
REQUIRED_ORB_REFERENCE_COLUMNS = {
    "sample_id",
    "symbol",
    "trading_date",
    "sample_timestamp_utc",
    "source_timestamp_utc",
    "captured_at_utc",
    "provider",
    "subscription_epoch_id",
    "subscription_generation",
    "reference_price",
    "spot_source",
    "spot_formula_version",
    "risk_free_rate",
    "time_to_expiration_years",
    "primary_expiration",
    "same_day_profile_available",
    "universe_sha256",
    "universe_is_fallback",
    "paired_quote_count",
    "minimum_paired_quote_count",
    "contributing_pair_count",
    "contributing_quote_count",
    "earliest_ts_event_ns",
    "latest_ts_event_ns",
    "earliest_ts_recv_ns",
    "latest_ts_recv_ns",
    "observation_index_ns",
    "source_quote_age_seconds",
    "maximum_source_quote_age_seconds",
    "source_timestamp_span_seconds",
    "quote_freshness_limit_seconds",
    "pair_identity_sha256",
    "symbol_mapping_version",
    "formula_inputs_json",
    "processing_clock_status",
    "timestamp_order_valid",
    "handoff_status",
    "generation_state_unchanged",
    "universe_state_unchanged",
    "validation_status",
}
REQUIRED_MARKET_STRUCTURE_TRIGGERS = {
    "market_structure_observations_insert_guard",
    "market_structure_observations_epoch_guard",
    "market_structure_observations_no_update",
    "market_structure_observations_no_delete",
}
REQUIRED_ORB_REFERENCE_TRIGGERS = {
    "orb_reference_samples_existing_guard",
    "orb_reference_samples_epoch_guard",
    "orb_reference_samples_insert_guard",
    "orb_reference_samples_no_update",
    "orb_reference_samples_no_delete",
}
REQUIRED_ORB_REFERENCE_INDEXES = {"uix_orb_reference_logical_sample"}
REQUIRED_ORB_REFERENCE_DECISION_COLUMNS = {
    "sample_id",
    "sample_timestamp_utc",
    "intended_bucket_utc",
    "attempt_completed_at_utc",
    "progress_eligible",
    "reason",
    "decision_status",
}
REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS = {
    "orb_reference_sample_decisions_existing_guard",
    "orb_reference_sample_decisions_insert_guard",
    "orb_reference_sample_decisions_no_update",
    "orb_reference_sample_decisions_no_delete",
}
REQUIRED_ORB_REFERENCE_DECISION_INDEXES = {
    "uix_orb_reference_decision_sample_id"
}
_BOOTSTRAPPABLE_MISSING_COLUMNS = {
    "market_structure_observations": {"subscription_epoch_id"},
    "orb_reference_samples": {"subscription_epoch_id"},
    "orb_reference_sample_decisions": set(),
}
_BOOTSTRAPPABLE_MISSING_OBJECT_CODES = {
    "RUNTIME_DATABASE_MISSING",
    "RUNTIME_TABLE_MISSING",
    "RUNTIME_IMMUTABILITY_TRIGGERS_MISSING",
    "RUNTIME_UNIQUE_INDEX_MISSING",
}


def _issue(code: str, **details: Any) -> dict[str, Any]:
    return {"code": code, **details}


def inspect_required_files(project_root: Path) -> list[dict[str, Any]]:
    """Return one actionable issue for every missing launch dependency."""

    root = project_root.resolve()
    return [
        _issue("REQUIRED_FILE_MISSING", path=relative_path)
        for relative_path in REQUIRED_FILES
        if not (root / relative_path).is_file()
    ]


def _safe_error_message(exc: BaseException) -> str:
    return " ".join(str(exc).split())[:300]


def capture_source_fingerprint(project_root: Path) -> str:
    """Hash the complete ordered preflight source contract."""

    root = project_root.resolve()
    digest = hashlib.sha256()
    for relative_path in REQUIRED_FILES:
        candidate = (root / relative_path).resolve()
        candidate.relative_to(root)
        file_digest = hashlib.sha256(candidate.read_bytes()).digest()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_digest)
        digest.update(b"\0")
    return digest.hexdigest()


def inspect_configured_database_target(
    database_path: Path,
) -> list[dict[str, Any]]:
    """Prove that the imported canonical database engine targets this file."""

    try:
        database = importlib.import_module("backend.database")
        database_target = importlib.import_module("backend.database_target")
        configured_path = database_target.sqlite_database_path_from_url(
            database.engine.url
        ).resolve()
    except Exception as exc:
        return [
            _issue(
                "CONFIGURED_DATABASE_TARGET_UNRESOLVED",
                error_type=type(exc).__name__,
                error=_safe_error_message(exc),
            )
        ]

    expected_path = database_path.resolve()
    if configured_path != expected_path:
        return [
            _issue(
                "CONFIGURED_DATABASE_TARGET_MISMATCH",
                expected_path=str(expected_path),
                configured_path=str(configured_path),
            )
        ]
    return []


def inspect_import_contract(project_root: Path) -> list[dict[str, Any]]:
    """Import runtime modules and verify their public acceptance contract."""

    root = project_root.resolve()
    root_text = str(root)
    if root_text not in sys.path:
        sys.path.insert(0, root_text)

    issues: list[dict[str, Any]] = []
    modules: dict[str, Any] = {}
    for module_name in REQUIRED_MODULES:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # Import failures are the evidence being audited.
            issues.append(
                _issue(
                    "REQUIRED_MODULE_IMPORT_FAILED",
                    module=module_name,
                    error_type=type(exc).__name__,
                    error=_safe_error_message(exc),
                )
            )
            continue
        modules[module_name] = module
        module_file = getattr(module, "__file__", None)
        try:
            Path(str(module_file)).resolve().relative_to(root)
        except (TypeError, ValueError):
            issues.append(
                _issue(
                    "MODULE_RESOLVED_OUTSIDE_PROJECT",
                    module=module_name,
                    path=str(module_file or "unavailable"),
                )
            )

    backend_app = modules.get("backend.app")
    if backend_app is not None:
        app = getattr(backend_app, "app", None)
        routes = {
            str(getattr(route, "path", ""))
            for route in getattr(getattr(app, "router", None), "routes", ())
        }
        missing_routes = sorted(REQUIRED_ROUTES.difference(routes))
        if missing_routes:
            issues.append(_issue("REQUIRED_API_ROUTES_MISSING", routes=missing_routes))

    lifecycle = modules.get("backend.api.lifecycle")
    if lifecycle is not None and not callable(
        getattr(lifecycle, "run_market_structure_capture_loop", None)
    ):
        issues.append(
            _issue(
                "MARKET_STRUCTURE_CAPTURE_LOOP_MISSING",
                module="backend.api.lifecycle",
            )
        )

    market_structure = modules.get("backend.market_structure")
    for attribute in ("MarketStructureJournal", "get_market_structure_journal"):
        if market_structure is not None and not callable(
            getattr(market_structure, attribute, None)
        ):
            issues.append(
                _issue(
                    "MARKET_STRUCTURE_CONTRACT_MISSING",
                    module="backend.market_structure",
                    attribute=attribute,
                )
            )

    streamer = modules.get("backend.databento_streamer")
    streamer_type = getattr(streamer, "DatabentoGammaStreamer", None)
    for attribute in ("_opening_reference_loop", "_capture_opening_reference_once"):
        if streamer_type is not None and not callable(getattr(streamer_type, attribute, None)):
            issues.append(
                _issue(
                    "ORB_SAMPLER_CONTRACT_MISSING",
                    module="backend.databento_streamer",
                    attribute=attribute,
                )
            )

    orb_view = modules.get("app.services.orb_view")
    for attribute in ("initialize_live_autorefresh", "render_opening_range_panel"):
        if orb_view is not None and not callable(getattr(orb_view, attribute, None)):
            issues.append(
                _issue(
                    "ORB_DASHBOARD_CONTRACT_MISSING",
                    module="app.services.orb_view",
                    attribute=attribute,
                )
            )

    for module_name, attributes in OPENING_ACCEPTANCE_CALLABLE_CONTRACTS.items():
        module = modules.get(module_name)
        for attribute in attributes:
            if module is not None and not callable(getattr(module, attribute, None)):
                issues.append(
                    _issue(
                        "OPENING_ACCEPTANCE_CONTRACT_MISSING",
                        module=module_name,
                        attribute=attribute,
                    )
                )

    database = modules.get("backend.database")
    model_contracts = (
        (
            "MarketStructureObservation",
            REQUIRED_MARKET_STRUCTURE_COLUMNS,
        ),
        ("OrbReferenceSample", REQUIRED_ORB_REFERENCE_COLUMNS),
        (
            "OrbReferenceSampleDecision",
            REQUIRED_ORB_REFERENCE_DECISION_COLUMNS,
        ),
    )
    for model_name, required_columns in model_contracts:
        model = getattr(database, model_name, None) if database is not None else None
        table = getattr(model, "__table__", None)
        columns = set(table.columns.keys()) if table is not None else set()
        missing_columns = sorted(required_columns.difference(columns))
        if missing_columns:
            issues.append(
                _issue(
                    "ORM_SCHEMA_COLUMNS_MISSING",
                    model=model_name,
                    columns=missing_columns,
                )
            )

    inspector = modules.get("tools.inspect_opening_capture")
    inspector_contracts = (
        (
            "EXPECTED_MARKET_STRUCTURE_COLUMNS",
            REQUIRED_MARKET_STRUCTURE_COLUMNS,
        ),
        ("EXPECTED_ORB_REFERENCE_COLUMNS", REQUIRED_ORB_REFERENCE_COLUMNS),
        (
            "EXPECTED_MARKET_STRUCTURE_TRIGGERS",
            REQUIRED_MARKET_STRUCTURE_TRIGGERS,
        ),
        ("EXPECTED_ORB_REFERENCE_TRIGGERS", REQUIRED_ORB_REFERENCE_TRIGGERS),
        ("EXPECTED_ORB_REFERENCE_INDEXES", REQUIRED_ORB_REFERENCE_INDEXES),
        (
            "EXPECTED_ORB_REFERENCE_DECISION_COLUMNS",
            REQUIRED_ORB_REFERENCE_DECISION_COLUMNS,
        ),
        (
            "EXPECTED_ORB_REFERENCE_DECISION_TRIGGERS",
            REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS,
        ),
        (
            "EXPECTED_ORB_REFERENCE_DECISION_INDEXES",
            REQUIRED_ORB_REFERENCE_DECISION_INDEXES,
        ),
    )
    for attribute, required_values in inspector_contracts:
        actual = set(getattr(inspector, attribute, ())) if inspector is not None else set()
        missing_values = sorted(required_values.difference(actual))
        if missing_values:
            issues.append(
                _issue(
                    "INSPECTOR_SCHEMA_CONTRACT_MISSING",
                    attribute=attribute,
                    values=missing_values,
                )
            )
    return issues


def _database_names(
    connection: sqlite3.Connection,
    *,
    object_type: str,
    table_name: str,
) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type=? AND tbl_name=?",
            (object_type, table_name),
        )
        if row[0]
    }


def inspect_database_contract(database_path: Path) -> list[dict[str, Any]]:
    """Inspect the deployed SQLite schema through a read-only connection."""

    path = database_path.resolve()
    if path.exists() and not path.is_file():
        return [_issue("RUNTIME_DATABASE_NOT_REGULAR_FILE", path=str(path))]
    if not path.exists():
        if not path.parent.is_dir():
            return [
                _issue(
                    "RUNTIME_DATABASE_PARENT_MISSING",
                    path=str(path.parent),
                ),
                _issue("RUNTIME_DATABASE_MISSING", path=str(path)),
            ]
        return [_issue("RUNTIME_DATABASE_MISSING", path=str(path))]

    issues: list[dict[str, Any]] = []
    try:
        connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        return [
            _issue(
                "RUNTIME_DATABASE_READ_FAILED",
                error_type=type(exc).__name__,
                error=_safe_error_message(exc),
            )
        ]
    try:
        connection.execute("PRAGMA query_only=ON")
        table_contracts = (
            ("market_structure_observations", REQUIRED_MARKET_STRUCTURE_COLUMNS),
            ("orb_reference_samples", REQUIRED_ORB_REFERENCE_COLUMNS),
            (
                "orb_reference_sample_decisions",
                REQUIRED_ORB_REFERENCE_DECISION_COLUMNS,
            ),
        )
        observed_columns: dict[str, set[str]] = {}
        for table_name, required_columns in table_contracts:
            table_exists = bool(
                _database_names(
                    connection,
                    object_type="table",
                    table_name=table_name,
                )
            )
            if not table_exists:
                issues.append(_issue("RUNTIME_TABLE_MISSING", table=table_name))
                continue
            # This preflight protects the opening-capture contract, not every
            # unrelated historical table sharing the live database.  A
            # table-scoped quick check retains b-tree/index integrity coverage
            # for each required opening table without rescanning the complete
            # multi-gigabyte database on every watchdog invocation.
            quick_check = [
                str(row[0])
                for row in connection.execute(
                    f'PRAGMA quick_check("{table_name}")'
                )
            ]
            if quick_check != ["ok"]:
                return [
                    _issue(
                        "RUNTIME_DATABASE_QUICK_CHECK_FAILED",
                        table=table_name,
                        results=quick_check[:1] or ["no_result"],
                    )
                ]
            columns = {
                str(row[1])
                for row in connection.execute(f'PRAGMA table_info("{table_name}")')
            }
            observed_columns[table_name] = columns
            missing_columns = sorted(required_columns.difference(columns))
            if missing_columns:
                issues.append(
                    _issue(
                        "RUNTIME_SCHEMA_COLUMNS_MISSING",
                        table=table_name,
                        columns=missing_columns,
                    )
                )

        trigger_contracts = (
            (
                "market_structure_observations",
                REQUIRED_MARKET_STRUCTURE_TRIGGERS,
            ),
            ("orb_reference_samples", REQUIRED_ORB_REFERENCE_TRIGGERS),
            (
                "orb_reference_sample_decisions",
                REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS,
            ),
        )
        for table_name, required_triggers in trigger_contracts:
            triggers = _database_names(
                connection,
                object_type="trigger",
                table_name=table_name,
            )
            missing_triggers = sorted(required_triggers.difference(triggers))
            if missing_triggers:
                issues.append(
                    _issue(
                        "RUNTIME_IMMUTABILITY_TRIGGERS_MISSING",
                        table=table_name,
                        triggers=missing_triggers,
                    )
                )

        index_contracts = (
            (
                "orb_reference_samples",
                REQUIRED_ORB_REFERENCE_INDEXES,
                (
                    "symbol",
                    "sample_timestamp_utc",
                    "subscription_epoch_id",
                    "subscription_generation",
                    "universe_sha256",
                    "primary_expiration",
                    "spot_formula_version",
                    "risk_free_rate",
                    "symbol_mapping_version",
                ),
            ),
            (
                "orb_reference_sample_decisions",
                REQUIRED_ORB_REFERENCE_DECISION_INDEXES,
                ("sample_id",),
            ),
        )
        for table_name, required_indexes, identity_columns in index_contracts:
            indexes = _database_names(
                connection,
                object_type="index",
                table_name=table_name,
            )
            missing_indexes = sorted(required_indexes.difference(indexes))
            if not missing_indexes:
                continue
            issues.append(
                _issue(
                    "RUNTIME_UNIQUE_INDEX_MISSING",
                    table=table_name,
                    indexes=missing_indexes,
                )
            )
            if set(identity_columns).issubset(observed_columns.get(table_name, set())):
                group_by = ", ".join(f'"{column}"' for column in identity_columns)
                duplicate = connection.execute(
                    f'SELECT 1 FROM "{table_name}" '
                    f"GROUP BY {group_by} HAVING COUNT(*) > 1 LIMIT 1"
                ).fetchone()
                if duplicate is not None:
                    issues.append(
                        _issue(
                            "RUNTIME_UNIQUE_INDEX_DATA_CONFLICT",
                            table=table_name,
                            indexes=missing_indexes,
                        )
                    )
    except sqlite3.Error as exc:
        issues.append(
            _issue(
                "RUNTIME_DATABASE_SCHEMA_READ_FAILED",
                error_type=type(exc).__name__,
                error=_safe_error_message(exc),
            )
        )
    finally:
        connection.close()
    return issues


def _database_issue_is_bootstrappable(issue: dict[str, Any]) -> bool:
    code = str(issue.get("code") or "")
    if code == "RUNTIME_SCHEMA_COLUMNS_MISSING":
        table_name = str(issue.get("table") or "")
        columns = {str(column) for column in issue.get("columns") or ()}
        return bool(columns) and columns.issubset(
            _BOOTSTRAPPABLE_MISSING_COLUMNS.get(table_name, set())
        )
    if code not in _BOOTSTRAPPABLE_MISSING_OBJECT_CODES:
        return False
    if code == "RUNTIME_DATABASE_MISSING":
        return True
    table_name = str(issue.get("table") or "")
    if table_name not in _BOOTSTRAPPABLE_MISSING_COLUMNS:
        return False
    if code == "RUNTIME_IMMUTABILITY_TRIGGERS_MISSING":
        expected = {
            "market_structure_observations": REQUIRED_MARKET_STRUCTURE_TRIGGERS,
            "orb_reference_samples": REQUIRED_ORB_REFERENCE_TRIGGERS,
            "orb_reference_sample_decisions": (
                REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS
            ),
        }[table_name]
        return set(issue.get("triggers") or ()).issubset(expected)
    if code == "RUNTIME_UNIQUE_INDEX_MISSING":
        expected = {
            "market_structure_observations": set(),
            "orb_reference_samples": REQUIRED_ORB_REFERENCE_INDEXES,
            "orb_reference_sample_decisions": (
                REQUIRED_ORB_REFERENCE_DECISION_INDEXES
            ),
        }[table_name]
        indexes = set(issue.get("indexes") or ())
        return bool(indexes) and indexes.issubset(expected)
    return code == "RUNTIME_TABLE_MISSING"


def database_issues_allow_additive_bootstrap(
    issues: list[dict[str, Any]],
) -> bool:
    """Return true only for the closed set that canonical init_db can repair."""

    return bool(issues) and all(
        _database_issue_is_bootstrappable(issue) for issue in issues
    )


def additive_bootstrap_scope(issues: list[dict[str, Any]]) -> str | None:
    """Choose the only bounded mutator capable of repairing these exact gaps."""

    if not database_issues_allow_additive_bootstrap(issues):
        return None
    issue_tables = {
        str(issue.get("table") or "")
        for issue in issues
        if issue.get("code") != "RUNTIME_DATABASE_MISSING"
    }
    if issue_tables == {"orb_reference_sample_decisions"}:
        return "orb_reference_decision_sidecar"
    return "canonical_init_db"


def _fingerprint_issue(
    project_root: Path,
    *,
    stage: str,
) -> tuple[str | None, list[dict[str, Any]]]:
    try:
        return capture_source_fingerprint(project_root), []
    except Exception as exc:
        return None, [
            _issue(
                "SOURCE_FINGERPRINT_FAILED",
                stage=stage,
                error_type=type(exc).__name__,
                error=_safe_error_message(exc),
            )
        ]


def run_preflight(
    project_root: Path,
    database_path: Path,
    *,
    phase: str = "strict",
    expected_source_fingerprint: str | None = None,
) -> dict[str, Any]:
    if phase not in {"strict", "bootstrap-eligibility"}:
        raise ValueError(f"Unsupported read-only preflight phase: {phase}")

    root = project_root.resolve()
    file_issues = inspect_required_files(root)
    source_fingerprint, fingerprint_issues = (None, [])
    if not file_issues:
        source_fingerprint, fingerprint_issues = _fingerprint_issue(
            root,
            stage="before_import",
        )
    import_issues = [] if file_issues else inspect_import_contract(root)
    target_issues = (
        []
        if file_issues or import_issues
        else inspect_configured_database_target(database_path)
    )
    after_fingerprint, after_fingerprint_issues = (None, [])
    if not file_issues:
        after_fingerprint, after_fingerprint_issues = _fingerprint_issue(
            root,
            stage="after_import",
        )
    fingerprint_issues.extend(after_fingerprint_issues)
    if source_fingerprint and after_fingerprint:
        if source_fingerprint != after_fingerprint:
            fingerprint_issues.append(
                _issue("SOURCE_CHANGED_DURING_PREFLIGHT")
            )
        source_fingerprint = after_fingerprint
    if (
        expected_source_fingerprint is not None
        and source_fingerprint != expected_source_fingerprint
    ):
        fingerprint_issues.append(
            _issue("SOURCE_FINGERPRINT_MISMATCH")
        )

    database_issues = inspect_database_contract(database_path)
    source_issues = [
        *file_issues,
        *fingerprint_issues,
        *import_issues,
        *target_issues,
    ]
    additive_bootstrap_allowed = database_issues_allow_additive_bootstrap(
        database_issues
    )
    bootstrap_scope = additive_bootstrap_scope(database_issues)
    schema_bootstrap_required = bool(database_issues) and additive_bootstrap_allowed
    database_contract = (
        "strict"
        if not database_issues
        else (
            "additive_bootstrap_required"
            if additive_bootstrap_allowed
            else "blocked"
        )
    )
    issues = list(source_issues)
    if phase == "strict" or not additive_bootstrap_allowed:
        issues.extend(database_issues)
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": phase,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "ready": not issues,
        "project_root": str(root),
        "database_path": str(database_path.resolve()),
        "database_contract": database_contract,
        "schema_bootstrap_required": schema_bootstrap_required,
        "schema_bootstrap_scope": bootstrap_scope,
        "schema_bootstrap_performed": False,
        "bootstrap_attempt_count": 0,
        "source_fingerprint_sha256": source_fingerprint,
        "required_file_count": len(REQUIRED_FILES),
        "required_module_count": len(REQUIRED_MODULES),
        "database_issues": database_issues,
        "issues": issues,
    }


def run_fingerprint_preflight(
    project_root: Path,
    database_path: Path,
    *,
    expected_source_fingerprint: str | None,
) -> dict[str, Any]:
    """Re-bind a cached full proof to unchanged on-disk source only."""

    root = project_root.resolve()
    file_issues = inspect_required_files(root)
    before, fingerprint_issues = (None, [])
    after = None
    if not file_issues:
        before, fingerprint_issues = _fingerprint_issue(
            root,
            stage="fingerprint_revalidation_before",
        )
        after, after_issues = _fingerprint_issue(
            root,
            stage="fingerprint_revalidation_after",
        )
        fingerprint_issues.extend(after_issues)
        if before and after and before != after:
            fingerprint_issues.append(_issue("SOURCE_CHANGED_DURING_PREFLIGHT"))
    source_fingerprint = after or before
    if (
        not expected_source_fingerprint
        or source_fingerprint != expected_source_fingerprint
    ):
        fingerprint_issues.append(_issue("SOURCE_FINGERPRINT_MISMATCH"))
    issues = [*file_issues, *fingerprint_issues]
    return {
        "schema_version": SCHEMA_VERSION,
        "phase": "fingerprint",
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "ready": not issues,
        "project_root": str(root),
        "database_path": str(database_path.resolve()),
        "database_contract": "cached_full_proof_not_rechecked",
        "schema_bootstrap_required": False,
        "schema_bootstrap_scope": None,
        "schema_bootstrap_performed": False,
        "bootstrap_attempt_count": 0,
        "source_fingerprint_sha256": source_fingerprint,
        "required_file_count": len(REQUIRED_FILES),
        "required_module_count": len(REQUIRED_MODULES),
        "database_issues": [],
        "issues": issues,
    }


def run_schema_bootstrap(
    project_root: Path,
    database_path: Path,
    *,
    expected_source_fingerprint: str | None,
    bootstrap_scope: str | None,
) -> dict[str, Any]:
    """Run canonical init_db once, only after repeatable read-only eligibility."""

    eligibility = run_preflight(
        project_root,
        database_path,
        phase="bootstrap-eligibility",
        expected_source_fingerprint=expected_source_fingerprint,
    )
    if (
        not expected_source_fingerprint
        or len(expected_source_fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in expected_source_fingerprint)
    ):
        eligibility["phase"] = "bootstrap"
        eligibility["ready"] = False
        eligibility["issues"] = [
            *eligibility["issues"],
            _issue("EXPECTED_SOURCE_FINGERPRINT_REQUIRED"),
        ]
        return eligibility
    if not eligibility["ready"]:
        eligibility["phase"] = "bootstrap"
        return eligibility
    if bootstrap_scope != eligibility["schema_bootstrap_scope"]:
        eligibility["phase"] = "bootstrap"
        eligibility["ready"] = False
        eligibility["issues"] = [
            _issue(
                "SCHEMA_BOOTSTRAP_SCOPE_MISMATCH",
                expected_scope=eligibility["schema_bootstrap_scope"],
                requested_scope=bootstrap_scope,
            )
        ]
        return eligibility
    if not eligibility["schema_bootstrap_required"]:
        strict = run_preflight(
            project_root,
            database_path,
            phase="strict",
            expected_source_fingerprint=expected_source_fingerprint,
        )
        strict["phase"] = "bootstrap"
        return strict

    try:
        database = importlib.import_module("backend.database")
        previous_logging_disable = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        try:
            if bootstrap_scope == "orb_reference_decision_sidecar":
                database.Base.metadata.create_all(
                    bind=database.engine,
                    tables=[database.OrbReferenceSampleDecision.__table__],
                )
                database._ensure_orb_reference_decision_schema()
                database._ensure_orb_reference_decision_immutability()
            elif bootstrap_scope == "canonical_init_db":
                database.init_db()
            else:
                raise RuntimeError("unsupported schema bootstrap scope")
        finally:
            logging.disable(previous_logging_disable)
    except Exception as exc:
        eligibility["phase"] = "bootstrap"
        eligibility["ready"] = False
        eligibility["schema_bootstrap_performed"] = False
        eligibility["bootstrap_attempt_count"] = 1
        eligibility["issues"] = [
            _issue(
                "SCHEMA_BOOTSTRAP_FAILED",
                error_type=type(exc).__name__,
                error=_safe_error_message(exc),
            )
        ]
        return eligibility
    finally:
        database_module = sys.modules.get("backend.database")
        engine = getattr(database_module, "engine", None)
        if engine is not None:
            engine.dispose()

    strict = run_preflight(
        project_root,
        database_path,
        phase="strict",
        expected_source_fingerprint=expected_source_fingerprint,
    )
    strict["phase"] = "bootstrap"
    strict["schema_bootstrap_performed"] = True
    strict["schema_bootstrap_scope"] = bootstrap_scope
    strict["bootstrap_attempt_count"] = 1
    strict["prebootstrap_database_issues"] = eligibility["database_issues"]
    return strict


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--phase", choices=PREFLIGHT_PHASES, default="strict")
    parser.add_argument("--expected-source-fingerprint")
    parser.add_argument(
        "--bootstrap-scope",
        choices=("canonical_init_db", "orb_reference_decision_sidecar"),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.phase == "bootstrap":
        report = run_schema_bootstrap(
            args.project_root,
            args.database,
            expected_source_fingerprint=args.expected_source_fingerprint,
            bootstrap_scope=args.bootstrap_scope,
        )
    elif args.phase == "fingerprint":
        report = run_fingerprint_preflight(
            args.project_root,
            args.database,
            expected_source_fingerprint=args.expected_source_fingerprint,
        )
    else:
        report = run_preflight(
            args.project_root,
            args.database,
            phase=args.phase,
            expected_source_fingerprint=args.expected_source_fingerprint,
        )
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0 if report["ready"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
