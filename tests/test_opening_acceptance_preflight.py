from __future__ import annotations

import importlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

import tools.preflight_opening_acceptance as preflight_module
from backend.opening_code_fingerprint import OPENING_CRITICAL_SOURCE_PATHS

from tools.preflight_opening_acceptance import (
    REQUIRED_FILES,
    REQUIRED_MARKET_STRUCTURE_COLUMNS,
    REQUIRED_MARKET_STRUCTURE_TRIGGERS,
    REQUIRED_MODULES,
    REQUIRED_ORB_REFERENCE_COLUMNS,
    REQUIRED_ORB_REFERENCE_DECISION_COLUMNS,
    REQUIRED_ORB_REFERENCE_DECISION_INDEXES,
    REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS,
    REQUIRED_ORB_REFERENCE_INDEXES,
    REQUIRED_ORB_REFERENCE_TRIGGERS,
    additive_bootstrap_scope,
    database_issues_allow_additive_bootstrap,
    inspect_database_contract,
    inspect_import_contract,
    inspect_required_files,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER_CALENDAR_DEPENDENCIES = {
    "market_calendar.psm1",
    "config/us_cash_equity_calendar.json",
}
HASH_ONLY_PYTHON_SOURCE_PATHS = {
    "server.py",
    "app.py",
    "app/services/dashboard_status.py",
    "app/services/databento_symbol_catalog.py",
    "app/services/live_data_client.py",
    "app/services/live_panel_view.py",
    "app/services/retained_parity_view.py",
    "app/utils/opra_parity_history.py",
    "app/utils/snapshot_history.py",
}
RELEASE_CRITICAL_SOURCE_PATHS = {
    ".streamlit/config.toml",
    "backend/database.py",
    "backend/databento_streamer.py",
    "backend/api/routers/orb.py",
    "backend/market_structure.py",
    "app/services/orb_view.py",
    "app/services/sidebar_live_state.py",
    "backend/closing_tape/catalog.py",
    "backend/closing_tape/config.py",
    "backend/closing_tape/live_recorder.py",
    "backend/closing_tape/start_gate.py",
    "start_closing_tape.ps1",
    "tools/prepare_databento_universe_cache.py",
    "backend/monitor_data_quality_outbox.py",
    "backend/monitor_notification_outbox.py",
    "backend/monitor_opening_acceptance.py",
    "backend/monitor_scan_ledger.py",
    "backend/monitor_session_rollover.py",
    "tools/ack_market_monitor_opening_acceptance.py",
    "tools/commit_market_monitor_opening_acceptance.py",
    "tools/inspect_opening_capture.py",
    "tools/preflight_opening_acceptance.py",
    "tools/prepare_market_monitor_session.py",
}


def _create_contract_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        for table_name, columns in (
            ("market_structure_observations", REQUIRED_MARKET_STRUCTURE_COLUMNS),
            ("orb_reference_samples", REQUIRED_ORB_REFERENCE_COLUMNS),
            (
                "orb_reference_sample_decisions",
                REQUIRED_ORB_REFERENCE_DECISION_COLUMNS,
            ),
        ):
            column_sql = ", ".join(f'"{column}" TEXT' for column in sorted(columns))
            connection.execute(f'CREATE TABLE "{table_name}" ({column_sql})')
        for table_name, triggers in (
            (
                "market_structure_observations",
                REQUIRED_MARKET_STRUCTURE_TRIGGERS,
            ),
            ("orb_reference_samples", REQUIRED_ORB_REFERENCE_TRIGGERS),
            (
                "orb_reference_sample_decisions",
                REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS,
            ),
        ):
            for trigger in triggers:
                connection.execute(
                    f'CREATE TRIGGER "{trigger}" BEFORE INSERT ON "{table_name}" '
                    "WHEN 0 BEGIN SELECT 1; END"
                )
        for index in REQUIRED_ORB_REFERENCE_INDEXES:
            connection.execute(
                f'CREATE UNIQUE INDEX "{index}" '
                'ON "orb_reference_samples" ("sample_id")'
            )
        for index in REQUIRED_ORB_REFERENCE_DECISION_INDEXES:
            connection.execute(
                f'CREATE UNIQUE INDEX "{index}" '
                'ON "orb_reference_sample_decisions" ("sample_id")'
            )
        connection.commit()
    finally:
        connection.close()


def test_current_source_import_contract_is_launch_compatible():
    assert ".streamlit/config.toml" in OPENING_CRITICAL_SOURCE_PATHS
    assert ".streamlit/config.toml" in REQUIRED_FILES
    assert "backend/opening_code_fingerprint.py" in REQUIRED_FILES
    assert "backend.opening_code_fingerprint" in REQUIRED_MODULES
    assert LAUNCHER_CALENDAR_DEPENDENCIES.issubset(REQUIRED_FILES)
    assert set(OPENING_CRITICAL_SOURCE_PATHS) == set(REQUIRED_FILES)
    assert RELEASE_CRITICAL_SOURCE_PATHS.issubset(REQUIRED_FILES)
    assert {
        "app.services.orb_view",
        "app.services.sidebar_live_state",
    }.issubset(REQUIRED_MODULES)
    fingerprinted_python_modules = {
        relative_path.removesuffix(".py").replace("/", ".")
        for relative_path in OPENING_CRITICAL_SOURCE_PATHS
        if relative_path.endswith(".py")
        and relative_path not in HASH_ONLY_PYTHON_SOURCE_PATHS
    }
    assert fingerprinted_python_modules.issubset(REQUIRED_MODULES)
    assert len(OPENING_CRITICAL_SOURCE_PATHS) == len(
        set(OPENING_CRITICAL_SOURCE_PATHS)
    )
    assert len(REQUIRED_FILES) == len(set(REQUIRED_FILES))
    assert len(REQUIRED_MODULES) == len(set(REQUIRED_MODULES))
    assert inspect_required_files(PROJECT_ROOT) == []
    assert inspect_import_contract(PROJECT_ROOT) == []


def test_database_contract_accepts_complete_schema_without_writing(tmp_path):
    database_path = tmp_path / "opening.db"
    _create_contract_database(database_path)
    before = database_path.read_bytes()

    assert inspect_database_contract(database_path) == []

    assert database_path.read_bytes() == before


def test_database_contract_quick_checks_only_required_opening_tables(
    tmp_path,
    monkeypatch,
):
    database_path = tmp_path / "opening.db"
    _create_contract_database(database_path)
    statements: list[str] = []
    real_connect = preflight_module.sqlite3.connect

    def traced_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(preflight_module.sqlite3, "connect", traced_connect)

    assert inspect_database_contract(database_path) == []

    quick_checks = {
        statement for statement in statements if statement.startswith("PRAGMA quick_check")
    }
    assert quick_checks == {
        'PRAGMA quick_check("market_structure_observations")',
        'PRAGMA quick_check("orb_reference_samples")',
        'PRAGMA quick_check("orb_reference_sample_decisions")',
    }


def test_database_contract_rejects_missing_append_only_trigger(tmp_path):
    database_path = tmp_path / "opening.db"
    _create_contract_database(database_path)
    missing_trigger = "orb_reference_samples_no_update"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(f'DROP TRIGGER "{missing_trigger}"')
        connection.commit()
    finally:
        connection.close()

    issues = inspect_database_contract(database_path)

    assert {
        "code": "RUNTIME_IMMUTABILITY_TRIGGERS_MISSING",
        "table": "orb_reference_samples",
        "triggers": [missing_trigger],
    } in issues


def test_missing_database_is_read_only_and_canonical_bootstrap_eligible(tmp_path):
    database_path = tmp_path / "fresh.db"

    issues = inspect_database_contract(database_path)

    assert issues == [
        {"code": "RUNTIME_DATABASE_MISSING", "path": str(database_path.resolve())}
    ]
    assert not database_path.exists()
    assert database_issues_allow_additive_bootstrap(issues)
    assert additive_bootstrap_scope(issues) == "canonical_init_db"


def test_missing_decision_sidecar_has_one_bounded_online_bootstrap_scope(tmp_path):
    database_path = tmp_path / "legacy.db"
    _create_contract_database(database_path)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("DROP TABLE orb_reference_sample_decisions")
        connection.commit()
    finally:
        connection.close()

    issues = inspect_database_contract(database_path)

    assert {issue["code"] for issue in issues} == {
        "RUNTIME_TABLE_MISSING",
        "RUNTIME_IMMUTABILITY_TRIGGERS_MISSING",
        "RUNTIME_UNIQUE_INDEX_MISSING",
    }
    assert {
        issue.get("table") for issue in issues
    } == {"orb_reference_sample_decisions"}
    assert database_issues_allow_additive_bootstrap(issues)
    assert additive_bootstrap_scope(issues) == "orb_reference_decision_sidecar"


def test_incompatible_legacy_decision_table_is_not_retried_as_additive(tmp_path):
    database_path = tmp_path / "incompatible.db"
    _create_contract_database(database_path)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("ALTER TABLE orb_reference_sample_decisions DROP COLUMN reason")
        connection.commit()
    finally:
        connection.close()

    issues = inspect_database_contract(database_path)

    assert {
        "code": "RUNTIME_SCHEMA_COLUMNS_MISSING",
        "table": "orb_reference_sample_decisions",
        "columns": ["reason"],
    } in issues
    assert not database_issues_allow_additive_bootstrap(issues)
    assert additive_bootstrap_scope(issues) is None


def test_corrupt_database_is_never_bootstrap_eligible(tmp_path):
    database_path = tmp_path / "corrupt.db"
    database_path.write_bytes(b"not-a-sqlite-database")

    issues = inspect_database_contract(database_path)

    assert issues
    assert issues[0]["code"] in {
        "RUNTIME_DATABASE_READ_FAILED",
        "RUNTIME_DATABASE_SCHEMA_READ_FAILED",
        "RUNTIME_DATABASE_QUICK_CHECK_FAILED",
    }
    assert not database_issues_allow_additive_bootstrap(issues)
    assert additive_bootstrap_scope(issues) is None


def _run_isolated_schema_bootstrap(database_path: Path) -> dict:
    program = r'''
import json
from pathlib import Path
from tools.preflight_opening_acceptance import run_preflight, run_schema_bootstrap

root = Path.cwd()
database = Path(__import__("sys").argv[1])
eligibility = run_preflight(root, database, phase="bootstrap-eligibility")
result = run_schema_bootstrap(
    root,
    database,
    expected_source_fingerprint=eligibility["source_fingerprint_sha256"],
    bootstrap_scope=eligibility["schema_bootstrap_scope"],
)
print(json.dumps({"eligibility": eligibility, "result": result}, sort_keys=True))
'''
    environment = os.environ.copy()
    environment["DATABASE_URL"] = f"sqlite:///{database_path.as_posix()}"
    completed = subprocess.run(
        [sys.executable, "-B", "-c", program, str(database_path)],
        cwd=PROJECT_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""
    output_lines = [line for line in completed.stdout.splitlines() if line.strip()]
    assert len(output_lines) == 1
    return json.loads(output_lines[0])


def test_canonical_bootstrap_preserves_legacy_rows_and_passes_strict_postcheck(
    tmp_path,
):
    database_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("CREATE TABLE legacy_evidence (value TEXT NOT NULL)")
        connection.execute("INSERT INTO legacy_evidence VALUES ('preserve-me')")
        connection.commit()
    finally:
        connection.close()

    report = _run_isolated_schema_bootstrap(database_path)

    assert report["eligibility"]["schema_bootstrap_scope"] == "canonical_init_db"
    result = report["result"]
    assert result["ready"] is True
    assert result["database_contract"] == "strict"
    assert result["schema_bootstrap_performed"] is True
    assert result["bootstrap_attempt_count"] == 1
    connection = sqlite3.connect(database_path)
    try:
        assert connection.execute("SELECT value FROM legacy_evidence").fetchone() == (
            "preserve-me",
        )
        assert connection.execute("PRAGMA quick_check(1)").fetchone() == ("ok",)
    finally:
        connection.close()


def test_decision_sidecar_bootstrap_does_not_rewrite_existing_core_objects(tmp_path):
    database_path = tmp_path / "sidecar.db"
    _create_contract_database(database_path)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("DROP TABLE orb_reference_sample_decisions")
        before = dict(
            connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE tbl_name IN ('market_structure_observations', "
                "'orb_reference_samples') AND sql IS NOT NULL"
            )
        )
        connection.commit()
    finally:
        connection.close()

    report = _run_isolated_schema_bootstrap(database_path)

    assert (
        report["eligibility"]["schema_bootstrap_scope"]
        == "orb_reference_decision_sidecar"
    )
    assert report["result"]["ready"] is True
    assert report["result"]["schema_bootstrap_performed"] is True
    connection = sqlite3.connect(database_path)
    try:
        after = dict(
            connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE tbl_name IN ('market_structure_observations', "
                "'orb_reference_samples') AND sql IS NOT NULL"
            )
        )
        assert after == before
        assert {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='trigger' AND tbl_name='orb_reference_sample_decisions'"
            )
        } == REQUIRED_ORB_REFERENCE_DECISION_TRIGGERS
    finally:
        connection.close()


def test_required_file_contract_reports_exact_missing_paths(tmp_path):
    issues = inspect_required_files(tmp_path)

    assert issues
    assert all(issue["code"] == "REQUIRED_FILE_MISSING" for issue in issues)
    assert "backend/api/routers/orb.py" in {issue["path"] for issue in issues}
    assert "backend/monitor_opening_acceptance.py" in {
        issue["path"] for issue in issues
    }
    assert "backend/monitor_session_rollover.py" in {
        issue["path"] for issue in issues
    }
    assert "backend/opening_code_fingerprint.py" in {
        issue["path"] for issue in issues
    }
    assert "tools/ack_market_monitor_opening_acceptance.py" in {
        issue["path"] for issue in issues
    }
    assert "tools/commit_market_monitor_opening_acceptance.py" in {
        issue["path"] for issue in issues
    }
    assert "tools/prepare_market_monitor_session.py" in {
        issue["path"] for issue in issues
    }


@pytest.mark.parametrize("omitted_path", sorted(LAUNCHER_CALENDAR_DEPENDENCIES))
def test_required_file_contract_rejects_each_missing_launcher_calendar_dependency(
    tmp_path,
    omitted_path,
):
    for relative_path in REQUIRED_FILES:
        if relative_path == omitted_path:
            continue
        candidate = tmp_path / relative_path
        candidate.parent.mkdir(parents=True, exist_ok=True)
        candidate.write_bytes(b"present\n")

    assert inspect_required_files(tmp_path) == [
        {"code": "REQUIRED_FILE_MISSING", "path": omitted_path}
    ]


@pytest.mark.parametrize(
    ("module_name", "attribute"),
    (
        ("backend.monitor_session_rollover", "prepare_monitor_session"),
        (
            "backend.monitor_opening_acceptance",
            "commit_opening_acceptance_milestone",
        ),
        (
            "backend.monitor_opening_acceptance",
            "ack_opening_acceptance_notifications",
        ),
        (
            "backend.monitor_opening_acceptance",
            "validate_opening_acceptance_receipt_replay",
        ),
        ("tools.ack_market_monitor_opening_acceptance", "main"),
        ("tools.commit_market_monitor_opening_acceptance", "main"),
        ("tools.inspect_opening_capture", "main"),
        ("tools.prepare_market_monitor_session", "main"),
    ),
)
def test_import_contract_rejects_missing_opening_acceptance_callable(
    monkeypatch,
    module_name,
    attribute,
):
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, attribute, None)

    issues = inspect_import_contract(PROJECT_ROOT)

    assert {
        "code": "OPENING_ACCEPTANCE_CONTRACT_MISSING",
        "module": module_name,
        "attribute": attribute,
    } in issues


@pytest.mark.parametrize(
    "missing_module",
    (
        "backend.monitor_opening_acceptance",
        "backend.monitor_session_rollover",
        "tools.ack_market_monitor_opening_acceptance",
        "tools.commit_market_monitor_opening_acceptance",
        "tools.prepare_market_monitor_session",
    ),
)
def test_import_contract_reports_opening_component_import_failure(
    monkeypatch,
    missing_module,
):
    real_import_module = preflight_module.importlib.import_module

    def import_module(module_name):
        if module_name == missing_module:
            raise ImportError("synthetic opening component import failure")
        return real_import_module(module_name)

    monkeypatch.setattr(preflight_module.importlib, "import_module", import_module)

    issues = inspect_import_contract(PROJECT_ROOT)

    assert {
        "code": "REQUIRED_MODULE_IMPORT_FAILED",
        "module": missing_module,
        "error_type": "ImportError",
        "error": "synthetic opening component import failure",
    } in issues
