import json
import hashlib
import os
import sqlite3
import subprocess
import sys
import zlib
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.sax.saxutils import escape

import tools.inspect_opening_capture as opening_capture_inspector
import backend.monitor_opening_acceptance as opening_acceptance
from backend.opening_code_fingerprint import (
    LOADED_CODE_CAPTURE_SEMANTICS,
    LOADED_CODE_FINGERPRINT_SCHEMA,
    LOADED_CODE_HASH_ALGORITHM,
    OPENING_CRITICAL_SOURCE_PATHS,
)

from tools.inspect_opening_capture import (
    CT,
    EXPECTED_ORB_REFERENCE_COLUMNS,
    EXPECTED_ORB_REFERENCE_DECISION_COLUMNS,
    EXPECTED_ORB_REFERENCE_DECISION_INDEXES,
    EXPECTED_ORB_REFERENCE_DECISION_TRIGGERS,
    EXPECTED_ORB_REFERENCE_INDEXES,
    EXPECTED_ORB_REFERENCE_TRIGGERS,
    compact_report,
    evaluate,
    inspect_database,
    parse_args,
    parse_clock_outputs,
    scope_acceptance_evaluation,
)

TEST_SUBSCRIPTION_EPOCH = "e" * 64
ROOT = Path(__file__).resolve().parents[1]


def _loaded_code_fingerprint(
    captured_at_utc: str = "2026-09-04T12:45:00+00:00",
) -> dict:
    return {
        "schema_version": LOADED_CODE_FINGERPRINT_SCHEMA,
        "capture_semantics": LOADED_CODE_CAPTURE_SEMANTICS,
        "captured_at_utc": captured_at_utc,
        "hash_algorithm": LOADED_CODE_HASH_ALGORITHM,
        "current_on_disk_recomputed": False,
        "current_on_disk_comparison": "not_performed_by_health_endpoint",
        "files": {
            relative_path: {
                "loaded_at_startup_sha256": hashlib.sha256(
                    (ROOT / relative_path).read_bytes()
                ).hexdigest()
            }
            for relative_path in OPENING_CRITICAL_SOURCE_PATHS
        },
    }


def test_direct_script_entrypoint_bootstraps_project_root(tmp_path):
    result = subprocess.run(
        [
            sys.executable,
            str(Path(opening_capture_inspector.__file__).resolve()),
            "--help",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_compact_output_is_default_and_full_output_is_explicit():
    defaults = parse_args([])
    assert defaults.full is False
    assert defaults.acceptance_scope is None
    assert defaults.timeout_seconds == 10.0
    assert parse_args(["--full"]).full is True
    assert parse_args(
        ["--acceptance-scope", "complete_5m_orb"]
    ).acceptance_scope == "complete_5m_orb"


def test_compact_report_retains_acceptance_evidence_without_endpoint_bulk():
    health = _primary_active_health()
    health["payload"]["optional_family_canary"]["evaluation_evidence"] = {
        "evaluation_deferred": False,
        "opening_orb_protection_active": False,
    }
    health["payload"]["large_unrelated_payload"] = {"rows": list(range(1_000))}
    orb = {
        "ok": True,
        "status_code": 200,
        "payload": {
            "schema_version": "marketpin-reference-orb.collection.v1",
            "configured_symbols": ["SPX"],
            "requested_symbols": ["SPX"],
            "large_unrelated_payload": {"rows": list(range(1_000))},
            "symbols": {
                "SPX": {
                    "configured": True,
                    "trading_date": "2026-09-08",
                    "capture_status": "partial",
                    "directional_evidence_eligible": False,
                    "combined_structure_directional_evidence_eligible": False,
                    "reference_semantics": {
                        "primary_expiration": "2026-09-08",
                        "directional_base_eligible": True,
                    },
                    "opening_ranges": {
                        "5m": {
                            "capture_status": "complete",
                            "orb_complete": True,
                            "orb_high": 7_750.0,
                            "orb_low": 7_730.0,
                            "directional_evidence_eligible": True,
                            "combined_structure_directional_evidence_eligible": False,
                            "capture_evidence": {
                                "sample_count": 60,
                                "expected_sample_count": 60,
                                "capture_ratio": 1.0,
                                "opening_bucket_present": True,
                                "max_gap_seconds": 5.0,
                            },
                            "provenance": {
                                "range_provenance_aligned": True,
                                "structure_reference_status": "unavailable",
                            },
                        }
                    },
                    "pin_behavior": {
                        "gamma_pin": None,
                        "max_pain": None,
                        "level_availability_status": "unavailable",
                    },
                    "provenance": {
                        "range_provenance_aligned": True,
                        "structure_reference_status": "unavailable",
                    },
                }
            },
        },
    }
    database = _database()
    database["path"] = "C:/MarketPin/data/market_data.db"
    database["orb_reference_columns"] = sorted(EXPECTED_ORB_REFERENCE_COLUMNS)
    full = {
        "schema_version": "marketpin-opening-readiness.v1",
        "state": "ready",
        "observed_at_ct": "2026-09-08T08:35:15-05:00",
        "observed_at_utc": "2026-09-08T13:35:15+00:00",
        "issues": [],
        "warnings": [],
        "clock": {"synchronized": True},
        "backend_health": health,
        "backend_live_health": {"ok": True, "payload": {"stream_progressing": True}},
        "dashboard_health": {"ok": True, "status_code": 200, "payload": "ok"},
        "orb": orb,
        "database": database,
        "scheduled_tasks": _tasks(),
        "read_only": True,
        "notes": ["read only"],
    }

    compact = compact_report(full)

    assert compact["output_mode"] == "compact"
    assert compact["due_orb_windows"] == ["5m"]
    assert compact["due_orb_window"] == "5m"
    assert compact["read_only"] is True
    assert "large_unrelated_payload" not in compact["backend_health"]["payload"]
    assert "large_unrelated_payload" not in compact["orb"]["payload"]
    spx = compact["orb"]["payload"]["symbols"]["SPX"]
    assert spx["opening_ranges"]["5m"]["capture_evidence"]["capture_ratio"] == 1.0
    assert spx["opening_ranges"]["5m"]["capture_evidence"][
        "opening_bucket_present"
    ] is True
    assert spx["opening_ranges"]["5m"]["provenance"][
        "structure_reference_status"
    ] == "unavailable"
    assert spx["pin_behavior"]["level_availability_status"] == "unavailable"
    assert compact["database"]["orb_reference_column_count"] == len(
        EXPECTED_ORB_REFERENCE_COLUMNS
    )
    assert compact["backend_health"]["payload"]["optional_family_canary"][
        "evaluation_evidence"
    ]["opening_orb_protection_active"] is False
    assert compact["backend_health"]["payload"]["loaded_code_fingerprint"] == (
        health["payload"]["loaded_code_fingerprint"]
    )
    assert compact["database"]["market_structure_rows"]["SPX"][
        "first_eligible_calculation_bound"
    ]["lineage"]["run_calculated_at_utc"] == "2026-09-04T13:30:30Z"
    compact_health = compact["backend_health"]["payload"]
    assert compact_health["symbols_selected"] == 3_200
    assert compact_health["subscription_staging"]["state"] == "primary_active"
    assert compact_health["subscription_staging"]["active_contract_count"] == 1_282
    assert compact_health["market_subscription_status"]["RUT"] == {
        "requested": True,
        "selected_contract_count": 676,
        "active_contract_count": 182,
        "deferred_contract_count": 494,
    }
    # The compact report retains one 64-file startup fingerprint so acceptance
    # can compare every release-critical source without reopening endpoint bulk.
    assert len(json.dumps(compact, separators=(",", ":"))) < 32_000


def _database():
    return {
        "present": True,
        "journal_mode": "wal",
        "quick_check": "ok",
        "market_structure_table_present": True,
        "missing_market_structure_columns": [],
        "missing_market_structure_triggers": [],
        "market_structure_rows": {
            symbol: {
                "row_count": 1,
                "latest_source_age_seconds": 1.0,
                "latest_capture_age_seconds": 0.5,
                "latest_provider": "databento",
                "latest_subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "latest_subscription_generation": 7,
                "latest_calculation_id": f"calculation-{symbol.lower()}",
                "latest_reference_price": 7_500.0,
                "latest_gamma_pin": 7_525.0,
                "latest_max_pain": 7_500.0,
                "latest_primary_expiration": "2026-09-04",
                "latest_same_day_profile_available": True,
                "latest_universe_sha256": "a" * 64,
                "latest_validation_status": "valid",
                "latest_calculation_bound": {
                    "row_count": 1,
                    "source_timestamp_utc": "2026-09-04T13:30:14+00:00",
                    "source_age_seconds": 1.0,
                    "captured_at_utc": "2026-09-04T13:30:14.500000+00:00",
                    "capture_age_seconds": 0.5,
                    "lag_from_latest_source_seconds": 0.0,
                    "provider": "databento",
                    "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                    "subscription_generation": 7,
                    "calculation_id": f"calculation-{symbol.lower()}",
                    "reference_price": 7_500.0,
                    "gamma_pin": 7_525.0,
                    "max_pain": 7_500.0,
                    "primary_expiration": "2026-09-04",
                    "same_day_profile_available": True,
                    "universe_sha256": "a" * 64,
                    "validation_status": "valid",
                    "lineage": {
                        "status": "verified",
                        "reason": None,
                        "calculation_id": f"calculation-{symbol.lower()}",
                        "gamma_run_present": True,
                        "input_blob_present": True,
                        "payload_integrity_verified": True,
                        "payload_sha256": "c" * 64,
                        "run_calculated_at_utc": "2026-09-04T13:30:14+00:00",
                        "run_age_seconds": 1.0,
                    },
                },
                "first_eligible_calculation_bound": (
                    {
                        "candidate_count": 1,
                        "selection_status": "verified",
                        "selection_reason": None,
                        "observation_id": f"first-observation-{symbol.lower()}",
                        "trading_date": "2026-09-04",
                        "source_timestamp_utc": "2026-09-04T13:30:30Z",
                        "captured_at_utc": "2026-09-04T13:30:30.500000Z",
                        "pair_completed_at_utc": (
                            "2026-09-04T13:30:30.500000Z"
                        ),
                        "provider": "databento",
                        "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                        "subscription_generation": 7,
                        "calculation_id": f"first-calculation-{symbol.lower()}",
                        "reference_price": 7_500.0,
                        "gamma_pin": 7_525.0,
                        "max_pain": 7_500.0,
                        "primary_expiration": "2026-09-04",
                        "same_day_profile_available": True,
                        "universe_sha256": "a" * 64,
                        "validation_status": "valid",
                        "lineage": {
                            "status": "verified",
                            "reason": None,
                            "calculation_id": (
                                f"first-calculation-{symbol.lower()}"
                            ),
                            "gamma_run_present": True,
                            "input_blob_present": True,
                            "payload_integrity_verified": True,
                            "payload_sha256": "d" * 64,
                            "run_calculated_at_utc": "2026-09-04T13:30:30Z",
                            "source_to_run_seconds": 0.0,
                            "run_to_capture_seconds": 0.5,
                        },
                    }
                    if symbol in ("SPX", "NDX")
                    else None
                ),
            }
            for symbol in ("SPX", "NDX", "VIX", "RUT")
        },
        "orb_reference_table_present": True,
        "missing_orb_reference_columns": [],
        "missing_orb_reference_triggers": [],
        "missing_orb_reference_indexes": [],
        "orb_reference_decision_table_present": True,
        "missing_orb_reference_decision_columns": [],
        "missing_orb_reference_decision_triggers": [],
        "missing_orb_reference_decision_indexes": [],
        "orb_reference_progress_decisions": {
            symbol: {
                "raw_row_count": 72,
                "pending_decision_count": 0,
                "ineligible_decision_count": 0,
                "eligible_decision_count": 72,
            }
            for symbol in ("SPX", "NDX", "VIX", "RUT")
        },
        "orb_reference_rows": {
            symbol: {
                "row_count": 72,
                "opening_capture_ratio": 1.0,
                "maximum_opening_gap_seconds": 5.0,
                "advancing_5s_evidence": True,
                "opening_bucket_present": True,
                "latest_subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_epoch_ids": [TEST_SUBSCRIPTION_EPOCH],
                "invalid_subscription_epoch_row_count": 0,
                "mixed_subscription_epoch_rows": False,
                "latest_subscription_generation": 7,
                "subscription_generations": [7],
                "invalid_subscription_generation_row_count": 0,
                "mixed_subscription_generation_rows": False,
                "latest_primary_expiration": "2026-09-04",
                "latest_same_day_profile_available": True,
            }
            for symbol in ("SPX", "NDX", "VIX", "RUT")
        },
    }


def _health():
    return {
        "ok": True,
        "payload": {
            "runtime_controls": {
                "sleep_prevention": {"requested": True, "active": True}
            },
            "processing_clock_telemetry": {"status": "synchronized"},
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "active_generation": 7,
            "handoff_status": "active",
            "subscription_allowed": True,
            "symbols_requested": ["SPX", "NDX", "VIX", "RUT"],
            "symbols_subscribed": 3_200,
            "subscription_bounds": {"max_subscription_contracts": 3_200},
            "root_contract_counts": {
                "SPXW": 1_058,
                "NDXP": 1_182,
                "VIXW": 284,
                "RUTW": 676,
            },
            "core_symbol_status": {
                "SPX": {"contracts_subscribed": 1_058},
                "NDX": {"contracts_subscribed": 1_182},
                "VIX": {"contracts_subscribed": 284},
                "RUT": {"contracts_subscribed": 676},
            },
            "subscription_metadata": {"reservation_shortfall_pairs": 0},
            "optional_family_canary": {"state": "armed"},
            "orb_reference_sampler": {
                "thread_alive": True,
                "interval_seconds": 5,
            },
            "loaded_code_fingerprint": _loaded_code_fingerprint(),
        },
    }


def _primary_active_health():
    health = _health()
    payload = health["payload"]
    selected_counts = {"SPX": 1_058, "NDX": 1_182, "VIX": 284, "RUT": 676}
    primary_counts = {"SPX": 450, "NDX": 510, "VIX": 140, "RUT": 182}
    selected_total = sum(selected_counts.values())
    active_total = sum(primary_counts.values())
    payload["symbols_selected"] = selected_total
    payload["symbols_subscribed"] = active_total
    payload["root_contract_counts"] = {
        "SPXW": primary_counts["SPX"],
        "NDXP": primary_counts["NDX"],
        "VIXW": primary_counts["VIX"],
        "RUTW": primary_counts["RUT"],
    }
    payload["core_symbol_status"] = {
        symbol: {"requested": True, "contracts_subscribed": count}
        for symbol, count in primary_counts.items()
    }
    payload["subscription_metadata"].update(
        full_contract_count=8_000,
        selected_contract_count=selected_total,
        selected_universe_sha256="a" * 64,
        markets={
            symbol: {
                "selected_contract_count": count,
                "market_reservation_shortfall_pairs": 0,
                "primary_reserved_pairs_retained": 100,
                "next_listed_reserved_pairs_retained": 50,
            }
            for symbol, count in selected_counts.items()
        },
    )
    payload["subscription_staging"] = {
        "mode": "all-primary-then-same-session-shadow",
        "state": "primary_active",
        "active_stage": "primary",
        "deferred_stage": "shadow",
        "full_selected_contract_count": selected_total,
        "active_contract_count": active_total,
        "deferred_contract_count": selected_total - active_total,
        "requested_orb_families": ["SPX", "NDX", "VIX", "RUT"],
        "primary_contract_counts": primary_counts.copy(),
        "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
        "subscription_generation": 7,
        "full_selected_universe_sha256": "a" * 64,
        "same_client_additive_subscription": True,
        "intraday_replay_for_deferred_stage": False,
        "promotion_eligible": False,
        "promotion_reasons": ["CLEAN_TRANSPORT_WINDOW_INCOMPLETE"],
        "additive_request_sent": False,
    }
    payload["market_subscription_status"] = {
        symbol: {
            "requested": True,
            "selected_contract_count": selected_counts[symbol],
            "active_contract_count": primary_counts[symbol],
            "deferred_contract_count": (
                selected_counts[symbol] - primary_counts[symbol]
            ),
        }
        for symbol in selected_counts
    }
    return health


def _runtime_bound_orb(payload):
    payload["runtime_binding_applied"] = True
    payload["runtime_context_stable"] = True
    payload["active_runtime_context"] = {
        "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
        "subscription_generation": 7,
        "handoff_status": "active",
    }
    for symbol, state in (payload.get("symbols") or {}).items():
        semantics = state.setdefault("reference_semantics", {})
        if symbol in {"SPX", "NDX", "RUT"}:
            semantics.setdefault("primary_expiration", "2026-09-04")
            semantics.setdefault("same_day_profile_available", True)
            semantics.setdefault("directional_base_eligible", True)
        elif symbol == "VIX":
            semantics.setdefault("primary_expiration", "2026-09-05")
            semantics.setdefault("same_day_profile_available", False)
            semantics.setdefault("directional_base_eligible", False)
        if symbol in {"SPX", "NDX"}:
            state.setdefault("pin_behavior", {}).update(
                gamma_pin=7_525.0,
                max_pain=7_500.0,
                level_availability_status="available",
                current_level_policy=(
                    "newest_structure_observation_only_no_carry_forward"
                ),
            )
            state.setdefault("last_known_structure", {}).update(
                status="aligned",
                level_availability_status="available",
                age_seconds=1.0,
                gamma_pin=7_525.0,
                max_pain=7_500.0,
                calculation_id=f"calculation-{symbol.lower()}",
            )
            state["last_calculation_bound_structure"] = {
                "status": "aligned",
                "level_availability_status": "available",
                "source_timestamp_utc": "2026-09-04T13:30:14+00:00",
                "captured_at_utc": "2026-09-04T13:30:14.500000+00:00",
                "freshness_timestamp_utc": "2026-09-04T13:30:14+00:00",
                "age_seconds": 1.0,
                "maximum_current_age_seconds": 90.0,
                "gamma_pin": 7_525.0,
                "max_pain": 7_500.0,
                "calculation_id": f"calculation-{symbol.lower()}",
                "provider": "databento",
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "universe_sha256": "a" * 64,
                "primary_expiration": "2026-09-04",
                "same_day_profile_available": True,
                "current_provenance_aligned": True,
                "runtime_aligned": True,
                "evidence_eligible": True,
            }
            semantics.update(
                primary_expiration="2026-09-04",
                same_day_profile_available=True,
            )
            for window in (state.get("opening_ranges") or {}).values():
                window["combined_structure_directional_evidence_eligible"] = True
        provenance = state.setdefault("provenance", {})
        provenance.update(
            runtime_binding_applied=True,
            active_runtime_epoch_aligned=True,
            active_subscription_epoch_id=TEST_SUBSCRIPTION_EPOCH,
            active_subscription_generation=7,
            active_handoff_status="active",
            subscription_epoch_ids=[TEST_SUBSCRIPTION_EPOCH],
            subscription_generations=[7],
        )
        if symbol in {"SPX", "NDX"}:
            provenance.update(
                structure_vs_reference_aligned=True,
                structure_reference_status="aligned",
                structure_reference_fresh=True,
                current_pin_level_availability="available",
            )
    return payload


def _regular_acceptance_fixture():
    symbols = {
        symbol: {
            "opening_ranges": {
                "5m": {
                    "capture_status": "complete",
                    "directional_evidence_eligible": symbol != "VIX",
                    "capture_evidence": {"capture_ratio": 1.0},
                }
            },
            "reference_semantics": {
                "directional_base_eligible": symbol != "VIX",
                "primary_expiration": (
                    "2026-09-05" if symbol == "VIX" else "2026-09-04"
                ),
                "same_day_profile_available": symbol != "VIX",
            },
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }
    return {
        "health": _health(),
        "live": {
            "ok": True,
            "payload": {
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "active_generation": 7,
                "handoff_status": "active",
                "stream_connected": True,
                "stream_progressing": True,
                "collection_ready": True,
                "calculation_ready": True,
                "prediction_pipeline_ok": True,
            },
        },
        "orb": {
            "ok": True,
            "payload": _runtime_bound_orb(
                {"configured_symbols": list(symbols), "symbols": symbols}
            ),
        },
        "database": _database(),
    }


def _tasks(*, startup_run_level="Highest", watchdog_run_level="Highest"):
    common = {
        "present": True,
        "enabled": True,
        "state": "Ready",
        "last_result": 0,
        "wake_to_run": True,
        "task_path_valid": True,
        "principal_system_account": True,
        "logon_type_service_account": True,
        "action_count_valid": True,
        "action_shape_valid": True,
        "executable_matches_system32_powershell": True,
        "working_directory_matches_project": True,
        "arguments_match_contract": True,
        "trigger_contract_valid": True,
        "multiple_instances_ignore_new": True,
        "execution_limit_valid": True,
        "battery_policy_valid": True,
        "restart_policy_valid": True,
        "start_when_available_policy_valid": True,
        "launch_script_matches_project": True,
        "rut_canary_requested": True,
    }
    return {
        "applicable": True,
        "tasks": {
            "MarketPinPredictor_AutoStart": {
                **common,
                "run_level": startup_run_level,
                "start_when_available": True,
                "clock_sync_skipped": False,
                "last_run_time": "2026-09-04T07:45:00-05:00",
            },
            "MarketPinPredictor_Watchdog": {
                **common,
                "run_level": watchdog_run_level,
                "start_when_available": False,
                "clock_sync_skipped": True,
                "last_run_time": "2026-09-04T07:50:00-05:00",
            },
        },
    }


def _scheduled_task_xml_row(name: str, project_root: Path):
    sid = "S-1-5-21-1000-1000-1000-1001"
    system_sid = "S-1-5-18"
    account = r"DESKTOP-TEST\LukeD"
    executable = Path(os.environ.get("SystemRoot", r"C:\Windows")) / (
        r"System32\WindowsPowerShell\v1.0\powershell.exe"
    )
    script = project_root / "start_market_day.ps1"
    watchdog = name == "MarketPinPredictor_Watchdog"
    arguments = (
        f'-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{script}" '
        "-EnableRutCanary"
        + (" -SkipClockSync" if watchdog else "")
    )
    if watchdog:
        triggers = """
    <CalendarTrigger>
      <StartBoundary>2026-09-04T07:50:00-05:00</StartBoundary>
      <Repetition><Interval>PT5M</Interval><Duration>PT10H10M</Duration><StopAtDurationEnd>true</StopAtDurationEnd></Repetition>
      <ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek>
    </CalendarTrigger>"""
        start_available = ""
        retry = ""
    else:
        triggers = f"""
    <BootTrigger><Enabled>true</Enabled></BootTrigger>
    <CalendarTrigger><StartBoundary>2026-09-04T07:00:00-05:00</StartBoundary><ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek></CalendarTrigger>
    <CalendarTrigger><StartBoundary>2026-09-04T07:15:00-05:00</StartBoundary><ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek></CalendarTrigger>
    <CalendarTrigger><StartBoundary>2026-09-04T07:45:00-05:00</StartBoundary><ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek></CalendarTrigger>
    <CalendarTrigger><StartBoundary>2026-09-04T08:15:00-05:00</StartBoundary><ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek></CalendarTrigger>
    <LogonTrigger><UserId>{escape(account)}</UserId></LogonTrigger>"""
        start_available = "<StartWhenAvailable>true</StartWhenAvailable>"
        retry = "<RestartOnFailure><Count>3</Count><Interval>PT1M</Interval></RestartOnFailure>"
    xml = f"""<?xml version="1.0"?>
<Task version="1.3" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><URI>\\{escape(name)}</URI></RegistrationInfo>
  <Principals><Principal id="Author"><UserId>{system_sid}</UserId><RunLevel>HighestAvailable</RunLevel></Principal></Principals>
  <Settings>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT15M</ExecutionTimeLimit>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    {retry}{start_available}<WakeToRun>true</WakeToRun>
  </Settings>
  <Triggers>{triggers}</Triggers>
  <Actions Context="Author"><Exec><Command>{escape(str(executable))}</Command><Arguments>{escape(arguments)}</Arguments><WorkingDirectory>{escape(str(project_root))}</WorkingDirectory></Exec></Actions>
</Task>"""
    return {
        "name": name,
        "present": True,
        "task_path": "\\",
        "state": "Ready",
        "enabled": True,
        "run_level": "Highest",
        "principal_user_id": "SYSTEM",
        "principal_sid": system_sid,
        "principal_logon_type": "ServiceAccount",
        "last_run_time": "2026-09-04T07:50:00-05:00",
        "last_result": 0,
        "current_user_sid": sid,
        "current_user_account": account,
        "discovered_task_paths": ["\\"],
        "xml": xml,
    }


def test_scheduled_task_argument_tokenizer_preserves_quoted_file_and_rejects_unclosed_quote():
    arguments = (
        '-NoProfile -NonInteractive -ExecutionPolicy Bypass '
        '-File "C:\\Market Pin\\start_market_day.ps1" '
        '-EnableRutCanary -SkipClockSync'
    )
    assert opening_capture_inspector._task_argument_tokens(arguments) == [
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        r"C:\Market Pin\start_market_day.ps1",
        "-EnableRutCanary",
        "-SkipClockSync",
    ]
    assert opening_capture_inspector._task_argument_tokens(
        '-File "C:\\Market Pin\\start_market_day.ps1'
    ) is None


def test_exact_scheduled_task_xml_contract_accepts_canonical_registration(tmp_path):
    for name in (
        "MarketPinPredictor_AutoStart",
        "MarketPinPredictor_Watchdog",
    ):
        row = _scheduled_task_xml_row(name, tmp_path)
        task = opening_capture_inspector._normalize_scheduled_task_contract(
            row, tmp_path
        )

        assert task["contract_reasons"] == []
        assert task["task_path_valid"] is True
        assert task["principal_system_account"] is True
        assert task["logon_type_service_account"] is True
        assert task["action_count_valid"] is True
        assert task["action_shape_valid"] is True
        assert task["executable_matches_system32_powershell"] is True
        assert task["working_directory_matches_project"] is True
        assert task["arguments_match_contract"] is True
        assert task["trigger_contract_valid"] is True
        assert task["multiple_instances_ignore_new"] is True
        assert task["execution_limit_valid"] is True
        assert task["battery_policy_valid"] is True
        assert task["restart_policy_valid"] is True
        assert task["start_when_available_policy_valid"] is True


def test_exact_scheduled_task_xml_contract_rejects_tampered_variants(tmp_path):
    name = "MarketPinPredictor_Watchdog"
    base = _scheduled_task_xml_row(name, tmp_path)
    variants = []

    task_path = deepcopy(base)
    task_path["task_path"] = "\\Tampered\\"
    variants.append(("task_path_valid", task_path))

    duplicate_path = deepcopy(base)
    duplicate_path["discovered_task_paths"] = ["\\", "\\Tampered\\"]
    variants.append(("task_path_valid", duplicate_path))

    user = deepcopy(base)
    user["xml"] = user["xml"].replace("<UserId>S-1-5-18</UserId>", "<UserId>S-1-5-19</UserId>", 1)
    variants.append(("principal_system_account", user))

    reported_user = deepcopy(base)
    reported_user["principal_sid"] = "S-1-5-19"
    variants.append(("principal_system_account", reported_user))

    logon = deepcopy(base)
    logon["principal_logon_type"] = "Password"
    logon["xml"] = logon["xml"].replace(
        "<UserId>S-1-5-18</UserId>",
        "<UserId>S-1-5-18</UserId><LogonType>Password</LogonType>",
        1,
    )
    variants.append(("logon_type_service_account", logon))

    replacements = {
        "executable_matches_system32_powershell": (
            r"System32\WindowsPowerShell\v1.0\powershell.exe",
            r"System32\cmd.exe",
        ),
        "working_directory_matches_project": (
            f"<WorkingDirectory>{escape(str(tmp_path))}</WorkingDirectory>",
            "<WorkingDirectory>C:\\Windows</WorkingDirectory>",
        ),
        "arguments_match_contract": (
            "-EnableRutCanary -SkipClockSync</Arguments>",
            "-EnableRutCanary -SkipClockSync -Extra</Arguments>",
        ),
        "trigger_contract_valid": (
            "2026-09-04T07:50:00-05:00",
            "2026-09-04T07:51:00-05:00",
        ),
        "multiple_instances_ignore_new": ("IgnoreNew", "Parallel"),
        "execution_limit_valid": ("PT15M", "PT1H"),
        "battery_policy_valid": (
            "<DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>",
            "<DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>",
        ),
        "restart_policy_valid": (
            "<WakeToRun>true</WakeToRun>",
            "<RestartOnFailure><Count>3</Count><Interval>PT1M</Interval></RestartOnFailure><WakeToRun>true</WakeToRun>",
        ),
    }
    for expected_field, (old, new) in replacements.items():
        variant = deepcopy(base)
        assert old in variant["xml"]
        variant["xml"] = variant["xml"].replace(old, new, 1)
        variants.append((expected_field, variant))

    action_count = deepcopy(base)
    action_count["xml"] = action_count["xml"].replace(
        "</Actions>",
        "<Exec><Command>C:\\Windows\\System32\\cmd.exe</Command></Exec></Actions>",
    )
    variants.append(("action_count_valid", action_count))

    for expected_field, row in variants:
        task = opening_capture_inspector._normalize_scheduled_task_contract(
            row, tmp_path
        )
        assert task[expected_field] is False, expected_field


def test_autostart_trigger_and_retry_contract_rejects_missing_logon_and_start_available(tmp_path):
    name = "MarketPinPredictor_AutoStart"
    row = _scheduled_task_xml_row(name, tmp_path)
    row["xml"] = row["xml"].replace(
        r"<LogonTrigger><UserId>DESKTOP-TEST\LukeD</UserId></LogonTrigger>",
        "",
    ).replace("<StartWhenAvailable>true</StartWhenAvailable>", "")
    task = opening_capture_inspector._normalize_scheduled_task_contract(row, tmp_path)

    assert task["trigger_contract_valid"] is False
    assert task["start_when_available_policy_valid"] is False
    assert task["restart_policy_valid"] is False


def test_autostart_trigger_contract_requires_exact_0715_retry(tmp_path):
    name = "MarketPinPredictor_AutoStart"
    row = _scheduled_task_xml_row(name, tmp_path)
    retry_trigger = """    <CalendarTrigger><StartBoundary>2026-09-04T07:15:00-05:00</StartBoundary><ScheduleByWeek><WeeksInterval>1</WeeksInterval><DaysOfWeek><Monday/><Tuesday/><Wednesday/><Thursday/><Friday/></DaysOfWeek></ScheduleByWeek></CalendarTrigger>
"""
    assert retry_trigger in row["xml"]
    row["xml"] = row["xml"].replace(retry_trigger, "", 1)

    task = opening_capture_inspector._normalize_scheduled_task_contract(row, tmp_path)

    assert task["trigger_contract_valid"] is False


def test_autostart_trigger_contract_requires_exactly_one_plain_boot_trigger(tmp_path):
    name = "MarketPinPredictor_AutoStart"
    base = _scheduled_task_xml_row(name, tmp_path)

    missing = deepcopy(base)
    missing["xml"] = missing["xml"].replace(
        "<BootTrigger><Enabled>true</Enabled></BootTrigger>", "", 1
    )
    duplicate = deepcopy(base)
    duplicate["xml"] = duplicate["xml"].replace(
        "<BootTrigger><Enabled>true</Enabled></BootTrigger>",
        "<BootTrigger><Enabled>true</Enabled></BootTrigger>" * 2,
        1,
    )
    disabled = deepcopy(base)
    disabled["xml"] = disabled["xml"].replace(
        "<BootTrigger><Enabled>true</Enabled></BootTrigger>",
        "<BootTrigger><Enabled>false</Enabled></BootTrigger>",
        1,
    )
    delayed = deepcopy(base)
    delayed["xml"] = delayed["xml"].replace(
        "<BootTrigger><Enabled>true</Enabled></BootTrigger>",
        "<BootTrigger><Enabled>true</Enabled><Delay>PT5M</Delay></BootTrigger>",
        1,
    )

    for row in (missing, duplicate, disabled, delayed):
        task = opening_capture_inspector._normalize_scheduled_task_contract(
            row, tmp_path
        )
        assert task["trigger_contract_valid"] is False


def test_clock_parser_requires_w32time_and_small_measured_offset():
    good = parse_clock_outputs(
        "Leap Indicator: 0(no warning)",
        "06:00:00, +00.0100000s\n06:00:02, +00.0120000s\n",
    )
    bad = parse_clock_outputs(
        "Leap Indicator: 3(not synchronized)",
        "06:00:00, +05.0700000s\n06:00:02, +05.0800000s\n",
    )

    assert good["synchronized"] is True
    assert good["median_offset_seconds"] == 0.011
    assert bad["synchronized"] is False
    assert bad["median_offset_seconds"] == 5.075

    windows_only = parse_clock_outputs("Leap Indicator: 0(no warning)", "")
    assert windows_only["windows_time_synchronized"] is True
    assert windows_only["external_offset_verified"] is False
    assert windows_only["synchronized"] is None
    assert windows_only["status"] == (
        "windows_time_synchronized_external_offset_unavailable"
    )


def test_opening_evaluator_requires_due_orb_authority_and_core_rows():
    now = datetime(2026, 9, 4, 8, 36, tzinfo=CT)
    health = _health()
    live = {
        "ok": True,
        "payload": {
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "subscription_generation": 7,
            "active_generation": 7,
            "handoff_status": "active",
            "stream_connected": True,
            "stream_progressing": True,
            "collection_ready": True,
            "calculation_ready": True,
            "prediction_pipeline_ok": True,
        },
    }
    symbols = {
        symbol: {
            "opening_ranges": {
                "5m": {
                    "capture_status": "complete",
                    "directional_evidence_eligible": symbol != "VIX",
                    "capture_evidence": {"capture_ratio": 1.0},
                }
            },
            "reference_semantics": {
                "directional_base_eligible": symbol not in {"VIX"}
            },
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }
    orb = {
        "ok": True,
        "payload": _runtime_bound_orb({
            "configured_symbols": list(symbols),
            "symbols": symbols,
        }),
    }

    state, issues, warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "ready"
    assert issues == []
    assert warnings == []

    health["payload"]["optional_family_canary"] = {
        "state": "protected_opening_orb",
        "evaluation_evidence": {
            "opening_orb_protection_active": True,
            "cash_session_reconnect_protection_active": True,
        },
    }
    state, issues, _ = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "degraded"
    assert "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE" in issues

    health["payload"]["optional_family_canary"] = {
        "state": "protected_cash_session",
        "evaluation_evidence": {
            "opening_orb_protection_active": False,
            "cash_session_reconnect_protection_active": True,
        },
    }
    state, issues, _ = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "degraded"
    assert "RUT_CANARY_CASH_SESSION_PROTECTION_ACTIVE" in issues
    assert "RUT_CANARY_STATE_INVALID:protected_cash_session" not in issues
    health["payload"]["optional_family_canary"] = {"state": "armed"}

    orb["payload"]["symbols"]["VIX"]["opening_ranges"]["5m"][
        "directional_evidence_eligible"
    ] = True
    state, issues, _ = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "degraded"
    assert "VIX_ORB_AUTHORITY_INVALID" in issues

    orb["payload"]["symbols"]["VIX"]["opening_ranges"]["5m"][
        "directional_evidence_eligible"
    ] = False
    health["payload"]["processing_clock_telemetry"]["status"] = "unknown"
    state, issues, _ = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "degraded"
    assert "BACKEND_PROCESSING_CLOCK_NOT_SYNCHRONIZED" in issues


def _evaluate_regular_fixture(fixture):
    return evaluate(
        now_ct=datetime(2026, 9, 4, 8, 36, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=fixture["health"],
        live=fixture["live"],
        dashboard={"ok": True},
        orb=fixture["orb"],
        database=fixture["database"],
        scheduled_tasks=_tasks(),
    )


def test_inspector_accepts_strict_primary_active_subscription_stage():
    fixture = _regular_acceptance_fixture()
    fixture["health"] = _primary_active_health()

    state, issues, warnings = _evaluate_regular_fixture(fixture)

    assert state == "ready"
    assert issues == []
    assert warnings == []


def test_inspector_rejects_terminal_and_broken_primary_subscription_stage():
    cases = {
        "frozen": (
            lambda payload: payload["subscription_staging"].update(state="frozen"),
            "SUBSCRIPTION_STAGING_STATE_INVALID:frozen",
        ),
        "canceled": (
            lambda payload: payload["subscription_staging"].update(
                state="canceled"
            ),
            "SUBSCRIPTION_STAGING_STATE_INVALID:canceled",
        ),
        "count": (
            lambda payload: payload["subscription_staging"].update(
                deferred_contract_count=(
                    payload["subscription_staging"]["deferred_contract_count"] - 1
                )
            ),
            "SUBSCRIPTION_STAGING_COUNT_MISMATCH",
        ),
        "hash": (
            lambda payload: payload["subscription_staging"].update(
                full_selected_universe_sha256="f" * 64
            ),
            "SUBSCRIPTION_STAGING_HASH_MISMATCH",
        ),
        "family-count": (
            lambda payload: payload["market_subscription_status"]["RUT"].update(
                active_contract_count=(
                    payload["market_subscription_status"]["RUT"][
                        "active_contract_count"
                    ]
                    - 2
                )
            ),
            "SUBSCRIPTION_STAGING_FAMILY_COUNT_MISMATCH:RUT",
        ),
        "primary-plan": (
            lambda payload: payload["subscription_staging"][
                "primary_contract_counts"
            ].update(RUT=180),
            "SUBSCRIPTION_STAGING_PRIMARY_COUNT_MISMATCH:RUT",
        ),
        "missing-family": (
            lambda payload: payload["subscription_staging"][
                "requested_orb_families"
            ].remove("RUT"),
            "SUBSCRIPTION_STAGING_REQUESTED_FAMILIES_MISSING:RUT",
        ),
    }

    for name, (mutate, expected) in cases.items():
        fixture = _regular_acceptance_fixture()
        fixture["health"] = _primary_active_health()
        mutate(fixture["health"]["payload"])

        state, issues, _warnings = _evaluate_regular_fixture(fixture)

        assert state == "degraded", name
        assert expected in issues, (name, issues)


def test_opening_evaluator_rejects_stale_or_mismatched_loaded_code_fingerprint():
    baseline = _regular_acceptance_fixture()
    assert _evaluate_regular_fixture(baseline)[:2] == ("ready", [])

    cases = []

    fixture = deepcopy(baseline)
    del fixture["health"]["payload"]["loaded_code_fingerprint"]
    cases.append((fixture, "loaded_code_fingerprint_missing"))

    fixture = deepcopy(baseline)
    fixture["health"]["payload"]["loaded_code_fingerprint"][
        "captured_at_utc"
    ] = "2026-09-03T12:45:00+00:00"
    cases.append((fixture, "loaded_code_fingerprint_not_current_session"))

    fixture = deepcopy(baseline)
    fixture["health"]["payload"]["loaded_code_fingerprint"]["files"].pop(
        "backend/market_structure.py"
    )
    cases.append((fixture, "loaded_code_fingerprint_source_set_invalid"))

    fixture = deepcopy(baseline)
    fixture["health"]["payload"]["loaded_code_fingerprint"]["files"][
        "backend/market_structure.py"
    ]["loaded_at_startup_sha256"] = "0" * 64
    cases.append(
        (
            fixture,
            "loaded_code_fingerprint_source_mismatch:backend/market_structure.py",
        )
    )

    for fixture, reason in cases:
        state, issues, _warnings = _evaluate_regular_fixture(fixture)
        assert state == "degraded"
        assert f"LOADED_CODE_FINGERPRINT_INVALID:{reason}" in issues


def test_opening_evaluator_rejects_generation_mismatch_mixing_and_invalidity():
    baseline = _regular_acceptance_fixture()
    assert _evaluate_regular_fixture(baseline)[:2] == ("ready", [])

    cases = []

    fixture = deepcopy(baseline)
    fixture["health"]["payload"]["active_generation"] = "7"
    cases.append((fixture, "ACTIVE_GENERATION_INVALID_OR_MISSING"))

    fixture = deepcopy(baseline)
    fixture["live"]["payload"]["subscription_generation"] = 6
    cases.append((fixture, "LIVE_HEALTH_SUBSCRIPTION_GENERATION_MISMATCH"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["active_runtime_context"][
        "subscription_generation"
    ] = 6
    cases.append((fixture, "ORB_RUNTIME_SUBSCRIPTION_GENERATION_MISMATCH"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["SPX"]["provenance"][
        "active_subscription_generation"
    ] = 6
    cases.append((fixture, "ORB_SYMBOL_RUNTIME_GENERATION_MISMATCH:SPX"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["SPX"]["provenance"][
        "subscription_generations"
    ] = [6, 7]
    cases.append((fixture, "ORB_SYMBOL_RANGE_GENERATION_MISMATCH:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["market_structure_rows"]["SPX"][
        "latest_subscription_generation"
    ] = 6
    cases.append((fixture, "GEX_STRUCTURE_SUBSCRIPTION_GENERATION_MISMATCH:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["orb_reference_rows"]["SPX"].update(
        latest_subscription_generation=6,
        subscription_generations=[6],
    )
    cases.append((fixture, "ORB_REFERENCE_SUBSCRIPTION_GENERATION_MISMATCH:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["orb_reference_rows"]["SPX"].update(
        subscription_generations=[6, 7],
        mixed_subscription_generation_rows=True,
    )
    cases.append((fixture, "ORB_REFERENCE_GENERATION_MIXED:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["orb_reference_rows"]["SPX"][
        "invalid_subscription_generation_row_count"
    ] = 1
    cases.append((fixture, "ORB_REFERENCE_GENERATION_INVALID:SPX"))

    for fixture, expected_issue in cases:
        state, issues, _warnings = _evaluate_regular_fixture(fixture)
        assert state == "degraded"
        assert expected_issue in issues


def test_opening_evaluator_reports_optional_market_structure_as_context_warning():
    for symbol in ("VIX", "RUT"):
        fixture = _regular_acceptance_fixture()
        fixture["database"]["market_structure_rows"].pop(symbol)

        state, issues, warnings = _evaluate_regular_fixture(fixture)

        assert state == "ready"
        assert issues == []
        assert f"OPTIONAL_VALID_GEX_STRUCTURE_NOT_ADVANCING:{symbol}" in warnings


def test_opening_evaluator_fails_closed_without_current_core_pin_evidence():
    baseline = _regular_acceptance_fixture()
    cases = []

    fixture = deepcopy(baseline)
    fixture["database"]["market_structure_rows"]["SPX"]["latest_gamma_pin"] = None
    cases.append((fixture, "GAMMA_PIN_UNAVAILABLE:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["market_structure_rows"]["NDX"]["latest_max_pain"] = None
    cases.append((fixture, "MAX_PAIN_UNAVAILABLE:NDX"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["SPX"].pop("pin_behavior")
    cases.append((fixture, "ORB_PIN_LEVELS_UNAVAILABLE:SPX"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["NDX"]["provenance"][
        "structure_reference_status"
    ] = "mismatch"
    cases.append((fixture, "ORB_PROVENANCE_STRUCTURE_NOT_ALIGNED:NDX"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["SPX"]["opening_ranges"]["5m"][
        "combined_structure_directional_evidence_eligible"
    ] = False
    cases.append((fixture, "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:SPX"))

    fixture = deepcopy(baseline)
    fixture["database"]["market_structure_rows"]["NDX"].update(
        latest_primary_expiration="2026-09-08",
        latest_same_day_profile_available=False,
    )
    cases.append((fixture, "GEX_STRUCTURE_PRIMARY_EXPIRATION_NOT_SAME_DAY:NDX"))

    for fixture, expected_issue in cases:
        state, issues, _warnings = _evaluate_regular_fixture(fixture)
        assert state == "degraded"
        assert expected_issue in issues


def test_opening_evaluator_accepts_newest_unbound_row_with_verified_bound_lineage():
    fixture = _regular_acceptance_fixture()
    for symbol in ("SPX", "NDX"):
        fixture["database"]["market_structure_rows"][symbol][
            "latest_calculation_id"
        ] = None
        fixture["orb"]["payload"]["symbols"][symbol]["last_known_structure"][
            "calculation_id"
        ] = None

    state, issues, _warnings = _evaluate_regular_fixture(fixture)

    assert state == "ready"
    assert issues == []


def test_opening_evaluator_fails_closed_on_bound_or_lineage_regression():
    fixture = _regular_acceptance_fixture()
    fixture["database"]["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["row_count"] = 0
    state, issues, _warnings = _evaluate_regular_fixture(fixture)
    assert state == "degraded"
    assert "GEX_CALCULATION_BOUND_STRUCTURE_MISSING:SPX" in issues

    fixture = _regular_acceptance_fixture()
    fixture["database"]["market_structure_rows"]["NDX"][
        "latest_calculation_bound"
    ]["lineage"].update(
        status="invalid",
        reason="GAMMA_CALCULATION_INPUT_BLOB_MISSING",
        input_blob_present=False,
    )
    state, issues, _warnings = _evaluate_regular_fixture(fixture)
    assert state == "degraded"
    assert "GEX_CALCULATION_LINEAGE_INVALID:NDX" in issues


def test_orb_acceptance_scope_excludes_only_exact_independent_gex_issues():
    fixture = _regular_acceptance_fixture()
    fixture["database"]["market_structure_rows"]["SPX"]["latest_gamma_pin"] = None
    fixture["orb"]["payload"]["symbols"]["SPX"]["opening_ranges"]["5m"][
        "combined_structure_directional_evidence_eligible"
    ] = False
    state, issues, _warnings = _evaluate_regular_fixture(fixture)
    assert state == "degraded"

    scoped_state, scoped_issues, excluded = scope_acceptance_evaluation(
        state=state,
        issues=issues,
        acceptance_scope="complete_5m_orb",
    )
    assert scoped_state == "ready"
    assert scoped_issues == []
    assert excluded == [
        "GAMMA_PIN_UNAVAILABLE:SPX",
        "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:SPX",
    ]

    retained_state, retained_issues, retained_excluded = (
        scope_acceptance_evaluation(
            state="degraded",
            issues=issues + ["LIVE_GATE_FAILED:stream_progressing"],
            acceptance_scope="complete_5m_orb",
        )
    )
    assert retained_state == "degraded"
    assert retained_issues == ["LIVE_GATE_FAILED:stream_progressing"]
    assert retained_excluded == excluded

    gamma_live_gate_issues = [
        "LIVE_GATE_FAILED:calculation_ready",
        "LIVE_GATE_FAILED:prediction_pipeline_ok",
    ]
    orb_state, orb_issues, orb_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=gamma_live_gate_issues,
        acceptance_scope="complete_5m_orb",
    )
    assert orb_state == "ready"
    assert orb_issues == []
    assert orb_excluded == gamma_live_gate_issues

    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=gamma_live_gate_issues,
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "degraded"
    assert gamma_issues == gamma_live_gate_issues
    assert gamma_excluded == []

    protected_orb_state, protected_orb_issues, protected_orb_excluded = (
        scope_acceptance_evaluation(
            state="degraded",
            issues=["RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE"],
            acceptance_scope="complete_5m_orb",
        )
    )
    assert protected_orb_state == "ready"
    assert protected_orb_issues == []
    assert protected_orb_excluded == [
        "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE"
    ]

    invalid_protection_state, invalid_protection_issues, _ = (
        scope_acceptance_evaluation(
            state="degraded",
            issues=["RUT_CANARY_PROTECTION_EVIDENCE_MISSING"],
            acceptance_scope="complete_5m_orb",
        )
    )
    assert invalid_protection_state == "degraded"
    assert invalid_protection_issues == [
        "RUT_CANARY_PROTECTION_EVIDENCE_MISSING"
    ]

    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state=state,
        issues=issues,
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "degraded"
    assert gamma_issues == ["GAMMA_PIN_UNAVAILABLE:SPX"]
    assert gamma_excluded == [
        "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:SPX"
    ]

    core_orb_only_issues = [
        "ORB_REFERENCE_DECISION_TABLE_MISSING",
        "ORB_REFERENCE_DECISION_SCHEMA_INCOMPATIBLE",
        "ORB_REFERENCE_DECISION_IMMUTABILITY_TRIGGERS_MISSING",
        "ORB_REFERENCE_DECISION_IDENTITY_INDEX_MISSING",
        "ORB_REFERENCE_DECISION_PENDING:SPX",
        "ORB_REFERENCE_OPENING_BUCKET_MISSING:SPX",
        "ORB_REFERENCE_EPOCH_MIXED:NDX",
        "ORB_5m_INCOMPLETE:SPX",
        "ORB_5m_CAPTURE_RATIO_LOW:NDX",
        "ORB_5m_NOT_DIRECTIONAL_ELIGIBLE:SPX",
        "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:NDX",
    ]
    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=core_orb_only_issues,
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "ready"
    assert gamma_issues == []
    assert gamma_excluded == core_orb_only_issues

    shared_core_issues = [
        "SUBSCRIPTION_ROOT_MISSING:SPX",
        "ORB_SYMBOL_RUNTIME_EPOCH_MISMATCH:NDX",
        "ORB_SYMBOL_NOT_CONFIGURED:SPX",
        "GAMMA_PIN_UNAVAILABLE:NDX",
    ]
    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=shared_core_issues,
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "degraded"
    assert gamma_issues == shared_core_issues
    assert gamma_excluded == []

    bound_issues = [
        "GEX_CALCULATION_BOUND_STRUCTURE_MISSING:SPX",
        "GEX_CALCULATION_BOUND_STRUCTURE_STALE:NDX",
        "GEX_CALCULATION_BOUND_STRUCTURE_INVALID:SPX",
        "GEX_CALCULATION_BOUND_IDENTITY_MISMATCH:NDX",
        "GEX_CALCULATION_LINEAGE_INVALID:SPX",
    ]
    orb_state, orb_issues, orb_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=bound_issues,
        acceptance_scope="complete_5m_orb",
    )
    assert orb_state == "ready"
    assert orb_issues == []
    assert orb_excluded == bound_issues

    optional_issues = [
        "SUBSCRIPTION_ROOT_MISSING:RUT",
        "ORB_REFERENCE_NOT_ADVANCING:VIX",
        "ORB_SYMBOL_NOT_CONFIGURED:RUT",
        "RUT_CANARY_STATE_INVALID:rolled_back",
    ]
    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=optional_issues,
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "ready"
    assert gamma_issues == []
    assert gamma_excluded == optional_issues

    gamma_state, gamma_issues, gamma_excluded = scope_acceptance_evaluation(
        state="degraded",
        issues=optional_issues
        + ["LIVE_GATE_FAILED:stream_progressing", "ORB_SYMBOL_NOT_CONFIGURED:SPX"],
        acceptance_scope="first_eligible_gamma_capture",
    )
    assert gamma_state == "degraded"
    assert gamma_issues == [
        "LIVE_GATE_FAILED:stream_progressing",
        "ORB_SYMBOL_NOT_CONFIGURED:SPX",
    ]
    assert gamma_excluded == optional_issues

    rut_capture_state, rut_capture_issues, rut_capture_excluded = (
        scope_acceptance_evaluation(
            state="degraded",
            issues=["RUT_ORB_CONTEXT_ONLY"],
            acceptance_scope="complete_5m_orb",
        )
    )
    assert rut_capture_state == "ready"
    assert rut_capture_issues == []
    assert rut_capture_excluded == ["RUT_ORB_CONTEXT_ONLY"]


def test_opening_evaluator_requires_active_handoff_across_live_authorities():
    baseline = _regular_acceptance_fixture()
    cases = []

    fixture = deepcopy(baseline)
    fixture["health"]["payload"]["handoff_status"] = "warming"
    cases.append((fixture, "HEALTH_HANDOFF_NOT_ACTIVE"))

    fixture = deepcopy(baseline)
    fixture["live"]["payload"]["handoff_status"] = "warming"
    cases.append((fixture, "LIVE_HEALTH_HANDOFF_NOT_ACTIVE"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["active_runtime_context"]["handoff_status"] = "warming"
    cases.append((fixture, "ORB_RUNTIME_HANDOFF_NOT_ACTIVE"))

    fixture = deepcopy(baseline)
    fixture["orb"]["payload"]["symbols"]["NDX"]["provenance"][
        "active_handoff_status"
    ] = "warming"
    cases.append((fixture, "ORB_SYMBOL_HANDOFF_NOT_ACTIVE:NDX"))

    for fixture, expected_issue in cases:
        state, issues, _warnings = _evaluate_regular_fixture(fixture)
        assert state == "degraded"
        assert expected_issue in issues


def test_preopen_external_clock_probe_outage_warns_without_false_failure():
    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 8, 20, tzinfo=CT),
        clock={
            "applicable": True,
            "synchronized": None,
            "windows_time_synchronized": True,
            "external_offset_verified": False,
        },
        health=_health(),
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb={
            "ok": True,
            "payload": {
                "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
                "symbols": {},
            },
        },
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "ready"
    assert issues == []
    assert warnings == ["PROCESSING_CLOCK_EXTERNAL_OFFSET_UNAVAILABLE"]


def test_startup_has_bounded_warming_grace_before_endpoint_acceptance():
    inputs = {
        "clock": {"applicable": True, "synchronized": True},
        "health": {"ok": False},
        "live": {"ok": False},
        "dashboard": {"ok": False},
        "orb": {"ok": False},
        "database": _database(),
        "scheduled_tasks": _tasks(),
    }

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 46, tzinfo=CT),
        **inputs,
    )
    assert state == "preflight"
    assert issues == []
    assert warnings == ["STARTUP_ACCEPTANCE_WARMING"]

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 56, tzinfo=CT),
        **inputs,
    )
    assert state == "degraded"
    assert "BACKEND_HEALTH_UNREACHABLE" in issues
    assert "BACKEND_LIVE_HEALTH_UNREACHABLE" in issues
    assert "DASHBOARD_HEALTH_UNREACHABLE" in issues
    assert "ORB_ENDPOINT_UNREACHABLE" in issues
    assert "STARTUP_ACCEPTANCE_WARMING" not in warnings


def test_preopen_canary_deferral_is_explicit_and_does_not_latch_failure():
    health = _health()
    health["payload"]["optional_family_canary"] = {
        "state": "deferred_preopen",
        "evaluation_evidence": {"evaluation_deferred": True},
    }

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 8, 20, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb={
            "ok": True,
            "payload": {
                "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
                "symbols": {},
            },
        },
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "ready"
    assert issues == []
    assert warnings == ["RUT_CANARY_EVALUATION_DEFERRED_PREOPEN"]


def test_all_due_windows_are_validated_and_compacted_without_latest_masking_earlier():
    now = datetime(2026, 9, 4, 9, 31, tzinfo=CT)
    windows = {
        name: {
            "capture_status": "complete",
            "directional_evidence_eligible": True,
            "capture_evidence": {"capture_ratio": 1.0},
        }
        for name in ("5m", "15m", "30m", "60m")
    }
    symbols = {
        symbol: {
            "opening_ranges": deepcopy(windows),
            "reference_semantics": {"directional_base_eligible": True},
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }
    for window in symbols["VIX"]["opening_ranges"].values():
        window["directional_evidence_eligible"] = False
    symbols["SPX"]["opening_ranges"]["5m"].update(
        capture_status="partial",
        directional_evidence_eligible=False,
        capture_evidence={"capture_ratio": 0.90},
    )
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": list(symbols),
            "symbols": symbols,
        },
    }
    live = {
        "ok": True,
        "payload": {
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "subscription_generation": 7,
            "active_generation": 7,
            "handoff_status": "active",
            "stream_connected": True,
            "stream_progressing": True,
            "collection_ready": True,
            "calculation_ready": True,
            "prediction_pipeline_ok": True,
        },
    }

    state, issues, _warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live=live,
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "degraded"
    assert "ORB_5m_INCOMPLETE:SPX" in issues
    assert "ORB_5m_CAPTURE_RATIO_LOW:SPX" in issues
    assert "ORB_60m_INCOMPLETE:SPX" not in issues

    compact = compact_report(
        {
            "schema_version": "marketpin-opening-readiness.v1",
            "state": state,
            "observed_at_ct": now.isoformat(),
            "observed_at_utc": now.astimezone(timezone.utc).isoformat(),
            "issues": issues,
            "warnings": [],
            "orb": orb,
        }
    )
    assert compact["due_orb_windows"] == ["5m", "15m", "30m", "60m"]
    assert compact["due_orb_window"] == "60m"
    assert set(
        compact["orb"]["payload"]["symbols"]["SPX"]["opening_ranges"]
    ) == {"5m", "15m", "30m", "60m"}


def test_opening_evaluator_fails_closed_on_rut_capacity_or_canary_regression():
    now = datetime(2026, 9, 4, 8, 20, tzinfo=CT)
    health = _health()
    health["payload"]["symbols_subscribed"] = 3_201
    health["payload"]["subscription_metadata"]["reservation_shortfall_pairs"] = 2
    health["payload"]["optional_family_canary"]["state"] = "rolled_back"
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
            "symbols": {},
        },
    }

    state, issues, warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "degraded"
    assert "RUT_CANARY_SUBSCRIPTION_CAP_EXCEEDED" in issues
    assert "RUT_RESERVATION_SHORTFALL_NONZERO" in issues
    assert "RUT_CANARY_STATE_INVALID:rolled_back" in issues
    assert warnings == []


def test_opening_evaluator_accepts_raw_option_root_aliases_without_family_summary():
    health = _health()
    health["payload"].pop("core_symbol_status")
    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 8, 20, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb={
            "ok": True,
            "payload": {
                "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
                "symbols": {},
            },
        },
        database=_database(),
        scheduled_tasks=_tasks(),
    )

    assert state == "ready"
    assert issues == []
    assert warnings == []


def test_opening_evaluator_requires_rut_before_upgrade_cutoff():
    health = _health()
    health["payload"]["symbols_requested"] = ["SPX", "NDX", "VIX"]
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": ["SPX", "NDX", "VIX"],
            "symbols": {},
        },
    }

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 8, 20, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "ready"
    assert issues == []
    assert warnings == ["RUT_PREOPEN_UPGRADE_PENDING"]

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 8, 25, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=_tasks(),
    )
    assert state == "degraded"
    assert "ORB_SYMBOL_NOT_CONFIGURED:RUT" in issues
    assert warnings == []


def test_opening_evaluator_requires_elevated_autostart_registration():
    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 0, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health={"ok": False, "not_due": True},
        live={"ok": False, "not_due": True},
        dashboard={"ok": False, "not_due": True},
        orb={"ok": False, "not_due": True},
        database=_database(),
        scheduled_tasks=_tasks(startup_run_level="Limited"),
    )

    assert state == "degraded"
    assert issues == ["AUTOSTART_RUN_LEVEL_NOT_HIGHEST"]
    assert warnings == []


def test_opening_evaluator_requires_watchdog_authority_to_match_elevated_listeners():
    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 0, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health={"ok": False, "not_due": True},
        live={"ok": False, "not_due": True},
        dashboard={"ok": False, "not_due": True},
        orb={"ok": False, "not_due": True},
        database=_database(),
        scheduled_tasks=_tasks(watchdog_run_level="Limited"),
    )

    assert state == "degraded"
    assert issues == ["WATCHDOG_RUN_LEVEL_NOT_HIGHEST"]
    assert warnings == []


def test_opening_evaluator_rejects_stale_pretrigger_task_runs_after_grace():
    scheduled_tasks = _tasks()
    scheduled_tasks["tasks"]["MarketPinPredictor_AutoStart"][
        "last_run_time"
    ] = "2026-09-04T01:43:01-05:00"
    scheduled_tasks["tasks"]["MarketPinPredictor_Watchdog"][
        "last_run_time"
    ] = "2026-09-03T18:00:00-05:00"
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
            "symbols": {},
        },
    }

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 53, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=scheduled_tasks,
    )

    assert state == "degraded"
    assert "AUTOSTART_DID_NOT_RUN_TODAY" in issues
    assert "WATCHDOG_DID_NOT_RUN_TODAY" in issues
    assert warnings == ["STARTUP_ACCEPTANCE_WARMING"]


def test_opening_evaluator_surfaces_exact_scheduler_contract_failures():
    scheduled_tasks = _tasks()
    startup = scheduled_tasks["tasks"]["MarketPinPredictor_AutoStart"]
    startup["task_path_valid"] = False
    startup["principal_system_account"] = False
    startup["logon_type_service_account"] = False
    startup["action_shape_valid"] = False
    startup["executable_matches_system32_powershell"] = False
    startup["arguments_match_contract"] = False
    startup["trigger_contract_valid"] = False
    startup["multiple_instances_ignore_new"] = False
    startup["execution_limit_valid"] = False
    startup["battery_policy_valid"] = False
    startup["restart_policy_valid"] = False

    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 0, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health={"ok": False, "not_due": True},
        live={"ok": False, "not_due": True},
        dashboard={"ok": False, "not_due": True},
        orb={"ok": False, "not_due": True},
        database=_database(),
        scheduled_tasks=scheduled_tasks,
    )

    assert state == "degraded"
    assert warnings == []
    assert "SCHEDULED_TASK_PATH_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_PRINCIPAL_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_LOGON_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_ACTION_SHAPE_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_EXECUTABLE_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_ARGUMENTS_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_TRIGGER_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_MULTIPLE_INSTANCES_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_EXECUTION_LIMIT_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_BATTERY_POLICY_INVALID:MarketPinPredictor_AutoStart" in issues
    assert "SCHEDULED_TASK_RETRY_POLICY_INVALID:MarketPinPredictor_AutoStart" in issues


def test_due_nonzero_task_result_fails_unless_task_is_still_running():
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
            "symbols": {},
        },
    }
    scheduled_tasks = _tasks()
    scheduled_tasks["tasks"]["MarketPinPredictor_Watchdog"]["last_result"] = 1

    _, issues, _ = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 53, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=scheduled_tasks,
    )
    assert (
        "SCHEDULED_TASK_LAST_RUN_FAILED:MarketPinPredictor_Watchdog:1" in issues
    )

    scheduled_tasks["tasks"]["MarketPinPredictor_Watchdog"]["state"] = "Running"
    _, running_issues, _ = evaluate(
        now_ct=datetime(2026, 9, 4, 7, 53, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live={"ok": True, "payload": {}},
        dashboard={"ok": True},
        orb=orb,
        database=_database(),
        scheduled_tasks=scheduled_tasks,
    )
    assert not any(
        issue.startswith("SCHEDULED_TASK_LAST_RUN_FAILED:MarketPinPredictor_Watchdog")
        for issue in running_issues
    )


def test_database_inspector_separates_current_5s_orb_evidence_from_gex(tmp_path):
    database_path = tmp_path / "opening.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE market_structure_observations ("
            "symbol TEXT, trading_date TEXT, source_timestamp_utc TEXT)"
        )
        for trigger in (
            "market_structure_observations_insert_guard",
            "market_structure_observations_no_update",
            "market_structure_observations_no_delete",
        ):
            connection.execute(
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON "
                "market_structure_observations WHEN 0 "
                "BEGIN SELECT RAISE(ABORT, 'guard'); END"
            )
        column_sql = ", ".join(
            f'"{column}" '
            + (
                "INTEGER"
                if column
                in {"subscription_generation", "same_day_profile_available"}
                else "TEXT"
            )
            for column in sorted(EXPECTED_ORB_REFERENCE_COLUMNS)
        )
        connection.execute(f"CREATE TABLE orb_reference_samples ({column_sql})")
        decision_column_sql = ", ".join(
            f'"{column}" '
            + (
                "INTEGER"
                if column == "progress_eligible"
                else "TEXT"
            )
            for column in sorted(EXPECTED_ORB_REFERENCE_DECISION_COLUMNS)
        )
        connection.execute(
            f"CREATE TABLE orb_reference_sample_decisions ({decision_column_sql})"
        )
        connection.execute(
            "CREATE UNIQUE INDEX uix_orb_reference_decision_sample_id "
            "ON orb_reference_sample_decisions (sample_id)"
        )
        connection.execute(
            "CREATE UNIQUE INDEX uix_orb_reference_logical_sample "
            "ON orb_reference_samples ("
            "symbol, sample_timestamp_utc, subscription_generation, "
            "universe_sha256, primary_expiration, spot_formula_version, "
            "risk_free_rate, symbol_mapping_version)"
        )
        opening_utc = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
        for symbol in ("SPX", "NDX"):
            for offset in range(5):
                sample = opening_utc + timedelta(seconds=offset * 5)
                connection.execute(
                    "INSERT INTO orb_reference_samples "
                    "(sample_id, symbol, trading_date, sample_timestamp_utc, "
                    "source_timestamp_utc, captured_at_utc, "
                    "subscription_epoch_id, subscription_generation, provider, "
                    "validation_status, primary_expiration, "
                    "same_day_profile_available) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        f"{symbol}-{offset}",
                        symbol,
                        "2026-09-08",
                        sample.isoformat(),
                        (sample + timedelta(seconds=4)).isoformat(),
                        (sample + timedelta(seconds=4, milliseconds=100)).isoformat(),
                        TEST_SUBSCRIPTION_EPOCH,
                        7,
                        "databento",
                        "valid",
                        "2026-09-08",
                        1,
                    ),
                )
                connection.execute(
                    "INSERT INTO orb_reference_sample_decisions "
                    "(sample_id, sample_timestamp_utc, intended_bucket_utc, "
                    "attempt_completed_at_utc, progress_eligible, reason, "
                    "decision_status) VALUES (?, ?, ?, ?, 1, NULL, 'final')",
                    (
                        f"{symbol}-{offset}",
                        sample.isoformat(),
                        sample.isoformat(),
                        (sample + timedelta(seconds=4, milliseconds=200)).isoformat(),
                    ),
                )
        off_grid = opening_utc + timedelta(seconds=1)
        connection.execute(
            "INSERT INTO orb_reference_samples "
            "(sample_id, symbol, trading_date, sample_timestamp_utc, "
            "source_timestamp_utc, captured_at_utc, "
            "subscription_epoch_id, subscription_generation, provider, "
            "validation_status, primary_expiration, "
            "same_day_profile_available) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "SPX-off-grid",
                "SPX",
                "2026-09-08",
                off_grid.isoformat(),
                (off_grid + timedelta(seconds=1)).isoformat(),
                (off_grid + timedelta(seconds=1, milliseconds=100)).isoformat(),
                TEST_SUBSCRIPTION_EPOCH,
                7,
                "databento",
                "valid",
                "2026-09-08",
                1,
            ),
        )
        connection.execute(
            "INSERT INTO orb_reference_sample_decisions "
            "(sample_id, sample_timestamp_utc, intended_bucket_utc, "
            "attempt_completed_at_utc, progress_eligible, reason, "
            "decision_status) VALUES (?, ?, ?, ?, 1, NULL, 'final')",
            (
                "SPX-off-grid",
                off_grid.isoformat(),
                off_grid.isoformat(),
                (off_grid + timedelta(seconds=1, milliseconds=200)).isoformat(),
            ),
        )
        for symbol, sample_id in (
            ("VIX", "vix-pending"),
            ("RUT", "rut-rejected"),
        ):
            sample = opening_utc
            connection.execute(
                "INSERT INTO orb_reference_samples "
                "(sample_id, symbol, trading_date, sample_timestamp_utc, "
                "source_timestamp_utc, captured_at_utc, "
                "subscription_epoch_id, subscription_generation, provider, "
                "validation_status, primary_expiration, "
                "same_day_profile_available) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    sample_id,
                    symbol,
                    "2026-09-08",
                    sample.isoformat(),
                    (sample + timedelta(milliseconds=50)).isoformat(),
                    (sample + timedelta(milliseconds=100)).isoformat(),
                    TEST_SUBSCRIPTION_EPOCH,
                    7,
                    "databento",
                    "valid",
                    "2026-09-09",
                    0,
                ),
            )
        connection.execute(
            "INSERT INTO orb_reference_sample_decisions "
            "(sample_id, sample_timestamp_utc, intended_bucket_utc, "
            "attempt_completed_at_utc, progress_eligible, reason, "
            "decision_status) VALUES (?, ?, ?, ?, 0, ?, 'final')",
            (
                "rut-rejected",
                opening_utc.isoformat(),
                opening_utc.isoformat(),
                (opening_utc + timedelta(seconds=5, milliseconds=100)).isoformat(),
                "REFERENCE_CAPTURE_COMPLETED_OUTSIDE_INTENDED_BUCKET",
            ),
        )
        for trigger in EXPECTED_ORB_REFERENCE_TRIGGERS:
            connection.execute(
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON "
                "orb_reference_samples WHEN 0 "
                "BEGIN SELECT RAISE(ABORT, 'guard'); END"
            )
        for trigger in EXPECTED_ORB_REFERENCE_DECISION_TRIGGERS:
            connection.execute(
                f"CREATE TRIGGER {trigger} BEFORE INSERT ON "
                "orb_reference_sample_decisions WHEN 0 "
                "BEGIN SELECT RAISE(ABORT, 'guard'); END"
            )
        connection.commit()
    finally:
        connection.close()

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 30, 25, tzinfo=CT),
    )

    assert report["market_structure_evidence_role"] == "gex_pin_max_pain_only"
    assert report["orb_reference_evidence_role"] == "opening_ranges_only"
    assert report["missing_orb_reference_columns"] == []
    assert report["missing_orb_reference_triggers"] == []
    assert report["missing_orb_reference_indexes"] == []
    assert set(EXPECTED_ORB_REFERENCE_INDEXES) <= set(
        report["orb_reference_indexes"]
    )
    assert report["missing_orb_reference_decision_columns"] == []
    assert report["missing_orb_reference_decision_triggers"] == []
    assert report["missing_orb_reference_decision_indexes"] == []
    assert set(EXPECTED_ORB_REFERENCE_DECISION_INDEXES) <= set(
        report["orb_reference_decision_indexes"]
    )
    assert report["orb_reference_progress_decisions"]["VIX"] == {
        "raw_row_count": 1,
        "pending_decision_count": 1,
        "ineligible_decision_count": 0,
        "eligible_decision_count": 0,
    }
    assert report["orb_reference_progress_decisions"]["RUT"] == {
        "raw_row_count": 1,
        "pending_decision_count": 0,
        "ineligible_decision_count": 1,
        "eligible_decision_count": 0,
    }
    assert "VIX" not in report["orb_reference_rows"]
    assert "RUT" not in report["orb_reference_rows"]
    assert report["orb_reference_progress_decisions"]["SPX"] == {
        "raw_row_count": 6,
        "pending_decision_count": 0,
        "ineligible_decision_count": 0,
        "eligible_decision_count": 6,
    }
    spx = report["orb_reference_rows"]["SPX"]
    # Expected-name no-op triggers cannot make an off-grid decision part of
    # the inspector's eligible projection.
    assert spx["row_count"] == 5
    assert spx["off_cadence_row_count"] == 0
    assert spx["cadence_seconds"] == 5
    assert spx["expected_opening_sample_count"] == 5
    assert spx["distinct_opening_bucket_count"] == 5
    assert spx["opening_capture_ratio"] == 1.0
    assert spx["maximum_opening_gap_seconds"] == 5.0
    assert spx["subscription_epoch_ids"] == [TEST_SUBSCRIPTION_EPOCH]
    assert spx["subscription_generations"] == [7]
    assert spx["invalid_subscription_generation_row_count"] == 0
    assert spx["mixed_subscription_generation_rows"] is False
    assert spx["latest_provider"] == "databento"
    assert spx["latest_validation_status"] == "valid"
    assert spx["latest_primary_expiration"] == "2026-09-08"
    assert spx["latest_same_day_profile_available"] is True
    assert spx["distinct_recent_provider_source_timestamp_count"] == 5
    assert spx["provider_source_timestamp_advancement_count"] == 4
    assert spx["provider_source_timestamp_regression_count"] == 0
    assert spx["provider_source_timestamps_advancing"] is True
    assert spx["advancing_5s_evidence"] is True


def test_orb_reference_evidence_rejects_a_missing_opening_bucket():
    opening_utc = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    rows = []
    for offset in range(1, 60):
        sample = opening_utc + timedelta(seconds=offset * 5)
        rows.append(
            {
                "symbol": "SPX",
                "sample_timestamp_utc": sample.isoformat(),
                "source_timestamp_utc": sample.isoformat(),
                "captured_at_utc": (sample + timedelta(milliseconds=100)).isoformat(),
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "provider": "databento",
                "validation_status": "valid",
            }
        )

    evidence = opening_capture_inspector._orb_reference_evidence(
        rows,
        trading_day=date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 35, tzinfo=CT),
    )["SPX"]

    assert evidence["opening_capture_ratio"] >= 0.95
    assert evidence["maximum_opening_gap_seconds"] == 5.0
    assert evidence["opening_bucket_present"] is False
    assert evidence["advancing_5s_evidence"] is False


def test_orb_reference_evidence_rejects_frozen_provider_source_timestamps():
    opening_utc = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    frozen_source = opening_utc
    rows = []
    for offset in range(5):
        sample = opening_utc + timedelta(seconds=offset * 5)
        rows.append(
            {
                "symbol": "SPX",
                "sample_timestamp_utc": sample.isoformat(),
                "source_timestamp_utc": frozen_source.isoformat(),
                "captured_at_utc": (sample + timedelta(milliseconds=100)).isoformat(),
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "provider": "databento",
                "validation_status": "valid",
            }
        )

    evidence = opening_capture_inspector._orb_reference_evidence(
        rows,
        trading_day=date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 30, 25, tzinfo=CT),
    )["SPX"]

    assert evidence["opening_capture_ratio"] == 1.0
    assert evidence["distinct_opening_bucket_count"] == 5
    assert evidence["distinct_recent_provider_source_timestamp_count"] == 1
    assert evidence["provider_source_timestamp_advancement_count"] == 0
    assert evidence["provider_source_timestamp_regression_count"] == 0
    assert evidence["provider_source_timestamps_advancing"] is False
    assert evidence["advancing_5s_evidence"] is False


def test_database_inspector_exposes_latest_gamma_pin_provenance(tmp_path):
    database_path = tmp_path / "structure.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            "CREATE TABLE market_structure_observations ("
            "observation_id TEXT, symbol TEXT, trading_date TEXT, "
            "source_timestamp_utc TEXT, captured_at_utc TEXT, provider TEXT, "
            "subscription_epoch_id TEXT, subscription_generation INTEGER, "
            "calculation_id TEXT, reference_price REAL, gamma_pin REAL, "
            "max_pain REAL, primary_expiration TEXT, "
            "same_day_profile_available INTEGER, universe_sha256 TEXT, "
            "validation_status TEXT)"
        )
        connection.execute(
            "INSERT INTO market_structure_observations VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "observation-spx",
                "SPX",
                "2026-09-08",
                "2026-09-08T13:35:59+00:00",
                "2026-09-08T13:35:59.500000+00:00",
                "databento",
                TEST_SUBSCRIPTION_EPOCH,
                7,
                "calculation-spx",
                7_510.0,
                7_525.0,
                7_500.0,
                "2026-09-08",
                1,
                "a" * 64,
                "valid",
            ),
        )
        connection.commit()
    finally:
        connection.close()

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    latest = report["market_structure_rows"]["SPX"]

    assert latest["latest_source_age_seconds"] == 1.0
    assert latest["latest_capture_age_seconds"] == 0.5
    assert latest["latest_provider"] == "databento"
    assert latest["latest_calculation_id"] == "calculation-spx"
    assert latest["latest_reference_price"] == 7_510.0
    assert latest["latest_gamma_pin"] == 7_525.0
    assert latest["latest_max_pain"] == 7_500.0
    assert latest["latest_primary_expiration"] == "2026-09-08"
    assert latest["latest_same_day_profile_available"] is True
    assert latest["latest_universe_sha256"] == "a" * 64
    assert latest["latest_validation_status"] == "valid"


def _write_calculation_bound_fixture(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    database_path = tmp_path / "calculation-bound.db"
    connection = sqlite3.connect(database_path)
    selected_universe = "a" * 64
    source_universe = "b" * 64
    calculation_id = "11111111-1111-1111-1111-111111111111"
    try:
        connection.executescript(
            "CREATE TABLE market_structure_observations ("
            "observation_id TEXT, symbol TEXT, trading_date TEXT, "
            "source_timestamp_utc TEXT, captured_at_utc TEXT, provider TEXT, "
            "subscription_epoch_id TEXT, subscription_generation INTEGER, "
            "calculation_id TEXT, reference_price REAL, gamma_pin REAL, "
            "max_pain REAL, primary_expiration TEXT, "
            "same_day_profile_available INTEGER, universe_sha256 TEXT, "
            "validation_status TEXT);"
            "CREATE TABLE gamma_calculation_runs ("
            "id INTEGER PRIMARY KEY, calculation_id TEXT, symbol TEXT, "
            "trading_date TEXT, calculated_at_utc TEXT, provider TEXT, "
            "subscription_epoch_id TEXT, subscription_generation INTEGER, "
            "status TEXT, input_schema_version TEXT, universe_sha256 TEXT, "
            "spot_price REAL, gamma_pin REAL, max_pain REAL);"
            "CREATE TABLE gamma_calculation_input_blobs ("
            "calculation_run_id INTEGER PRIMARY KEY, encoding TEXT, "
            "payload_sha256 TEXT, uncompressed_bytes INTEGER, "
            "compressed_bytes INTEGER, payload BLOB);"
        )
        for values in (
            (
                "bound",
                "2026-09-08T13:35:50+00:00",
                "2026-09-08T13:35:50.500000+00:00",
                calculation_id,
                7_510.0,
                7_525.0,
                7_500.0,
            ),
            (
                "newest",
                "2026-09-08T13:35:59+00:00",
                "2026-09-08T13:35:59.500000+00:00",
                None,
                7_511.0,
                7_530.0,
                7_505.0,
            ),
        ):
            connection.execute(
                "INSERT INTO market_structure_observations VALUES "
                "(?, 'SPX', '2026-09-08', ?, ?, 'databento', ?, 7, ?, ?, ?, ?, "
                "'2026-09-08', 1, ?, 'valid')",
                (
                    values[0],
                    values[1],
                    values[2],
                    TEST_SUBSCRIPTION_EPOCH,
                    values[3],
                    values[4],
                    values[5],
                    values[6],
                    selected_universe,
                ),
            )
        payload = {
            "input_schema_version": "gamma-inputs-v2-point-in-time",
            "calculation_id": calculation_id,
            "symbol": "SPX",
            "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
            "subscription_generation": 7,
            "calculated_at_utc": "2026-09-08T13:35:50+00:00",
            "raw_fresh_chain_rows": [],
            "calculated_gex_rows": [],
            "parameters": {},
            "rejection_counts": {},
            "output_summary": {
                "calculation_id": calculation_id,
                "symbol": "SPX",
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "validation_is_valid": True,
                "gamma_excluded_from_model": False,
                "price": 7_510.0,
                "gamma_pin": 7_525.0,
                "max_pain": 7_500.0,
                "primary_expiration": "2026-09-08",
                "same_day_profile_available": True,
                "selected_universe_sha256": selected_universe,
                "universe_sha256": source_universe,
                "universe_provenance": {
                    "source_sha256": source_universe,
                    "trading_date": "2026-09-08",
                    "source_date": "2026-09-08",
                    "is_fallback": False,
                },
            },
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        compressed = zlib.compress(canonical, level=6)
        connection.execute(
            "INSERT INTO gamma_calculation_runs VALUES "
            "(1, ?, 'SPX', '2026-09-08', '2026-09-08T13:35:50+00:00', "
            "'databento', ?, 7, 'valid', 'gamma-inputs-v2-point-in-time', ?, "
            "7510.0, 7525.0, 7500.0)",
            (calculation_id, TEST_SUBSCRIPTION_EPOCH, source_universe),
        )
        connection.execute(
            "INSERT INTO gamma_calculation_input_blobs VALUES "
            "(1, 'canonical-json+zlib-v1', ?, ?, ?, ?)",
            (
                hashlib.sha256(canonical).hexdigest(),
                len(canonical),
                len(compressed),
                compressed,
            ),
        )
        connection.commit()
    finally:
        connection.close()
    return database_path


def _append_verified_calculation_bound(
    database_path: Path,
    *,
    run_id: int,
    calculation_id: str,
    observation_id: str,
    calculated_at_utc: str,
    source_timestamp_utc: str,
    captured_at_utc: str,
    reference_price: float,
    gamma_pin: float,
    max_pain: float,
    symbol: str = "SPX",
    subscription_epoch_id: str = TEST_SUBSCRIPTION_EPOCH,
    subscription_generation: int = 7,
    selected_universe_sha256: str = "a" * 64,
    source_universe_sha256: str = "b" * 64,
) -> None:
    connection = sqlite3.connect(database_path)
    try:
        template_blob = connection.execute(
            "SELECT payload FROM gamma_calculation_input_blobs ORDER BY calculation_run_id "
            "LIMIT 1"
        ).fetchone()[0]
        payload = json.loads(zlib.decompress(template_blob).decode("utf-8"))
        payload.update(
            calculation_id=calculation_id,
            calculated_at_utc=calculated_at_utc,
            symbol=symbol,
            subscription_epoch_id=subscription_epoch_id,
            subscription_generation=subscription_generation,
        )
        payload["output_summary"].update(
            calculation_id=calculation_id,
            symbol=symbol,
            subscription_epoch_id=subscription_epoch_id,
            subscription_generation=subscription_generation,
            price=reference_price,
            gamma_pin=gamma_pin,
            max_pain=max_pain,
            selected_universe_sha256=selected_universe_sha256,
            universe_sha256=source_universe_sha256,
        )
        payload["output_summary"]["universe_provenance"][
            "source_sha256"
        ] = source_universe_sha256
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        compressed = zlib.compress(canonical, level=6)
        connection.execute(
            "INSERT INTO gamma_calculation_runs VALUES "
            "(?, ?, ?, '2026-09-08', ?, 'databento', ?, ?, 'valid', "
            "'gamma-inputs-v2-point-in-time', ?, ?, ?, ?)",
            (
                run_id,
                calculation_id,
                symbol,
                calculated_at_utc,
                subscription_epoch_id,
                subscription_generation,
                source_universe_sha256,
                reference_price,
                gamma_pin,
                max_pain,
            ),
        )
        connection.execute(
            "INSERT INTO gamma_calculation_input_blobs VALUES "
            "(?, 'canonical-json+zlib-v1', ?, ?, ?, ?)",
            (
                run_id,
                hashlib.sha256(canonical).hexdigest(),
                len(canonical),
                len(compressed),
                compressed,
            ),
        )
        connection.execute(
            "INSERT INTO market_structure_observations VALUES "
            "(?, ?, '2026-09-08', ?, ?, 'databento', ?, ?, ?, ?, ?, ?, "
            "'2026-09-08', 1, ?, 'valid')",
            (
                observation_id,
                symbol,
                source_timestamp_utc,
                captured_at_utc,
                subscription_epoch_id,
                subscription_generation,
                calculation_id,
                reference_price,
                gamma_pin,
                max_pain,
                selected_universe_sha256,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _rewrite_calculation_input_payload(database_path, mutate):
    connection = sqlite3.connect(database_path)
    try:
        compressed = connection.execute(
            "SELECT payload FROM gamma_calculation_input_blobs"
        ).fetchone()[0]
        payload = json.loads(zlib.decompress(compressed).decode("utf-8"))
        mutate(payload)
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        compressed = zlib.compress(canonical, level=6)
        connection.execute(
            "UPDATE gamma_calculation_input_blobs SET "
            "payload_sha256=?, uncompressed_bytes=?, compressed_bytes=?, payload=?",
            (
                hashlib.sha256(canonical).hexdigest(),
                len(canonical),
                len(compressed),
                compressed,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _append_verified_ndx_peer(database_path: Path, *, run_id: int = 2) -> None:
    _append_verified_calculation_bound(
        database_path,
        run_id=run_id,
        calculation_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        observation_id="bound-ndx",
        calculated_at_utc="2026-09-08T13:35:51+00:00",
        source_timestamp_utc="2026-09-08T13:35:51+00:00",
        captured_at_utc="2026-09-08T13:35:51.500000+00:00",
        reference_price=23_000.0,
        gamma_pin=23_025.0,
        max_pain=23_000.0,
        symbol="NDX",
    )


def test_database_inspector_keeps_newest_row_and_verifies_recent_bound_lineage(
    tmp_path,
):
    database_path = _write_calculation_bound_fixture(tmp_path)
    _append_verified_ndx_peer(database_path)

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    latest = report["market_structure_rows"]["SPX"]
    bound = latest["latest_calculation_bound"]

    assert latest["latest_calculation_id"] is None
    assert latest["latest_gamma_pin"] == 7_530.0
    assert bound["calculation_id"] == "11111111-1111-1111-1111-111111111111"
    assert bound["source_timestamp_utc"] == "2026-09-08T13:35:50Z"
    assert bound["captured_at_utc"] == "2026-09-08T13:35:50.500000Z"
    assert bound["lag_from_latest_source_seconds"] == 9.0
    assert bound["gamma_pin"] == 7_525.0
    assert bound["lineage"]["status"] == "verified"
    assert bound["lineage"]["payload_integrity_verified"] is True
    first = latest["first_eligible_calculation_bound"]
    assert first["candidate_count"] == 1
    assert first["selection_status"] == "verified"
    assert first["calculation_id"] == "11111111-1111-1111-1111-111111111111"
    assert first["lineage"]["run_calculated_at_utc"] == "2026-09-08T13:35:50Z"


def test_database_inspector_keeps_earliest_verified_run_when_a_later_run_arrives(
    tmp_path,
):
    database_path = _write_calculation_bound_fixture(tmp_path)
    _append_verified_ndx_peer(database_path)
    _append_verified_calculation_bound(
        database_path,
        run_id=3,
        calculation_id="22222222-2222-2222-2222-222222222222",
        observation_id="later-bound",
        calculated_at_utc="2026-09-08T13:35:55+00:00",
        source_timestamp_utc="2026-09-08T13:35:55+00:00",
        captured_at_utc="2026-09-08T13:35:55.500000+00:00",
        reference_price=7_512.0,
        gamma_pin=7_535.0,
        max_pain=7_510.0,
    )

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    evidence = report["market_structure_rows"]["SPX"]

    assert evidence["latest_calculation_bound"]["calculation_id"] == (
        "22222222-2222-2222-2222-222222222222"
    )
    assert evidence["first_eligible_calculation_bound"]["calculation_id"] == (
        "11111111-1111-1111-1111-111111111111"
    )
    assert evidence["first_eligible_calculation_bound"]["candidate_count"] == 2
    assert evidence["first_eligible_calculation_bound"]["lineage"][
        "payload_integrity_verified"
    ] is True
    assert "run_age_seconds" not in evidence["first_eligible_calculation_bound"][
        "lineage"
    ]
    assert evidence["first_eligible_calculation_bound"]["lineage"][
        "run_to_capture_seconds"
    ] == 0.5


def test_database_inspector_preserves_earliest_coherent_pair_across_restart_epoch(
    tmp_path,
):
    database_path = _write_calculation_bound_fixture(tmp_path)
    prior_epoch = TEST_SUBSCRIPTION_EPOCH
    restarted_epoch = "f" * 64
    _append_verified_calculation_bound(
        database_path,
        run_id=2,
        calculation_id="22222222-2222-2222-2222-222222222222",
        observation_id="prior-ndx",
        calculated_at_utc="2026-09-08T13:35:51+00:00",
        source_timestamp_utc="2026-09-08T13:35:51+00:00",
        captured_at_utc="2026-09-08T13:35:51.500000+00:00",
        reference_price=23_000.0,
        gamma_pin=23_025.0,
        max_pain=23_000.0,
        symbol="NDX",
        subscription_epoch_id=prior_epoch,
    )
    _append_verified_calculation_bound(
        database_path,
        run_id=3,
        calculation_id="33333333-3333-3333-3333-333333333333",
        observation_id="restarted-spx",
        calculated_at_utc="2026-09-08T13:36:05+00:00",
        source_timestamp_utc="2026-09-08T13:36:05+00:00",
        captured_at_utc="2026-09-08T13:36:05.500000+00:00",
        reference_price=7_512.0,
        gamma_pin=7_535.0,
        max_pain=7_510.0,
        subscription_epoch_id=restarted_epoch,
        subscription_generation=1,
        selected_universe_sha256="c" * 64,
    )
    _append_verified_calculation_bound(
        database_path,
        run_id=4,
        calculation_id="44444444-4444-4444-4444-444444444444",
        observation_id="restarted-ndx",
        calculated_at_utc="2026-09-08T13:36:06+00:00",
        source_timestamp_utc="2026-09-08T13:36:06+00:00",
        captured_at_utc="2026-09-08T13:36:06.500000+00:00",
        reference_price=23_012.0,
        gamma_pin=23_035.0,
        max_pain=23_010.0,
        symbol="NDX",
        subscription_epoch_id=restarted_epoch,
        subscription_generation=1,
        selected_universe_sha256="c" * 64,
    )

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 37, tzinfo=CT),
    )
    rows = report["market_structure_rows"]

    assert rows["SPX"]["latest_subscription_epoch_id"] == restarted_epoch
    assert rows["NDX"]["latest_subscription_epoch_id"] == restarted_epoch
    assert rows["SPX"]["first_eligible_calculation_bound"][
        "calculation_id"
    ] == "11111111-1111-1111-1111-111111111111"
    assert rows["NDX"]["first_eligible_calculation_bound"][
        "calculation_id"
    ] == "22222222-2222-2222-2222-222222222222"
    assert {
        rows[symbol]["first_eligible_calculation_bound"][
            "subscription_epoch_id"
        ]
        for symbol in ("SPX", "NDX")
    } == {prior_epoch}
    assert {
        rows[symbol]["first_eligible_calculation_bound"]["candidate_count"]
        for symbol in ("SPX", "NDX")
    } == {1}


def test_inspector_first_pair_is_accepted_by_monitor_validator(
    tmp_path,
    monkeypatch,
):
    database_path = _write_calculation_bound_fixture(tmp_path)
    _append_verified_ndx_peer(database_path)
    observed_utc = datetime(2026, 9, 8, 13, 36, tzinfo=timezone.utc)
    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=observed_utc.astimezone(CT),
    )
    monkeypatch.setattr(opening_acceptance, "_MAX_STRUCTURE_AGE_SECONDS", 360.0)

    identities = []
    pair_completions = []
    for symbol in ("SPX", "NDX"):
        identity, pair_completed = (
            opening_acceptance._validate_first_eligible_gamma_evidence(
                report["market_structure_rows"][symbol],
                symbol=symbol,
                session_date="2026-09-08",
                observed_utc=observed_utc,
            )
        )
        identities.append(identity)
        pair_completions.append(pair_completed)

    assert identities[0] == identities[1]
    assert pair_completions[0] == pair_completions[1]


def test_database_inspector_rejects_cross_identity_or_corrupt_bound_lineage(tmp_path):
    database_path = _write_calculation_bound_fixture(tmp_path)
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "UPDATE market_structure_observations SET subscription_generation=6 "
            "WHERE observation_id='bound'"
        )
        connection.commit()
    finally:
        connection.close()

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    assert report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["row_count"] == 0
    assert report["market_structure_rows"]["SPX"][
        "first_eligible_calculation_bound"
    ]["candidate_count"] == 0

    database_path = _write_calculation_bound_fixture(tmp_path / "corrupt")
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "UPDATE gamma_calculation_input_blobs SET payload_sha256=?",
            ("f" * 64,),
        )
        connection.commit()
    finally:
        connection.close()
    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    lineage = report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["lineage"]
    assert lineage["status"] == "invalid"
    assert lineage["reason"] == "GAMMA_CALCULATION_INPUT_INTEGRITY_INVALID"


def test_database_inspector_rejects_calculation_blob_with_trailing_data(tmp_path):
    database_path = _write_calculation_bound_fixture(tmp_path)
    connection = sqlite3.connect(database_path)
    try:
        compressed = bytes(
            connection.execute(
                "SELECT payload FROM gamma_calculation_input_blobs"
            ).fetchone()[0]
        )
        corrupted = compressed + b"trailing-corruption"
        connection.execute(
            "UPDATE gamma_calculation_input_blobs SET payload=?, compressed_bytes=?",
            (corrupted, len(corrupted)),
        )
        connection.commit()
    finally:
        connection.close()

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    lineage = report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["lineage"]
    assert lineage["status"] == "invalid"
    assert lineage["reason"] == "GAMMA_CALCULATION_INPUT_INTEGRITY_INVALID"
    assert lineage["payload_integrity_verified"] is False


def test_database_inspector_rejects_summary_only_calculation_blob(tmp_path):
    database_path = _write_calculation_bound_fixture(tmp_path)

    def remove_full_inputs(payload):
        for field in (
            "calculated_at_utc",
            "raw_fresh_chain_rows",
            "calculated_gex_rows",
            "parameters",
            "rejection_counts",
        ):
            payload.pop(field)

    _rewrite_calculation_input_payload(database_path, remove_full_inputs)

    report = inspect_database(
        database_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    lineage = report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["lineage"]
    assert lineage["status"] == "invalid"
    assert lineage["reason"] == "GAMMA_CALCULATION_INPUT_SCHEMA_INVALID"
    assert lineage["payload_integrity_verified"] is False


def test_database_inspector_binds_calculation_time_and_rejects_future_run(tmp_path):
    mismatched_path = _write_calculation_bound_fixture(tmp_path / "mismatched")
    _rewrite_calculation_input_payload(
        mismatched_path,
        lambda payload: payload.update(
            calculated_at_utc="2026-09-08T13:35:49+00:00"
        ),
    )
    report = inspect_database(
        mismatched_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    lineage = report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["lineage"]
    assert lineage["status"] == "invalid"
    assert lineage["reason"] == "GAMMA_CALCULATION_INPUT_MISMATCH"

    future_path = _write_calculation_bound_fixture(tmp_path / "future")
    future_calculation = "2026-09-08T13:35:59+00:00"
    connection = sqlite3.connect(future_path)
    try:
        connection.execute(
            "UPDATE gamma_calculation_runs SET calculated_at_utc=?",
            (future_calculation,),
        )
        connection.commit()
    finally:
        connection.close()
    _rewrite_calculation_input_payload(
        future_path,
        lambda payload: payload.update(calculated_at_utc=future_calculation),
    )
    report = inspect_database(
        future_path,
        date(2026, 9, 8),
        observed_at_ct=datetime(2026, 9, 8, 8, 36, tzinfo=CT),
    )
    lineage = report["market_structure_rows"]["SPX"][
        "latest_calculation_bound"
    ]["lineage"]
    assert lineage["status"] == "invalid"
    assert lineage["reason"] == "GAMMA_CALCULATION_INPUT_MISMATCH"


def test_opening_evaluator_fails_closed_on_orb_store_or_cadence_regression():
    now = datetime(2026, 9, 4, 8, 36, tzinfo=CT)
    database = _database()
    database["missing_orb_reference_triggers"] = [
        "orb_reference_samples_existing_guard"
    ]
    database["orb_reference_rows"]["SPX"].update(
        opening_capture_ratio=0.94,
        maximum_opening_gap_seconds=180.0,
        advancing_5s_evidence=False,
    )
    database["orb_reference_progress_decisions"]["SPX"].update(
        raw_row_count=73,
        pending_decision_count=1,
    )
    health = _health()
    health["payload"]["orb_reference_sampler"]["thread_alive"] = False
    symbols = {
        symbol: {
            "opening_ranges": {
                "5m": {
                    "capture_status": "complete",
                    "directional_evidence_eligible": symbol != "VIX",
                    "capture_evidence": {"capture_ratio": 1.0},
                }
            },
            "reference_semantics": {
                "directional_base_eligible": symbol != "VIX"
            },
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }

    state, issues, _warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=health,
        live={
            "ok": True,
            "payload": {
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "active_generation": 7,
                "handoff_status": "active",
                "stream_connected": True,
                "stream_progressing": True,
                "collection_ready": True,
                "calculation_ready": True,
                "prediction_pipeline_ok": True,
            },
        },
        dashboard={"ok": True},
        orb={
            "ok": True,
        "payload": _runtime_bound_orb({
            "configured_symbols": list(symbols),
            "symbols": symbols,
        }),
        },
        database=database,
        scheduled_tasks=_tasks(),
    )

    assert state == "degraded"
    assert "ORB_REFERENCE_IMMUTABILITY_TRIGGERS_MISSING" in issues
    assert "ORB_REFERENCE_SAMPLER_THREAD_NOT_ALIVE" in issues
    assert "ORB_REFERENCE_CAPTURE_RATIO_LOW:SPX" in issues
    assert "ORB_REFERENCE_5S_EVIDENCE_STALE:SPX" in issues
    assert "ORB_REFERENCE_DECISION_PENDING:SPX" in issues


def test_opening_evaluator_rejects_stale_same_day_gamma_structure_rows():
    now = datetime(2026, 9, 4, 8, 36, tzinfo=CT)
    database = _database()
    database["market_structure_rows"]["SPX"]["latest_source_age_seconds"] = 91.0
    symbols = {
        symbol: {
            "opening_ranges": {
                "5m": {
                    "capture_status": "complete",
                    "directional_evidence_eligible": symbol != "VIX",
                    "capture_evidence": {"capture_ratio": 1.0},
                }
            },
            "reference_semantics": {
                "directional_base_eligible": symbol != "VIX"
            },
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }

    state, issues, warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live={
            "ok": True,
            "payload": {
                "stream_connected": True,
                "stream_progressing": True,
                "collection_ready": True,
                "calculation_ready": True,
                "prediction_pipeline_ok": True,
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "active_generation": 7,
                "handoff_status": "active",
            },
        },
        dashboard={"ok": True},
        orb={"ok": True, "payload": _runtime_bound_orb({
            "configured_symbols": list(symbols),
            "symbols": symbols,
        })},
        database=database,
        scheduled_tasks=_tasks(),
    )

    assert state == "degraded"
    assert "VALID_GEX_STRUCTURE_STALE:SPX" in issues
    assert warnings == []


def test_post_close_stale_runtime_rows_do_not_hide_static_or_service_failures():
    now = datetime(2026, 9, 4, 15, 5, tzinfo=CT)
    database = _database()
    database["market_structure_rows"] = {}
    for evidence in database["orb_reference_rows"].values():
        evidence.update(
            opening_capture_ratio=0.25,
            maximum_opening_gap_seconds=600.0,
            advancing_5s_evidence=False,
        )
    health = _health()
    health["payload"].update(
        subscription_allowed=False,
        symbols_subscribed=0,
        root_contract_counts={},
        subscription_metadata={},
    )
    live = {
        "ok": True,
        "payload": {
            "stream_connected": False,
            "stream_progressing": False,
            "collection_ready": False,
            "calculation_ready": False,
            "prediction_pipeline_ok": False,
        },
    }
    dashboard = {"ok": True}
    orb = {
        "ok": True,
        "payload": {
            "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
            "symbols": {},
        },
    }

    def inspect(**overrides):
        inputs = {
            "now_ct": now,
            "clock": {"applicable": True, "synchronized": True},
            "health": health,
            "live": live,
            "dashboard": dashboard,
            "orb": orb,
            "database": database,
            "scheduled_tasks": _tasks(),
        }
        inputs.update(overrides)
        return evaluate(**inputs)

    state, issues, warnings = inspect()
    assert state == "ready"
    assert issues == []
    assert warnings == []

    broken_database = deepcopy(database)
    broken_database["missing_orb_reference_columns"] = ["captured_at_utc"]
    assert "ORB_REFERENCE_SCHEMA_INCOMPATIBLE" in inspect(
        database=broken_database
    )[1]

    unreachable_health = deepcopy(health)
    unreachable_health["ok"] = False
    assert "BACKEND_HEALTH_UNREACHABLE" in inspect(health=unreachable_health)[1]
    assert "BACKEND_LIVE_HEALTH_UNREACHABLE" in inspect(live={"ok": False})[1]
    assert "DASHBOARD_HEALTH_UNREACHABLE" in inspect(
        dashboard={"ok": False}
    )[1]

    missing_symbol_orb = deepcopy(orb)
    missing_symbol_orb["payload"]["configured_symbols"].remove("RUT")
    assert "ORB_SYMBOL_NOT_CONFIGURED:RUT" in inspect(orb=missing_symbol_orb)[1]


def test_rut_forward_reference_blocks_four_index_directional_acceptance():
    now = datetime(2026, 9, 4, 8, 36, tzinfo=CT)
    symbols = {
        symbol: {
            "opening_ranges": {
                "5m": {
                    "capture_status": "complete",
                    "directional_evidence_eligible": symbol in {"SPX", "NDX"},
                    "capture_evidence": {"capture_ratio": 1.0},
                }
            },
            "reference_semantics": {
                "directional_base_eligible": symbol in {"SPX", "NDX"},
                "primary_expiration": (
                    "2026-09-05" if symbol in {"VIX", "RUT"} else "2026-09-04"
                ),
                "same_day_profile_available": symbol in {"SPX", "NDX"},
            },
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }
    database = _database()
    database["orb_reference_rows"]["RUT"].update(
        latest_primary_expiration="2026-09-05",
        latest_same_day_profile_available=False,
    )

    state, issues, warnings = evaluate(
        now_ct=now,
        clock={"applicable": True, "synchronized": True},
        health=_health(),
        live={
            "ok": True,
            "payload": {
                "stream_connected": True,
                "stream_progressing": True,
                "collection_ready": True,
                "calculation_ready": True,
                "prediction_pipeline_ok": True,
                "subscription_epoch_id": TEST_SUBSCRIPTION_EPOCH,
                "subscription_generation": 7,
                "active_generation": 7,
                "handoff_status": "active",
            },
        },
        dashboard={"ok": True},
        orb={
            "ok": True,
            "payload": _runtime_bound_orb(
                {
                    "configured_symbols": list(symbols),
                    "symbols": symbols,
                }
            ),
        },
        database=database,
        scheduled_tasks=_tasks(),
    )

    assert state == "degraded"
    assert issues == ["RUT_ORB_CONTEXT_ONLY"]
    assert warnings == []


def test_rut_orb_classification_must_match_persisted_reference_evidence():
    fixture = _regular_acceptance_fixture()
    fixture["database"]["orb_reference_rows"]["RUT"].update(
        latest_primary_expiration="2026-09-05",
        latest_same_day_profile_available=False,
    )

    state, issues, warnings = _evaluate_regular_fixture(fixture)

    assert state == "degraded"
    assert "RUT_ORB_CONTEXT_ONLY" in issues
    assert "RUT_ORB_CLASSIFICATION_MISMATCH" in issues
    assert warnings == []


def test_labor_day_does_not_require_live_or_orb_advancement():
    state, issues, warnings = evaluate(
        now_ct=datetime(2026, 9, 7, 8, 35, tzinfo=CT),
        clock={"applicable": True, "synchronized": True},
        health={"ok": False, "not_due": True},
        live={"ok": False, "not_due": True},
        dashboard={"ok": False, "not_due": True},
        orb={"ok": False, "not_due": True},
        database=_database(),
        scheduled_tasks={"applicable": False, "tasks": {}},
    )

    assert state == "preflight"
    assert issues == []
    assert warnings == []
