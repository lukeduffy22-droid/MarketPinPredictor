from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import backend.monitor_opening_acceptance as opening
import tools.inspect_opening_capture as opening_capture_inspector
from backend.market_structure import MarketStructureJournal
from backend.opening_code_fingerprint import (
    LOADED_CODE_CAPTURE_SEMANTICS,
    LOADED_CODE_FINGERPRINT_SCHEMA,
    LOADED_CODE_HASH_ALGORITHM,
    OPENING_CRITICAL_SOURCE_PATHS,
)
from backend.monitor_scan_ledger import MonitorScanLedgerError
from backend.monitor_scan_ledger import commit_monitor_scan, with_scan_event_id
from backend.monitor_cadence import (
    CADENCE_EVIDENCE_SCHEMA,
    CADENCE_TRIGGER_CALCULATION_VERSION,
)
from backend.monitor_session_rollover import prepare_monitor_session


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "commit_market_monitor_opening_acceptance.py"
ACK_TOOL = ROOT / "tools" / "ack_market_monitor_opening_acceptance.py"
SESSION = "2026-09-08"
EPOCH = "e" * 64


def _loaded_code_fingerprint(captured_at_utc: str) -> dict:
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


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _journal(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def _armed_session(tmp_path: Path) -> tuple[Path, Path]:
    state_path = tmp_path / "market_monitor" / "state.json"
    journal_dir = state_path.parent
    _write_json(
        state_path,
        {
            "schema_version": 2,
            "session_date": "2026-09-04",
            "opening_acceptance": {},
            "policy_evaluator": {},
            "mode": "NORMAL",
        },
    )
    rolled = prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 8, 12, 45, tzinfo=timezone.utc),
        session_date=SESSION,
    )
    assert rolled["accepted"] is True
    return state_path, journal_dir


def _report(
    observed_at_utc: str = "2026-09-08T14:31:00Z",
    *,
    session_date: str = SESSION,
) -> dict:
    observed = datetime.fromisoformat(observed_at_utc.replace("Z", "+00:00"))
    observed_ct = observed.astimezone(opening._CT)
    startup_capture = datetime.fromisoformat(
        f"{session_date}T07:45:00"
    ).replace(tzinfo=opening._CT).astimezone(timezone.utc)
    due = opening._expected_due_windows(observed_ct)
    cash_open = datetime.fromisoformat(
        f"{session_date}T08:30:00"
    ).replace(tzinfo=opening._CT)
    first_gamma_run_utc = (cash_open + timedelta(seconds=30)).astimezone(
        timezone.utc
    )
    first_gamma_capture_utc = first_gamma_run_utc + timedelta(milliseconds=500)
    bound_source_utc = (observed - timedelta(seconds=1)).isoformat()
    bound_capture_utc = (observed - timedelta(milliseconds=500)).isoformat()
    windows = {}
    for name in due:
        duration_minutes = int(name[:-1])
        expected_sample_count = duration_minutes * 60 // 5
        windows[name] = {
            "duration_minutes": duration_minutes,
            "range_start_utc": cash_open.astimezone(timezone.utc).isoformat(),
            "range_end_utc": (
                cash_open + timedelta(minutes=duration_minutes)
            ).astimezone(timezone.utc).isoformat(),
            "capture_status": "complete",
            "orb_complete": True,
            "clock_status": "closed",
            "opening_price": 7_500.0,
            "orb_high": 7_550.0,
            "orb_low": 7_450.0,
            "current_price": 7_510.0,
            "current_reference_fresh": True,
            "directional_evidence_eligible": True,
            "combined_structure_directional_evidence_eligible": True,
            "capture_evidence": {
                "sample_count": expected_sample_count,
                "expected_sample_count": expected_sample_count,
                "capture_ratio": 1.0,
                "opening_bucket_present": True,
                "first_sample_lag_seconds": 0.0,
                "end_gap_seconds": 5.0,
                "max_gap_seconds": 5.0,
            },
        }
    symbols = {}
    for symbol in ("SPX", "NDX", "VIX", "RUT"):
        symbol_windows = copy.deepcopy(windows)
        if symbol == "VIX":
            for window in symbol_windows.values():
                window["directional_evidence_eligible"] = False
                window["combined_structure_directional_evidence_eligible"] = False
        symbols[symbol] = {
            "reference_semantics": {
                "kind": (
                    "vix_option_forward_context"
                    if symbol == "VIX"
                    else "same_day_index_option_parity"
                ),
                "source": "databento_opra_put_call_parity",
                "authority": "research_reference_only",
                "primary_expiration": (
                    "2026-09-09" if symbol == "VIX" else session_date
                ),
                "same_day_profile_available": symbol != "VIX",
                "directional_base_eligible": symbol != "VIX",
                "limitation": (
                    "VIX option parity is forward-like expiry context, not the "
                    "official VIX spot index."
                    if symbol == "VIX"
                    else "Same-day index-option parity is a sampled reference, "
                    "not official exchange OHLC."
                ),
            },
            "pin_behavior": {
                "level_availability_status": "available",
                "gamma_pin": 7_525.0,
                "max_pain": 7_500.0,
            },
            "provenance": {
                "runtime_binding_applied": True,
                "active_runtime_epoch_aligned": True,
                "active_subscription_epoch_id": EPOCH,
                "active_subscription_generation": 7,
                "subscription_generations": [7],
                "active_handoff_status": "active",
                "range_provenance_aligned": True,
                "current_vs_range_aligned": True,
                "structure_vs_reference_aligned": True,
                "structure_reference_status": "aligned",
                "structure_reference_fresh": True,
                "structure_reference_age_seconds": 1.0,
                "structure_reference_max_age_seconds": 90.0,
            },
            "last_known_structure": {
                "status": "aligned",
                "level_availability_status": "available",
                "age_seconds": 1.0,
                "maximum_current_age_seconds": 90.0,
                "gamma_pin": 7_525.0,
                "max_pain": 7_500.0,
                "calculation_id": f"calc-{symbol}",
            },
            "last_calculation_bound_structure": {
                "status": "aligned",
                "level_availability_status": "available",
                "source_timestamp_utc": bound_source_utc,
                "captured_at_utc": bound_capture_utc,
                "freshness_timestamp_utc": bound_source_utc,
                "age_seconds": 1.0,
                "maximum_current_age_seconds": 90.0,
                "gamma_pin": 7_525.0,
                "max_pain": 7_500.0,
                "calculation_id": f"calc-{symbol}",
                "provider": "databento",
                "subscription_epoch_id": EPOCH,
                "subscription_generation": 7,
                "universe_sha256": "a" * 64,
                "primary_expiration": session_date,
                "same_day_profile_available": True,
                "current_provenance_aligned": True,
                "runtime_aligned": True,
                "evidence_eligible": True,
            },
            "opening_ranges": symbol_windows,
        }
    return {
        "schema_version": "marketpin-opening-readiness.v1",
        "output_mode": "compact",
        "state": "ready",
        "observed_at_ct": observed_ct.isoformat(),
        "observed_at_utc": observed.isoformat().replace("+00:00", "Z"),
        "issues": [],
        "acceptance_scope": "startup",
        "out_of_scope_issues": [],
        "warnings": [],
        "due_orb_windows": due,
        "due_orb_window": due[-1] if due else None,
        "clock": {
            "applicable": True,
            "status": "synchronized",
            "synchronized": True,
            "windows_time_synchronized": True,
            "external_offset_verified": True,
            "leap_indicator": 0,
            "median_offset_seconds": 0.01,
            "maximum_allowed_absolute_offset_seconds": 0.25,
        },
        "backend_health": {
            "ok": True,
            "payload": {
                "provider": "databento",
                "websocket": "active",
                "subscription_session_state": (
                    "preopen" if observed_ct.time() < time(8, 30) else "regular_session"
                ),
                "subscription_allowed": True,
                "subscription_suppressed": False,
                "provider_queue_full_warnings": 0,
                "provider_slow_client_warnings": 0,
                "provider_skipped_record_warnings": 0,
                "provider_skipped_records": 0,
                "reconnect_attempts": 0,
                "connection_limit_rejections_total": 0,
                "connection_limit_consecutive": 0,
                "connection_limit_circuit_state": "closed",
                "connection_limit_retry_not_before_utc": None,
                "connection_limit_cooldown_remaining_seconds": 0.0,
                "last_client_close_status": "not_attempted",
                "last_client_close_elapsed_seconds": None,
                "pre_auth_transport_guard_status": "installed",
                "pre_auth_transport_aborts_total": 0,
                "pre_auth_transport_abort_failures_total": 0,
                "last_pre_auth_transport_event": None,
                "last_pre_auth_transport_reason": None,
                "last_pre_auth_transport_event_utc": None,
                "runtime_controls": {
                    "sleep_prevention": {
                        "requested": True,
                        "active": True,
                    }
                },
                "subscription_epoch_id": EPOCH,
                "active_generation": 7,
                "handoff_status": "active",
                "symbols_requested": ["SPX", "NDX", "VIX", "RUT"],
                "symbols_subscribed": 3_200,
                "core_symbol_status": {
                    symbol: {
                        "requested": True,
                        "contracts_subscribed": count,
                    }
                    for symbol, count in {
                        "SPX": 1_000,
                        "NDX": 1_000,
                        "VIX": 200,
                        "RUT": 1_000,
                    }.items()
                },
                "processing_clock_telemetry": {"status": "synchronized"},
                "universe_fallback_active": False,
                "universe_provenance": {
                    "mode": "current_day_cache",
                    "trading_date": session_date,
                    "source_date": session_date,
                    "source_sha256": "b" * 64,
                    "source_rows": 8_000,
                    "is_fallback": False,
                },
                "subscription_metadata": {
                    "full_contract_count": 8_000,
                    "selected_contract_count": 3_200,
                    "selected_universe_sha256": "a" * 64,
                    "reservation_shortfall_pairs": 0,
                    "universe_provenance": {
                        "mode": "current_day_cache",
                        "trading_date": session_date,
                        "source_date": session_date,
                        "source_sha256": "b" * 64,
                        "source_rows": 8_000,
                        "is_fallback": False,
                    },
                    "markets": {
                        symbol: {
                            "selected_contract_count": count,
                            "market_reservation_shortfall_pairs": 0,
                            "primary_reserved_pairs_retained": 100,
                            "next_listed_reserved_pairs_retained": 50,
                        }
                        for symbol, count in {
                            "SPX": 1_000,
                            "NDX": 1_000,
                            "VIX": 200,
                            "RUT": 1_000,
                        }.items()
                    },
                },
                "subscription_bounds": {
                    "max_subscription_contracts": 3_200,
                },
                "optional_family_canary": {
                    "state": "armed",
                    "rollback_required": False,
                },
                "orb_reference_sampler": {
                    "thread_alive": True,
                    "interval_seconds": 5,
                },
                "loaded_code_fingerprint": _loaded_code_fingerprint(
                    startup_capture.isoformat()
                ),
                "subscription_window": {
                    "state": (
                        "preopen"
                        if observed_ct.time() < time(8, 30)
                        else "regular_session"
                    ),
                    "subscription_allowed": True,
                    "trading_date": session_date,
                    "observed_at_utc": observed.isoformat().replace("+00:00", "Z"),
                },
            },
        },
        "backend_live_health": {
            "ok": True,
            "payload": {
                "subscription_epoch_id": EPOCH,
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
        "dashboard_health": {"ok": True},
        "orb": {
            "ok": True,
            "payload": {
                "configured_symbols": ["SPX", "NDX", "VIX", "RUT"],
                "requested_symbols": ["SPX", "NDX", "VIX", "RUT"],
                "runtime_binding_applied": True,
                "runtime_context_stable": True,
                "active_runtime_context": {
                    "subscription_epoch_id": EPOCH,
                    "subscription_generation": 7,
                    "handoff_status": "active",
                },
                "symbols": symbols,
            },
        },
        "database": {
            "present": True,
            "path": "C:/test/market_data.db",
            "journal_mode": "wal",
            "quick_check": "ok",
            "market_structure_table_present": True,
            "missing_market_structure_columns": [],
            "missing_market_structure_triggers": [],
            "market_structure_column_count": 20,
            "expected_market_structure_column_count": 20,
            "orb_reference_table_present": True,
            "missing_orb_reference_columns": [],
            "missing_orb_reference_triggers": [],
            "missing_orb_reference_indexes": [],
            "orb_reference_column_count": 18,
            "expected_orb_reference_column_count": 18,
            "orb_reference_decision_table_present": True,
            "missing_orb_reference_decision_columns": [],
            "missing_orb_reference_decision_triggers": [],
            "missing_orb_reference_decision_indexes": [],
            "orb_reference_decision_column_count": 7,
            "expected_orb_reference_decision_column_count": 7,
            "market_structure_rows": {
                symbol: {
                    "row_count": 1,
                    "latest_source_age_seconds": 1.0,
                    "latest_capture_age_seconds": 1.0,
                    "latest_provider": "databento",
                    "latest_validation_status": "valid",
                    "latest_primary_expiration": session_date,
                    "latest_same_day_profile_available": True,
                    "latest_subscription_epoch_id": EPOCH,
                    "latest_subscription_generation": 7,
                    "latest_gamma_pin": 7_525.0,
                    "latest_max_pain": 7_500.0,
                    "latest_reference_price": 7_510.0,
                    "latest_calculation_id": f"calc-{symbol}",
                    "latest_universe_sha256": "a" * 64,
                    "latest_calculation_bound": {
                        "row_count": 1,
                        "source_timestamp_utc": bound_source_utc,
                        "source_age_seconds": 1.0,
                        "captured_at_utc": bound_capture_utc,
                        "capture_age_seconds": 0.5,
                        "lag_from_latest_source_seconds": 0.0,
                        "provider": "databento",
                        "subscription_epoch_id": EPOCH,
                        "subscription_generation": 7,
                        "calculation_id": f"calc-{symbol}",
                        "reference_price": 7_510.0,
                        "gamma_pin": 7_525.0,
                        "max_pain": 7_500.0,
                        "primary_expiration": session_date,
                        "same_day_profile_available": True,
                        "universe_sha256": "a" * 64,
                        "validation_status": "valid",
                        "lineage": {
                            "status": "verified",
                            "reason": None,
                            "calculation_id": f"calc-{symbol}",
                            "gamma_run_present": True,
                            "input_blob_present": True,
                            "payload_integrity_verified": True,
                            "payload_sha256": "c" * 64,
                            "run_calculated_at_utc": bound_source_utc,
                            "run_age_seconds": 1.0,
                        },
                    },
                    "first_eligible_calculation_bound": {
                        "candidate_count": 1,
                        "selection_status": "verified",
                        "selection_reason": None,
                        "observation_id": f"first-observation-{symbol}",
                        "trading_date": session_date,
                        "source_timestamp_utc": first_gamma_run_utc.isoformat(),
                        "captured_at_utc": first_gamma_capture_utc.isoformat(),
                        "pair_completed_at_utc": (
                            first_gamma_capture_utc.isoformat()
                        ),
                        "provider": "databento",
                        "subscription_epoch_id": EPOCH,
                        "subscription_generation": 7,
                        "calculation_id": f"first-calc-{symbol}",
                        "reference_price": 7_510.0,
                        "gamma_pin": 7_525.0,
                        "max_pain": 7_500.0,
                        "primary_expiration": session_date,
                        "same_day_profile_available": True,
                        "universe_sha256": "a" * 64,
                        "validation_status": "valid",
                        "lineage": {
                            "status": "verified",
                            "reason": None,
                            "calculation_id": f"first-calc-{symbol}",
                            "gamma_run_present": True,
                            "input_blob_present": True,
                            "payload_integrity_verified": True,
                            "payload_sha256": "d" * 64,
                            "run_calculated_at_utc": first_gamma_run_utc.isoformat(),
                            "source_to_run_seconds": 0.0,
                            "run_to_capture_seconds": 0.5,
                        },
                    },
                }
                for symbol in ("SPX", "NDX")
            },
            "orb_reference_rows": {
                symbol: {
                    "row_count": 721,
                    "opening_bucket_present": True,
                    "opening_capture_ratio": 1.0,
                    "advancing_5s_evidence": True,
                    "provider_source_timestamps_advancing": True,
                    "latest_subscription_epoch_id": EPOCH,
                    "subscription_epoch_ids": [EPOCH],
                    "invalid_subscription_epoch_row_count": 0,
                    "mixed_subscription_epoch_rows": False,
                    "latest_subscription_generation": 7,
                    "subscription_generations": [7],
                    "invalid_subscription_generation_row_count": 0,
                    "mixed_subscription_generation_rows": False,
                    "latest_provider": "databento",
                    "latest_validation_status": "valid",
                    "latest_primary_expiration": (
                        "2026-09-09" if symbol == "VIX" else session_date
                    ),
                    "latest_same_day_profile_available": symbol != "VIX",
                }
                for symbol in ("SPX", "NDX", "VIX", "RUT")
            },
            "orb_reference_progress_decisions": {
                symbol: {
                    "raw_row_count": 721,
                    "pending_decision_count": 0,
                    "ineligible_decision_count": 0,
                    "eligible_decision_count": 721,
                }
                for symbol in ("SPX", "NDX", "VIX", "RUT")
            },
        },
        "scheduled_tasks": {
            "applicable": True,
            "tasks": {
                name: {
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
                    "run_level": "Highest",
                    "start_when_available": name.endswith("AutoStart"),
                    "clock_sync_skipped": name.endswith("Watchdog"),
                    "last_run_time": (
                        f"{session_date}T07:45:00-05:00"
                        if name.endswith("AutoStart")
                        else f"{session_date}T07:50:00-05:00"
                    ),
                }
                for name in (
                    "MarketPinPredictor_AutoStart",
                    "MarketPinPredictor_Watchdog",
                )
            },
        },
        "read_only": True,
        "authorities": {
            "backend_health": "/health",
            "backend_live_health": "/health/live",
            "orb": "/v1/orb",
            "database": "C:/test/market_data.db",
        },
        "notes": [],
    }


def _set_first_gamma_run(
    report: dict,
    symbol: str,
    *,
    seconds_after_cash_open: float,
    capture_seconds_after_cash_open: float | None = None,
) -> None:
    session_day = datetime.fromisoformat(report["observed_at_ct"]).date()
    cash_open_utc = datetime.combine(
        session_day,
        time(8, 30),
        tzinfo=opening._CT,
    ).astimezone(timezone.utc)
    run_utc = cash_open_utc + timedelta(seconds=seconds_after_cash_open)
    capture_utc = (
        cash_open_utc + timedelta(seconds=capture_seconds_after_cash_open)
        if capture_seconds_after_cash_open is not None
        else run_utc + timedelta(milliseconds=100)
    )
    evidence = report["database"]["market_structure_rows"][symbol][
        "first_eligible_calculation_bound"
    ]
    evidence["source_timestamp_utc"] = run_utc.isoformat()
    evidence["captured_at_utc"] = capture_utc.isoformat()
    evidence["lineage"]["run_calculated_at_utc"] = run_utc.isoformat()
    evidence["lineage"]["source_to_run_seconds"] = 0.0
    evidence["lineage"]["run_to_capture_seconds"] = (
        capture_utc - run_utc
    ).total_seconds()
    pair_completed_at_utc = max(
        datetime.fromisoformat(
            report["database"]["market_structure_rows"][candidate][
                "first_eligible_calculation_bound"
            ]["captured_at_utc"].replace("Z", "+00:00")
        )
        for candidate in ("SPX", "NDX")
    ).isoformat()
    for candidate in ("SPX", "NDX"):
        report["database"]["market_structure_rows"][candidate][
            "first_eligible_calculation_bound"
        ]["pair_completed_at_utc"] = pair_completed_at_utc


def _with_primary_active_staging(report: dict) -> dict:
    health = report["backend_health"]["payload"]
    selected_counts = {
        symbol: int(market["selected_contract_count"])
        for symbol, market in health["subscription_metadata"]["markets"].items()
    }
    primary_counts = {"SPX": 450, "NDX": 510, "VIX": 140, "RUT": 182}
    active_total = sum(primary_counts.values())
    selected_total = sum(selected_counts.values())
    health["symbols_subscribed"] = active_total
    health["symbols_selected"] = selected_total
    for symbol, count in primary_counts.items():
        health["core_symbol_status"][symbol]["contracts_subscribed"] = count
    health["subscription_staging"] = {
        "mode": "all-primary-then-same-session-shadow",
        "state": "primary_active",
        "active_stage": "primary",
        "deferred_stage": "shadow",
        "full_selected_contract_count": selected_total,
        "active_contract_count": active_total,
        "deferred_contract_count": selected_total - active_total,
        "requested_orb_families": ["SPX", "NDX", "VIX", "RUT"],
        "primary_contract_counts": primary_counts.copy(),
        "subscription_epoch_id": EPOCH,
        "subscription_generation": 7,
        "full_selected_universe_sha256": health["subscription_metadata"][
            "selected_universe_sha256"
        ],
        "same_client_additive_subscription": True,
        "intraday_replay_for_deferred_stage": False,
        "promotion_eligible": False,
        "promotion_reasons": ["CLEAN_TRANSPORT_WINDOW_INCOMPLETE"],
        "additive_request_sent": False,
    }
    health["market_subscription_status"] = {
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
    return report


def _set_primary_stage_pair_count(health: dict, symbol: str, pair_count: int) -> None:
    """Keep staged count identities coherent while changing one pair capacity."""

    staging = health["subscription_staging"]
    market_status = health["market_subscription_status"][symbol]
    prior_contract_count = staging["primary_contract_counts"][symbol]
    contract_count = pair_count * 2
    delta = contract_count - prior_contract_count
    staging["primary_contract_counts"][symbol] = contract_count
    staging["active_contract_count"] += delta
    staging["deferred_contract_count"] -= delta
    health["symbols_subscribed"] += delta
    health["core_symbol_status"][symbol]["contracts_subscribed"] = contract_count
    market_status["active_contract_count"] = contract_count
    market_status["deferred_contract_count"] = (
        market_status["selected_contract_count"] - contract_count
    )


def _commit(
    state_path: Path,
    journal_dir: Path,
    milestone: str,
    *,
    report: dict | None = None,
    failpoint=None,
) -> dict:
    bound_report = None
    if report is not None:
        bound_report = copy.deepcopy(report)
    else:
        bound_report = _report(
            {
                "startup": "2026-09-08T13:00:00Z",
                "first_eligible_gamma_capture": "2026-09-08T13:31:00Z",
                "complete_5m_orb": "2026-09-08T13:36:00Z",
                "complete_60m_orb": "2026-09-08T14:31:00Z",
            }.get(milestone, "2026-09-08T14:31:00Z")
        )
    if bound_report:
        bound_report["acceptance_scope"] = milestone
    return opening.commit_opening_acceptance_milestone(
        milestone=milestone,
        inspector_report=bound_report,
        state_path=state_path,
        journal_dir=journal_dir,
        failpoint=failpoint,
    )


def test_startup_accepts_strict_primary_active_subscription_stage(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    report = _with_primary_active_staging(_report("2026-09-08T13:00:00Z"))

    accepted = _commit(state_path, journal_dir, "startup", report=report)

    assert accepted["accepted"] is True, accepted
    assert accepted["action"] == "committed"


def test_primary_active_subscription_stage_rejects_terminal_and_broken_contracts(
    tmp_path: Path,
) -> None:
    cases = {
        "frozen": (
            lambda health: health["subscription_staging"].update(state="frozen"),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_STATE_INVALID:frozen",
        ),
        "canceled": (
            lambda health: health["subscription_staging"].update(state="canceled"),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_STATE_INVALID:canceled",
        ),
        "aggregate-count": (
            lambda health: health["subscription_staging"].update(
                deferred_contract_count=(
                    health["subscription_staging"]["deferred_contract_count"] - 1
                )
            ),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_COUNT_MISMATCH",
        ),
        "hash": (
            lambda health: health["subscription_staging"].update(
                full_selected_universe_sha256="f" * 64
            ),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_HASH_MISMATCH",
        ),
        "family-count": (
            lambda health: health["market_subscription_status"]["RUT"].update(
                active_contract_count=(
                    health["market_subscription_status"]["RUT"][
                        "active_contract_count"
                    ]
                    - 2
                )
            ),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_FAMILY_COUNT_MISMATCH:RUT",
        ),
        "primary-plan": (
            lambda health: health["subscription_staging"][
                "primary_contract_counts"
            ].update(RUT=180),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_PRIMARY_COUNT_MISMATCH:RUT",
        ),
        "missing-family": (
            lambda health: health["subscription_staging"][
                "requested_orb_families"
            ].remove("RUT"),
            "subscription_staging_invalid:SUBSCRIPTION_STAGING_REQUESTED_FAMILIES_MISSING:RUT",
        ),
        "vix-orb-pair-capacity": (
            lambda health: _set_primary_stage_pair_count(health, "VIX", 1),
            "subscription_staging_invalid:"
            "SUBSCRIPTION_STAGING_ORB_PRIMARY_PAIR_MINIMUM_NOT_MET:VIX:1<5",
        ),
        "rut-orb-pair-capacity": (
            lambda health: _set_primary_stage_pair_count(health, "RUT", 1),
            "subscription_staging_invalid:"
            "SUBSCRIPTION_STAGING_ORB_PRIMARY_PAIR_MINIMUM_NOT_MET:RUT:1<5",
        ),
    }

    for name, (mutate, expected) in cases.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _with_primary_active_staging(_report("2026-09-08T13:00:00Z"))
        mutate(report["backend_health"]["payload"])

        rejected = _commit(state_path, journal_dir, "startup", report=report)

        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [expected], (name, rejected)


def test_staging_family_set_ignores_non_orb_requested_symbols() -> None:
    report = _with_primary_active_staging(_report("2026-09-08T13:00:00Z"))
    health = report["backend_health"]["payload"]

    assert opening._subscription_staging_issues(
        health, {"SPX", "NDX", "VIX", "RUT", "SPY", "QQQ"}
    ) == []


def _rut_forward_context(report: dict, *, expiration: str = "2026-09-09") -> dict:
    rut = report["orb"]["payload"]["symbols"]["RUT"]
    rut["reference_semantics"].update(
        kind="non_same_day_index_option_forward_context",
        source="databento_opra_put_call_parity",
        authority="research_reference_only",
        primary_expiration=expiration,
        same_day_profile_available=False,
        directional_base_eligible=False,
        limitation=(
            "The primary option expiry is not same-day, so its parity level is "
            "context only."
        ),
    )
    for window in rut["opening_ranges"].values():
        window["directional_evidence_eligible"] = False
        window["combined_structure_directional_evidence_eligible"] = False
    report["database"]["orb_reference_rows"]["RUT"].update(
        latest_primary_expiration=expiration,
        latest_same_day_profile_available=False,
    )
    report["out_of_scope_issues"] = ["RUT_ORB_CONTEXT_ONLY"]
    return report


def _proof(delivered_at: str = "2026-09-08T14:32:00Z") -> dict:
    return {
        "proof_type": "prior_heartbeat_final_delivered",
        "conversation_history_sha256": "d" * 64,
        "prior_final_delivered_at_utc": delivered_at,
    }


def _ack(
    state_path: Path,
    journal_dir: Path,
    event_ids: list[str],
    *,
    observed_at: str = "2026-09-08T14:33:00Z",
    proof: dict | None = None,
    failpoint=None,
) -> dict:
    return opening.ack_opening_acceptance_notifications(
        session_date=SESSION,
        event_ids=event_ids,
        observed_at_utc=datetime.fromisoformat(observed_at.replace("Z", "+00:00")),
        delivery_proof=proof or _proof(),
        state_path=state_path,
        journal_dir=journal_dir,
        failpoint=failpoint,
    )


def _diagnostic_scan() -> dict:
    trigger_fields = (
        "half_threshold_gamma_pin_move",
        "contested_pin_leadership",
        "spot_near_or_crossed_gamma_pin",
        "spot_near_or_crossed_zero_gamma",
        "spot_near_or_crossed_gex_wall",
        "normalized_net_gex_near_or_crossed_zero",
        "forecast_bias_awaiting_confirmation",
        "high_volatility_regime",
    )
    adaptive = {
        "schema_version": CADENCE_EVIDENCE_SCHEMA,
        "trigger_calculation_version": CADENCE_TRIGGER_CALCULATION_VERSION,
        "active_data_quality_event_ids": [],
        "new_data_quality_event_ids": [],
        "symbols": {
            symbol: {field: False for field in trigger_fields}
            for symbol in ("SPX", "NDX")
        },
    }
    return with_scan_event_id(
        {
            "schema_version": 2,
            "event_type": "substantive_scan",
            "observed_at_ct": "2026-09-08T09:15:00-05:00",
            "observed_at_utc": "2026-09-08T14:15:00Z",
            "session_date": SESSION,
            "phase": "regular-session",
            "cadence": {
                "mode": "NORMAL",
                "substantive": True,
                "adaptive_evidence": adaptive,
            },
            "evidence": ["fixture:same-bucket-authorities"],
            "symbols": {
                symbol: {
                    "eligible": False,
                    "eligibility_reasons": ["SOURCE_INVALID"],
                }
                for symbol in ("SPX", "NDX")
            },
            "alerts": [],
            "directional_interpretation": "ABSTAIN",
            "research_hypotheses": [],
        }
    )


def test_ordered_milestones_are_deterministic_idempotent_and_terminal(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    milestones = [
        "startup",
        "first_eligible_gamma_capture",
        "complete_5m_orb",
        "complete_60m_orb",
    ]
    event_ids = []
    for milestone in milestones:
        result = _commit(state_path, journal_dir, milestone)
        assert result["accepted"] is True
        assert result["action"] == "committed"
        assert result["event_appended"] is True
        assert result["notification_required"] is True
        assert result["event_id"] == opening.milestone_event_id(SESSION, milestone)
        event_ids.append(result["event_id"])

    state = json.loads(state_path.read_text("utf-8"))
    acceptance = state["opening_acceptance"]
    assert acceptance["startup_milestone_notified"] is False
    assert acceptance["first_eligible_gamma_capture_notified"] is False
    assert acceptance["first_complete_5m_orb_notified"] is False
    assert acceptance["final_complete_60m_orb_notified"] is False
    assert acceptance["pending_notification_event_ids"] == event_ids
    assert acceptance["temporary_acceptance_checks_complete"] is True
    assert acceptance["temporary_acceptance_outcome"] == "complete_60m_orb"
    journal = _journal(journal_dir / f"{SESSION}.jsonl")
    milestone_rows = [row for row in journal if row.get("event_type") == opening.EVENT_TYPE]
    assert [row["event_id"] for row in milestone_rows] == event_ids
    assert [row["temporary_acceptance_terminal"] for row in milestone_rows] == [
        False,
        False,
        False,
        True,
    ]

    duplicate = _commit(state_path, journal_dir, "complete_60m_orb", report={})
    assert duplicate["accepted"] is True
    assert duplicate["action"] == "notification_pending"
    assert duplicate["event_appended"] is False
    assert duplicate["notification_required"] is True
    assert len(_journal(journal_dir / f"{SESSION}.jsonl")) == len(journal)

    acknowledged = _ack(state_path, journal_dir, event_ids)
    assert acknowledged["accepted"] is True, acknowledged
    assert acknowledged["action"] == "acknowledged"
    assert acknowledged["event_ids_acknowledged"] == event_ids
    assert acknowledged["pending_notification_event_ids"] == []
    acceptance = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert acceptance["startup_milestone_notified"] is True
    assert acceptance["first_eligible_gamma_capture_notified"] is True
    assert acceptance["first_complete_5m_orb_notified"] is True
    assert acceptance["final_complete_60m_orb_notified"] is True

    duplicate = _commit(state_path, journal_dir, "complete_60m_orb", report={})
    assert duplicate["action"] == "already_committed"
    assert duplicate["notification_required"] is False

    next_session = prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert next_session["accepted"] is True
    assert next_session["action"] == "rolled_over"
    wednesday = json.loads(state_path.read_text("utf-8"))
    assert wednesday["opening_acceptance"] == {
        "session_date": "2026-09-09",
        "startup_milestone_notified": False,
        "first_eligible_gamma_capture_notified": False,
        "first_complete_5m_orb_notified": False,
        "final_complete_60m_orb_notified": False,
        "temporary_acceptance_checks_complete": False,
    }
    assert wednesday["prior_session_reference"]["opening_acceptance"][
        "temporary_acceptance_checks_complete"
    ] is True


def test_real_orb_inspector_fields_survive_projection_and_acceptance(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    startup = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-08T13:35:04Z"),
    )
    assert startup["accepted"] is True, startup

    opening_utc = datetime(2026, 9, 8, 13, 30, tzinfo=timezone.utc)
    rows = []
    for symbol in ("SPX", "NDX", "VIX", "RUT"):
        for offset in range(61):
            sample = opening_utc + timedelta(seconds=offset * 5)
            rows.append(
                {
                    "symbol": symbol,
                    "sample_timestamp_utc": sample.isoformat(),
                    "source_timestamp_utc": sample.isoformat(),
                    "captured_at_utc": (sample + timedelta(milliseconds=100)).isoformat(),
                    "subscription_epoch_id": EPOCH,
                    "subscription_generation": 7,
                    "provider": "databento",
                    "validation_status": "valid",
                    "primary_expiration": SESSION,
                    "same_day_profile_available": True,
                    "reference_price": 7_500.0 + offset / 10.0,
                    "spot_source": "databento_opra_put_call_parity",
                    "spot_formula_version": "put-call-parity-v2-discounted-strike",
                    "risk_free_rate": 0.0525,
                    "universe_sha256": "a" * 64,
                    "symbol_mapping_version": "c" * 64,
                    "progress_eligible": True,
                }
            )
    inspected_rows = opening_capture_inspector._orb_reference_evidence(
        rows,
        trading_day=datetime.fromisoformat(SESSION).date(),
        observed_at_ct=datetime(2026, 9, 8, 8, 35, 5, tzinfo=opening._CT),
    )
    report = _report("2026-09-08T13:35:05Z")
    report["database"]["orb_reference_rows"] = inspected_rows
    report["database"]["orb_reference_progress_decisions"] = {
        symbol: {
            "raw_row_count": 61,
            "pending_decision_count": 0,
            "ineligible_decision_count": 0,
            "eligible_decision_count": 61,
        }
        for symbol in ("SPX", "NDX", "VIX", "RUT")
    }
    for symbol in ("SPX", "NDX", "VIX", "RUT"):
        symbol_rows = [row for row in rows if row["symbol"] == symbol]
        journal = MarketStructureJournal(
            saver=lambda _row: None,
            loader=lambda *_args, **_kwargs: [],
            reference_saver=lambda _row: None,
            reference_loader=lambda *_args, _rows=symbol_rows, **_kwargs: _rows,
            now_utc=lambda: datetime(2026, 9, 8, 13, 35, 5, tzinfo=timezone.utc),
            expected_cadence_seconds=5.0,
        )
        snapshot = journal.snapshot(
            symbol,
            trading_date=datetime.fromisoformat(SESSION).date(),
            as_of_utc=datetime(2026, 9, 8, 13, 35, 5, tzinfo=timezone.utc),
            active_subscription_epoch_id=EPOCH,
            active_subscription_generation=7,
            active_handoff_status="active",
        )
        actual_window = snapshot["opening_ranges"]["5m"]
        assert actual_window["clock_status"] == "closed"
        assert actual_window["capture_status"] == "complete"
        report["orb"]["payload"]["symbols"][symbol]["opening_ranges"]["5m"] = (
            actual_window
        )

    projected = opening._bounded_report_projection(report)
    for symbol in ("SPX", "NDX", "VIX", "RUT"):
        row = projected["database"]["orb_reference_rows"][symbol]
        assert row["latest_provider"] == "databento"
        assert row["latest_validation_status"] == "valid"

    accepted = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=report,
    )
    assert accepted["accepted"] is True, accepted.get("issues")


def test_orb_receipts_do_not_depend_on_gamma_and_on_time_run_can_be_receipted_later(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    startup = _commit(state_path, journal_dir, "startup")
    assert startup["accepted"] is True, startup

    five = _commit(state_path, journal_dir, "complete_5m_orb")
    assert five["accepted"] is True, five
    state = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert state.get("first_eligible_gamma_capture") is None
    assert state["first_complete_5m_orb"]["event_id"] == five["event_id"]

    sixty = _commit(state_path, journal_dir, "complete_60m_orb")
    assert sixty["accepted"] is True, sixty
    state = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert state.get("first_eligible_gamma_capture") is None
    assert state["temporary_acceptance_checks_complete"] is False

    gamma = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=_report("2026-09-08T14:32:00Z"),
    )
    assert gamma["accepted"] is True, gamma
    state = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert state["temporary_acceptance_checks_complete"] is True
    rows = [
        row
        for row in _journal(journal_dir / f"{SESSION}.jsonl")
        if row.get("event_type") == opening.EVENT_TYPE
    ]
    assert [row["milestone"] for row in rows] == [
        "startup",
        "complete_5m_orb",
        "complete_60m_orb",
        "first_eligible_gamma_capture",
    ]
    assert [row["temporary_acceptance_terminal"] for row in rows] == [
        False,
        False,
        False,
        True,
    ]
    assert gamma["event_id"] == opening.milestone_event_id(
        SESSION, "first_eligible_gamma_capture"
    )
    event_ids = [row["event_id"] for row in rows]
    acknowledged = _ack(state_path, journal_dir, event_ids)
    assert acknowledged["accepted"] is True, acknowledged
    next_session = prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert next_session["accepted"] is True, next_session
    assert next_session["action"] == "rolled_over"


def test_first_gamma_run_accepts_90_second_boundary_and_rejects_epsilon_late(
    tmp_path: Path,
) -> None:
    on_time_state, on_time_journal = _armed_session(tmp_path / "on-time")
    on_time_report = _report("2026-09-08T13:32:00Z")
    for symbol in ("SPX", "NDX"):
        _set_first_gamma_run(
            on_time_report,
            symbol,
            seconds_after_cash_open=90.0,
            capture_seconds_after_cash_open=90.0,
        )

    accepted = _commit(
        on_time_state,
        on_time_journal,
        "first_eligible_gamma_capture",
        report=on_time_report,
    )
    assert accepted["accepted"] is True, accepted

    late_state, late_journal = _armed_session(tmp_path / "late")
    late_report = _report("2026-09-08T13:32:00Z")
    _set_first_gamma_run(
        late_report,
        "SPX",
        seconds_after_cash_open=90.0,
    )
    _set_first_gamma_run(
        late_report,
        "NDX",
        seconds_after_cash_open=90.001,
    )
    before_state = late_state.read_bytes()
    before_journal = (late_journal / f"{SESSION}.jsonl").read_bytes()

    rejected = _commit(
        late_state,
        late_journal,
        "first_eligible_gamma_capture",
        report=late_report,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["first_eligible_gamma_capture_late:NDX"]
    assert late_state.read_bytes() == before_state
    assert (late_journal / f"{SESSION}.jsonl").read_bytes() == before_journal


def test_first_gamma_pair_capture_accepts_90_seconds_and_rejects_epsilon_late(
    tmp_path: Path,
) -> None:
    on_time_state, on_time_journal = _armed_session(tmp_path / "capture-on-time")
    on_time_report = _report("2026-09-08T13:32:00Z")
    for symbol in ("SPX", "NDX"):
        _set_first_gamma_run(
            on_time_report,
            symbol,
            seconds_after_cash_open=89.0,
            capture_seconds_after_cash_open=90.0,
        )

    accepted = _commit(
        on_time_state,
        on_time_journal,
        "first_eligible_gamma_capture",
        report=on_time_report,
    )
    assert accepted["accepted"] is True, accepted

    late_state, late_journal = _armed_session(tmp_path / "capture-late")
    late_report = _report("2026-09-08T13:32:00Z")
    for symbol in ("SPX", "NDX"):
        _set_first_gamma_run(
            late_report,
            symbol,
            seconds_after_cash_open=89.0,
            capture_seconds_after_cash_open=90.001,
        )
    before_state = late_state.read_bytes()
    before_journal = (late_journal / f"{SESSION}.jsonl").read_bytes()

    rejected = _commit(
        late_state,
        late_journal,
        "first_eligible_gamma_capture",
        report=late_report,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "first_eligible_gamma_capture_late:PAIR"
    ]
    assert late_state.read_bytes() == before_state
    assert (late_journal / f"{SESSION}.jsonl").read_bytes() == before_journal


def test_first_gamma_accepts_coherent_prior_runtime_identity(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    report = _report("2026-09-08T13:32:00Z")
    prior_epoch = "f" * 64
    prior_universe = "c" * 64
    for symbol in ("SPX", "NDX"):
        evidence = report["database"]["market_structure_rows"][symbol][
            "first_eligible_calculation_bound"
        ]
        evidence["subscription_epoch_id"] = prior_epoch
        evidence["subscription_generation"] = 6
        evidence["universe_sha256"] = prior_universe

    accepted = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )

    assert accepted["accepted"] is True, accepted


def test_first_gamma_rejects_incoherent_cross_runtime_pair(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    report = _report("2026-09-08T13:32:00Z")
    report["database"]["market_structure_rows"]["NDX"][
        "first_eligible_calculation_bound"
    ]["subscription_epoch_id"] = "f" * 64
    before_state = state_path.read_bytes()
    before_journal = (journal_dir / f"{SESSION}.jsonl").read_bytes()

    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "first_eligible_gamma_capture_identity_mismatch"
    ]
    assert state_path.read_bytes() == before_state
    assert (journal_dir / f"{SESSION}.jsonl").read_bytes() == before_journal


def test_late_first_gamma_does_not_revoke_independent_orb_receipts(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    five = _commit(state_path, journal_dir, "complete_5m_orb")
    sixty = _commit(state_path, journal_dir, "complete_60m_orb")
    assert five["accepted"] is True
    assert sixty["accepted"] is True

    late_report = _report("2026-09-08T14:32:00Z")
    for symbol in ("SPX", "NDX"):
        _set_first_gamma_run(
            late_report,
            symbol,
            seconds_after_cash_open=90.001,
        )
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=late_report,
    )

    assert rejected["accepted"] is False
    assert rejected["issues"] == ["first_eligible_gamma_capture_late:SPX"]
    acceptance = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert acceptance["first_complete_5m_orb"]["event_id"] == five["event_id"]
    assert acceptance["final_complete_60m_orb"]["event_id"] == sixty["event_id"]
    assert acceptance.get("first_eligible_gamma_capture") is None
    assert acceptance["temporary_acceptance_checks_complete"] is False


def test_orb_runtime_gates_are_independent_of_gamma_and_protected_rut_gex(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path / "orb")
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    report = _report("2026-09-08T13:36:00Z")
    report["backend_live_health"]["payload"].update(
        calculation_ready=False,
        prediction_pipeline_ok=False,
    )
    report["backend_health"]["payload"]["optional_family_canary"] = {
        "state": "protected_opening_orb",
        "rollback_required": False,
        "evaluation_evidence": {
            "decision_state": "protected_opening_orb",
            "opening_orb_protection_active": True,
            "cash_session_reconnect_protection_active": True,
        },
    }
    report["out_of_scope_issues"] = [
        "LIVE_GATE_FAILED:calculation_ready",
        "LIVE_GATE_FAILED:prediction_pipeline_ok",
        "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE",
    ]
    accepted = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=report,
    )
    assert accepted["accepted"] is True, accepted.get("issues")

    report = _report("2026-09-08T14:31:00Z")
    report["backend_live_health"]["payload"].update(
        calculation_ready=False,
        prediction_pipeline_ok=False,
    )
    report["backend_health"]["payload"]["optional_family_canary"] = {
        "state": "protected_cash_session",
        "rollback_required": False,
        "evaluation_evidence": {
            "decision_state": "protected_cash_session",
            "opening_orb_protection_active": False,
            "cash_session_reconnect_protection_active": True,
        },
    }
    report["out_of_scope_issues"] = [
        "LIVE_GATE_FAILED:calculation_ready",
        "LIVE_GATE_FAILED:prediction_pipeline_ok",
        "RUT_CANARY_CASH_SESSION_PROTECTION_ACTIVE",
    ]
    accepted = _commit(
        state_path,
        journal_dir,
        "complete_60m_orb",
        report=report,
    )
    assert accepted["accepted"] is True, accepted.get("issues")

    state_path, journal_dir = _armed_session(tmp_path / "transport")
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    report = _report("2026-09-08T13:36:00Z")
    report["backend_live_health"]["payload"]["stream_progressing"] = False
    rejected = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=report,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["live_gate_failed:stream_progressing"]

    state_path, journal_dir = _armed_session(tmp_path / "gamma")
    report = _report("2026-09-08T13:31:00Z")
    report["backend_live_health"]["payload"]["calculation_ready"] = False
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["live_gate_failed:calculation_ready"]

    state_path, journal_dir = _armed_session(tmp_path / "canary-evidence")
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    report = _report("2026-09-08T13:36:00Z")
    report["backend_health"]["payload"]["optional_family_canary"] = {
        "state": "protected_opening_orb",
        "rollback_required": False,
        "evaluation_evidence": {
            "decision_state": "protected_opening_orb",
            "opening_orb_protection_active": True,
            "cash_session_reconnect_protection_active": False,
        },
    }
    report["out_of_scope_issues"] = [
        "RUT_CANARY_OPENING_ORB_PROTECTION_ACTIVE"
    ]
    rejected = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=report,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["optional_family_canary_protection_invalid"]


def test_gamma_can_commit_first_then_startup_orbs_ack_and_rollover(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    gamma = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=_report("2026-09-08T13:31:00Z"),
    )
    assert gamma["accepted"] is True, gamma
    assert gamma["temporary_acceptance_checks_complete"] is False

    startup = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-08T13:32:00Z"),
    )
    five = _commit(state_path, journal_dir, "complete_5m_orb")
    sixty = _commit(state_path, journal_dir, "complete_60m_orb")
    assert startup["accepted"] is True, startup
    assert five["accepted"] is True, five
    assert sixty["accepted"] is True, sixty
    assert sixty["temporary_acceptance_checks_complete"] is True

    rows = [
        row
        for row in _journal(journal_dir / f"{SESSION}.jsonl")
        if row.get("event_type") == opening.EVENT_TYPE
    ]
    assert [row["milestone"] for row in rows] == [
        "first_eligible_gamma_capture",
        "startup",
        "complete_5m_orb",
        "complete_60m_orb",
    ]
    assert [row["temporary_acceptance_terminal"] for row in rows] == [
        False,
        False,
        False,
        True,
    ]
    event_ids = [row["event_id"] for row in rows]
    state = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert state["pending_notification_event_ids"] == event_ids
    assert state["temporary_acceptance_checks_complete"] is True

    replay = _commit(state_path, journal_dir, "first_eligible_gamma_capture", report={})
    assert replay["accepted"] is True, replay
    assert replay["action"] == "notification_pending"
    assert replay["event_id"] == gamma["event_id"]

    acknowledged = _ack(state_path, journal_dir, event_ids)
    assert acknowledged["accepted"] is True, acknowledged
    assert acknowledged["pending_notification_event_ids"] == []
    next_session = prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert next_session["accepted"] is True, next_session
    assert next_session["action"] == "rolled_over"


def test_gamma_acceptance_is_core_only_but_shared_runtime_stays_strict(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path / "no-startup")
    report = _report("2026-09-08T13:31:00Z")
    report["database"]["orb_reference_rows"]["SPX"][
        "opening_bucket_present"
    ] = False
    report["backend_health"]["payload"]["optional_family_canary"] = {
        "state": "rolled_back",
        "rollback_required": True,
    }
    report["out_of_scope_issues"] = [
        "ORB_REFERENCE_OPENING_BUCKET_MISSING:SPX",
        "RUT_CANARY_STATE_INVALID:rolled_back",
    ]
    startup_rejected = _commit(
        state_path,
        journal_dir,
        "startup",
        report=report,
    )
    assert startup_rejected["accepted"] is False
    assert startup_rejected["issues"] == [
        "inspector_out_of_scope_issues_not_allowed"
    ]
    accepted = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )
    assert accepted["accepted"] is True, accepted
    acceptance = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert acceptance.get("startup_milestone") is None
    assert acceptance["first_eligible_gamma_capture"]["event_id"] == accepted[
        "event_id"
    ]

    optional_counts = {"VIX": 200, "RUT": 1_000}
    for missing_symbol, missing_count in optional_counts.items():
        state_path, journal_dir = _armed_session(tmp_path / missing_symbol.lower())
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True

        report = _report("2026-09-08T13:31:00Z")
        report["orb"]["payload"]["configured_symbols"].remove(missing_symbol)
        report["orb"]["payload"]["requested_symbols"].remove(missing_symbol)
        report["orb"]["payload"]["symbols"].pop(missing_symbol)
        health_payload = report["backend_health"]["payload"]
        if missing_symbol == "RUT":
            health_payload["core_symbol_status"].pop(missing_symbol)
            health_payload["subscription_metadata"]["markets"].pop(
                missing_symbol
            )
        else:
            health_payload["core_symbol_status"][missing_symbol][
                "contracts_subscribed"
            ] = 0
            health_payload["subscription_metadata"]["markets"][missing_symbol][
                "selected_contract_count"
            ] = 0
        health_payload["symbols_subscribed"] -= missing_count
        health_payload["subscription_metadata"]["selected_contract_count"] -= (
            missing_count
        )
        report["out_of_scope_issues"] = [
            f"SUBSCRIPTION_ROOT_MISSING:{missing_symbol}",
            f"ORB_SYMBOL_NOT_CONFIGURED:{missing_symbol}",
        ]
        if missing_symbol == "RUT":
            health_payload["optional_family_canary"] = {
                "state": "rolled_back",
                "rollback_required": True,
            }
            report["out_of_scope_issues"].append(
                "RUT_CANARY_STATE_INVALID:rolled_back"
            )

        accepted = _commit(
            state_path,
            journal_dir,
            "first_eligible_gamma_capture",
            report=report,
        )
        assert accepted["accepted"] is True, (missing_symbol, accepted)

    state_path, journal_dir = _armed_session(tmp_path / "core-only-profile")
    core_only = _report("2026-09-08T13:31:00Z")
    core_only["orb"]["payload"]["configured_symbols"].remove("RUT")
    core_only["orb"]["payload"]["requested_symbols"].remove("RUT")
    core_only["orb"]["payload"]["symbols"].pop("RUT")
    core_health = core_only["backend_health"]["payload"]
    core_health["symbols_requested"].remove("RUT")
    core_health["core_symbol_status"].pop("RUT")
    core_health["subscription_metadata"]["markets"].pop("RUT")
    core_health["symbols_subscribed"] -= 1_000
    core_health["subscription_metadata"]["selected_contract_count"] -= 1_000
    core_health["subscription_bounds"]["max_subscription_contracts"] = 3_600
    core_health["optional_family_canary"] = {
        "state": "disabled",
        "rollback_required": False,
    }
    core_only["out_of_scope_issues"] = [
        "SUBSCRIPTION_ROOT_MISSING:RUT",
        "ORB_SYMBOL_NOT_CONFIGURED:RUT",
    ]
    accepted = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=core_only,
    )
    assert accepted["accepted"] is True, accepted

    state_path, journal_dir = _armed_session(tmp_path / "core-defect")
    core_defect = _report("2026-09-08T13:31:00Z")
    core_defect["database"]["market_structure_rows"]["SPX"][
        "latest_gamma_pin"
    ] = None
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=core_defect,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["gamma_pin_unavailable:SPX"]

    state_path, journal_dir = _armed_session(tmp_path / "runtime-defect")
    runtime_defect = _report("2026-09-08T13:31:00Z")
    runtime_defect["backend_live_health"]["payload"]["stream_progressing"] = False
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=runtime_defect,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["live_gate_failed:stream_progressing"]

    state_path, journal_dir = _armed_session(tmp_path / "current-day-defect")
    current_day_defect = _report("2026-09-08T13:31:00Z")
    current_day_defect["backend_health"]["payload"]["universe_fallback_active"] = True
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=current_day_defect,
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == ["current_day_universe_invalid"]


def test_opening_owner_recomputes_loaded_code_and_rejects_stale_process_evidence(
    tmp_path: Path,
) -> None:
    cases = []

    report = _report("2026-09-08T13:00:00Z")
    del report["backend_health"]["payload"]["loaded_code_fingerprint"]
    cases.append(("missing", report, "loaded_code_fingerprint_missing"))

    report = _report("2026-09-08T13:00:00Z")
    report["backend_health"]["payload"]["loaded_code_fingerprint"][
        "captured_at_utc"
    ] = "2026-09-07T12:45:00+00:00"
    cases.append(
        (
            "prior-session",
            report,
            "loaded_code_fingerprint_not_current_session",
        )
    )

    report = _report("2026-09-08T13:00:00Z")
    report["backend_health"]["payload"]["loaded_code_fingerprint"]["files"].pop(
        "backend/api/routers/orb.py"
    )
    cases.append(
        (
            "source-set",
            report,
            "loaded_code_fingerprint_source_set_invalid",
        )
    )

    report = _report("2026-09-08T13:00:00Z")
    report["backend_health"]["payload"]["loaded_code_fingerprint"]["files"][
        "backend/market_structure.py"
    ]["loaded_at_startup_sha256"] = "0" * 64
    cases.append(
        (
            "stale-source",
            report,
            "loaded_code_fingerprint_source_mismatch:backend/market_structure.py",
        )
    )

    for name, report, expected_issue in cases:
        state_path, journal_dir = _armed_session(tmp_path / name)
        rejected = _commit(
            state_path,
            journal_dir,
            "startup",
            report=report,
        )
        assert rejected["accepted"] is False
        assert rejected["commit_phase"] == "not_started"
        assert rejected["notification_required"] is False
        assert rejected["issues"] == [expected_issue]


def test_gamma_acceptance_preserves_newest_unbound_view_with_verified_bound_capture(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    report = _report("2026-09-08T13:31:00Z")
    for symbol in ("SPX", "NDX"):
        row = report["database"]["market_structure_rows"][symbol]
        row.update(
            latest_calculation_id=None,
            latest_reference_price=7_511.0,
            latest_gamma_pin=7_530.0,
            latest_max_pain=7_505.0,
        )
        state = report["orb"]["payload"]["symbols"][symbol]
        state["pin_behavior"].update(gamma_pin=7_530.0, max_pain=7_505.0)
        state["last_known_structure"].update(
            gamma_pin=7_530.0,
            max_pain=7_505.0,
            calculation_id=None,
        )

    accepted = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )

    assert accepted["accepted"] is True
    assert accepted["action"] == "committed"


def test_gamma_acceptance_rejects_invalid_bound_or_lineage_evidence(
    tmp_path: Path,
) -> None:
    cases = {
        "missing": (
            lambda report: report["database"]["market_structure_rows"]["SPX"][
                "latest_calculation_bound"
            ].update(row_count=0),
            "market_structure_calculation_bound_missing:SPX",
        ),
        "stale": (
            lambda report: report["database"]["market_structure_rows"]["SPX"][
                "latest_calculation_bound"
            ].update(source_age_seconds=91.0),
            "market_structure_calculation_bound_invalid:SPX",
        ),
        "cross-generation": (
            lambda report: report["database"]["market_structure_rows"]["SPX"][
                "latest_calculation_bound"
            ].update(subscription_generation=6),
            "market_structure_calculation_bound_invalid:SPX",
        ),
        "bad-lineage": (
            lambda report: report["database"]["market_structure_rows"]["SPX"][
                "latest_calculation_bound"
            ]["lineage"].update(
                status="invalid",
                reason="GAMMA_CALCULATION_INPUT_BLOB_MISSING",
                input_blob_present=False,
            ),
            "market_structure_calculation_lineage_invalid:SPX",
        ),
        "api-mismatch": (
            lambda report: report["orb"]["payload"]["symbols"]["SPX"][
                "last_calculation_bound_structure"
            ].update(calculation_id="different-calculation"),
            "orb_calculation_bound_structure_invalid:SPX",
        ),
    }
    for name, (mutate, expected_issue) in cases.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _report("2026-09-08T13:31:00Z")
        mutate(report)

        rejected = _commit(
            state_path,
            journal_dir,
            "first_eligible_gamma_capture",
            report=report,
        )

        assert rejected["accepted"] is False, name
        assert rejected["issues"] == [expected_issue]


def test_skip_and_wrong_session_reports_fail_without_writes(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    journal_path = journal_dir / f"{SESSION}.jsonl"
    before_state = state_path.read_bytes()
    before_journal = journal_path.read_bytes()

    skipped = _commit(state_path, journal_dir, "complete_5m_orb")
    assert skipped["accepted"] is False
    assert skipped["issues"] == [
        "opening_milestone_prerequisite_missing:startup"
    ]
    assert state_path.read_bytes() == before_state
    assert journal_path.read_bytes() == before_journal

    not_due = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-08T12:54:59Z"),
    )
    assert not_due["accepted"] is False
    assert not_due["issues"] == ["milestone_not_due:startup"]
    assert state_path.read_bytes() == before_state
    assert journal_path.read_bytes() == before_journal

    wrong = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-09T13:31:00Z", session_date="2026-09-09"),
    )
    assert wrong["accepted"] is False
    assert wrong["issues"] == ["inspector_report_session_mismatch"]
    assert state_path.read_bytes() == before_state
    assert journal_path.read_bytes() == before_journal


def test_state_regression_cannot_be_blessed_by_a_later_milestone(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    state = json.loads(state_path.read_text("utf-8"))
    state["opening_acceptance"]["startup_milestone_notified"] = True
    _write_json(state_path, state)

    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "opening_acceptance_state_receipt_mismatch"
    ]


def test_journal_durable_receipt_recovers_before_state_with_new_bad_input(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)

    def interrupt(stage: str) -> None:
        if stage == "after_event_append":
            raise OSError("simulated receipt/state gap")

    interrupted = _commit(
        state_path,
        journal_dir,
        "startup",
        failpoint=interrupt,
    )
    assert interrupted["accepted"] is False
    assert interrupted["action"] == "retry_required"
    assert interrupted["commit_phase"] == "journal_durable"
    assert interrupted["event_appended"] is True
    assert json.loads(state_path.read_text("utf-8"))["opening_acceptance"][
        "startup_milestone_notified"
    ] is False

    recovered = _commit(state_path, journal_dir, "startup", report={"bad": True})
    assert recovered["accepted"] is True
    assert recovered["action"] == "recovered_and_committed"
    assert recovered["event_appended"] is False
    assert recovered["notification_required"] is True
    assert json.loads(state_path.read_text("utf-8"))["opening_acceptance"][
        "startup_milestone_notified"
    ] is False
    assert recovered["pending_notification_event_ids"] == [recovered["event_id"]]


def test_lock_busy_abstains_and_uses_shared_rollover_lock(
    tmp_path: Path, monkeypatch
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    observed_paths = []

    @contextmanager
    def busy_lock(path):
        observed_paths.append(path)
        raise MonitorScanLedgerError("monitor_scan_lock_unavailable")
        yield

    monkeypatch.setattr(opening, "_exclusive_lock", busy_lock)
    result = _commit(state_path, journal_dir, "startup")
    assert result["accepted"] is False
    assert result["action"] == "abstain"
    assert result["issues"] == ["monitor_scan_lock_unavailable"]
    assert observed_paths == [
        state_path.resolve().with_name(f"{state_path.name}.rollover.lock")
    ]


def test_tampered_receipt_and_event_id_collision_fail_closed(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    journal_path = journal_dir / f"{SESSION}.jsonl"
    rows = _journal(journal_path)
    receipt = next(row for row in rows if row.get("event_type") == opening.EVENT_TYPE)
    receipt["evidence"]["bounded_inspector_evidence_sha256"] = "f" * 64
    _write_jsonl = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n"
    journal_path.write_text(_write_jsonl, encoding="utf-8")

    tampered = _commit(state_path, journal_dir, "startup")
    assert tampered["accepted"] is False
    assert "milestone_event_evidence_mismatch" in tampered["issues"]

    second_state, second_dir = _armed_session(tmp_path / "collision")
    collision_path = second_dir / f"{SESSION}.jsonl"
    with collision_path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "schema_version": 2,
                    "event_id": opening.milestone_event_id(SESSION, "startup"),
                    "event_type": "diagnostic_probe",
                    "session_date": SESSION,
                },
                separators=(",", ":"),
            )
            + "\n"
        )
    collision = _commit(second_state, second_dir, "startup")
    assert collision["accepted"] is False
    assert collision["issues"] == ["opening_milestone_event_id_collision"]


def test_cli_commits_startup_and_rejects_noncompact_evidence(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    command = [
        sys.executable,
        str(TOOL),
        "--milestone",
        "startup",
        "--state-path",
        str(state_path),
        "--journal-dir",
        str(journal_dir),
    ]
    completed = subprocess.run(
        command,
        input=json.dumps(_report()),
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["accepted"] is True
    assert payload["action"] == "committed"

    state_path_2, journal_dir_2 = _armed_session(tmp_path / "invalid")
    invalid = _report()
    invalid["output_mode"] = "full"
    rejected = subprocess.run(
        [
            sys.executable,
            str(TOOL),
            "--milestone",
            "startup",
            "--state-path",
            str(state_path_2),
            "--journal-dir",
            str(journal_dir_2),
        ],
        input=json.dumps(invalid),
        text=True,
        capture_output=True,
        check=False,
        cwd=ROOT,
    )
    assert rejected.returncode == 1
    assert json.loads(rejected.stdout)["issues"] == [
        "inspector_output_must_be_compact"
    ]


def test_critical_compact_authorities_are_revalidated_and_extensions_not_persisted(
    tmp_path: Path,
) -> None:
    mutations = {
        "clock": lambda report: report["clock"].update(synchronized=False),
        "task": lambda report: report["scheduled_tasks"].update(error="denied"),
        "trading_date": lambda report: report["backend_health"]["payload"][
            "subscription_window"
        ].update(trading_date="1999-01-01"),
        "db_trigger": lambda report: report["database"].update(
            missing_market_structure_triggers=["missing"]
        ),
        "fallback": lambda report: report["backend_health"]["payload"].update(
            universe_fallback_active=True
        ),
        "high_cap": lambda report: (
            report["backend_health"]["payload"]["subscription_bounds"].update(
                max_subscription_contracts=999_999
            ),
            report["backend_health"]["payload"]["subscription_metadata"].update(
                selected_contract_count=5_000
            ),
            report["backend_health"]["payload"].update(symbols_subscribed=5_000),
        ),
        "missing_health_family": lambda report: report["backend_health"][
            "payload"
        ]["symbols_requested"].remove("SPX"),
        "market_shortfall": lambda report: report["backend_health"]["payload"][
            "subscription_metadata"
        ]["markets"]["NDX"].update(market_reservation_shortfall_pairs=1),
    }
    for name, mutate in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _report()
        mutate(report)
        before_state = state_path.read_bytes()
        before_journal = (journal_dir / f"{SESSION}.jsonl").read_bytes()
        rejected = _commit(state_path, journal_dir, "startup", report=report)
        assert rejected["accepted"] is False, (name, rejected)
        assert state_path.read_bytes() == before_state
        assert (journal_dir / f"{SESSION}.jsonl").read_bytes() == before_journal

    runtime_mutations = {
        "binding": lambda report: report["orb"]["payload"].update(
            runtime_binding_applied=False
        ),
        "context": lambda report: report["orb"]["payload"].update(
            runtime_context_stable=False
        ),
        "backend_clock": lambda report: report["backend_health"]["payload"][
            "processing_clock_telemetry"
        ].update(status="unsynchronized"),
        "structure_age": lambda report: report["database"][
            "market_structure_rows"
        ]["SPX"].update(latest_source_age_seconds=999999.0),
        "gamma_cross_authority": lambda report: report["database"][
            "market_structure_rows"
        ]["SPX"].update(latest_gamma_pin=100.0, latest_max_pain=200.0),
        "cross_universe": lambda report: report["database"][
            "market_structure_rows"
        ]["SPX"].update(latest_universe_sha256="c" * 64),
        }
    for name, mutate in runtime_mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        startup_result = _commit(state_path, journal_dir, "startup")
        assert startup_result["accepted"] is True, (name, startup_result)
        report = _report()
        mutate(report)
        rejected = _commit(
            state_path,
            journal_dir,
            "first_eligible_gamma_capture",
            report=report,
        )
        assert rejected["accepted"] is False, (name, rejected)

    state_path, journal_dir = _armed_session(tmp_path / "orb_complete")
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    assert _commit(
        state_path, journal_dir, "first_eligible_gamma_capture"
    )["accepted"] is True
    incomplete = _report()
    for symbol in incomplete["orb"]["payload"]["symbols"].values():
        symbol["opening_ranges"]["5m"]["orb_complete"] = False
    rejected = _commit(
        state_path, journal_dir, "complete_5m_orb", report=incomplete
    )
    assert rejected["accepted"] is False

    state_path, journal_dir = _armed_session(tmp_path / "bounded")
    report = _report()
    report["backend_health"]["payload"]["secret_like_extension"] = (
        "DO_NOT_PERSIST_THIS_VALUE"
    )
    accepted = _commit(state_path, journal_dir, "startup", report=report)
    assert accepted["accepted"] is True, accepted
    journal_raw = (journal_dir / f"{SESSION}.jsonl").read_text("utf-8")
    assert "secret_like_extension" not in journal_raw
    assert "DO_NOT_PERSIST_THIS_VALUE" not in journal_raw
    milestone = next(
        record
        for record in _journal(journal_dir / f"{SESSION}.jsonl")
        if record.get("event_type") == "opening_acceptance_milestone"
    )
    assert milestone["evidence"]["bounded_inspector_evidence"]["backend_health"][
        "payload"
    ]["loaded_code_fingerprint"] == report["backend_health"]["payload"][
        "loaded_code_fingerprint"
    ]
    bounded_health = milestone["evidence"]["bounded_inspector_evidence"][
        "backend_health"
    ]["payload"]
    for field in opening._CONNECTION_LIFECYCLE_COMPACT_FIELDS:
        assert bounded_health[field] == report["backend_health"]["payload"][field]

    top_level = _report()
    top_level["secret_like_extension"] = "reject-me"
    state_path, journal_dir = _armed_session(tmp_path / "top-level")
    rejected = _commit(state_path, journal_dir, "startup", report=top_level)
    assert rejected["issues"] == ["inspector_compact_fields_invalid"]

    bounded_list_cases = (
        ("warning-object", "warnings", [{"nested_secret": "LEAK_ME"}]),
        ("note-object", "notes", [{"nested_secret": "LEAK_ME"}]),
        ("warning-oversize", "warnings", ["x" * 161]),
        ("note-oversize", "notes", ["x" * 161]),
        ("warning-count", "warnings", ["warning"] * 65),
    )
    for name, field, value in bounded_list_cases:
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _report()
        report[field] = value
        rejected = _commit(state_path, journal_dir, "startup", report=report)
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [f"inspector_{field}_invalid"]
        journal_raw = (journal_dir / f"{SESSION}.jsonl").read_text("utf-8")
        assert "LEAK_ME" not in journal_raw


def test_shared_runtime_counts_window_and_provenance_fail_closed(
    tmp_path: Path,
) -> None:
    mutations = {
        "provider": lambda report: report["backend_health"]["payload"].update(
            provider="polygon"
        ),
        "websocket": lambda report: report["backend_health"]["payload"].update(
            websocket="off_hours"
        ),
        "suppressed": lambda report: report["backend_health"]["payload"].update(
            subscription_suppressed=True
        ),
        "queue-full": lambda report: report["backend_health"]["payload"].update(
            provider_queue_full_warnings=1
        ),
        "slow-client": lambda report: report["backend_health"]["payload"].update(
            provider_slow_client_warnings=1
        ),
        "skipped-warning": lambda report: report["backend_health"][
            "payload"
        ].update(provider_skipped_record_warnings=1),
        "skipped-record": lambda report: report["backend_health"][
            "payload"
        ].update(provider_skipped_records=1),
        "reconnect": lambda report: report["backend_health"]["payload"].update(
            reconnect_attempts=1
        ),
        "connection-limit": lambda report: report["backend_health"][
            "payload"
        ].update(connection_limit_rejections_total=1),
        "connection-circuit": lambda report: report["backend_health"][
            "payload"
        ].update(connection_limit_circuit_state="open"),
        "pre-auth-guard": lambda report: report["backend_health"][
            "payload"
        ].update(pre_auth_transport_guard_status="unavailable"),
        "pre-auth-abort": lambda report: report["backend_health"][
            "payload"
        ].update(pre_auth_transport_aborts_total=1),
        "pre-auth-abort-failure": lambda report: report["backend_health"][
            "payload"
        ].update(pre_auth_transport_abort_failures_total=1),
        "client-close-unacknowledged": lambda report: report["backend_health"][
            "payload"
        ].update(last_client_close_status="pre_auth_close_unacknowledged"),
        "sleep-inactive": lambda report: report["backend_health"]["payload"][
            "runtime_controls"
        ]["sleep_prevention"].update(active=False),
        "window-closed": lambda report: report["backend_health"]["payload"][
            "subscription_window"
        ].update(state="post_close", subscription_allowed=False),
        "window-stale": lambda report: report["backend_health"]["payload"][
            "subscription_window"
        ].update(observed_at_utc="2026-09-08T12:59:44Z"),
        "window-future": lambda report: report["backend_health"]["payload"][
            "subscription_window"
        ].update(observed_at_utc="2026-09-08T13:00:01Z"),
        "nested-provenance": lambda report: report["backend_health"]["payload"][
            "subscription_metadata"
        ]["universe_provenance"].update(
            mode="prior_cache_filtered",
            source_date="2026-09-04",
            is_fallback=True,
        ),
        "source-count": lambda report: (
            report["backend_health"]["payload"]["universe_provenance"].update(
                source_rows=1
            ),
            report["backend_health"]["payload"]["subscription_metadata"][
                "universe_provenance"
            ].update(source_rows=1),
        ),
        "full-count": lambda report: report["backend_health"]["payload"][
            "subscription_metadata"
        ].update(full_contract_count=1),
        "global-subscribed": lambda report: report["backend_health"][
            "payload"
        ].update(symbols_subscribed=1),
        "family-status": lambda report: report["backend_health"]["payload"][
            "core_symbol_status"
        ]["SPX"].update(contracts_subscribed=1),
        "family-market": lambda report: report["backend_health"]["payload"][
            "subscription_metadata"
        ]["markets"]["SPX"].update(selected_contract_count=1),
        "market-over-global": lambda report: report["backend_health"][
            "payload"
        ]["subscription_metadata"]["markets"]["RUT"].update(
            selected_contract_count=10_000
        ),
    }
    for name, mutate in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _report("2026-09-08T13:00:00Z")
        mutate(report)
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(state_path, journal_dir, "startup", report=report)
        assert rejected["accepted"] is False, (name, rejected)
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_scheduled_task_run_times_are_phase_aware_and_causal(
    tmp_path: Path,
) -> None:
    task_cases = {
        "autostart-before-boundary": (
            "MarketPinPredictor_AutoStart",
            "2026-09-08T07:43:59-05:00",
        ),
        "autostart-future": (
            "MarketPinPredictor_AutoStart",
            "2026-09-08T23:59:00-05:00",
        ),
        "watchdog-before-boundary": (
            "MarketPinPredictor_Watchdog",
            "2026-09-08T07:48:59-05:00",
        ),
        "watchdog-future": (
            "MarketPinPredictor_Watchdog",
            "2026-09-08T23:59:00-05:00",
        ),
    }
    for name, (task_name, last_run_time) in task_cases.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        report = _report("2026-09-08T13:00:00Z")
        report["scheduled_tasks"]["tasks"][task_name][
            "last_run_time"
        ] = last_run_time
        rejected = _commit(state_path, journal_dir, "startup", report=report)
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [f"scheduled_task_not_current:{task_name}"]

    before_watchdog_due = _report("2026-09-08T12:48:30Z")
    opening._validate_task_evidence(
        before_watchdog_due,
        datetime(2026, 9, 8, 7, 48, 30, tzinfo=opening._CT),
    )


def test_warning_only_clock_and_preopen_rut_upgrade_remain_eligible(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    report = _report("2026-09-08T13:00:00Z")
    report["clock"].update(
        {
            "status": "windows_time_synchronized_external_offset_unavailable",
            "synchronized": None,
            "windows_time_synchronized": True,
            "external_offset_verified": False,
            "median_offset_seconds": None,
        }
    )
    report["warnings"] = ["PROCESSING_CLOCK_EXTERNAL_OFFSET_UNAVAILABLE"]
    report["orb"]["payload"]["configured_symbols"].remove("RUT")
    report["orb"]["payload"]["requested_symbols"].remove("RUT")
    report["orb"]["payload"]["symbols"].pop("RUT")
    health_payload = report["backend_health"]["payload"]
    health_payload["symbols_requested"].remove("RUT")
    health_payload["symbols_subscribed"] = 2_200
    health_payload["core_symbol_status"].pop("RUT")
    health_payload["subscription_metadata"]["selected_contract_count"] = 2_200
    health_payload["subscription_metadata"]["markets"].pop("RUT")
    health_payload["optional_family_canary"] = {
        "state": "blocked",
        "rollback_required": False,
    }
    report["warnings"].append("RUT_PREOPEN_UPGRADE_PENDING")

    accepted = _commit(state_path, journal_dir, "startup", report=report)
    assert accepted["accepted"] is True, accepted


def test_startup_revalidates_required_orb_symbol_sets_and_boundary(
    tmp_path: Path,
) -> None:
    cases: list[tuple[str, dict, str]] = []

    zero = _report("2026-09-08T13:00:00Z")
    zero["orb"]["payload"].update(
        configured_symbols=[], requested_symbols=[], symbols={}
    )
    cases.append(
        ("zero", zero, "orb_startup_configured_symbols_missing:NDX,SPX,VIX")
    )

    spx_missing = _report("2026-09-08T13:00:00Z")
    spx_missing["orb"]["payload"]["symbols"].pop("SPX")
    cases.append(
        ("spx", spx_missing, "orb_startup_symbol_evidence_missing:SPX")
    )

    rut_late = _report("2026-09-08T13:29:00Z")
    rut_late["orb"]["payload"]["configured_symbols"].remove("RUT")
    rut_late["orb"]["payload"]["requested_symbols"].remove("RUT")
    rut_late["orb"]["payload"]["symbols"].pop("RUT")
    rut_late["warnings"] = ["RUT_PREOPEN_UPGRADE_PENDING"]
    cases.append(
        ("rut-after-boundary", rut_late, "orb_startup_configured_symbols_missing:RUT")
    )

    for name, report, expected in cases:
        state_path, journal_dir = _armed_session(tmp_path / name)
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(state_path, journal_dir, "startup", report=report)
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [expected]
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_orb_milestones_remain_independent_of_current_gex_and_vix_structure(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    assert _commit(
        state_path, journal_dir, "first_eligible_gamma_capture"
    )["accepted"] is True

    report = _report()
    report["database"]["market_structure_rows"] = {}
    for symbol_state in report["orb"]["payload"]["symbols"].values():
        symbol_state.pop("last_known_structure", None)
        for field in (
            "structure_vs_reference_aligned",
            "structure_reference_status",
            "structure_reference_fresh",
            "structure_reference_age_seconds",
            "structure_reference_max_age_seconds",
        ):
            symbol_state["provenance"].pop(field, None)
        symbol_state.pop("pin_behavior", None)
    for symbol in ("SPX", "NDX"):
        report["orb"]["payload"]["symbols"][symbol]["opening_ranges"]["5m"][
            "combined_structure_directional_evidence_eligible"
        ] = False
    report["out_of_scope_issues"] = [
        "VALID_GEX_STRUCTURE_NOT_ADVANCING:SPX",
        "VALID_GEX_STRUCTURE_NOT_ADVANCING:NDX",
        "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:SPX",
        "ORB_5m_COMBINED_STRUCTURE_NOT_ELIGIBLE:NDX",
    ]

    accepted = _commit(
        state_path, journal_dir, "complete_5m_orb", report=report
    )
    assert accepted["accepted"] is True, accepted


def test_orb_complete_claim_revalidates_counts_gaps_ratio_and_window_identity(
    tmp_path: Path,
) -> None:
    mutations = {
        "gap": lambda report: report["orb"]["payload"]["symbols"]["SPX"][
            "opening_ranges"
        ]["5m"]["capture_evidence"].update(max_gap_seconds=31.0),
        "ratio": lambda report: report["orb"]["payload"]["symbols"]["SPX"][
            "opening_ranges"
        ]["5m"]["capture_evidence"].update(sample_count=59, capture_ratio=1.0),
        "boundary": lambda report: report["orb"]["payload"]["symbols"][
            "SPX"
        ]["opening_ranges"]["5m"].update(
            range_start_utc="2026-09-08T13:35:00Z",
            range_end_utc="2026-09-08T13:40:00Z",
        ),
        "persisted-ratio": lambda report: report["database"][
            "orb_reference_rows"
        ]["SPX"].update(opening_capture_ratio=2.0),
    }
    for name, mutate in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True
        report = _report("2026-09-08T13:36:00Z")
        mutate(report)
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(
            state_path, journal_dir, "complete_5m_orb", report=report
        )
        assert rejected["accepted"] is False, (name, rejected)
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_context_only_rut_can_complete_capture_without_directional_promotion(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True

    five_report = _rut_forward_context(_report("2026-09-08T13:36:00Z"))
    five = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=five_report,
    )
    assert five["accepted"] is True, five
    assert five["temporary_acceptance_checks_complete"] is False

    sixty_report = _rut_forward_context(_report("2026-09-08T14:31:00Z"))
    sixty = _commit(
        state_path,
        journal_dir,
        "complete_60m_orb",
        report=sixty_report,
    )
    assert sixty["accepted"] is True, sixty
    assert sixty["temporary_acceptance_checks_complete"] is False

    rows = [
        row
        for row in _journal(journal_dir / f"{SESSION}.jsonl")
        if row.get("event_type") == opening.EVENT_TYPE
    ]
    for row in rows[-2:]:
        bounded = row["evidence"]["bounded_inspector_evidence"]
        rut = bounded["orb"]["payload"]["symbols"]["RUT"]
        assert rut["reference_semantics"] == {
            "kind": "non_same_day_index_option_forward_context",
            "source": "databento_opra_put_call_parity",
            "authority": "research_reference_only",
            "primary_expiration": "2026-09-09",
            "same_day_profile_available": False,
            "directional_base_eligible": False,
            "limitation": (
                "The primary option expiry is not same-day, so its parity level "
                "is context only."
            ),
        }
        persisted = bounded["database"]["orb_reference_rows"]["RUT"]
        assert persisted["latest_primary_expiration"] == "2026-09-09"
        assert persisted["latest_same_day_profile_available"] is False
        assert row["symbols"]["RUT"]["directional_evidence_eligible"] is False


def test_context_only_rut_rejects_directional_promotion_and_bad_classification(
    tmp_path: Path,
) -> None:
    mutations = {
        "directional-promotion": (
            lambda report: report["orb"]["payload"]["symbols"]["RUT"][
                "opening_ranges"
            ]["5m"].update(directional_evidence_eligible=True),
            "rut_orb_directional_promotion_invalid",
        ),
        "pending-classification": (
            lambda report: report["orb"]["payload"]["symbols"]["RUT"][
                "reference_semantics"
            ].update(kind="pending_live_expiration_classification"),
            "rut_orb_classification_invalid",
        ),
        "persisted-mismatch": (
            lambda report: report["database"]["orb_reference_rows"]["RUT"].update(
                latest_primary_expiration=SESSION,
                latest_same_day_profile_available=True,
            ),
            "rut_orb_classification_mismatch",
        ),
    }
    for name, (mutate, expected_issue) in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True
        report = _rut_forward_context(_report("2026-09-08T13:36:00Z"))
        mutate(report)
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(
            state_path,
            journal_dir,
            "complete_5m_orb",
            report=report,
        )
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [expected_issue]
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_orb_receipt_revalidates_persisted_advancement_and_identity_sets(
    tmp_path: Path,
) -> None:
    mutations = {
        "advancing": lambda row: row.update(advancing_5s_evidence=False),
        "provider-source": lambda row: row.update(
            provider_source_timestamps_advancing=False
        ),
        "invalid-epoch": lambda row: row.update(
            invalid_subscription_epoch_row_count=1
        ),
        "mixed-epoch": lambda row: row.update(mixed_subscription_epoch_rows=True),
        "invalid-generation": lambda row: row.update(
            invalid_subscription_generation_row_count=1
        ),
        "mixed-generation": lambda row: row.update(
            mixed_subscription_generation_rows=True
        ),
    }
    for name, mutate in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True
        report = _report("2026-09-08T13:36:00Z")
        mutate(report["database"]["orb_reference_rows"]["SPX"])
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(
            state_path,
            journal_dir,
            "complete_5m_orb",
            report=report,
        )
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == ["orb_reference_row_invalid:SPX"]
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_orb_receipt_rejects_forged_or_incomplete_progress_decision_counts(
    tmp_path: Path,
) -> None:
    mutations = {
        "pending": (
            lambda row: row.update(
                raw_row_count=722,
                pending_decision_count=1,
            ),
            "orb_reference_decision_pending:SPX",
        ),
        "negative": (
            lambda row: row.update(ineligible_decision_count=-1),
            "orb_reference_decision_counts_invalid:SPX",
        ),
        "partition-mismatch": (
            lambda row: row.update(raw_row_count=722),
            "orb_reference_decision_counts_mismatch:SPX",
        ),
        "eligible-row-mismatch": (
            lambda row: row.update(
                raw_row_count=721,
                eligible_decision_count=720,
                ineligible_decision_count=1,
            ),
            "orb_reference_decision_counts_mismatch:SPX",
        ),
    }
    for name, (mutate, expected_issue) in mutations.items():
        state_path, journal_dir = _armed_session(tmp_path / name)
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True
        report = _report("2026-09-08T13:36:00Z")
        mutate(report["database"]["orb_reference_progress_decisions"]["SPX"])
        before_state = state_path.read_bytes()
        journal_path = journal_dir / f"{SESSION}.jsonl"
        before_journal = journal_path.read_bytes()
        rejected = _commit(
            state_path,
            journal_dir,
            "complete_5m_orb",
            report=report,
        )
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"] == [expected_issue]
        assert state_path.read_bytes() == before_state
        assert journal_path.read_bytes() == before_journal


def test_gamma_acceptance_is_independent_of_orb_decision_sidecar_contract(
    tmp_path: Path,
) -> None:
    decision_fields = (
        "orb_reference_decision_table_present",
        "missing_orb_reference_decision_columns",
        "missing_orb_reference_decision_triggers",
        "missing_orb_reference_decision_indexes",
        "orb_reference_decision_column_count",
        "expected_orb_reference_decision_column_count",
        "orb_reference_progress_decisions",
    )

    gamma_state, gamma_journal = _armed_session(tmp_path / "gamma")
    gamma_report = _report("2026-09-08T13:31:00Z")
    for field in decision_fields:
        gamma_report["database"].pop(field, None)
    gamma = _commit(
        gamma_state,
        gamma_journal,
        "first_eligible_gamma_capture",
        report=gamma_report,
    )
    assert gamma["accepted"] is True, gamma

    orb_state, orb_journal = _armed_session(tmp_path / "orb")
    assert _commit(orb_state, orb_journal, "startup")["accepted"] is True
    orb_report = _report("2026-09-08T13:36:00Z")
    for field in decision_fields:
        orb_report["database"].pop(field, None)
    orb = _commit(
        orb_state,
        orb_journal,
        "complete_5m_orb",
        report=orb_report,
    )
    assert orb["accepted"] is False
    assert orb["issues"] == [
        "database_contract_invalid:orb_reference_decision_table_present"
    ]


def test_scope_binding_and_exclusion_allowlist_fail_closed(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path / "scope")
    mismatched = _report()
    mismatched["acceptance_scope"] = "complete_5m_orb"
    rejected = opening.commit_opening_acceptance_milestone(
        milestone="startup",
        inspector_report=mismatched,
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert rejected["issues"] == ["inspector_acceptance_scope_mismatch"]

    for name, injected in (
        ("unknown-exclusion", "LIVE_GATE_FAILED:stream_progressing"),
        (
            "allowed-prefix-with-secret",
            "GAMMA_PIN_UNAVAILABLE:SPX:DO_NOT_PERSIST_SECRET",
        ),
    ):
        state_path, journal_dir = _armed_session(tmp_path / name)
        assert _commit(state_path, journal_dir, "startup")["accepted"] is True
        report = _report()
        report["out_of_scope_issues"] = [injected]
        rejected = _commit(
            state_path, journal_dir, "complete_5m_orb", report=report
        )
        assert rejected["issues"] == ["inspector_out_of_scope_issue_invalid"]
        assert injected not in (journal_dir / f"{SESSION}.jsonl").read_text("utf-8")

    state_path, journal_dir = _armed_session(tmp_path / "gamma-secret")
    assert _commit(state_path, journal_dir, "startup")["accepted"] is True
    report = _report("2026-09-08T13:31:00Z")
    injected = "RUT_CANARY_STATE_INVALID:DO_NOT_PERSIST_SECRET"
    report["out_of_scope_issues"] = [injected]
    rejected = _commit(
        state_path,
        journal_dir,
        "first_eligible_gamma_capture",
        report=report,
    )
    assert rejected["issues"] == ["inspector_out_of_scope_issue_invalid"]
    assert injected not in (journal_dir / f"{SESSION}.jsonl").read_text("utf-8")


def test_notification_stays_pending_until_durable_delivery_ack(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)

    def fail_after_state(stage: str) -> None:
        if stage == "after_state_replace":
            raise OSError("simulated post-state delivery gap")

    uncertain = _commit(
        state_path,
        journal_dir,
        "startup",
        failpoint=fail_after_state,
    )
    assert uncertain["accepted"] is False
    assert uncertain["action"] == "retry_required"
    retry = _commit(state_path, journal_dir, "startup", report={})
    assert retry["accepted"] is True
    assert retry["action"] == "notification_pending"
    assert retry["notification_required"] is True
    assert retry["pending_notification_event_ids"] == [retry["event_id"]]

    def fail_after_ack(stage: str) -> None:
        if stage == "after_ack_append":
            raise OSError("simulated ack receipt/state gap")

    failed_ack = _ack(
        state_path,
        journal_dir,
        [retry["event_id"]],
        failpoint=fail_after_ack,
    )
    assert failed_ack["accepted"] is False
    assert failed_ack["action"] == "retry_required"
    blocked_milestone_retry = _commit(
        state_path, journal_dir, "startup", report={}
    )
    assert blocked_milestone_retry["accepted"] is False
    assert blocked_milestone_retry["issues"] == [
        "unreflected_opening_receipt_requires_exact_recovery:"
        + failed_ack["ack_event_id"]
    ]
    recovered = _ack(state_path, journal_dir, [retry["event_id"]])
    assert recovered["accepted"] is True, recovered
    assert recovered["action"] == "recovered_and_acknowledged"
    acceptance = json.loads(state_path.read_text("utf-8"))["opening_acceptance"]
    assert acceptance["startup_milestone_notified"] is True
    assert acceptance["pending_notification_event_ids"] == []


def test_rollover_rejects_opening_orphan_and_pending_notification(
    tmp_path: Path,
) -> None:
    orphan_state, orphan_dir = _armed_session(tmp_path / "orphan")

    def interrupt(stage: str) -> None:
        if stage == "after_event_append":
            raise OSError("simulated receipt/state gap")

    orphaned = _commit(
        orphan_state, orphan_dir, "startup", failpoint=interrupt
    )
    assert orphaned["action"] == "retry_required"
    rejected = prepare_monitor_session(
        state_path=orphan_state,
        journal_dir=orphan_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "prior_session_unreflected_opening_milestone:" + orphaned["event_id"]
    ]

    pending_state, pending_dir = _armed_session(tmp_path / "pending")
    committed = _commit(pending_state, pending_dir, "startup")
    assert committed["accepted"] is True
    rejected = prepare_monitor_session(
        state_path=pending_state,
        journal_dir=pending_dir,
        observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-09",
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "prior_session_opening_notification_pending:" + committed["event_id"]
    ]


def test_rollover_runs_full_owner_semantics_for_self_consistent_forgery(
    tmp_path: Path,
) -> None:
    for name in ("degraded-milestone", "delivery-before-milestone"):
        state_path, journal_dir = _armed_session(tmp_path / name)
        milestone_result = _commit(state_path, journal_dir, "startup")
        assert milestone_result["accepted"] is True
        ack_result = _ack(
            state_path, journal_dir, [milestone_result["event_id"]]
        )
        assert ack_result["accepted"] is True

        journal_path = journal_dir / f"{SESSION}.jsonl"
        rows = _journal(journal_path)
        milestone = next(
            row for row in rows if row.get("event_type") == opening.EVENT_TYPE
        )
        ack = next(
            row for row in rows if row.get("event_type") == opening.ACK_EVENT_TYPE
        )
        if name == "degraded-milestone":
            bounded = milestone["evidence"]["bounded_inspector_evidence"]
            bounded["state"] = "degraded"
            bounded["issues"] = ["FORGED_FAILURE"]
            milestone["evidence"]["bounded_inspector_evidence_sha256"] = (
                opening._canonical_hash(bounded)
            )
        else:
            ack["delivery_proof"]["prior_final_delivered_at_utc"] = (
                "2026-09-08T12:00:00Z"
            )

        pre_opening = opening._owned_projection(
            opening._apply_event(opening._reset_projection(SESSION), milestone),
            session_date=SESSION,
        )
        ack["acked_milestone_events"] = [
            {
                "event_id": milestone["event_id"],
                "record_sha256": opening._canonical_hash(milestone),
            }
        ]
        ack["pre_opening_acceptance_sha256"] = opening._canonical_hash(
            pre_opening
        )
        ack["evidence"] = {
            "delivery_proof_sha256": opening._canonical_hash(
                ack["delivery_proof"]
            )
        }
        ack["event_id"] = opening._ack_event_id(ack)
        post_opening = opening._owned_projection(
            opening._apply_ack(pre_opening, ack), session_date=SESSION
        )
        state = json.loads(state_path.read_text("utf-8"))
        state["opening_acceptance"] = post_opening
        _write_json(state_path, state)
        journal_path.write_text(
            "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
            encoding="utf-8",
        )

        state_before = state_path.read_bytes()
        journal_before = journal_path.read_bytes()
        rejected = prepare_monitor_session(
            state_path=state_path,
            journal_dir=journal_dir,
            observed_at_utc=datetime(2026, 9, 9, 12, 45, tzinfo=timezone.utc),
            session_date="2026-09-09",
            dry_run=True,
        )
        assert rejected["accepted"] is False, (name, rejected)
        assert rejected["issues"][0].startswith(
            "prior_session_opening_semantic_replay_failed:"
        )
        expected_detail = (
            "inspector_state_not_ready"
            if name == "degraded-milestone"
            else "opening_ack_delivery_precedes_milestone:"
        )
        assert expected_detail in rejected["issues"][0]
        assert state_path.read_bytes() == state_before
        assert journal_path.read_bytes() == journal_before


def test_rollover_receipt_must_precede_opening_receipts(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    committed = _commit(state_path, journal_dir, "startup")
    assert committed["accepted"] is True
    journal_path = journal_dir / f"{SESSION}.jsonl"
    rows = _journal(journal_path)
    assert [row["event_type"] for row in rows] == [
        "session_rollover",
        opening.EVENT_TYPE,
    ]
    journal_path.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in reversed(rows))
        + "\n",
        encoding="utf-8",
    )
    rejected = _commit(state_path, journal_dir, "startup")
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "opening_receipt_must_follow_session_rollover"
    ]


def test_opening_commit_rejects_unreflected_scan_transaction(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)

    def interrupt(stage: str) -> None:
        if stage == "after_scan_append":
            raise OSError("simulated scan receipt gap")

    scan = _diagnostic_scan()
    failed_scan = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
        failpoint=interrupt,
    )
    assert failed_scan["accepted"] is False
    rejected = _commit(state_path, journal_dir, "startup")
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "unreflected_substantive_scan_requires_exact_retry:" + scan["event_id"]
    ]
    recovered_scan = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert recovered_scan["accepted"] is True, recovered_scan
    accepted = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-08T14:16:00Z"),
    )
    assert accepted["accepted"] is True, accepted


def test_opening_ack_must_follow_latest_journal_record(tmp_path: Path) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    committed = _commit(state_path, journal_dir, "startup")
    assert committed["accepted"] is True
    scan = _diagnostic_scan()
    scan["observed_at_ct"] = "2026-09-08T09:35:00-05:00"
    scan["observed_at_utc"] = "2026-09-08T14:35:00Z"
    scan = with_scan_event_id({key: value for key, value in scan.items() if key != "event_id"})
    scanned = commit_monitor_scan(
        scan=scan,
        policy_inputs={},
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert scanned["accepted"] is True, scanned
    rejected = _ack(
        state_path,
        journal_dir,
        [committed["event_id"]],
        observed_at="2026-09-08T14:34:00Z",
    )
    assert rejected["accepted"] is False
    assert rejected["issues"] == [
        "opening_ack_not_after_latest_journal_record"
    ]


def test_milestone_cannot_backdate_behind_ack_or_reuse_older_delivery_proof(
    tmp_path: Path,
) -> None:
    state_path, journal_dir = _armed_session(tmp_path)
    startup = _commit(
        state_path,
        journal_dir,
        "startup",
        report=_report("2026-09-08T14:30:00Z"),
    )
    assert startup["accepted"] is True, startup
    first_ack = _ack(
        state_path,
        journal_dir,
        [startup["event_id"]],
        observed_at="2026-09-08T14:33:00Z",
        proof=_proof("2026-09-08T14:32:00Z"),
    )
    assert first_ack["accepted"] is True, first_ack

    before_state = state_path.read_bytes()
    journal_path = journal_dir / f"{SESSION}.jsonl"
    before_journal = journal_path.read_bytes()
    backdated = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=_report("2026-09-08T14:31:00Z"),
    )
    assert backdated["accepted"] is False
    assert backdated["issues"] == [
        "opening_milestone_not_after_latest_journal_record"
    ]
    assert state_path.read_bytes() == before_state
    assert journal_path.read_bytes() == before_journal

    current = _commit(
        state_path,
        journal_dir,
        "complete_5m_orb",
        report=_report("2026-09-08T14:34:00Z"),
    )
    assert current["accepted"] is True, current
    reused_old_proof = _ack(
        state_path,
        journal_dir,
        [current["event_id"]],
        observed_at="2026-09-08T14:35:00Z",
        proof=_proof("2026-09-08T14:32:00Z"),
    )
    assert reused_old_proof["accepted"] is False
    assert reused_old_proof["issues"] == [
        "opening_ack_delivery_precedes_milestone:" + current["event_id"]
    ]
