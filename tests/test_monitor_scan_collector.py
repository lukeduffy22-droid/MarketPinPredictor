from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import zlib
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import backend.monitor_session_rollover as session_rollover
from backend.monitor_scan_collector import (
    COLLECTOR_SCHEMA,
    MONITORED_SYMBOLS,
    REQUIRED_PERSISTENCE_TABLES,
    TRANSPORT_COUNTER_FIELDS,
    TRANSPORT_HEALTH_SCHEMA,
    TRANSPORT_SCOPE,
    MonitorScanCollectorError,
    _candidate_observation,
    _database_evidence,
    _phase,
    build_commit_request,
    collect_commit_request,
)
from backend.monitor_policy import evaluate_monitor_policy
from backend.monitor_scan_ledger import (
    COMMIT_RECEIPT_EVENT_TYPE,
    commit_monitor_scan,
    with_policy_input_hashes,
    with_scan_event_id,
)


_CT = ZoneInfo("America/Chicago")


def _endpoint_suppressed_evidence(observed_at_utc: str) -> dict:
    unavailable = {"ok": False, "error": "expected_off_hours_suppression"}
    return {
        "schema_version": COLLECTOR_SCHEMA,
        "observed_at_utc": observed_at_utc,
        "health_first": copy.deepcopy(unavailable),
        "health_second": copy.deepcopy(unavailable),
        "health_overview": copy.deepcopy(unavailable),
        "workstation": {
            symbol: copy.deepcopy(unavailable) for symbol in MONITORED_SYMBOLS
        },
        "gex": {
            symbol: copy.deepcopy(unavailable) for symbol in MONITORED_SYMBOLS
        },
        "orb": copy.deepcopy(unavailable),
        "audits": {
            symbol: {
                "present": True,
                "path": f"C:/audit/{symbol}.json",
                "payload": {},
            }
            for symbol in MONITORED_SYMBOLS
        },
        "database": {
            "present": True,
            "path": "C:/data/market_data.db",
            "journal_mode": "wal",
            "quick_check": "ok",
            "tables_present": list(REQUIRED_PERSISTENCE_TABLES),
            "table_summaries": {},
            "symbols": {
                symbol: {
                    "market_structure": {},
                    "gamma_calculation_run": {},
                    "gamma_calculation_input_blob": {},
                    "prediction_snapshot": {},
                }
                for symbol in MONITORED_SYMBOLS
            },
        },
        "sample_seconds": 5.0,
    }


def _request(observed_at_utc: str, *, database_overrides: dict | None = None) -> dict:
    evidence = _endpoint_suppressed_evidence(observed_at_utc)
    if database_overrides:
        evidence["database"].update(database_overrides)
    return build_commit_request(
        evidence=evidence,
        state={"session_date": "2026-09-08", "mode": "NORMAL"},
        records=[],
        by_id={},
    )


def _eligible_evidence(
    source_time_utc: str,
    *,
    sequence: int,
    aggregate_pipeline_ready: bool = True,
    spx_usable: bool = True,
    transport_counters: dict[str, int] | None = None,
) -> dict:
    source_time = datetime.fromisoformat(source_time_utc.replace("Z", "+00:00"))
    source_ct = source_time.astimezone(_CT).isoformat()
    source_db = source_time.replace(tzinfo=None).isoformat(
        sep=" ", timespec="microseconds"
    )
    generated_time = source_time.replace(microsecond=500_000)
    generated_ct = generated_time.astimezone(_CT).isoformat()
    generated_db = generated_time.replace(tzinfo=None).isoformat(
        sep=" ", timespec="microseconds"
    )
    observed = source_time.replace(microsecond=900_000)
    epoch = "e" * 64
    universe = "a" * 64
    generation = 7
    counters = {field: 0 for field in TRANSPORT_COUNTER_FIELDS}
    counters.update(transport_counters or {})
    symbol_values = {
        "SPX": {
            "spot": 7_000.0,
            "pin": 7_010.0,
            "max_pain": 7_020.0,
            "zero_gamma": 6_990.0,
            "positive_wall": 7_050.0,
            "negative_wall": 6_950.0,
        },
        "NDX": {
            "spot": 29_000.0,
            "pin": 29_100.0,
            "max_pain": 29_050.0,
            "zero_gamma": 28_900.0,
            "positive_wall": 29_200.0,
            "negative_wall": 28_800.0,
        },
    }
    health = {
        "stream_connected": True,
        "stream_progressing": True,
        "transport_ready": True,
        "runtime_context_stable": True,
        # These are aggregate across required symbols and must not override
        # independently healthy symbol_status evidence.
        "collection_ready": aggregate_pipeline_ready,
        "calculation_ready": aggregate_pipeline_ready,
        "prediction_pipeline_ok": aggregate_pipeline_ready,
        "provider": "databento",
        "handoff_status": "active",
        "subscription_epoch_id": epoch,
        "subscription_generation": generation,
        "reconnect_attempts": counters["reconnect_attempts"],
        "last_reconnect_utc": None,
        "last_reconnect_reason": None,
        "messages_received": 1_000 + sequence,
        "symbol_status": {},
    }
    workstation: dict[str, dict] = {}
    gex: dict[str, dict] = {}
    audits: dict[str, dict] = {}
    database_symbols: dict[str, dict] = {}
    for index, symbol in enumerate(MONITORED_SYMBOLS, start=1):
        values = symbol_values[symbol]
        usable = spx_usable if symbol == "SPX" else True
        calculation_id = f"calculation-{symbol.lower()}-{sequence}"
        observation_id = hashlib.sha256(
            f"{symbol}:{sequence}:{source_time_utc}".encode("utf-8")
        ).hexdigest()
        common_identity = {
            "symbol": symbol,
            "provider": "databento",
            "subscription_epoch_id": epoch,
            "subscription_generation": generation,
            "selected_universe_sha256": universe,
            "primary_expiration": "2026-09-08",
            "same_day_profile_available": True,
            "gex_formula_version": "databento-gex-v2-call-minus-put",
            "calculation_id": calculation_id,
            "universe_provenance": {"is_fallback": False},
            "oi_analytics_provenance": {"is_fallback": False},
        }
        health["symbol_status"][symbol] = {
            "usable_for_prediction": usable,
            "is_stale": False,
            "epoch_is_current": True,
            "generation_is_current": True,
            "fresh_quote_count": 100,
        }
        pin = dict(common_identity)
        workstation[symbol] = {
            "ok": True,
            "payload": {
                "symbol": symbol,
                "provider": "databento",
                "status": "live" if usable else "degraded",
                "health": {"validation_is_valid": usable},
                "pin_payload": pin,
                "subscription_epoch_id": epoch,
                "subscription_generation": generation,
            },
        }
        gex_payload = dict(common_identity)
        gex[symbol] = {"ok": True, "payload": gex_payload}
        audit = {
            **common_identity,
            "symbol": symbol,
            "provider": "databento",
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "latest_ts_recv_utc": source_ct,
            "generated_at_utc": generated_ct,
            "spot_last": values["spot"],
            "gamma_pin": values["pin"],
            "max_pain": values["max_pain"],
            "zero_gamma": values["zero_gamma"],
            "gross_gex": 100.0,
            "net_gex": 20.0,
            "call_gex_total": 60.0,
            "put_gex_total": 40.0,
            "positive_gex_wall": values["positive_wall"],
            "negative_gex_wall": values["negative_wall"],
            "max_pain_source": "full-oi-universe",
            "max_pain_formula_version": "full-oi-max-pain-v1",
            "max_pain_as_of": "2026-09-08",
            "pin_is_contested": False,
            "pin_lead_ratio": 0.25,
            "top_strikes_by_abs_gex": [
                {"strike": values["pin"] - 5.0},
                {"strike": values["pin"]},
                {"strike": values["pin"] + 5.0},
            ],
        }
        audits[symbol] = {
            "present": True,
            "path": f"C:/audit/{symbol}-{sequence}.json",
            "payload": audit,
        }
        database_symbols[symbol] = {
            "market_structure": {
                "observation_id": observation_id,
                "symbol": symbol,
                "trading_date": "2026-09-08",
                "source_timestamp_utc": source_db,
                "provider": "databento",
                "subscription_epoch_id": epoch,
                "subscription_generation": generation,
                "universe_sha256": universe,
                "primary_expiration": "2026-09-08",
                "same_day_profile_available": True,
                "calculation_id": calculation_id,
                "reference_price": values["spot"],
                "gamma_pin": values["pin"],
                "max_pain": values["max_pain"],
                "zero_gamma": values["zero_gamma"],
                "gross_gex": 100.0,
                "net_gex": 20.0,
                "validation_status": "valid",
            },
            "gamma_calculation_run": {
                "id": sequence * 10 + index,
                "calculation_id": calculation_id,
                "symbol": symbol,
                "trading_date": "2026-09-08",
                "calculated_at_utc": generated_db,
                "provider": "databento",
                "status": "valid",
                "input_schema_version": "gamma-inputs-v2-point-in-time",
                "subscription_epoch_id": epoch,
                "subscription_generation": generation,
                "universe_sha256": "c" * 64,
                "formula_version": "databento-gex-v2-call-minus-put",
                "spot_price": values["spot"],
                "gamma_pin": values["pin"],
                "max_pain": values["max_pain"],
                "zero_gamma": values["zero_gamma"],
                "gross_gex": 100.0,
                "net_gex": 20.0,
            },
            "gamma_calculation_input_blob": {
                "calculation_run_id": sequence * 10 + index,
                "encoding": "canonical-json+zlib-v1",
                "payload_sha256": "b" * 64,
                "uncompressed_bytes": 1_024,
                "compressed_bytes": 512,
                "payload_integrity_verified": True,
                "payload_lineage_verified": True,
                "validation_reason": None,
            },
            "prediction_snapshot": {
                "symbol": symbol,
                "trading_date": "2026-09-08",
                "timestamp_utc": generated_db,
                "quote_timestamp_utc": source_db,
                "provider": "databento",
                "model_version": "databento_quant_ensemble_v1",
                "model_type": "Databento Quant Ensemble",
                "prediction_mode": "backend_periodic",
                "feature_schema_version": "databento-close-features-2.0",
                "subscription_epoch_id": epoch,
                "subscription_generation": generation,
                "is_valid": 1,
                "validation_status": "valid",
                "current_price": values["spot"],
                "predicted_close": values["spot"]
                * (1.002 if sequence == 1 else 0.998),
                "expected_move_pct": 0.20 if sequence == 1 else -0.20,
                "net_bias": "bullish" if sequence == 1 else "bearish",
                "gamma_pin": values["pin"],
                "max_pain": values["max_pain"],
                "zero_gamma": values["zero_gamma"],
                "gross_gex": 100.0,
                "net_gex": 20.0,
            },
        }
    first_health = copy.deepcopy(health)
    first_health["messages_received"] -= 10
    overview_health = {
        "provider": "databento",
        "subscription_epoch_id": epoch,
        "active_generation": generation,
        **counters,
        "compute_backpressure_remaining_seconds": 0.0,
        "last_reconnect_utc": None,
        "last_provider_warning_utc": None,
    }
    return {
        "schema_version": COLLECTOR_SCHEMA,
        "observed_at_utc": observed.isoformat(),
        "health_first": {"ok": True, "payload": first_health},
        "health_second": {"ok": True, "payload": health},
        "health_overview": {"ok": True, "payload": overview_health},
        "workstation": workstation,
        "gex": gex,
        "orb": {"ok": True, "payload": {"symbols": {}}},
        "audits": audits,
        "database": {
            "present": True,
            "path": "C:/data/market_data.db",
            "journal_mode": "wal",
            "quick_check": "ok",
            "tables_present": list(REQUIRED_PERSISTENCE_TABLES),
            "table_summaries": {},
            "symbols": database_symbols,
        },
        "sample_seconds": 5.0,
    }


def _candidate_from_evidence(evidence: dict, symbol: str = "NDX"):
    observed_at = datetime.fromisoformat(
        evidence["observed_at_utc"].replace("Z", "+00:00")
    )
    return _candidate_observation(
        symbol,
        evidence,
        observed_at=observed_at,
        global_health_issues=[],
    )


def test_exact_cash_close_suppression_is_diagnostic_not_active_data_quality():
    request = _request("2026-09-08T20:00:00+00:00")
    scan = request["scan"]

    assert scan["phase"] == "post-close"
    assert scan["directional_interpretation"] == "ABSTAIN"
    assert request["policy_inputs"] == {}
    assert scan["evidence"]["data_quality_events"] == []
    adaptive = scan["cadence"]["adaptive_evidence"]
    assert adaptive["active_data_quality_event_ids"] == []
    assert adaptive["new_data_quality_event_ids"] == []
    for symbol in MONITORED_SYMBOLS:
        payload = scan["symbols"][symbol]
        assert payload["eligible"] is False
        assert "SESSION_PHASE_NOT_REGULAR" in payload["eligibility_reasons"]
        assert "HEALTH_LIVE_UNAVAILABLE" in payload["eligibility_reasons"]
        assert "WORKSTATION_UNAVAILABLE" in payload["eligibility_reasons"]
        assert "GEX_ENDPOINT_UNAVAILABLE" in payload["eligibility_reasons"]
        assert payload["collector_diagnostic"][
            "phase_suppressed_data_quality_issues"
        ] == payload["eligibility_reasons"]


def test_one_second_before_cash_close_remains_regular_hours_fail_closed():
    request = _request("2026-09-08T19:59:59+00:00")
    scan = request["scan"]

    assert scan["phase"] == "regular-session"
    assert scan["directional_interpretation"] == "ABSTAIN"
    assert request["policy_inputs"] == {}
    events = scan["evidence"]["data_quality_events"]
    assert [event["symbol"] for event in events] == ["SPX", "NDX"]
    assert all(
        event["issues"]
        == [
            "GEX_ENDPOINT_UNAVAILABLE",
            "HEALTH_LIVE_UNAVAILABLE",
            "WORKSTATION_UNAVAILABLE",
        ]
        for event in events
    )
    adaptive = scan["cadence"]["adaptive_evidence"]
    assert len(adaptive["active_data_quality_event_ids"]) == 2
    assert (
        adaptive["new_data_quality_event_ids"]
        == adaptive["active_data_quality_event_ids"]
    )


def test_post_close_database_integrity_failures_remain_active_data_quality():
    request = _request(
        "2026-09-08T20:00:00+00:00",
        database_overrides={"journal_mode": "delete", "quick_check": "corrupt"},
    )
    scan = request["scan"]

    events = scan["evidence"]["data_quality_events"]
    assert [event["symbol"] for event in events] == ["SPX", "NDX"]
    assert all(
        event["issues"] == ["DATABASE_NOT_WAL", "DATABASE_QUICK_CHECK_FAILED"]
        for event in events
    )
    adaptive = scan["cadence"]["adaptive_evidence"]
    assert len(adaptive["active_data_quality_event_ids"]) == 2
    assert (
        adaptive["new_data_quality_event_ids"]
        == adaptive["active_data_quality_event_ids"]
    )


def test_database_read_failure_does_not_invent_exact_row_failures():
    evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    evidence["database"] = {
        "present": True,
        "path": "C:/data/market_data.db",
        "error": "OperationalError:database is locked",
    }

    candidate, issues, _diagnostic = _candidate_from_evidence(evidence)

    assert candidate is None
    assert issues == ["DATABASE_READ_FAILED"]


def test_phase_uses_reviewed_calendar_and_early_close_boundary():
    before_early_close = datetime(
        2026, 11, 27, 17, 59, 59, tzinfo=timezone.utc
    ).astimezone(_CT)
    at_early_close = datetime(
        2026, 11, 27, 18, 0, 0, tzinfo=timezone.utc
    ).astimezone(_CT)
    labor_day = datetime(
        2026, 9, 7, 15, 0, 0, tzinfo=timezone.utc
    ).astimezone(_CT)

    assert _phase(before_early_close) == "regular-session"
    assert _phase(at_early_close) == "post-close"
    assert _phase(labor_day) == "off-hours"


def _receipt_backed_seed(request: dict) -> tuple[list[dict], dict[str, dict]]:
    scan = with_scan_event_id(
        with_policy_input_hashes(request["scan"], request["policy_inputs"])
    )
    receipt = {
        "event_type": COMMIT_RECEIPT_EVENT_TYPE,
        "parent_scan_event_id": scan["event_id"],
    }
    return [scan, receipt], {scan["event_id"]: scan}


def _append_receipt_backed_scan(
    request: dict, records: list[dict], by_id: dict[str, dict]
) -> None:
    scan = with_scan_event_id(
        with_policy_input_hashes(request["scan"], request["policy_inputs"])
    )
    records.extend(
        [
            scan,
            {
                "event_type": COMMIT_RECEIPT_EVENT_TYPE,
                "parent_scan_event_id": scan["event_id"],
            },
        ]
    )
    by_id[scan["event_id"]] = scan


def _set_source_spot(evidence: dict, symbol: str, spot: float) -> None:
    audit = evidence["audits"][symbol]["payload"]
    database = evidence["database"]["symbols"][symbol]
    prediction = database["prediction_snapshot"]
    audit["spot_last"] = spot
    database["market_structure"]["reference_price"] = spot
    database["gamma_calculation_run"]["spot_price"] = spot
    prediction["current_price"] = spot
    prediction["predicted_close"] = spot * (
        1.0 + prediction["expected_move_pct"] / 100.0
    )


def test_receipt_backed_same_session_15m_moves_reach_price_confirmation():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    scans: list[dict] = []
    by_id: dict[str, dict] = {}
    requests: list[dict] = []
    spots = {
        "SPX": (7_000.0, 7_021.0, 7_042.1),
        "NDX": (29_000.0, 29_087.0, 29_174.261),
    }
    for sequence, source_time in enumerate(
        (
            "2026-09-08T14:15:00Z",
            "2026-09-08T14:30:00Z",
            "2026-09-08T14:45:00Z",
        ),
        start=1,
    ):
        evidence = _eligible_evidence(source_time, sequence=sequence)
        for symbol in MONITORED_SYMBOLS:
            _set_source_spot(evidence, symbol, spots[symbol][sequence - 1])
        request = build_commit_request(
            evidence=evidence,
            state=state,
            records=scans,
            by_id=by_id,
        )
        requests.append(request)
        _append_receipt_backed_scan(request, scans, by_id)

    for symbol in MONITORED_SYMBOLS:
        policy_input = requests[-1]["policy_inputs"][symbol]
        previous, current = policy_input["observations"]
        for observation, expected_baseline in (
            (previous, spots[symbol][0]),
            (current, spots[symbol][1]),
        ):
            context = observation["supported_price_changes"]["15m"]
            assert context["schema_version"] == (
                "marketpin-monitor-supported-price-change.v1"
            )
            assert context["actual_interval_seconds"] == 900.0
            assert context["baseline_spot"] == expected_baseline
            assert len(context["baseline_scan_event_id"]) == 64
            assert context["pct"] == pytest.approx(
                (context["current_spot"] - expected_baseline)
                / expected_baseline
                * 100.0
            )
        result = evaluate_monitor_policy(policy_input)
        assert result["accepted"] is True
        assert {
            (row["type"], row["direction"])
            for row in result["confirmations"]["price"]
        } >= {("persistent_15m_price_move", "bullish")}


def test_price_change_does_not_relabel_five_or_twenty_minute_history_as_15m():
    state = {"session_date": "2026-09-08", "mode": "ELEVATED"}
    seed = build_commit_request(
        evidence=_eligible_evidence("2026-09-08T14:30:00Z", sequence=1),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)

    for source_time, sequence in (
        ("2026-09-08T14:35:00Z", 2),
        ("2026-09-08T14:50:00Z", 3),
    ):
        request = build_commit_request(
            evidence=_eligible_evidence(source_time, sequence=sequence),
            state=state,
            records=records,
            by_id=by_id,
        )
        for symbol in MONITORED_SYMBOLS:
            candidate = request["scan"]["symbols"][symbol][
                "candidate_policy_observation"
            ]
            assert "supported_price_changes" not in candidate


def test_transport_health_first_nonzero_snapshot_seeds_without_deterioration():
    counters = {
        "reconnect_attempts": 2,
        "provider_queue_full_warnings": 4,
        "provider_slow_client_warnings": 3,
        "provider_skipped_record_warnings": 1,
        "provider_skipped_records": 42,
        "provider_pending_records_peak": 5_309,
    }
    request = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters=counters,
        ),
        state={"session_date": "2026-09-08", "mode": "NORMAL"},
        records=[],
        by_id={},
    )

    transport = request["scan"]["evidence"]["transport_health"]
    assert transport["schema_version"] == TRANSPORT_HEALTH_SCHEMA
    assert transport["scope"] == TRANSPORT_SCOPE
    assert transport["evaluation_status"] == "seeded"
    assert transport["baseline_status"] == "no_prior_receipt"
    assert transport["current_counters"] == counters
    assert transport["counter_deltas"] == {
        field: None for field in TRANSPORT_COUNTER_FIELDS
    }
    assert request["scan"]["evidence"]["data_quality_events"] == []
    assert request["scan"]["cadence"]["adaptive_evidence"][
        "new_data_quality_event_ids"
    ] == []


def test_transport_health_static_nonzero_same_epoch_is_quiet():
    counters = {
        "reconnect_attempts": 2,
        "provider_queue_full_warnings": 4,
        "provider_slow_client_warnings": 3,
        "provider_skipped_record_warnings": 1,
        "provider_skipped_records": 42,
        "provider_pending_records_peak": 5_309,
    }
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters=counters,
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    current = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:45:00Z",
            sequence=2,
            transport_counters=counters,
        ),
        state=state,
        records=records,
        by_id=by_id,
    )

    transport = current["scan"]["evidence"]["transport_health"]
    assert transport["evaluation_status"] == "compared"
    assert transport["baseline_status"] == "same_epoch_receipt"
    assert transport["baseline_scan_event_id"] == records[0]["event_id"]
    assert transport["baseline_counters"] == counters
    assert transport["counter_deltas"] == {
        field: 0 for field in TRANSPORT_COUNTER_FIELDS
    }
    assert current["scan"]["evidence"]["data_quality_events"] == []
    assert all(
        current["scan"]["symbols"][symbol]["eligible"] is True
        for symbol in MONITORED_SYMBOLS
    )


def test_transport_first_scan_catches_reconnect_during_acquisition():
    evidence = _eligible_evidence("2026-09-08T14:30:00Z", sequence=1)
    evidence["health_second"]["payload"].update(
        reconnect_attempts=1,
        subscription_generation=8,
    )
    evidence["health_overview"]["payload"].update(
        reconnect_attempts=1,
        active_generation=8,
    )
    request = build_commit_request(
        evidence=evidence,
        state={"session_date": "2026-09-08", "mode": "NORMAL"},
        records=[],
        by_id={},
    )

    transport = request["scan"]["evidence"]["transport_health"]
    assert transport["evaluation_status"] == "seeded"
    assert transport["acquisition_reconnect"] == {
        "first": 0,
        "second": 1,
        "overview": 1,
        "delta": 1,
    }
    global_events = [
        event
        for event in request["scan"]["evidence"]["data_quality_events"]
        if event.get("scope") == TRANSPORT_SCOPE
    ]
    assert len(global_events) == 1
    assert global_events[0]["baseline_scan_event_id"] is None
    assert global_events[0]["counter_transitions"] == {
        "reconnect_attempts": {"previous": 0, "current": 1, "delta": 1}
    }


@pytest.mark.parametrize(
    ("increments", "expected_issues"),
    (
        (
            {"reconnect_attempts": 1},
            ["TRANSPORT_RECONNECT_ATTEMPTS_INCREASED"],
        ),
        (
            {"provider_queue_full_warnings": 1},
            ["TRANSPORT_QUEUE_FULL_WARNINGS_INCREASED"],
        ),
        (
            {"provider_slow_client_warnings": 1},
            ["TRANSPORT_SLOW_CLIENT_WARNINGS_INCREASED"],
        ),
        (
            {"provider_skipped_record_warnings": 1},
            ["TRANSPORT_SKIPPED_RECORD_WARNINGS_INCREASED"],
        ),
        (
            {"provider_skipped_records": 7},
            ["TRANSPORT_SKIPPED_RECORDS_INCREASED"],
        ),
        (
            {
                "provider_slow_client_warnings": 1,
                "provider_skipped_record_warnings": 1,
                "provider_skipped_records": 42,
            },
            [
                "TRANSPORT_SKIPPED_RECORDS_INCREASED",
                "TRANSPORT_SKIPPED_RECORD_WARNINGS_INCREASED",
                "TRANSPORT_SLOW_CLIENT_WARNINGS_INCREASED",
            ],
        ),
    ),
)
def test_transport_counter_deltas_create_one_global_incident(
    increments: dict[str, int], expected_issues: list[str]
):
    baseline = {
        "reconnect_attempts": 2,
        "provider_queue_full_warnings": 4,
        "provider_slow_client_warnings": 3,
        "provider_skipped_record_warnings": 1,
        "provider_skipped_records": 42,
        "provider_pending_records_peak": 5_309,
    }
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters=baseline,
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    observed = dict(baseline)
    for field, increment in increments.items():
        observed[field] += increment
    evidence = _eligible_evidence(
        "2026-09-08T14:45:00Z",
        sequence=2,
        transport_counters=observed,
    )
    current = build_commit_request(
        evidence=evidence,
        state=state,
        records=records,
        by_id=by_id,
    )
    replay = build_commit_request(
        evidence=copy.deepcopy(evidence),
        state=state,
        records=records,
        by_id=by_id,
    )

    events = current["scan"]["evidence"]["data_quality_events"]
    assert len(events) == 1
    event = events[0]
    assert event["scope"] == TRANSPORT_SCOPE
    assert "symbol" not in event
    assert event["issues"] == expected_issues
    assert set(event["counter_transitions"]) == set(increments)
    for field, increment in increments.items():
        assert event["counter_transitions"][field] == {
            "previous": baseline[field],
            "current": observed[field],
            "delta": increment,
        }
    adaptive = current["scan"]["cadence"]["adaptive_evidence"]
    assert adaptive["active_data_quality_event_ids"] == [event["event_id"]]
    assert adaptive["new_data_quality_event_ids"] == [event["event_id"]]
    assert replay["scan"]["evidence"]["data_quality_events"] == events
    assert all(
        current["scan"]["symbols"][symbol]["eligible"] is True
        for symbol in MONITORED_SYMBOLS
    )


def test_transport_epoch_change_seeds_without_cross_epoch_delta():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters={
                "reconnect_attempts": 8,
                "provider_queue_full_warnings": 12,
            },
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    current_evidence = _eligible_evidence(
        "2026-09-08T14:45:00Z",
        sequence=2,
        transport_counters={
            "reconnect_attempts": 1,
            "provider_queue_full_warnings": 2,
        },
    )
    replacement_epoch = "f" * 64
    for endpoint in ("health_first", "health_second", "health_overview"):
        current_evidence[endpoint]["payload"][
            "subscription_epoch_id"
        ] = replacement_epoch
    current = build_commit_request(
        evidence=current_evidence,
        state=state,
        records=records,
        by_id=by_id,
    )

    transport = current["scan"]["evidence"]["transport_health"]
    assert transport["evaluation_status"] == "seeded"
    assert transport["baseline_status"] == "subscription_epoch_changed"
    assert transport["baseline_scan_event_id"] is None
    assert all(value is None for value in transport["counter_deltas"].values())
    assert not any(
        event.get("scope") == TRANSPORT_SCOPE
        for event in current["scan"]["evidence"]["data_quality_events"]
    )


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        (
            lambda evidence: evidence["health_overview"]["payload"].update(
                provider_queue_full_warnings="4"
            ),
            "transport_health_overview_provider_queue_full_warnings_must_be_nonnegative_integer",
        ),
        (
            lambda evidence: evidence["health_overview"]["payload"].update(
                subscription_epoch_id="f" * 64
            ),
            "transport_health_epoch_mismatch_during_acquisition",
        ),
        (
            lambda evidence: evidence["health_overview"]["payload"].update(
                active_generation=8
            ),
            "transport_health_generation_changed_without_reconnect",
        ),
        (
            lambda evidence: (
                evidence["health_first"]["payload"].update(reconnect_attempts=2),
                evidence["health_second"]["payload"].update(reconnect_attempts=1),
                evidence["health_overview"]["payload"].update(
                    reconnect_attempts=2
                ),
            ),
            "transport_health_reconnect_attempts_regressed_during_acquisition",
        ),
    ),
)
def test_transport_health_rejects_malformed_or_mismatched_current_evidence(
    mutation, expected_error: str
):
    evidence = _eligible_evidence("2026-09-08T14:30:00Z", sequence=1)
    mutation(evidence)

    with pytest.raises(MonitorScanCollectorError, match=expected_error):
        build_commit_request(
            evidence=evidence,
            state={"session_date": "2026-09-08", "mode": "NORMAL"},
            records=[],
            by_id={},
        )


def test_transport_health_rejects_same_epoch_counter_regression():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters={"provider_queue_full_warnings": 4},
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)

    with pytest.raises(
        MonitorScanCollectorError,
        match="transport_health_provider_queue_full_warnings_regressed_within_epoch",
    ):
        build_commit_request(
            evidence=_eligible_evidence(
                "2026-09-08T14:45:00Z",
                sequence=2,
                transport_counters={"provider_queue_full_warnings": 3},
            ),
            state=state,
            records=records,
            by_id=by_id,
        )


def test_transport_baseline_ignores_unreceipted_orphan_scan():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            transport_counters={"provider_queue_full_warnings": 2},
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    orphan_request = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:40:00Z",
            sequence=9,
            transport_counters={"provider_queue_full_warnings": 100},
        ),
        state=state,
        records=[],
        by_id={},
    )
    orphan = with_scan_event_id(
        with_policy_input_hashes(
            orphan_request["scan"], orphan_request["policy_inputs"]
        )
    )
    records.append(orphan)
    by_id[orphan["event_id"]] = orphan

    current = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:45:00Z",
            sequence=2,
            transport_counters={"provider_queue_full_warnings": 3},
        ),
        state=state,
        records=records,
        by_id=by_id,
    )
    transport = current["scan"]["evidence"]["transport_health"]
    assert transport["baseline_scan_event_id"] == records[0]["event_id"]
    assert transport["counter_deltas"]["provider_queue_full_warnings"] == 1
    event = current["scan"]["evidence"]["data_quality_events"][0]
    assert event["scope"] == TRANSPORT_SCOPE
    assert event["counter_transitions"]["provider_queue_full_warnings"] == {
        "previous": 2,
        "current": 3,
        "delta": 1,
    }


def test_future_expiration_profiles_are_bounded_context_only_and_policy_inert():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    evidence = _eligible_evidence("2026-09-08T14:30:00Z", sequence=1)
    for symbol in MONITORED_SYMBOLS:
        evidence["audits"][symbol]["payload"]["expiration_profiles"] = [
            {
                "expiration": "2026-09-08",
                "pin": 1.0,
                "max_pain": 1.0,
                "max_pain_source": "full-oi-universe",
                "max_pain_as_of": "2026-09-08",
            },
            {
                "expiration": "2026-09-11",
                "pin": 29_200.0 if symbol == "NDX" else 6_980.0,
                "max_pain": 29_400.0 if symbol == "NDX" else 7_000.0,
                "max_pain_source": "full-oi-universe",
                "max_pain_as_of": "2026-09-08",
            },
        ]
    baseline_evidence = copy.deepcopy(evidence)
    for symbol in MONITORED_SYMBOLS:
        baseline_evidence["audits"][symbol]["payload"].pop(
            "expiration_profiles"
        )

    request = build_commit_request(
        evidence=evidence, state=state, records=[], by_id={}
    )
    baseline = build_commit_request(
        evidence=baseline_evidence, state=state, records=[], by_id={}
    )

    assert request["policy_inputs"] == baseline["policy_inputs"] == {}
    assert (
        request["scan"]["cadence"]["adaptive_evidence"]
        == baseline["scan"]["cadence"]["adaptive_evidence"]
    )
    for symbol in MONITORED_SYMBOLS:
        payload = request["scan"]["symbols"][symbol]
        baseline_payload = baseline["scan"]["symbols"][symbol]
        assert payload["eligible"] == baseline_payload["eligible"] is False
        assert payload["eligibility_reasons"] == baseline_payload[
            "eligibility_reasons"
        ]
        assert payload["candidate_policy_observation"] == baseline_payload[
            "candidate_policy_observation"
        ]
        assert "future_expiration_profiles" not in payload[
            "candidate_policy_observation"
        ]
        assert payload["future_expiration_profiles"] == [
            {
                "expiration": "2026-09-11",
                "days_to_expiration": 3,
                "context_only": True,
                "role": "future_expiration_context_only",
                "source": "current_eligible_databento_audit.expiration_profiles",
                "source_timestamp_utc": "2026-09-08T14:30:00+00:00",
                "calculation_id": f"calculation-{symbol.lower()}-1",
                "subscription_epoch_id": "e" * 64,
                "subscription_generation": 7,
                "gex_formula_version": "databento-gex-v2-call-minus-put",
                "universe_sha256": "a" * 64,
                "universe_is_fallback": False,
                "gamma_pin": 29_200.0 if symbol == "NDX" else 6_980.0,
                "max_pain": 29_400.0 if symbol == "NDX" else 7_000.0,
                "max_pain_source": "full-oi-universe",
                "max_pain_as_of": "2026-09-08",
                "max_pain_formula_version": "full-oi-max-pain-v1",
            }
        ]
        diagnostic = payload["collector_diagnostic"][
            "future_expiration_context"
        ]
        assert diagnostic["status"] == "available"
        assert diagnostic["excluded_primary_count"] == 1
        assert diagnostic["abstained"] is False


def test_malformed_shadow_rows_are_excluded_without_weakening_live_candidate():
    evidence = _eligible_evidence("2026-09-08T14:30:00Z", sequence=1)
    for symbol in MONITORED_SYMBOLS:
        evidence["audits"][symbol]["payload"]["expiration_profiles"] = [
            {
                "expiration": "2026-09-08",
                "pin": 1.0,
                "max_pain": 1.0,
            },
            {"expiration": "20260911", "pin": 7_100.0},
            {"expiration": "2026-09-11", "pin": "NaN"},
            {
                "expiration": "2026-09-12",
                "max_pain": 7_100.0,
                "max_pain_source": ["borrowed-fallback"],
                "max_pain_as_of": "2026-09-07",
            },
            "not-an-object",
        ]

    request = build_commit_request(
        evidence=evidence,
        state={"session_date": "2026-09-08", "mode": "NORMAL"},
        records=[],
        by_id={},
    )

    assert request["policy_inputs"] == {}
    assert request["scan"]["evidence"]["data_quality_events"] == []
    for symbol in MONITORED_SYMBOLS:
        payload = request["scan"]["symbols"][symbol]
        assert payload["candidate_policy_observation"]["eligible"] is True
        assert payload["eligible"] is False
        assert payload["eligibility_reasons"] == ["confirmation_history_seeding"]
        assert payload["future_expiration_profiles"] == []
        diagnostic = payload["collector_diagnostic"][
            "future_expiration_context"
        ]
        assert diagnostic["status"] == "unavailable"
        assert diagnostic["abstained"] is True
        assert diagnostic["excluded_primary_count"] == 1
        assert diagnostic["rejected_count"] == 4
        assert set(diagnostic["rejection_reasons"]) == {
            "expiration_not_canonical",
            "max_pain_provenance_mismatch",
            "profile_not_an_object",
            "profile_price_not_positive_finite",
        }


def test_naive_sqlite_utc_timestamp_seeds_then_becomes_policy_eligible():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
        ),
        state=state,
        records=[],
        by_id={},
    )

    assert seed["policy_inputs"] == {}
    assert seed["scan"]["evidence"]["data_quality_events"] == []
    for symbol in MONITORED_SYMBOLS:
        payload = seed["scan"]["symbols"][symbol]
        assert payload["eligible"] is False
        assert payload["eligibility_reasons"] == ["confirmation_history_seeding"]
        assert payload["candidate_policy_observation"]["source_timestamp_utc"] == (
            "2026-09-08T14:30:00+00:00"
        )

    records, by_id = _receipt_backed_seed(seed)
    confirmed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:45:00Z",
            sequence=2,
        ),
        state=state,
        records=records,
        by_id=by_id,
    )

    assert set(confirmed["policy_inputs"]) == set(MONITORED_SYMBOLS)
    assert confirmed["scan"]["directional_interpretation"] == "POLICY_CONTROLLED"
    for symbol in MONITORED_SYMBOLS:
        payload = confirmed["scan"]["symbols"][symbol]
        assert payload["eligible"] is True
        assert payload["eligibility_reasons"] == []
        observation = payload["policy_observation"]
        assert observation["forecast_bias"] == "bearish"
        assert observation["expected_move_pct"] == -0.20
        assert observation["prediction_quote_timestamp_utc"] == (
            "2026-09-08T14:45:00+00:00"
        )
        assert payload["collector_diagnostic"]["prediction_context_status"] == (
            "aligned"
        )


@pytest.mark.parametrize(
    ("mode", "seed_time", "confirmation_time", "expected_gap_seconds"),
    (
        ("NORMAL", "2026-09-08T14:24:00Z", "2026-09-08T14:31:00Z", 420),
        ("ELEVATED", "2026-09-08T14:30:00Z", "2026-09-08T14:38:00Z", 480),
    ),
)
def test_confirmation_window_matches_policy_and_ledger_for_catch_up_wakes(
    mode: str,
    seed_time: str,
    confirmation_time: str,
    expected_gap_seconds: int,
):
    """A valid pair must not be mislabeled as an unnecessary history reseed."""

    state = {"session_date": "2026-09-08", "mode": mode}
    seed = build_commit_request(
        evidence=_eligible_evidence(seed_time, sequence=1),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)

    confirmed = build_commit_request(
        evidence=_eligible_evidence(confirmation_time, sequence=2),
        state=state,
        records=records,
        by_id=by_id,
    )

    assert set(confirmed["policy_inputs"]) == set(MONITORED_SYMBOLS)
    assert confirmed["scan"]["directional_interpretation"] == "POLICY_CONTROLLED"
    for symbol in MONITORED_SYMBOLS:
        payload = confirmed["scan"]["symbols"][symbol]
        assert payload["eligible"] is True
        assert payload["eligibility_reasons"] == []
        observations = confirmed["policy_inputs"][symbol]["observations"]
        previous_time = datetime.fromisoformat(observations[0]["observed_at_utc"])
        current_time = datetime.fromisoformat(observations[1]["observed_at_utc"])
        assert (current_time - previous_time).total_seconds() == expected_gap_seconds


def test_confirmation_window_still_reseeds_below_four_minutes():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence("2026-09-08T14:30:00Z", sequence=1),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)

    too_soon = build_commit_request(
        evidence=_eligible_evidence("2026-09-08T14:33:59Z", sequence=2),
        state=state,
        records=records,
        by_id=by_id,
    )

    assert too_soon["policy_inputs"] == {}
    for symbol in MONITORED_SYMBOLS:
        payload = too_soon["scan"]["symbols"][symbol]
        assert payload["eligible"] is False
        assert payload["eligibility_reasons"] == [
            "PRIOR_SCHEDULED_SCAN_GAP_INVALID",
            "confirmation_history_seeding",
        ]


def test_aggregate_pipeline_failure_does_not_cross_contaminate_healthy_ndx():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:30:00Z",
            sequence=1,
            aggregate_pipeline_ready=False,
            spx_usable=False,
        ),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    confirmed = build_commit_request(
        evidence=_eligible_evidence(
            "2026-09-08T14:45:00Z",
            sequence=2,
            aggregate_pipeline_ready=False,
            spx_usable=False,
        ),
        state=state,
        records=records,
        by_id=by_id,
    )

    assert confirmed["scan"]["symbols"]["SPX"]["eligible"] is False
    assert confirmed["scan"]["symbols"]["NDX"]["eligible"] is True
    assert set(confirmed["policy_inputs"]) == {"NDX"}
    assert not any(
        reason.startswith("HEALTH_CALCULATION_READY")
        or reason.startswith("HEALTH_PREDICTION_PIPELINE_OK")
        or reason.startswith("HEALTH_COLLECTION_READY")
        for reason in confirmed["scan"]["symbols"]["NDX"]["eligibility_reasons"]
    )


def test_prediction_context_rejects_future_and_bias_sign_mismatches():
    cases = (
        (
            "future_timestamp",
            "2026-09-08T14:45:00Z",
            "2026-09-08T14:45:00.900000+00:00",
            {"timestamp_utc": "2026-09-08 14:45:05.000000"},
            "2026-09-08",
        ),
        (
            "bias_sign_disagreement",
            "2026-09-08T14:45:00Z",
            "2026-09-08T14:45:00.900000+00:00",
            {"net_bias": "bullish", "expected_move_pct": -0.20},
            "2026-09-08",
        ),
    )

    for case, source_time, observed_at, overrides, session_date in cases:
        evidence = _eligible_evidence(source_time, sequence=2)
        evidence["observed_at_utc"] = observed_at
        for symbol in MONITORED_SYMBOLS:
            prediction = evidence["database"]["symbols"][symbol][
                "prediction_snapshot"
            ]
            prediction.update(overrides)

        request = build_commit_request(
            evidence=evidence,
            state={"session_date": session_date, "mode": "NORMAL"},
            records=[],
            by_id={},
        )

        for symbol in MONITORED_SYMBOLS:
            payload = request["scan"]["symbols"][symbol]
            observation = payload["candidate_policy_observation"]
            assert "forecast_bias" not in observation, case
            assert "expected_move_pct" not in observation, case
            assert payload["collector_diagnostic"]["prediction_context_status"] == (
                "unavailable_or_misaligned"
            ), case


def test_core_source_rejects_prior_ct_session_at_midnight_boundary():
    evidence = _eligible_evidence("2026-09-08T04:59:59Z", sequence=2)
    observed_at = datetime.fromisoformat("2026-09-08T05:00:00+00:00")

    for symbol in MONITORED_SYMBOLS:
        candidate, issues, diagnostic = _candidate_observation(
            symbol,
            evidence,
            observed_at=observed_at,
            global_health_issues=[],
        )

        assert candidate is None
        assert issues == [
            "AUDIT_SESSION_DATE_MISMATCH",
            "SOURCE_SESSION_DATE_MISMATCH",
        ]
        assert diagnostic["source_age_seconds"] == 1.0


def test_provenance_mismatch_candidate_is_persisted_as_confirmation_seed():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence("2026-09-08T14:30:00Z", sequence=1),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    current = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    replacement_epoch = "f" * 64
    current["health_first"]["payload"]["subscription_epoch_id"] = replacement_epoch
    current["health_second"]["payload"]["subscription_epoch_id"] = replacement_epoch
    current["health_overview"]["payload"][
        "subscription_epoch_id"
    ] = replacement_epoch
    for symbol in MONITORED_SYMBOLS:
        for health_sample in ("health_first", "health_second"):
            current[health_sample]["payload"]["symbol_status"][symbol][
                "epoch_is_current"
            ] = True
        current["workstation"][symbol]["payload"][
            "subscription_epoch_id"
        ] = replacement_epoch
        current["workstation"][symbol]["payload"]["pin_payload"][
            "subscription_epoch_id"
        ] = replacement_epoch
        current["gex"][symbol]["payload"][
            "subscription_epoch_id"
        ] = replacement_epoch
        current["audits"][symbol]["payload"][
            "subscription_epoch_id"
        ] = replacement_epoch
        database_symbol = current["database"]["symbols"][symbol]
        database_symbol["market_structure"][
            "subscription_epoch_id"
        ] = replacement_epoch
        database_symbol["gamma_calculation_run"][
            "subscription_epoch_id"
        ] = replacement_epoch
        database_symbol["prediction_snapshot"][
            "subscription_epoch_id"
        ] = replacement_epoch

    request = build_commit_request(
        evidence=current,
        state=state,
        records=records,
        by_id=by_id,
    )

    assert request["policy_inputs"] == {}
    for symbol in MONITORED_SYMBOLS:
        payload = request["scan"]["symbols"][symbol]
        assert payload["eligible"] is False
        assert payload["eligibility_reasons"] == [
            "PRIOR_SCHEDULED_PROVENANCE_MISMATCH",
            "confirmation_history_seeding",
        ]
        assert "candidate_policy_observation" in payload


def test_same_calculation_candidate_is_persisted_as_confirmation_seed():
    state = {"session_date": "2026-09-08", "mode": "NORMAL"}
    seed = build_commit_request(
        evidence=_eligible_evidence("2026-09-08T14:30:00Z", sequence=1),
        state=state,
        records=[],
        by_id={},
    )
    records, by_id = _receipt_backed_seed(seed)
    current = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    for symbol in MONITORED_SYMBOLS:
        calculation_id = f"calculation-{symbol.lower()}-1"
        current["workstation"][symbol]["payload"]["pin_payload"][
            "calculation_id"
        ] = calculation_id
        current["gex"][symbol]["payload"]["calculation_id"] = calculation_id
        current["audits"][symbol]["payload"]["calculation_id"] = calculation_id
        database_symbol = current["database"]["symbols"][symbol]
        database_symbol["market_structure"]["calculation_id"] = calculation_id
        database_symbol["gamma_calculation_run"]["calculation_id"] = calculation_id

    request = build_commit_request(
        evidence=current,
        state=state,
        records=records,
        by_id=by_id,
    )

    assert request["policy_inputs"] == {}
    for symbol in MONITORED_SYMBOLS:
        payload = request["scan"]["symbols"][symbol]
        assert payload["eligible"] is False
        assert payload["eligibility_reasons"] == [
            "PRIOR_SCHEDULED_OBSERVATION_NOT_ADVANCING",
            "confirmation_history_seeding",
        ]
        assert "candidate_policy_observation" in payload


def test_every_mandated_provenance_layer_must_be_present_and_non_fallback():
    for authority in ("audit", "pin", "gex"):
        for field in ("universe_provenance", "oi_analytics_provenance"):
            evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
            if authority == "audit":
                payload = evidence["audits"]["NDX"]["payload"]
            elif authority == "pin":
                payload = evidence["workstation"]["NDX"]["payload"]["pin_payload"]
            else:
                payload = evidence["gex"]["NDX"]["payload"]
            payload.pop(field)

            candidate, issues, diagnostic = _candidate_from_evidence(evidence)

            assert candidate is None, (authority, field)
            assert "FALLBACK_PROVENANCE" in issues, (authority, field)
            assert f"{authority}.{field}" in diagnostic["invalid_provenance_layers"]

    for invalid_layer in ({}, {"is_fallback": True}, "not-a-mapping"):
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        evidence["audits"]["NDX"]["payload"]["universe_provenance"] = (
            invalid_layer
        )
        candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
        assert candidate is None
        assert "FALLBACK_PROVENANCE" in issues


def test_primary_expiration_max_pain_and_profile_are_current_session_bound():
    primary_targets = ("pin", "gex", "audit", "structure")
    for target in primary_targets:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        if target == "pin":
            payload = evidence["workstation"]["NDX"]["payload"]["pin_payload"]
        elif target == "gex":
            payload = evidence["gex"]["NDX"]["payload"]
        elif target == "audit":
            payload = evidence["audits"]["NDX"]["payload"]
        else:
            payload = evidence["database"]["symbols"]["NDX"]["market_structure"]
        payload["primary_expiration"] = "2026-09-09"
        candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
        assert candidate is None, target
        assert "PRIMARY_EXPIRATION_ALIGNMENT_FAILED" in issues, target

    evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    evidence["workstation"]["NDX"]["payload"]["pin_payload"][
        "primary_expiration"
    ] = "2026-09-09"
    evidence["gex"]["NDX"]["payload"]["primary_expiration"] = "2026-09-09"
    evidence["audits"]["NDX"]["payload"]["primary_expiration"] = "2026-09-09"
    evidence["database"]["symbols"]["NDX"]["market_structure"][
        "primary_expiration"
    ] = "2026-09-09"
    candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
    assert candidate is None
    assert "PRIMARY_EXPIRATION_NOT_CURRENT_SESSION" in issues

    for target in primary_targets:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        if target == "pin":
            payload = evidence["workstation"]["NDX"]["payload"]["pin_payload"]
        elif target == "gex":
            payload = evidence["gex"]["NDX"]["payload"]
        elif target == "audit":
            payload = evidence["audits"]["NDX"]["payload"]
        else:
            payload = evidence["database"]["symbols"]["NDX"]["market_structure"]
        payload["same_day_profile_available"] = False
        candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
        assert candidate is None, target
        assert "SAME_DAY_PROFILE_UNAVAILABLE" in issues, target

    evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    evidence["audits"]["NDX"]["payload"]["max_pain_as_of"] = "2026-09-07"
    candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
    assert candidate is None
    assert "MAX_PAIN_AS_OF_NOT_CURRENT_SESSION" in issues


def test_core_symbol_persistence_authorities_and_input_proof_fail_closed():
    cases = (
        ("audit", "symbol", "SPX", "AUDIT_SYMBOL_MISMATCH"),
        (
            "structure",
            "symbol",
            "SPX",
            "MARKET_STRUCTURE_SYMBOL_MISMATCH",
        ),
        (
            "structure",
            "trading_date",
            "2026-09-07",
            "MARKET_STRUCTURE_SESSION_DATE_MISMATCH",
        ),
        (
            "structure",
            "provider",
            "polygon",
            "MARKET_STRUCTURE_PROVIDER_NOT_DATABENTO",
        ),
        (
            "structure",
            "validation_status",
            "invalid",
            "MARKET_STRUCTURE_VALIDATION_FAILED",
        ),
        ("gamma_run", "symbol", "SPX", "GAMMA_RUN_SYMBOL_MISMATCH"),
        (
            "gamma_run",
            "trading_date",
            "2026-09-07",
            "GAMMA_RUN_SESSION_DATE_MISMATCH",
        ),
        (
            "gamma_run",
            "provider",
            "polygon",
            "GAMMA_RUN_PROVIDER_NOT_DATABENTO",
        ),
        ("gamma_run", "status", "invalid", "GAMMA_RUN_STATUS_INVALID"),
        (
            "input_blob",
            "payload_integrity_verified",
            False,
            "GAMMA_INPUT_BLOB_INVALID",
        ),
        (
            "input_blob",
            "payload_lineage_verified",
            False,
            "GAMMA_INPUT_BLOB_INVALID",
        ),
    )
    for target, field, value, expected_issue in cases:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        if target == "audit":
            payload = evidence["audits"]["NDX"]["payload"]
        else:
            payload = evidence["database"]["symbols"]["NDX"][
                {
                    "structure": "market_structure",
                    "gamma_run": "gamma_calculation_run",
                    "input_blob": "gamma_calculation_input_blob",
                }[target]
            ]
        payload[field] = value
        candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
        assert candidate is None, (target, field)
        assert expected_issue in issues, (target, field)


def test_live_http_authority_symbol_and_provider_identity_fail_closed():
    cases = (
        ("workstation", "symbol", "SPX", "WORKSTATION_SYMBOL_MISMATCH"),
        (
            "workstation",
            "provider",
            "polygon",
            "WORKSTATION_PROVIDER_NOT_DATABENTO",
        ),
        ("pin", "symbol", "SPX", "PIN_PAYLOAD_SYMBOL_MISMATCH"),
        (
            "pin",
            "provider",
            "polygon",
            "PIN_PAYLOAD_PROVIDER_NOT_DATABENTO",
        ),
        ("gex", "symbol", "SPX", "GEX_ENDPOINT_SYMBOL_MISMATCH"),
        (
            "gex",
            "provider",
            "polygon",
            "GEX_ENDPOINT_PROVIDER_NOT_DATABENTO",
        ),
    )
    for target, field, value, expected_issue in cases:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        if target == "workstation":
            payload = evidence["workstation"]["NDX"]["payload"]
        elif target == "pin":
            payload = evidence["workstation"]["NDX"]["payload"]["pin_payload"]
        else:
            payload = evidence["gex"]["NDX"]["payload"]
        payload[field] = value
        candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
        assert candidate is None, (target, field)
        assert expected_issue in issues, (target, field)


def test_prediction_context_requires_canonical_identity_and_symbol():
    cases = (
        ("symbol", "SPX", "symbol_mismatch"),
        ("trading_date", "2026-09-07", "trading_date_mismatch"),
        ("prediction_mode", "balanced", "prediction_mode_not_canonical"),
        ("model_version", "fallback-1.0", "model_version_not_canonical"),
        ("model_type", "pin-payload-fallback", "model_type_not_canonical"),
        (
            "feature_schema_version",
            "legacy",
            "feature_schema_version_not_canonical",
        ),
    )
    for field, value, expected_reason in cases:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        evidence["database"]["symbols"]["NDX"]["prediction_snapshot"][field] = (
            value
        )
        candidate, issues, diagnostic = _candidate_from_evidence(evidence)
        assert issues == [], field
        assert candidate is not None, field
        assert "forecast_bias" not in candidate, field
        assert expected_reason in diagnostic["prediction_context_reasons"], field


def test_prediction_prices_and_expected_move_must_be_positive_and_consistent():
    cases = (
        ("current_price", -1.0, "current_price_not_positive_finite"),
        ("predicted_close", -1.0, "predicted_close_not_positive_finite"),
        (
            "expected_move_pct",
            12.5,
            "predicted_close_expected_move_inconsistent",
        ),
        ("gamma_pin", -1.0, "gamma_pin_not_positive_finite"),
    )
    for field, value, expected_reason in cases:
        evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
        evidence["database"]["symbols"]["NDX"]["prediction_snapshot"][field] = (
            value
        )
        candidate, issues, diagnostic = _candidate_from_evidence(evidence)
        assert issues == [], field
        assert candidate is not None, field
        assert "predicted_close" not in candidate, field
        assert expected_reason in diagnostic["prediction_context_reasons"], field


def test_source_and_prediction_quote_freshness_are_bounded_relative_to_scan():
    boundary = _eligible_evidence("2026-09-08T14:30:00Z", sequence=2)
    boundary["observed_at_utc"] = "2026-09-08T14:33:00+00:00"
    candidate, issues, diagnostic = _candidate_from_evidence(boundary)
    assert issues == []
    assert candidate is not None
    assert diagnostic["source_age_seconds"] == 180.0
    assert diagnostic["prediction_quote_age_seconds"] == 180.0
    assert candidate["forecast_bias"] == "bearish"

    stale_source = _eligible_evidence("2026-09-08T14:30:00Z", sequence=2)
    stale_source["observed_at_utc"] = "2026-09-08T14:33:01+00:00"
    stale_source["audits"]["NDX"]["payload"][
        "generated_at_utc"
    ] = "2026-09-08T14:33:01+00:00"
    candidate, issues, diagnostic = _candidate_from_evidence(stale_source)
    assert candidate is None
    assert "SOURCE_NOT_FRESH" in issues
    assert "AUDIT_NOT_FRESH" not in issues
    assert diagnostic["source_age_seconds"] == 181.0

    stale_quote = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    stale_quote["database"]["symbols"]["NDX"]["prediction_snapshot"][
        "quote_timestamp_utc"
    ] = "2026-09-08 14:41:00.000000"
    candidate, issues, diagnostic = _candidate_from_evidence(stale_quote)
    assert issues == []
    assert candidate is not None
    assert "forecast_bias" not in candidate
    assert "quote_not_fresh" in diagnostic["prediction_context_reasons"]


def test_missing_orb_decision_sidecar_is_a_persistence_failure():
    evidence = _eligible_evidence("2026-09-08T14:45:00Z", sequence=2)
    evidence["database"]["tables_present"].remove(
        "orb_reference_sample_decisions"
    )
    candidate, issues, _diagnostic = _candidate_from_evidence(evidence)
    assert candidate is None
    assert "DATABASE_REQUIRED_TABLES_MISSING" in issues


def test_database_evidence_rejects_truncated_gamma_input_blob(tmp_path: Path):
    database_path = tmp_path / "collector.db"
    calculation_id = "calculation-ndx-2"
    epoch = "e" * 64
    selected_universe = "a" * 64
    source_universe = "c" * 64
    payload = {
        "input_schema_version": "gamma-inputs-v2-point-in-time",
        "calculation_id": calculation_id,
        "symbol": "NDX",
        "calculated_at_utc": "2026-09-08T14:45:00",
        "subscription_epoch_id": epoch,
        "subscription_generation": 7,
        "raw_fresh_chain_rows": [],
        "calculated_gex_rows": [],
        "parameters": {},
        "rejection_counts": {},
        "output_summary": {
            "calculation_id": calculation_id,
            "symbol": "NDX",
            "subscription_epoch_id": epoch,
            "subscription_generation": 7,
            "validation_is_valid": True,
            "gamma_excluded_from_model": False,
            "price": 29_000.0,
            "gamma_pin": 29_100.0,
            "max_pain": 29_050.0,
            "primary_expiration": "2026-09-08",
            "same_day_profile_available": True,
            "selected_universe_sha256": selected_universe,
            "universe_sha256": source_universe,
            "universe_provenance": {
                "source_sha256": source_universe,
                "is_fallback": False,
                "trading_date": "2026-09-08",
                "source_date": "2026-09-08",
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
    connection = sqlite3.connect(database_path)
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE market_structure_observations (
            observation_id TEXT PRIMARY KEY, symbol TEXT, trading_date TEXT,
            source_timestamp_utc TEXT, captured_at_utc TEXT, provider TEXT,
            subscription_epoch_id TEXT, subscription_generation INTEGER,
            calculation_id TEXT, reference_price REAL, gamma_pin REAL,
            max_pain REAL, zero_gamma REAL, gross_gex REAL, net_gex REAL,
            primary_expiration TEXT, same_day_profile_available INTEGER,
            universe_sha256 TEXT, validation_status TEXT
        );
        CREATE TABLE gamma_calculation_runs (
            id INTEGER PRIMARY KEY, calculation_id TEXT, symbol TEXT,
            trading_date TEXT, calculated_at_utc TEXT, provider TEXT,
            subscription_epoch_id TEXT, subscription_generation INTEGER,
            status TEXT, input_schema_version TEXT, formula_version TEXT,
            universe_sha256 TEXT, spot_price REAL, gamma_pin REAL,
            max_pain REAL, zero_gamma REAL, gross_gex REAL, net_gex REAL
        );
        CREATE TABLE gamma_calculation_input_blobs (
            calculation_run_id INTEGER PRIMARY KEY, encoding TEXT,
            payload_sha256 TEXT, uncompressed_bytes INTEGER,
            compressed_bytes INTEGER, payload BLOB
        );
        """
    )
    connection.execute(
        "INSERT INTO market_structure_observations VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "f" * 64,
            "NDX",
            "2026-09-08",
            "2026-09-08 14:45:00.000000",
            "2026-09-08 14:45:00.500000",
            "databento",
            epoch,
            7,
            calculation_id,
            29_000.0,
            29_100.0,
            29_050.0,
            28_900.0,
            100.0,
            20.0,
            "2026-09-08",
            1,
            selected_universe,
            "valid",
        ),
    )
    connection.execute(
        "INSERT INTO gamma_calculation_runs VALUES "
        "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            1,
            calculation_id,
            "NDX",
            "2026-09-08",
            "2026-09-08 14:45:00.000000",
            "databento",
            epoch,
            7,
            "valid",
            "gamma-inputs-v2-point-in-time",
            "databento-gex-v2-call-minus-put",
            source_universe,
            29_000.0,
            29_100.0,
            29_050.0,
            28_900.0,
            100.0,
            20.0,
        ),
    )
    connection.execute(
        "INSERT INTO gamma_calculation_input_blobs VALUES (?,?,?,?,?,?)",
        (
            1,
            "canonical-json+zlib-v1",
            hashlib.sha256(canonical).hexdigest(),
            len(canonical),
            len(compressed),
            compressed,
        ),
    )
    connection.commit()
    connection.close()

    audits = {"NDX": {"payload": {"calculation_id": calculation_id}}}
    valid = _database_evidence(
        database_path, session_date="2026-09-08", audits=audits
    )
    valid_blob = valid["symbols"]["NDX"]["gamma_calculation_input_blob"]
    assert valid_blob["payload_integrity_verified"] is True
    assert valid_blob["payload_lineage_verified"] is True

    truncated = compressed[:-1]
    connection = sqlite3.connect(database_path)
    connection.execute(
        "UPDATE gamma_calculation_input_blobs SET compressed_bytes=?, payload=? "
        "WHERE calculation_run_id=1",
        (len(truncated), truncated),
    )
    connection.commit()
    connection.close()
    corrupt = _database_evidence(
        database_path, session_date="2026-09-08", audits=audits
    )
    corrupt_blob = corrupt["symbols"]["NDX"]["gamma_calculation_input_blob"]
    assert corrupt_blob["payload_integrity_verified"] is False
    assert corrupt_blob["payload_lineage_verified"] is False
    assert (
        corrupt_blob["validation_reason"]
        == "GAMMA_CALCULATION_INPUT_INTEGRITY_INVALID"
    )


def test_collector_commit_reload_collector_uses_real_receipt(tmp_path: Path):
    state_path = tmp_path / "state.json"
    journal_dir = tmp_path / "journal"
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "session_date": "2026-09-04",
                "mode": "ELEVATED",
                "opening_acceptance": {"session_date": "2026-09-04"},
                "policy_evaluator": {"SPX": {}, "NDX": {}},
            }
        ),
        encoding="utf-8",
    )
    prepared = session_rollover.prepare_monitor_session(
        state_path=state_path,
        journal_dir=journal_dir,
        observed_at_utc=datetime(2026, 9, 8, 12, 45, tzinfo=timezone.utc),
        session_date="2026-09-08",
    )
    assert prepared["accepted"] is True, prepared
    first = collect_commit_request(
        project_root=tmp_path,
        backend_url="http://unused.invalid",
        database_path=tmp_path / "unused.db",
        state_path=state_path,
        journal_dir=journal_dir,
        evidence=_eligible_evidence("2026-09-08T14:30:00Z", sequence=1),
    )
    first_scan = with_scan_event_id(
        with_policy_input_hashes(first["scan"], first["policy_inputs"])
    )
    committed = commit_monitor_scan(
        scan=first_scan,
        policy_inputs=first["policy_inputs"],
        state_path=state_path,
        journal_dir=journal_dir,
    )
    assert committed["accepted"] is True, committed
    assert committed["commit_receipt_appended"] is True

    second = collect_commit_request(
        project_root=tmp_path,
        backend_url="http://unused.invalid",
        database_path=tmp_path / "unused.db",
        state_path=state_path,
        journal_dir=journal_dir,
        evidence=_eligible_evidence("2026-09-08T14:45:00Z", sequence=2),
    )
    assert set(second["policy_inputs"]) == set(MONITORED_SYMBOLS)
    assert all(
        second["scan"]["symbols"][symbol]["eligible"] is True
        for symbol in MONITORED_SYMBOLS
    )
