"""Synthetic retained evidence only; never a live opening acceptance test."""
from copy import deepcopy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from backend.opening_ingress_report import opening_ingress_report, read_timeline, TIMELINE_SCHEMA
from tools.inspect_opening_readiness import main


OPEN = int(datetime(2026, 11, 17, 14, 30, tzinfo=timezone.utc).timestamp()) * 10**9


def event(number, kind, seconds=1, **fields):
    return {"schema_version": TIMELINE_SCHEMA, "authority": "DIAGNOSTIC_ONLY",
            "subscription_epoch_id": "test-epoch", "subscription_generation": 7,
            "cash_open_ns": OPEN, "first_bucket_end_ns": OPEN + 5 * 10**9,
            "quote_schema": "cbbo-1s", "event_id": f"test-epoch:7:{number}",
            "local_observed_at_ns": OPEN + round(seconds * 10**9),
            "local_observed_monotonic_ns": 10**12 + round(seconds * 10**9),
            "writer_enqueue_order": number, "kind": kind,
            "family_at_observation": "NDX", "record_key": [1, OPEN + 10**9, None], **fields}


def report(rows, **options):
    return opening_ingress_report(rows, epoch_id="test-epoch", generation=7, cash_open_ns=OPEN, **options)


def test_matches_record_and_event_id_and_reports_monotonic_gap():
    callback = event(1, "first_live_sdk_quote", 1.1, provider_interval_end_ns=OPEN+10**9)
    reader = event(2, "first_reader_processed_quote", 4, sdk_callback_receipt_for_record=callback["event_id"])
    admitted = event(3, "first_cache_admitted_quote", 4.1, sdk_callback_receipt_for_record=callback["event_id"])
    result = report([admitted, callback, reader])
    trace = result["record_stage_observations"]
    assert trace[0]["callback_to_stage_elapsed_ns"] == 2_900_000_000
    assert trace[1]["callback_to_stage_elapsed_ns"] == 3_000_000_000
    assert trace[0]["provider_interval_to_callback_wall_ns"] == 100_000_000
    assert result["diagnostic_durability"]["status"] == "UNKNOWN"
    assert result["live_opening_status"] == "NOT_VERIFIED_BY_OFFLINE_DIAGNOSTIC_REPORT"


@pytest.mark.parametrize("change", [{"record_key": [1, OPEN+2*10**9, None]},
                                    {"sdk_callback_receipt_for_record": "not-retained"}])
def test_never_pairs_different_records_or_unlinked_first_stages(change):
    callback = event(1, "first_live_sdk_quote")
    reader = event(2, "first_reader_processed_quote", sdk_callback_receipt_for_record=callback["event_id"])
    reader.update(change)
    trace = report([callback, reader])["record_stage_observations"][0]
    assert trace["callback_link_status"] == "UNKNOWN_CALLBACK_NOT_MATCHED"
    assert trace["callback_to_stage_elapsed_ns"] is None


def test_family_at_receipt_is_not_rewritten_by_later_reader_mapping():
    callback = event(1, "first_live_sdk_quote", family_at_observation="UNKNOWN")
    reader = event(2, "first_reader_processed_quote", sdk_callback_receipt_for_record=callback["event_id"])
    trace = report([callback, reader])["record_stage_observations"][0]
    assert trace["family_at_callback"] == "UNKNOWN"
    assert trace["family_at_observation"] == "NDX"


def test_unidentified_records_never_become_a_matched_trace():
    callback = event(1, "first_live_sdk_quote", record_key=[None, None, None])
    reader = event(2, "first_reader_processed_quote", record_key=[None, None, None],
                   sdk_callback_receipt_for_record=callback["event_id"])
    assert report([callback, reader])["record_stage_observations"][0]["callback_to_stage_elapsed_ns"] is None


def test_explicit_selection_filters_retiring_generation_and_deduplicates_rows():
    row = event(1, "provider_timing_admission", details={"reason": "REPLAY_PENDING", "admitted": False, "sample_count": 0})
    old = event(2, "first_live_sdk_quote", subscription_generation=6)
    result = report([row, deepcopy(row), old])
    assert result["selected_event_count"] == 1
    assert result["ignored_other_identity_event_count"] == 1
    assert result["timing_admission_observations"][0]["details"]["reason"] == "REPLAY_PENDING"


@pytest.mark.parametrize("field,value", [("cash_open_ns", OPEN+1), ("first_bucket_end_ns", OPEN+6*10**9),
                                         ("authority", "LIVE"), ("local_observed_at_ns", None),
                                         ("kind", None)])
def test_selected_identity_boundary_and_timestamp_mismatches_rejected(field, value):
    row = event(1, "first_live_sdk_quote")
    row[field] = value
    with pytest.raises(ValueError):
        report([row])


def test_schema_and_conflicting_duplicate_events_rejected():
    with pytest.raises(ValueError, match="SCHEMA"):
        report([event(1, "x", schema_version="unknown")])
    with pytest.raises(ValueError, match="CONFLICTING_DUPLICATE"):
        report([event(1, "x"), event(1, "y")])
    with pytest.raises(ValueError, match="QUOTE_SCHEMA"):
        report([event(1, "x"), event(2, "x", quote_schema="cbbo-1m")])


def test_empty_evidence_cannot_establish_provider_latency_or_capture_success():
    result = report([])
    assert result["sdk_callback_observations"] == []
    assert result["startup_check"]["status"] == "UNKNOWN"
    assert result["first_bucket_outcome_observations"] == []
    assert result["live_opening_status"].startswith("NOT_VERIFIED")


def test_preopen_and_exact_cutoff_are_distinct_and_no_clock_authority_inferred():
    rows = [event(1, "first_live_sdk_quote", -1), event(2, "first_live_sdk_quote", 5)]
    observed = report(rows)["sdk_callback_observations"]
    assert observed[0]["before_first_bucket_cutoff"] and not observed[0]["within_first_bucket"]
    assert not observed[1]["before_first_bucket_cutoff"]
    assert not observed[1]["within_first_bucket"]
    assert "HOST_CLOCK" in observed[1]["elapsed_authority"]


def snapshot():
    return {"subscription_epoch_id": "test-epoch", "subscription_generation": 7,
            "observer_overflow": 0, "callback_errors": 0,
            "instrumentation_error": None, "sdk_callback_registered": True,
            "writer": {"fsync_acknowledged_order": 1, "offered_events": 1,
                       "pending_events": 0, "all_offered_events_acknowledged": True,
                       "dropped_events": 0, "write_failures": 0, "write_outcome_unknown_events": 0}}


def test_durability_requires_matching_snapshot_and_no_gaps():
    rows = [event(1, "first_live_sdk_quote")]
    assert report(rows, writer_snapshot=snapshot())["diagnostic_durability"]["status"] == "ACKNOWLEDGED_RETAINED_TRACE"
    wrong = snapshot()
    wrong["subscription_generation"] = 6
    with pytest.raises(ValueError, match="SNAPSHOT_IDENTITY"):
        report(rows, writer_snapshot=wrong)
    failed = snapshot()
    failed["writer"]["write_failures"] = 1
    assert report(rows, writer_snapshot=failed)["diagnostic_durability"]["status"] == "UNKNOWN"
    stale = snapshot()
    stale["writer"]["fsync_acknowledged_order"] = 0
    assert report(rows, writer_snapshot=stale)["diagnostic_durability"]["status"] == "UNKNOWN"


@pytest.mark.parametrize("field,value", [("instrumentation_error", "RuntimeError"),
                                         ("sdk_callback_registered", False)])
def test_instrumentation_error_or_missing_callback_invalidates_durability(field, value):
    evidence = snapshot()
    evidence[field] = value
    assert report([event(1, "x")], writer_snapshot=evidence)["diagnostic_durability"]["status"] == "UNKNOWN"
    del evidence[field]
    assert report([event(1, "x")], writer_snapshot=evidence)["diagnostic_durability"]["status"] == "UNKNOWN"


def test_shared_overflow_notice_prevents_false_complete_acknowledgment():
    rows = [event(1, "first_live_sdk_quote"), {"schema_version": TIMELINE_SCHEMA, "kind": "writer_overflow"}]
    assert report(rows, writer_snapshot=snapshot())["diagnostic_durability"]["status"] == "UNKNOWN"


def test_startup_check_requires_observed_queue_order_and_states():
    stages = ["queue_ownership_established", "queue_enabled_before_iterator_start",
              "iterator_start_returned", "iterator_ownership_verified"]
    rows = [event(i, "sdk_iterator_startup", -1, details={"stage": stage, "owned_queue": True,
                  "queue_enabled": True, "startup_observed_monotonic_ns": i}) for i, stage in enumerate(stages, 1)]
    assert report(rows)["startup_check"]["status"] == "PASS"
    rows[1]["details"]["queue_enabled"] = False
    assert report(rows)["startup_check"]["status"] == "FAIL"
    assert report(rows[:1])["startup_check"]["status"] == "UNKNOWN"


def test_reader_monotonic_discontinuity_not_reported_as_negative_wait():
    callback = event(1, "first_live_sdk_quote", 2)
    reader = event(2, "first_reader_processed_quote", 1, sdk_callback_receipt_for_record=callback["event_id"])
    assert report([reader, callback])["record_stage_observations"][0]["callback_to_stage_elapsed_ns"] is None


def test_first_bucket_outcome_requires_exact_intended_bucket_and_keeps_producer_scope():
    result = report([event(1, "orb_attempt_outcome", 5.1, details={"intended_bucket_utc": "2026-11-17T14:30:00+00:00", "accepted_final_decision": True}),
                     event(2, "orb_attempt_outcome", 6, details={"intended_bucket_utc": "2026-11-17T14:30:05+00:00"})])
    assert len(result["first_bucket_outcome_observations"]) == 1
    assert "independently verify" in result["first_bucket_outcome_observations"][0]["meaning"]
    assert result["live_opening_status"].startswith("NOT_VERIFIED")


def test_cli_reads_only_and_rejects_truncated_json(tmp_path, capsys):
    source = tmp_path / "opening.ndjson"
    content = json.dumps(event(1, "first_live_sdk_quote")) + "\n"
    source.write_text(content)
    args = [str(source), "--epoch", "test-epoch", "--generation", "7", "--cash-open", "2026-11-17T14:30:00+00:00"]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["selected_event_count"] == 1
    assert source.read_text() == content
    assert list(tmp_path.iterdir()) == [source]
    source.write_text(content + '{"partial":')
    with pytest.raises(ValueError, match="INVALID_TIMELINE_LINE:2"):
        read_timeline(source)


def test_cli_subprocess_emits_json_without_app_connections_or_input_writes(tmp_path):
    source = tmp_path / "opening.ndjson"
    content = json.dumps(event(1, "provider_timing_admission", details={"reason": "STALE_RECEIVE_LAG", "admitted": False})) + "\n"
    source.write_text(content)
    cli = Path(__file__).resolve().parents[1] / "tools" / "inspect_opening_readiness.py"
    completed = subprocess.run([sys.executable, str(cli), str(source), "--epoch", "test-epoch",
                                "--generation", "7", "--cash-open", "2026-11-17T08:30:00-06:00"],
                               cwd=tmp_path, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
                               capture_output=True, text=True, timeout=10, check=False)
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["timing_admission_observations"][0]["details"]["reason"] == "STALE_RECEIVE_LAG"
    assert result["live_opening_status"].startswith("NOT_VERIFIED")
    assert source.read_text() == content
    assert list(tmp_path.iterdir()) == [source]
