"""Read-only attribution of retained opening observations; never live authority."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
import json


TIMELINE_SCHEMA = "marketpin-opening-readiness-timeline.v1"
REPORT_SCHEMA = "marketpin-opening-ingress-report.v1"
UPSTREAM_LIMIT = "Provider, network, socket arrival, decode start and earlier SDK work are not separated by callback entry."
READER_LIMIT = "Callback-to-reader elapsed time does not separate SDK queue residence from reader scheduling."


def _integer(value):
    return type(value) is int and value >= 0


def _details(row):
    value = row.get("details", {})
    return value if isinstance(value, dict) else {}


def _record_key(value):
    return (isinstance(value, list) and len(value) == 3 and _integer(value[0])
            and _integer(value[1]) and value[1] > 0
            and (value[2] is None or _integer(value[2])))


def _instant(value):
    try:
        observed = datetime.fromisoformat(value)
        if observed.tzinfo is not None:
            return int(observed.timestamp() * 1_000_000_000)
    except (TypeError, ValueError):
        pass
    return None


def read_timeline(path):
    """Reject malformed/truncated lines rather than silently losing evidence."""
    rows = []
    with open(path, encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
                if not isinstance(row, dict):
                    raise ValueError("event must be an object")
            except ValueError as exc:
                raise ValueError(f"INVALID_TIMELINE_LINE:{number}") from exc
            rows.append(row)
    return rows


def _selection(events, epoch_id, generation, cash_open_ns):
    if not isinstance(epoch_id, str) or not epoch_id or not _integer(generation) or generation <= 0:
        raise ValueError("EXPLICIT_EPOCH_AND_POSITIVE_GENERATION_REQUIRED")
    if not _integer(cash_open_ns) or cash_open_ns <= 0:
        raise ValueError("EXPLICIT_CASH_OPEN_NS_REQUIRED")
    selected, ids, ignored, unscoped = [], {}, 0, 0
    for row in events:
        if not isinstance(row, dict) or row.get("schema_version") != TIMELINE_SCHEMA:
            raise ValueError("UNSUPPORTED_TIMELINE_SCHEMA")
        if row.get("kind") == "writer_overflow":
            # Shared writer notices lack epoch/generation identity.
            unscoped += 1
            continue
        if not isinstance(row.get("subscription_epoch_id"), str) or not _integer(row.get("subscription_generation")):
            raise ValueError("EVENT_IDENTITY_MISSING")
        if (row["subscription_epoch_id"], row["subscription_generation"]) != (epoch_id, generation):
            ignored += 1
            continue
        if row.get("cash_open_ns") != cash_open_ns or row.get("first_bucket_end_ns") != cash_open_ns + 5_000_000_000:
            raise ValueError("SELECTED_OPENING_BOUNDARY_MISMATCH")
        if row.get("authority") != "DIAGNOSTIC_ONLY":
            raise ValueError("SELECTED_EVENT_AUTHORITY_INVALID")
        if not isinstance(row.get("kind"), str) or not row["kind"]:
            raise ValueError("SELECTED_EVENT_KIND_MISSING")
        if not _integer(row.get("local_observed_at_ns")) or not _integer(row.get("local_observed_monotonic_ns")):
            raise ValueError("SELECTED_EVENT_TIMESTAMP_INVALID")
        event_id = row.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("SELECTED_EVENT_ID_MISSING")
        if event_id in ids:
            if ids[event_id] != row:
                raise ValueError("CONFLICTING_DUPLICATE_EVENT_ID")
            continue
        ids[event_id] = row
        selected.append(row)
    schemas = {row.get("quote_schema") for row in selected}
    if len(schemas) > 1 or schemas - {"cbbo-1s", "cbbo-1m", "mbp-1", "tcbbo"}:
        raise ValueError("SELECTED_QUOTE_SCHEMA_MISMATCH")
    selected.sort(key=lambda row: (row["local_observed_monotonic_ns"], row["event_id"]))
    return selected, ignored, unscoped


def _observation(row, cash_open_ns):
    stamp = row["local_observed_at_ns"]
    return {"event_id": row["event_id"], "kind": row["kind"],
            "family_at_observation": row.get("family_at_observation"),
            "observed_offset_ns": stamp - cash_open_ns,
            "before_first_bucket_cutoff": stamp < cash_open_ns + 5_000_000_000,
            "within_first_bucket": cash_open_ns <= stamp < cash_open_ns + 5_000_000_000}


def _startup_check(observations):
    required = ("queue_ownership_established", "queue_enabled_before_iterator_start",
                "iterator_start_returned", "iterator_ownership_verified")
    stages = {_details(row).get("stage"): row for row in observations}
    failures = {"queue_contract_unavailable", "queue_ownership_rejected", "startup_failed"}
    if stages.keys() & failures:
        return {"status": "FAIL", "reason": "STARTUP_FAILURE_OBSERVED"}
    if not all(stage in stages for stage in required):
        return {"status": "UNKNOWN", "reason": "REQUIRED_STARTUP_STAGES_NOT_RETAINED"}
    times = [_details(stages[stage]).get("startup_observed_monotonic_ns") for stage in required]
    if not all(_integer(stamp) for stamp in times):
        return {"status": "UNKNOWN", "reason": "HELPER_MONOTONIC_TIMESTAMPS_NOT_RETAINED"}
    enabled = _details(stages[required[1]]).get("queue_enabled") is True
    owned = _details(stages[required[0]]).get("owned_queue") is True
    passed = enabled and owned and times == sorted(times)
    return {"status": "PASS" if passed else "FAIL", "reason": "OWNED_QUEUE_ENABLE_BEFORE_ITERATOR_START_OBSERVED" if passed else "STARTUP_ORDER_OR_QUEUE_STATE_INVALID",
            "scope": "Observed startup ordering only; no live capture authority."}


def _durability(rows, snapshot, epoch_id, generation, unscoped):
    if snapshot is None:
        return {"status": "UNKNOWN", "reason": "RETAINED_WRITER_SNAPSHOT_NOT_SUPPLIED"}
    if not isinstance(snapshot, dict) or (snapshot.get("subscription_epoch_id"), snapshot.get("subscription_generation")) != (epoch_id, generation):
        raise ValueError("WRITER_SNAPSHOT_IDENTITY_MISMATCH")
    writer = snapshot.get("writer", {})
    if not isinstance(writer, dict):
        raise ValueError("WRITER_SNAPSHOT_INVALID")
    ack, offered = writer.get("fsync_acknowledged_order"), writer.get("offered_events")
    row_orders = [row.get("writer_enqueue_order") for row in rows]
    known = (bool(rows) and _integer(ack) and _integer(offered) and ack == offered
             and writer.get("all_offered_events_acknowledged") is True
             and writer.get("pending_events") == 0
             and all(writer.get(name) == 0 for name in ("dropped_events", "write_failures", "write_outcome_unknown_events"))
             and snapshot.get("observer_overflow") == 0 and snapshot.get("callback_errors") == 0
             and "instrumentation_error" in snapshot and snapshot["instrumentation_error"] is None
             and snapshot.get("sdk_callback_registered") is True
             and all(_integer(order) and 0 < order <= ack for order in row_orders)
             and not unscoped)
    return {"status": "ACKNOWLEDGED_RETAINED_TRACE" if known else "UNKNOWN",
            "reason": "RETAINED_SNAPSHOT_ACKNOWLEDGES_SELECTED_ROWS" if known else "INCOMPLETE_OR_UNACKNOWLEDGED_DIAGNOSTICS",
            "scope": "Retained observations only; bounded first-stage events are not a complete per-record trace."}


def opening_ingress_report(events, *, epoch_id, generation, cash_open_ns, writer_snapshot=None):
    rows, ignored, unscoped = _selection(events, epoch_id, generation, cash_open_ns)
    by_id = {row["event_id"]: row for row in rows}
    traces = []
    stage_kinds = {"first_reader_processed_quote", "first_cache_admitted_quote", "first_quote_rejection"}
    for row in rows:
        if row["kind"] not in stage_kinds:
            continue
        callback_id = row.get("sdk_callback_receipt_for_record")
        callback = by_id.get(callback_id)
        linked = bool(callback is not None and callback["kind"] in {"first_live_sdk_quote", "first_replay_sdk_quote"}
                      and _record_key(row.get("record_key"))
                      and callback.get("record_key") == row["record_key"])
        elapsed = None
        if linked:
            delta = row["local_observed_monotonic_ns"] - callback["local_observed_monotonic_ns"]
            if delta >= 0:
                elapsed = delta
        trace = {**_observation(row, cash_open_ns), "record_key": row.get("record_key"),
                 "callback_link_status": "MATCHED_RECORD_AND_EVENT_ID" if linked else "UNKNOWN_CALLBACK_NOT_MATCHED",
                 "callback_event_id": callback_id if linked else None,
                 "callback_to_stage_elapsed_ns": elapsed,
                 "replay_pending": row.get("replay_pending"), "reason": _details(row).get("reason")}
        if linked:
            trace["family_at_callback"] = callback.get("family_at_observation")
            provider = callback.get("provider_interval_end_ns")
            trace["provider_interval_to_callback_wall_ns"] = callback["local_observed_at_ns"] - provider if _integer(provider) else None
            trace["provider_elapsed_authority"] = "UNVERIFIED_REQUIRES_HOST_CLOCK_EVIDENCE"
        traces.append(trace)

    startup = [{**_observation(row, cash_open_ns), "details": _details(row)}
               for row in rows if row["kind"] == "sdk_iterator_startup"]
    callbacks = []
    for row in rows:
        if row["kind"] not in {"first_live_sdk_quote", "first_replay_sdk_quote"}:
            continue
        provider = row.get("provider_interval_end_ns")
        callbacks.append({**_observation(row, cash_open_ns), "record_key": row.get("record_key"),
                          "provider_interval_to_callback_wall_ns": row["local_observed_at_ns"] - provider if _integer(provider) else None,
                          "elapsed_authority": "UNVERIFIED_REQUIRES_HOST_CLOCK_EVIDENCE",
                          "semantics": "After decoding and earlier SDK handlers; not socket arrival."})
    timing = [{**_observation(row, cash_open_ns), "details": _details(row)}
              for row in rows if row["kind"] in {"provider_timing_admission", "provider_timing_count"}]
    outcomes = []
    for row in rows:
        if row["kind"] not in {"orb_attempt_outcome", "retired_worker_outcome"}:
            continue
        details = _details(row)
        if _instant(details.get("intended_bucket_utc")) != cash_open_ns:
            continue
        outcomes.append({**_observation(row, cash_open_ns), "details": details,
                         "meaning": "Retained producer/sampler claim; independently verify the immutable capture journal."})

    return {"schema_version": REPORT_SCHEMA, "authority": "DIAGNOSTIC_ONLY",
            "selection": {"subscription_epoch_id": epoch_id, "subscription_generation": generation,
                          "cash_open_ns": cash_open_ns, "first_bucket_end_ns": cash_open_ns + 5_000_000_000},
            "selected_event_count": len(rows), "ignored_other_identity_event_count": ignored,
            "unscoped_writer_overflow_notice_count": unscoped,
            "event_kind_counts": dict(Counter(row["kind"] for row in rows)),
            "diagnostic_durability": _durability(rows, writer_snapshot, epoch_id, generation, unscoped),
            "startup_check": _startup_check(startup), "startup_observations": startup,
            "sdk_callback_observations": callbacks,
            "timing_admission_observations": timing,
            "timing_observation_scope": "First outcome/milestone events may coalesce; event counts are not attempted or admitted record counts.",
            "record_stage_observations": traces, "first_bucket_outcome_observations": outcomes,
            "live_opening_status": "NOT_VERIFIED_BY_OFFLINE_DIAGNOSTIC_REPORT",
            "limits": [UPSTREAM_LIMIT, READER_LIMIT,
                       "Missing or coalesced events are UNKNOWN; no events cannot prove provider latency or absence of provider records.",
                       "Timing milestones and cache admission do not grant clock, pair, handoff, sampler or persistence authority.",
                       "Wall-clock offsets require independent host-clock evidence; monotonic stage gaps have no invented latency threshold."]}
