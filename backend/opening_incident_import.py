"""Bounded, read-only conversion of retained ORB failures into replay evidence."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import re

from backend.incident_replay import (
    INCIDENT_EVIDENCE_SCHEMA, export_incident_package, validate_incident_record,
)


def opening_failure_record(receipt: dict, *, source_sha256: str, line: int,
                           raw_line_sha256: str) -> dict:
    """Keep producer rejection separate from checks supported by its telemetry."""
    if (receipt.get("schema_version") != "orb-reference-failed-attempt-v1"
            or receipt.get("progress_eligible") is not False
            or receipt.get("usable_for_prediction") is not False):
        raise ValueError("not an ineligible opening receipt")
    reason = receipt.get("reason")
    if not isinstance(reason, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]+", reason):
        raise ValueError("unsupported producer rejection reason")
    gates = receipt.get("opening_gate_evidence") or {}
    if not isinstance(gates, dict):
        raise ValueError("opening gate evidence must be an object")
    # Export allowlisted diagnostics only, never arbitrary log text or source paths.
    diagnostics = {
        key: gates[key] for key in (
            "processing_clock", "pair_evaluation", "complete_pair_count",
            "minimum_pair_count", "quote_rejections", "selected_primary_contract_count",
            "mapped_primary_contract_count", "available_quote_count", "snapshot_generation",
            "snapshot_subscription_epoch_id", "selected_universe_sha256",
            "minimum_source_quote_age_seconds", "maximum_source_quote_age_seconds",
            "earliest_ts_event_ns", "latest_ts_event_ns", "earliest_ts_recv_ns", "latest_ts_recv_ns",
        ) if key in gates
    }
    handoff = gates.get("handoff") or {}
    diagnostics["handoff"] = {
        key: handoff[key] for key in ("status", "required_symbols", "missing_fresh_symbols")
        if key in handoff
    }
    missing = []
    consistent = None
    if reason == "PROCESSING_CLOCK_NOT_SYNCHRONIZED":
        clock = gates.get("processing_clock") or {}
        values = [clock.get(key) for key in (
            "sample_count", "minimum_samples", "material_negative_count", "maximum_negative_ratio")]
        if all(type(value) in (int, float) for value in values):
            count, minimum, negative, maximum_ratio = values
            if count >= 0 and minimum > 0 and 0 <= negative <= count and 0 <= maximum_ratio <= 1:
                consistent = count < minimum or (count > 0 and negative / count > maximum_ratio)
    elif reason == "HANDOFF_NOT_ACTIVE":
        if isinstance(handoff.get("status"), str):
            consistent = handoff["status"] != "active"
    elif reason == "COMPLETE_PAIR_MINIMUM_NOT_MET":
        count, minimum = gates.get("complete_pair_count"), gates.get("minimum_pair_count")
        if type(count) is int and type(minimum) is int and count >= 0 and minimum > 0:
            consistent = count < minimum
    if consistent is None:
        missing.append("independent_gate_recheck_unavailable")
    checks = [{"name": "producer_progress_eligibility", "passed": False, "reason": reason}]
    if consistent is not None:
        checks.append({"name": "producer_reason_matches_retained_telemetry",
                       "passed": consistent, "reason": "PRODUCER_REASON_EVIDENCE_CONFLICT"})
    record = {
        "schema_version": INCIDENT_EVIDENCE_SCHEMA,
        "incident_id": f"opening-{raw_line_sha256}-{line}",
        "category": "opening",
        "observed_at_utc": receipt.get("attempt_completed_at_utc") or receipt.get("intended_bucket_utc"),
        "symbol": receipt.get("market"),
        "subscription_epoch_id": receipt.get("subscription_epoch_id"),
        "subscription_generation": receipt.get("subscription_generation"),
        "validation_method": "opening-receipt-import-v1",
        "missing_evidence_reasons": missing,
        "checks": checks,
        "evidence": {
            "evidence_role": "research_only_opening_diagnostic",
            "usable_for_prediction": False,
            "source_sha256": source_sha256, "source_line": line,
            "source_line_sha256": raw_line_sha256,
            "producer_schema_version": receipt["schema_version"],
            "producer_reason": reason,
            "intended_bucket_utc": receipt.get("intended_bucket_utc"),
            "diagnostics": diagnostics,
        },
    }
    return validate_incident_record(record)


def import_opening_failures(source: Path, *, destination: Path, project_root: Path,
                            examples_per_group: int = 2, max_bytes: int = 64_000_000) -> dict:
    """Count every receipt; package bounded examples per symbol/epoch/reason.

    This is retained-diagnostic verification, not reconstruction of raw quotes.
    The input must remain unchanged for the duration of the bounded read.
    """
    root, target = project_root.resolve(), destination.resolve()
    if target == root or target.is_relative_to(root):
        raise ValueError("incident replay packages must be written outside the repository")
    if target.exists():
        raise FileExistsError(target)
    if not 1 <= examples_per_group <= 10 or max_bytes <= 0:
        raise ValueError("invalid import bounds")
    with source.open("rb") as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("opening receipt file exceeds import bound")
    if not raw or not raw.endswith(b"\n"):
        raise ValueError("opening receipt file is empty or has an incomplete final line")
    digest = hashlib.sha256(raw).hexdigest()
    counts = Counter()
    selected = []
    for line_number, raw_line in enumerate(raw.splitlines(keepends=True), start=1):
        receipt = json.loads(raw_line)
        if not isinstance(receipt, dict):
            raise ValueError(f"invalid opening receipt at line {line_number}")
        record = opening_failure_record(
            receipt, source_sha256=digest, line=line_number,
            raw_line_sha256=hashlib.sha256(raw_line).hexdigest())
        group = (record["symbol"], record["subscription_epoch_id"],
                 record["subscription_generation"], receipt["reason"])
        counts[group] += 1
        if counts[group] <= examples_per_group:
            selected.append(record)
        if len(counts) > 1000:
            raise ValueError("too many incident groups")
    # Detect replacement, truncation, or append while collecting evidence.
    with source.open("rb") as handle:
        if hashlib.sha256(handle.read(max_bytes + 1)).hexdigest() != digest:
            raise ValueError("opening receipt source changed during import")
    target.mkdir(parents=True)
    sources_dir = target / "derived"
    sources_dir.mkdir()
    sources = []
    for index, record in enumerate(selected):
        path = sources_dir / f"{index:04d}.json"
        path.write_text(json.dumps(record, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        sources.append(path)
    package = export_incident_package(sources, destination=target / "package", project_root=root)
    summary = {
        "schema_version": "opening-incident-import-summary.v1",
        "validation_method": "opening-receipt-import-v1",
        "source_sha256": digest, "source_bytes": len(raw),
        "total_failed_attempts": sum(counts.values()),
        "packaged_examples": len(selected), "usable_for_prediction": False,
        "scope": "retained_failure_diagnostics_only_not_raw_feed_replay",
        "groups": [dict(symbol=key[0], subscription_epoch_id=key[1],
                        subscription_generation=key[2], reason=key[3], count=value)
                   for key, value in sorted(counts.items(), key=lambda item: str(item[0]))],
        "package_path": package["package_path"],
        "manifest_sha256": package["manifest_sha256"],
    }
    (target / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary
