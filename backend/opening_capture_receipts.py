"""Automatic, append-only opening checks driven by the existing sampler loop."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

MILESTONES = (("first_opening_bucket", 5), ("5m", 300), ("15m", 900),
              ("30m", 1800), ("60m", 3600))
CENTRAL = ZoneInfo("America/Chicago")


def due_checks(observed_at: datetime, completed: set[tuple], epoch: str, generation: int):
    local = observed_at.astimezone(CENTRAL)
    opening = datetime.combine(local.date(), time(8, 30), tzinfo=CENTRAL)
    for milestone, seconds in MILESTONES:
        key = (local.date().isoformat(), epoch, generation, milestone)
        if key not in completed and observed_at >= opening + timedelta(seconds=seconds):
            yield key


def capture_receipt(*, journal, symbols, key, observed_at, context_is_current, destination: Path):
    """Observe persisted ranges; a later inspection never changes sample times."""
    trading_date, epoch, generation, milestone = key
    windows = {}
    from datetime import date
    for symbol in symbols:
        snapshot = journal.snapshot(
            symbol, trading_date=date.fromisoformat(trading_date), as_of_utc=observed_at,
            active_subscription_epoch_id=epoch, active_subscription_generation=generation,
            active_handoff_status="active",
        )
        window = snapshot["opening_ranges"]["5m" if milestone == "first_opening_bucket" else milestone]
        evidence = window.get("capture_evidence") or {}
        provenance = window.get("provenance") or {}
        # The projection already binds all contributing rows and the immutable
        # final decisions. Never treat a retained opening bucket as current after
        # a process/generation transition.
        aligned = all(provenance.get(field) is True for field in (
            "active_runtime_epoch_aligned", "range_provenance_aligned", "current_vs_range_aligned"))
        present = evidence.get("opening_bucket_present") is True
        reasons = []
        if not present:
            reasons.append("EXACT_FIRST_BUCKET_MISSING")
        if not aligned:
            reasons.append("RUNTIME_OR_RANGE_IDENTITY_UNVERIFIED")
        if milestone != "first_opening_bucket" and window.get("capture_status") != "complete":
            reasons.append("WINDOW_COVERAGE_INCOMPLETE")
        passed = aligned and present and (
            milestone == "first_opening_bucket" or window.get("capture_status") == "complete")
        windows[symbol] = {
            "classification": "OBSERVED", "passed": passed,
            "reasons": reasons, "warnings": window.get("warnings") or [],
            "capture_status": window.get("capture_status"),
            "capture_evidence": evidence, "provenance": provenance,
            "runtime_aligned": aligned, "opening_bucket_present": present,
            "full_bucket_coverage": (present if milestone == "first_opening_bucket" else
                type(evidence.get("sample_count")) is int and evidence["sample_count"] > 0 and
                evidence["sample_count"] == evidence.get("expected_sample_count")),
        }
    stable = context_is_current()
    if not stable:
        for window in windows.values():
            window.update(passed=False, classification="NOT YET VERIFIED", reason="RUNTIME_CONTEXT_CHANGED_DURING_CHECK")
    receipt = {
        "schema_version": "marketpin-opening-capture-check.v1",
        "trading_date": trading_date, "milestone": milestone,
        "subscription_epoch_id": epoch, "subscription_generation": generation,
        "observed_at_utc": observed_at.astimezone(timezone.utc).isoformat(),
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "runtime_context_stable": stable, "symbols": windows,
        "inspector_process_id": os.getpid(),
        "evidence_role": "persisted_opening_capture_verification",
        "usable_for_prediction": False,
    }
    encoded = (json.dumps(receipt, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    digest = hashlib.sha256(encoded).hexdigest()
    folder = destination / trading_date
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{digest}.json"
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    path.chmod(0o444)
    return {"path": str(path), "sha256": digest, **receipt}
