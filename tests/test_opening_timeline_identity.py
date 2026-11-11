"""Diagnostic attribution must never borrow a replacement subscription identity."""
from datetime import timedelta
from types import SimpleNamespace
import json

import pytest

import backend.databento_streamer as stream_module
from backend.opening_readiness_timeline import BoundedOpeningTimelineWriter, OpeningReadinessTimeline
from tests.test_opening_readiness_timeline import Clock, OPEN, OPEN_NS, SYMBOLS, quote


def _events(tmp_path):
    return [json.loads(line) for line in
            (tmp_path / "identity-timeline" / "2026-11-10.ndjson").read_text().splitlines()]


def _generations(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    source = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    source.active_generation = 8
    clock = Clock()
    clock.set(5.2)
    writer = BoundedOpeningTimelineWriter(tmp_path / "identity-timeline").start()
    options = dict(writer=writer, epoch_id=source.subscription_epoch_id, cash_open_utc=OPEN,
                   clock_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono)
    old = OpeningReadinessTimeline(generation=7, **options)
    current = OpeningReadinessTimeline(generation=8, **options)
    old.retire("reconnect")
    source._opening_timeline = current
    source._opening_timeline_writer = writer
    source._opening_timeline_cash_open = OPEN
    source._opening_timeline_observers = {
        (old.epoch_id, 7): old, (current.epoch_id, 8): current,
    }
    return source, old, current, writer, clock


def test_old_dispatch_cannot_make_replacement_generation_look_dispatched(tmp_path, monkeypatch):
    source, old, current, writer, clock = _generations(tmp_path, monkeypatch)
    attempt = SimpleNamespace(
        market="SPX", subscription_epoch_id=old.epoch_id, subscription_generation=7,
        intended_bucket_utc=OPEN, deadline_utc=OPEN + timedelta(seconds=5),
        deadline_monotonic=1005.0,
    )
    source._opening_timeline_dispatch(attempt)
    source._audit_opening_timeline_dispatch(OPEN + timedelta(seconds=5.2))
    assert not source._opening_timeline_dispatch_seen
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _events(tmp_path)
    dispatch = next(r for r in rows if r["kind"] == "sampler_dispatch")
    assert dispatch["subscription_generation"] == 7
    assert dispatch["details"]["observation_runtime_generation"] == 8
    assert dispatch["details"]["observed_after_generation_retirement"]
    current_audits = [r for r in rows if r["kind"] == "first_bucket_dispatch_audit"]
    assert len(current_audits) == 2
    assert all(r["subscription_generation"] == 8 for r in current_audits)
    assert all(r["details"]["stage"] == "NO_DISPATCH_OBSERVED" for r in current_audits)


def test_late_worker_ack_is_old_identity_and_never_sampler_acceptance(tmp_path, monkeypatch):
    source, old, current, writer, _clock = _generations(tmp_path, monkeypatch)
    source._opening_timeline_outcome(
        "NDX", {"recorded": True, "progress_decision_recorded": True,
                "progress_eligible": True, "sampler_acceptance": False},
        OPEN, kind="retired_worker_outcome",
        expected_epoch_id=old.epoch_id, expected_generation=7,
    )
    old.sdk_callback(quote(99))  # Retired SDK callbacks remain ignored.
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _events(tmp_path)
    row = next(r for r in rows if r["kind"] == "retired_worker_outcome")
    assert row["subscription_generation"] == 7
    assert row["details"]["worker_final_decision_acknowledged"]
    assert row["details"]["accepted_final_decision"] is False
    assert row["details"]["sampler_acceptance"] is False
    assert row["details"]["stage"] == "RETIRED_WORKER_REPORTS_FINAL_DECISION"
    assert "SAMPLER_REJECTED" in row["details"]["write_outcome"]
    assert not any(r["kind"] == "first_live_sdk_quote" for r in rows)
    assert not any(r["kind"] == "retired_worker_outcome" and
                   r["subscription_generation"] == 8 for r in rows)


def test_two_late_unknown_ids_both_have_persisted_mapping_links(tmp_path):
    clock = Clock()
    clock.set(0.5)
    writer = BoundedOpeningTimelineWriter(tmp_path / "identity-timeline").start()
    observer = OpeningReadinessTimeline(writer, epoch_id="offline", generation=7,
                                       cash_open_utc=OPEN, clock_ns=lambda: clock.wall,
                                       monotonic_ns=lambda: clock.mono)
    for iid in (11, 12):
        observer.sdk_callback(quote(iid))
    clock.set(1.5)
    for iid in (11, 12):
        observer.mapped(iid, SYMBOLS["SPX"], mapping_start_ns=OPEN_NS,
                        mapping_end_ns=OPEN_NS + 2 * 10**9, mapping_version="valid")
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _events(tmp_path)
    links = [link for row in rows for link in row.get("details", {}).get("linked_receipts", [])]
    assert len(links) == 2
    assert len({link["receipt_event_id"] for link in links}) == 2
    assert all(link["original_family_at_receipt"] == "UNKNOWN" for link in links)
    assert all(link["link_status"] == "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY" for link in links)


def test_candidate_mapping_can_gain_one_separate_valid_link_without_rewriting(tmp_path):
    clock = Clock()
    clock.set(0.5)
    writer = BoundedOpeningTimelineWriter(tmp_path / "identity-timeline").start()
    observer = OpeningReadinessTimeline(writer, epoch_id="offline", generation=7,
                                       cash_open_utc=OPEN, clock_ns=lambda: clock.wall,
                                       monotonic_ns=lambda: clock.mono)
    observer.sdk_callback(quote(11))
    clock.set(1.5)
    observer.mapped(11, SYMBOLS["NDX"], mapping_version="unknown")
    observer.mapped(11, SYMBOLS["NDX"], mapping_start_ns=OPEN_NS,
                    mapping_end_ns=OPEN_NS + 2 * 10**9, mapping_version="valid")
    observer.mapped(11, SYMBOLS["NDX"], mapping_start_ns=0,
                    mapping_end_ns=1, mapping_version="later-invalid")
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _events(tmp_path)
    links = [link for row in rows for link in row.get("details", {}).get("linked_receipts", [])]
    assert [link["mapping_version"] for link in links] == ["unknown", "valid"]
    assert links[0]["link_status"] == "CANDIDATE_MAPPING_VALIDITY_UNKNOWN"
    assert links[1]["link_status"] == "PROVIDER_INTERVAL_WITHIN_MAPPING_VALIDITY"
    original = next(r for r in rows if r["kind"] == "first_live_sdk_quote")
    assert original["family_at_observation"] == "UNKNOWN"


def test_compact_summary_keeps_real_shared_gate_fields(tmp_path):
    clock = Clock()
    clock.set(1)
    writer = BoundedOpeningTimelineWriter(tmp_path / "identity-timeline").start()
    observer = OpeningReadinessTimeline(writer, epoch_id="offline", generation=7,
                                       cash_open_utc=OPEN, clock_ns=lambda: clock.wall,
                                       monotonic_ns=lambda: clock.mono)
    observer.milestone("provider_timing_count", details={
        "stage": "50", "sample_count": 50, "minimum_samples": 50,
        "scope": "shared_current_generation",
    })
    observer.milestone("handoff_state", details={"stage": "active", "previous_status": "warming"})
    assert writer.close()["all_offered_events_acknowledged"]
    summary = observer.snapshot()
    clock_row = next(r for r in summary["compact_critical_summary"] if r["kind"] == "provider_timing_count")
    assert clock_row["details"]["sample_count"] == 50
    assert clock_row["details"]["scope"] == "shared_current_generation"
    assert any(r["kind"] == "handoff_state" for r in summary["compact_critical_summary"])
    assert summary["writer"]["scope"] == "shared_writer_lifetime_all_retained_generations"



def test_dispatch_check_add_interleaving_cannot_mark_new_generation_seen(tmp_path, monkeypatch):
    monkeypatch.setattr(stream_module, "DATA_DIR", tmp_path / "isolated-data")
    source = stream_module.DatabentoGammaStreamer(["SPX", "NDX"])
    source.active_generation = 7
    clock = Clock()
    clock.set(5.2)
    writer = BoundedOpeningTimelineWriter(tmp_path / "identity-timeline").start()
    options = dict(writer=writer, epoch_id=source.subscription_epoch_id, cash_open_utc=OPEN,
                   clock_ns=lambda: clock.wall, monotonic_ns=lambda: clock.mono)
    old = OpeningReadinessTimeline(generation=7, **options)
    new = OpeningReadinessTimeline(generation=8, **options)
    source._opening_timeline = old
    source._opening_timeline_writer = writer
    source._opening_timeline_cash_open = OPEN
    source._opening_timeline_observers = {(old.epoch_id, 7): old, (new.epoch_id, 8): new}

    class BucketThatTriggersReconnect:
        def __eq__(self, other):
            assert other == OPEN
            old.retire("controlled reconnect between check and dispatch add")
            source.active_generation = 8
            source._opening_timeline = new
            source._opening_timeline_dispatch_seen = set()
            source._opening_timeline_dispatch_audited = set()
            return True

        def isoformat(self):
            return OPEN.isoformat()

    attempt = SimpleNamespace(
        market="SPX", subscription_epoch_id=old.epoch_id, subscription_generation=7,
        intended_bucket_utc=BucketThatTriggersReconnect(),
        deadline_utc=OPEN + timedelta(seconds=5), deadline_monotonic=1005.0,
    )
    source._opening_timeline_dispatch(attempt)
    source._audit_opening_timeline_dispatch(OPEN + timedelta(seconds=5.2))
    assert (new.epoch_id, 8, "SPX") not in source._opening_timeline_dispatch_seen
    assert writer.close()["all_offered_events_acknowledged"]
    rows = _events(tmp_path)
    audits = [r for r in rows if r["kind"] == "first_bucket_dispatch_audit"]
    assert len(audits) == 2
    assert all(r["subscription_generation"] == 8 for r in audits)
    assert all(r["details"]["stage"] == "NO_DISPATCH_OBSERVED" for r in audits)
