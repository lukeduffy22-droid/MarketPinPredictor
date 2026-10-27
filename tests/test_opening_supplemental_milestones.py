import copy

import pytest

from tests.test_monitor_opening_acceptance import (
    _armed_session, _commit, _report, _journal, _ack, EPOCH, SESSION,
)


def _first_bucket_report():
    report = _report("2026-09-08T13:30:06Z")
    for index, symbol in enumerate(("SPX", "NDX", "RUT", "VIX")):
        report["database"]["orb_reference_rows"][symbol]["first_bucket_receipts"] = [{
            "sample_id": str(index + 1) * 64,
            "sample_timestamp_utc": "2026-09-08T13:30:00Z",
            "captured_at_utc": "2026-09-08T13:30:01.1Z",
            "progress_decided_at_utc": "2026-09-08T13:30:01.2Z",
            "subscription_epoch_id": EPOCH,
            "subscription_generation": 7,
            "timestamp_contract_version": "databento-cbbo-interval.v1",
        }]
    return report


def test_new_milestones_replay_without_changing_legacy_startup_receipt(tmp_path):
    state, journal = _armed_session(tmp_path)
    assert _commit(state, journal, "startup")["accepted"]
    old_bytes = (journal / f"{SESSION}.jsonl").read_bytes()
    first = _commit(state, journal, "first_opening_bucket", report=_first_bucket_report())
    assert first["accepted"], first
    duplicate = _commit(state, journal, "first_opening_bucket", report={})
    assert duplicate["accepted"] and not duplicate["event_appended"], duplicate
    for milestone, timestamp in (("complete_15m_orb", "2026-09-08T13:46:00Z"),
                                  ("complete_30m_orb", "2026-09-08T14:01:00Z")):
        result = _commit(state, journal, milestone, report=_report(timestamp))
        assert result["accepted"], result
    assert (journal / f"{SESSION}.jsonl").read_bytes().startswith(old_bytes)
    events = [row for row in _journal(journal / f"{SESSION}.jsonl") if row.get("event_type") == "opening_acceptance_milestone"]
    acknowledged = _ack(state, journal, [row["event_id"] for row in events])
    assert acknowledged["accepted"], acknowledged


@pytest.mark.parametrize("corruption", ["late", "wrong_generation", "missing", "duplicate", "unknown_contract"])
def test_first_bucket_receipt_cannot_be_promoted_without_exact_proof(tmp_path, corruption):
    state, journal = _armed_session(tmp_path)
    assert _commit(state, journal, "startup")["accepted"]
    report = _first_bucket_report()
    receipts = report["database"]["orb_reference_rows"]["SPX"]["first_bucket_receipts"]
    if corruption == "late":
        receipts[0]["progress_decided_at_utc"] = "2026-09-08T13:30:05Z"
    elif corruption == "wrong_generation":
        receipts[0]["subscription_generation"] = 6
    elif corruption == "missing":
        receipts.clear()
    elif corruption == "duplicate":
        receipts.append(copy.deepcopy(receipts[0]))
    else:
        receipts[0]["timestamp_contract_version"] = "unknown"
    result = _commit(state, journal, "first_opening_bucket", report=report)
    assert not result["accepted"]
    assert not result["event_appended"]
