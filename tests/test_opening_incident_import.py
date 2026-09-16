import hashlib
import json

import pytest

from backend.incident_replay import replay_incident_package
from backend.opening_incident_import import import_opening_failures


def receipt(reason="COMPLETE_PAIR_MINIMUM_NOT_MET", count=3):
    return {
        "schema_version": "orb-reference-failed-attempt-v1", "market": "SPX",
        "reason": reason, "progress_eligible": False, "usable_for_prediction": False,
        "subscription_epoch_id": "a" * 64, "subscription_generation": 1,
        "intended_bucket_utc": "2026-09-16T13:30:00+00:00",
        "attempt_completed_at_utc": "2026-09-16T13:30:01+00:00",
        "opening_gate_evidence": {"complete_pair_count": count, "minimum_pair_count": 5},
    }


def run_import(tmp_path, records, **kwargs):
    source = tmp_path / "source.ndjson"
    source.write_bytes(b"".join(json.dumps(record).encode() + b"\n" for record in records))
    original = source.read_bytes()
    result = import_opening_failures(source, destination=tmp_path / "out",
                                    project_root=tmp_path / "repo", **kwargs)
    assert source.read_bytes() == original
    assert result["source_sha256"] == hashlib.sha256(original).hexdigest()
    return result


def test_counts_all_failures_but_packages_bounded_hash_bound_examples(tmp_path):
    result = run_import(tmp_path, [receipt()] * 20)
    assert result["total_failed_attempts"] == 20
    assert result["packaged_examples"] == 2
    replay = replay_incident_package(result["package_path"])
    assert len(replay["incidents"]) == 2
    assert all(reasons == ["COMPLETE_PAIR_MINIMUM_NOT_MET"] for reasons in replay["incidents"].values())
    assert result["usable_for_prediction"] is False


def test_inconsistent_producer_reason_is_not_silently_confirmed(tmp_path):
    result = run_import(tmp_path, [receipt(count=6)])
    reasons = next(iter(replay_incident_package(result["package_path"])["incidents"].values()))
    assert "PRODUCER_REASON_EVIDENCE_CONFLICT" in reasons


def test_timeout_keeps_explicit_missing_evidence(tmp_path):
    result = run_import(tmp_path, [receipt("REFERENCE_ATTEMPT_DEADLINE_EXCEEDED")])
    reasons = next(iter(replay_incident_package(result["package_path"])["incidents"].values()))
    assert "MISSING_EVIDENCE:independent_gate_recheck_unavailable" in reasons


@pytest.mark.parametrize("count,negative,expected_conflict", [(0, 0, False), (50, 50, False), (50, 0, True)])
def test_clock_recheck_uses_counts_and_thresholds_not_claimed_status(tmp_path, count, negative, expected_conflict):
    record = receipt("PROCESSING_CLOCK_NOT_SYNCHRONIZED")
    record["opening_gate_evidence"] = {"processing_clock": {
        "sample_count": count, "minimum_samples": 50,
        "material_negative_count": negative, "maximum_negative_ratio": .01,
        "status": "unknown",
    }}
    result = run_import(tmp_path, [record])
    reasons = next(iter(replay_incident_package(result["package_path"])["incidents"].values()))
    assert ("PRODUCER_REASON_EVIDENCE_CONFLICT" in reasons) is expected_conflict


def test_generations_are_not_combined(tmp_path):
    other = {**receipt(), "subscription_generation": 2}
    result = run_import(tmp_path, [receipt(), other], examples_per_group=1)
    assert len(result["groups"]) == 2
    assert result["packaged_examples"] == 2


@pytest.mark.parametrize("payload", [b"", b"{}", b"{}\n"])
def test_invalid_or_partial_receipts_fail_before_output(tmp_path, payload):
    source = tmp_path / "source.ndjson"
    source.write_bytes(payload)
    with pytest.raises(ValueError):
        import_opening_failures(source, destination=tmp_path / "out", project_root=tmp_path / "repo")
    assert not (tmp_path / "out").exists()


def test_bounded_read_and_repository_output_rejected(tmp_path):
    source = tmp_path / "source.ndjson"
    source.write_text(json.dumps(receipt()) + "\n")
    with pytest.raises(ValueError, match="bound"):
        import_opening_failures(source, destination=tmp_path / "out", project_root=tmp_path / "repo", max_bytes=10)
    with pytest.raises(ValueError, match="outside"):
        import_opening_failures(source, destination=tmp_path / "repo" / "out", project_root=tmp_path / "repo")
