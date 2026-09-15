import copy
import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from backend.closing_tape.promotion import (
    PROMOTION_APPROVAL_CONTRACT_VERSION,
    approve_and_write_promoted_model_manifest,
    load_promotion_approval_receipt,
    promotion_proposal_sha256,
    record_promotion_decision,
    validate_promotion_approval,
    write_promoted_model_manifest,
)
from backend.closing_tape.status import _model_gate
from tests.test_closing_tape_status import (
    _install_synthetic_promotion_source_replay,
    _passing_model_manifest,
)


UTC = timezone.utc
APPROVED_AT = datetime(2026, 8, 26, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _decode_synthetic_promotion_sources(monkeypatch):
    _install_synthetic_promotion_source_replay(monkeypatch)


def _approve(tmp_path, manifest, *, operator="risk-owner", approved_at=APPROVED_AT):
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by=operator,
        decision="APPROVE",
        approved_at_utc=approved_at,
    )
    return manifest


def test_manifest_publication_requires_explicit_human_approval_receipt(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    candidate = tmp_path / "models" / "candidate-manifest.json"
    candidate.write_text(json.dumps(manifest), encoding="utf-8")

    gate = _model_gate(tmp_path, manifest_path=candidate)

    assert not gate["passed"]
    assert "human promotion approval receipt is missing" in gate["reason"]
    assert not gate["promotion_approval_verified"]
    with pytest.raises(ValueError, match="human promotion approval receipt is missing"):
        write_promoted_model_manifest(tmp_path, manifest)
    assert not (tmp_path / "models" / "closing_tape_model.json").exists()


def test_matching_content_addressed_approval_receipt_passes_gate_and_publishes(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    _approve(tmp_path, manifest)

    validation = validate_promotion_approval(tmp_path, manifest)
    destination = write_promoted_model_manifest(tmp_path, manifest)
    gate = _model_gate(tmp_path)

    assert validation["passed"]
    assert validation["approved_by"] == "risk-owner"
    assert manifest["promotion_approval"]["contract_version"] == (
        PROMOTION_APPROVAL_CONTRACT_VERSION
    )
    assert destination.is_file()
    assert gate["passed"]
    assert gate["promotion_approval_verified"]
    assert gate["promotion_approval"]["approved_by"] == "risk-owner"


def test_rejected_decision_is_durable_but_never_authorizes_publication(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    manifest["promotion_approval"] = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="risk-owner",
        decision="REJECT",
        approved_at_utc=APPROVED_AT,
    )

    validation = validate_promotion_approval(tmp_path, manifest)

    assert not validation["passed"]
    assert validation["reason"] == "promotion decision is not APPROVE"
    with pytest.raises(ValueError, match="promotion decision is not APPROVE"):
        write_promoted_model_manifest(tmp_path, manifest)


def test_approval_is_invalid_after_any_manifest_content_change(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    _approve(tmp_path, manifest)
    manifest["publication_note"] = "changed after human review"

    validation = validate_promotion_approval(tmp_path, manifest)

    assert not validation["passed"]
    assert validation["reason"] == "promotion approval does not match the proposed manifest"
    with pytest.raises(ValueError, match="does not match the proposed manifest"):
        write_promoted_model_manifest(tmp_path, manifest)


def test_tampered_or_path_escaped_receipt_fails_closed(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    _approve(tmp_path, manifest)
    reference = manifest["promotion_approval"]
    receipt_path = tmp_path / "models" / reference["receipt_path"]
    receipt_path.write_bytes(receipt_path.read_bytes() + b"tampered")

    tampered = validate_promotion_approval(tmp_path, manifest)
    assert not tampered["passed"]
    assert "content hash does not match" in tampered["reason"]

    escaped = copy.deepcopy(manifest)
    escaped["promotion_approval"]["receipt_path"] = "../outside.json"
    escaped_result = validate_promotion_approval(tmp_path, escaped)
    assert not escaped_result["passed"]
    assert "promotion_approvals" in escaped_result["reason"] or "content-addressed" in escaped_result["reason"]


def test_replacement_requires_a_fresh_receipt_bound_to_replacement_content(tmp_path):
    original = _passing_model_manifest(tmp_path, include_approval=False)
    _approve(tmp_path, original)
    destination = write_promoted_model_manifest(tmp_path, original)

    replacement = _passing_model_manifest(
        tmp_path, include_approval=False, version="test-2"
    )
    replacement["promotion_approval"] = copy.deepcopy(
        original["promotion_approval"]
    )
    with pytest.raises(ValueError, match="does not match the proposed manifest"):
        write_promoted_model_manifest(tmp_path, replacement, allow_replace=True)
    assert json.loads(destination.read_text(encoding="utf-8"))["version"] == "test-1"

    _approve(
        tmp_path,
        replacement,
        approved_at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC),
    )
    assert write_promoted_model_manifest(
        tmp_path, replacement, allow_replace=True
    ) == destination
    assert json.loads(destination.read_text(encoding="utf-8"))["version"] == "test-2"


def test_concurrent_same_process_publishers_cannot_clobber_each_other(
    tmp_path, monkeypatch
):
    first = _passing_model_manifest(tmp_path, include_approval=False, version="first")
    second = _passing_model_manifest(tmp_path, include_approval=False, version="second")
    barrier = threading.Barrier(2)

    def coordinated_gate(_root, manifest_path=None, manifest_bytes=None, **_kwargs):
        assert manifest_path is not None
        assert manifest_bytes is not None
        barrier.wait(timeout=5)
        return {
            "passed": True,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }

    monkeypatch.setattr(
        "backend.closing_tape.status._model_gate",
        coordinated_gate,
    )

    def publish(manifest):
        try:
            return ("published", write_promoted_model_manifest(tmp_path, manifest))
        except Exception as exc:  # returned for deterministic cross-thread assertion
            return ("error", exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(publish, (first, second)))

    published = [value for status, value in outcomes if status == "published"]
    errors = [value for status, value in outcomes if status == "error"]
    assert len(published) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], FileExistsError)
    destination = tmp_path / "models" / "closing_tape_model.json"
    assert json.loads(destination.read_text(encoding="utf-8"))["version"] in {
        "first",
        "second",
    }
    assert list((tmp_path / "models").glob(".*.tmp")) == []


def test_recording_identical_decision_is_idempotent(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)

    first = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="risk-owner",
        decision="APPROVE",
        approved_at_utc=APPROVED_AT,
    )
    second = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="risk-owner",
        decision="APPROVE",
        approved_at_utc=APPROVED_AT,
    )

    assert second == first
    assert len(list((tmp_path / "models" / "promotion_approvals").glob("*.json"))) == 1


def test_future_dated_approval_is_rejected_when_recorded_loaded_or_gated(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    future = datetime.now(UTC) + timedelta(hours=1)
    with pytest.raises(ValueError, match="unreasonably in the future"):
        record_promotion_decision(
            tmp_path,
            manifest,
            approved_by="risk-owner",
            decision="APPROVE",
            approved_at_utc=future,
        )

    reference = record_promotion_decision(
        tmp_path,
        manifest,
        approved_by="risk-owner",
        decision="APPROVE",
        approved_at_utc=APPROVED_AT,
    )
    original_path = tmp_path / "models" / str(reference["receipt_path"])
    receipt = json.loads(original_path.read_text(encoding="utf-8"))
    receipt["approved_at_utc"] = future.isoformat()
    forged_bytes = (
        json.dumps(
            receipt, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        + b"\n"
    )
    forged_hash = hashlib.sha256(forged_bytes).hexdigest()
    forged_path = original_path.with_name(f"{forged_hash}.json")
    forged_path.write_bytes(forged_bytes)
    manifest["promotion_approval"] = {
        **reference,
        "receipt_path": f"promotion_approvals/{forged_path.name}",
        "receipt_sha256": forged_hash,
    }

    with pytest.raises(ValueError, match="unreasonably in the future"):
        load_promotion_approval_receipt(tmp_path, forged_hash)
    validation = validate_promotion_approval(tmp_path, manifest)
    assert validation["passed"] is False
    assert "unreasonably in the future" in str(validation["reason"])


def test_operator_workflow_binds_reviewed_proposal_before_publication(tmp_path):
    manifest = _passing_model_manifest(tmp_path, include_approval=False)
    proposal_hash = promotion_proposal_sha256(manifest)

    with pytest.raises(ValueError, match="operator-confirmed proposal SHA-256"):
        approve_and_write_promoted_model_manifest(
            tmp_path,
            manifest,
            approved_by="risk-owner",
            expected_proposal_sha256="0" * 64,
            approved_at_utc=APPROVED_AT,
        )
    assert not (tmp_path / "models" / "closing_tape_model.json").exists()

    destination = approve_and_write_promoted_model_manifest(
        tmp_path,
        manifest,
        approved_by="risk-owner",
        expected_proposal_sha256=proposal_hash,
        approved_at_utc=APPROVED_AT,
    )
    published = json.loads(destination.read_text(encoding="utf-8"))
    receipt = load_promotion_approval_receipt(
        tmp_path,
        published["promotion_approval"]["receipt_sha256"],
    )

    assert receipt["proposal_sha256"] == proposal_hash
    assert receipt["approved_by"] == "risk-owner"
    assert _model_gate(tmp_path)["passed"] is True


def test_model_gate_rejects_non_object_manifest_root(tmp_path):
    candidate = tmp_path / "models" / "candidate-manifest.json"
    candidate.parent.mkdir(parents=True)
    candidate.write_text("[]", encoding="utf-8")

    gate = _model_gate(tmp_path, manifest_path=candidate)

    assert gate == {
        "passed": False,
        "enabled": False,
        "reason": "invalid model manifest: root must be an object",
    }
