import hashlib

import pytest

from backend.closing_tape.close_evidence import (
    resolve_verified_close_artifact,
    resolve_verified_close_artifact_by_hash,
    resolve_verified_close_artifacts_by_hashes,
)
from backend.closing_tape.close_registry import reconcile_verified_close_artifact


@pytest.fixture
def artifact(tmp_path):
    content = b"retained official history fixture"
    digest = hashlib.sha256(content).hexdigest()
    path = tmp_path / "2026-08-25" / "SPX" / f"{digest}.html"
    path.parent.mkdir(parents=True)
    path.write_bytes(content)
    identity = dict(trading_date="2026-08-25", symbol="SPX", source_artifact_sha256=digest)
    return path, identity


def test_duplicate_requires_explicit_byte_proven_append_only_receipt(tmp_path, artifact):
    path, identity = artifact
    alias = path.with_suffix(".txt")
    alias.write_bytes(path.read_bytes())
    before = {p: p.read_bytes() for p in (path, alias)}
    with pytest.raises(ValueError, match="exactly one artifact"):
        resolve_verified_close_artifact(tmp_path, **identity)
    receipt = reconcile_verified_close_artifact(tmp_path, **identity)
    original = receipt.read_bytes(), receipt.stat().st_mtime_ns
    assert reconcile_verified_close_artifact(tmp_path, **identity) == receipt
    assert (receipt.read_bytes(), receipt.stat().st_mtime_ns) == original
    assert resolve_verified_close_artifact(tmp_path, **identity) == path
    assert all(p.read_bytes() == content for p, content in before.items())
    # A new alias needs a new receipt. Old receipts are never replaced.
    path.with_suffix(".csv").write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match="exact reconciliation receipt"):
        resolve_verified_close_artifact(tmp_path, **identity)
    new = reconcile_verified_close_artifact(tmp_path, **identity)
    assert new != receipt
    assert (receipt.read_bytes(), receipt.stat().st_mtime_ns) == original


def test_missing_artifact_never_creates_registry(tmp_path, artifact):
    path, identity = artifact
    path.unlink()
    with pytest.raises(ValueError, match="exactly one artifact"):
        reconcile_verified_close_artifact(tmp_path, **identity)
    assert not (tmp_path / "_registry").exists()


def test_valid_artifact_receipt_does_not_replace_normal_resolution(tmp_path, artifact):
    path, identity = artifact
    assert resolve_verified_close_artifact(tmp_path, **identity) == path
    reconcile_verified_close_artifact(tmp_path, **identity)
    assert resolve_verified_close_artifact(tmp_path, **identity) == path


def test_conflicting_alias_and_later_tampering_fail_closed(tmp_path, artifact):
    path, identity = artifact
    alias = path.with_suffix(".txt")
    alias.write_bytes(b"wrong bytes")
    with pytest.raises(ValueError, match="ledger SHA-256"):
        reconcile_verified_close_artifact(tmp_path, **identity)
    assert not (tmp_path / "_registry").exists()
    alias.write_bytes(path.read_bytes())
    receipt = reconcile_verified_close_artifact(tmp_path, **identity)
    alias.write_bytes(b"changed after registration")
    with pytest.raises(ValueError, match="ledger SHA-256"):
        resolve_verified_close_artifact(tmp_path, **identity)
    assert receipt.is_file()


def test_hash_only_resolution_requires_receipts_for_every_session(tmp_path, artifact):
    path, identity = artifact
    second = tmp_path / "2026-08-26" / "SPX" / path.name
    second.parent.mkdir(parents=True)
    second.write_bytes(path.read_bytes())
    digest = identity["source_artifact_sha256"]
    reconcile_verified_close_artifact(tmp_path, **identity)
    with pytest.raises(ValueError, match="exact reconciliation receipt"):
        resolve_verified_close_artifact_by_hash(tmp_path, source_artifact_sha256=digest)
    reconcile_verified_close_artifact(tmp_path, **{**identity, "trading_date": "2026-08-26"})
    assert resolve_verified_close_artifact_by_hash(tmp_path, source_artifact_sha256=digest) == path
    assert resolve_verified_close_artifacts_by_hashes(tmp_path, source_artifact_sha256s=(digest,)) == {digest: path}
    second.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="ledger SHA-256"):
        resolve_verified_close_artifacts_by_hashes(tmp_path, source_artifact_sha256s=(digest,))


def test_receipt_tampering_is_not_overwritten(tmp_path, artifact):
    path, identity = artifact
    path.with_suffix(".txt").write_bytes(path.read_bytes())
    receipt = reconcile_verified_close_artifact(tmp_path, **identity)
    receipt.write_bytes(b"invalid receipt")
    with pytest.raises(ValueError, match="exact reconciliation receipt"):
        resolve_verified_close_artifact(tmp_path, **identity)
    with pytest.raises(ValueError, match="conflicts"):
        reconcile_verified_close_artifact(tmp_path, **identity)
    assert receipt.read_bytes() == b"invalid receipt"


def test_reconciliation_cli_reports_valid_and_missing_artifacts(tmp_path, artifact, capsys):
    import json
    from tools.reconcile_verified_close_artifacts import main

    path, identity = artifact
    args = ["--vault-root", str(tmp_path), "--trading-date", identity["trading_date"],
            "--symbol", identity["symbol"], "--source-artifact-sha256",
            identity["source_artifact_sha256"]]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "reconciled"
    path.unlink()
    assert main(args) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "blocked"


def test_identity_and_size_limits_cannot_be_bypassed_by_receipts(tmp_path, artifact):
    path, identity = artifact
    path.with_suffix(".txt").write_bytes(path.read_bytes())
    reconcile_verified_close_artifact(tmp_path, **identity)
    with pytest.raises(ValueError, match="size"):
        resolve_verified_close_artifact(tmp_path, **identity, max_bytes=1)
    with pytest.raises(ValueError):
        resolve_verified_close_artifact(tmp_path, **{**identity, "trading_date": "2026-08-26"})
    with pytest.raises(ValueError):
        resolve_verified_close_artifact(tmp_path, **{**identity, "symbol": "NDX"})
