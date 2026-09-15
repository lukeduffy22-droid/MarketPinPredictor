import hashlib

import pytest

from backend.closing_tape.close_evidence import resolve_verified_close_artifact


def test_verified_close_artifact_resolver_rehashes_exact_vault_file(tmp_path):
    content = b"official close fixture"
    digest = hashlib.sha256(content).hexdigest()
    artifact = tmp_path / "2026-08-25" / "SPX" / f"{digest}.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(content)

    resolved = resolve_verified_close_artifact(
        tmp_path,
        trading_date="2026-08-25",
        symbol="SPX",
        source_artifact_sha256=digest,
    )

    assert resolved == artifact.resolve()


def test_verified_close_artifact_resolver_rejects_missing_or_tampered_bytes(tmp_path):
    expected = hashlib.sha256(b"expected").hexdigest()
    with pytest.raises(ValueError, match="exactly one artifact"):
        resolve_verified_close_artifact(
            tmp_path,
            trading_date="2026-08-25",
            symbol="SPX",
            source_artifact_sha256=expected,
        )

    artifact = tmp_path / "2026-08-25" / "SPX" / f"{expected}.html"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="do not match the ledger SHA-256"):
        resolve_verified_close_artifact(
            tmp_path,
            trading_date="2026-08-25",
            symbol="SPX",
            source_artifact_sha256=expected,
        )
