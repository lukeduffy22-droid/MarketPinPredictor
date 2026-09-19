"""Append-only receipts for byte-proven vault aliases; never modifies tapes."""
from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path

from .close_evidence import (
    MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
    VERIFIED_CLOSE_SOURCES_BY_SYMBOL,
    validate_source_artifact_sha256,
)


def _inventory(vault_root, trading_date, symbol, source_artifact_sha256, max_bytes):
    root = Path(vault_root).resolve()
    day = date.fromisoformat(str(trading_date)).isoformat()
    family = str(symbol).strip().upper()
    if family not in VERIFIED_CLOSE_SOURCES_BY_SYMBOL:
        raise ValueError("unsupported close family")
    digest = validate_source_artifact_sha256(source_artifact_sha256)
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    directory = root / day / family
    directory.resolve().relative_to(root)
    paths = sorted(directory.glob(f"{digest}.*"))
    if not paths:
        raise ValueError("verified close vault must contain exactly one artifact or proven aliases")
    for path in paths:
        # Aliases must be ordinary retained files, not links to another identity.
        if path.resolve() != path or not path.is_file():
            raise ValueError("verified close registry requires canonical regular files")
        path.resolve().relative_to(root)
        size = path.stat().st_size
        if not 0 < size <= max_bytes:
            raise ValueError("verified close artifact size is outside the allowed range")
        actual = hashlib.sha256()
        total = 0
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                total += len(chunk)
                if total > max_bytes:
                    raise ValueError("verified close artifact exceeds size limit")
                actual.update(chunk)
        if actual.hexdigest() != digest:
            raise ValueError("verified close vault bytes do not match the ledger SHA-256")
    payload = {
        "contract": "verified-close-aliases-v1",
        "trading_date": day,
        "symbol": family,
        "source_artifact_sha256": digest,
        "artifacts": [p.relative_to(root).as_posix() for p in paths],
        "selected_artifact": paths[0].relative_to(root).as_posix(),
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    receipt_hash = hashlib.sha256(encoded).hexdigest()
    receipt = root / "_registry" / day / family / digest / f"{receipt_hash}.json"
    receipt.resolve().relative_to(root)
    return paths[0], receipt, encoded


def reconcile_verified_close_artifact(
    vault_root, *, trading_date, symbol, source_artifact_sha256,
    max_bytes=MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
) -> Path:
    """Append an idempotent deterministic receipt only after hashing every alias.

    This proves byte identity only. Callers must still enforce official ledger,
    date/family, correction lineage, tape completion and model eligibility gates.
    """
    _selected, receipt, encoded = _inventory(
        vault_root, trading_date, symbol, source_artifact_sha256, max_bytes
    )
    receipt.parent.mkdir(parents=True, exist_ok=True)
    try:
        with receipt.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if receipt.read_bytes() != encoded:
            raise ValueError("verified close registry receipt conflicts with its identity")
    return receipt


def resolve_registered_close_artifact(
    vault_root, *, trading_date, symbol, source_artifact_sha256,
    max_bytes=MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
) -> Path:
    """Reprove current inventory and require its exact immutable receipt."""
    selected, receipt, encoded = _inventory(
        vault_root, trading_date, symbol, source_artifact_sha256, max_bytes
    )
    if not receipt.is_file() or receipt.read_bytes() != encoded:
        raise ValueError(
            "verified close vault must contain exactly one artifact or an exact reconciliation receipt"
        )
    return selected
