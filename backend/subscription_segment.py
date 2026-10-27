"""Append-only evidence for an additive, partial-session quote capture request."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path


def write_segment_receipt(root: Path, evidence: dict) -> dict[str, str]:
    encoded = (json.dumps(evidence, sort_keys=True, separators=(",", ":"),
                          allow_nan=False) + "\n").encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    root = Path(root).resolve()
    path = root / f"{digest}.json"
    root.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError:
        if path.read_bytes() != encoded:
            raise ValueError("subscription segment receipt identity conflict")
    return {"sha256": digest, "path": str(path)}
