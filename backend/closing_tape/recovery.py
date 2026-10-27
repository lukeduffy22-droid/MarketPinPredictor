"""Bounded live-only tape recovery; never repair or reclassify an earlier tape."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import uuid


MAX_RECOVERY_SEGMENTS = 2
RECOVERY_DELAYS_SECONDS = (30.0, 60.0, 120.0, 240.0, 240.0, 240.0)


def transport_failure_kind(exc: BaseException) -> str | None:
    text = str(exc).casefold()
    if any(word in text for word in (
        "license", "unauthorized", "authentication", "connection limit", "too many",
        "disk", "permission", "stream write", "aggregation", "queue",
    )):
        return None
    if "getaddrinfo" in text:
        return "dns_failure"
    if "gateway timeout" in text:
        return "gateway_timeout"
    if any(word in text for word in ("10054", "1236", "connection reset", "connection lost")):
        return "connection_lost"
    if isinstance(exc, (TimeoutError, ConnectionError)) or (
        "connection to " in text and "timed out" in text
    ):
        return "transport_timeout"
    return None


def write_recovery_receipt(directory: Path, payload: dict) -> dict[str, str]:
    """Exclusive-create receipt before a new connection; never replace evidence."""
    directory.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, sort_keys=True, indent=2, default=str) + "\n").encode()
    digest = hashlib.sha256(raw).hexdigest()
    path = directory / f"{uuid.uuid4().hex}-{digest}.json"
    with path.open("xb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(0o444)
    return {"path": str(path), "sha256": digest}
