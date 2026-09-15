from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.closing_tape.close_evidence import validate_official_close_reference


MAX_ARTIFACT_BYTES = 100 * 1024 * 1024
CONTENT_TYPE_SUFFIXES = {
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/json": ".json",
    "application/pdf": ".pdf",
    "text/html": ".html",
    "text/plain": ".txt",
}
SAFE_SUFFIXES = frozenset(CONTENT_TYPE_SUFFIXES.values()) | {".xml", ".xlsx", ".zip"}


def _artifact_suffix(url: str, content_type: str) -> str:
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in SAFE_SUFFIXES:
        return suffix
    media_type = str(content_type or "").split(";", 1)[0].strip().lower()
    return CONTENT_TYPE_SUFFIXES.get(media_type, ".bin")


def fetch_official_artifact(
    *,
    symbol: str,
    source: str,
    source_reference: str,
    trading_date: date,
    project_root: str | Path,
    max_bytes: int = MAX_ARTIFACT_BYTES,
    timeout_seconds: float = 30.0,
    http_session: Any = None,
) -> dict[str, object]:
    """Fetch official bytes into the immutable content-addressed close vault."""
    if max_bytes <= 0 or timeout_seconds <= 0:
        raise ValueError("max_bytes and timeout_seconds must be positive")
    normalized_symbol = str(symbol).strip().upper()
    normalized_source = str(source).strip().lower()
    requested_url = validate_official_close_reference(
        normalized_symbol, normalized_source, source_reference
    )
    client = http_session or requests
    response = client.get(
        requested_url,
        stream=True,
        allow_redirects=True,
        timeout=(min(10.0, timeout_seconds), timeout_seconds),
        headers={
            "Accept": "text/csv,application/json,application/pdf,text/html,text/plain,*/*",
            "Accept-Encoding": "identity",
            "User-Agent": "MarketPinPredictor-VerifiedCloseArtifact/1.0",
        },
    )
    try:
        response.raise_for_status()
        final_url = validate_official_close_reference(
            normalized_symbol, normalized_source, str(response.url)
        )
        content_length = response.headers.get("Content-Length")
        if content_length:
            try:
                if int(content_length) > max_bytes:
                    raise ValueError("official source artifact exceeds the configured size limit")
            except ValueError as exc:
                if "exceeds" in str(exc):
                    raise
                raise ValueError("official source returned an invalid Content-Length") from exc
    except Exception:
        response.close()
        raise

    directory = (
        Path(project_root).resolve() / "data" / "verified_close_sources"
        / trading_date.isoformat() / normalized_symbol
    )
    directory.mkdir(parents=True, exist_ok=True)
    temporary = directory / f".download.{uuid.uuid4().hex}.tmp"
    digest = hashlib.sha256()
    written = 0
    try:
        with temporary.open("xb") as stream:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                written += len(chunk)
                if written > max_bytes:
                    raise ValueError("official source artifact exceeds the configured size limit")
                digest.update(chunk)
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        if written <= 0:
            raise ValueError("official source artifact is empty")
        artifact_hash = digest.hexdigest()
        suffix = _artifact_suffix(final_url, response.headers.get("Content-Type", ""))
        destination = directory / f"{artifact_hash}{suffix}"
        if destination.exists():
            existing = hashlib.sha256(destination.read_bytes()).hexdigest()
            if existing != artifact_hash:
                raise ValueError("content-addressed vault file does not match its filename")
        else:
            os.replace(temporary, destination)
        return {
            "symbol": normalized_symbol,
            "trading_date": trading_date.isoformat(),
            "source": normalized_source,
            "source_reference": requested_url,
            "final_url": final_url,
            "source_artifact_sha256": artifact_hash,
            "source_artifact_path": str(destination),
            "artifact_bytes": written,
            "content_type": str(response.headers.get("Content-Type") or ""),
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    finally:
        response.close()
        if temporary.exists():
            temporary.unlink()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fetch an allowlisted official close artifact into MarketPin's immutable vault"
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--url", required=True)
    parser.add_argument("--trading-date", required=True)
    parser.add_argument("--project-root", default=str(PROJECT_ROOT))
    parser.add_argument("--max-bytes", type=int, default=MAX_ARTIFACT_BYTES)
    args = parser.parse_args(argv)
    result = fetch_official_artifact(
        symbol=args.symbol,
        source=args.source,
        source_reference=args.url,
        trading_date=date.fromisoformat(args.trading_date),
        project_root=args.project_root,
        max_bytes=args.max_bytes,
    )
    print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
