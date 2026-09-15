from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse
import re

from .config import _market_times


VERIFIED_CLOSE_SOURCES_BY_SYMBOL = {
    "SPX": frozenset({"cboe-official", "sp-global-official"}),
    "NDX": frozenset({"nasdaq-official"}),
    "RUT": frozenset({"cboe-official", "ftse-russell-official"}),
    "VIX": frozenset({"cboe-official"}),
    "SPY": frozenset({"nyse-arca-official"}),
}

OFFICIAL_REFERENCE_HOSTS_BY_SOURCE = {
    "sp-global-official": frozenset({"spglobal.com"}),
    "nasdaq-official": frozenset({"nasdaq.com"}),
    "ftse-russell-official": frozenset({"lseg.com", "ftserussell.com"}),
    "cboe-official": frozenset({"cboe.com"}),
    "nyse-arca-official": frozenset({"nyse.com"}),
}

MAX_VERIFIED_CLOSE_ARTIFACT_BYTES = 100 * 1024 * 1024
MAX_VERIFIED_CLOSE_FUTURE_SKEW = timedelta(minutes=5)
UTC = timezone.utc


def validate_verified_close_observed_at(
    trading_date: str | date,
    observed_at_utc: str | datetime,
    *,
    now_utc: datetime | None = None,
) -> datetime:
    """Validate official-close chronology against the exchange session and wall clock."""

    trading_day = (
        trading_date
        if isinstance(trading_date, date)
        else date.fromisoformat(str(trading_date))
    )
    try:
        observed = (
            observed_at_utc
            if isinstance(observed_at_utc, datetime)
            else datetime.fromisoformat(str(observed_at_utc).replace("Z", "+00:00"))
        )
    except ValueError as exc:
        raise ValueError("observed_at_utc is invalid") from exc
    if observed.tzinfo is None:
        raise ValueError("observed_at_utc must include a timezone")
    observed = observed.astimezone(UTC)
    reference_now = now_utc or datetime.now(UTC)
    if reference_now.tzinfo is None:
        raise ValueError("current verification time must include a timezone")
    reference_now = reference_now.astimezone(UTC)
    _cash_open, _analysis_due, cash_close, _stop = _market_times(trading_day)
    if observed < cash_close.astimezone(UTC):
        raise ValueError("observed_at_utc cannot precede the official cash-session close")
    if observed > reference_now + MAX_VERIFIED_CLOSE_FUTURE_SKEW:
        raise ValueError("observed_at_utc exceeds the allowed future clock skew")
    return observed


def validate_source_artifact_sha256(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError("source_artifact_sha256 must be a 64-character hexadecimal SHA-256")
    return normalized


def validate_official_close_reference(symbol: str, source: str, reference: str) -> str:
    normalized_symbol = str(symbol).strip().upper()
    normalized_source = str(source).strip().lower()
    if normalized_source not in VERIFIED_CLOSE_SOURCES_BY_SYMBOL.get(normalized_symbol, ()):
        raise ValueError(
            f"{normalized_source!r} is not an approved official-close source for {normalized_symbol}"
        )
    normalized_reference = str(reference or "").strip()
    if not normalized_reference:
        raise ValueError("source_reference is required for verified close evidence")
    parsed = urlparse(normalized_reference)
    host = (parsed.hostname or "").lower().rstrip(".")
    allowed_hosts = OFFICIAL_REFERENCE_HOSTS_BY_SOURCE[normalized_source]
    if parsed.scheme.lower() != "https" or not any(
        host == allowed or host.endswith(f".{allowed}") for allowed in allowed_hosts
    ):
        expected = ", ".join(sorted(allowed_hosts))
        raise ValueError(
            f"source_reference must be an HTTPS URL on the official {expected} domain"
        )
    return normalized_reference


def resolve_verified_close_artifact(
    vault_root: str | Path,
    *,
    trading_date: str | date,
    symbol: str,
    source_artifact_sha256: str,
    max_bytes: int = MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
) -> Path:
    """Resolve and stream-rehash one deterministic official-close vault artifact."""
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    trading_day = (
        trading_date if isinstance(trading_date, date)
        else date.fromisoformat(str(trading_date))
    )
    normalized_symbol = str(symbol).strip().upper()
    if normalized_symbol not in VERIFIED_CLOSE_SOURCES_BY_SYMBOL:
        raise ValueError(f"unsupported close family {normalized_symbol!r}")
    artifact_hash = validate_source_artifact_sha256(source_artifact_sha256)
    root = Path(vault_root).resolve()
    directory = (root / trading_day.isoformat() / normalized_symbol).resolve()
    try:
        directory.relative_to(root)
    except ValueError as exc:
        raise ValueError("verified close artifact directory escapes the vault") from exc
    matches = sorted(
        path.resolve()
        for path in directory.glob(f"{artifact_hash}.*")
        if path.is_file()
    ) if directory.is_dir() else []
    if len(matches) != 1:
        raise ValueError(
            "verified close vault must contain exactly one artifact for the content hash"
        )
    artifact = matches[0]
    try:
        artifact.relative_to(root)
    except ValueError as exc:
        raise ValueError("verified close artifact escapes the vault") from exc
    artifact_bytes = artifact.stat().st_size
    if artifact_bytes <= 0 or artifact_bytes > max_bytes:
        raise ValueError("verified close artifact size is outside the allowed range")
    digest = hashlib.sha256()
    with artifact.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != artifact_hash:
        raise ValueError("verified close vault bytes do not match the ledger SHA-256")
    return artifact


def resolve_verified_close_artifact_by_hash(
    vault_root: str | Path,
    *,
    source_artifact_sha256: str,
    max_bytes: int = MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
) -> Path:
    """Resolve exactly one canonical date/family artifact from only its hash."""

    artifact_hash = validate_source_artifact_sha256(source_artifact_sha256)
    root = Path(vault_root).resolve()
    matches = sorted(
        path.resolve()
        for path in root.glob(f"*/*/{artifact_hash}.*")
        if path.is_file()
    ) if root.is_dir() else []
    if len(matches) != 1:
        raise ValueError(
            "verified close vault must contain exactly one artifact for the content hash"
        )
    artifact = matches[0]
    try:
        relative = artifact.relative_to(root)
    except ValueError as exc:
        raise ValueError("verified close artifact escapes the vault") from exc
    if len(relative.parts) != 3:
        raise ValueError("verified close artifact path is not canonical")
    trading_day, symbol, _filename = relative.parts
    return resolve_verified_close_artifact(
        root,
        trading_date=trading_day,
        symbol=symbol,
        source_artifact_sha256=artifact_hash,
        max_bytes=max_bytes,
    )


def resolve_verified_close_artifacts_by_hashes(
    vault_root: str | Path,
    *,
    source_artifact_sha256s: tuple[str, ...],
    max_bytes: int = MAX_VERIFIED_CLOSE_ARTIFACT_BYTES,
) -> dict[str, Path]:
    """Resolve a set of canonical close artifacts with one vault inventory."""

    requested = tuple(
        sorted(validate_source_artifact_sha256(value) for value in source_artifact_sha256s)
    )
    if not requested or requested != tuple(sorted(set(requested))):
        raise ValueError("verified close artifact identities must be unique")
    root = Path(vault_root).resolve()
    matches: dict[str, list[Path]] = {value: [] for value in requested}
    if root.is_dir():
        for path in root.glob("*/*/*"):
            if not path.is_file():
                continue
            artifact_hash = path.name.split(".", 1)[0].lower()
            if artifact_hash in matches:
                matches[artifact_hash].append(path.resolve())
    resolved: dict[str, Path] = {}
    for artifact_hash, paths in matches.items():
        if len(paths) != 1:
            raise ValueError(
                "verified close vault must contain exactly one artifact for the content hash"
            )
        relative = paths[0].relative_to(root)
        if len(relative.parts) != 3:
            raise ValueError("verified close artifact path is not canonical")
        trading_day, symbol, _filename = relative.parts
        resolved[artifact_hash] = resolve_verified_close_artifact(
            root,
            trading_date=trading_day,
            symbol=symbol,
            source_artifact_sha256=artifact_hash,
            max_bytes=max_bytes,
        )
    return resolved
