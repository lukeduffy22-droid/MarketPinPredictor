from __future__ import annotations

from pathlib import Path


def sqlite_read_only_uri(
    path: str | Path,
    *,
    immutable: bool = False,
) -> str:
    """Return an escaped SQLite file URI for an existing evidence database."""

    uri = Path(path).resolve().as_uri()
    suffix = "?mode=ro"
    if immutable:
        suffix += "&immutable=1"
    return uri + suffix
