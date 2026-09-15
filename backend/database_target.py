"""Resolve the one configured persistent SQLite database used by MarketPin."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

from sqlalchemy.engine import make_url


class _DatabaseUrl(Protocol):
    database: str | None

    def get_backend_name(self) -> str: ...


def sqlite_database_path_from_url(url: _DatabaseUrl) -> Path:
    """Return a persistent SQLite path or fail before governance can split stores."""
    if url.get_backend_name() != "sqlite":
        raise RuntimeError(
            "closing-tape governance ledger currently requires the configured SQLite database"
        )
    database = str(url.database or "").strip()
    if not database or database == ":memory:":
        raise RuntimeError(
            "closing-tape governance ledger requires a persistent SQLite database path"
        )
    query = getattr(url, "query", {}) or {}
    uri_enabled = str(query.get("uri") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    memory_mode = str(query.get("mode") or "").strip().lower() == "memory"
    if database.lower().startswith("file:") or uri_enabled or memory_mode:
        raise RuntimeError(
            "closing-tape governance ledger requires a direct persistent SQLite file path"
        )
    return Path(database).resolve()


def configured_market_database_path(project_root: str | Path) -> Path:
    """Resolve DATABASE_URL, falling back to this checkout's canonical market DB."""
    root = Path(project_root).resolve()
    configured = os.getenv("DATABASE_URL")
    if not configured:
        return (root / "data" / "market_data.db").resolve()
    try:
        url = make_url(configured)
    except Exception as exc:
        raise RuntimeError("DATABASE_URL is invalid") from exc
    return sqlite_database_path_from_url(url)
