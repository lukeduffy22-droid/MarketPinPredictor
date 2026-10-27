from pathlib import Path

import pytest

from backend.config import resolve_database_url


def test_database_url_rejects_relative_sqlite_target():
    with pytest.raises(RuntimeError, match="absolute SQLite path"):
        resolve_database_url("sqlite:///market.db")


def test_database_url_accepts_absolute_sqlite_target(tmp_path):
    target = (tmp_path / "data" / "market_data.db").resolve()
    value = f"sqlite:///{target.as_posix()}"

    assert resolve_database_url(value) == value
