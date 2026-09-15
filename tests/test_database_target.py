import pytest
from sqlalchemy.engine import make_url

from backend.database_target import (
    configured_market_database_path,
    sqlite_database_path_from_url,
)


class _ConfiguredUrl:
    def __init__(self, backend: str, database: str | None):
        self._backend = backend
        self.database = database

    def get_backend_name(self) -> str:
        return self._backend


def test_configured_market_database_defaults_to_the_selected_project(
    monkeypatch, tmp_path
):
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert configured_market_database_path(tmp_path) == (
        tmp_path / "data" / "market_data.db"
    ).resolve()


def test_configured_market_database_honors_database_url(monkeypatch, tmp_path):
    configured = tmp_path / "configured" / "market.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{configured}")

    assert configured_market_database_path(tmp_path / "other-project") == configured.resolve()


@pytest.mark.parametrize(
    ("url", "message"),
    [
        (_ConfiguredUrl("postgresql", "marketpin"), "requires the configured SQLite"),
        (_ConfiguredUrl("sqlite", ":memory:"), "requires a persistent SQLite"),
    ],
)
def test_sqlite_database_path_rejects_split_or_ephemeral_targets(url, message):
    with pytest.raises(RuntimeError, match=message):
        sqlite_database_path_from_url(url)


@pytest.mark.parametrize(
    "configured",
    [
        "sqlite:///file:memdb1?mode=memory&cache=shared&uri=true",
        "sqlite:///file:market.db?uri=true",
    ],
)
def test_sqlite_database_path_rejects_uri_targets_that_direct_sqlite_would_misread(
    configured,
):
    with pytest.raises(RuntimeError, match="direct persistent SQLite file path"):
        sqlite_database_path_from_url(make_url(configured))
