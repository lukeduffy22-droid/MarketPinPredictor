import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from backend import app as application


class _ConfiguredUrl:
    def __init__(self, backend: str, database: str | None):
        self._backend = backend
        self.database = database

    def get_backend_name(self) -> str:
        return self._backend


def test_lifespan_initializes_governance_on_the_configured_database(
    monkeypatch, tmp_path
):
    configured_database = tmp_path / "configured" / "market.db"
    events = []

    @asynccontextmanager
    async def core_lifespan(app):
        events.append(("core", app))
        yield

    monkeypatch.setattr(application, "api_lifespan", core_lifespan)
    monkeypatch.setattr(
        application,
        "engine",
        SimpleNamespace(url=_ConfiguredUrl("sqlite", str(configured_database))),
    )
    monkeypatch.setattr(
        application,
        "initialize_governance_ledger",
        lambda path: events.append(("governance", path)),
    )
    monkeypatch.setattr(
        application,
        "request_training_readiness_refresh",
        lambda: events.append(("readiness_refresh", None)),
    )
    test_app = FastAPI()

    async def enter_lifespan():
        async with application.lifespan(test_app):
            events.append(("ready", test_app))

    asyncio.run(enter_lifespan())

    assert events == [
        ("core", test_app),
        ("governance", configured_database.resolve()),
        ("readiness_refresh", None),
        ("ready", test_app),
    ]
    assert not configured_database.exists()


def test_readiness_maintenance_refreshes_without_a_get(monkeypatch):
    events = []

    class StopMaintenance(Exception):
        pass

    async def advance_clock(seconds):
        events.append(("clock_advanced", seconds))
        if len(events) > 2:
            raise StopMaintenance

    monkeypatch.setattr(application.asyncio, "sleep", advance_clock)
    monkeypatch.setattr(application, "READINESS_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(
        application,
        "request_training_readiness_refresh",
        lambda: events.append(("producer_refresh", None)),
    )

    with pytest.raises(StopMaintenance):
        asyncio.run(application._maintain_training_readiness())

    assert events == [
        ("clock_advanced", 150.0),
        ("producer_refresh", None),
        ("clock_advanced", 150.0),
    ]


@pytest.mark.parametrize(
    ("backend", "database", "message"),
    [
        ("postgresql", "marketpin", "requires the configured SQLite database"),
        ("sqlite", ":memory:", "requires a persistent SQLite database path"),
    ],
)
def test_configured_governance_database_rejects_unsupported_targets(
    monkeypatch, backend, database, message
):
    monkeypatch.setattr(
        application,
        "engine",
        SimpleNamespace(url=_ConfiguredUrl(backend, database)),
    )

    with pytest.raises(RuntimeError, match=message):
        application._configured_governance_database_path()
