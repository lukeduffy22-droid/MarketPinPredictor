"""
Production FastAPI application.
Institutional-grade real-time prediction API.
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.lifecycle import lifespan as api_lifespan
from backend.api.routers.admin import router as admin_router
from backend.api.routers.closing_tape import (
    READINESS_CACHE_TTL_SECONDS,
    request_training_readiness_refresh,
    router as closing_tape_router,
)
from backend.api.routers.gex import router as gex_router
from backend.api.routers.health import router as health_router
from backend.api.routers.orb import router as orb_router
from backend.api.routers.predict import router as predict_router
from backend.api.routers.passports import router as passports_router
from backend.closing_tape.governance import initialize_governance_ledger
from backend.config import API_HOST, API_PORT, CORS_ORIGINS, LOG_FORMAT, LOG_LEVEL
from backend.database import engine
from backend.database_target import sqlite_database_path_from_url

# Setup logging
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
if os.getenv("DATABENTO_DEBUG_LOGS", "0") != "1":
    logging.getLogger("databento").setLevel(logging.WARNING)


def _configured_governance_database_path():
    """Resolve the already-configured SQLAlchemy SQLite database safely."""
    return sqlite_database_path_from_url(engine.url)


async def _maintain_training_readiness() -> None:
    """Refresh retained readiness evidence independently of all read requests."""
    interval_seconds = max(1.0, READINESS_CACHE_TTL_SECONDS / 2.0)
    while True:
        await asyncio.sleep(interval_seconds)
        request_training_readiness_refresh()


@asynccontextmanager
async def lifespan(application: FastAPI):
    """Initialize additive governance tables after the core database schema."""
    async with api_lifespan(application):
        initialize_governance_ledger(_configured_governance_database_path())
        request_training_readiness_refresh()
        readiness_maintenance = asyncio.create_task(
            _maintain_training_readiness(),
            name="closing-tape-readiness-maintenance",
        )
        try:
            yield
        finally:
            readiness_maintenance.cancel()
            with suppress(asyncio.CancelledError):
                await readiness_maintenance


app = FastAPI(
    title="Market Pin Predictor API",
    description="Institutional-grade real-time index prediction",
    version="2.0.0",
    lifespan=lifespan,
    openapi_tags=[
        {"name": "health", "description": "Service health and runtime status endpoints."},
        {"name": "market-data", "description": "Market data buffer and gamma exposure endpoints."},
        {"name": "prediction", "description": "Prediction and calibration endpoints."},
        {"name": "admin", "description": "Operational maintenance and scoring endpoints."},
        {"name": "closing-tape", "description": "Observed OPRA tape, inferred-flow, and model-gate status."},
        {"name": "prediction-governance", "description": "Immutable forecast passports and grounded evidence analysis."},
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(gex_router)
app.include_router(orb_router)
app.include_router(predict_router)
app.include_router(admin_router)
app.include_router(closing_tape_router)
app.include_router(passports_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.app:app",
        host=API_HOST,
        port=API_PORT,
        reload=False,
        log_level=LOG_LEVEL.lower(),
    )
