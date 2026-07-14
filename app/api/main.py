"""
FastAPI application with time-adaptive prediction endpoints.
Enforces cadence limits, freshness checks, and performance SLAs.
"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, List
import os
import json
from datetime import datetime
import time
import logging

from app.utils.settings import settings
from app.utils.metrics import timed, predict_latency, snapshot
from app.utils.time_et import (
    now_et, minutes_to_close_et, is_regular_hours,
    is_power_hour, get_cadence_ms
)
from app.state.ring_buffers import INDEX_RINGS, get_latest_price, get_latest_price_with_fallback
from app.features.calculators import compute_all_features
from app.models.db_models import load_coefficients, get_rmse_for_tau

log = logging.getLogger("api")

app = FastAPI(title="0-Day Index Predictor")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from app.api.routes import accuracy, ai, debug, historical, observability, predictions

app.include_router(predictions.router)
app.include_router(ai.router)
app.include_router(observability.router)
app.include_router(accuracy.router)
app.include_router(debug.router)
app.include_router(historical.router)

@app.get("/collector/qc-status")
async def get_qc_status():
    """
    Returns the latest QC status as JSON (written by tools/qc_status.py).
    """
    # Path should match where qc_status.py writes the status file
    status_path = os.environ.get("QC_STATUS_PATH", "/app/exports/collected/parquet/qc_status.json")
    if not os.path.exists(status_path):
        raise HTTPException(404, f"QC status file not found: {status_path}")
    try:
        with open(status_path, "r") as f:
            status = json.load(f)
        return JSONResponse(content=status)
    except Exception as e:
        raise HTTPException(500, f"Failed to read QC status: {str(e)}")

from app.api import state as api_state

# Backward-compatible aliases for tests/tools that import these from main.
_last_prediction_ts = api_state.last_prediction_ts
_coefficients_cache = api_state.coefficients_cache

@app.on_event("startup")
async def startup():
    """Initialize application - load coefficients from DB"""
    log.info("Starting 0-day predictor API")
    
    # Initialize database
    from app.models.db_models import init_db
    init_db()
    
    # Load coefficients for all symbols
    for symbol in ("SPX", "NDX", "DJI", "RUT"):
        coeff = load_coefficients(symbol)
        
        if coeff:
            api_state.coefficients_cache[symbol] = {
                "beta_vwap": coeff.beta_vwap,
                "beta_gamma": coeff.beta_gamma,
                "beta_flow": coeff.beta_flow,
                "beta_microtrend": coeff.beta_microtrend,
                "intercept": coeff.intercept
            }
            api_state.model_metadata_cache[symbol] = {
                "coefficients_source": "database",
                "coefficients_updated_at": coeff.updated_at.isoformat() if coeff.updated_at else None,
                "coefficients_sample_size": coeff.sample_size,
            }
            log.info(f"Loaded coefficients for {symbol}")
        else:
            # Use default coefficients (VWAP-only model)
            api_state.coefficients_cache[symbol] = {
                "beta_vwap": 1.0,
                "beta_gamma": 0.0,
                "beta_flow": 0.0,
                "beta_microtrend": 0.0,
                "intercept": 0.0
            }
            api_state.model_metadata_cache[symbol] = {
                "coefficients_source": "fallback-defaults",
                "coefficients_updated_at": None,
                "coefficients_sample_size": 0,
            }
            log.warning(f"No coefficients found for {symbol}, using defaults")
    
    # Start OI cache refresh background task
    from app.state.oi_cache import schedule_oi_refresh
    import asyncio
    asyncio.create_task(schedule_oi_refresh())
    
    # Start aggregation flush task
    from app.ingest.websocket_aggregator import flush_aggregates
    asyncio.create_task(flush_aggregates())
    
    # Start WebSocket stream for data ingestion
    from app.ingest.websocket_stream import start_websocket_stream
    asyncio.create_task(start_websocket_stream())
    
    # Start dedicated Options WebSocket stream for real-time gamma updates
    from app.ingest.options_websocket_stream import start_options_websocket_stream
    asyncio.create_task(start_options_websocket_stream())
    
    # Start market data fallback (Databento or Polygon based on settings)
    from app.ingest.rest_fallback import start_market_data_fallback, load_cached_snapshots
    asyncio.create_task(start_market_data_fallback())
    asyncio.create_task(load_cached_snapshots())  # Secondary fallback from DB
    
    # Start gamma pin scheduler to save snapshots throughout the day
    from gamma_scheduler import start_gamma_scheduler
    start_gamma_scheduler()
    log.info("Gamma pin scheduler started")
    
    log.info("API ready - WebSocket streams + provider-aware fallback poller starting")

@app.get("/health")
@app.get("/healthz")
async def health_check():
    """
    Health check endpoint.
    Returns per-symbol freshness and ring buffer status.
    """
    import time
    from app.ingest.rest_fallback import is_rest_only_mode, get_market_data_provider
    max_age = 5  # 5 second freshness threshold (1s REST polling)
    
    status = {}
    current_time = int(time.time())
    
    for symbol in ("SPX", "NDX", "DJI", "RUT"):
        ring = INDEX_RINGS.get(symbol)
        
        is_fresh = ring.is_fresh(max_age_seconds=max_age) if ring else False
        buffer_length = ring.length_seconds if ring else 0
        
        # Calculate data age (how old the latest tick is)
        data_age = 0
        if ring and ring.q:
            latest_ts = ring.q[-1][0]
            data_age = current_time - latest_ts
        
        status[symbol] = {
            "fresh": is_fresh,
            "data_age_seconds": data_age,
            "buffer_length": buffer_length,
            "latest_price": get_latest_price(symbol),
            "mode": "REST" if is_rest_only_mode() else "WebSocket"
        }
    
    overall_ok = all(s["fresh"] or not is_regular_hours(datetime.utcnow()) for s in status.values())
    
    return {
        "status": "ok" if overall_ok else "degraded",
        "market_data_provider": get_market_data_provider(),
        "symbols": status,
        "timestamp": now_et().isoformat()
    }
