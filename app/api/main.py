"""
FastAPI application with time-adaptive prediction endpoints.
Enforces cadence limits, freshness checks, and performance SLAs.
"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict
from datetime import datetime
import time
import logging

from app.utils.settings import settings
from app.utils.metrics import timed, predict_latency, snapshot
from app.utils.time_et import (
    now_et, minutes_to_close_et, is_regular_hours,
    is_power_hour, get_cadence_ms
)
from app.state.ring_buffers import INDEX_RINGS, get_latest_price
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

# Per-symbol last prediction timestamp for cadence enforcement
_last_prediction_ts: Dict[str, float] = {
    s: 0.0 for s in ("SPX", "NDX", "DJI", "RUT")
}

# Coefficient cache (loaded at startup)
_coefficients_cache: Dict[str, dict] = {}

class PredictionResponse(BaseModel):
    """0-day close prediction response"""
    symbol: str
    current_price: float
    predicted_close: float
    confidence_level: str  # high, medium, low
    tau_minutes: int
    rmse: Optional[float]
    mae: Optional[float]
    features: dict
    timestamp: str

class LevelsResponse(BaseModel):
    """Gamma levels response"""
    symbol: str
    current_price: float
    levels: list
    strongest_pin: Optional[float]
    timestamp: str

class EODPredictionResponse(BaseModel):
    """Advanced gamma-based EOD prediction response"""
    symbol: str
    current_price: float
    eod_estimate: float
    wwm: float
    pin_stability_index: float
    zero_gamma: float
    vacp: float
    hv10_points: Optional[float]
    num_walls: int
    num_pins: int
    timestamp: str

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
            _coefficients_cache[symbol] = {
                "beta_vwap": coeff.beta_vwap,
                "beta_gamma": coeff.beta_gamma,
                "beta_flow": coeff.beta_flow,
                "beta_microtrend": coeff.beta_microtrend,
                "intercept": coeff.intercept
            }
            log.info(f"Loaded coefficients for {symbol}")
        else:
            # Use default coefficients (VWAP-only model)
            _coefficients_cache[symbol] = {
                "beta_vwap": 1.0,
                "beta_gamma": 0.0,
                "beta_flow": 0.0,
                "beta_microtrend": 0.0,
                "intercept": 0.0
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
    
    # Start REST fallback (in case WebSocket fails)
    from app.ingest.rest_fallback import poll_rest_data
    asyncio.create_task(poll_rest_data())
    
    log.info("API ready - WebSocket stream + REST fallback starting")

@app.get("/healthz")
async def health_check():
    """
    Health check endpoint.
    Returns per-symbol freshness and ring buffer status.
    """
    status = {}
    
    for symbol in ("SPX", "NDX", "DJI", "RUT"):
        ring = INDEX_RINGS.get(symbol)
        
        is_fresh = ring.is_fresh(max_age_seconds=5) if ring else False
        length = ring.length_seconds if ring else 0
        
        status[symbol] = {
            "fresh": is_fresh,
            "index_seconds": length,
            "latest_price": get_latest_price(symbol)
        }
    
    overall_ok = all(s["fresh"] or not is_regular_hours(datetime.utcnow()) for s in status.values())
    
    return {
        "status": "ok" if overall_ok else "degraded",
        "symbols": status,
        "timestamp": now_et().isoformat()
    }

@app.get("/levels/eod")
async def get_gamma_levels(symbol: str) -> LevelsResponse:
    """
    Get gamma exposure levels for symbol.
    Uses OI cache (no REST calls).
    """
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    from app.state.oi_cache import oi_cache
    
    current_price = get_latest_price(symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {symbol}")
    
    # Get all strikes
    strikes = oi_cache.get_all_strikes(symbol)
    
    levels = []
    max_gex = 0
    strongest_pin = None
    
    for K, snapshot in strikes.items():
        # Simplified gamma calculation
        net_oi = snapshot.oi_call - snapshot.oi_put
        gex_estimate = net_oi * 100  # Rough estimate
        
        levels.append({
            "strike": K,
            "gamma_exposure": gex_estimate,
            "distance": abs(current_price - K)
        })
        
        if abs(gex_estimate) > max_gex:
            max_gex = abs(gex_estimate)
            strongest_pin = K
    
    # Sort by distance to current price
    levels.sort(key=lambda x: x["distance"])
    
    return LevelsResponse(
        symbol=symbol,
        current_price=current_price,
        levels=levels[:10],  # Top 10 nearest levels
        strongest_pin=strongest_pin,
        timestamp=now_et().isoformat()
    )

@app.get("/predict/close")
async def predict_close(symbol: str) -> PredictionResponse:
    """
    Predict EOD close price for symbol.
    Enforces cadence limits and freshness checks.
    """
    t0 = time.perf_counter()
    
    # Validate symbol
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    dt_utc = datetime.utcnow()
    
    # Check if market is open
    if not is_regular_hours(dt_utc):
        raise HTTPException(400, "Market is closed")
    
    # Check cadence enforcement
    tau = minutes_to_close_et(dt_utc)
    required_cadence_ms = get_cadence_ms(dt_utc)
    
    last_pred_ts = _last_prediction_ts.get(symbol, 0.0)
    elapsed_ms = (time.time() - last_pred_ts) * 1000
    
    if elapsed_ms < required_cadence_ms:
        raise HTTPException(429, f"Too many requests. Wait {int(required_cadence_ms - elapsed_ms)}ms")
    
    # Check freshness
    ring = INDEX_RINGS.get(symbol)
    if not ring or not ring.is_fresh(max_age_seconds=5):
        raise HTTPException(503, f"Stale data for {symbol}")
    
    if ring.length_seconds < 300:
        raise HTTPException(503, f"Insufficient data for {symbol}")
    
    # Get current price
    current_price = get_latest_price(symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {symbol}")
    
    # Circuit breaker: enforce time budget for feature computation
    feature_deadline = time.perf_counter() + 0.15  # 150ms budget
    
    # Compute features with timeout protection
    try:
        features = compute_all_features(symbol)
        
        # Check if we exceeded time budget
        if time.perf_counter() > feature_deadline:
            log.warning(f"Feature computation exceeded 150ms budget for {symbol}")
            raise HTTPException(503, "Feature computation too slow")
            
    except Exception as e:
        log.error(f"Feature computation failed for {symbol}: {e}")
        raise HTTPException(503, f"Feature computation error: {str(e)}")
    
    # Get coefficients
    coeffs = _coefficients_cache.get(symbol, {})
    
    # Compute prediction using Ridge model
    vwap_dev = features["vwap_deviation"]
    microtrend = features["microtrend"]
    gamma_pin = features["gamma_pin_strength"]
    flow_urgency = features["flow_urgency"]
    
    # Time-adaptive weighting (increase weight of faster signals near close)
    if tau <= 15:  # Power hour
        tau_weight_micro = 1.5
        tau_weight_flow = 1.3
    elif tau <= 30:
        tau_weight_micro = 1.2
        tau_weight_flow = 1.1
    else:
        tau_weight_micro = 1.0
        tau_weight_flow = 1.0
    
    # Ridge prediction
    predicted_close = (
        current_price +
        coeffs.get("beta_vwap", 1.0) * vwap_dev * current_price +
        coeffs.get("beta_microtrend", 0.0) * microtrend * tau * 60 * tau_weight_micro +
        coeffs.get("beta_gamma", 0.0) * gamma_pin * 10 +
        coeffs.get("beta_flow", 0.0) * flow_urgency * 5 * tau_weight_flow +
        coeffs.get("intercept", 0.0)
    )
    
    # Get RMSE for confidence level
    rmse_bucket = get_rmse_for_tau(symbol, tau)
    
    if rmse_bucket:
        rmse = rmse_bucket.rmse
        mae = rmse_bucket.mae
        
        # Classify confidence based on RMSE
        if rmse < 10:
            confidence = "high"
        elif rmse < 20:
            confidence = "medium"
        else:
            confidence = "low"
    else:
        rmse = None
        mae = None
        confidence = "unknown"
    
    # Update last prediction timestamp
    _last_prediction_ts[symbol] = time.time()
    
    # Track latency
    latency_ms = (time.perf_counter() - t0) * 1000
    predict_latency.record(latency_ms)
    
    return PredictionResponse(
        symbol=symbol,
        current_price=current_price,
        predicted_close=predicted_close,
        confidence_level=confidence,
        tau_minutes=tau,
        rmse=rmse,
        mae=mae,
        features=features,
        timestamp=now_et().isoformat()
    )

@app.get("/predict/eod")
async def predict_eod(symbol: str) -> EODPredictionResponse:
    """
    Advanced gamma-based EOD prediction using Wall-Weighted Magnet (WWM),
    Pin Stability Index (PSI), Zero-Gamma, and Volatility-Adjusted Close Predictor (VACP).
    
    Designed to reduce prediction error from 5-10 points to 1-3 points.
    """
    # Validate symbol
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    # Convert symbol to ticker format for Polygon API
    ticker_map = {
        "SPX": "I:SPX",
        "NDX": "I:NDX",
        "DJI": "I:DJI",
        "RUT": "I:RUT"
    }
    ticker = ticker_map[symbol]
    
    # Get current price
    current_price = get_latest_price(symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {symbol}")
    
    try:
        # Import EOD prediction modules
        from app.utils.eod_data_integration import get_eod_prediction_inputs
        from app.utils.gamma_eod_predictor import predict_eod_close
        
        # Get all required inputs
        inputs = get_eod_prediction_inputs(
            api_key=settings.POLYGON_API_KEY,
            ticker=ticker,
            spot_price=current_price,
            trading_date=None  # Uses today
        )
        
        # Validate we have minimum required data
        if not inputs['walls']:
            raise HTTPException(503, "No gamma walls data available")
        
        if not inputs['pin_history']:
            raise HTTPException(503, "No gamma pin snapshots available for today")
        
        # Run EOD prediction
        result = predict_eod_close(
            walls=inputs['walls'],
            pin_history=inputs['pin_history'],
            zero_gamma=inputs['zero_gamma'],
            spot_prices=inputs['spot_prices'],
            intraday_high=inputs['intraday_high'],
            intraday_low=inputs['intraday_low'],
            hv10_points=inputs['hv10_points']
        )
        
        return EODPredictionResponse(
            symbol=symbol,
            current_price=current_price,
            eod_estimate=result.eod_estimate,
            wwm=result.wwm,
            pin_stability_index=result.pin_stability_index,
            zero_gamma=result.zero_gamma,
            vacp=result.vacp,
            hv10_points=inputs['hv10_points'],
            num_walls=len(inputs['walls']),
            num_pins=len(inputs['pin_history']),
            timestamp=now_et().isoformat()
        )
        
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        log.error(f"EOD prediction failed for {symbol}: {e}")
        raise HTTPException(503, f"EOD prediction error: {str(e)}")

@app.get("/metrics")
async def get_metrics():
    """Performance metrics endpoint"""
    latencies = predict_latency.get_percentiles()
    mem = snapshot("current")
    
    return {
        "latency": latencies,
        "memory": mem,
        "timestamp": now_et().isoformat()
    }
