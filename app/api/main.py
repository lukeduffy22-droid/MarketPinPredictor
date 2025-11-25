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
    from app.ingest.rest_fallback import load_cached_snapshots
    asyncio.create_task(load_cached_snapshots())
    
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
            api_key=settings.polygon_api_key,
            ticker=ticker,
            spot_price=current_price,
            trading_date=None  # Uses today
        )
        
        # Validate we have minimum required data
        if not inputs['walls']:
            raise HTTPException(503, "No gamma walls data available")
        
        if not inputs['pin_history']:
            raise HTTPException(503, "No gamma pin snapshots available for today")
        
        # Run EOD prediction with multi-expiry data if available
        result = predict_eod_close(
            walls=inputs['walls'],
            pin_history=inputs['pin_history'],
            zero_gamma=inputs['zero_gamma'],
            spot_prices=inputs['spot_prices'],
            intraday_high=inputs['intraday_high'],
            intraday_low=inputs['intraday_low'],
            hv10_points=inputs['hv10_points'],
            multi_expiry_aggregate_pin=inputs.get('multi_expiry_aggregate_pin')
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

@app.get("/gamma/multi-expiry")
async def get_multi_expiry_gamma(symbol: str = "SPX", max_dte: int = 7):
    """
    Get multi-expiration gamma exposure analysis (0-DTE through 7-DTE).
    
    Returns time-weighted gamma exposure by expiration date, unified gamma walls,
    and an aggregate pin strike based on all near-term expirations.
    """
    # Normalize symbol - strip I: prefix if present
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price(clean_symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {clean_symbol}")
    
    try:
        import options_gamma
        
        analysis = options_gamma.get_multi_expiry_analysis(
            api_key=settings.polygon_api_key,
            underlying=clean_symbol,
            spot_price=current_price,
            max_dte=max_dte
        )
        
        if not analysis:
            raise HTTPException(503, "Unable to fetch multi-expiry gamma data")
        
        # Analysis is already JSON-safe from get_multi_expiry_analysis
        return {
            "symbol": clean_symbol,
            "current_price": float(current_price),
            "aggregate_pin": analysis['aggregate_pin'],
            "gamma_by_expiry": analysis['gamma_by_expiry'],
            "unified_walls": analysis['unified_walls'],
            "time_weights": {str(k): float(v) for k, v in analysis['time_weights'].items()},
            "is_mock_data": analysis.get('is_mock_data', False),
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"Multi-expiry gamma failed for {symbol}: {e}")
        raise HTTPException(503, f"Multi-expiry gamma error: {str(e)}")


@app.get("/predict/ai-enhanced")
async def get_ai_enhanced_prediction_endpoint(symbol: str = "SPX"):
    """
    Get AI-enhanced EOD prediction with critique and adjustment.
    
    This endpoint:
    1. Gets the base model's EOD prediction
    2. Sends it to AI for analysis with all live market context
    3. Returns both original and AI-adjusted predictions with reasoning
    
    The AI acts as a "second opinion" that can adjust predictions
    based on market conditions the statistical model might miss.
    
    Always returns valid JSON - if AI is unavailable, returns base prediction
    with ai_enhanced.available=False.
    """
    # Normalize symbol
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price(clean_symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {clean_symbol}")
    
    from app.utils.time_et import now_et
    now = now_et()
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    minutes_to_close = max(0, int((market_close - now).total_seconds() / 60))
    
    base_result = None
    base_model_response = None
    
    try:
        from app.utils.eod_data_integration import get_eod_prediction_inputs
        from app.utils.gamma_eod_predictor import predict_eod_close
        
        inputs = get_eod_prediction_inputs(
            api_key=settings.polygon_api_key,
            ticker=clean_symbol,
            spot_price=current_price
        )
        
        base_result = predict_eod_close(
            walls=inputs['walls'],
            pin_history=inputs['pin_history'],
            zero_gamma=inputs['zero_gamma'],
            spot_prices=inputs['spot_prices'],
            intraday_high=inputs['intraday_high'],
            intraday_low=inputs['intraday_low'],
            hv10_points=inputs['hv10_points'],
            multi_expiry_aggregate_pin=inputs.get('multi_expiry_aggregate_pin')
        )
        
        base_model_response = {
            "eod_prediction": float(base_result.eod_estimate),
            "wwm": float(base_result.wwm) if base_result.wwm else None,
            "vacp": float(base_result.vacp) if base_result.vacp else None,
            "pin_stability": float(base_result.pin_stability_index) if base_result.pin_stability_index else None
        }
        
    except Exception as e:
        log.error(f"Base prediction failed for {symbol}: {e}")
        raise HTTPException(503, f"Base prediction error: {str(e)}")
    
    ai_enhanced_response = None
    
    try:
        from app.utils.eod_data_integration import get_ai_enhanced_prediction
        import options_gamma
        
        gamma_data = options_gamma.get_gamma_analysis(
            settings.polygon_api_key,
            clean_symbol,
            current_price
        )
        
        multi_expiry_data = options_gamma.get_multi_expiry_analysis(
            api_key=settings.polygon_api_key,
            underlying=clean_symbol,
            spot_price=current_price,
            max_dte=7
        )
        
        ai_result = await get_ai_enhanced_prediction(
            api_key=settings.polygon_api_key,
            ticker=clean_symbol,
            current_price=current_price,
            eod_prediction=base_result.eod_estimate,
            gamma_data=gamma_data if gamma_data else {},
            vwap_deviation=0.0,
            microtrend=0.0,
            minutes_to_close=minutes_to_close,
            multi_expiry_data=multi_expiry_data
        )
        
        ai_enhanced_response = {
            "original_prediction": float(ai_result.original_prediction),
            "adjusted_prediction": float(ai_result.ai_adjusted_prediction),
            "confidence": float(ai_result.ai_confidence),
            "adjustment_reason": ai_result.adjustment_reason,
            "market_conditions": ai_result.market_conditions,
            "risk_factors": ai_result.risk_factors,
            "recommendation": ai_result.recommendation,
            "provider": ai_result.ai_provider,
            "available": ai_result.ai_available
        }
        
    except Exception as e:
        log.warning(f"AI enhancement failed for {symbol}, returning base prediction: {e}")
        ai_enhanced_response = {
            "original_prediction": float(base_result.eod_estimate),
            "adjusted_prediction": float(base_result.eod_estimate),
            "confidence": 0.0,
            "adjustment_reason": f"AI analysis unavailable: {str(e)}",
            "market_conditions": "Unable to analyze",
            "risk_factors": ["AI service error"],
            "recommendation": "Use base model prediction",
            "provider": "none",
            "available": False
        }
    
    return {
        "symbol": clean_symbol,
        "current_price": float(current_price),
        "base_model": base_model_response,
        "ai_enhanced": ai_enhanced_response,
        "minutes_to_close": minutes_to_close,
        "timestamp": now_et().isoformat()
    }


@app.get("/ai/market-briefing")
async def get_market_briefing_endpoint(symbol: str = "SPX"):
    """
    Get AI-generated market briefing for the given symbol.
    
    Returns a comprehensive market analysis including:
    - Executive summary of market conditions
    - Gamma positioning interpretation
    - Trend assessment with reasoning
    - Key support/resistance/magnet levels
    - Overall market sentiment
    """
    # Normalize symbol
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price(clean_symbol)
    if not current_price:
        raise HTTPException(503, f"No price data for {clean_symbol}")
    
    try:
        from app.utils.eod_data_integration import (
            get_eod_prediction_inputs,
            get_market_briefing
        )
        from app.utils.gamma_eod_predictor import predict_eod_close
        import options_gamma
        
        # Get prediction inputs
        inputs = get_eod_prediction_inputs(
            api_key=settings.polygon_api_key,
            ticker=clean_symbol,
            spot_price=current_price
        )
        
        # Get base EOD prediction for context
        base_result = predict_eod_close(
            walls=inputs['walls'],
            pin_history=inputs['pin_history'],
            zero_gamma=inputs['zero_gamma'],
            spot_prices=inputs['spot_prices'],
            intraday_high=inputs['intraday_high'],
            intraday_low=inputs['intraday_low'],
            hv10_points=inputs['hv10_points'],
            multi_expiry_aggregate_pin=inputs.get('multi_expiry_aggregate_pin')
        )
        
        # Get gamma analysis
        gamma_data = options_gamma.get_gamma_analysis(
            settings.polygon_api_key,
            clean_symbol,
            current_price
        )
        
        # Get multi-expiry data
        multi_expiry_data = options_gamma.get_multi_expiry_analysis(
            api_key=settings.polygon_api_key,
            underlying=clean_symbol,
            spot_price=current_price,
            max_dte=7
        )
        
        # Get AI market briefing
        briefing = await get_market_briefing(
            api_key=settings.polygon_api_key,
            ticker=clean_symbol,
            current_price=current_price,
            gamma_data=gamma_data if gamma_data else {},
            eod_prediction=base_result.eod_estimate,
            multi_expiry_data=multi_expiry_data
        )
        
        if not briefing:
            raise HTTPException(503, "AI briefing not available")
        
        return {
            "symbol": clean_symbol,
            "current_price": float(current_price),
            "eod_prediction": float(base_result.eod_estimate),
            "briefing": briefing,
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"Market briefing failed for {symbol}: {e}")
        raise HTTPException(503, f"Market briefing error: {str(e)}")


@app.get("/ai/status")
async def get_ai_status():
    """
    Check AI service availability and provider information.
    """
    try:
        from app.services.ai_service import get_ai_service, AIService
        
        ai_service = get_ai_service()
        
        return {
            "available": ai_service.is_available,
            "provider": ai_service.provider_name,
            "available_providers": AIService.get_available_providers(),
            "timestamp": now_et().isoformat()
        }
    except Exception as e:
        return {
            "available": False,
            "provider": "error",
            "error": str(e),
            "timestamp": now_et().isoformat()
        }


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
