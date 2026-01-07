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

class CloseSignalResponse(BaseModel):
    """Individual close predictor signal"""
    emoji: str
    category: str
    message: str
    bias: str
    strength: float

class ClosePredictorResponse(BaseModel):
    """Close predictor overlay response with checklist signals"""
    symbol: str
    pin_strike: float
    spot_price: float
    expected_close: float
    close_range_low: float
    close_range_high: float
    signals: List[CloseSignalResponse]
    net_bias: str
    confidence: float
    drift_adjustment: float
    summary: str
    call_gex: Optional[float]
    put_gex: Optional[float]
    gex_ratio: Optional[float]
    minutes_to_close: int
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
    
    # Start dedicated Options WebSocket stream for real-time gamma updates
    from app.ingest.options_websocket_stream import start_options_websocket_stream
    asyncio.create_task(start_options_websocket_stream())
    
    # Start REST fallback (in case WebSocket fails)
    # Use REST API polling as primary fallback (5-second intervals)
    from app.ingest.rest_fallback import poll_polygon_rest, load_cached_snapshots
    asyncio.create_task(poll_polygon_rest())
    asyncio.create_task(load_cached_snapshots())  # Secondary fallback from DB
    
    # Start gamma pin scheduler to save snapshots throughout the day
    from gamma_scheduler import start_gamma_scheduler
    start_gamma_scheduler()
    log.info("Gamma pin scheduler started")
    
    log.info("API ready - WebSocket streams (stocks + options) + REST fallback starting")

@app.get("/health")
@app.get("/healthz")
async def health_check():
    """
    Health check endpoint.
    Returns per-symbol freshness and ring buffer status.
    """
    import time
    from app.ingest.rest_fallback import is_rest_only_mode
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
        "symbols": status,
        "timestamp": now_et().isoformat()
    }

@app.get("/levels/eod")
async def get_gamma_levels(symbol: str) -> LevelsResponse:
    """
    Get gamma exposure levels for symbol.
    Uses OI cache (no REST calls).
    Works after hours using fallback pricing.
    """
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    from app.state.oi_cache import oi_cache
    
    current_price = get_latest_price_with_fallback(symbol, settings.polygon_api_key)
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
    
    # Check freshness - with 1-second REST polling, use 5s threshold for both modes
    from app.ingest.rest_fallback import is_rest_only_mode
    max_age = 5  # 5 seconds freshness threshold (1s REST polling, real-time WebSocket)
    min_data_seconds = 10 if is_rest_only_mode() else 300  # Lower threshold for REST
    
    ring = INDEX_RINGS.get(symbol)
    if not ring or not ring.is_fresh(max_age_seconds=max_age):
        raise HTTPException(503, f"Stale data for {symbol}")
    
    if ring.length_seconds < min_data_seconds:
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
    Works after hours using fallback pricing from database or Polygon REST API.
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
    
    # Get current price with fallback for after-hours access
    current_price = get_latest_price_with_fallback(symbol, settings.polygon_api_key)
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

@app.get("/predict/close-overlay")
async def get_close_predictor_overlay(symbol: str = "SPX") -> ClosePredictorResponse:
    """
    Close Predictor Overlay - Checklist-based close prediction signals.
    
    Analyzes:
    - Pin migration throughout the trading day
    - Spot vs pin deviation thresholds
    - Call vs Put GEX imbalance
    - Pull strength decay
    - Final-hour drift factors
    - Holiday/thin liquidity adjustments
    
    Returns signals and an adjusted close estimate based on behavioral factors.
    """
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price_with_fallback(symbol, settings.polygon_api_key)
    if not current_price:
        raise HTTPException(503, f"No price data for {symbol}")
    
    try:
        from app.utils.close_predictor import run_close_predictor, format_signals_for_ui
        from app.utils.eod_data_integration import fetch_pin_snapshots_from_db
        import options_gamma
        import pytz
        
        et_tz = pytz.timezone('US/Eastern')
        now = datetime.now(et_tz)
        trading_date = now.date()
        
        # Get UTC datetime for minutes_to_close calculation
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        minutes_to_close = minutes_to_close_et(now_utc)
        
        pin_snapshots = fetch_pin_snapshots_from_db(symbol, trading_date)
        
        pin_history = []
        deviation_history = []
        latest_pin = current_price
        
        for snap in pin_snapshots:
            # Handle both ORM objects and dicts
            if hasattr(snap, 'pin_strike'):
                pin_strike = snap.pin_strike
                spot = getattr(snap, 'spot_price', current_price)
                snap_ts = getattr(snap, 'timestamp', '')
            else:
                pin_strike = snap.get('pin_strike', 0)
                spot = snap.get('spot_price', current_price)
                snap_ts = snap.get('timestamp', '')
            
            if pin_strike:
                pin_history.append({
                    'pin_strike': pin_strike,
                    'spot': spot,
                    'timestamp': str(snap_ts) if snap_ts else ''
                })
                if pin_strike > 0:
                    deviation_history.append((spot - pin_strike) / pin_strike * 100)
                    latest_pin = pin_strike
        
        call_gex = 0.0
        put_gex = 0.0
        pull_strength = 0.0
        
        # Skip slow GEX fetch if market is closed - use cached pin data
        if is_regular_hours(now_utc):
            try:
                gex_analysis = options_gamma.get_gamma_analysis(
                    api_key=settings.polygon_api_key,
                    underlying=symbol,
                    spot_price=current_price
                )
                
                if gex_analysis and not gex_analysis.get('data_unavailable'):
                    call_gex = gex_analysis.get('call_gex_total', 0) or 0
                    put_gex = gex_analysis.get('put_gex_total', 0) or 0
                    pull_strength = gex_analysis.get('pull_strength', 0) or 0
                    if gex_analysis.get('pin_strike'):
                        latest_pin = gex_analysis['pin_strike']
            except Exception as e:
                log.warning(f"Could not fetch gamma analysis for close predictor: {e}")
        else:
            # After hours - pull strength from pin history deviation
            if deviation_history:
                avg_dev = abs(sum(deviation_history) / len(deviation_history))
                pull_strength = avg_dev  # Use deviation as proxy for pull strength
        
        prediction = run_close_predictor(
            index=symbol,
            spot=current_price,
            pin=latest_pin,
            call_gex=call_gex,
            put_gex=put_gex,
            pull_strength=pull_strength,
            pin_history=pin_history if pin_history else None,
            deviation_history=deviation_history if deviation_history else None,
            minutes_to_close=minutes_to_close
        )
        
        gex_ratio = None
        if put_gex and put_gex != 0:
            gex_ratio = call_gex / put_gex
        elif call_gex and call_gex != 0:
            gex_ratio = float('inf')
        
        signals_formatted = [
            CloseSignalResponse(
                emoji=s.emoji,
                category=s.category,
                message=s.message,
                bias=s.bias,
                strength=s.strength
            )
            for s in prediction.signals
        ]
        
        return ClosePredictorResponse(
            symbol=symbol,
            pin_strike=prediction.pin_strike,
            spot_price=prediction.spot_price,
            expected_close=prediction.expected_close,
            close_range_low=prediction.close_range_low,
            close_range_high=prediction.close_range_high,
            signals=signals_formatted,
            net_bias=prediction.net_bias,
            confidence=prediction.confidence,
            drift_adjustment=prediction.drift_adjustment,
            summary=prediction.summary,
            call_gex=call_gex if call_gex else None,
            put_gex=put_gex if put_gex else None,
            gex_ratio=gex_ratio,
            minutes_to_close=minutes_to_close,
            timestamp=now_et().isoformat()
        )
        
    except Exception as e:
        log.error(f"Close predictor failed for {symbol}: {e}")
        raise HTTPException(503, f"Close predictor error: {str(e)}")

@app.get("/gamma/multi-expiry")
async def get_multi_expiry_gamma(symbol: str = "SPX", max_dte: int = 7):
    """
    Get multi-expiration gamma exposure analysis (0-DTE through 7-DTE).
    
    Returns time-weighted gamma exposure by expiration date, unified gamma walls,
    and an aggregate pin strike based on all near-term expirations.
    Works after hours using fallback pricing from database or Polygon REST API.
    """
    # Normalize symbol - strip I: prefix if present
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price_with_fallback(clean_symbol, settings.polygon_api_key)
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
    
    current_price = get_latest_price_with_fallback(clean_symbol, settings.polygon_api_key)
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
    Works after hours using fallback pricing.
    """
    # Normalize symbol
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    current_price = get_latest_price_with_fallback(clean_symbol, settings.polygon_api_key)
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


@app.get("/orb/{symbol}")
async def get_orb_data(symbol: str):
    """
    Get Opening Range Breakout (ORB) data for a symbol.
    Tracks the 1-hour opening range (9:30-10:30 AM ET).
    """
    try:
        from app.state.orb_tracker import get_orb_tracker
        
        clean_symbol = symbol.upper().replace("I:", "")
        if clean_symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        tracker = get_orb_tracker()
        orb_data = tracker.get_orb_data(clean_symbol)
        
        if orb_data is None:
            return {
                "symbol": clean_symbol,
                "orb_data": None,
                "message": "ORB tracking not yet started for today",
                "timestamp": now_et().isoformat()
            }
        
        # Get current price for position calculation
        current_price = get_latest_price_with_fallback(clean_symbol)
        
        # Calculate features if we have data
        if orb_data.orb_high and orb_data.orb_low and current_price:
            position = orb_data.position_in_range(current_price)
            breakout = orb_data.breakout_direction(current_price)
        else:
            position = 0.5
            breakout = "forming"
        
        return {
            "symbol": clean_symbol,
            "orb_data": orb_data.to_dict(),
            "current_price": current_price,
            "position_in_range": position,
            "breakout_direction": breakout,
            "timestamp": now_et().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"ORB data error for {symbol}: {e}")
        raise HTTPException(503, f"ORB data error: {str(e)}")


@app.get("/orb")
async def get_all_orb_data():
    """
    Get ORB data for all tracked symbols.
    """
    try:
        from app.state.orb_tracker import get_orb_tracker
        
        tracker = get_orb_tracker()
        all_orb = tracker.get_all_orb_data()
        
        return {
            "orb_data": all_orb,
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"All ORB data error: {e}")
        raise HTTPException(503, f"ORB data error: {str(e)}")


@app.get("/market-events")
async def get_market_events():
    """
    Scan for current market events that could impact predictions.
    Returns macro/micro events including Fed announcements, economic data,
    earnings, and market structure events.
    """
    try:
        from app.services.market_event_scanner import scan_market_events
        
        scan = scan_market_events()
        
        return {
            "scan": scan.to_dict(),
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"Market events scan error: {e}")
        raise HTTPException(503, f"Market events error: {str(e)}")


@app.get("/market-events/summary")
async def get_market_events_summary():
    """
    Get a text summary of market events for AI integration.
    """
    try:
        from app.services.market_event_scanner import get_events_for_ai_prompt
        
        summary = get_events_for_ai_prompt()
        
        return {
            "summary": summary,
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"Market events summary error: {e}")
        raise HTTPException(503, f"Market events error: {str(e)}")


@app.get("/prediction-accuracy/by-regime")
async def get_prediction_accuracy_by_regime(symbol: Optional[str] = None, days: int = 30):
    """
    Get MAE (Mean Absolute Error) statistics by regime type.
    Helps identify systematic bias in specific session types:
    - half_day: Early close sessions (1 PM ET)
    - regular_day: Normal 4 PM close sessions
    - eom: End of month (last 3 trading days)
    - eow: End of week (Thursday/Friday)
    - holiday_adjacent: Day before/after market holidays
    
    Args:
        symbol: Optional symbol filter (None = all symbols)
        days: Number of days to look back (default 30)
    
    Returns:
        MAE and bias statistics by regime type
    """
    try:
        from app.models.db_models import get_mae_by_regime
        
        stats = get_mae_by_regime(symbol=symbol, days=days)
        
        return {
            "symbol": symbol or "all",
            "days_lookback": days,
            "stats_by_regime": stats,
            "interpretation": {
                "mae": "Mean Absolute Error in price points",
                "mae_pct": "Mean Absolute Error as percentage of price",
                "bias": "Average (predicted - actual), positive = over-prediction",
                "n_samples": "Number of predictions in this category"
            },
            "timestamp": now_et().isoformat()
        }
        
    except Exception as e:
        log.error(f"Prediction accuracy by regime error: {e}")
        raise HTTPException(503, f"Prediction accuracy error: {str(e)}")


# =============================================================================
# DEBUG / RECONCILE ENDPOINT
# This endpoint is for auditability - read-only, no calculations, works without UI
# =============================================================================

@app.get("/debug/reconcile/{symbol}")
async def debug_reconcile(symbol: str):
    """
    Debug reconciliation endpoint for system auditability.
    
    This endpoint MUST:
    1. Pull from latest audit snapshot
    2. Pull from latest prediction output
    3. Perform NO calculations
    4. Be read-only, presentation-only
    5. Work even if UI is down
    
    Returns exactly:
    - symbol
    - spot_last
    - predicted_close
    - delta_from_gamma
    - gamma_pull
    - gamma_pin_strength
    - gamma_net_level
    - primary_gamma_pin_strike
    - zero_gamma_level
    - top_3_strikes_by_abs_gex
    - validation_status
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from app.core.audit_persistence import get_latest_snapshot
        from app.state.ring_buffers import get_latest_price_with_fallback
        
        # Get latest audit snapshot (read-only)
        snapshot = get_latest_snapshot(symbol)
        
        # Get current spot price for comparison
        current_spot = get_latest_price_with_fallback(symbol)
        
        # Get latest prediction if available
        predicted_close = None
        delta_from_gamma = None
        gamma_pull = None
        gamma_pin_strength = None
        gamma_net_level = None
        
        # Try to get prediction data from cache if available
        if symbol in _coefficients_cache:
            try:
                from app.models.ridge_predictor import predict
                from app.state.ring_buffers import INDEX_RINGS
                
                ring = INDEX_RINGS.get(symbol)
                if ring and ring.count >= 30:
                    df = ring.to_dataframe()
                    if df is not None and not df.empty:
                        # Get prediction (this reads cached data, does not recalculate gamma)
                        result = predict(symbol)
                        if result:
                            predicted_close = result.get('predicted_close')
                            delta_from_gamma = result.get('delta_from_gamma', 0)
                            gamma_pull = result.get('features', {}).get('gamma_pull')
                            gamma_pin_strength = result.get('features', {}).get('gamma_pin', 0)
                            gamma_net_level = result.get('features', {}).get('gamma_net_level', 0)
            except Exception as e:
                log.warning(f"Could not get prediction for reconcile: {e}")
        
        # Build response from snapshot (no recalculation)
        if snapshot:
            top_3 = snapshot.top_strikes_by_abs_gex[:3] if snapshot.top_strikes_by_abs_gex else []
            
            return {
                "symbol": symbol,
                "snapshot_exists": True,
                "snapshot_timestamp": snapshot.generated_at_utc,
                
                # Core values
                "spot_last": snapshot.spot_last,
                "spot_current": current_spot,
                "predicted_close": predicted_close,
                
                # Gamma feature values
                "delta_from_gamma": delta_from_gamma,
                "gamma_pull": gamma_pull or snapshot.primary_gamma_pin_strike,
                "gamma_pin_strength": gamma_pin_strength,
                "gamma_net_level": gamma_net_level,
                
                # Derived gamma metrics
                "primary_gamma_pin_strike": snapshot.primary_gamma_pin_strike,
                "zero_gamma_level": snapshot.zero_gamma_level,
                
                # Aggregate GEX (canonical definitions)
                "total_gex_abs": snapshot.total_gex_abs,
                "total_gex_abs_definition": snapshot.total_gex_abs_definition,
                "total_gex_net": snapshot.total_gex_net,
                "total_gex_net_definition": snapshot.total_gex_net_definition,
                
                # Top 3 strikes
                "top_3_strikes_by_abs_gex": [
                    {
                        "strike": s.get('strike'),
                        "abs_gex": s.get('abs_gex'),
                        "net_gex": s.get('net_gex'),
                    }
                    for s in top_3
                ],
                
                # Chain info
                "chain_symbol_used": snapshot.chain_symbol_used,
                "contracts_count": snapshot.contracts_count,
                "expiration_scope": snapshot.expiration_scope,
                
                # Validation status
                "validation_is_valid": snapshot.validation_is_valid,
                "validation_failure_reasons": snapshot.validation_failure_reasons,
                "gamma_excluded_from_model": snapshot.gamma_excluded_from_model,
                
                # Timestamp
                "reconcile_timestamp": now_et().isoformat(),
            }
        else:
            # No snapshot available
            return {
                "symbol": symbol,
                "snapshot_exists": False,
                "spot_current": current_spot,
                "predicted_close": predicted_close,
                "message": f"No audit snapshot found for {symbol}. Run gamma sampling first.",
                "reconcile_timestamp": now_et().isoformat(),
            }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Debug reconcile error for {symbol}: {e}")
        raise HTTPException(503, f"Reconcile error: {str(e)}")


# =============================================================================
# ACCURACY LEDGER ENDPOINTS
# For post-close accuracy recording and analysis
# =============================================================================

@app.get("/accuracy/freeze-status")
async def get_freeze_status():
    """Get current market freeze status."""
    try:
        from app.utils.market_time import is_freeze_enforced, get_freeze_status as _get_freeze_status, now_et
        
        is_frozen, reason = _get_freeze_status()
        
        return {
            "is_frozen": is_frozen,
            "reason": reason,
            "timestamp": now_et().isoformat(),
        }
    except Exception as e:
        log.error(f"Freeze status error: {e}")
        return {
            "is_frozen": True,
            "reason": f"Error: {str(e)}",
            "timestamp": now_et().isoformat(),
        }


@app.post("/accuracy/record/{symbol}")
async def record_accuracy_endpoint(symbol: str, official_close: float):
    """
    Record prediction accuracy after market close.
    
    This endpoint should be called after 4 PM ET with the official closing price
    to log the prediction accuracy for the day.
    
    Security: Server-side snapshot retrieval (no client payload trusted).
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        official_close: Official closing price from data provider
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        # Validate official_close is reasonable
        if official_close <= 0 or official_close > 100000:
            raise HTTPException(400, f"Invalid official_close: {official_close}")
        
        from app.core.audit_persistence import load_last_valid_snapshot
        from app.core.accuracy_ledger import record_accuracy
        from app.utils.market_time import is_freeze_enforced, market_is_closed
        
        # Validate freeze state - accuracy should only be recorded after market close
        if not market_is_closed():
            raise HTTPException(400, "Cannot record accuracy while market is open")
        
        # Server-side snapshot retrieval (no client payload trusted)
        snapshot = load_last_valid_snapshot(symbol)
        if not snapshot:
            raise HTTPException(404, f"No valid snapshot found for {symbol}")
        
        # Build snapshot dict from server-retrieved data only
        snapshot_dict = {
            'timestamp_utc': snapshot.timestamp_utc,
            'primary_gamma_pin_strike': snapshot.primary_gamma_pin_strike,
            'spot_last': snapshot.spot_last,
            'contracts_count': snapshot.contracts_count,
            'expiration_scope': snapshot.expiration_scope,
            'validation_is_valid': snapshot.validation_is_valid,
            'gamma_excluded_from_model': snapshot.gamma_excluded_from_model,
            'top_strikes_by_abs_gex': snapshot.top_strikes_by_abs_gex,
            'total_gex_abs': snapshot.total_gex_abs,
        }
        
        record = record_accuracy(
            symbol=symbol,
            snapshot=snapshot_dict,
            official_close=official_close,
            freeze_enforced=is_freeze_enforced(),
        )
        
        if not record:
            raise HTTPException(500, "Failed to record accuracy")
        
        return {
            "status": "recorded",
            "symbol": symbol,
            "date": record.date,
            "pin_strike": record.pin,
            "official_close": record.official_close,
            "error_points": record.error_points,
            "error_pct": record.error_pct,
            "confidence": record.confidence,
            "freeze_enforced": record.freeze_enforced,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Record accuracy error for {symbol}: {e}")
        raise HTTPException(503, f"Record accuracy error: {str(e)}")


@app.get("/accuracy/stats/{symbol}")
async def get_accuracy_stats_endpoint(symbol: str, days: int = 30):
    """
    Get accuracy statistics for a symbol.
    
    Returns MAE, bias, hit rates, and other accuracy metrics.
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from app.core.accuracy_ledger import get_accuracy_stats
        
        stats = get_accuracy_stats(symbol, days)
        
        return {
            **stats,
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Accuracy stats error for {symbol}: {e}")
        raise HTTPException(503, f"Accuracy stats error: {str(e)}")


@app.get("/accuracy/ledger")
async def get_accuracy_ledger(symbol: Optional[str] = None, days: int = 30):
    """
    Get raw accuracy ledger records.
    
    Returns the full accuracy ledger for analysis.
    """
    try:
        if symbol:
            symbol = symbol.upper()
            if symbol not in ("SPX", "NDX", "DJI", "RUT"):
                raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from app.core.accuracy_ledger import get_ledger_records
        
        records = get_ledger_records(symbol, days)
        
        return {
            "symbol": symbol or "all",
            "days": days,
            "count": len(records),
            "records": records,
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Accuracy ledger error: {e}")
        raise HTTPException(503, f"Accuracy ledger error: {str(e)}")


# =============================================================================
# HISTORICAL GAMMA VALIDATION ENDPOINTS
# For backtesting and model validation
# =============================================================================

@app.post("/historical/build-today/{symbol}")
async def build_today_snapshot_endpoint(symbol: str):
    """
    Build a gamma snapshot for TODAY using live options chain.
    
    This uses the current options chain snapshot which includes OI data.
    Run this daily to build a historical validation dataset.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from tools.historical_gamma import build_today_snapshot, save_historical_snapshot
        
        snapshot = build_today_snapshot(symbol)
        if not snapshot:
            raise HTTPException(503, f"Could not build today's snapshot for {symbol}")
        
        save_historical_snapshot(snapshot)
        
        return {
            "status": "built",
            "symbol": symbol,
            "date": snapshot.date,
            "spot": snapshot.spot,
            "pin_strike": snapshot.pin_strike,
            "contracts_count": snapshot.contracts_count,
            "total_gex_abs": snapshot.total_gex_abs,
            "total_gex_net": snapshot.total_gex_net,
            "assumptions": snapshot.assumptions,
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Build today's snapshot error: {e}")
        raise HTTPException(503, f"Build error: {str(e)}")


@app.post("/historical/build/{symbol}")
async def build_historical_snapshot_endpoint(symbol: str, date: str):
    """
    Build a historical gamma snapshot for a specific date.
    
    NOTE: For historical dates (not today), this requires pre-built snapshots.
    Use /historical/build-today/{symbol} to build snapshots during market hours.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        date: Target date (YYYY-MM-DD)
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from datetime import datetime
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        from tools.historical_gamma import build_historical_snapshot, save_historical_snapshot
        
        snapshot = build_historical_snapshot(symbol, target_date)
        if not snapshot:
            raise HTTPException(404, f"Could not build snapshot for {symbol} on {date}. For historical dates, snapshots must be pre-built during market hours.")
        
        save_historical_snapshot(snapshot)
        
        return {
            "status": "built",
            "symbol": symbol,
            "date": snapshot.date,
            "spot": snapshot.spot,
            "pin_strike": snapshot.pin_strike,
            "contracts_count": snapshot.contracts_count,
            "total_gex_abs": snapshot.total_gex_abs,
            "total_gex_net": snapshot.total_gex_net,
            "assumptions": snapshot.assumptions,
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(400, f"Invalid date format: {str(e)}")
    except Exception as e:
        log.error(f"Build historical snapshot error: {e}")
        raise HTTPException(503, f"Build error: {str(e)}")


@app.post("/historical/batch/{symbol}")
async def build_batch_historical_endpoint(symbol: str, days: int = 30):
    """
    Build historical gamma snapshots for the last N trading days.
    
    This is a long-running operation that fetches historical data from Polygon.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        days: Number of trading days to build (default 30)
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        if days < 1 or days > 90:
            raise HTTPException(400, "Days must be between 1 and 90")
        
        from tools.historical_gamma import build_batch_historical
        
        snapshots = build_batch_historical(symbol, days)
        
        return {
            "status": "built",
            "symbol": symbol,
            "days_requested": days,
            "snapshots_built": len(snapshots),
            "snapshots": [
                {
                    "date": s.date,
                    "spot": s.spot,
                    "pin_strike": s.pin_strike,
                    "contracts": s.contracts_count
                }
                for s in snapshots
            ],
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Build batch historical error: {e}")
        raise HTTPException(503, f"Batch build error: {str(e)}")


@app.get("/historical/validate/{symbol}")
async def validate_historical_endpoint(symbol: str, days: int = 30):
    """
    Validate historical gamma predictions against actual closes.
    
    Returns error metrics including MAE, bias, and direction accuracy.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        days: Number of days to validate (default 30)
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from tools.historical_validation import validate_historical_snapshots
        
        summary = validate_historical_snapshots(symbol, days)
        
        return {
            "symbol": symbol,
            "days_validated": summary.days_validated,
            "days_with_pin": summary.days_with_pin,
            "mae_points": summary.mae_points,
            "mae_pct": summary.mae_pct,
            "bias_points": summary.bias_points,
            "direction_accuracy": summary.direction_accuracy,
            "median_error": summary.median_error,
            "max_error": summary.max_error,
            "min_error": summary.min_error,
            "error_std": summary.error_std,
            "errors_by_date": summary.errors_by_date,
            "interpretation": {
                "bias_direction": "underestimates" if summary.bias_points > 0 else "overestimates",
                "error_magnitude": "low" if summary.mae_pct < 0.5 else "moderate" if summary.mae_pct < 1.0 else "high",
                "direction_quality": "good" if summary.direction_accuracy > 0.6 else "marginal" if summary.direction_accuracy > 0.5 else "poor"
            },
            "disclaimers": {
                "oi_static": "OI from end-of-day snapshot, assumed static during session",
                "dealer_positioning": "Dealer net sign unknown, tracking magnitude only",
                "not_for_trading": "Model validation only, not a tradable signal"
            },
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Historical validation error: {e}")
        raise HTTPException(503, f"Validation error: {str(e)}")


@app.get("/historical/snapshot/{symbol}/{date}")
async def get_historical_snapshot_endpoint(symbol: str, date: str):
    """
    Get a saved historical gamma snapshot.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        date: Target date (YYYY-MM-DD)
    """
    try:
        symbol = symbol.upper()
        if symbol not in ("SPX", "NDX", "DJI", "RUT"):
            raise HTTPException(400, f"Invalid symbol: {symbol}")
        
        from datetime import datetime
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        from tools.historical_gamma import load_historical_snapshot
        
        snapshot = load_historical_snapshot(symbol, target_date)
        if not snapshot:
            raise HTTPException(404, f"No snapshot found for {symbol} on {date}")
        
        return {
            **snapshot.to_dict(),
            "timestamp": now_et().isoformat(),
        }
        
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(400, f"Invalid date format: {str(e)}")
    except Exception as e:
        log.error(f"Get historical snapshot error: {e}")
        raise HTTPException(503, f"Get snapshot error: {str(e)}")
