from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.api import state as api_state
from app.api.schemas import *
from app.utils.settings import settings
from app.utils.metrics import predict_latency, snapshot
from app.utils.time_et import now_et, minutes_to_close_et, is_regular_hours, get_cadence_ms
from app.state.ring_buffers import (
    INDEX_RINGS,
    PREDICTION_SYMBOLS,
    TRACKED_INDEX_SYMBOLS,
    get_latest_price,
    get_latest_price_with_fallback,
)
from app.features.calculators import compute_all_features
from app.models.db_models import get_rmse_for_tau

import logging
import time

log = logging.getLogger("api")

router = APIRouter()
SUPPORTED_SYMBOLS = PREDICTION_SYMBOLS
GAMMA_SYMBOLS = TRACKED_INDEX_SYMBOLS
DEFAULT_COEFFICIENTS = {
    "beta_vwap": 1.0,
    "beta_gamma": 0.0,
    "beta_flow": 0.0,
    "beta_microtrend": 0.0,
    "intercept": 0.0,
}
LIVE_FEATURES = ["vwap_deviation", "microtrend", "gamma_pin_strength", "flow_urgency"]
DEFAULT_MODEL_METADATA = {
    "coefficients_source": "fallback-defaults",
    "coefficients_updated_at": None,
    "coefficients_sample_size": 0,
}

@router.get("/levels/eod")
async def get_gamma_levels(symbol: str) -> LevelsResponse:
    """
    Get gamma exposure levels for symbol.
    Uses OI cache (no REST calls).
    Works after hours using fallback pricing.
    """
    if symbol not in GAMMA_SYMBOLS:
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


@router.get("/gamma/state")
async def get_gamma_state(symbol: str, spot_price: Optional[float] = None):
    """Return backend gamma state for Streamlit consumption."""
    clean_symbol = symbol.upper().replace("I:", "")
    if clean_symbol not in GAMMA_SYMBOLS:
        raise HTTPException(400, f"Invalid symbol: {symbol}")

    resolved_spot = spot_price or get_latest_price_with_fallback(clean_symbol, settings.polygon_api_key)
    if not resolved_spot:
        raise HTTPException(503, f"No price data for {clean_symbol}")

    try:
        import options_gamma

        gex = options_gamma.get_gamma_analysis(
            api_key=settings.polygon_api_key,
            underlying=clean_symbol,
            spot_price=resolved_spot,
        )
        if not gex:
            raise HTTPException(503, f"No gamma analysis available for {clean_symbol}")

        gamma_walls = gex.get("gamma_walls")
        if hasattr(gamma_walls, "to_dict"):
            gamma_walls = gamma_walls.to_dict(orient="records")
        if gamma_walls is None:
            gamma_walls = []
        summary = "Gamma state available"

        return {
            "symbol": clean_symbol,
            "spot_price": resolved_spot,
            "pin_strike": gex.get("pin_strike"),
            "pull_strength": gex.get("pull_strength", 0.0),
            "total_gex": gex.get("total_gex", 0.0),
            "net_gex": gex.get("net_gex", 0.0),
            "zero_gamma": gex.get("zero_gamma"),
            "direction": gex.get("direction"),
            "summary": summary,
            "is_etf_proxy": gex.get("is_etf_proxy", False),
            "options_root": gex.get("options_root", clean_symbol),
            "gamma_walls": gamma_walls,
            "timestamp": now_et().isoformat(),
        }
    except HTTPException:
        raise
    except Exception:
        log.exception("Gamma state failed for %s", clean_symbol)
        raise HTTPException(503, "Gamma state error")


@router.get("/buffer/latest/{symbol}")
async def get_latest_buffered_price(symbol: str):
    """Return the latest cached backend price for a live display symbol."""
    clean_symbol = symbol.upper().replace("I:", "")
    if clean_symbol not in GAMMA_SYMBOLS:
        raise HTTPException(400, f"Invalid symbol: {symbol}")

    ring = INDEX_RINGS.get(clean_symbol)
    latest = ring.latest() if ring else None
    fallback_price = get_latest_price_with_fallback(clean_symbol, settings.polygon_api_key)

    timestamp = None
    if latest:
        latest_ts, _ = latest
        timestamp = datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat()

    return {
        "symbol": clean_symbol,
        "price": fallback_price,
        "timestamp": timestamp,
        "fresh": ring.is_fresh(max_age_seconds=5) if ring else False,
        "buffer_length": ring.length_seconds if ring else 0,
        "has_ring_data": bool(latest),
    }

@router.get("/predict/close")
async def predict_close(symbol: str) -> PredictionResponse:
    """
    Predict EOD close price for symbol.
    Enforces cadence limits and freshness checks.
    """
    t0 = time.perf_counter()
    
    # Validate symbol
    if symbol not in SUPPORTED_SYMBOLS:
        raise HTTPException(400, f"Invalid symbol: {symbol}")
    
    dt_utc = datetime.utcnow()
    
    # Check if market is open
    if not is_regular_hours(dt_utc):
        raise HTTPException(400, "Market is closed")
    
    # Check cadence enforcement
    tau = minutes_to_close_et(dt_utc)
    required_cadence_ms = get_cadence_ms(dt_utc)
    
    last_pred_ts = api_state.last_prediction_ts.get(symbol, 0.0)
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
            
    except Exception:
        log.exception("Feature computation failed for %s", symbol)
        raise HTTPException(503, "Feature computation error")
    
    # Get coefficients and fallback metadata
    coeffs = api_state.coefficients_cache.get(symbol)
    fallback_active = False
    if not coeffs:
        coeffs = DEFAULT_COEFFICIENTS
        fallback_active = True
    
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
    api_state.last_prediction_ts[symbol] = time.time()
    
    # Track latency
    latency_ms = (time.perf_counter() - t0) * 1000
    predict_latency.record(latency_ms)
    memory = snapshot(f"predict_close:{symbol}")
    metadata = api_state.model_metadata_cache.get(symbol, DEFAULT_MODEL_METADATA)
    fallback_reason = (
        "coefficients cache missing; using deterministic defaults"
        if fallback_active
        else None
    )
    model_metadata = {
        "model_name": "time-adaptive-ridge",
        "model_version": settings.live_model_version,
        "feature_schema_version": settings.live_feature_schema_version,
        "feature_list": LIVE_FEATURES,
        "coefficients_source": metadata.get("coefficients_source", "fallback-defaults"),
        "coefficients_updated_at": metadata.get("coefficients_updated_at"),
        "coefficients_sample_size": metadata.get("coefficients_sample_size", 0),
        "fallback_active": fallback_active,
        "fallback_reason": fallback_reason,
    }
    runtime_metadata = {
        "inference_device": "cpu-live",
        "latency_ms": latency_ms,
        "memory_current_mb": memory["current_mb"],
        "memory_peak_mb": memory["peak_mb"],
    }
    log.info(
        "predict_close symbol=%s model=%s version=%s device=%s latency_ms=%.2f fallback=%s",
        symbol,
        model_metadata["model_name"],
        model_metadata["model_version"],
        runtime_metadata["inference_device"],
        latency_ms,
        fallback_active,
    )
    
    return PredictionResponse(
        symbol=symbol,
        current_price=current_price,
        predicted_close=predicted_close,
        confidence_level=confidence,
        tau_minutes=tau,
        rmse=rmse,
        mae=mae,
        features=features,
        model_metadata=model_metadata,
        runtime_metadata=runtime_metadata,
        timestamp=now_et().isoformat()
    )

@router.get("/predict/eod")
async def predict_eod(symbol: str) -> EODPredictionResponse:
    """
    Advanced gamma-based EOD prediction using Wall-Weighted Magnet (WWM),
    Pin Stability Index (PSI), Zero-Gamma, and Volatility-Adjusted Close Predictor (VACP).
    
    Designed to reduce prediction error from 5-10 points to 1-3 points.
    Works after hours using fallback pricing from database or Polygon REST API.
    """
    # Validate symbol
    if symbol not in SUPPORTED_SYMBOLS:
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

@router.get("/predict/close-overlay")
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
    if symbol not in SUPPORTED_SYMBOLS:
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

@router.get("/gamma/multi-expiry")
async def get_multi_expiry_gamma(symbol: str = "SPX", max_dte: int = 7):
    """
    Get multi-expiration gamma exposure analysis (0-DTE through 7-DTE).
    
    Returns time-weighted gamma exposure by expiration date, unified gamma walls,
    and an aggregate pin strike based on all near-term expirations.
    Works after hours using fallback pricing from database or Polygon REST API.
    """
    # Normalize symbol - strip I: prefix if present
    clean_symbol = symbol.replace('I:', '') if symbol.startswith('I:') else symbol
    
    if clean_symbol not in SUPPORTED_SYMBOLS:
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
