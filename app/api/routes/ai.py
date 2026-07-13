from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from app.api import state as api_state
from app.api.schemas import *
from app.utils.settings import settings
from app.utils.metrics import predict_latency, snapshot
from app.utils.time_et import now_et, minutes_to_close_et, is_regular_hours, get_cadence_ms
from app.state.ring_buffers import INDEX_RINGS, get_latest_price, get_latest_price_with_fallback
from app.features.calculators import compute_all_features
from app.models.db_models import get_rmse_for_tau

import logging
import time

log = logging.getLogger("api")

router = APIRouter()

@router.get("/predict/ai-enhanced")
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


@router.get("/ai/market-briefing")
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


@router.get("/ai/status")
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




@router.get("/api/predictions", response_model=AIPredictionsResponse)
async def get_all_predictions():
    """
    Get consolidated predictions for all major indices (SPX, NDX, DJI, RUT).
    
    This endpoint is designed for AI model consumption (ChatGPT, etc.) and provides:
    - Current prices for all indices
    - EOD predicted close prices
    - Gamma pin strikes (options dealer hedging levels)
    - Max pain strikes (option writer minimum payout level)
    - Confidence levels based on historical accuracy
    
    Returns data even when market is closed using cached/historical data.
    """
    from datetime import datetime
    from app.utils.time_et import is_regular_hours, minutes_to_close_et
    
    dt_utc = datetime.utcnow()
    market_open = is_regular_hours(dt_utc)
    tau = minutes_to_close_et(dt_utc) if market_open else None
    
    predictions = []
    symbols = ["SPX", "NDX", "DJI", "RUT"]
    from database import get_latest_audit_snapshots
    latest_snapshots = get_latest_audit_snapshots(symbols)
    
    for symbol in symbols:
        try:
            # Get current price with fallback
            current_price = get_latest_price_with_fallback(symbol, settings.polygon_api_key)
            
            # Try to get gamma data from cache or latest snapshot
            gamma_pin = None
            max_pain = None
            predicted_close = None
            confidence = None
            
            # Get latest gamma snapshot from batched in-memory lookup
            db_snapshot = latest_snapshots.get(symbol)
            
            if db_snapshot:
                gamma_pin = db_snapshot.primary_gamma_pin_strike
                max_pain = None  # Max pain not stored in this table yet
                
                # Calculate simple predicted close based on gamma pin pull
                if current_price and gamma_pin:
                    # Blend current price toward gamma pin (simplified EOD estimate)
                    gamma_weight = 0.3 if tau and tau > 60 else 0.5 if tau and tau > 30 else 0.7
                    predicted_close = current_price * (1 - gamma_weight) + gamma_pin * gamma_weight
                    confidence = "high" if tau and tau < 30 else "medium" if tau and tau < 60 else "low"
            
            predictions.append(AISymbolPrediction(
                symbol=symbol,
                current_price=current_price,
                predicted_close=predicted_close,
                gamma_pin=gamma_pin,
                max_pain=max_pain,
                confidence=confidence,
                minutes_to_close=tau
            ))
            
        except Exception as e:
            log.warning(f"Error getting prediction for {symbol}: {e}")
            predictions.append(AISymbolPrediction(
                symbol=symbol,
                current_price=None,
                predicted_close=None,
                gamma_pin=None,
                max_pain=None,
                confidence=None,
                minutes_to_close=tau,
                error=str(e)
            ))
    
    # Generate summary
    valid_preds = [p for p in predictions if p.predicted_close is not None]
    if market_open and valid_preds:
        summary = f"Market is open with {tau} minutes to close. Predictions available for {len(valid_preds)} indices."
    elif valid_preds:
        summary = f"Market is closed. Showing latest cached data for {len(valid_preds)} indices."
    else:
        summary = "No prediction data currently available."
    
    return AIPredictionsResponse(
        market_status="open" if market_open else "closed",
        timestamp=now_et().isoformat(),
        predictions=predictions,
        summary=summary
    )


@router.get("/api/prediction/{symbol}")
async def get_symbol_prediction(symbol: str):
    """
    Get detailed prediction for a single index.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, or RUT)
    
    Returns comprehensive prediction data including:
    - Current price
    - EOD predicted close
    - Gamma pin and max pain levels
    - Multi-expiry gamma analysis
    - Historical accuracy metrics
    """
    symbol = symbol.upper()
    if symbol not in ("SPX", "NDX", "DJI", "RUT"):
        raise HTTPException(400, f"Invalid symbol: {symbol}. Must be SPX, NDX, DJI, or RUT.")
    
    from datetime import datetime
    from app.utils.time_et import is_regular_hours, minutes_to_close_et
    
    dt_utc = datetime.utcnow()
    market_open = is_regular_hours(dt_utc)
    tau = minutes_to_close_et(dt_utc) if market_open else None
    
    # Get current price with fallback
    current_price = get_latest_price_with_fallback(symbol, settings.polygon_api_key)
    
    # Get latest gamma snapshot from database
    from database import get_latest_audit_snapshot
    db_snapshot = get_latest_audit_snapshot(symbol)
    
    # Get accuracy stats if available
    accuracy = None
    try:
        from app.models.db_models import get_session
        from database import Prediction
        with get_session() as session:
            recent = session.query(Prediction).filter(
                Prediction.symbol == symbol
            ).order_by(Prediction.created_at.desc()).limit(20).all()
            
            if recent:
                errors = [p.error_points for p in recent if p.error_points is not None]
                if errors:
                    accuracy = {
                        "sample_size": len(errors),
                        "mean_error": round(sum(errors) / len(errors), 2),
                        "min_error": round(min(errors), 2),
                        "max_error": round(max(errors), 2)
                    }
    except Exception as e:
        log.warning(f"Could not get accuracy stats: {e}")
    
    # Build response
    response = {
        "symbol": symbol,
        "market_status": "open" if market_open else "closed",
        "current_price": current_price,
        "minutes_to_close": tau,
        "timestamp": now_et().isoformat()
    }
    
    if db_snapshot:
        # Calculate predicted close
        gamma_pin = db_snapshot.primary_gamma_pin_strike
        if current_price and gamma_pin:
            gamma_weight = 0.3 if tau and tau > 60 else 0.5 if tau and tau > 30 else 0.7
            predicted_close = current_price * (1 - gamma_weight) + gamma_pin * gamma_weight
            response["predicted_close"] = round(predicted_close, 2)
            response["confidence"] = "high" if tau and tau < 30 else "medium" if tau and tau < 60 else "low"
        
        response["gamma_analysis"] = {
            "gamma_pin": gamma_pin,
            "max_pain": None,  # Not stored in this table yet
            "pin_stability": db_snapshot.confidence,  # Using confidence as proxy
            "wwm": None,  # Not stored in this table
            "zero_gamma": db_snapshot.zero_gamma_level,
            "vol_regime": db_snapshot.vol_regime,
            "net_gex": db_snapshot.net_gex,
            "snapshot_time": str(db_snapshot.generated_at_utc) if db_snapshot.generated_at_utc else None
        }
    
    if accuracy:
        response["accuracy_metrics"] = accuracy
    
    return response


@router.get("/api/status")
async def get_api_status():
    """
    Get API status and available endpoints.
    Useful for AI models to discover available capabilities.
    """
    from datetime import datetime
    from app.utils.time_et import is_regular_hours, minutes_to_close_et
    
    dt_utc = datetime.utcnow()
    market_open = is_regular_hours(dt_utc)
    tau = minutes_to_close_et(dt_utc) if market_open else None
    
    return {
        "status": "online",
        "market_status": "open" if market_open else "closed",
        "minutes_to_close": tau,
        "supported_symbols": ["SPX", "NDX", "DJI", "RUT"],
        "endpoints": {
            "/api/predictions": "Get all index predictions (recommended for AI)",
            "/api/prediction/{symbol}": "Get detailed prediction for one index",
            "/api/status": "This endpoint - API status and capabilities",
            "/predict/eod": "Advanced gamma-based EOD prediction",
            "/gamma/multi-expiry": "Multi-expiry gamma analysis",
            "/ai/market-briefing": "AI-generated market briefing"
        },
        "timestamp": now_et().isoformat()
    }
