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
from app.models.model_artifacts import load_gamma_model_artifact

import logging
import time

log = logging.getLogger("api")

router = APIRouter()

@router.get("/metrics")
async def get_metrics():
    """Performance metrics endpoint"""
    latencies = predict_latency.get_percentiles()
    mem = snapshot("current")
    
    return {
        "latency": latencies,
        "memory": mem,
        "timestamp": now_et().isoformat()
    }


@router.get("/models/live")
async def get_live_model_metadata():
    """Expose live model metadata and artifact provenance."""
    symbols = ("SPX", "NDX", "DJI", "RUT")
    return {
        "live_prediction_model": {
            "model_name": "time-adaptive-ridge",
            "model_version": settings.live_model_version,
            "feature_schema_version": settings.live_feature_schema_version,
        },
        "symbols": {
            symbol: {
                "live_runtime": api_state.model_metadata_cache.get(symbol, {}),
                "offline_gamma_artifact": load_gamma_model_artifact(symbol),
            }
            for symbol in symbols
        },
        "timestamp": now_et().isoformat(),
    }


@router.get("/diagnostics/system")
async def get_system_diagnostics():
    """Expose backend diagnostics for production operations."""
    try:
        import torch

        cuda_available = torch.cuda.is_available()
        cuda_device = torch.cuda.get_device_name(0) if cuda_available else None
    except Exception:
        cuda_available = False
        cuda_device = None

    return {
        "status": "ok",
        "environment": settings.env,
        "market_data_provider": settings.market_data_provider,
        "backend_base_url": settings.backend_base_url,
        "streamlit_backend_only": settings.streamlit_backend_only,
        "device": {
            "live_inference": "cpu-live",
            "cuda_available": cuda_available,
            "cuda_device": cuda_device,
            "batch_training_preference": "cuda-when-available",
        },
        "latency": predict_latency.get_percentiles(),
        "memory": snapshot("diagnostics"),
        "timestamp": now_et().isoformat(),
    }


@router.get("/orb/{symbol}")
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


@router.get("/orb")
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


@router.get("/market-events")
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


@router.get("/market-events/summary")
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
