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

@router.post("/historical/build-today/{symbol}")
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


@router.post("/historical/build/{symbol}")
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


@router.post("/historical/batch/{symbol}")
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


@router.get("/historical/validate/{symbol}")
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


@router.get("/historical/snapshot/{symbol}/{date}")
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
