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

@router.get("/prediction-accuracy/by-regime")
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


@router.get("/accuracy/freeze-status")
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


@router.post("/accuracy/record/{symbol}")
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


@router.get("/accuracy/stats/{symbol}")
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


@router.get("/accuracy/ledger")
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
