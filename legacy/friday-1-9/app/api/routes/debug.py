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


@router.get("/debug/subscriptions")
async def debug_subscriptions():
    """
    Return full websocket subscription visibility for diagnostics.

    Includes requested and confirmed channel lists so large subscription
    sets (for example 421 symbols/channels) can be inspected directly.
    """
    try:
        from app.ingest.websocket_stream import get_websocket_subscription_state
        from app.ingest.options_websocket_stream import get_options_subscription_state

        index_state = get_websocket_subscription_state()
        options_state = get_options_subscription_state()

        return {
            "index_stream": index_state,
            "options_stream": options_state,
            "total_requested_subscriptions": (
                index_state.get("requested_count", 0)
                + options_state.get("requested_count", 0)
            ),
            "total_confirmed_subscriptions": (
                index_state.get("confirmed_count", 0)
                + options_state.get("confirmed_count", 0)
            ),
            "timestamp": now_et().isoformat(),
        }
    except Exception as e:
        log.error(f"Debug subscriptions error: {e}")
        raise HTTPException(503, f"Subscription debug error: {str(e)}")


@router.get("/debug/reconcile/{symbol}")
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
        if symbol in api_state.coefficients_cache:
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
