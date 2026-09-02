"""
Feature calculators for 0-day prediction model.
All features computed from ring buffer data with <50ms latency target.
"""
import time
import numpy as np
from typing import Optional, Tuple, Dict, Any
from collections import deque
import logging

log = logging.getLogger("features")

_FEATURE_CACHE_TTL_SECONDS = 1.0
_feature_cache: Dict[str, Dict[str, Any]] = {}

def calc_vwap_deviation(symbol: str) -> float:
    """
    Calculate (current_price - VWAP) / VWAP as percentage.
    Primary mean-reversion signal.
    """
    from app.state.ring_buffers import INDEX_RINGS, get_session_vwap, IndexTick
    
    ring = INDEX_RINGS.get(symbol)
    if not ring or not ring.q:
        return 0.0
    
    # Get latest price
    _, latest_tick = ring.q[-1]
    if not isinstance(latest_tick, IndexTick):
        return 0.0
    
    current_price = latest_tick.price
    vwap = get_session_vwap(symbol)
    
    if vwap == 0:
        return 0.0
    
    return (current_price - vwap) / vwap

def calc_microtrend(symbol: str, lookback_seconds: int = 300) -> float:
    """
    Simple linear regression slope over last N seconds.
    Captures short-term momentum for final hour predictions.
    Returns slope in $/second.
    Optimized: Uses numpy polyfit instead of sklearn for <5ms latency.
    """
    from app.state.ring_buffers import INDEX_RINGS, IndexTick
    
    ring = INDEX_RINGS.get(symbol)
    if not ring or len(ring.q) < 10:
        return 0.0
    
    # Get recent data
    recent = list(ring.q)[-lookback_seconds:]
    if len(recent) < 10:
        return 0.0
    
    # Extract timestamps and prices
    times = []
    prices = []
    
    for ts, tick in recent:
        if isinstance(tick, IndexTick):
            times.append(ts)
            prices.append(tick.price)
    
    if len(times) < 10:
        return 0.0
    
    # Normalize time to start at 0
    t0 = times[0]
    x = np.array([t - t0 for t in times])
    y = np.array(prices)
    
    # Fast linear regression using numpy polyfit
    coeffs = np.polyfit(x, y, 1)
    
    # Return slope ($/second)
    return float(coeffs[0])

def calc_gamma_pinning(symbol: str) -> Tuple[float, float]:
    """
    Calculate gamma exposure and pinning effect.
    Returns (gamma_pin_strength, flip_distance).
    
    gamma_pin_strength: 0-1, higher means stronger pinning
    flip_distance: dollars to nearest gamma flip point
    
    Uses canonical GEX computation from app/core/gex.py.
    Optimized: Uses numpy approximation instead of scipy for <10ms latency.
    """
    from app.state.oi_cache import oi_cache
    from app.state.ring_buffers import get_latest_price
    from app.core.gex import compute_gex
    import math
    
    current_price = get_latest_price(symbol)
    if not current_price:
        return 0.0, 0.0
    
    strikes = oi_cache.get_all_strikes(symbol)
    if not strikes:
        return 0.0, 0.0
    
    # Fast normal PDF approximation (replaces scipy.stats.norm.pdf)
    def norm_pdf_fast(x):
        """Fast standard normal PDF approximation"""
        return np.exp(-0.5 * x * x) / np.sqrt(2 * np.pi)
    
    # Calculate gamma exposure for each strike using canonical GEX computation
    total_gamma_exposure = 0.0
    net_gamma_exposure = 0.0
    gamma_by_strike = {}
    
    for K, snapshot in strikes.items():
        # Black-Scholes gamma (simplified)
        S = current_price
        T = 1/365  # 0DTE approximation
        sigma = (snapshot.iv_call + snapshot.iv_put) / 2
        
        if sigma <= 0:
            continue
        
        # d1 for Black-Scholes
        d1 = (math.log(S/K) + 0.5 * sigma**2 * T) / (sigma * math.sqrt(T))
        
        # Gamma = N'(d1) / (S * sigma * sqrt(T))
        # This is an unsigned magnitude (same for calls and puts at same strike)
        gamma = norm_pdf_fast(d1) / (S * sigma * math.sqrt(T))
        
        # Use canonical GEX computation: compute call and put exposure separately
        # gamma is the same for calls/puts at the same strike (Black-Scholes)
        try:
            res = compute_gex(
                gamma_call=gamma,
                oi_call=float(snapshot.oi_call),
                gamma_put=gamma,
                oi_put=float(snapshot.oi_put),
                multiplier=100.0,
            )
            
            # Store total_gex magnitude for pin finding (not net_gex)
            gamma_by_strike[K] = res.total_gex
            net_gamma_exposure += res.net_gex
            total_gamma_exposure += res.total_gex
        except (ValueError, TypeError) as e:
            log.warning(f"GEX computation failed for strike {K}: {e}")
            continue
    
    if not gamma_by_strike:
        return 0.0, 0.0
    
    # Find strike with max gamma exposure magnitude (likely pin point)
    # Uses total_gex, not net_gex, to find the pin strike
    max_gex_strike = max(gamma_by_strike, key=lambda k: gamma_by_strike[k])
    max_gex = gamma_by_strike[max_gex_strike]
    
    # Pin strength: normalized gamma exposure
    pin_strength = min(1.0, max_gex / (total_gamma_exposure + 1e-9))
    
    # Distance to pin point
    flip_distance = abs(current_price - max_gex_strike)
    
    return pin_strength, flip_distance

def calc_flow_urgency(symbol: str, lookback_seconds: int = 60) -> float:
    """
    Calculate options flow urgency score.
    High urgency = large aggressors in recent window.
    Returns normalized score 0-1.
    """
    from app.state.ring_buffers import FLOW_RINGS, OptTrade
    
    ring = FLOW_RINGS.get(symbol)
    if not ring or not ring.q:
        return 0.0
    
    # Get recent trades
    import time
    cutoff = time.time() - lookback_seconds
    
    recent_trades = [
        (ts, trade) for ts, trade in ring.q 
        if ts >= cutoff and isinstance(trade, OptTrade)
    ]
    
    if not recent_trades:
        return 0.0
    
    # Calculate flow metrics
    total_notional = sum(trade.notional for _, trade in recent_trades)
    directional_notional = sum(
        trade.notional for _, trade in recent_trades 
        if trade.aggressor != 0
    )
    buy_notional = sum(
        trade.notional for _, trade in recent_trades
        if trade.aggressor > 0
    )
    
    # Urgency score based on:
    # 1. Total flow volume
    # 2. Directional bias (buy vs sell)
    volume_score = min(1.0, total_notional / 1e8)  # Normalize by $100M
    
    if directional_notional > 0:
        directional_bias = abs(
            buy_notional / directional_notional - 0.5
        ) * 2
    else:
        directional_bias = 0.0
    
    # Combine metrics
    urgency = (volume_score * 0.7 + directional_bias * 0.3)
    
    return urgency

def compute_all_features(symbol: str) -> dict:
    """
    Compute all features for a symbol.
    Used by prediction endpoint.
    """
    try:
        from app.state.ring_buffers import INDEX_RINGS

        ring = INDEX_RINGS.get(symbol)
        latest_ts = ring.q[-1][0] if ring and ring.q else None
        now = time.time()

        cached = _feature_cache.get(symbol)
        if (
            cached
            and cached.get("latest_ts") == latest_ts
            and (now - cached.get("cached_at", 0.0)) <= _FEATURE_CACHE_TTL_SECONDS
        ):
            return cached["features"]

        vwap_dev = calc_vwap_deviation(symbol)
        microtrend = calc_microtrend(symbol)
        gamma_pin, flip_dist = calc_gamma_pinning(symbol)
        flow_urgency = calc_flow_urgency(symbol)

        features = {
            "vwap_deviation": vwap_dev,
            "microtrend": microtrend,
            "gamma_pin_strength": gamma_pin,
            "gamma_flip_distance": flip_dist,
            "flow_urgency": flow_urgency
        }

        _feature_cache[symbol] = {
            "latest_ts": latest_ts,
            "cached_at": now,
            "features": features,
        }

        return features
    except Exception as e:
        log.error(f"Error computing features for {symbol}: {e}")
        return {
            "vwap_deviation": 0.0,
            "microtrend": 0.0,
            "gamma_pin_strength": 0.0,
            "gamma_flip_distance": 0.0,
            "flow_urgency": 0.0
        }
