"""
Time-Adaptive Ridge Regression Predictor
Optimized for final trading hour (3:00 PM - 4:00 PM ET) predictions.
Uses VWAP deviation, microtrend, gamma pinning, flow urgency, and ORB features.
"""
import numpy as np
import pandas as pd
from datetime import datetime, time
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
import pytz
import logging

log = logging.getLogger("ridge_predictor")

# Calibrated coefficients from backtesting (fallback defaults)
DEFAULT_COEFFICIENTS = {
    "SPX": {
        "beta_vwap": 1.2,
        "beta_microtrend": 0.8,
        "beta_gamma": 0.3,
        "beta_flow": 0.5,
        "intercept": 0.0
    },
    "NDX": {
        "beta_vwap": 1.3,
        "beta_microtrend": 0.9,
        "beta_gamma": 0.4,
        "beta_flow": 0.6,
        "intercept": 0.0
    },
    "DJI": {
        "beta_vwap": 1.1,
        "beta_microtrend": 0.7,
        "beta_gamma": 0.25,
        "beta_flow": 0.4,
        "intercept": 0.0
    },
    "RUT": {
        "beta_vwap": 1.15,
        "beta_microtrend": 0.75,
        "beta_gamma": 0.3,
        "beta_flow": 0.45,
        "intercept": 0.0
    }
}

@dataclass
class PredictionResult:
    """Unified prediction result structure"""
    predicted_price: float
    current_price: float
    confidence: float
    model_name: str
    features: Dict[str, float]
    time_to_close_minutes: Optional[int] = None
    recommendation: Optional[str] = None

def get_minutes_to_close() -> int:
    """
    Calculate minutes until market close with proper bounds.
    Delegates to time_et utilities to respect early close days (1 PM ET).
    """
    import datetime as dt_module
    from app.utils.time_et import minutes_to_close_et
    
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Check for weekend
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return 0
    
    # Use time_et utility which respects early close days
    now_utc = dt_module.datetime.now(dt_module.timezone.utc)
    return minutes_to_close_et(now_utc)

def load_coefficients(symbol: str) -> Dict[str, float]:
    """
    Load calibrated coefficients from database with fallback to defaults.
    Returns dict with beta_vwap, beta_microtrend, beta_gamma, beta_flow, intercept.
    """
    try:
        from app.models.db_models import load_coefficients as db_load_coefficients
        
        coeff = db_load_coefficients(symbol)
        if coeff:
            return {
                "beta_vwap": coeff.beta_vwap,
                "beta_microtrend": coeff.beta_microtrend,
                "beta_gamma": coeff.beta_gamma,
                "beta_flow": coeff.beta_flow,
                "intercept": coeff.intercept
            }
    except Exception as e:
        pass
    
    # Fallback to defaults
    return DEFAULT_COEFFICIENTS.get(symbol, DEFAULT_COEFFICIENTS["SPX"])

def compute_vwap_deviation(df: pd.DataFrame) -> Tuple[float, float]:
    """
    Calculate VWAP and deviation from current price.
    Returns (vwap, vwap_deviation_pct)
    """
    if df is None or len(df) < 5:
        return 0.0, 0.0
    
    # Use recent data (last 20 bars or session)
    recent_df = df.tail(20).copy()
    
    # Calculate VWAP (simplified - using equal weights since we don't have intraday volume)
    vwap = recent_df['close'].mean()
    
    # Current price
    current_price = df['close'].iloc[-1]
    
    # Deviation as percentage
    if vwap > 0:
        deviation = (current_price - vwap) / vwap
    else:
        deviation = 0.0
    
    return vwap, deviation

def compute_microtrend(df: pd.DataFrame, lookback_bars: int = 10) -> float:
    """
    Calculate microtrend using linear regression on recent prices.
    Returns slope in $/bar (approximates intraday momentum).
    """
    if df is None or len(df) < lookback_bars:
        return 0.0
    
    # Get recent prices
    recent_prices = df['close'].tail(lookback_bars).values
    
    # Linear regression
    x = np.arange(len(recent_prices))
    coeffs = np.polyfit(x, recent_prices, 1)
    
    # Return slope ($/bar)
    return float(coeffs[0])

def compute_gamma_pinning(current_price: float, gex_data: Optional[Dict] = None) -> float:
    """
    Simplified gamma pinning strength.
    Returns normalized pin strength 0-1.
    """
    if gex_data and 'pin_strike' in gex_data:
        pin_strike = gex_data['pin_strike']
        pull_strength = gex_data.get('pull_strength', 0.5)
        
        # Distance to pin
        distance_pct = abs(current_price - pin_strike) / current_price
        
        # Closer = stronger pull
        pin_effect = pull_strength * (1.0 - min(distance_pct, 0.1) * 10)
        return min(1.0, max(0.0, pin_effect))
    
    # Default moderate pinning
    return 0.5

def compute_flow_urgency(df: pd.DataFrame) -> float:
    """
    Simplified flow urgency from price momentum and volatility.
    Returns normalized urgency 0-1.
    """
    if df is None or len(df) < 5:
        return 0.3
    
    # Recent volatility and momentum
    recent_prices = df['close'].tail(10).values
    
    # Momentum score
    momentum = (recent_prices[-1] - recent_prices[0]) / recent_prices[0]
    momentum_score = min(1.0, abs(momentum) * 50)  # Normalize
    
    # Volatility score
    volatility = np.std(recent_prices) / np.mean(recent_prices)
    volatility_score = min(1.0, volatility * 20)
    
    # Combined urgency
    urgency = (momentum_score * 0.6 + volatility_score * 0.4)
    
    return urgency

def compute_orb_features(symbol: str, current_price: float) -> Dict[str, float]:
    """
    Get ORB (Opening Range Breakout) features for the symbol.
    Returns features for ML model integration.
    """
    try:
        from app.state.orb_tracker import get_orb_features, get_orb_tracker
        
        orb_features = get_orb_features(symbol, current_price)
        
        # Convert breakout direction to numeric signal
        # -1 = bearish breakout, 0 = inside range, +1 = bullish breakout
        breakout_signal = 0.0
        if orb_features.breakout_direction == 'bullish':
            breakout_signal = 1.0
        elif orb_features.breakout_direction == 'bearish':
            breakout_signal = -1.0
        
        return {
            "orb_position": orb_features.position_in_range,  # 0-1 (can exceed for breakouts)
            "orb_breakout_signal": breakout_signal,  # -1, 0, +1
            "orb_range_width_pct": orb_features.range_width_pct,  # Range as % of opening
            "orb_distance_to_high_pct": orb_features.distance_to_high_pct,
            "orb_distance_to_low_pct": orb_features.distance_to_low_pct,
            "orb_complete": 1.0 if orb_features.orb_complete else 0.0
        }
    except Exception as e:
        log.warning(f"Error computing ORB features for {symbol}: {e}")
        return {
            "orb_position": 0.5,
            "orb_breakout_signal": 0.0,
            "orb_range_width_pct": 0.0,
            "orb_distance_to_high_pct": 0.0,
            "orb_distance_to_low_pct": 0.0,
            "orb_complete": 0.0
        }


def compute_regime_flags() -> Dict[str, float]:
    """
    Compute regime flags for session-aware predictions.
    These flags help the model adapt to special market conditions.
    
    Returns:
        Dict with regime flags as binary (0.0 or 1.0) or continuous values
    """
    from datetime import timedelta
    from calendar import monthrange
    from app.utils.time_et import is_early_close, now_et as get_now_et, EARLY_CLOSE_ET_DATES, US_MARKET_HOLIDAYS
    
    current_et = get_now_et()
    today = current_et.date()
    
    # is_half_day: Early close day (1 PM ET) - different intraday dynamics
    is_half_day = 1.0 if is_early_close(current_et) else 0.0
    
    # is_holiday_adjacent: Day before or after a holiday
    # Use date strings directly to check against holiday set
    is_holiday_adj = 0.0
    try:
        yesterday = today - timedelta(days=1)
        tomorrow = today + timedelta(days=1)
        yesterday_str = yesterday.isoformat()
        tomorrow_str = tomorrow.isoformat()
        if yesterday_str in US_MARKET_HOLIDAYS or tomorrow_str in US_MARKET_HOLIDAYS:
            is_holiday_adj = 1.0
    except Exception:
        pass
    
    # is_EOM: End of month (last 3 calendar days)
    is_eom = 0.0
    try:
        _, last_day = monthrange(today.year, today.month)
        days_to_eom = last_day - today.day
        if days_to_eom <= 3:
            is_eom = 1.0
    except Exception:
        pass
    
    # is_EOW: End of week (Thursday, Friday)
    is_eow = 1.0 if current_et.weekday() >= 3 else 0.0  # Thu=3, Fri=4
    
    # VIX regime: Placeholder - will be populated with actual VIX if available
    vix_regime = 0.5  # Default to normal
    
    return {
        "regime_half_day": is_half_day,
        "regime_holiday_adjacent": is_holiday_adj,
        "regime_eom": is_eom,
        "regime_eow": is_eow,
        "regime_vix": vix_regime,
    }


def compute_gamma_snapshot_features(current_price: float, gex_data: Optional[Dict]) -> Dict[str, float]:
    """
    Extract gamma snapshot features for short-horizon predictions.
    These features are critical for the final 30-60 minutes before close.
    
    Args:
        current_price: Current spot price
        gex_data: Gamma exposure data from calculate_gamma_exposure()
    
    Returns:
        Dict with gamma-derived features
    """
    if not gex_data or 'pin_strike' not in gex_data:
        return {
            "gamma_distance_to_pin": 0.0,
            "gamma_distance_to_pin_pct": 0.0,
            "gamma_net_level": 0.0,
            "gamma_aggregate_total": 0.0,
            "gamma_aggregate_net": 0.0,
        }
    
    pin_strike = gex_data.get('pin_strike', current_price)
    
    # Distance from spot to pin (in points and percentage)
    distance_pts = pin_strike - current_price
    distance_pct = (distance_pts / current_price) * 100 if current_price > 0 else 0.0
    
    # Net GEX at pin strike (normalized by dividing by typical levels)
    net_gex = gex_data.get('net_gex', 0.0)
    net_gex_normalized = net_gex / 1e9 if abs(net_gex) > 0 else 0.0  # Normalize to billions
    
    # Aggregate GEX across all strikes (if available)
    aggregate_total = gex_data.get('aggregate_total_gex', gex_data.get('total_gex', 0.0))
    aggregate_net = gex_data.get('aggregate_net_gex', gex_data.get('net_gex', 0.0))
    
    # Normalize to log scale for model input (prevents extreme values)
    import math
    agg_total_log = math.log1p(abs(aggregate_total / 1e9)) if aggregate_total else 0.0
    agg_net_sign = 1 if aggregate_net >= 0 else -1
    agg_net_log = agg_net_sign * math.log1p(abs(aggregate_net / 1e9)) if aggregate_net else 0.0
    
    return {
        "gamma_distance_to_pin": distance_pts,
        "gamma_distance_to_pin_pct": distance_pct,
        "gamma_net_level": net_gex_normalized,
        "gamma_aggregate_total": agg_total_log,
        "gamma_aggregate_net": agg_net_log,
    }


def compute_features(df: pd.DataFrame, gex_data: Optional[Dict] = None, symbol: str = "SPX") -> Dict[str, float]:
    """
    Compute all features required for Ridge regression prediction.
    Now includes ORB, regime flags, and gamma snapshot features.
    """
    current_price = df['close'].iloc[-1]
    
    # Core features
    vwap, vwap_dev = compute_vwap_deviation(df)
    microtrend = compute_microtrend(df)
    gamma_pin = compute_gamma_pinning(current_price, gex_data)
    flow_urgency = compute_flow_urgency(df)
    
    # ORB features
    orb_features = compute_orb_features(symbol, current_price)
    
    # Regime flags - session-aware features
    regime_features = compute_regime_flags()
    
    # Gamma snapshot features - critical for final hour predictions
    gamma_features = compute_gamma_snapshot_features(current_price, gex_data)
    
    features = {
        "vwap": vwap,
        "vwap_deviation": vwap_dev,
        "microtrend": microtrend,
        "gamma_pin": gamma_pin,
        "flow_urgency": flow_urgency,
        "current_price": current_price,
    }
    
    # Merge all feature sets
    features.update(orb_features)
    features.update(regime_features)
    features.update(gamma_features)
    
    return features

def get_time_adaptive_weights(minutes_to_close: int) -> Tuple[float, float]:
    """
    Calculate time-adaptive weights for microtrend and flow features.
    
    Returns (microtrend_weight, flow_weight)
    
    Time buckets:
    - τ ≤ 15 min (3:45-4:00 PM): microtrend×1.5, flow×1.3
    - τ ≤ 30 min (3:30-3:45 PM): microtrend×1.2, flow×1.1
    - τ > 30 min (before 3:30 PM): microtrend×1.0, flow×1.0
    """
    if minutes_to_close <= 15:
        return 1.5, 1.3
    elif minutes_to_close <= 30:
        return 1.2, 1.1
    else:
        return 1.0, 1.0

def predict_time_adaptive(features: dict, now_et: datetime, close_et: datetime, last_price: float) -> float:
    """
    Conservative Time-Adaptive formula with regime awareness.
    Includes ORB features, gamma snapshot features, and regime flags.
    """
    minutes_to_close = max(int((close_et - now_et).total_seconds() // 60), 0)
    
    # Core features
    vwap_dev = float(features.get("vwap_deviation", 0.0))        # fraction, e.g., +0.003 = +0.3%
    micro = float(features.get("microtrend", 0.0))               # $/bar on shortest window
    gamma_pull = float(features.get("gamma_pull", 0.0))          # $ target toward pin
    flow_urg = float(features.get("flow_urgency", 0.0))          # 0..1
    
    # ORB features
    orb_breakout_signal = float(features.get("orb_breakout_signal", 0.0))  # -1, 0, +1
    orb_position = float(features.get("orb_position", 0.5))  # 0-1 (can exceed for breakouts)
    orb_range_width_pct = float(features.get("orb_range_width_pct", 0.0))  # Range as % of opening
    orb_complete = float(features.get("orb_complete", 0.0))  # 1.0 if ORB period complete
    
    # Regime flags (new) - adjust model behavior based on session type
    regime_half_day = float(features.get("regime_half_day", 0.0))
    regime_holiday_adj = float(features.get("regime_holiday_adjacent", 0.0))
    regime_eom = float(features.get("regime_eom", 0.0))
    regime_eow = float(features.get("regime_eow", 0.0))
    
    # Gamma snapshot features (new)
    gamma_distance_pct = float(features.get("gamma_distance_to_pin_pct", 0.0))  # Signed % distance to pin
    gamma_net_level = float(features.get("gamma_net_level", 0.0))  # Normalized net GEX
    
    # Horizon scalers - adjust for half-day sessions
    effective_session_length = 210 if regime_half_day > 0.5 else 390  # 3.5h vs 6.5h in minutes
    t = min(minutes_to_close, 60) / 60.0  # 0..1, last hour emphasized
    t_gamma = 1.0 - t  # INVERTED for gamma - strongest at close
    
    # Coefficients with regime adjustments
    k_vwap = 0.25
    k_micro = 0.15
    k_gamma = 0.65
    k_flow = 0.10
    k_orb = 0.20
    
    # Regime-based coefficient adjustments
    # Half-day sessions: Reduce VWAP influence (less mean-reversion time), increase gamma
    if regime_half_day > 0.5:
        k_vwap *= 0.7
        k_gamma *= 1.2
        k_micro *= 1.1  # Microtrend matters more in compressed sessions
    
    # EOM: Allow more drift toward the close (month-end rebalancing)
    if regime_eom > 0.5:
        k_micro *= 1.15
        k_vwap *= 0.85  # Less mean-reversion pressure
    
    # Holiday-adjacent: Lower liquidity, more volatile microtrend
    if regime_holiday_adj > 0.5:
        k_micro *= 0.8  # Less reliable microtrend signal
        k_flow *= 1.2   # Flow becomes more important
    
    delta_from_vwap = k_vwap * (vwap_dev * last_price) * t
    delta_from_micro = k_micro * micro * min(minutes_to_close, 20)
    delta_from_gamma = k_gamma * ((gamma_pull - last_price) * (0.30 + 0.70 * t_gamma))
    delta_from_flow = k_flow * (flow_urg - 0.5) * 0.006 * last_price
    
    # Enhanced gamma adjustment based on net gamma level
    # High positive gamma = strong pinning, reduce delta from gamma
    # High negative gamma = more volatility expected
    if gamma_net_level > 0.1:  # Strong positive gamma - strong pinning
        delta_from_gamma *= 1.2  # Increase pull toward pin
    elif gamma_net_level < -0.1:  # Negative gamma - breakaway possible
        delta_from_gamma *= 0.8  # Reduce pin pull, price may break away
    
    # ORB contribution - if breakout confirmed, trend continuation is likely
    delta_from_orb = 0.0
    if orb_complete > 0.5 and orb_range_width_pct > 0.1:
        range_multiplier = min(orb_range_width_pct / 0.5, 1.5)
        delta_from_orb = k_orb * orb_breakout_signal * range_multiplier * last_price * 0.003
    
    pred = last_price + delta_from_vwap + delta_from_micro + delta_from_gamma + delta_from_flow + delta_from_orb
    
    # Conditional bounds: widen when strong gamma pinning is detected OR clear breakout
    distance_to_pin_pct = abs(gamma_pull - last_price) / last_price
    
    # Check for strong ORB breakout (widen bounds for trend continuation)
    strong_orb_breakout = orb_complete > 0.5 and abs(orb_breakout_signal) > 0.5 and orb_range_width_pct > 0.2
    
    if distance_to_pin_pct > 0.025 and minutes_to_close <= 60:
        # Widen bounds to allow reaching the gamma pin (up to ±5%)
        max_move = min(distance_to_pin_pct * 1.2, 0.05)  # Cap at 5%
        lo = last_price * (1 - max_move)
        hi = last_price * (1 + max_move)
    elif strong_orb_breakout:
        # Widen bounds for confirmed ORB breakout (trend continuation)
        max_move = 0.03  # Allow up to ±3% for strong breakouts
        lo = last_price * (1 - max_move)
        hi = last_price * (1 + max_move)
    else:
        # Default conservative bounds: ±2.5% intraday
        lo = last_price * 0.975
        hi = last_price * 1.025
    
    return min(max(pred, lo), hi)

def predict(symbol: str, df: pd.DataFrame, gex_data: Optional[Dict] = None) -> PredictionResult:
    """
    Generate time-adaptive Ridge regression prediction using conservative formula.
    Now incorporates ORB (Opening Range Breakout) features.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        df: Historical price dataframe with OHLC data
        gex_data: Optional gamma exposure data from gamma analysis
    
    Returns:
        PredictionResult with prediction and metadata
    """
    # Compute features (now includes ORB features)
    features = compute_features(df, gex_data, symbol=symbol)
    current_price = features["current_price"]
    
    # Get time context - respects early close days
    from app.utils.time_et import close_time_et, now_et as get_now_et
    now_et = get_now_et()
    close_et = close_time_et(now_et)
    
    # Get gamma pull price if available
    if gex_data and 'pin_strike' in gex_data:
        features["gamma_pull"] = gex_data['pin_strike']
    else:
        features["gamma_pull"] = current_price  # No pull if no gamma data
    
    # Use conservative prediction formula
    predicted_close = predict_time_adaptive(features, now_et, close_et, current_price)
    
    # Calculate minutes to close for confidence
    minutes_to_close = get_minutes_to_close()
    
    # Calculate confidence based on time to close
    # Confidence increases as we approach close (more certainty)
    if minutes_to_close <= 15:
        base_confidence = 85
    elif minutes_to_close <= 30:
        base_confidence = 75
    elif minutes_to_close <= 60:
        base_confidence = 65
    else:
        base_confidence = 55
    
    # Adjust confidence based on feature agreement
    vwap_direction = 1 if features["vwap_deviation"] > 0 else -1
    micro_direction = 1 if features["microtrend"] > 0 else -1
    orb_direction = features.get("orb_breakout_signal", 0)
    orb_complete = features.get("orb_complete", 0)
    
    # Base confidence boost from VWAP/microtrend agreement
    confidence_boost = 0
    if vwap_direction == micro_direction:
        confidence_boost += 5
    else:
        confidence_boost -= 5
    
    # Additional boost if ORB breakout confirms other signals
    if orb_complete > 0.5:
        if (orb_direction > 0 and vwap_direction > 0 and micro_direction > 0):
            confidence_boost += 5  # All bullish signals align
        elif (orb_direction < 0 and vwap_direction < 0 and micro_direction < 0):
            confidence_boost += 5  # All bearish signals align
        elif orb_direction != 0 and (orb_direction != vwap_direction or orb_direction != micro_direction):
            confidence_boost -= 2  # Conflicting signals
    
    confidence = max(45, min(95, base_confidence + confidence_boost))
    
    # Generate recommendation - include ORB status
    orb_status = ""
    if orb_complete > 0.5:
        if orb_direction > 0:
            orb_status = " + Bullish ORB Breakout"
        elif orb_direction < 0:
            orb_status = " + Bearish ORB Breakout"
        else:
            orb_status = " + Inside ORB Range"
    
    if minutes_to_close <= 30:
        recommendation = f"Time-Adaptive (Recommended){orb_status}"
    else:
        recommendation = f"Traditional ML (Recommended){orb_status}"
    
    return PredictionResult(
        predicted_price=predicted_close,
        current_price=current_price,
        confidence=confidence,
        model_name="Time-Adaptive Ridge",
        features=features,
        time_to_close_minutes=minutes_to_close,
        recommendation=recommendation
    )
