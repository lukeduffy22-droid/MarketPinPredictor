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
    """Calculate minutes until market close (4:00 PM ET) with proper bounds"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Market close time
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Floor at 0 for after hours, weekends, holidays
    if now_et >= market_close:
        return 0
    if now_et.weekday() >= 5:  # Saturday=5, Sunday=6
        return 0
    
    # Calculate difference
    diff = market_close - now_et
    minutes = int(diff.total_seconds() / 60)
    
    # Cap at 0 minimum
    return max(0, minutes)

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


def compute_features(df: pd.DataFrame, gex_data: Optional[Dict] = None, symbol: str = "SPX") -> Dict[str, float]:
    """
    Compute all features required for Ridge regression prediction.
    Now includes ORB (Opening Range Breakout) features.
    """
    current_price = df['close'].iloc[-1]
    
    # Core features
    vwap, vwap_dev = compute_vwap_deviation(df)
    microtrend = compute_microtrend(df)
    gamma_pin = compute_gamma_pinning(current_price, gex_data)
    flow_urgency = compute_flow_urgency(df)
    
    # ORB features (new)
    orb_features = compute_orb_features(symbol, current_price)
    
    features = {
        "vwap": vwap,
        "vwap_deviation": vwap_dev,
        "microtrend": microtrend,
        "gamma_pin": gamma_pin,
        "flow_urgency": flow_urgency,
        "current_price": current_price,
    }
    
    # Merge ORB features
    features.update(orb_features)
    
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
    """Conservative Time-Adaptive formula with tight bounds, now including ORB features"""
    minutes_to_close = max(int((close_et - now_et).total_seconds() // 60), 0)
    
    # Normalized feature taps
    vwap_dev = float(features.get("vwap_deviation", 0.0))        # fraction, e.g., +0.003 = +0.3%
    micro = float(features.get("microtrend", 0.0))               # $/bar on shortest window
    gamma_pull = float(features.get("gamma_pull", 0.0))          # $ target toward pin
    flow_urg = float(features.get("flow_urgency", 0.0))          # 0..1
    
    # ORB features (new)
    orb_breakout_signal = float(features.get("orb_breakout_signal", 0.0))  # -1, 0, +1
    orb_position = float(features.get("orb_position", 0.5))  # 0-1 (can exceed for breakouts)
    orb_range_width_pct = float(features.get("orb_range_width_pct", 0.0))  # Range as % of opening
    orb_complete = float(features.get("orb_complete", 0.0))  # 1.0 if ORB period complete
    
    # Horizon scalers
    t = min(minutes_to_close, 60) / 60.0                         # 0..1, last hour emphasized
    t_gamma = 1.0 - t  # INVERTED for gamma - strongest at close (0 min = 1.0, 60 min = 0.0)
    
    # Coefficients with stronger gamma influence
    k_vwap = 0.25  # Slightly reduced to make room for ORB
    k_micro = 0.15
    k_gamma = 0.65  # Strong gamma influence
    k_flow = 0.10
    k_orb = 0.20  # ORB breakout coefficient (new)
    
    delta_from_vwap = k_vwap * (vwap_dev * last_price) * t
    delta_from_micro = k_micro * micro * min(minutes_to_close, 20)  # assume micro in $/5min or $/bar; no seconds
    delta_from_gamma = k_gamma * ((gamma_pull - last_price) * (0.30 + 0.70 * t_gamma))  # 30–100% of gap, STRONGEST at close
    delta_from_flow = k_flow * (flow_urg - 0.5) * 0.006 * last_price  # ~±0.6% max
    
    # ORB contribution - if breakout confirmed, trend continuation is likely
    # Scale by range width (wider range = more significant breakout)
    # Only apply if ORB is complete
    delta_from_orb = 0.0
    if orb_complete > 0.5 and orb_range_width_pct > 0.1:  # ORB complete and meaningful range
        # orb_breakout_signal: +1 = bullish breakout, -1 = bearish breakout, 0 = inside
        # Scale effect by range width (larger ranges = more conviction)
        range_multiplier = min(orb_range_width_pct / 0.5, 1.5)  # Cap at 1.5x for wide ranges
        
        # Breakouts tend to continue toward EOD (ORB theory)
        # Use the breakout direction to bias the prediction
        delta_from_orb = k_orb * orb_breakout_signal * range_multiplier * last_price * 0.003  # ~±0.3-0.45% contribution
    
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
    
    # Get time context
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    close_et = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    
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
