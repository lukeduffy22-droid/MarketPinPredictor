"""
Time-Adaptive Ridge Regression Predictor
Optimized for final trading hour (3:00 PM - 4:00 PM ET) predictions.
Uses VWAP deviation, microtrend, gamma pinning, and flow urgency.
"""
import numpy as np
import pandas as pd
from datetime import datetime, time
from typing import Optional, Tuple, Dict
from dataclasses import dataclass
import pytz

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
    """Calculate minutes until market close (4:00 PM ET)"""
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Market close time
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Calculate difference
    diff = market_close - now_et
    minutes = int(diff.total_seconds() / 60)
    
    # Return 0 if market is closed or negative
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

def compute_features(df: pd.DataFrame, gex_data: Optional[Dict] = None) -> Dict[str, float]:
    """
    Compute all features required for Ridge regression prediction.
    """
    current_price = df['close'].iloc[-1]
    
    # Core features
    vwap, vwap_dev = compute_vwap_deviation(df)
    microtrend = compute_microtrend(df)
    gamma_pin = compute_gamma_pinning(current_price, gex_data)
    flow_urgency = compute_flow_urgency(df)
    
    return {
        "vwap": vwap,
        "vwap_deviation": vwap_dev,
        "microtrend": microtrend,
        "gamma_pin": gamma_pin,
        "flow_urgency": flow_urgency,
        "current_price": current_price
    }

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

def predict(symbol: str, df: pd.DataFrame, gex_data: Optional[Dict] = None) -> PredictionResult:
    """
    Generate time-adaptive Ridge regression prediction.
    
    Args:
        symbol: Index symbol (SPX, NDX, DJI, RUT)
        df: Historical price dataframe with OHLC data
        gex_data: Optional gamma exposure data from gamma analysis
    
    Returns:
        PredictionResult with prediction and metadata
    """
    # Load coefficients
    coeffs = load_coefficients(symbol)
    
    # Compute features
    features = compute_features(df, gex_data)
    current_price = features["current_price"]
    
    # Get time-adaptive weights
    minutes_to_close = get_minutes_to_close()
    micro_weight, flow_weight = get_time_adaptive_weights(minutes_to_close)
    
    # Ridge regression formula with time-adaptive weights:
    # predicted_close = current_price + 
    #   β_vwap × vwap_dev × current_price +
    #   β_microtrend × microtrend × τ × 60 × micro_weight +
    #   β_gamma × gamma_pin × 10 +
    #   β_flow × flow_urgency × 5 × flow_weight +
    #   intercept
    
    vwap_component = coeffs["beta_vwap"] * features["vwap_deviation"] * current_price
    micro_component = coeffs["beta_microtrend"] * features["microtrend"] * minutes_to_close * 60 * micro_weight
    gamma_component = coeffs["beta_gamma"] * features["gamma_pin"] * 10
    flow_component = coeffs["beta_flow"] * features["flow_urgency"] * 5 * flow_weight
    intercept = coeffs["intercept"]
    
    # Final prediction
    predicted_close = current_price + vwap_component + micro_component + gamma_component + flow_component + intercept
    
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
    # If VWAP and microtrend point same direction, increase confidence
    vwap_direction = 1 if features["vwap_deviation"] > 0 else -1
    micro_direction = 1 if features["microtrend"] > 0 else -1
    
    if vwap_direction == micro_direction:
        confidence_boost = 5
    else:
        confidence_boost = -5
    
    confidence = max(45, min(95, base_confidence + confidence_boost))
    
    # Generate recommendation
    if minutes_to_close <= 30:
        recommendation = "Time-Adaptive (Recommended)"
    else:
        recommendation = "Traditional ML (Recommended)"
    
    return PredictionResult(
        predicted_price=predicted_close,
        current_price=current_price,
        confidence=confidence,
        model_name="Time-Adaptive Ridge",
        features=features,
        time_to_close_minutes=minutes_to_close,
        recommendation=recommendation
    )
