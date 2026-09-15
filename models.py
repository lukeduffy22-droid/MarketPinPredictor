"""
Machine Learning models for stock price prediction
"""
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler

def train_linear_regression(X_train, y_train, X_test, y_test):
    """Train Linear Regression model"""
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    
    test_predictions = model.predict(X_test_scaled)
    mape = np.mean(np.abs((y_test - test_predictions) / y_test)) * 100
    accuracy = max(0, 100 - mape)
    
    return model, scaler, accuracy

def train_random_forest(X_train, y_train, X_test, y_test):
    """Train Random Forest model"""
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train_scaled, y_train)
    
    test_predictions = model.predict(X_test_scaled)
    mape = np.mean(np.abs((y_test - test_predictions) / y_test)) * 100
    accuracy = max(0, 100 - mape)
    
    return model, scaler, accuracy

def blended_eod(last_price, pred_trad, pred_ta, minutes_to_close, realized_vol_5d=0.02):
    """
    Time-aware ensemble blend of Traditional and Time-Adaptive models.
    Weight shifts toward Time-Adaptive as we approach market close.
    
    Args:
        last_price: Current price
        pred_trad: Traditional ML prediction
        pred_ta: Time-Adaptive Ridge prediction
        minutes_to_close: Minutes until market close
        realized_vol_5d: 5-day realized volatility (default 2%)
    
    Returns:
        Blended prediction with volatility guardrail
    """
    # Weight calculation: 0 when far from close -> 1 at close
    w = min(max((60 - minutes_to_close) / 60.0, 0.0), 1.0)
    
    # Weighted blend
    base = (1 - w) * pred_trad + w * pred_ta
    
    # Volatility guardrail: clamp blend to ±k * RV
    k = 1.2  # Allow 120% of recent 1-day RV
    band = last_price * realized_vol_5d * k
    
    # Apply guardrail
    return min(max(base, last_price - band), last_price + band)

def pin_nudge(last_price, pin, minutes_to_close):
    """
    Add small nudge toward gamma pin in final minutes.
    Only applies when price is very close to pin already.
    
    Args:
        last_price: Current price
        pin: Gamma pin strike price
        minutes_to_close: Minutes until market close
    
    Returns:
        Price adjustment toward pin (0 if not applicable)
    """
    if minutes_to_close > 15 or pin is None:
        return 0.0
    
    gap = pin - last_price
    
    # Only nudge if already very close to pin
    if abs(gap) <= 0.0015 * last_price:  # Within 0.15%
        return 0.6 * gap
    elif abs(gap) <= 0.0030 * last_price:  # Within 0.30%
        return 0.35 * gap
    else:
        return 0.15 * gap  # Minimal nudge if further away

def calculate_realized_volatility(price_series, window=5):
    """
    Calculate realized volatility over specified window.
    
    Args:
        price_series: Series of prices
        window: Number of days for calculation (default 5)
    
    Returns:
        Annualized realized volatility
    """
    if len(price_series) < window + 1:
        return 0.02  # Default 2% if insufficient data
    
    # Calculate daily returns
    returns = np.diff(np.log(price_series[-window-1:]))
    
    # Daily volatility
    daily_vol = np.std(returns)
    
    # Annualize (assuming 252 trading days)
    annual_vol = daily_vol * np.sqrt(252)
    
    return annual_vol
