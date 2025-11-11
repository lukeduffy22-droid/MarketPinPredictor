"""
Machine Learning models for stock price prediction
"""
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

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

def create_lstm_model(input_shape):
    """Create LSTM model for time series prediction"""
    model = keras.Sequential([
        layers.LSTM(50, return_sequences=True, input_shape=input_shape),
        layers.Dropout(0.2),
        layers.LSTM(50, return_sequences=False),
        layers.Dropout(0.2),
        layers.Dense(25),
        layers.Dense(1)
    ])
    
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

def prepare_lstm_data(data, lookback=10):
    """Prepare data for LSTM model"""
    X, y = [], []
    for i in range(lookback, len(data)):
        X.append(data[i-lookback:i])
        y.append(data[i])
    return np.array(X), np.array(y)

def train_lstm(X_train, y_train, X_test, y_test, epochs=50):
    """Train LSTM model"""
    # Reshape for LSTM
    if len(X_train.shape) == 2:
        lookback = 10
        X_train_lstm, y_train_lstm = prepare_lstm_data(X_train, lookback)
        X_test_lstm, y_test_lstm = prepare_lstm_data(X_test, lookback)
    else:
        X_train_lstm, y_train_lstm = X_train, y_train
        X_test_lstm, y_test_lstm = X_test, y_test
    
    # Scale data
    scaler = StandardScaler()
    
    # Flatten for scaling
    n_samples_train, n_timesteps, n_features = X_train_lstm.shape
    X_train_reshaped = X_train_lstm.reshape(-1, n_features)
    X_train_scaled = scaler.fit_transform(X_train_reshaped)
    X_train_scaled = X_train_scaled.reshape(n_samples_train, n_timesteps, n_features)
    
    n_samples_test = X_test_lstm.shape[0]
    X_test_reshaped = X_test_lstm.reshape(-1, n_features)
    X_test_scaled = scaler.transform(X_test_reshaped)
    X_test_scaled = X_test_scaled.reshape(n_samples_test, n_timesteps, n_features)
    
    # Create and train model
    model = create_lstm_model((n_timesteps, n_features))
    
    # Train with early stopping
    early_stop = keras.callbacks.EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)
    
    model.fit(
        X_train_scaled, y_train_lstm,
        epochs=epochs,
        batch_size=32,
        validation_split=0.1,
        callbacks=[early_stop],
        verbose=0
    )
    
    # Calculate accuracy
    test_predictions = model.predict(X_test_scaled, verbose=0)
    mape = np.mean(np.abs((y_test_lstm - test_predictions.flatten()) / y_test_lstm)) * 100
    accuracy = max(0, 100 - mape)
    
    return model, scaler, accuracy, n_timesteps

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
