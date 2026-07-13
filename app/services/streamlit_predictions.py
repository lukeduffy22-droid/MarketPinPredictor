"""Prediction helpers for the Streamlit dashboard."""

import numpy as np

from app.features.technical_indicators import calculate_technical_indicators

def predict_eod_price(df, model_type='Linear Regression', timeframe='1-day', gex_data=None, indicator_params=None):
    """Predict end-of-day price using technical indicators, ML, and gamma pin alignment
    
    Args:
        df: DataFrame with OHLCV data
        model_type: 'Linear Regression' or 'Random Forest'
        timeframe: '1-day', '5-day', or '1-week'
        gex_data: Optional gamma exposure data with pin_strike for EOD alignment
    """
    if df is None or len(df) < 25:
        print(f"DEBUG: Initial check failed - df is None: {df is None}, len: {len(df) if df is not None else 0}")
        return None, None, None, None, "Initial data check failed (need 25+ rows)"
    
    # Calculate technical indicators
    df = calculate_technical_indicators(df, indicator_params)
    
    # Check which columns have NaN values
    nan_cols = df.columns[df.isna().any()].tolist()
    print(f"DEBUG: After indicators - {len(df)} rows, NaN columns: {nan_cols}")
    
    # Only drop rows with NaN in the feature columns we actually use
    feature_columns = ['SMA_5', 'SMA_10', 'SMA_20', 'EMA_5', 'EMA_10', 
                       'RSI', 'MACD', 'Signal_Line', 'Momentum', 'ROC', 
                       'Volume_Ratio', 'BB_Upper', 'BB_Lower', 'VWAP', 'AMA']
    
    # Check if all feature columns exist
    missing_cols = [c for c in feature_columns if c not in df.columns]
    if missing_cols:
        print(f"DEBUG: Missing columns: {missing_cols}")
        return None, None, None, None, f"Missing columns: {missing_cols}"
    
    # Drop rows only where feature columns have NaN
    df_clean = df.dropna(subset=feature_columns + ['close']).copy()
    print(f"DEBUG: After dropna on features - {len(df_clean)} rows")
    
    if len(df_clean) < 10:
        print(f"DEBUG: Not enough clean rows: {len(df_clean)}")
        return None, None, None, None, f"Only {len(df_clean)} clean rows (need 10+)"
    
    # Determine shift based on timeframe
    if timeframe == '1-day':
        shift_days = 1
    elif timeframe == '5-day':
        shift_days = 5
    elif timeframe == '1-week':
        shift_days = 7
    else:
        shift_days = 1
    
    # Create feature matrix (X) and target vector (y)
    # Shift target by shift_days to predict future close
    df_clean['next_close'] = df_clean['close'].shift(-shift_days)
    
    # Remove rows with NaN for next_close
    df_model = df_clean[:-shift_days].dropna(subset=feature_columns + ['next_close']).copy()
    print(f"DEBUG: After shift removal - {len(df_model)} rows for model")
    
    if len(df_model) < 10:
        print(f"DEBUG: Not enough model rows: {len(df_model)}")
        return None, None, None, None, f"Only {len(df_model)} rows after shift (need 10+)"
    
    X = df_model[feature_columns].values
    y = df_model['next_close'].values
    
    # Split: use earlier data for training, recent data for testing
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    from models import train_linear_regression, train_random_forest
    
    # Train model based on selected type
    if model_type == 'Linear Regression':
        model, scaler, accuracy = train_linear_regression(X_train, y_train, X_test, y_test)
    elif model_type == 'Random Forest':
        model, scaler, accuracy = train_random_forest(X_train, y_train, X_test, y_test)
    else:
        model, scaler, accuracy = train_linear_regression(X_train, y_train, X_test, y_test)
    
    # Get current price (last known close)
    current_price = df_clean['close'].iloc[-1]
    
    # Predict future price using the most recent features
    latest_features = df_clean[feature_columns].iloc[-1].values.reshape(1, -1)
    latest_scaled = scaler.transform(latest_features)
    ml_predicted_price = model.predict(latest_scaled)[0]
    
    # GAMMA PIN ALIGNMENT - Critical for EOD predictions
    # Gamma pin exerts strong magnetic pull on prices, especially near close
    gamma_pin = None
    gamma_weight = 0.0
    
    if gex_data and 'pin_strike' in gex_data and gex_data['pin_strike']:
        gamma_pin = gex_data['pin_strike']
        pull_strength = gex_data.get('pull_strength', 0)
        
        # Validate gamma pin is reasonable (within 5% of current price for same-day, 15% for multi-day)
        max_deviation = 0.05 if timeframe == '1-day' else 0.15
        if gamma_pin and abs(gamma_pin - current_price) / current_price < max_deviation:
            # CRITICAL: For same-day EOD predictions, gamma pin is THE dominant factor
            # Research shows prices are "magnetically" pulled to gamma pins at close
            # The ML model predicts T+1 (next day), so for same-day EOD we rely primarily on gamma
            if timeframe == '1-day':
                # SAME-DAY EOD: Gamma pin dominates (70-85% weight)
                # ML model was trained on T+1 data, not same-day, so trust gamma more
                # Higher pull_strength = stronger magnet effect = more weight
                gamma_weight = min(0.85, 0.70 + (pull_strength / 100) * 0.15)
            elif timeframe == '5-day':
                # Multi-day: gamma less influential (pins shift daily)
                gamma_weight = min(0.35, 0.20 + (pull_strength / 100) * 0.15)
            else:
                # Weekly: minimal gamma influence
                gamma_weight = min(0.20, 0.10 + (pull_strength / 100) * 0.10)
            
            print(f"DEBUG: Gamma pin at ${gamma_pin:.2f}, pull strength: {pull_strength}%, weight: {gamma_weight:.1%}")
        else:
            deviation_pct = abs(gamma_pin - current_price) / current_price * 100
            print(f"DEBUG: Gamma pin ${gamma_pin} rejected ({deviation_pct:.1f}% from current ${current_price:.2f}, max allowed {max_deviation*100}%)")
            gamma_pin = None
    
    # Blend ML prediction with gamma pin
    if gamma_pin and gamma_weight > 0:
        # Weighted average: ML model + gamma pin attraction
        predicted_price = (ml_predicted_price * (1 - gamma_weight)) + (gamma_pin * gamma_weight)
        print(f"DEBUG: Blended prediction: ML=${ml_predicted_price:.2f} + Gamma=${gamma_pin:.2f} (weight={gamma_weight:.1%}) = ${predicted_price:.2f}")
    else:
        predicted_price = ml_predicted_price
        print(f"DEBUG: Pure ML prediction (no valid gamma): ${predicted_price:.2f}")
    
    # Calculate confidence based on recent trend consistency and model performance
    recent_prices = df_clean['close'].tail(10).values
    price_std = np.std(recent_prices)
    price_mean = np.mean(recent_prices)
    volatility = (price_std / price_mean) * 100
    
    # Confidence decreases with volatility and poor accuracy
    base_confidence = min(accuracy, 85)
    confidence = max(40, base_confidence - (volatility * 2))
    
    # Boost confidence if gamma alignment is strong
    if gamma_pin and gamma_weight > 0.3:
        confidence = min(95, confidence + 5)  # Slight confidence boost for strong gamma alignment
    
    print(f"DEBUG: Prediction successful! Price: {predicted_price:.2f}, Confidence: {confidence:.1f}%")
    return predicted_price, confidence, df_clean, current_price, None
