"""Prediction helpers for the Streamlit dashboard."""

import logging

import numpy as np

from app.features.technical_indicators import calculate_technical_indicators

log = logging.getLogger(__name__)


MODEL_LINEAR = 'Linear Regression'
MODEL_RANDOM_FOREST = 'Random Forest'
MODEL_ENSEMBLE = 'Ensemble'


def _timeframe_shift_days(timeframe):
    """Map supported dashboard timeframes to target horizon rows."""
    return {
        '1-day': 1,
        '5-day': 5,
        '1-week': 7,
    }.get(timeframe, 1)


def _safe_float(value, default=None):
    """Convert numeric-like values to finite floats."""
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return default
    return converted if np.isfinite(converted) else default


def _weighted_ensemble_prediction(model_outputs):
    """Blend model outputs with weights derived from holdout accuracy."""
    predictions = np.array([output['prediction'] for output in model_outputs], dtype=float)
    accuracies = np.array([max(output['accuracy'], 0.0) for output in model_outputs], dtype=float)

    if not np.isfinite(predictions).all():
        raise ValueError("Model produced a non-finite prediction")

    if accuracies.sum() <= 0:
        return float(predictions.mean()), 0.0

    weights = accuracies / accuracies.sum()
    return float(np.dot(predictions, weights)), float(accuracies.mean())


def _prediction_guardrail(predicted_price, current_price, recent_prices, shift_days):
    """Clamp predictions to a recent-volatility range to reduce ML outliers."""
    if current_price <= 0 or len(recent_prices) < 3:
        return float(predicted_price)

    recent_prices = np.asarray(recent_prices, dtype=float)
    returns = np.diff(np.log(recent_prices[-min(len(recent_prices), 20):]))
    realized_vol = float(np.nanstd(returns)) if len(returns) else 0.0
    horizon_scale = np.sqrt(max(shift_days, 1))
    band_pct = min(max(realized_vol * horizon_scale * 2.0, 0.0075), 0.08)
    lower_bound = current_price * (1 - band_pct)
    upper_bound = current_price * (1 + band_pct)
    return float(np.clip(predicted_price, lower_bound, upper_bound))

def predict_eod_price(df, model_type='Linear Regression', timeframe='1-day', gex_data=None, indicator_params=None):
    """Predict end-of-day price using technical indicators, ML, and gamma pin alignment
    
    Args:
        df: DataFrame with OHLCV data
        model_type: 'Linear Regression' or 'Random Forest'
        timeframe: '1-day', '5-day', or '1-week'
        gex_data: Optional gamma exposure data with pin_strike for EOD alignment
    """
    if df is None or len(df) < 25:
        log.debug("Initial data check failed; df is None=%s len=%s", df is None, len(df) if df is not None else 0)
        return None, None, None, None, "Initial data check failed (need 25+ rows)"
    
    # Calculate technical indicators
    df = calculate_technical_indicators(df, indicator_params)
    
    # Check which columns have NaN values
    nan_cols = df.columns[df.isna().any()].tolist()
    log.debug("After indicators: %s rows, NaN columns: %s", len(df), nan_cols)
    
    # Only drop rows with NaN in the feature columns we actually use
    feature_columns = ['SMA_5', 'SMA_10', 'SMA_20', 'EMA_5', 'EMA_10', 
                       'RSI', 'MACD', 'Signal_Line', 'Momentum', 'ROC', 
                       'Volume_Ratio', 'BB_Upper', 'BB_Lower', 'VWAP', 'AMA']
    
    # Check if all feature columns exist
    missing_cols = [c for c in feature_columns if c not in df.columns]
    if missing_cols:
        log.debug("Missing feature columns: %s", missing_cols)
        return None, None, None, None, f"Missing columns: {missing_cols}"
    
    # Drop rows only where feature columns have NaN
    df_clean = df.dropna(subset=feature_columns + ['close']).copy()
    log.debug("After feature dropna: %s rows", len(df_clean))
    
    if len(df_clean) < 10:
        log.debug("Not enough clean rows: %s", len(df_clean))
        return None, None, None, None, f"Only {len(df_clean)} clean rows (need 10+)"
    
    shift_days = _timeframe_shift_days(timeframe)
    
    # Create feature matrix (X) and target vector (y)
    # Shift target by shift_days to predict future close
    df_clean['next_close'] = df_clean['close'].shift(-shift_days)
    
    # Remove rows with NaN for next_close
    df_model = df_clean[:-shift_days].dropna(subset=feature_columns + ['next_close']).copy()
    log.debug("After shift removal: %s rows for model", len(df_model))
    
    if len(df_model) < 10:
        log.debug("Not enough model rows: %s", len(df_model))
        return None, None, None, None, f"Only {len(df_model)} rows after shift (need 10+)"
    
    X = df_model[feature_columns].values
    y = df_model['next_close'].values
    
    # Split: use earlier data for training, recent data for testing
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]

    # Get current price (last known close)
    current_price = df_clean['close'].iloc[-1]
    
    # Predict future price using the most recent features
    latest_features = df_clean[feature_columns].iloc[-1].values.reshape(1, -1)

    from models import train_linear_regression, train_random_forest

    model_trainers = {
        MODEL_LINEAR: train_linear_regression,
        MODEL_RANDOM_FOREST: train_random_forest,
    }
    selected_models = [MODEL_LINEAR, MODEL_RANDOM_FOREST] if model_type == MODEL_ENSEMBLE else [model_type]

    model_outputs = []
    for selected_model in selected_models:
        trainer = model_trainers.get(selected_model, train_linear_regression)
        model, scaler, model_accuracy = trainer(X_train, y_train, X_test, y_test)
        latest_scaled = scaler.transform(latest_features)
        model_outputs.append({
            'name': selected_model,
            'prediction': float(model.predict(latest_scaled)[0]),
            'accuracy': float(model_accuracy),
        })

    ml_predicted_price, accuracy = _weighted_ensemble_prediction(model_outputs)
    
    # GAMMA PIN ALIGNMENT - Critical for EOD predictions
    # Gamma pin exerts strong magnetic pull on prices, especially near close
    gamma_pin = None
    gamma_weight = 0.0
    
    if gex_data and 'pin_strike' in gex_data and gex_data['pin_strike']:
        gamma_pin = _safe_float(gex_data.get('pin_strike'))
        pull_strength = min(max(_safe_float(gex_data.get('pull_strength'), 0.0), 0.0), 100.0)
        if gamma_pin is None:
            log.debug("Gamma pin rejected because it is not numeric: %s", gex_data.get('pin_strike'))
            gamma_pin = None
        
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
            
            log.debug("Gamma pin at $%.2f, pull strength %.1f%%, weight %.1f%%", gamma_pin, pull_strength, gamma_weight * 100)
        elif gamma_pin is not None:
            deviation_pct = abs(gamma_pin - current_price) / current_price * 100
            log.debug("Gamma pin $%s rejected (%.1f%% from current $%.2f, max allowed %.1f%%)", gamma_pin, deviation_pct, current_price, max_deviation * 100)
            gamma_pin = None
    
    # Blend ML prediction with gamma pin
    if gamma_pin and gamma_weight > 0:
        # Weighted average: ML model + gamma pin attraction
        predicted_price = (ml_predicted_price * (1 - gamma_weight)) + (gamma_pin * gamma_weight)
        log.debug("Blended prediction: ML=$%.2f + Gamma=$%.2f (weight=%.1f%%) = $%.2f", ml_predicted_price, gamma_pin, gamma_weight * 100, predicted_price)
    else:
        predicted_price = ml_predicted_price
        log.debug("Pure ML prediction without valid gamma: $%.2f", predicted_price)

    predicted_price = _prediction_guardrail(
        predicted_price=predicted_price,
        current_price=current_price,
        recent_prices=df_clean['close'].tail(30).values,
        shift_days=shift_days,
    )
    
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
    
    log.debug("Prediction successful: price %.2f confidence %.1f%% model outputs %s", predicted_price, confidence, model_outputs)
    return predicted_price, confidence, df_clean, current_price, None
