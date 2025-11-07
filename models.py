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
