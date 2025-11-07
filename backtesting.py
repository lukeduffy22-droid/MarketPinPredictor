"""
Backtesting module for validating and improving predictions
Uses historical Polygon data to test prediction accuracy
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from polygon import StocksClient, OptionsClient
import streamlit as st
from database import save_prediction
from options_gamma import get_gamma_analysis, fetch_options_chain, calculate_gamma_exposure

def fetch_historical_index_data(api_key, ticker, date, lookback_days=60):
    """
    Fetch historical data up to a specific date for backtesting
    
    Args:
        api_key: Polygon API key
        ticker: Index ticker (ETF proxy)
        date: The date to get data up to (datetime)
        lookback_days: How many days of history to fetch
    
    Returns:
        DataFrame with OHLCV data
    """
    try:
        client = StocksClient(api_key)
        
        # Calculate date range
        end_date = date.strftime('%Y-%m-%d')
        start_date = (date - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
        
        # Fetch aggregates
        aggs = client.get_aggregate_bars(
            ticker,
            start_date,
            end_date,
            timespan='day',
            limit=50000
        )
        
        # Handle response format
        if isinstance(aggs, dict):
            if 'results' in aggs:
                aggs_list = aggs['results']
            else:
                return None
        else:
            aggs_list = aggs
        
        if not aggs_list:
            return None
        
        # Convert to DataFrame
        data = []
        for bar in aggs_list:
            if isinstance(bar, dict):
                data.append({
                    'timestamp': pd.to_datetime(bar['t'], unit='ms'),
                    'open': bar['o'],
                    'high': bar['h'],
                    'low': bar['l'],
                    'close': bar['c'],
                    'volume': bar['v']
                })
        
        df = pd.DataFrame(data)
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        
        return df
        
    except Exception as e:
        st.warning(f"Error fetching historical data for {ticker}: {str(e)}")
        return None

def get_actual_eod_price(api_key, ticker, date):
    """
    Get the actual end-of-day closing price for a specific date
    
    Args:
        api_key: Polygon API key
        ticker: Index ticker (ETF proxy)
        date: Date to get EOD price for
    
    Returns:
        float: Closing price
    """
    try:
        client = StocksClient(api_key)
        
        date_str = date.strftime('%Y-%m-%d')
        
        # Get daily bar for that specific date
        aggs = client.get_aggregate_bars(
            ticker,
            date_str,
            date_str,
            timespan='day',
            limit=1
        )
        
        # Handle response format
        if isinstance(aggs, dict):
            if 'results' in aggs and len(aggs['results']) > 0:
                return aggs['results'][0]['c']
        
        return None
        
    except Exception as e:
        st.warning(f"Error fetching EOD price for {ticker} on {date}: {str(e)}")
        return None

def calculate_technical_indicators(df):
    """Calculate technical indicators for backtesting"""
    df = df.copy()
    
    # Moving averages
    df['SMA_5'] = df['close'].rolling(window=5).mean()
    df['SMA_10'] = df['close'].rolling(window=10).mean()
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    df['EMA_5'] = df['close'].ewm(span=5).mean()
    df['EMA_10'] = df['close'].ewm(span=10).mean()
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    df['EMA_12'] = df['close'].ewm(span=12).mean()
    df['EMA_26'] = df['close'].ewm(span=26).mean()
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['Signal_Line'] = df['MACD'].ewm(span=9).mean()
    
    # Bollinger Bands
    df['BB_Middle'] = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
    
    # Momentum
    df['Momentum'] = df['close'] - df['close'].shift(10)
    
    # Volume-based indicators
    df['Volume_SMA'] = df['volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['volume'] / df['Volume_SMA']
    
    return df

def make_backtest_prediction(df, model_type='linear_regression'):
    """
    Make a prediction using the same logic as the main app
    
    Args:
        df: DataFrame with historical data and indicators
        model_type: Type of model to use
    
    Returns:
        predicted_price, confidence
    """
    from sklearn.linear_model import LinearRegression
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.preprocessing import StandardScaler
    
    try:
        # Prepare features
        feature_cols = ['SMA_5', 'SMA_10', 'SMA_20', 'EMA_5', 'EMA_10', 
                       'RSI', 'MACD', 'Signal_Line', 'Momentum', 'Volume_Ratio']
        
        # Drop NaN values
        df_clean = df[feature_cols + ['close']].dropna()
        
        if len(df_clean) < 20:
            return None, 0.0
        
        # Prepare training data
        X = df_clean[feature_cols].values[:-1]  # All but last row
        y = df_clean['close'].values[1:]  # Shifted by 1 (next day's price)
        
        # Use last row for prediction
        X_current = df_clean[feature_cols].values[-1].reshape(1, -1)
        
        # Scale features
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        X_current_scaled = scaler.transform(X_current)
        
        # Train model
        if model_type == 'random_forest':
            model = RandomForestRegressor(n_estimators=100, random_state=42)
        else:
            model = LinearRegression()
        
        model.fit(X_scaled, y)
        
        # Make prediction
        predicted_price = model.predict(X_current_scaled)[0]
        
        # Calculate confidence based on R² score
        score = model.score(X_scaled, y)
        confidence = max(0, min(100, score * 100))
        
        return predicted_price, confidence
        
    except Exception as e:
        st.warning(f"Error making backtest prediction: {str(e)}")
        return None, 0.0

def run_backtest(api_key, ticker_name, index_ticker, etf_ticker, start_date, end_date, model_type='linear_regression'):
    """
    Run backtest for a specific ticker over a date range
    
    Args:
        api_key: Polygon API key
        ticker_name: Display name (e.g., "S&P 500 (SPX)")
        index_ticker: Actual index ticker (e.g., "SPX")
        etf_ticker: ETF proxy for price data (e.g., "SPY")
        start_date: Start date for backtest
        end_date: End date for backtest
        model_type: Type of model to use
    
    Returns:
        DataFrame with backtest results
    """
    results = []
    
    # Generate list of trading days
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends
        if current_date.weekday() < 5:  # Monday = 0, Friday = 4
            
            # Fetch historical data up to PREVIOUS day (T-1)
            # This simulates having data only up to the previous day's close
            previous_date = current_date - timedelta(days=1)
            df = fetch_historical_index_data(api_key, etf_ticker, previous_date, lookback_days=60)
            
            if df is not None and len(df) > 20:
                # Calculate indicators
                df = calculate_technical_indicators(df)
                
                # Get the "current" price (previous day's close, which is what we'd know at market open)
                current_price = df['close'].iloc[-1]
                
                # Make prediction for TODAY (current_date) using data from YESTERDAY
                predicted_price, confidence = make_backtest_prediction(df, model_type)
                
                if predicted_price is not None:
                    # Get actual EOD price for TODAY (current_date)
                    actual_eod = get_actual_eod_price(api_key, etf_ticker, current_date)
                    
                    if actual_eod is not None:
                        # Calculate metrics
                        predicted_change = predicted_price - current_price
                        predicted_change_pct = (predicted_change / current_price) * 100
                        
                        actual_change = actual_eod - current_price
                        actual_change_pct = (actual_change / current_price) * 100
                        
                        # Prediction error
                        error = abs(actual_eod - predicted_price)
                        error_pct = (error / actual_eod) * 100
                        
                        # Direction accuracy
                        direction_correct = (predicted_change > 0 and actual_change > 0) or \
                                          (predicted_change < 0 and actual_change < 0)
                        
                        results.append({
                            'date': current_date,
                            'ticker': ticker_name,
                            'index_ticker': index_ticker,
                            'current_price': current_price,
                            'predicted_eod': predicted_price,
                            'actual_eod': actual_eod,
                            'predicted_change_pct': predicted_change_pct,
                            'actual_change_pct': actual_change_pct,
                            'error': error,
                            'error_pct': error_pct,
                            'direction_correct': direction_correct,
                            'confidence': confidence,
                            'model_type': model_type
                        })
        
        # Move to next day
        current_date += timedelta(days=1)
    
    return pd.DataFrame(results)

def calculate_backtest_metrics(backtest_df):
    """
    Calculate performance metrics from backtest results
    
    Args:
        backtest_df: DataFrame with backtest results
    
    Returns:
        Dictionary with metrics
    """
    if backtest_df.empty:
        return None
    
    total_predictions = len(backtest_df)
    direction_accuracy = (backtest_df['direction_correct'].sum() / total_predictions) * 100
    mean_error_pct = backtest_df['error_pct'].mean()
    median_error_pct = backtest_df['error_pct'].median()
    rmse = np.sqrt((backtest_df['error'] ** 2).mean())
    
    # Best and worst predictions
    best_idx = backtest_df['error_pct'].idxmin()
    worst_idx = backtest_df['error_pct'].idxmax()
    
    metrics = {
        'total_predictions': total_predictions,
        'direction_accuracy': direction_accuracy,
        'mean_error_pct': mean_error_pct,
        'median_error_pct': median_error_pct,
        'rmse': rmse,
        'best_prediction': {
            'date': backtest_df.loc[best_idx, 'date'],
            'error_pct': backtest_df.loc[best_idx, 'error_pct']
        },
        'worst_prediction': {
            'date': backtest_df.loc[worst_idx, 'date'],
            'error_pct': backtest_df.loc[worst_idx, 'error_pct']
        }
    }
    
    return metrics

def optimize_model_features(backtest_df):
    """
    Analyze backtest results to identify which features are most predictive
    
    Returns:
        Dictionary with feature importance and recommended weights
    """
    # This is a simplified version - in production would use more sophisticated analysis
    
    if backtest_df.empty:
        return None
    
    # Group by confidence levels to see if higher confidence = better accuracy
    confidence_bins = pd.cut(backtest_df['confidence'], bins=[0, 50, 70, 100])
    accuracy_by_confidence = backtest_df.groupby(confidence_bins)['direction_correct'].mean()
    
    # Analyze error patterns
    # Get the interval objects from the index
    intervals = accuracy_by_confidence.index.categories
    
    high_conf_interval = intervals[-1] if len(intervals) >= 1 else None
    med_conf_interval = intervals[1] if len(intervals) >= 2 else None
    low_conf_interval = intervals[0] if len(intervals) >= 1 else None
    
    high_accuracy = accuracy_by_confidence[high_conf_interval] * 100 if high_conf_interval is not None else 0
    med_accuracy = accuracy_by_confidence[med_conf_interval] * 100 if med_conf_interval is not None else 0
    low_accuracy = accuracy_by_confidence[low_conf_interval] * 100 if low_conf_interval is not None else 0
    
    error_stats = {
        'high_confidence_accuracy': high_accuracy,
        'medium_confidence_accuracy': med_accuracy,
        'low_confidence_accuracy': low_accuracy,
        'recommendation': 'Trust predictions with confidence > 70%' if high_accuracy > 70 else 'Model needs improvement'
    }
    
    return error_stats