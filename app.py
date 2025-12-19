import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from polygon import RESTClient
from datetime import datetime, timedelta
import time
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from database import init_db, save_prediction, get_predictions_by_ticker, get_all_predictions, save_alert, get_active_alerts, get_prediction_accuracy_stats
from models import train_linear_regression, train_random_forest
from options_gamma import get_gamma_analysis
from backtesting import run_backtest, calculate_backtest_metrics, optimize_model_features
# BACKEND-ONLY WEBSOCKET: Only REST-based helpers are imported here
# WebSocket creation functions are NOT imported - all WebSocket is backend-only
from websocket_streaming import is_market_open, get_snapshot_data
from ai_analysis import (
    analyze_prediction, analyze_streaming_data, 
    get_risk_assessment, explain_gamma_exposure
)
from gamma_viz import show_gamma_evolution_section
from app.core.audit_persistence import load_last_valid_snapshot
from app.utils.market_time import market_is_closed, get_freeze_status

# Initialize database
init_db()

# Page configuration
st.set_page_config(
    page_title="Stock Index Price Predictor",
    page_icon="📈",
    layout="wide"
)

# Major stock indexes - Using actual index tickers
INDEXES = {
    "S&P 500 (SPX)": "SPX",
    "NASDAQ 100 (NDX)": "NDX", 
    "Dow Jones (DJI)": "DJI",
    "Russell 2000 (RUT)": "RUT"
}

# Polygon index ticker format (with I: prefix for indices)
INDEX_POLYGON_TICKERS = {
    "SPX": "I:SPX",
    "NDX": "I:NDX",
    "DJI": "I:DJI",
    "RUT": "I:RUT"
}

# ETF proxies as fallback (only used if direct index data unavailable)
INDEX_ETFS = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "DJI": "DIA",
    "RUT": "IWM"
}

import os

# Initialize session state - load API key from environment if available
if 'api_key' not in st.session_state:
    st.session_state.api_key = os.environ.get('Massive_API', '') or os.environ.get('POLYGON_API_KEY', '')
if 'predictions' not in st.session_state:
    st.session_state.predictions = {}
if 'selected_model' not in st.session_state:
    st.session_state.selected_model = 'Linear Regression'
# REMOVED: ws_stream and streaming_active session state
# WebSocket connections are now managed exclusively by the FastAPI backend
# Streamlit uses REST endpoints to read cached data from the backend
if 'timeframe' not in st.session_state:
    st.session_state.timeframe = '1-day'
if 'alerts' not in st.session_state:
    st.session_state.alerts = []
if 'indicator_params' not in st.session_state:
    st.session_state.indicator_params = {
        'sma_short': 5,
        'sma_medium': 10,
        'sma_long': 20,
        'ema_short': 5,
        'ema_long': 10,
        'rsi_period': 14,
        'macd_fast': 12,
        'macd_slow': 26,
        'macd_signal': 9,
        'bb_period': 20,
        'bb_std': 2,
        'momentum_period': 10
    }

def calculate_kama(prices, n_period=10, fast_period=2, slow_period=30):
    """Calculate Kaufman's Adaptive Moving Average (KAMA)"""
    import numpy as np
    
    # Calculate Efficiency Ratio
    direction = abs(prices - prices.shift(n_period))
    volatility = prices.diff().abs().rolling(window=n_period).sum()
    er = direction / volatility
    
    # Calculate Smoothing Constant
    fastest_sc = 2.0 / (fast_period + 1)
    slowest_sc = 2.0 / (slow_period + 1)
    sc = (er * (fastest_sc - slowest_sc) + slowest_sc) ** 2
    
    # Calculate KAMA
    kama = np.zeros(len(prices))
    kama[:] = np.nan
    
    # First valid KAMA = SMA of first n_period
    first_valid_idx = n_period
    if first_valid_idx < len(prices):
        kama[first_valid_idx] = prices[:first_valid_idx + 1].mean()
        
        # Recursive calculation
        for i in range(first_valid_idx + 1, len(prices)):
            if pd.notna(sc.iloc[i]):
                kama[i] = kama[i-1] + sc.iloc[i] * (prices.iloc[i] - kama[i-1])
            else:
                kama[i] = np.nan
    
    return pd.Series(kama, index=prices.index, name='KAMA')

def calculate_technical_indicators(df, params=None):
    """Calculate technical indicators for prediction with customizable parameters"""
    if params is None:
        params = st.session_state.indicator_params
    
    # Use smaller windows to work with limited data
    sma_short = min(params['sma_short'], 5)
    sma_medium = min(params['sma_medium'], 10)
    sma_long = min(params['sma_long'], 15)  # Reduced from 20
    bb_period = min(params['bb_period'], 15)  # Reduced from 20
    vol_window = min(15, len(df) // 4)  # Adaptive window for volume
    
    # Simple Moving Averages
    df['SMA_5'] = df['close'].rolling(window=sma_short, min_periods=1).mean()
    df['SMA_10'] = df['close'].rolling(window=sma_medium, min_periods=1).mean()
    df['SMA_20'] = df['close'].rolling(window=sma_long, min_periods=1).mean()
    
    # Exponential Moving Averages
    df['EMA_5'] = df['close'].ewm(span=params['ema_short'], adjust=False).mean()
    df['EMA_10'] = df['close'].ewm(span=params['ema_long'], adjust=False).mean()
    
    # VWAP (Volume Weighted Average Price)
    df['Typical_Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['PV'] = df['Typical_Price'] * df['volume']
    cumvol = df['volume'].cumsum()
    cumvol = cumvol.replace(0, np.nan)  # Avoid division by zero
    df['VWAP'] = df['PV'].cumsum() / cumvol
    df['VWAP'] = df['VWAP'].ffill().bfill()  # Fill any NaN
    
    # Kaufman's Adaptive Moving Average (AMA/KAMA) - with fallback
    try:
        df['AMA'] = calculate_kama(df['close'], n_period=min(10, len(df)//5), fast_period=2, slow_period=min(20, len(df)//3))
        df['AMA'] = df['AMA'].ffill().bfill()  # Fill NaN
    except Exception:
        df['AMA'] = df['close'].ewm(span=10, adjust=False).mean()  # Fallback to EMA
    
    # Relative Strength Index (RSI)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=params['rsi_period'], min_periods=1).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=params['rsi_period'], min_periods=1).mean()
    loss = loss.replace(0, 0.0001)  # Avoid division by zero
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df['RSI'] = df['RSI'].fillna(50)  # Default to neutral RSI
    
    # MACD
    exp1 = df['close'].ewm(span=params['macd_fast'], adjust=False).mean()
    exp2 = df['close'].ewm(span=params['macd_slow'], adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=params['macd_signal'], adjust=False).mean()
    
    # Bollinger Bands
    df['BB_Middle'] = df['close'].rolling(window=bb_period, min_periods=1).mean()
    bb_std = df['close'].rolling(window=bb_period, min_periods=1).std()
    bb_std = bb_std.fillna(df['close'].std())  # Fallback to overall std
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * params['bb_std'])
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * params['bb_std'])
    
    # Momentum
    mom_period = min(params['momentum_period'], len(df) // 5)
    df['Momentum'] = df['close'] - df['close'].shift(max(1, mom_period))
    df['Momentum'] = df['Momentum'].fillna(0)
    
    # Rate of Change
    shifted = df['close'].shift(max(1, mom_period))
    shifted = shifted.replace(0, np.nan)
    df['ROC'] = ((df['close'] - shifted) / shifted) * 100
    df['ROC'] = df['ROC'].fillna(0)
    
    # Volume indicators
    vol_window = max(5, vol_window)
    df['Volume_SMA'] = df['volume'].rolling(window=vol_window, min_periods=1).mean()
    df['Volume_SMA'] = df['Volume_SMA'].replace(0, 1)  # Avoid division by zero
    df['Volume_Ratio'] = df['volume'] / df['Volume_SMA']
    df['Volume_Ratio'] = df['Volume_Ratio'].fillna(1)
    
    return df

def fetch_vix_data(api_key, days=60):
    """Fetch VIX (Volatility Index) data from Polygon"""
    try:
        client = RESTClient(api_key)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch VIX data - try direct VIX index first, fallback to VXX ETF
        aggs = client.get_aggs(
            ticker='I:VIX',  # Polygon format for CBOE VIX index
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            timespan='day',
            multiplier=1,
            adjusted=True,
            sort='asc',
            limit=50000
        )
        
        if not aggs:
            # Fallback to VXX ETF if VIX index not available
            aggs = client.get_aggs(
                ticker='VXX',
                from_=start_date.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
                timespan='day',
                multiplier=1,
                adjusted=True,
                sort='asc',
                limit=50000
            )
        
        # Convert to DataFrame - new polygon API returns Agg objects
        data = []
        if aggs:
            for agg in aggs:
                data.append({
                    'timestamp': datetime.fromtimestamp(agg.timestamp / 1000),
                    'vix_close': agg.close
                })
        
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values('timestamp').reset_index(drop=True)
        return df
    except Exception as e:
        st.warning(f"Could not fetch VIX data: {str(e)}")
        return None

def calculate_gex(api_key, ticker, spot_price):
    """Calculate real Gamma Exposure (GEX) for options using gamma analysis"""
    try:
        # Use the real gamma analysis from options_gamma module
        gex_analysis = get_gamma_analysis(api_key, ticker, spot_price)
        
        if gex_analysis:
            # Convert to the expected format for the app
            gex_levels = {
                'total_gex': gex_analysis['total_gex'],
                'net_gex': gex_analysis['net_gex'],
                'pin_strike': gex_analysis['pin_strike'],
                'pin_expiry': gex_analysis['pin_expiry'],
                'zero_gamma': gex_analysis['zero_gamma'],
                'direction': gex_analysis['direction'],
                'pull_strength': gex_analysis['pull_strength'],
                'summary': gex_analysis['summary'],
                'gamma_walls': gex_analysis['gamma_walls'],
                'gex_by_strike': gex_analysis['gex_by_strike']
            }
            
            # Add key support/resistance levels from gamma walls
            if not gex_analysis['gamma_walls'].empty:
                key_levels = gex_analysis['gamma_walls']['strike'].tolist()[:3]
            else:
                key_levels = [spot_price * 0.98, spot_price * 1.02, gex_analysis['zero_gamma']]
            
            gex_levels['key_levels'] = key_levels
            
            return gex_levels
        else:
            # Fallback to simple calculation if real data not available
            return {
                'total_gex': 0,
                'net_gex': 0,
                'pin_strike': spot_price,
                'pin_expiry': datetime.now(),
                'zero_gamma': spot_price,
                'direction': 'at',
                'pull_strength': 0,
                'summary': 'Gamma data unavailable',
                'key_levels': [spot_price * 0.98, spot_price * 1.02]
            }
    except Exception as e:
        st.warning(f"Could not calculate GEX: {str(e)}")
        return None

def fetch_market_data(api_key, ticker, days=60, use_index=True):
    """Fetch historical market data from Polygon
    
    Args:
        api_key: Polygon API key
        ticker: Base ticker symbol (e.g., 'SPX', 'NDX', 'SPY')
        days: Number of days of history
        use_index: If True, try direct index data (I:SPX) first, then fall back to ETF
        
    Returns:
        tuple: (DataFrame, data_source) where data_source is 'index' or 'etf'
    """
    try:
        client = RESTClient(api_key)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Determine which ticker to use
        polygon_ticker = ticker
        is_index_data = False
        data_source = 'direct'  # Track whether we're using index or ETF data
        
        # If ticker is a base index (SPX, NDX, etc.), try direct index format first
        if use_index and ticker in INDEX_POLYGON_TICKERS:
            polygon_ticker = INDEX_POLYGON_TICKERS[ticker]
            is_index_data = True
            data_source = 'index'
        elif use_index and ticker in INDEX_ETFS:
            # Ticker might be passed as SPX instead of SPY
            polygon_ticker = INDEX_POLYGON_TICKERS.get(ticker, f"I:{ticker}")
            is_index_data = True
            data_source = 'index'
        
        # Fetch aggregates (daily bars)
        aggs = None
        aggs_list = []
        try:
            aggs = client.get_aggs(
                ticker=polygon_ticker,
                from_=start_date.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
                timespan='day',
                multiplier=1,
                adjusted=True,
                sort='asc',
                limit=50000
            )
            # Convert generator to list to check length
            if aggs:
                aggs_list = list(aggs)
        except Exception as e:
            # If index data fails, fall back to ETF
            if is_index_data and ticker in INDEX_ETFS:
                etf_ticker = INDEX_ETFS[ticker]
                st.warning(f"⚠️ INDEX DATA UNAVAILABLE for {polygon_ticker}, falling back to ETF {etf_ticker} - predictions may be less accurate")
                print(f"[DATA SOURCE WARNING] Index data not available for {polygon_ticker}, falling back to {etf_ticker}")
                data_source = 'etf_fallback'
                aggs = client.get_aggs(
                    ticker=etf_ticker,
                    from_=start_date.strftime("%Y-%m-%d"),
                    to=end_date.strftime("%Y-%m-%d"),
                    timespan='day',
                    multiplier=1,
                    adjusted=True,
                    sort='asc',
                    limit=50000
                )
                if aggs:
                    aggs_list = list(aggs)
        
        # If no data and this was an index, try ETF fallback
        if len(aggs_list) == 0 and is_index_data and ticker in INDEX_ETFS:
            etf_ticker = INDEX_ETFS[ticker]
            st.warning(f"⚠️ NO INDEX DATA for {polygon_ticker}, falling back to ETF {etf_ticker} - predictions may be less accurate")
            print(f"[DATA SOURCE WARNING] No index data for {polygon_ticker}, trying ETF {etf_ticker}")
            data_source = 'etf_fallback'
            aggs = client.get_aggs(
                ticker=etf_ticker,
                from_=start_date.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
                timespan='day',
                multiplier=1,
                adjusted=True,
                sort='asc',
                limit=50000
            )
            if aggs:
                aggs_list = list(aggs)
        
        # Convert to DataFrame - new polygon API returns Agg objects
        data = []
        for agg in aggs_list:
            data.append({
                'timestamp': datetime.fromtimestamp(agg.timestamp / 1000),
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume if agg.volume else 1  # Indices may not have volume
            })
        
        df = pd.DataFrame(data)
        if not df.empty:
            df = df.sort_values('timestamp').reset_index(drop=True)
            # Ensure volume is never 0 (indices don't have volume data)
            if 'volume' in df.columns:
                df['volume'] = df['volume'].replace(0, 1).fillna(1)
            # Add data source metadata
            df.attrs['data_source'] = data_source
            df.attrs['ticker_used'] = polygon_ticker if data_source == 'index' else INDEX_ETFS.get(ticker, ticker)
        
        return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def get_current_price(api_key, ticker):
    """Get current/latest price"""
    try:
        client = RESTClient(api_key)
        
        # Get previous day's close
        end_date = datetime.now()
        start_date = end_date - timedelta(days=5)
        
        aggs = client.get_aggs(
            ticker=ticker,
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            timespan='day',
            multiplier=1,
            adjusted=True,
            sort='desc',
            limit=1
        )
        
        # New polygon API returns list of Agg objects
        if aggs and len(aggs) > 0:
            return aggs[0].close
        return None
    except Exception as e:
        st.error(f"Error fetching current price: {str(e)}")
        return None

def is_near_market_close():
    """Check if current time is within 15 minutes of market close (3:45 PM - 4:00 PM ET)"""
    from datetime import time
    import pytz
    
    # Get current time in Eastern Time
    et_tz = pytz.timezone('US/Eastern')
    current_et = datetime.now(et_tz)
    current_time = current_et.time()
    
    # Market closes at 4:00 PM ET, critical window starts at 3:45 PM
    critical_start = time(15, 45)  # 3:45 PM
    market_close = time(16, 0)     # 4:00 PM
    
    return critical_start <= current_time <= market_close

def export_to_csv(predictions_data, include_indicators=True):
    """Export predictions and indicators to CSV for Excel compatibility"""
    export_data = []
    
    for index_name, pred_data in predictions_data.items():
        if include_indicators and 'df' in pred_data:
            df = pred_data['df'].copy()
            df['index_name'] = index_name
            df['predicted_price'] = pred_data.get('predicted_price', None)
            df['confidence'] = pred_data.get('confidence', None)
            df['prediction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            export_data.append(df)
        else:
            # Simple export with just predictions
            export_data.append({
                'Index': index_name,
                'Ticker': pred_data.get('ticker', ''),
                'Current Price': pred_data.get('current_price', 0),
                'Predicted Price': pred_data.get('predicted_price', 0),
                'Change %': pred_data.get('change_pct', 0),
                'Confidence %': pred_data.get('confidence', 0),
                'Model': pred_data.get('model_type', ''),
                'Timeframe': pred_data.get('timeframe', ''),
                'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
    
    if export_data:
        if isinstance(export_data[0], dict):
            # Simple export
            export_df = pd.DataFrame(export_data)
        else:
            # Full export with indicators
            export_df = pd.concat(export_data, ignore_index=True)
        
        # Generate CSV
        csv = export_df.to_csv(index=False)
        return csv
    return None

# DISABLED: WebSocket connections are now backend-only (singleton pattern)
# Streamlit must NOT create WebSocket connections directly - this causes Polygon 1008 errors
# All streaming data is read from the FastAPI backend via REST endpoints
def setup_websocket_streaming_DISABLED(api_key, tickers, on_data_callback):
    """
    DISABLED - WebSocket connections are now managed by the FastAPI backend only.
    
    Streamlit should use /api/buffer/latest or /api/gex/{symbol} endpoints 
    to read cached streaming data from the backend.
    """
    raise NotImplementedError(
        "WebSocket connections are managed by the FastAPI backend only. "
        "Use the /api/buffer/latest endpoint to read cached streaming data."
    )

def predict_eod_price(df, model_type='Linear Regression', timeframe='1-day', gex_data=None):
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
    df = calculate_technical_indicators(df)
    
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

def create_price_chart(df, predicted_price, ticker_name):
    """Create interactive price chart with prediction"""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f'{ticker_name} Price & Indicators', 'RSI', 'MACD'),
        row_heights=[0.6, 0.2, 0.2]
    )
    
    # Candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df['timestamp'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Moving averages
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['SMA_20'], 
                   name='SMA 20', line=dict(color='orange', width=1)),
        row=1, col=1
    )
    
    # Bollinger Bands
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['BB_Upper'], 
                   name='BB Upper', line=dict(color='gray', width=1, dash='dash')),
        row=1, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['BB_Lower'], 
                   name='BB Lower', line=dict(color='gray', width=1, dash='dash'),
                   fill='tonexty', fillcolor='rgba(128,128,128,0.1)'),
        row=1, col=1
    )
    
    # Predicted price point
    if predicted_price:
        last_timestamp = df['timestamp'].iloc[-1]
        next_timestamp = last_timestamp + timedelta(hours=16)  # Next close
        
        fig.add_trace(
            go.Scatter(
                x=[last_timestamp, next_timestamp],
                y=[df['close'].iloc[-1], predicted_price],
                mode='lines+markers',
                name='Prediction',
                line=dict(color='red', width=2, dash='dash'),
                marker=dict(size=10, color='red')
            ),
            row=1, col=1
        )
    
    # RSI
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['RSI'], 
                   name='RSI', line=dict(color='purple', width=1)),
        row=2, col=1
    )
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)
    
    # MACD (more useful than volume for indices which don't have volume data)
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['MACD'], 
                   name='MACD', line=dict(color='blue', width=1.5)),
        row=3, col=1
    )
    fig.add_trace(
        go.Scatter(x=df['timestamp'], y=df['Signal_Line'], 
                   name='Signal', line=dict(color='orange', width=1.5)),
        row=3, col=1
    )
    # MACD Histogram
    macd_hist = df['MACD'] - df['Signal_Line']
    colors = ['green' if val >= 0 else 'red' for val in macd_hist]
    fig.add_trace(
        go.Bar(x=df['timestamp'], y=macd_hist, 
               name='MACD Histogram', marker_color=colors, opacity=0.5),
        row=3, col=1
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)
    
    fig.update_layout(
        height=800,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1)
    fig.update_yaxes(title_text="MACD", row=3, col=1)
    
    return fig

# App UI
st.title("📈 Stock Index EOD Price Predictor")
st.markdown("Predict end-of-day closing prices for major stock indexes using live market data and technical analysis")

# Sidebar for API key
with st.sidebar:
    st.header("⚙️ Configuration")
    
    api_key_input = st.text_input(
        "Polygon API Key",
        type="password",
        value=st.session_state.api_key,
        help="Enter your Polygon.io API key with live market access"
    )
    
    if api_key_input:
        st.session_state.api_key = api_key_input
    
    st.divider()
    
    st.header("📊 Select Indexes")
    selected_indexes = st.multiselect(
        "Choose indexes to analyze",
        options=list(INDEXES.keys()),
        default=["S&P 500 (SPX)", "NASDAQ 100 (NDX)"]
    )
    
    days_history = st.slider(
        "Historical Data (days)",
        min_value=30,
        max_value=90,
        value=60,
        help="More data can improve prediction accuracy"
    )
    
    st.divider()
    
    st.header("🤖 Model Settings")
    selected_model = st.selectbox(
        "ML Model",
        options=["Linear Regression", "Random Forest"],
        index=0,
        help="Choose the machine learning model for predictions"
    )
    st.session_state.selected_model = selected_model
    
    selected_timeframe = st.selectbox(
        "Prediction Timeframe",
        options=["1-day", "5-day", "1-week"],
        index=0,
        help="Select how far ahead to predict"
    )
    st.session_state.timeframe = selected_timeframe
    
    st.divider()
    
    st.header("🤖 AI Analysis")
    enable_ai = st.checkbox("Enable OpenAI Analysis", value=True, 
                           help="Get AI-powered insights on predictions and market data")
    
    st.divider()
    
    st.header("🔔 Alert Settings")
    enable_alerts = st.checkbox("Enable Price Alerts", value=False)
    
    if enable_alerts:
        alert_threshold = st.slider(
            "Movement Threshold (%)",
            min_value=1.0,
            max_value=10.0,
            value=3.0,
            step=0.5,
            help="Alert when predicted change exceeds this percentage"
        )
        confidence_threshold = st.slider(
            "Confidence Threshold (%)",
            min_value=50,
            max_value=90,
            value=70,
            step=5,
            help="Alert only when confidence is above this level"
        )
    
    st.divider()
    
    # Advanced indicator settings
    with st.expander("⚙️ Advanced: Technical Indicator Parameters"):
        st.caption("Customize technical indicator calculation parameters")
        
        # Get current params
        params = st.session_state.indicator_params
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Moving Averages")
            sma_short = st.number_input("SMA Short Period", min_value=3, max_value=20, value=params['sma_short'], key='sma_short')
            sma_medium = st.number_input("SMA Medium Period", min_value=5, max_value=30, value=params['sma_medium'], key='sma_medium')
            sma_long = st.number_input("SMA Long Period", min_value=10, max_value=50, value=params['sma_long'], key='sma_long')
            ema_short = st.number_input("EMA Short Period", min_value=3, max_value=20, value=params['ema_short'], key='ema_short')
            ema_long = st.number_input("EMA Long Period", min_value=5, max_value=30, value=params['ema_long'], key='ema_long')
        
        with col2:
            st.subheader("Oscillators & Bands")
            rsi_period = st.number_input("RSI Period", min_value=7, max_value=30, value=params['rsi_period'], key='rsi_period')
            macd_fast = st.number_input("MACD Fast", min_value=8, max_value=20, value=params['macd_fast'], key='macd_fast')
            macd_slow = st.number_input("MACD Slow", min_value=20, max_value=35, value=params['macd_slow'], key='macd_slow')
            macd_signal = st.number_input("MACD Signal", min_value=5, max_value=15, value=params['macd_signal'], key='macd_signal')
            bb_period = st.number_input("Bollinger Bands Period", min_value=10, max_value=30, value=params['bb_period'], key='bb_period')
            bb_std = st.number_input("Bollinger Bands Std Dev", min_value=1.0, max_value=3.0, value=float(params['bb_std']), step=0.5, key='bb_std')
            momentum_period = st.number_input("Momentum Period", min_value=5, max_value=20, value=params['momentum_period'], key='momentum_period')
        
        # Update session state from widget values
        st.session_state.indicator_params = {
            'sma_short': st.session_state.sma_short,
            'sma_medium': st.session_state.sma_medium,
            'sma_long': st.session_state.sma_long,
            'ema_short': st.session_state.ema_short,
            'ema_long': st.session_state.ema_long,
            'rsi_period': st.session_state.rsi_period,
            'macd_fast': st.session_state.macd_fast,
            'macd_slow': st.session_state.macd_slow,
            'macd_signal': st.session_state.macd_signal,
            'bb_period': st.session_state.bb_period,
            'bb_std': st.session_state.bb_std,
            'momentum_period': st.session_state.momentum_period
        }
        
        if st.button("Reset to Defaults"):
            # Reset both indicator_params and widget keys
            defaults = {
                'sma_short': 5,
                'sma_medium': 10,
                'sma_long': 20,
                'ema_short': 5,
                'ema_long': 10,
                'rsi_period': 14,
                'macd_fast': 12,
                'macd_slow': 26,
                'macd_signal': 9,
                'bb_period': 20,
                'bb_std': 2.0,
                'momentum_period': 10
            }
            st.session_state.indicator_params = defaults
            # Also reset widget state
            for key, value in defaults.items():
                st.session_state[key] = value
            st.rerun()
    
    st.divider()
    
    # Streaming & Frozen Gamma Tabs
    st.header("🔌 Market Data")
    
    # Check market status (informational only)
    market_is_open = is_market_open()
    market_status_color = "🟢" if market_is_open else "🔴"
    market_status_text = "OPEN" if market_is_open else "CLOSED"
    st.info(f"Market Status: {market_status_color} {market_status_text}")
    
    # Create tabs for Live Streaming and Frozen Gamma
    streaming_tab, frozen_gamma_tab = st.tabs(["📡 Live Streaming", "🧊 Frozen Gamma (Audit)"])
    
    # === FROZEN GAMMA TAB ===
    with frozen_gamma_tab:
        st.markdown("**Read-only audit snapshot view** — Shows last valid gamma state from persisted snapshots.")
        
        # Check freeze status
        is_frozen, freeze_reason = get_freeze_status()
        if is_frozen:
            st.info(f"🔒 Market Freeze Active: {freeze_reason}")
        
        if selected_indexes:
            for index_name in selected_indexes:
                symbol = INDEXES[index_name]
                
                st.subheader(f"{symbol}")
                
                # Load last valid audit snapshot
                snapshot = load_last_valid_snapshot(symbol)
                
                if snapshot is None:
                    st.warning("⚠️ No frozen gamma snapshot available")
                    st.caption("Build snapshots during market hours using the historical API endpoints.")
                else:
                    # Display frozen status with full timestamp (date + time)
                    snapshot_time = snapshot.generated_at_utc
                    if snapshot_time:
                        try:
                            # Parse and format timestamp with full date
                            if isinstance(snapshot_time, str):
                                dt = datetime.fromisoformat(snapshot_time.replace('Z', '+00:00'))
                            else:
                                dt = snapshot_time
                            # Format: "Dec 19, 2025 at 10:30:25 AM ET"
                            date_str = dt.strftime('%b %d, %Y')
                            time_str = dt.strftime('%I:%M:%S %p ET')
                            st.success(f"🧊 Snapshot recorded: {date_str} at {time_str}")
                        except:
                            st.success(f"🧊 Snapshot recorded: {snapshot_time}")
                    else:
                        st.success("🧊 Frozen Snapshot (no timestamp)")
                    
                    # Display gamma metrics from snapshot
                    # Row 1: Pin, Gross GEX, Net GEX
                    metric_col1, metric_col2, metric_col3 = st.columns(3)
                    
                    with metric_col1:
                        st.metric("📍 Primary Gamma Pin", f"${snapshot.primary_gamma_pin_strike:,.0f}")
                    
                    with metric_col2:
                        # Use gross_gex if available, fall back to total_gex_abs for backward compatibility
                        # Values are already in billions - no scaling needed
                        gross_gex = getattr(snapshot, 'gross_gex', None) or snapshot.total_gex_abs
                        st.metric("Gross GEX", f"${gross_gex:.2f}B", help="sum(call_gex) + sum(put_gex)")
                    
                    with metric_col3:
                        # Use net_gex if available, fall back to total_gex_net for backward compatibility
                        # Values are already in billions - no scaling needed
                        net_gex = getattr(snapshot, 'net_gex', None)
                        if net_gex is None:
                            net_gex = snapshot.total_gex_net
                        sign = "+" if net_gex > 0 else ""
                        st.metric("Net GEX", f"{sign}${net_gex:.2f}B", help="sum(call_gex) - sum(put_gex)")
                    
                    # Row 2: Call GEX, Put GEX, Zero Gamma
                    gex_col1, gex_col2, gex_col3 = st.columns(3)
                    with gex_col1:
                        # Values are already in billions - no scaling needed
                        call_gex = getattr(snapshot, 'call_gex_total', 0)
                        if call_gex > 0:
                            st.metric("Call GEX", f"${call_gex:.2f}B")
                    with gex_col2:
                        # Values are already in billions - no scaling needed
                        put_gex = getattr(snapshot, 'put_gex_total', 0)
                        if put_gex > 0:
                            st.metric("Put GEX", f"${put_gex:.2f}B")
                    with gex_col3:
                        if snapshot.zero_gamma_level:
                            st.metric("Zero Gamma", f"${snapshot.zero_gamma_level:,.0f}")
                    
                    # Row 3: Spot, Pin Drift
                    spot_col1, spot_col2 = st.columns(2)
                    with spot_col1:
                        st.metric("Spot (at freeze)", f"${snapshot.spot_last:,.2f}")
                    with spot_col2:
                        # Display pin drift if available
                        pin_drift = getattr(snapshot, 'pin_drift_points_per_hour', None)
                        pin_change = getattr(snapshot, 'pin_change_points', None)
                        if pin_drift is not None and pin_drift != 0:
                            sign = "+" if pin_drift > 0 else ""
                            st.metric("Pin Drift", f"{sign}{pin_drift:.1f} pts/hr", help="Rate of pin migration")
                        elif pin_change is not None and pin_change != 0:
                            sign = "+" if pin_change > 0 else ""
                            st.metric("Pin Change", f"{sign}{pin_change:.0f} pts", help="Change since last snapshot")
                    
                    # Validation status
                    if snapshot.validation_is_valid:
                        st.caption("✅ Snapshot validated")
                    else:
                        st.caption(f"⚠️ Validation issues: {', '.join(snapshot.validation_failure_reasons or [])}")
                    
                    # Top gamma walls (if available)
                    if snapshot.top_strikes_by_abs_gex:
                        with st.expander("🧱 Top Gamma Walls", expanded=False):
                            for strike_data in snapshot.top_strikes_by_abs_gex[:5]:
                                if isinstance(strike_data, dict):
                                    strike = strike_data.get('strike', 0)
                                    # Values are already in billions - no scaling needed
                                    gex = strike_data.get('abs_gex', 0)
                                    st.caption(f"${strike:,.0f}: ${gex:.3f}B")
                                else:
                                    st.caption(str(strike_data))
                    
                    st.divider()
        else:
            st.info("Select indexes in the sidebar to view frozen gamma snapshots.")
    
    # === LIVE STREAMING TAB ===
    # NOTE: WebSocket connections are managed by the FastAPI backend ONLY
    # Streamlit reads cached data via REST endpoints (no direct WebSocket creation)
    with streaming_tab:
        st.info("📡 **Live data is streamed via the backend service**")
        st.caption("The FastAPI backend manages all WebSocket connections to prevent duplicate connections.")
        
        if not market_is_open:
            st.warning("ℹ️ Market closed - showing cached data from last session")
        
        # Show backend streaming status
        st.subheader("Backend Streaming Status")
        
        # Fetch status from backend API
        try:
            import requests
            health_resp = requests.get("http://localhost:8000/health", timeout=2)
            if health_resp.status_code == 200:
                health_data = health_resp.json()
                
                status_col1, status_col2, status_col3 = st.columns(3)
                with status_col1:
                    ws_status = health_data.get("websocket", "unknown")
                    if ws_status == "active":
                        st.success("✅ WebSocket: Connected")
                    else:
                        st.info(f"WebSocket: {ws_status}")
                with status_col2:
                    buffer_status = health_data.get("buffer_health", "unknown")
                    st.metric("Buffer Status", buffer_status)
                with status_col3:
                    st.metric("Uptime", health_data.get("uptime", "N/A"))
            else:
                st.warning("Backend not responding")
        except Exception as e:
            st.error(f"Cannot reach backend: {str(e)[:50]}")
        
        # Fetch latest cached data from backend
        st.subheader("Latest Cached Data")
        
        if st.button("🔄 Refresh Cached Data", type="secondary"):
            try:
                for idx_name in selected_indexes:
                    ticker = INDEXES[idx_name]
                    # Try to get buffer data from backend
                    try:
                        buffer_resp = requests.get(f"http://localhost:8000/buffer/latest/{ticker}", timeout=2)
                        if buffer_resp.status_code == 200:
                            buffer_data = buffer_resp.json()
                            if buffer_data and buffer_data.get("price"):
                                st.metric(
                                    f"{ticker}",
                                    f"${buffer_data['price']:,.2f}",
                                    help=f"Last update: {buffer_data.get('timestamp', 'N/A')}"
                                )
                    except Exception:
                        pass
                st.success("Data refreshed from backend cache")
            except Exception as e:
                st.error(f"Error fetching cached data: {str(e)[:50]}")
        
        # Snapshot data fallback using REST API
        st.divider()
        st.caption("**Snapshot Data (REST API Fallback)**")
        if st.button("📸 Get Current Prices", type="secondary", help="Fetch latest prices via Polygon REST API"):
            if selected_indexes and st.session_state.api_key:
                tickers_to_fetch = [INDEX_ETFS[INDEXES[idx]] for idx in selected_indexes]
                with st.spinner("Fetching snapshot data..."):
                    snapshot_data = get_snapshot_data(st.session_state.api_key, tickers_to_fetch)
                    if snapshot_data:
                        st.success(f"✅ Fetched prices for {len(snapshot_data)} tickers")
                        # Display snapshot data
                        cols = st.columns(len(snapshot_data))
                        for i, (ticker, data) in enumerate(snapshot_data.items()):
                            with cols[i]:
                                if data['price']:
                                    change = ((data['price'] - data['prev_close']) / data['prev_close'] * 100) if data['prev_close'] else 0
                                    st.metric(
                                        ticker, 
                                        f"${data['price']:.2f}",
                                        f"{change:+.2f}%"
                                    )
                                else:
                                    st.metric(ticker, "N/A")
                    else:
                        st.error("Failed to fetch snapshot data. Check your API key.")
            else:
                st.warning("Please enter API key and select indexes first!")
        
        # Show tips
        with st.expander("💡 Streaming Architecture", expanded=False):
            st.markdown("""
            **Backend-Only WebSocket Pattern**
            
            - The FastAPI backend manages a single WebSocket connection per API key
            - This prevents the Polygon 1008 "duplicate connection" error
            - Streamlit reads cached data via REST endpoints
            - Live data is buffered in ring buffers on the backend
            
            **Data Flow:**
            1. FastAPI backend connects to Polygon WebSocket (singleton)
            2. Incoming data is aggregated into 1-second bars
            3. Streamlit polls the backend for latest cached data
            """)
    
    st.divider()
    
    analyze_button = st.button("🔄 Analyze & Predict", type="primary", width='stretch')
    
    st.divider()
    
    # Backtesting section
    st.header("📊 Backtesting")
    run_backtest_btn = st.checkbox("Enable Backtesting Mode", value=False,
                                   help="Test prediction accuracy using historical data")
    
    if run_backtest_btn:
        st.subheader("Backtest Configuration")
        
        # Date range for backtest
        col1, col2 = st.columns(2)
        with col1:
            backtest_start = st.date_input(
                "Start Date",
                value=datetime.now() - timedelta(days=30),
                max_value=datetime.now()
            )
        with col2:
            backtest_end = st.date_input(
                "End Date",
                value=datetime.now() - timedelta(days=1),
                max_value=datetime.now()
            )
        
        # Model selection for backtest
        backtest_model = st.selectbox(
            "Backtest Model",
            ["linear_regression", "random_forest"],
            help="Model to use for backtesting"
        )
        
        run_backtest_analysis = st.button("🚀 Run Backtest", type="secondary", width='stretch')
    
    st.divider()
    st.caption("💡 This tool uses technical indicators and machine learning to predict closing prices. Predictions are estimates and should not be used as financial advice.")

# Main content
if not st.session_state.api_key:
    st.info("👈 Please enter your Polygon API key in the sidebar to get started")
    st.markdown("""
    ### How it works:
    1. Enter your Polygon API key (with live market access)
    2. Select the stock indexes you want to analyze
    3. Click 'Analyze & Predict' to generate predictions
    
    ### Features:
    - **Live market data** from Polygon.io with WebSocket streaming support
    - **Advanced technical analysis** including VWAP, AMA (Adaptive Moving Average), RSI, MACD, Bollinger Bands
    - **Options analytics** with Gamma Exposure (GEX) levels for key support/resistance
    - **VIX integration** for volatility analysis
    - **Critical time window** monitoring (15 minutes before market close at 3:45 PM ET)
    - **Machine learning predictions** using Linear Regression and Random Forest models
    - **Interactive charts** with all technical indicators
    - **CSV export** for Excel compatibility
    - **Confidence scores** and alerts for significant movements
    """)
else:
    if analyze_button and selected_indexes:
        st.session_state.predictions = {}
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Fetch VIX data once (shared across all indexes)
        status_text.text("Fetching VIX data...")
        vix_df = fetch_vix_data(st.session_state.api_key, days_history)
        
        # Check if we're in critical window for intraday data
        if is_near_market_close():
            st.warning("🔴 Critical window detected! Using intraday data for enhanced predictions.")
            data_timespan = 'minute'
            data_multiplier = 5  # 5-minute bars
        else:
            data_timespan = 'day'
            data_multiplier = 1
        
        for idx, index_name in enumerate(selected_indexes):
            index_ticker = INDEXES[index_name]  # Actual index ticker (SPX, NDX, etc.)
            status_text.text(f"Analyzing {index_name}...")
            
            # Fetch price data using direct index ticker (I:SPX format), falls back to ETF if needed
            df = fetch_market_data(st.session_state.api_key, index_ticker, days_history, use_index=True)
            
            # Debug: Show data fetch result
            if df is None:
                st.warning(f"⚠️ {index_name}: No data returned from fetch")
            elif len(df) == 0:
                st.warning(f"⚠️ {index_name}: Empty dataframe returned")
            else:
                st.caption(f"✓ {index_name}: Fetched {len(df)} rows")
            
            if df is not None and len(df) > 0:
                # Preserve data source metadata before any operations
                data_source = df.attrs.get('data_source', 'unknown') if hasattr(df, 'attrs') else 'unknown'
                ticker_used = df.attrs.get('ticker_used', index_ticker) if hasattr(df, 'attrs') else index_ticker
                
                # Merge VIX data if available
                if vix_df is not None and len(vix_df) > 0:
                    df = pd.merge(df, vix_df, on='timestamp', how='left')
                    df['vix_close'] = df['vix_close'].ffill()  # Fixed deprecated fillna(method='ffill')
                    # Restore attrs after merge (merge loses them)
                    df.attrs['data_source'] = data_source
                    df.attrs['ticker_used'] = ticker_used
                
                # Get current price for GEX calculation
                current_price = df['close'].iloc[-1]
                
                # Calculate GEX levels using actual index ticker for options
                gex_data = calculate_gex(st.session_state.api_key, index_ticker, current_price)
                
                # Predict EOD price using selected model, timeframe, and GAMMA PIN DATA
                # Pass gex_data so predictions align with gamma pin levels
                predicted_price, confidence, df_with_indicators, current_price, error_msg = predict_eod_price(
                    df, 
                    model_type=st.session_state.selected_model,
                    timeframe=st.session_state.timeframe,
                    gex_data=gex_data  # Critical: gamma pin influences EOD prediction
                )
                
                # Debug: Show prediction result
                if predicted_price is None:
                    st.warning(f"⚠️ {index_name}: Prediction failed - {error_msg or 'unknown error'}")
                else:
                    st.caption(f"✓ {index_name}: Predicted ${predicted_price:.2f}, confidence {confidence:.1f}%")
                
                if predicted_price and current_price:
                    change_pct = ((predicted_price - current_price) / current_price) * 100
                    
                    # data_source and ticker_used already captured before merge
                    
                    st.session_state.predictions[index_name] = {
                        'ticker': index_ticker,
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'confidence': confidence,
                        'df': df_with_indicators,
                        'change_pct': change_pct,
                        'model_type': st.session_state.selected_model,
                        'timeframe': st.session_state.timeframe,
                        'gex_data': gex_data,  # Add GEX data
                        'has_vix': vix_df is not None,  # Track VIX availability
                        'data_source': data_source,  # Track if using index or ETF data
                        'ticker_used': ticker_used  # Actual ticker used for data
                    }
                    
                    # Save prediction to database
                    try:
                        if st.session_state.timeframe == '1-day':
                            target_date = datetime.now() + timedelta(days=1)
                        elif st.session_state.timeframe == '5-day':
                            target_date = datetime.now() + timedelta(days=5)
                        else:
                            target_date = datetime.now() + timedelta(days=7)
                        
                        save_prediction(
                            ticker=index_ticker,
                            index_name=index_name,
                            current_price=current_price,
                            predicted_price=predicted_price,
                            confidence=confidence,
                            model_type=st.session_state.selected_model,
                            change_pct=change_pct,
                            target_date=target_date
                        )
                        
                        # Check alerts if enabled
                        if enable_alerts and abs(change_pct) >= alert_threshold and confidence >= confidence_threshold:
                            direction = "increase" if change_pct > 0 else "decrease"
                            message = f"{index_name} predicted to {direction} by {abs(change_pct):.2f}% (Confidence: {confidence:.1f}%)"
                            save_alert(
                                ticker=index_ticker,
                                index_name=index_name,
                                alert_type="price_movement",
                                threshold=alert_threshold,
                                current_value=abs(change_pct),
                                message=message
                            )
                            st.session_state.alerts.append(message)
                    except Exception as e:
                        st.warning(f"Could not save prediction: {str(e)}")
            
            progress_bar.progress((idx + 1) / len(selected_indexes))
        
        status_text.text("Analysis complete!")
        time.sleep(0.5)
        status_text.empty()
        progress_bar.empty()
        
        # Debug: Show prediction count
        if st.session_state.predictions:
            st.success(f"✅ Generated {len(st.session_state.predictions)} predictions")
        else:
            st.error("⚠️ No predictions were generated - check data availability")
    
    # Display predictions
    if st.session_state.predictions:
        # Critical time window indicator
        if is_near_market_close():
            st.error("🔴 **CRITICAL WINDOW: 15 Minutes to Market Close!**")
            st.info("This is the optimal time for predictions. Market closes at 4:00 PM ET.")
        else:
            import pytz
            et_tz = pytz.timezone('US/Eastern')
            current_et = datetime.now(et_tz)
            st.info(f"Current ET Time: {current_et.strftime('%I:%M %p')} | Critical window: 3:45-4:00 PM ET")
        
        st.header("📊 Prediction Results")
        
        # Export options
        col_export1, col_export2, col_export3 = st.columns([2, 2, 6])
        with col_export1:
            csv_data = export_to_csv(st.session_state.predictions, include_indicators=False)
            if csv_data:
                st.download_button(
                    label="📥 Export Summary (CSV)",
                    data=csv_data,
                    file_name=f"predictions_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col_export2:
            csv_full = export_to_csv(st.session_state.predictions, include_indicators=True)
            if csv_full:
                st.download_button(
                    label="📥 Export Full Data (CSV)",
                    data=csv_full,
                    file_name=f"predictions_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        
        with col_export3:
            st.caption("Export to Excel-compatible CSV format")
        
        # Summary cards
        cols = st.columns(len(st.session_state.predictions))
        
        for col, (index_name, pred) in zip(cols, st.session_state.predictions.items()):
            with col:
                change_color = "🟢" if pred['change_pct'] >= 0 else "🔴"
                st.metric(
                    label=f"{change_color} {index_name}",
                    value=f"${pred['predicted_price']:.2f}",
                    delta=f"{pred['change_pct']:.2f}%"
                )
                st.caption(f"Current: ${pred['current_price']:.2f}")
                st.caption(f"Confidence: {pred['confidence']:.1f}%")
                
                # Confidence bar
                conf_color = "green" if pred['confidence'] > 70 else "orange" if pred['confidence'] > 50 else "red"
                st.markdown(f"<div style='background-color: #f0f0f0; border-radius: 5px; padding: 2px;'><div style='background-color: {conf_color}; width: {pred['confidence']:.0f}%; height: 10px; border-radius: 5px;'></div></div>", unsafe_allow_html=True)
                
                # Display gamma pinning info if available
                if 'gex_data' in pred and pred['gex_data']:
                    gex = pred['gex_data']
                    if 'pin_strike' in gex:
                        pin_symbol = "📍"
                        direction_arrow = "⬆️" if gex['direction'] == 'above' else "⬇️" if gex['direction'] == 'below' else "↔️"
                        st.markdown(f"**{pin_symbol} Gamma Pin:** ${gex['pin_strike']:.0f} {direction_arrow}")
                        if 'pin_expiry' in gex and gex['pin_expiry']:
                            expiry_str = gex['pin_expiry'].strftime('%m/%d') if hasattr(gex['pin_expiry'], 'strftime') else str(gex['pin_expiry'])
                            st.caption(f"Expires: {expiry_str}")
                        if 'summary' in gex:
                            st.caption(gex['summary'])
        
        st.divider()
        
        # AI Portfolio Risk Assessment
        if enable_ai and st.session_state.predictions:
            st.header("🎯 AI Portfolio Risk Assessment")
            with st.spinner("Analyzing overall market risk..."):
                risk_assessment = get_risk_assessment(st.session_state.predictions)
                if risk_assessment:
                    st.warning(f"**Risk Analysis:** {risk_assessment}")
        
        st.divider()
        
        # Detailed charts
        st.header("📈 Detailed Analysis")
        
        for index_name, pred in st.session_state.predictions.items():
            with st.expander(f"📊 {index_name} ({pred['ticker']}) - Detailed Chart", expanded=True):
                # Data source indicator - warn if using ETF fallback
                data_source = pred.get('data_source', 'unknown')
                ticker_used = pred.get('ticker_used', pred['ticker'])
                if data_source == 'etf_fallback':
                    st.warning(f"⚠️ Using ETF fallback data ({ticker_used}) - predictions may be less accurate")
                elif data_source == 'index':
                    st.success(f"✓ Using direct index data ({ticker_used})")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Last Close", f"${pred['current_price']:.2f}")
                with col2:
                    st.metric("Predicted EOD", f"${pred['predicted_price']:.2f}")
                with col3:
                    st.metric("Expected Change", f"{pred['change_pct']:.2f}%")
                
                # AI Analysis
                if enable_ai:
                    with st.spinner("Getting AI analysis..."):
                        latest_data = pred['df'].iloc[-1]
                        tech_indicators = {
                            'RSI': latest_data['RSI'],
                            'MACD': latest_data['MACD'],
                            'Signal_Line': latest_data['Signal_Line'],
                            'SMA_20': latest_data['SMA_20'],
                            'Momentum': latest_data['Momentum'],
                            'VWAP': latest_data['VWAP'],
                            'AMA': latest_data['AMA']
                        }
                        
                        vix_val = latest_data.get('vix_close') if 'vix_close' in latest_data else None
                        
                        ai_analysis = analyze_prediction(
                            index_name,
                            pred['current_price'],
                            pred['predicted_price'],
                            pred['confidence'],
                            tech_indicators,
                            pred.get('gex_data'),
                            vix_val
                        )
                        
                        if ai_analysis:
                            st.info(f"🤖 **AI Analysis:** {ai_analysis}")
                
                # Create and display chart
                fig = create_price_chart(pred['df'], pred['predicted_price'], index_name)
                st.plotly_chart(fig, width='stretch')
                
                # Technical indicators summary
                st.subheader("Technical Indicators Summary")
                tech_col1, tech_col2, tech_col3, tech_col4 = st.columns(4)
                
                latest_data = pred['df'].iloc[-1]
                
                with tech_col1:
                    st.metric("RSI", f"{latest_data['RSI']:.1f}")
                    rsi_signal = "Overbought" if latest_data['RSI'] > 70 else "Oversold" if latest_data['RSI'] < 30 else "Neutral"
                    st.caption(rsi_signal)
                
                with tech_col2:
                    st.metric("MACD", f"{latest_data['MACD']:.2f}")
                    macd_signal = "Bullish" if latest_data['MACD'] > latest_data['Signal_Line'] else "Bearish"
                    st.caption(macd_signal)
                
                with tech_col3:
                    st.metric("SMA 20", f"${latest_data['SMA_20']:.2f}")
                    sma_signal = "Above" if pred['current_price'] > latest_data['SMA_20'] else "Below"
                    st.caption(f"Price {sma_signal}")
                
                with tech_col4:
                    st.metric("Momentum", f"{latest_data['Momentum']:.2f}")
                    mom_signal = "Positive" if latest_data['Momentum'] > 0 else "Negative"
                    st.caption(mom_signal)
                
                # Display Gamma Exposure Analysis if available
                if 'gex_data' in pred and pred['gex_data']:
                    gex = pred['gex_data']
                    st.divider()
                    st.subheader("🎯 Gamma Exposure Analysis")
                    
                    # Main gamma pin information
                    gamma_col1, gamma_col2, gamma_col3 = st.columns(3)
                    
                    with gamma_col1:
                        if 'pin_strike' in gex:
                            st.metric("📍 Primary Gamma Pin", f"${gex['pin_strike']:.0f}")
                            if 'pin_expiry' in gex and gex['pin_expiry']:
                                expiry_str = gex['pin_expiry'].strftime('%m/%d') if hasattr(gex['pin_expiry'], 'strftime') else str(gex['pin_expiry'])
                                st.caption(f"Expires: {expiry_str}")
                    
                    with gamma_col2:
                        if 'total_gex' in gex:
                            st.metric("Total GEX", f"${gex['total_gex']:.1f}B")
                            if 'net_gex' in gex:
                                net_sign = "+" if gex['net_gex'] > 0 else ""
                                st.caption(f"Net: {net_sign}${gex['net_gex']:.1f}B")
                    
                    with gamma_col3:
                        if 'zero_gamma' in gex:
                            st.metric("Zero Gamma Level", f"${gex['zero_gamma']:.0f}")
                            if 'direction' in gex and 'pull_strength' in gex:
                                st.caption(f"Pull: {gex['direction'].upper()} ({gex['pull_strength']:.1f}%)")
                    
                    # Show gamma walls table if available
                    if 'gamma_walls' in gex and not gex['gamma_walls'].empty:
                        st.write("**Major Gamma Walls (Top Strike Levels):**")
                        
                        # Format the gamma walls dataframe
                        gamma_walls_display = gex['gamma_walls'].copy()
                        gamma_walls_display['strike'] = gamma_walls_display['strike'].apply(lambda x: f"${x:.0f}")
                        gamma_walls_display['net_gex'] = gamma_walls_display['net_gex'].apply(lambda x: f"${x:.2f}B")
                        gamma_walls_display['total_gex'] = gamma_walls_display['total_gex'].apply(lambda x: f"${x:.2f}B")
                        gamma_walls_display['days_to_expiry'] = gamma_walls_display['days_to_expiry'].apply(lambda x: f"{x:.0f} days")
                        
                        gamma_walls_display = gamma_walls_display.rename(columns={
                            'strike': 'Strike Price',
                            'net_gex': 'Net GEX',
                            'total_gex': 'Total GEX',
                            'days_to_expiry': 'Days to Expiry'
                        })
                        
                        st.dataframe(gamma_walls_display, width='stretch')
                    
                    # Create gamma exposure bar chart if we have strike-level data
                    if 'gex_by_strike' in gex and not gex['gex_by_strike'].empty:
                        import plotly.graph_objects as go
                        
                        gex_df = gex['gex_by_strike']
                        
                        # Create bar chart showing gamma exposure by strike
                        fig_gex = go.Figure()
                        
                        # Add net GEX bars
                        fig_gex.add_trace(go.Bar(
                            x=gex_df['strike'],
                            y=gex_df['net_gex'],
                            name='Net GEX',
                            marker_color=['green' if x > 0 else 'red' for x in gex_df['net_gex']],
                            text=[f"${abs(x):.1f}B" for x in gex_df['net_gex']],
                            textposition='outside'
                        ))
                        
                        # Add current price line
                        if 'current_price' in pred:
                            fig_gex.add_vline(
                                x=pred['current_price'],
                                line_dash="dash",
                                line_color="blue",
                                annotation_text=f"Current: ${pred['current_price']:.0f}"
                            )
                        
                        # Add pin strike line
                        if 'pin_strike' in gex:
                            fig_gex.add_vline(
                                x=gex['pin_strike'],
                                line_dash="solid",
                                line_color="orange",
                                line_width=2,
                                annotation_text=f"Pin: ${gex['pin_strike']:.0f}"
                            )
                        
                        fig_gex.update_layout(
                            title="Gamma Exposure by Strike Price",
                            xaxis_title="Strike Price",
                            yaxis_title="Net Gamma Exposure (Billions)",
                            showlegend=False,
                            height=300
                        )
                        
                        st.plotly_chart(fig_gex, width='stretch')
                    
                    if 'summary' in gex:
                        st.info(f"💡 {gex['summary']}")
                    
                    # AI Explanation of Gamma
                    if enable_ai:
                        gamma_explanation = explain_gamma_exposure(gex)
                        if gamma_explanation:
                            st.success(f"🤖 **What This Means:** {gamma_explanation}")
                    
                    # Key levels
                    if gex.get('key_levels'):
                        st.info(f"Key Support/Resistance: ${gex['key_levels'][0]:.2f} / ${gex['key_levels'][1]:.2f}")
                
                # Display Intraday Gamma Pin Evolution Chart
                show_gamma_evolution_section(pred['ticker'], index_name, unique_suffix=index_name)
                
                # Display VWAP and AMA
                st.subheader("📊 Advanced Indicators")
                adv_col1, adv_col2, adv_col3 = st.columns(3)
                
                with adv_col1:
                    st.metric("VWAP", f"${latest_data['VWAP']:.2f}")
                    vwap_signal = "Above" if pred['current_price'] > latest_data['VWAP'] else "Below"
                    st.caption(f"Price {vwap_signal}")
                
                with adv_col2:
                    st.metric("AMA (Adaptive)", f"${latest_data['AMA']:.2f}")
                    ama_signal = "Above" if pred['current_price'] > latest_data['AMA'] else "Below"
                    st.caption(f"Price {ama_signal}")
                
                with adv_col3:
                    if 'vix_close' in latest_data:
                        st.metric("VIX", f"{latest_data['vix_close']:.2f}")
                        vix_level = "High Vol" if latest_data['vix_close'] > 20 else "Low Vol"
                        st.caption(vix_level)
    
    elif selected_indexes and not st.session_state.predictions:
        st.info("👆 Click 'Analyze & Predict' to generate predictions for selected indexes")
    
    # Backtesting execution
    if run_backtest_btn and 'run_backtest_analysis' in locals() and run_backtest_analysis and selected_indexes:
        st.header("📊 Backtest Results")
        
        # Initialize session state for backtest results
        if 'backtest_results' not in st.session_state:
            st.session_state.backtest_results = {}
        
        backtest_progress = st.progress(0)
        backtest_status = st.empty()
        
        all_backtest_results = []
        
        for idx, index_name in enumerate(selected_indexes):
            index_ticker = INDEXES[index_name]
            etf_ticker = INDEX_ETFS.get(index_ticker, index_ticker)
            
            backtest_status.text(f"Running backtest for {index_name}...")
            
            # Run backtest
            backtest_df = run_backtest(
                st.session_state.api_key,
                index_name,
                index_ticker,
                etf_ticker,
                datetime.combine(backtest_start, datetime.min.time()),
                datetime.combine(backtest_end, datetime.min.time()),
                backtest_model
            )
            
            if not backtest_df.empty:
                all_backtest_results.append(backtest_df)
                
                # Calculate metrics
                metrics = calculate_backtest_metrics(backtest_df)
                
                if metrics:
                    # Display metrics
                    st.subheader(f"📈 {index_name} Backtest Results")
                    
                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)
                    
                    with metric_col1:
                        st.metric("Total Predictions", f"{metrics['total_predictions']}")
                    
                    with metric_col2:
                        st.metric("Direction Accuracy", f"{metrics['direction_accuracy']:.1f}%")
                    
                    with metric_col3:
                        st.metric("Mean Error", f"{metrics['mean_error_pct']:.2f}%")
                    
                    with metric_col4:
                        st.metric("RMSE", f"${metrics['rmse']:.2f}")
                    
                    # Show detailed results
                    with st.expander(f"📊 Detailed Backtest Data for {index_name}", expanded=False):
                        # Format the dataframe for display
                        display_df = backtest_df.copy()
                        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
                        display_df['current_price'] = display_df['current_price'].apply(lambda x: f"${x:.2f}")
                        display_df['predicted_eod'] = display_df['predicted_eod'].apply(lambda x: f"${x:.2f}")
                        display_df['actual_eod'] = display_df['actual_eod'].apply(lambda x: f"${x:.2f}")
                        display_df['predicted_change_pct'] = display_df['predicted_change_pct'].apply(lambda x: f"{x:.2f}%")
                        display_df['actual_change_pct'] = display_df['actual_change_pct'].apply(lambda x: f"{x:.2f}%")
                        display_df['error_pct'] = display_df['error_pct'].apply(lambda x: f"{x:.2f}%")
                        display_df['direction_correct'] = display_df['direction_correct'].apply(lambda x: "✓" if x else "✗")
                        
                        st.dataframe(display_df, width='stretch', hide_index=True)
                    
                    # Create accuracy chart
                    import plotly.graph_objects as go
                    
                    fig_acc = go.Figure()
                    
                    # Add predicted vs actual lines
                    fig_acc.add_trace(go.Scatter(
                        x=backtest_df['date'],
                        y=backtest_df['predicted_eod'],
                        mode='lines+markers',
                        name='Predicted EOD',
                        line=dict(color='blue', width=2)
                    ))
                    
                    fig_acc.add_trace(go.Scatter(
                        x=backtest_df['date'],
                        y=backtest_df['actual_eod'],
                        mode='lines+markers',
                        name='Actual EOD',
                        line=dict(color='green', width=2)
                    ))
                    
                    fig_acc.update_layout(
                        title=f"{index_name} - Predicted vs Actual EOD Prices",
                        xaxis_title="Date",
                        yaxis_title="Price",
                        hovermode='x unified',
                        height=400
                    )
                    
                    st.plotly_chart(fig_acc, width='stretch')
                    
                    # Show best and worst predictions
                    st.write("**Best & Worst Predictions:**")
                    best_worst_col1, best_worst_col2 = st.columns(2)
                    
                    with best_worst_col1:
                        st.success(f"✨ Best: {metrics['best_prediction']['date'].strftime('%Y-%m-%d')} (Error: {metrics['best_prediction']['error_pct']:.2f}%)")
                    
                    with best_worst_col2:
                        st.error(f"⚠️ Worst: {metrics['worst_prediction']['date'].strftime('%Y-%m-%d')} (Error: {metrics['worst_prediction']['error_pct']:.2f}%)")
                    
                    st.divider()
            
            backtest_progress.progress((idx + 1) / len(selected_indexes))
        
        backtest_status.text("Backtest complete!")
        time.sleep(0.5)
        backtest_status.empty()
        backtest_progress.empty()
        
        # Model optimization suggestions
        if all_backtest_results:
            combined_results = pd.concat(all_backtest_results, ignore_index=True)
            optimization = optimize_model_features(combined_results)
            
            if optimization:
                st.header("🎯 Model Optimization Insights")
                
                opt_col1, opt_col2, opt_col3 = st.columns(3)
                
                with opt_col1:
                    st.metric("High Confidence Accuracy", f"{optimization['high_confidence_accuracy']:.1f}%")
                    st.caption("Predictions with confidence > 70%")
                
                with opt_col2:
                    st.metric("Medium Confidence Accuracy", f"{optimization['medium_confidence_accuracy']:.1f}%")
                    st.caption("Predictions with confidence 50-70%")
                
                with opt_col3:
                    st.metric("Low Confidence Accuracy", f"{optimization['low_confidence_accuracy']:.1f}%")
                    st.caption("Predictions with confidence < 50%")
                
                st.info(f"💡 Recommendation: {optimization['recommendation']}")
    
    # Add tabs for additional features
    if st.session_state.api_key:
        st.divider()
        
        tab1, tab2, tab3 = st.tabs(["📜 Prediction History", "🔔 Alerts", "📊 Performance Stats"])
        
        with tab1:
            st.subheader("Prediction History")
            
            try:
                all_predictions = get_all_predictions(limit=50)
                
                if all_predictions:
                    history_data = []
                    for p in all_predictions:
                        history_data.append({
                            'Date': p.prediction_date.strftime('%Y-%m-%d %H:%M'),
                            'Index': p.index_name,
                            'Ticker': p.ticker,
                            'Current Price': f"${p.current_price:.2f}",
                            'Predicted': f"${p.predicted_price:.2f}",
                            'Change %': f"{p.change_pct:.2f}%",
                            'Confidence': f"{p.confidence:.1f}%",
                            'Model': p.model_type,
                            'Actual Price': f"${p.actual_price:.2f}" if p.actual_price else 'Pending',
                            'Accuracy': f"{p.accuracy:.1f}%" if p.accuracy else 'N/A'
                        })
                    
                    df_history = pd.DataFrame(history_data)
                    st.dataframe(df_history, width='stretch', hide_index=True)
                    
                    st.caption(f"Showing {len(all_predictions)} most recent predictions")
                else:
                    st.info("No prediction history available yet. Make your first prediction!")
            except Exception as e:
                st.error(f"Error loading prediction history: {str(e)}")
        
        with tab2:
            st.subheader("Active Alerts")
            
            # Display session alerts
            if st.session_state.alerts:
                st.success(f"🔔 {len(st.session_state.alerts)} alert(s) triggered this session:")
                for alert in st.session_state.alerts:
                    st.warning(alert)
            else:
                st.info("No alerts triggered in this session")
            
            try:
                active_alerts = get_active_alerts()
                
                if active_alerts:
                    st.divider()
                    st.subheader("All Active Alerts")
                    
                    alert_data = []
                    for a in active_alerts:
                        alert_data.append({
                            'Created': a.created_at.strftime('%Y-%m-%d %H:%M'),
                            'Index': a.index_name,
                            'Type': a.alert_type,
                            'Message': a.message,
                            'Threshold': f"{a.threshold:.1f}%",
                            'Current Value': f"{a.current_value:.1f}%"
                        })
                    
                    df_alerts = pd.DataFrame(alert_data)
                    st.dataframe(df_alerts, width='stretch', hide_index=True)
            except Exception as e:
                st.error(f"Error loading alerts: {str(e)}")
        
        with tab3:
            st.subheader("Model Performance Statistics")
            
            try:
                overall_stats = get_prediction_accuracy_stats()
                
                if overall_stats:
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Total Predictions", overall_stats['count'])
                    with col2:
                        st.metric("Average Accuracy", f"{overall_stats['avg_accuracy']:.1f}%")
                    with col3:
                        st.metric("Best Accuracy", f"{overall_stats['max_accuracy']:.1f}%")
                    with col4:
                        st.metric("Worst Accuracy", f"{overall_stats['min_accuracy']:.1f}%")
                    
                    st.divider()
                    
                    # Per-ticker stats
                    st.subheader("Performance by Index")
                    ticker_stats = []
                    for index_name, ticker in INDEXES.items():
                        stats = get_prediction_accuracy_stats(ticker=ticker)
                        if stats:
                            ticker_stats.append({
                                'Index': index_name,
                                'Ticker': ticker,
                                'Predictions': stats['count'],
                                'Avg Accuracy': f"{stats['avg_accuracy']:.1f}%",
                                'Best': f"{stats['max_accuracy']:.1f}%",
                                'Worst': f"{stats['min_accuracy']:.1f}%"
                            })
                    
                    if ticker_stats:
                        df_stats = pd.DataFrame(ticker_stats)
                        st.dataframe(df_stats, width='stretch', hide_index=True)
                else:
                    st.info("No completed predictions yet. Accuracy statistics will appear once predictions are verified with actual prices.")
            except Exception as e:
                st.error(f"Error loading statistics: {str(e)}")
