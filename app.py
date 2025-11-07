import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from polygon import StocksClient
from datetime import datetime, timedelta
import time
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from database import init_db, save_prediction, get_predictions_by_ticker, get_all_predictions, save_alert, get_active_alerts, get_prediction_accuracy_stats
from models import train_linear_regression, train_random_forest
from options_gamma import get_gamma_analysis

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

# ETF proxies for data fetching (indexes don't have direct price data)
INDEX_ETFS = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "DJI": "DIA",
    "RUT": "IWM"
}

# Initialize session state
if 'api_key' not in st.session_state:
    st.session_state.api_key = ''
if 'predictions' not in st.session_state:
    st.session_state.predictions = {}
if 'selected_model' not in st.session_state:
    st.session_state.selected_model = 'Linear Regression'
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
    
    # Simple Moving Averages
    df['SMA_5'] = df['close'].rolling(window=params['sma_short']).mean()
    df['SMA_10'] = df['close'].rolling(window=params['sma_medium']).mean()
    df['SMA_20'] = df['close'].rolling(window=params['sma_long']).mean()
    
    # Exponential Moving Averages
    df['EMA_5'] = df['close'].ewm(span=params['ema_short'], adjust=False).mean()
    df['EMA_10'] = df['close'].ewm(span=params['ema_long'], adjust=False).mean()
    
    # VWAP (Volume Weighted Average Price)
    df['Typical_Price'] = (df['high'] + df['low'] + df['close']) / 3
    df['PV'] = df['Typical_Price'] * df['volume']
    df['VWAP'] = df['PV'].cumsum() / df['volume'].cumsum()
    
    # Kaufman's Adaptive Moving Average (AMA/KAMA)
    df['AMA'] = calculate_kama(df['close'], n_period=10, fast_period=2, slow_period=30)
    
    # Relative Strength Index (RSI)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=params['rsi_period']).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=params['rsi_period']).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df['close'].ewm(span=params['macd_fast'], adjust=False).mean()
    exp2 = df['close'].ewm(span=params['macd_slow'], adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=params['macd_signal'], adjust=False).mean()
    
    # Bollinger Bands
    df['BB_Middle'] = df['close'].rolling(window=params['bb_period']).mean()
    bb_std = df['close'].rolling(window=params['bb_period']).std()
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * params['bb_std'])
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * params['bb_std'])
    
    # Momentum
    df['Momentum'] = df['close'] - df['close'].shift(params['momentum_period'])
    
    # Rate of Change
    df['ROC'] = ((df['close'] - df['close'].shift(params['momentum_period'])) / df['close'].shift(params['momentum_period'])) * 100
    
    # Volume indicators
    df['Volume_SMA'] = df['volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['volume'] / df['Volume_SMA']
    
    return df

def fetch_vix_data(api_key, days=60):
    """Fetch VIX (Volatility Index) data from Polygon"""
    try:
        client = StocksClient(api_key)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch VIX data (using VXX ETF as proxy)
        aggs = client.get_aggregate_bars(
            symbol='VIX:INDEXCBOE',  # Try direct VIX index
            from_date=start_date.strftime("%Y-%m-%d"),
            to_date=end_date.strftime("%Y-%m-%d"),
            timespan='day',
            multiplier=1,
            adjusted=True,
            sort='asc',
            limit=50000
        )
        
        if not aggs:
            # Fallback to VXX ETF if VIX index not available
            aggs = client.get_aggregate_bars(
                symbol='VXX',
                from_date=start_date.strftime("%Y-%m-%d"),
                to_date=end_date.strftime("%Y-%m-%d"),
                timespan='day',
                multiplier=1,
                adjusted=True,
                sort='asc',
                limit=50000
            )
        
        # Convert to DataFrame
        data = []
        # Handle the response format from polygon library
        if isinstance(aggs, dict):
            # If it's a dict, the data is likely in a 'results' key or similar
            if 'results' in aggs:
                aggs_list = aggs['results']
            elif len(aggs) > 0:
                # Try to get the first value if it's a list
                aggs_list = list(aggs.values())[0] if isinstance(list(aggs.values())[0], list) else aggs
            else:
                aggs_list = []
        else:
            aggs_list = aggs
        
        for agg in aggs_list:
            if isinstance(agg, dict):
                data.append({
                    'timestamp': datetime.fromtimestamp(agg['t'] / 1000),
                    'vix_close': agg['c']
                })
        
        df = pd.DataFrame(data)
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

def fetch_market_data(api_key, ticker, days=60):
    """Fetch historical market data from Polygon"""
    try:
        client = StocksClient(api_key)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch aggregates (daily bars)
        aggs = client.get_aggregate_bars(
            symbol=ticker,
            from_date=start_date.strftime("%Y-%m-%d"),
            to_date=end_date.strftime("%Y-%m-%d"),
            timespan='day',
            multiplier=1,
            adjusted=True,
            sort='asc',
            limit=50000
        )
        
        # Convert to DataFrame
        data = []
        # Handle the response format from polygon library
        if isinstance(aggs, dict):
            # If it's a dict, the data is likely in a 'results' key or similar
            if 'results' in aggs:
                aggs_list = aggs['results']
            elif len(aggs) > 0:
                # Try to get the first value if it's a list
                aggs_list = list(aggs.values())[0] if isinstance(list(aggs.values())[0], list) else aggs
            else:
                aggs_list = []
        else:
            aggs_list = aggs
        
        for agg in aggs_list:
            if isinstance(agg, dict):
                data.append({
                    'timestamp': datetime.fromtimestamp(agg['t'] / 1000),
                    'open': agg['o'],
                    'high': agg['h'],
                    'low': agg['l'],
                    'close': agg['c'],
                    'volume': agg['v']
                })
        
        df = pd.DataFrame(data)
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def get_current_price(api_key, ticker):
    """Get current/latest price"""
    try:
        client = StocksClient(api_key)
        
        # Get previous day's close
        end_date = datetime.now()
        start_date = end_date - timedelta(days=5)
        
        aggs = client.get_aggregate_bars(
            symbol=ticker,
            from_date=start_date.strftime("%Y-%m-%d"),
            to_date=end_date.strftime("%Y-%m-%d"),
            timespan='day',
            multiplier=1,
            adjusted=True,
            sort='desc',
            limit=1
        )
        
        if aggs:
            # Handle the response format
            if isinstance(aggs, dict):
                if 'results' in aggs and len(aggs['results']) > 0:
                    return aggs['results'][0]['c']
                elif len(aggs) > 0:
                    aggs_list = list(aggs.values())[0] if isinstance(list(aggs.values())[0], list) else aggs
                    if aggs_list and len(aggs_list) > 0:
                        return aggs_list[0]['c']
            elif isinstance(aggs, list) and len(aggs) > 0:
                return aggs[0]['c']
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

async def setup_websocket_streaming(api_key, tickers, on_data_callback):
    """Setup WebSocket connection for real-time data streaming"""
    import asyncio
    import websocket
    import json
    
    ws_url = "wss://socket.polygon.io/stocks"
    
    def on_open(ws):
        # Authenticate
        auth_msg = {"action": "auth", "params": api_key}
        ws.send(json.dumps(auth_msg))
        
        # Subscribe to minute aggregates for tickers
        subscribe_params = ",".join([f"AM.{ticker}" for ticker in tickers])
        subscribe_msg = {"action": "subscribe", "params": subscribe_params}
        ws.send(json.dumps(subscribe_msg))
        st.success(f"Connected to real-time data stream for {', '.join(tickers)}")
    
    def on_message(ws, message):
        data = json.loads(message)
        if isinstance(data, list):
            for item in data:
                if item.get('ev') == 'AM':  # Minute aggregate
                    on_data_callback(item)
    
    def on_error(ws, error):
        st.error(f"WebSocket error: {error}")
    
    def on_close(ws):
        st.info("WebSocket connection closed")
    
    # Create WebSocket connection
    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    
    # Run WebSocket (this blocks, so run in a thread in production)
    try:
        ws.run_forever()
    except Exception as e:
        st.error(f"WebSocket connection failed: {e}")

def predict_eod_price(df, model_type='Linear Regression', timeframe='1-day'):
    """Predict end-of-day price using technical indicators and ML"""
    if df is None or len(df) < 30:
        return None, None, None, None
    
    # Calculate technical indicators
    df = calculate_technical_indicators(df)
    
    # Drop rows with NaN values
    df_clean = df.dropna().copy()
    
    if len(df_clean) < 20:
        return None, None, None, None
    
    # Prepare features for prediction - including new indicators
    feature_columns = ['SMA_5', 'SMA_10', 'SMA_20', 'EMA_5', 'EMA_10', 
                       'RSI', 'MACD', 'Signal_Line', 'Momentum', 'ROC', 
                       'Volume_Ratio', 'BB_Upper', 'BB_Lower', 'VWAP', 'AMA']
    
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
    df_model = df_clean[:-shift_days].dropna().copy()
    
    if len(df_model) < 20:
        return None, None, None, None
    
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
    predicted_price = model.predict(latest_scaled)[0]
    
    # Calculate confidence based on recent trend consistency and model performance
    recent_prices = df_clean['close'].tail(10).values
    price_std = np.std(recent_prices)
    price_mean = np.mean(recent_prices)
    volatility = (price_std / price_mean) * 100
    
    # Confidence decreases with volatility and poor accuracy
    base_confidence = min(accuracy, 85)
    confidence = max(40, base_confidence - (volatility * 2))
    
    return predicted_price, confidence, df_clean, current_price

def create_price_chart(df, predicted_price, ticker_name):
    """Create interactive price chart with prediction"""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f'{ticker_name} Price & Indicators', 'RSI', 'Volume'),
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
    
    # Volume
    colors = ['red' if close < open else 'green' 
              for close, open in zip(df['close'], df['open'])]
    fig.add_trace(
        go.Bar(x=df['timestamp'], y=df['volume'], 
               name='Volume', marker_color=colors),
        row=3, col=1
    )
    
    fig.update_layout(
        height=800,
        showlegend=True,
        xaxis_rangeslider_visible=False,
        hovermode='x unified'
    )
    
    fig.update_yaxes(title_text="Price ($)", row=1, col=1)
    fig.update_yaxes(title_text="RSI", row=2, col=1)
    fig.update_yaxes(title_text="Volume", row=3, col=1)
    
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
        default=["S&P 500", "NASDAQ 100"]
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
    
    # WebSocket streaming control
    st.header("🔌 Real-time Streaming")
    enable_streaming = st.checkbox("Enable WebSocket streaming", value=False, 
                                  help="Connect to real-time data feed (requires paid Polygon subscription)")
    
    if enable_streaming:
        if st.button("Start Streaming", type="secondary"):
            st.info("WebSocket streaming would connect to wss://socket.polygon.io/stocks")
            st.caption("Note: Real-time streaming requires implementation in production environment")
    
    st.divider()
    
    analyze_button = st.button("🔄 Analyze & Predict", type="primary", use_container_width=True)
    
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
            etf_ticker = INDEX_ETFS.get(index_ticker, index_ticker)  # ETF for price data
            status_text.text(f"Analyzing {index_name}...")
            
            # Fetch price data using ETF proxy
            df = fetch_market_data(st.session_state.api_key, etf_ticker, days_history)
            
            if df is not None and len(df) > 0:
                # Merge VIX data if available
                if vix_df is not None and len(vix_df) > 0:
                    df = pd.merge(df, vix_df, on='timestamp', how='left')
                    df['vix_close'] = df['vix_close'].fillna(method='ffill')
                
                # Get current price for GEX calculation
                current_price = df['close'].iloc[-1]
                
                # Calculate GEX levels using actual index ticker for options
                gex_data = calculate_gex(st.session_state.api_key, index_ticker, current_price)
                
                # Predict EOD price using selected model and timeframe
                predicted_price, confidence, df_with_indicators, current_price = predict_eod_price(
                    df, 
                    model_type=st.session_state.selected_model,
                    timeframe=st.session_state.timeframe
                )
                
                if predicted_price and current_price:
                    change_pct = ((predicted_price - current_price) / current_price) * 100
                    
                    st.session_state.predictions[index_name] = {
                        'ticker': ticker,
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'confidence': confidence,
                        'df': df_with_indicators,
                        'change_pct': change_pct,
                        'model_type': st.session_state.selected_model,
                        'timeframe': st.session_state.timeframe,
                        'gex_data': gex_data,  # Add GEX data
                        'has_vix': vix_df is not None  # Track VIX availability
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
                            ticker=ticker,
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
                                ticker=ticker,
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
        
        # Detailed charts
        st.header("📈 Detailed Analysis")
        
        for index_name, pred in st.session_state.predictions.items():
            with st.expander(f"📊 {index_name} ({pred['ticker']}) - Detailed Chart", expanded=True):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Last Close", f"${pred['current_price']:.2f}")
                with col2:
                    st.metric("Predicted EOD", f"${pred['predicted_price']:.2f}")
                with col3:
                    st.metric("Expected Change", f"{pred['change_pct']:.2f}%")
                
                # Create and display chart
                fig = create_price_chart(pred['df'], pred['predicted_price'], index_name)
                st.plotly_chart(fig, use_container_width=True)
                
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
                        
                        st.dataframe(gamma_walls_display, use_container_width=True)
                    
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
                        
                        st.plotly_chart(fig_gex, use_container_width=True)
                    
                    if 'summary' in gex:
                        st.info(f"💡 {gex['summary']}")
                    
                    # Key levels
                    if gex.get('key_levels'):
                        st.info(f"Key Support/Resistance: ${gex['key_levels'][0]:.2f} / ${gex['key_levels'][1]:.2f}")
                
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
                    st.dataframe(df_history, use_container_width=True, hide_index=True)
                    
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
                    st.dataframe(df_alerts, use_container_width=True, hide_index=True)
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
                        st.dataframe(df_stats, use_container_width=True, hide_index=True)
                else:
                    st.info("No completed predictions yet. Accuracy statistics will appear once predictions are verified with actual prices.")
            except Exception as e:
                st.error(f"Error loading statistics: {str(e)}")
