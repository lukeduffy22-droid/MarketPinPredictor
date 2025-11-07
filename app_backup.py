import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from polygon import RESTClient
from datetime import datetime, timedelta
import time
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

# Page configuration
st.set_page_config(
    page_title="Stock Index Price Predictor",
    page_icon="📈",
    layout="wide"
)

# Major stock indexes
INDEXES = {
    "S&P 500": "SPY",
    "Dow Jones": "DIA",
    "NASDAQ 100": "QQQ",
    "Russell 2000": "IWM"
}

# Initialize session state
if 'api_key' not in st.session_state:
    st.session_state.api_key = ''
if 'predictions' not in st.session_state:
    st.session_state.predictions = {}

def calculate_technical_indicators(df):
    """Calculate technical indicators for prediction"""
    # Simple Moving Averages
    df['SMA_5'] = df['close'].rolling(window=5).mean()
    df['SMA_10'] = df['close'].rolling(window=10).mean()
    df['SMA_20'] = df['close'].rolling(window=20).mean()
    
    # Exponential Moving Averages
    df['EMA_5'] = df['close'].ewm(span=5, adjust=False).mean()
    df['EMA_10'] = df['close'].ewm(span=10, adjust=False).mean()
    
    # Relative Strength Index (RSI)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = exp1 - exp2
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
    
    # Bollinger Bands
    df['BB_Middle'] = df['close'].rolling(window=20).mean()
    bb_std = df['close'].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
    
    # Momentum
    df['Momentum'] = df['close'] - df['close'].shift(10)
    
    # Rate of Change
    df['ROC'] = ((df['close'] - df['close'].shift(10)) / df['close'].shift(10)) * 100
    
    # Volume indicators
    df['Volume_SMA'] = df['volume'].rolling(window=20).mean()
    df['Volume_Ratio'] = df['volume'] / df['Volume_SMA']
    
    return df

def fetch_market_data(api_key, ticker, days=60):
    """Fetch historical market data from Polygon"""
    try:
        client = RESTClient(api_key)
        
        # Get date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Fetch aggregates (daily bars)
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            limit=50000
        )
        
        # Convert to DataFrame
        data = []
        for agg in aggs:
            data.append({
                'timestamp': datetime.fromtimestamp(agg.timestamp / 1000),
                'open': agg.open,
                'high': agg.high,
                'low': agg.low,
                'close': agg.close,
                'volume': agg.volume
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
        client = RESTClient(api_key)
        
        # Get the previous close
        prev_close = client.get_previous_close_agg(ticker=ticker)
        
        if prev_close and len(prev_close) > 0:
            return prev_close[0].close
        return None
    except Exception as e:
        st.error(f"Error fetching current price: {str(e)}")
        return None

def predict_eod_price(df):
    """Predict end-of-day price using technical indicators and ML"""
    if df is None or len(df) < 30:
        return None, None, None, None
    
    # Calculate technical indicators
    df = calculate_technical_indicators(df)
    
    # Drop rows with NaN values
    df_clean = df.dropna().copy()
    
    if len(df_clean) < 20:
        return None, None, None, None
    
    # Prepare features for prediction
    feature_columns = ['SMA_5', 'SMA_10', 'SMA_20', 'EMA_5', 'EMA_10', 
                       'RSI', 'MACD', 'Signal_Line', 'Momentum', 'ROC', 
                       'Volume_Ratio', 'BB_Upper', 'BB_Lower']
    
    # Create feature matrix (X) and target vector (y)
    # CRITICAL: Shift target by 1 to predict NEXT day's close
    df_clean['next_close'] = df_clean['close'].shift(-1)
    
    # Remove the last row (which has NaN for next_close) and any remaining NaNs
    df_model = df_clean[:-1].dropna().copy()
    
    if len(df_model) < 20:
        return None, None, None, None
    
    X = df_model[feature_columns].values
    y = df_model['next_close'].values
    
    # Split: use earlier data for training, recent data for testing
    train_size = int(len(X) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    # Scale features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Train model to predict next day's close
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    
    # Calculate accuracy on test set
    test_predictions = model.predict(X_test_scaled)
    mape = np.mean(np.abs((y_test - test_predictions) / y_test)) * 100
    accuracy = max(0, 100 - mape)
    
    # Get current price (last known close)
    current_price = df_clean['close'].iloc[-1]
    
    # Predict NEXT day's price using the most recent features
    # Use the second-to-last row of df_clean for features (last complete feature set)
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
    - **Live market data** from Polygon.io
    - **Technical analysis** using multiple indicators (RSI, MACD, Bollinger Bands, etc.)
    - **Machine learning predictions** for end-of-day prices
    - **Interactive charts** with historical data and trends
    - **Confidence scores** for each prediction
    """)
else:
    if analyze_button and selected_indexes:
        st.session_state.predictions = {}
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, index_name in enumerate(selected_indexes):
            ticker = INDEXES[index_name]
            status_text.text(f"Analyzing {index_name}...")
            
            # Fetch data
            df = fetch_market_data(st.session_state.api_key, ticker, days_history)
            
            if df is not None and len(df) > 0:
                # Predict EOD price (returns predicted price, confidence, df with indicators, and current price)
                predicted_price, confidence, df_with_indicators, current_price = predict_eod_price(df)
                
                if predicted_price and current_price:
                    st.session_state.predictions[index_name] = {
                        'ticker': ticker,
                        'current_price': current_price,
                        'predicted_price': predicted_price,
                        'confidence': confidence,
                        'df': df_with_indicators,
                        'change_pct': ((predicted_price - current_price) / current_price) * 100
                    }
            
            progress_bar.progress((idx + 1) / len(selected_indexes))
        
        status_text.text("Analysis complete!")
        time.sleep(0.5)
        status_text.empty()
        progress_bar.empty()
    
    # Display predictions
    if st.session_state.predictions:
        st.header("📊 Prediction Results")
        
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
    
    elif selected_indexes and not st.session_state.predictions:
        st.info("👆 Click 'Analyze & Predict' to generate predictions for selected indexes")
