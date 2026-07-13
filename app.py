import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from database import init_db, save_prediction, get_predictions_by_ticker, get_all_predictions, save_alert, get_active_alerts, get_prediction_accuracy_stats
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
from app.features.technical_indicators import DEFAULT_INDICATOR_PARAMS
from app.services.streamlit_market_data import calculate_gex, fetch_market_data, fetch_vix_data
from app.services.streamlit_predictions import predict_eod_price
from app.visualization.price_chart import create_price_chart

# Initialize database
init_db()

# Page configuration
st.set_page_config(
    page_title="Stock Index Price Predictor",
    page_icon="📈",
    layout="wide"
)

# Major stock indexes - Using actual index tickers
# NOTE: DJI uses DIA ETF options (no direct index options exist)
INDEXES = {
    "S&P 500 (SPX)": "SPX",
    "NASDAQ 100 (NDX)": "NDX", 
    "Dow Jones (DJI via DIA options)": "DJI",
    "Russell 2000 (RUT)": "RUT",
    "VIX (Volatility)": "VIX"
}

# Polygon index ticker format (with I: prefix for indices)
INDEX_POLYGON_TICKERS = {
    "SPX": "I:SPX",
    "NDX": "I:NDX",
    "DJI": "I:DJI",
    "RUT": "I:RUT",
    "VIX": "I:VIX"
}

# ETF proxies as fallback (only used if direct index data unavailable)
INDEX_ETFS = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "DJI": "DIA",
    "RUT": "IWM",
    "VIX": "UVXY"  # VIX ETF proxy (though VIX options trade directly)
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
    st.session_state.indicator_params = DEFAULT_INDICATOR_PARAMS.copy()

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

def load_daily_pin_history(symbol: str, date_str: str = None) -> pd.DataFrame:
    """
    Load all snapshots from NDJSON for a specific symbol and date.
    Returns a DataFrame with pin history (Time, Pin Strike, Spot, Distance, Pull Strength, etc.)
    """
    import os
    import json
    from datetime import datetime, timezone
    import pytz
    
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    ndjson_path = os.path.join('exports', symbol, f'{date_str}.ndjson')
    
    if not os.path.exists(ndjson_path):
        return pd.DataFrame()
    
    rows = []
    et_tz = pytz.timezone('US/Eastern')
    
    with open(ndjson_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                snap = json.loads(line)
                
                # Parse timestamp and convert to ET
                ts_str = snap.get('generated_at_utc', '')
                if ts_str:
                    try:
                        dt_utc = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        dt_et = dt_utc.astimezone(et_tz)
                        time_et = dt_et.strftime('%I:%M %p ET')
                    except:
                        time_et = ts_str[:8] if len(ts_str) >= 8 else 'N/A'
                else:
                    time_et = 'N/A'
                
                # Get values with backward compatibility
                pin_strike = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike', 0)
                spot = snap.get('spot_last', 0)
                gross_gex = snap.get('gross_gex', 0)
                net_gex = snap.get('net_gex', 0)
                call_gex = snap.get('call_gex_total', 0)
                put_gex = snap.get('put_gex_total', 0)
                
                # Calculate distance and pull strength
                distance_pct = ((spot - pin_strike) / pin_strike * 100) if pin_strike else 0
                pull_strength = abs(net_gex / gross_gex * 100) if gross_gex else 0
                
                rows.append({
                    'Time': time_et,
                    'Pin Strike': pin_strike,
                    'Spot Price': spot,
                    'Distance': f"{distance_pct:+.2f}%",
                    'Pull Strength': f"{pull_strength:.2f}",
                    'Total GEX': f"${gross_gex:.3f}B",
                    'Net GEX': f"${net_gex:.3f}B",
                    'Valid': '✅' if snap.get('validation_is_valid', False) else '❌',
                    # Raw values for CSV export
                    '_pin_strike': pin_strike,
                    '_spot': spot,
                    '_distance_pct': distance_pct,
                    '_pull_strength': pull_strength,
                    '_gross_gex': gross_gex,
                    '_net_gex': net_gex,
                    '_call_gex': call_gex,
                    '_put_gex': put_gex,
                    '_timestamp_utc': ts_str,
                    '_is_valid': snap.get('validation_is_valid', False)
                })
            except Exception as e:
                continue
    
    return pd.DataFrame(rows)

def create_eod_zip_export(date_str: str = None) -> bytes:
    """
    Create a ZIP file with all daily data for download.
    Includes: NDJSON files, combined CSV, pin history CSVs
    """
    import io
    import zipfile
    import os
    import json
    from datetime import datetime, timezone
    
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    buffer = io.BytesIO()
    
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        symbols = ['SPX', 'NDX', 'RUT']
        all_data = []
        
        for symbol in symbols:
            ndjson_path = os.path.join('exports', symbol, f'{date_str}.ndjson')
            
            # Add NDJSON file if exists
            if os.path.exists(ndjson_path):
                with open(ndjson_path, 'r') as f:
                    zf.writestr(f'{symbol}_{date_str}.ndjson', f.read())
                
                # Also create pin history CSV
                pin_df = load_daily_pin_history(symbol, date_str)
                if not pin_df.empty:
                    # Export clean CSV (without display formatting)
                    export_df = pd.DataFrame({
                        'symbol': symbol,
                        'timestamp_utc': pin_df['_timestamp_utc'],
                        'pin_strike': pin_df['_pin_strike'],
                        'spot': pin_df['_spot'],
                        'distance_pct': pin_df['_distance_pct'],
                        'pull_strength': pin_df['_pull_strength'],
                        'gross_gex': pin_df['_gross_gex'],
                        'net_gex': pin_df['_net_gex'],
                        'call_gex': pin_df['_call_gex'],
                        'put_gex': pin_df['_put_gex'],
                        'is_valid': pin_df['_is_valid']
                    })
                    zf.writestr(f'{symbol}_pin_history_{date_str}.csv', export_df.to_csv(index=False))
                    all_data.append(export_df)
        
        # Add combined CSV
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            zf.writestr(f'all_indices_{date_str}.csv', combined_df.to_csv(index=False))
    
    buffer.seek(0)
    return buffer.getvalue()

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
        options=["Ensemble", "Linear Regression", "Random Forest"],
        index=0,
        help="Choose the machine learning model for predictions. Ensemble blends linear and random forest models using recent holdout accuracy."
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
            defaults = DEFAULT_INDICATOR_PARAMS.copy()
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
    
    # Create tabs for Live Streaming, Frozen Gamma, and Debug Snapshot Viewer
    streaming_tab, frozen_gamma_tab, debug_snapshot_tab = st.tabs(["📡 Live Streaming", "🧊 Frozen Gamma (Audit)", "🔍 Debug Snapshot"])
    
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
                
                # Display ETF proxy notice if applicable
                if snapshot and getattr(snapshot, 'is_etf_proxy', False):
                    options_root = getattr(snapshot, 'chain_symbol_used', '')
                    st.info(f"Note: {symbol} gamma data sourced from {options_root} ETF options (no direct index options exist)")
                
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
                        failure_reasons = snapshot.validation_failure_reasons or []
                        pregate = getattr(snapshot, 'pregate_reason', '') or ''
                        # Check for GEX concentration failure (common for NDX due to thin liquidity)
                        has_concentration_failure = (
                            any('CONCENTRATION' in r for r in failure_reasons) or
                            'CONCENTRATION' in pregate
                        )
                        if has_concentration_failure and snapshot.symbol == 'NDX':
                            st.warning("⚠️ NDX gamma rejected due to extreme strike concentration (thin options liquidity). This is a feature, not a bug - the validation gate correctly blocks unreliable data.")
                        else:
                            st.caption(f"⚠️ Validation issues: {', '.join(failure_reasons)}")
                    
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
                    
                    # Pin History Table (from NDJSON)
                    today_str = datetime.now().strftime('%Y-%m-%d')
                    pin_history = load_daily_pin_history(symbol, today_str)
                    
                    if not pin_history.empty:
                        with st.expander(f"📊 Intraday Pin History ({len(pin_history)} samples)", expanded=False):
                            # Display table (hide internal columns)
                            display_cols = ['Time', 'Pin Strike', 'Spot Price', 'Distance', 'Pull Strength', 'Total GEX', 'Net GEX', 'Valid']
                            st.dataframe(pin_history[display_cols], hide_index=True, width='stretch')
                            
                            # Export button for this symbol
                            export_df = pd.DataFrame({
                                'symbol': symbol,
                                'timestamp_utc': pin_history['_timestamp_utc'],
                                'pin_strike': pin_history['_pin_strike'],
                                'spot': pin_history['_spot'],
                                'distance_pct': pin_history['_distance_pct'],
                                'pull_strength': pin_history['_pull_strength'],
                                'gross_gex': pin_history['_gross_gex'],
                                'net_gex': pin_history['_net_gex'],
                                'is_valid': pin_history['_is_valid']
                            })
                            st.download_button(
                                f"💾 Save {symbol} Pin History",
                                export_df.to_csv(index=False),
                                f"{symbol}_pin_history_{today_str}.csv",
                                "text/csv"
                            )
                    
                    st.divider()
            
            # End of Day Export (after all symbols)
            st.subheader("📦 End of Day Export")
            st.caption("Download everything in one click - all NDJSON files, CSVs, and pin history")
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            
            # Check if any data exists
            has_data = any(
                os.path.exists(os.path.join('exports', sym, f'{today_str}.ndjson'))
                for sym in ['SPX', 'NDX', 'RUT']
            )
            
            if has_data:
                try:
                    zip_data = create_eod_zip_export(today_str)
                    st.download_button(
                        "📥 Download ALL Data (ZIP)",
                        zip_data,
                        f"gamma_data_{today_str}.zip",
                        "application/zip",
                        type="primary"
                    )
                    st.success(f"ZIP includes: NDJSON files, pin history CSVs, combined all_indices.csv")
                except Exception as e:
                    st.error(f"Error creating ZIP: {str(e)[:50]}")
            else:
                st.info("No data available for today. Run gamma sampling during market hours to collect data.")
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
        
        if st.button("🔄 Reload from Cache", type="secondary", help="Repaint UI from backend memory - no live data fetch"):
            try:
                for idx_name in selected_indexes:
                    ticker = INDEXES[idx_name]
                    # Read from in-memory ring buffer only (no Polygon call, no socket)
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
                st.success("UI repainted from backend cache")
            except Exception as e:
                st.error(f"Error fetching cached data: {str(e)[:50]}")
        
        # Snapshot data fallback using REST API
        st.divider()
        st.caption("**Snapshot Data (REST API Fallback)**")
        if st.button("📸 Get Current Prices", type="secondary", help="Fetch latest prices via Polygon REST API"):
            if selected_indexes and st.session_state.api_key:
                tickers_to_fetch = [INDEX_POLYGON_TICKERS[INDEXES[idx]] for idx in selected_indexes]
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
        
        # Live Gamma Monitor
        st.divider()
        st.subheader("🧲 Live Gamma Pull Monitor")
        if st.button("📊 Analyze Gamma Pull", type="primary", help="Calculate real-time gamma pin and dealer hedging direction"):
            if st.session_state.api_key and selected_indexes:
                with st.spinner("Calculating gamma structure..."):
                    import numpy as np
                    from polygon.rest import RESTClient
                    from scipy.stats import norm
                    
                    def bs_gamma(S, K, T, sigma=0.25):
                        if T <= 0 or S <= 0: return 0
                        d1 = (np.log(S/K) + (0.05 + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
                        return norm.pdf(d1) / (S * sigma * np.sqrt(T))
                    
                    client = RESTClient(st.session_state.api_key)
                    today = datetime.now().strftime('%Y-%m-%d')
                    
                    # Get prices
                    tickers = [f"I:{INDEXES[idx]}" for idx in selected_indexes]
                    try:
                        results = list(client.get_snapshot_indices(ticker_any_of=tickers))
                        prices = {r.ticker.replace('I:', ''): r.value for r in results}
                    except:
                        prices = {}
                    
                    gamma_cols = st.columns(len(selected_indexes))
                    
                    for i, idx_name in enumerate(selected_indexes):
                        symbol = INDEXES[idx_name]
                        spot = prices.get(symbol)
                        
                        with gamma_cols[i]:
                            if not spot:
                                st.warning(f"{symbol}: No price")
                                continue
                            
                            st.markdown(f"**{symbol}** ${spot:,.2f}")
                            
                            # Fetch options
                            low, high = spot * 0.95, spot * 1.05
                            contracts = []
                            count = 0
                            try:
                                for c in client.list_snapshot_options_chain(symbol):
                                    count += 1
                                    if count > 1500: break
                                    if hasattr(c, 'details') and c.details:
                                        if c.details.expiration_date == today:
                                            s = c.details.strike_price
                                            if low <= s <= high:
                                                oi = getattr(c, 'open_interest', 0) or 0
                                                iv = c.implied_volatility if hasattr(c, 'implied_volatility') and c.implied_volatility else 0.25
                                                gamma = bs_gamma(spot, s, 0.003, iv if iv > 0 else 0.25)
                                                gex = gamma * oi * 100 * (spot**2) / 1e9
                                                contracts.append({'strike': s, 'type': c.details.contract_type, 'gex': gex})
                            except Exception as e:
                                st.error(f"Error: {str(e)[:30]}")
                                continue
                            
                            if not contracts:
                                st.info("No 0DTE contracts")
                                continue
                            
                            # Aggregate
                            gex_by_strike = {}
                            call_gex = {}
                            put_gex = {}
                            for c in contracts:
                                s = c['strike']
                                gex_by_strike[s] = gex_by_strike.get(s, 0) + c['gex']
                                if c['type'] == 'call':
                                    call_gex[s] = call_gex.get(s, 0) + c['gex']
                                else:
                                    put_gex[s] = put_gex.get(s, 0) + c['gex']
                            
                            pin = max(gex_by_strike.keys(), key=lambda s: abs(gex_by_strike[s]))
                            above = sum(g for s, g in gex_by_strike.items() if s > spot)
                            below = sum(g for s, g in gex_by_strike.items() if s < spot)
                            
                            pin_dist = (pin - spot) / spot * 100
                            st.metric("Gamma Pin", f"${pin:,.0f}", f"{pin_dist:+.2f}%")
                            
                            if above > below:
                                pull_pct = above / (above + below) * 100
                                st.success(f"⬆️ PULL UP ({pull_pct:.0f}%)")
                            else:
                                pull_pct = below / (above + below) * 100
                                st.error(f"⬇️ PULL DOWN ({pull_pct:.0f}%)")
                            
                            # Dealer hedge
                            pin_call = call_gex.get(pin, 0)
                            pin_put = put_gex.get(pin, 0)
                            if pin_call > pin_put:
                                st.caption("🏦 Call-heavy → Resistance")
                            else:
                                st.caption("🏦 Put-heavy → Support")
                    
                    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S ET')}")
            else:
                st.warning("Enter API key and select indexes first")
        
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
    
    # === DEBUG SNAPSHOT VIEWER TAB ===
    with debug_snapshot_tab:
        st.markdown("**Raw Audit Snapshot Viewer** — Shows exact values from the latest audit JSON for debugging.")
        st.caption("When UI looks wrong, use this to prove whether backend stored wrong values or UI displayed wrong values.")
        
        if selected_indexes:
            debug_symbol = st.selectbox("Select Index for Debug View", [INDEXES[idx] for idx in selected_indexes])
            
            if debug_symbol:
                snapshot = load_last_valid_snapshot(debug_symbol)
                
                if snapshot is None:
                    st.warning(f"No snapshot available for {debug_symbol}")
                else:
                    # Show snapshot file path
                    st.code(f"logs/audit/{debug_symbol}/latest.json", language="text")
                    
                    # Core GEX values section
                    st.subheader("Core GEX Values")
                    gex_col1, gex_col2, gex_col3, gex_col4 = st.columns(4)
                    with gex_col1:
                        st.metric("gross_gex", f"{snapshot.gross_gex:.6f}")
                    with gex_col2:
                        st.metric("net_gex", f"{snapshot.net_gex:.6f}")
                    with gex_col3:
                        st.metric("call_gex_total", f"{snapshot.call_gex_total:.6f}")
                    with gex_col4:
                        st.metric("put_gex_total", f"{snapshot.put_gex_total:.6f}")
                    
                    # Invariant check
                    invariant_ok = snapshot.gross_gex >= abs(snapshot.net_gex) - 1e-10
                    sum_ok = abs(snapshot.gross_gex - (snapshot.call_gex_total + snapshot.put_gex_total)) < 1e-10
                    if invariant_ok and sum_ok:
                        st.success("✅ GEX invariants hold: gross_gex >= |net_gex|, gross = call + put")
                    else:
                        st.error("❌ GEX invariant violated!")
                    
                    # Chain Identity (includes ETF proxy info)
                    st.subheader("Chain Identity")
                    chain_col1, chain_col2, chain_col3 = st.columns(3)
                    with chain_col1:
                        st.metric("chain_symbol_used", snapshot.chain_symbol_used or "N/A")
                    with chain_col2:
                        st.metric("underlying_reported", snapshot.underlying_reported or snapshot.symbol)
                    with chain_col3:
                        is_etf = getattr(snapshot, 'is_etf_proxy', False)
                        st.metric("is_etf_proxy", "Yes" if is_etf else "No")
                    if getattr(snapshot, 'is_etf_proxy', False):
                        st.info(f"Options data sourced from {snapshot.chain_symbol_used} ETF (not direct index options)")
                    
                    # Pre-gate explanation
                    st.subheader("Pre-Gate Explanation")
                    pg_col1, pg_col2, pg_col3, pg_col4 = st.columns(4)
                    with pg_col1:
                        st.metric("contracts_count", snapshot.contracts_count or 0)
                    with pg_col2:
                        st.metric("strike_count", snapshot.strike_count or 0)
                    with pg_col3:
                        st.metric("nonzero_strikes", snapshot.nonzero_strike_count or 0)
                    with pg_col4:
                        st.metric("top_strike_share", f"{(snapshot.top_strike_share or 0)*100:.1f}%")
                    
                    if snapshot.pregate_reason:
                        st.warning(f"⚠️ Pre-gate issue: {snapshot.pregate_reason}")
                    else:
                        st.success("✅ No pre-gate issues detected")
                    
                    # Top 5 strikes
                    st.subheader("Top 5 Strikes by |GEX|")
                    if snapshot.top_strikes_by_abs_gex:
                        for i, strike_data in enumerate(snapshot.top_strikes_by_abs_gex[:5]):
                            st.caption(f"#{i+1}: Strike ${strike_data.get('strike', 0):,.0f} | net_gex: {strike_data.get('net_gex', 0):.6f} | abs_gex: {strike_data.get('abs_gex', 0):.6f}")
                    else:
                        st.caption("No strike data available")
                    
                    # Diagnostics
                    st.subheader("Diagnostic Fields")
                    diag_col1, diag_col2 = st.columns(2)
                    with diag_col1:
                        st.json({
                            "gamma_by_distance": snapshot.gamma_by_distance,
                            "truncation": snapshot.truncation,
                            "vol_regime": snapshot.vol_regime,
                        })
                    with diag_col2:
                        st.json({
                            "confidence": snapshot.confidence,
                            "confidence_factors": snapshot.confidence_factors,
                            "dispersion_ratio": snapshot.dispersion_ratio,
                        })
                    
                    # Validation status
                    st.subheader("Validation Status")
                    if snapshot.validation_is_valid:
                        st.success("✅ Snapshot is valid")
                    else:
                        failure_reasons = snapshot.validation_failure_reasons or []
                        pregate = getattr(snapshot, 'pregate_reason', '') or ''
                        has_concentration_failure = (
                            any('CONCENTRATION' in r for r in failure_reasons) or
                            'CONCENTRATION' in pregate
                        )
                        if has_concentration_failure and snapshot.symbol == 'NDX':
                            st.error("❌ NDX gamma rejected due to extreme strike concentration")
                            st.info("This is expected for NDX due to thin options liquidity. The validation gate correctly blocks unreliable gamma data when a single strike dominates >90% of total GEX.")
                        else:
                            st.error(f"❌ Validation failed: {failure_reasons}")
                    
                    # Raw JSON expander
                    with st.expander("📄 Full Raw JSON", expanded=False):
                        st.json(snapshot.to_dict())
        else:
            st.info("Select indexes in the sidebar to view debug snapshot data.")
    
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
                    gex_data=gex_data,  # Critical: gamma pin influences EOD prediction
                    indicator_params=st.session_state.indicator_params
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
                
                # Display gamma pinning and max pain info if available
                if 'gex_data' in pred and pred['gex_data']:
                    gex = pred['gex_data']
                    if 'pin_strike' in gex:
                        pin_symbol = "📍"
                        direction_arrow = "⬆️" if gex['direction'] == 'above' else "⬇️" if gex['direction'] == 'below' else "↔️"
                        st.markdown(f"**{pin_symbol} Gamma Pin:** ${gex['pin_strike']:.0f} {direction_arrow}")
                    if 'max_pain_strike' in gex:
                        pain_arrow = "⬆️" if gex.get('max_pain_direction') == 'above' else "⬇️" if gex.get('max_pain_direction') == 'below' else "↔️"
                        st.markdown(f"**💰 Max Pain:** ${gex['max_pain_strike']:.0f} {pain_arrow}")
                    if 'summary' in gex:
                        st.caption(gex['summary'])
        
        st.divider()
        
        # Market Maker Target Section - Where MMs are incentivized to drive price
        st.header("🎯 Market Maker Targets")
        st.caption("Where market makers are most incentivized to drive price by end of day")
        
        mm_cols = st.columns(len(st.session_state.predictions))
        
        for mm_col, (index_name, pred) in zip(mm_cols, st.session_state.predictions.items()):
            with mm_col:
                current_price = pred['current_price']
                gex = pred.get('gex_data', {})
                
                gamma_pin = gex.get('pin_strike')
                max_pain = gex.get('max_pain_strike')
                
                st.subheader(index_name)
                
                if gamma_pin or max_pain:
                    # Calculate distances
                    gamma_dist = ((gamma_pin - current_price) / current_price * 100) if gamma_pin else None
                    pain_dist = ((max_pain - current_price) / current_price * 100) if max_pain else None
                    
                    # Determine primary target (gamma pin usually stronger intraday)
                    if gamma_pin and max_pain:
                        # If they're close (within 0.5%), show as aligned
                        if abs(gamma_pin - max_pain) / current_price < 0.005:
                            primary_target = gamma_pin
                            target_label = "Aligned Target"
                            target_icon = "🎯"
                            target_explanation = "Gamma Pin and Max Pain are aligned - strong magnet effect"
                        else:
                            # Gamma pin is primary for intraday
                            primary_target = gamma_pin
                            target_label = "Gamma Pin (Primary)"
                            target_icon = "📍"
                            target_explanation = "Strongest intraday pull from MM hedging activity"
                    elif gamma_pin:
                        primary_target = gamma_pin
                        target_label = "Gamma Pin"
                        target_icon = "📍"
                        target_explanation = "MM delta-hedging creates magnetic price action"
                    else:
                        primary_target = max_pain
                        target_label = "Max Pain"
                        target_icon = "💰"
                        target_explanation = "Where option writers pay out the least"
                    
                    primary_dist = ((primary_target - current_price) / current_price * 100) if primary_target else 0
                    direction = "⬆️" if primary_dist > 0 else "⬇️" if primary_dist < 0 else "↔️"
                    
                    # Primary MM Target
                    st.metric(
                        label=f"{target_icon} {target_label}",
                        value=f"${primary_target:,.0f}",
                        delta=f"{primary_dist:+.2f}% {direction}"
                    )
                    st.caption(target_explanation)
                    
                    # Show both levels if they exist and differ
                    if gamma_pin and max_pain and abs(gamma_pin - max_pain) / current_price >= 0.005:
                        st.divider()
                        level_col1, level_col2 = st.columns(2)
                        
                        with level_col1:
                            gamma_dir = "⬆️" if gamma_dist > 0 else "⬇️" if gamma_dist < 0 else "↔️"
                            st.markdown(f"**📍 Gamma Pin**")
                            st.markdown(f"${gamma_pin:,.0f} ({gamma_dist:+.2f}%)")
                            st.caption("Intraday hedging pull")
                        
                        with level_col2:
                            pain_dir = "⬆️" if pain_dist > 0 else "⬇️" if pain_dist < 0 else "↔️"
                            st.markdown(f"**💰 Max Pain**")
                            st.markdown(f"${max_pain:,.0f} ({pain_dist:+.2f}%)")
                            st.caption("Expiration settlement target")
                    
                    # Pull strength indicator
                    pull_strength = gex.get('pull_strength', 0)
                    if pull_strength:
                        if pull_strength < 1:
                            strength_label = "Strong Pull"
                            strength_color = "green"
                        elif pull_strength < 2:
                            strength_label = "Moderate Pull"
                            strength_color = "orange"
                        else:
                            strength_label = "Weak Pull"
                            strength_color = "gray"
                        st.markdown(f"<span style='color: {strength_color}; font-weight: bold;'>Pull Strength: {strength_label}</span>", unsafe_allow_html=True)
                    
                    # Close Predictor Signals
                    st.divider()
                    st.markdown("**📋 Close Predictor Signals**")
                    try:
                        close_resp = requests.get(f"http://localhost:8000/predict/close-overlay", params={"symbol": index_name}, timeout=8)
                        if close_resp.ok:
                            close_data = close_resp.json()
                            signals = close_data.get('signals', [])
                            
                            if signals:
                                for sig in signals[:4]:  # Show top 4 signals
                                    st.markdown(f"<span style='font-size: 0.85em;'>{sig['emoji']} {sig['message']}</span>", unsafe_allow_html=True)
                                
                                # Net bias summary
                                net_bias = close_data.get('net_bias', 'neutral')
                                bias_emoji = "📈" if net_bias == 'bullish' else "📉" if net_bias == 'bearish' else "↔️"
                                drift = close_data.get('drift_adjustment', 0)
                                expected = close_data.get('expected_close', 0)
                                
                                if expected:
                                    st.markdown(f"**{bias_emoji} Expected Close:** ${expected:,.0f} ({drift:+.1f} pts drift)")
                            else:
                                st.caption("Awaiting signals...")
                        elif close_resp.status_code == 429:
                            st.caption("⏳ Rate limited - signals pending...")
                        elif close_resp.status_code == 503:
                            st.caption("⏳ Computing signals...")
                        else:
                            st.caption("Close predictor unavailable")
                    except requests.exceptions.Timeout:
                        st.caption("⏳ Loading signals...")
                    except Exception as e:
                        st.caption(f"Close signals: {str(e)[:30]}...")
                else:
                    st.info("Gamma data unavailable")
                    st.caption("Waiting for options data...")
        
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
                    
                    # Display ETF proxy notice if applicable
                    if gex.get('is_etf_proxy'):
                        options_root = gex.get('options_root', '')
                        st.subheader(f"🎯 Gamma Exposure Analysis (via {options_root} options)")
                        st.info(f"Note: {pred['ticker']} options are traded via {options_root} ETF. All gamma metrics below use {options_root} options data.")
                    else:
                        st.subheader("🎯 Gamma Exposure Analysis")
                    
                    # Main gamma pin and max pain information
                    gamma_col1, gamma_col2, gamma_col3, gamma_col4 = st.columns(4)
                    
                    with gamma_col1:
                        if 'pin_strike' in gex:
                            st.metric("📍 Gamma Pin", f"${gex['pin_strike']:.0f}")
                            if 'pin_expiry' in gex and gex['pin_expiry']:
                                expiry_str = gex['pin_expiry'].strftime('%m/%d') if hasattr(gex['pin_expiry'], 'strftime') else str(gex['pin_expiry'])
                                st.caption(f"Expires: {expiry_str}")
                    
                    with gamma_col2:
                        if 'max_pain_strike' in gex:
                            st.metric("💰 Max Pain", f"${gex['max_pain_strike']:.0f}")
                            direction_arrow = "⬆️" if gex.get('max_pain_direction') == 'above' else "⬇️" if gex.get('max_pain_direction') == 'below' else "↔️"
                            st.caption(f"{direction_arrow} {gex.get('max_pain_distance_pct', 0):.1f}% from spot")
                    
                    with gamma_col3:
                        if 'total_gex' in gex:
                            st.metric("Total GEX", f"${gex['total_gex']:.1f}B")
                            if 'net_gex' in gex:
                                net_sign = "+" if gex['net_gex'] > 0 else ""
                                st.caption(f"Net: {net_sign}${gex['net_gex']:.1f}B")
                    
                    with gamma_col4:
                        if 'zero_gamma' in gex:
                            st.metric("Zero Gamma", f"${gex['zero_gamma']:.0f}")
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
                        
                        # Add max pain line
                        if 'max_pain_strike' in gex:
                            fig_gex.add_vline(
                                x=gex['max_pain_strike'],
                                line_dash="dot",
                                line_color="purple",
                                line_width=2,
                                annotation_text=f"Max Pain: ${gex['max_pain_strike']:.0f}"
                            )
                        
                        fig_gex.update_layout(
                            title="Gamma Exposure by Strike (w/ Gamma Pin & Max Pain)",
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
    
    # === NDJSON Snapshot Export Viewer ===
    st.divider()
    st.header("📁 Daily Snapshot Exports")
    st.caption("View and download intraday gamma snapshots saved throughout the trading day")
    
    # Get available export files
    export_base = "./exports"
    available_symbols = []
    if os.path.exists(export_base):
        available_symbols = [d for d in os.listdir(export_base) if os.path.isdir(os.path.join(export_base, d))]
    
    if available_symbols:
        # Add "All Indices" option at the start
        symbol_options = ["📦 All Indices"] + available_symbols
        
        export_col1, export_col2 = st.columns(2)
        
        with export_col1:
            export_symbol = st.selectbox("Select Index", symbol_options, key="export_symbol_select")
        
        with export_col2:
            # Get available dates - for "All Indices", combine dates from all symbols
            if export_symbol == "📦 All Indices":
                all_dates = set()
                for sym in available_symbols:
                    sym_dir = os.path.join(export_base, sym)
                    if os.path.exists(sym_dir):
                        all_dates.update(f.replace('.ndjson', '') for f in os.listdir(sym_dir) if f.endswith('.ndjson'))
                available_files = sorted(list(all_dates), reverse=True)
                symbol_dir = None
            else:
                symbol_dir = os.path.join(export_base, export_symbol)
                available_files = []
                if os.path.exists(symbol_dir):
                    available_files = sorted([f.replace('.ndjson', '') for f in os.listdir(symbol_dir) if f.endswith('.ndjson')], reverse=True)
            
            if available_files:
                export_date = st.selectbox("Select Date", available_files, key="export_date_select")
            else:
                export_date = None
                st.info("No export files found")
        
        # Handle "All Indices" view
        if export_date and export_symbol == "📦 All Indices":
            st.success(f"📦 All Indices export for {export_date}")
            
            # Collect data from all symbols
            all_snapshots = []
            symbol_counts = {}
            
            for sym in available_symbols:
                sym_path = os.path.join(export_base, sym, f"{export_date}.ndjson")
                if os.path.exists(sym_path):
                    count = 0
                    with open(sym_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    snap = json.loads(line)
                                    snap['_symbol'] = sym
                                    all_snapshots.append(snap)
                                    count += 1
                                except:
                                    pass
                    symbol_counts[sym] = count
            
            # Summary metrics
            if all_snapshots:
                sum_col1, sum_col2, sum_col3 = st.columns(3)
                with sum_col1:
                    st.metric("Total Snapshots", len(all_snapshots))
                with sum_col2:
                    st.metric("Symbols", ", ".join(symbol_counts.keys()))
                with sum_col3:
                    valid_count = sum(1 for s in all_snapshots if s.get('validation_is_valid', False))
                    st.metric("Valid", f"{valid_count}/{len(all_snapshots)}")
                
                # Breakdown by symbol
                st.caption(f"Per symbol: {', '.join(f'{k}: {v}' for k, v in symbol_counts.items())}")
                
                # Combined data table
                with st.expander("View All Snapshots", expanded=False):
                    display_data = []
                    for snap in all_snapshots:
                        snap_time = snap.get('generated_at_utc', '')
                        time_display = snap_time[11:19] if len(snap_time) > 19 else snap_time[:8] if snap_time else 'N/A'
                        gamma_pin_val = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike')
                        display_data.append({
                            'Symbol': snap.get('_symbol', ''),
                            'Time': time_display,
                            'Spot': f"${snap.get('spot_last', 0):,.2f}",
                            'Gamma Pin': f"${gamma_pin_val:,.0f}" if gamma_pin_val else 'N/A',
                            'Gross GEX': f"${snap.get('gross_gex', 0):.3f}B",
                            'Net GEX': f"${snap.get('net_gex', 0):.3f}B",
                            'Valid': '✅' if snap.get('validation_is_valid') else '❌'
                        })
                    st.dataframe(pd.DataFrame(display_data), hide_index=True, width='stretch')
                
                # Download ZIP with everything
                st.subheader("📥 Download All Data")
                try:
                    zip_data = create_eod_zip_export(export_date)
                    st.download_button(
                        "📦 Download Complete ZIP",
                        zip_data,
                        f"gamma_data_{export_date}.zip",
                        "application/zip",
                        type="primary"
                    )
                    st.caption("Includes: NDJSON files, pin history CSVs, combined all_indices.csv")
                except Exception as e:
                    st.error(f"Error creating ZIP: {str(e)[:50]}")
        
        elif export_date and symbol_dir:
            ndjson_path = os.path.join(symbol_dir, f"{export_date}.ndjson")
            
            if os.path.exists(ndjson_path):
                # Read and parse NDJSON
                snapshots = []
                with open(ndjson_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                snapshots.append(json.loads(line))
                            except:
                                pass
                
                st.success(f"Found {len(snapshots)} snapshots for {export_symbol} on {export_date}")
                
                # Summary metrics
                if snapshots:
                    sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
                    
                    first_snap = snapshots[0]
                    last_snap = snapshots[-1]
                    
                    with sum_col1:
                        # Use generated_at_utc field and extract time portion (HH:MM:SS)
                        first_time = first_snap.get('generated_at_utc', '')
                        if first_time:
                            # Format: extract time from ISO format (e.g., "2025-12-22T15:30:00Z" -> "15:30:00")
                            first_time_display = first_time[11:19] if len(first_time) > 19 else first_time[:8]
                        else:
                            first_time_display = 'N/A'
                        st.metric("First Snapshot", first_time_display)
                    with sum_col2:
                        last_time = last_snap.get('generated_at_utc', '')
                        if last_time:
                            last_time_display = last_time[11:19] if len(last_time) > 19 else last_time[:8]
                        else:
                            last_time_display = 'N/A'
                        st.metric("Last Snapshot", last_time_display)
                    with sum_col3:
                        valid_count = sum(1 for s in snapshots if s.get('validation_is_valid', False))
                        st.metric("Valid Snapshots", f"{valid_count}/{len(snapshots)}")
                    with sum_col4:
                        # Support both field names for backward compatibility
                        final_pin = last_snap.get('primary_gamma_pin_strike') or last_snap.get('gamma_pin_strike')
                        if final_pin:
                            st.metric("Final Pin", f"${final_pin:,.0f}")
                        else:
                            st.metric("Final Pin", "N/A")
                    
                    # Detailed view
                    with st.expander("View Snapshot Details", expanded=False):
                        display_data = []
                        for snap in snapshots:
                            # Extract time from generated_at_utc (ISO format)
                            snap_time = snap.get('generated_at_utc', '')
                            if snap_time:
                                time_display = snap_time[11:19] if len(snap_time) > 19 else snap_time[:8]
                            else:
                                time_display = 'N/A'
                            # Support both field names for backward compatibility
                            gamma_pin_val = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike')
                            display_data.append({
                                'Time': time_display,
                                'Spot': f"${snap.get('spot_last', 0):,.2f}",
                                'Gamma Pin': f"${gamma_pin_val:,.0f}" if gamma_pin_val else 'N/A',
                                'Gross GEX': f"${snap.get('gross_gex', 0):.3f}B",
                                'Net GEX': f"${snap.get('net_gex', 0):.3f}B",
                                'Valid': '✅' if snap.get('validation_is_valid') else '❌'
                            })
                        st.dataframe(pd.DataFrame(display_data), hide_index=True, width='stretch')
                    
                    # Download buttons row
                    with open(ndjson_path, 'r') as f:
                        file_content = f.read()
                    
                    dl_col1, dl_col2 = st.columns(2)
                    with dl_col1:
                        st.download_button(
                            label=f"📥 Download {export_symbol}_{export_date}.ndjson",
                            data=file_content,
                            file_name=f"{export_symbol}_{export_date}.ndjson",
                            mime="application/x-ndjson"
                        )
                    with dl_col2:
                        # Load actual training script from file
                        try:
                            with open('train_gamma_model.py', 'r') as f:
                                train_code = f.read()
                            st.download_button(
                                label="📜 Training Script",
                                data=train_code,
                                file_name="train_gamma_model.py",
                                mime="text/x-python"
                            )
                        except FileNotFoundError:
                            st.button("📜 Training Script (not found)", disabled=True)
                    
                    # Second row: Prediction script + Usage instructions
                    dl_col3, dl_col4 = st.columns(2)
                    with dl_col3:
                        try:
                            with open('predict_gamma_model.py', 'r') as f:
                                predict_code = f.read()
                            st.download_button(
                                label="🔮 Prediction Script",
                                data=predict_code,
                                file_name="predict_gamma_model.py",
                                mime="text/x-python"
                            )
                        except FileNotFoundError:
                            st.button("🔮 Prediction Script (not found)", disabled=True)
                    
                    with dl_col4:
                        with st.popover("📖 How to Use"):
                            st.markdown("""
**Step 1: Export unified dataset**
```bash
python export_all_indices.py
```
Creates `all_indices_YYYY-MM-DD.csv`

**Step 2: Train models (one per index)**
```bash
python train_gamma_model.py all_indices.csv SPX
python train_gamma_model.py all_indices.csv NDX
python train_gamma_model.py all_indices.csv RUT
```

**Step 3: Run predictions**
```bash
python predict_gamma_model.py all_indices.csv SPX
```

**Output files:**
- `gamma_model_SPX.pt` - Trained model
- `gamma_model_SPX_meta.json` - Features used
                            """)
                    
                    # CSV Export row
                    st.caption("CSV Exports")
                    csv_col1, csv_col2 = st.columns(2)
                    
                    with csv_col1:
                        # Convert current snapshots to CSV
                        csv_data = []
                        for snap in snapshots:
                            csv_data.append({
                                'symbol': export_symbol,
                                'date': export_date,
                                'timestamp_utc': snap.get('generated_at_utc', ''),
                                'spot': snap.get('spot_last', 0),
                                'gamma_pin': snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike'),
                                'gross_gex': snap.get('gross_gex', 0),
                                'net_gex': snap.get('net_gex', 0),
                                'call_gex': snap.get('call_gex', 0),
                                'put_gex': snap.get('put_gex', 0),
                                'max_pain': snap.get('max_pain_strike', None),
                                'is_valid': snap.get('validation_is_valid', False)
                            })
                        csv_df = pd.DataFrame(csv_data)
                        csv_content = csv_df.to_csv(index=False)
                        
                        st.download_button(
                            label=f"📊 Export {export_symbol} to CSV",
                            data=csv_content,
                            file_name=f"{export_symbol}_{export_date}.csv",
                            mime="text/csv"
                        )
                    
                    with csv_col2:
                        # Combine all symbols for selected date
                        all_symbols_data = []
                        for sym in available_symbols:
                            sym_file = os.path.join(export_base, sym, f"{export_date}.ndjson")
                            if os.path.exists(sym_file):
                                with open(sym_file, 'r') as f:
                                    for line in f:
                                        line = line.strip()
                                        if line:
                                            try:
                                                snap = json.loads(line)
                                                all_symbols_data.append({
                                                    'symbol': sym,
                                                    'date': export_date,
                                                    'timestamp_utc': snap.get('generated_at_utc', ''),
                                                    'spot': snap.get('spot_last', 0),
                                                    'gamma_pin': snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike'),
                                                    'gross_gex': snap.get('gross_gex', 0),
                                                    'net_gex': snap.get('net_gex', 0),
                                                    'call_gex': snap.get('call_gex', 0),
                                                    'put_gex': snap.get('put_gex', 0),
                                                    'max_pain': snap.get('max_pain_strike', None),
                                                    'is_valid': snap.get('validation_is_valid', False)
                                                })
                                            except:
                                                pass
                        
                        if all_symbols_data:
                            all_csv_df = pd.DataFrame(all_symbols_data)
                            all_csv_content = all_csv_df.to_csv(index=False)
                            
                            st.download_button(
                                label=f"📊 Export ALL Indices ({export_date})",
                                data=all_csv_content,
                                file_name=f"all_indices_{export_date}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.button("📊 No data for other indices", disabled=True)
    else:
        st.info("No snapshot exports available yet. Exports are created during market hours when the gamma scheduler runs.")
    
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
