"""
Streamlit UI for 0-day predictions - FastAPI client.
Polls /predict/close endpoint with adaptive refresh based on time-to-close.
"""
import streamlit as st
import requests
import plotly.graph_objects as go
from datetime import datetime
import time

# Page config
st.set_page_config(
    page_title="0-Day Index Predictor",
    page_icon="📈",
    layout="wide"
)

# API base URL
API_BASE = "http://localhost:8000"

# Symbols
SYMBOLS = {
    "S&P 500": "SPX",
    "NASDAQ 100": "NDX",
    "Dow Jones": "DJI",
    "Russell 2000": "RUT"
}

def get_health():
    """Check API health"""
    try:
        resp = requests.get(f"{API_BASE}/healthz", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def get_prediction(symbol: str):
    """Get 0-day prediction from API"""
    try:
        resp = requests.get(f"{API_BASE}/predict/close?symbol={symbol}", timeout=10)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            return {"error": "Rate limited - refresh too fast"}
        elif resp.status_code == 503:
            return {"error": "Stale data or market closed"}
        elif resp.status_code == 400:
            return {"error": "Market closed"}
        else:
            return {"error": f"API error: {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}

def get_gamma_levels(symbol: str):
    """Get gamma exposure levels"""
    try:
        resp = requests.get(f"{API_BASE}/levels/eod?symbol={symbol}", timeout=10)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def get_metrics():
    """Get performance metrics"""
    try:
        resp = requests.get(f"{API_BASE}/metrics", timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

# Title
st.title("📈 0-Day Index Price Predictor")
st.markdown("*Institutional-grade predictions with time-adaptive accuracy*")

# Check API key
import os
polygon_key = os.getenv("POLYGON_API_KEY", "")
if not polygon_key:
    st.error("⚠️ POLYGON_API_KEY not found in environment variables. Please add it to Replit Secrets.")
    st.stop()

# Sidebar
with st.sidebar:
    st.header("Settings")
    
    # Symbol selection
    selected_name = st.selectbox("Select Index", list(SYMBOLS.keys()))
    symbol = SYMBOLS[selected_name]
    
    st.divider()
    
    # Auto-refresh
    auto_refresh = st.checkbox("Auto-refresh", value=False)
    
    if auto_refresh:
        refresh_interval = st.slider("Refresh interval (seconds)", 5, 60, 15)
    
    st.divider()
    
    # API health
    st.subheader("System Health")
    health = get_health()
    
    if health:
        status_text = health.get('status', 'unknown')
        if status_text == "ok":
            st.success(f"✅ API: {status_text}")
        else:
            st.warning(f"⚠️ API: {status_text}")
        
        symbols_status = health.get("symbols", {})
        
        # Check if any data is available
        any_data = any(s.get("index_seconds", 0) > 0 for s in symbols_status.values())
        
        if not any_data:
            st.info("🔄 WebSocket connecting... Data will appear shortly.")
            st.caption("The system automatically connects to Polygon.io WebSocket during market hours.")
        
        for sym, status in symbols_status.items():
            fresh = status.get("fresh", False)
            data_secs = status.get("index_seconds", 0)
            
            if data_secs > 0:
                status_icon = "🟢" if fresh else "🟡"
                st.caption(f"{status_icon} {sym}: {data_secs}s data")
            else:
                st.caption(f"⚪ {sym}: waiting for data")
    else:
        st.error("❌ API unavailable")
    
    st.divider()
    
    # API Key Status
    st.subheader("Configuration")
    if polygon_key:
        masked_key = polygon_key[:8] + "..." + polygon_key[-4:] if len(polygon_key) > 12 else "***"
        st.success(f"🔑 API Key: {masked_key}")
    else:
        st.error("❌ No API key found")
    
    st.caption("WebSocket auto-connects during market hours (9:30 AM - 4:00 PM ET, Mon-Fri)")
    
    st.divider()
    
    # Performance metrics
    metrics = get_metrics()
    if metrics:
        st.subheader("Performance")
        
        latency = metrics.get("latency", {})
        st.caption(f"p99 latency: {latency.get('p99', 0):.1f}ms")
        
        memory = metrics.get("memory", {})
        st.caption(f"Memory: {memory.get('current_mb', 0):.1f}MB")

# Main content
col1, col2 = st.columns([2, 1])

with col1:
    st.header(f"{selected_name} Prediction")
    
    # Get prediction
    prediction = get_prediction(symbol)
    
    if "error" in prediction:
        st.error(prediction["error"])
    else:
        # Display prediction
        current = prediction.get("current_price", 0)
        predicted = prediction.get("predicted_close", 0)
        tau = prediction.get("tau_minutes", 0)
        confidence = prediction.get("confidence_level", "unknown")
        
        # Calculate change
        change = predicted - current
        change_pct = (change / current * 100) if current > 0 else 0
        
        # Metrics row
        met1, met2, met3, met4 = st.columns(4)
        
        with met1:
            st.metric("Current Price", f"${current:,.2f}")
        
        with met2:
            st.metric("Predicted Close", f"${predicted:,.2f}", 
                     f"{change:+.2f} ({change_pct:+.2f}%)")
        
        with met3:
            st.metric("Time to Close", f"{tau} min")
        
        with met4:
            conf_color = {
                "high": "🟢",
                "medium": "🟡",
                "low": "🔴",
                "unknown": "⚪"
            }.get(confidence, "⚪")
            
            st.metric("Confidence", f"{conf_color} {confidence.title()}")
        
        # Features
        st.subheader("Model Features")
        
        features = prediction.get("features", {})
        
        feat1, feat2, feat3, feat4 = st.columns(4)
        
        with feat1:
            vwap_dev = features.get("vwap_deviation", 0) * 100
            st.metric("VWAP Deviation", f"{vwap_dev:+.2f}%")
        
        with feat2:
            microtrend = features.get("microtrend", 0)
            st.metric("Microtrend", f"{microtrend:+.4f} $/s")
        
        with feat3:
            gamma_pin = features.get("gamma_pin_strength", 0)
            st.metric("Gamma Pin", f"{gamma_pin:.2%}")
        
        with feat4:
            flow_urgency = features.get("flow_urgency", 0)
            st.metric("Flow Urgency", f"{flow_urgency:.2%}")
        
        # RMSE info
        rmse = prediction.get("rmse")
        mae = prediction.get("mae")
        
        if rmse and mae:
            st.info(f"Expected error: ±${rmse:.2f} RMSE, ±${mae:.2f} MAE")

with col2:
    st.header("Gamma Levels")
    
    levels_data = get_gamma_levels(symbol)
    
    if levels_data:
        current_price = levels_data.get("current_price", 0)
        strongest_pin = levels_data.get("strongest_pin")
        levels = levels_data.get("levels", [])
        
        if strongest_pin:
            pin_dist = abs(current_price - strongest_pin)
            st.metric("Strongest Pin", f"${strongest_pin:,.0f}", 
                     f"{pin_dist:.2f} away")
        
        # Display top levels
        st.subheader("Nearest Levels")
        
        for level in levels[:5]:
            strike = level.get("strike", 0)
            gex = level.get("gamma_exposure", 0)
            dist = level.get("distance", 0)
            
            gex_icon = "🔴" if gex < 0 else "🟢"
            
            st.caption(f"{gex_icon} ${strike:,.0f} (${dist:.2f} away) - GEX: {gex:,.0f}")

# Auto-refresh logic
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.divider()
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
st.caption("Powered by FastAPI + Ridge Regression ML")
