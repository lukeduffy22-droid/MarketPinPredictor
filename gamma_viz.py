"""
Gamma Pin Evolution Visualization
Charts and tables showing intraday gamma pin movement
"""
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, date
import pandas as pd
from database import get_gamma_snapshots_for_day

def create_gamma_evolution_chart(ticker, trading_date_obj):
    """
    Create Plotly chart showing gamma pin evolution throughout the trading day
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date or datetime object for the trading day
    
    Returns:
        Plotly figure or None if no data
    """
    import pytz
    
    # Fetch gamma snapshots for the day
    snapshots = get_gamma_snapshots_for_day(ticker, trading_date_obj)
    
    if not snapshots or len(snapshots) == 0:
        return None
    
    # Convert to DataFrame for easier plotting
    et_tz = pytz.timezone('US/Eastern')
    data = []
    for snap in snapshots:
        # Convert UTC timestamp to ET for display
        time_et = snap.interval_timestamp.astimezone(et_tz)
        data.append({
            'time': time_et,
            'pin_strike': snap.pin_strike,
            'spot_price': snap.spot_price,
            'total_gex': snap.total_gex,
            'net_gex': snap.net_gex,
            'pull_strength': snap.pull_strength,
            'is_mock': snap.is_mock_data
        })
    
    df = pd.DataFrame(data)
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Separate real and simulated data for clear visualization
    real_data = df[df['is_mock'] == False]
    mock_data = df[df['is_mock'] == True]
    
    # Add REAL gamma pin line
    if len(real_data) > 0:
        fig.add_trace(go.Scatter(
            x=real_data['time'],
            y=real_data['pin_strike'],
            mode='lines+markers',
            name='Gamma Pin (Real)',
            line=dict(color='#FF6B6B', width=3),
            marker=dict(size=8, symbol='diamond'),
            hovertemplate='<b>✓ Real Pin:</b> $%{y:.2f}<br><b>Time:</b> %{x|%I:%M %p}<extra></extra>'
        ))
        
        # Add REAL spot price line
        fig.add_trace(go.Scatter(
            x=real_data['time'],
            y=real_data['spot_price'],
            mode='lines',
            name='Spot Price (Real)',
            line=dict(color='#4ECDC4', width=2),
            hovertemplate='<b>✓ Real Spot:</b> $%{y:.2f}<br><b>Time:</b> %{x|%I:%M %p}<extra></extra>'
        ))
    
    # Add SIMULATED data lines (if any) - clearly distinguished
    if len(mock_data) > 0:
        fig.add_trace(go.Scatter(
            x=mock_data['time'],
            y=mock_data['pin_strike'],
            mode='lines+markers',
            name='⚠️ Pin (SIMULATED)',
            line=dict(color='orange', width=2, dash='dash'),
            marker=dict(size=10, symbol='x', line=dict(width=2, color='white')),
            hovertemplate='<b>⚠️ SIMULATED Pin:</b> $%{y:.2f}<br><b>Time:</b> %{x|%I:%M %p}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=mock_data['time'],
            y=mock_data['spot_price'],
            mode='lines',
            name='⚠️ Spot (SIMULATED)',
            line=dict(color='gray', width=1, dash='dot'),
            hovertemplate='<b>⚠️ SIMULATED Spot:</b> $%{y:.2f}<br><b>Time:</b> %{x|%I:%M %p}<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title=f'{ticker} Gamma Pin Evolution - {trading_date_obj.strftime("%b %d, %Y")}',
        xaxis_title='Time (ET)',
        yaxis_title='Price ($)',
        hovermode='x unified',
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(128, 128, 128, 0.2)',
            tickformat='%I:%M %p'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(128, 128, 128, 0.2)'
        ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )
    
    return fig

def display_gamma_history_table(ticker, trading_date_obj):
    """
    Display table of gamma pin snapshots throughout the day
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date or datetime object for the trading day
    """
    import pytz
    
    # Fetch gamma snapshots for the day
    snapshots = get_gamma_snapshots_for_day(ticker, trading_date_obj)
    
    if not snapshots or len(snapshots) == 0:
        st.info("No gamma pin history available for today yet. The system samples every 15 minutes during market hours (9:30 AM - 4:00 PM ET).")
        return
    
    # Build table data for DISPLAY (formatted)
    et_tz = pytz.timezone('US/Eastern')
    table_data = []
    for snap in snapshots:
        # Convert UTC timestamp to ET for display
        time_et = snap.interval_timestamp.astimezone(et_tz)
        # Calculate pin movement from previous snapshot
        row = {
            'Time': time_et.strftime('%I:%M %p ET'),
            'Pin Strike': f"${snap.pin_strike:.2f}",
            'Spot Price': f"${snap.spot_price:.2f}",
            'Distance': f"{((snap.pin_strike - snap.spot_price) / snap.spot_price * 100):+.2f}%",
            'Pull Strength': f"{snap.pull_strength:.2f}",
            'Total GEX': f"${snap.total_gex:.2f}B",
            'Net GEX': f"${snap.net_gex:.2f}B",
            'Data Source': '⚠️ Simulated' if snap.is_mock_data else '✓ Real'
        }
        table_data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(table_data)
    
    # Display table
    st.dataframe(
        df,
        width='stretch',
        hide_index=True,
        height=min(400, len(df) * 35 + 38)  # Auto-height based on rows
    )
    
    # Build EXPORT data with raw values and additional columns
    export_data = []
    prev_pin = None
    prev_time = None
    
    for snap in snapshots:
        time_et = snap.interval_timestamp.astimezone(et_tz)
        
        # Calculate distance in points and percentage
        distance_points = snap.pin_strike - snap.spot_price
        distance_pct = (distance_points / snap.spot_price) * 100
        
        # Calculate pin drift rate (change in pin per hour)
        pin_drift_per_hour = 0.0
        if prev_pin is not None and prev_time is not None:
            time_diff_hours = (time_et - prev_time).total_seconds() / 3600
            if time_diff_hours > 0:
                pin_drift_per_hour = (snap.pin_strike - prev_pin) / time_diff_hours
        
        # Intraday range (high-low estimate based on distance from pin)
        # This is an approximation - actual high/low would need additional data
        spot_range_pct = abs(distance_pct) * 2  # Rough estimate
        
        export_row = {
            'IndexSymbol': ticker,
            'SessionDate': trading_date_obj.isoformat() if hasattr(trading_date_obj, 'isoformat') else str(trading_date_obj),
            'Time_ET': time_et.strftime('%Y-%m-%d %H:%M:%S'),
            'Time_Display': time_et.strftime('%I:%M %p ET'),
            'Pin_Strike': snap.pin_strike,
            'Spot_Price': snap.spot_price,
            'Distance_Points': distance_points,
            'Distance_Pct': distance_pct,
            'Pull_Strength': snap.pull_strength,
            'Total_GEX_Billions': snap.total_gex,
            'Net_GEX_Billions': snap.net_gex,
            'Pin_Drift_Per_Hour': pin_drift_per_hour,
            'Spot_Range_Pct_Est': spot_range_pct,
            'Data_Source': 'Simulated' if snap.is_mock_data else 'Real'
        }
        export_data.append(export_row)
        
        prev_pin = snap.pin_strike
        prev_time = time_et
    
    # Create export DataFrame
    export_df = pd.DataFrame(export_data)
    
    # Add CSV download button
    csv_data = export_df.to_csv(index=False)
    st.download_button(
        label=f"📥 Export {ticker} Gamma Data (CSV)",
        data=csv_data,
        file_name=f"gamma_data_{ticker}_{trading_date_obj.isoformat() if hasattr(trading_date_obj, 'isoformat') else str(trading_date_obj)}.csv",
        mime="text/csv",
        help="Download gamma pin data with raw values for analysis"
    )
    
    # Show summary stats
    real_snapshots = [s for s in snapshots if not s.is_mock_data]
    if len(real_snapshots) > 0:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_pin = sum(s.pin_strike for s in snapshots) / len(snapshots)
            st.metric("Avg Pin", f"${avg_pin:.2f}")
        
        with col2:
            pin_range = max(s.pin_strike for s in snapshots) - min(s.pin_strike for s in snapshots)
            st.metric("Pin Range", f"${pin_range:.2f}")
        
        with col3:
            avg_pull = sum(s.pull_strength for s in snapshots) / len(snapshots)
            st.metric("Avg Pull", f"{avg_pull:.2f}")
        
        with col4:
            real_pct = (len(real_snapshots) / len(snapshots)) * 100
            st.metric("Real Data", f"{real_pct:.0f}%")

def show_gamma_evolution_section(ticker, index_name):
    """
    Complete gamma evolution section with chart and table
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        index_name: Display name (e.g., 'S&P 500 (SPX)')
    """
    import pytz
    
    st.subheader(f"📊 {index_name} - Intraday Gamma Pin Evolution")
    
    # Use today's date in ET timezone
    et_tz = pytz.timezone('US/Eastern')
    today_et = datetime.now(et_tz).date()
    
    # Create and display chart
    fig = create_gamma_evolution_chart(ticker, today_et)
    
    if fig:
        # Check if any data is simulated and show prominent warning
        from database import get_gamma_snapshots_for_day
        snapshots = get_gamma_snapshots_for_day(ticker, today_et)
        has_mock_data = any(s.is_mock_data for s in snapshots)
        
        if has_mock_data:
            st.warning(
                "⚠️ **SOME DATA IS SIMULATED** - Parts of this chart use estimated gamma exposure. "
                "Real data from your Polygon API is clearly marked. Simulated sections shown with dashed lines."
            )
        
        st.plotly_chart(fig, use_container_width=True, key=f"gamma_evolution_{ticker}")
        
        # Show table in expander
        with st.expander("📋 View Detailed Snapshots", expanded=False):
            display_gamma_history_table(ticker, today_et)
    else:
        st.info(f"""
        **No gamma pin history available yet for {ticker}.**
        
        The system automatically samples gamma exposure every 15 minutes during market hours (9:30 AM - 4:00 PM ET).
        
        Next sample at: **{get_next_sample_time()}**
        
        Check back during market hours to see the evolution chart.
        """)

def get_next_sample_time():
    """Get the next scheduled gamma sample time"""
    import pytz
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Find next 15-minute boundary
    current_minute = now_et.minute
    minutes_until_next = 15 - (current_minute % 15)
    next_sample = now_et + pd.Timedelta(minutes=minutes_until_next)
    
    return next_sample.strftime('%I:%M %p ET')
