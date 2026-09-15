"""
Gamma Pin Evolution Visualization
Charts and tables showing intraday gamma pin movement
"""
import streamlit as st
import plotly.graph_objects as go
from datetime import datetime, date
import pandas as pd
from database import get_gamma_snapshots_for_day
from app.utils.display_time import (
    DisplayTimezone,
    format_display_timestamp,
    parse_utc_timestamp,
    resolve_display_timezone,
)
from app.utils.time_et import ET


def _distance_pct(pin_strike, spot_price):
    """Return pin distance as a percentage, or None for an unusable spot price."""
    try:
        spot = float(spot_price)
        pin = float(pin_strike)
    except (TypeError, ValueError):
        return None
    if spot == 0:
        return None
    return (pin - spot) / spot * 100


PERSISTED_VALID = "persisted_valid"
LEGACY_UNVERIFIED = "legacy_unverified"
SIMULATED = "simulated"
INVALID = "invalid"


def _explicit_validation_value(snapshot):
    """Return an explicit stored validation bool, never a guessed default."""
    for field_name in ("validation_is_valid", "is_valid"):
        if hasattr(snapshot, field_name):
            value = getattr(snapshot, field_name)
            if value is True:
                return True
            if value is False:
                return False
    return None


def _snapshot_provenance(snapshot):
    """Classify a snapshot without promoting missing legacy metadata."""
    validation = _explicit_validation_value(snapshot)
    if validation is False:
        return INVALID

    is_mock_data = getattr(snapshot, "is_mock_data", None)
    if is_mock_data is True:
        return SIMULATED
    if validation is True and is_mock_data is False:
        return PERSISTED_VALID
    return LEGACY_UNVERIFIED


def _displayable_snapshots(snapshots):
    """Keep usable history while excluding explicit invalid placeholders."""
    displayable = []
    for snapshot in snapshots:
        if _snapshot_provenance(snapshot) == INVALID:
            continue
        try:
            has_prices = (
                float(snapshot.spot_price or 0) > 0
                and float(snapshot.pin_strike or 0) > 0
            )
        except (TypeError, ValueError):
            has_prices = False
        if has_prices:
            displayable.append(snapshot)
    return displayable


def _history_as_of_caption(snapshots, viewer_timezone: DisplayTimezone):
    """Describe the persisted-history boundary and its latest usable instant."""
    timestamps = [
        parsed
        for snapshot in snapshots
        if (parsed := parse_utc_timestamp(snapshot.interval_timestamp)) is not None
    ]
    if not timestamps:
        return (
            "Captured history only; no parseable persisted observation timestamp is "
            "available. This panel does not establish current live state."
        )

    last_observation = format_display_timestamp(
        max(timestamps),
        viewer_timezone,
        format_string="%b %d, %Y at %I:%M:%S %p %Z",
    )
    provenance = {_snapshot_provenance(snapshot) for snapshot in snapshots}
    if LEGACY_UNVERIFIED in provenance:
        provenance_note = (
            "Legacy rows without explicit validity provenance are labeled unverified."
        )
    elif SIMULATED in provenance:
        provenance_note = "Simulated rows are labeled separately."
    else:
        provenance_note = "Displayed rows carry explicit persisted-valid provenance."

    return (
        "Captured history only; it can remain visible when current live state is "
        f"unavailable. Last persisted observation: {last_observation}. "
        f"{provenance_note}"
    )

def create_gamma_evolution_chart(
    ticker,
    trading_date_obj,
    display_timezone: DisplayTimezone | None = None,
    snapshots=None,
):
    """
    Create Plotly chart showing gamma pin evolution throughout the trading day
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date or datetime object for the trading day
    
    Returns:
        Plotly figure or None if no data
    """
    viewer_timezone = display_timezone or resolve_display_timezone(None)

    # Fetch gamma snapshots for the day. A supplied collection lets the section
    # render its chart, warnings, caption, and table from one consistent read.
    if snapshots is None:
        snapshots = get_gamma_snapshots_for_day(ticker, trading_date_obj)
    snapshots = _displayable_snapshots(snapshots)
    
    if not snapshots or len(snapshots) == 0:
        return None
    
    # Convert persisted UTC instants to the viewer's timezone for presentation.
    data = []
    for snap in snapshots:
        time_utc = parse_utc_timestamp(snap.interval_timestamp)
        if time_utc is None:
            continue
        data.append({
            'time': time_utc.astimezone(viewer_timezone.tzinfo),
            'pin_strike': snap.pin_strike,
            'spot_price': snap.spot_price,
            'total_gex': snap.total_gex,
            'net_gex': snap.net_gex,
            'pull_strength': snap.pull_strength,
            'provenance': _snapshot_provenance(snap),
        })

    if not data:
        return None
    
    df = pd.DataFrame(data)
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Separate explicit provenance classes instead of treating "not mock" as
    # proof that a legacy row was validated or is currently live.
    valid_data = df[df['provenance'] == PERSISTED_VALID]
    legacy_data = df[df['provenance'] == LEGACY_UNVERIFIED]
    mock_data = df[df['provenance'] == SIMULATED]
    
    if len(valid_data) > 0:
        fig.add_trace(go.Scatter(
            x=valid_data['time'],
            y=valid_data['pin_strike'],
            mode='lines+markers',
            name='Gamma Pin — Persisted valid snapshot',
            line=dict(color='#FF6B6B', width=3),
            marker=dict(size=8, symbol='diamond'),
            hovertemplate='<b>Persisted valid pin snapshot:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))

        fig.add_trace(go.Scatter(
            x=valid_data['time'],
            y=valid_data['spot_price'],
            mode='lines',
            name='Spot — Persisted valid snapshot',
            line=dict(color='#4ECDC4', width=2),
            hovertemplate='<b>Persisted valid spot snapshot:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))

    if len(legacy_data) > 0:
        fig.add_trace(go.Scatter(
            x=legacy_data['time'],
            y=legacy_data['pin_strike'],
            mode='lines+markers',
            name='Gamma Pin — Legacy unverified history',
            line=dict(color='#FFB347', width=2, dash='dash'),
            marker=dict(size=8, symbol='diamond-open'),
            hovertemplate='<b>Legacy unverified pin:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))

        fig.add_trace(go.Scatter(
            x=legacy_data['time'],
            y=legacy_data['spot_price'],
            mode='lines',
            name='Spot — Legacy unverified history',
            line=dict(color='#9AA0A6', width=2, dash='dot'),
            hovertemplate='<b>Legacy unverified spot:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))

    if len(mock_data) > 0:
        fig.add_trace(go.Scatter(
            x=mock_data['time'],
            y=mock_data['pin_strike'],
            mode='lines+markers',
            name='⚠️ Gamma Pin — Simulated history',
            line=dict(color='orange', width=2, dash='dash'),
            marker=dict(size=10, symbol='x', line=dict(width=2, color='white')),
            hovertemplate='<b>⚠️ Simulated pin:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))
        
        fig.add_trace(go.Scatter(
            x=mock_data['time'],
            y=mock_data['spot_price'],
            mode='lines',
            name='⚠️ Spot — Simulated history',
            line=dict(color='gray', width=1, dash='dot'),
            hovertemplate='<b>⚠️ Simulated spot:</b> $%{y:.2f}<br><b>Captured:</b> %{x|%I:%M %p}<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title=f'{ticker} Gamma Pin Captured History - {trading_date_obj.strftime("%b %d, %Y")}',
        xaxis_title=f'Time ({viewer_timezone.name})',
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

def display_gamma_history_table(
    ticker,
    trading_date_obj,
    display_timezone: DisplayTimezone | None = None,
    snapshots=None,
):
    """
    Display table of gamma pin snapshots throughout the day
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        trading_date_obj: datetime.date or datetime object for the trading day
    """
    viewer_timezone = display_timezone or resolve_display_timezone(None)

    if snapshots is None:
        snapshots = get_gamma_snapshots_for_day(ticker, trading_date_obj)
    snapshots = _displayable_snapshots(snapshots)
    
    if not snapshots or len(snapshots) == 0:
        st.info("No gamma pin history available for today yet. The system samples every 15 minutes during market hours (9:30 AM - 4:00 PM ET).")
        return
    
    # Build table data for DISPLAY (formatted)
    table_data = []
    for snap in snapshots:
        distance_pct = _distance_pct(snap.pin_strike, snap.spot_price)
        # Calculate pin movement from previous snapshot
        row = {
            'Time': format_display_timestamp(
                snap.interval_timestamp,
                viewer_timezone,
                format_string='%I:%M %p %Z',
            ),
            'Pin Strike': f"${snap.pin_strike:.2f}",
            'Spot Price': f"${snap.spot_price:.2f}",
            'Distance': f"{distance_pct:+.2f}%" if distance_pct is not None else "N/A",
            'Pull Strength': f"{snap.pull_strength:.2f}",
            'Total GEX': f"${snap.total_gex:.2f}B",
            'Net GEX': f"${snap.net_gex:.2f}B",
            'Snapshot Provenance': {
                PERSISTED_VALID: '✓ Persisted valid snapshot',
                LEGACY_UNVERIFIED: '⚠️ Legacy persisted — validity unverified',
                SIMULATED: '⚠️ Simulated history',
            }[_snapshot_provenance(snap)],
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
        time_utc = parse_utc_timestamp(snap.interval_timestamp)
        if time_utc is None:
            continue
        time_local = time_utc.astimezone(viewer_timezone.tzinfo)
        time_et = time_utc.astimezone(ET)
        
        # Calculate distance in points and percentage
        distance_points = snap.pin_strike - snap.spot_price
        distance_pct = _distance_pct(snap.pin_strike, snap.spot_price)
        
        # Calculate pin drift rate (change in pin per hour)
        pin_drift_per_hour = 0.0
        if prev_pin is not None and prev_time is not None:
            time_diff_hours = (time_utc - prev_time).total_seconds() / 3600
            if time_diff_hours > 0:
                pin_drift_per_hour = (snap.pin_strike - prev_pin) / time_diff_hours
        
        # Intraday range (high-low estimate based on distance from pin)
        # This is an approximation - actual high/low would need additional data
        spot_range_pct = abs(distance_pct) * 2 if distance_pct is not None else None  # Rough estimate
        
        export_row = {
            'IndexSymbol': ticker,
            'SessionDate': trading_date_obj.isoformat() if hasattr(trading_date_obj, 'isoformat') else str(trading_date_obj),
            'Timestamp_UTC': time_utc.isoformat().replace('+00:00', 'Z'),
            'Timestamp_Local': time_local.isoformat(),
            'Display_Timezone': viewer_timezone.name,
            'Time_ET': time_et.strftime('%Y-%m-%d %H:%M:%S'),
            'Time_Display': time_local.strftime('%I:%M %p %Z'),
            'Pin_Strike': snap.pin_strike,
            'Spot_Price': snap.spot_price,
            'Distance_Points': distance_points,
            'Distance_Pct': distance_pct,
            'Pull_Strength': snap.pull_strength,
            'Total_GEX_Billions': snap.total_gex,
            'Net_GEX_Billions': snap.net_gex,
            'Pin_Drift_Per_Hour': pin_drift_per_hour,
            'Spot_Range_Pct_Est': spot_range_pct,
            # Retain the legacy export column while removing the unsupported
            # implication that any persisted non-mock row is automatically real.
            'Data_Source': (
                'Simulated history'
                if _snapshot_provenance(snap) == SIMULATED
                else 'Persisted gamma history'
            ),
            'Snapshot_Provenance': {
                PERSISTED_VALID: 'Persisted valid snapshot',
                LEGACY_UNVERIFIED: 'Legacy persisted - validity unverified',
                SIMULATED: 'Simulated history',
            }[_snapshot_provenance(snap)],
        }
        export_data.append(export_row)
        
        prev_pin = snap.pin_strike
        prev_time = time_utc
    
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
    verified_snapshots = [
        snapshot
        for snapshot in snapshots
        if _snapshot_provenance(snapshot) == PERSISTED_VALID
    ]
    if verified_snapshots:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_pin = sum(s.pin_strike for s in verified_snapshots) / len(verified_snapshots)
            st.metric("Avg Pin", f"${avg_pin:.2f}")
        
        with col2:
            pin_range = max(s.pin_strike for s in verified_snapshots) - min(s.pin_strike for s in verified_snapshots)
            st.metric("Pin Range", f"${pin_range:.2f}")
        
        with col3:
            avg_pull = sum(s.pull_strength for s in verified_snapshots) / len(verified_snapshots)
            st.metric("Avg Pull", f"{avg_pull:.2f}")
        
        with col4:
            st.metric("Verified Rows", f"{len(verified_snapshots)}/{len(snapshots)}")
    else:
        st.info(
            "Summary metrics are unavailable because no displayed row carries "
            "explicit persisted-valid provenance."
        )

def show_gamma_evolution_section(
    ticker,
    index_name,
    unique_suffix="",
    display_timezone: DisplayTimezone | None = None,
):
    """
    Complete gamma evolution section with chart and table
    
    Args:
        ticker: Stock ticker (e.g., 'SPX')
        index_name: Display name (e.g., 'S&P 500 (SPX)')
        unique_suffix: Optional unique suffix for chart keys to prevent duplicates
    """
    import hashlib
    viewer_timezone = display_timezone or resolve_display_timezone(None)
    
    st.subheader(f"📊 {index_name} - Captured Gamma Pin History")
    
    # Use today's date in ET timezone
    today_et = datetime.now(ET).date()
    
    # Use one persisted read for every element in this section so its chart,
    # labels, timestamp, warnings, and table describe the same evidence.
    snapshots = get_gamma_snapshots_for_day(ticker, today_et)
    displayable_snapshots = _displayable_snapshots(snapshots)
    fig = create_gamma_evolution_chart(
        ticker,
        today_et,
        viewer_timezone,
        snapshots=displayable_snapshots,
    )
    
    if fig:
        provenance = {
            _snapshot_provenance(snapshot) for snapshot in displayable_snapshots
        }
        has_mock_data = SIMULATED in provenance
        has_legacy_unverified = LEGACY_UNVERIFIED in provenance
        
        if has_mock_data:
            st.warning(
                "⚠️ **SOME DATA IS SIMULATED** - Parts of this chart use estimated gamma exposure. "
                "Simulated history is labeled separately and does not establish current live state."
            )

        if has_legacy_unverified:
            st.warning(
                "⚠️ **LEGACY VALIDITY IS UNVERIFIED** - Some persisted rows do not "
                "carry an explicit validity marker. They remain visible as captured history "
                "but are excluded from verified summary metrics."
            )
        
        # Generate unique key using ticker, date, and optional suffix
        key_base = f"gamma_evolution_{ticker}_{today_et.isoformat()}_{unique_suffix}"
        chart_key = hashlib.md5(key_base.encode()).hexdigest()[:12]
        st.plotly_chart(fig, use_container_width=True, key=f"gamma_evo_{ticker}_{chart_key}")
        st.caption(_history_as_of_caption(displayable_snapshots, viewer_timezone))
        st.caption(
            f"Times use the dashboard display timezone: {viewer_timezone.name}. "
            "Market-session boundaries remain Eastern Time."
        )
        
        # Show table in expander
        with st.expander("📋 View Detailed Snapshots", expanded=False):
            display_gamma_history_table(
                ticker,
                today_et,
                viewer_timezone,
                snapshots=displayable_snapshots,
            )
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
