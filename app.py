import streamlit as st
from app.services.forecast_research_view import render_forecast_research
import pandas as pd
import numpy as np
import json
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from polygon import RESTClient
from datetime import datetime, timedelta
import time
import requests
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from typing import Any, Optional
from database import save_prediction, get_predictions_by_ticker, get_all_predictions, save_alert, get_active_alerts, get_prediction_accuracy_stats
from models import train_linear_regression, train_random_forest
from options_gamma import get_gamma_analysis
from backtesting import run_backtest, calculate_backtest_metrics, optimize_model_features
# BACKEND-ONLY WEBSOCKET: Only REST-based helpers are imported here
# WebSocket creation functions are NOT imported - all WebSocket is backend-only
from websocket_streaming import get_snapshot_data
from ai_analysis import (
    analyze_prediction, analyze_streaming_data,
    get_risk_assessment, explain_gamma_exposure
)
from gamma_viz import show_gamma_evolution_section
from app.core.audit_persistence import dict_to_audit_snapshot, load_last_valid_snapshot, load_latest_snapshot_data
from app.services.live_data_client import (
    build_databento_predictions_from_state_batch,
    ZERO_GAMMA_DISPLAY_LABEL,
    fetch_backend_market_session_status,
    fetch_databento_prediction as fetch_databento_prediction_live,
    fetch_promoted_closing_tape_predictions,
)
from app.services.dashboard_status import (
    diagnostic_state_label, near_close_caption, near_close_seconds,
)
from app.services.live_advisor import fetch_advisor_context, build_advisor_report
from app.services.sidebar_live_state import (
    fetch_sidebar_symbol_states,
    retained_prediction_matches_state,
    summarize_cache_reload,
)
from app.services.live_panel_view import (
    render_backend_streaming_status,
    render_closing_tape_evidence,
)
from app.services.shadow_research_view import render_shadow_research_panel
from app.services.orb_view import (
    initialize_live_autorefresh,
    render_opening_range_panel,
)
from app.services.retained_parity_view import (
    render_retained_opra_parity_history,
    symbols_requiring_retained_context,
)
from app.services.databento_symbol_catalog import (
    CANARY_CANDIDATE_SYMBOLS,
    active_canary_symbols,
    fetch_databento_universe,
    selectable_databento_symbols,
)
from app.utils.market_time import (
    get_freeze_status,
    market_is_closed,
)
from app.utils.time_et import utc_iso
from app.utils.display_time import (
    DisplayTimezone,
    display_timezone_label,
    format_display_timestamp,
    parse_utc_timestamp,
    resolve_context_display_timezone,
    resolve_display_timezone,
)
from app.utils.export_catalog import list_gamma_snapshot_symbols
from app.utils.gamma_wall_evidence import normalize_gamma_walls
from app.utils.snapshot_history import (
    DIAGNOSTIC_INVALID_SNAPSHOT,
    SnapshotSelection,
    available_local_dates_for_symbol,
    current_local_date,
    gamma_snapshot_provenance_status,
    load_snapshots_for_local_day,
    partition_snapshot_evidence,
    snapshot_coverage_manifest,
    snapshot_research_export_record,
    snapshot_research_export_fields,
)
from app.utils.opra_parity_history import (
    build_opra_parity_gamma_history,
)
from backend.workstation import payload_has_fallback_provenance

# The backend lifecycle owns the canonical market-data schema. Importing the
# legacy Polygon UI helpers must not create their ORM tables in that database
# during every Streamlit startup.

# Page configuration
st.set_page_config(
    page_title="Stock Index Price Predictor",
    page_icon="📈",
    layout="wide"
)


def _dashboard_display_timezone() -> DisplayTimezone:
    """Resolve the viewer's browser timezone on every Streamlit rerun."""

    return resolve_context_display_timezone(getattr(st, "context", None))


DISPLAY_TIMEZONE = _dashboard_display_timezone()


def _current_snapshot_display_date() -> str:
    """Return the viewer-local calendar date used by snapshot history controls."""

    return current_local_date(DISPLAY_TIMEZONE)


def _display_clock(value: Any | None = None) -> str:
    """Format an explicit UTC instant as a short viewer-local clock."""

    return format_display_timestamp(
        value or utc_iso(),
        DISPLAY_TIMEZONE,
        format_string='%H:%M:%S %Z',
    )


def _snapshot_timestamp_value(snapshot: dict[str, Any]) -> Any:
    """Prefer generated_at_utc, but recover from its malformed legacy values."""

    generated = snapshot.get('generated_at_utc')
    if parse_utc_timestamp(generated) is not None:
        return generated
    return snapshot.get('timestamp_utc') or generated or ''


@st.cache_data(ttl=5, show_spinner=False)
def _cached_local_snapshot_selection_payload(
    exports_root: str,
    symbol: str,
    local_date: str,
    timezone_name: str,
) -> tuple[tuple[dict[str, Any], ...], tuple[str, ...], int]:
    """Cache only reload-stable builtins while active NDJSON files append."""

    display_timezone = resolve_display_timezone(timezone_name)
    selection = load_snapshots_for_local_day(
        exports_root,
        symbol,
        local_date,
        display_timezone,
    )
    # Streamlit can reload imported modules after a source-file change while an
    # older class object is still referenced by the running script. Pickling
    # that dataclass instance then fails because its module now exposes a newer
    # SnapshotSelection class. The records originate from JSON, so keep the
    # cached boundary entirely in built-in, reload-stable types.
    return (
        selection.records,
        selection.source_files,
        selection.malformed_timestamp_records,
    )


def _cached_local_snapshot_selection(
    exports_root: str,
    symbol: str,
    local_date: str,
    timezone_name: str,
) -> SnapshotSelection:
    """Rehydrate the current SnapshotSelection class outside Streamlit's cache."""

    records, source_files, malformed_timestamp_records = (
        _cached_local_snapshot_selection_payload(
            exports_root,
            symbol,
            local_date,
            timezone_name,
        )
    )
    return SnapshotSelection(
        records=records,
        source_files=source_files,
        malformed_timestamp_records=malformed_timestamp_records,
    )


@st.cache_data(ttl=5, show_spinner=False)
def _cached_available_snapshot_dates(
    exports_root: str,
    symbol: str,
    timezone_name: str,
) -> list[str]:
    display_timezone = resolve_display_timezone(timezone_name)
    return available_local_dates_for_symbol(
        exports_root,
        symbol,
        display_timezone,
    )

# Major stock indexes - Using actual index tickers
# NOTE: DJI uses DIA ETF options (no direct index options exist)
INDEXES = {
    "S&P 500 (SPX)": "SPX",
    "NASDAQ 100 (NDX)": "NDX",
    "Dow Jones (DJI)": "DJI",
    "Russell 2000 (RUT)": "RUT",
    "S&P 400 Mid-Cap (MID)": "MID",
    "S&P 600 Small-Cap (SML)": "SML",
    "NASDAQ Composite (COMP)": "COMP",
    "NYSE Composite (NYA)": "NYA",
    "Russell 1000 (RUI)": "RUI",
    "Russell 3000 (RUA)": "RUA",
    "S&P 100 (OEX)": "OEX",
    "Dow Jones Mini (DJX)": "DJX",
    "PHLX Semiconductor (SOX)": "SOX",
    "PHLX Gold/Silver (XAU)": "XAU",
    "PHLX Housing (HGX)": "HGX",
    "PHLX Oil Service (OSX)": "OSX",
    "PHLX Utility (UTY)": "UTY",
    "VIX (Volatility)": "VIX"
}

DATABENTO_INDEXES = {
    "S&P 500 (SPX)": "SPX",
    "NASDAQ 100 (NDX)": "NDX",
    "SPDR S&P 500 ETF (SPY)": "SPY",
    "Invesco QQQ ETF (QQQ)": "QQQ",
    "SPDR Dow Jones ETF (DIA)": "DIA",
    "iShares Russell 2000 ETF (IWM)": "IWM",
    "Mini S&P 500 (XSP)": "XSP",
    "Mini NASDAQ 100 (XND)": "XND",
    "Russell 2000 (RUT)": "RUT",
    "Mini Russell 2000 (MRUT)": "MRUT",
    "S&P 100 (OEX)": "OEX",
    "Dow Jones Mini (DJX)": "DJX",
    "Russell 1000 (RUI)": "RUI",
    "PHLX Gold/Silver (XAU)": "XAU",
    "PHLX Housing (HGX)": "HGX",
    "PHLX Oil Service (OSX)": "OSX",
    "PHLX Utility (UTY)": "UTY",
    "VIX (Volatility)": "VIX",
}

# Every Databento selector label must resolve through the shared registry used
# by the live cards, debug view, advisor, exports, and analysis paths.  Updating
# the registry does not widen the OPRA subscription; the backend-authoritative
# canary gate below still controls which labels are actually selectable.
INDEXES.update(DATABENTO_INDEXES)

INDEX_POLYGON_TICKERS = {
    "SPX": "I:SPX", "NDX": "I:NDX", "DJI": "I:DJI", "RUT": "I:RUT",
    "SPY": "SPY", "QQQ": "QQQ",
    "MID": "I:MID", "SML": "I:SML", "COMP": "I:COMP", "NYA": "I:NYA",
    "RUI": "I:RUI", "RUA": "I:RUA", "OEX": "I:OEX", "DJX": "I:DJX",
    "SOX": "I:SOX", "XAU": "I:XAU", "HGX": "I:HGX", "OSX": "I:OSX",
    "UTY": "I:UTY", "VIX": "I:VIX"
}

INDEX_ETFS = {
    "SPX": "SPY", "NDX": "QQQ", "DJI": "DIA", "RUT": "IWM",
    "SPY": "SPY", "QQQ": "QQQ",
    "MID": "MDY", "SML": "IJR", "COMP": "QQQ", "NYA": "VTI",
    "RUI": "IWF", "RUA": "IWV", "OEX": "OEF", "DJX": "DIA",
    "SOX": "SOXX", "XAU": "GDX", "HGX": "XHB", "OSX": "XLE",
    "UTY": "XLU", "VIX": "UVXY"
}

DATABENTO_SUPPORTED_INDEXES = set(DATABENTO_INDEXES)

DEFAULT_POLYGON_INDEX_SELECTION = ["S&P 500 (SPX)", "NASDAQ 100 (NDX)"]
DEFAULT_DATABENTO_INDEX_SELECTION = ["S&P 500 (SPX)", "NASDAQ 100 (NDX)", "VIX (Volatility)"]
ETF_TICKER_SEARCH = {"SPY","QQQ","DIA","IWM","VOO","VTI","RSP","MDY","XLK","XLF","XLY","XLP","XLE","XLI","XLV","XLU","SMH","TLT","IEF","HYG","LQD"}
PERSISTED_PROVIDERS = {"databento"}


def _fmt_gex_units(value: Any, decimals: int = 2) -> str:
    """Format canonical gamma×OI×100 exposure without inventing dollar units."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "N/A"
    magnitude = abs(number)
    if magnitude >= 1_000_000_000:
        scaled, suffix = number / 1_000_000_000, "B"
    elif magnitude >= 1_000_000:
        scaled, suffix = number / 1_000_000, "M"
    elif magnitude >= 1_000:
        scaled, suffix = number / 1_000, "K"
    else:
        scaled, suffix = number, ""
    return f"{scaled:,.{decimals}f}{suffix} raw units"

def _agg_get(agg: Any, attr: str, dict_key: Optional[str] = None, default: Any = None) -> Any:
    """Read a Polygon aggregate value from either an Agg object or dict-like payload."""
    if isinstance(agg, dict):
        return agg.get(dict_key or attr, default)
    return getattr(agg, attr, default)


def _agg_timestamp_ms(agg: Any) -> Optional[int]:
    """Read an aggregate timestamp in milliseconds when present."""
    timestamp = _agg_get(agg, "timestamp", "t")
    if timestamp is None:
        return None
    try:
        return int(timestamp)
    except (TypeError, ValueError):
        return None


def _contract_get(contract: Any, attr: str, default: Any = None) -> Any:
    """Read an options contract field from a response object or dict-like payload."""
    if isinstance(contract, dict):
        return contract.get(attr, default)
    return getattr(contract, attr, default)

import os
from pathlib import Path

# Initialize session state - load API key from environment if available
def _ui_preferences_path() -> Path:
    raw_path = os.environ.get("MARKETPIN_UI_PREFS_PATH", "").strip()
    if raw_path:
        return Path(raw_path).expanduser()
    return Path.home() / ".marketpinpredictor" / "ui_preferences.json"


def _default_indexes_for_provider(provider: str) -> list[str]:
    return DEFAULT_DATABENTO_INDEX_SELECTION


def _allowed_indexes_for_provider(provider: str) -> set[str]:
    return set(DATABENTO_SUPPORTED_INDEXES)


def _normalize_indexes(provider: str, indexes: Any) -> list[str]:
    if not isinstance(indexes, list):
        return []
    allowed = _allowed_indexes_for_provider(provider)
    normalized: list[str] = []
    for value in indexes:
        if isinstance(value, str) and value in allowed and value not in normalized:
            normalized.append(value)
    return normalized


def _normalize_provider(provider: Any) -> str:
    if isinstance(provider, str) and provider in PERSISTED_PROVIDERS:
        return provider
    return ""


def _load_ui_preferences() -> dict[str, Any]:
    prefs_path = _ui_preferences_path()
    if not prefs_path.exists():
        return {}
    try:
        raw = prefs_path.read_text(encoding="utf-8")
    except OSError:
        return {}
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    if not isinstance(loaded, dict):
        return {}
    return loaded


def _save_ui_preferences(prefs: dict[str, Any]) -> None:
    prefs_path = _ui_preferences_path()
    prefs_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = prefs_path.with_suffix(f"{prefs_path.suffix}.tmp")
    payload = json.dumps(prefs, indent=2, sort_keys=True)
    temp_path.write_text(payload, encoding="utf-8")
    temp_path.replace(prefs_path)


def _read_env_file_value(key: str) -> str:
    """Best-effort read for .env values when process env is not populated."""
    env_path = Path(__file__).with_name(".env")
    if not env_path.exists():
        return ""
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key:
                return value.strip().strip("'\"")
    except OSError:
        return ""
    return ""


def _backend_prefers_databento() -> bool:
    """Detect Databento availability from backend health, then local env/.env fallback."""
    try:
        health_resp = requests.get("http://localhost:8000/health", timeout=1.0)
        if health_resp.status_code == 200:
            health = health_resp.json()
            if isinstance(health, dict):
                provider = str(health.get("market_data_provider", health.get("provider", ""))).lower()
                if provider == "databento":
                    return True
                if provider == "polygon":
                    return False
    except (requests.RequestException, ValueError):
        pass

    return bool(os.environ.get("DATABENTO_API_KEY") or _read_env_file_value("DATABENTO_API_KEY"))


if 'api_key' not in st.session_state:
    st.session_state.api_key = ""
if 'ui_preferences' not in st.session_state:
    st.session_state.ui_preferences = _load_ui_preferences()
if 'data_provider' not in st.session_state:
    st.session_state.data_provider = 'databento'
if 'selected_indexes_by_provider' not in st.session_state:
    saved_by_provider = st.session_state.ui_preferences.get("selected_indexes_by_provider", {})
    if not isinstance(saved_by_provider, dict):
        saved_by_provider = {}
    st.session_state.selected_indexes_by_provider = {
        "databento": _normalize_indexes("databento", saved_by_provider.get("databento", [])),
        "polygon": [],
    }
if 'predictions' not in st.session_state:
    st.session_state.predictions = {}
if 'last_analysis_attempt_symbols' not in st.session_state:
    st.session_state.last_analysis_attempt_symbols = ()
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
                # Handle both Agg objects and dict responses
                timestamp = _agg_timestamp_ms(agg)
                close_val = _agg_get(agg, 'close', 'c')

                if timestamp and close_val is not None:
                    data.append({
                        'timestamp': datetime.fromtimestamp(timestamp / 1000),
                        'vix_close': close_val
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
            # Check if data is unavailable
            if gex_analysis.get('data_unavailable'):
                return {
                    'data_unavailable': True,
                    'underlying': ticker,
                    'summary': gex_analysis.get('summary', 'Gamma data unavailable'),
                    'options_root': gex_analysis.get('options_root', ticker),
                    'is_etf_proxy': gex_analysis.get('is_etf_proxy', False),
                }

            # Convert to the expected format for the app
            gex_levels = {
                'total_gex': gex_analysis['total_gex'],
                'net_gex': gex_analysis['net_gex'],
                'pin_strike': gex_analysis['pin_strike'],
                'pin_expiry': gex_analysis.get('pin_expiry'),
                'zero_gamma': gex_analysis.get('zero_gamma'),
                'direction': gex_analysis['direction'],
                'pull_strength': gex_analysis['pull_strength'],
                'summary': gex_analysis['summary'],
                'gamma_walls': gex_analysis['gamma_walls'],
                'gex_by_strike': gex_analysis['gex_by_strike'],
                # ETF proxy info - MUST be displayed explicitly in UI
                'options_root': gex_analysis.get('options_root', ticker),
                'is_etf_proxy': gex_analysis.get('is_etf_proxy', False),
            }

            # Add key support/resistance levels from gamma walls
            if not gex_analysis['gamma_walls'].empty:
                key_levels = gex_analysis['gamma_walls']['strike'].tolist()[:3]
            else:
                crossing = gex_analysis.get('zero_gamma')
                key_levels = [crossing] if _is_number(crossing) else []

            gex_levels['key_levels'] = key_levels

            return gex_levels
        else:
            # Preserve an explicit unavailable state; spot is not an option-derived level.
            return {
                'data_unavailable': True,
                'total_gex': None,
                'net_gex': None,
                'pin_strike': None,
                'pin_expiry': None,
                'zero_gamma': None,
                'direction': None,
                'pull_strength': None,
                'summary': 'Gamma data unavailable',
                'key_levels': [],
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
            # Handle both Agg objects and dict responses
            timestamp = _agg_timestamp_ms(agg)
            if timestamp is not None:
                data.append({
                    'timestamp': datetime.fromtimestamp(timestamp / 1000),
                    'open': _agg_get(agg, 'open', 'o', 0),
                    'high': _agg_get(agg, 'high', 'h', 0),
                    'low': _agg_get(agg, 'low', 'l', 0),
                    'close': _agg_get(agg, 'close', 'c', 0),
                    'volume': _agg_get(agg, 'volume', 'v', 1) or 1  # Indices may not have volume
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
        aggs_list = list(aggs) if aggs else []
        if aggs_list:
            return _agg_get(aggs_list[0], 'close', 'c')
        return None
    except Exception as e:
        st.error(f"Error fetching current price: {str(e)}")
        return None

def fetch_databento_prediction(
    index_name: str,
    symbol: str,
    promoted_predictions: dict[str, Any] | None = None,
) -> Optional[dict]:
    """Read latest Databento pin/GEX payload from backend with staleness checks."""
    stale_after_seconds = float(os.environ.get("LIVE_DATA_STALE_AFTER_SECONDS", "30"))
    prediction, warning_message, info_message = fetch_databento_prediction_live(
        symbol=symbol,
        timeframe=st.session_state.timeframe,
        indicator_builder=calculate_technical_indicators,
        stale_after_seconds=stale_after_seconds,
        promoted_predictions=promoted_predictions,
    )
    if warning_message:
        st.warning(warning_message)
    if info_message:
        st.info(info_message)
    return prediction

def _is_number(value: Any) -> bool:
    try:
        return value is not None and pd.notna(value) and np.isfinite(float(value))
    except (TypeError, ValueError):
        return False

def _fmt_price(value: Any, decimals: int = 0) -> str:
    return f"${float(value):,.{decimals}f}" if _is_number(value) else "N/A"

def _fmt_number(value: Any, decimals: int = 1) -> str:
    return f"{float(value):.{decimals}f}" if _is_number(value) else "N/A"


def _expiration_scope_label(
    expiration: Any,
    *,
    is_primary: bool,
    context_only: bool = False,
) -> str:
    """Label exact-date primary and shadow expiration evidence without promotion."""
    exact_date = str(expiration or "").strip() or "date unavailable"
    if is_primary and context_only:
        return (
            f"PRIMARY forward-context expiration {exact_date} "
            "— not 0DTE authority"
        )
    if is_primary:
        return f"PRIMARY expiration {exact_date}"
    return f"SHADOW expiration {exact_date} — analytical context only"


def _structural_distance_from_spot(level: Any, spot: Any) -> str:
    """Describe level placement without converting it into forecast direction."""
    if not _is_number(level) or not _is_number(spot) or float(spot) <= 0:
        return "Distance from spot unavailable; no direction is implied."
    distance_pct = (float(level) - float(spot)) / float(spot) * 100.0
    if abs(distance_pct) < 0.005:
        placement = "at spot"
    else:
        relation = "above" if distance_pct > 0 else "below"
        placement = f"{abs(distance_pct):.2f}% {relation} spot"
    return f"{placement}; structural distance only, not a price forecast."


def _confidence_display(prediction: dict[str, Any]) -> str:
    """Describe forecast score semantics without calling a heuristic probability."""
    value = prediction.get("confidence")
    if not _is_number(value):
        return "Model quality score: unavailable"
    score = float(value)  # type: ignore[arg-type]
    if prediction.get("confidence_calibrated") is True:
        return f"Calibrated confidence: {score:.1f}%"
    kind = str(prediction.get("confidence_kind") or "unspecified_score")
    if kind == "data_quality_heuristic":
        return (
            f"Data-quality score: {score:.1f}/100 "
            "(heuristic; not a forecast probability)"
        )
    return (
        f"Reported score: {score:.1f}/100 "
        f"({kind}; calibration not verified)"
    )


def _prediction_source_label(prediction: dict[str, Any] | None = None, payload: dict[str, Any] | None = None) -> str:
    prediction = prediction or {}
    payload = payload or {}
    provider = str(prediction.get("provider") or payload.get("provider") or "databento")
    if (
        payload_has_fallback_provenance(prediction)
        or payload_has_fallback_provenance(payload)
        or provider == "historical-fallback"
    ):
        return str(
            prediction.get("source_label")
            or payload.get("source_label")
            or "Historical / fallback context only"
        )
    prediction_authority = (
        prediction.get("prediction_authority")
        or payload.get("prediction_authority")
        or {}
    )
    return str(
        prediction_authority.get("source_label")
        or prediction.get("source_label")
        or payload.get("source_label")
        or "Databento live data; prediction authority unavailable"
    )

def _is_fallback_prediction(prediction: dict[str, Any] | None = None, payload: dict[str, Any] | None = None) -> bool:
    prediction = prediction or {}
    payload = payload or {}
    provider = str(prediction.get("provider") or payload.get("provider") or "")
    return bool(
        payload_has_fallback_provenance(prediction)
        or payload_has_fallback_provenance(payload)
        or provider == "historical-fallback"
    )

def is_near_market_close():
    """Use the reviewed session calendar, including holidays and early closes."""
    return near_close_seconds() is not None


def export_to_csv(predictions_data, include_indicators=True):
    """Export predictions and indicators to CSV for Excel compatibility"""
    import math as _math
    import numbers as _numbers

    def _positive_finite_export_number(value):
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if _math.isfinite(number) and number > 0.0 else None

    def _canonical_subscription_epoch(value):
        if not isinstance(value, str) or len(value) != 64:
            return None
        return value if all(character in '0123456789abcdef' for character in value) else None

    def _positive_subscription_generation(value):
        if isinstance(value, bool) or not isinstance(value, _numbers.Integral) or value <= 0:
            return None
        return int(value)

    export_data = []
    required_indicator_columns = {
        'RSI', 'MACD', 'Signal_Line', 'SMA_20', 'Momentum', 'VWAP', 'AMA',
    }

    for index_name, pred_data in predictions_data.items():
        pin_payload = pred_data.get('pin_payload') or {}
        provider = str(
            pred_data.get('provider') or pin_payload.get('provider') or ''
        ).strip().lower()
        is_databento_record = bool(
            provider.startswith('databento')
            or pin_payload
            or 'validation_is_valid' in pred_data
        )
        raw_epochs = [
            value
            for value in (
                pred_data.get('subscription_epoch_id'),
                pin_payload.get('subscription_epoch_id'),
            )
            if value is not None
        ]
        canonical_epochs = [_canonical_subscription_epoch(value) for value in raw_epochs]
        epoch_identity_valid = bool(raw_epochs) and all(
            value is not None for value in canonical_epochs
        ) and len(set(canonical_epochs)) == 1
        raw_generations = [
            value
            for value in (
                pred_data.get('subscription_generation'),
                pin_payload.get('subscription_generation'),
            )
            if value is not None
        ]
        canonical_generations = [
            _positive_subscription_generation(value) for value in raw_generations
        ]
        generation_identity_valid = bool(raw_generations) and all(
            value is not None for value in canonical_generations
        ) and len(set(canonical_generations)) == 1
        subscription_epoch_id = canonical_epochs[0] if epoch_identity_valid else None
        subscription_generation = (
            canonical_generations[0] if generation_identity_valid else None
        )
        validation_is_valid = (
            pin_payload.get('validation_is_valid')
            if pin_payload
            else pred_data.get('validation_is_valid')
        )
        gamma_excluded = (
            pin_payload.get('gamma_excluded_from_model')
            if pin_payload
            else pred_data.get('gamma_excluded_from_model')
        )
        forecast_state = str(
            pred_data.get('forecast_state') or pin_payload.get('forecast_state') or ''
        ).upper()
        fallback = bool(
            payload_has_fallback_provenance(pred_data)
            or payload_has_fallback_provenance(pin_payload)
            or provider == 'historical-fallback'
        )
        numeric_forecast_available = bool(
            _positive_finite_export_number(pred_data.get('current_price')) is not None
            and _positive_finite_export_number(pred_data.get('predicted_price')) is not None
        )
        if (
            fallback
            or forecast_state in {'ABSTAIN', 'STALE', 'UNAVAILABLE'}
            or not numeric_forecast_available
            or (
                is_databento_record
                and (
                    validation_is_valid is not True
                    or gamma_excluded is not False
                    or subscription_epoch_id is None
                    or subscription_generation is None
                )
            )
        ):
            # Defense in depth: callers currently pass lifecycle-filtered
            # display_predictions, but this export helper must remain safe if
            # a future caller accidentally supplies a diagnostic observation.
            continue
        validation_status = (
            'fallback'
            if fallback
            else 'valid'
            if validation_is_valid is True and gamma_excluded is False
            else 'unverified'
        )
        prediction_authority = pred_data.get('prediction_authority') or {}
        indicator_provenance = pred_data.get('indicator_provenance') or {}
        indicator_frame = pred_data.get('df')
        indicator_flag = pred_data.get('indicators_available')
        indicator_frame_available = (
            isinstance(indicator_frame, pd.DataFrame)
            and not indicator_frame.empty
            and required_indicator_columns.issubset(indicator_frame.columns)
            and indicator_flag is True
            and indicator_provenance.get('is_observed') is True
            and indicator_provenance.get('is_synthetic') is False
        )
        if include_indicators and indicator_frame_available:
            df = indicator_frame.copy()
            df['record_type'] = (
                'observed_indicator_frame'
                if indicator_provenance.get('is_observed') is True
                else 'indicator_frame_unverified'
            )
            df['training_eligible'] = False
            df['eligibility_reason'] = (
                'display-only indicator context; not immutable promoted model evidence'
            )
            df['indicator_source_kind'] = indicator_provenance.get('required_source_kind')
            df['indicator_source_observed'] = indicator_provenance.get('is_observed') is True
            df['indicator_source_synthetic'] = indicator_provenance.get('is_synthetic') is True
            df['subscription_epoch_id'] = subscription_epoch_id
            df['subscription_generation'] = subscription_generation
            df['index_name'] = index_name
            df['predicted_price'] = pred_data.get('predicted_price', None)
            df['confidence'] = pred_data.get('confidence', None)
            df['confidence_kind'] = pred_data.get('confidence_kind', 'unavailable')
            df['confidence_scale'] = pred_data.get('confidence_scale', 'not_applicable')
            df['confidence_calibrated'] = pred_data.get('confidence_calibrated') is True
            df['prediction_mode'] = pred_data.get('prediction_mode')
            df['prediction_authority_state'] = prediction_authority.get('authority_state')
            df['is_estimate'] = prediction_authority.get('is_estimate') is True
            df['tcbbo_promoted'] = prediction_authority.get('tcbbo_promoted') is True
            df['observed_basis'] = prediction_authority.get('observed_basis')
            df['inferred_basis'] = prediction_authority.get('inferred_basis')
            df['authority_limitations'] = ';'.join(prediction_authority.get('limitations') or [])
            df['prediction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            export_data.append(df)
        else:
            # Simple export with just predictions
            export_data.append(pd.DataFrame([{
                'Record Type': 'prediction_record',
                'Training Eligible': False,
                'Eligibility Reason': 'display-only forecast; not immutable promoted model evidence',
                'Index': index_name,
                'Ticker': pred_data.get('ticker', ''),
                'Current Price': pred_data.get('current_price'),
                'Predicted Price': pred_data.get('predicted_price'),
                'Change %': pred_data.get('change_pct'),
                'Confidence / Quality Score': pred_data.get('confidence'),
                'Confidence Kind': pred_data.get('confidence_kind', 'unavailable'),
                'Confidence Scale': pred_data.get('confidence_scale', 'not_applicable'),
                'Confidence Calibrated': pred_data.get('confidence_calibrated') is True,
                'Model': pred_data.get('model_type', ''),
                'Model Version': pred_data.get('model_version'),
                'Model Artifact SHA-256': pred_data.get('model_artifact_sha256'),
                'Feature Schema Version': pred_data.get('feature_schema_version'),
                'Feature SHA-256': pred_data.get('feature_hash'),
                'Prediction Mode': pred_data.get('prediction_mode'),
                'Prediction Authority State': prediction_authority.get('authority_state'),
                'Is Estimate': prediction_authority.get('is_estimate') is True,
                'TCBBO Promoted': prediction_authority.get('tcbbo_promoted') is True,
                'Observed Basis': prediction_authority.get('observed_basis'),
                'Inferred Basis': prediction_authority.get('inferred_basis'),
                'Authority Limitations': ';'.join(prediction_authority.get('limitations') or []),
                'Indicators Available': indicator_frame_available,
                'Indicator Status': pred_data.get('indicator_status', 'unreported'),
                'Indicator Source Kind': indicator_provenance.get('required_source_kind'),
                'Indicator Source Observed': indicator_provenance.get('is_observed') is True,
                'Indicator Source Synthetic': indicator_provenance.get('is_synthetic') is True,
                'Indicator Reason': indicator_provenance.get('reason'),
                'Forecast ID': pred_data.get('forecast_id'),
                'Forecast State': pred_data.get('forecast_state'),
                'Decision Grade': pred_data.get('decision_grade') is True,
                'Provider': pred_data.get('provider', ''),
                'Source Label': pred_data.get('source_label', ''),
                'Quote Timestamp': pin_payload.get('timestamp', ''),
                'Subscription Epoch ID': subscription_epoch_id,
                'Subscription Generation': subscription_generation,
                'Quote Age Seconds': pred_data.get('quote_age_seconds', pred_data.get('payload_age_seconds', '')),
                'Active Contract Count': pred_data.get('active_contract_count', pin_payload.get('contracts', '')),
                'Fresh Quote Count': pred_data.get('fresh_quote_count', pin_payload.get('fresh_quote_count', '')),
                'Gamma Pin': (pred_data.get('gex_data') or {}).get('pin_strike', ''),
                'Max Pain': (pred_data.get('gex_data') or {}).get('max_pain_strike', ''),
                ZERO_GAMMA_DISPLAY_LABEL: (pred_data.get('gex_data') or {}).get('zero_gamma', ''),
                'Inference Device': pred_data.get('inference_device', ''),
                'Validation Status': validation_status,
                'Timeframe': pred_data.get('timeframe', ''),
                'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }]))

    if export_data:
        # Every branch contributes a frame, so partial indicator availability
        # cannot produce a mixed dict/DataFrame concat failure.
        export_df = pd.concat(export_data, ignore_index=True, sort=False)

        # Generate CSV
        csv = export_df.to_csv(index=False)
        return csv
    return None

def load_daily_pin_history(
    symbol: str,
    date_str: Optional[str] = None,
    display_timezone: DisplayTimezone | None = None,
) -> pd.DataFrame:
    """
    Load all snapshots from NDJSON for a specific symbol and date.
    Returns a DataFrame with pin history (Time, Pin Strike, Spot, Distance, Pull Strength, etc.)
    """
    viewer_timezone = display_timezone or DISPLAY_TIMEZONE

    if date_str is None:
        date_str = current_local_date(viewer_timezone)

    selection = _cached_local_snapshot_selection(
        'exports',
        symbol,
        date_str,
        viewer_timezone.name,
    )
    rows = []
    for snap in selection.records:
        provenance_status = gamma_snapshot_provenance_status(snap)
        if provenance_status == DIAGNOSTIC_INVALID_SNAPSHOT:
            continue
        export_provenance = snapshot_research_export_fields(snap)
        sanitized_snap = snapshot_research_export_record(snap)
        # Storage remains UTC; only the user-facing clock is localized.
        ts_str = _snapshot_timestamp_value(snap)
        time_display = format_display_timestamp(
            ts_str,
            viewer_timezone,
            format_string='%I:%M %p %Z',
        )

        # Get values with backward compatibility.
        pin_value = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike', 0)
        pin_strike = float(pin_value) if _is_number(pin_value) else 0.0
        spot = float(snap.get('spot_last')) if _is_number(snap.get('spot_last')) else 0.0
        gross_gex = float(snap.get('gross_gex')) if _is_number(snap.get('gross_gex')) else 0.0
        net_gex = float(snap.get('net_gex')) if _is_number(snap.get('net_gex')) else 0.0
        call_gex = float(snap.get('call_gex_total')) if _is_number(snap.get('call_gex_total')) else 0.0
        put_gex = float(snap.get('put_gex_total')) if _is_number(snap.get('put_gex_total')) else 0.0

        distance_pct = ((spot - pin_strike) / pin_strike * 100) if pin_strike else 0
        pull_strength = abs(net_gex / gross_gex * 100) if gross_gex else 0

        rows.append({
            'Time': time_display,
            'Pin Strike': pin_strike,
            'Spot Price': spot,
            'Distance': f"{distance_pct:+.2f}%",
            'Pull Strength': f"{pull_strength:.2f}",
            'Total GEX': _fmt_gex_units(gross_gex, 3),
            'Net GEX': _fmt_gex_units(net_gex, 3),
            'Producer Check': export_provenance['review_calculation_status'],
            'Validation Method': export_provenance['review_validation_method'],
            'Validation Policy': export_provenance['review_validation_policy_sha256'],
            'Provenance': provenance_status.replace('_', ' ').capitalize(),
            # Raw values for CSV export.
            '_pin_strike': pin_strike,
            '_spot': spot,
            '_distance_pct': distance_pct,
            '_pull_strength': pull_strength,
            '_gross_gex': gross_gex,
            '_net_gex': net_gex,
            '_call_gex': call_gex,
            '_put_gex': put_gex,
            '_timestamp_utc': ts_str,
            '_is_valid': snap.get('validation_is_valid', False),
            **{key: value for key, value in export_provenance.items() if key.startswith('review_')},
            '_subscription_epoch_id': snap.get('subscription_epoch_id'),
            '_subscription_generation': snap.get('subscription_generation'),
            '_provenance_status': provenance_status,
            '_current_live_eligible': False,
            '_fallback_provenance': export_provenance['fallback_provenance'],
            '_universe_provenance': json.dumps(sanitized_snap.get('universe_provenance') or {}, separators=(',', ':')),
            '_oi_analytics_provenance': json.dumps(sanitized_snap.get('oi_analytics_provenance') or {}, separators=(',', ':')),
        })

    return pd.DataFrame(rows)

def create_eod_zip_export(
    date_str: Optional[str] = None,
    display_timezone: DisplayTimezone | None = None,
) -> bytes:
    """
    Create a ZIP file with all daily data for download.
    Includes research NDJSON, separate diagnostics, and provenance-aware CSVs.
    Stored producer flags are retained; they do not establish current authority.
    """
    import io
    import hashlib
    import zipfile
    viewer_timezone = display_timezone or DISPLAY_TIMEZONE

    if date_str is None:
        date_str = current_local_date(viewer_timezone)

    buffer = io.BytesIO()

    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            'README.txt',
            'RESEARCH DATA ONLY. Stored calculation validation and process identity '
            'do not establish current live, prediction, training, or opening-range '
            'eligibility. Original producer fields are preserved. '
            'export_provenance_status, fallback_provenance, and current_live_eligible '
            'describe this export. Missing opening observations are not reconstructed. '
            'Validation policies and producer outcomes are separated in per-symbol '
            'validation-groups ZIP archives and validation-groups CSV folders. '
            'coverage-manifest.json reports expected, observed, failed, and missing-symbol evidence. '
            'Derived downloads redact machine-local source paths while preserving source hashes. '
            'review_* labels do not retrospectively validate unversioned records.\n',
        )
        symbols = list_gamma_snapshot_symbols('exports')
        all_data = []
        evidence_by_symbol = {}

        for symbol in symbols:
            selection = _cached_local_snapshot_selection(
                'exports',
                symbol,
                date_str,
                viewer_timezone.name,
            )
            evidence = partition_snapshot_evidence(selection.records)
            evidence_by_symbol[symbol] = evidence

            if evidence.usable_records:
                from app.services.validation_review import method_archive
                zf.writestr(f'{symbol}_{date_str}.validation-groups.zip', method_archive(evidence.usable_records))

            if evidence.historical_records:
                historical_ndjson = '\n'.join(
                    json.dumps(
                        {
                            **snapshot_research_export_record(record),
                        },
                        separators=(',', ':'),
                        default=str,
                    )
                    for record in evidence.historical_records
                ) + '\n'
                zf.writestr(
                    f'{symbol}_{date_str}.historical_unverified.ndjson',
                    historical_ndjson,
                )

            if evidence.diagnostic_records:
                zf.writestr(
                    f'{symbol}_{date_str}.failed_audits.ndjson',
                    '\n'.join(json.dumps(snapshot_research_export_record(record), separators=(',', ':'), default=str)
                              for record in evidence.diagnostic_records) + '\n',
                )

            if evidence.usable_records or evidence.historical_records:
                # Also create pin history CSV
                pin_df = load_daily_pin_history(symbol, date_str, viewer_timezone)
                if not pin_df.empty:
                    # Export clean CSV (without display formatting)
                    export_df = pd.DataFrame({
                        'symbol': symbol,
                        'timestamp_utc': pin_df['_timestamp_utc'],
                        'timestamp_local': pin_df['_timestamp_utc'].map(
                            lambda value: format_display_timestamp(
                                value,
                                viewer_timezone,
                                format_string='%Y-%m-%d %H:%M:%S %Z',
                            )
                        ),
                        'display_timezone': viewer_timezone.name,
                        'pin_strike': pin_df['_pin_strike'],
                        'spot': pin_df['_spot'],
                        'distance_pct': pin_df['_distance_pct'],
                        'pull_strength': pin_df['_pull_strength'],
                        'gross_gex': pin_df['_gross_gex'],
                        'net_gex': pin_df['_net_gex'],
                        'call_gex': pin_df['_call_gex'],
                        'put_gex': pin_df['_put_gex'],
                        'is_valid': pin_df['_is_valid'],
                        'subscription_epoch_id': pin_df['_subscription_epoch_id'],
                        'subscription_generation': pin_df['_subscription_generation'],
                        'provenance_status': pin_df['_provenance_status'],
                        'current_live_eligible': pin_df['_current_live_eligible'],
                        **{column: pin_df[column] for column in pin_df.columns if column.startswith('review_')},
                        'fallback_provenance': pin_df['_fallback_provenance'],
                        'universe_provenance': pin_df['_universe_provenance'],
                        'oi_analytics_provenance': pin_df['_oi_analytics_provenance'],
                    })
                    for method_key, method_frame in export_df.groupby('review_validation_group', dropna=False):
                        method_folder = hashlib.sha256(str(method_key).encode()).hexdigest()
                        zf.writestr(f'validation-groups/{method_folder}/{symbol}_pin_history_{date_str}.csv', method_frame.to_csv(index=False))
                    all_data.append(export_df)

        # Add combined CSV
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            for method_key, method_frame in combined_df.groupby('review_validation_group', dropna=False):
                method_folder = hashlib.sha256(str(method_key).encode()).hexdigest()
                zf.writestr(f'validation-groups/{method_folder}/all_indices_{date_str}.csv', method_frame.to_csv(index=False))

        coverage = snapshot_coverage_manifest(
            evidence_by_symbol,
            expected_symbols=("SPX", "NDX", "VIX", "RUT"),
            trading_date=date_str,
        )
        zf.writestr(
            'coverage-manifest.json',
            json.dumps(coverage, sort_keys=True, indent=2) + '\n',
        )

    buffer.seek(0)
    return buffer.getvalue()


def _render_eod_zip_download(
    date_str: str,
    display_timezone: DisplayTimezone,
    *,
    key: str,
) -> None:
    """Prepare research archives only on request and reuse the matching bytes."""

    cache_key = '_prepared_gamma_research_zip'
    request_identity = (date_str, display_timezone.name)
    if st.button("Prepare research ZIP", key=f'{key}_prepare'):
        # A failed rebuild must not leave an older archive looking newly prepared.
        st.session_state.pop(cache_key, None)
        try:
            with st.spinner("Preparing research ZIP…"):
                has_data = any(
                    gamma_snapshot_provenance_status(snap) != DIAGNOSTIC_INVALID_SNAPSHOT
                    for sym in list_gamma_snapshot_symbols('exports')
                    for snap in _cached_local_snapshot_selection(
                        'exports', sym, date_str, display_timezone.name,
                    ).records
                )
                if not has_data:
                    st.info(
                        "No calculation-valid research snapshots are available for this date. "
                        "Failed audits remain diagnostic evidence only."
                    )
                    return
                archive = create_eod_zip_export(date_str, display_timezone)
            st.session_state[cache_key] = {
                'identity': request_identity,
                'data': archive,
                'prepared_at_utc': utc_iso(),
            }
        except Exception as exc:
            st.error(f"Error creating ZIP: {str(exc)[:50]}")
            return

    prepared = st.session_state.get(cache_key)
    if not prepared or prepared['identity'] != request_identity:
        st.caption("Prepare the archive when you need a download.")
        return

    prepared_at = format_display_timestamp(
        prepared['prepared_at_utc'], display_timezone,
    )
    st.caption(
        f"Prepared {prepared_at} for {date_str} ({display_timezone.name}). "
        "Prepare again to include newer records. Research data only; "
        "legacy context and failed audits remain separately labeled."
    )
    st.download_button(
        "Download Gamma Research ZIP",
        prepared['data'],
        f"gamma_data_{date_str}.zip",
        "application/zip",
        key=f'{key}_download',
        type="primary",
        on_click="ignore",
    )

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
    missing_cols = [c for
    c in feature_columns if c not in df.columns]
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

    # Backend-only Databento frames may not include Bollinger Bands.
    if {'BB_Upper', 'BB_Lower'}.issubset(df.columns):
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
    fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)  # type: ignore[arg-type]
    fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)  # type: ignore[arg-type]

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
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=3, col=1)  # type: ignore[arg-type]

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
st.markdown("Research estimates from validated option-market data. Availability and authority are shown for each symbol.")
st.caption(
    f"Page snapshot: {_display_clock()} · Automatic refresh is off. "
    "Analyze & Predict requests new results; displayed ages describe the last check."
)

# Sidebar for API key
with st.sidebar:
    st.header("⚙️ Configuration")

    st.session_state.data_provider = 'databento'
    st.selectbox(
        "Market Data Provider",
        options=["Databento OPRA"],
        index=0,
        disabled=True,
        help="Databento-only mode is enabled."
    )
    if st.session_state.data_provider == 'databento' and not _backend_prefers_databento():
        st.warning("Databento mode selected, but backend has not reported Databento readiness yet. Keeping Databento mode and waiting for backend startup.")

    st.info("Databento runs through the FastAPI backend using DATABENTO_API_KEY. No Polygon key required.")

    st.divider()

    st.header("📊 Select Indexes")
    index_options = list(INDEXES.keys())
    if st.session_state.data_provider == 'databento':
        databento_universe = fetch_databento_universe()
        selectable_symbols = set(selectable_databento_symbols(databento_universe))
        index_options = [
            name for name, symbol in DATABENTO_INDEXES.items()
            if symbol in selectable_symbols
        ]
        active_canaries = active_canary_symbols(databento_universe)
        st.caption(
            "SPX and NDX are required; VIX is optional context. SPY, QQQ, and other symbols "
            "appear only after the backend selects live contracts for a canary."
        )
        with st.expander("Databento symbol coverage", expanded=False):
            st.markdown("**Required prediction set:** SPX, NDX")
            st.markdown("**Optional context:** VIX")
            st.markdown("**Supported ETF additions:** SPY, QQQ, DIA, IWM")
            st.markdown(
                "**Backend-active canaries:** "
                + (", ".join(active_canaries) if active_canaries else "None")
            )
            st.markdown(
                "**Adapter-supported canary candidates:** "
                + ", ".join(CANARY_CANDIDATE_SYMBOLS)
            )
            st.caption(
                "A canary must pass quote freshness, coverage, lineage, and persistence "
                "checks before it becomes selectable. The UI never expands the OPRA stream."
            )

    active_provider = st.session_state.data_provider
    saved_indexes = st.session_state.selected_indexes_by_provider.get(active_provider, [])
    selected_defaults = [name for name in saved_indexes if name in index_options]
    if not selected_defaults:
        provider_defaults = _default_indexes_for_provider(active_provider)
        selected_defaults = [name for name in provider_defaults if name in index_options]
    if not selected_defaults and index_options:
        selected_defaults = [index_options[0]]

    selected_indexes = st.multiselect(
        "Choose markets to analyze",
        options=index_options,
        default=selected_defaults
    )
    st.session_state.selected_indexes_by_provider[active_provider] = selected_indexes

    next_preferences = {
        "data_provider": st.session_state.data_provider,
        "selected_indexes_by_provider": {
            "databento": st.session_state.selected_indexes_by_provider.get("databento", []),
            "polygon": [],
        },
    }
    if next_preferences != st.session_state.ui_preferences:
        _save_ui_preferences(next_preferences)
        st.session_state.ui_preferences = next_preferences

    # The Databento live path does not consume a user-selected history window.
    # Keep the legacy value available to unreachable Polygon helpers without
    # presenting a control that falsely implies it changes live predictions.
    days_history = 60
    st.caption(
        "Historical lookback is not a live Databento control. Long-history "
        "data will be used by the separate CUDA/ML research pipeline after "
        "point-in-time validation."
    )

    st.divider()

    st.header("🤖 Model Settings")
    selected_model = st.text_input(
        "Live Estimator Contract",
        value="Backend-authoritative; exact model shown with each result",
        disabled=True,
        help=(
            "The Streamlit client does not select or relabel the backend model. "
            "TCBBO candidates remain research-only until evidence-gated promotion."
        ),
    )
    st.session_state.selected_model = selected_model
    st.caption(
        "The backend payload supplies model version, artifact identity, feature identity, "
        "forecast state, and decision-grade status."
    )

    selected_timeframe = st.selectbox(
        "Prediction Timeframe",
        options=["1-day"],
        index=0,
        help="Live close prediction horizon from backend."
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

    # The backend subscription window is the holiday/early-close authority.
    # If it cannot be read coherently, never infer an open session locally.
    market_session_status = fetch_backend_market_session_status()
    market_session_state = market_session_status["state"]
    if market_session_state == "open":
        st.info("Market Status: 🟢 OPEN")
    elif market_session_state == "closed":
        st.info("Market Status: 🔴 CLOSED")
    else:
        st.warning("Market Status: 🟠 UNAVAILABLE (backend session authority not verified)")

    # Create tabs for Live Streaming, Advisor, Frozen Gamma, and Debug Snapshot Viewer
    streaming_tab, advisor_tab, frozen_gamma_tab, debug_snapshot_tab = st.tabs(["📡 Live Streaming", "🧠 Advisor", "🧊 Frozen Gamma (Audit)", "🔍 Debug Snapshot"])

    # === FROZEN GAMMA TAB ===
    with frozen_gamma_tab:
        st.markdown("Read-only research view — Shows the last calculation-valid stored gamma state; current authority is not established.")

        # Check freeze status
        is_frozen, freeze_reason = get_freeze_status()
        if is_frozen:
            st.info(f"🔒 Market Freeze Active: {freeze_reason}")

        load_historical_audit = st.checkbox(
            "Load historical audit",
            value=False,
            key="load_historical_audit",
            help="Load stored gamma snapshots and pin history. This can take time; turn it off when returning to live analysis.",
        )
        if not load_historical_audit:
            st.caption("Historical audit loading is off. Enable it when you want to inspect stored snapshots.")
        elif selected_indexes:
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
                    # Display the persisted UTC instant in the viewer's timezone.
                    snapshot_time = snapshot.generated_at_utc
                    if snapshot_time:
                        snapshot_time_display = format_display_timestamp(
                            snapshot_time,
                            DISPLAY_TIMEZONE,
                        )
                        if snapshot_time_display == 'N/A':
                            st.warning("🧊 Snapshot timestamp is malformed; inspect the stored UTC record.")
                        else:
                            st.success(f"🧊 Snapshot recorded: {snapshot_time_display}")
                    else:
                        st.success("🧊 Frozen Snapshot (no timestamp)")
                    st.caption(
                        f"Display timezone: {display_timezone_label(DISPLAY_TIMEZONE)}. "
                        "Persisted snapshot timestamps remain UTC."
                    )

                    # Display gamma metrics from snapshot
                    # Row 1: Pin, Gross GEX, Net GEX
                    metric_col1, metric_col2, metric_col3 = st.columns(3)

                    with metric_col1:
                        st.metric("📍 Primary Gamma Pin", f"${snapshot.primary_gamma_pin_strike:,.0f}")

                    with metric_col2:
                        # Use gross_gex if available, fall back to total_gex_abs for backward compatibility
                        gross_gex = getattr(snapshot, 'gross_gex', None) or snapshot.total_gex_abs
                        st.metric("Gross GEX (raw)", _fmt_gex_units(gross_gex), help="sum(call_gex) + sum(put_gex), in raw gamma × OI × 100 units; not dollars")

                    with metric_col3:
                        # Use net_gex if available, fall back to total_gex_net for backward compatibility
                        net_gex = getattr(snapshot, 'net_gex', None)
                        if net_gex is None:
                            net_gex = snapshot.total_gex_net
                        st.metric("Net GEX (raw)", _fmt_gex_units(net_gex), help="sum(call_gex) - sum(put_gex), in raw gamma × OI × 100 units; not dollars")

                    # Row 2: Call GEX, Put GEX, first strike-bucket crossing
                    gex_col1, gex_col2, gex_col3 = st.columns(3)
                    with gex_col1:
                        call_gex = getattr(snapshot, 'call_gex_total', 0)
                        if call_gex > 0:
                            st.metric("Call GEX (raw)", _fmt_gex_units(call_gex))
                    with gex_col2:
                        put_gex = getattr(snapshot, 'put_gex_total', 0)
                        if put_gex > 0:
                            st.metric("Put GEX (raw)", _fmt_gex_units(put_gex))
                    with gex_col3:
                        if snapshot.zero_gamma_level:
                            st.metric(ZERO_GAMMA_DISPLAY_LABEL, f"${snapshot.zero_gamma_level:,.0f}")

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
                        st.caption("✅ Stored calculation validation passed; research only")
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
                                    gex = strike_data.get('abs_gex', 0)
                                    st.caption(f"${strike:,.0f}: {_fmt_gex_units(gex, 3)}")
                                else:
                                    st.caption(str(strike_data))

                    # Pin History Table (from NDJSON)
                    today_str = _current_snapshot_display_date()
                    pin_history = load_daily_pin_history(
                        symbol,
                        today_str,
                        DISPLAY_TIMEZONE,
                    )
                    parity_history = build_opra_parity_gamma_history(
                        _cached_local_snapshot_selection(
                            'exports',
                            symbol,
                            today_str,
                            DISPLAY_TIMEZONE.name,
                        ).records
                    )

                    render_retained_opra_parity_history(
                        parity_history,
                        symbol=symbol,
                        local_date=today_str,
                        display_timezone=DISPLAY_TIMEZONE,
                        key_prefix="frozen_opra_parity_history",
                    )

                    if not pin_history.empty:
                        with st.expander(f"📊 Intraday Pin History ({len(pin_history)} samples)", expanded=False):
                            # Display table (hide internal columns)
                            display_cols = ['Time', 'Pin Strike', 'Spot Price', 'Distance', 'Pull Strength', 'Total GEX', 'Net GEX', 'Valid', 'Provenance']
                            st.dataframe(pin_history[display_cols], hide_index=True, width='stretch')

                            # Export button for this symbol
                            export_df = pd.DataFrame({
                                'symbol': symbol,
                                'timestamp_utc': pin_history['_timestamp_utc'],
                                'timestamp_local': pin_history['_timestamp_utc'].map(
                                    lambda value: format_display_timestamp(
                                        value,
                                        DISPLAY_TIMEZONE,
                                        format_string='%Y-%m-%d %H:%M:%S %Z',
                                    )
                                ),
                                'display_timezone': DISPLAY_TIMEZONE.name,
                                'pin_strike': pin_history['_pin_strike'],
                                'spot': pin_history['_spot'],
                                'distance_pct': pin_history['_distance_pct'],
                                'pull_strength': pin_history['_pull_strength'],
                                'gross_gex': pin_history['_gross_gex'],
                                'net_gex': pin_history['_net_gex'],
                                'is_valid': pin_history['_is_valid'],
                                'subscription_epoch_id': pin_history['_subscription_epoch_id'],
                                'subscription_generation': pin_history['_subscription_generation'],
                                'provenance_status': pin_history['_provenance_status'],
                                'current_live_eligible': pin_history['_current_live_eligible'],
                                'fallback_provenance': pin_history['_fallback_provenance'],
                                'universe_provenance': pin_history['_universe_provenance'],
                                'oi_analytics_provenance': pin_history['_oi_analytics_provenance'],
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
            st.caption(
                "Prepare a download for the viewer-local calendar day when needed. "
                "Raw provenance timestamps remain UTC."
            )

            today_str = _current_snapshot_display_date()

            _render_eod_zip_download(
                today_str, DISPLAY_TIMEZONE, key='frozen_gamma_zip',
            )
        else:
            st.info("Select indexes in the sidebar to view frozen gamma snapshots.")

    # === LIVE STREAMING TAB ===
    # NOTE: WebSocket connections are managed by the FastAPI backend ONLY
    # Streamlit reads cached data via REST endpoints (no direct WebSocket creation)
    with streaming_tab:
        st.info("📡 **Live data is streamed via the backend service**")
        st.caption("The FastAPI backend manages all WebSocket connections to prevent duplicate connections.")

        # Clear the previous timer preference in already-open sessions too.
        initialize_live_autorefresh(st.session_state)
        st.caption(
            "Automatic refresh is off. Values and eligibility reflect the last page "
            "update; use Refresh live status now to check the latest data."
        )
        if st.button("Refresh live status now", type="secondary"):
            st.rerun()

        if market_session_state == "closed":
            st.warning("ℹ️ Market closed - showing cached data from last session")
        elif market_session_state == "unavailable":
            st.warning("Live session status is unavailable; current-only values remain withheld")

        # Show backend streaming status
        render_backend_streaming_status()
        if st.session_state.data_provider == 'databento':
            render_opening_range_panel()
        render_shadow_research_panel()

        def _render_sidebar_state_context(state: Any) -> None:
            observed = format_display_timestamp(
                state.source_as_of_utc,
                DISPLAY_TIMEZONE,
                format_string='%b %d, %Y %I:%M:%S %p %Z',
            ) if state.source_as_of_utc else "unavailable"
            checked = format_display_timestamp(
                state.checked_at_utc,
                DISPLAY_TIMEZONE,
                format_string='%b %d, %Y %I:%M:%S %p %Z',
            )
            generation = (
                f"{state.subscription_generation}/{state.active_generation}"
                if state.subscription_generation is not None
                and state.active_generation is not None
                else "unverified"
            )
            age = (
                f"{state.data_age_seconds:.1f}s"
                if state.data_age_seconds is not None
                else "unavailable"
            )
            st.caption(
                f"Observed: {observed} | Checked: {checked} | "
                f"Age: {age} | Generation payload/active: {generation}"
            )

        def _render_sidebar_state_failure(state: Any) -> None:
            optional_label = " (optional context)" if state.optional else ""
            label = diagnostic_state_label(state.state)
            message = (
                f"{state.symbol}{optional_label}: {label}. "
                f"Reason: {state.reason}"
            )
            if state.state in {
                "timeout",
                "connection_error",
                "request_error",
                "http_error",
                "malformed",
            }:
                st.error(message)
            else:
                st.warning(message)
            if state.state in {"fallback", "closed_context"}:
                st.caption(
                    "The backend supplied historical or fallback evidence. This label does "
                    "not mean the exchange is closed. A live forecast requires current-session "
                    "source validation and sufficient fresh paired option quotes."
                )
            _render_sidebar_state_context(state)

        def _render_sidebar_diagnostic_evidence(state: Any) -> None:
            """Show failed-state evidence without promoting it into a prediction."""
            evidence = getattr(state, "diagnostic_evidence", None)
            st.error(
                "⚠️ Diagnostic only / ABSTAIN — this symbol failed a validity, freshness, or "
                "generation gate. Values below are excluded from predictions, AI analysis, "
                "alerts, and validated exports."
            )

            if evidence is not None and evidence.scope == "historical_context_only":
                if _is_number(evidence.spot):
                    st.markdown(
                        "**Retained reference value (diagnostic; not verified live):** "
                        f"{_fmt_price(evidence.spot, 2)}"
                    )
            elif evidence is not None and evidence.scope == "partial_live_calculation":
                if _is_number(evidence.spot):
                    st.markdown(
                        "**Observed parity spot (diagnostic):** "
                        f"{_fmt_price(evidence.spot, 2)}"
                    )
                if _is_number(evidence.gamma_pin):
                    st.markdown(
                        "**Calculated gamma pin (rejected surface):** "
                        f"{_fmt_price(evidence.gamma_pin, 0)}"
                    )
                if _is_number(evidence.zero_gamma):
                    st.caption(
                        f"{ZERO_GAMMA_DISPLAY_LABEL}: {_fmt_price(evidence.zero_gamma, 0)}"
                    )
                if _is_number(evidence.gross_gex) or _is_number(evidence.net_gex):
                    st.caption(
                        "Rejected-surface GEX: "
                        f"gross {_fmt_gex_units(evidence.gross_gex, 3)}, "
                        f"net {_fmt_gex_units(evidence.net_gex, 3)}"
                    )
            elif evidence is None or evidence.scope == "status_only":
                st.caption(
                    "No numeric live GEX calculation survived the gate; zero placeholders "
                    "are intentionally hidden."
                )

            if evidence is not None and _is_number(evidence.max_pain):
                source = evidence.max_pain_source or "source not reported"
                as_of = (
                    f", as of {evidence.max_pain_as_of}"
                    if evidence.max_pain_as_of
                    else ""
                )
                st.caption(
                    f"OI-only max-pain context: {_fmt_price(evidence.max_pain, 0)} "
                    f"({source}{as_of}); this is not a live GEX result or closing-price promise."
                )

            if evidence is not None:
                coverage_parts = []
                if _is_number(evidence.primary_pair_coverage_ratio):
                    coverage_parts.append(
                        f"primary-pair coverage {float(evidence.primary_pair_coverage_ratio) * 100.0:.1f}%"
                    )
                if (
                    evidence.paired_primary_pair_count is not None
                    and evidence.expected_primary_pair_count is not None
                ):
                    coverage_parts.append(
                        "paired/expected "
                        f"{evidence.paired_primary_pair_count}/{evidence.expected_primary_pair_count}"
                    )
                elif evidence.paired_quote_count is not None:
                    coverage_parts.append(f"paired quotes {evidence.paired_quote_count}")
                if evidence.fresh_quote_count is not None:
                    coverage_parts.append(f"fresh quotes {evidence.fresh_quote_count}")
                if evidence.primary_expiration:
                    coverage_parts.append(f"primary expiration {evidence.primary_expiration}")
                if evidence.calculation_id:
                    coverage_parts.append(f"calculation {evidence.calculation_id}")
                if coverage_parts:
                    st.caption("Coverage evidence: " + " | ".join(coverage_parts))

            if (
                evidence is None
                or not _is_number(evidence.spot)
                or not _is_number(evidence.gamma_pin)
            ):
                st.caption(
                    "For a separately labeled last-valid historical reference, use the "
                    "Frozen Gamma (Audit) tab. It is never substituted for this failed row."
                )

        # Fetch latest cached data from backend
        st.subheader("Latest Cached Data")

        if st.button("🔄 Reload from Cache", type="secondary", help="Repaint UI from backend memory - no live data fetch"):
            if not selected_indexes:
                st.warning("Please select indexes first!")
            else:
                try:
                    cache_batch = fetch_sidebar_symbol_states(
                        [INDEXES[idx_name] for idx_name in selected_indexes],
                        timeout_seconds=2.0,
                    )
                    for state in cache_batch.results:
                        if not state.usable:
                            _render_sidebar_state_failure(state)
                            continue
                        delta = (
                            f"Research target {state.predicted_close:,.2f}"
                            if state.predicted_close is not None
                            else None
                        )
                        st.metric(
                            f"{state.symbol} - {state.market_data_source_label}",
                            f"${state.current_price:,.2f}",
                            delta=delta,
                            help=(
                                "Lifecycle-verified cached state. Numeric values are hidden "
                                "when validation, freshness, or generation checks fail."
                            ),
                        )
                        _render_sidebar_state_context(state)

                    reload_summary = summarize_cache_reload(cache_batch)
                    getattr(st, reload_summary.level)(reload_summary.message)
                except Exception as exc:
                    st.error(f"Cache reload could not be classified: {exc}")

        # Snapshot data using Databento-backed proxy prices
        st.divider()
        st.caption("**Snapshot Data (Backend Cached Prices)**")
        if st.button("📸 Get Current Prices", type="secondary", help="Fetch latest prices via Databento-backed proxy data"):
            if selected_indexes:
                with st.spinner("Fetching snapshot data..."):
                    if st.session_state.data_provider == 'databento':
                        price_batch = fetch_sidebar_symbol_states(
                            [INDEXES[idx_name] for idx_name in selected_indexes],
                            timeout_seconds=2.0,
                        )
                        for state in price_batch.results:
                            if not state.usable:
                                _render_sidebar_state_failure(state)
                                continue
                            st.metric(
                                state.symbol,
                                f"${state.current_price:,.2f}",
                                help=(
                                    "Lifecycle-verified current-generation cached price. "
                                    "No forecast is used as a price change baseline."
                                ),
                            )
                            _render_sidebar_state_context(state)
                        price_summary = summarize_cache_reload(price_batch)
                        getattr(st, price_summary.level)(price_summary.message)
                    elif st.session_state.api_key:
                        snapshot_data = {}
                        tickers_to_fetch = [INDEX_POLYGON_TICKERS[INDEXES[idx]] for idx in selected_indexes]
                        snapshot_data = get_snapshot_data(st.session_state.api_key, tickers_to_fetch)
                        if snapshot_data:
                            st.success(f"✅ Fetched prices for {len(snapshot_data)} tickers")
                            cols = st.columns(len(snapshot_data))
                            for i, (ticker, data) in enumerate(snapshot_data.items()):
                                with cols[i]:
                                    if data['price']:
                                        st.metric(
                                            ticker,
                                            f"${data['price']:.2f}",
                                        )
                                        st.caption(
                                            f"Cached quote: {data.get('timestamp') or 'timestamp unavailable'}; "
                                            "no prior-close change is inferred from a forecast."
                                        )
                                    else:
                                        st.metric(ticker, "N/A")
                        else:
                            st.error("Failed to fetch snapshot data. Check provider configuration.")
            else:
                st.warning("Please select indexes first!")

        # Live Gamma Monitor
        st.divider()
        st.subheader("🧲 Live Gamma Pull Monitor")
        if st.button("📊 Analyze Gamma Pull", type="primary", help="Calculate real-time gamma pin and dealer hedging direction"):
            if st.session_state.data_provider == 'databento' and selected_indexes:
                with st.spinner("Reading Databento gamma cache from backend..."):
                    gamma_batch = fetch_sidebar_symbol_states(
                        [INDEXES[idx_name] for idx_name in selected_indexes],
                        timeout_seconds=2.0,
                    )
                    gamma_cols = st.columns(len(gamma_batch.results))
                    failed_gamma_states = []
                    for i, state in enumerate(gamma_batch.results):
                        with gamma_cols[i]:
                            if not state.usable:
                                failed_gamma_states.append(state)
                                st.markdown(f"**{state.symbol}**")
                                st.warning(
                                    f"⚠️ Diagnostic only ({diagnostic_state_label(state.state)})"
                                )
                                continue

                            spot = state.current_price
                            st.markdown(f"**{state.symbol}** ${spot:,.2f}")
                            st.caption(
                                f"{state.market_data_source_label} | lifecycle verified"
                            )
                            if state.predicted_close is not None:
                                estimate_label = (
                                    "Promoted TCBBO estimate"
                                    if state.tcbbo_promoted
                                    else "Research GEX close estimate"
                                )
                                st.metric(
                                    estimate_label,
                                    f"${state.predicted_close:,.0f}",
                                    f"{state.predicted_close - spot:+.1f} pts",
                                )
                                if not state.tcbbo_promoted:
                                    st.warning(
                                        "Legacy GEX heuristic only; not TCBBO promoted and "
                                        "not a trade instruction."
                                    )
                            if state.gamma_pin is not None:
                                st.metric(
                                    "Gamma Pin",
                                    f"${state.gamma_pin:,.0f}",
                                    f"{state.gamma_pin - spot:+.1f} pts",
                                )
                            else:
                                st.caption("Gamma pin: unavailable in the verified state")
                            if state.max_pain is not None:
                                st.caption(
                                    f"Max pain: ${state.max_pain:,.0f} "
                                    "(options-payout context, not a guaranteed close)"
                                )
                            if (
                                state.positive_gex_wall is not None
                                and state.negative_gex_wall is not None
                            ):
                                st.caption(
                                    f"Walls: +GEX ${state.positive_gex_wall:,.0f} | "
                                    f"-GEX ${state.negative_gex_wall:,.0f}"
                                )
                            if state.predicted_close is not None:
                                direction = (
                                    "above"
                                    if state.predicted_close > spot
                                    else "below"
                                    if state.predicted_close < spot
                                    else "at"
                                )
                                st.caption(
                                    f"Estimate location: {direction} spot; analytical context only."
                                )
                            _render_sidebar_state_context(state)
                    st.session_state.gamma_diagnostic_states = tuple(
                        failed_gamma_states
                    )
                    st.session_state.gamma_diagnostic_checked_at = (
                        gamma_batch.checked_at_utc
                    )
                    st.caption(
                        "Lifecycle state checked: "
                        f"{format_display_timestamp(gamma_batch.checked_at_utc, DISPLAY_TIMEZONE, format_string='%H:%M:%S %Z')}"
                    )
            elif st.session_state.api_key and selected_indexes:
                with st.spinner("Calculating gamma structure..."):
                    from polygon import RESTClient
                    import numpy as np
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
                        prices = {}
                        for r in results:
                            ticker = _agg_get(r, 'ticker')
                            value = _agg_get(r, 'value')
                            if ticker is not None:
                                prices[str(ticker).replace('I:', '')] = value
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
                                    details = _contract_get(c, 'details')
                                    if details:
                                        if _contract_get(details, 'expiration_date') == today:
                                            s = _contract_get(details, 'strike_price')
                                            if s is not None and low <= float(s) <= high:
                                                oi = _contract_get(c, 'open_interest', 0) or 0
                                                iv = _contract_get(c, 'implied_volatility', 0.25) or 0.25
                                                gamma = bs_gamma(spot, s, 0.003, iv if iv > 0 else 0.25)
                                                gex = gamma * oi * 100 * (spot**2) / 1e9
                                                contracts.append({'strike': float(s), 'type': 'call' if _contract_get(details, 'contract_type') == 'call' else 'put', 'gex': gex})
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

                    st.caption(f"Updated: {_display_clock()}")
            else:
                st.warning("Select indexes first. Polygon mode also requires a Polygon API key.")

        # Show tips
        with st.expander("💡 Streaming Architecture", expanded=False):
            st.markdown("""
            **Backend-Only Streaming Pattern**

            - Databento mode: FastAPI backend manages one Databento Live OPRA session
            - Polygon mode: FastAPI backend manages one Polygon WebSocket connection per API key
            - This prevents duplicate frontend WebSocket connections
            - Streamlit reads cached data via REST endpoints
            - Live data is buffered in ring buffers on the backend

            **Data Flow:**
            1. FastAPI backend connects to the selected provider
            2. Incoming data is cached and converted into pin/GEX metrics
            3. Streamlit polls the backend cache for latest values
            """)

    # === DEBUG SNAPSHOT VIEWER TAB ===
    with debug_snapshot_tab:
        st.markdown("**Raw Audit Snapshot Viewer** — Shows exact values from the latest audit JSON for debugging.")
        st.caption("When UI looks wrong, use this to prove whether backend stored wrong values or UI displayed wrong values.")

        load_debug_audit = st.checkbox(
            "Load debug audit", value=False, key="load_debug_audit",
            help="Read the latest retained audit only when investigating a calculation.",
        )
        if not load_debug_audit:
            st.caption("Debug audit loading is off.")
        elif selected_indexes:
            debug_symbol = st.selectbox("Select Index for Debug View", [INDEXES[idx] for idx in selected_indexes])

            if debug_symbol:
                raw_snapshot = load_latest_snapshot_data(debug_symbol)
                snapshot = dict_to_audit_snapshot(raw_snapshot) if raw_snapshot else None

                if snapshot is None:
                    st.warning(f"No snapshot available for {debug_symbol}")
                else:
                    # Show snapshot file path
                    st.code(raw_snapshot.get("_source_file", f"logs/audit/{debug_symbol}"), language="text")

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
                        st.info("Arithmetic consistency only: gross_gex >= |net_gex| and gross = call + put. Zero placeholders can pass; this does not establish valid market evidence.")
                    else:
                        st.error("❌ GEX invariant violated!")

                    st.subheader("Pin Competition")
                    pin_col1, pin_col2, pin_col3 = st.columns(3)
                    with pin_col1:
                        st.metric("primary_pin", f"{snapshot.primary_gamma_pin_strike:,.0f}")
                    with pin_col2:
                        runner_up = snapshot.pin_runner_up_strike
                        st.metric("runner_up_pin", f"{runner_up:,.0f}" if runner_up is not None else "N/A")
                    with pin_col3:
                        lead_ratio = snapshot.pin_lead_ratio
                        st.metric("primary_lead", f"{lead_ratio * 100.0:.1f}%" if lead_ratio is not None else "N/A")
                    if snapshot.pin_is_contested:
                        st.warning(
                            "⚠️ Raw primary pin is contested. "
                            + str(snapshot.pin_competition_reason or "The top two strikes are nearly tied.")
                        )
                    elif snapshot.pin_runner_up_strike is not None:
                        st.success("Primary pin has a decisive lead over the runner-up strike.")

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
                    from backend.validation_method import validation_labels
                    st.json(validation_labels(raw_snapshot))
                    if snapshot.validation_is_valid:
                        st.info("Producer marked this snapshot as passing its original checks. See the recorded validation method; accuracy and current eligibility are not established here.")
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
                        st.json(raw_snapshot)
        else:
            st.info("Select indexes in the sidebar to view debug snapshot data.")

    # === LIVE ADVISOR TAB ===
    with advisor_tab:
        from app.services.advisor_view import render_advisor
        advisor_symbols = [INDEXES[idx] for idx in selected_indexes] if selected_indexes else ["SPX", "NDX", "VIX"]
        render_advisor(advisor_symbols)

    st.divider()

    analyze_button = st.button("🔄 Analyze & Predict", type="primary", width='stretch')

    st.divider()

    # Backtesting section
    st.header("📊 Backtesting")
    backtest_available = (
        st.session_state.data_provider == 'polygon'
        and bool(st.session_state.api_key.strip())
    )
    run_backtest_btn = st.checkbox(
        "Enable Backtesting Mode",
        value=False,
        disabled=not backtest_available,
        help="The legacy historical test requires a separate Polygon connection.",
    )
    # A retained widget value must not enable a different provider's test.
    run_backtest_btn = backtest_available and run_backtest_btn
    if st.session_state.data_provider == 'databento':
        st.info(
            "Historical backtesting is unavailable in this Databento dashboard. "
            "The legacy test uses Polygon ETF prices and a different model; it "
            "does not measure the live Databento predictions."
        )
        st.caption(
            "Use Shadow Formula Research and Closing Tape evidence to review "
            "retained research. Accuracy requires matched outcomes and validated replay."
        )
    elif not backtest_available:
        st.info("Legacy backtesting requires a configured Polygon API key.")

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
    if st.session_state.data_provider == 'databento':
        st.caption(
            "Databento mode uses lifecycle-validated options evidence and evidence-gated "
            "estimates. Technical indicators remain unavailable unless verified observed "
            "intraday OHLCV bars are present. Estimates are not trade instructions."
        )
    else:
        st.caption(
            "This legacy mode uses technical indicators and machine learning to estimate "
            "closing prices. Estimates are not financial advice."
        )

# Main content
if st.session_state.data_provider == 'databento':
    render_forecast_research()
    render_closing_tape_evidence()

if st.session_state.data_provider == 'polygon' and not st.session_state.api_key:
    st.info("👈 Please enter your Polygon API key in the sidebar to get started, or switch Market Data Provider to Databento OPRA")
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
        st.session_state.last_analysis_attempt_symbols = tuple(
            INDEXES[index_name] for index_name in selected_indexes
        )

        progress_bar = st.progress(0)
        status_text = st.empty()
        if st.session_state.data_provider == 'databento':
            status_text.text("Reading the current Databento lifecycle snapshot…")
        else:
            # Fetch VIX data once (shared across all indexes)
            status_text.text("Fetching VIX data...")
            vix_df = fetch_vix_data(st.session_state.api_key, days_history)

            # Near-close timing changes bar granularity, not forecast authority.
            if is_near_market_close():
                st.info(
                    "Near-close research window detected. Using intraday bars where the "
                    "provider supplies verified observations."
                )
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
                            'indicators_available': True,
                            'indicator_status': 'available',
                            'indicator_provenance': {
                                'status': 'available',
                                'required_source_kind': 'observed_intraday_ohlcv',
                                'is_observed': True,
                                'is_synthetic': False,
                                'row_count': int(len(df_with_indicators)),
                                'reason': 'Indicators calculated from provider OHLCV bars.',
                            },
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
                            alert_threshold = locals().get('alert_threshold')
                            confidence_threshold = locals().get('confidence_threshold')
                            if (
                                enable_alerts
                                and alert_threshold is not None
                                and confidence_threshold is not None
                                and abs(change_pct) >= alert_threshold
                                and confidence >= confidence_threshold
                            ):
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
        if st.session_state.data_provider != 'databento':
            time.sleep(0.5)
        status_text.empty()
        progress_bar.empty()

        # Debug: Show prediction count
        if st.session_state.data_provider == 'databento':
            pass  # The single coherent lifecycle read below builds the candidates.
        elif st.session_state.predictions:
            st.info(
                f"Captured {len(st.session_state.predictions)} candidate result(s); "
                "current lifecycle identity is checked below."
            )
        else:
            st.error("⚠️ No predictions were generated - check data availability")

    # Display only predictions that remain current under the lifecycle contract.
    # Retained Streamlit state is preserved as evidence, but never kept in the
    # live decision surface after validation, freshness, or generation fails.
    display_predictions = dict(st.session_state.predictions)
    retained_prediction_states = []
    final_lifecycle_batch = None
    if (
        st.session_state.data_provider == 'databento'
        and selected_indexes
        and (analyze_button or display_predictions)
    ):
        # Fetch promotion metadata before capturing live state, so optional
        # metadata latency cannot age a captured forecast before its checks.
        promoted_predictions = fetch_promoted_closing_tape_predictions()
        # One all-symbol sample supplies both forecast values and their final
        # identity/health checks. A later backend publication cannot split them.
        final_lifecycle_batch = fetch_sidebar_symbol_states(
            [INDEXES[index_name] for index_name in selected_indexes],
            timeout_seconds=1.2,
        )
        st.session_state.gamma_diagnostic_states = tuple(
            state
            for state in final_lifecycle_batch.results
            if not state.prediction_usable
        )
        st.session_state.gamma_diagnostic_checked_at = (
            final_lifecycle_batch.checked_at_utc
        )
        predictions_by_symbol, prediction_messages = build_databento_predictions_from_state_batch(
            final_lifecycle_batch,
            timeframe=st.session_state.timeframe,
            stale_after_seconds=float(os.environ.get("LIVE_DATA_STALE_AFTER_SECONDS", "30")),
            promoted_predictions=promoted_predictions,
        )
        display_predictions = {
            index_name: predictions_by_symbol[INDEXES[index_name]]
            for index_name in selected_indexes
            if INDEXES[index_name] in predictions_by_symbol
        }
        st.session_state.predictions = dict(display_predictions)
        for message in prediction_messages:
            st.warning(message)
        if analyze_button and not display_predictions:
            st.error("⚠️ No predictions were generated - check data availability")
    if display_predictions and st.session_state.data_provider == 'databento':
        current_predictions = {}
        states_by_symbol = {
            state.symbol: state
            for state in (
                final_lifecycle_batch.results if final_lifecycle_batch is not None else ()
            )
        }
        for index_name, prediction in display_predictions.items():
            symbol = str(prediction.get('ticker') or '').upper()
            state = states_by_symbol.get(symbol)
            if index_name not in selected_indexes:
                identity_matches = False
                mismatch_reason = "Index is no longer selected"
            elif state is None:
                identity_matches = False
                mismatch_reason = "No current lifecycle state was returned"
            else:
                identity_matches, mismatch_reason = retained_prediction_matches_state(
                    prediction,
                    state,
                )
            if identity_matches:
                current_predictions[index_name] = prediction
            else:
                retained_prediction_states.append(
                    {
                        'Index': index_name,
                        'Symbol': symbol or 'unknown',
                        'State': state.state if state is not None else 'unverified',
                        'Reason': (
                            mismatch_reason
                        ),
                        'Source as of (UTC)': (
                            state.source_as_of_utc if state is not None else None
                        ),
                        'Forecast ID': prediction.get('forecast_id'),
                        'Retained generation': prediction.get('subscription_generation'),
                        'Current generation': (
                            state.subscription_generation if state is not None else None
                        ),
                        'Retained revision': prediction.get('state_revision'),
                        'Current revision': state.state_revision if state is not None else None,
                        'Checked at (UTC)': (
                            final_lifecycle_batch.checked_at_utc
                            if final_lifecycle_batch is not None
                            else None
                        ),
                    }
                )
        display_predictions = current_predictions

    if retained_prediction_states:
        st.warning(
            "Retained prediction results failed current lifecycle checks and were moved "
            "out of the live decision surface. AI analysis and exports exclude them."
        )
        with st.expander("Retained historical/unavailable prediction state", expanded=False):
            st.dataframe(
                pd.DataFrame(retained_prediction_states),
                hide_index=True,
                width='stretch',
            )

    if st.session_state.data_provider == 'databento':
        selected_symbols = {
            INDEXES[index_name]
            for index_name in selected_indexes
        }
        forecast_symbols = {
            INDEXES[index_name]
            for index_name in display_predictions
            if index_name in INDEXES
        }
        context_state_candidates = (
            final_lifecycle_batch.results
            if final_lifecycle_batch is not None
            else st.session_state.get("gamma_diagnostic_states", ())
        )
        context_states_by_symbol = {
            state.symbol: state
            for state in context_state_candidates
            if getattr(state, "symbol", None) in selected_symbols
            and state.symbol not in forecast_symbols
        }
        context_symbols = symbols_requiring_retained_context(
            [INDEXES[index_name] for index_name in selected_indexes],
            forecast_symbols=forecast_symbols,
            state_symbols=set(context_states_by_symbol),
            lifecycle_check_completed=final_lifecycle_batch is not None,
        )
        if context_symbols:
            st.subheader("⚠️ Excluded symbol diagnostics — not predictions")
            st.caption(
                "These observations come from the same final lifecycle check used for the "
                "prediction cards. They remain visible for audit and historical context, "
                "but cannot enter "
                "prediction cards, AI analysis, alerts, or validated exports. Run the "
                "analysis again to refresh them."
            )
            context_date = _current_snapshot_display_date()
            for symbol in context_symbols:
                state = context_states_by_symbol.get(symbol)
                context_label = (
                    diagnostic_state_label(state.state)
                    if state is not None and not state.usable
                    else "No eligible forecast"
                )
                with st.expander(
                    f"⚠️ {symbol}: {context_label}",
                    expanded=True,
                ):
                    if state is None:
                        st.error(
                            f"{symbol}: no current lifecycle state was returned for this "
                            "analysis attempt."
                        )
                    elif state.usable:
                        st.warning(
                            f"{symbol}: current lifecycle evidence exists, but no eligible "
                            "end-of-day forecast was returned."
                        )
                        if _is_number(state.current_price):
                            st.metric(
                                "Current OPRA parity reference",
                                _fmt_price(state.current_price, 2),
                            )
                        _render_sidebar_state_context(state)
                    else:
                        _render_sidebar_state_failure(state)
                        _render_sidebar_diagnostic_evidence(state)
                    st.error("End-of-day close estimate: Unavailable — ABSTAIN")
                    parity_history = build_opra_parity_gamma_history(
                        _cached_local_snapshot_selection(
                            'exports',
                            symbol,
                            context_date,
                            DISPLAY_TIMEZONE.name,
                        ).records
                    )
                    render_retained_opra_parity_history(
                        parity_history,
                        symbol=symbol,
                        local_date=context_date,
                        display_timezone=DISPLAY_TIMEZONE,
                        key_prefix="excluded_opra_parity_history",
                        expanded=True,
                    )
            checked_at = st.session_state.get("gamma_diagnostic_checked_at")
            if checked_at:
                st.caption(
                    "Diagnostic lifecycle state checked: "
                    f"{format_display_timestamp(checked_at, DISPLAY_TIMEZONE, format_string='%b %d, %Y %I:%M:%S %p %Z')}"
                )

    # Display predictions
    if display_predictions:
        # Timing describes this manual page snapshot, not a live countdown.
        remaining_close_seconds = near_close_seconds()
        if remaining_close_seconds is not None and market_session_state == "open":
            st.warning(near_close_caption(remaining_close_seconds))
            st.info(
                "Near-close timing does not make an estimate reliable. Freshness, validation, "
                "generation, and promotion gates still apply."
            )
        else:
            import pytz
            et_tz = pytz.timezone('US/Eastern')
            current_et = datetime.now(et_tz)
            st.info(
                f"Current ET Time: {current_et.strftime('%I:%M %p')} | "
                "Research timing follows the scheduled session close."
            )

        # --- Searchable predictions panel ---
        with st.expander("🔍 Search Predictions", expanded=False):
            st.caption("Filter predictions by symbol, type, direction, or confidence. Select a row to pin it below.")
            _pred_search = st.text_input(
                "Search predictions",
                key="prediction_search_query",
                placeholder="e.g. SPX, SPY, ETF, bullish, HIGH ...",
            ).strip().lower()
            _pred_rows = []
            for _idx_name, _pred in display_predictions.items():
                _sym = str(_pred.get('ticker') or _idx_name).upper()
                _cur = _pred.get('current_price')
                _tgt = _pred.get('predicted_price')
                _chg = _pred.get('change_pct')
                _conf = _pred.get('confidence')
                _bias = "bullish" if (isinstance(_chg,(int,float)) and _chg and _chg>=0) else ("bearish" if isinstance(_chg,(int,float)) and _chg else "neutral")
                _type = "ETF" if _sym in ETF_TICKER_SEARCH else "Index"
                _pred_rows.append({
                    "Symbol": _sym, "Name": _idx_name, "Type": _type,
                    "Spot": _cur, "Predicted": _tgt, "Move %": _chg,
                    "Bias": _bias, "Confidence": _conf,
                })
            _pred_df = pd.DataFrame(_pred_rows)
            if _pred_search and not _pred_df.empty:
                _mask = _pred_df.astype(str).apply(
                    lambda row: row.str.contains(_pred_search, case=False, na=False).any(), axis=1)
                _pred_df = _pred_df[_mask]
            if _pred_df.empty:
                st.info("No predictions match your search.")
            else:
                st.dataframe(_pred_df, hide_index=True, use_container_width=True)
                _sel = st.dataframe(
                    _pred_df[["Symbol","Name","Predicted","Move %"]],
                    on_select="rerun", selection_mode="single-row",
                    key="prediction_search_select", hide_index=True,
                )
                _rows = _sel.get("selection", {}).get("rows", [])
                if _rows:
                    _picked = _pred_df.iloc[_rows[0]]
                    st.success(f"Selected **{_picked['Symbol']}** — predicted close "
                               f"${float(_picked['Predicted']):,.2f} ({float(_picked['Move %']):+.2f}%, "
                               f"{_picked['Bias']}, conf {_picked['Confidence']})")

        st.header("📊 Prediction Results")

        # Export options
        col_export1, col_export2, col_export3 = st.columns([2, 2, 6])
        with col_export1:
            csv_data = export_to_csv(display_predictions, include_indicators=False)
            if csv_data:
                st.download_button(
                    label="📥 Export Summary (CSV)",
                    data=csv_data,
                    file_name=f"predictions_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        with col_export2:
            csv_full = export_to_csv(display_predictions, include_indicators=True)
            if csv_full:
                st.download_button(
                    label="📥 Export Full Data (CSV)",
                    data=csv_full,
                    file_name=f"prediction_display_frame_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )

        with col_export3:
            st.caption("Export to Excel-compatible CSV format")

        # Summary cards
        cols = st.columns(len(display_predictions))

        for col, (index_name, pred) in zip(cols, display_predictions.items()):
            with col:
                change_color = "🟢" if pred['change_pct'] >= 0 else "🔴"
                st.metric(
                    label=f"{change_color} {index_name}",
                    value=f"${pred['predicted_price']:.2f}",
                    delta=f"{pred['change_pct']:.2f}%"
                )
                st.caption(f"Current: ${pred['current_price']:.2f}")
                st.caption(_confidence_display(pred))
                source_label = pred.get("source_label") or _prediction_source_label(pred)
                if pred.get("is_fallback"):
                    st.warning(f"{source_label}: not a live OPRA trade signal")
                else:
                    st.caption(f"Source: {source_label}")
                model_type = str(pred.get('model_type') or 'Model identity unavailable')
                model_version = str(pred.get('model_version') or 'version unavailable')
                st.caption(f"Model: {model_type} ({model_version})")
                if pred.get('decision_grade') is True:
                    st.success("Passport decision-grade checks passed for this estimate.")
                else:
                    st.warning(
                        "Research-only estimate: decision-grade passport or TCBBO promotion "
                        "evidence has not passed."
                    )

                if pred.get('confidence_calibrated') is not True:
                    st.caption("No empirical forecast-probability calibration is claimed.")

                # Display non-directional options-positioning structure if available.
                if 'gex_data' in pred and pred['gex_data']:
                    gex = pred['gex_data']
                    expiration_view = gex.get('expiration_view') or pred.get('expiration_view') or {}
                    primary_expiration = (
                        expiration_view.get('primary_expiration')
                        if isinstance(expiration_view, dict)
                        else None
                    ) or pred.get('primary_expiration')
                    primary_scope = _expiration_scope_label(
                        primary_expiration,
                        is_primary=True,
                        context_only=bool(
                            (
                                expiration_view.get("primary_expiration_context_only")
                                if isinstance(expiration_view, dict)
                                else None
                            )
                            or gex.get("primary_expiration_context_only")
                            or pred.get("primary_expiration_context_only")
                        ),
                    )
                    if _is_number(gex.get('pin_strike')):
                        st.markdown(
                            f"**📍 Gamma Pin — {primary_scope}:** "
                            f"${gex['pin_strike']:.0f}"
                        )
                        st.caption(
                            _structural_distance_from_spot(
                                gex.get('pin_strike'), pred.get('current_price')
                            )
                        )
                        if gex.get('pin_is_contested'):
                            runner_up = gex.get('pin_runner_up_strike')
                            lead_ratio = gex.get('pin_lead_ratio')
                            details = []
                            if runner_up is not None:
                                details.append(f"runner-up ${float(runner_up):,.0f}")
                            if lead_ratio is not None:
                                details.append(f"lead {float(lead_ratio) * 100.0:.1f}%")
                            st.warning("Contested raw pin" + (": " + ", ".join(details) if details else ""))
                    if _is_number(gex.get('max_pain_strike')):
                        st.markdown(
                            f"**💰 Max Pain — {primary_scope}:** "
                            f"${gex['max_pain_strike']:.0f}"
                        )
                        st.caption(
                            _structural_distance_from_spot(
                                gex.get('max_pain_strike'), pred.get('current_price')
                            )
                        )

        st.divider()

        # Options-positioning levels are non-directional context, not destinations.
        st.header("🎯 Options Positioning Levels — Non-Directional Structure")
        st.caption(
            "Gamma pin and max pain are analytical estimates from the available chain. "
            "Their location relative to spot does not predict direction, prove dealer intent, "
            "or guarantee an intraday or expiration close."
        )

        mm_cols = st.columns(len(display_predictions))

        for mm_col, (index_name, pred) in zip(mm_cols, display_predictions.items()):
            with mm_col:
                current_price = pred['current_price']
                gex = pred.get('gex_data', {})

                gamma_pin = gex.get('pin_strike')
                max_pain = gex.get('max_pain_strike')
                expiration_view = gex.get('expiration_view') or pred.get('expiration_view') or {}
                expiration_rows = (
                    expiration_view.get('rows')
                    if isinstance(expiration_view, dict)
                    else []
                ) or []
                primary_expiration = (
                    expiration_view.get('primary_expiration')
                    if isinstance(expiration_view, dict)
                    else None
                ) or pred.get('primary_expiration')
                primary_scope = _expiration_scope_label(
                    primary_expiration,
                    is_primary=True,
                    context_only=bool(
                        (
                            expiration_view.get("primary_expiration_context_only")
                            if isinstance(expiration_view, dict)
                            else None
                        )
                        or gex.get("primary_expiration_context_only")
                        or pred.get("primary_expiration_context_only")
                    ),
                )

                st.subheader(index_name)

                if gamma_pin or max_pain:
                    level_col1, level_col2 = st.columns(2)
                    with level_col1:
                        st.markdown(f"**📍 Gamma Pin — {primary_scope}**")
                        st.markdown(_fmt_price(gamma_pin, 0))
                        st.caption(
                            _structural_distance_from_spot(gamma_pin, current_price)
                        )
                    with level_col2:
                        st.markdown(f"**💰 Max Pain — {primary_scope}**")
                        st.markdown(_fmt_price(max_pain, 0))
                        st.caption(
                            _structural_distance_from_spot(max_pain, current_price)
                        )
                        st.caption(
                            "Options-payout context only; not a settlement forecast."
                        )
                else:
                    st.info(f"{primary_scope}: structural levels unavailable.")

                shadow_rows = [
                    row
                    for row in expiration_rows
                    if isinstance(row, dict) and row.get('is_primary') is False
                ]
                if shadow_rows:
                    st.markdown("**Shadow expiration structure (exact dates)**")
                    st.dataframe(
                        pd.DataFrame(
                            [
                                {
                                    'Scope': _expiration_scope_label(
                                        row.get('expiration'), is_primary=False
                                    ),
                                    'Gamma Pin': _fmt_price(row.get('gamma_pin'), 0),
                                    'Max Pain': _fmt_price(row.get('max_pain'), 0),
                                    'Status': str(
                                        row.get('validation_status') or 'unreported'
                                    ).replace('_', ' '),
                                }
                                for row in shadow_rows
                            ]
                        ),
                        hide_index=True,
                        width='stretch',
                    )
                    st.caption(
                        "SHADOW expirations are analytical comparisons only; they are not "
                        "production close targets and do not imply direction."
                    )

                # Verified lifecycle/promoted close forecast remains a separate surface.
                st.divider()
                st.markdown("**📋 Verified Close Forecast Signals**")
                try:
                    close_resp = requests.get(
                        "http://localhost:8000/predict/close-overlay",
                        params={"symbol": pred.get('ticker', index_name)},
                        timeout=8,
                    )
                    if close_resp.ok:
                        close_data = close_resp.json()
                        signals = close_data.get('signals', [])

                        if signals:
                            for sig in signals[:4]:  # Show top 4 signals
                                st.markdown(f"<span style='font-size: 0.85em;'>{sig['emoji']} {sig['message']}</span>", unsafe_allow_html=True)

                            # Net bias comes only from the verified close-overlay contract.
                            net_bias = close_data.get('net_bias', 'neutral')
                            bias_emoji = "📈" if net_bias == 'bullish' else "📉" if net_bias == 'bearish' else "↔️"
                            drift = close_data.get('drift_adjustment', 0)
                            expected = close_data.get('expected_close', 0)

                            try:
                                expected_value = float(expected)
                            except (TypeError, ValueError):
                                expected_value = 0.0

                            if expected_value:
                                st.markdown(f"**{bias_emoji} Expected Close:** ${expected_value:,.0f} ({float(drift):+.1f} pts drift)")
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

        st.divider()

        # AI Portfolio Risk Assessment
        if enable_ai and display_predictions:
            st.header("🎯 AI Portfolio Risk Assessment")
            with st.spinner("Analyzing overall market risk..."):
                risk_assessment = get_risk_assessment(display_predictions)
                if risk_assessment:
                    st.warning(f"**Risk Analysis:** {risk_assessment}")

        st.divider()

        # Detailed charts
        st.header("📈 Detailed Analysis")

        for index_name, pred in display_predictions.items():
            with st.expander(f"📊 {index_name} ({pred['ticker']}) - Detailed Chart", expanded=True):
                # Data source indicator - warn if using ETF fallback
                data_source = pred.get('data_source', 'unknown')
                ticker_used = pred.get('ticker_used', pred['ticker'])
                if data_source == 'etf_fallback':
                    st.warning(f"⚠️ Using ETF fallback data ({ticker_used}) - predictions may be less accurate")
                elif data_source == 'index':
                    st.success(f"✓ Using direct index data ({ticker_used})")

                indicator_frame = pred.get('df')
                required_indicator_columns = {
                    'RSI', 'MACD', 'Signal_Line', 'SMA_20',
                    'Momentum', 'VWAP', 'AMA',
                }
                indicators_available = (
                    isinstance(indicator_frame, pd.DataFrame)
                    and not indicator_frame.empty
                    and required_indicator_columns.issubset(indicator_frame.columns)
                    and pred.get('indicators_available') is True
                    and (pred.get('indicator_provenance') or {}).get('is_observed') is True
                    and (pred.get('indicator_provenance') or {}).get('is_synthetic') is False
                )
                latest_data = indicator_frame.iloc[-1] if indicators_available else None

                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Current OPRA parity reference", f"${pred['current_price']:.2f}")
                with col2:
                    st.metric("Predicted EOD", f"${pred['predicted_price']:.2f}")
                with col3:
                    st.metric("Expected Change", f"{pred['change_pct']:.2f}%")

                # AI Analysis
                if enable_ai and latest_data is not None:
                    with st.spinner("Getting AI analysis..."):
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
                            vix_val,
                            confidence_kind=pred.get('confidence_kind'),
                            confidence_calibrated=pred.get('confidence_calibrated') is True,
                        )

                        if ai_analysis:
                            st.info(f"🤖 **AI Analysis:** {ai_analysis}")
                elif enable_ai:
                    st.info(
                        "AI technical-indicator analysis is disabled because no verified "
                        "observed intraday OHLCV bars are available."
                    )

                if latest_data is not None:
                    # Create and display a chart only from a verified observed frame.
                    fig = create_price_chart(indicator_frame, pred['predicted_price'], index_name)
                    st.plotly_chart(fig, width='stretch')

                    st.subheader("Technical Indicators Summary")
                    tech_col1, tech_col2, tech_col3, tech_col4 = st.columns(4)

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
                else:
                    indicator_reason = (
                        (pred.get('indicator_provenance') or {}).get('reason')
                        or 'Verified observed intraday OHLCV bars are unavailable.'
                    )
                    st.subheader("Observed Technical Indicators")
                    st.info(f"Unavailable: {indicator_reason}")

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

                    # Main non-directional gamma and max-pain structure.
                    expiration_view = gex.get('expiration_view') or pred.get('expiration_view') or {}
                    primary_expiration = (
                        expiration_view.get('primary_expiration')
                        if isinstance(expiration_view, dict)
                        else None
                    ) or pred.get('primary_expiration')
                    primary_scope = _expiration_scope_label(
                        primary_expiration,
                        is_primary=True,
                        context_only=bool(
                            (
                                expiration_view.get("primary_expiration_context_only")
                                if isinstance(expiration_view, dict)
                                else None
                            )
                            or gex.get("primary_expiration_context_only")
                            or pred.get("primary_expiration_context_only")
                        ),
                    )
                    gamma_col1, gamma_col2, gamma_col3, gamma_col4 = st.columns(4)

                    with gamma_col1:
                        if 'pin_strike' in gex and _is_number(gex.get('pin_strike')):
                            st.metric(
                                f"📍 Gamma Pin — {primary_scope}",
                                _fmt_price(gex.get('pin_strike'), 0),
                            )
                            st.caption(
                                _structural_distance_from_spot(
                                    gex.get('pin_strike'), pred.get('current_price')
                                )
                            )

                    with gamma_col2:
                        if 'max_pain_strike' in gex and _is_number(gex.get('max_pain_strike')):
                            st.metric(
                                f"💰 Max Pain — {primary_scope}",
                                _fmt_price(gex.get('max_pain_strike'), 0),
                            )
                            st.caption(
                                _structural_distance_from_spot(
                                    gex.get('max_pain_strike'), pred.get('current_price')
                                )
                            )

                    with gamma_col3:
                        if 'total_gex' in gex and _is_number(gex.get('total_gex')):
                            st.metric("Gross GEX (raw)", _fmt_gex_units(gex['total_gex'], 1), help="Raw gamma × OI × 100 exposure; not dollars")
                            if 'net_gex' in gex and _is_number(gex.get('net_gex')):
                                st.caption(f"Net: {_fmt_gex_units(gex['net_gex'], 1)}")

                    with gamma_col4:
                        if 'zero_gamma' in gex and _is_number(gex.get('zero_gamma')):
                            st.metric(ZERO_GAMMA_DISPLAY_LABEL, _fmt_price(gex.get('zero_gamma'), 0))
                        else:
                            st.metric(ZERO_GAMMA_DISPLAY_LABEL, "N/A")
                        st.caption("Structural regime context; no direction is implied.")

                    expiration_rows = expiration_view.get('rows') if isinstance(expiration_view, dict) else []
                    if expiration_rows:
                        st.markdown("**Expiration structure (exact dates)**")
                        status_labels = {
                            'valid_primary': 'Valid primary',
                            'valid_profile_context': 'Validated context',
                            'valid_primary_context': 'Validated primary forward context',
                            'primary_gate_unreported': 'Primary gate unreported',
                            'primary_context_gate_unreported': 'Primary context gate unreported',
                            'calculated_context': 'Calculated context',
                            'invalid': 'Invalid',
                            'unavailable': 'Unavailable',
                        }
                        expiration_display_rows = []
                        for row in expiration_rows:
                            coverage = row.get('calculation_coverage_ratio')
                            quote_coverage = row.get('quote_coverage_ratio')
                            quote_age = row.get('quote_age_seconds')
                            calculated_contracts = row.get('calculated_contracts')
                            planned_contracts = row.get('planned_contracts')
                            contract_coverage = "N/A"
                            if _is_number(calculated_contracts) or _is_number(planned_contracts):
                                used = f"{int(float(calculated_contracts))}" if _is_number(calculated_contracts) else "?"
                                planned = f"{int(float(planned_contracts))}" if _is_number(planned_contracts) else "?"
                                contract_coverage = f"{used}/{planned}"
                            expiration_display_rows.append({
                                'Expiration': row.get('expiration') or 'Unavailable',
                                'Scope': _expiration_scope_label(
                                    row.get('expiration'),
                                    is_primary=row.get('is_primary') is True,
                                    context_only=row.get('context_only') is True,
                                ),
                                'Role': row.get('role') or 'Unreported',
                                'Mode': row.get('mode') or 'Unreported',
                                'Status': status_labels.get(
                                    str(row.get('validation_status')),
                                    str(row.get('validation_status') or 'Unreported'),
                                ),
                                'Gamma Pin': _fmt_price(row.get('gamma_pin'), 0),
                                'Max Pain': _fmt_price(row.get('max_pain'), 0),
                                '1st Strike-Bucket Crossing': _fmt_price(
                                    row.get('first_strike_bucket_gex_sign_crossing'), 0
                                ),
                                'Gross GEX (raw)': (
                                    _fmt_gex_units(row.get('gross_gex'))
                                    if _is_number(row.get('gross_gex')) else 'N/A'
                                ),
                                'Net GEX (raw)': (
                                    _fmt_gex_units(row.get('net_gex'))
                                    if _is_number(row.get('net_gex')) else 'N/A'
                                ),
                                'Calculated/Planned': contract_coverage,
                                'Subscription': row.get('subscription_status') or 'Unverified',
                                'Calculation Coverage': (
                                    f"{float(coverage):.1%}" if _is_number(coverage) else 'Unavailable'
                                ),
                                'Quote Coverage': (
                                    f"{float(quote_coverage):.1%}"
                                    if _is_number(quote_coverage) else 'Unavailable'
                                ),
                                'Freshness': (
                                    f"{row.get('freshness_status', 'unknown')} "
                                    f"({float(quote_age):.1f}s, {row.get('freshness_scope', 'unknown')})"
                                    if _is_number(quote_age)
                                    else f"{row.get('freshness_status', 'unknown')} (shared payload)"
                                ),
                                'OI As Of (UTC)': row.get('open_interest_as_of_utc') or 'Unavailable',
                                'OI Source End (UTC)': (
                                    row.get('open_interest_source_end_utc') or 'Unavailable'
                                ),
                                'Reason': '; '.join(row.get('validation_reasons') or []),
                            })
                        st.dataframe(
                            pd.DataFrame(expiration_display_rows),
                            hide_index=True,
                            width='stretch',
                        )
                        st.caption(
                            "Calculation coverage is calculated contracts divided by planned contracts; "
                            "it does not verify active subscriptions or quote coverage. Only the primary row receives the current "
                            "production validation gate. Tomorrow, max-pain, and other cross-expiration values "
                            "are analytical context—not guaranteed targets or evidence of dealer intent."
                        )
                        if any(
                            row.get('validation_status') in {'invalid', 'unavailable'}
                            for row in expiration_rows
                        ):
                            st.warning(
                                "One or more subscribed expirations lack usable calculated evidence; "
                                "unavailable fields were left as N/A."
                            )
                        malformed_entries = int(
                            expiration_view.get('invalid_profile_entries') or 0
                        ) + int(expiration_view.get('invalid_subscription_entries') or 0)
                        if malformed_entries:
                            st.warning(
                                f"{malformed_entries} malformed expiration metadata entr"
                                f"{'y was' if malformed_entries == 1 else 'ies were'} omitted."
                            )

                        provenance = expiration_view.get('provenance') or {}
                        provenance_parts = []
                        if provenance.get('mode'):
                            provenance_parts.append(f"mode={provenance['mode']}")
                        if provenance.get('source_date'):
                            provenance_parts.append(f"universe source date={provenance['source_date']}")
                        if provenance.get('source_sha256'):
                            provenance_parts.append(
                                f"source hash={str(provenance['source_sha256'])[:12]}…"
                            )
                        if provenance.get('selected_universe_sha256'):
                            provenance_parts.append(
                                "selected hash="
                                f"{str(provenance['selected_universe_sha256'])[:12]}…"
                            )
                        if provenance_parts:
                            st.caption("Universe provenance: " + " | ".join(provenance_parts))
                        if provenance.get('is_fallback'):
                            st.warning(
                                "Universe provenance uses a fallback scaffold: "
                                + str(provenance.get('reason') or 'reason unavailable')
                            )
                        if expiration_view.get('open_interest_as_of_utc') is None:
                            st.caption(
                                "Open-interest as-of time was not reported and is not inferred from the "
                                "universe cache date or provider statistics query boundary."
                            )
                    else:
                        st.info("Exact-date expiration profiles are unavailable for this payload.")

                    # Show gamma walls table if available
                    if 'gamma_walls' in gex and not gex['gamma_walls'].empty:
                        st.write("**Major Gamma Walls (Top Strike Levels):**")

                        # Format the gamma walls dataframe
                        gamma_walls_display = normalize_gamma_walls(gex['gamma_walls'])

                        if 'strike' in gamma_walls_display.columns:
                            gamma_walls_display['strike'] = gamma_walls_display['strike'].apply(lambda x: f"${float(x):.0f}" if pd.notna(x) else "")
                        if 'net_gex' in gamma_walls_display.columns:
                            gamma_walls_display['net_gex'] = gamma_walls_display['net_gex'].apply(lambda x: _fmt_gex_units(x) if pd.notna(x) else "")
                        if 'total_gex' in gamma_walls_display.columns:
                            gamma_walls_display['total_gex'] = gamma_walls_display['total_gex'].apply(lambda x: _fmt_gex_units(x) if pd.notna(x) else "N/A")
                        if 'days_to_expiry' in gamma_walls_display.columns:
                            def _format_days(row):
                                value = row.get('days_to_expiry')
                                if not pd.notna(value):
                                    return "N/A"
                                label = f"{float(value):.0f} days"
                                if pd.notna(row.get('expiration_count')) and float(row['expiration_count']) > 1:
                                    label += " avg"
                                return label
                            gamma_walls_display['days_to_expiry'] = gamma_walls_display.apply(_format_days, axis=1)

                        gamma_walls_display = gamma_walls_display.rename(columns={
                            'strike': 'Strike Price',
                            'net_gex': 'Net GEX',
                            'total_gex': 'Total GEX',
                            'days_to_expiry': 'Days to Expiry',
                            'expiration_count': 'Expiries'
                        })

                        st.dataframe(gamma_walls_display.fillna('N/A'), width='stretch')
                        st.caption("Side sums cover calculated contracts only. Missing or unverified zero sides are N/A; net values can represent partial coverage.")

                    # Create gamma exposure bar chart if we have strike-level data
                    gex_df = pd.DataFrame()
                    if 'gex_by_strike' in gex:
                        import plotly.graph_objects as go

                        gex_df = gex['gex_by_strike']
                        if isinstance(gex_df, list):
                            gex_df = pd.DataFrame(gex_df)
                        else:
                            gex_df = gex_df.copy()
                        if 'net_gex' not in gex_df.columns and 'gex' in gex_df.columns:
                            gex_df['net_gex'] = gex_df['gex']
                        if gex_df.empty or 'strike' not in gex_df.columns or 'net_gex' not in gex_df.columns:
                            gex_df = pd.DataFrame()
                    if 'gex_by_strike' in gex and not gex_df.empty:

                        # Create bar chart showing gamma exposure by strike
                        fig_gex = go.Figure()

                        # Add net GEX bars
                        fig_gex.add_trace(go.Bar(
                            x=gex_df['strike'],
                            y=gex_df['net_gex'],
                            name='Net GEX',
                            marker_color=['green' if x > 0 else 'red' for x in gex_df['net_gex']],
                            text=[_fmt_gex_units(abs(x), 1) for x in gex_df['net_gex']],
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
                            yaxis_title="Net Gamma Exposure (raw gamma × OI × 100 units)",
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
                show_gamma_evolution_section(
                    pred['ticker'],
                    index_name,
                    unique_suffix=index_name,
                    display_timezone=DISPLAY_TIMEZONE,
                )

                # Display VWAP and AMA only when they came from observed bars.
                st.subheader("📊 Advanced Indicators")
                if latest_data is None:
                    st.info(
                        "VWAP, AMA, and related indicators are unavailable until a verified "
                        "observed intraday OHLCV source is connected. No synthetic substitute is used."
                    )
                else:
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

    elif selected_indexes:
        selected_symbol_tuple = tuple(INDEXES[index_name] for index_name in selected_indexes)
        if st.session_state.get("last_analysis_attempt_symbols") == selected_symbol_tuple:
            st.warning(
                "Analysis completed, but no eligible end-of-day close forecast is "
                "available. Review the per-symbol ABSTAIN diagnostics and retained "
                "non-predictive context above."
            )
        else:
            st.info("👆 Click 'Analyze & Predict' to generate predictions for selected indexes")

    # Backtesting execution
    run_backtest_analysis = bool(locals().get('run_backtest_analysis', False))
    backtest_start = locals().get('backtest_start')
    backtest_end = locals().get('backtest_end')
    backtest_model = locals().get('backtest_model', st.session_state.selected_model)

    if (
        st.session_state.data_provider == 'polygon'
        and bool(st.session_state.api_key.strip())
        and run_backtest_btn
        and run_backtest_analysis
        and selected_indexes
        and backtest_start
        and backtest_end
        and backtest_model
    ):
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
    from app.services.closing_prices import render_closing_prices
    render_closing_prices([INDEXES[idx] for idx in selected_indexes] if selected_indexes else ["SPX", "NDX", "VIX"])
    timezone_source = "browser" if DISPLAY_TIMEZONE.source == "browser" else "explicit UTC fallback"
    st.caption(
        "View and download stored intraday research snapshots. Calculation validation "
        "does not establish current live, prediction, training, or complete opening-range eligibility. "
        f"Displayed times use the {timezone_source}: "
        f"{display_timezone_label(DISPLAY_TIMEZONE)}; persisted/exported timestamps remain UTC."
    )

    # Get available export files
    export_base = "./exports"
    available_symbols = list_gamma_snapshot_symbols(export_base)

    if available_symbols:
        # Add "All Indices" option at the start
        symbol_options = ["📦 All Indices"] + available_symbols

        export_col1, export_col2 = st.columns(2)

        with export_col1:
            export_symbol = st.selectbox("Select Index", symbol_options, key="export_symbol_select")

        with export_col2:
            # UTC filenames are storage partitions, not viewer-local dates.
            if export_symbol == "📦 All Indices":
                local_dates = set()
                for sym in available_symbols:
                    local_dates.update(
                        _cached_available_snapshot_dates(
                            export_base,
                            sym,
                            DISPLAY_TIMEZONE.name,
                        )
                    )
                available_files = sorted(local_dates, reverse=True)
                symbol_dir = None
            else:
                symbol_dir = os.path.join(export_base, export_symbol)
                available_files = _cached_available_snapshot_dates(
                    export_base,
                    export_symbol,
                    DISPLAY_TIMEZONE.name,
                )

            if available_files:
                export_date = st.selectbox(
                    "Select Local Date",
                    available_files,
                    key="export_date_select",
                    help=f"Calendar date in {display_timezone_label(DISPLAY_TIMEZONE)}.",
                )
            else:
                export_date = None
                st.info("No export files found")

        # Handle "All Indices" view
        if export_date and export_symbol == "📦 All Indices":
            st.subheader(f"📦 Snapshot evidence for {export_date}")

            # Stored calculation validity supports research, not live authority.
            all_records = []
            evidence_counts = {}
            malformed_count = 0

            for sym in available_symbols:
                selection = _cached_local_snapshot_selection(
                    export_base,
                    sym,
                    export_date,
                    DISPLAY_TIMEZONE.name,
                )
                malformed_count += selection.malformed_timestamp_records
                if selection.records:
                    for snap in selection.records:
                        all_records.append({**snap, '_symbol': sym})
                    evidence_counts[sym] = len(selection.records)

            all_records.sort(
                key=lambda snap: (
                    parse_utc_timestamp(
                        _snapshot_timestamp_value(snap)
                    ),
                    snap.get('_symbol', ''),
                )
            )

            from app.services.validation_review import render_validation_review
            all_records = render_validation_review(all_records, 'all_snapshot_validation')
            evidence = partition_snapshot_evidence(all_records)
            usable_snapshots = [
                snap for snap in all_records
                if gamma_snapshot_provenance_status(snap) != DIAGNOSTIC_INVALID_SNAPSHOT
            ]
            historical_snapshots = list(evidence.historical_records)
            diagnostic_records = list(evidence.diagnostic_records)
            usable_counts = {
                sym: sum(1 for snap in usable_snapshots if snap.get('_symbol') == sym)
                for sym in available_symbols
            }
            usable_counts = {sym: count for sym, count in usable_counts.items() if count}

            sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)
            with sum_col1:
                st.metric("Research Gamma Snapshots", len(usable_snapshots))
            with sum_col2:
                st.metric("Failed Records in Export Files", len(diagnostic_records))
            with sum_col3:
                st.metric("Total Evidence Records", len(all_records))
            with sum_col4:
                st.metric("Symbols With Research Data", len(usable_counts))
            st.caption('These totals cover export files only. Select an individual symbol to inspect retained audit attempts and failures through the close, which are stored separately.')

            if usable_snapshots:
                st.success(
                    f"Stored gamma research is available for: {', '.join(usable_counts)}."
                )
            elif diagnostic_records:
                st.error(
                    "No calculation-valid research snapshots were recorded for this local date. "
                    "The retained rows are failure diagnostics only and are excluded from "
                    "training and predictions; they remain separately downloadable."
                )
            else:
                st.info("No snapshot or diagnostic evidence was found for this local date.")

            if evidence_counts:
                st.caption(
                    "Retained evidence per symbol: "
                    + ", ".join(f'{key}: {value}' for key, value in evidence_counts.items())
                )
            if all_records:
                st.caption(
                    "Recorded coverage: "
                    + format_display_timestamp(_snapshot_timestamp_value(all_records[0]), DISPLAY_TIMEZONE)
                    + " to "
                    + format_display_timestamp(_snapshot_timestamp_value(all_records[-1]), DISPLAY_TIMEZONE)
                    + ". Counts do not establish continuous opening coverage; missing observations are not reconstructed."
                )
            if any(payload_has_fallback_provenance(snap) for snap in all_records):
                st.warning("FALLBACK RESEARCH: retained records include fallback universe or open-interest data. See per-record provenance and original source dates in the research downloads.")
            if usable_counts:
                st.caption(
                    "Research snapshots per symbol: "
                    + ", ".join(f'{key}: {value}' for key, value in usable_counts.items())
                )
            st.caption(
                "Research calculation validity requires validation_is_valid=true and "
                "gamma_excluded_from_model=false. A recorded "
                "subscription identity does not establish current live or prediction eligibility."
            )
            if historical_snapshots and usable_snapshots:
                st.warning(
                    f"Retained {len(historical_snapshots)} otherwise-valid historical row(s) "
                    "with an absent or malformed process identity. They are excluded from "
                    "current live evidence and training eligibility."
                )
            if malformed_count:
                st.warning(
                    f"Found {malformed_count} records with no parseable UTC timestamp while "
                    "scanning the relevant UTC partitions; they could not be assigned to a "
                    "local date."
                )

            if usable_snapshots:
                with st.expander("View Research Gamma Snapshots", expanded=False):
                    display_data = []
                    for snap in usable_snapshots:
                        snap_time = _snapshot_timestamp_value(snap)
                        time_display = format_display_timestamp(
                            snap_time,
                            DISPLAY_TIMEZONE,
                            format_string='%I:%M:%S %p %Z',
                        )
                        gamma_pin_val = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike')
                        display_data.append({
                            'Symbol': snap.get('_symbol', ''),
                            'Time': time_display,
                            'Spot': f"${snap.get('spot_last', 0):,.2f}",
                            'Gamma Pin': f"${gamma_pin_val:,.0f}" if gamma_pin_val else 'N/A',
                            'Gross GEX': _fmt_gex_units(snap.get('gross_gex', 0), 3),
                            'Net GEX': _fmt_gex_units(snap.get('net_gex', 0), 3),
                            'Calculation Valid': '✅',
                            'Research Provenance': gamma_snapshot_provenance_status(snap).replace('_', ' ').capitalize(),
                        })
                    st.dataframe(pd.DataFrame(display_data), hide_index=True, width='stretch')

            if diagnostic_records:
                with st.expander("View Failed Audit Records", expanded=False):
                    diagnostic_display = []
                    for snap in diagnostic_records:
                        reasons = snap.get('validation_failure_reasons') or []
                        diagnostic_display.append({
                            'Symbol': snap.get('_symbol', ''),
                            'Time': format_display_timestamp(
                                _snapshot_timestamp_value(snap),
                                DISPLAY_TIMEZONE,
                                format_string='%I:%M:%S %p %Z',
                            ),
                            'Failure Reasons': '; '.join(str(reason) for reason in reasons)
                            or str(snap.get('pregate_reason') or 'Unavailable'),
                            'Spot': snap.get('spot_last'),
                            'Gross GEX': snap.get('gross_gex'),
                            'Net GEX': snap.get('net_gex'),
                        })
                    st.dataframe(
                        pd.DataFrame(diagnostic_display),
                        hide_index=True,
                        width='stretch',
                    )

                diagnostic_content = '\n'.join(
                    json.dumps(snapshot_research_export_record(record), separators=(',', ':'), default=str)
                    for record in diagnostic_records
                ) + '\n'
                st.download_button(
                    "🧾 Download Failed Audit Evidence",
                    diagnostic_content,
                    f"failed_gamma_audits_{export_date}.ndjson",
                    "application/x-ndjson",
                )

            st.subheader("📥 Download Research Evidence")
            if usable_snapshots:
                _render_eod_zip_download(
                    export_date, DISPLAY_TIMEZONE, key='snapshot_export_zip',
                )
            else:
                st.button("📦 No calculation-valid research data to package", disabled=True)

        elif export_date and symbol_dir:
            from app.services.capture_review import render_capture_review
            render_capture_review(export_symbol, export_date, DISPLAY_TIMEZONE, Path(__file__).resolve().parent)
            selection = _cached_local_snapshot_selection(
                export_base,
                export_symbol,
                export_date,
                DISPLAY_TIMEZONE.name,
            )
            from app.services.validation_review import render_validation_review
            all_records = render_validation_review(list(selection.records), 'symbol_snapshot_validation')
            evidence = partition_snapshot_evidence(all_records)
            snapshots = [
                snap for snap in all_records
                if gamma_snapshot_provenance_status(snap) != DIAGNOSTIC_INVALID_SNAPSHOT
            ]
            historical_records = list(evidence.historical_records)
            diagnostic_records = list(evidence.diagnostic_records)

            if historical_records:
                st.warning(
                    f"{len(historical_records)} otherwise-valid {export_symbol} historical "
                    "row(s) have no canonical process identity. They remain historical / "
                    "unverified and remain available for research."
                )

            if selection.malformed_timestamp_records:
                st.warning(
                    f"Found {selection.malformed_timestamp_records} records with no parseable "
                    "UTC timestamp while scanning the relevant UTC partitions; they could not "
                    "be assigned to a local date."
                )

            if diagnostic_records:
                if not snapshots:
                    st.error(
                        f"No calculation-valid {export_symbol} research snapshots were recorded on "
                        f"{export_date}. The {len(diagnostic_records)} retained rows are failure "
                        "diagnostics only."
                    )
                else:
                    st.warning(
                        f"Excluded {len(diagnostic_records)} failed {export_symbol} audit records "
                        "from calculation-valid research downloads; separate evidence remains available."
                    )
                with st.expander("View Failed Audit Records", expanded=False):
                    diagnostic_display = []
                    for snap in diagnostic_records:
                        reasons = snap.get('validation_failure_reasons') or []
                        diagnostic_display.append({
                            'Time': format_display_timestamp(
                                _snapshot_timestamp_value(snap),
                                DISPLAY_TIMEZONE,
                                format_string='%I:%M:%S %p %Z',
                            ),
                            'Failure Reasons': '; '.join(str(reason) for reason in reasons)
                            or str(snap.get('pregate_reason') or 'Unavailable'),
                            'Spot': snap.get('spot_last'),
                            'Gross GEX': snap.get('gross_gex'),
                            'Net GEX': snap.get('net_gex'),
                        })
                    st.dataframe(
                        pd.DataFrame(diagnostic_display),
                        hide_index=True,
                        width='stretch',
                    )
                diagnostic_content = '\n'.join(
                    json.dumps(snapshot_research_export_record(record), separators=(',', ':'), default=str)
                    for record in diagnostic_records
                ) + '\n'
                st.download_button(
                    f"🧾 Download {export_symbol} Failed Audit Evidence",
                    diagnostic_content,
                    f"{export_symbol}_failed_audits_{export_date}.ndjson",
                    "application/x-ndjson",
                )

            if any(payload_has_fallback_provenance(snap) for snap in all_records):
                st.warning("FALLBACK RESEARCH: retained records include fallback universe or open-interest data. Downloads retain provenance identities while redacting machine-local paths; current authority is not established.")
            if snapshots:
                st.success(
                    f"Found {len(snapshots)} research gamma snapshots for "
                    f"{export_symbol} on {export_date}."
                )

                # Summary metrics
                if snapshots:
                    sum_col1, sum_col2, sum_col3, sum_col4 = st.columns(4)

                    first_snap = snapshots[0]
                    last_snap = snapshots[-1]

                    with sum_col1:
                        first_time = _snapshot_timestamp_value(first_snap)
                        first_time_display = format_display_timestamp(
                            first_time,
                            DISPLAY_TIMEZONE,
                            format_string='%I:%M:%S %p %Z',
                        )
                        st.metric("First Snapshot", first_time_display)
                    with sum_col2:
                        last_time = _snapshot_timestamp_value(last_snap)
                        last_time_display = format_display_timestamp(
                            last_time,
                            DISPLAY_TIMEZONE,
                            format_string='%I:%M:%S %p %Z',
                        )
                        st.metric("Last Valid Research Snapshot", last_time_display)
                    with sum_col3:
                        st.metric("Research Snapshots", len(snapshots))
                    with sum_col4:
                        # Support both field names for backward compatibility
                        final_pin = last_snap.get('primary_gamma_pin_strike') or last_snap.get('gamma_pin_strike')
                        if final_pin:
                            st.metric("Latest Recorded Gamma Pin", f"${final_pin:,.0f}")
                        else:
                            st.metric("Final Pin", "N/A")

                    # Detailed view
                    with st.expander("View Snapshot Details", expanded=False):
                        display_data = []
                        for snap in snapshots:
                            snap_time = _snapshot_timestamp_value(snap)
                            time_display = format_display_timestamp(
                                snap_time,
                                DISPLAY_TIMEZONE,
                                format_string='%I:%M:%S %p %Z',
                            )
                            # Support both field names for backward compatibility
                            gamma_pin_val = snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike')
                            display_data.append({
                                'Time': time_display,
                                'Spot': f"${snap.get('spot_last', 0):,.2f}",
                                'Gamma Pin': f"${gamma_pin_val:,.0f}" if gamma_pin_val else 'N/A',
                                'Gross GEX': _fmt_gex_units(snap.get('gross_gex', 0), 3),
                                'Net GEX': _fmt_gex_units(snap.get('net_gex', 0), 3),
                                'Producer Check': snapshot_research_export_fields(snap)['review_calculation_status'],
                                'Validation Method': snapshot_research_export_fields(snap)['review_validation_method'],
                                'Research Provenance': gamma_snapshot_provenance_status(snap).replace('_', ' ').capitalize(),
                            })
                        st.dataframe(pd.DataFrame(display_data), hide_index=True, width='stretch')

                    # Download buttons row
                    file_content = '\n'.join(
                        json.dumps(snapshot_research_export_record(record),
                                   separators=(',', ':'), default=str)
                        for record in snapshots
                    ) + '\n'

                    dl_col1, dl_col2 = st.columns(2)
                    with dl_col1:
                        st.download_button(
                            label=f"📥 Download {export_symbol}_{export_date}.ndjson",
                            data=file_content,
                            file_name=f"{export_symbol}_{export_date}.ndjson",
                            mime="application/x-ndjson"
                        )
                    with dl_col2:
                        st.info(
                            "Model research is evidence-gated. Use "
                            "tools/evaluate_closing_tape_models.py with a replay-verified "
                            "surface manifest and verified official-close labels."
                        )

                    with st.popover("📖 Evidence-gated workflow"):
                        st.markdown("""
1. Finalize retained closing tapes and ingest verified official-close artifacts.
2. Build and replay-verify an immutable research-surface manifest.
3. Run chronological walk-forward evaluation:
```bash
python tools/evaluate_closing_tape_models.py --surface-manifest <manifest> --device cuda
```
Candidate packaging requires explicit output and artifact paths. It never promotes a model to production automatically.
                        """)

                    # CSV Export row
                    st.caption("CSV Exports")
                    csv_col1, csv_col2 = st.columns(2)

                    with csv_col1:
                        # Convert current snapshots to CSV
                        csv_data = []
                        for snap in snapshots:
                            timestamp_utc = _snapshot_timestamp_value(snap)
                            csv_data.append({
                                'symbol': export_symbol,
                                'date': export_date,
                                'timestamp_utc': timestamp_utc,
                                'timestamp_local': format_display_timestamp(
                                    timestamp_utc,
                                    DISPLAY_TIMEZONE,
                                    format_string='%Y-%m-%d %H:%M:%S %Z',
                                ),
                                'display_timezone': DISPLAY_TIMEZONE.name,
                                'spot': snap.get('spot_last', 0),
                                'gamma_pin': snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike'),
                                'gross_gex': snap.get('gross_gex', 0),
                                'net_gex': snap.get('net_gex', 0),
                                'call_gex': snap.get('call_gex', 0),
                                'put_gex': snap.get('put_gex', 0),
                                'max_pain': snap.get('max_pain_strike', None),
                                'is_valid': snap.get('validation_is_valid', False),
                                'subscription_epoch_id': snap.get('subscription_epoch_id'),
                                'subscription_generation': snap.get('subscription_generation'),
                                'universe_provenance': json.dumps(snap.get('universe_provenance') or {}, separators=(',', ':')),
                                'oi_analytics_provenance': json.dumps(snap.get('oi_analytics_provenance') or {}, separators=(',', ':')),
                                **snapshot_research_export_fields(snap),
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
                            other_selection = _cached_local_snapshot_selection(
                                export_base,
                                sym,
                                export_date,
                                DISPLAY_TIMEZONE.name,
                            )
                            for snap in other_selection.records:
                                if gamma_snapshot_provenance_status(snap) == DIAGNOSTIC_INVALID_SNAPSHOT:
                                    continue
                                timestamp_utc = _snapshot_timestamp_value(snap)
                                all_symbols_data.append({
                                    'symbol': sym,
                                    'date': export_date,
                                    'timestamp_utc': timestamp_utc,
                                    'timestamp_local': format_display_timestamp(
                                        timestamp_utc,
                                        DISPLAY_TIMEZONE,
                                        format_string='%Y-%m-%d %H:%M:%S %Z',
                                    ),
                                    'display_timezone': DISPLAY_TIMEZONE.name,
                                    'spot': snap.get('spot_last', 0),
                                    'gamma_pin': snap.get('primary_gamma_pin_strike') or snap.get('gamma_pin_strike'),
                                    'gross_gex': snap.get('gross_gex', 0),
                                    'net_gex': snap.get('net_gex', 0),
                                    'call_gex': snap.get('call_gex', 0),
                                    'put_gex': snap.get('put_gex', 0),
                                    'max_pain': snap.get('max_pain_strike', None),
                                    'is_valid': snap.get('validation_is_valid', False),
                                    'subscription_epoch_id': snap.get('subscription_epoch_id'),
                                    'subscription_generation': snap.get('subscription_generation'),
                                    'universe_provenance': json.dumps(snap.get('universe_provenance') or {}, separators=(',', ':')),
                                    'oi_analytics_provenance': json.dumps(snap.get('oi_analytics_provenance') or {}, separators=(',', ':')),
                                    **snapshot_research_export_fields(snap),
                                })

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
                if diagnostic_records:
                    st.button("📦 No calculation-valid research data to package", disabled=True)
                else:
                    st.info(
                        f"No {export_symbol} snapshots fall within {export_date} in "
                        f"{display_timezone_label(DISPLAY_TIMEZONE)}."
                    )
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
                            'Actual Price': f"${p.actual_price:.2f}" if p.actual_price is not None else 'Pending',
                            'Accuracy': f"{p.accuracy:.1f}%" if p.accuracy is not None else 'N/A'
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
