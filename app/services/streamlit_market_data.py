"""Market data helpers for the Streamlit dashboard."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import streamlit as st
from polygon.rest import RESTClient

from options_gamma import get_gamma_analysis
from app.utils.settings import settings

INDEX_POLYGON_TICKERS = {
    "SPX": "I:SPX",
    "NDX": "I:NDX",
    "DJI": "I:DJI",
    "RUT": "I:RUT",
    "VIX": "I:VIX",
}

INDEX_ETFS = {
    "SPX": "SPY",
    "NDX": "QQQ",
    "DJI": "DIA",
    "RUT": "IWM",
    "VIX": "UVXY",
}

DATABENTO_SYMBOLS = {
    "SPX": "ES.c.0",
    "NDX": "NQ.c.0",
    "DJI": "YM.c.0",
    "RUT": "RTY.c.0",
}


def _resolve_market_data_provider(
    polygon_api_key: Optional[str],
    databento_api_key: Optional[str],
) -> Optional[str]:
    """Choose the best available dashboard price-data provider."""
    provider = (settings.market_data_provider or "auto").strip().lower()

    if provider == "databento":
        return "databento" if databento_api_key else ("polygon" if polygon_api_key else None)
    if provider == "polygon":
        return "polygon" if polygon_api_key else ("databento" if databento_api_key else None)
    if polygon_api_key:
        return "polygon"
    if databento_api_key:
        return "databento"
    return None


def _normalize_market_df(df: pd.DataFrame, data_source: str, ticker_used: str) -> pd.DataFrame:
    """Normalize OHLCV data shape and annotate source metadata."""
    if df.empty:
        return df

    normalized = df.sort_values("timestamp").reset_index(drop=True)
    if "volume" in normalized.columns:
        normalized["volume"] = normalized["volume"].replace(0, 1).fillna(1)

    normalized.attrs["data_source"] = data_source
    normalized.attrs["ticker_used"] = ticker_used
    return normalized


def _fetch_polygon_market_data(api_key: str, ticker: str, days: int = 60, use_index: bool = True) -> pd.DataFrame:
    """Fetch historical market data from Polygon with index-first fallback."""
    client = RESTClient(api_key)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    polygon_ticker = ticker
    is_index_data = False
    data_source = "direct"

    if use_index and ticker in INDEX_POLYGON_TICKERS:
        polygon_ticker = INDEX_POLYGON_TICKERS[ticker]
        is_index_data = True
        data_source = "index"
    elif use_index and ticker in INDEX_ETFS:
        polygon_ticker = INDEX_POLYGON_TICKERS.get(ticker, f"I:{ticker}")
        is_index_data = True
        data_source = "index"

    aggs_list = []
    try:
        aggs = client.get_aggs(
            ticker=polygon_ticker,
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            timespan="day",
            multiplier=1,
            adjusted=True,
            sort="asc",
            limit=50000,
        )
        if aggs:
            aggs_list = list(aggs)
    except Exception:
        if is_index_data and ticker in INDEX_ETFS:
            etf_ticker = INDEX_ETFS[ticker]
            st.warning(
                f"⚠️ INDEX DATA UNAVAILABLE for {polygon_ticker}, falling back to ETF {etf_ticker} - predictions may be less accurate"
            )
            print(f"[DATA SOURCE WARNING] Index data not available for {polygon_ticker}, falling back to {etf_ticker}")
            data_source = "etf_fallback"
            aggs = client.get_aggs(
                ticker=etf_ticker,
                from_=start_date.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
                timespan="day",
                multiplier=1,
                adjusted=True,
                sort="asc",
                limit=50000,
            )
            if aggs:
                aggs_list = list(aggs)

    if len(aggs_list) == 0 and is_index_data and ticker in INDEX_ETFS:
        etf_ticker = INDEX_ETFS[ticker]
        st.warning(
            f"⚠️ NO INDEX DATA for {polygon_ticker}, falling back to ETF {etf_ticker} - predictions may be less accurate"
        )
        print(f"[DATA SOURCE WARNING] No index data for {polygon_ticker}, trying ETF {etf_ticker}")
        data_source = "etf_fallback"
        aggs = client.get_aggs(
            ticker=etf_ticker,
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            timespan="day",
            multiplier=1,
            adjusted=True,
            sort="asc",
            limit=50000,
        )
        if aggs:
            aggs_list = list(aggs)

    data = []
    for agg in aggs_list:
        data.append(
            {
                "timestamp": datetime.fromtimestamp(agg.timestamp / 1000),
                "open": agg.open,
                "high": agg.high,
                "low": agg.low,
                "close": agg.close,
                "volume": agg.volume if agg.volume else 1,
            }
        )

    return _normalize_market_df(
        pd.DataFrame(data),
        data_source=data_source,
        ticker_used=polygon_ticker if data_source == "index" else INDEX_ETFS.get(ticker, ticker),
    )


def _fetch_databento_market_data(api_key: str, ticker: str, days: int = 60) -> pd.DataFrame:
    """Fetch historical daily bars from Databento futures proxies."""
    proxy_symbol = DATABENTO_SYMBOLS.get(ticker)
    if not proxy_symbol:
        return pd.DataFrame()

    try:
        import databento as db
    except Exception as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("databento package is not installed") from exc

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 5)

    client = db.Historical(api_key)
    store = client.timeseries.get_range(
        dataset="GLBX.MDP3",
        schema="ohlcv-1d",
        stype_in="continuous",
        symbols=[proxy_symbol],
        start=start_date.strftime("%Y-%m-%d"),
        end=(end_date + timedelta(days=1)).strftime("%Y-%m-%d"),
    )
    raw_df = store.to_df()
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    df = raw_df.reset_index()
    timestamp_col = next(
        (column for column in ("ts_event", "ts_recv", "timestamp") if column in df.columns),
        None,
    )
    if timestamp_col is None:
        timestamp_series = pd.Series(pd.to_datetime(df.index), index=df.index)
    else:
        timestamp_series = pd.to_datetime(df[timestamp_col])

    if getattr(timestamp_series.dt, "tz", None) is not None:
        timestamp_series = timestamp_series.dt.tz_localize(None)

    normalized = pd.DataFrame(
        {
            "timestamp": timestamp_series,
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["close"],
            "volume": df["volume"] if "volume" in df.columns else 1,
        }
    )

    return _normalize_market_df(
        normalized,
        data_source="databento_futures_proxy",
        ticker_used=proxy_symbol,
    )

def fetch_vix_data(api_key, days=60):
    """Fetch VIX (Volatility Index) data from Polygon"""
    if not api_key:
        return None

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
    if not api_key:
        return {
            'data_unavailable': True,
            'underlying': ticker,
            'summary': 'Gamma data unavailable: Polygon options API key is not configured',
            'options_root': ticker,
            'is_etf_proxy': False,
        }

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
                'pin_expiry': gex_analysis['pin_expiry'],
                'zero_gamma': gex_analysis['zero_gamma'],
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

def fetch_market_data(api_key, ticker, days=60, use_index=True, databento_api_key=None):
    """Fetch historical market data from the configured live provider.
    
    Args:
        api_key: Polygon API key
        ticker: Base ticker symbol (e.g., 'SPX', 'NDX', 'SPY')
        days: Number of days of history
        use_index: If True, try direct index data (I:SPX) first, then fall back to ETF
        databento_api_key: Optional Databento key for futures-proxy fallback
        
    Returns:
        DataFrame with source metadata in attrs
    """
    provider = _resolve_market_data_provider(api_key, databento_api_key)
    if provider is None:
        st.error("Error fetching data: no Polygon or Databento API key is configured")
        return None

    try:
        if provider == "polygon":
            return _fetch_polygon_market_data(api_key, ticker, days=days, use_index=use_index)

        df = _fetch_databento_market_data(databento_api_key, ticker, days=days)
        if df.empty and api_key:
            st.warning(
                f"⚠️ Databento returned no {ticker} history, falling back to Polygon direct index data"
            )
            return _fetch_polygon_market_data(api_key, ticker, days=days, use_index=use_index)
        return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def get_current_price(api_key, ticker, databento_api_key=None):
    """Get the latest price using the configured market-data providers.

    Args:
        api_key: Polygon API key
        ticker: Base ticker symbol (e.g., 'SPX', 'NDX', 'SPY')
        databento_api_key: Optional Databento key for futures-proxy fallback
    """
    try:
        df = fetch_market_data(
            api_key,
            ticker,
            days=5,
            use_index=True,
            databento_api_key=databento_api_key,
        )
        if df is not None and not df.empty:
            return float(df['close'].iloc[-1])
        return None
    except Exception as e:
        st.error(f"Error fetching current price: {str(e)}")
        return None
