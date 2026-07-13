"""Market data helpers for the Streamlit dashboard."""

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from polygon.rest import RESTClient

from options_gamma import get_gamma_analysis

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

