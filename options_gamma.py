"""
Options Gamma Exposure (GEX) Analysis Module
Calculates real gamma exposure from options chain data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import norm
from polygon import RESTClient
import streamlit as st

def black_scholes_gamma(S, K, T, r, sigma):
    """
    Calculate Black-Scholes gamma for an option
    
    S: Spot price
    K: Strike price
    T: Time to expiry (in years)
    r: Risk-free rate
    sigma: Implied volatility
    """
    if T <= 0 or sigma <= 0:
        return 0
    
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
    return gamma

def fetch_options_chain(api_key, underlying, spot_price, days_ahead=30):
    """
    Fetch REAL options chain data from Polygon Snapshot API with actual OI and IV
    
    Returns tuple: (DataFrame with strike/expiry/type/OI/IV/gamma, is_mock_data: bool)
    
    Uses Options Chain Snapshot API for real-time open interest and implied volatility.
    """
    try:
        client = RESTClient(api_key)
        
        # Use Options Chain Snapshot API to get REAL OI and IV
        # This endpoint returns actual market data for all contracts on the underlying
        snapshot = client.list_snapshot_options_chain(underlying)
        
        options_data = []
        
        # Process each contract in the snapshot
        for contract in snapshot:
            try:
                # Extract contract details
                if not hasattr(contract, 'details'):
                    continue
                    
                details = contract.details
                strike = float(details.strike_price) if hasattr(details, 'strike_price') else 0
                expiry_str = details.expiration_date if hasattr(details, 'expiration_date') else ''
                option_type = details.contract_type.lower() if hasattr(details, 'contract_type') else ''
                
                if strike <= 0 or not expiry_str:
                    continue
                
                # Parse expiry
                expiry = datetime.strptime(expiry_str, '%Y-%m-%d')
                days_to_expiry = (expiry - datetime.now()).days
                
                # Only include contracts expiring within our window
                if days_to_expiry < 0 or days_to_expiry > days_ahead:
                    continue
                
                # Get REAL market data from snapshot
                # These are actual values from the market, not estimates!
                oi = int(contract.open_interest) if hasattr(contract, 'open_interest') and contract.open_interest else 0
                iv = float(contract.implied_volatility) if hasattr(contract, 'implied_volatility') and contract.implied_volatility else 0.25
                
                # Calculate gamma using real IV
                T = max(days_to_expiry, 0.001) / 365.0  # Avoid division by zero
                gamma = black_scholes_gamma(spot_price, strike, T, 0.05, iv)
                
                options_data.append({
                    'strike': strike,
                    'expiry': expiry,
                    'days_to_expiry': days_to_expiry,
                    'type': option_type,
                    'open_interest': oi,
                    'implied_volatility': iv,
                    'gamma': gamma,
                    'ticker': details.ticker if hasattr(details, 'ticker') else ''
                })
            except Exception as contract_error:
                # Skip malformed contracts
                continue
        
        # If we got real data, return it
        if options_data:
            return pd.DataFrame(options_data), False  # is_mock_data = False - REAL DATA!
        else:
            # No data found, fall back to mock
            st.warning(f"No options snapshot data returned for {underlying}. Using simulated data.")
            return create_mock_options_chain(underlying, spot_price), True
        
    except Exception as e:
        # API error - fall back to mock data
        error_msg = str(e)
        if "not found" in error_msg.lower() or "404" in error_msg:
            st.warning(f"Options snapshot not available for {underlying}. Using simulated data. Error: {error_msg[:100]}")
        else:
            st.warning(f"Options snapshot API error for {underlying}. Using simulated data. Error: {error_msg[:100]}")
        
        return create_mock_options_chain(underlying, spot_price), True

def create_mock_options_chain(underlying, spot_price):
    """
    Create realistic mock options chain for demonstration
    """
    strikes = []
    data = []
    
    # Generate strikes around current price
    for pct in range(-10, 11, 1):  # -10% to +10% in 1% increments
        strike = round(spot_price * (1 + pct/100))
        strikes.append(strike)
    
    # Generate options data for next 3 expirations
    expiry_dates = [
        datetime.now(),                       # 0DTE (today)
        datetime.now() + timedelta(days=7),   # Weekly
        datetime.now() + timedelta(days=30),  # Monthly
    ]
    
    for expiry in expiry_dates:
        days_to_exp = max(0, (expiry - datetime.now()).days)  # Floor at 0
        # For 0DTE, use fractional day (hours remaining)
        if days_to_exp == 0:
            hours_remaining = max(1, 16 - datetime.now().hour)  # Assume 4 PM close
            T = hours_remaining / (365.0 * 24.0)  # Convert to years
        else:
            T = days_to_exp / 365.0
        
        for strike in strikes:
            moneyness = spot_price / strike
            
            # Higher OI near the money
            # Boost 0DTE significantly to ensure it's selected
            if days_to_exp == 0:
                base_oi = 50000 * np.exp(-abs(moneyness - 1) * 15)  # Much higher for 0DTE
            else:
                base_oi = 10000 * np.exp(-abs(moneyness - 1) * 20)
            
            # Calls
            call_oi = int(base_oi * np.random.uniform(0.8, 1.2))
            call_iv = 0.15 + abs(moneyness - 1) * 0.5  # IV smile
            call_gamma = black_scholes_gamma(spot_price, strike, T, 0.05, call_iv)
            
            data.append({
                'strike': strike,
                'expiry': expiry,
                'days_to_expiry': days_to_exp,
                'type': 'call',
                'open_interest': call_oi,
                'implied_volatility': call_iv,
                'gamma': call_gamma
            })
            
            # Puts
            put_oi = int(base_oi * np.random.uniform(0.6, 1.0))
            put_iv = 0.15 + abs(1/moneyness - 1) * 0.5
            put_gamma = black_scholes_gamma(spot_price, strike, T, 0.05, put_iv)
            
            data.append({
                'strike': strike,
                'expiry': expiry,
                'days_to_expiry': days_to_exp,
                'type': 'put',
                'open_interest': put_oi,
                'implied_volatility': put_iv,
                'gamma': put_gamma
            })
    
    return pd.DataFrame(data)

def choose_effective_expiry(now_et, expiries, total_oi_by_expiry, oi_min=50_000):
    """
    Choose the most relevant expiry for gamma calculations.
    Prefers 0DTE during market hours, otherwise next expiry with sufficient OI.
    """
    from datetime import time
    
    # Prefer same-day 0DTE during cash session
    same_day = [e for e in expiries if e.date() == now_et.date()]
    if same_day and now_et.time() <= time(16, 0) and total_oi_by_expiry.get(same_day[0], 0) >= oi_min:
        return same_day[0]
    
    # Otherwise pick earliest future expiry with OI above threshold
    future = sorted([e for e in expiries if e.date() >= now_et.date()])
    for e in future:
        if total_oi_by_expiry.get(e, 0) >= oi_min:
            return e
    
    # Fallback to earliest expiry if none meet OI threshold
    return min(future) if future else None

def calculate_gamma_exposure(options_df, spot_price):
    """
    Calculate net gamma exposure by strike and identify pin levels
    
    Returns dictionary with:
    - pin_strike: Strike with highest absolute GEX
    - pin_expiry: Expiry date of pin
    - total_gex: Total gamma exposure at pin
    - direction: Whether pin is above/below spot
    - gex_by_strike: DataFrame of GEX by strike
    - gamma_walls: Top strikes by absolute GEX
    """
    
    CONTRACT_MULTIPLIER = 100
    
    # Calculate GEX for each option
    options_df['gex'] = options_df.apply(
        lambda row: row['gamma'] * row['open_interest'] * CONTRACT_MULTIPLIER * (spot_price ** 2) / 1e9,  # In billions
        axis=1
    )
    
    # Apply sign convention: 
    # Calls: positive if dealers are short (customers long)
    # Puts: negative if dealers are short (customers long)
    options_df['signed_gex'] = options_df.apply(
        lambda row: row['gex'] if row['type'] == 'call' else -row['gex'],
        axis=1
    )
    
    # Get current time in ET
    import pytz
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Calculate total OI by expiry
    oi_by_expiry = options_df.groupby('expiry')['open_interest'].sum()
    
    # Choose effective expiry (prefer 0DTE during market hours)
    expiries = options_df['expiry'].unique()
    effective_expiry = choose_effective_expiry(now_et, expiries, oi_by_expiry)
    
    # Filter to effective expiry if found
    if effective_expiry:
        filtered_df = options_df[options_df['expiry'] == effective_expiry]
    else:
        filtered_df = options_df
    
    # Aggregate by strike for the selected expiry
    gex_by_strike = filtered_df.groupby('strike').agg({
        'signed_gex': 'sum',
        'gex': lambda x: abs(x).sum(),  # Total absolute GEX
        'expiry': 'first',
        'days_to_expiry': 'min'
    }).reset_index()
    
    gex_by_strike.columns = ['strike', 'net_gex', 'total_gex', 'expiry', 'days_to_expiry']
    
    # Find pin strike (highest absolute GEX)
    pin_row = gex_by_strike.loc[gex_by_strike['total_gex'].idxmax()]
    pin_strike = pin_row['strike']
    pin_expiry = pin_row['expiry']
    total_gex = pin_row['total_gex']
    net_gex = pin_row['net_gex']
    
    # Direction of pull
    direction = 'above' if pin_strike > spot_price else 'below' if pin_strike < spot_price else 'at'
    
    # Get top gamma walls (top 5 strikes by absolute GEX)
    gamma_walls = gex_by_strike.nlargest(5, 'total_gex')[['strike', 'net_gex', 'total_gex', 'days_to_expiry']]
    
    # Calculate cumulative GEX for gamma flip level
    gex_by_strike_sorted = gex_by_strike.sort_values('strike')
    gex_by_strike_sorted['cumulative_gex'] = gex_by_strike_sorted['net_gex'].cumsum()
    
    # Find zero gamma level (where cumulative GEX crosses zero)
    positive_to_negative = gex_by_strike_sorted[
        (gex_by_strike_sorted['cumulative_gex'].shift(1) > 0) & 
        (gex_by_strike_sorted['cumulative_gex'] <= 0)
    ]
    
    zero_gamma_level = positive_to_negative['strike'].iloc[0] if not positive_to_negative.empty else spot_price
    
    return {
        'pin_strike': pin_strike,
        'pin_expiry': pin_expiry,
        'total_gex': total_gex,
        'net_gex': net_gex,
        'direction': direction,
        'pull_strength': abs(pin_strike - spot_price) / spot_price * 100,  # % distance
        'gex_by_strike': gex_by_strike,
        'gamma_walls': gamma_walls,
        'zero_gamma': zero_gamma_level,
        'spot_price': spot_price
    }

def get_gamma_analysis(api_key, underlying, spot_price):
    """
    Main function to get complete gamma analysis for an index
    
    Returns gamma analysis dict with is_mock_data flag
    """
    # Fetch options chain
    options_df, is_mock_data = fetch_options_chain(api_key, underlying, spot_price)
    
    if options_df.empty:
        return None
    
    # Calculate gamma exposure
    gex_analysis = calculate_gamma_exposure(options_df, spot_price)
    
    # Add is_mock_data flag to the analysis
    gex_analysis['is_mock_data'] = is_mock_data
    
    # Add summary message
    pin_strike = gex_analysis['pin_strike']
    direction = gex_analysis['direction']
    pull_strength = gex_analysis['pull_strength']
    
    if pull_strength < 1:
        strength_desc = "strong"
    elif pull_strength < 2:
        strength_desc = "moderate"
    else:
        strength_desc = "weak"
    
    gex_analysis['summary'] = f"Price is being {strength_desc}ly pulled {direction} to ${pin_strike:.0f} (Pin at {gex_analysis['pin_expiry'].strftime('%m/%d')})"
    
    return gex_analysis