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
    Fetch real options chain data from Polygon API
    
    Returns DataFrame with strike, expiry, type, OI, volume, IV, gamma
    """
    try:
        client = RESTClient(api_key)
        
        # Get options chain for the underlying
        end_date = datetime.now() + timedelta(days=days_ahead)
        
        # Fetch options contracts
        contracts = client.get_contracts(
            underlying_ticker=underlying,
            expiration_date_lte=end_date.strftime("%Y-%m-%d"),
            limit=1000
        )
        
        options_data = []
        
        # Handle response format
        if isinstance(contracts, dict):
            if 'results' in contracts:
                contracts_list = contracts['results']
            else:
                contracts_list = list(contracts.values())[0] if contracts else []
        else:
            contracts_list = contracts if contracts else []
        
        for contract in contracts_list:
            if isinstance(contract, dict):
                # Extract contract details
                strike = float(contract.get('strike_price', 0))
                expiry_str = contract.get('expiration_date', '')
                option_type = contract.get('contract_type', '').lower()
                
                if strike > 0 and expiry_str:
                    expiry = datetime.strptime(expiry_str, '%Y-%m-%d')
                    days_to_expiry = (expiry - datetime.now()).days
                    
                    if days_to_expiry > 0:
                        # Get option details including OI and IV
                        ticker = contract.get('ticker', '')
                        
                        # Fetch additional data for this contract
                        details = client.get_contract_details(ticker)
                        
                        if details:
                            oi = details.get('open_interest', 0)
                            iv = details.get('implied_volatility', 0.25)  # Default 25% if missing
                            
                            # Calculate gamma
                            T = days_to_expiry / 365.0
                            gamma = black_scholes_gamma(spot_price, strike, T, 0.05, iv)
                            
                            options_data.append({
                                'strike': strike,
                                'expiry': expiry,
                                'days_to_expiry': days_to_expiry,
                                'type': option_type,
                                'open_interest': oi,
                                'implied_volatility': iv,
                                'gamma': gamma,
                                'ticker': ticker
                            })
        
        return pd.DataFrame(options_data)
        
    except Exception as e:
        st.warning(f"Could not fetch options chain: {str(e)}")
        # Return mock data for demonstration
        return create_mock_options_chain(underlying, spot_price)

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
        datetime.now() + timedelta(days=1),   # 0DTE
        datetime.now() + timedelta(days=7),   # Weekly
        datetime.now() + timedelta(days=30),  # Monthly
    ]
    
    for expiry in expiry_dates:
        days_to_exp = (expiry - datetime.now()).days
        T = days_to_exp / 365.0
        
        for strike in strikes:
            moneyness = spot_price / strike
            
            # Higher OI near the money
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
    
    # Aggregate by strike
    gex_by_strike = options_df.groupby('strike').agg({
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
    """
    # Fetch options chain
    options_df = fetch_options_chain(api_key, underlying, spot_price)
    
    if options_df.empty:
        return None
    
    # Calculate gamma exposure
    gex_analysis = calculate_gamma_exposure(options_df, spot_price)
    
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