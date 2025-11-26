"""
Options Gamma Exposure (GEX) Analysis Module
Calculates real gamma exposure from options chain data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import norm
from polygon import RESTClient
import time
import pytz
from database import get_latest_gamma_snapshot

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

def fetch_options_chain(api_key, underlying, spot_price, days_ahead=90):
    """
    Fetch REAL options chain data from Polygon Snapshot API with actual OI and IV
    
    Returns tuple: (DataFrame with strike/expiry/type/OI/IV/gamma, is_mock_data: bool)
    
    Uses Options Chain Snapshot API for real-time open interest and implied volatility.
    """
    import time
    
    try:
        client = RESTClient(api_key)
        
        # Use Options Chain Snapshot API to get REAL OI and IV
        # This endpoint returns actual market data for all contracts on the underlying
        # Add timeout protection by limiting iteration
        print(f"Fetching options chain snapshot for {underlying}...")
        start_time = time.time()
        timeout_seconds = 10  # Maximum 10 seconds to fetch options data
        
        snapshot = client.list_snapshot_options_chain(underlying)
        
        options_data = []
        contract_count = 0
        max_contracts = 200  # Reduced limit for faster processing
        skipped_no_details = 0
        skipped_invalid = 0
        skipped_expiry = 0
        errors = 0
        
        # Process each contract in the snapshot
        for contract in snapshot:
            # Check timeout
            if time.time() - start_time > timeout_seconds:
                print(f"Warning: Timeout after {timeout_seconds}s, processed {contract_count} contracts. Using what we have.")
                break
            
            contract_count += 1
            if contract_count > max_contracts:
                print(f"Warning: Processed {max_contracts} contracts, stopping to prevent timeout")
                break
            try:
                # Extract contract details
                if not hasattr(contract, 'details'):
                    skipped_no_details += 1
                    continue
                    
                details = contract.details
                strike = float(details.strike_price) if hasattr(details, 'strike_price') else 0
                expiry_str = details.expiration_date if hasattr(details, 'expiration_date') else ''
                option_type = details.contract_type.lower() if hasattr(details, 'contract_type') else ''
                
                if strike <= 0 or not expiry_str:
                    skipped_invalid += 1
                    continue
                
                # Parse expiry
                expiry = datetime.strptime(expiry_str, '%Y-%m-%d')
                
                # IMPORTANT: Include 0DTE (same-day) options
                # Calculate days properly - 0DTE should show as 0, not -1
                if expiry.date() == datetime.now().date():
                    days_to_expiry = 0  # Same day = 0 days
                else:
                    days_to_expiry = (expiry.date() - datetime.now().date()).days
                
                # Only exclude if expiry date is before today's date
                if expiry.date() < datetime.now().date() or days_to_expiry > days_ahead:
                    skipped_expiry += 1
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
                errors += 1
                if errors <= 3:  # Only print first few errors
                    print(f"Contract processing error: {str(contract_error)[:100]}")
                continue
        
        print(f"Processed {contract_count} contracts: {len(options_data)} valid, {skipped_no_details} no details, {skipped_invalid} invalid, {skipped_expiry} expired/far, {errors} errors")
        
        # If we got real data, return it
        if options_data:
            return pd.DataFrame(options_data), False  # is_mock_data = False - REAL DATA!
        else:
            # No data found, fall back to mock
            print(f"Warning: No options snapshot data returned for {underlying}. Using simulated data.")
            return create_mock_options_chain(underlying, spot_price), True
        
    except Exception as e:
        # API error - fall back to mock data
        error_msg = str(e)
        if "not found" in error_msg.lower() or "404" in error_msg:
            print(f"Warning: Options snapshot not available for {underlying}. Using simulated data. Error: {error_msg[:100]}")
        else:
            print(f"Warning: Options snapshot API error for {underlying}. Using simulated data. Error: {error_msg[:100]}")
        
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
    
    # Find pin strike (highest absolute GEX NEAR current price)
    # Filter to strikes within ±10% of spot (realistic pin range for indices)
    # Gamma pinning only works when strikes are close to current price
    price_range = 0.10  # 10% range
    lower_bound = spot_price * (1 - price_range)
    upper_bound = spot_price * (1 + price_range)
    
    nearby_strikes = gex_by_strike[
        (gex_by_strike['strike'] >= lower_bound) & 
        (gex_by_strike['strike'] <= upper_bound)
    ]
    
    # If no strikes in range, widen to ±15%
    if nearby_strikes.empty:
        price_range = 0.15
        lower_bound = spot_price * (1 - price_range)
        upper_bound = spot_price * (1 + price_range)
        nearby_strikes = gex_by_strike[
            (gex_by_strike['strike'] >= lower_bound) & 
            (gex_by_strike['strike'] <= upper_bound)
        ]
    
    # Find pin within realistic range
    if not nearby_strikes.empty:
        pin_row = nearby_strikes.loc[nearby_strikes['total_gex'].idxmax()]
    else:
        # Fallback: use all strikes (should rarely happen)
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

def calculate_multi_expiry_gamma(options_df, spot_price, max_dte=7):
    """
    Calculate gamma exposure across multiple expirations (0-7 DTE).
    
    Returns dictionary with:
    - gamma_by_expiry: Dict of {days_to_exp: gamma_data}
    - unified_walls: Top gamma walls weighted across all expirations
    - aggregate_pin: Weighted pin strike across all near-term expirations
    - time_weights: Weights applied to each expiration
    
    Uses vectorized pandas operations for performance.
    """
    if options_df.empty:
        return None
    
    CONTRACT_MULTIPLIER = 100
    
    # Filter to max DTE
    near_term_df = options_df[options_df['days_to_expiry'] <= max_dte].copy()
    
    if near_term_df.empty:
        return None
    
    # Vectorized GEX calculation (much faster than apply)
    near_term_df['gex'] = (
        near_term_df['gamma'] * 
        near_term_df['open_interest'] * 
        CONTRACT_MULTIPLIER * 
        (spot_price ** 2) / 1e9
    )
    
    # Vectorized sign convention: calls positive, puts negative
    near_term_df['signed_gex'] = np.where(
        near_term_df['type'] == 'call',
        near_term_df['gex'],
        -near_term_df['gex']
    )
    
    # Time decay weights: 0-DTE has highest weight, declining as expiration increases
    # Weights: 0DTE=1.0, 1DTE=0.5, 2DTE=0.3, 3DTE=0.2, 4DTE=0.15, 5DTE=0.12, 6DTE=0.10, 7DTE=0.08
    time_weights = {
        0: 1.0,
        1: 0.5,
        2: 0.3,
        3: 0.2,
        4: 0.15,
        5: 0.12,
        6: 0.10,
        7: 0.08
    }
    
    # Get unique expiration dates
    expiry_days = sorted(near_term_df['days_to_expiry'].unique())
    
    gamma_by_expiry = {}
    all_weighted_strikes = []
    
    for dte in expiry_days:
        dte_df = near_term_df[near_term_df['days_to_expiry'] == dte]
        
        # Aggregate by strike for this expiry
        gex_by_strike = dte_df.groupby('strike').agg({
            'signed_gex': 'sum',
            'gex': lambda x: abs(x).sum(),
            'expiry': 'first',
            'days_to_expiry': 'min'
        }).reset_index()
        
        gex_by_strike.columns = ['strike', 'net_gex', 'total_gex', 'expiry', 'days_to_expiry']
        
        # Find pin for this expiry
        if not gex_by_strike.empty:
            # Filter to strikes within ±10% of spot
            price_range = 0.10
            lower_bound = spot_price * (1 - price_range)
            upper_bound = spot_price * (1 + price_range)
            
            nearby_strikes = gex_by_strike[
                (gex_by_strike['strike'] >= lower_bound) & 
                (gex_by_strike['strike'] <= upper_bound)
            ]
            
            if not nearby_strikes.empty:
                pin_row = nearby_strikes.loc[nearby_strikes['total_gex'].idxmax()]
            else:
                pin_row = gex_by_strike.loc[gex_by_strike['total_gex'].idxmax()]
            
            weight = time_weights.get(int(dte), 0.05)
            
            gamma_by_expiry[int(dte)] = {
                'pin_strike': float(pin_row['strike']),
                'total_gex': float(pin_row['total_gex']),
                'net_gex': float(pin_row['net_gex']),
                'weight': weight,
                'weighted_gex': float(pin_row['total_gex']) * weight,
                'expiry_date': pin_row['expiry'].strftime('%Y-%m-%d') if hasattr(pin_row['expiry'], 'strftime') else str(pin_row['expiry']),
                'top_walls': gex_by_strike.nlargest(3, 'total_gex')[['strike', 'net_gex', 'total_gex']].to_dict('records')
            }
            
            # Add weighted strikes for unified wall calculation
            for _, row in gex_by_strike.iterrows():
                all_weighted_strikes.append({
                    'strike': row['strike'],
                    'net_gex': row['net_gex'] * weight,
                    'total_gex': row['total_gex'] * weight,
                    'days_to_expiry': int(dte),
                    'weight': weight
                })
    
    # Calculate unified gamma walls (weighted across all expirations)
    if all_weighted_strikes:
        unified_df = pd.DataFrame(all_weighted_strikes)
        
        # Aggregate weighted GEX by strike across all expirations
        unified_walls = unified_df.groupby('strike').agg({
            'net_gex': 'sum',
            'total_gex': 'sum',
            'days_to_expiry': lambda x: list(set(x))  # List of expirations with this strike
        }).reset_index()
        
        unified_walls.columns = ['strike', 'weighted_net_gex', 'weighted_total_gex', 'expirations']
        unified_walls = unified_walls.nlargest(10, 'weighted_total_gex')
        
        # Calculate aggregate pin (weighted average of pins)
        total_weight = sum(data['weight'] for data in gamma_by_expiry.values())
        if total_weight > 0:
            aggregate_pin = sum(
                data['pin_strike'] * data['weight'] 
                for data in gamma_by_expiry.values()
            ) / total_weight
        else:
            aggregate_pin = spot_price
    else:
        unified_walls = pd.DataFrame()
        aggregate_pin = spot_price
    
    return {
        'gamma_by_expiry': gamma_by_expiry,
        'unified_walls': unified_walls,
        'aggregate_pin': aggregate_pin,
        'time_weights': time_weights,
        'spot_price': spot_price,
        'max_dte': max_dte
    }


def get_multi_expiry_analysis(api_key, underlying, spot_price, max_dte=7):
    """
    Get multi-expiration gamma analysis for enhanced EOD predictions.
    
    Returns analysis across 0-DTE through 7-DTE expirations with:
    - Time-weighted gamma exposure by expiration
    - Unified gamma walls across all near-term expirations
    - Aggregate pin strike (weighted average)
    
    All return values are JSON-safe Python native types.
    """
    # Fetch options chain (already gets all expirations up to 90 days)
    options_df, is_mock_data = fetch_options_chain(api_key, underlying, spot_price)
    
    if options_df.empty:
        return None
    
    # Calculate multi-expiry gamma
    analysis = calculate_multi_expiry_gamma(options_df, spot_price, max_dte)
    
    if not analysis:
        return None
    
    # Convert unified_walls DataFrame to JSON-safe list
    unified_walls_list = []
    if not analysis['unified_walls'].empty:
        for _, row in analysis['unified_walls'].iterrows():
            unified_walls_list.append({
                'strike': float(row['strike']),
                'weighted_net_gex': float(row['weighted_net_gex']),
                'weighted_total_gex': float(row['weighted_total_gex']),
                'expirations': [int(e) for e in row['expirations']]
            })
    
    # Build fully JSON-safe result
    result = {
        'gamma_by_expiry': analysis['gamma_by_expiry'],  # Already dict with native types
        'unified_walls': unified_walls_list,
        'aggregate_pin': float(analysis['aggregate_pin']),
        'time_weights': {int(k): float(v) for k, v in analysis['time_weights'].items()},
        'spot_price': float(analysis['spot_price']),
        'max_dte': int(analysis['max_dte']),
        'is_mock_data': bool(is_mock_data),
        'underlying': str(underlying)
    }
    
    return result


def get_gamma_analysis(api_key, underlying, spot_price):
    """
    Main function to get complete gamma analysis for an index
    
    Returns gamma analysis dict with is_mock_data flag.
    Falls back to latest stored snapshot if live API fails.
    """
    # Fetch options chain
    options_df, is_mock_data = fetch_options_chain(api_key, underlying, spot_price)
    
    # If live API failed, try to use latest stored gamma snapshot
    if options_df.empty:
        print(f"Live options chain empty for {underlying}, attempting fallback to stored snapshot...")
        try:
            from database import get_latest_gamma_snapshot
            snapshot = get_latest_gamma_snapshot(underlying)
            
            if snapshot:
                print(f"✓ Using stored gamma snapshot from {snapshot.interval_timestamp}")
                # Reconstruct gamma analysis from stored snapshot
                gex_analysis = {
                    'pin_strike': snapshot.pin_strike,
                    'pin_expiry': snapshot.interval_timestamp,  # Use snapshot time as expiry
                    'total_gex': snapshot.total_gex,
                    'net_gex': snapshot.net_gex,
                    'direction': 'above' if snapshot.pin_strike > snapshot.spot_price else 'below' if snapshot.pin_strike < snapshot.spot_price else 'at',
                    'pull_strength': snapshot.pull_strength,
                    'gex_by_strike': pd.DataFrame(),  # Empty - we don't store full strike data
                    'gamma_walls': pd.DataFrame(),  # Empty - we don't store full wall data
                    'zero_gamma': snapshot.spot_price,  # Fallback to spot if not available
                    'spot_price': spot_price,  # Use current spot price
                    'is_mock_data': snapshot.is_mock_data,
                    'is_cached_data': True,  # Flag to indicate this is from cache
                    'cache_timestamp': snapshot.interval_timestamp
                }
                
                # Add summary message
                pull_strength = gex_analysis['pull_strength']
                if pull_strength < 1:
                    strength_desc = "strong"
                elif pull_strength < 2:
                    strength_desc = "moderate"
                else:
                    strength_desc = "weak"
                
                direction = gex_analysis['direction']
                pin_strike = gex_analysis['pin_strike']
                gex_analysis['summary'] = f"Price is being {strength_desc}ly pulled {direction} to ${pin_strike:.0f} (Cached data)"
                
                return gex_analysis
            else:
                print(f"✗ No stored snapshot found for {underlying}")
                return None
        except Exception as e:
            print(f"Error fetching fallback snapshot: {str(e)}")
            return None
    
    # Calculate gamma exposure from live data
    gex_analysis = calculate_gamma_exposure(options_df, spot_price)
    
    # Add is_mock_data flag to the analysis
    gex_analysis['is_mock_data'] = is_mock_data
    gex_analysis['is_cached_data'] = False
    
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