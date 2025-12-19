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

# Index to Options Root Mapping
# Some indices don't have direct options - they trade via ETFs
# This mapping is EXPLICIT and NEVER silent - UI must display the substitution
INDEX_TO_OPTIONS_ROOT = {
    'DJI': 'DIA',  # Dow Jones Industrial Average → SPDR Dow Jones ETF
}

def get_options_root(symbol: str) -> tuple[str, bool]:
    """
    Get the options root symbol for a given index.
    
    Returns:
        tuple: (options_root, is_etf_proxy)
        - options_root: The symbol to use for options chain lookups
        - is_etf_proxy: True if this is an ETF proxy (must be labeled in UI)
    """
    if symbol in INDEX_TO_OPTIONS_ROOT:
        return INDEX_TO_OPTIONS_ROOT[symbol], True
    return symbol, False

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

def fetch_options_chain(api_key, underlying, spot_price, days_ahead=90, max_retries=3):
    """
    Fetch REAL options chain data from Polygon Snapshot API with actual OI and IV
    
    Returns tuple: (DataFrame with strike/expiry/type/OI/IV/gamma, is_mock_data: bool)
    
    Uses Options Chain Snapshot API for real-time open interest and implied volatility.
    The Polygon Options Chain Snapshot API works AFTER HOURS and returns real EOD data.
    
    IMPORTANT: This function NEVER returns mock data. If the API fails after retries,
    it returns an empty DataFrame with is_mock_data=False to let the UI handle it gracefully.
    
    NOTE: Some indices (e.g., DJI) don't have direct options - they trade via ETFs.
    This function uses INDEX_TO_OPTIONS_ROOT mapping to fetch ETF options instead.
    The substitution is ALWAYS logged explicitly - never silent.
    """
    import time
    
    options_root, is_etf_proxy = get_options_root(underlying)
    if is_etf_proxy:
        print(f"[INFO] {underlying} uses ETF proxy: fetching {options_root} options instead")
    
    last_error = None
    
    for attempt in range(1, max_retries + 1):
        try:
            client = RESTClient(api_key)
            
            print(f"[Attempt {attempt}/{max_retries}] Fetching options chain snapshot for {options_root}...")
            start_time = time.time()
            timeout_seconds = 15  # Increased timeout for premium API
            
            snapshot = client.list_snapshot_options_chain(options_root)
            
            options_data = []
            contract_count = 0
            max_contracts = 500  # Increased for premium subscription
            skipped_no_details = 0
            skipped_invalid = 0
            skipped_expiry = 0
            errors = 0
            
            for contract in snapshot:
                if time.time() - start_time > timeout_seconds:
                    print(f"Warning: Timeout after {timeout_seconds}s, processed {contract_count} contracts. Using what we have.")
                    break
                
                contract_count += 1
                if contract_count > max_contracts:
                    print(f"Warning: Processed {max_contracts} contracts, stopping to prevent timeout")
                    break
                    
                try:
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
                    
                    expiry = datetime.strptime(expiry_str, '%Y-%m-%d')
                    
                    if expiry.date() == datetime.now().date():
                        days_to_expiry = 0
                    else:
                        days_to_expiry = (expiry.date() - datetime.now().date()).days
                    
                    if expiry.date() < datetime.now().date() or days_to_expiry > days_ahead:
                        skipped_expiry += 1
                        continue
                    
                    oi = int(contract.open_interest) if hasattr(contract, 'open_interest') and contract.open_interest else 0
                    iv = float(contract.implied_volatility) if hasattr(contract, 'implied_volatility') and contract.implied_volatility else 0.25
                    
                    T = max(days_to_expiry, 0.001) / 365.0
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
                    errors += 1
                    if errors <= 3:
                        print(f"Contract processing error: {str(contract_error)[:100]}")
                    continue
            
            print(f"✓ Processed {contract_count} contracts: {len(options_data)} valid, {skipped_no_details} no details, {skipped_invalid} invalid, {skipped_expiry} expired/far, {errors} errors")
            
            if options_data:
                return pd.DataFrame(options_data), False  # REAL DATA from Polygon API
            else:
                print(f"Warning: API returned data but no valid contracts for {underlying}. This may indicate the underlying symbol is incorrect.")
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                return pd.DataFrame(), False  # Empty DataFrame, NOT mock data
            
        except Exception as e:
            last_error = str(e)
            error_msg = str(e)
            
            if "429" in error_msg or "rate limit" in error_msg.lower():
                print(f"✗ Rate limit hit on attempt {attempt}. Waiting before retry...")
                time.sleep(5)
            elif "401" in error_msg or "unauthorized" in error_msg.lower():
                print(f"✗ Authentication error: API key may be invalid. Error: {error_msg[:100]}")
                break  # Don't retry auth errors
            elif "not found" in error_msg.lower() or "404" in error_msg:
                print(f"✗ Symbol not found: {underlying}. Error: {error_msg[:100]}")
                break  # Don't retry 404 errors
            else:
                print(f"✗ API error on attempt {attempt}/{max_retries}: {error_msg[:150]}")
            
            if attempt < max_retries:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
    
    print(f"✗ FAILED: Could not fetch options data for {underlying} after {max_retries} attempts. Last error: {last_error[:150] if last_error else 'Unknown'}")
    print(f"NOTE: Returning empty data instead of fake mock data. The UI should show 'data unavailable'.")
    return pd.DataFrame(), False  # Empty DataFrame, NOT mock data

def _create_mock_options_chain_for_testing(underlying, spot_price):
    """
    TESTING/DEVELOPMENT ONLY: Create mock options chain for unit tests.
    
    WARNING: This function should NEVER be called in production code.
    It generates fake random data that will show incorrect gamma pin levels.
    
    For production, use fetch_options_chain() which returns real Polygon API data.
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
    Calculate net gamma exposure by strike and identify pin levels.
    
    Uses canonical GEX computation:
    - row_gex = gamma * OI * 100 (per contract, unsigned magnitude)
    - net_gex = sum(call_gex) - sum(put_gex)
    - total_gex = sum(|call_gex|) + sum(|put_gex|)
    - Invariant: total_gex >= |net_gex|
    
    Returns dictionary with:
    - pin_strike: Strike with highest absolute GEX
    - pin_expiry: Expiry date of pin
    - total_gex: Total gamma exposure at pin
    - direction: Whether pin is above/below spot
    - gex_by_strike: DataFrame of GEX by strike
    - gamma_walls: Top strikes by absolute GEX
    """
    from app.core.gex import assert_gex_invariant
    
    CONTRACT_MULTIPLIER = 100
    
    # Calculate per-row GEX: gamma * OI * multiplier (unsigned magnitude)
    # Scaling to billions uses (spot_price ** 2) / 1e9 for dollar-weighted GEX
    options_df['row_gex'] = (
        options_df['gamma'].astype(float) * 
        options_df['open_interest'].astype(float) * 
        CONTRACT_MULTIPLIER * 
        (spot_price ** 2) / 1e9  # Scale to billions
    )
    
    # Calculate call and put sums separately (canonical approach)
    call_mask = options_df['type'] == 'call'
    put_mask = options_df['type'] == 'put'
    
    call_gex_sum = options_df.loc[call_mask, 'row_gex'].sum()
    put_gex_sum = options_df.loc[put_mask, 'row_gex'].sum()
    
    # Canonical GEX aggregation:
    # net_gex = call_sum - put_sum (puts contribute negative to net)
    # total_gex = |call_sum| + |put_sum| (sum of magnitudes)
    overall_net_gex = call_gex_sum - put_gex_sum
    overall_total_gex = abs(call_gex_sum) + abs(put_gex_sum)
    
    # Assert invariant at top level
    assert_gex_invariant(overall_net_gex, overall_total_gex, "calculate_gamma_exposure overall")
    
    # For per-strike aggregation, also apply sign convention
    # signed_gex: calls positive, puts negative
    options_df['signed_gex'] = options_df.apply(
        lambda row: row['row_gex'] if row['type'] == 'call' else -row['row_gex'],
        axis=1
    )
    # Keep 'gex' for backward compatibility (unsigned magnitude)
    options_df['gex'] = options_df['row_gex']
    
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
    
    # Aggregate by strike for the selected expiry using canonical GEX formula
    # Per-strike: net_gex = call_sum - put_sum, total_gex = |call_sum| + |put_sum|
    def aggregate_gex_by_strike(group):
        call_rows = group[group['type'] == 'call']
        put_rows = group[group['type'] == 'put']
        
        call_sum = call_rows['row_gex'].sum() if len(call_rows) > 0 else 0.0
        put_sum = put_rows['row_gex'].sum() if len(put_rows) > 0 else 0.0
        
        # Canonical formula: net = call - put, total = |call| + |put|
        net_gex = call_sum - put_sum
        total_gex = abs(call_sum) + abs(put_sum)
        
        return pd.Series({
            'call_gex': call_sum,
            'put_gex': put_sum,
            'net_gex': net_gex,
            'total_gex': total_gex,
            'expiry': group['expiry'].iloc[0],
            'days_to_expiry': group['days_to_expiry'].min()
        })
    
    gex_by_strike = filtered_df.groupby('strike').apply(aggregate_gex_by_strike, include_groups=False).reset_index()
    
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
    
    # Pin-level GEX values (original semantics for existing consumers)
    total_gex = pin_row['total_gex']  # GEX at pin strike
    net_gex = pin_row['net_gex']      # Net GEX at pin strike
    
    # Calculate AGGREGATE GEX using CANONICAL definitions from app/core/gex.py
    # TOTAL_GEX_ABS = sum(abs(net_gex_per_strike)) - gross magnitude
    # TOTAL_GEX_NET = sum(net_gex_per_strike) - net directional
    from app.core.gex import compute_aggregate_gex_from_arrays
    
    net_gex_values = gex_by_strike['net_gex'].tolist()
    agg_result = compute_aggregate_gex_from_arrays(net_gex_values)
    aggregate_total_gex = agg_result.total_gex_abs  # CANONICAL: sum(abs(net_gex_per_strike))
    aggregate_net_gex = agg_result.total_gex_net    # CANONICAL: sum(net_gex_per_strike)
    
    # Assert GEX invariant for aggregate values
    assert_gex_invariant(aggregate_net_gex, aggregate_total_gex, "calculate_gamma_exposure aggregate")
    
    # Direction of pull
    direction = 'above' if pin_strike > spot_price else 'below' if pin_strike < spot_price else 'at'
    
    # Get top gamma walls (top 5 strikes by absolute GEX)
    gamma_walls = gex_by_strike.nlargest(5, 'total_gex')[['strike', 'net_gex', 'total_gex', 'days_to_expiry']]
    
    # Calculate cumulative GEX for gamma flip level
    gex_by_strike_sorted = gex_by_strike.sort_values('strike')
    gex_by_strike_sorted['cumulative_gex'] = gex_by_strike_sorted['net_gex'].cumsum()
    
    # Find zero gamma level (where cumulative GEX crosses zero)
    # Look for sign changes in cumulative GEX
    positive_to_negative = gex_by_strike_sorted[
        (gex_by_strike_sorted['cumulative_gex'].shift(1) > 0) & 
        (gex_by_strike_sorted['cumulative_gex'] <= 0)
    ]
    
    negative_to_positive = gex_by_strike_sorted[
        (gex_by_strike_sorted['cumulative_gex'].shift(1) < 0) & 
        (gex_by_strike_sorted['cumulative_gex'] >= 0)
    ]
    
    # Get all available strikes from the options chain
    available_strikes = sorted(gex_by_strike['strike'].unique())
    
    # Find zero gamma crossing point
    zero_gamma_raw = None
    if not positive_to_negative.empty:
        zero_gamma_raw = positive_to_negative['strike'].iloc[0]
    elif not negative_to_positive.empty:
        zero_gamma_raw = negative_to_positive['strike'].iloc[0]
    
    # Snap to nearest VALID strike price
    if zero_gamma_raw is not None and available_strikes:
        # Find the nearest actual strike
        zero_gamma_level = min(available_strikes, key=lambda x: abs(x - zero_gamma_raw))
        
        # Validate: zero gamma should be within ±15% of spot price to be meaningful
        if abs(zero_gamma_level - spot_price) / spot_price > 0.15:
            # If too far from spot, find the nearest strike to spot that has meaningful GEX
            nearby_strikes = [s for s in available_strikes 
                            if abs(s - spot_price) / spot_price <= 0.15]
            if nearby_strikes:
                # Pick the strike closest to where cumulative GEX is smallest (near zero)
                strike_gex = gex_by_strike_sorted.set_index('strike')['cumulative_gex']
                valid_nearby = [s for s in nearby_strikes if s in strike_gex.index]
                if valid_nearby:
                    zero_gamma_level = min(valid_nearby, key=lambda s: abs(strike_gex[s]))
                else:
                    zero_gamma_level = min(nearby_strikes, key=lambda x: abs(x - spot_price))
            else:
                # No nearby strikes, use the one closest to spot
                zero_gamma_level = min(available_strikes, key=lambda x: abs(x - spot_price))
    else:
        # Fallback: use nearest strike to spot price
        if available_strikes:
            zero_gamma_level = min(available_strikes, key=lambda x: abs(x - spot_price))
        else:
            zero_gamma_level = spot_price
    
    # Determine expiration scope (FIRST-CLASS FIELD - no silent mixing)
    if effective_expiry:
        days_to_exp = (effective_expiry.date() - datetime.now().date()).days
        if days_to_exp == 0:
            expiration_scope = '0DTE'
        else:
            expiration_scope = f'{days_to_exp}DTE'
    else:
        # Fallback: using all expirations
        max_dte = gex_by_strike['days_to_expiry'].max() if not gex_by_strike.empty else 90
        expiration_scope = f'ALL<={max_dte}D'
    
    # Calculate aggregate call/put GEX totals from per-strike data
    call_gex_total = gex_by_strike['call_gex'].sum() if 'call_gex' in gex_by_strike.columns else 0.0
    put_gex_total = gex_by_strike['put_gex'].sum() if 'put_gex' in gex_by_strike.columns else 0.0
    gross_gex = call_gex_total + put_gex_total
    net_gex_aggregate = call_gex_total - put_gex_total
    
    return {
        'pin_strike': pin_strike,
        'pin_expiry': pin_expiry,
        'total_gex': total_gex,  # GEX at pin strike (original semantics)
        'net_gex': net_gex,      # Net GEX at pin strike (original semantics)
        'aggregate_total_gex': aggregate_total_gex,  # Legacy: Sum of absolute GEX across ALL strikes
        'aggregate_net_gex': aggregate_net_gex,      # Legacy: Sum of signed GEX across ALL strikes
        'call_gex_total': call_gex_total,    # NEW: Aggregate call gamma exposure
        'put_gex_total': put_gex_total,      # NEW: Aggregate put gamma exposure
        'gross_gex': gross_gex,              # NEW: call_gex_total + put_gex_total
        'net_gex_total': net_gex_aggregate,  # NEW: call_gex_total - put_gex_total
        'direction': direction,
        'pull_strength': abs(pin_strike - spot_price) / spot_price * 100,  # % distance
        'gex_by_strike': gex_by_strike,
        'gamma_walls': gamma_walls,
        'zero_gamma': zero_gamma_level,
        'spot_price': spot_price,
        'expiration_scope': expiration_scope,  # FIRST-CLASS FIELD: '0DTE', '1DTE', or 'ALL<=90D'
        'contracts_count': len(options_df),
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
    
    IMPORTANT: Filters strikes to reasonable moneyness range (±20% for 0-DTE, 
    ±25% for longer DTE) to avoid far OTM strikes with near-zero gamma dominating.
    """
    if options_df.empty:
        return None
    
    CONTRACT_MULTIPLIER = 100
    
    # Filter to max DTE
    near_term_df = options_df[options_df['days_to_expiry'] <= max_dte].copy()
    
    if near_term_df.empty:
        return None
    
    # Filter to strikes within reasonable moneyness range
    # Tighter range for 0-DTE (±15%), wider for longer DTE (±25%)
    min_strike = spot_price * 0.75  # 25% below spot
    max_strike = spot_price * 1.25  # 25% above spot
    
    near_term_df = near_term_df[
        (near_term_df['strike'] >= min_strike) & 
        (near_term_df['strike'] <= max_strike)
    ].copy()
    
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
    
    MIN_GEX_THRESHOLD = 0.001  # Minimum GEX in billions to be considered meaningful
    
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
        
        # Filter out strikes with near-zero GEX BEFORE any further processing
        gex_by_strike = gex_by_strike[gex_by_strike['total_gex'].abs() > MIN_GEX_THRESHOLD]
        
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
                # No nearby strikes - skip this expiry as it has no meaningful gamma
                continue
            
            # Skip expirations with essentially zero GEX (< 0.001B threshold)
            # This filters out far OTM options with near-zero gamma
            MIN_GEX_THRESHOLD = 0.001  # Minimum GEX in billions to be considered valid
            if float(pin_row['total_gex']) < MIN_GEX_THRESHOLD:
                print(f"Skipping DTE {dte}: GEX {pin_row['total_gex']:.2e}B is below threshold")
                continue
            
            # Validate pin strike is within reasonable range of spot
            pin_strike_value = float(pin_row['strike'])
            if abs(pin_strike_value - spot_price) / spot_price > 0.15:
                print(f"Skipping DTE {dte}: pin strike ${pin_strike_value:.0f} is >15% from spot ${spot_price:.0f}")
                continue
            
            weight = time_weights.get(int(dte), 0.05)
            
            # Get top walls (already filtered to meaningful GEX)
            top_walls = gex_by_strike.nlargest(3, 'total_gex')[['strike', 'net_gex', 'total_gex']].to_dict('records')
            
            # Calculate AGGREGATE GEX using CANONICAL definitions from app/core/gex.py
            # TOTAL_GEX_ABS = sum(abs(net_gex_per_strike)), TOTAL_GEX_NET = sum(net_gex_per_strike)
            from app.core.gex import compute_aggregate_gex_from_arrays
            expiry_net_gex_values = gex_by_strike['net_gex'].tolist()
            expiry_agg_result = compute_aggregate_gex_from_arrays(expiry_net_gex_values)
            total_gex_sum = expiry_agg_result.total_gex_abs  # CANONICAL: sum(abs(net_gex_per_strike))
            net_gex_sum = expiry_agg_result.total_gex_net    # CANONICAL: sum(net_gex_per_strike)
            
            # Pin strike's GEX for reference
            pin_gex = float(pin_row['total_gex'])
            
            gamma_by_expiry[int(dte)] = {
                'pin_strike': pin_strike_value,
                'total_gex': pin_gex,        # GEX at pin strike (original behavior for consumers)
                'net_gex': float(pin_row['net_gex']),  # Net GEX at pin strike (original behavior)
                'expiry_total_gex': total_gex_sum,  # NEW: Sum of absolute GEX across ALL strikes (for export)
                'expiry_net_gex': net_gex_sum,      # NEW: Sum of signed GEX across ALL strikes (for export)
                'weight': weight,
                'weighted_gex': pin_gex * weight,   # Use PIN's GEX for aggregate pin calculation
                'expiry_date': pin_row['expiry'].strftime('%Y-%m-%d') if hasattr(pin_row['expiry'], 'strftime') else str(pin_row['expiry']),
                'top_walls': top_walls
            }
            
            # Add weighted strikes for unified wall calculation (only meaningful GEX)
            for _, row in gex_by_strike.iterrows():
                # Only add strikes with meaningful weighted GEX
                weighted_gex = row['total_gex'] * weight
                if weighted_gex > MIN_GEX_THRESHOLD:
                    all_weighted_strikes.append({
                        'strike': row['strike'],
                        'net_gex': row['net_gex'] * weight,
                        'total_gex': weighted_gex,
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
    
    IMPORTANT: This function NEVER returns mock data. If no real data is available,
    it returns a special 'data_unavailable' response for the UI to handle gracefully.
    """
    options_df, is_mock_data = fetch_options_chain(api_key, underlying, spot_price)
    
    if options_df.empty:
        print(f"✗ No real options data available for {underlying} multi-expiry analysis")
        return {
            'data_unavailable': True,
            'underlying': str(underlying),
            'spot_price': float(spot_price),
            'max_dte': int(max_dte),
            'is_mock_data': False,
            'gamma_by_expiry': {},
            'unified_walls': [],
            'aggregate_pin': float(spot_price),
            'time_weights': {0: 1.0, 1: 0.5, 2: 0.3, 3: 0.2, 4: 0.15, 5: 0.12, 6: 0.10, 7: 0.08},
            'error_message': f"No real options data available for {underlying}. The Polygon API may be experiencing issues."
        }
    
    analysis = calculate_multi_expiry_gamma(options_df, spot_price, max_dte)
    
    if not analysis:
        print(f"✗ Failed to calculate multi-expiry gamma for {underlying}")
        return {
            'data_unavailable': True,
            'underlying': str(underlying),
            'spot_price': float(spot_price),
            'max_dte': int(max_dte),
            'is_mock_data': False,
            'gamma_by_expiry': {},
            'unified_walls': [],
            'aggregate_pin': float(spot_price),
            'time_weights': {0: 1.0, 1: 0.5, 2: 0.3, 3: 0.2, 4: 0.15, 5: 0.12, 6: 0.10, 7: 0.08},
            'error_message': f"Failed to calculate gamma exposure for {underlying}."
        }
    
    unified_walls_list = []
    if not analysis['unified_walls'].empty:
        for _, row in analysis['unified_walls'].iterrows():
            unified_walls_list.append({
                'strike': float(row['strike']),
                'weighted_net_gex': float(row['weighted_net_gex']),
                'weighted_total_gex': float(row['weighted_total_gex']),
                'expirations': [int(e) for e in row['expirations']]
            })
    
    result = {
        'gamma_by_expiry': analysis['gamma_by_expiry'],
        'unified_walls': unified_walls_list,
        'aggregate_pin': float(analysis['aggregate_pin']),
        'time_weights': {int(k): float(v) for k, v in analysis['time_weights'].items()},
        'spot_price': float(analysis['spot_price']),
        'max_dte': int(analysis['max_dte']),
        'is_mock_data': False,  # Always False - we never return mock data
        'underlying': str(underlying),
        'data_unavailable': False
    }
    
    return result


def get_gamma_analysis(api_key, underlying, spot_price):
    """
    Main function to get complete gamma analysis for an index
    
    Returns gamma analysis dict with is_mock_data flag.
    Falls back to latest stored REAL (non-mock) snapshot if live API fails.
    
    IMPORTANT: This function NEVER returns mock data. If no real data is available,
    it returns a special 'data_unavailable' response for the UI to handle gracefully.
    
    NOTE: Some indices (e.g., DJI) use ETF proxies for options data (e.g., DIA).
    The `options_root` and `is_etf_proxy` fields indicate the actual symbol used
    and whether this was a substitution. The UI MUST display this explicitly.
    """
    options_root, is_etf_proxy = get_options_root(underlying)
    options_df, is_mock_data = fetch_options_chain(api_key, underlying, spot_price)
    
    if options_df.empty:
        print(f"Live options chain empty for {underlying}, attempting fallback to stored REAL snapshot...")
        try:
            from database import get_latest_gamma_snapshot
            snapshot = get_latest_gamma_snapshot(underlying)
            
            if snapshot and not snapshot.is_mock_data:
                print(f"✓ Using stored REAL gamma snapshot from {snapshot.interval_timestamp}")
                gex_analysis = {
                    'pin_strike': snapshot.pin_strike,
                    'pin_expiry': snapshot.interval_timestamp,
                    'total_gex': snapshot.total_gex,
                    'net_gex': snapshot.net_gex,
                    'direction': 'above' if snapshot.pin_strike > snapshot.spot_price else 'below' if snapshot.pin_strike < snapshot.spot_price else 'at',
                    'pull_strength': snapshot.pull_strength,
                    'gex_by_strike': pd.DataFrame(),
                    'gamma_walls': pd.DataFrame(),
                    'zero_gamma': snapshot.spot_price,
                    'spot_price': spot_price,
                    'is_mock_data': False,  # Always False - we only use real snapshots
                    'is_cached_data': True,
                    'cache_timestamp': snapshot.interval_timestamp
                }
                
                pull_strength = gex_analysis['pull_strength']
                if pull_strength < 1:
                    strength_desc = "strong"
                elif pull_strength < 2:
                    strength_desc = "moderate"
                else:
                    strength_desc = "weak"
                
                direction = gex_analysis['direction']
                pin_strike = gex_analysis['pin_strike']
                gex_analysis['summary'] = f"Price is being {strength_desc}ly pulled {direction} to ${pin_strike:.0f} (Cached EOD data)"
                
                # Add ETF proxy information for explicit UI labeling
                gex_analysis['options_root'] = options_root
                gex_analysis['is_etf_proxy'] = is_etf_proxy
                
                return gex_analysis
            elif snapshot and snapshot.is_mock_data:
                print(f"✗ Found stored snapshot for {underlying} but it contains MOCK data - ignoring")
            else:
                print(f"✗ No stored snapshot found for {underlying}")
            
            print(f"✗ DATA UNAVAILABLE: No real gamma data available for {underlying}")
            return {
                'data_unavailable': True,
                'underlying': underlying,
                'spot_price': spot_price,
                'is_mock_data': False,
                'is_cached_data': False,
                'options_root': options_root,
                'is_etf_proxy': is_etf_proxy,
                'summary': f"Gamma data temporarily unavailable for {underlying}. Real-time data will be available during next market session.",
                'error_message': f"No real options data available for {underlying}. The Polygon API may be experiencing issues or the symbol may not have options data."
            }
        except Exception as e:
            print(f"Error fetching fallback snapshot: {str(e)}")
            return {
                'data_unavailable': True,
                'underlying': underlying,
                'spot_price': spot_price,
                'is_mock_data': False,
                'is_cached_data': False,
                'options_root': options_root,
                'is_etf_proxy': is_etf_proxy,
                'summary': f"Gamma data temporarily unavailable for {underlying}.",
                'error_message': f"Error retrieving gamma data: {str(e)[:100]}"
            }
    
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
    
    # Add ETF proxy information for explicit UI labeling
    gex_analysis['options_root'] = options_root
    gex_analysis['is_etf_proxy'] = is_etf_proxy
    
    return gex_analysis