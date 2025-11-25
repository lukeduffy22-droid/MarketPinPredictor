"""
EOD Data Integration Layer

Fetches and transforms data from various sources (database, ring buffers, options_gamma)
into the format required by gamma_eod_predictor.py
"""

from typing import List, Optional, Tuple
from datetime import datetime, date, timedelta
import pytz
import numpy as np

from app.utils.gamma_eod_predictor import GammaWall, PinSnapshot
from database import get_gamma_snapshots_for_day, SessionLocal
from database import GammaPinSnapshot as DBGammaPinSnapshot
import options_gamma


def fetch_gamma_walls_from_analysis(
    api_key: str,
    ticker: str,
    spot_price: float
) -> Tuple[List[GammaWall], float]:
    """
    Fetch gamma walls from current options chain analysis.
    
    Returns:
        (walls, zero_gamma): List of GammaWall objects and zero gamma level
    """
    # Get full gamma analysis
    gex_analysis = options_gamma.get_gamma_analysis(api_key, ticker, spot_price)
    
    if not gex_analysis or 'gamma_walls' not in gex_analysis:
        return [], spot_price
    
    # Convert DataFrame to GammaWall objects
    walls_df = gex_analysis['gamma_walls']
    walls = []
    
    for _, row in walls_df.iterrows():
        wall = GammaWall(
            strike=float(row['strike']),
            net_gex=float(row['net_gex']),
            total_gex=float(row['total_gex']),
            days_to_exp=int(row['days_to_expiry'])
        )
        walls.append(wall)
    
    zero_gamma = float(gex_analysis.get('zero_gamma', spot_price))
    
    return walls, zero_gamma


def fetch_pin_snapshots_from_db(
    ticker: str,
    trading_date: date
) -> List[PinSnapshot]:
    """
    Fetch all pin snapshots for the given ticker and trading date from database.
    
    Returns:
        List of PinSnapshot objects ordered by timestamp
    """
    snapshots = get_gamma_snapshots_for_day(ticker, trading_date)
    
    if not snapshots:
        return []
    
    # Convert to Eastern Time for display
    et_tz = pytz.timezone('US/Eastern')
    
    pin_history = []
    for snap in snapshots:
        # Convert UTC timestamp to ET
        timestamp_et = snap.interval_timestamp.replace(tzinfo=pytz.UTC).astimezone(et_tz)
        
        pin_snap = PinSnapshot(
            timestamp=timestamp_et.strftime('%H:%M'),
            pin_strike=float(snap.pin_strike)
        )
        pin_history.append(pin_snap)
    
    return pin_history


def fetch_spot_prices_from_db(
    ticker: str,
    trading_date: date,
    lookback_minutes: int = 60
) -> Tuple[List[float], float, float]:
    """
    Fetch recent spot prices from gamma snapshots.
    
    Returns:
        (spot_prices, intraday_high, intraday_low)
    """
    snapshots = get_gamma_snapshots_for_day(ticker, trading_date)
    
    if not snapshots:
        return [], 0.0, 0.0
    
    # Extract spot prices
    spot_prices = [float(snap.spot_price) for snap in snapshots]
    
    # Calculate intraday high/low
    intraday_high = max(spot_prices) if spot_prices else 0.0
    intraday_low = min(spot_prices) if spot_prices else 0.0
    
    # Return last N minutes of prices (approximate based on 15-min intervals)
    # For 60 minutes, we want ~4 snapshots
    num_snapshots = max(1, lookback_minutes // 15)
    recent_prices = spot_prices[-num_snapshots:] if len(spot_prices) >= num_snapshots else spot_prices
    
    return recent_prices, intraday_high, intraday_low


def calculate_hv10_points(
    ticker: str,
    current_spot: float,
    lookback_days: int = 10
) -> Optional[float]:
    """
    Calculate 10-day historical volatility in index points.
    
    Uses gamma snapshots to calculate realized volatility over the past N days.
    Returns the average daily range in points.
    """
    try:
        db = SessionLocal()
        et_tz = pytz.timezone('US/Eastern')
        
        # Get trading dates for past N days (excluding weekends approximately)
        # We'll fetch more days and filter to get enough data
        end_date = datetime.now(et_tz).date()
        start_date = end_date - timedelta(days=lookback_days * 2)  # Get extra days to account for weekends
        
        # Query snapshots for the date range
        snapshots = db.query(DBGammaPinSnapshot).filter(
            DBGammaPinSnapshot.ticker == ticker,
            DBGammaPinSnapshot.trading_date >= start_date,
            DBGammaPinSnapshot.trading_date <= end_date
        ).order_by(DBGammaPinSnapshot.trading_date, DBGammaPinSnapshot.interval_timestamp).all()
        
        if not snapshots:
            return None
        
        # Group by trading date and calculate daily high-low range
        daily_ranges = {}
        for snap in snapshots:
            trade_date = snap.trading_date
            spot = float(snap.spot_price)
            
            if trade_date not in daily_ranges:
                daily_ranges[trade_date] = {'high': spot, 'low': spot}
            else:
                daily_ranges[trade_date]['high'] = max(daily_ranges[trade_date]['high'], spot)
                daily_ranges[trade_date]['low'] = min(daily_ranges[trade_date]['low'], spot)
        
        # Calculate ranges
        ranges = [data['high'] - data['low'] for data in daily_ranges.values()]
        
        if not ranges:
            return None
        
        # Take most recent N days
        recent_ranges = ranges[-lookback_days:] if len(ranges) >= lookback_days else ranges
        
        # Return average daily range in points
        avg_range = np.mean(recent_ranges)
        
        return float(avg_range)
        
    except Exception as e:
        print(f"Error calculating HV10: {str(e)}")
        return None
    finally:
        try:
            db.close()
        except:
            pass


def get_eod_prediction_inputs(
    api_key: str,
    ticker: str,
    spot_price: float,
    trading_date: Optional[date] = None
) -> dict:
    """
    Fetch all inputs required for EOD prediction.
    
    Returns dictionary with:
        - walls: List[GammaWall]
        - pin_history: List[PinSnapshot]
        - zero_gamma: float
        - spot_prices: List[float]
        - intraday_high: float
        - intraday_low: float
        - hv10_points: Optional[float]
    """
    if trading_date is None:
        et_tz = pytz.timezone('US/Eastern')
        trading_date = datetime.now(et_tz).date()
    
    # Convert Polygon ticker format (I:SPX) to database format (SPX)
    db_ticker = ticker.replace('I:', '') if ticker.startswith('I:') else ticker
    
    # Fetch all components
    walls, zero_gamma = fetch_gamma_walls_from_analysis(api_key, ticker, spot_price)
    pin_history = fetch_pin_snapshots_from_db(db_ticker, trading_date)
    spot_prices, intraday_high, intraday_low = fetch_spot_prices_from_db(db_ticker, trading_date)
    hv10_points = calculate_hv10_points(db_ticker, spot_price)
    
    # Fetch multi-expiry aggregate pin for enhanced predictions
    multi_expiry_aggregate_pin = None
    try:
        multi_expiry_analysis = options_gamma.get_multi_expiry_analysis(
            api_key=api_key,
            underlying=db_ticker,
            spot_price=spot_price,
            max_dte=7
        )
        if multi_expiry_analysis:
            multi_expiry_aggregate_pin = multi_expiry_analysis.get('aggregate_pin')
    except Exception as e:
        print(f"Warning: Could not fetch multi-expiry data: {e}")
    
    return {
        'walls': walls,
        'pin_history': pin_history,
        'zero_gamma': zero_gamma,
        'spot_prices': spot_prices if spot_prices else [spot_price],  # Fallback to current spot
        'intraday_high': intraday_high if intraday_high > 0 else spot_price,
        'intraday_low': intraday_low if intraday_low > 0 else spot_price,
        'hv10_points': hv10_points,
        'multi_expiry_aggregate_pin': multi_expiry_aggregate_pin
    }
