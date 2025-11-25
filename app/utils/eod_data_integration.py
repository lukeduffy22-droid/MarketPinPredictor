"""
EOD Data Integration Layer

Fetches and transforms data from various sources (database, ring buffers, options_gamma)
into the format required by gamma_eod_predictor.py.

Also integrates AI enhancement for prediction critique and adjustment.
"""

from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, date, timedelta
from dataclasses import dataclass, asdict
import asyncio
import pytz
import numpy as np

from app.utils.gamma_eod_predictor import GammaWall, PinSnapshot
from database import get_gamma_snapshots_for_day, SessionLocal
from database import GammaPinSnapshot as DBGammaPinSnapshot
import options_gamma


@dataclass
class AIEnhancedPrediction:
    """Result of AI-enhanced EOD prediction."""
    original_prediction: float
    ai_adjusted_prediction: float
    ai_confidence: float
    adjustment_reason: str
    market_conditions: str
    risk_factors: List[str]
    recommendation: str
    ai_provider: str
    ai_available: bool
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


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


async def get_ai_enhanced_prediction(
    api_key: str,
    ticker: str,
    current_price: float,
    eod_prediction: float,
    gamma_data: Dict[str, Any],
    vwap_deviation: float = 0.0,
    microtrend: float = 0.0,
    minutes_to_close: int = 0,
    historical_accuracy: Optional[float] = None,
    multi_expiry_data: Optional[Dict[str, Any]] = None
) -> AIEnhancedPrediction:
    """
    Get AI-enhanced EOD prediction with critique and adjustment.
    
    Integrates the AI service to analyze the model's prediction and
    provide adjusted predictions with confidence scores.
    
    Args:
        api_key: Polygon API key
        ticker: Stock symbol (e.g., 'SPX', 'NDX')
        current_price: Current market price
        eod_prediction: Base model's EOD prediction
        gamma_data: Gamma exposure analysis data
        vwap_deviation: Current VWAP deviation
        microtrend: Recent price trend slope
        minutes_to_close: Minutes until market close
        historical_accuracy: Optional historical model accuracy
        multi_expiry_data: Optional multi-expiration gamma data
        
    Returns:
        AIEnhancedPrediction with original and AI-adjusted predictions
    """
    try:
        # Import here to avoid circular imports
        from app.services.ai_service import get_ai_service
        
        ai_service = get_ai_service()
        
        if not ai_service.is_available:
            return AIEnhancedPrediction(
                original_prediction=eod_prediction,
                ai_adjusted_prediction=eod_prediction,
                ai_confidence=0.0,
                adjustment_reason="AI service not available",
                market_conditions="Unable to analyze",
                risk_factors=[],
                recommendation="Use base model prediction",
                ai_provider="none",
                ai_available=False
            )
        
        # Get AI critique of the prediction
        critique = await ai_service.analyze_prediction(
            symbol=ticker,
            current_price=current_price,
            predicted_eod=eod_prediction,
            gamma_data=gamma_data,
            vwap_deviation=vwap_deviation,
            microtrend=microtrend,
            minutes_to_close=minutes_to_close,
            historical_accuracy=historical_accuracy
        )
        
        if critique is None:
            return AIEnhancedPrediction(
                original_prediction=eod_prediction,
                ai_adjusted_prediction=eod_prediction,
                ai_confidence=0.0,
                adjustment_reason="AI analysis failed",
                market_conditions="Unable to analyze",
                risk_factors=[],
                recommendation="Use base model prediction",
                ai_provider=ai_service.provider_name,
                ai_available=True
            )
        
        return AIEnhancedPrediction(
            original_prediction=critique.original_prediction,
            ai_adjusted_prediction=critique.adjusted_prediction,
            ai_confidence=critique.confidence,
            adjustment_reason=critique.adjustment_reason,
            market_conditions=critique.market_conditions,
            risk_factors=critique.risk_factors,
            recommendation=critique.recommendation,
            ai_provider=critique.provider,
            ai_available=True
        )
        
    except Exception as e:
        print(f"AI enhancement error: {e}")
        return AIEnhancedPrediction(
            original_prediction=eod_prediction,
            ai_adjusted_prediction=eod_prediction,
            ai_confidence=0.0,
            adjustment_reason=f"AI error: {str(e)}",
            market_conditions="Unable to analyze",
            risk_factors=["AI service error"],
            recommendation="Use base model prediction",
            ai_provider="error",
            ai_available=False
        )


def get_ai_enhanced_prediction_sync(
    api_key: str,
    ticker: str,
    current_price: float,
    eod_prediction: float,
    gamma_data: Dict[str, Any],
    vwap_deviation: float = 0.0,
    microtrend: float = 0.0,
    minutes_to_close: int = 0,
    historical_accuracy: Optional[float] = None,
    multi_expiry_data: Optional[Dict[str, Any]] = None
) -> AIEnhancedPrediction:
    """
    Synchronous wrapper for get_ai_enhanced_prediction.
    Use this when calling from non-async code (like Streamlit).
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(
        get_ai_enhanced_prediction(
            api_key=api_key,
            ticker=ticker,
            current_price=current_price,
            eod_prediction=eod_prediction,
            gamma_data=gamma_data,
            vwap_deviation=vwap_deviation,
            microtrend=microtrend,
            minutes_to_close=minutes_to_close,
            historical_accuracy=historical_accuracy,
            multi_expiry_data=multi_expiry_data
        )
    )


async def get_market_briefing(
    api_key: str,
    ticker: str,
    current_price: float,
    gamma_data: Dict[str, Any],
    eod_prediction: float,
    multi_expiry_data: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Get AI-generated market briefing for the given symbol.
    
    Returns a dictionary with market analysis including:
    - summary: Executive summary of market conditions
    - gamma_interpretation: What gamma positioning means
    - trend_assessment: Bullish/bearish/neutral with reasoning
    - key_levels: Support, resistance, and magnet levels
    - sentiment: Overall market sentiment
    - confidence: AI confidence in the analysis
    """
    try:
        from app.services.ai_service import get_ai_service
        
        ai_service = get_ai_service()
        
        if not ai_service.is_available:
            return None
        
        briefing = await ai_service.get_market_briefing(
            symbol=ticker,
            current_price=current_price,
            gamma_data=gamma_data,
            eod_prediction=eod_prediction,
            multi_expiry_data=multi_expiry_data
        )
        
        if briefing is None:
            return None
        
        return {
            'summary': briefing.summary,
            'gamma_interpretation': briefing.gamma_interpretation,
            'trend_assessment': briefing.trend_assessment,
            'key_levels': briefing.key_levels,
            'sentiment': briefing.sentiment,
            'confidence': briefing.confidence,
            'provider': briefing.provider
        }
        
    except Exception as e:
        print(f"Market briefing error: {e}")
        return None


def get_market_briefing_sync(
    api_key: str,
    ticker: str,
    current_price: float,
    gamma_data: Dict[str, Any],
    eod_prediction: float,
    multi_expiry_data: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Synchronous wrapper for get_market_briefing.
    Use this when calling from non-async code (like Streamlit).
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(
        get_market_briefing(
            api_key=api_key,
            ticker=ticker,
            current_price=current_price,
            gamma_data=gamma_data,
            eod_prediction=eod_prediction,
            multi_expiry_data=multi_expiry_data
        )
    )
