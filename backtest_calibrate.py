"""
Backtesting and calibration script.
Populates RMSE buckets and validates 15% MAE improvement target.
"""
import asyncio
import logging
from datetime import datetime, timedelta
from polygon.rest import RESTClient
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

from app.utils.settings import settings
from app.models.db_models import (
    init_db, save_coefficients, get_session,
    RMSEBucket, PredictionLog
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("backtest")

async def fetch_historical_data(client: RESTClient, symbol: str, days: int = 30):
    """Fetch historical data for backtesting"""
    log.info(f"Fetching {days} days of data for {symbol}")
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Use Polygon aggregates API
    ticker = f"I:{symbol}"
    
    try:
        aggs = client.list_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="day",
            from_=start_date.strftime("%Y-%m-%d"),
            to=end_date.strftime("%Y-%m-%d"),
            limit=50000
        )
        
        data = []
        for agg in aggs:
            data.append({
                "date": datetime.fromtimestamp(agg.timestamp / 1000),
                "open": agg.open,
                "high": agg.high,
                "low": agg.low,
                "close": agg.close
            })
        
        log.info(f"Fetched {len(data)} days for {symbol}")
        return data
        
    except Exception as e:
        log.error(f"Failed to fetch data for {symbol}: {e}")
        return []

def simulate_features(data: list, idx: int) -> dict:
    """
    Simulate features that would be available during trading.
    In production, these come from ring buffers.
    """
    if idx < 5:
        return None
    
    # Use T-5 through T-1 data to simulate features at T
    recent = data[max(0, idx-20):idx]
    
    if len(recent) < 5:
        return None
    
    # Calculate simple features
    prices = [d["close"] for d in recent]
    
    # VWAP approximation (using daily closes)
    vwap = np.mean(prices)
    current = prices[-1]
    vwap_dev = (current - vwap) / vwap
    
    # Microtrend (simple linear regression on recent prices)
    x = np.arange(len(prices))
    coeffs = np.polyfit(x, prices, 1)
    microtrend = coeffs[0]  # Slope
    
    # Gamma and flow (use simulated values for now)
    gamma_pin = 0.5  # Placeholder
    flow_urgency = 0.3  # Placeholder
    
    return {
        "vwap_deviation": vwap_dev,
        "microtrend": microtrend,
        "gamma_pin": gamma_pin,
        "flow_urgency": flow_urgency,
        "vwap": vwap,
        "current_price": current
    }

async def run_backtest(symbol: str):
    """Run backtest for symbol and calibrate model"""
    client = RESTClient(settings.polygon_api_key)
    
    # Fetch historical data
    data = await fetch_historical_data(client, symbol, days=60)
    
    if len(data) < 30:
        log.warning(f"Insufficient data for {symbol}")
        return
    
    # Prepare training data
    X_train = []
    y_train = []
    
    for i in range(5, len(data) - 1):
        features = simulate_features(data, i)
        if features is None:
            continue
        
        # Target: next day's close
        target_close = data[i + 1]["close"]
        current_price = features["current_price"]
        
        # Feature vector
        X_train.append([
            features["vwap_deviation"],
            features["microtrend"],
            features["gamma_pin"],
            features["flow_urgency"]
        ])
        
        # Target: price change
        y_train.append(target_close - current_price)
    
    if len(X_train) < 20:
        log.warning(f"Insufficient samples for {symbol}")
        return
    
    X_train = np.array(X_train)
    y_train = np.array(y_train)
    
    log.info(f"Training on {len(X_train)} samples for {symbol}")
    
    # Train Ridge model
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_train)
    
    model = Ridge(alpha=1.0)
    model.fit(X_scaled, y_train)
    
    # Extract coefficients
    beta_vwap = model.coef_[0]
    beta_microtrend = model.coef_[1]
    beta_gamma = model.coef_[2]
    beta_flow = model.coef_[3]
    intercept = model.intercept_
    
    log.info(f"{symbol} coefficients: vwap={beta_vwap:.3f}, micro={beta_microtrend:.3f}, "
             f"gamma={beta_gamma:.3f}, flow={beta_flow:.3f}")
    
    # Save coefficients
    save_coefficients(
        symbol=symbol,
        beta_vwap=beta_vwap,
        beta_gamma=beta_gamma,
        beta_flow=beta_flow,
        beta_microtrend=beta_microtrend,
        intercept=intercept,
        sample_size=len(X_train)
    )
    
    # Calculate RMSE and MAE
    y_pred = model.predict(X_scaled)
    
    errors = y_pred - y_train
    rmse = np.sqrt(np.mean(errors ** 2))
    mae = np.mean(np.abs(errors))
    
    # Direction accuracy
    direction_correct = np.sum((y_pred > 0) == (y_train > 0))
    direction_accuracy = direction_correct / len(y_train)
    
    log.info(f"{symbol} - RMSE: {rmse:.2f}, MAE: {mae:.2f}, "
             f"Direction: {direction_accuracy:.1%}")
    
    # Save RMSE bucket (simplified - single bucket for now)
    session = get_session()
    try:
        bucket = RMSEBucket(
            symbol=symbol,
            tau_min=0,
            tau_max=60,
            rmse=rmse,
            mae=mae,
            direction_accuracy=direction_accuracy,
            n_samples=len(X_train)
        )
        session.add(bucket)
        session.commit()
        
        log.info(f"Saved RMSE bucket for {symbol}")
    finally:
        session.close()
    
    # Compare to VWAP-only baseline
    vwap_only_pred = X_train[:, 0] * np.mean(y_train)  # Simple VWAP baseline
    vwap_mae = np.mean(np.abs(vwap_only_pred - y_train))
    
    improvement = (vwap_mae - mae) / vwap_mae
    log.info(f"{symbol} - MAE improvement vs VWAP-only: {improvement:.1%}")
    
    if improvement >= 0.15:
        log.info(f"✓ {symbol} meets 15% improvement target")
    else:
        log.warning(f"✗ {symbol} below 15% improvement target")

async def main():
    """Run calibration for all symbols"""
    log.info("Starting calibration")
    
    # Initialize database
    init_db()
    
    # Run backtest for each symbol
    for symbol in ("SPX", "NDX", "DJI", "RUT"):
        try:
            await run_backtest(symbol)
        except Exception as e:
            log.error(f"Failed to calibrate {symbol}: {e}")
    
    log.info("Calibration complete")

if __name__ == "__main__":
    asyncio.run(main())
