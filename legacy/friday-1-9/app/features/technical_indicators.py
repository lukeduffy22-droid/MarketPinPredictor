"""Technical indicator helpers for the Streamlit dashboard."""

import numpy as np
import pandas as pd

DEFAULT_INDICATOR_PARAMS = {
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
    'momentum_period': 10,
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
        params = DEFAULT_INDICATOR_PARAMS
    
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

