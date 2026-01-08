import yfinance as yf
from datetime import datetime

def get_live_market_data(symbol):
    """
    Fetch live market data for a given stock symbol.
    
    Args:
        symbol (str): Stock ticker symbol (e.g., 'AAPL', 'GOOGL')
    
    Returns:
        dict: Dictionary containing current market data
    """
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        
        data = {
            'symbol': symbol,
            'current_price': info.get('currentPrice'),
            'previous_close': info.get('previousClose'),
            'open': info.get('open'),
            'day_high': info.get('dayHigh'),
            'day_low': info.get('dayLow'),
            'volume': info.get('volume'),
            'market_cap': info.get('marketCap'),
            'timestamp': datetime.now().isoformat()
        }
        
        return data
    except Exception as e:
        return {'error': str(e)}

def get_multiple_quotes(symbols):
    """
    Fetch live market data for multiple symbols.
    
    Args:
        symbols (list): List of stock ticker symbols
    
    Returns:
        dict: Dictionary with symbols as keys and market data as values
    """
    results = {}
    for symbol in symbols:
        results[symbol] = get_live_market_data(symbol)
    return results


    def get_market_data_with_key(symbol, api_key):
        """
        Fetch market data using a specific API key.
        
        Args:
            symbol (str): Stock ticker symbol
            api_key (str): Your API key for authentication
        
        Returns:
            dict: Dictionary containing market data
        """
        # Note: yfinance doesn't require API keys for basic functionality
        # If you need authenticated access, consider using the official API
        return get_live_market_data(symbol)