"""
Real-time WebSocket streaming for live market data
Uses Polygon/Massive.com WebSocket API for real-time updates
"""

import streamlit as st
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage
try:
    from polygon.websocket.models import Market
except ImportError:
    # Fallback if Market enum doesn't exist
    class Market:
        STOCKS = "stocks"
        FOREX = "forex"
        CRYPTO = "crypto"
from typing import List, Dict, Callable
import threading
import queue
from datetime import datetime
import time

class RealTimeDataStream:
    """
    Manages real-time WebSocket connection to Polygon/Massive API
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = None
        self.data_queue = queue.Queue(maxsize=1000)
        self.is_connected = False
        self.thread = None
        self.latest_prices = {}
        self.trade_count = 0
        self.quote_count = 0
        self.error_message = None  # Store errors for main thread to display
        self.connection_status = "disconnected"
        
    def connect(self, tickers: List[str], callback: Callable = None):
        """
        Connect to WebSocket and subscribe to tickers
        
        Args:
            tickers: List of ticker symbols to subscribe to (e.g., ['SPY', 'QQQ'])
            callback: Optional callback function to process messages
        """
        try:
            # Create subscriptions for trades and quotes
            subscriptions = []
            for ticker in tickers:
                subscriptions.append(f"T.{ticker}")  # Trades
                subscriptions.append(f"Q.{ticker}")  # Quotes
                subscriptions.append(f"AM.{ticker}")  # Minute aggregates
            
            # Initialize WebSocket client
            self.client = WebSocketClient(
                api_key=self.api_key,
                feed="realtime",
                market=Market.STOCKS,  # Use STOCKS (all caps) not Stocks
                subscriptions=subscriptions,
                verbose=False
            )
            
            # Define message handler
            def handle_messages(msgs: List[WebSocketMessage]):
                for msg in msgs:
                    # Process based on event type
                    if hasattr(msg, 'event_type'):
                        # Use put_nowait to avoid blocking on high-volume feeds
                        try:
                            self.data_queue.put_nowait(msg)
                        except queue.Full:
                            # Drop oldest message and add new one
                            try:
                                self.data_queue.get_nowait()
                                self.data_queue.put_nowait(msg)
                            except:
                                pass  # If we can't add, just drop the message
                        
                        # Update latest prices
                        if hasattr(msg, 'symbol') and hasattr(msg, 'price'):
                            self.latest_prices[msg.symbol] = {
                                'price': msg.price,
                                'timestamp': datetime.now(),
                                'event_type': msg.event_type
                            }
                        
                        # Track counts
                        if msg.event_type == 'T':  # Trade
                            self.trade_count += 1
                        elif msg.event_type == 'Q':  # Quote
                            self.quote_count += 1
                        
                        # Call custom callback if provided
                        if callback:
                            try:
                                callback(msg)
                            except:
                                pass  # Don't let callback errors crash the stream
            
            # Start streaming in background thread
            def run_stream():
                try:
                    self.is_connected = True
                    self.connection_status = "connected"
                    self.client.run(handle_messages)
                except Exception as e:
                    # Store error for main thread to display
                    self.error_message = f"WebSocket error: {str(e)}"
                    self.is_connected = False
                    self.connection_status = "error"
            
            self.thread = threading.Thread(target=run_stream, daemon=True)
            self.thread.start()
            
            # Give it a moment to connect
            time.sleep(1)
            
            return True
            
        except Exception as e:
            # Store error for main thread to display
            self.error_message = f"Failed to connect to WebSocket: {str(e)}"
            self.connection_status = "error"
            return False
    
    def disconnect(self):
        """Disconnect from WebSocket"""
        try:
            if self.client:
                self.client.close()
            self.is_connected = False
            if self.thread:
                self.thread.join(timeout=2)
        except Exception as e:
            st.warning(f"Error disconnecting: {str(e)}")
    
    def get_latest_price(self, ticker: str) -> Dict:
        """Get latest price for a ticker"""
        return self.latest_prices.get(ticker)
    
    def get_recent_messages(self, n: int = 10) -> List:
        """Get n most recent messages from queue"""
        messages = []
        try:
            for _ in range(min(n, self.data_queue.qsize())):
                messages.append(self.data_queue.get_nowait())
        except queue.Empty:
            pass
        return messages
    
    def get_stats(self) -> Dict:
        """Get streaming statistics"""
        return {
            'connected': self.is_connected,
            'trade_count': self.trade_count,
            'quote_count': self.quote_count,
            'tickers_tracked': len(self.latest_prices),
            'queue_size': self.data_queue.qsize()
        }


def is_market_open() -> bool:
    """
    Check if US stock market is currently open
    Returns True during market hours (9:30 AM - 4:00 PM ET, Mon-Fri)
    """
    import pytz
    
    et_tz = pytz.timezone('US/Eastern')
    now_et = datetime.now(et_tz)
    
    # Check if it's a weekday (Monday = 0, Friday = 4)
    if now_et.weekday() >= 5:
        return False
    
    # Check if within market hours
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    
    return market_open <= now_et <= market_close


def format_websocket_message(msg: WebSocketMessage) -> str:
    """Format a WebSocket message for display"""
    try:
        if hasattr(msg, 'event_type'):
            if msg.event_type == 'T':  # Trade
                return f"Trade: {msg.symbol} @ ${msg.price:.2f} (Size: {msg.size})"
            elif msg.event_type == 'Q':  # Quote
                return f"Quote: {msg.symbol} Bid: ${msg.bid_price:.2f} Ask: ${msg.ask_price:.2f}"
            elif msg.event_type == 'AM':  # Minute aggregate
                return f"1min Bar: {msg.symbol} O:${msg.open:.2f} H:${msg.high:.2f} L:${msg.low:.2f} C:${msg.close:.2f}"
            else:
                return f"Event: {msg.event_type}"
        return str(msg)
    except Exception as e:
        return f"Message: {str(msg)[:100]}"


def start_streaming_session(api_key: str, tickers: List[str]) -> RealTimeDataStream:
    """
    Start a new streaming session
    
    Args:
        api_key: Polygon/Massive API key
        tickers: List of tickers to stream
    
    Returns:
        RealTimeDataStream instance
    """
    stream = RealTimeDataStream(api_key)
    
    def message_callback(msg):
        # Optional: Add custom processing here
        pass
    
    if stream.connect(tickers, callback=message_callback):
        return stream
    return None


def get_streaming_recommendations() -> str:
    """Get recommendations for best streaming performance"""
    market_status = "🟢 OPEN" if is_market_open() else "🔴 CLOSED"
    
    recommendations = f"""
    **Market Status**: {market_status}
    
    **WebSocket Streaming Tips**:
    - ✅ Data only flows during market hours (9:30 AM - 4:00 PM ET)
    - ✅ Subscribe to specific tickers instead of all (T.*) to reduce data volume
    - ✅ Minute aggregates (AM.*) are good for charts and predictions
    - ✅ Trades (T.*) show every transaction (high volume)
    - ✅ Quotes (Q.*) show bid/ask updates (very high volume)
    
    **Best Practices**:
    - Use ETF proxies: SPY, QQQ, DIA, IWM (lower volume than indexes)
    - For predictions: Subscribe to AM.* (minute bars) for updating charts
    - For monitoring: Subscribe to Q.* for real-time price tracking
    """
    
    return recommendations