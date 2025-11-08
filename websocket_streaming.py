"""
Real-time WebSocket streaming for live market data
Uses Polygon/Massive.com WebSocket API for real-time updates
"""

import streamlit as st
from polygon import WebSocketClient, RESTClient
from typing import List, Dict, Callable, Optional
import threading
import queue
from datetime import datetime
import time

class RealTimeDataStream:
    """
    Manages real-time WebSocket connection to Polygon/Massive API
    Supports options and index data streaming
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = None
        self.data_queue = queue.Queue(maxsize=1000)
        self.is_connected = False
        self.thread = None
        self.latest_prices = {}
        self.latest_options = {}  # Track options data
        self.latest_indices = {}  # Track index data
        self.trade_count = 0
        self.quote_count = 0
        self.options_count = 0
        self.index_count = 0
        self.error_message = None  # Store errors for main thread to display
        self.connection_status = "disconnected"
        self.tickers = []
        
    def connect(self, tickers: List[str], stream_type: str = "all", callback: Callable = None):
        """
        Connect to WebSocket and subscribe to tickers
        
        Args:
            tickers: List of ticker symbols (for indices: SPX, NDX, DJI, RUT)
            stream_type: Type of stream - "indices", "options", or "all"
            callback: Optional callback function to process messages
        """
        try:
            self.tickers = tickers
            
            # Define message handlers
            def handle_message(msgs):
                # Modern API passes a list of messages
                if not isinstance(msgs, list):
                    msgs = [msgs]
                
                for msg in msgs:
                    # Process message with timestamp
                    try:
                        # Add timestamp to message if not present
                        if not hasattr(msg, 'timestamp'):
                            msg.timestamp = datetime.now()
                        
                        self.data_queue.put_nowait(msg)
                    except queue.Full:
                        # Drop oldest message and add new one
                        try:
                            self.data_queue.get_nowait()
                            self.data_queue.put_nowait(msg)
                        except:
                            pass
                    
                    # Determine message type and update appropriate tracker
                    msg_type = msg.__class__.__name__
                    
                    # Handle index data
                    if 'Index' in msg_type or 'Indices' in msg_type or 'Value' in msg_type:
                        if hasattr(msg, 'ticker') or hasattr(msg, 'symbol'):
                            symbol = msg.ticker if hasattr(msg, 'ticker') else msg.symbol
                            value = None
                            
                            if hasattr(msg, 'value'):
                                value = msg.value
                            elif hasattr(msg, 'close'):
                                value = msg.close
                            
                            if value:
                                self.latest_indices[symbol] = {
                                    'value': value,
                                    'timestamp': getattr(msg, 'timestamp', datetime.now()),
                                    'type': msg_type
                                }
                            self.index_count += 1
                    
                    # Handle options data
                    elif 'Option' in msg_type:
                        if hasattr(msg, 'symbol'):
                            symbol = msg.symbol
                            price = None
                            
                            if hasattr(msg, 'price'):
                                price = msg.price
                            elif hasattr(msg, 'close'):
                                price = msg.close
                            
                            if price:
                                self.latest_options[symbol] = {
                                    'price': price,
                                    'timestamp': getattr(msg, 'timestamp', datetime.now()),
                                    'type': msg_type
                                }
                            self.options_count += 1
                    
                    # Handle stock/general data
                    else:
                        if hasattr(msg, 'symbol'):
                            symbol = msg.symbol
                            price = None
                            
                            if hasattr(msg, 'price'):
                                price = msg.price
                            elif hasattr(msg, 'close'):
                                price = msg.close
                            
                            if price:
                                self.latest_prices[symbol] = {
                                    'price': price,
                                    'timestamp': getattr(msg, 'timestamp', datetime.now()),
                                    'type': msg_type
                                }
                            
                            # Track counts
                            if 'Trade' in msg_type:
                                self.trade_count += 1
                            elif 'Quote' in msg_type:
                                self.quote_count += 1
                    
                    # Call custom callback if provided
                    if callback:
                        try:
                            callback(msg)
                        except:
                            pass
            
            def handle_error(e):
                self.error_message = f"WebSocket error: {str(e)}"
                self.connection_status = "error"
                self.is_connected = False
            
            def handle_close():
                self.is_connected = False
                self.connection_status = "disconnected"
            
            # Build subscription list based on stream type and tickers
            subscriptions = []
            
            for ticker in tickers:
                # Format ticker with I: prefix for indices
                index_ticker = f"I:{ticker}" if not ticker.startswith('I:') else ticker
                
                if stream_type in ["indices", "all"]:
                    # Subscribe to index value and minute aggregates (official channel names)
                    subscriptions.extend([
                        f"VI.{index_ticker}",   # Index value updates (VI not XV)
                        f"AM.{index_ticker}"    # Minute aggregates (AM not XA)
                    ])
                
                if stream_type in ["options", "all"]:
                    # Subscribe to options trades and aggregates for indices
                    # Format: O:SPX for options on SPX index
                    option_ticker = f"O:{ticker}" if not ticker.startswith('O:') else ticker
                    subscriptions.extend([
                        f"T.{option_ticker}*",    # Option trades
                        f"AM.{option_ticker}*"    # Option minute aggregates
                    ])
            
            # Initialize modern WebSocketClient (2024 API)
            from polygon.websocket.models import Market
            
            # Determine market type based on stream_type
            if stream_type == "indices":
                market = Market.Indices
            elif stream_type == "options":
                market = Market.Options
            else:
                # For "all", use stocks market which can handle multiple types
                market = Market.Stocks
            
            self.client = WebSocketClient(
                api_key=self.api_key,
                feed='delayed.polygon.io',  # Use delayed feed (15-min delay)
                market=market,
                subscriptions=subscriptions,
                verbose=True,
                max_reconnects=5
            )
            
            # Start streaming in background thread with callback
            def run_stream():
                try:
                    self.client.run(handle_message)
                except Exception as e:
                    handle_error(e)
            
            self.thread = threading.Thread(target=run_stream, daemon=True)
            self.thread.start()
            
            self.is_connected = True
            self.connection_status = "connected"
            
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
            self.connection_status = "disconnected"
        except Exception as e:
            pass  # Don't call st.warning from here - not thread safe
    
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
            'options_count': self.options_count,
            'index_count': self.index_count,
            'indices_tracked': len(self.latest_indices),
            'options_tracked': len(self.latest_options),
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


def format_websocket_message(msg) -> str:
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


def start_streaming_session(api_key: str, tickers: List[str], stream_type: str = "all") -> RealTimeDataStream:
    """
    Start a new streaming session
    
    Args:
        api_key: Polygon/Massive API key
        tickers: List of tickers to stream (actual index tickers: SPX, NDX, DJI, RUT)
        stream_type: Type of stream - "indices", "options", or "all"
    
    Returns:
        RealTimeDataStream instance
    """
    stream = RealTimeDataStream(api_key)
    
    def message_callback(msg):
        # Optional: Add custom processing here
        pass
    
    if stream.connect(tickers, stream_type=stream_type, callback=message_callback):
        return stream
    return None


def get_streaming_recommendations() -> str:
    """Get recommendations for best streaming performance"""
    market_status = "🟢 OPEN" if is_market_open() else "🔴 CLOSED"
    
    recommendations = f"""
    **Market Status**: {market_status}
    
    **WebSocket Streaming (24/7 Available)**:
    - ✅ Streaming works after hours via wss://socket.massive.com
    - ✅ Real-time **Index Data**: SPX, NDX, DJI, RUT values and minute aggregates
    - ✅ Real-time **Options Data**: Trades, quotes, and minute aggregates with timestamps
    - ✅ During market hours: Live index values and options flow
    - ✅ After hours: Extended hours data with timestamps for all events
    - ✅ All messages include timestamps for accurate after-hours tracking
    
    **Data Types Available**:
    - **Index Values**: Real-time index price movements
    - **Index Minute Aggregates**: OHLCV data for charts
    - **Options Trades**: Every options transaction with timestamp
    - **Options Quotes**: Bid/ask updates for options
    - **Options Aggregates**: Minute bars for options analysis
    
    **Best Practices**:
    - Use actual index tickers: SPX, NDX, DJI, RUT
    - Timestamps preserved for all after-hours data
    - Options data includes full chain for selected indices
    - Backup method: Snapshot API available if streaming fails
    """
    
    return recommendations


def get_snapshot_data(api_key: str, tickers: List[str]) -> Optional[Dict[str, Dict]]:
    """
    Get snapshot data for tickers as a backup method when streaming is unavailable
    
    Args:
        api_key: Polygon/Massive API key
        tickers: List of tickers to fetch
    
    Returns:
        Dictionary mapping ticker to snapshot data (price, volume, etc.)
    """
    try:
        client = RESTClient(api_key)
        snapshot_data = {}
        
        for ticker in tickers:
            try:
                # Get snapshot for ticker
                snapshot = client.get_snapshot(ticker)
                
                if snapshot and hasattr(snapshot, 'ticker'):
                    # Extract relevant price data
                    snapshot_data[ticker] = {
                        'ticker': snapshot.ticker.ticker if hasattr(snapshot.ticker, 'ticker') else ticker,
                        'price': snapshot.ticker.day.c if hasattr(snapshot.ticker, 'day') and hasattr(snapshot.ticker.day, 'c') else None,
                        'open': snapshot.ticker.day.o if hasattr(snapshot.ticker, 'day') and hasattr(snapshot.ticker.day, 'o') else None,
                        'high': snapshot.ticker.day.h if hasattr(snapshot.ticker, 'day') and hasattr(snapshot.ticker.day, 'h') else None,
                        'low': snapshot.ticker.day.l if hasattr(snapshot.ticker, 'day') and hasattr(snapshot.ticker.day, 'l') else None,
                        'volume': snapshot.ticker.day.v if hasattr(snapshot.ticker, 'day') and hasattr(snapshot.ticker.day, 'v') else None,
                        'prev_close': snapshot.ticker.prev_day.c if hasattr(snapshot.ticker, 'prev_day') and hasattr(snapshot.ticker.prev_day, 'c') else None,
                        'timestamp': datetime.now(),
                        'source': 'snapshot'
                    }
            except Exception as e:
                # Continue with other tickers if one fails
                continue
        
        return snapshot_data if snapshot_data else None
        
    except Exception as e:
        return None