"""
Real-time WebSocket streaming for live market data
Uses Polygon/Massive.com WebSocket API for real-time updates

================================================================================
DEPRECATED: WebSocket connections are now managed by FastAPI backend ONLY
================================================================================

This module's WebSocket functionality should NOT be used directly by Streamlit.
All WebSocket connections are now managed by the backend singleton pattern to
prevent Polygon 1008 "duplicate connection" errors.

USE INSTEAD:
- Backend: app/ingest/websocket_stream.py (singleton pattern)
- Backend: app/ingest/options_websocket_stream.py (singleton pattern)
- Streamlit: Call backend REST endpoints (/buffer/latest, /gex/{symbol})

STILL USABLE:
- is_market_open() - Market hours check (no WebSocket)
- get_snapshot_data() - REST API fallback (no WebSocket)
================================================================================
"""

import streamlit as st
from polygon import WebSocketClient, RESTClient
from typing import List, Dict, Callable, Optional
import threading
import queue
from datetime import datetime
import time
import warnings

class RealTimeDataStream:
    """
    Manages real-time WebSocket connection to Polygon/Massive API
    Supports options and index data streaming with connection pooling
    """
    
    # Class-level connection pool to prevent duplicates
    _active_connections = {}
    _connection_lock = threading.Lock()
    
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
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 3
        self.connection_id = None
        
    def connect(self, tickers: List[str], stream_type: str = "all", callback: Callable = None):
        """
        Connect to WebSocket and subscribe to tickers
        
        Automatically uses:
        - Real-time feed during market hours (9:30 AM - 4:00 PM ET, Mon-Fri)
        - Delayed feed (15-min delay) when market is closed
        
        Args:
            tickers: List of ticker symbols (for indices: SPX, NDX, DJI, RUT)
            stream_type: Type of stream - "indices", "options", or "all"
            callback: Optional callback function to process messages
        """
        try:
            # Create connection ID based on tickers and stream type
            self.connection_id = f"{'-'.join(sorted(tickers))}_{stream_type}"
            
            # Check if connection already exists - return existing instance
            with self._connection_lock:
                if self.connection_id in self._active_connections:
                    existing = self._active_connections[self.connection_id]
                    if existing.is_connected:
                        # Return the existing connection instance via special flag
                        # The caller should check for this and use the existing instance
                        self._reuse_existing = existing
                        return True
                    else:
                        # Clean up dead connection
                        del self._active_connections[self.connection_id]
            
            self.tickers = tickers
            
            # Determine which feed to use based on market hours
            market_is_open = is_market_open()
            if market_is_open:
                feed_host = 'socket.polygon.io'  # Real-time feed
                self.feed_type = 'real-time'
            else:
                feed_host = 'delayed.polygon.io'  # 15-min delayed feed
                self.feed_type = 'delayed'
            
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
                # For "all", use indices market since we're tracking indices + their options
                market = Market.Indices
            
            self.client = WebSocketClient(
                api_key=self.api_key,
                feed=feed_host,  # Smart switching: real-time during market hours, delayed otherwise
                market=market,
                subscriptions=subscriptions,
                verbose=False,  # Reduce logging noise
                max_reconnects=2  # Reduce from 5 to prevent connection spam
            )
            
            # Store connection info for display
            import pytz
            et_tz = pytz.timezone('US/Eastern')
            self.connection_time = datetime.now(et_tz)
            self.market_open_at_connection = market_is_open
            
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
            
            # Register in connection pool
            with self._connection_lock:
                self._active_connections[self.connection_id] = self
            
            # Give it a moment to connect
            time.sleep(0.5)  # Reduced from 1 second
            
            return True
            
        except Exception as e:
            # Store error for main thread to display
            self.error_message = f"Failed to connect to WebSocket: {str(e)}"
            self.connection_status = "error"
            return False
    
    def disconnect(self):
        """Disconnect from WebSocket and clean up connection pool"""
        try:
            # Remove from connection pool
            if self.connection_id:
                with self._connection_lock:
                    if self.connection_id in self._active_connections:
                        del self._active_connections[self.connection_id]
            
            # Close client connection
            if self.client:
                self.client.close()
            self.is_connected = False
            self.connection_status = "disconnected"
        except Exception as e:
            pass  # Don't call st.warning from here - not thread safe
    
    @classmethod
    def cleanup_stale_connections(cls):
        """Clean up disconnected connections from the pool"""
        with cls._connection_lock:
            stale_ids = [
                conn_id for conn_id, conn in cls._active_connections.items()
                if not conn.is_connected
            ]
            for conn_id in stale_ids:
                del cls._active_connections[conn_id]
    
    @classmethod
    def get_active_connection_count(cls) -> int:
        """Get the number of active connections"""
        with cls._connection_lock:
            return len(cls._active_connections)
    
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
        """Get streaming statistics with feed type and timestamp info"""
        import pytz
        et_tz = pytz.timezone('US/Eastern')
        current_et = datetime.now(et_tz)
        
        stats = {
            'connected': self.is_connected,
            'feed_type': getattr(self, 'feed_type', 'unknown'),
            'market_open': is_market_open(),
            'connection_time': getattr(self, 'connection_time', None),
            'current_time_et': current_et,
            'trade_count': self.trade_count,
            'quote_count': self.quote_count,
            'options_count': self.options_count,
            'index_count': self.index_count,
            'indices_tracked': len(self.latest_indices),
            'options_tracked': len(self.latest_options),
            'tickers_tracked': len(self.latest_prices),
            'queue_size': self.data_queue.qsize()
        }
        
        # Add data freshness indicator
        if hasattr(self, 'feed_type'):
            if self.feed_type == 'real-time':
                stats['data_delay'] = 'Live (Real-time)'
            else:
                stats['data_delay'] = '~15 min delay'
        
        return stats


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


def start_streaming_session(api_key: str, tickers: List[str], stream_type: str = "all") -> None:
    """
    DISABLED: WebSocket connections are now managed by the FastAPI backend ONLY.
    
    This function raises an error to prevent duplicate WebSocket connections
    that cause Polygon 1008 errors.
    
    USE INSTEAD:
    - Backend REST endpoints: /buffer/latest, /gex/{symbol}, /health
    - Backend singleton: app/ingest/websocket_stream.py
    
    Args:
        api_key: Polygon/Massive API key
        tickers: List of tickers to stream
        stream_type: Type of stream
    
    Raises:
        RuntimeError: Always - this function is disabled
    """
    raise RuntimeError(
        "start_streaming_session is DISABLED. "
        "WebSocket connections are managed by the FastAPI backend only. "
        "Use the /buffer/latest or /gex/{symbol} REST endpoints to read cached data. "
        "See: app/ingest/websocket_stream.py for backend singleton pattern."
    )
    # NOTE: Code below is unreachable - kept for documentation only
    stream = RealTimeDataStream(api_key)
    
    def message_callback(msg):
        # Optional: Add custom processing here
        pass
    
    if stream.connect(tickers, stream_type=stream_type, callback=message_callback):
        # Check if connection was reused - return existing instance
        if hasattr(stream, '_reuse_existing'):
            return stream._reuse_existing
        return stream
    return None


def get_streaming_recommendations() -> str:
    """Get recommendations for best streaming performance"""
    import pytz
    et_tz = pytz.timezone('US/Eastern')
    current_et = datetime.now(et_tz)
    
    market_status = "🟢 OPEN" if is_market_open() else "🔴 CLOSED"
    feed_status = "⚡ Real-time" if is_market_open() else "🕐 Delayed (~15 min)"
    
    recommendations = f"""
## 📡 Smart Feed Switching

**Current Status**: Market {market_status} | {feed_status} Feed Active | {current_et.strftime('%I:%M %p ET')}

The app automatically selects the optimal data feed:
- **During Market Hours** (9:30 AM - 4:00 PM ET, Mon-Fri): Uses real-time WebSocket feed for live data
- **After Hours & Weekends**: Uses delayed feed (~15 min) to prevent connection errors

### How It Works:
1. When you start streaming, the app checks if the market is currently open
2. If market is open → connects to `socket.polygon.io` (real-time)
3. If market is closed → connects to `delayed.polygon.io` (15-min delay)
4. All data is timestamped so you know exactly when it was captured


### Data Types Available:
- **Index Values**: Real-time index price movements (SPX, NDX, DJI, RUT)
- **Index Minute Aggregates**: OHLCV data for charts
- **Options Trades**: Every options transaction with timestamp
- **Options Quotes**: Bid/ask updates for options
- **Options Aggregates**: Minute bars for options analysis

### Best Practices:
- ✅ All data timestamped for accurate tracking
- ✅ Works 24/7 including after hours and weekends
- ✅ Automatic failsafe between real-time and delayed feeds
- ✅ Uses actual index data (SPX $6,700+, not SPY $670)
- ✅ Backup snapshot API available if streaming fails
"""
    
    return recommendations


def get_snapshot_data(api_key: str, tickers: List[str]) -> Optional[Dict[str, Dict]]:
    """
    Get snapshot data for index tickers as a backup method when streaming is unavailable
    
    Args:
        api_key: Polygon/Massive API key
        tickers: List of index tickers to fetch (e.g., ['I:SPX', 'I:NDX'])
    
    Returns:
        Dictionary mapping ticker to snapshot data (price, volume, etc.)
    """
    try:
        client = RESTClient(api_key)
        snapshot_data = {}
        
        try:
            # Get snapshot for indices using the correct API method
            # Note: ticker_any_of expects a LIST, not a comma-separated string
            results = client.get_snapshot_indices(ticker_any_of=tickers)
            
            # Process results - they come back as a list
            for snapshot in results:
                if snapshot and hasattr(snapshot, 'ticker'):
                    ticker = snapshot.ticker
                    
                    # Skip results with errors
                    if hasattr(snapshot, 'error') and snapshot.error:
                        print(f"  Warning: {ticker} returned error: {snapshot.error} - {getattr(snapshot, 'message', 'N/A')}")
                        continue
                    
                    # Extract data from session object (not day object for indices)
                    snapshot_data[ticker] = {
                        'ticker': ticker,
                        'price': snapshot.value if hasattr(snapshot, 'value') else None,
                        'open': snapshot.session.open if hasattr(snapshot, 'session') and hasattr(snapshot.session, 'open') else None,
                        'high': snapshot.session.high if hasattr(snapshot, 'session') and hasattr(snapshot.session, 'high') else None,
                        'low': snapshot.session.low if hasattr(snapshot, 'session') and hasattr(snapshot.session, 'low') else None,
                        'volume': None,  # Indices don't have volume
                        'prev_close': snapshot.session.previous_close if hasattr(snapshot, 'session') and hasattr(snapshot.session, 'previous_close') else None,
                        'timestamp': datetime.now(),
                        'source': 'snapshot'
                    }
                    
        except Exception as e:
            # Log the error for debugging
            print(f"Error fetching snapshots for {tickers}: {type(e).__name__}: {str(e)}")
            return None
        
        return snapshot_data if snapshot_data else None
        
    except Exception as e:
        print(f"Error in get_snapshot_data: {type(e).__name__}: {str(e)}")
        return None