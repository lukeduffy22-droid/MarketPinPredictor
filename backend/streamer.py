"""
Real-time WebSocket streamer for Polygon market data
Zero-latency direct connection to market data
"""
import asyncio
import websockets
import json
import logging
from datetime import datetime
from typing import Dict, Callable, Optional
from collections import deque

from backend.config import (
    DATABENTO_API_KEY,
    DATABENTO_SYMBOLS,
    MARKET_DATA_PROVIDER,
    POLYGON_API_KEY,
    SYMBOLS,
    WS_RECONNECT_DELAY,
    MAX_BUFFER_SIZE,
)

logger = logging.getLogger(__name__)


class MarketDataStreamer:
    """High-performance WebSocket streamer for real-time market data"""

    def __init__(self, symbols: list[str] = None):
        self.symbols = symbols or SYMBOLS
        self.ws_url = f"wss://socket.polygon.io/indices"
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.is_running = False

        # Ring buffers for each symbol (fast in-memory access)
        self.buffers: Dict[str, deque] = {
            symbol: deque(maxlen=MAX_BUFFER_SIZE) for symbol in self.symbols
        }

        # Callbacks for real-time data processing
        self.callbacks: list[Callable] = []

        # Statistics
        self.messages_received = 0
        self.last_message_time: Dict[str, datetime] = {}

    def add_callback(self, callback: Callable):
        """Add callback to process incoming data"""
        self.callbacks.append(callback)

    async def connect(self):
        """Establish WebSocket connection"""
        try:
            logger.info(f"Connecting to Polygon WebSocket...")
            self.ws = await websockets.connect(
                self.ws_url,
                ping_interval=30,
                ping_timeout=10
            )

            # Authenticate
            auth_msg = {"action": "auth", "params": POLYGON_API_KEY}
            await self.ws.send(json.dumps(auth_msg))
            auth_response = await self.ws.recv()
            logger.info(f"Auth response: {auth_response}")

            # Subscribe to index data
            for symbol in self.symbols:
                subscribe_msg = {
                    "action": "subscribe",
                    "params": f"V.{symbol}"  # V = Value (index price)
                }
                await self.ws.send(json.dumps(subscribe_msg))
                logger.info(f"Subscribed to {symbol}")

            logger.info("✅ WebSocket connected and subscribed")
            return True

        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False

    async def process_message(self, message: dict):
        """Process incoming market data message"""
        try:
            # Handle different message types
            msg_type = message.get("ev")

            if msg_type == "V":  # Index value update
                symbol = message.get("sym", "").replace("I:", "")

                if symbol in self.symbols:
                    data_point = {
                        "symbol": symbol,
                        "timestamp": datetime.fromtimestamp(message.get("t", 0) / 1000),
                        "price": message.get("val"),
                        "volume": message.get("v"),
                    }

                    # Add to ring buffer
                    self.buffers[symbol].append(data_point)
                    self.last_message_time[symbol] = datetime.utcnow()
                    self.messages_received += 1

                    # Execute callbacks
                    for callback in self.callbacks:
                        try:
                            await callback(data_point)
                        except Exception as e:
                            logger.error(f"Callback error: {e}")

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def listen(self):
        """Main listening loop"""
        while self.is_running:
            try:
                if not self.ws:
                    connected = await self.connect()
                    if not connected:
                        logger.warning(f"Reconnecting in {WS_RECONNECT_DELAY}s...")
                        await asyncio.sleep(WS_RECONNECT_DELAY)
                        continue

                # Receive and process messages
                async for message in self.ws:
                    try:
                        data = json.loads(message)

                        # Handle both single messages and arrays
                        if isinstance(data, list):
                            for msg in data:
                                await self.process_message(msg)
                        else:
                            await self.process_message(data)

                    except json.JSONDecodeError as e:
                        logger.error(f"JSON decode error: {e}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning("WebSocket connection closed, reconnecting...")
                self.ws = None
                await asyncio.sleep(WS_RECONNECT_DELAY)

            except Exception as e:
                logger.error(f"Listener error: {e}")
                await asyncio.sleep(WS_RECONNECT_DELAY)

    async def start(self):
        """Start streaming"""
        logger.info("Starting market data streamer...")
        self.is_running = True
        await self.listen()

    async def stop(self):
        """Stop streaming"""
        logger.info("Stopping market data streamer...")
        self.is_running = False
        if self.ws:
            await self.ws.close()

    def get_latest_data(self, symbol: str, n: int = 1) -> list:
        """Get latest N data points for a symbol"""
        if symbol not in self.buffers:
            return []

        buffer = self.buffers[symbol]
        return list(buffer)[-n:] if n < len(buffer) else list(buffer)

    def get_all_latest(self) -> Dict[str, dict]:
        """Get latest data point for all symbols"""
        result = {}
        for symbol in self.symbols:
            latest = self.get_latest_data(symbol, 1)
            if latest:
                result[symbol] = latest[0]
        return result


# Global streamer instance
_streamer: Optional[MarketDataStreamer] = None


def _should_use_databento(provider: str, polygon_api_key: str | None, databento_api_key: str | None) -> bool:
    normalized = (provider or "auto").lower()
    if normalized == "databento":
        return True
    if normalized == "polygon":
        return False
    return bool(databento_api_key)


def get_streamer() -> MarketDataStreamer:
    """Get or create global streamer instance"""
    global _streamer
    if _streamer is None:
        use_databento = _should_use_databento(MARKET_DATA_PROVIDER, POLYGON_API_KEY, DATABENTO_API_KEY)
        if use_databento:
            from backend.databento_streamer import DatabentoGammaStreamer
            _streamer = DatabentoGammaStreamer(DATABENTO_SYMBOLS)
        else:
            _streamer = MarketDataStreamer()
    return _streamer


async def start_streaming():
    """Start the global streamer"""
    streamer = get_streamer()
    await streamer.start()


if __name__ == "__main__":
    # Test the streamer
    logging.basicConfig(level=logging.INFO)

    async def test_callback(data):
        print(f"📊 {data['symbol']}: ${data['price']:.2f} @ {data['timestamp']}")

    async def main():
        streamer = MarketDataStreamer(["SPX", "NDX"])
        streamer.add_callback(test_callback)
        await streamer.start()

    asyncio.run(main())
