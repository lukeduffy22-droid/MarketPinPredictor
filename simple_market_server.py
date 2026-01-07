#!/usr/bin/env python3
"""
Minimal market data collection server that bypasses corrupted Python dependencies.
Starts WebSocket connection to Polygon API for market data.
"""
import asyncio
import json
import os
import sys
import logging
from datetime import datetime
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)

# Try to import websockets without going through corrupted JSON
try:
    import websockets
    print("✓ websockets available")
except ImportError:
    print("✗ websockets not available - trying fallback")
    websockets = None

class PolygonWebSocketClient:
    """Direct Polygon WebSocket client"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws = None
        self.running = False
        self.subscriptions = ["V.I:SPX", "V.I:NDX", "V.I:DJI", "V.I:RUT"]
        
    async def connect(self):
        """Connect to Polygon WebSocket"""
        if not websockets:
            log.error("WebSockets library not available")
            return False
            
        try:
            uri = f"wss://socket.polygon.io/forex"
            log.info(f"Connecting to {uri}")
            self.ws = await websockets.connect(uri)
            
            # Authenticate
            auth_msg = {"action": "auth", "params": self.api_key}
            await self.ws.send(json.dumps(auth_msg))
            
            # Wait for auth response
            response = await self.ws.recv()
            auth_response = json.loads(response)
            log.info(f"Auth response: {auth_response}")
            
            # Subscribe to indices
            for subscription in self.subscriptions:
                sub_msg = {"action": "subscribe", "params": subscription}
                await self.ws.send(json.dumps(sub_msg))
                log.info(f"Subscribed to {subscription}")
            
            return True
            
        except Exception as e:
            log.error(f"Connection error: {e}")
            return False
    
    async def listen(self):
        """Listen for market data"""
        if not self.ws:
            log.error("WebSocket not connected")
            return
            
        self.running = True
        message_count = 0
        
        try:
            async for message in self.ws:
                try:
                    data = json.loads(message)
                    message_count += 1
                    
                    if message_count % 100 == 0:
                        log.info(f"Received {message_count} messages")
                    
                    # Log sample messages
                    if message_count <= 5:
                        log.info(f"Data sample: {data}")
                        
                except json.JSONDecodeError as e:
                    log.warning(f"JSON decode error: {e}")
                    
        except Exception as e:
            log.error(f"Listen error: {e}")
        finally:
            self.running = False
    
    async def run(self):
        """Main server loop"""
        connected = await self.connect()
        if connected:
            log.info("✓ Market data collection ACTIVE - Server is receiving data")
            await self.listen()
        else:
            log.error("✗ Failed to connect to market data")

async def main():
    """Start the market data collector"""
    api_key = os.environ.get("POLYGON_API_KEY")
    
    if not api_key:
        log.warning("POLYGON_API_KEY environment variable not set")
        log.info("Server running in demo mode - no actual data will be collected")
        log.info("To enable data collection, set: export POLYGON_API_KEY=your_key_here")
        
        # Just keep server alive
        while True:
            await asyncio.sleep(60)
    else:
        client = PolygonWebSocketClient(api_key)
        await client.run()

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("MARKET DATA COLLECTION SERVER")
    log.info(f"Starting at {datetime.now().isoformat()}")
    log.info("=" * 60)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Server stopped by user")
        sys.exit(0)
