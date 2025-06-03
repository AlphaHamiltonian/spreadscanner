#!/usr/bin/env python3
"""
Example WebSocket client for receiving trading signals from the Exchange Monitor server.
This shows how to connect to the server and handle different message types.
"""

import asyncio
import websockets
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TradingSignalClient:
    def __init__(self, server_url="ws://localhost:8765"):
        self.server_url = server_url
        self.running = False
        
    async def handle_trading_signal(self, data):
        """Handle incoming trading signal"""
        logger.info("=" * 60)
        logger.info("TRADING SIGNAL RECEIVED")
        logger.info(f"Timestamp: {datetime.now()}")
        logger.info(f"Source 1: {data['source1']}")
        logger.info(f"Source 2: {data['source2']}")
        logger.info(f"Exchange 1: {data['exchange1']}")
        logger.info(f"Exchange 2: {data['exchange2']}")
        logger.info(f"Spread %: {data['spread_pct']:.4f}%")
        
        # Log config details
        logger.info("\nConfig 1:")
        logger.info(json.dumps(data['configs']['config1'], indent=2))
        logger.info("\nConfig 2:")
        logger.info(json.dumps(data['configs']['config2'], indent=2))
        logger.info("=" * 60)
        
        # Here you would add your own logic to handle the trading signal
        # For example: submit orders, update a database, etc.
        
    async def handle_alert(self, data):
        """Handle incoming alert message"""
        logger.info(f"ALERT: {data['message']}")
        if data.get('spread_data'):
            logger.info(f"Spread data: {json.dumps(data['spread_data'], indent=2)}")
            
    async def handle_message(self, message):
        """Handle different message types from server"""
        try:
            msg = json.loads(message)
            msg_type = msg.get('type')
            
            if msg_type == 'welcome':
                logger.info(f"Connected to server: {msg['data']['message']}")
                logger.info(f"Server version: {msg['data']['server_version']}")
                
                # Subscribe to all signals
                subscribe_msg = {
                    "type": "subscribe",
                    "channels": ["all"]
                }
                return json.dumps(subscribe_msg)
                
            elif msg_type == 'status':
                data = msg['data']
                logger.info(f"Server status - Active clients: {data['active_clients']}, "
                          f"Signals broadcast: {data['signals_broadcast']}")
                
            elif msg_type == 'trading_signal':
                await self.handle_trading_signal(msg['data'])
                
            elif msg_type == 'alert':
                await self.handle_alert(msg['data'])
                
            elif msg_type == 'subscription_confirmed':
                logger.info(f"Subscription confirmed: {msg['subscribed_to']}")
                
            elif msg_type == 'pong':
                logger.debug("Received pong")
                
            else:
                logger.warning(f"Unknown message type: {msg_type}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode message: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            
        return None
        
    async def run(self):
        """Main client loop"""
        self.running = True
        reconnect_delay = 5
        
        while self.running:
            try:
                logger.info(f"Connecting to {self.server_url}...")
                
                async with websockets.connect(self.server_url) as websocket:
                    logger.info("Connected successfully!")
                    reconnect_delay = 5  # Reset delay on successful connection
                    
                    # Send periodic pings
                    async def send_pings():
                        while self.running:
                            await asyncio.sleep(30)
                            try:
                                ping_msg = {"type": "ping"}
                                await websocket.send(json.dumps(ping_msg))
                            except:
                                break
                                
                    # Start ping task
                    ping_task = asyncio.create_task(send_pings())
                    
                    try:
                        # Handle messages
                        async for message in websocket:
                            response = await self.handle_message(message)
                            if response:
                                await websocket.send(response)
                                
                    finally:
                        ping_task.cancel()
                        
            except websockets.exceptions.ConnectionClosed:
                logger.warning("Connection closed by server")
            except Exception as e:
                logger.error(f"Connection error: {e}")
                
            if self.running:
                logger.info(f"Reconnecting in {reconnect_delay} seconds...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 1.5, 60)  # Exponential backoff
                
    def stop(self):
        """Stop the client"""
        self.running = False

async def main():
    """Example usage"""
    import sys
    
    # Get server URL from command line or use default
    server_url = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8765"
    
    client = TradingSignalClient(server_url)
    
    try:
        await client.run()
    except KeyboardInterrupt:
        logger.info("\nShutting down client...")
        client.stop()

if __name__ == "__main__":
    # Run the client
    asyncio.run(main())