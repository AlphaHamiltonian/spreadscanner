import asyncio
import websockets
import json
import logging
import threading
import ssl
import pathlib
from datetime import datetime
from typing import Set, Dict, Any, Optional

logger = logging.getLogger(__name__)

class TradingSignalServer:
    """WebSocket server for broadcasting trading signals with SSL support"""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8765, use_ssl: bool = True):
        self.host = host
        self.port = port
        self.use_ssl = use_ssl
        self.clients: Set[websockets.WebSocketServerProtocol] = set()
        self.server = None
        self.loop = None
        self.thread = None
        self.is_running = False
        self.message_queue = asyncio.Queue()
        
        # SSL Configuration
        self.ssl_context = None
        if use_ssl:
            self.ssl_context = self._create_ssl_context()
        
        # Statistics
        self.stats = {
            'connections_total': 0,
            'messages_sent': 0,
            'signals_broadcast': 0,
            'server_start_time': None
        }
        
        # Store last signal for new clients
        self.last_signal = None
        
    def _create_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Create SSL context for secure WebSocket connections"""
        try:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            
            # Look for certificate files in the project directory
            cert_path = pathlib.Path("server.crt")
            key_path = pathlib.Path("server.key")
            
            # If certificates don't exist in root, check source directory
            if not cert_path.exists():
                cert_path = pathlib.Path("source/server.crt")
                key_path = pathlib.Path("source/server.key")
            
            # If still not found, create self-signed certificate
            if not cert_path.exists() or not key_path.exists():
                logger.warning("SSL certificate files not found. Creating self-signed certificate...")
                self._create_self_signed_cert()
                cert_path = pathlib.Path("server.crt")
                key_path = pathlib.Path("server.key")
            
            ssl_context.load_cert_chain(cert_path, key_path)
            return ssl_context
            
        except Exception as e:
            logger.error(f"Failed to create SSL context: {e}")
            logger.warning("Falling back to non-SSL WebSocket server")
            self.use_ssl = False
            return None
    
    def _create_self_signed_cert(self):
        """Create a self-signed certificate for development/testing"""
        try:
            import subprocess
            
            # Generate self-signed certificate using openssl
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:4096",
                "-keyout", "server.key", "-out", "server.crt",
                "-days", "365", "-nodes", "-subj",
                "/C=US/ST=State/L=City/O=Organization/CN=localhost"
            ], check=True, capture_output=True)
            
            logger.info("Created self-signed SSL certificate")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to create self-signed certificate: {e}")
            raise
        except FileNotFoundError:
            logger.error("OpenSSL not found. Please install OpenSSL or provide SSL certificates")
            raise
        
    async def register_client(self, websocket: websockets.WebSocketServerProtocol):
        """Register a new client connection"""
        self.clients.add(websocket)
        self.stats['connections_total'] += 1
        client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        logger.info(f"Client connected: {client_info}")
        
        # Send welcome message
        welcome_msg = {
            "type": "welcome",
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "server_version": "1.0.0",
                "message": "Connected to Trading Signal Server",
                "secure": self.use_ssl
            }
        }
        await websocket.send(json.dumps(welcome_msg))
        
        # Send last signal if available (for clients that connect after a signal was sent)
        if self.last_signal:
            await websocket.send(json.dumps(self.last_signal))
            logger.info(f"Sent last signal to new client: {client_info}")
        
    async def unregister_client(self, websocket: websockets.WebSocketServerProtocol):
        """Remove a client connection"""
        if websocket in self.clients:
            self.clients.remove(websocket)
            client_info = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
            logger.info(f"Client disconnected: {client_info}")
            
    async def broadcast_signal(self, signal_data: Dict[str, Any]):
        """Broadcast trading signal to all connected clients"""
        # Create message
        message = {
            "type": "trading_signal",
            "timestamp": datetime.utcnow().isoformat(),
            "data": signal_data
        }
        
        # Store as last signal
        self.last_signal = message
        
        if not self.clients:
            logger.warning("No clients connected to broadcast signal")
            return
        
        message_json = json.dumps(message)
        
        # Send to all connected clients
        disconnected_clients = set()
        
        for client in self.clients:
            try:
                await client.send(message_json)
                self.stats['messages_sent'] += 1
            except websockets.exceptions.ConnectionClosed:
                disconnected_clients.add(client)
            except Exception as e:
                logger.error(f"Error sending to client: {e}")
                disconnected_clients.add(client)
                
        # Remove disconnected clients
        for client in disconnected_clients:
            await self.unregister_client(client)
            
        self.stats['signals_broadcast'] += 1
        logger.info(f"Broadcast signal to {len(self.clients)} clients")
        
    async def send_alert(self, alert_data: Dict[str, Any]):
        """Send alert message to all clients"""
        if not self.clients:
            return
            
        message = {
            "type": "alert",
            "timestamp": datetime.utcnow().isoformat(),
            "data": alert_data
        }
        
        message_json = json.dumps(message)
        
        disconnected_clients = set()
        for client in self.clients:
            try:
                await client.send(message_json)
                self.stats['messages_sent'] += 1
            except:
                disconnected_clients.add(client)
                
        for client in disconnected_clients:
            await self.unregister_client(client)
            
    async def handle_client(self, websocket: websockets.WebSocketServerProtocol):
        """Handle individual client connections"""
        await self.register_client(websocket)
        
        try:
            # Send current server status
            status_msg = {
                "type": "status",
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    "active_clients": len(self.clients),
                    "total_connections": self.stats['connections_total'],
                    "signals_broadcast": self.stats['signals_broadcast'],
                    "uptime_seconds": (datetime.utcnow() - self.stats['server_start_time']).total_seconds() if self.stats['server_start_time'] else 0
                }
            }
            await websocket.send(json.dumps(status_msg))
            
            # Handle incoming messages (if any)
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    # Handle different message types
                    if data.get('type') == 'ping':
                        pong_msg = {
                            "type": "pong",
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        await websocket.send(json.dumps(pong_msg))
                        
                    elif data.get('type') == 'subscribe':
                        # Handle subscription requests
                        response = {
                            "type": "subscription_confirmed",
                            "timestamp": datetime.utcnow().isoformat(),
                            "subscribed_to": data.get('channels', ['all'])
                        }
                        await websocket.send(json.dumps(response))
                        
                except json.JSONDecodeError:
                    error_msg = {
                        "type": "error",
                        "message": "Invalid JSON format"
                    }
                    await websocket.send(json.dumps(error_msg))
                except Exception as e:
                    logger.error(f"Error handling client message: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            logger.error(f"Error in client handler: {e}")
        finally:
            await self.unregister_client(websocket)
            
    async def message_processor(self):
        """Process queued messages"""
        while self.is_running:
            try:
                # Wait for messages with timeout
                message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                
                if message['type'] == 'trading_signal':
                    await self.broadcast_signal(message['data'])
                elif message['type'] == 'alert':
                    await self.send_alert(message['data'])
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    async def start_server(self):
        """Start the WebSocket server"""
        self.stats['server_start_time'] = datetime.utcnow()
        
        # Start message processor
        asyncio.create_task(self.message_processor())
        
        # Start WebSocket server with or without SSL
        if self.use_ssl and self.ssl_context:
            self.server = await websockets.serve(
                self.handle_client,
                self.host,
                self.port,
                ssl=self.ssl_context,
                ping_interval=20,
                ping_timeout=10
            )
            logger.info(f"Secure WebSocket server (WSS) started on {self.host}:{self.port}")
        else:
            self.server = await websockets.serve(
                self.handle_client,
                self.host,
                self.port,
                ping_interval=20,
                ping_timeout=10
            )
            logger.info(f"WebSocket server (WS) started on {self.host}:{self.port}")
        
        # Keep server running
        await asyncio.Future()  # Run forever
        
    def queue_trading_signal(self, source1: str, source2: str, exchange1: str, exchange2: str, 
                           spread_pct: float, config1: Dict, config2: Dict):
        """Queue a trading signal to be broadcast"""
        if not self.is_running:
            logger.warning("WebSocket server not running, cannot queue trading signal")
            return False
            
        if not self.loop:
            logger.warning("WebSocket event loop not initialized, cannot queue trading signal")
            return False
            
        signal_data = {
            "source1": source1,
            "source2": source2,
            "exchange1": exchange1,
            "exchange2": exchange2,
            "spread_pct": spread_pct,
            "configs": {
                "config1": config1,
                "config2": config2
            }
        }
        
        message = {
            'type': 'trading_signal',
            'data': signal_data
        }
        
        try:
            # Queue the message for async processing
            future = asyncio.run_coroutine_threadsafe(
                self.message_queue.put(message),
                self.loop
            )
            # Wait a bit to ensure it was queued (with timeout)
            future.result(timeout=1.0)
            logger.info(f"Trading signal queued successfully: {source1} vs {source2}")
            return True
        except Exception as e:
            logger.error(f"Failed to queue trading signal: {e}")
            return False
            
    def queue_spread_alert(self, message: str, spread_data: Dict[str, Any] = None):
        """Queue a spread alert to be broadcast"""
        if not self.is_running:
            logger.warning("WebSocket server not running, cannot queue alert")
            return False
            
        if not self.loop:
            logger.warning("WebSocket event loop not initialized, cannot queue alert")
            return False
            
        alert_data = {
            "message": message,
            "spread_data": spread_data if spread_data else {}
        }
        
        message_obj = {
            'type': 'alert',
            'data': alert_data
        }
        
        try:
            future = asyncio.run_coroutine_threadsafe(
                self.message_queue.put(message_obj),
                self.loop
            )
            future.result(timeout=1.0)
            logger.info(f"Alert queued successfully: {message}")
            return True
        except Exception as e:
            logger.error(f"Failed to queue alert: {e}")
            return False
            
    def run_in_thread(self):
        """Run the server in a separate thread"""
        self.is_running = True
        
        def run_server():
            # Create new event loop for this thread
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            try:
                self.loop.run_until_complete(self.start_server())
            except Exception as e:
                logger.error(f"WebSocket server error: {e}")
            finally:
                self.loop.close()
                
        self.thread = threading.Thread(target=run_server, daemon=True, name="websocket_server")
        self.thread.start()
        logger.info("WebSocket server thread started")
        
    def stop(self):
        """Stop the WebSocket server"""
        self.is_running = False
        
        if self.server:
            self.server.close()
            
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
            
        if self.thread:
            self.thread.join(timeout=5)
            
        logger.info("WebSocket server stopped")
        
    def has_clients(self) -> bool:
        """Check if any clients are connected"""
        return len(self.clients) > 0

    def get_client_info(self) -> list:
        """Get information about connected clients"""
        client_info = []
        for client in self.clients:
            try:
                info = {
                    "address": f"{client.remote_address[0]}:{client.remote_address[1]}",
                    "connected": True
                }
                client_info.append(info)
            except:
                pass
        return client_info
        
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics"""
        return {
            **self.stats,
            'active_clients': len(self.clients),
            'uptime_seconds': (datetime.utcnow() - self.stats['server_start_time']).total_seconds() if self.stats['server_start_time'] else 0
        }

# Global server instance - now with SSL by default
trading_signal_server = TradingSignalServer(use_ssl=True)