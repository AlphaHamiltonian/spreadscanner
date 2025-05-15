import threading
import concurrent.futures
import time
import logging
import random
import orjson as json
import websocket
from source.config import stop_event
import time
from source.exchanges.base import BaseExchangeConnector
from source.utils import data_store, WebSocketManager

logger = logging.getLogger(__name__) # module-specific logger

class BinanceConnector(BaseExchangeConnector):
    """Connects to Binance exchange API and WebSockets."""
    def __init__(self, app):
        super().__init__(app, "binance")
        
    def check_symbol_freshness(self):
        """Check for stale symbols in Binance data and trigger reconnection if needed"""
        if stop_event.is_set():
            return
        try:
            current_time = time.time()
            with data_store.lock:
                symbols = data_store.get_symbols('binance')
                stale_count = 0
                total_count = len(symbols)
                for symbol in symbols:
                    if symbol in data_store.price_data['binance']:
                        data = data_store.price_data['binance'][symbol]
                        if 'timestamp' not in data or current_time - data['timestamp'] > 60:
                            stale_count += 1
                            
                # If more than 20% of symbols are stale, reconnect all Binance WebSockets
                if total_count > 0 and stale_count > total_count * 0.2:
                    logger.warning(f"Binance data freshness issue: {stale_count}/{total_count} symbols are stale")
                    
                    # Reconnect all Binance WebSockets
                    for name, manager in self.websocket_managers.items():
                        try:
                            logger.info(f"Forcing reconnection of {name}")
                            
                            if isinstance(manager, dict):
                                # Dictionary type manager
                                if 'ws' in manager and manager['ws']:
                                    try:
                                        manager['ws'].close()
                                    except:
                                        pass
                                    time.sleep(1)
                                
                                if 'ws' in manager:
                                    # Close existing connection if any
                                    try:
                                        manager['ws'].close()
                                    except:
                                        pass
                                    # Wait a moment for cleanup
                                    time.sleep(1)
                                    
                                    # Create new WebSocket connection
                                    try:
                                        new_ws = websocket.WebSocketApp(
                                            manager.get('url', 'wss://fstream.binance.com/stream'),
                                            on_message=manager.get('on_message'),
                                            on_open=manager.get('on_open'),
                                            on_error=manager.get('on_error'),
                                            on_close=manager.get('on_close')
                                        )
                                        
                                        # Start in new thread
                                        thread = threading.Thread(
                                            target=new_ws.run_forever,
                                            kwargs={
                                                'ping_interval': 30,
                                                'ping_timeout': 10,
                                                'sslopt': {"cert_reqs": 0}
                                            },
                                            daemon=True,
                                            name=f"{name}_reconnect"
                                        )
                                        thread.start()
                                        
                                        # Update the manager dict with new connection
                                        manager['ws'] = new_ws
                                        manager['thread'] = thread
                                        manager['is_running'] = True
                                        manager['last_activity'] = time.time()
                                        logger.info(f"Reconnected {name} WebSocket")
                                    except Exception as e:
                                        logger.error(f"Error recreating WebSocket for {name}: {e}")
                            else:
                                # WebSocketManager object
                                manager.disconnect()
                                time.sleep(1)
                                manager.connect()
                        except Exception as e:
                            logger.error(f"Error reconnecting {name}: {e}")
        except Exception as e:
            logger.error(f"Error checking Binance symbol freshness: {e}")
            
        # Schedule next check
        if not stop_event.is_set():
            threading.Timer(120, self.check_symbol_freshness).start()

    def create_better_binance_websocket(self, url, name, on_message, on_open=None):
        """Create a more reliable Binance WebSocket connection"""
        # Define robust handlers
        def enhanced_on_open(ws):
            logger.info(f"Binance {name} WebSocket connected")
            if on_open:
                on_open(ws)
                
        def enhanced_on_ping(ws, message):
            """Immediately respond to ping frames"""
            logger.debug(f"Received ping from Binance {name}")
            if hasattr(ws, 'sock') and ws.sock:
                # Respond to server ping directly at socket level
                try:
                    ws.sock.pong(message)
                    logger.debug(f"Sent pong response to Binance {name}")
                except Exception as e:
                    logger.error(f"Error sending pong to Binance {name}: {e}")
                    
        def enhanced_on_close(ws, close_status_code, close_msg):
            logger.warning(f"Binance {name} WebSocket closed: {close_status_code} - {close_msg}")
            # Reconnect after a short delay
            if not stop_event.is_set():
                logger.info(f"Reconnecting Binance {name} in 5 seconds")
                threading.Timer(5, lambda: reconnect_ws()).start()
                
        def enhanced_on_error(ws, error):
            logger.error(f"Binance {name} WebSocket error: {error}")
            
        def reconnect_ws():
            try:
                new_ws = websocket.WebSocketApp(
                    url,
                    on_message=on_message,
                    on_open=enhanced_on_open,
                    on_error=enhanced_on_error,
                    on_close=enhanced_on_close,
                    on_ping=enhanced_on_ping
                )
                
                # Start in a new thread
                thread = threading.Thread(
                    target=new_ws.run_forever,
                    kwargs={
                        'ping_interval': 20,
                        'ping_timeout': 15,
                        'sslopt': {"cert_reqs": 0}
                    },
                    daemon=True,
                    name=name
                )
                thread.start()
                return new_ws, thread
            except Exception as e:
                logger.error(f"Error reconnecting {name}: {e}")
                return None, None
                
        # Initial connection
        ws, thread = reconnect_ws()
        
        # Store in the manager dictionary
        if isinstance(ws, websocket.WebSocketApp):
            manager = WebSocketManager(
                url=url,
                name=name,
                on_message=on_message,
                on_open=enhanced_on_open,
                on_error=enhanced_on_error,
                on_close=enhanced_on_close,
                ping_interval=20,
                ping_timeout=15
            )
            manager.ws = ws
            manager.thread = thread
            self.websocket_managers[name] = manager
            return manager
            
        return None

    def fetch_symbols(self):
        """Fetch all tradable futures symbols from Binance"""
        try:
            response = self.session.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
            if response.status_code == 200:
                data = response.json()
                with data_store.lock:
                    data_store.symbols['binance'].clear()
                    for symbol_info in data['symbols']:
                        if symbol_info['status'] == 'TRADING':
                            data_store.symbols['binance'].add(symbol_info['symbol'])
                            
                            # Extract tick size information
                            for filter_item in symbol_info['filters']:
                                if filter_item['filterType'] == 'PRICE_FILTER':
                                    if symbol_info['symbol'] not in data_store.tick_sizes['binance']:
                                        data_store.tick_sizes['binance'][symbol_info['symbol']] = {}
                                    data_store.tick_sizes['binance'][symbol_info['symbol']]['future_tick_size'] = float(filter_item['tickSize'])
                                    
                logger.info(f"Fetched {len(data_store.symbols['binance'])} Binance futures symbols")
                data_store.update_symbol_maps()
        except Exception as e:
            logger.error(f"Error fetching Binance symbols: {e}")

    def fetch_spot_symbols(self):
        """Fetch spot market information for corresponding futures"""
        try:
            response = self.session.get('https://api.binance.com/api/v3/exchangeInfo')
            if response.status_code == 200:
                data = response.json()
                futures_symbols = data_store.get_symbols('binance')
                
                with data_store.lock:
                    for symbol_info in data['symbols']:
                        # Convert spot symbols to match futures format if needed
                        spot_symbol = symbol_info['symbol']
                        
                        # Find matching futures symbols
                        for future_symbol in futures_symbols:
                            if future_symbol == spot_symbol:
                                # Store spot tick size for this corresponding future
                                for filter_item in symbol_info['filters']:
                                    if filter_item['filterType'] == 'PRICE_FILTER':
                                        if future_symbol not in data_store.tick_sizes['binance']:
                                            data_store.tick_sizes['binance'][future_symbol] = {}
                                        data_store.tick_sizes['binance'][future_symbol]['spot_tick_size'] = float(filter_item['tickSize'])
        except Exception as e:
            logger.error(f"Error fetching Binance spot symbols: {e}")

    def connect_futures_websocket(self):
        """Connect to Binance Futures WebSocket with improved ping/pong handling"""
        symbols = list(data_store.get_symbols('binance'))
        
        # Split symbols into batches of 200 (Binance limit)
        symbol_batches = [symbols[i:i + 200] for i in range(0, len(symbols), 200)]
        
        for i, symbol_batch in enumerate(symbol_batches):
            stream_param = "/".join(f"{s.lower()}@bookTicker" for s in symbol_batch)
            ws_url = f"wss://fstream.binance.com/stream?streams={stream_param}"
            
            # Create handler for this specific batch
            def on_message(ws, message):
                try:
                    data = json.loads(message)
                    
                    # For combined streams, data comes in a different format
                    if 'data' in data:
                        data = data['data']
                        
                    symbol = data['s']
                    
                    # Update directly without queueing
                    with data_store.lock:
                        if symbol not in data_store.price_data['binance']:
                            data_store.price_data['binance'][symbol] = {}
                            
                        data_store.price_data['binance'][symbol].update({
                            'bid': float(data['b']),
                            'ask': float(data['a']),
                            'bidQty': float(data['B']),
                            'askQty': float(data['A']),
                            'timestamp': time.time()
                        })
                        data_store.update_counters['binance'] += 1
                except Exception as e:
                    logger.error(f"Error processing Binance futures data: {e}")
                    
            def on_open(ws):
                logger.info(f"Binance futures WebSocket connected (batch {i+1}/{len(symbol_batches)})")
                
            # Use the enhanced WebSocket creation method
            manager = self.create_better_binance_websocket(
                url=ws_url,
                name=f"binance_futures_{i+1}",
                on_message=on_message,
                on_open=on_open
            )

    def connect_spot_websocket(self):
        """Connect to Binance spot WebSocket with improved reliability"""
        # Use a dictionary for tracking state
        self.spot_connection_state = {
            'connected': False,
            'last_data_time': time.time(),
            'last_pong_time': time.time(),
            'connection_time': time.time(),
            'reconnect_count': 0
        }
        
        # Base WebSocket URL
        base_url = "wss://stream.binance.com:9443/ws"
        
        def on_message(ws, message):
            """Handle WebSocket messages"""
            # Update activity timestamp
            self.spot_connection_state['last_data_time'] = time.time()
            
            try:
                data = json.loads(message)
                
                # Handle application-level ping
                if isinstance(data, dict) and 'ping' in data:
                    ws.send(json.dumps({"pong": data['ping']}))
                    logger.debug("Responded to Binance spot application ping")
                    return
                    
                # Process ticker data
                if isinstance(data, list):
                    # Process in batches for efficiency
                    updates = {}
                    for ticker in data:
                        if 's' in ticker:  # Symbol field
                            symbol = ticker['s']
                            spot_key = f"{symbol}_SPOT"
                            
                            # Extract prices
                            last_price = float(ticker['c'])
                            bid_price = float(ticker.get('b', last_price))
                            ask_price = float(ticker.get('a', last_price))
                            
                            # Get the corresponding futures data if it exists
                            future_data = data_store.get_price_data('binance', symbol)
                            
                            # If we have both futures and spot data, update atomically
                            if future_data and 'bid' in future_data and 'ask' in future_data:
                                spot_data = {
                                    'bid': bid_price,
                                    'ask': ask_price,
                                    'last': last_price
                                }
                                data_store.update_related_prices('binance', symbol, future_data, spot_key, spot_data)
                            else:
                                data_store.update_price_direct('binance', spot_key, bid_price, ask_price, last=last_price)                    
                    # Log occasionally
                    if random.random() < 0.0001:  # Very infrequent
                        logger.info(f"Processed {len(updates) if updates else 'batch of'} Binance spot symbols")
            except Exception as e:
                logger.error(f"Error processing Binance spot message: {e}")
        
        def on_open(ws):
            """Handle WebSocket open event"""
            logger.info("Binance spot WebSocket connected successfully")
            self.spot_connection_state['connected'] = True
            self.spot_connection_state['last_data_time'] = time.time()
            self.spot_connection_state['last_pong_time'] = time.time()
            self.spot_connection_state['connection_time'] = time.time()
            self.spot_connection_state['reconnect_count'] = 0
            
            # Subscribe to all tickers (single stream)
            subscribe_msg = json.dumps({
                "method": "SUBSCRIBE",
                "params": ["!ticker@arr"],
                "id": 1
            })
            
            # Send subscription
            ws.send(subscribe_msg)
            logger.info("Sent Binance spot subscription request")
        
        def on_error(ws, error):
            """Handle WebSocket errors"""
            logger.error(f"Binance spot WebSocket error: {error}")
            self.spot_connection_state['connected'] = False
        
        def on_close(ws, close_status_code, close_msg):
            """Handle WebSocket close events"""
            logger.warning(f"Binance spot WebSocket closed: {close_status_code} - {close_msg}")
            self.spot_connection_state['connected'] = False
            
            # Only reconnect if not shutting down
            if not stop_event.is_set():
                # Force immediate reconnection
                create_connection()
        
        def on_ping(ws, message):
            """Handle protocol-level ping frames"""
            self.spot_connection_state['last_data_time'] = time.time()
        
        def on_pong(ws, message):
            """Handle protocol-level pong responses"""
            self.spot_connection_state['last_data_time'] = time.time()
            self.spot_connection_state['last_pong_time'] = time.time()
        
        def create_connection():
            """Create and start a new WebSocket connection"""
            try:
                # Force close any existing connection
                if hasattr(self, 'spot_ws') and self.spot_ws:
                    try:
                        self.spot_ws.close()
                        logger.info("Closed existing Binance spot WebSocket")
                    except Exception as e:
                        logger.error(f"Error closing existing Binance spot connection: {e}")
                
                # Clear references
                self.spot_ws = None
                self.spot_thread = None
                
                # Create new WebSocket with all handlers
                new_ws = websocket.WebSocketApp(
                    f"{base_url}/!ticker@arr",
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                    on_ping=on_ping,
                    on_pong=on_pong
                )
                
                # Store reference
                self.spot_ws = new_ws
                
                # Start in a new thread
                new_thread = threading.Thread(
                    target=lambda: new_ws.run_forever(
                        ping_interval=20,
                        ping_timeout=15,
                        sslopt={"cert_reqs": 0}
                    ),
                    daemon=True,
                    name="binance_spot_ws"
                )
                new_thread.start()
                self.spot_thread = new_thread
                
                # Store in manager dictionary for consistent management
                self.websocket_managers["binance_spot_ws"] = {
                    'ws': new_ws,
                    'thread': new_thread,
                    'is_running': True,
                    'last_activity': time.time()
                }
                
                logger.info("Created new Binance spot WebSocket connection")
                return new_ws
            except Exception as e:
                logger.error(f"Error creating Binance spot connection: {e}")
                if not stop_event.is_set():
                    threading.Timer(5.0, create_connection).start()
        
        def monitor_health():
            """Aggressive health monitoring with forced reconnection"""
            if stop_event.is_set():
                return
            
            try:
                current_time = time.time()
                state = self.spot_connection_state
                
                # ALWAYS reconnect if no pong response for 3 minutes
                if current_time - state.get('last_pong_time', 0) > 180:
                    logger.warning(f"Forcing Binance spot reconnection: no pong for {current_time - state.get('last_pong_time', 0):.1f}s")
                    create_connection()
                    
                # Or if no data for 90 seconds
                elif current_time - state.get('last_data_time', 0) > 90:
                    logger.warning(f"Forcing Binance spot reconnection: no data for {current_time - state.get('last_data_time', 0):.1f}s")
                    create_connection()
                
                # Or if connection is marked as disconnected
                elif not state.get('connected', False):
                    logger.warning("Forcing Binance spot reconnection: connection marked as disconnected")
                    create_connection()
                    
                # Log health status
                else:
                    logger.info(f"Binance spot connection healthy: last data {current_time - state.get('last_data_time', 0):.1f}s ago")
            except Exception as e:
                logger.error(f"Error in Binance spot health monitor: {e}")
            
            # Schedule next check if not stopping
            if not stop_event.is_set():
                threading.Timer(30.0, monitor_health).start()
        
        # Start initial connection
        create_connection()
        
        # Start health monitoring after short delay
        threading.Timer(60.0, monitor_health).start()

    def _fetch_funding_batch(self, symbols_batch):
        """Fetch funding rates for a batch of symbols"""
        try:
            # Construct comma-separated symbols for batch request if API supports it
            symbols_param = ",".join(symbols_batch)
            url = f'https://fapi.binance.com/fapi/v1/premiumIndex?symbols={symbols_param}'
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    symbol = item['symbol']
                    # Convert to percentage and format
                    rate = float(item['lastFundingRate']) * 100
                    formatted_rate = f"{rate:.4f}%"
                    data_store.update_funding_rate('binance', symbol, formatted_rate)
            else:
                # Fallback to individual requests if batch fails
                for symbol in symbols_batch:
                    url = f'https://fapi.binance.com/fapi/v1/premiumIndex?symbol={symbol}'
                    response = self.session.get(url)
                    if response.status_code == 200:
                        item = response.json()
                        rate = float(item['lastFundingRate']) * 100
                        formatted_rate = f"{rate:.4f}%"
                        data_store.update_funding_rate('binance', symbol, formatted_rate)
        except Exception as e:
            logger.error(f"Error fetching funding batch: {e}")

    def update_funding_rates(self):
        """Fetch funding rates from Binance API using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('binance'))
                
                # Process symbols in parallel using thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    # Create tasks for batches of symbols
                    batch_size = 20
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    # Submit each batch to the executor
                    futures = [executor.submit(self._fetch_funding_batch, batch) for batch in batches]
                    
                    # Wait for all to complete with timeout
                    concurrent.futures.wait(futures, timeout=30)
                    
                logger.info(f"Updated Binance funding rates for {len(symbols_list)} symbols")
            except Exception as e:
                logger.error(f"Error updating Binance funding rates: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def _fetch_changes_batch(self, symbols_batch):
        """Fetch 24h changes for a batch of symbols"""
        try:
            # Use comma-separated symbols if API supports it
            symbols_param = ",".join(symbols_batch)
            url = f'https://fapi.binance.com/fapi/v1/ticker/24hr?symbols={symbols_param}'
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                with data_store.lock:
                    for item in data:
                        symbol = item['symbol']
                        change_percent = float(item['priceChangePercent'])
                        data_store.daily_changes['binance'][symbol] = change_percent
            else:
                # Fallback to individual requests
                for symbol in symbols_batch:
                    url = f'https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={symbol}'
                    response = self.session.get(url)
                    if response.status_code == 200:
                        item = response.json()
                        change_percent = float(item['priceChangePercent'])
                        with data_store.lock:
                            data_store.daily_changes['binance'][symbol] = change_percent
        except Exception as e:
            logger.error(f"Error fetching 24h changes batch: {e}")

    def update_24h_changes(self):
        """Fetch 24-hour price changes for symbols using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('binance'))
                
                # Process symbols in parallel using thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    # Create tasks for batches of symbols
                    batch_size = 20
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    # Submit each batch to the executor
                    futures = [executor.submit(self._fetch_changes_batch, batch) for batch in batches]
                    
                    # Wait for all to complete with timeout
                    concurrent.futures.wait(futures, timeout=30)
                    
                logger.info(f"Updated Binance 24h changes for {len(symbols_list)} symbols")
            except Exception as e:
                logger.error(f"Error updating Binance 24h changes: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)
