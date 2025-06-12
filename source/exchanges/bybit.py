import threading
import concurrent.futures
import time
import logging
import random
import orjson as json
import websocket
from source.core.config import stop_event
import time
from source.exchanges.base import BaseExchangeConnector
from source.core.utils import data_store, WebSocketManager, WriteLock
from source.core.symbol_matcher import symbol_matcher

logger = logging.getLogger(__name__) # module-specific logger


class BybitConnector(BaseExchangeConnector):
    """Connects to Bybit exchange API and WebSockets."""
    def __init__(self, app):
        super().__init__(app, "bybit")
        self.bybit_reconnect_attempts = 0
        self.symbol_last_update = {}
        self.correction_history = {}
        
        # Timer and reconnection management
        self._health_timer_active = False
        self._freshness_timer_active = False
        self._reconnection_lock = threading.Lock()
        self._last_reconnect_time = 0
        self._reconnect_cooldown = 15  # seconds
        
        # Add initialization for spot tracking attributes
        self.spot_active_symbols = set()
        self.spot_last_data_time = time.time()
        self.last_spot_pong_time = time.time()
        self.last_spot_ping_time = time.time()
        self.spot_connection_time = time.time()
        self.spot_subscribed_count = 0
        self.spot_subscription_batches = []

    # Timer management methods
    def _schedule_health_check(self):
        """Prevent multiple overlapping health check timers"""
        if self._health_timer_active:
            return  # Already scheduled
        self._health_timer_active = True
        threading.Timer(30.0, self._health_check_wrapper).start()

    def _health_check_wrapper(self):
        """Wrapper to reset timer flag after health check"""
        try:
            self.monitor_symbol_health()
        except Exception as e:
            logger.error(f"Error in health check: {e}")
        finally:
            self._health_timer_active = False

    def _schedule_freshness_check(self):
        """Prevent multiple overlapping freshness check timers"""
        if self._freshness_timer_active:
            return  # Already scheduled
        self._freshness_timer_active = True
        threading.Timer(15.0, self._freshness_check_wrapper).start()

    def _freshness_check_wrapper(self):
        """Wrapper to reset timer flag after freshness check"""
        try:
            self.check_data_freshness()
        except Exception as e:
            logger.error(f"Error in freshness check: {e}")
        finally:
            self._freshness_timer_active = False

    def _safe_reconnect(self, connection_type="linear"):
        """Thread-safe reconnection with cooldown to prevent race conditions"""
        current_time = time.time()
        
        # Check cooldown period
        if current_time - self._last_reconnect_time < self._reconnect_cooldown:
            logger.debug(f"Bybit {connection_type} reconnection on cooldown")
            return False
            
        # Try to acquire lock (non-blocking)
        if not self._reconnection_lock.acquire(blocking=False):
            logger.debug(f"Bybit {connection_type} reconnection already in progress")
            return False
            
        try:
            logger.info(f"Starting safe Bybit {connection_type} reconnection")
            self._last_reconnect_time = current_time
            
            if connection_type == "linear":
                if "bybit_linear_ws" in self.websocket_managers:
                    manager = self.websocket_managers["bybit_linear_ws"]
                    if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
            elif connection_type == "spot":
                if "bybit_spot_ws" in self.websocket_managers:
                    manager = self.websocket_managers["bybit_spot_ws"]
                    if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                        
            return True
            
        except Exception as e:
            logger.error(f"Error in safe Bybit {connection_type} reconnection: {e}")
            return False
        finally:
            self._reconnection_lock.release()

    def fetch_spot_symbols(self):
        """Fetch spot market information and map it to corresponding futures"""
        try:
            # First ensure we have the futures symbols
            futures_symbols = data_store.get_symbols('bybit')
            if not futures_symbols:
                logger.warning("No Bybit futures symbols available for mapping")
                return
                
            # Filter for only linear futures (ending with USDT)
            linear_futures = [symbol for symbol in futures_symbols if symbol.endswith('USDT')]
            logger.info(f"Filtered {len(linear_futures)} linear futures from {len(futures_symbols)} total Bybit futures")
            
            # Create a mapping of base currencies to their linear futures
            futures_bases = {}
            
            # Process futures symbols to extract base currencies
            for future in linear_futures:
                if future.endswith('USDT'):
                    base = future[:-4]  # Remove USDT suffix
                    futures_bases[base] = future
                    
            logger.info(f"Extracted {len(futures_bases)} base currencies from Bybit linear futures")
            
            # Now fetch spot symbols
            response = self.session.get('https://api.bybit.com/v5/market/instruments-info?category=spot')
            if response.status_code == 200:
                data = response.json()
                if data['retCode'] == 0 and 'list' in data['result']:
                    spot_to_future_map = {}
                    
                    # Process each spot symbol
                    for symbol_info in data['result']['list']:
                        if symbol_info['status'] == 'Trading':
                            spot_symbol = symbol_info['symbol']
                            base_coin = symbol_info.get('baseCoin', '')
                            quote_coin = symbol_info.get('quoteCoin', '')
                            
                            # Only consider USDT pairs for spot
                            if quote_coin != 'USDT':
                                continue
                                
                            # Direct match to a linear future (most reliable)
                            if spot_symbol in linear_futures:
                                spot_to_future_map[spot_symbol] = spot_symbol
                                logger.debug(f"Direct match: {spot_symbol} -> {spot_symbol}")
                                continue
                                
                            # Try base currency match
                            if base_coin in futures_bases:
                                future = futures_bases[base_coin]
                                spot_to_future_map[spot_symbol] = future
                                logger.debug(f"Base match: {spot_symbol} -> {future}")
                                continue
                                
                    # Store the mapping
                    data_store.bybit_spot_to_future_map = spot_to_future_map
                    logger.info(f"Created {len(spot_to_future_map)} spot-to-future mappings for Bybit (linear only)")
                    
                    # Create reverse mapping (future -> spot)
                    self.create_reverse_mapping()
                else:
                    logger.error(f"Error in Bybit spot response: {data.get('retMsg', 'Unknown error')}")
            else:
                logger.error(f"Error fetching Bybit spot symbols: {response.status_code}")
        except Exception as e:
            logger.error(f"Error in Bybit spot mapping: {e}")

    def create_reverse_mapping(self):
        """Create a reverse mapping from futures to spot"""
        if hasattr(data_store, 'bybit_spot_to_future_map'):
            # Create the reverse mapping
            future_to_spot_map = {}
            for spot_symbol, future_symbol in data_store.bybit_spot_to_future_map.items():
                future_to_spot_map[future_symbol] = spot_symbol
                
            # Store in data_store
            data_store.bybit_future_to_spot_map = future_to_spot_map
            logger.info(f"Created reverse mapping (future->spot) with {len(future_to_spot_map)} entries")

    def connect_spot_websocket(self):
        """Connect to Bybit WebSocket for spot prices using orderbook stream"""
        self.fetch_spot_symbols()
        
        # Ensure we have the mapping
        if not hasattr(data_store, 'bybit_spot_to_future_map') or not data_store.bybit_spot_to_future_map:
            logger.error("No valid spot-to-future mapping for Bybit")
            return
            
        # Get all spot symbols
        spot_symbols = list(data_store.bybit_spot_to_future_map.keys())

        logger.info(f"Preparing to subscribe to {len(spot_symbols)} Bybit spot symbols using orderbook stream")


        
        # Bybit V5 WebSocket for spot
        ws_url = "wss://stream.bybit.com/v5/public/spot"
        
        # Define all required handlers
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle pings
                if 'op' in data and data['op'] == 'ping':
                    pong_msg = json.dumps({"op": "pong"})
                    ws.send(pong_msg)
                    self.spot_last_data_time = time.time()
                    return
                    
                # Handle pongs
                if 'op' in data and data['op'] == 'pong':
                    self.spot_last_data_time = time.time()
                    self.last_spot_pong_time = time.time()
                    return
                    
                # Handle subscription responses
                if 'op' in data and data['op'] == 'subscribe':
                    if data.get('success'):
                        logger.info(f"Bybit spot orderbook subscription success: {data}")
                    else:
                        logger.warning(f"Bybit spot orderbook subscription failed: {data}")
                    return
                    
                # Process orderbook data
                if 'topic' in data and 'orderbook' in data['topic'] and 'data' in data:
                    self.spot_last_data_time = time.time()
                    
                    # Extract symbol from topic (format: orderbook.1.BTCUSDT)
                    topic_parts = data['topic'].split('.')
                    if len(topic_parts) >= 3:
                        spot_symbol = topic_parts[2]
                        
                        # Add to active symbols set for monitoring
                        self.spot_active_symbols.add(spot_symbol)
                        
                        # Check if this spot symbol is mapped to a future
                        if spot_symbol in data_store.bybit_spot_to_future_map:
                            future_symbol = data_store.bybit_spot_to_future_map[spot_symbol]
                            spot_key = f"{future_symbol}_SPOT"
                            
                            # Extract orderbook data
                            book_data = data['data']
                            if isinstance(book_data, list) and book_data:
                                book_data = book_data[0]  # Use first item if array
                                
                            # Verify we have both bids and asks with proper format
                            if (book_data and 'b' in book_data and 'a' in book_data and
                                    book_data['b'] and book_data['a']):
                                # Extract from orderbook format: [price, size]
                                bid_price = float(book_data['b'][0][0])  # Best bid price
                                ask_price = float(book_data['a'][0][0])  # Best ask price
                                bid_qty = float(book_data['b'][0][1])    # Best bid quantity
                                ask_qty = float(book_data['a'][0][1])    # Best ask quantity
                                
                                # Calculate mid price as approximation for "last"
                                mid_price = (bid_price + ask_price) / 2
                                
                                # Use the new update method (already uses write locks internally)
                                data_store.update_price_direct(
                                    'bybit', spot_key, bid_price, ask_price,
                                    bid_qty=bid_qty, ask_qty=ask_qty, last=mid_price
                                )
                                    
            except Exception as e:
                logger.error(f"Error processing Bybit spot orderbook message: {e}")
                import traceback
                logger.error(traceback.format_exc())
                
        def on_open(ws):
            logger.info("Bybit spot WebSocket connected for orderbook data")
            self.spot_connection_time = time.time()
            self.spot_last_data_time = time.time()
            
            # Subscribe in batches
            batch_subscribe_symbols(ws, spot_symbols)
            
            # Start heartbeat
            threading.Timer(5, start_heartbeat).start()
            
        # Add the missing error handler
        def on_error(ws, error):
            logger.error(f"Bybit spot WebSocket error: {error}")
            
        # Add the missing close handler
        def on_close(ws, close_status_code, close_msg):
            logger.warning(f"Bybit spot WebSocket closed: {close_status_code} - {close_msg}")
            
            # Reconnect after a short delay
            if not stop_event.is_set():
                delay = 5
                logger.info(f"Reconnecting Bybit spot in {delay} seconds")
                threading.Timer(delay, self.connect_spot_websocket).start()
                
        def start_heartbeat():
            if stop_event.is_set():
                return
                
            try:
                # Check if WebSocket is valid
                if "bybit_spot_ws" not in self.websocket_managers:
                    return
                    
                manager = self.websocket_managers["bybit_spot_ws"]
                if not hasattr(manager, 'ws') or not manager['ws']:
                    return
                    
                # Send ping
                ping_msg = json.dumps({"op": "ping"})
                manager['ws'].send(ping_msg)
                self.last_spot_ping_time = time.time()
            except Exception as e:
                logger.error(f"Error sending Bybit spot heartbeat: {e}")
                
            # Schedule next heartbeat
            if not stop_event.is_set():
                threading.Timer(20, start_heartbeat).start()
                
        def batch_subscribe_symbols(ws, symbols):
            """Subscribe to symbols in controlled batches using orderbook stream"""
            if not symbols:
                logger.warning("No Bybit spot symbols to subscribe")
                return
                
            # Use smaller batch size for reliable connection
            batch_size = 10
            batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
            logger.info(f"Prepared {len(batches)} orderbook subscription batches")
            
            # Track subscription progress
            self.spot_subscribed_count = 0
            self.spot_subscription_batches = []
            
            # Function to send a single batch
            def send_subscription_batch(batch_idx):
                if stop_event.is_set() or batch_idx >= len(batches):
                    if batch_idx >= len(batches):
                        logger.info(f"Completed all Bybit spot orderbook subscription batches ({len(symbols)} symbols)")
                    return
                    
                try:
                    # Get current batch
                    batch = batches[batch_idx]
                    
                    # Use orderbook.1.SYMBOL format
                    args = [f"orderbook.1.{symbol}" for symbol in batch]
                    
                    # Send subscription message
                    subscribe_msg = json.dumps({
                        "op": "subscribe",
                        "args": args
                    })
                    
                    # Safely check connection
                    if ws and hasattr(ws, 'sock') and ws.sock and hasattr(ws.sock, 'connected') and ws.sock.connected:
                        ws.send(subscribe_msg)
                        self.spot_subscribed_count += len(batch)
                        logger.info(f"Sent Bybit spot orderbook subscription batch {batch_idx+1}/{len(batches)}")
                        
                        # Store batch for potential resubscription
                        self.spot_subscription_batches.append((batch_idx, args))
                        
                        # Schedule next batch with progressive delay
                        if batch_idx + 1 < len(batches):
                            # Progressive delay to avoid rate limiting
                            base_delay = 2.0
                            batch_delay = base_delay * (1 + batch_idx / 20)
                            threading.Timer(batch_delay, send_subscription_batch, [batch_idx + 1]).start()
                    else:
                        logger.warning("Cannot send Bybit spot subscription - invalid socket")
                except Exception as e:
                    logger.error(f"Error sending Bybit spot subscription batch: {e}")
                    # Retry after delay
                    if not stop_event.is_set():
                        threading.Timer(5.0, send_subscription_batch, [batch_idx]).start()
                        
            # Start sending batches after short delay
            threading.Timer(2.0, lambda: send_subscription_batch(0)).start()
            
        # Create WebSocket - now with all required handlers defined
        manager = websocket.WebSocketApp(
            ws_url,
            on_message=on_message,
            on_open=on_open,
            on_error=on_error,
            on_close=on_close
        )
        
        # Start WebSocket in thread
        thread = threading.Thread(
            target=manager.run_forever,
            kwargs={
                'ping_interval': 30,
                'ping_timeout': 10,
                'sslopt': {"cert_reqs": 0}
            },
            daemon=True,
            name="bybit_spot_ws"
        )
        thread.start()
        
        # Store for management - fix structure to match what you expect
        self.websocket_managers["bybit_spot_ws"] = {
            'ws': manager,
            'thread': thread,
            'is_running': True
        }

    def fetch_symbols(self):
        """Fetch ONLY linear futures from Bybit"""
        try:
            # Only include linear futures category
            categories = ["linear"]  # REMOVED "inverse" - we only want USDT-margined
            
            # Use write lock for modifying symbols and tick sizes
            with WriteLock(data_store.exchange_rw_locks['bybit']):
                data_store.symbols['bybit'].clear()
                
            for cat in categories:
                url = f"https://api.bybit.com/v5/market/instruments-info?category={cat}"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['retCode'] == 0 and 'list' in data['result']:
                        instruments = data['result']['list']
                        
                        # Use write lock for updating symbols and tick sizes
                        with WriteLock(data_store.exchange_rw_locks['bybit']):
                            for symbol_info in instruments:
                                if symbol_info['status'] == 'Trading':
                                    symbol = symbol_info['symbol']
                                    
                                    # Double-check that this is a USDT linear future
                                    if not symbol.endswith('USDT'):
                                        logger.debug(f"Skipping non-USDT Bybit symbol: {symbol}")
                                        continue
                                        
                                    # Store the category with the symbol for proper WebSocket routing
                                    category_map = getattr(data_store, 'bybit_category_map', {})
                                    category_map[symbol] = cat
                                    data_store.bybit_category_map = category_map
                                    data_store.symbols['bybit'].add(symbol)
                                    
                                    # Extract tick size
                                    if 'priceFilter' in symbol_info and 'tickSize' in symbol_info['priceFilter']:
                                        if symbol not in data_store.tick_sizes['bybit']:
                                            data_store.tick_sizes['bybit'][symbol] = {}
                                        data_store.tick_sizes['bybit'][symbol]['future_tick_size'] = float(
                                            symbol_info['priceFilter']['tickSize']
                                        )
                                        
                        logger.info(f"Fetched {len(data_store.symbols['bybit'])} Bybit linear futures symbols.")
                    else:
                        logger.error(f"Error in Bybit symbol response: {data.get('retMsg','Unknown error')}")
                else:
                    logger.error(f"Error fetching Bybit symbols, Status={response.status_code}")
                    
            # Now update symbol maps
            data_store.update_symbol_maps()
            
        except Exception as e:
            logger.error(f"Error fetching Bybit symbols: {e}")

    def _create_linear_message_handler(self):
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle subscription confirmation
                if 'op' in data and data['op'] == 'subscribe':
                    if data.get('success', False):
                        logger.info(f"Bybit linear subscription confirmed: {data}")
                    else:
                        logger.warning(f"Bybit linear subscription failed: {data}")
                    return
                    
                # Handle ping/pong
                if 'op' in data:
                    if data['op'] == 'ping':
                        ws.send(json.dumps({"op": "pong"}))
                        return
                    elif data['op'] == 'pong':
                        self.last_pong_time = time.time()
                        return
                        
                # Process orderbook data directly
                if 'data' in data and 'topic' in data and 'orderbook' in data['topic']:
                    # Extract symbol from topic (format: orderbook.1.BTCUSDT)
                    topic_parts = data['topic'].split('.')
                    if len(topic_parts) >= 3:
                        symbol = topic_parts[2]
                        
                        # Record update for health monitoring
                        self._record_symbol_update('linear', symbol)
                        
                        # Process book data
                        book_data = data['data']
                        if isinstance(book_data, list) and book_data:
                            book_data = book_data[0]
                            
                        if 'b' in book_data and 'a' in book_data and book_data['b'] and book_data['a']:
                            best_bid = float(book_data['b'][0][0])
                            best_ask = float(book_data['a'][0][0])
                            bid_qty = float(book_data['b'][0][1])
                            ask_qty = float(book_data['a'][0][1])
                            
                            # Update using the new method (already uses write locks internally)
                            data_store.update_price_direct(
                                'bybit', symbol, best_bid, best_ask, bid_qty, ask_qty
                            )
                            return  # Successfully processed
            except Exception as e:
                logger.error(f"Error processing Bybit linear message: {e}")
                
        return on_message

    def _create_linear_open_handler(self, symbols):
        def on_open(ws):
            logger.info("Bybit linear WebSocket connected")
            # Subscribe to symbols in batches
            self._batch_subscribe_symbols(ws, "linear", symbols)
        return on_open

    def connect_websocket(self):
        """Connect to Bybit WebSocket for linear futures with improved reliability"""
        try:
            # Ensure we have symbols
            if not data_store.get_symbols('bybit'):
                self.fetch_symbols()
                
            symbols_list = list(data_store.get_symbols('bybit'))
            if not symbols_list:
                logger.error("No Bybit symbols found")
                return
                
            # Ensure we only have USDT perpetuals
            linear_symbols = [s for s in symbols_list if s.endswith('USDT')]
            logger.info(f"Connecting WebSocket for {len(linear_symbols)} Bybit linear futures")
            
            # Create WebSocket manager
            ws_url = "wss://stream.bybit.com/v5/public/linear"
            manager = WebSocketManager(
                url=ws_url,
                name="bybit_linear_ws",
                on_message=self._create_linear_message_handler(),
                on_open=self._create_linear_open_handler(linear_symbols),
                ping_interval=30,
                ping_timeout=10
            )
            
            self.websocket_managers["bybit_linear_ws"] = manager
            manager.connect()
            
            # Start symbol health monitoring with safe scheduling
            self._schedule_freshness_check()
            
        except Exception as e:
            logger.error(f"Error setting up Bybit WebSocket: {e}")

    def _record_symbol_update(self, category, symbol):
        """Record the timestamp of the latest update for a symbol"""
        if not hasattr(self, 'symbol_last_update'):
            self.symbol_last_update = {}
            
        if category not in self.symbol_last_update:
            self.symbol_last_update[category] = {}
            
        self.symbol_last_update[category][symbol] = time.time()
    
    def _batch_subscribe_symbols(self, ws, category, symbols):
        """Subscribe to symbols in small batches with safer connection handling"""
        if not symbols:
            logger.warning(f"No Bybit {category} symbols to subscribe")
            return
            
        # Use category-specific batch sizes
        if category == "linear":
            batch_size = 40  # Larger batch size for futures (Bybit's recommended max)
        else:  # spot or other categories
            batch_size = 10  # Smaller batch size for spot
            
        batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
        total_batches = len(batches)
        
        logger.info(f"Prepared {total_batches} subscription batches for {len(symbols)} Bybit {category} symbols")
        
        # Track subscription progress
        self.subscription_progress = {
            'total': len(symbols),
            'completed': 0,
            'category': category
        }
        
        def send_batch(batch_idx):
            if batch_idx >= total_batches or stop_event.is_set():
                if batch_idx >= total_batches:
                    logger.info(f"Completed all {total_batches} Bybit {category} subscription batches")
                return
                
            try:
                # Get current batch
                batch = batches[batch_idx]
                
                # SAFELY check connection state before sending
                if not ws or not hasattr(ws, 'sock') or not ws.sock or not hasattr(ws.sock, 'connected'):
                    logger.warning(f"Bybit {category} WebSocket not connected, cannot send subscription batch {batch_idx+1}")
                    return
                    
                if not ws.sock.connected:
                    logger.warning(f"Bybit {category} WebSocket connection closed, cannot send subscription batch {batch_idx+1}")
                    return
                    
                # Create subscription message
                args = [f"orderbook.1.{symbol}" for symbol in batch]
                subscribe_msg = {
                    "op": "subscribe",
                    "args": args
                }
                
                # Send subscription
                logger.info(f"Sending Bybit {category} subscription batch {batch_idx+1}/{total_batches} ({len(batch)} symbols)")
                ws.send(json.dumps(subscribe_msg))
                
                # Update progress
                self.subscription_progress['completed'] += len(batch)
                
                # Adaptive delay based on batch index to avoid overwhelming the connection
                delay = min(5.0, 1.0 + (batch_idx / 20))  # Gradually increase delay
                
                # Schedule next batch with delay
                if batch_idx + 1 < total_batches and not stop_event.is_set():
                    threading.Timer(delay, send_batch, [batch_idx + 1]).start()
                    
            except websocket._exceptions.WebSocketConnectionClosedException:
                logger.warning(f"Bybit {category} connection closed during subscription batch {batch_idx+1}")
            except Exception as e:
                logger.error(f"Error sending Bybit {category} batch {batch_idx+1}: {e}")
                # Try again after delay if error occurs
                if not stop_event.is_set():
                    threading.Timer(5.0, lambda: send_batch(batch_idx)).start()
                    
        # Start sending batches
        send_batch(0)
        
        # Monitor progress
        threading.Timer(30, self.monitor_subscription_progress).start()

    def monitor_subscription_progress(self):
        """Monitor subscription progress and retry if stalled"""
        if not hasattr(self, 'subscription_progress'):
            return
            
        if stop_event.is_set():
            return
            
        # Check if progress has stalled
        if self.subscription_progress['completed'] < self.subscription_progress['total']:
            progress_pct = (self.subscription_progress['completed'] / self.subscription_progress['total']) * 100
            logger.info(f"Bybit {self.subscription_progress['category']} subscription progress: "
                    f"{self.subscription_progress['completed']}/{self.subscription_progress['total']} "
                    f"symbols ({progress_pct:.1f}%)")
                    
            # If progress is very low, check if WebSocket is still good
            if progress_pct < 30 and self.subscription_progress['completed'] > 0:
                category = self.subscription_progress['category']
                ws_name = f"bybit_{category}_ws"
                
                if ws_name in self.websocket_managers:
                    manager = self.websocket_managers[ws_name]
                    
                    # Check WebSocket health
                    if not hasattr(manager, 'is_running') or not manager.is_running or not hasattr(manager, 'ws') or not manager.ws:
                        logger.warning(f"Bybit {category} WebSocket appears disconnected during subscription, reconnecting")
                        if hasattr(manager, 'connect'):
                            manager.connect()
                            
        # Schedule next check
        if not stop_event.is_set():
            threading.Timer(30, self.monitor_subscription_progress).start()

    def check_data_freshness(self):
        """Aggressively monitor data freshness and force reconnection if stale"""
        if stop_event.is_set():
            return
            
        try:
            current_time = time.time()
            last_update_time = 0
            
            # Find the most recent update time across all symbols - use read lock
            with data_store.exchange_rw_locks['bybit']:
                for symbol in data_store.price_data['bybit']:
                    if not symbol.endswith('_SPOT'):  # Only check futures
                        data = data_store.price_data['bybit'][symbol]
                        if 'timestamp' in data and data['timestamp'] > last_update_time:
                            last_update_time = data['timestamp']
            
            # If no recent updates, force reconnection
            if last_update_time > 0 and current_time - last_update_time > 20:
                logger.warning(f"No Bybit updates for {current_time - last_update_time:.1f}s, forcing reconnection")
                self._safe_reconnect("linear")
            
        except Exception as e:
            logger.error(f"Error checking Bybit data freshness: {e}")
            
        # Schedule next check with safe method
        if not stop_event.is_set():
            self._schedule_freshness_check()

    def monitor_symbol_health(self):
        """Unified health monitor for both futures and spot with selective resubscription"""
        if stop_event.is_set():
            return
            
        try:
            current_time = time.time()
            
            # PART 1: FUTURES MONITORING
            futures_stale_symbols = []
            futures_total = 0
            
            # Check which futures symbols are stale - use read lock
            with data_store.exchange_rw_locks['bybit']:
                futures_symbols = [s for s in data_store.get_symbols('bybit') if s.endswith('USDT')]
                futures_total = len(futures_symbols)
                
                for symbol in futures_symbols:
                    if symbol in data_store.price_data['bybit']:
                        data = data_store.price_data['bybit'][symbol]
                        # Symbol is stale if no updates in 2 minutes
                        if 'timestamp' not in data or current_time - data['timestamp'] > 120:
                            futures_stale_symbols.append(symbol)
                            
            # Calculate and log futures health metrics
            futures_stale_percent = (len(futures_stale_symbols) / futures_total * 100) if futures_total > 0 else 0
            logger.info(f"Bybit futures health: {futures_total - len(futures_stale_symbols)}/{futures_total} active ({futures_stale_percent:.1f}% stale)")

            if futures_stale_percent > 5 and futures_total > 20:  # More than 5% stale and at least 20 symbols
                # Check when the most recent update was received for any symbol
                most_recent_update = 0
                with data_store.exchange_rw_locks['bybit']:
                    for symbol in futures_symbols:
                        if symbol in data_store.price_data['bybit']:
                            data = data_store.price_data['bybit'][symbol]
                            if 'timestamp' in data and data['timestamp'] > most_recent_update:
                                most_recent_update = data['timestamp']
                
                # If no updates in the last 30 seconds, force reconnection
                if most_recent_update > 0 and current_time - most_recent_update > 30:
                    logger.warning(f"No Bybit futures updates received for {current_time - most_recent_update:.1f}s, forcing reconnection")
                    self._safe_reconnect("linear")

            # PART 2: SPOT MONITORING
            spot_stale_symbols = []
            spot_total = 0
            
            # Check which spot symbols are stale - use read lock
            expected_spot_symbols = set(getattr(data_store, 'bybit_spot_to_future_map', {}).keys())
            
            with data_store.exchange_rw_locks['bybit']:
                spot_keys = [k for k in data_store.price_data['bybit'].keys() if k.endswith('_SPOT')]
                spot_total = len(spot_keys)
                
                for key in spot_keys:
                    data = data_store.price_data['bybit'][key]
                    if 'timestamp' not in data or current_time - data['timestamp'] > 180:  # 3 min
                        # Extract original symbol from spot_key
                        if '_SPOT' in key:
                            symbol = key.replace('_SPOT', '')
                            # Find corresponding spot symbol
                            for spot_symbol, future_symbol in getattr(data_store, 'bybit_spot_to_future_map', {}).items():
                                if future_symbol == symbol:
                                    spot_stale_symbols.append(spot_symbol)
                                    break
                                    
            # Calculate and log spot health metrics
            spot_active_symbols = getattr(self, 'spot_active_symbols', set())
            spot_coverage_pct = (len(spot_active_symbols) / len(expected_spot_symbols)) * 100 if expected_spot_symbols else 0
            spot_stale_percent = (len(spot_stale_symbols) / spot_total * 100) if spot_total > 0 else 0
            
            logger.info(f"Bybit spot health: {len(spot_active_symbols)}/{len(expected_spot_symbols)} active symbols ({spot_coverage_pct:.1f}%)")
            logger.info(f"Bybit spot data freshness: {spot_total - len(spot_stale_symbols)}/{spot_total} fresh ({100 - spot_stale_percent:.1f}%)")
            
            # Check spot data age
            spot_data_age = current_time - getattr(self, 'spot_last_data_time', current_time)
            if spot_data_age > 120:  # No data for 2 minutes
                logger.warning(f"No Bybit spot data for {spot_data_age:.1f}s, reconnecting")
                self._safe_reconnect("spot")
                    
            # ACTION DECISION LOGIC
            # FUTURES ACTION:
            if futures_stale_symbols:
                # If many stale symbols (>50%), do a full reconnect
                if len(futures_stale_symbols) > futures_total * 0.5:
                    logger.warning(f"Too many stale futures symbols ({len(futures_stale_symbols)}/{futures_total}), performing full reconnect")
                    self._safe_reconnect("linear")
                # If severe issues (>70%), reinitialize completely
                elif len(futures_stale_symbols) > futures_total * 0.7:
                    logger.warning(f"Severe staleness in futures symbols ({len(futures_stale_symbols)}/{futures_total}), reinitializing")
                    self.reinitialize_linear_websocket()
                # Otherwise, just resubscribe to stale symbols
                else:
                    self.resubscribe_stale_symbols(futures_stale_symbols, "futures")
                    
            # SPOT ACTION:
            if spot_stale_symbols:
                # If many stale symbols (>50%), do a full reconnect
                if len(spot_stale_symbols) > spot_total * 0.5:
                    logger.warning(f"Too many stale spot symbols ({len(spot_stale_symbols)}/{spot_total}), performing full reconnect")
                    self._safe_reconnect("spot")
                # Otherwise, just resubscribe to stale symbols
                else:
                    self.resubscribe_stale_symbols(spot_stale_symbols, "spot")
                    
        except Exception as e:
            logger.error(f"Error in Bybit symbol health monitor: {e}")
            
        # Schedule next check with safe method
        if not stop_event.is_set():
            self._schedule_health_check()

    def reinitialize_linear_websocket(self):
        """Completely reinitialize the linear WebSocket connection"""
        try:
            logger.info("Reinitializing Bybit linear WebSocket from scratch")
            
            # Close existing connection if any
            if "bybit_linear_ws" in self.websocket_managers:
                try:
                    old_manager = self.websocket_managers["bybit_linear_ws"]
                    old_manager.disconnect()
                except Exception as e:
                    logger.error(f"Error closing old Bybit linear WebSocket: {e}")
                # Remove from managers dictionary
                del self.websocket_managers["bybit_linear_ws"]
                
            # Wait for resources to be released
            time.sleep(2)
            
            # Fetch latest symbols again to ensure freshness
            self.fetch_symbols()
            symbols_list = [s for s in data_store.get_symbols('bybit') if s.endswith('USDT')]
            if not symbols_list:
                logger.warning("No Bybit symbols found during reinitialization")
                return
                
            # Connect with fresh WebSocket
            category = "linear"
            ws_url = f"wss://stream.bybit.com/v5/public/{category}"
            
            # Create with existing handlers but fresh connection
            manager = WebSocketManager(
                url=ws_url,
                name="bybit_linear_ws",
                on_message=self._create_linear_message_handler(),
                on_open=self._create_linear_open_handler(symbols_list[:200]),  # Subscribe to fewer symbols initially
                ping_interval=15,
                ping_timeout=5,
                retry_count=20
            )
            
            # Store and connect
            self.websocket_managers["bybit_linear_ws"] = manager
            manager.connect()
            
            # Schedule subscription of remaining symbols
            def subscribe_remaining():
                if len(symbols_list) > 200:
                    remaining = symbols_list[200:]
                    logger.info(f"Scheduling subscription for remaining {len(remaining)} Bybit symbols")
                    self._batch_subscribe_symbols(manager.ws, "linear", remaining)
            threading.Timer(60.0, subscribe_remaining).start()
            
        except Exception as e:
            logger.error(f"Error reinitializing Bybit linear WebSocket: {e}")

    def resubscribe_stale_symbols(self, symbols, symbol_type="futures"):
        """Resubscribe only to stale symbols"""
        if not symbols:
            return
            
        # Determine manager and topic format based on symbol type
        if symbol_type == "futures":
            ws_name = "bybit_linear_ws"
            topic_format = "orderbook.1.{}"
        else:  # spot
            ws_name = "bybit_spot_ws"
            topic_format = "orderbook.1.{}"
            
        # Get WebSocket manager
        if ws_name not in self.websocket_managers:
            logger.warning(f"No Bybit {symbol_type} WebSocket manager found")
            return
            
        manager = self.websocket_managers[ws_name]
        
        # Check if WebSocket is connected
        if not hasattr(manager, 'ws') or not manager.ws or not hasattr(manager.ws, 'sock') or not manager.ws.sock:
            logger.warning(f"Bybit {symbol_type} WebSocket not available, cannot resubscribe")
            return
            
        if not manager.ws.sock.connected:
            logger.warning(f"Bybit {symbol_type} WebSocket not connected, cannot resubscribe")
            return

        # Use category-specific batch sizes
        if symbol_type == "futures":
            batch_size = 40  # Larger batch size for futures (Bybit's recommended max)
        else:  # spot or other categories
            batch_size = 10  # Smaller batch size for spot

        # Process symbols in small batches
        batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
        
        # Function to send a batch
        def send_batch(batch_idx):
            if batch_idx >= len(batches):
                logger.info(f"Completed resubscription of {len(symbols)} stale Bybit {symbol_type} symbols")
                return
                
            try:
                # Get current batch
                batch = batches[batch_idx]
                
                # Create subscription message
                args = [topic_format.format(symbol) for symbol in batch]
                subscribe_msg = {
                    "op": "subscribe",
                    "args": args
                }
                
                # Send subscription
                manager.ws.send(json.dumps(subscribe_msg))
                logger.info(f"Resubscribing to {len(batch)} stale Bybit {symbol_type} symbols (batch {batch_idx+1}/{len(batches)})")
                
                # Schedule next batch with delay
                if batch_idx + 1 < len(batches):
                    threading.Timer(2.0, send_batch, [batch_idx + 1]).start()
            except Exception as e:
                logger.error(f"Error resubscribing to stale Bybit {symbol_type} symbols: {e}")
                
                # If sending fails, try reconnection
                if ws_name in self.websocket_managers:
                    logger.warning(f"Subscription failed for {symbol_type}, attempting reconnection")
                    self._safe_reconnect(symbol_type.replace("futures", "linear"))
                        
        # Start sending batches
        send_batch(0)

    def _fetch_funding_batch(self, symbols_batch):
        """Fetch funding rates for a batch of symbols"""
        try:
            results = []
            # Process each symbol in the batch
            for symbol in symbols_batch:
                url = f"https://api.bybit.com/v5/market/funding/history?category=linear&symbol={symbol}&limit=1"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['retCode'] == 0 and 'list' in data['result'] and data['result']['list']:
                        funding_info = data['result']['list'][0]
                        # Convert to percentage and format
                        rate = float(funding_info['fundingRate']) * 100
                        formatted_rate = f"{rate:.4f}%"
                        results.append((symbol, formatted_rate))
                        
                # Add small delay to avoid rate limits
                time.sleep(0.1)
            for symbol, rate in results:
                data_store.update_funding_rate('bybit', symbol, rate)                
            # # Apply all updates at once - use write lock
            # with WriteLock(data_store.exchange_rw_locks['bybit']):
            #     for symbol, rate in results:
            #         data_store.funding_rates['bybit'][symbol] = rate
                    
            return len(results)
        except Exception as e:
            logger.error(f"Error in Bybit funding batch: {e}")
            return 0

    def update_funding_rates(self):
        """Fetch funding rates from Bybit API using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('bybit'))
                
                # Process symbols in parallel using thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    # Create tasks for batches of symbols
                    batch_size = 10
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    # Submit each batch to the executor
                    futures = [executor.submit(self._fetch_funding_batch, batch) for batch in batches]
                    
                    # Wait for all to complete with timeout
                    completed, _ = concurrent.futures.wait(
                        futures,
                        timeout=60,
                        return_when=concurrent.futures.ALL_COMPLETED
                    )
                    
                    # Sum up the number of successful updates
                    successful_updates = sum(future.result() for future in completed)
                    
                logger.info(f"Updated Bybit funding rates for {successful_updates} symbols")
            except Exception as e:
                logger.error(f"Error updating Bybit funding rates: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def _fetch_changes_batch(self, symbols_batch):
        """Fetch 24h changes for a batch of symbols"""
        try:
            results = []
            for symbol in symbols_batch:
                url = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={symbol}"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['retCode'] == 0 and 'list' in data['result'] and data['result']['list']:
                        ticker_info = data['result']['list'][0]
                        change_percent = float(ticker_info['price24hPcnt']) * 100
                        results.append((symbol, change_percent))
                        
                # Add small delay to avoid rate limits
                time.sleep(0.1)
                
            # Apply all updates at once - use write lock
            with WriteLock(data_store.exchange_rw_locks['bybit']):
                for symbol, change in results:
                    data_store.daily_changes['bybit'][symbol] = change
                    
            return len(results)
        except Exception as e:
            logger.error(f"Error in Bybit changes batch: {e}")
            return 0

    def update_24h_changes(self):
        """Fetch 24-hour price changes for symbols using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('bybit'))
                
                # Process symbols in parallel using thread pool
                with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                    # Create tasks for batches of symbols
                    batch_size = 10
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    # Submit each batch to the executor
                    futures = [executor.submit(self._fetch_changes_batch, batch) for batch in batches]
                    
                    # Wait for all to complete with timeout
                    completed, _ = concurrent.futures.wait(
                        futures,
                        timeout=60,
                        return_when=concurrent.futures.ALL_COMPLETED
                    )
                    
                    # Sum up the number of successful updates
                    successful_updates = sum(future.result() for future in completed)
                    
                logger.info(f"Updated Bybit 24h changes for {successful_updates} symbols")
            except Exception as e:
                logger.error(f"Error updating Bybit 24h changes: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def fetch_initial_spot_prices(self):
        """Get initial spot prices via REST API for immediate data"""
        if not hasattr(data_store, 'bybit_spot_to_future_map') or not data_store.bybit_spot_to_future_map:
            logger.warning("Cannot fetch initial spot prices: no spot-to-future mapping")
            return
            
        try:
            # Get symbols in batches
            symbols = list(data_store.bybit_spot_to_future_map.keys())
            batch_size = 20
            batches = [symbols[i:i+batch_size] for i in range(0, len(symbols), batch_size)]
            
            logger.info(f"Fetching initial prices for {len(symbols)} Bybit spot symbols")
            success_count = 0
            
            for i, batch in enumerate(batches):
                try:
                    # Construct symbol parameter (comma-separated)
                    symbol_param = ",".join(batch)
                    url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={symbol_param}"
                    response = self.session.get(url)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data['retCode'] == 0 and 'list' in data['result']:
                            for ticker in data['result']['list']:
                                spot_symbol = ticker['symbol']
                                
                                # Only process if we have a mapping
                                if spot_symbol in data_store.bybit_spot_to_future_map:
                                    future_symbol = data_store.bybit_spot_to_future_map[spot_symbol]
                                    spot_key = f"{future_symbol}_SPOT"
                                    
                                    # Extract prices
                                    last_price = float(ticker.get('lastPrice', 0))
                                    bid_price = float(ticker.get('bid1Price', 0))
                                    ask_price = float(ticker.get('ask1Price', 0))
                                    
                                    # Store using the new method (already uses write locks internally)
                                    data_store.update_price_direct(
                                        'bybit', spot_key, bid_price, ask_price, 
                                        last=last_price
                                    )
                                    success_count += 1
                                    
                    # Small delay to avoid rate limits
                    time.sleep(0.2)
                except Exception as e:
                    logger.error(f"Error fetching batch of initial Bybit spot prices: {e}")
                    
            logger.info(f"Completed initial Bybit spot price fetch: {success_count}/{len(symbols)} symbols")
        except Exception as e:
            logger.error(f"Error fetching initial Bybit spot prices: {e}")

    def initialize(self):
        """Initialize Bybit connector with proper sequence"""
        try:
            logger.info("Starting Bybit connector initialization...")

            # 1. Fetch futures symbols first (only linear/USDT perpetuals)
            self.fetch_symbols()
            futures_count = len(data_store.get_symbols('bybit'))
            logger.info(f"Initialized Bybit with {futures_count} futures symbols")
            
            # Retry if no futures symbols were found
            if futures_count == 0:
                logger.warning("No Bybit futures symbols found, retrying...")
                time.sleep(2)
                self.fetch_symbols()
                futures_count = len(data_store.get_symbols('bybit'))
                
            # 2. Create spot-to-future mapping
            self.fetch_spot_symbols()
            
            # Verify mapping exists
            spot_to_future = getattr(data_store, 'bybit_spot_to_future_map', {})
            if not spot_to_future:
                logger.warning("No Bybit spot-to-future mapping created, retrying...")
                time.sleep(2)
                self.fetch_spot_symbols()
                spot_to_future = getattr(data_store, 'bybit_spot_to_future_map', {})
                
            logger.info(f"Created mapping for {len(spot_to_future)} Bybit spot symbols")
            
            # 3. Load initial spot data via REST API
            self.fetch_initial_spot_prices()
            
            # 4. Connect to futures WebSocket (linear only)
            self.connect_websocket()
            
            # 5. Connect to spot WebSocket with all symbols
            self.connect_spot_websocket()
            
            # 6. Start health monitoring with safe scheduling
            self._schedule_health_check()

            # threading.Thread(
            #     target=self.update_funding_rates,
            #     daemon=True,
            #     name="bybit_funding_updater"
            # ).start()

            # threading.Thread(
            #     target=self.update_24h_changes,
            #     daemon=True,
            #     name="bybit_changes_updater"
            # ).start()
            
            logger.info("Bybit connector initialization complete")
        except Exception as e:
            logger.error(f"Error during Bybit connector initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())