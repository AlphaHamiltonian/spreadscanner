import threading
import concurrent.futures
import time
import logging
import random
import orjson as json
import websocket
from collections import defaultdict, deque
from source.config import stop_event
from source.exchanges.base import BaseExchangeConnector
from source.utils import data_store, WebSocketManager, WriteLock

logger = logging.getLogger(__name__)

class BinanceConnector(BaseExchangeConnector):
    """Binance exchange connector with enhanced recovery and monitoring."""
    
    def __init__(self, app):
        super().__init__(app, "binance")
        
        # Connection tracking
        self.ws_batches = {}  # batch_name -> {ws, thread, symbols, state}
        self.symbol_to_batch = {}  # symbol -> batch_name mapping
        
        # Health monitoring
        self.symbol_last_update = defaultdict(lambda: time.time())
        self.batch_health = defaultdict(lambda: {
            'connected': False,
            'last_message': time.time(),
            'last_ping': time.time(),
            'reconnect_attempts': 0,
            'symbols': set()
        })
        
        # Recovery configuration
        self.batch_size = 200  # Smaller batches for better recovery
        self.max_reconnect_attempts = 5
        self.stale_threshold = 30  # seconds
        self.health_check_interval = 20  # seconds
        self.reconnect_cooldown = 5  # seconds
        
        # Spot connection state
        self.spot_ws = None
        self.spot_last_update = time.time()
        self.spot_reconnect_attempts = 0
        
        # Thread safety
        self._reconnect_lock = threading.Lock()
        self._timer_refs = {}  # timer_id -> timer object
        self._timer_lock = threading.Lock()
        
    def fetch_symbols(self):
        """Fetch all tradable futures symbols from Binance."""
        try:
            response = self.session.get('https://fapi.binance.com/fapi/v1/exchangeInfo')
            if response.status_code == 200:
                data = response.json()
                with WriteLock(data_store.exchange_rw_locks['binance']):
                    data_store.symbols['binance'].clear()
                    for symbol_info in data['symbols']:
                        if symbol_info['status'] == 'TRADING':
                            symbol = symbol_info['symbol']
                            data_store.symbols['binance'].add(symbol)
                            
                            # Extract tick size
                            for filter_item in symbol_info['filters']:
                                if filter_item['filterType'] == 'PRICE_FILTER':
                                    if symbol not in data_store.tick_sizes['binance']:
                                        data_store.tick_sizes['binance'][symbol] = {}
                                    data_store.tick_sizes['binance'][symbol]['future_tick_size'] = float(filter_item['tickSize'])
                                    
                logger.info(f"Fetched {len(data_store.symbols['binance'])} Binance futures symbols")
                data_store.update_symbol_maps()
        except Exception as e:
            logger.error(f"Error fetching Binance symbols: {e}")

    def fetch_spot_symbols(self):
        """Fetch spot market information for corresponding futures."""
        try:
            response = self.session.get('https://api.binance.com/api/v3/exchangeInfo')
            if response.status_code == 200:
                data = response.json()
                futures_symbols = data_store.get_symbols('binance')
                
                with WriteLock(data_store.exchange_rw_locks['binance']):
                    for symbol_info in data['symbols']:
                        spot_symbol = symbol_info['symbol']
                        
                        # Find matching futures symbols
                        if spot_symbol in futures_symbols:
                            for filter_item in symbol_info['filters']:
                                if filter_item['filterType'] == 'PRICE_FILTER':
                                    if spot_symbol not in data_store.tick_sizes['binance']:
                                        data_store.tick_sizes['binance'][spot_symbol] = {}
                                    data_store.tick_sizes['binance'][spot_symbol]['spot_tick_size'] = float(filter_item['tickSize'])
        except Exception as e:
            logger.error(f"Error fetching Binance spot symbols: {e}")

    def connect_futures_websocket(self):
        """Connect to Binance Futures WebSocket with enhanced recovery."""
        symbols = list(data_store.get_symbols('binance'))
        if not symbols:
            logger.warning("No Binance futures symbols to connect")
            return
            
        # Split symbols into batches
        symbol_batches = [symbols[i:i + self.batch_size] for i in range(0, len(symbols), self.batch_size)]
        
        for i, symbol_batch in enumerate(symbol_batches):
            batch_name = f"binance_futures_batch_{i}"
            self._connect_futures_batch(batch_name, symbol_batch, i, len(symbol_batches))
            
            # Small delay between batch connections
            if i < len(symbol_batches) - 1:
                time.sleep(0.5)
                
        # Start health monitoring
        self._schedule_timer("futures_health_monitor", self.health_check_interval, self._monitor_futures_health)

    def _connect_futures_batch(self, batch_name, symbols, batch_idx, total_batches):
        """Connect a single batch of futures symbols."""
        try:
            # Create WebSocket URL
            streams = [f"{s.lower()}@bookTicker" for s in symbols]
            ws_url = f"wss://fstream.binance.com/stream?streams={'/'.join(streams)}"
            
            # Update symbol mapping
            for symbol in symbols:
                self.symbol_to_batch[symbol] = batch_name
                
            # Initialize batch health
            self.batch_health[batch_name].update({
                'symbols': set(symbols),
                'batch_idx': batch_idx,
                'total_batches': total_batches
            })
            
            def on_message(ws, message):
                try:
                    data = json.loads(message)
                    
                    # Handle stream data
                    if 'data' in data:
                        data = data['data']
                        
                    if 's' in data:
                        symbol = data['s']
                        
                        # Update price data
                        data_store.update_price_direct(
                            'binance', symbol,
                            float(data['b']), float(data['a']),
                            bid_qty=float(data['B']), ask_qty=float(data['A'])
                        )
                        
                        # Update health tracking
                        self.symbol_last_update[symbol] = time.time()
                        self.batch_health[batch_name]['last_message'] = time.time()
                        
                except Exception as e:
                    logger.error(f"Error processing Binance futures message: {e}")
                    
            def on_open(ws):
                logger.info(f"Binance futures WebSocket connected ({batch_name}: batch {batch_idx+1}/{total_batches})")
                self.batch_health[batch_name]['connected'] = True
                self.batch_health[batch_name]['reconnect_attempts'] = 0
                
            def on_error(ws, error):
                logger.error(f"Binance futures WebSocket error ({batch_name}): {error}")
                
            def on_close(ws, close_status_code, close_msg):
                logger.warning(f"Binance futures WebSocket closed ({batch_name}): {close_status_code} - {close_msg}")
                self.batch_health[batch_name]['connected'] = False
                
                # Schedule reconnection with backoff
                if not stop_event.is_set():
                    self._schedule_reconnection(batch_name, symbols, batch_idx, total_batches)
                    
            def on_ping(ws, message):
                try:
                    ws.sock.pong(message)
                    self.batch_health[batch_name]['last_ping'] = time.time()
                except Exception as e:
                    logger.error(f"Error handling ping for {batch_name}: {e}")
                    
            # Create WebSocket connection
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_open=on_open,
                on_error=on_error,
                on_close=on_close,
                on_ping=on_ping
            )
            
            # Start WebSocket thread
            thread = threading.Thread(
                target=ws.run_forever,
                kwargs={
                    'ping_interval': 20,
                    'ping_timeout': 10,
                    'sslopt': {"cert_reqs": 0}
                },
                daemon=True,
                name=batch_name
            )
            thread.start()
            
            # Store batch info
            self.ws_batches[batch_name] = {
                'ws': ws,
                'thread': thread,
                'symbols': symbols,
                'state': 'running'
            }
            
            # Store in websocket_managers for compatibility
            self.websocket_managers[batch_name] = {
                'ws': ws,
                'thread': thread,
                'is_running': True,
                'last_activity': time.time()
            }
            
        except Exception as e:
            logger.error(f"Error connecting futures batch {batch_name}: {e}")

    def _schedule_reconnection(self, batch_name, symbols, batch_idx, total_batches):
        """Schedule reconnection with exponential backoff."""
        health = self.batch_health[batch_name]
        attempts = health['reconnect_attempts']
        
        if attempts >= self.max_reconnect_attempts:
            logger.error(f"Max reconnection attempts reached for {batch_name}")
            # Schedule full reinitialization
            self._schedule_timer("reinit_futures", 60, self._reinitialize_futures)
            return
            
        # Calculate backoff delay
        delay = min(60, self.reconnect_cooldown * (2 ** attempts))
        health['reconnect_attempts'] += 1
        
        logger.info(f"Scheduling reconnection for {batch_name} in {delay}s (attempt {attempts + 1}/{self.max_reconnect_attempts})")
        
        self._schedule_timer(
            f"reconnect_{batch_name}",
            delay,
            lambda: self._reconnect_futures_batch(batch_name, symbols, batch_idx, total_batches)
        )

    def _reconnect_futures_batch(self, batch_name, symbols, batch_idx, total_batches):
        """Reconnect a specific futures batch."""
        with self._reconnect_lock:
            try:
                # Close existing connection
                if batch_name in self.ws_batches:
                    old_batch = self.ws_batches[batch_name]
                    if old_batch['ws']:
                        try:
                            old_batch['ws'].close()
                        except:
                            pass
                            
                # Remove from tracking
                self.ws_batches.pop(batch_name, None)
                self.websocket_managers.pop(batch_name, None)
                
                # Wait briefly
                time.sleep(1)
                
                # Reconnect
                self._connect_futures_batch(batch_name, symbols, batch_idx, total_batches)
                
            except Exception as e:
                logger.error(f"Error reconnecting futures batch {batch_name}: {e}")

    def connect_spot_websocket(self):
        """Connect to Binance spot WebSocket with enhanced reliability."""
        try:
            ws_url = "wss://stream.binance.com:9443/ws"
            
            def on_message(ws, message):
                try:
                    data = json.loads(message)
                    
                    # Handle ping/pong
                    if isinstance(data, dict) and 'ping' in data:
                        ws.send(json.dumps({"pong": data['ping']}))
                        return
                        
                    # Process ticker array
                    if isinstance(data, list):
                        for ticker in data:
                            if 's' in ticker:
                                symbol = ticker['s']
                                spot_key = f"{symbol}_SPOT"
                                
                                last_price = float(ticker['c'])
                                bid_price = float(ticker.get('b', last_price))
                                ask_price = float(ticker.get('a', last_price))
                                
                                # Update with atomic operation
                                future_data = data_store.get_price_data('binance', symbol)
                                if future_data and 'bid' in future_data and 'ask' in future_data:
                                    spot_data = {
                                        'bid': bid_price,
                                        'ask': ask_price,
                                        'last': last_price
                                    }
                                    data_store.update_related_prices('binance', symbol, future_data, spot_key, spot_data)
                                else:
                                    data_store.update_price_direct('binance', spot_key, bid_price, ask_price, last=last_price)
                                    
                        self.spot_last_update = time.time()
                        
                except Exception as e:
                    logger.error(f"Error processing Binance spot message: {e}")
                    
            def on_open(ws):
                logger.info("Binance spot WebSocket connected")
                self.spot_reconnect_attempts = 0
                
                # Subscribe to all tickers
                subscribe_msg = json.dumps({
                    "method": "SUBSCRIBE",
                    "params": ["!ticker@arr"],
                    "id": 1
                })
                ws.send(subscribe_msg)
                
            def on_error(ws, error):
                logger.error(f"Binance spot WebSocket error: {error}")
                
            def on_close(ws, close_status_code, close_msg):
                logger.warning(f"Binance spot WebSocket closed: {close_status_code} - {close_msg}")
                
                if not stop_event.is_set():
                    self._schedule_spot_reconnection()
                    
            # Create WebSocket
            self.spot_ws = websocket.WebSocketApp(
                f"{ws_url}/!ticker@arr",
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            
            # Start in thread
            thread = threading.Thread(
                target=lambda: self.spot_ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                    sslopt={"cert_reqs": 0}
                ),
                daemon=True,
                name="binance_spot_ws"
            )
            thread.start()
            
            # Store for compatibility
            self.websocket_managers["binance_spot_ws"] = {
                'ws': self.spot_ws,
                'thread': thread,
                'is_running': True,
                'last_activity': time.time()
            }
            
            # Start spot health monitoring
            self._schedule_timer("spot_health_monitor", 30, self._monitor_spot_health)
            
        except Exception as e:
            logger.error(f"Error connecting Binance spot WebSocket: {e}")

    def _schedule_spot_reconnection(self):
        """Schedule spot reconnection with backoff."""
        if self.spot_reconnect_attempts >= self.max_reconnect_attempts:
            logger.error("Max spot reconnection attempts reached")
            self.spot_reconnect_attempts = 0  # ADD THIS LINE
            return
            
        delay = min(60, self.reconnect_cooldown * (2 ** self.spot_reconnect_attempts))
        self.spot_reconnect_attempts += 1
        
        logger.info(f"Scheduling spot reconnection in {delay}s (attempt {self.spot_reconnect_attempts}/{self.max_reconnect_attempts})")
        self._schedule_timer("reconnect_spot", delay, self.connect_spot_websocket)

    def _monitor_futures_health(self):
        """Monitor health of futures connections."""
        if stop_event.is_set():
            return
            
        try:
            current_time = time.time()
            problem_batches = []
            
            # Check each batch
            for batch_name, health in self.batch_health.items():
                if not health['connected']:
                    continue
                    
                # Check for stale data
                time_since_message = current_time - health['last_message']
                if time_since_message > 60:
                    logger.warning(f"{batch_name}: No messages for {time_since_message:.1f}s")
                    problem_batches.append(batch_name)
                    continue
                    
                # Check symbol freshness
                stale_symbols = []
                for symbol in health['symbols']:
                    if current_time - self.symbol_last_update.get(symbol, 0) > self.stale_threshold:
                        stale_symbols.append(symbol)
                        
                if stale_symbols:
                    stale_percent = len(stale_symbols) / len(health['symbols']) * 100
                    logger.warning(f"{batch_name}: {len(stale_symbols)}/{len(health['symbols'])} symbols stale ({stale_percent:.1f}%)")
                    
                    if stale_percent > 50:
                        problem_batches.append(batch_name)
                        
            # Handle problematic batches
            for batch_name in problem_batches:
                if batch_name in self.ws_batches:
                    batch_info = self.ws_batches[batch_name]
                    health = self.batch_health[batch_name]
                    
                    logger.info(f"Triggering reconnection for {batch_name}")
                    self._reconnect_futures_batch(
                        batch_name,
                        batch_info['symbols'],
                        health['batch_idx'],
                        health['total_batches']
                    )
                    
        except Exception as e:
            logger.error(f"Error in futures health monitor: {e}")
            
        # Schedule next check
        self._schedule_timer("futures_health_monitor", self.health_check_interval, self._monitor_futures_health)

    def _monitor_spot_health(self):
        """Monitor health of spot connection."""
        if stop_event.is_set():
            return
            
        try:
            current_time = time.time()
            time_since_update = current_time - self.spot_last_update
            
            if time_since_update > 90:
                logger.warning(f"No spot data for {time_since_update:.1f}s, reconnecting")
                
                # Close existing connection
                if self.spot_ws:
                    try:
                        self.spot_ws.close()
                    except:
                        pass
                        
                # Reconnect
                self.connect_spot_websocket()
                return
                
        except Exception as e:
            logger.error(f"Error in spot health monitor: {e}")
            
        # Schedule next check
        self._schedule_timer("spot_health_monitor", 30, self._monitor_spot_health)

    def _reinitialize_futures(self):
        """Completely reinitialize futures connections."""
        logger.warning("Performing full Binance futures reinitialization")
        
        try:
            # Close all futures connections
            for batch_name in list(self.ws_batches.keys()):
                if batch_name in self.ws_batches:
                    batch = self.ws_batches[batch_name]
                    if batch['ws']:
                        try:
                            batch['ws'].close()
                        except:
                            pass
                            
            # Clear tracking
            self.ws_batches.clear()
            self.symbol_to_batch.clear()
            self.batch_health.clear()
            
            # Remove from websocket_managers
            for key in list(self.websocket_managers.keys()):
                if 'futures' in key:
                    self.websocket_managers.pop(key, None)
                    
            # Wait for cleanup
            time.sleep(2)
            
            # Reconnect
            self.connect_futures_websocket()
            
        except Exception as e:
            logger.error(f"Error reinitializing futures: {e}")

    def _schedule_timer(self, timer_id, delay, callback):
        """Schedule a timer with proper cleanup."""
        with self._timer_lock:
            # Cancel existing timer
            if timer_id in self._timer_refs:
                old_timer = self._timer_refs[timer_id]
                if hasattr(old_timer, 'cancel'):
                    old_timer.cancel()
                    
            # Create new timer
            timer = threading.Timer(delay, callback)
            timer.daemon = True
            timer.start()
            self._timer_refs[timer_id] = timer

    def update_funding_rates(self):
        """Fetch funding rates from Binance API using thread pool."""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('binance'))
                
                # Process in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    batch_size = 20
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    futures = [executor.submit(self._fetch_funding_batch, batch) for batch in batches]
                    concurrent.futures.wait(futures, timeout=30)
                    
                logger.info(f"Updated Binance funding rates for {len(symbols_list)} symbols")
            except Exception as e:
                logger.error(f"Error updating Binance funding rates: {e}")
                
            # Sleep with periodic checks
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def _fetch_funding_batch(self, symbols_batch):
        """Fetch funding rates for a batch of symbols."""
        try:
            # Try batch request first
            symbols_param = ",".join(symbols_batch)
            url = f'https://fapi.binance.com/fapi/v1/premiumIndex?symbols={symbols_param}'
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                for item in data:
                    symbol = item['symbol']
                    rate = float(item['lastFundingRate']) * 100
                    formatted_rate = f"{rate:.4f}%"
                    data_store.update_funding_rate('binance', symbol, formatted_rate)
            else:
                # Fallback to individual requests
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

    def update_24h_changes(self):
        """Fetch 24-hour price changes for symbols using thread pool."""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('binance'))
                
                # Process in parallel
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    batch_size = 20
                    batches = [symbols_list[i:i+batch_size] for i in range(0, len(symbols_list), batch_size)]
                    
                    futures = [executor.submit(self._fetch_changes_batch, batch) for batch in batches]
                    concurrent.futures.wait(futures, timeout=30)
                    
                logger.info(f"Updated Binance 24h changes for {len(symbols_list)} symbols")
            except Exception as e:
                logger.error(f"Error updating Binance 24h changes: {e}")
                
            # Sleep with periodic checks
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def _fetch_changes_batch(self, symbols_batch):
        """Fetch 24h changes for a batch of symbols."""
        try:
            # Try batch request
            symbols_param = ",".join(symbols_batch)
            url = f'https://fapi.binance.com/fapi/v1/ticker/24hr?symbols={symbols_param}'
            response = self.session.get(url)
            
            if response.status_code == 200:
                data = response.json()
                with WriteLock(data_store.exchange_rw_locks['binance']):
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
                        with WriteLock(data_store.exchange_rw_locks['binance']):
                            data_store.daily_changes['binance'][symbol] = change_percent
                            
        except Exception as e:
            logger.error(f"Error fetching 24h changes batch: {e}")