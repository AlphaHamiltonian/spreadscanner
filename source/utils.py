import time
import logging
import orjson as json
import websocket
import threading
import requests
from collections import deque
import random
from source.config import Config
from threading import RLock
from source.alerts import alert_manager
from source.symbol_matcher import symbol_matcher

class ReaderWriterLock:
    """Allows multiple concurrent readers or one exclusive writer"""
    def __init__(self):
        self._read_ready = threading.Condition(RLock())
        self._readers = 0

    def acquire_read(self):
        self._read_ready.acquire()
        try:
            self._readers += 1
        finally:
            self._read_ready.release()

    def release_read(self):
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self):
        self._read_ready.acquire()
        while self._readers > 0:
            self._read_ready.wait()

    def release_write(self):
        self._read_ready.release()

    def __enter__(self):
        self.acquire_read()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release_read()

class WriteLock:
    """Context manager for write access"""
    def __init__(self, rw_lock):
        self.rw_lock = rw_lock
    
    def __enter__(self):
        self.rw_lock.acquire_write()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rw_lock.release_write()

class SymbolLockManager:
    """Thread-safe manager for per-symbol locks with automatic cleanup"""
    
    def __init__(self, cleanup_interval=300):  # 5 minutes
        self._locks = {}  # symbol -> ReaderWriterLock
        self._lock_access_times = {}  # symbol -> last_access_time
        self._manager_lock = threading.RLock()
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = time.time()
        
    def get_lock(self, exchange, symbol):
        """Get or create a lock for the given exchange:symbol"""
        lock_key = f"{exchange}:{symbol}"
        current_time = time.time()
        
        with self._manager_lock:
            # Periodic cleanup of unused locks
            if current_time - self._last_cleanup > self._cleanup_interval:
                self._cleanup_unused_locks(current_time)
                self._last_cleanup = current_time
            
            # Update access time
            self._lock_access_times[lock_key] = current_time
            
            # Create lock if it doesn't exist
            if lock_key not in self._locks:
                self._locks[lock_key] = ReaderWriterLock()
                
            return self._locks[lock_key]
    
    def _cleanup_unused_locks(self, current_time, max_idle_time=600):  # 10 minutes
        """Remove locks that haven't been accessed recently"""
        keys_to_remove = []
        
        for lock_key, last_access in self._lock_access_times.items():
            if current_time - last_access > max_idle_time:
                keys_to_remove.append(lock_key)
        
        for key in keys_to_remove:
            self._locks.pop(key, None)
            self._lock_access_times.pop(key, None)
            
        if keys_to_remove:
            logging.getLogger(__name__).debug(f"Cleaned up {len(keys_to_remove)} unused symbol locks")


logger = logging.getLogger(__name__) # module-specific logger

class WebSocketManager:
    """Manages WebSocket connections with automatic reconnection and message processing."""
    def __init__(self, url, name, on_message, on_open=None, on_error=None, on_close=None,
                ping_interval=30, ping_timeout=10, retry_count=100):
        self.url = url
        self.name = name
        self.on_message = on_message
        self.on_open = on_open
        self.on_error = on_error or self._default_on_error
        self.on_close = on_close or self._default_on_close
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.retry_count = retry_count
        self.retry_delay = 5
        self.ws = None
        self.thread = None
        self.is_running = False
        self.reconnect_count = 0
        self.last_activity = time.time()
        self.pong_received = False
        self.last_pong_time = time.time()
        self.processor_threads = []
        

    def _start_heartbeat(self):
        if not self.is_running:
            return
        self.maintain_connection()      # send ping / skip for Binance
        threading.Timer(20, self._start_heartbeat).start()

    def disconnect(self):
        """Properly disconnect WebSocket and clean up resources"""
        if not self.is_running:
            return
            
        self.is_running = False
        logger.info(f"Disconnecting {self.name} WebSocket")
        
        if self.ws:
            try:
                self.ws.close()
                logger.debug(f"Closed {self.name} WebSocket")
            except Exception as e:
                logger.error(f"Error closing {self.name} WebSocket: {e}")
            
        self.processor_threads = []
        logger.info(f"Disconnected {self.name} WebSocket and cleaned up resources")

        
    def _on_message_wrapper(self, ws, message):
        self.last_activity = time.time()
        try:
            # For Bybit, check if this is a pong response
            try:
                data = json.loads(message)
                if data.get('op') == 'pong':
                    self.last_pong_time = time.time()
                    self.pong_received = True
                    logger.debug(f"{self.name} received pong response")
                    return
            except:
                pass
            # DIRECT PROCESSING - no queue
            self.on_message(ws, message)
        except Exception as e:
            logger.error(f"Error in {self.name} message handler: {e}")

    def _on_ping_handler(self, ws, message):
        """Handle protocol-level ping frames"""
        try:
            if hasattr(ws, 'sock') and ws.sock and hasattr(ws.sock, 'pong'):
                ws.sock.pong(message)
            else:
                import websocket
                ws.send(message, websocket.ABNF.OPCODE_PONG)
            self.last_activity = time.time()
            logger.debug(f"{self.name} received ping, sent pong response")
        except Exception as e:
            logger.error(f"Error sending pong response: {e}")

    def _on_pong_handler(self, ws, message):
        """Handle protocol-level pong frames"""
        self.pong_received = True
        self.last_pong_time = time.time()
        self.last_activity = time.time()
        logger.debug(f"{self.name} received pong response")

    def _default_on_error(self, ws, error):
        logger.error(f"{self.name} WebSocket error: {error}")

    def _default_on_close(self, ws, close_status_code, close_msg):
        logger.info(f"{self.name} WebSocket closed: {close_status_code} - {close_msg}")
        if self.is_running and self.reconnect_count < self.retry_count:
            self.reconnect_count += 1
            delay = min(30, self.reconnect_count * self.retry_delay)
            logger.info(f"Will reconnect {self.name} in {delay} seconds (attempt {self.reconnect_count}/{self.retry_count})")
            time.sleep(delay)
            self.connect()
        elif self.reconnect_count >= self.retry_count:
            logger.error(f"{self.name} WebSocket failed after {self.retry_count} attempts")


    def connect(self):
        """Connect to WebSocket with improved error handling and backoff strategy"""
        # Don't reconnect if already connected
        if self.is_running and self.ws and hasattr(self.ws, 'sock') and self.ws.sock and self.ws.sock.connected:
            logger.debug(f"{self.name} already connected, skipping reconnect")
            return
            
        # Reset state for a clean connection
        self.is_running = True
        
        # Implement exponential backoff for reconnection attempts
        if not hasattr(self, 'backoff_time'):
            self.backoff_time = 1.0  # Start with 1 second
        else:
            # Increase backoff time with each reconnect, up to 30 seconds
            self.backoff_time = min(30, self.backoff_time * 1.5)
        
        # If we've seen multiple failures, add a random jitter to prevent connection storms
        if self.reconnect_count > 2:
            jitter = random.uniform(0, 2)
            time.sleep(jitter)
                
        try:
            # Close any existing connection first
            if self.ws:
                try:
                    self.ws.close()
                    logger.debug(f"Closed existing {self.name} connection")
                except Exception as e:
                    logger.debug(f"Error closing existing {self.name} connection: {e}")
                    
            # Create new connection
            self.ws = websocket.WebSocketApp(
                self.url,
                on_message=self._on_message_wrapper,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
                on_ping=self._on_ping_handler,
                on_pong=self._on_pong_handler
            )
            
            # Start WebSocket thread only if not already running
            if not self.thread or not self.thread.is_alive():
                # Use different ping/pong settings for different exchanges
                ping_interval = 20
                ping_timeout = 10
                
                # Bybit needs more aggressive ping/pong
                if "bybit" in self.name.lower():
                    ping_interval = 20
                    ping_timeout = 15
                elif "okx" in self.name.lower():
                    ping_interval = 30
                    ping_timeout = 20                    
                self.thread = threading.Thread(
                    target=self.ws.run_forever,
                    kwargs={
                        'ping_interval': ping_interval,
                        'ping_timeout': ping_timeout,
                        'sslopt': {"cert_reqs": 0},
                        'skip_utf8_validation': True  # Add this
                    },
                    daemon=True,
                    name=self.name
                )
                self.thread.start()
                logger.info(f"Started {self.name} WebSocket thread")
                
            # Reset backoff on successful connection
            self.backoff_time = 1.0
            
            # Set last activity time for health monitoring
            self.last_activity = time.time()
            self._start_heartbeat()
            logger.info(f"Connected {self.name} WebSocket")
            
        except Exception as e:
            self.is_running = False
            logger.error(f"Failed to connect {self.name}: {e}")
            
            # Schedule reconnection with backoff
            threading.Timer(self.backoff_time, self.connect).start()

    def check_health(self):
        """Comprehensive health check with intelligent recovery"""
        if not self.is_running:
            return False
            
        current_time = time.time()
        health_issues = []
        
        # Check if thread is alive
        if not self.thread or not self.thread.is_alive():
            health_issues.append("WebSocket thread died")
            
        # Check for message activity timeout
        activity_timeout = 60  # 1 minute
        if hasattr(self, 'last_activity') and current_time - self.last_activity > activity_timeout:
            health_issues.append(f"No activity for {current_time - self.last_activity:.1f}s")
            
        # Check for pong response timeout
        pong_timeout = 120  # 2 minutes
        if hasattr(self, 'last_pong_time') and current_time - self.last_pong_time > pong_timeout:
            health_issues.append(f"No pong response for {current_time - self.last_pong_time:.1f}s")

        # Take action if there are health issues, but limit reconnection frequency
        if health_issues and (not hasattr(self, 'last_reconnect_time') or 
                                current_time - getattr(self, 'last_reconnect_time', 0) > 15):  # 15 sec minimum between reconnects
            logger.warning(f"{self.name} health check failed: {', '.join(health_issues)}")
            # Force reconnection for serious issues
            if "WebSocket thread died" in health_issues or "No activity" in health_issues or "No pong response" in health_issues:
                logger.info(f"Reconnecting {self.name} due to health issues")
                self.disconnect()
                time.sleep(1)
                self.connect()
                self.last_reconnect_time = current_time
                return False
                
        return len(health_issues) == 0

    def maintain_connection(self):
        """Send periodic heartbeats to keep connection alive with better error checking"""
        if not self.is_running:
            return
        try:
            # Only send ping if socket is valid and connected
            if (not self.ws or not hasattr(self.ws, 'sock') or
                not self.ws.sock or not hasattr(self.ws.sock, 'connected')):
                return
                
            # Double-check connection state to avoid errors
            if not self.ws.sock.connected:
                logger.debug(f"{self.name} socket disconnected, skipping heartbeat")
                return
                
            # Skip sending application-level pings for Binance
            if "binance" in self.name.lower():
                return
                
            # For other exchanges
            try:
                if "bybit" in self.name.lower():
                    self.ws.send(json.dumps({"op": "ping"}))
                elif "okx" in self.name.lower():
                    self.ws.send(json.dumps({"event": "ping"}))
                logger.debug(f"Sent heartbeat to {self.name}")
            except websocket._exceptions.WebSocketConnectionClosedException:
                logger.warning(f"{self.name} connection closed, cannot send heartbeat")
            except Exception as e:
                logger.error(f"Failed to send heartbeat to {self.name}: {e}")
        except Exception as e:
            logger.error(f"Error in maintain_connection for {self.name}: {e}")


class HttpSessionManager:
    """Manages HTTP sessions with rate limiting and retry handling."""
    def __init__(self, name, base_headers=None, max_retries=3, rate_limit=10):
        self.name = name
        self.session = requests.Session()
        if base_headers:
            self.session.headers.update(base_headers)
            
        # Configure retry strategy
        retry_strategy = requests.packages.urllib3.util.retry.Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Rate limiting
        self.rate_limit = rate_limit  # Requests per second
        self.request_times = deque(maxlen=rate_limit)
        self.rate_limit_lock = threading.Lock()

    def _wait_for_rate_limit(self):
        """Wait if necessary to comply with rate limits"""
        with self.rate_limit_lock:
            now = time.time()
            
            # If we haven't made enough requests yet, no need to wait
            if len(self.request_times) < self.rate_limit:
                self.request_times.append(now)
                return
                
            # Check if oldest request is older than 1 second
            oldest = self.request_times[0]
            if now - oldest < 1.0:
                # Need to wait to stay under rate limit
                sleep_time = 1.0 - (now - oldest)
                time.sleep(sleep_time)
                
            # Record this request time
            self.request_times.append(time.time())

    def get(self, url, **kwargs):
        """Make GET request with rate limiting"""
        try:
            # Apply rate limiting
            if self.name.lower() in ["okx", "bybit"]:
                self._wait_for_rate_limit()
                
            response = self.session.get(url, **kwargs)
            
            if response.status_code >= 400:
                logger.warning(f"{self.name} HTTP GET error {response.status_code}: {url}")
                
            return response
        except requests.RequestException as e:
            logger.error(f"{self.name} HTTP GET exception: {url} - {e}")
            return None


class DataStore:
    """Centralized data store with per-symbol thread-safe operations."""
    def __init__(self):
        # Keep original global lock for backward compatibility and operations affecting multiple symbols
        self.lock = threading.RLock()
        # Per-symbol lock manager for high-frequency price updates
        self.symbol_locks = SymbolLockManager()
        self.symbol_equivalence_map = {}  # Cache equivalent symbols
        # Exchange-specific reader-writer locks for better concurrency (keep for legacy compatibility)
        self.dirty_symbols = set()  # (exchange, symbol) tuples
        self.dirty_symbols_lock = threading.Lock()

        self.exchange_rw_locks = {
            'binance': ReaderWriterLock(),
            'bybit': ReaderWriterLock(),
            'okx': ReaderWriterLock()
        }
        
        # Keep old exchange_locks for any legacy code that might still use them
        self.exchange_locks = {
            'binance': threading.RLock(),
            'bybit': threading.RLock(),
            'okx': threading.RLock()
        }
        
        self.symbols = {'binance': set(), 'bybit': set(), 'okx': set()}
        self.price_data = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.funding_rates = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.tick_sizes = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.daily_changes = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.update_counters = {'binance': 0, 'bybit': 0, 'okx': 0}
        
        # Symbol mapping and caching
        self.symbol_maps = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.normalized_cache = {}
        self.equivalent_symbols = {}
        # Add spreads data structure
        self.spreads = {'binance': {}, 'bybit': {}, 'okx': {}}
        self.spread_timestamp = 0
    def update_symbol_maps(self):
        """Update normalized symbol maps for all exchanges"""
        with self.lock:
            # Clear existing maps
            for exchange in self.symbol_maps:
                self.symbol_maps[exchange] = {}
                
            # Create normalized to original symbol mappings
            for exchange in self.symbols:
                for symbol in self.symbols[exchange]:
                    normalized = symbol_matcher.normalize_symbol(exchange, symbol)
                    self.symbol_maps[exchange][normalized] = symbol
            
            # CHANGE: Clear the new cache when symbols change
            self.symbol_equivalence_map.clear()
            self.equivalent_symbols = {}  # Keep clearing old cache for compatibility
    def mark_symbol_dirty(self, exchange, symbol):
        """Mark a symbol as needing spread recalculation"""
        with self.dirty_symbols_lock:
            self.dirty_symbols.add((exchange, symbol))

    def update_related_prices(self, exchange, future_symbol, future_data, spot_symbol=None, spot_data=None):
        """Update futures and spot data atomically using symbol-level locks"""
        timestamp = time.time()
        
        # Get locks for the symbols we need to update
        future_lock = self.symbol_locks.get_lock(exchange, future_symbol)
        spot_lock = self.symbol_locks.get_lock(exchange, spot_symbol) if spot_symbol else None
        
        # Always acquire locks in consistent order to prevent deadlock
        locks_to_acquire = [(future_symbol, future_lock)]
        if spot_lock and spot_symbol:
            locks_to_acquire.append((spot_symbol, spot_lock))
        
        # Sort by symbol name to ensure consistent lock ordering
        locks_to_acquire.sort(key=lambda x: x[0])
        
        # Acquire all locks
        acquired_locks = []
        try:
            for symbol_name, lock in locks_to_acquire:
                write_lock = WriteLock(lock)
                write_lock.__enter__()
                acquired_locks.append(write_lock)
            
            # Update future price
            if future_symbol not in self.price_data[exchange]:
                self.price_data[exchange][future_symbol] = {}
            self.price_data[exchange][future_symbol].update(future_data)
            self.price_data[exchange][future_symbol]['timestamp'] = timestamp
            
            # Update spot price if provided
            if spot_symbol and spot_data:
                if spot_symbol not in self.price_data[exchange]:
                    self.price_data[exchange][spot_symbol] = {}
                self.price_data[exchange][spot_symbol].update(spot_data)
                self.price_data[exchange][spot_symbol]['timestamp'] = timestamp
                
            # Only update counter once after all updates
            with self.exchange_locks[exchange]:
                self.update_counters[exchange] += 1
                
        finally:
            # Release all locks in reverse order
            for write_lock in reversed(acquired_locks):
                write_lock.__exit__(None, None, None)

    def calculate_all_spreads(self):
        """Calculate spreads only for symbols that have been updated"""
        current_time = time.time()

        # Get dirty symbols and clear the set
        with self.dirty_symbols_lock:
            if not self.dirty_symbols:
                return  # Nothing to update
            symbols_to_process = self.dirty_symbols.copy()
            self.dirty_symbols.clear()

        # CHANGE 1: Pre-compute what symbols we actually need
        required_symbols = {}
        for exchange, symbol in symbols_to_process:
            if symbol.endswith('_SPOT'):
                continue  # Skip spot symbols early
                
            if exchange not in required_symbols:
                required_symbols[exchange] = set()
            
            # Add the symbol itself
            required_symbols[exchange].add(symbol)
            
            # Add its spot pair
            required_symbols[exchange].add(f"{symbol}_SPOT")
            
            # Add equivalent symbols from other exchanges
            for other in ['binance', 'bybit', 'okx']:
                if other != exchange:
                    #equiv = symbol_matcher.find_equivalent_symbol(exchange, symbol, other)
                    cache_key = (exchange, symbol, other)
                    if cache_key in self.symbol_equivalence_map:
                        equiv = self.symbol_equivalence_map[cache_key]
                    else:
                        equiv = symbol_matcher.find_equivalent_symbol(exchange, symbol, other)
                        self.symbol_equivalence_map[cache_key] = equiv

                    if equiv:
                        if other not in required_symbols:
                            required_symbols[other] = set()
                        required_symbols[other].add(equiv)

        # CHANGE 2: Only snapshot the symbols we actually need
        exchange_snapshots = {}
        for exchange, needed_symbols in required_symbols.items():
            exchange_snapshots[exchange] = {}
            
            for symbol in needed_symbols:
                symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
                with symbol_lock:  # Read lock
                    symbol_data = self.price_data[exchange].get(symbol, {})
                    if symbol_data and 'bid' in symbol_data and 'ask' in symbol_data:
                        # CHANGE 3: Only copy essential fields
                        exchange_snapshots[exchange][symbol] = {
                            'bid': symbol_data['bid'],
                            'ask': symbol_data['ask'],
                            'timestamp': symbol_data.get('timestamp', 0)
                        }

        # Rest of your code remains exactly the same...
        # Calculate spreads only for dirty symbols
        for exchange, dirty_symbol in symbols_to_process:
            if exchange not in exchange_snapshots:
                continue
                
            local_book = exchange_snapshots[exchange]
            if dirty_symbol not in local_book:
                continue
                
            # Skip spot symbols - they don't have their own spreads
            if dirty_symbol.endswith('_SPOT'):
                continue
                
            fut = local_book[dirty_symbol]
            bid, ask = fut.get("bid"), fut.get("ask")
            if bid is None or ask is None:
                continue

            # Calculate spreads for this specific symbol
            spreads_for_exch = self.spreads.setdefault(exchange, {})

            # vs-spot calculation
            spot_key = f"{dirty_symbol}_SPOT"
            spot_data = local_book.get(spot_key, {})
            vs_spot = self._calculate_spread(
                fut, spot_data,
                exchange, dirty_symbol,
                exchange, spot_key
            )

            # vs other exchanges
            vs_binance = vs_bybit = vs_okx = 'N/A'
            for other in ("binance", "bybit", "okx"):
                if other == exchange:
                    continue
                
                #equiv = symbol_matcher.find_equivalent_symbol(exchange, dirty_symbol, other)
                cache_key = (exchange, dirty_symbol, other)
                if cache_key in self.symbol_equivalence_map:
                    equiv = self.symbol_equivalence_map[cache_key]
                else:
                    equiv = symbol_matcher.find_equivalent_symbol(exchange, dirty_symbol, other)
                    self.symbol_equivalence_map[cache_key] = equiv

                if not equiv:
                    continue
                
                other_data = exchange_snapshots.get(other, {}).get(equiv, {})
                if "bid" in other_data and "ask" in other_data:
                    spread = self._calculate_spread(
                        fut, other_data,
                        exchange, dirty_symbol,
                        other, equiv
                    )
                    if   other == "binance": vs_binance = spread
                    elif other == "bybit":   vs_bybit   = spread
                    elif other == "okx":     vs_okx     = spread

            # Store results for this symbol
            spreads_for_exch[dirty_symbol] = {
                "vs_spot":    vs_spot,
                "vs_binance": vs_binance,
                "vs_bybit":   vs_bybit,
                "vs_okx":     vs_okx,
                "timestamp":  current_time,
            }

        self.spread_timestamp = current_time



    def _calculate_spread(self, price1, price2, exchange1=None, symbol1=None, exchange2=None, symbol2=None):
        """Calculate spread with different staleness thresholds for futures vs spot"""
        # Basic validation
        if (not price1 or not price2 or
                'bid' not in price1 or 'ask' not in price1 or
                'bid' not in price2 or 'ask' not in price2):
            return 'N/A'
        
        # Format source identifiers
        source1 = f"{exchange1}:{symbol1}" if exchange1 and symbol1 else "unknown"
        source2 = f"{exchange2}:{symbol2}" if exchange2 and symbol2 else "unknown"
        
        # Check for timestamps
        current_time = time.time()
        if 'timestamp' not in price1 or 'timestamp' not in price2:
            if random.random() < 0.05:  # Log only 5% of occurrences
                logger.warning(f"Missing timestamp: {source1} vs {source2}")
            return 'N/A'
        
        # Calculate data age
        price1_age = current_time - price1['timestamp']
        price2_age = current_time - price2['timestamp']
        
        # Different thresholds based on data type
        is_spot1 = symbol1 and "_SPOT" in symbol1
        is_spot2 = symbol2 and "_SPOT" in symbol2
        
        
        # Determine maximum allowed age for each source
        max_age1 = Config.SPOT_THRESHOLD if is_spot1 else Config.FUTURES_THRESHOLD
        max_age2 = Config.SPOT_THRESHOLD if is_spot2 else Config.FUTURES_THRESHOLD
        
        # Check if data is stale (using appropriate thresholds)
        if price1_age > max_age1 or price2_age > max_age2 or abs(price1_age-price2_age)> Config.DIFFERENCE_THRESHOLD:
            # Log with threshold info (reduce volume with sampling)
            if random.random() < 0.0001:  # Log only 5% of occurrences
                logger.warning(f"Stale data: {source1}({price1_age:.2f}s/{max_age1}s) vs "
                            f"{source2}({price2_age:.2f}s/{max_age2}s)")
            return 'N/A'
        
        # Rest of your spread calculation remains the same
        bid1 = price1['bid']
        ask1 = price1['ask']
        bid2 = price2['bid']
        ask2 = price2['ask']
        
        # Calculate spread
        if ask1 <= bid2:
            avg_ratio = (ask1/bid2-1)
        elif bid1 >= ask2:
            avg_ratio = (bid1/ask2-1)
        else:
            avg_ratio = 0
        
        # Express as percentage
        spread_pct = avg_ratio * 100
        alert_manager.check_spread_alert(spread_pct, source1, source2, exchange1, exchange2)
        return spread_pct

    def get_spread(self, exchange, symbol, spread_type='vs_spot'):
        """Get pre-calculated spread value"""
        try:
            # Quick read without lock first (optimistic)
            spreads_dict = self.spreads.get(exchange, {})
            symbol_dict = spreads_dict.get(symbol, {})
            value = symbol_dict.get(spread_type, 'N/A')
            
            # Only lock if we need to access more deeply and use the new read lock
            if value == 'N/A' and exchange in self.exchange_locks:
                with self.exchange_locks[exchange]:
                    value = self.spreads.get(exchange, {}).get(symbol, {}).get(spread_type, 'N/A')
            
            return value if isinstance(value, float) else 'N/A'
        except:
            return 'N/A'

    def get_symbols(self, exchange):
        with self.exchange_locks[exchange]:  # Read lock for specific exchange
            return self.symbols[exchange].copy()


    def update_price_direct(self, exchange, symbol, bid, ask, bid_qty=None, ask_qty=None, last=None):
        """Direct price update with per-symbol locking for maximum concurrency"""
        symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
        
        with WriteLock(symbol_lock):
            if symbol not in self.price_data[exchange]:
                self.price_data[exchange][symbol] = {}
            
            data = {
                'bid': bid,
                'ask': ask,
                'timestamp': time.time()
            }
            
            if bid_qty is not None:
                data['bidQty'] = bid_qty
            if ask_qty is not None:
                data['askQty'] = ask_qty
            if last is not None:
                data['last'] = last
                
            self.price_data[exchange][symbol].update(data)
        self.mark_symbol_dirty(exchange, symbol)
        # Update counter separately to minimize lock contention
        with self.exchange_locks[exchange]:
            self.update_counters[exchange] += 1

    def get_price_data(self, exchange, symbol):
        """Get price data for a symbol using per-symbol lock"""
        symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
        with symbol_lock:  # Read lock
            return self.price_data[exchange].get(symbol, {}).copy()

    def update_funding_rate(self, exchange, symbol, rate):

        symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
        with WriteLock(symbol_lock):
            if exchange not in self.funding_rates:
                self.funding_rates[exchange] = {}
            self.funding_rates[exchange][symbol] = rate
            now = time.time()
            key = (exchange, symbol)
            try:
                r = float(str(rate).strip().rstrip("%"))
            except (ValueError, TypeError):
                return                               
            # --- 3. alert logic -----------------------------------------------------
            alert_manager.check_funding_alert(exchange, symbol, rate)

    def get_funding_rate(self, exchange, symbol):
        symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
        with symbol_lock:  # Read lock
            return self.funding_rates[exchange].get(symbol, "N/A")

    def clean_old_data(self, max_age=3600):
        """Clean old data using per-symbol locks"""
        current_time = time.time()
        
        for exchange in list(self.price_data.keys()):
            # Get list of symbols first
            with self.exchange_locks[exchange]:
                symbols_to_check = list(self.price_data[exchange].keys())
            
            # Process each symbol individually
            for symbol in symbols_to_check:
                symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
                with WriteLock(symbol_lock):
                    if symbol in self.price_data[exchange]:
                        data = self.price_data[exchange][symbol]
                        if 'timestamp' in data and current_time - data['timestamp'] > max_age:
                            del self.price_data[exchange][symbol]
        alert_manager.cleanup_old_data()


    def check_data_freshness(self):
        """Check if data is fresh and log warnings if not"""
        current_time = time.time()
        stale_threshold = 60  # 60 seconds
        
        # Check each exchange independently - allows concurrent operations
        for exchange in list(self.price_data.keys()):
            # Take snapshot of symbols to check
            with self.exchange_locks[exchange]:
                symbols_to_check = list(self.price_data[exchange].keys())
            
            stale_count = 0
            total_count = 0
            
            # Check each symbol individually
            for symbol in symbols_to_check:
                symbol_lock = self.symbol_locks.get_lock(exchange, symbol)
                with symbol_lock:  # Read lock
                    if symbol in self.price_data[exchange]:
                        data = self.price_data[exchange][symbol]
                        if 'timestamp' in data:
                            total_count += 1
                            if current_time - data['timestamp'] > stale_threshold:
                                stale_count += 1
                                
            if total_count > 0 and stale_count > 0.1 * total_count:
                logger.warning(f"Data freshness issue: {exchange} has {stale_count}/{total_count} stale symbols")

# Create global data store
data_store = DataStore()