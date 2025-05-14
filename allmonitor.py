import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import concurrent.futures
import time
import logging
import signal
import math
import random
import csv
from collections import deque
import orjson as json
import requests
import websocket

# Disable WebSocket trace for cleaner logs
websocket.enableTrace(False)

# Configure logging
logging.basicConfig(
    level=logging.CRITICAL,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("exchange_monitor.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
websocket_debug_logger = logging.getLogger('websocket')
websocket_debug_logger.setLevel(logging.CRITICAL)
logger.info(f"Using websocket-client version: {websocket.__version__}")

# Global thread control
stop_event = threading.Event()
active_threads = {}

#---------------------------------------------
# Utility Classes
#---------------------------------------------
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
        """Connect to WebSocket with auto-reconnect and dynamic processors"""
        # Don't reconnect if already connected
        if self.is_running and self.ws and hasattr(self.ws, 'sock') and self.ws.sock and self.ws.sock.connected:
            logger.debug(f"{self.name} already connected, skipping reconnect")
            return
            
        # Reset state for a clean connection
        self.is_running = True
                
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
                self.thread = threading.Thread(
                    target=self.ws.run_forever,
                    kwargs={
                        'ping_interval': self.ping_interval,
                        'ping_timeout': self.ping_timeout,
                        'sslopt': {"cert_reqs": 0}
                    },
                    daemon=True,
                    name=self.name
                )
                self.thread.start()
                logger.info(f"Started {self.name} WebSocket thread")
                
            # Set last activity time for health monitoring
            self.last_activity = time.time()
            logger.info(f"Connected {self.name} WebSocket")
            
        except Exception as e:
            self.is_running = False
            logger.error(f"Failed to connect {self.name}: {e}")

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
            
        # Take action if there are health issues
        if health_issues:
            logger.warning(f"{self.name} health check failed: {', '.join(health_issues)}")
            # Force reconnection for serious issues
            if "WebSocket thread died" in health_issues or "No activity" in health_issues or "No pong response" in health_issues:
                logger.info(f"Reconnecting {self.name} due to health issues")
                self.disconnect()
                time.sleep(1)
                self.connect()
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

class SymbolNormalizer:
    """Normalizes symbols across exchanges with caching for performance."""
    _cache = {}  # Class-level cache
    
    @classmethod
    def normalize_symbol(cls, exchange, symbol):
        """Normalize symbols with caching for performance"""
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key in cls._cache:
            return cls._cache[cache_key]
            
        # Perform normalization
        result = None
        if exchange == 'okx' and '-SWAP' in symbol:
            result = symbol.replace('-SWAP', '').replace('-', '')
        elif exchange == 'bybit' and '-' in symbol:
            result = symbol.replace('-', '')
        else:
            result = symbol
            
        # Cache the result
        cls._cache[cache_key] = result
        return result

class DataStore:
    """Centralized data store with thread-safe operations."""
    def __init__(self):
        # Keep original global lock for backward compatibility
        self.lock = threading.RLock()
        
        # Exchange-specific locks for parallel processing
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

        
    def update_related_prices(self, exchange, future_symbol, future_data, spot_symbol=None, spot_data=None):
        """Update futures and spot data atomically to ensure consistent spread calculations"""
        with self.lock:
            timestamp = time.time()
            
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
                
            self.update_counters[exchange] += 1

    def calculate_all_spreads(self):
        """Pre-calculate all spreads to avoid doing it during UI updates"""
        with self.lock:  # Keep original lock for now
            current_time = time.time()
            
            # For each exchange and symbol
            for exchange in self.price_data:
                if exchange not in self.spreads:
                    self.spreads[exchange] = {}
                    
                for symbol in list(self.price_data[exchange].keys()):
                    # Skip spot symbols
                    if symbol.endswith('_SPOT'):
                        continue
                        
                    # Get futures data
                    future_data = self.price_data[exchange].get(symbol, {})
                    if not future_data or 'bid' not in future_data or 'ask' not in future_data:
                        continue
                    
                    # Calculate vs spot spread
                    spot_key = f"{symbol}_SPOT"
                    spot_data = self.price_data[exchange].get(spot_key, {})
                    vs_spot = self._calculate_spread(
                        future_data, spot_data,
                        exchange, symbol,    # Exchange and symbol for futures
                        exchange, spot_key   # Exchange and symbol for spot
                    )
                    
                    # Calculate vs other exchanges spreads
                    vs_binance = 'N/A'
                    vs_okx = 'N/A'
                    vs_bybit = 'N/A'
                    
                    for other_exchange in ['binance', 'bybit', 'okx']:
                        if other_exchange == exchange:
                            continue
                        
                        equiv_symbol = self.find_equivalent_symbol(exchange, symbol, other_exchange)
                        if equiv_symbol:
                            other_data = self.price_data.get(other_exchange, {}).get(equiv_symbol, {})
                            if other_data and 'bid' in other_data and 'ask' in other_data:
                                spread = self._calculate_spread(
                                    future_data, other_data,
                                    exchange, symbol,              # Source exchange/symbol
                                    other_exchange, equiv_symbol   # Target exchange/symbol
                                )
                                
                                # Store in appropriate variable
                                if other_exchange == 'binance':
                                    vs_binance = spread
                                elif other_exchange == 'okx':
                                    vs_okx = spread
                                elif other_exchange == 'bybit':
                                    vs_bybit = spread
                    
                    # Store all calculated spreads
                    self.spreads[exchange][symbol] = {
                        'vs_spot': vs_spot,
                        'vs_binance': vs_binance,
                        'vs_bybit': vs_bybit,
                        'vs_okx': vs_okx,
                        'timestamp': current_time
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
        
        # Set thresholds (configurable)
        FUTURES_THRESHOLD = 5  # Futures: 2 seconds
        SPOT_THRESHOLD = 10    # Spot: 10 seconds
        
        # Determine maximum allowed age for each source
        max_age1 = SPOT_THRESHOLD if is_spot1 else FUTURES_THRESHOLD
        max_age2 = SPOT_THRESHOLD if is_spot2 else FUTURES_THRESHOLD
        
        # Check if data is stale (using appropriate thresholds)
        if price1_age > max_age1 or price2_age > max_age2:
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
        return spread_pct
    def get_spread(self, exchange, symbol, spread_type='vs_spot'):
        """Get pre-calculated spread value"""
        try:
            # Quick read without lock first (optimistic)
            spreads_dict = self.spreads.get(exchange, {})
            symbol_dict = spreads_dict.get(symbol, {})
            value = symbol_dict.get(spread_type, 'N/A')
            
            # Only lock if we need to access more deeply
            if value == 'N/A' and exchange in self.exchange_locks:
                with self.exchange_locks[exchange]:
                    value = self.spreads.get(exchange, {}).get(symbol, {}).get(spread_type, 'N/A')
            
            return value if isinstance(value, float) else 'N/A'
        except:
            return 'N/A'

    def get_symbols(self, exchange):
        with self.lock:
            return self.symbols[exchange].copy()

    def update_symbol_maps(self):
        """Update normalized symbol maps for all exchanges"""
        with self.lock:
            # Clear existing maps
            for exchange in self.symbol_maps:
                self.symbol_maps[exchange] = {}
                
            # Create normalized to original symbol mappings
            for exchange in self.symbols:
                for symbol in self.symbols[exchange]:
                    normalized = self.normalize_symbol(exchange, symbol)
                    self.symbol_maps[exchange][normalized] = symbol
                    
            # Clear and rebuild equivalent symbol cache
            self.equivalent_symbols = {}

    def normalize_symbol(self, exchange, symbol):
        """Get normalized symbol with caching"""
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key not in self.normalized_cache:
            self.normalized_cache[cache_key] = SymbolNormalizer.normalize_symbol(exchange, symbol)
            
        return self.normalized_cache[cache_key]

    def find_equivalent_symbol(self, source_exchange, source_symbol, target_exchange):
        """Find equivalent symbol in target exchange with caching"""
        cache_key = f"{source_exchange}:{source_symbol}:{target_exchange}"
        
        # Return from cache if available
        if cache_key in self.equivalent_symbols:
            return self.equivalent_symbols[cache_key]
            
        # Get normalized version of source symbol
        normalized_source = self.normalize_symbol(source_exchange, source_symbol)
        
        # Look for matching symbol in target exchange
        with self.lock:
            target_symbols = self.symbols[target_exchange]
            for target_symbol in target_symbols:
                normalized_target = self.normalize_symbol(target_exchange, target_symbol)
                if normalized_source == normalized_target:
                    # Store in cache and return
                    self.equivalent_symbols[cache_key] = target_symbol
                    return target_symbol
                    
        # No match found
        self.equivalent_symbols[cache_key] = None
        return None

    def update_price_direct(self, exchange, symbol, bid, ask, bid_qty=None, ask_qty=None, last=None):
        """Direct price update with minimal overhead"""


        with self.exchange_locks[exchange]:  # Exchange-specific lock instead of global
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
            self.update_counters[exchange] += 1

    def get_price_data(self, exchange, symbol):
        """Get price data for a symbol, ensuring fresh data"""
        with self.exchange_locks[exchange]:  # Exchange-specific lock
            return self.price_data[exchange].get(symbol, {}).copy()

    def update_funding_rate(self, exchange, symbol, rate):
        with self.lock:
            self.funding_rates[exchange][symbol] = rate

    def get_funding_rate(self, exchange, symbol):
        with self.lock:
            return self.funding_rates[exchange].get(symbol, "N/A")

    def clean_old_data(self, max_age=3600):
        current_time = time.time()
        
        with self.lock:
            for exchange in self.price_data:
                for symbol in list(self.price_data[exchange].keys()):
                    data = self.price_data[exchange][symbol]
                    if 'timestamp' in data and current_time - data['timestamp'] > max_age:
                        del self.price_data[exchange][symbol]
    def check_data_freshness(self):
        """Check if data is fresh and log warnings if not"""
        current_time = time.time()
        stale_threshold = 60  # 60 seconds
        
        with self.lock:
            for exchange in self.price_data:
                stale_count = 0
                total_count = 0
                
                for symbol, data in self.price_data[exchange].items():
                    if 'timestamp' in data:
                        total_count += 1
                        if current_time - data['timestamp'] > stale_threshold:
                            stale_count += 1
                            
                if total_count > 0 and stale_count > 0.1 * total_count:  # More than 10% stale
                    logger.warning(f"Data freshness issue: {exchange} has {stale_count}/{total_count} stale symbols")

# Create global data store
data_store = DataStore()

class BaseExchangeConnector:
    """Base class for exchange connectors with common functionality"""
    def __init__(self, app, exchange_name):
        self.app = app
        self.exchange_name = exchange_name
        self.websocket_managers = {}
        
        # Create HTTP session with appropriate headers
        self.session_manager = HttpSessionManager(
            exchange_name,
            base_headers={
                'User-Agent': 'Mozilla/5.0',
                'Accept': 'application/json'
            },
            max_retries=3
        )
        self.session = self.session_manager.session


#---------------------------------------------
# Exchange Connector Classes
#---------------------------------------------
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

class BybitConnector(BaseExchangeConnector):
    """Connects to Bybit exchange API and WebSockets."""
    def __init__(self, app):
        super().__init__(app, "bybit")
        self.bybit_reconnect_attempts = 0
        self.symbol_last_update = {}
        self.correction_history = {}
        
        # Add initialization for spot tracking attributes
        self.spot_active_symbols = set()
        self.spot_last_data_time = time.time()
        self.last_spot_pong_time = time.time()
        self.last_spot_ping_time = time.time()
        self.spot_connection_time = time.time()
        self.spot_subscribed_count = 0
        self.spot_subscription_batches = []

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
                                
                                # Direct update to data store
                                with data_store.lock:
                                    if spot_key not in data_store.price_data['bybit']:
                                        data_store.price_data['bybit'][spot_key] = {}
                                    data_store.price_data['bybit'][spot_key].update({
                                        'bid': bid_price,
                                        'ask': ask_price,
                                        'bidQty': bid_qty,
                                        'askQty': ask_qty,
                                        'last': mid_price,  # Use mid price as proxy for last
                                        'timestamp': time.time()
                                    })
                                    
                                # Log occasionally for verification
                                if random.random() < 0.001:
                                    logger.info(f"Bybit spot orderbook {spot_symbol} -> {future_symbol}: Bid={bid_price}, Ask={ask_price}")
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
                if not hasattr(manager, 'ws') or not manager.ws:
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
                    
                    # CHANGE HERE: Use orderbook.1.SYMBOL format instead of tickers.SYMBOL
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
            
            with data_store.lock:
                data_store.symbols['bybit'].clear()
                
            for cat in categories:
                url = f"https://api.bybit.com/v5/market/instruments-info?category={cat}"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['retCode'] == 0 and 'list' in data['result']:
                        instruments = data['result']['list']
                        
                        with data_store.lock:
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
                            
                            # Direct update to data store - NO queueing
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
            
            # Start symbol health monitoring
            
            
        except Exception as e:
            logger.error(f"Error setting up Bybit WebSocket: {e}")
        threading.Timer(30.0, self.check_data_freshness).start()

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
        
        # Rest of your existing code...
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
            
            # Find the most recent update time across all symbols
            with data_store.lock:
                for symbol in data_store.price_data['bybit']:
                    if not symbol.endswith('_SPOT'):  # Only check futures
                        data = data_store.price_data['bybit'][symbol]
                        if 'timestamp' in data and data['timestamp'] > last_update_time:
                            last_update_time = data['timestamp']
            
            # If no recent updates, force reconnection
            if last_update_time > 0 and current_time - last_update_time > 20:
                logger.warning(f"No Bybit updates for {current_time - last_update_time:.1f}s, forcing reconnection")
                
                # Force reconnection of linear WebSocket
                if "bybit_linear_ws" in self.websocket_managers:
                    manager = self.websocket_managers["bybit_linear_ws"]
                    if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                        logger.info("Forced Bybit linear WebSocket reconnection")
            
        except Exception as e:
            logger.error(f"Error checking Bybit data freshness: {e}")
            
        # Schedule next check
        if not stop_event.is_set():
            threading.Timer(15.0, self.check_data_freshness).start()  # Check every 15 seconds

    def monitor_symbol_health(self):
        """Unified health monitor for both futures and spot with selective resubscription"""
        if stop_event.is_set():
            return
            
        try:
            current_time = time.time()
            
            # PART 1: FUTURES MONITORING
            futures_stale_symbols = []
            futures_total = 0
            
            # Check which futures symbols are stale
            with data_store.lock:
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
                with data_store.lock:
                    for symbol in futures_symbols:
                        if symbol in data_store.price_data['bybit']:
                            data = data_store.price_data['bybit'][symbol]
                            if 'timestamp' in data and data['timestamp'] > most_recent_update:
                                most_recent_update = data['timestamp']
                
                # If no updates in the last 30 seconds, force reconnection
                if most_recent_update > 0 and current_time - most_recent_update > 30:
                    logger.warning(f"No Bybit futures updates received for {current_time - most_recent_update:.1f}s, forcing reconnection")
                    if "bybit_linear_ws" in self.websocket_managers:
                        manager = self.websocket_managers["bybit_linear_ws"]
                        if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                            manager.disconnect()
                            time.sleep(1)
                            manager.connect()
                            self.futures_connection_time = current_time

            # PART 2: SPOT MONITORING
            spot_stale_symbols = []
            spot_total = 0
            
            # Check which spot symbols are stale
            expected_spot_symbols = set(getattr(data_store, 'bybit_spot_to_future_map', {}).keys())
            
            with data_store.lock:
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
            

            # PART 4: DATA AGE CHECKS
            # Check spot data age
            spot_data_age = current_time - getattr(self, 'spot_last_data_time', current_time)
            if spot_data_age > 120:  # No data for 2 minutes
                logger.warning(f"No Bybit spot data for {spot_data_age:.1f}s, reconnecting")
                if "bybit_spot_ws" in self.websocket_managers:
                    manager = self.websocket_managers["bybit_spot_ws"]
                    if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                    return
                    
            # PART 5: ACTION DECISION LOGIC
            # FUTURES ACTION:
            if futures_stale_symbols:
                # If many stale symbols (>50%), do a full reconnect
                if len(futures_stale_symbols) > futures_total * 0.5:
                    logger.warning(f"Too many stale futures symbols ({len(futures_stale_symbols)}/{futures_total}), performing full reconnect")
                    if "bybit_linear_ws" in self.websocket_managers:
                        manager = self.websocket_managers["bybit_linear_ws"]
                        if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                            manager.disconnect()
                            time.sleep(1)
                            manager.connect()
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
                    if "bybit_spot_ws" in self.websocket_managers:
                        manager = self.websocket_managers["bybit_spot_ws"]
                        if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                            manager.disconnect()
                            time.sleep(1)
                            manager.connect()
                # Otherwise, just resubscribe to stale symbols
                else:
                    self.resubscribe_stale_symbols(spot_stale_symbols, "spot")
                    
        except Exception as e:
            logger.error(f"Error in Bybit symbol health monitor: {e}")
            
        # Schedule next check
        if not stop_event.is_set():
            threading.Timer(30.0, self.monitor_symbol_health).start()

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

        # Process symbols in small batches (10 per batch)
        #batch_size = 10
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
                    manager = self.websocket_managers[ws_name]
                    if hasattr(manager, 'disconnect') and hasattr(manager, 'connect'):
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                        
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
                
            # Apply all updates at once
            with data_store.lock:
                for symbol, rate in results:
                    data_store.funding_rates['bybit'][symbol] = rate
                    
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
                
            # Apply all updates at once
            with data_store.lock:
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
                                    
                                    # Store in data store
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
            
            # 6. Start health monitoring
            threading.Timer(30, self.monitor_symbol_health).start()
            
            logger.info("Bybit connector initialization complete")
        except Exception as e:
            logger.error(f"Error during Bybit connector initialization: {e}")
            import traceback
            logger.error(traceback.format_exc())

class OkxConnector(BaseExchangeConnector):
    """Connects to OKX exchange API and WebSockets."""
    def __init__(self, app):
        super().__init__(app, "okx")
        self.session_manager.rate_limit = 5  # Lower rate limit for OKX
        self.reconnect_attempts = 0
        self.correction_history = {}
        
        # Initialize state tracking attributes
        self.okx_connection_time = time.time()
        self.okx_last_data_time = time.time()
        self.okx_active_symbols = set()

    def connect_websocket(self):
        """Connect to OKX WebSocket with optimized connection management"""
        self.fetch_symbols()
        symbols_list = data_store.get_symbols('okx')
        
        if not symbols_list:
            logger.error("No OKX symbols found")
            return
            
        # OKX WebSocket endpoint
        ws_url = "wss://ws.okx.com/ws/v5/public"

        def on_message(ws, message):
            try:
                data = json.loads(message)
                # Update last data time for health monitoring
                self.okx_last_data_time = time.time()
                
                # Handle pings (OKX uses different ping format)
                if 'event' in data and data['event'] == 'ping':
                    pong_msg = json.dumps({"event": "pong"})
                    ws.send(pong_msg)
                    return
                    
                # Process orderbook data
                if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                    # Get symbol from arg
                    if 'arg' in data and 'instId' in data['arg']:
                        symbol = data['arg']['instId']
                        
                        # Track active symbols for health monitoring
                        self.okx_active_symbols.add(symbol)
                        
                        book_data = data['data'][0]
                        if 'bids' in book_data and 'asks' in book_data and book_data['bids'] and book_data['asks']:
                            # Get previous prices for validation
                            previous_prices = data_store.get_price_data('okx', symbol)
                            
                            # Extract and validate prices
                            raw_bid = float(book_data['bids'][0][0])
                            raw_ask = float(book_data['asks'][0][0])
                            
                            # Validate prices
                            best_bid = self.validate_price(raw_bid, symbol, 'bid', previous_prices)
                            best_ask = self.validate_price(raw_ask, symbol, 'ask', previous_prices)
                            
                            bid_qty = float(book_data['bids'][0][1])
                            ask_qty = float(book_data['asks'][0][1])
                            
                            # DIRECT UPDATE - no queue
                            data_store.update_price_direct(
                                'okx', symbol, best_bid, best_ask, bid_qty, ask_qty
                            )
            except Exception as e:
                logger.error(f"Error processing OKX message: {e}")
                
        def on_open(ws):
            logger.info("OKX WebSocket connected")
            self.okx_connection_time = time.time()
            self.okx_last_data_time = time.time()
            
            # Subscribe to symbols in batches
            self._batch_subscribe_okx_symbols(ws, symbols_list)
            
            # Start heartbeat timer
            def send_heartbeat():
                if not manager.is_running or stop_event.is_set():
                    return
                    
                try:
                    # Check if socket is valid
                    if not ws or not hasattr(ws, 'sock') or not ws.sock or not hasattr(ws.sock, 'connected'):
                        logger.warning("OKX WebSocket disconnected, cannot send heartbeat")
                        return
                        
                    if not ws.sock.connected:
                        logger.warning("OKX WebSocket connection lost, cannot send heartbeat")
                        return
                        
                    # Send ping in OKX format
                    ws.send(json.dumps({"event": "ping"}))
                    logger.debug("Sent heartbeat to OKX")
                    
                    # Check data freshness
                    current_time = time.time()
                    if current_time - self.okx_last_data_time > 60:
                        logger.warning(f"No OKX data for {current_time - self.okx_last_data_time:.1f}s, reconnecting")
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                        return
                except Exception as e:
                    logger.error(f"Error sending heartbeat to OKX: {e}")
                    
                # Schedule next heartbeat if still running
                if manager.is_running and not stop_event.is_set():
                    threading.Timer(20.0, send_heartbeat).start()
                    
            # Start the heartbeat timer
            threading.Timer(20.0, send_heartbeat).start()
            
        # Create WebSocket manager configured for OKX performance
        manager = WebSocketManager(
            url=ws_url,
            name="okx_futures_ws",
            on_message=on_message,
            on_open=on_open,
            ping_interval=15,
            ping_timeout=10
        )
        
        
        # Store and connect
        self.websocket_managers["okx_futures_ws"] = manager
        manager.connect()
        
        # Start health monitoring specific to OKX
        def monitor_okx_health():
            if stop_event.is_set() or not manager.is_running:
                return
                
            try:
                current_time = time.time()
                
                # Check connection age (force periodic reconnect)
                connection_age = current_time - self.okx_connection_time
                if connection_age > 3600:  # 1 hour
                    logger.info(f"Performing scheduled OKX reconnection after {connection_age:.1f}s")
                    manager.disconnect()
                    time.sleep(1)
                    manager.connect()
                    return
                    
                # Check for recent data
                data_age = current_time - self.okx_last_data_time
                if data_age > 90:  # No data for 90 seconds
                    logger.warning(f"No OKX data for {data_age:.1f}s, reconnecting")
                    manager.disconnect()
                    time.sleep(1)
                    manager.connect()
                    return
                    
                # Check active symbols vs expected
                expected_symbols = set(symbols_list)
                if len(expected_symbols) > 10:
                    coverage_pct = (len(self.okx_active_symbols) / len(expected_symbols)) * 100
                    logger.info(f"OKX connection health: {len(self.okx_active_symbols)}/{len(expected_symbols)} active symbols ({coverage_pct:.1f}%)")
                    
                    # Force reconnect if active symbols coverage is too low
                    if coverage_pct < 50:
                        logger.warning(f"Poor OKX symbol coverage: only {coverage_pct:.1f}% of symbols active, reconnecting")
                        manager.disconnect()
                        time.sleep(1)
                        manager.connect()
                        return
            except Exception as e:
                logger.error(f"Error in OKX health monitor: {e}")
                
            # Schedule next check
            if not stop_event.is_set() and manager.is_running:
                threading.Timer(60, monitor_okx_health).start()
                
        # Start health monitoring
        threading.Timer(60, monitor_okx_health).start()

    def validate_price(self, price, symbol, side, previous_prices=None):
        """
        Validates and potentially corrects price data from OKX with improved stability.
        Args:
            price: The price value to validate
            symbol: Trading symbol for context
            side: 'bid' or 'ask' for logging context
            previous_prices: Dictionary of previous prices for this symbol for comparison
        Returns:
            Validated and potentially corrected price
        """
        try:
            # Convert to float if string
            if isinstance(price, str):
                price = float(price)
                
            # Initialize correction history tracker if not exists
            if not hasattr(self, 'correction_history'):
                self.correction_history = {}
                
            # Track price history for this symbol/side
            history_key = f"{symbol}_{side}"
            if history_key not in self.correction_history:
                self.correction_history[history_key] = {
                    'last_correction_time': 0,
                    'price_history': [price],
                    'correction_count': 0
                }
            else:
                # Add to price history (keep last 5)
                self.correction_history[history_key]['price_history'].append(price)
                if len(self.correction_history[history_key]['price_history']) > 5:
                    self.correction_history[history_key]['price_history'].pop(0)
                    
            # Skip validation if we don't have previous prices or recently corrected
            if not previous_prices or side not in previous_prices:
                return price
                
            prev_price = previous_prices[side]
            if prev_price is None:
                return price
                
            # Don't correct again if we corrected recently (within 30 seconds)
            current_time = time.time()
            if (current_time - self.correction_history[history_key]['last_correction_time'] < 30 and
                self.correction_history[history_key]['correction_count'] > 0):
                return price
                
            # Get reference prices from other exchanges if available
            reference_price = None
            for exchange in ['binance', 'bybit']:
                equiv_symbol = data_store.find_equivalent_symbol('okx', symbol, exchange)
                if equiv_symbol:
                    other_data = data_store.get_price_data(exchange, equiv_symbol)
                    if other_data and side in other_data:
                        reference_price = other_data[side]
                        break
                        
            # Calculate ratio between current and previous price
            ratio = price / prev_price if prev_price > 0 else 1.0
            
            # If price change is suspicious (factor of ~10)
            if (0.05 < ratio < 0.15 or 8 < ratio < 12):
                # If we have a reference price from another exchange, use it to validate
                if reference_price:
                    reference_ratio = price / reference_price
                    
                    # Only correct if reference price also confirms the issue
                    if (0.05 < reference_ratio < 0.15):
                        corrected = price * 10
                        logger.info(f"Correcting OKX price for {symbol} {side} (confirmed by reference): {price} -> {corrected}")
                        self.correction_history[history_key]['last_correction_time'] = current_time
                        self.correction_history[history_key]['correction_count'] += 1
                        return corrected
                    elif (8 < reference_ratio < 12):
                        corrected = price / 10
                        logger.info(f"Correcting OKX price for {symbol} {side} (confirmed by reference): {price} -> {corrected}")
                        self.correction_history[history_key]['last_correction_time'] = current_time
                        self.correction_history[history_key]['correction_count'] += 1
                        return corrected
                    else:
                        # Reference doesn't confirm issue, so keep original price
                        return price
                        
                # Without reference, look at historical consistency
                price_history = self.correction_history[history_key]['price_history']
                if len(price_history) >= 3:
                    # Check if this is a one-time anomaly vs consistent shift
                    recent_avg = sum(price_history[:-1]) / len(price_history[:-1])
                    recent_ratio = price / recent_avg
                    
                    if (0.05 < recent_ratio < 0.15 and
                        self.correction_history[history_key]['correction_count'] < 2):
                        corrected = price * 10
                        logger.info(f"Correcting OKX price for {symbol} {side} (based on history): {price} -> {corrected}")
                        self.correction_history[history_key]['last_correction_time'] = current_time
                        self.correction_history[history_key]['correction_count'] += 1
                        return corrected
                    elif (8 < recent_ratio < 12 and
                        self.correction_history[history_key]['correction_count'] < 2):
                        corrected = price / 10
                        logger.info(f"Correcting OKX price for {symbol} {side} (based on history): {price} -> {corrected}")
                        self.correction_history[history_key]['last_correction_time'] = current_time
                        self.correction_history[history_key]['correction_count'] += 1
                        return corrected
                        
            # If we get here, price is valid or we decided not to correct
            return price
        except Exception as e:
            logger.error(f"Error validating OKX price: {e}")
            # Return original price if validation fails
            return price

    def fetch_spot_symbols(self):
        """Fetch spot market information for corresponding futures"""
        try:
            response = self.session.get('https://www.okx.com/api/v5/public/instruments?instType=SPOT')
            if response.status_code == 200:
                data = response.json()
                futures_symbols = data_store.get_symbols('okx')
                
                if data['code'] == '0' and 'data' in data:
                    with data_store.lock:
                        for symbol_info in data['data']:
                            if symbol_info['state'] == 'live':
                                spot_symbol = symbol_info['instId']
                                
                                # Find corresponding futures symbol which has -SWAP suffix
                                base_quote = spot_symbol.split('-')
                                if len(base_quote) == 2:
                                    base, quote = base_quote
                                    potential_swap = f"{base}-{quote}-SWAP"
                                    
                                    if potential_swap in futures_symbols:
                                        # Store spot tick size
                                        if potential_swap not in data_store.tick_sizes['okx']:
                                            data_store.tick_sizes['okx'][potential_swap] = {}
                                        data_store.tick_sizes['okx'][potential_swap]['spot_tick_size'] = float(symbol_info['tickSz'])
                                        
            logger.info(f"Fetched OKX spot symbols info")
        except Exception as e:
            logger.error(f"Error fetching OKX spot symbols: {e}")

    def connect_spot_websocket(self):
        """Connect to OKX WebSocket for spot prices"""
        self.fetch_spot_symbols()
        
        # Map futures symbols to spot symbols
        futures_symbols = data_store.get_symbols('okx')
        spot_to_future_map = {}
        
        # Create a mapping of spot symbols to futures symbols
        for future_symbol in futures_symbols:
            if '-SWAP' in future_symbol:
                parts = future_symbol.split('-')
                if len(parts) == 3:  # Format: BTC-USDT-SWAP
                    base, quote = parts[0], parts[1]
                    spot_symbol = f"{base}-{quote}"
                    spot_to_future_map[spot_symbol] = future_symbol
                    
        # OKX WebSocket for spot data
        ws_url = "wss://ws.okx.com/ws/v5/public"
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle pings
                if 'event' in data and data['event'] == 'ping':
                    pong_msg = json.dumps({"event": "pong"})
                    ws.send(pong_msg)
                    return
                    
                # Handle orderbook data
                if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                    if 'arg' in data and 'instId' in data['arg']:
                        spot_symbol = data['arg']['instId']
                        
                        # Check if this spot symbol maps to a futures symbol
                        if spot_symbol in spot_to_future_map:
                            future_symbol = spot_to_future_map[spot_symbol]
                            spot_key = f"{future_symbol}_SPOT"
                            
                            book_data = data['data'][0]
                            if 'bids' in book_data and 'asks' in book_data and book_data['bids'] and book_data['asks']:
                                # Get previous prices for this symbol if available
                                previous_prices = data_store.get_price_data('okx', spot_key)
                                
                                # Extract raw prices
                                raw_bid = float(book_data['bids'][0][0])
                                raw_ask = float(book_data['asks'][0][0])
                                
                                # Validate and potentially correct the prices
                                best_bid = self.validate_price(raw_bid, spot_key, 'bid', previous_prices)
                                best_ask = self.validate_price(raw_ask, spot_key, 'ask', previous_prices)
                                
                                bid_qty = float(book_data['bids'][0][1])
                                ask_qty = float(book_data['asks'][0][1])
                                
                                # Store the validated prices using direct update
                                data_store.update_price_direct(
                                    'okx', spot_key, best_bid, best_ask, bid_qty, ask_qty
                                )
            except Exception as e:
                logger.error(f"Error processing OKX spot message: {e}")
                
        def on_open(ws):
            logger.info("OKX spot WebSocket connected")
            
            # Subscribe to spot orderbooks for major pairs
            # Get just the top 20 spot symbols from our mapping
            major_pairs = list(spot_to_future_map.keys())[:20]
            args = []
            
            for symbol in major_pairs:
                args.append({
                    "channel": "books",
                    "instId": symbol
                })
                
            subscribe_msg = json.dumps({
                "op": "subscribe",
                "args": args
            })
            
            logger.info(f"Sending OKX spot subscription for {len(major_pairs)} symbols")
            ws.send(subscribe_msg)
            
        manager = WebSocketManager(
            url=ws_url,
            name="okx_spot_ws",
            on_message=on_message,
            on_open=on_open,
            ping_interval=15,
            ping_timeout=10
        )
        
        self.websocket_managers["okx_spot_ws"] = manager
        manager.connect()

    def fetch_symbols(self):
        """Fetch all tradable symbols from OKX (futures and spot)"""
        try:
            # Get all swap/futures symbols
            response = self.session.get('https://www.okx.com/api/v5/public/instruments?instType=SWAP')
            if response.status_code == 200:
                data = response.json()
                if data['code'] == '0' and 'data' in data:
                    with data_store.lock:
                        data_store.symbols['okx'].clear()
                        for symbol_info in data['data']:
                            if symbol_info['state'] == 'live':
                                data_store.symbols['okx'].add(symbol_info['instId'])
                                
                                # Extract tick size
                                if symbol_info['instId'] not in data_store.tick_sizes['okx']:
                                    data_store.tick_sizes['okx'][symbol_info['instId']] = {}
                                data_store.tick_sizes['okx'][symbol_info['instId']]['future_tick_size'] = float(symbol_info['tickSz'])
                                
                    logger.info(f"Fetched {len(data_store.symbols['okx'])} OKX futures symbols")
                    data_store.update_symbol_maps()
                else:
                    logger.error(f"Error in OKX response: {data}")
            else:
                logger.error(f"Error fetching OKX symbols: Status {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching OKX symbols: {e}")

    def _batch_subscribe_okx_symbols(self, ws, symbols):
        """Subscribe to OKX symbols in multiple batches with smaller size and longer delays"""
        batch_size = 20  # Smaller batch size for OKX
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        def send_batch(batch_idx):
            if batch_idx >= total_batches:
                logger.info(f"Completed all {total_batches} subscription batches for OKX")
                return
                
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(symbols))
            batch = list(symbols)[start_idx:end_idx]
            
            args = []
            for symbol in batch:
                args.append({
                    "channel": "books",  # Use "books" channel for order book data
                    "instId": symbol
                })
                
            subscribe_msg = json.dumps({
                "op": "subscribe",
                "args": args
            })
            
            logger.info(f"Sending OKX subscription batch {batch_idx+1}/{total_batches} ({len(batch)} symbols)")
            ws.send(subscribe_msg)
            
            # Schedule the next batch with a longer delay (4 seconds instead of 2)
            if batch_idx + 1 < total_batches:
                threading.Timer(2.5, send_batch, [batch_idx + 1]).start()
                
        # Start the batch process
        send_batch(0)

    def _fetch_funding_batch(self, symbols_batch):
        """Fetch funding rates for a batch of symbols"""
        try:
            results = []
            for symbol in symbols_batch:
                url = f"https://www.okx.com/api/v5/public/funding-rate?instId={symbol}"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['code'] == '0' and 'data' in data and data['data']:
                        funding_info = data['data'][0]
                        # OKX returns rates directly as percentages
                        rate = float(funding_info['fundingRate']) * 100
                        formatted_rate = f"{rate:.4f}%"
                        results.append((symbol, formatted_rate))
                        
                # Add small delay to avoid rate limits
                time.sleep(0.1)
                
            # Apply all updates at once
            with data_store.lock:
                for symbol, rate in results:
                    data_store.funding_rates['okx'][symbol] = rate
                    
            return len(results)
        except Exception as e:
            logger.error(f"Error in OKX funding batch: {e}")
            return 0

    def update_funding_rates(self):
        """Fetch funding rates from OKX API using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('okx'))
                
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
                    
                logger.info(f"Updated OKX funding rates for {successful_updates} symbols")
            except Exception as e:
                logger.error(f"Error updating OKX funding rates: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

    def _fetch_changes_batch(self, symbols_batch):
        """Fetch 24h changes for a batch of symbols with better rate limiting"""
        try:
            results = []
            for symbol in symbols_batch:
                url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
                response = self.session.get(url)
                
                if response.status_code == 200:
                    data = response.json()
                    if data['code'] == '0' and 'data' in data and data['data']:
                        ticker_info = data['data'][0]
                        
                        # Calculate 24h percent change
                        last_price = float(ticker_info['last'])
                        open_24h = float(ticker_info['open24h'])
                        
                        if open_24h > 0:
                            change_percent = ((last_price - open_24h) / open_24h) * 100
                            results.append((symbol, change_percent))
                            
                # Sleep longer between requests to reduce rate limit issues
                time.sleep(0.5)
                
            # Apply all updates at once
            with data_store.lock:
                for symbol, change in results:
                    data_store.daily_changes['okx'][symbol] = change
                    
            return len(results)
        except Exception as e:
            logger.error(f"Error in OKX changes batch: {e}")
            return 0

    def update_24h_changes(self):
        """Fetch 24-hour price changes for symbols using thread pool"""
        while not stop_event.is_set():
            try:
                symbols_list = list(data_store.get_symbols('okx'))
                
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
                    
                logger.info(f"Updated OKX 24h changes for {successful_updates} symbols")
            except Exception as e:
                logger.error(f"Error updating OKX 24h changes: {e}")
                
            # Sleep with periodic checks for stop event
            for _ in range(30):
                if stop_event.is_set():
                    break
                time.sleep(10)

#---------------------------------------------
# UI Application Class
#---------------------------------------------

class ExchangeMonitorApp:
    """Main application UI class."""
    def __init__(self, root):
        self.root = root
        self.root.title("Crypto Exchange Monitor")
        self.root.geometry("1400x800")
        
        # Initialize sort tracking variables for both tables
        self.upper_sorted_column = 'symbol'
        self.lower_sorted_column = 'symbol'
        self.last_update_time = 0
        
        # Set up style
        self.style = ttk.Style()
        self.style.theme_use('clam')  # Use a more modern theme
        
        # Customize treeview colors
        self.style.configure(
            "Treeview",
            background="#f5f5f5",
            foreground="black",
            rowheight=25,
            fieldbackground="#f5f5f5"
        )
        self.style.map('Treeview', background=[('selected', '#347ab3')])
        
        # Create the main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create the main control panel (exchange selection and filter)
        self.create_control_panel()
        
        # Create upper table frame
        self.upper_table_frame = ttk.LabelFrame(self.main_frame, text="Main Data View", padding="5")
        self.upper_table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create lower table frame
        self.lower_table_frame = ttk.LabelFrame(self.main_frame, text="Secondary Data View", padding="5")
        self.lower_table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create sorting controls for each table
        self.create_table_controls(self.upper_table_frame, "upper")
        self.create_table_controls(self.lower_table_frame, "lower")
        
        # Create the data tables
        self.upper_table = self.create_data_table(self.upper_table_frame, "upper")
        self.lower_table = self.create_data_table(self.lower_table_frame, "lower")
        
        # Data refresh flags for both tables
        self.upper_mouse_over_table = False
        self.lower_mouse_over_table = False
        
        # Exchange connectors and websocket managers
        self.websocket_managers = {}
        self.binance = BinanceConnector(self)
        self.bybit = BybitConnector(self)
        self.okx = OkxConnector(self)
        
        # Initialize thread pools
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)
        
        # Start with Binance by default
        self.start_exchange_threads()
        
        # Start health monitor
        self.start_health_monitor()
        
        # Schedule periodic UI updates
        self.schedule_updates()

    def create_control_panel(self):
        """Create the control panel with configuration options"""
        control_frame = ttk.LabelFrame(self.main_frame, text="Controls", padding="10")
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Exchange selection
        ttk.Label(control_frame, text="Exchange:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.exchange_var = tk.StringVar(value="all")
        exchange_combo = ttk.Combobox(
            control_frame,
            textvariable=self.exchange_var,
            values=["all", "binance", "bybit", "okx"],
            width=10,
            state="readonly"
        )
        exchange_combo.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        exchange_combo.bind("<<ComboboxSelected>>", self.on_exchange_change)
        
        # Refresh button
        refresh_btn = ttk.Button(
            control_frame,
            text="Refresh Data",
            command=self.manual_refresh
        )
        refresh_btn.grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        
        # Export CSV button
        export_btn = ttk.Button(
            control_frame,
            text="Export CSV",
            command=self.export_to_csv
        )
        export_btn.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        # Filter by symbol
        ttk.Label(control_frame, text="Filter:").grid(row=0, column=10, padx=5, pady=5, sticky=tk.W)
        self.filter_var = tk.StringVar()
        filter_entry = ttk.Entry(control_frame, textvariable=self.filter_var, width=15)
        filter_entry.grid(row=0, column=11, padx=5, pady=5, sticky=tk.W)
        self.filter_var.trace("w", lambda *args: self.apply_filter())

    def create_table_controls(self, parent_frame, table_id):
        """Create sorting controls for a specific table"""
        control_frame = ttk.Frame(parent_frame)
        control_frame.pack(fill=tk.X, pady=(0, 5))
        
        # Sort options
        ttk.Label(control_frame, text="Sort by:").pack(side=tk.LEFT, padx=5)
        
        # Create sort variables for this table
        if table_id == "upper":
            self.upper_sort_column_var = tk.StringVar(value="Symbol")
            self.upper_sort_direction_var = tk.StringVar(value="ascending")
            sort_column_var = self.upper_sort_column_var
            sort_direction_var = self.upper_sort_direction_var
            callback = lambda *args: self.apply_sorting("upper")
        else:
            self.lower_sort_column_var = tk.StringVar(value="Symbol")
            self.lower_sort_direction_var = tk.StringVar(value="ascending")
            sort_column_var = self.lower_sort_column_var
            sort_direction_var = self.lower_sort_direction_var
            callback = lambda *args: self.apply_sorting("lower")
            
        sort_combo = ttk.Combobox(
            control_frame,
            textvariable=sort_column_var,
            values=["Symbol", "Exchange", "Funding Rate", "Spread vs Spot",
                    "Spread vs Binance", "Spread vs OKX", "Spread vs Bybit", "24h Change"],
            width=15,
            state="readonly"
        )
        sort_combo.pack(side=tk.LEFT, padx=5)
        sort_combo.bind("<<ComboboxSelected>>", callback)
        
        # Sort direction
        ttk.Radiobutton(
            control_frame,
            text="Ascending",
            variable=sort_direction_var,
            value="ascending",
            command=callback
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Radiobutton(
            control_frame,
            text="Descending",
            variable=sort_direction_var,
            value="descending",
            command=callback
        ).pack(side=tk.LEFT, padx=5)

    def export_to_csv(self):
        """Export the current data table to a CSV file"""
        # Ask user for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export data as CSV"
        )
        if not file_path:  # User cancelled
            return
            
        try:
            # Get all symbols from all exchanges
            all_symbols = {}
            for exchange in ['binance', 'bybit', 'okx']:
                symbols = data_store.get_symbols(exchange)
                all_symbols[exchange] = set(symbols)
                
            # Prepare the data using the same function that updates the table
            # Use the upper table's sort settings for export
            table_data = self._prepare_table_data(
                all_symbols,
                self.upper_sort_column_var.get(),
                self.upper_sort_direction_var.get()
            )
            
            # Write to CSV
            with open(file_path, 'w', newline='') as csvfile:
                # Create CSV writer and write header
                fieldnames = [
                    'Symbol', 'Exchange', 'Bid', 'Ask', 'Funding Rate',
                    'Spread vs Spot', 'Spread vs Binance', 'Spread vs OKX',
                    'Spread vs Bybit', 'Future Tick Size', 'Spot Tick Size',
                    '24h Change'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write data rows
                for item in table_data:
                    writer.writerow({
                        'Symbol': item['symbol'],
                        'Exchange': item['exchange'],
                        'Bid': item['bid'],
                        'Ask': item['ask'],
                        'Funding Rate': item['funding_rate'],
                        'Spread vs Spot': item['spread_vs_spot'],
                        'Spread vs Binance': item['spread_vs_binance'],
                        'Spread vs OKX': item['spread_vs_okx'],
                        'Spread vs Bybit': item['spread_vs_bybit'],
                        'Future Tick Size': item['future_tick_size'],
                        'Spot Tick Size': item['spot_tick_size'],
                        '24h Change': item['change_24h']
                    })
                    
            # Show confirmation
            messagebox.showinfo("Export Successful", f"Data exported to {file_path}")
            logger.info(f"Data exported to CSV: {file_path}")
        except Exception as e:
            # Show error message
            messagebox.showerror("Export Failed", f"Failed to export data: {str(e)}")
            logger.error(f"CSV export error: {e}")

    def create_data_table(self, parent_frame, table_id):
        """Create a data table in the given parent frame"""
        # Create a frame for the table with scrollbars
        table_frame = ttk.Frame(parent_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create scrollbars
        y_scrollbar = ttk.Scrollbar(table_frame)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Create treeview
        table = ttk.Treeview(
            table_frame,
            yscrollcommand=y_scrollbar.set,
            xscrollcommand=x_scrollbar.set,
            selectmode='none'
        )
        
        # Configure columns
        table['columns'] = (
            'symbol', 'exchange', 'bid', 'ask', 'funding_rate', 'spread_vs_spot', 'spread_vs_binance',
            'spread_vs_okx', 'spread_vs_bybit', 'future_tick_size', 'spot_tick_size', 'change_24h'
        )
        
        # Format columns
        table.column('#0', width=0, stretch=tk.NO)  # Hidden ID column
        table.column('symbol', width=120, anchor=tk.W)
        table.column('exchange', width=80, anchor=tk.W)
        table.column('bid', width=100, anchor=tk.E)
        table.column('ask', width=100, anchor=tk.E)
        table.column('funding_rate', width=100, anchor=tk.E)
        table.column('spread_vs_spot', width=120, anchor=tk.E)
        table.column('spread_vs_binance', width=120, anchor=tk.E)
        table.column('spread_vs_okx', width=120, anchor=tk.E)
        table.column('spread_vs_bybit', width=120, anchor=tk.E)
        table.column('future_tick_size', width=100, anchor=tk.E)
        table.column('spot_tick_size', width=100, anchor=tk.E)
        table.column('change_24h', width=100, anchor=tk.E)
        
        # Create headings
        table.heading('#0', text='', anchor=tk.W)
        table.heading('symbol', text='Symbol', anchor=tk.W)
        table.heading('exchange', text='Exchange', anchor=tk.W)
        table.heading('bid', text='Bid', anchor=tk.W)
        table.heading('ask', text='Ask', anchor=tk.W)
        table.heading('funding_rate', text='Funding Rate', anchor=tk.W)
        table.heading('spread_vs_spot', text='Spread vs Spot', anchor=tk.W)
        table.heading('spread_vs_binance', text='Spread vs Binance', anchor=tk.W)
        table.heading('spread_vs_okx', text='Spread vs OKX', anchor=tk.W)
        table.heading('spread_vs_bybit', text='Spread vs Bybit', anchor=tk.W)
        table.heading('future_tick_size', text='Future Tick', anchor=tk.W)
        table.heading('spot_tick_size', text='Spot Tick', anchor=tk.W)
        table.heading('change_24h', text='24h Change%', anchor=tk.W)
        
        # Add heading click handlers for sorting
        for col in table['columns']:
            table.heading(col, command=lambda _col=col, _id=table_id: self.on_heading_click(_col, _id))
        
        # Pack the table
        table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Configure scrollbars
        y_scrollbar.config(command=table.yview)
        x_scrollbar.config(command=table.xview)
        
        # Mouse over handling to freeze updates
        if table_id == "upper":
            table.bind('<Enter>', lambda e: self.on_table_enter(e, "upper"))
            table.bind('<Leave>', lambda e: self.on_table_leave(e, "upper"))
        else:
            table.bind('<Enter>', lambda e: self.on_table_enter(e, "lower"))
            table.bind('<Leave>', lambda e: self.on_table_leave(e, "lower"))
        
        # Set alternating row colors
        table.tag_configure('odd', background='#f0f0f0')
        table.tag_configure('even', background='#ffffff')
        
        # Set color tags for positive/negative values
        table.tag_configure('positive', foreground='green')
        table.tag_configure('negative', foreground='red')
        table.tag_configure('neutral', foreground='black')
        
        return table

    def on_table_enter(self, event, table_id):
        """Freeze updates when mouse enters the table"""
        if table_id == "upper":
            self.upper_mouse_over_table = True
        else:
            self.lower_mouse_over_table = True

    def on_table_leave(self, event, table_id):
        """Resume updates when mouse leaves the table"""
        if table_id == "upper":
            self.upper_mouse_over_table = False
            self.update_data_table(force_upper=True)
        else:
            self.lower_mouse_over_table = False
            self.update_data_table(force_lower=True)

    def on_exchange_change(self, event):
        """Handle exchange selection change"""
        self.restart_exchange_threads()

    def on_heading_click(self, column, table_id):
        """Handle column header click for sorting"""
        # Map treeview column names to sort field names
        column_to_field = {
            'symbol': 'Symbol',
            'exchange': 'Exchange',
            'funding_rate': 'Funding Rate',
            'spread_vs_spot': 'Spread vs Spot',
            'spread_vs_binance': 'Spread vs Binance',
            'spread_vs_okx': 'Spread vs OKX',
            'spread_vs_bybit': 'Spread vs Bybit',
            'change_24h': '24h Change'
        }
        
        if column in column_to_field:
            if table_id == "upper":
                logger.info(f"Upper table header clicked: {column} -> {column_to_field[column]}")
                
                # Set the sort column variable
                self.upper_sort_column_var.set(column_to_field[column])
                
                # Toggle sort direction if clicking the same column
                if self.upper_sorted_column == column:
                    if self.upper_sort_direction_var.get() == 'ascending':
                        self.upper_sort_direction_var.set('descending')
                    else:
                        self.upper_sort_direction_var.set('ascending')
                        
                self.upper_sorted_column = column
                logger.info(f"Sorting upper table by {column_to_field[column]} ({self.upper_sort_direction_var.get()})")
                
                # Force a sort and update now
                self.apply_sorting("upper")
            else:  # Lower table
                logger.info(f"Lower table header clicked: {column} -> {column_to_field[column]}")
                
                # Set the sort column variable
                self.lower_sort_column_var.set(column_to_field[column])
                
                # Toggle sort direction if clicking the same column
                if self.lower_sorted_column == column:
                    if self.lower_sort_direction_var.get() == 'ascending':
                        self.lower_sort_direction_var.set('descending')
                    else:
                        self.lower_sort_direction_var.set('ascending')
                        
                self.lower_sorted_column = column
                logger.info(f"Sorting lower table by {column_to_field[column]} ({self.lower_sort_direction_var.get()})")
                
                # Force a sort and update now
                self.apply_sorting("lower")

    def apply_sorting(self, table_id="upper"):
        """Apply sorting to the specified data table and trigger an update"""
        if table_id == "upper":
            logger.info(f"Applying upper sort: {self.upper_sort_column_var.get()} ({self.upper_sort_direction_var.get()})")
            self.update_data_table(force_upper=True)
        else:
            logger.info(f"Applying lower sort: {self.lower_sort_column_var.get()} ({self.lower_sort_direction_var.get()})")
            self.update_data_table(force_lower=True)

    def apply_filter(self):
        """Apply symbol filter to both data tables"""
        self.update_data_table(force_upper=True, force_lower=True)

    def manual_refresh(self):
        """Manually refresh data"""
        self.restart_exchange_threads()
        
    def _prepare_table_data(self, all_symbols, sort_column=None, sort_direction=None):
        """Prepare table data in background thread with explicit sort parameters"""
        # If sort parameters weren't provided, use the current values
        if sort_column is None:
            sort_column = self.upper_sort_column_var.get()
            
        if sort_direction is None:
            sort_direction = self.upper_sort_direction_var.get()
            
        # Filter by selected exchange
        selected_exchange = self.exchange_var.get()
        exchanges_to_show = ['binance', 'bybit', 'okx'] if selected_exchange == 'all' else [selected_exchange]
        
        # Apply symbol filter
        symbol_filter = self.filter_var.get().upper()
        
        # Prepare data for display
        table_data = []
        
        for exchange in exchanges_to_show:
            if exchange in all_symbols:
                for symbol in all_symbols[exchange]:
                    # Apply symbol filter
                    if symbol_filter and symbol_filter not in symbol:
                        continue
                        
                    # Get price data
                    price_data = data_store.get_price_data(exchange, symbol)
                    if not price_data or 'bid' not in price_data or 'ask' not in price_data:
                        continue
                        
                    # Extract bid and ask prices for display
                    bid_price = price_data.get('bid', 'N/A')
                    ask_price = price_data.get('ask', 'N/A')                    
                    
                    # Get funding rate
                    funding_rate = data_store.get_funding_rate(exchange, symbol)
                    
                    # Get tick sizes
                    future_tick_size_raw = data_store.tick_sizes.get(exchange, {}).get(symbol, {}).get('future_tick_size', 'N/A')
                    spot_tick_size_raw = data_store.tick_sizes.get(exchange, {}).get(symbol, {}).get('spot_tick_size', 'N/A')
                    
                    # Initialize formatted tick sizes
                    future_tick_size = future_tick_size_raw
                    spot_tick_size = spot_tick_size_raw
                    
                    # Calculate tick sizes as percentages of average bid-ask
                    if bid_price != 'N/A' and ask_price != 'N/A':
                        try:
                            # Convert to float if they're strings
                            bid_value = float(bid_price) if isinstance(bid_price, str) else bid_price
                            ask_value = float(ask_price) if isinstance(ask_price, str) else ask_price
                            
                            if isinstance(bid_value, (int, float)) and isinstance(ask_value, (int, float)):
                                avg_price = (bid_value + ask_value) / 2
                                
                                # Future tick size percentage
                                if future_tick_size_raw != 'N/A':
                                    try:
                                        future_tick_value = float(future_tick_size_raw) if isinstance(future_tick_size_raw, str) else future_tick_size_raw
                                        if avg_price > 0:
                                            future_tick_pct = (future_tick_value / avg_price) * 100
                                            future_tick_size = f"{future_tick_pct:.6f}%"
                                    except (ValueError, TypeError):
                                        pass
                                
                                # Spot tick size percentage
                                if spot_tick_size_raw != 'N/A':
                                    try:
                                        spot_tick_value = float(spot_tick_size_raw) if isinstance(spot_tick_size_raw, str) else spot_tick_size_raw
                                        if avg_price > 0:
                                            spot_tick_pct = (spot_tick_value / avg_price) * 100
                                            spot_tick_size = f"{spot_tick_pct:.6f}%"
                                    except (ValueError, TypeError):
                                        pass
                        except (ValueError, TypeError):
                            pass
                    
                    # Get the spread vs spot
                    spread_vs_spot_raw = data_store.get_spread(exchange, symbol, 'vs_spot')
                    spread_vs_spot = f"{spread_vs_spot_raw:.6f}%" if isinstance(spread_vs_spot_raw, float) else 'N/A'
                    
                    # Get spreads vs other exchanges
                    spreads = {'binance': 'N/A', 'bybit': 'N/A', 'okx': 'N/A'}
                    spreads_raw = {'binance': float('nan'), 'bybit': float('nan'), 'okx': float('nan')}
                    
                    for other_exchange in ['binance', 'bybit', 'okx']:
                        if other_exchange == exchange:
                            continue
                            
                        # Get pre-calculated spread
                        spread_raw = data_store.get_spread(exchange, symbol, f'vs_{other_exchange}')
                        
                        # Format for display
                        if isinstance(spread_raw, float):
                            spreads[other_exchange] = f"{spread_raw:.6f}%"
                            spreads_raw[other_exchange] = spread_raw
                        else:
                            spreads[other_exchange] = 'N/A'
                            spreads_raw[other_exchange] = float('nan')
                    
                    # Get 24h change
                    change_24h = data_store.daily_changes.get(exchange, {}).get(symbol, 'N/A')
                    if change_24h != 'N/A':
                        change_24h = f"{change_24h:.2f}%"
                        
                    # Append to table data
                    table_data.append({
                        'symbol': symbol,
                        'exchange': exchange,
                        'bid': bid_price,
                        'ask': ask_price,                    
                        'funding_rate': funding_rate,
                        'spread_vs_spot': spread_vs_spot,
                        'spread_vs_binance': spreads['binance'],
                        'spread_vs_okx': spreads['okx'],
                        'spread_vs_bybit': spreads['bybit'],
                        'future_tick_size': future_tick_size if future_tick_size != 'N/A' else 'N/A',
                        'spot_tick_size': spot_tick_size if spot_tick_size != 'N/A' else 'N/A',
                        'change_24h': change_24h,
                        # Additional fields for sorting
                        'sort_funding_rate': self.extract_number(funding_rate),
                        'sort_spread_vs_spot': spread_vs_spot_raw if isinstance(spread_vs_spot_raw, float) else float('nan'),
                        'sort_spread_vs_binance': spreads_raw['binance'],
                        'sort_spread_vs_okx': spreads_raw['okx'],
                        'sort_spread_vs_bybit': spreads_raw['bybit'],
                        'sort_change_24h': self.extract_number(change_24h)
                    })
                    
        # Sort the data
        sort_mapping = {
            'Symbol': 'symbol',
            'Exchange': 'exchange',
            'Funding Rate': 'sort_funding_rate',
            'Spread vs Spot': 'sort_spread_vs_spot',
            'Spread vs Binance': 'sort_spread_vs_binance',
            'Spread vs OKX': 'sort_spread_vs_okx',
            'Spread vs Bybit': 'sort_spread_vs_bybit',
            '24h Change': 'sort_change_24h'
        }
        
        # Log sorting parameters for debugging
        logger.debug(f"Sorting by {sort_column} in {sort_direction} order")
        
        sort_key = sort_mapping.get(sort_column, 'symbol')
        reverse = (sort_direction == 'descending')
        
        # Sort with N/A values always at the bottom, regardless of sort direction
        table_data.sort(
            key=lambda x: (
                # First sorting key: 0 for normal values, 1 for N/A values
                # This ensures N/A always sorts after normal values
                1 if x[sort_key] == 'N/A' or (isinstance(x[sort_key], float) and math.isnan(x[sort_key])) else 0,
                # Second sorting key: the actual value for normal sorting
                # For N/A values, use a placeholder that won't affect the sort
                0 if x[sort_key] == 'N/A' or (isinstance(x[sort_key], float) and math.isnan(x[sort_key])) else
                (-x[sort_key] if reverse else x[sort_key])
            )
        )
        
        return table_data

    def update_data_table(self, force_upper=False, force_lower=False):
        """Update both data tables with rate limiting to prevent hangs"""
        # If both tables have mouse over and no force, don't update
        if (self.upper_mouse_over_table and not force_upper) and (self.lower_mouse_over_table and not force_lower):
            return
            
        # Rate limit updates - don't update too frequently unless forced
        current_time = time.time()
        if not (force_upper or force_lower) and hasattr(self, 'last_update_time') and current_time - self.last_update_time < 1.0:
            return  # Wait at least 1 second between updates
            
        self.last_update_time = current_time
        
        # Process this in a separate thread
        def prepare_data():
            try:
                # Get all symbols from all exchanges
                all_symbols = {}
                for exchange in ['binance', 'bybit', 'okx']:
                    symbols = data_store.get_symbols(exchange)
                    all_symbols[exchange] = set(symbols)
                    
                # Update upper table if not mouse over or forced
                if not self.upper_mouse_over_table or force_upper:
                    # Capture upper sort settings
                    upper_sort_column = self.upper_sort_column_var.get()
                    upper_sort_direction = self.upper_sort_direction_var.get()
                    
                    # Prepare upper table data
                    upper_table_data = self._prepare_table_data(all_symbols, upper_sort_column, upper_sort_direction)
                    
                    # Update UI in main thread
                    self.root.after(0, lambda: self._update_ui_with_data(self.upper_table, upper_table_data, "upper"))
                
                # Update lower table if not mouse over or forced
                if not self.lower_mouse_over_table or force_lower:
                    # Capture lower sort settings
                    lower_sort_column = self.lower_sort_column_var.get()
                    lower_sort_direction = self.lower_sort_direction_var.get()
                    
                    # Prepare lower table data
                    lower_table_data = self._prepare_table_data(all_symbols, lower_sort_column, lower_sort_direction)
                    
                    # Update UI in main thread
                    self.root.after(0, lambda: self._update_ui_with_data(self.lower_table, lower_table_data, "lower"))
                    
            except Exception as e:
                logger.error(f"Error preparing table data: {e}")
                
        # Use a background thread for data preparation
        self.executor.submit(prepare_data)

    def _update_ui_with_data(self, table, table_data, table_id):
        """Update the specified UI table with the prepared data"""
        try:
            self._optimize_table_update(table, table_data)
            
            # Update status
            status_text = f"Showing {len(table_data)} symbols in {table_id} table"
            if self.filter_var.get():
                status_text += f" (filtered by '{self.filter_var.get().upper()}')"
                
            logger.info(status_text)
        except Exception as e:
            logger.error(f"Error updating {table_id} table with data: {e}")

    def _optimize_table_update(self, table, new_data):
        """Rebuild the specified table completely to ensure correct sorting"""
        try:
            # Get current items for later cleanup
            current_items = set(table.get_children())
            
            # Clear the entire table to ensure proper order
            for item_id in current_items:
                table.delete(item_id)
            
            # Insert all items in the correct (sorted) order
            for i, item_data in enumerate(new_data):
                item_id = f"{item_data['exchange']}_{item_data['symbol']}"
                values = (
                    item_data['symbol'],
                    item_data['exchange'],
                    item_data['bid'],
                    item_data['ask'],
                    item_data['funding_rate'],
                    item_data['spread_vs_spot'],
                    item_data['spread_vs_binance'],
                    item_data['spread_vs_okx'],
                    item_data['spread_vs_bybit'],
                    item_data['future_tick_size'],
                    item_data['spot_tick_size'],
                    item_data['change_24h']
                )
                # Insert at the end to maintain sorted order
                table.insert('', 'end', iid=item_id, values=values,
                             tags=('even' if i % 2 == 0 else 'odd',))
        except Exception as e:
            logger.error(f"Error updating table: {e}")

    def extract_number(self, value):
        """Extract numeric value from formatted string for sorting"""
        if value == 'N/A':
            return float('nan')
            
        try:
            # Remove % sign and convert to float
            return float(value.replace('%', ''))
        except (ValueError, AttributeError):
            return float('nan')

    def start_exchange_threads(self):
        """Start data collection threads for the selected exchange"""
        selected_exchange = self.exchange_var.get()
        
        # Fetch symbols first for all exchanges
        if selected_exchange == "all" or selected_exchange == "binance":
            self.binance.fetch_symbols()
            self.binance.fetch_spot_symbols()
            
        if selected_exchange == "all" or selected_exchange == "bybit":
            # self.bybit.fetch_symbols()
            # self.bybit.fetch_spot_symbols()
            self.bybit.initialize()  # This will handle everything in the proper sequence
            
        if selected_exchange == "all" or selected_exchange == "okx":
            self.okx.fetch_symbols()
            self.okx.fetch_spot_symbols()
            
        # Short delay to ensure symbols are loaded
        time.sleep(1)
        
        # Now connect WebSockets and start other threads
        if selected_exchange == "all" or selected_exchange == "binance":
            # Start Binance threads
            self.binance.connect_futures_websocket()
            self.binance.connect_spot_websocket()
            active_threads["binance_funding"] = self.executor.submit(self.binance.update_funding_rates)
            active_threads["binance_changes"] = self.executor.submit(self.binance.update_24h_changes)
            threading.Timer(30, self.binance.check_symbol_freshness).start()        
            
        if selected_exchange == "all" or selected_exchange == "bybit":
            # Start Bybit threads
            self.bybit.connect_websocket()
            self.bybit.connect_spot_websocket()
            active_threads["bybit_funding"] = self.executor.submit(self.bybit.update_funding_rates)
            active_threads["bybit_changes"] = self.executor.submit(self.bybit.update_24h_changes)       
            
        if selected_exchange == "all" or selected_exchange == "okx":
            # Start OKX threads
            self.okx.connect_websocket()
            self.okx.connect_spot_websocket()
            active_threads["okx_funding"] = self.executor.submit(self.okx.update_funding_rates)
            active_threads["okx_changes"] = self.executor.submit(self.okx.update_24h_changes)
    def restart_exchange_threads(self):
        """Stop and restart exchange data threads with better shutdown"""
        logger.info("Restarting exchange threads...")
        
        # Stop all current threads
        global stop_event
        stop_event.set()
        
        # Shut down executor
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
            
        # Close all WebSocket connections
        for exchange in [self.binance, self.bybit, self.okx]:
            for name, manager in exchange.websocket_managers.items():
                if hasattr(manager, 'disconnect'):
                    manager.disconnect()
                    
        # Clear active threads dictionary
        active_threads.clear()
        
        # Reset stop event flag
        stop_event.clear()
        
        # Clear data caches to prevent stale data
        with data_store.lock:
            for exchange in data_store.price_data:
                data_store.price_data[exchange].clear()
                data_store.update_counters[exchange] = 0
                
        # Create a new executor
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)
        
        # Start new threads                                
        logger.info("Starting new exchange threads...")
        self.start_exchange_threads()

    def start_health_monitor(self):
        """Start a thread to monitor the health of connections"""
        def health_monitor_worker():
            while not stop_event.is_set():
                try:
                    # Check all WebSocket connections
                    for exchange in [self.binance, self.bybit, self.okx]:
                        for name, manager in exchange.websocket_managers.items():
                            if hasattr(manager, 'check_health'):
                                manager.check_health()
                                
                    # Log data freshness statistics occasionally
                    if random.random() < 0.05:  # Log 5% of the time
                        exchange_stats = {}
                        current_time = time.time()
                        
                        for exchange in ['binance', 'bybit', 'okx']:
                            fresh_count = 0
                            stale_count = 0
                            
                            with data_store.lock:
                                for symbol, data in data_store.price_data[exchange].items():
                                    if 'timestamp' in data:
                                        if current_time - data['timestamp'] < 30:
                                            fresh_count += 1
                                        else:
                                            stale_count += 1
                                            
                            exchange_stats[exchange] = f"{fresh_count} fresh, {stale_count} stale"
                            
                        logger.info(f"Data freshness: Binance: {exchange_stats['binance']}, "
                                  f"Bybit: {exchange_stats['bybit']}, OKX: {exchange_stats['okx']}")
                except Exception as e:
                    logger.error(f"Error in health monitor: {e}")
                    
                # Check every 5 seconds
                for _ in range(5):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
                    
        self.health_monitor = threading.Thread(
            target=health_monitor_worker,
            daemon=True,
            name="health_monitor"
        )
        self.health_monitor.start()
        active_threads["health_monitor"] = self.health_monitor

    def schedule_updates(self):
        """Schedule periodic UI updates and maintenance tasks"""
        # Clean old data periodically
        def clean_old_data():
            data_store.clean_old_data()
            self.root.after(300000, clean_old_data)  # 5 minutes
            
        # Update the data tables periodically
        def update_tables():
            self.update_data_table()
            self.root.after(500, update_tables)  # Update every 500ms
            
        # Start the scheduled functions
        clean_old_data()
        update_tables()



def run_spread_calculator():
    """Background thread to calculate spreads periodically"""
    while not stop_event.is_set():
        try:
            data_store.calculate_all_spreads()
        except Exception as e:
            logger.error(f"Error calculating spreads: {e}")
        
        # Calculate 5 times per second
        time.sleep(0.2)
#---------------------------------------------
# Main Application Entry Point
#---------------------------------------------
def main():


    # Set up proper signal handling
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        global stop_event
        stop_event.set()
        # Allow time for threads to clean up
        time.sleep(1)
        # Then destroy the root window
        if 'root' in globals():
            root.after(100, root.destroy)
            
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create main window
    root = tk.Tk()
    
    # Initialize application
    app = ExchangeMonitorApp(root)
    # Start the spread calculator thread
    spread_calculator_thread = threading.Thread(
        target=run_spread_calculator, 
        daemon=True,
        name="spread_calculator"
    )
    spread_calculator_thread.start()
    active_threads["spread_calculator"] = spread_calculator_thread


    def on_close():
        logger.info("Application shutting down...")
        global stop_event
        stop_event.set()
        
        # Start shutdown in a separate thread to keep UI responsive
        def background_shutdown():
            try:
                # Aggressively terminate WebSockets
                for exchange in [app.binance, app.bybit, app.okx]:
                    for name, manager in exchange.websocket_managers.items():
                        if isinstance(manager, dict):
                            # Handle dictionary-type manager
                            manager['is_running'] = False  # Prevent reconnection attempts
                            if 'ws' in manager and manager['ws']:
                                try:
                                    manager['ws'].close()
                                except:
                                    pass
                        else:
                            # Handle WebSocketManager object
                            if hasattr(manager, 'is_running'):
                                manager.is_running = False  # Prevent reconnection attempts
                            if hasattr(manager, 'ws') and manager.ws:
                                try:
                                    manager.ws.close()
                                except:
                                    pass
                                    
                # Shutdown executor without waiting
                if hasattr(app, 'executor'):
                    app.executor.shutdown(wait=False)
            except Exception as e:
                logger.error(f"Error in background shutdown: {e}")
                
        # Start background shutdown and destroy window immediately
        threading.Thread(target=background_shutdown, daemon=True).start()
        root.destroy()
        
    root.protocol("WM_DELETE_WINDOW", on_close)
    
    # Start main loop with exception handling
    try:
        root.mainloop()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        on_close()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        on_close()
    finally:
        # Force exit after 3 seconds if normal shutdown fails
        def force_exit():
            logger.warning("Application taking too long to close, forcing exit")
            os._exit(0)
            
        # Schedule force exit
        exit_timer = threading.Timer(3.0, force_exit)
        exit_timer.daemon = True
        exit_timer.start()
        
        logger.info("Application terminated")

if __name__ == "__main__":
    main()