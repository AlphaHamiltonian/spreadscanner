import threading
import concurrent.futures
import time
import logging
import orjson as json
from source.config import stop_event
import time
from source.exchanges.base import BaseExchangeConnector
from source.utils import data_store, WebSocketManager

logger = logging.getLogger(__name__) # module-specific logger


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
