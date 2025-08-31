import threading
import concurrent.futures
import time
import logging
import random
import orjson as json
import websocket
from source.core.config import stop_event
from source.exchanges.base import BaseExchangeConnector
from source.core.utils import data_store, WebSocketManager, WriteLock
from source.core.symbol_matcher import symbol_matcher

logger = logging.getLogger(__name__)

class HyperliquidConnector(BaseExchangeConnector):
    """Connects to Hyperliquid exchange API and WebSockets."""
    def __init__(self, app):
        super().__init__(app, "hyperliquid")
        self.rest_url = "https://api.hyperliquid.xyz"
        self.ws_url = "wss://api.hyperliquid.xyz/ws"
        self.reconnect_delay = 5  # Start with 5 seconds
        self.max_reconnect_delay = 120  # Max 2 minutes
        
    def check_symbol_freshness(self):
        """Check for stale symbols in Hyperliquid data and trigger reconnection if needed"""
        if stop_event.is_set():
            return
        try:
            current_time = time.time()
            with data_store.exchange_rw_locks['hyperliquid']:
                symbols = data_store.get_symbols('hyperliquid')
                stale_count = 0
                total_count = len(symbols)
                for symbol in symbols:
                    if symbol in data_store.price_data['hyperliquid']:
                        data = data_store.price_data['hyperliquid'][symbol]
                        if 'timestamp' not in data or current_time - data['timestamp'] > 60:
                            stale_count += 1
                            
                # If more than 20% of symbols are stale, reconnect
                if total_count > 0 and stale_count > total_count * 0.2:
                    logger.warning(f"Hyperliquid data freshness issue: {stale_count}/{total_count} symbols are stale")
                    
                    # Reconnect WebSockets
                    for name, manager in self.websocket_managers.items():
                        try:
                            logger.info(f"Forcing reconnection of {name}")
                            if isinstance(manager, WebSocketManager):
                                manager.disconnect()
                                time.sleep(1)
                                manager.connect()
                        except Exception as e:
                            logger.error(f"Error reconnecting {name}: {e}")
        except Exception as e:
            logger.error(f"Error checking Hyperliquid symbol freshness: {e}")
            
        # Schedule next check
        if not stop_event.is_set():
            threading.Timer(120, self.check_symbol_freshness).start()

    def create_better_hyperliquid_websocket(self, url, name, on_message, on_open=None):
        """Create a more reliable Hyperliquid WebSocket connection"""
        def enhanced_on_open(ws):
            logger.info(f"Hyperliquid {name} WebSocket connected")
            # Reset reconnect delay on successful connection
            self.reconnect_delay = 5
            if on_open:
                on_open(ws)
                
        def enhanced_on_ping(ws, message):
            """Immediately respond to ping frames"""
            logger.debug(f"Received ping from Hyperliquid {name}")
            if hasattr(ws, 'sock') and ws.sock:
                try:
                    ws.sock.pong(message)
                    logger.debug(f"Sent pong response to Hyperliquid {name}")
                except Exception as e:
                    logger.error(f"Error sending pong to Hyperliquid {name}: {e}")
                    
        def enhanced_on_close(ws, close_status_code, close_msg):
            logger.warning(f"Hyperliquid {name} WebSocket closed: {close_status_code} - {close_msg}")
            if not stop_event.is_set():
                # Use exponential backoff for reconnection
                delay = min(self.reconnect_delay, self.max_reconnect_delay)
                logger.info(f"Reconnecting Hyperliquid {name} in {delay} seconds")
                self.reconnect_delay = min(self.reconnect_delay * 2, self.max_reconnect_delay)
                threading.Timer(delay, lambda: reconnect_ws()).start()
                
        def enhanced_on_error(ws, error):
            logger.error(f"Hyperliquid {name} WebSocket error: {error}")
            
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
                
                thread = threading.Thread(
                    target=new_ws.run_forever,
                    kwargs={
                        'ping_interval': 180,
                        'ping_timeout': 60,
                        'sslopt': {"cert_reqs": 0},
                        'skip_utf8_validation': True
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
        
        if isinstance(ws, websocket.WebSocketApp):
            manager = WebSocketManager(
                url=url,
                name=name,
                on_message=on_message,
                on_open=enhanced_on_open,
                on_error=enhanced_on_error,
                on_close=enhanced_on_close,
                ping_interval=180,
                ping_timeout=60
            )
            manager.ws = ws
            manager.thread = thread
            self.websocket_managers[name] = manager
            return manager
            
        return None

    def fetch_symbols(self):
        """Fetch all tradable perpetual symbols from Hyperliquid"""
        try:
            response = self.session.post(
                f"{self.rest_url}/info",
                json={"type": "meta"}
            )
            if response.status_code == 200:
                data = response.json()
                with WriteLock(data_store.exchange_rw_locks['hyperliquid']):
                    data_store.symbols['hyperliquid'].clear()
                    if 'universe' in data:
                        for asset in data['universe']:
                            # Skip delisted or isolated-only symbols
                            if asset.get('isDelisted', False):
                                continue
                            coin_name = asset['name']
                            # Create standard format for compatibility with other exchanges
                            symbol = f"{coin_name}USDT"
                            data_store.symbols['hyperliquid'].add(symbol)
                            
                            # Don't store tick size - szDecimals is for position sizing, not price ticks
                            # Hyperliquid tick sizes will show as N/A
                                
                logger.info(f"Fetched {len(data_store.symbols['hyperliquid'])} Hyperliquid futures symbols")
                data_store.update_symbol_maps()
        except Exception as e:
            logger.error(f"Error fetching Hyperliquid symbols: {e}")

    def fetch_spot_symbols(self):
        """Fetch spot market information - Hyperliquid has independent spot markets for different tokens"""
        try:
            response = self.session.post(
                f"{self.rest_url}/info",
                json={"type": "spotMeta"}
            )
            if response.status_code == 200:
                data = response.json()
                
                spot_to_perp_map = {}
                spot_count = 0
                
                with WriteLock(data_store.exchange_rw_locks['hyperliquid']):
                    if 'universe' in data:
                        for asset in data['universe'][:50]:  # Limit to first 50 spot markets
                            pair_name = asset['name']
                            
                            # Only process pairs with '/' (actual trading pairs)
                            if '/' in pair_name:
                                base, quote = pair_name.split('/')
                                base_upper = base.upper()
                                
                                # Create a standardized symbol for tracking
                                # Note: These are different tokens, not the same as perps
                                tracking_symbol = f"{base_upper}USDT"
                                
                                # Add to spot mapping
                                spot_to_perp_map[pair_name] = tracking_symbol
                                spot_count += 1
                                
                                # Don't store tick size - szDecimals is for position sizing, not price ticks
                                # Hyperliquid tick sizes will show as N/A
                            
                data_store.hyperliquid_spot_to_future_map = spot_to_perp_map
                logger.info(f"Found {spot_count} Hyperliquid spot markets (tokens like PURR, etc.)")
                
                # Note: Most of these won't have corresponding perpetuals
                logger.info("Note: Hyperliquid spot markets are for different tokens than perpetuals")
        except Exception as e:
            logger.error(f"Error fetching Hyperliquid spot symbols: {e}")

    def connect_futures_websocket(self):
        """Connect to Hyperliquid Futures WebSocket"""
        symbols = list(data_store.get_symbols('hyperliquid'))
        if not symbols:
            logger.error("No Hyperliquid symbols to subscribe")
            return
            
        # Create subscription list for all coins
        coins = [s.replace('USDT', '') for s in symbols]
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle subscription responses
                if 'channel' in data and data['channel'] == 'subscriptionResponse':
                    logger.debug(f"Hyperliquid subscription response: {data}")
                    return
                    
                # Handle all mids updates (most efficient for multiple symbols)
                if 'channel' in data and data['channel'] == 'allMids':
                    if 'data' in data and 'mids' in data['data']:
                        mids = data['data']['mids']
                        for coin, mid_price in mids.items():
                            symbol = f"{coin}USDT"
                            if symbol in symbols and mid_price:
                                # Use mid price to estimate bid/ask with small spread
                                mid = float(mid_price)
                                # Assume 0.01% spread for bid/ask estimation
                                spread = mid * 0.0001
                                bid = mid - spread
                                ask = mid + spread
                                
                                data_store.update_price_direct(
                                    'hyperliquid', symbol, bid, ask
                                )
                                
                # Handle L2 book updates for more accurate bid/ask
                if 'channel' in data and 'l2Book' in data['channel']:
                    if 'data' in data:
                        book_data = data['data']
                        # Extract coin from channel name (format: "l2Book:BTC")
                        parts = data['channel'].split(':')
                        if len(parts) > 1:
                            coin = parts[1]
                            symbol = f"{coin}USDT"
                            
                            if 'levels' in book_data:
                                levels = book_data['levels']
                                # Levels format: [[[price, size, count], ...], [[price, size, count], ...]]
                                # First array is bids, second is asks
                                if len(levels) >= 2 and levels[0] and levels[1]:
                                    # Get best bid and ask
                                    best_bid = float(levels[0][0][0]) if levels[0][0] else 0
                                    best_ask = float(levels[1][0][0]) if levels[1][0] else 0
                                    bid_qty = float(levels[0][0][1]) if levels[0][0] else 0
                                    ask_qty = float(levels[1][0][1]) if levels[1][0] else 0
                                    
                                    if best_bid > 0 and best_ask > 0:
                                        data_store.update_price_direct(
                                            'hyperliquid', symbol, best_bid, best_ask,
                                            bid_qty=bid_qty, ask_qty=ask_qty
                                        )
                                
            except Exception as e:
                logger.error(f"Error processing Hyperliquid futures data: {e}")
                
        def on_open(ws):
            logger.info(f"Hyperliquid futures WebSocket connected")
            
            # Subscribe to allMids for efficient updates
            subscribe_msg = json.dumps({
                "method": "subscribe",
                "subscription": {"type": "allMids"}
            })
            ws.send(subscribe_msg)
            logger.info("Subscribed to Hyperliquid allMids feed")
                
        # Use the enhanced WebSocket creation method
        manager = self.create_better_hyperliquid_websocket(
            url=self.ws_url,
            name="hyperliquid_futures",
            on_message=on_message,
            on_open=on_open
        )

    def connect_spot_websocket(self):
        """Connect to Hyperliquid spot WebSocket - Note: Hyperliquid spot is for different tokens"""
        # Get the actual spot pairs from the mapping
        if not hasattr(data_store, 'hyperliquid_spot_to_future_map'):
            logger.info("No Hyperliquid spot markets found - this is normal for Hyperliquid")
            return
            
        spot_pairs = data_store.hyperliquid_spot_to_future_map
        if not spot_pairs:
            logger.info("No Hyperliquid spot pairs available")
            return
        
        logger.info(f"Preparing spot WebSocket for {len(spot_pairs)} Hyperliquid-specific tokens")
        
        def on_message(ws, message):
            try:
                data = json.loads(message)
                
                # Handle L2 book updates for spot pairs
                if 'channel' in data and 'l2Book' in data['channel']:
                    # Parse the channel to get the coin
                    parts = data['channel'].split(':')
                    if len(parts) >= 2:
                        coin = parts[1]
                        
                        # Check if this is a spot pair we're tracking
                        for spot_pair, perp_symbol in spot_pairs.items():
                            if coin.upper() in spot_pair.upper():
                                spot_key = f"{perp_symbol}_SPOT"
                                
                                if 'data' in data:
                                    book_data = data['data']
                                    if 'levels' in book_data:
                                        levels = book_data['levels']
                                        # Get best bid and ask
                                        if len(levels) >= 2 and levels[0] and levels[1]:
                                            bids = levels[0]  # [[price, size, count], ...]
                                            asks = levels[1]
                                            
                                            if bids and asks:
                                                best_bid = float(bids[0][0]) if bids[0] else 0
                                                best_ask = float(asks[0][0]) if asks[0] else 0
                                                
                                                if best_bid > 0 and best_ask > 0:
                                                    last = (best_bid + best_ask) / 2
                                                    data_store.update_price_direct(
                                                        'hyperliquid', spot_key, best_bid, best_ask, last=last
                                                    )
                                                    logger.debug(f"Updated Hyperliquid spot: {spot_key} bid={best_bid:.4f} ask={best_ask:.4f}")
                                break
                                
            except Exception as e:
                logger.error(f"Error processing Hyperliquid spot message: {e}")
        
        def on_open(ws):
            logger.info(f"Hyperliquid spot WebSocket connected")
            
            # Subscribe to spot orderbook data for each pair
            for base, future_symbol in spot_pairs[:20]:  # Limit to first 20 to avoid rate limits
                # Subscribe to spot L2 book for this coin
                subscribe_msg = json.dumps({
                    "method": "subscribe",
                    "subscription": {
                        "type": "spotClearinghouseState",
                        "user": "0x0000000000000000000000000000000000000000"  # Public data
                    }
                })
                ws.send(subscribe_msg)
                break  # Only need one subscription for all spot data
                
            # Also try subscribing to spot orderbooks individually
            for base, future_symbol in spot_pairs[:10]:  # Limit subscriptions
                subscribe_msg = json.dumps({
                    "method": "subscribe", 
                    "subscription": {
                        "type": "l2Book",
                        "coin": f"{base}:USDC"  # Spot format might be BTC:USDC
                    }
                })
                ws.send(subscribe_msg)
                time.sleep(0.1)  # Small delay between subscriptions
            
        # Create WebSocket for spot
        manager = self.create_better_hyperliquid_websocket(
            url=self.ws_url,
            name="hyperliquid_spot",
            on_message=on_message,
            on_open=on_open
        )

    def _fetch_funding_batch(self, symbols_batch):
        """Fetch funding rates for a batch of symbols"""
        try:
            for symbol in symbols_batch:
                coin = symbol.replace('USDT', '')
                url = f"{self.rest_url}/info"
                response = self.session.post(
                    url,
                    json={"type": "fundingHistory", "coin": coin}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        # Get the most recent funding rate
                        latest = data[0]
                        rate = float(latest.get('fundingRate', 0)) * 100
                        formatted_rate = f"{rate:.4f}%"
                        data_store.update_funding_rate('hyperliquid', symbol, formatted_rate)
                        
                time.sleep(0.5)  # Increased delay to avoid rate limits
        except Exception as e:
            logger.error(f"Error fetching funding batch: {e}")

    def update_funding_rates(self):
        """Fetch funding rates from Hyperliquid API - DISABLED due to rate limits"""
        # Hyperliquid rate limits are too aggressive, disabling funding rate updates
        logger.info("Hyperliquid funding rate updates disabled due to rate limits")
        while not stop_event.is_set():
            time.sleep(60)  # Just sleep

    def _fetch_changes_batch(self, symbols_batch):
        """Fetch 24h changes for a batch of symbols"""
        try:
            for symbol in symbols_batch:
                coin = symbol.replace('USDT', '')
                url = f"{self.rest_url}/info"
                
                # Get recent candle data to calculate 24h change
                response = self.session.post(
                    url,
                    json={
                        "type": "candles",
                        "coin": coin,
                        "interval": "1d",
                        "limit": 2
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) >= 2:
                        # Calculate 24h change from daily candles
                        prev_close = float(data[1]['c'])
                        current = float(data[0]['c'])
                        
                        if prev_close > 0:
                            change_percent = ((current - prev_close) / prev_close) * 100
                            with WriteLock(data_store.exchange_rw_locks['hyperliquid']):
                                data_store.daily_changes['hyperliquid'][symbol] = change_percent
                                
                time.sleep(0.5)  # Increased delay to avoid rate limits
        except Exception as e:
            logger.error(f"Error fetching 24h changes batch: {e}")

    def update_24h_changes(self):
        """Fetch 24-hour price changes for symbols - DISABLED due to rate limits"""
        # Hyperliquid rate limits are too aggressive, disabling 24h change updates
        logger.info("Hyperliquid 24h change updates disabled due to rate limits")
        while not stop_event.is_set():
            time.sleep(60)  # Just sleep