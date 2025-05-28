import logging
import threading
import concurrent.futures
import time
from source.config import stop_event, active_threads
from source.utils import data_store
from source.exchanges.binance import BinanceConnector
from source.exchanges.bybit import BybitConnector
from source.exchanges.okx import OkxConnector

logger = logging.getLogger(__name__)

class HeadlessMonitor:
    """Headless version of the exchange monitor without UI dependencies."""
    def __init__(self):
        self.exchange = "all"  # Default to all exchanges
        self.websocket_managers = {}
        
        # Exchange connectors
        self.binance = BinanceConnector(self)
        self.bybit = BybitConnector(self)
        self.okx = OkxConnector(self)
        
        # Initialize thread pool
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)
        
        # Start health monitor
        self.start_health_monitor()
        
        logger.info("Headless monitor initialized")
        
    def start_exchange_threads(self, exchange_name="all"):
        """Start data collection threads for the specified exchange(s)"""
        self.exchange = exchange_name
        
        # Define a worker function to initialize an exchange connector in background
        def initialize_exchange(exchange_name, connector):
            try:
                logger.info(f"Initializing {exchange_name} connector in background thread")
                
                if exchange_name == "binance":
                    connector.fetch_symbols()
                    connector.fetch_spot_symbols()
                    connector.connect_futures_websocket()
                    connector.connect_spot_websocket()
                    active_threads[f"{exchange_name}_funding"] = self.executor.submit(connector.update_funding_rates)
                    active_threads[f"{exchange_name}_changes"] = self.executor.submit(connector.update_24h_changes)
                    threading.Timer(30, connector.check_symbol_freshness).start()
                elif exchange_name == "bybit":
                    # This already handles everything in the proper sequence
                    connector.initialize()
                elif exchange_name == "okx":
                    connector.fetch_symbols()
                    connector.fetch_spot_symbols()
                    connector.connect_websocket()
                    connector.connect_spot_websocket()
                    active_threads[f"{exchange_name}_funding"] = self.executor.submit(connector.update_funding_rates)
                    active_threads[f"{exchange_name}_changes"] = self.executor.submit(connector.update_24h_changes)
                
                logger.info(f"Completed initializing {exchange_name} connector")
            except Exception as e:
                logger.error(f"Error initializing {exchange_name} connector: {e}")
        
        # Start background threads for selected exchanges
        if exchange_name == "all" or exchange_name == "binance":
            threading.Thread(
                target=initialize_exchange,
                args=("binance", self.binance),
                daemon=True,
                name="binance_init_thread"
            ).start()
        
        if exchange_name == "all" or exchange_name == "bybit":
            threading.Thread(
                target=initialize_exchange,
                args=("bybit", self.bybit),
                daemon=True,
                name="bybit_init_thread"
            ).start()
            
        if exchange_name == "all" or exchange_name == "okx":
            threading.Thread(
                target=initialize_exchange,
                args=("okx", self.okx),
                daemon=True,
                name="okx_init_thread"
            ).start()

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
                    exchange_stats = {}
                    current_time = time.time()
                    
                    for exchange in ['binance', 'bybit', 'okx']:
                        fresh_count = 0
                        stale_count = 0
                        
                        # Use per-exchange read lock instead of global lock
                        with data_store.exchange_rw_locks[exchange]:
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
                    
                # Check every 30 seconds
                for _ in range(30):
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

    def shutdown(self):
        """Clean shutdown of all resources"""
        logger.info("Shutting down headless monitor...")
        
        # Shutdown executor
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
        
        # Close all WebSocket connections
        try:
            for exchange in [self.binance, self.bybit, self.okx]:
                for name, manager in exchange.websocket_managers.items():
                    if hasattr(manager, 'disconnect'):
                        manager.disconnect()
                    elif isinstance(manager, dict) and 'ws' in manager and manager['ws']:
                        try:
                            manager['ws'].close()
                        except:
                            pass
        except Exception as e:
            logger.error(f"Error closing connections: {e}")
            
        logger.info("Headless monitor shutdown complete")