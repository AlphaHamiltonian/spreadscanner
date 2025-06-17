# source/scanners/spread_calculator.py
"""
Spread calculation module for monitoring price spreads across exchanges and instrument types.
Extracted from DataStore for better code organization.
"""
import time
import logging
import math
import random
from source.core.config import Config
from source.actions.alerts import alert_manager
from source.core.symbol_matcher import symbol_matcher

logger = logging.getLogger(__name__)

class SpreadCalculator:
    """Handles spread calculations for the data store"""
    
    def __init__(self, data_store):
        """Initialize with reference to data store"""
        self.data_store = data_store
        # This cache was originally in DataStore.symbol_equivalence_map
        self.symbol_equivalence_map = {}
        
    def calculate_all_spreads(self):
        """Calculate spreads only for symbols that have been updated"""
        current_time = time.time()
        # Add this early exit
        if current_time - self.data_store.spread_timestamp < 0.5:  # Skip if calculated < 500ms ago
            return
        # Get dirty symbols and clear the set
        with self.data_store.dirty_symbols_lock:
            if not self.data_store.dirty_symbols:
                return  # Nothing to update
            symbols_to_process = self.data_store.dirty_symbols.copy()
            self.data_store.dirty_symbols.clear()

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
                    equiv = symbol_matcher.find_equivalent_symbol(exchange, symbol, other)
                    if equiv:
                        if other not in required_symbols:
                            required_symbols[other] = set()
                        required_symbols[other].add(equiv)

        # CHANGE 2: Only snapshot the symbols we actually need
        exchange_snapshots = {}
        for exchange, needed_symbols in required_symbols.items():
            exchange_snapshots[exchange] = {}
            
            for symbol in needed_symbols:
                symbol_lock = self.data_store.symbol_locks.get_lock(exchange, symbol)
                with symbol_lock:  # Read lock
                    symbol_data = self.data_store.price_data[exchange].get(symbol, {})
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
            spreads_for_exch = self.data_store.spreads.setdefault(exchange, {})

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

        self.data_store.spread_timestamp = current_time

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
            if random.random() < 0.00001:  # Log less of occurrences
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
        alert_manager.check_spread_alert(spread_pct, source1, source2, exchange1, exchange2,bid1,ask1,bid2,ask2)
        return spread_pct

__all__ = ['SpreadCalculator']