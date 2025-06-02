"""
Centralized symbol matching and normalization across exchanges.
Consolidates all symbol mapping logic in one place.
"""
import logging
from typing import Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)

class SymbolMatcher:
    """Centralized symbol matching and normalization across exchanges"""
    
    def __init__(self):
        # Cache for normalized symbols
        self._normalized_cache = {}
        # Cache for equivalent symbols across exchanges
        self._equivalence_cache = {}
        # Exchange-specific symbol mappings
        self._exchange_mappings = {
            'binance': {},  # Will store spot->future mappings
            'bybit': {},    # Will store spot->future mappings  
            'okx': {}       # Will store spot->future mappings
        }
        
    def normalize_symbol(self, exchange: str, symbol: str) -> str:
        """
        Normalize symbols for cross-exchange comparison.
        Returns a standardized format that can be compared across exchanges.
        """
        cache_key = f"{exchange}:{symbol}"
        
        if cache_key in self._normalized_cache:
            return self._normalized_cache[cache_key]
            
        # Normalization rules per exchange
        normalized = symbol
        
        if exchange == 'okx':
            # OKX: BTC-USDT-SWAP -> BTCUSDT
            if '-SWAP' in symbol:
                normalized = symbol.replace('-SWAP', '').replace('-', '')
            elif '-' in symbol:
                normalized = symbol.replace('-', '')
                
        elif exchange == 'bybit':
            # Bybit: BTC-USDT -> BTCUSDT (for both linear and spot)
            if '-' in symbol:
                normalized = symbol.replace('-', '')
                
        elif exchange == 'binance':
            # Binance symbols are already in BTCUSDT format
            normalized = symbol
            
        # Cache and return
        self._normalized_cache[cache_key] = normalized
        return normalized
        
    def find_equivalent_symbol(self, source_exchange: str, source_symbol: str, 
                             target_exchange: str, target_symbols: Set[str] = None) -> Optional[str]:
        """
        Find equivalent symbol in target exchange.
        Uses cached results for performance.
        
        Args:
            source_exchange: Source exchange name
            source_symbol: Symbol from source exchange
            target_exchange: Target exchange name  
            target_symbols: Set of symbols in target exchange (optional, will fetch if not provided)
        """
        cache_key = (source_exchange, source_symbol, target_exchange)
        
        if cache_key in self._equivalence_cache:
            return self._equivalence_cache[cache_key]
            
        # Get target symbols if not provided (for backward compatibility)
        if target_symbols is None:
            # Import here to avoid circular imports
            from source.utils import data_store
            with data_store.exchange_locks[target_exchange]:
                target_symbols = data_store.symbols[target_exchange]
            
        # Get normalized version of source symbol
        normalized_source = self.normalize_symbol(source_exchange, source_symbol)
        
        # Look for matching symbol in target exchange
        for target_symbol in target_symbols:
            normalized_target = self.normalize_symbol(target_exchange, target_symbol)
            if normalized_source == normalized_target:
                self._equivalence_cache[cache_key] = target_symbol
                return target_symbol
                
        # No match found - cache the miss
        self._equivalence_cache[cache_key] = None
        return None
        
    def create_spot_future_mapping(self, exchange: str, futures_symbols: Set[str], 
                                 spot_symbols: Set[str]) -> Dict[str, str]:
        """
        Create mapping from spot symbols to their corresponding futures.
        Returns dict of {spot_symbol: future_symbol}
        """
        mapping = {}
        
        if exchange == 'binance':
            # Binance: Direct mapping for symbols that exist in both
            for spot_symbol in spot_symbols:
                if spot_symbol in futures_symbols:
                    mapping[spot_symbol] = spot_symbol
                    
        elif exchange == 'bybit':
            # Bybit: Map USDT pairs only, handle different formats
            for spot_symbol in spot_symbols:
                # Only consider USDT pairs
                if not (spot_symbol.endswith('USDT') or spot_symbol.endswith('-USDT')):
                    continue
                    
                # Normalize spot symbol
                normalized_spot = self.normalize_symbol(exchange, spot_symbol)
                
                # Look for corresponding linear future (USDT-margined)
                for future_symbol in futures_symbols:
                    if future_symbol.endswith('USDT'):  # Linear futures only
                        normalized_future = self.normalize_symbol(exchange, future_symbol)
                        if normalized_spot == normalized_future:
                            mapping[spot_symbol] = future_symbol
                            break
                            
        elif exchange == 'okx':
            # OKX: Map spot to swap pairs
            for future_symbol in futures_symbols:
                if '-SWAP' in future_symbol:
                    # Convert BTC-USDT-SWAP to BTC-USDT for spot lookup
                    parts = future_symbol.split('-')
                    if len(parts) == 3:  # BTC-USDT-SWAP format
                        base, quote = parts[0], parts[1]
                        spot_symbol = f"{base}-{quote}"
                        if spot_symbol in spot_symbols:
                            mapping[spot_symbol] = future_symbol
                            
        # Cache the mapping
        self._exchange_mappings[exchange] = mapping
        logger.info(f"Created {len(mapping)} spot->future mappings for {exchange}")
        return mapping
        
    def get_spot_key(self, exchange: str, future_symbol: str) -> str:
        """
        Generate the spot key for a given future symbol.
        This is used internally for data storage.
        """
        return f"{future_symbol}_SPOT"
        
    def get_base_quote_from_symbol(self, exchange: str, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract base and quote currencies from a symbol.
        Returns (base, quote) tuple or (None, None) if parsing fails.
        """
        try:
            if exchange == 'okx':
                if '-' in symbol:
                    parts = symbol.replace('-SWAP', '').split('-')
                    if len(parts) >= 2:
                        return parts[0], parts[1]
                        
            elif exchange == 'bybit':
                if '-' in symbol:
                    parts = symbol.split('-')
                    if len(parts) >= 2:
                        return parts[0], parts[1]
                else:
                    # Handle BTCUSDT format - assume USDT quote
                    if symbol.endswith('USDT'):
                        return symbol[:-4], 'USDT'
                        
            elif exchange == 'binance':
                # Handle BTCUSDT format - assume USDT quote for most cases
                if symbol.endswith('USDT'):
                    return symbol[:-4], 'USDT'
                elif symbol.endswith('BTC'):
                    return symbol[:-3], 'BTC'
                elif symbol.endswith('ETH'):
                    return symbol[:-3], 'ETH'
                    
        except Exception as e:
            logger.debug(f"Failed to parse symbol {symbol} for {exchange}: {e}")
            
        return None, None
        
    def clear_cache(self):
        """Clear all caches - useful when symbol lists change"""
        self._normalized_cache.clear()
        self._equivalence_cache.clear()
        self._exchange_mappings = {k: {} for k in self._exchange_mappings.keys()}
        
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics for monitoring"""
        return {
            'normalized_cache_size': len(self._normalized_cache),
            'equivalence_cache_size': len(self._equivalence_cache),
            'binance_mappings': len(self._exchange_mappings['binance']),
            'bybit_mappings': len(self._exchange_mappings['bybit']),
            'okx_mappings': len(self._exchange_mappings['okx'])
        }

# Create global instance after class definition
symbol_matcher = SymbolMatcher()