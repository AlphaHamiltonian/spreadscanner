import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import time

logger = logging.getLogger(__name__)

class ConfigGenerator:
    """Config generator using modular JSON templates with strategy parameters"""
    
    def __init__(self, output_dir: str = "./trading_configs", template_dir: str = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.template_dir = Path(template_dir) if template_dir else Path(__file__).parent / "config_templates"
        self.counter = 0
        self._templates_cache = {}
        
        # Account and platform mappings
        self.accounts = {
            'binance': {'spot': 'bin_s10', 'futures': 'bin_h10'},
            'bybit': {'spot': 'bybit_spot_01', 'futures': 'bybit_01'},
            'okx': {'spot': 'okx_spot_01', 'futures': 'okx_01'}
        }
        
        self.platforms = {
            'binance': {'spot': 'BIN_CMAG', 'futures': 'BIN_FUT'},
            'bybit': {'spot': 'BYBIT_SPOT', 'futures': 'BYBIT_LIN'},
            'okx': {'spot': 'OKX_SPOT', 'futures': 'OKX_SWAP'}
        }
        
        # Load strategy config
        self._load_strategy_config()
    
    def _load_strategy_config(self):
        """Load strategy parameters from config file or use defaults"""
        config_file = Path(__file__).parent / "strategy_config.json"
        
        if config_file.exists():
            try:
                with open(config_file, 'r') as f:
                    data = json.load(f)
                self.strategy_params = data.get('strategy_parameters', {})
                self.profiles = data.get('custom_profiles', {})
                logger.info(f"Loaded strategy config with {len(self.profiles)} profiles")
                return
            except Exception as e:
                logger.warning(f"Error loading strategy config: {e}")
        
        # Default parameters if no config file
        self.strategy_params = {
            'spread': {
                'SC': {'offset_bid': '0', 'offset_ask': '0', 'bid_qty': '0', 'ask_qty': '0'},
                'MM': {'offset_bid': '2', 'offset_ask': '2', 'bid_qty': '10', 'ask_qty': '10'}
            },
            'momentum': {
                'SC': {'offset_bid': '0', 'offset_ask': '0', 'bid_qty': '0', 'ask_qty': '0'},
                'MM': {'offset_bid': '5', 'offset_ask': '5', 'bid_qty': '20', 'ask_qty': '20'}
            }
        }
        self.profiles = {}
    
    def get_params(self, strategy_type: str, trade_strategy: str, 
                   custom: Dict = None, profile: str = None) -> Dict[str, str]:
        """Get merged parameters: defaults -> profile -> custom"""
        # Start with strategy defaults
        params = self.strategy_params.get(strategy_type, {}).get(trade_strategy.upper(), {}).copy()
        
        # Apply profile if specified
        if profile and profile in self.profiles:
            profile_params = {k: v for k, v in self.profiles[profile].items() if k != 'description'}
            params.update(profile_params)
        
        # Apply custom overrides
        if custom:
            params.update(custom)
        
        return params
    
    def load_template(self, path: str) -> Dict[str, Any]:
        """Load and cache JSON template"""
        if path not in self._templates_cache:
            with open(self.template_dir / path, 'r') as f:
                self._templates_cache[path] = json.load(f)
        return json.loads(json.dumps(self._templates_cache[path]))
    
    def fill_template(self, template: Any, values: Dict[str, Any]) -> Any:
        """Fill template with values and resolve @references"""
        if isinstance(template, dict):
            return {k: self.fill_template(v, values) for k, v in template.items()}
        elif isinstance(template, list):
            return [self.fill_template(item, values) for item in template]
        elif isinstance(template, str):
            # Replace placeholders
            if '{' in template and '}' in template:
                for key, value in values.items():
                    template = template.replace(f"{{{key}}}", str(value))
            # Resolve references
            if template.startswith('@'):
                return self.fill_template(self.load_template(template[1:]), values)
        return template
    
    def format_symbol(self, symbol: str, exchange: str) -> str:
        """Format symbol for exchange"""
        if exchange.lower() == 'binance':
            return symbol.replace('_SPOT', '').replace('-SWAP', '').replace('-', '')
        return symbol

    def generate_spread_configs(self, source1: str, source2: str, exchange1: str, exchange2: str,
                            spread_pct: float = None, strategy: str = 'SC',
                            custom: Dict = None, profile: str = None) -> Tuple[Dict, Dict]:
        """Generate spread trading configs"""
        # Parse sources
        _, symbol1 = source1.split(':', 1)
        _, symbol2 = source2.split(':', 1)
        
        trade_sym, trade_ex = symbol1, exchange1
        hedge_sym, hedge_ex = symbol2, exchange2
        
        # Get parameters and base values
        params = self.get_params('spread', strategy, custom, profile)
        base = self.load_template("base.json")
        
        # Helper function to get short exchange name
        def get_short_name(exchange_name):
            parts = exchange_name.split('_')
            if len(parts) == 2:
                return parts[1]  # Return second part (SPOT, FUTURES, LINEAR, SWAP)
            return exchange_name  # Return as-is if not 2 parts
        
        common = {
            'theo_type': 'FAST',
            'trade_strategy': strategy.upper(),
            'hedge_strategy': 'HLimit',
            'theo_config_file': 'fast_spread.json',
            'trade_config_file': 'sc.json' if strategy.upper() == 'SC' else 'mm.json',
            'hedge_config_file': 'hlimit.json',
            'currency_symbol': '',
            'currency_exchange': '',
            'hedge_currency': 'no',
            'min_size_currency': '6u',
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'spread_pct': spread_pct or 0,
            'counter': self.counter,
            **params  # Include offset/qty parameters
        }
        # Config 1: First symbol is trade, second is hedge
        config1 = self.fill_template(base, {
            **common,
            'id': int(time.time() * 1000) % 100000,
            'trade_account': self.accounts[trade_ex]['spot' if '_SPOT' in trade_sym else 'futures'],
            'hedge_account': self.accounts[hedge_ex]['spot' if '_SPOT' in hedge_sym else 'futures'],
            'theo_comment': f"Trade {trade_sym} ({trade_ex}) hedge {hedge_sym} ({hedge_ex})",
            'asset_symbol': self.format_symbol(hedge_sym, hedge_ex),
            'asset_exchange': self.platforms[hedge_ex]['spot' if '_SPOT' in hedge_sym else 'futures'],
            'asset_exchange_short': get_short_name(self.platforms[hedge_ex]['spot' if '_SPOT' in hedge_sym else 'futures']),
            'trade_symbol': self.format_symbol(trade_sym, trade_ex),
            'trade_exchange': self.platforms[trade_ex]['spot' if '_SPOT' in trade_sym else 'futures'],
            'trade_exchange_short': get_short_name(self.platforms[trade_ex]['spot' if '_SPOT' in trade_sym else 'futures']),
            'strategy_type': 'spread_trade'
        })

        
        self.counter += 1
        return config1


    def save_config(self, config: Dict, suffix: str = "") -> Path:
        """Save config to file"""
        # Extract the config names from the nested configs
        theo_config_name = config.get("theo_config", {}).get("configName", "unknown")
        trade_config_name = config.get("trade_config", {}).get("configName", "unknown")
        
        # Clean the names for use in filename
        theo_clean = theo_config_name.replace(' ', '_').replace('-', '_').replace(':', '_')
        trade_clean = trade_config_name.replace(' ', '_').replace('-', '_').replace(':', '_')
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        # Combine theo and trade config names for the filename
        filename = f"{theo_clean}_{trade_clean}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Saved: {filepath.name}")
        return filepath
    
    def generate_momentum_config(self, source: str, exchange: str, momentum_pct: float = None,
                            strategy: str = 'SC', custom: Dict = None, profile: str = None) -> Dict:
        """Generate momentum trading config"""
        _, symbol = source.split(':', 1)
        is_spot = '_SPOT' in symbol
        
        params = self.get_params('momentum', strategy, custom, profile)
        base = self.load_template("base.json")
        account = self.accounts[exchange]['spot' if is_spot else 'futures']
        platform = self.platforms[exchange]['spot' if is_spot else 'futures']
        
        # Helper function to get short exchange name
        def get_short_name(exchange_name):
            parts = exchange_name.split('_')
            if len(parts) == 2:
                return parts[1]  # Return second part (SPOT, FUTURES, LINEAR, SWAP)
            return exchange_name  # Return as-is if not 2 parts
        
        config = self.fill_template(base, {
            'id': int(time.time() * 1000) % 100000,
            'theo_type': 'FAST',
            'trade_strategy': strategy.upper(),
            'hedge_strategy': 'HMomentum',
            'trade_account': account,
            'hedge_account': account,
            'theo_config_file': 'fast_momentum.json',
            'trade_config_file': 'sc.json' if strategy.upper() == 'SC' else 'mm.json',
            'hedge_config_file': 'hmomentum.json',
            'symbol': self.format_symbol(symbol, exchange),
            'exchange': platform,
            'exchange_short': get_short_name(platform),
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'strategy_type': 'momentum',
            'spread_pct': momentum_pct or 0,
            'counter': self.counter,
            **params
        })
        
        self.counter += 1
        return config
# API Functions
def create_spread_configs(source1: str, source2: str, exchange1: str, exchange2: str,
                         spread_pct: float = None, strategy: str = 'SC',
                         custom: Dict = None, profile: str = None) -> Tuple[Path, Path]:
    """Create spread configs with optional custom parameters or profile"""
    gen = ConfigGenerator()
    config1 = gen.generate_spread_configs(
        source1, source2, exchange1, exchange2, spread_pct, strategy, custom, profile
    )
    return gen.save_config(config1, "_SPOT")

def create_momentum_config(source: str, exchange: str, momentum_pct: float = None,
                          strategy: str = 'SC', custom: Dict = None, profile: str = None) -> Path:
    """Create momentum config with optional custom parameters or profile"""
    gen = ConfigGenerator()
    config = gen.generate_momentum_config(source, exchange, momentum_pct, strategy, custom, profile)
    return gen.save_config(config, "_MOM")

def generate_spread_configs_direct(source1: str, source2: str, exchange1: str, exchange2: str,
                                 spread_pct: float = None, strategy: str = 'SC',
                                 custom: Dict = None, profile: str = None) -> Tuple[Dict, Dict]:
    """Generate spread configs and return them directly without saving to disk"""
    gen = ConfigGenerator()
    config1 = gen.generate_spread_configs(
        source1, source2, exchange1, exchange2, spread_pct, strategy, custom, profile
    )
    # Return the actual config dictionaries, not file paths
    return config1

# Testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    # Test examples
    print("=== Testing Config Generation ===\n")
    
    # 1. MM with custom parameters
    p1 = create_spread_configs(
        "binance:ETHUSDT", "binance:ETHUSDT_SPOT", "binance", "binance", 1.2,
        strategy='MM'
    )
    print(f"✓ MM Custom: {p1.name}")
    
    # 3. Using profile (if strategy_config.json exists)

    p5 = create_momentum_config(
        "binance:ETHUSDT", "binance", 2.5, strategy='SC', profile='takeout'
    )
    print(f"✓ Profile: {p5.name}")

    
    print("\n✅ Complete!")