import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import time

logger = logging.getLogger(__name__)

class ConfigGenerator:
    """Clean config generator using modular JSON templates"""
    
    def __init__(self, output_dir: str = "./trading_configs", template_dir: str = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Set template directory
        if template_dir:
            self.template_dir = Path(template_dir)
        else:
            # Default to source/config_templates
            self.template_dir = Path(__file__).parent / "config_templates"
        
        self.counter = 0
        self._templates_cache = {}
        
        # Account and platform mappings
        self.accounts = {
            'binance': {'spot': 'bin_spot_10', 'futures': 'bin_10'},
            'bybit': {'spot': 'bybit_spot_01', 'futures': 'bybit_01'},
            'okx': {'spot': 'okx_spot_01', 'futures': 'okx_01'}
        }
        
        self.platforms = {
            'binance': {'spot': 'BIN_SPOT', 'futures': 'BIN_FUTURES'},
            'bybit': {'spot': 'BYBIT_SPOT', 'futures': 'BYBIT_LINEAR'},
            'okx': {'spot': 'OKX_SPOT', 'futures': 'OKX_SWAP'}
        }
    
    def load_template(self, template_path: str) -> Dict[str, Any]:
        """Load JSON template from file"""
        if template_path not in self._templates_cache:
            full_path = self.template_dir / template_path
            with open(full_path, 'r') as f:
                template = json.load(f)
            self._templates_cache[template_path] = template
        # Return a deep copy to avoid mutation
        return json.loads(json.dumps(self._templates_cache[template_path]))
    
    def fill_template(self, template: Any, values: Dict[str, Any]) -> Any:
        """Recursively fill template placeholders and resolve @references"""
        if isinstance(template, dict):
            return {k: self.fill_template(v, values) for k, v in template.items()}
        elif isinstance(template, list):
            return [self.fill_template(item, values) for item in template]
        elif isinstance(template, str):
            # Replace placeholders
            if '{' in template and '}' in template:
                result = template
                for key, value in values.items():
                    placeholder = f"{{{key}}}"
                    if placeholder in result:
                        result = result.replace(placeholder, str(value))
                template = result
            
            # Handle template references
            if template.startswith('@'):
                ref_path = template[1:]  # Remove @ prefix
                referenced_template = self.load_template(ref_path)
                return self.fill_template(referenced_template, values)
                
        return template
    
    def format_symbol(self, symbol: str, exchange: str) -> str:
        """Format symbol for exchange requirements"""
        if exchange.lower() == 'binance':
            # Clean format for Binance
            return symbol.replace('_SPOT', '').replace('-SWAP', '').replace('-', '')
        return symbol
    
    def parse_source(self, source: str) -> Tuple[str, str]:
        """Parse 'exchange:symbol' format"""
        exchange, symbol = source.split(':', 1)
        return exchange.lower(), symbol
    
    def generate_spread_configs(self, source1: str, source2: str, 
                              exchange1: str, exchange2: str,
                              spread_pct: float = None) -> Tuple[Dict, Dict]:
        """Generate two configs for spread strategy"""
        _, symbol1 = self.parse_source(source1)
        _, symbol2 = self.parse_source(source2)
        
        # Determine spot vs futures
        if '_SPOT' in symbol1:
            spot_sym, spot_ex = symbol1, exchange1
            fut_sym, fut_ex = symbol2, exchange2
        else:
            spot_sym, spot_ex = symbol2, exchange2
            fut_sym, fut_ex = symbol1, exchange1
        
        # Format symbols
        spot_formatted = self.format_symbol(spot_sym, spot_ex)
        fut_formatted = self.format_symbol(fut_sym, fut_ex)
        
        # Load the spread base template (with default offsets)
        base_template = self.load_template("base_spread.json")
        
        # Common values for both configs
        common_values = {
            'theo_config_file': 'fast_spread.json',
            'trade_config_file': 'sc.json',
            'hedge_config_file': 'hlimit.json',
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'spread_pct': spread_pct or 0,
            'counter': self.counter
        }
        
        # Config 1: Trade SPOT, Hedge FUTURES
        config1 = self.fill_template(base_template, {
            **common_values,
            'id': int(time.time() * 1000) % 100000,
            'trade_account': self.accounts[spot_ex]['spot'],
            'hedge_account': self.accounts[fut_ex]['futures'],
            'theo_comment': f"Trade {spot_sym} hedge {fut_sym}",
            'theo_config_name': f"FAST-SPOT-{self.counter}",
            'asset_symbol': fut_formatted,
            'asset_exchange': self.platforms[fut_ex]['futures'],
            'trade_symbol': spot_formatted,
            'trade_exchange': self.platforms[spot_ex]['spot'],
            'strategy_type': 'spread_trade_spot'
        })
        
        # Config 2: Trade FUTURES, Hedge SPOT
        config2 = self.fill_template(base_template, {
            **common_values,
            'id': (int(time.time() * 1000) + 1) % 100000,
            'trade_account': self.accounts[fut_ex]['futures'],
            'hedge_account': self.accounts[spot_ex]['spot'],
            'theo_comment': f"Trade {fut_sym} hedge {spot_sym}",
            'theo_config_name': f"FAST-FUT-{self.counter}",
            'asset_symbol': spot_formatted,
            'asset_exchange': self.platforms[spot_ex]['spot'],
            'trade_symbol': fut_formatted,
            'trade_exchange': self.platforms[fut_ex]['futures'],
            'strategy_type': 'spread_trade_futures'
        })
        
        self.counter += 1
        return config1, config2
    
    def generate_momentum_config(self, source: str, exchange: str,
                                momentum_pct: float = None) -> Dict:
        """Generate momentum strategy config"""
        _, symbol = self.parse_source(source)
        is_spot = '_SPOT' in symbol
        symbol_formatted = self.format_symbol(symbol, exchange)
        
        # Load the momentum base template (with default offsets)
        base_template = self.load_template("base_momentum.json")
        
        # Determine account and platform
        account = self.accounts[exchange]['spot' if is_spot else 'futures']
        platform = self.platforms[exchange]['spot' if is_spot else 'futures']
        
        # Fill template
        config = self.fill_template(base_template, {
            'id': int(time.time() * 1000) % 100000,
            'theo_config_file': 'fast_momentum.json',
            'trade_config_file': 'sc.json',
            'hedge_config_file': 'hmomentum.json',
            'trade_account': account,
            'hedge_account': account,
            'symbol': symbol_formatted,
            'exchange': platform,
            'timestamp': time.strftime("%Y-%m-%d %H:%M:%S"),
            'strategy_type': 'momentum',
            'momentum_pct': momentum_pct or 0,
            'counter': self.counter
        })
        
        self.counter += 1
        return config
    
    def save_config(self, config: Dict, suffix: str = "") -> Path:
        """Save config to file"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        # Extract symbol from config for filename
        symbol = "unknown"
        if "theo_config" in config:
            if "underlyingAssets" in config["theo_config"]:
                symbol = config["theo_config"]["underlyingAssets"].get("assetSymbol", "unknown")
            elif "tradeAsset" in config["theo_config"]:
                symbol = config["theo_config"]["tradeAsset"].get("assetSymbol", "unknown")
        
        clean_symbol = symbol.replace('-', '_').replace('/', '_')
        filename = f"trade_config_{clean_symbol}{suffix}_{timestamp}.json"
        filepath = self.output_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Saved: {filepath.name}")
        return filepath

# Simple API functions
def create_spread_configs(source1: str, source2: str, exchange1: str, exchange2: str,
                         spread_pct: float = None, output_dir: str = "./trading_configs") -> Tuple[Path, Path]:
    """Create spread strategy configs (returns 2 file paths)"""
    gen = ConfigGenerator(output_dir)
    config1, config2 = gen.generate_spread_configs(source1, source2, exchange1, exchange2, spread_pct)
    
    path1 = gen.save_config(config1, "_SPOT")
    path2 = gen.save_config(config2, "_FUT")
    
    return path1, path2

def create_momentum_config(source: str, exchange: str, momentum_pct: float = None,
                          output_dir: str = "./trading_configs") -> Path:
    """Create momentum strategy config (returns file path)"""
    gen = ConfigGenerator(output_dir)
    config = gen.generate_momentum_config(source, exchange, momentum_pct)
    path = gen.save_config(config, "_MOM")
    
    return path

# For alerts.py integration
def process_spread_alert(source1: str, source2: str, exchange1: str, exchange2: str,
                        spread_pct: float) -> Tuple[Path, Path]:
    """Process spread alert and generate configs"""
    logger.info(f"Spread alert: {source1} vs {source2} @ {spread_pct}%")
    return create_spread_configs(source1, source2, exchange1, exchange2, spread_pct)

def process_momentum_alert(source: str, exchange: str, momentum_pct: float) -> Path:
    """Process momentum alert and generate config"""
    logger.info(f"Momentum alert: {source} @ {momentum_pct}%")
    return create_momentum_config(source, exchange, momentum_pct)

# Diagnostic function
def check_templates(template_dir: str = None) -> bool:
    """Check if all required template files exist"""
    if template_dir:
        base_dir = Path(template_dir)
    else:
        base_dir = Path(__file__).parent / "config_templates"
    
    required_files = [
        "base_spread.json",
        "base_momentum.json",
        "theo_configs/fast_spread.json",
        "theo_configs/fast_momentum.json",
        "hedge_strategies/hlimit.json",
        "hedge_strategies/hmomentum.json",
        "trade_strategies/sc.json"
    ]
    
    print(f"Checking templates in: {base_dir}")
    all_exist = True
    
    for file in required_files:
        filepath = base_dir / file
        if filepath.exists():
            try:
                with open(filepath, 'r') as f:
                    json.load(f)
                print(f"  ✓ {file}")
            except json.JSONDecodeError as e:
                print(f"  ✗ {file} - Invalid JSON: {e}")
                all_exist = False
        else:
            print(f"  ✗ {file} - Not found")
            all_exist = False
    
    return all_exist

# Testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    
    print("=== TEMPLATE CHECK ===\n")
    if not check_templates():
        print("\n❌ Templates missing! Run setup_templates.py first.")
        print("   python setup_templates.py")
        exit(1)
    
    print("\n=== CONFIG GENERATOR TEST ===\n")
    
    # Test 1: Spread Strategy
    print("1. Spread Strategy (Binance BTCUSDT):")
    try:
        p1, p2 = create_spread_configs(
            "binance:BTCUSDT", 
            "binance:BTCUSDT_SPOT", 
            "binance", 
            "binance", 
            0.85
        )
        print(f"   ✓ {p1.name}")
        print(f"   ✓ {p2.name}")
        
        # Verify offsets are correct for spread
        with open(p1, 'r') as f:
            config = json.load(f)
            print(f"   Spread offsets: bid={config['offset_bid']}, ask={config['offset_ask']}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 2: Momentum Strategy  
    print("\n2. Momentum Strategy (Binance ETHUSDT):")
    try:
        p3 = create_momentum_config("binance:ETHUSDT", "binance", 2.5)
        print(f"   ✓ {p3.name}")
        
        # Verify offsets are correct for momentum
        with open(p3, 'r') as f:
            config = json.load(f)
            print(f"   Momentum offsets: bid={config['offset_bid']}, ask={config['offset_ask']}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 3: Cross-exchange Spread
    print("\n3. Cross-Exchange Spread (OKX):")
    try:
        p4, p5 = create_spread_configs(
            "okx:BTC-USDT-SWAP",
            "okx:BTC-USDT_SPOT",
            "okx",
            "okx",
            1.2
        )
        print(f"   ✓ {p4.name}")
        print(f"   ✓ {p5.name}")
    except Exception as e:
        print(f"   ✗ Error: {e}")
    
    # Test 4: Symbol Formatting
    print("\n4. Symbol Formatting Test:")
    gen = ConfigGenerator()
    tests = [
        ("BTCUSDT_SPOT", "binance"),
        ("BTC-USDT-SWAP", "okx"),
        ("BTC-USDT", "bybit")
    ]
    for symbol, exchange in tests:
        formatted = gen.format_symbol(symbol, exchange)
        print(f"   {exchange}: {symbol} → {formatted}")
    
    print("\n✅ Testing complete!")