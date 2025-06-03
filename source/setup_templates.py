#!/usr/bin/env python3
"""
Complete setup script to create all template files with proper default values
Run this to set up the entire template structure
"""

import json
from pathlib import Path
import sys

def create_templates(overwrite=False):
    """Create all template files with proper structure and default values
    
    Args:
        overwrite: If True, overwrites existing files. If False, skips existing files.
    """
    
    # Base directory
    base_dir = Path("source/config_templates")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    (base_dir / "hedge_strategies").mkdir(exist_ok=True)
    (base_dir / "trade_strategies").mkdir(exist_ok=True)
    (base_dir / "theo_configs").mkdir(exist_ok=True)
    
    # Template definitions with proper default values
    templates = {
        # Main base template for SPREAD strategies
        "base_spread.json": {
            "request_type": "scan",
            "id": "{id}",
            "theo_type": "FAST",
            "trade_strategy": "MM-TO",
            "hedge_strategy": "HLimit",
            "trade_account": "{trade_account}",
            "hedge_account": "{hedge_account}",
            "trade_times": "0",
            "offset_bid": "-100b",  # Default for spread
            "offset_ask": "100b",   # Default for spread
            "bid_qty": "100u",
            "ask_qty": "100u",
            "auto_hedge": "Yes",
            "theo_config": "@theo_configs/{theo_config_file}",
            "trade_config": "@trade_strategies/{trade_config_file}",
            "hedge_config": "@hedge_strategies/{hedge_config_file}",
            "metadata": {
                "generated_at": "{timestamp}",
                "strategy": "{strategy_type}",
                "spread_pct": "{spread_pct}"
            }
        },
        
        # Main base template for MOMENTUM strategies
        "base_momentum.json": {
            "request_type": "scan",
            "id": "{id}",
            "theo_type": "FAST",
            "trade_strategy": "SC",
            "hedge_strategy": "HMomentum",
            "trade_account": "{trade_account}",
            "hedge_account": "{hedge_account}",
            "trade_times": "0",
            "offset_bid": "10b",   # Default for momentum
            "offset_ask": "-10b",  # Default for momentum
            "bid_qty": "100u",
            "ask_qty": "100u",
            "auto_hedge": "Yes",
            "theo_config": "@theo_configs/{theo_config_file}",
            "trade_config": "@trade_strategies/{trade_config_file}",
            "hedge_config": "@hedge_strategies/{hedge_config_file}",
            "metadata": {
                "generated_at": "{timestamp}",
                "strategy": "{strategy_type}",
                "momentum_pct": "{momentum_pct}"
            }
        },
        
        # Theo configurations
        "theo_configs/fast_spread.json": {
            "comments": "{theo_comment}",
            "configName": "{theo_config_name}",
            "checkTickersFrequency": 30000,
            "underlyingAssets": {
                "assetSymbol": "{asset_symbol}",
                "assetExchange": "{asset_exchange}",
                "currencySymbol": "",
                "currencyExchange": "",
                "hedgeAsset": "yes",
                "hedgeCurrency": "no",
                "minSizeOrderAsset": "6u",
                "minSizeOrderCurrency": "0"
            },
            "tradeAsset": {
                "assetSymbol": "{trade_symbol}",
                "assetExchange": "{trade_exchange}",
                "minSizeOrder": "6u",
                "currencyType": "Divide"
            }
        },
        
        "theo_configs/fast_momentum.json": {
            "comments": "Momentum config for {symbol}",
            "configName": "FAST-MOM-{counter}",
            "checkTickersFrequency": 30000,
            "underlyingAssets": {
                "assetSymbol": "{symbol}",
                "assetExchange": "{exchange}",
                "currencySymbol": "",
                "currencyExchange": "",
                "hedgeAsset": "no",
                "hedgeCurrency": "no",
                "minSizeOrderAsset": "6u",
                "minSizeOrderCurrency": "0"
            },
            "tradeAsset": {
                "assetSymbol": "{symbol}",
                "assetExchange": "{exchange}",
                "minSizeOrder": "6u",
                "currencyType": "Divide"
            }
        },
        
        # Hedge strategies
        "hedge_strategies/hlimit.json": {
            "comments": "HLimit 1",
            "configName": "HLimit1 - 2sec",
            "orderType": 1,
            "cutlossTimeout": 3000,
            "cutlossBPS": 50,
            "delayHedgeTime": 2000
        },
        
        "hedge_strategies/hmomentum.json": {
            "comments": "Config file for HMomentum",
            "configName": "HMomentum BPS",
            "icebergParts": 1,
            "delayHedgeTime": 0,
            "trailingStop": "150b",
            "trailingStep": "5b",
            "trailingLimit": "20b",
            "takeProfitTrigger": "100b",
            "takeProfitTrailingStop": "5b"
        },
        
        # Trade strategies
        "trade_strategies/sc.json": {
            "comments": "Config file for SC",
            "configName": "SC-{counter}",
            "orderType": 2,
            "spikeCheckTime": 0,
            "autoSizing": "no",
            "maxQty": 0,
            "minQty": 0
        }
    }
    
    # Create all template files
    created_count = 0
    updated_count = 0
    skipped_count = 0
    
    for filename, content in templates.items():
        filepath = base_dir / filename
        
        # Check if file already exists
        if filepath.exists():
            if overwrite:
                with open(filepath, 'w') as f:
                    json.dump(content, f, indent=4)
                print(f"📝 Updated: {filepath}")
                updated_count += 1
            else:
                print(f"⏭️  Skipped: {filepath} (already exists)")
                skipped_count += 1
        else:
            with open(filepath, 'w') as f:
                json.dump(content, f, indent=4)
            print(f"✅ Created: {filepath}")
            created_count += 1
    
    print(f"\n📊 Summary:")
    print(f"   Total templates: {len(templates)}")
    print(f"   Created: {created_count}")
    print(f"   Updated: {updated_count}")
    print(f"   Skipped: {skipped_count}")
    print(f"\n📁 Templates location: {base_dir.absolute()}")
    
    # Verify all files exist and are valid JSON
    print(f"\n🔍 Verifying templates...")
    all_valid = True
    for filename in templates.keys():
        filepath = base_dir / filename
        try:
            with open(filepath, 'r') as f:
                json.load(f)
            print(f"   ✓ {filename}")
        except FileNotFoundError:
            print(f"   ✗ {filename} - File not found!")
            all_valid = False
        except json.JSONDecodeError as e:
            print(f"   ✗ {filename} - Invalid JSON: {e}")
            all_valid = False
    
    if all_valid:
        print(f"\n✅ All templates are valid!")
    else:
        print(f"\n❌ Some templates have issues. Please check above.")
    
    return base_dir

if __name__ == "__main__":
    print("=== TEMPLATE SETUP ===\n")
    
    # Check for command line arguments
    overwrite = False
    if len(sys.argv) > 1:
        if sys.argv[1] in ['--overwrite', '-o', '--force', '-f']:
            overwrite = True
            print("🔄 Overwrite mode: ENABLED\n")
        elif sys.argv[1] in ['--help', '-h']:
            print("Usage: python setup_templates.py [OPTIONS]")
            print("\nOptions:")
            print("  --overwrite, -o, --force, -f    Overwrite existing template files")
            print("  --help, -h                      Show this help message")
            print("\nBy default, existing files are preserved.")
            sys.exit(0)
    else:
        print("ℹ️  Preserve mode: Existing files will be skipped")
        print("   Use --overwrite to update existing files\n")
    
    template_dir = create_templates(overwrite=overwrite)
    
    print("\n📝 Next steps:")
    print("1. Run this script to create/update templates")
    print("2. Run: python source/actionToJSON.py")
    print("3. Check the trading_configs directory for generated files")
    
    # Quick test to ensure actionToJSON can find templates
    print("\n🧪 Quick test...")
    try:
        from pathlib import Path
        test_file = template_dir / "base_spread.json"
        if test_file.exists():
            print("✅ Template files are accessible")
        else:
            print("❌ Cannot find template files")
    except Exception as e:
        print(f"❌ Error: {e}")