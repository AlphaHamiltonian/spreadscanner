#!/usr/bin/env python3
"""Setup script to create all template files and strategy config"""

import json
from pathlib import Path
import sys

def create_templates(overwrite=False):
    """Create all template files and strategy config"""
    base_dir = Path("source/config_templates")
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    for subdir in ["hedge_strategies", "trade_strategies", "theo_configs"]:
        (base_dir / subdir).mkdir(exist_ok=True)
    
    # Template definitions
    templates = {
        "base.json": {
            "request_type": "scan",
            "id": "{id}",
            "theo_type": "{theo_type}",
            "trade_strategy": "{trade_strategy}",
            "hedge_strategy": "{hedge_strategy}",
            "trade_account": "{trade_account}",
            "hedge_account": "{hedge_account}",
            "trade_times": "0",
            "offset_bid": "{offset_bid}",
            "offset_ask": "{offset_ask}",
            "bid_qty": "{bid_qty}",
            "ask_qty": "{ask_qty}",
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
        
        "theo_configs/fast_spread.json": {
            "comments": "{theo_comment}",
            "configName": "{theo_config_name}",
            "checkTickersFrequency": 30000,
            "underlyingAssets": {
                "assetSymbol": "{asset_symbol}",
                "assetExchange": "{asset_exchange}",
                "currencySymbol": "{currency_symbol}",
                "currencyExchange": "{currency_exchange}",
                "hedgeAsset": "yes",
                "hedgeCurrency": "{hedge_currency}",
                "minSizeOrderAsset": "6u",
                "minSizeOrderCurrency": "{min_size_currency}"
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
            "takeProfitTrigger": 100,
            "takeProfitTrailingStop": "5b"
        },
        
        "trade_strategies/sc.json": {
            "comments": "Config file for SC",
            "configName": "SC-{counter}",
            "orderType": 0,
            "spikeCheckTime": 0,
            "autoSizing": "no",
            "maxQty": 0,
            "minQty": 0
        },
        
        "trade_strategies/mm.json": {
            "comments": "Config file for MM",
            "configName": "MM 10b",
            "orderType": 0,
            "updateFrequencyTick": "10b",
            "postOnlyOption": "Yes",
            "restartTradeOnPartial": "false",
            "waitTick": 2000
        }
    }
    
    # Strategy config definition
    strategy_config = {
        "strategy_parameters": {
            "spread": {
                "SC": {
                    "offset_bid": "0",
                    "offset_ask": "0",
                    "bid_qty": "0",
                    "ask_qty": "0"
                },
                "MM": {
                    "offset_bid": "2",
                    "offset_ask": "2",
                    "bid_qty": "10",
                    "ask_qty": "10"
                }
            },
            "momentum": {
                "SC": {
                    "offset_bid": "0",
                    "offset_ask": "0",
                    "bid_qty": "0",
                    "ask_qty": "0"
                },
                "MM": {
                    "offset_bid": "5",
                    "offset_ask": "5",
                    "bid_qty": "20",
                    "ask_qty": "20"
                }
            }
        },
        "custom_profiles": {
            "aggressive_mm": {
                "offset_bid": "10",
                "offset_ask": "10",
                "bid_qty": "50",
                "ask_qty": "50",
                "description": "Aggressive market making"
            },
            "conservative_mm": {
                "offset_bid": "1",
                "offset_ask": "1",
                "bid_qty": "5",
                "ask_qty": "5",
                "description": "Conservative market making"
            },
            "scalper": {
                "offset_bid": "0.5",
                "offset_ask": "0.5",
                "bid_qty": "100",
                "ask_qty": "100",
                "description": "High frequency scalping"
            }
        }
    }
    
    # Create template files
    created = updated = skipped = 0
    
    for filename, content in templates.items():
        filepath = base_dir / filename
        
        if filepath.exists() and not overwrite:
            print(f"⏭️  Skipped: {filepath}")
            skipped += 1
        else:
            with open(filepath, 'w') as f:
                json.dump(content, f, indent=4)
            if filepath.exists():
                print(f"📝 Updated: {filepath}")
                updated += 1
            else:
                print(f"✅ Created: {filepath}")
                created += 1
    
    # Create strategy_config.json in source directory
    strategy_config_path = Path("source/strategy_config.json")
    if strategy_config_path.exists() and not overwrite:
        print(f"⏭️  Skipped: {strategy_config_path}")
        skipped += 1
    else:
        with open(strategy_config_path, 'w') as f:
            json.dump(strategy_config, f, indent=4)
        if strategy_config_path.exists():
            print(f"📝 Updated: {strategy_config_path}")
            updated += 1
        else:
            print(f"✅ Created: {strategy_config_path}")
            created += 1
    
    print(f"\n📊 Summary: Created: {created}, Updated: {updated}, Skipped: {skipped}")
    print(f"📁 Templates location: {base_dir.absolute()}")
    print(f"📁 Strategy config: {strategy_config_path.absolute()}")
    
    # Verify all files
    print("\n🔍 Verifying files...")
    all_valid = True
    
    # Check templates
    for filename in templates.keys():
        filepath = base_dir / filename
        try:
            with open(filepath, 'r') as f:
                json.load(f)
            print(f"   ✓ {filename}")
        except Exception as e:
            print(f"   ✗ {filename} - {e}")
            all_valid = False
    
    # Check strategy config
    try:
        with open(strategy_config_path, 'r') as f:
            json.load(f)
        print(f"   ✓ strategy_config.json")
    except Exception as e:
        print(f"   ✗ strategy_config.json - {e}")
        all_valid = False
    
    print(f"\n{'✅ All files valid!' if all_valid else '❌ Some files have issues'}")

if __name__ == "__main__":
    overwrite = '--overwrite' in sys.argv or '-o' in sys.argv
    create_templates(overwrite)