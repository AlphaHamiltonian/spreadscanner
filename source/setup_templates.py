#!/usr/bin/env python3
"""Setup script to create all template files and strategy config"""

import json
from pathlib import Path
import sys
from datetime import datetime

def create_templates(overwrite=False):
    """Create all template files and strategy config"""
    base_dir = Path("source/actions/config_templates") 
    base_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories
    for subdir in ["hedge_strategies", "trade_strategies", "theo_configs"]:
        (base_dir / subdir).mkdir(exist_ok=True)

    timestamp = datetime.now().strftime('%Y-%m-%d_%H_%M_%S')    

    # Template definitions
    templates = {
        "base.json": {
            "request_type": "trade",
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
                "generated_at": timestamp,
                "strategy": "{strategy_type}",
                "spread_pct": "{spread_pct}"
            }
        },
        
        "theo_configs/fast_spread.json": {
            "comments": "autoGen Spread-{trade_symbol}-{trade_exchange_short} on {asset_exchange_short}- {counter}",
            "configName": "autoS-{trade_symbol}-{trade_exchange_short} on {asset_exchange_short}",
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
            "comments": "Momentum autoGen-{symbol}-{exchange_short}-{counter}",
            "configName": "autoM-{symbol}-{exchange_short}",
            "checkTickersFrequency": 30000,
            "underlyingAssets": {
                "assetSymbol": "{symbol}",
                "assetExchange": "{exchange}",
                "currencySymbol": "",
                "currencyExchange": "",
                "hedgeAsset": "yes",
                "hedgeCurrency": "no",
                "minSizeOrderAsset": "6u",
                "minSizeOrderCurrency": "6u"
            },
            "tradeAsset": {
                "assetSymbol": "{symbol}",
                "assetExchange": "{exchange}",
                "minSizeOrder": "6u",
                "currencyType": "Divide"
            }
        },
        
        "hedge_strategies/hlimit.json": {
            "comments": "HLimit auto-{counter}",
            "configName": "HLimit auto- 2sec",
            "orderType": 1,
            "cutlossTimeout": 3000,
            "cutlossBPS": 50,
            "delayHedgeTime": 2000
        },
        
        "hedge_strategies/hmomentum.json": {
            "comments": "HMomentum auto-{counter}",
            "configName": "HMomentum auto",
            "icebergParts": 1,
            "delayHedgeTime": 0,
            "trailingStop": "150b",
            "trailingStep": "5b",
            "trailingLimit": "20b",
            "takeProfitTrigger": "100b",
            "takeProfitTrailingStop": "5b"
        },
        
        "trade_strategies/sc.json": {
            "comments": "SC auto-{counter}",
            "configName": "SC-auto",
            "orderType": 2,
            "spikeCheckTime": 0,
            "autoSizing": "no",
            "maxQty": 0,
            "minQty": 0
        },
        
        "trade_strategies/mm.json": {
            "comments": "MM auto-{counter}",
            "configName": "MM 10b-auto",
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
                "MM": {
                    "offset_bid": "-100b",
                    "offset_ask": "100b",
                    "bid_qty": "100u",
                    "ask_qty": "100u"
                }
            },
            "momentum": {
                "SC": {
                    "offset_bid": "-100b",
                    "offset_ask": "100b",
                    "bid_qty": "100u",
                    "ask_qty": "100u"
                }
            }
        },
        "custom_profiles": {
            "takeout": {
                "offset_bid": "10b",
                "offset_ask": "-10b",
                "bid_qty": "100u",
                "ask_qty": "100u",
                "description": "Take out"
            },
            "conservative_mm": {
                "offset_bid": "-200b",
                "offset_ask": "200b",
                "bid_qty": "100u",
                "ask_qty": "100u",
                "description": "Conservative"
            },
            "aggressive_mm": {
                "offset_bid": "-50b",
                "offset_ask": "50b",
                "bid_qty": "100u",
                "ask_qty": "100u",
                "description": "Agressive"
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
    #strategy_config_path = Path("source/actions/strategy_config.json")
    strategy_config_path = Path("source/actions/config_templates/strategy_config.json")
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