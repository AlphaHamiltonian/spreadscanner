import requests
from source.actionToJSON import generate_spread_configs_direct
import json
import source.config as config
from source.websocket_server import trading_signal_server

# --------------------------------------------------------------------------- #
# Configuration – kept identical to your existing variable names
# --------------------------------------------------------------------------- #

TELEGRAM_BOT_TOKEN = "REDACTED_TOKEN"  # Replace with your bot token
TELEGRAM_CHAT_ID = "REDACTED_CHAT_ID"  # Replace with your group chat ID

# New configuration for broadcast method
BROADCAST_METHOD = "websocket"  # Options: "telegram", "websocket", "both"

# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def send_message(message):
    """Send a message via configured broadcast method"""
    print(f"Sending message via {BROADCAST_METHOD}")
    
    success = False
    
    if BROADCAST_METHOD in ["telegram", "both"]:
        success = send_telegram_message(message) or success
        
    if BROADCAST_METHOD in ["websocket", "both"]:
        success = send_websocket_message(message) or success
        
    return success

def send_telegram_message(message):
    """Send a message via Telegram bot."""
    if not config.TELEGRAM_ENABLED:
        print("Telegram notifications are disabled.")
        return False
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram bot token or chat ID not configured.")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"  # Enable HTML formatting
        }
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            print(f"Telegram message sent successfully.")
            return True
        else:
            print(f"Failed to send Telegram message. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error sending Telegram message: {e}")
        return False

def send_websocket_message(message, spread_data=None):
    """Send a message via WebSocket server"""
    try:
        # Check if server is running
        if not trading_signal_server.is_running:
            print("WebSocket server is not running.")
            return False
            
        # Send as alert
        trading_signal_server.queue_spread_alert(message, spread_data)
        print("WebSocket alert queued successfully.")
        return True
        
    except Exception as e:
        print(f"Error sending WebSocket message: {e}")
        return False

def send_trade(source1, source2, exchange1, exchange2, spread_pct):
    """Generate trading configs and send via configured broadcast method"""
    print(f"Sending trade signal via {BROADCAST_METHOD}")
    
    if BROADCAST_METHOD == "telegram":
        if not config.TELEGRAM_ENABLED:
            print("Telegram notifications are disabled.")
            return False
    
    try:
        # Custom parameters for MM strategy
        custom_params = {
            'bid_qty': '200u',  # Change this to your desired quantity
            'ask_qty': '200u'   # Change this to your desired quantity
        }
        
        # Generate spread configs with MM strategy and custom quantities
        config1, config2 = generate_spread_configs_direct(
            source1, source2, exchange1, exchange2, spread_pct, 
            strategy='MM',  # Changed from 'SC' to 'MM'
            custom=custom_params  # Added custom parameters
        )
        
        success = False
        
        # Send via Telegram if enabled
        if BROADCAST_METHOD in ["telegram", "both"]:
            message = f"Trading configs generated for {source1} vs {source2} (spread: {spread_pct:.2f}%)\n\n"
            message += f"Config 1:\n```json\n{json.dumps(config1, indent=2)}\n```\n\n"
            message += f"Config 2:\n```json\n{json.dumps(config2, indent=2)}\n```"
            
            if send_telegram_message(message):
                success = True
                
        # Send via WebSocket if enabled
        if BROADCAST_METHOD in ["websocket", "both"]:
            # Queue the trading signal for WebSocket broadcast
            trading_signal_server.queue_trading_signal(
                source1, source2, exchange1, exchange2, spread_pct, config1, config2
            )
            print("Trade signal queued for WebSocket broadcast")
            success = True
            
        return success
        
    except Exception as e:
        print(f"Error generating trade configs: {e}")
        return False

def set_broadcast_method(method: str):
    """Change the broadcast method at runtime"""
    global BROADCAST_METHOD
    if method in ["telegram", "websocket", "both"]:
        BROADCAST_METHOD = method
        print(f"Broadcast method set to: {method}")
        return True
    else:
        print(f"Invalid broadcast method: {method}. Use 'telegram', 'websocket', or 'both'")
        return False