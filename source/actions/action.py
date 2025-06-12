import requests
from source.actions.actionToJSON import generate_spread_configs_direct
import json
import source.core.config as config
from source.websockets.websocket_server import trading_signal_server

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
    """Send a message via Telegram only (for alerts)"""
    # Alerts always go to Telegram, not WebSocket
    # This is different from send_trade which respects BROADCAST_METHOD
    
    if not config.TELEGRAM_ENABLED:
        print("Alert triggered but Telegram notifications are disabled.")
        return False
    
    return send_telegram_message(message)

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

def send_trade(source1, source2, exchange1, exchange2, spread_pct, custom_params=None):
    """Generate trading configs and send via configured broadcast method"""
    print(f"Sending trade signal via {BROADCAST_METHOD}")
    
    try:
        
        config1 = generate_spread_configs_direct(
            source1, source2, exchange1, exchange2, spread_pct, 
            strategy='MM',
            custom=custom_params
        )
        
        # 2. Prepare common data
        config1_name = config1.get('theo_config', {}).get('configName', 'Config1')
        
        # 3. Track success for each method
        telegram_success = False
        websocket_success = False
        
        # 4. Handle Telegram
        if BROADCAST_METHOD in ["telegram", "both"]:
            if config.TELEGRAM_ENABLED:
                # Determine message format
                if BROADCAST_METHOD == "both":
                    # Short notification when using both methods
                    message = (
                        f"Trading configs generated for {source1} vs {source2} "
                        f"(spread: {spread_pct:.2f}%)\n\n"
                        f"Configs: {config1_name}\n"
                        f"Full details sent via WebSocket"
                    )
                else:
                    # Full details when Telegram-only
                    message = (
                        f"Trading configs generated for {source1} vs {source2} "
                        f"(spread: {spread_pct:.2f}%)\n\n"
                        f"Config 1:\n```json\n{json.dumps(config1, indent=2)}\n```\n\n"
                    )
                
                telegram_success = send_telegram_message(message)
            else:
                print("Telegram broadcast requested but notifications are disabled")
        
        # 5. Handle WebSocket
        if BROADCAST_METHOD in ["websocket", "both"]:
            trading_signal_server.queue_trading_signal(
                source1, source2, exchange1, exchange2, spread_pct, config1
            )
            print("Trade signal queued for WebSocket broadcast")
            websocket_success = True
        
        # 6. Return overall success
        return telegram_success or websocket_success
        
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