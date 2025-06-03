import requests
from source.actionToJSON import create_spread_configs
import orjson as json
# --------------------------------------------------------------------------- #
# Configuration – kept identical to your existing variable names
# --------------------------------------------------------------------------- #
import source.config as config

TELEGRAM_BOT_TOKEN = "REDACTED_TOKEN"  # Replace with your bot token
TELEGRAM_CHAT_ID = "REDACTED_CHAT_ID"  # Replace with your group chat ID


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def send_message(message):
    print("connecting")
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
    
def send_trade(source1, source2, exchange1, exchange2, spread_pct):
    print("connecting")
    """Generate trading configs and send via Telegram bot."""
    if not config.TELEGRAM_ENABLED:
        print("Telegram notifications are disabled.")
        return False
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram bot token or chat ID not configured.")
        return False
    
    try:
        # Custom parameters for MM strategy
        custom_params = {
            'bid_qty': '200u',  # Change this to your desired quantity
            'ask_qty': '200u'   # Change this to your desired quantity
        }
        
        # Generate spread configs with MM strategy and custom quantities
        config1_path, config2_path = create_spread_configs(
            source1, source2, exchange1, exchange2, spread_pct, 
            strategy='MM',  # Changed from 'SC' to 'MM'
            custom=custom_params  # Added custom parameters
        )
        
        # Read the generated configs
        with open(config1_path, 'r') as f:
            config1 = json.load(f)
        with open(config2_path, 'r') as f:
            config2 = json.load(f)
        
        # Create message with both configs
        message = f"Trading configs generated for {source1} vs {source2} (spread: {spread_pct:.2f}%)\n\n"
        message += f"Config 1:\n```json\n{json.dumps(config1, indent=2)}\n```\n\n"
        message += f"Config 2:\n```json\n{json.dumps(config2, indent=2)}\n```"
        
        # Send message
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"  # Use Markdown for code blocks
        }
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            print(f"Trade configs sent successfully.")
            return True
        else:
            print(f"Failed to send trade configs. Status code: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"Error sending trade configs: {e}")
        return False