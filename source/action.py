import requests

# --------------------------------------------------------------------------- #
# Configuration – kept identical to your existing variable names
# --------------------------------------------------------------------------- #
from source.config import TELEGRAM_ENABLED

TELEGRAM_BOT_TOKEN = "REDACTED_TOKEN"  # Replace with your bot token
TELEGRAM_CHAT_ID = "REDACTED_CHAT_ID"  # Replace with your group chat ID


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def send_message(message):
    print("connecting")
    """Send a message via Telegram bot."""
    if not TELEGRAM_ENABLED:
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

def send_trade(message):
    print("connecting")
    """Send a message via Telegram bot."""
    if not TELEGRAM_ENABLED:
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