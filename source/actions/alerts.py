# Create: source/alerts.py
import time
import logging
from source.core.config import Config
from source.actions.action import send_message, send_trade

logger = logging.getLogger(__name__)

class AlertManager:
    def __init__(self):
        self.threshold_timestamps = {}
        self.last_notification_time = {}
        self.last_funding_notif_time = {}
        self.last_trade_time = {}
        self.last_movement_alert_time = {} 

    def check_spread_alert(self, spread_pct, source1, source2, exchange1, exchange2,bid1,ask1,bid2,ask2):
            """Check spread thresholds and send alerts"""
            if not (spread_pct > Config.UPPER_LIMIT or spread_pct < Config.LOWER_LIMIT):
                return
                
            asset_pair_key = f"{source1}_vs_{source2}"
            current_time = time.time()
            
            # Initialize timestamp list if needed
            if asset_pair_key not in self.threshold_timestamps:
                self.threshold_timestamps[asset_pair_key] = []
            
            # Add current timestamp
            self.threshold_timestamps[asset_pair_key].append(current_time)
            
            # Remove old timestamps
            self.threshold_timestamps[asset_pair_key] = [
                ts for ts in self.threshold_timestamps[asset_pair_key] 
                if current_time - ts <= Config.DELETE_OLD_TIME
            ]
            
            # Count unique seconds
            unique_seconds = set(int(ts) for ts in self.threshold_timestamps[asset_pair_key])
            
            # Check notification cooldown
            last_notif_time = self.last_notification_time.get(asset_pair_key, 0)
            last_trade_time = self.last_trade_time.get(asset_pair_key, 0)  
            
            if len(unique_seconds) >= Config.NUMBER_OF_SEC_THRESHOLD_TRADE and current_time - last_notif_time > 1800:
                if spread_pct > Config.UPPER_LIMIT:
                    notification_message = f"{source1} vs {source2}: {spread_pct:.2f}% above upper limit ({Config.UPPER_LIMIT}%)"
                else:
                    notification_message = f"{source1} vs {source2}: {spread_pct:.2f}% below lower limit ({Config.LOWER_LIMIT}%)"
                
                if exchange1 == exchange2 and exchange1 == "binance":
                    # Check 24-hour cooldown for trades (86400 seconds = 24 hours)
                    if current_time - last_trade_time > 86400:
                        ask_qty = round(100/max(bid1,ask1,bid2,ask2))
                        offset= max(bid1,ask1,bid2,ask2)*2
                        #SPOT on FUTURE
                        custom_params_SoF = {
                            'bid_qty': '0',
                            'ask_qty': ask_qty,
                            'offset_bid': '-100b',
                            'offset_ask': offset
                        }
                        if send_trade(source2, source2, exchange2, exchange2, spread_pct,custom_params_SoF,'MM'):
                            self.last_trade_time[asset_pair_key] = current_time  # Update trade time
                            logger.info(f"Trade sent for {asset_pair_key}. Next trade allowed in 24 hours.")

                    # Regular notification logic remains unchanged
                    if len(unique_seconds) >= Config.NUMBER_OF_SEC_THRESHOLD:
                        if send_message(notification_message):
                            self.last_notification_time[asset_pair_key] = current_time
                            logger.info(f"Notification sent for {asset_pair_key}. Next notification in 30 minutes.")

    def check_funding_alert(self, exchange, symbol, rate):
        """Check funding rate alerts"""
        try:
            r = float(str(rate).strip().rstrip("%"))
        except (ValueError, TypeError):
            return
        
        if abs(r) >= Config.FUNDING_RATE_THRESHOLD and exchange == "binance":
            key = (exchange, symbol)
            now = time.time()
            
            if now - self.last_funding_notif_time.get(key, 0) < Config.FUNDING_RATE_COOLDOWN:
                return
            
            direction = "positive" if r > 0 else "negative"
            msg = f"⚠️ Funding rate alert\n{exchange}:{symbol} → {r:.2f}% ({direction})"
            
            if send_message(msg):
                self.last_funding_notif_time[key] = now
                logger.info("Funding alert sent for %s. Next in 30 min.", key)

    def check_movement_alert(self, exchange: str, symbol: str, old_price: float, 
                        new_price: float, pct_change: float, time_diff: float):
        """Check rapid price movement alerts with cooldown"""
        key = f"{exchange}:{symbol}"
        current_time = time.time()
        
        # Check cooldown (same pattern as funding alerts)
        if key in self.last_movement_alert_time:
            if current_time - self.last_movement_alert_time[key] < Config.MOVEMENT_ALERT_COOLDOWN:
                return False
                
        # Send alert
        message = (
            f"🚀 RAPID PRICE MOVEMENT DETECTED\n"
            f"{exchange}:{symbol}\n"
            f"Price: {old_price:.8f} → {new_price:.8f}\n"
            f"Change: +{pct_change:.2f}% in {time_diff:.1f}s"
        )
        
        logger.warning(f"Rapid movement: {message}")
        
        # Send notification
        if send_message(message):
            self.last_movement_alert_time[key] = current_time
            logger.info(f"Movement alert sent for {key}")
        
        return True 
        
    def cleanup_old_data(self):
        """Clean up old alert data"""
        current_time = time.time()
        
        # Clean threshold timestamps older than 10 minutes
        for key in list(self.threshold_timestamps.keys()):
            self.threshold_timestamps[key] = [
                ts for ts in self.threshold_timestamps[key] 
                if current_time - ts <= 600
            ]
            if not self.threshold_timestamps[key]:
                del self.threshold_timestamps[key]
        
        # Clean notification times older than 2 hours
        old_keys = [k for k, v in self.last_notification_time.items() if current_time - v > 7200]
        for key in old_keys:
            del self.last_notification_time[key]
            
        old_keys = [k for k, v in self.last_funding_notif_time.items() if current_time - v > 7200]
        for key in old_keys:
            del self.last_funding_notif_time[key]

        old_keys = [k for k, v in self.last_movement_alert_time.items() if current_time - v > 7200]
        for key in old_keys:
            del self.last_movement_alert_time[key]

# Global instance
alert_manager = AlertManager()