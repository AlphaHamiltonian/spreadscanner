# source/price_movement_detector.py
"""
Rapid price movement detector for headless mode.
Detects when prices move up by 20% in a second.
"""
import time
import logging
from collections import deque, defaultdict
from threading import Lock
from typing import Dict, Tuple, Optional
from source.actions.action import send_message

logger = logging.getLogger(__name__)

class PriceMovementDetector:
    """Detects rapid price movements with minimal overhead"""
    
    def __init__(self, threshold_pct: float = 20.0, time_window: float = 1.0):
        self.threshold_pct = threshold_pct
        self.time_window = time_window
        
        # Price history: {exchange: {symbol: deque[(timestamp, price, type)]}}
        self.price_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=50)))
        self.history_lock = Lock()
        
        # Alert cooldown to prevent spam
        self.last_alert_time = {}
        self.alert_cooldown = 60  # 1 minute between alerts for same symbol
        
        # Statistics
        self.detections = 0
        self.last_detection_time = 0
        
    def record_price(self, exchange: str, symbol: str, price: float, price_type: str = 'mid'):
        """Record a price point for movement detection
        
        Args:
            exchange: Exchange name
            symbol: Symbol name
            price: Price value (bid, ask, or mid)
            price_type: 'bid', 'ask', or 'mid'
        """
        if price <= 0:
            return
            
        current_time = time.time()
        
        with self.history_lock:
            # Add new price point
            history = self.price_history[exchange][symbol]
            history.append((current_time, price, price_type))
            
            # Check for rapid movement while we have the lock
            self._check_movement(exchange, symbol, history, current_time)
    
    def _check_movement(self, exchange: str, symbol: str, history: deque, current_time: float):
        """Check if rapid movement occurred"""
        if len(history) < 2:
            return
            
        # Get current price
        current_price = history[-1][1]
        
        # Look back through history within time window
        for timestamp, old_price, _ in reversed(history):
            time_diff = current_time - timestamp
            
            # Stop if outside time window
            if time_diff > self.time_window:
                break
                
            # Calculate percentage change
            if old_price > 0:
                pct_change = ((current_price - old_price) / old_price) * 100
                
                # Check if movement exceeds threshold
                if pct_change >= self.threshold_pct:
                    self._trigger_alert(exchange, symbol, old_price, current_price, 
                                      pct_change, time_diff)
                    break
    
    def _trigger_alert(self, exchange: str, symbol: str, old_price: float, 
                       new_price: float, pct_change: float, time_diff: float):
        """Trigger alert for rapid price movement"""
        key = f"{exchange}:{symbol}"
        current_time = time.time()
        
        # Check cooldown
        if key in self.last_alert_time:
            if current_time - self.last_alert_time[key] < self.alert_cooldown:
                return
                
        # Send alert
        message = (
            f"🚀 RAPID PRICE MOVEMENT DETECTED\n"
            f"{exchange}:{symbol}\n"
            f"Price: {old_price:.8f} → {new_price:.8f}\n"
            f"Change: +{pct_change:.2f}% in {time_diff:.1f}s"
        )
        
        logger.warning(f"Rapid movement: {message}")
        
        # Send to telegram/websocket
        try:
            send_message(message)
            self.last_alert_time[key] = current_time
            self.detections += 1
            self.last_detection_time = current_time
        except Exception as e:
            logger.error(f"Failed to send movement alert: {e}")
    
    def get_stats(self) -> Dict[str, any]:
        """Get detector statistics"""
        return {
            'detections': self.detections,
            'last_detection': self.last_detection_time,
            'monitored_symbols': sum(len(symbols) for symbols in self.price_history.values()),
            'total_price_points': sum(
                len(history) for exchange_data in self.price_history.values()
                for history in exchange_data.values()
            )
        }
    
    def cleanup_old_data(self, max_age: float = 300):
        """Remove price data older than max_age seconds"""
        current_time = time.time()
        
        with self.history_lock:
            for exchange_data in self.price_history.values():
                for symbol, history in list(exchange_data.items()):
                    # Remove old entries
                    while history and current_time - history[0][0] > max_age:
                        history.popleft()
                    
                    # Remove symbol if no data left
                    if not history:
                        del exchange_data[symbol]

# Global detector instance (only created in headless mode)
movement_detector = None

def initialize_detector(threshold_pct: float = 20.0, time_window: float = 1.0):
    """Initialize the movement detector"""
    global movement_detector
    movement_detector = PriceMovementDetector(threshold_pct, time_window)
    logger.info(f"Movement detector initialized: {threshold_pct}% in {time_window}s")
    return movement_detector