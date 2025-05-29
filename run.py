import os
import threading
import time
import logging
import signal
import websocket
from source.config import stop_event, active_threads
import source.config as config
import signal
import time
import argparse
from source.utils import data_store
from source.action import send_message

# Disable WebSocket trace for cleaner logs
websocket.enableTrace(False)

# Configure logging - using the original simple configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("exchange_monitor.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)
websocket_debug_logger = logging.getLogger('websocket')
websocket_debug_logger.setLevel(logging.CRITICAL)
logger.info(f"Using websocket-client version: {websocket.__version__}")


def run_spread_calculator():
    """Background thread to calculate spreads with health monitoring"""
    consecutive_errors = 0
    last_successful_run = time.time()
    
    while not stop_event.is_set():
        try:
            start_time = time.time()
            
            # Use the new lightweight spread calculation
            data_store.calculate_all_spreads()
            
            # Track performance
            calc_time = time.time() - start_time
            if calc_time > 0.1:  # Log if taking more than 100ms
                logger.warning(f"Spread calculation took {calc_time:.3f}s")
            
            # Reset error counter on success
            consecutive_errors = 0
            last_successful_run = time.time()
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Error calculating spreads (attempt {consecutive_errors}): {e}")
            
            # If too many consecutive errors, wait longer
            if consecutive_errors > 5:
                logger.error("Too many spread calculation errors, waiting 5 seconds")
                time.sleep(5)
        
        # Check if spread calculator is healthy
        if time.time() - last_successful_run > 60:
            logger.critical("Spread calculator hasn't run successfully in 60 seconds!")
            # Could trigger an alert here
            send_message("⚠️ CRITICAL: Spread calculator hasn't run successfully in 60 seconds!")
        
        # Calculate 5 times per second (but now it's much faster)
        time.sleep(0.2)

def run_spread_calculator_with_monitor():
    """Wrapper that restarts spread calculator if it crashes"""
    restart_count = 0
    
    while not stop_event.is_set():
        try:
            logger.info(f"Starting spread calculator (restart count: {restart_count})")
            run_spread_calculator()
        except Exception as e:
            restart_count += 1
            logger.error(f"Spread calculator crashed, restarting in 5s (count: {restart_count}): {e}")
            time.sleep(5)

def save_state_periodically():
    """Save critical state to disk for recovery after restart"""
    while not stop_event.is_set():
        try:
            # Convert tuple keys to strings for JSON serialization
            serializable_equiv_map = {}
            for key, value in data_store.symbol_equivalence_map.items():
                # key is a tuple (exchange, symbol, target_exchange)
                string_key = f"{key[0]}|{key[1]}|{key[2]}"
                serializable_equiv_map[string_key] = value
            
            state = {
                'timestamp': time.time(),
                'symbol_equivalence_map': serializable_equiv_map,
                'symbols': {ex: list(syms) for ex, syms in data_store.symbols.items()},
                'tick_sizes': data_store.tick_sizes,
            }
            
            # Save atomically
            import orjson as json
            with open('.scanner_state.tmp', 'wb') as f:
                f.write(json.dumps(state))
            os.rename('.scanner_state.tmp', '.scanner_state.json')
            
            logger.info("Saved scanner state to disk")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
        
        # Save every 5 minutes
        time.sleep(300)

def load_saved_state():
    """Load saved state from disk if available"""
    try:
        if os.path.exists('.scanner_state.json'):
            import orjson as json
            with open('.scanner_state.json', 'rb') as f:
                state = json.loads(f.read())
            
            # Only use if recent (less than 1 hour old)
            if time.time() - state['timestamp'] < 3600:
                logger.info("Loading saved state from disk")
                
                # Restore symbol maps for faster startup
                if 'symbol_equivalence_map' in state:
                    # Convert string keys back to tuples
                    data_store.symbol_equivalence_map = {}
                    for string_key, value in state['symbol_equivalence_map'].items():
                        parts = string_key.split('|')
                        if len(parts) == 3:
                            tuple_key = (parts[0], parts[1], parts[2])
                            data_store.symbol_equivalence_map[tuple_key] = value
                
                # Restore symbols
                if 'symbols' in state:
                    for exchange, symbol_list in state['symbols'].items():
                        if exchange in data_store.symbols:
                            data_store.symbols[exchange] = set(symbol_list)
                
                # Restore tick sizes
                if 'tick_sizes' in state:
                    data_store.tick_sizes = state['tick_sizes']
                
                logger.info(f"Loaded state with {len(data_store.symbol_equivalence_map)} symbol mappings")
                return True
    except Exception as e:
        logger.error(f"Failed to load saved state: {e}")
    return False

def monitor_system_health():
    """Monitor system health and performance"""
    try:
        import psutil
        
        while not stop_event.is_set():
            try:
                process = psutil.Process()
                cpu_percent = process.cpu_percent(interval=1)
                memory_mb = process.memory_info().rss / 1024 / 1024
                
                # Get thread count
                thread_count = threading.active_count()
                
                # Log metrics periodically
                if time.time() % 300 < 1:  # Every 5 minutes
                    logger.info(f"System health: CPU={cpu_percent:.1f}%, Memory={memory_mb:.1f}MB, Threads={thread_count}")
                
                # Alert on high resource usage
                if cpu_percent > 80:
                    logger.warning(f"High CPU usage: {cpu_percent}%")
                if memory_mb > 2000:  # 2GB
                    logger.warning(f"High memory usage: {memory_mb}MB")
                    send_message(f"⚠️ High memory usage: {memory_mb:.1f}MB")
                
            except Exception as e:
                logger.error(f"Error in system health monitor: {e}")
            
            # Check every 10 seconds
            time.sleep(10)
    except ImportError:
        logger.warning("psutil not installed, system health monitoring disabled")

#---------------------------------------------
# Headless Mode Functions
#---------------------------------------------
def run_headless():
    """Run the application in headless mode without UI"""

    from source.headless import HeadlessMonitor
    config.TELEGRAM_ENABLED = True
    logger.info("Starting in headless mode...")
    
    # Load saved state if available
    load_saved_state()
    
    # Set up proper signal handling
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        stop_event.set()
        time.sleep(1)  # Allow time for threads to clean up
            
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Initialize headless monitor
    monitor = HeadlessMonitor()
    
    # Start the spread calculator with monitoring
    spread_calculator_thread = threading.Thread(
        target=run_spread_calculator_with_monitor, 
        daemon=True,
        name="spread_calculator_monitor"
    )
    spread_calculator_thread.start()
    active_threads["spread_calculator_monitor"] = spread_calculator_thread
    
    # Start state persistence
    state_saver_thread = threading.Thread(
        target=save_state_periodically,
        daemon=True,
        name="state_saver"
    )
    state_saver_thread.start()
    active_threads["state_saver"] = state_saver_thread
    
    # Start system health monitoring
    health_monitor_thread = threading.Thread(
        target=monitor_system_health,
        daemon=True,
        name="system_health_monitor"
    )
    health_monitor_thread.start()
    active_threads["system_health_monitor"] = health_monitor_thread
    
    # Start exchange connections
    monitor.start_exchange_threads()
    
    try:
        # Main loop - keep running until stopped
        while not stop_event.is_set():
            # Check data freshness periodically
            data_store.check_data_freshness()
            time.sleep(5)  # Sleep for 5 seconds between checks
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Unhandled exception in headless mode: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Clean shutdown
        stop_event.set()
        logger.info("Shutting down headless monitor...")
        monitor.shutdown()
        
        # Force exit after 3 seconds if normal shutdown fails
        def force_exit():
            logger.warning("Application taking too long to close, forcing exit")
            os._exit(0)
            
        # Schedule force exit
        exit_timer = threading.Timer(3.0, force_exit)
        exit_timer.daemon = True
        exit_timer.start()
        
        logger.info("Headless monitor terminated")

#---------------------------------------------
# UI Mode Functions
#---------------------------------------------
def run_with_ui():
    """Run the application with the UI interface"""

    # Conditionally import tkinter only in UI mode
    import tkinter as tk
    from source.ui import ExchangeMonitorApp
    config.TELEGRAM_ENABLED = False
    
    # Load saved state if available
    load_saved_state()
    
    # Set up proper signal handling
    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down gracefully...")
        stop_event.set()
        # Allow time for threads to clean up
        time.sleep(1)
        # Then destroy the root window
        if 'root' in globals():
            root.after(100, root.destroy)
            
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create main window
    root = tk.Tk()
    
    # Initialize application
    app = ExchangeMonitorApp(root)
    
    # Start the spread calculator with monitoring
    spread_calculator_thread = threading.Thread(
        target=run_spread_calculator_with_monitor, 
        daemon=True,
        name="spread_calculator_monitor"
    )
    spread_calculator_thread.start()
    active_threads["spread_calculator_monitor"] = spread_calculator_thread
    
    # Start state persistence
    state_saver_thread = threading.Thread(
        target=save_state_periodically,
        daemon=True,
        name="state_saver"
    )
    state_saver_thread.start()
    active_threads["state_saver"] = state_saver_thread
    
    # Start system health monitoring
    health_monitor_thread = threading.Thread(
        target=monitor_system_health,
        daemon=True,
        name="system_health_monitor"
    )
    health_monitor_thread.start()
    active_threads["system_health_monitor"] = health_monitor_thread

    def on_close():
        logger.info("Application shutting down...")
        stop_event.set()
        
        # Start shutdown in a separate thread to keep UI responsive
        def background_shutdown():
            try:
                # Aggressively terminate WebSockets
                for exchange in [app.binance, app.bybit, app.okx]:
                    for name, manager in exchange.websocket_managers.items():
                        if isinstance(manager, dict):
                            # Handle dictionary-type manager
                            manager['is_running'] = False  # Prevent reconnection attempts
                            if 'ws' in manager and manager['ws']:
                                try:
                                    manager['ws'].close()
                                except:
                                    pass
                        else:
                            # Handle WebSocketManager object
                            if hasattr(manager, 'is_running'):
                                manager.is_running = False  # Prevent reconnection attempts
                            if hasattr(manager, 'ws') and manager.ws:
                                try:
                                    manager.ws.close()
                                except:
                                    pass
                                    
                # Shutdown executor without waiting
                if hasattr(app, 'executor'):
                    app.executor.shutdown(wait=False)
            except Exception as e:
                logger.error(f"Error in background shutdown: {e}")
                
        # Start background shutdown and destroy window immediately
        threading.Thread(target=background_shutdown, daemon=True).start()
        root.destroy()
        
    root.protocol("WM_DELETE_WINDOW", on_close)
    
    # Start main loop with exception handling
    try:
        root.mainloop()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        on_close()
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        on_close()
    finally:
        # Force exit after 3 seconds if normal shutdown fails
        def force_exit():
            logger.warning("Application taking too long to close, forcing exit")
            os._exit(0)
            
        # Schedule force exit
        exit_timer = threading.Timer(3.0, force_exit)
        exit_timer.daemon = True
        exit_timer.start()
        
        logger.info("Application terminated")

#---------------------------------------------
# Main Application Entry Point
#---------------------------------------------
def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Crypto Exchange Monitor')
    parser.add_argument('--headless', action='store_true', 
                        help='Run in headless mode without UI (for servers)')
    args = parser.parse_args()
    send_message("🚀 Exchange Monitor Starting Up")
    
    # Run in either headless or UI mode
    if args.headless:
        run_headless()
    else:
        run_with_ui()


if __name__ == "__main__":
    main()