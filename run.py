import os
import threading
import time
import logging
import signal
import websocket
from source.config import stop_event, active_threads
import signal
import time
import argparse
from source.utils import data_store
from source.message import send_message

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
    """Background thread to calculate spreads periodically"""
    while not stop_event.is_set():
        try:
            data_store.calculate_all_spreads()
        except Exception as e:
            logger.error(f"Error calculating spreads: {e}")
        
        # Calculate 5 times per second
        time.sleep(0.2)

#---------------------------------------------
# Headless Mode Functions
#---------------------------------------------
def run_headless():
    """Run the application in headless mode without UI"""
    from source.headless import HeadlessMonitor
    
    logger.info("Starting in headless mode...")
    
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
    
    # Start the spread calculator thread
    spread_calculator_thread = threading.Thread(
        target=run_spread_calculator, 
        daemon=True,
        name="spread_calculator"
    )
    spread_calculator_thread.start()
    active_threads["spread_calculator"] = spread_calculator_thread
    
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
    # Start the spread calculator thread
    spread_calculator_thread = threading.Thread(
        target=run_spread_calculator, 
        daemon=True,
        name="spread_calculator"
    )
    spread_calculator_thread.start()
    active_threads["spread_calculator"] = spread_calculator_thread


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
    send_message("Turning on telegram notifications")
    # Run in either headless or UI mode
    if args.headless:
        run_headless()
    else:
        run_with_ui()


if __name__ == "__main__":
    main()
