import os
import threading
import time
import logging
import signal
import websocket
from source.core.config import stop_event, active_threads
import source.core.config as config
import signal
import time
import argparse
from source.core.utils import data_store
from source.actions.action import send_message, set_broadcast_method
from source.websockets.websocket_server import trading_signal_server
import argparse

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
        time.sleep(0.5)

def start_websocket_server(port=8765, use_ssl=True, max_wait=10):
    """Start the WebSocket server for broadcasting trading signals with SSL support"""
    try:
        logger.info(f"Starting {'secure ' if use_ssl else ''}WebSocket server on port {port}")
        
        # Configure the global server instance
        trading_signal_server.port = port
        trading_signal_server.use_ssl = use_ssl
        
        # If we need to change SSL setting, recreate the SSL context
        if use_ssl and not trading_signal_server.ssl_context:
            trading_signal_server.ssl_context = trading_signal_server._create_ssl_context()
        elif not use_ssl:
            trading_signal_server.ssl_context = None
            
        trading_signal_server.run_in_thread()
        
        # Wait for server to actually start with timeout
        start_time = time.time()
        while time.time() - start_time < max_wait:
            if trading_signal_server.is_running and trading_signal_server.loop:
                # Give the event loop a moment to fully initialize
                time.sleep(0.5)
                protocol = "wss" if use_ssl else "ws"
                logger.info(f"WebSocket server successfully started on {protocol}://localhost:{port}")
                return True
            time.sleep(0.1)
        
        logger.error(f"WebSocket server failed to start within {max_wait} seconds")
        return False
    except Exception as e:
        logger.error(f"Error starting WebSocket server: {e}")
        return False
#---------------------------------------------
# Headless Mode Functions
#---------------------------------------------
def run_headless(websocket_port=8765, use_ssl=True):
    """Run the application in headless mode without UI"""

    from source.headless import HeadlessMonitor
    # Start WebSocket server
    if start_websocket_server(websocket_port, use_ssl):
        logger.info("WebSocket server started successfully")
        # Set broadcast method to websocket since we're in headless mode
        set_broadcast_method("websocket")
    else:
        logger.warning("WebSocket server failed to start, falling back to Telegram only")
        set_broadcast_method("telegram")    
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
    parser.add_argument('--broadcast', 
                        choices=['telegram', 'websocket', 'both'],
                        default='both',
                        help='Broadcast method for trading signals (default: websocket)')
    parser.add_argument('--websocket-port', 
                        type=int, 
                        default=8765,
                        help='WebSocket server port (default: 8765)')
    parser.add_argument('--no-ssl', 
                        action='store_true',
                        help='Disable SSL for WebSocket server (use ws:// instead of wss://)')

    args = parser.parse_args()
    
    # Set broadcast method
    set_broadcast_method(args.broadcast)
    
    # Determine if we should use SSL
    use_ssl = not args.no_ssl
    # Run in either headless or UI mode

    if args.headless:
        config.TELEGRAM_ENABLED = True if args.broadcast in ['telegram', 'both'] else False
        if config.TELEGRAM_ENABLED:
            send_message("Starting exchange monitor with Telegram notifications")
        run_headless(args.websocket_port, use_ssl)
    else:
        config.TELEGRAM_ENABLED = False
        run_with_ui()


if __name__ == "__main__":
    main()