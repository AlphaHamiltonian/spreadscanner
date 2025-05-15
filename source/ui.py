import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import concurrent.futures
import time
import math
import random
import csv
from source.config import stop_event, active_threads
import time
from source.utils import data_store
import logging
from source.exchanges.binance import BinanceConnector
from source.exchanges.bybit import BybitConnector
from source.exchanges.okx import OkxConnector

logger = logging.getLogger(__name__) # module-specific logger


class ExchangeMonitorApp:
    """Main application UI class."""
    def __init__(self, root):
        self.root = root
        self.root.title("Crypto Exchange Monitor")
        self.root.geometry("1400x800")
        
        # Initialize sort tracking variables for both tables
        self.upper_sorted_column = 'symbol'
        self.lower_sorted_column = 'symbol'
        self.last_update_time = 0
        
        # Set up style
        self.style = ttk.Style()
        self.style.theme_use('clam')  # Use a more modern theme
        
        # Customize treeview colors
        self.style.configure(
            "Treeview",
            background="#f5f5f5",
            foreground="black",
            rowheight=25,
            fieldbackground="#f5f5f5"
        )
        self.style.map('Treeview', background=[('selected', '#347ab3')])
        
        # Create the main frame
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Create the main control panel (exchange selection and filter)
        self.create_control_panel()
        
        # Create upper table frame
        self.upper_table_frame = ttk.LabelFrame(self.main_frame, text="Main Data View", padding="5")
        self.upper_table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create lower table frame
        self.lower_table_frame = ttk.LabelFrame(self.main_frame, text="Secondary Data View", padding="5")
        self.lower_table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create sorting controls for each table
        self.create_table_controls(self.upper_table_frame, "upper")
        self.create_table_controls(self.lower_table_frame, "lower")
        
        # Create the data tables
        self.upper_table = self.create_data_table(self.upper_table_frame, "upper")
        self.lower_table = self.create_data_table(self.lower_table_frame, "lower")
        
        # Data refresh flags for both tables
        self.upper_mouse_over_table = False
        self.lower_mouse_over_table = False
        
        # Exchange connectors and websocket managers
        self.websocket_managers = {}
        self.binance = BinanceConnector(self)
        self.bybit = BybitConnector(self)
        self.okx = OkxConnector(self)
        
        # Initialize thread pools
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)
        
        # Start with Binance by default
        self.start_exchange_threads()
        
        # Start health monitor
        self.start_health_monitor()
        
        # Schedule periodic UI updates
        self.schedule_updates()

    def create_control_panel(self):
        """Create the control panel with configuration options"""
        control_frame = ttk.LabelFrame(self.main_frame, text="Controls", padding="10")
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Exchange selection
        ttk.Label(control_frame, text="Exchange:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.exchange_var = tk.StringVar(value="all")
        exchange_combo = ttk.Combobox(
            control_frame,
            textvariable=self.exchange_var,
            values=["all", "binance", "bybit", "okx"],
            width=10,
            state="readonly"
        )
        exchange_combo.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        exchange_combo.bind("<<ComboboxSelected>>", self.on_exchange_change)
        
        # Refresh button
        refresh_btn = ttk.Button(
            control_frame,
            text="Refresh Data",
            command=self.manual_refresh
        )
        refresh_btn.grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        
        # Export CSV button
        export_btn = ttk.Button(
            control_frame,
            text="Export CSV",
            command=self.export_to_csv
        )
        export_btn.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        # Filter by symbol
        ttk.Label(control_frame, text="Filter:").grid(row=0, column=10, padx=5, pady=5, sticky=tk.W)
        self.filter_var = tk.StringVar()
        filter_entry = ttk.Entry(control_frame, textvariable=self.filter_var, width=15)
        filter_entry.grid(row=0, column=11, padx=5, pady=5, sticky=tk.W)
        self.filter_var.trace("w", lambda *args: self.apply_filter())

    def create_table_controls(self, parent_frame, table_id):
        """Create sorting controls for a specific table"""
        control_frame = ttk.Frame(parent_frame)
        control_frame.pack(fill=tk.X, pady=(0, 5))
        
        # Sort options
        ttk.Label(control_frame, text="Sort by:").pack(side=tk.LEFT, padx=5)
        
        # Create sort variables for this table
        if table_id == "upper":
            self.upper_sort_column_var = tk.StringVar(value="Symbol")
            self.upper_sort_direction_var = tk.StringVar(value="ascending")
            sort_column_var = self.upper_sort_column_var
            sort_direction_var = self.upper_sort_direction_var
            callback = lambda *args: self.apply_sorting("upper")
        else:
            self.lower_sort_column_var = tk.StringVar(value="Symbol")
            self.lower_sort_direction_var = tk.StringVar(value="ascending")
            sort_column_var = self.lower_sort_column_var
            sort_direction_var = self.lower_sort_direction_var
            callback = lambda *args: self.apply_sorting("lower")
            
        sort_combo = ttk.Combobox(
            control_frame,
            textvariable=sort_column_var,
            values=["Symbol", "Exchange", "Funding Rate", "Spread vs Spot",
                    "Spread vs Binance", "Spread vs OKX", "Spread vs Bybit", "24h Change"],
            width=15,
            state="readonly"
        )
        sort_combo.pack(side=tk.LEFT, padx=5)
        sort_combo.bind("<<ComboboxSelected>>", callback)
        
        # Sort direction
        ttk.Radiobutton(
            control_frame,
            text="Ascending",
            variable=sort_direction_var,
            value="ascending",
            command=callback
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Radiobutton(
            control_frame,
            text="Descending",
            variable=sort_direction_var,
            value="descending",
            command=callback
        ).pack(side=tk.LEFT, padx=5)

    def export_to_csv(self):
        """Export the current data table to a CSV file"""
        # Ask user for save location
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            title="Export data as CSV"
        )
        if not file_path:  # User cancelled
            return
            
        try:
            # Get all symbols from all exchanges
            all_symbols = {}
            for exchange in ['binance', 'bybit', 'okx']:
                symbols = data_store.get_symbols(exchange)
                all_symbols[exchange] = set(symbols)
                
            # Prepare the data using the same function that updates the table
            # Use the upper table's sort settings for export
            table_data = self._prepare_table_data(
                all_symbols,
                self.upper_sort_column_var.get(),
                self.upper_sort_direction_var.get()
            )
            
            # Write to CSV
            with open(file_path, 'w', newline='') as csvfile:
                # Create CSV writer and write header
                fieldnames = [
                    'Symbol', 'Exchange', 'Bid', 'Ask', 'Funding Rate',
                    'Spread vs Spot', 'Spread vs Binance', 'Spread vs OKX',
                    'Spread vs Bybit', 'Future Tick Size', 'Spot Tick Size',
                    '24h Change'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                # Write data rows
                for item in table_data:
                    writer.writerow({
                        'Symbol': item['symbol'],
                        'Exchange': item['exchange'],
                        'Bid': item['bid'],
                        'Ask': item['ask'],
                        'Funding Rate': item['funding_rate'],
                        'Spread vs Spot': item['spread_vs_spot'],
                        'Spread vs Binance': item['spread_vs_binance'],
                        'Spread vs OKX': item['spread_vs_okx'],
                        'Spread vs Bybit': item['spread_vs_bybit'],
                        'Future Tick Size': item['future_tick_size'],
                        'Spot Tick Size': item['spot_tick_size'],
                        '24h Change': item['change_24h']
                    })
                    
            # Show confirmation
            messagebox.showinfo("Export Successful", f"Data exported to {file_path}")
            logger.info(f"Data exported to CSV: {file_path}")
        except Exception as e:
            # Show error message
            messagebox.showerror("Export Failed", f"Failed to export data: {str(e)}")
            logger.error(f"CSV export error: {e}")

    def create_data_table(self, parent_frame, table_id):
        """Create a data table in the given parent frame"""
        # Create a frame for the table with scrollbars
        table_frame = ttk.Frame(parent_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create scrollbars
        y_scrollbar = ttk.Scrollbar(table_frame)
        y_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        x_scrollbar = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        x_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Create treeview
        table = ttk.Treeview(
            table_frame,
            yscrollcommand=y_scrollbar.set,
            xscrollcommand=x_scrollbar.set,
            selectmode='none'
        )
        
        # Configure columns
        table['columns'] = (
            'symbol', 'exchange', 'bid', 'ask', 'funding_rate', 'spread_vs_spot', 'spread_vs_binance',
            'spread_vs_okx', 'spread_vs_bybit', 'future_tick_size', 'spot_tick_size', 'change_24h'
        )
        
        # Format columns
        table.column('#0', width=0, stretch=tk.NO)  # Hidden ID column
        table.column('symbol', width=120, anchor=tk.W)
        table.column('exchange', width=80, anchor=tk.W)
        table.column('bid', width=100, anchor=tk.E)
        table.column('ask', width=100, anchor=tk.E)
        table.column('funding_rate', width=100, anchor=tk.E)
        table.column('spread_vs_spot', width=120, anchor=tk.E)
        table.column('spread_vs_binance', width=120, anchor=tk.E)
        table.column('spread_vs_okx', width=120, anchor=tk.E)
        table.column('spread_vs_bybit', width=120, anchor=tk.E)
        table.column('future_tick_size', width=100, anchor=tk.E)
        table.column('spot_tick_size', width=100, anchor=tk.E)
        table.column('change_24h', width=100, anchor=tk.E)
        
        # Create headings
        table.heading('#0', text='', anchor=tk.W)
        table.heading('symbol', text='Symbol', anchor=tk.W)
        table.heading('exchange', text='Exchange', anchor=tk.W)
        table.heading('bid', text='Bid', anchor=tk.W)
        table.heading('ask', text='Ask', anchor=tk.W)
        table.heading('funding_rate', text='Funding Rate', anchor=tk.W)
        table.heading('spread_vs_spot', text='Spread vs Spot', anchor=tk.W)
        table.heading('spread_vs_binance', text='Spread vs Binance', anchor=tk.W)
        table.heading('spread_vs_okx', text='Spread vs OKX', anchor=tk.W)
        table.heading('spread_vs_bybit', text='Spread vs Bybit', anchor=tk.W)
        table.heading('future_tick_size', text='Future Tick', anchor=tk.W)
        table.heading('spot_tick_size', text='Spot Tick', anchor=tk.W)
        table.heading('change_24h', text='24h Change%', anchor=tk.W)
        
        # Add heading click handlers for sorting
        for col in table['columns']:
            table.heading(col, command=lambda _col=col, _id=table_id: self.on_heading_click(_col, _id))
        
        # Pack the table
        table.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Configure scrollbars
        y_scrollbar.config(command=table.yview)
        x_scrollbar.config(command=table.xview)
        
        # Mouse over handling to freeze updates
        if table_id == "upper":
            table.bind('<Enter>', lambda e: self.on_table_enter(e, "upper"))
            table.bind('<Leave>', lambda e: self.on_table_leave(e, "upper"))
        else:
            table.bind('<Enter>', lambda e: self.on_table_enter(e, "lower"))
            table.bind('<Leave>', lambda e: self.on_table_leave(e, "lower"))
        
        # Set alternating row colors
        table.tag_configure('odd', background='#f0f0f0')
        table.tag_configure('even', background='#ffffff')
        
        # Set color tags for positive/negative values
        table.tag_configure('positive', foreground='green')
        table.tag_configure('negative', foreground='red')
        table.tag_configure('neutral', foreground='black')
        
        return table

    def on_table_enter(self, event, table_id):
        """Freeze updates when mouse enters the table"""
        if table_id == "upper":
            self.upper_mouse_over_table = True
        else:
            self.lower_mouse_over_table = True

    def on_table_leave(self, event, table_id):
        """Resume updates when mouse leaves the table"""
        if table_id == "upper":
            self.upper_mouse_over_table = False
            self.update_data_table(force_upper=True)
        else:
            self.lower_mouse_over_table = False
            self.update_data_table(force_lower=True)

    def on_exchange_change(self, event):
        """Handle exchange selection change"""
        self.restart_exchange_threads()

    def on_heading_click(self, column, table_id):
        """Handle column header click for sorting"""
        # Map treeview column names to sort field names
        column_to_field = {
            'symbol': 'Symbol',
            'exchange': 'Exchange',
            'funding_rate': 'Funding Rate',
            'spread_vs_spot': 'Spread vs Spot',
            'spread_vs_binance': 'Spread vs Binance',
            'spread_vs_okx': 'Spread vs OKX',
            'spread_vs_bybit': 'Spread vs Bybit',
            'change_24h': '24h Change'
        }
        
        if column in column_to_field:
            if table_id == "upper":
                logger.info(f"Upper table header clicked: {column} -> {column_to_field[column]}")
                
                # Set the sort column variable
                self.upper_sort_column_var.set(column_to_field[column])
                
                # Toggle sort direction if clicking the same column
                if self.upper_sorted_column == column:
                    if self.upper_sort_direction_var.get() == 'ascending':
                        self.upper_sort_direction_var.set('descending')
                    else:
                        self.upper_sort_direction_var.set('ascending')
                        
                self.upper_sorted_column = column
                logger.info(f"Sorting upper table by {column_to_field[column]} ({self.upper_sort_direction_var.get()})")
                
                # Force a sort and update now
                self.apply_sorting("upper")
            else:  # Lower table
                logger.info(f"Lower table header clicked: {column} -> {column_to_field[column]}")
                
                # Set the sort column variable
                self.lower_sort_column_var.set(column_to_field[column])
                
                # Toggle sort direction if clicking the same column
                if self.lower_sorted_column == column:
                    if self.lower_sort_direction_var.get() == 'ascending':
                        self.lower_sort_direction_var.set('descending')
                    else:
                        self.lower_sort_direction_var.set('ascending')
                        
                self.lower_sorted_column = column
                logger.info(f"Sorting lower table by {column_to_field[column]} ({self.lower_sort_direction_var.get()})")
                
                # Force a sort and update now
                self.apply_sorting("lower")

    def apply_sorting(self, table_id="upper"):
        """Apply sorting to the specified data table and trigger an update"""
        if table_id == "upper":
            logger.info(f"Applying upper sort: {self.upper_sort_column_var.get()} ({self.upper_sort_direction_var.get()})")
            self.update_data_table(force_upper=True)
        else:
            logger.info(f"Applying lower sort: {self.lower_sort_column_var.get()} ({self.lower_sort_direction_var.get()})")
            self.update_data_table(force_lower=True)

    def apply_filter(self):
        """Apply symbol filter to both data tables"""
        self.update_data_table(force_upper=True, force_lower=True)

    def manual_refresh(self):
        """Manually refresh data"""
        self.restart_exchange_threads()
        
    def _prepare_table_data(self, all_symbols, sort_column=None, sort_direction=None):
        """Prepare table data in background thread with explicit sort parameters"""
        # If sort parameters weren't provided, use the current values
        if sort_column is None:
            sort_column = self.upper_sort_column_var.get()
            
        if sort_direction is None:
            sort_direction = self.upper_sort_direction_var.get()
            
        # Filter by selected exchange
        selected_exchange = self.exchange_var.get()
        exchanges_to_show = ['binance', 'bybit', 'okx'] if selected_exchange == 'all' else [selected_exchange]
        
        # Apply symbol filter
        symbol_filter = self.filter_var.get().upper()
        
        # Prepare data for display
        table_data = []
        
        for exchange in exchanges_to_show:
            if exchange in all_symbols:
                for symbol in all_symbols[exchange]:
                    # Apply symbol filter
                    if symbol_filter and symbol_filter not in symbol:
                        continue
                        
                    # Get price data
                    price_data = data_store.get_price_data(exchange, symbol)
                    if not price_data or 'bid' not in price_data or 'ask' not in price_data:
                        continue
                        
                    # Extract bid and ask prices for display
                    bid_price = price_data.get('bid', 'N/A')
                    ask_price = price_data.get('ask', 'N/A')                    
                    
                    # Get funding rate
                    funding_rate = data_store.get_funding_rate(exchange, symbol)
                    
                    # Get tick sizes
                    future_tick_size_raw = data_store.tick_sizes.get(exchange, {}).get(symbol, {}).get('future_tick_size', 'N/A')
                    spot_tick_size_raw = data_store.tick_sizes.get(exchange, {}).get(symbol, {}).get('spot_tick_size', 'N/A')
                    
                    # Initialize formatted tick sizes
                    future_tick_size = future_tick_size_raw
                    spot_tick_size = spot_tick_size_raw
                    
                    # Calculate tick sizes as percentages of average bid-ask
                    if bid_price != 'N/A' and ask_price != 'N/A':
                        try:
                            # Convert to float if they're strings
                            bid_value = float(bid_price) if isinstance(bid_price, str) else bid_price
                            ask_value = float(ask_price) if isinstance(ask_price, str) else ask_price
                            
                            if isinstance(bid_value, (int, float)) and isinstance(ask_value, (int, float)):
                                avg_price = (bid_value + ask_value) / 2
                                
                                # Future tick size percentage
                                if future_tick_size_raw != 'N/A':
                                    try:
                                        future_tick_value = float(future_tick_size_raw) if isinstance(future_tick_size_raw, str) else future_tick_size_raw
                                        if avg_price > 0:
                                            future_tick_pct = (future_tick_value / avg_price) * 100
                                            future_tick_size = f"{future_tick_pct:.6f}%"
                                    except (ValueError, TypeError):
                                        pass
                                
                                # Spot tick size percentage
                                if spot_tick_size_raw != 'N/A':
                                    try:
                                        spot_tick_value = float(spot_tick_size_raw) if isinstance(spot_tick_size_raw, str) else spot_tick_size_raw
                                        if avg_price > 0:
                                            spot_tick_pct = (spot_tick_value / avg_price) * 100
                                            spot_tick_size = f"{spot_tick_pct:.6f}%"
                                    except (ValueError, TypeError):
                                        pass
                        except (ValueError, TypeError):
                            pass
                    
                    # Get the spread vs spot
                    spread_vs_spot_raw = data_store.get_spread(exchange, symbol, 'vs_spot')
                    spread_vs_spot = f"{spread_vs_spot_raw:.6f}%" if isinstance(spread_vs_spot_raw, float) else 'N/A'
                    
                    # Get spreads vs other exchanges
                    spreads = {'binance': 'N/A', 'bybit': 'N/A', 'okx': 'N/A'}
                    spreads_raw = {'binance': float('nan'), 'bybit': float('nan'), 'okx': float('nan')}
                    
                    for other_exchange in ['binance', 'bybit', 'okx']:
                        if other_exchange == exchange:
                            continue
                            
                        # Get pre-calculated spread
                        spread_raw = data_store.get_spread(exchange, symbol, f'vs_{other_exchange}')
                        
                        # Format for display
                        if isinstance(spread_raw, float):
                            spreads[other_exchange] = f"{spread_raw:.6f}%"
                            spreads_raw[other_exchange] = spread_raw
                        else:
                            spreads[other_exchange] = 'N/A'
                            spreads_raw[other_exchange] = float('nan')
                    
                    # Get 24h change
                    change_24h = data_store.daily_changes.get(exchange, {}).get(symbol, 'N/A')
                    if change_24h != 'N/A':
                        change_24h = f"{change_24h:.2f}%"
                        
                    # Append to table data
                    table_data.append({
                        'symbol': symbol,
                        'exchange': exchange,
                        'bid': bid_price,
                        'ask': ask_price,                    
                        'funding_rate': funding_rate,
                        'spread_vs_spot': spread_vs_spot,
                        'spread_vs_binance': spreads['binance'],
                        'spread_vs_okx': spreads['okx'],
                        'spread_vs_bybit': spreads['bybit'],
                        'future_tick_size': future_tick_size if future_tick_size != 'N/A' else 'N/A',
                        'spot_tick_size': spot_tick_size if spot_tick_size != 'N/A' else 'N/A',
                        'change_24h': change_24h,
                        # Additional fields for sorting
                        'sort_funding_rate': self.extract_number(funding_rate),
                        'sort_spread_vs_spot': spread_vs_spot_raw if isinstance(spread_vs_spot_raw, float) else float('nan'),
                        'sort_spread_vs_binance': spreads_raw['binance'],
                        'sort_spread_vs_okx': spreads_raw['okx'],
                        'sort_spread_vs_bybit': spreads_raw['bybit'],
                        'sort_change_24h': self.extract_number(change_24h)
                    })
                    
        # Sort the data
        sort_mapping = {
            'Symbol': 'symbol',
            'Exchange': 'exchange',
            'Funding Rate': 'sort_funding_rate',
            'Spread vs Spot': 'sort_spread_vs_spot',
            'Spread vs Binance': 'sort_spread_vs_binance',
            'Spread vs OKX': 'sort_spread_vs_okx',
            'Spread vs Bybit': 'sort_spread_vs_bybit',
            '24h Change': 'sort_change_24h'
        }
        
        # Log sorting parameters for debugging
        logger.debug(f"Sorting by {sort_column} in {sort_direction} order")
        
        sort_key = sort_mapping.get(sort_column, 'symbol')
        reverse = (sort_direction == 'descending')
        
        # Sort with N/A values always at the bottom, regardless of sort direction
        table_data.sort(
            key=lambda x: (
                # First sorting key: 0 for normal values, 1 for N/A values
                # This ensures N/A always sorts after normal values
                1 if x[sort_key] == 'N/A' or (isinstance(x[sort_key], float) and math.isnan(x[sort_key])) else 0,
                # Second sorting key: the actual value for normal sorting
                # For N/A values, use a placeholder that won't affect the sort
                0 if x[sort_key] == 'N/A' or (isinstance(x[sort_key], float) and math.isnan(x[sort_key])) else
                (-x[sort_key] if reverse else x[sort_key])
            )
        )
        
        return table_data

    def update_data_table(self, force_upper=False, force_lower=False):
        """Update both data tables with rate limiting to prevent hangs"""
        # If both tables have mouse over and no force, don't update
        if (self.upper_mouse_over_table and not force_upper) and (self.lower_mouse_over_table and not force_lower):
            return
            
        # Rate limit updates - don't update too frequently unless forced
        current_time = time.time()
        if not (force_upper or force_lower) and hasattr(self, 'last_update_time') and current_time - self.last_update_time < 1.0:
            return  # Wait at least 1 second between updates
            
        self.last_update_time = current_time
        
        # Process this in a separate thread
        def prepare_data():
            try:
                # Get all symbols from all exchanges
                all_symbols = {}
                for exchange in ['binance', 'bybit', 'okx']:
                    symbols = data_store.get_symbols(exchange)
                    all_symbols[exchange] = set(symbols)
                    
                # Update upper table if not mouse over or forced
                if not self.upper_mouse_over_table or force_upper:
                    # Capture upper sort settings
                    upper_sort_column = self.upper_sort_column_var.get()
                    upper_sort_direction = self.upper_sort_direction_var.get()
                    
                    # Prepare upper table data
                    upper_table_data = self._prepare_table_data(all_symbols, upper_sort_column, upper_sort_direction)
                    
                    # Update UI in main thread
                    self.root.after(0, lambda: self._update_ui_with_data(self.upper_table, upper_table_data, "upper"))
                
                # Update lower table if not mouse over or forced
                if not self.lower_mouse_over_table or force_lower:
                    # Capture lower sort settings
                    lower_sort_column = self.lower_sort_column_var.get()
                    lower_sort_direction = self.lower_sort_direction_var.get()
                    
                    # Prepare lower table data
                    lower_table_data = self._prepare_table_data(all_symbols, lower_sort_column, lower_sort_direction)
                    
                    # Update UI in main thread
                    self.root.after(0, lambda: self._update_ui_with_data(self.lower_table, lower_table_data, "lower"))
                    
            except Exception as e:
                logger.error(f"Error preparing table data: {e}")
                
        # Use a background thread for data preparation
        self.executor.submit(prepare_data)

    def _update_ui_with_data(self, table, table_data, table_id):
        """Update the specified UI table with the prepared data"""
        try:
            self._optimize_table_update(table, table_data)
            
            # Update status
            status_text = f"Showing {len(table_data)} symbols in {table_id} table"
            if self.filter_var.get():
                status_text += f" (filtered by '{self.filter_var.get().upper()}')"
                
            logger.info(status_text)
        except Exception as e:
            logger.error(f"Error updating {table_id} table with data: {e}")

    def _optimize_table_update(self, table, new_data):
        """Rebuild the specified table completely to ensure correct sorting"""
        try:
            # Get current items for later cleanup
            current_items = set(table.get_children())
            
            # Clear the entire table to ensure proper order
            for item_id in current_items:
                table.delete(item_id)
            
            # Insert all items in the correct (sorted) order
            for i, item_data in enumerate(new_data):
                item_id = f"{item_data['exchange']}_{item_data['symbol']}"
                values = (
                    item_data['symbol'],
                    item_data['exchange'],
                    item_data['bid'],
                    item_data['ask'],
                    item_data['funding_rate'],
                    item_data['spread_vs_spot'],
                    item_data['spread_vs_binance'],
                    item_data['spread_vs_okx'],
                    item_data['spread_vs_bybit'],
                    item_data['future_tick_size'],
                    item_data['spot_tick_size'],
                    item_data['change_24h']
                )
                # Insert at the end to maintain sorted order
                table.insert('', 'end', iid=item_id, values=values,
                             tags=('even' if i % 2 == 0 else 'odd',))
        except Exception as e:
            logger.error(f"Error updating table: {e}")

    def extract_number(self, value):
        """Extract numeric value from formatted string for sorting"""
        if value == 'N/A':
            return float('nan')
            
        try:
            # Remove % sign and convert to float
            return float(value.replace('%', ''))
        except (ValueError, AttributeError):
            return float('nan')

    def start_exchange_threads(self):
        """Start data collection threads for the selected exchange"""
        selected_exchange = self.exchange_var.get()
        
        # Fetch symbols first for all exchanges
        if selected_exchange == "all" or selected_exchange == "binance":
            self.binance.fetch_symbols()
            self.binance.fetch_spot_symbols()
            
        if selected_exchange == "all" or selected_exchange == "bybit":
            # self.bybit.fetch_symbols()
            # self.bybit.fetch_spot_symbols()
            self.bybit.initialize()  # This will handle everything in the proper sequence
            
        if selected_exchange == "all" or selected_exchange == "okx":
            self.okx.fetch_symbols()
            self.okx.fetch_spot_symbols()
            
        # Short delay to ensure symbols are loaded
        time.sleep(1)
        
        # Now connect WebSockets and start other threads
        if selected_exchange == "all" or selected_exchange == "binance":
            # Start Binance threads
            self.binance.connect_futures_websocket()
            self.binance.connect_spot_websocket()
            active_threads["binance_funding"] = self.executor.submit(self.binance.update_funding_rates)
            active_threads["binance_changes"] = self.executor.submit(self.binance.update_24h_changes)
            threading.Timer(30, self.binance.check_symbol_freshness).start()        
            
        if selected_exchange == "all" or selected_exchange == "bybit":
            # Start Bybit threads
            self.bybit.connect_websocket()
            self.bybit.connect_spot_websocket()
            active_threads["bybit_funding"] = self.executor.submit(self.bybit.update_funding_rates)
            active_threads["bybit_changes"] = self.executor.submit(self.bybit.update_24h_changes)       
            
        if selected_exchange == "all" or selected_exchange == "okx":
            # Start OKX threads
            self.okx.connect_websocket()
            self.okx.connect_spot_websocket()
            active_threads["okx_funding"] = self.executor.submit(self.okx.update_funding_rates)
            active_threads["okx_changes"] = self.executor.submit(self.okx.update_24h_changes)
    def restart_exchange_threads(self):
        """Stop and restart exchange data threads with better shutdown"""
        logger.info("Restarting exchange threads...")
        
        # Stop all current threads
        global stop_event
        stop_event.set()
        
        # Shut down executor
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
            
        # Close all WebSocket connections
        for exchange in [self.binance, self.bybit, self.okx]:
            for name, manager in exchange.websocket_managers.items():
                if hasattr(manager, 'disconnect'):
                    manager.disconnect()
                    
        # Clear active threads dictionary
        active_threads.clear()
        
        # Reset stop event flag
        stop_event.clear()
        
        # Clear data caches to prevent stale data
        with data_store.lock:
            for exchange in data_store.price_data:
                data_store.price_data[exchange].clear()
                data_store.update_counters[exchange] = 0
                
        # Create a new executor
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=15)
        
        # Start new threads                                
        logger.info("Starting new exchange threads...")
        self.start_exchange_threads()

    def start_health_monitor(self):
        """Start a thread to monitor the health of connections"""
        def health_monitor_worker():
            while not stop_event.is_set():
                try:
                    # Check all WebSocket connections
                    for exchange in [self.binance, self.bybit, self.okx]:
                        for name, manager in exchange.websocket_managers.items():
                            if hasattr(manager, 'check_health'):
                                manager.check_health()
                                
                    # Log data freshness statistics occasionally
                    if random.random() < 0.05:  # Log 5% of the time
                        exchange_stats = {}
                        current_time = time.time()
                        
                        for exchange in ['binance', 'bybit', 'okx']:
                            fresh_count = 0
                            stale_count = 0
                            
                            with data_store.lock:
                                for symbol, data in data_store.price_data[exchange].items():
                                    if 'timestamp' in data:
                                        if current_time - data['timestamp'] < 30:
                                            fresh_count += 1
                                        else:
                                            stale_count += 1
                                            
                            exchange_stats[exchange] = f"{fresh_count} fresh, {stale_count} stale"
                            
                        logger.info(f"Data freshness: Binance: {exchange_stats['binance']}, "
                                  f"Bybit: {exchange_stats['bybit']}, OKX: {exchange_stats['okx']}")
                except Exception as e:
                    logger.error(f"Error in health monitor: {e}")
                    
                # Check every 5 seconds
                for _ in range(5):
                    if stop_event.is_set():
                        break
                    time.sleep(1)
                    
        self.health_monitor = threading.Thread(
            target=health_monitor_worker,
            daemon=True,
            name="health_monitor"
        )
        self.health_monitor.start()
        active_threads["health_monitor"] = self.health_monitor

    def schedule_updates(self):
        """Schedule periodic UI updates and maintenance tasks"""
        # Clean old data periodically
        def clean_old_data():
            data_store.clean_old_data()
            self.root.after(300000, clean_old_data)  # 5 minutes
            
        # Update the data tables periodically
        def update_tables():
            self.update_data_table()
            self.root.after(500, update_tables)  # Update every 500ms
            
        # Start the scheduled functions
        clean_old_data()
        update_tables()


