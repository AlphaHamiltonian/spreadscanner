Great. I’ll update your README in the same format, improving clarity and completeness based on the codebase. I’ll also provide specific suggestions to improve your real-time crypto monitoring app for both traders and developers.
I’ll let you know when the updated README and recommendations are ready.


**Part 1: Revised README.md**

# Crypto Exchange Monitor

A modular, extensible Python application that streams real‑time market data from multiple cryptocurrency exchanges (Binance, Bybit, OKX) and presents it in a Tkinter desktop GUI. It is designed for **low‑latency monitoring** with a multi-threaded architecture, rapid expansion (adding new exchanges or analytics), and clean separation of concerns. The interface features a dual-table display for side-by-side views of market data and spreads, with robust connectivity and data normalization (funding rates, tick sizes, etc.) to ensure consistent, accurate real-time monitoring.

---

## ✨ Key Features

| Category                 | Highlights                                                                                                                                                                                                                                                                                                 |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Real‑time data**       | Order‑book top‑of‑book (bid/ask) updates streamed via WebSockets, **funding rates**, 24‑hour price change, **tick sizes** (normalized as % of price), and computed spreads (futures vs spot, cross-exchange).                                                                                              |
| **Multi‑exchange**       | Built‑in connectors for multiple exchanges: **Binance** (Futures + Spot), **Bybit** (Linear Futures + Spot), **OKX** (Perpetual Swap + Spot). Easily monitor one exchange or **aggregate all** simultaneously.                                                                                             |
| **Threaded & resilient** | Multi-threaded design keeps the UI responsive. Each exchange runs on background threads with auto‑reconnect WebSockets, periodic **heartbeat pings**, and health checks. **Stale-data detection** (timestamps) marks or skips outdated quotes, and automatic reconnection handles network drops.           |
| **Central data model**   | Thread-safe `DataStore` caches all symbols and prices. Connectors and the GUI share this centralized store for real-time updates (with locks to prevent race conditions). Pre-calculated spreads are stored here for efficient UI refresh.                                                                 |
| **Extensible**           | Add new exchanges by simply dropping a connector module into `source/exchanges/` and subclassing `BaseExchangeConnector`. The app auto-integrates new data feeds (leveraging shared WebSocket and HTTP utilities).                                                                                         |
| **Desktop GUI**          | **Dual sortable tables** (simultaneously view two sorted views of the data), interactive symbol filter, one-click CSV export of current data, and live refresh controls. Tables support column sorting (with separate sort settings per table) and highlight positive/negative changes for quick scanning. |
| **Configurable logging** | Logging to both file and console (built on Python `logging` config). Info-level console output for runtime events, and a debug-level log file (`exchange_monitor.log`) for detailed diagnostics.                                                                                                           |

---

## 🗂️ Project Structure

```text
spreadscanner/
├── source/                 # Python package with application code
│   ├── __init__.py
│   ├── config.py           # Global settings, logging configuration, stop_event, thread registry
│   ├── utils.py            # Reusable helpers (WebSocket manager, HTTP session, data store, symbol normalization)
│   ├── exchanges/          # One module per exchange connector
│   │   ├── __init__.py
│   │   ├── base.py         # BaseExchangeConnector class (common WebSocket/REST logic)
│   │   ├── binance.py      # Binance connector (futures & spot)
│   │   ├── bybit.py        # Bybit connector (linear futures & spot)
│   │   └── okx.py          # OKX connector (swap & spot)
│   └── ui.py               # Tkinter GUI (ExchangeMonitorApp, tables, controls)
├── run.py                  # Application entry point (initializes GUI and starts threads)
├── requirements.txt        # Pip dependencies
└── README.md               # (this file)
```

### Module Responsibilities

* **`utils.py`** – Infrastructure helpers, including `WebSocketManager` (auto-reconnecting WebSocket client with ping/pong), `HttpSessionManager` (requests session with retries and rate limiting), `DataStore` (central thread-safe storage for prices, funding, spreads, etc.), symbol normalization utilities, and periodic data cleanup.
* **`exchanges/`** – Exchange-specific connectors. Each inherits `BaseExchangeConnector` and implements methods to fetch symbol lists, subscribe to WebSocket streams, and periodically retrieve ancillary data (funding rates, 24h changes). These connectors maintain exchange-specific WebSocket connections and update the shared `DataStore`.
* **`ui.py`** – All Tkinter UI code in the `ExchangeMonitorApp` class. Manages the main window, **two Treeview tables** (market data views), control panel (exchange selector, refresh, filter, export), and periodic UI refresh scheduling. Also handles sorting logic and user interactions (e.g., filter input, CSV export dialog).
* **`config.py`** – Central configuration for global objects and logging. Defines a single `stop_event` (threading.Event) that all threads check for a clean shutdown signal, and an `active_threads` registry for tracking background threads (for optional joins or debugging).
* **`run.py`** – Thin launcher script. It sets up logging, initializes the Tkinter root window and `ExchangeMonitorApp`, spawns background threads (e.g. a spread calculation loop at \~5 Hz), and starts the Tk mainloop. It also catches SIGINT/SIGTERM to signal threads to stop and ensures graceful exit (closing sockets, etc.) when the GUI closes.

---

## ⚡ Quick Start

> **Prerequisites**: Python 3.10 or higher (for modern syntax and library support).

```bash
# 1. Clone repository
$ git clone git@github.com:AlphaHamiltonian/spreadscanner.git
$ cd spreadscanner

# 2. Install dependencies (preferably in a virtual environment)
$ python -m venv .venv && source .venv/bin/activate
$ pip install -r requirements.txt

# 3. Run the desktop app
$ python run.py
```

After launch, a window will open showing live order book data and metrics in two tables. Use the **Controls** panel at the top to select an exchange (or "all" for aggregated view), filter by symbol (search substring), or export the currently displayed data to CSV. The **Refresh Data** button triggers an immediate data refresh, and data updates also occur automatically multiple times per second. You can sort any column by clicking its header (each table can have its own sort). When you hover over a table, auto-refresh pauses to allow easy scrolling or copying data, and resumes when you move the cursor away.

---

## 🔧 Configuration

Most runtime parameters can be tuned in **`source/config.py`** or within connectors as appropriate. Key settings and behaviors include:

* **WebSocket & API Endpoints:** Each exchange’s connector defines the WebSocket URLs and any REST endpoints for funding rates or tickers. For example, Binance uses `wss://fstream.binance.com/stream` for futures data. These can be adjusted if endpoints change or for testing (e.g., pointing to testnet URLs).
* **Heartbeat/Ping Intervals:** The `WebSocketManager` defaults (30s ping interval, 10s pong timeout) ensure connections stay alive. The health monitor will send pings (for exchanges that require it) and reconnect if pongs aren’t received within a threshold.
* **Stale Data Thresholds:** In the spread calculations, futures prices older than \~5 seconds or spot prices older than \~10 seconds are considered stale and their spread computations are skipped (displayed as “N/A”). Additionally, a background health thread logs a warning if over 10% of symbols on any exchange haven’t updated in >60s.
* **Logging Levels:** By default, console logging is set to INFO and file logging to DEBUG. You can adjust these in `config.py` by modifying the `logging.config.dictConfig` call (or swap in a custom logging config file). This controls how verbose the output is, which is useful for debugging connectivity issues or performance.

**Note:** If any exchange requires authentication (e.g. future extensions for user-specific data), never hard-code API keys in the code. Instead, read credentials from environment variables or a secure config. The current application only uses public market data endpoints.

---

## ➕ Adding a New Exchange

1. **Create a connector module:** e.g. `source/exchanges/kraken.py`. Subclass `BaseExchangeConnector`.
2. **Implement required methods:**

   * `fetch_symbols()` – Retrieve the list of tradable instrument symbols via REST (both futures/swap and spot if applicable). Also load any instrument metadata like tick size.
   * `connect_websocket()` (and similar methods if splitting futures/spot) – Establish WebSocket connection(s), subscribe to relevant channels (top-of-book quotes, etc.), and handle incoming messages (update `DataStore` with bids/asks).
   * If available, implement `update_funding_rates()` to periodically fetch funding rate data (via REST if not provided in WS stream) and `update_24h_changes()` to fetch daily price changes. These run in background threads.
3. **Integrate with the UI:** Import and instantiate your connector in the `ExchangeMonitorApp` constructor (similar to how Binance/Bybit/OKX are added). Then include it in the exchange selection dropdown (e.g., add "kraken" to the list and handle the "all" case accordingly).
4. **Adjust settings if needed:** Add any new API endpoints or special timing constants in `config.py` or within your connector. The shared WebSocket/HTTP managers and DataStore will automatically integrate your exchange’s data feed into the GUI.

Thanks to the common `WebSocketManager` and `DataStore`, your new connector benefits from automatic heartbeats, centralized caching, and UI updates out-of-the-box. The GUI will immediately start displaying the new exchange’s data once the connector is wired up and running.

---

## 📝 Logging

The application uses Python’s built-in logging framework. Logging is configured in `config.py` (via a dictConfig setup). By default, the app logs to:

* **Console:** at INFO level – providing a high-level runtime log (connections established, major events, warnings).
* **Log file (`exchange_monitor.log`):** at DEBUG level – containing detailed information useful for development or troubleshooting (e.g., incoming data, reconnect attempts, exceptions).

You can tailor the logging levels or add handlers (e.g., rotating file handler, external logging service) by editing the config. This helps in production environments to balance verbosity vs. information needed for monitoring the app’s health.

---

## 🤝 Contributing

Contributions are welcome! To get started:

1. Fork the repo and create a feature branch: `git checkout -b feature/my-new-feature`.
2. Ensure code style and quality by running `flake8` or other linters. (Optional: set up pre-commit hooks for automatic formatting on commit.)
3. Add tests in a `tests/` directory if adding new functionality, especially for any critical data processing logic.
4. Open a Pull Request with a clear description of the changes and the motivation (link to any relevant issues). Please include details on how to reproduce or test new features.

All contributions should adhere to the project’s coding style and aim to maintain or improve the clarity and performance of the code. Discussion through issues or the PR is encouraged to refine the feature or fix.

---

## 📄 License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and distribute this software. Contributions you make will also fall under the MIT license.

---

## 📌 Roadmap

* [ ] **Additional Exchanges:** Extend coverage to more exchanges (e.g., Kraken, Coinbase, Bitget) by writing new connector modules. The architecture is in place to support many concurrent feeds.
* [ ] **Analytics & Alerts:** Plug-in custom analytics (volatility calculations, VWAP, arbitrage alerts). For example, user-defined alerts when a cross-exchange spread exceeds a threshold, or visual indicators for significant funding rate changes.
* [ ] **Enhanced UI Controls:** GUI options to adjust WebSocket subscription batch sizes or update frequencies on the fly for performance tuning. Possibly allow pausing specific exchange streams or limiting the number of symbols shown.
* [ ] **Headless/CLI Mode:** An optional command-line interface (using rich tables or console output) for running the monitor on a server or terminal without the Tkinter GUI.
* [ ] **Docker Deployment:** Provide a Dockerfile and CI workflow for easy deployment. This would allow running the monitor in a containerized environment (with X11 forwarding or VNC for the GUI, or using the headless mode when implemented).

> Have an idea or request? Please open an issue or start a discussion on the GitHub repo. We aim to make **Crypto Exchange Monitor** more powerful and user-friendly for the trading and developer community!

**Part 2: Improvement Recommendations**

* **For Traders:**

  * *Custom Alerting & Notifications:* Add the ability to set alerts on certain conditions (e.g. price spread above a threshold, drastic 24h change, funding rate spikes). This real-time notification system (popup or email/SMS) would help traders react immediately to market opportunities or risks.
  * *UI Enhancements:* Introduce a customizable interface with options like dark mode and column selection. Traders could enable/disable columns (volume, open interest, etc.) or switch to a dark theme for better visibility during night trading. Color-coded highlights (e.g. green/red for positive/negative spreads or changes) are partially implemented but could be extended for clearer visual cues.
  * *Asset Grouping & Multi-Window Views:* Allow grouping of symbols by underlying asset or opening multiple windows/tabs for different exchanges. For example, a trader could view all BTC pairs across exchanges side-by-side, simplifying cross-exchange arbitrage monitoring. Alternatively, enable a split view where each table is locked to a specific exchange or asset category for focused analysis.
  * *Performance Tuning Options:* Provide controls to adjust data refresh rate or limit the number of symbols displayed when monitoring in real time. Traders with slower machines or monitoring many markets could lower the update frequency or apply pre-defined watchlists, ensuring the UI remains smooth and latency stays low even with large data volumes.

* **For Developers:**

  * *Modular Exchange Plug-ins:* Refactor exchange connectors to be loaded via a plugin mechanism or configuration file. Instead of modifying the core UI for each new exchange, developers could drop in a new module and register it in a config, and the app would automatically discover and load it. This decoupling would make the system more extensible and encourage community-contributed exchange modules.
  * *API and Integration Hooks:* Expose a programmatic interface for the data (e.g., a local HTTP API or WebSocket server that mirrors the data feed). Developers could then integrate the monitor’s real-time data with other tools, like algorithmic trading systems or custom dashboards. This turns the app into a real-time data hub for broader use cases beyond the GUI.
  * *Enhanced Monitoring & Testing Tools:* Implement internal diagnostics and testing modes. For example, a debug panel or log viewer in the GUI to track thread performance (latency of updates, dropped connections count) would help in debugging. Additionally, provide a simulation mode where recorded market data can be replayed — this would allow developers to test the app’s behavior deterministically or run automated tests without live connections.
  * *Performance Optimization & Profiling:* Introduce profiling and optimization for the UI update loop and data processing. For instance, rather than rebuilding entire tables every 500ms, developers might optimize by updating only changed rows or using Tkinter virtualization for large tables. Providing profiling hooks or documentation on performance bottlenecks (like Python GIL considerations, network I/O vs UI thread) would assist developers in optimizing the app for high-frequency data scenarios.
  * *Developer Documentation & Tooling:* Expand the documentation to include guidelines on writing new connectors (with code examples) and using the app in different environments. Setting up continuous integration (CI) to run linters and unit tests for connectors can ensure code quality. Additionally, containerizing development environments (via Docker Compose for a dev setup with dummy data feeders) could make it easier for multiple developers to collaborate on real-time features without needing all exchanges’ connectivity during development.
