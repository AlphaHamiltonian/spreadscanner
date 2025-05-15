# Crypto Exchange Monitor

A modular, extensible Python application that streams real‑time market data from multiple cryptocurrency exchanges (Binance, Bybit, OKX) and presents it in a Tkinter desktop GUI.  It is designed for **low‑latency monitoring**, rapid expansion (adding new exchanges or analytics), and clean separation of concerns.

---

## ✨ Key Features

| Category                 | Highlights                                                                                    |
| ------------------------ | --------------------------------------------------------------------------------------------- |
| **Real‑time data**       | Order‑book top‑of‑book (bid/ask), funding rates, 24‑hour change, tick sizes, computed spreads |
| **Multi‑exchange**       | Built‑in connectors: Binance Futures & Spot, Bybit Linear Futures + Spot, OKX SWAP & Spot     |
| **Threaded & resilient** | Auto‑reconnect WebSockets, health checks, heartbeat pings, stale‑data detection               |
| **Central data model**   | Thread‑safe `DataStore` shared by connectors and UI                                           |
| **Extensible**           | Add a new exchange by dropping a new connector module in `exchange_monitor/exchanges/`        |
| **Desktop GUI**          | Sortable tables, symbol filter, CSV export, live refresh controls                             |
| **Configurable logging** | File + console logging via standard `logging` config                                          |

---

## 🗂️ Project Structure

```text
spreadscanner/
├── source/          # Python package
│   ├── __init__.py            
│   ├── config.py               # Global settings & logging config
│   ├── utils.py                 # Re‑usable helpers
│   ├── exchanges/             # One module per exchange
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── binance.py
│   │   ├── bybit.py
│   │   └── okx.py
│   └── ui.py                    # Tkinter GUI
├── run.py                     # Entry‑point script
├── requirements.txt           # Pip dependencies
└── README.md                  # (this file)
```

### Folder Responsibilities

* **`utils.py`** – generic infrastructure (WebSocket handling, HTTP sessions, symbol mapping, shared data).
* **`exchanges/`** – one connector per exchange; inherits from `BaseExchangeConnector`.
* **`ui.py`** – all user‑interface code; currently a single `ExchangeMonitorApp` but easy to split.
* **`config.py`** – central place for tunables (API URLs, intervals, log levels, global `stop_event`).
* **`run.py`** – thin launcher: sets up logging, creates `tk.Tk()`, instantiates `ExchangeMonitorApp`, starts `mainloop()`.

---

## ⚡ Quick Start

> **Prerequisites**: Python 3.10 or higher.

```bash
# 1. Clone repository
$ git clone git@github.com:AlphaHamiltonian/spreadscanner.git
$ cd spreadscanner

# 2. Install dependencies (ideally in a venv)
$ python -m venv .venv && source .venv/bin/activate
$ pip install -r requirements.txt

# 3. Run the desktop app
$ python run.py
```

A window will open showing live data.  Use the **Controls** panel (top) to switch exchanges, filter symbols, or export CSV.

---

## 🔧 Configuration

All tunables live in **`spreadscanner/config.py`**. Key items:

```python
# API & WebSocket endpoints
BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"
BYBIT_LINEAR_WS    = "wss://stream.bybit.com/v5/public/linear"
OKX_PUBLIC_WS      = "wss://ws.okx.com/ws/v5/public"

# Timing thresholds (seconds)
PING_INTERVAL  = 30
PONG_TIMEOUT   = 10
ACTIVITY_LIMIT = 60

# Global thread event
import threading
stop_event = threading.Event()
```

Adjust endpoints, heartbeat intervals, or stale‑data thresholds here. `stop_event` is imported by every long‑running thread so the whole app can exit cleanly.

*Sensitive data*: if future exchanges need API keys, **never hard‑code** them. Read from environment variables in `config.py` (e.g., `os.getenv("BINANCE_API_KEY")`).

---

## ➕ Adding a New Exchange

1. **Create** `exchange_monitor/exchanges/kraken.py` (example name).
2. **Subclass** `BaseExchangeConnector` and implement:

   * `fetch_symbols()` ➜ collect tradable pairs via REST.
   * `connect_websocket()` ➜ open WS, subscribe, process messages.
   * Optionally `update_funding_rates()` / `update_24h_changes()`.
3. **Import & instantiate** it inside `ExchangeMonitorApp` (UI) – e.g., `self.kraken = KrakenConnector(self)` – and add to the exchange‑selection dropdown.
4. Update any config constants if needed.

Because every connector uses **`WebSocketManager`**, **`HttpSessionManager`**, and writes to **`DataStore`**, you get health‑checks, central caching, and UI updates for free.

---

## 📝 Logging

Logging is configured in `config.py` (or `logging.conf` if you prefer an INI).  By default the app logs to:

* **Console** at `INFO` level (concise runtime info)
* **`exchange_monitor.log`** at `DEBUG` level (detailed diagnostics)

Change the levels/handlers in the config to suit production or dev needs.

---

## 🤝 Contributing

1. Fork the repo and create your branch: `git checkout -b feature/my‑feature`.
2. Run `pre‑commit` hooks or `flake8` to keep style consistent (optional but recommended).
3. Add tests in `tests/` when applicable.
4. Submit a PR; please describe **what / why** and link to issues.

---

## 📄 License

[MIT](LICENSE) — feel free to use, modify, and redistribute with attribution.

---

## 📌 TODO / Roadmap

* [ ] Support additional exchanges (Kraken, Coinbase, Bitget)
* [ ] Plug‑in analytics (e.g., volatility, VWAP, custom alerts)
* [ ] Configurable WebSocket batch sizes via GUI
* [ ] Optional headless mode (CLI with `rich` tables)
* [ ] Dockerfile + GH Actions build matrix

> Questions or suggestions? Open an issue or reach out on GitHub!
