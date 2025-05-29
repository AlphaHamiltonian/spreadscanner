"""exchange_monitor.config.config
================================
Minimal global configuration shared by **every** module in the project.

Only two objects are defined because they **must** be single‑instance
across the entire process:

1. `stop_event` — a `threading.Event` that signals all background loops
   (WebSocket threads, REST polling, UI timers) to terminate.
2. `active_threads` — a dictionary where each module can register the
   `threading.Thread` objects it starts so the main script can `join()`
   them on shutdown.

Keeping them here avoids circular imports and ensures every part of the
application references the *same* objects.
"""
from __future__ import annotations

import threading
from typing import MutableMapping
# source/config/logging_config.py
import logging.config
import pathlib

LOG_FILE = pathlib.Path.cwd() / "exchange_monitor.log"
        # Set thresholds (configurable)
FUTURES_THRESHOLD = 50  # Futures: 2 seconds
SPOT_THRESHOLD = 50    # Spot: 10 seconds
DIFFERENCE_THRESHOLD = 1
UPPER_LIMIT = 1
LOWER_LIMIT = -9
DELETE_OLD_TIME = 10
NUMBER_OF_SEC_THRESHOLD = 5
NUMBER_OF_SEC_THRESHOLD_TRADE =2
FUNDING_RATE_THRESHOLD = 0.9
TELEGRAM_ENABLED = False  # Will be set dynamically based on running mode

def init_logging(level: str = "INFO", *, logfile: str | pathlib.Path = LOG_FILE) -> None:
    fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"std": {"format": fmt}},
            "handlers": {
                "console": {"class": "logging.StreamHandler", "level": level, "formatter": "std"},
                "file": {"class": "logging.FileHandler", "level": "DEBUG", "filename": str(logfile), "formatter": "std"},
            },
            "root": {"level": level, "handlers": ["console", "file"]},
        }
    )

# ─────────────────────────────────────────────────────────────────────────────
# 🛑 Global Stop Event
# ----------------------------------------------------------------------------
# Example usage inside a connector loop:
#     while not stop_event.is_set():
#         ... do work ...
# ─────────────────────────────────────────────────────────────────────────────
stop_event: threading.Event = threading.Event()

# ─────────────────────────────────────────────────────────────────────────────
# 🧵 Registry of background threads
# ----------------------------------------------------------------------------
# Optional but handy for diagnostics / clean shutdown.
# Example:
#     t = threading.Thread(target=worker, daemon=True)
#     t.start()
#     active_threads["binance_funding"] = t
# ----------------------------------------------------------------------------
active_threads: MutableMapping[str, threading.Thread] = {}

__all__ = ["stop_event", "active_threads"]
