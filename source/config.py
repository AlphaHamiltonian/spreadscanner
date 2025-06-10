import threading
import logging.config
import pathlib

LOG_FILE = pathlib.Path.cwd() / "exchange_monitor.log"

# Consolidate thresholds into a single config object
class Config:
    # Spread monitoring thresholds
    FUTURES_THRESHOLD = 1
    SPOT_THRESHOLD = 1
    DIFFERENCE_THRESHOLD = 1
    UPPER_LIMIT = 1
    LOWER_LIMIT = -3
    DELETE_OLD_TIME = 10
    NUMBER_OF_SEC_THRESHOLD = 3
    NUMBER_OF_SEC_THRESHOLD_TRADE = 3
    FUNDING_RATE_THRESHOLD = 0.9
    
    # Runtime flags
    TELEGRAM_ENABLED = False

def init_logging(level: str = "INFO", *, logfile: str | pathlib.Path = LOG_FILE) -> None:
    fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"std": {"format": fmt}},
        "handlers": {
            "console": {"class": "logging.StreamHandler", "level": level, "formatter": "std"},
            "file": {"class": "logging.FileHandler", "level": "DEBUG", "filename": str(logfile), "formatter": "std"},
        },
        "root": {"level": level, "handlers": ["console", "file"]},
    })

# Global objects
stop_event: threading.Event = threading.Event()
active_threads: dict[str, threading.Thread] = {}