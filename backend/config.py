"""Shared configuration for the Databento live backend."""

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
MAX_BUFFER_SIZE = int(os.getenv("MAX_BUFFER_SIZE", "5400"))
LIVE_DATA_STALE_AFTER_SECONDS = int(
    os.getenv("LIVE_DATA_STALE_AFTER_SECONDS", "15")
)
MARKET_DATA_PROVIDER = os.getenv("MARKET_DATA_PROVIDER", "databento")
SYMBOLS = [
    symbol.strip().upper()
    for symbol in os.getenv("DATABENTO_GAMMA_SYMBOLS", "SPX,NDX,RUT").split(",")
    if symbol.strip()
]
