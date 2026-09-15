"""
Configuration for production FastAPI backend
"""
import os
from pathlib import Path
from backend.market_universe import (
	DEFAULT_TRACKED_SYMBOLS, EQUITY_CONTEXT_SYMBOLS, EXPANDED_OPRA_SYMBOLS,
	resolve_equity_context_symbols,
)

try:
	from dotenv import load_dotenv
	load_dotenv()
except ImportError:
	pass

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
MODELS_DIR = BASE_DIR / "models"
LOGS_DIR = BASE_DIR / "logs"

# Create directories
DATA_DIR.mkdir(exist_ok=True)
MODELS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# Database
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR / 'market_data.db'}")

# Market Data API
POLYGON_API_KEY = os.getenv("Massive_API") or os.getenv("POLYGON_API_KEY")
# API key will be validated when streaming starts, not at import time
DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
MARKET_DATA_PROVIDER = os.getenv("MARKET_DATA_PROVIDER", "auto").lower()

# Symbols to track
SYMBOLS = list(DEFAULT_TRACKED_SYMBOLS)
DATABENTO_SYMBOLS = [
	symbol.strip().upper()
	for symbol in os.getenv("DATABENTO_SYMBOLS", "SPX,NDX,VIX").split(",")
	if symbol.strip()
]
DATABENTO_SUPPORTED_SYMBOLS = [
	"SPX", "NDX", "SPY", "QQQ", "DIA", "IWM", "XSP", "XND", "RUT", "MRUT", "VIX", "OEX", "DJX", "RUI", "XAU", "HGX", "OSX", "UTY"
]
# Staged profile only: changing support never mutates a running subscription.
DATABENTO_EXPANDED_SYMBOLS = list(EXPANDED_OPRA_SYMBOLS)
DATABENTO_EQUITY_CONTEXT_ENABLED = os.getenv("DATABENTO_EQUITY_CONTEXT_ENABLED", "0") == "1"
DATABENTO_EQUITY_CONTEXT_SYMBOLS = resolve_equity_context_symbols(
	os.getenv("DATABENTO_EQUITY_CONTEXT_SYMBOLS", ",".join(EQUITY_CONTEXT_SYMBOLS))
)
DATABENTO_STABLE_SYMBOLS = ["SPX", "NDX", "VIX"]
DATABENTO_REQUIRED_SYMBOLS = [
	symbol.strip().upper()
	for symbol in os.getenv("DATABENTO_REQUIRED_SYMBOLS", "SPX,NDX").split(",")
	if symbol.strip()
]

# WebSocket settings
WS_RECONNECT_DELAY = 5  # seconds
WS_PING_INTERVAL = 30  # seconds
MAX_BUFFER_SIZE = 10000  # max data points in memory

# CUDA settings
CUDA_ENABLED = True
INFERENCE_BATCH_SIZE = 1  # Real-time inference
MODEL_PRECISION = "fp16"  # Use half precision for speed

# Performance targets (institutional grade)
MAX_INFERENCE_LATENCY_MS = 100
MAX_API_RESPONSE_MS = 50
TARGET_UPTIME_PCT = 99.9
LIVE_DATA_STALE_AFTER_SECONDS = float(os.getenv("LIVE_DATA_STALE_AFTER_SECONDS", "30"))
LIVE_DATA_WARN_AFTER_SECONDS = float(os.getenv("LIVE_DATA_WARN_AFTER_SECONDS", "15"))

# API settings
API_HOST = "0.0.0.0"
API_PORT = 8000
CORS_ORIGINS = ["*"]  # Restrict in production

# Rate limiting
RATE_LIMIT_PER_MINUTE = 600  # Much higher for local

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
