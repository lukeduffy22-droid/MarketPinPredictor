"""
Application settings using pydantic-settings for type-safe configuration.
Loads from environment variables with fail-fast validation.
"""
from pydantic_settings import BaseSettings
from pydantic import Field, AliasChoices
from typing import Optional, List
import os


def _pick_first_non_databento_key(*candidates: Optional[str]) -> str:
    """Pick first non-empty API key that is not a Databento key (db-*)."""
    for candidate in candidates:
        value = (candidate or "").strip()
        if not value:
            continue
        if value.startswith("db-"):
            continue
        return value
    return ""

class Settings(BaseSettings):
    """Application configuration"""
    
    # API keys
    databento_api_key: str = Field(
        default="",
        validation_alias=AliasChoices("DATABENTO_API_KEY"),
    )
    databento_symbols: str = Field(
        default="SPX,NDX,DJI,RUT",
        validation_alias=AliasChoices("DATABENTO_SYMBOLS", "TRACKED_SYMBOLS"),
    )
    polygon_api_key: str = Field(
        default="",
        validation_alias=AliasChoices(
            "Massive_API",
            "MASSIVE_API_KEY",
            "POLYGON_API_KEY",
            "POLYGON_STREAMING_KEY",
        ),
    )
    database_url: Optional[str] = None
    
    # Environment
    env: str = "dev"
    log_level: str = "INFO"
    
    # Ring buffer configuration
    ring_secs: int = 5400  # 90 minutes
    
    # Prediction recompute cadence (milliseconds) by time bucket
    recompute_ms_1500_1530: int = 60000  # 60 seconds before 3:30 PM
    recompute_ms_1530_1545: int = 30000  # 30 seconds 3:30-3:45 PM
    recompute_ms_1545_1600: int = 15000  # 15 seconds 3:45-4:00 PM (power hour)
    
    # Performance thresholds
    max_predict_latency_ms: int = 200  # p99 latency target
    max_memory_delta_mb: int = 100  # Maximum memory growth per session
    
    # Freshness requirements
    max_tick_age_seconds: int = 5  # Maximum age for index ticks during RTH
    min_ring_length_seconds: int = 300  # Minimum data required for predictions
    
    # WebSocket configuration
    ws_max_reconnects: int = 5
    ws_backoff_base: float = 2.0  # Exponential backoff base
    ws_backoff_jitter: float = 0.3  # Jitter fraction
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"
        
    def __init__(self, **kwargs):
        # Load from environment variables
        super().__init__(**kwargs)
        
        # Use Replit secrets if available
        if not self.databento_api_key:
            self.databento_api_key = os.getenv("DATABENTO_API_KEY", "")

        if not self.databento_symbols:
            self.databento_symbols = os.getenv("DATABENTO_SYMBOLS", "SPX,NDX,DJI,RUT")

        # Databento-only mode: keep Polygon key disabled even if legacy env vars exist.
        self.polygon_api_key = ""
        
        if not self.database_url:
            self.database_url = os.getenv("DATABASE_URL")

    def get_databento_symbols(self) -> List[str]:
        """Return de-duplicated, uppercase Databento symbols from configuration."""
        raw = self.databento_symbols or ""
        symbols: List[str] = []
        seen = set()

        for token in raw.split(","):
            sym = token.strip().upper()
            if sym and sym not in seen:
                symbols.append(sym)
                seen.add(sym)

        # Always include core index symbols.
        for core in ("SPX", "NDX", "DJI", "RUT"):
            if core not in seen:
                symbols.append(core)
                seen.add(core)

        return symbols

# Global settings instance
settings = Settings()

# Validate critical settings on import
if not settings.databento_api_key:
    raise ValueError("Set DATABENTO_API_KEY. This app is configured for Databento-only mode.")
