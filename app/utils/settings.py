"""
Application settings using pydantic-settings for type-safe configuration.
Loads from environment variables with fail-fast validation.
"""
from typing import Optional

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Application configuration"""

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False, extra="ignore")
    
    # API Keys - reads from Massive_API env var (Polygon rebranded to Massive Oct 2025)
    polygon_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("Massive_API", "POLYGON_API_KEY", "polygon_api_key"),
    )
    databento_api_key: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("DATABENTO_API_KEY", "DATABENTO_KEY", "databento_api_key"),
    )
    database_url: Optional[str] = Field(default=None, validation_alias=AliasChoices("DATABASE_URL", "database_url"))

    # Market data provider selection for live fallback polling
    # auto: prefer Databento when key exists, otherwise Polygon
    # databento: force Databento
    # polygon: force Polygon
    market_data_provider: str = Field(
        default="auto",
        validation_alias=AliasChoices("MARKET_DATA_PROVIDER", "market_data_provider"),
    )
    
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
    
# Global settings instance
settings = Settings()

# Validate critical settings on import
if not settings.polygon_api_key and not settings.databento_api_key:
    print("⚠️ WARNING: No live market data API keys found")
    print("⚠️ Set Massive_API and/or DATABENTO_API_KEY for live data collection")
