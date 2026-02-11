"""
Application settings using pydantic-settings for type-safe configuration.
Loads from environment variables with fail-fast validation.
"""
from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os

class Settings(BaseSettings):
    """Application configuration"""
    
    # API Keys - reads from Massive_API env var (Polygon rebranded to Massive Oct 2025)
    polygon_api_key: Optional[str] = Field(default=None, validation_alias="Massive_API")
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
        
    def __init__(self, **kwargs):
        # Load from environment variables
        super().__init__(**kwargs)
        
        # Use Replit secrets if available
        if not self.polygon_api_key:
            self.polygon_api_key = os.getenv("Massive_API", "")
        
        if not self.database_url:
            self.database_url = os.getenv("DATABASE_URL")

# Global settings instance
settings = Settings()

# Validate critical settings on import
if not settings.polygon_api_key:
    print("⚠️ WARNING: Massive_API environment variable is not set")
    print("⚠️ Live data collection will be disabled")
    print("⚠️ Set environment variable: export Massive_API='your_api_key'")
