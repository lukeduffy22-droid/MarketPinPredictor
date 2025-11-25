"""
Base AI Provider interface.
All AI providers must implement this interface for easy swapping.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class PredictionCritique:
    """Result of AI critiquing a prediction."""
    original_prediction: float
    adjusted_prediction: float
    confidence: float  # 0.0 to 1.0
    adjustment_reason: str
    market_conditions: str
    risk_factors: list
    recommendation: str
    provider: str  # Which AI provider generated this


@dataclass  
class MarketAnalysis:
    """AI analysis of current market conditions."""
    summary: str
    gamma_interpretation: str
    trend_assessment: str
    key_levels: Dict[str, float]
    sentiment: str  # bullish, bearish, neutral
    confidence: float
    provider: str


class BaseAIProvider(ABC):
    """
    Abstract base class for AI providers.
    Implement this interface to add support for new AI providers.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the provider name (e.g., 'openai', 'google', 'anthropic')"""
        pass
    
    @abstractmethod
    async def analyze_prediction(
        self,
        symbol: str,
        current_price: float,
        predicted_eod: float,
        gamma_data: Dict[str, Any],
        vwap_deviation: float,
        microtrend: float,
        minutes_to_close: int,
        historical_accuracy: Optional[float] = None
    ) -> PredictionCritique:
        """
        Analyze and critique an EOD prediction.
        
        Args:
            symbol: Stock symbol (SPX, NDX, etc.)
            current_price: Current market price
            predicted_eod: The model's predicted EOD price
            gamma_data: Gamma exposure analysis data
            vwap_deviation: Current VWAP deviation
            microtrend: Recent price trend slope
            minutes_to_close: Minutes until market close
            historical_accuracy: Optional historical model accuracy
            
        Returns:
            PredictionCritique with analysis and adjusted prediction
        """
        pass
    
    @abstractmethod
    async def get_market_briefing(
        self,
        symbol: str,
        current_price: float,
        gamma_data: Dict[str, Any],
        eod_prediction: float,
        multi_expiry_data: Optional[Dict[str, Any]] = None
    ) -> MarketAnalysis:
        """
        Generate a market briefing/analysis for the given symbol.
        
        Args:
            symbol: Stock symbol
            current_price: Current market price
            gamma_data: Gamma exposure data
            eod_prediction: Current EOD prediction
            multi_expiry_data: Optional multi-expiration gamma data
            
        Returns:
            MarketAnalysis with comprehensive market briefing
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if this provider is configured and available."""
        pass
