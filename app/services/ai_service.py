"""
AI Service - Main abstraction layer for AI providers.
Provides a unified interface for accessing any AI provider.
Easy to swap between OpenAI, Google, Anthropic, etc.
"""

import os
import logging
from typing import Dict, Any, Optional

from .ai_providers.base_provider import BaseAIProvider, PredictionCritique, MarketAnalysis
from .ai_providers.openai_provider import OpenAIProvider

log = logging.getLogger(__name__)

# Registry of available providers
_PROVIDERS: Dict[str, type] = {
    "openai": OpenAIProvider,
    # Future providers can be added here:
    # "google": GoogleProvider,
    # "anthropic": AnthropicProvider,
    # "grok": GrokProvider,
}

# Singleton instance
_ai_service_instance: Optional['AIService'] = None


class AIService:
    """
    Unified AI Service with swappable providers.
    
    Usage:
        service = get_ai_service()
        critique = await service.analyze_prediction(...)
        briefing = await service.get_market_briefing(...)
    
    To change providers:
        service = get_ai_service(provider="google")  # Once implemented
    """
    
    def __init__(self, provider_name: str = "openai"):
        """
        Initialize the AI service with specified provider.
        
        Args:
            provider_name: Name of the AI provider to use (default: "openai")
        """
        self._provider_name = provider_name
        self._provider: Optional[BaseAIProvider] = None
        self._initialize_provider()
    
    def _initialize_provider(self):
        """Initialize the selected AI provider."""
        if self._provider_name not in _PROVIDERS:
            available = list(_PROVIDERS.keys())
            log.error(f"Unknown provider: {self._provider_name}. Available: {available}")
            return
        
        try:
            provider_class = _PROVIDERS[self._provider_name]
            self._provider = provider_class()
            
            if self._provider.is_available():
                log.info(f"AI Service initialized with provider: {self._provider_name}")
            else:
                log.warning(f"Provider {self._provider_name} is not properly configured")
                self._provider = None
        except Exception as e:
            log.error(f"Failed to initialize provider {self._provider_name}: {e}")
            self._provider = None
    
    @property
    def provider_name(self) -> str:
        """Get the current provider name."""
        return self._provider_name
    
    @property
    def is_available(self) -> bool:
        """Check if AI service is available."""
        return self._provider is not None and self._provider.is_available()
    
    def switch_provider(self, provider_name: str) -> bool:
        """
        Switch to a different AI provider.
        
        Args:
            provider_name: Name of the new provider
            
        Returns:
            True if switch was successful, False otherwise
        """
        if provider_name not in _PROVIDERS:
            log.error(f"Cannot switch to unknown provider: {provider_name}")
            return False
        
        self._provider_name = provider_name
        self._initialize_provider()
        return self.is_available
    
    async def analyze_prediction(
        self,
        symbol: str,
        current_price: float,
        predicted_eod: float,
        gamma_data: Dict[str, Any],
        vwap_deviation: float = 0.0,
        microtrend: float = 0.0,
        minutes_to_close: int = 0,
        historical_accuracy: Optional[float] = None
    ) -> Optional[PredictionCritique]:
        """
        Analyze and critique an EOD prediction using AI.
        
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
            PredictionCritique with analysis and adjusted prediction,
            or None if AI is unavailable
        """
        if not self.is_available:
            log.warning("AI service not available for prediction analysis")
            return None
        
        try:
            return await self._provider.analyze_prediction(
                symbol=symbol,
                current_price=current_price,
                predicted_eod=predicted_eod,
                gamma_data=gamma_data,
                vwap_deviation=vwap_deviation,
                microtrend=microtrend,
                minutes_to_close=minutes_to_close,
                historical_accuracy=historical_accuracy
            )
        except Exception as e:
            log.error(f"AI prediction analysis error: {e}")
            return None
    
    async def get_market_briefing(
        self,
        symbol: str,
        current_price: float,
        gamma_data: Dict[str, Any],
        eod_prediction: float,
        multi_expiry_data: Optional[Dict[str, Any]] = None
    ) -> Optional[MarketAnalysis]:
        """
        Generate a market briefing/analysis for the given symbol.
        
        Args:
            symbol: Stock symbol
            current_price: Current market price
            gamma_data: Gamma exposure data
            eod_prediction: Current EOD prediction
            multi_expiry_data: Optional multi-expiration gamma data
            
        Returns:
            MarketAnalysis with comprehensive market briefing,
            or None if AI is unavailable
        """
        if not self.is_available:
            log.warning("AI service not available for market briefing")
            return None
        
        try:
            return await self._provider.get_market_briefing(
                symbol=symbol,
                current_price=current_price,
                gamma_data=gamma_data,
                eod_prediction=eod_prediction,
                multi_expiry_data=multi_expiry_data
            )
        except Exception as e:
            log.error(f"AI market briefing error: {e}")
            return None
    
    @staticmethod
    def get_available_providers() -> list:
        """Get list of available provider names."""
        return list(_PROVIDERS.keys())


def get_ai_service(provider: str = "openai", force_new: bool = False) -> AIService:
    """
    Get or create the AI service singleton.
    
    Args:
        provider: AI provider to use (default: "openai")
        force_new: If True, create a new instance instead of using cached
        
    Returns:
        AIService instance
    """
    global _ai_service_instance
    
    if force_new or _ai_service_instance is None:
        _ai_service_instance = AIService(provider_name=provider)
    elif _ai_service_instance.provider_name != provider:
        _ai_service_instance.switch_provider(provider)
    
    return _ai_service_instance
