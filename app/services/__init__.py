"""
AI Services module for the stock prediction system.
Provides swappable AI provider abstraction.
"""

from .ai_service import AIService, get_ai_service

__all__ = ['AIService', 'get_ai_service']
