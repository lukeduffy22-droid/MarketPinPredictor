"""Service helpers for the stock prediction system."""

__all__ = ["AIService", "get_ai_service"]


def __getattr__(name):
    """Load AI service exports lazily so optional providers are not eager imports."""
    if name in __all__:
        from .ai_service import AIService, get_ai_service

        return {"AIService": AIService, "get_ai_service": get_ai_service}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
