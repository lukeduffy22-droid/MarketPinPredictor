"""Service package exports with lazy loading to avoid optional dependency side effects."""

__all__ = ["AIService", "get_ai_service"]


def __getattr__(name: str):
    if name in __all__:
        from .ai_service import AIService, get_ai_service

        exports = {
            "AIService": AIService,
            "get_ai_service": get_ai_service,
        }
        return exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
