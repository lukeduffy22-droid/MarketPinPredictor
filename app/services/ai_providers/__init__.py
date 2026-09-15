"""AI provider package exports with lazy optional-provider loading."""

from .base_provider import BaseAIProvider

__all__ = ["BaseAIProvider", "OpenAIProvider"]


def __getattr__(name: str):
    if name == "OpenAIProvider":
        from .openai_provider import OpenAIProvider

        return OpenAIProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
