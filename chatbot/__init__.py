"""Chatbot service package."""

__all__ = ["ChatService"]


def __getattr__(name: str):
    if name == "ChatService":
        from .service import ChatService

        return ChatService
    raise AttributeError(name)
