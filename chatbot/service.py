from __future__ import annotations

import time
from typing import Any

from .providers import BaseChatProvider, ChatProviderError, build_provider_from_env


class ChatService:
    def __init__(self, provider: BaseChatProvider | None = None) -> None:
        self.provider = provider

    def _provider(self) -> BaseChatProvider:
        if self.provider is None:
            self.provider = build_provider_from_env()
        return self.provider

    def generate_reply(
        self,
        *,
        message: str,
        history: list[dict[str, str]] | None = None,
        user_context: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        try:
            provider_response = self._provider().generate_reply(
                message=message,
                history=history,
                user_context=user_context,
                mode=mode,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": provider_response.answer,
                "sources": provider_response.sources,
                "latency_ms": latency_ms,
                "model": provider_response.model,
                "status": "ok",
            }
        except ChatProviderError as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": exc.public_message,
                "sources": [],
                "latency_ms": latency_ms,
                "model": None,
                "status": exc.code,
            }
