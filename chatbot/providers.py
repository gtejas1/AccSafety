from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .settings import ChatRuntimeSettings


class ChatProviderError(RuntimeError):
    """Provider errors that are safe to surface to the UI."""

    def __init__(self, public_message: str, *, code: str = "provider_error") -> None:
        super().__init__(public_message)
        self.public_message = public_message
        self.code = code


@dataclass
class ChatProviderResponse:
    answer: str
    model: str
    sources: list[dict[str, str]]


class BaseChatProvider(ABC):
    @abstractmethod
    def generate_reply(
        self,
        *,
        message: str,
        history: list[dict[str, str]] | None = None,
        user_context: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> ChatProviderResponse:
        raise NotImplementedError


@dataclass
class ChatProviderConfig:
    provider: str
    model: str
    base_url: str
    api_key: str
    timeout_seconds: float = 20
    max_retries: int = 2


class RequestsChatProvider(BaseChatProvider):
    def __init__(self, *, timeout_seconds: float = 20, max_retries: int = 2) -> None:
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()
        retry = Retry(
            total=max_retries,
            read=max_retries,
            connect=max_retries,
            backoff_factor=0.6,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.mount("http://", HTTPAdapter(max_retries=retry))

    def _post_json(
        self,
        *,
        url: str,
        payload: dict[str, Any],
        headers: dict[str, str],
    ) -> dict[str, Any]:
        try:
            response = self.session.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout_seconds,
            )
        except requests.Timeout as exc:
            raise ChatProviderError("The chat request timed out. Please try again.", code="timeout") from exc
        except requests.RequestException as exc:
            raise ChatProviderError("Chat service is temporarily unavailable.", code="network_error") from exc

        if response.status_code in (401, 403):
            raise ChatProviderError("Chat service credentials are invalid.", code="auth_error")
        if response.status_code == 429:
            raise ChatProviderError("Chat service is busy. Please retry shortly.", code="rate_limited")
        if response.status_code >= 500:
            raise ChatProviderError("Chat service is temporarily unavailable.", code="provider_unavailable")
        if response.status_code >= 400:
            raise ChatProviderError("Unable to process chat request.", code="bad_request")

        try:
            return response.json()
        except ValueError as exc:
            raise ChatProviderError("Chat service returned an invalid response.", code="invalid_response") from exc


class OllamaChatProvider(RequestsChatProvider):
    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        timeout_seconds: float = 20,
        max_retries: int = 2,
    ) -> None:
        super().__init__(timeout_seconds=timeout_seconds, max_retries=max_retries)
        self.model = model
        self.base_url = base_url.rstrip("/")

    def generate_reply(
        self,
        *,
        message: str,
        history: list[dict[str, str]] | None = None,
        user_context: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> ChatProviderResponse:
        system_prompt = "You are a concise, helpful assistant for transportation safety analytics users."
        if mode:
            system_prompt += f" Answer in `{mode}` mode when possible."

        messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]
        for item in history or []:
            role = (item.get("role") or "").strip()
            content = (item.get("content") or "").strip()
            if role in {"system", "assistant", "user"} and content:
                messages.append({"role": role, "content": content})

        if user_context:
            username = str(user_context.get("username") or "").strip()
            if username:
                messages.append({"role": "system", "content": f"Authenticated user: {username}."})

        messages.append({"role": "user", "content": message})

        data = self._post_json(
            url=self.base_url,
            payload={"model": self.model, "messages": messages, "stream": False},
            headers={"Content-Type": "application/json"},
        )

        try:
            body = data.get("message") or {}
            answer = str(body["content"]).strip()
            model = str(data.get("model") or self.model)
        except (KeyError, TypeError) as exc:
            raise ChatProviderError("Chat service returned an invalid response.", code="invalid_response") from exc
        return ChatProviderResponse(answer=answer, model=model, sources=[])


def build_provider(config: ChatProviderConfig) -> BaseChatProvider:
    provider_name = (config.provider or "ollama").strip().lower()
    if provider_name != "ollama":
        raise ChatProviderError(f"Unsupported chat provider: {provider_name}. Only 'ollama' is supported.", code="unsupported_provider")
    return OllamaChatProvider(
        model=config.model,
        base_url=config.base_url,
        timeout_seconds=config.timeout_seconds,
        max_retries=config.max_retries,
    )


def build_provider_from_settings(settings: ChatRuntimeSettings) -> BaseChatProvider:
    timeout_seconds = float(os.environ.get("CHAT_TIMEOUT_SECONDS", "20"))
    max_retries = int(os.environ.get("CHAT_MAX_RETRIES", "2"))
    return build_provider(
        ChatProviderConfig(
            provider=settings.chat_provider,
            model=settings.chat_model,
            base_url=settings.chat_base_url,
            api_key=settings.chat_api_key,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    )


def build_provider_from_env() -> BaseChatProvider:
    from .settings import ChatSettingsStore

    settings = ChatSettingsStore(os.environ.get("CHAT_SETTINGS_PATH", "data/chat_settings.json")).load()
    return build_provider_from_settings(settings)
