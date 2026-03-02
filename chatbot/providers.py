from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


class OpenAICompatibleProvider(BaseChatProvider):
    """Simple OpenAI-compatible chat completions provider."""

    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str,
        timeout_seconds: float = 20,
        max_retries: int = 2,
    ) -> None:
        if not api_key:
            raise ChatProviderError("Chat service is not configured.", code="config_error")
        self.api_key = api_key
        self.model = model
        self.base_url = base_url.rstrip("/")
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
                messages.append(
                    {
                        "role": "system",
                        "content": f"Authenticated user: {username}.",
                    }
                )

        messages.append({"role": "user", "content": message})

        payload = {
            "model": self.model,
            "messages": messages,
            "temperature": 0.2,
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            response = self.session.post(
                self.base_url,
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
            data = response.json()
            answer = data["choices"][0]["message"]["content"].strip()
            model = str(data.get("model") or self.model)
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise ChatProviderError("Chat service returned an invalid response.", code="invalid_response") from exc

        return ChatProviderResponse(answer=answer, model=model, sources=[])


def build_provider_from_env() -> BaseChatProvider:
    provider_name = (os.environ.get("CHAT_PROVIDER") or "openai").strip().lower()
    if provider_name != "openai":
        raise ChatProviderError(f"Unsupported chat provider: {provider_name}", code="unsupported_provider")

    api_key = os.environ.get("CHAT_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
    model = os.environ.get("CHAT_MODEL", "gpt-4o-mini")
    base_url = os.environ.get("CHAT_BASE_URL", "https://api.openai.com/v1/chat/completions")
    timeout_seconds = float(os.environ.get("CHAT_TIMEOUT_SECONDS", "20"))
    max_retries = int(os.environ.get("CHAT_MAX_RETRIES", "2"))

    return OpenAICompatibleProvider(
        api_key=api_key,
        model=model,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )
