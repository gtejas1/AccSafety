"""Utility module for interacting with the conversational assistant provider."""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Deque, Dict, Iterable, Iterator, List, Optional

import requests


class RateLimitError(RuntimeError):
    """Raised when the caller exceeds the configured rate limit."""


class RateLimiter:
    """Simple sliding window rate limiter used to protect the LLM backend."""

    def __init__(self, max_calls: int, window_seconds: float) -> None:
        self._max_calls = max_calls
        self._window = float(window_seconds)
        self._hits: Dict[str, Deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def hit(self, identity: str) -> None:
        """Record a hit for *identity* or raise :class:`RateLimitError`."""

        now = time.monotonic()
        with self._lock:
            bucket = self._hits[identity]
            while bucket and now - bucket[0] > self._window:
                bucket.popleft()
            if len(bucket) >= self._max_calls:
                raise RateLimitError("Rate limit exceeded. Try again shortly.")
            bucket.append(now)


class AssistantServiceError(RuntimeError):
    """Raised when the assistant provider returns an error."""


def _default_headers(api_key: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _default_payload(model: str, messages: List[Dict[str, str]], temperature: float) -> Dict[str, object]:
    return {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }


@dataclass
class AssistantService:
    """Wrapper around the configured LLM provider."""

    api_key: str
    model: str
    endpoint: str
    timeout: float = 30.0
    max_calls: int = 10
    window_seconds: float = 60.0

    def __post_init__(self) -> None:
        self._limiter = RateLimiter(self.max_calls, self.window_seconds)

    def _prepare_messages(
        self,
        messages: Iterable[Dict[str, str]],
        *,
        system_prompt: Optional[str],
        user_context: Optional[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        prepared: List[Dict[str, str]] = []
        if system_prompt:
            prepared.append({"role": "system", "content": system_prompt})
        if user_context:
            context_text = ", ".join(f"{k}: {v}" for k, v in sorted(user_context.items()))
            prepared.append(
                {
                    "role": "system",
                    "content": f"User context: {context_text}",
                }
            )
        for message in messages:
            role = message.get("role")
            content = message.get("content")
            if not role or not content:
                continue
            prepared.append({"role": role, "content": content})
        return prepared

    def send_message(
        self,
        messages: Iterable[Dict[str, str]],
        *,
        stream: bool = True,
        temperature: float = 0.2,
        system_prompt: Optional[str] = None,
        user_context: Optional[Dict[str, str]] = None,
        identity: str = "global",
    ) -> Iterator[str] | Dict[str, object]:
        """Send a chat request to the provider.

        When ``stream`` is true the method returns an iterator yielding chunks of
        assistant text. Otherwise it returns the parsed JSON response.
        """

        self._limiter.hit(identity)

        payload = _default_payload(
            self.model,
            self._prepare_messages(messages, system_prompt=system_prompt, user_context=user_context),
            temperature,
        )

        headers = _default_headers(self.api_key)
        payload["stream"] = stream

        try:
            response = requests.post(
                self.endpoint,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout,
                stream=stream,
            )
        except requests.RequestException as exc:  # pragma: no cover - network failure handling
            raise AssistantServiceError(f"Failed to reach assistant provider: {exc}") from exc

        if response.status_code >= 400:
            message = "Unexpected response from assistant provider"
            try:
                data = response.json()
                message = data.get("error", {}).get("message", message)
            except ValueError:
                pass
            raise AssistantServiceError(message)

        if not stream:
            return response.json()

        def iter_chunks() -> Iterator[str]:
            for line in response.iter_lines():
                if not line:
                    continue
                if line == b"data: [DONE]":
                    break
                if line.startswith(b"data: "):
                    line = line[6:]
                try:
                    payload = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    continue
                delta = payload.get("choices", [{}])[0].get("delta", {})
                text = delta.get("content")
                if text:
                    yield text

        return iter_chunks()


def _load_service() -> AssistantService:
    api_key = os.environ.get("ASSISTANT_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Assistant API key is not configured.")

    endpoint = os.environ.get("ASSISTANT_API_BASE", "https://api.openai.com/v1/chat/completions")
    model = os.environ.get("ASSISTANT_MODEL", "gpt-3.5-turbo")
    max_calls = int(os.environ.get("ASSISTANT_MAX_CALLS", "10"))
    window = float(os.environ.get("ASSISTANT_WINDOW_SECONDS", "60"))

    return AssistantService(
        api_key=api_key,
        model=model,
        endpoint=endpoint,
        max_calls=max_calls,
        window_seconds=window,
    )


_SERVICE: Optional[AssistantService] = None


def get_service() -> AssistantService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = _load_service()
    return _SERVICE


def send_message(
    messages: Iterable[Dict[str, str]],
    *,
    stream: bool = True,
    temperature: float = 0.2,
    system_prompt: Optional[str] = None,
    user_context: Optional[Dict[str, str]] = None,
    identity: str = "global",
) -> Iterator[str] | Dict[str, object]:
    """Proxy helper that delegates to the singleton service."""

    service = get_service()
    return service.send_message(
        messages,
        stream=stream,
        temperature=temperature,
        system_prompt=system_prompt,
        user_context=user_context,
        identity=identity,
    )

