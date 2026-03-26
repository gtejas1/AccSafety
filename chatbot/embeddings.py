from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .providers import ChatProviderError
from .settings import ChatRuntimeSettings


@dataclass
class EmbeddingProviderConfig:
    provider: str
    model: str
    base_url: str
    api_key: str
    timeout_seconds: float = 30
    max_retries: int = 2


class BaseEmbeddingProvider(ABC):
    @abstractmethod
    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError


class _RequestsEmbeddingProvider(BaseEmbeddingProvider):
    def __init__(self, *, timeout_seconds: float, max_retries: int) -> None:
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
        headers: dict[str, str],
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            response = self.session.post(url, json=payload, headers=headers, timeout=self.timeout_seconds)
        except requests.Timeout as exc:
            raise ChatProviderError("The embedding request timed out. Please try again.", code="timeout") from exc
        except requests.RequestException as exc:
            raise ChatProviderError("Embedding service is temporarily unavailable.", code="network_error") from exc

        if response.status_code in (401, 403):
            raise ChatProviderError("Embedding service credentials are invalid.", code="auth_error")
        if response.status_code == 429:
            raise ChatProviderError("Embedding service is busy. Please retry shortly.", code="rate_limited")
        if response.status_code >= 500:
            raise ChatProviderError("Embedding service is temporarily unavailable.", code="provider_unavailable")
        if response.status_code >= 400:
            raise ChatProviderError("Unable to process embedding request.", code="bad_request")

        try:
            return response.json()
        except ValueError as exc:
            raise ChatProviderError("Embedding service returned an invalid response.", code="invalid_response") from exc


class OpenAICompatibleEmbeddingProvider(_RequestsEmbeddingProvider):
    def __init__(
        self,
        *,
        api_key: str,
        model: str,
        base_url: str,
        timeout_seconds: float = 30,
        max_retries: int = 2,
    ) -> None:
        if not api_key:
            raise ChatProviderError("Embedding service is not configured.", code="config_error")
        super().__init__(timeout_seconds=timeout_seconds, max_retries=max_retries)
        self.api_key = api_key
        self.model = model
        self.base_url = base_url.rstrip("/")

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        payload = {"model": self.model, "input": texts}
        data = self._post_json(
            url=self.base_url,
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            payload=payload,
        )
        try:
            items = sorted(data["data"], key=lambda item: item.get("index", 0))
            return [list(item["embedding"]) for item in items]
        except (KeyError, TypeError) as exc:
            raise ChatProviderError("Embedding service returned an invalid response.", code="invalid_response") from exc


class OllamaEmbeddingProvider(_RequestsEmbeddingProvider):
    def __init__(
        self,
        *,
        model: str,
        base_url: str,
        timeout_seconds: float = 30,
        max_retries: int = 2,
    ) -> None:
        super().__init__(timeout_seconds=timeout_seconds, max_retries=max_retries)
        self.model = model
        self.base_url = base_url.rstrip("/")

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        data = self._post_json(
            url=self.base_url,
            headers={"Content-Type": "application/json"},
            payload={"model": self.model, "input": texts},
        )
        if isinstance(data.get("embeddings"), list):
            return [list(item) for item in data["embeddings"]]
        if isinstance(data.get("embedding"), list):
            return [list(data["embedding"])]
        raise ChatProviderError("Embedding service returned an invalid response.", code="invalid_response")


def build_embedding_provider(
    config: EmbeddingProviderConfig,
) -> BaseEmbeddingProvider:
    provider_name = (config.provider or "").strip().lower()
    if provider_name == "ollama":
        return OllamaEmbeddingProvider(
            model=config.model,
            base_url=config.base_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
    if provider_name == "openai":
        return OpenAICompatibleEmbeddingProvider(
            api_key=config.api_key,
            model=config.model,
            base_url=config.base_url,
            timeout_seconds=config.timeout_seconds,
            max_retries=config.max_retries,
        )
    raise ChatProviderError(f"Unsupported embedding provider: {provider_name}", code="unsupported_provider")


def build_embedding_provider_from_settings(
    settings: ChatRuntimeSettings,
    *,
    timeout_seconds: float = 30,
    max_retries: int = 2,
) -> BaseEmbeddingProvider:
    return build_embedding_provider(
        EmbeddingProviderConfig(
            provider=settings.embedding_provider,
            model=settings.embedding_model,
            base_url=settings.embedding_base_url,
            api_key=settings.embedding_api_key,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
    )
