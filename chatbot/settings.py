from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_OLLAMA_HOST = "http://127.0.0.1:11434"
DEFAULT_CHAT_MODEL = "llama3.2"
DEFAULT_EMBEDDING_MODEL = "nomic-embed-text"
DEFAULT_OPENAI_CHAT_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_OPENAI_EMBEDDINGS_URL = "https://api.openai.com/v1/embeddings"


def _provider_default_base_url(provider: str, *, kind: str, ollama_host: str) -> str:
    normalized_provider = (provider or "").strip().lower()
    host = (ollama_host or DEFAULT_OLLAMA_HOST).rstrip("/")
    if normalized_provider == "openai":
        return DEFAULT_OPENAI_CHAT_URL if kind == "chat" else DEFAULT_OPENAI_EMBEDDINGS_URL
    if kind == "chat":
        return f"{host}/api/chat"
    return f"{host}/api/embed"


@dataclass
class ChatRuntimeSettings:
    chat_provider: str
    chat_model: str
    chat_base_url: str
    chat_api_key: str
    embedding_provider: str
    embedding_model: str
    embedding_base_url: str
    embedding_api_key: str
    updated_at: str = ""

    def with_defaults_applied(self, *, ollama_host: str) -> "ChatRuntimeSettings":
        chat_provider = (self.chat_provider or "ollama").strip().lower()
        embedding_provider = (self.embedding_provider or "ollama").strip().lower()
        return ChatRuntimeSettings(
            chat_provider=chat_provider,
            chat_model=(self.chat_model or DEFAULT_CHAT_MODEL).strip(),
            chat_base_url=(self.chat_base_url or _provider_default_base_url(chat_provider, kind="chat", ollama_host=ollama_host)).strip(),
            chat_api_key=(self.chat_api_key or "").strip(),
            embedding_provider=embedding_provider,
            embedding_model=(self.embedding_model or DEFAULT_EMBEDDING_MODEL).strip(),
            embedding_base_url=(
                self.embedding_base_url
                or _provider_default_base_url(embedding_provider, kind="embedding", ollama_host=ollama_host)
            ).strip(),
            embedding_api_key=(self.embedding_api_key or "").strip(),
            updated_at=(self.updated_at or "").strip(),
        )

    def signature(self) -> tuple[str, ...]:
        return (
            self.chat_provider,
            self.chat_model,
            self.chat_base_url,
            self.chat_api_key,
            self.embedding_provider,
            self.embedding_model,
            self.embedding_base_url,
            self.embedding_api_key,
        )

    def to_public_dict(self) -> dict[str, str]:
        return {
            "chat_provider": self.chat_provider,
            "chat_model": self.chat_model,
            "chat_base_url": self.chat_base_url,
            "embedding_provider": self.embedding_provider,
            "embedding_model": self.embedding_model,
            "embedding_base_url": self.embedding_base_url,
            "updated_at": self.updated_at,
        }


class ChatSettingsStore:
    def __init__(self, storage_path: str | Path):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _ollama_host() -> str:
        return (os.environ.get("OLLAMA_HOST") or DEFAULT_OLLAMA_HOST).strip()

    def defaults(self) -> ChatRuntimeSettings:
        chat_provider = (os.environ.get("CHAT_PROVIDER") or "ollama").strip().lower()
        embedding_provider = (os.environ.get("EMBEDDING_PROVIDER") or chat_provider or "ollama").strip().lower()
        ollama_host = self._ollama_host()
        return ChatRuntimeSettings(
            chat_provider=chat_provider,
            chat_model=(os.environ.get("CHAT_MODEL") or DEFAULT_CHAT_MODEL).strip(),
            chat_base_url=(
                os.environ.get("CHAT_BASE_URL")
                or _provider_default_base_url(chat_provider, kind="chat", ollama_host=ollama_host)
            ).strip(),
            chat_api_key=(os.environ.get("CHAT_API_KEY") or os.environ.get("OPENAI_API_KEY") or "").strip(),
            embedding_provider=embedding_provider,
            embedding_model=(
                os.environ.get("EMBEDDING_MODEL")
                or os.environ.get("RAG_EMBEDDING_MODEL")
                or DEFAULT_EMBEDDING_MODEL
            ).strip(),
            embedding_base_url=(
                os.environ.get("EMBEDDING_BASE_URL")
                or _provider_default_base_url(embedding_provider, kind="embedding", ollama_host=ollama_host)
            ).strip(),
            embedding_api_key=(os.environ.get("EMBEDDING_API_KEY") or os.environ.get("OPENAI_API_KEY") or "").strip(),
        )

    def load(self) -> ChatRuntimeSettings:
        settings = self.defaults()
        if not self.storage_path.exists():
            return settings
        try:
            raw_data = json.loads(self.storage_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return settings
        if not isinstance(raw_data, dict):
            return settings
        loaded = ChatRuntimeSettings(
            chat_provider=str(raw_data.get("chat_provider") or settings.chat_provider),
            chat_model=str(raw_data.get("chat_model") or settings.chat_model),
            chat_base_url=str(raw_data.get("chat_base_url") or settings.chat_base_url),
            chat_api_key=str(raw_data.get("chat_api_key") or settings.chat_api_key),
            embedding_provider=str(raw_data.get("embedding_provider") or settings.embedding_provider),
            embedding_model=str(raw_data.get("embedding_model") or settings.embedding_model),
            embedding_base_url=str(raw_data.get("embedding_base_url") or settings.embedding_base_url),
            embedding_api_key=str(raw_data.get("embedding_api_key") or settings.embedding_api_key),
            updated_at=str(raw_data.get("updated_at") or settings.updated_at),
        )
        return loaded.with_defaults_applied(ollama_host=self._ollama_host())

    def save(self, settings: ChatRuntimeSettings) -> ChatRuntimeSettings:
        normalized = settings.with_defaults_applied(ollama_host=self._ollama_host())
        serialized = asdict(normalized)
        serialized["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.storage_path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")
        return ChatRuntimeSettings(**serialized)

    def update(self, values: dict[str, Any]) -> ChatRuntimeSettings:
        current = self.load()
        next_settings = ChatRuntimeSettings(
            chat_provider=str(values.get("chat_provider") or current.chat_provider).strip().lower(),
            chat_model=str(values.get("chat_model") or current.chat_model).strip(),
            chat_base_url=str(values.get("chat_base_url") or current.chat_base_url).strip(),
            chat_api_key=str(values.get("chat_api_key") if values.get("chat_api_key") is not None else current.chat_api_key).strip(),
            embedding_provider=str(values.get("embedding_provider") or current.embedding_provider).strip().lower(),
            embedding_model=str(values.get("embedding_model") or current.embedding_model).strip(),
            embedding_base_url=str(values.get("embedding_base_url") or current.embedding_base_url).strip(),
            embedding_api_key=str(
                values.get("embedding_api_key")
                if values.get("embedding_api_key") is not None
                else current.embedding_api_key
            ).strip(),
            updated_at=current.updated_at,
        )
        return self.save(next_settings)
