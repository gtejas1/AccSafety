from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import requests

from .providers import ChatProviderError


DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_EMBEDDING_URL = "https://api.openai.com/v1/embeddings"


@dataclass(frozen=True)
class DocumentChunk:
    chunk_id: str
    text: str
    metadata: dict[str, Any]
    embedding: list[float]
    norm: float


@dataclass
class DocumentSearchResult:
    score: float
    chunk: DocumentChunk


def _normalize_text(value: str) -> str:
    return " ".join((value or "").split())


def _vector_norm(values: Iterable[float]) -> float:
    return math.sqrt(sum(v * v for v in values))


class OpenAIEmbedder:
    def __init__(
        self,
        *,
        api_key: str,
        model: str | None = None,
        base_url: str | None = None,
        timeout_seconds: float = 30,
    ) -> None:
        if not api_key:
            raise ChatProviderError("Embedding service is not configured.", code="config_error")
        self.api_key = api_key
        self.model = model or DEFAULT_EMBEDDING_MODEL
        self.base_url = (base_url or DEFAULT_EMBEDDING_URL).rstrip("/")
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()

    def embed(self, texts: list[str]) -> list[list[float]]:
        payload = {"model": self.model, "input": texts}
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
            raise ChatProviderError("Embedding request timed out.", code="timeout") from exc
        except requests.RequestException as exc:
            raise ChatProviderError("Embedding service is unavailable.", code="network_error") from exc

        if response.status_code in (401, 403):
            raise ChatProviderError("Embedding credentials are invalid.", code="auth_error")
        if response.status_code == 429:
            raise ChatProviderError("Embedding service is busy. Please retry shortly.", code="rate_limited")
        if response.status_code >= 500:
            raise ChatProviderError("Embedding service is temporarily unavailable.", code="provider_unavailable")
        if response.status_code >= 400:
            raise ChatProviderError("Unable to process embedding request.", code="bad_request")

        try:
            data = response.json()
            embeddings = [item["embedding"] for item in data["data"]]
        except (ValueError, KeyError, TypeError) as exc:
            raise ChatProviderError("Embedding response was invalid.", code="invalid_response") from exc

        return embeddings


class DocumentRAGIndex:
    def __init__(self, chunks: list[DocumentChunk]) -> None:
        self.chunks = chunks

    @classmethod
    def load(cls, path: str | Path) -> "DocumentRAGIndex":
        resolved = Path(path)
        if not resolved.exists():
            return cls([])

        chunks: list[DocumentChunk] = []
        with resolved.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                record = json.loads(raw)
                embedding = record.get("embedding") or []
                norm = _vector_norm(embedding)
                chunks.append(
                    DocumentChunk(
                        chunk_id=str(record.get("id")),
                        text=str(record.get("text") or ""),
                        metadata=record.get("metadata") or {},
                        embedding=embedding,
                        norm=norm,
                    )
                )
        return cls(chunks)

    def search(self, query_embedding: list[float], *, top_k: int = 8) -> list[DocumentSearchResult]:
        if not self.chunks:
            return []
        query_norm = _vector_norm(query_embedding)
        if query_norm == 0:
            return []

        scored: list[DocumentSearchResult] = []
        for chunk in self.chunks:
            if chunk.norm == 0:
                continue
            score = sum(q * c for q, c in zip(query_embedding, chunk.embedding)) / (query_norm * chunk.norm)
            scored.append(DocumentSearchResult(score=score, chunk=chunk))

        return sorted(scored, key=lambda item: item.score, reverse=True)[:top_k]


def build_embedder_from_env() -> OpenAIEmbedder:
    api_key = os.environ.get("CHAT_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
    model = os.environ.get("CHAT_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)
    base_url = os.environ.get("CHAT_EMBEDDING_BASE_URL", DEFAULT_EMBEDDING_URL)
    timeout_seconds = float(os.environ.get("CHAT_EMBEDDING_TIMEOUT", "30"))
    return OpenAIEmbedder(api_key=api_key, model=model, base_url=base_url, timeout_seconds=timeout_seconds)


def build_document_evidence(
    results: list[DocumentSearchResult],
    *,
    min_score: float = 0.2,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    evidence: list[dict[str, Any]] = []
    citations: list[dict[str, Any]] = []
    source_counts: dict[str, int] = {}

    for result in results:
        if result.score < min_score:
            continue
        chunk = result.chunk
        metadata = chunk.metadata or {}
        title = metadata.get("title") or metadata.get("source_name") or "Document"
        source = metadata.get("source_path") or metadata.get("source_name") or "unknown"
        snippet = _normalize_text(chunk.text)
        if len(snippet) > 320:
            snippet = snippet[:317].rstrip() + "..."

        evidence.append(
            {
                "title": title,
                "snippet": snippet,
                "source": source,
                "metadata": metadata,
            }
        )
        citations.append(
            {
                "title": title,
                "source": source,
                "page": metadata.get("page"),
                "sheet": metadata.get("sheet"),
            }
        )
        source_counts[source] = source_counts.get(source, 0) + 1

    stats = {
        "by_source": [
            {"name": source, "count": count} for source, count in sorted(source_counts.items(), key=lambda kv: kv[1], reverse=True)
        ],
        "by_facility": [],
        "by_mode": [],
    }

    return evidence, citations, stats
