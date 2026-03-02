from __future__ import annotations

import json
import logging
import math
import os
from pathlib import Path
from typing import Any

import requests

from .retrieval import RetrievalResult

logger = logging.getLogger(__name__)

DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_EMBEDDING_BASE_URL = "https://api.openai.com/v1/embeddings"


def _load_json_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _snippet(text: str, limit: int = 360) -> str:
    cleaned = " ".join(text.split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 1].rstrip()}…"


def _format_citation_location(metadata: dict[str, Any]) -> str:
    if metadata.get("page_number"):
        return f"page {metadata['page_number']}"
    if metadata.get("sheet_name"):
        return f"sheet {metadata['sheet_name']}"
    return "document"


class DocumentRetriever:
    def __init__(
        self,
        *,
        manifest_path: str | Path | None = None,
        embeddings_path: str | Path | None = None,
        top_k: int | None = None,
        embedding_model: str | None = None,
        embedding_base_url: str | None = None,
        openai_api_key: str | None = None,
    ) -> None:
        self.manifest_path = Path(
            manifest_path
            or os.environ.get("RAG_CHUNK_STORE_PATH")
            or os.environ.get("RAG_MANIFEST_PATH", "rag_manifest.jsonl")
        )
        self.embeddings_path = Path(
            embeddings_path
            or os.environ.get("RAG_INDEX_PATH")
            or os.environ.get("RAG_EMBEDDINGS_PATH", "rag_embeddings.jsonl")
        )
        self.top_k = top_k or int(os.environ.get("RAG_TOP_K", "8"))
        self.embedding_model = embedding_model or os.environ.get(
            "EMBEDDING_MODEL",
            os.environ.get("RAG_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        )
        self.embedding_base_url = embedding_base_url or os.environ.get(
            "EMBEDDING_BASE_URL", DEFAULT_EMBEDDING_BASE_URL
        )
        self.openai_api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")

        self._validate_paths()
        self._chunks = self._load_chunks()
        self._embeddings = self._load_embeddings()

    def _validate_paths(self) -> None:
        missing = []
        if not self.embeddings_path.exists():
            missing.append(
                f"RAG index missing at {self.embeddings_path} (set RAG_INDEX_PATH)."
            )
        if not self.manifest_path.exists():
            missing.append(
                f"RAG chunk store missing at {self.manifest_path} (set RAG_CHUNK_STORE_PATH)."
            )
        if missing:
            raise RuntimeError(" ".join(missing))

    def _load_chunks(self) -> dict[str, dict[str, Any]]:
        entries = {}
        for record in _load_json_records(self.manifest_path):
            chunk_id = record.get("chunk_id")
            if not chunk_id:
                continue
            entries[str(chunk_id)] = record
        if not entries:
            logger.warning("No RAG chunks loaded from %s.", self.manifest_path)
        return entries

    def _load_embeddings(self) -> dict[str, dict[str, Any]]:
        entries = {}
        for record in _load_json_records(self.embeddings_path):
            chunk_id = record.get("chunk_id")
            embedding = record.get("embedding")
            if not chunk_id or not embedding:
                continue
            vector = [float(value) for value in embedding]
            norm = math.sqrt(sum(value * value for value in vector))
            entries[str(chunk_id)] = {"vector": vector, "norm": norm}
        if not entries:
            logger.warning("No RAG embeddings loaded from %s.", self.embeddings_path)
        return entries

    def _embed_query(self, text: str) -> list[float]:
        if not self.openai_api_key:
            raise RuntimeError("OPENAI_API_KEY is required for document retrieval.")
        response = requests.post(
            self.embedding_base_url,
            headers={"Authorization": f"Bearer {self.openai_api_key}"},
            json={"model": self.embedding_model, "input": text},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        return payload["data"][0]["embedding"]

    @staticmethod
    def _cosine_similarity(query: list[float], query_norm: float, entry: dict[str, Any]) -> float:
        if query_norm == 0 or entry["norm"] == 0:
            return 0.0
        dot = sum(q * d for q, d in zip(query, entry["vector"]))
        return dot / (query_norm * entry["norm"])

    def retrieve(self, *, message: str, intent: str) -> RetrievalResult:
        if not self._chunks or not self._embeddings:
            return RetrievalResult(evidence=[], citations=[], stats={})

        query = message.strip()
        if not query:
            return RetrievalResult(evidence=[], citations=[], stats={})

        query_vector = self._embed_query(query)
        query_norm = math.sqrt(sum(value * value for value in query_vector))

        scored: list[tuple[str, float]] = []
        for chunk_id, embedding in self._embeddings.items():
            score = self._cosine_similarity(query_vector, query_norm, embedding)
            scored.append((chunk_id, score))

        scored.sort(key=lambda item: item[1], reverse=True)
        top_scored = scored[: self.top_k]

        evidence: list[dict[str, Any]] = []
        citations: list[dict[str, Any]] = []
        for chunk_id, score in top_scored:
            chunk = self._chunks.get(chunk_id)
            if not chunk:
                continue
            metadata = chunk.get("metadata") or {}
            source_name = metadata.get("source_name") or "Unknown source"
            location = _format_citation_location(metadata)
            title = f"{source_name} ({location})"
            evidence.append(
                {
                    "title": title,
                    "snippet": _snippet(chunk.get("text", "")),
                    "source": source_name,
                    "score": score,
                    "metadata": metadata,
                }
            )
            citations.append(
                {
                    "title": title,
                    "source": source_name,
                    "page": metadata.get("page_number"),
                    "sheet": metadata.get("sheet_name"),
                    "source_path": metadata.get("source_path"),
                    "chunk_id": chunk_id,
                }
            )

        return RetrievalResult(
            evidence=evidence,
            citations=citations,
            stats={"by_source": [], "by_facility": [], "by_mode": []},
        )
