from __future__ import annotations

import os
import re
import time
from typing import Any

from .providers import BaseChatProvider, ChatProviderError, build_provider_from_env
from .policy import build_system_policy_text, evaluate_user_request, refusal_text
from .retrieval import EvidenceRetriever, RetrievalResult
from .rag_retrieval import DocumentRetriever


NO_EVIDENCE_MESSAGE = (
    "I couldn't find matching evidence in the available transportation safety datasets. "
    "Please refine the location, source, facility type, or travel mode and try again."
)

HELP_MESSAGE = (
    "I can help with transportation safety analytics questions based on the available datasets. "
    "Try asking about crash trends, activity at a site, comparisons across locations, or what data sources are available."
)


class ChatService:
    def __init__(
        self,
        provider: BaseChatProvider | None = None,
        retriever: EvidenceRetriever | DocumentRetriever | None = None,
    ) -> None:
        self.provider = provider
        self.retriever = retriever or self._build_retriever_from_env()

    @staticmethod
    def _build_retriever_from_env() -> EvidenceRetriever | DocumentRetriever:
        mode = os.environ.get("RAG_MODE", "").strip().lower()
        if mode == "documents":
            return DocumentRetriever()
        return EvidenceRetriever()

    def _provider(self) -> BaseChatProvider:
        if self.provider is None:
            self.provider = build_provider_from_env()
        return self.provider

    def _classify_intent(self, message: str) -> str:
        text = message.strip().lower()
        if not text:
            return "help"
        if re.search(r"\b(help|how|what can you do|usage|options)\b", text):
            return "help"
        if re.search(r"\b(compare|versus|vs\.?|difference|higher|lower|between)\b", text):
            return "compare"
        if re.search(r"\b(why|explain|reason|interpret|insight)\b", text):
            return "explain"
        return "search"

    def _build_constraint_prompt(self, retrieval: RetrievalResult, intent: str) -> str:
        lines = [
            "You are an assistant for transportation safety analytics.",
            "Answer strictly using the provided evidence snippets.",
            "If the evidence does not support a claim, say so explicitly.",
            f"Detected intent: {intent}.",
            "Cite evidence by source and location names when summarizing.",
            build_system_policy_text(),
            "Evidence:",
        ]
        for idx, item in enumerate(retrieval.evidence[:12], start=1):
            lines.append(f"{idx}. [{item['source']}] {item['title']}: {item['snippet']}")
        return "\n".join(lines)

    def generate_reply(
        self,
        *,
        message: str,
        history: list[dict[str, str]] | None = None,
        user_context: dict[str, Any] | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        started = time.perf_counter()
        policy_decision = evaluate_user_request(message=message, history=history)
        if not policy_decision.allowed:
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": refusal_text(policy_decision.reason),
                "sources": [],
                "citations": [],
                "retrieval": {"stats": {}, "evidence_count": 0},
                "intent": "refusal",
                "latency_ms": latency_ms,
                "model": None,
                "status": "refused",
                "refusal_reason": policy_decision.reason,
            }

        intent = self._classify_intent(message)

        if intent == "help":
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": HELP_MESSAGE,
                "sources": [],
                "citations": [],
                "retrieval": {"stats": {}, "evidence_count": 0},
                "intent": intent,
                "latency_ms": latency_ms,
                "model": None,
                "status": "ok",
            }

        retrieval = self.retriever.retrieve(message=message, intent=intent)

        if not retrieval.evidence:
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": NO_EVIDENCE_MESSAGE,
                "sources": [],
                "citations": [],
                "retrieval": {"stats": retrieval.stats, "evidence_count": 0},
                "intent": intent,
                "latency_ms": latency_ms,
                "model": None,
                "status": "no_evidence",
            }

        constraint_prompt = self._build_constraint_prompt(retrieval, intent)
        model_history = list(history or [])
        model_history.insert(0, {"role": "system", "content": constraint_prompt})

        try:
            provider_response = self._provider().generate_reply(
                message=message,
                history=model_history,
                user_context=user_context,
                mode=mode,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            sources = provider_response.sources or retrieval.citations
            return {
                "answer": provider_response.answer,
                "sources": sources,
                "citations": retrieval.citations,
                "retrieval": {
                    "stats": retrieval.stats,
                    "evidence_count": len(retrieval.evidence),
                },
                "intent": intent,
                "latency_ms": latency_ms,
                "model": provider_response.model,
                "status": "ok",
            }
        except ChatProviderError as exc:
            latency_ms = int((time.perf_counter() - started) * 1000)
            return {
                "answer": exc.public_message,
                "sources": retrieval.citations,
                "citations": retrieval.citations,
                "retrieval": {
                    "stats": retrieval.stats,
                    "evidence_count": len(retrieval.evidence),
                },
                "intent": intent,
                "latency_ms": latency_ms,
                "model": None,
                "status": exc.code,
            }
