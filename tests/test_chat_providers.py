from pathlib import Path

from chatbot.providers import OllamaChatProvider
from chatbot.rag_retrieval import DocumentRetriever


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def test_ollama_provider_parses_chat_response(monkeypatch):
    captured = {}

    def fake_post(self, url, json=None, headers=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse({"model": "llama3.2", "message": {"content": "Local answer"}})

    monkeypatch.setattr("requests.sessions.Session.post", fake_post)

    provider = OllamaChatProvider(model="llama3.2", base_url="http://127.0.0.1:11434/api/chat")
    response = provider.generate_reply(
        message="Summarize the site counts.",
        history=[{"role": "user", "content": "Earlier question"}],
        user_context={"username": "admin"},
        mode="concise",
    )

    assert response.answer == "Local answer"
    assert response.model == "llama3.2"
    assert captured["url"] == "http://127.0.0.1:11434/api/chat"
    assert captured["json"]["model"] == "llama3.2"
    assert captured["json"]["stream"] is False
    assert captured["json"]["messages"][-1]["content"] == "Summarize the site counts."


class _StubEmbeddingProvider:
    def embed_texts(self, texts):
        assert texts == ["site summary"]
        return [[1.0, 0.0]]


def test_document_retriever_uses_embedding_provider_without_openai_key(tmp_path):
    manifest_path = tmp_path / "manifest.jsonl"
    index_path = tmp_path / "embeddings.jsonl"

    manifest_path.write_text(
        '{"chunk_id":"c1","text":"Crash summary text","metadata":{"source_name":"Portal source","page_number":2}}\n',
        encoding="utf-8",
    )
    index_path.write_text('{"chunk_id":"c1","embedding":[1.0,0.0]}\n', encoding="utf-8")

    retriever = DocumentRetriever(
        manifest_path=manifest_path,
        embeddings_path=index_path,
        embedding_provider=_StubEmbeddingProvider(),
    )
    result = retriever.retrieve(message="site summary", intent="search")

    assert len(result.evidence) == 1
    assert result.evidence[0]["source"] == "Portal source"
    assert result.citations[0]["page"] == 2

