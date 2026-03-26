# AccSafety Assistant Local LLM Configuration

AccSafety now supports a local-first assistant stack using Ollama for both chat generation and RAG embeddings. The chatbot keeps the same `/api/chat` API for regular users; admins can switch active chat and embedding backends from `/admin/chat-settings`.

## Recommended local setup

1. Install Ollama from [ollama.com](https://ollama.com).
2. Start the Ollama service locally.
3. Pull a chat model and an embedding model:

```bash
ollama pull llama3.2
ollama pull nomic-embed-text
```

4. Point AccSafety at the local Ollama host if you are not using the default `http://127.0.0.1:11434`.

## Runtime environment variables

| Variable | Purpose |
| --- | --- |
| `CHAT_PROVIDER` | Active chat provider. Defaults to `ollama`. |
| `CHAT_MODEL` | Default chat model. Defaults to `llama3.2`. |
| `CHAT_BASE_URL` | Chat API URL. Defaults to `http://127.0.0.1:11434/api/chat` for Ollama. |
| `CHAT_API_KEY` | Optional API key for chat providers that require one. |
| `EMBEDDING_PROVIDER` | Active embedding provider. Defaults to `ollama`. |
| `EMBEDDING_MODEL` | Embedding model. Defaults to `nomic-embed-text`. |
| `EMBEDDING_BASE_URL` | Embedding API URL. Defaults to `http://127.0.0.1:11434/api/embed` for Ollama. |
| `EMBEDDING_API_KEY` | Optional API key for embedding providers that require one. |
| `OLLAMA_HOST` | Base host for Ollama defaults. Defaults to `http://127.0.0.1:11434`. |
| `CHAT_SETTINGS_PATH` | Optional override for the persisted runtime settings file. Defaults to `data/chat_settings.json`. |
| `RAG_DOCS_DIR` | Directory of source documents for ingestion (`scripts/rag_ingest.py`). |
| `RAG_CHUNK_STORE_PATH` | Output JSONL manifest of chunked documents (chunk store). |
| `RAG_INDEX_PATH` | Output JSONL embeddings index used at runtime. |

OpenAI-compatible providers remain supported as a fallback, but OpenAI-specific defaults are no longer the primary path.

## Runtime validation

When `RAG_MODE=documents`, the chatbot validates that both the chunk store (`RAG_CHUNK_STORE_PATH`) and index (`RAG_INDEX_PATH`) exist. If either file is missing, startup fails with a clear error to prompt reindexing.

## Reindexing workflow

Rebuild the RAG artifacts end-to-end with:

```bash
python scripts/rag_reindex.py \
  --docs-dir "$RAG_DOCS_DIR" \
  --chunk-store-path "$RAG_CHUNK_STORE_PATH" \
  --index-path "$RAG_INDEX_PATH"
```

Or run the ingestion and embedding steps separately:

```bash
python scripts/rag_ingest.py --docs-dir "$RAG_DOCS_DIR" --manifest-path "$RAG_CHUNK_STORE_PATH"
python scripts/rag_embed.py \
  --manifest-path "$RAG_CHUNK_STORE_PATH" \
  --embeddings-path "$RAG_INDEX_PATH" \
  --embedding-provider ollama \
  --embedding-model nomic-embed-text
```

If you need to target a non-default Ollama host:

```bash
set OLLAMA_HOST=http://your-host:11434
python scripts/rag_embed.py --manifest-path "$RAG_CHUNK_STORE_PATH" --embeddings-path "$RAG_INDEX_PATH"
```
