# AccSafety RAG Configuration

This repository includes a lightweight document RAG pipeline for the chatbot service. The configuration is driven by environment variables so deployments can point at different document stores and embedding providers.

## Required environment variables

| Variable | Purpose |
| --- | --- |
| `RAG_DOCS_DIR` | Directory of source documents for ingestion (`scripts/rag_ingest.py`). |
| `RAG_CHUNK_STORE_PATH` | Output JSONL manifest of chunked documents (chunk store). |
| `RAG_INDEX_PATH` | Output JSONL embeddings index used at runtime. |
| `EMBEDDING_MODEL` | Embedding model name for the OpenAI embeddings API. |
| `OPENAI_API_KEY` | API key used for embedding requests. |

## Optional environment variables

| Variable | Purpose |
| --- | --- |
| `EMBEDDING_BASE_URL` | Override the embedding API base URL. Defaults to `https://api.openai.com/v1/embeddings`. |

## Runtime validation

When `RAG_MODE=documents`, the chatbot boot sequence validates that both the chunk store (`RAG_CHUNK_STORE_PATH`) and index (`RAG_INDEX_PATH`) exist. If either file is missing, startup fails with a clear error to prompt reindexing.

## Reindexing workflow

You can rebuild the RAG artifacts end-to-end with the helper script below. Make sure `OPENAI_API_KEY` is set and the embedding environment variables point at the desired model and base URL.

```bash
python scripts/rag_reindex.py \
  --docs-dir "$RAG_DOCS_DIR" \
  --chunk-store-path "$RAG_CHUNK_STORE_PATH" \
  --index-path "$RAG_INDEX_PATH"
```

Alternatively, you can run the ingestion and embedding steps separately:

```bash
python scripts/rag_ingest.py --docs-dir "$RAG_DOCS_DIR" --manifest-path "$RAG_CHUNK_STORE_PATH"
python scripts/rag_embed.py --manifest-path "$RAG_CHUNK_STORE_PATH" --embeddings-path "$RAG_INDEX_PATH"
```
