# Document RAG (OpenAI)

This app can optionally retrieve evidence from a local JSONL embedding index built from PDFs, DOCX, and Excel workbooks.

## Build the document index

```bash
export OPENAI_API_KEY="..."
python scripts/build_doc_index.py /path/to/documents data/document_index.jsonl
```

Supported file types:
- PDF (`.pdf`)
- Word (`.docx`)
- Excel (`.xlsx`, `.xlsm`)

## Configure the chatbot

Set these environment variables before running the app:

```bash
export CHAT_PROVIDER="openai"
export CHAT_DOCUMENT_INDEX="data/document_index.jsonl"
export CHAT_EMBEDDING_MODEL="text-embedding-3-large"
```

Optional overrides:
- `CHAT_EMBEDDING_BASE_URL` (default: `https://api.openai.com/v1/embeddings`)
- `CHAT_EMBEDDING_TIMEOUT` (default: `30`)

When `CHAT_DOCUMENT_INDEX` is set, the chatbot will retrieve evidence from both the existing site summary data and the document index.
