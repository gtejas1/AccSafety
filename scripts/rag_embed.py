import argparse
import json
import os
from pathlib import Path

import requests

DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"


def _load_json_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _embed_text(text: str, *, model: str, api_key: str) -> list[float]:
    response = requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {api_key}"},
        json={"model": model, "input": text},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    return payload["data"][0]["embedding"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build RAG embeddings JSONL from a manifest.")
    parser.add_argument(
        "--manifest-path",
        default=os.environ.get("RAG_MANIFEST_PATH", "rag_manifest.jsonl"),
        help="Input manifest JSONL path (env: RAG_MANIFEST_PATH).",
    )
    parser.add_argument(
        "--embeddings-path",
        default=os.environ.get("RAG_EMBEDDINGS_PATH", "rag_embeddings.jsonl"),
        help="Output embeddings JSONL path (env: RAG_EMBEDDINGS_PATH).",
    )
    parser.add_argument(
        "--embedding-model",
        default=os.environ.get("RAG_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        help="OpenAI embedding model name (env: RAG_EMBEDDING_MODEL).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest_path = Path(args.manifest_path)
    embeddings_path = Path(args.embeddings_path)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is required to generate embeddings.")

    entries = _load_json_records(manifest_path)
    if not entries:
        raise SystemExit(f"No manifest entries found at {manifest_path}.")

    embeddings_path.parent.mkdir(parents=True, exist_ok=True)
    with embeddings_path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            chunk_id = entry.get("chunk_id")
            text = entry.get("text", "")
            if not chunk_id or not text.strip():
                continue
            vector = _embed_text(text, model=args.embedding_model, api_key=api_key)
            handle.write(json.dumps({"chunk_id": chunk_id, "embedding": vector}) + "\n")


if __name__ == "__main__":
    main()
