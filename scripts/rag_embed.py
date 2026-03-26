import argparse
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any

from chatbot.embeddings import EmbeddingProviderConfig, build_embedding_provider
from chatbot.providers import ChatProviderError
from chatbot.settings import (
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_OLLAMA_HOST,
    DEFAULT_OPENAI_EMBEDDINGS_URL,
)


def _load_json_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    if raw.startswith("["):
        return json.loads(raw)
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _load_existing_chunk_ids(embeddings_path: Path) -> set[str]:
    """Read existing embeddings JSONL and return chunk_ids already embedded."""
    if not embeddings_path.exists():
        return set()
    existing: set[str] = set()
    with embeddings_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                cid = obj.get("chunk_id")
                if cid:
                    existing.add(str(cid))
            except json.JSONDecodeError:
                # ignore malformed lines
                continue
    return existing


def _clean_and_cap_text(text: str, max_chars: int) -> str:
    """
    Lightweight cleanup + hard cap.
    - Removes repeated 'Unnamed: <n>' spreadsheet artifacts
    - Collapses whitespace
    - Truncates to max_chars
    """
    if not text:
        return ""

    # Remove common spreadsheet export noise (optional but helps a lot)
    text = re.sub(r"(?:Unnamed:\s*\d+,?\s*)+", "", text)

    # Normalize whitespace and remove null bytes
    text = " ".join(text.replace("\x00", " ").split())

    # Hard cap
    if max_chars > 0 and len(text) > max_chars:
        text = text[:max_chars]

    return text


def _embed_batch(
    texts: list[str],
    *,
    provider_name: str,
    model: str,
    api_key: str,
    base_url: str,
) -> list[list[float]]:
    provider = build_embedding_provider(
        EmbeddingProviderConfig(
            provider=provider_name,
            model=model,
            base_url=base_url,
            api_key=api_key,
            timeout_seconds=60,
            max_retries=10,
        )
    )
    return provider.embed_texts(texts)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build RAG embeddings JSONL from a manifest.")
    parser.add_argument(
        "--manifest-path",
        default=os.environ.get(
            "RAG_CHUNK_STORE_PATH",
            os.environ.get("RAG_MANIFEST_PATH", "rag_manifest.jsonl"),
        ),
        help="Input manifest JSONL path (env: RAG_CHUNK_STORE_PATH).",
    )
    parser.add_argument(
        "--embeddings-path",
        default=os.environ.get(
            "RAG_INDEX_PATH",
            os.environ.get("RAG_EMBEDDINGS_PATH", "rag_embeddings.jsonl"),
        ),
        help="Output embeddings JSONL path (env: RAG_INDEX_PATH).",
    )
    parser.add_argument(
        "--embedding-provider",
        default=os.environ.get("EMBEDDING_PROVIDER", os.environ.get("CHAT_PROVIDER", "ollama")),
        help="Embedding provider name (env: EMBEDDING_PROVIDER).",
    )
    parser.add_argument(
        "--embedding-model",
        default=os.environ.get(
            "EMBEDDING_MODEL",
            os.environ.get("RAG_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        ),
        help="Embedding model name (env: EMBEDDING_MODEL).",
    )
    parser.add_argument(
        "--embedding-base-url",
        default=os.environ.get(
            "EMBEDDING_BASE_URL",
            (
                DEFAULT_OPENAI_EMBEDDINGS_URL
                if (os.environ.get("EMBEDDING_PROVIDER") or os.environ.get("CHAT_PROVIDER") or "ollama").strip().lower()
                == "openai"
                else f"{(os.environ.get('OLLAMA_HOST') or DEFAULT_OLLAMA_HOST).rstrip('/')}/api/embed"
            ),
        ),
        help="Embedding API base URL (env: EMBEDDING_BASE_URL).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("RAG_EMBED_BATCH_SIZE", "16")),
        help="How many chunks to embed per API request (env: RAG_EMBED_BATCH_SIZE).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="If set, skip chunk_ids already present in embeddings JSONL (resume mode).",
    )
    parser.add_argument(
        "--max-chars-per-chunk",
        type=int,
        default=int(os.environ.get("RAG_MAX_CHARS_PER_CHUNK", "4000")),
        help="Hard cap on characters per chunk before embedding (env: RAG_MAX_CHARS_PER_CHUNK).",
    )
    parser.add_argument(
        "--min-sleep",
        type=float,
        default=float(os.environ.get("RAG_EMBED_MIN_SLEEP", "0.5")),
        help="Sleep after each successful batch to smooth rate limits (env: RAG_EMBED_MIN_SLEEP).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest_path = Path(args.manifest_path)
    embeddings_path = Path(args.embeddings_path)

    provider_name = (args.embedding_provider or "ollama").strip().lower()
    api_key = (os.environ.get("EMBEDDING_API_KEY") or os.environ.get("OPENAI_API_KEY") or "").strip()
    if provider_name == "openai" and not api_key:
        raise SystemExit("EMBEDDING_API_KEY or OPENAI_API_KEY is required for OpenAI embeddings.")

    entries = _load_json_records(manifest_path)
    if not entries:
        raise SystemExit(f"No manifest entries found at {manifest_path}.")

    embeddings_path.parent.mkdir(parents=True, exist_ok=True)

    existing_ids: set[str] = set()
    if args.resume:
        existing_ids = _load_existing_chunk_ids(embeddings_path)
        print(f"[INFO] Resume enabled. Found {len(existing_ids)} existing embeddings.")

    # Filter valid entries and (optionally) skip already embedded ones
    work: list[dict] = []
    skipped_empty = 0
    for e in entries:
        chunk_id = e.get("chunk_id")
        raw_text = e.get("text", "")
        if not chunk_id or not str(raw_text).strip():
            skipped_empty += 1
            continue
        if args.resume and str(chunk_id) in existing_ids:
            continue

        clean_text = _clean_and_cap_text(str(raw_text), args.max_chars_per_chunk)
        if not clean_text.strip():
            skipped_empty += 1
            continue

        work.append({"chunk_id": str(chunk_id), "text": clean_text})

    if not work:
        print("[INFO] Nothing to embed (all chunks already embedded or invalid).")
        return

    print(
        f"[INFO] Embedding {len(work)} chunks using provider={provider_name} batch_size={args.batch_size} "
        f"max_chars_per_chunk={args.max_chars_per_chunk} model={args.embedding_model} "
        f"base_url={args.embedding_base_url}"
    )
    if skipped_empty:
        print(f"[INFO] Skipped {skipped_empty} empty/invalid chunks.")

    # Append in resume mode, otherwise overwrite
    mode = "a" if args.resume and embeddings_path.exists() else "w"

    done = 0
    with embeddings_path.open(mode, encoding="utf-8") as handle:
        for i in range(0, len(work), args.batch_size):
            batch = work[i : i + args.batch_size]
            texts = [b["text"] for b in batch]
            chunk_ids = [b["chunk_id"] for b in batch]

            try:
                embeddings = _embed_batch(
                    texts,
                    provider_name=provider_name,
                    model=args.embedding_model,
                    api_key=api_key,
                    base_url=args.embedding_base_url,
                )
            except ChatProviderError as exc:
                sleep_s = min(60.0, (2 ** max(i // max(args.batch_size, 1), 0))) + random.uniform(0.0, 1.0)
                if exc.code not in {"timeout", "rate_limited", "provider_unavailable", "network_error"}:
                    raise SystemExit(exc.public_message) from exc
                print(f"[WARN] {exc.public_message} Retrying in {sleep_s:.2f}s...")
                time.sleep(sleep_s)
                embeddings = _embed_batch(
                    texts,
                    provider_name=provider_name,
                    model=args.embedding_model,
                    api_key=api_key,
                    base_url=args.embedding_base_url,
                )

            for cid, emb in zip(chunk_ids, embeddings):
                handle.write(json.dumps({"chunk_id": cid, "embedding": emb}) + "\n")

            handle.flush()

            done += len(batch)
            print(f"[INFO] Progress: {done}/{len(work)} (last_batch={len(batch)})")

            # Smooth out bursts to reduce 429s
            if args.min_sleep > 0:
                time.sleep(args.min_sleep)

    print(f"[INFO] Wrote embeddings to: {embeddings_path}")


if __name__ == "__main__":
    main()
