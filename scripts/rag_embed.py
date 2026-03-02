import argparse
import json
import os
import random
import re
import time
from pathlib import Path
from typing import Any

import requests

DEFAULT_EMBEDDING_MODEL = "text-embedding-3-large"
DEFAULT_EMBEDDING_BASE_URL = "https://api.openai.com/v1/embeddings"


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


def _post_with_retries(
    session: requests.Session,
    url: str,
    *,
    headers: dict[str, str],
    json_body: dict[str, Any],
    timeout: int = 60,
    max_retries: int = 10,
    verbose_rate_headers: bool = True,
) -> dict:
    """
    Robust POST with retries for 429 and transient 5xx errors.
    Uses exponential backoff with jitter and honors Retry-After when available.
    """
    last_status = None
    for attempt in range(max_retries):
        resp = session.post(url, headers=headers, json=json_body, timeout=timeout)
        last_status = resp.status_code

        if resp.status_code == 200:
            return resp.json()

        # Rate limit or transient server errors
        if resp.status_code in (429, 500, 502, 503, 504):
            try:
                err = resp.json()
                if "error" in err:
                    print("[OPENAI ERROR]", err["error"].get("type"), err["error"].get("code"))
                    print("[OPENAI MESSAGE]", err["error"].get("message"))
            except Exception:
                print("[OPENAI RAW]", resp.text[:500])

            if resp.status_code == 429 and verbose_rate_headers:
                print("[RATE LIMIT HEADERS]")
                for k in [
                    "Retry-After",
                    "x-ratelimit-limit-requests",
                    "x-ratelimit-remaining-requests",
                    "x-ratelimit-reset-requests",
                    "x-ratelimit-limit-tokens",
                    "x-ratelimit-remaining-tokens",
                    "x-ratelimit-reset-tokens",
                ]:
                    if k in resp.headers:
                        print(f"  {k}: {resp.headers.get(k)}")

            retry_after = resp.headers.get("Retry-After")
            sleep_s: float | None
            if retry_after:
                try:
                    sleep_s = float(retry_after)
                except ValueError:
                    sleep_s = None
            else:
                sleep_s = None

            if sleep_s is None:
                # exponential backoff + jitter
                sleep_s = min(60.0, (2 ** attempt)) + random.uniform(0.0, 1.0)

            print(
                f"[WARN] HTTP {resp.status_code}. Retrying in {sleep_s:.2f}s... "
                f"(attempt {attempt+1}/{max_retries})"
            )
            time.sleep(sleep_s)
            continue

        # Non-retryable: raise
        resp.raise_for_status()

    raise RuntimeError(f"Failed after {max_retries} retries (last status={last_status}).")


def _embed_batch(
    session: requests.Session,
    texts: list[str],
    *,
    model: str,
    api_key: str,
    base_url: str,
) -> list[list[float]]:
    """
    Embed a batch of texts in one API call.
    Returns embeddings in the same order as texts.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"model": model, "input": texts}

    data = _post_with_retries(session, base_url, headers=headers, json_body=payload, timeout=60)
    # API returns list of {index, embedding, ...}; sort by index to be safe
    items = sorted(data["data"], key=lambda d: d.get("index", 0))
    return [it["embedding"] for it in items]


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
        "--embedding-model",
        default=os.environ.get(
            "EMBEDDING_MODEL",
            os.environ.get("RAG_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
        ),
        help="OpenAI embedding model name (env: EMBEDDING_MODEL).",
    )
    parser.add_argument(
        "--embedding-base-url",
        default=os.environ.get("EMBEDDING_BASE_URL", DEFAULT_EMBEDDING_BASE_URL),
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

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is required to generate embeddings.")

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
        f"[INFO] Embedding {len(work)} chunks using batch_size={args.batch_size} "
        f"max_chars_per_chunk={args.max_chars_per_chunk} model={args.embedding_model} "
        f"base_url={args.embedding_base_url}"
    )
    if skipped_empty:
        print(f"[INFO] Skipped {skipped_empty} empty/invalid chunks.")

    session = requests.Session()

    # Append in resume mode, otherwise overwrite
    mode = "a" if args.resume and embeddings_path.exists() else "w"

    done = 0
    with embeddings_path.open(mode, encoding="utf-8") as handle:
        for i in range(0, len(work), args.batch_size):
            batch = work[i : i + args.batch_size]
            texts = [b["text"] for b in batch]
            chunk_ids = [b["chunk_id"] for b in batch]

            embeddings = _embed_batch(
                session,
                texts,
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
