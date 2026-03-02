import argparse
import os
import subprocess
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="End-to-end RAG reindex (ingest + embed).")
    parser.add_argument(
        "--docs-dir",
        default=os.environ.get("RAG_DOCS_DIR", "data"),
        help="Directory to scan for documents (env: RAG_DOCS_DIR).",
    )
    parser.add_argument(
        "--chunk-store-path",
        default=os.environ.get(
            "RAG_CHUNK_STORE_PATH",
            os.environ.get("RAG_MANIFEST_PATH", "rag_manifest.jsonl"),
        ),
        help="Output manifest JSONL path (env: RAG_CHUNK_STORE_PATH).",
    )
    parser.add_argument(
        "--index-path",
        default=os.environ.get(
            "RAG_INDEX_PATH",
            os.environ.get("RAG_EMBEDDINGS_PATH", "rag_embeddings.jsonl"),
        ),
        help="Output embeddings JSONL path (env: RAG_INDEX_PATH).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    docs_dir = Path(args.docs_dir)
    if not docs_dir.exists():
        raise SystemExit(f"RAG docs directory not found at {docs_dir} (set RAG_DOCS_DIR).")

    ingest_cmd = [
        "python",
        "scripts/rag_ingest.py",
        "--docs-dir",
        str(docs_dir),
        "--manifest-path",
        str(args.chunk_store_path),
    ]
    embed_cmd = [
        "python",
        "scripts/rag_embed.py",
        "--manifest-path",
        str(args.chunk_store_path),
        "--embeddings-path",
        str(args.index_path),
    ]

    print("[INFO] Running:", " ".join(ingest_cmd))
    subprocess.run(ingest_cmd, check=True)
    print("[INFO] Running:", " ".join(embed_cmd))
    subprocess.run(embed_cmd, check=True)


if __name__ == "__main__":
    main()
