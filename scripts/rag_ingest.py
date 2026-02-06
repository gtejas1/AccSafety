import argparse
import importlib.util
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".xlsx", ".xlsm", ".csv"}

DEFAULT_CHUNK_TOKENS = 800
DEFAULT_OVERLAP_TOKENS = 120


def module_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def normalize_modified_time(path: Path) -> str:
    modified_ts = path.stat().st_mtime
    return datetime.fromtimestamp(modified_ts, tz=timezone.utc).isoformat()


def chunk_text(text: str, chunk_tokens: int, overlap_tokens: int) -> list[str]:
    tokens = re.findall(r"\S+", text)
    if not tokens:
        return []
    if chunk_tokens <= 0:
        return [" ".join(tokens)]
    if overlap_tokens >= chunk_tokens:
        overlap_tokens = max(chunk_tokens - 1, 0)
    chunks = []
    start = 0
    step = max(chunk_tokens - overlap_tokens, 1)
    while start < len(tokens):
        end = min(start + chunk_tokens, len(tokens))
        chunks.append(" ".join(tokens[start:end]))
        if end == len(tokens):
            break
        start += step
    return chunks


def extract_pdf(path: Path) -> list[dict]:
    if module_available("pdfplumber"):
        import pdfplumber

        pages = []
        with pdfplumber.open(path) as pdf:
            for index, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                pages.append({"text": text, "page_number": index})
        return pages
    if module_available("pypdf"):
        from pypdf import PdfReader

        reader = PdfReader(str(path))
        pages = []
        for index, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            pages.append({"text": text, "page_number": index})
        return pages
    raise RuntimeError("No PDF parser available. Install pdfplumber or pypdf.")


def extract_docx(path: Path) -> list[dict]:
    if not module_available("docx"):
        raise RuntimeError("python-docx is required for .docx files.")
    from docx import Document

    document = Document(str(path))
    lines = [para.text for para in document.paragraphs if para.text]
    for table in document.tables:
        for row in table.rows:
            row_text = [cell.text for cell in row.cells if cell.text]
            if row_text:
                lines.append(" | ".join(row_text))
    return [{"text": "\n".join(lines)}]


def extract_excel(path: Path) -> list[dict]:
    entries = []
    excel = pd.ExcelFile(path)
    for sheet_name in excel.sheet_names:
        df = excel.parse(sheet_name=sheet_name)
        text = df.to_csv(index=False)
        entries.append({"text": text, "sheet_name": sheet_name})
    return entries


def extract_csv(path: Path) -> list[dict]:
    df = pd.read_csv(path)
    return [{"text": df.to_csv(index=False)}]


def extract_text(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return extract_pdf(path)
    if suffix == ".docx":
        return extract_docx(path)
    if suffix in {".xlsx", ".xlsm"}:
        return extract_excel(path)
    if suffix == ".csv":
        return extract_csv(path)
    raise ValueError(f"Unsupported file type: {suffix}")


def scan_documents(docs_dir: Path) -> list[Path]:
    return [
        path
        for path in docs_dir.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS
    ]


def build_manifest_entries(
    path: Path, chunk_tokens: int, overlap_tokens: int
) -> list[dict]:
    entries = []
    try:
        extracted_sections = extract_text(path)
    except Exception as exc:
        logging.warning("Skipping %s due to extraction error: %s", path, exc)
        return entries

    file_metadata = {
        "source_name": path.name,
        "source_path": str(path.resolve()),
        "modified_time": normalize_modified_time(path),
        "file_type": path.suffix.lower().lstrip("."),
    }

    for section in extracted_sections:
        text = section.get("text", "")
        if not text.strip():
            logging.warning("No text extracted for %s (metadata: %s)", path, section)
            continue
        chunks = chunk_text(text, chunk_tokens, overlap_tokens)
        for idx, chunk in enumerate(chunks):
            metadata = file_metadata | {
                "chunk_index": idx,
                "total_chunks": len(chunks),
            }
            for key in ("sheet_name", "page_number"):
                if key in section:
                    metadata[key] = section[key]
            entries.append(
                {
                    "chunk_id": f"{path.resolve()}::{section.get('sheet_name') or section.get('page_number') or 'body'}::{idx}",
                    "text": chunk,
                    "metadata": metadata,
                }
            )
    return entries


def write_manifest(entries: list[dict], manifest_path: Path) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with manifest_path.open("w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="RAG ingestion manifest builder.")
    parser.add_argument(
        "--docs-dir",
        default=os.environ.get("RAG_DOCS_DIR", "data"),
        help="Directory to scan for documents (env: RAG_DOCS_DIR).",
    )
    parser.add_argument(
        "--manifest-path",
        default=os.environ.get("RAG_MANIFEST_PATH", "rag_manifest.jsonl"),
        help="Output JSONL manifest path (env: RAG_MANIFEST_PATH).",
    )
    parser.add_argument(
        "--chunk-tokens",
        type=int,
        default=DEFAULT_CHUNK_TOKENS,
        help="Approximate tokens per chunk.",
    )
    parser.add_argument(
        "--overlap-tokens",
        type=int,
        default=DEFAULT_OVERLAP_TOKENS,
        help="Approximate overlap tokens between chunks.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    docs_dir = Path(args.docs_dir)
    if not docs_dir.exists():
        raise SystemExit(f"Docs directory not found: {docs_dir}")

    logging.info("Scanning %s for RAG documents.", docs_dir)
    files = scan_documents(docs_dir)
    logging.info("Found %d supported documents.", len(files))

    all_entries = []
    for path in files:
        all_entries.extend(
            build_manifest_entries(path, args.chunk_tokens, args.overlap_tokens)
        )

    write_manifest(all_entries, Path(args.manifest_path))
    logging.info(
        "Wrote %d chunks to %s.", len(all_entries), args.manifest_path
    )


if __name__ == "__main__":
    main()
