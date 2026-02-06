from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pdfplumber
from docx import Document

from chatbot.document_rag import OpenAIEmbedder, _normalize_text
from chatbot.providers import ChatProviderError


def _chunk_text(text: str, *, chunk_size: int = 1200, overlap: int = 200) -> list[str]:
    cleaned = _normalize_text(text)
    if not cleaned:
        return []
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be larger than overlap")
    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(len(cleaned), start + chunk_size)
        chunks.append(cleaned[start:end])
        start = end - overlap
        if start < 0:
            start = 0
        if end == len(cleaned):
            break
    return chunks


def _iter_pdf_chunks(path: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    with pdfplumber.open(path) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            for chunk_index, chunk in enumerate(_chunk_text(text), start=1):
                metadata = {
                    "source_path": str(path),
                    "doc_type": "pdf",
                    "page": page_index,
                    "chunk": chunk_index,
                    "title": path.name,
                }
                yield chunk, metadata


def _iter_docx_chunks(path: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    doc = Document(str(path))
    paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
    text = "\n".join(paragraphs)
    for chunk_index, chunk in enumerate(_chunk_text(text), start=1):
        metadata = {
            "source_path": str(path),
            "doc_type": "docx",
            "chunk": chunk_index,
            "title": path.name,
        }
        yield chunk, metadata


def _iter_excel_chunks(path: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    try:
        sheets = pd.read_excel(path, sheet_name=None, engine="openpyxl")
    except Exception:
        return
    for sheet_name, df in sheets.items():
        if df.empty:
            continue
        csv_text = df.to_csv(index=False)
        for chunk_index, chunk in enumerate(_chunk_text(csv_text), start=1):
            metadata = {
                "source_path": str(path),
                "doc_type": "excel",
                "sheet": sheet_name,
                "rows": int(df.shape[0]),
                "columns": int(df.shape[1]),
                "chunk": chunk_index,
                "title": f"{path.name} ({sheet_name})",
            }
            yield chunk, metadata


def _iter_document_chunks(path: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        yield from _iter_pdf_chunks(path)
    elif suffix in {".docx"}:
        yield from _iter_docx_chunks(path)
    elif suffix in {".xlsx", ".xlsm"}:
        yield from _iter_excel_chunks(path)


def _iter_files(root: Path) -> Iterable[Path]:
    for dirpath, _, filenames in os.walk(root):
        for name in filenames:
            path = Path(dirpath) / name
            if path.suffix.lower() in {".pdf", ".docx", ".xlsx", ".xlsm"}:
                yield path


def build_index(input_dir: Path, output_file: Path, *, batch_size: int = 16) -> None:
    api_key = os.environ.get("CHAT_API_KEY") or os.environ.get("OPENAI_API_KEY") or ""
    embedder = OpenAIEmbedder(api_key=api_key)

    records: list[dict[str, Any]] = []
    with output_file.open("w", encoding="utf-8") as handle:
        for path in _iter_files(input_dir):
            for chunk, metadata in _iter_document_chunks(path):
                records.append(
                    {
                        "id": f"{metadata.get('source_path')}::{metadata.get('chunk')}",
                        "text": chunk,
                        "metadata": metadata,
                    }
                )
                if len(records) >= batch_size:
                    _flush_records(records, embedder, handle)
                    records = []

        if records:
            _flush_records(records, embedder, handle)


def _flush_records(records: list[dict[str, Any]], embedder: OpenAIEmbedder, handle) -> None:
    texts = [record["text"] for record in records]
    embeddings = embedder.embed(texts)
    for record, embedding in zip(records, embeddings):
        record["embedding"] = embedding
        handle.write(json.dumps(record) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a JSONL embedding index for document RAG.")
    parser.add_argument("input_dir", type=Path, help="Folder containing PDF/DOCX/XLSX/XLSM files.")
    parser.add_argument("output_file", type=Path, help="Path to write the JSONL index.")
    parser.add_argument("--batch-size", type=int, default=16, help="Embedding batch size.")
    args = parser.parse_args()

    try:
        build_index(args.input_dir, args.output_file, batch_size=args.batch_size)
    except ChatProviderError as exc:
        raise SystemExit(f"Embedding error: {exc.public_message}") from exc


if __name__ == "__main__":
    main()
