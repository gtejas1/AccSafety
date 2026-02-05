from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SENSITIVE_KEYS = {"password", "secret", "api_key", "token", "authorization", "credential"}


@dataclass
class ChatLogRecord:
    request_id: str
    username: str
    latency_ms: int | None
    token_usage: dict[str, Any] | None
    model: str | None
    retrieval_hits: int
    status: str


class ChatAuditLogger:
    def __init__(self, *, file_path: str | None = None, db_path: str | None = None) -> None:
        self.file_path = Path(file_path or os.environ.get("CHATBOT_LOG_PATH", "data/chatbot_audit.jsonl"))
        self.db_path = db_path or os.environ.get("CHATBOT_LOG_DB_PATH")
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if self.db_path:
            self._init_db(self.db_path)

    def _init_db(self, db_path: str) -> None:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chatbot_audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    latency_ms INTEGER,
                    model TEXT,
                    retrieval_hits INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    token_usage_json TEXT
                )
                """
            )
            conn.commit()

    def _sanitize(self, payload: dict[str, Any] | None) -> dict[str, Any] | None:
        if payload is None:
            return None
        clean: dict[str, Any] = {}
        for key, value in payload.items():
            if key.lower() in SENSITIVE_KEYS:
                clean[key] = "[REDACTED]"
            else:
                clean[key] = value
        return clean

    def log_chat_event(self, record: ChatLogRecord) -> None:
        created_at = datetime.now(timezone.utc).isoformat()
        token_usage = self._sanitize(record.token_usage)
        body = {
            "created_at": created_at,
            "request_id": record.request_id,
            "username": record.username,
            "latency_ms": record.latency_ms,
            "model": record.model,
            "retrieval_hits": record.retrieval_hits,
            "status": record.status,
            "token_usage": token_usage,
        }

        with self.file_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(body, ensure_ascii=False) + "\n")

        if self.db_path:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO chatbot_audit_logs
                    (created_at, request_id, username, latency_ms, model, retrieval_hits, status, token_usage_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        created_at,
                        record.request_id,
                        record.username,
                        record.latency_ms,
                        record.model,
                        record.retrieval_hits,
                        record.status,
                        json.dumps(token_usage) if token_usage is not None else None,
                    ),
                )
                conn.commit()
