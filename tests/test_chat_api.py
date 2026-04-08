from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


def _load_gateway(monkeypatch):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    sys.modules.pop("gateway", None)
    monkeypatch.setitem(sys.modules, "pbc_trail_app", types.SimpleNamespace(create_trail_dash=lambda *args, **kwargs: None))
    monkeypatch.setitem(sys.modules, "pbc_eco_app", types.SimpleNamespace(create_eco_dash=lambda *args, **kwargs: None))
    monkeypatch.setitem(
        sys.modules,
        "vivacity_app",
        types.SimpleNamespace(
            create_vivacity_dash=lambda *args, **kwargs: None,
            get_countline_counts=lambda *args, **kwargs: None,
            _align_range_to_bucket=lambda start, end, bucket: (start, end),
        ),
    )
    monkeypatch.setitem(sys.modules, "wisdot_files_app", types.SimpleNamespace(create_wisdot_files_app=lambda *args, **kwargs: None))
    monkeypatch.setitem(sys.modules, "live_detection_app", types.SimpleNamespace(create_live_detection_app=lambda *args, **kwargs: None))
    monkeypatch.setitem(sys.modules, "se_wi_trails_app", types.SimpleNamespace(create_se_wi_trails_app=lambda *args, **kwargs: None))
    monkeypatch.setitem(
        sys.modules,
        "unified_explore",
        types.SimpleNamespace(create_unified_explore=lambda *args, **kwargs: None, ENGINE=object()),
    )
    monkeypatch.setitem(
        sys.modules,
        "explore_data",
        types.SimpleNamespace(UNIFIED_NEARBY_SQL="SELECT 1", UNIFIED_SEARCH_SQL="SELECT 1"),
    )
    monkeypatch.setitem(sys.modules, "upload_service", types.SimpleNamespace(ensure_tables=lambda engine: None))
    return importlib.import_module("gateway")


def _login(client, username="admin"):
    with client.session_transaction() as session:
        session["user"] = username
        session["roles"] = ["admin"]


def test_chat_requires_authentication(monkeypatch):
    gateway = _load_gateway(monkeypatch)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()

    response = client.post("/api/chat", json={"message": "hi"}, follow_redirects=False)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_chat_payload_validation_errors(monkeypatch):
    gateway = _load_gateway(monkeypatch)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client)

    response = client.post("/api/chat", json={"message": ""})
    assert response.status_code == 400

    response = client.post("/api/chat", json={"message": "hello", "history": "bad"})
    assert response.status_code == 400

    response = client.post("/api/chat", json={"message": "hello", "mode": 123})
    assert response.status_code == 400


def test_chat_success(monkeypatch):
    captured = {}

    def fake_generate_reply(self, *, message, history=None, user_context=None, mode=None):
        captured["message"] = message
        captured["history"] = history
        captured["user_context"] = user_context
        captured["mode"] = mode
        return {
            "answer": "mocked",
            "sources": [{"title": "doc", "url": "https://example.com"}],
            "latency_ms": 5,
            "model": "mock-model",
            "status": "ok",
        }

    monkeypatch.setattr("chatbot.service.ChatService.generate_reply", fake_generate_reply)

    gateway = _load_gateway(monkeypatch)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client)

    response = client.post(
        "/api/chat",
        json={
            "message": "What is the latest count?",
            "history": [{"role": "user", "content": "previous question"}],
            "mode": "concise",
        },
    )

    payload = response.get_json()
    assert response.status_code == 200
    assert payload["answer"] == "mocked"
    assert payload["model"] == "mock-model"
    assert payload["status"] == "ok"
    assert captured["message"] == "What is the latest count?"
    assert captured["history"] == [{"role": "user", "content": "previous question"}]
    assert captured["user_context"]["username"] == "admin"
    assert captured["mode"] == "concise"


def test_chat_forbidden_when_role_not_allowed(monkeypatch):
    monkeypatch.setenv("CHATBOT_ALLOWED_ROLES", "admin")
    gateway = _load_gateway(monkeypatch)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    with client.session_transaction() as session:
        session["user"] = "ipit"
        session["roles"] = ["user"]

    response = client.post("/api/chat", json={"message": "hello"})

    assert response.status_code == 403


def test_chat_includes_request_id(monkeypatch):
    def fake_generate_reply(self, *, message, history=None, user_context=None, mode=None):
        return {
            "answer": "ok",
            "sources": [],
            "latency_ms": 1,
            "model": "mock-model",
            "status": "ok",
            "retrieval": {"evidence_count": 1},
        }

    monkeypatch.setattr("chatbot.service.ChatService.generate_reply", fake_generate_reply)
    gateway = _load_gateway(monkeypatch)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client)

    response = client.post("/api/chat", json={"message": "hello"}, headers={"X-Request-ID": "req-123"})
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["request_id"] == "req-123"
