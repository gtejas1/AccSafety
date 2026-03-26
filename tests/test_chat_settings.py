from __future__ import annotations

import importlib
import json
import sys
import types
from pathlib import Path


def _login(client, username="admin", roles=None):
    with client.session_transaction() as session:
        session["user"] = username
        session["roles"] = roles or ["admin"]


def _load_gateway(monkeypatch):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    sys.modules.pop("gateway", None)
    monkeypatch.setitem(
        sys.modules, "pbc_trail_app", types.SimpleNamespace(create_trail_dash=lambda *args, **kwargs: None)
    )
    monkeypatch.setitem(
        sys.modules, "pbc_eco_app", types.SimpleNamespace(create_eco_dash=lambda *args, **kwargs: None)
    )
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

    gateway = importlib.import_module("gateway")
    monkeypatch.setattr(gateway, "create_trail_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_eco_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_vivacity_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_live_detection_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_wisdot_files_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_se_wi_trails_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_unified_explore", lambda *args, **kwargs: None)
    return gateway


def test_admin_chat_settings_requires_admin(monkeypatch, tmp_path):
    gateway = _load_gateway(monkeypatch)
    monkeypatch.setattr(gateway, "CHAT_SETTINGS_PATH", tmp_path / "chat_settings.json")

    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client, username="ipit", roles=["user"])

    response = client.get("/admin/chat-settings")

    assert response.status_code == 403


def test_admin_chat_settings_persists_values(monkeypatch, tmp_path):
    gateway = _load_gateway(monkeypatch)
    settings_path = tmp_path / "chat_settings.json"
    monkeypatch.setattr(gateway, "CHAT_SETTINGS_PATH", settings_path)

    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client)

    response = client.post(
        "/admin/chat-settings",
        data={
            "chat_provider": "ollama",
            "chat_model": "llama3.2",
            "chat_base_url": "http://127.0.0.1:11434/api/chat",
            "chat_api_key": "",
            "embedding_provider": "ollama",
            "embedding_model": "nomic-embed-text",
            "embedding_base_url": "http://127.0.0.1:11434/api/embed",
            "embedding_api_key": "",
        },
    )

    assert response.status_code == 200
    assert settings_path.exists()

    stored = json.loads(settings_path.read_text(encoding="utf-8"))
    assert stored["chat_provider"] == "ollama"
    assert stored["chat_model"] == "llama3.2"
    assert stored["embedding_model"] == "nomic-embed-text"
    assert "Assistant backend settings updated." in response.get_data(as_text=True)
