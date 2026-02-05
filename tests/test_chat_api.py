from gateway import create_server


def _login(client, username="admin"):
    with client.session_transaction() as session:
        session["user"] = username
        session["roles"] = ["admin"]


def test_chat_requires_authentication():
    app = create_server()
    app.testing = True
    client = app.test_client()

    response = client.post("/api/chat", json={"message": "hi"}, follow_redirects=False)

    assert response.status_code == 302
    assert "/login" in response.headers["Location"]


def test_chat_payload_validation_errors():
    app = create_server()
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

    app = create_server()
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
