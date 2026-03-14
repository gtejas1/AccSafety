from datetime import datetime, timedelta, timezone
import sys
import types


def _stub_module(name, **attrs):
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules[name] = module


_stub_module("pbc_trail_app", create_trail_dash=lambda *args, **kwargs: None)
_stub_module("pbc_eco_app", create_eco_dash=lambda *args, **kwargs: None)
_stub_module(
    "vivacity_app",
    create_vivacity_dash=lambda *args, **kwargs: None,
    get_countline_counts=lambda *args, **kwargs: None,
    _align_range_to_bucket=lambda start, end, bucket: (start, end),
)
_stub_module("wisdot_files_app", create_wisdot_files_app=lambda *args, **kwargs: None)
_stub_module("live_detection_app", create_live_detection_app=lambda *args, **kwargs: None)
_stub_module("se_wi_trails_app", create_se_wi_trails_app=lambda *args, **kwargs: None)
_stub_module("unified_explore", create_unified_explore=lambda *args, **kwargs: None, ENGINE=None)

chatbot_pkg = types.ModuleType("chatbot")
sys.modules.setdefault("chatbot", chatbot_pkg)
_stub_module("chatbot.logging", ChatAuditLogger=type("ChatAuditLogger", (), {}), ChatLogRecord=dict)
_stub_module("chatbot.service", ChatService=type("ChatService", (), {}))

from auth.user_store import UserStore
from gateway import _hash_reset_token, create_server


def _make_store(tmp_path):
    store = UserStore(tmp_path / "users.json")
    store.create_user("person", "person@example.com", "oldpassword123", roles=["user"], approved=True)
    return store


def test_user_store_reset_token_round_trip(tmp_path):
    store = _make_store(tmp_path)
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=30)

    store.set_reset_token("person", "hashed-token", expires_at)
    user = store.get_user("person")

    assert user is not None
    assert user.flags["reset_token_hash"] == "hashed-token"
    assert store.get_user_by_reset_token("hashed-token").username == "person"

    store.update_password("person", "newpassword456")
    updated = store.get_user("person")

    assert updated is not None
    assert "reset_token_hash" not in updated.flags
    assert store.authenticate("person", "newpassword456") is not None
    assert store.authenticate("person", "oldpassword123") is None


def test_forgot_password_generates_reset_token_and_sends_email(monkeypatch, tmp_path):
    store = _make_store(tmp_path)
    monkeypatch.setattr("gateway.user_store", store)
    monkeypatch.setenv("ACC_SMTP_HOST", "smtp.office365.com")
    monkeypatch.setenv("ACC_SMTP_PORT", "587")
    monkeypatch.setenv("ACC_SMTP_USERNAME", "mailer@example.com")
    monkeypatch.setenv("ACC_SMTP_PASSWORD", "secret")
    monkeypatch.setenv("ACC_SMTP_FROM", "uwm-ipit@uwm.edu")
    monkeypatch.setenv("ACC_RESET_BASE_URL", "http://localhost:5000")

    sent = {}

    def fake_send(recipient_email, username, reset_token):
        sent["recipient_email"] = recipient_email
        sent["username"] = username
        sent["reset_token"] = reset_token

    monkeypatch.setattr("gateway._send_password_reset_email", fake_send)

    app = create_server()
    app.testing = True
    client = app.test_client()

    response = client.post("/forgot-password", data={"email": "person@example.com"})
    body = response.get_data(as_text=True)
    user = store.get_user("person")

    assert response.status_code == 200
    assert "If that email address is registered" in body
    assert sent["recipient_email"] == "person@example.com"
    assert sent["username"] == "person"
    assert user is not None
    assert user.flags.get("reset_token_hash") == _hash_reset_token(sent["reset_token"])


def test_register_creates_pending_user_and_sends_access_request_email(monkeypatch, tmp_path):
    store = UserStore(tmp_path / "users.json")
    monkeypatch.setattr("gateway.user_store", store)
    monkeypatch.setenv("ACC_SMTP_HOST", "smtp.office365.com")
    monkeypatch.setenv("ACC_SMTP_PORT", "587")
    monkeypatch.setenv("ACC_SMTP_USERNAME", "mailer@example.com")
    monkeypatch.setenv("ACC_SMTP_PASSWORD", "secret")
    monkeypatch.setenv("ACC_SMTP_FROM", "uwm-ipit@uwm.edu")
    monkeypatch.setenv("ACC_ACCESS_REQUEST_TO", "admin@example.com")

    sent = {}

    def fake_send(username, email, requested_at):
        sent["username"] = username
        sent["email"] = email
        sent["requested_at"] = requested_at

    monkeypatch.setattr("gateway._send_access_request_email", fake_send)

    app = create_server()
    app.testing = True
    client = app.test_client()

    response = client.post(
        "/register",
        data={
            "username": "newperson",
            "email": "newperson@example.com",
            "password": "newpassword456",
            "confirm_password": "newpassword456",
        },
    )
    body = response.get_data(as_text=True)
    user = store.get_user("newperson")

    assert response.status_code == 200
    assert "Registration received" in body
    assert user is not None
    assert user.approved is False
    assert user.email == "newperson@example.com"
    assert sent["username"] == "newperson"
    assert sent["email"] == "newperson@example.com"
    assert sent["requested_at"] == user.flags["requested_at"]


def test_reset_password_updates_credentials(monkeypatch, tmp_path):
    store = _make_store(tmp_path)
    token = "fixed-reset-token"
    store.set_reset_token(
        "person",
        _hash_reset_token(token),
        datetime.now(timezone.utc) + timedelta(minutes=30),
    )
    monkeypatch.setattr("gateway.user_store", store)

    app = create_server()
    app.testing = True
    client = app.test_client()

    response = client.post(
        f"/reset-password/{token}",
        data={"password": "newpassword456", "confirm_password": "newpassword456"},
        follow_redirects=False,
    )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/login?reset=1")
    assert store.authenticate("person", "newpassword456") is not None
    assert store.authenticate("person", "oldpassword123") is None
    assert "reset_token_hash" not in (store.get_user("person").flags or {})
