import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from typing import Optional


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _smtp_port(raw_port: Optional[str], use_tls: bool) -> int:
    try:
        port = int(raw_port or "")
    except (TypeError, ValueError):
        port = 0
    if port > 0:
        return port
    return 587 if use_tls else 25


def send_access_request_email(username: str, email: str, requested_at: str) -> bool:
    logger = logging.getLogger(__name__)
    host = os.environ.get("ACC_SMTP_HOST", "").strip()
    if not host:
        logger.warning("SMTP host not configured; skipping access request email.")
        return False

    use_tls = _bool_env("ACC_SMTP_TLS", False)
    port = _smtp_port(os.environ.get("ACC_SMTP_PORT"), use_tls)
    smtp_user = os.environ.get("ACC_SMTP_USER", "").strip()
    smtp_password = os.environ.get("ACC_SMTP_PASSWORD", "")
    recipient = os.environ.get("ACC_ACCESS_REQUEST_NOTIFY", "tguduru@uwm.edu").strip()

    message = EmailMessage()
    message["Subject"] = "ACC Portal access request"
    message["To"] = recipient
    message["From"] = smtp_user or "no-reply@localhost"
    message.set_content(
        "An ACC Portal access request was submitted.\n\n"
        f"Username: {username}\n"
        f"Email: {email}\n"
        f"Requested at: {requested_at}\n"
    )

    try:
        with smtplib.SMTP(host, port) as server:
            if use_tls:
                server.starttls(context=ssl.create_default_context())
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(message)
        return True
    except Exception as exc:
        logger.warning("Failed to send access request email", exc_info=exc)
        return False
