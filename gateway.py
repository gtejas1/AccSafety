# gateway.py
import hashlib
import json
import math
import os
import re
import secrets
import smtplib
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from difflib import SequenceMatcher
from typing import Dict, List
from pathlib import Path
from urllib.parse import quote
import pandas as pd
from flask import (
    Flask,
    jsonify,
    render_template,
    redirect,
    request,
    session,
)

from pbc_trail_app import create_trail_dash
from pbc_eco_app import create_eco_dash
from vivacity_app import create_vivacity_dash, get_countline_counts, _align_range_to_bucket
from wisdot_files_app import create_wisdot_files_app
from live_detection_app import create_live_detection_app
from se_wi_trails_app import create_se_wi_trails_app
from unified_explore import create_unified_explore, ENGINE
from explore_data import UNIFIED_NEARBY_SQL, UNIFIED_SEARCH_SQL
from flask import current_app
from auth.user_store import UserStore
from chatbot.logging import ChatAuditLogger, ChatLogRecord
from chatbot.service import ChatService
import upload_service


BASE_DIR = Path(__file__).resolve().parent
USER_DATA_PATH = BASE_DIR / "data" / "users.json"

PROTECTED_PREFIXES = ("/", "/eco/", "/trail/", "/vivacity/", "/live/", "/wisdot/", "/se-wi-trails/")


SPARKLINE_CACHE_TTL = timedelta(seconds=55)
_SPARKLINE_CACHE: Dict[str, object] = {"expires": None, "payload": None}
DEFAULT_PORTAL_VIVACITY_IDS = ["54315", "54316", "54317", "54318"]

DEFAULT_SEED_PASSWORD = os.environ.get("ACC_DEFAULT_PASSWORD", "IPIT&uwm2024")
user_store = UserStore(USER_DATA_PATH)
user_store.ensure_seed_users(
    {
        "admin": {
            "password": os.environ.get("ACC_ADMIN_PASSWORD", DEFAULT_SEED_PASSWORD),
            "roles": ["admin"],
            "approved": True,
            "email": os.environ.get("ACC_ADMIN_EMAIL", "admin@example.com"),
        },
        "ipit": {
            "password": os.environ.get("ACC_IPIT_PASSWORD", DEFAULT_SEED_PASSWORD),
            "roles": ["user"],
            "approved": True,
            "email": os.environ.get("ACC_IPIT_EMAIL", "ipit@example.com"),
        },
    }
)


def _portal_vivacity_ids() -> List[str]:
    raw = os.environ.get("PORTAL_VIVACITY_IDS") or os.environ.get("VIVACITY_DEFAULT_IDS") or ""
    ids = [item.strip() for item in raw.split(",") if item.strip()]
    return ids or DEFAULT_PORTAL_VIVACITY_IDS


def _placeholder_series(now_utc: datetime, points: int = 24) -> List[Dict[str, object]]:
    series: List[Dict[str, object]] = []
    if points <= 0:
        return series
    step = timedelta(hours=24) / points
    for idx in range(points):
        ts = now_utc - timedelta(hours=24) + step * (idx + 1)
        angle = (idx / max(points - 1, 1)) * math.tau
        baseline = 18 + 4 * math.sin(angle) + 2 * math.cos(angle * 2)
        value = max(0.0, round(baseline, 2))
        series.append(
            {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "count": value,
            }
        )
    return series


def _sparkline_payload(now_utc: datetime) -> Dict[str, object]:
    ids = _portal_vivacity_ids()
    if not ids:
        return {
            "status": "error",
            "message": "No countline IDs configured. Set PORTAL_VIVACITY_IDS or VIVACITY_DEFAULT_IDS.",
            "points": [],
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    # Use a 15-minute bucket and align the time range so Vivacity accepts it
    bucket = "15m"
    raw_from = now_utc - timedelta(hours=24)
    aligned_from, aligned_to = _align_range_to_bucket(raw_from, now_utc, bucket)

    try:
        df = get_countline_counts(
            ids,
            aligned_from,
            aligned_to,
            time_bucket=bucket,
            classes=["pedestrian", "cyclist"],
            fill_zeros=True,
        )
    except Exception as exc:  # defensive against API failures
        # Log full error on server, but only show a friendly message to users
        try:
            current_app.logger.warning("Sparkline fetch failed", exc_info=exc)
        except Exception:
            pass

        return {
            "status": "fallback",
            "message": "Live counts are temporarily unavailable.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    if df.empty:
        return {
            "status": "fallback",
            "message": "API returned no data in the last 24 hours.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    try:
        # Clean and aggregate
        df = df.dropna(subset=["count"])
        if df.empty:
            raise ValueError("Counts contained no numeric values")

        df = df.groupby("timestamp", as_index=False)["count"].sum()
        df = df.sort_values("timestamp")
    except Exception as exc:  # pandas defensive branch
        try:
            current_app.logger.warning("Sparkline processing failed", exc_info=exc)
        except Exception:
            pass

        return {
            "status": "fallback",
            "message": "Unable to process live data. Showing a simulated 24-hour trend instead.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    points: List[Dict[str, object]] = []
    last_ts: datetime | None = None

    for _, row in df.iterrows():
        ts = row["timestamp"]

        # Normalise to timezone-aware UTC datetime
        if hasattr(ts, "to_pydatetime"):
            ts = ts.to_pydatetime()
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)

        last_ts = ts

        count_val = float(row["count"]) if row["count"] is not None else None
        if count_val is None:
            continue

        points.append(
            {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "count": round(count_val, 2),
            }
        )

    if not points:
        return {
            "status": "fallback",
            "message": "API data was empty after processing.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    return {
        "status": "ok",
        "points": points,
        "last_updated": (last_ts or now_utc).isoformat().replace("+00:00", "Z"),
    }


def _get_cached_sparkline() -> Dict[str, object]:
    now_utc = datetime.now(timezone.utc)
    expires = _SPARKLINE_CACHE.get("expires")
    payload = _SPARKLINE_CACHE.get("payload")
    if isinstance(expires, datetime) and expires > now_utc and isinstance(payload, dict):
        return payload

    payload = _sparkline_payload(now_utc)
    _SPARKLINE_CACHE["payload"] = payload
    _SPARKLINE_CACHE["expires"] = now_utc + SPARKLINE_CACHE_TTL
    return payload


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 3958.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def _normalize_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_float(value):
    try:
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return None
        return float(value)
    except Exception:
        return None


def _normalize_location_key(value: str) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _fuzzy_location_score(query_key: str, location_key: str) -> float:
    if not query_key or not location_key:
        return 0.0
    if query_key == location_key:
        return 1.0
    if query_key in location_key:
        return 0.98

    query_tokens = [token for token in query_key.split(" ") if token]
    location_tokens = [token for token in location_key.split(" ") if token]
    if not query_tokens or not location_tokens:
        return 0.0

    token_hits = 0
    for qtok in query_tokens:
        for ltok in location_tokens:
            if ltok.startswith(qtok) or qtok.startswith(ltok):
                token_hits += 1
                break
    token_score = token_hits / len(query_tokens)

    ratio_full = SequenceMatcher(None, query_key, location_key).ratio()
    ratio_compact = SequenceMatcher(
        None,
        query_key.replace(" ", ""),
        location_key.replace(" ", ""),
    ).ratio()

    return max(ratio_full, ratio_compact, token_score)


def _fuzzy_match_locations(
    query: str,
    all_locations: list[dict],
    *,
    limit: int,
    min_score: float = 0.56,
) -> list[dict]:
    query_key = _normalize_location_key(query)
    if not query_key:
        return []

    scored: list[tuple[float, dict]] = []
    for item in all_locations:
        location = _normalize_text(item.get("Location"))
        location_key = _normalize_location_key(location)
        score = _fuzzy_location_score(query_key, location_key)
        if score >= min_score:
            scored.append((score, item))

    scored.sort(key=lambda pair: (-pair[0], _normalize_text(pair[1].get("Location")).lower()))
    return [dict(item) for _, item in scored[:limit]]


def _aggregate_locations(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    df = df.copy()
    df["Location"] = df["Location"].fillna("").astype(str).str.strip()
    results: list[dict] = []
    for location, group in df.groupby("Location"):
        location = location.strip()
        if not location:
            continue
        lon = None
        lat = None
        if "Longitude" in group.columns:
            for value in group["Longitude"].tolist():
                lon = _coerce_float(value)
                if lon is not None:
                    break
        if "Latitude" in group.columns:
            for value in group["Latitude"].tolist():
                lat = _coerce_float(value)
                if lat is not None:
                    break
        datasets = []
        for _, row in group.iterrows():
            datasets.append(
                {
                    "Source": _normalize_text(row.get("Source")),
                    "Facility type": _normalize_text(row.get("Facility type")),
                    "Mode": _normalize_text(row.get("Mode")),
                    "Total counts": (
                        None
                        if pd.isna(row.get("Total counts"))
                        else _coerce_float(row.get("Total counts"))
                    ),
                }
            )
        results.append(
            {
                "Location": location,
                "Longitude": lon,
                "Latitude": lat,
                "datasets": datasets,
            }
        )
    return results


def _compute_nearby_locations(
    matches: list[dict],
    all_locations: list[dict],
    *,
    radius_miles: float,
    limit: int,
) -> list[dict]:
    base_points = [
        match
        for match in matches
        if match.get("Latitude") is not None and match.get("Longitude") is not None
    ]
    if not base_points:
        return []
    nearby: dict[str, dict] = {}
    for base in base_points:
        base_lat = base["Latitude"]
        base_lon = base["Longitude"]
        for candidate in all_locations:
            if candidate["Location"] == base["Location"]:
                continue
            cand_lat = candidate.get("Latitude")
            cand_lon = candidate.get("Longitude")
            if cand_lat is None or cand_lon is None:
                continue
            distance = _haversine_miles(base_lat, base_lon, cand_lat, cand_lon)
            if distance > radius_miles:
                continue
            existing = nearby.get(candidate["Location"])
            if existing is None or distance < existing["distance_miles"]:
                entry = dict(candidate)
                entry["distance_miles"] = round(distance, 2)
                nearby[candidate["Location"]] = entry
    return sorted(nearby.values(), key=lambda item: item["distance_miles"])[:limit]

def _current_user():
    username = session.get("user")
    if not username:
        return None
    return user_store.get_user(username)


def _is_admin(user) -> bool:
    if not user:
        return False
    return "admin" in (user.roles or [])


def _parse_csv_set(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def _chat_access_allowed(user) -> bool:
    if not user:
        return False

    if os.environ.get("CHATBOT_ENABLED", "1").strip().lower() in {"0", "false", "no", "off"}:
        return False

    approved_only = os.environ.get("CHATBOT_REQUIRE_APPROVED", "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if approved_only and not bool(user.approved):
        return False

    approved_users = _parse_csv_set(os.environ.get("CHATBOT_APPROVED_USERS"))
    if approved_users and user.username.lower() not in approved_users:
        return False

    allowed_roles = _parse_csv_set(os.environ.get("CHATBOT_ALLOWED_ROLES"))
    if allowed_roles:
        user_roles = {str(role).strip().lower() for role in (user.roles or []) if str(role).strip()}
        if not user_roles.intersection(allowed_roles):
            return False

    return True

def load_whats_new_entries(limit: int = 15):
    """Load What's New entries from a manually curated JSON file."""

    whats_new_path = BASE_DIR / "whats_new.json"
    if not whats_new_path.exists():
        return []

    try:
        raw_entries = json.loads(whats_new_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []

    normalized_entries: List[Dict[str, object]] = []
    for raw_entry in raw_entries:
        if not isinstance(raw_entry, dict):
            continue

        version = str(raw_entry.get("version") or "").strip()
        if not version:
            continue

        version_full = str(raw_entry.get("version_full") or version).strip() or version
        date = str(raw_entry.get("date") or "").strip()

        highlights_raw = raw_entry.get("highlights", [])
        if isinstance(highlights_raw, str):
            highlights = [highlights_raw.strip()]
        else:
            highlights = [str(item).strip() for item in highlights_raw if str(item).strip()]

        if not highlights:
            continue

        links_raw = raw_entry.get("links") or []
        if isinstance(links_raw, dict):
            links_iterable = [links_raw]
        else:
            links_iterable = links_raw

        links = []
        for link in links_iterable:
            if not isinstance(link, dict):
                continue
            label = str(link.get("label") or "").strip()
            url = str(link.get("url") or "").strip()
            if label and url:
                links.append({"label": label, "url": url})

        normalized_entries.append(
            {
                "version": version,
                "version_full": version_full,
                "date": date,
                "highlights": highlights,
                "links": links or None,
            }
        )

        if len(normalized_entries) >= limit:
            break

    return normalized_entries


def _password_reset_token_ttl_minutes() -> int:
    raw = (os.environ.get("ACC_RESET_TOKEN_MINUTES") or "60").strip()
    try:
        ttl = int(raw)
    except ValueError:
        ttl = 60
    return max(5, ttl)


def _smtp_settings() -> Dict[str, object]:
    port_raw = (os.environ.get("ACC_SMTP_PORT") or "25").strip()
    try:
        port = int(port_raw)
    except ValueError:
        port = 25
    return {
        "host": (os.environ.get("ACC_SMTP_HOST") or "smtprelay.uwm.edu").strip(),
        "port": port,
        "username": (os.environ.get("ACC_SMTP_USERNAME") or "").strip(),
        "password": os.environ.get("ACC_SMTP_PASSWORD") or "",
        "sender": (os.environ.get("ACC_SMTP_FROM") or "uwm-ipit@uwm.edu").strip(),
        "use_tls": (os.environ.get("ACC_SMTP_USE_TLS") or "0").strip().lower() not in {"0", "false", "no", "off"},
        "reset_base_url": (os.environ.get("ACC_RESET_BASE_URL") or "https://accsafety.uwm.edu").strip().rstrip("/"),
        "access_request_to": (os.environ.get("ACC_ACCESS_REQUEST_TO") or "uwm-ipit@uwm.edu").strip(),
    }


def _smtp_ready() -> bool:
    settings = _smtp_settings()
    required = ("host", "sender", "reset_base_url")
    return all(str(settings[key]).strip() for key in required)


def _access_request_email_ready() -> bool:
    settings = _smtp_settings()
    required = ("host", "sender", "access_request_to")
    return all(str(settings[key]).strip() for key in required)


def _hash_reset_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _build_password_reset_url(token: str) -> str:
    base_url = str(_smtp_settings()["reset_base_url"])
    if not base_url:
        raise RuntimeError("ACC_RESET_BASE_URL is not configured")
    return f"{base_url}/reset-password/{token}"


def _send_password_reset_email(recipient_email: str, username: str, reset_token: str) -> None:
    settings = _smtp_settings()
    reset_url = _build_password_reset_url(reset_token)

    msg = EmailMessage()
    msg["Subject"] = "AccSafety password reset"
    msg["From"] = str(settings["sender"])
    msg["To"] = recipient_email
    msg.set_content(
        "Hello,\n\n"
        f"We received a password reset request for your AccSafety account ({username}).\n\n"
        f"Use this link to reset your password: {reset_url}\n\n"
        f"This link expires in {_password_reset_token_ttl_minutes()} minutes. If you did not request a reset, you can ignore this email.\n"
    )

    with smtplib.SMTP(str(settings["host"]), int(settings["port"]), timeout=30) as smtp:
        smtp.ehlo()
        if settings["use_tls"]:
            smtp.starttls()
            smtp.ehlo()
        if str(settings["username"]).strip() and str(settings["password"]):
            smtp.login(str(settings["username"]), str(settings["password"]))
        smtp.send_message(msg)


def _send_access_request_email(username: str, email: str, requested_at: str) -> None:
    settings = _smtp_settings()

    msg = EmailMessage()
    msg["Subject"] = "AccSafety access request"
    msg["From"] = str(settings["sender"])
    msg["To"] = str(settings["access_request_to"])
    msg.set_content(
        "Hello,\n\n"
        "A new AccSafety account request has been submitted.\n\n"
        f"Username: {username}\n"
        f"Email: {email}\n"
        f"Requested at: {requested_at}\n"
    )

    with smtplib.SMTP(str(settings["host"]), int(settings["port"]), timeout=30) as smtp:
        smtp.ehlo()
        if settings["use_tls"]:
            smtp.starttls()
            smtp.ehlo()
        if str(settings["username"]).strip() and str(settings["password"]):
            smtp.login(str(settings["username"]), str(settings["password"]))
        smtp.send_message(msg)


def _is_reset_token_valid(user) -> bool:
    flags = dict(getattr(user, "flags", {}) or {})
    expires_at = _parse_iso_datetime(flags.get("reset_token_expires_at"))
    return bool(expires_at and expires_at > datetime.now(timezone.utc))


def create_server():
    server = Flask(__name__)
    server.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev_secret_key")
    chat_service = ChatService()
    chat_logger = ChatAuditLogger()
    try:
        upload_service.ensure_tables(ENGINE)
    except Exception as exc:
        server.logger.warning("Failed to ensure upload tables", exc_info=exc)

    # ---- Global Auth Guard ----
    @server.before_request
    def require_login():
        path = request.path or "/"
        # allow login, registration, password reset, logout, favicon, and static assets
        if path.startswith("/static/") or path in ("/login", "/logout", "/register", "/forgot-password", "/favicon.ico") or path.startswith("/reset-password/"):
            return None

        current_user = _current_user()
        if path.startswith(PROTECTED_PREFIXES) and not current_user:
            full = request.full_path
            next_target = full[:-1] if full.endswith("?") else full
            return redirect(f"/login?next={quote(next_target)}", code=302)
        if current_user and not current_user.approved and path not in ("/logout", "/login"):
            session.clear()
            return redirect("/login", code=302)
        return None

    # ---- Login / Logout ----
    @server.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        success = "Your password has been updated. Sign in with your new password." if request.args.get("reset") == "1" else None
        nxt = request.args.get("next", request.form.get("next", "/"))
        if request.method == "POST":
            u = (request.form.get("username") or "").strip()
            p = request.form.get("password") or ""
            auth_user = user_store.authenticate(u, p)
            if auth_user:
                if not auth_user.approved:
                    error = "Your account is pending administrator approval."
                else:
                    session["user"] = auth_user.username
                    session["roles"] = auth_user.roles
                    nxt = request.form.get("next") or request.args.get("next") or "/"
                    if not nxt.startswith("/"):
                        nxt = "/"
                    return redirect(nxt, code=302)
            else:
                known_user = user_store.get_user(u)
                if known_user and not known_user.approved:
                    error = "Your account is pending administrator approval."
                else:
                    error = "Invalid username or password."

        return render_template("login.html", error=error, success=success, nxt=nxt)

    @server.route("/register", methods=["GET", "POST"])
    def register():
        error = None
        success = None
        form = {
            "username": (request.form.get("username") or "").strip(),
            "email": (request.form.get("email") or "").strip(),
        }

        if request.method == "POST":
            password = request.form.get("password") or ""
            confirm_password = request.form.get("confirm_password") or ""

            if not form["username"] or len(form["username"]) < 3:
                error = "Username must be at least 3 characters long."
            elif " " in form["username"]:
                error = "Username cannot contain spaces."
            elif not form["email"] or "@" not in form["email"]:
                error = "A valid email address is required."
            elif len(password) < 8:
                error = "Password must be at least 8 characters long."
            elif password != confirm_password:
                error = "Passwords do not match."
            elif user_store.get_user(form["username"]):
                error = "An account with that username already exists."
            elif user_store.email_exists(form["email"]):
                error = "An account with that email already exists."
            else:
                requested_at = datetime.now(timezone.utc).isoformat()
                user_store.create_user(
                    form["username"],
                    form["email"],
                    password,
                    roles=["user"],
                    approved=False,
                    flags={"requested_at": requested_at},
                )
                if _access_request_email_ready():
                    try:
                        _send_access_request_email(form["username"], form["email"], requested_at)
                    except Exception:
                        current_app.logger.exception("Failed to send access request email for %s", form["username"])
                success = "Registration received. An administrator will review your request."
                form = {"username": "", "email": ""}

        return render_template("register.html", error=error, success=success, form=form)


    @server.route("/forgot-password", methods=["GET", "POST"])
    def forgot_password():
        error = None
        success = None
        form = {"email": (request.form.get("email") or "").strip()}

        if request.method == "POST":
            if not form["email"] or "@" not in form["email"]:
                error = "A valid email address is required."
            elif not _smtp_ready():
                error = "Password reset email is not available because SMTP is not configured."
            else:
                success = "If that email address is registered, a password reset link has been sent."
                reset_user = user_store.get_user_by_email(form["email"])
                if reset_user:
                    reset_token = secrets.token_urlsafe(32)
                    expires_at = datetime.now(timezone.utc) + timedelta(minutes=_password_reset_token_ttl_minutes())
                    user_store.set_reset_token(reset_user.username, _hash_reset_token(reset_token), expires_at)
                    try:
                        _send_password_reset_email(reset_user.email, reset_user.username, reset_token)
                    except Exception:
                        current_app.logger.exception("Failed to send password reset email for %s", reset_user.username)
                        user_store.clear_reset_token(reset_user.username)
                        error = "Password reset email could not be sent. Please try again later."
                        success = None
                if success:
                    form = {"email": ""}

        return render_template("forgot_password.html", error=error, success=success, form=form)

    @server.route("/reset-password/<token>", methods=["GET", "POST"])
    def reset_password(token: str):
        error = None
        token_hash = _hash_reset_token(token)
        reset_user = user_store.get_user_by_reset_token(token_hash)

        if not reset_user or not _is_reset_token_valid(reset_user):
            if reset_user:
                user_store.clear_reset_token(reset_user.username)
            return render_template("reset_password.html", error="This password reset link is invalid or has expired.", token_valid=False)

        if request.method == "POST":
            password = request.form.get("password") or ""
            confirm_password = request.form.get("confirm_password") or ""

            if len(password) < 8:
                error = "Password must be at least 8 characters long."
            elif password != confirm_password:
                error = "Passwords do not match."
            else:
                user_store.update_password(reset_user.username, password)
                return redirect("/login?reset=1", code=302)

        return render_template("reset_password.html", error=error, token_valid=True)

    @server.route("/logout")
    def logout():
        session.clear()
        # optional: reset policy gate so next login shows it again
        return redirect("/login?reset_policy=1", code=302)

    @server.route("/admin/users", methods=["GET", "POST"])
    def manage_users():
        current_user = _current_user()
        if not _is_admin(current_user):
            return ("Forbidden", 403)

        message = None
        if request.method == "POST":
            target_username = (request.form.get("username") or "").strip()
            action = request.form.get("action")
            if action == "approve":
                updated = user_store.approve_user(target_username, True)
                if updated:
                    message = f"Approved {updated.username}."
            elif action == "revoke":
                updated = user_store.approve_user(target_username, False)
                if updated:
                    message = f"Revoked access for {updated.username}."
            elif action == "set_roles":
                raw_roles = request.form.get("roles") or ""
                roles = [role.strip() for role in raw_roles.split(",") if role.strip()]
                updated = user_store.update_roles(target_username, roles)
                if updated:
                    message = f"Updated roles for {updated.username}."

        users = user_store.list_users()
        return render_template("admin_users.html", users=users, message=message, is_admin=True)

    def _render_upload_page(*, selected_upload_id: str | None = None, message: str | None = None, error: str | None = None):
        uploads = upload_service.list_uploads(ENGINE)
        selected_upload = upload_service.get_upload_detail(ENGINE, selected_upload_id) if selected_upload_id else None
        return render_template(
            "admin_data_uploads.html",
            uploads=uploads,
            selected_upload=selected_upload,
            mode_options=upload_service.UPLOAD_MODE_OPTIONS,
            user=session.get("user", "user"),
            is_admin=True,
            message=message,
            error=error,
        )

    @server.route("/admin/data-uploads", methods=["GET", "POST"])
    def admin_data_uploads():
        current_user = _current_user()
        if not _is_admin(current_user):
            return ("Forbidden", 403)

        if request.method == "POST":
            uploaded_file = request.files.get("file")
            if uploaded_file is None or not (uploaded_file.filename or "").strip():
                return _render_upload_page(error="Choose an .xlsx file to upload.")

            file_name = uploaded_file.filename or "upload.xlsx"
            if not file_name.lower().endswith(".xlsx"):
                return _render_upload_page(error="Only .xlsx uploads are supported.")

            parsed_upload = upload_service.parse_excel_upload(
                uploaded_file.read(),
                filename=file_name,
                selected_mode=request.form.get("mode"),
                location_override=request.form.get("location_override", ""),
                notes=request.form.get("notes", ""),
            )
            upload_service.stage_upload(ENGINE, parsed_upload, uploaded_by=current_user.username)
            message = "Upload staged for review."
            if parsed_upload.status == "invalid":
                message = "Upload stored with validation errors."
            return _render_upload_page(selected_upload_id=parsed_upload.upload_id, message=message)

        selected_upload_id = (request.args.get("upload_id") or "").strip() or None
        return _render_upload_page(selected_upload_id=selected_upload_id)

    @server.get("/admin/data-uploads/<upload_id>")
    def admin_data_upload_detail(upload_id: str):
        current_user = _current_user()
        if not _is_admin(current_user):
            return ("Forbidden", 403)
        detail = upload_service.get_upload_detail(ENGINE, upload_id)
        if detail is None:
            return ("Not Found", 404)
        return _render_upload_page(selected_upload_id=upload_id)

    @server.post("/admin/data-uploads/<upload_id>/publish")
    def admin_data_upload_publish(upload_id: str):
        current_user = _current_user()
        if not _is_admin(current_user):
            return ("Forbidden", 403)
        try:
            result = upload_service.publish_upload(ENGINE, upload_id, published_by=current_user.username)
        except KeyError:
            return ("Not Found", 404)

        message = None
        error = None
        if result["status"] == "published":
            message = f"Published {result['inserted_rows']} row(s) to Explore."
        elif result["status"] == "already_published":
            message = "This upload was already published."
        else:
            error = "This upload has no valid rows to publish."
        return _render_upload_page(selected_upload_id=upload_id, message=message, error=error)

    # ---- Subapps ----
    create_trail_dash(server, prefix="/trail/")
    create_eco_dash(server, prefix="/eco/")
    create_vivacity_dash(server, prefix="/vivacity/")
    create_live_detection_app(server, prefix="/live/")
    create_wisdot_files_app(server, prefix="/wisdot/")
    create_se_wi_trails_app(server, prefix="/se-wi-trails/")
    create_unified_explore(server, prefix="/explore/")

    # ---- Portal Home ----
    @server.route("/")
    def home():
        return render_template(
            "home.html",
            user=session.get("user", "user"),
            is_admin=_is_admin(_current_user()),
        )

    @server.get("/api/v1/vivacity/sparkline")
    def api_vivacity_sparkline():
        payload = _get_cached_sparkline()
        status = payload.get("status")
        http_code = 200 if status in {"ok", "fallback"} else 503
        return jsonify(payload), http_code

    @server.get("/api/unified-search")
    def api_unified_search():
        query = _normalize_text(request.args.get("q") or request.args.get("location"))

        def _parse_radius(value, default: float) -> float:
            try:
                radius_val = float(value)
            except Exception:
                return default
            return radius_val if radius_val > 0 else default

        def _parse_limit(value, default: int) -> int:
            try:
                limit_val = int(float(value))
            except Exception:
                return default
            limit_val = max(1, limit_val)
            return min(limit_val, 50)

        radius_miles = _parse_radius(request.args.get("radius_miles"), 5.0)
        limit = _parse_limit(request.args.get("limit"), 10)

        if not query:
            return jsonify(
                {
                    "query": "",
                    "radius_miles": radius_miles,
                    "limit": limit,
                    "matches": [],
                    "nearby": [],
                }
            )

        try:
            matches_df = pd.read_sql(UNIFIED_SEARCH_SQL, ENGINE, params={"pattern": f"%{query}%"})
        except Exception:
            matches_df = pd.DataFrame(
                columns=[
                    "Location",
                    "Longitude",
                    "Latitude",
                    "Total counts",
                    "Source",
                    "Facility type",
                    "Mode",
                ]
            )

        matches = _aggregate_locations(matches_df)
        if matches:
            matches = matches[:limit]

        all_locations: list[dict] = []
        if matches or len(query.strip()) >= 3:
            try:
                all_df = pd.read_sql(UNIFIED_NEARBY_SQL, ENGINE)
            except Exception:
                all_df = pd.DataFrame(
                    columns=[
                        "Location",
                        "Longitude",
                        "Latitude",
                        "Total counts",
                        "Source",
                        "Facility type",
                        "Mode",
                    ]
                )
            all_locations = _aggregate_locations(all_df)

        if not matches and all_locations:
            # Fuzzy fallback for misspelled location names.
            matches = _fuzzy_match_locations(
                query,
                all_locations,
                limit=limit,
            )

        nearby: list[dict] = []
        if matches and all_locations:
            nearby = _compute_nearby_locations(
                matches,
                all_locations,
                radius_miles=radius_miles,
                limit=limit,
            )

        return jsonify(
            {
                "query": query,
                "radius_miles": radius_miles,
                "limit": limit,
                "matches": matches,
                "nearby": nearby,
            }
        )

    @server.post("/api/chat")
    def api_chat():
        request_started = time.perf_counter()
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        current_user = _current_user()
        if not current_user:
            return jsonify({"error": "Authentication required."}), 401
        if not _chat_access_allowed(current_user):
            return jsonify({"error": "Chatbot is not enabled for this account."}), 403

        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify({"error": "Invalid JSON payload."}), 400

        message = payload.get("message")
        if not isinstance(message, str) or not message.strip():
            return jsonify({"error": "`message` must be a non-empty string."}), 400

        history = payload.get("history", [])
        if history is None:
            history = []
        if not isinstance(history, list):
            return jsonify({"error": "`history` must be a list when provided."}), 400

        for entry in history:
            if not isinstance(entry, dict):
                return jsonify({"error": "Each `history` item must be an object."}), 400
            role = entry.get("role")
            content = entry.get("content")
            if not isinstance(role, str) or not isinstance(content, str):
                return jsonify({"error": "Each `history` item must include string `role` and `content`."}), 400

        mode = payload.get("mode")
        if mode is not None and not isinstance(mode, str):
            return jsonify({"error": "`mode` must be a string when provided."}), 400

        response_payload = chat_service.generate_reply(
            message=message.strip(),
            history=history,
            user_context={
                "username": current_user.username,
                "roles": current_user.roles or [],
                "approved": bool(current_user.approved),
            },
            mode=mode.strip() if isinstance(mode, str) else None,
        )

        latency_ms = response_payload.get("latency_ms")
        if latency_ms is None:
            latency_ms = int((time.perf_counter() - request_started) * 1000)

        retrieval_hits = 0
        retrieval = response_payload.get("retrieval")
        if isinstance(retrieval, dict):
            raw_hits = retrieval.get("evidence_count", 0)
            try:
                retrieval_hits = int(raw_hits)
            except Exception:
                retrieval_hits = 0

        token_usage = response_payload.get("usage")
        if not isinstance(token_usage, dict):
            token_usage = None

        chat_logger.log_chat_event(
            ChatLogRecord(
                request_id=request_id,
                username=current_user.username,
                latency_ms=latency_ms,
                token_usage=token_usage,
                model=response_payload.get("model"),
                retrieval_hits=retrieval_hits,
                status=str(response_payload.get("status") or "unknown"),
            )
        )

        response_payload["request_id"] = request_id
        _PROVIDER_ERROR_STATUSES = frozenset({
            "provider_error", "timeout", "network_error", "provider_unavailable",
            "invalid_response", "auth_error", "rate_limited", "bad_request",
            "config_error", "unsupported_provider",
        })
        http_status = 503 if response_payload.get("status") in _PROVIDER_ERROR_STATUSES else 200
        return jsonify(response_payload), http_status

    # Convenience redirects
    for p in ["trail","eco","vivacity","live","wisdot","se-wi-trails"]:
        server.add_url_rule(f"/{p}", f"{p}_no_slash", lambda p=p: redirect(f"/{p}/", code=302))

    @server.route("/guide")
    def user_guide():
        return render_template(
            "user_guide.html",
            user=session.get("user", "user"),
            is_admin=_is_admin(_current_user()),
        )

    @server.route("/whats-new")
    def whats_new():
        entries = load_whats_new_entries()
        return render_template(
            "whats_new.html",
            entries=entries,
            user=session.get("user", "user"),
            is_admin=_is_admin(_current_user()),
        )

    return server


if __name__ == "__main__":
    app = create_server()
    app.run(host="127.0.0.1", port=5000, debug=False)
