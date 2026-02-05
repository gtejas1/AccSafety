# gateway.py
import json
import math
import os
from datetime import datetime, timedelta, timezone
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
from flask import current_app
from auth.user_store import UserStore
from chatbot.service import ChatService


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

UNIFIED_SEARCH_SQL = """
    SELECT
        "Location",
        "Longitude",
        "Latitude",
        "Total counts",
        "Source",
        "Facility type",
        "Mode"
    FROM unified_site_summary
    WHERE "Location" ILIKE %(pattern)s
"""

UNIFIED_NEARBY_SQL = """
    SELECT
        "Location",
        "Longitude",
        "Latitude",
        "Total counts",
        "Source",
        "Facility type",
        "Mode"
    FROM unified_site_summary
    WHERE "Longitude" IS NOT NULL
      AND "Latitude" IS NOT NULL
"""


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


def create_server():
    server = Flask(__name__)
    server.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev_secret_key")
    chat_service = ChatService()

    # ---- Global Auth Guard ----
    @server.before_request
    def require_login():
        path = request.path or "/"
        # allow login, registration, logout, favicon, and static assets
        if path.startswith("/static/") or path in ("/login", "/logout", "/register", "/favicon.ico"):
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

        return render_template("login.html", error=error, nxt=nxt)

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
                user_store.create_user(
                    form["username"],
                    form["email"],
                    password,
                    roles=["user"],
                    approved=False,
                    flags={"requested_at": datetime.utcnow().isoformat()},
                )
                success = "Registration received. An administrator will review your request."
                form = {"username": "", "email": ""}

        return render_template("register.html", error=error, success=success, form=form)

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
        return render_template("admin_users.html", users=users, message=message)

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
        return render_template("home.html", user=session.get("user", "user"))

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
                limit_val = int(value)
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
        nearby: list[dict] = []
        if matches:
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
        current_user = _current_user()
        if not current_user:
            return jsonify({"error": "Authentication required."}), 401

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
        http_status = 200 if response_payload.get("status") == "ok" else 503
        return jsonify(response_payload), http_status

    # Convenience redirects
    for p in ["trail","eco","vivacity","live","wisdot","se-wi-trails"]:
        server.add_url_rule(f"/{p}", f"{p}_no_slash", lambda p=p: redirect(f"/{p}/", code=302))

    @server.route("/guide")
    def user_guide():
        return render_template("user_guide.html", user=session.get("user", "user"))

    @server.route("/whats-new")
    def whats_new():
        entries = load_whats_new_entries()
        return render_template("whats_new.html", entries=entries, user=session.get("user", "user"))

    return server


if __name__ == "__main__":
    app = create_server()
    app.run(host="127.0.0.1", port=5000, debug=False)
