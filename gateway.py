# gateway.py
import json
import math
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from pathlib import Path
from urllib.parse import quote
from flask import (
    Flask,
    jsonify,
    render_template,
    render_template_string,
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
from unified_explore import create_unified_explore
from flask import current_app


BASE_DIR = Path(__file__).resolve().parent

VALID_USERS = {"admin": "IPIT&uwm2024", "ipit": "IPIT&uwm2024"}
PROTECTED_PREFIXES = ("/", "/eco/", "/trail/", "/vivacity/", "/live/", "/wisdot/", "/se-wi-trails/")


SPARKLINE_CACHE_TTL = timedelta(seconds=55)
_SPARKLINE_CACHE: Dict[str, object] = {"expires": None, "payload": None}
DEFAULT_PORTAL_VIVACITY_IDS = ["54315", "54316", "54317", "54318"]


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
            "message": "No Vivacity countline IDs configured. Set PORTAL_VIVACITY_IDS or VIVACITY_DEFAULT_IDS.",
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
            current_app.logger.warning("Vivacity sparkline fetch failed", exc_info=exc)
        except Exception:
            pass

        return {
            "status": "fallback",
            "message": "Live counts are temporarily unavailable. Showing a simulated 24-hour trend instead.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    if df.empty:
        return {
            "status": "fallback",
            "message": "Vivacity API returned no data in the last 24 hours.",
            "points": _placeholder_series(now_utc),
            "last_updated": now_utc.isoformat().replace("+00:00", "Z"),
        }

    try:
        # Clean and aggregate
        df = df.dropna(subset=["count"])
        if df.empty:
            raise ValueError("Vivacity counts contained no numeric values")

        df = df.groupby("timestamp", as_index=False)["count"].sum()
        df = df.sort_values("timestamp")
    except Exception as exc:  # pandas defensive branch
        try:
            current_app.logger.warning("Vivacity sparkline processing failed", exc_info=exc)
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
            "message": "Vivacity data was empty after processing.",
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

    # ---- Global Auth Guard ----
    @server.before_request
    def require_login():
        path = request.path or "/"
        # allow login, logout, favicon, and static assets
        if path.startswith("/static/") or path in ("/login", "/logout", "/favicon.ico"):
            return None
        if path.startswith(PROTECTED_PREFIXES) and "user" not in session:
            full = request.full_path
            next_target = full[:-1] if full.endswith("?") else full
            return redirect(f"/login?next={quote(next_target)}", code=302)
        return None

    # ---- Login / Logout ----
    @server.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            u = (request.form.get("username") or "").strip()
            p = request.form.get("password") or ""
            if u in VALID_USERS and VALID_USERS[u] == p:
                session["user"] = u
                nxt = request.args.get("next") or "/"
                if not nxt.startswith("/"):
                    nxt = "/"
                return redirect(nxt, code=302)
            error = "Invalid username or password."

        nxt = request.args.get("next", "/")
        # Styled login with policy modal and show-password toggle
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Sign in · AccSafety</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <style>
    .login-card h1 { margin: 0 0 12px; font-size: 1.4rem; }
    .login-card p { margin: 0 0 20px; color: var(--brand-muted); }
    .login-card label { display: block; margin: 12px 0 6px; font-weight: 600; font-size: 0.9rem; color: #0b1736; }
    .login-card input[type="text"], .login-card input[type="password"] {
      width: 100%; padding: 12px 14px; border-radius: 10px; border: 1px solid rgba(15, 23, 42, 0.16);
      background: #f8fafc; font-size: 0.95rem;
    }
    .login-card button {
      width: 100%; margin-top: 20px; padding: 12px 16px; border: none; border-radius: 999px;
      background: linear-gradient(130deg, var(--brand-primary), var(--brand-secondary)); color: white; font-weight: 600;
      cursor: pointer; font-size: 1rem; box-shadow: 0 14px 30px rgba(11, 102, 195, 0.28);
    }
    .login-card button:hover { filter: brightness(1.05); }
    .login-card button:disabled { filter: grayscale(0.4); cursor: not-allowed; box-shadow: none; opacity: 0.7; }
    .login-card .showpw { margin-top: 10px; display: flex; align-items: center; gap: 8px; font-size: 0.85rem; color: #0b1736; }
    .login-card .error { margin-top: 12px; color: #b91c1c; font-weight: 600; font-size: 0.9rem; }

    .notice-backdrop { position: fixed; inset: 0; background: rgba(12, 23, 42, 0.72); display: flex; align-items: center; justify-content: center; padding: 20px; z-index: 999; }
    .notice-card { max-width: 540px; width: 100%; background: #ffffff; border-radius: 18px; box-shadow: 0 24px 60px rgba(11, 23, 54, 0.32); padding: 28px 32px; color: #0b1736; display: grid; gap: 18px; }
    .notice-card h2 { margin: 0; font-size: 1.35rem; }
    .notice-card p { margin: 0; line-height: 1.55; }
    .notice-actions { display: flex; gap: 12px; justify-content: flex-end; flex-wrap: wrap; }
    .notice-actions button { border-radius: 999px; border: none; padding: 10px 18px; font-weight: 600; cursor: pointer; font-size: 0.95rem; }
    .notice-actions .primary { background: linear-gradient(130deg, var(--brand-primary), var(--brand-secondary)); color: #fff; box-shadow: 0 12px 26px rgba(11, 102, 195, 0.28); }
    .notice-backdrop[hidden] { display: none; }
    
    /* --- Compact hero text for less vertical space --- */
    .portal-hero-text h1 {
      font-size: 2rem;
      line-height: 1.25;
      margin-bottom: 8px;
    }
    .portal-hero-text p {
      font-size: 0.98rem;
      line-height: 1.45;
      max-width: 640px;
      margin: 6px 0 10px;
    }

    .cta-wrap {
      margin-top: 4px;
      margin-bottom: 10px;
    }

    .portal-status-card {
      padding: 18px 18px;
      border-radius: 18px;
      box-shadow: 0 18px 32px rgba(15,23,42,0.10);
    }

    .portal-metric {
      padding: 12px 14px;
      border-radius: 14px;
    }
    .portal-metric-value {
      font-size: 1.6rem;
    }

    .portal-data-card {
      padding: 14px 16px;
      border-radius: 12px;
    }
    .portal-data-value {
      font-size: 1.6rem;
    }

    /* --- Desktop layout: put hero+CTA+status in one column, metrics in another --- */
    @media (min-width: 1100px) {
      .portal-overview {
        grid-template-columns: minmax(0, 1.3fr) minmax(0, 1.1fr);
        align-items: start;
        gap: 20px;
      }

      /* Turn the left side into a 2-column grid: 
         - column 1: hero + CTA + status
         - column 2: metric cards
      */
      .portal-primary {
        display: grid;
        grid-template-columns: minmax(0, 1.1fr) minmax(0, 1fr);
        grid-template-rows: auto auto 1fr;
        grid-template-areas:
          "hero   metrics"
          "cta    metrics"
          "status metrics";
        gap: 14px 20px;
        align-items: start;
      }

      .portal-hero-text {
        grid-area: hero;
      }
      .cta-wrap {
        grid-area: cta;
        align-self: start;
      }
      .portal-primary-cards {
        grid-area: metrics;
        align-self: stretch;
      }
      .portal-status-card {
        grid-area: status;
      }

      /* Make the 3 small cards themselves compact and side-by-side */
      .portal-primary-cards {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        grid-auto-rows: 1fr;
        gap: 10px;
      }

      /* Put the big "Unique Count Sites" metric on top spanning both columns */
      .portal-metric {
        grid-column: 1 / -1;
      }

      /* Right column slideshow fills available height but doesn’t get too tall */
      .portal-secondary {
        align-self: stretch;
      }
      .portal-map-card {
        height: 100%;
        max-height: 420px;
        display: flex;
        flex-direction: column;
      }
      .portal-map-slideshow {
        flex: 1;
        min-height: 240px;
      }
    }

  </style>
</head>
<body>
  <!-- Policy gate modal -->
  <div id="policy-modal" class="notice-backdrop" role="dialog" aria-modal="true" aria-labelledby="policy-title" aria-describedby="policy-copy">
    <div class="notice-card">
      <h2 id="policy-title">User Agreement:</h2>
      <div id="policy-copy">
        <p>By continuing, you confirm that you are an authorized AccSafety user and will use this portal only for official program analysis or research purposes. All insights and downloadable data may be confidential and may include sensitive roadway safety information.</p>
        <p>You acknowledge that AccSafety and its data providers are not liable for any decisions or actions taken based on this information, and you agree to comply with all applicable privacy, security, and data handling requirements.</p>
      </div>
      <div class="notice-actions">
        <button type="button" class="primary" id="policy-accept">I Understand &amp; Agree</button>
      </div>
    </div>
  </div>

  <div class="app-shell">
    <header class="app-header">
      <img src="/static/img/accsafety-logo.png" alt="AccSafety logo" class="app-logo">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Wisconsin Pedestrian & Bicycle Activity and Safety Portal</span>
      </div>
      <nav class="app-nav">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>

    <main class="app-content">
      <div class="app-main-centered">
        <form class="app-card app-card--narrow login-card" method="post" autocomplete="off">
          <h1>Welcome Back</h1>
          <p>Enter your credentials to continue to the AccSafety Data Portal and Dashboards</p>

          <input type="hidden" name="next" value="{{ nxt }}"/>

          <label for="username">Username</label>
          <input id="username" name="username" type="text" required placeholder="e.g. admin" autofocus>

          <label for="password">Password</label>
          <input id="password" name="password" type="password" required placeholder="••••••••">
          <label class="showpw"><input id="toggle" type="checkbox"> Show password</label>

          <button type="submit" disabled>Sign in</button>
          {% if error %}<div class="error">{{ error }}</div>{% endif %}
        </form>
      </div>
    </main>
  </div>

  <script>
    (function(){
      const policyModal = document.getElementById('policy-modal');
      const acceptPolicy = document.getElementById('policy-accept');
      const submitButton = document.querySelector('.login-card button[type="submit"]');
      const usernameInput = document.getElementById('username');
      const urlParams = new URLSearchParams(window.location.search);
      const LS_KEY = 'accsafetyPolicyAccepted';

      if (urlParams.get('reset_policy') === '1') {
        try { localStorage.removeItem(LS_KEY); } catch(e){}
      }

      function enableForm() {
        policyModal.hidden = true;
        submitButton.disabled = false;
        usernameInput && usernameInput.focus();
      }

      acceptPolicy.addEventListener('click', function () {
        try { localStorage.setItem(LS_KEY, 'true'); } catch(e){}
        enableForm();
      });

      try {
        if (localStorage.getItem(LS_KEY) === 'true') {
          enableForm();
        }
      } catch(e) {
        // If localStorage blocked, enable form anyway
        enableForm();
      }

      document.getElementById('toggle').addEventListener('change', function(){
        const pw = document.getElementById('password');
        pw.type = this.checked ? 'text' : 'password';
      });
    })();
  </script>
</body>
</html>
        """, error=error, nxt=nxt)

    @server.route("/logout")
    def logout():
        session.clear()
        # optional: reset policy gate so next login shows it again
        return redirect("/login?reset_policy=1", code=302)

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
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>AccSafety Portal</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <style>
    .cta-explore {
      display:inline-flex;align-items:center;gap:10px;
      padding:10px 16px;border-radius:999px;
      background:linear-gradient(130deg,var(--brand-primary),var(--brand-secondary));
      color:#fff!important;font-weight:700;text-decoration:none;
      box-shadow:0 12px 26px rgba(11,102,195,0.28);
      position:relative;z-index:2;
    }
    .cta-wrap {margin:8px 0 12px;position:relative;z-index:2;display:flex;align-items:center;gap:12px;}
    .portal-primary-cards {display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;align-items:stretch;}
    .portal-metric {margin-top:0;display:flex;align-items:center;gap:14px;padding:16px 18px;border-radius:16px;width:100%;height:100%;
      background:#ffffff;border:1px solid rgba(148,163,184,0.25);box-shadow:0 18px 34px rgba(15,23,42,0.08);
      font-feature-settings:"tnum" on;font-variant-numeric:tabular-nums;}
    .portal-metric-icon {width:44px;height:44px;display:flex;align-items:center;justify-content:center;border-radius:50%;
      background:rgba(76,81,191,0.12);color:#4c51bf;}
    .portal-metric-icon svg {width:24px;height:24px;fill:currentColor;}
    .portal-metric-text {display:flex;flex-direction:column;line-height:1.1;}
    .portal-metric-value {font-size:1.85rem;font-weight:700;color:#0b1736;margin:0;}
    .portal-metric-label {font-size:0.95rem;color:#475569;}
    .desc {color:#000;margin:10px 0 16px;line-height:1.55;font-size:1.3rem;max-width:820px;}

    .portal-overview {display:grid;gap:24px;grid-template-columns:repeat(2,minmax(0,1fr));align-items:stretch;}
    .portal-primary {display:grid;gap:18px;align-content:start;justify-items:stretch;}
    .portal-secondary {display:flex;flex-direction:column;align-self:stretch;align-items:stretch;gap:18px;}
    .portal-data-grid {display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;width:100%;}
    .portal-data-card {background:#fff;border:1px solid rgba(148,163,184,0.28);border-radius:14px;padding:18px 20px;box-shadow:0 16px 28px rgba(15,23,42,0.08);display:flex;gap:16px;align-items:flex-start;text-align:left;}
    .portal-data-card h3 {margin:0;font-size:0.85rem;font-weight:700;color:#0b1736;letter-spacing:0.04em;text-transform:uppercase;}
    .portal-data-icon {width:46px;height:46px;border-radius:16px;background:rgba(11,23,54,0.06);color:#0b1736;display:inline-flex;align-items:center;justify-content:center;flex-shrink:0;}
    .portal-data-icon svg {width:24px;height:24px;}
    .portal-data-content {display:flex;flex-direction:column;gap:6px;}
    .portal-data-value {font-size:2rem;font-weight:700;color:#0b1736;line-height:1;}
    .portal-data-note {margin:0;font-size:0.9rem;color:#475569;line-height:1.35;}
    .portal-map-card {background:rgba(255,255,255,0.92);border:1px solid rgba(148,163,184,0.26);border-radius:18px;box-shadow:0 16px 28px rgba(15,23,42,0.1);padding:0;display:flex;flex-direction:column;align-items:center;width:100%;height:100%;max-width:none;flex:1;overflow:hidden;}
    .portal-map-heading {margin:0;font-size:1.05rem;font-weight:700;color:#0b1736;}
    .portal-map-slideshow {flex:1;width:100%;max-width:800px;position:relative;border-radius:inherit;box-shadow:none;border:none;overflow:hidden;background:none;padding:0;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:6px;}
    .portal-map-track {position:relative;width:100%;aspect-ratio:1/1;max-width:800px;}
    .portal-map-slide {margin:0;position:absolute;inset:0;border-radius:inherit;overflow:hidden;box-shadow:0 10px 20px rgba(15,23,42,0.1);opacity:0;transform:scale(1.03);transition:opacity 600ms ease,transform 900ms ease;}
    .portal-map-slide:first-child {opacity:1;transform:scale(1);}
    .portal-map-track[data-has-js] .portal-map-slide:first-child {opacity:0;transform:scale(1.03);}
    .portal-map-track[data-has-js] .portal-map-slide[data-active] {opacity:1;transform:scale(1);z-index:2;}
    .portal-map-controls {display:flex;justify-content:center;align-items:center;gap:8px;padding:12px 0 16px;}
    .portal-map-dot {width:10px;height:10px;border-radius:50%;border:0;background:rgba(15,23,42,0.25);padding:0;cursor:pointer;transition:transform 200ms ease,background 200ms ease;}
    .portal-map-dot[data-active] {background:rgba(15,23,42,0.75);transform:scale(1.25);}
    .portal-map-dot:focus-visible {outline:2px solid var(--brand-primary);outline-offset:2px;}
    .portal-map-slide img {width:100%;height:100%;display:block;object-fit:contain;background:#000;}
    .portal-hero-text {justify-self:start;}
    .portal-hero-text h1 {margin:0;font-size:2.4rem;line-height:1.2;}
    .portal-status-card {
      background:#fff;
      border-radius:20px;
      border:1px solid rgba(15,23,42,0.12);
      box-shadow:0 22px 40px rgba(15,23,42,0.12);
      padding:22px 24px;
      display:grid;
      gap:18px;
      align-content:start;
      max-width:100%;
      width:100%;
    }
    .status-card-header {
      display:flex;
      justify-content:space-between;
      gap:16px;
      align-items:flex-start;
    }
    .status-card-title {margin:0;font-size:1.15rem;font-weight:700;color:#0b1736;}
    .status-card-subtitle {margin:4px 0 0;color:#475569;font-size:0.95rem;}
    .status-card-updated {margin:0;margin-top:4px;font-size:0.85rem;color:#64748b;white-space:nowrap;}
    .status-feed-list {list-style:none;margin:0;padding:0;display:grid;gap:12px;}
    .status-feed-item {
      display:grid;
      grid-template-columns:minmax(0,1fr) auto;
      gap:16px;
      align-items:center;
      padding:16px 18px;
      border-radius:18px;
      background:linear-gradient(135deg,rgba(14,165,233,0.08),rgba(15,118,110,0.04));
      border:1px solid rgba(148,163,184,0.22);
    }
    .status-feed-main {display:flex;align-items:center;gap:14px;min-width:0;}
    .status-feed-icon {
      width:42px;height:42px;border-radius:50%;
      background:rgba(37,99,235,0.16);
      display:flex;align-items:center;justify-content:center;
      color:rgba(37,99,235,1);
      flex-shrink:0;
    }
    .status-feed-icon svg {width:22px;height:22px;fill:currentColor;}
    .status-feed-body {display:grid;gap:6px;min-width:0;}
    .status-feed-title {display:flex;justify-content:space-between;gap:12px;align-items:flex-start;}
    .status-feed-location {font-weight:650;font-size:1rem;color:#0b1736;line-height:1.3;}
    .status-feed-area {color:#475569;font-weight:500;}
    .status-feed-time {font-size:0.95rem;font-weight:600;color:var(--brand-primary);white-space:nowrap;}
    .status-feed-meta {display:flex;flex-wrap:wrap;gap:8px 12px;align-items:center;color:#475569;font-size:0.9rem;}
    .status-feed-badge {
      background:rgba(37,99,235,0.12);
      color:rgba(37,99,235,1);
      font-weight:600;
      padding:4px 10px;
      border-radius:999px;
      font-size:0.85rem;
      letter-spacing:0.01em;
      text-decoration:none;
      display:inline-flex;
      align-items:center;
    }
    .status-feed-updated {font-size:0.85rem;color:#475569;}
    .status-feed-extra {width:120px;display:flex;justify-content:flex-end;}
    .status-feed-photo {width:120px;height:70px;object-fit:cover;border-radius:10px;border:1px solid #d1d5db;box-shadow:0 4px 12px rgba(0,0,0,0.12);}
    .status-feed-sparkline {width:120px;height:40px;display:block;}
    .status-feed-sparkline path {stroke:rgba(37,99,235,1);stroke-width:3;fill:none;stroke-linecap:round;stroke-linejoin:round;opacity:0.9;}
    .status-feed-sparkline circle {fill:rgba(37,99,235,1);}
    @media (max-width:720px) {
      .status-card-header {flex-direction:column;align-items:flex-start;}
      .status-card-updated {white-space:normal;}
      .status-feed-item {grid-template-columns:1fr;}
      .status-feed-extra {width:100%;justify-content:flex-start;}
      .status-feed-photo {width:100%;height:120px;}
      .status-feed-sparkline {width:100%;max-width:180px;}
      .status-feed-time {font-size:0.9rem;}
    }
    @media (max-width: 960px) {
      .portal-overview {grid-template-columns:1fr;gap:20px;}
      .portal-secondary {display:grid;align-self:auto;}
      .portal-map-card {max-width:100%;height:auto;}
      .portal-map-slideshow {padding:0;max-width:100%;}
    }

    /* Info tooltip beside the CTA */
    .info-button {
      display:inline-flex;align-items:center;justify-content:center;
      width:32px;height:32px;border-radius:999px;border:1px solid rgba(15,23,42,.18);
      background:#fff;color:#0b1736;font-weight:800;cursor:pointer;
      box-shadow:0 8px 18px rgba(11,23,54,.10);
    }
    .info-button:focus { outline: 3px solid rgba(11,102,195,.35); outline-offset: 2px; }

    .tooltip {
      position:relative;display:inline-block;
    }
    .tooltip .tooltip-panel {
      position:absolute;left:50%;transform:translateX(-50%);
      bottom:120%; /* above the icon */
      background:#111827;color:#fff;padding:8px 10px;border-radius:8px;
      font-size:.9rem;line-height:1.2;white-space:nowrap;
      box-shadow:0 12px 24px rgba(0,0,0,.25);
      opacity:0;pointer-events:none;transition:opacity .12s ease, transform .12s ease;
    }
    .tooltip .tooltip-panel::after {
      content:"";position:absolute;top:100%;left:50%;transform:translateX(-50%);
      border-width:6px;border-style:solid;border-color:#111827 transparent transparent transparent;
    }
    .tooltip:focus-within .tooltip-panel,
    .tooltip:hover .tooltip-panel {
      opacity:1;pointer-events:auto;transform:translateX(-50%) translateY(-2px);
    }

    .portal-footer {
      margin-top:32px;padding:28px 32px;
      background:#f8fafc;border-top:1px solid rgba(148,163,184,0.35);
      display:flex;flex-direction:column;align-items:center;gap:24px;
      text-align:center;
    }
    .footer-logos {display:flex;align-items:center;justify-content:center;gap:28px;flex-wrap:wrap;}
    .footer-logo {display:block;max-width:860px;width:auto;height:auto;}
    .footer-logo--uwm {max-height:78px;}
    .footer-logo--wisdot {max-height:96px;}
    .footer-copyright {margin:0;color:#475569;font-size:0.95rem;}
    @media (max-width: 1200px) {
      .portal-primary-cards {grid-template-columns:repeat(2,minmax(0,1fr));}
    }
    @media (max-width: 720px) {
      .portal-primary-cards {grid-template-columns:1fr;}
      .portal-metric {max-width:none;width:100%;}
      .portal-data-grid {grid-template-columns:1fr;}
      .portal-footer {padding:24px 20px;}
      .footer-logos {gap:20px;}
      .footer-logo {max-width:220px;}
    }

    /* Modal */
    .modal-backdrop {position:fixed;inset:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:2000;}
    .modal {background:white;border-radius:14px;max-width:600px;padding:24px 30px;box-shadow:0 24px 60px rgba(0,0,0,0.25);}
    .modal h2 {margin-top:0;}
    .modal button {margin-top:18px;padding:10px 20px;border:none;border-radius:999px;background:linear-gradient(130deg,var(--brand-primary),var(--brand-secondary));color:white;font-weight:600;cursor:pointer;}
    .modal .secondary {background:#e5e7eb;color:#111827;}
    .modal-backdrop[hidden]{display:none;}
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <img src="/static/img/accsafety-logo.png" alt="AccSafety logo" class="app-logo">
      <div class="app-header-title">
        <span class="app-brand">AccSafety – Bridging Research and Practice</span>
        <span class="app-subtitle">Wisconsin Pedestrian & Bicycle Activity and Safety Portal</span>
      </div>
      <nav class="app-nav portal-nav" aria-label="Main navigation">
        <a class="app-link" href="/guide">User Guide</a>
        <a class="app-link" href="/whats-new">What's New</a>
        <a class="app-link" href="https://uwm.edu/ipit/wi-pedbike-dashboard/" target="_blank" rel="noopener noreferrer">Program Home</a>
      </nav>
      <div class="app-user">Signed in as <strong>{{ user }}</strong> · <a href="/logout">Log out</a></div>
    </header>

    <main class="app-content">
      <section class="app-card">
        <div class="portal-overview">
          <div class="portal-primary">
            <div class="portal-hero-text">
              <h1>Explore Wisconsin's Pedestrian & Bicycle Activity Data</h1>
              <p>
                Use research-backed insights, statewide counts, and planning tools tailored for researchers and practitioners focused on people walking and biking.
                Explore integrated datasets, guidance, and quick-start resources to turn analysis into on-the-ground improvements.
              </p>
            </div>

            <div class="cta-wrap">
              <a class="cta-explore" href="/explore/">Explore Available Datasets</a>
              <span class="tooltip">
                <button id="info-button" class="info-button" aria-label="Show instructions" title="Show instructions">i</button>
                <span class="tooltip-panel" role="tooltip">Click for quick instructions</span>
              </span>
            </div>

            <div class="portal-primary-cards" aria-label="Portal data and tools overview">
              <div class="portal-metric" aria-label="Count sites available">
                <span class="portal-metric-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24" role="presentation" focusable="false"><path d="M12 2.25c-3.94 0-7.29 3.04-7.29 7.08 0 3.18 2.22 6.63 6.56 10.26.49.43 1.24.43 1.73 0 4.34-3.63 6.56-7.08 6.56-10.26 0-4.04-3.35-7.08-7.56-7.08Zm0 10.65a3.57 3.57 0 1 1 0-7.14 3.57 3.57 0 0 1 0 7.14Z"/></svg>
                </span>
                <div class="portal-metric-text">
                  <span class="portal-metric-value">1000+</span>
                  <span class="portal-metric-label">Unique Count Sites</span>
                </div>
              </div>
              <article class="portal-data-card">
                <span class="portal-data-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24" role="presentation" focusable="false">
                    <circle cx="6" cy="6" r="2" fill="currentColor"></circle>
                    <circle cx="18" cy="6" r="2" fill="currentColor"></circle>
                    <circle cx="12" cy="18" r="2.6" fill="currentColor" fill-opacity="0.9"></circle>
                    <path d="M6 8v5m12-5v5M6 13h12M12 13v3" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"></path>
                  </svg>
                </span>
                <div class="portal-data-content">
                  <h3>Data Sources Available</h3>
                  <span class="portal-data-value">7</span>
                  <p class="portal-data-note">short-term & long-term counts, intersection/midblock and trails, crowdsourced data</p>
                </div>
              </article>
              <article class="portal-data-card">
                <span class="portal-data-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24" role="presentation" focusable="false">
                    <rect x="5" y="3.5" width="14" height="17" rx="2.4" ry="2.4" fill="none" stroke="currentColor" stroke-width="1.6"></rect>
                    <rect x="7" y="5.5" width="10" height="4" fill="currentColor" fill-opacity="0.12" stroke="currentColor" stroke-width="1.4" rx="1"></rect>
                    <path d="M8 13h2M8 16h2M12 13h2M12 16h2M16 13h2M16 16h2" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"></path>
                  </svg>
                </span>
                <div class="portal-data-content">
                  <h3>Research Tools You Can Run</h3>
                  <span class="portal-data-value">6</span>
                  <p class="portal-data-note">Model-based Demand Estimates, Hourly Expansion Factors, Ped/bike Crash Prediction Models</p>
                </div>
              </article>
              <article class="portal-data-card">
                <span class="portal-data-icon" aria-hidden="true">
                  <svg viewBox="0 0 24 24" role="presentation" focusable="false">
                    <path d="M4 5h16l-6.5 7.4v5.2L10 20v-7.6z" fill="currentColor"></path>
                    <path d="m4 5 7.5 7.4V20" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"></path>
                  </svg>
                </span>
                <div class="portal-data-content">
                  <h3>Analysis Options</h3>
                  <span class="portal-data-value">13</span>
                  <p class="portal-data-note">Filter by mode, facility, source</p>
                </div>
              </article>
            </div>

            <aside class="portal-status-card" aria-labelledby="status-card-title">
                <div class="status-card-header">
                  <div>
                    <h2 id="status-card-title" class="status-card-title">Real-time intersection counts</h2>
                    <p class="status-card-subtitle">Live camera &amp; API feeds</p>
                  </div>

                </div>
                <ul class="status-feed-list" aria-label="Live intersection status">
                  <li class="status-feed-item">
                    <div class="status-feed-main">
                      <span class="status-feed-icon" aria-hidden="true">
                        <svg viewBox="0 0 24 24" role="presentation" focusable="false"><path d="M12 2.25c-3.9 0-7.25 3-7.25 7.02 0 3.1 2.16 6.45 6.41 10.01.49.41 1.2.41 1.69 0 4.25-3.56 6.41-6.91 6.41-10.01 0-4.02-3.35-7.02-7.26-7.02Zm0 10.49a3.47 3.47 0 1 1 0-6.94 3.47 3.47 0 0 1 0 6.94Z"/></svg>
                      </span>
                      <div class="status-feed-body">
                        <div class="status-feed-title">
                          <span class="status-feed-location">
                            N Santa Monica Blvd &amp; Silver Spring Drive <span class="status-feed-area">– Whitefish Bay, WI</span>
                          </span>
                        </div>
                        <div class="status-feed-meta">
                          <a class="status-feed-badge" href="/live/" title="Open live detection dashboard">LIVE-Video</a>
                        </div>
                      </div>
                    </div>
                    <div class="status-feed-extra status-feed-extra--photo" aria-hidden="true">
                      <img
                        class="status-feed-photo"
                        src="/static/img/whitefish-bay-intersection.jpg"
                        alt=""
                        loading="lazy"
                      >
                    </div>
                  </li>
                  <li class="status-feed-item status-feed-item--live" data-live-card>
                    <div class="status-feed-main">
                      <span class="status-feed-icon" aria-hidden="true">
                        <svg viewBox="0 0 24 24" role="presentation" focusable="false"><path d="M12 2.25c-3.9 0-7.25 3-7.25 7.02 0 3.1 2.16 6.45 6.41 10.01.49.41 1.2.41 1.69 0 4.25-3.56 6.41-6.91 6.41-10.01 0-4.02-3.35-7.02-7.26-7.02Zm0 10.49a3.47 3.47 0 1 1 0-6.94 3.47 3.47 0 0 1 0 6.94Z"/></svg>
                      </span>
                      <div class="status-feed-body">
                        <div class="status-feed-title">
                          <span class="status-feed-location">
                            W Wells St &amp; N 68th St <span class="status-feed-area">– Milwaukee, WI</span>
                          </span>
                        </div>
                        <div class="status-feed-meta">
                          <a class="status-feed-badge" href="/vivacity/" title="Open live counts dashboard">LIVE-Counts</a>
                        </div>
                        <div class="status-feed-message" data-live-message aria-live="polite"></div>
                      </div>
                    </div>
                    <div class="status-feed-extra status-feed-extra--photo" aria-hidden="true">
                      <img
                        class="status-feed-photo"
                        src="/static/img/vivacity.jpg"
                        alt=""
                        loading="lazy"
                      >
                    </div>
                  </li>
                </ul>
              </aside>
          </div>

          <div class="portal-secondary">
            <div class="portal-map-card">
              <div class="portal-map-slideshow">
                <div class="portal-map-track">
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/1.jpg" alt="1" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/2.jpg" alt="2" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/3.jpg" alt="3" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/4.jpg" alt="4" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/5.jpg" alt="5" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/6.jpg" alt="6" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/7.jpg" alt="7" loading="lazy">
                  </figure>
                  <figure class="portal-map-slide">
                    <img src="/static/img/slides/8.jpg" alt="8" loading="lazy">
                  </figure>
                </div>
                <div class="portal-map-controls" data-map-dots role="group" aria-label="Slideshow controls" hidden></div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </main>
    <footer class="portal-footer">
      <div class="footer-logos" aria-label="Program logos">
        <img src="/static/img/UWM_IPIT.png" alt="UWM IPIT logo" class="footer-logo footer-logo--uwm">
        <img src="/static/img/WisDOT.png" alt="WisDOT logo" class="footer-logo footer-logo--wisdot">
      </div>
      <p class="footer-copyright">Copyrights ©2025 All rights reserved.</p>
    </footer>
  </div>

  <!-- Getting Started Modal -->
  <div class="modal-backdrop" id="instructions-modal" hidden role="dialog" aria-modal="true" aria-labelledby="intro-title">
    <div class="modal">
      <h2 id="intro-title">Getting Started</h2>
      <p>Use the <strong>Explore Available Datasets</strong> button to open the unified data explorer.</p>
      <p>Use top filters to refine by <em>Mode</em>, <em>Facility</em>, and <em>Data source</em>. Look for “Open” links near sites to jump to analytics or related project pages.</p>
      <div style="display:flex;gap:10px;justify-content:flex-end;">
        <button id="close-modal" class="primary">Got it</button>
        <button id="close-once" class="secondary">Dismiss (don’t remember)</button>
      </div>
    </div>
  </div>

  <script>
    (function(){
      const INTERVAL_MS = 7000;
      const motionQuery = window.matchMedia ? window.matchMedia('(prefers-reduced-motion: reduce)') : null;

      function startSlideshow(track, slides){
        if (!slides.length) { return; }

        track.dataset.hasJs = '1';

        let index = 0;
        let timerId = null;
        const dotsRoot = track.closest('.portal-map-slideshow')?.querySelector('[data-map-dots]') || null;
        const dots = [];

        if (dotsRoot) {
          dotsRoot.innerHTML = '';
          const showDots = slides.length > 1;
          dotsRoot.hidden = !showDots;

          if (showDots) {
            slides.forEach((_, slideIndex) => {
              const button = document.createElement('button');
              button.type = 'button';
              button.className = 'portal-map-dot';
              button.setAttribute('aria-label', `Show slide ${slideIndex + 1} of ${slides.length}`);
              button.addEventListener('click', () => {
                setActiveSlide(slideIndex);
                if (!motionQuery || !motionQuery.matches) {
                  stop();
                  start();
                }
              });
              dotsRoot.appendChild(button);
              dots.push(button);
            });
          }
        }

        function setActiveSlide(nextIndex){
          slides[index].removeAttribute('data-active');
          if (dots[index]) {
            dots[index].removeAttribute('data-active');
          }
          index = nextIndex;
          slides[index].setAttribute('data-active', 'true');
          if (dots[index]) {
            dots[index].setAttribute('data-active', 'true');
          }
        }

        // Ensure the first slide is active for JS-driven rotation
        slides[index].setAttribute('data-active', 'true');
        if (dots[index]) {
          dots[index].setAttribute('data-active', 'true');
        }

        if (slides.length < 2) {
          return;
        }

        const tick = () => {
          const next = (index + 1) % slides.length;
          setActiveSlide(next);
        };

        const start = () => {
          if (timerId !== null) {
            return;
          }
          timerId = window.setInterval(tick, INTERVAL_MS);
        };

        const stop = () => {
          if (timerId === null) {
            return;
          }
          window.clearInterval(timerId);
          timerId = null;
        };

        if (!motionQuery || !motionQuery.matches) {
          start();
        }

        if (motionQuery) {
          const handleMotionChange = (event) => {
            if (event.matches) {
              stop();
            } else {
              start();
            }
          };

          if (typeof motionQuery.addEventListener === 'function') {
            motionQuery.addEventListener('change', handleMotionChange);
          } else if (typeof motionQuery.addListener === 'function') {
            motionQuery.addListener(handleMotionChange);
          }
        }
      }

      document.querySelectorAll('.portal-map-track').forEach((track) => {
        const slides = track.querySelectorAll('.portal-map-slide');
        if (!slides.length) {
          return;
        }
        startSlideshow(track, slides);
      });
    })();
  </script>

  <script>
    (function(){
      const card = document.querySelector('[data-live-card]');
      if (!card) { return; }

      const API_URL = '/api/v1/vivacity/sparkline';
      const REFRESH_MS = 60_000;

      const sparkline = card.querySelector('[data-sparkline]');
      const pathEl = sparkline ? sparkline.querySelector('path') : null;
      const dotEl = sparkline ? sparkline.querySelector('circle') : null;
      const timeEl = card.querySelector('[data-live-time]');
      const updatedEl = card.querySelector('[data-live-updated]');
      const messageEl = card.querySelector('[data-live-message]');
      const globalUpdatedEl = document.querySelector('[data-live-global="updated"]');

      let lastTimestampIso = null;

      function isoToDate(iso){
        if (!iso) { return null; }
        const d = new Date(iso);
        return Number.isNaN(d.getTime()) ? null : d;
      }

      const absoluteFormatter = new Intl.DateTimeFormat(undefined, {
        month: 'short',
        day: 'numeric',
        hour: 'numeric',
        minute: '2-digit',
      });

      function formatAbsolute(date){
        if (!date) { return null; }
        try {
          return absoluteFormatter.format(date);
        } catch (err) {
          try {
            return date.toLocaleString();
          } catch (err2) {
            return date.toISOString();
          }
        }
      }

      function updateTimestampLabels(){
        const tsDate = isoToDate(lastTimestampIso);
        const absoluteLabel = formatAbsolute(tsDate);
        if (timeEl) {
          timeEl.textContent = absoluteLabel || '—';
        }
        if (updatedEl) {
          if (!tsDate) {
            updatedEl.textContent = 'Awaiting live update…';
          } else {
            updatedEl.textContent = `Updated ${absoluteLabel}`;
          }
        }
        if (globalUpdatedEl) {
          globalUpdatedEl.textContent = absoluteLabel || '—';
        }
      }

      function drawSparkline(points){
        if (!sparkline || !pathEl || !dotEl || !points.length) { return; }
        const width = 120;
        const height = 40;
        const padding = 4;
        const usableWidth = width - padding * 2;
        const usableHeight = height - padding * 2;
        const counts = points.map((p) => {
          const val = typeof p.count === 'number' ? p.count : Number(p.count);
          return Number.isFinite(val) ? val : 0;
        });
        const min = Math.min(...counts);
        const max = Math.max(...counts);
        const spread = max - min || 1;
        const step = points.length > 1 ? usableWidth / (points.length - 1) : 0;
        const coords = points.map((point, idx) => {
          const val = counts[idx];
          const x = padding + idx * step;
          const normalized = spread === 0 ? 0.5 : (val - min) / spread;
          const y = padding + (1 - normalized) * usableHeight;
          return [x, y];
        });
        const pathData = coords
          .map((coord, idx) => `${idx === 0 ? 'M' : 'L'}${coord[0].toFixed(2)} ${coord[1].toFixed(2)}`)
          .join(' ');
        pathEl.setAttribute('d', pathData || '');
        const last = coords[coords.length - 1];
        if (last) {
          dotEl.setAttribute('cx', last[0].toFixed(2));
          dotEl.setAttribute('cy', last[1].toFixed(2));
          dotEl.setAttribute('r', 3.2);
        }
      }

      async function fetchData(){
        card.setAttribute('data-live-loading', '1');
        try {
          const response = await fetch(API_URL, { cache: 'no-store' });
          const payload = await response.json();
          lastTimestampIso = payload.last_updated || null;
          if (Array.isArray(payload.points) && payload.points.length) {
            drawSparkline(payload.points);
          }

          const state = payload.status || 'error';
          card.dataset.liveState = state;

          if (messageEl) {
            const hasMessage = Boolean(payload.message);
            messageEl.textContent = hasMessage ? payload.message : '';
            messageEl.classList.toggle('status-feed-message--visible', hasMessage);
            messageEl.classList.toggle('status-feed-message--error', state === 'error');
          }
        } catch (err) {
          lastTimestampIso = null;
          card.dataset.liveState = 'error';
          if (messageEl) {
            messageEl.textContent = 'Unable to reach live data feed.';
            messageEl.classList.add('status-feed-message--visible', 'status-feed-message--error');
          }
        } finally {
          card.removeAttribute('data-live-loading');
          updateTimestampLabels();
        }
      }

      updateTimestampLabels();
      fetchData();
      setInterval(fetchData, REFRESH_MS);
    })();
  </script>

  <script>
    (function(){
      const LS_KEY = 'accsafetyIntroShown';
      const modal = document.getElementById('instructions-modal');
      const btnClose = document.getElementById('close-modal');
      const btnCloseOnce = document.getElementById('close-once');
      const infoBtn = document.getElementById('info-button');
      const params = new URLSearchParams(window.location.search);

      function safeGetLS(key){ try { return localStorage.getItem(key); } catch(e){ return null; } }
      function safeSetLS(key,val){ try { localStorage.setItem(key,val); } catch(e){} }
      function safeRemoveLS(key){ try { localStorage.removeItem(key); } catch(e){} }

      function openIntro(){ modal.removeAttribute('hidden'); }
      function closeIntro(remember){
        if (remember) safeSetLS(LS_KEY, '1');
        modal.setAttribute('hidden','');
      }

      // Flags
      if (params.get('reset_intro') === '1') safeRemoveLS(LS_KEY);
      const forceIntro = params.get('intro') === '1';

      // First visit or forced
      if (forceIntro || !safeGetLS(LS_KEY)) openIntro();

      // Open via info icon
      infoBtn.addEventListener('click', (e) => {
        e.preventDefault();
        openIntro();
      });

      // Close actions
      btnClose.addEventListener('click', () => closeIntro(true));
      btnCloseOnce.addEventListener('click', () => closeIntro(false));

      // Click outside modal to close (remember)
      modal.addEventListener('click', (e) => {
        if (e.target === modal) closeIntro(true);
      });

      // ESC to close (remember)
      document.addEventListener('keydown', (e) => {
        if (!modal.hasAttribute('hidden') && e.key === 'Escape') closeIntro(true);
      });
    })();
  </script>
</body>
</html>
        """,
        user=session.get("user", "user"),
    )

    @server.get("/api/v1/vivacity/sparkline")
    def api_vivacity_sparkline():
        payload = _get_cached_sparkline()
        status = payload.get("status")
        http_code = 200 if status in {"ok", "fallback"} else 503
        return jsonify(payload), http_code

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
