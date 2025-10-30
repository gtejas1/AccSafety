# gateway.py
import json
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote

import pandas as pd
from flask import Flask, render_template, render_template_string, redirect, request, session

from pbc_trail_app import create_trail_dash
from pbc_eco_app import create_eco_dash
from vivacity_app import create_vivacity_dash
from wisdot_files_app import create_wisdot_files_app
from live_detection_app import create_live_detection_app
from se_wi_trails_app import create_se_wi_trails_app
from unified_explore import create_unified_explore


ASSETS_DIR = Path(__file__).resolve().parent / "assets"


def _format_int(value) -> str:
    try:
        return f"{int(round(float(value))):,}"
    except (TypeError, ValueError):
        return "0"


def _format_percent(value: float) -> str:
    try:
        return f"{float(value):.1f}%"
    except (TypeError, ValueError):
        return "0.0%"


def _format_date(timestamp) -> str:
    if timestamp is None or pd.isna(timestamp):
        return "—"
    # Remove leading zeros from the day for nicer display (e.g., "March 05" → "March 5")
    return timestamp.strftime("%A, %B %d, %Y").replace(" 0", " ")


def _format_range(start, end) -> str:
    if start is None or end is None or pd.isna(start) or pd.isna(end):
        return "—"
    same_year = start.year == end.year
    if same_year:
        left = start.strftime("%b %d").replace(" 0", " ")
        right = end.strftime("%b %d, %Y").replace(" 0", " ")
    else:
        left = start.strftime("%b %d, %Y").replace(" 0", " ")
        right = end.strftime("%b %d, %Y").replace(" 0", " ")
    return f"{left} – {right}"


def _load_mode_counts(filename: str, value_column: str, mode_label: str) -> pd.DataFrame:
    path = ASSETS_DIR / filename
    if not path.exists():
        return pd.DataFrame(columns=["location", "date", "count", "mode"])

    try:
        df = pd.read_excel(path, usecols=["Location Name", "Date", value_column])
    except ValueError:
        # Fallback: read full sheet and filter columns after the fact
        df = pd.read_excel(path)

    if "Location Name" not in df.columns or "Date" not in df.columns or value_column not in df.columns:
        return pd.DataFrame(columns=["location", "date", "count", "mode"])

    df = df.rename(columns={"Location Name": "location", value_column: "count", "Date": "date"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0)
    df = df.dropna(subset=["date"])
    df["mode"] = mode_label
    return df[["location", "date", "count", "mode"]]


@lru_cache(maxsize=1)
def get_eco_dashboard_summary() -> dict:
    frames = [
        _load_mode_counts(
            "WisconsinPedBikeCountDatabase_PedestrianCounts_032824.xlsx",
            "Total ped count during count period",
            "Pedestrian",
        ),
        _load_mode_counts(
            "WisconsinPedBikeCountDatabase_BicyclistCounts_032824.xlsx",
            "Total bike count during count period",
            "Bicyclist",
        ),
    ]

    frames = [f for f in frames if not f.empty]
    if not frames:
        return {
            "total_count": 0,
            "total_count_display": "0",
            "average_daily": 0,
            "average_daily_display": "0",
            "total_days": 0,
            "total_days_display": "0",
            "peak_total": 0,
            "peak_total_display": "0",
            "peak_day_display": "—",
            "site_count": 0,
            "site_count_display": "0",
            "date_range_display": "—",
            "mode_totals": [],
            "top_locations": [],
            "leading_location": None,
            "chart": {"labels": [], "values": [], "total": 0},
        }

    data = pd.concat(frames, ignore_index=True)
    data["count"] = data["count"].astype(float)

    total = float(data["count"].sum())
    site_totals = data.groupby("location")["count"].sum().sort_values(ascending=False)
    site_count = int(site_totals.size)

    daily = data.groupby("date")["count"].sum().sort_index()
    total_days = int(daily.size)
    avg_daily = float(total / total_days) if total_days else 0.0

    peak_day = daily.idxmax() if not daily.empty else None
    peak_total = float(daily.max()) if not daily.empty else 0.0

    min_date = data["date"].min()
    max_date = data["date"].max()

    chart_series = site_totals.head(8)
    if site_totals.size > 8:
        others = float(site_totals.iloc[8:].sum())
        if others:
            chart_series.loc["All other sites"] = others

    chart_labels = list(chart_series.index)
    chart_values = [int(round(float(v))) for v in chart_series.values]

    mode_series = data.groupby("mode")["count"].sum().sort_values(ascending=False)
    mode_totals = []
    for mode, value in mode_series.items():
        share = (float(value) / total * 100) if total else 0.0
        mode_totals.append(
            {
                "mode": mode,
                "count": int(round(float(value))),
                "display": _format_int(value),
                "share": share,
                "share_display": _format_percent(share),
            }
        )

    top_locations = []
    for rank, (location, value) in enumerate(site_totals.head(6).items(), start=1):
        share = (float(value) / total * 100) if total else 0.0
        top_locations.append(
            {
                "rank": rank,
                "location": location,
                "count": int(round(float(value))),
                "count_display": _format_int(value),
                "share": share,
                "share_display": _format_percent(share),
            }
        )

    leading_location = None
    if not site_totals.empty:
        top_location_name = site_totals.index[0]
        top_location_value = site_totals.iloc[0]
        leading_location = {
            "name": top_location_name,
            "count": int(round(float(top_location_value))),
            "count_display": _format_int(top_location_value),
        }

    summary = {
        "total_count": int(round(total)),
        "total_count_display": _format_int(total),
        "average_daily": int(round(avg_daily)),
        "average_daily_display": _format_int(avg_daily),
        "total_days": total_days,
        "total_days_display": _format_int(total_days),
        "peak_total": int(round(peak_total)),
        "peak_total_display": _format_int(peak_total),
        "peak_day_display": _format_date(peak_day),
        "site_count": site_count,
        "site_count_display": _format_int(site_count),
        "date_range_display": _format_range(min_date, max_date),
        "mode_totals": mode_totals,
        "top_locations": top_locations,
        "leading_location": leading_location,
        "chart": {
            "labels": chart_labels,
            "values": chart_values,
            "total": int(round(total)),
        },
    }
    return summary


VALID_USERS = {"admin": "admin", "user1": "mypassword"}
PROTECTED_PREFIXES = ("/", "/eco/", "/trail/", "/vivacity/", "/live/", "/wisdot/", "/se-wi-trails/")


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
  </style>
</head>
<body>
  <!-- Policy gate modal -->
  <div id="policy-modal" class="notice-backdrop" role="dialog" aria-modal="true" aria-labelledby="policy-title" aria-describedby="policy-copy">
    <div class="notice-card">
      <h2 id="policy-title">Data Use &amp; Liability Notice</h2>
      <div id="policy-copy">
        <p>By proceeding, you confirm that you are an authorized AccSafety partner and that you will use this portal solely for official program analysis. All insights and downloadable data are confidential and may contain sensitive roadway safety information.</p>
        <p>You acknowledge that AccSafety and its data providers are not liable for decisions made using this information and that you will comply with all applicable privacy and data handling obligations.</p>
      </div>
      <div class="notice-actions">
        <button type="button" class="primary" id="policy-accept">I Understand &amp; Agree</button>
      </div>
    </div>
  </div>

  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Secure Portal Access</span>
      </div>
      <nav class="app-nav">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>

    <main class="app-content">
      <div class="app-main-centered">
        <form class="app-card app-card--narrow login-card" method="post" autocomplete="off">
          <h1>Welcome back</h1>
          <p>Enter your credentials to continue to the unified dashboards.</p>

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
        eco_summary = get_eco_dashboard_summary()
        eco_chart_json = json.dumps(eco_summary.get("chart", {}))
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>AccSafety Portal</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <link rel="stylesheet" href="https://js.arcgis.com/4.33/esri/themes/light/main.css">
  <script type="module" src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.esm.js"></script>
  <script nomodule src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.js"></script>
  <style>
    .cta-explore {
      display:inline-flex;align-items:center;gap:10px;
      padding:10px 16px;border-radius:999px;
      background:linear-gradient(130deg,var(--brand-primary),var(--brand-secondary));
      color:#fff!important;font-weight:700;text-decoration:none;
      box-shadow:0 12px 26px rgba(11,102,195,0.28);
      position:relative;z-index:2;
    }
    .cta-secondary {
      display:inline-flex;align-items:center;gap:8px;
      padding:10px 16px;border-radius:999px;
      border:1px solid rgba(11,102,195,0.35);
      background:white;color:var(--brand-primary);font-weight:600;text-decoration:none;
      box-shadow:0 10px 24px rgba(15,23,42,0.08);
    }
    .cta-secondary:hover,
    .cta-secondary:focus {
      background:rgba(14,165,233,0.12);
      text-decoration:none;
    }
    .cta-wrap {margin:8px 0 16px;position:relative;z-index:2;display:flex;align-items:center;gap:10px;}
    .desc {color:#0b1736;margin:10px 0 20px;line-height:1.55;font-size:1rem;max-width:820px;}

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

    /* Modal */
    .modal-backdrop {position:fixed;inset:0;background:rgba(0,0,0,0.6);display:flex;align-items:center;justify-content:center;z-index:2000;}
    .modal {background:white;border-radius:14px;max-width:600px;padding:24px 30px;box-shadow:0 24px 60px rgba(0,0,0,0.25);}
    .modal h2 {margin-top:0;}
    .modal button {margin-top:18px;padding:10px 20px;border:none;border-radius:999px;background:linear-gradient(130deg,var(--brand-primary),var(--brand-secondary));color:white;font-weight:600;cursor:pointer;}
    .modal .secondary {background:#e5e7eb;color:#111827;}
    .modal-backdrop[hidden]{display:none;}

    .eco-card {margin-top:24px;display:grid;gap:24px;}
    .eco-card h2 {margin:0;font-size:1.35rem;color:#0b1736;}
    .eco-subtitle {margin:4px 0 0;color:#475569;font-size:0.95rem;max-width:640px;}
    .eco-meta {margin:0;color:#64748b;font-size:0.85rem;}
    .eco-card-header {display:flex;flex-wrap:wrap;gap:16px;justify-content:space-between;align-items:flex-start;}
    .eco-pill {padding:6px 14px;border-radius:999px;background:rgba(14,165,233,0.14);color:#0f172a;font-size:0.8rem;font-weight:600;}
    .eco-stat-tiles {display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));}
    .eco-tile {padding:18px;border-radius:18px;border:1px solid rgba(15,23,42,0.12);background:linear-gradient(135deg,rgba(14,165,233,0.08),rgba(14,165,233,0.02));display:grid;gap:6px;}
    .eco-tile-label {font-size:0.78rem;font-weight:600;letter-spacing:0.08em;text-transform:uppercase;color:#0ea5e9;}
    .eco-tile-value {font-size:1.9rem;font-weight:700;color:#0b1736;font-variant-numeric:tabular-nums;}
    .eco-tile-footnote {font-size:0.85rem;color:#475569;}
    .eco-mode-summary {display:flex;flex-wrap:wrap;gap:12px;}
    .eco-mode-chip {padding:12px 16px;border-radius:12px;background:rgba(15,23,42,0.04);border:1px solid rgba(15,23,42,0.08);min-width:150px;display:grid;gap:4px;}
    .eco-mode-name {font-size:0.75rem;text-transform:uppercase;color:#0f172a;letter-spacing:0.08em;opacity:0.7;}
    .eco-mode-value {font-size:1.15rem;font-weight:600;color:#0f172a;font-variant-numeric:tabular-nums;}
    .eco-mode-share {font-size:0.85rem;color:#334155;}
    .eco-insights-grid {display:grid;gap:24px;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));}
    .eco-panel {padding:20px;border-radius:18px;border:1px solid rgba(15,23,42,0.12);background:#fff;box-shadow:0 16px 34px rgba(15,23,42,0.08);display:grid;gap:12px;}
    .eco-panel h3 {margin:0;font-size:1rem;color:#0b1736;}
    .eco-table {width:100%;border-collapse:collapse;}
    .eco-table tbody tr + tr {border-top:1px solid rgba(148,163,184,0.2);}
    .eco-table td {padding:8px 0;font-size:0.9rem;color:#0f172a;vertical-align:top;}
    .eco-count {text-align:right;font-weight:600;font-variant-numeric:tabular-nums;}
    .eco-rank {width:2.4rem;font-weight:600;color:#64748b;font-variant-numeric:tabular-nums;}
    .eco-location {font-weight:600;color:#0b1736;}
    .eco-share {font-size:0.78rem;color:#64748b;margin-top:2px;}
    .eco-empty {margin:0;font-size:0.9rem;color:#64748b;}
    .eco-footnote {font-size:0.78rem;color:#64748b;}
    @media (max-width:768px){.eco-insights-grid{grid-template-columns:1fr;}.eco-card{gap:20px;}}
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Unified Mobility Analytics Portal</span>
      </div>
      <nav class="app-nav portal-nav" aria-label="Main navigation">
        <a class="app-link" href="/guide">User Guide</a>
        <a class="app-link" href="https://uwm.edu/ipit/wi-pedbike-dashboard/" target="_blank" rel="noopener noreferrer">Program Home</a>
      </nav>
      <div class="app-user">Signed in as <strong>{{ user }}</strong> · <a href="/logout">Log out</a></div>
    </header>

    <main class="app-content">
      <section class="app-card">
        <h1>Explore Wisconsin Pedestrian and Bicyclist Mobility Data</h1>
        <p class="desc">
          The AccSafety Dashboard visualizes statewide pedestrian and bicycle count data from real-time and historical sources to support data-driven safety and planning decisions.
          It bridges research and implementation by integrating current and legacy datasets across Wisconsin.
        </p>

        <div class="cta-wrap">
          <a class="cta-explore" href="/explore/">Explore Available Datasets</a>
          <a class="cta-secondary" href="/guide">Read the User Guide</a>

          <!-- Tooltip + info icon -->
          <span class="tooltip">
            <button id="info-button" class="info-button" aria-label="Show instructions" title="Show instructions">i</button>
            <span class="tooltip-panel" role="tooltip">Click for quick instructions</span>
          </span>
        </div>

        <arcgis-embedded-map
          class="portal-map"
          item-id="317bd3ebf0874aa9b1b4ac55fdd5a095"
          theme="light"
          portal-url="https://uwm.maps.arcgis.com"
          center="-88.01501274592921,43.039734737956515"
          scale="1155581.108577"
          legend-enabled
          information-enabled
          bookmarks-enabled
          layer-list-enabled
          search-enabled>
        </arcgis-embedded-map>
      </section>

      <section class="app-card eco-card" aria-labelledby="eco-card-title">
        <div class="eco-card-header">
          <div>
            <h2 id="eco-card-title">Eco-Counter Summary Dashboard</h2>
            <p class="eco-subtitle">Automatic pedestrian and bicyclist counts captured through statewide Eco-Counter deployments.</p>
          </div>
          {% if eco_summary.date_range_display and eco_summary.date_range_display != "—" %}
          <div class="eco-pill">{{ eco_summary.date_range_display }}</div>
          {% endif %}
        </div>
        <p class="eco-meta">Covering {{ eco_summary.site_count_display }} Eco-Counter locations across {{ eco_summary.total_days_display }} measurement days.</p>

        <div class="eco-stat-tiles">
          <article class="eco-tile" aria-label="Total recorded volume">
            <span class="eco-tile-label">Total Recorded Volume</span>
            <span class="eco-tile-value">{{ eco_summary.total_count_display }}</span>
            <span class="eco-tile-footnote">Combined pedestrian &amp; bicyclist counts</span>
          </article>
          <article class="eco-tile" aria-label="Peak day volume">
            <span class="eco-tile-label">Peak Day Volume</span>
            <span class="eco-tile-value">{{ eco_summary.peak_total_display }}</span>
            <span class="eco-tile-footnote">{{ eco_summary.peak_day_display }}</span>
          </article>
          <article class="eco-tile" aria-label="Average daily volume">
            <span class="eco-tile-label">Average Daily Count</span>
            <span class="eco-tile-value">{{ eco_summary.average_daily_display }}</span>
            <span class="eco-tile-footnote">Across {{ eco_summary.total_days_display }} measurement days</span>
          </article>
        </div>

        {% if eco_summary.mode_totals %}
        <div class="eco-mode-summary" role="list" aria-label="Counts by mode">
          {% for item in eco_summary.mode_totals %}
          <div class="eco-mode-chip" role="listitem">
            <span class="eco-mode-name">{{ item.mode }}</span>
            <span class="eco-mode-value">{{ item.display }}</span>
            <span class="eco-mode-share">{{ item.share_display }} of volume</span>
          </div>
          {% endfor %}
        </div>
        {% endif %}

        <div class="eco-insights-grid">
          <div class="eco-panel">
            <h3>Distribution by Location</h3>
            <canvas id="eco-distribution-chart" height="240" role="img" aria-label="Eco-Counter distribution by location"></canvas>
          </div>
          <div class="eco-panel">
            <h3>Top Locations by Volume</h3>
            {% if eco_summary.top_locations %}
            <table class="eco-table">
              <tbody>
              {% for loc in eco_summary.top_locations %}
                <tr>
                  <td class="eco-rank">{{ loc.rank }}</td>
                  <td>
                    <div class="eco-location">{{ loc.location }}</div>
                    <div class="eco-share">{{ loc.share_display }} of total</div>
                  </td>
                  <td class="eco-count">{{ loc.count_display }}</td>
                </tr>
              {% endfor %}
              </tbody>
            </table>
            {% else %}
            <p class="eco-empty">No Eco-Counter summaries available.</p>
            {% endif %}
          </div>
        </div>

        {% if eco_summary.leading_location %}
        <p class="eco-footnote">Highest volume site: {{ eco_summary.leading_location.name }} ({{ eco_summary.leading_location.count_display }} combined trips).</p>
        {% endif %}
        <p class="eco-footnote">Counts aggregated from Wisconsin Pedestrian and Bicyclist Database (March 2024 export).</p>
      </section>
    </main>
  </div>

  <!-- Getting Started Modal -->
  <div class="modal-backdrop" id="instructions-modal" hidden role="dialog" aria-modal="true" aria-labelledby="intro-title">
    <div class="modal">
      <h2 id="intro-title">Getting Started</h2>
      <p>Use the <strong>Explore Available Datasets</strong> button to open the unified data explorer.</p>
      <p>Hover charts and map layers for details; use top filters to refine by <em>Mode</em>, <em>Facility</em>, and <em>Data source</em>. Look for “Open” links near sites to jump to analytics or related project pages.</p>
      <div style="display:flex;gap:10px;justify-content:flex-end;">
        <button id="close-modal" class="primary">Got it</button>
        <button id="close-once" class="secondary">Dismiss (don’t remember)</button>
      </div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
  <script>
    (function(){
      const chartConfig = {{ eco_chart_json|safe }};
      if (!chartConfig || !Array.isArray(chartConfig.labels) || !Array.isArray(chartConfig.values) || !chartConfig.labels.length) {
        return;
      }
      const canvas = document.getElementById('eco-distribution-chart');
      if (!canvas) { return; }
      const total = chartConfig.total || chartConfig.values.reduce((sum, val) => sum + (Number(val) || 0), 0);
      const palette = ['#0f172a', '#1d4ed8', '#0ea5e9', '#22d3ee', '#14b8a6', '#f97316', '#6366f1', '#ec4899', '#a855f7'];
      const colors = chartConfig.labels.map((_, idx) => palette[idx % palette.length]);
      new Chart(canvas, {
        type: 'doughnut',
        data: {
          labels: chartConfig.labels,
          datasets: [{
            data: chartConfig.values,
            backgroundColor: colors,
            borderWidth: 0,
            hoverOffset: 8,
          }]
        },
        options: {
          cutout: '55%',
          plugins: {
            legend: {
              position: 'right',
              labels: { boxWidth: 14, color: '#0f172a' }
            },
            tooltip: {
              callbacks: {
                label: function(ctx) {
                  const value = Number(ctx.parsed) || 0;
                  const pct = total ? (value / total * 100) : 0;
                  return `${ctx.label}: ${value.toLocaleString()} (${pct.toFixed(1)}%)`;
                }
              }
            }
          }
        }
      });
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
        """, user=session.get("user", "user"), eco_summary=eco_summary, eco_chart_json=eco_chart_json)

    # Convenience redirects
    for p in ["trail","eco","vivacity","live","wisdot","se-wi-trails"]:
        server.add_url_rule(f"/{p}", f"{p}_no_slash", lambda p=p: redirect(f"/{p}/", code=302))

    @server.route("/guide")
    def user_guide():
        return render_template("user_guide.html", user=session.get("user", "user"))

    return server


if __name__ == "__main__":
    app = create_server()
    app.run(host="127.0.0.1", port=5000, debug=False)
