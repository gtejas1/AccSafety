# gateway.py
import json
import os
from typing import Dict, List
from pathlib import Path
from urllib.parse import quote
from flask import Flask, render_template, render_template_string, redirect, request, session

from pbc_trail_app import create_trail_dash
from pbc_eco_app import create_eco_dash
from vivacity_app import create_vivacity_dash
from wisdot_files_app import create_wisdot_files_app
from live_detection_app import create_live_detection_app
from se_wi_trails_app import create_se_wi_trails_app
from unified_explore import create_unified_explore


BASE_DIR = Path(__file__).resolve().parent

VALID_USERS = {"admin": "IPIT&uwm2024", "ipit": "IPIT&uwm2024"}
PROTECTED_PREFIXES = ("/", "/eco/", "/trail/", "/vivacity/", "/live/", "/wisdot/", "/se-wi-trails/")

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
      <img src="/static/img/accsafety-logo.png" alt="AccSafety logo" class="app-logo">
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
    .desc {color:#0b1736;margin:10px 0 16px;line-height:1.55;font-size:1rem;max-width:820px;}

    .portal-overview {display:grid;gap:24px;grid-template-columns:repeat(2,minmax(0,1fr));align-items:stretch;}
    .portal-primary {display:grid;gap:18px;align-content:start;justify-items:stretch;}
    .portal-secondary {display:flex;flex-direction:column;align-self:stretch;align-items:stretch;}
    .portal-highlight-row {display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;width:100%;}
    .portal-highlight-card {background:#f8fafc;border:1px solid rgba(148,163,184,0.28);border-radius:14px;padding:14px 16px;box-shadow:0 16px 28px rgba(15,23,42,0.08);display:grid;gap:6px;justify-items:center;text-align:center;min-height:110px;}
    .portal-highlight-card h3 {margin:0;font-size:0.95rem;font-weight:700;color:#0b1736;}
    .portal-highlight-count {margin:0;font-size:1.5rem;font-weight:700;color:var(--brand-primary);}
    .portal-map-card {background:rgba(255,255,255,0.92);border:1px solid rgba(148,163,184,0.26);border-radius:18px;box-shadow:0 16px 28px rgba(15,23,42,0.1);padding:18px 20px;display:flex;flex-direction:column;gap:14px;width:100%;height:100%;max-width:none;flex:1;}
    .portal-map-heading {margin:0;font-size:1.05rem;font-weight:700;color:#0b1736;}
    .portal-map-image {flex:1;width:100%;height:100%;display:block;border-radius:14px;box-shadow:0 12px 24px rgba(15,23,42,0.12);border:1px solid rgba(148,163,184,0.28);object-fit:cover;background:#e2e8f0;min-height:320px;}
    .portal-hero-text {justify-self:start;}
    .portal-quick-card {padding:18px 20px;gap:10px;max-width:520px;width:100%;}
    .portal-quick-links {margin:12px 0 0;display:grid;gap:8px;}
    .portal-quick-links a {text-decoration:none;}
    @media (max-width: 960px) {
      .portal-overview {grid-template-columns:1fr;gap:20px;}
      .portal-secondary {display:grid;align-self:auto;}
      .portal-map-card {max-width:100%;height:auto;}
      .portal-map-image {min-height:0;height:auto;max-height:320px;object-fit:contain;}
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
    .footer-logo {display:block;max-width:260px;width:auto;height:auto;}
    .footer-logo--uwm {max-height:78px;}
    .footer-logo--wisdot {max-height:96px;}
    .footer-copyright {margin:0;color:#475569;font-size:0.95rem;}
    @media (max-width: 720px) {
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
        <span class="app-subtitle">Wisconsin Non-Driver Safety Portal</span>
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
              <h1>Explore Wisconsin's non-motorist data</h1>
              <p class="desc">
                Use research-backed insights, statewide counts, and planning tools curated for practitioners focused on people walking and biking.
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

            <aside class="portal-quick-card" aria-labelledby="quick-access-title">
              <h2 id="quick-access-title">Quick Access</h2>
              <p class="portal-quick-card-section">Long Term Counts:</p>
              <ul class="portal-quick-links" aria-label="Long Term Counts">
                <li><a href="/live/">Monitor Live Detection</a></li>
                <li><a href="/vivacity/">Vivacity Analytics</a></li>
              </ul>
            </aside>

            <div class="portal-highlight-row" role="list">
              <article class="portal-highlight-card" role="listitem">
                <h3>Data Sources Available</h3>
                <p class="portal-highlight-count">7</p>
              </article>
              <article class="portal-highlight-card" role="listitem">
                <h3>Analysis Options</h3>
                <p class="portal-highlight-count">15</p>
              </article>
            </div>
          </div>

          <div class="portal-secondary">
            <div class="portal-map-card">
              <h2 class="portal-map-heading">Statewide non-driver activity &amp; safety view</h2>
              <img
                class="portal-map-image"
                src="/static/img/home-map.png"
                alt="Map of Wisconsin highlighting non-driver activity and safety"
                loading="lazy"
              >
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
      <p class="footer-copyright">Copyright ©2025 All rights reserved.</p>
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
