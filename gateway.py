# gateway.py
import os
from urllib.parse import quote
from flask import Flask, render_template_string, redirect, request, session

from pbc_trail_app import create_trail_dash
from pbc_eco_app import create_eco_dash
from vivacity_app import create_vivacity_dash
from wisdot_files_app import create_wisdot_files_app
from live_detection_app import create_live_detection_app
from se_wi_trails_app import create_se_wi_trails_app

VALID_USERS = {  # change as needed, or load from env/DB
    "admin": "admin",
    "user1": "mypassword",
}

PROTECTED_PREFIXES = ("/", "/eco/", "/trail/", "/vivacity/", "/live/", "/wisdot/", "/se-wi-trails/")  # guard home + all apps


def create_server():
    server = Flask(__name__)
    server.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev_secret_key")

    # ---- Global auth guard (protects home and app routes) -------------------
    @server.before_request
    def require_login():
        # allow login, logout, static, favicon
        path = request.path or "/"
        if path.startswith("/static/") or path == "/login" or path == "/logout" or path == "/favicon.ico":
            return None
        # gate protected prefixes
        if path.startswith(PROTECTED_PREFIXES) and "user" not in session:
            full = request.full_path  # includes "?..." and trailing "?"
            next_target = full[:-1] if full.endswith("?") else full
            return redirect(f"/login?next={quote(next_target)}", code=302)
        return None

    # ---- Login page ---------------------------------------------------------
    @server.route("/login", methods=["GET", "POST"])
    def login():
        error = None
        if request.method == "POST":
            u = (request.form.get("username") or "").strip()
            p = request.form.get("password") or ""
            if u in VALID_USERS and VALID_USERS[u] == p:
                session["user"] = u
                # Safety: only allow same-site redirects
                nxt = request.args.get("next") or "/"
                if not nxt.startswith("/"):
                    nxt = "/"
                return redirect(nxt, code=302)
            error = "Invalid username or password."

        # GET (or failed POST) → show page
        nxt = request.args.get("next", "/")
        return render_template_string("""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Sign in · AccSafety</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <style>
    .login-card h1 {
      margin: 0 0 12px;
      font-size: 1.4rem;
    }
    .login-card p {
      margin: 0 0 20px;
      color: var(--brand-muted);
    }
    .login-card label {
      display: block;
      margin: 12px 0 6px;
      font-weight: 600;
      font-size: 0.9rem;
      color: #0b1736;
    }
    .login-card input[type="text"],
    .login-card input[type="password"] {
      width: 100%;
      padding: 12px 14px;
      border-radius: 10px;
      border: 1px solid rgba(15, 23, 42, 0.16);
      background: #f8fafc;
      font-size: 0.95rem;
    }
    .login-card button {
      width: 100%;
      margin-top: 20px;
      padding: 12px 16px;
      border: none;
      border-radius: 999px;
      background: linear-gradient(130deg, var(--brand-primary), var(--brand-secondary));
      color: white;
      font-weight: 600;
      cursor: pointer;
      font-size: 1rem;
      box-shadow: 0 14px 30px rgba(11, 102, 195, 0.28);
    }
    .login-card button:hover {
      filter: brightness(1.05);
    }
    .login-card button:disabled {
      filter: grayscale(0.4);
      cursor: not-allowed;
      box-shadow: none;
      opacity: 0.7;
    }
    .login-card .showpw {
      margin-top: 10px;
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.85rem;
      color: #0b1736;
    }
    .login-card .error {
      margin-top: 12px;
      color: #b91c1c;
      font-weight: 600;
      font-size: 0.9rem;
    }
    .notice-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(12, 23, 42, 0.72);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
      z-index: 999;
    }
    .notice-card {
      max-width: 540px;
      width: 100%;
      background: #ffffff;
      border-radius: 18px;
      box-shadow: 0 24px 60px rgba(11, 23, 54, 0.32);
      padding: 28px 32px;
      color: #0b1736;
      display: grid;
      gap: 18px;
    }
    .notice-card h2 {
      margin: 0;
      font-size: 1.35rem;
    }
    .notice-card p {
      margin: 0;
      line-height: 1.55;
    }
    .notice-actions {
      display: flex;
      gap: 12px;
      justify-content: flex-end;
      flex-wrap: wrap;
    }
    .notice-actions button {
      border-radius: 999px;
      border: none;
      padding: 10px 18px;
      font-weight: 600;
      cursor: pointer;
      font-size: 0.95rem;
    }
    .notice-actions .primary {
      background: linear-gradient(130deg, var(--brand-primary), var(--brand-secondary));
      color: #fff;
      box-shadow: 0 12px 26px rgba(11, 102, 195, 0.28);
    }
    .notice-actions .secondary {
      background: #edf2f7;
      color: #0b1736;
    }
    .notice-backdrop[hidden] {
      display: none;
    }
  </style>
</head>
<body>
  <div id="policy-modal" class="notice-backdrop" role="dialog" aria-modal="true" aria-labelledby="policy-title" aria-describedby="policy-copy">
    <div class="notice-card">
      <h2 id="policy-title">Data Use &amp; Liability Notice</h2>
      <div id="policy-copy">
        <p>By proceeding, you confirm that you are an authorized AccSafety partner and that you will use this portal solely for official program analysis. All insights and downloadable data are confidential and may contain sensitive roadway safety information.</p>
        <p>You acknowledge that AccSafety and its data providers are not liable for decisions made using this information and that you will comply with all applicable privacy and data handling obligations.</p>
      </div>
      <div class="notice-actions">
        <button type="button" class="secondary" id="policy-decline">Decline</button>
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
    const policyModal = document.getElementById('policy-modal');
    const acceptPolicy = document.getElementById('policy-accept');
    const declinePolicy = document.getElementById('policy-decline');
    const submitButton = document.querySelector('.login-card button[type="submit"]');
    const usernameInput = document.getElementById('username');
    const urlParams = new URLSearchParams(window.location.search);

    if (urlParams.get('reset_policy') === '1') {
      window.localStorage.removeItem('accsafetyPolicyAccepted');
    }

    function enableForm() {
      policyModal.hidden = true;
      submitButton.disabled = false;
      usernameInput.focus();
    }

    acceptPolicy.addEventListener('click', function () {
      window.localStorage.setItem('accsafetyPolicyAccepted', 'true');
      enableForm();
    });

    declinePolicy.addEventListener('click', function () {
      window.localStorage.removeItem('accsafetyPolicyAccepted');
      window.location.href = 'https://www.dot.state.wi.us';
    });

    if (window.localStorage.getItem('accsafetyPolicyAccepted') === 'true') {
      enableForm();
    }

    document.getElementById('toggle').addEventListener('change', function(){
      const pw = document.getElementById('password');
      pw.type = this.checked ? 'text' : 'password';
    });
  </script>
</body>
</html>
        """, error=error, nxt=nxt)

    @server.route("/logout")
    def logout():
        session.clear()
        return redirect("/login?reset_policy=1", code=302)

    # ---- Gateway home (with your ArcGIS map) --------------------------------
    create_trail_dash(server, prefix="/trail/")
    create_eco_dash(server, prefix="/eco/")
    create_vivacity_dash(server, prefix="/vivacity/")
    create_live_detection_app(server, prefix="/live/")
    create_wisdot_files_app(server, prefix="/wisdot/")
    create_se_wi_trails_app(server, prefix="/se-wi-trails/")

    @server.route("/")
    def home():
        wisdot_link = "/wisdot/"
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
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Unified Mobility Analytics Portal</span>
      </div>
      <nav class="app-nav portal-nav" aria-label="Main navigation">
        <a class="app-link" href="https://uwm.edu/ipit/wi-pedbike-dashboard/" target="_blank" rel="noopener noreferrer">Program Home</a>
        <div class="portal-dropdown">
          <button class="portal-trigger" type="button" aria-haspopup="true">Short Term Counts</button>
          <div class="portal-menu">
            <a href="/eco/">Short Term Locations (Pilot Counts)</a>
            <a href="/trail/">WisDOT Trails</a>
            {% if wisdot_link %}<a href="{{ wisdot_link }}">WisDOT Intersections</a>{% endif %}
          </div>
        </div>
        <div class="portal-dropdown">
          <button class="portal-trigger" type="button" aria-haspopup="true">Long Term Counts</button>
          <div class="portal-menu">
            <a href="/vivacity/">Vivacity Locations</a>
            <a href="/live/">Live Object Detection</a>
          </div>
        </div>
        <a class="app-link" href="/se-wi-trails/">SE Wisconsin Trails</a>
      </nav>
      <div class="app-user">Signed in as <strong>{{ user }}</strong> · <a href="/logout">Log out</a></div>
    </header>

    <main class="app-content">
      <section class="app-card">
        <h1>Explore Wisconsin Pedestrian and Bicyclist Mobility Data</h1>
        <p class="app-muted">
          Use the navigation above to jump between short term and long term count dashboards,
          download WisDOT files, or explore the regional trails catalog. The interactive map
          below highlights current study areas.
        </p>
        <arcgis-embedded-map
          class="portal-map"
          item-id="a1e765b1cec34b2897d6a8b7c1ffe54b"
          theme="light"
          bookmarks-enabled
          legend-enabled
          information-enabled
          center="-87.87609699999999,43.122054"
          scale="577790.554289"
          portal-url="https://uwm.maps.arcgis.com">
        </arcgis-embedded-map>
      </section>
    </main>
  </div>
</body>
</html>
        """, wisdot_link=wisdot_link, user=session.get("user", "user"))


    # Convenience redirects
    @server.route("/trail")
    def trail_no_slash(): return redirect("/trail/", code=302)
    @server.route("/eco")
    def eco_no_slash(): return redirect("/eco/", code=302)
    @server.route("/vivacity")
    def vivacity_no_slash(): return redirect("/vivacity/", code=302)
    @server.route("/live")
    def live_no_slash(): return redirect("/live/", code=302)
    @server.route("/wisdot")
    def wisdot_no_slash(): return redirect("/wisdot/", code=302)
    @server.route("/se-wi-trails")
    def se_wi_trails_no_slash(): return redirect("/se-wi-trails/", code=302)

    return server


if __name__ == "__main__":
    app = create_server()
    app.run(host="127.0.0.1", port=5000, debug=False)
