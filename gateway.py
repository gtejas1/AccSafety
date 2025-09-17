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
  </style>
</head>
<body>
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

          <button type="submit">Sign in</button>
          {% if error %}<div class="error">{{ error }}</div>{% endif %}
        </form>
      </div>
    </main>
  </div>

  <script>
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
        return redirect("/login", code=302)

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
        <h1>Explore Wisconsin Mobility Data</h1>
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
          heading-enabled
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
    app.run(host="127.0.0.1", port=5000, debug=True)
