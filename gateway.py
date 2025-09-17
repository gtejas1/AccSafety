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
  <style>
    :root {
      --bg1: #0b1736; --bg2: #143e6e; --card: #ffffff; --muted:#6b7280;
      --brand:#0b66c3; --brand2:#22c55e; --danger:#b91c1c;
    }
    * { box-sizing: border-box; }
    html, body { height: 100%; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial;
      background: radial-gradient(1000px 600px at 10% 10%, #1e3a8a 0%, transparent 70%),
                  radial-gradient(800px 500px at 90% 90%, #047857 0%, transparent 70%),
                  linear-gradient(140deg, var(--bg1), var(--bg2));
      color: #0f172a; display: grid; place-items: center; padding: 24px;
    }
    .card {
      width: 100%; max-width: 420px; background: var(--card);
      border-radius: 16px; padding: 28px; box-shadow:
      0 10px 30px rgba(2,6,23,.25), inset 0 1px 0 rgba(255,255,255,.6);
      border: 1px solid rgba(2,6,23,.08);
    }
    .logo {
      display:flex; align-items:center; gap:10px; margin-bottom: 8px;
      font-weight: 700; color: #0b1a37; letter-spacing:.3px;
    }
    .logo-badge {
      width: 36px; height: 36px; border-radius: 10px;
      background: linear-gradient(135deg, var(--brand), var(--brand2));
      display:grid; place-items:center; color:white; font-weight:800;
    }
    h1 { font-size: 20px; margin: 8px 0 16px; }
    p.muted { color: var(--muted); margin: 0 0 18px; font-size: 14px;}
    label { display:block; font-size: 13px; margin: 10px 0 6px; color:#0b1a37; }
    input[type="text"], input[type="password"] {
      width: 100%; padding: 12px 14px; border-radius: 10px; outline: none;
      border: 1px solid #e5e7eb; font-size: 14px; background: #f8fafc;
    }
    .row { display:flex; gap:12px; }
    .btn {
      display:inline-flex; align-items:center; justify-content:center; gap:8px;
      padding: 12px 14px; border-radius: 10px; border: none; cursor: pointer;
      background: linear-gradient(135deg, var(--brand), #0ea5e9); color: white;
      font-weight: 600; width: 100%; margin-top: 14px;
      box-shadow: 0 6px 16px rgba(2,132,199,.35);
    }
    .btn:hover { filter: brightness(1.05); }
    .error { color: var(--danger); font-size: 13px; margin-top: 10px; }
    .foot { margin-top: 16px; font-size: 12px; color: var(--muted); text-align:center; }
    .showpw { font-size: 12px; color:#0b1a37; display:flex; gap:6px; align-items:center; margin-top:8px;}
  </style>
</head>
<body>
  <form class="card" method="post" autocomplete="off">
    <div class="logo">
      <div class="logo-badge">A</div>
      AccSafety
    </div>
    <h1>Welcome back</h1>
    <p class="muted">Sign in to access and visualize data.</p>

    <input type="hidden" name="next" value="{{ nxt }}"/>

    <label for="username">Username</label>
    <input id="username" name="username" type="text" required placeholder="e.g. admin" autofocus>

    <label for="password">Password</label>
    <input id="password" name="password" type="password" required placeholder="••••••••">
    <label class="showpw"><input id="toggle" type="checkbox"> Show password</label>

    <button class="btn" type="submit">Sign in</button>
    {% if error %}<div class="error">{{ error }}</div>{% endif %}
  </form>

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
  <script type="module" src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.esm.js"></script>
  <style>
    :root { --pad: 16px; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin:0; }
    header { padding: var(--pad); border-bottom: 1px solid #eee; display:flex; align-items:center; gap:16px; flex-wrap:wrap; }
    header h2 { margin: 0; font-size: 20px; }
    nav { display:flex; align-items:center; gap: 12px; flex-wrap: wrap; }
    .nav-link {
      display:inline-flex;
      align-items:center;
      gap:6px;
      text-decoration:none;
      color:#0b66c3;
      font-weight:500;
      font-size:14px;
      padding:8px 10px;
      border-radius:6px;
      background:none;
      border:none;
      cursor:pointer;
      font-family: inherit;
      transition: background-color .15s ease;
    }
    .nav-link:hover,
    .nav-link:focus {
      background-color: rgba(11, 102, 195, 0.12);
      text-decoration:none;
      outline:none;
    }
    .dropdown { position: relative; }
    .nav-trigger::after {
      content: "▾";
      font-size: 12px;
      line-height: 1;
    }
    .dropdown-menu {
      display:none;
      position:absolute;
      top: calc(100% + 6px);
      left: 0;
      min-width: 200px;
      background: #ffffff;
      border: 1px solid #e2e8f0;
      box-shadow: 0 12px 30px rgba(15, 23, 42, 0.18);
      border-radius: 8px;
      padding: 8px 0;
      z-index: 20;
    }
    .dropdown:hover .dropdown-menu,
    .dropdown:focus-within .dropdown-menu {
      display: block;
    }
    .dropdown-menu a,
    .dropdown-menu span {
      display:block;
      padding: 8px 16px;
      color: #0f172a;
      text-decoration:none;
      font-size:14px;
      white-space: nowrap;
    }
    .dropdown-menu a:hover,
    .dropdown-menu a:focus {
      background:#f1f5f9;
      outline:none;
    }
    .dropdown-menu .placeholder {
      color:#94a3b8;
      cursor: default;
    }
    .spacer { flex: 1 1 auto; }
    .wrap { padding: var(--pad); }
    arcgis-embedded-map {
      display: block; width: 100%; height: 70vh; border: 1px solid #ddd; border-radius: 8px;
    }
    .user { color:#334155; font-size:14px; }
    .logout { color:#b91c1c; text-decoration:none; margin-left:8px; }
  </style>
</head>
<body>
  <header>
    <h2>AccSafety Portal</h2>
    <nav>
      <a class="nav-link" href="https://uwm.edu/ipit/wi-pedbike-dashboard/" target="_blank" rel="noopener noreferrer">Home</a>
      <div class="dropdown">
        <button class="nav-link nav-trigger" type="button" aria-haspopup="true">Short Term Counts</button>
        <div class="dropdown-menu">
          <a href="/eco/">Short Term Locations(Pilot Counts)</a>
          <a href="/trail/">WisDOT Trails</a>
          {% if wisdot_link %}<a href="{{ wisdot_link }}">WisDOT Intersections</a>{% endif %}
        </div>
      </div>
      <div class="dropdown">
        <button class="nav-link nav-trigger" type="button" aria-haspopup="true">Long Term Counts</button>
        <div class="dropdown-menu">
          <a href="/vivacity/">Vivacity Locations</a>
          <a href="/live/">Live Object Detection</a>
        </div>
      </div>
      <a class="nav-link" href="/se-wi-trails/">SE Wisconsin Trails</a>
    </nav>
    <div class="spacer"></div>
    <div class="user">Signed in as <strong>{{ user }}</strong> · <a class="logout" href="/logout">Log out</a></div>
  </header>

  <div class="wrap">
    <arcgis-embedded-map
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
  </div>
</body>
</html>
        """, wisdot_link=wisdot_link, user=session.get("user","user"))


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
