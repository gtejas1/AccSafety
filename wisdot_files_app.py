# wisdot_files_app.py
import os
import re
import json
from datetime import datetime
from urllib.parse import quote

import pandas as pd
from flask import (
    Flask,
    Blueprint,
    render_template_string,
    send_from_directory,
    abort,
)

DATE_PATTERNS = [
    (re.compile(r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})$"), "%Y-%m-%d"),      # yyyymmdd
    (re.compile(r"(?P<y>\d{4})(?P<m>\d{2})$"), "%Y-%m"),                      # yyyymm
    (re.compile(r"(?P<y>\d{4})[-_](?P<m>\d{2})[-_](?P<d>\d{2})$"), "%Y-%m-%d"),  # yyyy-mm-dd
    (re.compile(r"(?P<y>\d{4})[-_](?P<m>\d{2})$"), "%Y-%m"),                  # yyyy-mm
]


def extract_location_date(filepath: str) -> tuple[str, str]:
    """Pull location name and first date value from a WisDOT .xlsm file."""
    try:
        xlsm = pd.ExcelFile(filepath)
        base_info = xlsm.parse("Base Information", header=None)
        loc = " ".join(
            str(base_info.iloc[6, c])
            for c in range(1, 8)
            if c < base_info.shape[1] and pd.notna(base_info.iloc[6, c])
        ).strip()
        date_val = next(
            (
                base_info.iloc[2, c]
                for c in range(13, 17)
                if c < base_info.shape[1] and pd.notna(base_info.iloc[2, c])
            ),
            None,
        )
        date_display = ""
        if isinstance(date_val, pd.Timestamp):
            date_display = date_val.strftime("%Y-%m-%d")
        elif date_val is not None:
            try:
                parsed = pd.to_datetime(date_val)
                date_display = parsed.strftime("%Y-%m-%d")
            except Exception:
                pass
        return loc, date_display
    except Exception:
        return "", ""


def parse_loc_date(filename: str):
    stem, _ = os.path.splitext(filename)
    if "_" in stem:
        loc_part, date_part = stem.rsplit("_", 1)
    elif "-" in stem:
        loc_part, date_part = stem.rsplit("-", 1)
    else:
        return stem, ""
    location = loc_part.replace("_", " ").strip()
    date_display = ""
    for pat, _out_fmt in DATE_PATTERNS:
        m = pat.match(date_part)
        if m:
            y = int(m.group("y")); mth = int(m.group("m"))
            d = int(m.group("d")) if "d" in m.groupdict() else 1
            try:
                dt = datetime(y, mth, d)
                date_display = dt.strftime("%Y-%m-%d" if "d" in m.groupdict() else "%Y-%m")
            except ValueError:
                date_display = ""
            break
    return location, date_display


def coalesce_metadata(filename: str, location: str | None, date_display: str | None) -> tuple[str, str]:
    """Ensure each file has at least a best-effort location/date."""

    location = (location or "").strip()
    date_display = (date_display or "").strip()

    if location and date_display:
        return location, date_display

    fallback_loc, fallback_date = parse_loc_date(filename)
    if not location:
        location = fallback_loc or "(unknown)"
    if not date_display:
        date_display = fallback_date

    return location or "(unknown)", date_display


def create_wisdot_files_app(server: Flask, prefix: str = "/wisdot/") -> None:
    """Register WisDOT file listing/download routes on a Flask server."""
    bp = Blueprint("wisdot_files", __name__)

    # Folder where your WisDOT files live
    BASE_DIR = r"/srv/AccSafety/WisDot_RawData"
    # Disk cache path for filename/location/date metadata
    CACHE_PATH = os.path.join(os.path.dirname(__file__), "wisdot_cache.json")

    # (location name, date) -> filename mapping for trail counts
    # Example: {("Capital City Trail at 4th St", "2023-08-15"): "CapCityTrail_4thSt_20230815.xlsm"}
    TRAIL_FILES = {}

    # Cache to avoid repeatedly opening Excel files
    # filename -> (location, date_display)
    INTERSECTION_META_CACHE = {}

    @bp.route("/")
    def index():
        try:
            files = [
                f
                for f in os.listdir(BASE_DIR)
                if os.path.isfile(os.path.join(BASE_DIR, f)) and f.lower().endswith(".xlsm")
            ]
        except Exception as e:
            return f"Error reading folder: {e}"

        # Prepare trail rows from mapping
        trail_rows = []
        for (loc, dt), fname in TRAIL_FILES.items():
            if fname in files:
                trail_rows.append({"location": loc, "date": dt, "href": "download/" + quote(fname)})
        trail_rows.sort(key=lambda r: (r["location"].lower(), r["date"]))

        trail_filenames = {fname for fname in TRAIL_FILES.values()}

        # Load disk cache if present
        try:
            with open(CACHE_PATH, "r", encoding="utf-8") as fh:
                disk_cache = json.load(fh)
        except Exception:
            disk_cache = {}
        disk_modified = False

        # Current file stats for validation
        file_stats = {}
        for f in files:
            try:
                st = os.stat(os.path.join(BASE_DIR, f))
                file_stats[f] = {"mtime": st.st_mtime, "size": st.st_size}
            except Exception:
                pass

        # Use disk cache for any files with matching mtime/size; otherwise mark for Excel parse
        need_excel = []
        for f in files:
            if f in trail_filenames:
                continue
            rec = disk_cache.get(f)
            st = file_stats.get(f)
            if isinstance(rec, dict) and st and rec.get("mtime") == st.get("mtime") and rec.get("size") == st.get("size"):
                loc, date_disp = coalesce_metadata(f, rec.get("location"), rec.get("date"))
                INTERSECTION_META_CACHE[f] = (loc, date_disp)
                if (rec.get("location") or "").strip() != loc or (rec.get("date") or "").strip() != date_disp:
                    disk_cache[f] = {
                        "location": loc,
                        "date": date_disp,
                        "mtime": st["mtime"],
                        "size": st["size"],
                    }
                    disk_modified = True
            else:
                need_excel.append(f)

        # Read Excel for those still missing (no filename parsing)
        for f in need_excel:
            loc, date_disp = extract_location_date(os.path.join(BASE_DIR, f))
            loc, date_disp = coalesce_metadata(f, loc, date_disp)
            INTERSECTION_META_CACHE[f] = (loc, date_disp)
            st = file_stats.get(f)
            if st:
                disk_cache[f] = {"location": loc, "date": date_disp, "mtime": st["mtime"], "size": st["size"]}
                disk_modified = True

        # Purge disk cache entries for files no longer present
        existing = set(files)
        prune = [k for k in list(disk_cache.keys()) if k not in existing]
        if prune:
            for k in prune:
                try:
                    del disk_cache[k]
                except KeyError:
                    pass
            disk_modified = True

        if disk_modified:
            try:
                with open(CACHE_PATH, "w", encoding="utf-8") as fh:
                    json.dump(disk_cache, fh)
            except Exception:
                pass

        # Build intersection rows using cache/Excel-derived values only (no filename parsing)
        intersection_rows = []
        for f in files:
            if f in trail_filenames:
                continue
            loc, date_disp = INTERSECTION_META_CACHE.get(f, ("", ""))
            loc, date_disp = coalesce_metadata(f, loc, date_disp)
            intersection_rows.append({
                "location": loc,
                "date": date_disp,
                "href": "download/" + quote(f),
            })
        intersection_rows.sort(key=lambda r: (r["location"].lower(), r["date"]))

        html = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Historical Data</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <img src="/static/img/accsafety-logo.png" alt="AccSafety logo" class="app-logo">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Historical Data</span>
      </div>
      <nav class="app-nav">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>
    <main class="app-content">
      <section class="app-card">
        <h1>Download historical data</h1>
        <p class="app-muted">Click download below to retrieve the spreadsheet.</p>
        {% if not trail_rows and not intersection_rows %}
          <div class="app-alert">No count files were found in the configured directory.</div>
        {% endif %}
        {% if trail_rows %}
          <h2>Trail Counts</h2>
          <div class="table-wrap">
            <table class="app-table">
              <thead>
                <tr>
                  <th>Location</th>
                  <th>Date</th>
                  <th>Download</th>
                </tr>
              </thead>
              <tbody>
                {% for r in trail_rows %}
                <tr>
                  <td>{{ r.location }}</td>
                  <td>{{ r.date }}</td>
                  <td><a href="{{ r.href }}">Download</a></td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        {% endif %}
        {% if intersection_rows %}
          <h2>Intersection Counts</h2>
          <div class="table-wrap">
            <table class="app-table">
              <thead>
                <tr>
                  <th>Location</th>
                  <th>Date</th>
                  <th>Download</th>
                </tr>
              </thead>
              <tbody>
                {% for r in intersection_rows %}
                <tr>
                  <td>{{ r.location }}</td>
                  <td>{{ r.date }}</td>
                  <td><a href="{{ r.href }}">Download</a></td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
        {% endif %}
      </section>
    </main>
  </div>
</body>
</html>
        """
        return render_template_string(html, trail_rows=trail_rows, intersection_rows=intersection_rows)

    @bp.route("/download/<path:filename>")
    def download(filename):
        try:
            if not filename.lower().endswith(".xlsm"):
                abort(403)
            return send_from_directory(BASE_DIR, filename, as_attachment=True)
        except FileNotFoundError:
            abort(404)

    server.register_blueprint(bp, url_prefix=prefix.rstrip("/"))


if __name__ == "__main__":
    app = Flask(__name__)
    create_wisdot_files_app(app, prefix="/")
    app.run(host="127.0.0.1", port=5001, debug=False)

