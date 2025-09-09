# wisdot_files_app.py
import os
import re
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


def create_wisdot_files_app(server: Flask, prefix: str = "/wisdot/") -> None:
    """Register WisDOT file listing/download routes on a Flask server."""
    bp = Blueprint("wisdot_files", __name__)

    # Folder where your WisDOT files live
    BASE_DIR = r"C:\\D-Drive\\IPIT_Research_Assistant\\Counts"

    # (location name, date) -> filename mapping for trail counts
    # Example: {("Capital City Trail at 4th St", "2023-08-15"): "CapCityTrail_4thSt_20230815.xlsm"}
    TRAIL_FILES = {}

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

        # Build intersection rows for remaining files
        intersection_rows = []
        for f in files:
            if f in trail_filenames:
                continue
            loc, date_disp = extract_location_date(os.path.join(BASE_DIR, f))
            if not loc and not date_disp:
                loc, date_disp = parse_loc_date(f)
            intersection_rows.append({
                "location": loc or "(unknown)",
                "date": date_disp or "",
                "href": "download/" + quote(f),
            })
        intersection_rows.sort(key=lambda r: (r["location"].lower(), r["date"]))

        html = """
        <h3>WisDOT Historical Files (.xlsm)</h3>
        {% if trail_rows %}
        <h4>Trail Counts</h4>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; font-family: sans-serif;">
          <thead style="background:#f5f5f5;">
            <tr>
              <th align="left">Location</th>
              <th align="left">Date</th>
              <th align="left">Download</th>
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
        {% endif %}
        {% if intersection_rows %}
        <h4>Intersection Counts</h4>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; font-family: sans-serif;">
          <thead style="background:#f5f5f5;">
            <tr>
              <th align="left">Location</th>
              <th align="left">Date</th>
              <th align="left">Download</th>
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
        {% endif %}
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
    app.run(host="127.0.0.1", port=5001, debug=True)

