# wisdot_files_app.py
import os
import re
from datetime import datetime
from urllib.parse import quote
from flask import Flask, render_template_string, send_from_directory, abort

app = Flask(__name__)

# Folder where your WisDOT files live
BASE_DIR = r"C:\D-Drive\IPIT_Research_Assistant\Counts"

DATE_PATTERNS = [
    (re.compile(r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})$"), "%Y-%m-%d"),      # yyyymmdd
    (re.compile(r"(?P<y>\d{4})(?P<m>\d{2})$"), "%Y-%m"),                      # yyyymm
    (re.compile(r"(?P<y>\d{4})[-_](?P<m>\d{2})[-_](?P<d>\d{2})$"), "%Y-%m-%d"),  # yyyy-mm-dd
    (re.compile(r"(?P<y>\d{4})[-_](?P<m>\d{2})$"), "%Y-%m"),                  # yyyy-mm
]

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

@app.route("/")
def index():
    try:
        files = [
            f for f in os.listdir(BASE_DIR)
            if os.path.isfile(os.path.join(BASE_DIR, f)) and f.lower().endswith(".xlsm")
        ]
    except Exception as e:
        return f"Error reading folder: {e}"

    rows = []
    for f in files:
        loc, date_disp = parse_loc_date(f)
        rows.append({
            "location": loc or "(unknown)",
            "date": date_disp or "",
            "href": "/download/" + quote(f),
        })
    rows.sort(key=lambda r: (r["location"].lower(), r["date"]))

    html = """
    <h3>WisDOT Historical Files (.xlsm)</h3>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse; font-family: sans-serif;">
      <thead style="background:#f5f5f5;">
        <tr>
          <th align="left">Location</th>
          <th align="left">Date</th>
          <th align="left">Download</th>
        </tr>
      </thead>
      <tbody>
      {% for r in rows %}
        <tr>
          <td>{{ r.location }}</td>
          <td>{{ r.date }}</td>
          <td><a href="{{ r.href }}">Download</a></td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    """
    return render_template_string(html, rows=rows)

@app.route("/download/<path:filename>")
def download(filename):
    try:
        if not filename.lower().endswith(".xlsm"):
            abort(403)
        return send_from_directory(BASE_DIR, filename, as_attachment=True)
    except FileNotFoundError:
        abort(404)

if __name__ == "__main__":
    # Run this separately, e.g. on port 5001
    app.run(host="127.0.0.1", port=5001, debug=True)
