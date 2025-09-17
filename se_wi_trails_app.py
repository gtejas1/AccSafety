# se_wi_trails_app.py
"""Flask blueprint that serves the SE Wisconsin Trails table view."""

from __future__ import annotations

import os
from typing import Optional

import pandas as pd
from flask import Blueprint, render_template_string


def _load_trails_table(data_path: str) -> tuple[Optional[str], Optional[str]]:
    """Load the trails spreadsheet and return an HTML table or an error."""

    if not os.path.exists(data_path):
        return None, "Data file not found. Add assets/se_wi_trails.xlsx to continue."

    try:
        df = pd.read_excel(data_path)
    except Exception as exc:  # pragma: no cover - defensive guard
        return None, f"Unable to load data: {exc}"

    table_html = df.fillna("").to_html(
        classes="data-table", index=False, border=0, justify="center"
    )
    return table_html, None


def create_se_wi_trails_app(server, prefix: str = "/se-wi-trails/") -> None:
    """Register the SE Wisconsin Trails route on the provided Flask server."""

    normalized_prefix = prefix.rstrip("/") or "/se-wi-trails"
    blueprint = Blueprint("se_wi_trails", __name__, url_prefix=normalized_prefix)

    data_path = os.path.join(os.path.dirname(__file__), "assets", "se_wi_trails.xlsx")

    @blueprint.route("/")
    def se_wi_trails():
        table_html, error = _load_trails_table(data_path)
        return render_template_string(
            """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>SE Wisconsin Trails</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial;
      margin: 0;
      background-color: #f8fafc;
      color: #0f172a;
    }
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 18px 24px;
      background: linear-gradient(135deg, #0b66c3, #0ea5e9);
      color: white;
    }
    header h1 {
      margin: 0;
      font-size: 22px;
      letter-spacing: .3px;
    }
    header a {
      color: white;
      text-decoration: none;
      font-weight: 500;
    }
    main {
      padding: 24px;
      max-width: 1200px;
      margin: 0 auto;
    }
    .intro {
      margin-bottom: 20px;
      line-height: 1.5;
    }
    .table-wrap {
      overflow-x: auto;
      background: white;
      border-radius: 10px;
      box-shadow: 0 18px 38px rgba(15, 23, 42, 0.12);
      border: 1px solid rgba(148, 163, 184, 0.35);
    }
    table.data-table {
      border-collapse: collapse;
      width: 100%;
    }
    table.data-table thead {
      background: #0b66c3;
      color: white;
    }
    table.data-table th,
    table.data-table td {
      padding: 10px 12px;
      text-align: left;
      font-size: 14px;
    }
    table.data-table tbody tr:nth-child(even) {
      background: #f1f5f9;
    }
    .error {
      padding: 16px;
      background: #fee2e2;
      color: #b91c1c;
      border-radius: 8px;
      border: 1px solid #fecaca;
    }
  </style>
</head>
<body>
  <header>
    <h1>SE Wisconsin Trails</h1>
    <a href="/">← Back to Portal</a>
  </header>
  <main>
    <p class="intro">
      This table lists SE Wisconsin trail counter locations, their descriptive details,
      and supporting metadata as provided in the regional spreadsheet.
      Use the horizontal scrollbar to view additional columns.
    </p>
    {% if error %}
      <div class="error">{{ error }}</div>
    {% else %}
      <div class="table-wrap">{{ table_html|safe }}</div>
    {% endif %}
  </main>
</body>
</html>
            """,
            table_html=table_html,
            error=error,
        )

    server.register_blueprint(blueprint)

