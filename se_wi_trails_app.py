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
  <link rel="stylesheet" href="/static/theme.css">
  <style>
    table.data-table {
      width: 100%;
    }
    table.data-table thead {
      background: var(--brand-primary);
      color: white;
    }
    table.data-table th,
    table.data-table td {
      padding: 12px 14px;
      text-align: left;
    }
    table.data-table tbody tr:nth-child(even) {
      background: rgba(15, 23, 42, 0.06);
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">SE Wisconsin Trails</span>
      </div>
      <nav class="app-nav">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>
    <main class="app-content">
      <section class="app-card">
        <h1>SE Wisconsin Trails</h1>
        <p class="app-muted">
          This reference table summarises known SE Wisconsin trail counter locations along with descriptive metadata.
          Scroll horizontally to view all available details.
        </p>
        {% if error %}
          <div class="app-alert">{{ error }}</div>
        {% else %}
          <div class="table-wrap">{{ table_html|safe }}</div>
        {% endif %}
      </section>
    </main>
  </div>
</body>
</html>
            """,
            table_html=table_html,
            error=error,
        )

    server.register_blueprint(blueprint)

