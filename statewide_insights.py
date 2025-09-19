"""Statewide ArcGIS map view for the AccSafety portal."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from flask import Flask, redirect, render_template_string


ASSETS_DIR = Path(__file__).resolve().parent / "assets"


EXCEL_FILES: Dict[str, str] = {
    "Bicyclists": "WisconsinPedBikeCountDatabase_BicyclistCounts_032824.xlsx",
    "Pedestrians": "WisconsinPedBikeCountDatabase_PedestrianCounts_032824.xlsx",
    "Trail Users": "WisconsinPedBikeCountDatabase_TrailUserCounts_032824.xlsx",
}


ANNUAL_COLS: Dict[str, str] = {
    "Bicyclists": "Estimated Annual Bicyclist Count",
    "Pedestrians": "Estimated Annual Pedestrian Count",
    "Trail Users": "Estimated Annual Trail User Count",
}


BASE_COLUMNS: List[str] = [
    "Location ID",
    "Location Name",
    "City",
    "WisDOT Region",
    "Year",
]


@dataclass(frozen=True)
class StatewideDataset:
    """Normalized statewide insights data prepared for the viewer."""

    records_json: str
    type_options: List[str]
    year_options: List[int]
    city_options: List[str]
    region_options: List[str]


def _load_dataset(label: str, path: Path) -> pd.DataFrame:
    """Load and normalize an Excel workbook for the statewide viewer."""

    df = pd.read_excel(path, sheet_name=0)
    df.columns = df.columns.astype(str)

    missing = [c for c in BASE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: missing required columns {missing} in {path.name}")

    annual_col = ANNUAL_COLS[label]
    if annual_col not in df.columns:
        raise ValueError(f"{label}: missing column '{annual_col}' in {path.name}")

    data = df[BASE_COLUMNS + [annual_col]].copy()
    data.rename(columns={annual_col: "EstimatedAnnual"}, inplace=True)
    data["Type"] = label
    data["EstimatedAnnual"] = pd.to_numeric(data["EstimatedAnnual"], errors="coerce")
    data["Year"] = pd.to_numeric(data["Year"], errors="coerce")
    return data


def _clean_text(value: object) -> Optional[str]:
    """Return a trimmed string representation or ``None`` for null-ish values."""

    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        # Avoid trailing ".0" for whole numbers that were read as floats.
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    if pd.isna(value):  # Handles pandas NA types gracefully.
        return None
    text = str(value).strip()
    return text or None


@lru_cache(maxsize=1)
def load_statewide_dataset() -> StatewideDataset:
    """Load and cache the statewide viewer dataset."""

    frames: List[pd.DataFrame] = []
    errors: Dict[str, str] = {}

    for label, filename in EXCEL_FILES.items():
        workbook_path = ASSETS_DIR / filename
        try:
            frames.append(_load_dataset(label, workbook_path))
        except Exception as exc:  # pragma: no cover - defensive guard
            errors[label] = str(exc)

    if not frames:
        raise RuntimeError(
            "Failed to load any statewide datasets. Errors: " + ", ".join(f"{k}: {v}" for k, v in errors.items())
        )

    data = pd.concat(frames, ignore_index=True)

    records = []
    for row in data.to_dict(orient="records"):
        year_val = int(row["Year"]) if pd.notna(row["Year"]) else None
        est_val = float(row["EstimatedAnnual"]) if pd.notna(row["EstimatedAnnual"]) else None
        city_val = _clean_text(row.get("City"))
        region_val = _clean_text(row.get("WisDOT Region"))
        location_id = _clean_text(row.get("Location ID"))
        location_name = _clean_text(row.get("Location Name")) or "Unknown"
        type_val = _clean_text(row.get("Type"))
        records.append(
            {
                "Type": type_val,
                "Year": year_val,
                "WisDOT Region": region_val,
                "City": city_val,
                "Location ID": location_id,
                "Location Name": location_name,
                "EstimatedAnnual": est_val,
            }
        )

    type_options = sorted({row["Type"] for row in records if row["Type"]}, key=str.casefold)
    year_options = sorted({row["Year"] for row in records if row["Year"] is not None})
    city_options = sorted({row["City"] for row in records if row["City"]}, key=str.casefold)
    region_options = sorted({row["WisDOT Region"] for row in records if row["WisDOT Region"]}, key=str.casefold)

    return StatewideDataset(
        records_json=json.dumps(records, separators=(",", ":")),
        type_options=list(type_options),
        year_options=[int(y) for y in year_options],
        city_options=list(city_options),
        region_options=list(region_options),
    )


def register_statewide_insights(app: Flask) -> None:
    """Attach the statewide insights routes to the provided Flask app."""

    @app.route("/statewide-map")
    def statewide_map():
        dataset = load_statewide_dataset()
        return render_template_string(
            """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Statewide Map &amp; Insights · AccSafety</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="/static/theme.css">
  <link rel="stylesheet" href="https://js.arcgis.com/4.33/esri/themes/light/main.css">
  <script type="module" src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.esm.js"></script>
  <script nomodule src="https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.js"></script>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fontsource/inter@5.0.16/400.css">
  <style>
    :root {
      color-scheme: light;
    }

    .map-frame {
      width: 100%;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 20px 44px rgba(15, 23, 42, 0.15);
    }

    .insights-viewer {
      margin-top: 32px;
      padding: 28px 30px 32px;
      border-radius: 22px;
      background: linear-gradient(145deg, rgba(245,248,255,0.88), rgba(255,255,255,0.92));
      box-shadow: 0 18px 40px rgba(15, 23, 42, 0.12);
    }

    .viewer-header {
      display: flex;
      flex-direction: column;
      gap: 6px;
      margin-bottom: 24px;
    }

    .viewer-title {
      font-size: 1.8rem;
      font-weight: 600;
      color: #0f172a;
      margin: 0;
    }

    .viewer-subtitle {
      font-size: 0.95rem;
      color: #475569;
      margin: 0;
    }

    .viewer-controls {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 16px;
      margin-bottom: 26px;
    }

    .control-block label {
      display: block;
      font-weight: 600;
      font-size: 0.85rem;
      color: #1e293b;
      margin-bottom: 6px;
    }

    .control-block select {
      width: 100%;
      min-height: 38px;
      padding: 8px 10px;
      border-radius: 12px;
      border: 1px solid #cbd5f5;
      background: #fff;
      font-size: 0.95rem;
      color: #0f172a;
      box-shadow: inset 0 1px 2px rgba(15, 23, 42, 0.08);
    }

    .viewer-grid {
      display: grid;
      grid-template-columns: minmax(0, 2fr) minmax(0, 1fr);
      gap: 22px;
      align-items: stretch;
    }

    .summary-box {
      border-radius: 18px;
      border: 1px solid rgba(148, 163, 184, 0.35);
      padding: 20px 22px;
      background: rgba(255, 255, 255, 0.95);
    }

    .summary-box h3 {
      margin: 0 0 12px;
      font-size: 1.05rem;
      color: #1e293b;
    }

    .summary-box ul {
      margin: 0;
      padding-left: 18px;
      color: #334155;
      font-size: 0.95rem;
      line-height: 1.6;
    }

    .records-section {
      margin-top: 34px;
    }

    .records-section h3 {
      font-size: 1.2rem;
      color: #0f172a;
      margin-bottom: 14px;
    }

    table.viewer-table {
      width: 100%;
      border-collapse: collapse;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.08);
      font-size: 0.92rem;
    }

    table.viewer-table thead {
      background: linear-gradient(135deg, rgba(30, 64, 175, 0.12), rgba(30, 64, 175, 0.32));
      color: #0f172a;
    }

    table.viewer-table th,
    table.viewer-table td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(226, 232, 240, 0.8);
      text-align: left;
      white-space: nowrap;
    }

    table.viewer-table tbody tr:nth-child(even) {
      background-color: rgba(241, 245, 249, 0.6);
    }

    .table-footnote {
      margin-top: 10px;
      color: #64748b;
      font-size: 0.78rem;
    }

    @media (max-width: 960px) {
      .viewer-grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <div class="app-shell">
    <header class="app-header">
      <div class="app-header-title">
        <span class="app-brand">AccSafety</span>
        <span class="app-subtitle">Statewide Map &amp; Insights</span>
      </div>
      <nav class="app-nav" aria-label="Main navigation">
        <a class="app-link" href="/">Back to Portal</a>
      </nav>
    </header>

    <main class="app-content">
      <section class="app-card">
        <div class="map-frame">
          <arcgis-embedded-map style="height:600px;width:100%;" item-id="5badd855f3384cb1ab03eb0470a93f20" theme="light" bookmarks-enabled heading-enabled legend-enabled information-enabled center="-88.01456655273279,42.991659663963226" scale="1155581.108577" portal-url="https://uwm.maps.arcgis.com"></arcgis-embedded-map>
        </div>
      </section>

      <section class="insights-viewer" aria-label="Statewide counts explorer">
        <div class="viewer-header">
          <h2 class="viewer-title">Wisconsin Pedestrian, Bicyclist &amp; Trail Activity</h2>
          <p class="viewer-subtitle">Interact with statewide counts sourced from the Wisconsin Ped/Bike Count Database (March 2024 extracts).</p>
        </div>

        <div class="viewer-controls">
          <div class="control-block">
            <label for="type-select">User Type</label>
            <select id="type-select">
            {% for option in dataset.type_options %}
              <option value="{{ option }}" {% if loop.first %}selected{% endif %}>{{ option }}</option>
            {% endfor %}
            </select>
          </div>
          <div class="control-block">
            <label for="year-select">Year</label>
            <select id="year-select" multiple size="6" aria-describedby="year-hint">
            {% for option in dataset.year_options %}
              <option value="{{ option }}">{{ option }}</option>
            {% endfor %}
            </select>
            <small id="year-hint" style="display:block;margin-top:6px;color:#64748b;">Hold Ctrl/⌘ to pick multiple years. Leave empty for all.</small>
          </div>
          <div class="control-block">
            <label for="city-select">City</label>
            <select id="city-select" multiple size="6">
            {% for option in dataset.city_options %}
              <option value="{{ option }}">{{ option }}</option>
            {% endfor %}
            </select>
          </div>
          <div class="control-block">
            <label for="region-select">WisDOT Region</label>
            <select id="region-select" multiple size="6">
            {% for option in dataset.region_options %}
              <option value="{{ option }}">{{ option }}</option>
            {% endfor %}
            </select>
          </div>
        </div>

        <div class="viewer-grid">
          <div id="bar-container" role="img" aria-label="Top statewide locations chart"></div>
          <aside class="summary-box" aria-live="polite">
            <h3>Snapshot</h3>
            <ul id="summary-list"></ul>
          </aside>
        </div>

        <div class="records-section">
          <h3>Filtered Records</h3>
          <div style="overflow:auto;">
            <table class="viewer-table" id="records-table">
              <thead>
                <tr>
                  <th scope="col">Type</th>
                  <th scope="col">Year</th>
                  <th scope="col">WisDOT Region</th>
                  <th scope="col">City</th>
                  <th scope="col">Location ID</th>
                  <th scope="col">Location Name</th>
                  <th scope="col">Estimated Annual Count</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
          <div class="table-footnote">Tip: Scroll horizontally to view long location names. Table updates automatically as filters change.</div>
        </div>
      </section>
    </main>
  </div>

  <script src="https://cdn.plot.ly/plotly-2.26.0.min.js" integrity="sha384-BJy5gJdAwVXyHqUAFzPZTftIQCzq4V3sBtL66JNJ1yvASRWQJGkmG6gJ+FXHXm7f" crossorigin="anonymous"></script>
  <script>
    const DATA = {{ dataset.records_json | safe }};

    const typeSelect = document.getElementById('type-select');
    const yearSelect = document.getElementById('year-select');
    const citySelect = document.getElementById('city-select');
    const regionSelect = document.getElementById('region-select');
    const summaryList = document.getElementById('summary-list');
    const tableBody = document.querySelector('#records-table tbody');

    function getSelectedValues(selectEl) {
      return Array.from(selectEl.selectedOptions).map(opt => opt.value);
    }

    function applyFilters(records) {
      const typeVal = typeSelect.value;
      const years = getSelectedValues(yearSelect).map(Number);
      const cities = new Set(getSelectedValues(citySelect));
      const regions = new Set(getSelectedValues(regionSelect));

      return records.filter(row => {
        if (typeVal && row['Type'] !== typeVal) return false;
        if (years.length && !years.includes(row['Year'])) return false;
        if (cities.size && !cities.has(row['City'])) return false;
        if (regions.size && !regions.has(row['WisDOT Region'])) return false;
        return true;
      });
    }

    function renderSummary(records) {
      const totalRows = records.length;
      const uniqueSites = new Set(records.map(row => row['Location ID'])).size;
      const annualSum = records.reduce((sum, row) => sum + (Number.isFinite(row['EstimatedAnnual']) ? row['EstimatedAnnual'] : 0), 0);
      const summaryItems = [
        `Rows: ${totalRows.toLocaleString()}`,
        `Unique Sites: ${uniqueSites.toLocaleString()}`,
        `Sum of Estimated Annual Count: ${Number.isFinite(annualSum) ? Math.round(annualSum).toLocaleString() : 'N/A'}`,
      ];
      summaryList.innerHTML = summaryItems.map(item => `<li>${item}</li>`).join('');
    }

    function renderTable(records) {
      const rows = records.slice(0, 500).map(row => {
        const est = Number.isFinite(row['EstimatedAnnual']) ? Math.round(row['EstimatedAnnual']).toLocaleString() : 'N/A';
        return `
          <tr>
            <td>${row['Type'] || ''}</td>
            <td>${row['Year'] ?? ''}</td>
            <td>${row['WisDOT Region'] || ''}</td>
            <td>${row['City'] || ''}</td>
            <td>${row['Location ID'] || ''}</td>
            <td>${row['Location Name'] || ''}</td>
            <td>${est}</td>
          </tr>`;
      }).join('');
      tableBody.innerHTML = rows || '<tr><td colspan="7" style="padding:16px;text-align:center;color:#64748b;">No records match the selected filters.</td></tr>';
    }

    function renderChart(records) {
      const chartContainer = document.getElementById('bar-container');
      const sorted = records
        .filter(row => Number.isFinite(row['EstimatedAnnual']))
        .slice()
        .sort((a, b) => b['EstimatedAnnual'] - a['EstimatedAnnual'])
        .slice(0, 20);

      if (!sorted.length) {
        chartContainer.innerHTML = '<div style="padding:24px;color:#64748b;">No chart data available for the current filters.</div>';
        return;
      }

      const labels = sorted.map(row => (row['Location Name'] || 'Unknown').slice(0, 40));
      const hoverText = sorted.map(row => `${row['Location Name'] || 'Unknown'}<br>${row['City'] || 'City N/A'} · ${row['WisDOT Region'] || 'Region N/A'}<br>Year ${row['Year'] ?? 'N/A'}`);
      const values = sorted.map(row => row['EstimatedAnnual']);

      const trace = {
        type: 'bar',
        x: labels,
        y: values,
        text: values.map(val => Math.round(val).toLocaleString()),
        textposition: 'outside',
        marker: {
          color: '#2563eb',
          opacity: 0.88,
        },
        hovertext: hoverText,
        hoverinfo: 'text+y',
      };

      const layout = {
        title: {
          text: 'Top Locations by Estimated Annual Count',
          font: {size: 20, family: 'Inter, system-ui, sans-serif', color: '#0f172a'},
        },
        margin: {l: 40, r: 18, t: 60, b: 120},
        height: 470,
        xaxis: {
          tickangle: -35,
          tickfont: {size: 11, family: 'Inter, system-ui, sans-serif'},
        },
        yaxis: {
          title: 'Estimated Annual Count',
          tickfont: {family: 'Inter, system-ui, sans-serif'},
        },
        bargap: 0.3,
        plot_bgcolor: 'rgba(255,255,255,0.97)',
        paper_bgcolor: 'rgba(255,255,255,0)',
      };

      Plotly.react(chartContainer, [trace], layout, {responsive: true, displayModeBar: false});
    }

    function handleUpdate() {
      const filtered = applyFilters(DATA);
      renderSummary(filtered);
      renderTable(filtered);
      renderChart(filtered);
    }

    typeSelect.addEventListener('change', handleUpdate);
    yearSelect.addEventListener('change', handleUpdate);
    citySelect.addEventListener('change', handleUpdate);
    regionSelect.addEventListener('change', handleUpdate);

    window.addEventListener('DOMContentLoaded', handleUpdate);
  </script>
</body>
</html>
            """,
            dataset=dataset,
        )

    @app.route("/statewide-map/")
    def statewide_map_slash():
        return redirect("/statewide-map", code=302)
