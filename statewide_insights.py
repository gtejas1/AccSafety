"""Statewide map and analytics view for the AccSafety portal."""

import os
from functools import lru_cache
from typing import List, Tuple

import pandas as pd
import plotly.express as px
from flask import Flask, redirect, render_template_string, session

ASSETS_DIR = os.path.join(os.path.dirname(__file__), "assets")

DATASETS = (
    {
        "label": "Bicyclist Counts",
        "file": "WisconsinPedBikeCountDatabase_BicyclistCounts_032824.xlsx",
        "total_col": "Total bike count during count period",
        "annual_col": "Estimated Annual Bicyclist Count",
    },
    {
        "label": "Pedestrian Counts",
        "file": "WisconsinPedBikeCountDatabase_PedestrianCounts_032824.xlsx",
        "total_col": "Total ped count during count period",
        "annual_col": "Estimated Annual Pedestrian Count",
    },
    {
        "label": "Trail User Counts",
        "file": "WisconsinPedBikeCountDatabase_TrailUserCounts_032824.xlsx",
        "total_col": "Total trail count during count period",
        "annual_col": "Estimated Annual Trail User Count",
    },
)


@lru_cache()
def load_dataset(file_name: str) -> pd.DataFrame:
    """Read an Excel workbook from the assets directory, caching the result."""
    path = os.path.join(ASSETS_DIR, file_name)
    return pd.read_excel(path)


def build_summary_and_charts() -> Tuple[List[dict], dict, dict]:
    """Aggregate dataset information for display on the statewide insights view."""

    summaries = []
    yearly_rows = []
    city_rows = []

    for spec in DATASETS:
        df = load_dataset(spec["file"])
        total_col = spec["total_col"]
        annual_col = spec["annual_col"]

        df = df.copy()
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df[total_col] = pd.to_numeric(df[total_col], errors="coerce")
        df[annual_col] = pd.to_numeric(df[annual_col], errors="coerce")

        year_series = df["Year"].dropna()
        year_min = int(year_series.min()) if not year_series.empty else None
        year_max = int(year_series.max()) if not year_series.empty else None
        count_total = float(df[total_col].sum(skipna=True))
        annual_total = float(df[annual_col].sum(skipna=True))

        summaries.append(
            {
                "label": spec["label"],
                "locations": int(df["Location ID"].nunique()),
                "year_min": year_min,
                "year_max": year_max,
                "count_total_fmt": f"{count_total:,.0f}",
                "annual_total_fmt": f"{annual_total:,.0f}",
            }
        )

        yearly = (
            df.dropna(subset=["Year", total_col])
            .groupby("Year", as_index=False)[total_col]
            .sum()
        )
        yearly["Dataset"] = spec["label"]
        yearly.rename(columns={total_col: "TotalCount"}, inplace=True)
        yearly_rows.append(yearly)

        city = (
            df.dropna(subset=["City", annual_col])
            .groupby("City", as_index=False)[annual_col]
            .sum()
        )
        city.rename(columns={annual_col: "AnnualCount"}, inplace=True)
        city_rows.append(city)

    yearly_df = pd.concat(yearly_rows, ignore_index=True)
    yearly_fig = px.line(
        yearly_df,
        x="Year",
        y="TotalCount",
        color="Dataset",
        markers=True,
        title="Total Counts Recorded by Year",
        labels={"TotalCount": "Count During Observation Period"},
    )

    city_df = pd.concat(city_rows, ignore_index=True)
    city_totals = (
        city_df.groupby("City", as_index=False)["AnnualCount"].sum().nlargest(10, "AnnualCount")
    )
    city_fig = px.bar(
        city_totals.sort_values("AnnualCount"),
        x="AnnualCount",
        y="City",
        orientation="h",
        title="Top 10 Cities by Estimated Annual Activity",
        labels={"AnnualCount": "Estimated Annual Count"},
    )

    return summaries, yearly_fig.to_plotly_json(), city_fig.to_plotly_json()


def register_statewide_insights(app: Flask) -> None:
    """Attach the statewide insights routes to the provided Flask app."""

    @app.route("/statewide-map")
    def statewide_map():
        summaries, yearly_payload, city_payload = build_summary_and_charts()
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
  <script src="https://cdn.plot.ly/plotly-2.26.0.min.js" integrity="sha384-TUwQysHrfTxr3yyaznB1ZEYMb9x3tv9PXVSlZNO9il5dIpsG5uhAgY8Cr2Mk3p+8" crossorigin="anonymous"></script>
  <style>
    .insights-layout {
      display: grid;
      gap: 24px;
    }
    .insight-summary-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
    }
    .insight-card {
      padding: 18px;
      border-radius: 16px;
      background: white;
      box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
    }
    .insight-card h3 {
      margin: 0 0 12px;
      font-size: 1.05rem;
      color: #0b1736;
    }
    .insight-card p {
      margin: 4px 0;
      color: #334155;
      font-size: 0.95rem;
    }
    .chart-card {
      padding: 18px;
      border-radius: 16px;
      background: white;
      box-shadow: 0 18px 36px rgba(15, 23, 42, 0.1);
    }
    .map-frame {
      width: 100%;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 20px 44px rgba(15, 23, 42, 0.15);
    }
    @media (min-width: 1000px) {
      .charts-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 24px;
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
        <a class="app-link" href="/">Portal Home</a>
        <a class="app-link" href="/statewide-map">Refresh Insights</a>
        <a class="app-link" href="https://uwm.edu/ipit/wi-pedbike-dashboard/" target="_blank" rel="noopener noreferrer">Program Home</a>
      </nav>
      <div class="app-user">Signed in as <strong>{{ user }}</strong></div>
    </header>

    <main class="app-content">
      <section class="app-card insights-layout">
        <div>
          <h1>Wisconsin Pedestrian, Bicyclist, and Trail Activity</h1>
          <p class="app-muted">Explore the statewide ArcGIS map and quickly scan key highlights derived from the short-term count databases.</p>
        </div>

        <div class="map-frame">
          <arcgis-embedded-map style="height:600px;width:100%;" item-id="5badd855f3384cb1ab03eb0470a93f20" theme="light" bookmarks-enabled heading-enabled legend-enabled information-enabled center="-88.01456655273279,42.991659663963226" scale="1155581.108577" portal-url="https://uwm.maps.arcgis.com"></arcgis-embedded-map>
        </div>

        <div>
          <h2>Database Snapshot</h2>
          <div class="insight-summary-grid">
            {% for summary in summaries %}
            <article class="insight-card">
              <h3>{{ summary.label }}</h3>
              <p><strong>{{ summary.locations }}</strong> unique count locations</p>
              {% if summary.year_min and summary.year_max %}
              <p>Coverage: {{ summary.year_min }} – {{ summary.year_max }}</p>
              {% endif %}
              <p>Observed totals: <strong>{{ summary.count_total_fmt }}</strong></p>
              <p>Estimated annual volume: <strong>{{ summary.annual_total_fmt }}</strong></p>
            </article>
            {% endfor %}
          </div>
        </div>

        <div class="charts-grid">
          <section class="chart-card">
            <h2>Total Counts Recorded by Year</h2>
            <div id="yearly-chart" style="height:420px"></div>
          </section>
          <section class="chart-card">
            <h2>Top Cities by Estimated Annual Activity</h2>
            <div id="city-chart" style="height:420px"></div>
          </section>
        </div>
      </section>
    </main>
  </div>

  <script>
    const yearlyFig = {{ yearly_payload|tojson }};
    const cityFig = {{ city_payload|tojson }};
    Plotly.newPlot('yearly-chart', yearlyFig.data, yearlyFig.layout, {responsive: true, displaylogo: false});
    Plotly.newPlot('city-chart', cityFig.data, cityFig.layout, {responsive: true, displaylogo: false});
  </script>
</body>
</html>
            """,
            summaries=summaries,
            yearly_payload=yearly_payload,
            city_payload=city_payload,
            user=session.get("user", "user"),
        )

    @app.route("/statewide-map/")
    def statewide_map_slash():
        return redirect("/statewide-map", code=302)
