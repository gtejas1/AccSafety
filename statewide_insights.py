"""Statewide ArcGIS map page with embedded Dash insights viewer."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, List

import pandas as pd
from dash import Dash, Input, Output, dash_table, dcc, html
import plotly.express as px
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


STATEWIDE_DASH_APP: Dash | None = None
STATEWIDE_DATA: pd.DataFrame | None = None
STATEWIDE_TYPE_OPTS: List[str] = []
STATEWIDE_YEAR_OPTS: List[int] = []
STATEWIDE_CITY_OPTS: List[str] = []
STATEWIDE_REGION_OPTS: List[str] = []


def _load_dataset(label: str, path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=0)
    df.columns = df.columns.astype(str)

    missing = [c for c in BASE_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{label}: Missing columns {missing} in {path.name}")

    annual_col = ANNUAL_COLS[label]
    if annual_col not in df.columns:
        raise ValueError(f"{label}: Missing '{annual_col}' in {path.name}")

    out = df[BASE_COLUMNS + [annual_col]].copy()
    out.rename(columns={annual_col: "EstimatedAnnual"}, inplace=True)
    out["Type"] = label
    out["EstimatedAnnual"] = pd.to_numeric(out["EstimatedAnnual"], errors="coerce")
    out["Year"] = pd.to_numeric(out["Year"], errors="coerce")
    return out


@lru_cache(maxsize=1)
def _load_statewide_frame() -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    errors: Dict[str, str] = {}

    for label, filename in EXCEL_FILES.items():
        try:
            frames.append(_load_dataset(label, ASSETS_DIR / filename))
        except Exception as exc:
            errors[label] = str(exc)

    if not frames:
        raise SystemExit(f"Failed to load any dataset. Errors: {errors}")

    return pd.concat(frames, ignore_index=True)


def _prepare_options(data: pd.DataFrame) -> None:
    global STATEWIDE_TYPE_OPTS, STATEWIDE_YEAR_OPTS
    global STATEWIDE_CITY_OPTS, STATEWIDE_REGION_OPTS

    STATEWIDE_TYPE_OPTS = sorted(data["Type"].dropna().unique().tolist())
    STATEWIDE_YEAR_OPTS = sorted(int(y) for y in data["Year"].dropna().unique())
    STATEWIDE_CITY_OPTS = sorted(data["City"].dropna().unique().tolist())
    STATEWIDE_REGION_OPTS = sorted(data["WisDOT Region"].dropna().unique().tolist())


def _ensure_statewide_dash(server: Flask) -> Dash:
    global STATEWIDE_DASH_APP, STATEWIDE_DATA

    if STATEWIDE_DASH_APP is not None:
        return STATEWIDE_DASH_APP

    STATEWIDE_DATA = _load_statewide_frame()
    _prepare_options(STATEWIDE_DATA)

    dash_app = Dash(
        __name__,
        server=server,
        url_base_pathname="/statewide-map/viewer/",
    )
    dash_app.title = "WI Ped/Bike/Trail Counts (Simple Viewer)"

    dash_app.layout = html.Div(
        style={
            "fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, Arial",
            "padding": "18px",
        },
        children=[
            html.H2(
                "Wisconsin Pedestrian / Bicyclist / Trail Users — Simple Viewer",
                style={"marginTop": "0"},
            ),
            html.Div(
                style={
                    "display": "grid",
                    "gridTemplateColumns": "repeat(4, minmax(180px, 1fr))",
                    "gap": "12px",
                    "alignItems": "end",
                },
                children=[
                    html.Div(
                        [
                            html.Label("Type"),
                            dcc.Dropdown(
                                STATEWIDE_TYPE_OPTS,
                                value=STATEWIDE_TYPE_OPTS[0] if STATEWIDE_TYPE_OPTS else None,
                                id="type-dd",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Label("Year"),
                            dcc.Dropdown(
                                STATEWIDE_YEAR_OPTS,
                                multi=True,
                                value=[],
                                placeholder="All",
                                id="year-dd",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Label("City"),
                            dcc.Dropdown(
                                STATEWIDE_CITY_OPTS,
                                multi=True,
                                value=[],
                                placeholder="All",
                                id="city-dd",
                            ),
                        ]
                    ),
                    html.Div(
                        [
                            html.Label("WisDOT Region"),
                            dcc.Dropdown(
                                STATEWIDE_REGION_OPTS,
                                multi=True,
                                value=[],
                                placeholder="All",
                                id="region-dd",
                            ),
                        ]
                    ),
                ],
            ),
            html.Br(),
            html.Div(
                style={"display": "grid", "gridTemplateColumns": "2fr 1fr", "gap": "16px"},
                children=[
                    dcc.Graph(id="bar-top-locs"),
                    html.Div(
                        [html.H4("Summary"), html.Div(id="summary-box")],
                        style={
                            "border": "1px solid #ddd",
                            "borderRadius": "10px",
                            "padding": "12px",
                        },
                    ),
                ],
            ),
            html.Br(),
            html.H4("Filtered Records"),
            dash_table.DataTable(
                id="table",
                columns=[
                    {"name": c, "id": c}
                    for c in [
                        "Type",
                        "Year",
                        "WisDOT Region",
                        "City",
                        "Location ID",
                        "Location Name",
                        "EstimatedAnnual",
                    ]
                ],
                page_size=10,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"fontSize": "14px", "padding": "8px"},
                style_header={"backgroundColor": "#f5f5f5", "fontWeight": "600"},
                export_format="csv",
            ),
            html.Div(
                style={"marginTop": "12px", "fontSize": "12px", "color": "#666"},
                children="Tip: Use the table header filter boxes to further refine results; click column headers to sort.",
            ),
            html.Hr(),
            html.Div(
                style={"fontSize": "12px", "color": "#666"},
                children=[
                    "Data source: Wisconsin Ped/Bike Count Database Excel files (032824 versions). ",
                    "This is a lightweight viewer generated automatically.",
                ],
            ),
        ],
    )

    _register_dash_callbacks(dash_app)

    STATEWIDE_DASH_APP = dash_app
    return dash_app


def _apply_filters(df: pd.DataFrame, type_val, years, cities, regions) -> pd.DataFrame:
    filtered = df.copy()
    if type_val:
        filtered = filtered[filtered["Type"] == type_val]
    if years:
        filtered = filtered[filtered["Year"].isin(years)]
    if cities:
        filtered = filtered[filtered["City"].isin(cities)]
    if regions:
        filtered = filtered[filtered["WisDOT Region"].isin(regions)]
    return filtered


def _register_dash_callbacks(dash_app: Dash) -> None:
    @dash_app.callback(  # type: ignore[misc]
        Output("bar-top-locs", "figure"),
        Output("table", "data"),
        Output("summary-box", "children"),
        Input("type-dd", "value"),
        Input("year-dd", "value"),
        Input("city-dd", "value"),
        Input("region-dd", "value"),
    )
    def update_view(type_val, years, cities, regions):
        if years is None:
            years = []
        if cities is None:
            cities = []
        if regions is None:
            regions = []

        assert STATEWIDE_DATA is not None  # Data is prepared during Dash setup.

        filtered = _apply_filters(STATEWIDE_DATA, type_val, years, cities, regions)

        top = filtered.sort_values("EstimatedAnnual", ascending=False).head(20).copy()
        top["Label"] = top["Location Name"].str.slice(0, 40).fillna("Unknown")
        fig = px.bar(
            top,
            x="Label",
            y="EstimatedAnnual",
            hover_data=[
                "Location Name",
                "City",
                "WisDOT Region",
                "Year",
                "EstimatedAnnual",
            ],
            labels={"EstimatedAnnual": "Estimated Annual Count", "Label": "Location"},
            title="Top Locations by Estimated Annual Count (Top 20)",
        )
        fig.update_layout(
            margin=dict(l=10, r=10, t=50, b=10),
            xaxis_tickangle=-35,
            height=450,
        )

        total_sites = filtered["Location ID"].nunique()
        total_rows = len(filtered)
        total_est = filtered["EstimatedAnnual"].sum(min_count=1)
        summary = html.Ul(
            [
                html.Li(f"Rows: {total_rows:,}"),
                html.Li(f"Unique Sites: {total_sites:,}"),
                html.Li(
                    "Sum of Estimated Annual Count: "
                    + (
                        f"{int(total_est):,}" if pd.notna(total_est) else "N/A"
                    )
                ),
            ]
        )

        table_data = filtered[
            [
                "Type",
                "Year",
                "WisDOT Region",
                "City",
                "Location ID",
                "Location Name",
                "EstimatedAnnual",
            ]
        ].to_dict("records")

        return fig, table_data, summary


def register_statewide_insights(app: Flask) -> None:
    _ensure_statewide_dash(app)

    @app.route("/statewide-map")
    def statewide_map():
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
  <style>
    .app-shell {
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      background: #f8fafc;
    }

    .app-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 20px 28px;
      background: white;
      border-bottom: 1px solid rgba(15, 23, 42, 0.08);
    }

    .app-header-title {
      display: flex;
      flex-direction: column;
      gap: 4px;
    }

    .app-brand {
      font-size: 1.4rem;
      font-weight: 600;
      color: #0f172a;
    }

    .app-subtitle {
      font-size: 0.95rem;
      color: #64748b;
    }

    .app-nav .app-link {
      color: #2563eb;
      font-weight: 600;
      text-decoration: none;
    }

    .app-content {
      flex: 1;
      padding: 28px;
      display: flex;
      flex-direction: column;
      gap: 28px;
    }

    .map-frame {
      width: 100%;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 20px 44px rgba(15, 23, 42, 0.15);
      background: white;
    }

    .viewer-frame {
      border: none;
      width: 100%;
      min-height: 900px;
      border-radius: 22px;
      box-shadow: 0 18px 40px rgba(15, 23, 42, 0.12);
      background: white;
    }

    .app-card {
      padding: 20px;
      background: white;
      border-radius: 24px;
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

      <section class="app-card">
        <iframe class="viewer-frame" src="/statewide-map/viewer/"></iframe>
      </section>
    </main>
  </div>
</body>
</html>
            """,
        )

    @app.route("/statewide-map/")
    def statewide_map_slash():
        return redirect("/statewide-map", code=302)
