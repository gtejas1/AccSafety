# unified_explore.py
from __future__ import annotations

import io
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc

from theme import card, dash_page

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

# Visible columns (no Lon/Lat)
DISPLAY_COLUMNS = [
    {"name": "Location", "id": "Location"},
    {"name": "Duration", "id": "Duration"},
    {"name": "Total counts", "id": "Total counts", "type": "numeric"},
    {"name": "Source type", "id": "Source type"},
    {"name": "View", "id": "View", "presentation": "markdown"},
]

# Duration buckets produced by the unified view
DURATION_OPTIONS = [
    "0-15hrs",
    "15-48hrs",
    "2days-14days",
    "14days-30days",
    "1month-3months",
    "3months-6months",
    ">6months",
]

# Query exactly what we need from the view
UNIFIED_SQL = """
  SELECT
    "Location",
    "Duration",
    "Total counts",
    "Source type",
    "Source",
    "Facility type",
    "Mode"
  FROM unified_site_summary
"""

def _fetch_all() -> pd.DataFrame:
    try:
        df = pd.read_sql(UNIFIED_SQL, ENGINE)
    except Exception:
        df = pd.DataFrame(columns=[c["id"] for c in DISPLAY_COLUMNS] + ["Source", "Facility type", "Mode"])
    # Normalize key fields once to prevent mismatch from whitespace/case
    for col in ["Mode", "Facility type", "Source", "Duration", "Location", "Source type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    return df.fillna("")

def _encode_location_for_href(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return urllib.parse.quote(urllib.parse.unquote(text), safe="")

def _build_view_link(row: pd.Series) -> str:
    src = (row.get("Source") or "").strip()
    loc = (row.get("Location") or "").strip()
    loc_q = _encode_location_for_href(loc)
    if src == "Wisconsin Pilot Counting Counts":
        return f"[Open](/eco/dashboard?location={loc_q})"
    if src == "Off-Street Trail (SEWRPC Trail User Counts)":
        return f"[Open](/trail/dashboard?location={loc_q})"
    # Statewide modeled (renamed in SQL) and anything else
    return "[Open](/statewide-map)"

def _opts(vals) -> list[dict]:
    uniq = sorted({v for v in vals if isinstance(v, str) and v.strip()})
    return [{"label": v, "value": v} for v in uniq]

def create_unified_explore(server, prefix: str = "/explore/"):
    app = dash.Dash(
        name="unified_explore",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets",
    )
    app.title = "Explore"

    # Base data (filters applied client-side)
    base_df = _fetch_all()

    # Controls — progressive flow
    filter_block = card(
        [
            html.H2("Explore Counts"),
            html.P("Pick Mode, then Facility, then Source, then Duration.", className="app-muted"),

            # Mode
            html.Div(
                [
                    html.Label("Mode"),
                    dcc.Dropdown(
                        id="pf-mode",
                        options=_opts(base_df["Mode"].unique().tolist() or ["Pedestrian", "Bicyclist", "Both"]),
                        placeholder="Select mode",
                        clearable=True,
                    ),
                ],
                className="mb-3",
            ),

            # Facility (hidden until Mode)
            html.Div(
                [
                    html.Label("Facility type"),
                    dcc.Dropdown(id="pf-facility", placeholder="Select facility type", clearable=True),
                ],
                id="wrap-facility",
                style={"display": "none"},
                className="mb-3",
            ),

            # Source (hidden until Facility)
            html.Div(
                [
                    html.Label("Data source"),
                    dcc.Dropdown(id="pf-source", placeholder="Select data source", clearable=True),
                ],
                id="wrap-source",
                style={"display": "none"},
                className="mb-3",
            ),

            # Duration (hidden until Source)
            html.Div(
                [
                    html.Label("Duration"),
                    dcc.Dropdown(
                        id="pf-duration",
                        options=[{"label": d, "value": d} for d in DURATION_OPTIONS],
                        placeholder="Select duration",
                        clearable=True,
                    ),
                ],
                id="wrap-duration",
                style={"display": "none"},
                className="mb-2",
            ),

            # Download (only shown when table is visible)
            html.Div(
                [
                    html.Button("Download CSV", id="pf-download-btn", className="btn btn-outline-primary"),
                    dcc.Download(id="pf-download"),
                ],
                id="wrap-download",
                style={"display": "none"},
                className="d-flex justify-content-end",
            ),
        ],
        class_name="mb-3",
    )

    # Results (initially hidden; only show when all filters are selected)
    results_block = card(
        [
            dash_table.DataTable(
                id="pf-table",
                columns=DISPLAY_COLUMNS,
                data=[],
                markdown_options={"html": True, "link_target": "_self"},
                page_size=25,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_as_list_view=True,
                style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
                style_cell={"textAlign": "left", "padding": "8px"},
                style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
            )
        ],
        class_name="mb-3",
    )

    app.layout = dash_page(
        "Explore",
        [
            dbc.Row(
                [
                    dbc.Col(filter_block, lg=4, md=12, className="mb-3"),
                    dbc.Col(
                        html.Div(id="wrap-results", children=[results_block], style={"display": "none"}),
                        lg=8, md=12, className="mb-3",
                    ),
                ],
                className="g-3",
            ),
        ],
    )

    # ---- Progressive options / reveal ----
    @app.callback(
        Output("pf-facility", "options"),
        Output("wrap-facility", "style"),
        Output("pf-facility", "value"),
        Input("pf-mode", "value"),
        prevent_initial_call=True,
    )
    def _on_mode(mode):
        if not mode:
            return [], {"display": "none"}, None
        df = base_df[base_df["Mode"].str.casefold() == str(mode).strip().casefold()]
        return _opts(df["Facility type"].unique().tolist()), {"display": "block"}, None

    @app.callback(
        Output("pf-source", "options"),
        Output("wrap-source", "style"),
        Output("pf-source", "value"),
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        prevent_initial_call=True,
    )
    def _on_facility(mode, facility):
        if not (mode and facility):
            return [], {"display": "none"}, None
        df = base_df[
            (base_df["Mode"].str.casefold() == str(mode).strip().casefold()) &
            (base_df["Facility type"].str.casefold() == str(facility).strip().casefold())
        ]
        return _opts(df["Source"].unique().tolist()), {"display": "block"}, None

    @app.callback(
        Output("wrap-duration", "style"),
        Output("pf-duration", "value"),
        Input("pf-source", "value"),
        prevent_initial_call=True,
    )
    def _on_source(source):
        if not source:
            return {"display": "none"}, None
        return {"display": "block"}, None

    # ---- Apply filters AND control table + download visibility ----
    @app.callback(
        Output("pf-table", "data"),
        Output("wrap-results", "style"),
        Output("wrap-download", "style"),
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        Input("pf-duration", "value"),
        prevent_initial_call=False,
    )
    def _apply_filters(mode, facility, source, duration_key):
        # only show results when all filters are selected
        has_all = all([mode, facility, source, duration_key])
        if not has_all:
            return [], {"display": "none"}, {"display": "none"}

        df = base_df.copy()
        cf = str.casefold  # tiny helper

        df = df[df["Mode"].str.casefold() == cf(str(mode).strip())]
        df = df[df["Facility type"].str.casefold() == cf(str(facility).strip())]
        df = df[df["Source"].str.casefold() == cf(str(source).strip())]
        df = df[df["Duration"].str.casefold() == cf(str(duration_key).strip())]

        if df.empty:
            return [], {"display": "block"}, {"display": "flex"}  # visible (empty) + allow download of empty CSV

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        df = df[[c["id"] for c in DISPLAY_COLUMNS]]
        return df.to_dict("records"), {"display": "block"}, {"display": "flex"}

    # ---- Download filtered CSV (click-only) ----
    @app.callback(
        Output("pf-download", "data"),
        Input("pf-download-btn", "n_clicks"),
        State("pf-table", "data"),
        prevent_initial_call=True,
    )
    def _download(n_clicks, rows):
        if not n_clicks:
            return dash.no_update
        df = pd.DataFrame(rows or [])
        if df.empty:
            df = pd.DataFrame(columns=[c["id"] for c in DISPLAY_COLUMNS])
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return dict(content=buf.read(), filename="unified_explore_filtered.csv", type="text/csv")

    return app

# Optional: run standalone
if __name__ == "__main__":
    _server = dash.Dash(__name__).server
    _app = create_unified_explore(_server, prefix="/")
    _app.run_server(host="127.0.0.1", port=8068, debug=True)
