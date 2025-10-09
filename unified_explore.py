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

# ---- New constants for the custom UI source ----
NEW_SOURCE_NAME = "Annual Average Estimated Counts (Milwaukee County)"
NEW_FACILITY    = "On-Street (Sidewalk/Crosswalk/Bike Lane)"
NEW_MODE        = "Both"
STORYMAP_URL    = "https://storymaps.arcgis.com/stories/281bfdde23a7411ca63f84d1fafa2098"

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
    return "[Open](https://uwm.edu/ipit/wisconsin-pedestrian-volume-model/)"

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
        assets_folder="assets",
        assets_url_path=f"{prefix.rstrip('/')}/assets",
    )
    app.title = "Explore"

    base_df = _fetch_all()

    # Controls
    filter_block = card(
        [
            html.H2("Explore Counts"),
            html.P("Pick Mode, then Facility, then Source, then Duration.", className="app-muted"),
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
            html.Div(
                [html.Label("Facility type"), dcc.Dropdown(id="pf-facility", placeholder="Select facility type", clearable=True)],
                id="wrap-facility",
                style={"display": "none"},
                className="mb-3",
            ),
            html.Div(
                [html.Label("Data source"), dcc.Dropdown(id="pf-source", placeholder="Select data source", clearable=True)],
                id="wrap-source",
                style={"display": "none"},
                className="mb-3",
            ),
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
            html.Div(
                [html.Button("Download CSV", id="pf-download-btn", className="btn btn-outline-primary"), dcc.Download(id="pf-download")],
                id="wrap-download",
                style={"display": "none"},
                className="d-flex justify-content-end",
            ),
        ],
        class_name="mb-3",
    )

    # Description + Results
    desc_block = card([html.Div(id="pf-desc", children=[])], class_name="mb-3")
    results_block = card(
        [
            html.Div(id="pf-map", style={"display": "none"}),  # map/image area
            html.Div(  # table area
                id="wrap-table",
                children=[
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
            ),
        ],
        class_name="mb-3",
    )

    app.layout = dash_page(
        "Explore",
        [
            dbc.Row(
                [
                    dbc.Col(filter_block, lg=4, md=12, className="mb-3"),
                    dbc.Col(html.Div(id="wrap-results", children=[desc_block, results_block], style={"display": "none"}), lg=8, md=12, className="mb-3"),
                ],
                className="g-3",
            ),
        ],
    )

    # Progressive options
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
        facilities = df["Facility type"].unique().tolist()
        if str(mode).strip().casefold() == NEW_MODE.casefold():
            facilities = list(set(facilities) | {NEW_FACILITY})
        return _opts(facilities), {"display": "block"}, None

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
            (base_df["Mode"].str.casefold() == str(mode).strip().casefold())
            & (base_df["Facility type"].str.casefold() == str(facility).strip().casefold())
        ]
        sources = df["Source"].unique().tolist()
        if str(mode).strip().casefold() == NEW_MODE.casefold() and str(facility).strip().casefold() == NEW_FACILITY.casefold():
            sources = list(set(sources) | {NEW_SOURCE_NAME})
        return _opts(sources), {"display": "block"}, None

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

    # Apply filters + toggle map/table + description
    @app.callback(
        Output("pf-map", "children"),
        Output("pf-map", "style"),
        Output("pf-table", "data"),
        Output("wrap-table", "style"),
        Output("wrap-results", "style"),
        Output("wrap-download", "style"),
        Output("pf-desc", "children"),
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        Input("pf-duration", "value"),
        prevent_initial_call=False,
    )
    def _apply_filters(mode, facility, source, duration_key):
        has_all = all([mode, facility, source, duration_key])
        if not has_all:
            return [], {"display": "none"}, [], {"display": "none"}, {"display": "none"}, {"display": "none"}, []

        df = base_df.copy()
        cf = str.casefold
        df = df[df["Mode"].str.casefold() == cf(str(mode).strip())]
        df = df[df["Facility type"].str.casefold() == cf(str(facility).strip())]
        df = df[df["Source"].str.casefold() == cf(str(source).strip())]
        df = df[df["Duration"].str.casefold() == cf(str(duration_key).strip())]

        show_custom = (
            str(mode or "").strip().casefold() == NEW_MODE.casefold()
            and str(facility or "").strip().casefold() == NEW_FACILITY.casefold()
            and str(source or "").strip() == NEW_SOURCE_NAME
        )

        description = _custom_mke_estimated_desc() if show_custom else []

        if show_custom:
            # Title link above the image/map
            title_link = html.H4(
                html.A(
                    "Exploring Non-Motorist Activities in Milwaukee",
                    href=STORYMAP_URL,
                    target="_blank",
                    rel="noopener noreferrer",
                ),
                style={"margin": "0 0 10px 0"},
            )

            img_src = app.get_asset_url("mke_estimated_counts.png")
            map_children = html.Div(
                [
                    title_link,
                    # Make the image clickable with the same hyperlink
                    html.A(
                        html.Img(
                            src=img_src,
                            alt="Milwaukee non-motorist activity map",
                            style={
                                "width": "100%",
                                "height": "auto",
                                "borderRadius": "10px",
                                "boxShadow": "0 1px 4px rgba(0,0,0,0.1)",
                            },
                        ),
                        href=STORYMAP_URL,
                        target="_blank",
                        rel="noopener noreferrer",
                        style={"display": "block"},
                    ),
                    html.Div(
                        "Placeholder map image — will be replaced by an embedded ArcGIS map.",
                        className="app-muted",
                        style={"marginTop": "6px"},
                    ),
                ]
            )
            return map_children, {"display": "block"}, [], {"display": "none"}, {"display": "block"}, {"display": "flex"}, description

        if df.empty:
            return [], {"display": "none"}, [], {"display": "block"}, {"display": "block"}, {"display": "flex"}, description

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        df = df[[c["id"] for c in DISPLAY_COLUMNS]]
        rows = df.to_dict("records")
        return [], {"display": "none"}, rows, {"display": "block"}, {"display": "block"}, {"display": "flex"}, description

    def _custom_mke_estimated_desc():
        return html.Div(
            [
                html.P(
                    [
                        "The estimated counts have been developed by utilizing crowdsourced data and long-term trail counts in Milwaukee County. ",
                        "For information regarding the source of the data, please refer to the project pages, ",
                        html.A(
                            "Estimating Statewide Bicycle Volumes Using Crowdsourced Data",
                            href="https://uwm.edu/ipit/projects/estimating-statewide-bicycle-volumes-using-crowdsourced-data/",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        " and ",
                        html.A(
                            "Estimating Statewide Bicycle Volumes Using Crowdsourced Data, Phase II",
                            href="https://uwm.edu/ipit/projects/estimating-statewide-bicycle-volumes-using-crowdsourced-data-phase-ii/",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        ".",
                    ],
                    className="app-muted",
                    style={"margin": "0"},
                )
            ]
        )

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
