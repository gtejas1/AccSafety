# # unified_explore.py
# from __future__ import annotations

# import io
# import urllib.parse
# import pandas as pd
# from sqlalchemy import create_engine

# import dash
# from dash import dcc, html, Input, Output, State, dash_table
# import dash_bootstrap_components as dbc

# from theme import card, dash_page

# DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
# ENGINE = create_engine(DB_URL)

# # Visible columns (no Lon/Lat)
# DISPLAY_COLUMNS = [
#     {"name": "Location", "id": "Location"},
#     {"name": "Duration", "id": "Duration"},
#     {"name": "Total counts", "id": "Total counts", "type": "numeric"},
#     {"name": "Source type", "id": "Source type"},
#     {"name": "View", "id": "View", "presentation": "markdown"},
# ]

# # Pull exactly what we need from the view (now includes Mode)
# UNIFIED_SQL = """
#   SELECT
#     "Location",
#     "Duration",
#     "Total counts",
#     "Source type",
#     "Source",
#     "Facility type",
#     "Mode"
#   FROM unified_site_summary
# """

# def _fetch_all() -> pd.DataFrame:
#     try:
#         df = pd.read_sql(UNIFIED_SQL, ENGINE)
#     except Exception:
#         df = pd.DataFrame(columns=[c["id"] for c in DISPLAY_COLUMNS] + ["Source", "Facility type", "Mode"])
#     return df

# def _encode_location_for_href(text: str) -> str:
#     if not isinstance(text, str):
#         return ""
#     return urllib.parse.quote(urllib.parse.unquote(text), safe="")

# def _build_view_link(row: pd.Series) -> str:
#     src = (row.get("Source") or "").strip()
#     loc = (row.get("Location") or "").strip()
#     loc_q = _encode_location_for_href(loc)
#     if src == "Wisconsin Pilot Counting Counts":
#         return f"[Open](/eco/dashboard?location={loc_q})"
#     if src == "Off-Street Trail (SEWRPC Trail User Counts)":
#         return f"[Open](/trail/dashboard?location={loc_q})"
#     return "[Open](/statewide-map)"

# def _opts(vals) -> list[dict]:
#     return [{"label": v, "value": v} for v in sorted([v for v in vals if isinstance(v, str) and v.strip()])]

# def create_unified_explore(server, prefix: str = "/explore/"):
#     app = dash.Dash(
#         name="unified_explore",
#         server=server,
#         routes_pathname_prefix=prefix,
#         requests_pathname_prefix=prefix,
#         external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
#         suppress_callback_exceptions=True,
#         assets_url_path=f"{prefix.rstrip('/')}/assets",
#     )
#     app.title = "Explore"

#     # Base data (filters are applied client-side)
#     base_df = _fetch_all().fillna("")

#     # Controls — progressive flow
#     filter_block = card(
#         [
#             html.H2("Explore Counts"),
#             html.P("Pick Mode, then Facility, then Source, then Duration.", className="app-muted"),

#             # Mode
#             html.Div(
#                 [
#                     html.Label("Mode"),
#                     dcc.Dropdown(
#                         id="pf-mode",
#                         options=_opts(base_df["Mode"].unique().tolist() or ["Pedestrian", "Bicyclist", "Both"]),
#                         placeholder="Select mode",
#                         clearable=True,
#                     ),
#                 ],
#                 className="mb-3",
#             ),

#             # Facility (hidden until Mode)
#             html.Div(
#                 [
#                     html.Label("Facility type"),
#                     dcc.Dropdown(id="pf-facility", placeholder="Select facility type", clearable=True),
#                 ],
#                 id="wrap-facility",
#                 style={"display": "none"},
#                 className="mb-3",
#             ),

#             # Source (hidden until Facility)
#             html.Div(
#                 [
#                     html.Label("Data source"),
#                     dcc.Dropdown(id="pf-source", placeholder="Select data source", clearable=True),
#                 ],
#                 id="wrap-source",
#                 style={"display": "none"},
#                 className="mb-3",
#             ),

#             # Duration (hidden until Source)
#             html.Div(
#                 [
#                     html.Label("Duration"),
#                     dcc.Dropdown(
#                         id="pf-duration",
#                         options=[
#                             {"label": "Short-term", "value": "short"},
#                             {"label": "Long-term", "value": "long"},
#                         ],
#                         placeholder="Select duration",
#                         clearable=True,
#                     ),
#                 ],
#                 id="wrap-duration",
#                 style={"display": "none"},
#                 className="mb-2",
#             ),

#             # Download
#             html.Div(
#                 [
#                     html.Button("Download CSV", id="pf-download-btn", className="btn btn-outline-primary"),
#                     dcc.Download(id="pf-download"),
#                 ],
#                 id="wrap-download",
#                 style={"display": "none"},
#                 className="d-flex justify-content-end",
#             ),
#         ],
#         class_name="mb-3",
#     )

#     # Results
#     def _initial_table_records():
#         if base_df.empty: return []
#         df = base_df.copy()
#         df["View"] = df.apply(_build_view_link, axis=1)
#         return df[[c["id"] for c in DISPLAY_COLUMNS]].to_dict("records")

#     results_block = card(
#         [
#             dash_table.DataTable(
#                 id="pf-table",
#                 columns=DISPLAY_COLUMNS,
#                 data=_initial_table_records(),
#                 markdown_options={"html": True, "link_target": "_self"},
#                 page_size=25,
#                 sort_action="native",
#                 style_table={"overflowX": "auto"},
#                 style_as_list_view=True,
#                 style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
#                 style_cell={"textAlign": "left", "padding": "8px"},
#                 style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
#             )
#         ],
#         class_name="mb-3",
#     )

#     app.layout = dash_page(
#         "Explore",
#         [
#             dbc.Row(
#                 [
#                     dbc.Col(filter_block, lg=4, md=12, className="mb-3"),
#                     dbc.Col(results_block, lg=8, md=12, className="mb-3"),
#                 ],
#                 className="g-3",
#             ),
#         ],
#     )

#     # ---- Progressive options / reveal ----
#     @app.callback(
#         Output("pf-facility", "options"),
#         Output("wrap-facility", "style"),
#         Output("pf-facility", "value"),
#         Input("pf-mode", "value"),
#         prevent_initial_call=True,
#     )
#     def _on_mode(mode):
#         if not mode:
#             return [], {"display": "none"}, None
#         df = base_df[base_df["Mode"] == mode] if mode else base_df
#         return _opts(df["Facility type"].unique().tolist()), {"display": "block"}, None

#     @app.callback(
#         Output("pf-source", "options"),
#         Output("wrap-source", "style"),
#         Output("pf-source", "value"),
#         Input("pf-mode", "value"),
#         Input("pf-facility", "value"),
#         prevent_initial_call=True,
#     )
#     def _on_facility(mode, facility):
#         if not (mode and facility):
#             return [], {"display": "none"}, None
#         df = base_df[(base_df["Mode"] == mode) & (base_df["Facility type"] == facility)]
#         return _opts(df["Source"].unique().tolist()), {"display": "block"}, None

#     @app.callback(
#         Output("wrap-duration", "style"),
#         Output("wrap-download", "style"),
#         Output("pf-duration", "value"),
#         Input("pf-source", "value"),
#         prevent_initial_call=True,
#     )
#     def _on_source(source):
#         if not source:
#             return {"display": "none"}, {"display": "none"}, None
#         return {"display": "block"}, {"display": "flex"}, None

#     # ---- Apply filters to table ----
#     @app.callback(
#         Output("pf-table", "data"),
#         Input("pf-mode", "value"),
#         Input("pf-facility", "value"),
#         Input("pf-source", "value"),
#         Input("pf-duration", "value"),
#         prevent_initial_call=False,
#     )
#     def _apply_filters(mode, facility, source, duration_key):
#         df = base_df.copy()

#         if mode:
#             df = df[df["Mode"] == mode]
#         if facility:
#             df = df[df["Facility type"] == facility]
#         if source:
#             df = df[df["Source"] == source]
#         if duration_key == "short":
#             df = df[df["Duration"].str.lower().str.startswith("short")]
#         elif duration_key == "long":
#             df = df[df["Duration"].str.lower().str.startswith("long")]

#         if df.empty:
#             return []

#         df = df.copy()
#         df["View"] = df.apply(_build_view_link, axis=1)
#         df = df[[c["id"] for c in DISPLAY_COLUMNS]]
#         return df.to_dict("records")

#     # ---- Download filtered CSV (click-only) ----
#     @app.callback(
#         Output("pf-download", "data"),
#         Input("pf-download-btn", "n_clicks"),
#         State("pf-table", "data"),
#         prevent_initial_call=True,
#     )
#     def _download(n_clicks, rows):
#         if not n_clicks:
#             return dash.no_update
#         df = pd.DataFrame(rows or [])
#         if df.empty:
#             df = pd.DataFrame(columns=[c["id"] for c in DISPLAY_COLUMNS])
#         buf = io.StringIO()
#         df.to_csv(buf, index=False)
#         buf.seek(0)
#         return dict(content=buf.read(), filename="unified_explore_filtered.csv", type="text/csv")

#     return app

# # Optional: run standalone
# if __name__ == "__main__":
#     _server = dash.Dash(__name__).server
#     _app = create_unified_explore(_server, prefix="/")
#     _app.run_server(host="127.0.0.1", port=8068, debug=True)
#-------------------------------------------------------------------------------------------------------------------------

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

# Pull exactly what we need from the view (now includes Mode)
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
    return df

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
    return "[Open](/statewide-map)"

def _opts(vals) -> list[dict]:
    return [{"label": v, "value": v} for v in sorted([v for v in vals if isinstance(v, str) and v.strip()])]

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

    # Base data (filters are applied client-side)
    base_df = _fetch_all().fillna("")

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
                        options=[
                            {"label": "Short-term", "value": "short"},
                            {"label": "Long-term", "value": "long"},
                        ],
                        placeholder="Select duration",
                        clearable=True,
                    ),
                ],
                id="wrap-duration",
                style={"display": "none"},
                className="mb-2",
            ),

            # Download
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

    # Results (initially hidden; only show when all filters selected)
    results_block = card(
        [
            dash_table.DataTable(
                id="pf-table",
                columns=DISPLAY_COLUMNS,
                data=[],  # start empty
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
        df = base_df[base_df["Mode"] == mode] if mode else base_df
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
        df = base_df[(base_df["Mode"] == mode) & (base_df["Facility type"] == facility)]
        return _opts(df["Source"].unique().tolist()), {"display": "block"}, None

    @app.callback(
        Output("wrap-duration", "style"),
        Output("wrap-download", "style"),
        Output("pf-duration", "value"),
        Input("pf-source", "value"),
        prevent_initial_call=True,
    )
    def _on_source(source):
        if not source:
            return {"display": "none"}, {"display": "none"}, None
        return {"display": "block"}, {"display": "flex"}, None

    # ---- Apply filters AND control table visibility ----
    @app.callback(
        Output("pf-table", "data"),
        Output("wrap-results", "style"),
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
            return [], {"display": "none"}

        df = base_df.copy()
        df = df[df["Mode"] == mode]
        df = df[df["Facility type"] == facility]
        df = df[df["Source"] == source]
        if duration_key == "short":
            df = df[df["Duration"].str.lower().str.startswith("short")]
        elif duration_key == "long":
            df = df[df["Duration"].str.lower().str.startswith("long")]

        if df.empty:
            return [], {"display": "block"}  # show empty state when all filters chosen

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        df = df[[c["id"] for c in DISPLAY_COLUMNS]]
        return df.to_dict("records"), {"display": "block"}

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
