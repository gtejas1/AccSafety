# # unified_explore.py
# from __future__ import annotations

# import io
# import urllib.parse
# from dataclasses import dataclass
# from typing import List, Optional

# import pandas as pd
# from sqlalchemy import create_engine, text

# import dash
# from dash import dcc, html, Input, Output, State, dash_table
# import dash_bootstrap_components as dbc

# from theme import card, dash_page

# # ─────────────────────────────────────────────────────────────────────────────
# # Config
# # Reuse the same DB as other apps. Adjust if yours differs.
# DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
# ENGINE = create_engine(DB_URL)

# # Optional: use these to normalize how each source appears in the unified view.
# @dataclass
# class SourceConfig:
#     # Where it comes from
#     label: str               # e.g. "Eco (Pilot Counts)"
#     source_key: str          # "eco" | "trail" | "vivacity" | "wisdot" | "modeled" | "crowdsourced"
#     # How we present it
#     source_type: str         # "Actual" | "Modeled" | "Crowdsourced"
#     duration: str            # "Short-term" | "Long-term" | "Unknown"
#     facility_type: str       # "Intersection" | "Midblock" | "On-street" | "Off-street trail" | "Unknown"
#     # How to build a deep link to the source-specific dashboard
#     link_prefix: str         # e.g. "/eco/dashboard?location="

# ECO = SourceConfig(
#     label="Eco (Pilot Counts)",
#     source_key="eco",
#     source_type="Actual",
#     duration="Short-term",
#     facility_type="Intersection",   # adjust if your eco data includes non-intersections
#     link_prefix="/eco/dashboard?location=",
# )

# TRAIL = SourceConfig(
#     label="WisDOT Trails",
#     source_key="trail",
#     source_type="Actual",
#     duration="Long-term",
#     facility_type="Off-street trail",
#     link_prefix="/trail/dashboard?location=",
# )

# # Future placeholders (easy to add later)
# # VIVACITY = SourceConfig(...); MODELED = SourceConfig(...); CROWDSOURCED = SourceConfig(...)

# # Columns we will output in the unified table
# UNIFIED_COLUMNS = [
#     "Location",
#     "Source",
#     "Source type",
#     "Duration",
#     "Facility type",
#     "Start date",
#     "End date",
#     "Total counts",
#     "Avg hourly",
#     "View",
# ]

# # ─────────────────────────────────────────────────────────────────────────────
# # Data access helpers

# def _read_eco_summary() -> pd.DataFrame:
#     """
#     Summary by location for eco_traffic_data.
#     Expects columns: location_name, date, count (and optionally direction).
#     """
#     q = """
#         SELECT
#             location_name,
#             MIN(date) AS start_date,
#             MAX(date) AS end_date,
#             SUM(count)::bigint AS total_counts,
#             AVG(count)::numeric(12,2) AS average_hourly_count
#         FROM eco_traffic_data
#         GROUP BY location_name
#         ORDER BY location_name
#     """
#     df = pd.read_sql(q, ENGINE)
#     if df.empty:
#         return df

#     df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
#     df["end_date"]   = pd.to_datetime(df["end_date"]).dt.date
#     df["average_hourly_count"] = df["average_hourly_count"].round(0).astype(int)
#     df.rename(columns={
#         "location_name": "Location",
#         "start_date": "Start date",
#         "end_date": "End date",
#         "total_counts": "Total counts",
#         "average_hourly_count": "Avg hourly",
#     }, inplace=True)
#     df["Source"] = ECO.label
#     df["Source type"] = ECO.source_type
#     df["Duration"] = ECO.duration
#     df["Facility type"] = ECO.facility_type
#     df["View"] = df["Location"].apply(lambda loc: f"[Open]({ECO.link_prefix}{urllib.parse.quote(str(loc))})")
#     return df[UNIFIED_COLUMNS]


# def _read_trail_summary() -> pd.DataFrame:
#     """
#     Summary by location for hr_traffic_data (WisDOT trails).
#     Expects columns: location_name, date, count (and optionally direction).
#     """
#     q = """
#         SELECT
#             location_name,
#             MIN(date) AS start_date,
#             MAX(date) AS end_date,
#             SUM(count)::bigint AS total_counts,
#             AVG(count)::numeric(12,2) AS average_hourly_count
#         FROM hr_traffic_data
#         GROUP BY location_name
#         ORDER BY location_name
#     """
#     df = pd.read_sql(q, ENGINE)
#     if df.empty:
#         return df

#     df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
#     df["end_date"]   = pd.to_datetime(df["end_date"]).dt.date
#     df["average_hourly_count"] = df["average_hourly_count"].round(0).astype(int)
#     df.rename(columns={
#         "location_name": "Location",
#         "start_date": "Start date",
#         "end_date": "End date",
#         "total_counts": "Total counts",
#         "average_hourly_count": "Avg hourly",
#     }, inplace=True)
#     df["Source"] = TRAIL.label
#     df["Source type"] = TRAIL.source_type
#     df["Duration"] = TRAIL.duration
#     df["Facility type"] = TRAIL.facility_type
#     df["View"] = df["Location"].apply(lambda loc: f"[Open]({TRAIL.link_prefix}{urllib.parse.quote(str(loc))})")
#     return df[UNIFIED_COLUMNS]


# def _load_unified_summary() -> pd.DataFrame:
#     """Union supported sources into one tidy frame."""
#     frames: List[pd.DataFrame] = []

#     try:
#         eco = _read_eco_summary()
#         if not eco.empty:
#             frames.append(eco)
#     except Exception as e:
#         # Keep running even if one source fails
#         frames.append(pd.DataFrame(columns=UNIFIED_COLUMNS))

#     try:
#         trail = _read_trail_summary()
#         if not trail.empty:
#             frames.append(trail)
#     except Exception as e:
#         frames.append(pd.DataFrame(columns=UNIFIED_COLUMNS))

#     if not frames:
#         return pd.DataFrame(columns=UNIFIED_COLUMNS)

#     df = pd.concat(frames, ignore_index=True)
#     # Stable sort by Location then Source for a predictable table
#     df.sort_values(["Location", "Source"], kind="stable", inplace=True)
#     df.reset_index(drop=True, inplace=True)
#     return df


# # ─────────────────────────────────────────────────────────────────────────────
# # Dash App

# def create_unified_explore(server, prefix: str = "/explore/"):
#     """
#     Attach a unified Explore page to the shared Flask server.
#     Route: /explore/
#     """
#     app = dash.Dash(
#         name="unified_explore",
#         server=server,
#         routes_pathname_prefix=prefix,
#         requests_pathname_prefix=prefix,
#         external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
#         suppress_callback_exceptions=True,
#         assets_url_path=f"{prefix.rstrip('/')}/assets",
#     )
#     app.title = "Explore (Unified Filters)"

#     # Preload the union once (fast to compute; we can also refresh via a button)
#     BASE_DF = _load_unified_summary()

#     # Filter option values
#     source_type_opts = sorted(BASE_DF["Source type"].dropna().unique().tolist())
#     duration_opts    = sorted(BASE_DF["Duration"].dropna().unique().tolist())
#     facility_opts    = sorted(BASE_DF["Facility type"].dropna().unique().tolist())

#     min_date = BASE_DF["Start date"].min() if not BASE_DF.empty else None
#     max_date = BASE_DF["End date"].max() if not BASE_DF.empty else None

#     # Layout
#     app.layout = dash_page(
#         "Explore (Unified Filters)",
#         [
#             card(
#                 [
#                     html.H2("Explore Locations Across Datasets"),
#                     html.P(
#                         "Filter by source type, duration, facility type, and date range—then open the corresponding dashboard page for details.",
#                         className="app-muted",
#                     ),
#                     dbc.Row(
#                         [
#                             dbc.Col(
#                                 [
#                                     html.Label("Source type"),
#                                     dcc.Dropdown(
#                                         id="u-src-type",
#                                         options=[{"label": s, "value": s} for s in source_type_opts],
#                                         multi=True,
#                                         placeholder="All",
#                                     ),
#                                 ],
#                                 lg=3, md=6, sm=12, className="mb-2",
#                             ),
#                             dbc.Col(
#                                 [
#                                     html.Label("Duration"),
#                                     dcc.Dropdown(
#                                         id="u-duration",
#                                         options=[{"label": s, "value": s} for s in duration_opts],
#                                         multi=True,
#                                         placeholder="All",
#                                     ),
#                                 ],
#                                 lg=3, md=6, sm=12, className="mb-2",
#                             ),
#                             dbc.Col(
#                                 [
#                                     html.Label("Facility type"),
#                                     dcc.Dropdown(
#                                         id="u-facility",
#                                         options=[{"label": s, "value": s} for s in facility_opts],
#                                         multi=True,
#                                         placeholder="All",
#                                     ),
#                                 ],
#                                 lg=3, md=6, sm=12, className="mb-2",
#                             ),
#                             dbc.Col(
#                                 [
#                                     html.Label("Search (location contains)"),
#                                     dcc.Input(
#                                         id="u-search",
#                                         type="text",
#                                         placeholder="e.g., Capitol City Trail…",
#                                         className="form-control",
#                                     ),
#                                 ],
#                                 lg=3, md=6, sm=12, className="mb-2",
#                             ),
#                         ],
#                         className="g-2",
#                     ),
#                     dbc.Row(
#                         [
#                             dbc.Col(
#                                 [
#                                     html.Label("Date range"),
#                                     dcc.DatePickerRange(
#                                         id="u-dates",
#                                         min_date_allowed=min_date,
#                                         max_date_allowed=max_date,
#                                         start_date=min_date,
#                                         end_date=max_date,
#                                         display_format="YYYY-MM-DD",
#                                     ),
#                                 ],
#                                 lg=6, md=12, sm=12, className="mb-2",
#                             ),
#                             dbc.Col(
#                                 [
#                                     html.Br(),
#                                     dbc.Button("Refresh", id="u-refresh", color="primary", className="me-2"),
#                                     dbc.Button("Download CSV", id="u-download-btn", color="secondary"),
#                                     dcc.Download(id="u-download"),
#                                 ],
#                                 lg=6, md=12, sm=12, className="mb-2 d-flex align-items-end justify-content-end",
#                             ),
#                         ],
#                         className="g-2",
#                     ),
#                 ],
#                 class_name="mb-4",
#             ),
#             card(
#                 [
#                     dash_table.DataTable(
#                         id="u-table",
#                         columns=[
#                             {"name": "Location", "id": "Location"},
#                             {"name": "Source", "id": "Source"},
#                             {"name": "Source type", "id": "Source type"},
#                             {"name": "Duration", "id": "Duration"},
#                             {"name": "Facility type", "id": "Facility type"},
#                             {"name": "Start date", "id": "Start date"},
#                             {"name": "End date", "id": "End date"},
#                             {"name": "Total counts", "id": "Total counts", "type": "numeric"},
#                             {"name": "Avg hourly", "id": "Avg hourly", "type": "numeric"},
#                             {"name": "View", "id": "View", "presentation": "markdown"},
#                         ],
#                         data=BASE_DF.to_dict("records"),
#                         markdown_options={"html": True, "link_target": "_self"},
#                         page_size=20,
#                         sort_action="native",
#                         filter_action="none",
#                         style_table={"overflowX": "auto"},
#                         style_as_list_view=True,
#                         style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
#                         style_cell={"textAlign": "center", "padding": "8px"},
#                         style_data_conditional=[
#                             {"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"},
#                         ],
#                     )
#                 ]
#             ),
#         ],
#     )

#     # Filter + refresh
#     @app.callback(
#         Output("u-table", "data"),
#         Input("u-refresh", "n_clicks"),
#         State("u-src-type", "value"),
#         State("u-duration", "value"),
#         State("u-facility", "value"),
#         State("u-search", "value"),
#         State("u-dates", "start_date"),
#         State("u-dates", "end_date"),
#         prevent_initial_call=True,
#     )
#     def _apply_filters(_n, src_types, durations, facilities, search_text, start_date, end_date):
#         df = _load_unified_summary()
#         if df.empty:
#             return []

#         def _in_multi(col, vals):
#             if not vals:
#                 return pd.Series([True] * len(df))
#             return df[col].isin(vals)

#         mask = _in_multi("Source type", src_types) & _in_multi("Duration", durations) & _in_multi("Facility type", facilities)

#         # Date range overlaps logic: keep rows where [Start..End] intersects [start_date..end_date]
#         if start_date:
#             mask &= pd.to_datetime(df["End date"]) >= pd.to_datetime(start_date)
#         if end_date:
#             mask &= pd.to_datetime(df["Start date"]) <= pd.to_datetime(end_date)

#         if search_text and str(search_text).strip():
#             s = str(search_text).strip().lower()
#             mask &= df["Location"].str.lower().str.contains(s, na=False)

#         out = df.loc[mask].copy()
#         out.sort_values(["Location", "Source"], kind="stable", inplace=True)
#         return out.to_dict("records")

#     # Download
#     @app.callback(
#         Output("u-download", "data"),
#         Input("u-download-btn", "n_clicks"),
#         State("u-table", "data"),
#         prevent_initial_call=True,
#     )
#     def _download(_n, rows):
#         df = pd.DataFrame(rows or [])
#         if df.empty:
#             df = pd.DataFrame(columns=UNIFIED_COLUMNS)
#         buf = io.StringIO()
#         df.to_csv(buf, index=False)
#         buf.seek(0)
#         return dict(content=buf.read(), filename="unified_explore.csv", type="text/csv")

#     return app
# --------------------------------------------------------------------------------------------------------------
# unified_explore.py
# from __future__ import annotations

# import io
# import urllib.parse
# from typing import List, Tuple

# import pandas as pd
# from sqlalchemy import create_engine

# import dash
# from dash import dcc, html, Input, Output, State, dash_table
# import dash_bootstrap_components as dbc

# from theme import card, dash_page

# # ─────────────────────────────────────────────────────────────────────────────
# # CONFIG — uses same DB URL style as your other apps
# DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
# ENGINE = create_engine(DB_URL)

# # Tweak these mappings if needed (based on your categorization doc)
# DATASET_MAP = {
#     # eco_traffic_data → “Actual / Short-term / Intersection”
#     "eco": {
#         "source_label": "Eco (Pilot Counts)",
#         "source_type": "Actual",
#         "duration": "short",                     # short | long
#         "facility_type": "intersection",         # intersection | midblock | on_street | off_street_trail
#         "mode": "both",                          # pedestrian | bicycle | both
#         "view_prefix": "/eco/dashboard?location=",
#         "table": "eco_traffic_data",
#     },
#     # hr_traffic_data → “Actual / Long-term / Off-street trail”
#     "trail": {
#         "source_label": "WisDOT Trails",
#         "source_type": "Actual",
#         "duration": "long",
#         "facility_type": "off_street_trail",
#         "mode": "both",
#         "view_prefix": "/trail/dashboard?location=",
#         "table": "hr_traffic_data",
#     },
#     # Add more datasets later (vivacity, modeled, crowdsourced, etc.)
# }

# UNIFIED_COLUMNS = [
#     "Location",          # str
#     "Mode",              # pedestrian | bicycle | both
#     "Facility type",     # intersection | midblock | on_street | off_street_trail
#     "Source",            # Eco (Pilot Counts) | WisDOT Trails | …
#     "Source type",       # Actual | Modeled | Crowdsourced
#     "Duration",          # short | long
#     "Start date",        # date
#     "End date",          # date
#     "Total counts",      # int
#     "Avg hourly",        # int
#     "View",              # [Open](/eco/dashboard?location=…)
# ]

# # ─────────────────────────────────────────────────────────────────────────────
# # DATA ACCESS

# def _read_summary_one(table_name: str) -> pd.DataFrame:
#     """
#     Generic summary by location for a table that has:
#       - location_name (text)
#       - date (date/datetime)
#       - count (numeric)
#     """
#     q = f"""
#         SELECT
#             location_name,
#             MIN(date) AS start_date,
#             MAX(date) AS end_date,
#             SUM(count)::bigint AS total_counts,
#             AVG(count)::numeric(12,2) AS average_hourly_count
#         FROM {table_name}
#         GROUP BY location_name
#     """
#     df = pd.read_sql(q, ENGINE)
#     if df.empty:
#         return df
#     df["start_date"] = pd.to_datetime(df["start_date"]).dt.date
#     df["end_date"]   = pd.to_datetime(df["end_date"]).dt.date
#     df["average_hourly_count"] = df["average_hourly_count"].round(0).astype("Int64")
#     df.rename(columns={
#         "location_name": "Location",
#         "start_date": "Start date",
#         "end_date": "End date",
#         "total_counts": "Total counts",
#         "average_hourly_count": "Avg hourly",
#     }, inplace=True)
#     return df[["Location", "Start date", "End date", "Total counts", "Avg hourly"]]


# def _attach_meta(df: pd.DataFrame, *, mode: str, facility_type: str,
#                  source_label: str, source_type: str, duration: str,
#                  view_prefix: str) -> pd.DataFrame:
#     if df.empty:
#         # still return all needed columns for concatenation
#         return pd.DataFrame(columns=UNIFIED_COLUMNS)
#     out = df.copy()
#     out["Mode"] = mode
#     out["Facility type"] = facility_type
#     out["Source"] = source_label
#     out["Source type"] = source_type
#     out["Duration"] = duration
#     out["View"] = out["Location"].apply(lambda loc: f"[Open]({view_prefix}{urllib.parse.quote(str(loc))})")
#     # Order columns
#     return out[UNIFIED_COLUMNS]


# def _load_unified_summary() -> pd.DataFrame:
#     frames: List[pd.DataFrame] = []
#     # ECO
#     eco = _read_summary_one(DATASET_MAP["eco"]["table"])
#     frames.append(_attach_meta(
#         eco,
#         mode=DATASET_MAP["eco"]["mode"],
#         facility_type=DATASET_MAP["eco"]["facility_type"],
#         source_label=DATASET_MAP["eco"]["source_label"],
#         source_type=DATASET_MAP["eco"]["source_type"],
#         duration=DATASET_MAP["eco"]["duration"],
#         view_prefix=DATASET_MAP["eco"]["view_prefix"],
#     ))
#     # TRAIL
#     trail = _read_summary_one(DATASET_MAP["trail"]["table"])
#     frames.append(_attach_meta(
#         trail,
#         mode=DATASET_MAP["trail"]["mode"],
#         facility_type=DATASET_MAP["trail"]["facility_type"],
#         source_label=DATASET_MAP["trail"]["source_label"],
#         source_type=DATASET_MAP["trail"]["source_type"],
#         duration=DATASET_MAP["trail"]["duration"],
#         view_prefix=DATASET_MAP["trail"]["view_prefix"],
#     ))
#     df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=UNIFIED_COLUMNS)
#     # stable sort
#     if not df.empty:
#         df.sort_values(["Location", "Source"], kind="stable", inplace=True)
#         df.reset_index(drop=True, inplace=True)
#     return df


# # ─────────────────────────────────────────────────────────────────────────────
# # DASH APP

# def create_unified_explore(server, prefix: str = "/explore/"):
#     """
#     Progressive filters:
#       1) Mode → shows Facility
#       2) Facility → shows Data source
#       3) Data source → shows Duration
#       4) Duration chosen → results table appears
#     """
#     app = dash.Dash(
#         name="unified_explore",
#         server=server,
#         routes_pathname_prefix=prefix,
#         requests_pathname_prefix=prefix,
#         external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
#         suppress_callback_exceptions=True,
#         assets_url_path=f"{prefix.rstrip('/')}/assets",
#     )
#     app.title = "Explore (Unified)"

#     # Load once at startup (you can swap to live DB DISTINCT queries later)
#     BASE_DF = _load_unified_summary()

#     # Option lists from data
#     mode_opts = sorted(BASE_DF["Mode"].dropna().unique().tolist())
#     facility_opts = sorted(BASE_DF["Facility type"].dropna().unique().tolist())
#     source_opts = sorted(BASE_DF["Source"].dropna().unique().tolist())

#     # Date extents for overall range
#     min_date = BASE_DF["Start date"].min() if not BASE_DF.empty else None
#     max_date = BASE_DF["End date"].max() if not BASE_DF.empty else None

#     # Layout — progressive reveal via wrapping divs we show/hide
#     filter_block = card(
#         [
#             html.H2("Explore Locations (Progressive Filters)"),
#             html.P("Start by selecting a Mode; more filters will appear as you go.", className="app-muted"),
#             dbc.Row(
#                 [
#                     dbc.Col(
#                         [
#                             html.Label("Mode"),
#                             dcc.Dropdown(
#                                 id="pf-mode",
#                                 options=[{"label": m.title(), "value": m} for m in mode_opts] or [
#                                     {"label": "Both", "value": "both"}
#                                 ],
#                                 placeholder="Select mode",
#                                 clearable=True,
#                             ),
#                         ],
#                         lg=3,
#                     ),
#                     dbc.Col(
#                         [
#                             html.Label("Facility type"),
#                             dcc.Dropdown(id="pf-facility", placeholder="Select facility type", clearable=True),
#                         ],
#                         lg=3,
#                         id="wrap-facility",
#                         style={"display": "none"},
#                     ),
#                     dbc.Col(
#                         [
#                             html.Label("Data source"),
#                             dcc.Dropdown(id="pf-source", placeholder="Select data source", clearable=True),
#                         ],
#                         lg=3,
#                         id="wrap-source",
#                         style={"display": "none"},
#                     ),
#                     dbc.Col(
#                         [
#                             html.Label("Duration"),
#                             dcc.Dropdown(
#                                 id="pf-duration",
#                                 options=[
#                                     {"label": "Short-term", "value": "short"},
#                                     {"label": "Long-term", "value": "long"},
#                                 ],
#                                 placeholder="Select duration",
#                                 clearable=True,
#                             ),
#                         ],
#                         lg=3,
#                         id="wrap-duration",
#                         style={"display": "none"},
#                     ),
#                 ],
#                 className="g-2",
#             ),
#             html.Div(
#                 [
#                     html.Label("Date range"),
#                     dcc.DatePickerRange(
#                         id="pf-dates",
#                         min_date_allowed=min_date,
#                         max_date_allowed=max_date,
#                         start_date=min_date,
#                         end_date=max_date,
#                         display_format="YYYY-MM-DD",
#                     ),
#                 ],
#                 id="wrap-dates",
#                 style={"display": "none", "marginTop": "10px"},
#             ),
#             html.Div(
#                 [
#                     html.Button("Download CSV", id="pf-download-btn", className="btn btn-outline-primary"),
#                     dcc.Download(id="pf-download"),
#                 ],
#                 id="wrap-download",
#                 style={"display": "none", "marginTop": "10px"},
#             ),
#         ],
#         class_name="mb-3",
#     )

#     results_block = card(
#         [
#             dash_table.DataTable(
#                 id="pf-table",
#                 columns=[
#                     {"name": "Location", "id": "Location"},
#                     {"name": "Mode", "id": "Mode"},
#                     {"name": "Facility type", "id": "Facility type"},
#                     {"name": "Source", "id": "Source"},
#                     {"name": "Source type", "id": "Source type"},
#                     {"name": "Duration", "id": "Duration"},
#                     {"name": "Start date", "id": "Start date"},
#                     {"name": "End date", "id": "End date"},
#                     {"name": "Total counts", "id": "Total counts", "type": "numeric"},
#                     {"name": "Avg hourly", "id": "Avg hourly", "type": "numeric"},
#                     {"name": "View", "id": "View", "presentation": "markdown"},
#                 ],
#                 data=[],
#                 markdown_options={"html": True, "link_target": "_self"},
#                 page_size=20,
#                 sort_action="native",
#                 style_table={"overflowX": "auto"},
#                 style_as_list_view=True,
#                 style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
#                 style_cell={"textAlign": "center", "padding": "8px"},
#                 style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
#             )
#         ],
#         class_name="mb-3",
#     )

#     app.layout = dash_page(
#         "Explore (Unified)",
#         [
#             filter_block,
#             html.Div(id="wrap-results", children=[results_block], style={"display": "none"}),
#         ],
#     )

#     # ───────────────────────────── Callbacks (Progressive) ────────────────────

#     # Step 2: Facility options + reveal
#     @app.callback(
#         Output("pf-facility", "options"),
#         Output("wrap-facility", "style"),
#         Output("pf-facility", "value"),
#         Input("pf-mode", "value"),
#         prevent_initial_call=True,
#     )
#     def step_facility(mode):
#         if not mode:
#             return [], {"display": "none"}, None
#         df = BASE_DF if 'BASE_DF' in locals() else _load_unified_summary()
#         opts = sorted(df.loc[df["Mode"] == mode, "Facility type"].dropna().unique().tolist())
#         return [{"label": o.replace("_", " ").title(), "value": o} for o in opts], {"display": "block"}, None

#     # Step 3: Source options + reveal
#     @app.callback(
#         Output("pf-source", "options"),
#         Output("wrap-source", "style"),
#         Output("pf-source", "value"),
#         Input("pf-mode", "value"),
#         Input("pf-facility", "value"),
#         prevent_initial_call=True,
#     )
#     def step_source(mode, facility):
#         if not (mode and facility):
#             return [], {"display": "none"}, None
#         df = BASE_DF if 'BASE_DF' in locals() else _load_unified_summary()
#         subset = df[(df["Mode"] == mode) & (df["Facility type"] == facility)]
#         opts = sorted(subset["Source"].dropna().unique().tolist())
#         return [{"label": o, "value": o} for o in opts], {"display": "block"}, None

#     # Step 4: Duration reveal (options are static; we just show the control now)
#     @app.callback(
#         Output("wrap-duration", "style"),
#         Output("pf-duration", "value"),
#         Output("wrap-dates", "style"),
#         Output("wrap-download", "style"),
#         Input("pf-source", "value"),
#         prevent_initial_call=True,
#     )
#     def step_duration(source):
#         if not source:
#             return {"display": "none"}, None, {"display": "none"}, {"display": "none"}
#         return {"display": "block"}, None, {"display": "block"}, {"display": "block"}

#     # Render results table (after all 4 are chosen) + show results
#     @app.callback(
#         Output("pf-table", "data"),
#         Output("wrap-results", "style"),
#         Input("pf-mode", "value"),
#         Input("pf-facility", "value"),
#         Input("pf-source", "value"),
#         Input("pf-duration", "value"),
#         Input("pf-dates", "start_date"),
#         Input("pf-dates", "end_date"),
#         prevent_initial_call=True,
#     )
#     def render_table(mode, facility, source, duration, start_date, end_date):
#         if not (mode and facility and source and duration):
#             return [], {"display": "none"}
#         df = _load_unified_summary()  # grab fresh union (in case DB changed)
#         mask = (
#             (df["Mode"] == mode)
#             & (df["Facility type"] == facility)
#             & (df["Source"] == source)
#             & (df["Duration"].str.lower() == duration.lower())
#         )
#         # Date overlap: keep rows where [Start..End] intersects [start_date..end_date]
#         if start_date:
#             mask &= pd.to_datetime(df["End date"]) >= pd.to_datetime(start_date)
#         if end_date:
#             mask &= pd.to_datetime(df["Start date"]) <= pd.to_datetime(end_date)

#         out = df.loc[mask].copy()
#         out.sort_values(["Location", "Source"], kind="stable", inplace=True)
#         return out.to_dict("records"), {"display": "block"}

#     # Download current table
#     @app.callback(
#         Output("pf-download", "data"),
#         Input("pf-download-btn", "n_clicks"),
#         State("pf-table", "data"),
#         prevent_initial_call=True,
#     )
#     def download_csv(n, rows):
#         df = pd.DataFrame(rows or [])
#         if df.empty:
#             df = pd.DataFrame(columns=UNIFIED_COLUMNS)
#         buf = io.StringIO()
#         df.to_csv(buf, index=False)
#         buf.seek(0)
#         return dict(content=buf.read(), filename="explore_filtered.csv", type="text/csv")

#     return app
# --------------------------------------------------------------------------------------------------------------


# unified_explore.py
from __future__ import annotations

import io
import pandas as pd
from sqlalchemy import create_engine

import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc

from theme import card, dash_page

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

UNIFIED_COLUMNS = [
    "Location",
    "Mode",
    "Facility type",
    "Source",
    "Counts",
    "Duration (number of days)",
    "View",
]

def _query_unified(where: str = "", params: dict | None = None) -> pd.DataFrame:
    sql = """
      SELECT
        "Location",
        "Mode",
        "Facility type",
        "Source",
        "Source type",
        "Duration",
        "Start date",
        "End date",
        "Total counts",
        "Avg hourly",
        "ViewHref"
      FROM unified_site_summary
    """
    if where:
        sql += f" WHERE {where}"
    df = pd.read_sql(sql, ENGINE, params=params or {})
    return df


def _format_unified_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)

    formatted = df.copy()

    counts = pd.to_numeric(formatted["Total counts"], errors="coerce")
    formatted["Counts"] = counts.round().astype("Int64")

    start = pd.to_datetime(formatted["Start date"], errors="coerce")
    end = pd.to_datetime(formatted["End date"], errors="coerce")
    duration = (end - start).dt.days + 1
    formatted["Duration (number of days)"] = duration.astype("Int64")

    def _format_view(href: str | float) -> str:
        if not href or not isinstance(href, str) or not href.strip():
            return ""
        return f"[Open]({href})"

    if "ViewHref" in formatted:
        view_values = formatted["ViewHref"].apply(_format_view)
    else:
        view_values = pd.Series(["" for _ in range(len(formatted))])
    formatted["View"] = view_values

    return formatted[UNIFIED_COLUMNS]

def _distinct(col: str, where: str = "", params: dict | None = None) -> list[str]:
    sql = f'SELECT DISTINCT "{col}" AS v FROM unified_site_summary'
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY 1"
    rows = pd.read_sql(sql, ENGINE, params=params or {})
    return [str(v) for v in rows["v"].dropna().tolist()]

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

    # Read global extents
    extent_df = _query_unified()
    min_date = pd.to_datetime(extent_df["Start date"], errors="coerce").min() if not extent_df.empty else None
    max_date = pd.to_datetime(extent_df["End date"], errors="coerce").max() if not extent_df.empty else None
    min_date = min_date.date() if pd.notna(min_date) else None
    max_date = max_date.date() if pd.notna(max_date) else None

    # Layout
    filter_block = card(
        [
            html.H2("Explore Locations (Progressive Filters)"),
            html.P("Choose Mode first; subsequent filters appear once you’ve selected the previous one.", className="app-muted"),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Mode"),
                            dcc.Dropdown(id="pf-mode", placeholder="Select mode", clearable=True),
                        ],
                        lg=3,
                    ),
                    dbc.Col(
                        [
                            html.Label("Facility type"),
                            dcc.Dropdown(id="pf-facility", placeholder="Select facility type", clearable=True),
                        ],
                        lg=3,
                        id="wrap-facility",
                        style={"display": "none"},
                    ),
                    dbc.Col(
                        [
                            html.Label("Data source"),
                            dcc.Dropdown(id="pf-source", placeholder="Select data source", clearable=True),
                        ],
                        lg=3,
                        id="wrap-source",
                        style={"display": "none"},
                    ),
                    dbc.Col(
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
                        lg=3,
                        id="wrap-duration",
                        style={"display": "none"},
                    ),
                ],
                className="g-2",
            ),
            html.Div(
                [
                    html.Label("Date range"),
                    dcc.DatePickerRange(
                        id="pf-dates",
                        min_date_allowed=min_date,
                        max_date_allowed=max_date,
                        start_date=min_date,
                        end_date=max_date,
                        display_format="YYYY-MM-DD",
                    ),
                ],
                id="wrap-dates",
                style={"display": "none", "marginTop": "10px"},
            ),
            html.Div(
                [
                    html.Button("Download CSV", id="pf-download-btn", className="btn btn-outline-primary"),
                    dcc.Download(id="pf-download"),
                ],
                id="wrap-download",
                style={"display": "none", "marginTop": "10px"},
            ),
        ],
        class_name="mb-3",
    )

    results_block = card(
        [
            dash_table.DataTable(
                id="pf-table",
                columns=[
                    {"name": "Location", "id": "Location"},
                    {"name": "Mode", "id": "Mode"},
                    {"name": "Facility type", "id": "Facility type"},
                    {"name": "Source", "id": "Source"},
                    {"name": "Counts", "id": "Counts", "type": "numeric"},
                    {
                        "name": "Duration (number of days)",
                        "id": "Duration (number of days)",
                        "type": "numeric",
                    },
                    {"name": "View", "id": "View", "presentation": "markdown"},
                ],
                data=[],
                markdown_options={"link_target": "_self"},
                page_size=20,
                sort_action="native",
                style_table={"overflowX": "auto"},
                style_as_list_view=True,
                style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
                style_cell={"textAlign": "center", "padding": "8px"},
                style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
            )
        ],
        class_name="mb-3",
    )

    app.layout = dash_page(
        "Explore",
        [
            filter_block,
            html.Div(id="wrap-results", children=[results_block], style={"display": "none"}),
            dcc.Store(id="pf-store-extent"),  # optional future use
        ],
    )

    # ---- INIT: populate Mode choices from DB
    @app.callback(
        Output("pf-mode", "options"),
        Input("pf-mode", "id"),  # dummy to run once
        prevent_initial_call=False,
    )
    def _init_modes(_):
        modes = _distinct("Mode")
        return [{"label": m.title(), "value": m} for m in modes]

    # Step 2: Facility options + reveal
    @app.callback(
        Output("pf-facility", "options"),
        Output("wrap-facility", "style"),
        Output("pf-facility", "value"),
        Input("pf-mode", "value"),
        prevent_initial_call=True,
    )
    def step_facility(mode):
        if not mode:
            return [], {"display": "none"}, None
        facs = _distinct("Facility type", where='"Mode" = %(m)s', params={"m": mode})
        return [{"label": f.replace("_", " ").title(), "value": f} for f in facs], {"display": "block"}, None

    # Step 3: Source options + reveal
    @app.callback(
        Output("pf-source", "options"),
        Output("wrap-source", "style"),
        Output("pf-source", "value"),
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        prevent_initial_call=True,
    )
    def step_source(mode, facility):
        if not (mode and facility):
            return [], {"display": "none"}, None
        srcs = _distinct(
            "Source",
            where='"Mode" = %(m)s AND "Facility type" = %(f)s',
            params={"m": mode, "f": facility},
        )
        return [{"label": s, "value": s} for s in srcs], {"display": "block"}, None

    # Step 4: Duration reveal + show dates/download
    @app.callback(
        Output("wrap-duration", "style"),
        Output("pf-duration", "value"),
        Output("wrap-dates", "style"),
        Output("wrap-download", "style"),
        Input("pf-source", "value"),
        prevent_initial_call=True,
    )
    def step_duration(source):
        if not source:
            return {"display": "none"}, None, {"display": "none"}, {"display": "none"}
        return {"display": "block"}, None, {"display": "block"}, {"display": "block"}

    # Render results table (after all are chosen) + show results
    @app.callback(
        Output("pf-table", "data"),
        Output("wrap-results", "style"),
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        Input("pf-duration", "value"),
        Input("pf-dates", "start_date"),
        Input("pf-dates", "end_date"),
        prevent_initial_call=True,
    )
    def render_table(mode, facility, source, duration, start_date, end_date):
        if not (mode and facility and source and duration):
            return [], {"display": "none"}

        where = (
            '"Mode" = %(m)s AND "Facility type" = %(f)s AND "Source" = %(s)s AND "Duration" = %(d)s'
        )
        params = {"m": mode, "f": facility, "s": source, "d": duration}

        df = _query_unified(where, params)
        if df.empty:
            return [], {"display": "block"}

        # Date overlap: keep rows where [Start..End] intersects selected range
        if start_date:
            df = df[pd.to_datetime(df["End date"]) >= pd.to_datetime(start_date)]
        if end_date:
            df = df[pd.to_datetime(df["Start date"]) <= pd.to_datetime(end_date)]

        df = _format_unified_table(df)
        df = df.sort_values(["Location", "Source"], kind="stable")
        return df.to_dict("records"), {"display": "block"}

    # Download
    @app.callback(
        Output("pf-download", "data"),
        Input("pf-download-btn", "n_clicks"),
        State("pf-table", "data"),
        prevent_initial_call=True,
    )
    def download_csv(n, rows):
        df = pd.DataFrame(rows or [])
        buf = io.StringIO()
        (df if not df.empty else pd.DataFrame(columns=UNIFIED_COLUMNS)).to_csv(buf, index=False)
        buf.seek(0)
        return dict(content=buf.read(), filename="explore_filtered.csv", type="text/csv")

    return app
