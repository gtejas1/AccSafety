# unified_explore.py
from __future__ import annotations

import io
import os
import time
import random
from datetime import datetime, timedelta, timezone
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine

import requests  # <-- added
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

# ---- Special row (Intersection) — additive, affects only the exact filter path ----
SP_LOCATION     = "W Wells St & N 68th St Intersection"
SP_MODE         = "Both"
SP_FACILITY     = "Intersection"
SP_SOURCE       = "Wisconsin Pilot Counting Counts"
SP_DURATION     = ">6months"
SP_SOURCE_TYPE  = "Actual"

# Vivacity API (optional) for special row total
VIV_API_BASE = "https://api.vivacitylabs.com"
VIV_API_KEY  = "e8893g6wfj7muf89s93n6xfu.rltm9dd6bei47gwbibjog20k"
VIV_IDS_ENV  = "54315,54316,54317,54318"  # comma-separated ids
VIV_TIMEOUT  = 30
VIV_RETRIES  = 3
VIV_MAX_HOURS_PER_REQ = 169  # documented limit for 1h bucket

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Chicago")
except Exception:
    LOCAL_TZ = timezone.utc

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

def _viv_headers():
    return {"x-vivacity-api-key": VIV_API_KEY} if VIV_API_KEY else {}

def _viv_get(path: str, params: dict | None = None) -> requests.Response:
    url = f"{VIV_API_BASE}{path}"
    last_err = None
    for attempt in range(1, VIV_RETRIES + 1):
        try:
            r = requests.get(url, headers=_viv_headers(), params=params, timeout=VIV_TIMEOUT)
            if r.status_code == 429:
                wait = float(r.headers.get("Retry-After") or (1.5 ** attempt))
                time.sleep(wait + random.uniform(0, 0.5))
                last_err = Exception(f"429: {r.text[:200]}")
                continue
            if r.status_code >= 500:
                time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
                last_err = Exception(f"{r.status_code}: {r.text[:200]}")
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last_err = e
            time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"Vivacity GET failed: {last_err}")

def _viv_iso_z(dt: datetime) -> str:
    # Truncate to whole seconds and format as RFC3339 without fractional seconds
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def _align_hour(dt: datetime, ceil: bool = False) -> datetime:
    """Align to the hour in UTC. If ceil=True, round up to next hour when not already aligned."""
    dt = dt.astimezone(timezone.utc)
    base = dt.replace(minute=0, second=0, microsecond=0)
    if ceil and dt != base:
        base += timedelta(hours=1)
    return base

def _parse_counts_payload(payload) -> int:
    """Sum pedestrian + cyclist across both directions from a single API response payload."""
    total = 0
    if isinstance(payload, dict):
        for _, arr in payload.items():
            if not isinstance(arr, list):
                continue
            for rec in arr:
                for direction in ("clockwise", "anti_clockwise"):
                    d = rec.get(direction) or {}
                    if isinstance(d, dict):
                        for cls in ("pedestrian", "cyclist"):
                            try:
                                total += int(float(d.get(cls, 0) or 0))
                            except Exception:
                                pass
    return int(total)

def _viv_sum_window(ids: list[str], dt_from: datetime, dt_to: datetime) -> int:
    """Fetch and sum counts for a single aligned window [dt_from, dt_to) with time_bucket=1h."""
    params = {
        "countline_ids": ",".join(ids),
        "from": _viv_iso_z(dt_from),
        "to": _viv_iso_z(dt_to),
        "time_bucket": "1h",
        "fill_zeros": "false",
        "classes": "pedestrian,cyclist",
    }
    try:
        payload = _viv_get("/countline/counts", params=params).json()
    except Exception:
        return 0
    return _parse_counts_payload(payload)

def _viv_total_last_n_months(ids: list[str], months: int = 6) -> int:
    """
    Sum pedestrian + cyclist across both directions for the last N months by chunking
    the request into ≤169-hour aligned windows (Vivacity limit for 1h bucket).
    """
    if not (VIV_API_KEY and ids):
        return 0

    now_local = datetime.now(LOCAL_TZ)
    dt_to = now_local.astimezone(timezone.utc)
    dt_from = (now_local - timedelta(days=30 * months)).astimezone(timezone.utc)

    # Align to bucket boundaries for time_bucket=1h (to is exclusive)
    dt_from = _align_hour(dt_from, ceil=False)  # floor to hour
    dt_to   = _align_hour(dt_to,   ceil=True)   # ceil to next hour (exclusive)

    if dt_to <= dt_from:
        dt_to = dt_from + timedelta(hours=1)

    grand_total = 0
    cur_from = dt_from
    max_delta = timedelta(hours=VIV_MAX_HOURS_PER_REQ)

    while cur_from < dt_to:
        cur_to = min(cur_from + max_delta, dt_to)
        # Safety: ensure at least 1h
        if cur_to <= cur_from:
            cur_to = cur_from + timedelta(hours=1)

        # Fetch one window
        grand_total += _viv_sum_window(ids, cur_from, cur_to)

        # Small jitter to be polite with back-to-back requests
        time.sleep(0.15)
        cur_from = cur_to

    return int(grand_total)

def _build_view_link(row: pd.Series) -> str:
    src = (row.get("Source") or "").strip()
    loc = (row.get("Location") or "").strip()
    # Special row routes to Vivacity
    if loc == SP_LOCATION and src == SP_SOURCE:
        loc_q = _encode_location_for_href(loc)
        return f"[Open](/vivacity/?location={loc_q})"
    # Existing behavior (unchanged)
    loc_q = _encode_location_for_href(loc)
    if src == "Wisconsin Pilot Counting Counts":
        return f"[Open](/eco/dashboard?location={loc_q})"
    if src == "Off-Street Trail (SEWRPC Trail User Counts)":
        return f"[Open](/trail/dashboard?location={loc_q})"
    # Statewide modeled (renamed in SQL) and anything else
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

    # Base data (filters applied client-side)
    base_df = _fetch_all()

    # Controls — progressive flow
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

    # Description block (shown conditionally) + Results (hidden initially)
    desc_block = card([html.Div(id="pf-desc", children=[])], class_name="mb-3")

    # Results: map/image placeholder OR table
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
                    dbc.Col(
                        html.Div(id="wrap-results", children=[desc_block, results_block], style={"display": "none"}),
                        lg=8, md=12, className="mb-3",
                    ),
                ],
                className="g-3",
            ),
        ],
    )

    # ── Progressive options / reveal ─────────────────────────────
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
        # Keep existing special Milwaukee path
        if str(mode).strip().casefold() == NEW_MODE.casefold():
            facilities = list(set(facilities) | {NEW_FACILITY})
        # Add Intersection facility only for Mode=Both (additive)
        if str(mode).strip().casefold() == SP_MODE.casefold():
            facilities = list(set(facilities) | {SP_FACILITY})
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
            (base_df["Mode"].str.casefold() == str(mode).strip().casefold()) &
            (base_df["Facility type"].str.casefold() == str(facility).strip().casefold())
        ]
        sources = df["Source"].unique().tolist()
        # Existing special source (Milwaukee estimated)
        if (str(mode).strip().casefold() == NEW_MODE.casefold()
            and str(facility).strip().casefold() == NEW_FACILITY.casefold()):
            sources = list(set(sources) | {NEW_SOURCE_NAME})
        # Add Wisconsin Pilot Counting Counts for Intersection facility (additive)
        if (str(mode).strip().casefold() == SP_MODE.casefold()
            and str(facility).strip().casefold() == SP_FACILITY.casefold()):
            sources = list(set(sources) | {SP_SOURCE})
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

    # ── Apply filters + control table/description/download visibility ──
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
        # only show results when all filters are selected
        has_all = all([mode, facility, source, duration_key])
        if not has_all:
            return [], {"display": "none"}, [], {"display": "none"}, {"display": "none"}, {"display": "none"}, []

        df = base_df.copy()
        cf = str.casefold

        df = df[df["Mode"].str.casefold() == cf(str(mode).strip())]
        df = df[df["Facility type"].str.casefold() == cf(str(facility).strip())]
        df = df[df["Source"].str.casefold() == cf(str(source).strip())]
        df = df[df["Duration"].str.casefold() == cf(str(duration_key).strip())]

        # --- Path 1: custom Milwaukee map (Both + NEW_FACILITY + NEW_SOURCE_NAME) ---
        show_custom = (
            str(mode or "").strip().casefold() == NEW_MODE.casefold()
            and str(facility or "").strip().casefold() == NEW_FACILITY.casefold()
            and str(source or "").strip() == NEW_SOURCE_NAME
        )

        if show_custom:
            # Show clickable title + image; hide table; show custom description; keep download visible per existing behavior
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
                ]
            )

            description = _custom_mke_estimated_desc()
            return map_children, {"display": "block"}, [], {"display": "none"}, {"display": "block"}, {"display": "flex"}, description

        # --- Special Intersection row injection (ONLY when exact filters match) ---
        is_special = (
            str(mode or "").strip().casefold() == SP_MODE.casefold()
            and str(facility or "").strip().casefold() == SP_FACILITY.casefold()
            and str(source or "").strip().casefold() == SP_SOURCE.casefold()
            and str(duration_key or "").strip().casefold() == SP_DURATION.casefold()
        )

        if is_special:
            ids = [s.strip() for s in (VIV_IDS_ENV.split(",") if VIV_IDS_ENV else []) if s.strip()]
            total = _viv_total_last_n_months(ids, months=6) if ids else 0
            sp_row = {
                "Location": SP_LOCATION,
                "Duration": SP_DURATION,
                "Total counts": int(total),
                "Source type": SP_SOURCE_TYPE,
                "Source": SP_SOURCE,
                "Facility type": SP_FACILITY,
                "Mode": SP_MODE,
            }
            # Append the special row without disturbing DB results
            df = pd.concat([df, pd.DataFrame([sp_row])], ignore_index=True)

        # --- Path 2: pedestrian statewide description (only when table has non-empty results) ---
        ped_mode = str(mode or "").strip().lower() == "pedestrian"
        ped_fac  = str(facility or "").strip().lower() == "on-street (sidewalk)"
        ped_src  = str(source or "").strip().lower() == "wisconsin ped/bike database (statewide)"

        if df.empty:
            # Empty table → show panel and allow download (unchanged behavior) but no description
            return [], {"display": "none"}, [], {"display": "block"}, {"display": "block"}, {"display": "flex"}, []

        # Non-empty rows → show table
        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        rows = df[[c["id"] for c in DISPLAY_COLUMNS]].to_dict("records")

        # Show the pedestrian statewide description iff the 3 filters match and rows exist
        description = _ped_statewide_desc() if (ped_mode and ped_fac and ped_src) else []

        return [], {"display": "none"}, rows, {"display": "block"}, {"display": "block"}, {"display": "flex"}, description

    def _custom_mke_estimated_desc():
        # Description for the Milwaukee County estimated counts (custom source)
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

    def _ped_statewide_desc():
        # Description to show for Pedestrian / On-Street (sidewalk/bike lane) / Wisconsin Ped/Bike Database (Statewide)
        return html.Div(
            [
                html.P(
                    [
                        "The estimated counts have been developed by utilizing statewide short-term intersectional pedestrian and long-term trail counts. ",
                        "For information regarding the source of the data, please refer to the project pages ",
                        html.A(
                            "“Pedestrian Exposure Data for the Wisconsin State Highway System: WisDOT Southeast Region Pilot Study”",
                            href="https://uwm.edu/ipit/projects/pedestrian-exposure-data-for-the-wisconsin-state-highway-system-wisdot-southeast-region-pilot-study/",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        " and ",
                        html.A(
                            "“Practical Application of Pedestrian Exposure Tools: Expanding Southeast Region Results Statewide”",
                            href="https://uwm.edu/ipit/projects/practical-application-of-pedestrian-exposure-tools-expanding-southeast-region-results-statewide/",
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
