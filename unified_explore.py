# unified_explore.py
from __future__ import annotations

import time
import random
import json
import itertools
from pathlib import Path
from datetime import datetime, timedelta, timezone
import urllib.parse
import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import create_engine

import requests
import dash
import dash_leaflet as dl
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate

from theme import card, dash_page

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

WI_COUNTIES_PATH = Path("assets/data/wi_counties.geojson")
try:
    _WI_COUNTIES = json.loads(WI_COUNTIES_PATH.read_text())
except FileNotFoundError:
    _WI_COUNTIES = {"type": "FeatureCollection", "features": []}

DEFAULT_CENTER = [43.07196694820907, -88.15601552734218]
DEFAULT_ZOOM = 7
HIGHLIGHT_ZOOM = 14
_SOURCE_TYPE_COLORS = [
    "#2563eb",
    "#0f766e",
    "#f97316",
    "#7c3aed",
    "#db2777",
    "#0ea5e9",
    "#ef4444",
]
_COUNTY_STYLE = {
    "color": "#1f2937",
    "weight": 1,
    "fillOpacity": 0,
}
_COUNTY_HOVER_STYLE = {
    "weight": 2,
    "color": "#0ea5e9",
    "fillOpacity": 0.05,
}

# ---- Custom UI source (Milwaukee StoryMap image card path) ----
NEW_SOURCE_NAME = "Annual Average Estimated Counts (Milwaukee County)"
NEW_FACILITY    = "On-Street (Sidewalk/Bike Lane)"
NEW_MODE        = "Both"
STORYMAP_URL    = "https://storymaps.arcgis.com/stories/281bfdde23a7411ca63f84d1fafa2098"
# ---- NEW: Mid-Block crossing (Pedestrian) - Milwaukee County ----
MIDBLOCK_MODE       = "Pedestrian"
MIDBLOCK_FACILITY   = "Mid-Block crossing"
MIDBLOCK_SOURCE     = "Mid-Block pedestrian counts (Milwaukee County)"

# ---- NEW: Trail Crossing Crash Models (Exposure-Based Study) ----
TRAIL_CROSS_MODE            = "Both"
TRAIL_CROSS_FACILITY        = "Trail Crossings"
TRAIL_CROSS_SOURCE          = "Trail Crossing Crash Models (Exposure-Based Study)"

# ---- Special rows (Intersection) ----
SP_LOCATION     = "W Wells St & N 68th St Intersection"
SP_FACILITY     = "Intersection"
SP_SOURCE       = "Wisconsin Pilot Counting Counts"
SP_SOURCE_TYPE  = "Actual"

SP2_LOCATION     = "N Santa Monica Blvd & Silver Spring Drive - Whitefish Bay"
SP2_FACILITY     = "Intersection"
SP2_SOURCE       = "Wisconsin Pilot Counting Counts"
SP2_SOURCE_TYPE  = "Actual"
SP2_VIEW_ROUTE   = "/live/"

# Vivacity API (optional) for special totals
VIV_API_BASE = "https://api.vivacitylabs.com"
VIV_API_KEY  = "e8893g6wfj7muf89s93n6xfu.rltm9dd6bei47gwbibjog20k"
VIV_IDS_ENV  = "54315,54316,54317,54318"
VIV_TIMEOUT  = 30
VIV_RETRIES  = 3
VIV_MAX_HOURS_PER_REQ = 169

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Chicago")
except Exception:
    LOCAL_TZ = timezone.utc

# ---- NEW: Pedestrian + Intersection + AAEC (Wisconsin Statewide) ----
PED_INT_AAEC_STATEWIDE = "Annual Average Estimated Counts (Wisconsin Statewide)"

# ---- Table display columns ----
DISPLAY_COLUMNS = [
    {"name": "Location", "id": "Location"},
    {"name": "Duration", "id": "Duration"},
    {"name": "Total counts", "id": "Total counts", "type": "numeric"},
    {"name": "Source type", "id": "Source type"},
    {"name": "View", "id": "View", "presentation": "markdown"},
]

TABLE_HIDDEN_COLUMNS = ["Latitude", "Longitude"]
TABLE_COLUMNS = DISPLAY_COLUMNS + [
    {"name": "Latitude", "id": "Latitude", "type": "numeric"},
    {"name": "Longitude", "id": "Longitude", "type": "numeric"},
]

UNIFIED_SQL = """
  SELECT
    "Location",
    "Duration",
    "Total counts",
    "Source type",
    "Longitude",
    "Latitude",
    "Source",
    "Facility type",
    "Mode"
  FROM unified_site_summary
"""

def _fetch_all() -> pd.DataFrame:
    try:
        df = pd.read_sql(UNIFIED_SQL, ENGINE)
    except Exception:
        df = pd.DataFrame(
            columns=[
                c["id"] for c in DISPLAY_COLUMNS
            ]
            + ["Source", "Facility type", "Mode", "Longitude", "Latitude"]
        )

    df = df.copy()
    required_cols = [
        c["id"] for c in DISPLAY_COLUMNS
    ] + ["Source", "Facility type", "Mode", "Longitude", "Latitude"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    text_cols = ["Mode", "Facility type", "Source", "Duration", "Location", "Source type"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()

    for coord_col in ("Longitude", "Latitude"):
        if coord_col in df.columns:
            df[coord_col] = pd.to_numeric(df[coord_col], errors="coerce")

    return df

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
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

def _align_hour(dt: datetime, ceil: bool = False) -> datetime:
    dt = dt.astimezone(timezone.utc)
    base = dt.replace(minute=0, second=0, microsecond=0)
    if ceil and dt != base:
        base += timedelta(hours=1)
    return base

def _parse_counts_payload(payload) -> int:
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

def _viv_total_windowed(ids: list[str], dt_from: datetime, dt_to: datetime) -> int:
    if dt_to <= dt_from:
        return 0
    dt_from = _align_hour(dt_from, ceil=False)
    dt_to   = _align_hour(dt_to,   ceil=True)
    grand_total = 0
    cur_from = dt_from
    max_delta = timedelta(hours=VIV_MAX_HOURS_PER_REQ)
    while cur_from < dt_to:
        cur_to = min(cur_from + max_delta, dt_to)
        if cur_to <= cur_from:
            cur_to = cur_from + timedelta(hours=1)
        grand_total += _viv_sum_window(ids, cur_from, cur_to)
        time.sleep(0.15)
        cur_from = cur_to
    return int(grand_total)

# NEW: helper to total last N days (used instead of duration dropdown)
def _viv_total_last_days(ids: list[str], days: int = 7) -> int:
    now_local = datetime.now(LOCAL_TZ)
    dt_to = now_local.astimezone(timezone.utc)
    dt_from = (now_local - timedelta(days=days)).astimezone(timezone.utc)
    return _viv_total_windowed(ids, dt_from, dt_to)

def _build_view_link(row: pd.Series) -> str:
    src = (row.get("Source") or "").strip()
    loc = (row.get("Location") or "").strip()
    if loc == SP2_LOCATION and src == SP2_SOURCE:
        return f"[Open]({SP2_VIEW_ROUTE})"
    if loc == SP_LOCATION and src == SP_SOURCE:
        loc_q = _encode_location_for_href(loc)
        mode = (row.get("Mode") or "").strip()
        if mode:
            mode_q = _encode_location_for_href(mode)
            return f"[Open](/vivacity/?location={loc_q}&mode={mode_q})"
        return f"[Open](/vivacity/?location={loc_q})"
    loc_q = _encode_location_for_href(loc)
    if src == "Wisconsin Pilot Counting Counts":
        return f"[Open](/eco/dashboard?location={loc_q})"
    if src == "Off-Street Trail (SEWRPC Trail User Counts)":
        return f"[Open](/trail/dashboard?location={loc_q})"
    return "[Open](https://uwm.edu/ipit/wisconsin-pedestrian-volume-model/)"


def _parse_markdown_link(value: str | None) -> tuple[str, str] | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not (value.startswith("[") and "](" in value and value.endswith(")")):
        return None
    try:
        label, remainder = value[1:].split("](", 1)
        href = remainder[:-1]
    except ValueError:
        return None
    if not href:
        return None
    return label or "Open", href


def _build_leaflet_layers(df: pd.DataFrame) -> tuple[list, list, bool]:
    layers: list = []
    legend_children: list = []

    counties_layer = dl.GeoJSON(
        id="pf-map-counties",
        data=_WI_COUNTIES,
        options={"style": _COUNTY_STYLE},
        hoverStyle=_COUNTY_HOVER_STYLE,
    )
    layers.append(counties_layer)

    if df is None or df.empty:
        return layers, legend_children, False

    df_points = df.dropna(subset=["Latitude", "Longitude"])
    if df_points.empty:
        return layers, legend_children, False

    df_points = df_points.copy()
    df_points["Latitude"] = pd.to_numeric(df_points["Latitude"], errors="coerce")
    df_points["Longitude"] = pd.to_numeric(df_points["Longitude"], errors="coerce")
    df_points = df_points.dropna(subset=["Latitude", "Longitude"])
    if df_points.empty:
        return layers, legend_children, False

    color_cycle = itertools.cycle(_SOURCE_TYPE_COLORS)
    color_map: dict[str, str] = {}

    def _color_for(source_type: str) -> str:
        key = (source_type or "Unknown").strip() or "Unknown"
        if key not in color_map:
            color_map[key] = next(color_cycle)
        return color_map[key]

    def _fmt_total(value) -> str:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return "—"
        if pd.isna(num):
            return "—"
        return f"{int(round(num)):,}"

    markers = []
    for _, row in df_points.iterrows():
        lat = row.get("Latitude")
        lon = row.get("Longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue

        location = (row.get("Location") or "Unknown location").strip() or "Unknown location"
        duration = (row.get("Duration") or "—").strip() or "—"
        total_counts = _fmt_total(row.get("Total counts"))
        source_type = (row.get("Source type") or "Unknown").strip() or "Unknown"
        source_name = (row.get("Source") or "Unknown").strip() or "Unknown"
        facility = (row.get("Facility type") or "—").strip() or "—"
        mode = (row.get("Mode") or "—").strip() or "—"
        color = _color_for(source_type)

        link_info = _parse_markdown_link(row.get("View"))
        if link_info:
            link_label, link_href = link_info
            link_component = html.A(
                link_label,
                href=link_href,
                target="_blank",
                rel="noopener noreferrer",
                className="pf-map-popup-link",
            )
        else:
            link_component = None

        popup_details = [
            html.Div(
                [
                    html.Span("Duration", className="pf-map-popup-label"),
                    html.Span(duration, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
            html.Div(
                [
                    html.Span("Total counts", className="pf-map-popup-label"),
                    html.Span(total_counts, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
            html.Div(
                [
                    html.Span("Source type", className="pf-map-popup-label"),
                    html.Span(source_type, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
            html.Div(
                [
                    html.Span("Facility", className="pf-map-popup-label"),
                    html.Span(facility, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
            html.Div(
                [
                    html.Span("Mode", className="pf-map-popup-label"),
                    html.Span(mode, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
            html.Div(
                [
                    html.Span("Source", className="pf-map-popup-label"),
                    html.Span(source_name, className="pf-map-popup-value"),
                ],
                className="pf-map-popup-row",
            ),
        ]
        if link_component:
            popup_details.append(
                html.Div(link_component, className="pf-map-popup-actions")
            )

        popup = dl.Popup(
            html.Div(
                [
                    html.H5(location, className="pf-map-popup-title"),
                    *popup_details,
                ],
                className="pf-map-popup",
            ),
            maxWidth=320,
        )

        marker = dl.CircleMarker(
            center=(lat, lon),
            radius=9,
            color=color,
            fill=True,
            fillOpacity=0.85,
            weight=2,
            children=popup,
        )
        markers.append(marker)

    if not markers:
        return layers, legend_children, False

    site_layer = dl.LayerGroup(markers, id="pf-map-sites")
    layers.append(site_layer)

    legend_children = [
        html.Div(
            [
                html.Div("Source type", className="pf-map-legend-title"),
                html.Ul(
                    [
                        html.Li(
                            [
                                html.Span(
                                    className="pf-map-legend-swatch",
                                    style={"backgroundColor": color_map[label]},
                                ),
                                html.Span(label, className="pf-map-legend-label"),
                            ],
                            className="pf-map-legend-item",
                        )
                        for label in color_map
                    ],
                    className="pf-map-legend-list list-unstyled mb-0",
                ),
            ],
            className="pf-map-legend-inner",
        )
    ]

    return layers, legend_children, True


def _build_eco_dashboard_content(df: pd.DataFrame) -> list:
    df_counts = df.copy()
    if "Total counts" not in df_counts.columns or df_counts.empty:
        df_counts = pd.DataFrame(columns=["Location", "Total counts"])
    else:
        df_counts["Total counts"] = pd.to_numeric(df_counts["Total counts"], errors="coerce")
        df_counts = df_counts.dropna(subset=["Total counts"])
        df_counts["Location"] = df_counts["Location"].fillna("Unknown location").astype(str)

    if df_counts.empty:
        peak_day_volume = None
        average_daily = None
        total_volume = None
    else:
        peak_day_volume = float(df_counts["Total counts"].max())
        average_daily = float(df_counts["Total counts"].mean())
        total_volume = float(df_counts["Total counts"].sum())

    per_location = (
        df_counts.groupby("Location", as_index=False)["Total counts"].sum()
        if not df_counts.empty
        else pd.DataFrame(columns=["Location", "Total counts"])
    )
    per_location = per_location.sort_values("Total counts", ascending=False)
    top_locations = per_location.head(5)

    def _fmt_int(value: float | int | None) -> str:
        if value is None or pd.isna(value):
            return "—"
        return f"{int(round(float(value))):,}"

    def _fmt_float(value: float | int | None) -> str:
        if value is None or pd.isna(value):
            return "—"
        return f"{float(value):,.1f}"

    metrics = [
        ("Peak day volume", _fmt_int(peak_day_volume)),
        ("Average daily count", _fmt_float(average_daily)),
        ("Total recorded volume", _fmt_int(total_volume)),
    ]

    metrics_row = dbc.Row(
        [
            dbc.Col(
                html.Div(
                    [
                        html.Div(label, className="app-muted small text-uppercase"),
                        html.Div(value, className="fw-semibold fs-4"),
                    ],
                    className="p-3 bg-light border rounded h-100",
                ),
                xs=12,
                md=4,
            )
            for label, value in metrics
        ],
        className="g-3",
    )

    pie_fig = go.Figure()
    if not per_location.empty:
        pie_fig.add_trace(
            go.Pie(
                labels=per_location["Location"],
                values=per_location["Total counts"],
                hole=0.55,
                sort=False,
                hovertemplate="%{label}: %{value:,}<extra></extra>",
            )
        )
    pie_fig.update_layout(
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=-0.1),
        height=320,
    )
    if per_location.empty:
        pie_fig.add_annotation(
            text="No data available",
            showarrow=False,
            font=dict(color="#94a3b8", size=16),
            x=0.5,
            y=0.5,
        )

    if top_locations.empty:
        top_list_items = [
            html.Li("No locations available", className="app-muted mb-0"),
        ]
    else:
        top_list_items = [
            html.Li(
                [
                    html.Span(row["Location"], className="me-2"),
                    html.Span(_fmt_int(row["Total counts"]), className="fw-semibold"),
                ],
                className="d-flex justify-content-between align-items-center mb-2",
            )
            for _, row in top_locations.iterrows()
        ]

    card_content = [
        html.H3("Pilot Eco-Counter Snapshot", className="mb-3"),
        metrics_row,
        dbc.Row(
            [
                dbc.Col(
                    dcc.Graph(
                        figure=pie_fig,
                        config={"displayModeBar": False},
                        style={"height": "100%"},
                    ),
                    xs=12,
                    lg=7,
                ),
                dbc.Col(
                    [
                        html.H5("Top locations", className="mb-3"),
                        html.Ul(top_list_items, className="list-unstyled mb-0"),
                    ],
                    xs=12,
                    lg=5,
                ),
            ],
            className="g-4 align-items-start",
        ),
    ]

    return [card(card_content, class_name="mb-3")]

def _opts(vals) -> list[dict]:
    uniq = sorted({v for v in vals if isinstance(v, str) and v.strip()})
    return [{"label": v, "value": v} for v in uniq]

# ---------- App ----------
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

    # Left: Filters
    filter_block = card(
        [
            html.H2("Explore Counts"),
            html.P("Pick Mode, then Facility, then Source.", className="app-muted"),

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
            # Duration filter REMOVED
        ],
        class_name="mb-3",
    )

    # Description (left under filters)
    desc_block = card([html.Div(id="pf-desc", children=[])], class_name="mb-3")

    # Map (right, top)
    map_card = card(
        [
            html.Div(
                [
                    dl.Map(
                        id="pf-leaflet-map",
                        center=DEFAULT_CENTER,
                        zoom=DEFAULT_ZOOM,
                        zoomControl=True,
                        style={"height": "600px", "width": "100%", "borderRadius": "12px"},
                        children=[
                            dl.TileLayer(),
                            dl.ScaleControl(position="bottomleft"),
                        ],
                    ),
                    html.Button(
                        "Reset view",
                        id="pf-map-home",
                        n_clicks=0,
                        className="pf-map-home btn btn-light btn-sm",
                        title="Return to statewide view",
                    ),
                    html.Div(id="pf-map-legend", className="pf-map-legend shadow-sm"),
                ],
                id="pf-map",
                className="pf-map-wrapper position-relative",
            )
        ],
        class_name="mb-3",
    )

    eco_wrap = html.Div(id="wrap-eco", children=[], style={"display": "none"})

    # Table (right, bottom)
    table_block = card(
        [
            dcc.Loading(
                id="pf-table-loader",
                type="default",
                children=[
                    dash_table.DataTable(
                        id="pf-table",
                        columns=TABLE_COLUMNS,
                        data=[],
                        markdown_options={"html": True, "link_target": "_self"},
                        page_size=25,
                        sort_action="native",
                        hidden_columns=TABLE_HIDDEN_COLUMNS,
                        style_table={"overflowX": "auto"},
                        style_as_list_view=True,
                        style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "15px"},
                        style_cell={"textAlign": "left", "padding": "8px"},
                        style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
                    )
                ],
            )
        ],
        class_name="mb-3",
    )

    # Layout: Left (Filters + Description) | Right (Map + Table)
    app.layout = dash_page(
        "Explore",
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            filter_block,
                            html.Div(id="wrap-desc", children=[desc_block], style={"display": "none"}),
                        ],
                        lg=4, md=12, className="mb-3",
                    ),
                    dbc.Col(
                        [
                            html.Div(id="wrap-map", children=[map_card], style={"display": "none"}),
                            eco_wrap,
                            dcc.Loading(
                                id="pf-wrap-loader",
                                type="default",
                                children=html.Div(id="wrap-table", children=[table_block], style={"display": "none"}),
                            ),
                        ],
                        lg=8, md=12, className="mb-3",
                    ),
                ],
                className="g-3",
            ),
            # sentinel to keep older show/hide logic happy (no children needed)
            html.Div(id="wrap-results", style={"display": "none"}),
        ],
    )

    # ---------- Progressive options ----------
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

        # Keep existing "Both / On-Street" option
        if str(mode).strip().casefold() == NEW_MODE.casefold():
            facilities = list(set(facilities) | {NEW_FACILITY})

        # Inject Trail Crossings facility for Both mode
        if str(mode).strip().casefold() == TRAIL_CROSS_MODE.casefold():
            facilities = list(set(facilities) | {TRAIL_CROSS_FACILITY})

        # Add Intersection option for Pilot special rows when mode is Pedestrian or Bicyclist
        if str(mode).strip().casefold() in {"pedestrian", "bicyclist"}:
            facilities = list(set(facilities) | {SP_FACILITY})

        # NEW: Add Mid-Block crossing for Pedestrian
        if str(mode).strip().casefold() == MIDBLOCK_MODE.casefold():
            facilities = list(set(facilities) | {MIDBLOCK_FACILITY})

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

        mode_cf = str(mode).strip().casefold()
        facility_cf = str(facility).strip().casefold()

        # Existing custom Milwaukee AAEC for "Both / On-Street"
        if (mode_cf == NEW_MODE.casefold()
            and facility_cf == NEW_FACILITY.casefold()):
            sources = list(set(sources) | {NEW_SOURCE_NAME})

        # Trail Crossing Crash Models (Exposure-Based Study)
        if (mode_cf == TRAIL_CROSS_MODE.casefold()
            and facility_cf == TRAIL_CROSS_FACILITY.casefold()):
            sources = list(set(sources) | {TRAIL_CROSS_SOURCE})

        # Add Pilot source for Intersection when mode is Pedestrian or Bicyclist
        if (mode_cf in {"pedestrian", "bicyclist"} and
            facility_cf == SP_FACILITY.casefold()):
            sources = list(set(sources) | {SP_SOURCE})

        # NEW: Add AAEC (Wisconsin Statewide) for Pedestrian + Intersection
        if (mode_cf == "pedestrian"
            and facility_cf == SP_FACILITY.casefold()):
            sources = list(set(sources) | {PED_INT_AAEC_STATEWIDE})

        # NEW: Add Mid-Block pedestrian counts (Milwaukee County) for Pedestrian + Mid-Block crossing
        if (mode_cf == MIDBLOCK_MODE.casefold()
            and facility_cf == MIDBLOCK_FACILITY.casefold()):
            sources = list(set(sources) | {MIDBLOCK_SOURCE})

        return _opts(sources), {"display": "block"}, None

    # ---------- Apply filters & toggle visibility ----------
    @app.callback(
        Output("pf-leaflet-map", "children"),  # 0 map layers
        Output("pf-map-legend", "children"),   # 1 legend overlay
        Output("wrap-map", "style"),           # 2 map card visibility
        Output("wrap-eco", "children"),        # 3 eco dashboard content
        Output("wrap-eco", "style"),           # 4 eco dashboard visibility
        Output("pf-table", "data"),            # 5 table rows
        Output("wrap-table", "style"),         # 6 table card visibility
        Output("wrap-results", "style"),       # 7 sentinel visibility
        Output("pf-desc", "children"),         # 8 description content
        Output("wrap-desc", "style"),          # 9 description visibility
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        prevent_initial_call=False,
    )
    def _apply_filters(mode, facility, source):
        default_layers, _, _ = _build_leaflet_layers(pd.DataFrame())
        base_map_children = [
            dl.TileLayer(),
            dl.ScaleControl(position="bottomleft"),
            *default_layers,
        ]

        # wait until all filters selected (Duration removed)
        has_all = all([mode, facility, source])
        if not has_all:
            return (
                base_map_children,
                [],
                {"display": "none"},
                [],
                {"display": "none"},
                [],
                {"display": "none"},
                {"display": "none"},
                [],
                {"display": "none"},
            )

        df = base_df.copy()
        cf = str.casefold
        df = df[df["Mode"].str.casefold() == cf(str(mode).strip())]
        df = df[df["Facility type"].str.casefold() == cf(str(facility).strip())]
        df = df[df["Source"].str.casefold() == cf(str(source).strip())]
        # NOTE: No Duration filter here; we take all rows for this combination.

        # --- Special Intersection rows (additive) ---
        # Trigger for Pilot special rows ONLY when mode is Pedestrian OR Bicyclist (not Both)
        is_special = (
            str(mode or "").strip().casefold() in {"pedestrian", "bicyclist"} and
            str(facility or "").strip().casefold() == SP_FACILITY.casefold() and
            str(source or "").strip().casefold() == SP_SOURCE.casefold()
        )
        if is_special:
            ids = [s.strip() for s in (VIV_IDS_ENV.split(",") if VIV_IDS_ENV else []) if s.strip()]
            # fetch last 7 days when hitting the API
            total = _viv_total_last_days(ids, days=7) if ids else 0
            sp_row = {
                "Location": SP_LOCATION,
                "Duration": "Last 7 days",          # Duration field shown for clarity
                "Total counts": int(total),
                "Source type": SP_SOURCE_TYPE,
                "Source": SP_SOURCE,
                "Facility type": SP_FACILITY,
                "Mode": str(mode).strip(),          # use selected mode (Pedestrian/Bicyclist)
            }
            df = pd.concat([df, pd.DataFrame([sp_row])], ignore_index=True)
            sp2_row = {
                "Location": SP2_LOCATION,
                "Duration": "Not available",
                "Total counts": None,
                "Source type": SP2_SOURCE_TYPE,
                "Source": SP2_SOURCE,
                "Facility type": SP2_FACILITY,
                "Mode": str(mode).strip(),
            }
            df = pd.concat([df, pd.DataFrame([sp2_row])], ignore_index=True)

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)

        source_val = str(source or "").strip().casefold()
        map_layers, legend_children, has_points = _build_leaflet_layers(df)
        map_children = [
            dl.TileLayer(),
            dl.ScaleControl(position="bottomleft"),
            *map_layers,
        ]
        map_style = {"display": "block"} if has_points else {"display": "none"}

        eco_children = []
        eco_style = {"display": "none"}
        show_eco_dashboard = (
            str(mode or "").strip().casefold() == "bicyclist"
            and str(facility or "").strip().casefold() == "on-street (sidewalk/bike lane)"
            and source_val == "wisconsin pilot counting counts"
        )
        if show_eco_dashboard:
            eco_children = _build_eco_dashboard_content(df)
            eco_style = {"display": "block"}

        # --- Descriptions by source selection ---
        mode_val = (mode or "").strip().lower()
        fac_val  = (facility or "").strip().lower()

        # Trail Crossing Crash Models (Exposure-Based Study)
        if (
            mode_val == TRAIL_CROSS_MODE.strip().lower()
            and fac_val == TRAIL_CROSS_FACILITY.strip().lower()
            and source_val == TRAIL_CROSS_SOURCE.strip().casefold()
        ):
            description = _trail_crossing_desc()

        # NEW: Pedestrian + Intersection + AAEC (Wisconsin Statewide)
        elif (mode_val == "pedestrian"
            and fac_val == "intersection"
            and source_val == PED_INT_AAEC_STATEWIDE.strip().casefold()):
            description = _ped_int_statewide_aaec_desc()

        elif (
            mode_val == "pedestrian"
            and fac_val == "mid-block crossing"
            and source_val == MIDBLOCK_SOURCE.strip().casefold()
        ):
            description = _midblock_ped_desc()

        else:
            bicyclist_combo = (
                mode_val == "bicyclist"
                and fac_val == "intersection" and source_val == "wisconsin pilot counting counts"
            ) or (
                mode_val == "bicyclist"
                and fac_val == "on-street (sidewalk/bike lane)"
                and source_val == "wisconsin ped/bike database (statewide)"
            )

            if bicyclist_combo:
                description = _statewide_onstreet_desc() if fac_val != "intersection" else _pilot_counts_desc()

            # Statewide + Intersection (Bicyclist)
            elif (mode_val == "bicyclist"
                  and fac_val == "intersection"
                  and source_val == "wisconsin ped/bike database (statewide)"):
                description = _statewide_onstreet_desc()

            # Statewide + Intersection (Pedestrian)
            elif (mode_val == "pedestrian"
                  and fac_val == "intersection"
                  and source_val == "wisconsin ped/bike database (statewide)"):
                description = _ped_statewide_desc()

            elif (mode_val == "pedestrian"
                  and fac_val == "on-street (sidewalk)"
                  and source_val == "wisconsin ped/bike database (statewide)"):
                description = _ped_statewide_desc()
            elif (mode_val == "pedestrian"
                  and fac_val == "intersection"
                  and source_val == "wisconsin pilot counting counts"):
                description = _pilot_counts_desc()
            elif source_val == "wisconsin pilot counting counts":
                description = _pilot_counts_desc()
            elif source_val in {
                "sewrpc trail user counts",
                "off-street trail (sewrpc trail user counts)",
            }:
                description = _sewrpc_trails_desc()
            elif source_val == NEW_SOURCE_NAME.strip().casefold():
                description = _custom_mke_estimated_desc()
            else:
                # No specific description requested for the Mid-Block dataset
                description = []

        desc_style = {"display": "block"} if description else {"display": "none"}

        if df.empty:
            return (
                map_children,
                legend_children,
                map_style,
                eco_children,
                eco_style,
                [],
                {"display": "block"},
                {"display": "block"},
                description,
                desc_style,
            )

        table_cols = [c["id"] for c in DISPLAY_COLUMNS] + TABLE_HIDDEN_COLUMNS
        rows = df[table_cols].to_dict("records")
        return (
            map_children,
            legend_children,
            map_style,
            eco_children,
            eco_style,
            rows,
            {"display": "block"},
            {"display": "block"},
            description,
            desc_style,
        )

    def _trail_crossing_desc():
        return html.Div(
            [
                html.P(
                    "This dataset supports analysis of crash risk at trail–roadway intersections. It includes modeled relationships between trail user crashes (2011–2018) and factors such as trail and roadway volumes, intersection type, and crossing length. Developed using data from 197 crossings in Minneapolis, MN, and Milwaukee, WI, the Poisson-lognormal model highlights how exposure and design characteristics influence trail crossing safety.",
                    className="app-muted",
                    style={"margin": "0 0 0.75rem 0"},
                ),
                html.P(
                    [
                        "Learn more: ",
                        html.A(
                            "https://trid.trb.org/View/1842179",
                            href="https://trid.trb.org/View/1842179",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                    ],
                    className="app-muted",
                    style={"margin": "0"},
                ),
            ]
        )

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

    def _statewide_onstreet_desc():
        return html.Div(
            [
                html.P(
                    [
                        "The estimated counts have been developed by utilizing statewide short-term intersectional pedestrian and bicyclist, as well as long-term trail counts. ",
                        "For information regarding the source of the data, please refer to the project page ",
                        html.A(
                            "Wisconsin Pedestrian and Bicycle Count Database and Expansion Factor Development",
                            href="https://uwm.edu/ipit/projects/wisconsin-pedestrian-and-bicycle-count-database-and-expansion-factor-development/",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        ", as well as the previous foundational work of the statewide modeling: ",
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

    def _ped_statewide_desc():
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

    @app.callback(
        Output("pf-leaflet-map", "center"),
        Output("pf-leaflet-map", "zoom"),
        Input("pf-table", "active_cell"),
        Input("pf-map-home", "n_clicks"),
        Input("pf-leaflet-map", "children"),
        State("pf-table", "derived_viewport_data"),
        prevent_initial_call=True,
    )
    def _sync_map_to_table(active_cell, home_clicks, map_children, viewport_rows):
        ctx = dash.callback_context
        if not getattr(ctx, "triggered", None):
            raise PreventUpdate

        trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]

        if trigger_id == "pf-map-home" or trigger_id == "pf-leaflet-map":
            return DEFAULT_CENTER, DEFAULT_ZOOM

        if trigger_id != "pf-table":
            raise PreventUpdate

        if not active_cell or not viewport_rows:
            raise PreventUpdate

        if active_cell.get("column_id") != "Location":
            raise PreventUpdate

        row_index = active_cell.get("row")
        if row_index is None:
            raise PreventUpdate

        if row_index < 0 or row_index >= len(viewport_rows):
            raise PreventUpdate

        row = viewport_rows[row_index]
        lat = row.get("Latitude")
        lon = row.get("Longitude")
        try:
            lat_val = float(lat)
            lon_val = float(lon)
        except (TypeError, ValueError):
            raise PreventUpdate
        if pd.isna(lat_val) or pd.isna(lon_val):
            raise PreventUpdate

        return [lat_val, lon_val], HIGHLIGHT_ZOOM

    def _pilot_counts_desc():
        return html.Div(
            [
                html.P(
                    [
                        "The Pilot Pedestrian and Bicycle Count Program, led by UWM in collaboration with WisDOT, is a regional effort focused on Southeast Wisconsin to establish the foundation for a future statewide non-motorist counting network. ",
                        "It integrates new and historical data to improve the accuracy and availability of pedestrian and bicycle volume information. ",
                        "Using technologies like Axis radar-video cameras, Viva V2 sensors, and Eco-Counter units, the program gathers data from diverse environments. ",
                        "This dashboard visualizes the results to support safer, more equitable, and sustainable transportation planning across Wisconsin."
                    ],
                    className="app-muted",
                    style={"margin": "0"},
                )
            ]
        )

    def _sewrpc_trails_desc():
        return html.Div(
            [
                html.P(
                    [
                        "The counts have been collected via the ",
                        html.A(
                            "SEWRPC’s Regional Non-Motorized Count Program",
                            href="https://www.sewrpc.org/Info-and-Data/Non-Motorized-Count-Program",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        ", and this list only contains portion of the counts before 2018, for the use in the project of ",
                        html.A(
                            "“Wisconsin Pedestrian and Bicycle Count Database and Expansion Factor Development”",
                            href="https://uwm.edu/ipit/projects/wisconsin-pedestrian-and-bicycle-count-database-and-expansion-factor-development/",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        ". For more information, please refer to the program page listed above.",
                    ],
                    className="app-muted",
                    style={"margin": "0"},
                )
            ]
        )

    def _ped_int_statewide_aaec_desc():
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

    def _midblock_ped_desc():
        return html.Div(
            [
                html.P(
                    [
                        "The Mid-Block pedestrian counts dataset includes exposure counts collected at mid-block crossings in Milwaukee County. ",
                        "For information about the protocol and database that support these counts, please visit ",
                        html.A(
                            "Mid-Block Pedestrian Crossing Exposure Count Protocol and Database",
                            href="https://uwm.edu/ipit/projects/mid-block-pedestrian-crossing-exposure-count-protocol-and-database/",
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

    return app

# Optional: run standalone
if __name__ == "__main__":
    _server = dash.Dash(__name__).server
    _app = create_unified_explore(_server, prefix="/")
    _app.run_server(host="127.0.0.1", port=8068, debug=True)
