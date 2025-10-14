# unified_explore.py
from __future__ import annotations

import io
import time
import random
from datetime import datetime, timedelta, timezone
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine

import requests
import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc

from theme import card, dash_page

DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

# ---- Custom UI source (Milwaukee StoryMap image card path) ----
NEW_SOURCE_NAME = "Annual Average Estimated Counts (Milwaukee County)"
NEW_FACILITY    = "On-Street (Sidewalk/Bike Lane)"
NEW_MODE        = "Both"
STORYMAP_URL    = "https://storymaps.arcgis.com/stories/281bfdde23a7411ca63f84d1fafa2098"

# ---- ArcGIS config for Wisconsin Ped/Bike Database (Statewide) ----
SW_ITEM_ID = "5badd855f3384cb1ab03eb0470a93f20"
SW_CENTER  = "-88.15601552734218,43.07196694820907"
SW_SCALE   = "1155581.108577"
SW_THEME   = "light"
SW_FLAGS   = [
    "bookmarks-enabled",
    "heading-enabled",
    "legend-enabled",
    "information-enabled",
    "share-enabled",
]

# ---- ArcGIS config for SEWRPC Trail User Counts ----
SEWRPC_ITEM_ID = "5e8b05a112b94650a301851d1e1a2261"
SEWRPC_CENTER  = "-88.03767204768742,43.16456958096229"
SEWRPC_SCALE   = "1155581.108577"
SEWRPC_THEME   = "light"
SEWRPC_FLAGS   = [
    "bookmarks-enabled",
    "legend-enabled",
    "information-enabled",
    "share-enabled",
]

# ---- ArcGIS config for Milwaukee AAEC map (NEW_SOURCE_NAME) ----
MKE_AAEC_ITEM_ID = "7ff38f1ef8fa4f43a939a7fdefc06129"
MKE_AAEC_CENTER  = "-87.92059898860481,43.02041024549958"
MKE_AAEC_SCALE   = "288895.277144"
MKE_AAEC_THEME   = "light"
MKE_AAEC_FLAGS   = [
    "bookmarks-enabled",
    "legend-enabled",
    "information-enabled",
    "share-enabled",
]

# ---- Special rows (Intersection) ----
SP_LOCATION     = "W Wells St & N 68th St Intersection"
SP_FACILITY     = "Intersection"
SP_SOURCE       = "Wisconsin Pilot Counting Counts"
SP_DURATION     = ">6months"
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

# ---- ArcGIS embeddable (Pilot map defaults) ----
ARCGIS_EMBED_SCRIPT_SRC = "https://js.arcgis.com/embeddable-components/4.33/arcgis-embeddable-components.esm.js"
ARCGIS_ITEM_ID          = "b1c8cf7f6ace440ea97743ef95e7b1f6"
ARCGIS_PORTAL_URL       = "https://uwm.maps.arcgis.com"
ARCGIS_CENTER           = "-88.29241753108553,43.84041853130462"
ARCGIS_SCALE            = "2311162.217155"
ARCGIS_THEME            = "light"
ARCGIS_BOOKMARKS        = True
ARCGIS_LEGEND           = True
ARCGIS_INFO             = True
ARCGIS_MIN_HEIGHT       = "420px"

# ---- Table display columns ----
DISPLAY_COLUMNS = [
    {"name": "Location", "id": "Location"},
    {"name": "Duration", "id": "Duration"},
    {"name": "Total counts", "id": "Total counts", "type": "numeric"},
    {"name": "Source type", "id": "Source type"},
    {"name": "View", "id": "View", "presentation": "markdown"},
]

# ---- Duration options ----
DURATION_OPTIONS = [
    "0-15hrs",
    "15-48hrs",
    "2days-14days",
    "14days-30days",
    "1month-3months",
    "3months-6months",
    ">6months",
]

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

def _duration_to_window(duration_key: str, now_local: datetime) -> tuple[datetime, datetime]:
    key = (duration_key or "").strip().lower()
    months = 6
    hours = None
    days = None
    if key == "0-15hrs":
        hours = 15
    elif key == "15-48hrs":
        hours = 48
    elif key == "2days-14days":
        days = 14
    elif key == "14days-30days":
        days = 30
    elif key == "1month-3months":
        months = 3
    elif key == "3months-6months":
        months = 6
    elif key == ">6months":
        months = 6
    if hours is not None:
        dt_to_local = now_local
        dt_from_local = now_local - timedelta(hours=hours)
    elif days is not None:
        dt_to_local = now_local
        dt_from_local = now_local - timedelta(days=days)
    else:
        dt_to_local = now_local
        dt_from_local = now_local - timedelta(days=30 * months)
    return dt_from_local, dt_to_local

def _viv_total_for_duration(ids: list[str], duration_key: str) -> int:
    if not (VIV_API_KEY and ids):
        return 0
    now_local = datetime.now(LOCAL_TZ)
    dt_from_local, dt_to_local = _duration_to_window(duration_key, now_local)
    dt_from = dt_from_local.astimezone(timezone.utc)
    dt_to   = dt_to_local.astimezone(timezone.utc)
    return _viv_total_windowed(ids, dt_from, dt_to)

def _build_view_link(row: pd.Series) -> str:
    src = (row.get("Source") or "").strip()
    loc = (row.get("Location") or "").strip()
    if loc == SP2_LOCATION and src == SP2_SOURCE:
        return f"[Open]({SP2_VIEW_ROUTE})"
    if loc == SP_LOCATION and src == SP_SOURCE:
        loc_q = _encode_location_for_href(loc)
        return f"[Open](/vivacity/?location={loc_q})"
    loc_q = _encode_location_for_href(loc)
    if src == "Wisconsin Pilot Counting Counts":
        return f"[Open](/eco/dashboard?location={loc_q})"
    if src == "Off-Street Trail (SEWRPC Trail User Counts)":
        return f"[Open](/trail/dashboard?location={loc_q})"
    return "[Open](https://uwm.edu/ipit/wisconsin-pedestrian-volume-model/)"

def _opts(vals) -> list[dict]:
    uniq = sorted({v for v in vals if isinstance(v, str) and v.strip()})
    return [{"label": v, "value": v} for v in uniq]

# ---------- ArcGIS iframe helper (reusable for multiple sources) ----------
def _arcgis_embedded_map_component(
    container_id: str,
    *,
    item_id: str,
    center: str,
    scale: str,
    theme: str = "light",
    flags: list[str] | None = None,
) -> html.Iframe:
    flags = (flags or [])
    flags_html = " ".join(flags)
    srcdoc = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width,initial-scale=1"/>
    <script type="module" src="{ARCGIS_EMBED_SCRIPT_SRC}"></script>
    <style>
      html,body{{margin:0;padding:0;height:100%;width:100%;background:transparent}}
      #holder{{height:100%;width:100%;display:flex;align-items:stretch;justify-content:stretch}}
      arcgis-embedded-map{{height:100%;width:100%}}
    </style>
  </head>
  <body>
    <div id="holder">
      <arcgis-embedded-map
        item-id="{item_id}"
        theme="{theme}"
        portal-url="{ARCGIS_PORTAL_URL}"
        center="{center}"
        scale="{scale}"
        {flags_html}
      ></arcgis-embedded-map>
    </div>
  </body>
</html>"""
    return html.Iframe(
        id=container_id,
        srcDoc=srcdoc,
        sandbox="allow-scripts allow-same-origin allow-popups allow-forms",
        style={
            "width": "100%",
            "height": "600px",
            "minHeight": ARCGIS_MIN_HEIGHT,
            "border": "0",
            "borderRadius": "10px",
            "boxShadow": "0 1px 4px rgba(0,0,0,0.1)",
            "background": "transparent",
        },
    )

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

    # Description (left under filters)
    desc_block = card([html.Div(id="pf-desc", children=[])], class_name="mb-3")

    # Map (right, top)
    map_card = card([html.Div(id="pf-map", children=[])], class_name="mb-3")

    # Table (right, bottom)
    table_block = card(
        [
            dcc.Loading(
                id="pf-table-loader",
                type="default",
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
        if str(mode).strip().casefold() == NEW_MODE.casefold():
            facilities = list(set(facilities) | {NEW_FACILITY})
        # Add Intersection option for Pilot special rows when mode is Pedestrian or Bicyclist
        if str(mode).strip().casefold() in {"pedestrian", "bicyclist"}:
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
        if (str(mode).strip().casefold() == NEW_MODE.casefold()
            and str(facility).strip().casefold() == NEW_FACILITY.casefold()):
            sources = list(set(sources) | {NEW_SOURCE_NAME})
        # Add Pilot source for Intersection when mode is Pedestrian or Bicyclist
        if (str(mode).strip().casefold() in {"pedestrian", "bicyclist"} and
            str(facility).strip().casefold() == SP_FACILITY.casefold()):
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

    # ---------- Apply filters & toggle visibility ----------
    @app.callback(
        Output("pf-map", "children"),     # 0 map content
        Output("wrap-map", "style"),      # 1 map card visibility
        Output("pf-table", "data"),       # 2 table rows
        Output("wrap-table", "style"),    # 3 table card visibility
        Output("wrap-results", "style"),  # 4 (sentinel) keep as block once ready
        Output("wrap-download", "style"), # 5 download
        Output("pf-desc", "children"),    # 6 description content
        Output("wrap-desc", "style"),     # 7 description visibility
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        Input("pf-duration", "value"),
        prevent_initial_call=False,
    )
    def _apply_filters(mode, facility, source, duration_key):
        # wait until all filters selected
        has_all = all([mode, facility, source, duration_key])
        if not has_all:
            return [], {"display": "none"}, [], {"display": "none"}, {"display": "none"}, {"display": "none"}, [], {"display": "none"}

        df = base_df.copy()
        cf = str.casefold
        df = df[df["Mode"].str.casefold() == cf(str(mode).strip())]
        df = df[df["Facility type"].str.casefold() == cf(str(facility).strip())]
        df = df[df["Source"].str.casefold() == cf(str(source).strip())]
        df = df[df["Duration"].str.casefold() == cf(str(duration_key).strip())]

        # --- Special Intersection rows (additive) ---
        # Trigger for Pilot special rows ONLY when mode is Pedestrian OR Bicyclist (not Both)
        is_special = (
            str(mode or "").strip().casefold() in {"pedestrian", "bicyclist"} and
            str(facility or "").strip().casefold() == SP_FACILITY.casefold() and
            str(source or "").strip().casefold() == SP_SOURCE.casefold()
        )
        if is_special:
            ids = [s.strip() for s in (VIV_IDS_ENV.split(",") if VIV_IDS_ENV else []) if s.strip()]
            total = _viv_total_for_duration(ids, duration_key) if ids else 0
            sp_row = {
                "Location": SP_LOCATION,
                "Duration": str(duration_key),
                "Total counts": int(total),
                "Source type": SP_SOURCE_TYPE,
                "Source": SP_SOURCE,
                "Facility type": SP_FACILITY,
                "Mode": str(mode).strip(),  # use selected mode (Pedestrian/Bicyclist)
            }
            df = pd.concat([df, pd.DataFrame([sp_row])], ignore_index=True)
            sp2_row = {
                "Location": SP2_LOCATION,
                "Duration": "Not available",
                "Total counts": None,
                "Source type": SP2_SOURCE_TYPE,
                "Source": SP2_SOURCE,
                "Facility type": SP2_FACILITY,
                "Mode": str(mode).strip(),  # use selected mode (Pedestrian/Bicyclist)
            }
            df = pd.concat([df, pd.DataFrame([sp2_row])], ignore_index=True)

        # --- Map selection (Pilot OR Statewide OR SEWRPC Trails OR Milwaukee AAEC) ---
        source_val = str(source or "").strip().casefold()
        map_children = []
        map_style = {"display": "none"}

        if source_val == "wisconsin pilot counting counts":
            pilot_flags = []
            if ARCGIS_BOOKMARKS: pilot_flags.append("bookmarks-enabled")
            if ARCGIS_LEGEND:    pilot_flags.append("legend-enabled")
            if ARCGIS_INFO:      pilot_flags.append("information-enabled")
            map_children = _arcgis_embedded_map_component(
                container_id="pilot-map-container",
                item_id=ARCGIS_ITEM_ID,
                center=ARCGIS_CENTER,
                scale=ARCGIS_SCALE,
                theme=ARCGIS_THEME,
                flags=pilot_flags,
            )
            map_style = {"display": "block"}

        elif source_val == "wisconsin ped/bike database (statewide)":
            map_children = _arcgis_embedded_map_component(
                container_id="statewide-map-container",
                item_id=SW_ITEM_ID,
                center=SW_CENTER,
                scale=SW_SCALE,
                theme=SW_THEME,
                flags=SW_FLAGS,
            )
            map_style = {"display": "block"}

        elif source_val in {
            "sewrpc trail user counts",
            "off-street trail (sewrpc trail user counts)",
        }:
            map_children = _arcgis_embedded_map_component(
                container_id="sewrpc-trails-map",
                item_id=SEWRPC_ITEM_ID,
                center=SEWRPC_CENTER,
                scale=SEWRPC_SCALE,
                theme=SEWRPC_THEME,
                flags=SEWRPC_FLAGS,
            )
            map_style = {"display": "block"}

        elif source_val == NEW_SOURCE_NAME.strip().casefold():
            # Embedded ArcGIS map for Milwaukee AAEC (replaces old static image)
            map_children = _arcgis_embedded_map_component(
                container_id="mke-aaec-map",
                item_id=MKE_AAEC_ITEM_ID,
                center=MKE_AAEC_CENTER,
                scale=MKE_AAEC_SCALE,
                theme=MKE_AAEC_THEME,
                flags=MKE_AAEC_FLAGS,
            )
            map_style = {"display": "block"}

        # --- Descriptions by source selection ---
        mode_val = (mode or "").strip().lower()
        fac_val  = (facility or "").strip().lower()

        bicyclist_combo = (
            mode_val == "bicyclist"
            and fac_val == "intersection" and source_val == "wisconsin pilot counting counts"
        ) or (
            mode_val == "bicyclist"
            and fac_val == "on-street (sidewalk/bike lane)"
            and source_val == "wisconsin ped/bike database (statewide)"
        )

        if bicyclist_combo:
            # If it's the statewide on-street combo we show statewide_onstreet;
            # if it's Pilot Intersection, we'll fall through to pilot description below.
            description = _statewide_onstreet_desc() if fac_val != "intersection" else _pilot_counts_desc()

        # ✅ Statewide + Intersection (Bicyclist)
        elif (mode_val == "bicyclist"
              and fac_val == "intersection"
              and source_val == "wisconsin ped/bike database (statewide)"):
            # Reuse the statewide text that references intersectional counts
            description = _statewide_onstreet_desc()

        # ✅ Statewide + Intersection (Pedestrian)
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
            description = []

        desc_style = {"display": "block"} if description else {"display": "none"}

        if df.empty:
            return map_children, map_style, [], {"display": "block"}, {"display": "block"}, {"display": "flex"}, description, desc_style

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        rows = df[[c["id"] for c in DISPLAY_COLUMNS]].to_dict("records")
        return map_children, map_style, rows, {"display": "block"}, {"display": "block"}, {"display": "flex"}, description, desc_style

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

    # Description for Bicyclist + On-Street (sidewalk/bike lane) + Statewide source
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

    # Description for Wisconsin Pilot Counting Counts
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
                        "The counts have been collected via the hyperlink ",
                        html.A(
                            "SEWRPC’s Regional Non-Motorized Count Program",
                            href="https://www.sewrpc.org/Info-and-Data/Non-Motorized-Count-Program",
                            target="_blank",
                            rel="noopener noreferrer",
                        ),
                        ", and this list only contains portion of the counts before 2018, for the use in the project of hyperlink ",
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

    # ---- Download filtered CSV ----
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
