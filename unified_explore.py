# unified_explore.py
from __future__ import annotations

import time
import random
from datetime import datetime, timedelta, timezone
import urllib.parse
import pandas as pd
import plotly.graph_objects as go
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

# ---- NEW: Mid-Block crossing (Pedestrian) - Milwaukee County map ----
MIDBLOCK_MODE       = "Pedestrian"
MIDBLOCK_FACILITY   = "Mid-Block crossing"
MIDBLOCK_SOURCE     = "Mid-Block pedestrian counts (Milwaukee County)"
MIDBLOCK_ITEM_ID    = "4fb509de628b44ffba4ef5d26c5145d9"
MIDBLOCK_CENTER     = "-87.95344553737603,43.065172443412855"
MIDBLOCK_SCALE      = "288895.277144"
MIDBLOCK_THEME      = "light"
MIDBLOCK_FLAGS      = [
    "legend-enabled",
    "information-enabled",
    "share-enabled",
]

# ---- NEW: Trail Crossing Crash Models (Exposure-Based Study) ----
TRAIL_CROSS_MODE            = "Both"
TRAIL_CROSS_FACILITY        = "Trail Crossings"
TRAIL_CROSS_SOURCE          = "Trail Crossing Crash Models (Exposure-Based Study)"
TRAIL_CROSS_ITEM_ID         = "08541fe9b8e044c2b864f224285087ee"
TRAIL_CROSS_CENTER          = "-88.09723807958986,43.058800525669064"
TRAIL_CROSS_SCALE           = "1155581.1085775"
TRAIL_CROSS_THEME           = "light"
TRAIL_CROSS_FLAGS           = [
    "bookmarks-enabled",
    "legend-enabled",
    "information-enabled",
]
TRAIL_CROSS_PORTAL_URL      = "https://uwm.maps.arcgis.com"
TRAIL_CROSS_SCRIPT_SRC      = "https://js.arcgis.com/4.34/embeddable-components/"

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

# ---- NEW: Pedestrian + Intersection + AAEC (Wisconsin Statewide) ----
PED_INT_AAEC_STATEWIDE = "Annual Average Estimated Counts (Wisconsin Statewide)"
PED_INT_AAEC_EMBED_URL = (
    "https://www.arcgis.com/apps/Embed/index.html"
    "?webmap=1c16b969156844dfb493597bbab5da75"
    "&extent=-87.9534,43.0184,-87.8522,43.0583"
    "&zoom=true&scale=true&legendlayers=true&disable_scroll=true&theme=light"
)

# ---- Table display columns ----
DISPLAY_COLUMNS = [
    {"name": "Location", "id": "Location"},
    {"name": "Duration", "id": "Duration"},
    {"name": "Total counts", "id": "Total counts", "type": "numeric"},
    {"name": "Source type", "id": "Source type"},
    {"name": "View", "id": "View", "presentation": "markdown"},
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


def _trail_crossing_embedded_map(container_id: str = "trail-crossing-map") -> html.Iframe:
    def _attrs_html() -> str:
        base_attrs = [
            ("item-id", TRAIL_CROSS_ITEM_ID),
            ("portal-url", TRAIL_CROSS_PORTAL_URL),
            ("theme", TRAIL_CROSS_THEME),
            ("center", TRAIL_CROSS_CENTER),
            ("scale", TRAIL_CROSS_SCALE),
        ]
        return "".join(
            f"\n        {name}=\"{value}\""
            for name, value in base_attrs
            if value
        )

    def _flags_html() -> str:
        return "".join(f"\n        {flag}" for flag in (TRAIL_CROSS_FLAGS or []))

    srcdoc = f"""<!doctype html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\"/>
    <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"/>
    <script type=\"module\" src=\"{TRAIL_CROSS_SCRIPT_SRC}\"></script>
    <style>
      html,body{{margin:0;padding:0;height:100%;width:100%;background:transparent}}
      #holder{{height:100%;width:100%;display:flex;align-items:stretch;justify-content:stretch}}
      arcgis-embedded-map{{height:100%;width:100%}}
    </style>
  </head>
  <body>
    <div id=\"holder\">
      <arcgis-embedded-map{_attrs_html()}{_flags_html()}>
      </arcgis-embedded-map>
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


DEFAULT_IFRAME_STYLE = {
    "width": "100%",
    "height": "600px",
    "minHeight": ARCGIS_MIN_HEIGHT,
    "border": "0",
    "borderRadius": "10px",
    "boxShadow": "0 1px 4px rgba(0,0,0,0.1)",
    "background": "transparent",
}


SOURCE_EMBEDS: dict[str, dict] = {
    "wisconsin pilot counting counts": {
        "kind": "arcgis",
        "container_id": "pilot-map-container",
        "item_id": ARCGIS_ITEM_ID,
        "center": ARCGIS_CENTER,
        "scale": ARCGIS_SCALE,
        "theme": ARCGIS_THEME,
        "flags": [
            flag
            for flag, enabled in (
                ("bookmarks-enabled", ARCGIS_BOOKMARKS),
                ("legend-enabled", ARCGIS_LEGEND),
                ("information-enabled", ARCGIS_INFO),
            )
            if enabled
        ],
    },
    "wisconsin ped/bike database (statewide)": {
        "kind": "arcgis",
        "container_id": "statewide-map-container",
        "item_id": SW_ITEM_ID,
        "center": SW_CENTER,
        "scale": SW_SCALE,
        "theme": SW_THEME,
        "flags": SW_FLAGS,
    },
    "sewrpc trail user counts": {
        "kind": "arcgis",
        "container_id": "sewrpc-trails-map",
        "item_id": SEWRPC_ITEM_ID,
        "center": SEWRPC_CENTER,
        "scale": SEWRPC_SCALE,
        "theme": SEWRPC_THEME,
        "flags": SEWRPC_FLAGS,
    },
    "off-street trail (sewrpc trail user counts)": {
        "kind": "arcgis",
        "container_id": "sewrpc-trails-map",
        "item_id": SEWRPC_ITEM_ID,
        "center": SEWRPC_CENTER,
        "scale": SEWRPC_SCALE,
        "theme": SEWRPC_THEME,
        "flags": SEWRPC_FLAGS,
    },
    NEW_SOURCE_NAME.strip().casefold(): {
        "kind": "arcgis",
        "container_id": "mke-aaec-map",
        "item_id": MKE_AAEC_ITEM_ID,
        "center": MKE_AAEC_CENTER,
        "scale": MKE_AAEC_SCALE,
        "theme": MKE_AAEC_THEME,
        "flags": MKE_AAEC_FLAGS,
    },
    TRAIL_CROSS_SOURCE.strip().casefold(): {
        "kind": "builder",
        "builder": _trail_crossing_embedded_map,
    },
    PED_INT_AAEC_STATEWIDE.strip().casefold(): {
        "kind": "iframe",
        "container_id": "ped-int-aaec-statewide-map",
        "src": PED_INT_AAEC_EMBED_URL,
    },
    MIDBLOCK_SOURCE.strip().casefold(): {
        "kind": "arcgis",
        "container_id": "midblock-map",
        "item_id": MIDBLOCK_ITEM_ID,
        "center": MIDBLOCK_CENTER,
        "scale": MIDBLOCK_SCALE,
        "theme": MIDBLOCK_THEME,
        "flags": MIDBLOCK_FLAGS,
    },
}


def _source_embed_component(source_val: str | None):
    if not source_val:
        return None
    key = source_val.strip().casefold()
    config = SOURCE_EMBEDS.get(key)
    if not config:
        return None

    kind = config.get("kind")
    if kind == "arcgis":
        return _arcgis_embedded_map_component(
            container_id=config["container_id"],
            item_id=config["item_id"],
            center=config["center"],
            scale=config["scale"],
            theme=config.get("theme", "light"),
            flags=config.get("flags") or [],
        )
    if kind == "iframe":
        return html.Iframe(
            id=config.get("container_id"),
            src=config.get("src"),
            style={**DEFAULT_IFRAME_STYLE, **(config.get("style") or {})},
            sandbox="allow-same-origin allow-scripts allow-popups allow-forms",
        )
    if kind == "builder":
        builder = config.get("builder")
        if callable(builder):
            return builder()
    return None

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
            html.Div(
                [
                    html.H2("Explore Counts", className="heading-with-info__title"),
                    html.Span(
                        [
                            html.Button(
                                "i",
                                id="explore-info-button",
                                className="info-button",
                                title="Show instructions",
                                n_clicks=0,
                                **{"aria-label": "Show instructions", "type": "button"},
                            ),
                            html.Span("Click for quick instructions", className="tooltip-panel", role="tooltip"),
                        ],
                        className="tooltip",
                    ),
                ],
                className="heading-with-info",
            ),
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
    map_card = card([html.Div(id="pf-map", children=[])], class_name="mb-3")

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
            dcc.Location(id="explore-url"),
            dcc.Store(id="explore-intro-state", storage_type="local"),
            html.Div(
                [
                    html.Div(id="explore-intro-backdrop", className="modal-backdrop__overlay", n_clicks=0),
                    html.Div(
                        [
                            html.H2("Getting Started", id="explore-intro-title"),
                            html.P(
                                "Use the Explore Available Datasets button to open the unified data explorer.",
                            ),
                            html.P(
                                "Hover charts and map layers for details; use top filters to refine by Mode, Facility, and Data source.",
                            ),
                            html.P(
                                "Look for “Open” links near sites to jump to analytics or related project pages.",
                            ),
                            html.Div(
                                [
                                    html.Button(
                                        "Got it",
                                        id="explore-dismiss",
                                        className="modal-button primary",
                                        n_clicks=0,
                                    ),
                                    html.Button(
                                        "Dismiss (don’t remember)",
                                        id="explore-dismiss-once",
                                        className="modal-button secondary",
                                        n_clicks=0,
                                    ),
                                ],
                                className="modal-actions",
                            ),
                        ],
                        className="modal",
                        role="document",
                        **{"aria-labelledby": "explore-intro-title"},
                    ),
                ],
                id="explore-intro-modal",
                className="modal-backdrop",
                hidden=True,
                role="dialog",
                **{"aria-modal": "true"},
            ),
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

    @app.callback(
        Output("explore-intro-modal", "hidden"),
        Output("explore-intro-state", "data"),
        Input("explore-url", "search"),
        Input("explore-info-button", "n_clicks"),
        Input("explore-dismiss", "n_clicks"),
        Input("explore-dismiss-once", "n_clicks"),
        Input("explore-intro-backdrop", "n_clicks"),
        State("explore-intro-state", "data"),
        prevent_initial_call=False,
    )
    def _toggle_intro_modal(search, info_clicks, got_it_clicks, dismiss_once_clicks, backdrop_clicks, store_data):
        ctx = dash.callback_context
        trigger = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

        stored = store_data or {}
        dismissed = bool(stored.get("dismissed"))

        params = {}
        if search:
            params = urllib.parse.parse_qs(search.lstrip("?"))
        force_intro = "1" in (params.get("intro") or [])
        reset_intro = "1" in (params.get("reset_intro") or [])

        store_update = dash.no_update
        if reset_intro:
            dismissed = False
            store_update = {"dismissed": False}

        if trigger == "explore-dismiss":
            return True, {"dismissed": True}

        if trigger == "explore-info-button":
            return False, store_update

        if trigger == "explore-dismiss-once":
            return True, store_update

        if trigger == "explore-intro-backdrop":
            return True, store_update

        # Initial load or URL change
        if force_intro:
            return False, store_update

        if dismissed:
            return True, store_update

        return False, store_update

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
        Output("pf-map", "children"),     # 0 map content
        Output("wrap-map", "style"),      # 1 map card visibility
        Output("wrap-eco", "children"),   # 2 eco dashboard content
        Output("wrap-eco", "style"),      # 3 eco dashboard visibility
        Output("pf-table", "data"),       # 4 table rows
        Output("wrap-table", "style"),    # 5 table card visibility
        Output("wrap-results", "style"),  # 6 (sentinel) keep as block once ready
        Output("pf-desc", "children"),    # 7 description content
        Output("wrap-desc", "style"),     # 8 description visibility
        Input("pf-mode", "value"),
        Input("pf-facility", "value"),
        Input("pf-source", "value"),
        prevent_initial_call=False,
    )
    def _apply_filters(mode, facility, source):
        # wait until all filters selected (Duration removed)
        has_all = all([mode, facility, source])
        if not has_all:
            return (
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

        # --- Map selection (Pilot OR Statewide OR SEWRPC Trails OR Milwaukee AAEC OR NEW AAEC Statewide embed OR Mid-Block) ---
        source_val = str(source or "").strip().casefold()
        map_children = []
        map_style = {"display": "none"}

        embed_component = _source_embed_component(source_val)
        if embed_component is not None:
            map_children = embed_component
            map_style = {"display": "block"}

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
                map_style,
                eco_children,
                eco_style,
                [],
                {"display": "block"},
                {"display": "block"},
                description,
                desc_style,
            )

        df = df.copy()
        df["View"] = df.apply(_build_view_link, axis=1)
        rows = df[[c["id"] for c in DISPLAY_COLUMNS]].to_dict("records")
        return (
            map_children,
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
