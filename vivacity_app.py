# vivacity_app.py
# import os
# from datetime import datetime, timedelta, timezone
# from typing import Dict, List, Tuple
# import time, random
# import pandas as pd
# import requests

# import dash
# from dash import dcc, html, Input, Output, State, dash_table
# from dash.exceptions import PreventUpdate

# # ── Config ─────────────────────────────────────────────────────
# API_BASE = "https://api.vivacitylabs.com"
# API_KEY = os.environ.get("VIVACITY_API_KEY", "e8893g6wfj7muf89s93n6xfu.rltm9dd6bei47gwbibjog20k")
# DEFAULT_CLASSES = ["pedestrian", "cyclist"]
# DEFAULT_TIME_BUCKET = "15m"
# REQUEST_TIMEOUT = 30
# MAX_RETRIES = 3
# CLASS_ALIAS = {"bicycle": "pedal_cycle", "bicyclist": "pedal_cycle", "bike": "pedal_cycle", "cyclist": "pedal_cycle"}

# try:
#     from zoneinfo import ZoneInfo
#     LOCAL_TZ = ZoneInfo("America/Chicago")
# except Exception:
#     LOCAL_TZ = timezone.utc

# session = requests.Session()
# session.headers.update({"User-Agent": "vivacity-dash-demo/1.2"})

# def _headers() -> Dict[str, str]:
#     return {"x-vivacity-api-key": API_KEY} if API_KEY else {}

# def http_get(path: str, params: Dict[str, str] | None = None) -> requests.Response:
#     url = f"{API_BASE}{path}"
#     last_err = None
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             resp = session.get(url, headers=_headers(), params=params, timeout=REQUEST_TIMEOUT)
#             if resp.status_code == 429:
#                 ra = resp.headers.get("Retry-After")
#                 wait = float(ra) if ra else (1.5 ** attempt)
#                 time.sleep(wait + random.uniform(0, 0.5))
#                 last_err = Exception(f"429: {resp.text[:300]}")
#                 continue
#             if resp.status_code >= 500:
#                 time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
#                 last_err = Exception(f"{resp.status_code}: {resp.text[:300]}")
#                 continue
#             resp.raise_for_status()
#             return resp
#         except requests.RequestException as e:
#             last_err = e
#             time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
#     raise RuntimeError(f"GET {path} failed after {MAX_RETRIES} retries: {last_err}")

# def _bucket_seconds(bucket: str) -> int:
#     bucket = (bucket or "").strip().lower()
#     if bucket.endswith("m"): return int(bucket[:-1]) * 60
#     if bucket.endswith("h"): return int(bucket[:-1]) * 3600
#     if bucket == "24h":     return 24 * 3600
#     return 15 * 60

# def _floor_to_bucket(dt: datetime, bucket_secs: int) -> datetime:
#     dt_utc = dt.astimezone(timezone.utc)
#     secs = int(dt_utc.timestamp())
#     aligned = secs - (secs % bucket_secs)
#     return datetime.fromtimestamp(aligned, tz=timezone.utc)

# def _ceil_to_bucket(dt: datetime, bucket_secs: int) -> datetime:
#     f = _floor_to_bucket(dt, bucket_secs)
#     if f < dt.astimezone(timezone.utc): return f + timedelta(seconds=bucket_secs)
#     return f

# def _align_range_to_bucket(dt_from_utc: datetime, dt_to_utc: datetime, bucket: str):
#     bsecs = _bucket_seconds(bucket)
#     return _floor_to_bucket(dt_from_utc, bsecs), _ceil_to_bucket(dt_to_utc, bsecs)

# def get_countline_metadata() -> pd.DataFrame:
#     data = http_get("/countline/metadata").json()
#     rows: List[Dict[str, str]] = []
#     if isinstance(data, dict):
#         for cid, o in data.items():
#             if not isinstance(o, dict): continue
#             name = o.get("name") or str(cid)
#             site = o.get("site") if isinstance(o.get("site"), dict) else {}
#             site_name = (site or {}).get("name") or (site or {}).get("short_name")
#             label = f"{site_name} — {name}" if site_name else name
#             if o.get("is_speed") or o.get("is_anpr"): continue
#             rows.append({"countline_id": str(cid), "label": label})
#     elif isinstance(data, list):
#         for o in data:
#             if not isinstance(o, dict): continue
#             cid = str(o.get("id") or o.get("countline_id") or o.get("uuid") or "")
#             if not cid: continue
#             name = o.get("name") or cid
#             rows.append({"countline_id": cid, "label": name})
#     df = pd.DataFrame(rows)
#     if not df.empty:
#         df = df.sort_values("label", kind="stable").reset_index(drop=True)
#     return df

# def _iso_z(dt: datetime) -> str:
#     s = dt.astimezone(timezone.utc).isoformat()
#     return s if s.endswith("Z") else s.replace("+00:00", "Z")

# def get_countline_counts(countline_ids: List[str], t_from: datetime, t_to: datetime,
#                          time_bucket: str = DEFAULT_TIME_BUCKET, classes: List[str] | None = None,
#                          fill_zeros: bool = True) -> pd.DataFrame:
#     if not countline_ids:
#         return pd.DataFrame(columns=["timestamp", "countline_id", "cls", "count"])
#     params = {
#         "countline_ids": ",".join(map(str, countline_ids)),
#         "from": _iso_z(t_from),
#         "to": _iso_z(t_to),
#         "time_bucket": time_bucket,
#         "fill_zeros": str(bool(fill_zeros)).lower(),
#     }
#     if classes:
#         params["classes"] = ",".join(classes)
#     payload = http_get("/countline/counts", params=params).json()
#     rows = []
#     for cid, arr in (payload or {}).items():
#         if not isinstance(arr, list): continue
#         for rec in arr:
#             ts = rec.get("from") or rec.get("to")
#             for direction in ("clockwise", "anti_clockwise"):
#                 d = rec.get(direction) or {}
#                 if isinstance(d, dict):
#                     for cls, val in d.items():
#                         try: v = float(val)
#                         except Exception: v = None
#                         rows.append({"timestamp": ts, "countline_id": str(cid), "cls": str(cls), "count": v})
#     df = pd.DataFrame(rows)
#     if df.empty: return df
#     df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
#     df = df.groupby(["countline_id", "timestamp", "cls"], as_index=False)["count"].sum()
#     df = df.sort_values(["countline_id", "timestamp", "cls"]).reset_index(drop=True)
#     return df

# def create_vivacity_dash(server, prefix="/vivacity/"):
#     app = dash.Dash(
#         name="vivacity_dash",                   # UNIQUE
#         server=server,
#         routes_pathname_prefix=prefix,          # keep these two; remove url_base_pathname
#         requests_pathname_prefix=prefix,
#         suppress_callback_exceptions=True,
#         assets_url_path=f"{prefix.rstrip('/')}/assets"  # UNIQUE
#     )
#     app.title = "Vivacity Simple Dashboard"

#     # Preload meta (optional)
#     try:
#         PRELOADED_META = get_countline_metadata() if API_KEY else pd.DataFrame()
#     except Exception:
#         PRELOADED_META = pd.DataFrame()

#     now_local = datetime.now(LOCAL_TZ)
#     DEFAULT_TO_LOCAL = now_local
#     DEFAULT_FROM_LOCAL = now_local - timedelta(days=1)

#     app.layout = html.Div(
#         [
#             html.H2("Vivacity Simple Dashboard"),
#             html.Div(
#                 [
#                     html.Div(
#                         [
#                             html.Label("API key status:"),
#                             html.Div(
#                                 "Loaded" if API_KEY else "Missing — set VIVACITY_API_KEY",
#                                 id="viv-api-status",
#                                 style={"color": "#0a0" if API_KEY else "#b00"},
#                             ),
#                         ],
#                         style={"marginBottom": 8},
#                     ),
#                     html.Div(
#                         [
#                             html.Label("Countlines"),
#                             dcc.Dropdown(
#                                 id="viv-countline-dd",
#                                 options=[{"label": row["label"], "value": row["countline_id"]}
#                                          for _, row in PRELOADED_META.iterrows()],
#                                 multi=True,
#                                 placeholder=("Select countlines from metadata"),
#                             ),
#                             dcc.Input(id="viv-manual-ids", placeholder="Or enter IDs e.g. 54315,54316",
#                                       style={"width": 320, "marginTop": 6}),
#                         ],
#                         style={"minWidth": 340, "flex": 2, "marginRight": 12},
#                     ),
#                     html.Div(
#                         [
#                             html.Label("From / To (Local time)"),
#                             dcc.DatePickerRange(
#                                 id="viv-date-range",
#                                 start_date=DEFAULT_FROM_LOCAL.date(),
#                                 end_date=DEFAULT_TO_LOCAL.date(),
#                                 display_format="YYYY-MM-DD",
#                                 updatemode="bothdates",
#                             ),
#                             html.Div("Time of day can be adjusted below.", style={"fontSize": 12, "opacity": 0.75, "marginTop": 4}),
#                             html.Div(
#                                 [
#                                     dcc.Input(id="viv-from-time", type="text", value="00:00", placeholder="HH:MM", style={"width": 80, "marginRight": 8}),
#                                     dcc.Input(id="viv-to-time", type="text", value="23:59", placeholder="HH:MM", style={"width": 80}),
#                                 ],
#                                 style={"marginTop": 6},
#                             ),
#                         ],
#                         style={"minWidth": 280, "flex": 2, "marginRight": 12},
#                     ),
#                     html.Div(
#                         [
#                             html.Label("Time bucket"),
#                             dcc.Dropdown(
#                                 id="viv-bucket-dd",
#                                 options=[
#                                     {"label": "5 minutes", "value": "5m"},
#                                     {"label": "15 minutes", "value": "15m"},
#                                     {"label": "1 hour", "value": "1h"},
#                                     {"label": "24 hours", "value": "24h"},
#                                 ],
#                                 value=DEFAULT_TIME_BUCKET,
#                                 clearable=False,
#                             ),
#                             html.Label("Classes"),
#                             dcc.Checklist(
#                                 id="viv-classes-check",
#                                 options=[{"label": c.title(), "value": c} for c in DEFAULT_CLASSES],
#                                 value=DEFAULT_CLASSES,
#                                 inline=True,
#                                 style={"marginTop": 6},
#                             ),
#                             html.Button("Refresh", id="viv-refresh-btn", n_clicks=0, style={"marginTop": 8}),
#                         ],
#                         style={"minWidth": 260, "flex": 1},
#                     ),
#                 ],
#                 style={"display": "flex", "flexWrap": "wrap", "gap": 12, "alignItems": "flex-end", "marginBottom": 12},
#             ),
#             dcc.Graph(id="viv-timeseries-graph"),
#             html.Hr(),
#             html.H4("Raw Data"),
#             html.Div([
#                 html.Button("Download CSV", id="viv-download-btn", n_clicks=0, style={"marginBottom": 8}),
#                 dcc.Download(id="viv-download-raw"),
#                 dcc.Store(id="viv-raw-df-store"),  # holds the raw dataframe from the last query
#             ]),
#             dash_table.DataTable(
#                 id="viv-data-table",
#                 columns=[
#                     {"name": "Timestamp (Local)", "id": "timestamp"},
#                     {"name": "Countline ID", "id": "countline_id"},
#                     {"name": "Class", "id": "cls"},
#                     {"name": "Count", "id": "count"},
#                 ],
#                 data=[],
#                 sort_action="native",
#                 filter_action="native",
#                 page_size=20,
#                 style_table={"overflowX": "auto"},
#             ),
#             html.Div(id="viv-error-box", style={"color": "#b00", "marginTop": 10}),
#         ],
#         style={"padding": 16, "fontFamily": "system-ui, -apple-system, Segoe UI, Roboto, Arial"},
#     )

#     # Callbacks
#     @app.callback(
#         Output("viv-timeseries-graph", "figure"),
#         Output("viv-data-table", "data"),
#         Output("viv-error-box", "children"),
#         Output("viv-raw-df-store", "data"),
#         Input("viv-refresh-btn", "n_clicks"),
#         State("viv-countline-dd", "value"),
#         State("viv-manual-ids", "value"),
#         State("viv-date-range", "start_date"),
#         State("viv-date-range", "end_date"),
#         State("viv-from-time", "value"),
#         State("viv-to-time", "value"),
#         State("viv-bucket-dd", "value"),
#         State("viv-classes-check", "value"),
#         prevent_initial_call=True,
#     )
#     def refresh(_n, dd_vals, manual_ids, start_date, end_date, from_time, to_time, bucket, classes):
#         import plotly.express as px
#         ids: List[str] = []
#         if isinstance(dd_vals, list):
#             ids.extend([str(v) for v in dd_vals])
#         if isinstance(manual_ids, str) and manual_ids.strip():
#             ids.extend([s.strip() for s in manual_ids.split(",") if s.strip()])
#         ids = sorted(set(ids))

#         def parse_hm(s: str):
#             try:
#                 hh, mm = s.split(":"); return int(hh), int(mm)
#             except Exception:
#                 return 0, 0

#         try:
#             sh, sm = parse_hm(from_time or "00:00")
#             eh, em = parse_hm(to_time or "23:59")
#             dt_from_local = datetime.fromisoformat(f"{start_date}T{sh:02d}:{sm:02d}:00").replace(tzinfo=LOCAL_TZ)
#             dt_to_local   = datetime.fromisoformat(f"{end_date}T{eh:02d}:{em:02d}:00").replace(tzinfo=LOCAL_TZ)
#             dt_from_utc, dt_to_utc = dt_from_local.astimezone(timezone.utc), dt_to_local.astimezone(timezone.utc)
#         except Exception as e:
#             return dash.no_update, dash.no_update, f"Failed to parse dates/times: {e}", None

#         if not API_KEY:
#             return dash.no_update, dash.no_update, "Missing API key. Set VIVACITY_API_KEY and reload.", None
#         if not ids:
#             return dash.no_update, dash.no_update, "Please enter/select at least one countline ID.", None

#         dt_from_utc, dt_to_utc = _align_range_to_bucket(dt_from_utc, dt_to_utc, bucket)
#         duration = (dt_to_utc - dt_from_utc).total_seconds()
#         fill_zeros = True
#         if duration > 7*24*3600 and len(ids) > 3:
#             fill_zeros = False
#             bucket = "1h" if bucket in {"5m", "15m"} else bucket

#         try:
#             probe_df = get_countline_counts(ids, dt_from_utc, dt_to_utc, time_bucket=bucket,
#                                             classes=DEFAULT_CLASSES, fill_zeros=fill_zeros)
#         except Exception as e:
#             return dash.no_update, dash.no_update, f"API error: {e}", None

#         if probe_df.empty:
#             fig = {"data": [], "layout": {"title": "No data returned for the selected filters",
#                                           "xaxis": {"title": "Time (local)"}, "yaxis": {"title": "Count"}}}
#             return fig, [], "", None

#         available = set(probe_df["cls"].unique())
#         requested = set((classes or []))
#         mapped = {CLASS_ALIAS.get(c, c) for c in requested}
#         keep = list(available & mapped) or list(available)
#         df = probe_df[probe_df["cls"].isin(keep)].copy()

#         plot_df = df.groupby(["timestamp", "cls"], as_index=False)["count"].sum()
#         plot_df["timestamp"] = plot_df["timestamp"].dt.tz_convert(LOCAL_TZ)
#         fig = px.line(plot_df, x="timestamp", y="count", color="cls", title="Counts by class (sum of directions)", markers=True)
#         fig.update_layout(legend_title_text="Class", xaxis_title="Time (local)", yaxis_title="Count")

#         tbl = df.copy()
#         tbl["timestamp"] = tbl["timestamp"].dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M:%S")
#         tbl = tbl.sort_values(["timestamp", "countline_id", "cls"]).to_dict("records")

#         store_json = df.to_json(date_format="iso", orient="split")
#         return fig, tbl, "", store_json

#     @app.callback(
#         Output("viv-download-raw", "data"),
#         Input("viv-download-btn", "n_clicks"),
#         State("viv-raw-df-store", "data"),
#         prevent_initial_call=True,
#     )
#     def do_download(n_clicks, store_json):
#         if not n_clicks or not store_json:
#             raise PreventUpdate
#         df = pd.read_json(store_json, orient="split")
#         cols = ["timestamp", "countline_id", "cls", "count"]
#         for c in cols:
#             if c not in df.columns:
#                 df[c] = None
#         df = df[cols]
#         df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")
#         return dcc.send_data_frame(df.to_csv, "vivacity_counts.csv", index=False)

#     return app

# vivacity_app.py
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import time, random
import pandas as pd
import requests

import dash
from dash import dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from theme import card, dash_page

# ── Config ─────────────────────────────────────────────────────
API_BASE = "https://api.vivacitylabs.com"
API_KEY = "e8893g6wfj7muf89s93n6xfu.rltm9dd6bei47gwbibjog20k"
DEFAULT_CLASSES = ["pedestrian", "cyclist"]
DEFAULT_TIME_BUCKET = "1h"  # <- 1-hour bucket by default
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
CLASS_ALIAS = {
    "bicycle": "pedal_cycle",
    "bicyclist": "pedal_cycle",
    "bike": "pedal_cycle",
    "cyclist": "pedal_cycle",
}

try:
    from zoneinfo import ZoneInfo
    LOCAL_TZ = ZoneInfo("America/Chicago")
except Exception:
    LOCAL_TZ = timezone.utc

session = requests.Session()
session.headers.update({"User-Agent": "vivacity-dash-demo/1.3"})

def _headers() -> Dict[str, str]:
    return {"x-vivacity-api-key": API_KEY} if API_KEY else {}

def http_get(path: str, params: Dict[str, str] | None = None) -> requests.Response:
    url = f"{API_BASE}{path}"
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, headers=_headers(), params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra else (1.5 ** attempt)
                time.sleep(wait + random.uniform(0, 0.5))
                last_err = Exception(f"429: {resp.text[:300]}")
                continue
            if resp.status_code >= 500:
                time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
                last_err = Exception(f"{resp.status_code}: {resp.text[:300]}")
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            last_err = e
            time.sleep((1.5 ** attempt) + random.uniform(0, 0.5))
    raise RuntimeError(f"GET {path} failed after {MAX_RETRIES} retries: {last_err}")

def _bucket_seconds(bucket: str) -> int:
    bucket = (bucket or "").strip().lower()
    if bucket.endswith("m"):
        return int(bucket[:-1]) * 60
    if bucket.endswith("h"):
        return int(bucket[:-1]) * 3600
    if bucket == "24h":
        return 24 * 3600
    return 15 * 60

def _floor_to_bucket(dt: datetime, bucket_secs: int) -> datetime:
    dt_utc = dt.astimezone(timezone.utc)
    secs = int(dt_utc.timestamp())
    aligned = secs - (secs % bucket_secs)
    return datetime.fromtimestamp(aligned, tz=timezone.utc)

def _ceil_to_bucket(dt: datetime, bucket_secs: int) -> datetime:
    f = _floor_to_bucket(dt, bucket_secs)
    if f < dt.astimezone(timezone.utc):
        return f + timedelta(seconds=bucket_secs)
    return f

def _align_range_to_bucket(dt_from_utc: datetime, dt_to_utc: datetime, bucket: str):
    bsecs = _bucket_seconds(bucket)
    return _floor_to_bucket(dt_from_utc, bsecs), _ceil_to_bucket(dt_to_utc, bsecs)

def get_countline_metadata() -> pd.DataFrame:
    data = http_get("/countline/metadata").json()
    rows: List[Dict[str, str]] = []
    if isinstance(data, dict):
        for cid, o in data.items():
            if not isinstance(o, dict):
                continue
            name = o.get("name") or str(cid)
            site = o.get("site") if isinstance(o.get("site"), dict) else {}
            site_name = (site or {}).get("name") or (site or {}).get("short_name")
            label = f"{site_name} — {name}" if site_name else name
            if o.get("is_speed") or o.get("is_anpr"):
                continue
            rows.append({"countline_id": str(cid), "label": label})
    elif isinstance(data, list):
        for o in data:
            if not isinstance(o, dict):
                continue
            cid = str(o.get("id") or o.get("countline_id") or o.get("uuid") or "")
            if not cid:
                continue
            name = o.get("name") or cid
            rows.append({"countline_id": cid, "label": name})
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("label", kind="stable").reset_index(drop=True)
    return df

def _iso_z(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).isoformat()
    return s if s.endswith("Z") else s.replace("+00:00", "Z")

def get_countline_counts(
    countline_ids: List[str],
    t_from: datetime,
    t_to: datetime,
    time_bucket: str = DEFAULT_TIME_BUCKET,
    classes: List[str] | None = None,
    fill_zeros: bool = True,
) -> pd.DataFrame:
    if not countline_ids:
        return pd.DataFrame(columns=["timestamp", "countline_id", "cls", "count"])
    params = {
        "countline_ids": ",".join(map(str, countline_ids)),
        "from": _iso_z(t_from),
        "to": _iso_z(t_to),
        "time_bucket": time_bucket,
        "fill_zeros": str(bool(fill_zeros)).lower(),
    }
    if classes:
        params["classes"] = ",".join(classes)
    payload = http_get("/countline/counts", params=params).json()
    rows = []
    for cid, arr in (payload or {}).items():
        if not isinstance(arr, list):
            continue
        for rec in arr:
            ts = rec.get("from") or rec.get("to")
            for direction in ("clockwise", "anti_clockwise"):
                d = rec.get(direction) or {}
                if isinstance(d, dict):
                    for cls, val in d.items():
                        try:
                            v = float(val)
                        except Exception:
                            v = None
                        rows.append({"timestamp": ts, "countline_id": str(cid), "cls": str(cls), "count": v})
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.groupby(["countline_id", "timestamp", "cls"], as_index=False)["count"].sum()
    df = df.sort_values(["countline_id", "timestamp", "cls"]).reset_index(drop=True)
    return df

def create_vivacity_dash(server, prefix="/vivacity/"):
    app = dash.Dash(
        name="vivacity_dash",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets",
    )
    app.title = " Vivacity W Wells St & N 68th St Intersection"

    # Preload meta (optional)
    try:
        PRELOADED_META = get_countline_metadata() if API_KEY else pd.DataFrame()
    except Exception:
        PRELOADED_META = pd.DataFrame()

    # Default IDs: ENV wins, otherwise first 2 from metadata (if available)
    ENV_DEFAULT_IDS = os.environ.get("VIVACITY_DEFAULT_IDS", "").strip()
    if ENV_DEFAULT_IDS:
        DEFAULT_IDS = [s.strip() for s in ENV_DEFAULT_IDS.split(",") if s.strip()]
    else:
        DEFAULT_IDS = PRELOADED_META["countline_id"].head(2).tolist() if not PRELOADED_META.empty else []

    # Default time range: last 7 days including today
    now_local = datetime.now(LOCAL_TZ)
    DEFAULT_TO_LOCAL = now_local
    DEFAULT_FROM_LOCAL = now_local - timedelta(days=6)

    app.layout = dash_page(
        "Long Term Counts · Vivacity",
        [
            card(
                [
                    html.H2("W Wells St & N 68th St Intersection"),
                    html.Div(
                        [
                            html.Div(
                                [
                                    html.Label("API key status:"),
                                    html.Div(
                                        "Loaded" if API_KEY else "Missing — set VIVACITY_API_KEY",
                                        id="viv-api-status",
                                        style={"color": "#0a0" if API_KEY else "#b00"},
                                    ),
                                ],
                                style={"marginBottom": 8},
                            ),
                            html.Div(
                                [
                                    html.Label("Countlines"),
                                    dcc.Dropdown(
                                        id="viv-countline-dd",
                                        options=[
                                            {"label": row["label"], "value": row["countline_id"]}
                                            for _, row in PRELOADED_META.iterrows()
                                        ],
                                        multi=True,
                                        placeholder=("Select countlines from metadata"),
                                        value=DEFAULT_IDS,
                                    ),
                                    dcc.Input(
                                        id="viv-manual-ids",
                                        placeholder="Or enter IDs e.g. 54315,54316",
                                        style={"width": 320, "marginTop": 6},
                                    ),
                                ],
                                style={"minWidth": 340, "flex": 2, "marginRight": 12},
                            ),
                            html.Div(
                                [
                                    html.Label("From / To (Local time)"),
                                    dcc.DatePickerRange(
                                        id="viv-date-range",
                                        start_date=DEFAULT_FROM_LOCAL.date(),
                                        end_date=DEFAULT_TO_LOCAL.date(),
                                        display_format="YYYY-MM-DD",
                                        updatemode="bothdates",
                                    ),
                                    html.Div(
                                        "Time of day can be adjusted below.",
                                        style={"fontSize": 12, "opacity": 0.75, "marginTop": 4},
                                    ),
                                    html.Div(
                                        [
                                            dcc.Input(
                                                id="viv-from-time",
                                                type="text",
                                                value="00:00",
                                                placeholder="HH:MM",
                                                style={"width": 80, "marginRight": 8},
                                            ),
                                            dcc.Input(
                                                id="viv-to-time",
                                                type="text",
                                                value="23:59",
                                                placeholder="HH:MM",
                                                style={"width": 80},
                                            ),
                                        ],
                                        style={"marginTop": 6},
                                    ),
                                ],
                                style={"minWidth": 280, "flex": 2, "marginRight": 12},
                            ),
                            html.Div(
                                [
                                    html.Label("Time bucket"),
                                    dcc.Dropdown(
                                        id="viv-bucket-dd",
                                        options=[
                                            {"label": "5 minutes", "value": "5m"},
                                            {"label": "15 minutes", "value": "15m"},
                                            {"label": "1 hour", "value": "1h"},
                                            {"label": "24 hours", "value": "24h"},
                                        ],
                                        value=DEFAULT_TIME_BUCKET,
                                        clearable=False,
                                    ),
                                    html.Label("Classes"),
                                    dcc.Checklist(
                                        id="viv-classes-check",
                                        options=[{"label": c.title(), "value": c} for c in DEFAULT_CLASSES],
                                        value=DEFAULT_CLASSES,
                                        inline=True,
                                        style={"marginTop": 6},
                                    ),
                                    html.Button("Refresh", id="viv-refresh-btn", n_clicks=0, style={"marginTop": 8}),
                                ],
                                style={"minWidth": 260, "flex": 1},
                            ),
                        ],
                        style={
                            "display": "flex",
                            "flexWrap": "wrap",
                            "gap": 12,
                            "alignItems": "flex-end",
                            "marginBottom": 12,
                        },
                    ),
                    dcc.Interval(id="viv-init", interval=200, n_intervals=0, max_intervals=1),
                    dcc.Graph(id="viv-timeseries-graph"),
                ],
                class_name="mb-4",
            ),
            card(
                [
                    html.H4("Raw Data"),
                    html.Div(
                        [
                            html.Button("Download CSV", id="viv-download-btn", n_clicks=0, style={"marginBottom": 8}),
                            dcc.Download(id="viv-download-raw"),
                            dcc.Store(id="viv-raw-df-store"),
                        ]
                    ),
                    dash_table.DataTable(
                        id="viv-data-table",
                        columns=[
                            {"name": "Timestamp (Local)", "id": "timestamp"},
                            {"name": "Countline ID", "id": "countline_id"},
                            {"name": "Class", "id": "cls"},
                            {"name": "Count", "id": "count"},
                        ],
                        data=[],
                        sort_action="native",
                        filter_action="native",
                        page_size=20,
                        style_table={"overflowX": "auto"},
                    ),
                    html.Div(id="viv-error-box", style={"color": "#b00", "marginTop": 10}),
                ]
            ),
        ],
    )

    # Main query callback: runs on first load (viv-init) and on Refresh button.
    @app.callback(
        Output("viv-timeseries-graph", "figure"),
        Output("viv-data-table", "data"),
        Output("viv-error-box", "children"),
        Output("viv-raw-df-store", "data"),
        Input("viv-init", "n_intervals"),         # fires once on page load
        Input("viv-refresh-btn", "n_clicks"),     # manual refresh
        State("viv-countline-dd", "value"),
        State("viv-manual-ids", "value"),
        State("viv-date-range", "start_date"),
        State("viv-date-range", "end_date"),
        State("viv-from-time", "value"),
        State("viv-to-time", "value"),
        State("viv-bucket-dd", "value"),
        State("viv-classes-check", "value"),
    )
    def refresh(_init_tick, _n, dd_vals, manual_ids, start_date, end_date, from_time, to_time, bucket, classes):
        import plotly.express as px

        # Build ID list from dropdown and manual input
        ids: List[str] = []
        if isinstance(dd_vals, list):
            ids.extend([str(v) for v in dd_vals])
        if isinstance(manual_ids, str) and manual_ids.strip():
            ids.extend([s.strip() for s in manual_ids.split(",") if s.strip()])
        ids = sorted(set(ids))

        # Parse times
        def parse_hm(s: str):
            try:
                hh, mm = s.split(":")
                return int(hh), int(mm)
            except Exception:
                return 0, 0

        try:
            sh, sm = parse_hm(from_time or "00:00")
            eh, em = parse_hm(to_time or "23:59")
            dt_from_local = datetime.fromisoformat(f"{start_date}T{sh:02d}:{sm:02d}:00").replace(tzinfo=LOCAL_TZ)
            dt_to_local = datetime.fromisoformat(f"{end_date}T{eh:02d}:{em:02d}:00").replace(tzinfo=LOCAL_TZ)
            dt_from_utc, dt_to_utc = dt_from_local.astimezone(timezone.utc), dt_to_local.astimezone(timezone.utc)
        except Exception as e:
            return dash.no_update, dash.no_update, f"Failed to parse dates/times: {e}", None

        if not API_KEY:
            return dash.no_update, dash.no_update, "Missing API key. Set VIVACITY_API_KEY.", None
        if not ids:
            return dash.no_update, dash.no_update, "Please select at least one countline ID.", None

        # Align to bucket and possibly optimize
        dt_from_utc, dt_to_utc = _align_range_to_bucket(dt_from_utc, dt_to_utc, bucket)
        duration = (dt_to_utc - dt_from_utc).total_seconds()
        fill_zeros = True
        if duration > 7 * 24 * 3600 and len(ids) > 3:
            fill_zeros = False
            bucket = "1h" if bucket in {"5m", "15m"} else bucket

        try:
            probe_df = get_countline_counts(
                ids, dt_from_utc, dt_to_utc, time_bucket=bucket, classes=DEFAULT_CLASSES, fill_zeros=fill_zeros
            )
        except Exception as e:
            return dash.no_update, dash.no_update, f"API error: {e}", None

        if probe_df.empty:
            fig = {
                "data": [],
                "layout": {
                    "title": "No data returned for the selected filters",
                    "xaxis": {"title": "Time (local)"},
                    "yaxis": {"title": "Count"},
                },
            }
            return fig, [], "", None

        # Filter classes to what's available / requested
        available = set(probe_df["cls"].unique())
        requested = set((classes or []))
        mapped = {CLASS_ALIAS.get(c, c) for c in requested}
        keep = list(available & mapped) or list(available)
        df = probe_df[probe_df["cls"].isin(keep)].copy()

        # Plotly figure (sum directions by class)
        plot_df = df.groupby(["timestamp", "cls"], as_index=False)["count"].sum()
        plot_df["timestamp"] = plot_df["timestamp"].dt.tz_convert(LOCAL_TZ)
        fig = px.line(
            plot_df,
            x="timestamp",
            y="count",
            color="cls",
            title="Counts by class (sum of directions)",
            markers=True,
        )
        fig.update_layout(legend_title_text="Class", xaxis_title="Time (local)", yaxis_title="Count")

        # Table rows
        tbl = df.copy()
        tbl["timestamp"] = tbl["timestamp"].dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M:%S")
        tbl = tbl.sort_values(["timestamp", "countline_id", "cls"]).to_dict("records")

        # Store raw df for download
        store_json = df.to_json(date_format="iso", orient="split")
        return fig, tbl, "", store_json

    @app.callback(
        Output("viv-download-raw", "data"),
        Input("viv-download-btn", "n_clicks"),
        State("viv-raw-df-store", "data"),
        prevent_initial_call=True,
    )
    def do_download(n_clicks, store_json):
        if not n_clicks or not store_json:
            raise PreventUpdate
        df = pd.read_json(store_json, orient="split")
        cols = ["timestamp", "countline_id", "cls", "count"]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")
        return dcc.send_data_frame(df.to_csv, "vivacity_counts.csv", index=False)

    return app
