# vivacity_app.py
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List
import time, random
import pandas as pd
import requests
from urllib.parse import parse_qs

import dash
from dash import dcc, html, Input, Output, State, dash_table
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc

from datetime import timezone

from theme import card, dash_page

# ── Config ─────────────────────────────────────────────────────
API_BASE = "https://api.vivacitylabs.com"
API_KEY = os.getenv("VIVACITY_API_KEY", "e8893g6wfj7muf89s93n6xfu.rltm9dd6bei47gwbibjog20k")

DEFAULT_CLASSES = ["pedestrian", "cyclist"]
DEFAULT_TIME_BUCKET = "1h"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# UI values -> API class names (keep as-is if your API expects these exact strings)
CLASS_ALIAS = {
    "cyclist": "cyclist",
    "pedestrian": "pedestrian",
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

            if 400 <= resp.status_code < 500:
                # 👇 include response text so we can see Vivacity’s explanation
                raise RuntimeError(
                    f"{resp.status_code} {resp.reason}: {resp.text[:500]}"
                )

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

# --- Helpers to clean/rename "countline" names into concise "Direction" labels ---
def _clean_direction_name(raw: str) -> str:
    """
    Turn names like 'S1_N68St_NorthCrossing_usuomw001' into 'N68St_NorthCrossing'.
    Heuristic: drop the first and last underscore-delimited tokens when there are >=3 tokens.
    """
    s = str(raw or "").strip()
    parts = s.split("_")
    if len(parts) >= 3:
        mid = parts[1:-1]
        if mid:
            return "_".join(mid)
    return s  # fallback: unchanged

def get_countline_metadata() -> pd.DataFrame:
    data = http_get("/countline/metadata").json()
    rows: List[Dict[str, str]] = []
    # Accept both dict and list payload styles
    if isinstance(data, dict):
        for cid, o in data.items():
            if not isinstance(o, dict):
                continue
            nm = o.get("name") or str(cid)
            if o.get("is_speed") or o.get("is_anpr"):
                continue
            rows.append({"countline_id": str(cid), "raw_name": nm})
    elif isinstance(data, list):
        for o in data:
            if not isinstance(o, dict):
                continue
            cid = str(o.get("id") or o.get("countline_id") or o.get("uuid") or "")
            if not cid:
                continue
            nm = o.get("name") or cid
            rows.append({"countline_id": cid, "raw_name": nm})

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Build cleaned label column used as "Direction"
    df["direction_label"] = df["raw_name"].map(_clean_direction_name)
    df = df.sort_values("direction_label", kind="stable").reset_index(drop=True)
    return df


def _iso_z(dt: datetime) -> str:
    # Force UTC & strip microseconds → 'YYYY-MM-DDTHH:MM:SSZ'
    dt_utc = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt_utc.isoformat().replace("+00:00", "Z")

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
        # ✅ single comma-separated parameter instead of repeated keys
        "countline_ids": ",".join(str(cid) for cid in countline_ids),
        "from": _iso_z(t_from),
        "to": _iso_z(t_to),
        "time_bucket": time_bucket,
        "fill_zeros": "true" if fill_zeros else "false",
    }

    if classes:
        # ✅ same idea for classes
        params["classes"] = ",".join(str(c) for c in classes)

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
                        rows.append({
                            "timestamp": ts,
                            "countline_id": str(cid),
                            "cls": str(cls),
                            "count": v,
                        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.groupby(["countline_id", "timestamp", "cls"], as_index=False)["count"].sum()
    df = df.sort_values(["countline_id", "timestamp", "cls"]).reset_index(drop=True)
    return df


def _classes_for_mode(mode_value: str | None) -> List[str] | None:
    mode = str(mode_value or "").strip().lower()
    if not mode:
        return None
    if mode in {"bicyclist", "bicyclists"}:
        return ["cyclist"]
    if mode in {"pedestrian", "pedestrians"}:
        return ["pedestrian"]
    return None

def _classes_from_search(search: str | None) -> List[str] | None:
    if not search:
        return None
    parsed = parse_qs(search.lstrip("?"), keep_blank_values=False)
    modes = parsed.get("mode") or []
    for raw_mode in reversed(modes):
        classes = _classes_for_mode(raw_mode)
        if classes:
            return classes
    return None

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
    app.title = "W Wells St & N 68th St Intersection"

    # Preload meta (optional)
    try:
        PRELOADED_META = get_countline_metadata() if API_KEY else pd.DataFrame()
    except Exception:
        PRELOADED_META = pd.DataFrame()

    # Build mapping: id -> cleaned "Direction" label
    ID_TO_DIRECTION = (
        dict(zip(PRELOADED_META["countline_id"], PRELOADED_META["direction_label"]))
        if not PRELOADED_META.empty else {}
    )

    # Default IDs: ENV wins, otherwise include all IDs from preloaded metadata
    ENV_DEFAULT_IDS = os.environ.get("VIVACITY_DEFAULT_IDS", "").strip()
    if ENV_DEFAULT_IDS:
        DEFAULT_IDS = [s.strip() for s in ENV_DEFAULT_IDS.split(",") if s.strip()]
    else:
        if not PRELOADED_META.empty:
            DEFAULT_IDS = (
                PRELOADED_META["countline_id"].dropna().astype(str).drop_duplicates().tolist()
            )
        else:
            DEFAULT_IDS = []

    # Default time range: last 7 days including today
    now_local = datetime.now(LOCAL_TZ)
    DEFAULT_TO_LOCAL = now_local
    DEFAULT_FROM_LOCAL = now_local - timedelta(days=6)

    # ── Layout: Left (filters) / Right (graph + table)
    app.layout = dash_page(
        "Long Term Counts · API",
        [
            dcc.Location(id="viv-url", refresh=False),
            dcc.Interval(id="viv-init", interval=200, n_intervals=0, max_intervals=1),

            dbc.Row(
                [
                    # LEFT: Preview + Filters
                    dbc.Col(
                        [
                            card(
                                [
                                    html.H3("Intersection Preview", className="mb-3"),
                                    html.Div(
                                        "Image coming soon",
                                        className="vivacity-image-placeholder",
                                    ),
                                ],
                                class_name="vivacity-image-card",
                            ),
                            card(
                                html.Div(
                                    [
                                        html.H3("Filters", className="mb-3"),
                                        html.Label("Direction"),
                                        dcc.Dropdown(
                                            id="viv-countline-dd",
                                            options=[
                                                {"label": row["direction_label"], "value": row["countline_id"]}
                                                for _, row in PRELOADED_META.iterrows()
                                            ],
                                            multi=True,
                                            placeholder=("Select direction(s)"),
                                            value=DEFAULT_IDS,
                                        ),
                                        dcc.Input(
                                            id="viv-manual-ids",
                                            placeholder="Or enter IDs e.g. 54315,54316",
                                            style={"width": "100%"},
                                        ),
                                        html.Hr(),

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
                                            className="vivacity-filters-hint",
                                        ),
                                        html.Div(
                                            [
                                                dcc.Input(
                                                    id="viv-from-time",
                                                    type="text",
                                                    value="00:00",
                                                    placeholder="HH:MM",
                                                    style={"width": 100, "marginRight": 8},
                                                ),
                                                dcc.Input(
                                                    id="viv-to-time",
                                                    type="text",
                                                    value="23:59",
                                                    placeholder="HH:MM",
                                                    style={"width": 100},
                                                ),
                                            ],
                                            className="vivacity-time-range",
                                        ),
                                        html.Hr(),

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
                                        html.Label("Classes", style={"marginTop": 8}),
                                        dcc.Checklist(
                                            id="viv-classes-check",
                                            options=[{"label": c.title(), "value": c} for c in DEFAULT_CLASSES],
                                            value=DEFAULT_CLASSES,
                                            inline=False,
                                            className="vivacity-class-checklist",
                                        ),
                                        html.Button(
                                            "Refresh",
                                            id="viv-refresh-btn",
                                            n_clicks=0,
                                            style={"marginTop": 10, "width": "100%"},
                                        ),
                                    ],
                                    className="vivacity-filters-content",
                                ),
                                class_name="vivacity-filters-card",
                            ),
                        ],
                        width=12,
                        lg=4,
                        xl=3,
                        className="vivacity-side-column",
                    ),

                    # RIGHT: Graph + Table
                    dbc.Col(
                        [
                            card(
                                [
                                    html.H2("W Wells St & N 68th St Intersection", className="mb-2"),
                                    dcc.Graph(id="viv-timeseries-graph", style={"height": 360}),
                                ],
                                class_name="vivacity-graph-card",
                            ),
                            card(
                                [
                                    html.H4("Raw Data"),
                                    html.Div(
                                        [
                                            html.Button(
                                                "Download CSV",
                                                id="viv-download-btn",
                                                n_clicks=0,
                                                style={"marginBottom": 8},
                                            ),
                                            dcc.Download(id="viv-download-raw"),
                                            dcc.Store(id="viv-raw-df-store"),
                                        ]
                                    ),
                                    dash_table.DataTable(
                                        id="viv-data-table",
                                        columns=[
                                            {"name": "Timestamp (Local)", "id": "timestamp"},
                                            {"name": "Direction", "id": "direction"},  # renamed column
                                            {"name": "Class", "id": "cls"},
                                            {"name": "Count", "id": "count"},
                                        ],
                                        data=[],
                                        sort_action="native",
                                        filter_action="native",
                                        page_size=20,
                                        style_table={"overflowX": "auto", "overflowY": "auto", "maxHeight": 320},
                                    ),
                                    html.Div(id="viv-error-box", style={"color": "#b00", "marginTop": 10}),
                                ],
                                class_name="vivacity-table-card",
                            ),
                        ],
                        width=12,
                        lg=8,
                        xl=9,
                        className="vivacity-main-column",
                    ),
                ],
                className="g-3 vivacity-layout-row",
            ),
        ],
    )

    @app.callback(
        Output("viv-classes-check", "value"),
        Input("viv-init", "n_intervals"),
        State("viv-url", "search"),
    )
    def _apply_mode_from_url(n_intervals, search):
        if not n_intervals:
            raise PreventUpdate
        override = _classes_from_search(search)
        if not override:
            raise PreventUpdate
        return override

    # Main query callback
    @app.callback(
        Output("viv-timeseries-graph", "figure"),
        Output("viv-data-table", "data"),
        Output("viv-error-box", "children"),
        Output("viv-raw-df-store", "data"),
        Input("viv-init", "n_intervals"),
        Input("viv-refresh-btn", "n_clicks"),
        State("viv-countline-dd", "value"),
        State("viv-manual-ids", "value"),
        State("viv-date-range", "start_date"),
        State("viv-date-range", "end_date"),
        State("viv-from-time", "value"),
        State("viv-to-time", "value"),
        State("viv-bucket-dd", "value"),
        State("viv-classes-check", "value"),
        State("viv-url", "search"),
    )
    def refresh(
        _init_tick,
        _n,
        dd_vals,
        manual_ids,
        start_date,
        end_date,
        from_time,
        to_time,
        bucket,
        classes,
        url_search,
    ):
        import plotly.express as px

        # Build ID list from dropdown and manual input
        ids: List[str] = []
        if isinstance(dd_vals, list):
            ids.extend([str(v) for v in dd_vals])
        if isinstance(manual_ids, str) and manual_ids.strip():
            ids.extend([s.strip() for s in manual_ids.split(",") if s.strip()])
        ids = sorted(set(ids))

        ctx = getattr(dash, "callback_context", None)
        triggered_id = None
        if ctx and getattr(ctx, "triggered", None):
            try:
                triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
            except (IndexError, KeyError, AttributeError):
                triggered_id = None
        is_initial_trigger = bool(_init_tick) and (
            triggered_id == "viv-init" or (triggered_id is None and not _n)
        )
        if is_initial_trigger:
            override_classes = _classes_from_search(url_search)
            if override_classes:
                classes = override_classes

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
            return dash.no_update, dash.no_update, "Please select at least one Direction.", None

        # Align to bucket and possibly optimize
        dt_from_utc, dt_to_utc = _align_range_to_bucket(dt_from_utc, dt_to_utc, bucket)
        duration = (dt_to_utc - dt_from_utc).total_seconds()
        fill_zeros = True
        if duration > 7 * 24 * 3600 and len(ids) > 3:
            fill_zeros = False
            bucket = "1h" if bucket in {"5m", "15m"} else bucket

        # Map user selection to API class names
        selected = set(classes or [])
        mapped_classes = sorted({CLASS_ALIAS.get(c, c) for c in selected})
        if not mapped_classes:
            mapped_classes = sorted({CLASS_ALIAS.get(c, c) for c in DEFAULT_CLASSES})

        try:
            probe_df = get_countline_counts(
                ids,
                dt_from_utc,
                dt_to_utc,
                time_bucket=bucket,
                classes=mapped_classes,
                fill_zeros=fill_zeros,
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

        # Label map for directions (fallback to id if unknown/manual)
        id_to_dir = ID_TO_DIRECTION

        # Keep only requested/available classes
        available = set(probe_df["cls"].unique())
        keep = list(available & set(mapped_classes)) or list(available)
        df = probe_df[probe_df["cls"].isin(keep)].copy()

        # Add a human-friendly "direction" column for display
        df["direction"] = df["countline_id"].map(id_to_dir).fillna(df["countline_id"])

        # Plotly figure — regroup so each trace is a single sensor direction (optionally per class)
        classes_in_df = sorted(df["cls"].unique())
        multi_class = len(classes_in_df) > 1
        group_cols = ["timestamp", "countline_id"] + (["cls"] if multi_class else [])
        plot_df = df.groupby(group_cols, as_index=False)["count"].sum()
        plot_df["timestamp"] = plot_df["timestamp"].dt.tz_convert(LOCAL_TZ)
        plot_df["direction"] = plot_df["countline_id"].map(id_to_dir).fillna(
            plot_df["countline_id"]
        )

        if multi_class:
            plot_df["series_label"] = plot_df.apply(
                lambda r: f"{r['direction']} – {r['cls']}", axis=1
            )
            legend_title = "Direction & class"
        else:
            plot_df["series_label"] = plot_df["direction"]
            legend_title = "Direction"

        # Ensure legend labels remain unique even if cleaned directions collide
        duplicate_labels = plot_df.duplicated("series_label", keep=False)
        plot_df.loc[duplicate_labels, "series_label"] = plot_df.loc[duplicate_labels].apply(
            lambda r: f"{r['series_label']} ({r['countline_id']})", axis=1
        )
        color_field = "series_label"

        hover_data = {"direction": True}
        hover_data["countline_id"] = True
        if multi_class:
            hover_data["cls"] = True

        fig = px.line(
            plot_df,
            x="timestamp",
            y="count",
            color=color_field,
            hover_data=hover_data,
            title="Counts by approach direction" + (" and class" if multi_class else ""),
            markers=True,
        )
        fig.update_layout(
            legend_title_text=legend_title,
            xaxis_title="Time (local)",
            yaxis_title="Count",
        )

        # Table rows (now uses 'direction' instead of raw id)
        tbl = df[["timestamp", "direction", "cls", "count"]].copy()
        tbl["timestamp"] = tbl["timestamp"].dt.tz_convert(LOCAL_TZ).dt.strftime("%Y-%m-%d %H:%M:%S")
        tbl = tbl.sort_values(["timestamp", "direction", "cls"]).to_dict("records")

        # Store raw df for download (keep both direction + id for CSV users)
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
        # Provide both ID and Direction in the CSV
        cols = ["timestamp", "countline_id", "direction", "cls", "count"]
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.strftime("%Y-%m-%d %H:%M:%S%z")
        return dcc.send_data_frame(df.to_csv, "vivacity_counts.csv", index=False)

    return app
