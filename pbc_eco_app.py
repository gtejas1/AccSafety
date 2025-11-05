# pbc_eco_app.py — ECO temporary counts dashboard
import io
import urllib.parse
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine, text
from flask import session as flask_session, request as flask_request, send_file

from theme import card, centered, dash_page

# ---- Config -----------------------------------------------------------------
VALID_USERS = {"admin": "admin", "user1": "mypassword"}
DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

# Updated per your SQL: we only read from the per-mode ECO tables for charts
MODE_TABLE = {
    "Pedestrian": "eco_ped_traffic_data",
    "Bicyclist":  "eco_bike_traffic_data",
    # historically Both existed; we’ll omit it from the UI by default,
    # but keep a mapping in case old links include &mode=Both
    "Both":       "eco_both_traffic_data",
}
MODE_OPTIONS = [{"label": m, "value": m} for m in ["Pedestrian", "Bicyclist"]]  # hide "Both" in UI

def _normalize_mode(mode: Optional[str]) -> str:
    mode = (mode or "").strip()
    if mode in MODE_TABLE:
        return mode
    return "Pedestrian"


def _table_for_mode(mode: str) -> str:
    normalized = _normalize_mode(mode)
    return MODE_TABLE.get(normalized, "")

# --- SQL helpers that USE your new tables/view layout -------------------------
_BOUNDS_SQL = text("""
WITH all_hits AS (
  SELECT MIN(date) AS mn, MAX(date) AS mx FROM eco_ped_traffic_data  WHERE location_name = :loc
  UNION ALL
  SELECT MIN(date) AS mn, MAX(date) AS mx FROM eco_bike_traffic_data WHERE location_name = :loc
  UNION ALL
  -- keep eco_both if still populated; harmless if empty
  SELECT MIN(date) AS mn, MAX(date) AS mx FROM eco_both_traffic_data WHERE location_name = :loc
  UNION ALL
  -- pilot trails table per your new SQL (doesn't render in this app's charts,
  -- but is included to ensure date picker never comes up blank for a location)
  SELECT MIN(date) AS mn, MAX(date) AS mx FROM trail_traffic_data     WHERE location_name = :loc
)
SELECT MIN(mn) AS min_date, MAX(mx) AS max_date FROM all_hits;
""")

def _min_max_any_sql(location: str):
    """Min/Max across ECO per-mode + trails using the new SQL layout."""
    if not location:
        return None, None
    with ENGINE.connect() as con:
        row = con.execute(_BOUNDS_SQL, {"loc": location}).mappings().first()
    if not row or row["min_date"] is None or row["max_date"] is None:
        return None, None
    return pd.to_datetime(row["min_date"]), pd.to_datetime(row["max_date"])

def _min_max_for_mode_sql(location: str, table: str):
    """Min/Max for a specific mode table."""
    if not (location and table):
        return None, None
    q = text(f"SELECT MIN(date) AS mn, MAX(date) AS mx FROM {table} WHERE location_name = :loc")
    with ENGINE.connect() as con:
        row = con.execute(q, {"loc": location}).mappings().first()
    if not row or row["mn"] is None or row["mx"] is None:
        return None, None
    return pd.to_datetime(row["mn"]), pd.to_datetime(row["mx"])

def create_eco_dash(server, prefix="/eco/"):
    app = dash.Dash(
        name="eco_dash",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets",
    )
    app.title = "Temporary Counts - TRCC Statewide Pilot Counting Projects"

    # ---- Summary table (from ECO per-mode tables only) -----------------------
    summary_query = """
        SELECT 'Pedestrian' AS mode, location_name,
               MIN(date) AS start_date, MAX(date) AS end_date,
               SUM(count)::bigint AS total_counts,
               AVG(count)::numeric(12,2) AS average_hourly_count
        FROM eco_ped_traffic_data
        GROUP BY location_name
        UNION ALL
        SELECT 'Bicyclist' AS mode, location_name,
               MIN(date), MAX(date),
               SUM(count)::bigint,
               AVG(count)::numeric(12,2)
        FROM eco_bike_traffic_data
        GROUP BY location_name
        ORDER BY location_name, mode;
    """
    summary_df = pd.read_sql(summary_query, ENGINE)
    if not summary_df.empty:
        summary_df["start_date"] = pd.to_datetime(summary_df["start_date"]).dt.date
        summary_df["end_date"] = pd.to_datetime(summary_df["end_date"]).dt.date
        summary_df["average_hourly_count"] = summary_df["average_hourly_count"].round(0).astype(int)
        summary_df["View"] = summary_df.apply(
            lambda r: f"[View]({prefix}dashboard?location="
                      f"{urllib.parse.quote(str(r['location_name']))}&mode={urllib.parse.quote(str(r['mode']))})",
            axis=1,
        )

    # ---- Pages (login + summary + dashboard) --------------------------------
    login_page_layout = centered(
        card(
            [
                html.H2("Temporary Counts Login", className="text-center mb-3"),
                dcc.Input(id="eco-username", type="text", placeholder="Username", className="form-control mb-2"),
                dcc.Input(id="eco-password", type="password", placeholder="Password", className="form-control mb-2"),
                dbc.Button("Login", id="eco-login-button", color="primary", className="w-100"),
                html.Div(id="eco-login-output", className="text-danger mt-2"),
            ],
            class_name="app-card--narrow",
        )
    )

    summary_layout = card([
        html.H2("Temporary Counts - TRCC Statewide Pilot Counting Projects", style={"textAlign": "center"}),
        dash_table.DataTable(
            id="eco-summary-table",
            columns=[
                {"name": "Location", "id": "location_name"},
                {"name": "Mode", "id": "mode"},
                {"name": "Start Date", "id": "start_date"},
                {"name": "End Date", "id": "end_date"},
                {"name": "Total Counts", "id": "total_counts", "type": "numeric"},
                {"name": "Avg Hourly Count", "id": "average_hourly_count", "type": "numeric"},
                {"name": "View", "id": "View", "presentation": "markdown"},
            ],
            data=summary_df.to_dict("records") if not summary_df.empty else [],
            markdown_options={"html": True, "link_target": "_self"},
            style_as_list_view=True,
            style_cell={"textAlign": "center", "padding": "8px"},
            style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "16px"},
            style_data_conditional=[{"if": {"row_index": "odd"}, "backgroundColor": "rgba(15,23,42,0.03)"}],
            page_size=20,
        ),
    ])

    dashboard_layout = card([
        dbc.Row([dbc.Col(html.H1(id="eco-dashboard-title", className="text-center mb-4"))]),
        # (Back to Summary button removed per request)
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(id="eco-mode", options=MODE_OPTIONS, value="Pedestrian", clearable=False),
                xs=12, md=6, lg=4
            ),
            dbc.Col(dcc.DatePickerRange(id="eco-date-picker", display_format="MM-DD-YYYY"), xs=12, md=6, lg=4),
            dbc.Col(
                html.Div(
                    html.A(
                        "Download Data", id="eco-download-link", href="", target="_blank",
                        className="btn btn-outline-primary float-end",
                    )
                ),
                xs=12, lg=4,
            ),
        ], className="g-3"),
        html.Hr(),
        dbc.Row([dbc.Col(html.H4("Volume Summary"), width=12), dbc.Col(dcc.Graph(id="eco-daily-traffic"), width=12)]),
        dbc.Row([dbc.Col(html.H4("Hourly Volumes"), width=12), dbc.Col(dcc.Graph(id="eco-hourly-traffic"), width=12)]),
        dbc.Row([dbc.Col(html.H4("Average Volume by Day of the Week"), width=12), dbc.Col(dcc.Graph(id="eco-dow-traffic"), width=12)]),
        html.Div(id="eco-note", className="text-muted mt-2"),
    ])

    # ---- Top-level layout ----------------------------------------------------
    app.layout = dash_page(
        "Short Term Counts · Pilot Projects",
        [
            dcc.Location(id="eco-url", refresh=False),
            html.Div(id="eco-page-content"),
        ],
    )

    app.validation_layout = html.Div([app.layout, login_page_layout, summary_layout, dashboard_layout])

    # ---- Download endpoint ----------------------------------------------------
    def _eco_download():
        location = flask_request.args.get("location")
        mode = flask_request.args.get("mode", "Pedestrian")
        table = _table_for_mode(mode)
        if not location or not table:
            return "Location or mode not specified", 400
        q = text(f"""
            SELECT location_name, date, direction, count
            FROM {table}
            WHERE location_name = :loc
            ORDER BY date
        """)
        with ENGINE.connect() as con:
            df = pd.read_sql(q, con, params={"loc": location})
        buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
        fname = f"{location}_{mode}_traffic_data.csv"
        return send_file(io.BytesIO(buf.read().encode()), mimetype="text/csv",
                         as_attachment=True, download_name=fname)
    server.add_url_rule(f"{prefix}download", endpoint="eco_download", view_func=_eco_download)

    # ---- Routing --------------------------------------------------------------
    @app.callback(Output("eco-page-content", "children"),
                  Input("eco-url", "pathname"), Input("eco-url", "search"))
    def eco_display_page(pathname, _search):
        if "user" not in flask_session:
            return login_page_layout
        if (pathname or "").endswith("/dashboard"):
            return dashboard_layout
        return summary_layout

    # ---- Seed dashboard controls from URL (?location=…&mode=…) ---------------
    @app.callback(
        Output("eco-mode", "value"),
        Output("eco-date-picker", "min_date_allowed"),
        Output("eco-date-picker", "max_date_allowed"),
        Output("eco-date-picker", "start_date"),
        Output("eco-date-picker", "end_date"),
        Output("eco-download-link", "href"),
        Output("eco-dashboard-title", "children"),
        Input("eco-url", "search"),
    )
    def eco_set_date_range(search):
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        loc = q.get("location")
        mode = _normalize_mode(q.get("mode"))
        # keep legacy deep links (e.g., "Both") functional; unknown/blank modes default to Pedestrian
        table = _table_for_mode(mode)

        if not loc:
            return dash.no_update, None, None, None, None, "", "Temporary Counts"

        # Try bounds for this mode, else SQL-wide bounds across ECO + trails (your new SQL layout)
        mn, mx = _min_max_for_mode_sql(loc, table)
        if mn is None or mx is None:
            mn, mx = _min_max_any_sql(loc)

        dl = f"{prefix}download?location={urllib.parse.quote(loc)}&mode={urllib.parse.quote(mode)}"
        title = f"{loc} · {mode}"

        if mn is None or mx is None:
            # Location exists in URL but no rows anywhere → leave dates unset
            return mode, None, None, None, None, dl, title

        return mode, mn, mx, mn, mx, dl, title

    # ---- When user changes the Mode dropdown, update the URL -----------------
    @app.callback(
        Output("eco-url", "search"),
        Input("eco-mode", "value"),
        State("eco-url", "search"),
    )
    def eco_update_mode_in_url(mode, search):
        if not mode:
            return dash.no_update
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        normalized = _normalize_mode(mode)
        if q.get("mode") == normalized:
            return dash.no_update
        q["mode"] = normalized
        return "?" + urllib.parse.urlencode(q)

    # ---- Charts ---------------------------------------------------------------
    @app.callback(
        Output("eco-hourly-traffic", "figure"),
        Output("eco-daily-traffic", "figure"),
        Output("eco-dow-traffic", "figure"),
        Output("eco-note", "children"),
        Input("eco-date-picker", "start_date"),
        Input("eco-date-picker", "end_date"),
        Input("eco-url", "search"),
    )
    def eco_update_graphs(start_date, end_date, search):
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        loc = q.get("location")
        mode = _normalize_mode(q.get("mode"))
        table = _table_for_mode(mode)

        # If dates missing (first render), infer from SQL
        if not (start_date and end_date) and loc:
            s, e = _min_max_for_mode_sql(loc, table)
            if s is None or e is None:
                s, e = _min_max_any_sql(loc)
            start_date, end_date = s, e

        if not (start_date and end_date and loc and table):
            empty = go.Figure(); empty.update_layout(title="No data in selected range")
            return empty, empty, empty, ""

        qdata = text(f"""
            SELECT date, direction, count
            FROM {table}
            WHERE location_name = :loc
              AND date BETWEEN :s AND :e
            ORDER BY date
        """)
        with ENGINE.connect() as con:
            data = pd.read_sql(qdata, con, params={"loc": loc, "s": start_date, "e": end_date})

        if data.empty:
            empty = go.Figure(); empty.update_layout(title="No data in selected range")
            # helpful hint if someone deep-links to a mode with no rows
            note = f"No data for {mode} at this site in the selected range."
            return empty, empty, empty, note

        data["date"] = pd.to_datetime(data["date"]).sort_values()

        # Hourly
        hourly_fig = px.line(data, x="date", y="count", color="direction", title="Hourly Traffic Trends")

        # Daily totals
        daily = data.set_index("date")["count"].resample("D").sum().reset_index()
        daily_fig = px.line(daily, x="date", y="count", title="Total Daily Traffic")

        # Day-of-week average
        tmp = data.set_index("date")
        tmp["dow"] = tmp.index.dayofweek
        dow_avg = tmp.groupby("dow")["count"].mean().reindex([0,1,2,3,4,5,6]).fillna(0)
        dow_fig = px.bar(
            x=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
            y=[dow_avg.get(i, 0) for i in range(7)],
            title="Average Traffic by Day of the Week"
        )

        # subtle nudge in case the other mode has data
        note = ""
        if mode == "Pedestrian":
            # quick check: does bike have rows here?
            bmn, bmx = _min_max_for_mode_sql(loc, "eco_bike_traffic_data")
            if bmn and bmx:
                note = "Tip: This site also has Bicyclist data."
        elif mode == "Bicyclist":
            pmn, pmx = _min_max_for_mode_sql(loc, "eco_ped_traffic_data")
            if pmn and pmx:
                note = "Tip: This site also has Pedestrian data."

        return hourly_fig, daily_fig, dow_fig, note

    # ---- login ---------------------------------------------------------------
    @app.callback(
        Output("eco-url", "pathname"),
        Output("eco-login-output", "children"),
        Input("eco-login-button", "n_clicks"),
        State("eco-username", "value"),
        State("eco-password", "value")
    )
    def eco_login(n_clicks, u, p):
        if n_clicks:
            if u in VALID_USERS and VALID_USERS[u] == p:
                flask_session["user"] = u
                return f"{prefix}summary", ""
            return dash.no_update, "Invalid username or password."
        return dash.no_update, ""

    return app
