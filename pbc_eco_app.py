# pbc_eco_app.py  — uses separated ECO tables with Pedestrian / Bicyclist / Both
import io
import urllib.parse
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc
from sqlalchemy import create_engine
from flask import session as flask_session, request as flask_request, send_file

from theme import card, centered, dash_page

# ---- Config -----------------------------------------------------------------
VALID_USERS = {"admin": "admin", "user1": "mypassword"}
DB_URL = "postgresql://postgres:gw2ksoft@localhost/TrafficDB"
ENGINE = create_engine(DB_URL)

# Map "Mode" -> underlying table
MODE_TABLE = {
    "Pedestrian": "eco_ped_traffic_data",
    "Bicyclist": "eco_bike_traffic_data",
    "Both": "eco_both_traffic_data",
}
MODE_OPTIONS = [{"label": m, "value": m} for m in ["Pedestrian", "Bicyclist", "Both"]]

def _table_for_mode(mode: str) -> str:
    return MODE_TABLE.get(mode or "", "")

def create_eco_dash(server, prefix="/eco/"):
    app = dash.Dash(
        name="eco_dash",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets"
    )
    app.title = "Temporary Counts - TRCC Statewide Pilot Counting Projects"

    # ── Summary table data (union of the three ECO tables) ────────────────────
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
        UNION ALL
        SELECT 'Both' AS mode, location_name,
               MIN(date), MAX(date),
               SUM(count)::bigint,
               AVG(count)::numeric(12,2)
        FROM eco_both_traffic_data
        GROUP BY location_name
        ORDER BY location_name, mode;
    """
    summary_df = pd.read_sql(summary_query, ENGINE)
    if not summary_df.empty:
        summary_df["start_date"] = pd.to_datetime(summary_df["start_date"]).dt.date
        summary_df["end_date"] = pd.to_datetime(summary_df["end_date"]).dt.date
        summary_df["average_hourly_count"] = summary_df["average_hourly_count"].round(0).astype(int)
        summary_df["View"] = summary_df.apply(
            lambda r: f"[View]({prefix}dashboard?location={urllib.parse.quote(str(r['location_name']))}&mode={urllib.parse.quote(str(r['mode']))})",
            axis=1,
        )

    # ── Pages (login + summary + dashboard) ───────────────────────────────────
    login_page_layout = centered(
        card(
            [
                html.H2("Temporary Counts Login", className="text-center mb-3"),
                dcc.Input(
                    id="eco-username",
                    type="text",
                    placeholder="Username",
                    className="form-control mb-2",
                ),
                dcc.Input(
                    id="eco-password",
                    type="password",
                    placeholder="Password",
                    className="form-control mb-2",
                ),
                dbc.Button(
                    "Login",
                    id="eco-login-button",
                    color="primary",
                    className="w-100",
                ),
                html.Div(id="eco-login-output", className="text-danger mt-2"),
            ],
            class_name="app-card--narrow",
        )
    )

    summary_layout = card([
        html.H2(
            "Temporary Counts - TRCC Statewide Pilot Counting Projects",
            style={"textAlign": "center"},
        ),
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
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgba(15, 23, 42, 0.03)"}
            ],
            page_size=20,
        ),
    ])

    dashboard_layout = card([
        dbc.Row([dbc.Col(html.H1(id="eco-dashboard-title", className="text-center mb-4"))]),
        dbc.Row([
            dbc.Col(
                dbc.Button(
                    "Back to Summary",
                    href=f"{prefix}summary",
                    color="secondary",
                    className="mb-3",
                ),
                width=12,
            )
        ]),
        dbc.Row([
            dbc.Col(
                dcc.Dropdown(
                    id="eco-mode",
                    options=MODE_OPTIONS,
                    value="Pedestrian",
                    clearable=False,
                    placeholder="Mode",
                ),
                xs=12, md=6, lg=4
            ),
            dbc.Col(dcc.DatePickerRange(id="eco-date-picker", display_format="MM-DD-YYYY"),
                    xs=12, md=6, lg=4),
            dbc.Col(
                html.Div(
                    html.A(
                        "Download Data",
                        id="eco-download-link",
                        href="",
                        target="_blank",
                        className="btn btn-outline-primary float-end",
                    )
                ),
                xs=12, lg=4,
            ),
        ], className="g-3"),
        html.Hr(),
        dbc.Row([
            dbc.Col(html.H4("Volume Summary"), width=12),
            dbc.Col(dcc.Graph(id="eco-daily-traffic"), width=12),
        ]),
        dbc.Row([
            dbc.Col(html.H4("Daily Volumes"), width=12),
            dbc.Col(dcc.Graph(id="eco-hourly-traffic"), width=12),
        ]),
        dbc.Row([
            dbc.Col(html.H4("Average Volume by Day of the Week"), width=12),
            dbc.Col(dcc.Graph(id="eco-dow-traffic"), width=12),
        ]),
    ])

    # ── Top-level layout (single Location) ────────────────────────────────────
    app.layout = dash_page(
        "Short Term Counts · Pilot Projects",
        [
            dcc.Location(id="eco-url", refresh=False),
            dcc.Store(id="eco-selected-location"),
            dcc.Store(id="eco-location-name-store"),
            html.Div(id="eco-page-content"),
        ],
    )

    # allow callbacks to validate against all components
    app.validation_layout = html.Div([app.layout, login_page_layout, summary_layout, dashboard_layout])

    # ── Scoped download endpoint ──────────────────────────────────────────────
    def _eco_download():
        location = flask_request.args.get("location")
        mode = flask_request.args.get("mode", "Pedestrian")
        table = _table_for_mode(mode)
        if not location or not table:
            return "Location or mode not specified", 400
        df = pd.read_sql(
            f"SELECT * FROM {table} WHERE location_name = %(location)s ORDER BY date",
            ENGINE, params={"location": location}
        )
        buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
        fname = f"{location}_{mode}_traffic_data.csv"
        return send_file(io.BytesIO(buf.read().encode()), mimetype="text/csv",
                         as_attachment=True, download_name=fname)
    server.add_url_rule(f"{prefix}download", endpoint="eco_download", view_func=_eco_download)

    # ── Routing (default → summary) ───────────────────────────────────────────
    @app.callback(Output("eco-page-content", "children"),
                  Input("eco-url", "pathname"), Input("eco-url", "search"))
    def eco_display_page(pathname, _search):
        if "user" not in flask_session:
            return login_page_layout
        if (pathname or "").endswith("/dashboard"):
            return dashboard_layout
        return summary_layout

    # seed dashboard controls from URL (?location=…&mode=…)
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
        mode = q.get("mode") or "Pedestrian"
        table = _table_for_mode(mode)
        if not (loc and table):
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, "", "Temporary Counts"

        date_range = pd.read_sql(
            f"SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {table} WHERE location_name = %(l)s",
            ENGINE, params={"l": loc}
        )
        if date_range.empty or pd.isna(date_range.iloc[0]["min_date"]):
            dl = f"{prefix}download?location={urllib.parse.quote(loc)}&mode={urllib.parse.quote(mode)}"
            return mode, None, None, None, None, dl, f"{loc} · {mode}"

        min_date = pd.to_datetime(date_range.iloc[0]["min_date"])
        max_date = pd.to_datetime(date_range.iloc[0]["max_date"])
        dl = f"{prefix}download?location={urllib.parse.quote(loc)}&mode={urllib.parse.quote(mode)}"
        return mode, min_date, max_date, min_date, max_date, dl, f"{loc} · {mode}"

    # When the user changes the Mode dropdown, update URL (keeps location)
    @app.callback(
        Output("eco-url", "search"),
        Input("eco-mode", "value"),
        State("eco-url", "search"),
    )
    def eco_update_mode_in_url(mode, search):
        if not mode:
            return dash.no_update
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        if q.get("mode") == mode:
            return dash.no_update
        q["mode"] = mode
        return "?" + urllib.parse.urlencode(q)

    # draw graphs using selected table/mode
    @app.callback(
        Output("eco-hourly-traffic", "figure"),
        Output("eco-daily-traffic", "figure"),
        Output("eco-dow-traffic", "figure"),
        Input("eco-date-picker", "start_date"),
        Input("eco-date-picker", "end_date"),
        Input("eco-url", "search"),
    )
    def eco_update_graphs(start_date, end_date, search):
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        loc = q.get("location")
        mode = q.get("mode") or "Pedestrian"
        table = _table_for_mode(mode)

        if not (start_date and end_date and loc and table):
            empty = go.Figure()
            return empty, empty, empty

        data = pd.read_sql(
            f"""
            SELECT date, direction, count
            FROM {table}
            WHERE location_name=%(l)s AND date BETWEEN %(s)s AND %(e)s
            ORDER BY date
            """,
            ENGINE, params={"l": loc, "s": start_date, "e": end_date}
        )
        if data.empty:
            empty = go.Figure(); empty.update_layout(title="No data in selected range")
            return empty, empty, empty

        data["date"] = pd.to_datetime(data["date"])
        data = data.sort_values("date")
        # Hourly (as recorded) — colored by direction
        hourly_fig = px.line(data, x="date", y="count", color="direction", title="Hourly Traffic Trends")

        # Daily totals
        tmp = data.set_index("date")
        daily = tmp["count"].resample("D").sum().reset_index()
        daily_fig = px.line(daily, x="date", y="count", title="Total Daily Traffic")

        # Day-of-week average
        tmp["dow"] = tmp.index.dayofweek
        dow_avg = tmp.groupby("dow")["count"].mean().reindex([0,1,2,3,4,5,6]).fillna(0)
        dow_fig = px.bar(
            x=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
            y=[dow_avg.get(i, 0) for i in range(7)],
            title="Average Traffic by Day of the Week"
        )
        return hourly_fig, daily_fig, dow_fig

    # login → go straight to summary
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
