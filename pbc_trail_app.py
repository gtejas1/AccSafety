# pbc_trail_app.py
import io
import urllib.parse
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output, State
import dash_bootstrap_components as dbc

from flask import session as flask_session, request as flask_request, send_file

from theme import card, centered, dash_page
from data_service import fetch_counts, fetch_site_metadata

# ---- Config -----------------------------------------------------------------
VALID_USERS = {"admin": "admin", "user1": "mypassword"}


def create_trail_dash(server, prefix="/trail/"):
    app = dash.Dash(
        name="trail_dash",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets"
    )
    app.title = "AccSafety Dashboard (Trail)"
    

    # ── Summary table data ────────────────────────────────────────────────────
    summary_df = fetch_site_metadata("trail")
    summary_df["View"] = summary_df["location_name"].apply(
        lambda loc: f"[View]({prefix}dashboard?location={urllib.parse.quote(loc)})"
    )

    # ── Pages (no welcome) ────────────────────────────────────────────────────
    login_page_layout = centered(
        card(
            [
                html.H2("Trail Dashboard Login", className="text-center mb-3"),
                dcc.Input(
                    id="trail-username",
                    type="text",
                    placeholder="Username",
                    className="form-control mb-2",
                ),
                dcc.Input(
                    id="trail-password",
                    type="password",
                    placeholder="Password",
                    className="form-control mb-2",
                ),
                dbc.Button(
                    "Login",
                    id="trail-login-button",
                    color="primary",
                    className="w-100",
                ),
                html.Div(id="trail-login-output", className="text-danger mt-2"),
            ],
            class_name="app-card--narrow",
        )
    )

    summary_layout = card([
        html.H2("WisDOT Trail Counter Locations", style={"textAlign": "center"}),
        dash_table.DataTable(
            id="trail-summary-table",
            columns=[
                {"name": "Location", "id": "location_name"},
                {"name": "Start Date", "id": "start_date"},
                {"name": "End Date", "id": "end_date"},
                {"name": "Total Counts", "id": "total_counts", "type": "numeric"},
                {"name": "Avg Hourly Count", "id": "average_hourly_count", "type": "numeric"},
                {"name": "View", "id": "View", "presentation": "markdown"},
            ],
            data=summary_df.to_dict("records"),
            markdown_options={"html": True, "link_target": "_self"},
            style_as_list_view=True,
            style_cell={"textAlign": "center", "padding": "8px"},
            style_header={"backgroundColor": "#f1f5f9", "fontWeight": "bold", "fontSize": "16px"},
            style_data_conditional=[
                {"if": {"row_index": "odd"}, "backgroundColor": "rgba(15, 23, 42, 0.03)"}
            ],
        ),
    ])

    dashboard_layout = card([
        dbc.Row([dbc.Col(html.H1(id="trail-dashboard-title", className="text-center mb-4"))]),
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
            dbc.Col(dcc.DatePickerRange(id="trail-date-picker", display_format="MM-DD-YYYY"), width=12, lg=6),
            dbc.Col(
                html.Div(
                    html.A(
                        "Download Data",
                        id="trail-download-link",
                        href="",
                        target="_blank",
                        className="btn btn-outline-primary float-end",
                    )
                ),
                width=12,
                lg=6,
            ),
        ], className="g-3"),
        html.Hr(),
        dbc.Row([
            dbc.Col(html.H4("Volume Summary"), width=12),
            dbc.Col(dcc.Graph(id="trail-daily-traffic"), width=12),
        ]),
        dbc.Row([
            dbc.Col(html.H4("Daily Volumes"), width=12),
            dbc.Col(dcc.Graph(id="trail-hourly-traffic"), width=12),
        ]),
        dbc.Row([
            dbc.Col(html.H4("Average Volume by Day of the Week"), width=12),
            dbc.Col(dcc.Graph(id="trail-dow-traffic"), width=12),
        ]),
    ])

    # ── Top-level layout (single Location) ────────────────────────────────────
    app.layout = dash_page(
        "Short Term Counts · Trails",
        [
            dcc.Location(id="trail-url", refresh=False),
            dcc.Store(id="trail-selected-location"),
            dcc.Store(id="trail-location-name-store"),
            html.Div(id="trail-page-content"),
        ],
    )

    app.validation_layout = html.Div([app.layout, login_page_layout, summary_layout, dashboard_layout])

    # ── Scoped download endpoint ──────────────────────────────────────────────
    def _trail_download():
        location = flask_request.args.get("location")
        if not location:
            return "Location not specified", 400
        df = fetch_counts("trail", site_ids=[location])
        buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
        return send_file(io.BytesIO(buf.read().encode()), mimetype="text/csv",
                         as_attachment=True, download_name=f"{location}_traffic_data.csv")
    server.add_url_rule(f"{prefix}download", endpoint="trail_download", view_func=_trail_download)

    # ── Routing (default → summary) ───────────────────────────────────────────
    @app.callback(Output("trail-page-content", "children"),
                  Input("trail-url", "pathname"), Input("trail-url", "search"))
    def trail_display_page(pathname, _search):
        if "user" not in flask_session:
            return login_page_layout
        if (pathname or "").endswith("/dashboard"):
            return dashboard_layout
        return summary_layout

    # seed controls from URL (?location=…)
    @app.callback(
        Output("trail-date-picker", "min_date_allowed"),
        Output("trail-date-picker", "max_date_allowed"),
        Output("trail-date-picker", "start_date"),
        Output("trail-date-picker", "end_date"),
        Output("trail-download-link", "href"),
        Output("trail-dashboard-title", "children"),
        Input("trail-url", "search"),
    )
    def trail_set_date_range(search):
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        loc = q.get("location")
        if not loc:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

        metadata = fetch_site_metadata("trail", site_ids=[loc])
        if metadata.empty or pd.isna(metadata.iloc[0]["start_date"]):
            return None, None, None, None, "", loc

        min_date = metadata.iloc[0]["start_date"]
        max_date = metadata.iloc[0]["end_date"]
        dl = f"{prefix}download?location={urllib.parse.quote(loc)}"
        return min_date, max_date, min_date, max_date, dl, loc

    # graphs
    @app.callback(
        Output("trail-hourly-traffic", "figure"),
        Output("trail-daily-traffic", "figure"),
        Output("trail-dow-traffic", "figure"),
        Input("trail-date-picker", "start_date"),
        Input("trail-date-picker", "end_date"),
        Input("trail-url", "search"),
    )
    def trail_update_graphs(start_date, end_date, search):
        q = dict(urllib.parse.parse_qsl((search or "").lstrip("?")))
        loc = q.get("location")
        if not (start_date and end_date and loc):
            empty = go.Figure()
            return empty, empty, empty

        data = fetch_counts("trail", site_ids=[loc], start=pd.to_datetime(start_date), end=pd.to_datetime(end_date))
        if data.empty:
            empty = go.Figure(); empty.update_layout(title="No data in selected range")
            return empty, empty, empty

        data["date"] = pd.to_datetime(data["date"])
        data.set_index("date", inplace=True)
        daily = data.resample("D").sum(numeric_only=True)
        hourly_fig = px.line(data, x=data.index, y="count", color="direction", title="Hourly Traffic Trends")
        daily_fig  = px.line(daily, x=daily.index, y="count", title="Total Daily Traffic")
        dow_avg = data.groupby(data.index.dayofweek)["count"].mean().reindex([0,1,2,3,4,5,6]).fillna(0)
        dow_fig = px.bar(x=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
                         y=dow_avg, title="Average Traffic by Day of the Week")
        return hourly_fig, daily_fig, dow_fig

    # login → go straight to summary
    @app.callback(
        Output("trail-url", "pathname"),
        Output("trail-login-output", "children"),
        Input("trail-login-button", "n_clicks"),
        State("trail-username", "value"),
        State("trail-password", "value")
    )
    def trail_login(n_clicks, u, p):
        if n_clicks:
            if u in VALID_USERS and VALID_USERS[u] == p:
                flask_session["user"] = u
                return f"{prefix}summary", ""
            return dash.no_update, "Invalid username or password."
        return dash.no_update, ""

    return app
