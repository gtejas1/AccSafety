# pbc_eco_app.py
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

    # ── Summary table data ────────────────────────────────────────────────────
    summary_df = fetch_site_metadata("eco")
    summary_df["View"] = summary_df["location_name"].apply(
        lambda loc: f"[View]({prefix}dashboard?location={urllib.parse.quote(loc)})"
    )

    # ── Pages (no welcome) ────────────────────────────────────────────────────
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
            dbc.Col(dcc.DatePickerRange(id="eco-date-picker", display_format="MM-DD-YYYY"), width=12, lg=6),
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
                width=12,
                lg=6,
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
        if not location:
            return "Location not specified", 400
        df = fetch_counts("eco", site_ids=[location])
        buf = io.StringIO(); df.to_csv(buf, index=False); buf.seek(0)
        return send_file(io.BytesIO(buf.read().encode()), mimetype="text/csv",
                         as_attachment=True, download_name=f"{location}_traffic_data.csv")
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

    # seed dashboard controls from URL (?location=…)
    @app.callback(
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
        if not loc:
            return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

        metadata = fetch_site_metadata("eco", site_ids=[loc])
        if metadata.empty or pd.isna(metadata.iloc[0]["start_date"]):
            return None, None, None, None, "", loc

        min_date = metadata.iloc[0]["start_date"]
        max_date = metadata.iloc[0]["end_date"]
        dl = f"{prefix}download?location={urllib.parse.quote(loc)}"
        return min_date, max_date, min_date, max_date, dl, loc

    # draw graphs
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
        if not (start_date and end_date and loc):
            empty = go.Figure()
            return empty, empty, empty

        data = fetch_counts("eco", site_ids=[loc], start=pd.to_datetime(start_date), end=pd.to_datetime(end_date))
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
