"""Shared theming helpers for Dash pages."""

from __future__ import annotations

from typing import Iterable, Optional

from dash import html
import dash_bootstrap_components as dbc

PORTAL_URL = "/"
BRAND_NAME = "AccSafety"
BRAND_LOGO_SRC = "/static/img/accsafety-logo.png"
BRAND_LOGO_ALT = "AccSafety logo"


def portal_link(text: str = "Back to Portal") -> html.A:
    """Return the standard link back to the main portal."""

    return html.A(text, href=PORTAL_URL, className="app-link")


def build_header(
    subtitle: str,
    *,
    nav_children: Optional[Iterable[html.Component]] = None,
    right_children: Optional[Iterable[html.Component] | html.Component] = None,
    show_portal_link: bool = True,
) -> html.Header:
    """Create a themed header for Dash pages."""

    nav_items = list(nav_children or [])
    if show_portal_link:
        nav_items.insert(0, portal_link())

    header_children = [
        html.Img(
            src=BRAND_LOGO_SRC,
            alt=BRAND_LOGO_ALT,
            className="app-logo",
        ),
        html.Div(
            [
                html.Span(BRAND_NAME, className="app-brand"),
                html.Span(subtitle, className="app-subtitle"),
            ],
            className="app-header-title",
        ),
    ]

    header_children.append(html.Nav(nav_items, className="app-nav"))

    if right_children:
        header_children.append(html.Div(right_children, className="app-user"))

    return html.Header(header_children, className="app-header")


def dash_page(
    subtitle: str,
    body_children,
    *,
    fluid: bool = True,
    nav_children: Optional[Iterable[html.Component]] = None,
    right_children: Optional[Iterable[html.Component] | html.Component] = None,
    show_portal_link: bool = True,
):
    """Wrap page content in the shared themed shell."""

    header = build_header(
        subtitle,
        nav_children=nav_children,
        right_children=right_children,
        show_portal_link=show_portal_link,
    )

    container = dbc.Container(body_children, fluid=fluid, className="app-content")
    launcher = html.Div(className="assistant-launcher", **{"data-endpoint": "/assistant/chat"})
    return html.Div([header, container, launcher], className="app-shell")


def card(children, *, class_name: str | None = None):
    """Return a stylised content card."""

    classes = "app-card"
    if class_name:
        classes = f"{classes} {class_name}".strip()
    return html.Div(children, className=classes)


def centered(children):
    """Utility wrapper to horizontally center content."""

    return html.Div(children, className="app-main-centered")
