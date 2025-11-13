from pathlib import Path
import sys

from dash import html
import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from unified_explore import _build_summary_dashboard_content

def _extract_metrics(metrics_row):
    metrics = {}
    for col in metrics_row.children:
        card = col.children
        if isinstance(card, (list, tuple)):
            card = card[0]
        label_div, value_div = card.children
        label = label_div.children
        value = value_div.children
        if isinstance(label, (list, tuple)):
            label = "".join(str(part) for part in label)
        if isinstance(value, (list, tuple)):
            value = "".join(str(part) for part in value)
        metrics[str(label)] = str(value)
    return metrics

def _as_iterable(children):
    if children is None:
        return []
    if isinstance(children, (list, tuple)):
        return list(children)
    return [children]


def _extract_top_locations(summary_row):
    for col in summary_row.children:
        for child in _as_iterable(col.children):
            if isinstance(child, html.Ul):
                top_items = []
                for item in _as_iterable(child.children):
                    location_span, value_span = _as_iterable(item.children)
                    location = location_span.children
                    value = value_span.children
                    top_items.append((str(location), str(value)))
                return top_items
    return []

def test_summary_snapshot_uses_deduplicated_locations():
    df = pd.DataFrame(
        [
            {
                "Location": "Trail Alpha",
                "Duration": "2023",
                "Total counts": 200,
                "Source type": "Total",
            },
            {
                "Location": "Trail Alpha",
                "Duration": "2023",
                "Total counts": 120,
                "Source type": "Northbound",
            },
            {
                "Location": "Trail Alpha",
                "Duration": "2023",
                "Total counts": 80,
                "Source type": "Southbound",
            },
            {
                "Location": "Trail Beta",
                "Duration": "2023",
                "Total counts": 150,
                "Source type": "Total",
            },
        ]
    )

    summary = _build_summary_dashboard_content(df)

    metrics_row = summary[1]
    metrics = _extract_metrics(metrics_row)

    assert metrics["Total recorded volume"] == "350"
    assert metrics["Peak day volume"] == "200"
    assert metrics["Average daily count"] == "175.0"

    top_locations_row = summary[2]
    top_locations = _extract_top_locations(top_locations_row)

    assert top_locations == [
        ("Trail Alpha", "200"),
        ("Trail Beta", "150"),
    ]
