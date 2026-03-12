from __future__ import annotations

import importlib
import sys
import types
from io import BytesIO
from pathlib import Path

import pandas as pd

from explore_data import UNIFIED_SEARCH_SQL
import upload_service


def _login(client, username="admin", roles=None):
    with client.session_transaction() as session:
        session["user"] = username
        session["roles"] = roles or ["admin"]


def _build_excel(rows: list[list[object]]) -> bytes:
    frame = pd.DataFrame(rows)
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        frame.to_excel(writer, index=False, header=False, sheet_name="eco-counter")
    return buffer.getvalue()


def _load_gateway(monkeypatch):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    sys.modules.pop("gateway", None)
    monkeypatch.setitem(
        sys.modules,
        "cv2",
        types.SimpleNamespace(
            CAP_FFMPEG=0,
            CAP_PROP_BUFFERSIZE=1,
            VideoCapture=lambda *args, **kwargs: types.SimpleNamespace(isOpened=lambda: True, set=lambda *a, **k: None),
            line=lambda *args, **kwargs: None,
            putText=lambda *args, **kwargs: None,
            FONT_HERSHEY_SIMPLEX=0,
            LINE_AA=0,
            resize=lambda img, size, interpolation=None: img,
            INTER_AREA=0,
            imencode=lambda ext, img, params=None: (True, b""),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "ultralytics",
        types.SimpleNamespace(YOLO=lambda *_args, **_kwargs: types.SimpleNamespace(names={0: "person", 1: "bicycle"})),
    )

    gateway = importlib.import_module("gateway")
    monkeypatch.setattr(gateway, "create_trail_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_eco_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_vivacity_dash", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_live_detection_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_wisdot_files_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_se_wi_trails_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(gateway, "create_unified_explore", lambda *args, **kwargs: None)
    return gateway


def test_parse_excel_upload_handles_directional_file():
    payload = _build_excel(
        [
            ["Period", "February 7, 2026 - March 5, 2026", None],
            [None, None, None],
            ["Time", "W Fond du Lac Ave. & W Locust St. Pedestrian IN", "W Fond du Lac Ave. & W Locust St. Pedestrian OUT"],
            [pd.Timestamp("2026-02-07 13:00:00"), 2, 3],
            [pd.Timestamp("2026-02-07 14:00:00"), 1, 4],
        ]
    )

    parsed = upload_service.parse_excel_upload(
        payload,
        filename="W Fond du Lac Ave. & W Locust St.xlsx",
        selected_mode="Pedestrian",
    )

    assert parsed.status == "ready_for_review"
    assert parsed.location_name == "W Fond du Lac Ave. & W Locust St"
    assert parsed.valid_rows == 4
    assert parsed.invalid_rows == 0
    assert {row.direction for row in parsed.rows} == {
        "W Fond du Lac Ave. & W Locust St. Pedestrian IN",
        "W Fond du Lac Ave. & W Locust St. Pedestrian OUT",
    }


def test_parse_excel_upload_handles_single_series_file():
    payload = _build_excel(
        [
            ["Period", "January 21, 2026 - February 19, 2026"],
            [None, None],
            ["Time", "N Sherman & W Capitol"],
            [pd.Timestamp("2026-01-21 11:00:00"), 352],
            [pd.Timestamp("2026-01-21 12:00:00"), 3],
        ]
    )

    parsed = upload_service.parse_excel_upload(
        payload,
        filename="N Sherman Blvd & W Capitol Dr.xlsx",
        selected_mode="Both",
    )

    assert parsed.status == "ready_for_review"
    assert parsed.valid_rows == 2
    assert all(row.direction == "Total" for row in parsed.rows)


def test_parse_excel_upload_rejects_missing_time_header():
    payload = _build_excel(
        [
            ["Period", "Window"],
            ["Start", "Count"],
            [pd.Timestamp("2026-01-21 11:00:00"), 10],
        ]
    )

    parsed = upload_service.parse_excel_upload(
        payload,
        filename="bad.xlsx",
        selected_mode="Pedestrian",
    )

    assert parsed.status == "invalid"
    assert "Time" in parsed.error_message


def test_parse_excel_upload_marks_non_numeric_rows_invalid():
    payload = _build_excel(
        [
            ["Period", "Window"],
            [None, None],
            ["Time", "Location Count"],
            [pd.Timestamp("2026-01-21 11:00:00"), "abc"],
            [pd.Timestamp("2026-01-21 12:00:00"), 9],
        ]
    )

    parsed = upload_service.parse_excel_upload(
        payload,
        filename="counts.xlsx",
        selected_mode="Pedestrian",
    )

    assert parsed.valid_rows == 1
    assert parsed.invalid_rows == 1
    assert any("not numeric" in row.validation_message for row in parsed.rows if row.validation_status == "invalid")


def test_admin_upload_routes_require_admin(monkeypatch):
    gateway = _load_gateway(monkeypatch)
    monkeypatch.setattr(upload_service, "ensure_tables", lambda engine: None)
    monkeypatch.setattr(upload_service, "list_uploads", lambda engine, limit=50: [])
    monkeypatch.setattr(upload_service, "get_upload_detail", lambda engine, upload_id: None)

    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client, username="ipit", roles=["user"])

    get_response = client.get("/admin/data-uploads")
    post_response = client.post(
        "/admin/data-uploads",
        data={"mode": "Pedestrian", "file": (BytesIO(b"fake"), "counts.xlsx")},
        content_type="multipart/form-data",
    )

    assert get_response.status_code == 403
    assert post_response.status_code == 403


class _PublishResult:
    def __init__(self, row=None, rowcount=None):
        self._row = row
        self.rowcount = rowcount

    def mappings(self):
        return self

    def first(self):
        return self._row


class _PublishConn:
    def __init__(self):
        self.statements: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "FROM admin_upload_manifests" in sql and "FOR UPDATE" in sql:
            return _PublishResult(
                {
                    "upload_id": "upload-1",
                    "selected_mode": "Bicyclist",
                    "valid_rows": 2,
                    "status": "ready_for_review",
                    "published_at": None,
                }
            )
        if sql.lstrip().startswith("INSERT INTO eco_bike_traffic_data"):
            return _PublishResult(rowcount=2)
        return _PublishResult(rowcount=1)


class _PublishEngine:
    def __init__(self):
        self.conn = _PublishConn()

    def begin(self):
        return self.conn


def test_publish_upload_targets_selected_mode_table(monkeypatch):
    monkeypatch.setattr(upload_service, "ensure_tables", lambda engine: None)
    engine = _PublishEngine()

    result = upload_service.publish_upload(engine, "upload-1", published_by="admin")

    assert result == {"status": "published", "inserted_rows": 2}
    assert any("INSERT INTO eco_bike_traffic_data" in sql for sql in engine.conn.statements)


def test_unified_search_sql_includes_uploaded_both_support():
    assert "eco_both_traffic_data" in UNIFIED_SEARCH_SQL


def test_unified_search_api_returns_both_mode(monkeypatch):
    gateway = _load_gateway(monkeypatch)
    monkeypatch.setattr(upload_service, "ensure_tables", lambda engine: None)

    def fake_read_sql(sql, engine, params=None):
        if params is not None:
            return pd.DataFrame(
                [
                    {
                        "Location": "N Sherman Blvd & W Capitol Dr",
                        "Longitude": None,
                        "Latitude": None,
                        "Total counts": 355,
                        "Source": "Wisconsin Pilot Counting Program Counts",
                        "Facility type": "Intersection",
                        "Mode": "Both",
                    }
                ]
            )
        return pd.DataFrame(columns=["Location", "Longitude", "Latitude", "Total counts", "Source", "Facility type", "Mode"])

    monkeypatch.setattr(gateway.pd, "read_sql", fake_read_sql)
    app = gateway.create_server()
    app.testing = True
    client = app.test_client()
    _login(client)

    response = client.get("/api/unified-search?q=Sherman")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["matches"][0]["datasets"][0]["Mode"] == "Both"
