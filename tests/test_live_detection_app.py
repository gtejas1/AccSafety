import importlib
import sys
import types
import logging
from datetime import datetime
from pathlib import Path

import pytest
from flask import Flask


class _DummyCapture:
    def __init__(self, *_, **__):
        self._opened = True

    def isOpened(self):
        return self._opened

    def set(self, *_):
        return None


class _DummyConn:
    def __init__(self, should_fail: bool = False, rows=None):
        self.should_fail = should_fail
        self._rows = rows or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, *_, **__):
        if self.should_fail:
            raise RuntimeError("insert failed")
        return types.SimpleNamespace(
            mappings=lambda: types.SimpleNamespace(all=lambda: self._rows)
        )


class _DummyEngine:
    def __init__(self, should_fail: bool = False, rows=None):
        self.should_fail = should_fail
        self.rows = rows or []

    def begin(self):
        return _DummyConn(self.should_fail, self.rows)

    def connect(self):
        return _DummyConn(self.should_fail, self.rows)


class _DummyText:
    def __init__(self, sql: str):
        self.sql = sql

    def bindparams(self, *_, **__):
        return self


@pytest.fixture
def live_detection(monkeypatch):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    sys.modules.pop("live_detection_app", None)

    cv2_stub = types.SimpleNamespace(
        CAP_FFMPEG=0,
        CAP_PROP_BUFFERSIZE=1,
        VideoCapture=_DummyCapture,
        line=lambda *args, **kwargs: None,
        putText=lambda *args, **kwargs: None,
        FONT_HERSHEY_SIMPLEX=0,
        LINE_AA=0,
        resize=lambda img, size, interpolation=None: img,
        INTER_AREA=0,
        imencode=lambda ext, img, params=None: (True, b""),
    )
    yolo_stub = lambda *_args, **_kwargs: types.SimpleNamespace(names={0: "person", 1: "bicycle"})
    ultralytics_stub = types.SimpleNamespace(YOLO=yolo_stub)
    sqlalchemy_stub = types.SimpleNamespace(
        create_engine=lambda *args, **kwargs: _DummyEngine(),
        text=lambda txt: _DummyText(txt),
        bindparam=lambda name, type_=None: {"name": name, "type": type_},
        JSON=object,
    )

    monkeypatch.setitem(sys.modules, "cv2", cv2_stub)
    monkeypatch.setitem(sys.modules, "ultralytics", ultralytics_stub)
    monkeypatch.setitem(sys.modules, "sqlalchemy", sqlalchemy_stub)

    module = importlib.import_module("live_detection_app")
    module.ENGINE = None
    module._ENGINE_LAST_FAIL_TS = 0.0
    return module


def test_get_engine_warns_when_unavailable(live_detection, caplog, monkeypatch):
    caplog.set_level(logging.WARNING)

    def failing_engine(*_, **__):
        raise RuntimeError("db offline")

    monkeypatch.setattr(live_detection, "create_engine", failing_engine)
    live_detection.ENGINE = None
    live_detection._ENGINE_LAST_FAIL_TS = 0.0

    assert live_detection._get_engine() is None
    assert any("Failed to create DB engine" in rec.message for rec in caplog.records)

    with caplog.at_level(logging.WARNING):
        assert live_detection._get_engine() is None
    assert any("retrying" in rec.message.lower() for rec in caplog.records)

    # Ensure we did not keep retrying immediately.
    assert live_detection.ENGINE is None


def test_persist_counts_logs_and_resets_engine_on_failure(live_detection, caplog):
    caplog.set_level(logging.ERROR)

    worker = object.__new__(live_detection.VideoWorker)
    worker._last_save_ts = 0
    worker.save_interval = 0
    worker._pending_totals = {"pedestrians": 1, "cyclists": 2}
    worker._pending_crosswalk_counts = {"north": {"pedestrians": 1, "cyclists": 0}}
    worker._interval_start = datetime.utcnow()

    live_detection.ENGINE = _DummyEngine(should_fail=True)
    live_detection._ENGINE_LAST_FAIL_TS = 0

    worker._persist_counts_if_needed_locked(force=True)

    assert any("Failed to persist live detection counts" in rec.message for rec in caplog.records)
    assert live_detection.ENGINE is None
    assert worker._pending_totals == {"pedestrians": 1, "cyclists": 2}
    assert worker._pending_crosswalk_counts == {"north": {"pedestrians": 1, "cyclists": 0}}
    assert live_detection._ENGINE_LAST_FAIL_TS > 0


def test_build_counts_csv_bytes_serializes_rows(live_detection):
    rows = [
        {
            "interval_start": datetime(2024, 1, 1, 0, 0, 0),
            "interval_end": datetime(2024, 1, 1, 0, 5, 0),
            "total_pedestrians": 3,
            "total_cyclists": 1,
            "crosswalk_counts": {"north": {"pedestrians": 3, "cyclists": 1}},
        }
    ]

    live_detection.ENGINE = _DummyEngine(rows=rows)
    live_detection._ENGINE_LAST_FAIL_TS = 0

    payload = live_detection._build_counts_csv_bytes()

    text = payload.decode("utf-8")
    assert "interval_start" in text
    assert "north" in text


def test_download_counts_route_returns_csv(monkeypatch, live_detection):
    class _Worker:
        def __init__(self, *_):
            self._config = [
                {"key": "north", "name": "North", "p1": (0, 0), "p2": (1, 1), "label": (0.5, 0.5)}
            ]

        def start(self):
            return None

        def get_crosswalk_config(self):
            return self._config

        def get_stats(self):
            return 0, 0, None, {}

        def get_jpeg(self):
            return b""

        def set_crosswalk_config(self, config):
            self._config = config

    server = Flask(__name__)
    monkeypatch.setattr(live_detection, "VideoWorker", _Worker)

    rows = [
        {
            "interval_start": datetime(2024, 1, 1, 0, 0, 0),
            "interval_end": datetime(2024, 1, 1, 0, 5, 0),
            "total_pedestrians": 2,
            "total_cyclists": 0,
            "crosswalk_counts": {"north": {"pedestrians": 2, "cyclists": 0}},
        }
    ]
    live_detection.ENGINE = _DummyEngine(rows=rows)
    live_detection._ENGINE_LAST_FAIL_TS = 0

    live_detection.create_live_detection_app(server, prefix="/dl/")
    endpoint = server.view_functions["live_detection_download_dl"]

    with server.test_request_context():
        resp = endpoint()

    assert resp.status_code == 200
    assert resp.mimetype == "text/csv"
    resp.direct_passthrough = False
    assert b"interval_start" in resp.get_data()
