"""Dash module that streams a live YOLO detection feed within the gateway.
Only 'person' and 'bicycle' detections are rendered, with cumulative counts shown.
"""

from __future__ import annotations

import os
import time
import threading
from typing import Optional, List, Dict, Tuple
from datetime import datetime

import cv2
from flask import Response
from ultralytics import YOLO

import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

from theme import card, dash_page

# ── Config ────────────────────────────────────────────────────────────────────
RTSP_URL = os.getenv(
    "YOLO_RTSP_URL",
    "http://root:Wisdot2018!@63.43.111.221:8881/axis-cgi/media.cgi?"
    "audiocodec=aac&audiosamplerate=16000&audiobitrate=32000&camera=1&"
    "videoframeskipmode=empty&videozprofile=classic&resolution=640x480&fps=30&"
    "audiodeviceid=0&audioinputid=0&timestamp=0&videocodec=h264&container=mp4",
)
MODEL_PATH = os.getenv("YOLO_MODEL", "yolo11n.pt")
TARGET_WIDTH = int(os.getenv("YOLO_TARGET_WIDTH", 960))
SCORE_THRESH = float(os.getenv("YOLO_SCORE_THRESH", 0.4))
FRAME_SKIP = int(os.getenv("YOLO_FRAME_SKIP", 0))
READ_TIMEOUT_SEC = float(os.getenv("YOLO_READ_TIMEOUT", 8))


def _find_allowed_class_ids(model: YOLO) -> Tuple[List[int], Dict[int, str]]:
    """Return target class IDs and mapping (id -> 'pedestrian' or 'cyclist')."""
    names = getattr(model, "names", {})
    if isinstance(names, list):
        id_to_name = {i: n for i, n in enumerate(names)}
    else:
        id_to_name = dict(names)

    wanted = set()
    group_map: Dict[int, str] = {}

    for cid, label in id_to_name.items():
        lbl = str(label).strip().lower()
        # pedestrian variants
        if lbl == "person" or "pedestrian" in lbl:
            wanted.add(cid)
            group_map[cid] = "pedestrian"
        # cyclist variants
        if lbl in {"bicycle", "bike", "cyclist"}:
            wanted.add(cid)
            group_map[cid] = "cyclist"

    if not wanted:
        # fallback to COCO defaults
        wanted = {0, 1}
        group_map = {0: "pedestrian", 1: "cyclist"}

    return sorted(wanted), group_map


class VideoWorker:
    """Background thread that maintains a connection to the video stream."""

    def __init__(self, rtsp_url: str, model_path: str) -> None:
        self.rtsp_url = rtsp_url
        self.model = YOLO(model_path)
        self.allowed_class_ids, self.class_group = _find_allowed_class_ids(self.model)

        self.cap: Optional[cv2.VideoCapture] = None
        self.frame_lock = threading.Lock()
        self.latest_jpeg: Optional[bytes] = None

        # Stats
        self.stats_lock = threading.Lock()
        self.total_counts = {"pedestrians": 0, "cyclists": 0}
        self.start_time: Optional[datetime] = None

        self.stop_flag = threading.Event()
        self.frame_count = 0
        self._started = threading.Event()

    # ────────────────────────────────────────────────────────────────────────
    def connect(self) -> None:
        """Connect to RTSP/HTTP source."""
        self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened():
            raise RuntimeError("Failed to open RTSP stream")
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    def _update_counts(self, results) -> None:
        """Accumulate total pedestrian/cyclist counts since start."""
        ped = cyc = 0
        try:
            boxes = results[0].boxes
            if boxes is not None and boxes.cls is not None:
                classes = boxes.cls.int().tolist()
                for cid in classes:
                    if cid in self.class_group:
                        if self.class_group[cid] == "pedestrian":
                            ped += 1
                        elif self.class_group[cid] == "cyclist":
                            cyc += 1
        except Exception:
            pass

        # Add detected counts to totals
        with self.stats_lock:
            self.total_counts["pedestrians"] += ped
            self.total_counts["cyclists"] += cyc
            if self.start_time is None:
                self.start_time = datetime.now()

    def loop(self) -> None:
        """Continuously read frames, run YOLO inference, and update totals."""
        last_ok = time.time()
        while not self.stop_flag.is_set():
            ok, frame = (self.cap.read() if self.cap else (False, None))
            now = time.time()

            if not ok:
                if now - last_ok > READ_TIMEOUT_SEC:
                    try:
                        if self.cap:
                            self.cap.release()
                        self.connect()
                        last_ok = time.time()
                        continue
                    except Exception:
                        time.sleep(1)
                        continue
                time.sleep(0.01)
                continue

            last_ok = now
            self.frame_count += 1
            if FRAME_SKIP and (self.frame_count % (FRAME_SKIP + 1) != 0):
                continue

            h, w = frame.shape[:2]
            if w > TARGET_WIDTH:
                scale = TARGET_WIDTH / float(w)
                frame = cv2.resize(
                    frame, (TARGET_WIDTH, int(h * scale)), interpolation=cv2.INTER_AREA
                )

            # detect pedestrians & cyclists only
            results = self.model.predict(
                frame,
                verbose=False,
                conf=SCORE_THRESH,
                imgsz=frame.shape[1],
                classes=self.allowed_class_ids,
            )

            self._update_counts(results)
            annotated = results[0].plot()

            ok, jpg = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 80])
            if ok:
                with self.frame_lock:
                    self.latest_jpeg = jpg.tobytes()

    def get_jpeg(self) -> Optional[bytes]:
        with self.frame_lock:
            return self.latest_jpeg

    def get_stats(self) -> Tuple[int, int, Optional[str]]:
        """Return cumulative totals and start time string."""
        with self.stats_lock:
            ped = self.total_counts.get("pedestrians", 0)
            cyc = self.total_counts.get("cyclists", 0)
            st = self.start_time
        start_str = st.strftime("%Y-%m-%d %H:%M:%S") if st else None
        return ped, cyc, start_str

    # ────────────────────────────────────────────────────────────────────────
    def start(self) -> None:
        if self._started.is_set():
            return

        def _run() -> None:
            while not self.stop_flag.is_set():
                try:
                    self.connect()
                    self.loop()
                except Exception:
                    if self.cap:
                        self.cap.release()
                        self.cap = None
                    time.sleep(1)

        thread = threading.Thread(target=_run, name="yolo-video-worker", daemon=True)
        thread.start()
        self._started.set()

    def stop(self) -> None:
        self.stop_flag.set()
        if self.cap:
            self.cap.release()


# ─────────────────────────────────────────────────────────────────────────────
def create_live_detection_app(server, prefix: str = "/live/"):
    """Attach the live detection Dash app to the shared Flask server."""
    worker = VideoWorker(RTSP_URL, MODEL_PATH)

    def _video_feed():
        worker.start()
        boundary = "frame"

        def gen():
            while True:
                jpeg = worker.get_jpeg()
                if jpeg is None:
                    time.sleep(0.02)
                    continue
                yield (
                    b"--" + boundary.encode() + b"\r\n"
                    b"Content-Type: image/jpeg\r\n"
                    b"Content-Length: " + str(len(jpeg)).encode() + b"\r\n\r\n" + jpeg + b"\r\n"
                )

        return Response(gen(), mimetype=f"multipart/x-mixed-replace; boundary={boundary}")

    endpoint_name = f"live_detection_video_feed_{prefix.strip('/').replace('/', '_') or 'root'}"
    route_path = f"{prefix}video_feed"
    if endpoint_name not in server.view_functions:
        server.add_url_rule(route_path, endpoint=endpoint_name, view_func=_video_feed)

    app = dash.Dash(
        name="live_detection_dash",
        server=server,
        routes_pathname_prefix=prefix,
        requests_pathname_prefix=prefix,
        external_stylesheets=[dbc.themes.BOOTSTRAP, "/static/theme.css"],
        suppress_callback_exceptions=True,
        assets_url_path=f"{prefix.rstrip('/')}/assets",
    )
    app.title = "Live Object Detection"

    # ── UI Layout: Video + cumulative stats ───────────────────────────────────
    app.layout = dash_page(
        "Long Term Counts · Live Detection",
        [
            card(
                [
                    html.H3("N Santa Monica Blvd & Silver Spring Drive - Whitefish Bay"),
                    html.P("Streaming live YOLO inference showing cumulative detections."),
                    html.P("NOTE: Only pedestrians and cyclists are counted."),
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Img(
                                    src=route_path,
                                    style={"width": "100%", "borderRadius": "12px"},
                                ),
                                md=8,
                            ),
                            dbc.Col(
                                [
                                    html.Div(
                                        [
                                            html.Div("Detection Started At", className="text-muted"),
                                            html.H5(id="start-time", className="mb-3"),
                                        ]
                                    ),
                                    html.Hr(),
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.Div("Pedestrians Detected", className="text-muted"),
                                                html.H2(id="ped-count", className="mb-0 text-primary"),
                                            ]
                                        ),
                                        className="mb-3 shadow-sm",
                                    ),
                                    dbc.Card(
                                        dbc.CardBody(
                                            [
                                                html.Div("Cyclists Detected", className="text-muted"),
                                                html.H2(id="cyc-count", className="mb-0 text-success"),
                                            ]
                                        ),
                                        className="mb-3 shadow-sm",
                                    ),
                                    dcc.Interval(id="stat-timer", interval=1000, n_intervals=0),
                                ],
                                md=4,
                            ),
                        ],
                        class_name="mt-2",
                    ),
                ],
                class_name="app-card--wide",
            )
        ],
    )

    # ── Live-updating counter ────────────────────────────────────────────────
    @app.callback(
        Output("ped-count", "children"),
        Output("cyc-count", "children"),
        Output("start-time", "children"),
        Input("stat-timer", "n_intervals"),
        prevent_initial_call=False,
    )
    def _update_stats(_):
        worker.start()
        ped, cyc, start_str = worker.get_stats()
        return str(ped), str(cyc), (start_str or "—")

    return app
