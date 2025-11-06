"""Dash module that streams a live YOLO detection feed within the gateway.
Only 'person' and 'bicycle' detections are rendered, with cumulative counts shown.
"""

from __future__ import annotations

import os
import time
import threading
import math
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

# Virtual crosswalk definitions expressed as normalized coordinates (x, y)
# relative to the incoming frame. These were calibrated against the public
# Silver Spring at Santa Monica feed and may need adjustment if the camera
# angle or resolution changes substantially.
CROSSWALK_LINES = [
    {
        "key": "north",
        "name": "North Crosswalk",
        "p1": (0.12, 0.22),
        "p2": (0.84, 0.16),
        "label": (0.48, 0.10),
    },
    {
        "key": "east",
        "name": "East Crosswalk",
        "p1": (0.86, 0.18),
        "p2": (0.92, 0.83),
        "label": (0.88, 0.50),
    },
    {
        "key": "south",
        "name": "South Crosswalk",
        "p1": (0.20, 0.84),
        "p2": (0.94, 0.92),
        "label": (0.58, 0.95),
    },
    {
        "key": "west",
        "name": "West Crosswalk",
        "p1": (0.10, 0.24),
        "p2": (0.04, 0.84),
        "label": (0.05, 0.55),
    },
]

CROSSWALK_DISTANCE_THRESHOLD = 60.0  # maximum distance (px) from a line to count a crossing
TRACK_STATE_TTL_SEC = 15.0


def _point_side_of_line(point: Tuple[float, float], p1: Tuple[int, int], p2: Tuple[int, int]) -> float:
    """Return the signed distance (scaled) of a point relative to a directed line."""
    x, y = point
    x1, y1 = p1
    x2, y2 = p2
    return (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)


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
        self.crosswalk_counts = {
            cw["key"]: {"pedestrians": 0, "cyclists": 0} for cw in CROSSWALK_LINES
        }
        self._counted_ids: Dict[str, set[int]] = {}
        self._track_sides: Dict[int, Dict[str, int]] = {}
        self._track_last_seen: Dict[int, float] = {}
        self._reset_tracker_history()

        self.stop_flag = threading.Event()
        self.frame_count = 0
        self._started = threading.Event()
        self._crosswalk_cache_shape: Optional[Tuple[int, int]] = None
        self._crosswalk_cache_pixels: List[
            Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]
        ] = []

    # ────────────────────────────────────────────────────────────────────────
    def connect(self) -> None:
        """Connect to RTSP/HTTP source."""
        self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened():
            raise RuntimeError("Failed to open RTSP stream")
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self._reset_tracker_history()

    def _reset_tracker_history(self) -> None:
        """Reset the cache of tracker IDs that have been counted."""
        groups = set(self.class_group.values())
        with self.stats_lock:
            self._counted_ids = {group: set() for group in groups}
            self._track_sides = {}
            self._track_last_seen = {}

    def clear_totals(self) -> None:
        """Clear cumulative counts and tracker history."""
        with self.stats_lock:
            self.total_counts = {"pedestrians": 0, "cyclists": 0}
            self.start_time = None
            groups = set(self.class_group.values())
            self._counted_ids = {group: set() for group in groups}
            self.crosswalk_counts = {
                cw["key"]: {"pedestrians": 0, "cyclists": 0} for cw in CROSSWALK_LINES
            }
            self._track_sides = {}
            self._track_last_seen = {}

    def _get_crosswalk_pixels(
        self, frame_shape: Tuple[int, int, int]
    ) -> List[Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]]:
        """Return cached crosswalk endpoints (in pixels) for the given frame shape."""
        h, w = frame_shape[:2]
        shape_key = (w, h)
        if self._crosswalk_cache_shape != shape_key:
            self._crosswalk_cache_shape = shape_key
            self._crosswalk_cache_pixels = []
            for cw in CROSSWALK_LINES:
                x1 = int(cw["p1"][0] * w)
                y1 = int(cw["p1"][1] * h)
                x2 = int(cw["p2"][0] * w)
                y2 = int(cw["p2"][1] * h)
                label = cw.get("label")
                label_px = None
                if label is not None:
                    label_px = (int(label[0] * w), int(label[1] * h))
                length = math.hypot(x2 - x1, y2 - y1) or 1.0
                self._crosswalk_cache_pixels.append(
                    (cw["key"], cw["name"], (x1, y1), (x2, y2), label_px, length)
                )
        return self._crosswalk_cache_pixels

    def _draw_crosswalks(
        self,
        frame,
        crosswalk_pixels: List[
            Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]
        ],
    ) -> None:
        """Overlay virtual crosswalk lines and labels on the annotated frame."""
        for _, name, p1, p2, label_px, _ in crosswalk_pixels:
            cv2.line(frame, p1, p2, (0, 0, 255), 2)
            if label_px:
                cv2.putText(
                    frame,
                    name,
                    label_px,
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    (0, 0, 255),
                    1,
                    cv2.LINE_AA,
                )

    def _update_counts(
        self,
        results,
        crosswalk_pixels: List[
            Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]
        ],
    ) -> None:
        """Accumulate total pedestrian/cyclist counts since start."""
        try:
            boxes = results[0].boxes
            if (
                boxes is None
                or boxes.cls is None
                or boxes.id is None
                or len(boxes.cls) == 0
            ):
                return
            classes = boxes.cls.int().tolist()
            track_ids = [int(tid) for tid in boxes.id.int().tolist()]
            centers: List[Optional[Tuple[float, float]]] = []
            if getattr(boxes, "xywh", None) is not None:
                centers = [(float(x), float(y)) for x, y, _, _ in boxes.xywh.cpu().tolist()]
            if len(centers) < len(track_ids):
                centers.extend([None] * (len(track_ids) - len(centers)))
            elif len(centers) > len(track_ids):
                centers = centers[: len(track_ids)]
        except Exception:
            pass
        else:
            deltas = {"pedestrians": 0, "cyclists": 0}
            with self.stats_lock:
                timestamp = time.time()
                for cid, track_id, center in zip(classes, track_ids, centers):
                    group = self.class_group.get(cid)
                    if group is None:
                        continue
                    counted_for_group = self._counted_ids.setdefault(group, set())
                    already_counted = track_id < 0 or track_id in counted_for_group
                    if not already_counted:
                        counted_for_group.add(track_id)
                        if group == "pedestrian":
                            deltas["pedestrians"] += 1
                        elif group == "cyclist":
                            deltas["cyclists"] += 1

                    if track_id < 0 or center is None:
                        continue

                    self._track_last_seen[track_id] = timestamp
                    track_crosswalk_state = self._track_sides.setdefault(track_id, {})
                    for key, _, p1, p2, _, length in crosswalk_pixels:
                        side_val = _point_side_of_line(center, p1, p2)
                        if side_val == 0:
                            continue
                        distance = abs(side_val) / length
                        if distance > CROSSWALK_DISTANCE_THRESHOLD:
                            continue
                        side = 1 if side_val > 0 else -1
                        prev_side = track_crosswalk_state.get(key)
                        if prev_side is None:
                            track_crosswalk_state[key] = side
                            continue
                        if prev_side != side:
                            track_crosswalk_state[key] = side
                            counts_for_crosswalk = self.crosswalk_counts.setdefault(
                                key,
                                {"pedestrians": 0, "cyclists": 0},
                            )
                            if group == "pedestrian":
                                counts_for_crosswalk["pedestrians"] += 1
                            elif group == "cyclist":
                                counts_for_crosswalk["cyclists"] += 1

                for key, delta in deltas.items():
                    if delta:
                        self.total_counts[key] += delta
                if (deltas["pedestrians"] or deltas["cyclists"]) and self.start_time is None:
                    self.start_time = datetime.now()

                self._prune_stale_tracks_locked(timestamp)
            return

        # If anything above failed, do not update counts.
        return

    def _prune_stale_tracks_locked(self, now_ts: float) -> None:
        """Remove cached per-track crosswalk state after a timeout."""
        stale = [
            track_id
            for track_id, last_seen in self._track_last_seen.items()
            if now_ts - last_seen > TRACK_STATE_TTL_SEC
        ]
        for track_id in stale:
            self._track_last_seen.pop(track_id, None)
            self._track_sides.pop(track_id, None)

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

            # detect pedestrians & cyclists only with persistent tracking
            crosswalk_pixels = self._get_crosswalk_pixels(frame.shape)
            results = self.model.track(
                frame,
                verbose=False,
                conf=SCORE_THRESH,
                imgsz=frame.shape[1],
                classes=self.allowed_class_ids,
                persist=True,
                tracker="bytetrack.yaml",
            )

            self._update_counts(results, crosswalk_pixels)
            annotated = results[0].plot()
            self._draw_crosswalks(annotated, crosswalk_pixels)

            ok, jpg = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 80])
            if ok:
                with self.frame_lock:
                    self.latest_jpeg = jpg.tobytes()

    def get_jpeg(self) -> Optional[bytes]:
        with self.frame_lock:
            return self.latest_jpeg

    def get_stats(self) -> Tuple[int, int, Optional[str], Dict[str, Dict[str, int]]]:
        """Return cumulative totals and start time string."""
        with self.stats_lock:
            ped = self.total_counts.get("pedestrians", 0)
            cyc = self.total_counts.get("cyclists", 0)
            st = self.start_time
            crosswalk_snapshot = {
                key: counts.copy() for key, counts in self.crosswalk_counts.items()
            }
        start_str = st.strftime("%Y-%m-%d %H:%M:%S") if st else None
        return ped, cyc, start_str, crosswalk_snapshot

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
    crosswalk_cards = []
    for cw in CROSSWALK_LINES:
        crosswalk_cards.append(
            dbc.Card(
                dbc.CardBody(
                    [
                        html.Div(cw["name"], className="text-muted"),
                        html.Div(
                            [
                                html.Span("Pedestrians", className="small text-uppercase text-muted"),
                                html.Span(
                                    "0",
                                    id=f"crosswalk-{cw['key']}-ped",
                                    className="fs-4 fw-bold text-primary",
                                ),
                            ],
                            className="d-flex justify-content-between align-items-baseline mt-2",
                        ),
                        html.Div(
                            [
                                html.Span("Cyclists", className="small text-uppercase text-muted"),
                                html.Span(
                                    "0",
                                    id=f"crosswalk-{cw['key']}-cyc",
                                    className="fs-4 fw-bold text-success",
                                ),
                            ],
                            className="d-flex justify-content-between align-items-baseline",
                        ),
                    ]
                ),
                className="mb-3 shadow-sm",
            )
        )

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
                                    html.Div(
                                        "Crosswalk Counts (both directions)",
                                        className="text-muted mt-4 mb-2",
                                    ),
                                    *crosswalk_cards,
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
        *[
            Output(f"crosswalk-{cw['key']}-ped", "children")
            for cw in CROSSWALK_LINES
        ],
        *[
            Output(f"crosswalk-{cw['key']}-cyc", "children")
            for cw in CROSSWALK_LINES
        ],
        Input("stat-timer", "n_intervals"),
        prevent_initial_call=False,
    )
    def _update_stats(_):
        worker.start()
        ped, cyc, start_str, crosswalk_counts = worker.get_stats()
        outputs: List[str] = [str(ped), str(cyc), (start_str or "—")]
        for cw in CROSSWALK_LINES:
            counts = crosswalk_counts.get(
                cw["key"], {"pedestrians": 0, "cyclists": 0}
            )
            outputs.append(str(counts.get("pedestrians", 0)))
        for cw in CROSSWALK_LINES:
            counts = crosswalk_counts.get(
                cw["key"], {"pedestrians": 0, "cyclists": 0}
            )
            outputs.append(str(counts.get("cyclists", 0)))
        return outputs

    return app
