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
        "p1": (0.15, 0.24),
        "p2": (0.90, 0.18),
        "label": (0.58, 0.11),
    },
    {
        "key": "east",
        "name": "East Crosswalk",
        "p1": (0.84, 0.20),
        "p2": (0.97, 0.86),
        "label": (0.90, 0.53),
    },
    {
        "key": "south",
        "name": "South Crosswalk",
        "p1": (0.18, 0.88),
        "p2": (0.92, 0.96),
        "label": (0.60, 0.97),
    },
    {
        "key": "west",
        "name": "West Crosswalk",
        "p1": (0.17, 0.26),
        "p2": (0.05, 0.86),
        "label": (0.07, 0.57),
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
        self.crosswalk_lock = threading.Lock()
        self.crosswalk_config: List[Dict[str, object]] = []
        self.crosswalk_counts: Dict[str, Dict[str, int]] = {}
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
        self.set_crosswalk_config(CROSSWALK_LINES)

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
        with self.crosswalk_lock:
            config_snapshot = [dict(cw) for cw in self.crosswalk_config]
        with self.stats_lock:
            self.total_counts = {"pedestrians": 0, "cyclists": 0}
            self.start_time = None
            groups = set(self.class_group.values())
            self._counted_ids = {group: set() for group in groups}
            self._track_sides = {}
            self._track_last_seen = {}
            self.crosswalk_counts = {
                cw["key"]: {"pedestrians": 0, "cyclists": 0} for cw in config_snapshot
            }

    def set_crosswalk_config(self, lines: List[Dict[str, object]]) -> None:
        """Replace the crosswalk configuration with new normalized endpoints."""

        def _clamp_pair(pair: Tuple[float, float]) -> Tuple[float, float]:
            return (max(0.0, min(1.0, float(pair[0]))), max(0.0, min(1.0, float(pair[1]))))

        normalized: List[Dict[str, object]] = []
        for cw in lines:
            key = str(cw.get("key"))
            if not key:
                continue
            name = cw.get("name") or f"{key.title()} Crosswalk"
            p1 = _clamp_pair(cw.get("p1", (0.0, 0.0)))
            p2 = _clamp_pair(cw.get("p2", (1.0, 1.0)))
            label = cw.get("label")
            if label is None:
                mid_x = (p1[0] + p2[0]) / 2.0
                mid_y = max(0.0, min(1.0, (p1[1] + p2[1]) / 2.0 - 0.05))
                label = (mid_x, mid_y)
            else:
                label = _clamp_pair(label)
            normalized.append({"key": key, "name": name, "p1": p1, "p2": p2, "label": label})

        if not normalized:
            return

        with self.crosswalk_lock:
            self.crosswalk_config = normalized
            self._crosswalk_cache_shape = None

        with self.stats_lock:
            prev_counts = {
                key: {"pedestrians": counts.get("pedestrians", 0), "cyclists": counts.get("cyclists", 0)}
                for key, counts in self.crosswalk_counts.items()
            }
            self.crosswalk_counts = {
                cw["key"]: prev_counts.get(
                    cw["key"], {"pedestrians": 0, "cyclists": 0}
                )
                for cw in normalized
            }
            self._track_sides = {}
            self._track_last_seen = {}

    def get_crosswalk_config(self) -> List[Dict[str, object]]:
        """Return a copy of the current crosswalk configuration."""
        with self.crosswalk_lock:
            snapshot = [
                {
                    "key": cw["key"],
                    "name": cw["name"],
                    "p1": tuple(cw["p1"]),
                    "p2": tuple(cw["p2"]),
                    "label": (tuple(cw["label"])) if cw.get("label") else None,
                }
                for cw in self.crosswalk_config
            ]
        return snapshot

    def _get_crosswalk_pixels(
        self, frame_shape: Tuple[int, int, int]
    ) -> List[Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]]:
        """Return cached crosswalk endpoints (in pixels) for the given frame shape."""
        h, w = frame_shape[:2]
        shape_key = (w, h)
        if self._crosswalk_cache_shape != shape_key:
            with self.crosswalk_lock:
                config_snapshot = [dict(cw) for cw in self.crosswalk_config]
            self._crosswalk_cache_shape = shape_key
            self._crosswalk_cache_pixels = []
            for cw in config_snapshot:
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
    initial_crosswalks = worker.get_crosswalk_config()

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
    slider_order: List[str] = []
    slider_inputs: List[Input] = []
    slider_items = []
    for cw in initial_crosswalks:
        key = cw["key"]
        slider_order.append(key)
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
                                    id=f"crosswalk-{key}-ped",
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
                                    id=f"crosswalk-{key}-cyc",
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

        slider_items.append(
            dbc.AccordionItem(
                [
                    html.Div(
                        [
                            html.Div("Start X (p1.x)", className="small text-muted"),
                            dcc.Slider(
                                id=f"slider-{key}-p1x",
                                min=0.0,
                                max=1.0,
                                step=0.005,
                                value=float(cw["p1"][0]),
                                tooltip={"always_visible": True, "placement": "bottom"},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        [
                            html.Div("Start Y (p1.y)", className="small text-muted"),
                            dcc.Slider(
                                id=f"slider-{key}-p1y",
                                min=0.0,
                                max=1.0,
                                step=0.005,
                                value=float(cw["p1"][1]),
                                tooltip={"always_visible": True, "placement": "bottom"},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        [
                            html.Div("End X (p2.x)", className="small text-muted"),
                            dcc.Slider(
                                id=f"slider-{key}-p2x",
                                min=0.0,
                                max=1.0,
                                step=0.005,
                                value=float(cw["p2"][0]),
                                tooltip={"always_visible": True, "placement": "bottom"},
                            ),
                        ],
                        className="mb-3",
                    ),
                    html.Div(
                        [
                            html.Div("End Y (p2.y)", className="small text-muted"),
                            dcc.Slider(
                                id=f"slider-{key}-p2y",
                                min=0.0,
                                max=1.0,
                                step=0.005,
                                value=float(cw["p2"][1]),
                                tooltip={"always_visible": True, "placement": "bottom"},
                            ),
                        ],
                        className="mb-1",
                    ),
                ],
                title=cw["name"],
                item_id=key,
            )
        )

        slider_inputs.extend(
            [
                Input(f"slider-{key}-p1x", "value"),
                Input(f"slider-{key}-p1y", "value"),
                Input(f"slider-{key}-p2x", "value"),
                Input(f"slider-{key}-p2y", "value"),
            ]
        )

    crosswalk_store_payload = [
        {
            "key": cw["key"],
            "name": cw["name"],
            "p1": [float(cw["p1"][0]), float(cw["p1"][1])],
            "p2": [float(cw["p2"][0]), float(cw["p2"][1])],
        }
        for cw in initial_crosswalks
    ]

    crosswalk_line_controls = dbc.Card(
        dbc.CardBody(
            [
                html.H5("Adjust Crosswalk Lines"),
                html.P(
                    "Drag the sliders to reposition each crosswalk line. Values are normalized to the video frame.",
                    className="text-muted",
                ),
                dbc.Accordion(
                    slider_items,
                    start_collapsed=True,
                    flush=True,
                    id="crosswalk-adjust-accordion",
                ),
                dcc.Store(id="crosswalk-config-store", data=crosswalk_store_payload),
            ]
        ),
        className="mt-3 shadow-sm",
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
                                [
                                    html.Img(
                                        src=route_path,
                                        style={"width": "100%", "borderRadius": "12px"},
                                    ),
                                    crosswalk_line_controls,
                                ],
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
            for cw in initial_crosswalks
        ],
        *[
            Output(f"crosswalk-{cw['key']}-cyc", "children")
            for cw in initial_crosswalks
        ],
        Input("stat-timer", "n_intervals"),
        prevent_initial_call=False,
    )
    def _update_stats(_):
        worker.start()
        ped, cyc, start_str, crosswalk_counts = worker.get_stats()
        outputs: List[str] = [str(ped), str(cyc), (start_str or "—")]
        for cw in initial_crosswalks:
            counts = crosswalk_counts.get(
                cw["key"], {"pedestrians": 0, "cyclists": 0}
            )
            outputs.append(str(counts.get("pedestrians", 0)))
        for cw in initial_crosswalks:
            counts = crosswalk_counts.get(
                cw["key"], {"pedestrians": 0, "cyclists": 0}
            )
            outputs.append(str(counts.get("cyclists", 0)))
        return outputs

    @app.callback(
        Output("crosswalk-config-store", "data"),
        [*slider_inputs],
        prevent_initial_call=False,
    )
    def _apply_crosswalk_adjustments(*slider_values):
        worker.start()
        current_by_key = {cw["key"]: cw for cw in worker.get_crosswalk_config()}
        values_iter = iter(slider_values)
        updated_config = []
        for key in slider_order:
            base = current_by_key.get(key, {})
            base_p1 = base.get("p1", (0.0, 0.0))
            base_p2 = base.get("p2", (1.0, 1.0))

            x1 = next(values_iter, None)
            y1 = next(values_iter, None)
            x2 = next(values_iter, None)
            y2 = next(values_iter, None)

            if x1 is None:
                x1 = base_p1[0]
            if y1 is None:
                y1 = base_p1[1]
            if x2 is None:
                x2 = base_p2[0]
            if y2 is None:
                y2 = base_p2[1]

            updated_config.append(
                {
                    "key": key,
                    "name": base.get("name", f"{key.title()} Crosswalk"),
                    "p1": (float(x1), float(y1)),
                    "p2": (float(x2), float(y2)),
                }
            )

        if updated_config:
            worker.set_crosswalk_config(updated_config)

        return [
            {
                "key": item["key"],
                "name": item["name"],
                "p1": [item["p1"][0], item["p1"][1]],
                "p2": [item["p2"][0], item["p2"][1]],
            }
            for item in updated_config
        ]

    return app
