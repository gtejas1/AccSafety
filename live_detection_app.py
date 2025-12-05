"""Dash module that streams a live YOLO detection feed within the gateway.
Only 'person' and 'bicycle' detections are rendered, with cumulative counts shown.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
import threading
import math
import json
from typing import Optional, List, Dict, Tuple, Any
from datetime import datetime, timedelta

import cv2
from flask import Response, send_file
from ultralytics import YOLO

import dash
from dash import html, dcc
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

from sqlalchemy import JSON, bindparam, create_engine, text

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
SAVE_INTERVAL_SEC = int(os.getenv("YOLO_SAVE_INTERVAL_SEC", 300))
DB_URL = os.getenv("YOLO_DB_URL", "postgresql://postgres:gw2ksoft@localhost/TrafficDB")
DB_RETRY_BACKOFF_SEC = 5.0

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
CROSSWALK_NUDGE_STEP = 0.01  # normalized amount each navigation press moves a crosswalk
CROSSWALK_ROTATE_STEP_DEG = 5.0
CROSSWALK_SCALE_STEP = 0.05
CROSSWALK_MIN_LENGTH = 0.05
CROSSWALK_CONFIG_PATH = os.getenv(
    "YOLO_CROSSWALK_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "crosswalk_config.json"),
)

ENGINE: Optional[Any] = None
_ENGINE_LOCK = threading.Lock()
_ENGINE_LAST_FAIL_TS = 0.0
_LOGGER = logging.getLogger(__name__)


def _get_engine() -> Optional[Any]:
    """Return a live SQLAlchemy engine, retrying creation on failures."""

    global ENGINE, _ENGINE_LAST_FAIL_TS

    with _ENGINE_LOCK:
        if ENGINE is not None:
            return ENGINE

        now = time.time()
        # Back off briefly between failures so we don't log-spam, but still
        # surface that persistence is currently unavailable.
        cooldown_remaining = DB_RETRY_BACKOFF_SEC - (now - _ENGINE_LAST_FAIL_TS)
        if cooldown_remaining > 0:
            _LOGGER.warning(
                "live_detection DB engine unavailable; retrying in %.1fs", cooldown_remaining
            )
            return None

        try:
            ENGINE = create_engine(DB_URL, pool_pre_ping=True)
        except Exception as exc:  # pragma: no cover - best effort DB init
            _ENGINE_LAST_FAIL_TS = now
            _LOGGER.warning("Failed to create DB engine: %s", exc)
            return None

        # Ensure schema exists once we have a working engine.
        _ensure_live_detection_table(engine=ENGINE)

        return ENGINE


def _ensure_live_detection_table(engine: Optional[Any] = None) -> None:
    engine = engine or _get_engine()
    if engine is None:
        return
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS live_detection_counts (
                        id SERIAL PRIMARY KEY,
                        interval_start TIMESTAMPTZ NOT NULL,
                        interval_end TIMESTAMPTZ NOT NULL,
                        total_pedestrians INTEGER NOT NULL,
                        total_cyclists INTEGER NOT NULL,
                        crosswalk_counts JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )
            )
    except Exception:
        pass


_ensure_live_detection_table()


def _clamp_norm(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _clamp_point(pair: Tuple[float, float]) -> Tuple[float, float]:
    return (_clamp_norm(pair[0]), _clamp_norm(pair[1]))


def _point_side_of_line(point: Tuple[float, float], p1: Tuple[int, int], p2: Tuple[int, int]) -> float:
    """Return the signed distance (scaled) of a point relative to a directed line."""
    x, y = point
    x1, y1 = p1
    x2, y2 = p2
    return (x - x1) * (y2 - y1) - (y - y1) * (x2 - x1)


def _rotate_point(
    point: Tuple[float, float],
    center: Tuple[float, float],
    angle_rad: float,
) -> Tuple[float, float]:
    """Rotate a normalized point around a center by angle_rad radians."""
    x, y = point
    cx, cy = center
    dx = x - cx
    dy = y - cy
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)
    rx = dx * cos_a - dy * sin_a + cx
    ry = dx * sin_a + dy * cos_a + cy
    return _clamp_point((rx, ry))


def _scale_point(
    point: Tuple[float, float],
    center: Tuple[float, float],
    factor: float,
) -> Tuple[float, float]:
    """Scale a normalized point away from or toward the center by factor."""
    x, y = point
    cx, cy = center
    dx = x - cx
    dy = y - cy
    sx = cx + dx * factor
    sy = cy + dy * factor
    return _clamp_point((sx, sy))


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
        self.save_interval = SAVE_INTERVAL_SEC
        self._last_save_ts = time.time()
        self._pending_totals = {"pedestrians": 0, "cyclists": 0}
        self._pending_crosswalk_counts: Dict[str, Dict[str, int]] = {}
        self._interval_start: Optional[datetime] = None

        self.stop_flag = threading.Event()
        self.frame_count = 0
        self._started = threading.Event()
        self._crosswalk_cache_shape: Optional[Tuple[int, int]] = None
        self._crosswalk_cache_pixels: List[
            Tuple[str, str, Tuple[int, int], Tuple[int, int], Optional[Tuple[int, int]], float]
        ] = []
        self.crosswalk_config_path = CROSSWALK_CONFIG_PATH
        self._config_io_lock = threading.Lock()

        saved_crosswalks = self._load_saved_crosswalk_config()
        if saved_crosswalks:
            self.set_crosswalk_config(saved_crosswalks)
        else:
            self.set_crosswalk_config(CROSSWALK_LINES)

        self._load_persisted_totals()

    # ────────────────────────────────────────────────────────────────────────
    def connect(self) -> None:
        """Connect to RTSP/HTTP source."""
        self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened():
            raise RuntimeError("Failed to open RTSP stream")
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self._reset_tracker_history()

    def _load_saved_crosswalk_config(self) -> Optional[List[Dict[str, Any]]]:
        """Load a persisted crosswalk configuration if available."""

        path = getattr(self, "crosswalk_config_path", None)
        if not path:
            return None

        try:
            with open(path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except FileNotFoundError:
            return None
        except Exception:
            return None

        if not isinstance(payload, list):
            return None

        loaded: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            if not key:
                continue
            name = item.get("name") or f"{key.title()} Crosswalk"
            p1 = item.get("p1")
            p2 = item.get("p2")
            label = item.get("label")
            try:
                if not (isinstance(p1, (list, tuple)) and isinstance(p2, (list, tuple))):
                    continue
                p1_pair = (float(p1[0]), float(p1[1]))
                p2_pair = (float(p2[0]), float(p2[1]))
            except (TypeError, ValueError, IndexError):
                continue

            label_pair: Optional[Tuple[float, float]] = None
            if isinstance(label, (list, tuple)):
                try:
                    label_pair = (float(label[0]), float(label[1]))
                except (TypeError, ValueError, IndexError):
                    label_pair = None

            loaded.append({"key": key, "name": name, "p1": p1_pair, "p2": p2_pair, "label": label_pair})

        if not loaded:
            return None

        return loaded

    def _persist_crosswalk_config(self, config: List[Dict[str, object]]) -> None:
        """Write the crosswalk configuration to disk for reuse."""

        path = getattr(self, "crosswalk_config_path", None)
        if not path:
            return

        payload = []
        for item in config:
            payload.append(
                {
                    "key": item["key"],
                    "name": item["name"],
                    "p1": [float(item["p1"][0]), float(item["p1"][1])],
                    "p2": [float(item["p2"][0]), float(item["p2"][1])],
                    "label": (
                        [float(item["label"][0]), float(item["label"][1])]
                        if item.get("label")
                        else None
                    ),
                }
            )

        tmp_path = f"{path}.tmp"
        try:
            dir_name = os.path.dirname(path)
            if dir_name:
                os.makedirs(dir_name, exist_ok=True)
            with self._config_io_lock:
                with open(tmp_path, "w", encoding="utf-8") as fh:
                    json.dump(payload, fh, indent=2)
                os.replace(tmp_path, path)
        except Exception:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except OSError:
                pass
            return

    def _load_persisted_totals(self) -> None:
        """Initialize counts from any saved database intervals."""

        engine = _get_engine()
        if engine is None:
            return
        try:
            with engine.connect() as conn:
                totals_row = conn.execute(
                    text(
                        """
                        SELECT
                            COALESCE(SUM(total_pedestrians), 0) AS ped,
                            COALESCE(SUM(total_cyclists), 0) AS cyc,
                            MIN(interval_start) AS start_ts
                        FROM live_detection_counts
                        """
                    )
                ).mappings().one()
                crosswalk_payloads = conn.execute(
                    text("SELECT crosswalk_counts FROM live_detection_counts")
                ).scalars().all()
        except Exception:
            return

        ped = int(totals_row.get("ped") or 0)
        cyc = int(totals_row.get("cyc") or 0)
        start_ts = totals_row.get("start_ts")

        crosswalk_totals: Dict[str, Dict[str, int]] = {}
        for payload in crosswalk_payloads:
            if not isinstance(payload, dict):
                continue
            for key, counts in payload.items():
                aggregated = crosswalk_totals.setdefault(
                    key, {"pedestrians": 0, "cyclists": 0}
                )
                aggregated["pedestrians"] += int(counts.get("pedestrians", 0) or 0)
                aggregated["cyclists"] += int(counts.get("cyclists", 0) or 0)

        with self.stats_lock:
            self.total_counts = {"pedestrians": ped, "cyclists": cyc}
            if start_ts and self.start_time is None:
                self.start_time = start_ts
            for key in list(self.crosswalk_counts.keys()):
                saved_counts = crosswalk_totals.get(key)
                if saved_counts:
                    self.crosswalk_counts[key] = saved_counts.copy()

    def _record_pending_deltas_locked(
        self,
        totals_delta: Dict[str, int],
        crosswalk_deltas: Dict[str, Dict[str, int]],
    ) -> None:
        if not totals_delta and not crosswalk_deltas:
            return

        had_pending = any(self._pending_totals.values()) or any(
            (counts.get("pedestrians", 0) or counts.get("cyclists", 0))
            for counts in self._pending_crosswalk_counts.values()
        )

        for group, delta in totals_delta.items():
            if not delta:
                continue
            self._pending_totals[group] = self._pending_totals.get(group, 0) + int(delta)

        for key, delta_counts in crosswalk_deltas.items():
            pending = self._pending_crosswalk_counts.setdefault(
                key, {"pedestrians": 0, "cyclists": 0}
            )
            pending["pedestrians"] += int(delta_counts.get("pedestrians", 0) or 0)
            pending["cyclists"] += int(delta_counts.get("cyclists", 0) or 0)

        if self._interval_start is None:
            self._interval_start = datetime.utcnow()
        if not had_pending:
            self._last_save_ts = time.time()

    def _persist_counts_if_needed_locked(self, force: bool = False) -> None:
        engine = _get_engine()
        if engine is None:
            return

        now = time.time()
        if not force and now - self._last_save_ts < self.save_interval:
            return

        totals_pending = any(self._pending_totals.values())
        crosswalk_pending = any(
            (counts.get("pedestrians", 0) or counts.get("cyclists", 0))
            for counts in self._pending_crosswalk_counts.values()
        )
        if not totals_pending and not crosswalk_pending:
            return

        interval_start = self._interval_start or datetime.utcnow() - timedelta(
            seconds=self.save_interval
        )
        interval_end = datetime.utcnow()
        payload = {
            "interval_start": interval_start,
            "interval_end": interval_end,
            "ped": int(self._pending_totals.get("pedestrians", 0)),
            "cyc": int(self._pending_totals.get("cyclists", 0)),
            "crosswalk": self._pending_crosswalk_counts or {},
        }

        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO live_detection_counts (
                            interval_start,
                            interval_end,
                            total_pedestrians,
                            total_cyclists,
                            crosswalk_counts
                        ) VALUES (
                            :interval_start,
                            :interval_end,
                            :ped,
                            :cyc,
                            :crosswalk
                        )
                        """
                    ).bindparams(bindparam("crosswalk", type_=JSON)),
                    payload,
                )
        except Exception:
            _LOGGER.exception(
                "Failed to persist live detection counts; will retry after reconnect"
            )
            with _ENGINE_LOCK:
                global ENGINE, _ENGINE_LAST_FAIL_TS
                ENGINE = None
                _ENGINE_LAST_FAIL_TS = time.time()
            return

        self._pending_totals = {"pedestrians": 0, "cyclists": 0}
        self._pending_crosswalk_counts = {}
        self._interval_start = interval_end
        self._last_save_ts = now

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
            self._pending_totals = {"pedestrians": 0, "cyclists": 0}
            self._pending_crosswalk_counts = {}
            self._interval_start = None
            self._last_save_ts = time.time()

    def set_crosswalk_config(self, lines: List[Dict[str, object]]) -> None:
        """Replace the crosswalk configuration with new normalized endpoints."""

        normalized: List[Dict[str, object]] = []
        for cw in lines:
            key = str(cw.get("key"))
            if not key:
                continue
            name = cw.get("name") or f"{key.title()} Crosswalk"
            p1 = _clamp_point(cw.get("p1", (0.0, 0.0)))
            p2 = _clamp_point(cw.get("p2", (1.0, 1.0)))
            label = cw.get("label")
            if label is None:
                mid_x = (p1[0] + p2[0]) / 2.0
                mid_y = max(0.0, min(1.0, (p1[1] + p2[1]) / 2.0 - 0.05))
                label = (mid_x, mid_y)
            else:
                label = _clamp_point(label)
            normalized.append({"key": key, "name": name, "p1": p1, "p2": p2, "label": label})

        if not normalized:
            return

        with self.crosswalk_lock:
            self.crosswalk_config = normalized
            self._crosswalk_cache_shape = None
            persist_snapshot = [dict(cw) for cw in normalized]

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

        self._persist_crosswalk_config(persist_snapshot)

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
            crosswalk_deltas: Dict[str, Dict[str, int]] = {}
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
                                delta_entry = crosswalk_deltas.setdefault(
                                    key, {"pedestrians": 0, "cyclists": 0}
                                )
                                delta_entry["pedestrians"] += 1
                            elif group == "cyclist":
                                counts_for_crosswalk["cyclists"] += 1
                                delta_entry = crosswalk_deltas.setdefault(
                                    key, {"pedestrians": 0, "cyclists": 0}
                                )
                                delta_entry["cyclists"] += 1

                for key, delta in deltas.items():
                    if delta:
                        self.total_counts[key] += delta
                if (deltas["pedestrians"] or deltas["cyclists"]) and self.start_time is None:
                    self.start_time = datetime.now()

                self._prune_stale_tracks_locked(timestamp)
                self._record_pending_deltas_locked(deltas, crosswalk_deltas)
                self._persist_counts_if_needed_locked()
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
            with self.stats_lock:
                self._persist_counts_if_needed_locked()

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
        with self.stats_lock:
            self._persist_counts_if_needed_locked(force=True)


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

    def _download_counts():
        engine = _get_engine()
        if engine is None:
            return "Database connection unavailable", 500
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT
                            interval_start,
                            interval_end,
                            total_pedestrians,
                            total_cyclists,
                            crosswalk_counts
                        FROM live_detection_counts
                        ORDER BY interval_start
                        """
                    )
                ).mappings().all()
        except Exception as exc:
            return f"Failed to load counts: {exc}", 500

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "interval_start",
                "interval_end",
                "total_pedestrians",
                "total_cyclists",
                "crosswalk_counts",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.get("interval_start"),
                    row.get("interval_end"),
                    row.get("total_pedestrians"),
                    row.get("total_cyclists"),
                    json.dumps(row.get("crosswalk_counts") or {}),
                ]
            )

        buffer = io.BytesIO(output.getvalue().encode("utf-8"))
        filename = f"live_detection_counts_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.csv"
        return send_file(
            buffer,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename,
        )

    download_endpoint = (
        f"live_detection_download_{prefix.strip('/').replace('/', '_') or 'root'}"
    )
    download_path = f"{prefix}download"
    if download_endpoint not in server.view_functions:
        server.add_url_rule(download_path, endpoint=download_endpoint, view_func=_download_counts)

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
    crosswalk_cards: List[dbc.Col] = []
    crosswalk_name_style = {"fontSize": "0.85rem"}
    crosswalk_value_style = {"fontSize": "1.15rem"}
    for cw in initial_crosswalks:
        key = cw["key"]
        crosswalk_cards.append(
            dbc.Col(
                dbc.Card(
                    dbc.CardBody(
                        [
                            html.Div(cw["name"], className="text-muted", style=crosswalk_name_style),
                            html.Div(
                                [
                                    html.Span(
                                        "Pedestrians",
                                        className="small text-uppercase text-muted",
                                    ),
                                    html.Span(
                                        "0",
                                        id=f"crosswalk-{key}-ped",
                                        className="fw-bold text-primary",
                                        style=crosswalk_value_style,
                                    ),
                                ],
                                className="d-flex justify-content-between align-items-baseline mt-2",
                            ),
                            html.Div(
                                [
                                    html.Span(
                                        "Cyclists",
                                        className="small text-uppercase text-muted",
                                    ),
                                    html.Span(
                                        "0",
                                        id=f"crosswalk-{key}-cyc",
                                        className="fw-bold text-success",
                                        style=crosswalk_value_style,
                                    ),
                                ],
                                className="d-flex justify-content-between align-items-baseline",
                            ),
                        ]
                    ),
                    className="shadow-sm h-100",
                ),
                className="d-flex",
            )
        )

    direction_icons = {"north": "↑", "south": "↓", "west": "←", "east": "→"}
    direction_titles = {
        "north": "Nudge the line upward",
        "south": "Nudge the line downward",
        "west": "Nudge the line to the left",
        "east": "Nudge the line to the right",
    }
    direction_vectors = {
        "north": (0.0, -CROSSWALK_NUDGE_STEP),
        "south": (0.0, CROSSWALK_NUDGE_STEP),
        "west": (-CROSSWALK_NUDGE_STEP, 0.0),
        "east": (CROSSWALK_NUDGE_STEP, 0.0),
    }
    rotation_titles = {
        "rotate-left": "Rotate counter-clockwise by 5 degrees",
        "rotate-right": "Rotate clockwise by 5 degrees",
    }
    scale_titles = {
        "expand": "Lengthen this crosswalk line by 5%",
        "shrink": "Shorten this crosswalk line by 5%",
    }

    control_items: List[dbc.AccordionItem] = []
    button_inputs: List[Input] = []
    button_lookup: Dict[str, Tuple[str, str]] = {}

    for cw in initial_crosswalks:
        key = cw["key"]
        control_items.append(
            dbc.AccordionItem(
                [
                    html.P(
                        "Use the controls to nudge, rotate, or resize this crosswalk line. Move presses shift endpoints by 1%, rotation adjusts by 5°, and resize changes the line length by 5%.",
                        className="small text-muted",
                    ),
                    html.Div(
                        [
                            html.Div(
                                [
                                    dbc.Button(
                                        direction_icons["north"],
                                        id=f"btn-{key}-north",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=direction_titles["north"],
                                    ),
                                    html.Div(
                                        [
                                            dbc.Button(
                                                direction_icons["west"],
                                                id=f"btn-{key}-west",
                                                color="secondary",
                                                outline=True,
                                                size="sm",
                                                title=direction_titles["west"],
                                            ),
                                            html.Span(
                                                "Move line",
                                                className="small text-muted",
                                            ),
                                            dbc.Button(
                                                direction_icons["east"],
                                                id=f"btn-{key}-east",
                                                color="secondary",
                                                outline=True,
                                                size="sm",
                                                title=direction_titles["east"],
                                            ),
                                        ],
                                        className="d-flex align-items-center gap-2",
                                    ),
                                    dbc.Button(
                                        direction_icons["south"],
                                        id=f"btn-{key}-south",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=direction_titles["south"],
                                    ),
                                ],
                                className="d-flex flex-column align-items-center gap-2",
                            ),
                            html.Div(
                                [
                                    dbc.Button(
                                        "⟲",
                                        id=f"btn-{key}-rotate-left",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=rotation_titles["rotate-left"],
                                    ),
                                    html.Span(
                                        "Rotate",
                                        className="small text-muted",
                                    ),
                                    dbc.Button(
                                        "⟳",
                                        id=f"btn-{key}-rotate-right",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=rotation_titles["rotate-right"],
                                    ),
                                ],
                                className="d-flex align-items-center gap-2",
                            ),
                            html.Div(
                                [
                                    dbc.Button(
                                        "−",
                                        id=f"btn-{key}-shrink",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=scale_titles["shrink"],
                                    ),
                                    html.Span(
                                        "Resize",
                                        className="small text-muted",
                                    ),
                                    dbc.Button(
                                        "+",
                                        id=f"btn-{key}-expand",
                                        color="secondary",
                                        outline=True,
                                        size="sm",
                                        title=scale_titles["expand"],
                                    ),
                                ],
                                className="d-flex align-items-center gap-2",
                            ),
                        ],
                        className="d-flex flex-column align-items-center gap-3 mt-2",
                    ),
                ],
                title=cw["name"],
                item_id=key,
            )
        )

        for direction in ("north", "west", "east", "south"):
            btn_id = f"btn-{key}-{direction}"
            button_inputs.append(Input(btn_id, "n_clicks"))
            button_lookup[btn_id] = (key, f"move:{direction}")

        for action in ("rotate-left", "rotate-right", "shrink", "expand"):
            btn_id = f"btn-{key}-{action}"
            button_inputs.append(Input(btn_id, "n_clicks"))
            button_lookup[btn_id] = (key, action)

    crosswalk_store_payload = [
        {
            "key": cw["key"],
            "name": cw["name"],
            "p1": [float(cw["p1"][0]), float(cw["p1"][1])],
            "p2": [float(cw["p2"][0]), float(cw["p2"][1])],
            "label": (
                [float(cw["label"][0]), float(cw["label"][1])]
                if cw.get("label")
                else None
            ),
        }
        for cw in initial_crosswalks
    ]

    panel_height = "72vh"

    crosswalk_line_controls = dbc.Card(
        dbc.CardBody(
            [
                html.H5("Adjust Crosswalk Lines"),
                html.P(
                    "Use the controls below to move, rotate, or resize each crosswalk line in small increments.",
                    className="text-muted",
                ),
                dbc.Accordion(
                    control_items,
                    start_collapsed=True,
                    flush=True,
                    id="crosswalk-adjust-accordion",
                ),
                dcc.Store(id="crosswalk-config-store", data=crosswalk_store_payload),
            ]
        ),
        className="shadow-sm h-100 w-100",
        style={"height": panel_height, "overflowY": "auto"},
    )

    metric_body_class = "d-flex flex-column gap-2"
    metric_title_style = {"fontSize": "0.7rem", "letterSpacing": "0.04em"}
    metric_value_style = {"fontSize": "1.45rem"}

    start_time_card = dbc.Card(
        dbc.CardBody(
            [
                html.Small(
                    "Detection Started",
                    className="text-muted text-uppercase",
                    style=metric_title_style,
                ),
                html.H5(
                    id="start-time",
                    className="mb-0 fw-semibold",
                    style={"fontSize": "1.05rem"},
                ),
            ],
            className=metric_body_class,
        ),
        className="shadow-sm h-100",
    )

    ped_card = dbc.Card(
        dbc.CardBody(
            [
                html.Small(
                    "Pedestrians Detected",
                    className="text-muted text-uppercase",
                    style=metric_title_style,
                ),
                html.H4(
                    id="ped-count",
                    className="mb-0 text-primary fw-bold",
                    style=metric_value_style,
                ),
            ],
            className=metric_body_class,
        ),
        className="shadow-sm h-100",
    )

    cyc_card = dbc.Card(
        dbc.CardBody(
            [
                html.Small(
                    "Cyclists Detected",
                    className="text-muted text-uppercase",
                    style=metric_title_style,
                ),
                html.H4(
                    id="cyc-count",
                    className="mb-0 text-success fw-bold",
                    style=metric_value_style,
                ),
            ],
            className=metric_body_class,
        ),
        className="shadow-sm h-100",
    )

    download_button = dbc.Button(
        "Download Saved Counts",
        href=download_path,
        target="_blank",
        color="outline-primary",
        className="ms-auto",
    )

    counts_panel = dbc.Card(
        dbc.CardBody(
            [
                dbc.Row(
                    [
                        dbc.Col(start_time_card, xs=12, md=4),
                        dbc.Col(ped_card, xs=6, md=4),
                        dbc.Col(cyc_card, xs=6, md=4),
                    ],
                    class_name="g-3",
                ),
                html.Div(download_button, className="d-flex justify-content-end"),
                html.Div(
                    "Crosswalk Counts (both directions)",
                    className="text-muted fw-semibold",
                    style={"fontSize": "0.9rem"},
                ),
                dbc.Row(
                    crosswalk_cards,
                    class_name="g-3 row-cols-1 row-cols-sm-2",
                ),
                dcc.Interval(id="stat-timer", interval=1000, n_intervals=0),
            ],
            className="d-flex flex-column gap-3",
        ),
        className="shadow-sm h-100 w-100",
        style={"height": panel_height, "overflowY": "auto"},
    )

    video_panel = dbc.Card(
        html.Div(
            html.Img(
                src=route_path,
                className="w-100 h-100",
                style={
                    "width": "100%",
                    "height": "100%",
                    "objectFit": "cover",
                    "objectPosition": "center",
                },
            ),
            className="w-100 h-100 overflow-hidden",
        ),
        className="shadow-sm h-100 w-100 overflow-hidden",
        style={"height": panel_height},
    )

    app.layout = dash_page(
        "Long Term Counts · Live Detection",
        [
            card(
                [
                    html.H3("N Santa Monica Blvd & Silver Spring Drive - Whitefish Bay, WI"),
                    html.P(
                        "NOTE: Only pedestrians and cyclists are counted when they cross the virtual countline.",
                    ),
                    dbc.Row(
                        [
                            dbc.Col(video_panel, lg=5, className="d-flex"),
                            dbc.Col(counts_panel, lg=4, className="d-flex"),
                            dbc.Col(crosswalk_line_controls, lg=3, className="d-flex"),
                        ],
                        class_name="g-3 gy-4 gy-lg-0 align-items-stretch",
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
        [*button_inputs],
        prevent_initial_call=False,
    )
    def _apply_crosswalk_adjustments(*_unused):
        worker.start()
        ctx = dash.callback_context
        config_snapshot = worker.get_crosswalk_config()

        if not ctx.triggered:
            return [
                {
                    "key": item["key"],
                    "name": item["name"],
                    "p1": [item["p1"][0], item["p1"][1]],
                    "p2": [item["p2"][0], item["p2"][1]],
                    "label": (
                        [item["label"][0], item["label"][1]]
                        if item.get("label")
                        else None
                    ),
                }
                for item in config_snapshot
            ]

        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
        target = button_lookup.get(triggered_id)
        if not target:
            return [
                {
                    "key": item["key"],
                    "name": item["name"],
                    "p1": [item["p1"][0], item["p1"][1]],
                    "p2": [item["p2"][0], item["p2"][1]],
                    "label": (
                        [item["label"][0], item["label"][1]]
                        if item.get("label")
                        else None
                    ),
                }
                for item in config_snapshot
            ]

        key, action = target

        updated_config = []
        for item in config_snapshot:
            if item["key"] == key:
                p1 = (float(item["p1"][0]), float(item["p1"][1]))
                p2 = (float(item["p2"][0]), float(item["p2"][1]))
                label = item.get("label")
                label_tuple = (
                    (float(label[0]), float(label[1])) if label is not None else None
                )

                if action.startswith("move:"):
                    direction = action.split(":", 1)[1]
                    dx, dy = direction_vectors.get(direction, (0.0, 0.0))
                    p1 = (_clamp_norm(p1[0] + dx), _clamp_norm(p1[1] + dy))
                    p2 = (_clamp_norm(p2[0] + dx), _clamp_norm(p2[1] + dy))
                    if label_tuple:
                        label_tuple = (
                            _clamp_norm(label_tuple[0] + dx),
                            _clamp_norm(label_tuple[1] + dy),
                        )
                elif action in {"rotate-left", "rotate-right"}:
                    angle = math.radians(CROSSWALK_ROTATE_STEP_DEG)
                    if action == "rotate-left":
                        angle = -angle
                    center = ((p1[0] + p2[0]) / 2.0, (p1[1] + p2[1]) / 2.0)
                    p1 = _rotate_point(p1, center, angle)
                    p2 = _rotate_point(p2, center, angle)
                    if label_tuple:
                        label_tuple = _rotate_point(label_tuple, center, angle)
                elif action in {"expand", "shrink"}:
                    factor = 1.0 + CROSSWALK_SCALE_STEP
                    if action == "shrink":
                        factor = max(0.0, 1.0 - CROSSWALK_SCALE_STEP)
                    center = ((p1[0] + p2[0]) / 2.0, (p1[1] + p2[1]) / 2.0)
                    new_p1 = _scale_point(p1, center, factor)
                    new_p2 = _scale_point(p2, center, factor)
                    new_length = math.hypot(new_p2[0] - new_p1[0], new_p2[1] - new_p1[1])
                    if new_length >= CROSSWALK_MIN_LENGTH:
                        p1, p2 = new_p1, new_p2
                        if label_tuple:
                            label_tuple = _scale_point(label_tuple, center, factor)
                updated_config.append(
                    {
                        "key": item["key"],
                        "name": item["name"],
                        "p1": p1,
                        "p2": p2,
                        "label": label_tuple,
                    }
                )
            else:
                updated_config.append(
                    {
                        "key": item["key"],
                        "name": item["name"],
                        "p1": tuple(item["p1"]),
                        "p2": tuple(item["p2"]),
                        "label": tuple(item["label"]) if item.get("label") else None,
                    }
                )

        worker.set_crosswalk_config(updated_config)
        refreshed = worker.get_crosswalk_config()

        return [
            {
                "key": item["key"],
                "name": item["name"],
                "p1": [item["p1"][0], item["p1"][1]],
                "p2": [item["p2"][0], item["p2"][1]],
                "label": (
                    [item["label"][0], item["label"][1]]
                    if item.get("label")
                    else None
                ),
            }
            for item in refreshed
        ]

    return app
