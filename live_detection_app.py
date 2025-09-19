"""Dash module that streams a live YOLO detection feed within the gateway."""

from __future__ import annotations

import os
import time
import threading
from typing import Optional

import cv2
from flask import Response
from ultralytics import YOLO

import dash
from dash import html
import dash_bootstrap_components as dbc

from theme import card, dash_page

# ── Config ────────────────────────────────────────────────────────────────────
# RTSP_URL = os.getenv(
#     "YOLO_RTSP_URL",
#     "http://root:Wisdot2018!@63.43.111.221:8881/axis-cgi/media.cgi?"
#     "audiocodec=aac&audiosamplerate=16000&audiobitrate=32000&camera=1&"
#     "videoframeskipmode=empty&videozprofile=classic&resolution=1920x1080&fps=30&"
#     "audiodeviceid=0&audioinputid=0&timestamp=0&videocodec=h264&container=mp4",
# )
RTSP_URL = "rtsp://trafficstudy:q;3Gq85RHL+G@10.1.52.24/axis-media/media.amp?videocodec=h264&camera=1"
MODEL_PATH = os.getenv("YOLO_MODEL", "yolo11n.pt")
TARGET_WIDTH = int(os.getenv("YOLO_TARGET_WIDTH", 960))
SCORE_THRESH = float(os.getenv("YOLO_SCORE_THRESH", 0.4))
FRAME_SKIP = int(os.getenv("YOLO_FRAME_SKIP", 0))
READ_TIMEOUT_SEC = float(os.getenv("YOLO_READ_TIMEOUT", 8))


class VideoWorker:
    """Background thread that maintains a connection to the video stream."""

    def __init__(self, rtsp_url: str, model_path: str) -> None:
        self.rtsp_url = rtsp_url
        self.model = YOLO(model_path)
        self.cap: Optional[cv2.VideoCapture] = None
        self.frame_lock = threading.Lock()
        self.latest_jpeg: Optional[bytes] = None
        self.stop_flag = threading.Event()
        self.frame_count = 0
        self._started = threading.Event()

    def connect(self) -> None:
        """Connect to the RTSP/HTTP video source."""
        self.cap = cv2.VideoCapture(self.rtsp_url, cv2.CAP_FFMPEG)
        if not self.cap.isOpened():
            raise RuntimeError("Failed to open RTSP stream")
        self.cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    def loop(self) -> None:
        """Continuously read frames, run YOLO inference and store JPEG output."""
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
                    frame,
                    (TARGET_WIDTH, int(h * scale)),
                    interpolation=cv2.INTER_AREA,
                )

            results = self.model.predict(
                frame,
                verbose=False,
                conf=SCORE_THRESH,
                imgsz=frame.shape[1],
            )
            annotated = results[0].plot()

            ok, jpg = cv2.imencode(".jpg", annotated, [cv2.IMWRITE_JPEG_QUALITY, 80])
            if ok:
                with self.frame_lock:
                    self.latest_jpeg = jpg.tobytes()

    def get_jpeg(self) -> Optional[bytes]:
        with self.frame_lock:
            return self.latest_jpeg

    def start(self) -> None:
        """Start the background worker once."""
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

    app.layout = dash_page(
        "Long Term Counts · Live Detection",
        [
            card(
                [
                    html.H3("N Maryland Avenue - UW Milwaukee Campus"),
                    html.P("Streaming from RTSP/HTTP with on-the-fly YOLO inference."),
                    html.P("NOTE: This is a demonstration to show live object detection."),
                    html.Img(
                        src=route_path,
                        style={"width": "100%", "borderRadius": "12px"},
                    ),
                    html.Div(
                        "Tip: If playback stutters, reduce TARGET_WIDTH or process with a GPU.",
                        className="mt-3 text-muted",
                    ),
                ],
                class_name="app-card--wide",
            )
        ],
    )

    return app
