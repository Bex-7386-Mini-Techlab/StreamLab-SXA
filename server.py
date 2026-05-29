#!/usr/bin/env python3
"""
server.py — StreamLab-SXA Camera Server
=======================================
Run on server

Services:
   - HTTP MJPEG stream      : http://SERVER_IP:8081/stream
   - WebSocket metrics feed : ws://SERVER_IP:8082/metrics
   - HTTP health endpoint   : http://SERVER_IP:8081/health

Access is implicitly restricted to Tailscale network by binding to Tailscale IP only.

Requirements:
  pip install opencv-python websockets aiohttp numpy
"""

import asyncio
import base64
import json
import logging
import signal
import struct
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread, Lock
import socket
import numpy as np

import cv2
import websockets
from aiohttp import web

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
TAILSCALE_IP   = "SERVER_IP"  # Replace with actual Tailscale IP
HTTP_PORT      = 8081
WS_PORT        = 8082

CAMERA_INDEX   = 0          # /dev/video0; change to RTSP URL if using IP cam
FRAME_WIDTH    = 640
FRAME_HEIGHT   = 480
TARGET_FPS     = 25
JPEG_QUALITY   = 80         # 0-100; lower = smaller packet, less quality
BUFFER_SIZE    = 10         # internal frame ring buffer

# FEC: duplicate every N-th frame as parity (lightweight redundancy)
FEC_INTERVAL   = 5          # send parity frame every N frames

LOG_DIR        = "./logs"
METRICS_WINDOW = 60         # seconds of history for stats

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
import os
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{LOG_DIR}/server_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)
log = logging.getLogger("server")

# ─────────────────────────────────────────────
# SHARED STATE
# ─────────────────────────────────────────────
class ServerState:
    def __init__(self):
        self.lock = Lock()
        self.latest_frame: bytes | None = None
        self.frame_id: int = 0
        self.is_parity: bool = False
        self.cap_fps_actual: float = 0.0
        self.encode_time_ms: float = 0.0
        self.frame_size_bytes: int = 0
        self.clients_connected: int = 0
        self.frames_sent: int = 0
        self.start_time: float = time.time()
        self.fps_history: deque = deque(maxlen=METRICS_WINDOW * TARGET_FPS)
        self.size_history: deque = deque(maxlen=METRICS_WINDOW * TARGET_FPS)

state = ServerState()

# ─────────────────────────────────────────────
# CAMERA CAPTURE THREAD
# ─────────────────────────────────────────────
def camera_thread():
    """Captures frames from camera, encodes to JPEG, stores in state."""
    cap = cv2.VideoCapture(CAMERA_INDEX)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, FRAME_WIDTH)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, FRAME_HEIGHT)
    cap.set(cv2.CAP_PROP_FPS, TARGET_FPS)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)  # minimize internal buffer lag

    if not cap.isOpened():
        log.error(f"Cannot open camera index {CAMERA_INDEX}")
        return

    log.info(f"Camera opened: {FRAME_WIDTH}x{FRAME_HEIGHT} @ {TARGET_FPS}fps")

    frame_interval = 1.0 / TARGET_FPS
    last_time = time.time()
    frame_counter = 0
    fps_timer = time.time()
    fps_count = 0

    encode_params = [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY]

    while True:
        ret, frame = cap.read()
        if not ret:
            log.warning("Frame capture failed, retrying...")
            time.sleep(0.1)
            continue

        t_encode = time.time()
        success, encoded = cv2.imencode(".jpg", frame, encode_params)
        encode_ms = (time.time() - t_encode) * 1000

        if not success:
            continue

        jpg_bytes = encoded.tobytes()
        frame_counter += 1
        fps_count += 1

        # FEC: mark every FEC_INTERVAL-th frame as parity (client can request retransmit)
        is_parity = (frame_counter % FEC_INTERVAL == 0)

        with state.lock:
            state.latest_frame = jpg_bytes
            state.frame_id = frame_counter
            state.is_parity = is_parity
            state.encode_time_ms = encode_ms
            state.frame_size_bytes = len(jpg_bytes)
            state.fps_history.append(time.time())
            state.size_history.append(len(jpg_bytes))

        # Actual FPS measurement
        now = time.time()
        if now - fps_timer >= 1.0:
            with state.lock:
                state.cap_fps_actual = fps_count / (now - fps_timer)
            fps_count = 0
            fps_timer = now

        # Pace to target FPS
        elapsed = time.time() - last_time
        sleep_time = frame_interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)
        last_time = time.time()

    cap.release()

# ─────────────────────────────────────────────
# MJPEG HTTP SERVER (aiohttp)
# ─────────────────────────────────────────────
async def mjpeg_stream(request):
    """Streams MJPEG frames with frame metadata headers."""
    client_ip = request.remote
    log.info(f"MJPEG client connected: {client_ip}")

    with state.lock:
        state.clients_connected += 1

    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "multipart/x-mixed-replace; boundary=--frame",
            "Cache-Control": "no-cache",
            "X-StreamLab-Version": "1.0",
        }
    )
    await response.prepare(request)

    last_frame_id = -1

    try:
        while True:
            with state.lock:
                if state.latest_frame is None or state.frame_id == last_frame_id:
                    frame_data = None
                else:
                    frame_data = state.latest_frame
                    frame_id = state.frame_id
                    is_parity = state.is_parity
                    encode_ms = state.encode_time_ms
                    last_frame_id = frame_id

            if frame_data is None:
                await asyncio.sleep(0.005)
                continue

            timestamp_us = int(time.time() * 1_000_000)

            # Frame header with metadata (parseable by client)
            header = (
                f"--frame\r\n"
                f"Content-Type: image/jpeg\r\n"
                f"Content-Length: {len(frame_data)}\r\n"
                f"X-Frame-ID: {frame_id}\r\n"
                f"X-Timestamp-US: {timestamp_us}\r\n"
                f"X-Is-Parity: {int(is_parity)}\r\n"
                f"X-Encode-MS: {encode_ms:.2f}\r\n"
                f"\r\n"
            ).encode()

            await response.write(header + frame_data + b"\r\n")

            with state.lock:
                state.frames_sent += 1

    except (ConnectionResetError, asyncio.CancelledError):
        log.info(f"MJPEG client disconnected: {client_ip}")
    finally:
        with state.lock:
            state.clients_connected -= 1

    return response


async def health_check(request):
    """Quick health endpoint."""
    with state.lock:
        uptime = time.time() - state.start_time
        data = {
            "status": "ok",
            "uptime_s": round(uptime, 1),
            "fps_actual": round(state.cap_fps_actual, 2),
            "clients": state.clients_connected,
            "frames_sent": state.frames_sent,
        }
    return web.json_response(data)


async def start_http_server():
    app = web.Application()
    app.router.add_get("/stream", mjpeg_stream)
    app.router.add_get("/health", health_check)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, TAILSCALE_IP, HTTP_PORT)
    await site.start()
    log.info(f"MJPEG HTTP server: http://{TAILSCALE_IP}:{HTTP_PORT}/stream")


# ─────────────────────────────────────────────
# WEBSOCKET METRICS SERVER
# ─────────────────────────────────────────────
connected_ws_clients = set()

async def metrics_handler(websocket):
    """Push server-side metrics to connected clients every second."""
    client = websocket.remote_address
    log.info(f"Metrics WS client connected: {client}")
    connected_ws_clients.add(websocket)

    try:
        while True:
            with state.lock:
                now = time.time()
                uptime = now - state.start_time

                # FPS over last second
                recent = [t for t in state.fps_history if now - t <= 1.0]
                fps_live = len(recent)

                # Avg frame size over last 30 frames
                recent_sizes = list(state.size_history)[-30:]
                avg_size_kb = (sum(recent_sizes) / len(recent_sizes) / 1024) if recent_sizes else 0

                # Estimated bitrate kbps
                bitrate_kbps = (fps_live * avg_size_kb * 8)

                metrics = {
                    "ts": datetime.utcnow().isoformat() + "Z",
                    "uptime_s": round(uptime, 1),
                    "fps_capture": round(state.cap_fps_actual, 2),
                    "fps_stream": fps_live,
                    "frame_id": state.frame_id,
                    "frame_size_kb": round(state.frame_size_bytes / 1024, 2),
                    "avg_frame_size_kb": round(avg_size_kb, 2),
                    "bitrate_kbps": round(bitrate_kbps, 1),
                    "encode_time_ms": round(state.encode_time_ms, 2),
                    "clients_mjpeg": state.clients_connected,
                    "clients_ws": len(connected_ws_clients),
                    "frames_total": state.frames_sent,
                    "fec_interval": FEC_INTERVAL,
                    "jpeg_quality": JPEG_QUALITY,
                    "resolution": f"{FRAME_WIDTH}x{FRAME_HEIGHT}",
                }

            await websocket.send(json.dumps(metrics))
            await asyncio.sleep(1.0)

    except websockets.exceptions.ConnectionClosed:
        log.info(f"Metrics WS client disconnected: {client}")
    finally:
        connected_ws_clients.discard(websocket)


async def start_ws_server():
    server = await websockets.serve(
        metrics_handler,
        TAILSCALE_IP,
        WS_PORT,
        ping_interval=20,
        ping_timeout=10,
    )
    log.info(f"Metrics WebSocket server: ws://{TAILSCALE_IP}:{WS_PORT}/metrics")
    return server


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def main():
log.info("=" * 60)
log.info("  StreamLab-SXA Server")
log.info(f"  Tailscale IP : {TAILSCALE_IP}")
log.info(f"  MJPEG stream : http://{TAILSCALE_IP}:{HTTP_PORT}/stream")
log.info(f"  Metrics WS   : ws://{TAILSCALE_IP}:{WS_PORT}")
log.info(f"  Health check : http://{TAILSCALE_IP}:{HTTP_PORT}/health")
log.info("=" * 60)

    # Start camera capture in background thread
    cam_thread = Thread(target=camera_thread, daemon=True)
    cam_thread.start()

    # Give camera a moment to warm up
    await asyncio.sleep(1.0)

    # Start HTTP and WS servers concurrently
    await asyncio.gather(
        start_http_server(),
        start_ws_server(),
    )

    # Keep alive
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    await stop.wait()
    log.info("Server shutting down.")


if __name__ == "__main__":
    asyncio.run(main())
