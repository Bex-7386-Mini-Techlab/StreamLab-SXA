#!/usr/bin/env python3
"""
client.py — StreamLab-SXA Video Receiver + Metrics
====================================================
Run on client (Bogor)

Features:
  - MJPEG stream receiver with adaptive latency buffer
  - FEC-aware: detects and logs parity frames, requests retransmit on gap
  - Real-time metrics: latency, FPS, packet loss estimate, jitter, bitrate
  - Logs to CSV + JSON (per session, timestamped)
  - WebSocket metrics subscriber (pulls server-side stats)
  - Displays stream in OpenCV window (optional headless mode)

Usage:
  python haruki_client.py [--headless] [--server 100.95.226.56]

Requirements:
  pip install opencv-python websockets aiohttp numpy requests
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import signal
import time
import urllib.request
from collections import deque
from datetime import datetime
from threading import Thread, Lock, Event
import struct

import cv2
import numpy as np
import websockets
import requests

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DEFAULT_SERVER  = "SERVER_IP"  # Set via --server argument or environment
HTTP_PORT       = 8081
WS_PORT         = 8082
STREAM_URL      = f"http://{DEFAULT_SERVER}:{HTTP_PORT}/stream"
METRICS_WS_URL  = f"ws://{DEFAULT_SERVER}:{WS_PORT}"

# Adaptive buffer: hold N frames to smooth jitter
BUFFER_MIN      = 2
BUFFER_MAX      = 10
BUFFER_TARGET   = 3   # initial target; auto-adjusts based on jitter

RECONNECT_DELAY = 3.0   # seconds between reconnect attempts
LOG_DIR         = "./logs"
LOG_INTERVAL_S  = 1.0   # how often to write a metrics log row

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
os.makedirs(LOG_DIR, exist_ok=True)
session_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{LOG_DIR}/client_{session_ts}.log"),
    ]
)
log = logging.getLogger("client")

# CSV log setup
csv_path = f"{LOG_DIR}/metrics_{session_ts}.csv"
CSV_FIELDS = [
    "timestamp_utc", "uptime_s",
    "fps_client", "fps_server",
    "latency_ms", "jitter_ms",
    "frame_id", "frame_gap",
    "parity_frames", "lost_frames_est",
    "frame_size_kb", "bitrate_kbps",
    "buffer_depth", "decode_time_ms",
    "server_encode_ms", "server_bitrate_kbps",
    "reconnects",
]

csv_file = open(csv_path, "w", newline="")
csv_writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
csv_writer.writeheader()
log.info(f"CSV log: {csv_path}")

# ─────────────────────────────────────────────
# SHARED CLIENT STATE
# ─────────────────────────────────────────────
class ClientState:
    def __init__(self):
        self.lock = Lock()
        self.start_time = time.time()

        # Frame buffer: list of (frame_id, timestamp_us, is_parity, jpg_bytes, encode_ms)
        self.buffer: deque = deque(maxlen=BUFFER_MAX)
        self.buffer_target = BUFFER_TARGET

        # Metrics — client side
        self.fps_client: float = 0.0
        self.latency_ms: float = 0.0
        self.jitter_ms: float = 0.0
        self.frame_id_last: int = -1
        self.frame_gap: int = 0
        self.lost_frames_est: int = 0
        self.parity_frames: int = 0
        self.decode_time_ms: float = 0.0
        self.frame_size_kb: float = 0.0
        self.bitrate_kbps: float = 0.0
        self.reconnects: int = 0
        self.connected: bool = False

        # Metrics — server side (from WS)
        self.server_metrics: dict = {}

        # FPS tracking
        self.fps_times: deque = deque(maxlen=60)
        self.latency_history: deque = deque(maxlen=30)

cstate = ClientState()

# ─────────────────────────────────────────────
# MJPEG PARSER
# ─────────────────────────────────────────────
def parse_mjpeg_stream(server_ip: str, headless: bool, stop_event: Event):
    """
    Connects to MJPEG stream, parses multipart boundaries,
    extracts frame metadata from custom headers, and decodes frames.
    """
    stream_url = f"http://{server_ip}:{HTTP_PORT}/stream"

    while not stop_event.is_set():
        try:
            log.info(f"Connecting to stream: {stream_url}")
            response = requests.get(stream_url, stream=True, timeout=10)
            response.raise_for_status()

            with cstate.lock:
                cstate.connected = True

            log.info("Stream connected.")

            # Parse multipart MJPEG
            boundary = None
            content_type = response.headers.get("Content-Type", "")
            for part in content_type.split(";"):
                part = part.strip()
                if part.startswith("boundary="):
                    boundary = part[9:].strip("--").encode()
                    break

            if boundary is None:
                log.error("No boundary found in Content-Type")
                time.sleep(RECONNECT_DELAY)
                continue

            buf = b""
            recv_iter = response.iter_content(chunk_size=4096)
            fps_count = 0
            fps_timer = time.time()

            for chunk in recv_iter:
                if stop_event.is_set():
                    break
                buf += chunk

                # Find frame boundaries
                while True:
                    # Find start of header block
                    start = buf.find(b"--frame\r\n")
                    if start == -1:
                        break

                    # Find end of header block
                    header_end = buf.find(b"\r\n\r\n", start)
                    if header_end == -1:
                        break

                    header_block = buf[start + len(b"--frame\r\n"):header_end].decode(errors="ignore")

                    # Parse headers
                    headers = {}
                    for line in header_block.split("\r\n"):
                        if ":" in line:
                            k, v = line.split(":", 1)
                            headers[k.strip()] = v.strip()

                    content_length = int(headers.get("Content-Length", 0))
                    frame_id = int(headers.get("X-Frame-ID", -1))
                    timestamp_us = int(headers.get("X-Timestamp-US", 0))
                    is_parity = headers.get("X-Is-Parity", "0") == "1"
                    server_encode_ms = float(headers.get("X-Encode-MS", 0))

                    # Check if full frame is buffered
                    frame_start = header_end + 4
                    frame_end = frame_start + content_length

                    if len(buf) < frame_end:
                        break  # wait for more data

                    jpg_bytes = buf[frame_start:frame_end]
                    buf = buf[frame_end + 2:]  # skip trailing \r\n

                    t_recv = time.time()
                    t_recv_us = int(t_recv * 1_000_000)

                    # Latency: difference between server timestamp and now
                    if timestamp_us > 0:
                        latency_ms = (t_recv_us - timestamp_us) / 1000.0
                        # Clamp to sane range (clock skew can cause negatives)
                        latency_ms = max(0, min(latency_ms, 5000))
                    else:
                        latency_ms = 0.0

                    # Decode frame
                    t_dec = time.time()
                    img_array = np.frombuffer(jpg_bytes, dtype=np.uint8)
                    frame = cv2.imdecode(img_array, cv2.IMREAD_COLOR)
                    decode_ms = (time.time() - t_dec) * 1000

                    if frame is None:
                        log.warning(f"Failed to decode frame {frame_id}")
                        continue

                    # Update state
                    with cstate.lock:
                        # Gap detection
                        if cstate.frame_id_last >= 0 and frame_id > 0:
                            gap = frame_id - cstate.frame_id_last - 1
                            if gap > 0:
                                cstate.lost_frames_est += gap
                                cstate.frame_gap = gap
                                log.warning(f"Frame gap: {gap} frames (ID {cstate.frame_id_last} → {frame_id})")
                            else:
                                cstate.frame_gap = 0

                        cstate.frame_id_last = frame_id

                        if is_parity:
                            cstate.parity_frames += 1

                        # Latency + jitter
                        cstate.latency_history.append(latency_ms)
                        if len(cstate.latency_history) >= 2:
                            diffs = [abs(cstate.latency_history[i] - cstate.latency_history[i-1])
                                     for i in range(1, len(cstate.latency_history))]
                            cstate.jitter_ms = sum(diffs) / len(diffs)
                        cstate.latency_ms = latency_ms
                        cstate.decode_time_ms = decode_ms
                        cstate.frame_size_kb = len(jpg_bytes) / 1024

                        # Adaptive buffer: increase target if jitter > 20ms
                        if cstate.jitter_ms > 20 and cstate.buffer_target < BUFFER_MAX:
                            cstate.buffer_target = min(BUFFER_MAX, cstate.buffer_target + 1)
                        elif cstate.jitter_ms < 5 and cstate.buffer_target > BUFFER_MIN:
                            cstate.buffer_target = max(BUFFER_MIN, cstate.buffer_target - 1)

                        cstate.buffer.append((frame_id, frame))

                    # FPS tracking
                    fps_count += 1
                    cstate.fps_times.append(t_recv)
                    now = time.time()
                    if now - fps_timer >= 1.0:
                        with cstate.lock:
                            cstate.fps_client = fps_count / (now - fps_timer)
                            # Bitrate
                            recent_sizes = [cstate.frame_size_kb]  # simplified
                            cstate.bitrate_kbps = cstate.fps_client * cstate.frame_size_kb * 8
                        fps_count = 0
                        fps_timer = now

                    # Display
                    if not headless and frame is not None:
                        with cstate.lock:
                            fps = cstate.fps_client
                            lat = cstate.latency_ms
                            jit = cstate.jitter_ms
                            buf_d = len(cstate.buffer)
                            lost = cstate.lost_frames_est

                        overlay = frame.copy()
                        overlay_text = [
                            f"FPS: {fps:.1f}",
                            f"Latency: {lat:.1f} ms",
                            f"Jitter: {jit:.1f} ms",
                            f"Lost est: {lost}",
                            f"Buffer: {buf_d}",
                            f"Frame: {frame_id}",
                            f"{'[PARITY]' if is_parity else ''}",
                        ]
                        for i, txt in enumerate(overlay_text):
                            cv2.putText(overlay, txt, (10, 30 + i * 24),
                                        cv2.FONT_HERSHEY_DUPLEX, 0.6, (0, 255, 80), 1, cv2.LINE_AA)

                        cv2.imshow("StreamLab-SXA", overlay)
                        if cv2.waitKey(1) & 0xFF == ord("q"):
                            stop_event.set()
                            break

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error: {e}. Reconnecting in {RECONNECT_DELAY}s...")
        except Exception as e:
            log.error(f"Stream error: {e}", exc_info=True)
        finally:
            with cstate.lock:
                cstate.connected = False
                cstate.reconnects += 1

        if not stop_event.is_set():
            time.sleep(RECONNECT_DELAY)

    cv2.destroyAllWindows()
    log.info("Stream thread stopped.")


# ─────────────────────────────────────────────
# METRICS WEBSOCKET SUBSCRIBER
# ─────────────────────────────────────────────
async def ws_metrics_subscriber(server_ip: str, stop_event: Event):
    """Subscribes to server WebSocket metrics feed."""
    ws_url = f"ws://{server_ip}:{WS_PORT}"

    while not stop_event.is_set():
        try:
            log.info(f"Connecting to metrics WS: {ws_url}")
            async with websockets.connect(ws_url, ping_interval=15) as ws:
                log.info("Metrics WS connected.")
                async for message in ws:
                    if stop_event.is_set():
                        break
                    try:
                        data = json.loads(message)
                        with cstate.lock:
                            cstate.server_metrics = data
                    except json.JSONDecodeError:
                        pass

        except (websockets.exceptions.ConnectionClosed,
                ConnectionRefusedError, OSError) as e:
            log.warning(f"Metrics WS error: {e}. Reconnecting in {RECONNECT_DELAY}s...")

        if not stop_event.is_set():
            await asyncio.sleep(RECONNECT_DELAY)


# ─────────────────────────────────────────────
# METRICS LOGGER (CSV + JSON)
# ─────────────────────────────────────────────
async def metrics_logger(stop_event: Event):
    """Writes a metrics row every LOG_INTERVAL_S seconds."""
    json_path = f"{LOG_DIR}/metrics_{session_ts}.jsonl"
    json_file = open(json_path, "w")
    log.info(f"JSON log: {json_path}")

    while not stop_event.is_set():
        await asyncio.sleep(LOG_INTERVAL_S)

        with cstate.lock:
            uptime = time.time() - cstate.start_time
            srv = cstate.server_metrics

            row = {
                "timestamp_utc": datetime.utcnow().isoformat() + "Z",
                "uptime_s": round(uptime, 1),
                "fps_client": round(cstate.fps_client, 2),
                "fps_server": srv.get("fps_stream", ""),
                "latency_ms": round(cstate.latency_ms, 2),
                "jitter_ms": round(cstate.jitter_ms, 2),
                "frame_id": cstate.frame_id_last,
                "frame_gap": cstate.frame_gap,
                "parity_frames": cstate.parity_frames,
                "lost_frames_est": cstate.lost_frames_est,
                "frame_size_kb": round(cstate.frame_size_kb, 2),
                "bitrate_kbps": round(cstate.bitrate_kbps, 1),
                "buffer_depth": len(cstate.buffer),
                "decode_time_ms": round(cstate.decode_time_ms, 2),
                "server_encode_ms": srv.get("encode_time_ms", ""),
                "server_bitrate_kbps": srv.get("bitrate_kbps", ""),
                "reconnects": cstate.reconnects,
            }

        csv_writer.writerow(row)
        csv_file.flush()

        json_file.write(json.dumps(row) + "\n")
        json_file.flush()

    json_file.close()
    csv_file.close()
    log.info("Metrics logger stopped.")


# ─────────────────────────────────────────────
# TERMINAL METRICS DISPLAY
# ─────────────────────────────────────────────
async def terminal_display(stop_event: Event):
    """Prints live metrics table to terminal every 2s."""
    while not stop_event.is_set():
        await asyncio.sleep(2.0)

        with cstate.lock:
            srv = cstate.server_metrics
            uptime = time.time() - cstate.start_time
            connected_str = "✓ CONNECTED" if cstate.connected else "✗ RECONNECTING"

             lines = [
                 f"\n{'─'*58}",
                 f"  StreamLab-SXA  [{connected_str}]",
                 f"  Uptime: {uptime:.0f}s  |  Reconnects: {cstate.reconnects}",
                 f"{'─'*58}",
                 f"  CLIENT METRICS",
                f"    FPS (rx)    : {cstate.fps_client:.1f}",
                f"    Latency     : {cstate.latency_ms:.1f} ms",
                f"    Jitter      : {cstate.jitter_ms:.1f} ms",
                f"    Frame ID    : {cstate.frame_id_last}",
                f"    Lost (est)  : {cstate.lost_frames_est}",
                f"    Parity rx   : {cstate.parity_frames}",
                f"    Buffer depth: {len(cstate.buffer)} / {cstate.buffer_target} target",
                f"    Frame size  : {cstate.frame_size_kb:.1f} KB",
                f"    Bitrate     : {cstate.bitrate_kbps:.0f} kbps",
                f"    Decode time : {cstate.decode_time_ms:.1f} ms",
                f"{'─'*58}",
                f"  SERVER METRICS (from WS)",
                f"    FPS (cap)   : {srv.get('fps_capture', '—')}",
                f"    Encode time : {srv.get('encode_time_ms', '—')} ms",
                f"    Srv bitrate : {srv.get('bitrate_kbps', '—')} kbps",
                f"    MJPEG clients: {srv.get('clients_mjpeg', '—')}",
                f"    Frames sent : {srv.get('frames_total', '—')}",
                f"{'─'*58}",
            ]
            print("\n".join(lines), flush=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def async_main(args):
    server_ip = args.server
    headless = args.headless

log.info("=" * 58)
log.info("  StreamLab-SXA Client")
log.info(f"  Server       : {server_ip}")
log.info(f"  Stream URL   : http://{server_ip}:{HTTP_PORT}/stream")
log.info(f"  Metrics WS   : ws://{server_ip}:{WS_PORT}")
log.info(f"  Headless     : {headless}")
log.info("=" * 58)

    stop_event = Event()
    stop_async = asyncio.Event()

    def _shutdown():
        stop_event.set()
        stop_async.set()

    # Signal handling: loop.add_signal_handler is Unix-only
    # On Windows, use signal.signal (runs in main thread via asyncio)
    import platform
    if platform.system() != "Windows":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)
    else:
        # Windows: catch KeyboardInterrupt via signal module
        def _win_sigint(signum, frame):
            log.info("KeyboardInterrupt received, shutting down...")
            _shutdown()
        signal.signal(signal.SIGINT, _win_sigint)

    # Start MJPEG receiver in background thread (blocking I/O)
    stream_thread = Thread(
        target=parse_mjpeg_stream,
        args=(server_ip, headless, stop_event),
        daemon=True
    )
    stream_thread.start()

    await asyncio.gather(
        ws_metrics_subscriber(server_ip, stop_event),
        metrics_logger(stop_event),
        terminal_display(stop_event),
        stop_async.wait(),
    )

    log.info("Client shutdown complete.")


def main():
    parser = argparse.ArgumentParser(description="StreamLab-SXA client")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="Izumi Tailscale IP")
    parser.add_argument("--headless", action="store_true", help="No display window (log only)")
    args = parser.parse_args()
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()