# StreamLab-SXA — Low-Latency Secured beX Algorithm Video Streaming
## Bex 7386 Mini-Techlab

```
server ──[Tailscale]──► client
  camera → MJPEG HTTP            receiver + metrics
  metrics → WebSocket            dashboard + logs
```

---

## Architecture

| Component | File | Role |
|---|---|---|
| Server | `server.py` | Camera capture, MJPEG stream, metrics WS |
| Client | `client.py` | Receiver, adaptive buffer, FEC detection, logger |
| Dashboard | `haruki_dashboard.html` | Real-time metrics UI (browser) |

### Ports (bound to Tailscale IP only — implicitly secure)
| Port | Protocol | Endpoint | Description |
|---|---|---|---|
| 8081 | HTTP | `/stream` | MJPEG video stream |
| 8081 | HTTP | `/health` | Health check JSON |
| 8082 | WS | `/` | Server metrics push (1 Hz) |
| 8090 | HTTP | `/metrics` | Local client metrics (client, localhost only) |

---

## Installation

### server
```bash
pip install opencv-python websockets aiohttp numpy
# If using USB cam, verify: ls /dev/video*
python server.py
```

### client
```bash
pip install opencv-python websockets aiohttp numpy requests
# With display window:
python client.py

# Headless (log only, for remote/SSH):
python client.py --headless

# Custom server IP:
python client.py --server SERVER_IP
```

### Dashboard
Open `haruki_dashboard.html` in browser.
- Server IP: SERVER_IP (set via --server argument)
- Click **CONNECT**
- Dashboard pulls from server WebSocket + local client HTTP (port 8090)

---

## Metrics Collected

### Client-side (`client.py`)
| Metric | Description |
|---|---|
| `fps_client` | Frames received per second |
| `latency_ms` | Server timestamp → client receive delta |
| `jitter_ms` | Variance in inter-frame latency |
| `frame_gap` | Missing frame IDs between received frames |
| `lost_frames_est` | Cumulative estimated lost frames |
| `parity_frames` | FEC parity frames received |
| `decode_time_ms` | JPEG decode time |
| `frame_size_kb` | Per-frame JPEG size |
| `bitrate_kbps` | Estimated receive bitrate |
| `buffer_depth` | Current adaptive buffer fill |
| `reconnects` | Number of stream reconnections |

### Server-side (`server.py` via WS)
| Metric | Description |
|---|---|
| `fps_capture` | Camera capture FPS |
| `fps_stream` | Frames actually streamed per second |
| `encode_time_ms` | JPEG encode time per frame |
| `avg_frame_size_kb` | Rolling average frame size |
| `bitrate_kbps` | Server-estimated stream bitrate |
| `clients_mjpeg` | Active MJPEG connections |
| `frames_total` | Total frames sent since start |

---

## Log Outputs (client)
```
logs/
  client_YYYYMMDD_HHMMSS.log            # plaintext event log
  metrics_YYYYMMDD_HHMMSS.csv           # structured metrics (1 row/sec)
  metrics_YYYYMMDD_HHMMSS.jsonl         # same in JSON Lines format
```

CSV columns:
```
timestamp_utc, uptime_s, fps_client, fps_server, latency_ms, jitter_ms,
frame_id, frame_gap, parity_frames, lost_frames_est, frame_size_kb,
bitrate_kbps, buffer_depth, decode_time_ms, server_encode_ms,
server_bitrate_kbps, reconnects
```

---

## FEC Strategy

Lightweight **mark-based FEC** (no complex Reed-Solomon):
- Server marks every N-th frame (default: 5) as a **parity frame** via header `X-Is-Parity: 1`
- Client detects frame ID gaps → counts as lost frames
- Client logs parity frame count; gaps between parity frames help bound loss window
- For full retransmit-based FEC: extend with a request channel (future: WS command back to server)

Adjust `FEC_INTERVAL` in `server.py`:
- Lower (e.g. 3) = more parity markers, easier loss detection
- Higher (e.g. 10) = less overhead

---

## Adaptive Buffer

Client auto-adjusts buffer target based on jitter:
- Jitter > 20ms → increase buffer (up to `BUFFER_MAX = 10`)
- Jitter < 5ms  → decrease buffer (down to `BUFFER_MIN = 2`)
- Initial target: 3 frames

This reduces freeze artifacts on jittery links while minimizing latency on clean links.

---

## Tuning for Research

### Latency minimization
```python
# server.py
JPEG_QUALITY = 60    # reduce: smaller frames, lower encode time
TARGET_FPS   = 15    # reduce: easier for slow links
# client.py minimum buffer: adjust BUFFER_MIN in client.py
```

### Quality / bandwidth study
```python
# Sweep JPEG_QUALITY: 40, 55, 70, 85 and log frame_size_kb + latency_ms
# Already logged to CSV — just change and restart, compare sessions
```

### Loss simulation (for research)
```bash
# On client, inject 5% packet loss via tc (traffic control):
sudo tc qdisc add dev tailscale0 root netem loss 5%
# Restore:
sudo tc qdisc del dev tailscale0 root
```

---

## Security

- All services bind **exclusively to Tailscale IP** (`SERVER_IP`)
- Tailscale handles WireGuard encryption end-to-end
- No auth layer needed at app level — only Tailscale-enrolled devices can reach the ports
- Verify binding: `ss -tlnp | grep 808`

---

## Troubleshooting

| Problem | Check |
|---|---|
| `Cannot open camera` | `ls /dev/video*`, try index 1 or 2 |
| High latency | Reduce `JPEG_QUALITY`, check Tailscale route (`tailscale ping 100.95.226.56`) |
| No WS metrics | Check port 8081 not blocked, check `izumi_server.py` logs |
| Dashboard no video | Browser may block mixed HTTP in HTTPS context — use `http://` page or local file |
| CSV missing rows | Check disk space: `df -h` |
