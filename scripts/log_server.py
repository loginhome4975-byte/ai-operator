#!/usr/bin/env python3
"""
Log Dashboard Server — 3 ta Kaggle node uchun web log ko'rish paneli.

Ishlatish:
    python3 scripts/log_server.py                    # port 8099 da ishga tushadi
    python3 scripts/log_server.py --port 8099        # boshqa port

Keyin brauzerda: http://localhost:8099
"""

import argparse
import asyncio
import json
import os
import queue
import sys
import threading
import time
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

# ── .env yuklash ──
load_dotenv()

# ── Node config ──
NODES = {
    0: {
        "label": "Node-0 (LLM+TTS UZ)",
        "kernel": "bunyodbek7/ai-operator-kaggle-node",
        "user_env": "KAGGLE_USERNAME",
        "key_env": "KAGGLE_KEY",
        "color": "#22c55e",
    },
    1: {
        "label": "Node-1 (STT RU+TTS)",
        "kernel": "bunyodozodboyev/ai-operator-kaggle-node-1",
        "user_env": "KAGGLE_USERNAME_1",
        "key_env": "KAGGLE_KEY_1",
        "color": "#06b6d4",
    },
    2: {
        "label": "Node-2 (STT EN+UZ)",
        "kernel": "bunyodbekozodboyev/ai-operator-kaggle-node-2",
        "user_env": "KAGGLE_USERNAME_2",
        "key_env": "KAGGLE_KEY_2",
        "color": "#a855f7",
    },
}

MAX_LINES = 500  # har bir node uchun maksimal qatorlar

# ── Har bir node uchun log buffer ──
buffers: dict[int, list[str]] = {0: [], 1: [], 2: []}
subscribers: dict[int, list[queue.Queue]] = {0: [], 1: [], 2: []}
node_status: dict[int, str] = {0: "connecting", 1: "connecting", 2: "connecting"}


def _get_env(node_id: int) -> dict:
    """Node'ga mos environment yaratish."""
    cfg = NODES[node_id]
    env = os.environ.copy()
    user = os.environ.get(cfg["user_env"], "")
    key = os.environ.get(cfg["key_env"], "")
    env["KAGGLE_USERNAME"] = user
    env["KAGGLE_KEY"] = key
    # Token transform
    env.pop("KAGGLE_API_TOKEN", None)
    suffix = "" if node_id == 0 else f"_{node_id}"
    token = os.environ.get(f"KAGGLE_API_TOKEN{suffix}") or os.environ.get(f"KAGGLE_KEY{suffix}", "")
    if token:
        env["KAGGLE_API_TOKEN"] = token
    return env


def _stream_kaggle(node_id: int):
    """Kaggle kernel log'larini o'qib, buffer va subscriber'larga yozadi."""
    import subprocess

    cfg = NODES[node_id]
    kernel = cfg["kernel"]
    env = _get_env(node_id)

    node_status[node_id] = "connecting"

    try:
        proc = subprocess.Popen(
            ["kaggle", "kernels", "logs", kernel, "-f"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1,
        )
        node_status[node_id] = "running"
        started_at = time.strftime("%H:%M:%S")

        # Birinchi qator: kernel info
        info_line = f"── {cfg['label']} | {kernel} | ishga tushdi: {started_at} ──"
        buffers[node_id].append(info_line)
        for q in subscribers[node_id]:
            try:
                q.put_nowait({"type": "line", "data": info_line})
            except queue.Full:
                pass

        for raw in iter(proc.stdout.readline, ""):
            line = raw.rstrip("\n").rstrip("\r")
            if not line:
                continue

            # Log spam filtrlash
            skip = (
                "/health" in line
                or "TQDM_DISABLE" in line
                or "TRANSFORMERS_VERBOSITY" in line
            )
            if skip:
                continue

            buffers[node_id].append(line)
            if len(buffers[node_id]) > MAX_LINES:
                buffers[node_id] = buffers[node_id][-MAX_LINES:]

            for q in subscribers[node_id]:
                try:
                    q.put_nowait({"type": "line", "data": line})
                except queue.Full:
                    pass

        proc.wait(timeout=5)
        status = f"ended (exit={proc.returncode})"
    except FileNotFoundError:
        status = "kaggle CLI topilmadi"
    except Exception as e:
        status = f"xatolik: {e}"

    node_status[node_id] = status
    msg = f"── Stream to'xtadi: {status} ──"
    buffers[node_id].append(msg)
    for q in subscribers[node_id]:
        try:
            q.put_nowait({"type": "line", "data": msg})
        except queue.Full:
            pass


# ── Stream thread'larni ishga tushirish ──
def _start_streams():
    for nid in [0, 1, 2]:
        cfg = NODES[nid]
        key = os.environ.get(cfg["key_env"], "")
        if not key:
            node_status[nid] = f"kalit yo'q ({cfg['key_env']})"
            buffers[nid].append(f"⚠️  Kaggle kaliti topilmadi: {cfg['key_env']}")
            continue
        t = threading.Thread(target=_stream_kaggle, args=(nid,), daemon=True)
        t.start()


# ── FastAPI app ──
app = FastAPI(title="Kaggle Log Dashboard")

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kaggle Node Loglar</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { background: #0f172a; color: #e2e8f0; font-family: 'JetBrains Mono', 'Fira Code', monospace; height: 100vh; display: flex; flex-direction: column; }
header { background: #1e293b; padding: 10px 20px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid #334155; }
header h1 { font-size: 16px; color: #f8fafc; }
header .status { display: flex; gap: 16px; align-items: center; }
header .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
header .dot.green { background: #22c55e; }
header .dot.red { background: #ef4444; }
header .dot.yellow { background: #eab308; }
header button { background: #334155; color: #e2e8f0; border: 1px solid #475569; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 13px; transition: all .2s; }
header button:hover { background: #475569; }
header .time { font-size: 12px; color: #94a3b8; }
.grid { display: grid; grid-template-columns: 1fr 1fr 1fr; flex: 1; overflow: hidden; gap: 2px; background: #334155; }
.panel { display: flex; flex-direction: column; background: #1e293b; }
.panel-header { padding: 8px 12px; border-bottom: 2px solid var(--c); display: flex; justify-content: space-between; align-items: center; font-size: 13px; font-weight: bold; }
.panel-header .label { color: var(--c); }
.panel-header .meta { font-size: 11px; color: #94a3b8; }
.log-box { flex: 1; overflow-y: auto; padding: 8px 12px; font-size: 12px; line-height: 1.6; word-break: break-all; white-space: pre-wrap; }
.log-box::-webkit-scrollbar { width: 4px; }
.log-box::-webkit-scrollbar-thumb { background: #475569; border-radius: 2px; }
.log-box .error { color: #f87171; }
.log-box .warn { color: #fbbf24; }
.log-box .info { color: #94a3b8; }
.log-box .dim { color: #64748b; }
</style>
</head>
<body>
<header>
  <h1>📊 Kaggle Node Log Dashboard</h1>
  <div class="status">
    <span class="time" id="clock">--</span>
    <span id="st0"><span class="dot red"></span>Node-0</span>
    <span id="st1"><span class="dot red"></span>Node-1</span>
    <span id="st2"><span class="dot red"></span>Node-2</span>
    <button onclick="refreshAll()">🔄 Yangilash</button>
  </div>
</header>
<div class="grid">
  <div class="panel" id="panel0">
    <div class="panel-header" style="--c:#22c55e">
      <span class="label">Node-0 (LLM+TTS UZ)</span>
      <span class="meta" id="meta0">ulanilmoqda...</span>
    </div>
    <div class="log-box" id="log0"></div>
  </div>
  <div class="panel" id="panel1">
    <div class="panel-header" style="--c:#06b6d4">
      <span class="label">Node-1 (STT RU+TTS)</span>
      <span class="meta" id="meta1">ulanilmoqda...</span>
    </div>
    <div class="log-box" id="log1"></div>
  </div>
  <div class="panel" id="panel2">
    <div class="panel-header" style="--c:#a855f7">
      <span class="label">Node-2 (STT EN+UZ)</span>
      <span class="meta" id="meta2">ulanilmoqda...</span>
    </div>
    <div class="log-box" id="log2"></div>
  </div>
</div>
<script>
const COLORS = {0:'#22c55e',1:'#06b6d4',2:'#a855f7'};
const SOURCES = {};
let AUTO_SCROLL = {0:true,1:true,2:true};

function colorize(line) {
  if (/ERROR|XATO|xatolik|Xatolik|❌/.test(line)) return '<span class="error">'+line+'</span>';
  if (/WARNING|WARN|⚠/.test(line)) return '<span class="warn">'+line+'</span>';
  if (/INFO|✅|🔌|─/.test(line)) return '<span class="info">'+line+'</span>';
  return '<span class="dim">'+line+'</span>';
}

async function connect(nodeId) {
  if (SOURCES[nodeId]) { SOURCES[nodeId].close(); }
  const es = new EventSource('/stream/'+nodeId);
  SOURCES[nodeId] = es;
  const box = document.getElementById('log'+nodeId);
  const meta = document.getElementById('meta'+nodeId);
  const st = document.getElementById('st'+nodeId);
  let lastHeight = 0;

  es.onmessage = e => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'line') {
        box.innerHTML += colorize(msg.data) + '\\n';
        // status
        if (msg.data.includes('ishga tushdi:')) {
          const m = msg.data.match(/tushdi: (\\d{2}:\\d{2}:\\d{2})/);
          if (m) meta.textContent = 'ochildi: '+m[1];
          st.innerHTML = '<span class="dot green"></span>Node-'+nodeId;
        }
        if (msg.data.includes('toxtadi:')) {
          st.innerHTML = '<span class="dot red"></span>Node-'+nodeId;
        }
        // Auto-scroll — agar user pastda bo'lsa
        if (AUTO_SCROLL[nodeId]) {
          box.scrollTop = box.scrollHeight;
        }
      }
      if (msg.type === 'full') {
        box.innerHTML = msg.data.split('\\n').map(colorize).join('\\n');
        if (AUTO_SCROLL[nodeId]) box.scrollTop = box.scrollHeight;
      }
    } catch(ex) {}
  };
  es.onerror = () => { st.innerHTML = '<span class="dot yellow"></span>Node-'+nodeId; };
}

// Scroll tracking
[0,1,2].forEach(id => {
  const box = document.getElementById('log'+id);
  box.addEventListener('scroll', () => {
    AUTO_SCROLL[id] = (box.scrollHeight - box.scrollTop - box.clientHeight) < 50;
  });
});

// Soat
setInterval(() => {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString('uz-UZ');
}, 1000);

function refreshAll() {
  [0,1,2].forEach(id => {
    document.getElementById('log'+id).innerHTML = '';
    connect(id);
  });
}

// Init
refreshAll();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return DASHBOARD_HTML


@app.get("/stream/{node_id}")
async def stream_logs(node_id: int, request: Request):
    """SSE endpoint — har bir node uchun real-time log stream."""
    if node_id not in (0, 1, 2):
        return StreamingResponse(iter([]), media_type="text/event-stream")

    q: queue.Queue = queue.Queue(maxsize=200)
    subscribers[node_id].append(q)

    async def generate():
        # Avval mavjud log'larni yuborish
        if buffers[node_id]:
            msg = json.dumps({"type": "full", "data": "\n".join(buffers[node_id])})
            yield f"data: {msg}\n\n"

        # Keyin real-time
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = q.get(timeout=3)
                    yield f"data: {json.dumps(msg)}\n\n"
                except queue.Empty:
                    yield ":\n\n"  # keep-alive
        except asyncio.CancelledError:
            pass
        finally:
            if q in subscribers[node_id]:
                subscribers[node_id].remove(q)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/api/status")
async def status():
    return {
        str(k): {"status": node_status[k], "lines": len(buffers[k])}
        for k in [0, 1, 2]
    }


def main():
    parser = argparse.ArgumentParser(description="Kaggle Log Dashboard Server")
    parser.add_argument("--port", type=int, default=8099, help="Port (default: 8099)")
    args = parser.parse_args()

    print(f"""
╔══════════════════════════════════════════════╗
║   📊 Kaggle Log Dashboard                   ║
║   http://localhost:{args.port:<5}                    ║
║   Ctrl+C = to'xtatish                        ║
╚══════════════════════════════════════════════╝
""")

    _start_streams()
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
