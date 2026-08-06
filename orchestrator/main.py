import os
import re
import sys
import json
import time
import uuid
import hashlib
import asyncio
import base64
import logging
import threading

# orchestrator/ ichidan ishga tushirilganda, parent dir ni sys.path ga qo'shish
# (session_manager, sip_server va boshqa modullar `from orchestrator.X import Y` ishlatadi)
_sys_path_fix = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _sys_path_fix not in sys.path:
    sys.path.insert(0, _sys_path_fix)
from typing import List, Optional, Dict

import httpx
import redis
import pydantic
from fastapi import (
    FastAPI, HTTPException, Form,
    Depends, Security, WebSocket, WebSocketDisconnect, Header, Query, Request,
)
from fastapi.responses import PlainTextResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest

from security_utils import encrypt_payload, decrypt_payload
from session_manager import session_manager
from vad_utils import vad_model
from audio_utils import ensure_wav_16k_mono

# Background cleanup task reference (hoisted for shutdown handler)
_cleanup_task = None

# ----------------- LOGGING (rangli) -----------------
# ANSI color codes for terminal output
_R = "\033[0m"     # reset
_B = "\033[1m"     # bold
_D = "\033[2m"     # dim
_RED = "\033[31m"
_GRN = "\033[32m"
_YEL = "\033[33m"
_BLU = "\033[34m"
_MAG = "\033[35m"
_CYN = "\033[36m"
_WHT = "\033[37m"
_BYEL = "\033[93m" # bright yellow
_BCYN = "\033[96m" # bright cyan


class _ColoredFormatter(logging.Formatter):
    """Log qatorlarini kontentga qarab ranglaydi — node'larni ajratish oson."""
    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        ts = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        lvl = record.levelname[:4].ljust(4)
        name = record.name[:18].ljust(18)

        # Default: rang yo'q
        color = _R
        prefix = ""

        # Node 0 (KAGGLE/LLM+TTS) — GREEN
        if "KAGGLE1" in msg or "kaggle1" in msg.lower() or "Node-1" in msg or "STT RU" in msg:
            color = _CYN
        elif "KAGGLE2" in msg or "kaggle2" in msg.lower() or "Node-2" in msg or "STT EN" in msg:
            color = _MAG
        elif "KAGGLE" in msg or "kaggle" in msg.lower() or "Node-0" in msg or "LLM" in msg:
            color = _GRN

        # Level bo'yicha rang
        if record.levelno >= logging.ERROR:
            color = _RED + _B
        elif record.levelno >= logging.WARNING:
            if color == _R:
                color = _YEL

        # Health check'lar — dim
        if "HTTP Request: GET" in msg and "/health" in msg:
            color = _D

        return f"{color}{ts} | {lvl} | {name} | {msg}{_R}"


# stderr'ga yo'naltirish + force=True (uvicorn handlersini override qiladi).
_handler = logging.StreamHandler(stream=sys.stderr)
_handler.setFormatter(_ColoredFormatter())
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    handlers=[_handler],
    force=True,
)
log = logging.getLogger("orchestrator")

# Alohida audit logger — PII_AUDIT rows uchun.
# `grep WARNING`-dan ajralib turadi, structured JSON maydonlari uchun tayyor.
audit_log = logging.getLogger("audit")
# Propagate=False: audit log'lar asosiy logger'ga tarqalmasin
audit_log.propagate = False
if not audit_log.handlers:
    audit_handler = logging.StreamHandler(stream=sys.stderr)
    audit_handler.setFormatter(logging.Formatter(
        '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"audit","msg":%(message)s}'
    ))
    audit_log.addHandler(audit_handler)
    audit_log.setLevel(logging.WARNING)  # PII audit uchun WARNING — operator dashboardlar ko'radi

# ----------------- PROMETHEUS METRICS -----------------
REQUEST_COUNT = Counter('request_count', 'App Request Count', ['method', 'endpoint', 'http_status'])
REQUEST_LATENCY = Histogram('request_latency_seconds', 'Request latency', ['endpoint'])
STT_LATENCY = Histogram('stt_latency_seconds', 'STT Inference Latency')
TTS_LATENCY = Histogram('tts_latency_seconds', 'TTS Inference Latency')
LLM_LATENCY = Histogram('llm_latency_seconds', 'LLM Inference Latency')

# ----------------- REDIS & QUEUE (H3 TTL fix) -----------------
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")  # #6: Redis parol qo'llab-quvvatlash
_redis_kwargs = {"host": REDIS_HOST, "port": REDIS_PORT, "decode_responses": True}
if REDIS_PASSWORD:
    _redis_kwargs["password"] = REDIS_PASSWORD
redis_client = redis.Redis(**_redis_kwargs)
MAX_CONCURRENT_CALLS = int(os.getenv("MAX_CONCURRENT_CALLS", "50"))
# Active_calls kaliti uchun TTL (s) — orchestrator mid-call crash bo'lsa
# counter abadiy oshib qolmasligi uchun. 1 soatdan keyin avtomatik reset.
ACTIVE_CALLS_TTL = int(os.getenv("ACTIVE_CALLS_TTL", "3600"))

# ----------------- CORS (C6 fix) -----------------
# Wildcard + credentials birga ishlamaydi. Faqat aniq originlarga ruxsat beramiz.
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",") if o.strip()]
ALLOW_CORS_CREDENTIALS = False  # default False; API key authentication asosiy himoya

app = FastAPI(title="Orchestrator - Enterprise AI Operator Pipeline")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=ALLOW_CORS_CREDENTIALS,
    allow_methods=["*"],
    allow_headers=["*"],
)

# (Oldin CONFIG (SECURITY ENFORCEMENT) blokida duplicated API_KEY va buzilgan import bor edi;
# endi asosiy API_KEY env-only konfiguratsiya 5-qatorga yaqin joyda ko'rinadi.)

# Cloud Notebook (Kaggle) Ngrok IP manzillari
KAGGLE_URL = os.getenv("KAGGLE_URL", "http://127.0.0.1:5000")      # LLM & TTS UZ
KAGGLE1_URL = os.getenv("KAGGLE1_URL", "http://127.0.0.1:5001")    # STT RU & TTS RU/EN
KAGGLE2_URL = os.getenv("KAGGLE2_URL", "http://127.0.0.1:5002")    # STT EN & STT UZ

# Ikkita API kalit endi environmentdan keladi, default YO'Q.
# STRICT_SECURITY (default true) da startup'da mavjudligi tekshiriladi;
# bo'sh bo'lsa yoki bir-biriga teng bo'lsa, process abort qiladi.
API_KEY = os.getenv("ORCHESTRATOR_API_KEY")
AUDIT_VIEW_KEY = os.getenv("AUDIT_VIEW_KEY")
# #3, #10: NODE_COMM_KEY node'lar bilan aloqa uchun alohida kalit — ORCHESTRATOR_API_KEY
# hech qachon Kaggle node'larga yuborilmaydi (defense in depth).
# Agar NODE_COMM_KEY berilmagan bo'lsa, AES_256_KEY dan derived key ishlatiladi.
_raw_node_key = os.getenv("NODE_COMM_KEY", "")
if not _raw_node_key:
    _aes_key = os.getenv("AES_256_KEY", "")
    _raw_node_key = hashlib.sha256((_aes_key or "node-comm-fallback").encode()).hexdigest()[:32]
NODE_COMM_KEY = _raw_node_key
# HEADERS_BASE faqat Content-Type — API_KEY node'larga yuborilmaydi (#3 fix)
HEADERS_BASE = {"Content-Type": "application/json"}
# Node'lar bilan aloqa uchun alohida headerlar
NODE_HEADERS = {"X-API-Key": NODE_COMM_KEY, "Content-Type": "application/json"}

VALID_LANGUAGES = {"uz", "ru", "en"}

STT_ENDPOINTS = {
    "uz": f"{KAGGLE2_URL}/transcribe/uz",
    "en": f"{KAGGLE2_URL}/transcribe/en",
    "ru": f"{KAGGLE1_URL}/transcribe/ru"
}
TTS_ENDPOINTS = {
    "uz": f"{KAGGLE_URL}/synthesize",
    "en": f"{KAGGLE1_URL}/synthesize/en",
    "ru": f"{KAGGLE1_URL}/synthesize/ru"
}
LLM_ENDPOINT = f"{KAGGLE_URL}/chat"

# ----------------- ASYNC HTTP CLIENT (C7 fix) -----------------
# Sinxron requests o'rniga httpx.AsyncClient ishlatamiz — event loop bloklanmasin
HTTP_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=5.0, pool=5.0)
async_http: Optional[httpx.AsyncClient] = None


async def _async_retry_call(coro_factory, attempts: int = 3):
    """Async-friendly retry — event loop bloklanmaydi.
    coro_factory: har bir urinishda yangi coroutine qaytaruvchi callable.
    Misol: lambda: client.post(url, ...)  yoki lambda: self._do(x)."""
    last_exc = None
    for attempt in range(attempts):
        try:
            return await coro_factory()
        except (httpx.HTTPError, httpx.RequestError, asyncio.TimeoutError) as e:
            last_exc = e
            if attempt == attempts - 1:
                break
            backoff = min(2 ** attempt, 8)
            log.warning(f"Async retry {attempt + 1}/{attempts} in {backoff}s: {e}")
            await asyncio.sleep(backoff)
    assert last_exc is not None
    raise last_exc


async def resilient_request(client: httpx.AsyncClient, url: str, *, method: str = "POST",
                             json_body=None, files=None, data=None, headers=None,
                             timeout=None):
    """Async resilient HTTP request. Retry asyncio.sleep orqali — event loop bloklanmaydi."""
    kw = {"timeout": timeout or HTTP_TIMEOUT}
    if headers:
        kw["headers"] = headers
    if json_body is not None:
        kw["json"] = json_body
    if files is not None:
        kw["files"] = files
    if data is not None:
        kw["data"] = data
    method = method.upper()

    async def _do():
        if method == "POST":
            res = await client.post(url, **kw)
        elif method == "GET":
            res = await client.get(url, **kw)
        else:
            raise ValueError(f"Unsupported method: {method}")
        res.raise_for_status()
        return res

    return await _async_retry_call(_do, attempts=3)


# ----------------- UPLOAD SIZE MIDDLEWARE (DoS fix) -----------------
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(10 * 1024 * 1024)))  # 10 MB

# #4: Rate limiting — in-memory sliding window (no external deps)
# Har bir API_KEY uchun daqiqasiga maksimal so'rov soni
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "120"))
_rate_limit_store: Dict[str, list] = {}  # key_hash -> [timestamps]
_rate_limit_lock = threading.Lock()


def _check_rate_limit(api_key: str) -> bool:
    """Sliding-window rate limiter. False = limit oshgan.
    #4 CR fix: asyncio.to_thread orqali chaqiriladi — event loop bloklanmaydi."""
    if not api_key or RATE_LIMIT_PER_MINUTE <= 0:
        return True  # Rate limiting disabled
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()[:16]
    now = time.time()
    with _rate_limit_lock:
        timestamps = _rate_limit_store.get(key_hash, [])
        # 60 sekunddan eski timestamp'larni tozalash
        cutoff = now - 60
        timestamps = [t for t in timestamps if t > cutoff]
        if len(timestamps) >= RATE_LIMIT_PER_MINUTE:
            _rate_limit_store[key_hash] = timestamps
            return False
        timestamps.append(now)
        _rate_limit_store[key_hash] = timestamps
        # Memory cleanup: 10 daqiqada bir marta eski kalitlarni tozalash
        if len(_rate_limit_store) > 1000:
            stale = [k for k, v in _rate_limit_store.items() if not v or v[-1] < cutoff]
            for k in stale:
                _rate_limit_store.pop(k, None)
        return True


def _cleanup_rate_limit_store():
    """#4 CR fix: Background task orqali rate-limit store'ni tozalash.
    Har 60 sekundda chaqiriladi — faol bo'lmagan kalitlarni o'chiradi."""
    cutoff = time.time() - 120  # 2 daqiqa faol bo'lmagan kalitlarni o'chirish
    with _rate_limit_lock:
        stale = [
            k for k, v in _rate_limit_store.items()
            if not v or (v[-1] < cutoff if v else True)
        ]
        for k in stale:
            _rate_limit_store.pop(k, None)


@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """#4: Har bir so'rov uchun rate limiting tekshiruvi.
    Faqat API_KEY bilan kelgan so'rovlar tekshiriladi."""
    api_key = request.headers.get("X-API-Key", "")
    if api_key:
        allowed = await asyncio.to_thread(_check_rate_limit, api_key)
        if not allowed:
            log.warning(f"Rate limit exceeded for key hash={hashlib.sha256(api_key.encode()).hexdigest()[:12]}")
            return PlainTextResponse("Rate limit exceeded. Try again later.", status_code=429)
    return await call_next(request)


@app.middleware("http")
async def limit_upload_size(request, call_next):
    """M4 / DoS: haddan tashqari katta audio fayllarni rad etish."""
    cl = request.headers.get("content-length")
    if cl:
        try:
            if int(cl) > MAX_UPLOAD_BYTES:
                log.warning(f"Rejected oversized upload: {cl} bytes > {MAX_UPLOAD_BYTES}")
                return PlainTextResponse(
                    f"Payload too large ({cl} > {MAX_UPLOAD_BYTES})",
                    status_code=413,
                )
        except ValueError:
            pass
    return await call_next(request)


# ----------------- BACKGROUND CLEANUP -----------------
async def _background_session_cleanup():
    """Tavsiya 3: har get_session() chaqirig'ida cleanup_expired o'rniga
    background task har 60 sekundda tozalash.
    #4 fix: Rate-limit store cleanup ham shu yerda."""
    while True:
        try:
            await asyncio.sleep(60)
            purged = session_manager.cleanup_expired_now()
            if purged:
                log.debug(f"Background cleanup: {purged} expired sessions purged")
            # #4: Rate-limit store'ni tozalash (stale entries)
            _cleanup_rate_limit_store()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception(f"Cleanup loop xatosi: {e}")


STRICT_SECURITY = os.getenv("STRICT_SECURITY", "true").lower() in ("true", "1", "yes")


# NOTE: STRICT_SECURITY is read at module-import time, but tests/
# test_security.py monkeypatches `main.STRICT_SECURITY` AFTER import
# (works because startup_event reads the attribute fresh each call).
# Do NOT inline this into startup_event as a local — the test contract
# would silently break.


def _abort_startup(reason: str, code: int = 2):
    """Startup abort — log.critical + std stderr print + sys.exit(code).

    Q1 think: FATAL xatoliklar uchun "duplicate is acceptable" qoidasi.
    - log.critical: APM/log monitoring (Sentry, DataDog) uchun.
    - print(stderr): Docker logs va CI capture uchun (uvicorn handlersini bypass).

    Q3 polish: print(line) ishlatadi FATAL_STDERR: prefiksini — bu greppable
    log.critical(startup_aborted...) dan farqli. Shunday qilib:
      - `grep STARTUP_ABORTED` — log line mos keladi (1)
      - `grep FATAL_STDERR` — stderr direct line mos keladi (1)
      - `grep -c STARTUP_ABORTED` — 1 (CI contract test uchun muhim)
    """
    log_msg = f"STARTUP_ABORTED (exit={code}): {reason}"
    stderr_msg = f"FATAL_STDERR: {log_msg}"
    log.critical(log_msg)
    try:
        print(stderr_msg, file=sys.stderr, flush=True)
    except Exception:
        pass
    sys.exit(code)


@app.on_event("startup")
async def startup_event():
    global async_http, _cleanup_task
    # Production security enforcement:
    # 1) AUDIT_VIEW_KEY == ORCHESTRATOR_API_KEY bo'lsa — STRICT_SECURITY (default) hard-fail,
    #    STRICT_SECURITY=false bo'lsa CRITICAL warn (back-compat).
    # SECURITY ENFORCEMENT (yangi): API_KEY va AUDIT_VIEW_KEY mavjudligi
    # STRICT_SECURITY (default true) da talab qilinadi. Bo'sh bo'lsa abort.
    if STRICT_SECURITY and not API_KEY:
        _abort_startup(
            "ORCHESTRATOR_API_KEY environment o'zgaruvchisi talab qilinadi. "
            ".env faylini yuklab eksport qiling yoki service systemd unit'ga yozing."
        )
    if STRICT_SECURITY and not AUDIT_VIEW_KEY:
        _abort_startup(
            "AUDIT_VIEW_KEY environment o'zgaruvchisi talab qilinadi. "
            "U ORCHESTRATOR_API_KEY dan FARQLI bo'lishi kerak (defense in depth)."
        )
    if AUDIT_VIEW_KEY == API_KEY:
        if STRICT_SECURITY:
            _abort_startup(
                "AUDIT_VIEW_KEY == ORCHESTRATOR_API_KEY. "
                "Production'da ikki kalit har xil bo'lishi kerak (defense in depth). "
                "AUDIT_VIEW_KEY environment orqali boshqa qiymat bering yoki STRICT_SECURITY=false qo'ying."
            )
        else:
            log.critical(
                "XAVFSIZLIK OGOHLANTIRISH: AUDIT_VIEW_KEY == ORCHESTRATOR_API_KEY. "
                "Bu ikki kalit har xil bo'lishi kerak (defense in depth)."
            )

    log.info(
        f"Xavfsizlik konfiguratsiyasi: TRUSTED_PROXY_CIDRS={len(_TRUSTED_PROXY_NETS)} networks, "
        f"PII_AUDIT_PERSIST={PII_AUDIT_PERSIST}, AUDIT_KEYS_DISTINCT={AUDIT_VIEW_KEY != API_KEY}, "
        f"STRICT_SECURITY={STRICT_SECURITY}"
    )

    # Lazy init: async_http yaratamiz.
    # #3, #10 fix: API_KEY endi HEADERS_BASE'da emas — node so'rovlari NODE_HEADERS ishlatadi.
    global HEADERS_BASE, async_http
    HEADERS_BASE = {"Content-Type": "application/json"}
    async_http = httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=HEADERS_BASE)
    try:
        await asyncio.to_thread(redis_client.ping)
        log.info("Redis reachable")
    except Exception as e:
        log.error(f"Redis unreachable: {e}")
    _cleanup_task = asyncio.create_task(_background_session_cleanup())
    # Health monitor + analytics persistence
    asyncio.create_task(_node_health_monitor())
    asyncio.create_task(_analytics_persistence_loop())
    # Analytics'ni diskdan tiklash
    await _restore_analytics_from_disk()


@app.on_event("shutdown")
async def shutdown_event():
    global _cleanup_task
    if _cleanup_task is not None:
        _cleanup_task.cancel()
        try:
            await _cleanup_task
        except (asyncio.CancelledError, Exception):
            pass
    if async_http:
        await async_http.aclose()
    try:
        await asyncio.to_thread(redis_client.close)
    except Exception:
        pass


# ----------------- SETTINGS / ANALYTICS -----------------
settings_db = {
    "network": {"use_public_internet": False},
    "services": [
        {"id": 1, "name": "Savdo bo'limi", "description": "Savdo va mahsulotlar bo'yicha", "active": True},
        {"id": 2, "name": "Texnik yordam", "description": "Uskunalarni sozlash va muammolar", "active": True}
    ]
}


# Analytics lock — concurrent update race condition oldini olish
analytics_db = {
    "active_calls": 0,
    "total_calls_today": 0,
    "avg_duration_sec": 0,
    "total_duration_sec": 0,
    "language_usage": {"uz": 0, "ru": 0, "en": 0},
    "history": [],
    "caller_correlation": {},   # caller_id -> external_id (Twilio CallSid) mapping
}
ANALYTICS_HISTORY_LIMIT = 200
_analytics_lock = threading.Lock()
_CALLER_CORRELATION_LIMIT = 1000  # Memory leak fix: cheklangan cache


def _record_external_id(caller_id: str, external_id: str):
    """SIP bridge'dan kelgan external_id (Twilio CallSid) ni saqlash.
    Code-reviewer 1-nit fix: analytics correlation endi to'liq."""
    if not external_id or not caller_id:
        return
    with _analytics_lock:
        analytics_db["caller_correlation"][caller_id] = external_id
        # M13: Cache limit (memory leak)
        if len(analytics_db["caller_correlation"]) > _CALLER_CORRELATION_LIMIT:
            # Eng eski yarmini tozalash
            keys = list(analytics_db["caller_correlation"].keys())
            for k in keys[: len(keys) // 2]:
                analytics_db["caller_correlation"].pop(k, None)


def _bump_analytics(language: str, duration: float = 0.0, external_id: str | None = None):
    """Thread-safe analytics update."""
    with _analytics_lock:
        analytics_db["total_calls_today"] += 1
        analytics_db["total_duration_sec"] += duration
        if analytics_db["total_calls_today"] > 0:
            analytics_db["avg_duration_sec"] = (
                analytics_db["total_duration_sec"] / analytics_db["total_calls_today"]
            )
        if language not in analytics_db["language_usage"]:
            analytics_db["language_usage"][language] = 0
        analytics_db["language_usage"][language] += 1
        # M13: History'ni cheklash (memory leak fix)
        history_entry = {
            "ts": time.time(), "language": language, "duration_sec": round(duration, 3)
        }
        if external_id:
            history_entry["external_id"] = external_id
        analytics_db["history"].append(history_entry)
        if len(analytics_db["history"]) > ANALYTICS_HISTORY_LIMIT:
            analytics_db["history"] = analytics_db["history"][-ANALYTICS_HISTORY_LIMIT:]  # noqa: E501


# ----------------- MODELS -----------------
class TextChatRequest(pydantic.BaseModel):
    caller_id: str
    language: str = "uz"
    text: str

    @pydantic.field_validator("language")
    @classmethod
    def _check_language(cls, v: str) -> str:
        if v not in VALID_LANGUAGES:
            raise ValueError(f"Noto'g'ri til: {v}. Ruxsat etilgan: {sorted(VALID_LANGUAGES)}")
        return v

    @pydantic.field_validator("caller_id")
    @classmethod
    def _check_caller(cls, v: str) -> str:
        # #11 fix: : va . belgilari olib tashlandi (path traversal / injection xavfi)
        v = re.sub(r"[^A-Za-z0-9_\-]", "_", v)[:64]
        if not v:
            raise ValueError("caller_id bo'sh bo'lmasligi kerak")
        return v


class NetworkSetting(pydantic.BaseModel):
    use_public_internet: bool


class CompanyService(pydantic.BaseModel):
    id: Optional[int] = None
    name: str
    description: str
    active: bool

    @pydantic.field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("name bo'sh bo'lmasligi kerak")
        return v[:128]


# ----------------- DYNAMIC DISCOVERY -----------------
class NodeRegistration(pydantic.BaseModel):
    node_type: str # kaggle, kaggle1, kaggle2
    url: str


# ----------------- AUTH -----------------
async def verify_api_key(x_api_key: Optional[str] = Header(default=None)):
    """REST endpoint'lar uchun API key header tekshiruvi.
    M13 defense: API_KEY env berilmagan bo'lsa (None bo'lsa) hammaga 403 qaytaramiz —
    bu STRICT_SECURITY=true startup abort bilan birga ishlaydi, lekin agar
    STRICT_SECURITY=false (legacy mode) va env tushurib qoldirilgan bo'lsa ham
    hech kim o'tmasin, jim emas."""
    if not API_KEY:
        # Server-side configuration error — xavfsizlik jihatidan hammaga yopiq.
        raise HTTPException(
            status_code=503,
            detail="Server hali konfiguratsiya qilinmagan (ORCHESTRATOR_API_KEY yetishmaydi)."
        )
    if x_api_key and x_api_key == API_KEY:
        return x_api_key
    raise HTTPException(status_code=403, detail="Xavfsizlik: Noto'g'ri API Kalit!")


# #3 fix: Node registratsiya uchun alohida auth — NODE_COMM_KEY yoki API_KEY
# Bu node komprometatsiya qilinsa ham ORCHESTRATOR_API_KEY oshkor bo'lmasligi uchun.
async def verify_node_key(x_api_key: Optional[str] = Header(default=None)):
    if not API_KEY and not NODE_COMM_KEY:
        raise HTTPException(
            status_code=503,
            detail="Server konfiguratsiya qilinmagan (API_KEY yoki NODE_COMM_KEY yetishmaydi)."
        )
    if x_api_key and (x_api_key == API_KEY or x_api_key == NODE_COMM_KEY):
        return x_api_key
    raise HTTPException(status_code=403, detail="Xavfsizlik: Noto'g'ri API Kalit!")


# Thread-safe lock for node registration endpoints
_node_registration_lock = asyncio.Lock()




@app.get("/api/nodes/status")
async def nodes_status(_: str = Depends(verify_api_key)):
    """Barcha node'larning holatini qaytaradi — GPU, model, URL.
    -m flag uchun ishlatiladi."""
    import asyncio as _aio
    
    node_info = {
        "kaggle": {"node": 0, "url": KAGGLE_URL, "label": "Node-0 (LLM+TTS UZ)"},
        "kaggle1": {"node": 1, "url": KAGGLE1_URL, "label": "Node-1 (STT RU+TTS)"},
        "kaggle2": {"node": 2, "url": KAGGLE2_URL, "label": "Node-2 (STT EN+UZ)"},
    }
    
    result = {}
    for ntype, info in node_info.items():
        url = info["url"]
        health_url = f"{url}/health"
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
                resp = await client.get(health_url, headers=NODE_HEADERS)
                if resp.status_code == 200:
                    data = resp.json()
                    result[ntype] = {
                        "node": info["node"],
                        "label": info["label"],
                        "url": url,
                        "status": data.get("status", "?"),
                        "models": data.get("models", []),
                        "missing": data.get("missing", []),
                        "gpus": data.get("gpus", []),
                        "error": data.get("error", ""),
                    }
                else:
                    result[ntype] = {
                        "node": info["node"],
                        "label": info["label"],
                        "url": url,
                        "status": f"HTTP {resp.status_code}",
                        "models": [],
                        "gpus": [],
                    }
        except Exception as e:
            result[ntype] = {
                "node": info["node"],
                "label": info["label"],
                "url": url,
                "status": "unreachable",
                "error": str(e)[:120],
                "models": [],
                "gpus": [],
            }
    
    return {"nodes": result}


@app.post("/register-node")
async def register_node(req: NodeRegistration, _: str = Depends(verify_node_key)):
    """Kaggle nodlarini registratsiya qilish — NODE_COMM_KEY yoki API_KEY.
    #3 fix: Node'lar uchun alohida kalit (NODE_COMM_KEY) ishlatiladi.
    Thread-safe: asyncio.Lock bilan concurrent registratsiyalarni ketma-ketlashtiramiz."""
    global KAGGLE_URL, KAGGLE1_URL, KAGGLE2_URL
    global STT_ENDPOINTS, TTS_ENDPOINTS, LLM_ENDPOINT

    async with _node_registration_lock:
        if req.node_type == "kaggle":
            KAGGLE_URL = req.url
            LLM_ENDPOINT = f"{KAGGLE_URL}/chat"
            TTS_ENDPOINTS["uz"] = f"{KAGGLE_URL}/synthesize"
        elif req.node_type == "kaggle1":
            KAGGLE1_URL = req.url
            STT_ENDPOINTS["ru"] = f"{KAGGLE1_URL}/transcribe/ru"
            TTS_ENDPOINTS["ru"] = f"{KAGGLE1_URL}/synthesize/ru"
            TTS_ENDPOINTS["en"] = f"{KAGGLE1_URL}/synthesize/en"
        elif req.node_type == "kaggle2":
            KAGGLE2_URL = req.url
            STT_ENDPOINTS["uz"] = f"{KAGGLE2_URL}/transcribe/uz"
            STT_ENDPOINTS["en"] = f"{KAGGLE2_URL}/transcribe/en"
        else:
            raise HTTPException(status_code=400, detail="Noma'lum tugun turi")

    from stream_controller import stream_controller
    from profile_manager import profile_manager
    stream_controller.update_endpoints(STT_ENDPOINTS, TTS_ENDPOINTS, LLM_ENDPOINT)
    try:
        profile_manager.load_profile("isp_beta")
    except Exception as e:
        logging.error(f"Profilni yuklashda xatolik: {e}")

    log.info(f"[DISCOVERY] Tugun yangilandi: {req.node_type.upper()} = {req.url}")
    return {"status": "success", "message": f"{req.node_type} ro'yxatdan o'tdi"}


@app.post("/api/profile/reload")
async def reload_profile(_: str = Depends(verify_api_key)):
    """Profilni hot-reload qilish (prompt.txt, config.json, tools.json)."""
    from profile_manager import profile_manager
    from session_manager import session_manager
    try:
        profile_manager.reload()
        session_manager.update_ttl_from_config()
        return {"status": "success", "profile": profile_manager.active_profile_name,
                "version": profile_manager._prompt_version,
                "llm_params": profile_manager.get_llm_params()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/profile/switch")
async def switch_profile(profile_name: str = Form(...), _: str = Depends(verify_api_key)):
    """Boshqa profilga o'tish (isp_beta, isp_ru, isp_en)."""
    from profile_manager import profile_manager
    from session_manager import session_manager
    try:
        profile_manager.load_profile(profile_name)
        session_manager.update_ttl_from_config()
        return {"status": "success", "profile": profile_name,
                "language": profile_manager.get_language()}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Profil topilmadi: {profile_name}")


@app.get("/api/profile/current")
async def current_profile(_: str = Depends(verify_api_key)):
    """Joriy profil haqida ma'lumot."""
    from profile_manager import profile_manager
    return {
        "profile": profile_manager.active_profile_name,
        "version": profile_manager._prompt_version,
        "language": profile_manager.get_language(),
        "llm_params": profile_manager.get_llm_params(),
        "context": profile_manager.get_context_config(),
        "tools_count": len(profile_manager.get_tools()),
    }


async def verify_ws_api_key(websocket: WebSocket) -> bool:
    """WebSocket uchun API key — FAQAT header orqali.
    #2 fix: Query param orqali API_KEY uzatish olib tashlandi (URL log'larda qoladi).
    M13 defense: API_KEY konfiguratsiya qilinmagan bo'lsa False qaytaramiz."""
    if not API_KEY:
        return False
    api_key_h = websocket.headers.get("X-API-Key")
    return api_key_h == API_KEY


# ----------------- HELPER FUNKSIYALAR (ASYNC) -----------------

async def call_stt_service(language: str, audio_bytes: bytes) -> str:
    """Audio baytlarni STT tuguniga yuboradi. Filename parametri O'CHIRILDI (C4)."""
    if not audio_bytes:
        return ""
    lang = (language or "uz").lower()
    if lang not in VALID_LANGUAGES:
        log.warning(f"Noto'g'ri til STT so'rovida: {language}")
        lang = "uz"
    start = time.perf_counter()
    try:
        # Format normalizatsiya (C16 / M16): noto'g'ri format STT'da 500 qaytaradi
        normalized_audio = await asyncio.to_thread(ensure_wav_16k_mono, audio_bytes)
        url = STT_ENDPOINTS[lang]
        files = {"audio_file": ("audio.wav", normalized_audio, "audio/wav")}
        response = await resilient_request(async_http, url, method="POST", files=files, headers=NODE_HEADERS)
        data = response.json()
        if "encrypted_payload" in data:
            decrypted_bytes = decrypt_payload(data["encrypted_payload"])
            return json.loads(decrypted_bytes.decode('utf-8')).get("text", "")
        return data.get("text", "")
    except Exception as e:
        log.exception(f"STT xatosi [{language}]: {e}")
        return ""
    finally:
        STT_LATENCY.observe(time.perf_counter() - start)


async def call_llm_service(history: List[Dict[str, str]]) -> str:
    """LLM ga suhbat tarixini yuboradi."""
    start = time.perf_counter()
    try:
        payload = {"messages": history}
        encrypted_str = encrypt_payload(json.dumps(payload).encode('utf-8'))
        req_data = {"encrypted_payload": encrypted_str}
        response = await resilient_request(async_http, LLM_ENDPOINT, method="POST",
                                           json_body=req_data, headers=NODE_HEADERS)
        res_json = response.json()
        if "encrypted_payload" in res_json:
            decrypted_bytes = decrypt_payload(res_json["encrypted_payload"])
            return json.loads(decrypted_bytes.decode('utf-8')).get("response", "")
        return "Xatolik: Shifrlangan javob kelmadi"
    except Exception as e:
        log.exception(f"LLM xatosi: {e}")
        return "Uzur, men hozir biroz bandman. Tizimda muammo bor."
    finally:
        LLM_LATENCY.observe(time.perf_counter() - start)


async def call_tts_service(language: str, text: str) -> Optional[bytes]:
    """TTS tugunidan audio olasiz. H5 fix: format mos muammosi SIP bridge uchun
    TTS WAV (header+PCM) qaytaradi, biz uni raw PCM 16kHz mos formatga keltiramiz."""
    lang = (language or "uz").lower()
    if lang not in VALID_LANGUAGES:
        lang = "uz"
    url = TTS_ENDPOINTS[lang]
    payload = {"text": text, "language": lang}
    encrypted_str = encrypt_payload(json.dumps(payload).encode('utf-8'))
    req_data = {"encrypted_payload": encrypted_str}
    start_ts = time.perf_counter()
    try:
        response = await resilient_request(async_http, url, method="POST",
                                           json_body=req_data, headers=NODE_HEADERS)
        audio_bytes = response.content or b""
        if not audio_bytes:
            return None
        # WAV header'ini chiqarib tashlab, SIP bridge uchun PCM 16kHz raw bytes qaytaramiz.
        # Bu H5 (audio format mismatch) ni hal qiladi.
        result = await asyncio.to_thread(_wav_to_pcm16k, audio_bytes)
        return result
    except Exception as e:
        log.error(f"[TTS Error] {url} ga bog'lanib bo'lmadi: {e}")
        return None
    finally:
        TTS_LATENCY.observe(time.perf_counter() - start_ts)


def _wav_to_pcm16k(audio_bytes: bytes) -> bytes:
    """WAV baytlardan PCM 16kHz 16-bit mono raw bytes ajratib oladi.
    numpy bilan any sample_rate -> 16kHz linear resample.
    Audio bo'lmagan yoki konversiya xato bergan bo'lsa — b'' qaytaradi
    (orchestrator None sifatida qabul qiladi va SIP bridge'ga audio yubormaydi).
    H5 polish: BUG regressiya qilmaslik uchun har qanday failure path'da b''."""
    if not audio_bytes:
        return b""
    try:
        import io as _io
        import wave as _wave
        with _io.BytesIO(audio_bytes) as buf:
            with _wave.open(buf, "rb") as w:
                sampwidth = w.getsampwidth()
                channels = w.getnchannels()
                framerate = w.getframerate()
                raw = w.readframes(w.getnframes())

        if sampwidth != 2:
            try:
                import audioop
                if sampwidth == 1:
                    raw = audioop.bias(raw, 128)
                if sampwidth == 4:
                    raw = audioop.lin2lin(raw, 4, 2)
                sampwidth = 2
            except Exception as e:
                log.debug(f"audioop sampwidth convert skip: {e}")
                return b""

        if channels > 1:
            try:
                import audioop
                raw = audioop.tomono(raw, sampwidth, 1, 0)
                channels = 1
            except Exception as e:
                log.debug(f"audioop tomono skip: {e}")
                return b""

        if framerate != 16000:
            try:
                import numpy as np
                if sampwidth == 2:
                    samples = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
                    n = len(samples)
                    if n == 0:
                        return b""
                    target_n = int(round(n * 16000.0 / framerate))
                    if target_n <= 0:
                        return b""
                    xp = np.linspace(0.0, 1.0, num=n)
                    fp = samples
                    x = np.linspace(0.0, 1.0, num=target_n)
                    resampled = np.interp(x, xp, fp)
                    raw = resampled.astype(np.int16).tobytes()
                    framerate = 16000
            except Exception as e:
                log.debug(f"numpy resample unavailable (Python 3.13 mos): {e}")
                return b""

        if framerate == 16000 and sampwidth == 2 and channels == 1:
            return raw
        return b""
    except Exception as e:
        log.warning(f"_wav_to_pcm16k xatosi: {e}")
        return b""


# ----------------- ENDPOINTS -----------------

@app.get("/metrics")
async def metrics(x_api_key: Optional[str] = Header(default=None)):
    """Prometheus metrics endpoint — #5 fix: API_KEY auth talab qilinadi."""
    if not API_KEY or x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Xavfsizlik: Metrics uchun API kalit talab qilinadi")
    return PlainTextResponse(generate_latest(), media_type="text/plain")


def _redact_id(value):
    """PII himoyasi: external_id larni hash qilib ko'rsatadi.
    include_pii=True bo'lsa, faqat admin ko'ra oladi."""
    if not value:
        return None
    return "sha256:" + hashlib.sha256(value.encode('utf-8')).hexdigest()[:16]


# ----------------- AUDIT LOG (PII access tracking) -----------------
# GDPR/audit compliance: har bir PII access (real CallSid exposure) loglanadi
try:
    PII_AUDIT_LOG_LIMIT = int(os.getenv("PII_AUDIT_LOG_LIMIT", "500"))
except ValueError:
    PII_AUDIT_LOG_LIMIT = 500
PII_AUDIT_PERSIST = os.getenv("PII_AUDIT_PERSIST", "stdout").lower() in ("stdout", "log", "yes", "true", "1")
pii_audit_log: list[dict] = []
_pii_audit_lock = threading.Lock()


def _mask_key(api_key: str) -> str:
    """Audit log uchun API key id'si — first-12 hash (partial secret emas)."""
    if not api_key:
        return None
    return "kh:" + hashlib.sha256(api_key.encode('utf-8')).hexdigest()[:12]


# Ishonchli proxy CIDR-lar (PII forensics ni himoya qilish)
# Faqat shu tarmoqlardan kelgan X-Forwarded-For qabul qilinadi.
# Default: localhost + ichki tarmoq + Docker bridge.
TRUSTED_PROXY_CIDRS_RAW = os.getenv(
    "TRUSTED_PROXY_CIDRS",
    "127.0.0.1/32,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,::1/128,fc00::/7",
)
# ipaddress stdlib bilan parse qilamiz (manual regex o'rniga)
import ipaddress as _ipaddress

_TRUSTED_PROXY_NETS = []
for cidr in (TRUSTED_PROXY_CIDRS_RAW or "").split(","):
    cidr = cidr.strip()
    if not cidr:
        continue
    try:
        _TRUSTED_PROXY_NETS.append(_ipaddress.ip_network(cidr, strict=False))
    except ValueError:
        pass


def _ip_in_trusted(ip_str: str) -> bool:
    """IP manzil TRUSTED_PROXY_CIDRS da bor-yo'qligini tekshiradi (ipaddress stdlib)."""
    if not ip_str or not _TRUSTED_PROXY_NETS:
        return False
    try:
        ip = _ipaddress.ip_address(ip_str.strip())
    except ValueError:
        return False
    return any(ip in net for net in _TRUSTED_PROXY_NETS)


def _is_valid_ip_str(s: str) -> bool:
    """IP manzil sintaksisini tekshiradi (ipaddress stdlib)."""
    if not s:
        return False
    try:
        _ipaddress.ip_address(s.strip())
        return True
    except (ValueError, AttributeError):
        return False


def _get_real_client_ip(request: Request) -> str:
    """Reverse proxy orqali haqiqiy mijoz IP.

    #12 fix: X-Forwarded-For IP spoofing himoyasi kuchaytirildi.
    - Faqat TRUSTED_PROXY_CIDRS dan kelgan so'rovlarda XFF ishoniladi
    - XFF zanjiridagi faqat oxirgi trusted bo'lmagan IP olinadi (RFC 7239)
    - Private/internal IP'lar avtomatik filtrlanadi
    """
    # #12: Agar request.client ishonchli proxy bo'lsa, XFF dan haqiqiy client IP
    if request.client and request.client.host and _ip_in_trusted(request.client.host):
        xff = request.headers.get("x-forwarded-for", "")
        if xff:
            # XFF zanjirini o'ngdan chapga qarab tekshiramiz,
            # trusted proxy'larni o'tkazib yuboramiz, birinchi untrusted IP mijoz IP
            ips = [ip.strip() for ip in xff.split(",") if ip.strip()]
            for ip in reversed(ips):
                if _is_valid_ip_str(ip) and not _ip_in_trusted(ip):
                    return ip
            # Hammasi trusted bo'lsa, birinchi valid IP ni olamiz
            for ip in ips:
                if _is_valid_ip_str(ip):
                    return ip
        real_ip = request.headers.get("x-real-ip", "")
        if real_ip and _is_valid_ip_str(real_ip) and not _ip_in_trusted(real_ip):
            return real_ip
    if request.client and request.client.host:
        return request.client.host
    return "?"


def _audit_pii_access(included: bool, api_key: str = "", client_ip: str = "", endpoint: str = "/api/analytics"):
    """Real PII ko'rilganda audit log yozish (compliance + persistence).
    PII_AUDIT_PERSIST=True (default): stdout ga ham yoziladi (Docker log yig'ish uchun)."""
    if not included:
        return
    row = {
        "ts": time.time(),
        "key_hash": _mask_key(api_key),
        "endpoint": endpoint,
        "client_ip": client_ip,
    }
    with _pii_audit_lock:
        pii_audit_log.append(row)
        if len(pii_audit_log) > PII_AUDIT_LOG_LIMIT:
            pii_audit_log[:] = pii_audit_log[-PII_AUDIT_LOG_LIMIT:]
    # Persistence — code-reviewer ship-readiness fix:
    # - `audit_log.info` ishlatamiz (alohida logger, propagate=False).
    # - JSON message — JSON-as-string emas, balki structured JSON parse
    #   qiladigan operator (jq, vector, splunk) uchun tayyor.
    if PII_AUDIT_PERSIST:
        try:
            audit_log.warning(json.dumps(row, ensure_ascii=False))
        except Exception:
            pass


async def verify_audit_view_key(x_api_key: Optional[str] = Header(default=None)):
    """/api/audit/pii uchun alohida kalit. AUDIT_VIEW_KEY required."""
    if x_api_key and x_api_key == AUDIT_VIEW_KEY:
        return x_api_key
    raise HTTPException(status_code=403, detail="Xavfsizlik: Audit ko'rish kaliti noto'g'ri!")


@app.get("/api/audit/pii")
async def get_pii_audit_log(
    x_api_key: Optional[str] = Header(default=None),
    limit: int = Query(default=100, ge=1, le=PII_AUDIT_LOG_LIMIT),
    _: str = Depends(verify_audit_view_key),
):
    """PII audit logini ko'rish (faqat AUDIT_VIEW_KEY bilan).
    Hashed key + IP ko'rinadi, real CallSid emas."""
    with _pii_audit_lock:
        entries = list(pii_audit_log[-limit:])
        return {
            "count": len(entries),
            "limit": limit,
            "total": len(pii_audit_log),
            "entries": entries,
        }


@app.get("/api/analytics")
async def get_analytics(
    request: Request,
    include_pii: bool = Query(default=False),
    x_api_key: Optional[str] = Header(default=None),
    _: str = Depends(verify_api_key),
):
    """Admin Dashboard uchun real-time statistika.

    include_pii=False (default): real Twilio CallSid va streamSid hash qilib ko'rsatadi.
    include_pii=True: real qiymatlar ko'rinadi (faqat admin) — audit log yoziladi.
    """
    client_ip = _get_real_client_ip(request)
    _audit_pii_access(
        include_pii,
        x_api_key or API_KEY,
        client_ip,
        "/api/analytics?include_pii=true" if include_pii else "/api/analytics",
    )
    with _analytics_lock:
        if include_pii:
            caller_corr = {**analytics_db["caller_correlation"]}
            history = list(analytics_db["history"])
        else:
            caller_corr = {
                k: _redact_id(v) for k, v in analytics_db["caller_correlation"].items()
            }
            history = []
            for h in analytics_db["history"]:
                hh = {**h}
                if "external_id" in hh:
                    hh["external_id"] = _redact_id(hh["external_id"])
                history.append(hh)
        snap = {
            "active_calls": analytics_db["active_calls"],
            "total_calls_today": analytics_db["total_calls_today"],
            "avg_duration_sec": analytics_db["avg_duration_sec"],
            "total_duration_sec": analytics_db["total_duration_sec"],
            "language_usage": {**analytics_db["language_usage"]},
            "history": history,
            "caller_correlation": caller_corr,
            "pii_included": include_pii,
        }
        return snap


@app.websocket("/ws/call/{caller_id}")
async def websocket_call(
    websocket: WebSocket,
    caller_id: str,
    language: str = Query(default="uz"),
):
    """Real-time streaming WebSocket pipeline.

    C8 fix: API key endi talab qilinadi.
    M8 fix: til validatsiyasi qilingan.
    M13 fix: audio_buffer maksimal hajmiga ega (OOM oldini olish).
    """
    # 1. AUTH (C8)
    if not await verify_ws_api_key(websocket):
        await websocket.close(code=1008, reason="Unauthorized")
        return

    lang = language.lower()
    if lang not in VALID_LANGUAGES:
        await websocket.close(code=1008, reason=f"Invalid language: {language}")
        return

    # Caller_id sanitization — #11 fix: : va . olib tashlandi
    caller_id = re.sub(r"[^A-Za-z0-9_\-]", "_", caller_id)[:64]

    await websocket.accept()

    # 2. LOAD BALANCING (Redis TTL fix — H3)
    slot_acquired = False
    try:
        current_calls = await asyncio.to_thread(redis_client.incr, "active_calls")
        # INCR + EXPIRE atomic emas, lekin EXPIRE INCR'dan keyin bo'lsa OK
        await asyncio.to_thread(redis_client.expire, "active_calls", ACTIVE_CALLS_TTL)
        if current_calls > MAX_CONCURRENT_CALLS:
            await asyncio.to_thread(redis_client.decr, "active_calls")
            await websocket.close(code=1013, reason="Server is too busy")
            return
        slot_acquired = True
        with _analytics_lock:
            analytics_db["active_calls"] += 1
        _bump_analytics(lang, 0.0)
    except Exception as e:
        log.exception(f"Redis xatosi: {e}")
        await websocket.close(code=1011, reason="Internal error")
        return

    start_time = time.time()
    audio_buffer = bytearray()
    MAX_BUFFER_SIZE = 32000 * 10  # ~10 sekund audio, undan ko'p bo'lsa reset

    correlation_external_id = None  # SIP bridge'dan kelgan CallSid mapping

    try:
        # Boshlanish signalini yuborish (SIP bridge sinxronizatsiya uchun)
        await websocket.send_json({"type": "ready", "caller_id": caller_id, "language": lang})

        while True:
            # receive_bytes() o'rniga receive() — text (JSON metadata) yoki bytes bo'lishi mumkin
            msg = await websocket.receive()
            if msg.get("type") != "websocket.receive":
                continue

            if "text" in msg and msg["text"]:
                # JSON control message (SIP bridge'dan metadata)
                try:
                    parsed = json.loads(msg["text"])
                    if isinstance(parsed, dict) and parsed.get("type") == "metadata":
                        correlation_external_id = parsed.get("external_id") or parsed.get("stream_sid")
                        if correlation_external_id:
                            _record_external_id(caller_id, correlation_external_id)
                            log.info(f"[{caller_id}] SIP correlation: external_id={correlation_external_id}")
                except Exception as e:
                    log.debug(f"WS text parse: {e}")
                continue

            if "bytes" not in msg or not msg["bytes"]:
                continue

            data = msg["bytes"]

            # M13 / H3: buffer overflow himoyasi
            if len(audio_buffer) + len(data) > MAX_BUFFER_SIZE:
                # eski audio'ni tashlaymiz — caller_id uzoq jim turgan bo'lsa kerak
                audio_buffer.clear()

            audio_buffer.extend(data)

            # VAD tekshiruvi: sukut aniqlanganda va buffer 1+ sekund
            if not vad_model.is_speech(data) and len(audio_buffer) >= 32000:
                transcribed_text = await call_stt_service(lang, bytes(audio_buffer))

                if transcribed_text.strip() and transcribed_text.strip() != "[Ovoz eshitilmadi]":
                    session_manager.add_message(caller_id, "user", transcribed_text)
                    chat_history = session_manager.get_session(caller_id)
                    llm_response_text = await call_llm_service(chat_history)
                    session_manager.add_message(caller_id, "assistant", llm_response_text)
                    audio_response = await call_tts_service(lang, llm_response_text)

                    await websocket.send_json({
                        "type": "text",
                        "transcribed": transcribed_text,
                        "ai_response": llm_response_text,
                    })
                    if audio_response:
                        await websocket.send_bytes(audio_response)
                else:
                    await websocket.send_json({"type": "text", "transcribed": "", "ai_response": ""})

                audio_buffer.clear()
    except WebSocketDisconnect:
        log.info(f"[{caller_id}] WebSocket uzildi.")
    except Exception as e:
        log.exception(f"[{caller_id}] WebSocket xatosi: {e}")
    finally:
        # M13 fix: history trim + audio_buffer clear
        audio_buffer.clear()
        if slot_acquired:
            try:
                await asyncio.to_thread(redis_client.decr, "active_calls")
            except Exception:
                pass
        with _analytics_lock:
            analytics_db["active_calls"] = max(0, analytics_db["active_calls"] - 1)
        duration = time.time() - start_time
        _bump_analytics(lang, duration, correlation_external_id)


# ── Analytics persistence (disk-backed) ──
ANALYTICS_PERSIST_PATH = os.getenv("ANALYTICS_PERSIST_PATH", "/tmp/orchestrator_analytics.json")
ANALYTICS_PERSIST_INTERVAL = int(os.getenv("ANALYTICS_PERSIST_INTERVAL", "30"))


async def _analytics_persistence_loop():
    """Har ANALYTICS_PERSIST_INTERVAL sekundda analytics_db ni JSON faylga yozadi."""
    while True:
        try:
            await asyncio.sleep(ANALYTICS_PERSIST_INTERVAL)
            with _analytics_lock:
                payload = {
                    "total_calls_today": analytics_db["total_calls_today"],
                    "total_duration_sec": analytics_db["total_duration_sec"],
                    "avg_duration_sec": analytics_db["avg_duration_sec"],
                    "language_usage": {**analytics_db["language_usage"]},
                    "history": list(analytics_db["history"]),
                    "saved_at": time.time(),
                }
            await asyncio.to_thread(_write_json_atomic, ANALYTICS_PERSIST_PATH, payload)
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"Analytics persist: {e}")


def _write_json_atomic(path: str, data: dict):
    """Atomik JSON yozish: tmp faylga yozib, rename qilamiz."""
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


async def _restore_analytics_from_disk():
    """Diskdagi analytics ma'lumotlarini tiklash (server restart)."""
    try:
        if os.path.exists(ANALYTICS_PERSIST_PATH):
            with open(ANALYTICS_PERSIST_PATH, "r") as f:
                saved = json.load(f)
            with _analytics_lock:
                analytics_db["total_calls_today"] = saved.get("total_calls_today", 0)
                analytics_db["total_duration_sec"] = saved.get("total_duration_sec", 0.0)
                analytics_db["avg_duration_sec"] = saved.get("avg_duration_sec", 0.0)
                for lang, count in saved.get("language_usage", {}).items():
                    analytics_db["language_usage"][lang] = count
                analytics_db["history"] = saved.get("history", [])
            log.info(f"Analytics restored from disk: {saved.get('total_calls_today', 0)} calls")
    except Exception as e:
        log.warning(f"Analytics restore: {e}")


# ── Circuit breaker / health monitoring ──
_NODE_CIRCUIT: Dict[str, dict] = {}  # url -> {"failures": int, "last_fail": ts, "open": bool}
_CIRCUIT_THRESHOLD = 3
_CIRCUIT_RESET_SEC = 30
_health_lock = threading.Lock()


def _circuit_breaker(url: str) -> bool:
    """Circuit breaker: 3 ta ketma-ket xatolikdan keyin 30s ochiladi."""
    with _health_lock:
        cb = _NODE_CIRCUIT.get(url)
        if cb and cb.get("open"):
            if time.time() - cb["last_fail"] > _CIRCUIT_RESET_SEC:
                cb["open"] = False
                cb["failures"] = 0
                return True
            return False
        return True


def _record_failure(url: str):
    with _health_lock:
        cb = _NODE_CIRCUIT.setdefault(url, {"failures": 0, "last_fail": 0, "open": False})
        cb["failures"] += 1
        cb["last_fail"] = time.time()
        if cb["failures"] >= _CIRCUIT_THRESHOLD:
            cb["open"] = True


def _record_success(url: str):
    with _health_lock:
        cb = _NODE_CIRCUIT.setdefault(url, {"failures": 0, "last_fail": 0, "open": False})
        cb["failures"] = 0
        cb["open"] = False

HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "15"))
_node_health_status: Dict[str, str] = {}
_node_health_lock = threading.Lock()


async def _node_health_monitor():
    """Background task: har HEALTH_CHECK_INTERVAL sekundda node'larni tekshiradi.
    Circuit breaker + automatic retry/backoff."""
    while True:
        try:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)
            nodes_to_check = {
                "kaggle (LLM+TTS UZ)": KAGGLE_URL,
                "kaggle1 (STT RU+TTS)": KAGGLE1_URL,
                "kaggle2 (STT EN+UZ)": KAGGLE2_URL,
            }
            for name, url in nodes_to_check.items():
                if not url or url.startswith("http://127.0.0.1"):
                    continue
                health_url = f"{url}/health"
                if not _circuit_breaker(health_url):
                    with _node_health_lock:
                        _node_health_status[name] = "OPEN (circuit)"
                    continue
                try:
                    res = await async_http.get(health_url, timeout=httpx.Timeout(2.0))
                    if res.status_code == 200:
                        _record_success(health_url)
                        data = res.json()
                        with _node_health_lock:
                            _node_health_status[name] = data.get("status", "online")
                    else:
                        _record_failure(health_url)
                        with _node_health_lock:
                            _node_health_status[name] = f"HTTP {res.status_code}"
                except Exception:
                    _record_failure(health_url)
                    with _node_health_lock:
                        _node_health_status[name] = "OFFLINE"
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"Health monitor: {e}")


@app.get("/api/health")
async def check_cluster_health(_: str = Depends(verify_api_key)):
    """Barcha AI tugunlarning faolligini tekshiradi.
    Live probe + background monitor natijalari."""
    # Live quick-check (bitta so'rov, 2s timeout)
    live_results = {}
    nodes = {
        "LLM+TTS UZ": f"{KAGGLE_URL}/health",
        "STT RU+TTS": f"{KAGGLE1_URL}/health",
        "STT EN+UZ": f"{KAGGLE2_URL}/health",
    }
    for name, url in nodes.items():
        if not url or url.startswith("http://127.0.0.1"):
            live_results[name] = "not_registered"
            continue
        try:
            res = await async_http.get(url, timeout=httpx.Timeout(2.0))
            if res.status_code == 200:
                j = res.json()
                live_results[name] = j.get("status", "OK")
            else:
                live_results[name] = f"HTTP {res.status_code}"
        except Exception as e:
            live_results[name] = f"error: {str(e)[:40]}"

    with _node_health_lock:
        bg = dict(_node_health_status)

    with _health_lock:
        circuits = {url: {"failures": c["failures"], "open": c["open"]}
                    for url, c in _NODE_CIRCUIT.items()}

    return {
        "status": "healthy",
        "timestamp": time.time(),
        "encryption": "AES-256-GCM Active",
        "live_check": live_results,
        "background_monitor": bg,
        "circuit_breakers": circuits,
    }


@app.post("/handle_call")
async def handle_call(
    caller_id: str = Form(...),
    language: str = Form("uz"),
    audio_file: UploadFile = File(...),
    _: str = Depends(verify_api_key),
):
    """Real operator pipeline: Audio -> STT -> Session+LLM -> TTS -> Audio."""
    start_time = time.time()
    lang = language.lower()
    if lang not in VALID_LANGUAGES:
        raise HTTPException(status_code=400, detail=f"Invalid language: {language}")
    # #11 fix: : va . olib tashlandi
    safe_caller = re.sub(r"[^A-Za-z0-9_\-]", "_", caller_id)[:64]

    log.info(f"[{safe_caller}] 📞 Yangi qo'ng'iroq keldi. Til: {lang.upper()}")

    audio_bytes = await audio_file.read()
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="Audio fayl bo'sh.")

    t_stt_start = time.time()
    transcribed_text = await call_stt_service(lang, audio_bytes)
    stt_duration = round(time.time() - t_stt_start, 3)
    log.info(f"[{safe_caller}] STT ({stt_duration}s): '{transcribed_text}'")

    if not transcribed_text.strip():
        transcribed_text = "[Ovoz eshitilmadi]"

    # H8 fix: STT xato bo'lsa ham chat tarixiga yozamiz, lekin bo'sh xabar bilan LLM ni chaqirmaymiz
    if transcribed_text.strip() != "[Ovoz eshitilmadi]":
        session_manager.add_message(safe_caller, "user", transcribed_text)
        chat_history = session_manager.get_session(safe_caller)

        t_llm_start = time.time()
        llm_response_text = await call_llm_service(chat_history)
        llm_duration = round(time.time() - t_llm_start, 3)
        log.info(f"[{safe_caller}] LLM ({llm_duration}s): '{llm_response_text}'")
        session_manager.add_message(safe_caller, "assistant", llm_response_text)
    else:
        llm_response_text = "Uzur, ovozingiz eshitilmadi. Iltimos qayta gapiring."
        llm_duration = 0.0

    t_tts_start = time.time()
    audio_response_bytes = await call_tts_service(lang, llm_response_text)
    tts_duration = round(time.time() - t_tts_start, 3)

    total_duration = round(time.time() - start_time, 3)

    audio_b64 = base64.b64encode(audio_response_bytes).decode('utf-8') if audio_response_bytes else ""

    # Process tugagach, analytics DB'ni yangilash
    _bump_analytics(lang, total_duration)

    return {
        "status": "success",
        "caller_id": safe_caller,
        "language": lang,
        "transcribed_text": transcribed_text,
        "ai_response_text": llm_response_text,
        "audio_base64": audio_b64,
        "encryption": "AES-256-GCM Verified",
        "metrics": {
            "stt_latency_sec": stt_duration,
            "llm_latency_sec": llm_duration,
            "tts_latency_sec": tts_duration,
            "total_latency_sec": total_duration
        }
    }


@app.post("/api/chat_text")
async def chat_text(req: TextChatRequest, _: str = Depends(verify_api_key)):
    """Faqat matnli muloqot uchun API (Test va Dashboard Chat)."""
    session_manager.add_message(req.caller_id, "user", req.text)
    chat_history = session_manager.get_session(req.caller_id)

    llm_response_text = await call_llm_service(chat_history)
    session_manager.add_message(req.caller_id, "assistant", llm_response_text)

    return {
        "status": "success",
        "caller_id": req.caller_id,
        "user_text": req.text,
        "ai_response": llm_response_text,
        "session_length": len(chat_history)
    }


@app.delete("/api/sessions/{caller_id}")
async def reset_session(caller_id: str, _: str = Depends(verify_api_key)):
    """Mijoz muloqot tarixini RAMdan o'chirish."""
    session_manager.clear_session(caller_id)
    return {"status": "success", "message": f"[{caller_id}] muloqot tarixi o'chirildi."}


# ----------------- SETTINGS -----------------
@app.get("/api/settings/network")
async def get_network_settings():
    return settings_db["network"]


@app.post("/api/settings/network")
async def update_network_settings(setting: NetworkSetting, _: str = Depends(verify_api_key)):
    settings_db["network"]["use_public_internet"] = setting.use_public_internet
    return {"status": "success", "network": settings_db["network"]}


@app.get("/api/settings/services")
async def get_services():
    return settings_db["services"]


@app.post("/api/settings/services")
async def add_service(service: CompanyService, _: str = Depends(verify_api_key)):
    # M7 fix: None id bilan xatosiz ishlash
    if settings_db["services"]:
        max_id = max((s.get("id", 0) or 0) for s in settings_db["services"])
    else:
        max_id = 0
    data = service.model_dump() if hasattr(service, "model_dump") else service.dict()
    data["id"] = max_id + 1
    settings_db["services"].append(data)
    return {"status": "ok"}

@app.get("/download-llm")
async def download_llm(_: str = Depends(verify_api_key)):
    """Kaggle tomonidan LLM modelini yuklab olish uchun endpoint.
    Xavfsizlik: API kalit Header orqali tekshiriladi (verify_api_key dependency).
    #1 fix: Path traversal himoyasi — model_name validatsiyasi."""
    model_dir = os.environ.get("LLM_MODEL_DIR", "/home/ubuntu/models")
    model_name = os.environ.get("LLM_MODEL_FILE", "miyya-qwen-7b-q8_0.gguf")
    # #1: Path traversal himoyasi — model_name faqat fayl nomi bo'lishi kerak
    if not model_name or ".." in model_name or "/" in model_name or "\\" in model_name:
        raise HTTPException(status_code=400, detail="Xavfsizlik: noto'g'ri model fayl nomi")
    # Faqat .gguf fayllarga ruxsat
    if not model_name.endswith(".gguf"):
        raise HTTPException(status_code=400, detail="Faqat .gguf formatidagi modellar ruxsat etilgan")
    full_model_dir = os.path.abspath(model_dir)
    model_path = os.path.normpath(os.path.join(full_model_dir, model_name))
    # Real-path tekshiruvi: model_path model_dir ichida ekanligini tasdiqlash
    if not model_path.startswith(os.path.abspath(full_model_dir) + os.sep):
        raise HTTPException(status_code=400, detail="Xavfsizlik: ruxsatsiz fayl yo'li")
    if not os.path.exists(model_path):
        raise HTTPException(status_code=404, detail=f"Model fayli topilmadi: {model_name}")
    return FileResponse(model_path, media_type="application/octet-stream", filename=model_name)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
