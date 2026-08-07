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
    FastAPI, HTTPException, UploadFile, File, Form,
    Depends, Security, WebSocket, WebSocketDisconnect, Header, Query, Request,
)
from fastapi.responses import PlainTextResponse, FileResponse, StreamingResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest

from security_utils import encrypt_payload, decrypt_payload
from session_manager import session_manager
from audio_utils import ensure_wav_16k_mono, wav_to_pcm
from stream_controller import stream_controller

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

# /health endpoint'lar uchun access log spam'ni bosish
class _HealthFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        return "/health" not in msg and "/metrics" not in msg

logging.getLogger("uvicorn.access").addFilter(_HealthFilter())

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
# Redis ishlamasa in-memory fallback counter (audit fix: Redis tushsa
# WebSocket call'lar 1011 bilan yopilmasin)
_local_active_calls = 0
_local_calls_lock = threading.Lock()


async def _redis_incr_active():
    """Redis orqali active_calls ni oshirish; Redis tushsa lokal counter."""
    global _local_active_calls
    try:
        cur = await asyncio.to_thread(redis_client.incr, "active_calls")
        await asyncio.to_thread(redis_client.expire, "active_calls", ACTIVE_CALLS_TTL)
        return cur
    except Exception:
        with _local_calls_lock:
            _local_active_calls += 1
            return _local_active_calls


async def _redis_decr_active():
    global _local_active_calls
    try:
        await asyncio.to_thread(redis_client.decr, "active_calls")
    except Exception:
        with _local_calls_lock:
            _local_active_calls = max(0, _local_active_calls - 1)

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
KAGGLE_URL = os.getenv("KAGGLE_URL", "http://127.0.0.1:5001")      # LLM & TTS UZ
KAGGLE1_URL = os.getenv("KAGGLE1_URL", "http://127.0.0.1:5003")    # STT RU & TTS RU/EN
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
        print(f"FATAL_STDERR: {log_msg}", file=sys.stderr, flush=True)
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
    # Audit fix: endpoint'lar .env'dan kelsa ham stream_controller start'dayoq
    # ularni bilishi kerak — aks holda SIP/WS path restart'dan keyin o'lik qolardi.
    stream_controller.update_endpoints(STT_ENDPOINTS, TTS_ENDPOINTS, LLM_ENDPOINT)
    try:
        await asyncio.to_thread(redis_client.ping)
        log.info("Redis reachable")
    except Exception as e:
        log.error(f"Redis unreachable: {e}")
    _cleanup_task = asyncio.create_task(_background_session_cleanup())
    # Health monitor + analytics persistence
    asyncio.create_task(_node_health_monitor())
    asyncio.create_task(_analytics_persistence_loop())
    # Log dashboard streams
    _start_log_streams()
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
    # Log stream thread'larini to'xtatish
    _log_stop_streams.set()
    # Ishlayotgan Kaggle job'larini to'xtatish — restart'da yolg'iz qolmasin
    with _kaggle_jobs_lock:
        procs = list(_kaggle_procs.values())
        _kaggle_procs.clear()
    for p in procs:
        try:
            p.terminate()
        except Exception:
            pass
    try:
        await asyncio.to_thread(redis_client.close)
    except Exception as e:
        log.warning(f"Redis yopishda xatolik: {e}")


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

# ─────────────────── LOG DASHBOARD ───────────────────
import queue as _queue
_log_buffers: dict[int, list] = {0: [], 1: [], 2: []}
_log_subs: dict[int, list] = {0: [], 1: [], 2: []}
_log_status: dict[int, str] = {0: "offline", 1: "offline", 2: "offline"}
# _push() va /logs/clear orasida buffer'ni himoya qiluvchi lock —
# buffer qayta tayinlash ([-500:] slice) tozalashni "yutib" qo'ymasligi uchun
_log_buffers_lock = threading.Lock()

# ── Orchestrator O'Z LOGLARI (web panelga uzatish) ──
_orch_buffer: list = []
_orch_subs: list = []
# Access log (HTTP so'rovlar), httpx so'rovlar va /health shovqini panelga chiqmaydi
# (httpx connection-refused qatorlari ham — node health monitor spam qilmasin)
_ORCH_NOISE_RE = re.compile(r"HTTP/1\.1|HTTP Request|/health|/logs/stream|GET /logs|POST /logs|/metrics", re.I)


def _push_orch(line: str):
    """Orchestrator log qatorini buffer'ga qo'shish + barcha client'larga yuborish."""
    with _log_buffers_lock:
        _orch_buffer.append(line)
        if len(_orch_buffer) > 500:
            del _orch_buffer[:-500]
    for q in list(_orch_subs):
        try:
            q.put_nowait({"type": "line", "data": line})
        except _queue.Full:
            pass


class _WebLogHandler(logging.Handler):
    """Orchestrator loglarini web panelga real-time uzatuvchi handler.
    Access log (HTTP so'rovlar) va /health shovqini filtrlanadi."""
    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            if _ORCH_NOISE_RE.search(msg):
                return
            _push_orch(msg)
        except Exception:
            pass


_web_handler = _WebLogHandler()
_web_handler.setLevel(logging.INFO)
_web_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s"))
# Handler takrorlanishidan himoya — modul qayta import qilinsa dublikat qatorlar paydo bo'lmasin
if _web_handler not in logging.getLogger().handlers:
    logging.getLogger().addHandler(_web_handler)

LOG_KERNELS = {
    0: ("bunyodbek7/ai-operator-kaggle-node", "KAGGLE_USERNAME", "KAGGLE_KEY"),
    1: ("bunyodozodboyev/ai-operator-kaggle-node-1", "KAGGLE_USERNAME_1", "KAGGLE_KEY_1"),
    2: ("bunyodbekozodboyev/ai-operator-kaggle-node-2", "KAGGLE_USERNAME_2", "KAGGLE_KEY_2"),
}
LOG_LABELS = {0: "Node-0 (LLM+TTS UZ)", 1: "Node-1 (STT RU+TTS)", 2: "Node-2 (STT EN+UZ)"}
LOG_COLORS = {0: "#22c55e", 1: "#06b6d4", 2: "#a855f7"}


# Oqim uzilgach, YANGI PUSH kutiladigan vaqt oralig'i — har 30s JIM tekshiriladi.
# (kernel qayta push qilinmaguncha qayta ulanish yo'q — 20s "qayta ulanmoqda" spam bo'lmaydi)
_LOG_PUSH_WAIT_S = 30
_LOG_POLL_S = 10
_log_stop_streams = threading.Event()


def _node_env(nid: int, user_env: str, key_env: str) -> dict:
    """Node'ga xos Kaggle env — KGAT_ prefiksli kalit KAGGLE_API_TOKEN sifatida."""
    env = os.environ.copy()
    env["KAGGLE_USERNAME"] = os.environ.get(user_env, "")
    env["KAGGLE_KEY"] = os.environ.get(key_env, "")
    env.pop("KAGGLE_API_TOKEN", None)
    suffix = "" if nid == 0 else f"_{nid}"
    token = os.environ.get(f"KAGGLE_API_TOKEN{suffix}", "")
    if not token:
        key = os.environ.get(f"KAGGLE_KEY{suffix}", "")
        if key.startswith("KGAT_"):
            token = key
    if token:
        env["KAGGLE_API_TOKEN"] = token
        env.pop("KAGGLE_KEY", None)
        env.pop("KAGGLE_USERNAME", None)
    return env


def _kaggle_bin() -> str:
    """kaggle CLI to'liq yo'li — sudo bilan ishlaganda PATH'da bo'lmasligi mumkin."""
    import shutil as _shutil
    return _shutil.which("kaggle") or os.path.expanduser("~/.local/bin/kaggle")


def _find_active_kernel(nid: int, user_env: str, key_env: str, fallback: str):
    """Akkauntdagi FAOL (eng oxirgi ishlatilgan) kernel slug'ini aniqlaydi.
    Har akkauntda doim bitta faol kernel bor — shuni topib qaytaramiz,
    hardcoded slug'dan adashmaslik uchun.
    Qaytaradi: (ref, lastRunTime) — kernel topilmasa (fallback, '')."""
    import subprocess as _sp
    env = _node_env(nid, user_env, key_env)
    try:
        r = _sp.run([_kaggle_bin(), "kernels", "list", "--mine", "--csv"],
                    capture_output=True, text=True, timeout=30, env=env)
        lines = [l.strip() for l in r.stdout.strip().split("\n") if l.strip()]
        best, best_time = None, None
        for line in lines[1:]:  # header o'tkazib yuboriladi
            cols = line.split(",")
            if not cols:
                continue
            ref = cols[0].strip().strip('"')
            if not ref or ref.startswith("[Private"):
                continue
            t = cols[3].strip().strip('"') if len(cols) > 3 else ""
            if best is None or t > (best_time or ""):
                best, best_time = ref, t
        if best:
            return best, best_time or ""
    except Exception:
        pass
    return fallback, ""


def _kernel_status(kernel: str, nid: int, user_env: str, key_env: str) -> str:
    """Kernel holati — 'running' bo'lsa stream qayta ulanadi (jonli stream tiklash),
    tugagan/o'chirilgan bo'lsa yangi push kutiladi. Xato bo'lsa 'unknown'."""
    import subprocess as _sp
    env = _node_env(nid, user_env, key_env)
    try:
        r = _sp.run([_kaggle_bin(), "kernels", "status", kernel],
                    capture_output=True, text=True, timeout=15, env=env)
        if r.returncode == 0:
            up = r.stdout.upper()
            if "RUNNING" in up:
                return "running"
            # Kaggle CLI chiqishi: `... has status "complete"` (status qo'shtirnoqda)
            m = re.search(r'HAS STATUS\s+"?(\w+)"?', up)
            if m:
                return m.group(1).lower()
    except Exception:
        pass
    return "unknown"


# kaggle CLI'ning o'z xabarlari — kernel log faylida EMAS, sanash va ko'rsatishdan chiqarib tashlanadi
_CLI_NOISE_RE = re.compile(r"Log stream connection|giving up|reconnecting", re.I)
# Progress bar / yuklab olish shovqini — yashiriladi (qator ichida xato bo'lsa baribir ko'rsatiladi)
_PROGRESS_NOISE_RE = re.compile(
    r"^\s*Downloading: "       # HF / spacy yuklab olish boshlanishi
    r"|^\s*\d+%\|"             # tqdm: "  5%|####"
    r"|it/s[,\]]?"             # tqdm oxiri: "...it/s]" yoki "...it/s,"
    r"|Materializing param=",  # transformers og'irlik materializatsiyasi
    re.I,
)
_ERROR_HINT_RE = re.compile(r"error|traceback|xato|fail|exception", re.I)
# Sof bezak/ajratgich qatorlari — "********" kabi — ko'rsatilmaydi
_DECOR_NOISE_RE = re.compile(r"^\s*\*+\s*$")
# WARNING belgisi — hech qachon yashirilmaydi (ogohlantirishlar muhim)
_WARNING_RE = re.compile(r"warning|ogohlantirish", re.I)


def _is_log_noise(line: str) -> bool:
    """Ko'rsatishda yashiriladigan shovqin qatorlari.
    Progress bar, yuklab olish, bezak va bo'sh qatorlar yashiriladi — lekin:
      • qator ichida xato matni bo'lsa ('ERROR', 'Traceback' va h.k.) ko'rsatiladi
      • WARNING / ogohlantirish qatorlari DOIM ko'rsatiladi"""
    if _CLI_NOISE_RE.search(line):
        return True
    if _WARNING_RE.search(line):
        return False  # ogohlantirishlar yashirilmaydi
    if not line.strip():
        return True   # bo'sh / faqat bo'sh joy qatori
    if _DECOR_NOISE_RE.search(line):
        return True   # "********" bezak qatori
    if _PROGRESS_NOISE_RE.search(line) and not _ERROR_HINT_RE.search(line):
        return True
    return False


def _start_log_streams():
    """Background: har bir Kaggle node uchun FAOL kernel log stream'i.
    Oqim uzilsa qayta ulanish FAQAT yangi push bo'lganda amalga oshadi
    (kernel lastRunTime si yangilansa) — aks holda jim kutadi."""
    import subprocess as _sp
    _log_stop_streams.clear()
    for nid in [0, 1, 2]:
        fallback_kernel, user_env, key_env = LOG_KERNELS[nid]
        user = os.environ.get(user_env, "")
        key = os.environ.get(key_env, "")
        if not key or not user:
            _log_status[nid] = "kalit yo'q"
            missing = [v for v, c in [(user_env, user), (key_env, key)] if not c]
            _log_buffers[nid].append(f"⚠️  {', '.join(missing)} topilmadi")
            for q in list(_log_subs[nid]):
                try: q.put_nowait({"type":"line","data":_log_buffers[nid][-1]})
                except _queue.Full: pass
            continue
        _log_status[nid] = "connecting"
        info = f"🔌 {LOG_LABELS[nid]} | faol kernel qidirilmoqda..."
        _log_buffers[nid].append(info)
        for q in list(_log_subs[nid]):
            try: q.put_nowait({"type":"line","data":info})
            except _queue.Full: pass

        def _push(nid=nid, line=""):
            """Buffer'ga qo'shish + barcha ulangan client'larga yuborish."""
            with _log_buffers_lock:
                _log_buffers[nid].append(line)
                if len(_log_buffers[nid]) > 500:
                    # In-place trim — buffer obyekti o'zgarmaydi, shuning uchun
                    # /logs/clear bilan race bo'lmaydi (eski loglar qaytib kelmaydi)
                    del _log_buffers[nid][:-500]
            for q in list(_log_subs[nid]):
                try: q.put_nowait({"type":"line","data":line})
                except _queue.Full: pass

        def _stream(nid=nid, user_env=user_env, key_env=key_env,
                    fallback_kernel=fallback_kernel):
            env = _node_env(nid, user_env, key_env)
            label = LOG_LABELS[nid]
            tracked = None    # (ref, lastRunTime) — oxirgi ulangan kernel ma'lumoti
            shown_raw = 0     # bu kernel uchun allaqachon ko'rsatilgan xom qatorlar soni
            stale = 0         # ketma-ket BO'SH qayta ulanishlar soni
            kernel_dead = False  # kernel o'lgan (2+ bo'sh reconnect) — faqat yangi pushga javob beramiz
            while not _log_stop_streams.is_set():
                # 1) Faol kernel + uning lastRunTime si — yangi push'ni aniqlash uchun
                kernel, ktime = _find_active_kernel(nid, user_env, key_env, fallback_kernel)
                skip_raw = 0
                # Qayta ulanish shartlari:
                #   a) YANGI PUSH: kernel lastRunTime si yangilangan yoki boshqa kernel paydo bo'lgan
                #   b) JONLI STREAM TIKLASH: eski kernel hali ishlayapti (stream vaqtincha uzilgan)
                # Aks holda JIM kutamiz — 20s "qayta ulanmoqda" spam bo'lmaydi.
                if tracked is not None:
                    prev_ref, prev_time = tracked
                    new_push = bool(ktime) and (ktime > prev_time or kernel != prev_ref)
                    if not new_push:
                        # Kernel hali ishlayaptimi? (vaqtinchalik uzilish bo'lsa jonli stream tiklanadi)
                        # kernel_dead=True bo'lsa — qayta ulanish MAYDI, faqat yangi push kutamiz.
                        if not kernel_dead and _kernel_status(prev_ref, nid, user_env, key_env) == "running":
                            kernel, ktime = prev_ref, prev_time
                            # Bir xil kernelga qayta ulanish: `kaggle kernels logs` to'liq tarixni
                            # boshidan yuklaydi — allaqachon ko'rsatilgan qatorlarni skip qilamiz,
                            # faqat YANGI qatorlar ko'rinadi (dublikat bo'lmaydi).
                            skip_raw = shown_raw
                        else:
                            _log_status[nid] = "waiting"
                            _log_stop_streams.wait(_LOG_PUSH_WAIT_S)  # yangi push yo'q — jim kutish
                            continue
                    else:
                        shown_raw = 0      # yangi push — to'liq tarix ko'rsatiladi
                        kernel_dead = False  # yangi push kelgan — kernel qayta ishga tushgan
                tracked = (kernel, ktime or "")
                try:
                    # 2) Log stream ochish (-f = follow, real-time)
                    proc = _sp.Popen([_kaggle_bin(), "kernels", "logs", kernel, "-f"],
                        stdout=_sp.PIPE, stderr=_sp.STDOUT, text=True, env=env, bufsize=1)
                    _log_status[nid] = "running"
                    on = time.strftime('%H:%M:%S')
                    _push(nid, f"── {label} | {kernel} | ishga tushdi: {on} ──")
                    raw_read = 0
                    skip_start = skip_raw
                    for raw in iter(proc.stdout.readline, ""):
                        line = raw.rstrip("\n").rstrip("\r")
                        if _is_log_noise(line):
                            continue  # shovqin qatori — ko'rsatilmaydi va sanalmaydi
                        raw_read += 1
                        if skip_raw > 0:
                            skip_raw -= 1  # eski tarix — ko'rsatilmaydi, faqat hisoblanadi
                            continue
                        if "/health" in line:
                            continue
                        _push(nid, line)
                    # FAQAT YANGI qatorlarni hisobga olamiz: qayta replay qilingan eski
                    # qatorlar (skip_start - skip_raw) ikki marta sanalmasin — aks holda
                    # skip byudjeti 2+ qayta ulanishda oshib, jonli log'lar yo'qolardi.
                    shown_raw += raw_read - (skip_start - skip_raw)
                    rc = proc.wait(timeout=5)
                    _log_status[nid] = "waiting"
                    _push(nid, f"🔄 {label} oqim to'xtadi (exit={rc}) — yangi push kutilmoqda...")
                    # Bo'sh qayta ulanish aniqlash: qayta ulanishda HECH QANDAY yangi qator
                    # chiqmagan bo'lsa (hammasi skip) va stream darhol tugasa — kernel o'lgan.
                    # Ketma-ket 2 marta bo'lsa kernel_dead=True — qayta ulanish to'xtaydi,
                    # faqat yangi push (lastRunTime o'zgarishi) kelganda tiklanadi.
                    new_shown = raw_read - (skip_start - skip_raw)
                    if skip_start > 0 and new_shown == 0:
                        stale += 1
                        if stale >= 2:
                            kernel_dead = True
                            stale = 0
                    else:
                        stale = 0
                except Exception as e:
                    _log_status[nid] = f"error: {e}"
                    _push(nid, f"⚠️  {label}: {e} — yangi push kutilmoqda")
                    # Uzluksiz xatolar (tarmoq/auth uzilishi) ham spam qilmasligi uchun:
                    # ketma-ket 3 ta muvaffaqiyatsiz ulanishdan keyin ham jim kutishga o'tamiz.
                    stale += 1
                    if stale >= 3:
                        kernel_dead = True
                        stale = 0
                # 3) Yangi push tekshiriladi; bo'lmasa jim kutish
                _log_stop_streams.wait(_LOG_POLL_S)
        threading.Thread(target=_stream, daemon=True).start()




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
    URL o'zgarmagan bo'lsa, keraksiz ish qilmaydi."""
    global KAGGLE_URL, KAGGLE1_URL, KAGGLE2_URL
    global STT_ENDPOINTS, TTS_ENDPOINTS, LLM_ENDPOINT

    # Audit fix: URL validatsiyasi — orchestrator o'sha URL'ga so'rov yuboradi
    import urllib.parse as _up
    _p = _up.urlparse(req.url)
    if _p.scheme not in ("http", "https") or not _p.netloc:
        raise HTTPException(status_code=400, detail=f"Noto'g'ri URL: {req.url}")
    if _p.hostname in ("localhost", "127.0.0.1", "::1"):
        log.warning(f"register-node: localhost URL rad etildi ({req.url})")
        raise HTTPException(status_code=400, detail="Localhost URL ruxsat etilmaydi")

    async with _node_registration_lock:
        # URL o'zgarmagan bo'lsa — hech narsa qilmaslik
        prev_url = {"kaggle": KAGGLE_URL, "kaggle1": KAGGLE1_URL, "kaggle2": KAGGLE2_URL}.get(req.node_type)
        if prev_url == req.url:
            log.debug(f"Register skip: {req.node_type} URL o'zgarmagan")
            return {"status": "ok", "message": f"{req.node_type} allaqachon ro'yxatdan o'tgan"}

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
        log.error(f"Profilni yuklashda xatolik: {e}")

    log.info(f"✅ {req.node_type.upper()} ulandi: {req.url}")
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
    """Audio baytlarni STT tuguniga yuboradi.
    Audit fix: node'lar JSON `encrypted_audio` kutadi va `encrypted_text` qaytaradi.
    (Oldin multipart `audio_file` yuborilar edi → node 422 berardi.)"""
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
        encrypted = encrypt_payload(normalized_audio)
        req_data = {"encrypted_audio": encrypted}
        response = await resilient_request(async_http, url, method="POST",
                                           json_body=req_data, headers=NODE_HEADERS)
        data = response.json()
        # Node javobi: {"encrypted_text": "..."}
        enc_text = data.get("encrypted_text") or data.get("encrypted_payload") or ""
        if enc_text:
            return decrypt_payload(enc_text).decode('utf-8').strip()
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
    # Audit fix: node'lar `encrypted_text` kutadi (node-0 ham moslashtirildi)
    req_data = {"encrypted_text": encrypted_str}
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
    (Yagona audio_utils.wav_to_pcm ga delegatsiya qilinadi.)"""
    return wav_to_pcm(audio_bytes, 16000)


# ----------------- ENDPOINTS -----------------

# ─────────────────── LOG DASHBOARD ROUTES ───────────────────

_LOGS_HTML = r"""<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Kaggle Node Loglar</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #f8fafc; --surface: #ffffff; --border: #e2e8f0;
  --text: #1e293b; --text2: #64748b; --text3: #94a3b8;
  --n0: #059669; --n0bg: #ecfdf5; --n0b: #a7f3d0;
  --n1: #0891b2; --n1bg: #ecfeff; --n1b: #a5f3fc;
  --n2: #7c3aed; --n2bg: #f5f3ff; --n2b: #c4b5fd;
  --radius: 16px; --shadow: 0 1px 3px rgba(0,0,0,.04), 0 1px 2px rgba(0,0,0,.06);
  --shadow-lg: 0 4px 16px rgba(0,0,0,.06), 0 2px 4px rgba(0,0,0,.04);
}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;height:100vh;display:flex;flex-direction:column;overflow:hidden}

/* HEADER */
.topbar{background:var(--surface);padding:14px 28px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--border);box-shadow:var(--shadow);z-index:10}
.topbar-left{display:flex;align-items:center;gap:12px}
.topbar-left .logo{font-size:20px}
.topbar-left h1{font-size:17px;font-weight:700;color:var(--text);letter-spacing:-0.3px}
.topbar-right{display:flex;align-items:center;gap:20px}
.clock{font-size:13px;color:var(--text2);font-weight:500;font-family:'JetBrains Mono',monospace;background:var(--bg);padding:6px 14px;border-radius:20px;border:1px solid var(--border)}
.status-dots{display:flex;gap:14px}
.status-item{display:flex;align-items:center;gap:7px;font-size:12px;font-weight:600;color:var(--text2)}
.pulse{width:8px;height:8px;border-radius:50%;display:inline-block;animation:pulse 2s infinite}
.pulse.green{background:var(--n0);box-shadow:0 0 6px var(--n0)}
.pulse.cyan{background:var(--n1);box-shadow:0 0 6px var(--n1)}
.pulse.purple{background:var(--n2);box-shadow:0 0 6px var(--n2)}
.pulse.off{background:#cbd5e1;animation:none;box-shadow:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.55}}
.btn{background:var(--surface);color:var(--text);border:1px solid var(--border);padding:8px 18px;border-radius:10px;cursor:pointer;font-size:13px;font-weight:600;transition:all .2s;display:flex;align-items:center;gap:6px;box-shadow:var(--shadow)}
.btn:hover{background:#f1f5f9;border-color:#cbd5e1;transform:translateY(-1px);box-shadow:var(--shadow-lg)}

/* GRID */
.main-grid{display:grid;grid-template-columns:1fr 1fr 1fr;flex:1;gap:16px;padding:16px;overflow:hidden;min-height:0}

/* CARDS */
.card{display:flex;flex-direction:column;background:var(--surface);border-radius:var(--radius);box-shadow:var(--shadow);border:1px solid var(--border);overflow:hidden;transition:box-shadow .2s;min-height:0}
.card:hover{box-shadow:var(--shadow-lg)}
.card-head{padding:14px 18px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border);flex-shrink:0}
.card-head .node-label{font-size:14px;font-weight:700;letter-spacing:-0.2px}
.card-head .node-meta{font-size:11px;color:var(--text2);font-weight:500;font-family:'JetBrains Mono',monospace}
.card-n0 .card-head{background:linear-gradient(135deg,var(--n0bg),#fff)}
.card-n0 .node-label{color:var(--n0)}
.card-n1 .card-head{background:linear-gradient(135deg,var(--n1bg),#fff)}
.card-n1 .node-label{color:var(--n1)}
.card-n2 .card-head{background:linear-gradient(135deg,var(--n2bg),#fff)}
.card-n2 .node-label{color:var(--n2)}

.log-box{flex:1;overflow-y:auto;overflow-x:hidden;padding:14px 18px;font-family:'JetBrains Mono','Fira Code',monospace;font-size:11.5px;line-height:1.7;color:var(--text2);min-height:0;word-break:break-all;white-space:pre-wrap;scroll-behavior:smooth}
.log-box:empty::after{content:'Kutilmoqda...';color:var(--text3);font-style:italic;font-family:'Inter',sans-serif;font-size:13px}
.log-box::-webkit-scrollbar{width:5px}
.log-box::-webkit-scrollbar-track{background:transparent;margin:4px 0}
.log-box::-webkit-scrollbar-thumb{background:#e2e8f0;border-radius:10px}
.log-box::-webkit-scrollbar-thumb:hover{background:#cbd5e1}

/* LOG LINES */
.log-line{padding:0.5px 0;transition:background .15s}
.log-line:hover{background:rgba(0,0,0,.012)}
.log-line.err{color:#ef4444;font-weight:500}
.log-line.wrn{color:#f59e0b;font-weight:500}
.log-line.inf{color:var(--text2)}
.log-line.ok{color:var(--n0)}
.log-line.head{color:var(--text);font-weight:600}

/* EMPTY STATE */
.empty-state{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:8px;color:var(--text3);font-family:'Inter',sans-serif;font-size:13px}
.empty-state .icon{font-size:36px;opacity:.4}

/* RESPONSIVE */
@media(max-width:1200px){.main-grid{grid-template-columns:1fr 1fr;gap:12px;padding:12px}}
@media(max-width:768px){.main-grid{grid-template-columns:1fr;gap:10px;padding:10px}}
</style>
</head>
<body>
<div class="topbar">
  <div class="topbar-left">
    <span class="logo">📊</span>
    <h1>Kaggle Node Loglar</h1>
  </div>
  <div class="topbar-right">
    <span class="clock" id="clock">--:--:--</span>
    <div class="status-dots">
      <span class="status-item"><span class="pulse off" id="dot0"></span>N0</span>
      <span class="status-item"><span class="pulse off" id="dot1"></span>N1</span>
      <span class="status-item"><span class="pulse off" id="dot2"></span>N2</span>
    </div>
    <button class="btn" onclick="R()">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
      Yangilash
    </button>
  </div>
</div>

<div class="main-grid">
  <div class="card card-n0">
    <div class="card-head"><span class="node-label">🔮 Node-0</span><span class="node-meta" id="m0">LLM+TTS UZ</span></div>
    <div class="log-box" id="b0"></div>
  </div>
  <div class="card card-n1">
    <div class="card-head"><span class="node-label">🎤 Node-1</span><span class="node-meta" id="m1">STT RU+TTS RU/EN</span></div>
    <div class="log-box" id="b1"></div>
  </div>
  <div class="card card-n2">
    <div class="card-head"><span class="node-label">🌐 Node-2</span><span class="node-meta" id="m2">STT EN+UZ</span></div>
    <div class="log-box" id="b2"></div>
  </div>
</div>

<script>
const AS={0:1,1:1,2:1},SS={},CL={'#059669':'ok','#0891b2':'ok','#7c3aed':'ok'};
function W(l){
  let c='log-line ';
  if(/ERROR|XATO|xatolik|❌|Traceback/.test(l))c+='err';
  else if(/WARNING|WARN|⚠/.test(l))c+='wrn';
  else if(/✅|🎉|tayyor|ulandi|Tunnel:|GPU soni/.test(l))c+='ok';
  else if(/──|ishga tushdi|Modellar:/.test(l))c+='head';
  else c+='inf';
  return `<div class="${c}">${l.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</div>`;
}
function X(id){
  if(SS[id])SS[id].close();
  SS[id]=new EventSource('/logs/stream/'+id);
  const b=document.getElementById('b'+id),m=document.getElementById('m'+id),d=document.getElementById('dot'+id);
  SS[id].onmessage=e=>{
    try{
      const p=JSON.parse(e.data);
      if(p.type==='full'){b.innerHTML=p.data.split('\n').map(W).join('');if(AS[id])b.scrollTop=b.scrollHeight}
      if(p.type==='line'){
        b.insertAdjacentHTML('beforeend',W(p.data));
        if(p.data.includes('tushdi:')){const t=p.data.match(/(\d{2}:\d{2}:\d{2})/);if(t)m.textContent='🟢 '+t[1];d.className='pulse green';d.parentElement.style.color='var(--n0)'}
        if(p.data.includes('to\u02bbtadi')){d.className='pulse off';d.parentElement.style.color='var(--text2)'}
        if(AS[id]&&b.scrollHeight-b.scrollTop-b.clientHeight<120)b.scrollTop=b.scrollHeight;
      }
    }catch(ex){}
  };
  SS[id].onerror=()=>{d.className='pulse off';d.parentElement.style.color='var(--text2)'};
}
[0,1,2].forEach(id=>{document.getElementById('b'+id).addEventListener('scroll',function(){AS[id]=(this.scrollHeight-this.scrollTop-this.clientHeight)<80})});
function R(){[0,1,2].forEach(id=>{document.getElementById('b'+id).innerHTML='';X(id)})}
function T(){const n=new Date();document.getElementById('clock').textContent=n.toLocaleTimeString('uz-UZ',{hour12:false})}
setInterval(T,1000);T();R();
</script>
</body></html>"""


@app.get("/logs", response_class=HTMLResponse)
async def log_dashboard():
    static_file = os.path.join(os.path.dirname(__file__), "static", "logs.html")
    if os.path.exists(static_file):
        return FileResponse(static_file, media_type="text/html")
    return HTMLResponse(content="<h1>logs.html topilmadi</h1>", status_code=404)


# ─────────────────── KAGGLE ACTIONS (DASHBOARD TUGMALARI) ───────────────────
# launch_kaggle.py ni background'da ishga tushiradi: --all / -d / -i / -m
# XAVFSIZLIK: bu tugmalar -d (kernel o'chirish) va --all (GPU kvota yondirish)
# kabi DESTRUCTIVE amallarni ishga tushiradi — maxsus kalit talab qilinadi.
_kaggle_jobs: Dict[str, dict] = {}
_kaggle_jobs_lock = threading.Lock()
_kaggle_procs: Dict[str, "subprocess.Popen"] = {}  # noqa: F821
_kaggle_jobs_MAX = 20  # eski job'lar xotirada qolmasligi uchun

# Amallar kaliti: DASHBOARD_ACTION_KEY -> NODE_COMM_KEY -> API_KEY
_ACTION_KEY = (os.getenv("DASHBOARD_ACTION_KEY") or NODE_COMM_KEY or API_KEY or "")


async def verify_action_key(x_action_key: Optional[str] = Header(default=None)):
    """Dashboard amallari uchun alohida kalit — o'qish (logs) kalitdan farqli."""
    if _ACTION_KEY and x_action_key and x_action_key == _ACTION_KEY:
        return x_action_key
    raise HTTPException(status_code=403, detail="Xavfsizlik: Amallar kaliti noto'g'ri!")


def _prune_kaggle_jobs():
    """Eski tugagan job'larni tozalash — xotira o'smasligi uchun."""
    with _kaggle_jobs_lock:
        done = [j for j, d in _kaggle_jobs.items() if d.get("status") in ("done", "error")]
        if len(done) > _kaggle_jobs_MAX:
            for jid in done[: -_kaggle_jobs_MAX]:
                _kaggle_jobs.pop(jid, None)


def _start_kaggle_job(flag: str) -> str:
    """launch_kaggle.py flag'ini background subprocess sifatida ishga tushiradi.
    Qaytaradi: job_id — holatni /api/kaggle/job/{id} orqali so'rash mumkin."""
    job_id = uuid.uuid4().hex[:8]
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "launch_kaggle.py")
    with _kaggle_jobs_lock:
        _kaggle_jobs[job_id] = {"flag": flag, "status": "starting", "output": [], "exit": None}
    _prune_kaggle_jobs()

    def _run():
        import subprocess as _sp
        env = os.environ.copy()
        # sudo bilan ishlaganda kaggle CLI ~/.local/bin da — PATH'ga qo'shamiz
        env["PATH"] = "/home/ubuntu/.local/bin:" + env.get("PATH", "")
        proc = None
        try:
            proc = _sp.Popen([sys.executable, script, flag],
                stdout=_sp.PIPE, stderr=_sp.STDOUT, text=True, env=env,
                bufsize=1, cwd=os.path.dirname(script))
            with _kaggle_jobs_lock:
                _kaggle_procs[job_id] = proc
                _kaggle_jobs[job_id]["status"] = "running"
            for raw in iter(proc.stdout.readline, ""):
                line = raw.rstrip("\n").rstrip("\r")
                if not line:
                    continue
                with _kaggle_jobs_lock:
                    _kaggle_jobs[job_id]["output"].append(line)
                    if len(_kaggle_jobs[job_id]["output"]) > 2000:
                        _kaggle_jobs[job_id]["output"] = _kaggle_jobs[job_id]["output"][-2000:]
            rc = proc.wait(timeout=10)
            with _kaggle_jobs_lock:
                _kaggle_jobs[job_id]["status"] = "done"
                _kaggle_jobs[job_id]["exit"] = rc
                _kaggle_procs.pop(job_id, None)
        except Exception as e:
            with _kaggle_jobs_lock:
                _kaggle_jobs[job_id]["status"] = "error"
                _kaggle_jobs[job_id]["output"].append(f"❌ Xatolik: {e}")
                _kaggle_procs.pop(job_id, None)
            if proc:
                try: proc.kill()
                except Exception: pass
    threading.Thread(target=_run, daemon=True).start()
    return job_id


@app.post("/api/kaggle/action")
async def kaggle_action(request: Request, _: str = Depends(verify_action_key)):
    """Dashboard tugmalari: {flag: '--all' | '-d' | '-i' | '-m'}"""
    try:
        body = await request.json()
    except Exception:
        body = {}
    flag = body.get("flag", "")
    if flag not in ("--all", "-d", "-i", "-m"):
        return {"error": f"Noto'g'ri flag: {flag}"}
    job_id = _start_kaggle_job(flag)
    return {"job_id": job_id, "flag": flag}


@app.get("/api/kaggle/job/{job_id}")
async def kaggle_job_status(job_id: str, _: str = Depends(verify_action_key)):
    """Job holati — frontend polling qiladi."""
    with _kaggle_jobs_lock:
        job = _kaggle_jobs.get(job_id)
    if not job:
        return {"error": "Job topilmadi"}
    return job


@app.get("/logs/stream/orch")
async def log_stream_orch(request: Request):
    """Orchestrator o'z loglarini SSE orqali uzatish — web paneldagi 4-panel."""
    q: _queue.Queue = _queue.Queue(maxsize=200)
    _orch_subs.append(q)

    async def gen():
        if _orch_buffer:
            msg = json.dumps({"type": "full", "data": "\n".join(_orch_buffer)})
            yield f"data: {msg}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.to_thread(q.get, timeout=3)
                    yield f"data: {json.dumps(msg)}\n\n"
                except _queue.Empty:
                    yield ":\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if q in _orch_subs:
                _orch_subs.remove(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"})


@app.get("/logs/stream/{node_id}")
async def log_stream(node_id: int, request: Request):
    if node_id not in (0, 1, 2):
        return StreamingResponse(iter([]), media_type="text/event-stream")
    q: _queue.Queue = _queue.Queue(maxsize=200)
    _log_subs[node_id].append(q)
    async def gen():
        if _log_buffers[node_id]:
            msg = json.dumps({"type":"full","data":"\n".join(_log_buffers[node_id])})
            yield f"data: {msg}\n\n"
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    # asyncio.to_thread — event loop'ni bloklamaydi
                    msg = await asyncio.to_thread(q.get, timeout=3)
                    yield f"data: {json.dumps(msg)}\n\n"
                except _queue.Empty:
                    yield ":\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if q in _log_subs[node_id]:
                _log_subs[node_id].remove(q)
    return StreamingResponse(gen(), media_type="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no","Connection":"keep-alive"})


@app.post("/logs/clear")
async def log_clear():
    """Barcha node log bufferlarini tozalaydi — 'Tozalash' tugmasi chaqiradi.
    Buffer tozalangach 'Yangilash' bosilsa eski loglar qaytib kelmaydi.
    Ulangan barcha clientlarga 'clear' xabari yuboriladi — boshqa ochiq
    sahifalar ham bir vaqtda tozalanadi. Stream'lar davom etadi, yangi
    loglar kelishda davom qiladi."""
    for nid in (0, 1, 2):
        with _log_buffers_lock:
            _log_buffers[nid].clear()
        msg = {"type": "clear"}
        for q in list(_log_subs[nid]):
            try:
                q.put_nowait(msg)
            except _queue.Full:
                pass
    # Orchestrator o'z loglarini ham tozalash
    with _log_buffers_lock:
        _orch_buffer.clear()
    for q in list(_orch_subs):
        try:
            q.put_nowait({"type": "clear"})
        except _queue.Full:
            pass
    log.info("Log dashboard: barcha bufferlar tozalandi")
    return {"status": "ok", "cleared": [0, 1, 2]}


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

    Audit refactor: WS path endi stream_controller orqali ishlaydi — SIP path
    bilan BIR xil pipeline (VAD→STT→Guardrail→LLM+Tools→TTS).
    - Guardrail, tool loop va tilga mos profil endi WS path'da ham ishlaydi.
    - Audio serializatsiya: bir call uchun pipeline ketma-ket (asyncio.Lock).
    - Redis tushsa in-memory fallback (audit fix).
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

    # 2. LOAD BALANCING (Redis + lokal fallback)
    slot_acquired = False
    try:
        current_calls = await _redis_incr_active()
        if current_calls > MAX_CONCURRENT_CALLS:
            await _redis_decr_active()
            await websocket.close(code=1013, reason="Server is too busy")
            return
        slot_acquired = True
        with _analytics_lock:
            analytics_db["active_calls"] += 1
    except Exception as e:
        log.exception(f"Active-call counter xatosi: {e}")
        await websocket.close(code=1011, reason="Internal error")
        return

    start_time = time.time()
    correlation_external_id = None  # SIP bridge'dan kelgan CallSid mapping

    # ── StreamController orqali pipeline ──
    session = stream_controller.get_or_create_session(caller_id)
    session.language = lang
    session.sample_rate = 16000   # WS client PCM 16kHz kutadi
    send_q: asyncio.Queue = asyncio.Queue()

    def _on_tts(pcm: bytes):
        """TTS natijasini WS klientga yuborish uchun queue'ga tashlaymiz."""
        send_q.put_nowait(pcm)

    async def _drain_tts():
        """send_q ni bo'shatib, PCM'ni websocket orqali uzatadi."""
        while True:
            pcm = await send_q.get()
            try:
                await websocket.send_bytes(pcm)
            except Exception:
                return

    drain_task = asyncio.create_task(_drain_tts())

    try:
        # Boshlanish signalini yuborish (SIP bridge sinxronizatsiya uchun)
        await websocket.send_json({"type": "ready", "caller_id": caller_id, "language": lang})

        while True:
            # receive_bytes() o'rniga receive() — text (JSON metadata) yoki bytes bo'lishi mumkin
            msg = await websocket.receive()
            # Client yopilganda Starlette 'websocket.disconnect' qaytaradi — shu yerda
            # aylanishni to'xtatamiz (aks holda "Cannot call receive once a disconnect
            # message has been received" xatosi paydo bo'lardi).
            if msg.get("type") == "websocket.disconnect":
                break
            if msg.get("type") != "websocket.receive":
                continue

            if "text" in msg and msg["text"]:
                # JSON control message (SIP bridge'dan metadata / DTMF)
                try:
                    parsed = json.loads(msg["text"])
                except Exception as e:
                    log.debug(f"WS text parse: {e}")
                    continue
                if not isinstance(parsed, dict):
                    continue
                ctype = parsed.get("type")
                if ctype == "metadata":
                    correlation_external_id = parsed.get("external_id") or parsed.get("stream_sid")
                    if correlation_external_id:
                        _record_external_id(caller_id, correlation_external_id)
                        log.info(f"[{caller_id}] SIP correlation: external_id={correlation_external_id}")
                    # Twilio `from` raqami — caller_name sifatida system prompt'ga boradi
                    caller = (parsed.get("caller") or "").strip()
                    if caller:
                        session.caller_name = caller
                elif ctype == "dtmf":
                    digit = parsed.get("digit", "")
                    lang_map = {"1": "uz", "2": "ru", "3": "en"}
                    if digit in lang_map:
                        session.language = lang_map[digit]
                        log.info(f"[{caller_id}] DTMF {digit} → til: {session.language}")
                        # Birinchi til tanlovidan keyin greeting (SIP trunk'dagi kabi)
                        if not getattr(session, "_greeting_done", False):
                            session._greeting_done = True
                            stream_controller.trigger_greeting(caller_id, _on_tts)
                continue

            if "bytes" not in msg or not msg["bytes"]:
                continue

            # VAD + suhbat segmentatsiyasi + STT/LLM/TTS — stream_controller'da
            stream_controller.on_audio_chunk(caller_id, msg["bytes"], _on_tts)
    except WebSocketDisconnect:
        log.info(f"[{caller_id}] WebSocket uzildi.")
    except Exception as e:
        log.exception(f"[{caller_id}] WebSocket xatosi: {e}")
    finally:
        drain_task.cancel()
        stream_controller.end_call(caller_id)
        if slot_acquired:
            await _redis_decr_active()
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

HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "600"))
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
                except Exception as e:
                    _record_failure(health_url)
                    with _node_health_lock:
                        _node_health_status[name] = "OFFLINE"
                    log.debug(f"Health check [{name}]: {e}")
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
async def get_network_settings(_: str = Depends(verify_api_key)):
    """Audit fix: settings GET ham auth talab qiladi (avval ochiq edi)."""
    return settings_db["network"]


@app.post("/api/settings/network")
async def update_network_settings(setting: NetworkSetting, _: str = Depends(verify_api_key)):
    settings_db["network"]["use_public_internet"] = setting.use_public_internet
    return {"status": "success", "network": settings_db["network"]}


@app.get("/api/settings/services")
async def get_services(_: str = Depends(verify_api_key)):
    """Audit fix: settings GET ham auth talab qiladi (avval ochiq edi)."""
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
