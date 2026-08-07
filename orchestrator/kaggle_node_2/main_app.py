import os, sys, json, time, re, base64, threading, logging, subprocess, uuid
import requests
import nest_asyncio
import uvicorn

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

log_path = "/kaggle/working/app.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)]
)
log = logging.getLogger("node2")
# Chet kutubxonalarning keraksiz log'larini o'chirish
for _lib in ["huggingface_hub", "transformers", "urllib3", "filelock", "httpcore", "httpx"]:
    logging.getLogger(_lib).setLevel(logging.WARNING)

os.environ["AES_256_KEY"] = "Z7krquwOJWmOtOhEDPzNuQ4sMbJ4MbtAgNqEaVziRMID"
import os
import time
import base64
import logging
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

log = logging.getLogger("security_utils")

# FAIL-FAST SECURITY: AES kaliti environmentdan keladi, default YO'Q.
# Oldin shu yerda hardcoded fallback bor edi — CRITICAL zaiflik edi
# (GitHub'da kodni ko'rgan hujumchi tarmoqdagi trafikni decrypt qila olardi).
# Endi env berilmasa import-time da xato beradi — bu STRICT_SECURITY ga
# bog'liq emas, chunki AES kaliti har doim talab qilinadi.
SHARED_SECRET_KEY = os.getenv("AES_256_KEY")
if not SHARED_SECRET_KEY:
    raise RuntimeError(
        "AES_256_KEY environment o'zgaruvchisi o'rnatilmagan. "
        "Tizim xavfsizlik sababli ishga tushmaydi. "
        "Yangi kalit generatsiya qilish: "
        "python3 -c \"import secrets; print(secrets.token_bytes(32).hex())\""
    )
# Mask format pin — versioned, so future migrations (v2, v3) remain
# self-describing and historical audit rows can still be parsed correctly.
# Format: "<version>:<prefix><truncated_sha256_hex>"
# V1: "v1:" + "kh:" + sha256[:12]
MASK_VERSION = "v1"
MASK_PREFIX = "kh:"
MASK_TRUNCATE_BYTES = 12
# Paketning maksimal yoshi (sekundlarda) — replay attack ga qarshi himoya
MAX_NONCE_AGE_SECONDS = int(os.getenv("MAX_NONCE_AGE_SECONDS", "300"))
# Oxirgi ishlatilgan nonce'lar (in-memory, TTL bilan)
_seen_nonces: dict[str, float] = {}


def get_key_bytes():
    """
    AES-256 kalitini 32 baytga normalizatsiya qiladi.

    Production'da AES_256_KEY environment orqali kiritilishi SHART (import-time
    RuntimeError bor). Bu funksiya ishlasa, demak kalit to'g'ri o'rnatilgan.
    Kalit entropiyasi < 256 bit bo'lsa ham xavfsizlik kuchsizlanadi — shuning
    uchun random 32-byte hex yoki base64 token_urlsafe(48) tavsiya qilamiz.
    """
    key = SHARED_SECRET_KEY.encode('utf-8')
    if len(key) < 32:
        key = key.ljust(32, b'0')
    elif len(key) > 32:
        key = key[:32]
    return key


def _purge_old_nonces(now: float):
    """#7 fix: Vaqtga asoslangan tozalash — har doim eski nonce'larni o'chiradi,
    faqat >5000 bo'lganda emas. Bu memory growth'ni oldini oladi."""
    cutoff = now - MAX_NONCE_AGE_SECONDS
    # Har safar tozalaymiz, faqat katta bo'lganda emas
    expired = [n for n, t in _seen_nonces.items() if t < cutoff]
    for n in expired:
        del _seen_nonces[n]
    # Safety net: agar 10,000 dan oshib ketsa (DDOS yoki vaqt drifti),
    # eng eski yarmini agressiv tozalash
    if len(_seen_nonces) > 10000:
        sorted_items = sorted(_seen_nonces.items(), key=lambda x: x[1])
        for n, _ in sorted_items[:len(sorted_items) // 2]:
            _seen_nonces.pop(n, None)
        log.warning(f"Nonce cache agressiv tozalandi: {len(_seen_nonces)} qoldi")


def encrypt_payload(data: bytes) -> str:
    """Ma'lumotni AES-256-GCM orqali shifrlaydi + timestamp qo'shib replay himoya.

    Format: base64( nonce(12) || timestamp(8) || ciphertext )
    """
    key = get_key_bytes()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ts = int(time.time()).to_bytes(8, "big")
    # Associated data (AAD) ga nonce o'zi va vaqtni bog'laydi
    ct = aesgcm.encrypt(nonce, data, ts)
    return base64.b64encode(nonce + ts + ct).decode('utf-8')


def decrypt_payload(encrypted_str: str) -> bytes:
    """Shifrlangan payload'ni ochadi, vaqt va replay tekshiruvi bilan."""
    key = get_key_bytes()
    aesgcm = AESGCM(key)
    encrypted_bytes = base64.b64decode(encrypted_str)
    if len(encrypted_bytes) < 12 + 8 + 16:  # nonce + ts + min tag
        raise ValueError("Shifrlangan payload juda qisqa")
    nonce = encrypted_bytes[:12]
    ts_bytes = encrypted_bytes[12:20]
    ts = int.from_bytes(ts_bytes, "big")
    ct = encrypted_bytes[20:]
    # Replay himoya — eski nonce'ni qayta ishlatmaslik
    now = time.time()
    if now - ts > MAX_NONCE_AGE_SECONDS or ts - now > 30:  # kelajakda ham cheklangan
        raise ValueError("Payload muddati o'tgan yoki vaqti noto'g'ri")
    nonce_key = nonce.hex()
    # Replay detection (in-memory)
    last_seen = _seen_nonces.get(nonce_key, 0)
    if last_seen and now - last_seen < MAX_NONCE_AGE_SECONDS:
        raise ValueError("Replay aniqlandi: nonce qayta ishlatilgan")
    _seen_nonces[nonce_key] = now
    _purge_old_nonces(now)
    # AAD = ts_bytes
    return aesgcm.decrypt(nonce, ct, ts_bytes)


ORCHESTRATOR_URL = "https://orchestrator.traffix.uz"
NODE_TYPE = "kaggle2"
NODE_PORT = 5002

# Keraksiz log spam'ni bosish: tqdm progress bar + transformers + uvicorn health
os.environ["TQDM_DISABLE"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
logging.getLogger("uvicorn.access").addFilter(lambda r: "/health" not in r.getMessage())

log.info("Tunnel ochilmoqda...")
# cloudflared allaqachon mavjud bo'lsa qayta yuklanmaydi
if not os.path.exists("./cloudflared"):
    subprocess.run("wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared && chmod +x cloudflared", shell=True, capture_output=True)
subprocess.Popen(["./cloudflared", "tunnel", "--url", f"http://127.0.0.1:{NODE_PORT}"], stdout=open("cloudflared.log", "w"), stderr=subprocess.STDOUT)

public_url = None
for _ in range(30):
    time.sleep(1)
    if os.path.exists("cloudflared.log"):
        with open("cloudflared.log") as f:
            m = re.search(r'https://[a-zA-Z0-9-]+\.trycloudflare\.com', f.read())
            if m:
                public_url = m.group(0)
                break
if not public_url:
    log.error("Tunnel URL topilmadi! cloudflared.log:")
    with open("cloudflared.log") as f:
        log.error(f.read()[-500:])
    sys.exit(1)
log.info(f"Tunnel: {public_url}")

def keep_registering(url):
    """Orchestrator'ga doimiy registratsiya — har 60 soniyada.
    Orchestrator restart bo'lsa ham node URL'i qayta yuboriladi
    (register-node 'URL o'zgarmasa skip' qiladi, spam bo'lmaydi).
    Log: faqat birinchi va har 15-muvaffaqiyatda chiqadi."""
    ok_count = 0
    while True:
        try:
            _nk = "ndk-cojIjUhnqTghdOevVd2JPEOUW84QjzdBdCEcSBrP"
            headers = {"X-API-Key": _nk}
            r = requests.post(f"{ORCHESTRATOR_URL}/register-node",
                json={"node_type": NODE_TYPE, "url": url}, headers=headers, timeout=10)
            if r.status_code == 200:
                ok_count += 1
                if ok_count == 1 or ok_count % 15 == 0:
                    log.info(f"✅ Orchestrator bilan aloqa OK (№{ok_count})")
            else:
                ok_count = 0
                log.warning(f"Register: HTTP {r.status_code}")
        except Exception as e:
            ok_count = 0
            log.warning(f"Register: {e}")
        time.sleep(60)

threading.Thread(target=keep_registering, args=(public_url,), daemon=True).start()

HF_TOKEN = os.environ.get("HF_TOKEN", "")
if not HF_TOKEN:
    log.warning("HF_TOKEN o'rnatilmagan")
else:
    from huggingface_hub import login
    login(token=HF_TOKEN)
    log.info("HuggingFace login qilindi")
# ── NCCL moslik fix ──
# `undefined symbol: ncclCommResume` xatosi: torch'ning libtorch_cuda.so si yangi
# libnccl talab qiladi, lekin Kaggle base image'ning eski libnccl.so.2 birinchi
# yuklanadi. Yechim: venv ichidagi (torch/nvidia bilan kelgan) liblarni
# LD_LIBRARY_PATH'ga birinchi qo'yish — base image'ning eski libi shadow bo'lmaydi.
import glob as _glob
_nv_dirs = _glob.glob("/kaggle/working/venv/lib/python3.10/site-packages/nvidia/*/lib")
_torch_lib = "/kaggle/working/venv/lib/python3.10/site-packages/torch/lib"
_cand = [d for d in [_torch_lib] + _nv_dirs if os.path.isdir(d)]
if _cand:
    _old_ld = os.environ.get("LD_LIBRARY_PATH", "")
    os.environ["LD_LIBRARY_PATH"] = ":".join(_cand) + ((":" + _old_ld) if _old_ld else "")

import torch, traceback
from transformers import pipeline

# --- GPU aniqlash ---
ngpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
log.info(f"GPU soni: {ngpus}")
if ngpus == 0:
    log.warning("CUDA mavjud emas! Modellar ishlamaydi.")
dev_en = f"cuda:0" if ngpus >= 1 else "cpu"
dev_uz = f"cuda:{1 if ngpus >= 2 else 0}" if ngpus >= 1 else "cpu"
dt = torch.float16 if ngpus >= 1 else torch.float32
models_ok = []
en_model_error = None

log.info(f"[1/2] Canary-Qwen 2.5B EN STT ({dev_en})...")
try:
    from nemo.collections.speechlm2.models import SALM
    en_model = SALM.from_pretrained("nvidia/canary-qwen-2.5b")
    en_model = en_model.to(dev_en).eval()
    models_ok.append("canary-qwen-2.5b-en")
except Exception as e:
    en_model_error = traceback.format_exc()
    log.error(f"Canary-Qwen EN STT yuklanmadi:\n{en_model_error}")
    en_model = None

log.info("[2/2] Kotib/uzbek_stt_v1 UZ STT (CUDA:1)...")
try:
    uz_pipe = pipeline("automatic-speech-recognition", model="Kotib/uzbek_stt_v1",
                       torch_dtype=dt, device=dev_uz)
    models_ok.append("kotib-uz")
except Exception as e:
    log.error(f"UZ STT yuklanmadi: {e}")
    uz_pipe = None

if not models_ok:
    raise RuntimeError("Hech qaysi model yuklanmadi!")
log.info(f"Modellar: {', '.join(models_ok)}")

app = FastAPI(title="Node-2: EN+UZ STT")

class STTRequest(BaseModel):
    encrypted_audio: str

@app.get("/health")
async def health():
    try:
        gpus = []
        for i in range(torch.cuda.device_count()):
            p = torch.cuda.get_device_properties(i)
            gpus.append({"id": i, "name": p.name,
                "mem_total_gb": round(p.total_memory/1024**3,1),
                "mem_used_gb": round(torch.cuda.memory_allocated(i)/1024**3,2)})
        missing = []
        if not en_model: missing.append("canary-qwen-2.5b-en")
        if not uz_pipe: missing.append("kotib-uz")
        status = "degraded" if missing else "healthy"
        resp = {"status": status, "node": "kaggle2", "models": models_ok,
                 "missing": missing, "gpus": gpus}
        if en_model_error:
            resp["error_detail"] = en_model_error[-1000:]
        return resp
    except Exception as e:
        return {"status": "starting", "error": str(e)}

@app.post("/transcribe/en")
async def transcribe_en(req: STTRequest):
    if not en_model:
        raise HTTPException(503, "EN STT yuklanmagan")
    audio_bytes = decrypt_payload(req.encrypted_audio)
    # Audit fix: uuid nom + tozalash
    path = f"/tmp/en_{uuid.uuid4().hex[:10]}.wav"
    try:
        with open(path, "wb") as f:
            f.write(audio_bytes)
        answer_ids = en_model.generate(
            prompts=[[{"role": "user", "content": f"Transcribe the following: {en_model.audio_locator_tag}", "audio": [path]}]],
            max_new_tokens=128,
        )
        text = en_model.tokenizer.ids_to_text(answer_ids[0].cpu())
        return {"encrypted_text": encrypt_payload(text.encode('utf-8'))}
    except Exception as e:
        log.error(f"Canary-Qwen transcribe: {e}")
        raise HTTPException(500, str(e))
    finally:
        if os.path.exists(path):
            try: os.remove(path)
            except Exception: pass

@app.post("/transcribe/uz")
async def transcribe_uz(req: STTRequest):
    if not uz_pipe:
        raise HTTPException(503, "UZ STT yuklanmagan")
    audio_bytes = decrypt_payload(req.encrypted_audio)
    # Audit fix: uuid nom + tozalash
    path = f"/tmp/uz_{uuid.uuid4().hex[:10]}.wav"
    try:
        with open(path, "wb") as f:
            f.write(audio_bytes)
        result = uz_pipe(path)
        return {"encrypted_text": encrypt_payload(result["text"].encode('utf-8'))}
    finally:
        if os.path.exists(path):
            try: os.remove(path)
            except Exception: pass

@app.get("/logs")
async def get_logs():
    try:
        with open(log_path) as f:
            return {"logs": f.read()[-50000:]}
    except Exception as e:
        return {"logs": str(e)}

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
