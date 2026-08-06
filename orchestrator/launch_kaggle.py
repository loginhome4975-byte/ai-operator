"""
Yagona Kaggle node launch skripti.

Eski 3 ta fayl (launch_kaggle.py / launch_kaggle_1.py / launch_kaggle_2.py)
bitta faylga birlashtirildi. Node tanlash --node argumenti orqali amalga oshiriladi.

Ishlatish:
    python3 launch_kaggle.py            # Node-0: LLM + TTS (UZ)
    python3 launch_kaggle.py --node 1   # Node-1: STT (RU) + TTS (RU/EN)
    python3 launch_kaggle.py --node 2   # Node-2: STT (EN) + STT (UZ)
    python3 launch_kaggle.py --all      # Barcha 3 node
    python3 launch_kaggle.py -d         # Barcha akkauntdagi kernel'larni o'chirish
    python3 launch_kaggle.py -i         # GPU/TPU kvota ma'lumoti
    python3 launch_kaggle.py --node 0 --dry-run   # faqat fayllarni generatsiya qiladi

Har bir node alohida Kaggle akkauntiga tegishli (.env dan o'qiladi):
    Node-0: KAGGLE_USERNAME / KAGGLE_KEY          (default: bunyodbek7)
    Node-1: KAGGLE_USERNAME_1 / KAGGLE_KEY_1
    Node-2: KAGGLE_USERNAME_2 / KAGGLE_KEY_2

Node-1/2 push'dan oldin eski ~/.kaggle/kaggle.json ni o'chiradi (akkaunt
almashinuvi). Node-0 ham env kredensiallari berilgan bo'lsa xuddi shunday qiladi.
"""
import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys

# =====================================================================
# NODE KONFIGURATSIYASI — har bir node uchun faqat farqli qismlar
# =====================================================================
NODE_CONFIGS = {
    0: {
        "label": "Node-0",
        "node_type": "kaggle",
        "node_port": 5001,
        "node_dir": "kaggle_node",
        "dataset_suffix": "ai-operator-node0-venv",
        "kernel_suffix": "ai-operator-kaggle-node",
        "env_prefix": "NODE0",
        "username_env": "KAGGLE_USERNAME",
        "key_env": "KAGGLE_KEY",
        "default_user": "bunyodbek7",
        "requires_env": False,
        "logger": "node0",
        "use_log_path": True,
        "hf_conditional": False,
        "extra_imports": [
            "import soundfile as sf",
        ],
        "apt_packages": "tzdata python3.10 python3.10-venv python3.10-dev build-essential wget sox libsox-fmt-all",
        "pip_commands": [
            "/kaggle/working/venv/bin/pip install --upgrade pip",
            "/kaggle/working/venv/bin/pip install fastapi uvicorn nest-asyncio cryptography requests soundfile huggingface_hub",
            "/kaggle/working/venv/bin/pip install -U qwen-tts",
            "/kaggle/working/venv/bin/pip install llama-cpp-python --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cu121 --no-cache-dir",
        ],
        "extra_dataset_sources": ["bunyodbek7/miyya-qwen-7b"],
    },
    1: {
        "label": "Node-1",
        "node_type": "kaggle1",
        "node_port": 5003,
        "node_dir": "kaggle_node_1",
        "dataset_suffix": "ai-operator-node1-venv",
        "kernel_suffix": "ai-operator-kaggle-node-1",
        "env_prefix": "NODE1",
        "username_env": "KAGGLE_USERNAME_1",
        "key_env": "KAGGLE_KEY_1",
        "default_user": None,
        "requires_env": True,
        "logger": "node1",
        "use_log_path": False,
        "hf_conditional": True,
        "extra_imports": [
            "import io, wave",
            "import soundfile as sf",
        ],
        "apt_packages": "tzdata python3.10 python3.10-venv python3.10-dev build-essential",
        "pip_commands": [
            "/kaggle/working/venv/bin/pip install --upgrade pip",
            "/kaggle/working/venv/bin/pip install fastapi uvicorn pydantic python-multipart transformers torch torchaudio librosa soundfile accelerate nest-asyncio requests cryptography",
            "/kaggle/working/venv/bin/pip install chatterbox-tts torchaudio",
        ],
        "extra_dataset_sources": [],
    },
    2: {
        "label": "Node-2",
        "node_type": "kaggle2",
        "node_port": 5002,
        "node_dir": "kaggle_node_2",
        "dataset_suffix": "ai-operator-node2-venv",
        "kernel_suffix": "ai-operator-kaggle-node-2",
        "env_prefix": "NODE2",
        "username_env": "KAGGLE_USERNAME_2",
        "key_env": "KAGGLE_KEY_2",
        "default_user": None,
        "requires_env": True,
        "logger": "node2",
        "use_log_path": True,
        "hf_conditional": True,
        "extra_imports": [],
        "apt_packages": "tzdata python3.10 python3.10-venv python3.10-dev build-essential",
        "pip_commands": [
            "/kaggle/working/venv/bin/pip install --upgrade pip",
            "/kaggle/working/venv/bin/pip install fastapi uvicorn pydantic python-multipart transformers torch torchaudio librosa soundfile accelerate nest-asyncio requests cryptography",
            "/kaggle/working/venv/bin/pip install 'nemo_toolkit[asr] @ git+https://github.com/NVIDIA/NeMo.git'",
        ],
        "extra_dataset_sources": [],
    },
}

# =====================================================================
# .env YUKLASH
# =====================================================================
def _load_env():
    """Proyekt ildizidagi .env faylini os.environ'ga yuklaydi."""
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
    )
    if not os.path.exists(env_path):
        return
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val
            if key == "KAGGLE_KEY" and val.startswith("KGAT_"):
                os.environ["KAGGLE_API_TOKEN"] = val


def _resolve_account(node):
    """Har bir node o'z Kaggle akkauntidan foydalanadi.

    Node-0: KAGGLE_USERNAME / KAGGLE_KEY (default user bor)
    Node-1: KAGGLE_USERNAME_1 / KAGGLE_KEY_1 (majburiy)
    Node-2: KAGGLE_USERNAME_2 / KAGGLE_KEY_2 (majburiy)

    Qaytaradi: (kaggle_user, kaggle_api_token)
    """
    cfg = NODE_CONFIGS[node]
    if node == 0:
        user = os.environ.get("KAGGLE_USERNAME", cfg["default_user"])
        key = os.environ.get("KAGGLE_KEY", "")
    else:
        user = os.environ.get(cfg["username_env"])
        if not user:
            print(f"XATO: .env faylida {cfg['username_env']} topilmadi!")
            sys.exit(1)
        key = os.environ.get(cfg["key_env"], "")
        os.environ["KAGGLE_USERNAME"] = user
        os.environ["KAGGLE_KEY"] = key

    # Token faqat shu node'ning kalitidan keladi
    if key.startswith("KGAT_"):
        os.environ["KAGGLE_API_TOKEN"] = key
    else:
        os.environ["KAGGLE_API_TOKEN"] = ""
    token = key
    return user, token


# =====================================================================
# SERVER KODI (main_app.py)
# =====================================================================
def _build_server_head(cfg):
    """Umumiy server kodi: importlar, logging, xavfsizlik, tunnel, upload, register."""
    aes_256_key = os.environ.get("AES_256_KEY", "")
    hf_token = os.environ.get("HF_TOKEN", "")
    orch_url = os.environ.get("ORCHESTRATOR_URL", "https://orchestrator.traffix.uz")
    node_comm_key = os.environ.get("NODE_COMM_KEY") or os.environ.get("ORCHESTRATOR_API_KEY", "")

    logger_name = cfg["logger"]
    node_type = cfg["node_type"]
    node_port = cfg["node_port"]
    extra_imports = "\n".join(cfg["extra_imports"])

    if cfg["use_log_path"]:
        log_block = f'''log_path = "/kaggle/working/app.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)]
)
log = logging.getLogger("{logger_name}")
'''
    else:
        log_block = f'''logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("{logger_name}")
'''

    if cfg["hf_conditional"]:
        hf_block = f'''HF_TOKEN = "{hf_token}"
if not HF_TOKEN:
    log.warning("HF_TOKEN o'rnatilmagan")
else:
    from huggingface_hub import login
    login(token=HF_TOKEN)
    log.info("HuggingFace login qilindi")
'''
    else:
        hf_block = f'''log.info("Modellar yuklanmoqda...")

from huggingface_hub import login
login(token="{hf_token}")
'''

    security_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "security_utils.py")
    with open(security_path, "r") as f:
        security_code = f.read()

    return f'''import os, sys, json, time, re, base64, threading, logging
import requests
import nest_asyncio
import uvicorn
{extra_imports}
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

{log_block}
os.environ["AES_256_KEY"] = "{aes_256_key}"
{security_code}

ORCHESTRATOR_URL = "{orch_url}"
NODE_TYPE = "{node_type}"
NODE_PORT = {node_port}

log.info("Cloudflare tunnel ochilmoqda...")
os.system("wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared && chmod +x cloudflared")
os.system(f"./cloudflared tunnel --url http://127.0.0.1:{{NODE_PORT}} > cloudflared.log 2>&1 &")

public_url = None
for _ in range(30):
    time.sleep(1)
    if os.path.exists("cloudflared.log"):
        with open("cloudflared.log") as f:
            m = re.search(r'https://[a-zA-Z0-9-]+\\.trycloudflare\\.com', f.read())
            if m:
                public_url = m.group(0)
                break
if not public_url:
    log.error("Cloudflared URL topilmadi!")
    os.system("cat cloudflared.log")
    sys.exit(1)
log.info(f"Tunnel URL: {{public_url}}")

UPLOAD_FLAG = "/kaggle/working/.venv_upload_info"

def _upload_venv_if_needed():
    if not os.path.exists(UPLOAD_FLAG):
        return
    try:
        with open(UPLOAD_FLAG) as f:
            info = json.load(f)
        tar_path = info.get("tar_path", "")
        if not os.path.exists(tar_path):
            log.warning(f"Venv arxiv topilmadi: {{tar_path}}")
            os.remove(UPLOAD_FLAG)
            return
        size_gb = os.path.getsize(tar_path) / 1024**3
        log.info(f"📦 Venv arxiv yuborilmoqda ({{size_gb:.1f}} GB, 45MB chunk'lab)...")

        import hashlib as _hl
        _sha = _hl.sha256()
        with open(tar_path, "rb") as tf:
            while True:
                buf = tf.read(8*1024*1024)
                if not buf: break
                _sha.update(buf)
        _file_hash = _sha.hexdigest()

        _uid = _hl.sha256(f"{{info['dataset_slug']}}-{{time.time()}}".encode()).hexdigest()[:12]
        CHUNK = 45 * 1024 * 1024
        total_size = os.path.getsize(tar_path)
        total_chunks = (total_size + CHUNK - 1) // CHUNK
        nk = "{node_comm_key}"
        base_data = {{k: str(info[k]) for k in ["dataset_slug", "dataset_name", "venv_hash", "kaggle_user", "kaggle_key", "deltadata", "node_type"] if k in info}}
        log.info(f"  Jami {{total_chunks}} chunk, upload_id={{_uid}}")

        ok = True
        with open(tar_path, "rb") as tf:
            for ci in range(total_chunks):
                chunk_data = tf.read(CHUNK)
                fd = {{**base_data, "chunk_index": str(ci), "total_chunks": str(total_chunks),
                      "upload_id": _uid, "file_sha256": _file_hash}}
                files = {{"file": (f"chunk_{{ci:04d}}", chunk_data, "application/octet-stream")}}
                r = requests.post(f"{{ORCHESTRATOR_URL}}/upload-venv", files=files, data=fd,
                                headers={{"X-API-Key": nk}}, timeout=300)
                if r.status_code != 200:
                    log.warning(f"  Chunk {{ci+1}}/{{total_chunks}} xatosi: {{r.status_code}}")
                    ok = False
                    break
                resp = r.json() if r.text else {{}}
                st = resp.get("status", "")
                if st in ("success", "uploaded_unverified"):
                    os.remove(UPLOAD_FLAG)
                    log.info(f"✅ Venv datasetga yuklandi! ({{size_gb:.1f}} GB, {{resp.get('size_gb','?')}} GB)")
                    ok = True
                    break
                elif ci % 10 == 0:
                    log.info(f"  Chunk {{ci+1}}/{{total_chunks}} ({{resp.get('received','?')}}/{{total_chunks}})")
        if not ok:
            log.warning(f"Venv upload muvaffaqiyatsiz, qayta uriniladi...")
    except Exception as e:
        log.warning(f"Venv upload xatosi: {{e}}")

def keep_registering(url):
    attempt = 0
    while True:
        try:
            _nk = "{node_comm_key}"
            headers = {{"X-API-Key": _nk}}
            r = requests.post(f"{{ORCHESTRATOR_URL}}/register-node",
                json={{"node_type": NODE_TYPE, "url": url}}, headers=headers, timeout=10)
            log.info(f"Register #{{attempt+1}}: {{r.status_code}} {{r.text[:80]}}")
            if r.status_code == 200:
                # Bug #6 fix: upload'ni alohida thread'da ishga tushiramiz —
                # keep_registering bloklanmasin va registratsiya davom etsin.
                threading.Thread(target=_upload_venv_if_needed, daemon=True).start()
        except Exception as e:
            log.warning(f"Register xatosi: {{e}}")
        attempt += 1
        time.sleep(2 if attempt < 5 else 60)

threading.Thread(target=keep_registering, args=(public_url,), daemon=True).start()

{hf_block}'''


def _build_server_body(cfg):
    """Node'ga xos qism: model yuklash + endpointlar."""
    if cfg["node_type"] == "kaggle":
        return _node0_body()
    elif cfg["node_type"] == "kaggle1":
        return _node1_body()
    else:
        return _node2_body()


def _node0_body():
    """Node-0: Miyya Qwen 7B (LLM) + Sayro TTS 1.7B (UZ)."""
    return '''import torch

# --- GPU aniqlash ---
ngpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
log.info(f"GPU soni: {ngpus}")
if ngpus == 0:
    log.warning("CUDA mavjud emas! Modellar ishlamaydi.")
dev_tts = f"cuda:{1 if ngpus >= 2 else 0}" if ngpus >= 1 else "cpu"
dev_llm = "cuda:0" if ngpus >= 1 else "cpu"

log.info(f"[1/2] Sayro TTS ({dev_tts})...")
from qwen_tts import Qwen3TTSModel
tts = Qwen3TTSModel.from_pretrained("uzlm/sayro-tts-1.7B", device_map=dev_tts, dtype=torch.float16 if ngpus >= 1 else torch.float32)
_speakers = tts.get_supported_speakers() if hasattr(tts, "get_supported_speakers") else []
_speaker = _speakers[0] if _speakers else "default"
_langs = tts.get_supported_languages() if hasattr(tts, "get_supported_languages") else []
_lang = "uz" if "uz" in _langs else (_langs[0] if _langs else "uz")

log.info(f"[2/2] Miyya LLM GGUF ({dev_llm})...")
import glob
from llama_cpp import Llama
gguf_files = glob.glob("/kaggle/input/**/*.gguf", recursive=True)
if not gguf_files:
    raise FileNotFoundError("GGUF model topilmadi!")
llm = Llama(model_path=gguf_files[0], n_gpu_layers=-1, n_ctx=4096, chat_format="chatml", verbose=False)

log.info("Barcha modellar tayyor!")

app = FastAPI(title="Node-0: LLM + UZ TTS")

class EncryptedRequest(BaseModel):
    encrypted_payload: str

@app.get("/health")
async def health():
    try:
        gpus = []
        for i in range(torch.cuda.device_count()):
            p = torch.cuda.get_device_properties(i)
            gpus.append({"id": i, "name": p.name,
                "mem_total_gb": round(p.total_memory / 1024**3, 1),
                "mem_used_gb": round(torch.cuda.memory_allocated(i) / 1024**3, 2)})
        return {"status": "healthy", "node": "kaggle0", "models": ["miyya-qwen-7b", "sayro-tts-1.7b"], "gpus": gpus}
    except Exception as e:
        return {"status": "starting", "error": str(e)}

@app.get("/logs")
async def get_logs():
    try:
        with open(log_path) as f:
            return {"logs": f.read()[-50000:]}
    except Exception as e:
        return {"logs": str(e)}

@app.post("/chat")
async def chat(req: EncryptedRequest):
    data = json.loads(decrypt_payload(req.encrypted_payload).decode('utf-8'))
    resp = llm.create_chat_completion(messages=data.get("messages", []), max_tokens=512)
    text = resp["choices"][0]["message"]["content"]
    return {"encrypted_payload": encrypt_payload(json.dumps({"response": text}).encode('utf-8'))}

@app.post("/synthesize")
async def synthesize(req: EncryptedRequest):
    data = json.loads(decrypt_payload(req.encrypted_payload).decode('utf-8'))
    text = (data.get("text") or "").strip()
    if hasattr(tts, "generate_custom_voice"):
        audio_data, sr = tts.generate_custom_voice(text=text, language=_lang, speaker=_speaker)
    elif hasattr(tts, "generate_voice_clone"):
        audio_data, sr = tts.generate_voice_clone(text=text)
    else:
        raise HTTPException(500, f"TTS method topilmadi: {dir(tts)}")
    audio = audio_data[0] if isinstance(audio_data, list) else audio_data
    path = f"/tmp/tts_{time.time()}.wav"
    sf.write(path, audio, samplerate=24000)
    return FileResponse(path, media_type="audio/wav")

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
'''


def _node1_body():
    """Node-1: Whisper large-v3 (RU STT) + Chatterbox (RU/EN TTS)."""
    return '''import torch
from transformers import pipeline, AutoModelForSpeechSeq2Seq, AutoProcessor

# --- GPU aniqlash ---
ngpus = torch.cuda.device_count() if torch.cuda.is_available() else 0
log.info(f"GPU soni: {ngpus}")
if ngpus == 0:
    log.warning("CUDA mavjud emas! Modellar ishlamaydi.")
dev_stt = f"cuda:0" if ngpus >= 1 else "cpu"
dev_tts = f"cuda:{1 if ngpus >= 2 else 0}" if ngpus >= 1 else "cpu"
dt = torch.float16 if ngpus >= 1 else torch.float32

log.info(f"[1/2] Whisper large-v3 RU STT ({dev_stt})...")
stt_model = AutoModelForSpeechSeq2Seq.from_pretrained(
    "openai/whisper-large-v3", torch_dtype=dt, low_cpu_mem_usage=True, use_safetensors=True
).to(dev_stt)
stt_processor = AutoProcessor.from_pretrained("openai/whisper-large-v3")
stt_pipe = pipeline("automatic-speech-recognition", model=stt_model,
    tokenizer=stt_processor.tokenizer, feature_extractor=stt_processor.feature_extractor,
    torch_dtype=dt, device=dev_stt)

log.info(f"[2/2] Chatterbox Multilingual TTS RU/EN ({dev_tts})...")
try:
    import torchaudio as ta
    from chatterbox.mtl_tts import ChatterboxMultilingualTTS
    tts_pipe = ChatterboxMultilingualTTS.from_pretrained(device=dev_tts, t3_model="v3")
    _speaker = None
except Exception as e:
    log.warning(f"Chatterbox TTS yuklanmadi: {e}")
    tts_pipe = None

log.info("Modellar tayyor!")

app = FastAPI(title="Node-1: RU STT + RU/EN TTS")

class STTRequest(BaseModel):
    encrypted_audio: str

class TTSRequest(BaseModel):
    encrypted_text: str

@app.get("/health")
async def health():
    try:
        gpus = []
        for i in range(torch.cuda.device_count()):
            p = torch.cuda.get_device_properties(i)
            gpus.append({"id": i, "name": p.name,
                "mem_total_gb": round(p.total_memory/1024**3,1),
                "mem_used_gb": round(torch.cuda.memory_allocated(i)/1024**3,2)})
        return {"status": "healthy", "node": "kaggle1",
                 "models": ["whisper-large-v3", "chatterbox-multilingual" if tts_pipe else "chatterbox-failed"],
                 "gpus": gpus}
    except Exception as e:
        return {"status": "starting", "error": str(e)}

@app.post("/transcribe/ru")
async def transcribe_ru(req: STTRequest):
    audio_bytes = decrypt_payload(req.encrypted_audio)
    path = f"/tmp/ru_{int(time.time())}.wav"
    with open(path, "wb") as f:
        f.write(audio_bytes)
    result = stt_pipe(path, generate_kwargs={"language": "russian"})
    return {"encrypted_text": encrypt_payload(result["text"].encode('utf-8'))}

@app.post("/synthesize/ru")
async def synthesize_ru(req: TTSRequest):
    return await _synthesize(req, "ru")

@app.post("/synthesize/en")
async def synthesize_en(req: TTSRequest):
    return await _synthesize(req, "en")

async def _synthesize(req: TTSRequest, lang: str):
    if not tts_pipe:
        raise HTTPException(500, "TTS yuklanmagan")
    text = decrypt_payload(req.encrypted_text).decode('utf-8')
    try:
        lang_id = "ru" if lang == "ru" else "en"
        wav = tts_pipe.generate(text, language_id=lang_id)
        path = f"/tmp/tts_{lang}_{int(time.time())}.wav"
        ta.save(path, wav, tts_pipe.sr)
        return FileResponse(path, media_type="audio/wav")
    except Exception as e:
        log.error(f"TTS {lang}: {e}")
        raise HTTPException(500, str(e))

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
'''


def _node2_body():
    """Node-2: Canary-Qwen 2.5B (EN STT) + Kotib (UZ STT)."""
    return '''import torch, traceback
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
    log.error(f"Canary-Qwen EN STT yuklanmadi:\\n{en_model_error}")
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
    path = f"/tmp/en_{int(time.time())}.wav"
    with open(path, "wb") as f:
        f.write(audio_bytes)
    try:
        answer_ids = en_model.generate(
            prompts=[[{"role": "user", "content": f"Transcribe the following: {en_model.audio_locator_tag}", "audio": [path]}]],
            max_new_tokens=128,
        )
        text = en_model.tokenizer.ids_to_text(answer_ids[0].cpu())
        return {"encrypted_text": encrypt_payload(text.encode('utf-8'))}
    except Exception as e:
        log.error(f"Canary-Qwen transcribe: {e}")
        raise HTTPException(500, str(e))

@app.post("/transcribe/uz")
async def transcribe_uz(req: STTRequest):
    if not uz_pipe:
        raise HTTPException(503, "UZ STT yuklanmagan")
    audio_bytes = decrypt_payload(req.encrypted_audio)
    path = f"/tmp/uz_{int(time.time())}.wav"
    with open(path, "wb") as f:
        f.write(audio_bytes)
    result = uz_pipe(path)
    return {"encrypted_text": encrypt_payload(result["text"].encode('utf-8'))}

@app.get("/logs")
async def get_logs():
    try:
        with open(log_path) as f:
            return {"logs": f.read()[-50000:]}
    except Exception as e:
        return {"logs": str(e)}

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
'''


def _build_main_app_code(cfg):
    """To'liq main_app.py kodini yig'adi."""
    return _build_server_head(cfg) + _build_server_body(cfg)


# =====================================================================
# RUNNER KODI (run_server.py)
# =====================================================================
def _build_runner_code(cfg, encoded_main, code_hash, deltadata, kaggle_user, kaggle_api_token):
    """Kaggle'da ishlaydigan bootstrap skripti."""
    node_label = cfg["label"]
    apt_packages = cfg["apt_packages"]
    dataset_slug = f"{kaggle_user}/{cfg['dataset_suffix']}"
    dataset_name = cfg["dataset_suffix"]
    node_type = cfg["node_type"]
    deltadata_str = "true" if deltadata else "false"
    pip_commands_str = ",\n        ".join(f'"""{cmd}"""' for cmd in cfg["pip_commands"])

    return f'''import os, sys, subprocess, hashlib, tarfile, json, base64, shutil, time

print("{node_label} initialization starting...")

encoded_main = "{encoded_main}"
with open("/kaggle/working/main_app.py", "w") as f:
    f.write(base64.b64decode(encoded_main).decode('utf-8'))

VENV_HASH = "{code_hash}"
DATASET_NAME = "{dataset_name}"
DATASET_SLUG = "{dataset_slug}"
DELTADATA = "{deltadata_str}"
DATASET_PATH = f"/kaggle/input/{{DATASET_NAME}}"

use_cache = False

if not DELTADATA == "true" and os.path.isdir(DATASET_PATH):
    hash_file = os.path.join(DATASET_PATH, "venv_hash.txt")
    tar_file = os.path.join(DATASET_PATH, "venv.tar.gz")
    if os.path.exists(hash_file) and os.path.exists(tar_file):
        with open(hash_file) as f:
            cached_hash = f.read().strip()
        if cached_hash == VENV_HASH:
            print("Cached venv topildi! Ko'chirilmoqda...")
            subprocess.run(f"tar -xzf {{tar_file}} -C /kaggle/working/", shell=True, check=True)
            use_cache = True
        else:
            print("Hash mismatch. Venv qayta quriladi.")
    else:
        print("Dataset mavjud lekin hash/tar topilmadi.")

if not use_cache:
    print("Python 3.10 + venv qurilmoqda...")
    os.system("DEBIAN_FRONTEND=noninteractive apt-get update -y")
    os.system(f"DEBIAN_FRONTEND=noninteractive apt-get install {apt_packages} -y")
    os.system("python3.10 -m venv /kaggle/working/venv")

    print("[1/2] Kutubxonalar venv ichiga o'rnatilmoqda...")
    pip_cmds = [
        {pip_commands_str}
    ]
    for cmd in pip_cmds:
        print(f"  $ {{cmd}}")
        ret = os.system(cmd)
        if ret != 0:
            print(f"  Pip buyrug'i xato (exit={{ret}})")

    # ======== ARXIVLASH (tunnel orqali orchestrator yuklaydi) ========
    print("[2/2] Venv arxivlanmoqda (orchestrator orqali dataset yuklanadi)...")

    ds_dir = "/kaggle/working/_dataset"
    os.makedirs(ds_dir, exist_ok=True)

    tar_path = f"{{ds_dir}}/venv.tar.gz"
    print("  Venv arxivlanmoqda...")
    subprocess.run(f"tar -czf {{tar_path}} -C /kaggle/working venv", shell=True, check=True)
    size_gb = os.path.getsize(tar_path) / 1024**3
    print(f"  Venv hajmi: {{size_gb:.1f}} GB")

    upload_info = {{
        "tar_path": tar_path,
        "dataset_slug": DATASET_SLUG,
        "dataset_name": DATASET_NAME,
        "venv_hash": VENV_HASH,
        "kaggle_user": "{kaggle_user}",
        "kaggle_key": "{kaggle_api_token}",
        "deltadata": DELTADATA,
        "node_type": "{node_type}",
    }}
    with open("/kaggle/working/.venv_upload_info", "w") as f:
        json.dump(upload_info, f)
    print("  Venv arxiv tayyor — tunnel ochilgach orchestrator'ga yuboriladi")
else:
    print("[1/2] Cached venv ishlatildi (pip o'tkazib yuborildi)")

print("[2/2] Server ishga tushirilmoqda...")
os.system(f"/kaggle/working/venv/bin/python /kaggle/working/main_app.py")
'''


# =====================================================================
# KVOTA MA'LUMOTI (-i)
# =====================================================================
def _show_quota():
    """Barcha 3 akkaunt uchun GPU/TPU kvotalarini ko'rsatadi."""
    print(f"\n{'='*70}")
    print(f"  📊 KAGGLE GPU / TPU KVOTA MA'LUMOTI")
    print(f"{'='*70}")

    totals = {"gpu_used": 0.0, "gpu_remaining": 0.0, "gpu_total": 0.0,
              "tpu_used": 0.0, "tpu_remaining": 0.0, "tpu_total": 0.0}

    for node in [0, 1, 2]:
        cfg = NODE_CONFIGS[node]
        user, token = _resolve_account(node)

        # kaggle.json tozalash (env orqali auth)
        kaggle_dir = os.path.expanduser("~/.kaggle")
        json_path = os.path.join(kaggle_dir, "kaggle.json")
        if os.path.exists(json_path):
            try:
                os.remove(json_path)
            except Exception:
                pass

        print(f"\n  {cfg['label']}: {user}")
        print(f"  {'-'*50}")

        try:
            result = subprocess.run(
                ["kaggle", "quota"],
                capture_output=True, text=True, timeout=15
            )
            if result.returncode != 0:
                print(f"     ⚠️  Kvota olinmadi: {result.stderr[:100]}")
                continue
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"     ⚠️  Xatolik: {e}")
            continue

        # kaggle quota jadvalini parse qilish:
        # resource  used    remaining  total   refreshAt
        # --------  ------  ---------  ------  -------------------
        # GPU       16.05h  13.95h     30.00h  2026-08-08T00:00:00
        # TPU       0.00h   20.00h     20.00h  2026-08-08T00:00:00
        lines = result.stdout.strip().split("\n")
        gpu_info = None
        tpu_info = None
        refresh_at = "?"

        for line in lines[2:]:  # header'larni o'tkazib yuborish
            parts = line.split()
            if len(parts) < 5:
                continue
            resource = parts[0]
            if resource == "GPU":
                gpu_info = {"used": parts[1], "remaining": parts[2],
                           "total": parts[3], "refresh": parts[4] if len(parts) > 4 else "?"}
                refresh_at = gpu_info["refresh"]
            elif resource == "TPU":
                tpu_info = {"used": parts[1], "remaining": parts[2],
                           "total": parts[3], "refresh": parts[4] if len(parts) > 4 else "?"}

        def _parse_hours(val: str) -> float:
            """'16.05h' yoki '30.00h' -> float."""
            if val.endswith("h"):
                val = val[:-1]
            try:
                return float(val)
            except ValueError:
                return 0.0

        if gpu_info:
            used_h = _parse_hours(gpu_info["used"])
            remain_h = _parse_hours(gpu_info["remaining"])
            total_h = _parse_hours(gpu_info["total"])
            pct = (used_h / total_h * 100) if total_h > 0 else 0
            bar_len = 20
            filled = int(bar_len * pct / 100)
            bar = "█" * filled + "░" * (bar_len - filled)

            print(f"     GPU:   {bar}  {pct:.0f}%")
            print(f"            Used: {used_h:.1f}h  |  Qolgan: {remain_h:.1f}h  |  Jami: {total_h:.0f}h/hafta")
            # Kunlik/soatlik taxminiy bo'linma
            days_left = 7 - (pct / 100 * 7)
            print(f"            ~kuniga {remain_h / max(days_left, 0.1):.1f}h, ~soatiga {remain_h / max(days_left * 24, 0.1):.2f}h")

            totals["gpu_used"] += used_h
            totals["gpu_remaining"] += remain_h
            totals["gpu_total"] += total_h
        else:
            print(f"     GPU:   ma'lumot yo'q")

        if tpu_info:
            used_h = _parse_hours(tpu_info["used"])
            remain_h = _parse_hours(tpu_info["remaining"])
            total_h = _parse_hours(tpu_info["total"])
            print(f"     TPU:   Used: {used_h:.1f}h  |  Qolgan: {remain_h:.1f}h  |  Jami: {total_h:.0f}h/hafta")

            totals["tpu_used"] += used_h
            totals["tpu_remaining"] += remain_h
            totals["tpu_total"] += total_h

        if refresh_at and refresh_at != "?":
            print(f"     🔄  Yangilanish: {refresh_at}")

    # Jami xulosa
    print(f"\n  {'='*50}")
    print(f"  📋 JAMI (3 akkaunt):")
    print(f"     GPU: {totals['gpu_used']:.1f}h ishlatilgan, {totals['gpu_remaining']:.1f}h qolgan ({totals['gpu_total']:.0f}h jami)")
    print(f"     TPU: {totals['tpu_used']:.1f}h ishlatilgan, {totals['tpu_remaining']:.1f}h qolgan ({totals['tpu_total']:.0f}h jami)")
    print()


# =====================================================================
# NODE MONITORING (-m)
# =====================================================================
def _monitor_nodes():
    """Barcha node'lar holatini ko'rsatadi: GPU, model, status.
    Orchestrator /api/nodes/status orqali yoki Kaggle CLI orqali."""
    import urllib.request
    import urllib.error
    
    orch_url = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000")
    api_key = os.environ.get("ORCHESTRATOR_API_KEY", "")
    
    print(f"\n{'='*70}")
    print(f"  📡 NODE MONITORING")
    print(f"{'='*70}")
    
    # URL'lar ro'yxati — avval localhost, keyin env URL
    urls_to_try = ["http://localhost:8000"]
    if orch_url and orch_url != "http://localhost:8000":
        urls_to_try.append(orch_url)
    
    # 1. Orchestrator orqali urinib ko'rish
    if api_key:
        for url in urls_to_try:
            try:
                full_url = f"{url}/api/nodes/status"
                req = urllib.request.Request(
                    full_url,
                    headers={"X-API-Key": api_key}
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    data = json.loads(resp.read().decode())
                    nodes_data = data.get("nodes", {})
                    
                    if nodes_data:
                        print(f"  ✅ Manba: {url}")
                        _display_nodes_from_orchestrator(nodes_data)
                        return
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print(f"  ⚠️  {url} — endpoint topilmadi (orchestrator'ni restart qiling)")
                else:
                    print(f"  ⚠️  {url} — HTTP {e.code}")
            except Exception as e:
                print(f"  ⚠️  {url} — ulanmadi: {e}")
        print(f"  Kaggle CLI orqali tekshirilmoqda...\n")
    
    # 2. Fallback: Kaggle CLI
    _monitor_via_kaggle_cli()


def _display_nodes_from_orchestrator(nodes_data):
    """Orchestrator'dan olingan node ma'lumotlarini chiroyli ko'rsatish."""
    for ntype in ["kaggle", "kaggle1", "kaggle2"]:
        info = nodes_data.get(ntype, {})
        if not info:
            continue
        
        node = info.get("node", "?")
        label = info.get("label", f"Node-{node}")
        status = info.get("status", "?")
        models = info.get("models", [])
        missing = info.get("missing", [])
        gpus = info.get("gpus", [])
        url = info.get("url", "")
        error = info.get("error", "")
        venv = info.get("venv", {})
        
        # Status emoji
        if status == "healthy":
            st_icon = "🟢"
        elif status == "degraded":
            st_icon = "🟡"
        elif status == "starting":
            st_icon = "🔵"
        else:
            st_icon = "🔴"
        
        print(f"\n  {st_icon} Node-{node}: {label}")
        print(f"     Status: {status}")
        
        if gpus:
            for gpu in gpus:
                gid = gpu.get("id", "?")
                gname = gpu.get("name", "?")
                mem_total = gpu.get("mem_total_gb", 0)
                mem_used = gpu.get("mem_used_gb", 0)
                print(f"     GPU #{gid}: {gname} | {mem_used:.1f}/{mem_total:.1f} GB")
        else:
            print(f"     GPU: ma'lumot yo'q")
        
        if models:
            print(f"     Modellar: {', '.join(models)}")
        if missing:
            print(f"     ❌ Yuklanmagan: {', '.join(missing)}")
        if error:
            print(f"     ⚠️  {error[:120]}")
        
        # Venv status
        if venv:
            _print_venv_status(venv)
    
    print()


def _print_venv_status(venv):
    """Venv holatini ko'rsatish."""
    vstatus = venv.get("status", "idle")
    vhash = venv.get("hash", "")
    vsize = venv.get("size_gb", 0)
    vchunks = venv.get("chunks", "")
    verror = venv.get("error", "")
    
    status_map = {
        "idle": ("⚪", "Venv: tayyor (o'zgarish yo'q)"),
        "receiving": ("📥", f"Venv: chunk'lab yuborilmoqda ({vchunks})"),
        "assembling": ("🔧", "Venv: yig'ilmoqda..."),
        "uploading_kaggle": ("📤", f"Venv: Kaggle'ga yuklanmoqda ({vsize:.1f} GB, hash={vhash})"),
        "verifying": ("🔍", f"Venv: tekshirilmoqda (5 ta urinish)..."),
        "verified": ("✅", f"Venv: yuklandi! (hash={vhash})"),
        "error": ("❌", f"Venv: xato — {verror[:80]}"),
    }
    
    icon, msg = status_map.get(vstatus, ("❓", f"Venv: {vstatus}"))
    print(f"     {icon} {msg}")


def _monitor_via_kaggle_cli():
    """Kaggle CLI orqali node holatini tekshirish."""
    for node in [0, 1, 2]:
        cfg = NODE_CONFIGS[node]
        user, token = _resolve_account(node)
        
        # kaggle.json tozalash
        kaggle_dir = os.path.expanduser("~/.kaggle")
        json_path = os.path.join(kaggle_dir, "kaggle.json")
        if os.path.exists(json_path):
            try:
                os.remove(json_path)
            except Exception:
                pass
        
        kernel_id = f"{user}/{cfg['kernel_suffix']}"
        print(f"\n  Node-{node}: {kernel_id}")
        
        try:
            result = subprocess.run(
                ["kaggle", "kernels", "status", kernel_id],
                capture_output=True, text=True, timeout=15
            )
            if result.returncode == 0 and "RUNNING" in result.stdout:
                print(f"     Status: 🟢 RUNNING")
                # Log'lardan GPU/model ajratib olishga harakat qilamiz
                _parse_logs_for_info(kernel_id, cfg["logger"])
            elif result.returncode == 0:
                print(f"     Status: ⚪ {result.stdout.strip().split('has status')[1].strip() if 'has status' in result.stdout else 'completed'}")
            else:
                print(f"     Status: ⚫ kernel topilmadi")
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"     ⚠️  Xatolik: {e}")
    print()


def _parse_logs_for_info(kernel_id: str, logger_name: str):
    """Kernel log'lardan GPU soni va model nomlarini ajratib olish."""
    try:
        result = subprocess.run(
            ["kaggle", "kernels", "logs", kernel_id],
            capture_output=True, text=True, timeout=15
        )
        lines = result.stdout.split("\n")
        
        gpu_count = None
        models = []
        
        for line in lines:
            # GPU soni: X
            if "GPU soni:" in line:
                try:
                    gpu_count = int(line.split("GPU soni:")[1].strip())
                except ValueError:
                    pass
            
            # Model nomlari: "Modellar tayyor!" yoki "Modellar: xxx"
            if "Modellar:" in line and "tayyor" not in line:
                models_str = line.split("Modellar:")[1].strip()
                models = [m.strip() for m in models_str.split(",")]
            elif "Modellar tayyor" in line:
                # Node-0 da "Modellar tayyor!" — oldingi qatorlardan model nomini topish
                pass
            
            # Node-0 specific: "[1/2] Sayro TTS" yoki "[2/2] Miyya LLM"
            if "[1/2]" in line or "[2/2]" in line:
                # Model yuklanayotgan qator — nomini ajratib olish
                parts = line.split("]")
                if len(parts) > 1:
                    model_desc = parts[1].split("(")[0].strip() if "(" in parts[1] else parts[1].strip()
                    if model_desc and len(model_desc) > 3:
                        models.append(model_desc)
        
        if gpu_count is not None:
            print(f"     GPU soni: {gpu_count}")
        if models:
            # Deduplicate
            unique_models = list(dict.fromkeys(models))
            print(f"     Modellar: {', '.join(unique_models[:5])}")
            
    except (subprocess.SubprocessError, FileNotFoundError):
        pass


# =====================================================================
# KERNEL TOZALASH (--d)
# =====================================================================
def _delete_all_kernels():
    """Barcha 3 Kaggle akkauntidagi barcha kernel'larni o'chiradi."""
    total_deleted = 0

    for node in [0, 1, 2]:
        cfg = NODE_CONFIGS[node]
        user, token = _resolve_account(node)

        # Kaggle akkauntini env orqali sozlash
        kaggle_dir = os.path.expanduser("~/.kaggle")
        json_path = os.path.join(kaggle_dir, "kaggle.json")
        if os.path.exists(json_path):
            try:
                os.remove(json_path)
            except Exception:
                pass

        print(f"\n{'='*60}")
        print(f"  {cfg['label']}: {user}")
        print(f"{'='*60}")

        # Kernel ro'yxatini olish
        try:
            result = subprocess.run(
                ["kaggle", "kernels", "list", "--mine", "--csv"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                print(f"   ⚠️  Kernel ro'yxati olinmadi: {result.stderr[:100]}")
                continue
        except (subprocess.SubprocessError, FileNotFoundError) as e:
            print(f"   ⚠️  Xatolik: {e}")
            continue

        # CSV parser — birinchi qator header
        lines = result.stdout.strip().split("\n")
        if len(lines) < 2:
            print("   ✅ Kernel topilmadi.")
            continue

        # Header: ref,title,author,lastRunTime,totalVotes,...
        header = lines[0].split(",")
        try:
            ref_idx = header.index("ref")
        except ValueError:
            print("   ⚠️  CSV format noto'g'ri.")
            continue

        kernels = []
        for line in lines[1:]:
            cols = line.split(",")
            if len(cols) > ref_idx:
                ref = cols[ref_idx].strip()
                if ref:
                    kernels.append(ref)

        if not kernels:
            print("   ✅ Kernel topilmadi.")
            continue

        print(f"   {len(kernels)} ta kernel topildi, o'chirilmoqda...")

        for kref in kernels:
            try:
                del_result = subprocess.run(
                    ["kaggle", "kernels", "delete", kref, "-y"],
                    capture_output=True, text=True, timeout=30
                )
                if del_result.returncode == 0:
                    print(f"   ✅ {kref}")
                    total_deleted += 1
                else:
                    print(f"   ⚠️  {kref}: {del_result.stderr[:80]}")
            except (subprocess.SubprocessError, FileNotFoundError) as e:
                print(f"   ⚠️  {kref}: {e}")

    print(f"\n🎯 Jami: {total_deleted} ta kernel o'chirildi.")


# =====================================================================
# MAIN
# =====================================================================
def _launch_node(node: int, dry_run: bool = False):
    """Bitta node'ni generatsiya qilish + push qilish."""
    cfg = NODE_CONFIGS[node]
    kaggle_user, kaggle_api_token = _resolve_account(node)

    node_dir = cfg["node_dir"]
    os.makedirs(node_dir, exist_ok=True)

    # --- Server kodi (main_app.py) ---
    main_app_code = _build_main_app_code(cfg)
    encoded_main = base64.b64encode(main_app_code.encode("utf-8")).decode("utf-8")

    # --- Kod hash va delta aniqlash ---
    code_hash = hashlib.sha256(
        ("\n".join(cfg["pip_commands"]) + encoded_main).encode()
    ).hexdigest()[:16]

    env_prefix = cfg["env_prefix"]
    stored_hash = os.environ.get(f"{env_prefix}_VENV_HASH", "")
    stored_path = os.environ.get(f"{env_prefix}_DATASET_PATH", "")
    deltadata = (stored_hash != code_hash) if stored_hash else True

    print(f"\n{'='*60}")
    print(f"  {cfg['label']}: {kaggle_user}/{cfg['kernel_suffix']}")
    print(f"{'='*60}")

    if deltadata:
        print(f"🔄 DELTA ANIQLANDI! (eski={stored_hash[:8] if stored_hash else 'yoq'}, yangi={code_hash[:8]})")
        print("   Eski dataset ishlatilmaydi, venv yangidan quriladi.")
    else:
        print(f"✅ Kod o'zgarmagan (hash={code_hash[:8]}). Dataset ishlatiladi: {stored_path}")

    dataset_exists = bool(stored_path) and not deltadata

    # --- Runner kodi (run_server.py) ---
    runner_code = _build_runner_code(
        cfg, encoded_main, code_hash, deltadata, kaggle_user, kaggle_api_token
    )
    with open(os.path.join(node_dir, "run_server.py"), "w") as f:
        f.write(runner_code)

    # --- Metadata (kernel-metadata.json) ---
    dataset_sources = list(cfg.get("extra_dataset_sources", []))
    if dataset_exists:
        dataset_sources.append(stored_path)

    metadata = json.dumps({
        "id": f"{kaggle_user}/{cfg['kernel_suffix']}",
        "title": cfg["kernel_suffix"],
        "code_file": "run_server.py",
        "language": "python",
        "kernel_type": "script",
        "is_private": "true",
        "enable_gpu": "true",
        "enable_internet": "true",
        "machine_shape": "NvidiaTeslaT4x2",
        "dataset_sources": dataset_sources,
        "competition_sources": [],
        "kernel_sources": []
    }, indent=2)
    with open(os.path.join(node_dir, "kernel-metadata.json"), "w") as f:
        f.write(metadata)

    if dry_run:
        with open(os.path.join(node_dir, "main_app.py"), "w") as f:
            f.write(main_app_code)
        print(f"[DRY-RUN] {node_dir}/ fayllari yaratildi — Kaggle'ga push qilinmadi.")
        return

    # --- Kaggle akkaunt almashinuvi ---
    if cfg["requires_env"] or kaggle_api_token:
        kaggle_dir = os.path.expanduser("~/.kaggle")
        json_path = os.path.join(kaggle_dir, "kaggle.json")
        if os.path.exists(json_path):
            try:
                os.remove(json_path)
            except Exception:
                pass

    kernel_id = f"{kaggle_user}/{cfg['kernel_suffix']}"
    print(f"📊 Delta: {deltadata}, Hash: {code_hash[:8]}, Dataset: {dataset_sources}")

    # --- Avto-delete: eski kernel'ni o'chirish (running bo'lsa ham) ---
    print(f"🗑️  Eski kernel tekshirilmoqda: {kernel_id}...")
    try:
        status_result = subprocess.run(
            ["kaggle", "kernels", "status", kernel_id],
            capture_output=True, text=True, timeout=15
        )
        if status_result.returncode == 0:
            print(f"   Eski kernel topildi, o'chirilmoqda...")
            del_result = subprocess.run(
                ["kaggle", "kernels", "delete", kernel_id, "-y"],
                capture_output=True, text=True, timeout=30
            )
            if del_result.returncode == 0:
                print(f"   ✅ Eski kernel o'chirildi.")
            else:
                print(f"   ⚠️  O'chirishda xatolik (davom etilmoqda): {del_result.stderr[:120]}")
        else:
            print(f"   Eski kernel topilmadi — yangisi yuklanadi.")
    except (subprocess.SubprocessError, FileNotFoundError) as e:
        print(f"   ⚠️  Status tekshirishda xatolik (davom etilmoqda): {e}")

    print("Tayyor! Kagglega API orqali yuborilmoqda...")
    subprocess.run(["kaggle", "kernels", "push", "-p", node_dir, "--accelerator", "nvidiaTeslaT4x2"])
    print(f"✅ {cfg['label']} Kernel yuborildi!")


def main():
    parser = argparse.ArgumentParser(
        description="Yagona Kaggle node launch skripti (Node-0/1/2).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Misollar:\n"
            "  python3 launch_kaggle.py            # Node-0: LLM + TTS (UZ)\n"
            "  python3 launch_kaggle.py --node 1   # Node-1: STT (RU) + TTS (RU/EN)\n"
            "  python3 launch_kaggle.py --node 2   # Node-2: STT (EN) + STT (UZ)\n"
            "  python3 launch_kaggle.py --all       # Barcha 3 node'ni ketma-ket push qiladi\n"
            "  python3 launch_kaggle.py -d          # Barcha akkauntdagi kernel'larni o'chirish\n"
            "  python3 launch_kaggle.py -i          # GPU/TPU kvota ma'lumoti\n"
            "  python3 launch_kaggle.py -m          # Node monitoring (GPU, model, status)\n"
        ),
    )
    parser.add_argument("--node", type=int, choices=[0, 1, 2], default=0,
                        help="Qaysi node'ni ishga tushirish (default: 0)")
    parser.add_argument("--all", action="store_true",
                        help="Barcha 3 node'ni ketma-ket push qilish (0 → 1 → 2)")
    parser.add_argument("-d", "--delete-all", action="store_true",
                        help="Barcha 3 akkauntdagi barcha kernel'larni o'chirish")
    parser.add_argument("-i", "--info", action="store_true",
                        help="GPU/TPU kvota ma'lumotlarini ko'rsatish (ishlatilgan/qolgan/jami)")
    parser.add_argument("-m", "--monitor", action="store_true",
                        help="Barcha node'lar holatini ko'rsatish: GPU, model, status (auto-detect)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Faqat fayllarni generatsiya qiladi, Kaggle'ga push qilmaydi")
    args = parser.parse_args()

    _load_env()

    # -i flag: kvota ma'lumoti
    if args.info:
        _show_quota()
        return

    # -d flag: faqat o'chirish, launch qilmaslik
    if args.delete_all:
        print("🗑️  BARCHA AKKAUNTDAGI KERNELLAR O'CHIRILMOQDA...")
        _delete_all_kernels()
        return

    # -m flag: node monitoring
    if args.monitor:
        _monitor_nodes()
        return

    aes_256_key = os.environ.get("AES_256_KEY", "")
    if not aes_256_key:
        print("XATO: .env faylida AES_256_KEY topilmadi!")
        sys.exit(1)

    if args.all:
        nodes = [0, 1, 2]
        print(f"\n🚀 BARCHA 3 NODE LAUNCH QILINMOQDA...")
    else:
        nodes = [args.node]

    for node in nodes:
        _launch_node(node, dry_run=args.dry_run)

    if args.all:
        print(f"\n🎉 Barcha 3 node launch qilindi!")
        print(f"   Log'larni kuzatish: python3 scripts/log_stream.py --node all")


if __name__ == "__main__":
    main()
