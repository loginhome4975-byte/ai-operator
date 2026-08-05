import os
import sys
import subprocess
import time
import base64
import hashlib
import json

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
if os.path.exists(env_path):
    with open(env_path, "r") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                val = val.strip().strip('"').strip("'")
                if key.strip() == "KAGGLE_USERNAME_1":
                    os.environ["KAGGLE_USERNAME"] = val
                elif key.strip() == "KAGGLE_KEY_1":
                    os.environ["KAGGLE_KEY"] = val
                    if val.startswith("KGAT_"):
                        os.environ["KAGGLE_API_TOKEN"] = val
                else:
                    os.environ[key.strip()] = val

kaggle_user = os.environ.get("KAGGLE_USERNAME")
if not kaggle_user:
    print("XATO: .env faylida KAGGLE_USERNAME_1 topilmadi!")
    sys.exit(1)

ORCHESTRATOR_URL = "https://orchestrator.traffix.uz"
NODE_PORT = 5003
node_comm_key = os.environ.get("NODE_COMM_KEY") or os.environ.get("ORCHESTRATOR_API_KEY", "")
hf_token = os.environ.get("HF_TOKEN", "")
aes_256_key = os.environ.get("AES_256_KEY", "")
if not aes_256_key:
    print("XATO: .env faylida AES_256_KEY topilmadi!")
    sys.exit(1)

NODE_DIR = "kaggle_node_1"
os.makedirs(NODE_DIR, exist_ok=True)
kaggle_api_token = os.environ.get("KAGGLE_API_TOKEN") or os.environ.get("KAGGLE_KEY", "")

dataset_slug = f"{kaggle_user}/ai-operator-node1-venv"
dataset_name = "ai-operator-node1-venv"

pip_commands = [
    "/kaggle/working/venv/bin/pip install --upgrade pip",
    "/kaggle/working/venv/bin/pip install fastapi uvicorn pydantic python-multipart transformers torch torchaudio librosa soundfile accelerate nest-asyncio requests cryptography",
    "/kaggle/working/venv/bin/pip install chatterbox-tts torchaudio",
]

security_path = os.path.join(os.path.dirname(__file__), "security_utils.py")
with open(security_path, "r") as f:
    security_code = f.read()

main_app_code = f"""
import os, sys, json, time, re, base64, threading, logging, io, wave
import requests
import nest_asyncio
import uvicorn
import soundfile as sf
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("node1")

os.environ["AES_256_KEY"] = "{aes_256_key}"
{security_code}

ORCHESTRATOR_URL = "{ORCHESTRATOR_URL}"
NODE_TYPE = "kaggle1"
NODE_PORT = 5003

HF_TOKEN = "{hf_token}"
if not HF_TOKEN:
    log.warning("HF_TOKEN o'rnatilmagan")
else:
    from huggingface_hub import login
    login(token=HF_TOKEN)
    log.info("HuggingFace login qilindi")

log.info("Cloudflare tunnel ochilmoqda...")
os.system("wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -O cloudflared && chmod +x cloudflared")
os.system(f"./cloudflared tunnel --url http://127.0.0.1:{NODE_PORT} > cloudflared.log 2>&1 &")

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
        base_data = {{k: str(info[k]) for k in ["dataset_slug", "dataset_name", "venv_hash", "kaggle_user", "kaggle_key", "deltadata"] if k in info}}
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
            log.info(f"Register #{{attempt+1}}: {{r.status_code}}")
            if r.status_code == 200:
                _upload_venv_if_needed()
        except Exception as e:
            log.warning(f"Register: {{e}}")
        attempt += 1
        time.sleep(2 if attempt < 5 else 60)

threading.Thread(target=keep_registering, args=(public_url,), daemon=True).start()

import torch
from transformers import pipeline, AutoModelForSpeechSeq2Seq, AutoProcessor

device_stt, device_tts = "cuda:0", "cuda:1"
dt = torch.float16

log.info("[1/2] Whisper large-v3 RU STT (CUDA:0)...")
stt_model = AutoModelForSpeechSeq2Seq.from_pretrained(
    "openai/whisper-large-v3", torch_dtype=dt, low_cpu_mem_usage=True, use_safetensors=True
).to(device_stt)
stt_processor = AutoProcessor.from_pretrained("openai/whisper-large-v3")
stt_pipe = pipeline("automatic-speech-recognition", model=stt_model,
    tokenizer=stt_processor.tokenizer, feature_extractor=stt_processor.feature_extractor,
    torch_dtype=dt, device=device_stt)

log.info("[2/2] Chatterbox Multilingual TTS RU/EN (CUDA:1)...")
try:
    import torchaudio as ta
    from chatterbox.mtl_tts import ChatterboxMultilingualTTS
    tts_pipe = ChatterboxMultilingualTTS.from_pretrained(device="cuda:1", t3_model="v3")
    _speaker = None
except Exception as e:
    log.warning(f"Chatterbox TTS yuklanmadi: {{e}}")
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
            gpus.append({{"id": i, "name": p.name,
                "mem_total_gb": round(p.total_memory/1024**3,1),
                "mem_used_gb": round(torch.cuda.memory_allocated(i)/1024**3,2)}})
        return {{"status": "healthy", "node": "kaggle1",
                 "models": ["whisper-large-v3", "chatterbox-multilingual" if tts_pipe else "chatterbox-failed"],
                 "gpus": gpus}}
    except Exception as e:
        return {{"status": "starting", "error": str(e)}}

@app.post("/transcribe/ru")
async def transcribe_ru(req: STTRequest):
    audio_bytes = decrypt_payload(req.encrypted_audio)
    path = f"/tmp/ru_{{int(time.time())}}.wav"
    with open(path, "wb") as f:
        f.write(audio_bytes)
    result = stt_pipe(path, generate_kwargs={{"language": "russian"}})
    return {{"encrypted_text": encrypt_payload(result["text"].encode('utf-8'))}}

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
        path = f"/tmp/tts_{{lang}}_{{int(time.time())}}.wav"
        ta.save(path, wav, tts_pipe.sr)
        return FileResponse(path, media_type="audio/wav")
    except Exception as e:
        log.error(f"TTS {{lang}}: {{e}}")
        raise HTTPException(500, str(e))

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
"""

encoded_main = base64.b64encode(main_app_code.encode('utf-8')).decode('utf-8')

# --- CODE HASH ---
code_hash = hashlib.sha256(
    ("\n".join(pip_commands) + encoded_main).encode()
).hexdigest()[:16]

stored_hash = os.environ.get("NODE1_VENV_HASH", "")
stored_path = os.environ.get("NODE1_DATASET_PATH", "")

deltadata = (stored_hash != code_hash) if stored_hash else True

if deltadata:
    print(f"🔄 DELTA ANIQLANDI! (eski={stored_hash[:8] if stored_hash else 'yoq'}, yangi={code_hash[:8]})")
    print(f"   Eski dataset ishlatilmaydi, venv yangidan quriladi.")
else:
    print(f"✅ Kod o'zgarmagan (hash={code_hash[:8]}). Dataset ishlatiladi: {stored_path}")

dataset_exists = bool(stored_path) and not deltadata

pip_commands_str = ",\n        ".join(f'"""{cmd}"""' for cmd in pip_commands)

runner_code = f"""
import os, sys, subprocess, hashlib, tarfile, json, base64, shutil, time

print("Kaggle Node 1 initialization starting...")

encoded_main = "{encoded_main}"
with open("/kaggle/working/main_app.py", "w") as f:
    f.write(base64.b64decode(encoded_main).decode('utf-8'))

VENV_HASH = "{code_hash}"
DATASET_NAME = "{dataset_name}"
DATASET_SLUG = "{dataset_slug}"
DELTADATA = "{'true' if deltadata else 'false'}"
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

if not use_cache:
    print("Python 3.10 + venv qurilmoqda...")
    os.system("DEBIAN_FRONTEND=noninteractive apt-get update -y")
    os.system("DEBIAN_FRONTEND=noninteractive apt-get install tzdata python3.10 python3.10-venv python3.10-dev build-essential -y")
    os.system("python3.10 -m venv /kaggle/working/venv")

    print("[1/2] Kutubxonalar venv ichiga o'rnatilmoqda...")
    pip_cmds = [{pip_commands_str}]
    for cmd in pip_cmds:
        print(f"  $ {{cmd}}")
        ret = os.system(cmd)
        if ret != 0:
            print(f"  Pip xato (exit={{ret}})")

    # ======== ARXIVLASH ========
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
    }}
    with open("/kaggle/working/.venv_upload_info", "w") as f:
        json.dump(upload_info, f)
    print("  Venv arxiv tayyor — tunnel ochilgach orchestrator'ga yuboriladi")
else:
    print("[1/2] Cached venv ishlatildi")

print("[2/2] Server ishga tushirilmoqda...")
os.system(f"/kaggle/working/venv/bin/python /kaggle/working/main_app.py")
"""

with open(os.path.join(NODE_DIR, "run_server.py"), "w") as f:
    f.write(runner_code)

dataset_sources = []
if dataset_exists:
    dataset_sources.append(stored_path)

metadata = json.dumps({
    "id": f"{kaggle_user}/ai-operator-kaggle-node-1",
    "title": "ai-operator-kaggle-node-1",
    "code_file": "run_server.py",
    "language": "python",
    "kernel_type": "script",
    "is_private": "false",
    "enable_gpu": "true",
    "enable_internet": "true",
    "machine_shape": "NvidiaTeslaT4",
    "dataset_sources": dataset_sources,
    "competition_sources": [],
    "kernel_sources": []
}, indent=2)

with open(os.path.join(NODE_DIR, "kernel-metadata.json"), "w") as f:
    f.write(metadata)

kaggle_dir = os.path.expanduser("~/.kaggle")
json_path = os.path.join(kaggle_dir, "kaggle.json")
if os.path.exists(json_path):
    try: os.remove(json_path)
    except Exception: pass

print(f"📊 Delta: {deltadata}, Hash: {code_hash[:8]}, Dataset: {dataset_sources}")
print("Tayyor! Kagglega API orqali yuborilmoqda...")
subprocess.run(["kaggle", "kernels", "push", "-p", NODE_DIR])
print("Kaggle 1 Kernel yuborildi!")
