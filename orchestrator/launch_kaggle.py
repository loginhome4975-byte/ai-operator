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
                os.environ[key.strip()] = val
                if key.strip() == "KAGGLE_KEY" and val.startswith("KGAT_"):
                    os.environ["KAGGLE_API_TOKEN"] = val

ORCHESTRATOR_URL = "https://orchestrator.traffix.uz"
NODE_PORT = 5001
hf_token = os.environ.get("HF_TOKEN", "")
node_comm_key = os.environ.get("NODE_COMM_KEY") or os.environ.get("ORCHESTRATOR_API_KEY", "")
aes_256_key = os.environ.get("AES_256_KEY", "")
if not aes_256_key:
    print("XATO: .env faylida AES_256_KEY topilmadi!")
    sys.exit(1)

kaggle_user = os.environ.get("KAGGLE_USERNAME", "bunyodbek7")
NODE_DIR = "kaggle_node"
os.makedirs(NODE_DIR, exist_ok=True)
kaggle_api_token = os.environ.get("KAGGLE_API_TOKEN") or os.environ.get("KAGGLE_KEY", "")

dataset_slug = f"{kaggle_user}/ai-operator-node0-venv"
dataset_name = "ai-operator-node0-venv"

pip_commands = [
    "/kaggle/working/venv/bin/pip install --upgrade pip",
    "/kaggle/working/venv/bin/pip install fastapi uvicorn nest-asyncio cryptography requests soundfile huggingface_hub",
    "/kaggle/working/venv/bin/pip install -U qwen-tts",
    "/kaggle/working/venv/bin/pip install llama-cpp-python --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cu121 --no-cache-dir",
]

security_path = os.path.join(os.path.dirname(__file__), "security_utils.py")
with open(security_path, "r") as f:
    security_code = f.read()

main_app_code = f"""
import os, sys, json, time, re, base64, threading, logging
import requests
import nest_asyncio
import uvicorn
import soundfile as sf
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

log_path = "/kaggle/working/app.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_path)]
)
log = logging.getLogger("node0")

ORCHESTRATOR_URL = "{ORCHESTRATOR_URL}"
NODE_TYPE = "kaggle"
NODE_PORT = 5001

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
            log.info(f"Register #{{attempt+1}}: {{r.status_code}} {{r.text[:80]}}")
            if r.status_code == 200:
                _upload_venv_if_needed()
        except Exception as e:
            log.warning(f"Register xatosi: {{e}}")
        attempt += 1
        time.sleep(2 if attempt < 5 else 60)

threading.Thread(target=keep_registering, args=(public_url,), daemon=True).start()

os.environ["AES_256_KEY"] = "{aes_256_key}"
{security_code}

log.info("Modellar yuklanmoqda...")

from huggingface_hub import login
login(token="{hf_token}")

log.info("[1/2] Sayro TTS (CUDA:1)...")
import torch
from qwen_tts import Qwen3TTSModel
tts = Qwen3TTSModel.from_pretrained("uzlm/sayro-tts-1.7B", device_map="cuda:1", dtype=torch.float16)
_speakers = tts.get_supported_speakers() if hasattr(tts, "get_supported_speakers") else []
_speaker = _speakers[0] if _speakers else "default"
_langs = tts.get_supported_languages() if hasattr(tts, "get_supported_languages") else []
_lang = "uz" if "uz" in _langs else (_langs[0] if _langs else "uz")

log.info("[2/2] Miyya LLM GGUF (CUDA:0)...")
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
            gpus.append({{
                "id": i, "name": p.name,
                "mem_total_gb": round(p.total_memory / 1024**3, 1),
                "mem_used_gb": round(torch.cuda.memory_allocated(i) / 1024**3, 2),
            }})
        return {{"status": "healthy", "node": "kaggle0", "models": ["miyya-qwen-7b", "sayro-tts-1.7b"], "gpus": gpus}}
    except Exception as e:
        return {{"status": "starting", "error": str(e)}}

@app.get("/logs")
async def get_logs():
    try:
        with open(log_path) as f:
            return {{"logs": f.read()[-50000:]}}
    except Exception as e:
        return {{"logs": str(e)}}

@app.post("/chat")
async def chat(req: EncryptedRequest):
    data = json.loads(decrypt_payload(req.encrypted_payload).decode('utf-8'))
    resp = llm.create_chat_completion(messages=data.get("messages", []), max_tokens=512)
    text = resp["choices"][0]["message"]["content"]
    return {{"encrypted_payload": encrypt_payload(json.dumps({{"response": text}}).encode('utf-8'))}}

@app.post("/synthesize")
async def synthesize(req: EncryptedRequest):
    data = json.loads(decrypt_payload(req.encrypted_payload).decode('utf-8'))
    text = (data.get("text") or "").strip()
    if hasattr(tts, "generate_custom_voice"):
        audio_data, sr = tts.generate_custom_voice(text=text, language=_lang, speaker=_speaker)
    elif hasattr(tts, "generate_voice_clone"):
        audio_data, sr = tts.generate_voice_clone(text=text)
    else:
        raise HTTPException(500, f"TTS method topilmadi: {{dir(tts)}}")
    audio = audio_data[0] if isinstance(audio_data, list) else audio_data
    path = f"/tmp/tts_{{time.time()}}.wav"
    sf.write(path, audio, samplerate=24000)
    return FileResponse(path, media_type="audio/wav")

nest_asyncio.apply()
uvicorn.run(app, host="0.0.0.0", port=NODE_PORT)
"""

encoded_main = base64.b64encode(main_app_code.encode('utf-8')).decode('utf-8')

# --- CODE HASH ---
code_hash = hashlib.sha256(
    ("\n".join(pip_commands) + encoded_main).encode()
).hexdigest()[:16]

stored_hash = os.environ.get("NODE0_VENV_HASH", "")
stored_path = os.environ.get("NODE0_DATASET_PATH", "")

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

print("Kaggle Node initialization starting...")

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
    os.system("DEBIAN_FRONTEND=noninteractive apt-get install tzdata python3.10 python3.10-venv python3.10-dev build-essential wget sox libsox-fmt-all -y")
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

# Node 0 always needs miyya-qwen-7b dataset for GGUF model
dataset_sources = ["bunyodbek7/miyya-qwen-7b"]
if dataset_exists:
    dataset_sources.append(stored_path)

metadata = json.dumps({
    "id": "bunyodbek7/ai-operator-kaggle-node",
    "title": "AI Operator Kaggle Node",
    "code_file": "run_server.py",
    "language": "python",
    "kernel_type": "script",
    "is_private": "true",
    "enable_gpu": "true",
    "enable_internet": "true",
    "machine_shape": "NvidiaTeslaT4",
    "dataset_sources": dataset_sources,
    "competition_sources": [],
    "kernel_sources": []
}, indent=2)

with open(os.path.join(NODE_DIR, "kernel-metadata.json"), "w") as f:
    f.write(metadata)

print(f"📊 Delta: {deltadata}, Hash: {code_hash[:8]}, Dataset: {dataset_sources}")
print("Tayyor! Kagglega API orqali yuborilmoqda...")
subprocess.run(["kaggle", "kernels", "push", "-p", NODE_DIR])
print("Kaggle Kernel yuborildi!")
