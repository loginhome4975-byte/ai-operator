# 🚚 AI Operator — Yangi Serverga Ko'chirish Bo'yicha To'liq Hujjat

> **Sana:** 2026-08-07
> **Maqsad:** Hozirgi serverdagi AI Operator loyihasiga bog'langan HAMMA narsani yangi serverga 1:1 ko'chirish uchun qo'llanma.
> **Muhim:** Ushbu hujjatda maxfiy qiymatlar (`<YASHIRIN>`) ko'rinishida berilgan. Haqiqiy qiymatlar backup arxivda:
> `/home/ubuntu/ai-operator-migration-20260807.tar.gz` (eski serverda).

---

## 📋 Mundarija

1. [Umumiy server holati](#1-umumiy-server-holati)
2. [Arxitektura (kim nima qiladi)](#2-arxitektura)
3. [Loyiha katalog tuzilishi](#3-loyiha-katalog-tuzilishi)
4. [Systemd servislar (5 ta)](#4-systemd-servislar)
5. [Portlar jadvali](#5-portlar-jadvali)
6. [DNS + Cloudflare Tunnel](#6-dns--cloudflare-tunnel)
7. [.env o'zgaruvchilari](#7-env-ozgaruvchilari)
8. [Kaggle (3 hisob, kernel'lar, dataset'lar)](#8-kaggle)
9. [Bog'liqliklar: Python / Node / APT](#9-bogliqliklar)
10. [Redis roli](#10-redis)
11. [Ma'lumot fayllari (runtime)](#11-malumot-fayllari)
12. [Log ko'rish (dashboard)](#12-log-korish)
13. [Backup arxiv tarkibi](#13-backup-arxiv-tarkibi)
14. [Yangi serverga ko'chirish — bosqichma-bosqich](#14-yangi-serverga-ko-chirish-reja)
15. [So'nggi testlar](#15-songgi-testlar)
16. [Boshqa loyihalar (tegishli emas)](#16-boshqa-loyihalar)
17. [Ma'lum cheklovlar va eslatmalar](#17-malum-cheklovlar)

---

## 1. Umumiy server holati

| Narsa | Qiymat |
|---|---|
| Provayder | Oracle Cloud Infrastructure (OCI) |
| OS | Ubuntu (hostname: `ozodboyev`) |
| Foydalanuvchi | `ubuntu` (sudo huquqli) |
| Loyiha yo'li | `/home/ubuntu/ai-operator` |
| Python | `python3` (system-wide paketlar, venv YO'Q) |
| Uvicorn | `/usr/bin/uvicorn` (orchestrator) |
| Git remote | `https://github.com/loginhome4975-byte/ai-operator.git` |
| Git user | `AI Operator <ubuntu@ai-operator>` |
| Redis | `active` (localhost:6379, parolsiz) |
| PostgreSQL | ishlamoqda (127.0.0.1:5432) — **ai-operator'ga bog'liq EMAS** (boshqa loyiha) |
| Nginx | o'rnatilmagan / `inactive` — ishlatilmaydi |
| Crontab | yo'q (user ham, root ham) |
| tmux | `down`, `ngrok`, `tgbot`, `wk` — **boshqa loyihalar**, ai-operator'ga aloqasi yo'q |

### Ishga tushgan servislar (2026-08-07 holati)

```
ai-orchestrator : active (enabled) — 0.0.0.0:8080
ai-sip-bridge   : active (enabled) — 0.0.0.0:8005
cloudflared     : active (enabled) — tunnel 1c95c754-... (traffix.uz)
ai-tunnel       : active (enabled) — LocalTunnel (zaxira)
ai-webhook      : enabled — BOSHQA loyiha (/home/ubuntu/ai-model)
redis-server    : active
```

---

## 2. Arxitektura

```
                     ┌───────────────────────────────┐
   Mijoz (qo'ng'iroq)│  Twilio / SIP provider        │
        │            └───────────────┬───────────────┘
        ▼                            │
┌───────────────────┐   TwiML/webhook│   wss://sip.traffix.uz/media-stream
│  AI SIP Bridge    │◄───────────────┤
│  (orchestrator/sip │               │
│   /main.py) :8005 │               │
└─────────┬─────────┘               │
          │  ws://127.0.0.1:8080/ws/call (X-API-Key header)
          ▼
┌───────────────────────────────────────────────────────────────┐
│  ORCHESTRATOR  (main.py)  :8080                                │
│  • WebSocket qo'ng'iroq sessiyalari (stream_controller)        │
│  • VAD → STT → Guardrail → LLM+tools → TTS                     │
│  • Profillar (isp_beta/isp_ru/isp_en) + session tarixi         │
│  • Redis: active_calls counter                                 │
│  • /logs web panel + /logs/stream SSE                          │
│  • /register-node (node URL'larini qabul qiladi)               │
└───────┬───────────────┬───────────────┬────────────────────────┘
        │ STT/TTS/LLM   │               │
        ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  NODE-0      │ │  NODE-1      │ │  NODE-2      │
│ LLM+TTS UZ   │ │ STT RU+TTS   │ │ STT EN+UZ    │
│ Kaggle       │ │ Kaggle       │ │ Kaggle       │
│ (bunyodbek7) │ │ (bunyodoz...)│ │ (bunyodbekoz)│
│ :5001 (cloud)│ │ :5003 (cloud)│ │ :5002 (cloud)│
└──────────────┘ └──────────────┘ └──────────────┘
```

**Asosiy oqim:** Mijoz qo'ng'iroq qiladi → Twilio/SIP → Bridge → Orchestrator WS → audio → VAD (energy) → STT (node) → guardrail → LLM (node-0, tool_calls bilan) → TTS (node) → ovoz mijozga.

---

## 3. Loyiha katalog tuzilishi

```
/home/ubuntu/ai-operator/
├── .env                        ← ⚠️ MAXFIY (arxivda) — git'da YO'Q
├── .env.example                ← .env shabloni (git'da bor)
├── .gitignore
├── dataset-metadata.json       ← Kaggle dataset (miyya-qwen-7b) meta
├── package.json                ← npm skriptlar (logs, logs:web)
├── requirements.txt            ← (bo'sh — asosiy orchestrator/requirements.txt)
├── deploy/systemd/             ← ★ YANGI: 5 ta systemd unit nusxasi
│   ├── ai-orchestrator.service
│   ├── ai-sip-bridge.service
│   ├── ai-tunnel.service
│   ├── ai-webhook.service      ← boshqa loyiha (ai-model), izoh bilan
│   └── cloudflared.service
├── orchestrator/
│   ├── main.py                 ← orchestrator (uvicorn main:app)
│   ├── launch_kaggle.py        ← node'larni generatsiya+push qiladi
│   ├── stream_controller.py    ← WS call pipeline (VAD/STT/LLM/TTS)
│   ├── session_manager.py      ← suhbat sessiyalari (TTL)
│   ├── profile_manager.py      ← profillar (prompt/tools/llm params)
│   ├── vad_utils.py            ← energy VAD (SILERO_VAD=1 da silero)
│   ├── audio_utils.py          ← WAV/PCM konversiya
│   ├── security_utils.py       ← AES shifrlash (node'lar bilan aloqa)
│   ├── sip_server.py           ← SIP trunk (UDP 5060) — qo'lda ishga tushiriladi
│   ├── sip_trunk.py            ← SIP registratsiya
│   ├── mock_llm.py             ← LLM yo'q paytda test uchun
│   ├── kaggle_node/  kaggle_node_1/  kaggle_node_2/
│   │       ├── run_server.py        ← ★ generatsiya qilinadi (gitignore)
│   │       ├── main_app.py          ← ★ generatsiya qilinadi
│   │       └── kernel-metadata.json ← ★ generatsiya qilinadi (gitignore)
│   ├── sip/
│   │   ├── main.py             ← Twilio bridge (8005)
│   │   └── requirements.txt
│   ├── crm/                    ← CRM integratsiya (base.py, dummy_sql.py)
│   ├── security/guardrail.py   ← input/output guardrail
│   ├── static/logs.html        ← log panel sahifasi
│   ├── menu.wav, wait.wav      ← IVR ovozlari (generatsiya: scripts/gen_ivr_wavs.py)
│   └── requirements.txt
├── scripts/
│   ├── log_stream.mjs          ← npm run logs (blessed TUI)
│   ├── log_stream.py           ← CLI log oqimi
│   ├── log_server.py           ← web log dashboard server (default 8099)
│   ├── chat_cli.py, tts_cli.py ← CLI test
│   ├── gen_ivr_wavs.py         ← menu/wait.wav generatsiya
│   ├── g711_roundtrip_test.py  ← G.711 kodlash testi
│   ├── pipeline_smoke_test.py  ← ★ E2E pipeline testi (STT→LLM→TTS)
│   └── ws_bridge_test.py       ← Twilio media-stream simulyatsiya
├── profiles/
│   ├── isp_beta/  (config.json, prompt.txt, tools.json, faq.txt)
│   ├── isp_ru/    (config.json, prompt.txt, tools.json, faq.txt)
│   └── isp_en/    (config.json, prompt.txt, tools.json, faq.txt)
├── builder/                    ← model trenirovka (lokal, katta — gitignore)
│   ├── data/companies/medline.json
│   ├── setup_model.py, merge_lora.py
│   └── src/model_training/...
└── .github/workflows/ci.yml
```

> ⚠️ **Gitignore eslatmasi:** `orchestrator/kaggle_node*/run_server.py`, `main_app.py`, `kernel-metadata.json` — `launch_kaggle.py` tomonidan generatsiya qilinadigan fayllar (asosiy manba `launch_kaggle.py` ichidagi template'lar).
> 🔒 **XAVFSIZLIK:** Bu fayllar generatsiya paytida `.env` dan `HF_TOKEN` kabi maxfiy qiymatlarni ichiga yozadi — **commit qilinmasligi shart** (gitignore'ga kiritilgan). Hozirgi to'liq nusxalar **backup arxivda** saqlangan. Qayta generatsiyadan keyin git status'da ko'rinsa, commit qilmang.

---

## 4. Systemd servislar

Barcha unit fayllar loyihada: `deploy/systemd/` (va backup arxivda). Yangi serverda ularni `/etc/systemd/system/` ga nusxalab `systemctl enable --now` qilish kerak.

### 4.1 `ai-orchestrator.service` — ASOSIY SERVIS

```ini
[Unit]
Description=AI Orchestrator Service
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/ai-operator/orchestrator
EnvironmentFile=/home/ubuntu/ai-operator/.env
ExecStart=/usr/bin/uvicorn main:app --host 0.0.0.0 --port 8080
Restart=always
Environment="PYTHONPATH=/home/ubuntu/ai-operator"
Environment="PYTHONDONTWRITEBYTECODE=1"
```

- **Port:** 8080 (TCP)
- **Log:** `journalctl -u ai-orchestrator.service -f`
- **Web panel:** `http://localhost:8080/logs` (shuningdek `https://orchestrator.traffix.uz/logs`)
- **API:** `/health`, `/register-node`, `/api/nodes/status`, `/api/profile/*`, `/ws/call/{caller_id}`, `/logs/stream/{nid}`

### 4.2 `ai-sip-bridge.service` — Twilio Bridge

```ini
[Unit]
Description=AI SIP Bridge (Twilio media stream)
After=ai-orchestrator.service
...
ExecStart=/usr/bin/python3 -m uvicorn orchestrator.sip.main:app --host 0.0.0.0 --port 8005
Environment=PYTHONPATH=/home/ubuntu/ai-operator
```

- **Port:** 8005 (TCP, HTTP+WebSocket)
- **Log:** `journalctl -u ai-sip-bridge.service -f`
- **Endpoints:** `POST /incoming-call` (TwiML), `WS /media-stream`
- **Talablar:** `.env` da `ORCHESTRATOR_API_KEY` (yo'q bo'lsa ishga tushmaydi), `TWILIO_AUTH_TOKEN` (bo'sh bo'lsa DEV mode), `PUBLIC_BASE_URL`

### 4.3 `cloudflared.service` — Cloudflare Tunnel

```ini
[Unit]
Description=Cloudflare Tunnel (traffix.uz)
...
ExecStart=/usr/local/bin/cloudflared tunnel --config /home/ubuntu/.cloudflared/config.yml run 1c95c754-7572-4f1f-ba81-242c7cbecffb
Restart=always
```

- **Tunnel ID:** `1c95c754-7572-4f1f-ba81-242c7cbecffb`
- **Config:** `/home/ubuntu/.cloudflared/config.yml`
- **Log:** `journalctl -u cloudflared.service -f` (standart output journal'da)
- **Metrics:** `http://127.0.0.1:20241/metrics`
- ⚠️ **Eslatma:** `StandardOutput=append:` ISHLATILMAYDI — append log fayliga `Permission denied` berib servisni yiqitar edi (status=209). Loglar journald'da.

### 4.4 `ai-tunnel.service` — LocalTunnel (zaxira/backup)

```ini
ExecStart=/usr/bin/npx localtunnel --port 8080 --subdomain ai-orchestrator-152
```

- Orqali: `https://ai-orchestrator-152.loca.lt` → orchestrator 8080
- Cloudflare tunnel ishlamay qolsa zaxira yo'l. Ixtiyoriy.

### 4.5 `ai-webhook.service` — ⚠️ BOSHQA LOYIHA

```ini
# /home/ubuntu/ai-model (Football AI Webhook, port 8000)
ExecStart=/home/ubuntu/ai-model/.venv/bin/uvicorn src.api.webhook:app --port 8000
```

- **ai-operator'ga tegishli EMAS.** Alohida loyiha (`/home/ubuntu/ai-model`). Yangi serverda bu loyiha kerak bo'lmasa — tashlab ketish mumkin. (tmux: `down` va `wk` sessiyalari ham o'sha loyihalar.)

---

## 5. Portlar jadvali

| Port | Protokol | Servis | Izoh |
|---|---|---|---|
| **8080** | TCP | ai-orchestrator | Asosiy orchestrator API + /logs panel |
| **8005** | TCP | ai-sip-bridge | Twilio webhook + media-stream WS |
| **8081** | TCP | (bo'sh) | Log dashboard uchun rezerv (`web.traffix.uz` → 8081) |
| 20241 | TCP | cloudflared | Metrics (127.0.0.1) |
| 5060 | UDP | sip_server.py | SIP trunk (qo'lda ishga tushirilganda; `.env` `SIP_LOCAL_PORT`) |
| 6379 | TCP | redis-server | Orchestrator cache (127.0.0.1) |
| 5001/5002/5003 | — | Kaggle node'lar | Node ichida (orchestrator emas) |
| 5432 | TCP | postgresql | Boshqa loyiha uchun — ai-operator ishlatmaydi |

> **Firewall:** Oracle Cloud OCI Security List'da faqat kerakli portlar ochilgan. Tunnel ishlatilgani uchun 8080/8005 ni tashqaridan ochish **shart emas** — cloudflared tashqariga chiqadigan ulanish orqali ishlaydi.

---

## 6. DNS + Cloudflare Tunnel

### 6.1 Cloudflare zona: `traffix.uz`

Tunnel bitta: `1c95c754-7572-4f1f-ba81-242c7cbecffb` — barcha domenlar shu tunnel orqali.

| DNS (CNAME) | Target | Tunnel ingress → servis |
|---|---|---|
| `orchestrator.traffix.uz` | `<tunnel-id>.cfargotunnel.com` (Proxied) | `http://localhost:8080` |
| `web.traffix.uz` | `<tunnel-id>.cfargotunnel.com` (Proxied) | `http://localhost:8081` |
| `sip.traffix.uz` | `<tunnel-id>.cfargotunnel.com` (Proxied) | `http://localhost:8005` |

> CNAME target formati: `1c95c754-7572-4f1f-ba81-242c7cbecffb.cfargotunnel.com`, **Proxied (orange cloud) YONIQ** bo'lishi shart.

### 6.2 `/home/ubuntu/.cloudflared/config.yml`

```yaml
tunnel: 1c95c754-7572-4f1f-ba81-242c7cbecffb
credentials-file: /home/ubuntu/.cloudflared/1c95c754-7572-4f1f-ba81-242c7cbecffb.json

ingress:
  - hostname: orchestrator.traffix.uz
    service: http://localhost:8080
  - hostname: web.traffix.uz
    service: http://localhost:8081
  - hostname: sip.traffix.uz
    service: http://localhost:8005
  - service: http_status:404
```

### 6.3 Kredensial fayllar (arxivda)

- `/home/ubuntu/.cloudflared/1c95c754-7572-4f1f-ba81-242c7cbecffb.json` — tunnel token (175 bayt)
- `/home/ubuntu/.cloudflared/cert.pem` — cloudflared sertifikat
- Binary: `/usr/local/bin/cloudflared` (2026.7.3) — installer: `/tmp/cloudflared.deb` (arxivda)

---

## 7. .env o'zgaruvchilari

Fayl: `/home/ubuntu/ai-operator/.env` (git'da YO'Q, backup arxivda). To'liq ro'yxat:

| O'zgaruvchi | Maqsad | Holat |
|---|---|---|
| `KAGGLE_USERNAME` | Node-0 hisob logini | `bunyodbek7` |
| `KAGGLE_KEY` / `KAGGLE_API_TOKEN` | Node-0 Kaggle token | `<YASHIRIN>` (KGAT_...) |
| `KAGGLE_USERNAME_1` | Node-1 hisob | `bunyodozodboyev` |
| `KAGGLE_KEY_1` | Node-1 token | `<YASHIRIN>` |
| `KAGGLE_USERNAME_2` | Node-2 hisob | `bunyodbekozodboyev` |
| `KAGGLE_KEY_2` | Node-2 token | `<YASHIRIN>` |
| `NODE0_DATASET_PATH` | Node-0 venv dataset | `bunyodbek7/ai-operator-node0-venv` |
| `NODE1_DATASET_PATH` | Node-1 venv dataset | `bunyodozodboyev/ai-operator-node1-venv` |
| `NODE2_DATASET_PATH` | Node-2 venv dataset | `bunyodbekozodboyev/ai-operator-node2-venv` |
| `NODE0_VENV_HASH` | Node-0 venv identifikatori | `100aaa09c3a3c8da` |
| `NODE1_VENV_HASH` | Node-1 venv identifikatori | `e7bad62b43486321` |
| `NODE2_VENV_HASH` | Node-2 venv identifikatori | `8670bd31b9d4a3f9` |
| `HF_TOKEN` | HuggingFace token (model yuklash) | `<YASHIRIN>` |
| `ORCHESTRATOR_API_KEY` | REST auth (REST endpoint'lar) | `<YASHIRIN>` |
| `AUDIT_VIEW_KEY` | Audit ko'rish — API_KEY'dan FARQLI bo'lishi shart | `<YASHIRIN>` |
| `NODE_COMM_KEY` | Node'lar bilan aloqa kaliti (derived: AES_256_KEY) | `<YASHIRIN>` |
| `AES_256_KEY` | Payload shifrlash | `<YASHIRIN>` |
| `SHARED_SECRET_KEY` | Qo'shimcha shifrlash | `<YASHIRIN>` |
| `PUBLIC_BASE_URL` | TwiML'dagi public URL | `https://sip.traffix.uz` |
| `TWILIO_AUTH_TOKEN` | Twilio signature tekshiruvi | **BO'SH (DEV mode!)** — to'ldirish kerak |
| `SIP_BRIDGE_PORT` | Bridge porti | `8005` |

> ⚠️ **STRICT_SECURITY=true** (default): `ORCHESTRATOR_API_KEY` va `AUDIT_VIEW_KEY` bo'lmasa yoki teng bo'lsa orchestrator start'da o'zini o'ldiradi.

---

## 8. Kaggle

### 8.1 Uch hisob va kernel'lar

| Node | Hisob | Kernel slug | Vazifasi | Models |
|---|---|---|---|---|
| 0 | `bunyodbek7` | `ai-operator-kaggle-node` | LLM + TTS UZ | Miyya Qwen 7B (GGUF, llama.cpp), Sayro TTS 1.7B |
| 1 | `bunyodozodboyev` | `ai-operator-kaggle-node-1` | STT RU + TTS RU/EN | Whisper large-v3, Chatterbox Multilingual TTS |
| 2 | `bunyodbekozodboyev` | `ai-operator-kaggle-node-2` | STT EN + STT UZ | Canary-Qwen, Kotib/uzbek_stt_v1 |

### 8.2 Kernel metadata (generatsiya: `launch_kaggle.py`)

```json
{
  "id": "bunyodbek7/ai-operator-kaggle-node",
  "code_file": "run_server.py",
  "kernel_type": "script",
  "is_private": "true",
  "enable_gpu": "true",
  "enable_internet": "true",
  "machine_shape": "NvidiaTeslaT4x2",
  "dataset_sources": ["bunyodbek7/miyya-qwen-7b"]
}
```

- **GPU:** `NvidiaTeslaT4x2` (2× T4) — CLI'da `NvidiaTeslaT4x2` nomi mavjud emas, `launch_kaggle.py` uni kernel-metadata.json'ga yozadi.
- Node-1/2 `dataset_sources` bo'sh (o'z HF model'larini internet orqali yuklaydi).

### 8.3 Venv dataset'lar (hash'lar)

Karnellar venv'ni Kaggle dataset'idan tez yuklaydi:
- `bunyodbek7/ai-operator-node0-venv` (hash `100aaa09c3a3c8da`)
- `bunyodozodboyev/ai-operator-node1-venv` (hash `e7bad62b43486321`)
- `bunyodbekozodboyev/ai-operator-node2-venv` (hash `8670bd31b9d4a3f9`)

Hash o'zgarsa → yangi dataset versiyasi push qilinadi (launch_kaggle.py avtomatik).

### 8.4 Node kod manbasi

- **Asosiy manba:** `orchestrator/launch_kaggle.py` (ichida `_build_main_app_code()` va runner template'lari)
- **Generatsiya qilingan fayllar** (`orchestrator/kaggle_node*/run_server.py`, `main_app.py`, `kernel-metadata.json`) — backup arxivda saqlangan.
- **Push:** `python3 launch_kaggle.py --all` | `--node 0/1/2` | `-m` (monitor) | `-d` (delete-all) | `-i` (info) | `--dry-run`

### 8.5 Kredensiallar

- Kaggle tokenlar **faqat `.env` da** (`KAGGLE_API_TOKEN`, `KAGGLE_KEY_1`, `KAGGLE_KEY_2`).
- `~/.kaggle/` **bo'sh** (kaggle.json yo'q) — hammasi .env orqali.
- `kaggle` CLI: `/home/ubuntu/.local/bin/kaggle` (2.2.4)

---

## 9. Bog'liqliklar

### 9.1 Python (system-wide, /usr/bin/uvicorn)

`orchestrator/requirements.txt`:
```
fastapi
uvicorn
httpx
redis
pydantic
prometheus_client
cryptography
torch
numpy
python-multipart
```

Amalda o'rnatilgan muhimlari (to'liq 236 paket: `pip-freeze-system.txt` — arxivda):
`fastapi 0.140.0, uvicorn 0.27.1, httpx 0.28.1, redis 8.0.1, pydantic 2.13.4, prometheus_client 0.26.0, cryptography 41.0.7, torch 2.13.0, numpy 2.5.1, transformers 5.14.1, websockets 17.0.1, nest-asyncio 1.6.0, kaggle 2.2.4, requests 2.31.0`

SIP bridge (`orchestrator/sip/requirements.txt`): `fastapi, uvicorn, websockets` (amalda websockets 17.0.1).

> ⚠️ Python 3.13'da `audioop` yo'qoladi — bridge'da numpy LUT fallback bor (`g711_roundtrip_test.py` bilan tasdiqlangan).

### 9.2 Node (package.json)

```json
"dependencies": { "blessed": "^0.1.81", "dotenv": "^17.4.2" }
"scripts": {
  "logs":    "node scripts/log_stream.mjs",
  "logs:0|1|2": "node scripts/log_stream.mjs --node N",
  "logs:web": "python3 scripts/log_server.py"
}
```

- `localtunnel` — `npx` orqali (ai-tunnel.service ishlatadi)
- npm-global: `diffray`, `omniroute` (keraksiz)

### 9.3 APT paketlar

| Paket | Versiya | Kerakligi |
|---|---|---|
| `cloudflared` | 2026.7.3 | Tunnel (deb: `/tmp/cloudflared.deb`, arxivda) |
| `ffmpeg` | 6.1.1 | Audio konversiya |
| `redis-server` | — | Orchestrator counter (active) |
| `python3-pip`, `python3-venv` | — | Pip/venv |

---

## 10. Redis

- **Holat:** active, `localhost:6379`, parolsiz
- **Rol:** faqat `active_calls` counter (key: `active_calls`, TTL 3600s). Redis tushsa orchestrator in-memory fallback'ga o'tadi (WS call 1011 bermaydi).
- **Ma'lumotlarni qaytarib bo'lmaydi** — cache xarakterida.

---

## 11. Ma'lumot fayllari

| Fayl | Tavsif |
|---|---|
| `/tmp/orchestrator_analytics.json` | Analitika (qo'ng'iroqlar soni, til taqsimoti, tarix). `ANALYTICS_PERSIST_PATH` (default `/tmp/...`). **Backup qilish kerak** — arxivda `data/orchestrator_analytics.json` |
| `orchestrator/menu.wav` | IVR menyu (8kHz mono, 1.2s) — `scripts/gen_ivr_wavs.py` generatsiya qiladi |
| `orchestrator/wait.wav` | IVR kutish (8kHz mono, 2s) |
| `orchestrator/static/logs.html` | Log panel sahifa nusxasi |

---

## 12. Log ko'rish

| Usul | Buyruq | Tavsif |
|---|---|---|
| Web panel | `http://localhost:8080/logs` | Orchestrator ichidagi panel (3 node + orchestrator loglari, real-time SSE) — **asosiy usul** |
| Web dashboard | `python3 scripts/log_server.py --port 8099` | Alohida server (`web.traffix.uz` → 8081 uchun `--port 8081` bilan ishga tushirish mumkin) |
| TUI | `npm run logs` | blessed terminal interfeysi (3 ta oyna) |
| CLI | `python3 scripts/log_stream.py --node all` | Terminal oqimi |
| Journal | `journalctl -u ai-orchestrator.service -f` | Orchestrator |
| | `journalctl -u ai-sip-bridge.service -f` | Bridge |
| | `journalctl -u cloudflared.service -f` | Tunnel |

---

## 13. Backup arxiv tarkibi

Fayl: **`/home/ubuntu/ai-operator-migration-20260807.tar.gz`** (~19 MB)

```
ai-operator-migration-staging/
├── env/
│   ├── .env                     ← MAXFIY (real qiymatlar)
│   └── .env.example
├── cloudflared/
│   ├── config.yml
│   ├── 1c95c754-7572-4f1f-ba81-242c7cbecffb.json   ← tunnel token
│   └── cert.pem
├── kaggle_nodes/
│   ├── kaggle_node/    (run_server.py, main_app.py, kernel-metadata.json)
│   ├── kaggle_node_1/  (…)
│   └── kaggle_node_2/  (…)
├── audio/
│   ├── menu.wav
│   └── wait.wav
├── data/
│   ├── orchestrator_analytics.json
│   └── pip-freeze-system.txt
├── deploy/
│   ├── cloudflared.deb          ← cloudflared 2026.7.3 installer
│   └── systemd/                 ← 5 ta unit
```

---

## 14. Yangi serverga ko'chirish reja

### Bosqich 0 — Tayyorgarlik (eski serverda)
- [x] Git commit (hozirgi holat 1:1) — **bu commit**
- [x] Backup arxiv: `ai-operator-migration-20260807.tar.gz`
- [ ] Arxivni yangi serverga ko'chirish (scp/rsync/usb)

### Bosqich 1 — Asosiy dasturlar
```bash
# Yangi serverda (Ubuntu 22.04/24.04, root/sudo)
sudo apt update && sudo apt install -y python3 python3-pip python3-venv redis-server ffmpeg nodejs npm
# cloudflared
sudo dpkg -i /path/to/cloudflared.deb        # yoki arxivdan
# Python paketlar
sudo pip3 install -r /home/ubuntu/ai-operator/orchestrator/requirements.txt
sudo pip3 install websockets nest-asyncio kaggle
# Node
cd /home/ubuntu/ai-operator && npm install
```

### Bosqich 2 — Loyiha
```bash
# Yo'l 1: git clone
git clone https://github.com/loginhome4975-byte/ai-operator.git /home/ubuntu/ai-operator
# Yo'l 2: arxiv + git (agar maxfiy fayllar kerak bo'lsa)
#   tar -xzf ai-operator-migration-20260807.tar.gz
#   .env, .cloudflared/, kaggle_nodes/, audio/, data/ ni joy-joyiga qo'yish
```

### Bosqich 3 — .env va systemd
```bash
cp /home/ubuntu/ai-operator/.env.example /home/ubuntu/ai-operator/.env
# → .env'ni arxivdagi REAL fayl bilan almashtiring (tokenlar!)
sudo cp /home/ubuntu/ai-operator/deploy/systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ai-orchestrator ai-sip-bridge
sudo systemctl enable --now cloudflared        # config+creds joyida bo'lsa
sudo systemctl enable --now ai-tunnel          # ixtiyoriy
```

### Bosqich 4 — Tunnel
```bash
# 1) DNS: Cloudflare'da CNAME (Proxied) — zona yangi bo'lsa
#    orchestrator.traffix.uz / web.traffix.uz / sip.traffix.uz
#    → 1c95c754-7572-4f1f-ba81-242c7cbecffb.cfargotunnel.com
# 2) Arxivdan: /home/ubuntu/.cloudflared/ (config.yml + token + cert)
# 3) systemctl restart cloudflared
```

### Bosqich 5 — Kaggle node'lar
```bash
cd /home/ubuntu/ai-operator/orchestrator
python3 launch_kaggle.py --dry-run   # fayllar generatsiyasini tekshirish
python3 launch_kaggle.py --all       # 3 node'ni push qilish (T4x2)
python3 launch_kaggle.py -m          # holat kuzatish
```

### Bosqich 6 — Tekshiruv (pastga qarang)

---

## 15. So'nggi testlar

```bash
# Orchestrator
curl http://localhost:8080/health
curl http://localhost:8080/logs                 # 200 bo'lishi kerak

# SIP bridge (tunnel orqali)
curl -s -X POST 'https://sip.traffix.uz/incoming-call' \
  -d 'CallSid=CA1&From=%2B998901234567&To=%2B998700000000'
#  → TwiML: <Stream url="wss://sip.traffix.uz/media-stream">

# E2E pipeline (orchestrator qatnashgan to'liq test)
cd /home/ubuntu/ai-operator/orchestrator && python3 ../scripts/pipeline_smoke_test.py

# Twilio simulyatsiya (bridge + orchestrator + correlation)
python3 scripts/ws_bridge_test.py

# G.711 kodlash (audioop/numpy parity)
python3 scripts/g711_roundtrip_test.py
```

---

## 16. Boshqa loyihalar

Ushbu serverda ai-operator'dan boshqa loyihalar ham bor — **ular bu hujjatga kirmaydi**, faqat eslatma:

| Loyiha | Yo'l | Servis/tmux |
|---|---|---|
| Football AI Webhook | `/home/ubuntu/ai-model` | `ai-webhook.service` (8000) |
| Football AI Match | `~/ai-match` | tmux `down` |
| Telegram bot (ai-model) | — | tmux `tgbot` |
| boshqa | — | tmux `ngrok`, `wk` |

---

## 17. Ma'lum cheklovlar

1. **`TWILIO_AUTH_TOKEN` bo'sh** — bridge DEV mode'da ishlayapti (Twilio signature tekshirilmaydi). Twilio console'dan token olib `.env` ga yozish kerak, keyin `systemctl restart ai-sip-bridge`.
2. **GPU:** Kernel metadata `NvidiaTeslaT4x2`. CLI flag'da bu nom yo'q — metadata orqali yuboriladi. P100'da torch (cu130) kernel image'ga mos emas (CC 6.0) — shuning uchun T4x2 majburiy.
3. **NCCL:** Node-2 torch import'ida `libtorch_cuda.so: undefined symbol: ncclCommResume` chiqishi mumkin — yechim `launch_kaggle.py`'ga kiritilgan (NCCL fix).
4. **`web.traffix.uz` → 8081 hozir bo'sh.** Panel asosiy `orchestrator.traffix.uz/logs` orqali ishlaydi. `web.traffix.uz` ishlashi uchun: `python3 scripts/log_server.py --port 8081` (systemd unit yaratish tavsiya).
5. **`builder/llama.cpp`, `builder/venv`, `builder/models`** — lokal katta resurslar (gitignore), Kaggle/dataset'lar orqali almashtiriladi.
6. **cloudflared restart** qilganda eski jarayon `kill` (SIGTERM) bilan o'lmasligi mumkin — `sudo kill -9 <pid>` kerak bo'lishi mumkin; yoki `systemctl restart cloudflared`.
7. **Analytics fayli** `/tmp/orchestrator_analytics.json` — `/tmp` tozalansa yo'qoladi; saqlash uchun `ANALYTICS_PERSIST_PATH`'ni boshqa joyga ko'rsatish mumkin.
