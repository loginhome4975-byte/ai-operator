import os
import re
import json
import uuid
import hmac
import hashlib
import asyncio
import logging
from urllib.parse import parse_qs

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
log = logging.getLogger("sip_bridge")

# ── audioop: Python 3.13 da olib tashlangan. Yo'q bo'lsa numpy-based G.711 ──
try:
    import audioop  # type: ignore
    _HAS_AUDIOOP = True
except Exception as e:  # noqa: BLE001
    log.warning(f"audioop yuklanmadi (Python 3.13 mos): {e}")
    audioop = None  # type: ignore
    _HAS_AUDIOOP = False

try:
    import numpy as np
    _HAS_NUMPY = True
except Exception:  # noqa: BLE001
    np = None  # type: ignore
    _HAS_NUMPY = False

_BIAS = 0x84
_CLIP = 32635


def _build_ulaw_luts():
    """G.711 mu-law decode/encode jadvallari (audioop bo'lmasa ishlatiladi).

    Decode: standart formula (audioop.ulaw2lin bilan bir xil — test qilingan).
    Encode: decode jadvalidan ENG YAQIN byte'ni topish orqali quriladi —
    kafolatlangan to'g'ri round-trip (Twilio ham standart G.711 dekodlaydi).
    """
    if not _HAS_NUMPY:
        return None, None

    def _dec(b):
        u = (~b) & 0xFF
        sign = 0x80 & u
        exponent = (u >> 4) & 0x07
        mantissa = u & 0x0F
        sample = (((mantissa << 3) + _BIAS) << exponent) - _BIAS
        return -sample if sign else sample

    dec = np.array([_dec(b) for b in range(256)], dtype=np.int32)

    # Har bir int16 sample uchun dekod qiymati eng yaqin bo'lgan byte
    order = np.argsort(dec)
    sorted_dec = dec[order]
    targets = np.arange(-32768, 32768, dtype=np.int32)
    pos = np.searchsorted(sorted_dec, targets, side="left")
    pos = np.clip(pos, 1, 255)
    a = order[pos - 1]
    b = order[pos]
    da = np.abs(dec[a] - targets)
    db = np.abs(dec[b] - targets)
    enc = np.where(da <= db, a, b).astype(np.uint8)
    return dec.astype(np.int16), enc


_ULAW_DEC, _ULAW_ENC = _build_ulaw_luts()


def _safe_id(value) -> str:
    return re.sub(r"[^A-Za-z0-9_\-:.]", "_", str(value or ""))[:64]


def _resample_linear(pcm_i16: bytes, src_rate: int, dst_rate: int) -> bytes:
    """np.interp asosida linear resample — audioop.ratecv o'rnini bosadi."""
    if not _HAS_NUMPY or src_rate == dst_rate or not pcm_i16:
        return pcm_i16
    arr = np.frombuffer(pcm_i16, dtype=np.int16).astype(np.float32)
    n = len(arr)
    if n == 0:
        return b""
    target_n = max(1, int(round(n * dst_rate / float(src_rate))))
    xp = np.linspace(0.0, 1.0, num=n)
    x = np.linspace(0.0, 1.0, num=target_n)
    return np.interp(x, xp, arr).astype(np.int16).tobytes()


def _mulaw_to_pcm16k(payload: bytes, src_rate: int = 8000) -> bytes:
    """mulaw → PCM 16kHz. audioop bo'lsa audioop, bo'lmasa numpy LUT."""
    if not payload:
        return b""
    if _HAS_AUDIOOP and audioop is not None:
        pcm = audioop.ulaw2lin(payload, 2)
        if src_rate != 16000:
            pcm, _ = audioop.ratecv(pcm, 2, 1, src_rate, 16000, None)
        return pcm
    if _HAS_NUMPY and _ULAW_DEC is not None:
        arr = _ULAW_DEC[np.frombuffer(payload, dtype=np.uint8)]
        return _resample_linear(arr.tobytes(), src_rate, 16000)
    log.warning("[SIP Bridge] mulaw konversiya imkonsiz — audioop ham, numpy ham yo'q")
    return payload


def _pcm16k_to_mulaw(data: bytes, src_rate: int = 16000) -> bytes:
    """PCM 16kHz → mulaw 8kHz. audioop bo'lsa audioop, bo'lmasa numpy LUT."""
    if not data:
        return b""
    if _HAS_AUDIOOP and audioop is not None:
        if src_rate != 8000:
            pcm8, _ = audioop.ratecv(data, 2, 1, src_rate, 8000, None)
        else:
            pcm8 = data
        return audioop.lin2ulaw(pcm8, 2)
    if _HAS_NUMPY and _ULAW_ENC is not None:
        pcm8 = _resample_linear(data, src_rate, 8000)
        arr = np.frombuffer(pcm8, dtype=np.int16)
        # LUT index = int16 qiymat + 32768 (0..65535). `& 0xFFFF` MANFIY
        # qiymatlarni noto'g'ri index'laydi (s=-1000 -> 64536, target 31768 o'rniga).
        idx = arr.astype(np.int32) + 32768
        np.clip(idx, 0, 65535, out=idx)
        return _ULAW_ENC[idx].tobytes()
    log.warning("[SIP Bridge] lin2mulaw konversiya imkonsiz — audioop ham, numpy ham yo'q")
    return data


app = FastAPI(title="SIP / Telephony Bridge (Twilio/VoIP)")

ORCHESTRATOR_WS_URL = os.getenv("ORCHESTRATOR_WS_URL", "ws://127.0.0.1:8080/ws/call")
ORCHESTRATOR_API_KEY = os.getenv("ORCHESTRATOR_API_KEY")
if not ORCHESTRATOR_API_KEY:
    raise RuntimeError("ORCHESTRATOR_API_KEY env required for SIP bridge")

# Twilio webhook autentifikatsiyasi (X-Twilio-Signature). Bo'sh bo'lsa DEV mode.
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")

# Public bazaviy URL — TwiML'da wss manzili shundan quriladi (Host poisoning'ga qarshi).
# Masalan: PUBLIC_BASE_URL=https://voice.traffix.uz
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")


def _public_wss_url(request_host: str) -> str:
    """TwiML Stream url — PUBLIC_BASE_URL dan (fallback: request Host, DEV mode)."""
    base = PUBLIC_BASE_URL or f"https://{request_host}"
    return base.replace("https://", "wss://").replace("http://", "ws://") + "/media-stream"


async def _verify_twilio_signature(request: Request) -> bool:
    """Twilio webhook imzosini tekshiradi (HMAC-SHA1, url + sorted POST params)."""
    if not TWILIO_AUTH_TOKEN:
        log.warning("[SIP Bridge] TWILIO_AUTH_TOKEN o'rnatilmagan — webhook validatsiyasi O'CHIQ (DEV)")
        return True
    sig = request.headers.get("X-Twilio-Signature", "")
    if not sig:
        return False
    try:
        body = await request.body()
        params = parse_qs(body.decode("utf-8", errors="ignore"))
        flat = []
        for k in sorted(params):
            for v in params[k]:
                flat.append((k, v))
        url = str(request.url)
        msg = url + "".join(f"{k}{v}" for k, v in flat)
        expected = hmac.new(TWILIO_AUTH_TOKEN.encode(), msg.encode(), hashlib.sha1).hexdigest()
        return hmac.compare_digest(sig, expected)
    except Exception as e:
        log.exception(f"[SIP Bridge] Signature tekshiruv xatosi: {e}")
        return False


@app.post("/incoming-call")
async def incoming_call(request: Request):
    """Twilio webhook — Media Stream boshlash uchun TwiML qaytaradi.
    Audit fix: X-Twilio-Signature tekshiruvi + PUBLIC_BASE_URL (host poisoning himoyasi)."""
    if not await _verify_twilio_signature(request):
        return Response(content="Forbidden", status_code=403)
    host = request.headers.get("host", "")
    wss_url = _public_wss_url(host)
    log.info(f"[SIP Bridge] incoming-call: wss={wss_url} host={host}")
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <Response>
        <Connect>
            <Stream url="{wss_url}">
                <Parameter name="language" value="uz" />
            </Stream>
        </Connect>
    </Response>"""
    return Response(content=twiml, media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    """Twilio media stream (mulaw 8kHz) → Orchestrator (PCM 16kHz) ko'prigi.

    Audit fixlar:
    - DTMF event'lari ishlanadi (til menyusi: 1=UZ, 2=RU, 3=EN)
    - Twilio `from` (caller) orchestrator'ga metadata orqali uzatiladi
    - audioop yo'q bo'lsa numpy G.711 fallback
    """
    await websocket.accept()
    stream_sid = None
    orchestrator_ws = None
    sip_ended = asyncio.Event()

    # Har bir qo'ng'iroq uchun unikal caller_id
    caller_id = "sip_" + uuid.uuid4().hex

    async def cleanup():
        if orchestrator_ws is not None:
            try:
                await orchestrator_ws.close()
            except Exception:
                pass

    try:
        additional_headers = [("X-API-Key", ORCHESTRATOR_API_KEY)]
        try:
            orchestrator_ws = await websockets.connect(
                f"{ORCHESTRATOR_WS_URL}/{caller_id}",
                additional_headers=additional_headers,
            )
        except Exception as e:
            log.exception(f"Orchestrator WebSocket'ga ulanib bo'lmadi: {e}")
            await websocket.close(code=1011, reason="Orchestrator unavailable")
            return

        async def receive_from_sip():
            nonlocal stream_sid
            try:
                while True:
                    message = await websocket.receive_text()
                    try:
                        data = json.loads(message)
                    except Exception:
                        continue

                    ev = data.get("event")
                    if ev == "start":
                        stream_sid = data.get("start", {}).get("streamSid", "") or ""
                        start = data.get("start", {})
                        call_sid = start.get("callSid") or stream_sid or ""
                        caller = (start.get("from") or "").strip()
                        log.info(f"[SIP Bridge] Qo'ng'iroq boshlandi: stream={stream_sid} "
                                 f"callSid={call_sid} caller={caller or '-'} orch_sid={caller_id}")
                        try:
                            await orchestrator_ws.send(json.dumps({
                                "type": "metadata",
                                "external_id": call_sid,
                                "stream_sid": stream_sid,
                                "caller": caller,
                            }))
                        except Exception:
                            pass
                    elif ev == "dtmf":
                        digit = data.get("dtmf", {}).get("digit", "") or ""
                        if digit:
                            log.info(f"[SIP Bridge] DTMF: {digit}")
                            try:
                                await orchestrator_ws.send(json.dumps({
                                    "type": "dtmf", "digit": digit,
                                }))
                            except Exception:
                                pass
                    elif ev == "media":
                        if not stream_sid:
                            log.warning("[SIP Bridge] media event start'siz keldi — e'tiborsiz")
                            continue
                        payload = base64.b64decode(data["media"]["payload"])
                        try:
                            pcm_16k = _mulaw_to_pcm16k(payload, src_rate=8000)
                        except Exception as e:
                            log.exception(f"audio konversiya xatosi: {e}")
                            continue
                        try:
                            await orchestrator_ws.send(pcm_16k)
                        except Exception as e:
                            log.exception(f"Orchestrator'ga yuborishda xatolik: {e}")
                            return
                    elif ev == "stop":
                        log.info(f"[SIP Bridge] Qo'ng'iroq tugadi: {stream_sid}")
                        return
            except WebSocketDisconnect:
                log.info("[SIP Bridge] SIP provayder ulanishi uzildi.")
            except Exception as e:
                log.exception(f"[SIP Bridge] Provayder o'qishda xatolik: {e}")
            finally:
                sip_ended.set()
                if orchestrator_ws is not None:
                    try:
                        await orchestrator_ws.close()
                    except Exception:
                        pass

        async def receive_from_orchestrator():
            nonlocal orchestrator_ws
            reconnect_backoff = 1
            max_backoff = 30
            while not sip_ended.is_set():
                try:
                    while not sip_ended.is_set():
                        msg = await orchestrator_ws.recv()
                        reconnect_backoff = 1  # Success — reset backoff

                        if isinstance(msg, str):
                            try:
                                data = json.loads(msg)
                                if data.get("type") == "ready":
                                    log.info(f"[SIP Bridge] Orchestrator tayyor: {data.get('caller_id')}")
                            except Exception:
                                pass
                            continue

                        if isinstance(msg, (bytes, bytearray)) and msg:
                            try:
                                mulaw_8k = _pcm16k_to_mulaw(bytes(msg), src_rate=16000)
                            except Exception as e:
                                log.exception(f"audio konvert xatosi (out): {e}")
                                continue
                            payload = base64.b64encode(mulaw_8k).decode('utf-8')
                            if not stream_sid:
                                continue
                            media_msg = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": payload},
                            }
                            try:
                                await websocket.send_text(json.dumps(media_msg))
                            except Exception as e:
                                log.exception(f"SIP provayderga yuborishda xatolik: {e}")
                                return
                except Exception as e:
                    if sip_ended.is_set():
                        log.info("[SIP Bridge] SIP tugagan — orchestrator loop'idan chiqamiz")
                        break
                    log.warning(f"[SIP Bridge] Orchestrator WS uzildi, {reconnect_backoff}s da qayta ulanamiz: {e}")
                    await asyncio.sleep(reconnect_backoff)
                    if sip_ended.is_set():
                        break
                    reconnect_backoff = min(reconnect_backoff * 2, max_backoff)
                    try:
                        orchestrator_ws = await websockets.connect(
                            f"{ORCHESTRATOR_WS_URL}/{caller_id}",
                            additional_headers=additional_headers,
                        )
                        log.info(f"[SIP Bridge] Orchestrator'ga qayta ulandik: {caller_id}")
                    except Exception as ce:
                        log.error(f"[SIP Bridge] Qayta ulana olmadi: {ce}")

        await asyncio.gather(
            receive_from_sip(),
            receive_from_orchestrator(),
            return_exceptions=True,
        )
    except WebSocketDisconnect:
        log.info("[SIP Bridge] SIP Provayder ulanishi uzildi.")
    except Exception as e:
        log.exception(f"[SIP Bridge] Umumiy xatolik: {e}")
    finally:
        await cleanup()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("SIP_BRIDGE_PORT", "8005")))
