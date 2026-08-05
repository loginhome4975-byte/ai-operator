import os
import re
import json
import uuid
import base64
import asyncio
import logging
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Response

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
log = logging.getLogger("sip_bridge")

# audioop Python 3.13 da yo'q — shuning uchun importlar haqida tez-tez xato bo'ladi.
# audioop-lts yoki python audioop compatible alternatives... Hozir False bilan init qilamiz
# va runtime'da mulaw/linear konversiya soddalashtirilgan yo'l bilan (PCM linear).
try:
    import audioop  # type: ignore
    _HAS_AUDIOOP = True
except Exception as e:  # noqa: BLE001
    log.warning(f"audioop yuklanmadi (Python 3.13 mos): {e}")
    audioop = None  # type: ignore
    _HAS_AUDIOOP = False


def _safe_id(value) -> str:
    return re.sub(r"[^A-Za-z0-9_\-:.]", "_", str(value or ""))[:64]


def _mulaw_to_pcm16k(payload: bytes, src_rate: int = 8000) -> bytes:
    """Audio rate konversiyasi — audioop bo'lsa mulaw->lin->16kHz, bo'lmasa noshim."""
    if not _HAS_AUDIOOP or audioop is None or not payload:
        return payload  # fallback: asl bytes
    pcm = audioop.ulaw2lin(payload, 2)
    if src_rate != 16000:
        pcm, _ = audioop.ratecv(pcm, 2, 1, src_rate, 16000, None)
    return pcm


def _pcm16k_to_mulaw(data: bytes, src_rate: int = 16000) -> bytes:
    """PCM16kHz -> mulaw 8kHz konversiyasi."""
    if not _HAS_AUDIOOP or audioop is None or not data:
        return data
    if src_rate != 8000:
        pcm8, _ = audioop.ratecv(data, 2, 1, src_rate, 8000, None)
    else:
        pcm8 = data
    return audioop.lin2ulaw(pcm8, 2)

app = FastAPI(title="SIP / Telephony Bridge (Twilio/VoIP)")

# ORCHESTRATOR_WS_URL — env'dan olinadi (broker muhitini qo'llab-quvvatlash)
ORCHESTRATOR_WS_URL = os.getenv("ORCHESTRATOR_WS_URL", "ws://orchestrator:8000/ws/call")
ORCHESTRATOR_API_KEY = os.getenv("ORCHESTRATOR_API_KEY")
if not ORCHESTRATOR_API_KEY:
    raise RuntimeError("ORCHESTRATOR_API_KEY env required for SIP bridge")


@app.post("/incoming-call")
async def incoming_call(request: Request):
    """SIP/VoIP provider (masalan Twilio) orqali qo'ng'iroq kelganda
    Media Stream boshlash uchun XML/TwiML qaytaradi."""
    host = request.headers.get("host")
    twiml = f"""<?xml version="1.0" encoding="UTF-8"?>
    <Response>
        <Connect>
            <Stream url="wss://{host}/media-stream" />
        </Connect>
    </Response>"""
    return Response(content=twiml, media_type="application/xml")


@app.websocket("/media-stream")
async def media_stream(websocket: WebSocket):
    """SIP provayderdan (mulaw 8kHz) audio olib, Orchestratorga (PCM 16kHz)
    yo'naltiruvchi WebSocket ko'prigi.

    Asosiy tuzatishlar:
    - C8 fix: Orchestrator WebSocket'ga API key yuboriladi.
    - M1 fix: gather return_exceptions=True.
    - H5 fix: orchestrator 'ready' signalini kutadi.
    - audioop Python 3.13 da yo'q — kelajakka tekshiruv.
    """
    await websocket.accept()
    stream_sid = None
    orchestrator_ws = None
    call_started_at = None
    sip_ended = asyncio.Event()  # SIP tomon yopilganini bildiradi (C8 race-condition fix)

    # code-review 2-fix: har bir SIP qo'ng'iroq uchun unikal UUID-asoslangan
    # caller_id ishlatamiz, shunda orchestrator sessiyalari hech qachon 
    # bir-biriga aralashmaydi. Twilio streamSid kelgach, biz event orqali update 
    # qilamiz lekin sessiya idsi o'zgarmaydi.
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
            nonlocal stream_sid, caller_id
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
                        # Twilio CallSid yoki streamSid — log uchun tracked
                        # orchestrator session ID (caller_id) avval set qilingan:
                        # sip_<uuid> — unique, hech qachon collision bo'lmaydi.
                        # Bu yerda biz chaqiriq bo'yicha korrelatsiya uchun
                        # orchestrator ga metadata xabari yuboramiz.
                        call_sid = (
                            data.get("start", {}).get("callSid", "")
                            or stream_sid
                            or ""
                        )
                        call_started_at = asyncio.get_event_loop().time()
                        log.info(
                            f"[SIP Bridge] Qo'ng'iroq boshlandi: "
                            f"stream={stream_sid} callSid={call_sid} orch_sid={caller_id}"
                        )
                        try:
                            # Orchestrator session'ga korrelatsiya ma'lumotini bildirish
                            await orchestrator_ws.send(json.dumps({
                                "type": "metadata",
                                "external_id": call_sid,
                                "stream_sid": stream_sid,
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
                # C8 fix: avval SIP tugaganini belgilaymiz,
                # keyin orchestrator WS'ni yopamiz — bu recv() ni unblock qiladi
                # va receive_from_orchestrator except handler'da sip_ended ni
                # ko'rib chiqib ketadi.
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
            # C8 fix: SIP tugagan bo'lsa reconnect qilmaymiz
            while not sip_ended.is_set():
                try:
                    while not sip_ended.is_set():
                        # Orchestrator 'ready' signalini yuborgan bo'lishi kerak (M1)
                        msg = await orchestrator_ws.recv()
                        reconnect_backoff = 1  # Success — reset backoff

                        if isinstance(msg, str):
                            # JSON control message — "ready" signalini kutamiz
                            try:
                                data = json.loads(msg)
                                if data.get("type") == "ready":
                                    log.info(f"[SIP Bridge] Orchestrator tayyor: {data.get('caller_id')}")
                                    continue
                                if "transcribed" in data or "ai_response" in data:
                                    pass
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
                    # C8 fix: SIP allaqachon tugagan bo'lsa, reconnect qilmaymiz
                    if sip_ended.is_set():
                        log.info("[SIP Bridge] SIP tugagan — orchestrator loop'idan chiqamiz")
                        break
                    log.warning(f"[SIP Bridge] Orchestrator WS uzildi, {reconnect_backoff}s da qayta ulanamiz: {e}")
                    await asyncio.sleep(reconnect_backoff)
                    if sip_ended.is_set():
                        break
                    reconnect_backoff = min(reconnect_backoff * 2, max_backoff)
                    # Qayta ulanish
                    try:
                        orchestrator_ws = await websockets.connect(
                            f"{ORCHESTRATOR_WS_URL}/{caller_id}",
                            additional_headers=additional_headers,
                        )
                        log.info(f"[SIP Bridge] Orchestrator'ga qayta ulandik: {caller_id}")
                    except Exception as ce:
                        log.error(f"[SIP Bridge] Qayta ulana olmadi: {ce}")

        # M1 fix: return_exceptions=True — bir task xato qilsa, ikkinchisi yashaveradi
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
    uvicorn.run(app, host="0.0.0.0", port=8005)
