"""
Stream Controller — SIP RTP audio oqimlarini AI pipeline orqali boshqarish.
- Tool reflection: CRM metodlarini dinamik chaqirish
- Tool loop: LLM tool chaqirishdan to'xtaguncha qayta chaqiriladi
- Tilga mos greeting
- caller_id avtomatik tool argument'ga qo'shiladi
"""
import time
import asyncio
import io
import wave
import logging

import requests
import json

from orchestrator.vad_utils import vad_model
from orchestrator.security.guardrail import guardrail
from orchestrator.security_utils import encrypt_payload, decrypt_payload

logger = logging.getLogger(__name__)


# ─────────────────── CALL SESSION ───────────────────

class CallSession:
    def __init__(self, call_id):
        self.call_id = call_id
        self.audio_buffer = bytearray()
        self.chunk_buffer = bytearray()
        self.is_speaking = False
        self.silence_start_time = None
        self.last_speech_time = time.time()
        self.SILENCE_THRESHOLD = 0.8
        self.CHUNK_SIZE = 512
        self.language = "uz"

    def add_pcm_chunk(self, pcm_data: bytes):
        self.chunk_buffer.extend(pcm_data)
        if len(self.chunk_buffer) >= self.CHUNK_SIZE:
            chunk = self.chunk_buffer[:self.CHUNK_SIZE]
            self.chunk_buffer = self.chunk_buffer[self.CHUNK_SIZE:]
            has_speech = vad_model.is_speech(bytes(chunk))
            if has_speech:
                self.is_speaking = True
                self.silence_start_time = None
                self.last_speech_time = time.time()
                self.audio_buffer.extend(chunk)
            elif self.is_speaking:
                self.audio_buffer.extend(chunk)
                if self.silence_start_time is None:
                    self.silence_start_time = time.time()
                if time.time() - self.silence_start_time > self.SILENCE_THRESHOLD:
                    self.is_speaking = False
                    return self.flush_audio()
        return None

    def flush_audio(self):
        if len(self.audio_buffer) < 4000:
            self.audio_buffer.clear()
            self.silence_start_time = None
            return None
        audio_data = bytes(self.audio_buffer)
        self.audio_buffer.clear()
        self.silence_start_time = None
        wav_io = io.BytesIO()
        with wave.open(wav_io, 'wb') as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(8000)
            wav_file.writeframes(audio_data)
        wav_bytes = wav_io.getvalue()
        logger.info(f"[VAD] 🎤 Call {self.call_id} — {len(wav_bytes)} bayt audio tayyor.")
        return wav_bytes


# ─────────────────── TOOL EXECUTOR (Reflection) ───────────────────

class ToolExecutor:
    """CRM metodlarini reflection orqali dinamik chaqirish."""

    def __init__(self, crm_instance):
        self.crm = crm_instance
        self._method_map = self._build_map()

    def _build_map(self) -> dict:
        """CRM'dagi barcha callable metodlarni ro'yxatga olish."""
        methods = {}
        for name in dir(self.crm):
            if name.startswith("_"):
                continue
            attr = getattr(self.crm, name)
            if callable(attr):
                methods[name] = attr
        return methods

    def execute(self, tool_call: dict, caller_id: str = "") -> dict:
        """Tool call'ni bajarish. caller_id avtomatik argument sifatida qo'shiladi."""
        func_name = tool_call["function"]["name"]
        tool_id = tool_call.get("id", "call_unknown")
        try:
            args_str = tool_call["function"].get("arguments", "{}")
            args = json.loads(args_str) if isinstance(args_str, str) else args_str
        except (json.JSONDecodeError, TypeError):
            args = {}

        # caller_id ni avtomatik qo'shish (telefon raqami sifatida)
        if "phone_number" not in args and caller_id:
            args["phone_number"] = caller_id

        method = self._method_map.get(func_name)
        if not method:
            return {"tool_call_id": tool_id, "success": False,
                    "error": f"Funksiya topilmadi: {func_name}"}

        try:
            import inspect
            sig = inspect.signature(method)
            # Faqat kerakli parametrlarni uzatish
            filtered_args = {k: v for k, v in args.items() if k in sig.parameters}
            result = method(**filtered_args)
            return {"tool_call_id": tool_id, "success": True, "result": result}
        except Exception as e:
            logger.error(f"Tool {func_name} xatosi: {e}")
            return {"tool_call_id": tool_id, "success": False, "error": str(e)}


# ─────────────────── STREAM CONTROLLER ───────────────────

class StreamController:
    def __init__(self):
        self.sessions = {}
        self.stt_endpoints = {}
        self.tts_endpoints = {}
        self.llm_endpoint = None

    def update_endpoints(self, stt, tts, llm):
        self.stt_endpoints = stt
        self.tts_endpoints = tts
        self.llm_endpoint = llm

    def get_or_create_session(self, call_id) -> CallSession:
        if call_id not in self.sessions:
            self.sessions[call_id] = CallSession(call_id)
            if self.llm_endpoint:
                self._prewarm_llm()
        return self.sessions[call_id]

    def _prewarm_llm(self):
        import threading
        def _do():
            try:
                payload = encrypt_payload(json.dumps(
                    {"messages": [{"role": "system", "content": "ping"}]}).encode('utf-8'))
                requests.post(self.llm_endpoint, json={"encrypted_payload": payload}, timeout=2)
            except Exception:
                pass
        threading.Thread(target=_do, daemon=True).start()

    def on_audio_chunk(self, call_id, pcm_chunk, send_audio_callback=None):
        session = self.get_or_create_session(call_id)
        if send_audio_callback:
            session.send_audio_callback = send_audio_callback
        completed_wav = session.add_pcm_chunk(pcm_chunk)
        if completed_wav:
            logger.info(f"[StreamController] 🎧 Call {call_id} → STT ({session.language})")
            asyncio.create_task(self.route_to_ai(call_id, session.language, completed_wav))

    # ─────────────────── ASOSIY AI PIPELINE ───────────────────

    async def route_to_ai(self, call_id, language, wav_bytes):
        from orchestrator.session_manager import session_manager
        from orchestrator.profile_manager import profile_manager

        # 1. STT
        stt_url = self.stt_endpoints.get(language)
        if not stt_url:
            logger.error(f"STT URL topilmadi: {language}")
            return

        try:
            encrypted_audio = encrypt_payload(wav_bytes)
            loop = asyncio.get_running_loop()
            res = await loop.run_in_executor(None, lambda: requests.post(
                stt_url, json={"encrypted_audio": encrypted_audio}, timeout=30))
            if res.status_code != 200:
                logger.error(f"STT xatolik: {res.text}")
                return

            enc_response = res.json().get("encrypted_text", "")
            if not enc_response:
                return
            text = decrypt_payload(enc_response).decode('utf-8').strip()
            if not text:
                return
            logger.info(f"[{call_id}] 📝 STT: {text[:80]}")

            # 2. Guardrail
            session_manager.add_message(call_id, "user", text)
            if guardrail.check_input_violation(text):
                ai_text = (
                    "Kechirasiz, sizning talabingiz xavfsizlik qoidalariga ziddir. "
                    "Tizimga zarar yetkazishga yoki ruxsatsiz ma'lumot olishga urinish "
                    "qat'iyan man etiladi."
                )
                session_manager.add_message(call_id, "assistant", ai_text)
                await self._send_tts(call_id, language, ai_text)
                return

            # 3. LLM + Tool Loop
            profile_manager.auto_select(language)
            session_manager.update_ttl_from_config()

            llm_params = profile_manager.get_llm_params()
            crm = profile_manager.get_crm()
            tools = profile_manager.get_tools()
            executor = ToolExecutor(crm)

            ai_text = await self._llm_tool_loop(
                call_id, tools, executor, llm_params, loop
            )

            if ai_text:
                session_manager.add_message(call_id, "assistant", ai_text)
                await self._send_tts(call_id, language, ai_text)

        except Exception as e:
            logger.error(f"[StreamController] Xatolik: {e}")

    # ─────────────────── LLM + TOOL LOOP ───────────────────

    async def _llm_tool_loop(self, call_id, tools, executor, llm_params, loop, max_iterations=5):
        """LLM'ga so'rov yuborish va tool chaqiruvlarni qayta ishlash.
        LLM tool chaqirishdan to'xtaguncha davom etadi (max 5 marta)."""
        from orchestrator.session_manager import session_manager

        for iteration in range(max_iterations):
            history = session_manager.get_session(call_id)
            if not self.llm_endpoint:
                return "LLM endpoint topilmadi."

            chat_data = {"messages": history, "tools": tools}
            enc_chat = encrypt_payload(json.dumps(chat_data).encode('utf-8'))

            llm_res = await loop.run_in_executor(None, lambda: requests.post(
                self.llm_endpoint, json={"encrypted_payload": enc_chat}, timeout=30))

            if llm_res.status_code != 200:
                logger.error(f"LLM xatolik: {llm_res.text[:200]}")
                return "Uzur, hozir javob bera olmayman."

            enc_llm_resp = llm_res.json().get("encrypted_payload", "")
            ai_resp = json.loads(decrypt_payload(enc_llm_resp).decode('utf-8'))

            # Tool call yo'q — yakuniy javob
            if "tool_calls" not in ai_resp:
                return ai_resp.get("response", "")

            # Tool call'lar bor
            tool_calls = ai_resp["tool_calls"]
            logger.info(f"[{call_id}] 🛠 Tool chaqirildi: {[t['function']['name'] for t in tool_calls]}")

            session_manager.add_message(call_id, "assistant", None, tool_calls=tool_calls)

            for tool in tool_calls:
                result = executor.execute(tool, caller_id=call_id)
                session_manager.add_message(call_id, "tool",
                    json.dumps(result, ensure_ascii=False),
                    tool_call_id=tool.get("id", "call_unknown"))

            # Yana LLM ga yuboramiz (keyingi iteration)

        return "Uzur, so'rovingizni bajarishda tizimda murakkablik yuzaga keldi."

    # ─────────────────── TTS YUBORISH ───────────────────

    async def _send_tts(self, call_id, language, text):
        tts_url = self.tts_endpoints.get(language)
        if not tts_url or not text:
            return
        try:
            loop = asyncio.get_running_loop()
            enc_tts = encrypt_payload(text.encode('utf-8'))
            tts_res = await loop.run_in_executor(None, lambda: requests.post(
                tts_url, json={"encrypted_text": enc_tts}, timeout=20))
            if tts_res.status_code == 200:
                enc_audio = tts_res.json().get("encrypted_audio", "")
                if enc_audio:
                    tts_wav = decrypt_payload(enc_audio)
                    session = self.sessions.get(call_id)
                    if session and getattr(session, 'send_audio_callback', None):
                        with wave.open(io.BytesIO(tts_wav), 'rb') as w:
                            session.send_audio_callback(w.readframes(w.getnframes()))
        except Exception as e:
            logger.error(f"TTS xatolik: {e}")

    # ─────────────────── GREETING (Tilga mos) ───────────────────

    def trigger_greeting(self, call_id, send_audio_callback):
        session = self.get_or_create_session(call_id)
        session.send_audio_callback = send_audio_callback

        async def _trigger():
            try:
                from orchestrator.session_manager import session_manager
                from orchestrator.profile_manager import profile_manager

                lang = session.language
                profile_manager.auto_select(lang)
                greeting_text = profile_manager.get_greeting(lang)

                history = session_manager.get_session(call_id)
                chat_data = {"messages": history, "tools": profile_manager.get_tools()}
                chat_data["messages"].append({"role": "user", "content": greeting_text})

                enc_chat = encrypt_payload(json.dumps(chat_data).encode('utf-8'))
                loop = asyncio.get_running_loop()
                llm_res = await loop.run_in_executor(None, lambda: requests.post(
                    self.llm_endpoint, json={"encrypted_payload": enc_chat}, timeout=30))

                if llm_res.status_code == 200:
                    ai_resp = json.loads(decrypt_payload(
                        llm_res.json()["encrypted_payload"]).decode('utf-8'))
                    ai_text = ai_resp.get("response", "")
                    session_manager.add_message(call_id, "assistant", ai_text)
                    await self._send_tts(call_id, lang, ai_text)
            except Exception as e:
                logger.error(f"Greeting Error: {e}")

        asyncio.create_task(_trigger())

    def end_call(self, call_id):
        if call_id in self.sessions:
            del self.sessions[call_id]


stream_controller = StreamController()
