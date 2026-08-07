"""Pipeline smoke test (local, mock node bilan):
STT -> Guardrail -> LLM+Tools -> TTS -> resample zanjirini to'liq sinaydi.
Ishga tushirish:  cd orchestrator && PYTHONPATH=/home/ubuntu/ai-operator python3 ../scripts/pipeline_smoke_test.py
"""
import asyncio
import io
import wave
import json
import threading
import sys

import numpy as np
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, '/home/ubuntu/ai-operator')
from orchestrator.security_utils import encrypt_payload, decrypt_payload

report = {}


class MockNode(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)
        try:
            if '/transcribe' in self.path:
                data = json.loads(body.decode())
                audio = decrypt_payload(data['encrypted_audio'])
                report['stt_audio_len'] = len(audio)
                text = 'salom balansim qancha'
                resp = {'encrypted_text': encrypt_payload(text.encode())}
                self._json(resp)
            elif '/chat' in self.path:
                data = json.loads(decrypt_payload(json.loads(body.decode())['encrypted_payload']).decode())
                report['chat_had_tools'] = bool(data.get('tools'))
                report['chat_had_llm_cfg'] = bool(data.get('llm'))
                msgs = data.get('messages', [])
                last = msgs[-1] if msgs else {}
                if last.get('role') == 'tool':
                    out = {'response': "Hisobingiz -15000 so'm. To'lov qilishingiz kerak."}
                else:
                    out = {'tool_calls': [{'id': 'call_test_1', 'type': 'function',
                                           'function': {'name': 'get_client_info', 'arguments': '{}'}}]}
                resp = {'encrypted_payload': encrypt_payload(json.dumps(out).encode())}
                self._json(resp)
            elif '/synthesize' in self.path:
                data = json.loads(body.decode())
                text = decrypt_payload(data['encrypted_text']).decode()
                report['tts_text'] = text
                sr = 24000
                n = int(0.5 * sr)
                s = (np.sin(np.linspace(0, 2 * np.pi * 440, n)) * 5000).astype(np.int16)
                buf = io.BytesIO()
                with wave.open(buf, 'wb') as w:
                    w.setnchannels(1); w.setsampwidth(2); w.setframerate(sr)
                    w.writeframes(s.tobytes())
                payload = buf.getvalue()
                self.send_response(200)
                self.send_header('Content-Type', 'audio/wav')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            else:
                self._json({'error': 'not found'}, code=404)
        except Exception as e:
            self._json({'error': repr(e)}, code=500)

    def _json(self, obj, code=200):
        data = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def main():
    srv = ThreadingHTTPServer(('127.0.0.1', 5099), MockNode)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    import orchestrator.stream_controller as sc
    base = 'http://127.0.0.1:5099'
    sc.stream_controller.update_endpoints(
        {'uz': base + '/transcribe/uz', 'ru': base + '/transcribe/ru', 'en': base + '/transcribe/en'},
        {'uz': base + '/synthesize', 'ru': base + '/synthesize/ru', 'en': base + '/synthesize/en'},
        base + '/chat')

    async def run():
        cid = 'test_call_001'
        sess = sc.stream_controller.get_or_create_session(cid)
        sess.language = 'uz'
        sess.sample_rate = 16000
        received = []

        def cb(pcm):
            received.append(pcm)

        sr = 16000
        speech = (np.sin(np.linspace(0, 2 * np.pi * 300, int(0.8 * sr))) * 8000).astype(np.int16).tobytes()
        silence = np.zeros(int(1.2 * sr), dtype=np.int16).tobytes()
        # Haqiqiy oqimni simulyatsiya: 640 bayt = 20ms audio
        async def feed(data):
            for i in range(0, len(data), 640):
                sc.stream_controller.on_audio_chunk(cid, data[i:i + 640], cb)
                await asyncio.sleep(0.02)
        await feed(speech)
        await feed(silence)

        await asyncio.sleep(12)  # pipeline tugashini kutish
        sc.stream_controller.end_call(cid)

        from orchestrator.session_manager import session_manager
        h = session_manager.get_session(cid)
        roles = [m['role'] for m in h]
        print('=== NATIJALAR ===')
        print('history rollar:', roles)
        print('STT audio uzunligi:', report.get('stt_audio_len'), '(WAV ekanligini bilish uchun: header RIFF bormi)')
        print('chat tools uzatildimi:', report.get('chat_had_tools'))
        print('chat llm cfg uzatildimi:', report.get('chat_had_llm_cfg'))
        print('TTS matn:', report.get('tts_text'))
        print('TTS PCM bo\'laklar soni:', len(received))
        total = sum(len(p) for p in received)
        print('TTS PCM jami bayt:', total, '(kutilgan ~16000)')
        session_manager.clear_session(cid)
        ok = (report.get('chat_had_tools') is True and report.get('chat_had_llm_cfg') is True
              and 'assistant' in roles and report.get('stt_audio_len', 0) > 1000
              and len(received) >= 1 and 12000 <= total <= 22000)
        print('SMOKE TEST:', 'PASS ✅' if ok else 'FAIL ❌')
        return ok

    ok = asyncio.run(run())
    srv.shutdown()
    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
