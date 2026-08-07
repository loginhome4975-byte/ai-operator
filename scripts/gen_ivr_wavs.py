#!/usr/bin/env python3
"""IVR ovoz fayllarini yaratadi: orchestrator/menu.wav va orchestrator/wait.wav.

1) Real TTS (Sayro UZ) — orchestrator /synthesize orqali, agar node-0 ishlayotgan bo'lsa.
2) Node ishlamasa — tone placeholder (850Hz beep) generatsiya qilinadi,
   shunda SIP trunk menyu o'lik bo'lmaydi.

Ishlatish:  cd /home/ubuntu/ai-operator && python3 scripts/gen_ivr_wavs.py
"""
import base64
import io
import os
import sys
import json
import wave
import urllib.request

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("AES_256_KEY", os.environ.get("AES_256_KEY", ""))

MENU_TEXT = ("Assalomu alaykum! O'zbek tili uchun bir tugmasini, "
             "rus tili uchun ikki, ingliz tili uchun uch tugmasini bosing.")
WAIT_TEXT = "Iltimos, biroz kuting..."


def _wav_to_pcm8(wav_bytes: bytes) -> bytes:
    """WAV (istalgan rate) -> 8kHz mono 16-bit PCM."""
    with io.BytesIO(wav_bytes) as buf:
        with wave.open(buf, "rb") as w:
            sr = w.getframerate()
            ch = w.getnchannels()
            sw = w.getsampwidth()
            raw = w.readframes(w.getnframes())
    if sw != 2:
        import audioop
        if sw == 1:
            raw = audioop.bias(raw, 1, 128)
        elif sw == 4:
            raw = audioop.lin2lin(raw, 4, 2)
    if ch > 1:
        import audioop
        raw = audioop.tomono(raw, 2, 1, 0)
    if sr != 8000:
        arr = np.frombuffer(raw, dtype=np.int16).astype(np.float32)
        n = len(arr)
        target_n = int(round(n * 8000.0 / sr))
        xp = np.linspace(0, 1, num=n)
        x = np.linspace(0, 1, num=target_n)
        raw = np.interp(x, xp, arr).astype(np.int16).tobytes()
    return raw


def _save_pcm8(path: str, pcm: bytes):
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(8000)
        w.writeframes(pcm)
    with open(path, "wb") as f:
        f.write(buf.getvalue())
    print(f"✅ {path} ({len(pcm) // 16000} soniya @8k)")


def _tone_pcm(freq: float, dur_s: float, amp: int = 6000, sr: int = 8000) -> bytes:
    n = int(sr * dur_s)
    t = np.linspace(0, dur_s, n, endpoint=False)
    s = (np.sin(2 * np.pi * freq * t) * amp).astype(np.int16)
    return s.tobytes()


def _tts_synthesize(text: str) -> bytes:
    """Orchestrator /synthesize (Sayro UZ) orqali real ovoz olish."""
    from orchestrator.security_utils import encrypt_payload, decrypt_payload

    api_key = os.environ.get("ORCHESTRATOR_API_KEY", "")
    tts_url = os.environ.get("KAGGLE_URL", "http://127.0.0.1:5001") + "/synthesize"
    enc = encrypt_payload(json.dumps({"text": text, "language": "uz"}).encode())
    req = urllib.request.Request(
        tts_url,
        data=json.dumps({"encrypted_text": enc}).encode(),
        headers={"Content-Type": "application/json", "X-API-Key": api_key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def main():
    out_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "orchestrator")
    os.makedirs(out_dir, exist_ok=True)
    menu_path = os.path.join(out_dir, "menu.wav")
    wait_path = os.path.join(out_dir, "wait.wav")

    # 1) Real TTS sinovi
    try:
        wav = _tts_synthesize(MENU_TEXT)
        _save_pcm8(menu_path, _wav_to_pcm8(wav))
        wav2 = _tts_synthesize(WAIT_TEXT)
        _save_pcm8(wait_path, _wav_to_pcm8(wav2))
        print("🎙️  Real TTS ovozlari yaratildi.")
        return
    except Exception as e:
        print(f"⚠️  TTS mavjud emas ({e}) — tone placeholder generatsiya qilinmoqda.")

    # 2) Placeholder: 4 ta qisqa beep (menyu), yumshoq hold tone (kutish)
    pcm = bytearray()
    for _ in range(4):
        pcm += _tone_pcm(850, 0.15, amp=5000)
        pcm += np.zeros(int(0.15 * 8000), dtype=np.int16).tobytes()
    _save_pcm8(menu_path, bytes(pcm))

    pcm2 = bytearray()
    for _ in range(4):
        pcm2 += _tone_pcm(1000, 0.4, amp=2500)
        pcm2 += np.zeros(int(0.1 * 8000), dtype=np.int16).tobytes()
    _save_pcm8(wait_path, bytes(pcm2))
    print("📢 Tone placeholder yaratildi (real TTS uchun node-0 ishlaganda qayta ishga tushiring).")


if __name__ == "__main__":
    main()
